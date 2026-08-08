use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use maker_domain::{ClientOrderId, InstrumentSpec, OrderIntent, Side, Symbol};
use maker_ports::{ExchangeError, ExchangeErrorKind, ExchangeFuture, ExchangeResult};
use maker_runtime::{SpscConsumer, SpscProducer, TryPushError, spsc_channel};
use secrecy::ExposeSecret;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::sync::oneshot;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::{
    config::{BinanceCredentials, BinanceUsdmConfig},
    error,
    models::{ApiErrorDto, WsApiCancelAllDto, WsApiOrderAckDto, WsApiOrderDto},
    network::NetworkRuntime,
    rest::RestClient,
    signing::sign_payload,
};

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
type Parameters = BTreeMap<String, Value>;

const COMMAND_BUFFER: usize = 256;

/// Move-only strategy-side producer for one persistent trading WebSocket worker.
pub(crate) struct WsApiClient {
    commands: SpscProducer<Command>,
    response_timeout: Duration,
}

struct WorkerConfig {
    endpoint: String,
    credentials: BinanceCredentials,
    clock: RestClient,
    recv_window_ms: u64,
    connect_timeout: Duration,
}

struct WorkerState {
    clock_offset_ms: Option<i64>,
}

struct Command {
    method: &'static str,
    parameters: Parameters,
    mode: ResponseMode,
    clock_retried: bool,
    reply: oneshot::Sender<ExchangeResult<Value>>,
}

#[derive(Clone, Copy)]
enum ResponseMode {
    Direct,
    Cancel,
    CancelQuery,
}

struct PendingRequest {
    method: &'static str,
    mode: ResponseMode,
    parameters: Parameters,
    clock_retried: bool,
    reply: oneshot::Sender<ExchangeResult<Value>>,
}

#[derive(Debug, Deserialize)]
struct WsApiResponse {
    id: u64,
    status: u16,
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<ApiErrorDto>,
}

impl WsApiClient {
    pub(crate) fn new(
        config: &BinanceUsdmConfig,
        credentials: BinanceCredentials,
        clock: RestClient,
        network: &mut NetworkRuntime,
    ) -> ExchangeResult<Self> {
        config.validate()?;
        let recv_window_ms = u64::try_from(config.recv_window().as_millis()).map_err(|_| {
            ExchangeError::new(
                ExchangeErrorKind::InvalidRequest,
                "Binance recv window does not fit in milliseconds",
            )
        })?;
        let (commands, receiver) = spsc_channel(COMMAND_BUFFER);
        network.spawn(run_worker(
            WorkerConfig {
                endpoint: config.websocket_api_url().to_owned(),
                credentials,
                clock,
                recv_window_ms,
                connect_timeout: config.request_timeout(),
            },
            receiver,
        ))?;
        Ok(Self {
            commands,
            response_timeout: config.request_timeout(),
        })
    }

    pub(crate) fn place_order(
        &mut self,
        spec: &InstrumentSpec,
        intent: &OrderIntent,
    ) -> ExchangeFuture<WsApiOrderAckDto> {
        let price = spec
            .ticks_to_price(intent.price())
            .map_err(|error| error::invalid_response("order price", error));
        let quantity = spec
            .lots_to_quantity(intent.quantity())
            .map_err(|error| error::invalid_response("order quantity", error));
        let parameters = match (price, quantity) {
            (Ok(price), Ok(quantity)) => Parameters::from([
                (
                    "newClientOrderId".to_owned(),
                    json!(intent.client_order_id().to_string()),
                ),
                ("newOrderRespType".to_owned(), json!("ACK")),
                ("positionSide".to_owned(), json!("BOTH")),
                ("price".to_owned(), json!(price.to_string())),
                ("quantity".to_owned(), json!(quantity.to_string())),
                (
                    "side".to_owned(),
                    json!(match intent.side() {
                        Side::Buy => "BUY",
                        Side::Sell => "SELL",
                    }),
                ),
                ("symbol".to_owned(), json!(intent.symbol().as_str())),
                ("timeInForce".to_owned(), json!("GTX")),
                ("type".to_owned(), json!("LIMIT")),
            ]),
            (Err(error), _) | (_, Err(error)) => return ready_error(error),
        };
        self.request("order.place", parameters)
    }

    pub(crate) fn cancel_order(
        &mut self,
        symbol: &Symbol,
        client_order_id: &ClientOrderId,
    ) -> ExchangeFuture<Option<WsApiOrderDto>> {
        let response = self.request_value(
            "order.cancel",
            order_identity_parameters(symbol, client_order_id),
            ResponseMode::Cancel,
        );
        Box::pin(async move {
            let value = response.await?;
            if value.is_null() {
                Ok(None)
            } else {
                serde_json::from_value(value)
                    .map(Some)
                    .map_err(|error| error::invalid_response("WebSocket API result", error))
            }
        })
    }

    #[cfg(test)]
    pub(crate) fn query_order(
        &mut self,
        symbol: &Symbol,
        client_order_id: &ClientOrderId,
    ) -> ExchangeFuture<WsApiOrderDto> {
        self.request(
            "order.status",
            order_identity_parameters(symbol, client_order_id),
        )
    }

    pub(crate) fn cancel_all(&mut self, symbol: &Symbol) -> ExchangeFuture<()> {
        let response = self.request::<WsApiCancelAllDto>(
            "openOrders.cancelAll",
            Parameters::from([("symbol".to_owned(), json!(symbol.as_str()))]),
        );
        Box::pin(async move {
            let response = response.await?;
            if response.code != 200 {
                return Err(ExchangeError::new(
                    ExchangeErrorKind::InvalidResponse,
                    format!(
                        "Binance WebSocket cancel-all result had code {}: {}",
                        response.code, response.message
                    ),
                ));
            }
            Ok(())
        })
    }

    fn request<T>(&mut self, method: &'static str, parameters: Parameters) -> ExchangeFuture<T>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let response = self.request_value(method, parameters, ResponseMode::Direct);
        Box::pin(async move {
            let value = response.await?;
            serde_json::from_value(value)
                .map_err(|error| error::invalid_response("WebSocket API result", error))
        })
    }

    fn request_value(
        &mut self,
        method: &'static str,
        parameters: Parameters,
        mode: ResponseMode,
    ) -> ExchangeFuture<Value> {
        let (reply, response) = oneshot::channel();
        if let Err(error) = self.commands.try_push(Command {
            method,
            parameters,
            mode,
            clock_retried: false,
            reply,
        }) {
            return ready_error(command_enqueue_error(error));
        }
        let response_timeout = self.response_timeout;
        Box::pin(async move {
            match tokio::time::timeout(response_timeout, response).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err(ExchangeError::new(
                    ExchangeErrorKind::Network,
                    "Binance WebSocket API response channel closed",
                )),
                Err(_) => Err(ExchangeError::new(
                    ExchangeErrorKind::Timeout,
                    "Binance WebSocket API response timed out",
                )),
            }
        })
    }
}

fn ready_error<T: Send + 'static>(error: ExchangeError) -> ExchangeFuture<T> {
    Box::pin(async move { Err(error) })
}

fn command_enqueue_error(error: TryPushError<Command>) -> ExchangeError {
    match error {
        TryPushError::Full(_) => ExchangeError::new(
            ExchangeErrorKind::ServiceUnavailable,
            "Binance WebSocket API command queue is full",
        ),
        TryPushError::ConsumerDropped(_) => ExchangeError::new(
            ExchangeErrorKind::ServiceUnavailable,
            "Binance WebSocket API worker stopped",
        ),
    }
}

async fn run_worker(config: WorkerConfig, mut commands: SpscConsumer<Command>) {
    let mut socket = None;
    let mut pending = HashMap::new();
    let mut next_request_id = 1_u64;
    let mut state = WorkerState {
        clock_offset_ms: None,
    };

    loop {
        if socket.is_none() {
            let Some(command) = commands.recv().await else {
                return;
            };
            match connect(&config.endpoint, config.connect_timeout).await {
                Ok(mut connected) => {
                    if dispatch(
                        &mut connected,
                        &mut pending,
                        &mut next_request_id,
                        &config,
                        &mut state,
                        command,
                    )
                    .await
                    {
                        socket = Some(connected);
                    }
                }
                Err(error) => {
                    let _ = command.reply.send(Err(error));
                }
            }
            continue;
        }

        let connected = socket
            .as_mut()
            .expect("WebSocket API socket was checked above");
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else {
                    return;
                };
                if !dispatch(
                    connected,
                    &mut pending,
                    &mut next_request_id,
                    &config,
                    &mut state,
                    command,
                ).await {
                    fail_pending(
                        &mut pending,
                        ExchangeError::new(
                            ExchangeErrorKind::Network,
                            "Binance WebSocket API connection was lost while sending",
                        ),
                    );
                    socket = None;
                }
            }
            message = connected.next() => {
                if let Some(error) = handle_message(
                    connected,
                    &mut pending,
                    &mut next_request_id,
                    &config,
                    &mut state,
                    message,
                ).await {
                    fail_pending(&mut pending, error);
                    socket = None;
                }
            }
        }
    }
}

async fn dispatch(
    socket: &mut Socket,
    pending: &mut HashMap<u64, PendingRequest>,
    next_request_id: &mut u64,
    config: &WorkerConfig,
    state: &mut WorkerState,
    command: Command,
) -> bool {
    let clock_offset_ms = match state.clock_offset_ms {
        Some(offset) => offset,
        None => match config.clock.clock_offset_ms().await {
            Ok(offset) => {
                state.clock_offset_ms = Some(offset);
                offset
            }
            Err(error) => {
                let _ = command.reply.send(Err(error));
                return true;
            }
        },
    };
    let id = *next_request_id;
    *next_request_id = next_request_id.wrapping_add(1);
    let mode = command.mode;
    let method = command.method;
    let recovery_parameters = command.parameters.clone();
    let mut signed = command.parameters;
    signed.insert(
        "apiKey".to_owned(),
        json!(config.credentials.api_key().expose_secret()),
    );
    signed.insert("recvWindow".to_owned(), json!(config.recv_window_ms));
    let timestamp = match signed_timestamp_ms(clock_offset_ms) {
        Ok(timestamp) => timestamp,
        Err(error) => {
            let _ = command.reply.send(Err(error));
            return true;
        }
    };
    signed.insert("timestamp".to_owned(), json!(timestamp));
    let payload = match signature_payload(&signed) {
        Ok(payload) => payload,
        Err(error) => {
            let _ = command.reply.send(Err(error));
            return true;
        }
    };
    signed.insert(
        "signature".to_owned(),
        json!(sign_payload(&payload, config.credentials.signing_key())),
    );
    let payload = match serde_json::to_string(&json!({
        "id": id,
        "method": command.method,
        "params": signed,
    })) {
        Ok(payload) => payload,
        Err(error) => {
            let _ = command
                .reply
                .send(Err(error::invalid_response("WebSocket API request", error)));
            return true;
        }
    };
    if socket.send(Message::Text(payload.into())).await.is_err() {
        let _ = command.reply.send(Err(ExchangeError::new(
            ExchangeErrorKind::Network,
            "Binance WebSocket API request send failed",
        )));
        return false;
    }
    if let Some(previous) = pending.insert(
        id,
        PendingRequest {
            method,
            mode,
            parameters: recovery_parameters,
            clock_retried: command.clock_retried,
            reply: command.reply,
        },
    ) {
        let _ = previous.reply.send(Err(ExchangeError::new(
            ExchangeErrorKind::StateConflict,
            "duplicate Binance WebSocket API request ID",
        )));
    }
    true
}

async fn handle_message(
    socket: &mut Socket,
    pending: &mut HashMap<u64, PendingRequest>,
    next_request_id: &mut u64,
    config: &WorkerConfig,
    state: &mut WorkerState,
    message: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
) -> Option<ExchangeError> {
    match message {
        Some(Ok(Message::Text(text))) => {
            let response: WsApiResponse = match serde_json::from_str(text.as_ref()) {
                Ok(response) => response,
                Err(error) => {
                    return Some(error::invalid_response("WebSocket API response", error));
                }
            };
            let request = pending.remove(&response.id)?;
            if !request.clock_retried
                && response
                    .error
                    .as_ref()
                    .is_some_and(|error| error.code == -1021)
            {
                state.clock_offset_ms = match config.clock.clock_offset_ms().await {
                    Ok(offset) => Some(offset),
                    Err(error) => {
                        let _ = request.reply.send(Err(error));
                        return None;
                    }
                };
                let command = Command {
                    method: request.method,
                    parameters: request.parameters,
                    mode: request.mode,
                    clock_retried: true,
                    reply: request.reply,
                };
                if !dispatch(socket, pending, next_request_id, config, state, command).await {
                    return Some(ExchangeError::new(
                        ExchangeErrorKind::Network,
                        "Binance WebSocket API connection was lost during clock retry",
                    ));
                }
                return None;
            }
            if matches!(request.mode, ResponseMode::Cancel)
                && response
                    .error
                    .as_ref()
                    .is_some_and(|error| matches!(error.code, -2011 | -2013))
            {
                let command = Command {
                    method: "order.status",
                    parameters: request.parameters,
                    mode: ResponseMode::CancelQuery,
                    clock_retried: request.clock_retried,
                    reply: request.reply,
                };
                if !dispatch(socket, pending, next_request_id, config, state, command).await {
                    return Some(ExchangeError::new(
                        ExchangeErrorKind::Network,
                        "Binance WebSocket API connection was lost during cancel recovery",
                    ));
                }
                return None;
            }

            let result = if (200..300).contains(&response.status) {
                Ok(response.result.unwrap_or(Value::Null))
            } else if matches!(request.mode, ResponseMode::CancelQuery)
                && response
                    .error
                    .as_ref()
                    .is_some_and(|error| error.code == -2013)
            {
                Ok(Value::Null)
            } else if let Some(api_error) = response.error {
                Err(error::websocket_api(response.status, api_error))
            } else {
                Err(ExchangeError::new(
                    ExchangeErrorKind::InvalidResponse,
                    format!(
                        "Binance WebSocket API returned status {} without an error body",
                        response.status
                    ),
                ))
            };
            let _ = request.reply.send(result);
            None
        }
        Some(Ok(Message::Ping(payload))) => {
            if socket.send(Message::Pong(payload)).await.is_err() {
                Some(ExchangeError::new(
                    ExchangeErrorKind::Network,
                    "Binance WebSocket API pong send failed",
                ))
            } else {
                None
            }
        }
        Some(Ok(Message::Close(_))) | None => Some(ExchangeError::new(
            ExchangeErrorKind::Network,
            "Binance WebSocket API connection closed",
        )),
        Some(Ok(Message::Binary(_) | Message::Pong(_) | Message::Frame(_))) => None,
        Some(Err(error)) => Some(error::websocket(error)),
    }
}

async fn connect(endpoint: &str, timeout: Duration) -> ExchangeResult<Socket> {
    let connection = tokio::time::timeout(timeout, connect_async(endpoint))
        .await
        .map_err(|_| {
            ExchangeError::new(
                ExchangeErrorKind::Timeout,
                "Binance WebSocket API connection timed out",
            )
        })?
        .map_err(error::websocket)?;
    Ok(connection.0)
}

fn fail_pending(pending: &mut HashMap<u64, PendingRequest>, error: ExchangeError) {
    for (_, request) in pending.drain() {
        let _ = request.reply.send(Err(error.clone()));
    }
}

fn signed_timestamp_ms(clock_offset_ms: i64) -> ExchangeResult<u64> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| {
            ExchangeError::new(
                ExchangeErrorKind::StateConflict,
                "local system time is before the Unix epoch",
            )
        })?;
    let now = i64::try_from(elapsed.as_millis()).map_err(|_| {
        ExchangeError::new(
            ExchangeErrorKind::StateConflict,
            "local system time exceeds supported range",
        )
    })?;
    let adjusted = now.checked_add(clock_offset_ms).ok_or_else(|| {
        ExchangeError::new(
            ExchangeErrorKind::StateConflict,
            "adjusted Binance timestamp overflowed",
        )
    })?;
    u64::try_from(adjusted).map_err(|_| {
        ExchangeError::new(
            ExchangeErrorKind::StateConflict,
            "adjusted Binance timestamp is before the Unix epoch",
        )
    })
}

fn order_identity_parameters(symbol: &Symbol, client_order_id: &ClientOrderId) -> Parameters {
    Parameters::from([
        (
            "origClientOrderId".to_owned(),
            json!(client_order_id.to_string()),
        ),
        ("symbol".to_owned(), json!(symbol.as_str())),
    ])
}

fn signature_payload(parameters: &Parameters) -> ExchangeResult<String> {
    parameters
        .iter()
        .map(|(name, value)| {
            let value = match value {
                Value::String(value) => value.clone(),
                Value::Number(value) => value.to_string(),
                Value::Bool(value) => value.to_string(),
                _ => {
                    return Err(ExchangeError::new(
                        ExchangeErrorKind::InvalidRequest,
                        format!(
                            "Binance WebSocket API signature parameter {name:?} must be scalar"
                        ),
                    ));
                }
            };
            Ok(format!("{name}={value}"))
        })
        .collect::<ExchangeResult<Vec<_>>>()
        .map(|pairs| pairs.join("&"))
}

#[cfg(test)]
mod tests {
    use maker_domain::{MarketKind, PriceTicks, QuantityLots};
    use rust_decimal_macros::dec;
    use secrecy::SecretString;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    use super::*;

    fn test_spec() -> InstrumentSpec {
        InstrumentSpec::new(
            Symbol::new("BTCUSDT").unwrap(),
            MarketKind::LinearPerpetual,
            dec!(0.1),
            dec!(0.001),
            dec!(0.001),
            dec!(1000),
        )
        .unwrap()
    }

    // The full persistent-connection integration test below exercises all four
    // command variants through the move-only producer.
    include!("ws_api_tests.inc.rs");
}
