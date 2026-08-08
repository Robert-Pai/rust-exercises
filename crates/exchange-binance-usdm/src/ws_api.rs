use std::{collections::BTreeMap, time::Duration};

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
const TRANSPORT_ID_SLOTS: usize = 256;
const TRANSPORT_ID_MASK: u64 = (TRANSPORT_ID_SLOTS as u64) - 1;

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
    response_timeout: Duration,
}

struct WorkerState {
    clock_offset_ms: Option<i64>,
}

struct Command {
    method: &'static str,
    parameters: Parameters,
    mode: ResponseMode,
    clock_retried: bool,
    deadline: tokio::time::Instant,
    reply: oneshot::Sender<ExchangeResult<Value>>,
}

#[derive(Clone, Copy)]
enum ResponseMode {
    Direct,
    Cancel,
    CancelQuery,
}

struct PendingRequest {
    id: u64,
    deadline: tokio::time::Instant,
    method: &'static str,
    mode: ResponseMode,
    parameters: Parameters,
    clock_retried: bool,
    reply: oneshot::Sender<ExchangeResult<Value>>,
}

type PendingSlots = [Option<PendingRequest>; TRANSPORT_ID_SLOTS];

enum DispatchOutcome {
    Continue,
    Reset(ExchangeError),
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
                response_timeout: config.request_timeout(),
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
        let command = Command {
            method,
            parameters,
            mode,
            clock_retried: false,
            deadline: tokio::time::Instant::now() + self.response_timeout,
            reply,
        };
        if let Err(error) = self.commands.try_push(command) {
            return ready_error(command_enqueue_error(error));
        }
        Box::pin(async move {
            response.await.unwrap_or_else(|_| {
                Err(ExchangeError::new(
                    ExchangeErrorKind::Network,
                    "Binance WebSocket API response channel closed",
                ))
            })
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
    let mut pending: PendingSlots = std::array::from_fn(|_| None);
    let mut next_request_id = 1_u64;
    let mut state = WorkerState {
        clock_offset_ms: None,
    };

    loop {
        if socket.is_none() {
            let Some(command) = commands.recv().await else {
                return;
            };
            if command_is_inactive(&command) {
                finish_inactive(command);
                continue;
            }
            let connect_timeout = remaining(command.deadline).unwrap_or(Duration::ZERO);
            match connect(
                &config.endpoint,
                connect_timeout.min(config.connect_timeout),
            )
            .await
            {
                Ok(mut connected) => {
                    match dispatch(
                        &mut connected,
                        &mut pending,
                        &mut next_request_id,
                        &config,
                        &mut state,
                        command,
                    )
                    .await
                    {
                        DispatchOutcome::Continue => socket = Some(connected),
                        DispatchOutcome::Reset(error) => fail_pending(&mut pending, error),
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
        let deadline = next_pending_deadline(&pending);
        tokio::select! {
            command = commands.recv(), if pending_count(&pending) < TRANSPORT_ID_SLOTS => {
                let Some(command) = command else {
                    fail_pending(
                        &mut pending,
                        ExchangeError::new(
                            ExchangeErrorKind::ServiceUnavailable,
                            "Binance WebSocket API worker stopped",
                        ),
                    );
                    return;
                };
                if command_is_inactive(&command) {
                    finish_inactive(command);
                    continue;
                }
                if let DispatchOutcome::Reset(error) = dispatch(
                    connected,
                    &mut pending,
                    &mut next_request_id,
                    &config,
                    &mut state,
                    command,
                ).await {
                    fail_pending(&mut pending, error);
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
                    write_timeout(deadline, config.response_timeout),
                    message,
                ).await {
                    fail_pending(&mut pending, error);
                    socket = None;
                }
            }
            () = wait_for_deadline(deadline) => {
                expire_pending(&mut pending, tokio::time::Instant::now());
            }
        }
    }
}

fn command_is_inactive(command: &Command) -> bool {
    command.reply.is_closed() || remaining(command.deadline).is_none()
}

fn finish_inactive(command: Command) {
    if !command.reply.is_closed() {
        let _ = command.reply.send(Err(response_timeout_error()));
    }
}

fn remaining(deadline: tokio::time::Instant) -> Option<Duration> {
    deadline
        .checked_duration_since(tokio::time::Instant::now())
        .filter(|remaining| !remaining.is_zero())
}

fn write_timeout(deadline: Option<tokio::time::Instant>, fallback: Duration) -> Duration {
    deadline
        .and_then(remaining)
        .map_or(fallback, |remaining| remaining.min(fallback))
}

async fn wait_for_deadline(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

async fn dispatch(
    socket: &mut Socket,
    pending: &mut PendingSlots,
    next_request_id: &mut u64,
    config: &WorkerConfig,
    state: &mut WorkerState,
    command: Command,
) -> DispatchOutcome {
    let deadline = command.deadline;
    if command.reply.is_closed() {
        return DispatchOutcome::Continue;
    }
    if remaining(deadline).is_none() {
        let _ = command.reply.send(Err(response_timeout_error()));
        return DispatchOutcome::Continue;
    }
    let clock_offset_ms = match state.clock_offset_ms {
        Some(offset) => offset,
        None => match config.clock.clock_offset_ms().await {
            Ok(offset) => {
                state.clock_offset_ms = Some(offset);
                offset
            }
            Err(error) => {
                let _ = command.reply.send(Err(error));
                return DispatchOutcome::Continue;
            }
        },
    };
    if command.reply.is_closed() {
        return DispatchOutcome::Continue;
    }
    if remaining(deadline).is_none() {
        let _ = command.reply.send(Err(response_timeout_error()));
        return DispatchOutcome::Continue;
    }

    let id = next_transport_id(next_request_id);
    let slot = transport_slot(id);
    if pending[slot].is_some() {
        let error = ExchangeError::new(
            ExchangeErrorKind::StateConflict,
            "Binance WebSocket API transport slot was occupied below capacity",
        );
        let _ = command.reply.send(Err(error.clone()));
        return DispatchOutcome::Reset(error);
    }

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
            return DispatchOutcome::Continue;
        }
    };
    signed.insert("timestamp".to_owned(), json!(timestamp));
    let payload = match signature_payload(&signed) {
        Ok(payload) => payload,
        Err(error) => {
            let _ = command.reply.send(Err(error));
            return DispatchOutcome::Continue;
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
            return DispatchOutcome::Continue;
        }
    };

    pending[slot] = Some(PendingRequest {
        id,
        deadline,
        method,
        mode,
        parameters: recovery_parameters,
        clock_retried: command.clock_retried,
        reply: command.reply,
    });
    let Some(send_timeout) = remaining(deadline) else {
        return DispatchOutcome::Reset(response_timeout_error());
    };
    match tokio::time::timeout(send_timeout, socket.send(Message::Text(payload.into()))).await {
        Ok(Ok(())) => DispatchOutcome::Continue,
        Ok(Err(_)) => DispatchOutcome::Reset(ExchangeError::new(
            ExchangeErrorKind::Network,
            "Binance WebSocket API request send failed",
        )),
        Err(_) => DispatchOutcome::Reset(response_timeout_error()),
    }
}

async fn handle_message(
    socket: &mut Socket,
    pending: &mut PendingSlots,
    next_request_id: &mut u64,
    config: &WorkerConfig,
    state: &mut WorkerState,
    write_timeout: Duration,
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
            let request = take_pending(pending, response.id)?;
            if request.reply.is_closed() {
                return None;
            }
            if remaining(request.deadline).is_none() {
                let _ = request.reply.send(Err(response_timeout_error()));
                return None;
            }
            if !request.clock_retried
                && response
                    .error
                    .as_ref()
                    .is_some_and(|error| error.code == -1021)
            {
                fail_pending(
                    pending,
                    ExchangeError::new(
                        ExchangeErrorKind::ServiceUnavailable,
                        "Binance WebSocket API clock resynchronization interrupted pending requests",
                    ),
                );
                let Some(clock_timeout) = remaining(request.deadline) else {
                    let _ = request.reply.send(Err(response_timeout_error()));
                    return None;
                };
                state.clock_offset_ms =
                    match tokio::time::timeout(clock_timeout, config.clock.clock_offset_ms()).await
                    {
                        Ok(Ok(offset)) => Some(offset),
                        Ok(Err(error)) => {
                            let _ = request.reply.send(Err(error));
                            return None;
                        }
                        Err(_) => {
                            let _ = request.reply.send(Err(response_timeout_error()));
                            return None;
                        }
                    };
                let command = Command {
                    method: request.method,
                    parameters: request.parameters,
                    mode: request.mode,
                    clock_retried: true,
                    deadline: request.deadline,
                    reply: request.reply,
                };
                if let DispatchOutcome::Reset(error) =
                    dispatch(socket, pending, next_request_id, config, state, command).await
                {
                    return Some(error);
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
                    deadline: request.deadline,
                    reply: request.reply,
                };
                if let DispatchOutcome::Reset(error) =
                    dispatch(socket, pending, next_request_id, config, state, command).await
                {
                    return Some(error);
                }
                return None;
            }

            let result = if (200..300).contains(&response.status) {
                response.result.ok_or_else(|| {
                    ExchangeError::new(
                        ExchangeErrorKind::InvalidResponse,
                        format!(
                            "Binance WebSocket API returned status {} without a result body",
                            response.status
                        ),
                    )
                })
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
            match tokio::time::timeout(write_timeout, socket.send(Message::Pong(payload))).await {
                Ok(Ok(())) => None,
                Ok(Err(_)) => Some(ExchangeError::new(
                    ExchangeErrorKind::Network,
                    "Binance WebSocket API pong send failed",
                )),
                Err(_) => Some(ExchangeError::new(
                    ExchangeErrorKind::Timeout,
                    "Binance WebSocket API pong send timed out",
                )),
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

fn next_transport_id(next_request_id: &mut u64) -> u64 {
    if *next_request_id == 0 {
        *next_request_id = 1;
    }
    let id = *next_request_id;
    *next_request_id = next_request_id.wrapping_add(1);
    if *next_request_id == 0 {
        *next_request_id = 1;
    }
    id
}

fn pending_count(pending: &PendingSlots) -> usize {
    pending.iter().filter(|slot| slot.is_some()).count()
}

fn transport_slot(id: u64) -> usize {
    usize::try_from(id & TRANSPORT_ID_MASK).expect("transport slot fits usize")
}

fn take_pending(pending: &mut PendingSlots, id: u64) -> Option<PendingRequest> {
    let slot = transport_slot(id);
    pending[slot]
        .as_ref()
        .is_some_and(|request| request.id == id)
        .then(|| {
            pending[slot]
                .take()
                .expect("validated request slot is occupied")
        })
}

fn next_pending_deadline(pending: &PendingSlots) -> Option<tokio::time::Instant> {
    pending
        .iter()
        .filter_map(|request| request.as_ref().map(|request| request.deadline))
        .min()
}

fn expire_pending(pending: &mut PendingSlots, now: tokio::time::Instant) {
    for slot in pending.iter_mut() {
        if slot.as_ref().is_some_and(|request| request.deadline <= now) {
            let request = slot.take().expect("expired request slot is occupied");
            let _ = request.reply.send(Err(response_timeout_error()));
        }
    }
}

fn response_timeout_error() -> ExchangeError {
    ExchangeError::new(
        ExchangeErrorKind::Timeout,
        "Binance WebSocket API response timed out",
    )
}

fn fail_pending(pending: &mut PendingSlots, error: ExchangeError) {
    for slot in pending.iter_mut() {
        if let Some(request) = slot.take() {
            let _ = request.reply.send(Err(error.clone()));
        }
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
