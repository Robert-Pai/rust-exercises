use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use maker_domain::{ClientOrderId, InstrumentSpec, OrderIntent, Side, Symbol};
use maker_ports::{ExchangeError, ExchangeErrorKind, ExchangeResult};
use secrecy::ExposeSecret;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc, oneshot};
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

/// Multiplexed request/response client for the Binance USD-M WebSocket API.
///
/// The worker owns one persistent connection, correlates concurrent requests
/// by ID, services WebSocket ping frames while idle, and reconnects before the
/// next request after transport failure.
#[derive(Clone)]
pub(crate) struct WsApiClient {
    inner: Arc<WsApiInner>,
}

struct WsApiInner {
    endpoint: Arc<str>,
    credentials: BinanceCredentials,
    clock: RestClient,
    recv_window_ms: u64,
    connect_timeout: Duration,
    response_timeout: Duration,
    next_request_id: AtomicU64,
    network: NetworkRuntime,
    worker: Mutex<Option<mpsc::Sender<Command>>>,
}

enum Command {
    Request {
        id: u64,
        method: &'static str,
        parameters: Parameters,
        reply: oneshot::Sender<ExchangeResult<Value>>,
    },
    Forget {
        id: u64,
    },
}

struct PendingRequest {
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
        network: NetworkRuntime,
    ) -> ExchangeResult<Self> {
        config.validate()?;
        let recv_window_ms = u64::try_from(config.recv_window().as_millis()).map_err(|_| {
            ExchangeError::new(
                ExchangeErrorKind::InvalidRequest,
                "Binance recv window does not fit in milliseconds",
            )
        })?;
        Ok(Self {
            inner: Arc::new(WsApiInner {
                endpoint: Arc::from(config.websocket_api_url()),
                credentials,
                clock,
                recv_window_ms,
                connect_timeout: config.request_timeout(),
                response_timeout: config.request_timeout(),
                next_request_id: AtomicU64::new(1),
                network,
                worker: Mutex::new(None),
            }),
        })
    }

    pub(crate) async fn place_order(
        &self,
        spec: &InstrumentSpec,
        intent: &OrderIntent,
    ) -> ExchangeResult<WsApiOrderAckDto> {
        let price = spec
            .ticks_to_price(intent.price())
            .map_err(|error| error::invalid_response("order price", error))?;
        let quantity = spec
            .lots_to_quantity(intent.quantity())
            .map_err(|error| error::invalid_response("order quantity", error))?;
        let side = match intent.side() {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        };
        let parameters = Parameters::from([
            (
                "newClientOrderId".to_owned(),
                json!(intent.client_order_id().to_string()),
            ),
            ("newOrderRespType".to_owned(), json!("ACK")),
            ("positionSide".to_owned(), json!("BOTH")),
            ("price".to_owned(), json!(price.to_string())),
            ("quantity".to_owned(), json!(quantity.to_string())),
            ("side".to_owned(), json!(side)),
            ("symbol".to_owned(), json!(intent.symbol().as_str())),
            ("timeInForce".to_owned(), json!("GTX")),
            ("type".to_owned(), json!("LIMIT")),
        ]);
        self.authenticated_request("order.place", parameters).await
    }

    pub(crate) async fn cancel_order(
        &self,
        symbol: &Symbol,
        client_order_id: &ClientOrderId,
    ) -> ExchangeResult<WsApiOrderDto> {
        self.authenticated_request(
            "order.cancel",
            order_identity_parameters(symbol, client_order_id),
        )
        .await
    }

    pub(crate) async fn query_order(
        &self,
        symbol: &Symbol,
        client_order_id: &ClientOrderId,
    ) -> ExchangeResult<WsApiOrderDto> {
        self.authenticated_request(
            "order.status",
            order_identity_parameters(symbol, client_order_id),
        )
        .await
    }

    pub(crate) async fn cancel_all(&self, symbol: &Symbol) -> ExchangeResult<()> {
        let response: WsApiCancelAllDto = self
            .authenticated_request(
                "openOrders.cancelAll",
                Parameters::from([("symbol".to_owned(), json!(symbol.as_str()))]),
            )
            .await?;
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
    }

    async fn authenticated_request<T>(
        &self,
        method: &'static str,
        parameters: Parameters,
    ) -> ExchangeResult<T>
    where
        T: DeserializeOwned,
    {
        self.synchronize_clock(false).await?;
        for attempt in 0..2 {
            let mut signed = parameters.clone();
            signed.insert(
                "apiKey".to_owned(),
                json!(self.inner.credentials.api_key().expose_secret()),
            );
            signed.insert("recvWindow".to_owned(), json!(self.inner.recv_window_ms));
            signed.insert(
                "timestamp".to_owned(),
                json!(self.inner.clock.signed_timestamp_ms()?),
            );
            let payload = signature_payload(&signed)?;
            signed.insert(
                "signature".to_owned(),
                json!(sign_payload(&payload, self.inner.credentials.signing_key())),
            );

            match self.request(method, signed).await {
                Err(error) if attempt == 0 && error.exchange_code() == Some("-1021") => {
                    self.synchronize_clock(true).await?;
                }
                result => return result,
            }
        }
        unreachable!("authenticated request loop always returns on its second attempt")
    }

    async fn synchronize_clock(&self, force: bool) -> ExchangeResult<()> {
        let clock = self.inner.clock.clone();
        self.inner
            .network
            .call(async move { clock.synchronize_clock(force).await })
            .await
    }

    async fn request<T>(&self, method: &'static str, parameters: Parameters) -> ExchangeResult<T>
    where
        T: DeserializeOwned,
    {
        let id = self.inner.next_request_id.fetch_add(1, Ordering::Relaxed);
        let sender = self.worker_sender().await?;
        let (reply, response) = oneshot::channel();
        sender
            .try_send(Command::Request {
                id,
                method,
                parameters,
                reply,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => ExchangeError::new(
                    ExchangeErrorKind::ServiceUnavailable,
                    "Binance WebSocket API command queue is full",
                ),
                mpsc::error::TrySendError::Closed(_) => ExchangeError::new(
                    ExchangeErrorKind::ServiceUnavailable,
                    "Binance WebSocket API worker stopped",
                ),
            })?;

        let value = match tokio::time::timeout(self.inner.response_timeout, response).await {
            Ok(Ok(result)) => result?,
            Ok(Err(_)) => {
                return Err(ExchangeError::new(
                    ExchangeErrorKind::Network,
                    "Binance WebSocket API response channel closed",
                ));
            }
            Err(_) => {
                let _ = sender.try_send(Command::Forget { id });
                return Err(ExchangeError::new(
                    ExchangeErrorKind::Timeout,
                    "Binance WebSocket API response timed out",
                ));
            }
        };
        serde_json::from_value(value)
            .map_err(|error| error::invalid_response("WebSocket API result", error))
    }

    async fn worker_sender(&self) -> ExchangeResult<mpsc::Sender<Command>> {
        let mut worker = self.inner.worker.lock().await;
        if let Some(sender) = worker.as_ref().filter(|sender| !sender.is_closed()) {
            return Ok(sender.clone());
        }

        let (sender, receiver) = mpsc::channel(COMMAND_BUFFER);
        self.inner.network.spawn(run_worker(
            self.inner.endpoint.clone(),
            self.inner.connect_timeout,
            receiver,
        ))?;
        *worker = Some(sender.clone());
        Ok(sender)
    }
}

async fn run_worker(
    endpoint: Arc<str>,
    connect_timeout: Duration,
    mut commands: mpsc::Receiver<Command>,
) {
    let mut socket = None;
    let mut pending = HashMap::new();

    loop {
        if socket.is_none() {
            let Some(command) = commands.recv().await else {
                return;
            };
            match command {
                Command::Request { .. } => match connect(&endpoint, connect_timeout).await {
                    Ok(mut connected) => {
                        if dispatch(&mut connected, &mut pending, command).await {
                            socket = Some(connected);
                        }
                    }
                    Err(error) => reply_command(command, error),
                },
                Command::Forget { id } => {
                    pending.remove(&id);
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
                if !dispatch(connected, &mut pending, command).await {
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
                if let Some(error) = handle_message(connected, &mut pending, message).await {
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
    command: Command,
) -> bool {
    match command {
        Command::Request {
            id,
            method,
            parameters,
            reply,
        } => {
            let payload = match serde_json::to_string(&json!({
                "id": id,
                "method": method,
                "params": parameters,
            })) {
                Ok(payload) => payload,
                Err(error) => {
                    let _ =
                        reply.send(Err(error::invalid_response("WebSocket API request", error)));
                    return true;
                }
            };
            if socket.send(Message::Text(payload.into())).await.is_err() {
                let _ = reply.send(Err(ExchangeError::new(
                    ExchangeErrorKind::Network,
                    "Binance WebSocket API request send failed",
                )));
                return false;
            }
            if let Some(previous) = pending.insert(id, PendingRequest { reply }) {
                let _ = previous.reply.send(Err(ExchangeError::new(
                    ExchangeErrorKind::StateConflict,
                    "duplicate Binance WebSocket API request ID",
                )));
            }
            true
        }
        Command::Forget { id } => {
            pending.remove(&id);
            true
        }
    }
}

async fn handle_message(
    socket: &mut Socket,
    pending: &mut HashMap<u64, PendingRequest>,
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
            let result = if (200..300).contains(&response.status) {
                Ok(response.result.unwrap_or(Value::Null))
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

fn reply_command(command: Command, error: ExchangeError) {
    if let Command::Request { reply, .. } = command {
        let _ = reply.send(Err(error));
    }
}

fn fail_pending(pending: &mut HashMap<u64, PendingRequest>, error: ExchangeError) {
    for (_, request) in pending.drain() {
        let _ = request.reply.send(Err(error.clone()));
    }
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

    #[test]
    fn signature_payload_is_sorted_and_uses_wire_values() {
        let parameters = Parameters::from([
            ("timestamp".to_owned(), json!(1_700_000_000_000_u64)),
            ("apiKey".to_owned(), json!("public-key")),
            ("recvWindow".to_owned(), json!(5000)),
            ("symbol".to_owned(), json!("BTCUSDT")),
        ]);

        assert_eq!(
            signature_payload(&parameters).unwrap(),
            "apiKey=public-key&recvWindow=5000&symbol=BTCUSDT&timestamp=1700000000000"
        );
    }

    #[test]
    fn signature_payload_rejects_nested_values() {
        let parameters = Parameters::from([("bad".to_owned(), json!({"nested": true}))]);

        assert_eq!(
            signature_payload(&parameters).unwrap_err().kind(),
            ExchangeErrorKind::InvalidRequest
        );
    }

    #[tokio::test]
    async fn sends_all_order_mutations_over_one_signed_websocket_connection() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_address = http_listener.local_addr().unwrap();
        let http_server = tokio::spawn(async move {
            let (mut connection, _) = http_listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0_u8; 1024];
            loop {
                let count = connection.read(&mut chunk).await.unwrap();
                assert!(count > 0);
                request.extend_from_slice(&chunk[..count]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            assert!(
                String::from_utf8(request)
                    .unwrap()
                    .starts_with("GET /fapi/v1/time HTTP/1.1\r\n")
            );
            let body = r#"{"serverTime":1700000000000}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            connection.write_all(response.as_bytes()).await.unwrap();
        });

        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_address = ws_listener.local_addr().unwrap();
        let ws_server = tokio::spawn(async move {
            let (connection, _) = ws_listener.accept().await.unwrap();
            let mut socket = accept_async(connection).await.unwrap();
            for method in [
                "order.place",
                "order.cancel",
                "order.status",
                "openOrders.cancelAll",
            ] {
                let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
                    panic!("expected a text WebSocket API request");
                };
                let request: Value = serde_json::from_str(text.as_ref()).unwrap();
                assert_eq!(request["method"], method);
                let id = request["id"].as_u64().unwrap();
                assert_valid_test_signature(&request["params"]);

                let result = match method {
                    "order.place" => json!({
                        "symbol": "BTCUSDT",
                        "clientOrderId": "1",
                        "orderId": 42
                    }),
                    "order.cancel" => order_result("CANCELED", "0.000"),
                    "order.status" => order_result("FILLED", "0.001"),
                    "openOrders.cancelAll" => json!({
                        "code": 200,
                        "msg": "The operation of cancel all open order is done."
                    }),
                    _ => unreachable!(),
                };
                socket
                    .send(Message::Text(
                        json!({"id": id, "status": 200, "result": result})
                            .to_string()
                            .into(),
                    ))
                    .await
                    .unwrap();
            }
        });

        let config = BinanceUsdmConfig::new(
            format!("http://{http_address}"),
            "ws://127.0.0.1:1",
            format!("ws://{ws_address}/ws-fapi/v1"),
            Duration::from_secs(5),
            Duration::from_secs(2),
            Duration::from_secs(60),
            Duration::from_secs(240),
        )
        .unwrap();
        let credentials = BinanceCredentials::new(
            SecretString::new("test-key".to_owned()),
            SecretString::new(crate::config::TEST_PRIVATE_KEY_PEM.to_owned()),
        )
        .unwrap();
        let rest = RestClient::new(&config, credentials.clone()).unwrap();
        let network = NetworkRuntime::new(
            crate::network::NetworkRole::Trading,
            maker_runtime::ExecutionMode::EventDriven,
            None,
        )
        .unwrap();
        let client = WsApiClient::new(&config, credentials, rest, network).unwrap();
        let symbol = Symbol::new("BTCUSDT").unwrap();
        let client_order_id = ClientOrderId::new(1).unwrap();
        let intent = OrderIntent::post_only(
            symbol,
            client_order_id,
            Side::Buy,
            PriceTicks::new(640_001).unwrap(),
            QuantityLots::new(1).unwrap(),
        );

        let ack = client.place_order(&test_spec(), &intent).await.unwrap();
        assert_eq!(ack.order_id, 42);
        let canceled = client
            .cancel_order(&symbol, &client_order_id)
            .await
            .unwrap();
        assert_eq!(canceled.status, "CANCELED");
        let queried = client.query_order(&symbol, &client_order_id).await.unwrap();
        assert_eq!(queried.status, "FILLED");
        client.cancel_all(&symbol).await.unwrap();

        http_server.await.unwrap();
        ws_server.await.unwrap();
    }

    fn order_result(status: &str, executed_quantity: &str) -> Value {
        json!({
            "symbol": "BTCUSDT",
            "clientOrderId": "1",
            "orderId": 42,
            "side": "BUY",
            "price": "64000.1",
            "origQty": "0.001",
            "executedQty": executed_quantity,
            "status": status
        })
    }

    fn assert_valid_test_signature(value: &Value) {
        let mut parameters: Parameters = value
            .as_object()
            .unwrap()
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        let signature = parameters
            .remove("signature")
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned();
        assert_eq!(parameters["apiKey"], "test-key");
        let payload = signature_payload(&parameters).unwrap();
        let credentials = BinanceCredentials::new(
            SecretString::new("test-key".to_owned()),
            SecretString::new(crate::config::TEST_PRIVATE_KEY_PEM.to_owned()),
        )
        .unwrap();
        assert_eq!(signature, sign_payload(&payload, credentials.signing_key()));
    }
}
