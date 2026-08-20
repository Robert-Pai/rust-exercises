use std::{collections::BTreeMap, time::Duration};

use futures_util::{SinkExt, StreamExt};
use maker_domain::{ClientOrderId, InstrumentSpec, OrderIntent, Side, Symbol};
use maker_ports::{ExchangeError, ExchangeErrorKind, ExchangeFuture, ExchangeResult};
use maker_runtime::{
    EventOrigin, RuntimeTelemetry, SpscConsumer, SpscProducer, TryPushError, current_event_origin,
    spsc_channel,
};
use secrecy::ExposeSecret;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::sync::oneshot;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::{
    config::{BinanceCredentials, BinanceUsdmConfig},
    error,
    models::{
        ApiErrorDto, RateLimitDto, WsApiAccountBalanceDto, WsApiAccountStatusDto, WsApiOrderAckDto,
        WsApiOrderDto,
    },
    network::NetworkRuntime,
    rate_limit::{
        AcquireDecision, RateLimitSnapshot, RequestCost, RequestRateLimiter,
        SharedRequestRateLimiter,
    },
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
    queue_timeout: Duration,
    telemetry: Option<RuntimeTelemetry>,
}

struct WorkerConfig {
    endpoint: String,
    credentials: BinanceCredentials,
    clock: RestClient,
    recv_window_ms: u64,
    connect_timeout: Duration,
    response_timeout: Duration,
    request_rate_limiter: SharedRequestRateLimiter,
    telemetry: Option<RuntimeTelemetry>,
}

struct WorkerState {
    clock_offset_ms: Option<i64>,
}

struct Command {
    method: &'static str,
    parameters: Parameters,
    mode: ResponseMode,
    clock_retried: bool,
    submitted_ns: u64,
    dequeued_ns: u64,
    origin: Option<EventOrigin>,
    measure_dispatch: bool,
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
    submitted_ns: u64,
    origin: Option<EventOrigin>,
    reply: oneshot::Sender<ExchangeResult<Value>>,
}

type PendingSlots = [Option<PendingRequest>; TRANSPORT_ID_SLOTS];

struct DeferredCommand {
    command: Command,
    retry_at: tokio::time::Instant,
}

type DeferredSlots = [Option<DeferredCommand>; TRANSPORT_ID_SLOTS];

enum DispatchOutcome {
    Continue,
    Deferred(DeferredCommand),
    Reset(ExchangeError),
}

#[derive(Debug, Deserialize)]
struct WsApiResponse {
    id: WsApiResponseId,
    status: u16,
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<ApiErrorDto>,
    #[serde(default, rename = "rateLimits")]
    rate_limits: Vec<RateLimitDto>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum WsApiResponseId {
    Integer(i64),
    String(String),
    Null(()),
}

impl WsApiResponseId {
    fn transport_id(&self) -> Option<u64> {
        match self {
            Self::Integer(id) => u64::try_from(*id).ok(),
            Self::String(id) => id.parse().ok(),
            Self::Null(()) => None,
        }
    }
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
        let queue_timeout = queue_timeout(config.request_timeout());
        let request_rate_limiter = clock.request_rate_limiter();
        network.spawn(run_worker(
            WorkerConfig {
                endpoint: config.websocket_api_url().to_owned(),
                credentials,
                clock,
                recv_window_ms,
                connect_timeout: config.request_timeout(),
                response_timeout: config.request_timeout(),
                request_rate_limiter,
                telemetry: config.telemetry(),
            },
            receiver,
        ))?;
        Ok(Self {
            commands,
            queue_timeout,
            telemetry: config.telemetry(),
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

    pub(crate) fn account_balance_v2(&mut self) -> ExchangeFuture<Vec<WsApiAccountBalanceDto>> {
        self.request("v2/account.balance", Parameters::new())
    }

    pub(crate) fn account_status_v2(&mut self) -> ExchangeFuture<WsApiAccountStatusDto> {
        self.request("v2/account.status", Parameters::new())
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
        let submitted_ns = self
            .telemetry
            .as_ref()
            .map_or(0, RuntimeTelemetry::monotonic_time_ns);
        let origin = current_event_origin();
        let command = Command {
            method,
            parameters,
            mode,
            clock_retried: false,
            submitted_ns,
            dequeued_ns: 0,
            origin,
            measure_dispatch: true,
            deadline: tokio::time::Instant::now() + self.queue_timeout,
            reply,
        };
        if let Err(error) = self.commands.try_push(command) {
            return ready_error(command_enqueue_error(error));
        }
        if let Some(telemetry) = &self.telemetry {
            telemetry.observe_request_submitted(origin, submitted_ns);
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
    let mut deferred: DeferredSlots = std::array::from_fn(|_| None);
    let mut next_request_id = 1_u64;
    let mut state = WorkerState {
        clock_offset_ms: None,
    };

    loop {
        if socket.is_none() {
            let Some(mut command) = commands.recv().await else {
                return;
            };
            command.dequeued_ns = config
                .telemetry
                .as_ref()
                .map_or(0, RuntimeTelemetry::monotonic_time_ns);
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
                        &config.request_rate_limiter,
                        command,
                    )
                    .await
                    {
                        DispatchOutcome::Continue => socket = Some(connected),
                        DispatchOutcome::Deferred(command) => {
                            if let Some(error) = defer_command(&mut deferred, command) {
                                fail_worker(&mut pending, &mut deferred, error);
                            } else {
                                socket = Some(connected);
                            }
                        }
                        DispatchOutcome::Reset(error) => {
                            fail_worker(&mut pending, &mut deferred, error);
                        }
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
        let deadline = next_worker_deadline(&pending, &deferred);
        let retry_at = next_deferred_retry(&deferred);
        tokio::select! {
            command = commands.recv(), if active_count(&pending, &deferred) < TRANSPORT_ID_SLOTS => {
                let Some(mut command) = command else {
                    fail_worker(
                        &mut pending,
                        &mut deferred,
                        ExchangeError::new(
                            ExchangeErrorKind::ServiceUnavailable,
                            "Binance WebSocket API worker stopped",
                        ),
                    );
                    return;
                };
                command.dequeued_ns = config
                    .telemetry
                    .as_ref()
                    .map_or(0, RuntimeTelemetry::monotonic_time_ns);
                if command_is_inactive(&command) {
                    finish_inactive(command);
                    continue;
                }
                let outcome = dispatch(
                    connected,
                    &mut pending,
                    &mut next_request_id,
                    &config,
                    &mut state,
                    &config.request_rate_limiter,
                    command,
                ).await;
                if let Some(error) = apply_dispatch_outcome(&mut deferred, outcome) {
                    fail_worker(&mut pending, &mut deferred, error);
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
                    &mut deferred,
                    message,
                ).await {
                    fail_worker(&mut pending, &mut deferred, error);
                    socket = None;
                }
            }
            () = wait_for_deadline(retry_at) => {
                let now = tokio::time::Instant::now();
                let Some(command) = take_due_deferred(&mut deferred, now) else {
                    continue;
                };
                if command_is_inactive(&command) {
                    finish_inactive(command);
                    continue;
                }
                let outcome = dispatch(
                    connected,
                    &mut pending,
                    &mut next_request_id,
                    &config,
                    &mut state,
                    &config.request_rate_limiter,
                    command,
                ).await;
                if let Some(error) = apply_dispatch_outcome(&mut deferred, outcome) {
                    fail_worker(&mut pending, &mut deferred, error);
                    socket = None;
                }
            }
            () = wait_for_deadline(deadline) => {
                let now = tokio::time::Instant::now();
                expire_pending(&mut pending, now);
                expire_deferred(&mut deferred, now);
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

fn queue_timeout(response_timeout: Duration) -> Duration {
    let queue_budget = Duration::from_secs(120);
    response_timeout
        .checked_add(queue_budget)
        .unwrap_or(Duration::MAX)
}

fn request_cost(method: &str) -> RequestCost {
    match method {
        "order.place" | "order.cancel" => RequestCost::ORDER,
        "v2/account.balance" | "v2/account.status" => RequestCost::ACCOUNT_QUERY,
        _ => RequestCost::GENERIC,
    }
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
    request_rate_limiter: &SharedRequestRateLimiter,
    command: Command,
) -> DispatchOutcome {
    let queue_deadline = command.deadline;
    if command.reply.is_closed() {
        return DispatchOutcome::Continue;
    }
    if remaining(queue_deadline).is_none() {
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
    if remaining(queue_deadline).is_none() {
        let _ = command.reply.send(Err(response_timeout_error()));
        return DispatchOutcome::Continue;
    }

    match RequestRateLimiter::acquire(
        request_rate_limiter,
        request_cost(command.method),
        queue_deadline,
    ) {
        Ok(AcquireDecision::Ready) => {}
        Ok(AcquireDecision::RetryAt(retry_at)) => {
            return DispatchOutcome::Deferred(DeferredCommand { command, retry_at });
        }
        Ok(AcquireDecision::DeadlineExceeded) => {
            let _ = command.reply.send(Err(response_timeout_error()));
            return DispatchOutcome::Continue;
        }
        Err(error) => {
            let _ = command.reply.send(Err(error));
            return DispatchOutcome::Continue;
        }
    }

    // Initial commands use a queue-inclusive deadline. Retries carry the
    // original in-flight request deadline, so taking the earlier instant
    // preserves the existing retry timeout semantics.
    let deadline = std::cmp::min(
        queue_deadline,
        tokio::time::Instant::now() + config.response_timeout,
    );
    let prepare_started_ns = config
        .telemetry
        .as_ref()
        .map_or(0, RuntimeTelemetry::monotonic_time_ns);

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
    let submitted_ns = command.submitted_ns;
    let dequeued_ns = command.dequeued_ns;
    let origin = command.origin;
    let measure_dispatch = command.measure_dispatch;
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
        "id": id.to_string(),
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
        submitted_ns,
        origin,
        reply: command.reply,
    });
    let Some(send_timeout) = remaining(deadline) else {
        return DispatchOutcome::Reset(response_timeout_error());
    };
    let send_started_ns = config
        .telemetry
        .as_ref()
        .map_or(0, RuntimeTelemetry::monotonic_time_ns);
    match tokio::time::timeout(send_timeout, socket.send(Message::Text(payload.into()))).await {
        Ok(Ok(())) => {
            if measure_dispatch {
                if let Some(telemetry) = &config.telemetry {
                    telemetry.observe_request_sent(
                        origin,
                        submitted_ns,
                        dequeued_ns,
                        prepare_started_ns,
                        send_started_ns,
                        telemetry.monotonic_time_ns(),
                    );
                }
            }
            DispatchOutcome::Continue
        }
        Ok(Err(_)) => {
            if let Some(telemetry) = &config.telemetry {
                telemetry.observe_request_send_failure();
            }
            DispatchOutcome::Reset(ExchangeError::new(
                ExchangeErrorKind::Network,
                "Binance WebSocket API request send failed",
            ))
        }
        Err(_) => {
            if let Some(telemetry) = &config.telemetry {
                telemetry.observe_request_send_failure();
            }
            DispatchOutcome::Reset(response_timeout_error())
        }
    }
}

async fn handle_message(
    socket: &mut Socket,
    pending: &mut PendingSlots,
    next_request_id: &mut u64,
    config: &WorkerConfig,
    state: &mut WorkerState,
    deferred: &mut DeferredSlots,
    message: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
) -> Option<ExchangeError> {
    match message {
        Some(Ok(Message::Text(text))) => {
            let response: WsApiResponse = match serde_json::from_str(text.as_ref()) {
                Ok(response) => response,
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        raw_response = %text,
                        "failed to parse Binance WebSocket API response"
                    );
                    return None;
                }
            };
            let snapshots = response
                .rate_limits
                .iter()
                .filter_map(|limit| {
                    RateLimitSnapshot::from_wire(
                        &limit.rate_limit_type,
                        &limit.interval,
                        limit.interval_num,
                        limit.non_negative_limit(),
                        limit.non_negative_count(),
                    )
                })
                .collect::<Vec<_>>();
            if snapshots.len() != response.rate_limits.len() {
                return Some(RequestRateLimiter::reject_unrecognized_telemetry(
                    &config.request_rate_limiter,
                ));
            }
            if let Err(error) =
                RequestRateLimiter::update_counts(&config.request_rate_limiter, &snapshots)
            {
                return Some(error);
            }
            let Some(response_id) = response.id.transport_id() else {
                tracing::warn!(
                    response_id = ?response.id,
                    status = response.status,
                    "Binance WebSocket API response has no matchable request ID"
                );
                return None;
            };
            let Some(request) = take_pending(pending, response_id) else {
                tracing::warn!(
                    response_id,
                    status = response.status,
                    "Binance WebSocket API response does not match a pending request"
                );
                return None;
            };
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
                    submitted_ns: request.submitted_ns,
                    dequeued_ns: 0,
                    origin: request.origin,
                    measure_dispatch: false,
                    deadline: request.deadline,
                    reply: request.reply,
                };
                let outcome = dispatch(
                    socket,
                    pending,
                    next_request_id,
                    config,
                    state,
                    &config.request_rate_limiter,
                    command,
                )
                .await;
                if let Some(error) = apply_dispatch_outcome(deferred, outcome) {
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
                    submitted_ns: request.submitted_ns,
                    dequeued_ns: 0,
                    origin: request.origin,
                    measure_dispatch: false,
                    deadline: request.deadline,
                    reply: request.reply,
                };
                let outcome = dispatch(
                    socket,
                    pending,
                    next_request_id,
                    config,
                    state,
                    &config.request_rate_limiter,
                    command,
                )
                .await;
                if let Some(error) = apply_dispatch_outcome(deferred, outcome) {
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
            if let Err(error) = RequestRateLimiter::observe_error(
                &config.request_rate_limiter,
                result.as_ref().err(),
            ) {
                let _ = request.reply.send(Err(error.clone()));
                return Some(error);
            }
            let _ = request.reply.send(result);
            None
        }
        Some(Ok(Message::Ping(payload))) => {
            let write_timeout = write_timeout(
                next_worker_deadline(pending, deferred),
                config.response_timeout,
            );
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

fn deferred_count(deferred: &DeferredSlots) -> usize {
    deferred.iter().filter(|slot| slot.is_some()).count()
}

fn active_count(pending: &PendingSlots, deferred: &DeferredSlots) -> usize {
    pending_count(pending) + deferred_count(deferred)
}

fn apply_dispatch_outcome(
    deferred: &mut DeferredSlots,
    outcome: DispatchOutcome,
) -> Option<ExchangeError> {
    match outcome {
        DispatchOutcome::Continue => None,
        DispatchOutcome::Deferred(command) => defer_command(deferred, command),
        DispatchOutcome::Reset(error) => Some(error),
    }
}

fn defer_command(deferred: &mut DeferredSlots, command: DeferredCommand) -> Option<ExchangeError> {
    if let Some(slot) = deferred.iter_mut().find(|slot| slot.is_none()) {
        *slot = Some(command);
        return None;
    }

    let error = ExchangeError::new(
        ExchangeErrorKind::StateConflict,
        "Binance WebSocket API deferred-command capacity was exhausted",
    );
    let _ = command.command.reply.send(Err(error.clone()));
    Some(error)
}

fn next_deferred_retry(deferred: &DeferredSlots) -> Option<tokio::time::Instant> {
    deferred
        .iter()
        .filter_map(|slot| slot.as_ref().map(|command| command.retry_at))
        .min()
}

fn take_due_deferred(deferred: &mut DeferredSlots, now: tokio::time::Instant) -> Option<Command> {
    let index = deferred
        .iter()
        .enumerate()
        .filter_map(|(index, slot)| {
            slot.as_ref()
                .filter(|command| command.retry_at <= now)
                .map(|command| (index, command.retry_at))
        })
        .min_by_key(|(_, retry_at)| *retry_at)
        .map(|(index, _)| index)?;
    deferred[index].take().map(|command| command.command)
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

fn next_worker_deadline(
    pending: &PendingSlots,
    deferred: &DeferredSlots,
) -> Option<tokio::time::Instant> {
    let pending = next_pending_deadline(pending);
    let deferred = deferred
        .iter()
        .filter_map(|slot| slot.as_ref().map(|command| command.command.deadline))
        .min();
    match (pending, deferred) {
        (Some(pending), Some(deferred)) => Some(std::cmp::min(pending, deferred)),
        (pending, deferred) => pending.or(deferred),
    }
}

fn expire_pending(pending: &mut PendingSlots, now: tokio::time::Instant) {
    for slot in pending.iter_mut() {
        if slot.as_ref().is_some_and(|request| request.deadline <= now) {
            let request = slot.take().expect("expired request slot is occupied");
            let _ = request.reply.send(Err(response_timeout_error()));
        }
    }
}

fn expire_deferred(deferred: &mut DeferredSlots, now: tokio::time::Instant) {
    for slot in deferred.iter_mut() {
        if slot
            .as_ref()
            .is_some_and(|command| command.command.deadline <= now)
        {
            let command = slot.take().expect("expired deferred slot is occupied");
            let _ = command.command.reply.send(Err(response_timeout_error()));
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

fn fail_deferred(deferred: &mut DeferredSlots, error: ExchangeError) {
    for slot in deferred.iter_mut() {
        if let Some(command) = slot.take() {
            let _ = command.command.reply.send(Err(error.clone()));
        }
    }
}

fn fail_worker(pending: &mut PendingSlots, deferred: &mut DeferredSlots, error: ExchangeError) {
    fail_pending(pending, error.clone());
    fail_deferred(deferred, error);
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
