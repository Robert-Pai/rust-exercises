use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use maker_domain::{BestBidAsk, InstrumentSpec, Symbol};
use maker_ports::{
    EventStream, ExchangeError, ExchangeErrorKind, ExchangeResult, LatestBboPublisher,
    LatestBboSubscription, OrderUpdatePublisher, OrderUpdateSubscription, PrivateEvent,
    ReceivedPrivateEvent,
};
use maker_runtime::{EventSource, RuntimeTelemetry, unix_time_us};
use tokio::sync::oneshot;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::{
    error, mapping,
    models::{BookTickerEventDto, PrivateEventDto},
    rest::RestClient,
};

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

const EVENT_BUFFER: usize = 256;
const PRIVATE_EVENTS: &str = "ORDER_TRADE_UPDATE/ACCOUNT_UPDATE/TRADE_LITE/listenKeyExpired";
const PRIVATE_STREAM_SETTLE_DELAY: Duration = Duration::from_millis(100);

pub(crate) async fn subscribe_book_ticker(
    websocket_url: &str,
    symbol: Symbol,
    spec: InstrumentSpec,
    connect_timeout: Duration,
    idle_timeout: Duration,
    initial: BestBidAsk,
    telemetry: Option<RuntimeTelemetry>,
) -> ExchangeResult<LatestBboSubscription> {
    let stream_name = format!("{}@bookTicker", symbol.as_str().to_ascii_lowercase());
    let endpoint = stream_endpoint(websocket_url, &stream_name);
    let (publisher, subscription) = LatestBboSubscription::channel(initial);
    let socket = connect(&endpoint, connect_timeout).await?;
    let (ready_sender, ready_receiver) = oneshot::channel();
    tokio::spawn(run_book_ticker(
        socket,
        publisher,
        symbol,
        spec,
        idle_timeout,
        telemetry,
        ready_sender,
    ));
    wait_for_reader_ready(
        ready_receiver,
        connect_timeout,
        "Binance bookTicker WebSocket reader",
    )
    .await?;
    Ok(subscription)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn subscribe_order_updates(
    rest: RestClient,
    target_symbol: Symbol,
    spec: InstrumentSpec,
    websocket_url: &str,
    connect_timeout: Duration,
    idle_timeout: Duration,
    keepalive_interval: Duration,
    telemetry: Option<RuntimeTelemetry>,
) -> ExchangeResult<EventStream<ReceivedPrivateEvent>> {
    let listen_key = rest.create_listen_key().await?;
    let endpoint = private_stream_endpoint(websocket_url, &listen_key);
    let socket = match connect(&endpoint, connect_timeout).await {
        Ok(socket) => socket,
        Err(error) => {
            close_listen_key_with_log(&rest, &target_symbol, &listen_key, "connect failure").await;
            return Err(error);
        }
    };
    tracing::info!(
        symbol = %target_symbol,
        listen_key_fingerprint = %listen_key_fingerprint(&listen_key),
        endpoint = %private_stream_endpoint(websocket_url, "<redacted>"),
        "Binance private user-data WebSocket connected"
    );
    let (sender, receiver) = OrderUpdateSubscription::channel(EVENT_BUFFER);
    let (ready_sender, ready_receiver) = oneshot::channel();
    tokio::spawn(run_order_updates(
        socket,
        sender,
        spec,
        rest,
        target_symbol,
        listen_key,
        idle_timeout,
        keepalive_interval,
        telemetry,
        ready_sender,
    ));
    wait_for_reader_ready(
        ready_receiver,
        connect_timeout,
        "Binance private user-data WebSocket reader",
    )
    .await?;
    // This URL-bound stream has no application-level subscription
    // acknowledgement. Give Binance's listen-key routing a short one-time
    // settling window before bootstrap can place its first order.
    tokio::time::sleep(PRIVATE_STREAM_SETTLE_DELAY).await;
    Ok(Box::pin(receiver) as EventStream<ReceivedPrivateEvent>)
}

async fn run_book_ticker(
    mut socket: Socket,
    mut publisher: LatestBboPublisher,
    symbol: Symbol,
    spec: InstrumentSpec,
    idle_timeout: Duration,
    telemetry: Option<RuntimeTelemetry>,
    ready: oneshot::Sender<()>,
) {
    // There is no application-level subscription acknowledgement for a
    // URL-bound stream. Signal readiness only from the spawned reader task;
    // after this synchronous send it immediately polls socket.next() below.
    let _ = ready.send(());
    let terminal_error = loop {
        let message = tokio::select! {
            _ = publisher.closed() => {
                let _ = socket.close(None).await;
                return;
            }
            message = socket.next() => message,
            _ = tokio::time::sleep(idle_timeout) => {
                break ExchangeError::new(
                    ExchangeErrorKind::Timeout,
                    "Binance bookTicker WebSocket became idle",
                );
            }
        };
        let Some(message) = message else {
            break ExchangeError::new(
                ExchangeErrorKind::Network,
                "Binance bookTicker WebSocket ended",
            );
        };
        match message {
            Ok(Message::Text(text)) => {
                let received_ns = telemetry
                    .as_ref()
                    .map_or(0, RuntimeTelemetry::monotonic_time_ns);
                let received_unix_us = unix_time_us();
                let event: BookTickerEventDto = match serde_json::from_str(text.as_ref()) {
                    Ok(event) => event,
                    Err(error) => break error::invalid_response("bookTicker event", error),
                };
                if let Some(telemetry) = &telemetry {
                    telemetry.observe_exchange_event(
                        EventSource::MarketData,
                        received_ns,
                        received_unix_us,
                        event.event_time,
                        event.transaction_time,
                    );
                }
                let update = match mapping::websocket_book(&symbol, &spec, event) {
                    Ok(update) => update,
                    Err(error) => break error,
                };
                if let Err(error) = publisher.publish_received(update, received_ns) {
                    break error;
                }
            }
            Ok(Message::Ping(payload)) => {
                if let Err(error) = socket.send(Message::Pong(payload)).await {
                    break error::websocket(error);
                }
            }
            Ok(Message::Close(_)) => {
                break ExchangeError::new(
                    ExchangeErrorKind::Network,
                    "Binance bookTicker WebSocket closed",
                );
            }
            Ok(Message::Binary(_) | Message::Pong(_) | Message::Frame(_)) => {}
            Err(error) => break error::websocket(error),
        }
    };
    publisher.fail(terminal_error);
}

#[allow(clippy::too_many_arguments)]
async fn run_order_updates(
    mut socket: Socket,
    mut sender: OrderUpdatePublisher,
    spec: InstrumentSpec,
    rest: RestClient,
    target_symbol: Symbol,
    listen_key: String,
    idle_timeout: Duration,
    keepalive_interval: Duration,
    telemetry: Option<RuntimeTelemetry>,
    ready: oneshot::Sender<()>,
) {
    let mut keepalive = tokio::time::interval(keepalive_interval);
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    keepalive.tick().await;
    let idle = tokio::time::sleep(idle_timeout);
    tokio::pin!(idle);
    let mut received_text_frame = false;

    // The WebSocket upgrade has completed before this task is spawned. This
    // barrier additionally proves that the private reader itself is running
    // before bootstrap can continue toward its first order placement.
    let _ = ready.send(());

    let terminal_error = loop {
        tokio::select! {
            _ = sender.closed() => {
                let _ = socket.close(None).await;
                close_listen_key_with_log(
                    &rest,
                    &target_symbol,
                    &listen_key,
                    "subscription closed",
                ).await;
                return;
            }
            message = socket.next() => {
                idle.as_mut().reset(tokio::time::Instant::now() + idle_timeout);
                let Some(message) = message else {
                    break ExchangeError::new(
                        ExchangeErrorKind::Network,
                        "Binance user-data WebSocket ended",
                    );
                };
                match message {
                    Ok(Message::Text(text)) => {
                        if !received_text_frame {
                            received_text_frame = true;
                            tracing::info!(
                                symbol = %target_symbol,
                                "first Binance private user-data text frame received"
                            );
                        }
                        let received_ns = telemetry
                            .as_ref()
                            .map_or(0, RuntimeTelemetry::monotonic_time_ns);
                        let received_unix_us = unix_time_us();
                        let event: PrivateEventDto = match serde_json::from_str(text.as_ref()) {
                            Ok(event) => event,
                            Err(error) => {
                                tracing::error!(
                                    symbol = %target_symbol,
                                    error = %error,
                                    raw_notification = %text,
                                    "failed to parse Binance private user-data notification"
                                );
                                break error::invalid_response("user-data event", error);
                            }
                        };
                        match event {
                            PrivateEventDto::AccountUpdate {
                                event_time,
                                transaction_time,
                                account,
                            } => {
                                observe_private_event(
                                    telemetry.as_ref(),
                                    received_ns,
                                    received_unix_us,
                                    event_time,
                                    transaction_time,
                                );
                                let update = match mapping::websocket_account_update(
                                    event_time,
                                    transaction_time,
                                    account,
                                ) {
                                    Ok(update) => update,
                                    Err(error) => break error,
                                };
                                if let Err(error) = sender.publish_event(
                                    PrivateEvent::AccountUpdate(update),
                                    received_ns,
                                ) {
                                    if sender.is_closed() {
                                        return;
                                    }
                                    break error;
                                }
                            }
                            PrivateEventDto::OrderTradeUpdate {
                                event_time,
                                transaction_time,
                                order,
                            } => {
                                observe_private_event(
                                    telemetry.as_ref(),
                                    received_ns,
                                    received_unix_us,
                                    event_time,
                                    transaction_time,
                                );
                                // Binance user-data streams are account-wide. Ignore
                                // other symbols before consulting the single-symbol
                                // instrument cache used by this adapter.
                                if !is_owned_target_order(&target_symbol, &order) {
                                    tracing::warn!(
                                        target_symbol = %target_symbol,
                                        event_symbol = %order.symbol,
                                        order_type = %order.order_type,
                                        client_order_id = %order.client_order_id,
                                        symbol_matches = order.symbol == target_symbol.as_str(),
                                        numeric_client_order_id = order
                                            .client_order_id
                                            .parse::<u64>()
                                            .is_ok(),
                                        "ignored Binance private order update"
                                    );
                                    continue;
                                }
                                let update = match mapping::websocket_order(&spec, &order) {
                                    Ok(Some(update)) => update,
                                    Ok(None) => continue,
                                    Err(error) => break error,
                                };
                                let trade = match mapping::websocket_order_trade(
                                    update,
                                    event_time,
                                    transaction_time,
                                    &order,
                                ) {
                                    Ok(trade) => trade,
                                    Err(error) => break error,
                                };
                                if let Err(error) = sender.publish_event(
                                    PrivateEvent::OrderUpdate { update, trade },
                                    received_ns,
                                ) {
                                    if sender.is_closed() {
                                        return;
                                    }
                                    break error;
                                }
                            }
                            PrivateEventDto::TradeLite {
                                event_time,
                                transaction_time,
                                trade,
                            } => {
                                observe_private_event(
                                    telemetry.as_ref(),
                                    received_ns,
                                    received_unix_us,
                                    event_time,
                                    transaction_time,
                                );
                                if !is_owned_target_trade_lite(&target_symbol, &trade) {
                                    tracing::warn!(
                                        target_symbol = %target_symbol,
                                        event_symbol = %trade.symbol,
                                        client_order_id = %trade.client_order_id,
                                        symbol_matches = trade.symbol == target_symbol.as_str(),
                                        numeric_client_order_id = trade
                                            .client_order_id
                                            .parse::<u64>()
                                            .is_ok(),
                                        "ignored Binance private trade-lite update"
                                    );
                                    continue;
                                }
                                let trade = match mapping::websocket_trade_lite(
                                    event_time,
                                    transaction_time,
                                    &trade,
                                ) {
                                    Ok(trade) => trade,
                                    Err(error) => break error,
                                };
                                if let Err(error) = sender.publish_event(
                                    PrivateEvent::TradeLite(trade),
                                    received_ns,
                                ) {
                                    if sender.is_closed() {
                                        return;
                                    }
                                    break error;
                                }
                            }
                            PrivateEventDto::ListenKeyExpired => {
                                break ExchangeError::new(
                                    ExchangeErrorKind::StateConflict,
                                    "Binance listen key expired",
                                );
                            }
                            PrivateEventDto::Other => {
                                tracing::warn!(
                                    symbol = %target_symbol,
                                    raw_notification = %text,
                                    "unsupported Binance private user-data notification"
                                );
                            }
                        }
                    }
                    Ok(Message::Ping(payload)) => {
                        if let Err(error) = socket.send(Message::Pong(payload)).await {
                            break error::websocket(error);
                        }
                    }
                    Ok(Message::Close(frame)) => {
                        tracing::warn!(
                            symbol = %target_symbol,
                            close_frame = ?frame,
                            "Binance private user-data WebSocket close frame received"
                        );
                        break ExchangeError::new(
                            ExchangeErrorKind::Network,
                            "Binance user-data WebSocket closed",
                        );
                    }
                    Ok(Message::Binary(payload)) => {
                        tracing::warn!(
                            symbol = %target_symbol,
                            payload_bytes = payload.len(),
                            "Binance private user-data binary frame received"
                        );
                    }
                    Ok(Message::Pong(_) | Message::Frame(_)) => {}
                    Err(error) => break error::websocket(error),
                }
            }
            _ = keepalive.tick() => {
                if let Err(error) = rest.keepalive_listen_key(&listen_key).await {
                    break error;
                }
            }
            _ = &mut idle => {
                break ExchangeError::new(
                    ExchangeErrorKind::Timeout,
                    "Binance user-data WebSocket became idle",
                );
            }
        }
    };
    close_listen_key_with_log(&rest, &target_symbol, &listen_key, "private reader stopped").await;
    sender.fail(terminal_error);
}

async fn close_listen_key_with_log(
    rest: &RestClient,
    symbol: &Symbol,
    listen_key: &str,
    reason: &'static str,
) {
    let fingerprint = listen_key_fingerprint(listen_key);
    match rest.close_listen_key(listen_key).await {
        Ok(()) => tracing::info!(
            %symbol,
            reason,
            listen_key_fingerprint = %fingerprint,
            "Binance listen key closed"
        ),
        Err(error) => tracing::error!(
            %symbol,
            reason,
            listen_key_fingerprint = %fingerprint,
            %error,
            "failed to close Binance listen key"
        ),
    }
}

fn listen_key_fingerprint(listen_key: &str) -> String {
    let mut hasher = DefaultHasher::new();
    listen_key.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

async fn connect(endpoint: &str, timeout: Duration) -> ExchangeResult<Socket> {
    let connection = tokio::time::timeout(timeout, connect_async(endpoint))
        .await
        .map_err(|_| {
            ExchangeError::new(
                ExchangeErrorKind::Timeout,
                "Binance WebSocket connection timed out",
            )
        })?
        .map_err(error::websocket)?;
    Ok(connection.0)
}

async fn wait_for_reader_ready(
    ready: oneshot::Receiver<()>,
    timeout: Duration,
    stream: &'static str,
) -> ExchangeResult<()> {
    tokio::time::timeout(timeout, ready)
        .await
        .map_err(|_| {
            ExchangeError::new(
                ExchangeErrorKind::Timeout,
                format!("{stream} readiness timed out"),
            )
        })?
        .map_err(|_| {
            ExchangeError::new(
                ExchangeErrorKind::ServiceUnavailable,
                format!("{stream} stopped before becoming ready"),
            )
        })
}

fn stream_endpoint(base: &str, stream_name: &str) -> String {
    let base = base.trim_end_matches('/');
    if base.ends_with("/ws") {
        format!("{base}/{stream_name}")
    } else {
        format!("{base}/ws/{stream_name}")
    }
}

fn private_stream_endpoint(base: &str, listen_key: &str) -> String {
    let base = base.trim_end_matches('/');
    let base = base.strip_suffix("/ws").unwrap_or(base);
    if base.ends_with("/private") {
        format!("{base}/ws?listenKey={listen_key}&events={PRIVATE_EVENTS}")
    } else {
        format!("{base}/private/ws?listenKey={listen_key}&events={PRIVATE_EVENTS}")
    }
}

fn is_owned_target_order(
    target_symbol: &Symbol,
    order: &crate::models::OrderTradeEventDto<'_>,
) -> bool {
    order.symbol == target_symbol.as_str() && order.client_order_id.parse::<u64>().is_ok()
}

fn is_owned_target_trade_lite(
    target_symbol: &Symbol,
    trade: &crate::models::TradeLiteEventDto<'_>,
) -> bool {
    trade.symbol == target_symbol.as_str() && trade.client_order_id.parse::<u64>().is_ok()
}

fn observe_private_event(
    telemetry: Option<&RuntimeTelemetry>,
    received_ns: u64,
    received_unix_us: i64,
    event_time: u64,
    transaction_time: u64,
) {
    if let Some(telemetry) = telemetry {
        telemetry.observe_exchange_event(
            EventSource::PrivateData,
            received_ns,
            received_unix_us,
            event_time,
            transaction_time,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::OrderTradeEventDto;

    fn order<'a>(symbol: &'a str, order_type: &'a str) -> OrderTradeEventDto<'a> {
        OrderTradeEventDto {
            symbol,
            client_order_id: "1",
            order_id: 1,
            side: "BUY",
            order_type,
            price: "1",
            original_quantity: "1",
            average_price: "0",
            execution_type: "NEW",
            cumulative_filled: "0",
            last_filled_price: "0",
            last_filled_quantity: "0",
            commission_asset: None,
            commission: None,
            trade_time: 1,
            trade_id: 0,
            maker: false,
            realized_pnl: "0",
            status: "NEW",
        }
    }

    #[test]
    fn builds_market_stream_urls_with_or_without_ws_suffix() {
        assert_eq!(
            stream_endpoint("wss://fstream.binance.com", "btcusdt@bookTicker"),
            "wss://fstream.binance.com/ws/btcusdt@bookTicker"
        );
        assert_eq!(
            stream_endpoint("wss://example.test/ws/", "listen-key"),
            "wss://example.test/ws/listen-key"
        );
    }

    #[test]
    fn builds_private_stream_url_with_private_path() {
        for base in [
            "wss://fstream.binance.com",
            "wss://fstream.binance.com/",
            "wss://fstream.binance.com/ws",
            "wss://fstream.binance.com/private",
            "wss://fstream.binance.com/private/ws/",
        ] {
            assert_eq!(
                private_stream_endpoint(base, "listen-key"),
                concat!(
                    "wss://fstream.binance.com/private/ws?listenKey=listen-key",
                    "&events=ORDER_TRADE_UPDATE/ACCOUNT_UPDATE/TRADE_LITE/listenKeyExpired"
                )
            );
        }
    }

    #[test]
    fn filters_account_order_events_before_instrument_lookup() {
        let target = Symbol::new("BTCUSDT").unwrap();

        assert!(!is_owned_target_order(&target, &order("ETHUSDT", "LIMIT")));
        assert!(is_owned_target_order(&target, &order("BTCUSDT", "MARKET")));
        assert!(is_owned_target_order(&target, &order("BTCUSDT", "LIMIT")));
        let mut external = order("BTCUSDT", "LIMIT");
        external.client_order_id = "manual-order";
        assert!(!is_owned_target_order(&target, &external));
    }
}
