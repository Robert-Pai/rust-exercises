use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use maker_domain::{BestBidAsk, InstrumentSpec, OrderUpdate, Symbol};
use maker_ports::{
    EventStream, ExchangeError, ExchangeErrorKind, ExchangeResult, LatestBboPublisher,
    LatestBboSubscription, OrderUpdatePublisher, OrderUpdateSubscription,
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::{
    error, mapping,
    models::{BookTickerEventDto, PrivateEventDto},
    rest::RestClient,
};

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

const EVENT_BUFFER: usize = 256;

pub(crate) async fn subscribe_book_ticker(
    websocket_url: &str,
    symbol: Symbol,
    spec: InstrumentSpec,
    connect_timeout: Duration,
    idle_timeout: Duration,
    initial: BestBidAsk,
) -> ExchangeResult<LatestBboSubscription> {
    let stream_name = format!("{}@bookTicker", symbol.as_str().to_ascii_lowercase());
    let endpoint = stream_endpoint(websocket_url, &stream_name);
    let (publisher, subscription) = LatestBboSubscription::channel(initial);
    let socket = connect(&endpoint, connect_timeout).await?;
    tokio::spawn(run_book_ticker(
        socket,
        publisher,
        symbol,
        spec,
        idle_timeout,
    ));
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
) -> ExchangeResult<EventStream<OrderUpdate>> {
    let listen_key = rest.create_listen_key().await?;
    let endpoint = stream_endpoint(websocket_url, &listen_key);
    let socket = connect(&endpoint, connect_timeout).await?;
    let (sender, receiver) = OrderUpdateSubscription::channel(EVENT_BUFFER);
    tokio::spawn(run_order_updates(
        socket,
        sender,
        spec,
        rest,
        target_symbol,
        listen_key,
        idle_timeout,
        keepalive_interval,
    ));
    Ok(Box::pin(receiver) as EventStream<OrderUpdate>)
}

async fn run_book_ticker(
    mut socket: Socket,
    mut publisher: LatestBboPublisher,
    symbol: Symbol,
    spec: InstrumentSpec,
    idle_timeout: Duration,
) {
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
                let event: BookTickerEventDto = match serde_json::from_str(text.as_ref()) {
                    Ok(event) => event,
                    Err(error) => break error::invalid_response("bookTicker event", error),
                };
                let update = match mapping::websocket_book(&symbol, &spec, event) {
                    Ok(update) => update,
                    Err(error) => break error,
                };
                if let Err(error) = publisher.publish(update) {
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
) {
    let mut keepalive = tokio::time::interval(keepalive_interval);
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    keepalive.tick().await;
    let idle = tokio::time::sleep(idle_timeout);
    tokio::pin!(idle);

    let terminal_error = loop {
        tokio::select! {
            _ = sender.closed() => {
                let _ = socket.close(None).await;
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
                        let event: PrivateEventDto = match serde_json::from_str(text.as_ref()) {
                            Ok(event) => event,
                            Err(error) => {
                                break error::invalid_response("user-data event", error);
                            }
                        };
                        match event {
                            PrivateEventDto::OrderTradeUpdate { order } => {
                                // Binance user-data streams are account-wide. Ignore
                                // other symbols before consulting the single-symbol
                                // instrument cache used by this adapter.
                                if !is_owned_target_limit_order(&target_symbol, &order) {
                                    continue;
                                }
                                let update = match mapping::websocket_order(&spec, order) {
                                    Ok(Some(update)) => update,
                                    Ok(None) => continue,
                                    Err(error) => break error,
                                };
                                if let Err(error) = sender.publish(update) {
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
                            PrivateEventDto::Other => {}
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
                            "Binance user-data WebSocket closed",
                        );
                    }
                    Ok(Message::Binary(_) | Message::Pong(_) | Message::Frame(_)) => {}
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
    sender.fail(terminal_error);
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

fn stream_endpoint(base: &str, stream_name: &str) -> String {
    let base = base.trim_end_matches('/');
    if base.ends_with("/ws") {
        format!("{base}/{stream_name}")
    } else {
        format!("{base}/ws/{stream_name}")
    }
}

fn is_owned_target_limit_order(
    target_symbol: &Symbol,
    order: &crate::models::OrderTradeEventDto<'_>,
) -> bool {
    order.order_type == "LIMIT"
        && order.symbol == target_symbol.as_str()
        && order.client_order_id.parse::<u64>().is_ok()
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
            cumulative_filled: "0",
            status: "NEW",
        }
    }

    #[test]
    fn builds_stream_urls_with_or_without_ws_suffix() {
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
    fn filters_account_order_events_before_instrument_lookup() {
        let target = Symbol::new("BTCUSDT").unwrap();

        assert!(!is_owned_target_limit_order(
            &target,
            &order("ETHUSDT", "LIMIT")
        ));
        assert!(!is_owned_target_limit_order(
            &target,
            &order("BTCUSDT", "MARKET")
        ));
        assert!(is_owned_target_limit_order(
            &target,
            &order("BTCUSDT", "LIMIT")
        ));
        let mut external = order("BTCUSDT", "LIMIT");
        external.client_order_id = "manual-order";
        assert!(!is_owned_target_limit_order(&target, &external));
    }
}
