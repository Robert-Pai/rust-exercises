use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use maker_domain::{BestBidAsk, InstrumentSpec, OrderUpdate, Symbol};
use maker_ports::{EventStream, ExchangeError, ExchangeErrorKind, ExchangeResult};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::{
    adapter::BinanceUsdm,
    error, mapping,
    models::{BookTickerEventDto, PrivateEventDto},
    network::NetworkRuntime,
    rest::RestClient,
};

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

const EVENT_BUFFER: usize = 256;

pub(crate) async fn subscribe_book_ticker(
    network: NetworkRuntime,
    websocket_url: &str,
    symbol: Symbol,
    spec: InstrumentSpec,
    connect_timeout: Duration,
    idle_timeout: Duration,
) -> ExchangeResult<EventStream<BestBidAsk>> {
    let stream_name = format!("{}@bookTicker", symbol.as_str().to_ascii_lowercase());
    let endpoint = stream_endpoint(websocket_url, &stream_name);
    network
        .call(async move {
            let socket = connect(&endpoint, connect_timeout).await?;
            let (sender, receiver) = mpsc::channel(EVENT_BUFFER);
            tokio::spawn(run_book_ticker(socket, sender, symbol, spec, idle_timeout));
            Ok(Box::pin(ReceiverStream::new(receiver)) as EventStream<BestBidAsk>)
        })
        .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn subscribe_order_updates(
    network: NetworkRuntime,
    adapter: BinanceUsdm,
    rest: RestClient,
    target_symbol: Symbol,
    websocket_url: &str,
    connect_timeout: Duration,
    idle_timeout: Duration,
    keepalive_interval: Duration,
) -> ExchangeResult<EventStream<OrderUpdate>> {
    let websocket_url = websocket_url.to_owned();
    network
        .call(async move {
            let listen_key = rest.create_listen_key().await?;
            let endpoint = stream_endpoint(&websocket_url, &listen_key);
            let socket = connect(&endpoint, connect_timeout).await?;
            let (sender, receiver) = mpsc::channel(EVENT_BUFFER);
            tokio::spawn(run_order_updates(
                socket,
                sender,
                adapter,
                rest,
                target_symbol,
                listen_key,
                idle_timeout,
                keepalive_interval,
            ));
            Ok(Box::pin(ReceiverStream::new(receiver)) as EventStream<OrderUpdate>)
        })
        .await
}

async fn run_book_ticker(
    mut socket: Socket,
    sender: mpsc::Sender<ExchangeResult<BestBidAsk>>,
    symbol: Symbol,
    spec: InstrumentSpec,
    idle_timeout: Duration,
) {
    let terminal_error = loop {
        let message = tokio::select! {
            _ = sender.closed() => {
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
                match sender.try_send(Ok(update)) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Closed(_)) => return,
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        break ExchangeError::new(
                            ExchangeErrorKind::ServiceUnavailable,
                            "Binance bookTicker event queue is full",
                        );
                    }
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
    let _ = sender.try_send(Err(terminal_error));
}

#[allow(clippy::too_many_arguments)]
async fn run_order_updates(
    mut socket: Socket,
    sender: mpsc::Sender<ExchangeResult<OrderUpdate>>,
    adapter: BinanceUsdm,
    rest: RestClient,
    target_symbol: Symbol,
    listen_key: String,
    idle_timeout: Duration,
    keepalive_interval: Duration,
) {
    let mut keepalive = tokio::time::interval(keepalive_interval);
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    keepalive.tick().await;

    let terminal_error = loop {
        tokio::select! {
            _ = sender.closed() => {
                let _ = socket.close(None).await;
                return;
            }
            message = socket.next() => {
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
                                if !is_target_limit_order(&target_symbol, &order) {
                                    continue;
                                }
                                let spec = match adapter.cached_instrument_spec(&target_symbol) {
                                    Ok(spec) => spec,
                                    Err(error) => break error,
                                };
                                let update = match mapping::websocket_order(&spec, order) {
                                    Ok(Some(update)) => update,
                                    Ok(None) => continue,
                                    Err(error) => break error,
                                };
                                match sender.try_send(Ok(update)) {
                                    Ok(()) => {}
                                    Err(mpsc::error::TrySendError::Closed(_)) => return,
                                    Err(mpsc::error::TrySendError::Full(_)) => {
                                        break ExchangeError::new(
                                            ExchangeErrorKind::ServiceUnavailable,
                                            "Binance user-data event queue is full",
                                        );
                                    }
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
            _ = tokio::time::sleep(idle_timeout) => {
                break ExchangeError::new(
                    ExchangeErrorKind::Timeout,
                    "Binance user-data WebSocket became idle",
                );
            }
        }
    };
    let _ = sender.try_send(Err(terminal_error));
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

fn is_target_limit_order(
    target_symbol: &Symbol,
    order: &crate::models::OrderTradeEventDto,
) -> bool {
    order.order_type == "LIMIT" && order.symbol == target_symbol.as_str()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::OrderTradeEventDto;

    fn order(symbol: &str, order_type: &str) -> OrderTradeEventDto {
        OrderTradeEventDto {
            symbol: symbol.to_owned(),
            client_order_id: "maker-1".to_owned(),
            order_id: 1,
            side: "BUY".to_owned(),
            order_type: order_type.to_owned(),
            price: "1".to_owned(),
            original_quantity: "1".to_owned(),
            cumulative_filled: "0".to_owned(),
            status: "NEW".to_owned(),
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

        assert!(!is_target_limit_order(&target, &order("ETHUSDT", "LIMIT")));
        assert!(!is_target_limit_order(&target, &order("BTCUSDT", "MARKET")));
        assert!(is_target_limit_order(&target, &order("BTCUSDT", "LIMIT")));
    }
}
