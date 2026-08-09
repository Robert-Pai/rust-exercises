//! Exchange-neutral asynchronous boundaries used by the maker engine.
//!
//! Implementations belong in exchange adapter crates. Consumers should depend
//! on these traits rather than concrete REST or WebSocket clients.

#![forbid(unsafe_code)]

mod error;
mod latest_bbo;
mod order_updates;
mod ports;
mod stream;
mod types;

pub use error::{ExchangeError, ExchangeErrorKind, ExchangeResult};
pub use latest_bbo::{LatestBbo, LatestBboPublisher, LatestBboSubscription, ReceivedBestBidAsk};
pub use order_updates::{OrderUpdatePublisher, OrderUpdateSubscription, ReceivedOrderUpdate};
pub use ports::{
    Exchange, ExchangeFuture, InstrumentPort, MarketDataPort, OrderEventPort, TradingPort,
};
pub use stream::EventStream;
pub use types::{CancelOutcome, PlaceOrderAck, PositionMode};
