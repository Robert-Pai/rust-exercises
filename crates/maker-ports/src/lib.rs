//! Exchange-neutral asynchronous boundaries used by the maker engine.
//!
//! Implementations belong in exchange adapter crates. Consumers should depend
//! on these traits rather than concrete REST or WebSocket clients.

#![forbid(unsafe_code)]

mod error;
mod ports;
mod stream;
mod types;

pub use error::{ExchangeError, ExchangeErrorKind, ExchangeResult};
pub use ports::{Exchange, InstrumentPort, MarketDataPort, OrderEventPort, TradingPort};
pub use stream::EventStream;
pub use types::{CancelOutcome, PlaceOrderAck, PositionMode};
