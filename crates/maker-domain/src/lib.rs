//! Exchange-neutral domain types and the pure rolling-grid state machine.

#![forbid(unsafe_code)]

pub mod grid;
pub mod market;
pub mod order;
pub mod value;

pub use grid::{
    FilledLevel, GridConfig, GridError, GridLevel, GridModel, GridPurpose, GridReassignment,
    GridRevision, GridTransition,
};
pub use market::{BestBidAsk, BookError, InstrumentError, InstrumentSpec, MarketKind};
pub use order::{ExecutionPolicy, OrderIntent, OrderStatus, OrderUpdate, OrderUpdateError};
pub use value::{
    ClientOrderId, ExchangeOrderId, FilledLots, NonZeroTickCount, PriceTicks, QuantityLots, Side,
    Symbol, TickCount, ValueError,
};
