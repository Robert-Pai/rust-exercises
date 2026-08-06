mod order_id;
mod price;
mod quantity;
mod side;
mod symbol;

pub use order_id::{ClientOrderId, ExchangeOrderId};
pub use price::{NonZeroTickCount, PriceTicks, TickCount};
pub use quantity::{FilledLots, QuantityLots};
pub use side::Side;
pub use symbol::Symbol;

use thiserror::Error;

/// Errors raised while constructing primitive domain values.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ValueError {
    #[error("symbol cannot be empty")]
    EmptySymbol,

    #[error("symbol cannot have leading or trailing whitespace: {0:?}")]
    SymbolWhitespace(String),

    #[error("{kind} cannot be empty")]
    EmptyIdentifier { kind: &'static str },

    #[error("{kind} cannot have leading or trailing whitespace")]
    IdentifierWhitespace { kind: &'static str },

    #[error("price ticks must be greater than zero")]
    ZeroPrice,

    #[error("tick count must be greater than zero")]
    ZeroTickCount,

    #[error("quantity lots must be greater than zero")]
    ZeroQuantity,
}
