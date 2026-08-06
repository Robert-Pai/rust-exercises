use thiserror::Error;

use crate::{GridPurpose, PriceTicks, Side};

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum GridError {
    #[error("price underflow while moving {side:?} price {base} by {ticks} ticks")]
    PriceUnderflow {
        side: Side,
        base: PriceTicks,
        ticks: u64,
    },

    #[error("price overflow while moving {side:?} price {base} by {ticks} ticks")]
    PriceOverflow {
        side: Side,
        base: PriceTicks,
        ticks: u64,
    },

    #[error("tick offset overflow at grid level {level}")]
    TickOffsetOverflow { level: usize },

    #[error("{side:?} price {price} is not an active grid level")]
    LevelNotFound { side: Side, price: PriceTicks },

    #[error("{side:?} price {price} has grid purpose {actual:?}, expected {expected:?}")]
    PurposeMismatch {
        side: Side,
        price: PriceTicks,
        expected: GridPurpose,
        actual: GridPurpose,
    },

    #[error("{side:?} take-profit price {price} is already assigned to take profit")]
    TakeProfitCollision { side: Side, price: PriceTicks },

    #[error("{side:?} grid has no quote level available for take-profit replacement")]
    NoQuoteCapacity { side: Side },

    #[error("{side:?} price {price} already exists in the candidate grid")]
    PriceCollision { side: Side, price: PriceTicks },

    #[error("grid has {actual} {side:?} levels, expected {expected}")]
    InvalidLevelCount {
        side: Side,
        expected: usize,
        actual: usize,
    },

    #[error("grid would be crossed: highest bid {highest_bid}, lowest ask {lowest_ask}")]
    CrossedGrid {
        highest_bid: PriceTicks,
        lowest_ask: PriceTicks,
    },

    #[error("grid revision overflow")]
    RevisionOverflow,
}
