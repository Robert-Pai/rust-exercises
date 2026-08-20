use std::num::NonZeroUsize;

use crate::{NonZeroTickCount, QuantityLots};

/// Immutable parameters shared by every level in one rolling grid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GridConfig {
    levels_per_side: NonZeroUsize,
    inner_offset: NonZeroTickCount,
    spacing: NonZeroTickCount,
    take_profit: NonZeroTickCount,
    quantity: QuantityLots,
}

impl GridConfig {
    pub const fn new(
        levels_per_side: NonZeroUsize,
        inner_offset: NonZeroTickCount,
        spacing: NonZeroTickCount,
        take_profit: NonZeroTickCount,
        quantity: QuantityLots,
    ) -> Self {
        Self {
            levels_per_side,
            inner_offset,
            spacing,
            take_profit,
            quantity,
        }
    }

    pub const fn levels_per_side(self) -> NonZeroUsize {
        self.levels_per_side
    }

    pub const fn inner_offset(self) -> NonZeroTickCount {
        self.inner_offset
    }

    pub const fn spacing(self) -> NonZeroTickCount {
        self.spacing
    }

    pub const fn take_profit(self) -> NonZeroTickCount {
        self.take_profit
    }

    pub const fn quantity(self) -> QuantityLots {
        self.quantity
    }
}
