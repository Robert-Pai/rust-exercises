use crate::{PriceTicks, QuantityLots, Side};

/// Local strategy meaning of a grid level, independent of exchange order status.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum GridPurpose {
    Quote,
    TakeProfit,
}

/// One desired level in the grid.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct GridLevel {
    side: Side,
    price: PriceTicks,
    quantity: QuantityLots,
    purpose: GridPurpose,
}

impl GridLevel {
    pub const fn new(
        side: Side,
        price: PriceTicks,
        quantity: QuantityLots,
        purpose: GridPurpose,
    ) -> Self {
        Self {
            side,
            price,
            quantity,
            purpose,
        }
    }

    pub const fn side(self) -> Side {
        self.side
    }

    pub const fn price(self) -> PriceTicks {
        self.price
    }

    pub const fn quantity(self) -> QuantityLots {
        self.quantity
    }

    pub const fn purpose(self) -> GridPurpose {
        self.purpose
    }

    pub const fn with_purpose(self, purpose: GridPurpose) -> Self {
        Self { purpose, ..self }
    }

    pub fn same_order(self, other: Self) -> bool {
        self.side == other.side && self.price == other.price && self.quantity == other.quantity
    }
}

/// A fully filled level accepted as input to the grid state machine.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FilledLevel {
    side: Side,
    price: PriceTicks,
    purpose: GridPurpose,
}

impl FilledLevel {
    pub const fn new(side: Side, price: PriceTicks, purpose: GridPurpose) -> Self {
        Self {
            side,
            price,
            purpose,
        }
    }

    pub const fn side(self) -> Side {
        self.side
    }

    pub const fn price(self) -> PriceTicks {
        self.price
    }

    pub const fn purpose(self) -> GridPurpose {
        self.purpose
    }
}

impl From<GridLevel> for FilledLevel {
    fn from(level: GridLevel) -> Self {
        Self::new(level.side(), level.price(), level.purpose())
    }
}
