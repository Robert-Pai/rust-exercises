use thiserror::Error;

use crate::{PriceTicks, Symbol};

/// A validated best bid and best ask for one instrument.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BestBidAsk {
    bid: PriceTicks,
    ask: PriceTicks,
    symbol: Symbol,
}

impl BestBidAsk {
    pub fn new(symbol: Symbol, bid: PriceTicks, ask: PriceTicks) -> Result<Self, BookError> {
        if bid >= ask {
            return Err(BookError::LockedOrCrossed { bid, ask });
        }
        Ok(Self { symbol, bid, ask })
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub const fn bid(&self) -> PriceTicks {
        self.bid
    }

    pub const fn ask(&self) -> PriceTicks {
        self.ask
    }

    pub fn floor_mid(&self) -> PriceTicks {
        let spread = self.ask.get() - self.bid.get();
        PriceTicks::new(self.bid.get() + spread / 2)
            .expect("a midpoint between two positive prices is positive")
    }

    pub fn ceil_mid(&self) -> PriceTicks {
        let spread = self.ask.get() - self.bid.get();
        PriceTicks::new(self.bid.get() + spread.div_ceil(2))
            .expect("a midpoint between two positive prices is positive")
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum BookError {
    #[error("best bid {bid} must be below best ask {ask}")]
    LockedOrCrossed { bid: PriceTicks, ask: PriceTicks },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn symbol() -> Symbol {
        Symbol::new("BTCUSDT").unwrap()
    }

    #[test]
    fn has_compact_inline_layout() {
        assert!(std::mem::size_of::<BestBidAsk>() <= 32);
    }

    #[test]
    fn computes_floor_and_ceil_mid_without_floats() {
        let odd = BestBidAsk::new(
            symbol(),
            PriceTicks::new(99).unwrap(),
            PriceTicks::new(100).unwrap(),
        )
        .unwrap();
        assert_eq!(odd.floor_mid().get(), 99);
        assert_eq!(odd.ceil_mid().get(), 100);

        let even = BestBidAsk::new(
            symbol(),
            PriceTicks::new(98).unwrap(),
            PriceTicks::new(100).unwrap(),
        )
        .unwrap();
        assert_eq!(even.floor_mid().get(), 99);
        assert_eq!(even.ceil_mid().get(), 99);
    }
}
