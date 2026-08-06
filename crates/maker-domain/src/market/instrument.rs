use rust_decimal::{Decimal, prelude::ToPrimitive};
use thiserror::Error;

use crate::{PriceTicks, QuantityLots, Symbol, ValueError};

/// The broad settlement model of an instrument.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MarketKind {
    Spot,
    LinearPerpetual,
    InversePerpetual,
}

/// Exchange rules normalized into exact decimal increments and integer limits.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstrumentSpec {
    symbol: Symbol,
    market_kind: MarketKind,
    tick_size: Decimal,
    quantity_step: Decimal,
    min_quantity: QuantityLots,
    max_quantity: QuantityLots,
}

impl InstrumentSpec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        symbol: Symbol,
        market_kind: MarketKind,
        tick_size: Decimal,
        quantity_step: Decimal,
        min_quantity: Decimal,
        max_quantity: Decimal,
    ) -> Result<Self, InstrumentError> {
        ensure_positive_rule("tick size", tick_size)?;
        ensure_positive_rule("quantity step", quantity_step)?;

        let min_lots = exact_units("minimum quantity", min_quantity, quantity_step)?;
        let max_lots = exact_units("maximum quantity", max_quantity, quantity_step)?;
        let min_quantity =
            QuantityLots::new(min_lots).map_err(InstrumentError::InvalidDomainValue)?;
        let max_quantity =
            QuantityLots::new(max_lots).map_err(InstrumentError::InvalidDomainValue)?;

        if min_quantity > max_quantity {
            return Err(InstrumentError::InvalidQuantityRange {
                min: min_quantity,
                max: max_quantity,
            });
        }

        Ok(Self {
            symbol,
            market_kind,
            tick_size,
            quantity_step,
            min_quantity,
            max_quantity,
        })
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub const fn market_kind(&self) -> MarketKind {
        self.market_kind
    }

    pub const fn tick_size(&self) -> Decimal {
        self.tick_size
    }

    pub const fn quantity_step(&self) -> Decimal {
        self.quantity_step
    }

    pub const fn min_quantity(&self) -> QuantityLots {
        self.min_quantity
    }

    pub const fn max_quantity(&self) -> QuantityLots {
        self.max_quantity
    }

    pub fn price_to_ticks_exact(&self, price: Decimal) -> Result<PriceTicks, InstrumentError> {
        let ticks = exact_units("price", price, self.tick_size)?;
        PriceTicks::new(ticks).map_err(InstrumentError::InvalidDomainValue)
    }

    pub fn ticks_to_price(&self, ticks: PriceTicks) -> Result<Decimal, InstrumentError> {
        self.tick_size
            .checked_mul(Decimal::from(ticks.get()))
            .ok_or(InstrumentError::DecimalOverflow { field: "price" })
    }

    pub fn quantity_to_lots_exact(
        &self,
        quantity: Decimal,
    ) -> Result<QuantityLots, InstrumentError> {
        let lots = QuantityLots::new(exact_units("quantity", quantity, self.quantity_step)?)
            .map_err(InstrumentError::InvalidDomainValue)?;

        if lots < self.min_quantity || lots > self.max_quantity {
            return Err(InstrumentError::QuantityOutOfRange {
                quantity: lots,
                min: self.min_quantity,
                max: self.max_quantity,
            });
        }
        Ok(lots)
    }

    pub fn lots_to_quantity(&self, lots: QuantityLots) -> Result<Decimal, InstrumentError> {
        self.quantity_step
            .checked_mul(Decimal::from(lots.get()))
            .ok_or(InstrumentError::DecimalOverflow { field: "quantity" })
    }
}

fn ensure_positive_rule(field: &'static str, value: Decimal) -> Result<(), InstrumentError> {
    if value <= Decimal::ZERO {
        return Err(InstrumentError::NonPositiveRule { field, value });
    }
    Ok(())
}

fn exact_units(
    field: &'static str,
    value: Decimal,
    increment: Decimal,
) -> Result<u64, InstrumentError> {
    if value <= Decimal::ZERO {
        return Err(InstrumentError::NonPositiveValue { field, value });
    }
    let units = value
        .checked_div(increment)
        .ok_or(InstrumentError::DecimalOverflow { field })?;
    if !units.fract().is_zero() {
        return Err(InstrumentError::NotIncrementAligned {
            field,
            value,
            increment,
        });
    }
    units
        .to_u64()
        .ok_or(InstrumentError::UnitConversionOverflow { field, value })
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum InstrumentError {
    #[error("{field} rule must be positive, got {value}")]
    NonPositiveRule { field: &'static str, value: Decimal },

    #[error("{field} must be positive, got {value}")]
    NonPositiveValue { field: &'static str, value: Decimal },

    #[error("{field} {value} is not aligned to increment {increment}")]
    NotIncrementAligned {
        field: &'static str,
        value: Decimal,
        increment: Decimal,
    },

    #[error("{field} {value} cannot be represented as integer units")]
    UnitConversionOverflow { field: &'static str, value: Decimal },

    #[error("decimal overflow while converting {field}")]
    DecimalOverflow { field: &'static str },

    #[error("minimum quantity {min} exceeds maximum quantity {max}")]
    InvalidQuantityRange {
        min: QuantityLots,
        max: QuantityLots,
    },

    #[error("quantity {quantity} is outside [{min}, {max}]")]
    QuantityOutOfRange {
        quantity: QuantityLots,
        min: QuantityLots,
        max: QuantityLots,
    },

    #[error(transparent)]
    InvalidDomainValue(ValueError),
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;

    fn spec() -> InstrumentSpec {
        InstrumentSpec::new(
            Symbol::new("BTCUSDT").unwrap(),
            MarketKind::LinearPerpetual,
            Decimal::new(1, 1),
            Decimal::new(1, 3),
            Decimal::new(1, 3),
            Decimal::new(1000, 3),
        )
        .unwrap()
    }

    #[test]
    fn converts_exact_prices_and_quantities() {
        let spec = spec();
        let ticks = spec.price_to_ticks_exact(Decimal::new(1234, 1)).unwrap();
        assert_eq!(ticks.get(), 1234);
        assert_eq!(spec.ticks_to_price(ticks).unwrap(), Decimal::new(1234, 1));

        let lots = spec.quantity_to_lots_exact(Decimal::new(25, 3)).unwrap();
        assert_eq!(lots.get(), 25);
        assert_eq!(spec.lots_to_quantity(lots).unwrap(), Decimal::new(25, 3));
    }

    #[test]
    fn rejects_implicit_rounding() {
        let spec = spec();
        assert!(matches!(
            spec.price_to_ticks_exact(Decimal::new(12345, 2)),
            Err(InstrumentError::NotIncrementAligned { .. })
        ));
        assert!(matches!(
            spec.quantity_to_lots_exact(Decimal::new(25, 4)),
            Err(InstrumentError::NotIncrementAligned { .. })
        ));
    }
}
