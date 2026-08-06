use std::{num::NonZeroUsize, time::Duration};

use maker_domain::{NonZeroTickCount, Symbol, ValueError};
use rust_decimal::Decimal;
use thiserror::Error;

/// Runtime and strategy parameters needed by the rolling-grid engine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineConfig {
    symbol: Symbol,
    levels_per_side: NonZeroUsize,
    inner_ticks: NonZeroTickCount,
    spacing_ticks: NonZeroTickCount,
    take_profit_ticks: NonZeroTickCount,
    quantity: Decimal,
    reconcile_interval: Duration,
    instrument_refresh_interval: Duration,
    reconnect_delay: Duration,
}

impl EngineConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        symbol: Symbol,
        levels_per_side: usize,
        inner_ticks: u64,
        spacing_ticks: u64,
        take_profit_ticks: u64,
        quantity: Decimal,
        reconcile_interval: Duration,
        instrument_refresh_interval: Duration,
        reconnect_delay: Duration,
    ) -> Result<Self, EngineConfigError> {
        let levels_per_side =
            NonZeroUsize::new(levels_per_side).ok_or(EngineConfigError::ZeroLevels)?;
        let inner_ticks =
            NonZeroTickCount::new(inner_ticks).map_err(EngineConfigError::DomainValue)?;
        let spacing_ticks =
            NonZeroTickCount::new(spacing_ticks).map_err(EngineConfigError::DomainValue)?;
        let take_profit_ticks =
            NonZeroTickCount::new(take_profit_ticks).map_err(EngineConfigError::DomainValue)?;
        if quantity <= Decimal::ZERO {
            return Err(EngineConfigError::NonPositiveQuantity(quantity));
        }
        if reconcile_interval.is_zero() {
            return Err(EngineConfigError::ZeroReconcileInterval);
        }
        if instrument_refresh_interval.is_zero() {
            return Err(EngineConfigError::ZeroInstrumentRefreshInterval);
        }
        if reconnect_delay.is_zero() {
            return Err(EngineConfigError::ZeroReconnectDelay);
        }

        Ok(Self {
            symbol,
            levels_per_side,
            inner_ticks,
            spacing_ticks,
            take_profit_ticks,
            quantity,
            reconcile_interval,
            instrument_refresh_interval,
            reconnect_delay,
        })
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub const fn levels_per_side(&self) -> NonZeroUsize {
        self.levels_per_side
    }

    pub const fn inner_ticks(&self) -> NonZeroTickCount {
        self.inner_ticks
    }

    pub const fn spacing_ticks(&self) -> NonZeroTickCount {
        self.spacing_ticks
    }

    pub const fn take_profit_ticks(&self) -> NonZeroTickCount {
        self.take_profit_ticks
    }

    pub const fn quantity(&self) -> Decimal {
        self.quantity
    }

    pub const fn reconcile_interval(&self) -> Duration {
        self.reconcile_interval
    }

    pub const fn instrument_refresh_interval(&self) -> Duration {
        self.instrument_refresh_interval
    }

    pub const fn reconnect_delay(&self) -> Duration {
        self.reconnect_delay
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum EngineConfigError {
    #[error("levels per side must be greater than zero")]
    ZeroLevels,

    #[error("quantity must be greater than zero, got {0}")]
    NonPositiveQuantity(Decimal),

    #[error("reconcile interval must be greater than zero")]
    ZeroReconcileInterval,

    #[error("instrument refresh interval must be greater than zero")]
    ZeroInstrumentRefreshInterval,

    #[error("reconnect delay must be greater than zero")]
    ZeroReconnectDelay,

    #[error(transparent)]
    DomainValue(ValueError),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn symbol() -> Symbol {
        Symbol::new("BTCUSDT").unwrap()
    }

    #[test]
    fn rejects_zero_values() {
        assert!(matches!(
            EngineConfig::new(
                symbol(),
                0,
                1,
                1,
                1,
                Decimal::ONE,
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(1),
            ),
            Err(EngineConfigError::ZeroLevels)
        ));
        assert!(matches!(
            EngineConfig::new(
                symbol(),
                1,
                1,
                1,
                1,
                Decimal::ZERO,
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(1),
            ),
            Err(EngineConfigError::NonPositiveQuantity(_))
        ));
        assert!(matches!(
            EngineConfig::new(
                symbol(),
                1,
                1,
                1,
                1,
                Decimal::ONE,
                Duration::from_secs(1),
                Duration::ZERO,
                Duration::from_secs(1),
            ),
            Err(EngineConfigError::ZeroInstrumentRefreshInterval)
        ));
    }
}
