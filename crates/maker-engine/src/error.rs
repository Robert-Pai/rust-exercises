use maker_domain::{GridError, InstrumentError, Symbol, ValueError};
use maker_ports::{ExchangeError, ExchangeErrorKind, PositionMode};
use thiserror::Error;

use crate::{EngineConfigError, RegistryError};

#[derive(Debug, Error)]
pub enum EngineError {
    #[error(transparent)]
    Config(#[from] EngineConfigError),

    #[error("exchange operation `{operation}` failed: {source}")]
    Exchange {
        operation: &'static str,
        #[source]
        source: ExchangeError,
    },

    #[error("account must use one-way position mode, got {0:?}")]
    UnsupportedPositionMode(PositionMode),

    #[error("expected symbol {expected}, received {actual}")]
    SymbolMismatch { expected: Symbol, actual: Symbol },

    #[error("exchange trading rules changed for {symbol}; rebuilding maker session")]
    InstrumentRulesChanged { symbol: Symbol },

    #[error("exchange adapter violated its contract: {0}")]
    AdapterContract(String),

    #[error(transparent)]
    Instrument(#[from] InstrumentError),

    #[error(transparent)]
    Grid(#[from] GridError),

    #[error(transparent)]
    Registry(#[from] RegistryError),

    #[error(transparent)]
    DomainValue(#[from] ValueError),

    #[error("fixed engine storage `{storage}` is full; quoting must recover without eviction")]
    CapacityExhausted { storage: &'static str },

    #[error("client-order session counter overflowed")]
    SessionOverflow,

    #[error("client-order sequence overflowed")]
    OrderSequenceOverflow,
}

impl EngineError {
    pub(crate) fn exchange(operation: &'static str, source: ExchangeError) -> Self {
        Self::Exchange { operation, source }
    }

    pub(crate) fn exchange_is_fatal(&self) -> bool {
        let Self::Exchange { source, .. } = self else {
            return false;
        };
        matches!(
            source.kind(),
            ExchangeErrorKind::Authentication
                | ExchangeErrorKind::InvalidRequest
                | ExchangeErrorKind::Unsupported
        )
    }

    pub(crate) fn recommends_recovery(&self) -> bool {
        match self {
            Self::AdapterContract(_)
            | Self::CapacityExhausted { .. }
            | Self::Registry(_)
            | Self::SymbolMismatch { .. }
            | Self::InstrumentRulesChanged { .. } => true,
            Self::Grid(GridError::LevelNotFound { .. })
            | Self::Grid(GridError::PriceCollision { .. })
            | Self::Grid(GridError::CrossedGrid { .. })
            | Self::Grid(GridError::InvalidLevelCount { .. }) => true,
            Self::Exchange { .. } => !self.exchange_is_fatal(),
            Self::Config(_)
            | Self::UnsupportedPositionMode(_)
            | Self::Instrument(_)
            | Self::Grid(_)
            | Self::DomainValue(_)
            | Self::SessionOverflow
            | Self::OrderSequenceOverflow => false,
        }
    }
}
