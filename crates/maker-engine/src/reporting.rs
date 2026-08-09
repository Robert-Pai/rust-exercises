use maker_domain::{ClientOrderId, OrderUpdate, PriceTicks, QuantityLots, Side, Symbol};
use maker_ports::{
    AccountSnapshot, AccountUpdate, ExchangeError, ExchangeResult, OrderTradeExecution,
    TradeLiteExecution,
};

/// Lifecycle point at which an explicit account snapshot was requested.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountSnapshotStage {
    Startup,
    Recovery,
    Shutdown,
}

impl AccountSnapshotStage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Recovery => "recovery",
            Self::Shutdown => "shutdown",
        }
    }
}

/// Lifecycle point at which all open orders are canceled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancelAllStage {
    Startup,
    Recovery,
    Shutdown,
}

impl CancelAllStage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Recovery => "recovery",
            Self::Shutdown => "shutdown",
        }
    }
}

/// Cold-path events forwarded to the asynchronous CLI logger.
#[derive(Debug)]
pub enum EngineReport {
    AccountSnapshot {
        stage: AccountSnapshotStage,
        result: ExchangeResult<AccountSnapshot>,
    },
    CancelAllStarted {
        stage: CancelAllStage,
        symbol: Symbol,
    },
    CancelAllFinished {
        stage: CancelAllStage,
        symbol: Symbol,
        duration_us: u64,
        result: ExchangeResult<()>,
    },
    AccountUpdate(AccountUpdate),
    OrderUpdate(OrderUpdate),
    OrderTrade(OrderTradeExecution),
    TradeLite(TradeLiteExecution),
    ExchangeFailure {
        operation: &'static str,
        symbol: Symbol,
        client_order_id: Option<ClientOrderId>,
        side: Option<Side>,
        price_ticks: Option<PriceTicks>,
        quantity_lots: Option<QuantityLots>,
        error: ExchangeError,
    },
    EngineFailure {
        operation: &'static str,
        error: String,
    },
}
