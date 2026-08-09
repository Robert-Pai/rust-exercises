use maker_ports::{
    AccountSnapshot, AccountUpdate, ExchangeResult, OrderTradeExecution, TradeLiteExecution,
};

/// Lifecycle point at which an explicit account snapshot was requested.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountSnapshotStage {
    Startup,
    Shutdown,
}

impl AccountSnapshotStage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
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
    AccountUpdate(AccountUpdate),
    OrderTrade(OrderTradeExecution),
    TradeLite(TradeLiteExecution),
}
