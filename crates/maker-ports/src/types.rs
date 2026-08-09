use maker_domain::{ClientOrderId, ExchangeOrderId, OrderUpdate, Side, Symbol};
use rust_decimal::Decimal;

/// Position side reported by Binance account endpoints and user-data streams.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AccountPositionSide {
    Both,
    Long,
    Short,
}

impl AccountPositionSide {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Both => "BOTH",
            Self::Long => "LONG",
            Self::Short => "SHORT",
        }
    }
}

/// Margin accounting mode attached to a user-data position update.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MarginType {
    Cross,
    Isolated,
}

impl MarginType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cross => "cross",
            Self::Isolated => "isolated",
        }
    }
}

/// Reason attached to an `ACCOUNT_UPDATE` event.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AccountUpdateReason {
    Deposit,
    Withdraw,
    Order,
    FundingFee,
    WithdrawReject,
    Adjustment,
    InsuranceClear,
    AdminDeposit,
    AdminWithdraw,
    MarginTransfer,
    MarginTypeChange,
    AssetTransfer,
    OptionsPremiumFee,
    OptionsSettleProfit,
    AutoExchange,
    CoinSwapDeposit,
    CoinSwapWithdraw,
    Other,
}

impl AccountUpdateReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Deposit => "DEPOSIT",
            Self::Withdraw => "WITHDRAW",
            Self::Order => "ORDER",
            Self::FundingFee => "FUNDING_FEE",
            Self::WithdrawReject => "WITHDRAW_REJECT",
            Self::Adjustment => "ADJUSTMENT",
            Self::InsuranceClear => "INSURANCE_CLEAR",
            Self::AdminDeposit => "ADMIN_DEPOSIT",
            Self::AdminWithdraw => "ADMIN_WITHDRAW",
            Self::MarginTransfer => "MARGIN_TRANSFER",
            Self::MarginTypeChange => "MARGIN_TYPE_CHANGE",
            Self::AssetTransfer => "ASSET_TRANSFER",
            Self::OptionsPremiumFee => "OPTIONS_PREMIUM_FEE",
            Self::OptionsSettleProfit => "OPTIONS_SETTLE_PROFIT",
            Self::AutoExchange => "AUTO_EXCHANGE",
            Self::CoinSwapDeposit => "COIN_SWAP_DEPOSIT",
            Self::CoinSwapWithdraw => "COIN_SWAP_WITHDRAW",
            Self::Other => "OTHER",
        }
    }
}

/// One balance returned by the V2 futures account-balance query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountBalance {
    pub asset: Symbol,
    pub wallet_balance: Decimal,
    pub cross_wallet_balance: Decimal,
    pub cross_unrealized_pnl: Decimal,
    pub available_balance: Decimal,
    pub max_withdraw_amount: Decimal,
    pub margin_available: Option<bool>,
    pub update_time_ms: u64,
}

/// One position returned by the V2 futures account-information query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountPosition {
    pub symbol: Symbol,
    pub position_side: AccountPositionSide,
    pub position_amount: Decimal,
    pub unrealized_pnl: Decimal,
    pub isolated_margin: Decimal,
    pub notional: Decimal,
    pub isolated_wallet: Decimal,
    pub initial_margin: Decimal,
    pub maintenance_margin: Decimal,
    pub update_time_ms: u64,
}

/// A point-in-time account view assembled from the two V2 account queries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountSnapshot {
    pub balances: Vec<AccountBalance>,
    pub positions: Vec<AccountPosition>,
}

/// One changed balance inside an `ACCOUNT_UPDATE` event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BalanceUpdate {
    pub asset: Symbol,
    pub wallet_balance: Decimal,
    pub cross_wallet_balance: Decimal,
    pub balance_change: Decimal,
}

/// One changed position inside an `ACCOUNT_UPDATE` event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PositionUpdate {
    pub symbol: Symbol,
    pub position_amount: Decimal,
    pub entry_price: Decimal,
    pub breakeven_price: Decimal,
    pub accumulated_realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub margin_type: MarginType,
    pub isolated_wallet: Decimal,
    pub position_side: AccountPositionSide,
}

/// Normalized balance and position changes from the user-data stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountUpdate {
    pub event_time_ms: u64,
    pub transaction_time_ms: u64,
    pub reason: AccountUpdateReason,
    pub balances: Vec<BalanceUpdate>,
    pub positions: Vec<PositionUpdate>,
}

/// Fill detail carried by an `ORDER_TRADE_UPDATE` event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderTradeExecution {
    pub update: OrderUpdate,
    pub event_time_ms: u64,
    pub transaction_time_ms: u64,
    pub trade_time_ms: u64,
    pub average_price: Decimal,
    pub last_filled_price: Decimal,
    pub last_filled_quantity: Decimal,
    pub cumulative_filled_quantity: Decimal,
    pub commission_asset: Option<Symbol>,
    pub commission: Option<Decimal>,
    pub trade_id: u64,
    pub realized_pnl: Decimal,
    pub maker: bool,
}

/// Low-latency fill detail carried by a `TRADE_LITE` event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TradeLiteExecution {
    pub symbol: Symbol,
    pub client_order_id: ClientOrderId,
    pub exchange_order_id: ExchangeOrderId,
    pub side: Side,
    pub event_time_ms: u64,
    pub transaction_time_ms: u64,
    pub original_quantity: Decimal,
    pub original_price: Decimal,
    pub last_filled_price: Decimal,
    pub last_filled_quantity: Decimal,
    pub trade_id: u64,
    pub maker: bool,
}

/// The account position accounting mode relevant to order placement.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PositionMode {
    OneWay,
    Hedge,
}

/// Identity assigned to a successfully accepted order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlaceOrderAck {
    client_order_id: ClientOrderId,
    exchange_order_id: ExchangeOrderId,
    symbol: Symbol,
}

impl PlaceOrderAck {
    pub fn new(
        symbol: Symbol,
        client_order_id: ClientOrderId,
        exchange_order_id: ExchangeOrderId,
    ) -> Self {
        Self {
            symbol,
            client_order_id,
            exchange_order_id,
        }
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn client_order_id(&self) -> &ClientOrderId {
        &self.client_order_id
    }

    pub fn exchange_order_id(&self) -> &ExchangeOrderId {
        &self.exchange_order_id
    }
}

/// The terminal resolution of a single-order cancellation request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancelOutcome {
    /// The exchange acknowledged cancellation but supplied no full update.
    Canceled,

    /// The exchange supplied the final state, including cancellation or a fill
    /// that raced with cancellation.
    Terminal(OrderUpdate),

    /// The exchange no longer recognizes either identifier for this order.
    NotFound,
}

impl CancelOutcome {
    pub fn terminal_update(&self) -> Option<&OrderUpdate> {
        match self {
            Self::Terminal(update) => Some(update),
            Self::Canceled | Self::NotFound => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use maker_domain::{FilledLots, OrderStatus, PriceTicks, QuantityLots, Side};

    use super::*;

    #[test]
    fn exposes_terminal_cancel_update() {
        let update = OrderUpdate::new(
            Symbol::new("BTCUSDT").unwrap(),
            ClientOrderId::new(1).unwrap(),
            ExchangeOrderId::new(42).unwrap(),
            Side::Buy,
            PriceTicks::new(100).unwrap(),
            QuantityLots::new(2).unwrap(),
            FilledLots::new(2),
            OrderStatus::Filled,
        )
        .unwrap();
        let outcome = CancelOutcome::Terminal(update);

        assert_eq!(outcome.terminal_update(), Some(&update));
        assert_eq!(CancelOutcome::Canceled.terminal_update(), None);
    }
}
