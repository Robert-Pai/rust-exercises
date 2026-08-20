use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub(crate) struct BookTickerEventDto<'a> {
    #[serde(rename = "E")]
    pub(crate) event_time: u64,
    #[serde(rename = "T")]
    pub(crate) transaction_time: u64,
    #[serde(borrow, rename = "s")]
    pub(crate) symbol: &'a str,
    #[serde(borrow, rename = "b")]
    pub(crate) bid_price: &'a str,
    #[serde(borrow, rename = "a")]
    pub(crate) ask_price: &'a str,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "e")]
pub(crate) enum PrivateEventDto<'a> {
    #[serde(rename = "ACCOUNT_UPDATE")]
    AccountUpdate {
        #[serde(rename = "E")]
        event_time: u64,
        #[serde(rename = "T")]
        transaction_time: u64,
        #[serde(borrow, rename = "a")]
        account: AccountUpdateDto<'a>,
    },
    #[serde(rename = "ORDER_TRADE_UPDATE")]
    OrderTradeUpdate {
        #[serde(rename = "E")]
        event_time: u64,
        #[serde(rename = "T")]
        transaction_time: u64,
        #[serde(borrow, rename = "o")]
        order: OrderTradeEventDto<'a>,
    },
    #[serde(rename = "TRADE_LITE")]
    TradeLite {
        #[serde(rename = "E")]
        event_time: u64,
        #[serde(rename = "T")]
        transaction_time: u64,
        #[serde(borrow, flatten)]
        trade: TradeLiteEventDto<'a>,
    },
    #[serde(rename = "listenKeyExpired")]
    ListenKeyExpired,
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TradeLiteEventDto<'a> {
    #[serde(borrow, rename = "s")]
    pub(crate) symbol: &'a str,
    #[serde(borrow, rename = "q")]
    pub(crate) original_quantity: &'a str,
    #[serde(borrow, rename = "p")]
    pub(crate) original_price: &'a str,
    #[serde(rename = "m")]
    pub(crate) maker: bool,
    #[serde(borrow, rename = "c")]
    pub(crate) client_order_id: &'a str,
    #[serde(borrow, rename = "S")]
    pub(crate) side: &'a str,
    #[serde(borrow, rename = "L")]
    pub(crate) last_filled_price: &'a str,
    #[serde(borrow, rename = "l")]
    pub(crate) last_filled_quantity: &'a str,
    #[serde(rename = "t")]
    pub(crate) trade_id: u64,
    #[serde(rename = "i")]
    pub(crate) order_id: u64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct OrderTradeEventDto<'a> {
    #[serde(borrow, rename = "s")]
    pub(crate) symbol: &'a str,
    #[serde(borrow, rename = "c")]
    pub(crate) client_order_id: &'a str,
    #[serde(rename = "i")]
    pub(crate) order_id: u64,
    #[serde(borrow, rename = "S")]
    pub(crate) side: &'a str,
    #[serde(borrow, rename = "o")]
    pub(crate) order_type: &'a str,
    #[serde(borrow, rename = "p")]
    pub(crate) price: &'a str,
    #[serde(borrow, rename = "q")]
    pub(crate) original_quantity: &'a str,
    #[serde(borrow, rename = "ap")]
    pub(crate) average_price: &'a str,
    #[serde(borrow, rename = "x")]
    pub(crate) execution_type: &'a str,
    #[serde(borrow, rename = "z")]
    pub(crate) cumulative_filled: &'a str,
    #[serde(borrow, rename = "L")]
    pub(crate) last_filled_price: &'a str,
    #[serde(borrow, rename = "l")]
    pub(crate) last_filled_quantity: &'a str,
    #[serde(default, borrow, rename = "N")]
    pub(crate) commission_asset: Option<&'a str>,
    #[serde(default, borrow, rename = "n")]
    pub(crate) commission: Option<&'a str>,
    #[serde(rename = "T")]
    pub(crate) trade_time: u64,
    #[serde(rename = "t")]
    pub(crate) trade_id: u64,
    #[serde(rename = "m")]
    pub(crate) maker: bool,
    #[serde(borrow, rename = "rp")]
    pub(crate) realized_pnl: &'a str,
    #[serde(borrow, rename = "X")]
    pub(crate) status: &'a str,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AccountUpdateDto<'a> {
    #[serde(borrow, rename = "m")]
    pub(crate) reason: &'a str,
    #[serde(default, borrow, rename = "B")]
    pub(crate) balances: Vec<BalanceUpdateDto<'a>>,
    #[serde(default, borrow, rename = "P")]
    pub(crate) positions: Vec<PositionUpdateDto<'a>>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BalanceUpdateDto<'a> {
    #[serde(borrow, rename = "a")]
    pub(crate) asset: &'a str,
    #[serde(borrow, rename = "wb")]
    pub(crate) wallet_balance: &'a str,
    #[serde(borrow, rename = "cw")]
    pub(crate) cross_wallet_balance: &'a str,
    #[serde(borrow, rename = "bc")]
    pub(crate) balance_change: &'a str,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PositionUpdateDto<'a> {
    #[serde(borrow, rename = "s")]
    pub(crate) symbol: &'a str,
    #[serde(borrow, rename = "pa")]
    pub(crate) position_amount: &'a str,
    #[serde(borrow, rename = "ep")]
    pub(crate) entry_price: &'a str,
    #[serde(borrow, rename = "bep")]
    pub(crate) breakeven_price: &'a str,
    #[serde(borrow, rename = "cr")]
    pub(crate) accumulated_realized_pnl: &'a str,
    #[serde(borrow, rename = "up")]
    pub(crate) unrealized_pnl: &'a str,
    #[serde(borrow, rename = "mt")]
    pub(crate) margin_type: &'a str,
    #[serde(borrow, rename = "iw")]
    pub(crate) isolated_wallet: &'a str,
    #[serde(borrow, rename = "ps")]
    pub(crate) position_side: &'a str,
}
