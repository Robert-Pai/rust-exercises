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
    #[serde(rename = "ORDER_TRADE_UPDATE")]
    OrderTradeUpdate {
        #[serde(rename = "E")]
        event_time: u64,
        #[serde(rename = "T")]
        transaction_time: u64,
        #[serde(borrow, rename = "o")]
        order: OrderTradeEventDto<'a>,
    },
    #[serde(rename = "listenKeyExpired")]
    ListenKeyExpired,
    #[serde(other)]
    Other,
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
    #[serde(borrow, rename = "z")]
    pub(crate) cumulative_filled: &'a str,
    #[serde(borrow, rename = "X")]
    pub(crate) status: &'a str,
}
