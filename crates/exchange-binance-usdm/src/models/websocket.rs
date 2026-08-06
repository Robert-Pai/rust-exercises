use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub(crate) struct BookTickerEventDto {
    #[serde(rename = "s")]
    pub(crate) symbol: String,
    #[serde(rename = "b")]
    pub(crate) bid_price: String,
    #[serde(rename = "a")]
    pub(crate) ask_price: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "e")]
pub(crate) enum PrivateEventDto {
    #[serde(rename = "ORDER_TRADE_UPDATE")]
    OrderTradeUpdate {
        #[serde(rename = "o")]
        order: OrderTradeEventDto,
    },
    #[serde(rename = "listenKeyExpired")]
    ListenKeyExpired,
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
pub(crate) struct OrderTradeEventDto {
    #[serde(rename = "s")]
    pub(crate) symbol: String,
    #[serde(rename = "c")]
    pub(crate) client_order_id: String,
    #[serde(rename = "i")]
    pub(crate) order_id: u64,
    #[serde(rename = "S")]
    pub(crate) side: String,
    #[serde(rename = "o")]
    pub(crate) order_type: String,
    #[serde(rename = "p")]
    pub(crate) price: String,
    #[serde(rename = "q")]
    pub(crate) original_quantity: String,
    #[serde(rename = "z")]
    pub(crate) cumulative_filled: String,
    #[serde(rename = "X")]
    pub(crate) status: String,
}
