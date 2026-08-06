use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WsApiOrderAckDto {
    pub(crate) symbol: String,
    pub(crate) client_order_id: String,
    pub(crate) order_id: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WsApiOrderDto {
    pub(crate) symbol: String,
    pub(crate) client_order_id: String,
    pub(crate) order_id: u64,
    pub(crate) side: String,
    pub(crate) price: String,
    pub(crate) orig_qty: String,
    pub(crate) executed_qty: String,
    pub(crate) status: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct WsApiCancelAllDto {
    pub(crate) code: i64,
    #[serde(rename = "msg")]
    pub(crate) message: String,
}
