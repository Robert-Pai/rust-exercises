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
#[serde(rename_all = "camelCase")]
pub(crate) struct WsApiAccountBalanceDto {
    pub(crate) asset: String,
    pub(crate) balance: String,
    pub(crate) cross_wallet_balance: String,
    pub(crate) cross_un_pnl: String,
    pub(crate) available_balance: String,
    pub(crate) max_withdraw_amount: String,
    #[serde(default)]
    pub(crate) margin_available: Option<bool>,
    pub(crate) update_time: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WsApiAccountStatusDto {
    #[serde(default)]
    pub(crate) positions: Vec<WsApiAccountPositionDto>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WsApiAccountPositionDto {
    pub(crate) symbol: String,
    pub(crate) position_side: String,
    pub(crate) position_amt: String,
    pub(crate) unrealized_profit: String,
    pub(crate) isolated_margin: String,
    pub(crate) notional: String,
    pub(crate) isolated_wallet: String,
    pub(crate) initial_margin: String,
    pub(crate) maint_margin: String,
    pub(crate) update_time: u64,
}
