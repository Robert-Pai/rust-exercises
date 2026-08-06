use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub(crate) struct ApiErrorDto {
    pub(crate) code: i64,
    #[serde(rename = "msg")]
    pub(crate) message: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ServerTimeDto {
    pub(crate) server_time: u64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ExchangeInfoDto {
    pub(crate) symbols: Vec<ExchangeSymbolDto>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExchangeSymbolDto {
    pub(crate) symbol: String,
    pub(crate) status: String,
    pub(crate) contract_type: String,
    pub(crate) filters: Vec<SymbolFilterDto>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "filterType")]
pub(crate) enum SymbolFilterDto {
    #[serde(rename = "PRICE_FILTER")]
    Price {
        #[serde(rename = "tickSize")]
        tick_size: String,
    },
    #[serde(rename = "LOT_SIZE")]
    LotSize {
        #[serde(rename = "stepSize")]
        step_size: String,
        #[serde(rename = "minQty")]
        min_quantity: String,
        #[serde(rename = "maxQty")]
        max_quantity: String,
    },
    #[serde(other)]
    Other,
}

impl ExchangeSymbolDto {
    pub(crate) fn price_tick_size(&self) -> Option<&str> {
        self.filters.iter().find_map(|filter| match filter {
            SymbolFilterDto::Price { tick_size } => Some(tick_size.as_str()),
            _ => None,
        })
    }

    pub(crate) fn lot_size(&self) -> Option<(&str, &str, &str)> {
        self.filters.iter().find_map(|filter| match filter {
            SymbolFilterDto::LotSize {
                step_size,
                min_quantity,
                max_quantity,
            } => Some((
                step_size.as_str(),
                min_quantity.as_str(),
                max_quantity.as_str(),
            )),
            _ => None,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BookTickerDto {
    pub(crate) symbol: String,
    pub(crate) bid_price: String,
    pub(crate) ask_price: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DualSidePositionDto {
    pub(crate) dual_side_position: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListenKeyDto {
    pub(crate) listen_key: String,
}
