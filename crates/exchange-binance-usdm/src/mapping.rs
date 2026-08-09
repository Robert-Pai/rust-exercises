use std::str::FromStr;

use maker_domain::{
    BestBidAsk, ClientOrderId, ExchangeOrderId, FilledLots, InstrumentSpec, MarketKind,
    OrderStatus, OrderUpdate, PriceTicks, QuantityLots, Side, Symbol,
};
use maker_ports::{ExchangeError, ExchangeErrorKind, ExchangeResult, PlaceOrderAck, PositionMode};
use rust_decimal::{Decimal, prelude::ToPrimitive};

use crate::{
    error::invalid_response,
    models::{
        BookTickerDto, BookTickerEventDto, DualSidePositionDto, ExchangeSymbolDto,
        OrderTradeEventDto, WsApiOrderAckDto, WsApiOrderDto,
    },
};

pub(crate) fn instrument(
    requested_symbol: &Symbol,
    wire: ExchangeSymbolDto,
) -> ExchangeResult<InstrumentSpec> {
    ensure_symbol(requested_symbol, &wire.symbol, "exchangeInfo")?;
    if wire.status != "TRADING" {
        return Err(ExchangeError::new(
            ExchangeErrorKind::StateConflict,
            format!("Binance symbol {} is not trading", wire.symbol),
        ));
    }
    if wire.contract_type != "PERPETUAL" {
        return Err(ExchangeError::new(
            ExchangeErrorKind::Unsupported,
            format!(
                "Binance symbol {} has unsupported contract type {}",
                wire.symbol, wire.contract_type
            ),
        ));
    }

    let tick_size = parse_decimal(
        "exchangeInfo PRICE_FILTER.tickSize",
        wire.price_tick_size().ok_or_else(|| {
            ExchangeError::new(
                ExchangeErrorKind::InvalidResponse,
                format!("Binance symbol {} has no PRICE_FILTER", wire.symbol),
            )
        })?,
    )?;
    let (step_size, min_quantity, max_quantity) = wire.lot_size().ok_or_else(|| {
        ExchangeError::new(
            ExchangeErrorKind::InvalidResponse,
            format!("Binance symbol {} has no LOT_SIZE filter", wire.symbol),
        )
    })?;

    InstrumentSpec::new(
        *requested_symbol,
        MarketKind::LinearPerpetual,
        tick_size,
        parse_decimal("exchangeInfo LOT_SIZE.stepSize", step_size)?,
        parse_decimal("exchangeInfo LOT_SIZE.minQty", min_quantity)?,
        parse_decimal("exchangeInfo LOT_SIZE.maxQty", max_quantity)?,
    )
    .map_err(|error| invalid_response("instrument rules", error))
}

pub(crate) fn rest_book(
    requested_symbol: &Symbol,
    spec: &InstrumentSpec,
    wire: BookTickerDto,
) -> ExchangeResult<BestBidAsk> {
    ensure_symbol(requested_symbol, &wire.symbol, "book ticker")?;
    book(requested_symbol, spec, &wire.bid_price, &wire.ask_price)
}

pub(crate) fn websocket_book(
    requested_symbol: &Symbol,
    spec: &InstrumentSpec,
    wire: BookTickerEventDto,
) -> ExchangeResult<BestBidAsk> {
    ensure_symbol(requested_symbol, wire.symbol, "bookTicker event")?;
    book(requested_symbol, spec, wire.bid_price, wire.ask_price)
}

fn book(
    symbol: &Symbol,
    spec: &InstrumentSpec,
    bid: &str,
    ask: &str,
) -> ExchangeResult<BestBidAsk> {
    let bid = price_ticks(spec, "best bid", bid)?;
    let ask = price_ticks(spec, "best ask", ask)?;
    BestBidAsk::new(*symbol, bid, ask).map_err(|error| invalid_response("best bid/ask", error))
}

pub(crate) fn position_mode(wire: DualSidePositionDto) -> PositionMode {
    if wire.dual_side_position {
        PositionMode::Hedge
    } else {
        PositionMode::OneWay
    }
}

pub(crate) fn ws_api_order_ack(
    expected_symbol: &Symbol,
    expected_client_id: &ClientOrderId,
    wire: WsApiOrderAckDto,
) -> ExchangeResult<PlaceOrderAck> {
    ensure_symbol(expected_symbol, &wire.symbol, "new order response")?;
    let wire_client_id = wire
        .client_order_id
        .parse::<u64>()
        .map_err(|error| invalid_response("client order ID", error))?;
    if wire_client_id != expected_client_id.get() {
        return Err(ExchangeError::new(
            ExchangeErrorKind::InvalidResponse,
            "Binance new order response contained a different client order ID",
        ));
    }
    Ok(PlaceOrderAck::new(
        *expected_symbol,
        *expected_client_id,
        exchange_order_id(wire.order_id)?,
    ))
}

pub(crate) fn ws_api_order(
    expected_symbol: &Symbol,
    spec: &InstrumentSpec,
    wire: WsApiOrderDto,
) -> ExchangeResult<OrderUpdate> {
    ensure_symbol(expected_symbol, &wire.symbol, "order response")?;
    normalized_order(
        *expected_symbol,
        &wire.client_order_id,
        wire.order_id,
        &wire.side,
        &wire.price,
        &wire.orig_qty,
        &wire.executed_qty,
        &wire.status,
        spec,
    )
}

pub(crate) fn websocket_order(
    spec: &InstrumentSpec,
    wire: OrderTradeEventDto,
) -> ExchangeResult<Option<OrderUpdate>> {
    if wire.order_type != "LIMIT" {
        return Ok(None);
    }
    let symbol =
        Symbol::new(wire.symbol).map_err(|error| invalid_response("order event symbol", error))?;
    normalized_order(
        symbol,
        wire.client_order_id,
        wire.order_id,
        wire.side,
        wire.price,
        wire.original_quantity,
        wire.cumulative_filled,
        wire.status,
        spec,
    )
    .map(Some)
}

#[allow(clippy::too_many_arguments)]
fn normalized_order(
    symbol: Symbol,
    client_id: &str,
    order_id: u64,
    side: &str,
    price: &str,
    original_quantity: &str,
    cumulative_filled: &str,
    status: &str,
    spec: &InstrumentSpec,
) -> ExchangeResult<OrderUpdate> {
    if spec.symbol() != &symbol {
        return Err(ExchangeError::new(
            ExchangeErrorKind::InvalidResponse,
            "instrument rules do not match Binance order symbol",
        ));
    }

    let original_quantity = quantity_lots(spec, "original order quantity", original_quantity)?;
    let cumulative_filled = filled_lots(spec, "cumulative filled quantity", cumulative_filled)?;
    OrderUpdate::new(
        symbol,
        ClientOrderId::new(
            client_id
                .parse::<u64>()
                .map_err(|error| invalid_response("client order ID", error))?,
        )
        .map_err(|error| invalid_response("client order ID", error))?,
        exchange_order_id(order_id)?,
        parse_side(side)?,
        price_ticks(spec, "order price", price)?,
        original_quantity,
        cumulative_filled,
        parse_status(status)?,
    )
    .map_err(|error| invalid_response("order update", error))
}

fn parse_side(value: &str) -> ExchangeResult<Side> {
    match value {
        "BUY" => Ok(Side::Buy),
        "SELL" => Ok(Side::Sell),
        _ => Err(ExchangeError::new(
            ExchangeErrorKind::InvalidResponse,
            format!("unknown Binance order side {value:?}"),
        )),
    }
}

fn parse_status(value: &str) -> ExchangeResult<OrderStatus> {
    match value {
        "NEW" => Ok(OrderStatus::Accepted),
        "PARTIALLY_FILLED" => Ok(OrderStatus::PartiallyFilled),
        "FILLED" => Ok(OrderStatus::Filled),
        "CANCELED" => Ok(OrderStatus::Canceled),
        "REJECTED" => Ok(OrderStatus::Rejected),
        "EXPIRED" | "EXPIRED_IN_MATCH" => Ok(OrderStatus::Expired),
        _ => Err(ExchangeError::new(
            ExchangeErrorKind::InvalidResponse,
            format!("unknown Binance order status {value:?}"),
        )),
    }
}

fn price_ticks(
    spec: &InstrumentSpec,
    field: &'static str,
    value: &str,
) -> ExchangeResult<PriceTicks> {
    spec.price_to_ticks_exact(parse_decimal(field, value)?)
        .map_err(|error| invalid_response(field, error))
}

fn quantity_lots(
    spec: &InstrumentSpec,
    field: &'static str,
    value: &str,
) -> ExchangeResult<QuantityLots> {
    spec.quantity_to_lots_exact(parse_decimal(field, value)?)
        .map_err(|error| invalid_response(field, error))
}

fn filled_lots(
    spec: &InstrumentSpec,
    field: &'static str,
    value: &str,
) -> ExchangeResult<FilledLots> {
    let value = parse_decimal(field, value)?;
    if value.is_zero() {
        return Ok(FilledLots::ZERO);
    }
    if value.is_sign_negative() {
        return Err(ExchangeError::new(
            ExchangeErrorKind::InvalidResponse,
            format!("Binance {field} cannot be negative"),
        ));
    }
    let units = value.checked_div(spec.quantity_step()).ok_or_else(|| {
        ExchangeError::new(
            ExchangeErrorKind::InvalidResponse,
            format!("Binance {field} overflowed during lot conversion"),
        )
    })?;
    if !units.fract().is_zero() {
        return Err(ExchangeError::new(
            ExchangeErrorKind::InvalidResponse,
            format!(
                "Binance {field} {value} is not aligned to {}",
                spec.quantity_step()
            ),
        ));
    }
    let units = units.to_u64().ok_or_else(|| {
        ExchangeError::new(
            ExchangeErrorKind::InvalidResponse,
            format!("Binance {field} cannot be represented as integer lots"),
        )
    })?;
    Ok(FilledLots::new(units))
}

fn parse_decimal(field: &str, value: &str) -> ExchangeResult<Decimal> {
    Decimal::from_str(value).map_err(|error| {
        ExchangeError::new(
            ExchangeErrorKind::InvalidResponse,
            format!("invalid Binance {field} {value:?}: {error}"),
        )
    })
}

fn exchange_order_id(value: u64) -> ExchangeResult<ExchangeOrderId> {
    ExchangeOrderId::new(value).map_err(|error| invalid_response("exchange order ID", error))
}

fn ensure_symbol(expected: &Symbol, actual: &str, context: &str) -> ExchangeResult<()> {
    if expected.as_str() != actual {
        return Err(ExchangeError::new(
            ExchangeErrorKind::InvalidResponse,
            format!(
                "Binance {context} symbol mismatch: expected {}, got {actual}",
                expected
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use maker_domain::MarketKind;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::models::PrivateEventDto;

    fn symbol() -> Symbol {
        Symbol::new("BTCUSDT").unwrap()
    }

    fn spec() -> InstrumentSpec {
        InstrumentSpec::new(
            symbol(),
            MarketKind::LinearPerpetual,
            dec!(0.1),
            dec!(0.001),
            dec!(0.001),
            dec!(1000),
        )
        .unwrap()
    }

    #[test]
    fn maps_exchange_info_filters_exactly() {
        let json = r#"{
            "symbol":"BTCUSDT",
            "status":"TRADING",
            "contractType":"PERPETUAL",
            "filters":[
                {"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000000","tickSize":"0.10"},
                {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"1000","stepSize":"0.001"},
                {"filterType":"MIN_NOTIONAL","notional":"5"}
            ]
        }"#;
        let wire: ExchangeSymbolDto = serde_json::from_str(json).unwrap();

        let mapped = instrument(&symbol(), wire).unwrap();

        assert_eq!(mapped.tick_size(), dec!(0.10));
        assert_eq!(mapped.quantity_step(), dec!(0.001));
        assert_eq!(mapped.min_quantity().get(), 1);
        assert_eq!(mapped.max_quantity().get(), 1_000_000);
    }

    #[test]
    fn maps_book_ticker_without_floating_point_rounding() {
        let wire: BookTickerEventDto =
            serde_json::from_str(r#"{"e":"bookTicker","E":1700000000001,"T":1700000000000,"s":"BTCUSDT","b":"64000.1","a":"64000.2"}"#)
                .unwrap();

        let mapped = websocket_book(&symbol(), &spec(), wire).unwrap();

        assert_eq!(mapped.bid().get(), 640_001);
        assert_eq!(mapped.ask().get(), 640_002);
    }

    #[test]
    fn maps_order_trade_update_fixture() {
        let json = r#"{
            "e":"ORDER_TRADE_UPDATE",
            "E":1700000000000,
            "T":1700000000000,
            "o":{
                "s":"BTCUSDT","c":"12345","S":"SELL","o":"LIMIT",
                "f":"GTX","q":"0.003","p":"64000.1","ap":"64000.1",
                "x":"TRADE","X":"FILLED","i":987654321,"l":"0.003","z":"0.003"
            }
        }"#;
        let event: PrivateEventDto = serde_json::from_str(json).unwrap();
        let PrivateEventDto::OrderTradeUpdate {
            event_time,
            transaction_time,
            order,
        } = event
        else {
            panic!("expected order event");
        };
        assert_eq!(event_time, 1_700_000_000_000);
        assert_eq!(transaction_time, 1_700_000_000_000);

        let update = websocket_order(&spec(), order).unwrap().unwrap();

        assert_eq!(update.symbol(), &symbol());
        assert_eq!(update.client_order_id().get(), 12345);
        assert_eq!(update.exchange_order_id().get(), 987654321);
        assert_eq!(update.side(), Side::Sell);
        assert_eq!(update.price().get(), 640_001);
        assert_eq!(update.original_quantity().get(), 3);
        assert_eq!(update.cumulative_filled().get(), 3);
        assert_eq!(update.status(), OrderStatus::Filled);
    }

    #[test]
    fn permits_step_aligned_partial_fill_below_min_order_quantity() {
        let spec = InstrumentSpec::new(
            symbol(),
            MarketKind::LinearPerpetual,
            dec!(0.1),
            dec!(0.001),
            dec!(0.010),
            dec!(1000),
        )
        .unwrap();
        let json = r#"{
            "s":"BTCUSDT","c":"12346","S":"BUY","o":"LIMIT",
            "q":"0.010","p":"64000.1","z":"0.001","X":"PARTIALLY_FILLED","i":7
        }"#;
        let wire: OrderTradeEventDto = serde_json::from_str(json).unwrap();

        let update = websocket_order(&spec, wire).unwrap().unwrap();

        assert_eq!(update.cumulative_filled().get(), 1);
        assert_eq!(update.status(), OrderStatus::PartiallyFilled);
    }

    #[test]
    fn ignores_non_limit_private_orders() {
        let json = r#"{
            "s":"BTCUSDT","c":"manual-market","S":"BUY","o":"MARKET",
            "q":"0.010","p":"0","z":"0.010","X":"FILLED","i":8
        }"#;
        let wire: OrderTradeEventDto = serde_json::from_str(json).unwrap();

        assert_eq!(websocket_order(&spec(), wire).unwrap(), None);
    }
}
