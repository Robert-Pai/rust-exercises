use std::str::FromStr;

use maker_domain::{
    BestBidAsk, ClientOrderId, ExchangeOrderId, FilledLots, InstrumentSpec, MarketKind,
    OrderStatus, OrderUpdate, PriceTicks, QuantityLots, Side, Symbol,
};
use maker_ports::{
    AccountBalance, AccountPosition, AccountPositionSide, AccountSnapshot, AccountUpdate,
    AccountUpdateReason, BalanceUpdate, MarginType, OrderTradeExecution, PositionUpdate,
    TradeLiteExecution,
};
use maker_ports::{ExchangeError, ExchangeErrorKind, ExchangeResult, PlaceOrderAck, PositionMode};
use rust_decimal::{Decimal, prelude::ToPrimitive};

use crate::{
    error::invalid_response,
    models::{
        AccountUpdateDto, BookTickerDto, BookTickerEventDto, DualSidePositionDto,
        ExchangeSymbolDto, OrderTradeEventDto, TradeLiteEventDto, WsApiAccountBalanceDto,
        WsApiAccountPositionDto, WsApiAccountStatusDto, WsApiOrderAckDto, WsApiOrderDto,
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

pub(crate) fn account_snapshot(
    balances: Vec<WsApiAccountBalanceDto>,
    status: WsApiAccountStatusDto,
) -> ExchangeResult<AccountSnapshot> {
    let balances = balances
        .into_iter()
        .map(account_balance)
        .collect::<ExchangeResult<Vec<_>>>()?;
    let positions = status
        .positions
        .into_iter()
        .map(account_position)
        .collect::<ExchangeResult<Vec<_>>>()?;
    Ok(AccountSnapshot {
        balances,
        positions,
    })
}

fn account_balance(wire: WsApiAccountBalanceDto) -> ExchangeResult<AccountBalance> {
    Ok(AccountBalance {
        asset: parse_symbol("account balance asset", &wire.asset)?,
        wallet_balance: parse_decimal("account wallet balance", &wire.balance)?,
        cross_wallet_balance: parse_decimal(
            "account cross wallet balance",
            &wire.cross_wallet_balance,
        )?,
        cross_unrealized_pnl: parse_decimal("account cross unrealized PnL", &wire.cross_un_pnl)?,
        available_balance: parse_decimal("account available balance", &wire.available_balance)?,
        max_withdraw_amount: parse_decimal(
            "account maximum withdraw amount",
            &wire.max_withdraw_amount,
        )?,
        margin_available: wire.margin_available,
        update_time_ms: wire.update_time,
    })
}

fn account_position(wire: WsApiAccountPositionDto) -> ExchangeResult<AccountPosition> {
    Ok(AccountPosition {
        symbol: parse_symbol("account position symbol", &wire.symbol)?,
        position_side: parse_position_side(&wire.position_side)?,
        position_amount: parse_decimal("account position amount", &wire.position_amt)?,
        unrealized_pnl: parse_decimal("account position unrealized PnL", &wire.unrealized_profit)?,
        isolated_margin: parse_decimal("account isolated margin", &wire.isolated_margin)?,
        notional: parse_decimal("account position notional", &wire.notional)?,
        isolated_wallet: parse_decimal("account isolated wallet", &wire.isolated_wallet)?,
        initial_margin: parse_decimal("account initial margin", &wire.initial_margin)?,
        maintenance_margin: parse_decimal("account maintenance margin", &wire.maint_margin)?,
        update_time_ms: wire.update_time,
    })
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
    wire: &OrderTradeEventDto<'_>,
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

pub(crate) fn websocket_order_trade(
    update: OrderUpdate,
    event_time_ms: u64,
    transaction_time_ms: u64,
    wire: &OrderTradeEventDto<'_>,
) -> ExchangeResult<Option<OrderTradeExecution>> {
    if wire.execution_type != "TRADE" {
        return Ok(None);
    }
    let (commission_asset, commission) = match (wire.commission_asset, wire.commission) {
        (Some(asset), Some(commission)) => (
            Some(parse_symbol("trade commission asset", asset)?),
            Some(parse_decimal("trade commission", commission)?),
        ),
        (None, None) => (None, None),
        _ => {
            return Err(ExchangeError::new(
                ExchangeErrorKind::InvalidResponse,
                "Binance trade event supplied only one commission field",
            ));
        }
    };
    Ok(Some(OrderTradeExecution {
        update,
        event_time_ms,
        transaction_time_ms,
        trade_time_ms: wire.trade_time,
        average_price: parse_decimal("trade average price", wire.average_price)?,
        last_filled_price: parse_decimal("trade last filled price", wire.last_filled_price)?,
        last_filled_quantity: parse_decimal(
            "trade last filled quantity",
            wire.last_filled_quantity,
        )?,
        cumulative_filled_quantity: parse_decimal(
            "trade cumulative filled quantity",
            wire.cumulative_filled,
        )?,
        commission_asset,
        commission,
        trade_id: wire.trade_id,
        realized_pnl: parse_decimal("trade realized PnL", wire.realized_pnl)?,
        maker: wire.maker,
    }))
}

pub(crate) fn websocket_account_update(
    event_time_ms: u64,
    transaction_time_ms: u64,
    wire: AccountUpdateDto<'_>,
) -> ExchangeResult<AccountUpdate> {
    let balances = wire
        .balances
        .into_iter()
        .map(|balance| {
            Ok(BalanceUpdate {
                asset: parse_symbol("balance update asset", balance.asset)?,
                wallet_balance: parse_decimal(
                    "balance update wallet balance",
                    balance.wallet_balance,
                )?,
                cross_wallet_balance: parse_decimal(
                    "balance update cross wallet balance",
                    balance.cross_wallet_balance,
                )?,
                balance_change: parse_decimal(
                    "balance update balance change",
                    balance.balance_change,
                )?,
            })
        })
        .collect::<ExchangeResult<Vec<_>>>()?;
    let positions = wire
        .positions
        .into_iter()
        .map(|position| {
            Ok(PositionUpdate {
                symbol: parse_symbol("position update symbol", position.symbol)?,
                position_amount: parse_decimal("position update amount", position.position_amount)?,
                entry_price: parse_decimal("position update entry price", position.entry_price)?,
                breakeven_price: parse_decimal(
                    "position update breakeven price",
                    position.breakeven_price,
                )?,
                accumulated_realized_pnl: parse_decimal(
                    "position update accumulated realized PnL",
                    position.accumulated_realized_pnl,
                )?,
                unrealized_pnl: parse_decimal(
                    "position update unrealized PnL",
                    position.unrealized_pnl,
                )?,
                margin_type: parse_margin_type(position.margin_type)?,
                isolated_wallet: parse_decimal(
                    "position update isolated wallet",
                    position.isolated_wallet,
                )?,
                position_side: parse_position_side(position.position_side)?,
            })
        })
        .collect::<ExchangeResult<Vec<_>>>()?;
    Ok(AccountUpdate {
        event_time_ms,
        transaction_time_ms,
        reason: parse_account_update_reason(wire.reason),
        balances,
        positions,
    })
}

pub(crate) fn websocket_trade_lite(
    event_time_ms: u64,
    transaction_time_ms: u64,
    wire: &TradeLiteEventDto<'_>,
) -> ExchangeResult<TradeLiteExecution> {
    Ok(TradeLiteExecution {
        symbol: parse_symbol("Trade Lite symbol", wire.symbol)?,
        client_order_id: parse_client_order_id(wire.client_order_id)?,
        exchange_order_id: exchange_order_id(wire.order_id)?,
        side: parse_side(wire.side)?,
        event_time_ms,
        transaction_time_ms,
        original_quantity: parse_decimal("Trade Lite original quantity", wire.original_quantity)?,
        original_price: parse_decimal("Trade Lite original price", wire.original_price)?,
        last_filled_price: parse_decimal("Trade Lite last filled price", wire.last_filled_price)?,
        last_filled_quantity: parse_decimal(
            "Trade Lite last filled quantity",
            wire.last_filled_quantity,
        )?,
        trade_id: wire.trade_id,
        maker: wire.maker,
    })
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
        parse_client_order_id(client_id)?,
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

fn parse_client_order_id(value: &str) -> ExchangeResult<ClientOrderId> {
    ClientOrderId::new(
        value
            .parse::<u64>()
            .map_err(|error| invalid_response("client order ID", error))?,
    )
    .map_err(|error| invalid_response("client order ID", error))
}

fn parse_symbol(field: &str, value: &str) -> ExchangeResult<Symbol> {
    Symbol::new(value).map_err(|error| invalid_response(field, error))
}

fn parse_position_side(value: &str) -> ExchangeResult<AccountPositionSide> {
    match value {
        "BOTH" => Ok(AccountPositionSide::Both),
        "LONG" => Ok(AccountPositionSide::Long),
        "SHORT" => Ok(AccountPositionSide::Short),
        _ => Err(ExchangeError::new(
            ExchangeErrorKind::InvalidResponse,
            format!("unknown Binance position side {value:?}"),
        )),
    }
}

fn parse_margin_type(value: &str) -> ExchangeResult<MarginType> {
    match value {
        "cross" | "crossed" => Ok(MarginType::Cross),
        "isolated" => Ok(MarginType::Isolated),
        _ => Err(ExchangeError::new(
            ExchangeErrorKind::InvalidResponse,
            format!("unknown Binance margin type {value:?}"),
        )),
    }
}

fn parse_account_update_reason(value: &str) -> AccountUpdateReason {
    match value {
        "DEPOSIT" => AccountUpdateReason::Deposit,
        "WITHDRAW" => AccountUpdateReason::Withdraw,
        "ORDER" => AccountUpdateReason::Order,
        "FUNDING_FEE" => AccountUpdateReason::FundingFee,
        "WITHDRAW_REJECT" => AccountUpdateReason::WithdrawReject,
        "ADJUSTMENT" => AccountUpdateReason::Adjustment,
        "INSURANCE_CLEAR" => AccountUpdateReason::InsuranceClear,
        "ADMIN_DEPOSIT" => AccountUpdateReason::AdminDeposit,
        "ADMIN_WITHDRAW" => AccountUpdateReason::AdminWithdraw,
        "MARGIN_TRANSFER" => AccountUpdateReason::MarginTransfer,
        "MARGIN_TYPE_CHANGE" => AccountUpdateReason::MarginTypeChange,
        "ASSET_TRANSFER" => AccountUpdateReason::AssetTransfer,
        "OPTIONS_PREMIUM_FEE" => AccountUpdateReason::OptionsPremiumFee,
        "OPTIONS_SETTLE_PROFIT" => AccountUpdateReason::OptionsSettleProfit,
        "AUTO_EXCHANGE" => AccountUpdateReason::AutoExchange,
        "COIN_SWAP_DEPOSIT" => AccountUpdateReason::CoinSwapDeposit,
        "COIN_SWAP_WITHDRAW" => AccountUpdateReason::CoinSwapWithdraw,
        _ => AccountUpdateReason::Other,
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
                "x":"TRADE","X":"FILLED","i":987654321,"l":"0.003","z":"0.003",
                "L":"64000.1","N":"USDT","n":"0.0768","T":1700000000000,
                "t":444,"m":true,"rp":"1.25"
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

        let update = websocket_order(&spec(), &order).unwrap().unwrap();

        assert_eq!(update.symbol(), &symbol());
        assert_eq!(update.client_order_id().get(), 12345);
        assert_eq!(update.exchange_order_id().get(), 987654321);
        assert_eq!(update.side(), Side::Sell);
        assert_eq!(update.price().get(), 640_001);
        assert_eq!(update.original_quantity().get(), 3);
        assert_eq!(update.cumulative_filled().get(), 3);
        assert_eq!(update.status(), OrderStatus::Filled);

        let trade = websocket_order_trade(update, event_time, transaction_time, &order)
            .unwrap()
            .unwrap();
        assert_eq!(trade.last_filled_quantity, dec!(0.003));
        assert_eq!(trade.commission_asset, Some(Symbol::new("USDT").unwrap()));
        assert_eq!(trade.commission, Some(dec!(0.0768)));
        assert_eq!(trade.trade_id, 444);
        assert_eq!(trade.realized_pnl, dec!(1.25));
        assert!(trade.maker);
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
            "q":"0.010","p":"64000.1","ap":"64000.1","x":"TRADE",
            "z":"0.001","L":"64000.1","l":"0.001","T":1,"t":1,"m":true,
            "rp":"0","X":"PARTIALLY_FILLED","i":7
        }"#;
        let wire: OrderTradeEventDto = serde_json::from_str(json).unwrap();

        let update = websocket_order(&spec, &wire).unwrap().unwrap();

        assert_eq!(update.cumulative_filled().get(), 1);
        assert_eq!(update.status(), OrderStatus::PartiallyFilled);
    }

    #[test]
    fn ignores_non_limit_private_orders() {
        let json = r#"{
            "s":"BTCUSDT","c":"manual-market","S":"BUY","o":"MARKET",
            "q":"0.010","p":"0","ap":"64000","x":"TRADE","z":"0.010",
            "L":"64000","l":"0.010","T":1,"t":1,"m":false,"rp":"0",
            "X":"FILLED","i":8
        }"#;
        let wire: OrderTradeEventDto = serde_json::from_str(json).unwrap();

        assert_eq!(websocket_order(&spec(), &wire).unwrap(), None);
    }

    #[test]
    fn maps_account_update_fixture() {
        let json = r#"{
            "e":"ACCOUNT_UPDATE","E":1564745798939,"T":1564745798938,
            "a":{"m":"ORDER","B":[
                {"a":"USDT","wb":"122624.12345678","cw":"100.12345678","bc":"50.12345678"}
            ],"P":[
                {"s":"BTCUSDT","pa":"0.001","ep":"64000","bep":"64001",
                 "cr":"200","up":"1.5","mt":"isolated","iw":"10","ps":"BOTH"}
            ]}
        }"#;
        let event: PrivateEventDto = serde_json::from_str(json).unwrap();
        let PrivateEventDto::AccountUpdate {
            event_time,
            transaction_time,
            account,
        } = event
        else {
            panic!("expected account update");
        };

        let update = websocket_account_update(event_time, transaction_time, account).unwrap();

        assert_eq!(update.reason, AccountUpdateReason::Order);
        assert_eq!(update.balances[0].wallet_balance, dec!(122624.12345678));
        assert_eq!(update.positions[0].position_amount, dec!(0.001));
        assert_eq!(update.positions[0].margin_type, MarginType::Isolated);
        assert_eq!(update.positions[0].position_side, AccountPositionSide::Both);
    }

    #[test]
    fn maps_trade_lite_fixture() {
        let json = r#"{
            "e":"TRADE_LITE","E":1721895408092,"T":1721895408214,
            "s":"BTCUSDT","q":"0.001","p":"0","m":false,"c":"12345",
            "S":"BUY","L":"64089.20","l":"0.040","t":109100866,"i":8886774
        }"#;
        let event: PrivateEventDto = serde_json::from_str(json).unwrap();
        let PrivateEventDto::TradeLite {
            event_time,
            transaction_time,
            trade,
        } = event
        else {
            panic!("expected Trade Lite event");
        };

        let trade = websocket_trade_lite(event_time, transaction_time, &trade).unwrap();

        assert_eq!(trade.symbol, symbol());
        assert_eq!(trade.client_order_id.get(), 12345);
        assert_eq!(trade.last_filled_price, dec!(64089.20));
        assert_eq!(trade.last_filled_quantity, dec!(0.040));
        assert_eq!(trade.trade_id, 109100866);
        assert!(!trade.maker);
    }

    #[test]
    fn maps_v2_account_snapshot_fixture() {
        let balances: Vec<WsApiAccountBalanceDto> = serde_json::from_str(
            r#"[{"asset":"USDT","balance":"122607.35","crossWalletBalance":"23.72",
                "crossUnPnl":"1.25","availableBalance":"20","maxWithdrawAmount":"19",
                "marginAvailable":true,"updateTime":1617939110373}]"#,
        )
        .unwrap();
        let status: WsApiAccountStatusDto = serde_json::from_str(
            r#"{"positions":[{"symbol":"BTCUSDT","positionSide":"BOTH",
                "positionAmt":"0.001","unrealizedProfit":"1.5","isolatedMargin":"0",
                "notional":"64","isolatedWallet":"0","initialMargin":"6.4",
                "maintMargin":"0.4","updateTime":1625474304765}]}"#,
        )
        .unwrap();

        let snapshot = account_snapshot(balances, status).unwrap();

        assert_eq!(snapshot.balances[0].available_balance, dec!(20));
        assert_eq!(snapshot.positions[0].position_amount, dec!(0.001));
        assert_eq!(
            snapshot.positions[0].position_side,
            AccountPositionSide::Both
        );
    }
}
