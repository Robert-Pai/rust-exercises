use std::{sync::Arc, time::Duration};

use crate::{
    config::{BinanceCredentials, BinanceUsdmConfig},
    mapping,
    network::{NetworkRole, NetworkRuntime},
    rest::RestClient,
    websocket,
    ws_api::WsApiClient,
};
use maker_domain::{BestBidAsk, ClientOrderId, InstrumentSpec, OrderIntent, OrderUpdate, Symbol};
use maker_ports::{
    CancelOutcome, EventStream, ExchangeError, ExchangeErrorKind, ExchangeFuture, ExchangeResult,
    InstrumentPort, LatestBboSubscription, MarketDataPort, OrderEventPort, PlaceOrderAck,
    PositionMode, TradingPort,
};

/// Move-only Binance USD-M Futures session owned by the strategy thread.
pub struct BinanceUsdm {
    rest: RestClient,
    market_network: NetworkRuntime,
    trading_network: NetworkRuntime,
    trading: WsApiClient,
    websocket_url: Arc<str>,
    websocket_connect_timeout: Duration,
    websocket_idle_timeout: Duration,
    listen_key_keepalive: Duration,
    instrument: Option<InstrumentSpec>,
}

impl BinanceUsdm {
    pub fn new(config: BinanceUsdmConfig, credentials: BinanceCredentials) -> ExchangeResult<Self> {
        let rest = RestClient::new(&config, credentials.clone())?;
        let market_network = NetworkRuntime::new(NetworkRole::MarketData, &config)?;
        let mut trading_network = NetworkRuntime::new(NetworkRole::Trading, &config)?;
        let trading = WsApiClient::new(&config, credentials, rest.clone(), &mut trading_network)?;
        Ok(Self {
            rest,
            market_network,
            trading_network,
            trading,
            websocket_url: Arc::from(config.websocket_url()),
            websocket_connect_timeout: config.request_timeout(),
            websocket_idle_timeout: config.websocket_idle_timeout(),
            listen_key_keepalive: config.listen_key_keepalive(),
            instrument: None,
        })
    }

    pub(crate) fn cached_instrument_spec(&self, symbol: &Symbol) -> ExchangeResult<InstrumentSpec> {
        self.instrument
            .as_ref()
            .filter(|spec| spec.symbol() == symbol)
            .cloned()
            .ok_or_else(|| {
                ExchangeError::new(
                    ExchangeErrorKind::StateConflict,
                    format!("instrument specification for {symbol} was not initialized"),
                )
            })
    }

    fn replace_instrument_spec(&mut self, spec: InstrumentSpec) {
        self.instrument = Some(spec);
    }
}

impl InstrumentPort for BinanceUsdm {
    fn start(&mut self) -> ExchangeResult<()> {
        Ok(())
    }

    fn instrument_spec(&mut self, symbol: Symbol) -> ExchangeFuture<InstrumentSpec> {
        if let Ok(spec) = self.cached_instrument_spec(&symbol) {
            return Box::pin(async move { Ok(spec) });
        }
        let rest = self.rest.clone();
        self.market_network.call(async move {
            let wire = rest.exchange_symbol(&symbol).await?;
            mapping::instrument(&symbol, wire)
        })
    }

    fn refresh_instrument_spec(&mut self, symbol: Symbol) -> ExchangeFuture<InstrumentSpec> {
        let rest = self.rest.clone();
        self.market_network.call(async move {
            let wire = rest.exchange_symbol(&symbol).await?;
            mapping::instrument(&symbol, wire)
        })
    }

    fn apply_instrument_spec(&mut self, spec: InstrumentSpec) -> ExchangeResult<()> {
        self.replace_instrument_spec(spec);
        Ok(())
    }

    fn position_mode(&mut self) -> ExchangeFuture<PositionMode> {
        let rest = self.rest.clone();
        self.trading_network
            .call(async move { rest.position_mode().await.map(mapping::position_mode) })
    }
}

impl MarketDataPort for BinanceUsdm {
    fn best_bid_ask(&mut self, symbol: Symbol) -> ExchangeFuture<BestBidAsk> {
        let spec = self.cached_instrument_spec(&symbol);
        let rest = self.rest.clone();
        self.market_network.call(async move {
            let spec = spec?;
            let wire = rest.book_ticker(&symbol).await?;
            mapping::rest_book(&symbol, &spec, wire)
        })
    }

    fn subscribe_best_bid_ask(
        &mut self,
        symbol: Symbol,
        initial: BestBidAsk,
    ) -> ExchangeFuture<LatestBboSubscription> {
        let spec = self.cached_instrument_spec(&symbol);
        let websocket_url = self.websocket_url.clone();
        let connect_timeout = self.websocket_connect_timeout;
        let idle_timeout = self.websocket_idle_timeout;
        self.market_network.call(async move {
            websocket::subscribe_book_ticker(
                &websocket_url,
                symbol,
                spec?,
                connect_timeout,
                idle_timeout,
                initial,
            )
            .await
        })
    }
}

impl TradingPort for BinanceUsdm {
    fn place_post_only(&mut self, intent: OrderIntent) -> ExchangeFuture<PlaceOrderAck> {
        let symbol = *intent.symbol();
        let client_order_id = *intent.client_order_id();
        let spec = match self.cached_instrument_spec(&symbol) {
            Ok(spec) => spec,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let request = self.trading.place_order(&spec, &intent);
        Box::pin(async move {
            let wire = request.await?;
            mapping::ws_api_order_ack(&symbol, &client_order_id, wire)
        })
    }

    fn cancel_order(
        &mut self,
        symbol: Symbol,
        client_order_id: ClientOrderId,
    ) -> ExchangeFuture<CancelOutcome> {
        let spec = match self.cached_instrument_spec(&symbol) {
            Ok(spec) => spec,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let cancel = self.trading.cancel_order(&symbol, &client_order_id);
        Box::pin(async move {
            match cancel.await {
                Ok(Some(order)) => {
                    let update = mapping::ws_api_order(&symbol, &spec, order)?;
                    terminal_cancel_outcome(update)
                }
                Ok(None) => Ok(CancelOutcome::NotFound),
                Err(error) => Err(error),
            }
        })
    }

    fn cancel_all(&mut self, symbol: Symbol) -> ExchangeFuture<()> {
        let rest = self.rest.clone();
        self.trading_network
            .call(async move { rest.cancel_all(&symbol).await })
    }
}

impl OrderEventPort for BinanceUsdm {
    fn subscribe_order_updates(
        &mut self,
        symbol: Symbol,
    ) -> ExchangeFuture<EventStream<OrderUpdate>> {
        let spec = self.cached_instrument_spec(&symbol);
        let rest = self.rest.clone();
        let websocket_url = self.websocket_url.clone();
        let connect_timeout = self.websocket_connect_timeout;
        let idle_timeout = self.websocket_idle_timeout;
        let keepalive = self.listen_key_keepalive;
        self.trading_network.call(async move {
            websocket::subscribe_order_updates(
                rest,
                symbol,
                spec?,
                &websocket_url,
                connect_timeout,
                idle_timeout,
                keepalive,
            )
            .await
        })
    }
}

fn terminal_cancel_outcome(update: OrderUpdate) -> ExchangeResult<CancelOutcome> {
    if update.status().is_terminal() {
        Ok(CancelOutcome::Terminal(update))
    } else {
        Err(ExchangeError::new(
            ExchangeErrorKind::StateConflict,
            format!(
                "Binance cancel resolution is not terminal: {:?}",
                update.status()
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use maker_domain::{
        ClientOrderId, ExchangeOrderId, FilledLots, MarketKind, OrderStatus, PriceTicks,
        QuantityLots, Side,
    };
    use rust_decimal::Decimal;
    use secrecy::SecretString;

    use super::*;

    #[test]
    fn accepts_terminal_cancel_resolution() {
        let update = OrderUpdate::new(
            Symbol::new("BTCUSDT").unwrap(),
            ClientOrderId::new(1).unwrap(),
            ExchangeOrderId::new(1).unwrap(),
            Side::Buy,
            PriceTicks::new(100).unwrap(),
            QuantityLots::new(1).unwrap(),
            FilledLots::ZERO,
            OrderStatus::Canceled,
        )
        .unwrap();

        assert_eq!(
            terminal_cancel_outcome(update).unwrap(),
            CancelOutcome::Terminal(update)
        );
    }

    #[test]
    fn rejects_non_terminal_cancel_resolution() {
        let update = OrderUpdate::new(
            Symbol::new("BTCUSDT").unwrap(),
            ClientOrderId::new(1).unwrap(),
            ExchangeOrderId::new(1).unwrap(),
            Side::Buy,
            PriceTicks::new(100).unwrap(),
            QuantityLots::new(1).unwrap(),
            FilledLots::ZERO,
            OrderStatus::Accepted,
        )
        .unwrap();

        assert_eq!(
            terminal_cancel_outcome(update).unwrap_err().kind(),
            ExchangeErrorKind::StateConflict
        );
    }

    #[test]
    fn instrument_snapshot_requires_initialization_then_reads_without_a_lock() {
        let credentials = BinanceCredentials::new(
            SecretString::new("test-api-key".to_owned()),
            SecretString::new(crate::config::TEST_PRIVATE_KEY_PEM.to_owned()),
        )
        .unwrap();
        let mut adapter = BinanceUsdm::new(BinanceUsdmConfig::default(), credentials).unwrap();
        let symbol = Symbol::new("BTCUSDT").unwrap();

        assert_eq!(
            adapter.cached_instrument_spec(&symbol).unwrap_err().kind(),
            ExchangeErrorKind::StateConflict
        );

        let spec = InstrumentSpec::new(
            symbol,
            MarketKind::LinearPerpetual,
            Decimal::ONE,
            Decimal::new(1, 3),
            Decimal::new(1, 3),
            Decimal::ONE,
        )
        .unwrap();
        adapter.replace_instrument_spec(spec.clone());

        assert_eq!(adapter.cached_instrument_spec(&symbol).unwrap(), spec);
    }
}
