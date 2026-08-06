use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::{
    config::{BinanceCredentials, BinanceUsdmConfig},
    mapping,
    network::NetworkRuntime,
    rest::RestClient,
    websocket,
    ws_api::WsApiClient,
};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use maker_domain::{BestBidAsk, ClientOrderId, InstrumentSpec, OrderIntent, OrderUpdate, Symbol};
use maker_ports::{
    CancelOutcome, EventStream, ExchangeError, ExchangeErrorKind, ExchangeResult, InstrumentPort,
    MarketDataPort, OrderEventPort, PlaceOrderAck, PositionMode, TradingPort,
};

/// Binance USD-M Futures adapter implementing all exchange ports used by the
/// maker engine.
#[derive(Clone)]
pub struct BinanceUsdm {
    rest: RestClient,
    network: NetworkRuntime,
    trading: WsApiClient,
    websocket_url: Arc<str>,
    websocket_connect_timeout: Duration,
    websocket_idle_timeout: Duration,
    listen_key_keepalive: Duration,
    instruments: Arc<ArcSwap<HashMap<Symbol, InstrumentSpec>>>,
}

impl BinanceUsdm {
    pub fn new(config: BinanceUsdmConfig, credentials: BinanceCredentials) -> ExchangeResult<Self> {
        let rest = RestClient::new(&config, credentials.clone())?;
        let network = NetworkRuntime::new(config.network_mode(), config.network_cpu_core())?;
        let trading = WsApiClient::new(&config, credentials, rest.clone(), network.clone())?;
        Ok(Self {
            rest,
            network,
            trading,
            websocket_url: Arc::from(config.websocket_url()),
            websocket_connect_timeout: config.request_timeout(),
            websocket_idle_timeout: config.websocket_idle_timeout(),
            listen_key_keepalive: config.listen_key_keepalive(),
            instruments: Arc::new(ArcSwap::from_pointee(HashMap::new())),
        })
    }

    pub(crate) fn cached_instrument_spec(&self, symbol: &Symbol) -> ExchangeResult<InstrumentSpec> {
        self.instruments.load().get(symbol).cloned().ok_or_else(|| {
            ExchangeError::new(
                ExchangeErrorKind::StateConflict,
                format!("instrument specification for {symbol} was not initialized"),
            )
        })
    }

    async fn load_instrument_spec(&self, symbol: &Symbol) -> ExchangeResult<InstrumentSpec> {
        let rest = self.rest.clone();
        let symbol_for_request = symbol.clone();
        let wire = self
            .network
            .call(async move { rest.exchange_symbol(&symbol_for_request).await })
            .await?;
        let spec = mapping::instrument(symbol, wire)?;
        self.replace_instrument_spec(spec.clone());
        Ok(spec)
    }

    fn replace_instrument_spec(&self, spec: InstrumentSpec) {
        let current = self.instruments.load_full();
        let mut updated = (*current).clone();
        updated.insert(spec.symbol().clone(), spec);
        self.instruments.store(Arc::new(updated));
    }

    async fn recover_cancel(
        &self,
        symbol: &Symbol,
        client_order_id: &ClientOrderId,
        spec: &InstrumentSpec,
    ) -> ExchangeResult<CancelOutcome> {
        match self.trading.query_order(symbol, client_order_id).await {
            Ok(order) => {
                let update = mapping::ws_api_order(symbol, spec, order)?;
                terminal_cancel_outcome(update)
            }
            Err(error) if error.exchange_code() == Some("-2013") => Ok(CancelOutcome::NotFound),
            Err(error) => Err(error),
        }
    }
}

#[async_trait]
impl InstrumentPort for BinanceUsdm {
    async fn instrument_spec(&self, symbol: &Symbol) -> ExchangeResult<InstrumentSpec> {
        match self.cached_instrument_spec(symbol) {
            Ok(spec) => Ok(spec),
            Err(_) => self.load_instrument_spec(symbol).await,
        }
    }

    async fn refresh_instrument_spec(&self, symbol: &Symbol) -> ExchangeResult<InstrumentSpec> {
        let rest = self.rest.clone();
        let symbol_for_request = symbol.clone();
        let wire = self
            .network
            .call(async move { rest.exchange_symbol(&symbol_for_request).await })
            .await?;
        mapping::instrument(symbol, wire)
    }

    fn apply_instrument_spec(&self, spec: InstrumentSpec) -> ExchangeResult<()> {
        self.replace_instrument_spec(spec);
        Ok(())
    }

    async fn position_mode(&self) -> ExchangeResult<PositionMode> {
        let rest = self.rest.clone();
        self.network
            .call(async move { rest.position_mode().await })
            .await
            .map(mapping::position_mode)
    }
}

#[async_trait]
impl MarketDataPort for BinanceUsdm {
    async fn best_bid_ask(&self, symbol: &Symbol) -> ExchangeResult<BestBidAsk> {
        let spec = self.cached_instrument_spec(symbol)?;
        let rest = self.rest.clone();
        let symbol_for_request = symbol.clone();
        let wire = self
            .network
            .call(async move { rest.book_ticker(&symbol_for_request).await })
            .await?;
        mapping::rest_book(symbol, &spec, wire)
    }

    async fn subscribe_best_bid_ask(
        &self,
        symbol: &Symbol,
    ) -> ExchangeResult<EventStream<BestBidAsk>> {
        let spec = self.cached_instrument_spec(symbol)?;
        websocket::subscribe_book_ticker(
            self.network.clone(),
            &self.websocket_url,
            symbol.clone(),
            spec,
            self.websocket_connect_timeout,
            self.websocket_idle_timeout,
        )
        .await
    }
}

#[async_trait]
impl TradingPort for BinanceUsdm {
    async fn place_post_only(&self, intent: OrderIntent) -> ExchangeResult<PlaceOrderAck> {
        let symbol = intent.symbol().clone();
        let client_order_id = intent.client_order_id().clone();
        let spec = self.cached_instrument_spec(&symbol)?;
        let wire = self.trading.place_order(&spec, &intent).await?;
        mapping::ws_api_order_ack(&symbol, &client_order_id, wire)
    }

    async fn cancel_order(
        &self,
        symbol: &Symbol,
        client_order_id: &ClientOrderId,
    ) -> ExchangeResult<CancelOutcome> {
        let spec = self.cached_instrument_spec(symbol)?;
        match self.trading.cancel_order(symbol, client_order_id).await {
            Ok(order) => {
                let update = mapping::ws_api_order(symbol, &spec, order)?;
                terminal_cancel_outcome(update)
            }
            Err(error) if matches!(error.exchange_code(), Some("-2011") | Some("-2013")) => {
                self.recover_cancel(symbol, client_order_id, &spec).await
            }
            Err(error) => Err(error),
        }
    }

    async fn cancel_all(&self, symbol: &Symbol) -> ExchangeResult<()> {
        self.trading.cancel_all(symbol).await
    }
}

#[async_trait]
impl OrderEventPort for BinanceUsdm {
    async fn subscribe_order_updates(
        &self,
        symbol: &Symbol,
    ) -> ExchangeResult<EventStream<OrderUpdate>> {
        websocket::subscribe_order_updates(
            self.network.clone(),
            self.clone(),
            self.rest.clone(),
            symbol.clone(),
            &self.websocket_url,
            self.websocket_connect_timeout,
            self.websocket_idle_timeout,
            self.listen_key_keepalive,
        )
        .await
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
            ClientOrderId::new("maker-1").unwrap(),
            ExchangeOrderId::new("1").unwrap(),
            Side::Buy,
            PriceTicks::new(100).unwrap(),
            QuantityLots::new(1).unwrap(),
            FilledLots::ZERO,
            OrderStatus::Canceled,
        )
        .unwrap();

        assert_eq!(
            terminal_cancel_outcome(update.clone()).unwrap(),
            CancelOutcome::Terminal(update)
        );
    }

    #[test]
    fn rejects_non_terminal_cancel_resolution() {
        let update = OrderUpdate::new(
            Symbol::new("BTCUSDT").unwrap(),
            ClientOrderId::new("maker-1").unwrap(),
            ExchangeOrderId::new("1").unwrap(),
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
        let adapter = BinanceUsdm::new(BinanceUsdmConfig::default(), credentials).unwrap();
        let symbol = Symbol::new("BTCUSDT").unwrap();

        assert_eq!(
            adapter.cached_instrument_spec(&symbol).unwrap_err().kind(),
            ExchangeErrorKind::StateConflict
        );

        let spec = InstrumentSpec::new(
            symbol.clone(),
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
