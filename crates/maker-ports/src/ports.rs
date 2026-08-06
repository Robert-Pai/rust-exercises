use async_trait::async_trait;
use maker_domain::{BestBidAsk, ClientOrderId, InstrumentSpec, OrderIntent, OrderUpdate, Symbol};

use crate::{CancelOutcome, EventStream, ExchangeResult, PlaceOrderAck, PositionMode};

/// Static instrument and account capabilities needed before quoting starts.
#[async_trait]
pub trait InstrumentPort: Send + Sync {
    /// Initializes the instrument specification before the engine starts
    /// quoting. Implementations may cache the result for hot-path use.
    async fn instrument_spec(&self, symbol: &Symbol) -> ExchangeResult<InstrumentSpec>;

    /// Loads the latest exchange rules for a configured instrument.
    ///
    /// The default keeps simple adapters source-compatible. Production
    /// adapters should override this to bypass their local cache.
    async fn refresh_instrument_spec(&self, symbol: &Symbol) -> ExchangeResult<InstrumentSpec> {
        self.instrument_spec(symbol).await
    }

    /// Commits a previously refreshed specification immediately before the
    /// engine rebuilds its session. The default suits adapters without a
    /// local cache.
    fn apply_instrument_spec(&self, _spec: InstrumentSpec) -> ExchangeResult<()> {
        Ok(())
    }

    async fn position_mode(&self) -> ExchangeResult<PositionMode>;
}

/// Public top-of-book snapshots and continuous updates.
#[async_trait]
pub trait MarketDataPort: Send + Sync {
    async fn best_bid_ask(&self, symbol: &Symbol) -> ExchangeResult<BestBidAsk>;

    async fn subscribe_best_bid_ask(
        &self,
        symbol: &Symbol,
    ) -> ExchangeResult<EventStream<BestBidAsk>>;
}

/// Private order mutation operations.
#[async_trait]
pub trait TradingPort: Send + Sync {
    async fn place_post_only(&self, intent: OrderIntent) -> ExchangeResult<PlaceOrderAck>;

    async fn cancel_order(
        &self,
        symbol: &Symbol,
        client_order_id: &ClientOrderId,
    ) -> ExchangeResult<CancelOutcome>;

    async fn cancel_all(&self, symbol: &Symbol) -> ExchangeResult<()>;
}

/// Private account order lifecycle updates.
#[async_trait]
pub trait OrderEventPort: Send + Sync {
    /// Subscribes to the account stream and emits updates for `symbol`.
    /// Implementations may receive a wider account-wide stream underneath.
    async fn subscribe_order_updates(
        &self,
        symbol: &Symbol,
    ) -> ExchangeResult<EventStream<OrderUpdate>>;
}

/// Complete exchange capability set consumed by the maker engine.
///
/// This trait has a blanket implementation, so adapters only implement the
/// four focused ports above.
pub trait Exchange: InstrumentPort + MarketDataPort + TradingPort + OrderEventPort {}

impl<T> Exchange for T where
    T: InstrumentPort + MarketDataPort + TradingPort + OrderEventPort + ?Sized
{
}

#[cfg(test)]
mod tests {
    use std::{
        marker::PhantomData,
        pin::Pin,
        task::{Context, Poll},
    };

    use futures_core::Stream;
    use maker_domain::MarketKind;
    use rust_decimal::Decimal;

    use super::*;
    use crate::ExchangeError;

    struct EmptyStream<T>(PhantomData<fn() -> T>);

    impl<T> EmptyStream<T> {
        const fn new() -> Self {
            Self(PhantomData)
        }
    }

    impl<T> Stream for EmptyStream<T> {
        type Item = T;

        fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(None)
        }
    }

    struct MockExchange;

    #[async_trait]
    impl InstrumentPort for MockExchange {
        async fn instrument_spec(&self, symbol: &Symbol) -> ExchangeResult<InstrumentSpec> {
            InstrumentSpec::new(
                *symbol,
                MarketKind::LinearPerpetual,
                Decimal::new(1, 1),
                Decimal::new(1, 3),
                Decimal::new(1, 3),
                Decimal::ONE,
            )
            .map_err(|error| {
                ExchangeError::new(crate::ExchangeErrorKind::InvalidResponse, error.to_string())
            })
        }

        async fn position_mode(&self) -> ExchangeResult<PositionMode> {
            Ok(PositionMode::OneWay)
        }
    }

    #[async_trait]
    impl MarketDataPort for MockExchange {
        async fn best_bid_ask(&self, symbol: &Symbol) -> ExchangeResult<BestBidAsk> {
            Ok(BestBidAsk::new(
                *symbol,
                maker_domain::PriceTicks::new(99).unwrap(),
                maker_domain::PriceTicks::new(100).unwrap(),
            )
            .unwrap())
        }

        async fn subscribe_best_bid_ask(
            &self,
            _symbol: &Symbol,
        ) -> ExchangeResult<EventStream<BestBidAsk>> {
            Ok(Box::pin(EmptyStream::new()))
        }
    }

    #[async_trait]
    impl TradingPort for MockExchange {
        async fn place_post_only(&self, intent: OrderIntent) -> ExchangeResult<PlaceOrderAck> {
            Ok(PlaceOrderAck::new(
                *intent.symbol(),
                *intent.client_order_id(),
                maker_domain::ExchangeOrderId::new(1).unwrap(),
            ))
        }

        async fn cancel_order(
            &self,
            _symbol: &Symbol,
            _client_order_id: &ClientOrderId,
        ) -> ExchangeResult<CancelOutcome> {
            Ok(CancelOutcome::Canceled)
        }

        async fn cancel_all(&self, _symbol: &Symbol) -> ExchangeResult<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl OrderEventPort for MockExchange {
        async fn subscribe_order_updates(
            &self,
            _symbol: &Symbol,
        ) -> ExchangeResult<EventStream<OrderUpdate>> {
            Ok(Box::pin(EmptyStream::new()))
        }
    }

    fn accepts_trait_object(_exchange: &dyn Exchange) {}

    #[test]
    fn complete_exchange_is_object_safe() {
        let exchange = MockExchange;
        accepts_trait_object(&exchange);

        let boxed: Box<dyn Exchange> = Box::new(exchange);
        drop(boxed);
    }
}
