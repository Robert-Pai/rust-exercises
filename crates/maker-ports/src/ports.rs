use std::{future::Future, pin::Pin};

use maker_domain::{BestBidAsk, ClientOrderId, InstrumentSpec, OrderIntent, Symbol};

use crate::{
    CancelOutcome, EventStream, ExchangeResult, LatestBboSubscription, PlaceOrderAck, PositionMode,
    ReceivedOrderUpdate,
};

/// Owned response future returned after a strategy-thread dispatch completes.
pub type ExchangeFuture<T> = Pin<Box<dyn Future<Output = ExchangeResult<T>> + Send + 'static>>;

/// Static instrument and account capabilities needed before quoting starts.
pub trait InstrumentPort: Send {
    /// Initializes the adapter on its unique strategy owner.
    fn start(&mut self) -> ExchangeResult<()> {
        Ok(())
    }

    /// Initializes the instrument specification before the engine starts
    /// quoting. Implementations may cache the result for hot-path use.
    fn instrument_spec(&mut self, symbol: Symbol) -> ExchangeFuture<InstrumentSpec>;

    /// Loads the latest exchange rules for a configured instrument.
    fn refresh_instrument_spec(&mut self, symbol: Symbol) -> ExchangeFuture<InstrumentSpec> {
        self.instrument_spec(symbol)
    }

    /// Commits a previously refreshed specification immediately before rebuild.
    fn apply_instrument_spec(&mut self, _spec: InstrumentSpec) -> ExchangeResult<()> {
        Ok(())
    }

    fn position_mode(&mut self) -> ExchangeFuture<PositionMode>;
}

/// Public top-of-book snapshots and continuous updates.
pub trait MarketDataPort: Send {
    fn best_bid_ask(&mut self, symbol: Symbol) -> ExchangeFuture<BestBidAsk>;

    fn subscribe_best_bid_ask(
        &mut self,
        symbol: Symbol,
        initial: BestBidAsk,
    ) -> ExchangeFuture<LatestBboSubscription>;
}

/// Private order mutation operations.
pub trait TradingPort: Send {
    fn place_post_only(&mut self, intent: OrderIntent) -> ExchangeFuture<PlaceOrderAck>;

    fn cancel_order(
        &mut self,
        symbol: Symbol,
        client_order_id: ClientOrderId,
    ) -> ExchangeFuture<CancelOutcome>;

    fn cancel_all(&mut self, symbol: Symbol) -> ExchangeFuture<()>;
}

/// Private account order lifecycle updates.
pub trait OrderEventPort: Send {
    fn subscribe_order_updates(
        &mut self,
        symbol: Symbol,
    ) -> ExchangeFuture<EventStream<ReceivedOrderUpdate>>;
}

/// Move-only exchange session consumed by one maker strategy.
pub trait Exchange: InstrumentPort + MarketDataPort + TradingPort + OrderEventPort {}

impl<T> Exchange for T where
    T: InstrumentPort + MarketDataPort + TradingPort + OrderEventPort + ?Sized
{
}

#[cfg(test)]
mod tests {
    use std::{
        marker::PhantomData,
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

    impl InstrumentPort for MockExchange {
        fn instrument_spec(&mut self, symbol: Symbol) -> ExchangeFuture<InstrumentSpec> {
            Box::pin(async move {
                InstrumentSpec::new(
                    symbol,
                    MarketKind::LinearPerpetual,
                    Decimal::new(1, 1),
                    Decimal::new(1, 3),
                    Decimal::new(1, 3),
                    Decimal::ONE,
                )
                .map_err(|error| {
                    ExchangeError::new(crate::ExchangeErrorKind::InvalidResponse, error.to_string())
                })
            })
        }

        fn position_mode(&mut self) -> ExchangeFuture<PositionMode> {
            Box::pin(async { Ok(PositionMode::OneWay) })
        }
    }

    impl MarketDataPort for MockExchange {
        fn best_bid_ask(&mut self, symbol: Symbol) -> ExchangeFuture<BestBidAsk> {
            Box::pin(async move {
                Ok(BestBidAsk::new(
                    symbol,
                    maker_domain::PriceTicks::new(99).unwrap(),
                    maker_domain::PriceTicks::new(100).unwrap(),
                )
                .unwrap())
            })
        }

        fn subscribe_best_bid_ask(
            &mut self,
            _symbol: Symbol,
            initial: BestBidAsk,
        ) -> ExchangeFuture<LatestBboSubscription> {
            Box::pin(async move { Ok(LatestBboSubscription::channel(initial).1) })
        }
    }

    impl TradingPort for MockExchange {
        fn place_post_only(&mut self, intent: OrderIntent) -> ExchangeFuture<PlaceOrderAck> {
            Box::pin(async move {
                Ok(PlaceOrderAck::new(
                    *intent.symbol(),
                    *intent.client_order_id(),
                    maker_domain::ExchangeOrderId::new(1).unwrap(),
                ))
            })
        }

        fn cancel_order(
            &mut self,
            _symbol: Symbol,
            _client_order_id: ClientOrderId,
        ) -> ExchangeFuture<CancelOutcome> {
            Box::pin(async { Ok(CancelOutcome::Canceled) })
        }

        fn cancel_all(&mut self, _symbol: Symbol) -> ExchangeFuture<()> {
            Box::pin(async { Ok(()) })
        }
    }

    impl OrderEventPort for MockExchange {
        fn subscribe_order_updates(
            &mut self,
            _symbol: Symbol,
        ) -> ExchangeFuture<EventStream<ReceivedOrderUpdate>> {
            Box::pin(async { Ok(Box::pin(EmptyStream::new()) as EventStream<ReceivedOrderUpdate>) })
        }
    }

    fn accepts_trait_object(_exchange: &mut dyn Exchange) {}

    #[test]
    fn move_only_exchange_is_object_safe() {
        let mut exchange = MockExchange;
        accepts_trait_object(&mut exchange);

        let boxed: Box<dyn Exchange> = Box::new(exchange);
        drop(boxed);
    }
}
