use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use maker_domain::{
    BestBidAsk, ClientOrderId, ExchangeOrderId, FilledLots, InstrumentSpec, MarketKind,
    OrderIntent, OrderStatus, OrderUpdate, PriceTicks, Side, Symbol,
};
use maker_engine::{EngineConfig, MakerEngine};
use maker_ports::{
    CancelOutcome, EventStream, ExchangeFuture, ExchangeResult, InstrumentPort, LatestBboPublisher,
    LatestBboSubscription, MarketDataPort, OrderEventPort, PlaceOrderAck, PositionMode,
    ReceivedOrderUpdate, TradingPort,
};
use rust_decimal::Decimal;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;

#[derive(Clone, Debug)]
enum Call {
    Place {
        intent: OrderIntent,
        exchange_order_id: ExchangeOrderId,
    },
    Cancel {
        client_order_id: ClientOrderId,
    },
    CancelAll,
}

type BookPublisher = Arc<Mutex<Option<LatestBboPublisher>>>;

struct MockExchange {
    symbol: Symbol,
    spec: InstrumentSpec,
    book: BestBidAsk,
    calls: Mutex<Vec<Call>>,
    next_order_id: AtomicU64,
    order_receiver: Mutex<Option<mpsc::UnboundedReceiver<ExchangeResult<ReceivedOrderUpdate>>>>,
    book_publisher: BookPublisher,
}

impl MockExchange {
    fn new() -> (
        Arc<Self>,
        mpsc::UnboundedSender<ExchangeResult<ReceivedOrderUpdate>>,
        BookPublisher,
    ) {
        let symbol = Symbol::new("BTCUSDT").unwrap();
        let spec = InstrumentSpec::new(
            symbol,
            MarketKind::LinearPerpetual,
            Decimal::ONE,
            Decimal::new(1, 3),
            Decimal::new(1, 3),
            Decimal::ONE,
        )
        .unwrap();
        let book = BestBidAsk::new(
            symbol,
            PriceTicks::new(99).unwrap(),
            PriceTicks::new(100).unwrap(),
        )
        .unwrap();
        let (order_sender, order_receiver) = mpsc::unbounded_channel();
        let book_publisher = Arc::new(Mutex::new(None));
        (
            Arc::new(Self {
                symbol,
                spec,
                book,
                calls: Mutex::new(Vec::new()),
                next_order_id: AtomicU64::new(1),
                order_receiver: Mutex::new(Some(order_receiver)),
                book_publisher: book_publisher.clone(),
            }),
            order_sender,
            book_publisher,
        )
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }

    fn placements(&self) -> Vec<(OrderIntent, ExchangeOrderId)> {
        self.calls()
            .into_iter()
            .filter_map(|call| match call {
                Call::Place {
                    intent,
                    exchange_order_id,
                } => Some((intent, exchange_order_id)),
                Call::Cancel { .. } | Call::CancelAll => None,
            })
            .collect()
    }

    fn cancel_count(&self) -> usize {
        self.calls()
            .iter()
            .filter(|call| matches!(call, Call::Cancel { .. }))
            .count()
    }

    fn cancel_all_count(&self) -> usize {
        self.calls()
            .iter()
            .filter(|call| matches!(call, Call::CancelAll))
            .count()
    }
}

struct MockSession(Arc<MockExchange>);

impl std::ops::Deref for MockSession {
    type Target = MockExchange;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl InstrumentPort for MockSession {
    fn instrument_spec(&mut self, _symbol: Symbol) -> ExchangeFuture<InstrumentSpec> {
        let spec = self.spec.clone();
        Box::pin(async move { Ok(spec) })
    }

    fn position_mode(&mut self) -> ExchangeFuture<PositionMode> {
        Box::pin(async { Ok(PositionMode::OneWay) })
    }
}

impl MarketDataPort for MockSession {
    fn best_bid_ask(&mut self, _symbol: Symbol) -> ExchangeFuture<BestBidAsk> {
        let book = self.book;
        Box::pin(async move { Ok(book) })
    }

    fn subscribe_best_bid_ask(
        &mut self,
        _symbol: Symbol,
        initial: BestBidAsk,
    ) -> ExchangeFuture<LatestBboSubscription> {
        let (publisher, subscription) = LatestBboSubscription::channel(initial);
        *self.book_publisher.lock().unwrap() = Some(publisher);
        Box::pin(async move { Ok(subscription) })
    }
}

impl TradingPort for MockSession {
    fn place_post_only(&mut self, intent: OrderIntent) -> ExchangeFuture<PlaceOrderAck> {
        let exchange_order_id =
            ExchangeOrderId::new(self.next_order_id.fetch_add(1, Ordering::Relaxed)).unwrap();
        self.calls.lock().unwrap().push(Call::Place {
            intent,
            exchange_order_id,
        });
        let symbol = self.symbol;
        Box::pin(async move {
            Ok(PlaceOrderAck::new(
                symbol,
                *intent.client_order_id(),
                exchange_order_id,
            ))
        })
    }

    fn cancel_order(
        &mut self,
        _symbol: Symbol,
        client_order_id: ClientOrderId,
    ) -> ExchangeFuture<CancelOutcome> {
        self.calls
            .lock()
            .unwrap()
            .push(Call::Cancel { client_order_id });
        Box::pin(async { Ok(CancelOutcome::Canceled) })
    }

    fn cancel_all(&mut self, _symbol: Symbol) -> ExchangeFuture<()> {
        self.calls.lock().unwrap().push(Call::CancelAll);
        Box::pin(async { Ok(()) })
    }
}

impl OrderEventPort for MockSession {
    fn subscribe_order_updates(
        &mut self,
        _symbol: Symbol,
    ) -> ExchangeFuture<EventStream<ReceivedOrderUpdate>> {
        let receiver = self
            .order_receiver
            .lock()
            .unwrap()
            .take()
            .expect("order stream subscribed once");
        Box::pin(async move {
            Ok(Box::pin(UnboundedReceiverStream::new(receiver))
                as EventStream<ReceivedOrderUpdate>)
        })
    }
}

fn engine_config() -> EngineConfig {
    engine_config_with_take_profit(1)
}

fn engine_config_with_take_profit(take_profit_ticks: u64) -> EngineConfig {
    EngineConfig::new(
        Symbol::new("BTCUSDT").unwrap(),
        3,
        1,
        1,
        take_profit_ticks,
        Decimal::new(2, 3),
        Duration::from_millis(10),
        Duration::from_secs(3600),
        Duration::from_millis(10),
    )
    .unwrap()
}

#[tokio::test]
async fn same_price_quote_is_reassigned_locally_to_take_profit() {
    let (exchange, order_sender, book_sender) = MockExchange::new();
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let mut engine = MakerEngine::new(
        engine_config_with_take_profit(3),
        Box::new(MockSession(exchange.clone())),
    );
    let task = tokio::spawn(async move {
        engine
            .run(async {
                let _ = shutdown_receiver.await;
            })
            .await
    });

    wait_for(|| exchange.placements().len() == 6).await;
    let original_buy = exchange
        .placements()
        .into_iter()
        .find(|(intent, _)| intent.side() == Side::Buy && intent.price().get() == 98)
        .unwrap();
    let original_ask = exchange
        .placements()
        .into_iter()
        .find(|(intent, _)| intent.side() == Side::Sell && intent.price().get() == 101)
        .unwrap();
    book_sender
        .lock()
        .unwrap()
        .as_mut()
        .expect("book subscription")
        .publish(
            BestBidAsk::new(
                Symbol::new("BTCUSDT").unwrap(),
                PriceTicks::new(101).unwrap(),
                PriceTicks::new(102).unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
    order_sender
        .send(Ok(filled_update(
            &original_ask.0,
            original_ask.1,
            OrderStatus::Filled,
            2,
        )
        .into()))
        .unwrap();
    wait_for(|| exchange.placements().len() == 7).await;
    assert_eq!(
        exchange
            .placements()
            .iter()
            .filter(|(intent, _)| intent.side() == Side::Buy && intent.price().get() == 98)
            .count(),
        1
    );
    assert_eq!(exchange.cancel_count(), 0);

    order_sender
        .send(Ok(filled_update(
            &original_buy.0,
            original_buy.1,
            OrderStatus::Filled,
            2,
        )
        .into()))
        .unwrap();
    wait_for(|| exchange.placements().len() == 8).await;
    assert!(
        exchange
            .placements()
            .iter()
            .any(|(intent, _)| { intent.side() == Side::Buy && intent.price().get() == 95 })
    );

    shutdown_sender.send(()).unwrap();
    task.await.unwrap().unwrap();
}

fn filled_update(
    intent: &OrderIntent,
    exchange_order_id: ExchangeOrderId,
    status: OrderStatus,
    filled: u64,
) -> OrderUpdate {
    OrderUpdate::new(
        *intent.symbol(),
        *intent.client_order_id(),
        exchange_order_id,
        intent.side(),
        intent.price(),
        intent.quantity(),
        FilledLots::new(filled),
        status,
    )
    .unwrap()
}

async fn wait_for(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("condition was not reached");
}

#[tokio::test]
async fn boots_full_grid_and_cancels_all_on_shutdown() {
    let (exchange, _order_sender, _book_sender) = MockExchange::new();
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let mut engine = MakerEngine::new(engine_config(), Box::new(MockSession(exchange.clone())));
    let task = tokio::spawn(async move {
        engine
            .run(async {
                let _ = shutdown_receiver.await;
            })
            .await
    });

    wait_for(|| exchange.placements().len() == 6).await;
    let mut bids: Vec<_> = exchange
        .placements()
        .into_iter()
        .filter(|(intent, _)| intent.side() == Side::Buy)
        .map(|(intent, _)| intent.price().get())
        .collect();
    let mut asks: Vec<_> = exchange
        .placements()
        .into_iter()
        .filter(|(intent, _)| intent.side() == Side::Sell)
        .map(|(intent, _)| intent.price().get())
        .collect();
    bids.sort_unstable_by(|left, right| right.cmp(left));
    asks.sort_unstable();
    assert_eq!(bids, [98, 97, 96]);
    assert_eq!(asks, [101, 102, 103]);
    assert_eq!(exchange.cancel_all_count(), 1);

    shutdown_sender.send(()).unwrap();
    task.await.unwrap().unwrap();
    assert_eq!(exchange.cancel_all_count(), 2);
}

#[tokio::test]
async fn full_ask_fill_cancels_far_bid_and_places_near_bid_and_far_ask() {
    let (exchange, order_sender, book_sender) = MockExchange::new();
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let mut engine = MakerEngine::new(engine_config(), Box::new(MockSession(exchange.clone())));
    let task = tokio::spawn(async move {
        engine
            .run(async {
                let _ = shutdown_receiver.await;
            })
            .await
    });

    wait_for(|| exchange.placements().len() == 6).await;
    let initial = exchange.placements();
    let (filled_intent, filled_exchange_id) = initial
        .iter()
        .find(|(intent, _)| intent.side() == Side::Sell && intent.price().get() == 101)
        .cloned()
        .unwrap();
    let far_bid_client_id = initial
        .iter()
        .find(|(intent, _)| intent.side() == Side::Buy && intent.price().get() == 96)
        .map(|(intent, _)| *intent.client_order_id())
        .unwrap();

    order_sender
        .send(Ok(filled_update(
            &filled_intent,
            filled_exchange_id,
            OrderStatus::Filled,
            2,
        )
        .into()))
        .unwrap();
    book_sender
        .lock()
        .unwrap()
        .as_mut()
        .expect("book subscription")
        .publish(
            BestBidAsk::new(
                Symbol::new("BTCUSDT").unwrap(),
                PriceTicks::new(101).unwrap(),
                PriceTicks::new(102).unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
    wait_for(|| exchange.placements().len() == 8 && exchange.cancel_count() == 1).await;

    let calls = exchange.calls();
    assert!(calls.iter().any(|call| {
        matches!(
            call,
            Call::Cancel { client_order_id } if client_order_id == &far_bid_client_id
        )
    }));
    let new_orders = &exchange.placements()[6..];
    assert!(
        new_orders
            .iter()
            .any(|(intent, _)| { intent.side() == Side::Buy && intent.price().get() == 100 })
    );
    assert!(
        new_orders
            .iter()
            .any(|(intent, _)| { intent.side() == Side::Sell && intent.price().get() == 104 })
    );

    shutdown_sender.send(()).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn partial_fill_does_not_roll_the_grid() {
    let (exchange, order_sender, _book_sender) = MockExchange::new();
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let mut engine = MakerEngine::new(engine_config(), Box::new(MockSession(exchange.clone())));
    let task = tokio::spawn(async move {
        engine
            .run(async {
                let _ = shutdown_receiver.await;
            })
            .await
    });

    wait_for(|| exchange.placements().len() == 6).await;
    let (intent, exchange_order_id) = exchange
        .placements()
        .into_iter()
        .find(|(intent, _)| intent.side() == Side::Sell && intent.price().get() == 101)
        .unwrap();
    order_sender
        .send(Ok(filled_update(
            &intent,
            exchange_order_id,
            OrderStatus::PartiallyFilled,
            1,
        )
        .into()))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(exchange.placements().len(), 6);
    assert_eq!(exchange.cancel_count(), 0);

    shutdown_sender.send(()).unwrap();
    task.await.unwrap().unwrap();
}
