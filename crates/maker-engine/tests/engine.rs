use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use maker_domain::{
    BestBidAsk, ClientOrderId, ExchangeOrderId, FilledLots, InstrumentSpec, MarketKind,
    OrderIntent, OrderStatus, OrderUpdate, PriceTicks, Side, Symbol,
};
use maker_engine::{EngineConfig, MakerEngine};
use maker_ports::{
    CancelOutcome, EventStream, ExchangeError, ExchangeErrorKind, ExchangeResult, InstrumentPort,
    MarketDataPort, OrderEventPort, PlaceOrderAck, PositionMode, TradingPort,
};
use rust_decimal::Decimal;
use tokio::{
    sync::{Notify, Semaphore, mpsc, oneshot},
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_stream::wrappers::UnboundedReceiverStream;

#[derive(Clone, Debug)]
enum Action {
    Instrument,
    RefreshInstrument,
    PositionMode,
    SubscribeOrders,
    SubscribeBook,
    CancelAll,
    BestBook,
    Place,
    Cancel(ClientOrderId),
}

#[derive(Clone, Debug)]
struct AcceptedOrder {
    intent: OrderIntent,
    exchange_order_id: ExchangeOrderId,
}

#[derive(Default)]
struct MockState {
    actions: Vec<Action>,
    accepted: Vec<AcceptedOrder>,
    next_exchange_order_id: u64,
    place_failures: VecDeque<ExchangeError>,
    order_senders: Vec<mpsc::UnboundedSender<ExchangeResult<OrderUpdate>>>,
    book_senders: Vec<mpsc::UnboundedSender<ExchangeResult<BestBidAsk>>>,
}

struct MockExchange {
    state: Mutex<MockState>,
    instrument: Mutex<InstrumentSpec>,
    next_refreshed_instrument: Mutex<Option<InstrumentSpec>>,
    changed: Notify,
    blocked_place: Mutex<Option<(Side, u64, Arc<Semaphore>)>>,
    blocked_cancels: Mutex<HashMap<ClientOrderId, Arc<Semaphore>>>,
}

impl MockExchange {
    fn new() -> Arc<Self> {
        let instrument = InstrumentSpec::new(
            Symbol::new("BTCUSDT").unwrap(),
            MarketKind::LinearPerpetual,
            Decimal::ONE,
            Decimal::new(1, 3),
            Decimal::new(1, 3),
            Decimal::ONE,
        )
        .unwrap();
        Arc::new(Self {
            state: Mutex::new(MockState {
                next_exchange_order_id: 1,
                ..MockState::default()
            }),
            instrument: Mutex::new(instrument),
            next_refreshed_instrument: Mutex::new(None),
            changed: Notify::new(),
            blocked_place: Mutex::new(None),
            blocked_cancels: Mutex::new(HashMap::new()),
        })
    }

    fn block_place(&self, side: Side, price: u64) -> Arc<Semaphore> {
        let gate = Arc::new(Semaphore::new(0));
        *self.blocked_place.lock().unwrap() = Some((side, price, gate.clone()));
        gate
    }

    fn block_cancel(&self, client_order_id: ClientOrderId) -> Arc<Semaphore> {
        let gate = Arc::new(Semaphore::new(0));
        self.blocked_cancels
            .lock()
            .unwrap()
            .insert(client_order_id, gate.clone());
        gate
    }

    fn record(&self, action: Action) {
        self.state.lock().unwrap().actions.push(action);
        self.changed.notify_waiters();
    }

    fn fail_next_placement(&self) {
        self.state
            .lock()
            .unwrap()
            .place_failures
            .push_back(ExchangeError::new(
                ExchangeErrorKind::Network,
                "injected placement failure",
            ));
    }

    fn set_next_refreshed_instrument(&self, instrument: InstrumentSpec) {
        *self.next_refreshed_instrument.lock().unwrap() = Some(instrument);
    }

    fn snapshot_actions(&self) -> Vec<Action> {
        self.state.lock().unwrap().actions.clone()
    }

    fn accepted_orders(&self) -> Vec<AcceptedOrder> {
        self.state.lock().unwrap().accepted.clone()
    }

    fn action_count(&self, predicate: impl Fn(&Action) -> bool) -> usize {
        self.state
            .lock()
            .unwrap()
            .actions
            .iter()
            .filter(|action| predicate(action))
            .count()
    }

    async fn wait_for(&self, predicate: impl Fn(&MockState) -> bool) {
        timeout(Duration::from_secs(3), async {
            loop {
                let notified = self.changed.notified();
                if predicate(&self.state.lock().unwrap()) {
                    return;
                }
                notified.await;
            }
        })
        .await
        .expect("mock condition timed out");
    }

    async fn wait_for_accepted(&self, count: usize) {
        self.wait_for(|state| state.accepted.len() >= count).await;
    }

    async fn wait_for_order(&self, side: Side, price: u64) {
        self.wait_for(|state| {
            state
                .accepted
                .iter()
                .any(|order| order.intent.side() == side && order.intent.price().get() == price)
        })
        .await;
    }

    fn update_for(
        &self,
        side: Side,
        price: u64,
        status: OrderStatus,
        cumulative_filled: u64,
    ) -> OrderUpdate {
        let order = self
            .accepted_orders()
            .into_iter()
            .find(|order| order.intent.side() == side && order.intent.price().get() == price)
            .expect("requested accepted order");
        OrderUpdate::new(
            order.intent.symbol().clone(),
            order.intent.client_order_id().clone(),
            order.exchange_order_id,
            order.intent.side(),
            order.intent.price(),
            order.intent.quantity(),
            FilledLots::new(cumulative_filled),
            status,
        )
        .unwrap()
    }

    fn latest_update_for(
        &self,
        side: Side,
        price: u64,
        status: OrderStatus,
        cumulative_filled: u64,
    ) -> OrderUpdate {
        let order = self
            .accepted_orders()
            .into_iter()
            .rev()
            .find(|order| order.intent.side() == side && order.intent.price().get() == price)
            .expect("requested accepted order");
        OrderUpdate::new(
            order.intent.symbol().clone(),
            order.intent.client_order_id().clone(),
            order.exchange_order_id,
            order.intent.side(),
            order.intent.price(),
            order.intent.quantity(),
            FilledLots::new(cumulative_filled),
            status,
        )
        .unwrap()
    }

    fn send_order_update(&self, update: OrderUpdate) {
        self.state
            .lock()
            .unwrap()
            .order_senders
            .last()
            .expect("order subscription")
            .send(Ok(update))
            .expect("live order subscription");
    }

    fn send_book(&self, bid: u64, ask: u64) {
        let book = BestBidAsk::new(
            Symbol::new("BTCUSDT").unwrap(),
            PriceTicks::new(bid).unwrap(),
            PriceTicks::new(ask).unwrap(),
        )
        .unwrap();
        self.state
            .lock()
            .unwrap()
            .book_senders
            .last()
            .expect("book subscription")
            .send(Ok(book))
            .expect("live book subscription");
    }

    fn fail_order_stream(&self) {
        self.state
            .lock()
            .unwrap()
            .order_senders
            .last()
            .expect("order subscription")
            .send(Err(ExchangeError::new(
                ExchangeErrorKind::Network,
                "injected stream failure",
            )))
            .expect("live order subscription");
    }
}

#[async_trait]
impl InstrumentPort for MockExchange {
    async fn instrument_spec(&self, _symbol: &Symbol) -> ExchangeResult<InstrumentSpec> {
        self.record(Action::Instrument);
        Ok(self.instrument.lock().unwrap().clone())
    }

    async fn refresh_instrument_spec(&self, _symbol: &Symbol) -> ExchangeResult<InstrumentSpec> {
        self.record(Action::RefreshInstrument);
        Ok(self
            .next_refreshed_instrument
            .lock()
            .unwrap()
            .take()
            .unwrap_or_else(|| self.instrument.lock().unwrap().clone()))
    }

    fn apply_instrument_spec(&self, instrument: InstrumentSpec) -> ExchangeResult<()> {
        *self.instrument.lock().unwrap() = instrument;
        Ok(())
    }

    async fn position_mode(&self) -> ExchangeResult<PositionMode> {
        self.record(Action::PositionMode);
        Ok(PositionMode::OneWay)
    }
}

#[async_trait]
impl MarketDataPort for MockExchange {
    async fn best_bid_ask(&self, symbol: &Symbol) -> ExchangeResult<BestBidAsk> {
        self.record(Action::BestBook);
        Ok(BestBidAsk::new(
            symbol.clone(),
            PriceTicks::new(99).unwrap(),
            PriceTicks::new(101).unwrap(),
        )
        .unwrap())
    }

    async fn subscribe_best_bid_ask(
        &self,
        _symbol: &Symbol,
    ) -> ExchangeResult<EventStream<BestBidAsk>> {
        self.record(Action::SubscribeBook);
        let (sender, receiver) = mpsc::unbounded_channel();
        self.state.lock().unwrap().book_senders.push(sender);
        Ok(Box::pin(UnboundedReceiverStream::new(receiver)))
    }
}

#[async_trait]
impl TradingPort for MockExchange {
    async fn place_post_only(&self, intent: OrderIntent) -> ExchangeResult<PlaceOrderAck> {
        self.record(Action::Place);
        let exchange_order_id = {
            let mut state = self.state.lock().unwrap();
            if let Some(error) = state.place_failures.pop_front() {
                return Err(error);
            }

            let exchange_order_id =
                ExchangeOrderId::new(state.next_exchange_order_id.to_string()).unwrap();
            state.next_exchange_order_id += 1;
            state.accepted.push(AcceptedOrder {
                intent: intent.clone(),
                exchange_order_id: exchange_order_id.clone(),
            });
            exchange_order_id
        };
        self.changed.notify_waiters();
        let gate = self
            .blocked_place
            .lock()
            .unwrap()
            .as_ref()
            .filter(|(side, price, _)| *side == intent.side() && *price == intent.price().get())
            .map(|(_, _, gate)| gate.clone());
        if let Some(gate) = gate {
            gate.acquire().await.unwrap().forget();
        }
        Ok(PlaceOrderAck::new(
            intent.symbol().clone(),
            intent.client_order_id().clone(),
            exchange_order_id,
        ))
    }

    async fn cancel_order(
        &self,
        _symbol: &Symbol,
        client_order_id: &ClientOrderId,
    ) -> ExchangeResult<CancelOutcome> {
        self.record(Action::Cancel(client_order_id.clone()));
        let gate = self
            .blocked_cancels
            .lock()
            .unwrap()
            .get(client_order_id)
            .cloned();
        if let Some(gate) = gate {
            gate.acquire().await.unwrap().forget();
        }
        Ok(CancelOutcome::Canceled)
    }

    async fn cancel_all(&self, _symbol: &Symbol) -> ExchangeResult<()> {
        self.record(Action::CancelAll);
        Ok(())
    }
}

#[async_trait]
impl OrderEventPort for MockExchange {
    async fn subscribe_order_updates(
        &self,
        _symbol: &Symbol,
    ) -> ExchangeResult<EventStream<OrderUpdate>> {
        self.record(Action::SubscribeOrders);
        let (sender, receiver) = mpsc::unbounded_channel();
        self.state.lock().unwrap().order_senders.push(sender);
        Ok(Box::pin(UnboundedReceiverStream::new(receiver)))
    }
}

fn config() -> EngineConfig {
    config_with_refresh(Duration::from_secs(3600))
}

fn config_with_refresh(instrument_refresh_interval: Duration) -> EngineConfig {
    EngineConfig::new(
        Symbol::new("BTCUSDT").unwrap(),
        3,
        1,
        1,
        1,
        Decimal::new(2, 3),
        Duration::from_millis(20),
        instrument_refresh_interval,
        Duration::from_millis(5),
    )
    .unwrap()
}

fn start_engine(
    exchange: Arc<MockExchange>,
) -> (
    oneshot::Sender<()>,
    JoinHandle<Result<(), maker_engine::EngineError>>,
) {
    start_engine_with_config(exchange, config())
}

fn start_engine_with_config(
    exchange: Arc<MockExchange>,
    config: EngineConfig,
) -> (
    oneshot::Sender<()>,
    JoinHandle<Result<(), maker_engine::EngineError>>,
) {
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let exchange_port: Arc<dyn maker_ports::Exchange> = exchange;
    let mut engine = MakerEngine::new(config, exchange_port);
    let task = tokio::spawn(async move {
        engine
            .run(async move {
                let _ = shutdown_receiver.await;
            })
            .await
    });
    (shutdown_sender, task)
}

async fn stop_engine(
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<Result<(), maker_engine::EngineError>>,
) {
    shutdown.send(()).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn bootstraps_streams_before_cancel_and_places_both_sides() {
    let exchange = MockExchange::new();
    let (shutdown, task) = start_engine(exchange.clone());
    exchange.wait_for_accepted(6).await;

    let actions = exchange.snapshot_actions();
    assert!(matches!(actions[0], Action::Instrument));
    assert!(matches!(actions[1], Action::PositionMode));
    assert!(matches!(actions[2], Action::SubscribeOrders));
    assert!(matches!(actions[3], Action::SubscribeBook));
    assert!(matches!(actions[4], Action::CancelAll));
    assert!(matches!(actions[5], Action::BestBook));

    let mut prices: Vec<_> = exchange
        .accepted_orders()
        .into_iter()
        .map(|order| (order.intent.side(), order.intent.price().get()))
        .collect();
    prices.sort_unstable();
    assert_eq!(
        prices,
        vec![
            (Side::Buy, 97),
            (Side::Buy, 98),
            (Side::Buy, 99),
            (Side::Sell, 101),
            (Side::Sell, 102),
            (Side::Sell, 103),
        ]
    );

    stop_engine(shutdown, task).await;
    assert_eq!(
        exchange.action_count(|action| matches!(action, Action::CancelAll)),
        2
    );
}

#[tokio::test]
async fn ask_fill_places_take_profit_and_far_quote() {
    let exchange = MockExchange::new();
    let (shutdown, task) = start_engine(exchange.clone());
    exchange.wait_for_accepted(6).await;

    exchange.send_book(101, 102);
    exchange.send_order_update(exchange.update_for(Side::Sell, 101, OrderStatus::Filled, 2));
    exchange.wait_for_accepted(8).await;

    exchange.wait_for_order(Side::Buy, 100).await;
    exchange.wait_for_order(Side::Sell, 104).await;
    let accepted = exchange.accepted_orders();
    assert!(
        accepted[6..]
            .iter()
            .any(|order| { order.intent.side() == Side::Buy && order.intent.price().get() == 100 })
    );
    assert!(
        accepted[6..].iter().any(|order| {
            order.intent.side() == Side::Sell && order.intent.price().get() == 104
        })
    );

    let canceled = exchange
        .snapshot_actions()
        .into_iter()
        .find_map(|action| match action {
            Action::Cancel(client_order_id) => Some(client_order_id),
            _ => None,
        })
        .unwrap();
    let far_bid = accepted
        .iter()
        .find(|order| order.intent.side() == Side::Buy && order.intent.price().get() == 97)
        .unwrap();
    assert_eq!(&canceled, far_bid.intent.client_order_id());

    stop_engine(shutdown, task).await;
}

#[tokio::test]
async fn late_fill_of_retired_quote_is_processed_before_cancel_completion() {
    let exchange = MockExchange::new();
    let config = EngineConfig::new(
        Symbol::new("BTCUSDT").unwrap(),
        3,
        1,
        1,
        10,
        Decimal::new(2, 3),
        Duration::from_millis(20),
        Duration::from_secs(3600),
        Duration::from_millis(5),
    )
    .unwrap();
    let (shutdown, task) = start_engine_with_config(exchange.clone(), config);
    exchange.wait_for_accepted(6).await;

    let far_bid = exchange
        .accepted_orders()
        .into_iter()
        .find(|order| order.intent.side() == Side::Buy && order.intent.price().get() == 97)
        .expect("initial far bid");
    let cancel_gate = exchange.block_cancel(far_bid.intent.client_order_id().clone());

    exchange.send_book(102, 103);
    exchange.send_order_update(exchange.update_for(Side::Sell, 102, OrderStatus::Filled, 2));
    exchange.wait_for_order(Side::Buy, 92).await;
    exchange.wait_for_order(Side::Sell, 104).await;

    // The ask fill retires bid 97 and starts its cancellation, but the
    // exchange can report bid 97 as filled before that cancellation resolves.
    exchange.send_order_update(exchange.update_for(Side::Buy, 97, OrderStatus::Filled, 2));
    exchange.wait_for_order(Side::Buy, 96).await;
    exchange.wait_for_order(Side::Sell, 107).await;

    cancel_gate.add_permits(1);
    stop_engine(shutdown, task).await;
}

#[tokio::test]
async fn hanging_cancel_does_not_delay_opposite_replacement() {
    let exchange = MockExchange::new();
    let (shutdown, task) = start_engine(exchange.clone());
    exchange.wait_for_accepted(6).await;

    let far_bid = exchange
        .accepted_orders()
        .into_iter()
        .find(|order| order.intent.side() == Side::Buy && order.intent.price().get() == 97)
        .unwrap();
    let cancel_gate = exchange.block_cancel(far_bid.intent.client_order_id().clone());

    exchange.send_book(101, 102);
    exchange.send_order_update(exchange.update_for(Side::Sell, 101, OrderStatus::Filled, 2));

    exchange.wait_for_order(Side::Buy, 100).await;
    exchange.wait_for_order(Side::Sell, 104).await;
    assert_eq!(cancel_gate.available_permits(), 0);

    cancel_gate.add_permits(1);
    stop_engine(shutdown, task).await;
}

#[tokio::test]
async fn fill_before_place_ack_advances_once_without_reviving_order() {
    let exchange = MockExchange::new();
    let place_gate = exchange.block_place(Side::Sell, 101);
    let (shutdown, task) = start_engine(exchange.clone());
    exchange.wait_for_accepted(6).await;

    exchange.send_book(101, 102);
    exchange.send_order_update(exchange.update_for(Side::Sell, 101, OrderStatus::Filled, 2));
    exchange.wait_for_accepted(8).await;
    assert_eq!(place_gate.available_permits(), 0);

    place_gate.add_permits(1);
    sleep(Duration::from_millis(60)).await;
    assert_eq!(exchange.accepted_orders().len(), 8);
    assert_eq!(
        exchange.action_count(|action| matches!(action, Action::Cancel(_))),
        1
    );

    stop_engine(shutdown, task).await;
}

#[tokio::test]
async fn near_replacement_waits_until_latest_book_is_post_only_safe() {
    let exchange = MockExchange::new();
    let (shutdown, task) = start_engine(exchange.clone());
    exchange.wait_for_accepted(6).await;

    exchange.send_order_update(exchange.update_for(Side::Sell, 101, OrderStatus::Filled, 2));
    sleep(Duration::from_millis(60)).await;

    assert!(
        exchange
            .accepted_orders()
            .iter()
            .any(|order| order.intent.side() == Side::Buy && order.intent.price().get() == 100),
        "take-profit order should be placed below the current best ask"
    );

    exchange.send_book(101, 102);
    exchange.wait_for_order(Side::Buy, 100).await;
    stop_engine(shutdown, task).await;
}

#[tokio::test]
async fn bid_fill_rolls_symmetrically() {
    let exchange = MockExchange::new();
    let (shutdown, task) = start_engine(exchange.clone());
    exchange.wait_for_accepted(6).await;

    exchange.send_book(98, 99);
    exchange.send_order_update(exchange.update_for(Side::Buy, 99, OrderStatus::Filled, 2));
    exchange.wait_for_accepted(8).await;

    let accepted = exchange.accepted_orders();
    assert!(
        accepted[6..].iter().any(|order| {
            order.intent.side() == Side::Sell && order.intent.price().get() == 100
        })
    );
    assert!(
        accepted[6..]
            .iter()
            .any(|order| { order.intent.side() == Side::Buy && order.intent.price().get() == 96 })
    );

    stop_engine(shutdown, task).await;
}

#[tokio::test]
async fn partial_fill_does_not_roll_the_grid() {
    let exchange = MockExchange::new();
    let (shutdown, task) = start_engine(exchange.clone());
    exchange.wait_for_accepted(6).await;
    let actions_before = exchange.snapshot_actions().len();

    exchange.send_order_update(exchange.update_for(
        Side::Sell,
        101,
        OrderStatus::PartiallyFilled,
        1,
    ));
    sleep(Duration::from_millis(60)).await;

    assert_eq!(exchange.accepted_orders().len(), 6);
    assert_eq!(
        exchange.action_count(|action| matches!(action, Action::Cancel(_))),
        0
    );
    assert_eq!(exchange.snapshot_actions().len(), actions_before);

    stop_engine(shutdown, task).await;
}

#[tokio::test]
async fn duplicate_full_fill_is_idempotent() {
    let exchange = MockExchange::new();
    let (shutdown, task) = start_engine(exchange.clone());
    exchange.wait_for_accepted(6).await;
    let update = exchange.update_for(Side::Sell, 101, OrderStatus::Filled, 2);

    exchange.send_book(101, 102);
    exchange.send_order_update(update.clone());
    exchange.send_order_update(update);
    exchange.wait_for_accepted(8).await;
    sleep(Duration::from_millis(60)).await;

    assert_eq!(exchange.accepted_orders().len(), 8);
    assert_eq!(
        exchange.action_count(|action| matches!(action, Action::Cancel(_))),
        1
    );

    stop_engine(shutdown, task).await;
}

#[tokio::test]
async fn retries_failed_placement_on_reconcile_timer() {
    let exchange = MockExchange::new();
    exchange.fail_next_placement();
    let (shutdown, task) = start_engine(exchange.clone());

    exchange.wait_for_accepted(6).await;
    assert_eq!(
        exchange.action_count(|action| matches!(action, Action::Place)),
        7
    );
    assert_eq!(
        exchange.action_count(|action| matches!(action, Action::Cancel(_))),
        1,
        "an ambiguous failed placement must be resolved before retrying"
    );

    stop_engine(shutdown, task).await;
}

#[tokio::test]
async fn defers_out_of_order_fills_until_grid_can_advance() {
    let exchange = MockExchange::new();
    let (shutdown, task) = start_engine(exchange.clone());
    exchange.wait_for_accepted(6).await;

    exchange.send_book(102, 103);
    exchange.send_order_update(exchange.update_for(Side::Sell, 102, OrderStatus::Filled, 2));
    sleep(Duration::from_millis(40)).await;
    assert_eq!(exchange.accepted_orders().len(), 6);

    exchange.send_order_update(exchange.update_for(Side::Sell, 101, OrderStatus::Filled, 2));
    exchange.wait_for_accepted(10).await;
    exchange.wait_for_order(Side::Buy, 100).await;
    exchange.wait_for_order(Side::Buy, 101).await;
    exchange.wait_for_order(Side::Sell, 104).await;
    exchange.wait_for_order(Side::Sell, 105).await;

    stop_engine(shutdown, task).await;
}

#[tokio::test]
async fn stream_loss_preserves_take_profit_grid_purpose() {
    let exchange = MockExchange::new();
    let (shutdown, task) = start_engine(exchange.clone());
    exchange.wait_for_accepted(6).await;

    exchange.send_book(101, 102);
    exchange.send_order_update(exchange.update_for(Side::Sell, 101, OrderStatus::Filled, 2));
    exchange.wait_for_order(Side::Buy, 100).await;
    exchange.wait_for_order(Side::Sell, 104).await;

    exchange.fail_order_stream();
    exchange
        .wait_for(|state| {
            state
                .actions
                .iter()
                .filter(|action| matches!(action, Action::SubscribeOrders))
                .count()
                >= 2
                && state.accepted.len() >= 14
        })
        .await;

    assert!(exchange.action_count(|action| matches!(action, Action::CancelAll)) >= 3);
    assert!(
        exchange
            .accepted_orders()
            .iter()
            .filter(|order| {
                order.intent.side() == Side::Buy && order.intent.price().get() == 100
            })
            .count()
            >= 2,
        "the preserved take-profit level must be placed again after stream recovery"
    );

    let accepted_before_take_profit = exchange.accepted_orders().len();
    exchange.send_order_update(exchange.latest_update_for(Side::Buy, 100, OrderStatus::Filled, 2));
    exchange
        .wait_for(|state| state.accepted.len() > accepted_before_take_profit)
        .await;
    assert!(
        exchange
            .accepted_orders()
            .iter()
            .skip(accepted_before_take_profit)
            .any(|order| order.intent.side() == Side::Buy && order.intent.price().get() == 97),
        "a recovered take-profit fill must roll a far-side quote"
    );
    assert_eq!(
        exchange.action_count(|action| matches!(action, Action::SubscribeBook)),
        2
    );

    stop_engine(shutdown, task).await;
}

#[tokio::test]
async fn changed_instrument_rules_cancel_and_rebuild_the_grid() {
    let exchange = MockExchange::new();
    exchange.set_next_refreshed_instrument(
        InstrumentSpec::new(
            Symbol::new("BTCUSDT").unwrap(),
            MarketKind::LinearPerpetual,
            Decimal::new(2, 0),
            Decimal::new(1, 3),
            Decimal::new(1, 3),
            Decimal::ONE,
        )
        .unwrap(),
    );
    let (shutdown, task) = start_engine_with_config(
        exchange.clone(),
        config_with_refresh(Duration::from_millis(10)),
    );

    exchange
        .wait_for(|state| {
            state.accepted.len() >= 12
                && state
                    .actions
                    .iter()
                    .any(|action| matches!(action, Action::RefreshInstrument))
                && state
                    .actions
                    .iter()
                    .filter(|action| matches!(action, Action::CancelAll))
                    .count()
                    >= 3
        })
        .await;

    stop_engine(shutdown, task).await;
}
