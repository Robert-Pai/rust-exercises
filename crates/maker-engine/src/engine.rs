use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use futures_util::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use maker_domain::{
    BestBidAsk, ClientOrderId, FilledLevel, GridConfig, GridError, GridLevel, GridModel,
    GridReassignment, InstrumentSpec, OrderIntent, OrderStatus, OrderUpdate, Side,
};
use maker_ports::{
    CancelOutcome, EventStream, Exchange, ExchangeErrorKind, LatestBbo, LatestBboSubscription,
    PlaceOrderAck, PositionMode, PrivateEvent, ReceivedPrivateEvent,
};
use maker_runtime::{
    EngineRuntimePhase, EngineState, EventOrigin, EventSource, RuntimeTelemetry, SpscProducer,
    TryPushError, with_event_origin,
};
use tokio::time::{MissedTickBehavior, interval, sleep};

use crate::storage::{FixedVec, IdMap, IdSet, LIFECYCLE_CAPACITY, MAX_GRID_LEVELS};
use crate::{
    AccountSnapshotStage, CancelAllStage, EngineConfig, EngineError, EngineReport, OrderRegistry,
};

static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);
const SESSION_MASK: u64 = u32::MAX as u64;
const CLIENT_GENERATION_SHIFT: u32 = 24;
const CLIENT_SEQUENCE_MAX: u32 = (1 << CLIENT_GENERATION_SHIFT) - 1;

/// Coarse lifecycle phase of the engine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnginePhase {
    Starting,
    Running,
    Recovering,
    Stopping,
}

/// Single-owner coordinator for a desired rolling grid and its exchange orders.
pub struct MakerEngine {
    config: EngineConfig,
    exchange: Box<dyn Exchange>,
    phase: EnginePhase,
    instrument: Option<InstrumentSpec>,
    grid: Option<GridModel>,
    latest_book: Option<LatestBbo>,
    registry: OrderRegistry,
    placement_attempts: IdMap<GridLevel>,
    inflight_placements: IdSet,
    filled_before_ack: IdSet,
    pre_ack_updates: IdMap<OrderUpdate>,
    unresolved_attempts: IdSet,
    inflight_cancels: IdSet,
    deferred_cancels: IdSet,
    deferred_placements: FixedVec<GridLevel, MAX_GRID_LEVELS>,
    instrument_refresh_inflight: bool,
    pending_fills: IdMap<PendingFill>,
    pending_fill_order: FixedVec<ClientOrderId, LIFECYCLE_CAPACITY>,
    placement_priority: FixedVec<GridLevel, MAX_GRID_LEVELS>,
    session_seed: u32,
    session_generation: u8,
    next_order_sequence: u32,
    telemetry: Option<RuntimeTelemetry>,
    reporter: Option<SpscProducer<EngineReport>>,
}

impl MakerEngine {
    pub fn new(config: EngineConfig, exchange: Box<dyn Exchange>) -> Self {
        let clock = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos() as u64);
        let counter = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
        let session_seed = ((clock ^ counter.rotate_left(17)) & SESSION_MASK) as u32;

        Self {
            config,
            exchange,
            phase: EnginePhase::Starting,
            instrument: None,
            grid: None,
            latest_book: None,
            registry: OrderRegistry::new(),
            placement_attempts: IdMap::default(),
            inflight_placements: IdSet::default(),
            filled_before_ack: IdSet::default(),
            pre_ack_updates: IdMap::default(),
            unresolved_attempts: IdSet::default(),
            inflight_cancels: IdSet::default(),
            deferred_cancels: IdSet::default(),
            deferred_placements: FixedVec::default(),
            instrument_refresh_inflight: false,
            pending_fills: IdMap::default(),
            pending_fill_order: FixedVec::default(),
            placement_priority: FixedVec::default(),
            session_seed,
            session_generation: 0,
            next_order_sequence: 0,
            telemetry: None,
            reporter: None,
        }
    }

    pub fn with_telemetry(mut self, telemetry: RuntimeTelemetry) -> Self {
        self.telemetry = Some(telemetry);
        self.publish_runtime_state();
        self
    }

    pub fn with_reporter(mut self, reporter: SpscProducer<EngineReport>) -> Self {
        self.reporter = Some(reporter);
        self
    }

    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    pub const fn phase(&self) -> EnginePhase {
        self.phase
    }

    pub fn grid(&self) -> Option<&GridModel> {
        self.grid.as_ref()
    }

    pub const fn registry(&self) -> &OrderRegistry {
        &self.registry
    }

    /// Runs until `shutdown` resolves or a non-recoverable error occurs.
    ///
    /// Stream loss is recoverable: known orders are resolved individually,
    /// unknown symbol orders are canceled, and both subscriptions are
    /// recreated while preserving the rolling grid and its local purposes.
    pub async fn run<F>(&mut self, shutdown: F) -> Result<(), EngineError>
    where
        F: Future<Output = ()> + Send,
    {
        self.exchange
            .start()
            .map_err(|error| EngineError::exchange("start exchange session", error))?;
        let result = self.run_started(shutdown).await;
        let _ = self
            .load_account_snapshot(AccountSnapshotStage::Shutdown)
            .await;
        result
    }

    async fn run_started<F>(&mut self, shutdown: F) -> Result<(), EngineError>
    where
        F: Future<Output = ()> + Send,
    {
        let mut shutdown = Box::pin(shutdown);
        let mut first_attempt = true;
        let mut rebuild_grid = true;

        loop {
            self.set_phase(if first_attempt {
                EnginePhase::Starting
            } else {
                EnginePhase::Recovering
            });

            let bootstrap_result = tokio::select! {
                _ = shutdown.as_mut() => return self.stop().await,
                result = self.bootstrap(rebuild_grid) => result,
            };

            match bootstrap_result {
                Ok(mut subscriptions) => {
                    first_attempt = false;
                    self.set_phase(EnginePhase::Running);
                    if let Some(telemetry) = &self.telemetry {
                        telemetry.observe_session_started();
                    }

                    match self.drive(&mut subscriptions, &mut shutdown).await {
                        DriveExit::Shutdown => return self.stop().await,
                        DriveExit::Recover => {
                            self.set_phase(EnginePhase::Recovering);
                            if let Some(telemetry) = &self.telemetry {
                                telemetry.observe_recovery_started();
                            }
                            if let Err(error) = self.recover_stream_loss().await {
                                if let Err(cancel_error) = self.cancel_all_for_recovery().await {
                                    self.publish_engine_failure(
                                        "cancel all after recovery failure",
                                        &cancel_error,
                                    );
                                }
                                return Err(error);
                            }
                            rebuild_grid = false;
                        }
                        DriveExit::Rebuild => {
                            self.set_phase(EnginePhase::Stopping);
                            if let Some(telemetry) = &self.telemetry {
                                telemetry.observe_rebuild_started();
                            }
                            self.cancel_all_for_recovery().await?;
                            rebuild_grid = true;
                        }
                        DriveExit::Fatal(error) => {
                            self.set_phase(EnginePhase::Stopping);
                            self.cancel_all_for_recovery().await?;
                            return Err(error);
                        }
                    }
                }
                Err(error) if error.recommends_recovery() => {
                    self.publish_engine_failure("bootstrap maker session", &error);
                    first_attempt = false;
                    self.set_phase(EnginePhase::Recovering);
                    if let Some(telemetry) = &self.telemetry {
                        telemetry.observe_recovery_started();
                    }
                    if matches!(error, EngineError::InstrumentRulesChanged { .. }) {
                        rebuild_grid = true;
                        if let Some(telemetry) = &self.telemetry {
                            telemetry.observe_rebuild_started();
                        }
                    }
                    if rebuild_grid {
                        self.cancel_all_for_recovery().await?;
                    } else {
                        // A failed re-subscription can happen after a prior
                        // session already has live orders. Resolve those
                        // orders through the same terminal-state path as a
                        // stream failure; cancel-all alone would leave the
                        // local registry claiming canceled orders are live.
                        if let Err(recovery_error) = self.recover_stream_loss().await {
                            if let Err(cancel_error) = self.cancel_all_for_recovery().await {
                                self.publish_engine_failure(
                                    "cancel all after recovery failure",
                                    &cancel_error,
                                );
                            }
                            return Err(recovery_error);
                        }
                    }
                }
                Err(error) => {
                    self.publish_engine_failure("bootstrap maker session", &error);
                    self.set_phase(EnginePhase::Stopping);
                    self.cancel_all_for_recovery().await?;
                    return Err(error);
                }
            }

            tokio::select! {
                _ = shutdown.as_mut() => return self.stop().await,
                _ = sleep(self.config.reconnect_delay()) => {}
            }
        }
    }

    async fn bootstrap(&mut self, rebuild_grid: bool) -> Result<Subscriptions, EngineError> {
        let instrument = self
            .exchange
            .instrument_spec(*self.config.symbol())
            .await
            .map_err(|error| EngineError::exchange("load instrument", error))?;
        self.validate_instrument(&instrument)?;
        self.exchange
            .apply_instrument_spec(instrument.clone())
            .map_err(|error| EngineError::exchange("apply instrument", error))?;

        let position_mode = self
            .exchange
            .position_mode()
            .await
            .map_err(|error| EngineError::exchange("load position mode", error))?;
        if position_mode != PositionMode::OneWay {
            return Err(EngineError::UnsupportedPositionMode(position_mode));
        }

        // Subscribe before canceling and placing so no new lifecycle event can
        // be missed between the initial cleanup and the first order.
        let order_updates = self
            .exchange
            .subscribe_order_updates(*self.config.symbol())
            .await
            .map_err(|error| EngineError::exchange("subscribe order updates", error))?;

        let book = self
            .exchange
            .best_bid_ask(*self.config.symbol())
            .await
            .map_err(|error| EngineError::exchange("load initial best bid/ask", error))?;
        self.validate_book(&book)?;
        let books = self
            .exchange
            .subscribe_best_bid_ask(*self.config.symbol(), book)
            .await
            .map_err(|error| EngineError::exchange("subscribe best bid/ask", error))?;
        let book = books.latest().ok_or_else(|| {
            EngineError::AdapterContract(
                "best-bid/ask subscription did not expose its initial snapshot".to_owned(),
            )
        })?;

        // Both private and market-data readers are now connected and running.
        // Only after that stream-readiness barrier do we load the initial
        // account view. Balances and positions must be confirmed before cancel,
        // grid initialization, Running, and the first quote submission.
        let account_stage = if self.phase == EnginePhase::Starting {
            AccountSnapshotStage::Startup
        } else {
            AccountSnapshotStage::Recovery
        };
        self.load_account_snapshot(account_stage).await?;

        let cancel_stage = if self.phase == EnginePhase::Starting {
            CancelAllStage::Startup
        } else {
            CancelAllStage::Recovery
        };
        self.cancel_all_with_report(cancel_stage, "initial cancel all")
            .await?;

        if rebuild_grid {
            self.begin_session()?;
            self.instrument = Some(instrument.clone());
            self.latest_book = Some(books.reader());
            let quantity = instrument.quantity_to_lots_exact(self.config.quantity())?;
            let grid_config = GridConfig::new(
                self.config.levels_per_side(),
                self.config.inner_ticks(),
                self.config.spacing_ticks(),
                self.config.take_profit_ticks(),
                quantity,
            );
            self.grid = Some(GridModel::initialize(grid_config, &book)?);
        } else {
            let current = self
                .instrument
                .as_ref()
                .expect("preserving recovery requires an instrument specification");
            if current != &instrument {
                return Err(EngineError::InstrumentRulesChanged {
                    symbol: *self.config.symbol(),
                });
            }
            if self.grid.is_none() {
                return Err(EngineError::AdapterContract(
                    "cannot preserve a missing maker grid during recovery".to_owned(),
                ));
            }
            self.latest_book = Some(books.reader());
        }
        Ok(Subscriptions {
            order_updates,
            books,
        })
    }

    fn validate_instrument(&self, instrument: &InstrumentSpec) -> Result<(), EngineError> {
        if instrument.symbol() != self.config.symbol() {
            return Err(EngineError::SymbolMismatch {
                expected: *self.config.symbol(),
                actual: *instrument.symbol(),
            });
        }
        Ok(())
    }

    fn validate_book(&self, book: &BestBidAsk) -> Result<(), EngineError> {
        if book.symbol() != self.config.symbol() {
            return Err(EngineError::SymbolMismatch {
                expected: *self.config.symbol(),
                actual: *book.symbol(),
            });
        }
        Ok(())
    }

    fn begin_session(&mut self) -> Result<(), EngineError> {
        self.session_generation = self
            .session_generation
            .checked_add(1)
            .ok_or(EngineError::SessionOverflow)?;
        self.next_order_sequence = 0;
        self.grid = None;
        self.instrument = None;
        self.latest_book = None;
        self.registry.clear();
        self.placement_attempts.clear();
        self.inflight_placements.clear();
        self.filled_before_ack.clear();
        self.pre_ack_updates.clear();
        self.unresolved_attempts.clear();
        self.inflight_cancels.clear();
        self.deferred_cancels.clear();
        self.deferred_placements.clear();
        self.instrument_refresh_inflight = false;
        self.pending_fills.clear();
        self.pending_fill_order.clear();
        self.placement_priority.clear();
        Ok(())
    }

    async fn drive<F>(
        &mut self,
        subscriptions: &mut Subscriptions,
        shutdown: &mut Pin<Box<F>>,
    ) -> DriveExit
    where
        F: Future<Output = ()> + Send,
    {
        let mut reconcile_timer = interval(self.config.reconcile_interval());
        reconcile_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        reconcile_timer.tick().await;
        let mut instrument_refresh_timer = interval(self.config.instrument_refresh_interval());
        instrument_refresh_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        instrument_refresh_timer.tick().await;
        let mut commands = FuturesUnordered::new();

        if let Err(error) = self.schedule_reconcile(&mut commands, None) {
            return Self::classify_error(error);
        }

        loop {
            let (result, origin) = tokio::select! {
                _ = shutdown.as_mut() => return DriveExit::Shutdown,
                received = subscriptions.order_updates.next() => {
                    match received {
                        Some(Ok(received)) => {
                            let received_ns = received.received_ns();
                            match received.into_event() {
                                PrivateEvent::OrderUpdate { update, trade } => {
                                    self.publish_report(EngineReport::OrderUpdate(update));
                                    if let Some(trade) = trade {
                                        self.publish_report(EngineReport::OrderTrade(trade));
                                    }
                                    let origin = EventOrigin::new(
                                        EventSource::PrivateData,
                                        received_ns,
                                    );
                                    (self.handle_order_update(update), origin)
                                }
                                PrivateEvent::AccountUpdate(update) => {
                                    self.publish_report(EngineReport::AccountUpdate(update));
                                    (Ok(()), None)
                                }
                                PrivateEvent::TradeLite(trade) => {
                                    self.publish_report(EngineReport::TradeLite(trade));
                                    (Ok(()), None)
                                }
                            }
                        }
                        Some(Err(error)) => {
                            self.publish_exchange_failure(
                                "private user-data stream",
                                None,
                                None,
                                error,
                            );
                            return DriveExit::Recover;
                        }
                        None => {
                            self.publish_engine_failure(
                                "private user-data stream",
                                "stream ended without a terminal error",
                            );
                            return DriveExit::Recover;
                        }
                    }
                }
                changed = subscriptions.books.changed() => {
                    let result = changed.map_err(|error| {
                        EngineError::exchange("best-bid/ask stream failed", error)
                    });
                    let origin = result.as_ref().ok().and_then(|()| {
                        subscriptions.books.latest_received().and_then(|received| {
                            EventOrigin::new(EventSource::MarketData, received.received_ns())
                        })
                    });
                    (result, origin)
                }
                completion = commands.next(), if !commands.is_empty() => {
                    (self.handle_command_completion(
                        completion.expect("a non-empty command set yields a completion")
                    ), None)
                }
                _ = instrument_refresh_timer.tick(), if !self.instrument_refresh_inflight => {
                    self.schedule_instrument_refresh(&mut commands);
                    (Ok(()), None)
                }
                _ = reconcile_timer.tick() => {
                    self.deferred_cancels.clear();
                    self.deferred_placements.clear();
                    (Ok(()), None)
                },
            };

            if let Err(error) = result {
                self.publish_engine_failure("drive maker session", &error);
                return Self::classify_error(error);
            }
            if let Err(error) = self.schedule_reconcile(&mut commands, origin) {
                return Self::classify_error(error);
            }
        }
    }

    fn classify_error(error: EngineError) -> DriveExit {
        if matches!(error, EngineError::InstrumentRulesChanged { .. }) {
            return DriveExit::Rebuild;
        }
        if error.recommends_recovery() {
            DriveExit::Recover
        } else {
            DriveExit::Fatal(error)
        }
    }

    async fn load_account_snapshot(
        &mut self,
        stage: AccountSnapshotStage,
    ) -> Result<(), EngineError> {
        let result = self.exchange.account_snapshot().await;
        self.publish_report(EngineReport::AccountSnapshot {
            stage,
            result: result.clone(),
        });
        result
            .map(|_| ())
            .map_err(|error| EngineError::exchange("load account snapshot", error))
    }

    fn publish_report(&mut self, report: EngineReport) {
        let Some(reporter) = &mut self.reporter else {
            return;
        };
        if let Err(error) = reporter.try_push(report) {
            match error {
                TryPushError::Full(_) | TryPushError::ConsumerDropped(_) => {
                    if let Some(telemetry) = &self.telemetry {
                        telemetry.observe_report_dropped();
                    }
                }
            }
        }
    }

    fn publish_exchange_failure(
        &mut self,
        operation: &'static str,
        client_order_id: Option<ClientOrderId>,
        level: Option<GridLevel>,
        error: maker_ports::ExchangeError,
    ) {
        self.publish_report(EngineReport::ExchangeFailure {
            operation,
            symbol: *self.config.symbol(),
            client_order_id,
            side: level.map(GridLevel::side),
            price_ticks: level.map(GridLevel::price),
            quantity_lots: level.map(GridLevel::quantity),
            error,
        });
    }

    fn publish_engine_failure(
        &mut self,
        operation: &'static str,
        error: &(impl std::fmt::Display + ?Sized),
    ) {
        self.publish_report(EngineReport::EngineFailure {
            operation,
            error: error.to_string(),
        });
    }

    fn handle_order_update(&mut self, update: OrderUpdate) -> Result<(), EngineError> {
        if update.symbol() != self.config.symbol() {
            return Ok(());
        }
        if (update.client_order_id().get() >> CLIENT_GENERATION_SHIFT)
            != self.current_client_id_prefix()
        {
            return Ok(());
        }

        if update.status() != OrderStatus::Filled {
            if self.registry.get(update.client_order_id()).is_some() {
                self.registry.apply_update(&update)?;
                if update.status().is_terminal() {
                    self.registry.discard(update.client_order_id());
                }
            } else if let Some(level) = self
                .placement_attempts
                .get(update.client_order_id())
                .copied()
            {
                validate_attempt_update(level, &update)?;
                self.pre_ack_updates
                    .insert(*update.client_order_id(), update)
                    .map_err(|()| EngineError::CapacityExhausted {
                        storage: "pre-ack update lifecycle slots",
                    })?;
            }
            return Ok(());
        }

        if self.pending_fills.contains_key(update.client_order_id()) {
            return Ok(());
        }

        let level = match self.registry.validate_update(&update)? {
            Some(level) => {
                self.registry.apply_update(&update)?;
                level
            }
            None => {
                let Some(level) = self
                    .placement_attempts
                    .get(update.client_order_id())
                    .copied()
                else {
                    return Ok(());
                };
                validate_attempt_update(level, &update)?;
                level
            }
        };

        let filled_before_ack = self.inflight_placements.contains(update.client_order_id());
        if filled_before_ack {
            self.filled_before_ack
                .insert(*update.client_order_id())
                .map_err(|()| EngineError::CapacityExhausted {
                    storage: "filled-before-ack lifecycle slots",
                })?;
        }

        match self.apply_fill(level) {
            Ok(()) => {
                self.finish_fill(update.client_order_id());
            }
            Err(error) if is_deferrable_fill(&error) => {
                if filled_before_ack {
                    self.filled_before_ack.remove(update.client_order_id());
                }
                let client_order_id = *update.client_order_id();
                if !self.pending_fills.contains_key(&client_order_id) {
                    self.pending_fills
                        .insert(client_order_id, PendingFill { update, level })
                        .map_err(|()| EngineError::CapacityExhausted {
                            storage: "pending fills",
                        })?;
                    if self.pending_fill_order.push(client_order_id).is_err() {
                        self.pending_fills.remove(&client_order_id);
                        return Err(EngineError::CapacityExhausted {
                            storage: "pending fill FIFO",
                        });
                    }
                }
            }
            Err(error) => {
                if filled_before_ack {
                    self.filled_before_ack.remove(update.client_order_id());
                }
                return Err(error.into());
            }
        }
        self.drain_pending_fills()
    }

    fn apply_fill(&mut self, level: GridLevel) -> Result<(), GridError> {
        let filled = FilledLevel::from(level);
        let grid = self
            .grid
            .as_mut()
            .expect("a running engine always has a grid");
        let transition = match grid.apply_fill(filled) {
            Err(GridError::LevelNotFound { .. })
                if level.purpose() == maker_domain::GridPurpose::Quote =>
            {
                grid.apply_late_quote_fill(filled)?
            }
            result => result?,
        };
        if let Some(reassignment) = transition.reassignment() {
            self.apply_local_reassignment(reassignment);
        }
        let desired = self.desired_levels();
        self.placement_priority
            .retain(|level| contains_level(&desired, *level));
        for placement in transition.placements() {
            if !self
                .placement_priority
                .iter()
                .any(|level| level.same_order(placement))
                && contains_level(&desired, placement)
            {
                self.placement_priority
                    .push(placement)
                    .expect("unique desired priorities fit grid capacity");
            }
        }
        Ok(())
    }

    fn apply_local_reassignment(&mut self, reassignment: GridReassignment) {
        let previous = reassignment.previous();
        let current = reassignment.current();
        let _ = self.registry.reassign_matching_level(previous, current);
        for level in self.placement_attempts.values_mut() {
            if level.same_order(previous) {
                *level = current;
            }
        }
        for level in self.placement_priority.iter_mut() {
            if level.same_order(previous) {
                *level = current;
            }
        }
        for pending in self.pending_fills.values_mut() {
            if pending.level.same_order(previous) {
                pending.level = current;
            }
        }
        let deferred_index = self
            .deferred_placements
            .iter()
            .position(|level| level.same_order(previous));
        if let Some(index) = deferred_index {
            self.deferred_placements.remove(index);
            if !self
                .deferred_placements
                .iter()
                .any(|level| level.same_order(current))
            {
                self.deferred_placements
                    .push(current)
                    .expect("reassignment preserves deferred-placement length");
            }
        }
    }

    fn drain_pending_fills(&mut self) -> Result<(), EngineError> {
        loop {
            let Some(client_order_id) = self.pending_fill_order.get(0).copied() else {
                return Ok(());
            };
            let Some(pending) = self.pending_fills.get(&client_order_id).cloned() else {
                self.pending_fill_order.remove(0);
                continue;
            };
            let level = match self.registry.validate_update(&pending.update)? {
                Some(level) => level,
                None => {
                    validate_attempt_update(pending.level, &pending.update)?;
                    pending.level
                }
            };

            match self.apply_fill(level) {
                Ok(()) => {
                    if self.registry.get(&client_order_id).is_some() {
                        self.registry.apply_update(&pending.update)?;
                    }
                    self.finish_fill(&client_order_id);
                    self.pending_fills.remove(&client_order_id);
                    self.pending_fill_order.remove(0);
                }
                Err(error) if is_deferrable_fill(&error) => return Ok(()),
                Err(error) => return Err(error.into()),
            }
        }
    }

    fn schedule_reconcile(
        &mut self,
        commands: &mut FuturesUnordered<CommandFuture>,
        origin: Option<EventOrigin>,
    ) -> Result<(), EngineError> {
        self.drain_pending_fills()?;
        let desired_levels = self.desired_levels();

        let mut unresolved = FixedVec::<ClientOrderId, LIFECYCLE_CAPACITY>::default();
        for client_order_id in self.unresolved_attempts.iter().filter(|client_order_id| {
            !self.inflight_cancels.contains(client_order_id)
                && !self.deferred_cancels.contains(client_order_id)
        }) {
            unresolved
                .push(client_order_id)
                .expect("lifecycle set cannot exceed lifecycle scratch capacity");
        }
        unresolved.sort_by(Ord::cmp);
        for client_order_id in unresolved.iter().copied() {
            self.schedule_cancel(
                commands,
                client_order_id,
                CancellationKind::ResolveUncertain,
                origin,
            )?;
        }

        let mut undesired = FixedVec::<(ClientOrderId, GridLevel), LIFECYCLE_CAPACITY>::default();
        for order in self.registry.active_iter().filter(|order| {
            !contains_level(&desired_levels, order.level())
                && !self.inflight_cancels.contains(order.client_order_id())
                && !self.deferred_cancels.contains(order.client_order_id())
        }) {
            undesired
                .push((*order.client_order_id(), order.level()))
                .expect("registry cannot exceed lifecycle scratch capacity");
        }
        undesired.sort_by(|(_, left), (_, right)| {
            (left.side(), left.price()).cmp(&(right.side(), right.price()))
        });
        for (client_order_id, _) in undesired.iter().copied() {
            self.schedule_cancel(
                commands,
                client_order_id,
                CancellationKind::Undesired,
                origin,
            )?;
        }

        self.placement_priority.retain(|level| {
            contains_level(&desired_levels, *level)
                && !self.registry.has_active_level(*level)
                && !self
                    .placement_attempts
                    .values()
                    .any(|attempt| attempt.same_order(*level))
        });
        let mut blocked_levels = FixedVec::<GridLevel, MAX_GRID_LEVELS>::default();
        for level in self.pending_fills.values().filter_map(|pending| {
            self.grid
                .as_ref()
                .and_then(|grid| grid.level(pending.update.side(), pending.update.price()))
        }) {
            push_unique_level(&mut blocked_levels, level)?;
        }
        for level in self.placement_attempts.values().copied() {
            push_unique_level(&mut blocked_levels, level)?;
        }

        while let Some(level) =
            self.next_missing_level(&desired_levels, &blocked_levels, &self.deferred_placements)
        {
            let client_order_id = self.next_client_order_id()?;
            let intent = OrderIntent::post_only(
                *self.config.symbol(),
                client_order_id,
                level.side(),
                level.price(),
                level.quantity(),
            );
            self.reserve_placement(client_order_id, level)?;
            if let Err(error) = push_unique_level(&mut blocked_levels, level) {
                self.placement_attempts.remove(&client_order_id);
                self.inflight_placements.remove(&client_order_id);
                return Err(error);
            }

            let request = with_event_origin(origin, || self.exchange.place_post_only(intent));
            if let Some(telemetry) = &self.telemetry {
                telemetry.observe_placement_submitted();
            }
            commands.push(
                async move {
                    let result = request.await;
                    CommandCompletion::Place {
                        client_order_id,
                        level,
                        result,
                    }
                }
                .boxed(),
            );
        }

        self.publish_runtime_state();
        Ok(())
    }

    fn reserve_placement(
        &mut self,
        client_order_id: ClientOrderId,
        level: GridLevel,
    ) -> Result<(), EngineError> {
        self.placement_attempts
            .insert(client_order_id, level)
            .map_err(|()| EngineError::CapacityExhausted {
                storage: "placement-attempt lifecycle slots",
            })?;
        if self.inflight_placements.insert(client_order_id).is_err() {
            self.placement_attempts.remove(&client_order_id);
            return Err(EngineError::CapacityExhausted {
                storage: "inflight-placement lifecycle slots",
            });
        }
        Ok(())
    }

    fn schedule_cancel(
        &mut self,
        commands: &mut FuturesUnordered<CommandFuture>,
        client_order_id: ClientOrderId,
        kind: CancellationKind,
        origin: Option<EventOrigin>,
    ) -> Result<(), EngineError> {
        self.inflight_cancels
            .insert(client_order_id)
            .map_err(|()| EngineError::CapacityExhausted {
                storage: "inflight cancel lifecycle slots",
            })?;
        let symbol = *self.config.symbol();
        let request = with_event_origin(origin, || {
            self.exchange.cancel_order(symbol, client_order_id)
        });
        if let Some(telemetry) = &self.telemetry {
            telemetry.observe_cancel_submitted();
        }
        commands.push(
            async move {
                let result = request.await;
                CommandCompletion::Cancel {
                    client_order_id,
                    kind,
                    result,
                }
            }
            .boxed(),
        );
        Ok(())
    }

    fn schedule_instrument_refresh(&mut self, commands: &mut FuturesUnordered<CommandFuture>) {
        self.instrument_refresh_inflight = true;
        let symbol = *self.config.symbol();
        let request = self.exchange.refresh_instrument_spec(symbol);
        commands.push(
            async move {
                let result = request.await;
                CommandCompletion::InstrumentRefresh { result }
            }
            .boxed(),
        );
    }

    fn handle_command_completion(
        &mut self,
        completion: CommandCompletion,
    ) -> Result<(), EngineError> {
        match completion {
            CommandCompletion::Place {
                client_order_id,
                level,
                result,
            } => self.handle_place_completion(client_order_id, level, result),
            CommandCompletion::Cancel {
                client_order_id,
                kind,
                result,
            } => self.handle_cancel_completion(client_order_id, kind, result),
            CommandCompletion::InstrumentRefresh { result } => {
                self.handle_instrument_refresh(result)
            }
        }
    }

    fn handle_instrument_refresh(
        &mut self,
        refreshed: maker_ports::ExchangeResult<InstrumentSpec>,
    ) -> Result<(), EngineError> {
        self.instrument_refresh_inflight = false;
        let refreshed = match refreshed {
            Ok(refreshed) => refreshed,
            Err(error) => {
                self.publish_exchange_failure(
                    "refresh instrument specification",
                    None,
                    None,
                    error,
                );
                return Ok(());
            }
        };
        let current = self
            .instrument
            .as_ref()
            .expect("a running engine always has an instrument specification");
        if current != &refreshed {
            self.exchange
                .apply_instrument_spec(refreshed)
                .map_err(|error| EngineError::exchange("apply refreshed instrument", error))?;
            return Err(EngineError::InstrumentRulesChanged {
                symbol: *self.config.symbol(),
            });
        }
        Ok(())
    }

    fn handle_place_completion(
        &mut self,
        client_order_id: ClientOrderId,
        level: GridLevel,
        result: maker_ports::ExchangeResult<PlaceOrderAck>,
    ) -> Result<(), EngineError> {
        if let Some(telemetry) = &self.telemetry {
            telemetry.observe_placement_completed(result.is_ok());
        }
        let level = self.current_level_for(level);
        self.inflight_placements.remove(&client_order_id);

        if self.filled_before_ack.remove(&client_order_id) {
            match &result {
                Ok(ack) => self.validate_ack(&client_order_id, ack)?,
                Err(error) => self.publish_exchange_failure(
                    "place post-only order after pre-ACK fill",
                    Some(client_order_id),
                    Some(level),
                    error.clone(),
                ),
            }
            self.placement_attempts.remove(&client_order_id);
            self.pre_ack_updates.remove(&client_order_id);
            self.unresolved_attempts.remove(&client_order_id);
            return Ok(());
        }

        match result {
            Ok(ack) => {
                self.validate_ack(&client_order_id, &ack)?;
                self.registry.register(level, ack)?;
                self.placement_attempts.remove(&client_order_id);
                self.placement_priority
                    .retain(|candidate| *candidate != level);
                if let Some(update) = self.pre_ack_updates.remove(&client_order_id) {
                    self.registry.apply_update(&update)?;
                    if update.status().is_terminal() {
                        self.registry.discard(&client_order_id);
                    }
                }
                self.drain_pending_fills()?;
            }
            Err(error) => {
                self.publish_exchange_failure(
                    "place post-only order",
                    Some(client_order_id),
                    Some(level),
                    error.clone(),
                );
                let may_exist = placement_may_exist(error.kind())
                    || self.pre_ack_updates.contains_key(&client_order_id);
                if may_exist {
                    self.unresolved_attempts
                        .insert(client_order_id)
                        .map_err(|()| EngineError::CapacityExhausted {
                            storage: "unresolved-placement lifecycle slots",
                        })?;
                } else {
                    self.placement_attempts.remove(&client_order_id);
                    self.pre_ack_updates.remove(&client_order_id);
                }
                let engine_error = EngineError::exchange("place post-only order", error);
                if engine_error.exchange_is_fatal() {
                    return Err(engine_error);
                }
                if !self
                    .deferred_placements
                    .iter()
                    .any(|candidate| candidate.same_order(level))
                {
                    self.deferred_placements.push(level).map_err(|()| {
                        EngineError::CapacityExhausted {
                            storage: "deferred placement levels",
                        }
                    })?;
                }
            }
        }
        Ok(())
    }

    fn handle_cancel_completion(
        &mut self,
        client_order_id: ClientOrderId,
        kind: CancellationKind,
        result: maker_ports::ExchangeResult<CancelOutcome>,
    ) -> Result<(), EngineError> {
        if let Some(telemetry) = &self.telemetry {
            telemetry.observe_cancel_completed(result.is_ok());
        }
        self.inflight_cancels.remove(&client_order_id);
        match result {
            Ok(CancelOutcome::Canceled | CancelOutcome::NotFound) => {
                if kind == CancellationKind::ResolveUncertain {
                    self.unresolved_attempts.remove(&client_order_id);
                    self.placement_attempts.remove(&client_order_id);
                    self.pre_ack_updates.remove(&client_order_id);
                } else if self.registry.get(&client_order_id).is_some() {
                    self.registry.mark_canceled(&client_order_id)?;
                    self.registry.discard(&client_order_id);
                }
            }
            Ok(CancelOutcome::Terminal(update)) => {
                if !update.status().is_terminal() {
                    return Err(EngineError::AdapterContract(format!(
                        "cancel outcome for {client_order_id} was not terminal"
                    )));
                }
                self.handle_order_update(update)?;
                if kind == CancellationKind::ResolveUncertain {
                    self.unresolved_attempts.remove(&client_order_id);
                    self.placement_attempts.remove(&client_order_id);
                    self.pre_ack_updates.remove(&client_order_id);
                }
            }
            Err(error) => {
                let operation = if kind == CancellationKind::ResolveUncertain {
                    "resolve uncertain placement"
                } else {
                    "cancel order"
                };
                self.publish_exchange_failure(
                    operation,
                    Some(client_order_id),
                    self.known_level(&client_order_id),
                    error.clone(),
                );
                let engine_error = EngineError::exchange(operation, error);
                if engine_error.exchange_is_fatal() {
                    return Err(engine_error);
                }
                self.deferred_cancels
                    .insert(client_order_id)
                    .map_err(|()| EngineError::CapacityExhausted {
                        storage: "deferred-cancel lifecycle slots",
                    })?;
            }
        }
        Ok(())
    }

    fn desired_levels(&self) -> FixedVec<GridLevel, MAX_GRID_LEVELS> {
        let grid = self
            .grid
            .as_ref()
            .expect("reconcile is only called after grid initialization");
        let mut levels = FixedVec::default();
        let mut bids = FixedVec::<GridLevel, { crate::storage::MAX_LEVELS_PER_SIDE }>::default();
        let mut asks = FixedVec::<GridLevel, { crate::storage::MAX_LEVELS_PER_SIDE }>::default();
        grid.for_each_level(Side::Buy, |level| {
            bids.push(level)
                .expect("validated buy grid fits configured capacity");
        });
        grid.for_each_level(Side::Sell, |level| {
            asks.push(level)
                .expect("validated sell grid fits configured capacity");
        });
        for index in 0..bids.len() {
            levels
                .push(*bids.get(index).expect("bid index is initialized"))
                .expect("validated grid fits desired-level capacity");
            levels
                .push(*asks.get(index).expect("ask index is initialized"))
                .expect("validated grid fits desired-level capacity");
        }
        levels
    }

    fn next_missing_level(
        &self,
        desired_levels: &FixedVec<GridLevel, MAX_GRID_LEVELS>,
        pending_levels: &FixedVec<GridLevel, MAX_GRID_LEVELS>,
        failed: &FixedVec<GridLevel, MAX_GRID_LEVELS>,
    ) -> Option<GridLevel> {
        let max_per_side = self.config.levels_per_side().get();
        let can_place = |level: GridLevel| {
            !self.registry.has_active_level(level)
                && !pending_levels
                    .iter()
                    .any(|pending| pending.same_order(level))
                && !failed.iter().any(|failed| failed.same_order(level))
                && self.effective_side_count(level.side()) < max_per_side
                && self.level_would_rest(level)
        };

        self.placement_priority
            .iter()
            .copied()
            .find(|level| can_place(*level))
            .or_else(|| {
                desired_levels
                    .iter()
                    .copied()
                    .find(|level| can_place(*level))
            })
    }

    fn effective_side_count(&self, side: Side) -> usize {
        let active = self
            .registry
            .active_iter()
            .filter(|order| {
                order.level().side() == side
                    && !self.inflight_cancels.contains(order.client_order_id())
            })
            .count();
        let attempted = self
            .placement_attempts
            .values()
            .filter(|level| level.side() == side)
            .count();
        active + attempted
    }

    fn level_would_rest(&self, level: GridLevel) -> bool {
        let Some(book) = self.latest_book.as_ref().and_then(LatestBbo::latest) else {
            return false;
        };
        match level.side() {
            Side::Buy => level.price() < book.ask(),
            Side::Sell => level.price() > book.bid(),
        }
    }

    fn validate_ack(
        &self,
        expected_client_order_id: &ClientOrderId,
        ack: &PlaceOrderAck,
    ) -> Result<(), EngineError> {
        if ack.symbol() != self.config.symbol() {
            return Err(EngineError::AdapterContract(format!(
                "placement acknowledgement used symbol {}, expected {}",
                ack.symbol(),
                self.config.symbol()
            )));
        }
        if ack.client_order_id() != expected_client_order_id {
            return Err(EngineError::AdapterContract(format!(
                "placement acknowledgement used client ID {}, expected {}",
                ack.client_order_id(),
                expected_client_order_id
            )));
        }
        Ok(())
    }

    fn current_client_id_prefix(&self) -> u64 {
        (u64::from(self.session_seed) << 8) | u64::from(self.session_generation)
    }

    fn next_client_order_id(&mut self) -> Result<ClientOrderId, EngineError> {
        for _ in 0..LIFECYCLE_CAPACITY {
            if self.next_order_sequence == CLIENT_SEQUENCE_MAX {
                return Err(EngineError::OrderSequenceOverflow);
            }
            self.next_order_sequence += 1;
            let value = (self.current_client_id_prefix() << CLIENT_GENERATION_SHIFT)
                | u64::from(self.next_order_sequence);
            let client_order_id = ClientOrderId::new(value)?;
            if !self.lifecycle_slot_occupied(&client_order_id) {
                return Ok(client_order_id);
            }
        }
        Err(EngineError::CapacityExhausted {
            storage: "client-order lifecycle slots",
        })
    }

    fn lifecycle_slot_occupied(&self, candidate: &ClientOrderId) -> bool {
        let low_byte = candidate.get() & 0xff;
        self.registry
            .active_iter()
            .map(|order| *order.client_order_id())
            .chain(self.placement_attempts.keys())
            .chain(self.pending_fills.keys())
            .chain(self.inflight_placements.iter())
            .chain(self.filled_before_ack.iter())
            .chain(self.pre_ack_updates.keys())
            .chain(self.unresolved_attempts.iter())
            .chain(self.inflight_cancels.iter())
            .chain(self.deferred_cancels.iter())
            .any(|id| id.get() & 0xff == low_byte)
    }

    fn current_level_for(&self, candidate: GridLevel) -> GridLevel {
        self.grid
            .as_ref()
            .and_then(|grid| grid.level(candidate.side(), candidate.price()))
            .filter(|level| level.quantity() == candidate.quantity())
            .unwrap_or(candidate)
    }

    fn finish_fill(&mut self, client_order_id: &ClientOrderId) {
        if let Some(telemetry) = &self.telemetry {
            telemetry.observe_fill_applied();
        }
        self.registry.discard(client_order_id);
        self.placement_attempts.remove(client_order_id);
        self.pre_ack_updates.remove(client_order_id);
        self.unresolved_attempts.remove(client_order_id);
    }

    /// Resolves every order known by this session before rebuilding streams.
    ///
    /// A stream can disappear after an exchange-side fill but before the
    /// corresponding user-data event reaches the engine. Asking the exchange
    /// to cancel each known order gives us one final terminal result. A raced
    /// fill is fed through the same grid state machine; an unconfirmed result
    /// stops recovery instead of silently resuming quotes.
    async fn recover_stream_loss(&mut self) -> Result<(), EngineError> {
        let mut resolved = IdSet::default();
        loop {
            let client_order_id =
                self.registry
                    .active_iter()
                    .map(|order| *order.client_order_id())
                    .chain(self.placement_attempts.keys())
                    .chain(self.pending_fills.keys())
                    .chain(self.unresolved_attempts.iter())
                    .chain(self.inflight_placements.iter().filter(|client_order_id| {
                        !self.filled_before_ack.contains(client_order_id)
                    }))
                    .chain(self.inflight_cancels.iter().filter(|client_order_id| {
                        !self.filled_before_ack.contains(client_order_id)
                    }))
                    .filter(|client_order_id| !resolved.contains(client_order_id))
                    .min();
            let Some(client_order_id) = client_order_id else {
                break;
            };
            resolved
                .insert(client_order_id)
                .map_err(|()| EngineError::CapacityExhausted {
                    storage: "recovery resolved lifecycle slots",
                })?;
            // `filled_before_ack` entries already advanced the grid and cannot
            // represent a live order; the dropped placement ACK needs no replay.
            if self.known_level(&client_order_id).is_none() {
                return Err(EngineError::AdapterContract(format!(
                    "cannot resolve recovery order {client_order_id}: local level is unknown"
                )));
            }

            let outcome = self
                .exchange
                .cancel_order(*self.config.symbol(), client_order_id)
                .await
                .map_err(|error| EngineError::exchange("resolve order during recovery", error))?;

            match outcome {
                CancelOutcome::Canceled | CancelOutcome::NotFound => {
                    self.clear_local_order(&client_order_id);
                }
                CancelOutcome::Terminal(update) => {
                    if !update.status().is_terminal() {
                        return Err(EngineError::AdapterContract(format!(
                            "recovery cancel for {client_order_id} was not terminal"
                        )));
                    }
                    self.handle_order_update(update)?;
                    self.clear_local_order(&client_order_id);
                }
            }
        }

        self.drain_pending_fills()?;
        if !self.pending_fills.is_empty() {
            return Err(EngineError::AdapterContract(
                "cannot resume quoting while a recovery fill remains unresolved".to_owned(),
            ));
        }

        self.cancel_all_for_recovery().await?;
        self.begin_recovery_session()?;
        Ok(())
    }

    fn known_level(&self, client_order_id: &ClientOrderId) -> Option<GridLevel> {
        self.registry
            .get(client_order_id)
            .map(|order| order.level())
            .or_else(|| self.placement_attempts.get(client_order_id).copied())
            .or_else(|| {
                self.pending_fills
                    .get(client_order_id)
                    .map(|pending| pending.level)
            })
    }

    fn clear_local_order(&mut self, client_order_id: &ClientOrderId) {
        self.registry.discard(client_order_id);
        self.placement_attempts.remove(client_order_id);
        self.inflight_placements.remove(client_order_id);
        self.filled_before_ack.remove(client_order_id);
        self.pre_ack_updates.remove(client_order_id);
        self.unresolved_attempts.remove(client_order_id);
        self.inflight_cancels.remove(client_order_id);
        self.deferred_cancels.remove(client_order_id);
    }

    fn begin_recovery_session(&mut self) -> Result<(), EngineError> {
        self.session_generation = self
            .session_generation
            .checked_add(1)
            .ok_or(EngineError::SessionOverflow)?;
        self.next_order_sequence = 0;
        self.registry.clear();
        self.placement_attempts.clear();
        self.inflight_placements.clear();
        self.filled_before_ack.clear();
        self.pre_ack_updates.clear();
        self.unresolved_attempts.clear();
        self.inflight_cancels.clear();
        self.deferred_cancels.clear();
        self.deferred_placements.clear();
        self.instrument_refresh_inflight = false;
        self.pending_fills.clear();
        self.pending_fill_order.clear();
        Ok(())
    }

    async fn cancel_all_for_recovery(&mut self) -> Result<(), EngineError> {
        self.cancel_all_with_report(CancelAllStage::Recovery, "cancel all during recovery")
            .await
    }

    async fn cancel_all_with_report(
        &mut self,
        stage: CancelAllStage,
        operation: &'static str,
    ) -> Result<(), EngineError> {
        let symbol = *self.config.symbol();
        self.publish_report(EngineReport::CancelAllStarted { stage, symbol });
        let started = Instant::now();
        let result = self.exchange.cancel_all(symbol).await;
        let duration_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);

        match result {
            Ok(()) => {
                self.publish_report(EngineReport::CancelAllFinished {
                    stage,
                    symbol,
                    duration_us,
                    result: Ok(()),
                });
                Ok(())
            }
            Err(error) => {
                self.publish_report(EngineReport::CancelAllFinished {
                    stage,
                    symbol,
                    duration_us,
                    result: Err(error.clone()),
                });
                Err(EngineError::exchange(operation, error))
            }
        }
    }

    async fn stop(&mut self) -> Result<(), EngineError> {
        self.set_phase(EnginePhase::Stopping);
        let result = self
            .cancel_all_with_report(CancelAllStage::Shutdown, "cancel all on shutdown")
            .await;
        self.registry.clear();
        self.placement_attempts.clear();
        self.inflight_placements.clear();
        self.filled_before_ack.clear();
        self.pre_ack_updates.clear();
        self.unresolved_attempts.clear();
        self.inflight_cancels.clear();
        self.deferred_cancels.clear();
        self.deferred_placements.clear();
        self.instrument_refresh_inflight = false;
        self.pending_fills.clear();
        self.pending_fill_order.clear();
        self.instrument = None;
        self.latest_book = None;
        self.publish_runtime_state();
        result
    }

    fn set_phase(&mut self, phase: EnginePhase) {
        self.phase = phase;
        if let Some(telemetry) = &self.telemetry {
            let phase = match phase {
                EnginePhase::Starting => EngineRuntimePhase::Starting,
                EnginePhase::Running => EngineRuntimePhase::Running,
                EnginePhase::Recovering => EngineRuntimePhase::Recovering,
                EnginePhase::Stopping => EngineRuntimePhase::Stopping,
            };
            telemetry.set_engine_phase(phase);
        }
    }

    fn publish_runtime_state(&self) {
        let Some(telemetry) = &self.telemetry else {
            return;
        };
        let book = self.latest_book.as_ref().and_then(LatestBbo::latest);
        telemetry.update_engine_state(EngineState {
            active_orders: self.registry.len() as u64,
            placement_attempts: self.placement_attempts.len() as u64,
            inflight_placements: self.inflight_placements.len() as u64,
            inflight_cancels: self.inflight_cancels.len() as u64,
            pending_fills: self.pending_fills.len() as u64,
            deferred_placements: self.deferred_placements.len() as u64,
            deferred_cancels: self.deferred_cancels.len() as u64,
            bid_ticks: book.map_or(0, |book| book.bid().get()),
            ask_ticks: book.map_or(0, |book| book.ask().get()),
        });
    }
}

fn contains_level(levels: &FixedVec<GridLevel, MAX_GRID_LEVELS>, candidate: GridLevel) -> bool {
    levels.iter().any(|level| level.same_order(candidate))
}

fn push_unique_level(
    levels: &mut FixedVec<GridLevel, MAX_GRID_LEVELS>,
    candidate: GridLevel,
) -> Result<(), EngineError> {
    if !contains_level(levels, candidate) {
        levels
            .push(candidate)
            .map_err(|()| EngineError::CapacityExhausted {
                storage: "reconcile level scratch",
            })?;
    }
    Ok(())
}

fn is_deferrable_fill(error: &GridError) -> bool {
    matches!(
        error,
        GridError::PriceCollision { .. } | GridError::CrossedGrid { .. }
    )
}

fn placement_may_exist(kind: ExchangeErrorKind) -> bool {
    matches!(
        kind,
        ExchangeErrorKind::Network
            | ExchangeErrorKind::Timeout
            | ExchangeErrorKind::ServiceUnavailable
    )
}

fn validate_attempt_update(expected: GridLevel, update: &OrderUpdate) -> Result<(), EngineError> {
    if expected.side() != update.side()
        || expected.price() != update.price()
        || expected.quantity() != update.original_quantity()
    {
        return Err(EngineError::AdapterContract(format!(
            "order update identity does not match attempted order {}",
            update.client_order_id()
        )));
    }
    Ok(())
}

type CommandFuture = BoxFuture<'static, CommandCompletion>;

/// A terminal fill that cannot be applied until an earlier grid transition
/// makes the desired ladder valid again. The attempted level is retained even
/// after its placement ACK/uncertain-order bookkeeping is cleared; otherwise
/// a fill received before ACK could be lost during cancellation resolution.
#[derive(Clone, Debug)]
struct PendingFill {
    update: OrderUpdate,
    level: GridLevel,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CancellationKind {
    ResolveUncertain,
    Undesired,
}

enum CommandCompletion {
    Place {
        client_order_id: ClientOrderId,
        level: GridLevel,
        result: maker_ports::ExchangeResult<PlaceOrderAck>,
    },
    Cancel {
        client_order_id: ClientOrderId,
        kind: CancellationKind,
        result: maker_ports::ExchangeResult<CancelOutcome>,
    },
    InstrumentRefresh {
        result: maker_ports::ExchangeResult<InstrumentSpec>,
    },
}

struct Subscriptions {
    order_updates: EventStream<ReceivedPrivateEvent>,
    books: LatestBboSubscription,
}

enum DriveExit {
    Shutdown,
    Recover,
    Rebuild,
    Fatal(EngineError),
}
