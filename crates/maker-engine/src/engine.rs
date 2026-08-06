use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use futures_util::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use maker_domain::{
    BestBidAsk, ClientOrderId, FilledLevel, GridConfig, GridError, GridLevel, GridModel,
    GridReassignment, InstrumentSpec, OrderIntent, OrderStatus, OrderUpdate, Side,
};
use maker_ports::{
    CancelOutcome, EventStream, Exchange, ExchangeErrorKind, PlaceOrderAck, PositionMode,
};
use tokio::time::{MissedTickBehavior, interval, sleep};
use tracing::{debug, info, warn};

use crate::{EngineConfig, EngineError, OrderRegistry};

static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);
const SESSION_MASK: u64 = (1_u64 << 48) - 1;

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
    exchange: Arc<dyn Exchange>,
    phase: EnginePhase,
    instrument: Option<InstrumentSpec>,
    grid: Option<GridModel>,
    latest_book: Option<BestBidAsk>,
    registry: OrderRegistry,
    placement_attempts: HashMap<ClientOrderId, GridLevel>,
    inflight_placements: HashSet<ClientOrderId>,
    filled_before_ack: HashSet<ClientOrderId>,
    pre_ack_updates: HashMap<ClientOrderId, OrderUpdate>,
    unresolved_attempts: HashSet<ClientOrderId>,
    inflight_cancels: HashSet<ClientOrderId>,
    deferred_cancels: HashSet<ClientOrderId>,
    deferred_placements: HashSet<GridLevel>,
    instrument_refresh_inflight: bool,
    pending_fills: HashMap<ClientOrderId, PendingFill>,
    pending_fill_order: VecDeque<ClientOrderId>,
    placement_priority: Vec<GridLevel>,
    session_seed: u64,
    session_generation: u16,
    client_id_prefix: String,
    next_order_sequence: u32,
}

impl MakerEngine {
    pub fn new(config: EngineConfig, exchange: Arc<dyn Exchange>) -> Self {
        let clock = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos() as u64);
        let counter = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
        let session_seed = (clock ^ counter.rotate_left(17)) & SESSION_MASK;

        Self {
            config,
            exchange,
            phase: EnginePhase::Starting,
            instrument: None,
            grid: None,
            latest_book: None,
            registry: OrderRegistry::new(),
            placement_attempts: HashMap::new(),
            inflight_placements: HashSet::new(),
            filled_before_ack: HashSet::new(),
            pre_ack_updates: HashMap::new(),
            unresolved_attempts: HashSet::new(),
            inflight_cancels: HashSet::new(),
            deferred_cancels: HashSet::new(),
            deferred_placements: HashSet::new(),
            instrument_refresh_inflight: false,
            pending_fills: HashMap::new(),
            pending_fill_order: VecDeque::new(),
            placement_priority: Vec::new(),
            session_seed,
            session_generation: 0,
            client_id_prefix: String::new(),
            next_order_sequence: 0,
        }
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
        let mut shutdown = Box::pin(shutdown);
        let mut first_attempt = true;
        let mut rebuild_grid = true;

        loop {
            self.phase = if first_attempt {
                EnginePhase::Starting
            } else {
                EnginePhase::Recovering
            };

            let bootstrap_result = tokio::select! {
                _ = shutdown.as_mut() => return self.stop().await,
                result = self.bootstrap(rebuild_grid) => result,
            };

            match bootstrap_result {
                Ok(mut subscriptions) => {
                    first_attempt = false;
                    self.phase = EnginePhase::Running;
                    info!(symbol = %self.config.symbol(), "maker grid is running");

                    match self.drive(&mut subscriptions, &mut shutdown).await {
                        DriveExit::Shutdown => return self.stop().await,
                        DriveExit::Recover(reason) => {
                            warn!(symbol = %self.config.symbol(), %reason, "recovering maker grid");
                            self.phase = EnginePhase::Recovering;
                            if let Err(error) = self.recover_stream_loss().await {
                                warn!(
                                    symbol = %self.config.symbol(),
                                    %error,
                                    "could not resolve orders during stream recovery; attempting a final cancel-all"
                                );
                                let _ = self.cancel_all_for_recovery().await;
                                return Err(error);
                            }
                            rebuild_grid = false;
                        }
                        DriveExit::Rebuild(error) => {
                            warn!(
                                symbol = %self.config.symbol(),
                                error = %error,
                                "rebuilding maker grid"
                            );
                            self.phase = EnginePhase::Stopping;
                            self.cancel_all_for_recovery().await?;
                            rebuild_grid = true;
                        }
                        DriveExit::Fatal(error) => {
                            self.phase = EnginePhase::Stopping;
                            self.cancel_all_for_recovery().await?;
                            return Err(error);
                        }
                    }
                }
                Err(error) if error.recommends_recovery() => {
                    first_attempt = false;
                    warn!(
                        symbol = %self.config.symbol(),
                        error = %error,
                        "maker bootstrap failed; retrying"
                    );
                    self.phase = EnginePhase::Recovering;
                    if matches!(error, EngineError::InstrumentRulesChanged { .. }) {
                        rebuild_grid = true;
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
                            warn!(
                                symbol = %self.config.symbol(),
                                %recovery_error,
                                "could not resolve orders after bootstrap failure; attempting a final cancel-all"
                            );
                            let _ = self.cancel_all_for_recovery().await;
                            return Err(recovery_error);
                        }
                    }
                }
                Err(error) => {
                    self.phase = EnginePhase::Stopping;
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
            .instrument_spec(self.config.symbol())
            .await
            .map_err(|error| EngineError::exchange("load instrument", error))?;
        self.validate_instrument(&instrument)?;

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
            .subscribe_order_updates(self.config.symbol())
            .await
            .map_err(|error| EngineError::exchange("subscribe order updates", error))?;
        let books = self
            .exchange
            .subscribe_best_bid_ask(self.config.symbol())
            .await
            .map_err(|error| EngineError::exchange("subscribe best bid/ask", error))?;

        self.exchange
            .cancel_all(self.config.symbol())
            .await
            .map_err(|error| EngineError::exchange("initial cancel all", error))?;
        info!(
            symbol = %self.config.symbol(),
            "canceled all existing symbol orders before bootstrap"
        );

        let book = self
            .exchange
            .best_bid_ask(self.config.symbol())
            .await
            .map_err(|error| EngineError::exchange("load initial best bid/ask", error))?;
        self.validate_book(&book)?;

        if rebuild_grid {
            self.begin_session()?;
            self.instrument = Some(instrument.clone());
            self.latest_book = Some(book.clone());
            let quantity = instrument.quantity_to_lots_exact(self.config.quantity())?;
            let grid_config = GridConfig::new(
                self.config.levels_per_side(),
                self.config.inner_ticks(),
                self.config.spacing_ticks(),
                self.config.take_profit_ticks(),
                quantity,
            );
            self.grid = Some(GridModel::initialize(grid_config, &book)?);
            info!(
                symbol = %self.config.symbol(),
                bid_ticks = book.bid().get(),
                ask_ticks = book.ask().get(),
                levels_per_side = self.config.levels_per_side().get(),
                "initialized maker grid"
            );
        } else {
            let current = self
                .instrument
                .as_ref()
                .expect("preserving recovery requires an instrument specification");
            if current != &instrument {
                return Err(EngineError::InstrumentRulesChanged {
                    symbol: self.config.symbol().clone(),
                });
            }
            if self.grid.is_none() {
                return Err(EngineError::AdapterContract(
                    "cannot preserve a missing maker grid during recovery".to_owned(),
                ));
            }
            self.latest_book = Some(book);
            info!(
                symbol = %self.config.symbol(),
                revision = self
                    .grid
                    .as_ref()
                    .expect("grid presence checked above")
                    .revision()
                    .get(),
                "preserved maker grid during stream recovery"
            );
        }
        Ok(Subscriptions {
            order_updates,
            books,
        })
    }

    fn validate_instrument(&self, instrument: &InstrumentSpec) -> Result<(), EngineError> {
        if instrument.symbol() != self.config.symbol() {
            return Err(EngineError::SymbolMismatch {
                expected: self.config.symbol().clone(),
                actual: instrument.symbol().clone(),
            });
        }
        Ok(())
    }

    fn validate_book(&self, book: &BestBidAsk) -> Result<(), EngineError> {
        if book.symbol() != self.config.symbol() {
            return Err(EngineError::SymbolMismatch {
                expected: self.config.symbol().clone(),
                actual: book.symbol().clone(),
            });
        }
        Ok(())
    }

    fn begin_session(&mut self) -> Result<(), EngineError> {
        self.session_generation = self
            .session_generation
            .checked_add(1)
            .ok_or(EngineError::SessionOverflow)?;
        self.client_id_prefix = format!(
            "mk{:012x}{:04x}",
            self.session_seed, self.session_generation
        );
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

        if let Err(error) = self.schedule_reconcile(&mut commands) {
            return Self::classify_error(error);
        }

        loop {
            let result = tokio::select! {
                _ = shutdown.as_mut() => return DriveExit::Shutdown,
                update = subscriptions.order_updates.next() => {
                    match update {
                        Some(Ok(update)) => self.handle_order_update(update),
                        Some(Err(error)) => {
                            return DriveExit::Recover(format!(
                                "order-update stream failed: {error}"
                            ));
                        }
                        None => {
                            return DriveExit::Recover(
                                "order-update stream ended".to_owned()
                            );
                        }
                    }
                }
                book = subscriptions.books.next() => {
                    match book {
                        Some(Ok(book)) => self.accept_book(book),
                        Some(Err(error)) => {
                            return DriveExit::Recover(format!(
                                "best-bid/ask stream failed: {error}"
                            ));
                        }
                        None => {
                            return DriveExit::Recover(
                                "best-bid/ask stream ended".to_owned()
                            );
                        }
                    }
                }
                completion = commands.next(), if !commands.is_empty() => {
                    self.handle_command_completion(
                        completion.expect("a non-empty command set yields a completion")
                    )
                }
                _ = instrument_refresh_timer.tick(), if !self.instrument_refresh_inflight => {
                    self.schedule_instrument_refresh(&mut commands);
                    Ok(())
                }
                _ = reconcile_timer.tick() => {
                    self.deferred_cancels.clear();
                    self.deferred_placements.clear();
                    Ok(())
                },
            };

            if let Err(error) = result {
                return Self::classify_error(error);
            }
            if let Err(error) = self.schedule_reconcile(&mut commands) {
                return Self::classify_error(error);
            }
        }
    }

    fn classify_error(error: EngineError) -> DriveExit {
        if matches!(error, EngineError::InstrumentRulesChanged { .. }) {
            return DriveExit::Rebuild(error);
        }
        if error.recommends_recovery() {
            DriveExit::Recover(error.to_string())
        } else {
            DriveExit::Fatal(error)
        }
    }

    fn accept_book(&mut self, book: BestBidAsk) -> Result<(), EngineError> {
        self.validate_book(&book)?;
        self.latest_book = Some(book);
        Ok(())
    }

    fn handle_order_update(&mut self, update: OrderUpdate) -> Result<(), EngineError> {
        if update.symbol() != self.config.symbol() {
            return Ok(());
        }
        if !update
            .client_order_id()
            .as_str()
            .starts_with(&self.client_id_prefix)
        {
            return Ok(());
        }

        debug!(
            symbol = %update.symbol(),
            client_order_id = %update.client_order_id(),
            exchange_order_id = %update.exchange_order_id(),
            side = ?update.side(),
            status = ?update.status(),
            purpose = ?self.order_purpose(update.client_order_id(), update.side(), update.price()),
            price_ticks = update.price().get(),
            original_quantity_lots = update.original_quantity().get(),
            cumulative_filled_lots = update.cumulative_filled().get(),
            "received order update"
        );

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
                    .insert(update.client_order_id().clone(), update);
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

        match self.apply_fill(level) {
            Ok(()) => {
                self.finish_fill(update.client_order_id());
            }
            Err(error) if is_deferrable_fill(&error) => {
                let client_order_id = update.client_order_id().clone();
                let was_pending = self
                    .pending_fills
                    .insert(client_order_id.clone(), PendingFill { update, level });
                if was_pending.is_none() {
                    self.pending_fill_order.push_back(client_order_id);
                }
            }
            Err(error) => return Err(error.into()),
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
                warn!(
                    symbol = %self.config.symbol(),
                    side = ?level.side(),
                    price_ticks = level.price().get(),
                    "processing a fill for a quote retired by an earlier transition"
                );
                grid.apply_late_quote_fill(filled)?
            }
            result => result?,
        };
        if let Some(reassignment) = transition.reassignment() {
            self.apply_local_reassignment(reassignment);
        }
        self.placement_priority.extend(transition.placements());
        Ok(())
    }

    fn order_purpose(
        &self,
        client_order_id: &ClientOrderId,
        side: Side,
        price: maker_domain::PriceTicks,
    ) -> Option<maker_domain::GridPurpose> {
        self.registry
            .get(client_order_id)
            .map(|order| order.level().purpose())
            .or_else(|| {
                self.placement_attempts
                    .get(client_order_id)
                    .map(|level| level.purpose())
            })
            .or_else(|| {
                self.grid
                    .as_ref()?
                    .level(side, price)
                    .map(|level| level.purpose())
            })
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
        if self.deferred_placements.remove(&previous) {
            self.deferred_placements.insert(current);
        }
    }

    fn drain_pending_fills(&mut self) -> Result<(), EngineError> {
        loop {
            let mut progressed = false;
            let client_order_ids: Vec<_> = self.pending_fill_order.iter().cloned().collect();

            for client_order_id in client_order_ids {
                let Some(pending) = self.pending_fills.get(&client_order_id).cloned() else {
                    self.pending_fill_order
                        .retain(|candidate| candidate != &client_order_id);
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
                        self.pending_fill_order
                            .retain(|candidate| candidate != &client_order_id);
                        progressed = true;
                    }
                    Err(error) if is_deferrable_fill(&error) => {}
                    Err(error) => return Err(error.into()),
                }
            }

            if !progressed {
                return Ok(());
            }
        }
    }

    fn schedule_reconcile(
        &mut self,
        commands: &mut FuturesUnordered<CommandFuture>,
    ) -> Result<(), EngineError> {
        self.drain_pending_fills()?;
        let desired_levels = self.desired_levels();
        let desired: HashSet<_> = desired_levels.iter().copied().collect();

        let mut unresolved: Vec<_> = self
            .unresolved_attempts
            .iter()
            .filter(|client_order_id| {
                !self.inflight_cancels.contains(*client_order_id)
                    && !self.deferred_cancels.contains(*client_order_id)
            })
            .cloned()
            .collect();
        unresolved.sort();
        for client_order_id in unresolved {
            self.schedule_cancel(
                commands,
                client_order_id,
                CancellationKind::ResolveUncertain,
            );
        }

        let mut undesired: Vec<_> = self
            .registry
            .active_orders()
            .into_iter()
            .filter(|order| {
                !desired.contains(&order.level())
                    && !self.inflight_cancels.contains(order.client_order_id())
                    && !self.deferred_cancels.contains(order.client_order_id())
            })
            .map(|order| (order.client_order_id().clone(), order.level()))
            .collect();
        undesired.sort_by_key(|(_, level)| (level.side(), level.price()));
        for (client_order_id, _) in undesired {
            self.schedule_cancel(commands, client_order_id, CancellationKind::Undesired);
        }

        self.placement_priority.retain(|level| {
            desired.contains(level)
                && !self.registry.has_active_level(*level)
                && !self
                    .placement_attempts
                    .values()
                    .any(|attempt| attempt.same_order(*level))
        });
        let mut blocked_levels: Vec<_> = self
            .pending_fills
            .values()
            .filter_map(|pending| {
                self.grid
                    .as_ref()
                    .and_then(|grid| grid.level(pending.update.side(), pending.update.price()))
            })
            .collect();
        blocked_levels.extend(self.placement_attempts.values().copied());

        while let Some(level) =
            self.next_missing_level(&desired_levels, &blocked_levels, &self.deferred_placements)
        {
            let client_order_id = self.next_client_order_id()?;
            let intent = OrderIntent::post_only(
                self.config.symbol().clone(),
                client_order_id.clone(),
                level.side(),
                level.price(),
                level.quantity(),
            );
            self.placement_attempts
                .insert(client_order_id.clone(), level);
            self.inflight_placements.insert(client_order_id.clone());
            blocked_levels.push(level);

            let exchange = self.exchange.clone();
            commands.push(
                async move {
                    let result = exchange.place_post_only(intent).await;
                    CommandCompletion::Place {
                        client_order_id,
                        level,
                        result,
                    }
                }
                .boxed(),
            );
        }

        Ok(())
    }

    fn schedule_cancel(
        &mut self,
        commands: &mut FuturesUnordered<CommandFuture>,
        client_order_id: ClientOrderId,
        kind: CancellationKind,
    ) {
        self.inflight_cancels.insert(client_order_id.clone());
        let exchange = self.exchange.clone();
        let symbol = self.config.symbol().clone();
        commands.push(
            async move {
                let result = exchange.cancel_order(&symbol, &client_order_id).await;
                CommandCompletion::Cancel {
                    client_order_id,
                    kind,
                    result,
                }
            }
            .boxed(),
        );
    }

    fn schedule_instrument_refresh(&mut self, commands: &mut FuturesUnordered<CommandFuture>) {
        self.instrument_refresh_inflight = true;
        let exchange = self.exchange.clone();
        let symbol = self.config.symbol().clone();
        commands.push(
            async move {
                let result = exchange.refresh_instrument_spec(&symbol).await;
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
                warn!(
                    symbol = %self.config.symbol(),
                    error = %error,
                    "instrument refresh failed; retaining current trading rules"
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
                symbol: self.config.symbol().clone(),
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
        let level = self.current_level_for(level);
        self.inflight_placements.remove(&client_order_id);

        if self.filled_before_ack.remove(&client_order_id) {
            if let Ok(ack) = &result {
                self.validate_ack(&client_order_id, ack)?;
            }
            self.placement_attempts.remove(&client_order_id);
            self.pre_ack_updates.remove(&client_order_id);
            self.unresolved_attempts.remove(&client_order_id);
            debug!(
                symbol = %self.config.symbol(),
                client_order_id = %client_order_id,
                "placement completed after its fill was already processed"
            );
            return Ok(());
        }

        match result {
            Ok(ack) => {
                self.validate_ack(&client_order_id, &ack)?;
                info!(
                    symbol = %self.config.symbol(),
                    client_order_id = %client_order_id,
                    exchange_order_id = %ack.exchange_order_id(),
                    side = ?level.side(),
                    purpose = ?level.purpose(),
                    price_ticks = level.price().get(),
                    quantity_lots = level.quantity().get(),
                    "post-only order accepted"
                );
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
                let may_exist = placement_may_exist(error.kind())
                    || self.pre_ack_updates.contains_key(&client_order_id);
                if may_exist {
                    self.unresolved_attempts.insert(client_order_id.clone());
                } else {
                    self.placement_attempts.remove(&client_order_id);
                    self.pre_ack_updates.remove(&client_order_id);
                }
                let engine_error = EngineError::exchange("place post-only order", error);
                if engine_error.exchange_is_fatal() {
                    return Err(engine_error);
                }
                self.deferred_placements.insert(level);
                debug!(
                    symbol = %self.config.symbol(),
                    client_order_id = %client_order_id,
                    side = ?level.side(),
                    purpose = ?level.purpose(),
                    price_ticks = level.price().get(),
                    error = %engine_error,
                    "will retry placement during a later reconcile pass"
                );
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
        self.inflight_cancels.remove(&client_order_id);
        match result {
            Ok(CancelOutcome::Canceled | CancelOutcome::NotFound) => {
                if kind == CancellationKind::ResolveUncertain {
                    self.unresolved_attempts.remove(&client_order_id);
                    self.placement_attempts.remove(&client_order_id);
                    self.pre_ack_updates.remove(&client_order_id);
                    info!(
                        symbol = %self.config.symbol(),
                        client_order_id = %client_order_id,
                        "resolved uncertain order cancellation"
                    );
                } else if self.registry.get(&client_order_id).is_some() {
                    self.registry.mark_canceled(&client_order_id)?;
                    self.registry.discard(&client_order_id);
                    info!(
                        symbol = %self.config.symbol(),
                        client_order_id = %client_order_id,
                        "canceled undesired order"
                    );
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
                let engine_error = EngineError::exchange(operation, error);
                if engine_error.exchange_is_fatal() {
                    return Err(engine_error);
                }
                self.deferred_cancels.insert(client_order_id.clone());
                warn!(
                    symbol = %self.config.symbol(),
                    client_order_id = %client_order_id,
                    error = %engine_error,
                    "will retry cancel during a later reconcile pass"
                );
            }
        }
        Ok(())
    }

    fn desired_levels(&self) -> Vec<GridLevel> {
        let grid = self
            .grid
            .as_ref()
            .expect("reconcile is only called after grid initialization");
        let bids = grid.levels(Side::Buy);
        let asks = grid.levels(Side::Sell);
        let mut levels = Vec::with_capacity(bids.len() + asks.len());
        for (bid, ask) in bids.into_iter().zip(asks) {
            levels.push(bid);
            levels.push(ask);
        }
        levels
    }

    fn next_missing_level(
        &self,
        desired_levels: &[GridLevel],
        pending_levels: &[GridLevel],
        failed: &HashSet<GridLevel>,
    ) -> Option<GridLevel> {
        let max_per_side = self.config.levels_per_side().get();
        let can_place = |level: GridLevel| {
            !self.registry.has_active_level(level)
                && !pending_levels
                    .iter()
                    .any(|pending| pending.same_order(level))
                && !failed.contains(&level)
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
            .active_orders()
            .into_iter()
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
        let Some(book) = &self.latest_book else {
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

    fn next_client_order_id(&mut self) -> Result<ClientOrderId, EngineError> {
        self.next_order_sequence = self
            .next_order_sequence
            .checked_add(1)
            .ok_or(EngineError::OrderSequenceOverflow)?;
        ClientOrderId::new(format!(
            "{}{:08x}",
            self.client_id_prefix, self.next_order_sequence
        ))
        .map_err(Into::into)
    }

    fn current_level_for(&self, candidate: GridLevel) -> GridLevel {
        self.grid
            .as_ref()
            .and_then(|grid| grid.level(candidate.side(), candidate.price()))
            .filter(|level| level.quantity() == candidate.quantity())
            .unwrap_or(candidate)
    }

    fn finish_fill(&mut self, client_order_id: &ClientOrderId) {
        self.registry.discard(client_order_id);
        if self.inflight_placements.contains(client_order_id) {
            self.filled_before_ack.insert(client_order_id.clone());
        }
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
        let mut client_order_ids: Vec<_> = self
            .registry
            .active_orders()
            .into_iter()
            .map(|order| order.client_order_id().clone())
            .collect();
        client_order_ids.extend(self.placement_attempts.keys().cloned());
        client_order_ids.extend(self.pending_fills.keys().cloned());
        client_order_ids.extend(self.unresolved_attempts.iter().cloned());
        client_order_ids.extend(
            self.inflight_placements
                .iter()
                .filter(|client_order_id| !self.filled_before_ack.contains(*client_order_id))
                .cloned(),
        );
        client_order_ids.extend(
            self.inflight_cancels
                .iter()
                .filter(|client_order_id| !self.filled_before_ack.contains(*client_order_id))
                .cloned(),
        );
        // `filled_before_ack` entries already advanced the grid and cannot
        // represent a live order; the dropped placement ACK needs no replay.
        client_order_ids.sort();
        client_order_ids.dedup();

        for client_order_id in client_order_ids {
            if self.known_level(&client_order_id).is_none() {
                return Err(EngineError::AdapterContract(format!(
                    "cannot resolve recovery order {client_order_id}: local level is unknown"
                )));
            }

            let outcome = self
                .exchange
                .cancel_order(self.config.symbol(), &client_order_id)
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
        self.client_id_prefix = format!(
            "mk{:012x}{:04x}",
            self.session_seed, self.session_generation
        );
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

    async fn cancel_all_for_recovery(&self) -> Result<(), EngineError> {
        match self.exchange.cancel_all(self.config.symbol()).await {
            Ok(()) => {
                info!(
                    symbol = %self.config.symbol(),
                    "canceled all symbol orders for recovery"
                );
                Ok(())
            }
            Err(error) => {
                warn!(
                    symbol = %self.config.symbol(),
                    error = %error,
                    "failed to cancel all orders during recovery"
                );
                Err(EngineError::exchange("cancel all during recovery", error))
            }
        }
    }

    async fn stop(&mut self) -> Result<(), EngineError> {
        self.phase = EnginePhase::Stopping;
        let result = self
            .exchange
            .cancel_all(self.config.symbol())
            .await
            .map_err(|error| EngineError::exchange("cancel all on shutdown", error));
        match &result {
            Ok(()) => info!(
                symbol = %self.config.symbol(),
                "canceled all symbol orders on shutdown"
            ),
            Err(error) => warn!(
                symbol = %self.config.symbol(),
                error = %error,
                "failed to cancel all symbol orders on shutdown"
            ),
        }
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
        result
    }
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
    order_updates: EventStream<OrderUpdate>,
    books: EventStream<BestBidAsk>,
}

enum DriveExit {
    Shutdown,
    Recover(String),
    Rebuild(EngineError),
    Fatal(EngineError),
}
