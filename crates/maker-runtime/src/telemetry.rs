use std::{
    cell::Cell,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

const UNSIGNED_BUCKET_UPPER_NS: [u64; 26] = [
    500,
    1_000,
    2_000,
    5_000,
    10_000,
    20_000,
    50_000,
    100_000,
    200_000,
    500_000,
    1_000_000,
    2_000_000,
    5_000_000,
    10_000_000,
    20_000_000,
    50_000_000,
    100_000_000,
    200_000_000,
    500_000_000,
    1_000_000_000,
    2_000_000_000,
    5_000_000_000,
    10_000_000_000,
    30_000_000_000,
    60_000_000_000,
    u64::MAX,
];

const SIGNED_BUCKET_UPPER_US: [i64; 50] = [
    -60_000_000,
    -30_000_000,
    -10_000_000,
    -5_000_000,
    -2_000_000,
    -1_000_000,
    -500_000,
    -200_000,
    -100_000,
    -50_000,
    -20_000,
    -10_000,
    -5_000,
    -2_000,
    -1_000,
    -500,
    -200,
    -100,
    -50,
    -20,
    -10,
    -5,
    -2,
    -1,
    0,
    1,
    2,
    5,
    10,
    20,
    50,
    100,
    200,
    500,
    1_000,
    2_000,
    5_000,
    10_000,
    20_000,
    50_000,
    100_000,
    200_000,
    500_000,
    1_000_000,
    2_000_000,
    5_000_000,
    10_000_000,
    30_000_000,
    60_000_000,
    i64::MAX,
];

thread_local! {
    static EVENT_ORIGIN: Cell<Option<EventOrigin>> = const { Cell::new(None) };
}

/// The stream whose local receipt caused a strategy request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventSource {
    MarketData,
    PrivateData,
}

/// Coarse engine phase exported without depending on the engine crate.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum EngineRuntimePhase {
    #[default]
    Starting = 0,
    Running = 1,
    Recovering = 2,
    Stopping = 3,
}

impl EngineRuntimePhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Recovering => "recovering",
            Self::Stopping => "stopping",
        }
    }

    const fn from_atomic(value: u8) -> Self {
        match value {
            1 => Self::Running,
            2 => Self::Recovering,
            3 => Self::Stopping,
            _ => Self::Starting,
        }
    }
}

/// Current fixed-capacity engine gauges copied into atomics for reporting.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EngineState {
    pub active_orders: u64,
    pub placement_attempts: u64,
    pub inflight_placements: u64,
    pub inflight_cancels: u64,
    pub pending_fills: u64,
    pub deferred_placements: u64,
    pub deferred_cancels: u64,
    pub bid_ticks: u64,
    pub ask_ticks: u64,
}

/// A monotonic local receive timestamp carried through one strategy reaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventOrigin {
    source: EventSource,
    received_ns: u64,
}

impl EventOrigin {
    pub const fn new(source: EventSource, received_ns: u64) -> Option<Self> {
        if received_ns == 0 {
            None
        } else {
            Some(Self {
                source,
                received_ns,
            })
        }
    }

    pub const fn source(self) -> EventSource {
        self.source
    }

    pub const fn received_ns(self) -> u64 {
        self.received_ns
    }
}

/// Returns the current Unix wall-clock time in microseconds.
pub fn unix_time_us() -> i64 {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_micros());
    i64::try_from(micros).unwrap_or(i64::MAX)
}

/// Runs a synchronous exchange submission with the event that caused it.
///
/// The scope is thread-local and is restored even if `operation` panics.
pub fn with_event_origin<T>(origin: Option<EventOrigin>, operation: impl FnOnce() -> T) -> T {
    struct Restore(Option<EventOrigin>);

    impl Drop for Restore {
        fn drop(&mut self) {
            EVENT_ORIGIN.with(|current| current.set(self.0));
        }
    }

    EVENT_ORIGIN.with(|current| {
        let restore = Restore(current.replace(origin));
        let result = operation();
        drop(restore);
        result
    })
}

pub fn current_event_origin() -> Option<EventOrigin> {
    EVENT_ORIGIN.with(Cell::get)
}

struct UnsignedHistogram {
    buckets: [AtomicU64; UNSIGNED_BUCKET_UPPER_NS.len()],
    sum: AtomicU64,
    minimum: AtomicU64,
    maximum: AtomicU64,
}

impl Default for UnsignedHistogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            sum: AtomicU64::new(0),
            minimum: AtomicU64::new(u64::MAX),
            maximum: AtomicU64::new(0),
        }
    }
}

impl UnsignedHistogram {
    fn observe(&self, value: u64) {
        let index = UNSIGNED_BUCKET_UPPER_NS.partition_point(|upper| *upper < value);
        self.buckets[index].fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
        self.minimum.fetch_min(value, Ordering::Relaxed);
        self.maximum.fetch_max(value, Ordering::Relaxed);
    }

    fn take(&self) -> LatencySnapshot {
        let counts = self
            .buckets
            .each_ref()
            .map(|bucket| bucket.swap(0, Ordering::Relaxed));
        let samples = counts.iter().sum();
        let sum = self.sum.swap(0, Ordering::Relaxed);
        let minimum = self.minimum.swap(u64::MAX, Ordering::Relaxed);
        let maximum = self.maximum.swap(0, Ordering::Relaxed);
        LatencySnapshot {
            samples,
            mean: sum.checked_div(samples).unwrap_or(0),
            minimum: if samples == 0 { 0 } else { minimum },
            p20_upper: unsigned_percentile(&counts, samples, 20, maximum),
            p30_upper: unsigned_percentile(&counts, samples, 30, maximum),
            p50_upper: unsigned_percentile(&counts, samples, 50, maximum),
            p99_upper: unsigned_percentile(&counts, samples, 99, maximum),
            maximum,
        }
    }
}

struct SignedHistogram {
    buckets: [AtomicU64; SIGNED_BUCKET_UPPER_US.len()],
    sum: AtomicI64,
    minimum: AtomicI64,
    maximum: AtomicI64,
}

impl Default for SignedHistogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            sum: AtomicI64::new(0),
            minimum: AtomicI64::new(i64::MAX),
            maximum: AtomicI64::new(i64::MIN),
        }
    }
}

impl SignedHistogram {
    fn observe(&self, value: i64) {
        let index = SIGNED_BUCKET_UPPER_US.partition_point(|upper| *upper < value);
        self.buckets[index].fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
        self.minimum.fetch_min(value, Ordering::Relaxed);
        self.maximum.fetch_max(value, Ordering::Relaxed);
    }

    fn take(&self) -> SignedLatencySnapshot {
        let counts = self
            .buckets
            .each_ref()
            .map(|bucket| bucket.swap(0, Ordering::Relaxed));
        let samples = counts.iter().sum();
        let sum = self.sum.swap(0, Ordering::Relaxed);
        let minimum = self.minimum.swap(i64::MAX, Ordering::Relaxed);
        let maximum = self.maximum.swap(i64::MIN, Ordering::Relaxed);
        SignedLatencySnapshot {
            samples,
            mean: sum
                .checked_div(i64::try_from(samples).unwrap_or(i64::MAX))
                .unwrap_or(0),
            minimum: if samples == 0 { 0 } else { minimum },
            p20_upper: signed_percentile(&counts, samples, 20, maximum),
            p30_upper: signed_percentile(&counts, samples, 30, maximum),
            p50_upper: signed_percentile(&counts, samples, 50, maximum),
            p99_upper: signed_percentile(&counts, samples, 99, maximum),
            maximum: if samples == 0 { 0 } else { maximum },
        }
    }
}

fn percentile_rank(samples: u64, percentile: u64) -> u64 {
    samples.saturating_mul(percentile).div_ceil(100).max(1)
}

fn unsigned_percentile(
    counts: &[u64; UNSIGNED_BUCKET_UPPER_NS.len()],
    samples: u64,
    percentile: u64,
    maximum: u64,
) -> u64 {
    if samples == 0 {
        return 0;
    }
    let rank = percentile_rank(samples, percentile);
    let mut cumulative = 0_u64;
    for (index, count) in counts.iter().enumerate() {
        cumulative = cumulative.saturating_add(*count);
        if cumulative >= rank {
            return UNSIGNED_BUCKET_UPPER_NS[index].min(maximum);
        }
    }
    maximum
}

fn signed_percentile(
    counts: &[u64; SIGNED_BUCKET_UPPER_US.len()],
    samples: u64,
    percentile: u64,
    maximum: i64,
) -> i64 {
    if samples == 0 {
        return 0;
    }
    let rank = percentile_rank(samples, percentile);
    let mut cumulative = 0_u64;
    for (index, count) in counts.iter().enumerate() {
        cumulative = cumulative.saturating_add(*count);
        if cumulative >= rank {
            return SIGNED_BUCKET_UPPER_US[index].min(maximum);
        }
    }
    maximum
}

struct RuntimeTelemetryInner {
    epoch: Instant,
    market_event_delay_us: SignedHistogram,
    market_transaction_delay_us: SignedHistogram,
    private_event_delay_us: SignedHistogram,
    private_transaction_delay_us: SignedHistogram,
    market_strategy_reaction_ns: UnsignedHistogram,
    private_strategy_reaction_ns: UnsignedHistogram,
    event_request_dispatch_ns: UnsignedHistogram,
    background_request_dispatch_ns: UnsignedHistogram,
    request_queue_wait_ns: UnsignedHistogram,
    request_enqueue_to_dequeue_ns: UnsignedHistogram,
    request_preflight_wait_ns: UnsignedHistogram,
    request_prepare_ns: UnsignedHistogram,
    socket_send_ns: UnsignedHistogram,
    market_end_to_end_ns: UnsignedHistogram,
    private_end_to_end_ns: UnsignedHistogram,
    market_events: AtomicU64,
    private_events: AtomicU64,
    event_requests: AtomicU64,
    background_requests: AtomicU64,
    requests_sent: AtomicU64,
    request_send_failures: AtomicU64,
    reports_dropped: AtomicU64,
    last_market_receive_ns: AtomicU64,
    last_private_receive_ns: AtomicU64,
    engine_phase: AtomicU64,
    sessions_started: AtomicU64,
    recoveries_started: AtomicU64,
    rebuilds_started: AtomicU64,
    fills_applied: AtomicU64,
    placements_submitted: AtomicU64,
    placements_succeeded: AtomicU64,
    placements_failed: AtomicU64,
    cancels_submitted: AtomicU64,
    cancels_succeeded: AtomicU64,
    cancels_failed: AtomicU64,
    active_orders: AtomicU64,
    placement_attempts: AtomicU64,
    inflight_placements: AtomicU64,
    inflight_cancels: AtomicU64,
    pending_fills: AtomicU64,
    deferred_placements: AtomicU64,
    deferred_cancels: AtomicU64,
    bid_ticks: AtomicU64,
    ask_ticks: AtomicU64,
}

impl Default for RuntimeTelemetryInner {
    fn default() -> Self {
        Self {
            epoch: Instant::now(),
            market_event_delay_us: SignedHistogram::default(),
            market_transaction_delay_us: SignedHistogram::default(),
            private_event_delay_us: SignedHistogram::default(),
            private_transaction_delay_us: SignedHistogram::default(),
            market_strategy_reaction_ns: UnsignedHistogram::default(),
            private_strategy_reaction_ns: UnsignedHistogram::default(),
            event_request_dispatch_ns: UnsignedHistogram::default(),
            background_request_dispatch_ns: UnsignedHistogram::default(),
            request_queue_wait_ns: UnsignedHistogram::default(),
            request_enqueue_to_dequeue_ns: UnsignedHistogram::default(),
            request_preflight_wait_ns: UnsignedHistogram::default(),
            request_prepare_ns: UnsignedHistogram::default(),
            socket_send_ns: UnsignedHistogram::default(),
            market_end_to_end_ns: UnsignedHistogram::default(),
            private_end_to_end_ns: UnsignedHistogram::default(),
            market_events: AtomicU64::new(0),
            private_events: AtomicU64::new(0),
            event_requests: AtomicU64::new(0),
            background_requests: AtomicU64::new(0),
            requests_sent: AtomicU64::new(0),
            request_send_failures: AtomicU64::new(0),
            reports_dropped: AtomicU64::new(0),
            last_market_receive_ns: AtomicU64::new(0),
            last_private_receive_ns: AtomicU64::new(0),
            engine_phase: AtomicU64::new(EngineRuntimePhase::Starting as u64),
            sessions_started: AtomicU64::new(0),
            recoveries_started: AtomicU64::new(0),
            rebuilds_started: AtomicU64::new(0),
            fills_applied: AtomicU64::new(0),
            placements_submitted: AtomicU64::new(0),
            placements_succeeded: AtomicU64::new(0),
            placements_failed: AtomicU64::new(0),
            cancels_submitted: AtomicU64::new(0),
            cancels_succeeded: AtomicU64::new(0),
            cancels_failed: AtomicU64::new(0),
            active_orders: AtomicU64::new(0),
            placement_attempts: AtomicU64::new(0),
            inflight_placements: AtomicU64::new(0),
            inflight_cancels: AtomicU64::new(0),
            pending_fills: AtomicU64::new(0),
            deferred_placements: AtomicU64::new(0),
            deferred_cancels: AtomicU64::new(0),
            bid_ticks: AtomicU64::new(0),
            ask_ticks: AtomicU64::new(0),
        }
    }
}

/// Lock-free runtime measurements shared by network, strategy, and reporter threads.
#[derive(Clone, Default)]
pub struct RuntimeTelemetry {
    inner: Arc<RuntimeTelemetryInner>,
}

impl fmt::Debug for RuntimeTelemetry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RuntimeTelemetry")
    }
}

impl RuntimeTelemetry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns nanoseconds elapsed from this telemetry instance's monotonic epoch.
    ///
    /// Zero is reserved to mean "no causal receive timestamp".
    pub fn monotonic_time_ns(&self) -> u64 {
        let elapsed = self.inner.epoch.elapsed().as_nanos();
        u64::try_from(elapsed)
            .unwrap_or(u64::MAX - 1)
            .saturating_add(1)
    }

    pub fn observe_exchange_event(
        &self,
        source: EventSource,
        received_ns: u64,
        received_unix_us: i64,
        exchange_event_ms: u64,
        exchange_transaction_ms: u64,
    ) {
        let event_delay = wall_clock_delta_us(received_unix_us, exchange_event_ms);
        let transaction_delay = wall_clock_delta_us(received_unix_us, exchange_transaction_ms);
        match source {
            EventSource::MarketData => {
                self.inner.market_events.fetch_add(1, Ordering::Relaxed);
                self.inner
                    .last_market_receive_ns
                    .store(received_ns, Ordering::Relaxed);
                self.inner.market_event_delay_us.observe(event_delay);
                self.inner
                    .market_transaction_delay_us
                    .observe(transaction_delay);
            }
            EventSource::PrivateData => {
                self.inner.private_events.fetch_add(1, Ordering::Relaxed);
                self.inner
                    .last_private_receive_ns
                    .store(received_ns, Ordering::Relaxed);
                self.inner.private_event_delay_us.observe(event_delay);
                self.inner
                    .private_transaction_delay_us
                    .observe(transaction_delay);
            }
        }
    }

    pub fn observe_request_submitted(&self, origin: Option<EventOrigin>, submitted_ns: u64) {
        let Some(origin) = origin else {
            self.inner
                .background_requests
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        self.inner.event_requests.fetch_add(1, Ordering::Relaxed);
        let reaction = submitted_ns.saturating_sub(origin.received_ns());
        match origin.source() {
            EventSource::MarketData => self.inner.market_strategy_reaction_ns.observe(reaction),
            EventSource::PrivateData => self.inner.private_strategy_reaction_ns.observe(reaction),
        }
    }

    pub fn observe_request_sent(
        &self,
        origin: Option<EventOrigin>,
        submitted_ns: u64,
        dequeued_ns: u64,
        prepare_started_ns: u64,
        send_started_ns: u64,
        sent_ns: u64,
    ) {
        self.inner.requests_sent.fetch_add(1, Ordering::Relaxed);
        let dispatch = sent_ns.saturating_sub(submitted_ns);
        self.inner
            .request_queue_wait_ns
            .observe(prepare_started_ns.saturating_sub(submitted_ns));
        self.inner
            .request_enqueue_to_dequeue_ns
            .observe(dequeued_ns.saturating_sub(submitted_ns));
        self.inner
            .request_preflight_wait_ns
            .observe(prepare_started_ns.saturating_sub(dequeued_ns));
        self.inner
            .request_prepare_ns
            .observe(send_started_ns.saturating_sub(prepare_started_ns));
        self.inner
            .socket_send_ns
            .observe(sent_ns.saturating_sub(send_started_ns));
        let Some(origin) = origin else {
            self.inner.background_request_dispatch_ns.observe(dispatch);
            return;
        };
        self.inner.event_request_dispatch_ns.observe(dispatch);
        let end_to_end = sent_ns.saturating_sub(origin.received_ns());
        match origin.source() {
            EventSource::MarketData => self.inner.market_end_to_end_ns.observe(end_to_end),
            EventSource::PrivateData => self.inner.private_end_to_end_ns.observe(end_to_end),
        }
    }

    pub fn observe_request_send_failure(&self) {
        self.inner
            .request_send_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_report_dropped(&self) {
        self.inner.reports_dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_engine_phase(&self, phase: EngineRuntimePhase) {
        self.inner
            .engine_phase
            .store(phase as u64, Ordering::Relaxed);
    }

    pub fn observe_session_started(&self) {
        self.inner.sessions_started.fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_recovery_started(&self) {
        self.inner
            .recoveries_started
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_rebuild_started(&self) {
        self.inner.rebuilds_started.fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_fill_applied(&self) {
        self.inner.fills_applied.fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_placement_submitted(&self) {
        self.inner
            .placements_submitted
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_placement_completed(&self, succeeded: bool) {
        let counter = if succeeded {
            &self.inner.placements_succeeded
        } else {
            &self.inner.placements_failed
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_cancel_submitted(&self) {
        self.inner.cancels_submitted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_cancel_completed(&self, succeeded: bool) {
        let counter = if succeeded {
            &self.inner.cancels_succeeded
        } else {
            &self.inner.cancels_failed
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn update_engine_state(&self, state: EngineState) {
        self.inner
            .active_orders
            .store(state.active_orders, Ordering::Relaxed);
        self.inner
            .placement_attempts
            .store(state.placement_attempts, Ordering::Relaxed);
        self.inner
            .inflight_placements
            .store(state.inflight_placements, Ordering::Relaxed);
        self.inner
            .inflight_cancels
            .store(state.inflight_cancels, Ordering::Relaxed);
        self.inner
            .pending_fills
            .store(state.pending_fills, Ordering::Relaxed);
        self.inner
            .deferred_placements
            .store(state.deferred_placements, Ordering::Relaxed);
        self.inner
            .deferred_cancels
            .store(state.deferred_cancels, Ordering::Relaxed);
        self.inner
            .bid_ticks
            .store(state.bid_ticks, Ordering::Relaxed);
        self.inner
            .ask_ticks
            .store(state.ask_ticks, Ordering::Relaxed);
    }

    pub fn take_snapshot(&self) -> TelemetrySnapshot {
        let now = self.monotonic_time_ns();
        TelemetrySnapshot {
            market_events: self.inner.market_events.swap(0, Ordering::Relaxed),
            private_events: self.inner.private_events.swap(0, Ordering::Relaxed),
            event_requests: self.inner.event_requests.swap(0, Ordering::Relaxed),
            background_requests: self.inner.background_requests.swap(0, Ordering::Relaxed),
            requests_sent: self.inner.requests_sent.swap(0, Ordering::Relaxed),
            request_send_failures: self.inner.request_send_failures.swap(0, Ordering::Relaxed),
            reports_dropped: self.inner.reports_dropped.swap(0, Ordering::Relaxed),
            engine_phase: EngineRuntimePhase::from_atomic(
                self.inner.engine_phase.load(Ordering::Relaxed) as u8,
            ),
            sessions_started: self.inner.sessions_started.swap(0, Ordering::Relaxed),
            recoveries_started: self.inner.recoveries_started.swap(0, Ordering::Relaxed),
            rebuilds_started: self.inner.rebuilds_started.swap(0, Ordering::Relaxed),
            fills_applied: self.inner.fills_applied.swap(0, Ordering::Relaxed),
            placements_submitted: self.inner.placements_submitted.swap(0, Ordering::Relaxed),
            placements_succeeded: self.inner.placements_succeeded.swap(0, Ordering::Relaxed),
            placements_failed: self.inner.placements_failed.swap(0, Ordering::Relaxed),
            cancels_submitted: self.inner.cancels_submitted.swap(0, Ordering::Relaxed),
            cancels_succeeded: self.inner.cancels_succeeded.swap(0, Ordering::Relaxed),
            cancels_failed: self.inner.cancels_failed.swap(0, Ordering::Relaxed),
            engine_state: EngineState {
                active_orders: self.inner.active_orders.load(Ordering::Relaxed),
                placement_attempts: self.inner.placement_attempts.load(Ordering::Relaxed),
                inflight_placements: self.inner.inflight_placements.load(Ordering::Relaxed),
                inflight_cancels: self.inner.inflight_cancels.load(Ordering::Relaxed),
                pending_fills: self.inner.pending_fills.load(Ordering::Relaxed),
                deferred_placements: self.inner.deferred_placements.load(Ordering::Relaxed),
                deferred_cancels: self.inner.deferred_cancels.load(Ordering::Relaxed),
                bid_ticks: self.inner.bid_ticks.load(Ordering::Relaxed),
                ask_ticks: self.inner.ask_ticks.load(Ordering::Relaxed),
            },
            last_market_event_age_ns: event_age(
                now,
                self.inner.last_market_receive_ns.load(Ordering::Relaxed),
            ),
            last_private_event_age_ns: event_age(
                now,
                self.inner.last_private_receive_ns.load(Ordering::Relaxed),
            ),
            market_event_delay_us: self.inner.market_event_delay_us.take(),
            market_transaction_delay_us: self.inner.market_transaction_delay_us.take(),
            private_event_delay_us: self.inner.private_event_delay_us.take(),
            private_transaction_delay_us: self.inner.private_transaction_delay_us.take(),
            market_strategy_reaction_ns: self.inner.market_strategy_reaction_ns.take(),
            private_strategy_reaction_ns: self.inner.private_strategy_reaction_ns.take(),
            event_request_dispatch_ns: self.inner.event_request_dispatch_ns.take(),
            background_request_dispatch_ns: self.inner.background_request_dispatch_ns.take(),
            request_queue_wait_ns: self.inner.request_queue_wait_ns.take(),
            request_enqueue_to_dequeue_ns: self.inner.request_enqueue_to_dequeue_ns.take(),
            request_preflight_wait_ns: self.inner.request_preflight_wait_ns.take(),
            request_prepare_ns: self.inner.request_prepare_ns.take(),
            socket_send_ns: self.inner.socket_send_ns.take(),
            market_end_to_end_ns: self.inner.market_end_to_end_ns.take(),
            private_end_to_end_ns: self.inner.private_end_to_end_ns.take(),
        }
    }
}

fn wall_clock_delta_us(received_unix_us: i64, exchange_ms: u64) -> i64 {
    let exchange_us = i128::from(exchange_ms).saturating_mul(1_000);
    let delta = i128::from(received_unix_us) - exchange_us;
    i64::try_from(delta).unwrap_or(if delta.is_negative() {
        i64::MIN
    } else {
        i64::MAX
    })
}

fn event_age(now: u64, received: u64) -> Option<u64> {
    (received != 0).then(|| now.saturating_sub(received))
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LatencySnapshot {
    pub samples: u64,
    pub mean: u64,
    pub minimum: u64,
    pub p20_upper: u64,
    pub p30_upper: u64,
    pub p50_upper: u64,
    pub p99_upper: u64,
    pub maximum: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SignedLatencySnapshot {
    pub samples: u64,
    pub mean: i64,
    pub minimum: i64,
    pub p20_upper: i64,
    pub p30_upper: i64,
    pub p50_upper: i64,
    pub p99_upper: i64,
    pub maximum: i64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TelemetrySnapshot {
    pub market_events: u64,
    pub private_events: u64,
    pub event_requests: u64,
    pub background_requests: u64,
    pub requests_sent: u64,
    pub request_send_failures: u64,
    pub reports_dropped: u64,
    pub engine_phase: EngineRuntimePhase,
    pub sessions_started: u64,
    pub recoveries_started: u64,
    pub rebuilds_started: u64,
    pub fills_applied: u64,
    pub placements_submitted: u64,
    pub placements_succeeded: u64,
    pub placements_failed: u64,
    pub cancels_submitted: u64,
    pub cancels_succeeded: u64,
    pub cancels_failed: u64,
    pub engine_state: EngineState,
    pub last_market_event_age_ns: Option<u64>,
    pub last_private_event_age_ns: Option<u64>,
    pub market_event_delay_us: SignedLatencySnapshot,
    pub market_transaction_delay_us: SignedLatencySnapshot,
    pub private_event_delay_us: SignedLatencySnapshot,
    pub private_transaction_delay_us: SignedLatencySnapshot,
    pub market_strategy_reaction_ns: LatencySnapshot,
    pub private_strategy_reaction_ns: LatencySnapshot,
    pub event_request_dispatch_ns: LatencySnapshot,
    pub background_request_dispatch_ns: LatencySnapshot,
    pub request_queue_wait_ns: LatencySnapshot,
    pub request_enqueue_to_dequeue_ns: LatencySnapshot,
    pub request_preflight_wait_ns: LatencySnapshot,
    pub request_prepare_ns: LatencySnapshot,
    pub socket_send_ns: LatencySnapshot,
    pub market_end_to_end_ns: LatencySnapshot,
    pub private_end_to_end_ns: LatencySnapshot,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restores_nested_event_origins() {
        let outer = EventOrigin::new(EventSource::MarketData, 10).unwrap();
        let inner = EventOrigin::new(EventSource::PrivateData, 20).unwrap();
        with_event_origin(Some(outer), || {
            assert_eq!(current_event_origin(), Some(outer));
            with_event_origin(Some(inner), || {
                assert_eq!(current_event_origin(), Some(inner));
            });
            assert_eq!(current_event_origin(), Some(outer));
        });
        assert_eq!(current_event_origin(), None);
    }

    #[test]
    fn snapshots_causal_latency_without_response_time() {
        let telemetry = RuntimeTelemetry::new();
        let origin = EventOrigin::new(EventSource::PrivateData, 1_000).unwrap();
        telemetry.observe_request_submitted(Some(origin), 11_000);
        telemetry.observe_request_sent(Some(origin), 11_000, 14_000, 16_000, 23_000, 31_000);

        let snapshot = telemetry.take_snapshot();
        assert_eq!(snapshot.event_requests, 1);
        assert_eq!(snapshot.requests_sent, 1);
        assert_eq!(snapshot.private_strategy_reaction_ns.mean, 10_000);
        assert_eq!(snapshot.event_request_dispatch_ns.mean, 20_000);
        assert_eq!(snapshot.request_queue_wait_ns.mean, 5_000);
        assert_eq!(snapshot.request_enqueue_to_dequeue_ns.mean, 3_000);
        assert_eq!(snapshot.request_preflight_wait_ns.mean, 2_000);
        assert_eq!(snapshot.request_prepare_ns.mean, 7_000);
        assert_eq!(snapshot.socket_send_ns.mean, 8_000);
        assert_eq!(snapshot.background_request_dispatch_ns.samples, 0);
        assert_eq!(snapshot.private_end_to_end_ns.mean, 30_000);
    }

    #[test]
    fn keeps_negative_exchange_clock_deltas_visible() {
        let telemetry = RuntimeTelemetry::new();
        telemetry.observe_exchange_event(EventSource::MarketData, 10, 900_250, 1_000, 950);

        let snapshot = telemetry.take_snapshot();
        assert_eq!(snapshot.market_events, 1);
        assert_eq!(snapshot.market_event_delay_us.mean, -99_750);
        assert_eq!(snapshot.market_transaction_delay_us.mean, -49_750);
    }

    #[test]
    fn snapshots_unsigned_minimum_and_lower_percentiles() {
        let histogram = UnsignedHistogram::default();
        for value in [500, 1_000, 2_000, 5_000, 10_000] {
            histogram.observe(value);
        }

        let snapshot = histogram.take();
        assert_eq!(snapshot.samples, 5);
        assert_eq!(snapshot.mean, 3_700);
        assert_eq!(snapshot.minimum, 500);
        assert_eq!(snapshot.p20_upper, 500);
        assert_eq!(snapshot.p30_upper, 1_000);
        assert_eq!(snapshot.p50_upper, 2_000);
        assert_eq!(snapshot.p99_upper, 10_000);
        assert_eq!(snapshot.maximum, 10_000);

        assert_eq!(histogram.take(), LatencySnapshot::default());
    }

    #[test]
    fn snapshots_signed_minimum_and_lower_percentiles() {
        let histogram = SignedHistogram::default();
        for value in [-100, -20, 0, 10, 50] {
            histogram.observe(value);
        }

        let snapshot = histogram.take();
        assert_eq!(snapshot.samples, 5);
        assert_eq!(snapshot.mean, -12);
        assert_eq!(snapshot.minimum, -100);
        assert_eq!(snapshot.p20_upper, -100);
        assert_eq!(snapshot.p30_upper, -20);
        assert_eq!(snapshot.p50_upper, 0);
        assert_eq!(snapshot.p99_upper, 50);
        assert_eq!(snapshot.maximum, 50);

        assert_eq!(histogram.take(), SignedLatencySnapshot::default());
    }

    #[test]
    fn engine_counters_reset_while_gauges_persist() {
        let telemetry = RuntimeTelemetry::new();
        telemetry.set_engine_phase(EngineRuntimePhase::Running);
        telemetry.observe_session_started();
        telemetry.observe_fill_applied();
        telemetry.observe_placement_submitted();
        telemetry.observe_placement_completed(true);
        telemetry.update_engine_state(EngineState {
            active_orders: 6,
            inflight_placements: 2,
            bid_ticks: 99,
            ask_ticks: 100,
            ..EngineState::default()
        });

        let first = telemetry.take_snapshot();
        assert_eq!(first.engine_phase, EngineRuntimePhase::Running);
        assert_eq!(first.sessions_started, 1);
        assert_eq!(first.fills_applied, 1);
        assert_eq!(first.placements_submitted, 1);
        assert_eq!(first.placements_succeeded, 1);
        assert_eq!(first.engine_state.active_orders, 6);
        assert_eq!(first.engine_state.inflight_placements, 2);

        let second = telemetry.take_snapshot();
        assert_eq!(second.sessions_started, 0);
        assert_eq!(second.fills_applied, 0);
        assert_eq!(second.engine_state.active_orders, 6);
        assert_eq!(second.engine_state.bid_ticks, 99);
    }
}
