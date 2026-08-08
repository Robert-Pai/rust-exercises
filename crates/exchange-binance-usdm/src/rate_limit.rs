use std::{
    cmp,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use maker_ports::{ExchangeError, ExchangeErrorKind, ExchangeResult};

const STATE_COUNT_MASK: u64 = u32::MAX as u64;
const UNKNOWN_LIMIT: u32 = u32::MAX;
const WINDOW_COUNT: usize = 3;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum RateLimitType {
    RawRequests,
    RequestWeight,
    Orders,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WindowSpec {
    kind: RateLimitType,
    interval: Duration,
}

const WINDOW_SPECS: [WindowSpec; WINDOW_COUNT] = [
    WindowSpec {
        kind: RateLimitType::RequestWeight,
        interval: Duration::from_secs(60),
    },
    WindowSpec {
        kind: RateLimitType::Orders,
        interval: Duration::from_secs(10),
    },
    WindowSpec {
        kind: RateLimitType::Orders,
        interval: Duration::from_secs(60),
    },
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RateLimitSnapshot {
    pub(crate) kind: RateLimitType,
    pub(crate) interval: Duration,
    pub(crate) limit: Option<u64>,
    pub(crate) count: u64,
}

impl RateLimitSnapshot {
    pub(crate) fn from_wire(
        rate_limit_type: &str,
        interval: &str,
        interval_num: u64,
        limit: u64,
        count: Option<u64>,
    ) -> Option<Self> {
        let kind = match rate_limit_type {
            "RAW_REQUEST" | "RAW_REQUESTS" => RateLimitType::RawRequests,
            "REQUEST_WEIGHT" => RateLimitType::RequestWeight,
            "ORDER" | "ORDERS" => RateLimitType::Orders,
            _ => return None,
        };
        let unit = match interval {
            "SECOND" => Duration::from_secs(1),
            "MINUTE" => Duration::from_secs(60),
            "HOUR" => Duration::from_secs(60 * 60),
            "DAY" => Duration::from_secs(24 * 60 * 60),
            _ => return None,
        };
        let interval = unit.checked_mul(u32::try_from(interval_num).ok()?)?;
        Some(Self {
            kind,
            interval,
            limit: Some(limit),
            count: count.unwrap_or(0),
        })
    }

    fn usage(kind: RateLimitType, interval: Duration, count: u64) -> Self {
        Self {
            kind,
            interval,
            limit: None,
            count,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RequestCost {
    pub(crate) raw_requests: u64,
    pub(crate) request_weight: u64,
    pub(crate) orders: u64,
}

impl RequestCost {
    pub(crate) const GENERIC: Self = Self {
        raw_requests: 1,
        request_weight: 1,
        orders: 0,
    };

    pub(crate) const ORDER: Self = Self {
        raw_requests: 1,
        request_weight: 1,
        orders: 1,
    };

    const fn for_kind(self, kind: RateLimitType) -> u64 {
        match kind {
            RateLimitType::RawRequests => self.raw_requests,
            RateLimitType::RequestWeight => self.request_weight,
            RateLimitType::Orders => self.orders,
        }
    }
}

#[derive(Debug)]
struct AtomicWindow {
    // The high 32 bits contain the locally assigned exchangeInfo version and
    // the low 32 bits contain the limit. Only a newer response may replace it.
    limit_state: AtomicU64,
    // Both counters pack the UTC fixed-window generation in the high 32 bits
    // and the absolute count in the low 32 bits.
    local_state: AtomicU64,
    server_state: AtomicU64,
}

impl AtomicWindow {
    fn new() -> Self {
        Self {
            limit_state: AtomicU64::new(pack_state(0, UNKNOWN_LIMIT)),
            local_state: AtomicU64::new(0),
            server_state: AtomicU64::new(0),
        }
    }

    fn limit(&self) -> u32 {
        state_count(self.limit_state.load(Ordering::Acquire))
    }

    fn update_limit(&self, version: u32, limit: u32) {
        let mut observed = self.limit_state.load(Ordering::Acquire);
        loop {
            if state_generation(observed) >= version {
                return;
            }
            match self.limit_state.compare_exchange_weak(
                observed,
                pack_state(version, limit),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(actual) => observed = actual,
            }
        }
    }

    fn update_server_count(&self, generation: u32, count: u32) {
        // A packed state is monotonic until the 32-bit one-second generation
        // wraps. That cannot occur before 2106 and cannot collide with live
        // process state, so fetch_max rejects late responses from older windows.
        self.server_state
            .fetch_max(pack_state(generation, count), Ordering::AcqRel);
    }

    fn effective_count(&self, generation: u32) -> u32 {
        let local = count_in_generation(self.local_state.load(Ordering::Acquire), generation);
        let server = count_in_generation(self.server_state.load(Ordering::Acquire), generation);
        cmp::max(local, server)
    }

    fn reserve(
        &self,
        spec: WindowSpec,
        cost: u32,
    ) -> ExchangeResult<Result<Reservation, tokio::time::Instant>> {
        loop {
            let clock = WindowClock::now(spec.interval)?;
            let limit = self.limit();
            let observed = self.local_state.load(Ordering::Acquire);
            let local = count_in_generation(observed, clock.generation);
            let server =
                count_in_generation(self.server_state.load(Ordering::Acquire), clock.generation);
            let effective = cmp::max(local, server);
            let Some(reserved) = effective.checked_add(cost) else {
                return Ok(Err(clock.retry_at));
            };
            if reserved > limit {
                return Ok(Err(clock.retry_at));
            }

            match self.local_state.compare_exchange_weak(
                observed,
                pack_state(clock.generation, reserved),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let after = WindowClock::now(spec.interval)?;
                    if after.generation != clock.generation {
                        self.rollback(Reservation {
                            generation: clock.generation,
                            cost,
                        });
                        continue;
                    }

                    let server_after = count_in_generation(
                        self.server_state.load(Ordering::Acquire),
                        clock.generation,
                    );
                    if cmp::max(reserved, server_after) > self.limit() {
                        self.rollback(Reservation {
                            generation: clock.generation,
                            cost,
                        });
                        return Ok(Err(clock.retry_at));
                    }
                    return Ok(Ok(Reservation {
                        generation: clock.generation,
                        cost,
                    }));
                }
                Err(_) => continue,
            }
        }
    }

    fn rollback(&self, reservation: Reservation) {
        let mut observed = self.local_state.load(Ordering::Acquire);
        loop {
            if state_generation(observed) != reservation.generation {
                return;
            }
            let count = state_count(observed);
            if count < reservation.cost {
                return;
            }
            let replacement = pack_state(reservation.generation, count - reservation.cost);
            match self.local_state.compare_exchange_weak(
                observed,
                replacement,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(actual) => observed = actual,
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Reservation {
    generation: u32,
    cost: u32,
}

#[derive(Clone, Copy, Debug)]
struct WindowClock {
    generation: u32,
    retry_at: tokio::time::Instant,
    boundary_unix_nanos: u64,
}

impl WindowClock {
    fn now(interval: Duration) -> ExchangeResult<Self> {
        let now = tokio::time::Instant::now();
        let unix_nanos = unix_time_nanos()?;
        let interval_nanos = duration_nanos(interval)?;
        if interval_nanos == 0 {
            return Err(ExchangeError::new(
                ExchangeErrorKind::InvalidRequest,
                "Binance rate-limit interval must be positive",
            ));
        }
        let window = unix_nanos / interval_nanos;
        let remaining = interval_nanos - unix_nanos % interval_nanos;
        let retry_at = now
            .checked_add(Duration::from_nanos(remaining))
            .unwrap_or(now);
        Ok(Self {
            generation: window as u32,
            retry_at,
            boundary_unix_nanos: unix_nanos.saturating_add(remaining),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AcquireDecision {
    Ready,
    RetryAt(tokio::time::Instant),
    DeadlineExceeded,
}

#[derive(Debug)]
pub(crate) struct AtomicFixedWindowLimiter {
    windows: [AtomicWindow; WINDOW_COUNT],
    next_limit_version: AtomicU32,
    blocked_until_unix_nanos: AtomicU64,
    invalid: AtomicBool,
}

pub(crate) type SharedRequestRateLimiter = Arc<AtomicFixedWindowLimiter>;

pub(crate) struct RequestRateLimiter;

impl RequestRateLimiter {
    pub(crate) fn shared() -> SharedRequestRateLimiter {
        Arc::new(AtomicFixedWindowLimiter {
            windows: std::array::from_fn(|_| AtomicWindow::new()),
            next_limit_version: AtomicU32::new(1),
            blocked_until_unix_nanos: AtomicU64::new(0),
            invalid: AtomicBool::new(false),
        })
    }

    pub(crate) fn begin_limit_refresh(shared: &SharedRequestRateLimiter) -> ExchangeResult<u32> {
        let version = shared.next_limit_version.fetch_add(1, Ordering::Relaxed);
        if version == 0 || version == u32::MAX {
            shared.invalid.store(true, Ordering::Release);
            return Err(invalid_limiter(
                "Binance rate-limit version counter exhausted",
            ));
        }
        Ok(version)
    }

    pub(crate) fn reject_unrecognized_telemetry(
        shared: &SharedRequestRateLimiter,
    ) -> ExchangeError {
        invalidate(
            shared,
            "Binance returned an unrecognized rate-limit type or interval",
        )
    }

    pub(crate) fn update_limits(
        shared: &SharedRequestRateLimiter,
        version: u32,
        snapshots: &[RateLimitSnapshot],
    ) -> ExchangeResult<()> {
        let mut limits = [None; WINDOW_COUNT];
        for snapshot in snapshots {
            let Some(limit) = snapshot.limit else {
                continue;
            };
            let index = window_index(snapshot.kind, snapshot.interval)
                .ok_or_else(|| unsupported_window(shared, snapshot))?;
            let limit = u32::try_from(limit).map_err(|_| {
                invalidate(
                    shared,
                    "Binance rate-limit value exceeds the atomic counter capacity",
                )
            })?;
            if limits[index].replace(limit).is_some() {
                return Err(invalidate(
                    shared,
                    "Binance exchangeInfo returned a duplicate rate-limit window",
                ));
            }
        }
        if limits.iter().any(Option::is_none) {
            return Err(invalidate(
                shared,
                "Binance exchangeInfo omitted a required rate-limit window",
            ));
        }
        for (window, limit) in shared.windows.iter().zip(limits) {
            window.update_limit(version, limit.expect("all fixed limits were validated"));
        }
        Ok(())
    }

    pub(crate) fn update_counts(
        shared: &SharedRequestRateLimiter,
        snapshots: &[RateLimitSnapshot],
    ) -> ExchangeResult<()> {
        for snapshot in snapshots {
            let index = window_index(snapshot.kind, snapshot.interval)
                .ok_or_else(|| unsupported_window(shared, snapshot))?;
            let count = u32::try_from(snapshot.count).map_err(|_| {
                invalidate(
                    shared,
                    "Binance rate-limit count exceeds the atomic counter capacity",
                )
            })?;
            let clock = WindowClock::now(WINDOW_SPECS[index].interval)?;
            shared.windows[index].update_server_count(clock.generation, count);
        }
        Ok(())
    }

    /// Atomically reserves request cost in every fixed window. This function
    /// never waits: callers decide how to schedule a RetryAt result.
    pub(crate) fn acquire(
        shared: &SharedRequestRateLimiter,
        cost: RequestCost,
        deadline: tokio::time::Instant,
    ) -> ExchangeResult<AcquireDecision> {
        if shared.invalid.load(Ordering::Acquire) {
            return Err(invalid_limiter(
                "Binance rate limiter is fail-closed after invalid telemetry",
            ));
        }

        if let Some(retry_at) = blocked_retry_at(shared)? {
            return Ok(classify_retry(retry_at, deadline));
        }

        let mut retry_at = None;
        for (index, spec) in WINDOW_SPECS.iter().copied().enumerate() {
            let cost = request_cost_u32(cost.for_kind(spec.kind))?;
            if cost == 0 {
                continue;
            }
            let clock = WindowClock::now(spec.interval)?;
            let effective = shared.windows[index].effective_count(clock.generation);
            if u64::from(effective).saturating_add(u64::from(cost))
                > u64::from(shared.windows[index].limit())
            {
                retry_at = Some(
                    retry_at.map_or(clock.retry_at, |current: tokio::time::Instant| {
                        cmp::max(current, clock.retry_at)
                    }),
                );
            }
        }
        if let Some(retry_at) = retry_at {
            return Ok(classify_retry(retry_at, deadline));
        }

        let mut reservations: [Option<Reservation>; WINDOW_COUNT] = [None; WINDOW_COUNT];
        for (index, spec) in WINDOW_SPECS.iter().copied().enumerate() {
            let cost = request_cost_u32(cost.for_kind(spec.kind))?;
            if cost == 0 {
                continue;
            }
            match shared.windows[index].reserve(spec, cost)? {
                Ok(reservation) => reservations[index] = Some(reservation),
                Err(retry_at) => {
                    rollback_all(shared, &reservations);
                    return Ok(classify_retry(retry_at, deadline));
                }
            }
        }

        if let Some(retry_at) = blocked_retry_at(shared)? {
            rollback_all(shared, &reservations);
            return Ok(classify_retry(retry_at, deadline));
        }
        Ok(AcquireDecision::Ready)
    }

    pub(crate) fn observe_error(
        shared: &SharedRequestRateLimiter,
        error: Option<&ExchangeError>,
    ) -> ExchangeResult<()> {
        let Some(error) = error.filter(|error| error.kind() == ExchangeErrorKind::RateLimited)
        else {
            return Ok(());
        };
        let now = unix_time_nanos()?;
        let explicit = error
            .retry_after()
            .map(duration_nanos)
            .transpose()?
            .map(|delay| now.saturating_add(delay));
        let fallback = if explicit.is_none() {
            saturated_window_boundary(shared)?.or_else(|| {
                duration_nanos(Duration::from_secs(1))
                    .ok()
                    .map(|delay| now.saturating_add(delay))
            })
        } else {
            None
        };
        let blocked_until = explicit.or(fallback).unwrap_or(now);
        shared
            .blocked_until_unix_nanos
            .fetch_max(blocked_until, Ordering::AcqRel);
        Ok(())
    }
}

fn rollback_all(
    shared: &SharedRequestRateLimiter,
    reservations: &[Option<Reservation>; WINDOW_COUNT],
) {
    for (window, reservation) in shared.windows.iter().zip(reservations) {
        if let Some(reservation) = reservation {
            window.rollback(*reservation);
        }
    }
}

fn blocked_retry_at(
    shared: &SharedRequestRateLimiter,
) -> ExchangeResult<Option<tokio::time::Instant>> {
    let now_unix = unix_time_nanos()?;
    let blocked_until = shared.blocked_until_unix_nanos.load(Ordering::Acquire);
    if blocked_until <= now_unix {
        return Ok(None);
    }
    let delay = Duration::from_nanos(blocked_until - now_unix);
    tokio::time::Instant::now()
        .checked_add(delay)
        .map(Some)
        .ok_or_else(|| invalidate(shared, "Binance rate-limit retry instant overflowed"))
}

fn saturated_window_boundary(shared: &SharedRequestRateLimiter) -> ExchangeResult<Option<u64>> {
    let mut boundary = None;
    for (index, spec) in WINDOW_SPECS.iter().copied().enumerate() {
        let clock = WindowClock::now(spec.interval)?;
        if shared.windows[index].effective_count(clock.generation) >= shared.windows[index].limit()
        {
            boundary = Some(boundary.map_or(clock.boundary_unix_nanos, |current: u64| {
                cmp::max(current, clock.boundary_unix_nanos)
            }));
        }
    }
    Ok(boundary)
}

fn classify_retry(
    retry_at: tokio::time::Instant,
    deadline: tokio::time::Instant,
) -> AcquireDecision {
    if retry_at >= deadline {
        AcquireDecision::DeadlineExceeded
    } else {
        AcquireDecision::RetryAt(retry_at)
    }
}

fn window_index(kind: RateLimitType, interval: Duration) -> Option<usize> {
    WINDOW_SPECS
        .iter()
        .position(|spec| spec.kind == kind && spec.interval == interval)
}

fn pack_state(generation: u32, count: u32) -> u64 {
    (u64::from(generation) << 32) | u64::from(count)
}

fn state_generation(state: u64) -> u32 {
    (state >> 32) as u32
}

fn state_count(state: u64) -> u32 {
    (state & STATE_COUNT_MASK) as u32
}

fn count_in_generation(state: u64, generation: u32) -> u32 {
    if state_generation(state) == generation {
        state_count(state)
    } else {
        0
    }
}

fn request_cost_u32(cost: u64) -> ExchangeResult<u32> {
    u32::try_from(cost).map_err(|_| {
        ExchangeError::new(
            ExchangeErrorKind::InvalidRequest,
            "Binance request cost exceeds the atomic counter capacity",
        )
    })
}

fn duration_nanos(duration: Duration) -> ExchangeResult<u64> {
    u64::try_from(duration.as_nanos()).map_err(|_| {
        ExchangeError::new(
            ExchangeErrorKind::InvalidRequest,
            "Binance rate-limit duration exceeds the supported range",
        )
    })
}

fn unix_time_nanos() -> ExchangeResult<u64> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| {
        ExchangeError::new(
            ExchangeErrorKind::StateConflict,
            "local system time is before the Unix epoch",
        )
    })?;
    u64::try_from(elapsed.as_nanos()).map_err(|_| {
        ExchangeError::new(
            ExchangeErrorKind::StateConflict,
            "local system time exceeds the Binance rate-limit clock range",
        )
    })
}

fn invalidate(shared: &SharedRequestRateLimiter, message: &'static str) -> ExchangeError {
    shared.invalid.store(true, Ordering::Release);
    invalid_limiter(message)
}

fn unsupported_window(
    shared: &SharedRequestRateLimiter,
    snapshot: &RateLimitSnapshot,
) -> ExchangeError {
    shared.invalid.store(true, Ordering::Release);
    ExchangeError::new(
        ExchangeErrorKind::InvalidResponse,
        format!(
            "unsupported Binance rate-limit window {:?}/{:?}",
            snapshot.kind, snapshot.interval
        ),
    )
}

fn invalid_limiter(message: impl Into<String>) -> ExchangeError {
    ExchangeError::new(ExchangeErrorKind::StateConflict, message)
}

pub(crate) fn rest_header_snapshot(name: &str, value: &str) -> Option<RateLimitSnapshot> {
    let (kind, suffix) = if let Some(suffix) = name.strip_prefix("X-MBX-USED-WEIGHT-") {
        (RateLimitType::RequestWeight, suffix)
    } else {
        (
            RateLimitType::Orders,
            name.strip_prefix("X-MBX-ORDER-COUNT-")?,
        )
    };
    let (interval_num, interval) = suffix.split_at(suffix.len().checked_sub(1)?);
    let interval_num = interval_num.parse::<u64>().ok()?;
    let interval = match interval {
        "S" => Duration::from_secs(1).checked_mul(u32::try_from(interval_num).ok()?)?,
        "M" => Duration::from_secs(60).checked_mul(u32::try_from(interval_num).ok()?)?,
        "H" => Duration::from_secs(60 * 60).checked_mul(u32::try_from(interval_num).ok()?)?,
        "D" => Duration::from_secs(24 * 60 * 60).checked_mul(u32::try_from(interval_num).ok()?)?,
        _ => return None,
    };
    Some(RateLimitSnapshot::usage(
        kind,
        interval,
        value.parse().ok()?,
    ))
}

#[cfg(test)]
mod tests {
    use std::{sync::Barrier, thread};

    use super::*;

    fn configured_limiter(
        request_weight: u64,
        orders_ten_seconds: u64,
        orders_minute: u64,
    ) -> SharedRequestRateLimiter {
        let limiter = RequestRateLimiter::shared();
        let version = RequestRateLimiter::begin_limit_refresh(&limiter).unwrap();
        RequestRateLimiter::update_limits(
            &limiter,
            version,
            &[
                RateLimitSnapshot {
                    kind: RateLimitType::RequestWeight,
                    interval: Duration::from_secs(60),
                    limit: Some(request_weight),
                    count: 0,
                },
                RateLimitSnapshot {
                    kind: RateLimitType::Orders,
                    interval: Duration::from_secs(10),
                    limit: Some(orders_ten_seconds),
                    count: 0,
                },
                RateLimitSnapshot {
                    kind: RateLimitType::Orders,
                    interval: Duration::from_secs(60),
                    limit: Some(orders_minute),
                    count: 0,
                },
            ],
        )
        .unwrap();
        limiter
    }

    #[test]
    fn concurrent_reservations_cannot_consume_the_same_capacity() {
        let limiter = configured_limiter(1, 100, 100);
        let barrier = Arc::new(Barrier::new(3));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let limiter = limiter.clone();
            let barrier = barrier.clone();
            workers.push(thread::spawn(move || {
                barrier.wait();
                RequestRateLimiter::acquire(
                    &limiter,
                    RequestCost::GENERIC,
                    tokio::time::Instant::now() + Duration::from_secs(120),
                )
                .unwrap()
            }));
        }
        barrier.wait();
        let decisions = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            decisions
                .iter()
                .filter(|decision| matches!(decision, AcquireDecision::Ready))
                .count(),
            1
        );
        assert_eq!(
            decisions
                .iter()
                .filter(|decision| matches!(decision, AcquireDecision::RetryAt(_)))
                .count(),
            1
        );
    }

    #[test]
    fn rollback_preserves_a_newer_server_telemetry_floor() {
        let limiter = configured_limiter(100, 100, 100);
        RequestRateLimiter::update_counts(
            &limiter,
            &[RateLimitSnapshot::usage(
                RateLimitType::RequestWeight,
                Duration::from_secs(60),
                5,
            )],
        )
        .unwrap();

        let reservation = limiter.windows[0]
            .reserve(WINDOW_SPECS[0], 1)
            .unwrap()
            .unwrap();
        RequestRateLimiter::update_counts(
            &limiter,
            &[RateLimitSnapshot::usage(
                RateLimitType::RequestWeight,
                Duration::from_secs(60),
                10,
            )],
        )
        .unwrap();
        limiter.windows[0].rollback(reservation);

        let clock = WindowClock::now(Duration::from_secs(60)).unwrap();
        assert_eq!(limiter.windows[0].effective_count(clock.generation), 10);
        assert_eq!(
            count_in_generation(
                limiter.windows[0].local_state.load(Ordering::Acquire),
                clock.generation
            ),
            5
        );
    }

    #[test]
    fn newer_exchange_info_limit_wins_over_a_late_response() {
        let limiter = configured_limiter(100, 100, 100);
        let older = RequestRateLimiter::begin_limit_refresh(&limiter).unwrap();
        let newer = RequestRateLimiter::begin_limit_refresh(&limiter).unwrap();
        let snapshots = |weight| {
            [
                RateLimitSnapshot {
                    kind: RateLimitType::RequestWeight,
                    interval: Duration::from_secs(60),
                    limit: Some(weight),
                    count: 0,
                },
                RateLimitSnapshot {
                    kind: RateLimitType::Orders,
                    interval: Duration::from_secs(10),
                    limit: Some(100),
                    count: 0,
                },
                RateLimitSnapshot {
                    kind: RateLimitType::Orders,
                    interval: Duration::from_secs(60),
                    limit: Some(100),
                    count: 0,
                },
            ]
        };
        RequestRateLimiter::update_limits(&limiter, newer, &snapshots(50)).unwrap();
        RequestRateLimiter::update_limits(&limiter, older, &snapshots(200)).unwrap();

        assert_eq!(limiter.windows[0].limit(), 50);
    }

    #[test]
    fn unsupported_window_fails_closed() {
        let limiter = configured_limiter(100, 100, 100);
        let result = RequestRateLimiter::update_counts(
            &limiter,
            &[RateLimitSnapshot::usage(
                RateLimitType::RawRequests,
                Duration::from_secs(300),
                1,
            )],
        );
        assert!(result.is_err());
        assert!(
            RequestRateLimiter::acquire(
                &limiter,
                RequestCost::GENERIC,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .is_err()
        );
    }

    #[test]
    fn parses_rest_usage_headers() {
        let snapshot = rest_header_snapshot("X-MBX-ORDER-COUNT-10S", "12").unwrap();
        assert_eq!(snapshot.kind, RateLimitType::Orders);
        assert_eq!(snapshot.interval, Duration::from_secs(10));
        assert_eq!(snapshot.count, 12);
        assert_eq!(snapshot.limit, None);
    }
}
