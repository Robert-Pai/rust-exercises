mod config;
mod strategy_runtime;

use std::{
    fs,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use exchange_binance_usdm::BinanceUsdm;
use maker_engine::MakerEngine;
use maker_runtime::{LatencySnapshot, RuntimeTelemetry, SignedLatencySnapshot, TelemetrySnapshot};
use tracing::{error, info};
use tracing_appender::{non_blocking::WorkerGuard, rolling};
use tracing_subscriber::{Layer, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::{AppConfig, LoggingSettings, config_path_from_args};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let config_path = config_path_from_args()?;
    let config = AppConfig::load(&config_path)?;
    let (exchange_config, credentials, engine_config, logging, strategy_settings) =
        config.into_components()?;

    let _log_guard = init_logging(&logging)?;

    info!(
        config = %config_path.display(),
        symbol = %engine_config.symbol(),
        "starting live maker"
    );

    let telemetry = RuntimeTelemetry::new();
    let exchange = BinanceUsdm::new(
        exchange_config.with_telemetry(telemetry.clone()),
        credentials,
    )
    .context("failed to create Binance USD-M adapter")?;
    let engine =
        MakerEngine::new(engine_config, Box::new(exchange)).with_telemetry(telemetry.clone());
    let strategy = strategy_runtime::spawn(engine, strategy_settings)
        .context("failed to start maker strategy thread")?;
    let reporter = tokio::spawn(report_telemetry(
        telemetry.clone(),
        logging.telemetry_interval(),
    ));

    let result = strategy
        .run_until(async {
            match tokio::signal::ctrl_c().await {
                Ok(()) => info!("shutdown signal received"),
                Err(error) => error!(%error, "failed to listen for shutdown signal"),
            }
        })
        .await;
    reporter.abort();
    log_telemetry_snapshot(&telemetry.take_snapshot(), logging.telemetry_interval());
    if let Err(error) = &result {
        error!(error = %format!("{error:#}"), "maker strategy stopped with an error");
    }
    result.context("maker engine stopped with an error")
}

async fn report_telemetry(telemetry: RuntimeTelemetry, period: Duration) {
    let mut timer = tokio::time::interval(period);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    timer.tick().await;
    loop {
        timer.tick().await;
        log_telemetry_snapshot(&telemetry.take_snapshot(), period);
    }
}

fn log_telemetry_snapshot(snapshot: &TelemetrySnapshot, period: Duration) {
    let market_age_ms = snapshot.last_market_event_age_ns.map(|age| age / 1_000_000);
    let private_age_ms = snapshot
        .last_private_event_age_ns
        .map(|age| age / 1_000_000);
    let state = snapshot.engine_state;
    info!(
        window_ms = period.as_millis(),
        phase = snapshot.engine_phase.as_str(),
        market_events = snapshot.market_events,
        private_events = snapshot.private_events,
        event_requests = snapshot.event_requests,
        background_requests = snapshot.background_requests,
        requests_sent = snapshot.requests_sent,
        request_send_failures = snapshot.request_send_failures,
        sessions_started = snapshot.sessions_started,
        recoveries_started = snapshot.recoveries_started,
        rebuilds_started = snapshot.rebuilds_started,
        fills_applied = snapshot.fills_applied,
        placements_submitted = snapshot.placements_submitted,
        placements_succeeded = snapshot.placements_succeeded,
        placements_failed = snapshot.placements_failed,
        cancels_submitted = snapshot.cancels_submitted,
        cancels_succeeded = snapshot.cancels_succeeded,
        cancels_failed = snapshot.cancels_failed,
        active_orders = state.active_orders,
        placement_attempts = state.placement_attempts,
        inflight_placements = state.inflight_placements,
        inflight_cancels = state.inflight_cancels,
        pending_fills = state.pending_fills,
        deferred_placements = state.deferred_placements,
        deferred_cancels = state.deferred_cancels,
        bid_ticks = state.bid_ticks,
        ask_ticks = state.ask_ticks,
        ?market_age_ms,
        ?private_age_ms,
        "maker runtime telemetry"
    );

    log_signed_latency(
        "market_exchange_event_delay",
        &snapshot.market_event_delay_ms,
    );
    log_signed_latency(
        "market_exchange_transaction_delay",
        &snapshot.market_transaction_delay_ms,
    );
    log_signed_latency(
        "private_exchange_event_delay",
        &snapshot.private_event_delay_ms,
    );
    log_signed_latency(
        "private_exchange_transaction_delay",
        &snapshot.private_transaction_delay_ms,
    );
    log_latency(
        "market_strategy_reaction",
        &snapshot.market_strategy_reaction_ns,
    );
    log_latency(
        "private_strategy_reaction",
        &snapshot.private_strategy_reaction_ns,
    );
    log_latency(
        "event_request_dispatch_send",
        &snapshot.event_request_dispatch_ns,
    );
    log_latency(
        "background_request_dispatch_send",
        &snapshot.background_request_dispatch_ns,
    );
    log_latency("market_receive_to_send", &snapshot.market_end_to_end_ns);
    log_latency("private_receive_to_send", &snapshot.private_end_to_end_ns);
}

fn log_latency(metric: &str, latency: &LatencySnapshot) {
    if latency.samples == 0 {
        return;
    }
    info!(
        metric,
        unit = "ns",
        samples = latency.samples,
        mean = latency.mean,
        p50_upper = latency.p50_upper,
        p99_upper = latency.p99_upper,
        maximum = latency.maximum,
        "maker latency telemetry"
    );
}

fn log_signed_latency(metric: &str, latency: &SignedLatencySnapshot) {
    if latency.samples == 0 {
        return;
    }
    info!(
        metric,
        unit = "ms",
        samples = latency.samples,
        mean = latency.mean,
        p50_upper = latency.p50_upper,
        p99_upper = latency.p99_upper,
        maximum = latency.maximum,
        "maker exchange latency telemetry"
    );
}

fn init_logging(settings: &LoggingSettings) -> Result<WorkerGuard> {
    fs::create_dir_all(settings.directory()).with_context(|| {
        format!(
            "failed to create log directory {}",
            settings.directory().display()
        )
    })?;
    cleanup_old_logs(settings.directory(), settings.retention_days())?;

    let appender = rolling::daily(settings.directory(), "maker.log");
    let (file_writer, guard) = tracing_appender::non_blocking(appender);
    let level = settings.level();
    let stdout_layer = settings.stdout_enabled().then(|| {
        fmt::layer()
            .compact()
            .with_target(false)
            .with_ansi(false)
            .with_filter(level)
    });
    let file_layer = fmt::layer()
        .json()
        .with_target(false)
        .with_ansi(false)
        .with_writer(file_writer)
        .with_filter(level);

    tracing_subscriber::registry()
        .with(stdout_layer)
        .with(file_layer)
        .try_init()
        .map_err(|error| anyhow::anyhow!("failed to initialize logging: {error}"))?;
    Ok(guard)
}

fn cleanup_old_logs(directory: &Path, retention_days: u64) -> Result<()> {
    let age = Duration::from_secs(retention_days.saturating_mul(24 * 60 * 60));
    let cutoff = SystemTime::now().checked_sub(age).unwrap_or(UNIX_EPOCH);
    for entry in fs::read_dir(directory)
        .with_context(|| format!("failed to read log directory {}", directory.display()))?
    {
        let entry = entry.with_context(|| "failed to inspect a log directory entry")?;
        let file_type = entry
            .file_type()
            .with_context(|| "failed to inspect a log file type")?;
        let name = entry.file_name();
        if !file_type.is_file() || !name.to_string_lossy().starts_with("maker.log.") {
            continue;
        }
        let modified = entry
            .metadata()
            .and_then(|metadata| metadata.modified())
            .with_context(|| format!("failed to inspect log file {}", entry.path().display()))?;
        if modified < cutoff {
            fs::remove_file(entry.path()).with_context(|| {
                format!(
                    "failed to remove expired log file {}",
                    entry.path().display()
                )
            })?;
        }
    }
    Ok(())
}
