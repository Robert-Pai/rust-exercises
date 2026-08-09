mod config;
mod strategy_runtime;

use std::{
    fs,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use exchange_binance_usdm::BinanceUsdm;
use maker_engine::{EngineReport, MakerEngine};
use maker_runtime::{
    LatencySnapshot, RuntimeTelemetry, SignedLatencySnapshot, SpscConsumer, TelemetrySnapshot,
    spsc_channel,
};
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
    let (reports, report_receiver) = spsc_channel(1_024);
    let engine = MakerEngine::new(engine_config, Box::new(exchange))
        .with_telemetry(telemetry.clone())
        .with_reporter(reports);
    let strategy = strategy_runtime::spawn(engine, strategy_settings)
        .context("failed to start maker strategy thread")?;
    let reporter = tokio::spawn(report_telemetry(
        telemetry.clone(),
        logging.telemetry_interval(),
        report_receiver,
    ));

    let result = strategy.run_until(shutdown_signal()).await;
    if let Err(error) = reporter.await {
        error!(%error, "runtime reporter task failed");
    }
    log_telemetry_snapshot(&telemetry.take_snapshot(), logging.telemetry_interval());
    if let Err(error) = &result {
        error!(error = %format!("{error:#}"), "maker strategy stopped with an error");
    }
    result.context("maker engine stopped with an error")
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(terminate) => terminate,
        Err(error) => {
            error!(%error, signal = "SIGTERM", "failed to install shutdown signal handler");
            wait_for_ctrl_c().await;
            return;
        }
    };

    tokio::select! {
        result = tokio::signal::ctrl_c() => match result {
            Ok(()) => info!(signal = "SIGINT", "shutdown signal received"),
            Err(error) => error!(%error, signal = "SIGINT", "failed to listen for shutdown signal"),
        },
        received = terminate.recv() => match received {
            Some(()) => info!(signal = "SIGTERM", "shutdown signal received"),
            None => error!(signal = "SIGTERM", "shutdown signal stream ended"),
        },
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    wait_for_ctrl_c().await;
}

async fn wait_for_ctrl_c() {
    match tokio::signal::ctrl_c().await {
        Ok(()) => info!(signal = "SIGINT", "shutdown signal received"),
        Err(error) => error!(%error, signal = "SIGINT", "failed to listen for shutdown signal"),
    }
}

async fn report_telemetry(
    telemetry: RuntimeTelemetry,
    period: Duration,
    mut reports: SpscConsumer<EngineReport>,
) {
    let mut timer = tokio::time::interval(period);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    timer.tick().await;
    loop {
        tokio::select! {
            _ = timer.tick() => {
                log_telemetry_snapshot(&telemetry.take_snapshot(), period);
            }
            report = reports.recv() => {
                let Some(report) = report else {
                    return;
                };
                log_engine_report(report);
            }
        }
    }
}

fn log_engine_report(report: EngineReport) {
    match report {
        EngineReport::AccountSnapshot { stage, result } => match result {
            Ok(snapshot) => {
                info!(
                    stage = stage.as_str(),
                    balances = snapshot.balances.len(),
                    positions = snapshot.positions.len(),
                    "Binance account snapshot"
                );
                for balance in snapshot.balances {
                    info!(
                        stage = stage.as_str(),
                        asset = %balance.asset,
                        wallet_balance = %balance.wallet_balance,
                        cross_wallet_balance = %balance.cross_wallet_balance,
                        cross_unrealized_pnl = %balance.cross_unrealized_pnl,
                        available_balance = %balance.available_balance,
                        max_withdraw_amount = %balance.max_withdraw_amount,
                        ?balance.margin_available,
                        update_time_ms = balance.update_time_ms,
                        "Binance account balance"
                    );
                }
                for position in snapshot.positions {
                    info!(
                        stage = stage.as_str(),
                        symbol = %position.symbol,
                        position_side = position.position_side.as_str(),
                        position_amount = %position.position_amount,
                        unrealized_pnl = %position.unrealized_pnl,
                        isolated_margin = %position.isolated_margin,
                        notional = %position.notional,
                        isolated_wallet = %position.isolated_wallet,
                        initial_margin = %position.initial_margin,
                        maintenance_margin = %position.maintenance_margin,
                        update_time_ms = position.update_time_ms,
                        "Binance account position"
                    );
                }
            }
            Err(error) => {
                error!(
                    stage = stage.as_str(),
                    %error,
                    "failed to load Binance account snapshot"
                );
            }
        },
        EngineReport::CancelAllStarted { stage, symbol } => {
            info!(
                stage = stage.as_str(),
                %symbol,
                "Binance cancel-all started"
            );
        }
        EngineReport::CancelAllFinished {
            stage,
            symbol,
            duration_us,
            result,
        } => match result {
            Ok(()) => {
                info!(
                    stage = stage.as_str(),
                    %symbol,
                    duration_us,
                    "Binance cancel-all succeeded"
                );
            }
            Err(exchange_error) => {
                error!(
                    stage = stage.as_str(),
                    %symbol,
                    duration_us,
                    error_kind = %exchange_error.kind(),
                    exchange_code = ?exchange_error.exchange_code(),
                    retry_after_ms = ?exchange_error.retry_after().map(|delay| delay.as_millis()),
                    error_message = exchange_error.message(),
                    "Binance cancel-all failed"
                );
            }
        },
        EngineReport::AccountUpdate(update) => {
            info!(
                event_time_ms = update.event_time_ms,
                transaction_time_ms = update.transaction_time_ms,
                reason = update.reason.as_str(),
                balances = update.balances.len(),
                positions = update.positions.len(),
                "Binance account update"
            );
            for balance in update.balances {
                info!(
                    event_time_ms = update.event_time_ms,
                    transaction_time_ms = update.transaction_time_ms,
                    reason = update.reason.as_str(),
                    asset = %balance.asset,
                    wallet_balance = %balance.wallet_balance,
                    cross_wallet_balance = %balance.cross_wallet_balance,
                    balance_change = %balance.balance_change,
                    "Binance balance update"
                );
            }
            for position in update.positions {
                info!(
                    event_time_ms = update.event_time_ms,
                    transaction_time_ms = update.transaction_time_ms,
                    reason = update.reason.as_str(),
                    symbol = %position.symbol,
                    position_side = position.position_side.as_str(),
                    position_amount = %position.position_amount,
                    entry_price = %position.entry_price,
                    breakeven_price = %position.breakeven_price,
                    accumulated_realized_pnl = %position.accumulated_realized_pnl,
                    unrealized_pnl = %position.unrealized_pnl,
                    margin_type = position.margin_type.as_str(),
                    isolated_wallet = %position.isolated_wallet,
                    "Binance position update"
                );
            }
        }
        EngineReport::OrderUpdate(update) => {
            info!(
                event = "ORDER_TRADE_UPDATE",
                symbol = %update.symbol(),
                client_order_id = update.client_order_id().get(),
                exchange_order_id = update.exchange_order_id().get(),
                side = ?update.side(),
                status = ?update.status(),
                price_ticks = update.price().get(),
                original_quantity_lots = update.original_quantity().get(),
                cumulative_filled_lots = update.cumulative_filled().get(),
                "Binance order update"
            );
        }
        EngineReport::OrderTrade(trade) => {
            let update = trade.update;
            info!(
                event = "ORDER_TRADE_UPDATE",
                event_time_ms = trade.event_time_ms,
                transaction_time_ms = trade.transaction_time_ms,
                trade_time_ms = trade.trade_time_ms,
                symbol = %update.symbol(),
                client_order_id = update.client_order_id().get(),
                exchange_order_id = update.exchange_order_id().get(),
                side = ?update.side(),
                status = ?update.status(),
                average_price = %trade.average_price,
                last_filled_price = %trade.last_filled_price,
                last_filled_quantity = %trade.last_filled_quantity,
                cumulative_filled_quantity = %trade.cumulative_filled_quantity,
                commission_asset = ?trade.commission_asset,
                commission = ?trade.commission,
                trade_id = trade.trade_id,
                realized_pnl = %trade.realized_pnl,
                maker = trade.maker,
                "Binance trade update"
            );
        }
        EngineReport::TradeLite(trade) => {
            info!(
                event = "TRADE_LITE",
                event_time_ms = trade.event_time_ms,
                transaction_time_ms = trade.transaction_time_ms,
                symbol = %trade.symbol,
                client_order_id = trade.client_order_id.get(),
                exchange_order_id = trade.exchange_order_id.get(),
                side = ?trade.side,
                original_quantity = %trade.original_quantity,
                original_price = %trade.original_price,
                last_filled_price = %trade.last_filled_price,
                last_filled_quantity = %trade.last_filled_quantity,
                trade_id = trade.trade_id,
                maker = trade.maker,
                "Binance Trade Lite execution"
            );
        }
        EngineReport::ExchangeFailure {
            operation,
            symbol,
            client_order_id,
            side,
            price_ticks,
            quantity_lots,
            error: exchange_error,
        } => {
            error!(
                operation,
                %symbol,
                client_order_id = ?client_order_id.map(|id| id.get()),
                ?side,
                price_ticks = ?price_ticks.map(|price| price.get()),
                quantity_lots = ?quantity_lots.map(|quantity| quantity.get()),
                error_kind = %exchange_error.kind(),
                exchange_code = ?exchange_error.exchange_code(),
                retry_after_ms = ?exchange_error.retry_after().map(|delay| delay.as_millis()),
                error_message = exchange_error.message(),
                "Binance exchange operation failed"
            );
        }
        EngineReport::EngineFailure {
            operation,
            error: failure,
        } => {
            error!(operation, error = %failure, "maker engine operation failed");
        }
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
        reports_dropped = snapshot.reports_dropped,
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
        &snapshot.market_event_delay_us,
    );
    log_signed_latency(
        "market_exchange_transaction_delay",
        &snapshot.market_transaction_delay_us,
    );
    log_signed_latency(
        "private_exchange_event_delay",
        &snapshot.private_event_delay_us,
    );
    log_signed_latency(
        "private_exchange_transaction_delay",
        &snapshot.private_transaction_delay_us,
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
    log_latency("request_queue_wait", &snapshot.request_queue_wait_ns);
    log_latency(
        "request_enqueue_to_dequeue",
        &snapshot.request_enqueue_to_dequeue_ns,
    );
    log_latency(
        "request_preflight_wait",
        &snapshot.request_preflight_wait_ns,
    );
    log_latency("request_prepare", &snapshot.request_prepare_ns);
    log_latency("socket_send", &snapshot.socket_send_ns);
    log_latency("market_receive_to_send", &snapshot.market_end_to_end_ns);
    log_latency("private_receive_to_send", &snapshot.private_end_to_end_ns);
}

fn log_latency(metric: &str, latency: &LatencySnapshot) {
    info!(
        metric,
        unit = "ns",
        samples = latency.samples,
        mean = latency.mean,
        minimum = latency.minimum,
        p20_upper = latency.p20_upper,
        p30_upper = latency.p30_upper,
        p50_upper = latency.p50_upper,
        p99_upper = latency.p99_upper,
        maximum = latency.maximum,
        "maker latency telemetry"
    );
}

fn log_signed_latency(metric: &str, latency: &SignedLatencySnapshot) {
    info!(
        metric,
        unit = "us",
        samples = latency.samples,
        mean = latency.mean,
        minimum = latency.minimum,
        p20_upper = latency.p20_upper,
        p30_upper = latency.p30_upper,
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
