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

    let exchange = BinanceUsdm::new(exchange_config, credentials)
        .context("failed to create Binance USD-M adapter")?;
    let engine = MakerEngine::new(engine_config, Box::new(exchange));
    let strategy = strategy_runtime::spawn(engine, strategy_settings)
        .context("failed to start maker strategy thread")?;

    strategy
        .run_until(async {
            match tokio::signal::ctrl_c().await {
                Ok(()) => info!("shutdown signal received"),
                Err(error) => error!(%error, "failed to listen for shutdown signal"),
            }
        })
        .await
        .context("maker engine stopped with an error")
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
    let stdout_layer = fmt::layer()
        .compact()
        .with_target(false)
        .with_ansi(false)
        .with_filter(level);
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
