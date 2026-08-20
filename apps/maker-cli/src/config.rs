use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use exchange_binance_usdm::{BinanceCredentials, BinanceUsdmConfig};
use maker_domain::Symbol;
use maker_engine::EngineConfig;
use maker_runtime::ExecutionMode;
use rust_decimal::Decimal;
use secrecy::SecretString;
use serde::Deserialize;
use tracing::level_filters::LevelFilter;

use crate::strategy_runtime::StrategyRuntimeSettings;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    exchange: ExchangeConfig,
    strategy: StrategyConfig,
    runtime: RuntimeConfig,
    logging: LoggingConfig,
}

#[derive(Debug)]
pub struct LoggingSettings {
    directory: PathBuf,
    retention_days: u64,
    level: LevelFilter,
    stdout: bool,
    telemetry_interval: Duration,
}

impl LoggingSettings {
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub const fn retention_days(&self) -> u64 {
        self.retention_days
    }

    pub const fn level(&self) -> LevelFilter {
        self.level
    }

    pub const fn stdout_enabled(&self) -> bool {
        self.stdout
    }

    pub const fn telemetry_interval(&self) -> Duration {
        self.telemetry_interval
    }
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        validate_permissions(path)?;
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        toml::from_str(&raw).with_context(|| format!("failed to parse config {}", path.display()))
    }

    pub fn into_components(
        self,
    ) -> Result<(
        BinanceUsdmConfig,
        BinanceCredentials,
        EngineConfig,
        LoggingSettings,
        StrategyRuntimeSettings,
    )> {
        let exchange = BinanceUsdmConfig::new(
            self.exchange.rest_url,
            self.exchange.websocket_url,
            self.exchange.websocket_api_url,
            millis("exchange.recv_window_ms", self.exchange.recv_window_ms)?,
            millis(
                "exchange.request_timeout_ms",
                self.exchange.request_timeout_ms,
            )?,
            seconds(
                "exchange.listen_key_keepalive_secs",
                self.exchange.listen_key_keepalive_secs,
            )?,
            seconds(
                "exchange.websocket_idle_timeout_secs",
                self.exchange.websocket_idle_timeout_secs,
            )?,
        )
        .context("invalid Binance adapter configuration")?
        .with_network_runtimes(
            self.runtime.market_data_mode,
            self.runtime.market_data_cpu_core,
            self.runtime.trading_mode,
            self.runtime.trading_cpu_core,
        );

        let credentials = BinanceCredentials::new(
            SecretString::new(self.exchange.api_key),
            SecretString::new(self.exchange.private_key_pem),
        )
        .context("invalid Binance credentials")?;

        let symbol = Symbol::new(self.strategy.symbol).context("invalid strategy symbol")?;
        let engine = EngineConfig::new(
            symbol,
            self.strategy.levels_per_side,
            self.strategy.inner_ticks,
            self.strategy.spacing_ticks,
            self.strategy.take_profit_ticks,
            self.strategy.quantity,
            millis(
                "runtime.reconcile_interval_ms",
                self.runtime.reconcile_interval_ms,
            )?,
            seconds(
                "runtime.instrument_refresh_interval_secs",
                self.runtime.instrument_refresh_interval_secs,
            )?,
            millis(
                "runtime.reconnect_delay_ms",
                self.runtime.reconnect_delay_ms,
            )?,
        )
        .context("invalid maker engine configuration")?;

        let level = self
            .logging
            .level
            .parse::<LevelFilter>()
            .with_context(|| format!("invalid logging.level {:?}", self.logging.level))?;

        if self.logging.retention_days == 0 {
            bail!("logging.retention_days must be greater than zero");
        }
        let logging = LoggingSettings {
            directory: PathBuf::from(self.logging.directory),
            retention_days: self.logging.retention_days,
            level,
            stdout: self.logging.stdout,
            telemetry_interval: seconds(
                "logging.telemetry_interval_secs",
                self.logging.telemetry_interval_secs,
            )?,
        };

        let strategy_runtime = StrategyRuntimeSettings {
            mode: self.runtime.strategy_mode,
            cpu_core: self.runtime.strategy_cpu_core,
        };

        Ok((exchange, credentials, engine, logging, strategy_runtime))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExchangeConfig {
    rest_url: String,
    websocket_url: String,
    websocket_api_url: String,
    api_key: String,
    private_key_pem: String,
    recv_window_ms: u64,
    request_timeout_ms: u64,
    listen_key_keepalive_secs: u64,
    websocket_idle_timeout_secs: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StrategyConfig {
    symbol: String,
    levels_per_side: usize,
    inner_ticks: u64,
    spacing_ticks: u64,
    take_profit_ticks: u64,
    #[serde(with = "rust_decimal::serde::str")]
    quantity: Decimal,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeConfig {
    reconcile_interval_ms: u64,
    instrument_refresh_interval_secs: u64,
    reconnect_delay_ms: u64,
    #[serde(default = "default_execution_mode")]
    strategy_mode: ExecutionMode,
    #[serde(default)]
    strategy_cpu_core: Option<usize>,
    #[serde(default = "default_execution_mode")]
    market_data_mode: ExecutionMode,
    #[serde(default)]
    market_data_cpu_core: Option<usize>,
    #[serde(default = "default_execution_mode")]
    trading_mode: ExecutionMode,
    #[serde(default)]
    trading_cpu_core: Option<usize>,
}

const fn default_execution_mode() -> ExecutionMode {
    ExecutionMode::BusySpin
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LoggingConfig {
    level: String,
    directory: String,
    retention_days: u64,
    #[serde(default)]
    stdout: bool,
    #[serde(default = "default_telemetry_interval_secs")]
    telemetry_interval_secs: u64,
}

const fn default_telemetry_interval_secs() -> u64 {
    10
}

pub fn config_path_from_args() -> Result<PathBuf> {
    let mut args = std::env::args_os().skip(1);
    match (args.next(), args.next(), args.next()) {
        (None, None, None) => Ok(PathBuf::from("config.toml")),
        (Some(flag), Some(path), None) if flag == "--config" => Ok(PathBuf::from(path)),
        _ => bail!("usage: maker [--config PATH]"),
    }
}

fn millis(field: &'static str, value: u64) -> Result<Duration> {
    if value == 0 {
        bail!("{field} must be greater than zero");
    }
    Ok(Duration::from_millis(value))
}

fn seconds(field: &'static str, value: u64) -> Result<Duration> {
    if value == 0 {
        bail!("{field} must be greater than zero");
    }
    Ok(Duration::from_secs(value))
}

#[cfg(unix)]
fn validate_permissions(path: &Path) -> Result<()> {
    let metadata = fs::metadata(path)
        .with_context(|| format!("failed to inspect config {}", path.display()))?;
    let mode = metadata.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        bail!(
            "config {} has permissions {:03o}; API-key config must be 600 or stricter",
            path.display(),
            mode
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    const VALID_CONFIG: &str = r#"
[exchange]
rest_url = "https://fapi.binance.com"
websocket_url = "wss://fstream.binance.com"
websocket_api_url = "wss://ws-fapi.binance.com/ws-fapi/v1"
api_key = "key"
private_key_pem = '''-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIFMUOwaEd1lnpVLKe7FvQK5eMeQyY6xvW7GwHu+fSvd7
-----END PRIVATE KEY-----
'''
recv_window_ms = 5000
request_timeout_ms = 5000
listen_key_keepalive_secs = 1800
websocket_idle_timeout_secs = 240

[strategy]
symbol = "BTCUSDT"
levels_per_side = 5
inner_ticks = 10
spacing_ticks = 5
take_profit_ticks = 3
quantity = "0.001"

[runtime]
reconcile_interval_ms = 1000
instrument_refresh_interval_secs = 3600
reconnect_delay_ms = 1000
strategy_mode = "busy_spin"
strategy_cpu_core = 1
market_data_mode = "event_driven"
market_data_cpu_core = 3
trading_mode = "event_driven"
trading_cpu_core = 2

[logging]
level = "info"
directory = "logs"
retention_days = 14
stdout = false
telemetry_interval_secs = 10
"#;

    fn config_file(contents: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        #[cfg(unix)]
        file.as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))
            .unwrap();
        file
    }

    #[test]
    fn loads_and_builds_all_components() {
        let file = config_file(VALID_CONFIG);
        let config = AppConfig::load(file.path()).unwrap();
        let (exchange, _credentials, engine, logging, runtime) = config.into_components().unwrap();

        assert_eq!(exchange.rest_url(), "https://fapi.binance.com");
        assert_eq!(
            exchange.websocket_api_url(),
            "wss://ws-fapi.binance.com/ws-fapi/v1"
        );
        assert_eq!(engine.symbol().as_str(), "BTCUSDT");
        assert_eq!(engine.levels_per_side().get(), 5);
        assert_eq!(engine.take_profit_ticks().get(), 3);
        assert_eq!(runtime.mode, ExecutionMode::BusySpin);
        assert_eq!(runtime.cpu_core, Some(1));
        assert_eq!(exchange.market_data_mode(), ExecutionMode::EventDriven);
        assert_eq!(exchange.market_data_cpu_core(), Some(3));
        assert_eq!(exchange.trading_mode(), ExecutionMode::EventDriven);
        assert_eq!(exchange.trading_cpu_core(), Some(2));
        assert_eq!(logging.level(), LevelFilter::INFO);
        assert_eq!(logging.directory(), Path::new("logs"));
        assert_eq!(logging.retention_days(), 14);
        assert!(!logging.stdout_enabled());
        assert_eq!(logging.telemetry_interval(), Duration::from_secs(10));
    }

    #[test]
    fn configures_stdout_logging() {
        let file = config_file(&VALID_CONFIG.replace("stdout = false", "stdout = true"));
        let config = AppConfig::load(file.path()).unwrap();
        let (_, _, _, logging, _) = config.into_components().unwrap();

        assert!(logging.stdout_enabled());
    }

    #[test]
    fn defaults_telemetry_interval_to_ten_seconds() {
        let contents = VALID_CONFIG.replace("telemetry_interval_secs = 10\n", "");
        let file = config_file(&contents);
        let config = AppConfig::load(file.path()).unwrap();
        let (_, _, _, logging, _) = config.into_components().unwrap();

        assert_eq!(logging.telemetry_interval(), Duration::from_secs(10));
    }

    #[test]
    fn defaults_network_affinity_to_none() {
        let contents = VALID_CONFIG
            .replace("market_data_cpu_core = 3\n", "")
            .replace("trading_cpu_core = 2\n", "");
        let file = config_file(&contents);
        let config = AppConfig::load(file.path()).unwrap();
        let (exchange, ..) = config.into_components().unwrap();

        assert_eq!(exchange.market_data_cpu_core(), None);
        assert_eq!(exchange.trading_cpu_core(), None);
    }

    #[test]
    fn defaults_omitted_execution_modes_to_busy_spin() {
        let contents = VALID_CONFIG
            .replace("strategy_mode = \"busy_spin\"\n", "")
            .replace("market_data_mode = \"event_driven\"\n", "")
            .replace("trading_mode = \"event_driven\"\n", "");
        let file = config_file(&contents);
        let config = AppConfig::load(file.path()).unwrap();
        let (exchange, _, _, _, runtime) = config.into_components().unwrap();

        assert_eq!(runtime.mode, ExecutionMode::BusySpin);
        assert_eq!(exchange.market_data_mode(), ExecutionMode::BusySpin);
        assert_eq!(exchange.trading_mode(), ExecutionMode::BusySpin);
    }

    #[test]
    fn rejects_unknown_configuration_fields() {
        let file = config_file(
            &VALID_CONFIG.replace("level = \"info\"", "level = \"info\"\nunknown = true"),
        );

        assert!(AppConfig::load(file.path()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn rejects_group_readable_secret_file() {
        let file = config_file(VALID_CONFIG);
        file.as_file()
            .set_permissions(fs::Permissions::from_mode(0o640))
            .unwrap();

        assert!(AppConfig::load(file.path()).is_err());
    }
}
