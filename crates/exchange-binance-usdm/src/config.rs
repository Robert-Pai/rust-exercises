use std::{fmt, sync::Arc, time::Duration};

use ed25519_dalek::{SigningKey, pkcs8::DecodePrivateKey};
use maker_ports::{ExchangeError, ExchangeErrorKind, ExchangeResult};
use maker_runtime::ExecutionMode;
use secrecy::{ExposeSecret, SecretString};

const DEFAULT_REST_URL: &str = "https://fapi.binance.com";
const DEFAULT_WEBSOCKET_URL: &str = "wss://fstream.binance.com";
const DEFAULT_WEBSOCKET_API_URL: &str = "wss://ws-fapi.binance.com/ws-fapi/v1";

#[cfg(test)]
pub(crate) const TEST_PRIVATE_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIFMUOwaEd1lnpVLKe7FvQK5eMeQyY6xvW7GwHu+fSvd7\n-----END PRIVATE KEY-----\n";

/// Credentials used for signed USD-M Futures requests.
///
/// The private key must be an Ed25519 PKCS#8 PEM. It is parsed immediately,
/// and neither the PEM nor key material is exposed through `Debug`.
#[derive(Clone)]
pub struct BinanceCredentials {
    api_key: SecretString,
    signing_key: Arc<SigningKey>,
}

impl BinanceCredentials {
    pub fn new(api_key: SecretString, private_key_pem: SecretString) -> ExchangeResult<Self> {
        if api_key.expose_secret().is_empty() {
            return Err(ExchangeError::new(
                ExchangeErrorKind::InvalidRequest,
                "Binance API key cannot be empty",
            ));
        }
        if private_key_pem.expose_secret().is_empty() {
            return Err(ExchangeError::new(
                ExchangeErrorKind::InvalidRequest,
                "Binance Ed25519 private key PEM cannot be empty",
            ));
        }
        let signing_key =
            SigningKey::from_pkcs8_pem(private_key_pem.expose_secret()).map_err(|_| {
                ExchangeError::new(
                    ExchangeErrorKind::InvalidRequest,
                    "invalid Binance Ed25519 PKCS#8 private key PEM",
                )
            })?;
        Ok(Self {
            api_key,
            signing_key: Arc::new(signing_key),
        })
    }

    pub(crate) fn api_key(&self) -> &SecretString {
        &self.api_key
    }

    pub(crate) fn signing_key(&self) -> &SigningKey {
        &self.signing_key
    }
}

impl fmt::Debug for BinanceCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BinanceCredentials")
            .field("api_key", &"[REDACTED]")
            .field("private_key", &"[REDACTED]")
            .finish()
    }
}

/// Runtime endpoints and timeouts for the Binance USD-M adapter.
#[derive(Clone, Debug)]
pub struct BinanceUsdmConfig {
    rest_url: String,
    websocket_url: String,
    websocket_api_url: String,
    recv_window: Duration,
    request_timeout: Duration,
    listen_key_keepalive: Duration,
    websocket_idle_timeout: Duration,
    market_data_mode: ExecutionMode,
    market_data_cpu_core: Option<usize>,
    trading_mode: ExecutionMode,
    trading_cpu_core: Option<usize>,
}

impl Default for BinanceUsdmConfig {
    fn default() -> Self {
        Self {
            rest_url: DEFAULT_REST_URL.to_owned(),
            websocket_url: DEFAULT_WEBSOCKET_URL.to_owned(),
            websocket_api_url: DEFAULT_WEBSOCKET_API_URL.to_owned(),
            recv_window: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            listen_key_keepalive: Duration::from_secs(30 * 60),
            websocket_idle_timeout: Duration::from_secs(4 * 60),
            market_data_mode: ExecutionMode::BusySpin,
            market_data_cpu_core: None,
            trading_mode: ExecutionMode::BusySpin,
            trading_cpu_core: None,
        }
    }
}

impl BinanceUsdmConfig {
    pub fn new(
        rest_url: impl Into<String>,
        websocket_url: impl Into<String>,
        websocket_api_url: impl Into<String>,
        recv_window: Duration,
        request_timeout: Duration,
        listen_key_keepalive: Duration,
        websocket_idle_timeout: Duration,
    ) -> ExchangeResult<Self> {
        let config = Self {
            rest_url: rest_url.into(),
            websocket_url: websocket_url.into(),
            websocket_api_url: websocket_api_url.into(),
            recv_window,
            request_timeout,
            listen_key_keepalive,
            websocket_idle_timeout,
            market_data_mode: ExecutionMode::BusySpin,
            market_data_cpu_core: None,
            trading_mode: ExecutionMode::BusySpin,
            trading_cpu_core: None,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn rest_url(&self) -> &str {
        &self.rest_url
    }

    pub fn websocket_url(&self) -> &str {
        &self.websocket_url
    }

    pub fn websocket_api_url(&self) -> &str {
        &self.websocket_api_url
    }

    pub const fn recv_window(&self) -> Duration {
        self.recv_window
    }

    pub const fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    pub const fn listen_key_keepalive(&self) -> Duration {
        self.listen_key_keepalive
    }

    pub const fn websocket_idle_timeout(&self) -> Duration {
        self.websocket_idle_timeout
    }

    pub const fn market_data_mode(&self) -> ExecutionMode {
        self.market_data_mode
    }

    pub const fn market_data_cpu_core(&self) -> Option<usize> {
        self.market_data_cpu_core
    }

    pub const fn trading_mode(&self) -> ExecutionMode {
        self.trading_mode
    }

    pub const fn trading_cpu_core(&self) -> Option<usize> {
        self.trading_cpu_core
    }

    pub fn with_network_cpu_cores(
        mut self,
        market_data_cpu_core: Option<usize>,
        trading_cpu_core: Option<usize>,
    ) -> Self {
        self.market_data_cpu_core = market_data_cpu_core;
        self.trading_cpu_core = trading_cpu_core;
        self
    }

    pub fn with_network_runtimes(
        mut self,
        market_data_mode: ExecutionMode,
        market_data_cpu_core: Option<usize>,
        trading_mode: ExecutionMode,
        trading_cpu_core: Option<usize>,
    ) -> Self {
        self.market_data_mode = market_data_mode;
        self.market_data_cpu_core = market_data_cpu_core;
        self.trading_mode = trading_mode;
        self.trading_cpu_core = trading_cpu_core;
        self
    }

    pub(crate) fn validate(&self) -> ExchangeResult<()> {
        validate_url("REST", &self.rest_url, &["http", "https"])?;
        validate_url("stream WebSocket", &self.websocket_url, &["ws", "wss"])?;
        validate_url(
            "trading WebSocket API",
            &self.websocket_api_url,
            &["ws", "wss"],
        )?;

        if self.recv_window.is_zero() || self.recv_window.as_millis() > 60_000 {
            return Err(ExchangeError::new(
                ExchangeErrorKind::InvalidRequest,
                "Binance recv window must be between 1 and 60000 milliseconds",
            ));
        }
        if self.request_timeout.is_zero() {
            return Err(ExchangeError::new(
                ExchangeErrorKind::InvalidRequest,
                "request timeout must be positive",
            ));
        }
        if self.listen_key_keepalive.is_zero() {
            return Err(ExchangeError::new(
                ExchangeErrorKind::InvalidRequest,
                "listen-key keepalive interval must be positive",
            ));
        }
        if self.listen_key_keepalive >= Duration::from_secs(60 * 60) {
            return Err(ExchangeError::new(
                ExchangeErrorKind::InvalidRequest,
                "listen-key keepalive interval must be shorter than Binance's 60-minute expiry",
            ));
        }
        if self.websocket_idle_timeout.is_zero() {
            return Err(ExchangeError::new(
                ExchangeErrorKind::InvalidRequest,
                "WebSocket idle timeout must be positive",
            ));
        }
        Ok(())
    }
}

fn validate_url(label: &str, raw: &str, allowed_schemes: &[&str]) -> ExchangeResult<()> {
    let url = url::Url::parse(raw).map_err(|error| {
        ExchangeError::new(
            ExchangeErrorKind::InvalidRequest,
            format!("invalid Binance {label} URL: {error}"),
        )
    })?;
    if !allowed_schemes.contains(&url.scheme()) || url.host_str().is_none() {
        return Err(ExchangeError::new(
            ExchangeErrorKind::InvalidRequest,
            format!("invalid Binance {label} URL scheme or host"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_debug_output_is_redacted() {
        let credentials = BinanceCredentials::new(
            SecretString::new("public-key".to_owned()),
            SecretString::new(TEST_PRIVATE_KEY_PEM.to_owned()),
        )
        .unwrap();

        let output = format!("{credentials:?}");
        assert!(!output.contains("public-key"));
        assert!(!output.contains("MC4CAQ"));
        assert!(output.contains("REDACTED"));
    }

    #[test]
    fn rejects_non_ed25519_private_key_material() {
        let error = BinanceCredentials::new(
            SecretString::new("public-key".to_owned()),
            SecretString::new("not-a-private-key".to_owned()),
        )
        .unwrap_err();

        assert_eq!(error.kind(), ExchangeErrorKind::InvalidRequest);
    }

    #[test]
    fn rejects_invalid_endpoint_scheme() {
        let result = BinanceUsdmConfig::new(
            "ftp://example.com",
            "wss://example.com",
            "wss://api.example.com/ws-fapi/v1",
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_secs(60),
            Duration::from_secs(240),
        );

        assert_eq!(
            result.unwrap_err().kind(),
            ExchangeErrorKind::InvalidRequest
        );
    }

    #[test]
    fn rejects_keepalive_at_or_beyond_listen_key_expiry() {
        let result = BinanceUsdmConfig::new(
            "https://example.test",
            "wss://example.test",
            "wss://api.example.test/ws-fapi/v1",
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_secs(60 * 60),
            Duration::from_secs(240),
        );

        assert_eq!(
            result.unwrap_err().kind(),
            ExchangeErrorKind::InvalidRequest
        );
    }

    #[test]
    fn configures_network_runtimes_independently() {
        let config = BinanceUsdmConfig::default().with_network_runtimes(
            ExecutionMode::EventDriven,
            Some(3),
            ExecutionMode::BusySpin,
            Some(2),
        );

        assert_eq!(config.market_data_mode(), ExecutionMode::EventDriven);
        assert_eq!(config.market_data_cpu_core(), Some(3));
        assert_eq!(config.trading_mode(), ExecutionMode::BusySpin);
        assert_eq!(config.trading_cpu_core(), Some(2));
    }

    #[test]
    fn rejects_zero_websocket_idle_timeout() {
        let result = BinanceUsdmConfig::new(
            "https://example.test",
            "wss://example.test",
            "wss://api.example.test/ws-fapi/v1",
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_secs(60),
            Duration::ZERO,
        );

        assert_eq!(
            result.unwrap_err().kind(),
            ExchangeErrorKind::InvalidRequest
        );
    }
}
