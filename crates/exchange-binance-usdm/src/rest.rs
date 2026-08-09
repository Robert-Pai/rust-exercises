use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use maker_domain::Symbol;
use maker_ports::{ExchangeError, ExchangeErrorKind, ExchangeResult};
use reqwest::{Method, RequestBuilder};
use secrecy::ExposeSecret;
use serde::de::DeserializeOwned;

use crate::{
    config::{BinanceCredentials, BinanceUsdmConfig},
    error,
    models::{
        ApiErrorDto, BookTickerDto, CancelAllOrdersDto, DualSidePositionDto, ExchangeInfoDto,
        ExchangeSymbolDto, ListenKeyDto, ServerTimeDto,
    },
    rate_limit::{
        AcquireDecision, RateLimitSnapshot, RequestCost, RequestRateLimiter,
        SharedRequestRateLimiter,
    },
    signing::{build_signed_query, encode_query},
};

const API_KEY_HEADER: &str = "X-MBX-APIKEY";

#[derive(Clone)]
pub(crate) struct RestClient {
    inner: Arc<RestInner>,
}

struct RestInner {
    http: reqwest::Client,
    base_url: String,
    credentials: BinanceCredentials,
    recv_window_ms: u64,
    request_timeout: Duration,
    request_rate_limiter: SharedRequestRateLimiter,
}

impl RestClient {
    pub(crate) fn new(
        config: &BinanceUsdmConfig,
        credentials: BinanceCredentials,
    ) -> ExchangeResult<Self> {
        config.validate()?;
        let http = reqwest::Client::builder()
            .timeout(config.request_timeout())
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(error::transport)?;
        let recv_window_ms = u64::try_from(config.recv_window().as_millis()).map_err(|_| {
            ExchangeError::new(
                ExchangeErrorKind::InvalidRequest,
                "Binance recv window does not fit in milliseconds",
            )
        })?;
        Ok(Self {
            inner: Arc::new(RestInner {
                http,
                base_url: config.rest_url().trim_end_matches('/').to_owned(),
                credentials,
                recv_window_ms,
                request_timeout: config.request_timeout(),
                request_rate_limiter: RequestRateLimiter::shared(),
            }),
        })
    }

    pub(crate) async fn exchange_symbol(
        &self,
        symbol: &Symbol,
    ) -> ExchangeResult<ExchangeSymbolDto> {
        let limit_version =
            RequestRateLimiter::begin_limit_refresh(&self.inner.request_rate_limiter)?;
        let response: ExchangeInfoDto = self
            .public_json(Method::GET, "/fapi/v1/exchangeInfo", Vec::new())
            .await?;
        let snapshots = response
            .rate_limits
            .iter()
            .filter_map(|limit| {
                RateLimitSnapshot::from_wire(
                    &limit.rate_limit_type,
                    &limit.interval,
                    limit.interval_num,
                    limit.non_negative_limit(),
                    limit.non_negative_count(),
                )
            })
            .collect::<Vec<_>>();
        if snapshots.len() != response.rate_limits.len() {
            return Err(RequestRateLimiter::reject_unrecognized_telemetry(
                &self.inner.request_rate_limiter,
            ));
        }
        RequestRateLimiter::update_limits(
            &self.inner.request_rate_limiter,
            limit_version,
            &snapshots,
        )?;
        response
            .symbols
            .into_iter()
            .find(|candidate| candidate.symbol == symbol.as_str())
            .ok_or_else(|| {
                ExchangeError::new(
                    ExchangeErrorKind::InvalidRequest,
                    format!("Binance USD-M symbol {symbol} was not found"),
                )
            })
    }

    pub(crate) fn request_rate_limiter(&self) -> SharedRequestRateLimiter {
        self.inner.request_rate_limiter.clone()
    }

    pub(crate) async fn book_ticker(&self, symbol: &Symbol) -> ExchangeResult<BookTickerDto> {
        self.public_json(
            Method::GET,
            "/fapi/v1/ticker/bookTicker",
            vec![("symbol".to_owned(), symbol.as_str().to_owned())],
        )
        .await
    }

    pub(crate) async fn position_mode(&self) -> ExchangeResult<DualSidePositionDto> {
        self.signed_json(Method::GET, "/fapi/v1/positionSide/dual", Vec::new())
            .await
    }

    pub(crate) async fn cancel_all(&self, symbol: &Symbol) -> ExchangeResult<()> {
        let response: CancelAllOrdersDto = self
            .signed_json(
                Method::DELETE,
                "/fapi/v1/allOpenOrders",
                vec![("symbol".to_owned(), symbol.as_str().to_owned())],
            )
            .await?;
        if response.code != 200 {
            return Err(ExchangeError::new(
                ExchangeErrorKind::InvalidResponse,
                format!(
                    "Binance REST cancel-all result had code {}: {}",
                    response.code, response.message
                ),
            ));
        }
        Ok(())
    }

    pub(crate) async fn create_listen_key(&self) -> ExchangeResult<String> {
        let response: ListenKeyDto = self
            .api_key_json(Method::POST, "/fapi/v1/listenKey", Vec::new())
            .await?;
        if response.listen_key.is_empty() {
            return Err(ExchangeError::new(
                ExchangeErrorKind::InvalidResponse,
                "Binance returned an empty listen key",
            ));
        }
        Ok(response.listen_key)
    }

    /// Extends the lifetime of the exact user-data stream key that was created
    /// for the corresponding WebSocket connection.
    pub(crate) async fn keepalive_listen_key(&self, listen_key: &str) -> ExchangeResult<()> {
        if listen_key.is_empty() {
            return Err(ExchangeError::new(
                ExchangeErrorKind::InvalidRequest,
                "Binance listen key cannot be empty",
            ));
        }
        let _: serde_json::Value = self
            .api_key_json(
                Method::PUT,
                "/fapi/v1/listenKey",
                vec![("listenKey".to_owned(), listen_key.to_owned())],
            )
            .await?;
        Ok(())
    }

    async fn public_json<T>(
        &self,
        method: Method,
        path: &str,
        parameters: Vec<(String, String)>,
    ) -> ExchangeResult<T>
    where
        T: DeserializeOwned,
    {
        let query = encode_query(&parameters);
        let url = self.endpoint_with_query(path, &query);
        self.execute_json(self.inner.http.request(method, url), RequestCost::GENERIC)
            .await
    }

    async fn api_key_json<T>(
        &self,
        method: Method,
        path: &str,
        parameters: Vec<(String, String)>,
    ) -> ExchangeResult<T>
    where
        T: DeserializeOwned,
    {
        let query = encode_query(&parameters);
        let url = self.endpoint_with_query(path, &query);
        self.execute_json(
            self.inner.http.request(method, url).header(
                API_KEY_HEADER,
                self.inner.credentials.api_key().expose_secret(),
            ),
            RequestCost::GENERIC,
        )
        .await
    }

    async fn signed_json<T>(
        &self,
        method: Method,
        path: &str,
        parameters: Vec<(String, String)>,
    ) -> ExchangeResult<T>
    where
        T: DeserializeOwned,
    {
        let mut clock_offset_ms = self.clock_offset_ms().await?;
        for attempt in 0..2 {
            let timestamp = signed_timestamp_ms(clock_offset_ms)?;
            let query = build_signed_query(
                parameters.clone(),
                self.inner.recv_window_ms,
                timestamp,
                self.inner.credentials.signing_key(),
            );
            let request = self
                .inner
                .http
                .request(method.clone(), self.endpoint_with_query(path, &query))
                .header(
                    API_KEY_HEADER,
                    self.inner.credentials.api_key().expose_secret(),
                );
            match self.execute_json(request, RequestCost::GENERIC).await {
                Err(error) if attempt == 0 && error.exchange_code() == Some("-1021") => {
                    clock_offset_ms = self.clock_offset_ms().await?;
                }
                result => return result,
            }
        }
        unreachable!("signed request loop always returns on its second attempt")
    }

    pub(crate) async fn clock_offset_ms(&self) -> ExchangeResult<i64> {
        let before = unix_time_ms()?;
        let server: ServerTimeDto = self
            .public_json(Method::GET, "/fapi/v1/time", Vec::new())
            .await?;
        let after = unix_time_ms()?;
        let local_midpoint = before + after.saturating_sub(before) / 2;
        let server = i64::try_from(server.server_time).map_err(|_| {
            ExchangeError::new(
                ExchangeErrorKind::InvalidResponse,
                "Binance server time exceeds supported range",
            )
        })?;
        let local = i64::try_from(local_midpoint).map_err(|_| {
            ExchangeError::new(
                ExchangeErrorKind::StateConflict,
                "local system time exceeds supported range",
            )
        })?;
        server.checked_sub(local).ok_or_else(|| {
            ExchangeError::new(
                ExchangeErrorKind::StateConflict,
                "clock offset exceeds supported range",
            )
        })
    }

    fn endpoint_with_query(&self, path: &str, query: &str) -> String {
        if query.is_empty() {
            format!("{}{}", self.inner.base_url, path)
        } else {
            format!("{}{}?{}", self.inner.base_url, path, query)
        }
    }

    async fn execute_json<T>(&self, request: RequestBuilder, cost: RequestCost) -> ExchangeResult<T>
    where
        T: DeserializeOwned,
    {
        self.acquire_request_capacity(cost).await?;
        let response = request.send().await.map_err(error::transport)?;
        let status = response.status();
        let header_snapshots = response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                crate::rate_limit::rest_header_snapshot(
                    &name.as_str().to_ascii_uppercase(),
                    value.to_str().ok()?,
                )
            })
            .collect::<Vec<_>>();
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_secs);
        let body = response.bytes().await.map_err(error::transport)?;
        RequestRateLimiter::update_counts(&self.inner.request_rate_limiter, &header_snapshots)?;
        if status.is_success() {
            let result = serde_json::from_slice(&body)
                .map_err(|error| error::invalid_response("JSON response", error));
            RequestRateLimiter::observe_error(
                &self.inner.request_rate_limiter,
                result.as_ref().err(),
            )?;
            return result;
        }

        let api_error: ApiErrorDto = serde_json::from_slice(&body).map_err(|decode_error| {
            ExchangeError::new(
                ExchangeErrorKind::InvalidResponse,
                format!(
                    "Binance returned HTTP {status} with an invalid error body: {decode_error}"
                ),
            )
        })?;
        let error = error::api(status, api_error, retry_after);
        RequestRateLimiter::observe_error(&self.inner.request_rate_limiter, Some(&error))?;
        Err(error)
    }

    async fn acquire_request_capacity(&self, cost: RequestCost) -> ExchangeResult<()> {
        let deadline = tokio::time::Instant::now() + self.inner.request_timeout;
        loop {
            match RequestRateLimiter::acquire(&self.inner.request_rate_limiter, cost, deadline)? {
                AcquireDecision::Ready => return Ok(()),
                AcquireDecision::RetryAt(retry_at) => tokio::time::sleep_until(retry_at).await,
                AcquireDecision::DeadlineExceeded => return Err(rate_limit_timeout_error()),
            }
        }
    }
}

fn rate_limit_timeout_error() -> ExchangeError {
    ExchangeError::new(
        ExchangeErrorKind::Timeout,
        "Binance request rate-limit wait exceeded its deadline",
    )
}

fn signed_timestamp_ms(clock_offset_ms: i64) -> ExchangeResult<u64> {
    let now = i64::try_from(unix_time_ms()?).map_err(|_| {
        ExchangeError::new(
            ExchangeErrorKind::StateConflict,
            "local system time exceeds supported range",
        )
    })?;
    let adjusted = now.checked_add(clock_offset_ms).ok_or_else(|| {
        ExchangeError::new(
            ExchangeErrorKind::StateConflict,
            "adjusted Binance timestamp overflowed",
        )
    })?;
    u64::try_from(adjusted).map_err(|_| {
        ExchangeError::new(
            ExchangeErrorKind::StateConflict,
            "adjusted Binance timestamp is before the Unix epoch",
        )
    })
}

fn unix_time_ms() -> ExchangeResult<u64> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| {
        ExchangeError::new(
            ExchangeErrorKind::StateConflict,
            "local system time is before the Unix epoch",
        )
    })?;
    u64::try_from(elapsed.as_millis()).map_err(|_| {
        ExchangeError::new(
            ExchangeErrorKind::StateConflict,
            "local system time exceeds supported range",
        )
    })
}

#[cfg(test)]
mod tests {
    use secrecy::SecretString;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::mpsc,
    };

    use super::*;
    use crate::{
        config::{BinanceCredentials, BinanceUsdmConfig},
        signing::sign_payload,
    };

    #[tokio::test]
    async fn signs_authenticated_requests_sent_to_mock_server() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (sender, mut receiver) = mpsc::channel(2);
        let server = tokio::spawn(async move {
            for response_body in [
                r#"{"serverTime":1700000000000}"#,
                r#"{"dualSidePosition":false}"#,
            ] {
                let (mut connection, _) = listener.accept().await.unwrap();
                let request = read_http_request(&mut connection).await;
                sender.send(request).await.unwrap();
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    response_body.len(),
                    response_body
                );
                connection.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let config = BinanceUsdmConfig::new(
            format!("http://{address}"),
            "wss://example.test",
            "wss://api.example.test/ws-fapi/v1",
            Duration::from_secs(5),
            Duration::from_secs(2),
            Duration::from_secs(60),
            Duration::from_secs(240),
        )
        .unwrap();
        let credentials = BinanceCredentials::new(
            SecretString::new("test-key".to_owned()),
            SecretString::new(crate::config::TEST_PRIVATE_KEY_PEM.to_owned()),
        )
        .unwrap();
        let client = RestClient::new(&config, credentials.clone()).unwrap();

        let mode = client.position_mode().await.unwrap();

        assert!(!mode.dual_side_position);
        let time_request = receiver.recv().await.unwrap();
        assert!(time_request.starts_with("GET /fapi/v1/time HTTP/1.1\r\n"));
        let signed_request = receiver.recv().await.unwrap();
        server.await.unwrap();
        assert!(
            signed_request
                .to_ascii_lowercase()
                .contains("\r\nx-mbx-apikey: test-key\r\n")
        );
        let request_target = signed_request
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap();
        let query = request_target.split_once('?').unwrap().1;
        let (unsigned_query, encoded_signature) = query.rsplit_once("&signature=").unwrap();
        let signature =
            url::form_urlencoded::parse(format!("signature={encoded_signature}").as_bytes())
                .next()
                .unwrap()
                .1
                .into_owned();
        assert!(unsigned_query.contains("recvWindow=5000"));
        assert!(unsigned_query.contains("timestamp="));
        assert_eq!(
            signature,
            sign_payload(unsigned_query, credentials.signing_key())
        );
    }

    #[tokio::test]
    async fn cancels_all_symbol_orders_through_signed_futures_rest_endpoint() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (sender, mut receiver) = mpsc::channel(2);
        let server = tokio::spawn(async move {
            for response_body in [
                r#"{"serverTime":1700000000000}"#,
                r#"{"code":200,"msg":"The operation of cancel all open order is done."}"#,
            ] {
                let (mut connection, _) = listener.accept().await.unwrap();
                let request = read_http_request(&mut connection).await;
                sender.send(request).await.unwrap();
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    response_body.len(),
                    response_body
                );
                connection.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let config = BinanceUsdmConfig::new(
            format!("http://{address}"),
            "wss://example.test",
            "wss://api.example.test/ws-fapi/v1",
            Duration::from_secs(5),
            Duration::from_secs(2),
            Duration::from_secs(60),
            Duration::from_secs(240),
        )
        .unwrap();
        let credentials = BinanceCredentials::new(
            SecretString::new("test-key".to_owned()),
            SecretString::new(crate::config::TEST_PRIVATE_KEY_PEM.to_owned()),
        )
        .unwrap();
        let client = RestClient::new(&config, credentials).unwrap();

        client
            .cancel_all(&Symbol::new("BTCUSDT").unwrap())
            .await
            .unwrap();

        let time_request = receiver.recv().await.unwrap();
        let cancel_request = receiver.recv().await.unwrap();
        server.await.unwrap();
        assert!(time_request.starts_with("GET /fapi/v1/time HTTP/1.1\r\n"));
        assert!(cancel_request.starts_with("DELETE /fapi/v1/allOpenOrders?symbol=BTCUSDT&"));
        assert!(cancel_request.contains("recvWindow=5000"));
        assert!(cancel_request.contains("timestamp="));
        assert!(cancel_request.contains("&signature="));
        assert!(
            cancel_request
                .to_ascii_lowercase()
                .contains("\r\nx-mbx-apikey: test-key\r\n")
        );
    }

    #[tokio::test]
    async fn creates_and_keeps_alive_futures_listen_key_with_query_parameter() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (sender, mut receiver) = mpsc::channel(2);
        let server = tokio::spawn(async move {
            for response_body in [r#"{"listenKey":"opaque-listen-key"}"#, r#"{}"#] {
                let (mut connection, _) = listener.accept().await.unwrap();
                let request = read_http_request(&mut connection).await;
                sender.send(request).await.unwrap();
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    response_body.len(),
                    response_body
                );
                connection.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let config = BinanceUsdmConfig::new(
            format!("http://{address}"),
            "wss://example.test",
            "wss://api.example.test/ws-fapi/v1",
            Duration::from_secs(5),
            Duration::from_secs(2),
            Duration::from_secs(60),
            Duration::from_secs(240),
        )
        .unwrap();
        let credentials = BinanceCredentials::new(
            SecretString::new("test-key".to_owned()),
            SecretString::new(crate::config::TEST_PRIVATE_KEY_PEM.to_owned()),
        )
        .unwrap();
        let client = RestClient::new(&config, credentials).unwrap();

        assert_eq!(
            client.create_listen_key().await.unwrap(),
            "opaque-listen-key"
        );
        client
            .keepalive_listen_key("opaque-listen-key")
            .await
            .unwrap();

        let create_request = receiver.recv().await.unwrap();
        let keepalive_request = receiver.recv().await.unwrap();
        server.await.unwrap();
        assert!(create_request.starts_with("POST /fapi/v1/listenKey HTTP/1.1\r\n"));
        assert!(
            keepalive_request
                .starts_with("PUT /fapi/v1/listenKey?listenKey=opaque-listen-key HTTP/1.1\r\n")
        );
        assert!(
            keepalive_request
                .to_ascii_lowercase()
                .contains("\r\nx-mbx-apikey: test-key\r\n")
        );
    }

    async fn read_http_request(stream: &mut tokio::net::TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 1024];
        loop {
            let count = stream.read(&mut chunk).await.unwrap();
            assert!(count > 0, "connection closed before request headers");
            bytes.extend_from_slice(&chunk[..count]);
            if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        String::from_utf8(bytes).unwrap()
    }
}
