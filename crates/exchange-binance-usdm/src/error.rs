use std::time::Duration;

use maker_ports::{ExchangeError, ExchangeErrorKind};

use crate::models::ApiErrorDto;

pub(crate) fn transport(error: reqwest::Error) -> ExchangeError {
    let kind = if error.is_timeout() {
        ExchangeErrorKind::Timeout
    } else if error.is_connect() || error.is_request() {
        ExchangeErrorKind::Network
    } else {
        ExchangeErrorKind::InvalidResponse
    };
    // reqwest's display text can contain a signed URL. Keep signatures and
    // credentials out of propagated errors.
    ExchangeError::new(kind, "Binance HTTP transport failed")
}

pub(crate) fn websocket(_error: impl std::fmt::Display) -> ExchangeError {
    ExchangeError::new(
        ExchangeErrorKind::Network,
        "Binance WebSocket transport failed",
    )
}

pub(crate) fn invalid_response(context: &str, error: impl std::fmt::Display) -> ExchangeError {
    ExchangeError::new(
        ExchangeErrorKind::InvalidResponse,
        format!("invalid Binance {context}: {error}"),
    )
}

pub(crate) fn api(
    status: reqwest::StatusCode,
    error: ApiErrorDto,
    retry_after: Option<Duration>,
) -> ExchangeError {
    let kind = classify_api_error(status, error.code, &error.message);
    let mut normalized =
        ExchangeError::new(kind, error.message).with_exchange_code(error.code.to_string());
    if let Some(delay) = retry_after {
        normalized = normalized.with_retry_after(delay);
    }
    normalized
}

pub(crate) fn websocket_api(status: u16, error: ApiErrorDto) -> ExchangeError {
    let status =
        reqwest::StatusCode::from_u16(status).unwrap_or(reqwest::StatusCode::INTERNAL_SERVER_ERROR);
    api(status, error, None)
}

fn classify_api_error(status: reqwest::StatusCode, code: i64, message: &str) -> ExchangeErrorKind {
    match code {
        -1022 | -2014 | -2015 => ExchangeErrorKind::Authentication,
        -1003 => ExchangeErrorKind::RateLimited,
        -1000 | -1001 | -1006 | -1007 | -1008 => ExchangeErrorKind::ServiceUnavailable,
        -1021 => ExchangeErrorKind::StateConflict,
        -5022 => ExchangeErrorKind::PostOnlyWouldTake,
        -1100..=-1010 => ExchangeErrorKind::InvalidRequest,
        -2010 if message.to_ascii_lowercase().contains("immediately match") => {
            ExchangeErrorKind::PostOnlyWouldTake
        }
        -2010 | -2011 | -2013 => ExchangeErrorKind::ExchangeRejected,
        _ if status == reqwest::StatusCode::TOO_MANY_REQUESTS => ExchangeErrorKind::RateLimited,
        _ if status == reqwest::StatusCode::UNAUTHORIZED
            || status == reqwest::StatusCode::FORBIDDEN =>
        {
            ExchangeErrorKind::Authentication
        }
        _ if status.is_server_error() => ExchangeErrorKind::ServiceUnavailable,
        _ => ExchangeErrorKind::ExchangeRejected,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifies_gtx_cross_rejection() {
        let error = api(
            reqwest::StatusCode::BAD_REQUEST,
            ApiErrorDto {
                code: -2010,
                message: "Order would immediately match and take.".to_owned(),
            },
            None,
        );

        assert_eq!(error.kind(), ExchangeErrorKind::PostOnlyWouldTake);
        assert_eq!(error.exchange_code(), Some("-2010"));
    }

    #[test]
    fn identifies_futures_specific_gtx_rejection_code() {
        let error = api(
            reqwest::StatusCode::BAD_REQUEST,
            ApiErrorDto {
                code: -5022,
                message: "Due to the order could not be executed as maker.".to_owned(),
            },
            None,
        );

        assert_eq!(error.kind(), ExchangeErrorKind::PostOnlyWouldTake);
    }
}
