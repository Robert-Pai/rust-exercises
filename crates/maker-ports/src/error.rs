use std::{fmt, time::Duration};

use thiserror::Error;

/// Exchange-facing error categories on which orchestration code may branch.
///
/// Adapter-specific error codes and messages remain available on
/// [`ExchangeError`], but must not leak into engine control flow.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ExchangeErrorKind {
    Authentication,
    RateLimited,
    Network,
    Timeout,
    PostOnlyWouldTake,
    ExchangeRejected,
    StateConflict,
    InvalidRequest,
    InvalidResponse,
    ServiceUnavailable,
    Unsupported,
}

impl ExchangeErrorKind {
    /// Whether retrying the same operation can normally succeed without a
    /// state or configuration change.
    pub const fn is_transient(self) -> bool {
        matches!(
            self,
            Self::RateLimited | Self::Network | Self::Timeout | Self::ServiceUnavailable
        )
    }
}

impl fmt::Display for ExchangeErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Authentication => "authentication",
            Self::RateLimited => "rate limited",
            Self::Network => "network",
            Self::Timeout => "timeout",
            Self::PostOnlyWouldTake => "post-only order would take liquidity",
            Self::ExchangeRejected => "exchange rejected request",
            Self::StateConflict => "state conflict",
            Self::InvalidRequest => "invalid request",
            Self::InvalidResponse => "invalid response",
            Self::ServiceUnavailable => "service unavailable",
            Self::Unsupported => "unsupported operation",
        };
        formatter.write_str(value)
    }
}

/// A normalized failure returned by an exchange adapter.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("{kind}: {message}")]
pub struct ExchangeError {
    kind: ExchangeErrorKind,
    message: String,
    exchange_code: Option<String>,
    retry_after: Option<Duration>,
}

impl ExchangeError {
    pub fn new(kind: ExchangeErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            exchange_code: None,
            retry_after: None,
        }
    }

    pub fn with_exchange_code(mut self, code: impl Into<String>) -> Self {
        self.exchange_code = Some(code.into());
        self
    }

    pub const fn with_retry_after(mut self, retry_after: Duration) -> Self {
        self.retry_after = Some(retry_after);
        self
    }

    pub const fn kind(&self) -> ExchangeErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn exchange_code(&self) -> Option<&str> {
        self.exchange_code.as_deref()
    }

    pub const fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }

    pub const fn is_transient(&self) -> bool {
        self.kind.is_transient()
    }
}

pub type ExchangeResult<T> = Result<T, ExchangeError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_machine_readable_error_metadata() {
        let retry_after = Duration::from_millis(250);
        let error = ExchangeError::new(ExchangeErrorKind::RateLimited, "slow down")
            .with_exchange_code("-1003")
            .with_retry_after(retry_after);

        assert_eq!(error.kind(), ExchangeErrorKind::RateLimited);
        assert_eq!(error.message(), "slow down");
        assert_eq!(error.exchange_code(), Some("-1003"));
        assert_eq!(error.retry_after(), Some(retry_after));
        assert!(error.is_transient());
    }

    #[test]
    fn classifies_post_only_rejection_as_non_transient() {
        let error = ExchangeError::new(
            ExchangeErrorKind::PostOnlyWouldTake,
            "order would immediately match",
        );

        assert!(!error.is_transient());
    }
}
