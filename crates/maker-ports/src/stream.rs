use std::pin::Pin;

use futures_core::Stream;

use crate::ExchangeResult;

/// A sendable event subscription owned by its consumer.
///
/// A subscription must emit an error and then terminate when it can no longer
/// guarantee a continuous event sequence. The engine can then recover and
/// request a fresh subscription.
pub type EventStream<T> = Pin<Box<dyn Stream<Item = ExchangeResult<T>> + Send + 'static>>;
