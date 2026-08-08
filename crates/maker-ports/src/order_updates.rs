use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::Stream;
use maker_domain::OrderUpdate;
use maker_runtime::{SpscConsumer, SpscProducer, TryPushError, spsc_channel};

use crate::{ExchangeError, ExchangeErrorKind, ExchangeResult};

/// Exclusive trading-network publisher for ordered private order updates.
pub struct OrderUpdatePublisher {
    updates: SpscProducer<OrderUpdate>,
    terminal: SpscProducer<ExchangeError>,
}

impl OrderUpdatePublisher {
    pub fn publish(&mut self, update: OrderUpdate) -> ExchangeResult<()> {
        self.updates.try_push(update).map_err(|error| match error {
            TryPushError::Full(_) => ExchangeError::new(
                ExchangeErrorKind::ServiceUnavailable,
                "private order-update queue is full",
            ),
            TryPushError::ConsumerDropped(_) => ExchangeError::new(
                ExchangeErrorKind::Network,
                "private order-update subscription ended",
            ),
        })
    }

    pub fn fail(mut self, error: ExchangeError) {
        let _ = self.terminal.try_push(error);
    }

    pub fn is_closed(&self) -> bool {
        self.updates.is_consumer_dropped()
    }

    pub async fn closed(&mut self) {
        self.updates.consumer_dropped().await;
    }
}

/// Ordered strategy-side stream backed by a bounded SPSC ring.
pub struct OrderUpdateSubscription {
    updates: SpscConsumer<OrderUpdate>,
    terminal: SpscConsumer<ExchangeError>,
    updates_closed: bool,
    terminated: bool,
}

impl OrderUpdateSubscription {
    pub fn channel(capacity: usize) -> (OrderUpdatePublisher, Self) {
        let (updates, update_receiver) = spsc_channel(capacity);
        let (terminal, terminal_receiver) = spsc_channel(1);
        (
            OrderUpdatePublisher { updates, terminal },
            Self {
                updates: update_receiver,
                terminal: terminal_receiver,
                updates_closed: false,
                terminated: false,
            },
        )
    }
}

impl Stream for OrderUpdateSubscription {
    type Item = ExchangeResult<OrderUpdate>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.terminated {
            return Poll::Ready(None);
        }

        if !self.updates_closed {
            match self.updates.poll_recv(context) {
                Poll::Ready(Some(update)) => return Poll::Ready(Some(Ok(update))),
                Poll::Ready(None) => self.updates_closed = true,
                Poll::Pending => return Poll::Pending,
            }
        }

        match self.terminal.poll_recv(context) {
            Poll::Ready(Some(error)) => {
                self.terminated = true;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                self.terminated = true;
                Poll::Ready(Some(Err(ExchangeError::new(
                    ExchangeErrorKind::Network,
                    "private order-update publisher ended",
                ))))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;
    use maker_domain::{
        ClientOrderId, ExchangeOrderId, FilledLots, OrderStatus, PriceTicks, QuantityLots, Side,
        Symbol,
    };

    use super::*;

    fn update(id: u64) -> OrderUpdate {
        OrderUpdate::new(
            Symbol::new("BTCUSDT").unwrap(),
            ClientOrderId::new(id).unwrap(),
            ExchangeOrderId::new(id).unwrap(),
            Side::Buy,
            PriceTicks::new(100).unwrap(),
            QuantityLots::new(1).unwrap(),
            FilledLots::ZERO,
            OrderStatus::Accepted,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn drains_fifo_before_terminal_failure() {
        let (mut publisher, mut subscription) = OrderUpdateSubscription::channel(2);
        publisher.publish(update(1)).unwrap();
        publisher.publish(update(2)).unwrap();
        let overflow = publisher.publish(update(3)).unwrap_err();
        publisher.fail(overflow);

        assert_eq!(subscription.next().await.unwrap().unwrap(), update(1));
        assert_eq!(subscription.next().await.unwrap().unwrap(), update(2));
        assert_eq!(
            subscription.next().await.unwrap().unwrap_err().kind(),
            ExchangeErrorKind::ServiceUnavailable
        );
        assert!(subscription.next().await.is_none());
    }

    #[tokio::test]
    async fn publisher_drop_without_failure_is_reported() {
        let (publisher, mut subscription) = OrderUpdateSubscription::channel(1);
        drop(publisher);

        assert_eq!(
            subscription.next().await.unwrap().unwrap_err().kind(),
            ExchangeErrorKind::Network
        );
        assert!(subscription.next().await.is_none());
    }

    #[tokio::test]
    async fn consumer_drop_wakes_publisher() {
        let (mut publisher, subscription) = OrderUpdateSubscription::channel(1);
        drop(subscription);

        publisher.closed().await;
        assert!(publisher.is_closed());
    }
}
