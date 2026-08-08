use std::{
    future::poll_fn,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    task::{Context, Poll},
};

use futures_util::task::AtomicWaker;
use maker_domain::{BestBidAsk, PriceTicks, Symbol};
use maker_runtime::{SpscConsumer, SpscProducer, spsc_channel};

use crate::{ExchangeError, ExchangeErrorKind, ExchangeResult};

const SLOT_COUNT: u8 = 2;
const READ_ATTEMPTS: usize = 8;

#[repr(align(64))]
struct PublishedIndex(AtomicU8);

#[repr(align(64))]
struct BboSlot {
    version: AtomicU64,
    bid_ticks: AtomicU64,
    ask_ticks: AtomicU64,
}

impl BboSlot {
    const fn empty() -> Self {
        Self {
            version: AtomicU64::new(0),
            bid_ticks: AtomicU64::new(0),
            ask_ticks: AtomicU64::new(0),
        }
    }

    fn write(&self, book: BestBidAsk) {
        let current = self.version.load(Ordering::SeqCst);
        let writing = if current & 1 == 0 {
            current.wrapping_add(1)
        } else {
            current.wrapping_add(2)
        };
        self.version.store(writing, Ordering::SeqCst);
        self.bid_ticks.store(book.bid().get(), Ordering::SeqCst);
        self.ask_ticks.store(book.ask().get(), Ordering::SeqCst);
        self.version
            .store(writing.wrapping_add(1), Ordering::SeqCst);
    }

    fn read(&self) -> Option<(u64, u64)> {
        for _ in 0..READ_ATTEMPTS {
            let before = self.version.load(Ordering::SeqCst);
            if before & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }
            let bid = self.bid_ticks.load(Ordering::SeqCst);
            let ask = self.ask_ticks.load(Ordering::SeqCst);
            let after = self.version.load(Ordering::SeqCst);
            if before == after && after & 1 == 0 && bid != 0 && ask != 0 {
                return Some((bid, ask));
            }
            std::hint::spin_loop();
        }
        None
    }
}

struct LatestBboInner {
    published: PublishedIndex,
    slots: [BboSlot; SLOT_COUNT as usize],
    symbol: Symbol,
}

/// Cloneable strategy-side handle for the newest coherent BBO snapshot.
#[derive(Clone)]
pub struct LatestBbo {
    inner: Arc<LatestBboInner>,
}

impl LatestBbo {
    pub fn new(initial: BestBidAsk) -> Self {
        let inner = Arc::new(LatestBboInner {
            published: PublishedIndex(AtomicU8::new(0)),
            slots: [BboSlot::empty(), BboSlot::empty()],
            symbol: *initial.symbol(),
        });
        inner.slots[0].write(initial);
        Self { inner }
    }

    pub fn latest(&self) -> Option<BestBidAsk> {
        for _ in 0..READ_ATTEMPTS {
            let index = self.inner.published.0.load(Ordering::SeqCst);
            if index >= SLOT_COUNT {
                return None;
            }
            if let Some((bid, ask)) = self.inner.slots[usize::from(index)].read() {
                return BestBidAsk::new(
                    self.inner.symbol,
                    PriceTicks::new(bid).expect("published BBO bid is positive"),
                    PriceTicks::new(ask).expect("published BBO ask is positive"),
                )
                .ok();
            }
            std::hint::spin_loop();
        }
        None
    }
}

struct NotificationState {
    generation: AtomicU64,
    publisher_alive: AtomicBool,
    subscriber_alive: AtomicBool,
    changed_waker: AtomicWaker,
    closed_waker: AtomicWaker,
}

impl NotificationState {
    fn new() -> Self {
        Self {
            generation: AtomicU64::new(0),
            publisher_alive: AtomicBool::new(true),
            subscriber_alive: AtomicBool::new(true),
            changed_waker: AtomicWaker::new(),
            closed_waker: AtomicWaker::new(),
        }
    }
}

/// Single market-thread writer for a [`LatestBbo`] double buffer.
pub struct LatestBboPublisher {
    latest: LatestBbo,
    notification: Arc<NotificationState>,
    terminal: SpscProducer<ExchangeError>,
}

impl LatestBboPublisher {
    pub fn publish(&mut self, book: BestBidAsk) -> ExchangeResult<()> {
        if book.symbol() != &self.latest.inner.symbol {
            return Err(ExchangeError::new(
                ExchangeErrorKind::InvalidResponse,
                "BBO publication symbol does not match its mailbox",
            ));
        }
        let current = self.latest.inner.published.0.load(Ordering::SeqCst);
        let next = (current + 1) % SLOT_COUNT;
        self.latest.inner.slots[usize::from(next)].write(book);
        self.latest.inner.published.0.store(next, Ordering::SeqCst);
        self.notification.generation.fetch_add(1, Ordering::Release);
        self.notification.changed_waker.wake();
        Ok(())
    }

    pub fn fail(mut self, error: ExchangeError) {
        let _ = self.terminal.try_push(error);
        self.notification.changed_waker.wake();
    }

    pub fn is_closed(&self) -> bool {
        !self.notification.subscriber_alive.load(Ordering::Acquire)
    }

    pub fn closed(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let notification = self.notification.clone();
        poll_fn(move |context| {
            if !notification.subscriber_alive.load(Ordering::Acquire) {
                return Poll::Ready(());
            }
            notification.closed_waker.register(context.waker());
            if !notification.subscriber_alive.load(Ordering::Acquire) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
    }
}

impl Drop for LatestBboPublisher {
    fn drop(&mut self) {
        self.notification
            .publisher_alive
            .store(false, Ordering::Release);
        self.notification.changed_waker.wake();
    }
}

/// Latest-value BBO storage plus coalesced update and failure notification.
pub struct LatestBboSubscription {
    latest: LatestBbo,
    notification: Arc<NotificationState>,
    terminal: SpscConsumer<ExchangeError>,
    observed_generation: u64,
}

impl LatestBboSubscription {
    pub fn channel(initial: BestBidAsk) -> (LatestBboPublisher, Self) {
        let latest = LatestBbo::new(initial);
        let notification = Arc::new(NotificationState::new());
        let (terminal, terminal_receiver) = spsc_channel(1);
        (
            LatestBboPublisher {
                latest: latest.clone(),
                notification: notification.clone(),
                terminal,
            },
            Self {
                latest,
                notification,
                terminal: terminal_receiver,
                observed_generation: 0,
            },
        )
    }

    pub fn reader(&self) -> LatestBbo {
        self.latest.clone()
    }

    pub fn latest(&self) -> Option<BestBidAsk> {
        self.latest.latest()
    }

    pub async fn changed(&mut self) -> ExchangeResult<()> {
        poll_fn(|context| self.poll_changed(context)).await
    }

    fn poll_changed(&mut self, context: &mut Context<'_>) -> Poll<ExchangeResult<()>> {
        if let Some(error) = self.terminal.try_pop() {
            return Poll::Ready(Err(error));
        }
        let generation = self.notification.generation.load(Ordering::Acquire);
        if generation != self.observed_generation {
            self.observed_generation = generation;
            return Poll::Ready(Ok(()));
        }
        if !self.notification.publisher_alive.load(Ordering::Acquire) {
            return Poll::Ready(Err(ExchangeError::new(
                ExchangeErrorKind::Network,
                "best-bid/ask subscription ended",
            )));
        }

        self.notification.changed_waker.register(context.waker());
        if let Some(error) = self.terminal.try_pop() {
            return Poll::Ready(Err(error));
        }
        let generation = self.notification.generation.load(Ordering::Acquire);
        if generation != self.observed_generation {
            self.observed_generation = generation;
            Poll::Ready(Ok(()))
        } else if !self.notification.publisher_alive.load(Ordering::Acquire) {
            Poll::Ready(Err(ExchangeError::new(
                ExchangeErrorKind::Network,
                "best-bid/ask subscription ended",
            )))
        } else {
            Poll::Pending
        }
    }
}

impl Drop for LatestBboSubscription {
    fn drop(&mut self) {
        self.notification
            .subscriber_alive
            .store(false, Ordering::Release);
        self.notification.closed_waker.wake();
    }
}

#[cfg(test)]
mod tests {
    use std::{mem::align_of, thread};

    use super::*;

    fn book(bid: u64) -> BestBidAsk {
        BestBidAsk::new(
            Symbol::new("BTCUSDT").unwrap(),
            PriceTicks::new(bid).unwrap(),
            PriceTicks::new(bid + 1).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn returns_the_latest_publication() {
        let (mut publisher, subscription) = LatestBboSubscription::channel(book(100));
        publisher.publish(book(101)).unwrap();
        publisher.publish(book(102)).unwrap();

        assert_eq!(subscription.latest().unwrap(), book(102));
    }

    #[test]
    fn publication_state_is_cache_line_aligned() {
        assert!(align_of::<PublishedIndex>() >= 64);
        assert!(align_of::<BboSlot>() >= 64);
    }

    #[test]
    fn concurrent_reads_never_observe_a_torn_book() {
        let (mut publisher, subscription) = LatestBboSubscription::channel(book(1));
        let reader = subscription.reader();
        let writer = thread::spawn(move || {
            for bid in 2..100_000 {
                publisher.publish(book(bid)).unwrap();
            }
        });

        for _ in 0..100_000 {
            if let Some(snapshot) = reader.latest() {
                assert_eq!(snapshot.ask().get(), snapshot.bid().get() + 1);
            }
        }
        writer.join().unwrap();
    }

    #[tokio::test]
    async fn update_notifications_coalesce_to_the_latest_value() {
        let (mut publisher, mut subscription) = LatestBboSubscription::channel(book(100));
        publisher.publish(book(101)).unwrap();
        publisher.publish(book(102)).unwrap();

        subscription.changed().await.unwrap();
        assert_eq!(subscription.observed_generation, 2);
        assert_eq!(subscription.latest().unwrap(), book(102));
    }

    #[tokio::test]
    async fn failure_wakes_the_subscription() {
        let (publisher, mut subscription) = LatestBboSubscription::channel(book(100));
        publisher.fail(ExchangeError::new(
            ExchangeErrorKind::Network,
            "injected failure",
        ));

        assert_eq!(
            subscription.changed().await.unwrap_err().kind(),
            ExchangeErrorKind::Network
        );
    }

    #[tokio::test]
    async fn subscriber_drop_wakes_the_publisher() {
        let (publisher, subscription) = LatestBboSubscription::channel(book(100));
        drop(subscription);

        publisher.closed().await;
        assert!(publisher.is_closed());
    }
}
