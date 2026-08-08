use std::{
    future::poll_fn,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use futures_core::Stream;
use futures_util::task::AtomicWaker;
use rtrb::{Consumer, PopError, Producer, PushError, RingBuffer};

#[repr(align(64))]
struct EndpointState {
    alive: AtomicBool,
    waker: AtomicWaker,
}

impl EndpointState {
    fn new() -> Self {
        Self {
            alive: AtomicBool::new(true),
            waker: AtomicWaker::new(),
        }
    }
}

struct ChannelState {
    producer: EndpointState,
    consumer: EndpointState,
}

/// Creates a bounded single-producer, single-consumer queue.
///
/// Both endpoints are move-only. Values are never overwritten and all storage
/// is allocated when the channel is created.
pub fn spsc_channel<T>(capacity: usize) -> (SpscProducer<T>, SpscConsumer<T>) {
    assert!(capacity > 0, "SPSC capacity must be positive");
    let (producer, consumer) = RingBuffer::new(capacity);
    let state = Arc::new(ChannelState {
        producer: EndpointState::new(),
        consumer: EndpointState::new(),
    });
    (
        SpscProducer {
            ring: producer,
            state: state.clone(),
        },
        SpscConsumer {
            ring: consumer,
            state,
        },
    )
}

#[derive(Debug, Eq, PartialEq)]
pub enum TryPushError<T> {
    Full(T),
    ConsumerDropped(T),
}

/// Exclusive producer endpoint for a bounded SPSC queue.
pub struct SpscProducer<T> {
    ring: Producer<T>,
    state: Arc<ChannelState>,
}

impl<T> SpscProducer<T> {
    pub fn try_push(&mut self, value: T) -> Result<(), TryPushError<T>> {
        if !self.state.consumer.alive.load(Ordering::Acquire) || self.ring.is_abandoned() {
            return Err(TryPushError::ConsumerDropped(value));
        }
        match self.ring.push(value) {
            Ok(()) => {
                self.state.producer.waker.wake();
                Ok(())
            }
            Err(PushError::Full(value)) => Err(TryPushError::Full(value)),
        }
    }

    pub fn is_consumer_dropped(&self) -> bool {
        !self.state.consumer.alive.load(Ordering::Acquire) || self.ring.is_abandoned()
    }

    pub async fn consumer_dropped(&mut self) {
        poll_fn(|context| self.poll_consumer_dropped(context)).await
    }

    fn poll_consumer_dropped(&mut self, context: &mut Context<'_>) -> Poll<()> {
        if self.is_consumer_dropped() {
            return Poll::Ready(());
        }
        self.state.consumer.waker.register(context.waker());
        if self.is_consumer_dropped() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl<T> Drop for SpscProducer<T> {
    fn drop(&mut self) {
        self.state.producer.alive.store(false, Ordering::Release);
        self.state.producer.waker.wake();
    }
}

/// Exclusive consumer endpoint for a bounded SPSC queue.
pub struct SpscConsumer<T> {
    ring: Consumer<T>,
    state: Arc<ChannelState>,
}

impl<T> SpscConsumer<T> {
    pub fn try_pop(&mut self) -> Option<T> {
        self.ring.pop().ok()
    }

    pub fn is_producer_dropped(&self) -> bool {
        !self.state.producer.alive.load(Ordering::Acquire) || self.ring.is_abandoned()
    }

    pub fn poll_recv(&mut self, context: &mut Context<'_>) -> Poll<Option<T>> {
        match self.ring.pop() {
            Ok(value) => return Poll::Ready(Some(value)),
            Err(PopError::Empty) if self.is_producer_dropped() => return Poll::Ready(None),
            Err(PopError::Empty) => {}
        }

        self.state.producer.waker.register(context.waker());
        match self.ring.pop() {
            Ok(value) => Poll::Ready(Some(value)),
            Err(PopError::Empty) if self.is_producer_dropped() => Poll::Ready(None),
            Err(PopError::Empty) => Poll::Pending,
        }
    }

    pub async fn recv(&mut self) -> Option<T> {
        poll_fn(|context| self.poll_recv(context)).await
    }
}

impl<T> Stream for SpscConsumer<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_recv(context)
    }
}

impl<T> Drop for SpscConsumer<T> {
    fn drop(&mut self) {
        self.state.consumer.alive.store(false, Ordering::Release);
        self.state.consumer.waker.wake();
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use super::*;

    #[test]
    fn preserves_fifo_order_through_wraparound() {
        let (mut producer, mut consumer) = spsc_channel(2);
        producer.try_push(1).unwrap();
        producer.try_push(2).unwrap();
        assert_eq!(consumer.try_pop(), Some(1));
        producer.try_push(3).unwrap();

        assert_eq!(consumer.try_pop(), Some(2));
        assert_eq!(consumer.try_pop(), Some(3));
        assert_eq!(consumer.try_pop(), None);
    }

    #[test]
    fn full_queue_returns_rejected_value_without_overwrite() {
        let (mut producer, mut consumer) = spsc_channel(1);
        producer.try_push(1).unwrap();

        assert_eq!(producer.try_push(2), Err(TryPushError::Full(2)));
        assert_eq!(consumer.try_pop(), Some(1));
    }

    #[test]
    fn consumer_observes_producer_drop_after_draining() {
        let (mut producer, mut consumer) = spsc_channel(1);
        producer.try_push(7).unwrap();
        drop(producer);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        assert_eq!(runtime.block_on(consumer.recv()), Some(7));
        assert_eq!(runtime.block_on(consumer.recv()), None);
    }

    #[test]
    fn producer_is_woken_when_consumer_drops() {
        let (mut producer, consumer) = spsc_channel::<u8>(1);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let worker = thread::spawn(move || {
            thread::sleep(Duration::from_millis(1));
            drop(consumer);
        });

        runtime.block_on(producer.consumer_dropped());
        worker.join().unwrap();
        assert!(producer.is_consumer_dropped());
    }

    #[test]
    fn cross_thread_stress_preserves_every_value() {
        const COUNT: u64 = 100_000;
        let (mut producer, mut consumer) = spsc_channel(64);
        let writer = thread::spawn(move || {
            for value in 0..COUNT {
                let mut pending = value;
                loop {
                    match producer.try_push(pending) {
                        Ok(()) => break,
                        Err(TryPushError::Full(value)) => {
                            pending = value;
                            std::hint::spin_loop();
                        }
                        Err(TryPushError::ConsumerDropped(_)) => panic!("consumer dropped"),
                    }
                }
            }
        });

        for expected in 0..COUNT {
            loop {
                if let Some(value) = consumer.try_pop() {
                    assert_eq!(value, expected);
                    break;
                }
                std::hint::spin_loop();
            }
        }
        writer.join().unwrap();
    }
}
