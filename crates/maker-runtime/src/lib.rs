mod spsc;

pub use spsc::{SpscConsumer, SpscProducer, TryPushError, spsc_channel};

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use serde::Deserialize;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    EventDriven,
    BusySpin,
}

#[derive(Debug, Error)]
pub enum CpuAffinityError {
    #[error("CPU affinity is unavailable on this platform")]
    Unavailable,
    #[error("CPU core {requested} is out of range; {available} logical CPUs are available")]
    OutOfRange { requested: usize, available: usize },
    #[error("failed to set CPU affinity to core {0}")]
    SetFailed(usize),
}

pub fn bind_cpu(core_index: Option<usize>) -> Result<(), CpuAffinityError> {
    let Some(core_index) = core_index else {
        return Ok(());
    };
    let cores = core_affinity::get_core_ids().ok_or(CpuAffinityError::Unavailable)?;
    if core_index >= cores.len() {
        return Err(CpuAffinityError::OutOfRange {
            requested: core_index,
            available: cores.len(),
        });
    }

    #[cfg(target_os = "macos")]
    {
        let _ = cores;
        Err(CpuAffinityError::Unavailable)
    }
    #[cfg(not(target_os = "macos"))]
    {
        if !core_affinity::set_for_current(cores[core_index]) {
            return Err(CpuAffinityError::SetFailed(core_index));
        }
        Ok(())
    }
}

/// Keeps a current-thread runtime active by immediately scheduling another poll
/// whenever the wrapped future is pending.
pub struct BusyPoll<F> {
    future: Pin<Box<F>>,
}

impl<F> BusyPoll<F> {
    pub fn new(future: F) -> Self {
        Self {
            future: Box::pin(future),
        }
    }
}

impl<F: Future> Future for BusyPoll<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this.future.as_mut().poll(context) {
            Poll::Ready(output) => Poll::Ready(output),
            Poll::Pending => {
                context.waker().wake_by_ref();
                std::hint::spin_loop();
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use tokio::sync::oneshot;

    use super::*;

    #[test]
    fn busy_poll_drives_a_woken_future_to_completion() {
        let (sender, receiver) = oneshot::channel();
        let worker = thread::spawn(move || {
            thread::sleep(Duration::from_millis(1));
            sender.send(7_u8).unwrap();
        });
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        assert_eq!(runtime.block_on(BusyPoll::new(receiver)).unwrap(), 7);
        worker.join().unwrap();
    }

    #[test]
    fn rejects_an_out_of_range_core() {
        let available = core_affinity::get_core_ids().unwrap().len();
        assert!(matches!(
            bind_cpu(Some(available)),
            Err(CpuAffinityError::OutOfRange { .. })
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_reports_affinity_as_unavailable() {
        assert!(matches!(
            bind_cpu(Some(0)),
            Err(CpuAffinityError::Unavailable)
        ));
    }
}
