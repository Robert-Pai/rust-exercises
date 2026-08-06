//! Dedicated single-thread asynchronous network runtime.
//!
//! The adapter is called by the strategy runtime, but all WebSocket I/O is
//! scheduled onto this runtime. A current-thread Tokio runtime gives the
//! network side one stable OS thread with configurable parking behavior.

use std::{future::Future, sync::mpsc as std_mpsc, thread};

use futures_util::future::BoxFuture;
use maker_ports::{ExchangeError, ExchangeErrorKind, ExchangeResult};
use maker_runtime::{BusyPoll, ExecutionMode, bind_cpu};
use tokio::sync::{mpsc, oneshot};
use tracing::info;

type NetworkTask = BoxFuture<'static, ()>;
const TASK_BUFFER: usize = 64;

/// Handle used by non-network tasks to schedule work on the dedicated network
/// thread.
#[derive(Clone)]
pub(crate) struct NetworkRuntime {
    tasks: mpsc::Sender<NetworkTask>,
}

impl NetworkRuntime {
    pub(crate) fn new(mode: ExecutionMode, cpu_core: Option<usize>) -> ExchangeResult<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                ExchangeError::new(
                    ExchangeErrorKind::ServiceUnavailable,
                    format!("failed to build Binance network runtime: {error}"),
                )
            })?;
        let (tasks, receiver) = mpsc::channel(TASK_BUFFER);
        let (startup, started) = std_mpsc::sync_channel(1);

        thread::Builder::new()
            .name("maker-network".to_owned())
            .spawn(move || {
                if let Err(error) = bind_cpu(cpu_core) {
                    let _ = startup.send(Err(error));
                    return;
                }
                let _ = startup.send(Ok(()));
                info!(?mode, ?cpu_core, "Binance network runtime started");
                match mode {
                    ExecutionMode::EventDriven => runtime.block_on(run_network_loop(receiver)),
                    ExecutionMode::BusySpin => {
                        runtime.block_on(BusyPoll::new(run_network_loop(receiver)))
                    }
                }
            })
            .map_err(|error| {
                ExchangeError::new(
                    ExchangeErrorKind::ServiceUnavailable,
                    format!("failed to start Binance network thread: {error}"),
                )
            })?;
        started
            .recv()
            .map_err(|_| network_stopped("Binance network thread stopped during startup"))?
            .map_err(|error| {
                ExchangeError::new(
                    ExchangeErrorKind::InvalidRequest,
                    format!("failed to bind Binance network thread: {error}"),
                )
            })?;

        Ok(Self { tasks })
    }

    /// Schedules a long-lived network task. The task is spawned by the
    /// current-thread runtime, so its socket polling remains on the network
    /// thread even when the caller is the strategy runtime.
    pub(crate) fn spawn<F>(&self, task: F) -> ExchangeResult<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.tasks
            .try_send(Box::pin(task))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    network_stopped("Binance network runtime task queue is full")
                }
                mpsc::error::TrySendError::Closed(_) => {
                    network_stopped("Binance network runtime stopped")
                }
            })
    }

    /// Runs a short network operation on the network thread and returns its
    /// result to the caller's runtime.
    pub(crate) async fn call<F, T>(&self, operation: F) -> ExchangeResult<T>
    where
        F: Future<Output = ExchangeResult<T>> + Send + 'static,
        T: Send + 'static,
    {
        let (reply, response) = oneshot::channel();
        self.spawn(async move {
            let _ = reply.send(operation.await);
        })?;
        response
            .await
            .map_err(|_| network_stopped("Binance network operation response channel closed"))?
    }
}

async fn run_network_loop(mut tasks: mpsc::Receiver<NetworkTask>) {
    while let Some(task) = tasks.recv().await {
        tokio::spawn(task);
    }
}

fn network_stopped(message: &'static str) -> ExchangeError {
    ExchangeError::new(ExchangeErrorKind::ServiceUnavailable, message)
}

#[cfg(test)]
mod tests {
    use std::thread::ThreadId;

    use super::*;

    #[tokio::test]
    async fn runs_operations_on_the_dedicated_network_thread() {
        let runtime = NetworkRuntime::new(ExecutionMode::EventDriven, None).unwrap();
        let caller = thread::current().id();
        let (network, name): (ThreadId, String) = runtime
            .call(async move {
                Ok((
                    thread::current().id(),
                    thread::current().name().unwrap().to_owned(),
                ))
            })
            .await
            .unwrap();

        assert_ne!(caller, network);
        assert_eq!(name, "maker-network");
    }

    #[tokio::test]
    async fn busy_spin_runtime_drives_network_operations() {
        let runtime = NetworkRuntime::new(ExecutionMode::BusySpin, None).unwrap();

        assert_eq!(runtime.call(async { Ok(7_u8) }).await.unwrap(), 7);
    }
}
