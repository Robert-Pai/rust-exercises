//! Dedicated single-thread asynchronous network runtimes.
//!
//! Public market data and latency-critical trading traffic are scheduled on
//! separate current-thread Tokio runtimes so bursts on one cannot delay the
//! other. Each runtime has independently configurable parking behavior and CPU
//! affinity.

use std::{future::Future, sync::mpsc as std_mpsc, thread};

use futures_util::future::BoxFuture;
use maker_ports::{ExchangeError, ExchangeErrorKind, ExchangeResult};
use maker_runtime::{BusyPoll, ExecutionMode, bind_cpu};
use tokio::sync::{mpsc, oneshot};
use tracing::info;

type NetworkTask = BoxFuture<'static, ()>;
const TASK_BUFFER: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NetworkRole {
    MarketData,
    Trading,
}

impl NetworkRole {
    const fn label(self) -> &'static str {
        match self {
            Self::MarketData => "market-data",
            Self::Trading => "trading",
        }
    }

    const fn thread_name(self) -> &'static str {
        match self {
            Self::MarketData => "maker-network-market-data",
            Self::Trading => "maker-network-trading",
        }
    }
}

/// Handle used by non-network tasks to schedule work on a dedicated network
/// thread.
#[derive(Clone)]
pub(crate) struct NetworkRuntime {
    role: NetworkRole,
    tasks: mpsc::Sender<NetworkTask>,
}

impl NetworkRuntime {
    pub(crate) fn new(
        role: NetworkRole,
        mode: ExecutionMode,
        cpu_core: Option<usize>,
    ) -> ExchangeResult<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                ExchangeError::new(
                    ExchangeErrorKind::ServiceUnavailable,
                    format!(
                        "failed to build Binance {} network runtime: {error}",
                        role.label()
                    ),
                )
            })?;
        let (tasks, receiver) = mpsc::channel(TASK_BUFFER);
        let (startup, started) = std_mpsc::sync_channel(1);

        thread::Builder::new()
            .name(role.thread_name().to_owned())
            .spawn(move || {
                if let Err(error) = bind_cpu(cpu_core) {
                    let _ = startup.send(Err(error));
                    return;
                }
                let _ = startup.send(Ok(()));
                info!(
                    network_role = role.label(),
                    ?mode,
                    ?cpu_core,
                    "Binance network runtime started"
                );
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
                    format!(
                        "failed to start Binance {} network thread: {error}",
                        role.label()
                    ),
                )
            })?;
        started
            .recv()
            .map_err(|_| self_stopped(role, "network thread stopped during startup"))?
            .map_err(|error| {
                ExchangeError::new(
                    ExchangeErrorKind::InvalidRequest,
                    format!(
                        "failed to bind Binance {} network thread: {error}",
                        role.label()
                    ),
                )
            })?;

        Ok(Self { role, tasks })
    }

    /// Schedules a long-lived network task. The task is spawned by the
    /// current-thread runtime, so its socket polling remains on the selected
    /// network thread even when the caller is the strategy runtime.
    pub(crate) fn spawn<F>(&self, task: F) -> ExchangeResult<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.tasks
            .try_send(Box::pin(task))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    self_stopped(self.role, "network runtime task queue is full")
                }
                mpsc::error::TrySendError::Closed(_) => {
                    self_stopped(self.role, "network runtime stopped")
                }
            })
    }

    /// Runs a short network operation on the selected network thread and
    /// returns its result to the caller's runtime.
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
            .map_err(|_| self_stopped(self.role, "network operation response channel closed"))?
    }
}

async fn run_network_loop(mut tasks: mpsc::Receiver<NetworkTask>) {
    while let Some(task) = tasks.recv().await {
        tokio::spawn(task);
    }
}

fn self_stopped(role: NetworkRole, detail: &'static str) -> ExchangeError {
    ExchangeError::new(
        ExchangeErrorKind::ServiceUnavailable,
        format!("Binance {} {detail}", role.label()),
    )
}

#[cfg(test)]
mod tests {
    use std::thread::ThreadId;

    use super::*;

    async fn runtime_identity(runtime: &NetworkRuntime) -> (ThreadId, String) {
        runtime
            .call(async move {
                Ok((
                    thread::current().id(),
                    thread::current().name().unwrap().to_owned(),
                ))
            })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn roles_run_on_distinct_named_threads() {
        let market =
            NetworkRuntime::new(NetworkRole::MarketData, ExecutionMode::EventDriven, None).unwrap();
        let trading =
            NetworkRuntime::new(NetworkRole::Trading, ExecutionMode::EventDriven, None).unwrap();

        let (market_id, market_name) = runtime_identity(&market).await;
        let (trading_id, trading_name) = runtime_identity(&trading).await;

        assert_ne!(market_id, trading_id);
        assert_eq!(market_name, "maker-network-market-data");
        assert_eq!(trading_name, "maker-network-trading");
    }

    #[tokio::test]
    async fn busy_spin_runtime_drives_network_operations() {
        let runtime =
            NetworkRuntime::new(NetworkRole::Trading, ExecutionMode::BusySpin, None).unwrap();

        assert_eq!(runtime.call(async { Ok(7_u8) }).await.unwrap(), 7);
    }
}
