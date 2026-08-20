//! Dedicated single-thread asynchronous network runtimes.
//!
//! Public market data and latency-critical trading traffic are scheduled on
//! separate current-thread Tokio runtimes so bursts on one cannot delay the
//! other. Each runtime has independently configurable parking behavior and CPU
//! affinity.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use futures_util::future::BoxFuture;
use maker_ports::{ExchangeError, ExchangeErrorKind, ExchangeFuture, ExchangeResult};
use maker_runtime::{
    BusyPoll, ExecutionMode, SpscConsumer, SpscProducer, TryPushError, bind_cpu, spsc_channel,
};
use tokio::sync::oneshot;

use crate::config::BinanceUsdmConfig;

type NetworkTask = BoxFuture<'static, ()>;
const TASK_BUFFER: usize = 64;
const STARTING: u8 = 0;
const READY: u8 = 1;
const AFFINITY_FAILED: u8 = 2;
const STARTUP_TIMEOUT: Duration = Duration::from_secs(5);

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

/// Move-only strategy-side producer for one dedicated network thread.
pub(crate) struct NetworkRuntime {
    role: NetworkRole,
    tasks: SpscProducer<NetworkTask>,
}

impl NetworkRuntime {
    pub(crate) fn new(role: NetworkRole, config: &BinanceUsdmConfig) -> ExchangeResult<Self> {
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
        let (tasks, receiver) = spsc_channel(TASK_BUFFER);
        let (mode, cpu_core) = match role {
            NetworkRole::MarketData => (config.market_data_mode(), config.market_data_cpu_core()),
            NetworkRole::Trading => (config.trading_mode(), config.trading_cpu_core()),
        };

        let startup = Arc::new(AtomicU8::new(STARTING));
        let worker_startup = startup.clone();
        thread::Builder::new()
            .name(role.thread_name().to_owned())
            .spawn(move || {
                if bind_cpu(cpu_core).is_err() {
                    worker_startup.store(AFFINITY_FAILED, Ordering::Release);
                    return;
                }
                worker_startup.store(READY, Ordering::Release);
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
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        loop {
            match startup.load(Ordering::Acquire) {
                READY => break,
                AFFINITY_FAILED => {
                    return Err(ExchangeError::new(
                        ExchangeErrorKind::InvalidRequest,
                        format!("failed to bind Binance {} network thread", role.label()),
                    ));
                }
                STARTING if Instant::now() < deadline => std::hint::spin_loop(),
                STARTING => {
                    return Err(self_stopped(role, "network thread startup timed out"));
                }
                _ => unreachable!("network startup state is valid"),
            }
        }

        Ok(Self { role, tasks })
    }

    /// Schedules a long-lived task without waiting or cloning the producer.
    pub(crate) fn spawn<F>(&mut self, task: F) -> ExchangeResult<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.tasks
            .try_push(Box::pin(task))
            .map_err(|error| match error {
                TryPushError::Full(_) => {
                    self_stopped(self.role, "network runtime task queue is full")
                }
                TryPushError::ConsumerDropped(_) => {
                    self_stopped(self.role, "network runtime stopped")
                }
            })
    }

    /// Dispatches a short operation and returns an owned response future.
    pub(crate) fn call<F, T>(&mut self, operation: F) -> ExchangeFuture<T>
    where
        F: Future<Output = ExchangeResult<T>> + Send + 'static,
        T: Send + 'static,
    {
        let (reply, response) = oneshot::channel();
        if let Err(error) = self.spawn(async move {
            let _ = reply.send(operation.await);
        }) {
            return Box::pin(async move { Err(error) });
        }
        let role = self.role;
        Box::pin(async move {
            response
                .await
                .map_err(|_| self_stopped(role, "network operation response channel closed"))?
        })
    }
}

async fn run_network_loop(mut tasks: SpscConsumer<NetworkTask>) {
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

    async fn runtime_identity(runtime: &mut NetworkRuntime) -> (ThreadId, String) {
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
        let mut market =
            NetworkRuntime::new(NetworkRole::MarketData, &BinanceUsdmConfig::default()).unwrap();
        let mut trading =
            NetworkRuntime::new(NetworkRole::Trading, &BinanceUsdmConfig::default()).unwrap();

        let (market_id, market_name) = runtime_identity(&mut market).await;
        let (trading_id, trading_name) = runtime_identity(&mut trading).await;

        assert_ne!(market_id, trading_id);
        assert_eq!(market_name, "maker-network-market-data");
        assert_eq!(trading_name, "maker-network-trading");
    }

    #[tokio::test]
    async fn busy_spin_runtime_drives_network_operations() {
        let mut runtime =
            NetworkRuntime::new(NetworkRole::Trading, &BinanceUsdmConfig::default()).unwrap();

        assert_eq!(runtime.call(async { Ok(7_u8) }).await.unwrap(), 7);
    }

    #[tokio::test]
    async fn event_driven_runtime_wakes_for_network_operations() {
        let config = BinanceUsdmConfig::default().with_network_runtimes(
            ExecutionMode::EventDriven,
            None,
            ExecutionMode::EventDriven,
            None,
        );
        let mut runtime = NetworkRuntime::new(NetworkRole::Trading, &config).unwrap();

        assert_eq!(runtime.call(async { Ok(7_u8) }).await.unwrap(), 7);
    }
}
