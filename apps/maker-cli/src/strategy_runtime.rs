use std::{future::Future, thread};

use anyhow::{Context as _, Result, anyhow};
use maker_engine::MakerEngine;
use maker_runtime::{BusyPoll, ExecutionMode, bind_cpu};
use tokio::sync::oneshot;
use tracing::info;

#[derive(Clone, Copy, Debug)]
pub(crate) struct StrategyRuntimeSettings {
    pub(crate) mode: ExecutionMode,
    pub(crate) cpu_core: Option<usize>,
}

pub(crate) struct StrategyThread {
    shutdown: Option<oneshot::Sender<()>>,
    completion: oneshot::Receiver<Result<()>>,
    join: thread::JoinHandle<()>,
}

pub(crate) fn spawn(
    engine: MakerEngine,
    settings: StrategyRuntimeSettings,
) -> Result<StrategyThread> {
    let (shutdown, shutdown_receiver) = oneshot::channel();
    let (completion, completion_receiver) = oneshot::channel();
    let join = thread::Builder::new()
        .name("maker-strategy".to_owned())
        .spawn(move || {
            let result = run(engine, settings, shutdown_receiver);
            let _ = completion.send(result);
        })
        .context("failed to start maker strategy thread")?;

    Ok(StrategyThread {
        shutdown: Some(shutdown),
        completion: completion_receiver,
        join,
    })
}

impl StrategyThread {
    pub(crate) async fn run_until<S>(mut self, shutdown_signal: S) -> Result<()>
    where
        S: Future<Output = ()> + Send,
    {
        let result = tokio::select! {
            result = &mut self.completion => completion_result(result),
            _ = shutdown_signal => {
                if let Some(shutdown) = self.shutdown.take() {
                    let _ = shutdown.send(());
                }
                completion_result(self.completion.await)
            }
        };

        self.join
            .join()
            .map_err(|_| anyhow!("maker strategy thread panicked"))?;
        result
    }
}

fn completion_result(result: Result<Result<()>, oneshot::error::RecvError>) -> Result<()> {
    result.context("maker strategy thread stopped without a result")?
}

fn run(
    mut engine: MakerEngine,
    settings: StrategyRuntimeSettings,
    shutdown: oneshot::Receiver<()>,
) -> Result<()> {
    bind_cpu(settings.cpu_core).context("failed to bind maker strategy thread")?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build maker strategy runtime")?;
    info!(
        mode = ?settings.mode,
        cpu_core = ?settings.cpu_core,
        "maker strategy runtime started"
    );
    let future = engine.run(async move {
        let _ = shutdown.await;
    });

    match settings.mode {
        ExecutionMode::EventDriven => runtime
            .block_on(future)
            .context("maker strategy engine failed"),
        ExecutionMode::BusySpin => runtime
            .block_on(BusyPoll::new(future))
            .context("maker strategy engine failed"),
    }
}
