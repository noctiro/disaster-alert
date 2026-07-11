use crate::db::Database;
use crate::providers::{FanStudioSource, WolfxSource};
use crate::services::DisasterDispatcher;
use anyhow::{Context, Result};
use axum::Router;
use std::time::Duration;
use tokio::sync::{oneshot, watch};
use tokio::task::{JoinError, JoinHandle};

pub(crate) const FORCED_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

// This coordinator owns every JoinHandle, so each task result is observed once.
type TaskResult = Result<&'static str>;
type JoinResult = std::result::Result<TaskResult, JoinError>;

#[derive(Clone, Copy)]
enum TaskKind {
    HttpServer,
    Dispatcher,
    Wolfx,
    FanStudio,
}

struct ManagedTask {
    handle: JoinHandle<TaskResult>,
    completed: bool,
}

impl ManagedTask {
    fn new(handle: JoinHandle<TaskResult>) -> Self {
        Self {
            handle,
            completed: false,
        }
    }

    fn mark_completed(&mut self) {
        debug_assert!(!self.completed);
        self.completed = true;
    }

    fn collect_completion(&mut self, result: JoinResult, errors: &mut Vec<anyhow::Error>) {
        self.mark_completed();
        collect_task_result(flatten_task_result(result).map(Some), errors);
    }

    async fn abort_and_reap(&mut self) -> Result<Option<&'static str>> {
        if self.completed {
            return Ok(None);
        }
        let requested_abort = !self.handle.is_finished();
        if requested_abort {
            self.handle.abort();
        }
        self.completed = true;
        match (&mut self.handle).await {
            Ok(Ok(name)) => Ok(Some(name)),
            Ok(Err(error)) => Err(error),
            Err(error) if requested_abort && error.is_cancelled() => Ok(None),
            Err(error) => Err(error).context("managed task panicked during forced shutdown"),
        }
    }
}

struct ManagedTasks {
    server: ManagedTask,
    dispatcher: ManagedTask,
    wolfx: ManagedTask,
    fanstudio: ManagedTask,
}

impl ManagedTasks {
    fn new(
        server: JoinHandle<TaskResult>,
        dispatcher: JoinHandle<TaskResult>,
        wolfx: JoinHandle<TaskResult>,
        fanstudio: JoinHandle<TaskResult>,
    ) -> Self {
        Self {
            server: ManagedTask::new(server),
            dispatcher: ManagedTask::new(dispatcher),
            wolfx: ManagedTask::new(wolfx),
            fanstudio: ManagedTask::new(fanstudio),
        }
    }

    fn mark_completed(&mut self, task: TaskKind) {
        match task {
            TaskKind::HttpServer => self.server.mark_completed(),
            TaskKind::Dispatcher => self.dispatcher.mark_completed(),
            TaskKind::Wolfx => self.wolfx.mark_completed(),
            TaskKind::FanStudio => self.fanstudio.mark_completed(),
        }
    }

    fn providers_completed(&self) -> bool {
        self.wolfx.completed && self.fanstudio.completed
    }

    fn all_completed(&self) -> bool {
        self.server.completed
            && self.dispatcher.completed
            && self.wolfx.completed
            && self.fanstudio.completed
    }

    async fn abort_and_reap(&mut self) -> Result<()> {
        let (server_result, dispatcher_result, wolfx_result, fanstudio_result) = tokio::join!(
            self.server.abort_and_reap(),
            self.dispatcher.abort_and_reap(),
            self.wolfx.abort_and_reap(),
            self.fanstudio.abort_and_reap(),
        );
        let mut errors = Vec::new();
        collect_task_result(server_result, &mut errors);
        collect_task_result(dispatcher_result, &mut errors);
        collect_task_result(wolfx_result, &mut errors);
        collect_task_result(fanstudio_result, &mut errors);
        finish_task_results(errors)
    }
}

#[cfg(unix)]
struct ShutdownSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(not(unix))]
struct ShutdownSignals;

impl ShutdownSignals {
    fn new() -> Result<Self> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};

            Ok(Self {
                interrupt: signal(SignalKind::interrupt())
                    .context("failed to install SIGINT handler")?,
                terminate: signal(SignalKind::terminate())
                    .context("failed to install SIGTERM handler")?,
            })
        }

        #[cfg(not(unix))]
        Ok(Self)
    }

    async fn recv(&mut self) -> Result<()> {
        #[cfg(unix)]
        {
            tokio::select! {
                signal = self.interrupt.recv() => {
                    if signal.is_none() {
                        anyhow::bail!("SIGINT handler closed unexpectedly");
                    }
                }
                signal = self.terminate.recv() => {
                    if signal.is_none() {
                        anyhow::bail!("SIGTERM handler closed unexpectedly");
                    }
                }
            }
        }

        #[cfg(not(unix))]
        tokio::signal::ctrl_c()
            .await
            .context("failed to install Ctrl+C handler")?;

        Ok(())
    }
}

pub(crate) async fn run_until_shutdown(
    listener: tokio::net::TcpListener,
    app: Router,
    db: Database,
    dispatcher: DisasterDispatcher,
    wolfx: WolfxSource,
    fanstudio: FanStudioSource,
    shutdown_timeout: Duration,
) -> Result<()> {
    let mut shutdown_signals = ShutdownSignals::new()?;
    let dispatcher_for_shutdown = dispatcher.clone();
    let (provider_shutdown, provider_shutdown_receiver) = watch::channel(false);
    let dispatcher_task = tokio::spawn(async move {
        dispatcher.run().await.context("dispatcher task failed")?;
        Ok::<_, anyhow::Error>("dispatcher")
    });
    let wolfx_shutdown = provider_shutdown_receiver.clone();
    let wolfx_task = tokio::spawn(async move {
        wolfx
            .run(wolfx_shutdown)
            .await
            .context("Wolfx provider failed")?;
        Ok("Wolfx provider")
    });
    let fanstudio_task = tokio::spawn(async move {
        fanstudio
            .run(provider_shutdown_receiver)
            .await
            .context("Fan Studio provider failed")?;
        Ok("Fan Studio provider")
    });
    let (http_shutdown, http_shutdown_receiver) = oneshot::channel();
    let server_task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _result = http_shutdown_receiver.await;
            })
            .await
            .context("HTTP server failed")?;
        Ok::<_, anyhow::Error>("HTTP server")
    });
    let mut tasks = ManagedTasks::new(server_task, dispatcher_task, wolfx_task, fanstudio_task);

    let (run_result, completed_task) = tokio::select! {
        signal = shutdown_signals.recv() => (signal, None),
        result = &mut tasks.server.handle => (
            unexpected_task_completion(result),
            Some(TaskKind::HttpServer),
        ),
        result = &mut tasks.dispatcher.handle => (
            unexpected_task_completion(result),
            Some(TaskKind::Dispatcher),
        ),
        result = &mut tasks.wolfx.handle => (
            unexpected_task_completion(result),
            Some(TaskKind::Wolfx),
        ),
        result = &mut tasks.fanstudio.handle => (
            unexpected_task_completion(result),
            Some(TaskKind::FanStudio),
        ),
    };
    if let Some(task) = completed_task {
        tasks.mark_completed(task);
    }

    tracing::info!(event = "server.shutdown_started", "server.shutdown_started");
    let _result = http_shutdown.send(());
    let _result = provider_shutdown.send(true);

    let cleanup_result = drain_tasks(
        &mut tasks,
        &dispatcher_for_shutdown,
        &mut shutdown_signals,
        shutdown_timeout,
    )
    .await;
    let cleanup_result = if tasks.all_completed() {
        cleanup_result
    } else {
        let forced_cleanup = tokio::time::timeout(FORCED_SHUTDOWN_TIMEOUT, tasks.abort_and_reap())
            .await
            .unwrap_or_else(|_elapsed| {
                Err(anyhow::anyhow!(
                    "timed out while reaping tasks during forced shutdown"
                ))
            });
        append_shutdown_result(
            cleanup_result,
            forced_cleanup,
            "forced shutdown task cleanup failed",
        )
    };
    let flush_result = flush_database(&db, &mut shutdown_signals, shutdown_timeout).await;
    if cleanup_result.is_ok() && flush_result.is_ok() {
        tracing::info!(
            event = "server.shutdown_complete",
            "server.shutdown_complete"
        );
    } else {
        tracing::warn!(
            event = "server.shutdown_incomplete",
            cleanup_succeeded = cleanup_result.is_ok(),
            flush_succeeded = flush_result.is_ok(),
            "server.shutdown_incomplete"
        );
    }

    combine_shutdown_results(run_result, cleanup_result, flush_result)
}

async fn drain_tasks(
    tasks: &mut ManagedTasks,
    dispatcher: &DisasterDispatcher,
    shutdown_signals: &mut ShutdownSignals,
    timeout: Duration,
) -> Result<()> {
    let mut dispatcher_closed = false;
    let mut errors = Vec::new();
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);

    loop {
        if tasks.providers_completed() && !dispatcher_closed {
            dispatcher.close().await;
            dispatcher_closed = true;
        }
        if tasks.all_completed() {
            return finish_task_results(errors);
        }

        let server_pending = !tasks.server.completed;
        let dispatcher_pending = dispatcher_closed && !tasks.dispatcher.completed;
        let wolfx_pending = !tasks.wolfx.completed;
        let fanstudio_pending = !tasks.fanstudio.completed;
        tokio::select! {
            result = &mut tasks.server.handle, if server_pending => {
                tasks.server.collect_completion(result, &mut errors);
            }
            result = &mut tasks.dispatcher.handle, if dispatcher_pending => {
                tasks.dispatcher.collect_completion(result, &mut errors);
            }
            result = &mut tasks.wolfx.handle, if wolfx_pending => {
                tasks.wolfx.collect_completion(result, &mut errors);
            }
            result = &mut tasks.fanstudio.handle, if fanstudio_pending => {
                tasks.fanstudio.collect_completion(result, &mut errors);
            }
            () = &mut deadline => {
                tracing::warn!(event = "server.shutdown_timed_out", timeout_seconds = timeout.as_secs(), "server.shutdown_timed_out");
                return append_shutdown_result(
                    Err(anyhow::anyhow!(
                        "graceful shutdown timed out after {} seconds",
                        timeout.as_secs()
                    )),
                    finish_task_results(errors),
                    "shutdown task failed",
                );
            }
            signal = shutdown_signals.recv() => {
                let signal_result = match signal {
                    Ok(()) => {
                        tracing::warn!(event = "server.shutdown_forced", "server.shutdown_forced");
                        Err(anyhow::anyhow!("graceful shutdown interrupted by a second signal"))
                    }
                    Err(error) => Err(error.context("failed to listen for a second shutdown signal")),
                };
                return append_shutdown_result(
                    signal_result,
                    finish_task_results(errors),
                    "shutdown task failed",
                );
            }
        }
    }
}

fn unexpected_task_completion(result: JoinResult) -> Result<()> {
    let name = flatten_task_result(result)?;
    anyhow::bail!("{name} terminated unexpectedly")
}

fn flatten_task_result(result: JoinResult) -> TaskResult {
    result.context("managed task panicked or was cancelled")?
}

fn log_stopped_task(name: &'static str) {
    tracing::info!(
        event = "server.background_task_stopped",
        task = name,
        "server.background_task_stopped"
    );
}

fn collect_task_result(result: Result<Option<&'static str>>, errors: &mut Vec<anyhow::Error>) {
    match result {
        Ok(Some(name)) => log_stopped_task(name),
        Ok(None) => {}
        Err(error) => errors.push(error),
    }
}

fn finish_task_results(mut errors: Vec<anyhow::Error>) -> Result<()> {
    let mut errors = errors.drain(..);
    let Some(first) = errors.next() else {
        return Ok(());
    };
    let additional = errors.map(|error| format!("{error:#}")).collect::<Vec<_>>();
    if additional.is_empty() {
        Err(first)
    } else {
        Err(first.context(format!(
            "additional shutdown task failures: {}",
            additional.join("; ")
        )))
    }
}

async fn flush_database(
    db: &Database,
    shutdown_signals: &mut ShutdownSignals,
    timeout: Duration,
) -> Result<()> {
    let flush = db.flush();
    tokio::pin!(flush);
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);
    let mut deadline_observed = false;
    let mut signal_observed = false;
    let mut errors = Vec::new();
    let flush_result = loop {
        tokio::select! {
            result = &mut flush => break result.context("failed to flush database during shutdown"),
            () = &mut deadline, if !deadline_observed => {
                deadline_observed = true;
                tracing::warn!(event = "server.database_flush_timed_out", "server.database_flush_timed_out");
                errors.push(anyhow::anyhow!("database flush exceeded the shutdown timeout"));
            }
            signal = shutdown_signals.recv(), if !signal_observed => {
                signal_observed = true;
                match signal {
                    Ok(()) => {
                        tracing::warn!(
                            event = "server.shutdown_signal_during_flush",
                            "server.shutdown_signal_during_flush"
                        );
                        errors.push(anyhow::anyhow!("database flush interrupted by a shutdown signal"));
                    }
                    Err(error) => errors.push(error.context("failed to listen for a shutdown signal during database flush")),
                }
            }
        }
    };
    append_shutdown_result(
        finish_task_results(errors),
        flush_result,
        "database flush failed",
    )
}

fn append_shutdown_result(
    primary_result: Result<()>,
    additional_result: Result<()>,
    additional_context: &str,
) -> Result<()> {
    match (primary_result, additional_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(primary), Err(additional)) => {
            Err(primary.context(format!("{additional_context}: {additional:#}")))
        }
    }
}

fn combine_shutdown_results(
    run_result: Result<()>,
    cleanup_result: Result<()>,
    flush_result: Result<()>,
) -> Result<()> {
    match run_result {
        Err(error) => append_shutdown_result(
            append_shutdown_result(Err(error), cleanup_result, "shutdown cleanup failed"),
            flush_result,
            "database flush failed",
        ),
        Ok(()) => append_shutdown_result(cleanup_result, flush_result, "database flush failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::{ManagedTask, combine_shutdown_results, finish_task_results};

    #[test]
    fn shutdown_error_preserves_the_original_failure() {
        let result = combine_shutdown_results(
            Err(anyhow::anyhow!("provider failed")),
            Err(anyhow::anyhow!("cleanup timed out")),
            Err(anyhow::anyhow!("flush failed")),
        );
        let message = result
            .err()
            .map(|error| format!("{error:#}"))
            .unwrap_or_default();
        assert!(message.contains("provider failed"));
        assert!(message.contains("cleanup timed out"));
        assert!(message.contains("flush failed"));
    }

    #[test]
    fn clean_run_reports_cleanup_failure() {
        let result =
            combine_shutdown_results(Ok(()), Err(anyhow::anyhow!("cleanup timed out")), Ok(()));
        assert!(result.is_err());
    }

    #[test]
    fn cleanup_failure_also_reports_a_flush_failure() {
        let result = combine_shutdown_results(
            Ok(()),
            Err(anyhow::anyhow!("cleanup interrupted")),
            Err(anyhow::anyhow!("flush failed")),
        );
        let message = result
            .err()
            .map(|error| format!("{error:#}"))
            .unwrap_or_default();
        assert!(message.contains("cleanup interrupted"));
        assert!(message.contains("flush failed"));
    }

    #[test]
    fn task_error_uses_the_earliest_failure_as_its_source() {
        let result = finish_task_results(vec![
            anyhow::anyhow!("first worker failed"),
            anyhow::anyhow!("second worker failed"),
        ]);
        let message = result
            .err()
            .map(|error| format!("{error:#}"))
            .unwrap_or_default();
        assert!(message.ends_with("first worker failed"));
        assert!(message.contains("second worker failed"));
    }

    #[tokio::test]
    async fn managed_task_abort_is_reaped_once() {
        let handle = tokio::spawn(async {
            std::future::pending::<()>().await;
            Ok::<_, anyhow::Error>("pending task")
        });
        let mut task = ManagedTask::new(handle);
        assert!(task.abort_and_reap().await.is_ok());
        assert!(task.completed);
        assert!(task.abort_and_reap().await.is_ok_and(|name| name.is_none()));
    }

    #[tokio::test]
    async fn managed_task_collects_a_finished_result_once() {
        let handle = tokio::spawn(async { Ok::<_, anyhow::Error>("finished task") });
        let mut task = ManagedTask::new(handle);
        let result = (&mut task.handle).await;
        let mut errors = Vec::new();
        task.collect_completion(result, &mut errors);
        assert!(task.completed);
        assert!(errors.is_empty());
    }
}
