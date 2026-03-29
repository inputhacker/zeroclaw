use super::ContextBookHandle;
use crate::config::Config;
use anyhow::{Context, Result};
use tokio::time::{Duration, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

const HEALTH_TICK_SECS: u64 = 30;

pub async fn run(
    _config: Config,
    handle: ContextBookHandle,
    shutdown: Option<CancellationToken>,
) -> Result<()> {
    handle.mark_daemon_supervised();

    if let Some(error) = handle.resolved_config().validation_error.clone() {
        handle.mark_error(error.clone());
        if let Err(persist_error) = persist_runtime_state(&handle) {
            tracing::warn!("context_book failed to persist error state: {persist_error}");
        }
        anyhow::bail!("{error}");
    }

    handle
        .store()
        .initialize()
        .context("failed to initialize context_book store")?;
    handle.set_store_initialized(true);
    handle.mark_idle("phase1 noop worker active; connectivity not started yet");
    persist_runtime_state(&handle)?;
    crate::health::mark_component_ok("context_book");

    let mut interval = tokio::time::interval(Duration::from_secs(HEALTH_TICK_SECS));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        if let Some(token) = &shutdown {
            tokio::select! {
                () = token.cancelled() => {
                    handle.mark_shutdown_requested();
                    persist_runtime_state(&handle)?;
                    handle.mark_stopped("context_book worker stopped after daemon shutdown");
                    persist_runtime_state(&handle)?;
                    crate::health::mark_component_ok("context_book");
                    return Ok(());
                }
                _ = interval.tick() => {}
            }
        } else {
            interval.tick().await;
        }

        handle.mark_idle("phase1 noop worker active; connectivity not started yet");
        persist_runtime_state(&handle)?;
        crate::health::mark_component_ok("context_book");
    }
}

fn persist_runtime_state(handle: &ContextBookHandle) -> Result<()> {
    handle
        .store()
        .save_runtime_state(&handle.snapshot())
        .context("failed to persist context_book runtime snapshot")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context_book::shared_handle;
    use tempfile::TempDir;

    #[tokio::test]
    async fn worker_persists_idle_runtime_state() {
        let tmp = TempDir::new().expect("temp dir");
        let mut config = Config {
            workspace_dir: tmp.path().join("workspace"),
            config_path: tmp.path().join("config.toml"),
            ..Config::default()
        };
        config.context_book.enabled = true;

        let handle = shared_handle(&config);
        let worker = tokio::spawn(run(config, handle.clone(), None));

        tokio::time::sleep(Duration::from_millis(50)).await;
        worker.abort();
        let _ = worker.await;

        let status = handle.status_report();
        assert_eq!(status.runtime.worker_state, "idle");
        assert!(
            status
                .persisted_runtime
                .as_ref()
                .is_some_and(|runtime| runtime.worker_state == "idle")
        );
    }

    #[tokio::test]
    async fn worker_persists_stopped_state_on_shutdown_signal() {
        let tmp = TempDir::new().expect("temp dir");
        let mut config = Config {
            workspace_dir: tmp.path().join("workspace"),
            config_path: tmp.path().join("config.toml"),
            ..Config::default()
        };
        config.context_book.enabled = true;

        let handle = shared_handle(&config);
        let shutdown = CancellationToken::new();
        let worker = tokio::spawn(run(config, handle.clone(), Some(shutdown.child_token())));

        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown.cancel();
        let result = worker.await.expect("worker join");

        assert!(result.is_ok());
        let status = handle.status_report();
        assert_eq!(status.runtime.worker_state, "stopped");
        assert!(
            status
                .persisted_runtime
                .as_ref()
                .is_some_and(|runtime| runtime.worker_state == "stopped")
        );
    }
}
