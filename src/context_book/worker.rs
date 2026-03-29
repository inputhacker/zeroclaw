use super::ContextBookHandle;
use super::client::{ContextBookClient, ContextBookClientErrorKind};
use super::events::{ContextBookSseParser, ParsedContextBookSseFrame};
use crate::config::Config;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use tokio::time::{Duration, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

const HEALTH_TICK_SECS: u64 = 30;

pub async fn run(
    config: Config,
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
    handle.restore_persisted_runtime();
    handle.mark_idle("context_book worker initialized; waiting for connectivity");
    persist_runtime_state(&handle)?;
    crate::health::mark_component_ok("context_book");

    let client = ContextBookClient::new(&config);
    let reconnect_backoff = handle.resolved_config().reconnect_backoff_ms.max(1);
    let max_backoff = handle
        .resolved_config()
        .max_reconnect_backoff_ms
        .max(reconnect_backoff);
    let mut backoff_ms = reconnect_backoff;
    let mut interval = tokio::time::interval(Duration::from_secs(HEALTH_TICK_SECS));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        if let Some(token) = &shutdown {
            tokio::select! {
                () = token.cancelled() => {
                    handle.mark_shutdown_requested();
                    persist_runtime_state(&handle)?;
                    if let Ok(session) = client.ensure_session().await {
                        if let Err(error) = client.mark_inactive(&session).await {
                            tracing::warn!("context_book graceful inactive transition failed: {error}");
                        }
                    }
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

        match connect_and_sync_once(&client, &handle, shutdown.as_ref()).await {
            Ok(progressed) => {
                backoff_ms = reconnect_backoff;
                if !progressed {
                    handle.mark_idle("context_book SSE idle; waiting for next reconnect tick");
                    persist_runtime_state(&handle)?;
                }
                crate::health::mark_component_ok("context_book");
            }
            Err(error) => {
                handle.mark_error(error.to_string());
                persist_runtime_state(&handle)?;
                crate::health::mark_component_error("context_book", error.to_string());
                sleep_or_shutdown(Duration::from_millis(backoff_ms), shutdown.as_ref()).await;
                backoff_ms = (backoff_ms.saturating_mul(2)).min(max_backoff);
            }
        }
    }
}

fn persist_runtime_state(handle: &ContextBookHandle) -> Result<()> {
    handle
        .store()
        .save_runtime_state(&handle.snapshot())
        .context("failed to persist context_book runtime snapshot")
}

async fn connect_and_sync_once(
    client: &ContextBookClient,
    handle: &ContextBookHandle,
    shutdown: Option<&CancellationToken>,
) -> Result<bool> {
    let session = client
        .ensure_session()
        .await
        .map_err(anyhow::Error::new)
        .context("failed to establish Context Book session")?;
    client
        .activate_agent(&session)
        .await
        .map_err(anyhow::Error::new)
        .context("failed to activate Context Book agent")?;
    handle.mark_session_ready(
        &session.agent_id,
        "context_book session ready; opening events stream",
    );
    persist_runtime_state(handle)?;

    let cursor = handle.snapshot().last_event_id;
    match client.open_event_stream(&session, cursor.as_deref()).await {
        Ok(response) => {
            handle.mark_stream_connected(&session.agent_id, "context_book SSE connected");
            persist_runtime_state(handle)?;
            consume_sse_stream(response, handle, shutdown).await?;
            Ok(true)
        }
        Err(error) if error.kind == ContextBookClientErrorKind::CursorNotFound => {
            handle.reset_cursor("context_book resume cursor rejected by server");
            handle
                .store()
                .reset_cursor(&handle.snapshot())
                .context("failed to persist cursor reset after CURSOR_NOT_FOUND")?;
            persist_runtime_state(handle)?;
            if handle.resolved_config().polling_fallback_enabled {
                poll_once(client, &session, handle).await?;
                return Ok(true);
            }
            Ok(false)
        }
        Err(error) => {
            if handle.resolved_config().polling_fallback_enabled {
                handle.mark_retrying("context_book SSE unavailable; polling fallback active");
                persist_runtime_state(handle)?;
                poll_once(client, &session, handle).await?;
                return Ok(true);
            }
            Err(anyhow::Error::new(error).context("failed to open Context Book SSE stream"))
        }
    }
}

async fn consume_sse_stream(
    response: reqwest::Response,
    handle: &ContextBookHandle,
    shutdown: Option<&CancellationToken>,
) -> Result<()> {
    let mut parser = ContextBookSseParser::default();
    let mut buffer = String::new();
    let mut stream = response.bytes_stream();

    loop {
        tokio::select! {
            maybe_chunk = stream.next() => {
                let Some(chunk) = maybe_chunk else {
                    handle.mark_retrying("context_book SSE stream closed; reconnecting");
                    persist_runtime_state(handle)?;
                    return Ok(());
                };
                let chunk = chunk.context("failed to read Context Book SSE chunk")?;
                buffer.push_str(&String::from_utf8_lossy(&chunk));
                while let Some(pos) = buffer.find('\n') {
                    let line = buffer[..pos].trim_end_matches('\r').to_string();
                    buffer.drain(..=pos);
                    if let Some(frame) = parser.push_line(&line)? {
                        match frame {
                            ParsedContextBookSseFrame::Heartbeat => {}
                            ParsedContextBookSseFrame::Event(event) => {
                                handle.mark_event_applied(&event.event_id);
                                let snapshot = handle.snapshot();
                                if handle
                                    .store()
                                    .record_event(&event, &snapshot)
                                    .context("failed to record Context Book event")?
                                {
                                    persist_runtime_state(handle)?;
                                }
                            }
                        }
                    }
                }
            }
            () = cancel_or_never(shutdown) => {
                return Ok(());
            }
        }
    }
}

async fn poll_once(
    client: &ContextBookClient,
    session: &super::client::ContextBookSession,
    handle: &ContextBookHandle,
) -> Result<()> {
    let cursor = handle.snapshot().last_event_id;
    let events = client
        .poll_events(session, cursor.as_deref())
        .await
        .map_err(anyhow::Error::new)
        .context("failed to poll Context Book events")?;
    for event in events {
        handle.mark_event_applied(&event.event_id);
        let snapshot = handle.snapshot();
        if handle
            .store()
            .record_event(&event, &snapshot)
            .context("failed to persist polled Context Book event")?
        {
            persist_runtime_state(handle)?;
        }
    }
    Ok(())
}

async fn sleep_or_shutdown(duration: Duration, shutdown: Option<&CancellationToken>) {
    if let Some(token) = shutdown {
        tokio::select! {
            () = token.cancelled() => {}
            () = tokio::time::sleep(duration) => {}
        }
    } else {
        tokio::time::sleep(duration).await;
    }
}

async fn cancel_or_never(shutdown: Option<&CancellationToken>) {
    if let Some(token) = shutdown {
        token.cancelled().await;
    } else {
        std::future::pending::<()>().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::profiles::{
        AuthProfile, AuthProfileKind, AuthProfilesStore, TokenSet, profile_id,
    };
    use crate::auth::state_dir_from_config;
    use crate::context_book::shared_handle;
    use axum::{
        Router,
        extract::State,
        http::{HeaderMap, StatusCode},
        response::IntoResponse,
        routing::{get, patch},
    };
    use chrono::{Duration as ChronoDuration, Utc};
    use std::collections::BTreeMap;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tempfile::TempDir;
    use tokio::net::TcpListener;

    #[derive(Clone)]
    struct WorkerAppState {
        poll_calls: Arc<AtomicUsize>,
    }

    fn test_config(tmp: &TempDir, manual_url: String) -> Config {
        let mut config = Config {
            workspace_dir: tmp.path().join("workspace"),
            config_path: tmp.path().join("config.toml"),
            ..Config::default()
        };
        config.context_book.enabled = true;
        config.context_book.discovery_enabled = false;
        config.context_book.manual_url = Some(manual_url);
        config.context_book.allowed_hosts = vec!["127.0.0.1".into()];
        config.context_book.allow_private_hosts = true;
        config
    }

    #[tokio::test]
    async fn worker_resets_cursor_and_uses_polling_fallback() {
        async fn patch_status() -> impl IntoResponse {
            axum::Json(serde_json::json!({"ok": true}))
        }

        async fn events_stream(headers: HeaderMap) -> impl IntoResponse {
            let last_event_id = headers
                .get("last-event-id")
                .and_then(|value| value.to_str().ok());
            if last_event_id == Some("evt-stale") {
                return (
                    StatusCode::CONFLICT,
                    axum::Json(serde_json::json!({
                        "error": {
                            "code": "CURSOR_NOT_FOUND",
                            "message": "stale cursor"
                        }
                    })),
                )
                    .into_response();
            }
            (
                StatusCode::OK,
                [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                "",
            )
                .into_response()
        }

        async fn events(State(state): State<WorkerAppState>) -> impl IntoResponse {
            state.poll_calls.fetch_add(1, Ordering::SeqCst);
            axum::Json(serde_json::json!([
                {
                    "eventId": "evt-100",
                    "eventType": "context.created",
                    "occurredAt": "2026-03-29T00:00:00Z",
                    "producerAgentId": "peer-a",
                    "entityId": "ctx-100",
                    "payload": {},
                    "meta": {}
                },
                {
                    "eventId": "evt-100",
                    "eventType": "context.created",
                    "occurredAt": "2026-03-29T00:00:00Z",
                    "producerAgentId": "peer-a",
                    "entityId": "ctx-100",
                    "payload": {},
                    "meta": {}
                }
            ]))
        }

        let app_state = WorkerAppState {
            poll_calls: Arc::new(AtomicUsize::new(0)),
        };
        let app = Router::new()
            .route("/agents/workspace/status", patch(patch_status))
            .route("/events/stream", get(events_stream))
            .route("/events", get(events))
            .with_state(app_state.clone());
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve axum");
        });

        let tmp = TempDir::new().expect("temp dir");
        let config = test_config(&tmp, format!("http://{addr}"));
        let state_dir = state_dir_from_config(&config);
        let auth_store = AuthProfilesStore::new(&state_dir, config.secrets.encrypt);
        auth_store
            .upsert_profile(
                AuthProfile {
                    id: profile_id("context-book", "default"),
                    provider: "context-book".into(),
                    profile_name: "default".into(),
                    kind: AuthProfileKind::OAuth,
                    account_id: None,
                    workspace_id: None,
                    token_set: Some(TokenSet {
                        access_token: "worker-token".into(),
                        refresh_token: Some("worker-refresh".into()),
                        id_token: None,
                        expires_at: Some(Utc::now() + ChronoDuration::minutes(30)),
                        token_type: None,
                        scope: None,
                    }),
                    token: None,
                    metadata: BTreeMap::from([("agent_id".to_string(), "workspace".to_string())]),
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                },
                true,
            )
            .await
            .expect("seed worker auth profile");

        let handle = shared_handle(&config);
        let mut snapshot = handle.snapshot();
        snapshot.agent_id = Some("workspace".into());
        snapshot.last_event_id = Some("evt-stale".into());
        snapshot.cursor_generation = 0;
        handle
            .store()
            .save_runtime_state(&snapshot)
            .expect("seed persisted runtime");
        let shutdown = CancellationToken::new();
        let worker = tokio::spawn(run(config, handle.clone(), Some(shutdown.child_token())));

        tokio::time::sleep(Duration::from_millis(200)).await;
        shutdown.cancel();
        let result = worker.await.expect("worker join");

        assert!(result.is_ok());
        let status = handle.status_report();
        assert_eq!(status.runtime.worker_state, "stopped");
        assert_eq!(app_state.poll_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            handle.store().seen_event_count().expect("seen event count"),
            1
        );
        assert!(status.persisted_runtime.as_ref().is_some_and(|runtime| {
            runtime.worker_state == "stopped"
                && runtime.last_event_id.as_deref() == Some("evt-100")
                && runtime.cursor_generation == 1
        }));

        server.abort();
        let _ = server.await;
    }
}
