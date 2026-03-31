use super::ContextBookHandle;
use super::client::{ContextBookClient, ContextBookClientErrorKind};
use super::events::{ContextBookSseParser, ParsedContextBookSseFrame};
use super::handle::{ContextBookContractValidationState, ContextBookDegradedMode};
use super::service::ContextBookStatusReport;
use super::store::{
    ContextBookAgentSnapshot, ContextBookEventSyncUpdate, ContextBookSubscriptionsSnapshot,
};
use crate::config::{Config, ContextBookSubscriptionMode};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use tokio::time::{Duration, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

const HEALTH_TICK_SECS: u64 = 30;
const HEALTH_STALE_SECONDS: i64 = 120;

fn format_error_chain(error: &anyhow::Error) -> String {
    let mut parts = Vec::new();
    for cause in error.chain() {
        let message = cause.to_string();
        if !message.is_empty() {
            parts.push(message);
        }
    }

    if parts.is_empty() {
        return String::new();
    }

    parts.join(": ")
}

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
    seed_configured_subscriptions_if_missing(&handle)?;
    handle.set_store_initialized(true);
    handle.restore_persisted_runtime();
    handle.mark_idle("context_book worker initialized; waiting for connectivity");
    persist_runtime_state(&handle)?;
    refresh_component_health(&handle);

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
                    let client = ContextBookClient::new(&handle.source_config());
                    if let Ok(session) = client.ensure_session().await {
                        if let Err(error) = client.mark_inactive(&session).await {
                            tracing::warn!("context_book graceful inactive transition failed: {error}");
                        }
                    }
                    handle.mark_stopped("context_book worker stopped after daemon shutdown");
                    persist_runtime_state(&handle)?;
                    refresh_component_health(&handle);
                    return Ok(());
                }
                _ = interval.tick() => {}
            }
        } else {
            interval.tick().await;
        }

        match connect_and_sync_once(&handle, shutdown.as_ref()).await {
            Ok(progressed) => {
                backoff_ms = reconnect_backoff;
                if !progressed {
                    handle.mark_idle("context_book SSE idle; waiting for next reconnect tick");
                    persist_runtime_state(&handle)?;
                }
                refresh_component_health(&handle);
            }
            Err(error) => {
                let error_text = format_error_chain(&error);
                handle.mark_error(if error_text.is_empty() {
                    error.to_string()
                } else {
                    error_text.clone()
                });
                persist_runtime_state(&handle)?;
                crate::health::mark_component_error(
                    "context_book",
                    if error_text.is_empty() {
                        error.to_string()
                    } else {
                        error_text
                    },
                );
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

fn refresh_component_health(handle: &ContextBookHandle) {
    let status = handle.status_report();
    if let Some(error) = status
        .runtime
        .last_error
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        crate::health::mark_component_error("context_book", error);
        return;
    }

    let mut warnings = Vec::new();
    if !status.contract.degraded_modes.is_empty() {
        warnings.push(format!(
            "degraded modes: {}",
            status
                .contract
                .degraded_modes
                .iter()
                .map(|mode| serde_json::to_string(mode).unwrap_or_else(|_| "\"unknown\"".into()))
                .map(|mode| mode.trim_matches('"').to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    let stale_collections = stale_cache_collections(&status);
    if !stale_collections.is_empty() {
        warnings.push(format!(
            "stale cache collections: {}",
            stale_collections.join(", ")
        ));
    }

    if warnings.is_empty() {
        crate::health::mark_component_ok("context_book");
    } else {
        crate::health::mark_component_warn("context_book", warnings.join("; "));
    }
}

fn stale_cache_collections(status: &ContextBookStatusReport) -> Vec<String> {
    [
        ("agents", &status.cache_inventory.agents),
        ("contexts", &status.cache_inventory.contexts),
        ("votes", &status.cache_inventory.votes),
    ]
    .into_iter()
    .filter_map(|(name, summary)| {
        let updated_at = summary.updated_at.as_deref()?;
        let age = age_seconds(updated_at)?;
        (summary.count > 0 && age > HEALTH_STALE_SECONDS).then(|| format!("{name}({age}s)"))
    })
    .collect()
}

fn age_seconds(value: &str) -> Option<i64> {
    let parsed = DateTime::parse_from_rfc3339(value).ok()?;
    Some(
        Utc::now()
            .signed_duration_since(parsed.with_timezone(&Utc))
            .num_seconds(),
    )
}

async fn connect_and_sync_once(
    handle: &ContextBookHandle,
    shutdown: Option<&CancellationToken>,
) -> Result<bool> {
    let client = ContextBookClient::new(&handle.source_config());
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
    let inspection = client
        .inspect_runtime_contract(&session)
        .await
        .map_err(anyhow::Error::new)
        .context("failed to validate Context Book runtime contract")?;
    let disconnect_required = inspection
        .contract
        .degraded_modes
        .contains(&ContextBookDegradedMode::Disconnect);
    let validation_state = inspection.contract.validation_state;
    let auto_write_allowed = auto_subscription_writes_allowed(&inspection.contract.degraded_modes);
    let mut subscriptions = inspection.subscriptions;
    if auto_write_allowed
        && let Some(reconciled) =
            reconcile_auto_subscriptions(&client, &session, handle, Some(&subscriptions), None)
                .await?
    {
        subscriptions = reconciled;
    }
    handle.apply_contract_snapshot(inspection.contract);
    handle
        .store()
        .save_subscriptions(&subscriptions)
        .context("failed to persist contract-validated Context Book subscriptions")?;
    persist_runtime_state(handle)?;
    if disconnect_required {
        anyhow::bail!("Context Book runtime contract validation requires disconnect");
    }
    handle.mark_session_ready(
        &session.agent_id,
        if validation_state == ContextBookContractValidationState::Validated {
            "context_book session ready; contract validated and opening events stream"
        } else {
            "context_book session ready; opening events stream in degraded mode"
        },
    );
    persist_runtime_state(handle)?;

    let cursor = handle.snapshot().last_event_id;
    match client.open_event_stream(&session, cursor.as_deref()).await {
        Ok(response) => {
            handle.mark_stream_connected(&session.agent_id, "context_book SSE connected");
            persist_runtime_state(handle)?;
            consume_sse_stream(response, &client, &session, handle, shutdown).await?;
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
                poll_once(&client, &session, handle).await?;
                return Ok(true);
            }
            Ok(false)
        }
        Err(error) => {
            if handle.resolved_config().polling_fallback_enabled {
                handle.mark_retrying("context_book SSE unavailable; polling fallback active");
                persist_runtime_state(handle)?;
                poll_once(&client, &session, handle).await?;
                return Ok(true);
            }
            Err(anyhow::Error::new(error).context("failed to open Context Book SSE stream"))
        }
    }
}

async fn consume_sse_stream(
    response: reqwest::Response,
    client: &ContextBookClient,
    session: &super::client::ContextBookSession,
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
                                let update =
                                    build_event_sync_update(client, session, handle, &event)
                                    .await
                                    .with_context(|| {
                                        format!(
                                            "failed to prepare Context Book cache sync for event {}",
                                            event.event_id
                                        )
                                    })?;
                                handle.mark_event_applied(&event.event_id);
                                let snapshot = handle.snapshot();
                                if handle
                                    .store()
                                    .apply_event_sync(&event, &snapshot, update)
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
        let update = build_event_sync_update(client, session, handle, &event)
            .await
            .with_context(|| {
                format!(
                    "failed to prepare Context Book cache sync for polled event {}",
                    event.event_id
                )
            })?;
        handle.mark_event_applied(&event.event_id);
        let snapshot = handle.snapshot();
        if handle
            .store()
            .apply_event_sync(&event, &snapshot, update)
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

async fn build_event_sync_update(
    client: &ContextBookClient,
    session: &super::client::ContextBookSession,
    handle: &ContextBookHandle,
    event: &super::events::ContextBookEventEnvelope,
) -> Result<ContextBookEventSyncUpdate> {
    let mut update = ContextBookEventSyncUpdate::default();
    match event.event_type.as_str() {
        "subscription.updated" => {
            update.subscriptions = Some(
                client
                    .get_subscriptions(session)
                    .await
                    .map_err(anyhow::Error::new)
                    .context("failed to refresh subscriptions after subscription.updated")?,
            );
        }
        "agent.registered" | "agent.status.changed" | "agent.connection.changed" => {
            let agents = client
                .get_agents(session)
                .await
                .map_err(anyhow::Error::new)
                .context("failed to refresh agents after agent event")?;
            if auto_subscription_writes_allowed(&handle.contract_snapshot().degraded_modes) {
                update.subscriptions =
                    reconcile_auto_subscriptions(client, session, handle, None, Some(&agents))
                        .await?;
            }
            update.agents = Some(agents);
        }
        "agent.unregistered" => {
            let agents = client
                .get_agents(session)
                .await
                .map_err(anyhow::Error::new)
                .context("failed to refresh agents after agent.unregistered")?;
            if auto_subscription_writes_allowed(&handle.contract_snapshot().degraded_modes) {
                update.subscriptions =
                    reconcile_auto_subscriptions(client, session, handle, None, Some(&agents))
                        .await?;
            }
            update.agents = Some(agents);
        }
        "context.created" | "context.updated" | "context.deleted" => {
            update.contexts = Some(
                client
                    .get_contexts(session)
                    .await
                    .map_err(anyhow::Error::new)
                    .context("failed to refresh contexts after context event")?,
            );
        }
        "vote.created" | "vote.updated" | "vote.deleted" => {
            update.votes = Some(
                client
                    .get_votes(session)
                    .await
                    .map_err(anyhow::Error::new)
                    .context("failed to refresh votes after vote event")?,
            );
        }
        _ => {}
    }
    Ok(update)
}

fn seed_configured_subscriptions_if_missing(handle: &ContextBookHandle) -> Result<()> {
    if handle
        .store()
        .load_subscriptions()
        .context("failed to inspect cached subscriptions before seeding")?
        .is_some()
    {
        return Ok(());
    }

    let desired = configured_seed_agent_ids(&handle.resolved_config());
    if desired.is_empty() {
        return Ok(());
    }

    handle
        .store()
        .save_subscriptions(&ContextBookSubscriptionsSnapshot {
            consumer_agent_id: None,
            desired_producer_agent_ids: desired,
            effective_producer_agent_ids: Vec::new(),
            updated_at: Utc::now().to_rfc3339(),
        })
        .context("failed to seed configured Context Book subscriptions")
}

fn configured_seed_agent_ids(
    resolved: &crate::context_book::config::ResolvedContextBookConfig,
) -> Vec<String> {
    normalize_agent_ids(
        &resolved
            .subscription_seed
            .iter()
            .filter_map(|agent_id| {
                let trimmed = agent_id.trim();
                (!trimmed.is_empty() && trimmed != "*").then(|| trimmed.to_string())
            })
            .collect::<Vec<_>>(),
    )
}

fn auto_subscription_writes_allowed(degraded_modes: &[ContextBookDegradedMode]) -> bool {
    !degraded_modes.iter().any(|mode| {
        matches!(
            mode,
            ContextBookDegradedMode::ReadOnly
                | ContextBookDegradedMode::NoWrite
                | ContextBookDegradedMode::Disconnect
        )
    })
}

async fn reconcile_auto_subscriptions(
    client: &ContextBookClient,
    session: &super::client::ContextBookSession,
    handle: &ContextBookHandle,
    current: Option<&ContextBookSubscriptionsSnapshot>,
    agents: Option<&[ContextBookAgentSnapshot]>,
) -> Result<Option<ContextBookSubscriptionsSnapshot>> {
    if handle.resolved_config().subscription_mode != ContextBookSubscriptionMode::Auto {
        return Ok(None);
    }

    let desired = resolve_auto_subscription_targets(client, session, handle, agents).await?;
    let current = match current {
        Some(current) => current.clone(),
        None => match handle
            .store()
            .load_subscriptions()
            .context("failed to load cached subscriptions for auto reconcile")?
        {
            Some(cached) => cached,
            None => client
                .get_subscriptions(session)
                .await
                .map_err(anyhow::Error::new)
                .context("failed to load remote subscriptions for auto reconcile")?,
        },
    };

    if current.desired_producer_agent_ids == desired {
        return Ok(None);
    }

    let updated = client
        .set_subscriptions(session, &desired)
        .await
        .map_err(anyhow::Error::new)
        .context("failed to auto-reconcile Context Book subscriptions")?;
    Ok(Some(updated))
}

async fn resolve_auto_subscription_targets(
    client: &ContextBookClient,
    session: &super::client::ContextBookSession,
    handle: &ContextBookHandle,
    agents: Option<&[ContextBookAgentSnapshot]>,
) -> Result<Vec<String>> {
    let resolved = handle.resolved_config();
    let wildcard_requested = resolved
        .subscription_seed
        .iter()
        .any(|agent_id| agent_id.trim() == "*");
    let mut desired = configured_seed_agent_ids(&resolved);

    if wildcard_requested {
        let discovered_agents = match agents {
            Some(agents) => agents.to_vec(),
            None => client
                .get_agents(session)
                .await
                .map_err(anyhow::Error::new)
                .context("failed to load agents for auto subscription reconcile")?,
        };
        desired.extend(
            discovered_agents
                .into_iter()
                .map(|agent| agent.agent_id)
                .filter(|agent_id| agent_id != &session.agent_id),
        );
    }

    desired.retain(|agent_id| agent_id != &session.agent_id);
    Ok(normalize_agent_ids(&desired))
}

fn normalize_agent_ids(values: &[String]) -> Vec<String> {
    let mut ids = values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    ids
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
        body::Body,
        extract::Query,
        extract::State,
        http::{HeaderMap, StatusCode},
        response::IntoResponse,
        routing::{get, patch, post},
    };
    use chrono::{Duration as ChronoDuration, Utc};
    use std::collections::BTreeMap;
    use std::sync::{
        Arc, Mutex,
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

    #[test]
    fn format_error_chain_includes_nested_causes() {
        let error = anyhow::anyhow!("outer").context("middle").context("inner");
        assert_eq!(format_error_chain(&error), "inner: middle: outer");
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

        async fn events(
            State(state): State<WorkerAppState>,
            Query(query): Query<std::collections::HashMap<String, String>>,
        ) -> impl IntoResponse {
            if query.get("sinceEventId").map(String::as_str)
                == Some("__zeroclaw_contract_probe_cursor__")
            {
                return (
                    StatusCode::CONFLICT,
                    axum::Json(serde_json::json!({
                        "error": {
                            "code": "CURSOR_NOT_FOUND",
                            "message": "missing cursor"
                        }
                    })),
                )
                    .into_response();
            }
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
            .into_response()
        }

        async fn agents() -> impl IntoResponse {
            axum::Json(serde_json::json!([
                {
                    "agentId": "workspace",
                    "lifecycleState": "Active",
                    "connectionState": "Disconnected"
                }
            ]))
        }

        async fn subscriptions() -> impl IntoResponse {
            axum::Json(serde_json::json!({
                "consumerAgentId": "workspace",
                "desiredProducerAgentIds": ["peer-a"],
                "effectiveProducerAgentIds": ["peer-a"]
            }))
        }

        async fn contexts() -> impl IntoResponse {
            axum::Json(serde_json::json!({
                "items": [
                    {
                        "contextId": "ctx-100",
                        "authorAgentId": "peer-a",
                        "title": "Remote context",
                        "contents": "payload",
                        "tag": "ops",
                        "status": "Published",
                        "createdAt": "2026-03-29T00:00:00Z",
                        "updatedAt": "2026-03-29T00:00:00Z"
                    }
                ]
            }))
        }

        async fn legacy_refresh() -> impl IntoResponse {
            (
                StatusCode::BAD_REQUEST,
                axum::Json(serde_json::json!({
                    "error": {
                        "code": "REFRESH_TOKEN_REQUIRED",
                        "message": "missing refresh token"
                    }
                })),
            )
        }

        let app_state = WorkerAppState {
            poll_calls: Arc::new(AtomicUsize::new(0)),
        };
        let app = Router::new()
            .route("/agents/workspace/status", patch(patch_status))
            .route("/agents", get(agents))
            .route("/subscriptions", get(subscriptions))
            .route("/contexts", get(contexts))
            .route("/events/stream", get(events_stream))
            .route("/events", get(events))
            .route("/auth/refresh", post(legacy_refresh))
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
        let cached_contexts = handle
            .store()
            .load_contexts()
            .expect("load cached contexts")
            .expect("cached contexts");
        assert_eq!(cached_contexts.items.len(), 1);
        assert_eq!(cached_contexts.items[0].context_id, "ctx-100");
        let cached_subscriptions = handle
            .store()
            .load_subscriptions()
            .expect("load cached subscriptions")
            .expect("cached subscriptions");
        assert_eq!(
            cached_subscriptions.desired_producer_agent_ids,
            vec!["peer-a".to_string()]
        );
        assert_eq!(
            cached_subscriptions.effective_producer_agent_ids,
            vec!["peer-a".to_string()]
        );
        assert!(status.persisted_runtime.as_ref().is_some_and(|runtime| {
            runtime.worker_state == "stopped"
                && runtime.last_event_id.as_deref() == Some("evt-100")
                && runtime.cursor_generation == 1
        }));
        assert_eq!(status.cache_inventory.contexts.count, 1);

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn connect_and_sync_once_auto_mode_reconciles_wildcard_subscriptions() {
        #[derive(Clone)]
        struct AutoSubscriptionsState {
            agent_calls: Arc<AtomicUsize>,
            current_desired: Arc<Mutex<Vec<String>>>,
            put_requests: Arc<Mutex<Vec<Vec<String>>>>,
        }

        async fn patch_status() -> impl IntoResponse {
            axum::Json(serde_json::json!({"ok": true}))
        }

        async fn agents(State(state): State<AutoSubscriptionsState>) -> impl IntoResponse {
            let call_index = state.agent_calls.fetch_add(1, Ordering::SeqCst);
            let body = match call_index {
                0 => serde_json::json!([
                    {
                        "agentId": "workspace",
                        "lifecycleState": "Active",
                        "connectionState": "Disconnected"
                    }
                ]),
                1 => serde_json::json!([
                    {
                        "agentId": "workspace",
                        "lifecycleState": "Active",
                        "connectionState": "Disconnected"
                    },
                    {
                        "agentId": "peer-a",
                        "lifecycleState": "Active",
                        "connectionState": "Connected"
                    }
                ]),
                _ => serde_json::json!([
                    {
                        "agentId": "workspace",
                        "lifecycleState": "Active",
                        "connectionState": "Disconnected"
                    },
                    {
                        "agentId": "peer-a",
                        "lifecycleState": "Active",
                        "connectionState": "Connected"
                    },
                    {
                        "agentId": "peer-b",
                        "lifecycleState": "Active",
                        "connectionState": "Connected"
                    }
                ]),
            };
            axum::Json(body)
        }

        async fn get_subscriptions(
            State(state): State<AutoSubscriptionsState>,
        ) -> impl IntoResponse {
            let desired = state
                .current_desired
                .lock()
                .expect("current_desired mutex")
                .clone();
            axum::Json(serde_json::json!({
                "consumerAgentId": "workspace",
                "desiredProducerAgentIds": desired,
                "effectiveProducerAgentIds": desired,
            }))
        }

        async fn put_subscriptions(
            State(state): State<AutoSubscriptionsState>,
            axum::Json(body): axum::Json<serde_json::Value>,
        ) -> impl IntoResponse {
            let desired = body["desiredProducerAgentIds"]
                .as_array()
                .expect("desiredProducerAgentIds array")
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .expect("desiredProducerAgentIds string")
                        .to_string()
                })
                .collect::<Vec<_>>();
            assert_eq!(body["producerAgentIds"], body["desiredProducerAgentIds"]);
            state
                .put_requests
                .lock()
                .expect("put_requests mutex")
                .push(desired.clone());
            *state.current_desired.lock().expect("current_desired mutex") = desired.clone();
            axum::Json(serde_json::json!({
                "consumerAgentId": "workspace",
                "desiredProducerAgentIds": desired,
                "effectiveProducerAgentIds": desired,
            }))
        }

        async fn events_probe() -> impl IntoResponse {
            (
                StatusCode::CONFLICT,
                axum::Json(serde_json::json!({
                    "error": {
                        "code": "CURSOR_NOT_FOUND",
                        "message": "missing cursor"
                    }
                })),
            )
        }

        async fn delete_vote_probe() -> impl IntoResponse {
            (
                StatusCode::NOT_FOUND,
                axum::Json(serde_json::json!({
                    "error": {
                        "code": "VOTE_NOT_FOUND",
                        "message": "missing vote"
                    }
                })),
            )
        }

        async fn legacy_refresh() -> impl IntoResponse {
            (
                StatusCode::BAD_REQUEST,
                axum::Json(serde_json::json!({
                    "error": {
                        "code": "REFRESH_TOKEN_REQUIRED",
                        "message": "missing refresh token"
                    }
                })),
            )
        }

        async fn events_stream() -> impl IntoResponse {
            (
                StatusCode::OK,
                [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                concat!(
                    "id: evt-agent-1\n",
                    "event: agent.registered\n",
                    "data: {\"eventId\":\"evt-agent-1\",\"eventType\":\"agent.registered\",",
                    "\"occurredAt\":\"2026-03-31T00:00:01Z\",\"producerAgentId\":\"peer-b\",",
                    "\"entityId\":\"peer-b\",\"payload\":{},\"meta\":{}}\n\n"
                ),
            )
                .into_response()
        }

        let app_state = AutoSubscriptionsState {
            agent_calls: Arc::new(AtomicUsize::new(0)),
            current_desired: Arc::new(Mutex::new(Vec::new())),
            put_requests: Arc::new(Mutex::new(Vec::new())),
        };
        let app = Router::new()
            .route("/agents/workspace/status", patch(patch_status))
            .route("/agents", get(agents))
            .route(
                "/subscriptions",
                get(get_subscriptions).put(put_subscriptions),
            )
            .route("/events/stream", get(events_stream))
            .route("/events", get(events_probe))
            .route("/votes/{vote_id}", axum::routing::delete(delete_vote_probe))
            .route("/auth/refresh", post(legacy_refresh))
            .with_state(app_state.clone());
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve axum");
        });

        let tmp = TempDir::new().expect("temp dir");
        let mut config = test_config(&tmp, format!("http://{addr}"));
        config.context_book.subscription_mode = ContextBookSubscriptionMode::Auto;
        config.context_book.subscription_seed = vec!["*".into()];
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
        handle.store().initialize().expect("initialize store");

        let progressed = connect_and_sync_once(&handle, None)
            .await
            .expect("connect and sync once");

        assert!(progressed);
        let cached_subscriptions = handle
            .store()
            .load_subscriptions()
            .expect("load cached subscriptions")
            .expect("cached subscriptions");
        assert_eq!(
            cached_subscriptions.desired_producer_agent_ids,
            vec!["peer-a".to_string(), "peer-b".to_string()]
        );
        assert_eq!(
            cached_subscriptions.effective_producer_agent_ids,
            vec!["peer-a".to_string(), "peer-b".to_string()]
        );
        let put_requests = app_state
            .put_requests
            .lock()
            .expect("put_requests mutex")
            .clone();
        assert_eq!(
            put_requests,
            vec![
                vec!["peer-a".to_string()],
                vec!["peer-a".to_string(), "peer-b".to_string()]
            ]
        );
        let cached_agents = handle
            .store()
            .load_agents()
            .expect("load cached agents")
            .expect("cached agents");
        assert_eq!(cached_agents.items.len(), 3);
        assert_eq!(
            cached_agents
                .items
                .iter()
                .map(|agent| agent.agent_id.clone())
                .collect::<Vec<_>>(),
            vec![
                "peer-a".to_string(),
                "peer-b".to_string(),
                "workspace".to_string()
            ]
        );

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn worker_resumes_from_last_event_id_and_preserves_subscriptions_across_restart() {
        #[derive(Clone)]
        struct ResumeAppState {
            stream_calls: Arc<AtomicUsize>,
            seen_last_event_ids: Arc<Mutex<Vec<String>>>,
        }

        async fn patch_status() -> impl IntoResponse {
            axum::Json(serde_json::json!({"ok": true}))
        }

        async fn events_stream(
            State(state): State<ResumeAppState>,
            headers: HeaderMap,
        ) -> impl IntoResponse {
            let last_event_id = headers
                .get("last-event-id")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string();
            state
                .seen_last_event_ids
                .lock()
                .expect("last-event-id mutex")
                .push(last_event_id.clone());

            let body = match state.stream_calls.fetch_add(1, Ordering::SeqCst) {
                0 => {
                    assert_eq!(last_event_id, "evt-prev");
                    concat!(
                        "id: evt-next\n",
                        "event: context.created\n",
                        "data: {\"eventId\":\"evt-next\",\"eventType\":\"context.created\",",
                        "\"occurredAt\":\"2026-03-29T00:00:01Z\",\"producerAgentId\":\"peer-a\",",
                        "\"entityId\":\"ctx-101\",\"payload\":{},\"meta\":{}}\n\n"
                    )
                }
                1 => {
                    assert_eq!(last_event_id, "evt-next");
                    concat!(
                        "id: evt-next\n",
                        "event: context.created\n",
                        "data: {\"eventId\":\"evt-next\",\"eventType\":\"context.created\",",
                        "\"occurredAt\":\"2026-03-29T00:00:01Z\",\"producerAgentId\":\"peer-a\",",
                        "\"entityId\":\"ctx-101\",\"payload\":{},\"meta\":{}}\n\n",
                        "id: evt-next-2\n",
                        "event: context.created\n",
                        "data: {\"eventId\":\"evt-next-2\",\"eventType\":\"context.created\",",
                        "\"occurredAt\":\"2026-03-29T00:00:02Z\",\"producerAgentId\":\"peer-b\",",
                        "\"entityId\":\"ctx-202\",\"payload\":{},\"meta\":{}}\n\n"
                    )
                }
                _ => "",
            };

            (
                StatusCode::OK,
                [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                body,
            )
                .into_response()
        }

        async fn agents() -> impl IntoResponse {
            axum::Json(serde_json::json!([
                {
                    "agentId": "workspace",
                    "lifecycleState": "Active",
                    "connectionState": "Disconnected"
                }
            ]))
        }

        async fn subscriptions() -> impl IntoResponse {
            axum::Json(serde_json::json!({
                "consumerAgentId": "workspace",
                "desiredProducerAgentIds": ["peer-a", "peer-b"],
                "effectiveProducerAgentIds": ["peer-a"]
            }))
        }

        async fn contexts() -> impl IntoResponse {
            axum::Json(serde_json::json!({
                "items": [
                    {
                        "contextId": "ctx-101",
                        "authorAgentId": "peer-a",
                        "title": "Resume one",
                        "contents": "first replayed context",
                        "tag": "ops",
                        "status": "Published",
                        "createdAt": "2026-03-29T00:00:01Z",
                        "updatedAt": "2026-03-29T00:00:01Z"
                    },
                    {
                        "contextId": "ctx-202",
                        "authorAgentId": "peer-b",
                        "title": "Resume two",
                        "contents": "second replayed context",
                        "tag": "ops",
                        "status": "Published",
                        "createdAt": "2026-03-29T00:00:02Z",
                        "updatedAt": "2026-03-29T00:00:02Z"
                    }
                ]
            }))
        }

        async fn delete_vote_probe() -> impl IntoResponse {
            (
                StatusCode::NOT_FOUND,
                axum::Json(serde_json::json!({
                    "error": {
                        "code": "VOTE_NOT_FOUND",
                        "message": "missing vote"
                    }
                })),
            )
        }

        async fn legacy_refresh() -> impl IntoResponse {
            (
                StatusCode::BAD_REQUEST,
                axum::Json(serde_json::json!({
                    "error": {
                        "code": "REFRESH_TOKEN_REQUIRED",
                        "message": "missing refresh token"
                    }
                })),
            )
        }

        async fn events_probe() -> impl IntoResponse {
            (
                StatusCode::CONFLICT,
                axum::Json(serde_json::json!({
                    "error": {
                        "code": "CURSOR_NOT_FOUND",
                        "message": "missing cursor"
                    }
                })),
            )
        }

        let app_state = ResumeAppState {
            stream_calls: Arc::new(AtomicUsize::new(0)),
            seen_last_event_ids: Arc::new(Mutex::new(Vec::new())),
        };
        let app = Router::new()
            .route("/agents/workspace/status", patch(patch_status))
            .route("/agents", get(agents))
            .route("/subscriptions", get(subscriptions))
            .route("/contexts", get(contexts))
            .route("/events/stream", get(events_stream))
            .route("/events", get(events_probe))
            .route("/votes/{vote_id}", axum::routing::delete(delete_vote_probe))
            .route("/auth/refresh", post(legacy_refresh))
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
                        access_token: "resume-token".into(),
                        refresh_token: Some("resume-refresh".into()),
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
        handle
            .store()
            .save_subscriptions(
                &crate::context_book::store::ContextBookSubscriptionsSnapshot {
                    consumer_agent_id: Some("workspace".into()),
                    desired_producer_agent_ids: vec!["peer-a".into(), "peer-b".into()],
                    effective_producer_agent_ids: vec!["peer-a".into()],
                    updated_at: "2026-03-29T00:00:00Z".into(),
                },
            )
            .expect("seed persisted subscriptions");
        let mut snapshot = handle.snapshot();
        snapshot.agent_id = Some("workspace".into());
        snapshot.last_event_id = Some("evt-prev".into());
        snapshot.cursor_generation = 0;
        handle
            .store()
            .save_runtime_state(&snapshot)
            .expect("seed persisted runtime");

        let shutdown = CancellationToken::new();
        let worker = tokio::spawn(run(
            config.clone(),
            handle.clone(),
            Some(shutdown.child_token()),
        ));
        tokio::time::sleep(Duration::from_millis(200)).await;
        shutdown.cancel();
        worker
            .await
            .expect("worker join")
            .expect("worker should stop cleanly");

        let persisted_after_first = handle
            .store()
            .load_runtime_state()
            .expect("load runtime after first run")
            .expect("persisted runtime after first run");
        assert_eq!(
            persisted_after_first.last_event_id.as_deref(),
            Some("evt-next")
        );
        assert_eq!(
            handle.store().seen_event_count().expect("seen event count"),
            1
        );

        let mut restart_config = config.clone();
        restart_config.config_path = tmp.path().join("config-restart.toml");
        let restart_handle = crate::context_book::bootstrap(&restart_config).handle;
        let restart_shutdown = CancellationToken::new();
        let restarted_worker = tokio::spawn(run(
            restart_config,
            restart_handle.clone(),
            Some(restart_shutdown.child_token()),
        ));
        tokio::time::sleep(Duration::from_millis(200)).await;
        restart_shutdown.cancel();
        restarted_worker
            .await
            .expect("restarted worker join")
            .expect("restarted worker should stop cleanly");

        let persisted_after_restart = restart_handle
            .store()
            .load_runtime_state()
            .expect("load runtime after restart")
            .expect("persisted runtime after restart");
        assert_eq!(
            persisted_after_restart.last_event_id.as_deref(),
            Some("evt-next-2")
        );
        assert_eq!(
            restart_handle
                .store()
                .seen_event_count()
                .expect("seen event count after restart"),
            2
        );
        let persisted_subscriptions = restart_handle
            .store()
            .load_subscriptions()
            .expect("load subscriptions after restart")
            .expect("persisted subscriptions after restart");
        assert_eq!(
            persisted_subscriptions.desired_producer_agent_ids,
            vec!["peer-a".to_string(), "peer-b".to_string()]
        );
        assert_eq!(
            persisted_subscriptions.effective_producer_agent_ids,
            vec!["peer-a".to_string()]
        );
        let seen_last_event_ids = app_state
            .seen_last_event_ids
            .lock()
            .expect("last-event-id mutex")
            .clone();
        assert_eq!(
            seen_last_event_ids,
            vec!["evt-prev".to_string(), "evt-next".to_string()]
        );
        let cached_contexts = restart_handle
            .store()
            .load_contexts()
            .expect("load cached contexts")
            .expect("cached contexts after restart");
        assert_eq!(cached_contexts.items.len(), 2);

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn worker_error_state_clears_active_connection_status() {
        async fn patch_status() -> impl IntoResponse {
            axum::Json(serde_json::json!({"ok": true}))
        }

        async fn agents() -> impl IntoResponse {
            axum::Json(serde_json::json!([
                {
                    "agentId": "workspace",
                    "lifecycleState": "Active",
                    "connectionState": "Disconnected"
                }
            ]))
        }

        async fn subscriptions() -> impl IntoResponse {
            axum::Json(serde_json::json!({
                "consumerAgentId": "workspace",
                "desiredProducerAgentIds": ["peer-a"],
                "effectiveProducerAgentIds": ["peer-a"]
            }))
        }

        async fn delete_vote_probe() -> impl IntoResponse {
            (
                StatusCode::NOT_FOUND,
                axum::Json(serde_json::json!({
                    "error": {
                        "code": "VOTE_NOT_FOUND",
                        "message": "missing vote"
                    }
                })),
            )
        }

        async fn legacy_refresh() -> impl IntoResponse {
            (
                StatusCode::BAD_REQUEST,
                axum::Json(serde_json::json!({
                    "error": {
                        "code": "REFRESH_TOKEN_REQUIRED",
                        "message": "missing refresh token"
                    }
                })),
            )
        }

        async fn events_probe() -> impl IntoResponse {
            (
                StatusCode::CONFLICT,
                axum::Json(serde_json::json!({
                    "error": {
                        "code": "CURSOR_NOT_FOUND",
                        "message": "missing cursor"
                    }
                })),
            )
        }

        async fn events_stream() -> impl IntoResponse {
            (
                StatusCode::OK,
                [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                Body::from_stream(futures_util::stream::iter(vec![Err::<
                    axum::body::Bytes,
                    std::io::Error,
                >(
                    std::io::Error::new(std::io::ErrorKind::ConnectionReset, "boom"),
                )])),
            )
                .into_response()
        }

        let app_state = WorkerAppState {
            poll_calls: Arc::new(AtomicUsize::new(0)),
        };
        let app = Router::new()
            .route("/agents/workspace/status", patch(patch_status))
            .route("/agents", get(agents))
            .route("/subscriptions", get(subscriptions))
            .route("/events/stream", get(events_stream))
            .route("/events", get(events_probe))
            .route("/votes/{vote_id}", axum::routing::delete(delete_vote_probe))
            .route("/auth/refresh", post(legacy_refresh))
            .with_state(app_state);
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
                        access_token: "error-token".into(),
                        refresh_token: Some("error-refresh".into()),
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
        let shutdown = CancellationToken::new();
        let worker = tokio::spawn(run(config, handle.clone(), Some(shutdown.child_token())));

        tokio::time::sleep(Duration::from_millis(200)).await;
        shutdown.cancel();
        let result = worker.await.expect("worker join");

        assert!(result.is_ok());
        let status = handle.status_report();
        assert_eq!(status.runtime.worker_state, "stopped");
        let persisted_runtime = status
            .persisted_runtime
            .expect("persisted runtime after stream error");
        assert_eq!(persisted_runtime.lifecycle_state, "inactive");
        assert_eq!(persisted_runtime.connection_state, "disconnected");
        assert!(
            persisted_runtime
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("failed to read Context Book SSE chunk"))
        );

        server.abort();
        let _ = server.await;
    }
}
