use anyhow::Result;
use axum::{
    Json, Router,
    extract::Path as AxumPath,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{delete, get, patch, post},
};
use serde_json::{Value, json};
use std::fs;
use std::path::Path;
use std::sync::{Arc, OnceLock};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use zeroclaw::Config;
use zeroclaw::context_book::{
    ContextBookClient, ContextBookContextCreateRequest, ContextBookContractSnapshot,
    ContextBookContractValidationState, ContextBookDegradedMode, ContextBookRefreshMode,
    ContextBookService, bootstrap,
};

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[derive(Clone)]
struct EnvGuard {
    key: String,
    previous: Option<String>,
}

impl EnvGuard {
    fn set(key: &str, value: Option<&str>) -> Self {
        let previous = std::env::var(key).ok();
        match value {
            Some(value) => unsafe { std::env::set_var(key, value) },
            None => unsafe { std::env::remove_var(key) },
        }
        Self {
            key: key.to_string(),
            previous,
        }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match self.previous.as_deref() {
            Some(value) => unsafe { std::env::set_var(&self.key, value) },
            None => unsafe { std::env::remove_var(&self.key) },
        }
    }
}

fn test_config(tmp: &TempDir) -> Config {
    let mut config = Config {
        workspace_dir: tmp.path().join("workspace"),
        config_path: tmp.path().join("config.toml"),
        ..Config::default()
    };
    config.context_book.enabled = true;
    config.context_book.allow_private_hosts = true;
    config
}

fn write_fake_avahi_browse(bin_dir: &Path, port: u16) -> Result<()> {
    let script = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
if [[ "${{1:-}}" != "-rtp" ]]; then
  echo "unexpected args" >&2
  exit 2
fi
printf '=;lo;IPv4;context-book-test;_contextbook._tcp;local;context-book-test.local;127.0.0.1;{port};"service=context-book" "ver=1" "api=rest,sse" "path=/" "bootstrap=trusted-network+shared-secret"\n'
"#
    );
    let path = bin_dir.join("avahi-browse");
    fs::write(&path, script)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&path)?.permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&path, permissions)?;
    }
    Ok(())
}

#[tokio::test]
async fn context_book_discovery_bootstraps_against_fake_avahi_service() {
    async fn preflight() -> impl IntoResponse {
        Json(json!({
            "service": "context-book",
            "status": "ok"
        }))
    }

    async fn connect(headers: HeaderMap, Json(body): Json<Value>) -> impl IntoResponse {
        assert_eq!(
            headers
                .get("x-context-book-bootstrap-secret")
                .and_then(|value| value.to_str().ok()),
            Some("bootstrap-secret")
        );
        assert_eq!(body["agentId"], "workspace");
        Json(json!({
            "agent": { "agentId": "workspace-discovered" },
            "access_token": "discovered-access",
            "refresh_token": "discovered-refresh",
            "expires_in": 1800
        }))
    }

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("local addr");
    let server = tokio::spawn(async move {
        let app = Router::new()
            .route("/", get(preflight))
            .route("/agents/connect", post(connect));
        axum::serve(listener, app).await.expect("serve axum");
    });

    let _env_guard = env_lock().lock().await;
    let tmp = TempDir::new().expect("temp dir");
    let bin_dir = tmp.path().join("bin");
    fs::create_dir_all(&bin_dir).expect("create bin dir");
    write_fake_avahi_browse(&bin_dir, addr.port()).expect("write fake avahi-browse");

    let original_path = std::env::var("PATH").unwrap_or_default();
    let merged_path = format!("{}:{original_path}", bin_dir.display());
    let _path_guard = EnvGuard::set("PATH", Some(&merged_path));

    let mut config = test_config(&tmp);
    config.context_book.manual_url = None;
    config.context_book.discovery_enabled = true;
    config.context_book.service_type = "_contextbook._tcp.local.".to_string();
    config.context_book.allowed_hosts = vec!["127.0.0.1".into()];
    let _secret_guard = EnvGuard::set(
        &config.context_book.bootstrap_secret_env_key,
        Some("bootstrap-secret"),
    );

    let client = ContextBookClient::new(&config);
    let session = client.ensure_session().await.expect("discovery bootstrap");

    assert_eq!(session.agent_id, "workspace-discovered");
    assert_eq!(session.access_token, "discovered-access");
    assert_eq!(
        session.base_url.as_str(),
        &format!("http://127.0.0.1:{}/", addr.port())
    );

    server.abort();
    let _ = server.await;
}

#[test]
fn context_book_bootstrap_refreshes_contract_snapshot_on_reload() {
    let tmp = TempDir::new().expect("temp dir");
    let mut config = test_config(&tmp);
    config.context_book.discovery_enabled = false;
    config.context_book.manual_url = Some("http://127.0.0.1:8080".into());
    config.context_book.allowed_hosts = vec!["127.0.0.1".into()];

    let first = bootstrap(&config).handle;
    first.apply_contract_snapshot(ContextBookContractSnapshot {
        validation_state: ContextBookContractValidationState::Validated,
        checked_at: Some("2026-03-30T00:00:00Z".into()),
        lifecycle_connection_split: Some(true),
        subscriptions_desired_effective_split: Some(true),
        cursor_not_found_returns_409: Some(true),
        vote_deleted_supported: Some(true),
        refresh_mode: ContextBookRefreshMode::LegacyAuthRefresh,
        degraded_modes: Vec::new(),
        notes: Vec::new(),
    });

    config.context_book.manual_url = Some("http://127.0.0.1:8181".into());
    let refreshed = bootstrap(&config).handle;

    assert!(Arc::ptr_eq(&first, &refreshed));
    assert_eq!(
        refreshed.resolved_config().manual_url.as_deref(),
        Some("http://127.0.0.1:8181")
    );
    assert_eq!(
        refreshed.contract_snapshot().validation_state,
        ContextBookContractValidationState::Unknown
    );
}

#[tokio::test]
async fn context_book_config_reload_and_auth_rotation_revalidate_before_writes() {
    async fn stale_connect(headers: HeaderMap) -> impl IntoResponse {
        assert_eq!(
            headers
                .get("x-context-book-bootstrap-secret")
                .and_then(|value| value.to_str().ok()),
            Some("bootstrap-secret")
        );
        Json(json!({
            "agent": { "agentId": "workspace-old" },
            "access_token": "primary-token",
            "refresh_token": "primary-refresh",
            "expires_in": 1800
        }))
    }

    async fn stale_activate(AxumPath(agent_id): AxumPath<String>) -> impl IntoResponse {
        assert_eq!(agent_id, "workspace-old");
        StatusCode::NO_CONTENT
    }

    async fn stale_agents() -> impl IntoResponse {
        Json(json!([
            {
                "agentId": "workspace-old",
                "lifecycleState": "Active",
                "connectionState": "Disconnected"
            }
        ]))
    }

    async fn stale_subscriptions() -> impl IntoResponse {
        Json(json!({
            "consumerAgentId": "workspace-old",
            "desiredProducerAgentIds": ["peer-a"]
        }))
    }

    async fn events_probe() -> impl IntoResponse {
        (
            StatusCode::CONFLICT,
            Json(json!({
                "error": {
                    "code": "CURSOR_NOT_FOUND",
                    "message": "missing cursor"
                }
            })),
        )
    }

    async fn legacy_refresh() -> impl IntoResponse {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": {
                    "code": "REFRESH_TOKEN_REQUIRED",
                    "message": "missing refresh token"
                }
            })),
        )
    }

    let stale_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stale listener");
    let stale_addr = stale_listener.local_addr().expect("stale addr");
    let stale_server = tokio::spawn(async move {
        let stale_app = Router::new()
            .route("/agents/connect", post(stale_connect))
            .route("/agents/{agent_id}/status", patch(stale_activate))
            .route("/agents", get(stale_agents))
            .route("/subscriptions", get(stale_subscriptions))
            .route("/events", get(events_probe))
            .route("/auth/refresh", post(legacy_refresh));
        axum::serve(stale_listener, stale_app)
            .await
            .expect("serve stale app");
    });

    async fn fresh_connect(headers: HeaderMap) -> impl IntoResponse {
        assert_eq!(
            headers
                .get("x-context-book-bootstrap-secret")
                .and_then(|value| value.to_str().ok()),
            Some("bootstrap-secret")
        );
        Json(json!({
            "agent": { "agentId": "workspace-new" },
            "access_token": "rotated-token",
            "refresh_token": "rotated-refresh",
            "expires_in": 1800
        }))
    }

    async fn fresh_activate(AxumPath(agent_id): AxumPath<String>) -> impl IntoResponse {
        assert_eq!(agent_id, "workspace-new");
        StatusCode::NO_CONTENT
    }

    async fn fresh_agents() -> impl IntoResponse {
        Json(json!([
            {
                "agentId": "workspace-new",
                "lifecycleState": "Active",
                "connectionState": "Disconnected"
            }
        ]))
    }

    async fn fresh_subscriptions() -> impl IntoResponse {
        Json(json!({
            "consumerAgentId": "workspace-new",
            "desiredProducerAgentIds": ["peer-a"],
            "effectiveProducerAgentIds": ["peer-a"]
        }))
    }

    async fn fresh_delete_vote_probe(AxumPath(vote_id): AxumPath<String>) -> impl IntoResponse {
        assert_eq!(vote_id, "__zeroclaw_contract_probe_vote__");
        (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": {
                    "code": "VOTE_NOT_FOUND",
                    "message": "missing vote"
                }
            })),
        )
    }

    async fn fresh_create_context(Json(body): Json<Value>) -> impl IntoResponse {
        assert_eq!(body["contextId"], "workspace-new_ctx");
        Json(json!({
            "contextId": "workspace-new_ctx",
            "authorAgentId": "workspace-new",
            "title": "Rotated",
            "contents": "write after reload",
            "tag": "ops",
            "status": "Published",
            "createdAt": "2026-03-30T00:00:00Z",
            "updatedAt": "2026-03-30T00:00:00Z"
        }))
    }

    let fresh_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fresh listener");
    let fresh_addr = fresh_listener.local_addr().expect("fresh addr");
    let fresh_server = tokio::spawn(async move {
        let fresh_app = Router::new()
            .route("/agents/connect", post(fresh_connect))
            .route("/agents/{agent_id}/status", patch(fresh_activate))
            .route("/agents", get(fresh_agents))
            .route("/subscriptions", get(fresh_subscriptions))
            .route("/events", get(events_probe))
            .route("/votes/{vote_id}", delete(fresh_delete_vote_probe))
            .route("/contexts", post(fresh_create_context))
            .route("/auth/refresh", post(legacy_refresh));
        axum::serve(fresh_listener, fresh_app)
            .await
            .expect("serve fresh app");
    });

    let tmp = TempDir::new().expect("temp dir");
    let mut config = test_config(&tmp);
    config.context_book.discovery_enabled = false;
    config.context_book.manual_url = Some(format!("http://{stale_addr}"));
    config.context_book.allowed_hosts = vec!["127.0.0.1".into()];
    config.context_book.auth_profile = Some("primary".into());
    let _secret_guard = EnvGuard::set(
        &config.context_book.bootstrap_secret_env_key,
        Some("bootstrap-secret"),
    );

    let handle = bootstrap(&config).handle;
    let service = ContextBookService::new(handle.clone());
    let stale_error = service
        .create_context(&ContextBookContextCreateRequest {
            context_id: Some("workspace-old_ctx".into()),
            title: "Should Fail".into(),
            contents: "blocked by stale contract".into(),
            tag: "ops".into(),
            status: "Published".into(),
        })
        .await
        .expect_err("stale no-write contract should block writes");
    assert!(stale_error.to_string().contains("no-write/read-only"));
    assert!(
        handle
            .contract_snapshot()
            .degraded_modes
            .contains(&ContextBookDegradedMode::NoWrite)
    );

    let mut reloaded = config.clone();
    reloaded.context_book.manual_url = Some(format!("http://{fresh_addr}"));
    reloaded.context_book.auth_profile = Some("rotated".into());
    let refreshed = bootstrap(&reloaded).handle;

    assert!(Arc::ptr_eq(&handle, &refreshed));
    assert_eq!(
        refreshed.contract_snapshot().validation_state,
        ContextBookContractValidationState::Unknown
    );

    let reloaded_service = ContextBookService::new(refreshed.clone());
    let created = reloaded_service
        .create_context(&ContextBookContextCreateRequest {
            context_id: Some("workspace-new_ctx".into()),
            title: "Rotated".into(),
            contents: "write after reload".into(),
            tag: "ops".into(),
            status: "Published".into(),
        })
        .await
        .expect("write should succeed after reload");

    assert_eq!(created.author_agent_id, "workspace-new");
    assert_eq!(
        refreshed.contract_snapshot().validation_state,
        ContextBookContractValidationState::Validated
    );
    assert!(refreshed.contract_snapshot().degraded_modes.is_empty());

    stale_server.abort();
    let _ = stale_server.await;
    fresh_server.abort();
    let _ = fresh_server.await;
}
