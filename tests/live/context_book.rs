use chrono::Utc;
use serde_json::{Value, json};
use std::sync::OnceLock;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::Mutex;
use zeroclaw::Config;
use zeroclaw::context_book::{ContextBookClient, ContextBookRefreshMode};

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

fn live_config(tmp: &TempDir, base_url: &str) -> Config {
    let mut config = Config {
        workspace_dir: tmp.path().join("workspace"),
        config_path: tmp.path().join("config.toml"),
        ..Config::default()
    };
    let parsed = reqwest::Url::parse(base_url).expect("live base URL should parse");
    let host = parsed
        .host_str()
        .expect("live base URL should include a host")
        .to_string();

    config.context_book.enabled = true;
    config.context_book.discovery_enabled = false;
    config.context_book.manual_url = Some(base_url.to_string());
    config.context_book.allowed_hosts = vec![host];
    config.context_book.allow_private_hosts = true;
    config.context_book.agent_identity_override.agent_id = Some(format!(
        "zeroclaw-live-target-{}",
        Utc::now()
            .timestamp_nanos_opt()
            .expect("timestamp should fit")
    ));
    config.context_book.agent_identity_override.device_type = Some("notepc".to_string());
    config.context_book.agent_identity_override.display_name =
        Some("ZeroClaw Live Component Test".to_string());
    config
}

#[tokio::test]
#[ignore = "requires a live Context Book server with dashboard approval access"]
async fn context_book_live_bootstrap_and_contract_validation() {
    let _env_guard = env_lock().lock().await;
    let shared_secret = std::env::var("CONTEXT_BOOK_BOOTSTRAP_SHARED_SECRET")
        .expect("set CONTEXT_BOOK_BOOTSTRAP_SHARED_SECRET to run the live Context Book test");
    let base_url = std::env::var("CONTEXT_BOOK_LIVE_BASE_URL")
        .unwrap_or_else(|_| "http://127.0.1.1:8080".to_string());

    let tmp = TempDir::new().expect("temp dir");
    let config = live_config(&tmp, &base_url);
    let approval_http = reqwest::Client::new();
    let approval_base = base_url.clone();
    let requested_agent_name = config
        .context_book
        .agent_identity_override
        .agent_id
        .clone()
        .expect("agent id override");
    let _secret_guard = EnvGuard::set(
        &config.context_book.bootstrap_secret_env_key,
        Some(shared_secret.as_str()),
    );

    let approval_task = tokio::spawn(async move {
        for _ in 0..40 {
            let response = approval_http
                .get(format!("{approval_base}/dashboard/api/bootstrap/requests"))
                .send()
                .await
                .expect("bootstrap queue request");
            let body = response
                .json::<Value>()
                .await
                .expect("bootstrap queue body");
            if let Some(request_id) = body["items"].as_array().and_then(|items| {
                items.iter().find_map(|item| {
                    (item["requestedAgentName"].as_str() == Some(requested_agent_name.as_str()))
                        .then(|| item["requestId"].as_str())
                        .flatten()
                })
            }) {
                let response = approval_http
                    .post(format!(
                        "{approval_base}/dashboard/api/bootstrap/requests/{request_id}/approve"
                    ))
                    .json(&json!({
                        "actor": "zeroclaw-live-test",
                        "reason": "component live Context Book verification",
                        "channel": "dashboard"
                    }))
                    .send()
                    .await
                    .expect("bootstrap approval request");
                assert!(
                    response.status().is_success(),
                    "bootstrap approval failed with status {}",
                    response.status()
                );
                return;
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        panic!("timed out waiting for bootstrap request for {requested_agent_name}");
    });

    let client = ContextBookClient::new(&config);
    let session = tokio::time::timeout(Duration::from_secs(30), client.ensure_session())
        .await
        .expect("ensure_session should finish before timeout")
        .expect("live bootstrap session");
    approval_task
        .await
        .expect("bootstrap approval task should finish cleanly");

    assert!(!session.access_token.is_empty());
    assert_eq!(
        session.base_url.as_str(),
        format!("{}/", base_url.trim_end_matches('/'))
    );

    client
        .activate_agent(&session)
        .await
        .expect("activate live agent");
    client
        .open_event_stream(&session, None)
        .await
        .expect("open live event stream");
    let contract = client
        .validate_runtime_contract(&session)
        .await
        .expect("validate live runtime contract");

    assert_eq!(contract.lifecycle_connection_split, Some(true));
    assert_eq!(contract.subscriptions_desired_effective_split, Some(true));
    assert_eq!(contract.cursor_not_found_returns_409, Some(true));
    assert_ne!(contract.refresh_mode, ContextBookRefreshMode::Unknown);

    let agents = client.get_agents(&session).await.expect("GET /agents");
    let contexts = client.get_contexts(&session).await.expect("GET /contexts");
    let votes = client.get_votes(&session).await.expect("GET /votes");

    assert!(
        agents
            .iter()
            .any(|agent| agent.agent_id == session.agent_id)
    );
    assert!(
        contexts
            .iter()
            .all(|context| !context.context_id.trim().is_empty())
    );
    assert!(votes.iter().all(|vote| !vote.vote_id.trim().is_empty()));
}
