use crate::config::ContextBookConfig;
use crate::context_book::client::ContextBookClient;
use crate::context_book::store::ContextBookStore;
use crate::context_book::types::{
    AgentLifecycleState, AgentRecordDto, AgentStatusUpdateRequest, ContextCreateRequest,
    ContextRecordDto, ContextUpdateRequest, VoteCreateRequest, VoteRecordDto,
};
use anyhow::{anyhow, Context, Result};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextBookServiceMode {
    Disabled,
    EnabledPlaceholder,
    EnabledRuntime,
}

#[derive(Debug, Clone)]
pub struct ContextBookService {
    inner: Arc<ContextBookServiceState>,
}

#[derive(Debug)]
struct ContextBookServiceState {
    config: ContextBookConfig,
    mode: ContextBookServiceMode,
    client: Option<ContextBookClient>,
    store: Option<Arc<ContextBookStore>>,
}

impl ContextBookService {
    pub fn from_config(config: ContextBookConfig) -> Self {
        if config.enabled {
            Self::enabled_placeholder(config)
        } else {
            Self::disabled(config)
        }
    }

    pub fn with_store(config: ContextBookConfig, store: Arc<ContextBookStore>) -> Result<Self> {
        if !config.enabled {
            return Ok(Self::disabled(config));
        }

        let client = ContextBookClient::new(&config)
            .context("failed to build Context Book service client")?;

        Ok(Self {
            inner: Arc::new(ContextBookServiceState {
                config,
                mode: ContextBookServiceMode::EnabledRuntime,
                client: Some(client),
                store: Some(store),
            }),
        })
    }

    pub fn disabled(config: ContextBookConfig) -> Self {
        Self {
            inner: Arc::new(ContextBookServiceState {
                config,
                mode: ContextBookServiceMode::Disabled,
                client: None,
                store: None,
            }),
        }
    }

    pub fn enabled_placeholder(config: ContextBookConfig) -> Self {
        Self {
            inner: Arc::new(ContextBookServiceState {
                config,
                mode: ContextBookServiceMode::EnabledPlaceholder,
                client: None,
                store: None,
            }),
        }
    }

    pub fn mode(&self) -> ContextBookServiceMode {
        self.inner.mode
    }

    pub fn is_enabled(&self) -> bool {
        self.mode() != ContextBookServiceMode::Disabled
    }

    pub fn is_operational(&self) -> bool {
        self.mode() == ContextBookServiceMode::EnabledRuntime
    }

    pub fn config(&self) -> &ContextBookConfig {
        &self.inner.config
    }

    pub async fn set_local_status(&self, status: AgentLifecycleState) -> Result<AgentRecordDto> {
        let (client, store) = self.operational_parts()?;
        let session = store
            .load_auth_session()?
            .ok_or_else(|| anyhow!("Context Book auth session is not available"))?;
        let local_agent_id = store
            .load_local_identity()?
            .map(|identity| identity.agent_id)
            .unwrap_or_else(|| session.agent_id.clone());

        let agent = client
            .update_agent_status(
                &session.access_token,
                &local_agent_id,
                &AgentStatusUpdateRequest { status },
            )
            .await
            .with_context(|| {
                format!("failed to update Context Book agent status for {local_agent_id}")
            })?;

        store
            .upsert_mirrored_agent(&agent)
            .context("failed to persist Context Book status update result")?;

        Ok(agent)
    }

    pub async fn create_local_context(
        &self,
        request: &ContextCreateRequest,
    ) -> Result<ContextRecordDto> {
        let (client, store) = self.operational_parts()?;
        let session = store
            .load_auth_session()?
            .ok_or_else(|| anyhow!("Context Book auth session is not available"))?;

        let context = client
            .create_context(&session.access_token, request)
            .await
            .context("failed to create Context Book context")?;

        store
            .upsert_mirrored_context(&context)
            .context("failed to persist Context Book context create result")?;

        Ok(context)
    }

    pub async fn delete_local_context(&self, context_id: &str) -> Result<()> {
        let (client, store) = self.operational_parts()?;
        let session = store
            .load_auth_session()?
            .ok_or_else(|| anyhow!("Context Book auth session is not available"))?;

        client
            .delete_context(&session.access_token, context_id)
            .await
            .with_context(|| format!("failed to delete Context Book context {context_id}"))?;

        store
            .delete_mirrored_context(context_id)
            .context("failed to persist Context Book context delete result")?;

        Ok(())
    }

    pub async fn update_local_context(
        &self,
        context_id: &str,
        request: &ContextUpdateRequest,
    ) -> Result<ContextRecordDto> {
        let (client, store) = self.operational_parts()?;
        let session = store
            .load_auth_session()?
            .ok_or_else(|| anyhow!("Context Book auth session is not available"))?;

        let context = client
            .update_context(&session.access_token, context_id, request)
            .await
            .with_context(|| format!("failed to update Context Book context {context_id}"))?;

        store
            .upsert_mirrored_context(&context)
            .context("failed to persist Context Book context update result")?;

        Ok(context)
    }

    pub async fn create_local_vote(&self, request: &VoteCreateRequest) -> Result<VoteRecordDto> {
        let (client, store) = self.operational_parts()?;
        let session = store
            .load_auth_session()?
            .ok_or_else(|| anyhow!("Context Book auth session is not available"))?;

        let vote = client
            .create_vote(&session.access_token, request)
            .await
            .context("failed to create Context Book vote")?;

        store
            .upsert_mirrored_vote(&vote, "rest")
            .context("failed to persist Context Book vote create result")?;

        Ok(vote)
    }

    fn operational_parts(&self) -> Result<(&ContextBookClient, &Arc<ContextBookStore>)> {
        if !self.config().enabled {
            return Err(anyhow!("Context Book is disabled in config"));
        }

        let client = self.inner.client.as_ref().ok_or_else(|| {
            anyhow!("Context Book service is enabled in config but not wired for runtime actions")
        })?;
        let store = self.inner.store.as_ref().ok_or_else(|| {
            anyhow!("Context Book service is enabled in config but no dedicated store is attached")
        })?;

        Ok((client, store))
    }
}

impl Default for ContextBookService {
    fn default() -> Self {
        Self::disabled(ContextBookConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context_book::store::LocalIdentityRecord;
    use crate::context_book::types::{AuthSessionDto, TransportConnectionState, VoteCreateRequest};
    use tempfile::TempDir;
    use wiremock::matchers::{body_partial_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn temp_store() -> (TempDir, Arc<ContextBookStore>) {
        let tmp = TempDir::new().expect("temp dir");
        let db_path = tmp
            .path()
            .join("state")
            .join("context_book")
            .join("state.db");
        let store = Arc::new(ContextBookStore::open_at(&db_path).expect("open store"));
        (tmp, store)
    }

    #[test]
    fn default_service_is_disabled_noop() {
        let service = ContextBookService::default();

        assert_eq!(service.mode(), ContextBookServiceMode::Disabled);
        assert!(!service.is_enabled());
        assert!(!service.is_operational());
    }

    #[test]
    fn enabled_placeholder_service_tracks_enabled_config() {
        let mut config = ContextBookConfig::default();
        config.enabled = true;

        let service = ContextBookService::from_config(config.clone());

        assert_eq!(service.mode(), ContextBookServiceMode::EnabledPlaceholder);
        assert!(service.is_enabled());
        assert!(!service.is_operational());
        assert_eq!(service.config().agent_id, config.agent_id);
    }

    #[tokio::test]
    async fn runtime_service_updates_status_and_persists_mirror() {
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/agents/agent-1/status"))
            .and(header("authorization", "Bearer access-token"))
            .and(body_partial_json(serde_json::json!({
                "status": "Active"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "agentId": "agent-1",
                "deviceType": "notepc",
                "displayName": "ZeroClaw Main",
                "lifecycleState": "Active",
                "connectionState": "Connected",
                "createdAt": "2026-04-03T00:00:00Z",
                "updatedAt": "2026-04-03T00:00:10Z",
                "lastSeenAt": "2026-04-03T00:00:10Z"
            })))
            .mount(&server)
            .await;

        let (_tmp, store) = temp_store();
        store
            .save_local_identity(&LocalIdentityRecord {
                agent_id: "agent-1".into(),
                device_type: "notepc".into(),
                display_name: "ZeroClaw Main".into(),
                bootstrap_approved: true,
                last_bootstrap_request_id: Some("req-1".into()),
                last_bootstrap_approval_state: None,
                last_bootstrap_completed_at: Some("2026-04-03T00:00:00Z".into()),
                updated_at: "2026-04-03T00:00:00Z".into(),
            })
            .expect("save identity");
        store
            .save_auth_session(
                &AuthSessionDto {
                    agent_id: "agent-1".into(),
                    access_token: "access-token".into(),
                    refresh_token: "refresh-token".into(),
                    access_token_expires_at: "2026-04-04T00:00:00Z".into(),
                },
                "2026-04-03T00:00:00Z",
            )
            .expect("save session");

        let mut config = ContextBookConfig::default();
        config.enabled = true;
        config.base_url = server.uri();

        let service = ContextBookService::with_store(config, store.clone()).expect("service");
        let updated = service
            .set_local_status(AgentLifecycleState::Active)
            .await
            .expect("set status");

        assert_eq!(service.mode(), ContextBookServiceMode::EnabledRuntime);
        assert!(service.is_operational());
        assert_eq!(updated.agent_id, "agent-1");
        assert_eq!(updated.lifecycle_state, AgentLifecycleState::Active);

        let mirrored = store.list_mirrored_agents().expect("list mirrored agents");
        assert_eq!(mirrored.len(), 1);
        assert_eq!(mirrored[0].lifecycle_state, AgentLifecycleState::Active);
        assert_eq!(
            mirrored[0].connection_state,
            TransportConnectionState::Connected
        );
    }

    #[tokio::test]
    async fn runtime_service_requires_saved_auth_session() {
        let (_tmp, store) = temp_store();
        let mut config = ContextBookConfig::default();
        config.enabled = true;

        let service = ContextBookService::with_store(config, store).expect("service");
        let error = service
            .set_local_status(AgentLifecycleState::Active)
            .await
            .expect_err("missing session should fail");

        assert!(error.to_string().contains("auth session"));
    }

    #[tokio::test]
    async fn runtime_service_creates_context_and_persists_mirror() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/contexts"))
            .and(header("authorization", "Bearer access-token"))
            .and(body_partial_json(serde_json::json!({
                "title": "Daily Summary",
                "contents": "Agent heartbeat summary",
                "tag": "ops",
                "status": "Published"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "contextId": "ctx-1",
                "authorAgentId": "agent-1",
                "title": "Daily Summary",
                "contents": "Agent heartbeat summary",
                "tag": "ops",
                "status": "Published",
                "createdAt": "2026-04-03T00:00:00Z",
                "updatedAt": "2026-04-03T00:00:10Z"
            })))
            .mount(&server)
            .await;

        let (_tmp, store) = temp_store();
        store
            .save_auth_session(
                &AuthSessionDto {
                    agent_id: "agent-1".into(),
                    access_token: "access-token".into(),
                    refresh_token: "refresh-token".into(),
                    access_token_expires_at: "2026-04-04T00:00:00Z".into(),
                },
                "2026-04-03T00:00:00Z",
            )
            .expect("save session");

        let mut config = ContextBookConfig::default();
        config.enabled = true;
        config.base_url = server.uri();

        let service = ContextBookService::with_store(config, store.clone()).expect("service");
        let created = service
            .create_local_context(&ContextCreateRequest {
                context_id: None,
                title: "Daily Summary".into(),
                contents: "Agent heartbeat summary".into(),
                tag: Some("ops".into()),
                status: crate::context_book::ContextStatus::Published,
            })
            .await
            .expect("create context");

        assert_eq!(created.context_id, "ctx-1");
        let mirrored = store
            .list_mirrored_contexts()
            .expect("list mirrored contexts");
        assert_eq!(mirrored.len(), 1);
        assert_eq!(mirrored[0].context_id, "ctx-1");
        assert_eq!(mirrored[0].author_agent_id, "agent-1");
    }

    #[tokio::test]
    async fn runtime_service_deletes_context_and_removes_mirror() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/contexts/ctx-1"))
            .and(header("authorization", "Bearer access-token"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;

        let (_tmp, store) = temp_store();
        store
            .save_auth_session(
                &AuthSessionDto {
                    agent_id: "agent-1".into(),
                    access_token: "access-token".into(),
                    refresh_token: "refresh-token".into(),
                    access_token_expires_at: "2026-04-04T00:00:00Z".into(),
                },
                "2026-04-03T00:00:00Z",
            )
            .expect("save session");
        store
            .upsert_mirrored_context(&ContextRecordDto {
                context_id: "ctx-1".into(),
                author_agent_id: "agent-1".into(),
                title: "Daily Summary".into(),
                contents: "Agent heartbeat summary".into(),
                tag: Some("ops".into()),
                status: crate::context_book::ContextStatus::Published,
                created_at: "2026-04-03T00:00:00Z".into(),
                updated_at: "2026-04-03T00:00:10Z".into(),
            })
            .expect("seed mirrored context");

        let mut config = ContextBookConfig::default();
        config.enabled = true;
        config.base_url = server.uri();

        let service = ContextBookService::with_store(config, store.clone()).expect("service");
        service
            .delete_local_context("ctx-1")
            .await
            .expect("delete context");

        let mirrored = store
            .list_mirrored_contexts()
            .expect("list mirrored contexts");
        assert!(mirrored.is_empty());
    }

    #[tokio::test]
    async fn runtime_service_updates_context_and_persists_mirror() {
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/contexts/ctx-1"))
            .and(header("authorization", "Bearer access-token"))
            .and(body_partial_json(serde_json::json!({
                "title": "Updated Summary",
                "status": "Archived"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "contextId": "ctx-1",
                "authorAgentId": "agent-1",
                "title": "Updated Summary",
                "contents": "Agent heartbeat summary",
                "tag": "ops",
                "status": "Archived",
                "createdAt": "2026-04-03T00:00:00Z",
                "updatedAt": "2026-04-03T00:00:30Z"
            })))
            .mount(&server)
            .await;

        let (_tmp, store) = temp_store();
        store
            .save_auth_session(
                &AuthSessionDto {
                    agent_id: "agent-1".into(),
                    access_token: "access-token".into(),
                    refresh_token: "refresh-token".into(),
                    access_token_expires_at: "2026-04-04T00:00:00Z".into(),
                },
                "2026-04-03T00:00:00Z",
            )
            .expect("save session");
        store
            .upsert_mirrored_context(&ContextRecordDto {
                context_id: "ctx-1".into(),
                author_agent_id: "agent-1".into(),
                title: "Daily Summary".into(),
                contents: "Agent heartbeat summary".into(),
                tag: Some("ops".into()),
                status: crate::context_book::ContextStatus::Published,
                created_at: "2026-04-03T00:00:00Z".into(),
                updated_at: "2026-04-03T00:00:10Z".into(),
            })
            .expect("seed mirrored context");

        let mut config = ContextBookConfig::default();
        config.enabled = true;
        config.base_url = server.uri();

        let service = ContextBookService::with_store(config, store.clone()).expect("service");
        let updated = service
            .update_local_context(
                "ctx-1",
                &ContextUpdateRequest {
                    title: Some("Updated Summary".into()),
                    contents: None,
                    tag: None,
                    status: Some(crate::context_book::ContextStatus::Archived),
                },
            )
            .await
            .expect("update context");

        assert_eq!(updated.context_id, "ctx-1");
        assert_eq!(updated.title, "Updated Summary");
        assert_eq!(updated.status, crate::context_book::ContextStatus::Archived);

        let mirrored = store
            .list_mirrored_contexts()
            .expect("list mirrored contexts");
        assert_eq!(mirrored.len(), 1);
        assert_eq!(mirrored[0].context_id, "ctx-1");
        assert_eq!(mirrored[0].title, "Updated Summary");
        assert_eq!(
            mirrored[0].status,
            crate::context_book::ContextStatus::Archived
        );
    }

    #[tokio::test]
    async fn runtime_service_creates_vote_and_persists_mirror() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/votes"))
            .and(header("authorization", "Bearer access-token"))
            .and(body_partial_json(serde_json::json!({
                "voteId": "vote-1",
                "voteScore": 2.5,
                "voteContext": "Approve deploy"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "voteId": "vote-1",
                "ownerAgentId": "agent-1",
                "voteScore": 2.5,
                "voteContext": "Approve deploy",
                "voterAgentIds": [],
                "requiredScore": 5.0,
                "executable": false,
                "createdAt": "2026-04-03T00:00:00Z",
                "updatedAt": "2026-04-03T00:00:10Z"
            })))
            .mount(&server)
            .await;

        let (_tmp, store) = temp_store();
        store
            .save_auth_session(
                &AuthSessionDto {
                    agent_id: "agent-1".into(),
                    access_token: "access-token".into(),
                    refresh_token: "refresh-token".into(),
                    access_token_expires_at: "2026-04-04T00:00:00Z".into(),
                },
                "2026-04-03T00:00:00Z",
            )
            .expect("save session");

        let mut config = ContextBookConfig::default();
        config.enabled = true;
        config.base_url = server.uri();

        let service = ContextBookService::with_store(config, store.clone()).expect("service");
        let created = service
            .create_local_vote(&VoteCreateRequest {
                vote_id: Some("vote-1".into()),
                vote_score: Some(2.5),
                vote_context: "Approve deploy".into(),
            })
            .await
            .expect("create vote");

        assert_eq!(created.vote_id, "vote-1");
        assert_eq!(created.vote_score, Some(2.5));

        let mirrored = store.list_mirrored_votes().expect("list mirrored votes");
        assert_eq!(mirrored.len(), 1);
        assert_eq!(mirrored[0].vote_id, "vote-1");
        assert_eq!(mirrored[0].required_score, Some(5.0));
        assert_eq!(mirrored[0].executable, Some(false));
    }
}
