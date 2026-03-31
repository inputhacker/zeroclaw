use super::config::ResolvedContextBookConfig;
use super::events::{ContextBookEventEnvelope, parse_polled_events};
use super::handle::{
    ContextBookContractSnapshot, ContextBookContractValidationState, ContextBookDegradedMode,
    ContextBookRefreshMode,
};
use super::store::{
    ContextBookAgentSnapshot, ContextBookContextSnapshot, ContextBookSubscriptionsSnapshot,
    ContextBookVoteSnapshot,
};
use crate::auth::profiles::{AuthProfileKind, AuthProfilesStore, TokenSet};
use crate::auth::{AuthService, state_dir_from_config};
use crate::config::Config;
use anyhow::{Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use reqwest::{Response, StatusCode, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::process::Stdio;
use thiserror::Error;
use tokio::process::Command;
use tokio::time::{Duration, timeout};

const CONTEXT_BOOK_PROVIDER: &str = "context-book";
const ACCESS_TOKEN_REFRESH_SKEW_SECS: i64 = 90;
const REQUEST_WAIT_MS: u64 = 5_000;
const HTTP_TIMEOUT_SECS: u64 = 30;
const HTTP_CONNECT_TIMEOUT_SECS: u64 = 10;
const DISCOVERY_TIMEOUT_SECS: u64 = 5;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextBookAgentIdentity {
    pub agent_id: String,
    pub agent_name: String,
    pub device_type: String,
    pub display_name: String,
}

#[derive(Debug, Clone)]
pub struct ContextBookSession {
    pub base_url: Url,
    pub agent_id: String,
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct ContextBookRuntimeInspection {
    pub contract: ContextBookContractSnapshot,
    pub subscriptions: ContextBookSubscriptionsSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextBookAgentStatusSnapshot {
    pub agent_id: String,
    pub lifecycle_state: Option<String>,
    pub connection_state: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextBookClientErrorKind {
    AuthRequired,
    BootstrapApprovalRequired,
    BootstrapDenied,
    BootstrapExpired,
    AgentNotRegistered,
    CursorNotFound,
    Unauthorized,
    DiscoveryUnavailable,
    ContractViolation,
    Network,
    Unexpected,
}

#[derive(Debug, Error)]
#[error("{kind:?}: {message}")]
pub struct ContextBookClientError {
    pub kind: ContextBookClientErrorKind,
    pub message: String,
}

#[derive(Debug, Deserialize)]
struct ContextBookErrorEnvelope {
    error: ContextBookApiError,
}

#[derive(Debug, Deserialize)]
struct ContextBookApiError {
    code: String,
    message: String,
}

#[derive(Debug, Clone, Deserialize)]
struct TokenResponse {
    #[serde(default, alias = "accessToken")]
    access_token: String,
    #[serde(default, alias = "refreshToken")]
    refresh_token: Option<String>,
    #[serde(default)]
    token_type: Option<String>,
    #[serde(default, alias = "expiresIn")]
    expires_in: Option<i64>,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default, alias = "agentId")]
    agent_id: Option<String>,
    #[serde(default)]
    agent: Option<TokenResponseAgent>,
}

#[derive(Debug, Clone, Deserialize)]
struct TokenResponseAgent {
    #[serde(default, alias = "agentId")]
    agent_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentStatusResponse {
    #[serde(default, alias = "agentId")]
    agent_id: String,
    #[serde(default, alias = "deviceType")]
    device_type: Option<String>,
    #[serde(default, alias = "displayName")]
    display_name: Option<String>,
    #[serde(default, alias = "lifecycleState")]
    lifecycle_state: Option<String>,
    #[serde(default, alias = "connectionState")]
    connection_state: Option<String>,
    #[serde(default, alias = "lastSeenAt")]
    last_seen_at: Option<String>,
    #[serde(default, alias = "createdAt")]
    created_at: Option<String>,
    #[serde(default, alias = "updatedAt")]
    updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ContextRecordResponse {
    #[serde(default, alias = "contextId")]
    context_id: String,
    #[serde(default, alias = "authorAgentId")]
    author_agent_id: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    contents: String,
    #[serde(default)]
    tag: String,
    #[serde(default)]
    status: String,
    #[serde(default, alias = "createdAt")]
    created_at: Option<String>,
    #[serde(default, alias = "updatedAt")]
    updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VoteResponse {
    #[serde(default, alias = "voteId")]
    vote_id: String,
    #[serde(default, alias = "ownerAgentId")]
    owner_agent_id: String,
    #[serde(default, alias = "voteScore")]
    vote_score: f64,
    #[serde(default, alias = "voteContext")]
    vote_context: String,
    #[serde(default, alias = "voterAgentIds")]
    voter_agent_ids: Vec<String>,
    #[serde(default, alias = "requiredScore")]
    required_score: Option<i64>,
    #[serde(default)]
    executable: Option<bool>,
    #[serde(default, alias = "createdAt")]
    created_at: Option<String>,
    #[serde(default, alias = "updatedAt")]
    updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ContextBookContextCreateRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_id: Option<String>,
    pub title: String,
    pub contents: String,
    pub tag: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ContextBookContextUpdateRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contents: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ContextBookVoteCreateRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vote_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vote_score: Option<f64>,
    pub vote_context: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ContextBookVoteUpdateRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vote_score: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vote_context: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ContextBookVoteCastRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vote_score: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct SubscriptionsResponse {
    #[serde(default, alias = "consumerAgentId")]
    consumer_agent_id: Option<String>,
    #[serde(default, alias = "desiredProducerAgentIds")]
    desired_producer_agent_ids: Option<Vec<String>>,
    #[serde(default, alias = "effectiveProducerAgentIds")]
    effective_producer_agent_ids: Option<Vec<String>>,
    #[serde(default, alias = "producerAgentIds")]
    producer_agent_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
struct DiscoveredContextBookEndpoint {
    instance_name: String,
    service_type: String,
    domain: String,
    hostname: String,
    address: String,
    port: u16,
    path: String,
    health_path: Option<String>,
    instance_id: Option<String>,
    env: Option<String>,
    priority: i64,
    weight: i64,
    metadata: BTreeMap<String, String>,
}

impl TokenResponse {
    fn resolved_agent_id(&self, fallback: &str) -> String {
        self.agent_id
            .clone()
            .or_else(|| self.agent.as_ref().and_then(|agent| agent.agent_id.clone()))
            .unwrap_or_else(|| fallback.to_string())
    }
}

impl SubscriptionsResponse {
    fn split_view_available(&self) -> bool {
        self.desired_producer_agent_ids.is_some() && self.effective_producer_agent_ids.is_some()
    }

    fn into_snapshot(self) -> ContextBookSubscriptionsSnapshot {
        ContextBookSubscriptionsSnapshot {
            consumer_agent_id: self.consumer_agent_id,
            desired_producer_agent_ids: normalize_agent_ids(
                &self.desired_producer_agent_ids.unwrap_or_default(),
            ),
            effective_producer_agent_ids: normalize_agent_ids(
                &self
                    .effective_producer_agent_ids
                    .or(self.producer_agent_ids)
                    .unwrap_or_default(),
            ),
            updated_at: Utc::now().to_rfc3339(),
        }
    }
}

impl DiscoveredContextBookEndpoint {
    fn label(&self) -> String {
        format!(
            "{}.{}.{} -> {}:{}",
            self.instance_name, self.service_type, self.domain, self.address, self.port
        )
    }

    fn base_url(&self) -> Result<Url> {
        let host = format_url_host(&self.address);
        let raw = if self.path == "/" {
            format!("http://{host}:{}/", self.port)
        } else {
            format!("http://{host}:{}{}", self.port, self.path)
        };
        Url::parse(&raw).context("invalid discovered base URL")
    }

    fn preflight_url(&self, base_url: &Url) -> Result<Url> {
        join_relative_url(
            base_url,
            self.health_path.as_deref().unwrap_or(self.path.as_str()),
        )
    }
}

#[derive(Debug, Clone, Deserialize)]
struct BootstrapWaitResponse {
    #[serde(default, alias = "requestId")]
    request_id: Option<String>,
    #[serde(default, alias = "waitToken")]
    wait_token: Option<String>,
    #[serde(default, alias = "statusUrl")]
    status_url: Option<String>,
    #[serde(default, alias = "watchUrl")]
    watch_url: Option<String>,
    #[serde(default, alias = "completeUrl")]
    complete_url: Option<String>,
    #[serde(default, alias = "nextAction")]
    next_action: Option<String>,
    #[serde(default, alias = "approvalState")]
    approval_state: Option<String>,
    #[serde(default)]
    request: Option<BootstrapWaitRequestSnapshot>,
}

#[derive(Debug, Clone, Deserialize)]
struct BootstrapWaitRequestSnapshot {
    #[serde(default, alias = "requestId")]
    request_id: Option<String>,
    #[serde(default, alias = "waitToken")]
    wait_token: Option<String>,
    #[serde(default, alias = "statusUrl")]
    status_url: Option<String>,
    #[serde(default, alias = "watchUrl")]
    watch_url: Option<String>,
    #[serde(default, alias = "completeUrl")]
    complete_url: Option<String>,
    #[serde(default, alias = "nextAction")]
    next_action: Option<String>,
    #[serde(default, alias = "approvalState")]
    approval_state: Option<String>,
}

impl BootstrapWaitResponse {
    fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref().or_else(|| {
            self.request
                .as_ref()
                .and_then(|request| request.request_id.as_deref())
        })
    }

    fn wait_token(&self) -> Option<&str> {
        self.wait_token.as_deref().or_else(|| {
            self.request
                .as_ref()
                .and_then(|request| request.wait_token.as_deref())
        })
    }

    fn status_url(&self) -> Option<&str> {
        self.status_url.as_deref().or_else(|| {
            self.request
                .as_ref()
                .and_then(|request| request.status_url.as_deref())
        })
    }

    fn complete_url(&self) -> Option<&str> {
        self.complete_url.as_deref().or_else(|| {
            self.request
                .as_ref()
                .and_then(|request| request.complete_url.as_deref())
        })
    }

    fn approval_state(&self) -> Option<&str> {
        self.approval_state.as_deref().or_else(|| {
            self.request
                .as_ref()
                .and_then(|request| request.approval_state.as_deref())
        })
    }

    fn has_wait_metadata(&self) -> bool {
        self.wait_token().is_some()
    }
}

#[derive(Clone)]
pub struct ContextBookClient {
    resolved: ResolvedContextBookConfig,
    auth_service: AuthService,
    auth_store: AuthProfilesStore,
    http_client: reqwest::Client,
    identity: ContextBookAgentIdentity,
    bootstrap_secret: Option<String>,
}

impl ContextBookClient {
    pub fn new(config: &Config) -> Self {
        let resolved = ResolvedContextBookConfig::resolve(config);
        let state_dir = state_dir_from_config(config);
        let auth_service = AuthService::from_config(config);
        let auth_store = AuthProfilesStore::new(&state_dir, config.secrets.encrypt);
        let http_client = crate::config::build_runtime_proxy_client_with_timeouts(
            "context_book.client",
            HTTP_TIMEOUT_SECS,
            HTTP_CONNECT_TIMEOUT_SECS,
        );

        Self {
            bootstrap_secret: std::env::var(&resolved.bootstrap_secret_env_key).ok(),
            resolved,
            auth_service,
            auth_store,
            http_client,
            identity: derive_identity(config),
        }
    }

    pub fn identity(&self) -> &ContextBookAgentIdentity {
        &self.identity
    }

    pub fn resolved(&self) -> &ResolvedContextBookConfig {
        &self.resolved
    }

    pub async fn ensure_session(&self) -> Result<ContextBookSession, ContextBookClientError> {
        let base_url = self.resolve_base_url().await?;
        match self.load_stored_session(&base_url).await {
            Ok(Some(session)) => return Ok(session),
            Ok(None) => {}
            Err(stored_error)
                if self.bootstrap_secret.is_some()
                    && session_recovery_fallback_allowed(stored_error.kind) =>
            {
                tracing::warn!(
                    "context_book stored session recovery failed; attempting bootstrap fallback: {}",
                    stored_error.message
                );
                return match self.bootstrap_session(&base_url).await {
                    Ok(session) => Ok(session),
                    Err(bootstrap_error) => Err(ContextBookClientError {
                        kind: bootstrap_error.kind,
                        message: format!(
                            "stored session recovery failed: {}; bootstrap fallback failed: {}",
                            stored_error.message, bootstrap_error.message
                        ),
                    }),
                };
            }
            Err(error) => return Err(error),
        }
        self.bootstrap_session(&base_url).await
    }

    pub async fn activate_agent(
        &self,
        session: &ContextBookSession,
    ) -> Result<(), ContextBookClientError> {
        let url = session
            .base_url
            .join(&format!("agents/{}/status", session.agent_id))
            .map_err(|error| self.contract_error(format!("invalid activation URL: {error}")))?;
        let response = self
            .http_client
            .patch(url)
            .bearer_auth(&session.access_token)
            .json(&json!({
                "status": "Active",
                "lifecycleState": "Active",
            }))
            .send()
            .await
            .map_err(|error| self.network_error(format!("failed to activate agent: {error}")))?;
        self.expect_success(response, "failed to activate agent")
            .await?;
        Ok(())
    }

    pub async fn mark_inactive(
        &self,
        session: &ContextBookSession,
    ) -> Result<(), ContextBookClientError> {
        let url = session
            .base_url
            .join(&format!("agents/{}/status", session.agent_id))
            .map_err(|error| self.contract_error(format!("invalid deactivate URL: {error}")))?;
        let response = self
            .http_client
            .patch(url)
            .bearer_auth(&session.access_token)
            .json(&json!({
                "status": "Inactive",
                "lifecycleState": "Inactive",
            }))
            .send()
            .await
            .map_err(|error| {
                self.network_error(format!("failed to mark agent inactive: {error}"))
            })?;
        self.expect_success(response, "failed to mark agent inactive")
            .await?;
        Ok(())
    }

    pub async fn open_event_stream(
        &self,
        session: &ContextBookSession,
        last_event_id: Option<&str>,
    ) -> Result<Response, ContextBookClientError> {
        let mut url = session
            .base_url
            .join("events/stream")
            .map_err(|error| self.contract_error(format!("invalid events stream URL: {error}")))?;
        url.query_pairs_mut()
            .append_pair("agentId", &session.agent_id);

        let mut request = self
            .http_client
            .get(url)
            .bearer_auth(&session.access_token)
            .header(reqwest::header::ACCEPT, "text/event-stream");
        if let Some(cursor) = last_event_id.filter(|cursor| !cursor.trim().is_empty()) {
            request = request.header("Last-Event-ID", cursor);
        }

        let response = request.send().await.map_err(|error| {
            self.network_error(format!("failed to open Context Book SSE stream: {error}"))
        })?;
        self.expect_success(response, "failed to open Context Book SSE stream")
            .await
    }

    pub async fn poll_events(
        &self,
        session: &ContextBookSession,
        since_event_id: Option<&str>,
    ) -> Result<Vec<ContextBookEventEnvelope>, ContextBookClientError> {
        let mut url = session
            .base_url
            .join("events")
            .map_err(|error| self.contract_error(format!("invalid events poll URL: {error}")))?;
        if let Some(cursor) = since_event_id.filter(|cursor| !cursor.trim().is_empty()) {
            url.query_pairs_mut().append_pair("sinceEventId", cursor);
        }
        let response = self
            .http_client
            .get(url)
            .bearer_auth(&session.access_token)
            .send()
            .await
            .map_err(|error| {
                self.network_error(format!("failed to poll Context Book events: {error}"))
            })?;
        let response = self
            .expect_success(response, "failed to poll Context Book events")
            .await?;
        let body = response.json::<Value>().await.map_err(|error| {
            self.contract_error(format!("failed to parse polling response: {error}"))
        })?;
        parse_polled_events(body)
            .map_err(|error| self.contract_error(format!("invalid polling event payload: {error}")))
    }

    pub async fn get_subscriptions(
        &self,
        session: &ContextBookSession,
    ) -> Result<ContextBookSubscriptionsSnapshot, ContextBookClientError> {
        Ok(self
            .fetch_subscriptions_response(session)
            .await?
            .into_snapshot())
    }

    pub async fn get_agents(
        &self,
        session: &ContextBookSession,
    ) -> Result<Vec<ContextBookAgentSnapshot>, ContextBookClientError> {
        let url = session
            .base_url
            .join("agents")
            .map_err(|error| self.contract_error(format!("invalid agents URL: {error}")))?;
        let response = self
            .http_client
            .get(url)
            .bearer_auth(&session.access_token)
            .send()
            .await
            .map_err(|error| {
                self.network_error(format!("failed to fetch Context Book agents: {error}"))
            })?;
        let response = self
            .expect_success(response, "failed to fetch Context Book agents")
            .await?;
        let body = response.json::<Value>().await.map_err(|error| {
            self.contract_error(format!(
                "failed to parse Context Book agents response: {error}"
            ))
        })?;
        parse_agent_snapshots(body)
            .map_err(|error| self.contract_error(format!("invalid agents payload: {error}")))
    }

    pub async fn get_contexts(
        &self,
        session: &ContextBookSession,
    ) -> Result<Vec<ContextBookContextSnapshot>, ContextBookClientError> {
        let url = session
            .base_url
            .join("contexts")
            .map_err(|error| self.contract_error(format!("invalid contexts URL: {error}")))?;
        let response = self
            .http_client
            .get(url)
            .bearer_auth(&session.access_token)
            .send()
            .await
            .map_err(|error| {
                self.network_error(format!("failed to fetch Context Book contexts: {error}"))
            })?;
        let response = self
            .expect_success(response, "failed to fetch Context Book contexts")
            .await?;
        let body = response.json::<Value>().await.map_err(|error| {
            self.contract_error(format!(
                "failed to parse Context Book contexts response: {error}"
            ))
        })?;
        parse_context_snapshots(body)
            .map_err(|error| self.contract_error(format!("invalid contexts payload: {error}")))
    }

    pub async fn get_votes(
        &self,
        session: &ContextBookSession,
    ) -> Result<Vec<ContextBookVoteSnapshot>, ContextBookClientError> {
        let url = session
            .base_url
            .join("votes")
            .map_err(|error| self.contract_error(format!("invalid votes URL: {error}")))?;
        let response = self
            .http_client
            .get(url)
            .bearer_auth(&session.access_token)
            .send()
            .await
            .map_err(|error| {
                self.network_error(format!("failed to fetch Context Book votes: {error}"))
            })?;
        let response = self
            .expect_success(response, "failed to fetch Context Book votes")
            .await?;
        let body = response.json::<Value>().await.map_err(|error| {
            self.contract_error(format!(
                "failed to parse Context Book votes response: {error}"
            ))
        })?;
        parse_vote_snapshots(body)
            .map_err(|error| self.contract_error(format!("invalid votes payload: {error}")))
    }

    pub async fn set_subscriptions(
        &self,
        session: &ContextBookSession,
        desired_producer_agent_ids: &[String],
    ) -> Result<ContextBookSubscriptionsSnapshot, ContextBookClientError> {
        let url = session
            .base_url
            .join("subscriptions")
            .map_err(|error| self.contract_error(format!("invalid subscriptions URL: {error}")))?;
        let desired = normalize_agent_ids(desired_producer_agent_ids);
        let response = self
            .http_client
            .put(url)
            .bearer_auth(&session.access_token)
            .json(&json!({
                "desiredProducerAgentIds": desired,
                "producerAgentIds": desired,
            }))
            .send()
            .await
            .map_err(|error| {
                self.network_error(format!(
                    "failed to update Context Book subscriptions: {error}"
                ))
            })?;
        let response = self
            .expect_success(response, "failed to update Context Book subscriptions")
            .await?;
        let body = response
            .json::<SubscriptionsResponse>()
            .await
            .map_err(|error| {
                self.contract_error(format!("failed to parse subscriptions response: {error}"))
            })?;
        Ok(body.into_snapshot())
    }

    pub async fn create_context(
        &self,
        session: &ContextBookSession,
        request: &ContextBookContextCreateRequest,
    ) -> Result<ContextBookContextSnapshot, ContextBookClientError> {
        let url = session
            .base_url
            .join("contexts")
            .map_err(|error| self.contract_error(format!("invalid contexts URL: {error}")))?;
        let response = self
            .http_client
            .post(url)
            .bearer_auth(&session.access_token)
            .json(request)
            .send()
            .await
            .map_err(|error| {
                self.network_error(format!("failed to create Context Book context: {error}"))
            })?;
        let response = self
            .expect_success(response, "failed to create Context Book context")
            .await?;
        let body = response.json::<Value>().await.map_err(|error| {
            self.contract_error(format!(
                "failed to parse Context Book context create response: {error}"
            ))
        })?;
        parse_context_snapshot(body)
            .map_err(|error| self.contract_error(format!("invalid context payload: {error}")))
    }

    pub async fn update_context(
        &self,
        session: &ContextBookSession,
        context_id: &str,
        request: &ContextBookContextUpdateRequest,
    ) -> Result<ContextBookContextSnapshot, ContextBookClientError> {
        let url = session
            .base_url
            .join(&format!("contexts/{context_id}"))
            .map_err(|error| self.contract_error(format!("invalid context update URL: {error}")))?;
        let response = self
            .http_client
            .patch(url)
            .bearer_auth(&session.access_token)
            .json(request)
            .send()
            .await
            .map_err(|error| {
                self.network_error(format!("failed to update Context Book context: {error}"))
            })?;
        let response = self
            .expect_success(response, "failed to update Context Book context")
            .await?;
        let body = response.json::<Value>().await.map_err(|error| {
            self.contract_error(format!(
                "failed to parse Context Book context update response: {error}"
            ))
        })?;
        parse_context_snapshot(body)
            .map_err(|error| self.contract_error(format!("invalid context payload: {error}")))
    }

    pub async fn delete_context(
        &self,
        session: &ContextBookSession,
        context_id: &str,
    ) -> Result<(), ContextBookClientError> {
        let url = session
            .base_url
            .join(&format!("contexts/{context_id}"))
            .map_err(|error| self.contract_error(format!("invalid context delete URL: {error}")))?;
        let response = self
            .http_client
            .delete(url)
            .bearer_auth(&session.access_token)
            .send()
            .await
            .map_err(|error| {
                self.network_error(format!("failed to delete Context Book context: {error}"))
            })?;
        self.expect_success(response, "failed to delete Context Book context")
            .await?;
        Ok(())
    }

    pub async fn create_vote(
        &self,
        session: &ContextBookSession,
        request: &ContextBookVoteCreateRequest,
    ) -> Result<ContextBookVoteSnapshot, ContextBookClientError> {
        let url = session
            .base_url
            .join("votes")
            .map_err(|error| self.contract_error(format!("invalid votes URL: {error}")))?;
        let response = self
            .http_client
            .post(url)
            .bearer_auth(&session.access_token)
            .json(request)
            .send()
            .await
            .map_err(|error| {
                self.network_error(format!("failed to create Context Book vote: {error}"))
            })?;
        let response = self
            .expect_success(response, "failed to create Context Book vote")
            .await?;
        let body = response.json::<Value>().await.map_err(|error| {
            self.contract_error(format!(
                "failed to parse Context Book vote create response: {error}"
            ))
        })?;
        parse_vote_snapshot(body)
            .map_err(|error| self.contract_error(format!("invalid vote payload: {error}")))
    }

    pub async fn update_vote(
        &self,
        session: &ContextBookSession,
        vote_id: &str,
        request: &ContextBookVoteUpdateRequest,
    ) -> Result<ContextBookVoteSnapshot, ContextBookClientError> {
        let url = session
            .base_url
            .join(&format!("votes/{vote_id}"))
            .map_err(|error| self.contract_error(format!("invalid vote update URL: {error}")))?;
        let response = self
            .http_client
            .patch(url)
            .bearer_auth(&session.access_token)
            .json(request)
            .send()
            .await
            .map_err(|error| {
                self.network_error(format!("failed to update Context Book vote: {error}"))
            })?;
        let response = self
            .expect_success(response, "failed to update Context Book vote")
            .await?;
        let body = response.json::<Value>().await.map_err(|error| {
            self.contract_error(format!(
                "failed to parse Context Book vote update response: {error}"
            ))
        })?;
        parse_vote_snapshot(body)
            .map_err(|error| self.contract_error(format!("invalid vote payload: {error}")))
    }

    pub async fn delete_vote(
        &self,
        session: &ContextBookSession,
        vote_id: &str,
    ) -> Result<(), ContextBookClientError> {
        let url = session
            .base_url
            .join(&format!("votes/{vote_id}"))
            .map_err(|error| self.contract_error(format!("invalid vote delete URL: {error}")))?;
        let response = self
            .http_client
            .delete(url)
            .bearer_auth(&session.access_token)
            .send()
            .await
            .map_err(|error| {
                self.network_error(format!("failed to delete Context Book vote: {error}"))
            })?;
        self.expect_success(response, "failed to delete Context Book vote")
            .await?;
        Ok(())
    }

    pub async fn cast_vote(
        &self,
        session: &ContextBookSession,
        vote_id: &str,
        request: &ContextBookVoteCastRequest,
    ) -> Result<ContextBookVoteSnapshot, ContextBookClientError> {
        let url = session
            .base_url
            .join(&format!("votes/{vote_id}/cast"))
            .map_err(|error| self.contract_error(format!("invalid vote cast URL: {error}")))?;
        let response = self
            .http_client
            .post(url)
            .bearer_auth(&session.access_token)
            .json(request)
            .send()
            .await
            .map_err(|error| {
                self.network_error(format!("failed to cast Context Book vote: {error}"))
            })?;
        let response = self
            .expect_success(response, "failed to cast Context Book vote")
            .await?;
        let body = response.json::<Value>().await.map_err(|error| {
            self.contract_error(format!(
                "failed to parse Context Book vote cast response: {error}"
            ))
        })?;
        parse_vote_snapshot(body)
            .map_err(|error| self.contract_error(format!("invalid vote payload: {error}")))
    }

    pub async fn inspect_runtime_contract(
        &self,
        session: &ContextBookSession,
    ) -> Result<ContextBookRuntimeInspection, ContextBookClientError> {
        let current_agent = self.current_agent_status(session).await?;
        let subscriptions = self.fetch_subscriptions_response(session).await?;
        let cursor_not_found_returns_409 = self.probe_cursor_not_found_contract(session).await?;
        let vote_deleted_supported = self.probe_vote_delete_support(session).await?;
        let refresh_mode = self
            .probe_refresh_mode(&session.base_url, &session.agent_id)
            .await?;

        let lifecycle_connection_split = Some(current_agent.as_ref().is_some_and(|agent| {
            agent
                .lifecycle_state
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
                && agent
                    .connection_state
                    .as_deref()
                    .is_some_and(|value| !value.trim().is_empty())
        }));
        let subscriptions_split = Some(subscriptions.split_view_available());
        let mut degraded_modes = Vec::new();
        let mut notes = Vec::new();

        if lifecycle_connection_split == Some(false) {
            degraded_modes.push(ContextBookDegradedMode::Disconnect);
            notes.push(
                "GET /agents did not expose both lifecycleState and connectionState".to_string(),
            );
        }
        if subscriptions_split == Some(false) {
            degraded_modes.push(ContextBookDegradedMode::NoWrite);
            notes.push(
                "GET /subscriptions did not expose both desiredProducerAgentIds and effectiveProducerAgentIds"
                    .to_string(),
            );
        }
        if !cursor_not_found_returns_409 {
            degraded_modes.push(ContextBookDegradedMode::Disconnect);
            notes.push(
                "GET /events probe did not return 409 CURSOR_NOT_FOUND for an unknown cursor"
                    .to_string(),
            );
        }
        if !vote_deleted_supported {
            degraded_modes.push(ContextBookDegradedMode::NoWrite);
            notes.push(
                "DELETE /votes/{voteId} probe did not indicate explicit vote delete support"
                    .to_string(),
            );
        }
        if refresh_mode == ContextBookRefreshMode::Disabled {
            degraded_modes.push(ContextBookDegradedMode::NoRefresh);
            notes.push("no supported refresh endpoint was detected".to_string());
        }

        dedup_degraded_modes(&mut degraded_modes);
        let validation_state = if degraded_modes.contains(&ContextBookDegradedMode::Disconnect) {
            ContextBookContractValidationState::Invalid
        } else if degraded_modes.is_empty() {
            ContextBookContractValidationState::Validated
        } else {
            ContextBookContractValidationState::Degraded
        };

        Ok(ContextBookRuntimeInspection {
            contract: ContextBookContractSnapshot {
                validation_state,
                checked_at: Some(Utc::now().to_rfc3339()),
                lifecycle_connection_split,
                subscriptions_desired_effective_split: subscriptions_split,
                cursor_not_found_returns_409: Some(cursor_not_found_returns_409),
                vote_deleted_supported: Some(vote_deleted_supported),
                refresh_mode,
                degraded_modes,
                notes,
            },
            subscriptions: subscriptions.into_snapshot(),
        })
    }

    pub async fn validate_runtime_contract(
        &self,
        session: &ContextBookSession,
    ) -> Result<ContextBookContractSnapshot, ContextBookClientError> {
        Ok(self.inspect_runtime_contract(session).await?.contract)
    }

    async fn current_agent_status(
        &self,
        session: &ContextBookSession,
    ) -> Result<Option<ContextBookAgentStatusSnapshot>, ContextBookClientError> {
        let url = session
            .base_url
            .join("agents")
            .map_err(|error| self.contract_error(format!("invalid agents URL: {error}")))?;
        let response = self
            .http_client
            .get(url)
            .bearer_auth(&session.access_token)
            .send()
            .await
            .map_err(|error| {
                self.network_error(format!("failed to fetch Context Book agents: {error}"))
            })?;
        let response = self
            .expect_success(response, "failed to fetch Context Book agents")
            .await?;
        let body = response.json::<Value>().await.map_err(|error| {
            self.contract_error(format!(
                "failed to parse Context Book agents response: {error}"
            ))
        })?;
        let agents = parse_agents_response(body)
            .map_err(|error| self.contract_error(format!("invalid agents payload: {error}")))?;
        Ok(agents
            .into_iter()
            .find(|agent| agent.agent_id == session.agent_id))
    }

    async fn fetch_subscriptions_response(
        &self,
        session: &ContextBookSession,
    ) -> Result<SubscriptionsResponse, ContextBookClientError> {
        let url = session
            .base_url
            .join("subscriptions")
            .map_err(|error| self.contract_error(format!("invalid subscriptions URL: {error}")))?;
        let response = self
            .http_client
            .get(url)
            .bearer_auth(&session.access_token)
            .send()
            .await
            .map_err(|error| {
                self.network_error(format!(
                    "failed to fetch Context Book subscriptions: {error}"
                ))
            })?;
        let response = self
            .expect_success(response, "failed to fetch Context Book subscriptions")
            .await?;
        response
            .json::<SubscriptionsResponse>()
            .await
            .map_err(|error| {
                self.contract_error(format!("failed to parse subscriptions response: {error}"))
            })
    }

    async fn probe_cursor_not_found_contract(
        &self,
        session: &ContextBookSession,
    ) -> Result<bool, ContextBookClientError> {
        let mut url = session
            .base_url
            .join("events")
            .map_err(|error| self.contract_error(format!("invalid events URL: {error}")))?;
        url.query_pairs_mut()
            .append_pair("sinceEventId", "__zeroclaw_contract_probe_cursor__");
        let response = self
            .http_client
            .get(url)
            .bearer_auth(&session.access_token)
            .send()
            .await
            .map_err(|error| {
                self.network_error(format!(
                    "failed to probe Context Book cursor-not-found contract: {error}"
                ))
            })?;
        if response.status() == StatusCode::CONFLICT {
            let error = self.parse_http_error(response).await;
            return Ok(error.kind == ContextBookClientErrorKind::CursorNotFound);
        }
        Ok(false)
    }

    async fn probe_vote_delete_support(
        &self,
        session: &ContextBookSession,
    ) -> Result<bool, ContextBookClientError> {
        let url = session
            .base_url
            .join("votes/__zeroclaw_contract_probe_vote__")
            .map_err(|error| {
                self.contract_error(format!("invalid vote delete probe URL: {error}"))
            })?;
        let response = self
            .http_client
            .delete(url)
            .bearer_auth(&session.access_token)
            .send()
            .await
            .map_err(|error| {
                self.network_error(format!(
                    "failed to probe Context Book vote delete contract: {error}"
                ))
            })?;
        let status = response.status();
        if status.is_success() {
            return Ok(true);
        }

        let body = response.text().await.unwrap_or_default();
        let parsed = serde_json::from_str::<ContextBookErrorEnvelope>(&body).ok();
        if matches!(
            status,
            StatusCode::BAD_REQUEST
                | StatusCode::UNAUTHORIZED
                | StatusCode::FORBIDDEN
                | StatusCode::CONFLICT
        ) {
            return Ok(parsed.is_some());
        }
        if status == StatusCode::NOT_FOUND {
            return Ok(parsed.is_some());
        }
        if matches!(
            status,
            StatusCode::METHOD_NOT_ALLOWED | StatusCode::NOT_IMPLEMENTED
        ) {
            return Ok(false);
        }
        Ok(parsed.is_some())
    }

    async fn probe_refresh_mode(
        &self,
        base_url: &Url,
        agent_id: &str,
    ) -> Result<ContextBookRefreshMode, ContextBookClientError> {
        if self
            .refresh_endpoint_exists(base_url, RefreshProbeKind::OAuth2Token)
            .await?
        {
            return Ok(ContextBookRefreshMode::OAuth2Token);
        }
        if self
            .refresh_endpoint_exists(
                base_url,
                RefreshProbeKind::LegacyAuthRefresh {
                    agent_id: agent_id.to_string(),
                },
            )
            .await?
        {
            return Ok(ContextBookRefreshMode::LegacyAuthRefresh);
        }
        Ok(ContextBookRefreshMode::Disabled)
    }

    async fn refresh_endpoint_exists(
        &self,
        base_url: &Url,
        probe: RefreshProbeKind,
    ) -> Result<bool, ContextBookClientError> {
        let request = match probe {
            RefreshProbeKind::OAuth2Token => self
                .http_client
                .post(base_url.join("oauth2/token").map_err(|error| {
                    self.contract_error(format!("invalid oauth2/token URL: {error}"))
                })?)
                .form(&[
                    ("grant_type", "refresh_token"),
                    ("refresh_token", "__zeroclaw_probe__"),
                ]),
            RefreshProbeKind::LegacyAuthRefresh { ref agent_id } => self
                .http_client
                .post(base_url.join("auth/refresh").map_err(|error| {
                    self.contract_error(format!("invalid auth/refresh URL: {error}"))
                })?)
                .json(&json!({
                    "agentId": agent_id,
                    "refreshToken": "__zeroclaw_probe__",
                })),
        };
        let response = request.send().await.map_err(|error| {
            self.network_error(format!(
                "failed to probe Context Book refresh endpoint: {error}"
            ))
        })?;
        Ok(response.status() != StatusCode::NOT_FOUND)
    }

    async fn refresh_via_oauth2_token(
        &self,
        base_url: &Url,
        refresh_token: &str,
    ) -> Result<TokenResponse, ContextBookClientError> {
        let token_url = base_url
            .join("oauth2/token")
            .map_err(|error| self.contract_error(format!("invalid oauth2/token URL: {error}")))?;
        let response = self
            .http_client
            .post(token_url)
            .form(&[
                ("grant_type", "refresh_token"),
                ("refresh_token", refresh_token),
            ])
            .send()
            .await
            .map_err(|error| {
                self.network_error(format!("failed to refresh Context Book token: {error}"))
            })?;
        if response.status() == StatusCode::NOT_FOUND {
            return Err(self.contract_error(
                "oauth2/token refresh endpoint is not available on this Context Book server",
            ));
        }
        let response = self
            .expect_success(response, "failed to refresh Context Book token")
            .await?;
        response.json::<TokenResponse>().await.map_err(|error| {
            self.contract_error(format!("failed to parse refresh token response: {error}"))
        })
    }

    async fn refresh_via_legacy_auth_refresh(
        &self,
        base_url: &Url,
        agent_id: &str,
        refresh_token: &str,
    ) -> Result<TokenResponse, ContextBookClientError> {
        let token_url = base_url
            .join("auth/refresh")
            .map_err(|error| self.contract_error(format!("invalid auth/refresh URL: {error}")))?;
        let response = self
            .http_client
            .post(token_url)
            .json(&json!({
                "agentId": agent_id,
                "refreshToken": refresh_token,
            }))
            .send()
            .await
            .map_err(|error| {
                self.network_error(format!(
                    "failed to refresh Context Book token via legacy auth/refresh: {error}"
                ))
            })?;
        let response = self
            .expect_success(
                response,
                "failed to refresh Context Book token via auth/refresh",
            )
            .await?;
        response.json::<TokenResponse>().await.map_err(|error| {
            self.contract_error(format!(
                "failed to parse legacy refresh token response: {error}"
            ))
        })
    }

    async fn load_stored_session(
        &self,
        base_url: &Url,
    ) -> Result<Option<ContextBookSession>, ContextBookClientError> {
        let profile = self
            .auth_service
            .get_profile(CONTEXT_BOOK_PROVIDER, self.resolved.auth_profile.as_deref())
            .await
            .map_err(|error| {
                self.unexpected_error(format!("failed to load Context Book auth profile: {error}"))
            })?;

        let Some(profile) = profile else {
            return Ok(None);
        };

        match profile.kind {
            AuthProfileKind::Token => {
                let access_token = profile.token.unwrap_or_default();
                if access_token.trim().is_empty() {
                    return Ok(None);
                }
                Ok(Some(ContextBookSession {
                    base_url: base_url.clone(),
                    agent_id: profile
                        .metadata
                        .get("agent_id")
                        .cloned()
                        .unwrap_or_else(|| self.identity.agent_id.clone()),
                    access_token,
                    refresh_token: None,
                    expires_at: None,
                }))
            }
            AuthProfileKind::OAuth => {
                let Some(tokens) = profile.token_set else {
                    return Ok(None);
                };
                let agent_id = profile
                    .metadata
                    .get("agent_id")
                    .cloned()
                    .unwrap_or_else(|| self.identity.agent_id.clone());
                let expires_soon = tokens.expires_at.is_some_and(|expires_at| {
                    expires_at
                        <= Utc::now() + ChronoDuration::seconds(ACCESS_TOKEN_REFRESH_SKEW_SECS)
                });
                if expires_soon {
                    return self
                        .refresh_session(base_url, &profile.id, &agent_id, tokens)
                        .await
                        .map(Some);
                }
                if tokens.access_token.trim().is_empty() {
                    return Ok(None);
                }
                Ok(Some(ContextBookSession {
                    base_url: base_url.clone(),
                    agent_id,
                    access_token: tokens.access_token,
                    refresh_token: tokens.refresh_token,
                    expires_at: tokens.expires_at,
                }))
            }
        }
    }

    async fn refresh_session(
        &self,
        base_url: &Url,
        profile_id: &str,
        current_agent_id: &str,
        existing: TokenSet,
    ) -> Result<ContextBookSession, ContextBookClientError> {
        let refresh_token = existing
            .refresh_token
            .clone()
            .ok_or_else(|| self.auth_error("Context Book auth profile is missing refresh_token"))?;
        let token = match self
            .refresh_via_oauth2_token(base_url, &refresh_token)
            .await
        {
            Ok(token) => token,
            Err(error) if error.kind == ContextBookClientErrorKind::ContractViolation => {
                self.refresh_via_legacy_auth_refresh(base_url, current_agent_id, &refresh_token)
                    .await?
            }
            Err(error) => return Err(error),
        };
        let agent_id = token.resolved_agent_id(current_agent_id);

        let refreshed = TokenSet {
            access_token: token.access_token.clone(),
            refresh_token: token
                .refresh_token
                .clone()
                .or(existing.refresh_token.clone()),
            id_token: None,
            expires_at: token
                .expires_in
                .map(|seconds| Utc::now() + ChronoDuration::seconds(seconds.max(0))),
            token_type: token.token_type,
            scope: token.scope,
        };

        self.auth_store
            .update_profile(profile_id, |profile| {
                profile.kind = AuthProfileKind::OAuth;
                profile.token_set = Some(refreshed.clone());
                profile
                    .metadata
                    .insert("agent_id".to_string(), agent_id.clone());
                Ok(())
            })
            .await
            .map_err(|error| {
                self.unexpected_error(format!(
                    "failed to persist refreshed Context Book token: {error}"
                ))
            })?;

        Ok(ContextBookSession {
            base_url: base_url.clone(),
            agent_id,
            access_token: token.access_token,
            refresh_token: refreshed.refresh_token,
            expires_at: refreshed.expires_at,
        })
    }

    async fn bootstrap_session(
        &self,
        base_url: &Url,
    ) -> Result<ContextBookSession, ContextBookClientError> {
        let bootstrap_secret = self.bootstrap_secret.as_deref().ok_or_else(|| {
            self.auth_error(format!(
                "missing bootstrap secret env {}; set it before enabling Context Book bootstrap",
                self.resolved.bootstrap_secret_env_key
            ))
        })?;

        match self.connect(base_url, bootstrap_secret).await {
            Ok(token) => self.persist_token_response(base_url, token).await,
            Err(error) if error.kind == ContextBookClientErrorKind::AgentNotRegistered => {
                let wait = self.register_init(base_url, bootstrap_secret).await?;
                let token = self.complete_bootstrap_wait(base_url, wait).await?;
                self.persist_token_response(base_url, token).await
            }
            Err(error) if error.kind == ContextBookClientErrorKind::BootstrapApprovalRequired => {
                let token = self
                    .complete_bootstrap_wait(
                        base_url,
                        self.connect_wait(base_url, bootstrap_secret).await?,
                    )
                    .await?;
                self.persist_token_response(base_url, token).await
            }
            Err(error) => Err(error),
        }
    }

    async fn connect(
        &self,
        base_url: &Url,
        bootstrap_secret: &str,
    ) -> Result<TokenResponse, ContextBookClientError> {
        let url = base_url
            .join("agents/connect")
            .map_err(|error| self.contract_error(format!("invalid connect URL: {error}")))?;
        let response = self
            .http_client
            .post(url)
            .header("X-Context-Book-Bootstrap-Secret", bootstrap_secret)
            .json(&json!({ "agentId": &self.identity.agent_id }))
            .send()
            .await
            .map_err(|error| {
                self.network_error(format!("failed to call POST /agents/connect: {error}"))
            })?;

        if response.status().is_success() {
            return response.json::<TokenResponse>().await.map_err(|error| {
                self.contract_error(format!("failed to parse connect token response: {error}"))
            });
        }

        Err(self.parse_http_error(response).await)
    }

    async fn connect_wait(
        &self,
        base_url: &Url,
        bootstrap_secret: &str,
    ) -> Result<BootstrapWaitResponse, ContextBookClientError> {
        let url = base_url
            .join("agents/connect")
            .map_err(|error| self.contract_error(format!("invalid connect URL: {error}")))?;
        let response = self
            .http_client
            .post(url)
            .header("X-Context-Book-Bootstrap-Secret", bootstrap_secret)
            .json(&json!({ "agentId": &self.identity.agent_id }))
            .send()
            .await
            .map_err(|error| {
                self.network_error(format!(
                    "failed to call POST /agents/connect for wait metadata: {error}"
                ))
            })?;
        if response.status() != StatusCode::FORBIDDEN {
            return Err(self.parse_http_error(response).await);
        }

        let body = response
            .json::<BootstrapWaitResponse>()
            .await
            .map_err(|error| {
                self.contract_error(format!("failed to parse connect wait metadata: {error}"))
            })?;
        Ok(body)
    }

    async fn register_init(
        &self,
        base_url: &Url,
        bootstrap_secret: &str,
    ) -> Result<BootstrapWaitResponse, ContextBookClientError> {
        let url = base_url
            .join("bootstrap/register/init")
            .map_err(|error| self.contract_error(format!("invalid register/init URL: {error}")))?;
        let response = self
            .http_client
            .post(url)
            .header("X-Context-Book-Bootstrap-Secret", bootstrap_secret)
            .json(&json!({
                "agentName": &self.identity.agent_name,
                "deviceType": &self.identity.device_type,
                "displayName": &self.identity.display_name,
            }))
            .send()
            .await
            .map_err(|error| {
                self.network_error(format!(
                    "failed to call POST /bootstrap/register/init: {error}"
                ))
            })?;

        if response.status().is_success() || response.status() == StatusCode::ACCEPTED {
            return response
                .json::<BootstrapWaitResponse>()
                .await
                .map_err(|error| {
                    self.contract_error(format!("failed to parse register/init response: {error}"))
                });
        }

        if response.status() == StatusCode::FORBIDDEN {
            return response
                .json::<BootstrapWaitResponse>()
                .await
                .map_err(|error| {
                    self.contract_error(format!(
                        "failed to parse register/init wait response: {error}"
                    ))
                });
        }

        Err(self.parse_http_error(response).await)
    }

    async fn complete_bootstrap_wait(
        &self,
        base_url: &Url,
        wait: BootstrapWaitResponse,
    ) -> Result<TokenResponse, ContextBookClientError> {
        let request_id = wait
            .request_id()
            .map(ToOwned::to_owned)
            .ok_or_else(|| self.contract_error("bootstrap wait metadata missing requestId"))?;
        let wait_token = wait
            .wait_token()
            .map(ToOwned::to_owned)
            .ok_or_else(|| self.contract_error("bootstrap wait metadata missing waitToken"))?;
        let status_path = wait
            .status_url()
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("/bootstrap/requests/{request_id}"));
        let complete_path = wait
            .complete_url()
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| "/bootstrap/register/complete".to_string());

        let mut approval_state = wait.approval_state().map(ToOwned::to_owned);
        while !matches_terminal_or_approved(approval_state.as_deref()) {
            let status_url = join_relative_url(base_url, &status_path).map_err(|error| {
                self.contract_error(format!("invalid bootstrap status URL: {error}"))
            })?;
            let response = self
                .http_client
                .get(status_url)
                .header("X-Context-Book-Bootstrap-Wait-Token", &wait_token)
                .query(&[("waitMs", REQUEST_WAIT_MS)])
                .send()
                .await
                .map_err(|error| {
                    self.network_error(format!("failed to wait on bootstrap request: {error}"))
                })?;
            let response = self
                .expect_success(response, "failed while waiting on bootstrap request")
                .await?;
            let body = response
                .json::<BootstrapWaitResponse>()
                .await
                .map_err(|error| {
                    self.contract_error(format!("failed to parse bootstrap wait status: {error}"))
                })?;
            approval_state = body
                .approval_state
                .or_else(|| body.request.and_then(|request| request.approval_state));
        }

        match approval_state.as_deref() {
            Some("Denied") => {
                return Err(ContextBookClientError {
                    kind: ContextBookClientErrorKind::BootstrapDenied,
                    message: "bootstrap request denied".to_string(),
                });
            }
            Some("Expired") => {
                return Err(ContextBookClientError {
                    kind: ContextBookClientErrorKind::BootstrapExpired,
                    message: "bootstrap request expired".to_string(),
                });
            }
            _ => {}
        }

        let complete_url = join_relative_url(base_url, &complete_path).map_err(|error| {
            self.contract_error(format!("invalid bootstrap complete URL: {error}"))
        })?;
        let response = self
            .http_client
            .post(complete_url)
            .header("X-Context-Book-Bootstrap-Wait-Token", wait_token)
            .json(&json!({ "requestId": request_id }))
            .send()
            .await
            .map_err(|error| {
                self.network_error(format!("failed to complete bootstrap request: {error}"))
            })?;
        let response = self
            .expect_success(response, "failed to complete bootstrap request")
            .await?;
        response.json::<TokenResponse>().await.map_err(|error| {
            self.contract_error(format!(
                "failed to parse bootstrap complete token response: {error}"
            ))
        })
    }

    async fn persist_token_response(
        &self,
        base_url: &Url,
        token: TokenResponse,
    ) -> Result<ContextBookSession, ContextBookClientError> {
        let profile_name = self.resolved.auth_profile.as_deref().unwrap_or("default");
        let agent_id = token.resolved_agent_id(&self.identity.agent_id);
        let tokens = TokenSet {
            access_token: token.access_token.clone(),
            refresh_token: token.refresh_token.clone(),
            id_token: None,
            expires_at: token
                .expires_in
                .map(|seconds| Utc::now() + ChronoDuration::seconds(seconds.max(0))),
            token_type: token.token_type.clone(),
            scope: token.scope.clone(),
        };
        let mut metadata = BTreeMap::new();
        metadata.insert("agent_id".to_string(), agent_id.clone());
        metadata.insert("base_url".to_string(), base_url.to_string());
        self.auth_store
            .upsert_profile(
                crate::auth::profiles::AuthProfile {
                    id: crate::auth::profiles::profile_id(CONTEXT_BOOK_PROVIDER, profile_name),
                    provider: CONTEXT_BOOK_PROVIDER.to_string(),
                    profile_name: profile_name.to_string(),
                    kind: AuthProfileKind::OAuth,
                    account_id: None,
                    workspace_id: None,
                    token_set: Some(tokens.clone()),
                    token: None,
                    metadata,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                },
                true,
            )
            .await
            .map_err(|error| {
                self.unexpected_error(format!(
                    "failed to persist Context Book auth profile: {error}"
                ))
            })?;

        Ok(ContextBookSession {
            base_url: base_url.clone(),
            agent_id,
            access_token: token.access_token,
            refresh_token: token.refresh_token,
            expires_at: tokens.expires_at,
        })
    }

    async fn expect_success(
        &self,
        response: Response,
        context: &str,
    ) -> Result<Response, ContextBookClientError> {
        if response.status().is_success() {
            return Ok(response);
        }
        let error = self.parse_http_error(response).await;
        Err(ContextBookClientError {
            kind: error.kind,
            message: format!("{context}: {}", error.message),
        })
    }

    async fn parse_http_error(&self, response: Response) -> ContextBookClientError {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        let parsed = serde_json::from_str::<ContextBookErrorEnvelope>(&body).ok();
        let wait = serde_json::from_str::<BootstrapWaitResponse>(&body).ok();
        let code = parsed
            .as_ref()
            .map(|body| body.error.code.clone())
            .unwrap_or_default();
        let message = parsed
            .as_ref()
            .map(|body| sanitize_sensitive_text(&body.error.message))
            .or_else(|| {
                wait.as_ref().and_then(|wait| {
                    wait.has_wait_metadata()
                        .then(|| "bootstrap approval required".to_string())
                })
            })
            .unwrap_or_else(|| {
                format!(
                    "unexpected Context Book response status {status}: {}",
                    sanitize_sensitive_text(&body)
                )
            });

        let kind = match (status, code.as_str()) {
            (StatusCode::UNAUTHORIZED, _) => ContextBookClientErrorKind::Unauthorized,
            (_, "BOOTSTRAP_APPROVAL_REQUIRED") => {
                ContextBookClientErrorKind::BootstrapApprovalRequired
            }
            (_, "BOOTSTRAP_REQUEST_DENIED") => ContextBookClientErrorKind::BootstrapDenied,
            (_, "BOOTSTRAP_REQUEST_EXPIRED") => ContextBookClientErrorKind::BootstrapExpired,
            (_, "AGENT_NOT_REGISTERED") => ContextBookClientErrorKind::AgentNotRegistered,
            (_, "CURSOR_NOT_FOUND") => ContextBookClientErrorKind::CursorNotFound,
            (StatusCode::FORBIDDEN, _)
                if wait
                    .as_ref()
                    .is_some_and(BootstrapWaitResponse::has_wait_metadata) =>
            {
                ContextBookClientErrorKind::BootstrapApprovalRequired
            }
            _ if status.is_server_error() => ContextBookClientErrorKind::Network,
            _ => ContextBookClientErrorKind::Unexpected,
        };

        ContextBookClientError { kind, message }
    }

    async fn resolve_base_url(&self) -> Result<Url, ContextBookClientError> {
        if let Some(manual_url) = self.resolved.manual_url.as_deref() {
            let url = Url::parse(manual_url).map_err(|error| {
                self.contract_error(format!("invalid Context Book base URL: {error}"))
            })?;
            validate_url_against_runtime_policy(
                &url,
                &self.resolved.allowed_hosts,
                self.resolved.allow_private_hosts,
            )
            .map_err(|error| self.contract_error(error.to_string()))?;
            return Ok(normalize_base_url(url));
        }

        if !self.resolved.discovery_enabled {
            return Err(ContextBookClientError {
                kind: ContextBookClientErrorKind::DiscoveryUnavailable,
                message: "Context Book discovery is disabled and context_book.manual_url is unset"
                    .to_string(),
            });
        }

        self.discover_base_url().await
    }

    async fn discover_base_url(&self) -> Result<Url, ContextBookClientError> {
        let mut candidates = discover_context_book_endpoints(&self.resolved.service_type)
            .await
            .map_err(|message| ContextBookClientError {
                kind: ContextBookClientErrorKind::DiscoveryUnavailable,
                message,
            })?;
        candidates.sort_by_key(discovery_sort_key);

        let mut failures = Vec::new();
        for candidate in candidates {
            if let Err(error) = validate_discovered_endpoint_against_runtime_policy(
                &candidate,
                &self.resolved.allowed_hosts,
                self.resolved.allow_private_hosts,
            ) {
                failures.push(format!(
                    "{} rejected by runtime policy: {error}",
                    candidate.label()
                ));
                continue;
            }

            let url = candidate
                .base_url()
                .map_err(|error| self.contract_error(format!("invalid discovered URL: {error}")))?;
            let preflight_url = candidate.preflight_url(&url).map_err(|error| {
                self.contract_error(format!("invalid discovered preflight URL: {error}"))
            })?;
            let response = self
                .http_client
                .get(preflight_url.clone())
                .send()
                .await
                .map_err(|error| {
                    self.network_error(format!(
                        "failed discovery preflight against {}: {error}",
                        preflight_url
                    ))
                });

            match response {
                Ok(response) if response.status().is_success() => {
                    return Ok(normalize_base_url(url));
                }
                Ok(response) => failures.push(format!(
                    "{} preflight returned {}",
                    candidate.label(),
                    response.status()
                )),
                Err(error) => failures.push(error.message),
            }
        }

        let details = if failures.is_empty() {
            "no compatible discovery candidates were found".to_string()
        } else {
            failures.join("; ")
        };
        Err(ContextBookClientError {
            kind: ContextBookClientErrorKind::DiscoveryUnavailable,
            message: format!(
                "failed to discover a reachable Context Book endpoint for {}: {details}",
                self.resolved.service_type
            ),
        })
    }

    fn auth_error(&self, message: impl Into<String>) -> ContextBookClientError {
        ContextBookClientError {
            kind: ContextBookClientErrorKind::AuthRequired,
            message: message.into(),
        }
    }

    fn contract_error(&self, message: impl Into<String>) -> ContextBookClientError {
        ContextBookClientError {
            kind: ContextBookClientErrorKind::ContractViolation,
            message: message.into(),
        }
    }

    fn network_error(&self, message: impl Into<String>) -> ContextBookClientError {
        ContextBookClientError {
            kind: ContextBookClientErrorKind::Network,
            message: message.into(),
        }
    }

    fn unexpected_error(&self, message: impl Into<String>) -> ContextBookClientError {
        ContextBookClientError {
            kind: ContextBookClientErrorKind::Unexpected,
            message: message.into(),
        }
    }
}

fn derive_identity(config: &Config) -> ContextBookAgentIdentity {
    let workspace_name = config
        .workspace_dir
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.trim().is_empty())
        .unwrap_or("zeroclaw")
        .to_string();
    let overrides = &config.context_book.agent_identity_override;
    let agent_id = overrides
        .agent_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| sanitize_identity_component(&workspace_name));
    let device_type = overrides
        .device_type
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| "unknown".to_string());
    let display_name = overrides
        .display_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| workspace_name.clone());

    ContextBookAgentIdentity {
        agent_id: agent_id.clone(),
        agent_name: agent_id,
        device_type,
        display_name,
    }
}

fn sanitize_identity_component(value: &str) -> String {
    let normalized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    normalized
        .trim_matches('-')
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

fn join_relative_url(base_url: &Url, candidate: &str) -> Result<Url> {
    if candidate.starts_with("http://") || candidate.starts_with("https://") {
        return Url::parse(candidate).context("invalid absolute URL");
    }
    let path = candidate.trim_start_matches('/');
    base_url.join(path).context("invalid relative URL")
}

fn normalize_base_url(mut url: Url) -> Url {
    let path = url.path().trim_end_matches('/').to_string();
    if path.is_empty() {
        url.set_path("/");
    } else {
        url.set_path(&format!("{path}/"));
    }
    url
}

fn discovery_sort_key(endpoint: &DiscoveredContextBookEndpoint) -> (i64, i64, String) {
    (
        endpoint.priority,
        -endpoint.weight,
        endpoint
            .instance_id
            .clone()
            .unwrap_or_else(|| endpoint.label()),
    )
}

async fn discover_context_book_endpoints(
    service_type: &str,
) -> std::result::Result<Vec<DiscoveredContextBookEndpoint>, String> {
    let browse_service_type = normalize_discovery_service_type(service_type);
    let mut command = Command::new("avahi-browse");
    command
        .arg("-rtp")
        .arg(&browse_service_type)
        .stdin(Stdio::null())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped());
    let output = timeout(Duration::from_secs(DISCOVERY_TIMEOUT_SECS), command.output())
        .await
        .map_err(|_| {
            format!(
                "timed out after {DISCOVERY_TIMEOUT_SECS}s waiting for avahi-browse on {browse_service_type}"
            )
        })?
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                "avahi-browse not found in PATH; install Avahi or configure context_book.manual_url".to_string()
            } else {
                format!("failed to execute avahi-browse: {error}")
            }
        })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(if stderr.is_empty() {
            format!("avahi-browse exited with status {}", output.status)
        } else {
            format!(
                "avahi-browse exited with status {}: {stderr}",
                output.status
            )
        });
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut endpoints = stdout
        .lines()
        .filter_map(parse_discovered_endpoint_line)
        .filter(is_compatible_discovery_candidate)
        .collect::<Vec<_>>();
    endpoints.sort_by_key(discovery_sort_key);
    if endpoints.is_empty() {
        return Err(format!(
            "no compatible Context Book DNS-SD advertisements found for {browse_service_type}"
        ));
    }
    Ok(endpoints)
}

fn normalize_discovery_service_type(service_type: &str) -> String {
    let trimmed = service_type.trim().trim_end_matches('.');
    let without_local = trimmed.strip_suffix(".local").unwrap_or(trimmed);
    if without_local.is_empty() {
        "_contextbook._tcp".to_string()
    } else {
        without_local.to_string()
    }
}

fn parse_discovered_endpoint_line(line: &str) -> Option<DiscoveredContextBookEndpoint> {
    if !line.starts_with("=;") {
        return None;
    }

    let fields = line.splitn(10, ';').collect::<Vec<_>>();
    if fields.len() < 9 {
        return None;
    }

    let hostname = fields.get(6)?.trim();
    let address = fields.get(7)?.trim();
    let port = fields.get(8)?.trim().parse::<u16>().ok()?;
    let txt = fields.get(9).copied().unwrap_or_default();
    let metadata = parse_discovery_txt(txt);
    let path = normalize_discovery_path(metadata.get("path").map(String::as_str).unwrap_or("/"));
    let health_path = metadata
        .get("health")
        .map(|value| normalize_discovery_path(value));

    Some(DiscoveredContextBookEndpoint {
        instance_name: fields.get(3)?.trim().to_string(),
        service_type: fields.get(4)?.trim().to_string(),
        domain: fields.get(5)?.trim().to_string(),
        hostname: hostname.to_string(),
        address: if address.is_empty() {
            hostname.to_string()
        } else {
            address.to_string()
        },
        port,
        path,
        health_path,
        instance_id: metadata.get("instance_id").cloned(),
        env: metadata.get("env").cloned(),
        priority: metadata
            .get("priority")
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or_default(),
        weight: metadata
            .get("weight")
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or_default(),
        metadata,
    })
}

fn parse_discovery_txt(raw: &str) -> BTreeMap<String, String> {
    let mut metadata = BTreeMap::new();
    let mut current = String::new();
    let mut in_quotes = false;

    for ch in raw.chars() {
        match ch {
            '"' if in_quotes => {
                if let Some((key, value)) = current.split_once('=') {
                    metadata.insert(key.trim().to_ascii_lowercase(), value.trim().to_string());
                }
                current.clear();
                in_quotes = false;
            }
            '"' => in_quotes = true,
            _ if in_quotes => current.push(ch),
            _ => {}
        }
    }

    metadata
}

fn normalize_discovery_path(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed == "/" {
        "/".to_string()
    } else if trimmed.starts_with('/') {
        trimmed.trim_end_matches('/').to_string()
    } else {
        format!("/{}", trimmed.trim_end_matches('/'))
    }
}

fn is_compatible_discovery_candidate(endpoint: &DiscoveredContextBookEndpoint) -> bool {
    if endpoint
        .metadata
        .get("service")
        .is_none_or(|service| service != "context-book")
    {
        return false;
    }

    if endpoint
        .metadata
        .get("ver")
        .is_none_or(|version| version != "1")
    {
        return false;
    }

    endpoint
        .metadata
        .get("api")
        .map(|value| {
            let mut tokens = value
                .split(',')
                .map(|token| token.trim().to_ascii_lowercase())
                .collect::<Vec<_>>();
            tokens.sort_unstable();
            tokens.contains(&"rest".to_string()) && tokens.contains(&"sse".to_string())
        })
        .unwrap_or(false)
}

fn validate_discovered_endpoint_against_runtime_policy(
    endpoint: &DiscoveredContextBookEndpoint,
    allowed_hosts: &[String],
    allow_private_hosts: bool,
) -> Result<()> {
    let hosts = [endpoint.address.as_str(), endpoint.hostname.as_str()];
    if !allowed_hosts.is_empty()
        && !hosts
            .iter()
            .filter(|host| !host.trim().is_empty())
            .any(|host| host_matches_allowlist(host, allowed_hosts))
    {
        anyhow::bail!(
            "discovered endpoint '{}' did not match context_book.allowed_hosts",
            endpoint.label()
        );
    }

    if !allow_private_hosts && is_private_like_host(&endpoint.address) {
        anyhow::bail!(
            "discovered endpoint '{}' resolved to private or loopback host '{}'",
            endpoint.label(),
            endpoint.address
        );
    }

    Ok(())
}

fn format_url_host(host: &str) -> String {
    match host.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V6(_)) => format!("[{host}]"),
        _ => host.to_string(),
    }
}

fn validate_url_against_runtime_policy(
    url: &Url,
    allowed_hosts: &[String],
    allow_private_hosts: bool,
) -> Result<()> {
    let host = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("Context Book URL must include a host"))?;
    if !allowed_hosts.is_empty() && !host_matches_allowlist(host, allowed_hosts) {
        anyhow::bail!("Context Book host '{host}' is not present in context_book.allowed_hosts");
    }
    if !allow_private_hosts && is_private_like_host(host) {
        anyhow::bail!(
            "Context Book host '{host}' is private or loopback; set context_book.allow_private_hosts = true to allow it"
        );
    }
    Ok(())
}

enum RefreshProbeKind {
    OAuth2Token,
    LegacyAuthRefresh { agent_id: String },
}

fn parse_agents_response(value: Value) -> anyhow::Result<Vec<ContextBookAgentStatusSnapshot>> {
    if value.is_array() {
        let agents = serde_json::from_value::<Vec<AgentStatusResponse>>(value)?;
        return Ok(agents
            .into_iter()
            .map(|agent| ContextBookAgentStatusSnapshot {
                agent_id: agent.agent_id,
                lifecycle_state: normalize_optional_string(agent.lifecycle_state.as_deref()),
                connection_state: normalize_optional_string(agent.connection_state.as_deref()),
            })
            .collect());
    }

    if let Some(items) = value.get("items") {
        return parse_agents_response(items.clone());
    }

    if value.is_object() {
        let agent = serde_json::from_value::<AgentStatusResponse>(value)?;
        return Ok(vec![ContextBookAgentStatusSnapshot {
            agent_id: agent.agent_id,
            lifecycle_state: normalize_optional_string(agent.lifecycle_state.as_deref()),
            connection_state: normalize_optional_string(agent.connection_state.as_deref()),
        }]);
    }

    anyhow::bail!("Context Book agents response did not contain a parseable agent list")
}

fn parse_agent_snapshots(value: Value) -> anyhow::Result<Vec<ContextBookAgentSnapshot>> {
    let synced_at = Utc::now().to_rfc3339();
    parse_list_response::<AgentStatusResponse>(value)?
        .into_iter()
        .map(|agent| {
            let raw_json = serde_json::to_value(&agent)?;
            Ok(ContextBookAgentSnapshot {
                agent_id: agent.agent_id,
                device_type: normalize_optional_string(agent.device_type.as_deref()),
                display_name: normalize_optional_string(agent.display_name.as_deref()),
                lifecycle_state: normalize_optional_string(agent.lifecycle_state.as_deref()),
                connection_state: normalize_optional_string(agent.connection_state.as_deref()),
                last_seen_at: normalize_optional_string(agent.last_seen_at.as_deref()),
                created_at: normalize_optional_string(agent.created_at.as_deref()),
                updated_at: normalize_optional_string(agent.updated_at.as_deref()),
                raw_json,
                synced_at: synced_at.clone(),
            })
        })
        .collect()
}

fn parse_context_snapshots(value: Value) -> anyhow::Result<Vec<ContextBookContextSnapshot>> {
    let synced_at = Utc::now().to_rfc3339();
    parse_list_response::<ContextRecordResponse>(value)?
        .into_iter()
        .map(|context| {
            let raw_json = serde_json::to_value(&context)?;
            Ok(ContextBookContextSnapshot {
                context_id: context.context_id,
                author_agent_id: context.author_agent_id,
                title: context.title,
                contents: context.contents,
                tag: context.tag,
                status: context.status,
                created_at: normalize_optional_string(context.created_at.as_deref()),
                updated_at: normalize_optional_string(context.updated_at.as_deref()),
                raw_json,
                synced_at: synced_at.clone(),
            })
        })
        .collect()
}

fn parse_context_snapshot(value: Value) -> anyhow::Result<ContextBookContextSnapshot> {
    if let Some(context) = value.get("context") {
        return parse_context_snapshot(context.clone());
    }
    parse_context_snapshots(value)?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("Context Book context response was empty"))
}

fn parse_vote_snapshots(value: Value) -> anyhow::Result<Vec<ContextBookVoteSnapshot>> {
    let synced_at = Utc::now().to_rfc3339();
    parse_list_response::<VoteResponse>(value)?
        .into_iter()
        .map(|vote| {
            let raw_json = serde_json::to_value(&vote)?;
            Ok(ContextBookVoteSnapshot {
                vote_id: vote.vote_id,
                owner_agent_id: vote.owner_agent_id,
                vote_score: vote.vote_score,
                vote_context: vote.vote_context,
                voter_agent_ids: vote.voter_agent_ids,
                required_score: vote.required_score,
                executable: vote.executable,
                created_at: normalize_optional_string(vote.created_at.as_deref()),
                updated_at: normalize_optional_string(vote.updated_at.as_deref()),
                raw_json,
                synced_at: synced_at.clone(),
            })
        })
        .collect()
}

fn parse_vote_snapshot(value: Value) -> anyhow::Result<ContextBookVoteSnapshot> {
    if let Some(vote) = value.get("vote") {
        return parse_vote_snapshot(vote.clone());
    }
    parse_vote_snapshots(value)?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("Context Book vote response was empty"))
}

fn parse_list_response<T>(value: Value) -> anyhow::Result<Vec<T>>
where
    T: serde::de::DeserializeOwned,
{
    if value.is_array() {
        return Ok(serde_json::from_value(value)?);
    }

    if let Some(items) = value.get("items") {
        return parse_list_response(items.clone());
    }

    if value.is_object() {
        return Ok(vec![serde_json::from_value(value)?]);
    }

    anyhow::bail!("Context Book list response did not contain a parseable items array")
}

fn normalize_optional_string(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn sanitize_sensitive_text(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    if let Ok(mut json) = serde_json::from_str::<Value>(trimmed) {
        redact_sensitive_json(&mut json);
        return json.to_string();
    }

    match crate::security::LeakDetector::default().scan(trimmed) {
        crate::security::LeakResult::Detected { redacted, .. } => redacted,
        crate::security::LeakResult::Clean => trimmed.to_string(),
    }
}

fn redact_sensitive_json(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, value) in map.iter_mut() {
                if is_sensitive_payload_key(key) {
                    *value = Value::String("[REDACTED]".to_string());
                } else {
                    redact_sensitive_json(value);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_sensitive_json(item);
            }
        }
        _ => {}
    }
}

fn is_sensitive_payload_key(key: &str) -> bool {
    let normalized = key
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase();
    matches!(
        normalized.as_str(),
        "accesstoken"
            | "refreshtoken"
            | "idtoken"
            | "token"
            | "tokenvalue"
            | "secret"
            | "clientsecret"
            | "bootstrapsecret"
            | "bootstrapsharedsecret"
            | "authorization"
            | "waittoken"
    )
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

fn dedup_degraded_modes(modes: &mut Vec<ContextBookDegradedMode>) {
    modes.sort_by_key(|mode| match mode {
        ContextBookDegradedMode::ReadOnly => 0_u8,
        ContextBookDegradedMode::NoRefresh => 1,
        ContextBookDegradedMode::NoWrite => 2,
        ContextBookDegradedMode::Disconnect => 3,
    });
    modes.dedup();
}

fn host_matches_allowlist(host: &str, allowlist: &[String]) -> bool {
    let normalized = host.trim().to_ascii_lowercase();
    allowlist.iter().any(|pattern| {
        pattern == "*"
            || *pattern == normalized
            || pattern.strip_prefix("*.").is_some_and(|suffix| {
                normalized == suffix || normalized.ends_with(&format!(".{suffix}"))
            })
    })
}

fn is_private_like_host(host: &str) -> bool {
    let normalized = host.trim().trim_end_matches('.').to_ascii_lowercase();
    if matches!(normalized.as_str(), "localhost" | "localhost.localdomain")
        || normalized.ends_with(".localhost")
        || normalized.ends_with(".local")
    {
        return true;
    }
    let Ok(ip) = normalized.parse::<std::net::IpAddr>() else {
        return false;
    };
    match ip {
        std::net::IpAddr::V4(v4) => {
            v4.is_private() || v4.is_loopback() || v4.is_link_local() || v4.is_unspecified()
        }
        std::net::IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_unique_local()
                || v6.is_unicast_link_local()
        }
    }
}

fn matches_terminal_or_approved(state: Option<&str>) -> bool {
    matches!(state, Some("Approved" | "Completed" | "Denied" | "Expired"))
}

fn session_recovery_fallback_allowed(kind: ContextBookClientErrorKind) -> bool {
    matches!(
        kind,
        ContextBookClientErrorKind::AuthRequired
            | ContextBookClientErrorKind::Unauthorized
            | ContextBookClientErrorKind::ContractViolation
            | ContextBookClientErrorKind::Unexpected
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use axum::{
        Router,
        extract::{Path as AxumPath, State},
        http::{HeaderMap, StatusCode},
        response::IntoResponse,
        routing::{get, patch, post},
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration as StdDuration;
    use tempfile::TempDir;
    use tokio::net::TcpListener;

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
        config.context_book.discovery_enabled = false;
        config.context_book.manual_url = Some("http://context.example:8080".into());
        config.context_book.allowed_hosts = vec!["context.example".into()];
        config
    }

    #[test]
    fn derive_identity_uses_workspace_name_by_default() {
        let tmp = TempDir::new().expect("temp dir");
        let config = test_config(&tmp);

        let identity = derive_identity(&config);

        assert_eq!(identity.agent_id, "workspace");
        assert_eq!(identity.device_type, "unknown");
        assert_eq!(identity.display_name, "workspace");
    }

    #[tokio::test]
    async fn base_url_validation_rejects_unlisted_host() {
        let tmp = TempDir::new().expect("temp dir");
        let mut config = test_config(&tmp);
        config.context_book.allowed_hosts = vec!["other.example".into()];

        let client = ContextBookClient::new(&config);
        let error = client
            .resolve_base_url()
            .await
            .expect_err("expected allowlist error");

        assert_eq!(error.kind, ContextBookClientErrorKind::ContractViolation);
        assert!(error.message.contains("allowed_hosts"));
    }

    #[tokio::test]
    async fn client_bootstraps_via_register_wait_complete_flow() {
        #[derive(Clone)]
        struct AppState {
            connect_calls: Arc<AtomicUsize>,
        }

        async fn connect(State(state): State<AppState>) -> impl IntoResponse {
            state.connect_calls.fetch_add(1, Ordering::SeqCst);
            (
                StatusCode::NOT_FOUND,
                axum::Json(json!({
                    "error": { "code": "AGENT_NOT_REGISTERED", "message": "missing agent" }
                })),
            )
        }

        async fn register_init(headers: HeaderMap) -> impl IntoResponse {
            assert_eq!(
                headers
                    .get("x-context-book-bootstrap-secret")
                    .and_then(|value| value.to_str().ok()),
                Some("bootstrap-secret")
            );
            (
                StatusCode::ACCEPTED,
                axum::Json(json!({
                    "request": {
                        "requestId": "req-1",
                        "waitToken": "wait-1",
                        "statusUrl": "/bootstrap/requests/req-1",
                        "completeUrl": "/bootstrap/register/complete",
                        "approvalState": "Pending"
                    },
                    "nextAction": "wait"
                })),
            )
        }

        async fn request_status(AxumPath(request_id): AxumPath<String>) -> impl IntoResponse {
            assert_eq!(request_id, "req-1");
            axum::Json(json!({
                "request": {
                    "requestId": "req-1",
                    "approvalState": "Approved"
                },
                "nextAction": "complete"
            }))
        }

        async fn register_complete(headers: HeaderMap) -> impl IntoResponse {
            assert_eq!(
                headers
                    .get("x-context-book-bootstrap-wait-token")
                    .and_then(|value| value.to_str().ok()),
                Some("wait-1")
            );
            axum::Json(json!({
                "agent": {
                    "agentId": "workspace-remote"
                },
                "access_token": "access-1",
                "refresh_token": "refresh-1",
                "expires_in": 1800
            }))
        }

        let state = AppState {
            connect_calls: Arc::new(AtomicUsize::new(0)),
        };
        let app = Router::new()
            .route("/agents/connect", post(connect))
            .route("/bootstrap/register/init", post(register_init))
            .route("/bootstrap/requests/{request_id}", get(request_status))
            .route("/bootstrap/register/complete", post(register_complete))
            .with_state(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve axum");
        });

        let tmp = TempDir::new().expect("temp dir");
        let mut config = test_config(&tmp);
        config.context_book.manual_url = Some(format!("http://{addr}"));
        config.context_book.allowed_hosts = vec!["127.0.0.1".into()];
        config.context_book.allow_private_hosts = true;
        let _guard = EnvGuard::set(
            &config.context_book.bootstrap_secret_env_key,
            Some("bootstrap-secret"),
        );

        let client = ContextBookClient::new(&config);
        let session = client.ensure_session().await.expect("bootstrap session");

        assert_eq!(session.access_token, "access-1");
        assert_eq!(session.refresh_token.as_deref(), Some("refresh-1"));
        assert_eq!(session.agent_id, "workspace-remote");
        assert_eq!(state.connect_calls.load(Ordering::SeqCst), 1);

        let stored = client
            .auth_service
            .get_profile(CONTEXT_BOOK_PROVIDER, None)
            .await
            .expect("load stored profile")
            .expect("stored profile");
        assert_eq!(stored.provider, CONTEXT_BOOK_PROVIDER);
        assert_eq!(
            stored.metadata.get("agent_id").map(String::as_str),
            Some("workspace-remote")
        );
        assert_eq!(
            stored
                .token_set
                .as_ref()
                .map(|tokens| tokens.access_token.as_str()),
            Some("access-1")
        );

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn client_handles_connect_reapproval_wait_flow() {
        async fn connect() -> impl IntoResponse {
            (
                StatusCode::FORBIDDEN,
                axum::Json(json!({
                    "request": {
                        "requestId": "req-reapprove",
                        "waitToken": "wait-reapprove",
                        "statusUrl": "/bootstrap/requests/req-reapprove",
                        "completeUrl": "/bootstrap/register/complete",
                        "approvalState": "Pending"
                    },
                    "nextAction": "wait"
                })),
            )
        }

        async fn request_status() -> impl IntoResponse {
            axum::Json(json!({
                "request": {
                    "requestId": "req-reapprove",
                    "approvalState": "Approved"
                },
                "nextAction": "complete"
            }))
        }

        async fn register_complete() -> impl IntoResponse {
            axum::Json(json!({
                "agent": {
                    "agentId": "remote-reapprove"
                },
                "access_token": "access-reapprove",
                "refresh_token": "refresh-reapprove",
                "expires_in": 600
            }))
        }

        let app = Router::new()
            .route("/agents/connect", post(connect))
            .route("/bootstrap/requests/req-reapprove", get(request_status))
            .route("/bootstrap/register/complete", post(register_complete));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve axum");
        });

        let tmp = TempDir::new().expect("temp dir");
        let mut config = test_config(&tmp);
        config.context_book.manual_url = Some(format!("http://{addr}"));
        config.context_book.allowed_hosts = vec!["127.0.0.1".into()];
        config.context_book.allow_private_hosts = true;
        let _guard = EnvGuard::set(
            &config.context_book.bootstrap_secret_env_key,
            Some("bootstrap-secret"),
        );

        let client = ContextBookClient::new(&config);
        let session = client.ensure_session().await.expect("reapproval session");

        assert_eq!(session.access_token, "access-reapprove");
        assert_eq!(session.refresh_token.as_deref(), Some("refresh-reapprove"));
        assert_eq!(session.agent_id, "remote-reapprove");

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn client_handles_legacy_top_level_connect_reapproval_wait_flow() {
        async fn connect() -> impl IntoResponse {
            (
                StatusCode::FORBIDDEN,
                axum::Json(json!({
                    "requestId": "req-legacy",
                    "waitToken": "wait-legacy",
                    "statusUrl": "/bootstrap/requests/req-legacy",
                    "completeUrl": "/bootstrap/register/complete",
                    "nextAction": "wait"
                })),
            )
        }

        async fn request_status() -> impl IntoResponse {
            axum::Json(json!({
                "request": {
                    "requestId": "req-legacy",
                    "approvalState": "Approved"
                },
                "nextAction": "complete"
            }))
        }

        async fn register_complete() -> impl IntoResponse {
            axum::Json(json!({
                "agent": {
                    "agentId": "remote-legacy"
                },
                "access_token": "access-legacy",
                "refresh_token": "refresh-legacy",
                "expires_in": 600
            }))
        }

        let app = Router::new()
            .route("/agents/connect", post(connect))
            .route("/bootstrap/requests/req-legacy", get(request_status))
            .route("/bootstrap/register/complete", post(register_complete));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve axum");
        });

        let tmp = TempDir::new().expect("temp dir");
        let mut config = test_config(&tmp);
        config.context_book.manual_url = Some(format!("http://{addr}"));
        config.context_book.allowed_hosts = vec!["127.0.0.1".into()];
        config.context_book.allow_private_hosts = true;
        let _guard = EnvGuard::set(
            &config.context_book.bootstrap_secret_env_key,
            Some("bootstrap-secret"),
        );

        let client = ContextBookClient::new(&config);
        let session = client
            .ensure_session()
            .await
            .expect("legacy reapproval session");

        assert_eq!(session.access_token, "access-legacy");
        assert_eq!(session.refresh_token.as_deref(), Some("refresh-legacy"));
        assert_eq!(session.agent_id, "remote-legacy");

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn client_validates_contract_and_detects_legacy_refresh_mode() {
        async fn agents() -> impl IntoResponse {
            axum::Json(json!([
                {
                    "agentId": "workspace",
                    "lifecycleState": "Active",
                    "connectionState": "Disconnected"
                }
            ]))
        }

        async fn subscriptions() -> impl IntoResponse {
            axum::Json(json!({
                "consumerAgentId": "workspace",
                "desiredProducerAgentIds": ["peer-a", "peer-b"],
                "effectiveProducerAgentIds": ["peer-a"]
            }))
        }

        async fn events() -> impl IntoResponse {
            (
                StatusCode::CONFLICT,
                axum::Json(json!({
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
                axum::Json(json!({
                    "error": {
                        "code": "REFRESH_TOKEN_REQUIRED",
                        "message": "missing refresh token"
                    }
                })),
            )
        }

        async fn delete_vote_probe() -> impl IntoResponse {
            (
                StatusCode::NOT_FOUND,
                axum::Json(json!({
                    "error": {
                        "code": "VOTE_NOT_FOUND",
                        "message": "missing vote"
                    }
                })),
            )
        }

        let app = Router::new()
            .route("/agents", get(agents))
            .route("/subscriptions", get(subscriptions))
            .route("/events", get(events))
            .route("/votes/{vote_id}", axum::routing::delete(delete_vote_probe))
            .route("/auth/refresh", post(legacy_refresh));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve axum");
        });

        let tmp = TempDir::new().expect("temp dir");
        let mut config = test_config(&tmp);
        config.context_book.manual_url = Some(format!("http://{addr}"));
        config.context_book.allowed_hosts = vec!["127.0.0.1".into()];
        config.context_book.allow_private_hosts = true;

        let client = ContextBookClient::new(&config);
        let session = ContextBookSession {
            base_url: Url::parse(&format!("http://{addr}/")).expect("base URL should parse"),
            agent_id: "workspace".into(),
            access_token: "access-token".into(),
            refresh_token: Some("refresh-token".into()),
            expires_at: None,
        };
        let contract = client
            .validate_runtime_contract(&session)
            .await
            .expect("contract validation");

        assert_eq!(
            contract.validation_state,
            ContextBookContractValidationState::Validated
        );
        assert_eq!(contract.lifecycle_connection_split, Some(true));
        assert_eq!(contract.subscriptions_desired_effective_split, Some(true));
        assert_eq!(contract.cursor_not_found_returns_409, Some(true));
        assert_eq!(contract.vote_deleted_supported, Some(true));
        assert_eq!(
            contract.refresh_mode,
            ContextBookRefreshMode::LegacyAuthRefresh
        );
        assert!(contract.degraded_modes.is_empty());

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn client_marks_contract_no_write_when_vote_delete_probe_route_is_missing() {
        async fn agents() -> impl IntoResponse {
            axum::Json(json!([
                {
                    "agentId": "workspace",
                    "lifecycleState": "Active",
                    "connectionState": "Disconnected"
                }
            ]))
        }

        async fn subscriptions() -> impl IntoResponse {
            axum::Json(json!({
                "consumerAgentId": "workspace",
                "desiredProducerAgentIds": ["peer-a"],
                "effectiveProducerAgentIds": ["peer-a"]
            }))
        }

        async fn events() -> impl IntoResponse {
            (
                StatusCode::CONFLICT,
                axum::Json(json!({
                    "error": {
                        "code": "CURSOR_NOT_FOUND",
                        "message": "missing cursor"
                    }
                })),
            )
        }

        let app = Router::new()
            .route("/agents", get(agents))
            .route("/subscriptions", get(subscriptions))
            .route("/events", get(events));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve axum");
        });

        let tmp = TempDir::new().expect("temp dir");
        let mut config = test_config(&tmp);
        config.context_book.manual_url = Some(format!("http://{addr}"));
        config.context_book.allowed_hosts = vec!["127.0.0.1".into()];
        config.context_book.allow_private_hosts = true;

        let client = ContextBookClient::new(&config);
        let session = ContextBookSession {
            base_url: Url::parse(&format!("http://{addr}/")).expect("base URL should parse"),
            agent_id: "workspace".into(),
            access_token: "access-token".into(),
            refresh_token: Some("refresh-token".into()),
            expires_at: None,
        };
        let contract = client
            .validate_runtime_contract(&session)
            .await
            .expect("contract validation");

        assert_eq!(
            contract.validation_state,
            ContextBookContractValidationState::Degraded
        );
        assert_eq!(contract.vote_deleted_supported, Some(false));
        assert!(
            contract
                .degraded_modes
                .contains(&ContextBookDegradedMode::NoWrite)
        );
        assert!(
            contract
                .notes
                .iter()
                .any(|note| note.contains("vote delete"))
        );

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn client_redacts_sensitive_values_from_http_errors() {
        async fn votes() -> impl IntoResponse {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({
                    "access_token": "error-access-token",
                    "refreshToken": "error-refresh-token",
                    "bootstrapSecret": "bootstrap-secret",
                    "message": "raw upstream failure"
                })),
            )
        }

        let app = Router::new().route("/votes", get(votes));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve axum");
        });

        let tmp = TempDir::new().expect("temp dir");
        let mut config = test_config(&tmp);
        config.context_book.manual_url = Some(format!("http://{addr}"));
        config.context_book.allowed_hosts = vec!["127.0.0.1".into()];
        config.context_book.allow_private_hosts = true;

        let client = ContextBookClient::new(&config);
        let session = ContextBookSession {
            base_url: Url::parse(&format!("http://{addr}/")).expect("base URL should parse"),
            agent_id: "workspace".into(),
            access_token: "access-token".into(),
            refresh_token: Some("refresh-token".into()),
            expires_at: None,
        };
        let error = client
            .get_votes(&session)
            .await
            .expect_err("get votes should fail");

        assert_eq!(error.kind, ContextBookClientErrorKind::Network);
        assert!(!error.message.contains("error-access-token"));
        assert!(!error.message.contains("error-refresh-token"));
        assert!(!error.message.contains("bootstrap-secret"));
        assert!(error.message.contains("[REDACTED]"));

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn client_supports_context_and_vote_write_routes() {
        async fn create_context(axum::Json(body): axum::Json<Value>) -> impl IntoResponse {
            assert_eq!(body["contextId"], "workspace_ctx10");
            assert_eq!(body["title"], "Launch Plan");
            assert_eq!(body["contents"], "Ship it");
            assert_eq!(body["tag"], "eng");
            assert_eq!(body["status"], "Published");
            axum::Json(json!({
                "context": {
                    "contextId": "workspace_ctx10",
                    "authorAgentId": "workspace",
                    "title": "Launch Plan",
                    "contents": "Ship it",
                    "tag": "eng",
                    "status": "Published",
                    "createdAt": "2026-03-29T00:00:00Z",
                    "updatedAt": "2026-03-29T00:00:00Z"
                }
            }))
        }

        async fn update_context(
            AxumPath(context_id): AxumPath<String>,
            axum::Json(body): axum::Json<Value>,
        ) -> impl IntoResponse {
            assert_eq!(context_id, "workspace_ctx10");
            assert_eq!(body["contents"], "Ship it now");
            axum::Json(json!({
                "context": {
                    "contextId": "workspace_ctx10",
                    "authorAgentId": "workspace",
                    "title": "Launch Plan",
                    "contents": "Ship it now",
                    "tag": "eng",
                    "status": "Published",
                    "createdAt": "2026-03-29T00:00:00Z",
                    "updatedAt": "2026-03-29T00:00:05Z"
                }
            }))
        }

        async fn delete_context(AxumPath(context_id): AxumPath<String>) -> impl IntoResponse {
            assert_eq!(context_id, "workspace_ctx10");
            StatusCode::NO_CONTENT
        }

        async fn create_vote(axum::Json(body): axum::Json<Value>) -> impl IntoResponse {
            assert_eq!(body["voteId"], "workspace_vote10");
            assert_eq!(body["voteScore"], 1.0);
            assert_eq!(body["voteContext"], "approve launch");
            axum::Json(json!({
                "vote": {
                    "voteId": "workspace_vote10",
                    "ownerAgentId": "workspace",
                    "voteScore": 1,
                    "voteContext": "approve launch",
                    "voterAgentIds": ["workspace"],
                    "requiredScore": 2,
                    "executable": false,
                    "createdAt": "2026-03-29T00:00:00Z",
                    "updatedAt": "2026-03-29T00:00:00Z"
                }
            }))
        }

        async fn update_vote(
            AxumPath(vote_id): AxumPath<String>,
            axum::Json(body): axum::Json<Value>,
        ) -> impl IntoResponse {
            assert_eq!(vote_id, "workspace_vote10");
            assert_eq!(body["voteScore"], 2.0);
            axum::Json(json!({
                "vote": {
                    "voteId": "workspace_vote10",
                    "ownerAgentId": "workspace",
                    "voteScore": 2,
                    "voteContext": "approve launch",
                    "voterAgentIds": ["workspace"],
                    "requiredScore": 2,
                    "executable": true,
                    "createdAt": "2026-03-29T00:00:00Z",
                    "updatedAt": "2026-03-29T00:00:10Z"
                }
            }))
        }

        async fn cast_vote(
            AxumPath(vote_id): AxumPath<String>,
            axum::Json(body): axum::Json<Value>,
        ) -> impl IntoResponse {
            assert_eq!(vote_id, "workspace_vote10");
            assert_eq!(body["voteScore"], 1.0);
            axum::Json(json!({
                "vote": {
                    "voteId": "workspace_vote10",
                    "ownerAgentId": "workspace",
                    "voteScore": 3,
                    "voteContext": "approve launch",
                    "voterAgentIds": ["workspace", "peer-a"],
                    "requiredScore": 2,
                    "executable": true,
                    "createdAt": "2026-03-29T00:00:00Z",
                    "updatedAt": "2026-03-29T00:00:15Z"
                }
            }))
        }

        async fn delete_vote(AxumPath(vote_id): AxumPath<String>) -> impl IntoResponse {
            assert_eq!(vote_id, "workspace_vote10");
            StatusCode::NO_CONTENT
        }

        let app = Router::new()
            .route("/contexts", post(create_context))
            .route(
                "/contexts/{context_id}",
                patch(update_context).delete(delete_context),
            )
            .route("/votes", post(create_vote))
            .route("/votes/{vote_id}", patch(update_vote).delete(delete_vote))
            .route("/votes/{vote_id}/cast", post(cast_vote));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve axum");
        });

        let tmp = TempDir::new().expect("temp dir");
        let mut config = test_config(&tmp);
        config.context_book.manual_url = Some(format!("http://{addr}"));
        config.context_book.allowed_hosts = vec!["127.0.0.1".into()];
        config.context_book.allow_private_hosts = true;

        let client = ContextBookClient::new(&config);
        let session = ContextBookSession {
            base_url: Url::parse(&format!("http://{addr}/")).expect("base URL should parse"),
            agent_id: "workspace".into(),
            access_token: "access-token".into(),
            refresh_token: Some("refresh-token".into()),
            expires_at: None,
        };

        let created_context = client
            .create_context(
                &session,
                &ContextBookContextCreateRequest {
                    context_id: Some("workspace_ctx10".into()),
                    title: "Launch Plan".into(),
                    contents: "Ship it".into(),
                    tag: "eng".into(),
                    status: "Published".into(),
                },
            )
            .await
            .expect("create context");
        assert_eq!(created_context.context_id, "workspace_ctx10");

        let updated_context = client
            .update_context(
                &session,
                "workspace_ctx10",
                &ContextBookContextUpdateRequest {
                    contents: Some("Ship it now".into()),
                    ..ContextBookContextUpdateRequest::default()
                },
            )
            .await
            .expect("update context");
        assert_eq!(updated_context.contents, "Ship it now");
        client
            .delete_context(&session, "workspace_ctx10")
            .await
            .expect("delete context");

        let created_vote = client
            .create_vote(
                &session,
                &ContextBookVoteCreateRequest {
                    vote_id: Some("workspace_vote10".into()),
                    vote_score: Some(1.0),
                    vote_context: "approve launch".into(),
                },
            )
            .await
            .expect("create vote");
        assert_eq!(created_vote.vote_id, "workspace_vote10");

        let updated_vote = client
            .update_vote(
                &session,
                "workspace_vote10",
                &ContextBookVoteUpdateRequest {
                    vote_score: Some(2.0),
                    ..ContextBookVoteUpdateRequest::default()
                },
            )
            .await
            .expect("update vote");
        assert_eq!(updated_vote.vote_score, 2.0);

        let cast_vote = client
            .cast_vote(
                &session,
                "workspace_vote10",
                &ContextBookVoteCastRequest {
                    vote_score: Some(1.0),
                },
            )
            .await
            .expect("cast vote");
        assert_eq!(cast_vote.vote_score, 3.0);
        assert_eq!(cast_vote.voter_agent_ids, vec!["workspace", "peer-a"]);

        client
            .delete_vote(&session, "workspace_vote10")
            .await
            .expect("delete vote");

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    #[ignore = "requires a live Context Book server with dashboard approval access"]
    async fn live_client_bootstraps_against_context_book_server() {
        let shared_secret = std::env::var("CONTEXT_BOOK_BOOTSTRAP_SHARED_SECRET")
            .expect("set CONTEXT_BOOK_BOOTSTRAP_SHARED_SECRET to run the live test");
        let base_url = std::env::var("CONTEXT_BOOK_LIVE_BASE_URL")
            .unwrap_or_else(|_| "http://127.0.1.1:8080".to_string());
        let parsed_base_url = Url::parse(&base_url).expect("live base URL should parse");
        let host = parsed_base_url
            .host_str()
            .expect("live base URL should include a host")
            .to_string();
        let agent_id = format!(
            "zeroclaw-live-test-{}",
            Utc::now()
                .timestamp_nanos_opt()
                .expect("current timestamp should fit in i64")
        );

        let tmp = TempDir::new().expect("temp dir");
        let mut config = test_config(&tmp);
        config.context_book.manual_url = Some(base_url.clone());
        config.context_book.discovery_enabled = false;
        config.context_book.allowed_hosts = vec![host];
        config.context_book.allow_private_hosts = true;
        config.context_book.agent_identity_override.agent_id = Some(agent_id.clone());
        config.context_book.agent_identity_override.device_type = Some("notepc".to_string());
        config.context_book.agent_identity_override.display_name =
            Some("ZeroClaw Live Test".to_string());

        let _guard = EnvGuard::set(
            &config.context_book.bootstrap_secret_env_key,
            Some(shared_secret.as_str()),
        );
        let client = ContextBookClient::new(&config);
        let approval_http = reqwest::Client::new();
        let approval_agent_id = agent_id.clone();
        let approval_base = base_url.clone();
        let approval_task = tokio::spawn(async move {
            for _ in 0..40 {
                let queue = approval_http
                    .get(format!("{approval_base}/dashboard/api/bootstrap/requests"))
                    .send()
                    .await
                    .expect("bootstrap queue request");
                let body = queue
                    .json::<Value>()
                    .await
                    .expect("bootstrap queue JSON should parse");
                if let Some(request_id) = body["items"].as_array().and_then(|items| {
                    items.iter().find_map(|item| {
                        (item["requestedAgentName"].as_str() == Some(approval_agent_id.as_str()))
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
                            "reason": "live client bootstrap verification",
                            "channel": "dashboard",
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
                tokio::time::sleep(StdDuration::from_millis(500)).await;
            }
            panic!("timed out waiting for bootstrap request for {approval_agent_id}");
        });

        let session = tokio::time::timeout(StdDuration::from_secs(30), client.ensure_session())
            .await
            .expect("ensure_session should complete before timeout")
            .expect("live bootstrap session");
        approval_task
            .await
            .expect("bootstrap approval task should finish cleanly");

        assert!(!session.access_token.is_empty());
        assert!(session.refresh_token.is_some());

        client
            .activate_agent(&session)
            .await
            .expect("live agent activation");
        client
            .open_event_stream(&session, None)
            .await
            .expect("live event stream open");
        let contract = client
            .validate_runtime_contract(&session)
            .await
            .expect("live contract validation");
        assert_eq!(contract.lifecycle_connection_split, Some(true));
        assert_eq!(contract.subscriptions_desired_effective_split, Some(true));
        assert_eq!(contract.cursor_not_found_returns_409, Some(true));
        assert_ne!(contract.refresh_mode, ContextBookRefreshMode::Unknown);

        let stored = client
            .auth_service
            .get_profile(CONTEXT_BOOK_PROVIDER, None)
            .await
            .expect("load stored live profile")
            .expect("stored live profile");
        assert_eq!(stored.provider, CONTEXT_BOOK_PROVIDER);
        assert_eq!(
            stored
                .token_set
                .as_ref()
                .map(|tokens| tokens.access_token.as_str()),
            Some(session.access_token.as_str())
        );

        let agents = client.get_agents(&session).await.expect("live GET /agents");
        let contexts = client
            .get_contexts(&session)
            .await
            .expect("live GET /contexts");
        let votes = client.get_votes(&session).await.expect("live GET /votes");
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

    #[tokio::test]
    async fn client_refreshes_stored_oauth_profile_via_oauth2_token() {
        async fn refresh_token() -> impl IntoResponse {
            axum::Json(json!({
                "access_token": "fresh-access",
                "refresh_token": "fresh-refresh",
                "expires_in": 1800
            }))
        }

        let app = Router::new().route("/oauth2/token", post(refresh_token));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve axum");
        });

        let tmp = TempDir::new().expect("temp dir");
        let mut config = test_config(&tmp);
        config.context_book.manual_url = Some(format!("http://{addr}"));
        config.context_book.allowed_hosts = vec!["127.0.0.1".into()];
        config.context_book.allow_private_hosts = true;

        let state_dir = state_dir_from_config(&config);
        let store = AuthProfilesStore::new(&state_dir, config.secrets.encrypt);
        store
            .upsert_profile(
                crate::auth::profiles::AuthProfile {
                    id: crate::auth::profiles::profile_id(CONTEXT_BOOK_PROVIDER, "default"),
                    provider: CONTEXT_BOOK_PROVIDER.to_string(),
                    profile_name: "default".to_string(),
                    kind: AuthProfileKind::OAuth,
                    account_id: None,
                    workspace_id: None,
                    token_set: Some(TokenSet {
                        access_token: "stale-access".into(),
                        refresh_token: Some("stale-refresh".into()),
                        id_token: None,
                        expires_at: Some(Utc::now() - ChronoDuration::minutes(5)),
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
            .expect("seed auth profile");

        let client = ContextBookClient::new(&config);
        let session = client.ensure_session().await.expect("refresh session");

        assert_eq!(session.access_token, "fresh-access");
        assert_eq!(session.refresh_token.as_deref(), Some("fresh-refresh"));

        let stored = client
            .auth_service
            .get_profile(CONTEXT_BOOK_PROVIDER, None)
            .await
            .expect("load refreshed profile")
            .expect("refreshed profile");
        assert_eq!(
            stored
                .token_set
                .as_ref()
                .map(|tokens| tokens.access_token.as_str()),
            Some("fresh-access")
        );

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn client_refreshes_stored_oauth_profile_via_legacy_auth_refresh_when_oauth2_missing() {
        async fn legacy_refresh() -> impl IntoResponse {
            axum::Json(json!({
                "access_token": "legacy-access",
                "refresh_token": "legacy-refresh",
                "expires_in": 1800
            }))
        }

        let app = Router::new().route("/auth/refresh", post(legacy_refresh));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve axum");
        });

        let tmp = TempDir::new().expect("temp dir");
        let mut config = test_config(&tmp);
        config.context_book.manual_url = Some(format!("http://{addr}"));
        config.context_book.allowed_hosts = vec!["127.0.0.1".into()];
        config.context_book.allow_private_hosts = true;

        let state_dir = state_dir_from_config(&config);
        let store = AuthProfilesStore::new(&state_dir, config.secrets.encrypt);
        store
            .upsert_profile(
                crate::auth::profiles::AuthProfile {
                    id: crate::auth::profiles::profile_id(CONTEXT_BOOK_PROVIDER, "default"),
                    provider: CONTEXT_BOOK_PROVIDER.to_string(),
                    profile_name: "default".to_string(),
                    kind: AuthProfileKind::OAuth,
                    account_id: None,
                    workspace_id: None,
                    token_set: Some(TokenSet {
                        access_token: "stale-access".into(),
                        refresh_token: Some("stale-refresh".into()),
                        id_token: None,
                        expires_at: Some(Utc::now() - ChronoDuration::minutes(5)),
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
            .expect("seed auth profile");

        let client = ContextBookClient::new(&config);
        let session = client
            .ensure_session()
            .await
            .expect("legacy refresh session");

        assert_eq!(session.access_token, "legacy-access");
        assert_eq!(session.refresh_token.as_deref(), Some("legacy-refresh"));

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn client_falls_back_to_bootstrap_when_refresh_fails() {
        async fn legacy_refresh() -> impl IntoResponse {
            (
                StatusCode::UNAUTHORIZED,
                axum::Json(json!({
                    "error": {
                        "code": "AUTH_INVALID_TOKEN",
                        "message": "Invalid or expired refresh token."
                    }
                })),
            )
        }

        async fn connect() -> impl IntoResponse {
            (
                StatusCode::NOT_FOUND,
                axum::Json(json!({
                    "error": {
                        "code": "AGENT_NOT_REGISTERED",
                        "message": "Agent is not registered."
                    }
                })),
            )
        }

        async fn register_init(
            headers: HeaderMap,
            axum::Json(body): axum::Json<Value>,
        ) -> impl IntoResponse {
            assert_eq!(
                headers
                    .get("x-context-book-bootstrap-secret")
                    .and_then(|value| value.to_str().ok()),
                Some("bootstrap-secret")
            );
            assert_eq!(body["agentName"], json!("workspace"));
            assert_eq!(body["deviceType"], json!("unknown"));
            assert_eq!(body["displayName"], json!("workspace"));
            (
                StatusCode::ACCEPTED,
                axum::Json(json!({
                    "request": {
                        "requestId": "bootreq-123",
                        "waitToken": "bootwait-123",
                        "approvalState": "Approved",
                        "statusUrl": "/bootstrap/requests/bootreq-123",
                        "completeUrl": "/bootstrap/register/complete"
                    }
                })),
            )
        }

        async fn register_complete(
            headers: HeaderMap,
            axum::Json(body): axum::Json<Value>,
        ) -> impl IntoResponse {
            assert_eq!(
                headers
                    .get("x-context-book-bootstrap-wait-token")
                    .and_then(|value| value.to_str().ok()),
                Some("bootwait-123")
            );
            assert_eq!(body["requestId"], json!("bootreq-123"));
            axum::Json(json!({
                "access_token": "bootstrap-access",
                "refresh_token": "bootstrap-refresh",
                "expires_in": 1800,
                "agentId": "workspace"
            }))
        }

        let app = Router::new()
            .route("/auth/refresh", post(legacy_refresh))
            .route("/agents/connect", post(connect))
            .route("/bootstrap/register/init", post(register_init))
            .route("/bootstrap/register/complete", post(register_complete));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve axum");
        });

        let tmp = TempDir::new().expect("temp dir");
        let mut config = test_config(&tmp);
        config.context_book.manual_url = Some(format!("http://{addr}"));
        config.context_book.allowed_hosts = vec!["127.0.0.1".into()];
        config.context_book.allow_private_hosts = true;
        config.context_book.bootstrap_secret_env_key = "ZER0CLAW_TEST_CONTEXT_BOOK_SECRET".into();

        let _env = EnvGuard::set(
            "ZER0CLAW_TEST_CONTEXT_BOOK_SECRET",
            Some("bootstrap-secret"),
        );

        let state_dir = state_dir_from_config(&config);
        let store = AuthProfilesStore::new(&state_dir, config.secrets.encrypt);
        store
            .upsert_profile(
                crate::auth::profiles::AuthProfile {
                    id: crate::auth::profiles::profile_id(CONTEXT_BOOK_PROVIDER, "default"),
                    provider: CONTEXT_BOOK_PROVIDER.to_string(),
                    profile_name: "default".to_string(),
                    kind: AuthProfileKind::OAuth,
                    account_id: None,
                    workspace_id: None,
                    token_set: Some(TokenSet {
                        access_token: "stale-access".into(),
                        refresh_token: Some("stale-refresh".into()),
                        id_token: None,
                        expires_at: Some(Utc::now() - ChronoDuration::minutes(5)),
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
            .expect("seed auth profile");

        let client = ContextBookClient::new(&config);
        let session = client
            .ensure_session()
            .await
            .expect("bootstrap fallback session");

        assert_eq!(session.access_token, "bootstrap-access");
        assert_eq!(session.refresh_token.as_deref(), Some("bootstrap-refresh"));

        let stored = client
            .auth_service
            .get_profile(CONTEXT_BOOK_PROVIDER, None)
            .await
            .expect("load bootstrap-fallback profile")
            .expect("stored bootstrap-fallback profile");
        assert_eq!(
            stored
                .token_set
                .as_ref()
                .map(|tokens| tokens.access_token.as_str()),
            Some("bootstrap-access")
        );

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn client_gets_and_sets_subscriptions() {
        async fn get_subscriptions() -> impl IntoResponse {
            axum::Json(json!({
                "consumerAgentId": "workspace",
                "desiredProducerAgentIds": ["peer-a"],
                "effectiveProducerAgentIds": ["peer-a"]
            }))
        }

        async fn put_subscriptions(axum::Json(body): axum::Json<Value>) -> impl IntoResponse {
            assert_eq!(body["desiredProducerAgentIds"], json!(["peer-a", "peer-b"]));
            assert_eq!(body["producerAgentIds"], json!(["peer-a", "peer-b"]));
            axum::Json(json!({
                "consumerAgentId": "workspace",
                "desiredProducerAgentIds": ["peer-a", "peer-b"],
                "effectiveProducerAgentIds": ["peer-a"]
            }))
        }

        let app = Router::new().route(
            "/subscriptions",
            get(get_subscriptions).put(put_subscriptions),
        );
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve axum");
        });

        let tmp = TempDir::new().expect("temp dir");
        let mut config = test_config(&tmp);
        config.context_book.manual_url = Some(format!("http://{addr}"));
        config.context_book.allowed_hosts = vec!["127.0.0.1".into()];
        config.context_book.allow_private_hosts = true;

        let client = ContextBookClient::new(&config);
        let session = ContextBookSession {
            base_url: Url::parse(&format!("http://{addr}/")).expect("base URL should parse"),
            agent_id: "workspace".into(),
            access_token: "access-token".into(),
            refresh_token: Some("refresh-token".into()),
            expires_at: None,
        };
        let initial = client
            .get_subscriptions(&session)
            .await
            .expect("get subscriptions");
        let updated = client
            .set_subscriptions(&session, &["peer-b".into(), "peer-a".into()])
            .await
            .expect("set subscriptions");

        assert_eq!(
            initial.desired_producer_agent_ids,
            vec!["peer-a".to_string()]
        );
        assert_eq!(
            updated.desired_producer_agent_ids,
            vec!["peer-a".to_string(), "peer-b".to_string()]
        );
        assert_eq!(
            updated.effective_producer_agent_ids,
            vec!["peer-a".to_string()]
        );

        server.abort();
        let _ = server.await;
    }
}
