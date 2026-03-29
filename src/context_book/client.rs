use super::config::ResolvedContextBookConfig;
use super::events::{ContextBookEventEnvelope, parse_polled_events};
use super::handle::{
    ContextBookContractSnapshot, ContextBookContractValidationState, ContextBookDegradedMode,
    ContextBookRefreshMode,
};
use super::store::ContextBookSubscriptionsSnapshot;
use crate::auth::profiles::{AuthProfileKind, AuthProfilesStore, TokenSet};
use crate::auth::{AuthService, state_dir_from_config};
use crate::config::Config;
use anyhow::{Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use reqwest::{Response, StatusCode, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use thiserror::Error;

const CONTEXT_BOOK_PROVIDER: &str = "context-book";
const ACCESS_TOKEN_REFRESH_SKEW_SECS: i64 = 90;
const REQUEST_WAIT_MS: u64 = 5_000;
const HTTP_TIMEOUT_SECS: u64 = 30;
const HTTP_CONNECT_TIMEOUT_SECS: u64 = 10;

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

#[derive(Debug, Clone, Deserialize)]
struct AgentStatusResponse {
    #[serde(default, alias = "agentId")]
    agent_id: String,
    #[serde(default, alias = "lifecycleState")]
    lifecycle_state: Option<String>,
    #[serde(default, alias = "connectionState")]
    connection_state: Option<String>,
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
        let base_url = self.resolve_base_url()?;
        if let Some(session) = self.load_stored_session(&base_url).await? {
            return Ok(session);
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

    pub async fn validate_runtime_contract(
        &self,
        session: &ContextBookSession,
    ) -> Result<ContextBookContractSnapshot, ContextBookClientError> {
        let current_agent = self.current_agent_status(session).await?;
        let subscriptions = self.fetch_subscriptions_response(session).await?;
        let cursor_not_found_returns_409 = self.probe_cursor_not_found_contract(session).await?;
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

        Ok(ContextBookContractSnapshot {
            validation_state,
            checked_at: Some(Utc::now().to_rfc3339()),
            lifecycle_connection_split,
            subscriptions_desired_effective_split: subscriptions_split,
            cursor_not_found_returns_409: Some(cursor_not_found_returns_409),
            vote_deleted_supported: None,
            refresh_mode,
            degraded_modes,
            notes,
        })
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
            .map(|body| body.error.message.clone())
            .or_else(|| {
                wait.as_ref().and_then(|wait| {
                    wait.has_wait_metadata()
                        .then(|| "bootstrap approval required".to_string())
                })
            })
            .unwrap_or_else(|| format!("unexpected Context Book response status {status}: {body}"));

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

    fn resolve_base_url(&self) -> Result<Url, ContextBookClientError> {
        let Some(manual_url) = self.resolved.manual_url.as_deref() else {
            return Err(ContextBookClientError {
                kind: ContextBookClientErrorKind::DiscoveryUnavailable,
                message: "Context Book discovery is not implemented yet; configure context_book.manual_url for Phase 2 connectivity".to_string(),
            });
        };
        let url = Url::parse(manual_url).map_err(|error| {
            self.contract_error(format!("invalid Context Book base URL: {error}"))
        })?;
        validate_url_against_runtime_policy(
            &url,
            &self.resolved.allowed_hosts,
            self.resolved.allow_private_hosts,
        )
        .map_err(|error| self.contract_error(error.to_string()))?;
        Ok(normalize_base_url(url))
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

fn normalize_optional_string(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use axum::{
        Router,
        extract::{Path as AxumPath, State},
        http::{HeaderMap, StatusCode},
        response::IntoResponse,
        routing::{get, post},
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

    #[test]
    fn base_url_validation_rejects_unlisted_host() {
        let tmp = TempDir::new().expect("temp dir");
        let mut config = test_config(&tmp);
        config.context_book.allowed_hosts = vec!["other.example".into()];

        let client = ContextBookClient::new(&config);
        let error = client
            .resolve_base_url()
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

        let app = Router::new()
            .route("/agents", get(agents))
            .route("/subscriptions", get(subscriptions))
            .route("/events", get(events))
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
        assert_eq!(
            contract.refresh_mode,
            ContextBookRefreshMode::LegacyAuthRefresh
        );
        assert!(contract.degraded_modes.is_empty());

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
