use super::config::ResolvedContextBookConfig;
use super::events::{ContextBookEventEnvelope, parse_polled_events};
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
    #[serde(default, alias = "approvalState")]
    approval_state: Option<String>,
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
                let expires_soon = tokens.expires_at.is_some_and(|expires_at| {
                    expires_at
                        <= Utc::now() + ChronoDuration::seconds(ACCESS_TOKEN_REFRESH_SKEW_SECS)
                });
                if expires_soon {
                    return self
                        .refresh_session(base_url, &profile.id, tokens)
                        .await
                        .map(Some);
                }
                if tokens.access_token.trim().is_empty() {
                    return Ok(None);
                }
                Ok(Some(ContextBookSession {
                    base_url: base_url.clone(),
                    agent_id: profile
                        .metadata
                        .get("agent_id")
                        .cloned()
                        .unwrap_or_else(|| self.identity.agent_id.clone()),
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
        existing: TokenSet,
    ) -> Result<ContextBookSession, ContextBookClientError> {
        let refresh_token = existing
            .refresh_token
            .clone()
            .ok_or_else(|| self.auth_error("Context Book auth profile is missing refresh_token"))?;
        let token_url = base_url
            .join("oauth2/token")
            .map_err(|error| self.contract_error(format!("invalid oauth2/token URL: {error}")))?;

        let response = self
            .http_client
            .post(token_url)
            .form(&[
                ("grant_type", "refresh_token"),
                ("refresh_token", refresh_token.as_str()),
            ])
            .send()
            .await
            .map_err(|error| {
                self.network_error(format!("failed to refresh Context Book token: {error}"))
            })?;
        let response = self
            .expect_success(response, "failed to refresh Context Book token")
            .await?;
        let token = response.json::<TokenResponse>().await.map_err(|error| {
            self.contract_error(format!("failed to parse refresh token response: {error}"))
        })?;

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
                    .insert("agent_id".to_string(), self.identity.agent_id.clone());
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
            agent_id: token
                .agent_id
                .unwrap_or_else(|| self.identity.agent_id.clone()),
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
            .request_id
            .clone()
            .or_else(|| {
                wait.request
                    .as_ref()
                    .and_then(|request| request.request_id.clone())
            })
            .ok_or_else(|| self.contract_error("bootstrap wait metadata missing requestId"))?;
        let wait_token = wait
            .wait_token
            .clone()
            .ok_or_else(|| self.contract_error("bootstrap wait metadata missing waitToken"))?;
        let status_path = wait
            .status_url
            .clone()
            .unwrap_or_else(|| format!("/bootstrap/requests/{request_id}"));
        let complete_path = wait
            .complete_url
            .clone()
            .unwrap_or_else(|| "/bootstrap/register/complete".to_string());

        let mut approval_state = wait.approval_state.clone().or_else(|| {
            wait.request
                .as_ref()
                .and_then(|request| request.approval_state.clone())
        });
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
        metadata.insert("agent_id".to_string(), self.identity.agent_id.clone());
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
            agent_id: token
                .agent_id
                .unwrap_or_else(|| self.identity.agent_id.clone()),
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
                    wait.wait_token
                        .as_ref()
                        .map(|_| "bootstrap approval required".to_string())
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
                    .and_then(|wait| wait.wait_token.as_ref())
                    .is_some() =>
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
        .unwrap_or_else(|| "daemon".to_string());
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
        assert_eq!(identity.device_type, "daemon");
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
                    "requestId": "req-1",
                    "waitToken": "wait-1",
                    "statusUrl": "/bootstrap/requests/req-1",
                    "completeUrl": "/bootstrap/register/complete",
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
                "access_token": "access-1",
                "refresh_token": "refresh-1",
                "expires_in": 1800,
                "agentId": "workspace"
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
        assert_eq!(state.connect_calls.load(Ordering::SeqCst), 1);

        let stored = client
            .auth_service
            .get_profile(CONTEXT_BOOK_PROVIDER, None)
            .await
            .expect("load stored profile")
            .expect("stored profile");
        assert_eq!(stored.provider, CONTEXT_BOOK_PROVIDER);
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
                    "requestId": "req-reapprove",
                    "waitToken": "wait-reapprove",
                    "statusUrl": "/bootstrap/requests/req-reapprove",
                    "completeUrl": "/bootstrap/register/complete",
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

        server.abort();
        let _ = server.await;
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
}
