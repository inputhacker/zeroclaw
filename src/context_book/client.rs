use crate::config::ContextBookConfig;
use crate::context_book::types::{
    AgentRecordDto, AgentStatusUpdateRequest, AuthRefreshRequest, AuthSessionDto,
    BootstrapCompleteRequest, BootstrapCompleteResponse, BootstrapInitRequest,
    BootstrapInitResponse, BootstrapRequestStatusResponse, ConnectRequest, ConnectResponse,
    ContextCreateRequest, ContextRecordDto, ContextUpdateRequest, RuntimeEventEnvelope,
    SubscriptionReplaceRequest, SubscriptionStateDto, VoteCastRequest, VoteCreateRequest,
    VoteRecordDto, VoteUpdateRequest,
};
use reqwest::header::{ACCEPT, HeaderValue};
use reqwest::{Client, Method, StatusCode, Url};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::time::Duration;
use thiserror::Error;

const CONTEXT_BOOK_BOOTSTRAP_SECRET_HEADER: &str = "X-Context-Book-Bootstrap-Secret";
const CONTEXT_BOOK_LAST_EVENT_ID_HEADER: &str = "Last-Event-ID";
const CONTEXT_BOOK_ACCEPT_EVENT_STREAM: &str = "text/event-stream";
const CONTEXT_BOOK_PROXY_SERVICE_KEY: &str = "integration.context_book";
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;
const ERROR_BODY_LIMIT: usize = 512;

pub type ClientResult<T> = std::result::Result<T, ContextBookClientError>;

#[derive(Debug, Error)]
pub enum ContextBookClientError {
    #[error("Context Book client config error: {message}")]
    Config { message: String },
    #[error("Context Book transport error for {method} {url}: {source}")]
    Transport {
        method: Method,
        url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("Context Book auth error for {method} {url}: HTTP {status}")]
    Auth {
        method: Method,
        url: String,
        status: StatusCode,
        body: Option<String>,
    },
    #[error("Context Book HTTP error for {method} {url}: HTTP {status}")]
    Http {
        method: Method,
        url: String,
        status: StatusCode,
        body: Option<String>,
    },
    #[error("Context Book parse error for {method} {url}: {source}")]
    Parse {
        method: Method,
        url: String,
        body: Option<String>,
        #[source]
        source: serde_json::Error,
    },
}

impl ContextBookClientError {
    fn body_suffix(&self) -> String {
        match self {
            Self::Auth { body, .. } | Self::Http { body, .. } => format_error_body(body.as_deref()),
            _ => String::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ContextBookClient {
    base_url: Url,
    rest_client: Client,
    stream_client: Client,
    bootstrap_secret: String,
}

#[derive(Debug, Clone, Copy)]
enum RequestAuth<'a> {
    None,
    BootstrapSecret,
    Bearer(&'a str),
}

impl ContextBookClient {
    pub fn new(config: &ContextBookConfig) -> ClientResult<Self> {
        let base_url = parse_base_url(&config.base_url)?;
        let rest_client = build_rest_client(config.rest_timeout_secs)?;
        let stream_client = build_stream_client(config.stream_connect_timeout_secs)?;

        Ok(Self {
            base_url,
            rest_client,
            stream_client,
            bootstrap_secret: config.bootstrap_secret.clone(),
        })
    }

    #[cfg(test)]
    fn for_tests(base_url: Url, bootstrap_secret: impl Into<String>) -> ClientResult<Self> {
        let rest_client = build_rest_client(5)?;
        let stream_client = build_stream_client(5)?;
        Ok(Self {
            base_url,
            rest_client,
            stream_client,
            bootstrap_secret: bootstrap_secret.into(),
        })
    }

    pub async fn bootstrap_init(
        &self,
        request: &BootstrapInitRequest,
    ) -> ClientResult<BootstrapInitResponse> {
        self.send_json(
            &self.rest_client,
            Method::POST,
            "bootstrap/register/init",
            RequestAuth::BootstrapSecret,
            Some(request),
            &[],
            &[],
        )
        .await
    }

    pub async fn bootstrap_request_status(
        &self,
        request_id: &str,
    ) -> ClientResult<BootstrapRequestStatusResponse> {
        self.send_json::<BootstrapRequestStatusResponse, serde_json::Value>(
            &self.rest_client,
            Method::GET,
            &format!("bootstrap/requests/{request_id}"),
            RequestAuth::None,
            None,
            &[],
            &[],
        )
        .await
    }

    pub async fn bootstrap_complete(
        &self,
        request: &BootstrapCompleteRequest,
    ) -> ClientResult<BootstrapCompleteResponse> {
        self.send_json(
            &self.rest_client,
            Method::POST,
            "bootstrap/register/complete",
            RequestAuth::None,
            Some(request),
            &[],
            &[],
        )
        .await
    }

    pub async fn connect(&self, request: &ConnectRequest) -> ClientResult<ConnectResponse> {
        self.send_json(
            &self.rest_client,
            Method::POST,
            "agents/connect",
            RequestAuth::BootstrapSecret,
            Some(request),
            &[],
            &[],
        )
        .await
    }

    pub async fn auth_refresh(&self, request: &AuthRefreshRequest) -> ClientResult<AuthSessionDto> {
        let response: BootstrapCompleteResponse = self
            .send_json(
                &self.rest_client,
                Method::POST,
                "auth/refresh",
                RequestAuth::None,
                Some(request),
                &[],
                &[],
            )
            .await?;
        Ok(response.session)
    }

    pub async fn list_agents(&self, access_token: &str) -> ClientResult<Vec<AgentRecordDto>> {
        self.send_json::<Vec<AgentRecordDto>, serde_json::Value>(
            &self.rest_client,
            Method::GET,
            "agents",
            RequestAuth::Bearer(access_token),
            None,
            &[],
            &[],
        )
        .await
    }

    pub async fn update_agent_status(
        &self,
        access_token: &str,
        agent_id: &str,
        request: &AgentStatusUpdateRequest,
    ) -> ClientResult<AgentRecordDto> {
        self.send_json(
            &self.rest_client,
            Method::PATCH,
            &format!("agents/{agent_id}/status"),
            RequestAuth::Bearer(access_token),
            Some(request),
            &[],
            &[],
        )
        .await
    }

    pub async fn disconnect_agent(&self, access_token: &str, agent_id: &str) -> ClientResult<()> {
        self.send_empty(
            &self.rest_client,
            Method::POST,
            &format!("agents/{agent_id}/disconnect"),
            RequestAuth::Bearer(access_token),
            None::<&serde_json::Value>,
            &[],
            &[],
        )
        .await
    }

    pub async fn get_subscriptions(
        &self,
        access_token: &str,
    ) -> ClientResult<SubscriptionStateDto> {
        self.send_json::<SubscriptionStateDto, serde_json::Value>(
            &self.rest_client,
            Method::GET,
            "subscriptions",
            RequestAuth::Bearer(access_token),
            None,
            &[],
            &[],
        )
        .await
    }

    pub async fn replace_subscriptions(
        &self,
        access_token: &str,
        request: &SubscriptionReplaceRequest,
    ) -> ClientResult<SubscriptionStateDto> {
        self.send_json(
            &self.rest_client,
            Method::PUT,
            "subscriptions",
            RequestAuth::Bearer(access_token),
            Some(request),
            &[],
            &[],
        )
        .await
    }

    pub async fn list_contexts(&self, access_token: &str) -> ClientResult<Vec<ContextRecordDto>> {
        self.send_json::<Vec<ContextRecordDto>, serde_json::Value>(
            &self.rest_client,
            Method::GET,
            "contexts",
            RequestAuth::Bearer(access_token),
            None,
            &[],
            &[],
        )
        .await
    }

    pub async fn create_context(
        &self,
        access_token: &str,
        request: &ContextCreateRequest,
    ) -> ClientResult<ContextRecordDto> {
        self.send_json(
            &self.rest_client,
            Method::POST,
            "contexts",
            RequestAuth::Bearer(access_token),
            Some(request),
            &[],
            &[],
        )
        .await
    }

    pub async fn update_context(
        &self,
        access_token: &str,
        context_id: &str,
        request: &ContextUpdateRequest,
    ) -> ClientResult<ContextRecordDto> {
        self.send_json(
            &self.rest_client,
            Method::PATCH,
            &format!("contexts/{context_id}"),
            RequestAuth::Bearer(access_token),
            Some(request),
            &[],
            &[],
        )
        .await
    }

    pub async fn delete_context(&self, access_token: &str, context_id: &str) -> ClientResult<()> {
        self.send_empty(
            &self.rest_client,
            Method::DELETE,
            &format!("contexts/{context_id}"),
            RequestAuth::Bearer(access_token),
            None::<&serde_json::Value>,
            &[],
            &[],
        )
        .await
    }

    pub async fn list_votes(&self, access_token: &str) -> ClientResult<Vec<VoteRecordDto>> {
        self.send_json::<Vec<VoteRecordDto>, serde_json::Value>(
            &self.rest_client,
            Method::GET,
            "votes",
            RequestAuth::Bearer(access_token),
            None,
            &[],
            &[],
        )
        .await
    }

    pub async fn create_vote(
        &self,
        access_token: &str,
        request: &VoteCreateRequest,
    ) -> ClientResult<VoteRecordDto> {
        self.send_json(
            &self.rest_client,
            Method::POST,
            "votes",
            RequestAuth::Bearer(access_token),
            Some(request),
            &[],
            &[],
        )
        .await
    }

    pub async fn update_vote(
        &self,
        access_token: &str,
        vote_id: &str,
        request: &VoteUpdateRequest,
    ) -> ClientResult<VoteRecordDto> {
        self.send_json(
            &self.rest_client,
            Method::PATCH,
            &format!("votes/{vote_id}"),
            RequestAuth::Bearer(access_token),
            Some(request),
            &[],
            &[],
        )
        .await
    }

    pub async fn cast_vote(
        &self,
        access_token: &str,
        vote_id: &str,
        request: &VoteCastRequest,
    ) -> ClientResult<VoteRecordDto> {
        self.send_json(
            &self.rest_client,
            Method::POST,
            &format!("votes/{vote_id}/cast"),
            RequestAuth::Bearer(access_token),
            Some(request),
            &[],
            &[],
        )
        .await
    }

    pub async fn delete_vote(&self, access_token: &str, vote_id: &str) -> ClientResult<()> {
        self.send_empty(
            &self.rest_client,
            Method::DELETE,
            &format!("votes/{vote_id}"),
            RequestAuth::Bearer(access_token),
            None::<&serde_json::Value>,
            &[],
            &[],
        )
        .await
    }

    pub async fn poll_events(
        &self,
        access_token: &str,
        since_event_id: Option<&str>,
    ) -> ClientResult<Vec<RuntimeEventEnvelope>> {
        let mut query = Vec::new();
        if let Some(since_event_id) = since_event_id {
            query.push(("sinceEventId", since_event_id));
        }

        self.send_json::<Vec<RuntimeEventEnvelope>, serde_json::Value>(
            &self.rest_client,
            Method::GET,
            "events",
            RequestAuth::Bearer(access_token),
            None,
            &query,
            &[],
        )
        .await
    }

    pub async fn open_event_stream(
        &self,
        access_token: &str,
        agent_id: &str,
        last_event_id: Option<&str>,
    ) -> ClientResult<reqwest::Response> {
        let mut extra_headers = vec![(ACCEPT.as_str(), CONTEXT_BOOK_ACCEPT_EVENT_STREAM)];
        if let Some(last_event_id) = last_event_id {
            extra_headers.push((CONTEXT_BOOK_LAST_EVENT_ID_HEADER, last_event_id));
        }

        self.send_request(
            &self.stream_client,
            Method::GET,
            "events/stream",
            RequestAuth::Bearer(access_token),
            None::<&serde_json::Value>,
            &[("agentId", agent_id)],
            &extra_headers,
        )
        .await
    }

    async fn send_json<T, B>(
        &self,
        client: &Client,
        method: Method,
        path: &str,
        auth: RequestAuth<'_>,
        body: Option<&B>,
        query: &[(&str, &str)],
        extra_headers: &[(&str, &str)],
    ) -> ClientResult<T>
    where
        T: DeserializeOwned,
        B: Serialize + ?Sized,
    {
        let response = self
            .send_request(
                client,
                method.clone(),
                path,
                auth,
                body,
                query,
                extra_headers,
            )
            .await?;
        let url = response.url().to_string();
        let text = response
            .text()
            .await
            .map_err(|source| ContextBookClientError::Transport {
                method: method.clone(),
                url: url.clone(),
                source,
            })?;

        serde_json::from_str(&text).map_err(|source| ContextBookClientError::Parse {
            method,
            url,
            body: Some(truncate_body(&text)),
            source,
        })
    }

    async fn send_empty<B>(
        &self,
        client: &Client,
        method: Method,
        path: &str,
        auth: RequestAuth<'_>,
        body: Option<&B>,
        query: &[(&str, &str)],
        extra_headers: &[(&str, &str)],
    ) -> ClientResult<()>
    where
        B: Serialize + ?Sized,
    {
        let response = self
            .send_request(
                client,
                method.clone(),
                path,
                auth,
                body,
                query,
                extra_headers,
            )
            .await?;
        let response_url = response.url().to_string();
        let _ = response
            .text()
            .await
            .map_err(|source| ContextBookClientError::Transport {
                method,
                url: response_url,
                source,
            })?;
        Ok(())
    }

    async fn send_request<B>(
        &self,
        client: &Client,
        method: Method,
        path: &str,
        auth: RequestAuth<'_>,
        body: Option<&B>,
        query: &[(&str, &str)],
        extra_headers: &[(&str, &str)],
    ) -> ClientResult<reqwest::Response>
    where
        B: Serialize + ?Sized,
    {
        let url = self.url(path)?;
        let mut request = client.request(method.clone(), url.clone());
        request = match auth {
            RequestAuth::None => request,
            RequestAuth::BootstrapSecret => {
                request.header(CONTEXT_BOOK_BOOTSTRAP_SECRET_HEADER, &self.bootstrap_secret)
            }
            RequestAuth::Bearer(access_token) => request.bearer_auth(access_token),
        };

        if !query.is_empty() {
            request = request.query(query);
        }

        for (name, value) in extra_headers {
            request = request.header(*name, *value);
        }

        if let Some(body) = body {
            request = request.json(body);
        }

        let response =
            request
                .send()
                .await
                .map_err(|source| ContextBookClientError::Transport {
                    method: method.clone(),
                    url: url.to_string(),
                    source,
                })?;

        if response.status().is_success() {
            return Ok(response);
        }

        let status = response.status();
        let response_url = response.url().to_string();
        let body = response
            .text()
            .await
            .ok()
            .filter(|body| !body.trim().is_empty())
            .map(|body| truncate_body(&body));

        if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
            Err(ContextBookClientError::Auth {
                method,
                url: response_url,
                status,
                body,
            })
        } else {
            Err(ContextBookClientError::Http {
                method,
                url: response_url,
                status,
                body,
            })
        }
    }

    fn url(&self, path: &str) -> ClientResult<Url> {
        self.base_url
            .join(path.trim_start_matches('/'))
            .map_err(|error| ContextBookClientError::Config {
                message: format!("failed to join base URL with path '{path}': {error}"),
            })
    }
}

fn build_rest_client(timeout_secs: u64) -> ClientResult<Client> {
    let timeout_secs = timeout_secs.max(1);
    let builder = Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .connect_timeout(Duration::from_secs(DEFAULT_CONNECT_TIMEOUT_SECS));
    let builder =
        crate::config::apply_runtime_proxy_to_builder(builder, CONTEXT_BOOK_PROXY_SERVICE_KEY);

    builder
        .build()
        .map_err(|error| ContextBookClientError::Config {
            message: format!("failed to build Context Book REST client: {error}"),
        })
}

fn build_stream_client(connect_timeout_secs: u64) -> ClientResult<Client> {
    let connect_timeout_secs = connect_timeout_secs.max(1);
    let builder = Client::builder()
        .connect_timeout(Duration::from_secs(connect_timeout_secs))
        .default_headers(reqwest::header::HeaderMap::from_iter([(
            ACCEPT,
            HeaderValue::from_static(CONTEXT_BOOK_ACCEPT_EVENT_STREAM),
        )]));
    let builder =
        crate::config::apply_runtime_proxy_to_builder(builder, CONTEXT_BOOK_PROXY_SERVICE_KEY);

    builder
        .build()
        .map_err(|error| ContextBookClientError::Config {
            message: format!("failed to build Context Book stream client: {error}"),
        })
}

fn parse_base_url(raw: &str) -> ClientResult<Url> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(ContextBookClientError::Config {
            message: "base_url cannot be empty".into(),
        });
    }

    let normalized = if trimmed.ends_with('/') {
        trimmed.to_string()
    } else {
        format!("{trimmed}/")
    };

    Url::parse(&normalized).map_err(|error| ContextBookClientError::Config {
        message: format!("invalid base_url '{trimmed}': {error}"),
    })
}

fn truncate_body(body: &str) -> String {
    let mut snippet = body
        .trim()
        .chars()
        .take(ERROR_BODY_LIMIT)
        .collect::<String>();
    if body.trim().chars().count() > ERROR_BODY_LIMIT {
        snippet.push_str("...");
    }
    snippet
}

fn format_error_body(body: Option<&str>) -> String {
    match body {
        Some(body) if !body.is_empty() => format!(" body={body:?}"),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context_book::types::{
        AgentLifecycleState, ContextStatus, RuntimeEventKind, RuntimeEventMeta, RuntimeEventScope,
        VoteRecordDto,
    };
    use serde_json::json;
    use wiremock::matchers::{body_partial_json, header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn client(server: &MockServer) -> ContextBookClient {
        let base_url = Url::parse(&server.uri()).expect("server URL");
        ContextBookClient::for_tests(base_url, "bootstrap-secret").expect("client")
    }

    #[tokio::test]
    async fn bootstrap_init_sends_secret_header_and_body() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/bootstrap/register/init"))
            .and(header(
                CONTEXT_BOOK_BOOTSTRAP_SECRET_HEADER,
                "bootstrap-secret",
            ))
            .and(body_partial_json(json!({
                "agentName": "zeroclaw-main",
                "deviceType": "notepc",
                "displayName": "ZeroClaw Main"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "requestId": "req-1",
                "approvalState": "Pending",
                "statusUrl": "/bootstrap/requests/req-1",
                "completeUrl": "/bootstrap/register/complete"
            })))
            .mount(&server)
            .await;

        let response = client(&server)
            .bootstrap_init(&BootstrapInitRequest {
                agent_name: "zeroclaw-main".into(),
                device_type: "notepc".into(),
                display_name: "ZeroClaw Main".into(),
            })
            .await
            .expect("bootstrap init");

        assert_eq!(response.request_id, "req-1");
        assert_eq!(
            response.approval_state,
            crate::context_book::ApprovalState::Pending
        );
    }

    #[tokio::test]
    async fn list_agents_uses_bearer_auth_and_parses_response() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/agents"))
            .and(header("authorization", "Bearer access-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
                {
                    "agentId": "agent-1",
                    "deviceType": "notepc",
                    "displayName": "Peer One",
                    "lifecycleState": "Active",
                    "connectionState": "Connected",
                    "createdAt": "2026-04-02T00:00:00Z",
                    "updatedAt": "2026-04-02T00:00:10Z",
                    "lastSeenAt": "2026-04-02T00:00:10Z"
                }
            ])))
            .mount(&server)
            .await;

        let agents = client(&server)
            .list_agents("access-token")
            .await
            .expect("agents");

        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].agent_id, "agent-1");
        assert_eq!(agents[0].lifecycle_state, AgentLifecycleState::Active);
    }

    #[tokio::test]
    async fn poll_events_includes_since_event_id_query() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/events"))
            .and(query_param("sinceEventId", "evt-42"))
            .and(header("authorization", "Bearer access-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
                {
                    "eventId": "evt-43",
                    "eventType": "context.created",
                    "occurredAt": "2026-04-02T00:00:00Z",
                    "producerAgentId": "agent-1",
                    "entityId": "ctx-1",
                    "payload": { "contextId": "ctx-1" },
                    "meta": { "scope": "data-plane" }
                }
            ])))
            .mount(&server)
            .await;

        let events = client(&server)
            .poll_events("access-token", Some("evt-42"))
            .await
            .expect("events");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_id, "evt-43");
        assert_eq!(events[0].event_type, RuntimeEventKind::ContextCreated);
    }

    #[tokio::test]
    async fn open_event_stream_sends_last_event_id_header() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/events/stream"))
            .and(query_param("agentId", "agent-1"))
            .and(header("authorization", "Bearer access-token"))
            .and(header(CONTEXT_BOOK_LAST_EVENT_ID_HEADER, "evt-100"))
            .and(header("accept", CONTEXT_BOOK_ACCEPT_EVENT_STREAM))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(""),
            )
            .mount(&server)
            .await;

        let response = client(&server)
            .open_event_stream("access-token", "agent-1", Some("evt-100"))
            .await
            .expect("stream response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn auth_failures_are_classified_separately() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/agents"))
            .respond_with(ResponseTemplate::new(401).set_body_string("token expired"))
            .mount(&server)
            .await;

        let error = client(&server)
            .list_agents("access-token")
            .await
            .expect_err("expected auth error");

        match error {
            ContextBookClientError::Auth { status, body, .. } => {
                assert_eq!(status, StatusCode::UNAUTHORIZED);
                assert_eq!(body.as_deref(), Some("token expired"));
            }
            other => panic!("expected auth error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn parse_failures_are_classified_separately() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/votes"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{invalid-json"))
            .mount(&server)
            .await;

        let error = client(&server)
            .list_votes("access-token")
            .await
            .expect_err("expected parse error");

        match error {
            ContextBookClientError::Parse { body, .. } => {
                assert_eq!(body.as_deref(), Some("{invalid-json"));
            }
            other => panic!("expected parse error, got {other:?}"),
        }
    }

    #[test]
    fn empty_base_url_is_rejected() {
        let error = parse_base_url(" ").expect_err("expected config error");

        match error {
            ContextBookClientError::Config { message } => {
                assert!(message.contains("base_url cannot be empty"));
            }
            other => panic!("expected config error, got {other:?}"),
        }
    }

    #[test]
    fn error_body_suffix_is_rendered_for_http_errors() {
        let error = ContextBookClientError::Http {
            method: Method::GET,
            url: "https://example.test/agents".into(),
            status: StatusCode::CONFLICT,
            body: Some("cursor missing".into()),
        };

        assert_eq!(error.body_suffix(), " body=\"cursor missing\"");
    }

    #[test]
    fn vote_record_roundtrip_matches_client_expectations() {
        let vote = VoteRecordDto {
            vote_id: "vote-1".into(),
            owner_agent_id: "agent-1".into(),
            vote_score: Some(2.0),
            vote_context: "context".into(),
            voter_agent_ids: vec!["agent-2".into()],
            required_score: Some(3.0),
            executable: Some(false),
            created_at: "2026-04-02T00:00:00Z".into(),
            updated_at: "2026-04-02T00:00:01Z".into(),
        };
        let event = RuntimeEventEnvelope {
            event_id: "evt-1".into(),
            event_type: RuntimeEventKind::VoteUpdated,
            occurred_at: "2026-04-02T00:00:02Z".into(),
            producer_agent_id: Some("agent-2".into()),
            entity_id: Some("vote-1".into()),
            payload: json!({ "voteId": "vote-1" }),
            meta: RuntimeEventMeta {
                scope: RuntimeEventScope::DataPlane,
            },
        };

        let vote_json = serde_json::to_string(&vote).expect("serialize vote");
        let event_json = serde_json::to_string(&event).expect("serialize event");

        assert!(vote_json.contains("\"voteId\":\"vote-1\""));
        assert!(event_json.contains("\"eventType\":\"vote.updated\""));
        assert!(
            serde_json::to_string(&ContextStatus::Published)
                .expect("serialize context status")
                .contains("Published")
        );
    }
}
