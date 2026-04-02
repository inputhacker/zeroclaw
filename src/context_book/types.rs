use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AgentLifecycleState {
    #[serde(rename = "Unregistered")]
    Unregistered,
    #[serde(rename = "Registered")]
    Registered,
    #[serde(rename = "Active")]
    Active,
    #[serde(rename = "Inactive")]
    Inactive,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransportConnectionState {
    #[serde(rename = "Disconnected")]
    Disconnected,
    #[serde(rename = "Connected")]
    Connected,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ApprovalState {
    #[serde(rename = "Pending")]
    Pending,
    #[serde(rename = "Approved")]
    Approved,
    #[serde(rename = "Denied")]
    Denied,
    #[serde(rename = "Expired")]
    Expired,
    #[serde(rename = "Completed")]
    Completed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum BootstrapNextAction {
    #[serde(rename = "poll_status")]
    PollStatus,
    #[serde(rename = "complete_registration")]
    CompleteRegistration,
    #[serde(rename = "retry_connect")]
    RetryConnect,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ContextStatus {
    #[serde(rename = "Published")]
    Published,
    #[serde(rename = "Archived")]
    Archived,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuntimeEventScope {
    #[serde(rename = "control-plane")]
    ControlPlane,
    #[serde(rename = "data-plane")]
    DataPlane,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuntimeEventKind {
    #[serde(rename = "agent.registered")]
    AgentRegistered,
    #[serde(rename = "agent.unregistered")]
    AgentUnregistered,
    #[serde(rename = "agent.status.changed")]
    AgentStatusChanged,
    #[serde(rename = "agent.connection.changed")]
    AgentConnectionChanged,
    #[serde(rename = "subscription.updated")]
    SubscriptionUpdated,
    #[serde(rename = "context.created")]
    ContextCreated,
    #[serde(rename = "context.updated")]
    ContextUpdated,
    #[serde(rename = "context.deleted")]
    ContextDeleted,
    #[serde(rename = "vote.created")]
    VoteCreated,
    #[serde(rename = "vote.updated")]
    VoteUpdated,
    #[serde(rename = "vote.deleted")]
    VoteDeleted,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum BootstrapWatchEventKind {
    #[serde(rename = "bootstrap.state")]
    State,
    #[serde(rename = "bootstrap.approved")]
    Approved,
    #[serde(rename = "bootstrap.denied")]
    Denied,
    #[serde(rename = "bootstrap.expired")]
    Expired,
    #[serde(rename = "bootstrap.completed")]
    Completed,
    #[serde(rename = "bootstrap.keepalive")]
    Keepalive,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeEventMeta {
    pub scope: RuntimeEventScope,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeEventEnvelope {
    pub event_id: String,
    pub event_type: RuntimeEventKind,
    pub occurred_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer_agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entity_id: Option<String>,
    #[serde(default)]
    pub payload: Value,
    pub meta: RuntimeEventMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapWatchEventEnvelope {
    pub event_type: BootstrapWatchEventKind,
    #[serde(default)]
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapInitRequest {
    pub agent_name: String,
    pub device_type: String,
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapInitResponse {
    pub request_id: String,
    pub approval_state: ApprovalState,
    pub status_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watch_url: Option<String>,
    pub complete_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapRequestStatusResponse {
    pub request_id: String,
    pub approval_state: ApprovalState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_action: Option<BootstrapNextAction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapCompleteRequest {
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AuthSessionDto {
    pub agent_id: String,
    pub access_token: String,
    pub refresh_token: String,
    pub access_token_expires_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapCompleteResponse {
    pub session: AuthSessionDto,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectRequest {
    pub agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<AuthSessionDto>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_state: Option<ApprovalState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AuthRefreshRequest {
    pub refresh_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AgentRecordDto {
    pub agent_id: String,
    pub device_type: String,
    pub display_name: String,
    pub lifecycle_state: AgentLifecycleState,
    pub connection_state: TransportConnectionState,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AgentStatusUpdateRequest {
    pub status: AgentLifecycleState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionStateDto {
    pub consumer_agent_id: String,
    pub desired_producer_agent_ids: Vec<String>,
    pub effective_producer_agent_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionReplaceRequest {
    pub producer_agent_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ContextRecordDto {
    pub context_id: String,
    pub author_agent_id: String,
    pub title: String,
    pub contents: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    pub status: ContextStatus,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ContextCreateRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_id: Option<String>,
    pub title: String,
    pub contents: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    pub status: ContextStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ContextUpdateRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contents: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<ContextStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct VoteRecordDto {
    pub vote_id: String,
    pub owner_agent_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vote_score: Option<f64>,
    pub vote_context: String,
    #[serde(default)]
    pub voter_agent_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_score: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable: Option<bool>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct VoteCreateRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vote_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vote_score: Option<f64>,
    pub vote_context: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct VoteUpdateRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vote_score: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vote_context: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct VoteCastRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vote_score: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_event_kind_serializes_with_protocol_name() {
        let json =
            serde_json::to_string(&RuntimeEventKind::AgentRegistered).expect("serialize event");

        assert_eq!(json, "\"agent.registered\"");
    }

    #[test]
    fn runtime_event_envelope_uses_expected_wire_keys() {
        let envelope = RuntimeEventEnvelope {
            event_id: "evt-1".into(),
            event_type: RuntimeEventKind::VoteUpdated,
            occurred_at: "2026-04-02T00:00:00Z".into(),
            producer_agent_id: Some("agent-a".into()),
            entity_id: Some("vote-1".into()),
            payload: serde_json::json!({ "voteId": "vote-1" }),
            meta: RuntimeEventMeta {
                scope: RuntimeEventScope::DataPlane,
            },
        };

        let value = serde_json::to_value(envelope).expect("serialize envelope");

        assert_eq!(value["eventId"], "evt-1");
        assert_eq!(value["eventType"], "vote.updated");
        assert_eq!(value["meta"]["scope"], "data-plane");
    }
}
