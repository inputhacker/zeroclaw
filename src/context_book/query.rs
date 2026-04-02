use crate::context_book::store::ContextBookStore;
use crate::context_book::types::{
    AgentLifecycleState, AgentRecordDto, ContextRecordDto, ContextStatus, TransportConnectionState,
    VoteRecordDto,
};
use anyhow::Result;
use std::sync::Arc;

const DEFAULT_QUERY_LIMIT: usize = 20;
const MAX_QUERY_LIMIT: usize = 100;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MirroredAgentQuery {
    pub agent_id_prefix: Option<String>,
    pub lifecycle_state: Option<AgentLifecycleState>,
    pub connection_state: Option<TransportConnectionState>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContextMirrorQuery {
    pub author_agent_id: Option<String>,
    pub status: Option<ContextStatus>,
    pub tag: Option<String>,
    pub text_contains: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MirroredVoteQuery {
    pub owner_agent_id: Option<String>,
    pub executable: Option<bool>,
    pub min_required_score: Option<f64>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorSubscriptionSnapshot {
    pub desired_producer_agent_ids: Vec<String>,
    pub effective_producer_agent_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ContextBookQuery {
    store: Arc<ContextBookStore>,
}

impl ContextBookQuery {
    pub fn new(store: Arc<ContextBookStore>) -> Self {
        Self { store }
    }

    pub fn agents(&self, query: &MirroredAgentQuery) -> Result<Vec<AgentRecordDto>> {
        let limit = normalize_limit(query.limit);
        let agent_id_prefix = query.agent_id_prefix.as_deref().map(str::trim);
        let mut agents: Vec<_> = self
            .store
            .list_mirrored_agents()?
            .into_iter()
            .filter(|agent| {
                agent_id_prefix.map_or(true, |prefix| agent.agent_id.starts_with(prefix))
                    && query
                        .lifecycle_state
                        .map_or(true, |state| agent.lifecycle_state == state)
                    && query
                        .connection_state
                        .map_or(true, |state| agent.connection_state == state)
            })
            .collect();
        agents.truncate(limit);
        Ok(agents)
    }

    pub fn contexts(&self, query: &ContextMirrorQuery) -> Result<Vec<ContextRecordDto>> {
        let limit = normalize_limit(query.limit);
        let text_contains = normalized_search(&query.text_contains);
        let tag = query.tag.as_deref().map(str::trim);
        let mut contexts: Vec<_> = self
            .store
            .list_mirrored_contexts()?
            .into_iter()
            .filter(|context| {
                query
                    .author_agent_id
                    .as_deref()
                    .map_or(true, |author| context.author_agent_id == author)
                    && query.status.map_or(true, |status| context.status == status)
                    && tag.map_or(true, |tag| context.tag.as_deref() == Some(tag))
                    && text_contains.as_deref().map_or(true, |needle| {
                        context.title.to_ascii_lowercase().contains(needle)
                            || context.contents.to_ascii_lowercase().contains(needle)
                    })
            })
            .collect();
        contexts.truncate(limit);
        Ok(contexts)
    }

    pub fn votes(&self, query: &MirroredVoteQuery) -> Result<Vec<VoteRecordDto>> {
        let limit = normalize_limit(query.limit);
        let mut votes: Vec<_> = self
            .store
            .list_mirrored_votes()?
            .into_iter()
            .filter(|vote| {
                query
                    .owner_agent_id
                    .as_deref()
                    .map_or(true, |owner| vote.owner_agent_id == owner)
                    && query
                        .executable
                        .map_or(true, |executable| vote.executable == Some(executable))
                    && query.min_required_score.map_or(true, |min_required_score| {
                        vote.required_score
                            .map_or(false, |required_score| required_score >= min_required_score)
                    })
            })
            .collect();
        votes.truncate(limit);
        Ok(votes)
    }

    pub fn subscriptions(&self) -> Result<MirrorSubscriptionSnapshot> {
        Ok(MirrorSubscriptionSnapshot {
            desired_producer_agent_ids: self.store.list_desired_subscriptions()?,
            effective_producer_agent_ids: self.store.list_effective_subscriptions()?,
        })
    }
}

fn normalize_limit(limit: Option<usize>) -> usize {
    limit
        .unwrap_or(DEFAULT_QUERY_LIMIT)
        .clamp(1, MAX_QUERY_LIMIT)
}

fn normalized_search(value: &Option<String>) -> Option<String> {
    let trimmed = value.as_deref()?.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_ascii_lowercase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context_book::store::ContextBookStore;
    use crate::context_book::types::{ContextStatus, VoteRecordDto};
    use crate::context_book::{AgentLifecycleState, TransportConnectionState};
    use tempfile::TempDir;

    fn temp_query() -> (TempDir, ContextBookQuery) {
        let tmp = TempDir::new().expect("temp dir");
        let db_path = tmp
            .path()
            .join("state")
            .join("context_book")
            .join("state.db");
        let store = Arc::new(ContextBookStore::open_at(&db_path).expect("open store"));
        let query = ContextBookQuery::new(store);
        (tmp, query)
    }

    #[test]
    fn agent_query_filters_by_state_and_limit() {
        let (_tmp, query) = temp_query();
        query
            .store
            .upsert_mirrored_agent(&AgentRecordDto {
                agent_id: "agent-alpha".into(),
                device_type: "notepc".into(),
                display_name: "Agent Alpha".into(),
                lifecycle_state: AgentLifecycleState::Active,
                connection_state: TransportConnectionState::Connected,
                created_at: "2026-04-03T10:00:00Z".into(),
                updated_at: "2026-04-03T10:10:00Z".into(),
                last_seen_at: Some("2026-04-03T10:10:00Z".into()),
            })
            .expect("seed agent alpha");
        query
            .store
            .upsert_mirrored_agent(&AgentRecordDto {
                agent_id: "agent-beta".into(),
                device_type: "edge".into(),
                display_name: "Agent Beta".into(),
                lifecycle_state: AgentLifecycleState::Inactive,
                connection_state: TransportConnectionState::Disconnected,
                created_at: "2026-04-03T09:00:00Z".into(),
                updated_at: "2026-04-03T09:10:00Z".into(),
                last_seen_at: None,
            })
            .expect("seed agent beta");

        let agents = query
            .agents(&MirroredAgentQuery {
                agent_id_prefix: Some("agent-".into()),
                lifecycle_state: Some(AgentLifecycleState::Active),
                connection_state: Some(TransportConnectionState::Connected),
                limit: Some(1),
            })
            .expect("query agents");

        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].agent_id, "agent-alpha");
    }

    #[test]
    fn context_query_matches_text_and_tag() {
        let (_tmp, query) = temp_query();
        query
            .store
            .upsert_mirrored_context(&ContextRecordDto {
                context_id: "ctx-1".into(),
                author_agent_id: "agent-alpha".into(),
                title: "Motor inspection".into(),
                contents: "Bearing temperature is rising.".into(),
                tag: Some("ops".into()),
                status: ContextStatus::Published,
                created_at: "2026-04-03T10:00:00Z".into(),
                updated_at: "2026-04-03T10:05:00Z".into(),
            })
            .expect("seed context 1");
        query
            .store
            .upsert_mirrored_context(&ContextRecordDto {
                context_id: "ctx-2".into(),
                author_agent_id: "agent-beta".into(),
                title: "Daily note".into(),
                contents: "No issues found.".into(),
                tag: Some("journal".into()),
                status: ContextStatus::Archived,
                created_at: "2026-04-03T08:00:00Z".into(),
                updated_at: "2026-04-03T08:05:00Z".into(),
            })
            .expect("seed context 2");

        let contexts = query
            .contexts(&ContextMirrorQuery {
                author_agent_id: Some("agent-alpha".into()),
                status: Some(ContextStatus::Published),
                tag: Some("ops".into()),
                text_contains: Some("bearing".into()),
                limit: Some(10),
            })
            .expect("query contexts");

        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].context_id, "ctx-1");
    }

    #[test]
    fn vote_query_and_subscription_snapshot_read_mirrors() {
        let (_tmp, query) = temp_query();
        query
            .store
            .upsert_mirrored_vote(
                &VoteRecordDto {
                    vote_id: "vote-1".into(),
                    owner_agent_id: "agent-alpha".into(),
                    vote_score: Some(3.0),
                    vote_context: "Approve maintenance".into(),
                    voter_agent_ids: vec!["agent-beta".into()],
                    required_score: Some(2.0),
                    executable: Some(true),
                    created_at: "2026-04-03T10:00:00Z".into(),
                    updated_at: "2026-04-03T10:02:00Z".into(),
                },
                "rest",
            )
            .expect("seed vote 1");
        query
            .store
            .upsert_mirrored_vote(
                &VoteRecordDto {
                    vote_id: "vote-2".into(),
                    owner_agent_id: "agent-beta".into(),
                    vote_score: Some(1.0),
                    vote_context: "Delay maintenance".into(),
                    voter_agent_ids: vec![],
                    required_score: Some(3.0),
                    executable: Some(false),
                    created_at: "2026-04-03T09:00:00Z".into(),
                    updated_at: "2026-04-03T09:02:00Z".into(),
                },
                "sse",
            )
            .expect("seed vote 2");
        query
            .store
            .replace_desired_subscriptions(&["*".into(), "agent-beta".into()])
            .expect("seed desired subscriptions");
        query
            .store
            .replace_effective_subscriptions(&["agent-beta".into()])
            .expect("seed effective subscriptions");

        let votes = query
            .votes(&MirroredVoteQuery {
                owner_agent_id: Some("agent-alpha".into()),
                executable: Some(true),
                min_required_score: Some(1.5),
                limit: Some(5),
            })
            .expect("query votes");
        let subscriptions = query.subscriptions().expect("read subscriptions");

        assert_eq!(votes.len(), 1);
        assert_eq!(votes[0].vote_id, "vote-1");
        assert_eq!(
            subscriptions.desired_producer_agent_ids,
            vec!["*".to_string(), "agent-beta".to_string()]
        );
        assert_eq!(
            subscriptions.effective_producer_agent_ids,
            vec!["agent-beta".to_string()]
        );
    }

    #[test]
    fn query_limit_is_bounded() {
        assert_eq!(normalize_limit(None), DEFAULT_QUERY_LIMIT);
        assert_eq!(normalize_limit(Some(0)), 1);
        assert_eq!(normalize_limit(Some(MAX_QUERY_LIMIT + 1)), MAX_QUERY_LIMIT);
    }
}
