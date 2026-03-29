use super::traits::{Tool, ToolResult};
use crate::context_book::{ContextBookHandle, ContextBookService, ContextBookVoteSnapshot};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::json;

pub struct ContextBookVotesQueryTool {
    service: ContextBookService,
}

impl ContextBookVotesQueryTool {
    pub fn new(handle: ContextBookHandle) -> Self {
        Self {
            service: ContextBookService::new(handle),
        }
    }
}

#[async_trait]
impl Tool for ContextBookVotesQueryTool {
    fn name(&self) -> &str {
        "context_book_votes_query"
    }

    fn description(&self) -> &str {
        "Query Context Book votes from the local cache or refresh them from the remote server."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "source": {
                    "type": "string",
                    "enum": ["auto", "cache", "remote"],
                    "description": "Choose cached votes, a remote read-through refresh, or auto (cache first, remote fallback)."
                },
                "owner_agent_id": {
                    "type": "string",
                    "description": "Optional owner agent ID filter."
                },
                "executable": {
                    "type": "boolean",
                    "description": "Optional executable state filter."
                },
                "min_vote_score": {
                    "type": "number",
                    "description": "Optional minimum vote score filter."
                },
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Optional max number of items to return after filtering."
                }
            },
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> anyhow::Result<ToolResult> {
        let requested_source = args
            .get("source")
            .and_then(|value| value.as_str())
            .unwrap_or("auto");
        let owner_agent_id = args
            .get("owner_agent_id")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let executable = args.get("executable").and_then(|value| value.as_bool());
        let min_vote_score = args.get("min_vote_score").and_then(|value| value.as_f64());
        let limit = args.get("limit").and_then(|value| value.as_u64());

        let (source, snapshot) = match requested_source {
            "cache" => ("cache", self.service.cached_votes()?),
            "remote" => ("remote", Some(self.service.get_votes().await?)),
            "auto" => match self.service.cached_votes()? {
                some @ Some(_) => ("cache", some),
                None => ("remote", Some(self.service.get_votes().await?)),
            },
            _ => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some("Invalid 'source'. Use one of: auto, cache, remote".to_string()),
                });
            }
        };

        let mut items = snapshot
            .as_ref()
            .map(|snapshot| snapshot.items.clone())
            .unwrap_or_default();
        items.retain(|item| matches_vote(item, owner_agent_id, executable, min_vote_score));
        if let Some(limit) = limit.and_then(|value| usize::try_from(value).ok()) {
            items.truncate(limit);
        }

        Ok(ToolResult {
            success: true,
            output: serde_json::to_string_pretty(&json!({
                "source": source,
                "requested_source": requested_source,
                "freshness": snapshot.as_ref().and_then(|snapshot| snapshot.updated_at.as_deref()).map(freshness),
                "count": items.len(),
                "items": items,
            }))?,
            error: None,
        })
    }
}

fn matches_vote(
    item: &ContextBookVoteSnapshot,
    owner_agent_id: Option<&str>,
    executable: Option<bool>,
    min_vote_score: Option<f64>,
) -> bool {
    owner_agent_id.is_none_or(|value| item.owner_agent_id == value)
        && executable.is_none_or(|value| item.executable == Some(value))
        && min_vote_score.is_none_or(|value| item.vote_score >= value)
}

fn freshness(updated_at: &str) -> serde_json::Value {
    let age_seconds = DateTime::parse_from_rfc3339(updated_at)
        .ok()
        .map(|timestamp| {
            Utc::now()
                .signed_duration_since(timestamp.with_timezone(&Utc))
                .num_seconds()
        });
    json!({
        "updated_at": updated_at,
        "age_seconds": age_seconds,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::context_book::shared_handle;
    use tempfile::TempDir;

    #[tokio::test]
    async fn votes_query_tool_filters_cached_items() {
        let tmp = TempDir::new().expect("temp dir");
        let config = Config {
            workspace_dir: tmp.path().join("workspace"),
            config_path: tmp.path().join("config.toml"),
            ..Config::default()
        };
        let handle = shared_handle(&config);
        handle
            .store()
            .save_votes(&[
                ContextBookVoteSnapshot {
                    vote_id: "vote-1".into(),
                    owner_agent_id: "peer-a".into(),
                    vote_score: 2.0,
                    vote_context: "approve".into(),
                    voter_agent_ids: vec!["peer-a".into()],
                    required_score: Some(2),
                    executable: Some(true),
                    created_at: Some("2026-03-29T00:00:00Z".into()),
                    updated_at: Some("2026-03-29T00:00:00Z".into()),
                    raw_json: json!({"voteId": "vote-1"}),
                    synced_at: "2026-03-29T00:00:05Z".into(),
                },
                ContextBookVoteSnapshot {
                    vote_id: "vote-2".into(),
                    owner_agent_id: "peer-b".into(),
                    vote_score: 1.0,
                    vote_context: "hold".into(),
                    voter_agent_ids: vec!["peer-b".into()],
                    required_score: Some(2),
                    executable: Some(false),
                    created_at: Some("2026-03-29T00:00:01Z".into()),
                    updated_at: Some("2026-03-29T00:00:01Z".into()),
                    raw_json: json!({"voteId": "vote-2"}),
                    synced_at: "2026-03-29T00:00:05Z".into(),
                },
            ])
            .expect("save cached votes");

        let tool = ContextBookVotesQueryTool::new(handle);
        let result = tool
            .execute(json!({
                "source": "cache",
                "executable": true,
            }))
            .await
            .expect("tool result");
        let body: serde_json::Value =
            serde_json::from_str(&result.output).expect("votes output json");

        assert!(result.success);
        assert_eq!(body["source"], "cache");
        assert_eq!(body["count"], 1);
        assert_eq!(body["items"][0]["vote_id"], "vote-1");
    }
}
