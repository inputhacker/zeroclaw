use super::traits::{Tool, ToolResult};
use crate::context_book::{ContextBookQuery, MirroredVoteQuery, VoteRecordDto};
use crate::security::SecurityPolicy;
use crate::security::policy::ToolOperation;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

pub struct ContextBookQueryVotesTool {
    query: Arc<ContextBookQuery>,
    security: Arc<SecurityPolicy>,
}

impl ContextBookQueryVotesTool {
    pub fn new(query: Arc<ContextBookQuery>, security: Arc<SecurityPolicy>) -> Self {
        Self { query, security }
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct QueryArgs {
    owner_agent_id: Option<String>,
    executable: Option<bool>,
    min_required_score: Option<f64>,
    limit: Option<usize>,
}

#[async_trait]
impl Tool for ContextBookQueryVotesTool {
    fn name(&self) -> &str {
        "context_book_query_votes"
    }

    fn description(&self) -> &str {
        "Query mirrored Context Book votes from the dedicated local store. Supports owner, executability, required-score, and limit filters."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "owner_agent_id": {
                    "type": "string",
                    "description": "Optional filter for the mirrored vote owner."
                },
                "executable": {
                    "type": "boolean",
                    "description": "Optional filter for derived executable state."
                },
                "min_required_score": {
                    "type": "number",
                    "description": "Optional lower bound for the mirrored vote required score."
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum mirrored votes to return. Defaults to 20 and is capped at 100."
                }
            },
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> anyhow::Result<ToolResult> {
        if let Err(error) = self
            .security
            .enforce_tool_operation(ToolOperation::Read, self.name())
        {
            return Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(error),
            });
        }

        let args: QueryArgs = match serde_json::from_value(args) {
            Ok(args) => args,
            Err(error) => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some(format!("Invalid arguments: {error}")),
                });
            }
        };

        match self.query.votes(&MirroredVoteQuery {
            owner_agent_id: args.owner_agent_id,
            executable: args.executable,
            min_required_score: args.min_required_score,
            limit: args.limit,
        }) {
            Ok(votes) => Ok(ToolResult {
                success: true,
                output: render_votes(votes)?,
                error: None,
            }),
            Err(error) => Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(format!("Context Book vote query failed: {error}")),
            }),
        }
    }
}

fn render_votes(votes: Vec<VoteRecordDto>) -> anyhow::Result<String> {
    Ok(serde_json::to_string_pretty(&json!({
        "count": votes.len(),
        "votes": votes,
    }))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context_book::ContextBookStore;
    use tempfile::TempDir;

    fn seeded_tool() -> (TempDir, ContextBookQueryVotesTool) {
        let tmp = TempDir::new().expect("temp dir");
        let db_path = tmp
            .path()
            .join("state")
            .join("context_book")
            .join("state.db");
        let store = Arc::new(ContextBookStore::open_at(&db_path).expect("open store"));
        store
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
            .expect("seed vote");
        let query = Arc::new(ContextBookQuery::new(store));
        let tool = ContextBookQueryVotesTool::new(query, Arc::new(SecurityPolicy::default()));
        (tmp, tool)
    }

    #[tokio::test]
    async fn queries_mirrored_votes() {
        let (_tmp, tool) = seeded_tool();

        let result = tool
            .execute(json!({
                "owner_agent_id": "agent-alpha",
                "executable": true,
                "min_required_score": 1.5,
            }))
            .await
            .expect("execute tool");

        assert!(result.success);
        assert!(result.output.contains("\"count\": 1"));
        assert!(result.output.contains("vote-1"));
    }
}
