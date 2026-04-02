use super::traits::{Tool, ToolResult};
use crate::context_book::{
    AgentLifecycleState, ContextBookQuery, MirroredAgentQuery, TransportConnectionState,
};
use crate::security::SecurityPolicy;
use crate::security::policy::ToolOperation;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

pub struct ContextBookQueryAgentsTool {
    query: Arc<ContextBookQuery>,
    security: Arc<SecurityPolicy>,
}

impl ContextBookQueryAgentsTool {
    pub fn new(query: Arc<ContextBookQuery>, security: Arc<SecurityPolicy>) -> Self {
        Self { query, security }
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct QueryArgs {
    agent_id_prefix: Option<String>,
    lifecycle_state: Option<AgentLifecycleState>,
    connection_state: Option<TransportConnectionState>,
    limit: Option<usize>,
}

#[async_trait]
impl Tool for ContextBookQueryAgentsTool {
    fn name(&self) -> &str {
        "context_book_query_agents"
    }

    fn description(&self) -> &str {
        "Query mirrored Context Book agent state from the dedicated local store. Supports filtering by agent ID prefix, lifecycle, connection state, and limit."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "agent_id_prefix": {
                    "type": "string",
                    "description": "Optional prefix filter for mirrored agent IDs."
                },
                "lifecycle_state": {
                    "type": "string",
                    "enum": ["Unregistered", "Registered", "Active", "Inactive"],
                    "description": "Optional lifecycle-state filter."
                },
                "connection_state": {
                    "type": "string",
                    "enum": ["Disconnected", "Connected"],
                    "description": "Optional transport-state filter."
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum mirrored agents to return. Defaults to 20 and is capped at 100."
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

        match self.query.agents(&MirroredAgentQuery {
            agent_id_prefix: args.agent_id_prefix,
            lifecycle_state: args.lifecycle_state,
            connection_state: args.connection_state,
            limit: args.limit,
        }) {
            Ok(agents) => Ok(ToolResult {
                success: true,
                output: serde_json::to_string_pretty(&json!({
                    "count": agents.len(),
                    "agents": agents,
                }))?,
                error: None,
            }),
            Err(error) => Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(format!("Context Book agent query failed: {error}")),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context_book::{AgentRecordDto, ContextBookStore};
    use tempfile::TempDir;

    fn seeded_tool() -> (TempDir, ContextBookQueryAgentsTool) {
        let tmp = TempDir::new().expect("temp dir");
        let db_path = tmp
            .path()
            .join("state")
            .join("context_book")
            .join("state.db");
        let store = Arc::new(ContextBookStore::open_at(&db_path).expect("open store"));
        store
            .upsert_mirrored_agent(&AgentRecordDto {
                agent_id: "agent-alpha".into(),
                device_type: "notepc".into(),
                display_name: "Agent Alpha".into(),
                lifecycle_state: AgentLifecycleState::Active,
                connection_state: TransportConnectionState::Connected,
                created_at: "2026-04-03T10:00:00Z".into(),
                updated_at: "2026-04-03T10:01:00Z".into(),
                last_seen_at: Some("2026-04-03T10:01:00Z".into()),
            })
            .expect("seed agent");
        let query = Arc::new(ContextBookQuery::new(store));
        let tool = ContextBookQueryAgentsTool::new(query, Arc::new(SecurityPolicy::default()));
        (tmp, tool)
    }

    #[tokio::test]
    async fn queries_mirrored_agents() {
        let (_tmp, tool) = seeded_tool();

        let result = tool
            .execute(json!({
                "agent_id_prefix": "agent-",
                "lifecycle_state": "Active",
                "connection_state": "Connected",
            }))
            .await
            .expect("execute tool");

        assert!(result.success);
        assert!(result.output.contains("\"count\": 1"));
        assert!(result.output.contains("agent-alpha"));
    }

    #[tokio::test]
    async fn rejects_unknown_lifecycle_state() {
        let (_tmp, tool) = seeded_tool();

        let result = tool
            .execute(json!({
                "lifecycle_state": "Broken"
            }))
            .await
            .expect("execute tool");

        assert!(!result.success);
        assert!(
            result
                .error
                .as_deref()
                .expect("error")
                .contains("Invalid arguments")
        );
    }
}
