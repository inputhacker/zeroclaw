use super::traits::{Tool, ToolResult};
use crate::context_book::ContextBookQuery;
use crate::security::SecurityPolicy;
use crate::security::policy::ToolOperation;
use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;

pub struct ContextBookQuerySubscriptionsTool {
    query: Arc<ContextBookQuery>,
    security: Arc<SecurityPolicy>,
}

impl ContextBookQuerySubscriptionsTool {
    pub fn new(query: Arc<ContextBookQuery>, security: Arc<SecurityPolicy>) -> Self {
        Self { query, security }
    }
}

#[async_trait]
impl Tool for ContextBookQuerySubscriptionsTool {
    fn name(&self) -> &str {
        "context_book_query_subscriptions"
    }

    fn description(&self) -> &str {
        "Read the local mirrored Context Book desired and effective subscription sets from the dedicated store."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        })
    }

    async fn execute(&self, _args: serde_json::Value) -> anyhow::Result<ToolResult> {
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

        match self.query.subscriptions() {
            Ok(snapshot) => Ok(ToolResult {
                success: true,
                output: serde_json::to_string_pretty(&json!({
                    "desired_producer_agent_ids": snapshot.desired_producer_agent_ids,
                    "effective_producer_agent_ids": snapshot.effective_producer_agent_ids,
                }))?,
                error: None,
            }),
            Err(error) => Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(format!("Context Book subscription query failed: {error}")),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context_book::ContextBookStore;
    use tempfile::TempDir;

    fn seeded_tool() -> (TempDir, ContextBookQuerySubscriptionsTool) {
        let tmp = TempDir::new().expect("temp dir");
        let db_path = tmp
            .path()
            .join("state")
            .join("context_book")
            .join("state.db");
        let store = Arc::new(ContextBookStore::open_at(&db_path).expect("open store"));
        store
            .replace_desired_subscriptions(&["*".into(), "agent-beta".into()])
            .expect("seed desired subscriptions");
        store
            .replace_effective_subscriptions(&["agent-beta".into()])
            .expect("seed effective subscriptions");
        let query = Arc::new(ContextBookQuery::new(store));
        let tool =
            ContextBookQuerySubscriptionsTool::new(query, Arc::new(SecurityPolicy::default()));
        (tmp, tool)
    }

    #[tokio::test]
    async fn reads_subscription_snapshot() {
        let (_tmp, tool) = seeded_tool();

        let result = tool.execute(json!({})).await.expect("execute tool");

        assert!(result.success);
        assert!(result.output.contains("desired_producer_agent_ids"));
        assert!(result.output.contains("effective_producer_agent_ids"));
        assert!(result.output.contains("agent-beta"));
    }
}
