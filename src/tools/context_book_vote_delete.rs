use super::traits::{Tool, ToolResult};
use crate::context_book::{ContextBookHandle, ContextBookService};
use crate::security::SecurityPolicy;
use crate::security::policy::ToolOperation;
use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;

pub struct ContextBookVoteDeleteTool {
    service: ContextBookService,
    security: Arc<SecurityPolicy>,
}

impl ContextBookVoteDeleteTool {
    pub fn new(handle: ContextBookHandle, security: Arc<SecurityPolicy>) -> Self {
        Self {
            service: ContextBookService::new(handle),
            security,
        }
    }
}

#[async_trait]
impl Tool for ContextBookVoteDeleteTool {
    fn name(&self) -> &str {
        "context_book_vote_delete"
    }

    fn description(&self) -> &str {
        "Delete a Context Book vote and remove it from the local cache."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "vote_id": { "type": "string" }
            },
            "required": ["vote_id"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> anyhow::Result<ToolResult> {
        if let Err(error) = self
            .security
            .enforce_tool_operation(ToolOperation::Act, self.name())
        {
            return Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(error),
            });
        }

        let vote_id = args
            .get("vote_id")
            .and_then(|value| value.as_str())
            .ok_or_else(|| anyhow::anyhow!("'vote_id' is required"))?;
        self.service.delete_vote(vote_id).await?;

        Ok(ToolResult {
            success: true,
            output: serde_json::to_string_pretty(&json!({
                "deleted": true,
                "vote_id": vote_id,
            }))?,
            error: None,
        })
    }
}
