use super::traits::{Tool, ToolResult};
use crate::context_book::{ContextBookHandle, ContextBookService, ContextBookVoteCreateRequest};
use crate::security::SecurityPolicy;
use crate::security::policy::ToolOperation;
use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;

pub struct ContextBookVoteCreateTool {
    service: ContextBookService,
    security: Arc<SecurityPolicy>,
}

impl ContextBookVoteCreateTool {
    pub fn new(handle: ContextBookHandle, security: Arc<SecurityPolicy>) -> Self {
        Self {
            service: ContextBookService::new(handle),
            security,
        }
    }
}

#[async_trait]
impl Tool for ContextBookVoteCreateTool {
    fn name(&self) -> &str {
        "context_book_vote_create"
    }

    fn description(&self) -> &str {
        "Create a Context Book vote and sync the local cache."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "vote_id": { "type": "string", "description": "Optional custom vote ID. Must start with the local agent ID prefix." },
                "vote_score": { "type": "number" },
                "vote_context": { "type": "string" }
            },
            "required": ["vote_context"],
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

        let request = serde_json::from_value::<ContextBookVoteCreateRequest>(args)?;
        let vote = self.service.create_vote(&request).await?;

        Ok(ToolResult {
            success: true,
            output: serde_json::to_string_pretty(&json!({ "vote": vote }))?,
            error: None,
        })
    }
}
