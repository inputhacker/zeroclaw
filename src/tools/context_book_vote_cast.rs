use super::traits::{Tool, ToolResult};
use crate::context_book::{ContextBookHandle, ContextBookService, ContextBookVoteCastRequest};
use crate::security::SecurityPolicy;
use crate::security::policy::ToolOperation;
use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;

pub struct ContextBookVoteCastTool {
    service: ContextBookService,
    security: Arc<SecurityPolicy>,
}

impl ContextBookVoteCastTool {
    pub fn new(handle: ContextBookHandle, security: Arc<SecurityPolicy>) -> Self {
        Self {
            service: ContextBookService::new(handle),
            security,
        }
    }
}

#[async_trait]
impl Tool for ContextBookVoteCastTool {
    fn name(&self) -> &str {
        "context_book_vote_cast"
    }

    fn description(&self) -> &str {
        "Cast on an existing Context Book vote and sync the updated vote snapshot."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "vote_id": { "type": "string" },
                "vote_score": { "type": "number", "description": "Optional positive cast increment. Defaults to 1 on the server." }
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
            .ok_or_else(|| anyhow::anyhow!("'vote_id' is required"))?
            .to_string();
        let mut body = args;
        body.as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("tool arguments must be an object"))?
            .remove("vote_id");
        let request = serde_json::from_value::<ContextBookVoteCastRequest>(body)?;
        let vote = self.service.cast_vote(&vote_id, &request).await?;

        Ok(ToolResult {
            success: true,
            output: serde_json::to_string_pretty(&json!({ "vote": vote }))?,
            error: None,
        })
    }
}
