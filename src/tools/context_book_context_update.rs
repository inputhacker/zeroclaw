use super::traits::{Tool, ToolResult};
use crate::context_book::{ContextBookContextUpdateRequest, ContextBookHandle, ContextBookService};
use crate::security::SecurityPolicy;
use crate::security::policy::ToolOperation;
use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;

pub struct ContextBookContextUpdateTool {
    service: ContextBookService,
    security: Arc<SecurityPolicy>,
}

impl ContextBookContextUpdateTool {
    pub fn new(handle: ContextBookHandle, security: Arc<SecurityPolicy>) -> Self {
        Self {
            service: ContextBookService::new(handle),
            security,
        }
    }
}

#[async_trait]
impl Tool for ContextBookContextUpdateTool {
    fn name(&self) -> &str {
        "context_book_context_update"
    }

    fn description(&self) -> &str {
        "Patch a Context Book context and update the local cache."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "context_id": { "type": "string" },
                "title": { "type": "string" },
                "contents": { "type": "string" },
                "tag": { "type": "string" },
                "status": { "type": "string", "enum": ["Published", "Archived"] }
            },
            "required": ["context_id"],
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

        let context_id = args
            .get("context_id")
            .and_then(|value| value.as_str())
            .ok_or_else(|| anyhow::anyhow!("'context_id' is required"))?
            .to_string();
        let mut body = args;
        body.as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("tool arguments must be an object"))?
            .remove("context_id");
        let request = serde_json::from_value::<ContextBookContextUpdateRequest>(body)?;
        let context = self.service.update_context(&context_id, &request).await?;

        Ok(ToolResult {
            success: true,
            output: serde_json::to_string_pretty(&json!({ "context": context }))?,
            error: None,
        })
    }
}
