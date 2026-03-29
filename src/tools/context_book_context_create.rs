use super::traits::{Tool, ToolResult};
use crate::context_book::{ContextBookContextCreateRequest, ContextBookHandle, ContextBookService};
use crate::security::SecurityPolicy;
use crate::security::policy::ToolOperation;
use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;

pub struct ContextBookContextCreateTool {
    service: ContextBookService,
    security: Arc<SecurityPolicy>,
}

impl ContextBookContextCreateTool {
    pub fn new(handle: ContextBookHandle, security: Arc<SecurityPolicy>) -> Self {
        Self {
            service: ContextBookService::new(handle),
            security,
        }
    }
}

#[async_trait]
impl Tool for ContextBookContextCreateTool {
    fn name(&self) -> &str {
        "context_book_context_create"
    }

    fn description(&self) -> &str {
        "Create a Context Book context and sync the local cache after the remote write succeeds."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "context_id": { "type": "string", "description": "Optional custom context ID. Must start with the local agent ID prefix." },
                "title": { "type": "string" },
                "contents": { "type": "string" },
                "tag": { "type": "string" },
                "status": { "type": "string", "enum": ["Published", "Archived"] }
            },
            "required": ["title", "contents", "tag", "status"],
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

        let request = serde_json::from_value::<ContextBookContextCreateRequest>(args)?;
        let context = self.service.create_context(&request).await?;

        Ok(ToolResult {
            success: true,
            output: serde_json::to_string_pretty(&json!({ "context": context }))?,
            error: None,
        })
    }
}
