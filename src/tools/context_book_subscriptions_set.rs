use super::traits::{Tool, ToolResult};
use crate::context_book::{ContextBookHandle, ContextBookService};
use crate::security::SecurityPolicy;
use crate::security::policy::ToolOperation;
use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;

pub struct ContextBookSubscriptionsSetTool {
    service: ContextBookService,
    security: Arc<SecurityPolicy>,
}

impl ContextBookSubscriptionsSetTool {
    pub fn new(handle: ContextBookHandle, security: Arc<SecurityPolicy>) -> Self {
        Self {
            service: ContextBookService::new(handle),
            security,
        }
    }
}

#[async_trait]
impl Tool for ContextBookSubscriptionsSetTool {
    fn name(&self) -> &str {
        "context_book_subscriptions_set"
    }

    fn description(&self) -> &str {
        "Update Context Book desired subscriptions and return the server-resolved desired/effective state."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "producer_agent_ids": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Desired producer agent IDs to follow. Duplicates and blanks are ignored."
                }
            },
            "required": ["producer_agent_ids"],
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

        let desired = args
            .get("producer_agent_ids")
            .and_then(|value| value.as_array())
            .ok_or_else(|| anyhow::anyhow!("'producer_agent_ids' must be a string array"))?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .ok_or_else(|| {
                        anyhow::anyhow!("'producer_agent_ids' must only contain strings")
                    })
                    .map(ToOwned::to_owned)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let snapshot = self.service.set_subscriptions(&desired).await?;

        Ok(ToolResult {
            success: true,
            output: serde_json::to_string_pretty(&json!({
                "subscriptions": snapshot,
            }))?,
            error: None,
        })
    }
}
