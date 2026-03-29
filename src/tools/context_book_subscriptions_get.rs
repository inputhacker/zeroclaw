use super::traits::{Tool, ToolResult};
use crate::context_book::{ContextBookHandle, ContextBookService};
use async_trait::async_trait;
use serde_json::json;

pub struct ContextBookSubscriptionsGetTool {
    service: ContextBookService,
}

impl ContextBookSubscriptionsGetTool {
    pub fn new(handle: ContextBookHandle) -> Self {
        Self {
            service: ContextBookService::new(handle),
        }
    }
}

#[async_trait]
impl Tool for ContextBookSubscriptionsGetTool {
    fn name(&self) -> &str {
        "context_book_subscriptions_get"
    }

    fn description(&self) -> &str {
        "Inspect Context Book desired/effective subscriptions from cache or refresh them from the remote server."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "source": {
                    "type": "string",
                    "enum": ["auto", "cache", "remote"],
                    "description": "Choose cached subscriptions, a remote read-through refresh, or auto (cache first, remote fallback)."
                }
            },
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> anyhow::Result<ToolResult> {
        let source = args
            .get("source")
            .and_then(|value| value.as_str())
            .unwrap_or("auto");

        let snapshot = match source {
            "cache" => self.service.cached_subscriptions()?,
            "remote" => Some(self.service.get_subscriptions().await?),
            "auto" => match self.service.cached_subscriptions()? {
                some @ Some(_) => some,
                None => Some(self.service.get_subscriptions().await?),
            },
            _ => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some("Invalid 'source'. Use one of: auto, cache, remote".to_string()),
                });
            }
        };

        Ok(ToolResult {
            success: true,
            output: serde_json::to_string_pretty(&json!({
                "source": source,
                "subscriptions": snapshot,
            }))?,
            error: None,
        })
    }
}
