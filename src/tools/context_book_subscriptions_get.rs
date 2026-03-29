use super::traits::{Tool, ToolResult};
use crate::context_book::{ContextBookHandle, ContextBookService};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
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

        let (resolved_source, snapshot) = match source {
            "cache" => ("cache", self.service.cached_subscriptions()?),
            "remote" => ("remote", Some(self.service.get_subscriptions().await?)),
            "auto" => match self.service.cached_subscriptions()? {
                some @ Some(_) => ("cache", some),
                None => ("remote", Some(self.service.get_subscriptions().await?)),
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
                "source": resolved_source,
                "requested_source": source,
                "freshness": snapshot.as_ref().map(|snapshot| freshness(&snapshot.updated_at)),
                "subscriptions": snapshot,
            }))?,
            error: None,
        })
    }
}

fn freshness(updated_at: &str) -> serde_json::Value {
    let age_seconds = DateTime::parse_from_rfc3339(updated_at)
        .ok()
        .map(|timestamp| {
            Utc::now()
                .signed_duration_since(timestamp.with_timezone(&Utc))
                .num_seconds()
        });
    json!({
        "updated_at": updated_at,
        "age_seconds": age_seconds,
    })
}
