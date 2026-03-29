use super::traits::{Tool, ToolResult};
use crate::context_book::{ContextBookContextSnapshot, ContextBookHandle, ContextBookService};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::json;

pub struct ContextBookContextsQueryTool {
    service: ContextBookService,
}

impl ContextBookContextsQueryTool {
    pub fn new(handle: ContextBookHandle) -> Self {
        Self {
            service: ContextBookService::new(handle),
        }
    }
}

#[async_trait]
impl Tool for ContextBookContextsQueryTool {
    fn name(&self) -> &str {
        "context_book_contexts_query"
    }

    fn description(&self) -> &str {
        "Query Context Book contexts from the local cache or refresh them from the remote server."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "source": {
                    "type": "string",
                    "enum": ["auto", "cache", "remote"],
                    "description": "Choose cached contexts, a remote read-through refresh, or auto (cache first, remote fallback)."
                },
                "author_agent_id": {
                    "type": "string",
                    "description": "Optional author agent ID filter."
                },
                "tag": {
                    "type": "string",
                    "description": "Optional exact tag filter."
                },
                "status": {
                    "type": "string",
                    "description": "Optional exact status filter such as Published or Archived."
                },
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Optional max number of items to return after filtering."
                }
            },
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> anyhow::Result<ToolResult> {
        let requested_source = args
            .get("source")
            .and_then(|value| value.as_str())
            .unwrap_or("auto");
        let author_agent_id = args
            .get("author_agent_id")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let tag = args
            .get("tag")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let status = args
            .get("status")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let limit = args.get("limit").and_then(|value| value.as_u64());

        let (source, snapshot) = match requested_source {
            "cache" => ("cache", self.service.cached_contexts()?),
            "remote" => ("remote", Some(self.service.get_contexts().await?)),
            "auto" => match self.service.cached_contexts()? {
                some @ Some(_) => ("cache", some),
                None => ("remote", Some(self.service.get_contexts().await?)),
            },
            _ => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some("Invalid 'source'. Use one of: auto, cache, remote".to_string()),
                });
            }
        };

        let mut items = snapshot
            .as_ref()
            .map(|snapshot| snapshot.items.clone())
            .unwrap_or_default();
        items.retain(|item| matches_context(item, author_agent_id, tag, status));
        if let Some(limit) = limit.and_then(|value| usize::try_from(value).ok()) {
            items.truncate(limit);
        }

        Ok(ToolResult {
            success: true,
            output: serde_json::to_string_pretty(&json!({
                "source": source,
                "requested_source": requested_source,
                "freshness": snapshot.as_ref().and_then(|snapshot| snapshot.updated_at.as_deref()).map(freshness),
                "count": items.len(),
                "items": items,
            }))?,
            error: None,
        })
    }
}

fn matches_context(
    item: &ContextBookContextSnapshot,
    author_agent_id: Option<&str>,
    tag: Option<&str>,
    status: Option<&str>,
) -> bool {
    author_agent_id.is_none_or(|value| item.author_agent_id == value)
        && tag.is_none_or(|value| item.tag == value)
        && status.is_none_or(|value| item.status == value)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::context_book::shared_handle;
    use tempfile::TempDir;

    #[tokio::test]
    async fn contexts_query_tool_filters_cached_items() {
        let tmp = TempDir::new().expect("temp dir");
        let config = Config {
            workspace_dir: tmp.path().join("workspace"),
            config_path: tmp.path().join("config.toml"),
            ..Config::default()
        };
        let handle = shared_handle(&config);
        handle
            .store()
            .save_contexts(&[
                ContextBookContextSnapshot {
                    context_id: "ctx-1".into(),
                    author_agent_id: "peer-a".into(),
                    title: "One".into(),
                    contents: "alpha".into(),
                    tag: "ops".into(),
                    status: "Published".into(),
                    created_at: Some("2026-03-29T00:00:00Z".into()),
                    updated_at: Some("2026-03-29T00:00:00Z".into()),
                    raw_json: json!({"contextId": "ctx-1"}),
                    synced_at: "2026-03-29T00:00:05Z".into(),
                },
                ContextBookContextSnapshot {
                    context_id: "ctx-2".into(),
                    author_agent_id: "peer-b".into(),
                    title: "Two".into(),
                    contents: "beta".into(),
                    tag: "eng".into(),
                    status: "Archived".into(),
                    created_at: Some("2026-03-29T00:00:01Z".into()),
                    updated_at: Some("2026-03-29T00:00:01Z".into()),
                    raw_json: json!({"contextId": "ctx-2"}),
                    synced_at: "2026-03-29T00:00:05Z".into(),
                },
            ])
            .expect("save cached contexts");

        let tool = ContextBookContextsQueryTool::new(handle);
        let result = tool
            .execute(json!({
                "source": "cache",
                "author_agent_id": "peer-a",
            }))
            .await
            .expect("tool result");
        let body: serde_json::Value =
            serde_json::from_str(&result.output).expect("contexts output json");

        assert!(result.success);
        assert_eq!(body["source"], "cache");
        assert_eq!(body["count"], 1);
        assert_eq!(body["items"][0]["context_id"], "ctx-1");
    }
}
