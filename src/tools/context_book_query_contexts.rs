use super::traits::{Tool, ToolResult};
use crate::context_book::{ContextBookQuery, ContextMirrorQuery, ContextStatus};
use crate::security::SecurityPolicy;
use crate::security::policy::ToolOperation;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

pub struct ContextBookQueryContextsTool {
    query: Arc<ContextBookQuery>,
    security: Arc<SecurityPolicy>,
}

impl ContextBookQueryContextsTool {
    pub fn new(query: Arc<ContextBookQuery>, security: Arc<SecurityPolicy>) -> Self {
        Self { query, security }
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct QueryArgs {
    author_agent_id: Option<String>,
    status: Option<ContextStatus>,
    tag: Option<String>,
    text_contains: Option<String>,
    limit: Option<usize>,
}

#[async_trait]
impl Tool for ContextBookQueryContextsTool {
    fn name(&self) -> &str {
        "context_book_query_contexts"
    }

    fn description(&self) -> &str {
        "Query mirrored Context Book contexts from the dedicated local store. Supports author, status, tag, text, and limit filters."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "author_agent_id": {
                    "type": "string",
                    "description": "Optional filter for the mirrored context author."
                },
                "status": {
                    "type": "string",
                    "enum": ["Published", "Archived"],
                    "description": "Optional mirrored context status filter."
                },
                "tag": {
                    "type": "string",
                    "description": "Optional exact tag filter."
                },
                "text_contains": {
                    "type": "string",
                    "description": "Optional case-insensitive text filter applied to title and contents."
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum mirrored contexts to return. Defaults to 20 and is capped at 100."
                }
            },
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> anyhow::Result<ToolResult> {
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

        let args: QueryArgs = match serde_json::from_value(args) {
            Ok(args) => args,
            Err(error) => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some(format!("Invalid arguments: {error}")),
                });
            }
        };

        match self.query.contexts(&ContextMirrorQuery {
            author_agent_id: args.author_agent_id,
            status: args.status,
            tag: args.tag,
            text_contains: args.text_contains,
            limit: args.limit,
        }) {
            Ok(contexts) => Ok(ToolResult {
                success: true,
                output: serde_json::to_string_pretty(&json!({
                    "count": contexts.len(),
                    "contexts": contexts,
                }))?,
                error: None,
            }),
            Err(error) => Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(format!("Context Book context query failed: {error}")),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context_book::{ContextBookStore, ContextRecordDto};
    use tempfile::TempDir;

    fn seeded_tool() -> (TempDir, ContextBookQueryContextsTool) {
        let tmp = TempDir::new().expect("temp dir");
        let db_path = tmp
            .path()
            .join("state")
            .join("context_book")
            .join("state.db");
        let store = Arc::new(ContextBookStore::open_at(&db_path).expect("open store"));
        store
            .upsert_mirrored_context(&ContextRecordDto {
                context_id: "ctx-1".into(),
                author_agent_id: "agent-alpha".into(),
                title: "Motor inspection".into(),
                contents: "Bearing temperature is rising.".into(),
                tag: Some("ops".into()),
                status: ContextStatus::Published,
                created_at: "2026-04-03T10:00:00Z".into(),
                updated_at: "2026-04-03T10:05:00Z".into(),
            })
            .expect("seed context");
        let query = Arc::new(ContextBookQuery::new(store));
        let tool = ContextBookQueryContextsTool::new(query, Arc::new(SecurityPolicy::default()));
        (tmp, tool)
    }

    #[tokio::test]
    async fn queries_mirrored_contexts() {
        let (_tmp, tool) = seeded_tool();

        let result = tool
            .execute(json!({
                "author_agent_id": "agent-alpha",
                "status": "Published",
                "tag": "ops",
                "text_contains": "bearing",
            }))
            .await
            .expect("execute tool");

        assert!(result.success);
        assert!(result.output.contains("\"count\": 1"));
        assert!(result.output.contains("ctx-1"));
    }
}
