use super::traits::{Tool, ToolResult};
use crate::context_book::{ContextBookService, ContextStatus, ContextUpdateRequest};
use crate::security::SecurityPolicy;
use crate::security::policy::ToolOperation;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

pub struct ContextBookContextUpdateTool {
    service: Arc<ContextBookService>,
    security: Arc<SecurityPolicy>,
}

impl ContextBookContextUpdateTool {
    pub fn new(service: Arc<ContextBookService>, security: Arc<SecurityPolicy>) -> Self {
        Self { service, security }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
enum StatusArg {
    Published,
    Archived,
}

impl StatusArg {
    fn into_context_status(self) -> ContextStatus {
        match self {
            Self::Published => ContextStatus::Published,
            Self::Archived => ContextStatus::Archived,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContextUpdateArgs {
    context_id: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    contents: Option<String>,
    #[serde(default)]
    tag: Option<String>,
    #[serde(default)]
    status: Option<StatusArg>,
}

#[async_trait]
impl Tool for ContextBookContextUpdateTool {
    fn name(&self) -> &str {
        "context_book_context_update"
    }

    fn description(&self) -> &str {
        "Update a local Context Book context immediately via REST and persist the confirmed result into the dedicated store."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "context_id": {
                    "type": "string",
                    "description": "Context ID to update."
                },
                "title": {
                    "type": "string",
                    "description": "Updated context title."
                },
                "contents": {
                    "type": "string",
                    "description": "Updated context body contents."
                },
                "tag": {
                    "type": "string",
                    "description": "Updated context tag."
                },
                "status": {
                    "type": "string",
                    "enum": ["Published", "Archived"],
                    "description": "Updated context publication status."
                }
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

        let args: ContextUpdateArgs = match serde_json::from_value(args) {
            Ok(args) => args,
            Err(error) => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some(format!("Invalid arguments: {error}")),
                });
            }
        };

        if args.title.is_none()
            && args.contents.is_none()
            && args.tag.is_none()
            && args.status.is_none()
        {
            return Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(
                    "Context Book context update requires at least one field to change".into(),
                ),
            });
        }

        let request = ContextUpdateRequest {
            title: args.title,
            contents: args.contents,
            tag: args.tag,
            status: args.status.map(StatusArg::into_context_status),
        };

        match self
            .service
            .update_local_context(&args.context_id, &request)
            .await
        {
            Ok(context) => Ok(ToolResult {
                success: true,
                output: serde_json::to_string_pretty(&json!({
                    "context": context,
                }))?,
                error: None,
            }),
            Err(error) => Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(format!("Context Book context update failed: {error}")),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context_book::{AuthSessionDto, ContextBookStore, ContextRecordDto, ContextStatus};
    use tempfile::TempDir;
    use wiremock::matchers::{body_partial_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn temp_service(
        server: &MockServer,
    ) -> (TempDir, Arc<ContextBookService>, Arc<ContextBookStore>) {
        let tmp = TempDir::new().expect("temp dir");
        let db_path = tmp
            .path()
            .join("state")
            .join("context_book")
            .join("state.db");
        let store = Arc::new(ContextBookStore::open_at(&db_path).expect("open store"));
        store
            .save_auth_session(
                &AuthSessionDto {
                    agent_id: "agent-1".into(),
                    access_token: "access-token".into(),
                    refresh_token: "refresh-token".into(),
                    access_token_expires_at: "2026-04-04T00:00:00Z".into(),
                },
                "2026-04-03T00:00:00Z",
            )
            .expect("save session");
        store
            .upsert_mirrored_context(&ContextRecordDto {
                context_id: "ctx-1".into(),
                author_agent_id: "agent-1".into(),
                title: "Daily Summary".into(),
                contents: "Agent heartbeat summary".into(),
                tag: Some("ops".into()),
                status: ContextStatus::Published,
                created_at: "2026-04-03T00:00:00Z".into(),
                updated_at: "2026-04-03T00:00:10Z".into(),
            })
            .expect("seed mirrored context");

        let mut config = crate::config::ContextBookConfig::default();
        config.enabled = true;
        config.base_url = server.uri();
        let runtime_config =
            serde_json::from_value(serde_json::to_value(&config).expect("serialize config"))
                .expect("convert config");

        let service = Arc::new(
            ContextBookService::with_store(runtime_config, store.clone()).expect("service"),
        );
        (tmp, service, store)
    }

    #[tokio::test]
    async fn updates_context_via_service_and_persists_mirror() {
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/contexts/ctx-1"))
            .and(header("authorization", "Bearer access-token"))
            .and(body_partial_json(json!({
                "title": "Updated Summary",
                "status": "Archived"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "contextId": "ctx-1",
                "authorAgentId": "agent-1",
                "title": "Updated Summary",
                "contents": "Agent heartbeat summary",
                "tag": "ops",
                "status": "Archived",
                "createdAt": "2026-04-03T00:00:00Z",
                "updatedAt": "2026-04-03T00:00:30Z"
            })))
            .mount(&server)
            .await;

        let (_tmp, service, store) = temp_service(&server);
        let tool = ContextBookContextUpdateTool::new(service, Arc::new(SecurityPolicy::default()));

        let result = tool
            .execute(json!({
                "context_id": "ctx-1",
                "title": "Updated Summary",
                "status": "Archived"
            }))
            .await
            .expect("execute tool");

        assert!(result.success);
        assert!(result.output.contains("\"contextId\": \"ctx-1\""));
        assert!(result.output.contains("\"status\": \"Archived\""));

        let mirrored = store
            .list_mirrored_contexts()
            .expect("list mirrored contexts");
        assert_eq!(mirrored.len(), 1);
        assert_eq!(mirrored[0].context_id, "ctx-1");
        assert_eq!(mirrored[0].title, "Updated Summary");
        assert_eq!(mirrored[0].status, ContextStatus::Archived);
    }

    #[tokio::test]
    async fn rejects_context_update_without_mutations() {
        let server = MockServer::start().await;
        let (_tmp, service, _store) = temp_service(&server);
        let tool = ContextBookContextUpdateTool::new(service, Arc::new(SecurityPolicy::default()));

        let result = tool
            .execute(json!({
                "context_id": "ctx-1"
            }))
            .await
            .expect("execute tool");

        assert!(!result.success);
        assert_eq!(
            result.error.as_deref(),
            Some("Context Book context update requires at least one field to change")
        );
    }
}
