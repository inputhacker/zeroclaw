use super::traits::{Tool, ToolResult};
use crate::context_book::ContextBookService;
use crate::security::SecurityPolicy;
use crate::security::policy::ToolOperation;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

pub struct ContextBookContextDeleteTool {
    service: Arc<ContextBookService>,
    security: Arc<SecurityPolicy>,
}

impl ContextBookContextDeleteTool {
    pub fn new(service: Arc<ContextBookService>, security: Arc<SecurityPolicy>) -> Self {
        Self { service, security }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContextDeleteArgs {
    context_id: String,
}

#[async_trait]
impl Tool for ContextBookContextDeleteTool {
    fn name(&self) -> &str {
        "context_book_context_delete"
    }

    fn description(&self) -> &str {
        "Delete a local Context Book context immediately via REST and remove the mirrored record from the dedicated store."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "context_id": {
                    "type": "string",
                    "description": "Context ID to delete."
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

        let args: ContextDeleteArgs = match serde_json::from_value(args) {
            Ok(args) => args,
            Err(error) => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some(format!("Invalid arguments: {error}")),
                });
            }
        };

        match self.service.delete_local_context(&args.context_id).await {
            Ok(()) => Ok(ToolResult {
                success: true,
                output: serde_json::to_string_pretty(&json!({
                    "deleted": true,
                    "contextId": args.context_id,
                }))?,
                error: None,
            }),
            Err(error) => Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(format!("Context Book context delete failed: {error}")),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context_book::{AuthSessionDto, ContextBookStore, ContextRecordDto, ContextStatus};
    use tempfile::TempDir;
    use wiremock::matchers::{header, method, path};
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
    async fn deletes_context_via_service_and_removes_mirror() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/contexts/ctx-1"))
            .and(header("authorization", "Bearer access-token"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;

        let (_tmp, service, store) = temp_service(&server);
        let tool = ContextBookContextDeleteTool::new(service, Arc::new(SecurityPolicy::default()));

        let result = tool
            .execute(json!({
                "context_id": "ctx-1"
            }))
            .await
            .expect("execute tool");

        assert!(result.success);
        assert!(result.output.contains("\"deleted\": true"));
        assert!(result.output.contains("\"contextId\": \"ctx-1\""));
        assert!(
            store
                .list_mirrored_contexts()
                .expect("list mirrored contexts")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn rejects_unknown_context_delete_arguments() {
        let server = MockServer::start().await;
        let (_tmp, service, _store) = temp_service(&server);
        let tool = ContextBookContextDeleteTool::new(service, Arc::new(SecurityPolicy::default()));

        let result = tool
            .execute(json!({
                "context_id": "ctx-1",
                "unexpected": true
            }))
            .await
            .expect("execute tool");

        assert!(!result.success);
        assert!(
            result
                .error
                .as_deref()
                .expect("error")
                .contains("Invalid arguments")
        );
    }
}
