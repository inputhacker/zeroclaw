use super::traits::{Tool, ToolResult};
use crate::context_book::{ContextBookService, ContextCreateRequest, ContextStatus};
use crate::security::SecurityPolicy;
use crate::security::policy::ToolOperation;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

pub struct ContextBookContextCreateTool {
    service: Arc<ContextBookService>,
    security: Arc<SecurityPolicy>,
}

impl ContextBookContextCreateTool {
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
struct ContextCreateArgs {
    #[serde(default)]
    context_id: Option<String>,
    title: String,
    contents: String,
    #[serde(default)]
    tag: Option<String>,
    status: StatusArg,
}

#[async_trait]
impl Tool for ContextBookContextCreateTool {
    fn name(&self) -> &str {
        "context_book_context_create"
    }

    fn description(&self) -> &str {
        "Create a local Context Book context immediately via REST and persist the confirmed result into the dedicated store."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "context_id": {
                    "type": "string",
                    "description": "Optional owner-scoped context ID to request during creation."
                },
                "title": {
                    "type": "string",
                    "description": "Context title."
                },
                "contents": {
                    "type": "string",
                    "description": "Context body contents."
                },
                "tag": {
                    "type": "string",
                    "description": "Optional context tag."
                },
                "status": {
                    "type": "string",
                    "enum": ["Published", "Archived"],
                    "description": "Initial context status to publish."
                }
            },
            "required": ["title", "contents", "status"],
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

        let args: ContextCreateArgs = match serde_json::from_value(args) {
            Ok(args) => args,
            Err(error) => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some(format!("Invalid arguments: {error}")),
                });
            }
        };

        let request = ContextCreateRequest {
            context_id: args.context_id,
            title: args.title,
            contents: args.contents,
            tag: args.tag,
            status: args.status.into_context_status(),
        };

        match self.service.create_local_context(&request).await {
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
                error: Some(format!("Context Book context create failed: {error}")),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context_book::{AuthSessionDto, ContextBookStore};
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
    async fn creates_context_via_service_and_persists_mirror() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/contexts"))
            .and(header("authorization", "Bearer access-token"))
            .and(body_partial_json(json!({
                "contextId": "ctx-1",
                "title": "Daily Summary",
                "contents": "Agent heartbeat summary",
                "tag": "ops",
                "status": "Published"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "contextId": "ctx-1",
                "authorAgentId": "agent-1",
                "title": "Daily Summary",
                "contents": "Agent heartbeat summary",
                "tag": "ops",
                "status": "Published",
                "createdAt": "2026-04-03T00:00:00Z",
                "updatedAt": "2026-04-03T00:00:10Z"
            })))
            .mount(&server)
            .await;

        let (_tmp, service, store) = temp_service(&server);
        let tool = ContextBookContextCreateTool::new(service, Arc::new(SecurityPolicy::default()));

        let result = tool
            .execute(json!({
                "context_id": "ctx-1",
                "title": "Daily Summary",
                "contents": "Agent heartbeat summary",
                "tag": "ops",
                "status": "Published"
            }))
            .await
            .expect("execute tool");

        assert!(result.success);
        assert!(result.output.contains("\"contextId\": \"ctx-1\""));

        let mirrored = store
            .list_mirrored_contexts()
            .expect("list mirrored contexts");
        assert_eq!(mirrored.len(), 1);
        assert_eq!(mirrored[0].context_id, "ctx-1");
        assert_eq!(mirrored[0].status, ContextStatus::Published);
    }

    #[tokio::test]
    async fn rejects_unknown_context_status_argument() {
        let server = MockServer::start().await;
        let (_tmp, service, _store) = temp_service(&server);
        let tool = ContextBookContextCreateTool::new(service, Arc::new(SecurityPolicy::default()));

        let result = tool
            .execute(json!({
                "title": "Daily Summary",
                "contents": "Agent heartbeat summary",
                "status": "Draft"
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
