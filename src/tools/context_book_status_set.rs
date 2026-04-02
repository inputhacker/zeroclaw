use super::traits::{Tool, ToolResult};
use crate::context_book::{AgentLifecycleState, ContextBookService};
use crate::security::SecurityPolicy;
use crate::security::policy::ToolOperation;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

pub struct ContextBookStatusSetTool {
    service: Arc<ContextBookService>,
    security: Arc<SecurityPolicy>,
}

impl ContextBookStatusSetTool {
    pub fn new(service: Arc<ContextBookService>, security: Arc<SecurityPolicy>) -> Self {
        Self { service, security }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
enum StatusArg {
    Registered,
    Active,
    Inactive,
}

impl StatusArg {
    fn into_lifecycle_state(self) -> AgentLifecycleState {
        match self {
            Self::Registered => AgentLifecycleState::Registered,
            Self::Active => AgentLifecycleState::Active,
            Self::Inactive => AgentLifecycleState::Inactive,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StatusSetArgs {
    status: StatusArg,
}

#[async_trait]
impl Tool for ContextBookStatusSetTool {
    fn name(&self) -> &str {
        "context_book_status_set"
    }

    fn description(&self) -> &str {
        "Set the local Context Book agent lifecycle state immediately via REST and persist the confirmed result into the dedicated store."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "enum": ["Registered", "Active", "Inactive"],
                    "description": "Local lifecycle state to publish."
                }
            },
            "required": ["status"],
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

        let args: StatusSetArgs = match serde_json::from_value(args) {
            Ok(args) => args,
            Err(error) => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some(format!("Invalid arguments: {error}")),
                });
            }
        };

        match self
            .service
            .set_local_status(args.status.into_lifecycle_state())
            .await
        {
            Ok(agent) => Ok(ToolResult {
                success: true,
                output: serde_json::to_string_pretty(&json!({
                    "agent": agent,
                }))?,
                error: None,
            }),
            Err(error) => Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(format!("Context Book status update failed: {error}")),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ContextBookConfig;
    use crate::context_book::{AuthSessionDto, ContextBookStore, LocalIdentityRecord};
    use tempfile::TempDir;
    use wiremock::matchers::{body_partial_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn temp_service(server: &MockServer) -> (TempDir, Arc<ContextBookService>) {
        let tmp = TempDir::new().expect("temp dir");
        let db_path = tmp
            .path()
            .join("state")
            .join("context_book")
            .join("state.db");
        let store = Arc::new(ContextBookStore::open_at(&db_path).expect("open store"));
        store
            .save_local_identity(&LocalIdentityRecord {
                agent_id: "agent-1".into(),
                device_type: "notepc".into(),
                display_name: "ZeroClaw Main".into(),
                bootstrap_approved: true,
                last_bootstrap_request_id: Some("req-1".into()),
                last_bootstrap_approval_state: None,
                last_bootstrap_completed_at: Some("2026-04-03T00:00:00Z".into()),
                updated_at: "2026-04-03T00:00:00Z".into(),
            })
            .expect("save identity");
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

        let mut config = ContextBookConfig::default();
        config.enabled = true;
        config.base_url = server.uri();

        let service = Arc::new(ContextBookService::with_store(config, store).expect("service"));
        (tmp, service)
    }

    #[tokio::test]
    async fn updates_local_status_via_service() {
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/agents/agent-1/status"))
            .and(header("authorization", "Bearer access-token"))
            .and(body_partial_json(json!({
                "status": "Inactive"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "agentId": "agent-1",
                "deviceType": "notepc",
                "displayName": "ZeroClaw Main",
                "lifecycleState": "Inactive",
                "connectionState": "Disconnected",
                "createdAt": "2026-04-03T00:00:00Z",
                "updatedAt": "2026-04-03T00:00:10Z"
            })))
            .mount(&server)
            .await;

        let (_tmp, service) = temp_service(&server);
        let tool = ContextBookStatusSetTool::new(service, Arc::new(SecurityPolicy::default()));

        let result = tool
            .execute(json!({
                "status": "Inactive"
            }))
            .await
            .expect("execute tool");

        assert!(result.success);
        assert!(result.output.contains("\"lifecycleState\": \"Inactive\""));
        assert!(result.output.contains("\"agentId\": \"agent-1\""));
    }

    #[tokio::test]
    async fn rejects_unknown_status_argument() {
        let server = MockServer::start().await;
        let (_tmp, service) = temp_service(&server);
        let tool = ContextBookStatusSetTool::new(service, Arc::new(SecurityPolicy::default()));

        let result = tool
            .execute(json!({
                "status": "Unregistered"
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
