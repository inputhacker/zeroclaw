use super::traits::{Tool, ToolResult};
use crate::context_book::{ContextBookService, VoteCreateRequest};
use crate::security::policy::ToolOperation;
use crate::security::SecurityPolicy;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

pub struct ContextBookVoteCreateTool {
    service: Arc<ContextBookService>,
    security: Arc<SecurityPolicy>,
}

impl ContextBookVoteCreateTool {
    pub fn new(service: Arc<ContextBookService>, security: Arc<SecurityPolicy>) -> Self {
        Self { service, security }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct VoteCreateArgs {
    #[serde(default)]
    vote_id: Option<String>,
    #[serde(default)]
    vote_score: Option<f64>,
    vote_context: String,
}

#[async_trait]
impl Tool for ContextBookVoteCreateTool {
    fn name(&self) -> &str {
        "context_book_vote_create"
    }

    fn description(&self) -> &str {
        "Create a local Context Book vote immediately via REST and persist the confirmed result into the dedicated store."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "vote_id": {
                    "type": "string",
                    "description": "Optional owner-scoped vote ID to request during creation."
                },
                "vote_score": {
                    "type": "number",
                    "description": "Optional initial owner score."
                },
                "vote_context": {
                    "type": "string",
                    "description": "Vote text or context body."
                }
            },
            "required": ["vote_context"],
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

        let args: VoteCreateArgs = match serde_json::from_value(args) {
            Ok(args) => args,
            Err(error) => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some(format!("Invalid arguments: {error}")),
                });
            }
        };

        let request = VoteCreateRequest {
            vote_id: args.vote_id,
            vote_score: args.vote_score,
            vote_context: args.vote_context,
        };

        match self.service.create_local_vote(&request).await {
            Ok(vote) => Ok(ToolResult {
                success: true,
                output: serde_json::to_string_pretty(&json!({
                    "vote": vote,
                }))?,
                error: None,
            }),
            Err(error) => Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(format!("Context Book vote create failed: {error}")),
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
    async fn creates_vote_via_service_and_persists_mirror() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/votes"))
            .and(header("authorization", "Bearer access-token"))
            .and(body_partial_json(json!({
                "voteId": "vote-1",
                "voteScore": 2.5,
                "voteContext": "Approve deploy"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "voteId": "vote-1",
                "ownerAgentId": "agent-1",
                "voteScore": 2.5,
                "voteContext": "Approve deploy",
                "voterAgentIds": [],
                "requiredScore": 5.0,
                "executable": false,
                "createdAt": "2026-04-03T00:00:00Z",
                "updatedAt": "2026-04-03T00:00:10Z"
            })))
            .mount(&server)
            .await;

        let (_tmp, service, store) = temp_service(&server);
        let tool = ContextBookVoteCreateTool::new(service, Arc::new(SecurityPolicy::default()));

        let result = tool
            .execute(json!({
                "vote_id": "vote-1",
                "vote_score": 2.5,
                "vote_context": "Approve deploy"
            }))
            .await
            .expect("execute tool");

        assert!(result.success);
        assert!(result.output.contains("\"voteId\": \"vote-1\""));

        let mirrored = store.list_mirrored_votes().expect("list mirrored votes");
        assert_eq!(mirrored.len(), 1);
        assert_eq!(mirrored[0].vote_id, "vote-1");
        assert_eq!(mirrored[0].required_score, Some(5.0));
    }

    #[tokio::test]
    async fn rejects_unknown_vote_create_arguments() {
        let server = MockServer::start().await;
        let (_tmp, service, _store) = temp_service(&server);
        let tool = ContextBookVoteCreateTool::new(service, Arc::new(SecurityPolicy::default()));

        let result = tool
            .execute(json!({
                "vote_context": "Approve deploy",
                "unexpected": true
            }))
            .await
            .expect("execute tool");

        assert!(!result.success);
        assert!(result
            .error
            .as_deref()
            .expect("error")
            .contains("Invalid arguments"));
    }
}
