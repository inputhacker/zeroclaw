use super::traits::{Tool, ToolResult};
use crate::context_book::{ContextBookHandle, ContextBookService};
use async_trait::async_trait;
use serde_json::json;

pub struct ContextBookStatusTool {
    service: ContextBookService,
}

impl ContextBookStatusTool {
    pub fn new(handle: ContextBookHandle) -> Self {
        Self {
            service: ContextBookService::new(handle),
        }
    }
}

#[async_trait]
impl Tool for ContextBookStatusTool {
    fn name(&self) -> &str {
        "context_book_status"
    }

    fn description(&self) -> &str {
        "Inspect Context Book integration status, resolved runtime configuration, and the persisted local cache state."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        })
    }

    async fn execute(&self, _args: serde_json::Value) -> anyhow::Result<ToolResult> {
        let report = self.service.status_report();
        Ok(ToolResult {
            success: true,
            output: serde_json::to_string_pretty(&report)?,
            error: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::context_book::shared_handle;
    use tempfile::TempDir;

    #[tokio::test]
    async fn status_tool_reports_runtime_snapshot() {
        let tmp = TempDir::new().expect("temp dir");
        let config = Config {
            workspace_dir: tmp.path().join("workspace"),
            config_path: tmp.path().join("config.toml"),
            ..Config::default()
        };
        let tool = ContextBookStatusTool::new(shared_handle(&config));

        let result = tool
            .execute(serde_json::json!({}))
            .await
            .expect("tool result");
        let json: serde_json::Value =
            serde_json::from_str(&result.output).expect("status json should parse");

        assert!(result.success);
        assert_eq!(json["runtime"]["worker_state"], "disabled");
        assert_eq!(json["resolved"]["enabled"], false);
    }
}
