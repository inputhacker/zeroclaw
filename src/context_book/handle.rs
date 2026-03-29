use super::config::ResolvedContextBookConfig;
use super::service::ContextBookStatusReport;
use super::store::ContextBookStore;
use chrono::Utc;
use parking_lot::RwLock;
use serde::Serialize;
use std::sync::Arc;

pub type ContextBookHandle = Arc<ContextBookHandleState>;

#[derive(Debug, Clone, Serialize)]
pub struct ContextBookRuntimeSnapshot {
    pub enabled: bool,
    pub owner_mode: String,
    pub worker_state: String,
    pub lifecycle_state: String,
    pub connection_state: String,
    pub shutdown_requested: bool,
    pub status_message: Option<String>,
    pub last_error: Option<String>,
    pub last_status_at: String,
    pub last_connect_at: Option<String>,
    pub last_sync_at: Option<String>,
    pub cache_db_path: String,
    pub store_initialized: bool,
}

pub struct ContextBookHandleState {
    resolved: RwLock<ResolvedContextBookConfig>,
    store: Arc<ContextBookStore>,
    runtime: RwLock<ContextBookRuntimeSnapshot>,
}

impl ContextBookHandleState {
    pub fn shared(
        resolved: ResolvedContextBookConfig,
        store: Arc<ContextBookStore>,
    ) -> ContextBookHandle {
        let worker_state = if resolved.enabled {
            "not_started"
        } else {
            "disabled"
        };
        let runtime = ContextBookRuntimeSnapshot {
            enabled: resolved.enabled,
            owner_mode: "service_only".to_string(),
            worker_state: worker_state.to_string(),
            lifecycle_state: "inactive".to_string(),
            connection_state: "disconnected".to_string(),
            shutdown_requested: false,
            status_message: Some("phase1 foundation initialized".to_string()),
            last_error: resolved.validation_error.clone(),
            last_status_at: now_rfc3339(),
            last_connect_at: None,
            last_sync_at: None,
            cache_db_path: resolved.cache_db_path.display().to_string(),
            store_initialized: false,
        };

        Arc::new(Self {
            resolved: RwLock::new(resolved),
            store,
            runtime: RwLock::new(runtime),
        })
    }

    pub fn resolved_config(&self) -> ResolvedContextBookConfig {
        self.resolved.read().clone()
    }

    pub fn refresh_resolved_config(&self, resolved: ResolvedContextBookConfig) {
        {
            let mut current = self.resolved.write();
            *current = resolved.clone();
        }

        self.update_runtime(|runtime| {
            runtime.enabled = resolved.enabled;
            runtime.cache_db_path = resolved.cache_db_path.display().to_string();
            runtime.last_error = resolved.validation_error.clone();
            if !resolved.enabled {
                runtime.owner_mode = "service_only".to_string();
                runtime.worker_state = "disabled".to_string();
                runtime.lifecycle_state = "inactive".to_string();
                runtime.connection_state = "disconnected".to_string();
                runtime.shutdown_requested = false;
                runtime.status_message = Some("context_book disabled in config".to_string());
            }
        });
    }

    pub fn store(&self) -> Arc<ContextBookStore> {
        self.store.clone()
    }

    pub fn snapshot(&self) -> ContextBookRuntimeSnapshot {
        self.runtime.read().clone()
    }

    pub fn mark_disabled(&self) {
        self.update_runtime(|runtime| {
            runtime.owner_mode = "service_only".to_string();
            runtime.worker_state = "disabled".to_string();
            runtime.lifecycle_state = "inactive".to_string();
            runtime.connection_state = "disconnected".to_string();
            runtime.shutdown_requested = false;
            runtime.status_message = Some("context_book disabled in config".to_string());
        });
    }

    pub fn mark_daemon_supervised(&self) {
        self.update_runtime(|runtime| {
            runtime.owner_mode = "daemon_supervised".to_string();
            runtime.worker_state = "starting".to_string();
            runtime.shutdown_requested = false;
            runtime.status_message = Some("daemon-owned context_book worker starting".to_string());
        });
    }

    pub fn mark_idle(&self, message: &str) {
        self.update_runtime(|runtime| {
            runtime.worker_state = "idle".to_string();
            runtime.lifecycle_state = "inactive".to_string();
            runtime.connection_state = "disconnected".to_string();
            runtime.shutdown_requested = false;
            runtime.status_message = Some(message.to_string());
            runtime.last_error = self.resolved.read().validation_error.clone();
        });
    }

    pub fn mark_error(&self, error: impl Into<String>) {
        let error = error.into();
        self.update_runtime(|runtime| {
            runtime.worker_state = "error".to_string();
            runtime.status_message = Some("context_book worker error".to_string());
            runtime.last_error = Some(error);
        });
    }

    pub fn mark_shutdown_requested(&self) {
        self.update_runtime(|runtime| {
            runtime.shutdown_requested = true;
            runtime.status_message = Some("daemon requested context_book shutdown".to_string());
        });
    }

    pub fn mark_stopped(&self, message: &str) {
        self.update_runtime(|runtime| {
            runtime.worker_state = "stopped".to_string();
            runtime.lifecycle_state = "inactive".to_string();
            runtime.connection_state = "disconnected".to_string();
            runtime.shutdown_requested = false;
            runtime.status_message = Some(message.to_string());
        });
    }

    pub fn set_store_initialized(&self, initialized: bool) {
        self.update_runtime(|runtime| {
            runtime.store_initialized = initialized;
        });
    }

    pub fn status_report(&self) -> ContextBookStatusReport {
        let persisted_runtime = self.store.load_runtime_state().unwrap_or_else(|error| {
            tracing::debug!("context_book status could not load persisted runtime: {error}");
            None
        });

        ContextBookStatusReport {
            resolved: self.resolved_config(),
            runtime: self.snapshot(),
            persisted_runtime,
        }
    }

    fn update_runtime<F>(&self, update: F)
    where
        F: FnOnce(&mut ContextBookRuntimeSnapshot),
    {
        let mut runtime = self.runtime.write();
        update(&mut runtime);
        runtime.last_status_at = now_rfc3339();
    }
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}
