use super::config::ResolvedContextBookConfig;
use super::service::ContextBookStatusReport;
use super::store::ContextBookStore;
use crate::config::Config;
use chrono::Utc;
use parking_lot::RwLock;
use serde::Serialize;
use std::sync::Arc;

pub type ContextBookHandle = Arc<ContextBookHandleState>;

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContextBookDegradedMode {
    ReadOnly,
    NoRefresh,
    NoWrite,
    Disconnect,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContextBookContractValidationState {
    Unknown,
    Validated,
    Degraded,
    Invalid,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContextBookRefreshMode {
    Unknown,
    OAuth2Token,
    LegacyAuthRefresh,
    Disabled,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContextBookContractSnapshot {
    pub validation_state: ContextBookContractValidationState,
    pub checked_at: Option<String>,
    pub lifecycle_connection_split: Option<bool>,
    pub subscriptions_desired_effective_split: Option<bool>,
    pub cursor_not_found_returns_409: Option<bool>,
    pub vote_deleted_supported: Option<bool>,
    pub refresh_mode: ContextBookRefreshMode,
    pub degraded_modes: Vec<ContextBookDegradedMode>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContextBookRuntimeSnapshot {
    pub enabled: bool,
    pub owner_mode: String,
    pub worker_state: String,
    pub agent_id: Option<String>,
    pub lifecycle_state: String,
    pub connection_state: String,
    pub shutdown_requested: bool,
    pub status_message: Option<String>,
    pub last_error: Option<String>,
    pub last_status_at: String,
    pub last_event_id: Option<String>,
    pub cursor_generation: i64,
    pub last_connect_at: Option<String>,
    pub last_sync_at: Option<String>,
    pub cache_db_path: String,
    pub store_initialized: bool,
}

pub struct ContextBookHandleState {
    source_config: RwLock<Config>,
    resolved: RwLock<ResolvedContextBookConfig>,
    store: Arc<ContextBookStore>,
    runtime: RwLock<ContextBookRuntimeSnapshot>,
    contract: RwLock<ContextBookContractSnapshot>,
}

impl ContextBookHandleState {
    pub fn shared(
        config: Config,
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
            agent_id: None,
            lifecycle_state: "inactive".to_string(),
            connection_state: "disconnected".to_string(),
            shutdown_requested: false,
            status_message: Some("phase1 foundation initialized".to_string()),
            last_error: resolved.validation_error.clone(),
            last_status_at: now_rfc3339(),
            last_event_id: None,
            cursor_generation: 0,
            last_connect_at: None,
            last_sync_at: None,
            cache_db_path: resolved.cache_db_path.display().to_string(),
            store_initialized: false,
        };

        Arc::new(Self {
            source_config: RwLock::new(config),
            resolved: RwLock::new(resolved),
            store,
            runtime: RwLock::new(runtime),
            contract: RwLock::new(ContextBookContractSnapshot {
                validation_state: ContextBookContractValidationState::Unknown,
                checked_at: None,
                lifecycle_connection_split: None,
                subscriptions_desired_effective_split: None,
                cursor_not_found_returns_409: None,
                vote_deleted_supported: None,
                refresh_mode: ContextBookRefreshMode::Unknown,
                degraded_modes: Vec::new(),
                notes: Vec::new(),
            }),
        })
    }

    pub fn source_config(&self) -> Config {
        self.source_config.read().clone()
    }

    pub fn resolved_config(&self) -> ResolvedContextBookConfig {
        self.resolved.read().clone()
    }

    pub fn refresh_config(&self, config: Config, resolved: ResolvedContextBookConfig) {
        {
            let mut current = self.source_config.write();
            *current = config;
        }
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

    pub fn contract_snapshot(&self) -> ContextBookContractSnapshot {
        self.contract.read().clone()
    }

    pub fn apply_contract_snapshot(&self, contract: ContextBookContractSnapshot) {
        let validation_state = contract.validation_state;
        let degraded_modes = contract.degraded_modes.clone();
        let notes = contract.notes.clone();
        {
            let mut current = self.contract.write();
            *current = contract;
        }

        self.update_runtime(|runtime| {
            runtime.status_message = Some(match validation_state {
                ContextBookContractValidationState::Validated => {
                    "context_book contract validated".to_string()
                }
                ContextBookContractValidationState::Degraded => {
                    format!(
                        "context_book contract degraded: {}",
                        degraded_modes
                            .iter()
                            .map(|mode| degraded_mode_name(*mode))
                            .collect::<Vec<_>>()
                            .join(",")
                    )
                }
                ContextBookContractValidationState::Invalid => {
                    "context_book contract invalid".to_string()
                }
                ContextBookContractValidationState::Unknown => {
                    "context_book contract not yet validated".to_string()
                }
            });
            if validation_state == ContextBookContractValidationState::Invalid {
                runtime.last_error = Some(notes.join("; "));
            }
        });
    }

    pub fn has_degraded_mode(&self, mode: ContextBookDegradedMode) -> bool {
        self.contract.read().degraded_modes.contains(&mode)
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

    pub fn mark_session_ready(&self, agent_id: &str, message: &str) {
        let agent_id = agent_id.to_string();
        self.update_runtime(|runtime| {
            runtime.agent_id = Some(agent_id);
            runtime.worker_state = "connected".to_string();
            runtime.lifecycle_state = "active".to_string();
            runtime.connection_state = "disconnected".to_string();
            runtime.status_message = Some(message.to_string());
            runtime.last_error = None;
        });
    }

    pub fn mark_stream_connected(&self, agent_id: &str, message: &str) {
        let agent_id = agent_id.to_string();
        self.update_runtime(|runtime| {
            runtime.agent_id = Some(agent_id);
            runtime.worker_state = "streaming".to_string();
            runtime.lifecycle_state = "active".to_string();
            runtime.connection_state = "connected".to_string();
            runtime.status_message = Some(message.to_string());
            runtime.last_connect_at = Some(now_rfc3339());
            runtime.last_error = None;
        });
    }

    pub fn mark_retrying(&self, message: &str) {
        self.update_runtime(|runtime| {
            runtime.worker_state = "retrying".to_string();
            runtime.connection_state = "disconnected".to_string();
            runtime.status_message = Some(message.to_string());
        });
    }

    pub fn mark_event_applied(&self, event_id: &str) {
        let event_id = event_id.to_string();
        self.update_runtime(|runtime| {
            runtime.last_event_id = Some(event_id);
            runtime.last_sync_at = Some(now_rfc3339());
            runtime.status_message = Some("context_book event applied".to_string());
        });
    }

    pub fn reset_cursor(&self, message: &str) {
        self.update_runtime(|runtime| {
            runtime.last_event_id = None;
            runtime.cursor_generation += 1;
            runtime.status_message = Some(message.to_string());
        });
    }

    pub fn restore_persisted_runtime(&self) {
        let Ok(Some(persisted)) = self.store.load_runtime_state() else {
            return;
        };

        self.update_runtime(|runtime| {
            runtime.owner_mode = persisted.owner_mode;
            runtime.worker_state = persisted.worker_state;
            runtime.agent_id = persisted.agent_id;
            runtime.lifecycle_state = persisted.lifecycle_state;
            runtime.connection_state = persisted.connection_state;
            runtime.status_message = persisted.status_message;
            runtime.last_error = persisted.last_error;
            runtime.last_event_id = persisted.last_event_id;
            runtime.cursor_generation = persisted.cursor_generation;
            runtime.last_connect_at = persisted.last_connect_at;
            runtime.last_sync_at = persisted.last_sync_at;
            runtime.last_status_at = persisted.updated_at;
        });
    }

    pub fn status_report(&self) -> ContextBookStatusReport {
        let persisted_runtime = self.store.load_runtime_state().unwrap_or_else(|error| {
            tracing::debug!("context_book status could not load persisted runtime: {error}");
            None
        });
        let persisted_subscriptions = self.store.load_subscriptions().unwrap_or_else(|error| {
            tracing::debug!("context_book status could not load subscriptions: {error}");
            None
        });
        let cache_inventory = self.store.cache_inventory().unwrap_or_else(|error| {
            tracing::debug!("context_book status could not load cache inventory: {error}");
            super::store::ContextBookCacheInventory {
                seen_event_count: 0,
                agents: super::store::ContextBookCacheCollectionSummary {
                    count: 0,
                    updated_at: None,
                },
                contexts: super::store::ContextBookCacheCollectionSummary {
                    count: 0,
                    updated_at: None,
                },
                votes: super::store::ContextBookCacheCollectionSummary {
                    count: 0,
                    updated_at: None,
                },
            }
        });

        ContextBookStatusReport {
            resolved: self.resolved_config(),
            runtime: self.snapshot(),
            contract: self.contract_snapshot(),
            persisted_runtime,
            persisted_subscriptions,
            cache_inventory,
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

fn degraded_mode_name(mode: ContextBookDegradedMode) -> &'static str {
    match mode {
        ContextBookDegradedMode::ReadOnly => "read_only",
        ContextBookDegradedMode::NoRefresh => "no_refresh",
        ContextBookDegradedMode::NoWrite => "no_write",
        ContextBookDegradedMode::Disconnect => "disconnect",
    }
}
