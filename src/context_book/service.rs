use super::config::ResolvedContextBookConfig;
use super::handle::{ContextBookHandle, ContextBookRuntimeSnapshot};
use super::store::ContextBookPersistedRuntimeState;
use serde::Serialize;

#[derive(Clone)]
pub struct ContextBookService {
    handle: ContextBookHandle,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContextBookStatusReport {
    pub resolved: ResolvedContextBookConfig,
    pub runtime: ContextBookRuntimeSnapshot,
    pub persisted_runtime: Option<ContextBookPersistedRuntimeState>,
}

impl ContextBookService {
    pub fn new(handle: ContextBookHandle) -> Self {
        Self { handle }
    }

    pub fn status_report(&self) -> ContextBookStatusReport {
        let persisted_runtime = self
            .handle
            .store()
            .load_runtime_state()
            .unwrap_or_else(|error| {
                tracing::debug!("context_book status could not load persisted runtime: {error}");
                None
            });

        ContextBookStatusReport {
            resolved: self.handle.resolved_config().clone(),
            runtime: self.handle.snapshot(),
            persisted_runtime,
        }
    }
}
