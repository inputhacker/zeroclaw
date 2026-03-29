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
        self.handle.status_report()
    }
}
