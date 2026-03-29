pub mod client;
pub mod config;
pub mod events;
pub mod handle;
pub mod service;
pub mod store;
pub mod worker;

#[allow(unused_imports)]
pub use client::{ContextBookAgentIdentity, ContextBookClient, ContextBookClientErrorKind};
#[allow(unused_imports)]
pub use config::ResolvedContextBookConfig;
#[allow(unused_imports)]
pub use events::{ContextBookEventEnvelope, ContextBookSseParser, ParsedContextBookSseFrame};
#[allow(unused_imports)]
pub use handle::{ContextBookHandle, ContextBookRuntimeSnapshot};
#[allow(unused_imports)]
pub use service::{ContextBookService, ContextBookStatusReport};
#[allow(unused_imports)]
pub use store::{ContextBookPersistedRuntimeState, ContextBookStore};

use crate::config::Config;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;

#[derive(Clone)]
pub struct ContextBookBootstrap {
    pub handle: ContextBookHandle,
    pub service: ContextBookService,
}

static HANDLE_REGISTRY: OnceLock<Mutex<HashMap<String, ContextBookHandle>>> = OnceLock::new();

fn registry() -> &'static Mutex<HashMap<String, ContextBookHandle>> {
    HANDLE_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn handle_key(config: &Config) -> String {
    format!(
        "{}::{}",
        config.config_path.display(),
        config.workspace_dir.display()
    )
}

pub fn shared_handle(config: &Config) -> ContextBookHandle {
    bootstrap(config).handle
}

pub fn bootstrap(config: &Config) -> ContextBookBootstrap {
    let key = handle_key(config);
    let mut registry = registry().lock();
    let resolved = config::ResolvedContextBookConfig::resolve(config);
    if let Some(handle) = registry.get(&key) {
        handle.refresh_resolved_config(resolved);
        return ContextBookBootstrap {
            handle: handle.clone(),
            service: ContextBookService::new(handle.clone()),
        };
    }

    let store = Arc::new(store::ContextBookStore::new(resolved.cache_db_path.clone()));
    let handle = handle::ContextBookHandleState::shared(resolved, store);
    registry.insert(key, handle.clone());
    ContextBookBootstrap {
        handle: handle.clone(),
        service: ContextBookService::new(handle),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn shared_handle_reuses_same_arc_for_same_workspace() {
        let tmp = TempDir::new().expect("temp dir");
        let config = Config {
            workspace_dir: tmp.path().join("workspace"),
            config_path: tmp.path().join("config.toml"),
            ..Config::default()
        };

        let first = shared_handle(&config);
        let second = shared_handle(&config);

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn bootstrap_refreshes_resolved_contract_for_existing_handle() {
        let tmp = TempDir::new().expect("temp dir");
        let mut config = Config {
            workspace_dir: tmp.path().join("workspace"),
            config_path: tmp.path().join("config.toml"),
            ..Config::default()
        };

        let first = bootstrap(&config).handle;
        assert_eq!(first.resolved_config().manual_url, None);

        config.context_book.manual_url = Some("https://context.example".into());
        config.context_book.allowed_hosts = vec!["context.example".into()];

        let second = bootstrap(&config).handle;

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(
            second.resolved_config().manual_url.as_deref(),
            Some("https://context.example")
        );
    }
}
