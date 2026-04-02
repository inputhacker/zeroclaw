use crate::config::ContextBookConfig;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextBookServiceMode {
    Disabled,
    EnabledPlaceholder,
}

#[derive(Debug, Clone)]
pub struct ContextBookService {
    inner: Arc<ContextBookServiceState>,
}

#[derive(Debug)]
struct ContextBookServiceState {
    config: ContextBookConfig,
    mode: ContextBookServiceMode,
}

impl ContextBookService {
    pub fn from_config(config: ContextBookConfig) -> Self {
        if config.enabled {
            Self::enabled_placeholder(config)
        } else {
            Self::disabled(config)
        }
    }

    pub fn disabled(config: ContextBookConfig) -> Self {
        Self {
            inner: Arc::new(ContextBookServiceState {
                config,
                mode: ContextBookServiceMode::Disabled,
            }),
        }
    }

    pub fn enabled_placeholder(config: ContextBookConfig) -> Self {
        Self {
            inner: Arc::new(ContextBookServiceState {
                config,
                mode: ContextBookServiceMode::EnabledPlaceholder,
            }),
        }
    }

    pub fn mode(&self) -> ContextBookServiceMode {
        self.inner.mode
    }

    pub fn is_enabled(&self) -> bool {
        self.mode() == ContextBookServiceMode::EnabledPlaceholder
    }

    pub fn config(&self) -> &ContextBookConfig {
        &self.inner.config
    }
}

impl Default for ContextBookService {
    fn default() -> Self {
        Self::disabled(ContextBookConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_service_is_disabled_noop() {
        let service = ContextBookService::default();

        assert_eq!(service.mode(), ContextBookServiceMode::Disabled);
        assert!(!service.is_enabled());
    }

    #[test]
    fn enabled_placeholder_service_tracks_enabled_config() {
        let mut config = ContextBookConfig::default();
        config.enabled = true;

        let service = ContextBookService::from_config(config.clone());

        assert_eq!(service.mode(), ContextBookServiceMode::EnabledPlaceholder);
        assert!(service.is_enabled());
        assert_eq!(service.config().agent_id, config.agent_id);
    }
}
