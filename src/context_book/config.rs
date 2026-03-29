use crate::config::{
    Config, ContextBookConfig, ContextBookCursorNotFoundPolicy, ContextBookSubscriptionMode,
};
use reqwest::Url;
use serde::Serialize;
use std::net::IpAddr;
use std::path::PathBuf;

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ResolvedContextBookConfig {
    pub enabled: bool,
    pub manual_url: Option<String>,
    pub discovery_enabled: bool,
    pub service_type: String,
    pub subscription_mode: ContextBookSubscriptionMode,
    pub subscription_seed: Vec<String>,
    pub forward_to_host: bool,
    pub polling_fallback_enabled: bool,
    pub reconnect_backoff_ms: u64,
    pub max_reconnect_backoff_ms: u64,
    pub cursor_not_found_policy: ContextBookCursorNotFoundPolicy,
    pub bootstrap_secret_env_key: String,
    pub auth_profile: Option<String>,
    pub allowed_hosts: Vec<String>,
    pub allow_private_hosts: bool,
    pub cache_db_path: PathBuf,
    pub bearer_token_source: ContextBookBearerTokenSource,
    pub refresh_owner: ContextBookRefreshOwner,
    pub refresh_protocol: ContextBookRefreshProtocol,
    pub legacy_refresh_enabled: bool,
    pub validation_error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContextBookBearerTokenSource {
    AuthService,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContextBookRefreshOwner {
    ContextBookClient,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContextBookRefreshProtocol {
    OAuth2Token,
}

impl ResolvedContextBookConfig {
    pub fn resolve(config: &Config) -> Self {
        let raw = &config.context_book;
        let allowed_hosts = normalize_allowed_hosts(&raw.allowed_hosts);
        let cache_db_path = context_book_cache_path(config);
        let validation_error = validate_runtime_contract(raw, &allowed_hosts);

        Self {
            enabled: raw.enabled,
            manual_url: normalize_optional_string(raw.manual_url.as_deref()),
            discovery_enabled: raw.discovery_enabled,
            service_type: raw.service_type.trim().to_string(),
            subscription_mode: raw.subscription_mode.clone(),
            subscription_seed: raw
                .subscription_seed
                .iter()
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect(),
            forward_to_host: raw.forward_to_host,
            polling_fallback_enabled: raw.polling_fallback_enabled,
            reconnect_backoff_ms: raw.reconnect_backoff_ms,
            max_reconnect_backoff_ms: raw.max_reconnect_backoff_ms,
            cursor_not_found_policy: raw.cursor_not_found_policy.clone(),
            bootstrap_secret_env_key: raw.bootstrap_secret_env_key.trim().to_string(),
            auth_profile: normalize_optional_string(raw.auth_profile.as_deref()),
            allowed_hosts,
            allow_private_hosts: raw.allow_private_hosts,
            cache_db_path,
            bearer_token_source: ContextBookBearerTokenSource::AuthService,
            refresh_owner: ContextBookRefreshOwner::ContextBookClient,
            refresh_protocol: ContextBookRefreshProtocol::OAuth2Token,
            legacy_refresh_enabled: false,
            validation_error,
        }
    }
}

pub fn context_book_cache_path(config: &Config) -> PathBuf {
    config.workspace_dir.join("context_book").join("cache.db")
}

fn validate_runtime_contract(
    config: &ContextBookConfig,
    allowed_hosts: &[String],
) -> Option<String> {
    if config.reconnect_backoff_ms == 0 {
        return Some("context_book.reconnect_backoff_ms must be greater than 0".to_string());
    }
    if config.max_reconnect_backoff_ms < config.reconnect_backoff_ms {
        return Some(
            "context_book.max_reconnect_backoff_ms must be greater than or equal to reconnect_backoff_ms"
                .to_string(),
        );
    }
    if config.bootstrap_secret_env_key.trim().is_empty() {
        return Some("context_book.bootstrap_secret_env_key must not be empty".to_string());
    }

    let manual_url = normalize_optional_string(config.manual_url.as_deref());
    if config.enabled && manual_url.is_none() && !config.discovery_enabled {
        return Some(
            "context_book.enabled requires context_book.manual_url or discovery_enabled = true"
                .to_string(),
        );
    }

    if let Some(url) = manual_url {
        let parsed = match Url::parse(&url) {
            Ok(parsed) => parsed,
            Err(error) => {
                return Some(format!("context_book.manual_url is invalid: {error}"));
            }
        };

        if !matches!(parsed.scheme(), "http" | "https") {
            return Some("context_book.manual_url must use http or https".to_string());
        }

        let Some(host) = parsed.host_str() else {
            return Some("context_book.manual_url must include a host".to_string());
        };

        if !allowed_hosts.is_empty() && !host_matches_allowlist(host, allowed_hosts) {
            return Some(format!(
                "context_book.manual_url host '{host}' is not present in context_book.allowed_hosts"
            ));
        }

        if !config.allow_private_hosts && is_private_like_host(host) {
            return Some(format!(
                "context_book.manual_url host '{host}' is private/loopback; set context_book.allow_private_hosts = true to allow it"
            ));
        }
    }

    None
}

fn normalize_optional_string(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn normalize_allowed_hosts(values: &[String]) -> Vec<String> {
    let mut normalized = values
        .iter()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    normalized.sort_unstable();
    normalized.dedup();
    normalized
}

fn host_matches_allowlist(host: &str, allowlist: &[String]) -> bool {
    let host = host.trim().to_ascii_lowercase();
    allowlist.iter().any(|pattern| {
        pattern == "*"
            || *pattern == host
            || pattern
                .strip_prefix("*.")
                .is_some_and(|suffix| host == suffix || host.ends_with(&format!(".{suffix}")))
    })
}

fn is_private_like_host(host: &str) -> bool {
    let host = host.trim().to_ascii_lowercase();
    if matches!(host.as_str(), "localhost" | "localhost.localdomain") || host.ends_with(".local") {
        return true;
    }

    let Ok(ip) = host.parse::<IpAddr>() else {
        return false;
    };

    match ip {
        IpAddr::V4(addr) => {
            addr.is_private() || addr.is_loopback() || addr.is_link_local() || addr.is_unspecified()
        }
        IpAddr::V6(addr) => {
            addr.is_loopback()
                || addr.is_unspecified()
                || addr.is_unique_local()
                || addr.is_unicast_link_local()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn config_with_context_book(tmp: &TempDir) -> Config {
        Config {
            workspace_dir: tmp.path().join("workspace"),
            config_path: tmp.path().join("config.toml"),
            ..Config::default()
        }
    }

    #[test]
    fn resolve_sets_default_cache_path() {
        let tmp = TempDir::new().expect("temp dir");
        let config = config_with_context_book(&tmp);

        let resolved = ResolvedContextBookConfig::resolve(&config);

        assert_eq!(
            resolved.cache_db_path,
            tmp.path()
                .join("workspace")
                .join("context_book")
                .join("cache.db")
        );
        assert_eq!(
            resolved.bearer_token_source,
            ContextBookBearerTokenSource::AuthService
        );
        assert_eq!(
            resolved.refresh_owner,
            ContextBookRefreshOwner::ContextBookClient
        );
        assert_eq!(
            resolved.refresh_protocol,
            ContextBookRefreshProtocol::OAuth2Token
        );
        assert!(!resolved.legacy_refresh_enabled);
        assert!(resolved.validation_error.is_none());
    }

    #[test]
    fn resolve_rejects_private_manual_url_without_override() {
        let tmp = TempDir::new().expect("temp dir");
        let mut config = config_with_context_book(&tmp);
        config.context_book.enabled = true;
        config.context_book.discovery_enabled = false;
        config.context_book.manual_url = Some("http://127.0.0.1:7777".into());
        config.context_book.allowed_hosts = vec!["127.0.0.1".into()];

        let resolved = ResolvedContextBookConfig::resolve(&config);

        assert!(
            resolved
                .validation_error
                .as_deref()
                .unwrap_or_default()
                .contains("allow_private_hosts")
        );
    }

    #[test]
    fn resolve_allows_private_manual_url_when_explicitly_permitted() {
        let tmp = TempDir::new().expect("temp dir");
        let mut config = config_with_context_book(&tmp);
        config.context_book.enabled = true;
        config.context_book.discovery_enabled = false;
        config.context_book.manual_url = Some("http://127.0.0.1:7777".into());
        config.context_book.allowed_hosts = vec!["127.0.0.1".into()];
        config.context_book.allow_private_hosts = true;

        let resolved = ResolvedContextBookConfig::resolve(&config);

        assert!(resolved.validation_error.is_none());
    }
}
