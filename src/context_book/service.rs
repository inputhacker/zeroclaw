use super::client::{
    ContextBookClient, ContextBookContextCreateRequest, ContextBookContextUpdateRequest,
    ContextBookVoteCastRequest, ContextBookVoteCreateRequest, ContextBookVoteUpdateRequest,
};
use super::config::ResolvedContextBookConfig;
use super::handle::{
    ContextBookContractSnapshot, ContextBookDegradedMode, ContextBookHandle,
    ContextBookRuntimeSnapshot,
};
use super::policy::{
    ContextBookPolicyReadMode, ContextBookPolicyReference, ContextBookPolicySource,
    build_policy_reference,
};
use super::store::{
    ContextBookAgentSnapshot, ContextBookCacheInventory, ContextBookCachedItems,
    ContextBookContextSnapshot, ContextBookPersistedRuntimeState, ContextBookSubscriptionsSnapshot,
    ContextBookVoteSnapshot,
};
use anyhow::{Context, Result};
use serde::Serialize;
use std::fmt::Display;

#[derive(Clone)]
pub struct ContextBookService {
    handle: ContextBookHandle,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContextBookStatusReport {
    pub resolved: ResolvedContextBookConfig,
    pub runtime: ContextBookRuntimeSnapshot,
    pub contract: ContextBookContractSnapshot,
    pub persisted_runtime: Option<ContextBookPersistedRuntimeState>,
    pub persisted_subscriptions: Option<ContextBookSubscriptionsSnapshot>,
    pub cache_inventory: ContextBookCacheInventory,
}

impl ContextBookService {
    pub fn new(handle: ContextBookHandle) -> Self {
        Self { handle }
    }

    pub fn status_report(&self) -> ContextBookStatusReport {
        self.handle.status_report()
    }

    pub fn cached_subscriptions(&self) -> Result<Option<ContextBookSubscriptionsSnapshot>> {
        self.handle
            .store()
            .load_subscriptions()
            .context("failed to load cached Context Book subscriptions")
    }

    pub async fn get_subscriptions(&self) -> Result<ContextBookSubscriptionsSnapshot> {
        let client = ContextBookClient::new(&self.handle.source_config());
        let session = client
            .ensure_session()
            .await
            .map_err(anyhow::Error::new)
            .context("failed to establish Context Book session for subscriptions get")?;
        let subscriptions = client
            .get_subscriptions(&session)
            .await
            .map_err(anyhow::Error::new)
            .context("failed to fetch Context Book subscriptions")?;
        self.handle
            .store()
            .save_subscriptions(&subscriptions)
            .context("failed to persist fetched Context Book subscriptions")?;
        Ok(subscriptions)
    }

    pub fn cached_contexts(
        &self,
    ) -> Result<Option<ContextBookCachedItems<ContextBookContextSnapshot>>> {
        self.handle
            .store()
            .load_contexts()
            .context("failed to load cached Context Book contexts")
    }

    pub async fn get_contexts(&self) -> Result<ContextBookCachedItems<ContextBookContextSnapshot>> {
        let client = ContextBookClient::new(&self.handle.source_config());
        let session = client
            .ensure_session()
            .await
            .map_err(anyhow::Error::new)
            .context("failed to establish Context Book session for contexts query")?;
        let contexts = client
            .get_contexts(&session)
            .await
            .map_err(anyhow::Error::new)
            .context("failed to fetch Context Book contexts")?;
        self.handle
            .store()
            .save_contexts(&contexts)
            .context("failed to persist fetched Context Book contexts")?;
        let updated_at = contexts.iter().map(|item| item.synced_at.clone()).max();
        Ok(ContextBookCachedItems {
            items: contexts,
            updated_at,
        })
    }

    pub fn cached_votes(&self) -> Result<Option<ContextBookCachedItems<ContextBookVoteSnapshot>>> {
        self.handle
            .store()
            .load_votes()
            .context("failed to load cached Context Book votes")
    }

    pub async fn get_votes(&self) -> Result<ContextBookCachedItems<ContextBookVoteSnapshot>> {
        let client = ContextBookClient::new(&self.handle.source_config());
        let session = client
            .ensure_session()
            .await
            .map_err(anyhow::Error::new)
            .context("failed to establish Context Book session for votes query")?;
        let votes = client
            .get_votes(&session)
            .await
            .map_err(anyhow::Error::new)
            .context("failed to fetch Context Book votes")?;
        self.handle
            .store()
            .save_votes(&votes)
            .context("failed to persist fetched Context Book votes")?;
        let updated_at = votes.iter().map(|item| item.synced_at.clone()).max();
        Ok(ContextBookCachedItems {
            items: votes,
            updated_at,
        })
    }

    pub async fn build_policy_reference(
        &self,
        read_mode: ContextBookPolicyReadMode,
        focus: Option<&str>,
    ) -> Result<Option<ContextBookPolicyReference>> {
        if !self.handle.resolved_config().enabled {
            return Ok(None);
        }

        let client = ContextBookClient::new(&self.handle.source_config());
        let agent_id = match read_mode {
            ContextBookPolicyReadMode::Cache => self
                .handle
                .snapshot()
                .agent_id
                .unwrap_or_else(|| client.identity().agent_id.clone()),
            ContextBookPolicyReadMode::Auto | ContextBookPolicyReadMode::Remote => client
                .ensure_session()
                .await
                .map(|session| session.agent_id)
                .unwrap_or_else(|_| {
                    self.handle
                        .snapshot()
                        .agent_id
                        .unwrap_or_else(|| client.identity().agent_id.clone())
                }),
        };

        let cached_contexts = self.cached_contexts()?;
        let cached_votes = self.cached_votes()?;
        let (contexts, context_source) = self
            .select_policy_contexts(read_mode, cached_contexts.as_ref())
            .await?;
        let (votes, vote_source) = self
            .select_policy_votes(read_mode, cached_votes.as_ref())
            .await?;

        if contexts.items.is_empty() && votes.items.is_empty() {
            return Ok(None);
        }

        let source = match (context_source, vote_source) {
            (ContextBookPolicySource::Cache, ContextBookPolicySource::Cache) => {
                ContextBookPolicySource::Cache
            }
            (ContextBookPolicySource::Remote, ContextBookPolicySource::Remote) => {
                ContextBookPolicySource::Remote
            }
            _ => ContextBookPolicySource::Mixed,
        };

        Ok(Some(build_policy_reference(
            &agent_id, focus, source, &contexts, &votes,
        )))
    }

    pub fn cached_agents(
        &self,
    ) -> Result<Option<ContextBookCachedItems<ContextBookAgentSnapshot>>> {
        self.handle
            .store()
            .load_agents()
            .context("failed to load cached Context Book agents")
    }

    pub async fn get_agents(&self) -> Result<ContextBookCachedItems<ContextBookAgentSnapshot>> {
        let client = ContextBookClient::new(&self.handle.source_config());
        let session = client
            .ensure_session()
            .await
            .map_err(anyhow::Error::new)
            .context("failed to establish Context Book session for agents query")?;
        let agents = client
            .get_agents(&session)
            .await
            .map_err(anyhow::Error::new)
            .context("failed to fetch Context Book agents")?;
        self.handle
            .store()
            .save_agents(&agents)
            .context("failed to persist fetched Context Book agents")?;
        let updated_at = agents.iter().map(|item| item.synced_at.clone()).max();
        Ok(ContextBookCachedItems {
            items: agents,
            updated_at,
        })
    }

    pub async fn set_subscriptions(
        &self,
        desired_producer_agent_ids: &[String],
    ) -> Result<ContextBookSubscriptionsSnapshot> {
        self.ensure_remote_writes_available("subscription writes")?;
        let (client, session) = self.write_client_and_session("subscriptions set").await?;
        let subscriptions = client
            .set_subscriptions(&session, desired_producer_agent_ids)
            .await
            .map_err(anyhow::Error::new)
            .context("failed to update Context Book subscriptions")?;
        self.handle
            .store()
            .save_subscriptions(&subscriptions)
            .context("failed to persist updated Context Book subscriptions")?;
        Ok(subscriptions)
    }

    pub async fn create_context(
        &self,
        request: &ContextBookContextCreateRequest,
    ) -> Result<ContextBookContextSnapshot> {
        self.ensure_remote_writes_available("context writes")?;
        let (client, session) = self.write_client_and_session("context create").await?;
        validate_context_create(&session.agent_id, request)?;
        let created = client
            .create_context(&session, request)
            .await
            .map_err(anyhow::Error::new)
            .context("failed to create Context Book context")?;
        self.sync_context_snapshot_with_fallback(&client, &session, &created)
            .await?;
        Ok(created)
    }

    pub async fn update_context(
        &self,
        context_id: &str,
        request: &ContextBookContextUpdateRequest,
    ) -> Result<ContextBookContextSnapshot> {
        self.ensure_remote_writes_available("context writes")?;
        let (client, session) = self.write_client_and_session("context update").await?;
        validate_context_update(request)?;
        if let Some(cached) = self
            .handle
            .store()
            .load_context(context_id)
            .context("failed to inspect cached context before update")?
            && cached.author_agent_id != session.agent_id
        {
            anyhow::bail!(
                "Context Book context '{context_id}' is owned by '{}' and cannot be updated by '{}'",
                cached.author_agent_id,
                session.agent_id
            );
        }
        let updated = client
            .update_context(&session, context_id, request)
            .await
            .map_err(anyhow::Error::new)
            .context("failed to update Context Book context")?;
        self.sync_context_snapshot_with_fallback(&client, &session, &updated)
            .await?;
        Ok(updated)
    }

    pub async fn delete_context(&self, context_id: &str) -> Result<()> {
        self.ensure_remote_writes_available("context writes")?;
        let (client, session) = self.write_client_and_session("context delete").await?;
        if let Some(cached) = self
            .handle
            .store()
            .load_context(context_id)
            .context("failed to inspect cached context before delete")?
            && cached.author_agent_id != session.agent_id
        {
            anyhow::bail!(
                "Context Book context '{context_id}' is owned by '{}' and cannot be deleted by '{}'",
                cached.author_agent_id,
                session.agent_id
            );
        }
        client
            .delete_context(&session, context_id)
            .await
            .map_err(anyhow::Error::new)
            .context("failed to delete Context Book context")?;
        self.delete_context_with_fallback(&client, &session, context_id)
            .await
    }

    pub async fn create_vote(
        &self,
        request: &ContextBookVoteCreateRequest,
    ) -> Result<ContextBookVoteSnapshot> {
        self.ensure_remote_writes_available("vote writes")?;
        let (client, session) = self.write_client_and_session("vote create").await?;
        validate_vote_create(&session.agent_id, request)?;
        let created = client
            .create_vote(&session, request)
            .await
            .map_err(anyhow::Error::new)
            .context("failed to create Context Book vote")?;
        self.sync_vote_snapshot_with_fallback(&client, &session, &created)
            .await?;
        Ok(created)
    }

    pub async fn update_vote(
        &self,
        vote_id: &str,
        request: &ContextBookVoteUpdateRequest,
    ) -> Result<ContextBookVoteSnapshot> {
        self.ensure_remote_writes_available("vote writes")?;
        let (client, session) = self.write_client_and_session("vote update").await?;
        let cached = self
            .handle
            .store()
            .load_vote(vote_id)
            .context("failed to inspect cached vote before update")?;
        validate_vote_update(&session.agent_id, cached.as_ref(), request)?;
        let updated = client
            .update_vote(&session, vote_id, request)
            .await
            .map_err(anyhow::Error::new)
            .context("failed to update Context Book vote")?;
        self.sync_vote_snapshot_with_fallback(&client, &session, &updated)
            .await?;
        Ok(updated)
    }

    pub async fn delete_vote(&self, vote_id: &str) -> Result<()> {
        self.ensure_remote_writes_available("vote writes")?;
        let (client, session) = self.write_client_and_session("vote delete").await?;
        if let Some(cached) = self
            .handle
            .store()
            .load_vote(vote_id)
            .context("failed to inspect cached vote before delete")?
            && cached.owner_agent_id != session.agent_id
        {
            anyhow::bail!(
                "Context Book vote '{vote_id}' is owned by '{}' and cannot be deleted by '{}'",
                cached.owner_agent_id,
                session.agent_id
            );
        }
        client
            .delete_vote(&session, vote_id)
            .await
            .map_err(anyhow::Error::new)
            .context("failed to delete Context Book vote")?;
        self.delete_vote_with_fallback(&client, &session, vote_id)
            .await
    }

    pub async fn cast_vote(
        &self,
        vote_id: &str,
        request: &ContextBookVoteCastRequest,
    ) -> Result<ContextBookVoteSnapshot> {
        self.ensure_remote_writes_available("vote writes")?;
        let (client, session) = self.write_client_and_session("vote cast").await?;
        let cached = self
            .handle
            .store()
            .load_vote(vote_id)
            .context("failed to inspect cached vote before cast")?;
        validate_vote_cast(&session.agent_id, cached.as_ref(), request)?;
        let cast = client
            .cast_vote(&session, vote_id, request)
            .await
            .map_err(anyhow::Error::new)
            .context("failed to cast Context Book vote")?;
        self.sync_vote_snapshot_with_fallback(&client, &session, &cast)
            .await?;
        Ok(cast)
    }

    fn ensure_remote_writes_available(&self, operation: &str) -> Result<()> {
        if self
            .handle
            .has_degraded_mode(ContextBookDegradedMode::Disconnect)
        {
            anyhow::bail!(
                "Context Book contract validation requires disconnect; remote {operation} are unavailable"
            );
        }
        if self
            .handle
            .has_degraded_mode(ContextBookDegradedMode::ReadOnly)
            || self
                .handle
                .has_degraded_mode(ContextBookDegradedMode::NoWrite)
        {
            anyhow::bail!(
                "Context Book is in a degraded no-write/read-only mode; remote {operation} are unavailable"
            );
        }
        Ok(())
    }

    async fn write_client_and_session(
        &self,
        operation: &str,
    ) -> Result<(ContextBookClient, super::client::ContextBookSession)> {
        let client = ContextBookClient::new(&self.handle.source_config());
        let session = client
            .ensure_session()
            .await
            .map_err(anyhow::Error::new)
            .with_context(|| format!("failed to establish Context Book session for {operation}"))?;
        client
            .activate_agent(&session)
            .await
            .map_err(anyhow::Error::new)
            .with_context(|| format!("failed to activate Context Book agent before {operation}"))?;
        Ok((client, session))
    }

    async fn sync_context_snapshot_with_fallback(
        &self,
        client: &ContextBookClient,
        session: &super::client::ContextBookSession,
        context: &ContextBookContextSnapshot,
    ) -> Result<()> {
        if let Err(error) = self.handle.store().save_context_snapshot(context) {
            self.refresh_context_cache(client, session)
                .await
                .map_err(|refresh_error| {
                    sync_recovery_error(
                        "context write succeeded remotely but failed to sync local cache",
                        error,
                        refresh_error,
                    )
                })?;
        }
        Ok(())
    }

    async fn delete_context_with_fallback(
        &self,
        client: &ContextBookClient,
        session: &super::client::ContextBookSession,
        context_id: &str,
    ) -> Result<()> {
        if let Err(error) = self.handle.store().delete_context(context_id) {
            self.refresh_context_cache(client, session)
                .await
                .map_err(|refresh_error| {
                    sync_recovery_error(
                        "context delete succeeded remotely but failed to sync local cache",
                        error,
                        refresh_error,
                    )
                })?;
        }
        Ok(())
    }

    async fn sync_vote_snapshot_with_fallback(
        &self,
        client: &ContextBookClient,
        session: &super::client::ContextBookSession,
        vote: &ContextBookVoteSnapshot,
    ) -> Result<()> {
        if let Err(error) = self.handle.store().save_vote_snapshot(vote) {
            self.refresh_vote_cache(client, session)
                .await
                .map_err(|refresh_error| {
                    sync_recovery_error(
                        "vote write succeeded remotely but failed to sync local cache",
                        error,
                        refresh_error,
                    )
                })?;
        }
        Ok(())
    }

    async fn delete_vote_with_fallback(
        &self,
        client: &ContextBookClient,
        session: &super::client::ContextBookSession,
        vote_id: &str,
    ) -> Result<()> {
        if let Err(error) = self.handle.store().delete_vote(vote_id) {
            self.refresh_vote_cache(client, session)
                .await
                .map_err(|refresh_error| {
                    sync_recovery_error(
                        "vote delete succeeded remotely but failed to sync local cache",
                        error,
                        refresh_error,
                    )
                })?;
        }
        Ok(())
    }

    async fn refresh_context_cache(
        &self,
        client: &ContextBookClient,
        session: &super::client::ContextBookSession,
    ) -> Result<()> {
        let contexts = client
            .get_contexts(session)
            .await
            .map_err(anyhow::Error::new)
            .context("failed to refresh contexts after local cache sync failure")?;
        self.handle
            .store()
            .save_contexts(&contexts)
            .context("failed to persist refreshed Context Book contexts")
    }

    async fn refresh_vote_cache(
        &self,
        client: &ContextBookClient,
        session: &super::client::ContextBookSession,
    ) -> Result<()> {
        let votes = client
            .get_votes(session)
            .await
            .map_err(anyhow::Error::new)
            .context("failed to refresh votes after local cache sync failure")?;
        self.handle
            .store()
            .save_votes(&votes)
            .context("failed to persist refreshed Context Book votes")
    }

    async fn select_policy_contexts(
        &self,
        read_mode: ContextBookPolicyReadMode,
        cached: Option<&ContextBookCachedItems<ContextBookContextSnapshot>>,
    ) -> Result<(
        ContextBookCachedItems<ContextBookContextSnapshot>,
        ContextBookPolicySource,
    )> {
        match read_mode {
            ContextBookPolicyReadMode::Cache => Ok((
                cached.cloned().unwrap_or_else(empty_cached_items),
                ContextBookPolicySource::Cache,
            )),
            ContextBookPolicyReadMode::Remote => {
                Ok((self.get_contexts().await?, ContextBookPolicySource::Remote))
            }
            ContextBookPolicyReadMode::Auto => {
                if let Some(cached) = cached
                    && !cached.items.is_empty()
                {
                    return Ok((cached.clone(), ContextBookPolicySource::Cache));
                }
                match self.get_contexts().await {
                    Ok(remote) => Ok((remote, ContextBookPolicySource::Remote)),
                    Err(error) => {
                        if let Some(cached) = cached {
                            Ok((cached.clone(), ContextBookPolicySource::Cache))
                        } else {
                            Err(error)
                        }
                    }
                }
            }
        }
    }

    async fn select_policy_votes(
        &self,
        read_mode: ContextBookPolicyReadMode,
        cached: Option<&ContextBookCachedItems<ContextBookVoteSnapshot>>,
    ) -> Result<(
        ContextBookCachedItems<ContextBookVoteSnapshot>,
        ContextBookPolicySource,
    )> {
        match read_mode {
            ContextBookPolicyReadMode::Cache => Ok((
                cached.cloned().unwrap_or_else(empty_cached_items),
                ContextBookPolicySource::Cache,
            )),
            ContextBookPolicyReadMode::Remote => {
                Ok((self.get_votes().await?, ContextBookPolicySource::Remote))
            }
            ContextBookPolicyReadMode::Auto => {
                if let Some(cached) = cached
                    && !cached.items.is_empty()
                {
                    return Ok((cached.clone(), ContextBookPolicySource::Cache));
                }
                match self.get_votes().await {
                    Ok(remote) => Ok((remote, ContextBookPolicySource::Remote)),
                    Err(error) => {
                        if let Some(cached) = cached {
                            Ok((cached.clone(), ContextBookPolicySource::Cache))
                        } else {
                            Err(error)
                        }
                    }
                }
            }
        }
    }
}

fn empty_cached_items<T>() -> ContextBookCachedItems<T> {
    ContextBookCachedItems {
        items: Vec::new(),
        updated_at: None,
    }
}

fn validate_context_create(
    agent_id: &str,
    request: &ContextBookContextCreateRequest,
) -> Result<()> {
    if request.title.trim().is_empty() {
        anyhow::bail!("'title' is required");
    }
    if request.contents.trim().is_empty() {
        anyhow::bail!("'contents' is required");
    }
    if request.tag.trim().is_empty() {
        anyhow::bail!("'tag' is required");
    }
    validate_context_status(&request.status)?;
    if let Some(context_id) = request
        .context_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        && !context_id.starts_with(&format!("{agent_id}_"))
    {
        anyhow::bail!("custom context_id must start with '{agent_id}_'");
    }
    Ok(())
}

fn validate_context_update(request: &ContextBookContextUpdateRequest) -> Result<()> {
    if request.title.is_none()
        && request.contents.is_none()
        && request.tag.is_none()
        && request.status.is_none()
    {
        anyhow::bail!("at least one of 'title', 'contents', 'tag', or 'status' must be provided");
    }
    if let Some(title) = request.title.as_deref()
        && title.trim().is_empty()
    {
        anyhow::bail!("'title' cannot be blank");
    }
    if let Some(contents) = request.contents.as_deref()
        && contents.trim().is_empty()
    {
        anyhow::bail!("'contents' cannot be blank");
    }
    if let Some(tag) = request.tag.as_deref()
        && tag.trim().is_empty()
    {
        anyhow::bail!("'tag' cannot be blank");
    }
    if let Some(status) = request.status.as_deref() {
        validate_context_status(status)?;
    }
    Ok(())
}

fn validate_context_status(status: &str) -> Result<()> {
    if !matches!(status.trim(), "Published" | "Archived") {
        anyhow::bail!("'status' must be one of: Published, Archived");
    }
    Ok(())
}

fn validate_vote_create(agent_id: &str, request: &ContextBookVoteCreateRequest) -> Result<()> {
    if request.vote_context.trim().is_empty() {
        anyhow::bail!("'vote_context' is required");
    }
    if let Some(vote_score) = request.vote_score {
        validate_finite_score(vote_score, "'vote_score'")?;
    }
    if let Some(vote_id) = request
        .vote_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        && !vote_id.starts_with(&format!("{agent_id}_"))
    {
        anyhow::bail!("custom vote_id must start with '{agent_id}_'");
    }
    Ok(())
}

fn validate_vote_update(
    agent_id: &str,
    cached: Option<&ContextBookVoteSnapshot>,
    request: &ContextBookVoteUpdateRequest,
) -> Result<()> {
    if request.vote_score.is_none() && request.vote_context.is_none() {
        anyhow::bail!("at least one of 'vote_score' or 'vote_context' must be provided");
    }
    if let Some(vote_score) = request.vote_score {
        validate_finite_score(vote_score, "'vote_score'")?;
    }
    if let Some(vote_context) = request.vote_context.as_deref()
        && vote_context.trim().is_empty()
    {
        anyhow::bail!("'vote_context' cannot be blank");
    }
    if let Some(cached) = cached
        && cached.owner_agent_id != agent_id
    {
        if request.vote_context.is_some() {
            anyhow::bail!("only the vote owner may update 'vote_context'");
        }
        if request.vote_score.is_none() {
            anyhow::bail!("non-owner vote updates must provide 'vote_score'");
        }
    }
    Ok(())
}

fn validate_vote_cast(
    agent_id: &str,
    cached: Option<&ContextBookVoteSnapshot>,
    request: &ContextBookVoteCastRequest,
) -> Result<()> {
    if let Some(vote_score) = request.vote_score {
        validate_positive_score(vote_score, "'vote_score'")?;
    }
    if let Some(cached) = cached {
        if cached.owner_agent_id == agent_id {
            anyhow::bail!(
                "vote owner '{agent_id}' cannot cast on vote '{}'",
                cached.vote_id
            );
        }
        if cached.voter_agent_ids.iter().any(|voter| voter == agent_id) {
            anyhow::bail!(
                "agent '{agent_id}' has already cast on vote '{}'",
                cached.vote_id
            );
        }
    }
    Ok(())
}

fn validate_finite_score(score: f64, field: &str) -> Result<()> {
    if !score.is_finite() {
        anyhow::bail!("{field} must be a finite number");
    }
    Ok(())
}

fn validate_positive_score(score: f64, field: &str) -> Result<()> {
    validate_finite_score(score, field)?;
    if score <= 0.0 {
        anyhow::bail!("{field} must be greater than 0");
    }
    Ok(())
}

fn sync_recovery_error(
    context: &str,
    initial: impl Display,
    recovery: impl Display,
) -> anyhow::Error {
    anyhow::anyhow!("{context}: {initial}; fallback refresh also failed: {recovery}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::profiles::{AuthProfile, AuthProfileKind, AuthProfilesStore, profile_id};
    use crate::auth::state_dir_from_config;
    use crate::config::Config;
    use crate::context_book::shared_handle;
    use crate::memory::traits::Memory;
    use axum::{
        Json, Router,
        extract::Path as AxumPath,
        http::StatusCode,
        response::IntoResponse,
        routing::{delete, get, patch, post},
    };
    use chrono::Utc;
    use serde_json::{Value, json};
    use std::collections::BTreeMap;
    use tempfile::TempDir;
    use tokio::net::TcpListener;

    fn test_config(tmp: &TempDir, base_url: String) -> Config {
        let mut config = Config {
            workspace_dir: tmp.path().join("workspace"),
            config_path: tmp.path().join("config.toml"),
            ..Config::default()
        };
        config.context_book.enabled = true;
        config.context_book.discovery_enabled = false;
        config.context_book.manual_url = Some(base_url);
        config.context_book.allowed_hosts = vec!["127.0.0.1".into()];
        config.context_book.allow_private_hosts = true;
        config
    }

    async fn seed_token_profile(config: &Config, agent_id: &str) {
        let state_dir = state_dir_from_config(config);
        let store = AuthProfilesStore::new(&state_dir, config.secrets.encrypt);
        store
            .upsert_profile(
                AuthProfile {
                    id: profile_id("context-book", "default"),
                    provider: "context-book".into(),
                    profile_name: "default".into(),
                    kind: AuthProfileKind::Token,
                    account_id: None,
                    workspace_id: None,
                    token_set: None,
                    token: Some("service-token".into()),
                    metadata: BTreeMap::from([("agent_id".to_string(), agent_id.to_string())]),
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                },
                true,
            )
            .await
            .expect("seed token profile");
    }

    #[tokio::test]
    async fn context_writes_sync_local_cache() {
        async fn activate(AxumPath(agent_id): AxumPath<String>) -> impl IntoResponse {
            assert_eq!(agent_id, "workspace");
            StatusCode::NO_CONTENT
        }

        async fn create_context(Json(body): Json<Value>) -> impl IntoResponse {
            assert_eq!(body["contextId"], "workspace_ctx9");
            assert_eq!(body["title"], "Morning Brief");
            assert_eq!(body["contents"], "Summary");
            assert_eq!(body["tag"], "daily");
            assert_eq!(body["status"], "Published");
            Json(json!({
                "context": {
                    "contextId": "workspace_ctx9",
                    "authorAgentId": "workspace",
                    "title": "Morning Brief",
                    "contents": "Summary",
                    "tag": "daily",
                    "status": "Published",
                    "createdAt": "2026-03-29T00:00:00Z",
                    "updatedAt": "2026-03-29T00:00:00Z"
                }
            }))
        }

        async fn delete_context(AxumPath(context_id): AxumPath<String>) -> impl IntoResponse {
            assert_eq!(context_id, "workspace_ctx9");
            StatusCode::NO_CONTENT
        }

        let app = Router::new()
            .route("/agents/{agent_id}/status", patch(activate))
            .route("/contexts", post(create_context))
            .route("/contexts/{context_id}", delete(delete_context));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve axum");
        });

        let tmp = TempDir::new().expect("temp dir");
        let config = test_config(&tmp, format!("http://{addr}"));
        seed_token_profile(&config, "workspace").await;

        let handle = shared_handle(&config);
        let service = ContextBookService::new(handle.clone());

        let created = service
            .create_context(&ContextBookContextCreateRequest {
                context_id: Some("workspace_ctx9".into()),
                title: "Morning Brief".into(),
                contents: "Summary".into(),
                tag: "daily".into(),
                status: "Published".into(),
            })
            .await
            .expect("create context");

        assert_eq!(created.context_id, "workspace_ctx9");
        let cached = handle
            .store()
            .load_context("workspace_ctx9")
            .expect("load cached context")
            .expect("cached context");
        assert_eq!(cached.title, "Morning Brief");

        service
            .delete_context("workspace_ctx9")
            .await
            .expect("delete context");
        assert!(
            handle
                .store()
                .load_context("workspace_ctx9")
                .expect("reload cached context")
                .is_none()
        );

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn cast_vote_rejects_owner_and_duplicate_from_cache() {
        async fn activate(AxumPath(agent_id): AxumPath<String>) -> impl IntoResponse {
            assert_eq!(agent_id, "workspace");
            StatusCode::NO_CONTENT
        }

        let app = Router::new().route("/agents/{agent_id}/status", patch(activate));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve axum");
        });

        let tmp = TempDir::new().expect("temp dir");
        let config = test_config(&tmp, format!("http://{addr}"));
        seed_token_profile(&config, "workspace").await;

        let handle = shared_handle(&config);
        let service = ContextBookService::new(handle.clone());

        handle
            .store()
            .save_vote_snapshot(&ContextBookVoteSnapshot {
                vote_id: "workspace_vote1".into(),
                owner_agent_id: "workspace".into(),
                vote_score: 1.0,
                vote_context: "approve".into(),
                voter_agent_ids: vec!["workspace".into()],
                required_score: Some(2),
                executable: Some(false),
                created_at: Some("2026-03-29T00:00:00Z".into()),
                updated_at: Some("2026-03-29T00:00:00Z".into()),
                raw_json: json!({"voteId": "workspace_vote1"}),
                synced_at: "2026-03-29T00:00:01Z".into(),
            })
            .expect("save owner vote snapshot");

        let owner_error = service
            .cast_vote("workspace_vote1", &ContextBookVoteCastRequest::default())
            .await
            .expect_err("owner cast should be rejected");
        assert!(owner_error.to_string().contains("cannot cast"));

        handle
            .store()
            .save_vote_snapshot(&ContextBookVoteSnapshot {
                vote_id: "peer_vote1".into(),
                owner_agent_id: "peer-a".into(),
                vote_score: 2.0,
                vote_context: "ship".into(),
                voter_agent_ids: vec!["workspace".into()],
                required_score: Some(2),
                executable: Some(true),
                created_at: Some("2026-03-29T00:00:00Z".into()),
                updated_at: Some("2026-03-29T00:00:00Z".into()),
                raw_json: json!({"voteId": "peer_vote1"}),
                synced_at: "2026-03-29T00:00:01Z".into(),
            })
            .expect("save duplicate vote snapshot");

        let duplicate_error = service
            .cast_vote("peer_vote1", &ContextBookVoteCastRequest::default())
            .await
            .expect_err("duplicate cast should be rejected");
        assert!(duplicate_error.to_string().contains("already cast"));

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn policy_reference_uses_remote_read_through_when_cache_is_empty() {
        async fn contexts() -> impl IntoResponse {
            Json(json!([
                {
                    "contextId": "peer_ctx_launch",
                    "authorAgentId": "peer-a",
                    "title": "Launch Plan",
                    "contents": "Ship launch plan today",
                    "tag": "eng",
                    "status": "Published",
                    "createdAt": "2026-03-29T00:00:00Z",
                    "updatedAt": "2026-03-29T00:00:01Z"
                }
            ]))
        }

        async fn votes() -> impl IntoResponse {
            Json(json!([
                {
                    "voteId": "peer_vote_launch",
                    "ownerAgentId": "peer-a",
                    "voteScore": 1,
                    "voteContext": "Approve launch plan from peer_ctx_launch",
                    "voterAgentIds": ["peer-b"],
                    "requiredScore": 2,
                    "executable": false,
                    "createdAt": "2026-03-29T00:00:00Z",
                    "updatedAt": "2026-03-29T00:00:02Z"
                }
            ]))
        }

        let app = Router::new()
            .route("/contexts", get(contexts))
            .route("/votes", get(votes));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve axum");
        });

        let tmp = TempDir::new().expect("temp dir");
        let config = test_config(&tmp, format!("http://{addr}"));
        seed_token_profile(&config, "workspace").await;

        let service = ContextBookService::new(shared_handle(&config));
        let reference = service
            .build_policy_reference(ContextBookPolicyReadMode::Auto, Some("launch"))
            .await
            .expect("policy reference")
            .expect("policy reference should exist");

        assert_eq!(reference.source, ContextBookPolicySource::Remote);
        assert_eq!(reference.castable_votes.len(), 1);
        assert_eq!(
            reference.castable_votes[0].related_context_ids,
            vec!["peer_ctx_launch"]
        );
        assert!(reference.prompt_block.contains("cast opportunities"));

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn policy_reference_does_not_write_context_book_data_into_memory() {
        let tmp = TempDir::new().expect("temp dir");
        let config = test_config(&tmp, "http://127.0.0.1:1".to_string());
        let handle = shared_handle(&config);
        handle
            .store()
            .save_context_snapshot(&ContextBookContextSnapshot {
                context_id: "peer_ctx_memory".into(),
                author_agent_id: "peer-a".into(),
                title: "Memory Isolation".into(),
                contents: "Do not store this in memory automatically".into(),
                tag: "ops".into(),
                status: "Published".into(),
                created_at: Some("2026-03-29T00:00:00Z".into()),
                updated_at: Some("2026-03-29T00:00:01Z".into()),
                raw_json: json!({"contextId": "peer_ctx_memory"}),
                synced_at: "2026-03-29T00:00:02Z".into(),
            })
            .expect("save cached context");
        handle
            .store()
            .save_vote_snapshot(&ContextBookVoteSnapshot {
                vote_id: "peer_vote_memory".into(),
                owner_agent_id: "peer-a".into(),
                vote_score: 1.0,
                vote_context: "Review memory isolation context".into(),
                voter_agent_ids: vec![],
                required_score: Some(2),
                executable: Some(false),
                created_at: Some("2026-03-29T00:00:00Z".into()),
                updated_at: Some("2026-03-29T00:00:01Z".into()),
                raw_json: json!({"voteId": "peer_vote_memory"}),
                synced_at: "2026-03-29T00:00:02Z".into(),
            })
            .expect("save cached vote");

        let service = ContextBookService::new(handle);
        let reference = service
            .build_policy_reference(ContextBookPolicyReadMode::Cache, Some("memory isolation"))
            .await
            .expect("policy reference")
            .expect("policy reference should exist");
        assert!(reference.has_actionable_items());

        let memory = crate::memory::SqliteMemory::new(&config.workspace_dir).expect("memory");
        assert_eq!(memory.count().await.expect("memory count"), 0);
        assert!(
            memory
                .recall("Memory Isolation", 10, None, None, None)
                .await
                .expect("memory recall")
                .is_empty()
        );
    }
}
