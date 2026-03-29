use super::client::ContextBookClient;
use super::config::ResolvedContextBookConfig;
use super::handle::{
    ContextBookContractSnapshot, ContextBookDegradedMode, ContextBookHandle,
    ContextBookRuntimeSnapshot,
};
use super::store::{
    ContextBookAgentSnapshot, ContextBookCacheInventory, ContextBookCachedItems,
    ContextBookContextSnapshot, ContextBookPersistedRuntimeState, ContextBookSubscriptionsSnapshot,
    ContextBookVoteSnapshot,
};
use anyhow::{Context, Result};
use serde::Serialize;

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
        if self
            .handle
            .has_degraded_mode(ContextBookDegradedMode::Disconnect)
        {
            anyhow::bail!(
                "Context Book contract validation requires disconnect; remote subscription writes are unavailable"
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
                "Context Book is in a degraded no-write/read-only mode; remote subscription writes are unavailable"
            );
        }

        let client = ContextBookClient::new(&self.handle.source_config());
        let session = client
            .ensure_session()
            .await
            .map_err(anyhow::Error::new)
            .context("failed to establish Context Book session for subscriptions set")?;
        client
            .activate_agent(&session)
            .await
            .map_err(anyhow::Error::new)
            .context("failed to activate Context Book agent before subscriptions set")?;
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
}
