use super::events::ContextBookEventEnvelope;
use super::handle::ContextBookRuntimeSnapshot;
use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug, Clone, Serialize)]
pub struct ContextBookPersistedRuntimeState {
    pub owner_mode: String,
    pub worker_state: String,
    pub agent_id: Option<String>,
    pub lifecycle_state: String,
    pub connection_state: String,
    pub status_message: Option<String>,
    pub last_error: Option<String>,
    pub last_event_id: Option<String>,
    pub cursor_generation: i64,
    pub last_connect_at: Option<String>,
    pub last_sync_at: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ContextBookSubscriptionsSnapshot {
    pub consumer_agent_id: Option<String>,
    pub desired_producer_agent_ids: Vec<String>,
    pub effective_producer_agent_ids: Vec<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContextBookAgentSnapshot {
    pub agent_id: String,
    pub device_type: Option<String>,
    pub display_name: Option<String>,
    pub lifecycle_state: Option<String>,
    pub connection_state: Option<String>,
    pub last_seen_at: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub raw_json: Value,
    pub synced_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContextBookContextSnapshot {
    pub context_id: String,
    pub author_agent_id: String,
    pub title: String,
    pub contents: String,
    pub tag: String,
    pub status: String,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub raw_json: Value,
    pub synced_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContextBookVoteSnapshot {
    pub vote_id: String,
    pub owner_agent_id: String,
    pub vote_score: f64,
    pub vote_context: String,
    pub voter_agent_ids: Vec<String>,
    pub required_score: Option<i64>,
    pub executable: Option<bool>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub raw_json: Value,
    pub synced_at: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ContextBookCachedItems<T> {
    pub items: Vec<T>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ContextBookCacheCollectionSummary {
    pub count: usize,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ContextBookCacheInventory {
    pub seen_event_count: i64,
    pub agents: ContextBookCacheCollectionSummary,
    pub contexts: ContextBookCacheCollectionSummary,
    pub votes: ContextBookCacheCollectionSummary,
}

#[derive(Debug, Clone, Default)]
pub struct ContextBookEventSyncUpdate {
    pub subscriptions: Option<ContextBookSubscriptionsSnapshot>,
    pub agents: Option<Vec<ContextBookAgentSnapshot>>,
    pub contexts: Option<Vec<ContextBookContextSnapshot>>,
    pub votes: Option<Vec<ContextBookVoteSnapshot>>,
}

#[derive(Debug)]
pub struct ContextBookStore {
    path: PathBuf,
}

impl ContextBookStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn initialize(&self) -> Result<()> {
        self.with_connection(|conn| {
            conn.execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = NORMAL;
                 PRAGMA foreign_keys = ON;
                 CREATE TABLE IF NOT EXISTS cb_runtime_state (
                     singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                     owner_mode TEXT NOT NULL,
                     worker_state TEXT NOT NULL,
                     lifecycle_state TEXT NOT NULL,
                     connection_state TEXT NOT NULL,
                     status_message TEXT,
                     last_error TEXT,
                     updated_at TEXT NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS cb_cursor_state (
                     singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                     agent_id TEXT,
                     last_event_id TEXT,
                     cursor_generation INTEGER NOT NULL DEFAULT 0,
                     last_connect_at TEXT,
                     last_sync_at TEXT,
                     updated_at TEXT NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS cb_events_seen (
                     event_id TEXT PRIMARY KEY,
                     event_type TEXT NOT NULL,
                     occurred_at TEXT NOT NULL,
                     producer_agent_id TEXT NOT NULL,
                     raw_json TEXT NOT NULL,
                     recorded_at TEXT NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS cb_subscription_state (
                     singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                     consumer_agent_id TEXT,
                     updated_at TEXT NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS cb_desired_subscriptions (
                     producer_agent_id TEXT PRIMARY KEY,
                     updated_at TEXT NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS cb_effective_subscriptions (
                     producer_agent_id TEXT PRIMARY KEY,
                     updated_at TEXT NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS cb_agent_snapshots (
                     agent_id TEXT PRIMARY KEY,
                     device_type TEXT,
                     display_name TEXT,
                     lifecycle_state TEXT,
                     connection_state TEXT,
                     last_seen_at TEXT,
                     created_at TEXT,
                     updated_at TEXT,
                     raw_json TEXT NOT NULL,
                     synced_at TEXT NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS cb_context_snapshots (
                     context_id TEXT PRIMARY KEY,
                     author_agent_id TEXT NOT NULL,
                     title TEXT NOT NULL,
                     contents TEXT NOT NULL,
                     tag TEXT NOT NULL,
                     status TEXT NOT NULL,
                     created_at TEXT,
                     updated_at TEXT,
                     raw_json TEXT NOT NULL,
                     synced_at TEXT NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS cb_vote_snapshots (
                     vote_id TEXT PRIMARY KEY,
                     owner_agent_id TEXT NOT NULL,
                     vote_score REAL NOT NULL,
                     vote_context TEXT NOT NULL,
                     voter_agent_ids_json TEXT NOT NULL,
                     required_score INTEGER,
                     executable INTEGER,
                     created_at TEXT,
                     updated_at TEXT,
                     raw_json TEXT NOT NULL,
                     synced_at TEXT NOT NULL
                 );",
            )
            .context("failed to initialize context_book schema")?;
            Ok(())
        })
    }

    pub fn save_runtime_state(&self, snapshot: &ContextBookRuntimeSnapshot) -> Result<()> {
        self.initialize()?;
        self.with_connection(|conn| {
            conn.execute(
                "INSERT INTO cb_runtime_state (
                     singleton, owner_mode, worker_state, lifecycle_state, connection_state,
                     status_message, last_error, updated_at
                 ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(singleton) DO UPDATE SET
                     owner_mode = excluded.owner_mode,
                     worker_state = excluded.worker_state,
                     lifecycle_state = excluded.lifecycle_state,
                     connection_state = excluded.connection_state,
                     status_message = excluded.status_message,
                     last_error = excluded.last_error,
                     updated_at = excluded.updated_at",
                params![
                    snapshot.owner_mode,
                    snapshot.worker_state,
                    snapshot.lifecycle_state,
                    snapshot.connection_state,
                    snapshot.status_message,
                    snapshot.last_error,
                    snapshot.last_status_at,
                ],
            )
            .context("failed to persist context_book runtime state")?;
            conn.execute(
                "INSERT INTO cb_cursor_state (
                     singleton, agent_id, last_event_id, cursor_generation,
                     last_connect_at, last_sync_at, updated_at
                 ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(singleton) DO UPDATE SET
                     agent_id = excluded.agent_id,
                     last_event_id = excluded.last_event_id,
                     cursor_generation = excluded.cursor_generation,
                     last_connect_at = excluded.last_connect_at,
                     last_sync_at = excluded.last_sync_at,
                     updated_at = excluded.updated_at",
                params![
                    snapshot.agent_id,
                    snapshot.last_event_id,
                    snapshot.cursor_generation,
                    snapshot.last_connect_at,
                    snapshot.last_sync_at,
                    snapshot.last_status_at,
                ],
            )
            .context("failed to persist context_book cursor state")?;
            Ok(())
        })
    }

    pub fn record_event(
        &self,
        event: &ContextBookEventEnvelope,
        snapshot: &ContextBookRuntimeSnapshot,
    ) -> Result<bool> {
        self.apply_event_sync(event, snapshot, ContextBookEventSyncUpdate::default())
    }

    pub fn apply_event_sync(
        &self,
        event: &ContextBookEventEnvelope,
        snapshot: &ContextBookRuntimeSnapshot,
        update: ContextBookEventSyncUpdate,
    ) -> Result<bool> {
        self.initialize()?;
        self.with_connection(|conn| {
            let tx = conn.unchecked_transaction()?;
            let inserted = tx
                .execute(
                    "INSERT OR IGNORE INTO cb_events_seen (
                         event_id, event_type, occurred_at, producer_agent_id, raw_json, recorded_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        event.event_id,
                        event.event_type,
                        event.occurred_at,
                        event.producer_agent_id,
                        serde_json::to_string(event)
                            .context("failed to serialize context_book event")?,
                        Utc::now().to_rfc3339(),
                    ],
                )
                .context("failed to insert context_book event dedup record")?;
            if inserted > 0 {
                if let Some(subscriptions) = update.subscriptions.as_ref() {
                    replace_subscriptions_tx(&tx, subscriptions)?;
                }
                if let Some(agents) = update.agents.as_ref() {
                    replace_agents_tx(&tx, agents)?;
                }
                if let Some(contexts) = update.contexts.as_ref() {
                    replace_contexts_tx(&tx, contexts)?;
                }
                if let Some(votes) = update.votes.as_ref() {
                    replace_votes_tx(&tx, votes)?;
                }
                tx.execute(
                    "INSERT INTO cb_cursor_state (
                         singleton, agent_id, last_event_id, cursor_generation,
                         last_connect_at, last_sync_at, updated_at
                     ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT(singleton) DO UPDATE SET
                         agent_id = excluded.agent_id,
                         last_event_id = excluded.last_event_id,
                         cursor_generation = excluded.cursor_generation,
                         last_connect_at = excluded.last_connect_at,
                         last_sync_at = excluded.last_sync_at,
                         updated_at = excluded.updated_at",
                    params![
                        snapshot.agent_id,
                        event.event_id,
                        snapshot.cursor_generation,
                        snapshot.last_connect_at,
                        snapshot.last_sync_at,
                        snapshot.last_status_at,
                    ],
                )
                .context("failed to persist context_book cursor after event")?;
                tx.execute(
                    "INSERT INTO cb_runtime_state (
                         singleton, owner_mode, worker_state, lifecycle_state, connection_state,
                         status_message, last_error, updated_at
                     ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7)
                     ON CONFLICT(singleton) DO UPDATE SET
                         owner_mode = excluded.owner_mode,
                         worker_state = excluded.worker_state,
                         lifecycle_state = excluded.lifecycle_state,
                         connection_state = excluded.connection_state,
                         status_message = excluded.status_message,
                         last_error = excluded.last_error,
                         updated_at = excluded.updated_at",
                    params![
                        snapshot.owner_mode,
                        snapshot.worker_state,
                        snapshot.lifecycle_state,
                        snapshot.connection_state,
                        snapshot.status_message,
                        snapshot.last_error,
                        snapshot.last_status_at,
                    ],
                )
                .context("failed to persist runtime state after event")?;
            }
            tx.commit()
                .context("failed to commit context_book event transaction")?;
            Ok(inserted > 0)
        })
    }

    pub fn reset_cursor(&self, snapshot: &ContextBookRuntimeSnapshot) -> Result<()> {
        self.initialize()?;
        self.with_connection(|conn| {
            conn.execute(
                "INSERT INTO cb_cursor_state (
                     singleton, agent_id, last_event_id, cursor_generation,
                     last_connect_at, last_sync_at, updated_at
                 ) VALUES (1, ?1, NULL, ?2, ?3, ?4, ?5)
                 ON CONFLICT(singleton) DO UPDATE SET
                     agent_id = excluded.agent_id,
                     last_event_id = NULL,
                     cursor_generation = excluded.cursor_generation,
                     last_connect_at = excluded.last_connect_at,
                     last_sync_at = excluded.last_sync_at,
                     updated_at = excluded.updated_at",
                params![
                    snapshot.agent_id,
                    snapshot.cursor_generation,
                    snapshot.last_connect_at,
                    snapshot.last_sync_at,
                    snapshot.last_status_at,
                ],
            )
            .context("failed to reset context_book cursor")?;
            Ok(())
        })
    }

    pub fn seen_event_count(&self) -> Result<i64> {
        if !self.path.exists() {
            return Ok(0);
        }

        self.with_connection(|conn| {
            let count = conn
                .query_row("SELECT COUNT(*) FROM cb_events_seen", [], |row| row.get(0))
                .context("failed to count seen context_book events")?;
            Ok(count)
        })
    }

    pub fn save_subscriptions(&self, snapshot: &ContextBookSubscriptionsSnapshot) -> Result<()> {
        self.initialize()?;
        self.with_connection(|conn| {
            let tx = conn.unchecked_transaction()?;
            replace_subscriptions_tx(&tx, snapshot)?;
            tx.commit()
                .context("failed to commit subscription snapshot transaction")?;
            Ok(())
        })
    }

    pub fn load_subscriptions(&self) -> Result<Option<ContextBookSubscriptionsSnapshot>> {
        if !self.path.exists() {
            return Ok(None);
        }

        self.with_connection(|conn| {
            let metadata = conn
                .query_row(
                    "SELECT consumer_agent_id, updated_at
                     FROM cb_subscription_state
                     WHERE singleton = 1",
                    [],
                    |row| Ok((row.get::<_, Option<String>>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()?;
            let Some((consumer_agent_id, updated_at)) = metadata else {
                return Ok(None);
            };

            let desired_producer_agent_ids = read_subscription_ids(
                conn,
                "SELECT producer_agent_id
                 FROM cb_desired_subscriptions
                 ORDER BY producer_agent_id ASC",
            )?;
            let effective_producer_agent_ids = read_subscription_ids(
                conn,
                "SELECT producer_agent_id
                 FROM cb_effective_subscriptions
                 ORDER BY producer_agent_id ASC",
            )?;

            Ok(Some(ContextBookSubscriptionsSnapshot {
                consumer_agent_id,
                desired_producer_agent_ids,
                effective_producer_agent_ids,
                updated_at,
            }))
        })
    }

    pub fn load_runtime_state(&self) -> Result<Option<ContextBookPersistedRuntimeState>> {
        if !self.path.exists() {
            return Ok(None);
        }

        self.with_connection(|conn| {
            let mut runtime_stmt = conn.prepare(
                "SELECT owner_mode, worker_state, lifecycle_state, connection_state,
                        status_message, last_error, updated_at
                 FROM cb_runtime_state WHERE singleton = 1",
            )?;
            let mut rows = runtime_stmt.query([])?;
            let Some(row) = rows.next()? else {
                return Ok(None);
            };

            let cursor = conn
                .query_row(
                    "SELECT agent_id, last_event_id, cursor_generation,
                            last_connect_at, last_sync_at, updated_at
                     FROM cb_cursor_state WHERE singleton = 1",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, Option<String>>(0)?,
                            row.get::<_, Option<String>>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, Option<String>>(3)?,
                            row.get::<_, Option<String>>(4)?,
                            row.get::<_, String>(5)?,
                        ))
                    },
                )
                .optional()?;

            Ok(Some(ContextBookPersistedRuntimeState {
                owner_mode: row.get(0)?,
                worker_state: row.get(1)?,
                agent_id: cursor.as_ref().and_then(|cursor| cursor.0.clone()),
                lifecycle_state: row.get(2)?,
                connection_state: row.get(3)?,
                status_message: row.get(4)?,
                last_error: row.get(5)?,
                last_event_id: cursor.as_ref().and_then(|cursor| cursor.1.clone()),
                cursor_generation: cursor.as_ref().map_or(0, |cursor| cursor.2),
                last_connect_at: cursor.as_ref().and_then(|cursor| cursor.3.clone()),
                last_sync_at: cursor.as_ref().and_then(|cursor| cursor.4.clone()),
                updated_at: cursor.map_or_else(|| row.get(6), |cursor| Ok(cursor.5))?,
            }))
        })
    }

    pub fn save_agents(&self, agents: &[ContextBookAgentSnapshot]) -> Result<()> {
        self.initialize()?;
        self.with_connection(|conn| {
            let tx = conn.unchecked_transaction()?;
            replace_agents_tx(&tx, agents)?;
            tx.commit()
                .context("failed to commit agent snapshot transaction")?;
            Ok(())
        })
    }

    pub fn load_agents(&self) -> Result<Option<ContextBookCachedItems<ContextBookAgentSnapshot>>> {
        if !self.path.exists() {
            return Ok(None);
        }

        self.with_connection(|conn| {
            let mut stmt = conn.prepare(
                "SELECT agent_id, device_type, display_name, lifecycle_state, connection_state,
                        last_seen_at, created_at, updated_at, raw_json, synced_at
                 FROM cb_agent_snapshots
                 ORDER BY agent_id ASC",
            )?;
            let rows = stmt.query_map([], |row| {
                let raw_json = row.get::<_, String>(8)?;
                Ok(ContextBookAgentSnapshot {
                    agent_id: row.get(0)?,
                    device_type: row.get(1)?,
                    display_name: row.get(2)?,
                    lifecycle_state: row.get(3)?,
                    connection_state: row.get(4)?,
                    last_seen_at: row.get(5)?,
                    created_at: row.get(6)?,
                    updated_at: row.get(7)?,
                    raw_json: serde_json::from_str(&raw_json).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            8,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?,
                    synced_at: row.get(9)?,
                })
            })?;
            let items = rows
                .collect::<rusqlite::Result<Vec<_>>>()
                .context("failed to load agent snapshots")?;
            if items.is_empty() {
                return Ok(None);
            }

            let updated_at = items.iter().map(|item| item.synced_at.clone()).max();
            Ok(Some(ContextBookCachedItems { items, updated_at }))
        })
    }

    pub fn save_contexts(&self, contexts: &[ContextBookContextSnapshot]) -> Result<()> {
        self.initialize()?;
        self.with_connection(|conn| {
            let tx = conn.unchecked_transaction()?;
            replace_contexts_tx(&tx, contexts)?;
            tx.commit()
                .context("failed to commit context snapshot transaction")?;
            Ok(())
        })
    }

    pub fn load_contexts(
        &self,
    ) -> Result<Option<ContextBookCachedItems<ContextBookContextSnapshot>>> {
        if !self.path.exists() {
            return Ok(None);
        }

        self.with_connection(|conn| {
            let mut stmt = conn.prepare(
                "SELECT context_id, author_agent_id, title, contents, tag, status,
                        created_at, updated_at, raw_json, synced_at
                 FROM cb_context_snapshots
                 ORDER BY context_id ASC",
            )?;
            let rows = stmt.query_map([], |row| {
                let raw_json = row.get::<_, String>(8)?;
                Ok(ContextBookContextSnapshot {
                    context_id: row.get(0)?,
                    author_agent_id: row.get(1)?,
                    title: row.get(2)?,
                    contents: row.get(3)?,
                    tag: row.get(4)?,
                    status: row.get(5)?,
                    created_at: row.get(6)?,
                    updated_at: row.get(7)?,
                    raw_json: serde_json::from_str(&raw_json).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            8,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?,
                    synced_at: row.get(9)?,
                })
            })?;
            let items = rows
                .collect::<rusqlite::Result<Vec<_>>>()
                .context("failed to load context snapshots")?;
            if items.is_empty() {
                return Ok(None);
            }

            let updated_at = items.iter().map(|item| item.synced_at.clone()).max();
            Ok(Some(ContextBookCachedItems { items, updated_at }))
        })
    }

    pub fn save_votes(&self, votes: &[ContextBookVoteSnapshot]) -> Result<()> {
        self.initialize()?;
        self.with_connection(|conn| {
            let tx = conn.unchecked_transaction()?;
            replace_votes_tx(&tx, votes)?;
            tx.commit()
                .context("failed to commit vote snapshot transaction")?;
            Ok(())
        })
    }

    pub fn load_votes(&self) -> Result<Option<ContextBookCachedItems<ContextBookVoteSnapshot>>> {
        if !self.path.exists() {
            return Ok(None);
        }

        self.with_connection(|conn| {
            let mut stmt = conn.prepare(
                "SELECT vote_id, owner_agent_id, vote_score, vote_context, voter_agent_ids_json,
                        required_score, executable, created_at, updated_at, raw_json, synced_at
                 FROM cb_vote_snapshots
                 ORDER BY vote_id ASC",
            )?;
            let rows = stmt.query_map([], |row| {
                let voter_agent_ids_json = row.get::<_, String>(4)?;
                let raw_json = row.get::<_, String>(9)?;
                let executable = row.get::<_, Option<i64>>(6)?.map(|value| value != 0);
                Ok(ContextBookVoteSnapshot {
                    vote_id: row.get(0)?,
                    owner_agent_id: row.get(1)?,
                    vote_score: row.get(2)?,
                    vote_context: row.get(3)?,
                    voter_agent_ids: serde_json::from_str(&voter_agent_ids_json).map_err(
                        |error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                4,
                                rusqlite::types::Type::Text,
                                Box::new(error),
                            )
                        },
                    )?,
                    required_score: row.get(5)?,
                    executable,
                    created_at: row.get(7)?,
                    updated_at: row.get(8)?,
                    raw_json: serde_json::from_str(&raw_json).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            9,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?,
                    synced_at: row.get(10)?,
                })
            })?;
            let items = rows
                .collect::<rusqlite::Result<Vec<_>>>()
                .context("failed to load vote snapshots")?;
            if items.is_empty() {
                return Ok(None);
            }

            let updated_at = items.iter().map(|item| item.synced_at.clone()).max();
            Ok(Some(ContextBookCachedItems { items, updated_at }))
        })
    }

    pub fn cache_inventory(&self) -> Result<ContextBookCacheInventory> {
        if !self.path.exists() {
            return Ok(ContextBookCacheInventory {
                seen_event_count: 0,
                agents: ContextBookCacheCollectionSummary {
                    count: 0,
                    updated_at: None,
                },
                contexts: ContextBookCacheCollectionSummary {
                    count: 0,
                    updated_at: None,
                },
                votes: ContextBookCacheCollectionSummary {
                    count: 0,
                    updated_at: None,
                },
            });
        }

        self.with_connection(|conn| {
            Ok(ContextBookCacheInventory {
                seen_event_count: conn
                    .query_row("SELECT COUNT(*) FROM cb_events_seen", [], |row| row.get(0))
                    .context("failed to count seen context_book events")?,
                agents: load_collection_summary(conn, "cb_agent_snapshots", "synced_at")?,
                contexts: load_collection_summary(conn, "cb_context_snapshots", "synced_at")?,
                votes: load_collection_summary(conn, "cb_vote_snapshots", "synced_at")?,
            })
        })
    }

    fn with_connection<T>(&self, f: impl FnOnce(&mut Connection) -> Result<T>) -> Result<T> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create context_book cache directory {}",
                    parent.display()
                )
            })?;
        }

        let mut conn = Connection::open(self.path())
            .with_context(|| format!("failed to open context_book db {}", self.path.display()))?;
        conn.busy_timeout(Duration::from_secs(1))
            .context("failed to configure context_book sqlite busy timeout")?;
        f(&mut conn)
    }
}

fn read_subscription_ids(conn: &Connection, query: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(query)?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let ids = rows
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to read subscription IDs")?;
    Ok(ids)
}

fn load_collection_summary(
    conn: &Connection,
    table: &str,
    updated_at_column: &str,
) -> Result<ContextBookCacheCollectionSummary> {
    let query = format!(
        "SELECT COUNT(*), MAX({updated_at_column}) FROM {table}",
        updated_at_column = updated_at_column,
        table = table
    );
    let (count, updated_at) = conn
        .query_row(&query, [], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?))
        })
        .with_context(|| format!("failed to summarize cache table {table}"))?;
    Ok(ContextBookCacheCollectionSummary {
        count: usize::try_from(count).unwrap_or(0),
        updated_at,
    })
}

fn replace_subscriptions_tx(
    tx: &rusqlite::Transaction<'_>,
    snapshot: &ContextBookSubscriptionsSnapshot,
) -> Result<()> {
    tx.execute(
        "INSERT INTO cb_subscription_state (singleton, consumer_agent_id, updated_at)
         VALUES (1, ?1, ?2)
         ON CONFLICT(singleton) DO UPDATE SET
             consumer_agent_id = excluded.consumer_agent_id,
             updated_at = excluded.updated_at",
        params![snapshot.consumer_agent_id, snapshot.updated_at,],
    )
    .context("failed to persist subscription state metadata")?;
    tx.execute("DELETE FROM cb_desired_subscriptions", [])
        .context("failed to clear desired subscriptions")?;
    tx.execute("DELETE FROM cb_effective_subscriptions", [])
        .context("failed to clear effective subscriptions")?;

    for producer_agent_id in normalize_agent_ids(&snapshot.desired_producer_agent_ids) {
        tx.execute(
            "INSERT INTO cb_desired_subscriptions (producer_agent_id, updated_at)
             VALUES (?1, ?2)",
            params![producer_agent_id, snapshot.updated_at],
        )
        .context("failed to persist desired subscription")?;
    }

    for producer_agent_id in normalize_agent_ids(&snapshot.effective_producer_agent_ids) {
        tx.execute(
            "INSERT INTO cb_effective_subscriptions (producer_agent_id, updated_at)
             VALUES (?1, ?2)",
            params![producer_agent_id, snapshot.updated_at],
        )
        .context("failed to persist effective subscription")?;
    }

    Ok(())
}

fn replace_agents_tx(
    tx: &rusqlite::Transaction<'_>,
    agents: &[ContextBookAgentSnapshot],
) -> Result<()> {
    tx.execute("DELETE FROM cb_agent_snapshots", [])
        .context("failed to clear cached agent snapshots")?;
    for agent in agents {
        tx.execute(
            "INSERT INTO cb_agent_snapshots (
                 agent_id, device_type, display_name, lifecycle_state, connection_state,
                 last_seen_at, created_at, updated_at, raw_json, synced_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                agent.agent_id,
                agent.device_type,
                agent.display_name,
                agent.lifecycle_state,
                agent.connection_state,
                agent.last_seen_at,
                agent.created_at,
                agent.updated_at,
                serde_json::to_string(&agent.raw_json)
                    .context("failed to serialize cached agent raw_json")?,
                agent.synced_at,
            ],
        )
        .context("failed to persist cached agent snapshot")?;
    }
    Ok(())
}

fn replace_contexts_tx(
    tx: &rusqlite::Transaction<'_>,
    contexts: &[ContextBookContextSnapshot],
) -> Result<()> {
    tx.execute("DELETE FROM cb_context_snapshots", [])
        .context("failed to clear cached context snapshots")?;
    for context in contexts {
        tx.execute(
            "INSERT INTO cb_context_snapshots (
                 context_id, author_agent_id, title, contents, tag, status,
                 created_at, updated_at, raw_json, synced_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                context.context_id,
                context.author_agent_id,
                context.title,
                context.contents,
                context.tag,
                context.status,
                context.created_at,
                context.updated_at,
                serde_json::to_string(&context.raw_json)
                    .context("failed to serialize cached context raw_json")?,
                context.synced_at,
            ],
        )
        .context("failed to persist cached context snapshot")?;
    }
    Ok(())
}

fn replace_votes_tx(
    tx: &rusqlite::Transaction<'_>,
    votes: &[ContextBookVoteSnapshot],
) -> Result<()> {
    tx.execute("DELETE FROM cb_vote_snapshots", [])
        .context("failed to clear cached vote snapshots")?;
    for vote in votes {
        tx.execute(
            "INSERT INTO cb_vote_snapshots (
                 vote_id, owner_agent_id, vote_score, vote_context, voter_agent_ids_json,
                 required_score, executable, created_at, updated_at, raw_json, synced_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                vote.vote_id,
                vote.owner_agent_id,
                vote.vote_score,
                vote.vote_context,
                serde_json::to_string(&vote.voter_agent_ids)
                    .context("failed to serialize cached vote voter_agent_ids")?,
                vote.required_score,
                vote.executable.map(i64::from),
                vote.created_at,
                vote.updated_at,
                serde_json::to_string(&vote.raw_json)
                    .context("failed to serialize cached vote raw_json")?,
                vote.synced_at,
            ],
        )
        .context("failed to persist cached vote snapshot")?;
    }
    Ok(())
}

fn normalize_agent_ids(values: &[String]) -> Vec<String> {
    let mut ids = values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    ids
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn store_initializes_and_round_trips_runtime_state() {
        let tmp = TempDir::new().expect("temp dir");
        let store = ContextBookStore::new(tmp.path().join("context_book").join("cache.db"));
        store.initialize().expect("initialize store");

        let snapshot = ContextBookRuntimeSnapshot {
            enabled: true,
            owner_mode: "daemon_supervised".into(),
            worker_state: "idle".into(),
            agent_id: Some("zc-agent".into()),
            lifecycle_state: "inactive".into(),
            connection_state: "disconnected".into(),
            shutdown_requested: false,
            status_message: Some("phase1 noop worker active".into()),
            last_error: None,
            last_status_at: "2026-03-29T00:00:00Z".into(),
            last_event_id: Some("evt-001".into()),
            cursor_generation: 1,
            last_connect_at: None,
            last_sync_at: None,
            cache_db_path: store.path().display().to_string(),
            store_initialized: true,
        };

        store
            .save_runtime_state(&snapshot)
            .expect("save runtime state");

        let persisted = store
            .load_runtime_state()
            .expect("load runtime state")
            .expect("persisted row");
        assert_eq!(persisted.worker_state, "idle");
        assert_eq!(persisted.updated_at, "2026-03-29T00:00:00Z");
        assert_eq!(persisted.agent_id.as_deref(), Some("zc-agent"));
        assert_eq!(persisted.last_event_id.as_deref(), Some("evt-001"));
        assert_eq!(persisted.cursor_generation, 1);
    }

    #[test]
    fn record_event_deduplicates_and_persists_cursor() {
        let tmp = TempDir::new().expect("temp dir");
        let store = ContextBookStore::new(tmp.path().join("context_book").join("cache.db"));
        let snapshot = ContextBookRuntimeSnapshot {
            enabled: true,
            owner_mode: "daemon_supervised".into(),
            worker_state: "streaming".into(),
            agent_id: Some("zc-agent".into()),
            lifecycle_state: "active".into(),
            connection_state: "connected".into(),
            shutdown_requested: false,
            status_message: Some("streaming".into()),
            last_error: None,
            last_status_at: "2026-03-29T00:00:00Z".into(),
            last_event_id: Some("evt-001".into()),
            cursor_generation: 0,
            last_connect_at: Some("2026-03-29T00:00:00Z".into()),
            last_sync_at: Some("2026-03-29T00:00:01Z".into()),
            cache_db_path: store.path().display().to_string(),
            store_initialized: true,
        };
        let event = ContextBookEventEnvelope {
            event_id: "evt-002".into(),
            event_type: "context.created".into(),
            occurred_at: "2026-03-29T00:00:02Z".into(),
            producer_agent_id: "peer".into(),
            entity_id: "ctx-1".into(),
            payload: serde_json::json!({}),
            meta: serde_json::json!({}),
        };

        assert!(store.record_event(&event, &snapshot).expect("insert event"));
        assert!(!store.record_event(&event, &snapshot).expect("dedup event"));
        assert_eq!(store.seen_event_count().expect("count events"), 1);

        let persisted = store
            .load_runtime_state()
            .expect("load runtime")
            .expect("persisted runtime");
        assert_eq!(persisted.last_event_id.as_deref(), Some("evt-002"));
    }

    #[test]
    fn store_round_trips_subscription_snapshots() {
        let tmp = TempDir::new().expect("temp dir");
        let store = ContextBookStore::new(tmp.path().join("context_book").join("cache.db"));
        let snapshot = ContextBookSubscriptionsSnapshot {
            consumer_agent_id: Some("zc-agent".into()),
            desired_producer_agent_ids: vec![
                "peer-b".into(),
                "peer-a".into(),
                "peer-a".into(),
                String::new(),
            ],
            effective_producer_agent_ids: vec!["peer-b".into(), "peer-c".into()],
            updated_at: "2026-03-29T00:00:00Z".into(),
        };

        store
            .save_subscriptions(&snapshot)
            .expect("save subscription snapshot");

        let persisted = store
            .load_subscriptions()
            .expect("load subscription snapshot")
            .expect("persisted subscriptions");
        assert_eq!(persisted.consumer_agent_id.as_deref(), Some("zc-agent"));
        assert_eq!(
            persisted.desired_producer_agent_ids,
            vec!["peer-a".to_string(), "peer-b".to_string()]
        );
        assert_eq!(
            persisted.effective_producer_agent_ids,
            vec!["peer-b".to_string(), "peer-c".to_string()]
        );
        assert_eq!(persisted.updated_at, "2026-03-29T00:00:00Z");
    }

    #[test]
    fn apply_event_sync_replaces_cached_snapshots_in_same_transaction() {
        let tmp = TempDir::new().expect("temp dir");
        let store = ContextBookStore::new(tmp.path().join("context_book").join("cache.db"));
        let snapshot = ContextBookRuntimeSnapshot {
            enabled: true,
            owner_mode: "daemon_supervised".into(),
            worker_state: "streaming".into(),
            agent_id: Some("zc-agent".into()),
            lifecycle_state: "active".into(),
            connection_state: "connected".into(),
            shutdown_requested: false,
            status_message: Some("streaming".into()),
            last_error: None,
            last_status_at: "2026-03-29T00:00:00Z".into(),
            last_event_id: None,
            cursor_generation: 0,
            last_connect_at: Some("2026-03-29T00:00:00Z".into()),
            last_sync_at: Some("2026-03-29T00:00:01Z".into()),
            cache_db_path: store.path().display().to_string(),
            store_initialized: true,
        };
        let event = ContextBookEventEnvelope {
            event_id: "evt-003".into(),
            event_type: "context.updated".into(),
            occurred_at: "2026-03-29T00:00:03Z".into(),
            producer_agent_id: "peer".into(),
            entity_id: "peer_ctx_1".into(),
            payload: serde_json::json!({}),
            meta: serde_json::json!({}),
        };
        let update = ContextBookEventSyncUpdate {
            subscriptions: Some(ContextBookSubscriptionsSnapshot {
                consumer_agent_id: Some("zc-agent".into()),
                desired_producer_agent_ids: vec!["peer".into()],
                effective_producer_agent_ids: vec!["peer".into()],
                updated_at: "2026-03-29T00:00:03Z".into(),
            }),
            agents: Some(vec![ContextBookAgentSnapshot {
                agent_id: "peer".into(),
                device_type: Some("notepc".into()),
                display_name: Some("Peer".into()),
                lifecycle_state: Some("Active".into()),
                connection_state: Some("Connected".into()),
                last_seen_at: Some("2026-03-29T00:00:03Z".into()),
                created_at: Some("2026-03-29T00:00:00Z".into()),
                updated_at: Some("2026-03-29T00:00:03Z".into()),
                raw_json: serde_json::json!({"agentId": "peer"}),
                synced_at: "2026-03-29T00:00:03Z".into(),
            }]),
            contexts: Some(vec![ContextBookContextSnapshot {
                context_id: "peer_ctx_1".into(),
                author_agent_id: "peer".into(),
                title: "Title".into(),
                contents: "Body".into(),
                tag: "ops".into(),
                status: "Published".into(),
                created_at: Some("2026-03-29T00:00:02Z".into()),
                updated_at: Some("2026-03-29T00:00:03Z".into()),
                raw_json: serde_json::json!({"contextId": "peer_ctx_1"}),
                synced_at: "2026-03-29T00:00:03Z".into(),
            }]),
            votes: Some(vec![ContextBookVoteSnapshot {
                vote_id: "peer_vote_1".into(),
                owner_agent_id: "peer".into(),
                vote_score: 2.0,
                vote_context: "ship".into(),
                voter_agent_ids: vec!["peer".into()],
                required_score: Some(2),
                executable: Some(true),
                created_at: Some("2026-03-29T00:00:02Z".into()),
                updated_at: Some("2026-03-29T00:00:03Z".into()),
                raw_json: serde_json::json!({"voteId": "peer_vote_1"}),
                synced_at: "2026-03-29T00:00:03Z".into(),
            }]),
        };

        assert!(
            store
                .apply_event_sync(&event, &snapshot, update)
                .expect("apply event sync")
        );

        let contexts = store
            .load_contexts()
            .expect("load contexts")
            .expect("contexts cache");
        let votes = store
            .load_votes()
            .expect("load votes")
            .expect("votes cache");
        let agents = store
            .load_agents()
            .expect("load agents")
            .expect("agents cache");
        let subscriptions = store
            .load_subscriptions()
            .expect("load subscriptions")
            .expect("subscription cache");
        let inventory = store.cache_inventory().expect("cache inventory");

        assert_eq!(contexts.items.len(), 1);
        assert_eq!(votes.items.len(), 1);
        assert_eq!(agents.items.len(), 1);
        assert_eq!(subscriptions.effective_producer_agent_ids, vec!["peer"]);
        assert_eq!(inventory.seen_event_count, 1);
        assert_eq!(inventory.contexts.count, 1);
        assert_eq!(inventory.votes.count, 1);
        assert_eq!(inventory.agents.count, 1);
    }
}
