use super::events::ContextBookEventEnvelope;
use super::handle::ContextBookRuntimeSnapshot;
use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
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
}
