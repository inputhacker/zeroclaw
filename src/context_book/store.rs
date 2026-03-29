use super::handle::ContextBookRuntimeSnapshot;
use anyhow::{Context, Result};
use rusqlite::{Connection, params};
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug, Clone, Serialize)]
pub struct ContextBookPersistedRuntimeState {
    pub owner_mode: String,
    pub worker_state: String,
    pub lifecycle_state: String,
    pub connection_state: String,
    pub status_message: Option<String>,
    pub last_error: Option<String>,
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
            Ok(())
        })
    }

    pub fn load_runtime_state(&self) -> Result<Option<ContextBookPersistedRuntimeState>> {
        if !self.path.exists() {
            return Ok(None);
        }

        self.with_connection(|conn| {
            let mut stmt = conn.prepare(
                "SELECT owner_mode, worker_state, lifecycle_state, connection_state,
                        status_message, last_error, updated_at
                 FROM cb_runtime_state WHERE singleton = 1",
            )?;
            let mut rows = stmt.query([])?;
            let Some(row) = rows.next()? else {
                return Ok(None);
            };

            Ok(Some(ContextBookPersistedRuntimeState {
                owner_mode: row.get(0)?,
                worker_state: row.get(1)?,
                lifecycle_state: row.get(2)?,
                connection_state: row.get(3)?,
                status_message: row.get(4)?,
                last_error: row.get(5)?,
                updated_at: row.get(6)?,
            }))
        })
    }

    fn with_connection<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create context_book cache directory {}",
                    parent.display()
                )
            })?;
        }

        let conn = Connection::open(self.path())
            .with_context(|| format!("failed to open context_book db {}", self.path.display()))?;
        conn.busy_timeout(Duration::from_secs(1))
            .context("failed to configure context_book sqlite busy timeout")?;
        f(&conn)
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
            lifecycle_state: "inactive".into(),
            connection_state: "disconnected".into(),
            shutdown_requested: false,
            status_message: Some("phase1 noop worker active".into()),
            last_error: None,
            last_status_at: "2026-03-29T00:00:00Z".into(),
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
    }
}
