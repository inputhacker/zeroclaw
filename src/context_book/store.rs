use crate::config::ContextBookConfig;
use crate::context_book::types::{
    AgentRecordDto, ApprovalState, AuthSessionDto, BootstrapNextAction,
    BootstrapRequestStatusResponse, ContextRecordDto, VoteRecordDto,
};
use anyhow::{Context, Result};
use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};
use serde::de::DeserializeOwned;
use std::path::{Path, PathBuf};

const CONTEXT_BOOK_SCHEMA_VERSION: i64 = 1;

#[derive(Debug)]
pub struct ContextBookStore {
    conn: Mutex<Connection>,
    db_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalIdentityRecord {
    pub agent_id: String,
    pub device_type: String,
    pub display_name: String,
    pub bootstrap_approved: bool,
    pub last_bootstrap_request_id: Option<String>,
    pub last_bootstrap_approval_state: Option<ApprovalState>,
    pub last_bootstrap_completed_at: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamCursorRecord {
    pub last_event_id: String,
    pub source: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EventJournalEntry {
    pub event_id: String,
    pub event_type: String,
    pub occurred_at: String,
    pub producer_agent_id: Option<String>,
    pub entity_id: Option<String>,
    pub scope: String,
    pub projected_at: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconciliationJobKind {
    VoteRefresh,
    InitialSnapshot,
    FullResync,
}

impl ReconciliationJobKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::VoteRefresh => "vote_refresh",
            Self::InitialSnapshot => "initial_snapshot",
            Self::FullResync => "full_resync",
        }
    }

    fn from_db(value: String) -> Self {
        match value.as_str() {
            "vote_refresh" => Self::VoteRefresh,
            "initial_snapshot" => Self::InitialSnapshot,
            "full_resync" => Self::FullResync,
            _ => Self::FullResync,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationJobRecord {
    pub job_id: i64,
    pub job_kind: ReconciliationJobKind,
    pub entity_id: Option<String>,
    pub reason: String,
    pub created_at: String,
}

impl ContextBookStore {
    pub fn new(workspace_dir: &Path, config: &ContextBookConfig) -> Result<Self> {
        let db_path = resolve_store_path(workspace_dir, &config.store_path);
        Self::open_at(&db_path)
    }

    pub fn open_at(db_path: &Path) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create Context Book store dir {}",
                    parent.display()
                )
            })?;
        }

        let conn = Connection::open(db_path)
            .with_context(|| format!("failed to open Context Book DB {}", db_path.display()))?;

        configure_connection(&conn)?;
        apply_migrations(&conn)?;

        Ok(Self {
            conn: Mutex::new(conn),
            db_path: db_path.to_path_buf(),
        })
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn schema_version(&self) -> Result<i64> {
        let conn = self.conn.lock();
        conn.query_row("PRAGMA user_version", [], |row| row.get(0))
            .context("failed to read Context Book schema version")
    }

    pub fn save_local_identity(&self, record: &LocalIdentityRecord) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO local_identity (
                singleton_key,
                agent_id,
                device_type,
                display_name,
                bootstrap_approved,
                last_bootstrap_request_id,
                last_bootstrap_approval_state,
                last_bootstrap_completed_at,
                updated_at
             ) VALUES (
                1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8
             )
             ON CONFLICT(singleton_key) DO UPDATE SET
                agent_id = excluded.agent_id,
                device_type = excluded.device_type,
                display_name = excluded.display_name,
                bootstrap_approved = excluded.bootstrap_approved,
                last_bootstrap_request_id = excluded.last_bootstrap_request_id,
                last_bootstrap_approval_state = excluded.last_bootstrap_approval_state,
                last_bootstrap_completed_at = excluded.last_bootstrap_completed_at,
                updated_at = excluded.updated_at",
            params![
                record.agent_id,
                record.device_type,
                record.display_name,
                record.bootstrap_approved,
                record.last_bootstrap_request_id,
                record
                    .last_bootstrap_approval_state
                    .as_ref()
                    .map(approval_state_to_db),
                record.last_bootstrap_completed_at,
                record.updated_at,
            ],
        )
        .context("failed to save Context Book local identity")?;

        Ok(())
    }

    pub fn load_local_identity(&self) -> Result<Option<LocalIdentityRecord>> {
        let conn = self.conn.lock();
        conn.query_row(
            "SELECT
                agent_id,
                device_type,
                display_name,
                bootstrap_approved,
                last_bootstrap_request_id,
                last_bootstrap_approval_state,
                last_bootstrap_completed_at,
                updated_at
             FROM local_identity
             WHERE singleton_key = 1",
            [],
            |row| {
                let approval_state: Option<String> = row.get(5)?;
                Ok(LocalIdentityRecord {
                    agent_id: row.get(0)?,
                    device_type: row.get(1)?,
                    display_name: row.get(2)?,
                    bootstrap_approved: row.get(3)?,
                    last_bootstrap_request_id: row.get(4)?,
                    last_bootstrap_approval_state: approval_state.and_then(approval_state_from_db),
                    last_bootstrap_completed_at: row.get(6)?,
                    updated_at: row.get(7)?,
                })
            },
        )
        .optional()
        .context("failed to load Context Book local identity")
    }

    pub fn save_bootstrap_request(
        &self,
        request_kind: &str,
        status: &BootstrapRequestStatusResponse,
        status_url: Option<&str>,
        watch_url: Option<&str>,
        complete_url: Option<&str>,
    ) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO bootstrap_requests (
                request_id,
                request_kind,
                approval_state,
                next_action,
                wait_token,
                status_url,
                watch_url,
                complete_url,
                terminal_reason,
                updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, CURRENT_TIMESTAMP)
             ON CONFLICT(request_id) DO UPDATE SET
                request_kind = excluded.request_kind,
                approval_state = excluded.approval_state,
                next_action = excluded.next_action,
                wait_token = excluded.wait_token,
                status_url = excluded.status_url,
                watch_url = excluded.watch_url,
                complete_url = excluded.complete_url,
                terminal_reason = excluded.terminal_reason,
                updated_at = CURRENT_TIMESTAMP",
            params![
                status.request_id,
                request_kind,
                approval_state_to_db(&status.approval_state),
                status.next_action.as_ref().map(bootstrap_next_action_to_db),
                status.wait_token,
                status_url,
                watch_url,
                complete_url,
                status.terminal_reason,
            ],
        )
        .context("failed to save Context Book bootstrap request")?;

        Ok(())
    }

    pub fn save_auth_session(&self, session: &AuthSessionDto, saved_at: &str) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO auth_session (
                singleton_key,
                agent_id,
                access_token,
                refresh_token,
                access_token_expires_at,
                last_refresh_result,
                updated_at
             ) VALUES (1, ?1, ?2, ?3, ?4, 'issued', ?5)
             ON CONFLICT(singleton_key) DO UPDATE SET
                agent_id = excluded.agent_id,
                access_token = excluded.access_token,
                refresh_token = excluded.refresh_token,
                access_token_expires_at = excluded.access_token_expires_at,
                last_refresh_result = excluded.last_refresh_result,
                updated_at = excluded.updated_at",
            params![
                session.agent_id,
                session.access_token,
                session.refresh_token,
                session.access_token_expires_at,
                saved_at,
            ],
        )
        .context("failed to save Context Book auth session")?;

        Ok(())
    }

    pub fn load_auth_session(&self) -> Result<Option<AuthSessionDto>> {
        let conn = self.conn.lock();
        conn.query_row(
            "SELECT agent_id, access_token, refresh_token, access_token_expires_at
             FROM auth_session
             WHERE singleton_key = 1",
            [],
            |row| {
                Ok(AuthSessionDto {
                    agent_id: row.get(0)?,
                    access_token: row.get(1)?,
                    refresh_token: row.get(2)?,
                    access_token_expires_at: row.get(3)?,
                })
            },
        )
        .optional()
        .context("failed to load Context Book auth session")
    }

    pub fn replace_desired_subscriptions(&self, producer_agent_ids: &[String]) -> Result<()> {
        self.replace_subscription_rows("desired_subscriptions", producer_agent_ids)
    }

    pub fn list_desired_subscriptions(&self) -> Result<Vec<String>> {
        self.list_subscription_rows("desired_subscriptions")
    }

    pub fn replace_effective_subscriptions(&self, producer_agent_ids: &[String]) -> Result<()> {
        self.replace_subscription_rows("effective_subscriptions", producer_agent_ids)
    }

    pub fn list_effective_subscriptions(&self) -> Result<Vec<String>> {
        self.list_subscription_rows("effective_subscriptions")
    }

    fn replace_subscription_rows(&self, table: &str, producer_agent_ids: &[String]) -> Result<()> {
        let mut conn = self.conn.lock();
        let tx = conn
            .transaction()
            .with_context(|| format!("failed to open Context Book {table} transaction"))?;

        tx.execute(&format!("DELETE FROM {table}"), [])
            .with_context(|| format!("failed to clear Context Book {table}"))?;

        {
            let mut stmt = tx
                .prepare(&format!(
                    "INSERT OR IGNORE INTO {table} (producer_agent_id) VALUES (?1)"
                ))
                .with_context(|| format!("failed to prepare Context Book {table} insert"))?;
            for producer_agent_id in producer_agent_ids {
                stmt.execute([producer_agent_id])
                    .with_context(|| format!("failed to insert Context Book {table} row"))?;
            }
        }

        tx.commit()
            .with_context(|| format!("failed to commit Context Book {table} update"))?;
        Ok(())
    }

    fn list_subscription_rows(&self, table: &str) -> Result<Vec<String>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(&format!(
                "SELECT producer_agent_id FROM {table} ORDER BY producer_agent_id ASC"
            ))
            .with_context(|| format!("failed to prepare Context Book {table} select"))?;
        let rows = stmt
            .query_map([], |row| row.get(0))
            .with_context(|| format!("failed to query Context Book {table}"))?;

        rows.collect::<Result<Vec<_>, _>>()
            .with_context(|| format!("failed to read Context Book {table} rows"))
    }

    pub fn upsert_mirrored_agent(&self, agent: &AgentRecordDto) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO mirrored_agents (
                agent_id,
                device_type,
                display_name,
                lifecycle_state,
                connection_state,
                created_at,
                updated_at,
                last_seen_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(agent_id) DO UPDATE SET
                device_type = excluded.device_type,
                display_name = excluded.display_name,
                lifecycle_state = excluded.lifecycle_state,
                connection_state = excluded.connection_state,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                last_seen_at = excluded.last_seen_at",
            params![
                agent.agent_id,
                agent.device_type,
                agent.display_name,
                serde_json::to_string(&agent.lifecycle_state)
                    .context("failed to encode Context Book lifecycle_state")?,
                serde_json::to_string(&agent.connection_state)
                    .context("failed to encode Context Book connection_state")?,
                agent.created_at,
                agent.updated_at,
                agent.last_seen_at,
            ],
        )
        .context("failed to upsert Context Book mirrored agent")?;
        Ok(())
    }

    pub fn list_mirrored_agents(&self) -> Result<Vec<AgentRecordDto>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "SELECT
                    agent_id,
                    device_type,
                    display_name,
                    lifecycle_state,
                    connection_state,
                    created_at,
                    updated_at,
                    last_seen_at
                 FROM mirrored_agents
                 ORDER BY updated_at DESC, agent_id ASC",
            )
            .context("failed to prepare mirrored agent query")?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Option<String>>(7)?,
                ))
            })
            .context("failed to query mirrored agents")?;

        let rows = rows
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("failed to read mirrored agent rows")?;

        rows.into_iter()
            .map(
                |(
                    agent_id,
                    device_type,
                    display_name,
                    lifecycle_state,
                    connection_state,
                    created_at,
                    updated_at,
                    last_seen_at,
                )| {
                    Ok(AgentRecordDto {
                        agent_id,
                        device_type,
                        display_name,
                        lifecycle_state: parse_json_column(
                            lifecycle_state,
                            "mirrored agent lifecycle_state",
                        )?,
                        connection_state: parse_json_column(
                            connection_state,
                            "mirrored agent connection_state",
                        )?,
                        created_at,
                        updated_at,
                        last_seen_at,
                    })
                },
            )
            .collect()
    }

    pub fn upsert_mirrored_context(&self, context_record: &ContextRecordDto) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO mirrored_contexts (
                context_id,
                author_agent_id,
                title,
                contents,
                tag,
                status,
                created_at,
                updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(context_id) DO UPDATE SET
                author_agent_id = excluded.author_agent_id,
                title = excluded.title,
                contents = excluded.contents,
                tag = excluded.tag,
                status = excluded.status,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at",
            params![
                context_record.context_id,
                context_record.author_agent_id,
                context_record.title,
                context_record.contents,
                context_record.tag,
                serde_json::to_string(&context_record.status)
                    .context("failed to encode Context Book context status")?,
                context_record.created_at,
                context_record.updated_at,
            ],
        )
        .context("failed to upsert Context Book mirrored context")?;
        Ok(())
    }

    pub fn list_mirrored_contexts(&self) -> Result<Vec<ContextRecordDto>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "SELECT
                    context_id,
                    author_agent_id,
                    title,
                    contents,
                    tag,
                    status,
                    created_at,
                    updated_at
                 FROM mirrored_contexts
                 ORDER BY updated_at DESC, context_id ASC",
            )
            .context("failed to prepare mirrored context query")?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                ))
            })
            .context("failed to query mirrored contexts")?;

        let rows = rows
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("failed to read mirrored context rows")?;

        rows.into_iter()
            .map(
                |(
                    context_id,
                    author_agent_id,
                    title,
                    contents,
                    tag,
                    status,
                    created_at,
                    updated_at,
                )| {
                    Ok(ContextRecordDto {
                        context_id,
                        author_agent_id,
                        title,
                        contents,
                        tag,
                        status: parse_json_column(status, "mirrored context status")?,
                        created_at,
                        updated_at,
                    })
                },
            )
            .collect()
    }

    pub fn delete_mirrored_context(&self, context_id: &str) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "DELETE FROM mirrored_contexts WHERE context_id = ?1",
            [context_id],
        )
        .context("failed to delete Context Book mirrored context")?;
        Ok(())
    }

    pub fn upsert_mirrored_vote(&self, vote: &VoteRecordDto, derived_source: &str) -> Result<()> {
        let conn = self.conn.lock();
        let voter_agent_ids = serde_json::to_string(&vote.voter_agent_ids)
            .context("failed to encode Context Book voter list")?;

        conn.execute(
            "INSERT INTO mirrored_votes (
                vote_id,
                owner_agent_id,
                vote_score,
                vote_context,
                voter_agent_ids,
                required_score,
                executable,
                derived_refreshed_at,
                derived_source,
                created_at,
                updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, CURRENT_TIMESTAMP, ?8, ?9, ?10)
             ON CONFLICT(vote_id) DO UPDATE SET
                owner_agent_id = excluded.owner_agent_id,
                vote_score = excluded.vote_score,
                vote_context = excluded.vote_context,
                voter_agent_ids = excluded.voter_agent_ids,
                required_score = excluded.required_score,
                executable = excluded.executable,
                derived_refreshed_at = excluded.derived_refreshed_at,
                derived_source = excluded.derived_source,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at",
            params![
                vote.vote_id,
                vote.owner_agent_id,
                vote.vote_score,
                vote.vote_context,
                voter_agent_ids,
                vote.required_score,
                vote.executable,
                derived_source,
                vote.created_at,
                vote.updated_at,
            ],
        )
        .context("failed to upsert Context Book mirrored vote")?;
        Ok(())
    }

    pub fn list_mirrored_votes(&self) -> Result<Vec<VoteRecordDto>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "SELECT
                    vote_id,
                    owner_agent_id,
                    vote_score,
                    vote_context,
                    voter_agent_ids,
                    required_score,
                    executable,
                    created_at,
                    updated_at
                 FROM mirrored_votes
                 ORDER BY updated_at DESC, vote_id ASC",
            )
            .context("failed to prepare mirrored vote query")?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<f64>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<f64>>(5)?,
                    row.get::<_, Option<bool>>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                ))
            })
            .context("failed to query mirrored votes")?;

        let rows = rows
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("failed to read mirrored vote rows")?;

        rows.into_iter()
            .map(
                |(
                    vote_id,
                    owner_agent_id,
                    vote_score,
                    vote_context,
                    voter_agent_ids,
                    required_score,
                    executable,
                    created_at,
                    updated_at,
                )| {
                    Ok(VoteRecordDto {
                        vote_id,
                        owner_agent_id,
                        vote_score,
                        vote_context,
                        voter_agent_ids: parse_json_column(
                            voter_agent_ids,
                            "mirrored vote voter_agent_ids",
                        )?,
                        required_score,
                        executable,
                        created_at,
                        updated_at,
                    })
                },
            )
            .collect()
    }

    pub fn delete_mirrored_vote(&self, vote_id: &str) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute("DELETE FROM mirrored_votes WHERE vote_id = ?1", [vote_id])
            .context("failed to delete Context Book mirrored vote")?;
        Ok(())
    }

    pub fn record_vote_cast_audit(
        &self,
        vote_id: &str,
        cast_score: Option<f64>,
        requested_at: &str,
    ) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO vote_cast_audit (vote_id, cast_score, requested_at)
             VALUES (?1, ?2, ?3)",
            params![vote_id, cast_score, requested_at],
        )
        .context("failed to save Context Book vote cast audit")?;
        Ok(())
    }

    pub fn mark_event_processed(&self, entry: &EventJournalEntry) -> Result<bool> {
        let conn = self.conn.lock();
        let inserted = conn
            .execute(
                "INSERT OR IGNORE INTO event_journal (
                    event_id,
                    event_type,
                    occurred_at,
                    producer_agent_id,
                    entity_id,
                    scope,
                    projected_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    entry.event_id,
                    entry.event_type,
                    entry.occurred_at,
                    entry.producer_agent_id,
                    entry.entity_id,
                    entry.scope,
                    entry.projected_at,
                ],
            )
            .context("failed to insert Context Book event journal row")?;
        Ok(inserted == 1)
    }

    pub fn is_event_processed(&self, event_id: &str) -> Result<bool> {
        let conn = self.conn.lock();
        let exists = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM event_journal WHERE event_id = ?1",
                [event_id],
                |row| row.get(0),
            )
            .context("failed to query Context Book event journal")?;
        Ok(exists)
    }

    pub fn event_journal_count(&self) -> Result<i64> {
        let conn = self.conn.lock();
        conn.query_row("SELECT COUNT(*) FROM event_journal", [], |row| row.get(0))
            .context("failed to count Context Book event journal rows")
    }

    pub fn save_stream_cursor(&self, cursor: &StreamCursorRecord) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO stream_cursor (singleton_key, last_event_id, source, updated_at)
             VALUES (1, ?1, ?2, ?3)
             ON CONFLICT(singleton_key) DO UPDATE SET
                last_event_id = excluded.last_event_id,
                source = excluded.source,
                updated_at = excluded.updated_at",
            params![cursor.last_event_id, cursor.source, cursor.updated_at],
        )
        .context("failed to save Context Book stream cursor")?;
        Ok(())
    }

    pub fn load_stream_cursor(&self) -> Result<Option<StreamCursorRecord>> {
        let conn = self.conn.lock();
        conn.query_row(
            "SELECT last_event_id, source, updated_at
             FROM stream_cursor
             WHERE singleton_key = 1",
            [],
            |row| {
                Ok(StreamCursorRecord {
                    last_event_id: row.get(0)?,
                    source: row.get(1)?,
                    updated_at: row.get(2)?,
                })
            },
        )
        .optional()
        .context("failed to load Context Book stream cursor")
    }

    pub fn clear_stream_cursor(&self) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute("DELETE FROM stream_cursor WHERE singleton_key = 1", [])
            .context("failed to clear Context Book stream cursor")?;
        Ok(())
    }

    pub fn enqueue_reconciliation_job(
        &self,
        job_kind: ReconciliationJobKind,
        entity_id: Option<&str>,
        reason: &str,
    ) -> Result<i64> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO reconciliation_jobs (job_kind, entity_id, reason, created_at)
             VALUES (?1, ?2, ?3, CURRENT_TIMESTAMP)",
            params![job_kind.as_str(), entity_id, reason],
        )
        .context("failed to enqueue Context Book reconciliation job")?;
        Ok(conn.last_insert_rowid())
    }

    pub fn list_reconciliation_jobs(&self) -> Result<Vec<ReconciliationJobRecord>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "SELECT job_id, job_kind, entity_id, reason, created_at
                 FROM reconciliation_jobs
                 ORDER BY job_id ASC",
            )
            .context("failed to prepare reconciliation job query")?;
        let rows = stmt
            .query_map([], |row| {
                let job_kind: String = row.get(1)?;
                Ok(ReconciliationJobRecord {
                    job_id: row.get(0)?,
                    job_kind: ReconciliationJobKind::from_db(job_kind),
                    entity_id: row.get(2)?,
                    reason: row.get(3)?,
                    created_at: row.get(4)?,
                })
            })
            .context("failed to query reconciliation jobs")?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to load reconciliation jobs")
    }
}

fn resolve_store_path(workspace_dir: &Path, store_path: &str) -> PathBuf {
    let store_path = Path::new(store_path);
    if store_path.is_absolute() {
        store_path.to_path_buf()
    } else {
        workspace_dir.join(store_path)
    }
}

fn parse_json_column<T>(raw: String, column_name: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    serde_json::from_str(&raw).with_context(|| format!("failed to decode {column_name}: {raw}"))
}

fn configure_connection(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA foreign_keys = ON;
         PRAGMA temp_store = MEMORY;
         PRAGMA busy_timeout = 5000;",
    )
    .context("failed to configure Context Book SQLite connection")
}

fn apply_migrations(conn: &Connection) -> Result<()> {
    let current_version: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .context("failed to read Context Book schema version")?;

    if current_version >= CONTEXT_BOOK_SCHEMA_VERSION {
        return Ok(());
    }

    let tx = conn
        .unchecked_transaction()
        .context("failed to start Context Book migration transaction")?;

    if current_version < 1 {
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS local_identity (
                singleton_key INTEGER PRIMARY KEY CHECK (singleton_key = 1),
                agent_id TEXT NOT NULL,
                device_type TEXT NOT NULL,
                display_name TEXT NOT NULL,
                bootstrap_approved INTEGER NOT NULL DEFAULT 0,
                last_bootstrap_request_id TEXT,
                last_bootstrap_approval_state TEXT,
                last_bootstrap_completed_at TEXT,
                updated_at TEXT NOT NULL
             );

             CREATE TABLE IF NOT EXISTS bootstrap_requests (
                request_id TEXT PRIMARY KEY,
                request_kind TEXT NOT NULL,
                approval_state TEXT NOT NULL,
                next_action TEXT,
                wait_token TEXT,
                status_url TEXT,
                watch_url TEXT,
                complete_url TEXT,
                terminal_reason TEXT,
                updated_at TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_context_book_bootstrap_requests_updated_at
                 ON bootstrap_requests(updated_at DESC);

             CREATE TABLE IF NOT EXISTS auth_session (
                singleton_key INTEGER PRIMARY KEY CHECK (singleton_key = 1),
                agent_id TEXT NOT NULL,
                access_token TEXT NOT NULL,
                refresh_token TEXT NOT NULL,
                access_token_expires_at TEXT NOT NULL,
                last_refresh_result TEXT,
                updated_at TEXT NOT NULL
             );

             CREATE TABLE IF NOT EXISTS desired_subscriptions (
                producer_agent_id TEXT PRIMARY KEY
             );

             CREATE TABLE IF NOT EXISTS effective_subscriptions (
                producer_agent_id TEXT PRIMARY KEY
             );

             CREATE TABLE IF NOT EXISTS mirrored_agents (
                agent_id TEXT PRIMARY KEY,
                device_type TEXT NOT NULL,
                display_name TEXT NOT NULL,
                lifecycle_state TEXT NOT NULL,
                connection_state TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_seen_at TEXT
             );

             CREATE TABLE IF NOT EXISTS mirrored_contexts (
                context_id TEXT PRIMARY KEY,
                author_agent_id TEXT NOT NULL,
                title TEXT NOT NULL,
                contents TEXT NOT NULL,
                tag TEXT,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
             );

             CREATE TABLE IF NOT EXISTS mirrored_votes (
                vote_id TEXT PRIMARY KEY,
                owner_agent_id TEXT NOT NULL,
                vote_score REAL,
                vote_context TEXT NOT NULL,
                voter_agent_ids TEXT NOT NULL,
                required_score REAL,
                executable INTEGER,
                derived_refreshed_at TEXT,
                derived_source TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
             );

             CREATE TABLE IF NOT EXISTS vote_cast_audit (
                audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                vote_id TEXT NOT NULL,
                cast_score REAL,
                requested_at TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_context_book_vote_cast_vote_id
                 ON vote_cast_audit(vote_id, requested_at DESC);

             CREATE TABLE IF NOT EXISTS event_journal (
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                producer_agent_id TEXT,
                entity_id TEXT,
                scope TEXT NOT NULL,
                projected_at TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_context_book_event_journal_occurred_at
                 ON event_journal(occurred_at DESC);

             CREATE TABLE IF NOT EXISTS stream_cursor (
                singleton_key INTEGER PRIMARY KEY CHECK (singleton_key = 1),
                last_event_id TEXT NOT NULL,
                source TEXT NOT NULL,
                updated_at TEXT NOT NULL
             );

             CREATE TABLE IF NOT EXISTS reconciliation_jobs (
                job_id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_kind TEXT NOT NULL,
                entity_id TEXT,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_context_book_reconciliation_jobs_created_at
                 ON reconciliation_jobs(created_at ASC);",
        )
        .context("failed to apply Context Book schema migration v1")?;
    }

    tx.pragma_update(None, "user_version", CONTEXT_BOOK_SCHEMA_VERSION)
        .context("failed to update Context Book schema version")?;
    tx.commit()
        .context("failed to commit Context Book migration transaction")
}

fn approval_state_to_db(state: &ApprovalState) -> String {
    serde_json::to_string(state)
        .unwrap_or_else(|_| "\"Pending\"".to_string())
        .trim_matches('"')
        .to_string()
}

fn approval_state_from_db(value: String) -> Option<ApprovalState> {
    serde_json::from_str(&format!("\"{value}\"")).ok()
}

fn bootstrap_next_action_to_db(action: &BootstrapNextAction) -> String {
    serde_json::to_string(action)
        .unwrap_or_else(|_| "\"poll_status\"".to_string())
        .trim_matches('"')
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn temp_store() -> (TempDir, ContextBookStore) {
        let tmp = TempDir::new().expect("temp dir");
        let db_path = tmp
            .path()
            .join("state")
            .join("context_book")
            .join("state.db");
        let store = ContextBookStore::open_at(&db_path).expect("open store");
        (tmp, store)
    }

    #[test]
    fn store_initializes_with_required_tables() {
        let (_tmp, store) = temp_store();
        {
            let conn = store.conn.lock();
            let tables = [
                "local_identity",
                "bootstrap_requests",
                "auth_session",
                "desired_subscriptions",
                "effective_subscriptions",
                "mirrored_agents",
                "mirrored_contexts",
                "mirrored_votes",
                "vote_cast_audit",
                "event_journal",
                "stream_cursor",
                "reconciliation_jobs",
            ];

            for table in tables {
                let exists: bool = conn
                    .query_row(
                        "SELECT COUNT(*) > 0 FROM sqlite_master
                         WHERE type = 'table' AND name = ?1",
                        [table],
                        |row| row.get(0),
                    )
                    .expect("table exists query");
                assert!(exists, "expected table {table} to exist");
            }
        }

        assert_eq!(
            store.schema_version().expect("schema version"),
            CONTEXT_BOOK_SCHEMA_VERSION
        );
        assert!(store.db_path().exists());
    }

    #[test]
    fn store_reopens_without_losing_state() {
        let tmp = TempDir::new().expect("temp dir");
        let db_path = tmp
            .path()
            .join("state")
            .join("context_book")
            .join("state.db");

        {
            let store = ContextBookStore::open_at(&db_path).expect("open store");
            let record = LocalIdentityRecord {
                agent_id: "agent-1".into(),
                device_type: "notepc".into(),
                display_name: "ZeroClaw Main".into(),
                bootstrap_approved: true,
                last_bootstrap_request_id: Some("req-1".into()),
                last_bootstrap_approval_state: Some(ApprovalState::Approved),
                last_bootstrap_completed_at: Some("2026-04-02T10:00:00Z".into()),
                updated_at: "2026-04-02T10:00:00Z".into(),
            };
            store
                .save_local_identity(&record)
                .expect("save local identity");

            store
                .replace_desired_subscriptions(&["*".into(), "peer-1".into()])
                .expect("save desired subscriptions");
        }

        let reopened = ContextBookStore::open_at(&db_path).expect("reopen store");
        let loaded = reopened
            .load_local_identity()
            .expect("load local identity")
            .expect("local identity record");

        assert_eq!(loaded.agent_id, "agent-1");
        assert_eq!(loaded.last_bootstrap_request_id.as_deref(), Some("req-1"));
        assert_eq!(
            reopened
                .list_desired_subscriptions()
                .expect("load desired subscriptions"),
            vec!["*".to_string(), "peer-1".to_string()]
        );
    }

    #[test]
    fn event_journal_deduplicates_event_ids() {
        let (_tmp, store) = temp_store();
        let entry = EventJournalEntry {
            event_id: "evt-1".into(),
            event_type: "context.created".into(),
            occurred_at: "2026-04-02T10:00:00Z".into(),
            producer_agent_id: Some("agent-a".into()),
            entity_id: Some("ctx-1".into()),
            scope: "data-plane".into(),
            projected_at: "2026-04-02T10:00:01Z".into(),
        };

        assert!(store.mark_event_processed(&entry).expect("first insert"));
        assert!(!store
            .mark_event_processed(&entry)
            .expect("duplicate insert"));
        assert!(store.is_event_processed("evt-1").expect("event exists"));
        assert_eq!(store.event_journal_count().expect("journal count"), 1);
    }

    #[test]
    fn reconciliation_jobs_and_cursor_are_persisted() {
        let (_tmp, store) = temp_store();
        let job_id = store
            .enqueue_reconciliation_job(
                ReconciliationJobKind::VoteRefresh,
                Some("vote-1"),
                "vote.updated missing derived fields",
            )
            .expect("enqueue job");
        store
            .save_stream_cursor(&StreamCursorRecord {
                last_event_id: "evt-2".into(),
                source: "stream".into(),
                updated_at: "2026-04-02T10:10:00Z".into(),
            })
            .expect("save cursor");

        let jobs = store
            .list_reconciliation_jobs()
            .expect("list reconciliation jobs");
        let cursor = store
            .load_stream_cursor()
            .expect("load cursor")
            .expect("cursor");

        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].job_id, job_id);
        assert_eq!(jobs[0].job_kind, ReconciliationJobKind::VoteRefresh);
        assert_eq!(jobs[0].entity_id.as_deref(), Some("vote-1"));
        assert_eq!(cursor.last_event_id, "evt-2");
    }
}
