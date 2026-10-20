//! The read-only importer for Waku's `app.db` (#63, R4, R18, R23).
//!
//! Two sources: the fork's own `app.db` next to `state.sqlite`, imported
//! automatically, and upstream Waku's, imported only on an explicit user
//! action. Either way the file is opened with `SQLITE_OPEN_READ_ONLY` and
//! never written. The importer reads outside `decide` and hands plain data to
//! internal commands, so every write still goes through the sink:
//!
//! - [`import_shells`] (eager): every project (`ImportLegacyProject`) and
//!   every session as a thread shell (`ImportLegacyThread`), with command ids
//!   `import-project:<fingerprint>:<id>` and `import:<fingerprint>:<id>`, so
//!   a rerun is a no-op. Sessions already imported (from any source) are
//!   skipped, and `legacy_imports` records each imported one.
//! - [`import_transcript`] (lazy, through `Orchestrator::ensure_transcript`):
//!   one session's turns, messages, transcript blocks, checkpoints and queue
//!   as one `ImportLegacyTranscript` (command id
//!   `import-transcript:<fingerprint>:<id>`), converted by the pure
//!   [`transcript_from_session`].
//!
//! `legacy_imports` and `legacy_import_sources` are the importer's own
//! bookkeeping, written after the command commits; a crash between the two
//! repeats the command, which its receipt turns into a no-op.

use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;
use waku_protocol::model as legacy;
use waku_protocol::orchestration::legacy::resume_cursor_ref;
use waku_protocol::orchestration::*;

use crate::command::{InternalCommand, LegacyThreadShell, LegacyTranscript};
use crate::decide::PLACEHOLDER_TITLE;
use crate::ids::derived_id;
use crate::orchestrator::{DispatchError, Orchestrator};

/// Shown at the end of a transcript whose session was busy when it was
/// imported.
pub const NOTICE_INTERRUPTED: &str =
    "This conversation was running when it was imported; that turn was interrupted.";
/// Shown when the session was waiting on an approval: the callback is gone.
pub const NOTICE_PENDING_REQUEST: &str =
    "A pending approval or question was not imported; ask again if it is still needed.";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LegacySourceKind {
    /// The fork's own `app.db`: imported automatically.
    Fork,
    /// Upstream Waku's `app.db`: imported only when the user asks.
    Upstream,
}

impl LegacySourceKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fork => "fork_app_db",
            Self::Upstream => "upstream_app_db",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "fork_app_db" => Some(Self::Fork),
            "upstream_app_db" => Some(Self::Upstream),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacySource {
    pub kind: LegacySourceKind,
    pub path: PathBuf,
}

impl LegacySource {
    pub fn fork(path: impl Into<PathBuf>) -> Self {
        Self {
            kind: LegacySourceKind::Fork,
            path: path.into(),
        }
    }

    pub fn upstream(path: impl Into<PathBuf>) -> Self {
        Self {
            kind: LegacySourceKind::Upstream,
            path: path.into(),
        }
    }
}

#[derive(Debug)]
pub enum ImportError {
    /// The file cannot be read as a Waku database.
    Source(String),
    Dispatch(DispatchError),
    Storage(String),
}

impl fmt::Display for ImportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(detail) => write!(formatter, "legacy database: {detail}"),
            Self::Dispatch(error) => write!(formatter, "{error}"),
            Self::Storage(detail) => write!(formatter, "storage: {detail}"),
        }
    }
}

impl std::error::Error for ImportError {}

impl From<rusqlite::Error> for ImportError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Source(error.to_string())
    }
}

impl From<DispatchError> for ImportError {
    fn from(error: DispatchError) -> Self {
        Self::Dispatch(error)
    }
}

/// What [`import_shells`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ImportReport {
    pub fingerprint: String,
    pub projects: usize,
    pub threads: usize,
    /// Sessions imported earlier (from this or another source).
    pub skipped: usize,
    /// `(session or project id, error)` for rows that did not import.
    pub failed: Vec<(String, String)>,
}

/// What [`import_transcript`] did for one thread.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TranscriptOutcome {
    Imported {
        runs: usize,
        items: usize,
    },
    /// Not an imported thread, or its transcript was imported already.
    NotPending,
    /// The thread had native runs first; its legacy transcript is not
    /// merged in.
    Skipped,
    /// The source could not be read; recorded in `legacy_imports.error`.
    Failed(String),
}

// -- reading ------------------------------------------------------------------

/// Opens a legacy `app.db` without write access (`SQLITE_OPEN_READ_ONLY`):
/// SQLite never writes the main file of a read-only connection.
///
/// A database without a `-wal` file (its app is not running and checkpointed
/// on close) is opened `immutable`, which creates no `-wal`/`-shm` files next
/// to it either. One with a `-wal` (its app is running, or crashed) is read
/// through it, so committed but uncheckpointed sessions are not missed.
pub fn open_read_only(path: &Path) -> Result<Connection, ImportError> {
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    if !path.is_file() {
        return Err(ImportError::Source(format!(
            "{} is not a file",
            path.display()
        )));
    }
    let probe = |connection: Connection| -> Result<Connection, rusqlite::Error> {
        connection.query_row("SELECT COUNT(*) FROM sqlite_master", [], |_| Ok(()))?;
        Ok(connection)
    };
    let immutable = || -> Result<Connection, ImportError> {
        let mut uri = String::from("file:");
        for byte in path.to_string_lossy().bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'/' | b'-' | b'_' | b'.' | b'~' => {
                    uri.push(byte as char)
                }
                other => uri.push_str(&format!("%{other:02X}")),
            }
        }
        uri.push_str("?mode=ro&immutable=1");
        Ok(probe(Connection::open_with_flags(
            uri,
            flags | OpenFlags::SQLITE_OPEN_URI,
        )?)?)
    };
    let wal = PathBuf::from(format!("{}-wal", path.display()));
    if !wal.exists() {
        return immutable();
    }
    match Connection::open_with_flags(path, flags).and_then(probe) {
        Ok(connection) => Ok(connection),
        Err(_) => immutable(),
    }
}

/// `path + size + mtime` of the file read, hashed to 16 hex digits (RFC 0001
/// `legacy_imports.source_fingerprint`; short enough for command ids).
pub fn fingerprint(path: &Path) -> Result<String, ImportError> {
    let canonical = std::fs::canonicalize(path)
        .map_err(|error| ImportError::Source(format!("{}: {error}", path.display())))?;
    let metadata = std::fs::metadata(&canonical)
        .map_err(|error| ImportError::Source(format!("{}: {error}", path.display())))?;
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0, |duration| duration.as_nanos());
    let digest = Sha256::digest(
        format!(
            "{}\n{}\n{modified}",
            canonical.to_string_lossy(),
            metadata.len()
        )
        .as_bytes(),
    );
    Ok(digest
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

/// Which optional columns this `app.db` has (upstream migrations 0000–0002).
#[derive(Clone, Copy, Debug)]
struct Columns {
    auto_title: bool,
    display_content: bool,
    attachments: bool,
}

fn has_column(connection: &Connection, table: &str, column: &str) -> rusqlite::Result<bool> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let names = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(names.iter().any(|name| name == column))
}

fn columns(connection: &Connection) -> rusqlite::Result<Columns> {
    Ok(Columns {
        auto_title: has_column(connection, "sessions", "auto_title")?,
        display_content: has_column(connection, "messages", "display_content")?,
        attachments: has_column(connection, "messages", "attachments")?,
    })
}

/// A `projects` row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacyProject {
    pub id: Uuid,
    pub name: String,
    pub path: PathBuf,
    pub position: i64,
    /// Unix seconds.
    pub created_at: i64,
}

pub fn read_projects(connection: &Connection) -> rusqlite::Result<Vec<LegacyProject>> {
    let mut statement = connection
        .prepare("SELECT id, name, path, position, created_at FROM projects ORDER BY position")?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows
        .into_iter()
        .filter_map(|(id, name, path, position, created_at)| {
            Some(LegacyProject {
                id: Uuid::parse_str(&id).ok()?,
                name,
                path: PathBuf::from(path),
                position,
                created_at,
            })
        })
        .collect())
}

fn parse_tag<T: serde::de::DeserializeOwned>(value: &str) -> Option<T> {
    serde_json::from_value(serde_json::Value::String(value.to_owned())).ok()
}

/// The list columns of a `sessions` row.
struct SessionRow {
    id: String,
    project_id: String,
    title: String,
    auto_title: Option<String>,
    provider: String,
    model: Option<String>,
    status: String,
    created_at: i64,
    updated_at: i64,
    last_reply_at: Option<i64>,
    data: Option<String>,
}

fn session_select(columns: Columns, data: &str, filter: &str) -> String {
    format!(
        "SELECT s.id, s.project_id, s.title, {}, s.provider, s.model, s.status,
                s.created_at, s.updated_at, s.last_reply_at, {data}
         FROM sessions AS s LEFT JOIN session_details AS d ON d.session_id = s.id
         {filter}",
        if columns.auto_title {
            "s.auto_title"
        } else {
            "NULL"
        },
    )
}

fn session_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionRow> {
    Ok(SessionRow {
        id: row.get(0)?,
        project_id: row.get(1)?,
        title: row.get(2)?,
        auto_title: row.get(3)?,
        provider: row.get(4)?,
        model: row.get(5)?,
        status: row.get(6)?,
        created_at: row.get(7)?,
        updated_at: row.get(8)?,
        last_reply_at: row.get(9)?,
        data: row.get(10)?,
    })
}

/// A session from its list columns and its `session_details` document. The
/// list columns win where both exist: they are what the sidebar showed.
fn session_from_row(row: SessionRow) -> Option<legacy::AgentSession> {
    let id = Uuid::parse_str(&row.id).ok()?;
    let project_id = Uuid::parse_str(&row.project_id).ok()?;
    let provider = parse_tag::<legacy::ProviderKind>(&row.provider);
    let mut session = row
        .data
        .as_deref()
        .and_then(|data| serde_json::from_str::<legacy::AgentSession>(data).ok())
        .unwrap_or_else(|| {
            let mut session = legacy::AgentSession::new(project_id, provider.unwrap_or_default());
            session.id = id;
            session
        });
    session.id = id;
    session.project_id = project_id;
    session.title = row.title;
    if row.auto_title.is_some() {
        session.auto_title = row.auto_title;
    }
    if let Some(provider) = provider {
        session.provider = provider;
    }
    session.model = row.model.or(session.model);
    if let Some(status) = parse_tag(&row.status) {
        session.status = status;
    }
    session.created_at = row.created_at.max(0) as u64;
    session.updated_at = row.updated_at.max(0) as u64;
    session.last_reply_at = row.last_reply_at.map(|at| at.max(0) as u64);
    Some(session)
}

/// Every session's shell fields, without transcripts (the heavy arrays are
/// cut in SQLite before the JSON reaches Rust).
pub fn read_session_shells(connection: &Connection) -> rusqlite::Result<Vec<legacy::AgentSession>> {
    let columns = columns(connection)?;
    let sql = session_select(
        columns,
        "json_remove(d.data, '$.transcript_blocks', '$.turns', '$.queued_messages', '$.messages')",
        "ORDER BY s.updated_at, s.id",
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement
        .query_map([], session_row)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows.into_iter().filter_map(session_from_row).collect())
}

/// One session with its whole transcript and its messages, in order.
pub fn read_session(
    connection: &Connection,
    id: Uuid,
) -> rusqlite::Result<Option<(legacy::AgentSession, Vec<legacy::Message>)>> {
    let columns = columns(connection)?;
    let sql = session_select(columns, "d.data", "WHERE s.id = ?1");
    let Some(row) = connection
        .query_row(&sql, [id.to_string()], session_row)
        .optional()?
    else {
        return Ok(None);
    };
    let Some(session) = session_from_row(row) else {
        return Ok(None);
    };
    let sql = format!(
        "SELECT id, turn_id, role, content, {}, {}, created_at, streaming
         FROM messages WHERE session_id = ?1 ORDER BY position",
        if columns.display_content {
            "display_content"
        } else {
            "NULL"
        },
        if columns.attachments {
            "attachments"
        } else {
            "'[]'"
        },
    );
    let mut statement = connection.prepare(&sql)?;
    let messages = statement
        .query_map([id.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, i64>(7)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter_map(
            |(id, turn_id, role, content, display_content, attachments, created_at, streaming)| {
                Some(legacy::Message {
                    id: Uuid::parse_str(&id).ok()?,
                    turn_id: turn_id.as_deref().and_then(|id| Uuid::parse_str(id).ok()),
                    role: parse_tag(&role)?,
                    content,
                    display_content,
                    attachments: attachments
                        .as_deref()
                        .and_then(|json| serde_json::from_str(json).ok())
                        .unwrap_or_default(),
                    created_at: created_at.max(0) as u64,
                    streaming: streaming != 0,
                })
            },
        )
        .collect();
    Ok(Some((session, messages)))
}

// -- conversion -----------------------------------------------------------------

fn millis(seconds: u64) -> Timestamp {
    (seconds as Timestamp).saturating_mul(1_000)
}

/// A project position as a fixed-width base-62 key, so imported projects
/// keep their order under bytewise comparison.
fn position_order_key(position: i64) -> OrderKey {
    const DIGITS: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let mut value = position.max(0) as u64;
    let mut key = [b'0'; 11];
    for slot in key.iter_mut().rev() {
        *slot = DIGITS[(value % 62) as usize];
        value /= 62;
    }
    OrderKey::new(String::from_utf8_lossy(&key).into_owned())
}

pub fn project_from_legacy(project: &LegacyProject) -> Project {
    let created_at = (project.created_at.max(0)).saturating_mul(1_000);
    Project {
        id: ProjectId(project.id),
        name: project.name.clone(),
        path: project.path.clone(),
        order_key: position_order_key(project.position),
        created_at,
        updated_at: created_at,
        removed_at: None,
    }
}

/// The provider thread an imported shell resumes natively (R23).
fn provider_thread_id(thread_id: ThreadId) -> ProviderThreadId {
    // The scope `thread.create` uses for an imported native thread.
    ProviderThreadId(derived_id(
        "imported_provider_thread",
        &thread_id.to_string(),
    ))
}

/// The shell of a session: the thread and, with a resume cursor, its native
/// provider thread (RFC 0001 "Shell import").
pub fn shell_from_session(session: &legacy::AgentSession) -> LegacyThreadShell {
    let thread_id = ThreadId(session.id);
    let driver = DriverKind::from(session.provider);
    let instance = ProviderInstanceId::new(driver.as_str());
    let title = session.title.trim();
    let auto_title = session
        .auto_title
        .as_deref()
        .map(str::trim)
        .filter(|title| !title.is_empty());
    let (title, title_source) = if !title.is_empty() && title != legacy::AgentSession::DEFAULT_TITLE
    {
        (title.to_owned(), TitleSource::User)
    } else if let Some(auto_title) = auto_title {
        (auto_title.to_owned(), TitleSource::Generated)
    } else {
        (PLACEHOLDER_TITLE.to_owned(), TitleSource::Placeholder)
    };
    let (branch, worktree_path, worktree_owned) = match &session.workspace {
        legacy::SessionWorkspace::Worktree { path, branch } => {
            (Some(branch.clone()), Some(path.clone()), true)
        }
        legacy::SessionWorkspace::Local | legacy::SessionWorkspace::NewWorktree { .. } => {
            (None, None, false)
        }
    };
    let created_at = millis(session.created_at);
    let updated_at = millis(session.updated_at);
    let provider_thread = session.provider_cursor.as_ref().and_then(|cursor| {
        let (reference, cursor_json) = resume_cursor_ref(cursor).ok()?;
        Some(ProviderThread {
            id: provider_thread_id(thread_id),
            driver: reference.driver,
            provider_instance_id: ProviderInstanceId::new(reference.driver.as_str()),
            provider_session_id: None,
            app_thread_id: Some(thread_id),
            owner_node_id: None,
            native_thread_ref: Some(reference),
            native_conversation_head_ref: None,
            status: ProviderThreadStatus::NotLoaded,
            first_run_ordinal: None,
            last_run_ordinal: None,
            handoff_ids: Vec::new(),
            forked_from: None,
            pending_background_tasks: Vec::new(),
            context_usage: session.context_usage.map(Into::into),
            goal: session.thread_goal.as_ref().map(Into::into),
            native_metadata: Some(NativeMetadata {
                title: auto_title.map(str::to_owned),
                updated_at: Some(updated_at),
                resume_cursor: Some(cursor_json),
            }),
            created_at,
            updated_at,
        })
    });
    let thread = AppThread {
        id: thread_id,
        project_id: ProjectId(session.project_id),
        created_by: Actor::User,
        creation_source: CreationSource::Import,
        title,
        title_source,
        model_selection: ModelSelection {
            instance,
            model: session.model.clone(),
            reasoning_effort: session.reasoning_effort.clone(),
            service_tier: session.service_tier.clone(),
            context_window: session.context_window.clone(),
            agent_preset: session.agent_preset.clone(),
        },
        runtime_mode: session.runtime_mode.into(),
        interaction_mode: InteractionMode::Default,
        branch,
        worktree_path,
        worktree_owned,
        active_provider_thread_id: provider_thread.as_ref().map(|thread| thread.id),
        history_origin: HistoryOrigin::WakuImport,
        lineage: ThreadLineage::root(thread_id),
        forked_from: None,
        created_at,
        updated_at,
        last_reply_at: session.last_reply_at.map(millis),
        archived_at: None,
        deleted_at: None,
        settled_override: None,
        settled_at: None,
        unsettled_at: None,
        auto_settle_disabled_at: None,
        snoozed_until: None,
        snoozed_at: None,
        last_snooze_wake_at: None,
        pinned_at: None,
        pin_order_key: None,
        active_order_key: None,
        // An imported thread opens as read.
        last_visited_at: Some(updated_at),
        limit_recovery: None,
        title_regeneration: None,
        rollback: None,
    };
    LegacyThreadShell {
        thread,
        provider_thread,
    }
}

fn run_status(
    status: legacy::TurnStatus,
) -> (RunStatus, AttemptStatus, NodeStatus, ProviderTurnStatus) {
    match status {
        legacy::TurnStatus::Completed => (
            RunStatus::Completed,
            AttemptStatus::Completed,
            NodeStatus::Completed,
            ProviderTurnStatus::Completed,
        ),
        legacy::TurnStatus::Failed => (
            RunStatus::Failed,
            AttemptStatus::Failed,
            NodeStatus::Failed,
            ProviderTurnStatus::Failed,
        ),
        // A turn still running in the old app cannot be running now.
        legacy::TurnStatus::Running | legacy::TurnStatus::Interrupted => (
            RunStatus::Interrupted,
            AttemptStatus::Interrupted,
            NodeStatus::Interrupted,
            ProviderTurnStatus::Interrupted,
        ),
    }
}

fn checkpoint_status(status: legacy::CheckpointStatus) -> CheckpointStatus {
    match status {
        legacy::CheckpointStatus::Ready => CheckpointStatus::Ready,
        legacy::CheckpointStatus::Unavailable => CheckpointStatus::Missing,
        legacy::CheckpointStatus::Error => CheckpointStatus::Error,
    }
}

fn saturate(count: u64) -> u32 {
    u32::try_from(count).unwrap_or(u32::MAX)
}

/// The display item of a legacy activity (RFC 0001 "Transcript import").
fn activity_kind(activity: &legacy::ActivityItem) -> TurnItemKind {
    if let Some(reasoning) = &activity.reasoning {
        return TurnItemKind::Reasoning {
            text: reasoning.content.clone(),
            streaming: false,
        };
    }
    let target = activity
        .display_target
        .clone()
        .or_else(|| activity.detail.clone());
    match activity.kind {
        legacy::ActivityKind::Reasoning => TurnItemKind::Reasoning {
            text: activity
                .detail
                .clone()
                .or_else(|| activity.output.clone())
                .unwrap_or_default(),
            streaming: false,
        },
        legacy::ActivityKind::Command => TurnItemKind::CommandExecution {
            input: target.unwrap_or_else(|| activity.title.clone()),
            output: activity.output.clone(),
            output_truncated: false,
            output_omitted: false,
            output_indicates_failure: activity.failed,
            exit_code: None,
        },
        legacy::ActivityKind::FileChange => {
            let additions = activity
                .file_changes
                .iter()
                .filter_map(|change| change.additions)
                .sum::<u64>();
            let deletions = activity
                .file_changes
                .iter()
                .filter_map(|change| change.deletions)
                .sum::<u64>();
            let counted = activity
                .file_changes
                .iter()
                .any(|change| change.additions.is_some() || change.deletions.is_some());
            let diff = match activity.file_changes.as_slice() {
                [only] => only.diff.clone(),
                _ => None,
            };
            TurnItemKind::FileChange {
                file_name: activity
                    .file_changes
                    .first()
                    .map(|change| change.path.clone())
                    .or(target)
                    .unwrap_or_else(|| activity.title.clone()),
                additions: counted.then_some(saturate(additions)),
                deletions: counted.then_some(saturate(deletions)),
                diff,
                old_text: None,
                new_text: None,
                changes: activity
                    .file_changes
                    .iter()
                    .map(|change| FileChangeDetail {
                        operation: match change.status {
                            Some(legacy::ActivityFileChangeStatus::Added) => "add",
                            Some(legacy::ActivityFileChangeStatus::Deleted) => "delete",
                            _ => "update",
                        }
                        .to_owned(),
                        path: change.path.clone(),
                        old_path: None,
                        file_type: None,
                        mime_type: None,
                    })
                    .collect(),
                body_omitted: false,
            }
        }
        legacy::ActivityKind::FileSearch => TurnItemKind::FileSearch {
            pattern: target,
            results: Vec::new(),
        },
        legacy::ActivityKind::Search => TurnItemKind::WebSearch {
            patterns: target.into_iter().collect(),
            results: Vec::new(),
        },
        legacy::ActivityKind::FileRead
        | legacy::ActivityKind::FileList
        | legacy::ActivityKind::Plan
        | legacy::ActivityKind::Tool => TurnItemKind::DynamicTool {
            tool_name: activity.tool_name.clone(),
            input: JsonText(serde_json::to_string(activity).unwrap_or_else(|_| "{}".to_owned())),
            output: activity
                .output
                .as_ref()
                .map(|output| JsonText(serde_json::to_string(output).unwrap_or_default())),
            output_omitted: false,
            viewed_image_path: None,
        },
    }
}

/// Assigns R15 positions in order of appearance: an item of a known turn
/// goes to that run's block, any other item to the latest block seen.
struct Placer<'a> {
    thread_id: ThreadId,
    provider_thread_id: ProviderThreadId,
    runs_by_turn: &'a HashMap<Uuid, (RunId, NodeId, ProviderTurnId, u32)>,
    next_index: HashMap<u32, u32>,
    current_block: u32,
    updated_at: Timestamp,
    items: Vec<TurnItem>,
}

impl Placer<'_> {
    fn push(
        &mut self,
        id: TurnItemId,
        turn_id: Option<Uuid>,
        status: ItemStatus,
        title: Option<String>,
        at: Option<Timestamp>,
        kind: TurnItemKind,
    ) {
        let run = turn_id
            .and_then(|turn| self.runs_by_turn.get(&turn))
            .copied();
        let block = match run {
            Some((.., ordinal)) => {
                self.current_block = self.current_block.max(ordinal);
                ordinal
            }
            None => self.current_block,
        };
        let index = self.next_index.entry(block).or_insert(0);
        let position = TurnItem::position_of(block, *index);
        *index += 1;
        self.items.push(TurnItem {
            id,
            thread_id: self.thread_id,
            run_id: run.map(|(run_id, ..)| run_id),
            node_id: run.map(|(_, node_id, ..)| node_id),
            provider_thread_id: run.map(|_| self.provider_thread_id),
            provider_turn_id: run.map(|(_, _, provider_turn_id, _)| provider_turn_id),
            native_item_ref: None,
            parent_item_id: None,
            position,
            status,
            title,
            tool: None,
            stream_rev: 0,
            started_at: at,
            completed_at: at.filter(|_| status.is_terminal()),
            updated_at: at.unwrap_or(self.updated_at),
            kind,
        });
    }

    /// The activities of one transcript block.
    fn block(&mut self, block: &legacy::TranscriptBlock) {
        for activity in &block.activities {
            let status = if activity.failed {
                ItemStatus::Failed
            } else if activity.complete {
                ItemStatus::Completed
            } else {
                ItemStatus::Interrupted
            };
            let at = activity
                .reasoning
                .as_ref()
                .map(|reasoning| reasoning.started_at_ms as Timestamp);
            self.push(
                TurnItemId(derived_id("imported_activity", &activity.id.to_string())),
                block.turn_id,
                status,
                Some(activity.title.clone()).filter(|title| !title.is_empty()),
                at,
                activity_kind(activity),
            );
        }
    }
}

/// Converts one session's history (RFC 0001 "Transcript import"). Pure.
///
/// Each `AgentTurn` becomes a run with the turn's id, ordinal `turn_count`
/// (made strictly increasing if the old data repeats one), one initial
/// attempt, a root node and a provider turn with a weak native ref from
/// `provider_resume_at`. Messages keep their ids. Items are placed in their
/// turn's run block, in the order messages and transcript blocks appeared;
/// a run's checkpoint item ends its block. Queued messages become held
/// queued runs. `project_root` is the working directory of checkpoint scopes
/// without a worktree.
pub fn transcript_from_session(
    thread: &AppThread,
    provider_thread: Option<&ProviderThread>,
    project_root: &Path,
    session: &legacy::AgentSession,
    messages: &[legacy::Message],
) -> LegacyTranscript {
    let thread_id = thread.id;
    let derived = |scope: &str, key: &dyn fmt::Display| derived_id(scope, &key.to_string());
    let updated_at = millis(session.updated_at);
    let driver = DriverKind::from(session.provider);
    let mut transcript = LegacyTranscript::default();

    // Ordinals, strictly increasing.
    let mut turns = session.turns.iter().collect::<Vec<_>>();
    turns.sort_by_key(|turn| (turn.turn_count, turn.started_at));
    let mut ordinals = HashMap::new();
    let mut last = 0u32;
    for turn in &turns {
        let wanted = u32::try_from(turn.turn_count).unwrap_or(u32::MAX);
        let ordinal = if wanted > last { wanted } else { last + 1 };
        last = ordinal;
        ordinals.insert(turn.id, ordinal);
    }

    let mut provider_thread = provider_thread.cloned().unwrap_or_else(|| ProviderThread {
        // Runs need a provider thread; without a cursor there is no native
        // conversation to resume, so the placeholder is closed and never the
        // thread's active one.
        id: provider_thread_id(thread_id),
        driver,
        provider_instance_id: thread.model_selection.instance.clone(),
        provider_session_id: None,
        app_thread_id: Some(thread_id),
        owner_node_id: None,
        native_thread_ref: None,
        native_conversation_head_ref: None,
        status: ProviderThreadStatus::Closed,
        first_run_ordinal: None,
        last_run_ordinal: None,
        handoff_ids: Vec::new(),
        forked_from: None,
        pending_background_tasks: Vec::new(),
        context_usage: None,
        goal: None,
        native_metadata: None,
        created_at: thread.created_at,
        updated_at: thread.created_at,
    });
    if !turns.is_empty() {
        provider_thread.first_run_ordinal = ordinals.values().min().copied();
        provider_thread.last_run_ordinal = ordinals.values().max().copied();
    }
    let native_thread_id = provider_thread
        .native_thread_ref
        .as_ref()
        .and_then(|reference| reference.native_id.clone());

    // Runs and their control rows.
    let mut runs_by_turn: HashMap<Uuid, (RunId, NodeId, ProviderTurnId, u32)> = HashMap::new();
    for turn in &turns {
        let ordinal = ordinals[&turn.id];
        let run_id = RunId(turn.id);
        let node_id = NodeId(derived("root_node", &run_id));
        let attempt_id = RunAttemptId(derived("run_attempt", &format!("{run_id}:1")));
        let provider_turn_id = ProviderTurnId(derived("imported_provider_turn", &run_id));
        let (status, attempt_status, node_status, turn_status) = run_status(turn.status);
        let started_at = millis(turn.started_at);
        let completed_at = turn
            .completed_at
            .map(millis)
            .or_else(|| (turn.status == legacy::TurnStatus::Running).then_some(updated_at));
        let user_message_id = messages
            .iter()
            .find(|message| {
                message.turn_id == Some(turn.id) && message.role == legacy::MessageRole::User
            })
            .map(|message| MessageId(message.id))
            .unwrap_or_else(|| MessageId(derived("imported_user_message", &run_id)));
        let checkpoint = turn.checkpoint.as_ref().map(|checkpoint| {
            let scope_id = CheckpointScopeId(derived("root_scope", &run_id));
            let checkpoint_id = CheckpointId(derived("checkpoint", &format!("{scope_id}:0")));
            transcript.checkpoint_scopes.push(CheckpointScope {
                id: scope_id,
                thread_id,
                run_id: Some(run_id),
                node_id,
                parent_scope_id: None,
                provider_thread_id: Some(provider_thread.id),
                kind: ScopeKind::RootRun,
                ordinal_within_parent: ordinal,
                advances_app_run_count: true,
                cwd: thread
                    .worktree_path
                    .clone()
                    .unwrap_or_else(|| project_root.to_owned()),
                created_at: millis(checkpoint.created_at),
            });
            transcript.checkpoints.push(Checkpoint {
                id: checkpoint_id,
                thread_id,
                scope_id,
                run_id: Some(run_id),
                node_id,
                parent_checkpoint_id: None,
                ordinal_within_scope: 0,
                app_run_ordinal: Some(ordinal),
                git_ref: checkpoint.git_ref.clone(),
                status: checkpoint_status(checkpoint.status),
                files: checkpoint
                    .files
                    .iter()
                    .map(|file| CheckpointFile {
                        path: file.path.clone(),
                        kind: "modified".to_owned(),
                        additions: saturate(file.additions),
                        deletions: saturate(file.deletions),
                    })
                    .collect(),
                captured_at: millis(checkpoint.created_at),
            });
            (scope_id, checkpoint_id)
        });
        transcript.runs.push(Run {
            id: run_id,
            thread_id,
            ordinal: Some(ordinal),
            model_selection: thread.model_selection.clone(),
            provider_thread_id: Some(provider_thread.id),
            user_message_id,
            root_node_id: Some(node_id),
            active_attempt_id: Some(attempt_id),
            status,
            queue_position: None,
            queue_held: false,
            requested_at: started_at,
            started_at: Some(started_at),
            provider_completed_at: completed_at,
            completed_at,
            failure: None,
            checkpoint_id: checkpoint.map(|(_, id)| id),
            context_handoff_id: None,
            continuation: None,
            workspace_preparation: None,
            restart_cancelled_background_work: Vec::new(),
            source_plan: None,
            delegated_task_id: None,
            delegated_completion: None,
        });
        transcript.attempts.push(RunAttempt {
            id: attempt_id,
            run_id,
            attempt_ordinal: 1,
            root_node_id: node_id,
            provider_instance_id: provider_thread.provider_instance_id.clone(),
            provider_thread_id: provider_thread.id,
            native_thread_id: native_thread_id.clone(),
            provider_turn_id: Some(provider_turn_id),
            reason: AttemptReason::Initial,
            status: attempt_status,
            started_at: Some(started_at),
            completed_at,
        });
        transcript.nodes.push(ExecutionNode {
            id: node_id,
            thread_id,
            run_id: Some(run_id),
            parent_node_id: None,
            root_node_id: node_id,
            kind: NodeKind::RootTurn,
            status: node_status,
            counts_for_run: true,
            provider_thread_id: Some(provider_thread.id),
            provider_turn_id: Some(provider_turn_id),
            native_item_ref: None,
            runtime_request_id: None,
            checkpoint_scope_id: checkpoint.map(|(scope, _)| scope),
            started_at: Some(started_at),
            completed_at,
        });
        transcript.provider_turns.push(ProviderTurn {
            id: provider_turn_id,
            provider_thread_id: provider_thread.id,
            node_id,
            run_attempt_id: Some(attempt_id),
            native_turn_ref: turn.provider_resume_at.as_ref().map(|native| ProviderRef {
                driver: provider_thread.driver,
                native_id: Some(native.clone()),
                strength: RefStrength::Weak,
                fingerprint: None,
                ordinal: Some(ordinal),
            }),
            ordinal,
            status: turn_status,
            started_at: Some(started_at),
            completed_at,
            token_usage: None,
            turn_token_usage: None,
        });
        runs_by_turn.insert(turn.id, (run_id, node_id, provider_turn_id, ordinal));
    }

    // Items in order of appearance: blocks render after `after_message`
    // messages.
    let mut placer = Placer {
        thread_id,
        provider_thread_id: provider_thread.id,
        runs_by_turn: &runs_by_turn,
        next_index: HashMap::new(),
        current_block: 0,
        updated_at,
        items: Vec::new(),
    };
    let mut blocks = session.transcript_blocks.iter().collect::<Vec<_>>();
    blocks.sort_by_key(|block| block.after_message);
    let mut blocks = blocks.into_iter().peekable();
    let run_prompts = transcript
        .runs
        .iter()
        .map(|run| run.user_message_id)
        .collect::<std::collections::HashSet<_>>();
    for (shown, message) in messages.iter().enumerate() {
        while let Some(block) = blocks.next_if(|block| block.after_message <= shown) {
            placer.block(block);
        }
        let run = message
            .turn_id
            .and_then(|turn| runs_by_turn.get(&turn))
            .copied();
        let created_at = millis(message.created_at);
        let (role, created_by) = match message.role {
            legacy::MessageRole::User => (MessageRole::User, Actor::User),
            legacy::MessageRole::Assistant => (MessageRole::Assistant, Actor::Agent),
            legacy::MessageRole::System => (MessageRole::System, Actor::System),
        };
        let attachments = message
            .attachments
            .iter()
            .map(Attachment::from)
            .collect::<Vec<_>>();
        let message_id = MessageId(message.id);
        transcript.messages.push(Message {
            id: message_id,
            thread_id,
            run_id: run.map(|(run_id, ..)| run_id),
            node_id: run.map(|(_, node_id, ..)| node_id),
            role,
            created_by,
            creation_source: CreationSource::Import,
            text: message.content.clone(),
            display_text: message.display_content.clone(),
            attachments: attachments.clone(),
            context: Vec::new(),
            notification: None,
            sender_thread_id: None,
            streaming: false,
            created_at,
            updated_at: created_at,
        });
        let status = if message.streaming {
            ItemStatus::Interrupted
        } else {
            ItemStatus::Completed
        };
        let scope = if role == MessageRole::User && run_prompts.contains(&message_id) {
            "user_item"
        } else {
            "imported_item"
        };
        let kind = match role {
            MessageRole::User => TurnItemKind::UserMessage {
                message_id,
                input_intent: InputIntent::TurnStart,
                text: message.content.clone(),
                display_text: message.display_content.clone(),
                attachments,
                created_by,
                sender_thread_id: None,
            },
            MessageRole::Assistant => TurnItemKind::AssistantMessage {
                message_id,
                text: message.content.clone(),
                streaming: false,
            },
            _ => TurnItemKind::SystemNotice {
                message: message.content.clone(),
            },
        };
        placer.push(
            TurnItemId(derived_id(scope, &message_id.to_string())),
            message.turn_id,
            status,
            None,
            Some(created_at),
            kind,
        );
    }
    for block in blocks {
        placer.block(block);
    }

    // Each checkpoint ends its run's block.
    for checkpoint in transcript
        .checkpoints
        .iter()
        .filter(|checkpoint| checkpoint.status == CheckpointStatus::Ready)
    {
        placer.push(
            TurnItemId(derived_id("checkpoint_item", &checkpoint.id.to_string())),
            checkpoint.run_id.map(|run| run.0),
            ItemStatus::Completed,
            None,
            Some(checkpoint.captured_at),
            TurnItemKind::Checkpoint {
                checkpoint_id: checkpoint.id,
                scope_id: checkpoint.scope_id,
                files: checkpoint.files.clone(),
            },
        );
    }

    // What the old app was doing when it stopped.
    let last_turn = turns.last().map(|turn| turn.id);
    if session.status.is_busy() {
        placer.push(
            TurnItemId(derived_id("imported_notice", &thread_id.to_string())),
            last_turn,
            ItemStatus::Completed,
            None,
            Some(updated_at),
            TurnItemKind::SystemNotice {
                message: NOTICE_INTERRUPTED.to_owned(),
            },
        );
    }
    if session.status == legacy::SessionStatus::Waiting {
        placer.push(
            TurnItemId(derived_id(
                "imported_request_notice",
                &thread_id.to_string(),
            )),
            last_turn,
            ItemStatus::Completed,
            None,
            Some(updated_at),
            TurnItemKind::SystemNotice {
                message: NOTICE_PENDING_REQUEST.to_owned(),
            },
        );
    }
    transcript.items = placer.items;

    // The follow-up queue, held so nothing starts without the user.
    for (index, queued) in session.queued_messages.iter().enumerate() {
        let message_id = MessageId(queued.id);
        let run_id = RunId(derived_id("run", &message_id.to_string()));
        let created_at = millis(queued.created_at);
        transcript.runs.push(Run {
            id: run_id,
            thread_id,
            ordinal: None,
            model_selection: thread.model_selection.clone(),
            provider_thread_id: None,
            user_message_id: message_id,
            root_node_id: None,
            active_attempt_id: None,
            status: RunStatus::Queued,
            queue_position: Some(index as u32 + 1),
            queue_held: true,
            requested_at: created_at,
            started_at: None,
            provider_completed_at: None,
            completed_at: None,
            failure: None,
            checkpoint_id: None,
            context_handoff_id: None,
            continuation: None,
            workspace_preparation: None,
            restart_cancelled_background_work: Vec::new(),
            source_plan: None,
            delegated_task_id: None,
            delegated_completion: None,
        });
        transcript.messages.push(Message {
            id: message_id,
            thread_id,
            run_id: Some(run_id),
            node_id: None,
            role: MessageRole::User,
            created_by: Actor::User,
            creation_source: CreationSource::Import,
            text: queued.content.clone(),
            display_text: queued.display_content.clone(),
            attachments: queued.attachments.iter().map(Attachment::from).collect(),
            context: Vec::new(),
            notification: None,
            sender_thread_id: None,
            streaming: false,
            created_at,
            updated_at: created_at,
        });
    }
    if !transcript.runs.is_empty() || !transcript.items.is_empty() {
        transcript.provider_thread = Some(provider_thread);
    }
    transcript
}

// -- importing ------------------------------------------------------------------

fn command_id(prefix: &str, fingerprint: &str, id: impl fmt::Display) -> CommandId {
    CommandId::new(format!("{prefix}:{fingerprint}:{id}")).expect("import command ids are valid")
}

fn storage(error: impl fmt::Display) -> ImportError {
    ImportError::Storage(error.to_string())
}

/// Imports every project and session shell of `source`. Safe to rerun:
/// already imported sessions are skipped and command receipts dedupe the
/// rest. Transcripts follow lazily.
pub fn import_shells(
    orchestrator: &Orchestrator,
    source: &LegacySource,
) -> Result<ImportReport, ImportError> {
    let fingerprint = fingerprint(&source.path)?;
    let connection = open_read_only(&source.path)?;
    let projects = read_projects(&connection)?;
    let sessions = read_session_shells(&connection)?;
    drop(connection);
    let now = orchestrator.clock().now();
    {
        let canonical = std::fs::canonicalize(&source.path).unwrap_or_else(|_| source.path.clone());
        orchestrator
            .store()
            .writer()
            .execute(
                "INSERT OR IGNORE INTO legacy_import_sources
                   (source_fingerprint, source, path, imported_at) VALUES (?1, ?2, ?3, ?4)",
                params![
                    fingerprint,
                    source.kind.as_str(),
                    canonical.to_string_lossy(),
                    now
                ],
            )
            .map_err(storage)?;
    }
    let mut report = ImportReport {
        fingerprint: fingerprint.clone(),
        ..ImportReport::default()
    };
    for project in &projects {
        let command = InternalCommand::ImportLegacyProject {
            project: project_from_legacy(project),
        };
        match orchestrator.dispatch_internal(
            command_id("import-project", &fingerprint, project.id),
            command,
        ) {
            Ok(_) => report.projects += 1,
            Err(error) => report
                .failed
                .push((project.id.to_string(), error.to_string())),
        }
    }
    let imported = orchestrator
        .reader()
        .with(
            |connection| -> rusqlite::Result<std::collections::HashSet<String>> {
                let mut statement = connection.prepare("SELECT session_id FROM legacy_imports")?;
                let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
                rows.collect()
            },
        )
        .map_err(storage)?;
    for session in &sessions {
        let id = session.id.to_string();
        if imported.contains(&id) {
            report.skipped += 1;
            continue;
        }
        let shell = shell_from_session(session);
        let thread_id = shell.thread.id;
        match orchestrator.dispatch_internal(
            command_id("import", &fingerprint, &id),
            InternalCommand::ImportLegacyThread { shell },
        ) {
            Ok(_) => {
                orchestrator
                    .store()
                    .writer()
                    .execute(
                        "INSERT OR IGNORE INTO legacy_imports
                           (session_id, source, source_fingerprint, shell_imported_at)
                         VALUES (?1, ?2, ?3, ?4)",
                        params![
                            id,
                            source.kind.as_str(),
                            fingerprint,
                            orchestrator.clock().now()
                        ],
                    )
                    .map_err(storage)?;
                orchestrator.note_pending_transcript(thread_id);
                report.threads += 1;
            }
            Err(error) => report.failed.push((id, error.to_string())),
        }
    }
    Ok(report)
}

/// Imports the fork's own `app.db` if it exists (the automatic path, R18).
pub fn import_fork_app_db(
    orchestrator: &Orchestrator,
    path: &Path,
) -> Result<Option<ImportReport>, ImportError> {
    if !path.is_file() {
        return Ok(None);
    }
    import_shells(orchestrator, &LegacySource::fork(path)).map(Some)
}

/// Imports one thread's transcript from the source its shell came from.
/// Called through `Orchestrator::ensure_transcript`, which serializes it.
pub fn import_transcript(
    orchestrator: &Orchestrator,
    thread_id: ThreadId,
) -> Result<TranscriptOutcome, ImportError> {
    let pending = orchestrator
        .reader()
        .with(|connection| {
            connection
                .query_row(
                    "SELECT i.source, i.source_fingerprint, s.path
                     FROM legacy_imports AS i
                     LEFT JOIN legacy_import_sources AS s
                       ON s.source_fingerprint = i.source_fingerprint
                     WHERE i.session_id = ?1 AND i.transcript_imported_at IS NULL",
                    [thread_id.to_string()],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Option<String>>(2)?,
                        ))
                    },
                )
                .optional()
        })
        .map_err(storage)?;
    let Some((source, fingerprint, path)) = pending else {
        return Ok(TranscriptOutcome::NotPending);
    };
    let record = |error: Option<&str>| -> Result<(), ImportError> {
        let now = orchestrator.clock().now();
        orchestrator
            .store()
            .writer()
            .execute(
                "UPDATE legacy_imports SET transcript_imported_at = ?2, error = ?3
                 WHERE session_id = ?1",
                params![thread_id.to_string(), error.is_none().then_some(now), error],
            )
            .map_err(storage)?;
        Ok(())
    };
    let read = (|| -> Result<_, ImportError> {
        let path = path
            .map(PathBuf::from)
            .ok_or_else(|| ImportError::Source("the import source is unknown".to_owned()))?;
        if LegacySourceKind::parse(&source).is_none() {
            return Err(ImportError::Source(format!("unknown source {source}")));
        }
        let connection = open_read_only(&path)?;
        Ok(read_session(&connection, thread_id.0)?)
    })();
    let (session, messages) = match read {
        Ok(Some(found)) => found,
        Ok(None) => {
            // The session is gone from the source: nothing more to import.
            record(None)?;
            return Ok(TranscriptOutcome::Imported { runs: 0, items: 0 });
        }
        Err(error) => {
            let message = error.to_string();
            record(Some(&message))?;
            return Ok(TranscriptOutcome::Failed(message));
        }
    };
    let projection = orchestrator.thread(thread_id)?;
    let project_root = orchestrator
        .project(projection.thread.project_id)
        .map(|project| project.path)
        .unwrap_or_default();
    let provider_thread = projection
        .thread
        .active_provider_thread_id
        .and_then(|id| projection.provider_threads.get(&id));
    if !projection.runs.is_empty() || !projection.next_position.is_empty() {
        record(Some("skipped: the thread has newer history"))?;
        return Ok(TranscriptOutcome::Skipped);
    }
    let transcript = transcript_from_session(
        &projection.thread,
        provider_thread,
        &project_root,
        &session,
        &messages,
    );
    let (runs, items) = (transcript.runs.len(), transcript.items.len());
    drop(projection);
    orchestrator.dispatch_internal(
        command_id("import-transcript", &fingerprint, thread_id),
        InternalCommand::ImportLegacyTranscript {
            thread_id,
            transcript,
        },
    )?;
    record(None)?;
    Ok(TranscriptOutcome::Imported { runs, items })
}

/// Threads whose transcript has not been imported yet.
pub(crate) fn pending_transcripts(connection: &Connection) -> rusqlite::Result<Vec<ThreadId>> {
    let mut statement = connection
        .prepare("SELECT session_id FROM legacy_imports WHERE transcript_imported_at IS NULL")?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows.into_iter().filter_map(|id| id.parse().ok()).collect())
}
