//! Shell reads for the `shell` stream and `threads.archived` (#54).
//!
//! The shell holds navigation rows only: projects and threads with their
//! derived sidebar state, never transcript bodies. The active location reads
//! `p_threads_shell`, the archive `p_threads_archived`; a resume sends the
//! rows touched since the cursor (compact deltas), not a full snapshot.

use std::collections::{BTreeMap, HashMap};

use rusqlite::{Connection, OptionalExtension, params};
use waku_protocol::orchestration::*;
use waku_protocol::v8::StreamCursor;
use waku_protocol::v8::orchestration::{
    ArchivedPage, MAX_ARCHIVED_PAGE, SHELL_REPLAY_EVENTS, SHELL_SNAPSHOT_BYTES, ShellItem,
    ShellLocation, ShellSpec, ThreadActivity, ThreadRow,
};

use super::cursor::{BadCursor, decode_archived, encode_archived};
use super::thread::encoded_len;
use crate::store::{StoreError, StoreResult, decode, max_sequence};

/// Event types that can change a navigation row.
pub const SHELL_EVENT_TYPES: [&str; 4] = [
    "project.updated",
    "thread.updated",
    "run.updated",
    "runtime_request.updated",
];

/// Whether `event` can change a navigation row.
pub fn touches_shell(event: &DomainEvent) -> bool {
    matches!(
        event,
        DomainEvent::ProjectUpdated(_)
            | DomainEvent::ThreadUpdated(_)
            | DomainEvent::RunUpdated(_)
            | DomainEvent::RuntimeRequestUpdated(_)
    )
}

fn thread_row_from(
    json: &str,
    activity: &str,
    pending: i64,
    queued: i64,
) -> StoreResult<ThreadRow> {
    Ok(ThreadRow {
        thread: decode("p_threads", json)?,
        activity: ThreadActivity::parse(activity),
        pending_request_count: pending.max(0) as u32,
        queued_run_count: queued.max(0) as u32,
    })
}

const ROW_COLUMNS: &str = "payload_json, activity, pending_request_count, queued_run_count";

/// One thread's navigation row as of now.
pub fn thread_row(connection: &Connection, thread_id: ThreadId) -> StoreResult<Option<ThreadRow>> {
    let row = connection
        .prepare_cached(&format!(
            "SELECT {ROW_COLUMNS} FROM p_threads WHERE thread_id = ?1"
        ))?
        .query_row([thread_id.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .optional()?;
    row.map(|(json, activity, pending, queued)| thread_row_from(&json, &activity, pending, queued))
        .transpose()
}

fn project_row(connection: &Connection, project_id: ProjectId) -> StoreResult<Option<Project>> {
    connection
        .prepare_cached("SELECT payload_json FROM p_projects WHERE project_id = ?1")?
        .query_row([project_id.to_string()], |row| row.get::<_, String>(0))
        .optional()?
        .map(|json| decode("p_projects", &json))
        .transpose()
}

/// The rows of a shell at one consistent point of the log.
pub struct ShellRows {
    pub cursor: StreamCursor,
    pub projects: Vec<Project>,
    pub threads: Vec<ThreadRow>,
}

/// Every row `spec` holds now, in one read transaction.
pub fn shell_rows(
    connection: &Connection,
    store_id: uuid::Uuid,
    spec: &ShellSpec,
) -> StoreResult<ShellRows> {
    let transaction = connection.unchecked_transaction()?;
    let connection = &*transaction;
    let head = max_sequence(connection)?;
    let mut projects: Vec<Project> = Vec::new();
    {
        let mut statement = connection.prepare_cached(
            "SELECT payload_json FROM p_projects WHERE removed_at IS NULL ORDER BY order_key",
        )?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        for row in rows {
            let project: Project = decode("p_projects", &row?)?;
            if spec.project_id.is_none_or(|id| id == project.id) {
                projects.push(project);
            }
        }
    }
    let filter = match spec.location {
        ShellLocation::Archived => "archived_at IS NOT NULL AND deleted_at IS NULL",
        // An unknown location is answered with the active rows.
        _ => "archived_at IS NULL AND deleted_at IS NULL",
    };
    let sql = match spec.project_id {
        Some(_) => format!(
            "SELECT {ROW_COLUMNS} FROM p_threads WHERE project_id = ?1 AND {filter}
             ORDER BY last_reply_at DESC, thread_id"
        ),
        None => format!(
            "SELECT {ROW_COLUMNS} FROM p_threads WHERE {filter}
             ORDER BY last_reply_at DESC, thread_id"
        ),
    };
    let mut statement = connection.prepare_cached(&sql)?;
    let map = |row: &rusqlite::Row<'_>| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
        ))
    };
    let rows = match spec.project_id {
        Some(project_id) => statement
            .query_map([project_id.to_string()], map)?
            .collect::<Result<Vec<_>, _>>()?,
        None => statement
            .query_map([], map)?
            .collect::<Result<Vec<_>, _>>()?,
    };
    // Decode after the reads (T3: decoding is slower than reading).
    drop(statement);
    drop(transaction);
    let threads = rows
        .iter()
        .map(|(json, activity, pending, queued)| thread_row_from(json, activity, *pending, *queued))
        .collect::<StoreResult<Vec<_>>>()?;
    Ok(ShellRows {
        cursor: StreamCursor::Sequence {
            store: store_id,
            seq: head,
        },
        projects,
        threads,
    })
}

/// `rows` as `snapshot` items of at most `max_bytes` each (only the last has
/// `complete`). Always at least one item.
pub fn split_snapshot(rows: ShellRows, max_bytes: usize) -> Vec<ShellItem> {
    let mut items = Vec::new();
    let mut projects = Vec::new();
    let mut threads = Vec::new();
    let mut size = 0;
    let flush = |projects: &mut Vec<Project>,
                 threads: &mut Vec<ThreadRow>,
                 items: &mut Vec<ShellItem>,
                 size: &mut usize| {
        items.push(ShellItem::Snapshot {
            cursor: rows.cursor.clone(),
            projects: std::mem::take(projects),
            threads: std::mem::take(threads),
            complete: false,
        });
        *size = 0;
    };
    for project in rows.projects.iter().cloned() {
        let length = encoded_len(&project);
        if size + length > max_bytes && (!projects.is_empty() || !threads.is_empty()) {
            flush(&mut projects, &mut threads, &mut items, &mut size);
        }
        size += length;
        projects.push(project);
    }
    for thread in rows.threads.iter().cloned() {
        let length = encoded_len(&thread);
        if size + length > max_bytes && (!projects.is_empty() || !threads.is_empty()) {
            flush(&mut projects, &mut threads, &mut items, &mut size);
        }
        size += length;
        threads.push(thread);
    }
    flush(&mut projects, &mut threads, &mut items, &mut size);
    if let Some(ShellItem::Snapshot { complete, .. }) = items.last_mut() {
        *complete = true;
    }
    items
}

/// [`split_snapshot`] with the protocol's limit.
pub fn snapshot_items(rows: ShellRows) -> Vec<ShellItem> {
    split_snapshot(rows, SHELL_SNAPSHOT_BYTES)
}

/// A navigation row changed by one committed event, as the live shell sees
/// it: computed once per event and filtered per subscription.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShellChange {
    Project { seq: Sequence, row: Project },
    Thread { seq: Sequence, row: ThreadRow },
    ThreadGone { seq: Sequence, thread_id: ThreadId },
}

/// The navigation row `event` changed, read after its commit.
pub fn shell_change(
    connection: &Connection,
    event: &StoredEvent,
) -> StoreResult<Option<ShellChange>> {
    if !touches_shell(&event.event) {
        return Ok(None);
    }
    let seq = event.sequence;
    if let DomainEvent::ProjectUpdated(project) = &event.event {
        return Ok(Some(ShellChange::Project {
            seq,
            row: project.clone(),
        }));
    }
    let Some(thread_id) = event.thread_id.or_else(|| event.event.payload_thread_id()) else {
        return Ok(None);
    };
    Ok(Some(match thread_row(connection, thread_id)? {
        Some(row) if row.thread.deleted_at.is_none() => ShellChange::Thread { seq, row },
        _ => ShellChange::ThreadGone { seq, thread_id },
    }))
}

/// One subscription's view of the shell: which rows it holds, so a live
/// change becomes the right item or nothing.
pub struct ShellFilter {
    spec: ShellSpec,
    /// The subscription resumed from a cursor, so the client may hold rows
    /// this filter never saw: removals and moves out of the location are
    /// sent for every row, not only for rows sent here.
    resumed: bool,
    /// Rows sent, to skip a change that changed nothing visible.
    rows: HashMap<ThreadId, ThreadRow>,
    projects: HashMap<ProjectId, Project>,
}

impl ShellFilter {
    pub fn new(spec: ShellSpec) -> Self {
        Self {
            spec,
            resumed: false,
            rows: HashMap::new(),
            projects: HashMap::new(),
        }
    }

    /// A filter for a subscription that resumed from a cursor.
    pub fn resumed(spec: ShellSpec) -> Self {
        Self {
            resumed: true,
            ..Self::new(spec)
        }
    }

    fn location(&self) -> ShellLocation {
        match self.spec.location {
            ShellLocation::Archived => ShellLocation::Archived,
            _ => ShellLocation::Active,
        }
    }

    /// Records rows a snapshot sent.
    pub fn seed(&mut self, item: &ShellItem) {
        if let ShellItem::Snapshot {
            projects, threads, ..
        } = item
        {
            for project in projects {
                self.projects.insert(project.id, project.clone());
            }
            for row in threads {
                self.rows.insert(row.thread.id, row.clone());
            }
        }
    }

    /// The item `change` becomes for this subscription, if any.
    pub fn admit(&mut self, change: &ShellChange) -> Option<ShellItem> {
        match change {
            ShellChange::Project { seq, row } => {
                if self.spec.project_id.is_some_and(|id| id != row.id) {
                    return None;
                }
                if row.removed_at.is_some() {
                    let held = self.projects.remove(&row.id).is_some();
                    return (held || self.resumed).then_some(ShellItem::ProjectRemoved {
                        seq: *seq,
                        project_id: row.id,
                    });
                }
                if self.projects.get(&row.id) == Some(row) {
                    return None;
                }
                self.projects.insert(row.id, row.clone());
                Some(ShellItem::ProjectUpdated {
                    seq: *seq,
                    row: row.clone(),
                })
            }
            ShellChange::Thread { seq, row } => {
                if self
                    .spec
                    .project_id
                    .is_some_and(|id| id != row.thread.project_id)
                {
                    return None;
                }
                let location = row.location()?;
                let id = row.thread.id;
                if location == self.location() {
                    if self.rows.get(&id) == Some(row) {
                        return None;
                    }
                    self.rows.insert(id, row.clone());
                } else if self.rows.remove(&id).is_none() && !self.resumed {
                    // Neither held nor wanted here.
                    return None;
                }
                Some(ShellItem::ThreadUpdated {
                    seq: *seq,
                    row: row.clone(),
                    location,
                })
            }
            ShellChange::ThreadGone { seq, thread_id } => {
                let held = self.rows.remove(thread_id).is_some();
                (held || self.resumed).then_some(ShellItem::ThreadRemoved {
                    seq: *seq,
                    thread_id: *thread_id,
                })
            }
        }
    }
}

/// How a `shell` subscription with a cursor starts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShellResume {
    /// The changes touched since the cursor, oldest first, then
    /// `synchronized` at `head`.
    Changes {
        changes: Vec<ShellChange>,
        head: Sequence,
    },
    Snapshot,
}

/// Decides between compact deltas and a snapshot for a shell subscriber at
/// `after`: deltas when the gap is at most `max_events` events. Each touched
/// row is read once, at its newest state, under the sequence of its last
/// change.
pub fn plan_shell_resume(
    connection: &Connection,
    store_id: uuid::Uuid,
    after: &StreamCursor,
    max_events: usize,
) -> StoreResult<ShellResume> {
    let StreamCursor::Sequence { store, seq } = after else {
        return Ok(ShellResume::Snapshot);
    };
    if *store != store_id {
        return Ok(ShellResume::Snapshot);
    }
    let transaction = connection.unchecked_transaction()?;
    let connection = &*transaction;
    let head = max_sequence(connection)?;
    if *seq > head {
        return Ok(ShellResume::Snapshot);
    }
    let gap: i64 = connection
        .prepare_cached("SELECT COUNT(*) FROM (SELECT 1 FROM events WHERE sequence > ?1 LIMIT ?2)")?
        .query_row(params![*seq as i64, max_events as i64 + 1], |row| {
            row.get(0)
        })?;
    if gap as usize > max_events {
        return Ok(ShellResume::Snapshot);
    }
    let mut threads: BTreeMap<ThreadId, Sequence> = BTreeMap::new();
    let mut projects: BTreeMap<ProjectId, Sequence> = BTreeMap::new();
    {
        let mut statement = connection.prepare_cached(
            "SELECT sequence, event_type, project_id, thread_id FROM events
             WHERE sequence > ?1 AND event_type IN
               ('project.updated', 'thread.updated', 'run.updated', 'runtime_request.updated')",
        )?;
        let rows = statement.query_map([*seq as i64], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })?;
        for row in rows {
            let (sequence, event_type, project_id, thread_id) = row?;
            let sequence = sequence as Sequence;
            let parse =
                |text: Option<String>| text.and_then(|text| uuid::Uuid::parse_str(&text).ok());
            if event_type == "project.updated" {
                if let Some(id) = parse(project_id) {
                    projects.insert(ProjectId(id), sequence);
                }
            } else if let Some(id) = parse(thread_id) {
                threads.insert(ThreadId(id), sequence);
            }
        }
    }
    let mut changes = Vec::new();
    for (project_id, seq) in projects {
        if let Some(row) = project_row(connection, project_id)? {
            changes.push(ShellChange::Project { seq, row });
        }
    }
    for (thread_id, seq) in threads {
        changes.push(match thread_row(connection, thread_id)? {
            Some(row) if row.thread.deleted_at.is_none() => ShellChange::Thread { seq, row },
            _ => ShellChange::ThreadGone { seq, thread_id },
        });
    }
    changes.sort_by_key(|change| match change {
        ShellChange::Project { seq, .. }
        | ShellChange::Thread { seq, .. }
        | ShellChange::ThreadGone { seq, .. } => *seq,
    });
    Ok(ShellResume::Changes { changes, head })
}

/// [`plan_shell_resume`] with the protocol's gap limit.
pub fn plan_shell_resume_default(
    connection: &Connection,
    store_id: uuid::Uuid,
    after: &StreamCursor,
) -> StoreResult<ShellResume> {
    plan_shell_resume(connection, store_id, after, SHELL_REPLAY_EVENTS)
}

/// Why an archive page could not be served.
#[derive(Debug)]
pub enum ArchiveError {
    BadCursor(BadCursor),
    Store(StoreError),
}

impl From<StoreError> for ArchiveError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<rusqlite::Error> for ArchiveError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Store(error.into())
    }
}

/// Archived threads, most recently archived first, after `cursor`.
pub fn archived_page(
    connection: &Connection,
    cursor: Option<&str>,
    limit: Option<u32>,
    project_id: Option<ProjectId>,
) -> Result<ArchivedPage, ArchiveError> {
    let limit = limit
        .unwrap_or(MAX_ARCHIVED_PAGE)
        .clamp(1, MAX_ARCHIVED_PAGE) as usize;
    let (after_time, after_id) = match cursor {
        Some(cursor) => {
            let (time, id) = decode_archived(cursor).map_err(ArchiveError::BadCursor)?;
            (time, id.to_string())
        }
        None => (i64::MAX, String::new()),
    };
    let project = project_id.map(|id| id.to_string());
    let mut statement = connection.prepare_cached(&format!(
        "SELECT {ROW_COLUMNS}, archived_at FROM p_threads
         WHERE archived_at IS NOT NULL AND deleted_at IS NULL
           AND (?1 IS NULL OR project_id = ?1)
           AND (archived_at < ?2 OR (archived_at = ?2 AND thread_id > ?3))
         ORDER BY archived_at DESC, thread_id LIMIT ?4"
    ))?;
    let rows = statement
        .query_map(
            params![project, after_time, after_id, limit as i64 + 1],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;
    let more = rows.len() > limit;
    let mut page = Vec::new();
    let mut last = None;
    for (json, activity, pending, queued, archived_at) in rows.into_iter().take(limit) {
        let row = thread_row_from(&json, &activity, pending, queued)?;
        last = Some((archived_at, row.thread.id));
        page.push(row);
    }
    Ok(ArchivedPage {
        rows: page,
        next: more
            .then(|| last.map(|(time, id)| encode_archived(time, id)))
            .flatten(),
    })
}
