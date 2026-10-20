//! Projection maintenance for `waku doctor` (#42): verify and rebuild in
//! bounded pages.
//!
//! - [`catch_up`] applies log events past the projection's last sequence in
//!   pages of [`REBUILD_PAGE`], one transaction per page, so an interrupted
//!   rebuild resumes where it stopped.
//! - [`rebuild`] clears the projection tables and catches up from 0.
//! - [`verify`] rebuilds the log into a scratch in-memory database and
//!   compares every projection table row by row, then checks references
//!   (R19: tables declare no foreign keys) and that every effect has its
//!   events.

use rusqlite::types::Value;
use rusqlite::{Connection, TransactionBehavior, params};
use waku_protocol::orchestration::*;

use super::sql::apply_sql;
use crate::store::sink::set_projection_sequence;
use crate::store::{
    GRAPH_PROJECTION, GRAPH_SCHEMA_VERSION, Store, StoreError, StoreResult, apply_migrations,
};

/// Events per rebuild transaction (RFC 0001 "DDL: projection tables").
pub const REBUILD_PAGE: usize = 10_000;

/// Every projection table and its primary key, in apply order.
pub const PROJECTION_TABLES: &[(&str, &str)] = &[
    ("p_projects", "project_id"),
    ("p_threads", "thread_id"),
    ("p_runs", "run_id"),
    ("p_run_attempts", "attempt_id"),
    ("p_nodes", "node_id"),
    ("p_subagents", "subagent_id"),
    ("p_provider_sessions", "provider_session_id"),
    (
        "p_provider_session_bindings",
        "provider_session_id, thread_id",
    ),
    ("p_provider_threads", "provider_thread_id"),
    ("p_provider_turns", "provider_turn_id"),
    ("p_runtime_requests", "runtime_request_id"),
    ("p_messages", "message_id"),
    ("p_plans", "plan_id"),
    ("p_turn_items", "turn_item_id"),
    ("p_checkpoint_scopes", "scope_id"),
    ("p_checkpoints", "checkpoint_id"),
    ("p_context_transfers", "context_transfer_id"),
    ("p_context_handoffs", "context_handoff_id"),
];

/// The graph projection's stored version and sequence.
pub fn projection_state(connection: &Connection) -> StoreResult<Option<(i64, Sequence)>> {
    use rusqlite::OptionalExtension;
    Ok(connection
        .query_row(
            "SELECT schema_version, last_sequence FROM projection_metadata WHERE name = ?1",
            [GRAPH_PROJECTION],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)? as Sequence)),
        )
        .optional()?)
}

/// Applies one page of events after `after` from `source` to `target`.
/// Returns the last applied sequence and how many events the page held.
fn apply_page(
    source: &Connection,
    target: &Connection,
    after: Sequence,
    page: usize,
) -> StoreResult<(Sequence, usize)> {
    let mut statement = source.prepare_cached(
        "SELECT sequence, event_id, command_id, project_id, thread_id, occurred_at, event_type,
                payload_json
         FROM events WHERE sequence > ?1 ORDER BY sequence LIMIT ?2",
    )?;
    let rows = statement
        .query_map(params![after as i64, page as i64], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut last = after;
    let count = rows.len();
    for (sequence, event_id, command_id, project_id, thread_id, occurred_at, event_type, payload) in
        rows
    {
        let corrupt = |what: &str| StoreError::Corrupt(format!("event {sequence}: bad {what}"));
        let event = DomainEvent::from_parts(&event_type, &payload)
            .map_err(|error| StoreError::Corrupt(format!("event {sequence}: {error}")))?;
        let stored = StoredEvent {
            sequence: sequence as Sequence,
            event_id: event_id.parse().map_err(|_| corrupt("event_id"))?,
            command_id: command_id.and_then(|id| CommandId::new(id).ok()),
            project_id: project_id
                .map(|id| id.parse().map_err(|_| corrupt("project_id")))
                .transpose()?,
            thread_id: thread_id
                .map(|id| id.parse().map_err(|_| corrupt("thread_id")))
                .transpose()?,
            occurred_at,
            event,
        };
        apply_sql(target, &stored, &payload)?;
        last = stored.sequence;
    }
    Ok((last, count))
}

/// Applies every event past the projection's last sequence, one page per
/// transaction. Returns the projection's new last sequence.
pub fn catch_up(
    connection: &mut Connection,
    page: usize,
    mut progress: impl FnMut(Sequence),
) -> StoreResult<Sequence> {
    let mut last = projection_state(connection)?.map_or(0, |(_, sequence)| sequence);
    loop {
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (applied_to, count) = apply_page(&transaction, &transaction, last, page)?;
        if count > 0 {
            set_projection_sequence(&transaction, applied_to, 0)?;
        }
        transaction.commit()?;
        last = applied_to;
        progress(last);
        if count < page {
            return Ok(last);
        }
    }
}

/// Clears the projection tables and rebuilds them from the log.
pub fn rebuild(
    connection: &mut Connection,
    page: usize,
    progress: impl FnMut(Sequence),
) -> StoreResult<Sequence> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    for (table, _) in PROJECTION_TABLES {
        transaction.execute(&format!("DELETE FROM {table}"), [])?;
    }
    transaction.execute(
        "INSERT INTO projection_metadata (name, schema_version, last_sequence, updated_at)
         VALUES (?1, ?2, 0, 0)
         ON CONFLICT(name) DO UPDATE SET schema_version = excluded.schema_version,
                                         last_sequence = 0",
        params![GRAPH_PROJECTION, GRAPH_SCHEMA_VERSION],
    )?;
    transaction.commit()?;
    catch_up(connection, page, progress)
}

/// One row that differs between the live projection and a rebuild.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mismatch {
    pub table: &'static str,
    pub key: String,
    pub kind: MismatchKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MismatchKind {
    /// In the live projection only: ahead of (or not derivable from) the log.
    LiveOnly,
    /// In the rebuild only: the live projection is behind the log.
    RebuildOnly,
    Different,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VerifyReport {
    pub log_sequence: Sequence,
    pub projection_sequence: Sequence,
    pub rows_checked: usize,
    pub mismatches: Vec<Mismatch>,
    /// `(table, id, what is missing)`.
    pub dangling_references: Vec<(String, String, String)>,
    /// Outbox rows whose command has no accepted receipt: an effect without
    /// its events.
    pub effects_without_events: Vec<EffectId>,
}

impl VerifyReport {
    pub fn is_clean(&self) -> bool {
        self.log_sequence == self.projection_sequence
            && self.mismatches.is_empty()
            && self.dangling_references.is_empty()
            && self.effects_without_events.is_empty()
    }
}

/// The most mismatches recorded per table.
const MAX_MISMATCHES_PER_TABLE: usize = 64;

fn table_rows(
    connection: &Connection,
    table: &str,
    key: &str,
) -> StoreResult<Vec<(String, Vec<Value>)>> {
    let key_columns = key.split(',').map(str::trim).collect::<Vec<_>>();
    let mut statement = connection.prepare(&format!("SELECT * FROM {table} ORDER BY {key}"))?;
    let names = statement
        .column_names()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let key_indexes = key_columns
        .iter()
        .filter_map(|column| names.iter().position(|name| name == column))
        .collect::<Vec<_>>();
    let width = names.len();
    let rows = statement
        .query_map([], |row| {
            (0..width)
                .map(|index| row.get::<_, Value>(index))
                .collect::<Result<Vec<_>, _>>()
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows
        .into_iter()
        .map(|values| {
            let key = key_indexes
                .iter()
                .map(|index| match &values[*index] {
                    Value::Text(text) => text.clone(),
                    other => format!("{other:?}"),
                })
                .collect::<Vec<_>>()
                .join("/");
            (key, values)
        })
        .collect())
}

fn compare_table(
    live: &Connection,
    rebuilt: &Connection,
    table: &'static str,
    key: &str,
    report: &mut VerifyReport,
) -> StoreResult<()> {
    let live_rows = table_rows(live, table, key)?;
    let rebuilt_rows = table_rows(rebuilt, table, key)?;
    report.rows_checked += live_rows.len();
    let mut recorded = 0;
    let mut record = |report: &mut VerifyReport, key: &str, kind| {
        if recorded < MAX_MISMATCHES_PER_TABLE {
            report.mismatches.push(Mismatch {
                table,
                key: key.to_owned(),
                kind,
            });
        }
        recorded += 1;
    };
    let (mut left, mut right) = (live_rows.iter().peekable(), rebuilt_rows.iter().peekable());
    loop {
        match (left.peek(), right.peek()) {
            (None, None) => break,
            (Some((key, _)), None) => {
                record(report, key, MismatchKind::LiveOnly);
                left.next();
            }
            (None, Some((key, _))) => {
                record(report, key, MismatchKind::RebuildOnly);
                right.next();
            }
            (Some((live_key, live_values)), Some((rebuilt_key, rebuilt_values))) => {
                match live_key.cmp(rebuilt_key) {
                    std::cmp::Ordering::Less => {
                        record(report, live_key, MismatchKind::LiveOnly);
                        left.next();
                    }
                    std::cmp::Ordering::Greater => {
                        record(report, rebuilt_key, MismatchKind::RebuildOnly);
                        right.next();
                    }
                    std::cmp::Ordering::Equal => {
                        if live_values != rebuilt_values {
                            record(report, live_key, MismatchKind::Different);
                        }
                        left.next();
                        right.next();
                    }
                }
            }
        }
    }
    Ok(())
}

/// Rows that point at a missing parent (R19).
const REFERENCE_CHECKS: &[(&str, &str)] = &[
    (
        "p_threads.project_id",
        "SELECT thread_id, project_id FROM p_threads
         WHERE project_id NOT IN (SELECT project_id FROM p_projects)",
    ),
    (
        "p_runs.thread_id",
        "SELECT run_id, thread_id FROM p_runs WHERE thread_id NOT IN (SELECT thread_id FROM p_threads)",
    ),
    (
        "p_run_attempts.run_id",
        "SELECT attempt_id, run_id FROM p_run_attempts WHERE run_id NOT IN (SELECT run_id FROM p_runs)",
    ),
    (
        "p_nodes.run_id",
        "SELECT node_id, run_id FROM p_nodes
         WHERE run_id IS NOT NULL AND run_id NOT IN (SELECT run_id FROM p_runs)",
    ),
    (
        "p_messages.thread_id",
        "SELECT message_id, thread_id FROM p_messages
         WHERE thread_id NOT IN (SELECT thread_id FROM p_threads)",
    ),
    (
        "p_turn_items.thread_id",
        "SELECT turn_item_id, thread_id FROM p_turn_items
         WHERE thread_id NOT IN (SELECT thread_id FROM p_threads)",
    ),
    (
        "p_turn_items.run_id",
        "SELECT turn_item_id, run_id FROM p_turn_items
         WHERE run_id IS NOT NULL AND run_id NOT IN (SELECT run_id FROM p_runs)",
    ),
    (
        "p_runtime_requests.node_id",
        "SELECT runtime_request_id, node_id FROM p_runtime_requests
         WHERE node_id NOT IN (SELECT node_id FROM p_nodes)",
    ),
    (
        "p_checkpoints.scope_id",
        "SELECT checkpoint_id, scope_id FROM p_checkpoints
         WHERE scope_id NOT IN (SELECT scope_id FROM p_checkpoint_scopes)",
    ),
];

/// Verifies the live projection against a rebuild of the log. Reads one
/// consistent snapshot through a read-only connection.
pub fn verify(store: &Store) -> StoreResult<VerifyReport> {
    store.reader().with(|live| {
        let snapshot = live.unchecked_transaction()?;
        let mut scratch = Connection::open_in_memory()?;
        apply_migrations(&mut scratch)?;
        let mut report = VerifyReport {
            log_sequence: crate::store::max_sequence(&snapshot)?,
            projection_sequence: projection_state(&snapshot)?.map_or(0, |(_, sequence)| sequence),
            ..VerifyReport::default()
        };
        {
            let target = scratch.transaction()?;
            let mut after = 0;
            loop {
                let (last, count) = apply_page(&snapshot, &target, after, REBUILD_PAGE)?;
                after = last;
                if count < REBUILD_PAGE {
                    break;
                }
            }
            target.commit()?;
        }
        for (table, key) in PROJECTION_TABLES {
            compare_table(&snapshot, &scratch, table, key, &mut report)?;
        }
        for (what, sql) in REFERENCE_CHECKS {
            let mut statement = snapshot.prepare(sql)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            report.dangling_references.extend(
                rows.into_iter()
                    .map(|(id, missing)| ((*what).to_owned(), id, missing)),
            );
        }
        // The derived thread columns are maintained by deltas; recount them.
        let derived = format!(
            "SELECT t.thread_id, t.activity FROM p_threads AS t
             WHERE t.queued_run_count <> (SELECT COUNT(*) FROM p_runs AS r
                                          WHERE r.thread_id = t.thread_id AND r.status = 'queued')
                OR t.pending_request_count <> (SELECT COUNT(*) FROM p_runtime_requests AS q
                                               WHERE q.thread_id = t.thread_id
                                                 AND q.status = 'pending')
                OR t.activity <> ({})",
            super::sql::ACTIVITY
                .replace("?1", "t.thread_id")
                .replace("pending_request_count", "t.pending_request_count")
        );
        let mut statement = snapshot.prepare(&derived)?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        report.dangling_references.extend(
            rows.into_iter()
                .map(|(id, activity)| ("p_threads.derived".to_owned(), id, activity)),
        );
        let mut statement = snapshot.prepare(
            "SELECT o.effect_id FROM effect_outbox AS o
             LEFT JOIN command_receipts AS r
               ON r.command_id = o.command_id AND r.status = 'accepted'
             WHERE o.command_id IS NOT NULL AND r.command_id IS NULL
             ORDER BY o.effect_id",
        )?;
        report.effects_without_events = statement
            .query_map([], |row| row.get::<_, i64>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(report)
    })
}
