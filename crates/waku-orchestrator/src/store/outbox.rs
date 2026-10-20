//! `effect_outbox`: lease-based claims, settles, cancellation, process-loss
//! reconcile and pruning (#41, RFC 0001 "Effects").

use rusqlite::{Connection, OptionalExtension, Row, params};
use waku_protocol::orchestration::*;

use super::{StoreError, StoreResult};
use crate::effects::{Effect, EffectType};

/// Outbox row states.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EffectStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl EffectStatus {
    pub fn parse(text: &str) -> Option<Self> {
        Some(match text {
            "pending" => Self::Pending,
            "running" => Self::Running,
            "succeeded" => Self::Succeeded,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            _ => return None,
        })
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

/// A row as a worker holds it after a claim.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxRow {
    pub effect_id: EffectId,
    pub command_id: Option<CommandId>,
    pub thread_id: Option<ThreadId>,
    pub effect_type: String,
    pub attempt_count: u32,
    /// `Err` holds the payload when it does not decode (a newer effect).
    pub effect: Result<Effect, String>,
}

fn row_to_outbox(
    row: &Row<'_>,
) -> rusqlite::Result<(i64, Option<String>, Option<String>, String, u32, String)> {
    Ok((
        row.get("effect_id")?,
        row.get("command_id")?,
        row.get("thread_id")?,
        row.get("effect_type")?,
        row.get("attempt_count")?,
        row.get("payload_json")?,
    ))
}

fn into_outbox(
    (effect_id, command_id, thread_id, effect_type, attempt_count, payload): (
        i64,
        Option<String>,
        Option<String>,
        String,
        u32,
        String,
    ),
) -> StoreResult<OutboxRow> {
    Ok(OutboxRow {
        effect_id,
        command_id: command_id.and_then(|id| CommandId::new(id).ok()),
        thread_id: thread_id
            .map(|id| {
                id.parse::<ThreadId>().map_err(|error| {
                    StoreError::Corrupt(format!("effect_outbox.thread_id {id}: {error}"))
                })
            })
            .transpose()?,
        effect_type,
        attempt_count,
        effect: serde_json::from_str(&payload).map_err(|_| payload),
    })
}

/// Claims the next runnable effect: the oldest pending row whose lane of its
/// thread has no running row and no earlier pending row. An earlier effect
/// waiting out its backoff therefore still blocks later ones (T3 semantics).
pub fn claim(
    connection: &Connection,
    worker: &str,
    now: Timestamp,
    lease_ms: i64,
) -> StoreResult<Option<OutboxRow>> {
    let row = connection
        .prepare_cached(
            "UPDATE effect_outbox
             SET status = 'running', lease_owner = ?1, lease_expires_at = ?2 + ?3,
                 attempt_count = attempt_count + 1, updated_at = ?2
             WHERE effect_id = (
               SELECT c.effect_id FROM effect_outbox AS c
               WHERE c.status = 'pending' AND c.available_at <= ?2
                 AND NOT EXISTS (
                   SELECT 1 FROM effect_outbox AS a
                   WHERE a.lane_key = c.lane_key AND a.lane = c.lane
                     AND (a.status = 'running'
                          OR (a.status = 'pending' AND a.effect_id < c.effect_id)))
               ORDER BY c.effect_id LIMIT 1)
             RETURNING effect_id, command_id, thread_id, effect_type, attempt_count, payload_json",
        )?
        .query_row(params![worker, now, lease_ms], row_to_outbox)
        .optional()?;
    row.map(into_outbox).transpose()
}

/// The row's current status, re-read after a claim to close the race with a
/// concurrent cancel.
pub fn status(connection: &Connection, effect_id: EffectId) -> StoreResult<Option<EffectStatus>> {
    Ok(connection
        .prepare_cached("SELECT status FROM effect_outbox WHERE effect_id = ?1")?
        .query_row([effect_id], |row| row.get::<_, String>(0))
        .optional()?
        .and_then(|status| EffectStatus::parse(&status)))
}

/// How a worker settles a claimed effect.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Settle {
    Succeeded,
    Retry {
        available_at: Timestamp,
        error: String,
    },
    Failed {
        error: String,
    },
    Cancelled {
        reason: String,
    },
}

/// Settles a running effect, only while `worker` still holds the lease.
/// Returns whether the row was updated.
pub fn settle(
    connection: &Connection,
    effect_id: EffectId,
    worker: &str,
    outcome: &Settle,
    now: Timestamp,
) -> StoreResult<bool> {
    let changed = match outcome {
        Settle::Succeeded => connection
            .prepare_cached(
                "UPDATE effect_outbox SET status = 'succeeded', lease_owner = NULL,
                   lease_expires_at = NULL, settled_at = ?3, updated_at = ?3
                 WHERE effect_id = ?1 AND lease_owner = ?2 AND status = 'running'",
            )?
            .execute(params![effect_id, worker, now])?,
        Settle::Retry {
            available_at,
            error,
        } => connection
            .prepare_cached(
                "UPDATE effect_outbox SET status = 'pending', lease_owner = NULL,
                   lease_expires_at = NULL, available_at = ?3, last_error = ?4, updated_at = ?5
                 WHERE effect_id = ?1 AND lease_owner = ?2 AND status = 'running'",
            )?
            .execute(params![effect_id, worker, available_at, error, now])?,
        Settle::Failed { error } => connection
            .prepare_cached(
                "UPDATE effect_outbox SET status = 'failed', lease_owner = NULL,
                   lease_expires_at = NULL, last_error = ?3, settled_at = ?4, updated_at = ?4
                 WHERE effect_id = ?1 AND lease_owner = ?2 AND status = 'running'",
            )?
            .execute(params![effect_id, worker, error, now])?,
        Settle::Cancelled { reason } => connection
            .prepare_cached(
                "UPDATE effect_outbox SET status = 'cancelled', lease_owner = NULL,
                   lease_expires_at = NULL, cancel_reason = ?3, settled_at = ?4, updated_at = ?4
                 WHERE effect_id = ?1 AND lease_owner = ?2 AND status = 'running'",
            )?
            .execute(params![effect_id, worker, reason, now])?,
    };
    Ok(changed > 0)
}

/// Cancels pending and running rows of one type for a thread.
pub fn cancel_unsettled(
    connection: &Connection,
    thread_id: ThreadId,
    effect_type: EffectType,
    reason: &str,
    now: Timestamp,
) -> StoreResult<Vec<EffectId>> {
    let mut statement = connection.prepare_cached(
        "UPDATE effect_outbox SET status = 'cancelled', cancel_reason = ?3, settled_at = ?4,
           updated_at = ?4
         WHERE thread_id = ?1 AND effect_type = ?2 AND status IN ('pending', 'running')
         RETURNING effect_id",
    )?;
    let ids = statement
        .query_map(
            params![thread_id.to_string(), effect_type.as_str(), reason, now],
            |row| row.get::<_, i64>(0),
        )?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ids)
}

/// What `reconcile_after_process_loss` did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    pub cancelled: Vec<EffectId>,
    pub requeued: Vec<EffectId>,
}

/// After a restart: process-bound effects that were pending or running are
/// cancelled (their provider process is gone); running replay-safe effects
/// go back to pending.
pub fn reconcile_after_process_loss(
    connection: &Connection,
    now: Timestamp,
) -> StoreResult<ReconcileReport> {
    let cancelled = connection
        .prepare(
            "UPDATE effect_outbox SET status = 'cancelled', cancel_reason = 'process_loss',
               lease_owner = NULL, lease_expires_at = NULL, settled_at = ?1, updated_at = ?1
             WHERE process_bound = 1 AND status IN ('pending', 'running')
             RETURNING effect_id",
        )?
        .query_map([now], |row| row.get::<_, i64>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    let requeued = connection
        .prepare(
            "UPDATE effect_outbox SET status = 'pending', lease_owner = NULL,
               lease_expires_at = NULL, available_at = ?1, updated_at = ?1
             WHERE process_bound = 0 AND status = 'running'
             RETURNING effect_id",
        )?
        .query_map([now], |row| row.get::<_, i64>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    let mut report = ReconcileReport {
        cancelled,
        requeued,
    };
    report.cancelled.sort_unstable();
    report.requeued.sort_unstable();
    Ok(report)
}

/// The liveness poll: running rows whose lease expired go back to pending.
pub fn expire_leases(connection: &Connection, now: Timestamp) -> StoreResult<usize> {
    Ok(connection
        .prepare_cached(
            "UPDATE effect_outbox SET status = 'pending', lease_owner = NULL,
               lease_expires_at = NULL, updated_at = ?1
             WHERE status = 'running' AND lease_expires_at < ?1",
        )?
        .execute([now])?)
}

/// The earliest `available_at` of a pending row, to bound the worker sleep.
pub fn next_available(connection: &Connection) -> StoreResult<Option<Timestamp>> {
    Ok(connection
        .prepare_cached("SELECT MIN(available_at) FROM effect_outbox WHERE status = 'pending'")?
        .query_row([], |row| row.get::<_, Option<i64>>(0))?)
}

/// Deletes succeeded and cancelled rows settled before `before`, at most
/// `limit`. Failed rows are kept (R3).
pub fn prune(connection: &Connection, before: Timestamp, limit: usize) -> StoreResult<usize> {
    Ok(connection
        .prepare_cached(
            "DELETE FROM effect_outbox WHERE effect_id IN (
               SELECT effect_id FROM effect_outbox
               WHERE status IN ('succeeded', 'cancelled') AND settled_at < ?1
               ORDER BY settled_at LIMIT ?2)",
        )?
        .execute(params![before, limit as i64])?)
}

/// Rows with their status, for tests and `waku doctor`.
pub fn list(connection: &Connection) -> StoreResult<Vec<(EffectId, String, EffectStatus)>> {
    let mut statement = connection
        .prepare("SELECT effect_id, effect_type, status FROM effect_outbox ORDER BY effect_id")?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows
        .into_iter()
        .filter_map(|(id, kind, status)| Some((id, kind, EffectStatus::parse(&status)?)))
        .collect())
}
