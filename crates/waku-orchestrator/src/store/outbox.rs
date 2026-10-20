//! `effect_outbox`: rows are enqueued and cancelled inside the sink's
//! transaction (#40).

use rusqlite::{Connection, params};
use waku_protocol::orchestration::*;

use super::StoreResult;
use crate::effects::EffectType;

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
