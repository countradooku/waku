//! `command_receipts`: idempotent dispatch (RFC 0001 rule 2, R16).

use rusqlite::{Connection, OptionalExtension, params};
use waku_protocol::orchestration::{CommandId, Sequence, Timestamp};

use super::{StoreResult, decode};
use crate::command::CommandError;

/// A stored command outcome. Re-dispatching its command id returns it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    pub command_type: String,
    pub result_sequence: Sequence,
    /// `Some` for a rejected command (R16).
    pub error: Option<CommandError>,
}

pub fn lookup(connection: &Connection, command_id: &CommandId) -> StoreResult<Option<Receipt>> {
    let row = connection
        .prepare_cached(
            "SELECT command_type, result_sequence, error_json FROM command_receipts
             WHERE command_id = ?1",
        )?
        .query_row([command_id.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .optional()?;
    row.map(|(command_type, sequence, error_json)| {
        Ok(Receipt {
            command_type,
            result_sequence: sequence as Sequence,
            error: error_json
                .map(|json| decode::<CommandError>("command_receipts.error_json", &json))
                .transpose()?,
        })
    })
    .transpose()
}

pub(crate) struct NewReceipt<'a> {
    pub command_id: &'a CommandId,
    pub command_type: &'a str,
    pub project_id: Option<String>,
    pub thread_id: Option<String>,
    pub result_sequence: Sequence,
    pub error: Option<&'a CommandError>,
    pub accepted_at: Timestamp,
}

pub(crate) fn insert(connection: &Connection, receipt: NewReceipt<'_>) -> StoreResult<()> {
    let error_json = receipt.error.map(serde_json::to_string).transpose()?;
    connection
        .prepare_cached(
            "INSERT INTO command_receipts
               (command_id, command_type, project_id, thread_id, status, result_sequence,
                error_json, accepted_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )?
        .execute(params![
            receipt.command_id.as_str(),
            receipt.command_type,
            receipt.project_id,
            receipt.thread_id,
            if receipt.error.is_some() {
                "rejected"
            } else {
                "accepted"
            },
            receipt.result_sequence as i64,
            error_json,
            receipt.accepted_at,
        ])?;
    Ok(())
}

/// Deletes receipts accepted before `before` (R3: 30 days), at most `limit`.
pub fn prune(connection: &Connection, before: Timestamp, limit: usize) -> StoreResult<usize> {
    Ok(connection
        .prepare_cached(
            "DELETE FROM command_receipts WHERE command_id IN (
               SELECT command_id FROM command_receipts WHERE accepted_at < ?1 LIMIT ?2)",
        )?
        .execute(params![before, limit as i64])?)
}
