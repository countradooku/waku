//! Search documents (#65, R24), maintained by `apply_sql` in the sink
//! transaction.
//!
//! Documents come from the rows that carry text and a transcript position:
//! user and assistant message turn items once they stop streaming, plan turn
//! items (a proposed plan once it stops streaming, a todo list on every
//! update), and the thread title. The FTS table is contentless; the text is
//! read back from `p_turn_items` and `p_threads` when a snippet is cut.

use rusqlite::{Connection, OptionalExtension, params};
use waku_protocol::orchestration::*;

use crate::store::StoreResult;

/// `search_docs.entity_kind`.
pub const KIND_MESSAGE: &str = "message";
pub const KIND_PLAN: &str = "plan";
pub const KIND_THREAD_TITLE: &str = "thread_title";

/// The token `search_fts.thread` holds for a thread: one word, so an
/// in-thread find is a two-doclist intersection.
pub fn thread_token(thread_id: ThreadId) -> String {
    format!("t{}", thread_id.0.simple())
}

/// FNV-1a, 64 bits: stable across builds, so a rebuild skips the same
/// unchanged bodies the live sink skipped.
pub(crate) fn body_hash(body: &str) -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in body.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash as i64
}

/// The searchable text of a turn item, with its entity kind and id, or
/// `None` for items that are not indexed (yet).
pub fn item_document(item: &TurnItem) -> Option<(&'static str, String, String)> {
    match &item.kind {
        TurnItemKind::UserMessage {
            message_id,
            text,
            display_text,
            ..
        } => Some((
            KIND_MESSAGE,
            message_id.to_string(),
            display_text.clone().unwrap_or_else(|| text.clone()),
        )),
        TurnItemKind::AssistantMessage {
            message_id,
            text,
            streaming,
        } => (!streaming).then(|| (KIND_MESSAGE, message_id.to_string(), text.clone())),
        TurnItemKind::ProposedPlan {
            plan_id,
            markdown,
            streaming,
        } => (!streaming).then(|| (KIND_PLAN, plan_id.to_string(), markdown.clone())),
        TurnItemKind::TodoList {
            plan_id,
            steps,
            explanation,
        } => {
            let mut body = explanation.clone().unwrap_or_default();
            for step in steps {
                if !body.is_empty() {
                    body.push('\n');
                }
                body.push_str(&step.text);
            }
            Some((KIND_PLAN, plan_id.to_string(), body))
        }
        _ => None,
    }
}

/// Indexes a turn item if it carries searchable text.
pub(crate) fn index_item(
    connection: &Connection,
    project_id: &str,
    item: &TurnItem,
) -> StoreResult<()> {
    if let Some((kind, entity_id, body)) = item_document(item) {
        upsert(
            connection,
            kind,
            &entity_id,
            item.thread_id,
            project_id,
            Some(item.position),
            &body,
            item.updated_at,
        )?;
    }
    Ok(())
}

/// Indexes a thread's title.
pub(crate) fn index_title(connection: &Connection, thread: &AppThread) -> StoreResult<()> {
    upsert(
        connection,
        KIND_THREAD_TITLE,
        &thread.id.to_string(),
        thread.id,
        &thread.project_id.to_string(),
        None,
        &thread.title,
        thread.updated_at,
    )
}

/// Writes one document: insert, replace a changed body, or nothing when the
/// body is unchanged. An empty body removes the document.
#[allow(clippy::too_many_arguments)]
fn upsert(
    connection: &Connection,
    kind: &str,
    entity_id: &str,
    thread_id: ThreadId,
    project_id: &str,
    position: Option<u64>,
    body: &str,
    updated_at: Timestamp,
) -> StoreResult<()> {
    let hash = body_hash(body);
    let existing = connection
        .prepare_cached("SELECT doc_id, body_hash FROM search_docs WHERE entity_id = ?1")?
        .query_row([entity_id], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
        })
        .optional()?;
    if body.trim().is_empty() {
        if let Some((doc_id, _)) = existing {
            connection
                .prepare_cached("DELETE FROM search_fts WHERE rowid = ?1")?
                .execute([doc_id])?;
            connection
                .prepare_cached("DELETE FROM search_docs WHERE doc_id = ?1")?
                .execute([doc_id])?;
        }
        return Ok(());
    }
    let doc_id = match existing {
        Some((_, stored)) if stored == hash => return Ok(()),
        Some((doc_id, _)) => {
            connection
                .prepare_cached("DELETE FROM search_fts WHERE rowid = ?1")?
                .execute([doc_id])?;
            connection
                .prepare_cached(
                    "UPDATE search_docs SET body_hash = ?2, position = ?3, updated_at = ?4
                     WHERE doc_id = ?1",
                )?
                .execute(params![
                    doc_id,
                    hash,
                    position.map(|p| p as i64),
                    updated_at
                ])?;
            doc_id
        }
        None => {
            connection
                .prepare_cached(
                    "INSERT INTO search_docs
                       (entity_kind, entity_id, thread_id, project_id, position, body_hash,
                        updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                )?
                .execute(params![
                    kind,
                    entity_id,
                    thread_id.to_string(),
                    project_id,
                    position.map(|p| p as i64),
                    hash,
                    updated_at,
                ])?;
            connection.last_insert_rowid()
        }
    };
    connection
        .prepare_cached("INSERT INTO search_fts (rowid, body, thread) VALUES (?1, ?2, ?3)")?
        .execute(params![doc_id, body, thread_token(thread_id)])?;
    Ok(())
}

/// Empties both search tables (a projection rebuild).
pub(crate) fn clear(connection: &Connection) -> StoreResult<()> {
    connection.execute("DELETE FROM search_docs", [])?;
    connection.execute(
        "INSERT INTO search_fts (search_fts) VALUES ('delete-all')",
        [],
    )?;
    Ok(())
}
