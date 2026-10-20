//! `apply_sql`: projection tables as upserts by entity id, plus the loaders
//! that read a [`ThreadProjection`] back.

use std::collections::{BTreeMap, HashMap};
use std::sync::LazyLock;

use rusqlite::{Connection, OptionalExtension, params};
use waku_protocol::orchestration::*;

use super::{ThreadProjection, search_index};
use crate::store::{StoreError, StoreResult, decode};

/// Run statuses after which a run's rows leave the hot sets; mirrors
/// `RunStatus::is_terminal`.
const TERMINAL_RUN_STATUSES: &str =
    "('completed', 'interrupted', 'failed', 'cancelled', 'rolled_back')";

/// `keep` columns are written on insert but left alone on conflict.
fn build_upsert(table: &str, conflict: &str, keep: &[&str], columns: &[&str]) -> String {
    let placeholders = (1..=columns.len())
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let updates = columns
        .iter()
        .filter(|column| !conflict.split(',').any(|key| key.trim() == **column))
        .filter(|column| !keep.contains(column))
        .map(|column| format!("{column} = excluded.{column}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO {table} ({}) VALUES ({placeholders}) ON CONFLICT({conflict}) DO UPDATE SET {updates}",
        columns.join(", ")
    )
}

/// An `INSERT … ON CONFLICT(pk) DO UPDATE` statement, built once.
macro_rules! upsert {
    ($table:literal, $conflict:literal, keep[$($keep:literal),* $(,)?], [$($column:literal),+ $(,)?]) => {{
        static SQL: LazyLock<String> =
            LazyLock::new(|| build_upsert($table, $conflict, &[$($keep),*], &[$($column),+]));
        SQL.as_str()
    }};
    ($table:literal, $conflict:literal, [$($column:literal),+ $(,)?]) => {
        upsert!($table, $conflict, keep[], [$($column),+])
    };
}

fn text<T: ToString>(value: Option<T>) -> Option<String> {
    value.map(|value| value.to_string())
}

fn required_thread(event: &StoredEvent) -> StoreResult<String> {
    event.thread_id.map(|id| id.to_string()).ok_or_else(|| {
        StoreError::Corrupt(format!("{} without a thread", event.event.event_type()))
    })
}

/// Applies one stored event to the projection tables. `payload_json` is the
/// event's stored payload (the entity's JSON), reused as the row payload.
pub fn apply_sql(
    connection: &Connection,
    event: &StoredEvent,
    payload_json: &str,
) -> StoreResult<()> {
    match &event.event {
        DomainEvent::Unknown => {}
        DomainEvent::ProjectUpdated(project) => {
            connection
                .prepare_cached(upsert!(
                    "p_projects",
                    "project_id",
                    [
                        "project_id",
                        "name",
                        "path",
                        "order_key",
                        "created_at",
                        "updated_at",
                        "removed_at",
                        "payload_json"
                    ]
                ))?
                .execute(params![
                    project.id.to_string(),
                    project.name,
                    project.path.to_string_lossy(),
                    project.order_key.as_str(),
                    project.created_at,
                    project.updated_at,
                    project.removed_at,
                    payload_json,
                ])?;
        }
        DomainEvent::ThreadUpdated(update) => {
            let thread = &update.thread;
            let id = thread.id.to_string();
            let thread_json = serde_json::to_string(thread)?;
            let existed = connection
                .prepare_cached("SELECT 1 FROM p_threads WHERE thread_id = ?1")?
                .query_row([&id], |_| Ok(()))
                .optional()?
                .is_some();
            connection
                .prepare_cached(upsert!(
                    "p_threads",
                    "thread_id",
                    keep["activity", "pending_request_count", "queued_run_count"],
                    [
                        "thread_id",
                        "project_id",
                        "title",
                        "provider_instance_id",
                        "active_provider_thread_id",
                        "parent_thread_id",
                        "root_thread_id",
                        "activity",
                        "pending_request_count",
                        "queued_run_count",
                        "created_at",
                        "updated_at",
                        "last_reply_at",
                        "archived_at",
                        "deleted_at",
                        "settled_at",
                        "snoozed_until",
                        "pinned_at",
                        "pin_order_key",
                        "active_order_key",
                        "last_visited_at",
                        "payload_json"
                    ]
                ))?
                .execute(params![
                    thread.id.to_string(),
                    thread.project_id.to_string(),
                    thread.title,
                    thread.model_selection.instance.as_str(),
                    text(thread.active_provider_thread_id),
                    text(thread.lineage.parent_thread_id),
                    thread.lineage.root_thread_id.to_string(),
                    "idle",
                    0,
                    0,
                    thread.created_at,
                    thread.updated_at,
                    thread.last_reply_at,
                    thread.archived_at,
                    thread.deleted_at,
                    thread.settled_at,
                    thread.snoozed_until,
                    thread.pinned_at,
                    thread.pin_order_key.as_ref().map(OrderKey::as_str),
                    thread.active_order_key.as_ref().map(OrderKey::as_str),
                    thread.last_visited_at,
                    thread_json,
                ])?;
            // Run and request rows may have arrived first (R19); count them
            // once. Afterwards their own upserts keep the columns current.
            if !existed {
                recount_thread_derived(connection, &id)?;
            }
            search_index::index_title(connection, thread)?;
        }
        DomainEvent::RunUpdated(run) => {
            let old = previous_status(
                connection,
                "SELECT status FROM p_runs WHERE run_id = ?1",
                &run.id.to_string(),
            )?;
            connection
                .prepare_cached(upsert!(
                    "p_runs",
                    "run_id",
                    [
                        "run_id",
                        "thread_id",
                        "ordinal",
                        "status",
                        "queue_position",
                        "queue_held",
                        "provider_thread_id",
                        "continuation_of_run_id",
                        "requested_at",
                        "started_at",
                        "completed_at",
                        "payload_json"
                    ]
                ))?
                .execute(params![
                    run.id.to_string(),
                    run.thread_id.to_string(),
                    run.ordinal,
                    run.status.as_str(),
                    run.queue_position,
                    run.queue_held,
                    text(run.provider_thread_id),
                    text(
                        run.continuation
                            .as_ref()
                            .map(|continuation| continuation.of_run_id)
                    ),
                    run.requested_at,
                    run.started_at,
                    run.completed_at,
                    payload_json,
                ])?;
            if old.as_deref() != Some(run.status.as_str()) {
                let queued = i64::from(run.status == RunStatus::Queued)
                    - i64::from(old.as_deref() == Some(RunStatus::Queued.as_str()));
                update_thread_derived(connection, &run.thread_id.to_string(), queued, 0)?;
            }
        }
        DomainEvent::RunAttemptUpdated(attempt) => {
            connection
                .prepare_cached(upsert!(
                    "p_run_attempts",
                    "attempt_id",
                    [
                        "attempt_id",
                        "thread_id",
                        "run_id",
                        "attempt_ordinal",
                        "root_node_id",
                        "provider_thread_id",
                        "provider_turn_id",
                        "status",
                        "payload_json"
                    ]
                ))?
                .execute(params![
                    attempt.id.to_string(),
                    required_thread(event)?,
                    attempt.run_id.to_string(),
                    attempt.attempt_ordinal,
                    attempt.root_node_id.to_string(),
                    attempt.provider_thread_id.to_string(),
                    text(attempt.provider_turn_id),
                    attempt.status.as_str(),
                    payload_json,
                ])?;
        }
        DomainEvent::NodeUpdated(node) => {
            connection
                .prepare_cached(upsert!(
                    "p_nodes",
                    "node_id",
                    [
                        "node_id",
                        "thread_id",
                        "run_id",
                        "parent_node_id",
                        "root_node_id",
                        "kind",
                        "status",
                        "provider_turn_id",
                        "runtime_request_id",
                        "started_at",
                        "completed_at",
                        "payload_json"
                    ]
                ))?
                .execute(params![
                    node.id.to_string(),
                    node.thread_id.to_string(),
                    text(node.run_id),
                    text(node.parent_node_id),
                    node.root_node_id.to_string(),
                    node.kind.as_str(),
                    node.status.as_str(),
                    text(node.provider_turn_id),
                    text(node.runtime_request_id),
                    node.started_at,
                    node.completed_at,
                    payload_json,
                ])?;
        }
        DomainEvent::SubagentUpdated(subagent) => {
            connection
                .prepare_cached(upsert!(
                    "p_subagents",
                    "subagent_id",
                    [
                        "subagent_id",
                        "thread_id",
                        "run_id",
                        "parent_node_id",
                        "child_thread_id",
                        "origin",
                        "status",
                        "driver",
                        "updated_at",
                        "payload_json"
                    ]
                ))?
                .execute(params![
                    subagent.id.to_string(),
                    subagent.thread_id.to_string(),
                    text(subagent.run_id),
                    subagent.parent_node_id.to_string(),
                    text(subagent.child_thread_id),
                    subagent.origin.as_str(),
                    subagent.status.as_str(),
                    subagent.driver.as_str(),
                    subagent.updated_at,
                    payload_json,
                ])?;
        }
        DomainEvent::ProviderSessionUpdated(session) => {
            connection
                .prepare_cached(upsert!(
                    "p_provider_sessions",
                    "provider_session_id",
                    [
                        "provider_session_id",
                        "driver",
                        "provider_instance_id",
                        "status",
                        "updated_at",
                        "payload_json"
                    ]
                ))?
                .execute(params![
                    session.id.to_string(),
                    session.driver.as_str(),
                    session.provider_instance_id.as_str(),
                    session.status.as_str(),
                    session.updated_at,
                    payload_json,
                ])?;
            // The session is bound to the thread whose command or ingest
            // reported it.
            if let Some(thread_id) = event.thread_id {
                connection
                    .prepare_cached(
                        "INSERT OR IGNORE INTO p_provider_session_bindings
                           (provider_session_id, thread_id) VALUES (?1, ?2)",
                    )?
                    .execute(params![session.id.to_string(), thread_id.to_string()])?;
            }
        }
        DomainEvent::ProviderThreadUpdated(provider_thread) => {
            connection
                .prepare_cached(upsert!(
                    "p_provider_threads",
                    "provider_thread_id",
                    [
                        "provider_thread_id",
                        "thread_id",
                        "owner_node_id",
                        "provider_session_id",
                        "driver",
                        "provider_instance_id",
                        "native_id",
                        "status",
                        "first_run_ordinal",
                        "last_run_ordinal",
                        "updated_at",
                        "payload_json"
                    ]
                ))?
                .execute(params![
                    provider_thread.id.to_string(),
                    text(provider_thread.app_thread_id),
                    text(provider_thread.owner_node_id),
                    text(provider_thread.provider_session_id),
                    provider_thread.driver.as_str(),
                    provider_thread.provider_instance_id.as_str(),
                    provider_thread
                        .native_thread_ref
                        .as_ref()
                        .and_then(|reference| reference.native_id.as_deref()),
                    provider_thread.status.as_str(),
                    provider_thread.first_run_ordinal,
                    provider_thread.last_run_ordinal,
                    provider_thread.updated_at,
                    payload_json,
                ])?;
        }
        DomainEvent::ProviderTurnUpdated(turn) => {
            connection
                .prepare_cached(upsert!(
                    "p_provider_turns",
                    "provider_turn_id",
                    [
                        "provider_turn_id",
                        "thread_id",
                        "provider_thread_id",
                        "node_id",
                        "run_attempt_id",
                        "ordinal",
                        "status",
                        "started_at",
                        "completed_at",
                        "payload_json"
                    ]
                ))?
                .execute(params![
                    turn.id.to_string(),
                    required_thread(event)?,
                    turn.provider_thread_id.to_string(),
                    turn.node_id.to_string(),
                    text(turn.run_attempt_id),
                    turn.ordinal,
                    turn.status.as_str(),
                    turn.started_at,
                    turn.completed_at,
                    payload_json,
                ])?;
        }
        DomainEvent::RuntimeRequestUpdated(request) => {
            let old = previous_status(
                connection,
                "SELECT status FROM p_runtime_requests WHERE runtime_request_id = ?1",
                &request.id.to_string(),
            )?;
            connection
                .prepare_cached(upsert!(
                    "p_runtime_requests",
                    "runtime_request_id",
                    [
                        "runtime_request_id",
                        "thread_id",
                        "node_id",
                        "provider_turn_id",
                        "kind",
                        "status",
                        "created_at",
                        "resolved_at",
                        "payload_json"
                    ]
                ))?
                .execute(params![
                    request.id.to_string(),
                    request.thread_id.to_string(),
                    request.node_id.to_string(),
                    text(request.provider_turn_id),
                    request.kind.as_str(),
                    request.status.as_str(),
                    request.created_at,
                    request.resolved_at,
                    payload_json,
                ])?;
            let pending = i64::from(request.status == RequestStatus::Pending)
                - i64::from(old.as_deref() == Some(RequestStatus::Pending.as_str()));
            if pending != 0 {
                update_thread_derived(connection, &request.thread_id.to_string(), 0, pending)?;
            }
        }
        DomainEvent::MessageUpdated(message) => {
            connection
                .prepare_cached(upsert!(
                    "p_messages",
                    "message_id",
                    [
                        "message_id",
                        "thread_id",
                        "run_id",
                        "node_id",
                        "role",
                        "streaming",
                        "created_at",
                        "updated_at",
                        "payload_json"
                    ]
                ))?
                .execute(params![
                    message.id.to_string(),
                    message.thread_id.to_string(),
                    text(message.run_id),
                    text(message.node_id),
                    message.role.as_str(),
                    message.streaming,
                    message.created_at,
                    message.updated_at,
                    payload_json,
                ])?;
        }
        DomainEvent::PlanUpdated(plan) => {
            connection
                .prepare_cached(upsert!(
                    "p_plans",
                    "plan_id",
                    [
                        "plan_id",
                        "thread_id",
                        "run_id",
                        "node_id",
                        "kind",
                        "status",
                        "payload_json"
                    ]
                ))?
                .execute(params![
                    plan.id.to_string(),
                    plan.thread_id.to_string(),
                    text(plan.run_id),
                    plan.node_id.to_string(),
                    plan.kind.type_name(),
                    plan.status.as_str(),
                    payload_json,
                ])?;
        }
        DomainEvent::TurnItemUpdated(item) => {
            connection
                .prepare_cached(upsert!(
                    "p_turn_items",
                    "turn_item_id",
                    [
                        "turn_item_id",
                        "thread_id",
                        "run_id",
                        "node_id",
                        "provider_turn_id",
                        "parent_item_id",
                        "position",
                        "item_type",
                        "status",
                        "payload_bytes",
                        "updated_at",
                        "payload_json"
                    ]
                ))?
                .execute(params![
                    item.id.to_string(),
                    item.thread_id.to_string(),
                    text(item.run_id),
                    text(item.node_id),
                    text(item.provider_turn_id),
                    text(item.parent_item_id),
                    item.position as i64,
                    item.kind.type_name(),
                    item.status.as_str(),
                    payload_json.len() as i64,
                    item.updated_at,
                    payload_json,
                ])?;
            if search_index::item_document(item).is_some() {
                let project_id = match event.project_id {
                    Some(project_id) => project_id.to_string(),
                    None => thread_project(connection, &item.thread_id.to_string())?,
                };
                search_index::index_item(connection, &project_id, item)?;
            }
        }
        DomainEvent::CheckpointScopeUpdated(scope) => {
            connection
                .prepare_cached(upsert!(
                    "p_checkpoint_scopes",
                    "scope_id",
                    [
                        "scope_id",
                        "thread_id",
                        "run_id",
                        "node_id",
                        "parent_scope_id",
                        "kind",
                        "ordinal_within_parent",
                        "advances_app_run_count",
                        "created_at",
                        "payload_json"
                    ]
                ))?
                .execute(params![
                    scope.id.to_string(),
                    scope.thread_id.to_string(),
                    text(scope.run_id),
                    scope.node_id.to_string(),
                    text(scope.parent_scope_id),
                    scope.kind.as_str(),
                    scope.ordinal_within_parent,
                    scope.advances_app_run_count,
                    scope.created_at,
                    payload_json,
                ])?;
        }
        DomainEvent::CheckpointUpdated(checkpoint) => {
            connection
                .prepare_cached(upsert!(
                    "p_checkpoints",
                    "checkpoint_id",
                    [
                        "checkpoint_id",
                        "thread_id",
                        "scope_id",
                        "run_id",
                        "app_run_ordinal",
                        "ordinal_within_scope",
                        "status",
                        "git_ref",
                        "captured_at",
                        "payload_json"
                    ]
                ))?
                .execute(params![
                    checkpoint.id.to_string(),
                    checkpoint.thread_id.to_string(),
                    checkpoint.scope_id.to_string(),
                    text(checkpoint.run_id),
                    checkpoint.app_run_ordinal,
                    checkpoint.ordinal_within_scope,
                    checkpoint.status.as_str(),
                    checkpoint.git_ref,
                    checkpoint.captured_at,
                    payload_json,
                ])?;
        }
        DomainEvent::ContextTransferUpdated(transfer) => {
            connection
                .prepare_cached(upsert!(
                    "p_context_transfers",
                    "context_transfer_id",
                    [
                        "context_transfer_id",
                        "source_thread_id",
                        "target_thread_id",
                        "target_run_id",
                        "kind",
                        "status",
                        "updated_at",
                        "payload_json"
                    ]
                ))?
                .execute(params![
                    transfer.id.to_string(),
                    transfer.source_thread_id.to_string(),
                    transfer.target_thread_id.to_string(),
                    text(transfer.target_run_id),
                    transfer.kind.as_str(),
                    transfer.status.as_str(),
                    transfer.updated_at,
                    payload_json,
                ])?;
        }
        DomainEvent::ContextHandoffUpdated(handoff) => {
            connection
                .prepare_cached(upsert!(
                    "p_context_handoffs",
                    "context_handoff_id",
                    [
                        "context_handoff_id",
                        "thread_id",
                        "target_run_id",
                        "to_provider_thread_id",
                        "strategy",
                        "status",
                        "updated_at",
                        "payload_json"
                    ]
                ))?
                .execute(params![
                    handoff.id.to_string(),
                    handoff.thread_id.to_string(),
                    handoff.target_run_id.to_string(),
                    handoff.to_provider_thread_id.to_string(),
                    handoff.strategy.as_str(),
                    handoff.status.as_str(),
                    handoff.updated_at,
                    payload_json,
                ])?;
        }
    }
    Ok(())
}

/// The project of a thread row, for events recorded without one.
fn thread_project(connection: &Connection, thread_id: &str) -> StoreResult<String> {
    connection
        .prepare_cached("SELECT project_id FROM p_threads WHERE thread_id = ?1")?
        .query_row([thread_id], |row| row.get::<_, String>(0))
        .optional()?
        .ok_or_else(|| StoreError::Corrupt(format!("turn item of unknown thread {thread_id}")))
}

fn previous_status(connection: &Connection, sql: &str, id: &str) -> StoreResult<Option<String>> {
    Ok(connection
        .prepare_cached(sql)?
        .query_row([id], |row| row.get::<_, String>(0))
        .optional()?)
}

/// The derived `activity` of a thread; `?1` is the thread id and
/// `pending_request_count` must already hold the new value. Every branch is
/// an index lookup (`p_runs_thread_status`, `p_runs_ordinal`).
pub(crate) const ACTIVITY: &str = "CASE
     WHEN pending_request_count > 0
       OR EXISTS (SELECT 1 FROM p_runs WHERE thread_id = ?1 AND status = 'waiting')
       THEN 'waiting'
     WHEN EXISTS (SELECT 1 FROM p_runs WHERE thread_id = ?1 AND status = 'finalizing')
       THEN 'finalizing'
     WHEN EXISTS (SELECT 1 FROM p_runs WHERE thread_id = ?1 AND status IN ('starting', 'running'))
       THEN 'working'
     WHEN EXISTS (SELECT 1 FROM p_runs WHERE thread_id = ?1 AND status = 'preparing')
       THEN 'preparing'
     WHEN (SELECT status FROM p_runs WHERE thread_id = ?1 AND ordinal IS NOT NULL
           ORDER BY ordinal DESC LIMIT 1) = 'failed'
       THEN 'failed'
     ELSE 'idle'
   END";

/// Counts a thread's queued runs and pending requests from scratch (when the
/// thread row is first inserted), then derives its activity.
fn recount_thread_derived(connection: &Connection, thread_id: &str) -> StoreResult<()> {
    static COUNT: LazyLock<String> = LazyLock::new(|| {
        "UPDATE p_threads SET
           queued_run_count = (SELECT COUNT(*) FROM p_runs
                               WHERE thread_id = ?1 AND status = 'queued'),
           pending_request_count = (SELECT COUNT(*) FROM p_runtime_requests
                                    WHERE thread_id = ?1 AND status = 'pending')
         WHERE thread_id = ?1"
            .to_owned()
    });
    static ACTIVITY_ONLY: LazyLock<String> = LazyLock::new(|| {
        format!("UPDATE p_threads SET activity = {ACTIVITY} WHERE thread_id = ?1")
    });
    connection.prepare_cached(&COUNT)?.execute([thread_id])?;
    connection
        .prepare_cached(&ACTIVITY_ONLY)?
        .execute([thread_id])?;
    Ok(())
}

/// Applies count deltas from one run or request transition and re-derives
/// the activity. Counting by delta keeps a commit O(log n) in the thread's
/// history; the deltas come from the row's actual previous status, so the
/// counts stay exact in any arrival order (`verify` rechecks them).
fn update_thread_derived(
    connection: &Connection,
    thread_id: &str,
    queued: i64,
    pending: i64,
) -> StoreResult<()> {
    static COUNTS: &str = "UPDATE p_threads SET queued_run_count = queued_run_count + ?2,
                             pending_request_count = pending_request_count + ?3
                           WHERE thread_id = ?1";
    static ACTIVITY_ONLY: LazyLock<String> = LazyLock::new(|| {
        format!("UPDATE p_threads SET activity = {ACTIVITY} WHERE thread_id = ?1")
    });
    if queued != 0 || pending != 0 {
        connection
            .prepare_cached(COUNTS)?
            .execute(params![thread_id, queued, pending])?;
    }
    connection
        .prepare_cached(&ACTIVITY_ONLY)?
        .execute([thread_id])?;
    Ok(())
}

// -- loading -----------------------------------------------------------------

fn load_rows<T: serde::de::DeserializeOwned>(
    connection: &Connection,
    sql: &str,
    key: &str,
    what: &str,
) -> StoreResult<Vec<T>> {
    let mut statement = connection.prepare_cached(sql)?;
    let rows = statement.query_map([key], |row| row.get::<_, String>(0))?;
    let mut values = Vec::new();
    for row in rows {
        values.push(decode(what, &row?)?);
    }
    Ok(values)
}

/// Loads one thread's projection, or `None` if the thread does not exist.
pub fn load_thread(
    connection: &Connection,
    thread_id: ThreadId,
) -> StoreResult<Option<ThreadProjection>> {
    let key = thread_id.to_string();
    let Some(thread_json) = connection
        .prepare_cached("SELECT payload_json FROM p_threads WHERE thread_id = ?1")?
        .query_row([&key], |row| row.get::<_, String>(0))
        .optional()?
    else {
        return Ok(None);
    };
    let mut projection = ThreadProjection::new(decode("p_threads", &thread_json)?);
    let runs: Vec<Run> = load_rows(
        connection,
        "SELECT payload_json FROM p_runs WHERE thread_id = ?1",
        &key,
        "p_runs",
    )?;
    projection.runs = runs
        .into_iter()
        .map(|run| (run.id, run))
        .collect::<BTreeMap<_, _>>();
    projection.reindex_runs();
    projection.attempts = load_rows::<RunAttempt>(
        connection,
        "SELECT payload_json FROM p_run_attempts WHERE thread_id = ?1",
        &key,
        "p_run_attempts",
    )?
    .into_iter()
    .map(|entity| (entity.id, entity))
    .collect();
    projection.nodes = load_rows::<ExecutionNode>(
        connection,
        "SELECT payload_json FROM p_nodes WHERE thread_id = ?1",
        &key,
        "p_nodes",
    )?
    .into_iter()
    .map(|entity| (entity.id, entity))
    .collect();
    projection.subagents = load_rows::<Subagent>(
        connection,
        "SELECT payload_json FROM p_subagents WHERE thread_id = ?1",
        &key,
        "p_subagents",
    )?
    .into_iter()
    .map(|entity| (entity.id, entity))
    .collect();
    projection.provider_sessions = load_rows::<ProviderSession>(
        connection,
        "SELECT s.payload_json FROM p_provider_session_bindings AS b
         JOIN p_provider_sessions AS s ON s.provider_session_id = b.provider_session_id
         WHERE b.thread_id = ?1",
        &key,
        "p_provider_sessions",
    )?
    .into_iter()
    .map(|entity| (entity.id, entity))
    .collect();
    projection.provider_threads = load_rows::<ProviderThread>(
        connection,
        "SELECT payload_json FROM p_provider_threads WHERE thread_id = ?1",
        &key,
        "p_provider_threads",
    )?
    .into_iter()
    .map(|entity| (entity.id, entity))
    .collect();
    projection.provider_turns = load_rows::<ProviderTurn>(
        connection,
        "SELECT payload_json FROM p_provider_turns WHERE thread_id = ?1",
        &key,
        "p_provider_turns",
    )?
    .into_iter()
    .map(|entity| (entity.id, entity))
    .collect();
    projection.runtime_requests = load_rows::<RuntimeRequest>(
        connection,
        "SELECT payload_json FROM p_runtime_requests WHERE thread_id = ?1",
        &key,
        "p_runtime_requests",
    )?
    .into_iter()
    .map(|entity| (entity.id, entity))
    .collect();
    projection.plans = load_rows::<Plan>(
        connection,
        "SELECT payload_json FROM p_plans WHERE thread_id = ?1",
        &key,
        "p_plans",
    )?
    .into_iter()
    .map(|entity| (entity.id, entity))
    .collect();
    projection.checkpoint_scopes = load_rows::<CheckpointScope>(
        connection,
        "SELECT payload_json FROM p_checkpoint_scopes WHERE thread_id = ?1",
        &key,
        "p_checkpoint_scopes",
    )?
    .into_iter()
    .map(|entity| (entity.id, entity))
    .collect();
    projection.checkpoints = load_rows::<Checkpoint>(
        connection,
        "SELECT payload_json FROM p_checkpoints WHERE thread_id = ?1",
        &key,
        "p_checkpoints",
    )?
    .into_iter()
    .map(|entity| (entity.id, entity))
    .collect();
    projection.context_transfers = load_rows::<ContextTransfer>(
        connection,
        "SELECT payload_json FROM p_context_transfers
         WHERE source_thread_id = ?1 OR target_thread_id = ?1",
        &key,
        "p_context_transfers",
    )?
    .into_iter()
    .map(|entity| (entity.id, entity))
    .collect();
    projection.context_handoffs = load_rows::<ContextHandoff>(
        connection,
        "SELECT payload_json FROM p_context_handoffs WHERE thread_id = ?1",
        &key,
        "p_context_handoffs",
    )?
    .into_iter()
    .map(|entity| (entity.id, entity))
    .collect();

    static HOT_MESSAGES: LazyLock<String> = LazyLock::new(|| {
        format!(
            "SELECT m.payload_json FROM p_messages AS m
             LEFT JOIN p_runs AS r ON r.run_id = m.run_id AND r.thread_id = m.thread_id
             WHERE m.thread_id = ?1 AND m.run_id IS NOT NULL
               AND (r.run_id IS NULL OR r.status NOT IN {TERMINAL_RUN_STATUSES})"
        )
    });
    static HOT_ITEMS: LazyLock<String> = LazyLock::new(|| {
        format!(
            "SELECT i.payload_json FROM p_turn_items AS i
             LEFT JOIN p_runs AS r ON r.run_id = i.run_id AND r.thread_id = i.thread_id
             WHERE i.thread_id = ?1 AND i.run_id IS NOT NULL
               AND (r.run_id IS NULL OR r.status NOT IN {TERMINAL_RUN_STATUSES})"
        )
    });
    projection.hot_messages = load_rows::<Message>(connection, &HOT_MESSAGES, &key, "p_messages")?
        .into_iter()
        .map(|entity| (entity.id, entity))
        .collect();
    projection.hot_items = load_rows::<TurnItem>(connection, &HOT_ITEMS, &key, "p_turn_items")?
        .into_iter()
        .map(|item| (item.position, item))
        .collect();

    let mut statement = connection.prepare_cached(
        "SELECT position >> 24, MAX(position & 16777215) + 1 FROM p_turn_items
         WHERE thread_id = ?1 GROUP BY position >> 24",
    )?;
    projection.next_position = statement
        .query_map([&key], |row| {
            Ok((row.get::<_, u32>(0)?, row.get::<_, u32>(1)?))
        })?
        .collect::<Result<HashMap<_, _>, _>>()?;
    projection.last_sequence = connection
        .prepare_cached("SELECT COALESCE(MAX(sequence), 0) FROM events WHERE thread_id = ?1")?
        .query_row([&key], |row| row.get::<_, i64>(0))? as Sequence;
    Ok(Some(projection))
}

/// Every project, removed ones included.
pub fn load_projects(connection: &Connection) -> StoreResult<Vec<Project>> {
    let mut statement = connection.prepare("SELECT payload_json FROM p_projects")?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    let mut projects = Vec::new();
    for row in rows {
        projects.push(decode("p_projects", &row?)?);
    }
    Ok(projects)
}

/// One message by id, from any run (title generation reads old ones).
pub fn load_message(connection: &Connection, id: MessageId) -> StoreResult<Option<Message>> {
    connection
        .prepare_cached("SELECT payload_json FROM p_messages WHERE message_id = ?1")?
        .query_row([id.to_string()], |row| row.get::<_, String>(0))
        .optional()?
        .map(|json| decode("p_messages", &json))
        .transpose()
}

/// The first user message of a thread.
pub fn first_user_message(
    connection: &Connection,
    thread_id: ThreadId,
) -> StoreResult<Option<Message>> {
    connection
        .prepare_cached(
            "SELECT payload_json FROM p_messages WHERE thread_id = ?1 AND role = 'user'
             ORDER BY created_at, message_id LIMIT 1",
        )?
        .query_row([thread_id.to_string()], |row| row.get::<_, String>(0))
        .optional()?
        .map(|json| decode("p_messages", &json))
        .transpose()
}
