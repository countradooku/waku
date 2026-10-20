//! Thread reads for the `thread` stream and the `threads.*` methods (#54):
//! the bounded snapshot, history pages, one full turn item, and the resume
//! decision between a replay and a fresh snapshot.
//!
//! Every read runs in one read transaction, so a snapshot and its cursor are
//! one consistent state of the log. Lookups use the per-thread indexes
//! (`p_turn_items_position`, `p_turn_items_run`, `events_thread_seq`, …) and
//! never scan the history of other threads; a long thread costs a bounded
//! number of rows, not its length.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use rusqlite::{Connection, OptionalExtension, params};
use waku_protocol::orchestration::*;
use waku_protocol::v8::StreamCursor;
use waku_protocol::v8::orchestration::{
    HistoryPage, SNAPSHOT_BYTES, SNAPSHOT_ITEMS, SNAPSHOT_USER_TURNS, THREAD_REPLAY_BYTES,
    THREAD_REPLAY_EVENTS, ThreadProjection, ThreadSnapshot,
};

use super::cursor::{BadCursor, decode_history, encode_history};
use super::wire::{project_event, project_turn_item};
use crate::store::{StoreError, StoreResult, decode, max_sequence};

/// Run statuses whose rows are always part of a snapshot.
const OPEN_RUN_STATUSES: &str =
    "('preparing', 'queued', 'starting', 'running', 'waiting', 'finalizing')";

/// Bounds of a snapshot window or a history page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WindowBudget {
    pub user_turns: usize,
    pub items: usize,
    /// Encoded (JSON) bytes of the wire-projected items.
    pub bytes: usize,
}

impl Default for WindowBudget {
    fn default() -> Self {
        Self {
            user_turns: SNAPSHOT_USER_TURNS,
            items: SNAPSHOT_ITEMS,
            bytes: SNAPSHOT_BYTES,
        }
    }
}

/// Bounds of a resume replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplayBudget {
    pub events: usize,
    /// Both the raw persisted payloads (checked before decoding) and the
    /// encoded wire-projected events.
    pub bytes: usize,
}

impl Default for ReplayBudget {
    fn default() -> Self {
        Self {
            events: THREAD_REPLAY_EVENTS,
            bytes: THREAD_REPLAY_BYTES,
        }
    }
}

/// The JSON size of `value`: the larger of the two encodings, so a budget
/// in it holds for MessagePack too.
pub fn encoded_len<T: serde::Serialize>(value: &T) -> usize {
    serde_json::to_vec(value).map_or(0, |bytes| bytes.len())
}

fn load_one<T: serde::de::DeserializeOwned>(
    connection: &Connection,
    sql: &str,
    key: &str,
    what: &str,
) -> StoreResult<Option<T>> {
    connection
        .prepare_cached(sql)?
        .query_row([key], |row| row.get::<_, String>(0))
        .optional()?
        .map(|json| decode(what, &json))
        .transpose()
}

fn load_all<T: serde::de::DeserializeOwned>(
    connection: &Connection,
    sql: &str,
    keys: &[&dyn rusqlite::ToSql],
    what: &str,
) -> StoreResult<Vec<T>> {
    let mut statement = connection.prepare_cached(sql)?;
    let rows = statement.query_map(keys, |row| row.get::<_, String>(0))?;
    let mut values = Vec::new();
    for row in rows {
        values.push(decode(what, &row?)?);
    }
    Ok(values)
}

/// One window of turn items, newest first while selected.
struct Window {
    items: Vec<TurnItem>,
    /// Older items exist.
    more: bool,
    budget_exceeded: bool,
}

/// Selects the newest items of `thread_id` before `before` (exclusive) within
/// `budget`. At least one item is admitted when any exists.
fn select_window(
    connection: &Connection,
    thread_id: &str,
    before: Option<u64>,
    budget: WindowBudget,
) -> StoreResult<Window> {
    let mut statement = connection.prepare_cached(
        "SELECT item_type, payload_json FROM p_turn_items
         WHERE thread_id = ?1 AND position < ?2 ORDER BY position DESC",
    )?;
    // Positions are below 2^53 (R15), so `i64::MAX` means "no bound".
    let before = before.map_or(i64::MAX, |position| position as i64);
    let mut rows = statement.query(params![thread_id, before])?;
    let mut window = Window {
        items: Vec::new(),
        more: false,
        budget_exceeded: false,
    };
    let mut user_turns = 0;
    let mut bytes = 0;
    while let Some(row) = rows.next()? {
        // Rows older than the oldest admitted user message belong to an
        // earlier turn.
        if window.items.len() >= budget.items || user_turns >= budget.user_turns {
            window.more = true;
            break;
        }
        let item_type: String = row.get(0)?;
        let is_user_turn = item_type == "user_message";
        let item: TurnItem = decode("p_turn_items", &row.get::<_, String>(1)?)?;
        let projected = project_turn_item(&item);
        let size = encoded_len(&projected);
        if !window.items.is_empty() && bytes + size > budget.bytes {
            window.more = true;
            window.budget_exceeded = true;
            break;
        }
        if size > budget.bytes {
            window.budget_exceeded = true;
        }
        bytes += size;
        user_turns += usize::from(is_user_turn);
        window.items.push(projected);
    }
    Ok(window)
}

/// The rows of pending runtime requests and their turn items, which every
/// snapshot carries wherever they sit in the history.
fn required_controls(
    connection: &Connection,
    thread_id: &str,
) -> StoreResult<(Vec<RuntimeRequest>, Vec<ExecutionNode>, Vec<TurnItem>)> {
    let requests: Vec<RuntimeRequest> = load_all(
        connection,
        "SELECT payload_json FROM p_runtime_requests WHERE thread_id = ?1 AND status = 'pending'",
        &[&thread_id],
        "p_runtime_requests",
    )?;
    let mut nodes = Vec::new();
    let mut items = Vec::new();
    for request in &requests {
        let Some(node) = load_one::<ExecutionNode>(
            connection,
            "SELECT payload_json FROM p_nodes WHERE node_id = ?1",
            &request.node_id.to_string(),
            "p_nodes",
        )?
        else {
            continue;
        };
        if let Some(run_id) = node.run_id {
            let run_items: Vec<TurnItem> = load_all(
                connection,
                "SELECT payload_json FROM p_turn_items WHERE run_id = ?1
                   AND item_type IN ('approval_request', 'user_input_request', 'secret_request')",
                &[&run_id.to_string()],
                "p_turn_items",
            )?;
            items.extend(run_items.into_iter().filter(|item| {
                item.node_id == Some(node.id)
                    || matches!(
                        &item.kind,
                        TurnItemKind::ApprovalRequest { request_id, .. }
                        | TurnItemKind::UserInputRequest { request_id, .. }
                            if *request_id == request.id
                    )
            }));
        }
        nodes.push(node);
    }
    Ok((requests, nodes, items))
}

fn sorted_by_id<T>(mut rows: Vec<T>, id: impl Fn(&T) -> String) -> Vec<T> {
    rows.sort_by_key(|row| id(row));
    rows
}

/// Loads `run_ids` (by primary key), sorted by id.
fn load_runs(connection: &Connection, run_ids: &BTreeSet<RunId>) -> StoreResult<Vec<Run>> {
    let mut runs = Vec::new();
    for run_id in run_ids {
        if let Some(run) = load_one::<Run>(
            connection,
            "SELECT payload_json FROM p_runs WHERE run_id = ?1",
            &run_id.to_string(),
            "p_runs",
        )? {
            runs.push(run);
        }
    }
    Ok(runs)
}

/// The control rows of `runs` that a client needs to render them.
fn run_controls(
    connection: &Connection,
    thread_id: &str,
    runs: &[Run],
    projection: &mut ThreadProjection,
) -> StoreResult<()> {
    let included: HashSet<RunId> = runs.iter().map(|run| run.id).collect();
    let mut attempts = Vec::new();
    let mut nodes = Vec::new();
    let mut plans = Vec::new();
    for run in runs {
        let key = run.id.to_string();
        attempts.extend(load_all::<RunAttempt>(
            connection,
            "SELECT payload_json FROM p_run_attempts WHERE thread_id = ?1 AND run_id = ?2",
            &[&thread_id, &key],
            "p_run_attempts",
        )?);
        // Nodes only for runs still moving: a finished run renders from its
        // turn items.
        if !run.status.is_terminal() {
            nodes.extend(load_all::<ExecutionNode>(
                connection,
                "SELECT payload_json FROM p_nodes WHERE thread_id = ?1 AND run_id = ?2",
                &[&thread_id, &key],
                "p_nodes",
            )?);
        }
        plans.extend(load_all::<Plan>(
            connection,
            "SELECT payload_json FROM p_plans WHERE run_id = ?1",
            &[&key],
            "p_plans",
        )?);
    }
    let mut turns = Vec::new();
    for turn_id in attempts
        .iter()
        .filter_map(|attempt| attempt.provider_turn_id)
    {
        if let Some(turn) = load_one::<ProviderTurn>(
            connection,
            "SELECT payload_json FROM p_provider_turns WHERE provider_turn_id = ?1",
            &turn_id.to_string(),
            "p_provider_turns",
        )? {
            turns.push(turn);
        }
    }
    // Scope and checkpoint rows are one per run: pick by run id first, then
    // decode only the included ones.
    let pick = |sql: &str| -> StoreResult<Vec<String>> {
        let mut statement = connection.prepare_cached(sql)?;
        let rows = statement.query_map([thread_id], |row| {
            Ok((row.get::<_, Option<String>>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut picked = Vec::new();
        for row in rows {
            let (run_id, id) = row?;
            if run_id
                .and_then(|run_id| run_id.parse().ok().map(RunId))
                .is_some_and(|run_id| included.contains(&run_id))
            {
                picked.push(id);
            }
        }
        Ok(picked)
    };
    for id in pick("SELECT run_id, scope_id FROM p_checkpoint_scopes WHERE thread_id = ?1")? {
        if let Some(scope) = load_one::<CheckpointScope>(
            connection,
            "SELECT payload_json FROM p_checkpoint_scopes WHERE scope_id = ?1",
            &id,
            "p_checkpoint_scopes",
        )? {
            projection.checkpoint_scopes.push(scope);
        }
    }
    for id in pick("SELECT run_id, checkpoint_id FROM p_checkpoints WHERE thread_id = ?1")? {
        if let Some(checkpoint) = load_one::<Checkpoint>(
            connection,
            "SELECT payload_json FROM p_checkpoints WHERE checkpoint_id = ?1",
            &id,
            "p_checkpoints",
        )? {
            projection.checkpoints.push(checkpoint);
        }
    }
    let subagents: Vec<Subagent> = {
        let mut statement = connection
            .prepare_cached("SELECT run_id, payload_json FROM p_subagents WHERE thread_id = ?1")?;
        let rows = statement.query_map([thread_id], |row| {
            Ok((row.get::<_, Option<String>>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut subagents = Vec::new();
        for row in rows {
            let (run_id, json) = row?;
            if run_id
                .and_then(|run_id| run_id.parse().ok().map(RunId))
                .is_some_and(|run_id| included.contains(&run_id))
            {
                subagents.push(decode("p_subagents", &json)?);
            }
        }
        subagents
    };
    let transfers: Vec<ContextTransfer> = load_all(
        connection,
        "SELECT payload_json FROM p_context_transfers
         WHERE source_thread_id = ?1 OR target_thread_id = ?1",
        &[&thread_id],
        "p_context_transfers",
    )?;
    let handoffs: Vec<ContextHandoff> = load_all(
        connection,
        "SELECT payload_json FROM p_context_handoffs WHERE thread_id = ?1",
        &[&thread_id],
        "p_context_handoffs",
    )?;
    projection.run_attempts = sorted_by_id(attempts, |row| row.id.to_string());
    projection.provider_turns = sorted_by_id(turns, |row| row.id.to_string());
    projection.plans = sorted_by_id(plans, |row| row.id.to_string());
    projection.subagents = sorted_by_id(subagents, |row| row.id.to_string());
    projection.context_transfers = sorted_by_id(
        transfers
            .into_iter()
            .filter(|row| row.target_run_id.is_none_or(|run| included.contains(&run)))
            .collect(),
        |row| row.id.to_string(),
    );
    projection.context_handoffs = sorted_by_id(
        handoffs
            .into_iter()
            .filter(|row| included.contains(&row.target_run_id))
            .collect(),
        |row| row.id.to_string(),
    );
    projection.checkpoint_scopes =
        sorted_by_id(std::mem::take(&mut projection.checkpoint_scopes), |row| {
            row.id.to_string()
        });
    projection.checkpoints = sorted_by_id(std::mem::take(&mut projection.checkpoints), |row| {
        row.id.to_string()
    });
    projection.nodes.extend(nodes);
    Ok(())
}

/// Options of a bounded snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SnapshotOptions {
    /// Leave out messages a turn item in the window already shows.
    pub compact: bool,
    pub budget: WindowBudget,
}

/// A bounded snapshot of `thread_id` (RFC 0003 §5.4), or `None` when the
/// thread does not exist. Runs in one read transaction; its cursor is the
/// log's head at that point.
pub fn thread_snapshot(
    connection: &Connection,
    store_id: uuid::Uuid,
    thread_id: ThreadId,
    options: SnapshotOptions,
) -> StoreResult<Option<ThreadSnapshot>> {
    let transaction = connection.unchecked_transaction()?;
    let connection = &*transaction;
    let key = thread_id.to_string();
    let head = max_sequence(connection)?;
    let Some(thread) = load_one::<AppThread>(
        connection,
        "SELECT payload_json FROM p_threads WHERE thread_id = ?1",
        &key,
        "p_threads",
    )?
    else {
        return Ok(None);
    };
    let mut projection = ThreadProjection::new(thread);
    let window = select_window(connection, &key, None, options.budget)?;
    let history = window
        .more
        .then(|| {
            window
                .items
                .last()
                .map(|oldest| encode_history(thread_id, oldest.position))
        })
        .flatten();

    let (requests, request_nodes, request_items) = required_controls(connection, &key)?;
    let mut items: BTreeMap<u64, TurnItem> = window
        .items
        .into_iter()
        .map(|item| (item.position, item))
        .collect();
    for item in request_items {
        items
            .entry(item.position)
            .or_insert_with(|| project_turn_item(&item));
    }

    // Runs in view, and every run still moving (queued ones included).
    let mut run_ids: BTreeSet<RunId> = items.values().filter_map(|item| item.run_id).collect();
    let open: Vec<Run> = load_all(
        connection,
        &format!(
            "SELECT payload_json FROM p_runs WHERE thread_id = ?1 AND status IN {OPEN_RUN_STATUSES}"
        ),
        &[&key],
        "p_runs",
    )?;
    run_ids.extend(open.iter().map(|run| run.id));
    run_ids.extend(request_nodes.iter().filter_map(|node| node.run_id));
    let runs = load_runs(connection, &run_ids)?;
    run_controls(connection, &key, &runs, &mut projection)?;

    // Nodes of pending requests even when their run is not moving.
    let mut seen_nodes: HashSet<NodeId> = projection.nodes.iter().map(|node| node.id).collect();
    for node in request_nodes {
        if seen_nodes.insert(node.id) {
            projection.nodes.push(node);
        }
    }
    projection.nodes = sorted_by_id(std::mem::take(&mut projection.nodes), |row| {
        row.id.to_string()
    });
    projection.runtime_requests = sorted_by_id(requests, |row| row.id.to_string());

    // Messages of runs still moving (queued messages have no item yet).
    let mut messages = Vec::new();
    for run in runs.iter().filter(|run| !run.status.is_terminal()) {
        messages.extend(load_all::<Message>(
            connection,
            "SELECT payload_json FROM p_messages WHERE run_id = ?1",
            &[&run.id.to_string()],
            "p_messages",
        )?);
    }
    if options.compact {
        let shown: HashSet<MessageId> = items
            .values()
            .filter_map(|item| match &item.kind {
                TurnItemKind::UserMessage { message_id, .. }
                | TurnItemKind::AssistantMessage { message_id, .. } => Some(*message_id),
                _ => None,
            })
            .collect();
        messages.retain(|message| !shown.contains(&message.id));
    }
    projection.messages = sorted_by_id(messages, |row| row.id.to_string());

    projection.provider_sessions = sorted_by_id(
        load_all(
            connection,
            "SELECT s.payload_json FROM p_provider_session_bindings AS b
             JOIN p_provider_sessions AS s ON s.provider_session_id = b.provider_session_id
             WHERE b.thread_id = ?1",
            &[&key],
            "p_provider_sessions",
        )?,
        |row: &ProviderSession| row.id.to_string(),
    );
    projection.provider_threads = sorted_by_id(
        load_all(
            connection,
            "SELECT payload_json FROM p_provider_threads WHERE thread_id = ?1",
            &[&key],
            "p_provider_threads",
        )?,
        |row: &ProviderThread| row.id.to_string(),
    );
    projection.runs = runs;
    projection.turn_items = items.into_values().collect();
    Ok(Some(ThreadSnapshot {
        cursor: StreamCursor::Sequence {
            store: store_id,
            seq: head,
        },
        projection,
        has_more_history: history.is_some(),
        history,
        budget_exceeded: window.budget_exceeded,
    }))
}

/// Why a history page could not be served.
#[derive(Debug)]
pub enum HistoryError {
    BadCursor(BadCursor),
    Store(StoreError),
}

impl From<StoreError> for HistoryError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<rusqlite::Error> for HistoryError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Store(error.into())
    }
}

/// The page of items before `cursor`, bounded like the snapshot window.
pub fn history_page(
    connection: &Connection,
    thread_id: ThreadId,
    cursor: &str,
    budget: WindowBudget,
) -> Result<HistoryPage, HistoryError> {
    let before = decode_history(cursor, thread_id).map_err(HistoryError::BadCursor)?;
    let transaction = connection.unchecked_transaction()?;
    let connection = &*transaction;
    let window = select_window(connection, &thread_id.to_string(), Some(before), budget)?;
    let next = window
        .more
        .then(|| {
            window
                .items
                .last()
                .map(|oldest| encode_history(thread_id, oldest.position))
        })
        .flatten();
    let run_ids: BTreeSet<RunId> = window.items.iter().filter_map(|item| item.run_id).collect();
    let runs = load_runs(connection, &run_ids)?;
    let mut items = window.items;
    items.reverse();
    Ok(HistoryPage { items, runs, next })
}

/// One turn item as stored: no wire projection.
pub fn turn_item(
    connection: &Connection,
    thread_id: ThreadId,
    item_id: TurnItemId,
) -> StoreResult<Option<TurnItem>> {
    let item = load_one::<TurnItem>(
        connection,
        "SELECT payload_json FROM p_turn_items WHERE turn_item_id = ?1",
        &item_id.to_string(),
        "p_turn_items",
    )?;
    Ok(item.filter(|item| item.thread_id == thread_id))
}

/// How a `thread` subscription with a cursor starts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ThreadResume {
    /// Send these wire-projected events, then `synchronized` at `head`.
    Replay {
        events: Vec<(Sequence, DomainEvent)>,
        head: Sequence,
    },
    /// The cursor is from another store, ahead of the log, or the gap is
    /// over budget: send a fresh bounded snapshot.
    Snapshot,
}

/// Decides between replay and snapshot for a subscriber at `after`
/// (RFC 0003 §4): replay when the thread's gap is at most `budget.events`
/// events, at most `budget.bytes` raw persisted bytes (checked before
/// decoding anything) and at most `budget.bytes` encoded.
pub fn plan_thread_resume(
    connection: &Connection,
    store_id: uuid::Uuid,
    thread_id: ThreadId,
    after: &StreamCursor,
    budget: ReplayBudget,
) -> StoreResult<ThreadResume> {
    let StreamCursor::Sequence { store, seq } = after else {
        return Ok(ThreadResume::Snapshot);
    };
    if *store != store_id {
        return Ok(ThreadResume::Snapshot);
    }
    let transaction = connection.unchecked_transaction()?;
    let connection = &*transaction;
    let head = max_sequence(connection)?;
    if *seq > head {
        return Ok(ThreadResume::Snapshot);
    }
    let key = thread_id.to_string();
    let exists = connection
        .prepare_cached("SELECT 1 FROM p_threads WHERE thread_id = ?1")?
        .query_row([&key], |_| Ok(()))
        .optional()?
        .is_some();
    if !exists {
        return Ok(ThreadResume::Snapshot);
    }
    let limit = budget.events as i64 + 1;
    // Pre-check sizes without reading the payloads into memory.
    let mut count = 0;
    let mut raw = 0;
    {
        let mut statement = connection.prepare_cached(
            "SELECT LENGTH(payload_json) FROM events
             WHERE thread_id = ?1 AND sequence > ?2 ORDER BY sequence LIMIT ?3",
        )?;
        let mut rows = statement.query(params![key, *seq as i64, limit])?;
        while let Some(row) = rows.next()? {
            count += 1;
            raw += row.get::<_, i64>(0)? as usize;
        }
    }
    if count > budget.events || raw > budget.bytes {
        return Ok(ThreadResume::Snapshot);
    }
    let mut statement = connection.prepare_cached(
        "SELECT sequence, event_type, payload_json FROM events
         WHERE thread_id = ?1 AND sequence > ?2 ORDER BY sequence LIMIT ?3",
    )?;
    let rows = statement.query_map(params![key, *seq as i64, limit], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    let mut events = Vec::new();
    let mut encoded = 0;
    for row in rows {
        let (sequence, event_type, payload) = row?;
        let event = DomainEvent::from_parts(&event_type, &payload)
            .map_err(|error| StoreError::Corrupt(format!("event {sequence}: {error}")))?;
        let event = project_event(&event).into_owned();
        encoded += encoded_len(&event);
        if encoded > budget.bytes {
            return Ok(ThreadResume::Snapshot);
        }
        events.push((sequence as Sequence, event));
    }
    Ok(ThreadResume::Replay { events, head })
}
