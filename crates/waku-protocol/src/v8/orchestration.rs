//! Orchestration over v8 (RFC 0003 §5.2–§5.5): `orchestration.dispatch`, the
//! `threads.*` reads, and the items of the `shell`, `thread` and
//! `commandOutput` streams (#54, #56, #57).
//!
//! The entities, turn items and domain events themselves are
//! [`crate::orchestration`]'s (RFC 0001 R17); this module only adds the
//! envelopes the streams and methods carry, and the limits that bound them.

use serde::{Deserialize, Serialize};

use super::{Scope, StreamCursor};
use crate::orchestration::{
    AppThread, Checkpoint, CheckpointScope, Command, CommandId, ContextHandoff, ContextTransfer,
    DomainEvent, ExecutionNode, Message, Plan, Project, ProjectId, ProviderSession, ProviderThread,
    ProviderTurn, Run, RunAttempt, RuntimeRequest, Subagent, ThreadId, TurnItem, TurnItemId,
    WorkspaceStrategy,
};

fn is_false(value: &bool) -> bool {
    !*value
}

/// An opaque position in a thread's (or the archive's) history, at most
/// [`MAX_HISTORY_CURSOR_BYTES`] long. Clients pass it back unchanged.
pub type HistoryCursor = String;

/// Longest history cursor a daemon issues or accepts.
pub const MAX_HISTORY_CURSOR_BYTES: usize = 4096;
/// A bounded thread snapshot (and a history page) holds the items of at most
/// this many user turns…
pub const SNAPSHOT_USER_TURNS: usize = 10;
/// …and at most this many turn items…
pub const SNAPSHOT_ITEMS: usize = 75;
/// …and at most this many encoded bytes of turn items. At least one item is
/// always admitted, so an oversized item cannot stall paging.
pub const SNAPSHOT_BYTES: usize = 1024 * 1024;
/// A `thread` resume replays at most this many events…
pub const THREAD_REPLAY_EVENTS: usize = 128;
/// …of at most this many encoded (and raw persisted) bytes; a larger gap gets
/// a fresh bounded snapshot.
pub const THREAD_REPLAY_BYTES: usize = 1024 * 1024;
/// A `shell` resume replays deltas for a gap of at most this many events.
pub const SHELL_REPLAY_EVENTS: usize = 4096;
/// A shell snapshot over this is split into several `snapshot` items.
pub const SHELL_SNAPSHOT_BYTES: usize = 1024 * 1024;
/// Running tool `turn_item.updated` events are coalesced over this window…
pub const TOOL_COALESCE_MS: u64 = 50;
/// …with at most this many pending.
pub const TOOL_COALESCE_PENDING: usize = 512;
/// `commandOutput` frames are at least this far apart…
pub const OUTPUT_FRAME_MS: u64 = 100;
/// …and this far apart after an overflow.
pub const OUTPUT_OVERFLOW_FRAME_MS: u64 = 500;
/// An `append` frame carries at most this many bytes; more becomes a
/// `replace` with the retained tail.
pub const OUTPUT_APPEND_BYTES: usize = 16 * 1024;
/// Running commands whose output tail the hub retains.
pub const OUTPUT_LIVE_TAILS: usize = 64;
/// Finished commands whose output tail the hub keeps.
pub const OUTPUT_FINISHED_TAILS: usize = 16;
/// Retained output of one command: the first bytes…
pub const OUTPUT_HEAD_BYTES: usize = 64 * 1024;
/// …and the last bytes (RFC 0001 R12: 256 KiB in all).
pub const OUTPUT_TAIL_BYTES: usize = 192 * 1024;
/// A dynamic value over [`WIRE_VALUE_BYTES`] is replaced on the wire by a
/// summary of at most this many characters.
pub const WIRE_SUMMARY_CHARS: usize = 160;
/// Dynamic values up to this size travel as they are.
pub const WIRE_VALUE_BYTES: usize = 4 * 1024;
/// `threads.archived` returns at most this many rows per page.
pub const MAX_ARCHIVED_PAGE: u32 = 200;

// -- orchestration.dispatch ------------------------------------------------

/// `orchestration.dispatch` params. Idempotent by `commandId`: a retry with
/// the same id returns the first outcome.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct DispatchParams {
    pub command_id: CommandId,
    pub command: DispatchCommand,
}

/// A client command (RFC 0001 "Commands", dotted tags). A command this
/// daemon does not know — or a daemon-internal one — does not decode, and
/// the call is answered `badRequest`.
///
/// The union is closed (`x-waku-closed` in the schema) because only clients
/// send it: the daemon is the side that must refuse what it does not know.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(transparent)]
pub struct DispatchCommand(pub Command);

/// `orchestration.dispatch` result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct DispatchResult {
    /// The command's last committed sequence. On the dispatching connection,
    /// every event up to it is queued on subscribed streams before this
    /// reply.
    pub sequence: u64,
    /// The command id was already dispatched; nothing new happened.
    #[serde(default, skip_serializing_if = "is_false")]
    pub duplicate: bool,
}

/// The static scope of the command tagged `command_type` (RFC 0003 §6.1):
/// read receipts need `read`, everything else `operate`.
pub fn command_base_scope(command_type: &str) -> Scope {
    match command_type {
        "thread.visit" | "thread.mark_unread" => Scope::Read,
        _ => Scope::Operate,
    }
}

/// The scopes a dispatched command needs (RFC 0003 §6.1, §6.3): its static
/// scope plus payload-dependent additions.
pub fn command_scopes(command: &Command) -> Vec<Scope> {
    match command {
        Command::ThreadVisit { .. } | Command::ThreadMarkUnread { .. } => {
            vec![command_base_scope(command.command_type())]
        }
        Command::ThreadCreate {
            workspace: WorkspaceStrategy::NewWorktree { .. },
            ..
        } => vec![Scope::Operate, Scope::VcsWrite],
        Command::CheckpointRollback {
            restore_files: true,
            ..
        } => vec![Scope::Operate, Scope::FilesWrite],
        Command::ProjectCreate { .. } => vec![Scope::Operate, Scope::FilesRead],
        _ => vec![Scope::Operate],
    }
}

// -- threads.* reads -------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ThreadSnapshotParams {
    pub thread_id: ThreadId,
    /// Leave out messages a turn item in the snapshot already shows.
    #[serde(default, skip_serializing_if = "is_false")]
    pub compact_turn_items: bool,
}

/// A bounded thread snapshot: the `thread` stream's first item and the
/// `threads.snapshot` result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ThreadSnapshot {
    /// The event-store position the snapshot reflects.
    pub cursor: StreamCursor,
    pub projection: ThreadProjection,
    /// Pass to `threads.historyPage` for older items.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history: Option<HistoryCursor>,
    pub has_more_history: bool,
    /// The window was cut by its byte budget rather than by its turn or item
    /// count.
    #[serde(default, skip_serializing_if = "is_false")]
    pub budget_exceeded: bool,
}

/// The part of a thread a client holds: the thread, the control rows of the
/// runs in view and of every non-terminal run, the always-required control
/// records (pending runtime requests and their rows), and a bounded window of
/// wire-projected turn items in position order. Events apply to it as
/// upserts by entity id.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ThreadProjection {
    pub thread: AppThread,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runs: Vec<Run>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub run_attempts: Vec<RunAttempt>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<ExecutionNode>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subagents: Vec<Subagent>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provider_sessions: Vec<ProviderSession>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provider_threads: Vec<ProviderThread>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provider_turns: Vec<ProviderTurn>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runtime_requests: Vec<RuntimeRequest>,
    /// Messages of non-terminal runs (queued ones included); with
    /// `compactTurnItems`, only those no turn item in the window shows.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<Message>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plans: Vec<Plan>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checkpoint_scopes: Vec<CheckpointScope>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checkpoints: Vec<Checkpoint>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context_transfers: Vec<ContextTransfer>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context_handoffs: Vec<ContextHandoff>,
    /// Wire-projected (#57), ascending by `position`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub turn_items: Vec<TurnItem>,
}

impl ThreadProjection {
    pub fn new(thread: AppThread) -> Self {
        Self {
            thread,
            runs: Vec::new(),
            run_attempts: Vec::new(),
            nodes: Vec::new(),
            subagents: Vec::new(),
            provider_sessions: Vec::new(),
            provider_threads: Vec::new(),
            provider_turns: Vec::new(),
            runtime_requests: Vec::new(),
            messages: Vec::new(),
            plans: Vec::new(),
            checkpoint_scopes: Vec::new(),
            checkpoints: Vec::new(),
            context_transfers: Vec::new(),
            context_handoffs: Vec::new(),
            turn_items: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct HistoryPageParams {
    pub thread_id: ThreadId,
    pub cursor: HistoryCursor,
}

/// Older turn items, bounded like the snapshot window, with the runs they
/// belong to. `next` continues further back.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct HistoryPage {
    /// Wire-projected, ascending by `position`.
    pub items: Vec<TurnItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runs: Vec<Run>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<HistoryCursor>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct TurnItemParams {
    pub thread_id: ThreadId,
    pub item_id: TurnItemId,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ArchivedParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<HistoryCursor>,
    /// At most [`MAX_ARCHIVED_PAGE`]; that is also the default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<ProjectId>,
}

/// Archived threads, most recently archived first.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ArchivedPage {
    pub rows: Vec<ThreadRow>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<HistoryCursor>,
}

// -- threads.search, threads.find ------------------------------------------

/// `threads.search` returns at most this many threads; that is also the
/// default.
pub const MAX_SEARCH_RESULTS: u32 = 200;
/// `threads.find` cuts snippets for at most this many matches around its
/// anchor…
pub const MAX_FIND_WINDOW: u32 = 50;
/// …and loads at most this many turn items on either side of it.
pub const MAX_FIND_ITEMS_AROUND: u32 = 50;

open_string_enum! {
    /// Where a search or find match was found.
    pub enum SearchSource {
        Title = "title",
        Message = "message",
        Plan = "plan",
    }
}

/// `threads.search` params: threads across projects whose title, messages
/// or plans hold every word of `query` (as prefixes).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ThreadSearchParams {
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<ProjectId>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub include_archived: bool,
    /// At most [`MAX_SEARCH_RESULTS`]; 50 when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// One thread found by `threads.search`: its best match (the title, else
/// the latest matching message, else the latest matching plan). Results are
/// ordered by that source, then by recency.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ThreadSearchHit {
    pub thread_id: ThreadId,
    pub project_id: ProjectId,
    pub title: String,
    pub source: SearchSource,
    /// The message, plan or thread id that matched.
    pub entity_id: String,
    /// The matching turn item's position; absent for a title match.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<u64>,
    pub snippet: String,
    /// The thread's last reply, else its last update (epoch milliseconds).
    pub activity_at: i64,
}

/// `threads.find` params: every match of `query` inside one thread, loaded
/// by any client or not.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ThreadFindParams {
    pub thread_id: ThreadId,
    pub query: String,
    /// The position the reader is at: the current match is the first at or
    /// after it. Absent: the latest match.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor: Option<u64>,
    /// Snippets around the current match, at most [`MAX_FIND_WINDOW`]; 17
    /// when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<u32>,
    /// Also load the turn items around the current match, so a client can
    /// jump to history it has not paged in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub items_around: Option<ItemsAround>,
}

/// How many turn items `threads.find` loads before and after the current
/// match, each at most [`MAX_FIND_ITEMS_AROUND`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ItemsAround {
    pub before: u32,
    pub after: u32,
}

/// One match inside a thread.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct FindHit {
    pub source: SearchSource,
    pub entity_id: String,
    pub position: u64,
}

/// A match of the navigation window, with its snippet.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct FindSnippet {
    /// Index into [`ThreadFindResult::matches`].
    pub index: u32,
    pub hit: FindHit,
    pub snippet: String,
}

/// `threads.find` result.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ThreadFindResult {
    /// Every match in transcript order (at most 5,000).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub matches: Vec<FindHit>,
    /// More matches exist than `matches` holds.
    #[serde(default, skip_serializing_if = "is_false")]
    pub truncated: bool,
    /// The current match, as an index into `matches`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current: Option<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub window: Vec<FindSnippet>,
    /// With `itemsAround`: the turn items around the current match,
    /// wire-projected, ascending by `position`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<TurnItem>,
}

// -- shell -----------------------------------------------------------------

open_string_enum! {
    /// Which navigation rows a shell holds.
    #[derive(Default)]
    pub enum ShellLocation {
        #[default]
        Active = "active",
        Archived = "archived",
    }
}

open_string_enum! {
    /// A thread's derived activity, for sidebar badges.
    pub enum ThreadActivity {
        Idle = "idle",
        Preparing = "preparing",
        Working = "working",
        Waiting = "waiting",
        Finalizing = "finalizing",
        Failed = "failed",
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ShellSpec {
    #[serde(default)]
    pub location: ShellLocation,
    /// Only this project's threads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<ProjectId>,
}

/// One navigation row: the thread (never its transcript) and its derived
/// sidebar state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ThreadRow {
    pub thread: AppThread,
    pub activity: ThreadActivity,
    pub pending_request_count: u32,
    pub queued_run_count: u32,
}

impl ThreadRow {
    /// Where the row belongs; `None` once the thread is deleted.
    pub fn location(&self) -> Option<ShellLocation> {
        if self.thread.deleted_at.is_some() {
            None
        } else if self.thread.archived_at.is_some() {
            Some(ShellLocation::Archived)
        } else {
            Some(ShellLocation::Active)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ShellItem {
    /// Rows at `cursor`. A snapshot over [`SHELL_SNAPSHOT_BYTES`] is split;
    /// only the last part has `complete`.
    Snapshot {
        cursor: StreamCursor,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        projects: Vec<Project>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        threads: Vec<ThreadRow>,
        complete: bool,
    },
    ProjectUpdated {
        seq: u64,
        row: Project,
    },
    ProjectRemoved {
        seq: u64,
        project_id: ProjectId,
    },
    /// The row changed; `location` is where it is now, so a client drops a
    /// row that left the location it subscribed to.
    ThreadUpdated {
        seq: u64,
        row: ThreadRow,
        location: ShellLocation,
    },
    ThreadRemoved {
        seq: u64,
        thread_id: ThreadId,
    },
    /// Everything up to `cursor` has been delivered.
    Synchronized {
        cursor: StreamCursor,
    },
    #[serde(other)]
    Unknown,
}

// -- thread ----------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ThreadSpec {
    pub thread_id: ThreadId,
    /// As for `threads.snapshot`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub compact_turn_items: bool,
}

open_string_enum! {
    /// Which text of a streaming item a `delta` extends.
    pub enum DeltaChannel {
        Text = "text",
        Reasoning = "reasoning",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ThreadItem {
    Snapshot(ThreadSnapshot),
    /// A committed event of the thread, wire-projected. Apply as an upsert
    /// by entity id; `seq` is the stream cursor.
    Event {
        seq: u64,
        event: DomainEvent,
    },
    /// Live text, never sequenced, persisted or replayed. Apply only when
    /// `rev` is the item's `streamRev + 1`, then increment `streamRev`; on a
    /// gap ignore deltas until the next snapshot of the item.
    Delta {
        item_id: TurnItemId,
        channel: DeltaChannel,
        rev: u32,
        text: String,
    },
    /// The snapshot or replay is complete; data is fresh up to `cursor`.
    Synchronized {
        cursor: StreamCursor,
    },
    #[serde(other)]
    Unknown,
}

// -- commandOutput ---------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct CommandOutputSpec {
    pub thread_id: ThreadId,
    /// A command execution turn item (or a background node's item).
    pub item_id: TurnItemId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum CommandOutputItem {
    /// The whole retained output; `truncated` when some of it was dropped.
    Replace {
        text: String,
        truncated: bool,
        running: bool,
    },
    /// New output after the previous frame.
    Append { text: String },
    /// The command finished; the stream ends after this.
    Finished {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
        truncated: bool,
    },
    #[serde(other)]
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestration::{CheckpointId, CheckpointScopeId, ModelSelection, RuntimeMode};

    #[test]
    fn payload_dependent_scopes() {
        let thread_id = ThreadId(uuid::Uuid::nil());
        assert_eq!(
            command_scopes(&Command::ThreadVisit {
                thread_id,
                visited_at: 0,
            }),
            [Scope::Read]
        );
        assert_eq!(
            command_scopes(&Command::ThreadArchive { thread_id }),
            [Scope::Operate]
        );
        let create = |workspace| Command::ThreadCreate {
            thread_id,
            project_id: ProjectId(uuid::Uuid::nil()),
            title: None,
            model_selection: ModelSelection::new("codex"),
            runtime_mode: RuntimeMode::FullAccess,
            interaction_mode: Default::default(),
            workspace,
            imported_native_thread: None,
        };
        assert_eq!(
            command_scopes(&create(WorkspaceStrategy::NewWorktree {
                base_ref: "main".into(),
                branch: None,
                start_from_origin: false,
            })),
            [Scope::Operate, Scope::VcsWrite]
        );
        assert_eq!(
            command_scopes(&create(WorkspaceStrategy::Root { branch: None })),
            [Scope::Operate]
        );
        let rollback = |restore_files| Command::CheckpointRollback {
            thread_id,
            scope_id: CheckpointScopeId(uuid::Uuid::nil()),
            checkpoint_id: CheckpointId(uuid::Uuid::nil()),
            restore_files,
        };
        assert_eq!(
            command_scopes(&rollback(true)),
            [Scope::Operate, Scope::FilesWrite]
        );
        assert_eq!(command_scopes(&rollback(false)), [Scope::Operate]);
    }

    #[test]
    fn an_unknown_command_does_not_decode() {
        let json = serde_json::json!({
            "commandId": "c1",
            "command": { "type": "thread.stop", "threadId": uuid::Uuid::nil() },
        });
        assert!(serde_json::from_value::<DispatchParams>(json).is_err());
        let json = serde_json::json!({
            "commandId": "c1",
            "command": { "type": "thread.archive", "threadId": uuid::Uuid::nil() },
        });
        let params = serde_json::from_value::<DispatchParams>(json).unwrap();
        assert!(matches!(params.command.0, Command::ThreadArchive { .. }));
    }

    #[test]
    fn unknown_items_decode_to_unknown() {
        let item: ThreadItem =
            serde_json::from_value(serde_json::json!({"type": "hologram", "x": 1})).unwrap();
        assert_eq!(item, ThreadItem::Unknown);
        let item: ShellItem =
            serde_json::from_value(serde_json::json!({"type": "nebula"})).unwrap();
        assert_eq!(item, ShellItem::Unknown);
        let location: ShellLocation = serde_json::from_value(serde_json::json!("orbit")).unwrap();
        assert_eq!(location, ShellLocation::Unknown("orbit".into()));
    }
}
