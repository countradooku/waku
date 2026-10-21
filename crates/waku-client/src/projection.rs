//! Client-side reducers for the v8 orchestration streams (RFC 0003 §5.4),
//! the Rust twin of `packages/waku-client/src/v8/projection.ts`: a thread
//! projection fed by `thread` items and history pages, the shell's
//! navigation rows fed by `shell` items, and a command's output fed by
//! `commandOutput` frames.
//!
//! Applying an event is an upsert by entity id (RFC 0001 rule 4), so
//! replays and duplicates are harmless. Unknown items and events still
//! advance the cursor. Live `delta`s apply only in `streamRev` order: a gap
//! is ignored until the next snapshot of the item replaces it.
//!
//! Both reducers replay `fixtures/reducers.json` (recorded by `waku-core`'s
//! `reducer_fixture` test) to exactly the daemon's projection; see the tests
//! at the bottom.

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

use waku_protocol::orchestration::*;
use waku_protocol::v8::StreamCursor;
use waku_protocol::v8::orchestration::{
    CommandOutputItem, DeltaChannel, HistoryCursor, HistoryPage, ShellItem, ShellLocation,
    ThreadItem, ThreadProjection, ThreadRow, ThreadSnapshot,
};

/// What applying one `thread` item changed, so a consumer can route the
/// change to the cheapest redraw.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ThreadApplied {
    /// The state was replaced by a snapshot.
    pub snapshot: bool,
    /// A sequenced event changed an entity (anything besides live text).
    pub event: bool,
    /// A live `delta` was folded into an item's text.
    pub delta: bool,
    /// The stream reported `synchronized`.
    pub synchronized: bool,
}

impl ThreadApplied {
    pub fn changed(self) -> bool {
        self.snapshot || self.event || self.delta
    }

    pub fn union(self, other: Self) -> Self {
        Self {
            snapshot: self.snapshot || other.snapshot,
            event: self.event || other.event,
            delta: self.delta || other.delta,
            synchronized: self.synchronized || other.synchronized,
        }
    }
}

/// A thread as one client holds it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ThreadState {
    /// Where the client stands; resubscribe with it after a disconnect.
    pub cursor: Option<StreamCursor>,
    /// A snapshot or replay has been followed by `synchronized`.
    pub synchronized: bool,
    pub thread: Option<AppThread>,
    pub runs: HashMap<RunId, Run>,
    pub run_attempts: HashMap<RunAttemptId, RunAttempt>,
    pub nodes: HashMap<NodeId, ExecutionNode>,
    pub subagents: HashMap<NodeId, Subagent>,
    pub provider_sessions: HashMap<ProviderSessionId, ProviderSession>,
    pub provider_threads: HashMap<ProviderThreadId, ProviderThread>,
    pub provider_turns: HashMap<ProviderTurnId, ProviderTurn>,
    pub runtime_requests: HashMap<RuntimeRequestId, RuntimeRequest>,
    pub messages: HashMap<MessageId, Message>,
    pub plans: HashMap<PlanId, Plan>,
    pub checkpoint_scopes: HashMap<CheckpointScopeId, CheckpointScope>,
    pub checkpoints: HashMap<CheckpointId, Checkpoint>,
    pub context_transfers: HashMap<ContextTransferId, ContextTransfer>,
    pub context_handoffs: HashMap<ContextHandoffId, ContextHandoff>,
    /// Turn items by id; see [`Self::ordered_turn_items`].
    pub turn_items: HashMap<TurnItemId, TurnItem>,
    /// Pass to `threads.historyPage` for older items.
    pub history: Option<HistoryCursor>,
    pub has_more_history: bool,
    pub budget_exceeded: bool,
}

fn by_id<K: Eq + Hash, V>(rows: Vec<V>, key: impl Fn(&V) -> K) -> HashMap<K, V> {
    rows.into_iter().map(|row| (key(&row), row)).collect()
}

fn advance(cursor: &mut Option<StreamCursor>, seq: u64) {
    if let Some(StreamCursor::Sequence { seq: current, .. }) = cursor {
        *current = (*current).max(seq);
    }
}

impl ThreadState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces the state with a bounded snapshot.
    pub fn apply_snapshot(&mut self, snapshot: ThreadSnapshot) {
        let ThreadSnapshot {
            cursor,
            projection,
            history,
            has_more_history,
            budget_exceeded,
        } = snapshot;
        let ThreadProjection {
            thread,
            runs,
            run_attempts,
            nodes,
            subagents,
            provider_sessions,
            provider_threads,
            provider_turns,
            runtime_requests,
            messages,
            plans,
            checkpoint_scopes,
            checkpoints,
            context_transfers,
            context_handoffs,
            turn_items,
        } = projection;
        *self = Self {
            cursor: Some(cursor),
            synchronized: false,
            thread: Some(thread),
            runs: by_id(runs, |row| row.id),
            run_attempts: by_id(run_attempts, |row| row.id),
            nodes: by_id(nodes, |row| row.id),
            subagents: by_id(subagents, |row| row.id),
            provider_sessions: by_id(provider_sessions, |row| row.id),
            provider_threads: by_id(provider_threads, |row| row.id),
            provider_turns: by_id(provider_turns, |row| row.id),
            runtime_requests: by_id(runtime_requests, |row| row.id),
            messages: by_id(messages, |row| row.id),
            plans: by_id(plans, |row| row.id),
            checkpoint_scopes: by_id(checkpoint_scopes, |row| row.id),
            checkpoints: by_id(checkpoints, |row| row.id),
            context_transfers: by_id(context_transfers, |row| row.id),
            context_handoffs: by_id(context_handoffs, |row| row.id),
            turn_items: by_id(turn_items, |row| row.id),
            history,
            has_more_history,
            budget_exceeded,
        };
    }

    /// Applies one committed event (an upsert by entity id). Returns whether
    /// it changed anything this thread holds.
    pub fn apply_event(&mut self, event: DomainEvent) -> bool {
        match event {
            DomainEvent::ThreadUpdated(update) => {
                if self
                    .thread
                    .as_ref()
                    .is_none_or(|thread| thread.id == update.thread.id)
                {
                    self.thread = Some(update.thread);
                    return true;
                }
                false
            }
            DomainEvent::RunUpdated(run) => upsert(&mut self.runs, run.id, run),
            DomainEvent::RunAttemptUpdated(attempt) => {
                upsert(&mut self.run_attempts, attempt.id, attempt)
            }
            DomainEvent::NodeUpdated(node) => upsert(&mut self.nodes, node.id, node),
            DomainEvent::SubagentUpdated(subagent) => {
                upsert(&mut self.subagents, subagent.id, subagent)
            }
            DomainEvent::ProviderSessionUpdated(session) => {
                upsert(&mut self.provider_sessions, session.id, session)
            }
            DomainEvent::ProviderThreadUpdated(provider_thread) => {
                // A provider thread that left this thread is dropped, as on
                // the daemon.
                let left = self
                    .thread
                    .as_ref()
                    .is_some_and(|thread| provider_thread.app_thread_id != Some(thread.id));
                if left {
                    self.provider_threads.remove(&provider_thread.id).is_some()
                } else {
                    upsert(
                        &mut self.provider_threads,
                        provider_thread.id,
                        provider_thread,
                    )
                }
            }
            DomainEvent::ProviderTurnUpdated(turn) => {
                upsert(&mut self.provider_turns, turn.id, turn)
            }
            DomainEvent::RuntimeRequestUpdated(request) => {
                upsert(&mut self.runtime_requests, request.id, request)
            }
            DomainEvent::MessageUpdated(message) => upsert(&mut self.messages, message.id, message),
            DomainEvent::PlanUpdated(plan) => upsert(&mut self.plans, plan.id, plan),
            // A sequenced snapshot replaces the item and its live text.
            DomainEvent::TurnItemUpdated(item) => upsert(&mut self.turn_items, item.id, item),
            DomainEvent::CheckpointScopeUpdated(scope) => {
                upsert(&mut self.checkpoint_scopes, scope.id, scope)
            }
            DomainEvent::CheckpointUpdated(checkpoint) => {
                upsert(&mut self.checkpoints, checkpoint.id, checkpoint)
            }
            DomainEvent::ContextTransferUpdated(transfer) => {
                upsert(&mut self.context_transfers, transfer.id, transfer)
            }
            DomainEvent::ContextHandoffUpdated(handoff) => {
                upsert(&mut self.context_handoffs, handoff.id, handoff)
            }
            // `project.updated` does not belong to a thread; unknown events
            // are skipped (the cursor still advances).
            DomainEvent::ProjectUpdated(_) | DomainEvent::Unknown => false,
        }
    }

    /// Applies a live delta when it is the next one of its item (`rev ==
    /// streamRev + 1`). Returns whether it applied; a gap is ignored until
    /// the item's next snapshot.
    pub fn apply_delta(
        &mut self,
        item_id: TurnItemId,
        channel: &DeltaChannel,
        rev: u32,
        text: &str,
    ) -> bool {
        let Some(item) = self.turn_items.get_mut(&item_id) else {
            return false;
        };
        if rev != item.stream_rev.wrapping_add(1) {
            return false;
        }
        let target = match (channel, &mut item.kind) {
            (DeltaChannel::Reasoning, TurnItemKind::Reasoning { text, .. })
            | (DeltaChannel::Text, TurnItemKind::AssistantMessage { text, .. })
            | (DeltaChannel::Text, TurnItemKind::ProposedPlan { markdown: text, .. }) => text,
            _ => return false,
        };
        target.push_str(text);
        item.stream_rev = rev;
        true
    }

    /// Applies one `thread` stream item.
    pub fn apply_item(&mut self, item: ThreadItem) -> ThreadApplied {
        let mut applied = ThreadApplied::default();
        match item {
            ThreadItem::Snapshot(snapshot) => {
                self.apply_snapshot(snapshot);
                applied.snapshot = true;
            }
            ThreadItem::Event { seq, event } => {
                applied.event = self.apply_event(event);
                advance(&mut self.cursor, seq);
            }
            ThreadItem::Delta {
                item_id,
                channel,
                rev,
                text,
            } => applied.delta = self.apply_delta(item_id, &channel, rev, &text),
            ThreadItem::Synchronized { cursor } => {
                self.cursor = Some(cursor);
                self.synchronized = true;
                applied.synchronized = true;
            }
            ThreadItem::Unknown => {}
        }
        applied
    }

    /// Merges an older page from `threads.historyPage`.
    pub fn merge_history_page(&mut self, page: HistoryPage) {
        for item in page.items {
            // A live update may already be newer; keep it.
            self.turn_items.entry(item.id).or_insert(item);
        }
        for run in page.runs {
            self.runs.entry(run.id).or_insert(run);
        }
        self.has_more_history = page.next.is_some();
        self.history = page.next;
    }

    /// Turn items in display order.
    pub fn ordered_turn_items(&self) -> Vec<&TurnItem> {
        let mut items = self.turn_items.values().collect::<Vec<_>>();
        items.sort_by_key(|item| (item.position, item.id));
        items
    }

    /// The state as a projection, collections sorted by id and turn items by
    /// position.
    pub fn projection(&self) -> Option<ThreadProjection> {
        let thread = self.thread.clone()?;
        let mut projection = ThreadProjection::new(thread);
        projection.runs = sorted(&self.runs);
        projection.run_attempts = sorted(&self.run_attempts);
        projection.nodes = sorted(&self.nodes);
        projection.subagents = sorted(&self.subagents);
        projection.provider_sessions = sorted(&self.provider_sessions);
        projection.provider_threads = sorted(&self.provider_threads);
        projection.provider_turns = sorted(&self.provider_turns);
        projection.runtime_requests = sorted(&self.runtime_requests);
        projection.messages = sorted(&self.messages);
        projection.plans = sorted(&self.plans);
        projection.checkpoint_scopes = sorted(&self.checkpoint_scopes);
        projection.checkpoints = sorted(&self.checkpoints);
        projection.context_transfers = sorted(&self.context_transfers);
        projection.context_handoffs = sorted(&self.context_handoffs);
        projection.turn_items = self.ordered_turn_items().into_iter().cloned().collect();
        Some(projection)
    }
}

fn upsert<K: Eq + Hash, V: PartialEq>(map: &mut HashMap<K, V>, key: K, value: V) -> bool {
    match map.get_mut(&key) {
        Some(existing) if *existing == value => false,
        Some(existing) => {
            *existing = value;
            true
        }
        None => {
            map.insert(key, value);
            true
        }
    }
}

fn sorted<K: Ord + Clone, V: Clone>(map: &HashMap<K, V>) -> Vec<V> {
    map.iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect()
}

/// What applying one `shell` item changed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ShellApplied {
    pub projects: bool,
    pub threads: bool,
    pub synchronized: bool,
}

impl ShellApplied {
    pub fn changed(self) -> bool {
        self.projects || self.threads
    }

    pub fn union(self, other: Self) -> Self {
        Self {
            projects: self.projects || other.projects,
            threads: self.threads || other.threads,
            synchronized: self.synchronized || other.synchronized,
        }
    }
}

/// Parts of a split shell snapshot still arriving.
#[derive(Clone, Debug, Default, PartialEq)]
struct PartialShell {
    projects: HashMap<ProjectId, Project>,
    threads: HashMap<ThreadId, ThreadRow>,
}

/// The shell's navigation rows.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ShellState {
    pub location: ShellLocation,
    pub cursor: Option<StreamCursor>,
    pub synchronized: bool,
    /// The last snapshot has arrived whole.
    pub complete: bool,
    pub projects: HashMap<ProjectId, Project>,
    pub threads: HashMap<ThreadId, ThreadRow>,
    partial: Option<PartialShell>,
}

impl ShellState {
    pub fn new(location: ShellLocation) -> Self {
        Self {
            location,
            ..Self::default()
        }
    }

    /// Applies one `shell` stream item.
    pub fn apply_item(&mut self, item: ShellItem) -> ShellApplied {
        let mut applied = ShellApplied::default();
        match item {
            ShellItem::Snapshot {
                cursor,
                projects,
                threads,
                complete,
            } => {
                let mut partial = self.partial.take().unwrap_or_default();
                for project in projects {
                    partial.projects.insert(project.id, project);
                }
                for row in threads {
                    partial.threads.insert(row.thread.id, row);
                }
                self.cursor = Some(cursor);
                if complete {
                    self.projects = partial.projects;
                    self.threads = partial.threads;
                    self.complete = true;
                    applied.projects = true;
                    applied.threads = true;
                } else {
                    self.partial = Some(partial);
                }
            }
            ShellItem::ProjectUpdated { seq, row } => {
                applied.projects = upsert(&mut self.projects, row.id, row);
                advance(&mut self.cursor, seq);
            }
            ShellItem::ProjectRemoved { seq, project_id } => {
                applied.projects = self.projects.remove(&project_id).is_some();
                advance(&mut self.cursor, seq);
            }
            ShellItem::ThreadUpdated { seq, row, location } => {
                // A row that moved to another location leaves this one.
                applied.threads = if location == self.location {
                    upsert(&mut self.threads, row.thread.id, row)
                } else {
                    self.threads.remove(&row.thread.id).is_some()
                };
                advance(&mut self.cursor, seq);
            }
            ShellItem::ThreadRemoved { seq, thread_id } => {
                applied.threads = self.threads.remove(&thread_id).is_some();
                advance(&mut self.cursor, seq);
            }
            ShellItem::Synchronized { cursor } => {
                self.cursor = Some(cursor);
                self.synchronized = true;
                applied.synchronized = true;
            }
            ShellItem::Unknown => {}
        }
        applied
    }
}

/// One command's output, as an expanded row shows it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandOutputState {
    pub text: String,
    pub truncated: bool,
    pub running: bool,
    pub exit_code: Option<i32>,
}

impl Default for CommandOutputState {
    fn default() -> Self {
        Self {
            text: String::new(),
            truncated: false,
            running: true,
            exit_code: None,
        }
    }
}

impl CommandOutputState {
    /// Applies one `commandOutput` frame. Returns whether it changed.
    pub fn apply_item(&mut self, item: CommandOutputItem) -> bool {
        match item {
            CommandOutputItem::Replace {
                text,
                truncated,
                running,
            } => {
                self.text = text;
                self.truncated = truncated;
                self.running = running;
                true
            }
            CommandOutputItem::Append { text } => {
                self.text.push_str(&text);
                !text.is_empty()
            }
            CommandOutputItem::Finished {
                exit_code,
                truncated,
            } => {
                self.running = false;
                self.exit_code = exit_code;
                self.truncated |= truncated;
                true
            }
            CommandOutputItem::Unknown => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use uuid::Uuid;

    /// `packages/waku-client/src/v8/fixtures/reducers.json`: what a client
    /// saw of a thread and a shell while things changed, and what a fresh
    /// snapshot said afterwards. The TypeScript reducers replay the same file.
    const FIXTURE: &str =
        include_str!("../../../packages/waku-client/src/v8/fixtures/reducers.json");
    const STORE: Uuid = Uuid::nil();

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Fixture {
        thread: ThreadFixture,
        shell: ShellFixture,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct ThreadFixture {
        items: Vec<ThreadItem>,
        expected: ThreadProjection,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct ShellFixture {
        items: Vec<ShellItem>,
        expected_threads: Vec<ThreadRow>,
    }

    fn fixture() -> Fixture {
        serde_json::from_str(FIXTURE).expect("the reducer fixture decodes")
    }

    fn assert_held<K: Ord + Clone + std::fmt::Debug, V: PartialEq + std::fmt::Debug>(
        held: &[V],
        expected: &[V],
        key: impl Fn(&V) -> K,
    ) {
        let held = held
            .iter()
            .map(|row| (key(row), row))
            .collect::<BTreeMap<_, _>>();
        for row in expected {
            assert_eq!(held.get(&key(row)), Some(&row), "row {:?}", key(row));
        }
    }

    #[test]
    fn snapshot_events_and_deltas_equal_the_daemons_projection() {
        let fixture = fixture().thread;
        let last_seq = match fixture.items.last() {
            Some(ThreadItem::Event { seq, .. }) => *seq,
            other => panic!("the fixture ends with an event, not {other:?}"),
        };
        let mut state = ThreadState::new();
        for item in fixture.items {
            state.apply_item(item);
        }
        let actual = state.projection().unwrap();
        let expected = fixture.expected;
        assert_eq!(actual.thread, expected.thread);
        // The window the daemon would send now is exactly what the client
        // holds.
        assert_eq!(actual.turn_items, expected.turn_items);
        // Every control row the daemon would send is held, unchanged.
        assert_held(&actual.runs, &expected.runs, |row| row.id);
        assert_held(&actual.run_attempts, &expected.run_attempts, |row| row.id);
        assert_held(&actual.nodes, &expected.nodes, |row| row.id);
        assert_held(
            &actual.runtime_requests,
            &expected.runtime_requests,
            |row| row.id,
        );
        assert_held(&actual.plans, &expected.plans, |row| row.id);
        assert_held(&actual.checkpoints, &expected.checkpoints, |row| row.id);
        assert_held(&actual.messages, &expected.messages, |row| row.id);
        // The cursor followed the events.
        assert_eq!(
            state.cursor,
            Some(StreamCursor::Sequence {
                store: STORE,
                seq: last_seq,
            })
        );
        assert!(state.synchronized);
    }

    #[test]
    fn deltas_apply_in_stream_rev_order_and_wait_for_a_snapshot_after_a_gap() {
        let items = fixture().thread.items;
        let deltas = items
            .iter()
            .filter_map(|item| match item {
                ThreadItem::Delta {
                    item_id,
                    channel,
                    rev,
                    text,
                } => Some((*item_id, channel.clone(), *rev, text.clone())),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            deltas.iter().map(|delta| delta.2).collect::<Vec<_>>(),
            [3, 4, 6]
        );
        let mut state = ThreadState::new();
        for item in items {
            if matches!(item, ThreadItem::Delta { .. }) {
                break;
            }
            state.apply_item(item);
        }
        let answer = deltas[0].0;
        let text = |state: &ThreadState| match &state.turn_items[&answer].kind {
            TurnItemKind::AssistantMessage { text, .. } => text.clone(),
            other => panic!("not an answer: {other:?}"),
        };
        let apply = |state: &mut ThreadState, index: usize| {
            let (item_id, channel, rev, text) = &deltas[index];
            state.apply_delta(*item_id, channel, *rev, text)
        };
        assert_eq!(text(&state), "He");
        assert!(apply(&mut state, 0));
        assert!(!apply(&mut state, 0), "already applied");
        assert!(apply(&mut state, 1));
        assert_eq!(text(&state), "Hello,");
        assert_eq!(state.turn_items[&answer].stream_rev, 4);
        assert!(!apply(&mut state, 2), "rev 6 after 4 is a gap");
        assert_eq!(text(&state), "Hello,");
    }

    #[test]
    fn history_pages_merge_older_items_without_replacing_newer_ones() {
        let mut state = ThreadState::new();
        for item in fixture().thread.items {
            state.apply_item(item);
        }
        let first = state.ordered_turn_items()[0].clone();
        let older = TurnItem {
            id: TurnItemId(Uuid::from_u128(0xaa)),
            position: 1,
            ..first.clone()
        };
        let stale = TurnItem {
            title: Some("stale".into()),
            ..first.clone()
        };
        state.merge_history_page(HistoryPage {
            items: vec![older.clone(), stale],
            runs: Vec::new(),
            next: Some("h1.next".into()),
        });
        assert_eq!(state.ordered_turn_items()[0], &older);
        assert_eq!(state.turn_items[&first.id], first);
        assert_eq!(state.history.as_deref(), Some("h1.next"));
        assert!(state.has_more_history);
        state.merge_history_page(HistoryPage {
            items: Vec::new(),
            runs: Vec::new(),
            next: None,
        });
        assert!(!state.has_more_history);
    }

    #[test]
    fn unknown_items_and_events_are_skipped_but_still_advance_the_cursor() {
        let mut state = ThreadState::new();
        let first = fixture().thread.items.into_iter().next().unwrap();
        state.apply_item(first);
        let Some(StreamCursor::Sequence { seq: before, .. }) = state.cursor else {
            panic!("a sequence cursor");
        };
        let unknown: ThreadItem = serde_json::from_value(serde_json::json!({
            "type": "event",
            "seq": before + 7,
            "event": {"type": "galaxy.updated", "id": "x"},
        }))
        .unwrap();
        assert!(!state.apply_item(unknown).changed());
        assert!(
            matches!(state.cursor, Some(StreamCursor::Sequence { seq, .. }) if seq == before + 7)
        );
        let hologram: ThreadItem =
            serde_json::from_value(serde_json::json!({"type": "hologram"})).unwrap();
        state.apply_item(hologram);
        assert!(
            matches!(state.cursor, Some(StreamCursor::Sequence { seq, .. }) if seq == before + 7)
        );
    }

    #[test]
    fn shell_snapshot_and_deltas_equal_the_daemons_active_rows() {
        let fixture = fixture().shell;
        let mut state = ShellState::new(ShellLocation::Active);
        for item in fixture.items {
            state.apply_item(item);
        }
        assert!(state.complete && state.synchronized);
        let expected = fixture
            .expected_threads
            .into_iter()
            .map(|row| (row.thread.id, row))
            .collect::<HashMap<_, _>>();
        assert_eq!(state.threads, expected);
    }

    #[test]
    fn split_shell_snapshots_are_applied_whole() {
        let mut state = ShellState::new(ShellLocation::Active);
        let cursor = StreamCursor::Sequence {
            store: STORE,
            seq: 3,
        };
        let row = fixture().shell.expected_threads.remove(0);
        state.apply_item(ShellItem::Snapshot {
            cursor: cursor.clone(),
            projects: Vec::new(),
            threads: vec![row.clone()],
            complete: false,
        });
        assert!(state.threads.is_empty());
        state.apply_item(ShellItem::Snapshot {
            cursor,
            projects: Vec::new(),
            threads: Vec::new(),
            complete: true,
        });
        assert_eq!(state.threads.len(), 1);
        let applied = state.apply_item(ShellItem::ThreadUpdated {
            seq: 9,
            row,
            location: ShellLocation::Archived,
        });
        assert!(applied.threads);
        assert!(state.threads.is_empty());
        assert_eq!(
            state.cursor,
            Some(StreamCursor::Sequence {
                store: STORE,
                seq: 9,
            })
        );
    }

    #[test]
    fn command_output_replace_append_and_finished() {
        let mut state = CommandOutputState::default();
        state.apply_item(CommandOutputItem::Replace {
            text: "a".into(),
            truncated: false,
            running: true,
        });
        state.apply_item(CommandOutputItem::Append { text: "b".into() });
        state.apply_item(CommandOutputItem::Replace {
            text: "…tail".into(),
            truncated: true,
            running: true,
        });
        state.apply_item(CommandOutputItem::Finished {
            exit_code: Some(2),
            truncated: true,
        });
        assert_eq!(
            state,
            CommandOutputState {
                text: "…tail".into(),
                truncated: true,
                running: false,
                exit_code: Some(2),
            }
        );
    }
}
