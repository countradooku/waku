//! A working copy of a thread projection for one decision.
//!
//! Handlers read through the draft (changes first, then the projection) and
//! write entities into it. `finish` emits one `*.updated` event per changed
//! entity with its final state, so several steps of one decision (end a run,
//! then start the next queued one) compose without duplicate events.

use std::collections::HashMap;
use std::path::PathBuf;

use uuid::Uuid;
use waku_protocol::orchestration::*;

use super::{CancelUnsettled, Decision};
use crate::effects::{Effect, EffectType};
use crate::ids::IdSource;
use crate::projection::{ThreadProjection, queue_key};

/// Entities the draft tracks, keyed by their id.
pub(crate) trait Keyed {
    type Key: PartialEq + Copy;
    fn key(&self) -> Self::Key;
}

macro_rules! keyed {
    ($($entity:ty => $key:ty),+ $(,)?) => {
        $(impl Keyed for $entity {
            type Key = $key;
            fn key(&self) -> $key {
                self.id
            }
        })+
    };
}

keyed!(
    Run => RunId,
    RunAttempt => RunAttemptId,
    ExecutionNode => NodeId,
    Subagent => NodeId,
    ProviderSession => ProviderSessionId,
    ProviderThread => ProviderThreadId,
    ProviderTurn => ProviderTurnId,
    Plan => PlanId,
    RuntimeRequest => RuntimeRequestId,
    Message => MessageId,
    CheckpointScope => CheckpointScopeId,
    Checkpoint => CheckpointId,
    TurnItem => TurnItemId,
);

fn put<T: Keyed>(changes: &mut Vec<T>, value: T) {
    match changes
        .iter_mut()
        .find(|existing| existing.key() == value.key())
    {
        Some(existing) => *existing = value,
        None => changes.push(value),
    }
}

fn find<T: Keyed>(changes: &[T], key: T::Key) -> Option<&T> {
    changes.iter().find(|existing| existing.key() == key)
}

pub(crate) struct Draft<'a> {
    pub base: &'a ThreadProjection,
    pub now: Timestamp,
    pub ids: &'a mut dyn IdSource,
    /// The project root, the working directory of runs without a worktree.
    pub project_path: Option<PathBuf>,
    pub thread: AppThread,
    thread_change: Option<ThreadChange>,
    thread_dirty: bool,
    runs: Vec<Run>,
    attempts: Vec<RunAttempt>,
    nodes: Vec<ExecutionNode>,
    subagents: Vec<Subagent>,
    provider_sessions: Vec<ProviderSession>,
    provider_threads: Vec<ProviderThread>,
    provider_turns: Vec<ProviderTurn>,
    plans: Vec<Plan>,
    requests: Vec<RuntimeRequest>,
    messages: Vec<Message>,
    scopes: Vec<CheckpointScope>,
    checkpoints: Vec<Checkpoint>,
    items: Vec<TurnItem>,
    positions: HashMap<u32, u32>,
    effects: Vec<Effect>,
    cancels: Vec<CancelUnsettled>,
}

impl<'a> Draft<'a> {
    pub fn new(
        base: &'a ThreadProjection,
        now: Timestamp,
        ids: &'a mut dyn IdSource,
        project_path: Option<PathBuf>,
    ) -> Self {
        Self {
            base,
            now,
            ids,
            project_path,
            thread: base.thread.clone(),
            thread_change: None,
            thread_dirty: false,
            runs: Vec::new(),
            attempts: Vec::new(),
            nodes: Vec::new(),
            subagents: Vec::new(),
            provider_sessions: Vec::new(),
            provider_threads: Vec::new(),
            provider_turns: Vec::new(),
            plans: Vec::new(),
            requests: Vec::new(),
            messages: Vec::new(),
            scopes: Vec::new(),
            checkpoints: Vec::new(),
            items: Vec::new(),
            positions: HashMap::new(),
            effects: Vec::new(),
            cancels: Vec::new(),
        }
    }

    pub fn thread_id(&self) -> ThreadId {
        self.base.thread.id
    }

    pub fn derived(&self, scope: &str, key: impl std::fmt::Display) -> Uuid {
        self.ids.derived(scope, &key.to_string())
    }

    pub fn new_id(&mut self) -> Uuid {
        self.ids.new_id()
    }

    // -- thread ---------------------------------------------------------------

    /// Marks the thread changed. The first specific reason wins over
    /// `Activity`.
    pub fn touch_thread(&mut self, change: ThreadChange) -> &mut AppThread {
        self.thread_dirty = true;
        match self.thread_change {
            None | Some(ThreadChange::Activity) => self.thread_change = Some(change),
            Some(_) => {}
        }
        &mut self.thread
    }

    // -- reads ----------------------------------------------------------------

    pub fn run(&self, id: RunId) -> Option<&Run> {
        find(&self.runs, id).or_else(|| self.base.runs.get(&id))
    }

    /// Every run, with this decision's changes applied.
    pub fn all_runs(&self) -> Vec<&Run> {
        let mut runs = self
            .base
            .runs
            .values()
            .map(|run| find(&self.runs, run.id).unwrap_or(run))
            .collect::<Vec<_>>();
        runs.extend(
            self.runs
                .iter()
                .filter(|run| !self.base.runs.contains_key(&run.id)),
        );
        runs
    }

    pub fn has_runs(&self) -> bool {
        !self.base.runs.is_empty() || !self.runs.is_empty()
    }

    /// The run occupying the thread, with this decision's changes applied.
    /// Reads the projection's active-run index: O(1), not O(history).
    pub fn active_run(&self) -> Option<&Run> {
        self.base
            .active_runs
            .iter()
            .filter_map(|id| self.run(*id))
            .chain(self.runs.iter())
            .find(|run| run.status.is_active())
    }

    /// Base queue entries still queued after this decision's changes, in
    /// delivery order, followed by runs this decision queued.
    fn queue_iter(&self) -> impl DoubleEndedIterator<Item = &Run> {
        self.base
            .queue
            .iter()
            .filter_map(|(_, _, id)| self.run(*id))
            .filter(|run| run.status == RunStatus::Queued)
    }

    fn newly_queued(&self) -> impl Iterator<Item = &Run> {
        self.runs.iter().filter(|run| {
            run.status == RunStatus::Queued
                && self
                    .base
                    .runs
                    .get(&run.id)
                    .is_none_or(|before| before.status != RunStatus::Queued)
        })
    }

    /// The next run to deliver: O(1) for an untouched queue.
    pub fn queue_head(&self) -> Option<&Run> {
        let base = self.queue_iter().next();
        let new = self.newly_queued().min_by_key(|run| queue_key(run));
        match (base, new) {
            (Some(base), Some(new)) => Some(if queue_key(new) < queue_key(base) {
                new
            } else {
                base
            }),
            (base, new) => base.or(new),
        }
    }

    /// The highest queue position in use.
    pub fn max_queue_position(&self) -> Option<u32> {
        let base = self
            .queue_iter()
            .next_back()
            .and_then(|run| run.queue_position);
        let new = self
            .newly_queued()
            .filter_map(|run| run.queue_position)
            .max();
        base.max(new)
    }

    /// Every queued run in delivery order. O(queue): only for commands
    /// that touch the whole queue (hold, resume, delete).
    pub fn queued_runs(&self) -> Vec<&Run> {
        let mut queued = self
            .queue_iter()
            .chain(self.newly_queued())
            .collect::<Vec<_>>();
        queued.sort_by_key(|run| queue_key(run));
        queued
    }

    pub fn latest_ordinal(&self) -> u32 {
        self.runs
            .iter()
            .filter_map(|run| run.ordinal)
            .fold(self.base.max_ordinal, u32::max)
    }

    pub fn attempt(&self, id: RunAttemptId) -> Option<&RunAttempt> {
        find(&self.attempts, id).or_else(|| self.base.attempts.get(&id))
    }

    pub fn node(&self, id: NodeId) -> Option<&ExecutionNode> {
        find(&self.nodes, id).or_else(|| self.base.nodes.get(&id))
    }

    pub fn subagent(&self, id: NodeId) -> Option<&Subagent> {
        find(&self.subagents, id).or_else(|| self.base.subagents.get(&id))
    }

    pub fn provider_session(&self, id: ProviderSessionId) -> Option<&ProviderSession> {
        find(&self.provider_sessions, id).or_else(|| self.base.provider_sessions.get(&id))
    }

    pub fn provider_thread(&self, id: ProviderThreadId) -> Option<&ProviderThread> {
        find(&self.provider_threads, id).or_else(|| self.base.provider_threads.get(&id))
    }

    pub fn provider_turn(&self, id: ProviderTurnId) -> Option<&ProviderTurn> {
        find(&self.provider_turns, id).or_else(|| self.base.provider_turns.get(&id))
    }

    /// A turn item of a non-terminal run, by id. Scans the hot set, which
    /// only holds the rows of runs that are still open.
    pub fn item(&self, id: TurnItemId) -> Option<&TurnItem> {
        find(&self.items, id).or_else(|| self.base.hot_items.values().find(|item| item.id == id))
    }

    /// The hot turn items of a run, with this decision's changes applied.
    pub fn items_of_run(&self, run_id: RunId) -> Vec<TurnItem> {
        let mut items = self
            .base
            .hot_items
            .values()
            .filter(|item| item.run_id == Some(run_id))
            .map(|item| find(&self.items, item.id).unwrap_or(item).clone())
            .collect::<Vec<_>>();
        items.extend(
            self.items
                .iter()
                .filter(|item| {
                    item.run_id == Some(run_id)
                        && !self.base.hot_items.values().any(|base| base.id == item.id)
                })
                .cloned(),
        );
        items
    }

    /// The hot messages of a run, with this decision's changes applied.
    pub fn messages_of_run(&self, run_id: RunId) -> Vec<Message> {
        let mut messages = self
            .base
            .hot_messages
            .values()
            .filter(|message| message.run_id == Some(run_id))
            .map(|message| find(&self.messages, message.id).unwrap_or(message).clone())
            .collect::<Vec<_>>();
        messages.extend(
            self.messages
                .iter()
                .filter(|message| {
                    message.run_id == Some(run_id)
                        && !self.base.hot_messages.contains_key(&message.id)
                })
                .cloned(),
        );
        messages
    }

    /// The nodes of a run, with this decision's changes applied.
    pub fn nodes_of_run(&self, run_id: RunId) -> Vec<ExecutionNode> {
        let mut nodes = self
            .base
            .nodes
            .values()
            .filter(|node| node.run_id == Some(run_id))
            .map(|node| find(&self.nodes, node.id).unwrap_or(node).clone())
            .collect::<Vec<_>>();
        nodes.extend(
            self.nodes
                .iter()
                .filter(|node| {
                    node.run_id == Some(run_id) && !self.base.nodes.contains_key(&node.id)
                })
                .cloned(),
        );
        nodes
    }

    pub fn request(&self, id: RuntimeRequestId) -> Option<&RuntimeRequest> {
        find(&self.requests, id).or_else(|| self.base.runtime_requests.get(&id))
    }

    pub fn all_requests(&self) -> Vec<&RuntimeRequest> {
        let mut requests = self
            .base
            .runtime_requests
            .values()
            .map(|request| find(&self.requests, request.id).unwrap_or(request))
            .collect::<Vec<_>>();
        requests.extend(
            self.requests
                .iter()
                .filter(|request| !self.base.runtime_requests.contains_key(&request.id)),
        );
        requests
    }

    /// A message of a non-terminal run (the only ones `decide` may need).
    pub fn message(&self, id: MessageId) -> Option<&Message> {
        find(&self.messages, id).or_else(|| self.base.hot_messages.get(&id))
    }

    pub fn scope(&self, id: CheckpointScopeId) -> Option<&CheckpointScope> {
        find(&self.scopes, id).or_else(|| self.base.checkpoint_scopes.get(&id))
    }

    pub fn checkpoint(&self, id: CheckpointId) -> Option<&Checkpoint> {
        find(&self.checkpoints, id).or_else(|| self.base.checkpoints.get(&id))
    }

    pub fn checkpoints_in_scope(&self, scope_id: CheckpointScopeId) -> Vec<&Checkpoint> {
        let mut checkpoints = self
            .base
            .checkpoints
            .values()
            .filter(|checkpoint| checkpoint.scope_id == scope_id)
            .map(|checkpoint| find(&self.checkpoints, checkpoint.id).unwrap_or(checkpoint))
            .collect::<Vec<_>>();
        checkpoints.extend(self.checkpoints.iter().filter(|checkpoint| {
            checkpoint.scope_id == scope_id && !self.base.checkpoints.contains_key(&checkpoint.id)
        }));
        checkpoints.sort_by_key(|checkpoint| checkpoint.ordinal_within_scope);
        checkpoints
    }

    /// Whether a message id is already used by a known message or run.
    ///
    /// A message of a non-terminal run is hot; a message that started a run
    /// is found through the run id derived from it. (A steer message of a
    /// finished run is in neither; clients never reuse message ids.)
    pub fn message_id_used(&self, id: MessageId) -> bool {
        self.message(id).is_some() || self.run(RunId(self.derived("run", id))).is_some()
    }

    // -- writes ---------------------------------------------------------------

    pub fn put_run(&mut self, run: Run) {
        put(&mut self.runs, run);
    }

    pub fn put_attempt(&mut self, attempt: RunAttempt) {
        put(&mut self.attempts, attempt);
    }

    pub fn put_node(&mut self, node: ExecutionNode) {
        put(&mut self.nodes, node);
    }

    pub fn put_provider_session(&mut self, session: ProviderSession) {
        put(&mut self.provider_sessions, session);
    }

    pub fn put_provider_thread(&mut self, provider_thread: ProviderThread) {
        put(&mut self.provider_threads, provider_thread);
    }

    pub fn put_subagent(&mut self, subagent: Subagent) {
        put(&mut self.subagents, subagent);
    }

    pub fn put_provider_turn(&mut self, turn: ProviderTurn) {
        put(&mut self.provider_turns, turn);
    }

    pub fn put_plan(&mut self, plan: Plan) {
        put(&mut self.plans, plan);
    }

    pub fn put_request(&mut self, request: RuntimeRequest) {
        put(&mut self.requests, request);
    }

    pub fn put_message(&mut self, message: Message) {
        put(&mut self.messages, message);
    }

    pub fn put_scope(&mut self, scope: CheckpointScope) {
        put(&mut self.scopes, scope);
    }

    pub fn put_checkpoint(&mut self, checkpoint: Checkpoint) {
        put(&mut self.checkpoints, checkpoint);
    }

    pub fn put_item(&mut self, item: TurnItem) {
        put(&mut self.items, item);
    }

    pub fn effect(&mut self, effect: Effect) {
        self.effects.push(effect);
    }

    pub fn cancel(&mut self, effect_types: &[EffectType], reason: &str) {
        self.cancels.push(CancelUnsettled {
            thread_id: self.thread_id(),
            effect_types: effect_types.to_vec(),
            reason: reason.to_owned(),
        });
    }

    /// Assigns the next position in a run block (R15).
    pub fn alloc_position(&mut self, block: u32) -> u64 {
        let next = self
            .positions
            .get(&block)
            .copied()
            .or_else(|| self.base.next_position.get(&block).copied())
            .unwrap_or(0);
        self.positions.insert(block, next + 1);
        TurnItem::position_of(block, next)
    }

    /// A new turn item with the common fields filled in.
    pub fn new_item(
        &mut self,
        id: TurnItemId,
        run_id: Option<RunId>,
        block: u32,
        status: ItemStatus,
        kind: TurnItemKind,
    ) -> TurnItem {
        let position = self.alloc_position(block);
        let run = run_id.and_then(|run_id| self.run(run_id));
        TurnItem {
            id,
            thread_id: self.thread_id(),
            run_id,
            node_id: run.and_then(|run| run.root_node_id),
            provider_thread_id: run.and_then(|run| run.provider_thread_id),
            provider_turn_id: None,
            native_item_ref: None,
            parent_item_id: None,
            position,
            status,
            title: None,
            tool: None,
            stream_rev: 0,
            started_at: Some(self.now),
            completed_at: status.is_terminal().then_some(self.now),
            updated_at: self.now,
            kind,
        }
    }

    /// Emits one event per changed entity, in dependency order.
    pub fn finish(self) -> Decision {
        let mut events = Vec::new();
        if self.thread_dirty {
            events.push(DomainEvent::ThreadUpdated(ThreadUpdate {
                change: self.thread_change.unwrap_or(ThreadChange::Activity),
                thread: self.thread,
            }));
        }
        events.extend(
            self.provider_sessions
                .into_iter()
                .map(DomainEvent::ProviderSessionUpdated),
        );
        events.extend(
            self.provider_threads
                .into_iter()
                .map(DomainEvent::ProviderThreadUpdated),
        );
        events.extend(
            self.provider_turns
                .into_iter()
                .map(DomainEvent::ProviderTurnUpdated),
        );
        events.extend(self.runs.into_iter().map(DomainEvent::RunUpdated));
        events.extend(
            self.attempts
                .into_iter()
                .map(DomainEvent::RunAttemptUpdated),
        );
        events.extend(self.nodes.into_iter().map(DomainEvent::NodeUpdated));
        events.extend(self.subagents.into_iter().map(DomainEvent::SubagentUpdated));
        events.extend(
            self.scopes
                .into_iter()
                .map(DomainEvent::CheckpointScopeUpdated),
        );
        events.extend(
            self.checkpoints
                .into_iter()
                .map(DomainEvent::CheckpointUpdated),
        );
        events.extend(
            self.requests
                .into_iter()
                .map(DomainEvent::RuntimeRequestUpdated),
        );
        events.extend(self.plans.into_iter().map(DomainEvent::PlanUpdated));
        events.extend(self.messages.into_iter().map(DomainEvent::MessageUpdated));
        events.extend(self.items.into_iter().map(DomainEvent::TurnItemUpdated));
        Decision {
            events,
            effects: self.effects,
            cancel_unsettled: self.cancels,
        }
    }
}
