//! The in-memory thread projection (R10) and its `apply`, the twin of
//! [`sql::apply_sql`].
//!
//! A `ThreadProjection` holds a thread's whole control graph plus the
//! messages and turn items of its non-terminal runs ("hot" rows). Loading it
//! from SQLite ([`sql::load_thread`]) and applying the same events live must
//! give equal values; the property test in `tests::rebuild` checks that.

pub mod maintenance;
pub mod sql;

use std::collections::{BTreeMap, BTreeSet, HashMap};

use waku_protocol::orchestration::*;

/// One thread's control graph, as `decide` reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadProjection {
    pub thread: AppThread,
    /// The last committed sequence of an event recorded for this thread.
    pub last_sequence: Sequence,
    pub runs: BTreeMap<RunId, Run>,
    pub attempts: HashMap<RunAttemptId, RunAttempt>,
    pub nodes: HashMap<NodeId, ExecutionNode>,
    pub subagents: HashMap<NodeId, Subagent>,
    pub provider_sessions: HashMap<ProviderSessionId, ProviderSession>,
    pub provider_threads: HashMap<ProviderThreadId, ProviderThread>,
    pub provider_turns: HashMap<ProviderTurnId, ProviderTurn>,
    pub runtime_requests: HashMap<RuntimeRequestId, RuntimeRequest>,
    pub plans: HashMap<PlanId, Plan>,
    pub checkpoint_scopes: HashMap<CheckpointScopeId, CheckpointScope>,
    pub checkpoints: HashMap<CheckpointId, Checkpoint>,
    pub context_transfers: HashMap<ContextTransferId, ContextTransfer>,
    pub context_handoffs: HashMap<ContextHandoffId, ContextHandoff>,
    /// Messages of non-terminal (including queued) runs.
    pub hot_messages: HashMap<MessageId, Message>,
    /// Turn items of non-terminal runs, keyed by position.
    pub hot_items: BTreeMap<u64, TurnItem>,
    /// Run block → next item index in that block (R15).
    pub next_position: HashMap<u32, u32>,
    /// Derived from `runs`: runs occupying the thread (preparing through
    /// finalizing; normally at most one).
    pub active_runs: BTreeSet<RunId>,
    /// Derived from `runs`: queued runs in delivery order. With these two
    /// indexes `decide` never scans a long run history or a long queue.
    pub queue: BTreeSet<QueueKey>,
    /// Derived from `runs`: the highest ordinal ever taken.
    pub max_ordinal: u32,
}

/// Delivery order of a queued run: position, then request time.
pub type QueueKey = (u32, Timestamp, RunId);

/// The queue key of a queued run.
pub fn queue_key(run: &Run) -> QueueKey {
    (
        run.queue_position.unwrap_or(u32::MAX),
        run.requested_at,
        run.id,
    )
}

/// What applying an event asks of the cache.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ApplyOutcome {
    /// A terminal run became non-terminal again (a retried preparation): rows
    /// dropped from the hot sets must be reloaded from SQLite.
    pub reload: bool,
}

impl ApplyOutcome {
    fn merge(&mut self, other: Self) {
        self.reload |= other.reload;
    }
}

impl ThreadProjection {
    pub fn new(thread: AppThread) -> Self {
        Self {
            thread,
            last_sequence: 0,
            runs: BTreeMap::new(),
            attempts: HashMap::new(),
            nodes: HashMap::new(),
            subagents: HashMap::new(),
            provider_sessions: HashMap::new(),
            provider_threads: HashMap::new(),
            provider_turns: HashMap::new(),
            runtime_requests: HashMap::new(),
            plans: HashMap::new(),
            checkpoint_scopes: HashMap::new(),
            checkpoints: HashMap::new(),
            context_transfers: HashMap::new(),
            context_handoffs: HashMap::new(),
            hot_messages: HashMap::new(),
            hot_items: BTreeMap::new(),
            next_position: HashMap::new(),
            active_runs: BTreeSet::new(),
            queue: BTreeSet::new(),
            max_ordinal: 0,
        }
    }

    /// Recomputes the indexes derived from `runs` (after a load).
    pub fn reindex_runs(&mut self) {
        self.active_runs = self
            .runs
            .values()
            .filter(|run| run.status.is_active())
            .map(|run| run.id)
            .collect();
        self.queue = self
            .runs
            .values()
            .filter(|run| run.status == RunStatus::Queued)
            .map(queue_key)
            .collect();
        self.max_ordinal = self
            .runs
            .values()
            .filter_map(|run| run.ordinal)
            .max()
            .unwrap_or(0);
    }

    pub fn id(&self) -> ThreadId {
        self.thread.id
    }

    /// Whether rows of `run_id` belong in the hot sets: the run is unknown
    /// here or not terminal. Rows outside runs are never hot.
    pub fn is_hot_run(&self, run_id: Option<RunId>) -> bool {
        match run_id {
            None => false,
            Some(run_id) => self
                .runs
                .get(&run_id)
                .is_none_or(|run| !run.status.is_terminal()),
        }
    }

    /// The run occupying the thread (preparing through finalizing).
    pub fn active_run(&self) -> Option<&Run> {
        self.active_runs.iter().find_map(|id| self.runs.get(id))
    }

    /// Queued runs in delivery order.
    pub fn queued_runs(&self) -> Vec<&Run> {
        self.queue
            .iter()
            .filter_map(|(_, _, id)| self.runs.get(id))
            .collect()
    }

    /// The highest run ordinal, the block of items outside runs (R15).
    pub fn latest_ordinal(&self) -> u32 {
        self.max_ordinal
    }

    /// Applies one committed event of this thread.
    pub fn apply_stored(&mut self, event: &StoredEvent) -> ApplyOutcome {
        self.last_sequence = self.last_sequence.max(event.sequence);
        self.apply(&event.event)
    }

    /// The in-memory twin of `apply_sql`: an upsert by id.
    pub fn apply(&mut self, event: &DomainEvent) -> ApplyOutcome {
        let mut outcome = ApplyOutcome::default();
        let own = self.thread.id;
        match event {
            DomainEvent::ProjectUpdated(_) | DomainEvent::Unknown => {}
            DomainEvent::ThreadUpdated(update) => {
                if update.thread.id == own {
                    self.thread = update.thread.clone();
                }
            }
            DomainEvent::RunUpdated(run) => {
                if run.thread_id != own {
                    return outcome;
                }
                let previous = self.runs.insert(run.id, run.clone());
                if let Some(ordinal) = run.ordinal {
                    self.max_ordinal = self.max_ordinal.max(ordinal);
                }
                if let Some(previous) = &previous
                    && previous.status == RunStatus::Queued
                {
                    self.queue.remove(&queue_key(previous));
                }
                if run.status == RunStatus::Queued {
                    self.queue.insert(queue_key(run));
                }
                if run.status.is_active() {
                    self.active_runs.insert(run.id);
                } else {
                    self.active_runs.remove(&run.id);
                }
                if run.status.is_terminal() {
                    let run_id = run.id;
                    self.hot_messages
                        .retain(|_, message| message.run_id != Some(run_id));
                    self.hot_items.retain(|_, item| item.run_id != Some(run_id));
                } else if previous.is_some_and(|previous| previous.status.is_terminal()) {
                    outcome.merge(ApplyOutcome { reload: true });
                }
            }
            DomainEvent::RunAttemptUpdated(attempt) => {
                self.attempts.insert(attempt.id, attempt.clone());
            }
            DomainEvent::NodeUpdated(node) => {
                if node.thread_id == own {
                    self.nodes.insert(node.id, node.clone());
                }
            }
            DomainEvent::SubagentUpdated(subagent) => {
                if subagent.thread_id == own {
                    self.subagents.insert(subagent.id, subagent.clone());
                }
            }
            DomainEvent::ProviderSessionUpdated(session) => {
                self.provider_sessions.insert(session.id, session.clone());
            }
            DomainEvent::ProviderThreadUpdated(provider_thread) => {
                if provider_thread.app_thread_id == Some(own) {
                    self.provider_threads
                        .insert(provider_thread.id, provider_thread.clone());
                } else {
                    self.provider_threads.remove(&provider_thread.id);
                }
            }
            DomainEvent::ProviderTurnUpdated(turn) => {
                self.provider_turns.insert(turn.id, turn.clone());
            }
            DomainEvent::RuntimeRequestUpdated(request) => {
                if request.thread_id == own {
                    self.runtime_requests.insert(request.id, request.clone());
                }
            }
            DomainEvent::MessageUpdated(message) => {
                if message.thread_id != own {
                    return outcome;
                }
                if self.is_hot_run(message.run_id) {
                    self.hot_messages.insert(message.id, message.clone());
                } else {
                    self.hot_messages.remove(&message.id);
                }
            }
            DomainEvent::PlanUpdated(plan) => {
                if plan.thread_id == own {
                    self.plans.insert(plan.id, plan.clone());
                }
            }
            DomainEvent::TurnItemUpdated(item) => {
                if item.thread_id != own {
                    return outcome;
                }
                let next = self.next_position.entry(item.block()).or_insert(0);
                *next = (*next).max(item.index_in_block() + 1);
                if self.is_hot_run(item.run_id) {
                    self.hot_items.insert(item.position, item.clone());
                } else {
                    self.hot_items.remove(&item.position);
                }
            }
            DomainEvent::CheckpointScopeUpdated(scope) => {
                if scope.thread_id == own {
                    self.checkpoint_scopes.insert(scope.id, scope.clone());
                }
            }
            DomainEvent::CheckpointUpdated(checkpoint) => {
                if checkpoint.thread_id == own {
                    self.checkpoints.insert(checkpoint.id, checkpoint.clone());
                }
            }
            DomainEvent::ContextTransferUpdated(transfer) => {
                if transfer.source_thread_id == own || transfer.target_thread_id == own {
                    self.context_transfers.insert(transfer.id, transfer.clone());
                }
            }
            DomainEvent::ContextHandoffUpdated(handoff) => {
                if handoff.thread_id == own {
                    self.context_handoffs.insert(handoff.id, handoff.clone());
                }
            }
        }
        outcome
    }
}
