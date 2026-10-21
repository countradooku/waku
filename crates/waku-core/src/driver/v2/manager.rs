//! `ProviderSessionManager`: opens and resumes driver sessions, runs the
//! orchestrator's provider-control effects through them, and feeds their
//! adapter events back into `Orchestrator::ingest` (RFC 0001 "Effects",
//! RFC 0002, #44).
//!
//! ```text
//! EffectWorkerPool ─► DefaultExecutor ─► ProviderSessionManager::execute   (worker OS thread)
//!                                          └─ runtime.block_on(start / steer / interrupt /
//!                                             restart / respond / rollback / detach)
//! DriverSession events ─► pump task ─┬─ CommandOutputDelta ─► OutputSink
//!                                    ├─ every event ─► live fan-out (subscribe)
//!                                    └─ persist throttle (R12) ─► spawn_blocking(ingest)
//! ```
//!
//! One live session per app thread (no driver serves several threads per
//! session yet). Sessions are released after 30 minutes without a turn,
//! pinned for up to 4 hours while the driver reports background work, and
//! can be unloaded per thread. Every policy here reads the session's
//! capabilities, never the provider's name.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::{Notify, broadcast};
use waku_orchestrator::command::InternalCommand;
use waku_orchestrator::decide::policy::{
    ForkPointKind, ForkStrategy, RollbackStrategy, RuntimeModeDecision, SessionTransitionAction,
    effective_runtime_mode, fork_strategy, handoff_budget, rollback_strategy, session_transition,
};
use waku_orchestrator::effects::executor::CheckpointPort;
use waku_orchestrator::{
    ClaimedEffect, DispatchError, Effect, EffectContext, EffectError, EffectExecutor,
    IngestEnvelope, IngestOutcome, Orchestrator, ThreadProjection,
};
use waku_protocol::orchestration::{self as orch, *};
use waku_protocol::v8::orchestration::DeltaChannel;

use super::coalescer::SnapshotCoalescer;
use super::*;
use super::{Attachment, RuntimeResponse};

/// Tunables of the session manager.
#[derive(Clone, Debug)]
pub struct SessionManagerConfig {
    /// Release a session after this long without a turn (30 min).
    pub idle_release_ms: i64,
    /// Keep a session with pending background work up to this long (4 h).
    pub background_pin_ms: i64,
    /// R12: persist streaming snapshots at most this often (250 ms).
    pub persist_interval_ms: i64,
    /// How long a steering restart waits for the interrupted turn's terminal.
    pub restart_wait: Duration,
}

impl Default for SessionManagerConfig {
    fn default() -> Self {
        Self {
            idle_release_ms: 30 * 60 * 1000,
            background_pin_ms: 4 * 60 * 60 * 1000,
            persist_interval_ms: 250,
            restart_wait: Duration::from_secs(60),
        }
    }
}

/// A configured provider instance and the driver that serves it.
#[derive(Clone)]
pub struct RegisteredInstance {
    pub driver: Arc<dyn DriverV2>,
    pub config: InstanceConfig,
}

/// Every configured instance. Clones share one table, so the daemon can
/// re-register instances when its settings change (binary overrides,
/// disabled providers) while the manager and `decide` read the same set; a
/// session already open keeps the configuration it opened with.
#[derive(Clone, Default)]
pub struct DriverRegistry {
    entries: Arc<parking_lot::RwLock<HashMap<ProviderInstanceId, RegisteredInstance>>>,
}

impl DriverRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, driver: Arc<dyn DriverV2>, config: InstanceConfig) {
        self.entries.write().insert(
            config.instance_id.clone(),
            RegisteredInstance { driver, config },
        );
    }

    /// Replaces every instance at once.
    pub fn replace(&self, instances: impl IntoIterator<Item = RegisteredInstance>) {
        let entries = instances
            .into_iter()
            .map(|instance| (instance.config.instance_id.clone(), instance))
            .collect();
        *self.entries.write() = entries;
    }

    pub fn get(&self, instance: &ProviderInstanceId) -> Option<RegisteredInstance> {
        self.entries.read().get(instance).cloned()
    }

    /// The configured instance ids, sorted.
    pub fn instances(&self) -> Vec<ProviderInstanceId> {
        let mut ids = self.entries.read().keys().cloned().collect::<Vec<_>>();
        ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        ids
    }
}

/// Static capabilities for `decide` before a session exists.
impl waku_orchestrator::CapabilitySource for DriverRegistry {
    fn for_instance(&self, instance: &ProviderInstanceId) -> Option<Capabilities> {
        self.get(instance)
            .map(|entry| entry.driver.capabilities(&entry.config))
    }
}

/// What the manager needs from its host.
pub struct ManagerDeps {
    pub orchestrator: Arc<Orchestrator>,
    pub registry: DriverRegistry,
    pub transports: Arc<dyn TransportFactory>,
    pub ids: Arc<dyn IdAllocator>,
    pub clock: Arc<dyn Clock>,
    pub output: Arc<dyn OutputSink>,
    /// Live text deltas of streaming items for `thread` streams (RFC 0003
    /// §5.4); `None` publishes none.
    pub deltas: Option<Arc<waku_orchestrator::streams::DeltaBus>>,
    pub raw_log: Arc<dyn RawFrameLog>,
    /// Restores files on a rollback that asks for it.
    pub checkpoints: Option<Arc<dyn CheckpointPort>>,
    /// Drivers run here (until #18, the manager's own runtime).
    pub runtime: tokio::runtime::Handle,
}

/// One adapter event as it left a driver, for live subscribers (thread
/// streams' `delta` items, account usage). Lossy: a slow subscriber lags.
#[derive(Clone, Debug)]
pub struct LiveEvent {
    pub thread_id: ThreadId,
    pub provider_session_id: ProviderSessionId,
    pub event: AdapterEvent,
}

/// How a fork was made.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ForkResult {
    /// The provider forked its thread; the target thread now points at it.
    Native {
        provider_thread_id: ProviderThreadId,
    },
    /// The target thread's first run carries a portable context handoff.
    Portable,
}

struct Live {
    thread_id: ThreadId,
    registered: RegisteredInstance,
    provider_session_id: ProviderSessionId,
    generation: u64,
    session: Arc<dyn DriverSession>,
    capabilities: Capabilities,
    entity: Mutex<ProviderSession>,
    selection: Mutex<Selection>,
    bound: Mutex<Option<ProviderThreadRef>>,
    current_run: Mutex<Option<RunRef>>,
    /// Provider turns of subagents (no run attempt); their terminals never
    /// end the session's turn.
    child_turns: Mutex<HashSet<ProviderTurnId>>,
    /// The streaming items of this session: their text so far and
    /// `stream_rev`, for live deltas.
    streams: Mutex<HashMap<TurnItemId, StreamText>>,
    last_activity: AtomicI64,
    closed: AtomicBool,
    shutdown: Shutdown,
}

impl Live {
    fn instance(&self) -> &ProviderInstanceId {
        &self.registered.config.instance_id
    }
}

/// What a streaming item's clients have been sent.
#[derive(Default)]
struct StreamText {
    text: String,
    rev: u32,
}

/// A streaming item's channel and text, if `item` is one.
fn streaming_text(item: &TurnItem) -> Option<(DeltaChannel, &str, bool)> {
    match &item.kind {
        TurnItemKind::AssistantMessage {
            text, streaming, ..
        } => Some((DeltaChannel::Text, text, *streaming)),
        TurnItemKind::Reasoning { text, streaming } => {
            Some((DeltaChannel::Reasoning, text, *streaming))
        }
        _ => None,
    }
}

pub struct ProviderSessionManager {
    deps: ManagerDeps,
    config: SessionManagerConfig,
    sessions: Mutex<HashMap<ThreadId, Arc<Live>>>,
    open_lock: tokio::sync::Mutex<()>,
    generations: AtomicU64,
    handoffs: Mutex<HashMap<ThreadId, Vec<ContextBlock>>>,
    live_events: broadcast::Sender<LiveEvent>,
    in_flight: AtomicUsize,
    settled: Mutex<HashSet<RunAttemptId>>,
    terminal_signal: Notify,
}

/// The cwd a run works in: its checkpoint scope's, else the thread's
/// worktree, else the project root.
fn run_cwd(projection: &ThreadProjection, run: &Run, project_root: Option<PathBuf>) -> PathBuf {
    run.root_node_id
        .and_then(|node| projection.nodes.get(&node))
        .and_then(|node| node.checkpoint_scope_id)
        .and_then(|scope| projection.checkpoint_scopes.get(&scope))
        .map(|scope| scope.cwd.clone())
        .filter(|cwd| !cwd.as_os_str().is_empty())
        .or_else(|| projection.thread.worktree_path.clone())
        .or(project_root)
        .unwrap_or_default()
}

fn retryable(error: DispatchError) -> EffectError {
    match error {
        DispatchError::Storage(error) => EffectError::Retryable(error),
        DispatchError::ShuttingDown => EffectError::Cancelled,
        DispatchError::Rejected(error) => EffectError::Permanent(error.to_string()),
    }
}

impl ProviderSessionManager {
    pub fn new(deps: ManagerDeps, config: SessionManagerConfig) -> Arc<Self> {
        let (live_events, _) = broadcast::channel(4096);
        Arc::new(Self {
            deps,
            config,
            sessions: Mutex::new(HashMap::new()),
            open_lock: tokio::sync::Mutex::new(()),
            generations: AtomicU64::new(0),
            handoffs: Mutex::new(HashMap::new()),
            live_events,
            in_flight: AtomicUsize::new(0),
            settled: Mutex::new(HashSet::new()),
            terminal_signal: Notify::new(),
        })
    }

    pub fn orchestrator(&self) -> &Arc<Orchestrator> {
        &self.deps.orchestrator
    }

    pub fn registry(&self) -> &DriverRegistry {
        &self.deps.registry
    }

    /// Every adapter event as it leaves a driver.
    pub fn subscribe(&self) -> broadcast::Receiver<LiveEvent> {
        self.live_events.subscribe()
    }

    /// No effect and no event is being processed right now.
    pub fn is_idle(&self) -> bool {
        self.in_flight.load(Ordering::Acquire) == 0
    }

    /// The live provider session of a thread, if any.
    pub fn live_session(&self, thread_id: ThreadId) -> Option<ProviderSessionId> {
        self.sessions
            .lock()
            .get(&thread_id)
            .map(|live| live.provider_session_id)
    }

    /// The negotiated capabilities of a thread's live session.
    pub fn session_capabilities(&self, thread_id: ThreadId) -> Option<Capabilities> {
        self.sessions
            .lock()
            .get(&thread_id)
            .map(|live| live.capabilities.clone())
    }

    /// Context blocks waiting for a thread's next turn.
    pub fn pending_handoff(&self, thread_id: ThreadId) -> Vec<ContextBlock> {
        self.handoffs
            .lock()
            .get(&thread_id)
            .cloned()
            .unwrap_or_default()
    }

    fn ingest(&self, envelope: IngestEnvelope) -> Option<IngestOutcome> {
        let kind = envelope.event.kind();
        match self.deps.orchestrator.ingest(envelope) {
            Ok(outcome) => Some(outcome),
            Err(error) => {
                self.deps.raw_log.record(
                    "manager",
                    FrameDirection::Note,
                    &format!("ingest {kind} failed: {error}"),
                );
                None
            }
        }
    }

    fn by_session(&self, provider_session_id: ProviderSessionId) -> Option<Arc<Live>> {
        self.sessions
            .lock()
            .values()
            .find(|live| live.provider_session_id == provider_session_id)
            .cloned()
    }

    fn touch(&self, live: &Live) {
        live.last_activity
            .store(self.deps.clock.now(), Ordering::Release);
    }

    // -- sessions ---------------------------------------------------------------

    /// The thread's live session for `registered`, opened (or reopened with a
    /// selection transition) as needed.
    async fn ensure_live(
        self: &Arc<Self>,
        thread_id: ThreadId,
        registered: &RegisteredInstance,
        cwd: PathBuf,
        selection: &Selection,
    ) -> DriverResult<Arc<Live>> {
        let _open = self.open_lock.lock().await;
        let existing = self.sessions.lock().get(&thread_id).cloned();
        if let Some(live) = existing {
            if live.instance() != &registered.config.instance_id
                || live.closed.load(Ordering::Acquire)
            {
                self.close_live(&live, "provider changed").await;
            } else {
                let from = live.selection.lock().clone();
                if &from == selection {
                    return Ok(live);
                }
                let plan = registered.driver.plan_selection_transition(
                    &live.capabilities,
                    &from,
                    selection,
                );
                match session_transition(&live.capabilities, &plan) {
                    SessionTransitionAction::Reuse => {
                        *live.selection.lock() = selection.clone();
                        return Ok(live);
                    }
                    SessionTransitionAction::SwitchInSession => {
                        if live.session.apply_selection(selection).await.is_ok() {
                            *live.selection.lock() = selection.clone();
                            return Ok(live);
                        }
                        self.close_live(&live, "selection changed").await;
                    }
                    // A handoff to a new provider thread arrives with the
                    // provider-switch work (Phase 3); until then the session
                    // restarts and resumes the same native thread.
                    SessionTransitionAction::RestartAndResume
                    | SessionTransitionAction::NewWithHandoff => {
                        self.close_live(&live, "selection changed").await;
                    }
                    SessionTransitionAction::Reject { reason } => {
                        return Err(DriverError::Provider(ProviderFailure::new(
                            FailureClass::ValidationError,
                            reason,
                        )));
                    }
                }
            }
        }
        self.open_live(thread_id, registered, cwd, selection).await
    }

    async fn open_live(
        self: &Arc<Self>,
        thread_id: ThreadId,
        registered: &RegisteredInstance,
        cwd: PathBuf,
        selection: &Selection,
    ) -> DriverResult<Arc<Live>> {
        let provider_session_id = ProviderSessionId(self.deps.ids.fresh(EntityKind::Session));
        let generation = self.generations.fetch_add(1, Ordering::AcqRel) + 1;
        let shutdown = Shutdown::new();
        let opened = registered
            .driver
            .open_session(SessionContext {
                provider_session_id,
                thread_id,
                instance: registered.config.clone(),
                cwd: cwd.clone(),
                selection: selection.clone(),
                resume: None,
                ids: self.deps.ids.clone(),
                clock: self.deps.clock.clone(),
                transports: self.deps.transports.clone(),
                mcp: None,
                output: self.deps.output.clone(),
                raw_log: self.deps.raw_log.clone(),
                shutdown: shutdown.clone(),
            })
            .await?;
        let now = self.deps.clock.now();
        let entity = ProviderSession {
            id: provider_session_id,
            driver: registered.config.provider,
            provider_instance_id: registered.config.instance_id.clone(),
            status: SessionStatus::Ready,
            cwd,
            model: selection.model.clone(),
            capabilities: opened.capabilities.clone(),
            created_at: now,
            updated_at: now,
            last_error: None,
            detached_at: None,
            detach_reason: None,
        };
        let live = Arc::new(Live {
            thread_id,
            registered: registered.clone(),
            provider_session_id,
            generation,
            session: opened.session,
            capabilities: opened.capabilities,
            entity: Mutex::new(entity.clone()),
            selection: Mutex::new(selection.clone()),
            bound: Mutex::new(None),
            current_run: Mutex::new(None),
            child_turns: Mutex::new(HashSet::new()),
            streams: Mutex::new(HashMap::new()),
            last_activity: AtomicI64::new(now),
            closed: AtomicBool::new(false),
            shutdown,
        });
        self.ingest(IngestEnvelope::for_thread(
            thread_id,
            provider_session_id,
            generation,
            AdapterEvent::SessionUpdated { session: entity },
        ));
        self.sessions.lock().insert(thread_id, live.clone());
        let manager = self.clone();
        let pump_live = live.clone();
        self.deps
            .runtime
            .spawn(async move { manager.pump(pump_live, opened.events).await });
        Ok(live)
    }

    /// Closes a session on purpose and records why.
    async fn close_live(&self, live: &Arc<Live>, reason: &str) {
        if live.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        {
            let mut sessions = self.sessions.lock();
            if sessions
                .get(&live.thread_id)
                .is_some_and(|current| Arc::ptr_eq(current, live))
            {
                sessions.remove(&live.thread_id);
            }
        }
        let _ = live.session.close().await;
        live.shutdown.cancel();
        let now = self.deps.clock.now();
        let entity = {
            let mut entity = live.entity.lock();
            entity.status = SessionStatus::Stopped;
            entity.detached_at = Some(now);
            entity.detach_reason = Some(reason.to_owned());
            entity.updated_at = now;
            entity.clone()
        };
        self.ingest(IngestEnvelope::for_thread(
            live.thread_id,
            live.provider_session_id,
            live.generation,
            AdapterEvent::SessionUpdated { session: entity },
        ));
        self.terminal_signal.notify_waiters();
    }

    /// Binds the live session to the app's provider-thread row: resumes a
    /// native thread the row names, or starts one, and records the native id.
    async fn bind_thread(
        &self,
        live: &Live,
        row: &ProviderThread,
        selection: &Selection,
        title: Option<String>,
    ) -> DriverResult<ProviderThreadRef> {
        if let Some(bound) = live.bound.lock().clone()
            && bound.provider_thread_id == row.id
        {
            return Ok(bound);
        }
        let existing = ProviderThreadRef::from_row(row);
        let reference = if existing.is_bound() {
            live.session
                .resume_thread(ResumeThread {
                    thread: existing,
                    selection: selection.clone(),
                })
                .await?
        } else {
            live.session
                .ensure_thread(EnsureThread {
                    thread_id: live.thread_id,
                    title,
                    selection: selection.clone(),
                    existing: Some(existing),
                })
                .await?
        };
        let mut updated = row.clone();
        reference.apply_to(&mut updated);
        updated.provider_session_id = Some(live.provider_session_id);
        if updated.status == ProviderThreadStatus::NotLoaded {
            updated.status = ProviderThreadStatus::Idle;
        }
        updated.updated_at = self.deps.clock.now();
        self.ingest(IngestEnvelope::for_thread(
            live.thread_id,
            live.provider_session_id,
            live.generation,
            AdapterEvent::ProviderThreadUpdated {
                thread: updated,
                retained_native_turn_ids: None,
            },
        ));
        *live.bound.lock() = Some(reference.clone());
        Ok(reference)
    }

    // -- event pump -------------------------------------------------------------

    async fn pump(self: Arc<Self>, live: Arc<Live>, mut events: AdapterEventReceiver) {
        let mut throttle = SnapshotCoalescer::new(
            self.deps.clock.clone(),
            self.config.persist_interval_ms,
            true,
        );
        let tick = Duration::from_millis(self.config.persist_interval_ms.max(1) as u64);
        loop {
            let event = if throttle.has_pending() {
                tokio::select! {
                    event = events.recv() => event,
                    () = tokio::time::sleep(tick) => {
                        let due = throttle.due();
                        self.in_flight.fetch_add(1, Ordering::AcqRel);
                        self.persist(&live, due).await;
                        self.in_flight.fetch_sub(1, Ordering::AcqRel);
                        continue;
                    }
                }
            } else {
                events.recv().await
            };
            let Some(event) = event else { break };
            self.in_flight.fetch_add(1, Ordering::AcqRel);
            self.route(&live, &mut throttle, event).await;
            self.in_flight.fetch_sub(1, Ordering::AcqRel);
        }
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        let rest = throttle.flush();
        self.persist(&live, rest).await;
        self.session_ended(&live);
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
    }

    async fn route(&self, live: &Arc<Live>, throttle: &mut SnapshotCoalescer, event: AdapterEvent) {
        let _ = self.live_events.send(LiveEvent {
            thread_id: live.thread_id,
            provider_session_id: live.provider_session_id,
            event: event.clone(),
        });
        match event {
            AdapterEvent::CommandOutputDelta {
                item,
                chunk,
                replace,
            } => {
                self.deps
                    .output
                    .command_output(live.thread_id, item, &chunk, replace);
            }
            AdapterEvent::AccountUsageUpdated { .. } => {}
            AdapterEvent::TurnTerminal { terminal } => {
                let pending = throttle.flush();
                self.persist(live, pending).await;
                let attempt = terminal.run.attempt_id;
                let child = terminal
                    .provider_turn
                    .as_ref()
                    .is_some_and(|turn| live.child_turns.lock().remove(&turn.provider_turn_id));
                self.persist(live, vec![AdapterEvent::TurnTerminal { terminal }])
                    .await;
                if child {
                    // A subagent's turn ended; the session's turn goes on.
                    return;
                }
                {
                    let mut current = live.current_run.lock();
                    if current.is_some_and(|run| run.attempt_id == attempt) {
                        *current = None;
                    }
                }
                live.streams.lock().clear();
                self.touch(live);
                self.settled.lock().insert(attempt);
                self.terminal_signal.notify_waiters();
            }
            other => {
                let mut other = self.merge_bound_thread(live, other);
                if let AdapterEvent::TurnItemUpdated { item } = &mut other {
                    self.publish_text(live, item);
                    self.store_output(item);
                }
                if let AdapterEvent::ProviderTurnUpdated { turn } = &other
                    && turn.run_attempt_id.is_none()
                {
                    live.child_turns.lock().insert(turn.id);
                }
                let released = throttle.offer(other);
                self.persist(live, released).await;
            }
        }
    }

    /// Live deltas (RFC 0001 R12, RFC 0003 §5.4): text a streaming item
    /// gained since its last snapshot goes to `thread` streams at once, and
    /// the snapshot carries `stream_rev`, the number of deltas folded in, so
    /// a client applies only the deltas newer than the snapshot it holds.
    /// Text that does not extend what was sent (a rewrite), and the text of
    /// the final snapshot, bump the revision without a delta: the snapshot
    /// follows at once, and a client that saw the gap waits for it.
    fn publish_text(&self, live: &Live, item: &mut TurnItem) {
        let Some((channel, text, streaming)) = streaming_text(item) else {
            return;
        };
        let done = !streaming || item.status.is_terminal();
        let mut streams = live.streams.lock();
        let state = streams.entry(item.id).or_default();
        if text != state.text {
            state.rev += 1;
            if !done
                && let Some(added) = text.strip_prefix(state.text.as_str())
                && let Some(bus) = &self.deps.deltas
                && bus.has_subscribers(live.thread_id)
            {
                bus.publish(waku_orchestrator::streams::LiveDelta {
                    thread_id: live.thread_id,
                    item_id: item.id,
                    channel,
                    rev: state.rev,
                    text: added.to_owned(),
                });
            }
            text.clone_into(&mut state.text);
        }
        item.stream_rev = state.rev;
        if done {
            streams.remove(&item.id);
        }
    }

    /// A finished command stores the output the sink retained (capped, R12),
    /// or its own output capped the same way.
    fn store_output(&self, item: &mut TurnItem) {
        if !item.status.is_terminal() {
            return;
        }
        let TurnItemKind::CommandExecution {
            output,
            output_truncated,
            exit_code,
            ..
        } = &mut item.kind
        else {
            return;
        };
        let stored = self.deps.output.finish(item.id, *exit_code).or_else(|| {
            output
                .as_deref()
                .map(waku_orchestrator::streams::output_hub::cap_output)
        });
        if let Some(stored) = stored {
            *output = Some(stored.text);
            *output_truncated |= stored.truncated;
        }
    }

    /// A driver's snapshot of the session's bound provider thread is partial:
    /// it reports what the driver knows (the resume cursor, the native
    /// title, the conversation head). It is merged onto the stored row, so
    /// lineage, ordinals and handoffs survive (#47).
    fn merge_bound_thread(&self, live: &Live, event: AdapterEvent) -> AdapterEvent {
        let AdapterEvent::ProviderThreadUpdated {
            thread,
            retained_native_turn_ids,
        } = event
        else {
            return event;
        };
        let bound = live
            .bound
            .lock()
            .as_ref()
            .is_some_and(|bound| bound.provider_thread_id == thread.id);
        let stored = (bound && thread.owner_node_id.is_none())
            .then(|| self.deps.orchestrator.thread(live.thread_id).ok())
            .flatten()
            .and_then(|projection| projection.provider_threads.get(&thread.id).cloned());
        let Some(mut merged) = stored else {
            return AdapterEvent::ProviderThreadUpdated {
                thread,
                retained_native_turn_ids,
            };
        };
        if let Some(incoming) = thread.native_metadata {
            let metadata = merged.native_metadata.get_or_insert_with(Default::default);
            metadata.title = incoming.title.or(metadata.title.take());
            metadata.resume_cursor = incoming.resume_cursor.or(metadata.resume_cursor.take());
            metadata.updated_at = incoming.updated_at.or(metadata.updated_at);
        }
        if thread.native_conversation_head_ref.is_some() {
            merged.native_conversation_head_ref = thread.native_conversation_head_ref;
        }
        merged.updated_at = thread.updated_at.max(merged.updated_at);
        let reference = ProviderThreadRef::from_row(&merged);
        if reference.is_bound() {
            *live.bound.lock() = Some(reference);
        }
        AdapterEvent::ProviderThreadUpdated {
            thread: merged,
            retained_native_turn_ids,
        }
    }

    /// Ingests events in order on the blocking pool (R9: the orchestrator's
    /// APIs block).
    async fn persist(&self, live: &Arc<Live>, events: Vec<AdapterEvent>) {
        for event in events {
            let envelope = self.envelope(live, event);
            let orchestrator = self.deps.orchestrator.clone();
            let raw_log = self.deps.raw_log.clone();
            let kind = envelope.event.kind();
            let result = tokio::task::spawn_blocking(move || orchestrator.ingest(envelope)).await;
            if let Ok(Err(error)) = result {
                raw_log.record(
                    "manager",
                    FrameDirection::Note,
                    &format!("ingest {kind} failed: {error}"),
                );
            }
        }
    }

    /// Scopes an event to the run it belongs to: the session's current run
    /// when the event names that run, else no run (a late frame of a
    /// finished attempt, which `decide_ingest` then drops).
    fn envelope(&self, live: &Live, event: AdapterEvent) -> IngestEnvelope {
        let current = *live.current_run.lock();
        let run_of = |run_id: Option<RunId>| -> Option<RunRef> {
            current.filter(|current| run_id.is_none_or(|run_id| run_id == current.run_id))
        };
        let scope = match &event {
            AdapterEvent::TurnTerminal { terminal } => Some(terminal.run),
            AdapterEvent::ProviderTurnUpdated { turn } => {
                current.filter(|current| turn.run_attempt_id == Some(current.attempt_id))
            }
            AdapterEvent::MessageUpdated { message } => {
                message.run_id.and_then(|id| run_of(Some(id)))
            }
            AdapterEvent::TurnItemUpdated { item } => item.run_id.and_then(|id| run_of(Some(id))),
            AdapterEvent::NodeUpdated { node } => node.run_id.and_then(|id| run_of(Some(id))),
            AdapterEvent::SubagentUpdated { subagent } => {
                subagent.run_id.and_then(|id| run_of(Some(id)))
            }
            AdapterEvent::PlanUpdated { plan } => plan.run_id.and_then(|id| run_of(Some(id))),
            AdapterEvent::RuntimeRequestUpdated { .. } => current,
            _ => None,
        };
        match scope {
            Some(run) => {
                IngestEnvelope::for_run(live.provider_session_id, live.generation, &run, event)
            }
            None => IngestEnvelope::for_thread(
                live.thread_id,
                live.provider_session_id,
                live.generation,
                event,
            ),
        }
    }

    /// The driver's event stream ended. A session nobody closed is lost: its
    /// live requests expire and the next turn reopens it.
    fn session_ended(&self, live: &Arc<Live>) {
        self.terminal_signal.notify_waiters();
        if live.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        {
            let mut sessions = self.sessions.lock();
            if sessions
                .get(&live.thread_id)
                .is_some_and(|current| Arc::ptr_eq(current, live))
            {
                sessions.remove(&live.thread_id);
            }
        }
        let command_id = CommandId::new(format!(
            "session-lost:{}:{}",
            live.provider_session_id, live.generation
        ))
        .expect("valid command id");
        let _ = self.deps.orchestrator.dispatch_internal(
            command_id,
            InternalCommand::SessionLost {
                thread_id: live.thread_id,
                provider_session_id: live.provider_session_id,
                reason: "The provider process exited.".to_owned(),
            },
        );
    }

    async fn wait_settled(&self, attempt: RunAttemptId) -> bool {
        let deadline = tokio::time::Instant::now() + self.config.restart_wait;
        loop {
            let notified = self.terminal_signal.notified();
            if self.settled.lock().contains(&attempt) {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self.settled.lock().contains(&attempt);
            }
        }
    }

    // -- effects ----------------------------------------------------------------

    async fn execute_async(
        self: &Arc<Self>,
        claimed: &ClaimedEffect,
        cx: &EffectContext<'_>,
    ) -> Result<(), EffectError> {
        let thread_id = claimed
            .thread_id
            .ok_or_else(|| EffectError::Permanent("provider effect without a thread".to_owned()))?;
        match &claimed.effect {
            Effect::ProviderTurnStart { run_id, attempt_id } => {
                self.start_attempt(thread_id, *run_id, *attempt_id, None, cx)
                    .await
            }
            Effect::ProviderTurnRestart {
                run_id,
                interrupted_attempt_id,
                next_attempt_id,
                session_transition,
            } => {
                self.restart(
                    thread_id,
                    *run_id,
                    *interrupted_attempt_id,
                    *next_attempt_id,
                    session_transition.is_some(),
                    cx,
                )
                .await
            }
            Effect::ProviderTurnSteer {
                provider_session_id,
                provider_turn_id,
                message_id,
                ..
            } => {
                self.steer(
                    thread_id,
                    *provider_session_id,
                    *provider_turn_id,
                    *message_id,
                )
                .await
            }
            Effect::ProviderTurnInterrupt {
                provider_session_id,
                provider_turn_id,
                subagent,
                ..
            } => {
                self.interrupt(
                    thread_id,
                    *provider_session_id,
                    *provider_turn_id,
                    subagent.as_ref(),
                )
                .await
            }
            Effect::RuntimeRequestRespond {
                provider_session_id,
                request_id,
                response,
            } => {
                self.respond(thread_id, *provider_session_id, *request_id, response)
                    .await
            }
            Effect::ProviderThreadRollback {
                provider_thread_id,
                checkpoint_id,
                restore_files,
                request_id,
                ..
            } => {
                self.rollback(
                    thread_id,
                    *provider_thread_id,
                    *checkpoint_id,
                    *restore_files,
                    request_id,
                    cx,
                )
                .await
            }
            Effect::ProviderSessionDetach {
                provider_session_id,
                detail,
            } => {
                if let Some(live) = self.by_session(*provider_session_id) {
                    self.close_live(&live, detail.as_deref().unwrap_or("detached"))
                        .await;
                }
                Ok(())
            }
            Effect::ProviderRuntimeContinue { source_run_id, .. } => {
                self.continue_after_restart(thread_id, *source_run_id)
            }
            // Delegated tasks are Phase 3; there is nothing to stop yet.
            Effect::DelegatedTasksStop { .. } => Ok(()),
            other => Err(EffectError::Permanent(format!(
                "{} is not a provider effect",
                other.effect_type().as_str()
            ))),
        }
    }

    /// `provider_runtime.continue` (#59): dispatches the `ContinueRun` the
    /// recovery asked for, with its stable command id. Its run's
    /// `provider_turn.start` then resumes the provider thread natively
    /// (`bind_thread`), so the conversation continues where it stopped. A
    /// rejection (the thread moved on, or the run was already continued)
    /// settles the effect.
    fn continue_after_restart(
        &self,
        thread_id: ThreadId,
        source_run_id: RunId,
    ) -> Result<(), EffectError> {
        let projection = self
            .deps
            .orchestrator
            .thread(thread_id)
            .map_err(retryable)?;
        let Some(source) = projection.runs.get(&source_run_id) else {
            return Ok(());
        };
        let (command_id, command) =
            waku_orchestrator::recovery::continuation_command(thread_id, source);
        match self
            .deps
            .orchestrator
            .dispatch_internal(command_id, command)
        {
            Ok(_) | Err(DispatchError::Rejected(_)) => Ok(()),
            Err(DispatchError::ShuttingDown) => Err(EffectError::Cancelled),
            Err(DispatchError::Storage(error)) => Err(EffectError::Retryable(error)),
        }
    }

    fn fail_attempt(
        &self,
        cx: &EffectContext<'_>,
        thread_id: ThreadId,
        run_id: RunId,
        attempt_id: RunAttemptId,
        failure: ProviderFailure,
    ) -> Result<(), EffectError> {
        cx.report(
            1,
            InternalCommand::ProviderTurnFailed {
                thread_id,
                run_id,
                attempt_id,
                failure,
            },
        )
    }

    async fn start_attempt(
        self: &Arc<Self>,
        thread_id: ThreadId,
        run_id: RunId,
        attempt_id: RunAttemptId,
        continuation_of: Option<RunId>,
        cx: &EffectContext<'_>,
    ) -> Result<(), EffectError> {
        let projection = self
            .deps
            .orchestrator
            .thread(thread_id)
            .map_err(retryable)?;
        let Some(run) = projection.runs.get(&run_id).cloned() else {
            return Ok(());
        };
        if run.status.is_terminal() || run.active_attempt_id != Some(attempt_id) {
            // The run moved on (interrupted, superseded) before it started.
            return Ok(());
        }
        let Some(attempt) = projection.attempts.get(&attempt_id).cloned() else {
            return Ok(());
        };
        let Some(row) = projection
            .provider_threads
            .get(&attempt.provider_thread_id)
            .cloned()
        else {
            return Ok(());
        };
        let fail = |failure: ProviderFailure| {
            self.fail_attempt(cx, thread_id, run_id, attempt_id, failure)
        };
        let Some(registered) = self.deps.registry.get(&attempt.provider_instance_id) else {
            return fail(
                ProviderFailure::new(
                    FailureClass::ValidationError,
                    format!(
                        "Provider instance {} is not configured.",
                        attempt.provider_instance_id
                    ),
                )
                .with_code("unknown_instance"),
            );
        };
        let static_caps = registered.driver.capabilities(&registered.config);
        let mut selection = Selection::from_model(
            &run.model_selection,
            projection.thread.runtime_mode,
            projection.thread.interaction_mode,
        );
        match effective_runtime_mode(&static_caps, selection.runtime_mode) {
            RuntimeModeDecision::Run(mode) => selection.runtime_mode = mode,
            RuntimeModeDecision::Refuse { reason } => {
                return fail(
                    ProviderFailure::new(FailureClass::ValidationError, reason)
                        .with_code("runtime_mode_unsupported"),
                );
            }
        }
        let project_root = self
            .deps
            .orchestrator
            .project(projection.thread.project_id)
            .map(|project| project.path);
        let cwd = run_cwd(&projection, &run, project_root);
        let live = match self
            .ensure_live(thread_id, &registered, cwd, &selection)
            .await
        {
            Ok(live) => live,
            Err(error) => return fail(error.to_failure()),
        };
        let thread_ref = match self
            .bind_thread(
                &live,
                &row,
                &selection,
                Some(projection.thread.title.clone()),
            )
            .await
        {
            Ok(reference) => reference,
            Err(error) => return fail(error.to_failure()),
        };
        // The initial attempt sends the run's message; a restart sends the
        // newest message of the run (the steering message).
        let message = if continuation_of.is_some() {
            projection
                .hot_messages
                .values()
                .filter(|message| {
                    message.run_id == Some(run.id) && message.role == MessageRole::User
                })
                .max_by_key(|message| (message.created_at, message.id))
                .cloned()
        } else {
            projection.hot_messages.get(&run.user_message_id).cloned()
        };
        let Some(message) = message else {
            return fail(
                ProviderFailure::new(FailureClass::Unknown, "The run's message is missing.")
                    .with_code("message_missing"),
            );
        };
        let provider_turn_ordinal = projection
            .provider_turns
            .values()
            .filter(|turn| turn.provider_thread_id == row.id && turn.run_attempt_id.is_some())
            .count() as u32
            + 1;
        let run_ref = RunRef {
            thread_id,
            run_id,
            attempt_id,
            root_node_id: attempt.root_node_id,
        };
        *live.current_run.lock() = Some(run_ref);
        self.touch(&live);
        let app_context = self.handoffs.lock().remove(&thread_id).unwrap_or_default();
        let started = live
            .session
            .start_turn(TurnStart {
                run: run_ref,
                run_ordinal: run.ordinal.unwrap_or_default(),
                provider_turn_ordinal,
                // A steering restart continues its own run; a restart
                // continuation (#59) names the run the daemon restart
                // interrupted.
                restart_continuation_of: continuation_of.or_else(|| {
                    run.continuation
                        .as_ref()
                        .filter(|continuation| continuation.kind == ContinuationKind::Restart)
                        .map(|continuation| continuation.of_run_id)
                }),
                thread: thread_ref.clone(),
                message: TurnMessage {
                    message_id: message.id,
                    text: message.text.clone(),
                    attachments: message
                        .attachments
                        .iter()
                        .map(|attachment| Attachment {
                            name: attachment.name.clone(),
                            mime: None,
                            path: attachment.path.clone(),
                            is_image: attachment.is_image,
                        })
                        .collect(),
                },
                selection,
                app_context,
                intent: TurnIntent::Prompt,
            })
            .await;
        match started {
            Ok(accepted) => {
                if !live.capabilities.turns.emits_turn_started {
                    // The orchestrator still needs the turn to start the run.
                    let reference = accepted.provider_turn.unwrap_or_else(|| ProviderTurnRef {
                        provider_turn_id: ProviderTurnId(self.deps.ids.bind_ordinal(
                            row.id.0,
                            EntityKind::ProviderTurn,
                            provider_turn_ordinal as u64,
                        )),
                        native: NativeRef {
                            id: String::new(),
                            strength: RefStrength::None,
                        },
                        ordinal: provider_turn_ordinal,
                    });
                    self.ingest(IngestEnvelope::for_run(
                        live.provider_session_id,
                        live.generation,
                        &run_ref,
                        AdapterEvent::ProviderTurnUpdated {
                            turn: ProviderTurn {
                                id: reference.provider_turn_id,
                                provider_thread_id: row.id,
                                node_id: attempt.root_node_id,
                                run_attempt_id: Some(attempt_id),
                                native_turn_ref: (!reference.native.id.is_empty())
                                    .then(|| reference.native.to_provider_ref(row.driver)),
                                ordinal: provider_turn_ordinal,
                                status: ProviderTurnStatus::Running,
                                started_at: Some(self.deps.clock.now()),
                                completed_at: None,
                                token_usage: None,
                                turn_token_usage: None,
                            },
                        },
                    ));
                }
                Ok(())
            }
            Err(error) => {
                {
                    let mut current = live.current_run.lock();
                    if current.is_some_and(|current| current.attempt_id == attempt_id) {
                        *current = None;
                    }
                }
                if matches!(error, DriverError::SessionLost { .. }) {
                    self.close_live(&live, "session lost").await;
                }
                fail(error.to_failure())
            }
        }
    }

    /// Steering without native support: interrupt the running attempt, wait
    /// for its terminal, then start the next attempt of the same run.
    async fn restart(
        self: &Arc<Self>,
        thread_id: ThreadId,
        run_id: RunId,
        interrupted: RunAttemptId,
        next: RunAttemptId,
        new_session: bool,
        cx: &EffectContext<'_>,
    ) -> Result<(), EffectError> {
        let live = self.sessions.lock().get(&thread_id).cloned();
        if let Some(live) = live {
            let current = *live.current_run.lock();
            if let Some(current) = current.filter(|current| current.attempt_id == interrupted) {
                let projection = self
                    .deps
                    .orchestrator
                    .thread(thread_id)
                    .map_err(retryable)?;
                let provider_turn = turn_ref(&projection, interrupted);
                let thread = live.bound.lock().clone();
                if let Some(thread) = thread {
                    let delivered = live
                        .session
                        .interrupt_turn(Interrupt {
                            run: current,
                            thread,
                            provider_turn,
                            restart_follows: true,
                        })
                        .await;
                    if delivered.is_ok() {
                        self.wait_settled(interrupted).await;
                    }
                }
            }
            if new_session {
                self.close_live(&live, "session transition").await;
            }
        }
        self.start_attempt(thread_id, run_id, next, Some(run_id), cx)
            .await
    }

    async fn steer(
        &self,
        thread_id: ThreadId,
        provider_session_id: ProviderSessionId,
        provider_turn_id: ProviderTurnId,
        message_id: MessageId,
    ) -> Result<(), EffectError> {
        let Some(live) = self.by_session(provider_session_id) else {
            return Ok(());
        };
        let projection = self
            .deps
            .orchestrator
            .thread(thread_id)
            .map_err(retryable)?;
        let Some(message) = projection.hot_messages.get(&message_id).cloned() else {
            return Ok(());
        };
        let current = *live.current_run.lock();
        let Some(run) = current.filter(|run| Some(run.run_id) == message.run_id) else {
            return Ok(());
        };
        let Some(thread) = live.bound.lock().clone() else {
            return Ok(());
        };
        let provider_turn = projection
            .provider_turns
            .get(&provider_turn_id)
            .map(provider_turn_ref);
        self.touch(&live);
        let steered = live
            .session
            .steer_turn(Steer {
                run,
                thread,
                provider_turn,
                message: TurnMessage {
                    message_id,
                    text: message.text,
                    attachments: Vec::new(),
                },
            })
            .await;
        if let Err(error) = steered {
            // The turn settled first; the message stays in the transcript.
            // Promoting it to a queued run is Phase 3 queue editing.
            self.deps.raw_log.record(
                "manager",
                FrameDirection::Note,
                &format!("steer rejected: {error}"),
            );
        }
        Ok(())
    }

    async fn interrupt(
        &self,
        thread_id: ThreadId,
        provider_session_id: ProviderSessionId,
        provider_turn_id: ProviderTurnId,
        subagent: Option<&waku_orchestrator::effects::SubagentTarget>,
    ) -> Result<(), EffectError> {
        let projection = self
            .deps
            .orchestrator
            .thread(thread_id)
            .map_err(retryable)?;
        let live = self.by_session(provider_session_id);
        if let Some(target) = subagent {
            if let Some(live) = live {
                let _ = live
                    .session
                    .stop_subagent(SubagentRef {
                        node_id: target.node_id,
                        native_task_id: target.native_task_id.clone(),
                    })
                    .await;
            }
            return Ok(());
        }
        let attempt = projection
            .attempts
            .values()
            .find(|attempt| attempt.provider_turn_id == Some(provider_turn_id))
            .cloned();
        let Some(attempt) = attempt else {
            return Ok(());
        };
        let run = RunRef {
            thread_id,
            run_id: attempt.run_id,
            attempt_id: attempt.id,
            root_node_id: attempt.root_node_id,
        };
        let thread = live.as_ref().and_then(|live| live.bound.lock().clone());
        match (live, thread) {
            (Some(live), Some(thread)) => {
                let delivered = live
                    .session
                    .interrupt_turn(Interrupt {
                        run,
                        thread,
                        provider_turn: turn_ref(&projection, attempt.id),
                        restart_follows: false,
                    })
                    .await;
                if delivered.is_ok() {
                    return Ok(());
                }
                self.close_live(&live, "interrupt failed").await;
                self.synthesize_interrupt(provider_session_id, run);
            }
            // The session is gone: nobody will report this turn's end, so
            // the manager does (exactly one terminal).
            _ => self.synthesize_interrupt(provider_session_id, run),
        }
        Ok(())
    }

    fn synthesize_interrupt(&self, provider_session_id: ProviderSessionId, run: RunRef) {
        self.ingest(IngestEnvelope::for_run(
            provider_session_id,
            0,
            &run,
            AdapterEvent::TurnTerminal {
                terminal: TurnTerminal {
                    run,
                    provider_turn: None,
                    outcome: TurnOutcome::Interrupted,
                    thread_disposition: ThreadDisposition::Reusable,
                    child_turns_continue: false,
                    background_pending: false,
                    summary: Some("Interrupted; the provider session had ended".to_owned()),
                },
            },
        ));
    }

    async fn respond(
        &self,
        thread_id: ThreadId,
        provider_session_id: ProviderSessionId,
        request_id: RuntimeRequestId,
        response: &orch::RuntimeResponse,
    ) -> Result<(), EffectError> {
        let Some(live) = self.by_session(provider_session_id) else {
            // A lost session expires its live requests (`SessionLost`).
            return Ok(());
        };
        let projection = self
            .deps
            .orchestrator
            .thread(thread_id)
            .map_err(retryable)?;
        let Some(request) = projection.runtime_requests.get(&request_id) else {
            return Ok(());
        };
        let native = request
            .native_request_ref
            .as_ref()
            .and_then(|reference| {
                Some(NativeRef {
                    id: reference.native_id.clone()?,
                    strength: reference.strength,
                })
            })
            .unwrap_or(NativeRef {
                id: String::new(),
                strength: RefStrength::None,
            });
        let reply = match response {
            orch::RuntimeResponse::Approval { option_id } => RuntimeReply::Decision {
                option_id: option_id.clone(),
            },
            orch::RuntimeResponse::UserInput { answers } => RuntimeReply::Answers(answers.clone()),
            orch::RuntimeResponse::Dismiss | orch::RuntimeResponse::Unknown => {
                RuntimeReply::Dismiss
            }
        };
        self.touch(&live);
        live.session
            .respond(RuntimeResponse {
                request_id,
                native,
                reply,
            })
            .await
            .map_err(|error| EffectError::Permanent(error.to_string()))
    }

    async fn rollback(
        self: &Arc<Self>,
        thread_id: ThreadId,
        provider_thread_id: ProviderThreadId,
        checkpoint_id: CheckpointId,
        restore_files: bool,
        request_id: &CommandId,
        cx: &EffectContext<'_>,
    ) -> Result<(), EffectError> {
        let failed = |message: String| {
            cx.report(
                2,
                InternalCommand::CheckpointRollbackFailed {
                    thread_id,
                    request_id: request_id.clone(),
                    message,
                },
            )
        };
        let projection = self
            .deps
            .orchestrator
            .thread(thread_id)
            .map_err(retryable)?;
        let (Some(checkpoint), Some(row)) = (
            projection.checkpoints.get(&checkpoint_id).cloned(),
            projection
                .provider_threads
                .get(&provider_thread_id)
                .cloned(),
        ) else {
            return failed("The checkpoint or provider thread is gone.".to_owned());
        };
        let target = checkpoint.app_run_ordinal.unwrap_or(0);
        let Some(registered) = self.deps.registry.get(&row.provider_instance_id) else {
            return failed(format!(
                "Provider instance {} is not configured.",
                row.provider_instance_id
            ));
        };
        let caps = self
            .session_capabilities(thread_id)
            .unwrap_or_else(|| registered.driver.capabilities(&registered.config));
        // Provider turns of this thread, in order, with their app run ordinal.
        let mut turns = projection
            .provider_turns
            .values()
            .filter(|turn| turn.provider_thread_id == row.id)
            .filter_map(|turn| {
                let attempt = projection.attempts.get(&turn.run_attempt_id?)?;
                let ordinal = projection.runs.get(&attempt.run_id)?.ordinal?;
                Some((turn.ordinal, ordinal, turn.clone()))
            })
            .collect::<Vec<_>>();
        turns.sort_by_key(|(turn_ordinal, _, _)| *turn_ordinal);
        let turns_to_remove = turns.iter().filter(|(_, run, _)| *run > target).count() as u32;
        let kept = turns
            .iter()
            .rfind(|(_, run, _)| *run <= target)
            .map(|(_, _, turn)| provider_turn_ref(turn));
        let mut snapshot = None;
        match rollback_strategy(&caps) {
            RollbackStrategy::Native if ProviderThreadRef::from_row(&row).is_bound() => {
                let selection = Selection::from_model(
                    &projection.thread.model_selection,
                    projection.thread.runtime_mode,
                    projection.thread.interaction_mode,
                );
                let cwd = projection
                    .thread
                    .worktree_path
                    .clone()
                    .or_else(|| {
                        self.deps
                            .orchestrator
                            .project(projection.thread.project_id)
                            .map(|project| project.path)
                    })
                    .unwrap_or_default();
                let live = match self
                    .ensure_live(thread_id, &registered, cwd, &selection)
                    .await
                {
                    Ok(live) => live,
                    Err(error) => return failed(error.to_string()),
                };
                let thread = match self.bind_thread(&live, &row, &selection, None).await {
                    Ok(thread) => thread,
                    Err(error) => return failed(error.to_string()),
                };
                let outcome = live
                    .session
                    .rollback(Rollback {
                        thread,
                        target: match kept.clone() {
                            Some(turn) => RollbackTarget::AfterProviderTurn(turn),
                            None => RollbackTarget::ThreadStart,
                        },
                        turns: turns
                            .iter()
                            .map(|(_, _, turn)| provider_turn_ref(turn))
                            .collect(),
                        turns_to_remove,
                    })
                    .await;
                let outcome = match outcome {
                    Ok(outcome) => outcome,
                    Err(error) => return failed(error.to_string()),
                };
                if outcome.replaced {
                    // Re-point the app thread at the replacement; the old row
                    // stays for lineage.
                    let now = self.deps.clock.now();
                    let mut replacement = ProviderThread {
                        id: outcome.thread.provider_thread_id,
                        provider_session_id: Some(live.provider_session_id),
                        native_thread_ref: None,
                        native_conversation_head_ref: None,
                        status: ProviderThreadStatus::Idle,
                        handoff_ids: Vec::new(),
                        forked_from: Some(ProviderThreadFork {
                            provider_thread_id: row.id,
                            provider_turn_id: kept.as_ref().map(|turn| turn.provider_turn_id),
                            checkpoint_id: Some(checkpoint_id),
                        }),
                        pending_background_tasks: Vec::new(),
                        native_metadata: None,
                        created_at: now,
                        updated_at: now,
                        ..row.clone()
                    };
                    outcome.thread.apply_to(&mut replacement);
                    self.ingest(IngestEnvelope::for_thread(
                        thread_id,
                        live.provider_session_id,
                        live.generation,
                        AdapterEvent::ProviderThreadUpdated {
                            thread: replacement,
                            retained_native_turn_ids: outcome.retained_native_turn_ids.clone(),
                        },
                    ));
                    *live.bound.lock() = Some(outcome.thread.clone());
                }
                snapshot = outcome.snapshot;
            }
            _ => {
                // No provider rollback: the provider thread is retired and the
                // next run starts a fresh one with a checkpoint-summary
                // handoff (marked divergent).
                let live = self.sessions.lock().get(&thread_id).cloned();
                if let Some(live) = live {
                    self.close_live(&live, "rolled back without provider support")
                        .await;
                }
                let now = self.deps.clock.now();
                self.ingest(IngestEnvelope::for_thread(
                    thread_id,
                    row.provider_session_id
                        .unwrap_or(ProviderSessionId(uuid::Uuid::nil())),
                    0,
                    AdapterEvent::ProviderThreadUpdated {
                        thread: ProviderThread {
                            status: ProviderThreadStatus::Closed,
                            updated_at: now,
                            ..row.clone()
                        },
                        retained_native_turn_ids: None,
                    },
                ));
                let block = self.portable_context(
                    &projection,
                    Some(target),
                    "rollback",
                    handoff_budget(&caps),
                );
                self.handoffs
                    .lock()
                    .entry(thread_id)
                    .or_default()
                    .push(block);
            }
        }
        if restore_files {
            let scope_cwd = projection
                .checkpoint_scopes
                .get(&checkpoint.scope_id)
                .map(|scope| scope.cwd.clone())
                .unwrap_or_default();
            if let Some(port) = &self.deps.checkpoints
                && let Err(error) = port.restore(&scope_cwd, &checkpoint.git_ref)
            {
                return failed(error.message);
            }
        }
        cx.report(
            1,
            InternalCommand::CheckpointRollbackApplied {
                thread_id,
                request_id: request_id.clone(),
                snapshot,
            },
        )
    }

    // -- thread-level operations --------------------------------------------------

    /// Forks `source` into the existing app thread `target`, after `after_run`
    /// (or at the tip). Native when the session can fork from that point;
    /// otherwise the target's first run carries a portable handoff
    /// (`ContextKind::SyntheticUser`, D-6).
    pub async fn fork_thread(
        self: &Arc<Self>,
        source: ThreadId,
        after_run: Option<RunId>,
        target: ThreadId,
    ) -> DriverResult<ForkResult> {
        let orchestrator = &self.deps.orchestrator;
        let lost = |error: DispatchError| DriverError::SessionLost {
            detail: error.to_string(),
        };
        let projection = orchestrator.thread(source).map_err(lost)?;
        let row = projection
            .thread
            .active_provider_thread_id
            .and_then(|id| projection.provider_threads.get(&id))
            .cloned();
        let instance = row
            .as_ref()
            .map(|row| row.provider_instance_id.clone())
            .unwrap_or_else(|| projection.thread.model_selection.instance.clone());
        let registered = self
            .deps
            .registry
            .get(&instance)
            .ok_or(DriverError::Unsupported { operation: "fork" })?;
        let caps = self
            .session_capabilities(source)
            .unwrap_or_else(|| registered.driver.capabilities(&registered.config));
        let after_ordinal = after_run
            .and_then(|run| projection.runs.get(&run))
            .and_then(|run| run.ordinal);
        let point = if after_ordinal.is_some() {
            ForkPointKind::AfterTurn
        } else {
            ForkPointKind::Tip
        };
        let native_row = row
            .clone()
            .filter(|row| ProviderThreadRef::from_row(row).is_bound());
        let budget = handoff_budget(&caps);
        let Some(row) = native_row.filter(|_| fork_strategy(&caps, point) == ForkStrategy::Native)
        else {
            let block = self.portable_context(&projection, after_ordinal, "fork", budget);
            self.handoffs.lock().entry(target).or_default().push(block);
            return Ok(ForkResult::Portable);
        };
        let selection = Selection::from_model(
            &projection.thread.model_selection,
            projection.thread.runtime_mode,
            projection.thread.interaction_mode,
        );
        let cwd = projection
            .thread
            .worktree_path
            .clone()
            .or_else(|| {
                orchestrator
                    .project(projection.thread.project_id)
                    .map(|project| project.path)
            })
            .unwrap_or_default();
        // `fork_in_target_session`: the target's session makes the fork from
        // the source's native ref, and the source session is not involved.
        let in_target = caps.threads.fork_in_target_session;
        let (live, thread) = if in_target {
            let live = self
                .ensure_live(target, &registered, cwd, &selection)
                .await?;
            (live, ProviderThreadRef::from_row(&row))
        } else {
            let live = self
                .ensure_live(source, &registered, cwd, &selection)
                .await?;
            let thread = self.bind_thread(&live, &row, &selection, None).await?;
            (live, thread)
        };
        let mut turns = projection
            .provider_turns
            .values()
            .filter(|turn| turn.provider_thread_id == row.id)
            .filter_map(|turn| {
                let attempt = projection.attempts.get(&turn.run_attempt_id?)?;
                let ordinal = projection.runs.get(&attempt.run_id)?.ordinal?;
                Some((turn.ordinal, ordinal, turn.clone()))
            })
            .collect::<Vec<_>>();
        turns.sort_by_key(|(turn_ordinal, _, _)| *turn_ordinal);
        let at = match after_ordinal {
            Some(after) => {
                let kept = turns
                    .iter()
                    .rfind(|(_, run, _)| *run <= after)
                    .map(|(_, _, turn)| provider_turn_ref(turn));
                match kept {
                    Some(turn) => ForkPoint::AfterProviderTurn {
                        turn,
                        turns_to_remove: turns.iter().filter(|(_, run, _)| *run > after).count()
                            as u32,
                    },
                    None => ForkPoint::Tip,
                }
            }
            None => ForkPoint::Tip,
        };
        let fork_turn = match &at {
            ForkPoint::AfterProviderTurn { turn, .. } => Some(turn.provider_turn_id),
            _ => None,
        };
        let forked = live
            .session
            .fork(ForkRequest {
                source: thread,
                at,
                target_thread_id: target,
                owner_node: None,
                selection: None,
            })
            .await?;
        let now = self.deps.clock.now();
        let mut new_row = ProviderThread {
            id: forked.provider_thread_id,
            driver: row.driver,
            provider_instance_id: row.provider_instance_id.clone(),
            provider_session_id: None,
            app_thread_id: Some(target),
            owner_node_id: None,
            native_thread_ref: None,
            native_conversation_head_ref: None,
            status: ProviderThreadStatus::NotLoaded,
            first_run_ordinal: None,
            last_run_ordinal: None,
            handoff_ids: Vec::new(),
            forked_from: Some(ProviderThreadFork {
                provider_thread_id: row.id,
                provider_turn_id: fork_turn,
                checkpoint_id: None,
            }),
            pending_background_tasks: Vec::new(),
            context_usage: None,
            goal: None,
            native_metadata: None,
            created_at: now,
            updated_at: now,
        };
        forked.apply_to(&mut new_row);
        if in_target {
            // Loaded in the session that runs its turns: no resume needed.
            new_row.provider_session_id = Some(live.provider_session_id);
            new_row.status = ProviderThreadStatus::Idle;
            *live.bound.lock() = Some(forked.clone());
        }
        self.ingest(IngestEnvelope::for_thread(
            target,
            live.provider_session_id,
            live.generation,
            AdapterEvent::ProviderThreadUpdated {
                thread: new_row,
                retained_native_turn_ids: None,
            },
        ));
        Ok(ForkResult::Native {
            provider_thread_id: forked.provider_thread_id,
        })
    }

    /// A portable transcript of `projection` up to run `through` (all runs
    /// when `None`), newest text kept when over `budget` characters.
    fn portable_context(
        &self,
        projection: &ThreadProjection,
        through: Option<u32>,
        key: &str,
        budget: usize,
    ) -> ContextBlock {
        let thread_id = projection.id();
        let messages = self.deps.orchestrator.reader().with(|connection| {
            let mut statement = connection.prepare_cached(
                "SELECT payload_json FROM p_messages WHERE thread_id = ?1
                 ORDER BY created_at, message_id",
            )?;
            let rows = statement
                .query_map([thread_id.to_string()], |row| row.get::<_, String>(0))?
                .filter_map(Result::ok)
                .filter_map(|json| serde_json::from_str::<Message>(&json).ok())
                .collect::<Vec<_>>();
            Ok::<_, rusqlite::Error>(rows)
        });
        let mut transcript = String::new();
        for message in messages.unwrap_or_default() {
            let ordinal = message
                .run_id
                .and_then(|run| projection.runs.get(&run))
                .and_then(|run| run.ordinal);
            if let (Some(through), Some(ordinal)) = (through, ordinal)
                && ordinal > through
            {
                continue;
            }
            let role = match message.role {
                MessageRole::User => "user",
                MessageRole::Assistant => "assistant",
                _ => "system",
            };
            transcript.push_str(&format!("{role}: {}\n", message.text));
        }
        if transcript.chars().count() > budget {
            let skip = transcript.chars().count() - budget;
            transcript = transcript.chars().skip(skip).collect();
        }
        // App ids never reach provider frames: the key names the purpose only.
        ContextBlock {
            key: key.to_owned(),
            kind: ContextKind::SyntheticUser,
            text: transcript,
        }
    }

    /// Compacts the thread's provider context (gated by
    /// `extras.compaction`).
    pub async fn compact(&self, thread_id: ThreadId) -> DriverResult<()> {
        let live = self
            .sessions
            .lock()
            .get(&thread_id)
            .cloned()
            .ok_or(DriverError::NotReady(NotReadyReason::Connecting))?;
        if !live.capabilities.extras.compaction {
            return Err(DriverError::Unsupported {
                operation: "compact",
            });
        }
        let thread = live
            .bound
            .lock()
            .clone()
            .ok_or(DriverError::NotReady(NotReadyReason::ThreadMissing))?;
        self.touch(&live);
        live.session.compact(&thread).await
    }

    /// Unloads a thread's provider state and releases its session.
    pub async fn unload_thread(&self, thread_id: ThreadId) -> DriverResult<()> {
        let Some(live) = self.sessions.lock().get(&thread_id).cloned() else {
            return Ok(());
        };
        let thread = live.bound.lock().clone();
        if let Some(thread) = thread {
            live.session.unload_thread(&thread).await?;
        }
        self.close_live(&live, "unloaded").await;
        Ok(())
    }

    /// Releases sessions idle for `idle_release_ms`, or up to
    /// `background_pin_ms` while the driver reports background work. Returns
    /// the released threads.
    pub async fn release_idle(&self) -> Vec<ThreadId> {
        let now = self.deps.clock.now();
        let candidates = self
            .sessions
            .lock()
            .values()
            .filter(|live| live.current_run.lock().is_none())
            .filter(|live| {
                let idle = now - live.last_activity.load(Ordering::Acquire);
                let limit = if live.session.has_pending_background_work() {
                    self.config.background_pin_ms
                } else {
                    self.config.idle_release_ms
                };
                idle >= limit
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut candidates = candidates;
        // Oldest session first, so releases are recorded in a stable order.
        candidates.sort_by_key(|live| live.generation);
        let mut released = Vec::new();
        for live in candidates {
            self.close_live(&live, "idle").await;
            released.push(live.thread_id);
        }
        released
    }

    /// Runs `release_idle` every `period` until the runtime stops.
    pub fn spawn_idle_reaper(self: &Arc<Self>, period: Duration) {
        let manager = Arc::downgrade(self);
        self.deps.runtime.spawn(async move {
            let mut interval = tokio::time::interval(period);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                let Some(manager) = manager.upgrade() else {
                    return;
                };
                manager.release_idle().await;
            }
        });
    }

    /// Closes every session (daemon shutdown).
    pub async fn shutdown(&self) {
        let mut sessions = self.sessions.lock().values().cloned().collect::<Vec<_>>();
        sessions.sort_by_key(|live| live.generation);
        for live in sessions {
            self.close_live(&live, "shutdown").await;
        }
    }
}

fn provider_turn_ref(turn: &ProviderTurn) -> ProviderTurnRef {
    ProviderTurnRef {
        provider_turn_id: turn.id,
        native: turn
            .native_turn_ref
            .as_ref()
            .and_then(|reference| {
                Some(NativeRef {
                    id: reference.native_id.clone()?,
                    strength: reference.strength,
                })
            })
            .unwrap_or(NativeRef {
                id: String::new(),
                strength: RefStrength::None,
            }),
        ordinal: turn.ordinal,
    }
}

fn turn_ref(projection: &ThreadProjection, attempt: RunAttemptId) -> Option<ProviderTurnRef> {
    projection
        .attempts
        .get(&attempt)
        .and_then(|attempt| attempt.provider_turn_id)
        .and_then(|id| projection.provider_turns.get(&id))
        .map(provider_turn_ref)
}

/// The provider half of `DefaultExecutor` (RFC 0001 amendment: the
/// `provider: Arc<dyn EffectExecutor>` seam).
pub struct ManagerExecutor(pub Arc<ProviderSessionManager>);

impl EffectExecutor for ManagerExecutor {
    fn execute(&self, claimed: &ClaimedEffect, cx: &EffectContext<'_>) -> Result<(), EffectError> {
        let manager = &self.0;
        manager.in_flight.fetch_add(1, Ordering::AcqRel);
        let result = manager
            .deps
            .runtime
            .block_on(manager.execute_async(claimed, cx));
        manager.in_flight.fetch_sub(1, Ordering::AcqRel);
        result
    }
}
