//! `Orchestrator`: receipt lookup, per-key locks, the thread cache, `decide`
//! and the sink, wired together (RFC 0001 "Public APIs").

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::lock_api::ArcMutexGuard;
use parking_lot::{Mutex, RawMutex, RwLock};
use smallvec::SmallVec;
use waku_protocol::orchestration::*;

use crate::clock::Clock;
use crate::command::{AnyCommand, CommandError, InternalCommand, LockKey};
use crate::decide::{CapabilitySource, DecideContext, Decision, OrchestratorSettings, decide};
use crate::effects::worker::WorkerConfig;
use crate::feed::{CommittedFeed, DEFAULT_FEED_CAPACITY, FeedPublisher};
use crate::ids::IdSource;
use crate::projection::maintenance::{self, REBUILD_PAGE, VerifyReport};
use crate::projection::{ThreadProjection, sql};
use crate::store::receipts::{self, Receipt};
use crate::store::sink::{CommitContext, CommitResult, EventSink, RejectionOutcome, WriteGuard};
use crate::store::{GRAPH_SCHEMA_VERSION, ReadPool, Store, StoreError, StoreResult, max_sequence};

pub struct OrchestratorConfig {
    /// `<data dir>/state.sqlite`.
    pub database_path: PathBuf,
    pub read_connections: usize,
    /// Hot threads kept in memory (R10).
    pub cache_threads: usize,
    pub workers: WorkerConfig,
    pub settings: OrchestratorSettings,
}

impl OrchestratorConfig {
    pub fn new(database_path: impl Into<PathBuf>) -> Self {
        Self {
            database_path: database_path.into(),
            read_connections: 4,
            cache_threads: 64,
            workers: WorkerConfig::default(),
            settings: OrchestratorSettings::default(),
        }
    }
}

pub struct Dependencies {
    pub clock: Arc<dyn Clock>,
    pub ids: Box<dyn IdSource>,
    pub capabilities: Arc<dyn CapabilitySource>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DispatchReceipt {
    /// The last committed sequence of the command (or the log's head when it
    /// committed no events).
    pub sequence: Sequence,
    /// The command id had already been dispatched; nothing new happened.
    pub duplicate: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DispatchError {
    Rejected(CommandError),
    Storage(String),
    ShuttingDown,
}

impl fmt::Display for DispatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(error) => write!(formatter, "rejected: {error}"),
            Self::Storage(error) => write!(formatter, "storage: {error}"),
            Self::ShuttingDown => formatter.write_str("orchestrator is shutting down"),
        }
    }
}

impl std::error::Error for DispatchError {}

impl From<StoreError> for DispatchError {
    fn from(error: StoreError) -> Self {
        Self::Storage(error.to_string())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IngestOutcome {
    Committed(Sequence),
    /// The write guard failed; the decision was dropped.
    Stale,
    Noop,
}

pub type EffectWaker = Arc<dyn Fn(&CommitResult) + Send + Sync>;

// -- keyed locks ---------------------------------------------------------------

/// Per-key mutexes, created on demand and dropped when unused.
#[derive(Default)]
pub struct KeyedLocks {
    map: Mutex<HashMap<LockKey, Arc<Mutex<()>>>>,
}

pub struct KeyGuards<'a> {
    locks: &'a KeyedLocks,
    held: SmallVec<[(LockKey, ArcMutexGuard<RawMutex, ()>); 2]>,
}

impl KeyedLocks {
    /// Locks `keys` in sorted order, so two commands never deadlock.
    pub fn lock(&self, keys: &[LockKey]) -> KeyGuards<'_> {
        let mut sorted = SmallVec::<[LockKey; 2]>::from_slice(keys);
        sorted.sort();
        sorted.dedup();
        let mut held = SmallVec::new();
        for key in sorted {
            let mutex = self.map.lock().entry(key).or_default().clone();
            held.push((key, mutex.lock_arc()));
        }
        KeyGuards { locks: self, held }
    }

    /// Keys currently tracked (held or being acquired).
    pub fn len(&self) -> usize {
        self.map.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Drop for KeyGuards<'_> {
    fn drop(&mut self) {
        for (key, guard) in self.held.drain(..) {
            drop(guard);
            let mut map = self.locks.map.lock();
            if map
                .get(&key)
                .is_some_and(|mutex| Arc::strong_count(mutex) == 1)
            {
                map.remove(&key);
            }
        }
    }
}

// -- thread cache --------------------------------------------------------------

/// A bounded LRU of hot thread projections (R10).
struct ThreadCache {
    inner: Mutex<CacheInner>,
}

struct CacheInner {
    capacity: usize,
    tick: u64,
    entries: HashMap<ThreadId, (Arc<ThreadProjection>, u64)>,
}

impl ThreadCache {
    fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(CacheInner {
                capacity: capacity.max(1),
                tick: 0,
                entries: HashMap::new(),
            }),
        }
    }

    fn get(&self, id: ThreadId) -> Option<Arc<ThreadProjection>> {
        let mut inner = self.inner.lock();
        inner.tick += 1;
        let tick = inner.tick;
        inner.entries.get_mut(&id).map(|(projection, stamp)| {
            *stamp = tick;
            projection.clone()
        })
    }

    fn take(&self, id: ThreadId) -> Option<Arc<ThreadProjection>> {
        self.inner
            .lock()
            .entries
            .remove(&id)
            .map(|(projection, _)| projection)
    }

    fn insert(&self, projection: Arc<ThreadProjection>) {
        let mut inner = self.inner.lock();
        inner.tick += 1;
        let tick = inner.tick;
        inner.entries.insert(projection.id(), (projection, tick));
        while inner.entries.len() > inner.capacity {
            let Some(oldest) = inner
                .entries
                .iter()
                .min_by_key(|(_, (_, stamp))| *stamp)
                .map(|(id, _)| *id)
            else {
                break;
            };
            inner.entries.remove(&oldest);
        }
    }

    fn evict(&self, id: ThreadId) {
        self.inner.lock().entries.remove(&id);
    }

    fn clear(&self) {
        self.inner.lock().entries.clear();
    }

    fn len(&self) -> usize {
        self.inner.lock().entries.len()
    }
}

// -- orchestrator --------------------------------------------------------------

pub struct Orchestrator {
    store: Arc<Store>,
    sink: EventSink,
    feed: Arc<FeedPublisher>,
    ids: Arc<Mutex<Box<dyn IdSource>>>,
    clock: Arc<dyn Clock>,
    capabilities: Arc<dyn CapabilitySource>,
    settings: OrchestratorSettings,
    workers: WorkerConfig,
    locks: KeyedLocks,
    cache: ThreadCache,
    projects: RwLock<HashMap<ProjectId, Project>>,
    last_sequence: AtomicU64,
    waker: RwLock<Option<EffectWaker>>,
    shutting_down: AtomicBool,
    /// Imported threads whose transcript is still in the legacy `app.db`
    /// (#63); imported lazily by [`Orchestrator::ensure_transcript`].
    pending_transcripts: Mutex<HashSet<ThreadId>>,
    /// Serializes lazy transcript imports.
    transcript_import: Mutex<()>,
}

impl fmt::Debug for Orchestrator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Orchestrator")
            .field("store", &self.store)
            .field("last_sequence", &self.last_sequence())
            .finish_non_exhaustive()
    }
}

fn receipt_result(receipt: Receipt) -> Result<DispatchReceipt, DispatchError> {
    match receipt.error {
        Some(error) => Err(DispatchError::Rejected(error)),
        None => Ok(DispatchReceipt {
            sequence: receipt.result_sequence,
            duplicate: true,
        }),
    }
}

/// The project a command reads, besides its thread's.
fn command_project(command: &AnyCommand) -> Option<ProjectId> {
    match command {
        AnyCommand::Client(envelope) => match &envelope.command {
            Command::ThreadCreate { project_id, .. } => Some(*project_id),
            _ => None,
        },
        AnyCommand::Internal {
            command: InternalCommand::ImportLegacyThread { shell },
            ..
        } => Some(shell.thread.project_id),
        AnyCommand::Internal { .. } => None,
    }
}

impl Orchestrator {
    /// Opens `state.sqlite`: pragmas, migrations and the projection version
    /// check (a stale projection is rebuilt from the log before any command).
    pub fn open(config: OrchestratorConfig, deps: Dependencies) -> anyhow::Result<Arc<Self>> {
        let store = Arc::new(Store::open(&config.database_path, config.read_connections)?);
        {
            let mut writer = store.writer();
            let log_head = max_sequence(&writer)?;
            match maintenance::projection_state(&writer)? {
                Some((version, _)) if version < GRAPH_SCHEMA_VERSION => {
                    maintenance::rebuild(&mut writer, REBUILD_PAGE, |_| {})?;
                }
                Some((_, last)) if last < log_head => {
                    maintenance::catch_up(&mut writer, REBUILD_PAGE, |_| {})?;
                }
                Some(_) => {}
                None if log_head > 0 => {
                    maintenance::rebuild(&mut writer, REBUILD_PAGE, |_| {})?;
                }
                None => crate::store::sink::set_projection_sequence(&writer, 0, deps.clock.now())?,
            }
        }
        let projects = store
            .reader()
            .with(sql::load_projects)?
            .into_iter()
            .map(|project| (project.id, project))
            .collect();
        let last_sequence = max_sequence(&store.writer())?;
        let pending_transcripts = store
            .reader()
            .with(crate::import::legacy::pending_transcripts)?
            .into_iter()
            .collect();
        let feed = Arc::new(FeedPublisher::default());
        let ids = Arc::new(Mutex::new(deps.ids));
        Ok(Arc::new(Self {
            sink: EventSink::new(store.clone(), ids.clone(), feed.clone()),
            store,
            feed,
            ids,
            clock: deps.clock,
            capabilities: deps.capabilities,
            settings: config.settings,
            workers: config.workers,
            locks: KeyedLocks::default(),
            cache: ThreadCache::new(config.cache_threads),
            projects: RwLock::new(projects),
            last_sequence: AtomicU64::new(last_sequence),
            waker: RwLock::new(None),
            shutting_down: AtomicBool::new(false),
            pending_transcripts: Mutex::new(pending_transcripts),
            transcript_import: Mutex::new(()),
        }))
    }

    pub fn dispatch(&self, envelope: CommandEnvelope) -> Result<DispatchReceipt, DispatchError> {
        self.run(AnyCommand::Client(envelope))
    }

    pub fn dispatch_internal(
        &self,
        command_id: CommandId,
        command: InternalCommand,
    ) -> Result<DispatchReceipt, DispatchError> {
        self.run(AnyCommand::Internal {
            command_id,
            command,
        })
    }

    fn run(&self, command: AnyCommand) -> Result<DispatchReceipt, DispatchError> {
        if self.shutting_down.load(Ordering::Acquire) {
            return Err(DispatchError::ShuttingDown);
        }
        // Fast path: a known command id returns its stored outcome.
        if let Some(receipt) = self
            .store
            .reader()
            .with(|connection| receipts::lookup(connection, command.command_id()))?
        {
            return receipt_result(receipt);
        }
        let primary = command.primary_key();
        // An imported thread's history arrives before anything else
        // touches the thread (#63).
        if let LockKey::Thread(thread_id) = primary
            && !matches!(&command, AnyCommand::Internal { command, .. } if command.is_import())
        {
            self.ensure_transcript(thread_id);
        }
        let guards = self.locks.lock(&command.lock_keys());

        let thread = match primary {
            LockKey::Thread(id) => self.cached_or_load(id)?,
            LockKey::Project(_) => None,
        };
        let project_id = match primary {
            LockKey::Project(id) => Some(id),
            LockKey::Thread(_) => thread
                .as_ref()
                .map(|projection| projection.thread.project_id)
                .or_else(|| command_project(&command)),
        };
        let project = project_id.and_then(|id| self.projects.read().get(&id).cloned());
        let now = self.clock.now();
        let decided = {
            let mut ids = self.ids.lock();
            let mut cx = DecideContext {
                now,
                ids: &mut **ids,
                capabilities: &*self.capabilities,
                settings: &self.settings,
                thread: thread.as_deref(),
                related: None,
                project: project.as_ref(),
            };
            decide(&mut cx, &command)
        };
        let commit_cx = CommitContext {
            command_id: Some(command.command_id()),
            command_type: command.command_type(),
            key: primary,
            project_id,
            guard: None,
            occurred_at: now,
        };
        let decision = match decided {
            Ok(decision) => decision,
            Err(error) => {
                return match self.sink.commit_rejection(commit_cx, &error)? {
                    RejectionOutcome::Recorded(_) => Err(DispatchError::Rejected(error)),
                    RejectionOutcome::Duplicate(receipt) => receipt_result(receipt),
                };
            }
        };
        let commit = self.commit_and_apply(primary, thread, commit_cx, &decision)?;
        drop(guards);
        // Locks are released before the waker runs: workers may dispatch
        // effect results for this thread right away.
        if commit.duplicate {
            return receipt_result(commit.receipt.clone().expect("duplicate has a receipt"));
        }
        self.wake(&commit);
        Ok(DispatchReceipt {
            sequence: commit.last_sequence,
            duplicate: false,
        })
    }

    /// Commits under the held key lock and applies the events to the cache;
    /// drops the cache entry if the commit failed.
    fn commit_and_apply(
        &self,
        key: LockKey,
        thread: Option<Arc<ThreadProjection>>,
        cx: CommitContext<'_>,
        decision: &Decision,
    ) -> Result<CommitResult, StoreError> {
        let commit = match self.sink.commit(cx, decision) {
            Ok(commit) => commit,
            Err(error) => {
                if let LockKey::Thread(id) = key {
                    self.cache.evict(id);
                }
                return Err(error);
            }
        };
        if commit.duplicate {
            return Ok(commit);
        }
        self.last_sequence
            .fetch_max(commit.last_sequence, Ordering::AcqRel);
        match key {
            LockKey::Project(_) => {
                let mut projects = self.projects.write();
                for event in &commit.events {
                    if let DomainEvent::ProjectUpdated(project) = &event.event {
                        projects.insert(project.id, project.clone());
                    }
                }
            }
            LockKey::Thread(id) => {
                drop(thread);
                let mut entry = self.cache.take(id).or_else(|| {
                    commit.events.iter().find_map(|event| match &event.event {
                        DomainEvent::ThreadUpdated(update) if update.thread.id == id => {
                            Some(Arc::new(ThreadProjection::new(update.thread.clone())))
                        }
                        _ => None,
                    })
                });
                if let Some(entry) = entry.as_mut() {
                    let projection = Arc::make_mut(entry);
                    let mut reload = false;
                    for event in &commit.events {
                        reload |= projection.apply_stored(event).reload;
                    }
                    if reload {
                        return Ok(commit);
                    }
                }
                if let Some(entry) = entry {
                    self.cache.insert(entry);
                }
            }
        }
        Ok(commit)
    }

    fn cached_or_load(&self, id: ThreadId) -> Result<Option<Arc<ThreadProjection>>, DispatchError> {
        if let Some(projection) = self.cache.get(id) {
            return Ok(Some(projection));
        }
        let loaded = self
            .store
            .reader()
            .with(|connection| sql::load_thread(connection, id))?
            .map(Arc::new);
        if let Some(projection) = &loaded {
            self.cache.insert(projection.clone());
        }
        Ok(loaded)
    }

    /// Commits a provider-ingestion decision under the thread's lock, guarded
    /// by `guard`. `decide_ingest` (#44) produces the decision; this is the
    /// half of `ingest` that does not depend on adapter events.
    pub fn ingest_decision(
        &self,
        thread_id: ThreadId,
        decision: Decision,
        guard: WriteGuard,
    ) -> Result<IngestOutcome, DispatchError> {
        if self.shutting_down.load(Ordering::Acquire) {
            return Err(DispatchError::ShuttingDown);
        }
        if decision.is_empty() {
            return Ok(IngestOutcome::Noop);
        }
        let key = LockKey::Thread(thread_id);
        let guards = self.locks.lock(&[key]);
        let thread = self.cached_or_load(thread_id)?;
        let Some(project_id) = thread
            .as_ref()
            .map(|projection| projection.thread.project_id)
        else {
            return Err(DispatchError::Rejected(CommandError::not_found(
                "thread", thread_id,
            )));
        };
        let cx = CommitContext {
            command_id: None,
            command_type: "ingest",
            key,
            project_id: Some(project_id),
            guard: Some(&guard),
            occurred_at: self.clock.now(),
        };
        let commit = match self.commit_and_apply(key, thread, cx, &decision) {
            Ok(commit) => commit,
            Err(StoreError::GuardFailed) => return Ok(IngestOutcome::Stale),
            Err(error) => return Err(error.into()),
        };
        drop(guards);
        self.wake(&commit);
        Ok(IngestOutcome::Committed(commit.last_sequence))
    }

    /// A read-only snapshot of a thread (copy-on-write).
    pub fn thread(&self, id: ThreadId) -> Result<Arc<ThreadProjection>, DispatchError> {
        if let Some(projection) = self.cache.get(id) {
            return Ok(projection);
        }
        // Load under the key lock so a concurrent commit cannot be
        // overwritten by an older snapshot.
        let _guards = self.locks.lock(&[LockKey::Thread(id)]);
        self.cached_or_load(id)?
            .ok_or_else(|| DispatchError::Rejected(CommandError::not_found("thread", id)))
    }

    pub fn project(&self, id: ProjectId) -> Option<Project> {
        self.projects.read().get(&id).cloned()
    }

    pub fn projects(&self) -> Vec<Project> {
        let mut projects = self.projects.read().values().cloned().collect::<Vec<_>>();
        projects.sort_by(|left, right| left.order_key.cmp(&right.order_key));
        projects
    }

    /// Every `StoredEvent` after COMMIT, in sequence order.
    pub fn subscribe(&self) -> CommittedFeed {
        self.feed.subscribe(DEFAULT_FEED_CAPACITY)
    }

    pub fn subscribe_with_capacity(&self, capacity: usize) -> CommittedFeed {
        self.feed.subscribe(capacity)
    }

    pub fn last_sequence(&self) -> Sequence {
        self.last_sequence.load(Ordering::Acquire)
    }

    /// Read-only connections for streams, history and search.
    pub fn reader(&self) -> &ReadPool {
        self.store.reader()
    }

    /// Threads matching a search across projects (#65), on a read
    /// connection.
    pub fn search_threads(
        &self,
        query: &crate::search::SearchThreadsQuery,
    ) -> StoreResult<Vec<crate::search::ThreadSearchMatch>> {
        self.store
            .reader()
            .with(|connection| crate::search::search_threads(connection, query))
    }

    /// Every match inside one thread, loaded or not (#65). An imported
    /// thread's transcript is imported first.
    pub fn find_in_thread(
        &self,
        query: &crate::search::ThreadFindQuery,
    ) -> StoreResult<crate::search::ThreadFindResult> {
        self.ensure_transcript(query.thread_id);
        self.store
            .reader()
            .with(|connection| crate::search::find_in_thread(connection, query))
    }

    /// The turn items around a position, for jumping to a find match.
    pub fn items_around(
        &self,
        thread_id: ThreadId,
        position: u64,
        before: usize,
        after: usize,
    ) -> StoreResult<Vec<TurnItem>> {
        self.store.reader().with(|connection| {
            crate::search::items_around(connection, thread_id, position, before, after)
        })
    }

    /// Imports an imported thread's transcript from its legacy `app.db` the
    /// first time anything needs it (#63): a dispatch for the thread, a find,
    /// and the thread stream's subscribe or history page (#54 calls this).
    /// A no-op for every other thread. Imports are serialized; a dispatch for
    /// the same thread waits for its history. A source that cannot be read
    /// is recorded in `legacy_imports.error` and not retried until restart,
    /// so the thread stays usable.
    pub fn ensure_transcript(
        &self,
        thread_id: ThreadId,
    ) -> Option<crate::import::legacy::TranscriptOutcome> {
        if !self.pending_transcripts.lock().contains(&thread_id) {
            return None;
        }
        let _import = self.transcript_import.lock();
        if !self.pending_transcripts.lock().contains(&thread_id) {
            return None;
        }
        let outcome =
            crate::import::legacy::import_transcript(self, thread_id).unwrap_or_else(|error| {
                crate::import::legacy::TranscriptOutcome::Failed(error.to_string())
            });
        self.pending_transcripts.lock().remove(&thread_id);
        Some(outcome)
    }

    /// Imported threads whose transcript has not been imported yet.
    pub fn pending_transcripts(&self) -> Vec<ThreadId> {
        let mut pending = self
            .pending_transcripts
            .lock()
            .iter()
            .copied()
            .collect::<Vec<_>>();
        pending.sort();
        pending
    }

    pub(crate) fn note_pending_transcript(&self, thread_id: ThreadId) {
        self.pending_transcripts.lock().insert(thread_id);
    }

    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }

    /// The identity of the event log, for stream cursors (RFC 0003 §4).
    pub fn store_id(&self) -> uuid::Uuid {
        self.store.store_id()
    }

    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    pub fn worker_config(&self) -> &WorkerConfig {
        &self.workers
    }

    pub fn settings(&self) -> &OrchestratorSettings {
        &self.settings
    }

    /// Called after every commit that enqueued or cancelled effects.
    pub fn set_effect_waker(&self, waker: EffectWaker) {
        *self.waker.write() = Some(waker);
    }

    fn wake(&self, commit: &CommitResult) {
        if commit.enqueued.is_empty() && commit.cancelled.is_empty() {
            return;
        }
        if let Some(waker) = self.waker.read().clone() {
            waker(commit);
        }
    }

    /// Verifies the live projection against a rebuild of the log.
    pub fn verify(&self) -> StoreResult<VerifyReport> {
        maintenance::verify(&self.store)
    }

    /// Rebuilds every projection table from the log, holding the writer so
    /// no command commits meanwhile, then drops the cache.
    pub fn rebuild_projections(&self) -> StoreResult<Sequence> {
        let mut writer = self.store.writer();
        let last = maintenance::rebuild(&mut writer, REBUILD_PAGE, |_| {})?;
        drop(writer);
        self.cache.clear();
        Ok(last)
    }

    /// Hot threads currently cached.
    pub fn cached_threads(&self) -> usize {
        self.cache.len()
    }

    /// Stops accepting commands; commits in flight finish first.
    pub fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::Release);
        drop(self.store.writer());
        self.feed.close();
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Acquire)
    }
}
