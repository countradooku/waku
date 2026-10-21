//! Orchestration over v8 (#54, #56, #57): `orchestration.dispatch`, the
//! `threads.*` reads and searches, `projects.list`, and the `shell`,
//! `thread` and `commandOutput` streams, served from an [`Orchestrator`].
//!
//! The orchestrator is the daemon's source of truth (#55):
//! [`crate::daemon_host::DaemonHost`] opens it with the effect workers, the
//! provider session manager and the lifecycle service, and hands it to the
//! endpoint. A server started without one (the bench daemon, legacy-protocol
//! tests) answers these methods `unsupported` and ends the streams the same
//! way.
//!
//! **Event hub.** One pump thread receives the orchestrator's
//! [`CommittedFeed`](waku_orchestrator::CommittedFeed) and routes each event
//! to the subscriptions of its thread, wire-projected and encoded at most
//! once per encoding ([`WireEvent`]), and to shell subscriptions as one
//! [`ShellChange`] read once. Subscriptions are registered before their
//! snapshot is read, so nothing committed in between is lost (T3
//! `LiveStreamBudget`); events at or below the snapshot's cursor are
//! dropped as duplicates. A subscription whose hub queue fills is ended with
//! `end{overflow}` and resumes from its cursor; so is every subscription
//! when the pump itself falls behind the feed.
//!
//! **Read-your-writes.** `orchestration.dispatch` replies only after the
//! hub has routed its sequence and every `thread` and `shell` stream of the
//! same connection has queued what it was routed (a barrier through each
//! stream's queue, which also flushes held tool updates).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::{Notify, mpsc, oneshot, watch};
use waku_orchestrator::checkpoints::{self, CheckpointRange, RangeError};
use waku_orchestrator::search::{SearchSource as Source, SearchThreadsQuery, ThreadFindQuery};
use waku_orchestrator::streams::coalesce::ToolCoalescer;
use waku_orchestrator::streams::shell::{
    ShellChange, ShellFilter, ShellResume, archived_page, plan_shell_resume_default, shell_change,
    shell_rows, snapshot_items, touches_shell,
};
use waku_orchestrator::streams::thread::{
    HistoryError, ReplayBudget, SnapshotOptions, ThreadResume, WindowBudget, history_page,
    plan_thread_resume, thread_snapshot, turn_item,
};
use waku_orchestrator::streams::wire::{externalize_item_images, project_event};
use waku_orchestrator::streams::{LiveDelta, LiveStreams};
use waku_orchestrator::{CommandError, DispatchError, Orchestrator};
use waku_protocol::orchestration::{
    Actor, Command, CommandEnvelope, CreationSource, DomainEvent, Project, ProjectId, RunId,
    Sequence, StoredEvent, ThreadId, TurnItemKind,
};
use waku_protocol::v8::orchestration::{
    ArchivedPage, ArchivedParams, CommandOutputItem, CommandOutputSpec, DispatchParams,
    DispatchResult, FindHit, FindSnippet, FullDiffParams, HistoryPage, HistoryPageParams,
    MAX_FIND_ITEMS_AROUND, MAX_FIND_WINDOW, MAX_SEARCH_RESULTS, SearchSource, ShellItem, ShellSpec,
    ThreadFindParams, ThreadFindResult, ThreadItem, ThreadSearchHit, ThreadSearchParams,
    ThreadSnapshot, ThreadSnapshotParams, ThreadSpec, TurnDiffParams, TurnItemParams,
    command_scopes,
};
use waku_protocol::v8::{
    ClientKind, Encoding, EndReason, Error, ErrorCode, ErrorDetails, Method, MethodResult,
    ReviewDiff, ReviewDiffSource, StreamCursor, codec,
};

use super::V8Context;
use super::connection::{Conn, StreamSink};

/// Events a subscription's hub queue holds before it is ended with
/// `overflow` (the connection's live budget applies after this).
const HUB_QUEUE: usize = 4_096;
/// Live deltas a subscription holds before dropping new ones (a gap the
/// client recovers from at the next snapshot).
const DELTA_QUEUE: usize = 1_024;
/// The shell's cursor advances at least this often while unrelated events
/// commit, so a resume rarely needs a snapshot.
const SHELL_ADVANCE: Duration = Duration::from_secs(1);
/// How long a dispatch reply waits for its events to be queued.
const READ_YOUR_WRITES_TIMEOUT: Duration = Duration::from_secs(2);

/// A committed event as `thread` streams send it: wire-projected once and
/// encoded at most once per encoding, shared by every subscriber.
pub(crate) struct WireEvent {
    pub(crate) seq: Sequence,
    pub(crate) event: DomainEvent,
    encoded: [OnceLock<Arc<[u8]>>; 2],
}

impl WireEvent {
    fn new(stored: &StoredEvent) -> Self {
        Self {
            seq: stored.sequence,
            event: project_event(&stored.event).into_owned(),
            encoded: Default::default(),
        }
    }

    fn from_parts(seq: Sequence, event: DomainEvent) -> Self {
        Self {
            seq,
            event,
            encoded: Default::default(),
        }
    }

    fn encoded(&self, encoding: Encoding) -> Arc<[u8]> {
        let slot = match encoding {
            Encoding::MessagePack => &self.encoded[0],
            Encoding::Json => &self.encoded[1],
        };
        slot.get_or_init(|| {
            codec::to_vec(
                encoding,
                &ThreadItem::Event {
                    seq: self.seq,
                    event: self.event.clone(),
                },
            )
            .expect("thread items encode")
            .into()
        })
        .clone()
    }
}

/// What the hub sends a subscription.
pub(crate) enum HubMessage {
    Event(Arc<WireEvent>),
    Shell(Arc<ShellChange>),
    /// The log reached this sequence (shell cursor keep-alive).
    Advance(Sequence),
    /// Read-your-writes: reply once everything before is queued.
    Barrier(oneshot::Sender<()>),
}

struct Registration {
    id: u64,
    sender: mpsc::Sender<HubMessage>,
    overflowed: Arc<AtomicBool>,
}

impl Registration {
    /// Hands `message` over; a full queue marks the subscription overflowed
    /// and drops it.
    fn offer(&self, message: HubMessage) -> bool {
        match self.sender.try_send(message) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.overflowed.store(true, Ordering::Release);
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }
}

#[derive(Default)]
struct Routes {
    threads: HashMap<ThreadId, Vec<Registration>>,
    shells: Vec<Registration>,
}

/// Routes committed events to `thread` and `shell` subscriptions.
pub(crate) struct EventHub {
    routes: Mutex<Routes>,
    next: AtomicU64,
    routed: watch::Sender<Sequence>,
    /// Capacity of each subscription's queue.
    queue: usize,
}

/// A subscription's place in the hub; dropping it unsubscribes.
pub(crate) struct HubSubscription {
    hub: Weak<EventHub>,
    thread: Option<ThreadId>,
    id: u64,
    pub(crate) receiver: mpsc::Receiver<HubMessage>,
    /// Weak, so the queue closes once the hub drops the registration
    /// (overflow, shutdown) and the stream sees it end.
    pub(crate) sender: mpsc::WeakSender<HubMessage>,
    overflowed: Arc<AtomicBool>,
}

impl HubSubscription {
    /// Why the hub stopped sending: overflow, or the orchestrator closed.
    pub(crate) fn end_reason(&self) -> EndReason {
        if self.overflowed.load(Ordering::Acquire) {
            EndReason::Overflow
        } else {
            EndReason::ShuttingDown
        }
    }
}

impl Drop for HubSubscription {
    fn drop(&mut self) {
        let Some(hub) = self.hub.upgrade() else {
            return;
        };
        let mut routes = hub.routes.lock();
        match self.thread {
            Some(thread) => {
                if let Some(registrations) = routes.threads.get_mut(&thread) {
                    registrations.retain(|registration| registration.id != self.id);
                    if registrations.is_empty() {
                        routes.threads.remove(&thread);
                    }
                }
            }
            None => routes
                .shells
                .retain(|registration| registration.id != self.id),
        }
    }
}

impl EventHub {
    fn new(head: Sequence) -> Self {
        Self::with_queue(head, HUB_QUEUE)
    }

    fn with_queue(head: Sequence, queue: usize) -> Self {
        Self {
            routes: Mutex::new(Routes::default()),
            next: AtomicU64::new(1),
            routed: watch::channel(head).0,
            queue,
        }
    }

    fn subscribe(self: &Arc<Self>, thread: Option<ThreadId>) -> HubSubscription {
        let (sender, receiver) = mpsc::channel(self.queue);
        let overflowed = Arc::new(AtomicBool::new(false));
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let registration = Registration {
            id,
            sender: sender.clone(),
            overflowed: overflowed.clone(),
        };
        let mut routes = self.routes.lock();
        match thread {
            Some(thread) => routes.threads.entry(thread).or_default().push(registration),
            None => routes.shells.push(registration),
        }
        HubSubscription {
            hub: Arc::downgrade(self),
            thread,
            id,
            receiver,
            sender: sender.downgrade(),
            overflowed,
        }
    }

    /// Ends every subscription with `overflow`: the pump lost events.
    fn kick_all(&self) {
        let mut routes = self.routes.lock();
        for registration in routes
            .threads
            .values()
            .flatten()
            .chain(routes.shells.iter())
        {
            registration.overflowed.store(true, Ordering::Release);
        }
        routes.threads.clear();
        routes.shells.clear();
    }

    fn route(&self, orchestrator: &Orchestrator, stored: &Arc<StoredEvent>) {
        let mut routes = self.routes.lock();
        if let Some(thread) = stored.thread_id
            && let Some(registrations) = routes.threads.get_mut(&thread)
        {
            let wire = Arc::new(WireEvent::new(stored));
            registrations
                .retain(|registration| registration.offer(HubMessage::Event(wire.clone())));
            if registrations.is_empty() {
                routes.threads.remove(&thread);
            }
        }
        if !routes.shells.is_empty() && touches_shell(&stored.event) {
            // The row as of now (at least this event's state).
            match orchestrator
                .reader()
                .with(|connection| shell_change(connection, stored))
            {
                Ok(Some(change)) => {
                    let change = Arc::new(change);
                    routes.shells.retain(|registration| {
                        registration.offer(HubMessage::Shell(change.clone()))
                    });
                }
                Ok(None) => {}
                Err(error) => {
                    eprintln!("waku-daemon could not read a shell row: {error}");
                }
            }
        }
    }

    fn advance_shells(&self, sequence: Sequence) {
        let mut routes = self.routes.lock();
        // Best effort: a full queue already has newer items coming.
        routes
            .shells
            .retain(|registration| !registration.sender.is_closed());
        for registration in &routes.shells {
            let _ = registration.sender.try_send(HubMessage::Advance(sequence));
        }
    }

    /// Waits until the hub has routed `sequence`.
    async fn routed_through(&self, sequence: Sequence) -> bool {
        let mut routed = self.routed.subscribe();
        tokio::time::timeout(
            READ_YOUR_WRITES_TIMEOUT,
            routed.wait_for(|routed| *routed >= sequence),
        )
        .await
        .is_ok_and(|result| result.is_ok())
    }
}

/// The pump: the feed to the hub, until the orchestrator shuts down.
fn pump(orchestrator: Arc<Orchestrator>, hub: Arc<EventHub>) {
    use waku_orchestrator::feed::FeedEnded;
    let mut feed = orchestrator.subscribe();
    let mut last_advance = Instant::now();
    loop {
        let first = match feed.recv() {
            Ok(event) => event,
            Err(FeedEnded::Overflowed) => {
                hub.kick_all();
                feed = orchestrator.subscribe();
                continue;
            }
            Err(FeedEnded::Closed) => break,
        };
        let mut last = first.sequence;
        hub.route(&orchestrator, &first);
        for event in feed.drain() {
            last = event.sequence;
            hub.route(&orchestrator, &event);
        }
        hub.routed.send_replace(last);
        if last_advance.elapsed() >= SHELL_ADVANCE {
            last_advance = Instant::now();
            hub.advance_shells(last);
        }
    }
    let mut routes = hub.routes.lock();
    routes.threads.clear();
    routes.shells.clear();
}

/// The orchestrator, its live streams and the event hub, as the v8 endpoint
/// serves them.
pub struct Orchestration {
    pub orchestrator: Arc<Orchestrator>,
    /// The output hub and delta bus the provider session manager feeds.
    pub live: LiveStreams,
    pub(crate) hub: Arc<EventHub>,
}

impl std::fmt::Debug for Orchestration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Orchestration")
            .field("orchestrator", &self.orchestrator)
            .finish_non_exhaustive()
    }
}

impl Orchestration {
    /// Serves `orchestrator` and starts the event pump.
    pub fn new(orchestrator: Arc<Orchestrator>, live: LiveStreams) -> Arc<Self> {
        let hub = Arc::new(EventHub::new(orchestrator.last_sequence()));
        let pump_orchestrator = orchestrator.clone();
        let pump_hub = hub.clone();
        std::thread::Builder::new()
            .name("waku-v8-hub".into())
            .spawn(move || pump(pump_orchestrator, pump_hub))
            .expect("the v8 event hub thread starts");
        Arc::new(Self {
            orchestrator,
            live,
            hub,
        })
    }

    pub fn store_id(&self) -> uuid::Uuid {
        self.orchestrator.store_id()
    }

    /// Stops the orchestrator; the pump ends and every stream with it.
    pub fn shutdown(&self) {
        self.orchestrator.shutdown();
    }
}

fn unsupported() -> Error {
    Error::new(
        ErrorCode::Unsupported,
        "orchestration is not served by this daemon",
    )
}

fn orchestration(context: &V8Context) -> Result<&Arc<Orchestration>, Error> {
    context.orchestration.as_ref().ok_or_else(unsupported)
}

fn storage(error: impl std::fmt::Display) -> Error {
    Error::new(ErrorCode::Internal, format!("{error}"))
}

fn thread_not_found(thread_id: ThreadId) -> Error {
    Error::new(ErrorCode::NotFound, "no such thread").with_details(ErrorDetails::NotFound {
        resource: "thread".into(),
        id: thread_id.to_string(),
    })
}

/// A rejected command as a call error.
fn rejected(error: CommandError) -> Error {
    let message = error.to_string();
    match error {
        CommandError::NotFound { entity, id } => Error::new(ErrorCode::NotFound, message)
            .with_details(ErrorDetails::NotFound {
                resource: entity,
                id,
            }),
        CommandError::InvalidState { .. } | CommandError::Conflict { .. } => {
            Error::new(ErrorCode::Conflict, message)
        }
        CommandError::Unsupported { capability } => Error::new(ErrorCode::Unsupported, message)
            .with_details(ErrorDetails::Capability { capability }),
        CommandError::Invalid { field, reason } => Error::new(ErrorCode::BadRequest, message)
            .with_details(ErrorDetails::Validation { field, reason }),
    }
}

fn dispatch_error(error: DispatchError) -> Error {
    match error {
        DispatchError::Rejected(error) => rejected(error),
        DispatchError::Storage(error) => storage(error),
        DispatchError::ShuttingDown => {
            Error::new(ErrorCode::NotReady, "the orchestrator is shutting down").retryable()
        }
    }
}

fn creation_source(kind: &ClientKind) -> CreationSource {
    match kind {
        ClientKind::Desktop => CreationSource::Desktop,
        ClientKind::Web => CreationSource::Web,
        ClientKind::Ios | ClientKind::Android => CreationSource::Mobile,
        _ => CreationSource::Server,
    }
}

/// `project.create_scratch` as the `project.create` it stands for: a fresh
/// folder under the projectless root (`<root>/<date>/<slug>`, named from the
/// prompt), recorded as a project. A retry of a command whose project
/// already exists reuses that project's folder, so it creates nothing and
/// hits the first dispatch's receipt.
fn scratch_project(
    orchestrator: &Orchestrator,
    project_id: ProjectId,
    name: Option<String>,
    prompt: Option<String>,
) -> Result<Command, Error> {
    let name = name
        .map(|name| name.trim().to_owned())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| waku_protocol::model::Project::PROJECTLESS_NAME.to_owned());
    let path = match orchestrator.project(project_id) {
        Some(existing) => existing.path,
        None => {
            crate::projectless::create_workspace(prompt.as_deref())
                .map_err(|error| {
                    Error::new(
                        ErrorCode::Io,
                        format!("could not create a folder for the task: {error}"),
                    )
                })?
                .cwd
        }
    };
    Ok(Command::ProjectCreate {
        project_id,
        name,
        path,
        order_key: None,
    })
}

/// `orchestration.dispatch`, with read-your-writes on `conn`.
pub(crate) async fn dispatch(
    conn: &Arc<Conn>,
    context: &Arc<V8Context>,
    params: DispatchParams,
) -> Result<DispatchResult, Error> {
    let orchestration = orchestration(context)?.clone();
    let command = params.command.0;
    for scope in command_scopes(&command) {
        if !scope.granted_by(&conn.scopes) {
            return Err(Error::new(
                ErrorCode::Forbidden,
                format!("{} needs {scope}", command.command_type()),
            )
            .with_details(ErrorDetails::MissingScope { required: scope }));
        }
    }
    let command = match command {
        Command::ProjectCreateScratch {
            project_id,
            name,
            prompt,
        } => {
            let resolving = orchestration.clone();
            tokio::task::spawn_blocking(move || {
                scratch_project(&resolving.orchestrator, project_id, name, prompt)
            })
            .await
            .map_err(|_| Error::new(ErrorCode::Internal, "the dispatch failed"))??
        }
        command => command,
    };
    let envelope = CommandEnvelope {
        command_id: params.command_id,
        issued_by: Actor::User,
        source: creation_source(&conn.client_kind),
        command,
    };
    let dispatching = orchestration.clone();
    let receipt = tokio::task::spawn_blocking(move || dispatching.orchestrator.dispatch(envelope))
        .await
        .map_err(|_| Error::new(ErrorCode::Internal, "the dispatch failed"))?
        .map_err(dispatch_error)?;
    if orchestration.hub.routed_through(receipt.sequence).await {
        for barrier in conn.barriers() {
            // A stream that already ended has nothing left to queue.
            let Some(barrier) = barrier.upgrade() else {
                continue;
            };
            let (done, wait) = oneshot::channel();
            let queued = tokio::time::timeout(READ_YOUR_WRITES_TIMEOUT, async {
                barrier.send(HubMessage::Barrier(done)).await.ok()?;
                drop(barrier);
                wait.await.ok()
            })
            .await;
            let _ = queued;
        }
    }
    Ok(DispatchResult {
        sequence: receipt.sequence,
        duplicate: receipt.duplicate,
    })
}

/// The `threads.*` reads. `None` for methods this module does not serve.
pub(crate) fn handle(context: &V8Context, method: Method) -> Option<Result<MethodResult, Error>> {
    Some(match method {
        Method::ThreadsSnapshot(params) => {
            snapshot(context, params).map(MethodResult::ThreadsSnapshot)
        }
        Method::ThreadsHistoryPage(params) => {
            history(context, params).map(MethodResult::ThreadsHistoryPage)
        }
        Method::ThreadsTurnItem(params) => {
            full_turn_item(context, params).map(MethodResult::ThreadsTurnItem)
        }
        Method::ThreadsArchived(params) => {
            archived(context, params).map(MethodResult::ThreadsArchived)
        }
        Method::ThreadsSearch(params) => search(context, params).map(MethodResult::ThreadsSearch),
        Method::ThreadsFind(params) => find(context, params).map(MethodResult::ThreadsFind),
        Method::ThreadsTurnDiff(params) => {
            turn_diff(context, params).map(MethodResult::ThreadsTurnDiff)
        }
        Method::ThreadsFullDiff(params) => {
            full_diff(context, params).map(MethodResult::ThreadsFullDiff)
        }
        Method::OrchestrationDispatch(_) => Err(Error::new(
            ErrorCode::Internal,
            "orchestration.dispatch is served by the connection",
        )),
        _ => return None,
    })
}

fn range_error(error: RangeError, run_id: Option<RunId>) -> Error {
    match error {
        RangeError::RunNotFound => Error::new(ErrorCode::NotFound, error.to_string()).with_details(
            ErrorDetails::NotFound {
                resource: "run".into(),
                id: run_id.map(|id| id.to_string()).unwrap_or_default(),
            },
        ),
        RangeError::NotCaptured | RangeError::StartUnavailable | RangeError::NoTurns => {
            Error::new(ErrorCode::Conflict, error.to_string())
        }
    }
}

/// The diff of a checkpoint range, in the `review.diff` shape.
fn checkpoint_diff(
    context: &V8Context,
    source: ReviewDiffSource,
    range: &CheckpointRange<'_>,
    ignore_whitespace: bool,
) -> Result<ReviewDiff, Error> {
    let diff = crate::workspace::collect_ref_diff(
        range.cwd,
        &range.from.git_ref,
        &range.to.git_ref,
        ignore_whitespace,
    )
    .map_err(|error| Error::new(ErrorCode::Vcs, format!("{error:#}")))?;
    super::catalog::build_review_diff(
        context,
        source,
        diff.numstat,
        diff.patch,
        diff.complete_context,
    )
}

/// `threads.turnDiff`: what one run changed, from its turn-start checkpoint
/// to the one it ended with.
fn turn_diff(context: &V8Context, params: TurnDiffParams) -> Result<ReviewDiff, Error> {
    let projection = orchestrator(context)?
        .thread(params.thread_id)
        .map_err(|_| thread_not_found(params.thread_id))?;
    let range = checkpoints::turn_range(&projection, params.run_id)
        .map_err(|error| range_error(error, Some(params.run_id)))?;
    checkpoint_diff(
        context,
        ReviewDiffSource::Turn {
            run_id: params.run_id,
        },
        &range,
        params.ignore_whitespace,
    )
}

/// `threads.fullDiff`: what the thread's live runs changed together.
fn full_diff(context: &V8Context, params: FullDiffParams) -> Result<ReviewDiff, Error> {
    let projection = orchestrator(context)?
        .thread(params.thread_id)
        .map_err(|_| thread_not_found(params.thread_id))?;
    let range = checkpoints::thread_range(&projection).map_err(|error| range_error(error, None))?;
    checkpoint_diff(
        context,
        ReviewDiffSource::Thread,
        &range,
        params.ignore_whitespace,
    )
}

fn snapshot(context: &V8Context, params: ThreadSnapshotParams) -> Result<ThreadSnapshot, Error> {
    let orchestration = orchestration(context)?;
    let store = orchestration.store_id();
    orchestration
        .orchestrator
        .reader()
        .with(|connection| {
            thread_snapshot(
                connection,
                store,
                params.thread_id,
                SnapshotOptions {
                    compact: params.compact_turn_items,
                    budget: WindowBudget::default(),
                },
            )
        })
        .map_err(storage)?
        .ok_or_else(|| thread_not_found(params.thread_id))
}

fn history(context: &V8Context, params: HistoryPageParams) -> Result<HistoryPage, Error> {
    let orchestration = orchestration(context)?;
    orchestration
        .orchestrator
        .reader()
        .with(|connection| {
            history_page(
                connection,
                params.thread_id,
                &params.cursor,
                WindowBudget::default(),
            )
        })
        .map_err(|error| match error {
            HistoryError::BadCursor(error) => Error::new(ErrorCode::BadRequest, error.to_string())
                .with_details(ErrorDetails::Validation {
                    field: "cursor".into(),
                    reason: error.0.into(),
                }),
            HistoryError::Store(error) => storage(error),
        })
}

fn full_turn_item(
    context: &V8Context,
    params: TurnItemParams,
) -> Result<waku_protocol::orchestration::TurnItem, Error> {
    let orchestration = orchestration(context)?;
    let mut item = orchestration
        .orchestrator
        .reader()
        .with(|connection| turn_item(connection, params.thread_id, params.item_id))
        .map_err(storage)?
        .ok_or_else(|| {
            Error::new(ErrorCode::NotFound, "no such turn item").with_details(
                ErrorDetails::NotFound {
                    resource: "turnItem".into(),
                    id: params.item_id.to_string(),
                },
            )
        })?;
    // Images in tool output become blob references, never data URLs.
    externalize_item_images(&mut item, &mut |bytes, mime| match context.backend.call(
        crate::protocol::Command::StoreBlob {
            mime_type: mime.to_owned(),
            bytes: bytes.to_vec(),
        },
    ) {
        Ok(crate::protocol::ResponsePayload::BlobStored { reference, .. }) => Some(reference),
        _ => None,
    });
    Ok(item)
}

fn archived(context: &V8Context, params: ArchivedParams) -> Result<ArchivedPage, Error> {
    let orchestration = orchestration(context)?;
    orchestration
        .orchestrator
        .reader()
        .with(|connection| {
            archived_page(
                connection,
                params.cursor.as_deref(),
                params.limit,
                params.project_id,
            )
        })
        .map_err(|error| match error {
            waku_orchestrator::streams::shell::ArchiveError::BadCursor(error) => {
                Error::new(ErrorCode::BadRequest, error.to_string())
            }
            waku_orchestrator::streams::shell::ArchiveError::Store(error) => storage(error),
        })
}

/// The live projects in their sidebar order (`projects.list`, and the roots
/// of project-scoped files calls).
pub(crate) fn projects(context: &V8Context) -> Result<Vec<Project>, Error> {
    let orchestration = orchestration(context)?;
    Ok(orchestration
        .orchestrator
        .projects()
        .into_iter()
        .filter(|project| project.removed_at.is_none())
        .collect())
}

fn search_source(source: Source) -> SearchSource {
    match source {
        Source::Title => SearchSource::Title,
        Source::Message => SearchSource::Message,
        Source::Plan => SearchSource::Plan,
    }
}

fn find_hit(hit: waku_orchestrator::search::FindHit) -> FindHit {
    FindHit {
        source: search_source(hit.source),
        entity_id: hit.entity_id,
        position: hit.position,
    }
}

fn index(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

/// `threads.search`.
fn search(context: &V8Context, params: ThreadSearchParams) -> Result<Vec<ThreadSearchHit>, Error> {
    let orchestration = orchestration(context)?;
    let limit = params
        .limit
        .unwrap_or(waku_orchestrator::search::DEFAULT_SEARCH_LIMIT as u32)
        .clamp(1, MAX_SEARCH_RESULTS);
    let query = SearchThreadsQuery {
        text: params.query,
        project_id: params.project_id,
        include_archived: params.include_archived,
        limit: limit as usize,
    };
    let matches = orchestration
        .orchestrator
        .search_threads(&query)
        .map_err(storage)?;
    Ok(matches
        .into_iter()
        .map(|found| ThreadSearchHit {
            thread_id: found.thread_id,
            project_id: found.project_id,
            title: found.title,
            source: search_source(found.source),
            entity_id: found.entity_id,
            position: found.position,
            snippet: found.snippet,
            activity_at: found.activity_at,
        })
        .collect())
}

/// `threads.find`: the matches, the snippets around the current one, and
/// with `itemsAround` the turn items there, wire-projected.
fn find(context: &V8Context, params: ThreadFindParams) -> Result<ThreadFindResult, Error> {
    let orchestration = orchestration(context)?;
    let orchestrator = &orchestration.orchestrator;
    if orchestrator.thread(params.thread_id).is_err() {
        return Err(thread_not_found(params.thread_id));
    }
    let window = params
        .window
        .map_or(waku_orchestrator::search::FIND_WINDOW, |window| {
            window.min(MAX_FIND_WINDOW) as usize
        });
    let query = ThreadFindQuery {
        thread_id: params.thread_id,
        text: params.query,
        anchor: params.anchor,
        window,
    };
    let found = orchestrator.find_in_thread(&query).map_err(storage)?;
    let items = match (params.items_around, found.current) {
        (Some(around), Some(current)) => {
            let position = found.matches[current].position;
            orchestrator
                .items_around(
                    params.thread_id,
                    position,
                    around.before.min(MAX_FIND_ITEMS_AROUND) as usize,
                    around.after.min(MAX_FIND_ITEMS_AROUND) as usize,
                )
                .map_err(storage)?
                .iter()
                .map(waku_orchestrator::streams::wire::project_turn_item)
                .collect()
        }
        _ => Vec::new(),
    };
    Ok(ThreadFindResult {
        current: found.current.map(index),
        truncated: found.truncated,
        window: found
            .window
            .into_iter()
            .map(|snippet| FindSnippet {
                index: index(snippet.index),
                hit: find_hit(snippet.hit),
                snippet: snippet.snippet,
            })
            .collect(),
        matches: found.matches.into_iter().map(find_hit).collect(),
        items,
    })
}

/// The orchestrator, or `unsupported` from a server composed without one.
pub(crate) fn orchestrator(context: &V8Context) -> Result<&Arc<Orchestrator>, Error> {
    Ok(&orchestration(context)?.orchestrator)
}

/// Whether `project_id` is a project that is not removed.
pub(crate) fn live_project(context: &V8Context, project_id: uuid::Uuid) -> Result<bool, Error> {
    Ok(orchestration(context)?
        .orchestrator
        .project(waku_protocol::orchestration::ProjectId(project_id))
        .is_some_and(|project| project.removed_at.is_none()))
}

/// Which of `thread_ids` are threads that exist and are not deleted. Reads
/// the thread rows, not the projections, so it stays cheap for many ids.
pub(crate) fn live_threads(
    context: &V8Context,
    thread_ids: impl IntoIterator<Item = uuid::Uuid>,
) -> Result<std::collections::HashSet<uuid::Uuid>, Error> {
    let orchestration = orchestration(context)?;
    orchestration.orchestrator.reader().with(|connection| {
        let mut statement = connection
            .prepare_cached("SELECT 1 FROM p_threads WHERE thread_id = ?1 AND deleted_at IS NULL")
            .map_err(storage)?;
        let mut live = std::collections::HashSet::new();
        for thread_id in thread_ids {
            if statement
                .exists([ThreadId(thread_id).to_string()])
                .map_err(storage)?
            {
                live.insert(thread_id);
            }
        }
        Ok(live)
    })
}

/// A thread's root for files, vcs and terminal calls: its worktree, or its
/// project's path.
pub(crate) fn thread_root(context: &V8Context, thread_id: ThreadId) -> Result<PathBuf, Error> {
    let orchestration = orchestration(context)?;
    let projection = orchestration
        .orchestrator
        .thread(thread_id)
        .map_err(|_| thread_not_found(thread_id))?;
    if let Some(path) = &projection.thread.worktree_path {
        return Ok(path.clone());
    }
    orchestration
        .orchestrator
        .project(projection.thread.project_id)
        .map(|project| project.path)
        .ok_or_else(|| thread_not_found(thread_id))
}

fn end_unsupported(sink: &StreamSink) {
    sink.end(EndReason::Error {
        error: unsupported(),
    });
}

async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> Option<T> {
    tokio::task::spawn_blocking(work).await.ok()
}

fn sequence_cursor(store: uuid::Uuid, seq: Sequence) -> StreamCursor {
    StreamCursor::Sequence { store, seq }
}

/// `thread`: a bounded snapshot or the replayed gap, `synchronized`, then
/// the thread's events (running tool updates coalesced over 50 ms) and live
/// deltas.
pub(crate) async fn thread_stream(
    sink: StreamSink,
    context: Arc<V8Context>,
    spec: ThreadSpec,
    after: Option<StreamCursor>,
) {
    let Some(orchestration) = context.orchestration.clone() else {
        end_unsupported(&sink);
        return;
    };
    let store = orchestration.store_id();
    let thread_id = spec.thread_id;
    // Live first, then the high-water mark: nothing falls in between.
    let mut subscription = orchestration.hub.subscribe(Some(thread_id));
    sink.register_barrier(subscription.sender.clone());
    let (delta_sender, mut deltas) = mpsc::channel::<Arc<LiveDelta>>(DELTA_QUEUE);
    let _deltas = orchestration.live.deltas.subscribe(
        thread_id,
        Box::new(move |delta| match delta_sender.try_send(delta.clone()) {
            // A dropped delta is a gap the client skips until the next
            // snapshot of the item.
            Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => true,
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }),
    );

    let encoding = sink.encoding();
    let reader = orchestration.clone();
    let start = blocking(move || {
        reader.orchestrator.reader().with(|connection| {
            if let Some(after) = &after {
                match plan_thread_resume(
                    connection,
                    store,
                    thread_id,
                    after,
                    ReplayBudget::default(),
                )? {
                    ThreadResume::Replay { events, head } => return Ok(Err((events, head))),
                    ThreadResume::Snapshot => {}
                }
            }
            thread_snapshot(
                connection,
                store,
                thread_id,
                SnapshotOptions {
                    compact: spec.compact_turn_items,
                    budget: WindowBudget::default(),
                },
            )
            .map(Ok)
        })
    })
    .await;
    let mut cursor = match start {
        Some(Ok(Ok(Some(snapshot)))) => {
            let StreamCursor::Sequence { seq, .. } = snapshot.cursor else {
                return;
            };
            let cursor = snapshot.cursor.clone();
            if sink
                .push(&ThreadItem::Snapshot(snapshot), Some(cursor))
                .is_err()
            {
                return;
            }
            seq
        }
        Some(Ok(Ok(None))) => {
            sink.end(EndReason::NotFound);
            return;
        }
        Some(Ok(Err((events, head)))) => {
            for (seq, event) in events {
                let wire = WireEvent::from_parts(seq, event);
                if sink
                    .push_encoded(wire.encoded(encoding), Some(sequence_cursor(store, seq)))
                    .is_err()
                {
                    return;
                }
            }
            head
        }
        Some(Err(error)) => {
            sink.end(EndReason::Error {
                error: storage(error),
            });
            return;
        }
        None => return,
    };
    if sink
        .push(
            &ThreadItem::Synchronized {
                cursor: sequence_cursor(store, cursor),
            },
            Some(sequence_cursor(store, cursor)),
        )
        .is_err()
    {
        return;
    }

    let mut coalescer: ToolCoalescer<Arc<WireEvent>> = ToolCoalescer::default();
    let send = |events: Vec<Arc<WireEvent>>| -> bool {
        events.into_iter().all(|wire| {
            sink.push_encoded(
                wire.encoded(encoding),
                Some(sequence_cursor(store, wire.seq)),
            )
            .is_ok()
        })
    };
    loop {
        let deadline = coalescer.deadline();
        tokio::select! {
            message = subscription.receiver.recv() => {
                let Some(message) = message else {
                    if !send(coalescer.flush()) {
                        return;
                    }
                    sink.end(subscription.end_reason());
                    return;
                };
                match message {
                    HubMessage::Event(wire) => {
                        if wire.seq <= cursor {
                            continue;
                        }
                        cursor = wire.seq;
                        let ready = coalescer.push(Instant::now(), wire.seq, &wire.event, wire.clone());
                        if !send(ready) {
                            return;
                        }
                    }
                    HubMessage::Barrier(done) => {
                        if !send(coalescer.flush()) {
                            return;
                        }
                        let _ = done.send(());
                    }
                    HubMessage::Shell(_) | HubMessage::Advance(_) => {}
                }
            }
            Some(delta) = deltas.recv() => {
                if sink.push(&delta.item(), None).is_err() {
                    return;
                }
            }
            () = sleep_until(deadline), if deadline.is_some() => {
                if !send(coalescer.due(Instant::now())) {
                    return;
                }
            }
        }
    }
}

async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await,
        None => std::future::pending().await,
    }
}

/// `shell`: a snapshot (or the rows touched since `after`), `synchronized`,
/// then a delta per changed row and `synchronized` as the log moves on.
pub(crate) async fn shell_stream(
    sink: StreamSink,
    context: Arc<V8Context>,
    spec: ShellSpec,
    after: Option<StreamCursor>,
) {
    let Some(orchestration) = context.orchestration.clone() else {
        end_unsupported(&sink);
        return;
    };
    let store = orchestration.store_id();
    let mut subscription = orchestration.hub.subscribe(None);
    sink.register_barrier(subscription.sender.clone());
    let reader = orchestration.clone();
    let read_spec = spec.clone();
    let start = blocking(move || {
        reader.orchestrator.reader().with(|connection| {
            if let Some(after) = &after {
                match plan_shell_resume_default(connection, store, after)? {
                    ShellResume::Changes { changes, head } => return Ok(Err((changes, head))),
                    ShellResume::Snapshot => {}
                }
            }
            shell_rows(connection, store, &read_spec).map(Ok)
        })
    })
    .await;
    let (mut filter, mut cursor) = match start {
        Some(Ok(Ok(rows))) => {
            let StreamCursor::Sequence { seq, .. } = rows.cursor else {
                return;
            };
            let mut filter = ShellFilter::new(spec);
            for item in snapshot_items(rows) {
                filter.seed(&item);
                let cursor = match &item {
                    ShellItem::Snapshot { cursor, .. } => Some(cursor.clone()),
                    _ => None,
                };
                if sink.push(&item, cursor).is_err() {
                    return;
                }
            }
            (filter, seq)
        }
        Some(Ok(Err((changes, head)))) => {
            let mut filter = ShellFilter::resumed(spec);
            for change in changes {
                if let Some(item) = filter.admit(&change)
                    && sink.push(&item, None).is_err()
                {
                    return;
                }
            }
            (filter, head)
        }
        Some(Err(error)) => {
            sink.end(EndReason::Error {
                error: storage(error),
            });
            return;
        }
        None => return,
    };
    let mut announced = cursor;
    if sink
        .push(
            &ShellItem::Synchronized {
                cursor: sequence_cursor(store, cursor),
            },
            Some(sequence_cursor(store, cursor)),
        )
        .is_err()
    {
        return;
    }
    while let Some(message) = subscription.receiver.recv().await {
        let seq = match &message {
            HubMessage::Shell(change) => match change.as_ref() {
                ShellChange::Project { seq, .. }
                | ShellChange::Thread { seq, .. }
                | ShellChange::ThreadGone { seq, .. } => *seq,
            },
            HubMessage::Advance(seq) => *seq,
            HubMessage::Barrier(_) | HubMessage::Event(_) => 0,
        };
        match message {
            HubMessage::Shell(change) => {
                if seq <= cursor {
                    continue;
                }
                cursor = seq;
                if let Some(item) = filter.admit(&change) {
                    if sink.push(&item, Some(sequence_cursor(store, seq))).is_err() {
                        return;
                    }
                    announced = seq;
                }
            }
            HubMessage::Advance(seq) => {
                cursor = cursor.max(seq);
                if cursor > announced {
                    announced = cursor;
                    let at = sequence_cursor(store, cursor);
                    if sink
                        .push(&ShellItem::Synchronized { cursor: at.clone() }, Some(at))
                        .is_err()
                    {
                        return;
                    }
                }
            }
            HubMessage::Barrier(done) => {
                let _ = done.send(());
            }
            HubMessage::Event(_) => {}
        }
    }
    sink.end(subscription.end_reason());
}

/// `commandOutput`: the command's retained output as `replace`, then
/// `append`s paced at 100 ms (500 ms after an overflow), then `finished`.
/// A command the hub no longer has is served from its stored output.
pub(crate) async fn command_output_stream(
    sink: StreamSink,
    context: Arc<V8Context>,
    spec: CommandOutputSpec,
) {
    let Some(orchestration) = context.orchestration.clone() else {
        end_unsupported(&sink);
        return;
    };
    let CommandOutputSpec { thread_id, item_id } = spec;
    let reader = orchestration.clone();
    let stored = blocking(move || {
        reader
            .orchestrator
            .reader()
            .with(|connection| turn_item(connection, thread_id, item_id))
    })
    .await;
    let item = match stored {
        Some(Ok(Some(item))) => item,
        Some(Ok(None)) => {
            sink.end(EndReason::NotFound);
            return;
        }
        Some(Err(error)) => {
            sink.end(EndReason::Error {
                error: storage(error),
            });
            return;
        }
        None => return,
    };
    let wake = Arc::new(Notify::new());
    let waker = wake.clone();
    let Some(subscription) = orchestration
        .live
        .output
        .subscribe(thread_id, item_id, move || waker.notify_one())
    else {
        sink.end(EndReason::NotFound);
        return;
    };
    if !subscription.was_known() && item.status.is_terminal() {
        serve_stored(&sink, &item.kind);
        return;
    }
    loop {
        let (frames, interval) = subscription.take_frames();
        if !frames.is_empty() {
            for frame in &frames {
                if sink.push(frame, None).is_err() {
                    return;
                }
            }
            if subscription.is_finished() {
                sink.end(EndReason::Completed);
                return;
            }
            tokio::time::sleep(interval).await;
            continue;
        }
        wake.notified().await;
    }
}

/// The stored output of a finished command the hub no longer holds.
fn serve_stored(sink: &StreamSink, kind: &TurnItemKind) {
    let (text, truncated, exit_code) = match kind {
        TurnItemKind::CommandExecution {
            output,
            output_truncated,
            exit_code,
            ..
        } => (
            output.clone().unwrap_or_default(),
            *output_truncated,
            *exit_code,
        ),
        _ => (String::new(), false, None),
    };
    let frames = [
        CommandOutputItem::Replace {
            text,
            truncated,
            running: false,
        },
        CommandOutputItem::Finished {
            exit_code,
            truncated,
        },
    ];
    for frame in &frames {
        if sink.push(frame, None).is_err() {
            return;
        }
    }
    sink.end(EndReason::Completed);
}

#[cfg(test)]
mod tests {
    use super::*;
    use waku_orchestrator::streams::seed::{SeedOptions, seed_history, seed_thread};

    fn orchestrator(root: &std::path::Path) -> Arc<Orchestrator> {
        Orchestrator::open(
            OrchestratorConfig::new(root.join("state.sqlite")),
            Dependencies {
                clock: Arc::new(waku_orchestrator::ManualClock::new(1_700_000_000_000)),
                ids: Box::new(waku_orchestrator::SequentialIds::default()),
                capabilities: Arc::new(NoCapabilities),
            },
        )
        .unwrap()
    }

    use waku_orchestrator::{Dependencies, NoCapabilities, OrchestratorConfig};

    #[test]
    fn a_full_hub_queue_ends_its_subscription_with_overflow() {
        let root = std::env::temp_dir().join(format!("wk8-hub-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&root).unwrap();
        let orchestrator = orchestrator(&root);
        let (_, thread_id) = seed_thread(&orchestrator, "hub").unwrap();
        let (_, other) = seed_thread(&orchestrator, "other").unwrap();
        let feed = orchestrator.subscribe();
        seed_history(
            &orchestrator,
            thread_id,
            SeedOptions {
                turns: 1,
                ..SeedOptions::default()
            },
        )
        .unwrap();
        let events = feed.drain();
        assert_eq!(events.len(), 5);

        let hub = Arc::new(EventHub::with_queue(0, 2));
        let mut full = hub.subscribe(Some(thread_id));
        let mut quiet = hub.subscribe(Some(other));
        for event in &events {
            hub.route(&orchestrator, event);
        }
        // Two events fit; the third overflowed and dropped the registration.
        assert!(matches!(full.receiver.try_recv(), Ok(HubMessage::Event(_))));
        assert!(matches!(full.receiver.try_recv(), Ok(HubMessage::Event(_))));
        assert!(matches!(
            full.receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
        assert_eq!(full.end_reason(), EndReason::Overflow);
        assert!(full.sender.upgrade().is_none());
        // Another thread's subscription is unaffected.
        assert!(matches!(
            quiet.receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(hub.routes.lock().threads.len(), 1);
        drop(quiet);
        assert!(hub.routes.lock().threads.is_empty());
        // Losing the feed ends every subscription with overflow.
        let mut kicked = hub.subscribe(None);
        hub.kick_all();
        assert!(kicked.receiver.try_recv().is_err());
        assert_eq!(kicked.end_reason(), EndReason::Overflow);
        orchestrator.shutdown();
        let _ = std::fs::remove_dir_all(&root);
    }
}
