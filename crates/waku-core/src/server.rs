//! The daemon's WebSocket server.
//!
//! The transport is evented (#18). One small multi-thread tokio runtime owns
//! the accept loop, a reader and a writer task per connection, and the
//! keepalive timers; nothing polls. Backend requests are synchronous (git,
//! filesystem, SQLite, provider probes) and run on that runtime's blocking
//! pool, which reuses a bounded set of threads instead of spawning one per
//! request. Runtime-targeted commands keep their per-session order through an
//! async mailbox that holds no thread while it waits.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::net::TcpListener;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use anyhow::{Context as _, bail};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt as _, StreamExt as _};
use parking_lot::{Condvar, Mutex};
use subtle::ConstantTimeEq as _;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::handshake::server::{
    ErrorResponse, Request as HandshakeRequest, Response as HandshakeResponse,
};
use tokio_tungstenite::tungstenite::http::{StatusCode, header::ORIGIN};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::{self, Message, Utf8Bytes};
use uuid::Uuid;

use crate::local_socket::LocalListener;
use crate::model::{AgentSession, Project, ProviderKind, SessionStatus};
use crate::protocol::{
    ClientMessage, Command, KEEPALIVE_INTERVAL, KEEPALIVE_TIMEOUT, MAX_WIRE_MESSAGE_BYTES,
    PROTOCOL_VERSION, ReplayCursor, Request, ResponseOutcome, ResponsePayload, RpcError,
    SequencedEvent, ServerMessage, WireDriverEvent,
};

/// A socket write that makes no progress for this long means the client is
/// not reading. Keeping the connection would let its event queue grow without
/// bound, so it is dropped; clients reconnect and resume from their cursors.
const SOCKET_WRITE_STALL_TIMEOUT: Duration = Duration::from_secs(5);
/// The WebSocket upgrade and the client's hello must both arrive within this.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_HANDSHAKE_MESSAGE_BYTES: usize = 64 * 1024;
/// Bytes a connection may send before it has authenticated: the HTTP upgrade
/// request plus a hello. Anything beyond that is refused unread, so an
/// unauthenticated peer cannot make the daemon buffer a large message.
const MAX_PRE_AUTH_BYTES: usize = MAX_HANDSHAKE_MESSAGE_BYTES + 16 * 1024;
const MAX_CONNECTIONS: usize = 64;
const MAX_REPLAY_EVENTS_PER_SESSION: usize = 2048;
/// A runtime's replay journal also stops at this many serialized bytes, so a
/// burst of large tool outputs cannot pin hundreds of megabytes (#21).
const MAX_REPLAY_BYTES_PER_RUNTIME: usize = 8 * 1024 * 1024;
/// Text and reasoning deltas of one runtime that arrive within this window
/// are sent as one event. Providers emit hundreds of small chunks a second,
/// and each event pays an envelope of ~150 bytes plus a frame and a wakeup.
const DELTA_COALESCE_WINDOW: Duration = Duration::from_millis(12);
/// A pending delta this large is sent at once rather than at its deadline.
const MAX_COALESCED_DELTA_BYTES: usize = 64 * 1024;
/// Per-subscriber cap on queued daemon messages. A subscriber that falls this
/// far behind live events is dropped instead of buffered for, so a stalled
/// client cannot make the daemon's heap grow at the stream rate forever.
/// (Max 8 replaying runtimes at `MAX_REPLAY_EVENTS_PER_SESSION` fits; an
/// overflow during replay or live emission closes the connection and the
/// client reconnects from its last replayed cursor.)
const MAX_QUEUED_MESSAGES_PER_SUBSCRIBER: usize = 16384;
const MAX_CACHED_RESPONSES: usize = 2048;
/// Responses are cached by request id so a client that missed a reply can
/// fetch it after reconnecting. Caching is bounded by bytes as well as count:
/// outcomes such as a hydrated session can be megabytes, and a count-only cap
/// would let a handful of them pin hundreds of megabytes in the daemon.
const MAX_CACHED_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
const NATIVE_CLIENT_HEADER: &str = "x-waku-client";
const NATIVE_CLIENT_HEADER_VALUE: &str = "native";
/// Async workers only move frames between sockets and queues; two keep one
/// connection's burst from delaying another's reply.
const TRANSPORT_WORKERS: usize = 2;
/// Cap on threads running blocking backend work at once (git, filesystem,
/// SQLite, provider probes). Further requests queue for a free thread.
const MAX_BLOCKING_THREADS: usize = 64;
/// An idle blocking thread exits after this, so a burst of requests does not
/// leave a crowd of parked threads behind.
const BLOCKING_THREAD_KEEP_ALIVE: Duration = Duration::from_secs(30);
/// How long [`serve`] waits for in-flight work when it stops.
const RUNTIME_SHUTDOWN_GRACE: Duration = Duration::from_secs(1);
/// Queued messages written before one flush, so a burst of events costs one
/// socket write rather than one per event.
const MAX_MESSAGES_PER_FLUSH: usize = 64;

#[derive(Clone, Debug, Default)]
pub struct ServerOptions {
    /// Browser WebSocket handshakes carry an Origin header. Most native clients
    /// do not; React Native does and identifies itself with `x-waku-client`.
    /// An empty set therefore still permits native clients only.
    pub allowed_origins: HashSet<String>,
    /// Only a daemon owned by the desktop process should accept the global
    /// shutdown control message. Service-managed daemons keep running when an
    /// authenticated client disconnects.
    pub allow_shutdown: bool,
}

/// Stops [`serve`]. Triggering it wakes the accept loop and every connection
/// immediately; nothing polls a flag.
#[derive(Clone, Default)]
pub struct ShutdownSignal {
    inner: Arc<ShutdownState>,
}

#[derive(Default)]
struct ShutdownState {
    triggered: AtomicBool,
    notify: Notify,
    lock: Mutex<()>,
    condvar: Condvar,
}

impl ShutdownSignal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn trigger(&self) {
        if self.inner.triggered.swap(true, Ordering::AcqRel) {
            return;
        }
        self.inner.notify.notify_waiters();
        // Taking the lock orders this wakeup after a waiter's check of the
        // flag, so `wait_timeout` cannot miss it.
        let _guard = self.inner.lock.lock();
        self.inner.condvar.notify_all();
    }

    pub fn is_triggered(&self) -> bool {
        self.inner.triggered.load(Ordering::Acquire)
    }

    /// Block a plain thread until the signal fires or `timeout` passes.
    /// Returns whether it fired.
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut guard = self.inner.lock.lock();
        while !self.is_triggered() {
            if self
                .inner
                .condvar
                .wait_until(&mut guard, deadline)
                .timed_out()
            {
                return self.is_triggered();
            }
        }
        true
    }

    /// Block a plain thread until the signal fires.
    pub fn wait(&self) {
        let mut guard = self.inner.lock.lock();
        while !self.is_triggered() {
            self.inner.condvar.wait(&mut guard);
        }
    }

    pub async fn triggered(&self) {
        loop {
            let notified = self.inner.notify.notified();
            let mut notified = std::pin::pin!(notified);
            notified.as_mut().enable();
            if self.is_triggered() {
                return;
            }
            notified.await;
        }
    }
}

struct ConnectionPermit(Arc<AtomicUsize>);

impl ConnectionPermit {
    fn acquire(active: &Arc<AtomicUsize>) -> Option<Self> {
        active
            .try_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < MAX_CONNECTIONS).then_some(count + 1)
            })
            .ok()
            .map(|_| Self(active.clone()))
    }
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

pub trait Backend: Send + Sync + 'static {
    fn handle(&self, request: Request, events: EventSink) -> anyhow::Result<ResponsePayload>;

    fn shutdown(&self) {}
}

#[derive(Clone)]
pub struct EventSink {
    session_id: Uuid,
    runtime_id: Uuid,
    hub: Arc<Hub>,
}

impl EventSink {
    pub fn send(&self, event: WireDriverEvent) -> anyhow::Result<()> {
        self.hub.emit(self.session_id, self.runtime_id, event, true);
        Ok(())
    }

    /// Broadcast a live-only event without retaining it in the replay journal.
    /// High-volume PTY output is meaningful only to a terminal emulator that
    /// is currently attached; replaying raw chunks into a fresh emulator would
    /// also retain an unbounded terminal transcript in daemon memory.
    pub fn send_ephemeral(&self, event: WireDriverEvent) -> anyhow::Result<()> {
        self.hub
            .emit(self.session_id, self.runtime_id, event, false);
        Ok(())
    }
}

/// A daemon message serialized once (#21). Cloning it bumps a reference
/// count, so the journal and every subscriber queue share one buffer.
type Frame = Utf8Bytes;

/// One item of a connection's outgoing queue.
enum Outbound {
    /// A JSON text message.
    Text(Frame),
    /// The connection's last message: written and flushed, then the writer
    /// stops.
    Final(Frame),
}

/// A connection's outgoing queue. Hub fan-out and request workers push into
/// it; the connection's writer task drains it.
type Outgoing = mpsc::Sender<Outbound>;

/// Serialize a message for the wire, once.
fn encode<T: serde::Serialize>(message: &T) -> Option<Frame> {
    match serde_json::to_string(message) {
        Ok(text) => Some(Frame::from(text)),
        Err(error) => {
            eprintln!("waku-daemon could not encode a message: {error}");
            None
        }
    }
}

/// `ServerMessage::Response` serialized from a borrowed outcome, so a large
/// outcome (a hydrated session) is not cloned just to be written.
#[derive(serde::Serialize)]
#[serde(tag = "type", rename = "response", rename_all = "camelCase")]
struct ResponseFrame<'a> {
    request_id: Uuid,
    outcome: &'a ResponseOutcome,
}

/// One retained, already serialized runtime event.
struct JournalEntry {
    sequence: u64,
    frame: Frame,
}

/// A runtime's replay journal, bounded by count and by bytes.
#[derive(Default)]
struct Journal {
    entries: VecDeque<JournalEntry>,
    bytes: usize,
}

impl Journal {
    fn push(&mut self, sequence: u64, frame: Frame) {
        self.bytes = self.bytes.saturating_add(frame.len());
        self.entries.push_back(JournalEntry { sequence, frame });
        while self.entries.len() > MAX_REPLAY_EVENTS_PER_SESSION
            || (self.bytes > MAX_REPLAY_BYTES_PER_RUNTIME && self.entries.len() > 1)
        {
            if let Some(evicted) = self.entries.pop_front() {
                self.bytes = self.bytes.saturating_sub(evicted.frame.len());
            }
        }
    }
}

/// Streamed text held back for up to [`DELTA_COALESCE_WINDOW`] so that a
/// provider's many small chunks become one event.
struct PendingDelta {
    event: WireDriverEvent,
    deadline: Instant,
}

impl PendingDelta {
    /// Append `next` if it is the same kind of delta. Returns it otherwise.
    fn absorb(&mut self, next: WireDriverEvent) -> Option<WireDriverEvent> {
        match (&mut self.event, next) {
            (WireDriverEvent::TextDelta(text), WireDriverEvent::TextDelta(more))
            | (WireDriverEvent::ReasoningDelta(text), WireDriverEvent::ReasoningDelta(more)) => {
                text.push_str(&more);
                None
            }
            (_, next) => Some(next),
        }
    }

    fn len(&self) -> usize {
        match &self.event {
            WireDriverEvent::TextDelta(text) | WireDriverEvent::ReasoningDelta(text) => text.len(),
            _ => 0,
        }
    }
}

/// One connected client's delivery state.
struct Subscriber {
    messages: Outgoing,
    /// Fired when the subscriber falls too far behind and is dropped from the
    /// hub. The connection closes, letting the client reconnect and resume
    /// from its last replayed cursor.
    kicked: Option<oneshot::Sender<()>>,
}

impl Subscriber {
    fn new(messages: Outgoing) -> (Self, oneshot::Receiver<()>) {
        let (kicked, kicked_rx) = oneshot::channel();
        (
            Self {
                messages,
                kicked: Some(kicked),
            },
            kicked_rx,
        )
    }
}

#[derive(Default)]
struct HubState {
    next_subscriber_id: u64,
    task_state_revision: u64,
    subscribers: HashMap<u64, Subscriber>,
    active_runtimes: HashMap<Uuid, Uuid>,
    next_sequences: HashMap<(Uuid, Uuid), u64>,
    journal: HashMap<(Uuid, Uuid), Journal>,
    pending_deltas: HashMap<(Uuid, Uuid), PendingDelta>,
    /// Serialized responses by request id; their length is their cost.
    responses: VecDeque<(Uuid, Frame)>,
    cached_response_bytes: usize,
    catalog_projects: HashMap<Uuid, ProjectCatalogEntry>,
    catalog_sessions: HashMap<Uuid, SessionCatalogEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProjectCatalogEntry {
    name: String,
    path: std::path::PathBuf,
    created_at: u64,
}

impl From<&Project> for ProjectCatalogEntry {
    fn from(project: &Project) -> Self {
        Self {
            name: project.name.clone(),
            path: project.path.clone(),
            created_at: project.created_at,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SessionCatalogEntry {
    title: String,
    auto_title: Option<String>,
    project_id: Uuid,
    provider: ProviderKind,
    model: Option<String>,
    status: SessionStatus,
    created_at: u64,
    last_reply_at: Option<u64>,
}

impl From<&AgentSession> for SessionCatalogEntry {
    fn from(session: &AgentSession) -> Self {
        Self {
            title: session.title.clone(),
            auto_title: session.auto_title.clone(),
            project_id: session.project_id,
            provider: session.provider,
            model: session.model.clone(),
            status: session.status,
            created_at: session.created_at,
            last_reply_at: session.last_reply_at,
        }
    }
}

struct Hub {
    epoch: Uuid,
    state: Mutex<HubState>,
    /// Wakes the delta-coalescing thread; started on the first delta.
    coalescer: Mutex<Option<crossbeam_channel::Sender<Instant>>>,
}

impl Default for Hub {
    fn default() -> Self {
        Self {
            epoch: Uuid::new_v4(),
            state: Mutex::new(HubState::default()),
            coalescer: Mutex::new(None),
        }
    }
}

struct DispatchedRequest {
    request: Request,
    outgoing: Outgoing,
    source_subscriber_id: u64,
}

struct RuntimeMailbox {
    id: Uuid,
    sender: mpsc::UnboundedSender<DispatchedRequest>,
}

struct RequestDispatcher {
    backend: Arc<dyn Backend>,
    hub: Arc<Hub>,
    runtime: tokio::runtime::Handle,
    /// A live provider runtime is an actor owned by the daemon, not by any
    /// particular WebSocket connection. One mailbox per session preserves
    /// lifecycle order across desktop and web clients without serializing
    /// unrelated sessions or read-only requests.
    runtime_mailboxes: Arc<Mutex<HashMap<Uuid, RuntimeMailbox>>>,
}

impl Hub {
    fn event_sink(self: &Arc<Self>, session_id: Uuid, runtime_id: Uuid) -> EventSink {
        EventSink {
            session_id,
            runtime_id,
            hub: self.clone(),
        }
    }

    fn begin_runtime(&self, session_id: Uuid, runtime_id: Uuid) {
        let mut state = self.state.lock();
        state.active_runtimes.insert(session_id, runtime_id);
        state
            .next_sequences
            .retain(|(candidate, _), _| *candidate != session_id);
        state
            .journal
            .retain(|(candidate, _), _| *candidate != session_id);
        state
            .pending_deltas
            .retain(|(candidate, _), _| *candidate != session_id);
    }

    fn end_runtime(&self, session_id: Uuid, runtime_id: Option<Uuid>) {
        let mut state = self.state.lock();
        let Some(&active) = state.active_runtimes.get(&session_id) else {
            return;
        };
        if runtime_id.is_some_and(|runtime_id| runtime_id != active) {
            return;
        }
        // Text the provider streamed just before closing still goes out.
        let key = (session_id, active);
        if let Some(pending) = state.pending_deltas.remove(&key) {
            self.publish(&mut state, key, pending.event, true);
        }
        state.active_runtimes.remove(&session_id);
        state
            .next_sequences
            .retain(|(candidate, _), _| *candidate != session_id);
        state
            .journal
            .retain(|(candidate, _), _| *candidate != session_id);
        state
            .pending_deltas
            .retain(|(candidate, _), _| *candidate != session_id);
    }

    /// Accept one runtime event. Text and reasoning deltas are merged with
    /// the runtime's next deltas of the same kind for up to
    /// [`DELTA_COALESCE_WINDOW`]; any other event first flushes the pending
    /// delta, so order is preserved exactly.
    fn emit(
        self: &Arc<Self>,
        session_id: Uuid,
        runtime_id: Uuid,
        event: WireDriverEvent,
        replayable: bool,
    ) {
        profile_span!(TRACE, "Hub::emit");
        let mut state = self.state.lock();
        if state.active_runtimes.get(&session_id) != Some(&runtime_id) {
            return;
        }
        let key = (session_id, runtime_id);
        if replayable && event.is_delta() {
            let event = match state.pending_deltas.get_mut(&key) {
                Some(pending) => match pending.absorb(event) {
                    None if pending.len() < MAX_COALESCED_DELTA_BYTES => return,
                    None => {
                        let full = state
                            .pending_deltas
                            .remove(&key)
                            .expect("the pending delta was just extended");
                        self.publish(&mut state, key, full.event, true);
                        return;
                    }
                    Some(other_kind) => {
                        let previous = state
                            .pending_deltas
                            .remove(&key)
                            .expect("the pending delta was just inspected");
                        self.publish(&mut state, key, previous.event, true);
                        other_kind
                    }
                },
                None => event,
            };
            let deadline = Instant::now() + DELTA_COALESCE_WINDOW;
            state
                .pending_deltas
                .insert(key, PendingDelta { event, deadline });
            drop(state);
            self.arm_coalescer(deadline);
            return;
        }
        if let Some(pending) = state.pending_deltas.remove(&key) {
            self.publish(&mut state, key, pending.event, true);
        }
        self.publish(&mut state, key, event, replayable);
    }

    /// Sequence, serialize once, journal and broadcast one event.
    fn publish(
        &self,
        state: &mut HubState,
        (session_id, runtime_id): (Uuid, Uuid),
        event: WireDriverEvent,
        replayable: bool,
    ) {
        let sequence = state
            .next_sequences
            .entry((session_id, runtime_id))
            .or_default();
        *sequence = sequence.saturating_add(1);
        let sequence = *sequence;
        let Some(frame) = encode(&ServerMessage::Event(SequencedEvent {
            session_id,
            runtime_id,
            epoch: self.epoch,
            sequence,
            event,
        })) else {
            return;
        };
        if replayable {
            state
                .journal
                .entry((session_id, runtime_id))
                .or_default()
                .push(sequence, frame.clone());
        }
        Self::broadcast(state, &frame, None);
    }

    /// Wake the coalescing thread for `deadline`, starting it on first use.
    /// The thread sleeps on its channel and wakes only while deltas wait.
    fn arm_coalescer(self: &Arc<Self>, deadline: Instant) {
        let mut wake = self.coalescer.lock();
        if wake.is_none() {
            let (sender, deadlines) = crossbeam_channel::unbounded();
            let hub = Arc::downgrade(self);
            let started = std::thread::Builder::new()
                .name("waku-daemon-coalesce".into())
                .spawn(move || run_coalescer(hub, deadlines));
            if started.is_err() {
                drop(wake);
                // Without the thread nothing would flush; send at once.
                self.flush_due_deltas(Instant::now() + DELTA_COALESCE_WINDOW);
                return;
            }
            *wake = Some(sender);
        }
        if let Some(wake) = wake.as_ref() {
            let _ = wake.send(deadline);
        }
    }

    /// Publish every pending delta whose window has closed by `now`, and
    /// return the earliest deadline still waiting.
    fn flush_due_deltas(&self, now: Instant) -> Option<Instant> {
        let mut state = self.state.lock();
        let due = state
            .pending_deltas
            .iter()
            .filter(|(_, pending)| pending.deadline <= now)
            .map(|(key, _)| *key)
            .collect::<Vec<_>>();
        for key in due {
            if let Some(pending) = state.pending_deltas.remove(&key) {
                self.publish(&mut state, key, pending.event, true);
            }
        }
        state
            .pending_deltas
            .values()
            .map(|pending| pending.deadline)
            .min()
    }

    fn subscribe(&self, resume_from: &[ReplayCursor], subscriber: Subscriber) -> u64 {
        let mut state = self.state.lock();
        let id = state.next_subscriber_id;
        state.next_subscriber_id = state.next_subscriber_id.saturating_add(1);
        // The subscriber is registered before replay so an oversized journal
        // can only truncate the replay, never leave a live connection without
        // events. Replay runs under the hub lock so no live event can
        // interleave with the journal catch-up, and pushes into the bounded
        // queue are non-blocking. Dropping the oldest replayed events matches
        // the journal's own rollover semantics; clients reconcile any gap
        // from session state.
        let messages = subscriber.messages.clone();
        state.subscribers.insert(id, subscriber);
        for (&(session_id, runtime_id), journal) in &state.journal {
            let sequence = resume_from
                .iter()
                .find(|cursor| {
                    cursor.session_id == session_id
                        && cursor.runtime_id == runtime_id
                        && cursor.epoch == self.epoch
                })
                .map(|cursor| cursor.sequence)
                .unwrap_or_default();
            for entry in journal
                .entries
                .iter()
                .filter(|entry| entry.sequence > sequence)
            {
                if messages
                    .try_send(Outbound::Text(entry.frame.clone()))
                    .is_err()
                {
                    return id;
                }
            }
        }
        id
    }

    fn unsubscribe(&self, subscriber_id: u64) {
        self.state.lock().subscribers.remove(&subscriber_id);
    }

    /// Sends `frame` to every subscriber except `skip`, dropping any whose
    /// bounded queue is full. Buffering a stalled client at the daemon's
    /// event rate is how resident memory grew without bound; a dropped
    /// subscriber's connection observes the kick, closes, and reconnects with
    /// replay cursors, so no event stream is lost permanently.
    fn broadcast(state: &mut HubState, frame: &Frame, skip: Option<u64>) {
        let mut overwhelmed = Vec::new();
        for (&subscriber_id, subscriber) in &state.subscribers {
            if Some(subscriber_id) == skip {
                continue;
            }
            if subscriber
                .messages
                .try_send(Outbound::Text(frame.clone()))
                .is_err()
            {
                overwhelmed.push(subscriber_id);
            }
        }
        for subscriber_id in overwhelmed {
            if let Some(kicked) = state
                .subscribers
                .remove(&subscriber_id)
                .and_then(|subscriber| subscriber.kicked)
            {
                let _ = kicked.send(());
            }
        }
    }

    fn task_state_changed(&self, source_subscriber_id: u64) {
        let mut state = self.state.lock();
        Self::broadcast_task_state_changed(&mut state, source_subscriber_id);
    }

    fn replace_task_catalog(&self, projects: &[Project], sessions: &[AgentSession]) {
        let mut state = self.state.lock();
        state.catalog_projects = projects
            .iter()
            .map(|project| (project.id, ProjectCatalogEntry::from(project)))
            .collect();
        state.catalog_sessions = sessions
            .iter()
            .map(|session| (session.id, SessionCatalogEntry::from(session)))
            .collect();
    }

    fn task_state_saved(
        &self,
        source_subscriber_id: u64,
        projects: &[Project],
        sessions: &[AgentSession],
    ) {
        let mut state = self.state.lock();
        let mut changed = false;
        for project in projects {
            let next = ProjectCatalogEntry::from(project);
            changed |= state
                .catalog_projects
                .insert(project.id, next.clone())
                .is_none_or(|previous| previous != next);
        }
        for session in sessions {
            let next = SessionCatalogEntry::from(session);
            changed |= state
                .catalog_sessions
                .insert(session.id, next.clone())
                .is_none_or(|previous| previous != next);
        }
        if changed {
            Self::broadcast_task_state_changed(&mut state, source_subscriber_id);
        }
    }

    fn broadcast_task_state_changed(state: &mut HubState, source_subscriber_id: u64) {
        state.task_state_revision = state.task_state_revision.saturating_add(1);
        if let Some(frame) = encode(&ServerMessage::TaskStateChanged {
            revision: state.task_state_revision,
        }) {
            Self::broadcast(state, &frame, Some(source_subscriber_id));
        }
    }

    fn cached_response(&self, request_id: Uuid) -> Option<Frame> {
        self.state
            .lock()
            .responses
            .iter()
            .rev()
            .find_map(|(cached_id, frame)| (*cached_id == request_id).then(|| frame.clone()))
    }

    /// Keep a response's serialized frame so a client that missed it can
    /// fetch it again. The frame is the one the connection writes, so its
    /// length is exactly what it costs.
    fn cache_response(&self, request_id: Uuid, frame: Frame) {
        let bytes = frame.len();
        let mut state = self.state.lock();
        state.cached_response_bytes = state.cached_response_bytes.saturating_add(bytes);
        state.responses.push_back((request_id, frame));
        while state.responses.len() > MAX_CACHED_RESPONSES
            || state.cached_response_bytes > MAX_CACHED_RESPONSE_BYTES
        {
            if let Some((_, evicted)) = state.responses.pop_front() {
                state.cached_response_bytes =
                    state.cached_response_bytes.saturating_sub(evicted.len());
            }
        }
    }
}

/// Flushes pending deltas when their window closes. It blocks on its channel
/// with no timeout while nothing is pending, and exits when the hub is gone.
fn run_coalescer(hub: Weak<Hub>, deadlines: crossbeam_channel::Receiver<Instant>) {
    let mut next: Option<Instant> = None;
    loop {
        let received = match next {
            Some(at) => deadlines.recv_deadline(at),
            None => deadlines
                .recv()
                .map_err(|_| crossbeam_channel::RecvTimeoutError::Disconnected),
        };
        match received {
            Ok(deadline) => {
                next = Some(next.map_or(deadline, |next| next.min(deadline)));
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
        }
        let now = Instant::now();
        if next.is_some_and(|next| next <= now) {
            let Some(hub) = hub.upgrade() else {
                return;
            };
            next = hub.flush_due_deltas(now);
        }
    }
}

impl RequestDispatcher {
    fn new(backend: Arc<dyn Backend>, hub: Arc<Hub>, runtime: tokio::runtime::Handle) -> Self {
        Self {
            backend,
            hub,
            runtime,
            runtime_mailboxes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn dispatch(&self, request: Request, outgoing: Outgoing, source_subscriber_id: u64) {
        profile_span!(DEBUG, "dispatch");
        if command_targets_runtime(&request.command) {
            self.dispatch_runtime(request, outgoing, source_subscriber_id);
        } else {
            self.dispatch_independent(request, outgoing, source_subscriber_id);
        }
    }

    /// Read-only and session-independent requests run concurrently on the
    /// blocking pool, so a slow probe never delays a hydration.
    fn dispatch_independent(
        &self,
        request: Request,
        outgoing: Outgoing,
        source_subscriber_id: u64,
    ) {
        let backend = self.backend.clone();
        let hub = self.hub.clone();
        self.runtime.spawn_blocking(move || {
            handle_request(request, outgoing, source_subscriber_id, backend, hub);
        });
    }

    fn dispatch_runtime(&self, request: Request, outgoing: Outgoing, source_subscriber_id: u64) {
        let session_id = request.session_id;
        let mut dispatched = DispatchedRequest {
            request,
            outgoing,
            source_subscriber_id,
        };
        let mut mailboxes = self.runtime_mailboxes.lock();
        if let Some(mailbox) = mailboxes.get(&session_id) {
            match mailbox.sender.send(dispatched) {
                Ok(()) => return,
                Err(error) => {
                    // The previous actor exited (its backend call panicked);
                    // a successor picks the request up.
                    dispatched = error.0;
                    mailboxes.remove(&session_id);
                }
            }
        }

        let mailbox_id = Uuid::new_v4();
        let (sender, requests) = mpsc::unbounded_channel();
        sender
            .send(dispatched)
            .expect("a new runtime mailbox still has its receiver");
        mailboxes.insert(
            session_id,
            RuntimeMailbox {
                id: mailbox_id,
                sender,
            },
        );
        drop(mailboxes);
        self.runtime.spawn(run_runtime_mailbox(
            session_id,
            mailbox_id,
            requests,
            Arc::downgrade(&self.runtime_mailboxes),
            self.backend.clone(),
            self.hub.clone(),
        ));
    }
}

/// Everything a connection task needs from the server.
pub(crate) struct ServerContext {
    token: String,
    dispatcher: Arc<RequestDispatcher>,
    hub: Arc<Hub>,
    pub(crate) shutdown: ShutdownSignal,
    options: ServerOptions,
    active_connections: Arc<AtomicUsize>,
}

impl ServerContext {
    pub(crate) fn spawn_connection<S>(self: &Arc<Self>, stream: S)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let Some(permit) = ConnectionPermit::acquire(&self.active_connections) else {
            return;
        };
        let context = self.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(error) = handle_connection(stream, &context).await {
                eprintln!("waku-daemon connection ended: {error:#}");
            }
        });
    }
}

/// Where the daemon accepts connections. At least one is required.
#[derive(Default)]
pub struct Listeners {
    /// Loopback or, with exposure enabled, a network address.
    pub tcp: Option<TcpListener>,
    /// The per-user Unix socket or named pipe (#19).
    pub local: Option<LocalListener>,
}

/// Serve on one TCP listener.
pub fn serve(
    listener: TcpListener,
    token: String,
    backend: Arc<dyn Backend>,
    shutdown: ShutdownSignal,
    options: ServerOptions,
) -> anyhow::Result<()> {
    serve_listeners(
        Listeners {
            tcp: Some(listener),
            local: None,
        },
        token,
        backend,
        shutdown,
        options,
    )
}

/// Serve on every listener in `listeners` until `shutdown` fires or one of
/// them fails.
pub fn serve_listeners(
    listeners: Listeners,
    token: String,
    backend: Arc<dyn Backend>,
    shutdown: ShutdownSignal,
    options: ServerOptions,
) -> anyhow::Result<()> {
    if listeners.tcp.is_none() && listeners.local.is_none() {
        bail!("the Waku daemon needs at least one listener");
    }
    if let Some(listener) = &listeners.tcp {
        listener
            .set_nonblocking(true)
            .context("could not configure Waku daemon listener")?;
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(TRANSPORT_WORKERS)
        .max_blocking_threads(MAX_BLOCKING_THREADS)
        .thread_keep_alive(BLOCKING_THREAD_KEEP_ALIVE)
        .thread_name("waku-daemon-worker")
        .enable_all()
        .build()
        .context("could not start the Waku daemon runtime")?;
    let hub = Arc::new(Hub::default());
    let dispatcher = Arc::new(RequestDispatcher::new(
        backend.clone(),
        hub.clone(),
        runtime.handle().clone(),
    ));
    let context = Arc::new(ServerContext {
        token,
        dispatcher,
        hub,
        shutdown,
        options,
        active_connections: Arc::new(AtomicUsize::new(0)),
    });
    let result = runtime.block_on(async move {
        let mut accepting = Vec::new();
        if let Some(listener) = listeners.tcp {
            accepting.push(tokio::spawn(accept_tcp(listener, context.clone())));
        }
        if let Some(listener) = listeners.local {
            accepting.push(tokio::spawn(crate::local_socket::accept_local(
                listener,
                context.clone(),
            )));
        }
        // Every accept loop returns on shutdown; the first to return (or
        // fail) ends the server.
        let (finished, _, rest) = futures_util::future::select_all(accepting).await;
        context.shutdown.trigger();
        for task in rest {
            let _ = task.await;
        }
        finished.context("a Waku daemon accept loop panicked")?
    });
    backend.shutdown();
    // Connections still open are dropped with the runtime, which closes their
    // sockets so clients see the daemon go away at once.
    runtime.shutdown_timeout(RUNTIME_SHUTDOWN_GRACE);
    result
}

async fn accept_tcp(listener: TcpListener, context: Arc<ServerContext>) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::from_std(listener)
        .context("could not register Waku daemon listener")?;
    loop {
        let accepted = tokio::select! {
            () = context.shutdown.triggered() => return Ok(()),
            accepted = listener.accept() => accepted,
        };
        match accepted {
            Ok((stream, _)) => {
                // Replies are small and latency-bound; never wait for Nagle.
                let _ = stream.set_nodelay(true);
                context.spawn_connection(stream);
            }
            Err(error) => accept_failed(error).await?,
        }
    }
}

/// Transient accept failures are retried; anything else stops the server.
pub(crate) async fn accept_failed(error: io::Error) -> anyhow::Result<()> {
    match error.kind() {
        io::ErrorKind::Interrupted
        | io::ErrorKind::WouldBlock
        | io::ErrorKind::ConnectionAborted
        | io::ErrorKind::ConnectionReset => Ok(()),
        // Out of descriptors: back off briefly instead of spinning, and keep
        // the daemon (and every live runtime) up.
        _ if is_descriptor_exhaustion(&error) => {
            eprintln!("waku-daemon could not accept a connection: {error}");
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(())
        }
        _ => Err(error).context("Waku daemon listener failed"),
    }
}

fn is_descriptor_exhaustion(error: &io::Error) -> bool {
    #[cfg(unix)]
    {
        matches!(error.raw_os_error(), Some(libc::EMFILE | libc::ENFILE))
    }
    #[cfg(not(unix))]
    {
        let _ = error;
        false
    }
}

/// How a connection's reader stopped.
enum ReaderExit {
    Closed,
    /// The client asked a desktop-owned daemon to stop.
    Shutdown,
}

async fn handle_connection<S>(stream: S, context: &ServerContext) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let budget = Arc::new(AtomicUsize::new(MAX_PRE_AUTH_BYTES));
    let stream = PreAuthLimit {
        inner: stream,
        remaining: budget.clone(),
    };
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_WIRE_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_WIRE_MESSAGE_BYTES));
    let allowed_origins = context.options.allowed_origins.clone();
    let handshake = async {
        let mut socket = tokio_tungstenite::accept_hdr_async_with_config(
            stream,
            move |request: &HandshakeRequest, response: HandshakeResponse| {
                validate_handshake(request, response, &allowed_origins)
            },
            Some(config),
        )
        .await
        .context("WebSocket handshake failed")?;
        let hello = read_client_message(&mut socket).await?;
        anyhow::Ok((socket, hello))
    };
    let (mut socket, hello) = tokio::time::timeout(HANDSHAKE_TIMEOUT, handshake)
        .await
        .context("daemon handshake timed out")??;
    let resume_from = match hello {
        ClientMessage::Hello {
            protocol_version,
            token,
            resume_from,
            ..
        } if protocol_version == PROTOCOL_VERSION && token_matches(&context.token, &token) => {
            resume_from
        }
        ClientMessage::Hello {
            protocol_version, ..
        } if protocol_version != PROTOCOL_VERSION => {
            send_now(
                &mut socket,
                &ServerMessage::Rejected {
                    message: format!(
                        "protocol {protocol_version} is unsupported; expected {PROTOCOL_VERSION}"
                    ),
                },
            )
            .await?;
            return Ok(());
        }
        ClientMessage::Hello { .. } => {
            send_now(
                &mut socket,
                &ServerMessage::Rejected {
                    message: "authentication failed".into(),
                },
            )
            .await?;
            return Ok(());
        }
        _ => bail!("first daemon message was not a hello"),
    };
    budget.store(usize::MAX, Ordering::Release);
    send_now(
        &mut socket,
        &ServerMessage::Hello {
            protocol_version: PROTOCOL_VERSION,
            daemon_version: env!("CARGO_PKG_VERSION").into(),
        },
    )
    .await?;

    let (outgoing, outgoing_rx) = mpsc::channel(MAX_QUEUED_MESSAGES_PER_SUBSCRIBER);
    let (subscriber, mut kicked) = Subscriber::new(outgoing.clone());
    let subscriber_id = context.hub.subscribe(&resume_from, subscriber);
    let (sink, stream) = socket.split();
    let mut writer = tokio::spawn(write_outgoing(sink, outgoing_rx));
    let exit = tokio::select! {
        exit = read_incoming(stream, &outgoing, subscriber_id, context) => exit,
        _ = &mut kicked => Ok(ReaderExit::Closed),
        () = context.shutdown.triggered() => Ok(ReaderExit::Closed),
        written = &mut writer => {
            context.hub.unsubscribe(subscriber_id);
            return written.unwrap_or(Ok(()));
        }
    };
    context.hub.unsubscribe(subscriber_id);
    drop(outgoing);
    if matches!(exit, Ok(ReaderExit::Shutdown)) {
        // The writer stops right after flushing `ShuttingDown`, so the
        // desktop sees the acknowledgement before the daemon goes away.
        let _ = tokio::time::timeout(SOCKET_WRITE_STALL_TIMEOUT, &mut writer).await;
        context.shutdown.trigger();
    }
    writer.abort();
    exit.map(|_| ())
}

type ServerSink<S> = SplitSink<WebSocketStream<PreAuthLimit<S>>, Message>;
type ServerStream<S> = SplitStream<WebSocketStream<PreAuthLimit<S>>>;

/// Drains the connection's queue onto the socket. Waits on the queue without
/// a timeout except for keepalive: after `KEEPALIVE_INTERVAL` with nothing to
/// send it writes a ping, which also keeps the peer's dead-connection timer
/// fed.
async fn write_outgoing<S>(
    mut sink: ServerSink<S>,
    mut outgoing: mpsc::Receiver<Outbound>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let message = match tokio::time::timeout(KEEPALIVE_INTERVAL, outgoing.recv()).await {
            Ok(Some(message)) => message,
            Ok(None) => return Ok(()),
            Err(_) => {
                within_stall(sink.send(Message::Ping(Default::default()))).await?;
                continue;
            }
        };
        let mut shutting_down = matches!(message, Outbound::Final(_));
        within_stall(sink.feed(outbound_message(message))).await?;
        let mut batched = 1;
        while !shutting_down && batched < MAX_MESSAGES_PER_FLUSH {
            let Ok(message) = outgoing.try_recv() else {
                break;
            };
            shutting_down = matches!(message, Outbound::Final(_));
            within_stall(sink.feed(outbound_message(message))).await?;
            batched += 1;
        }
        within_stall(sink.flush()).await?;
        if shutting_down {
            return Ok(());
        }
    }
}

fn outbound_message(outbound: Outbound) -> Message {
    match outbound {
        Outbound::Text(frame) | Outbound::Final(frame) => Message::Text(frame),
    }
}

async fn within_stall<T>(
    write: impl Future<Output = Result<T, tungstenite::Error>>,
) -> anyhow::Result<T> {
    tokio::time::timeout(SOCKET_WRITE_STALL_TIMEOUT, write)
        .await
        .context("client stopped reading")?
        .context("Waku daemon WebSocket write failed")
}

async fn read_incoming<S>(
    mut stream: ServerStream<S>,
    outgoing: &Outgoing,
    subscriber_id: u64,
    context: &ServerContext,
) -> anyhow::Result<ReaderExit>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let Ok(next) = tokio::time::timeout(KEEPALIVE_TIMEOUT, stream.next()).await else {
            bail!(
                "client sent nothing for {}s, not even a pong",
                KEEPALIVE_TIMEOUT.as_secs()
            );
        };
        let message = match next {
            None => return Ok(ReaderExit::Closed),
            Some(Err(error)) if connection_closed(&error) => return Ok(ReaderExit::Closed),
            Some(Err(error)) => return Err(error).context("Waku daemon WebSocket failed"),
            Some(Ok(message)) => message,
        };
        let text = match message {
            Message::Text(text) => text,
            Message::Close(_) => return Ok(ReaderExit::Closed),
            // Pings are answered by the WebSocket layer; pongs only need to
            // arrive to reset the timeout above.
            _ => continue,
        };
        match serde_json::from_str(text.as_str()) {
            Ok(ClientMessage::Request(request)) => {
                context
                    .dispatcher
                    .dispatch(request, outgoing.clone(), subscriber_id);
            }
            Ok(ClientMessage::Shutdown) => {
                if context.options.allow_shutdown {
                    if let Some(frame) = encode(&ServerMessage::ShuttingDown) {
                        let _ = outgoing.send(Outbound::Final(frame)).await;
                    }
                    return Ok(ReaderExit::Shutdown);
                }
                if let Some(frame) = encode(&ServerMessage::Rejected {
                    message: "daemon shutdown is managed by its service owner".into(),
                }) {
                    let _ = outgoing.send(Outbound::Text(frame)).await;
                }
            }
            Ok(ClientMessage::Hello { .. }) => {}
            Err(error) => {
                eprintln!("waku-daemon ignored invalid message: {error}");
            }
        }
    }
}

fn connection_closed(error: &tungstenite::Error) -> bool {
    matches!(
        error,
        tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed
    )
}

/// Caps what a peer can make the daemon read before it has authenticated.
/// `remaining` is lifted to `usize::MAX` once the hello checks out.
struct PreAuthLimit<S> {
    inner: S,
    remaining: Arc<AtomicUsize>,
}

impl<S: AsyncRead + Unpin> AsyncRead for PreAuthLimit<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let remaining = self.remaining.load(Ordering::Acquire);
        if remaining == usize::MAX {
            return Pin::new(&mut self.inner).poll_read(cx, buf);
        }
        if remaining == 0 {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "client sent too much before authenticating",
            )));
        }
        let read = if buf.remaining() <= remaining {
            let before = buf.filled().len();
            std::task::ready!(Pin::new(&mut self.inner).poll_read(cx, buf))?;
            buf.filled().len() - before
        } else {
            let mut scratch = vec![0; remaining];
            let mut limited = ReadBuf::new(&mut scratch);
            std::task::ready!(Pin::new(&mut self.inner).poll_read(cx, &mut limited))?;
            buf.put_slice(limited.filled());
            limited.filled().len()
        };
        self.remaining.fetch_sub(read, Ordering::AcqRel);
        Poll::Ready(Ok(()))
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for PreAuthLimit<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

fn validate_handshake(
    request: &HandshakeRequest,
    response: HandshakeResponse,
    allowed_origins: &HashSet<String>,
) -> Result<HandshakeResponse, ErrorResponse> {
    if request.uri().path() != "/v1" {
        return Err(handshake_error(
            StatusCode::NOT_FOUND,
            "unknown daemon endpoint",
        ));
    }
    let is_native_client = request
        .headers()
        .get(NATIVE_CLIENT_HEADER)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == NATIVE_CLIENT_HEADER_VALUE);
    if let Some(origin) = request.headers().get(ORIGIN) {
        let allowed = origin
            .to_str()
            .ok()
            .is_some_and(|origin| allowed_origins.contains(origin));
        if !allowed && !is_native_client {
            return Err(handshake_error(
                StatusCode::FORBIDDEN,
                "WebSocket origin is not allowed",
            ));
        }
    }
    Ok(response)
}

fn handshake_error(status: StatusCode, message: &str) -> ErrorResponse {
    tungstenite::http::Response::builder()
        .status(status)
        .body(Some(message.to_owned()))
        .expect("static WebSocket rejection is valid")
}

fn token_matches(expected: &str, candidate: &str) -> bool {
    expected.as_bytes().ct_eq(candidate.as_bytes()).into()
}

fn command_targets_runtime(command: &Command) -> bool {
    matches!(
        command,
        Command::AttachSession
            | Command::Start { .. }
            | Command::Prompt { .. }
            | Command::Steer { .. }
            | Command::Cancel
            | Command::CancelComputerUse
            | Command::RefreshBackgroundWork
            | Command::StopBackgroundWork { .. }
            | Command::Respond { .. }
            | Command::RespondUserInput { .. }
            | Command::RunComputerTool { .. }
            | Command::RejectComputerTool { .. }
            | Command::ApplyOptions { .. }
            | Command::Rollback { .. }
            | Command::Fork { .. }
            | Command::ForkSessionFromResponse { .. }
            | Command::RewindSessionToMessage { .. }
            | Command::OpenTerminal { .. }
            | Command::WriteTerminal { .. }
            | Command::ResizeTerminal { .. }
            | Command::CloseTerminal
            | Command::CloseSession
            | Command::RemoveSession
    )
}

/// One session's ordered command stream. It runs each command on the
/// blocking pool and awaits it before taking the next, so it holds no thread
/// while it waits for work.
async fn run_runtime_mailbox(
    session_id: Uuid,
    mailbox_id: Uuid,
    mut requests: mpsc::UnboundedReceiver<DispatchedRequest>,
    mailbox_registry: Weak<Mutex<HashMap<Uuid, RuntimeMailbox>>>,
    backend: Arc<dyn Backend>,
    hub: Arc<Hub>,
) {
    let mut active_runtime_id = None;
    let mut pending = None;
    loop {
        let dispatched = match pending.take() {
            Some(request) => request,
            None => match requests.recv().await {
                Some(request) => request,
                None => return,
            },
        };
        let runtime_id = dispatched.request.runtime_id;
        let starts_runtime = matches!(
            &dispatched.request.command,
            Command::Start { .. } | Command::OpenTerminal { .. }
        );
        let closes_runtime = matches!(
            &dispatched.request.command,
            Command::CloseSession | Command::CloseTerminal | Command::RemoveSession
        );
        let removes_session = matches!(&dispatched.request.command, Command::RemoveSession);
        let worker_backend = backend.clone();
        let worker_hub = hub.clone();
        let Ok(handled) = tokio::task::spawn_blocking(move || {
            handle_request(
                dispatched.request,
                dispatched.outgoing,
                dispatched.source_subscriber_id,
                worker_backend,
                worker_hub,
            )
        })
        .await
        else {
            // The backend panicked or the server is stopping. Dropping the
            // receiver makes the next dispatch start a fresh mailbox.
            return;
        };

        if let Some(outcome) = &handled.outcome {
            if starts_runtime {
                active_runtime_id =
                    matches!(outcome, ResponseOutcome::Ok { .. }).then_some(runtime_id);
            } else if let ResponseOutcome::Ok {
                payload:
                    ResponsePayload::SessionRuntime {
                        runtime_id: Some(attached_runtime_id),
                        ..
                    },
            } = outcome
            {
                // A replacement mailbox can rediscover a provider runtime
                // that survived its previous actor worker.
                active_runtime_id = Some(*attached_runtime_id);
            } else if closes_runtime {
                if (removes_session || active_runtime_id == Some(runtime_id))
                    && matches!(outcome, ResponseOutcome::Ok { .. })
                {
                    hub.end_runtime(session_id, (!removes_session).then_some(runtime_id));
                    active_runtime_id = None;
                }
            } else if active_runtime_id.is_none()
                && !matches!(
                    outcome,
                    ResponseOutcome::Ok {
                        payload: ResponsePayload::SessionRuntime { .. }
                    }
                )
                && matches!(outcome, ResponseOutcome::Ok { .. })
            {
                // Recover the supervisor state if a previous mailbox worker
                // exited unexpectedly while the backend runtime stayed alive.
                active_runtime_id = Some(runtime_id);
            }
        }

        if active_runtime_id.is_none() {
            pending = take_queued_request_or_retire(
                session_id,
                mailbox_id,
                &mut requests,
                &mailbox_registry,
            );
            if pending.is_none() {
                return;
            }
        }
    }
}

fn take_queued_request_or_retire(
    session_id: Uuid,
    mailbox_id: Uuid,
    requests: &mut mpsc::UnboundedReceiver<DispatchedRequest>,
    mailbox_registry: &Weak<Mutex<HashMap<Uuid, RuntimeMailbox>>>,
) -> Option<DispatchedRequest> {
    let Some(mailbox_registry) = mailbox_registry.upgrade() else {
        return requests.try_recv().ok();
    };
    // Dispatchers send while holding this same lock. Therefore an empty
    // receiver followed by removal is atomic with respect to a new command:
    // it either joins this actor before retirement or creates its successor.
    let mut mailboxes = mailbox_registry.lock();
    match requests.try_recv() {
        Ok(request) => Some(request),
        Err(mpsc::error::TryRecvError::Empty) => {
            if mailboxes
                .get(&session_id)
                .is_some_and(|mailbox| mailbox.id == mailbox_id)
            {
                mailboxes.remove(&session_id);
            }
            None
        }
        Err(mpsc::error::TryRecvError::Disconnected) => None,
    }
}

struct HandledRequest {
    /// The backend's outcome; `None` when a cached response was resent
    /// instead of running the request again.
    outcome: Option<ResponseOutcome>,
}

enum TaskCatalogAction {
    None,
    Load,
    Save { projects: Vec<Project> },
    Changed,
}

/// Runs one request on the calling (blocking-pool) thread and queues its
/// response on the connection that sent it.
fn handle_request(
    request: Request,
    outgoing: Outgoing,
    source_subscriber_id: u64,
    backend: Arc<dyn Backend>,
    hub: Arc<Hub>,
) -> HandledRequest {
    profile_span!(DEBUG, "handle_request");
    let request_id = request.request_id;
    let notification = request_id.is_nil();
    let session_id = request.session_id;
    let runtime_id = request.runtime_id;
    let task_catalog_action = task_catalog_action(&request.command);
    let starts_runtime = matches!(
        &request.command,
        Command::Start { .. } | Command::OpenTerminal { .. }
    );
    if !notification && let Some(cached) = hub.cached_response(request_id) {
        let _ = outgoing.blocking_send(Outbound::Text(cached));
        return HandledRequest { outcome: None };
    }
    if starts_runtime {
        hub.begin_runtime(session_id, runtime_id);
    }
    let outcome = match backend.handle(request, hub.event_sink(session_id, runtime_id)) {
        Ok(payload) => ResponseOutcome::Ok { payload },
        Err(error) => ResponseOutcome::Error {
            error: RpcError::from(error),
        },
    };
    if starts_runtime && matches!(&outcome, ResponseOutcome::Error { .. }) {
        hub.end_runtime(session_id, Some(runtime_id));
    }
    match (&task_catalog_action, &outcome) {
        (
            TaskCatalogAction::Load,
            ResponseOutcome::Ok {
                payload:
                    ResponsePayload::TaskState {
                        projects, sessions, ..
                    },
            },
        ) => hub.replace_task_catalog(projects, sessions),
        (
            TaskCatalogAction::Save { projects },
            ResponseOutcome::Ok {
                payload: ResponsePayload::TaskStateSaved { sessions },
            },
        ) => hub.task_state_saved(source_subscriber_id, projects, sessions),
        (TaskCatalogAction::Changed, ResponseOutcome::Ok { .. }) => {
            hub.task_state_changed(source_subscriber_id);
        }
        _ => {}
    }
    if !notification
        && let Some(frame) = encode(&ResponseFrame {
            request_id,
            outcome: &outcome,
        })
    {
        // File bytes are bulk data that clients read once; caching a copy
        // per request would duplicate every open file in daemon memory, and
        // the request is cheap to re-run if it is ever retried.
        let bulk = matches!(
            &outcome,
            ResponseOutcome::Ok {
                payload: ResponsePayload::BlobData { .. }
            }
        );
        if !bulk {
            hub.cache_response(request_id, frame.clone());
        }
        // A full queue means the connection is being kicked for falling
        // behind; waiting here is bounded by its writer draining or closing.
        let _ = outgoing.blocking_send(Outbound::Text(frame));
    }
    HandledRequest {
        outcome: Some(outcome),
    }
}

fn task_catalog_action(command: &Command) -> TaskCatalogAction {
    match command {
        Command::LoadTaskState => TaskCatalogAction::Load,
        Command::SaveTaskState { projects, .. } => TaskCatalogAction::Save {
            projects: projects.clone(),
        },
        Command::RemoveSession
        | Command::ForkSessionFromResponse { .. }
        | Command::RewindSessionToMessage { .. } => TaskCatalogAction::Changed,
        _ => TaskCatalogAction::None,
    }
}

async fn read_client_message<S>(socket: &mut WebSocketStream<S>) -> anyhow::Result<ClientMessage>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        match socket.next().await {
            Some(Ok(Message::Text(text))) => return Ok(serde_json::from_str(text.as_str())?),
            Some(Ok(Message::Close(_))) | None => bail!("client closed during daemon handshake"),
            Some(Ok(_)) => {}
            Some(Err(error)) => return Err(error.into()),
        }
    }
}

fn text_message<T: serde::Serialize>(value: &T) -> anyhow::Result<Message> {
    Ok(Message::Text(serde_json::to_string(value)?.into()))
}

async fn send_now<S, T>(socket: &mut WebSocketStream<S>, value: &T) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: serde::Serialize,
{
    within_stall(socket.send(text_message(value)?)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use crate::daemon::WakuBackend;
    #[cfg(unix)]
    use crate::model::Project;
    use crate::model::{AgentSession, ProviderKind};
    #[cfg(unix)]
    use crate::persistence::StateStore;
    #[cfg(unix)]
    use crate::settings::DaemonSettingsStore;
    use crate::{DaemonSettings, WireDriverStartOptions};
    #[cfg(unix)]
    use base64::Engine as _;
    use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, bounded, unbounded};
    use serde_json::json;
    use std::path::PathBuf;
    use waku_client::{DaemonClient, DaemonSupervisor};

    /// The receiving end of a connection queue, for driving the hub and the
    /// dispatcher without a socket.
    struct TestQueue(mpsc::Receiver<Outbound>);

    fn decode(outbound: Outbound) -> ServerMessage {
        match outbound {
            Outbound::Text(frame) | Outbound::Final(frame) => {
                serde_json::from_str(frame.as_str()).unwrap()
            }
        }
    }

    /// A non-delta event that carries `text`, so it is never coalesced.
    fn marker(text: &str) -> WireDriverEvent {
        WireDriverEvent::Error(text.into())
    }

    fn marker_text(event: &SequencedEvent) -> &str {
        match &event.event {
            WireDriverEvent::Error(text) => text,
            other => panic!("expected a marker event, got {}", other.kind()),
        }
    }

    impl TestQueue {
        fn try_recv(&mut self) -> Result<ServerMessage, mpsc::error::TryRecvError> {
            self.0.try_recv().map(decode)
        }

        fn recv_timeout(&mut self, timeout: Duration) -> Result<ServerMessage, ()> {
            let deadline = Instant::now() + timeout;
            loop {
                match self.0.try_recv() {
                    Ok(message) => return Ok(decode(message)),
                    Err(mpsc::error::TryRecvError::Disconnected) => return Err(()),
                    Err(mpsc::error::TryRecvError::Empty) if Instant::now() >= deadline => {
                        return Err(());
                    }
                    Err(mpsc::error::TryRecvError::Empty) => {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                }
            }
        }

        fn recv(&mut self) -> Result<ServerMessage, ()> {
            self.recv_timeout(Duration::from_secs(5))
        }
    }

    fn test_queue() -> (Outgoing, TestQueue) {
        test_queue_with_capacity(MAX_QUEUED_MESSAGES_PER_SUBSCRIBER)
    }

    fn test_queue_with_capacity(capacity: usize) -> (Outgoing, TestQueue) {
        let (sender, receiver) = mpsc::channel(capacity);
        (sender, TestQueue(receiver))
    }

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    }

    #[derive(Default)]
    struct TestBackend {
        runtimes: Mutex<HashMap<Uuid, Uuid>>,
    }

    impl Backend for TestBackend {
        fn handle(&self, request: Request, events: EventSink) -> anyhow::Result<ResponsePayload> {
            let session_id = request.session_id;
            let runtime_id = request.runtime_id;
            match request.command {
                Command::Start { .. } => {
                    self.runtimes.lock().insert(session_id, runtime_id);
                    events.send(WireDriverEvent::Connected(None))?;
                    Ok(ResponsePayload::Started {
                        supports_steer: true,
                    })
                }
                Command::AttachSession => Ok(ResponsePayload::SessionRuntime {
                    runtime_id: self.runtimes.lock().get(&session_id).copied(),
                    supports_steer: true,
                }),
                Command::GetSettings => Ok(ResponsePayload::Settings {
                    settings: DaemonSettings::default(),
                }),
                Command::Prompt { prompt, .. } => {
                    events.send(WireDriverEvent::TextDelta(prompt))?;
                    Ok(ResponsePayload::Ack)
                }
                Command::CloseSession => {
                    self.runtimes.lock().remove(&session_id);
                    Ok(ResponsePayload::Ack)
                }
                _ => Ok(ResponsePayload::Ack),
            }
        }
    }

    #[derive(Default)]
    struct TaskStateBackend {
        sessions: Mutex<Vec<AgentSession>>,
    }

    impl Backend for TaskStateBackend {
        fn handle(&self, request: Request, _events: EventSink) -> anyhow::Result<ResponsePayload> {
            match request.command {
                Command::SaveTaskState { sessions, .. } => {
                    let mut stored = self.sessions.lock();
                    for session in sessions {
                        if let Some(existing) =
                            stored.iter_mut().find(|existing| existing.id == session.id)
                        {
                            *existing = session;
                        } else {
                            stored.push(session);
                        }
                    }
                    Ok(ResponsePayload::TaskStateSaved {
                        sessions: stored.clone(),
                    })
                }
                Command::LoadTaskState => Ok(ResponsePayload::TaskState {
                    projects: Vec::new(),
                    sessions: self.sessions.lock().clone(),
                    default_cwd: PathBuf::from("/tmp"),
                    projectless_root: Some(PathBuf::from("/tmp/.waku/projects")),
                }),
                _ => Ok(ResponsePayload::Ack),
            }
        }
    }

    #[test]
    fn task_state_revisions_notify_other_clients_only() {
        let hub = Hub::default();
        let (source_tx, mut source_rx) = test_queue();
        let source_id = hub.subscribe(&[], Subscriber::new(source_tx).0);
        let (observer_tx, mut observer_rx) = test_queue();
        hub.subscribe(&[], Subscriber::new(observer_tx).0);

        hub.task_state_changed(source_id);

        assert!(source_rx.try_recv().is_err());
        assert!(matches!(
            observer_rx.recv_timeout(Duration::from_secs(1)),
            Ok(ServerMessage::TaskStateChanged { revision: 1 })
        ));
    }

    #[test]
    fn websocket_task_state_changes_reach_another_client() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = ShutdownSignal::new();
        let server_shutdown = shutdown.clone();
        let server = std::thread::spawn(move || {
            serve(
                listener,
                "secret".into(),
                Arc::new(TaskStateBackend::default()),
                server_shutdown,
                ServerOptions {
                    allow_shutdown: true,
                    ..ServerOptions::default()
                },
            )
            .unwrap()
        });

        let source = DaemonClient::connect(&address.to_string(), "secret".into()).unwrap();
        let observer = DaemonClient::connect(&address.to_string(), "secret".into()).unwrap();
        let source_revisions = source.subscribe_task_state();
        let observer_revisions = observer.subscribe_task_state();
        // The handshake can finish before the server registers its subscriber.
        // Complete the observer's initial load before another client publishes.
        assert!(matches!(
            observer
                .request(Uuid::nil(), Uuid::nil(), Command::LoadTaskState)
                .unwrap(),
            ResponsePayload::TaskState { .. }
        ));
        let session = AgentSession::new(Uuid::new_v4(), ProviderKind::Codex);
        let session_id = session.id;

        assert!(matches!(
            source
                .request(
                    Uuid::nil(),
                    Uuid::nil(),
                    Command::SaveTaskState {
                        projects: Vec::new(),
                        live_session_ids: vec![session_id],
                        sessions: vec![session],
                    },
                )
                .unwrap(),
            ResponsePayload::TaskStateSaved { .. }
        ));
        assert_eq!(
            observer_revisions.recv_timeout(Duration::from_secs(1)),
            Ok(1)
        );
        assert!(source_revisions.try_recv().is_err());
        let ResponsePayload::TaskState { sessions, .. } = observer
            .request(Uuid::nil(), Uuid::nil(), Command::LoadTaskState)
            .unwrap()
        else {
            panic!("expected daemon task state");
        };
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, session_id);

        // Streaming checkpoints update transcript detail and `updated_at`,
        // but do not change anything rendered in another client's task
        // catalog. They must not trigger a list reload for every stream save.
        let mut checkpoint = sessions[0].clone();
        checkpoint.updated_at = checkpoint.updated_at.saturating_add(1);
        source
            .request(
                Uuid::nil(),
                Uuid::nil(),
                Command::SaveTaskState {
                    projects: Vec::new(),
                    live_session_ids: vec![session_id],
                    sessions: vec![checkpoint],
                },
            )
            .unwrap();
        assert!(
            observer_revisions
                .recv_timeout(Duration::from_millis(100))
                .is_err()
        );

        // Desktop persistence uses fire-and-forget notifications, while Web
        // uses requests. Both directions must wake the other application's
        // catalog without echoing back to the source connection.
        let second = AgentSession::new(Uuid::new_v4(), ProviderKind::Claude);
        let second_id = second.id;
        observer
            .notify(
                Uuid::nil(),
                Uuid::nil(),
                Command::SaveTaskState {
                    projects: Vec::new(),
                    live_session_ids: vec![session_id, second_id],
                    sessions: vec![second],
                },
            )
            .unwrap();
        assert_eq!(source_revisions.recv_timeout(Duration::from_secs(1)), Ok(2));
        assert!(observer_revisions.try_recv().is_err());
        let ResponsePayload::TaskState { sessions, .. } = source
            .request(Uuid::nil(), Uuid::nil(), Command::LoadTaskState)
            .unwrap()
        else {
            panic!("expected daemon task state");
        };
        assert_eq!(sessions.len(), 2);
        assert!(sessions.iter().any(|session| session.id == second_id));

        source.shutdown();
        server.join().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn stale_projection_cannot_resurrect_a_removed_session() {
        let root = std::env::temp_dir().join(format!("waku-remove-race-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let backend = WakuBackend::new(
            DaemonSettingsStore::open(root.join("settings.json")).unwrap(),
            StateStore::daemon(root.join("app.db")),
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = ShutdownSignal::new();
        let server_shutdown = shutdown.clone();
        let server = std::thread::spawn(move || {
            serve(
                listener,
                "secret".into(),
                Arc::new(backend),
                server_shutdown,
                ServerOptions {
                    allow_shutdown: true,
                    ..ServerOptions::default()
                },
            )
            .unwrap()
        });

        let stale_client = DaemonClient::connect(&address.to_string(), "secret".into()).unwrap();
        let remover = DaemonClient::connect(&address.to_string(), "secret".into()).unwrap();
        let project = Project::from_path(root.join("repo"));
        let mut session = AgentSession::new(project.id, ProviderKind::Codex);
        session.begin_turn("persist me");
        stale_client
            .request(
                Uuid::nil(),
                Uuid::nil(),
                Command::SaveTaskState {
                    projects: vec![project.clone()],
                    live_session_ids: vec![session.id],
                    sessions: vec![session.clone()],
                },
            )
            .unwrap();
        remover
            .request(session.id, Uuid::nil(), Command::RemoveSession)
            .unwrap();
        let ResponsePayload::TaskStateSaved { sessions } = stale_client
            .request(
                Uuid::nil(),
                Uuid::nil(),
                Command::SaveTaskState {
                    projects: vec![project],
                    live_session_ids: vec![session.id],
                    sessions: vec![session],
                },
            )
            .unwrap()
        else {
            panic!("expected task-state save response");
        };
        assert!(sessions.is_empty());
        let ResponsePayload::TaskState { sessions, .. } = stale_client
            .request(Uuid::nil(), Uuid::nil(), Command::LoadTaskState)
            .unwrap()
        else {
            panic!("expected task state");
        };
        assert!(sessions.is_empty());

        stale_client.shutdown();
        server.join().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn websocket_round_trip_sequences_provider_events() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = ShutdownSignal::new();
        let server_shutdown = shutdown.clone();
        let server = std::thread::spawn(move || {
            serve(
                listener,
                "secret".into(),
                Arc::new(TestBackend::default()),
                server_shutdown,
                ServerOptions {
                    allow_shutdown: true,
                    ..ServerOptions::default()
                },
            )
            .unwrap()
        });

        assert!(
            DaemonClient::connect(&address.to_string(), "wrong-secret".into()).is_err(),
            "the server must reject a client before it can issue requests"
        );
        let client = DaemonClient::connect(&address.to_string(), "secret".into()).unwrap();
        let session_id = Uuid::new_v4();
        let runtime_id = Uuid::new_v4();
        let response = client
            .request(
                session_id,
                runtime_id,
                Command::Start {
                    options: WireDriverStartOptions {
                        provider: "codex".into(),
                        binary: PathBuf::from("codex"),
                        cwd: PathBuf::from("."),
                        mode: "fullAccess".into(),
                        model: None,
                        reasoning_effort: None,
                        service_tier: None,
                        context_window: None,
                        agent_preset: None,
                        computer_use_enabled: false,
                        provider_cursor: None,
                    },
                },
            )
            .unwrap();
        assert!(matches!(
            response,
            ResponsePayload::Started {
                supports_steer: true
            }
        ));
        // Start can emit before a refreshed app discovers and subscribes to
        // the daemon-owned runtime. The client must retain that replay.
        let events = client.subscribe(session_id, runtime_id);
        let event = events.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(event.runtime_id, runtime_id);
        assert_eq!(event.sequence, 1);
        assert_eq!(event.event.kind(), "connected");

        client.shutdown();
        assert!(matches!(
            events.recv_timeout(Duration::from_secs(1)),
            Err(RecvTimeoutError::Disconnected)
        ));
        server.join().unwrap();
    }

    #[test]
    fn late_client_attaches_to_replay_and_live_runtime_events() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = ShutdownSignal::new();
        let server_shutdown = shutdown.clone();
        let server = std::thread::spawn(move || {
            serve(
                listener,
                "secret".into(),
                Arc::new(TestBackend::default()),
                server_shutdown,
                ServerOptions {
                    allow_shutdown: true,
                    ..ServerOptions::default()
                },
            )
            .unwrap()
        });

        let source = DaemonClient::connect(&address.to_string(), "secret".into()).unwrap();
        let session_id = Uuid::new_v4();
        let runtime_id = Uuid::new_v4();
        let source_events = source.subscribe(session_id, runtime_id);
        source
            .request(
                session_id,
                runtime_id,
                Command::Start {
                    options: WireDriverStartOptions {
                        provider: "codex".into(),
                        binary: PathBuf::from("codex"),
                        cwd: PathBuf::from("."),
                        mode: "fullAccess".into(),
                        model: None,
                        reasoning_effort: None,
                        service_tier: None,
                        context_window: None,
                        agent_preset: None,
                        computer_use_enabled: false,
                        provider_cursor: None,
                    },
                },
            )
            .unwrap();
        assert_eq!(
            source_events
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .event
                .kind(),
            "connected"
        );

        let late = DaemonClient::connect(&address.to_string(), "secret".into()).unwrap();
        assert!(matches!(
            late.request(session_id, Uuid::nil(), Command::AttachSession)
                .unwrap(),
            ResponsePayload::SessionRuntime {
                runtime_id: Some(attached),
                supports_steer: true,
            } if attached == runtime_id
        ));
        let late_events = late.subscribe(session_id, runtime_id);
        let replayed = late_events.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(replayed.sequence, 1);
        assert_eq!(replayed.event.kind(), "connected");

        source
            .request(
                session_id,
                runtime_id,
                Command::Prompt {
                    prompt: "streamed from the first client".into(),
                    turn_id: None,
                    message_id: None,
                },
            )
            .unwrap();
        for events in [&source_events, &late_events] {
            let live = events.recv_timeout(Duration::from_secs(1)).unwrap();
            assert_eq!(live.sequence, 2);
            assert_eq!(live.event.kind(), "textDelta");
            assert_eq!(
                live.event.payload(),
                json!("streamed from the first client")
            );
        }

        source
            .request(session_id, runtime_id, Command::CloseSession)
            .unwrap();
        let after_close = DaemonClient::connect(&address.to_string(), "secret".into()).unwrap();
        assert!(matches!(
            after_close
                .request(session_id, Uuid::nil(), Command::AttachSession)
                .unwrap(),
            ResponsePayload::SessionRuntime {
                runtime_id: None,
                supports_steer: true,
            }
        ));
        let stale_events = after_close.subscribe(session_id, runtime_id);
        assert!(
            stale_events
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "an explicitly closed runtime must not replay into future clients"
        );

        source.shutdown();
        server.join().unwrap();
    }

    #[test]
    fn remote_supervisor_reconnects_without_losing_the_daemon_runtime() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        // Keep the address reserved between servers. If the first listener is
        // dropped before the replacement binds, a parallel test can claim the
        // newly freed ephemeral port and make this test fail with AddrInUse.
        let replacement_listener = listener.try_clone().unwrap();
        let address = listener.local_addr().unwrap();
        let backend = Arc::new(TestBackend::default());
        let first_shutdown = ShutdownSignal::new();
        let first_server = {
            let backend = backend.clone();
            let shutdown = first_shutdown.clone();
            std::thread::spawn(move || {
                serve(
                    listener,
                    "secret".into(),
                    backend,
                    shutdown,
                    ServerOptions::default(),
                )
                .unwrap()
            })
        };

        let supervisor = DaemonSupervisor::connect(&address.to_string(), "secret".into()).unwrap();
        let clients = supervisor.subscribe_clients();
        let initial = clients.recv_timeout(Duration::from_secs(1)).unwrap();
        let session_id = Uuid::new_v4();
        let runtime_id = Uuid::new_v4();
        initial
            .request(
                session_id,
                runtime_id,
                Command::Start {
                    options: WireDriverStartOptions {
                        provider: "codex".into(),
                        binary: PathBuf::from("codex"),
                        cwd: PathBuf::from("."),
                        mode: "fullAccess".into(),
                        model: None,
                        reasoning_effort: None,
                        service_tier: None,
                        context_window: None,
                        agent_preset: None,
                        computer_use_enabled: false,
                        provider_cursor: None,
                    },
                },
            )
            .unwrap();

        first_shutdown.trigger();
        first_server.join().unwrap();

        let second_shutdown = ShutdownSignal::new();
        let second_server = {
            let backend = backend.clone();
            let shutdown = second_shutdown.clone();
            std::thread::spawn(move || {
                serve(
                    replacement_listener,
                    "secret".into(),
                    backend,
                    shutdown,
                    ServerOptions::default(),
                )
                .unwrap()
            })
        };

        let replacement = clients.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(!initial.same_connection(&replacement));
        assert!(matches!(
            replacement
                .request(session_id, Uuid::nil(), Command::AttachSession)
                .unwrap(),
            ResponsePayload::SessionRuntime {
                runtime_id: Some(attached),
                supports_steer: true,
            } if attached == runtime_id
        ));

        second_shutdown.trigger();
        second_server.join().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn dropping_an_idle_terminal_does_not_wait_for_output() {
        let root = std::env::temp_dir().join(format!("waku-terminal-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let hub = Arc::new(Hub::default());
        let terminal = crate::terminal::DaemonTerminal::open_with_shell(
            &root,
            80,
            24,
            hub.event_sink(Uuid::new_v4(), Uuid::new_v4()),
            terminal_test_shell("while IFS= read -r line; do :; done"),
        )
        .unwrap();
        let (dropped, finished) = bounded(1);
        std::thread::spawn(move || {
            drop(terminal);
            let _ = dropped.send(());
        });

        assert!(
            finished.recv_timeout(Duration::from_secs(3)).is_ok(),
            "dropping an idle daemon terminal blocked on its output reader"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn websocket_terminal_round_trip_streams_input_and_output() {
        websocket_terminal_round_trip(false);
    }

    #[cfg(unix)]
    #[test]
    fn websocket_terminal_close_does_not_wait_for_a_shell_ignoring_hangup() {
        websocket_terminal_round_trip(true);
    }

    #[cfg(unix)]
    fn terminal_test_shell(script: &str) -> alacritty_terminal::tty::Shell {
        // Do not load the developer's or CI runner's login files, prompt
        // plugins, or terminal capability queries in a transport test.
        alacritty_terminal::tty::Shell::new("/bin/sh".into(), vec!["-c".into(), script.into()])
    }

    #[cfg(unix)]
    fn websocket_terminal_round_trip(ignore_hangup: bool) {
        let root = std::env::temp_dir().join(format!("waku-terminal-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let backend = WakuBackend::new(
            DaemonSettingsStore::open(root.join("settings.json")).unwrap(),
            StateStore::daemon(root.join("app.db")),
        )
        .unwrap()
        .with_terminal_shell(terminal_test_shell(&format!(
            "{}\nprintf 'ready:%s\\n' \"$$\"\nwhile IFS= read -r line; do printf 'received:%s\\n' \"$line\"; done",
            if ignore_hangup { "trap '' HUP" } else { ":" },
        )));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = ShutdownSignal::new();
        let server_shutdown = shutdown.clone();
        let server = std::thread::spawn(move || {
            serve(
                listener,
                "secret".into(),
                Arc::new(backend),
                server_shutdown,
                ServerOptions {
                    allow_shutdown: true,
                    ..ServerOptions::default()
                },
            )
            .unwrap()
        });

        let client = DaemonClient::connect(&address.to_string(), "secret".into()).unwrap();
        let terminal_id = Uuid::new_v4();
        let events = client.subscribe(terminal_id, terminal_id);
        assert!(matches!(
            client
                .request(
                    terminal_id,
                    terminal_id,
                    Command::OpenTerminal {
                        cwd: root.clone(),
                        cols: 80,
                        rows: 24,
                    },
                )
                .unwrap(),
            ResponsePayload::Ack
        ));
        // Wait until the shell has installed its signal handler. Receiving
        // local echo alone does not prove that shell startup has completed.
        let ready = terminal_output_until(&events, b"\n");
        let child_pid: libc::pid_t = String::from_utf8_lossy(&ready)
            .trim()
            .strip_prefix("ready:")
            .unwrap()
            .parse()
            .unwrap();
        client
            .request(
                terminal_id,
                terminal_id,
                Command::WriteTerminal {
                    data: b"waku-terminal-round-trip\r".to_vec(),
                },
            )
            .unwrap();

        // The response prefix is absent from the input, so a PTY echo cannot
        // satisfy this assertion before the child has actually read it.
        terminal_output_until(&events, b"received:waku-terminal-round-trip");

        let (closed, finished) = bounded(1);
        let closing_client = client.clone();
        let close = std::thread::spawn(move || {
            let _ = closed.send(closing_client.request(
                terminal_id,
                terminal_id,
                Command::CloseTerminal,
            ));
        });
        let result = finished.recv_timeout(Duration::from_secs(3));
        if result.is_err() {
            // Clean up the fixture even when shutdown regresses, and fail
            // here instead of waiting for the client's 120-second timeout.
            unsafe {
                libc::kill(child_pid, libc::SIGKILL);
            }
        }
        client.shutdown();
        server.join().unwrap();
        close.join().unwrap();
        std::fs::remove_dir_all(root).unwrap();
        assert!(
            matches!(result, Ok(Ok(ResponsePayload::Ack))),
            "closing daemon terminal did not complete: {result:?}"
        );
    }

    #[cfg(unix)]
    fn terminal_output_until(events: &Receiver<SequencedEvent>, marker: &[u8]) -> Vec<u8> {
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        let mut output = Vec::new();
        let mut seen_events = Vec::new();
        while std::time::Instant::now() < deadline
            && !output.windows(marker.len()).any(|window| window == marker)
        {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let Ok(event) = events.recv_timeout(remaining) else {
                break;
            };
            seen_events.push(event.event.kind());
            let WireDriverEvent::TerminalOutput(chunk) = &event.event else {
                continue;
            };
            let data = &chunk.data;
            output.extend(
                base64::engine::general_purpose::STANDARD
                    .decode(data)
                    .unwrap(),
            );
        }
        assert!(
            output.windows(marker.len()).any(|window| window == marker),
            "daemon terminal did not return the shell marker; events={seen_events:?}, output={}",
            String::from_utf8_lossy(&output)
        );
        output
    }

    #[test]
    fn nil_request_ids_execute_without_responses_or_cache_entries() {
        let (outgoing, mut responses) = test_queue();
        let hub = Arc::new(Hub::default());
        let handled = handle_request(
            Request {
                request_id: Uuid::nil(),
                session_id: Uuid::nil(),
                runtime_id: Uuid::nil(),
                command: Command::GetSettings,
            },
            outgoing,
            0,
            Arc::new(TestBackend::default()),
            hub.clone(),
        );

        assert!(matches!(handled.outcome, Some(ResponseOutcome::Ok { .. })));
        assert!(responses.try_recv().is_err());
        assert!(hub.cached_response(Uuid::nil()).is_none());
    }

    #[test]
    fn browser_origins_are_denied_unless_explicitly_allowed() {
        let request = HandshakeRequest::builder()
            .uri("/v1")
            .header(ORIGIN, "https://app.waku.test")
            .body(())
            .unwrap();
        let response = HandshakeResponse::new(());
        assert_eq!(
            validate_handshake(&request, response, &HashSet::new())
                .unwrap_err()
                .status(),
            StatusCode::FORBIDDEN
        );

        let allowed = HashSet::from(["https://app.waku.test".to_owned()]);
        assert!(validate_handshake(&request, HandshakeResponse::new(()), &allowed).is_ok());
        let native = HandshakeRequest::builder().uri("/v1").body(()).unwrap();
        assert!(validate_handshake(&native, HandshakeResponse::new(()), &HashSet::new()).is_ok());

        let react_native = HandshakeRequest::builder()
            .uri("/v1")
            .header(ORIGIN, "http://192.168.0.114:34125")
            .header(NATIVE_CLIENT_HEADER, NATIVE_CLIENT_HEADER_VALUE)
            .body(())
            .unwrap();
        assert!(
            validate_handshake(&react_native, HandshakeResponse::new(()), &HashSet::new()).is_ok()
        );

        let forged_native = HandshakeRequest::builder()
            .uri("/v1")
            .header(ORIGIN, "https://attacker.example")
            .header(NATIVE_CLIENT_HEADER, "browser")
            .body(())
            .unwrap();
        assert_eq!(
            validate_handshake(&forged_native, HandshakeResponse::new(()), &HashSet::new())
                .unwrap_err()
                .status(),
            StatusCode::FORBIDDEN
        );
    }

    #[test]
    fn unauthenticated_clients_cannot_send_more_than_a_hello() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = ShutdownSignal::new();
        let server_shutdown = shutdown.clone();
        let server = std::thread::spawn(move || {
            serve(
                listener,
                "secret".into(),
                Arc::new(TestBackend::default()),
                server_shutdown,
                ServerOptions::default(),
            )
            .unwrap()
        });

        // A hello far larger than the pre-authentication budget is refused
        // before the daemon buffers it, so the handshake fails.
        let oversized = "x".repeat(MAX_PRE_AUTH_BYTES * 2);
        assert!(DaemonClient::connect(&address.to_string(), oversized).is_err());
        // An ordinary client is unaffected, and its requests are answered
        // without the old 25 ms polling delay.
        let client = DaemonClient::connect(&address.to_string(), "secret".into()).unwrap();
        let started = Instant::now();
        for _ in 0..20 {
            assert!(matches!(
                client
                    .request(Uuid::nil(), Uuid::nil(), Command::GetSettings)
                    .unwrap(),
                ResponsePayload::Settings { .. }
            ));
        }
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "20 round trips took {:?}",
            started.elapsed()
        );

        shutdown.trigger();
        server.join().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn local_socket_is_private_and_serves_without_tcp() {
        use std::os::unix::fs::PermissionsExt as _;

        // Short, so the path fits in `sun_path` on every runner.
        let root =
            PathBuf::from("/tmp").join(format!("wk-{}", &Uuid::new_v4().simple().to_string()[..8]));
        let path = root.join("run").join("daemon.sock");
        let listener = LocalListener::bind(&path).unwrap();
        assert_eq!(listener.path(), path);
        let mode =
            |path: &std::path::Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&path), 0o600);
        assert_eq!(mode(path.parent().unwrap()), 0o700);
        // A second daemon of the same user cannot take the locked default
        // path, so it binds a pid-suffixed one beside it.
        let second = LocalListener::bind(&path).unwrap();
        assert_ne!(second.path(), path);
        drop(second);

        let shutdown = ShutdownSignal::new();
        let server_shutdown = shutdown.clone();
        let server = std::thread::spawn(move || {
            serve_listeners(
                Listeners {
                    tcp: None,
                    local: Some(listener),
                },
                "secret".into(),
                Arc::new(TestBackend::default()),
                server_shutdown,
                ServerOptions::default(),
            )
            .unwrap()
        });
        let address = format!("unix:{}", path.display());
        assert!(DaemonClient::connect(&address, "wrong".into()).is_err());
        let client = DaemonClient::connect(&address, "secret".into()).unwrap();
        assert!(matches!(
            client
                .request(Uuid::nil(), Uuid::nil(), Command::GetSettings)
                .unwrap(),
            ResponsePayload::Settings { .. }
        ));

        shutdown.trigger();
        server.join().unwrap();
        assert!(
            !path.exists(),
            "the daemon removes its socket when it stops"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    fn next_event(queue: &mut TestQueue) -> SequencedEvent {
        match queue.recv().expect("an event") {
            ServerMessage::Event(event) => event,
            other => panic!("expected an event, got {other:?}"),
        }
    }

    #[test]
    fn streamed_deltas_are_coalesced_in_order() {
        let hub = Arc::new(Hub::default());
        let session_id = Uuid::new_v4();
        let runtime_id = Uuid::new_v4();
        let (outgoing, mut events) = test_queue();
        hub.subscribe(&[], Subscriber::new(outgoing).0);
        hub.begin_runtime(session_id, runtime_id);
        let sink = hub.event_sink(session_id, runtime_id);

        for chunk in ["Hel", "lo", ", world"] {
            sink.send(WireDriverEvent::TextDelta(chunk.into())).unwrap();
        }
        // Nothing is sent before the window closes...
        assert!(events.try_recv().is_err());
        // ...and then the chunks arrive as one event.
        let merged = next_event(&mut events);
        assert!(
            matches!(&merged.event, WireDriverEvent::TextDelta(text) if text == "Hello, world")
        );
        assert_eq!(merged.sequence, 1);

        // A different kind, or any other event, flushes what is pending
        // first, so the stream keeps its order.
        sink.send(WireDriverEvent::TextDelta("a".into())).unwrap();
        sink.send(WireDriverEvent::ReasoningDelta("b".into()))
            .unwrap();
        sink.send(marker("done")).unwrap();
        let text = next_event(&mut events);
        let reasoning = next_event(&mut events);
        let done = next_event(&mut events);
        assert!(matches!(&text.event, WireDriverEvent::TextDelta(text) if text == "a"));
        assert!(matches!(&reasoning.event, WireDriverEvent::ReasoningDelta(text) if text == "b"));
        assert_eq!(marker_text(&done), "done");
        assert_eq!(
            [text.sequence, reasoning.sequence, done.sequence],
            [2, 3, 4]
        );

        // Closing the runtime sends text still inside its window.
        sink.send(WireDriverEvent::TextDelta("tail".into()))
            .unwrap();
        hub.end_runtime(session_id, Some(runtime_id));
        let tail = next_event(&mut events);
        assert!(matches!(&tail.event, WireDriverEvent::TextDelta(text) if text == "tail"));
    }

    #[test]
    fn replay_journal_is_bounded_by_bytes() {
        let hub = Arc::new(Hub::default());
        let session_id = Uuid::new_v4();
        let runtime_id = Uuid::new_v4();
        hub.begin_runtime(session_id, runtime_id);
        let sink = hub.event_sink(session_id, runtime_id);
        let large = "x".repeat(1024 * 1024);
        for index in 0..20 {
            sink.send(marker(&format!("{index}:{large}"))).unwrap();
        }

        let (outgoing, mut events) = test_queue();
        hub.subscribe(&[], Subscriber::new(outgoing).0);
        let mut replayed = Vec::new();
        while let Ok(ServerMessage::Event(event)) = events.try_recv() {
            replayed.push(event);
        }
        // About 8 MiB of 1 MiB events survive, newest last.
        assert!((6..=8).contains(&replayed.len()), "{}", replayed.len());
        assert!(marker_text(replayed.last().unwrap()).starts_with("19:"));
        assert!(
            replayed
                .windows(2)
                .all(|pair| pair[1].sequence == pair[0].sequence + 1)
        );
    }

    #[test]
    fn daemon_tokens_require_an_exact_match() {
        assert!(token_matches("secret", "secret"));
        assert!(!token_matches("secret", "Secret"));
        assert!(!token_matches("secret", "secret-extra"));
    }

    #[test]
    fn replaced_runtime_ignores_late_events_from_the_old_generation() {
        let hub = Arc::new(Hub::default());
        let session_id = Uuid::new_v4();
        let old_runtime_id = Uuid::new_v4();
        let new_runtime_id = Uuid::new_v4();
        let (outgoing, mut events) = test_queue();
        hub.subscribe(&[], Subscriber::new(outgoing).0);

        hub.begin_runtime(session_id, old_runtime_id);
        let old_sink = hub.event_sink(session_id, old_runtime_id);
        old_sink.send(marker("old")).unwrap();
        assert!(matches!(
            events.recv().unwrap(),
            ServerMessage::Event(event) if event.runtime_id == old_runtime_id
        ));

        hub.begin_runtime(session_id, new_runtime_id);
        old_sink.send(marker("stale")).unwrap();
        hub.event_sink(session_id, new_runtime_id)
            .send(marker("new"))
            .unwrap();

        let ServerMessage::Event(event) = events.recv().unwrap() else {
            panic!("expected a daemon event");
        };
        assert_eq!(event.runtime_id, new_runtime_id);
        assert_eq!(event.sequence, 1);
        assert_eq!(marker_text(&event), "new");
        assert!(events.try_recv().is_err());
    }

    #[test]
    fn replay_cursor_from_an_old_daemon_epoch_does_not_hide_new_events() {
        let hub = Arc::new(Hub::default());
        let session_id = Uuid::new_v4();
        let runtime_id = Uuid::new_v4();
        hub.begin_runtime(session_id, runtime_id);
        hub.event_sink(session_id, runtime_id)
            .send(marker("new-daemon"))
            .unwrap();

        let (outgoing, mut events) = test_queue();
        hub.subscribe(
            &[ReplayCursor {
                session_id,
                runtime_id,
                epoch: Uuid::nil(),
                sequence: u64::MAX,
            }],
            Subscriber::new(outgoing).0,
        );

        let ServerMessage::Event(event) = events.recv().unwrap() else {
            panic!("expected the new daemon event to replay");
        };
        assert_eq!(event.epoch, hub.epoch);
        assert_eq!(event.sequence, 1);
    }

    #[test]
    fn subscriber_that_cannot_keep_up_is_kicked_and_removed() {
        let hub = Arc::new(Hub::default());
        let session_id = Uuid::new_v4();
        let runtime_id = Uuid::new_v4();
        // Capacity one and no drainer: the second queued message overflows.
        let (outgoing, mut events) = test_queue_with_capacity(1);
        let (subscriber, mut kicked) = Subscriber::new(outgoing);
        hub.subscribe(&[], subscriber);
        hub.begin_runtime(session_id, runtime_id);
        let sink = hub.event_sink(session_id, runtime_id);

        sink.send(marker("one")).unwrap();
        // Nobody drains, so a second queued message overflows: the subscriber
        // is dropped from the hub and its connection is told to close.
        sink.send(marker("two")).unwrap();
        assert!(kicked.try_recv().is_ok());

        assert!(matches!(
            events.try_recv(),
            Ok(ServerMessage::Event(event)) if marker_text(&event) == "one"
        ));
        assert!(events.try_recv().is_err());
        sink.send(marker("three")).unwrap();
        assert!(events.try_recv().is_err());
    }

    #[test]
    fn response_cache_evicts_oversized_and_byte_budget_exceeding_outcomes() {
        let hub = Hub::default();
        let oversized_id = Uuid::new_v4();
        let oversized = ResponseOutcome::Ok {
            payload: ResponsePayload::Cursor {
                cursor: Some(serde_json::Value::String(
                    "x".repeat(MAX_CACHED_RESPONSE_BYTES + 1),
                )),
            },
        };
        hub.cache_response(
            oversized_id,
            encode(&ResponseFrame {
                request_id: oversized_id,
                outcome: &oversized,
            })
            .unwrap(),
        );
        // A single outcome larger than the whole byte budget is never
        // retained; retrying the request runs it again instead of pinning a
        // >64 MB payload in daemon memory.
        assert!(hub.cached_response(oversized_id).is_none());

        let kept_id = Uuid::new_v4();
        let kept = ResponseOutcome::Ok {
            payload: ResponsePayload::Ack,
        };
        let frame = encode(&ResponseFrame {
            request_id: kept_id,
            outcome: &kept,
        })
        .unwrap();
        // The borrowed frame is exactly what `ServerMessage::Response` writes.
        assert_eq!(
            frame.as_str(),
            serde_json::to_string(&ServerMessage::Response {
                request_id: kept_id,
                outcome: kept,
            })
            .unwrap()
        );
        hub.cache_response(kept_id, frame);
        assert!(hub.cached_response(kept_id).is_some());
        assert!(hub.cached_response(oversized_id).is_none());
    }

    struct BlockingProbeBackend {
        probe_started: Sender<()>,
        release_probe: Receiver<()>,
    }

    impl Backend for BlockingProbeBackend {
        fn handle(&self, request: Request, _: EventSink) -> anyhow::Result<ResponsePayload> {
            if matches!(request.command, Command::ProbeProvider { .. }) {
                self.probe_started.send(()).unwrap();
                self.release_probe.recv().unwrap();
            }
            Ok(ResponsePayload::Ack)
        }
    }

    #[test]
    fn slow_background_command_does_not_block_session_hydration() {
        let (outgoing, mut response_rx) = test_queue();
        let (probe_started, probe_started_rx) = bounded(1);
        let (release_probe, release_probe_rx) = bounded(1);
        let backend: Arc<dyn Backend> = Arc::new(BlockingProbeBackend {
            probe_started,
            release_probe: release_probe_rx,
        });
        let hub = Arc::new(Hub::default());
        let runtime = test_runtime();
        let dispatcher = RequestDispatcher::new(backend, hub, runtime.handle().clone());

        let probe_id = Uuid::new_v4();
        dispatcher.dispatch(
            Request {
                request_id: probe_id,
                session_id: Uuid::nil(),
                runtime_id: Uuid::nil(),
                command: Command::ProbeProvider {
                    provider: crate::model::ProviderKind::Codex,
                    binary_override: None,
                    discover_models: false,
                    probe_version: false,
                },
            },
            outgoing.clone(),
            0,
        );
        probe_started_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();

        let hydration_id = Uuid::new_v4();
        dispatcher.dispatch(
            Request {
                request_id: hydration_id,
                session_id: Uuid::nil(),
                runtime_id: Uuid::nil(),
                command: Command::HydrateSession {
                    session_id: Uuid::new_v4(),
                },
            },
            outgoing,
            0,
        );
        assert!(matches!(
            response_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            ServerMessage::Response { request_id, .. } if request_id == hydration_id
        ));

        release_probe.send(()).unwrap();
        assert!(matches!(
            response_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            ServerMessage::Response { request_id, .. } if request_id == probe_id
        ));
    }

    struct RuntimeOrderingBackend {
        blocked_session_id: Uuid,
        handled: Sender<(Uuid, &'static str)>,
        release_start: Receiver<()>,
    }

    impl Backend for RuntimeOrderingBackend {
        fn handle(&self, request: Request, _: EventSink) -> anyhow::Result<ResponsePayload> {
            let command = match request.command {
                Command::Start { .. } => {
                    self.handled.send((request.session_id, "start")).unwrap();
                    if request.session_id == self.blocked_session_id {
                        self.release_start.recv().unwrap();
                    }
                    return Ok(ResponsePayload::Started {
                        supports_steer: true,
                    });
                }
                Command::Prompt { .. } => "prompt",
                Command::CloseSession => "close",
                _ => "other",
            };
            self.handled.send((request.session_id, command)).unwrap();
            Ok(ResponsePayload::Ack)
        }
    }

    #[test]
    fn runtime_commands_are_ordered_per_session_without_blocking_other_sessions() {
        let blocked_session_id = Uuid::new_v4();
        let blocked_runtime_id = Uuid::new_v4();
        let other_session_id = Uuid::new_v4();
        let other_runtime_id = Uuid::new_v4();
        let (handled, handled_rx) = unbounded();
        let (release_start, release_start_rx) = bounded(1);
        let runtime = test_runtime();
        let dispatcher = RequestDispatcher::new(
            Arc::new(RuntimeOrderingBackend {
                blocked_session_id,
                handled,
                release_start: release_start_rx,
            }),
            Arc::new(Hub::default()),
            runtime.handle().clone(),
        );
        let (start_outgoing, mut start_responses) = test_queue();
        let (second_client_outgoing, mut second_client_responses) = test_queue();
        let (other_outgoing, mut other_responses) = test_queue();

        let blocked_start_id = Uuid::new_v4();
        dispatcher.dispatch(
            Request {
                request_id: blocked_start_id,
                session_id: blocked_session_id,
                runtime_id: blocked_runtime_id,
                command: Command::Start {
                    options: test_start_options(),
                },
            },
            start_outgoing,
            0,
        );
        assert_eq!(
            handled_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            (blocked_session_id, "start")
        );

        let prompt_id = Uuid::new_v4();
        dispatcher.dispatch(
            Request {
                request_id: prompt_id,
                session_id: blocked_session_id,
                runtime_id: blocked_runtime_id,
                command: Command::Prompt {
                    prompt: "after start".into(),
                    turn_id: None,
                    message_id: None,
                },
            },
            second_client_outgoing,
            0,
        );

        let other_start_id = Uuid::new_v4();
        dispatcher.dispatch(
            Request {
                request_id: other_start_id,
                session_id: other_session_id,
                runtime_id: other_runtime_id,
                command: Command::Start {
                    options: test_start_options(),
                },
            },
            other_outgoing.clone(),
            0,
        );
        assert_eq!(
            handled_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            (other_session_id, "start")
        );
        assert!(matches!(
            other_responses
                .recv_timeout(Duration::from_secs(1))
                .unwrap(),
            ServerMessage::Response { request_id, .. } if request_id == other_start_id
        ));
        assert!(handled_rx.recv_timeout(Duration::from_millis(50)).is_err());

        release_start.send(()).unwrap();
        assert!(matches!(
            start_responses
                .recv_timeout(Duration::from_secs(1))
                .unwrap(),
            ServerMessage::Response { request_id, .. } if request_id == blocked_start_id
        ));
        assert_eq!(
            handled_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            (blocked_session_id, "prompt")
        );
        assert!(matches!(
            second_client_responses
                .recv_timeout(Duration::from_secs(1))
                .unwrap(),
            ServerMessage::Response { request_id, .. } if request_id == prompt_id
        ));

        let blocked_close_id = Uuid::new_v4();
        dispatcher.dispatch(
            Request {
                request_id: blocked_close_id,
                session_id: blocked_session_id,
                runtime_id: blocked_runtime_id,
                command: Command::CloseSession,
            },
            other_outgoing.clone(),
            0,
        );
        let other_close_id = Uuid::new_v4();
        dispatcher.dispatch(
            Request {
                request_id: other_close_id,
                session_id: other_session_id,
                runtime_id: other_runtime_id,
                command: Command::CloseSession,
            },
            other_outgoing,
            0,
        );
        let mut close_responses = [false; 2];
        for _ in 0..2 {
            let ServerMessage::Response { request_id, .. } = other_responses
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
            else {
                panic!("expected a close response");
            };
            if request_id == blocked_close_id {
                close_responses[0] = true;
            } else if request_id == other_close_id {
                close_responses[1] = true;
            }
        }
        assert_eq!(close_responses, [true, true]);
    }

    fn test_start_options() -> WireDriverStartOptions {
        WireDriverStartOptions {
            provider: "codex".into(),
            binary: PathBuf::from("codex"),
            cwd: PathBuf::from("."),
            mode: "fullAccess".into(),
            model: None,
            reasoning_effort: None,
            service_tier: None,
            context_window: None,
            agent_preset: None,
            computer_use_enabled: false,
            provider_cursor: None,
        }
    }
}
