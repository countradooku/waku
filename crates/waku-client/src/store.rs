//! The client-side projection store (#55): the desktop's only source of
//! thread and project state.
//!
//! One worker thread owns the v8 connection's `shell` subscription, a
//! bounded set of `thread` subscriptions (the threads the user has open),
//! the `commandOutput` subscriptions of expanded command rows, and every
//! call the desktop makes (`orchestration.dispatch`, history pages, full
//! turn items). It folds stream items into [`ThreadState`] / [`ShellState`]
//! with the [`crate::projection`] reducers, derives render views
//! ([`ThreadView`], [`ShellView`]) off the UI thread, and publishes them
//! into a slot the UI drains with [`ProjectionStore::take_updates`]. A
//! publish replaces the previous unconsumed one, so however fast a provider
//! streams, the UI applies at most one view per thread per drain, at its
//! own commit cadence; each update carries a compact [`ThreadChange`] so the
//! UI can route it to the cheapest redraw.
//!
//! Nothing is persisted and nothing is written back: the daemon owns the
//! state, mutations are `orchestration.dispatch` commands, and a reconnect
//! resubscribes every stream from its last applied cursor (RFC 0003 §2.6),
//! re-issuing in-flight dispatches with their command ids.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvError, Select, Sender, unbounded};
use parking_lot::{Condvar, Mutex};
use uuid::Uuid;

use crate::projection::{CommandOutputState, ShellState, ThreadState};
use crate::thread_view::{ShellCache, ShellView, ThreadView, ViewCache, ViewExtras};
use crate::v8::{
    CallError, EndReason, Error, PendingCall, StreamEvent, StreamItem, SubscribeOptions,
    Subscription, V8Client, method, stream,
};
use waku_protocol::orchestration::{
    Command, CommandId, CommandTarget, DomainEvent, ThreadId, TurnItem, TurnItemId, TurnItemKind,
};
use waku_protocol::v8::orchestration::{
    CommandOutputSpec, DispatchCommand, DispatchParams, DispatchResult, HistoryPage,
    HistoryPageParams, ShellLocation, ShellSpec, ThreadItem, ThreadSpec, TurnItemParams,
};

/// Opens a fresh v8 connection; called again after a disconnect.
pub type Connector = Box<dyn FnMut() -> anyhow::Result<V8Client> + Send>;

#[derive(Clone, Debug)]
pub struct StoreConfig {
    /// Threads kept subscribed at once; the least recently opened beyond
    /// this is closed and its state dropped.
    pub max_open_threads: usize,
    /// Command rows whose output streams at once.
    pub max_output_watches: usize,
    /// Unprojected turn items kept per open thread.
    pub max_full_items: usize,
    /// A thread's view is rebuilt and published at most this often.
    pub publish_interval: Duration,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            max_open_threads: 8,
            max_output_watches: 16,
            max_full_items: 32,
            publish_interval: Duration::from_millis(16),
        }
    }
}

/// What changed in a thread since the UI last drained it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ThreadChange {
    /// Live text of a streaming answer, reasoning or plan grew.
    pub text: bool,
    /// Transcript rows were added or changed (tool activity included).
    pub rows: bool,
    /// Tool activity changed: what the right panel shows may have moved.
    pub tools: bool,
    /// Runs, requests, the queue or the thread's own columns changed.
    pub state: bool,
    /// The stream (re)started from a snapshot: rows may have changed anywhere.
    pub snapshot: bool,
    /// Older history merged in above the rows already shown.
    pub history: bool,
}

impl ThreadChange {
    pub fn union(self, other: Self) -> Self {
        Self {
            text: self.text || other.text,
            rows: self.rows || other.rows,
            tools: self.tools || other.tools,
            state: self.state || other.state,
            snapshot: self.snapshot || other.snapshot,
            history: self.history || other.history,
        }
    }

    pub fn is_empty(self) -> bool {
        self == Self::default()
    }

    /// Only live text moved: the cheapest redraw suffices.
    pub fn text_only(self) -> bool {
        self.text && !self.rows && !self.tools && !self.state && !self.snapshot && !self.history
    }
}

/// What changed in the shell since the UI last drained it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ShellChange {
    pub projects: bool,
    pub threads: bool,
}

/// One thread's newest view.
#[derive(Clone, Debug)]
pub struct ThreadUpdate {
    /// `None`: the thread is gone (deleted, or not found) or was closed.
    pub view: Option<Arc<ThreadView>>,
    pub change: ThreadChange,
}

/// The outcome of a dispatch, matched to the action by its ticket.
#[derive(Clone, Debug)]
pub enum StoreNotice {
    Dispatched {
        ticket: DispatchTicket,
        result: DispatchResult,
    },
    DispatchFailed {
        ticket: DispatchTicket,
        command_type: &'static str,
        error: CallError,
    },
    /// `threads.historyPage` failed; the thread keeps what it has.
    HistoryFailed { thread_id: Uuid, error: CallError },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnectionState {
    Connected,
    /// The connection is down; the next attempt is at the given backoff.
    Reconnecting {
        attempt: u32,
        error: String,
    },
}

/// Everything published since the last [`ProjectionStore::take_updates`].
#[derive(Debug, Default)]
pub struct StoreUpdates {
    pub shell: Option<Arc<ShellView>>,
    pub shell_change: ShellChange,
    pub threads: HashMap<Uuid, ThreadUpdate>,
    pub notices: Vec<StoreNotice>,
    pub connection: Option<ConnectionState>,
}

impl StoreUpdates {
    pub fn is_empty(&self) -> bool {
        self.shell.is_none()
            && self.threads.is_empty()
            && self.notices.is_empty()
            && self.connection.is_none()
    }

    fn merge_thread(&mut self, id: Uuid, update: ThreadUpdate) {
        match self.threads.get_mut(&id) {
            Some(existing) => {
                existing.change = existing.change.union(update.change);
                existing.view = update.view;
            }
            None => {
                self.threads.insert(id, update);
            }
        }
    }
}

/// Matches a dispatch to its [`StoreNotice`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DispatchTicket(pub u64);

enum Control {
    OpenThread(ThreadId),
    CloseThread(ThreadId),
    LoadOlder(ThreadId),
    WatchOutput(ThreadId, TurnItemId),
    UnwatchOutput(TurnItemId),
    FetchItem(ThreadId, TurnItemId),
    Dispatch(Queued),
    Shutdown,
}

/// A command waiting to be dispatched, with the commands that must follow
/// it in order once it succeeds.
struct Queued {
    ticket: DispatchTicket,
    command_id: CommandId,
    command: Box<Command>,
    then: Vec<Queued>,
}

#[derive(Default)]
struct Ready {
    shell: bool,
    threads: std::collections::HashSet<Uuid>,
}

struct Shared {
    updates: Mutex<StoreUpdates>,
    client: Mutex<Option<V8Client>>,
    ready: Mutex<Ready>,
    ready_changed: Condvar,
    wake: Box<dyn Fn() + Send + Sync>,
    next_ticket: AtomicU64,
}

/// A handle to the store; cheap to clone. Dropping the last handle does not
/// stop the worker; call [`Self::shutdown`].
#[derive(Clone)]
pub struct ProjectionStore {
    shared: Arc<Shared>,
    control: Sender<Control>,
}

impl ProjectionStore {
    /// Start the worker. It connects through `connector` (and again after a
    /// disconnect, with backoff) and calls `wake` after each publish.
    pub fn start(
        connector: Connector,
        wake: impl Fn() + Send + Sync + 'static,
        config: StoreConfig,
    ) -> Self {
        Self::spawn(None, connector, wake, config)
    }

    /// Start the worker on an already open connection.
    pub fn start_connected(
        client: V8Client,
        connector: Connector,
        wake: impl Fn() + Send + Sync + 'static,
        config: StoreConfig,
    ) -> Self {
        Self::spawn(Some(client), connector, wake, config)
    }

    fn spawn(
        client: Option<V8Client>,
        connector: Connector,
        wake: impl Fn() + Send + Sync + 'static,
        config: StoreConfig,
    ) -> Self {
        let shared = Arc::new(Shared {
            updates: Mutex::new(StoreUpdates::default()),
            client: Mutex::new(None),
            ready: Mutex::new(Ready::default()),
            ready_changed: Condvar::new(),
            wake: Box::new(wake),
            next_ticket: AtomicU64::new(1),
        });
        let (control, control_rx) = unbounded();
        let worker_shared = shared.clone();
        std::thread::Builder::new()
            .name("waku-projection-store".into())
            .spawn(move || {
                let mut worker = Worker::new(worker_shared, control_rx, connector, config);
                if let Some(client) = client {
                    worker.attach(client);
                }
                worker.run();
            })
            .expect("the projection store thread starts");
        Self { shared, control }
    }

    /// Everything published since the last call. Cheap: one short lock.
    pub fn take_updates(&self) -> StoreUpdates {
        std::mem::take(&mut *self.shared.updates.lock())
    }

    /// Subscribe to `thread_id` (or mark it most recently used).
    pub fn open_thread(&self, thread_id: Uuid) {
        let _ = self.control.send(Control::OpenThread(ThreadId(thread_id)));
    }

    pub fn close_thread(&self, thread_id: Uuid) {
        let _ = self.control.send(Control::CloseThread(ThreadId(thread_id)));
    }

    /// Page older history into an open thread (no-op when there is none
    /// or a page is already loading).
    pub fn load_older(&self, thread_id: Uuid) {
        let _ = self.control.send(Control::LoadOlder(ThreadId(thread_id)));
    }

    /// Stream a command row's output while it is expanded.
    pub fn watch_output(&self, thread_id: Uuid, item_id: Uuid) {
        let _ = self.control.send(Control::WatchOutput(
            ThreadId(thread_id),
            TurnItemId(item_id),
        ));
    }

    pub fn unwatch_output(&self, item_id: Uuid) {
        let _ = self
            .control
            .send(Control::UnwatchOutput(TurnItemId(item_id)));
    }

    /// Fetch the unprojected body of a turn item the wire projection
    /// shortened (`threads.turnItem`).
    pub fn fetch_item(&self, thread_id: Uuid, item_id: Uuid) {
        let _ = self
            .control
            .send(Control::FetchItem(ThreadId(thread_id), TurnItemId(item_id)));
    }

    /// Dispatch `command`; its outcome arrives as a [`StoreNotice`] with
    /// the returned ticket. Retried with the same command id after a
    /// reconnect, so it applies at most once.
    pub fn dispatch(&self, command: Command) -> DispatchTicket {
        self.dispatch_all(vec![command])[0]
    }

    /// Dispatch `commands` one after another: each waits for the previous
    /// one's reply, and a failure fails the rest (`thread.create` then the
    /// first `message.dispatch`). One ticket per command, in order.
    pub fn dispatch_all(&self, commands: Vec<Command>) -> Vec<DispatchTicket> {
        let tickets = commands
            .iter()
            .map(|_| DispatchTicket(self.shared.next_ticket.fetch_add(1, Ordering::Relaxed)))
            .collect::<Vec<_>>();
        let mut chain = None::<Queued>;
        for (command, ticket) in commands.into_iter().zip(&tickets).rev() {
            chain = Some(Queued {
                ticket: *ticket,
                command_id: CommandId::new(format!("desktop:{}", Uuid::new_v4().simple()))
                    .expect("a generated command id is valid"),
                command: Box::new(command),
                then: chain.into_iter().collect(),
            });
        }
        if let Some(chain) = chain {
            let _ = self.control.send(Control::Dispatch(chain));
        }
        tickets
    }

    /// The current connection, for one-off reads (`threads.search`) made
    /// off the UI thread. `None` while reconnecting.
    pub fn client(&self) -> Option<V8Client> {
        self.shared.client.lock().clone()
    }

    /// Block until the shell's first snapshot is live, and each of
    /// `threads` has its first view (or `timeout`). For start-up, never the
    /// UI thread.
    pub fn wait_ready(&self, threads: &[Uuid], timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut ready = self.shared.ready.lock();
        loop {
            if ready.shell && threads.iter().all(|id| ready.threads.contains(id)) {
                return true;
            }
            if self
                .shared
                .ready_changed
                .wait_until(&mut ready, deadline)
                .timed_out()
            {
                return ready.shell && threads.iter().all(|id| ready.threads.contains(id));
            }
        }
    }

    /// Stop the worker and close its connection.
    pub fn shutdown(&self) {
        let _ = self.control.send(Control::Shutdown);
    }
}

struct OpenThread {
    state: ThreadState,
    subscription: Option<Subscription>,
    cache: ViewCache,
    full_items: HashMap<TurnItemId, TurnItem>,
    full_order: VecDeque<TurnItemId>,
    change: ThreadChange,
    dirty: bool,
    history_loading: bool,
    /// The thread was not found; resubscribe once a dispatch naming it
    /// (its creation) settles.
    awaiting_dispatch: bool,
    published: bool,
}

impl OpenThread {
    fn new() -> Self {
        Self {
            state: ThreadState::new(),
            subscription: None,
            cache: ViewCache::default(),
            full_items: HashMap::new(),
            full_order: VecDeque::new(),
            change: ThreadChange::default(),
            dirty: false,
            history_loading: false,
            awaiting_dispatch: false,
            published: false,
        }
    }
}

struct OutputWatch {
    thread_id: ThreadId,
    subscription: Option<Subscription>,
    state: CommandOutputState,
}

enum PendingOp {
    Dispatch {
        queued: Queued,
        call: Option<PendingCall<method::OrchestrationDispatch>>,
    },
    History {
        thread_id: ThreadId,
        cursor: String,
        call: Option<PendingCall<method::ThreadsHistoryPage>>,
    },
    Item {
        thread_id: ThreadId,
        item_id: TurnItemId,
        call: Option<PendingCall<method::ThreadsTurnItem>>,
    },
}

impl PendingOp {
    fn receiver(&self) -> Option<&Receiver<Result<crate::v8::MethodResult, CallError>>> {
        match self {
            Self::Dispatch { call, .. } => call.as_ref().map(PendingCall::receiver),
            Self::History { call, .. } => call.as_ref().map(PendingCall::receiver),
            Self::Item { call, .. } => call.as_ref().map(PendingCall::receiver),
        }
    }
}

/// A ready receiver's next message: `Ok` or, once it is closed, `Err`.
fn take<T>(receiver: &Receiver<T>) -> Option<Result<T, RecvError>> {
    match receiver.try_recv() {
        Ok(value) => Some(Ok(value)),
        Err(crossbeam_channel::TryRecvError::Empty) => None,
        Err(crossbeam_channel::TryRecvError::Disconnected) => Some(Err(RecvError)),
    }
}

/// Where a ready `Select` operation came from.
#[derive(Clone, Copy)]
enum Source {
    Control,
    Shell,
    Thread(ThreadId),
    Output(TurnItemId),
    Pending(usize),
}

struct Worker {
    shared: Arc<Shared>,
    control: Receiver<Control>,
    connector: Connector,
    config: StoreConfig,
    client: Option<V8Client>,
    reconnect_at: Option<Instant>,
    reconnect_attempt: u32,
    shell: ShellState,
    shell_subscription: Option<Subscription>,
    shell_cache: ShellCache,
    shell_change: ShellChange,
    shell_dirty: bool,
    threads: HashMap<ThreadId, OpenThread>,
    /// Most recently opened last.
    lru: VecDeque<ThreadId>,
    outputs: HashMap<TurnItemId, OutputWatch>,
    output_order: VecDeque<TurnItemId>,
    pending: Vec<PendingOp>,
    /// Dispatches waiting for an earlier one on the same thread or project:
    /// a thread's commands reach the daemon in the order they were made.
    dispatch_waiting: HashMap<CommandTarget, VecDeque<Queued>>,
    last_publish: HashMap<ThreadId, Instant>,
    publish_at: Option<Instant>,
    stopped: bool,
}

impl Worker {
    fn new(
        shared: Arc<Shared>,
        control: Receiver<Control>,
        connector: Connector,
        config: StoreConfig,
    ) -> Self {
        Self {
            shared,
            control,
            connector,
            config,
            client: None,
            reconnect_at: Some(Instant::now()),
            reconnect_attempt: 0,
            shell: ShellState::new(ShellLocation::Active),
            shell_subscription: None,
            shell_cache: ShellCache::default(),
            shell_change: ShellChange::default(),
            shell_dirty: false,
            threads: HashMap::new(),
            lru: VecDeque::new(),
            outputs: HashMap::new(),
            output_order: VecDeque::new(),
            pending: Vec::new(),
            dispatch_waiting: HashMap::new(),
            last_publish: HashMap::new(),
            publish_at: None,
            stopped: false,
        }
    }

    fn run(&mut self) {
        while !self.stopped {
            self.step();
            self.publish_due();
        }
        if let Some(client) = self.client.take() {
            client.close();
        }
    }

    /// Wait for the next event (or deadline) and handle it.
    fn step(&mut self) {
        let deadline = [self.publish_at, self.reconnect_at]
            .into_iter()
            .flatten()
            .min();
        let mut select = Select::new();
        let mut sources = Vec::new();
        select.recv(&self.control);
        sources.push(Source::Control);
        if let Some(subscription) = &self.shell_subscription {
            select.recv(subscription.events());
            sources.push(Source::Shell);
        }
        for (id, thread) in &self.threads {
            if let Some(subscription) = &thread.subscription {
                select.recv(subscription.events());
                sources.push(Source::Thread(*id));
            }
        }
        for (id, watch) in &self.outputs {
            if let Some(subscription) = &watch.subscription {
                select.recv(subscription.events());
                sources.push(Source::Output(*id));
            }
        }
        for (index, op) in self.pending.iter().enumerate() {
            if let Some(receiver) = op.receiver() {
                select.recv(receiver);
                sources.push(Source::Pending(index));
            }
        }
        let ready = match deadline {
            Some(deadline) => select.ready_deadline(deadline).ok(),
            None => Some(select.ready()),
        };
        drop(select);
        let Some(index) = ready else {
            self.on_deadline();
            return;
        };
        // Readiness can be spurious: an empty receiver just loops.
        match sources[index] {
            Source::Control => match self.control.try_recv() {
                Ok(message) => self.on_control(message),
                Err(crossbeam_channel::TryRecvError::Empty) => {}
                Err(crossbeam_channel::TryRecvError::Disconnected) => self.stopped = true,
            },
            Source::Shell => {
                if let Some(event) = self
                    .shell_subscription
                    .as_ref()
                    .and_then(|subscription| take(subscription.events()))
                {
                    self.on_shell(event);
                }
            }
            Source::Thread(id) => {
                if let Some(event) = self
                    .threads
                    .get(&id)
                    .and_then(|thread| thread.subscription.as_ref())
                    .and_then(|subscription| take(subscription.events()))
                {
                    self.on_thread(id, event);
                }
            }
            Source::Output(id) => {
                if let Some(event) = self
                    .outputs
                    .get(&id)
                    .and_then(|watch| watch.subscription.as_ref())
                    .and_then(|subscription| take(subscription.events()))
                {
                    self.on_output(id, event);
                }
            }
            Source::Pending(index) => {
                let raw = self.pending[index].receiver().and_then(take);
                if let Some(raw) = raw {
                    let op = self.pending.swap_remove(index);
                    self.on_reply(op, raw);
                }
            }
        }
    }

    fn on_deadline(&mut self) {
        if self.reconnect_at.is_some_and(|at| at <= Instant::now()) {
            self.reconnect();
        }
    }

    // -- connection ------------------------------------------------------

    fn reconnect(&mut self) {
        self.reconnect_at = None;
        match (self.connector)() {
            Ok(client) => self.attach(client),
            Err(error) => {
                self.reconnect_attempt += 1;
                self.schedule_reconnect(error.to_string());
            }
        }
    }

    fn schedule_reconnect(&mut self, error: String) {
        // 250 ms doubling to a 10 s cap, ±20% (RFC 0003 §2.6); the first
        // retry is immediate.
        let attempt = self.reconnect_attempt;
        let base = if attempt <= 1 {
            Duration::ZERO
        } else {
            Duration::from_millis(250 << (attempt - 2).min(6)).min(Duration::from_secs(10))
        };
        let jitter = (Uuid::new_v4().as_u128() % 41) as f64 / 100.0 - 0.2;
        let delay = base.mul_f64(1.0 + jitter);
        self.reconnect_at = Some(Instant::now() + delay);
        self.publish_connection(ConnectionState::Reconnecting { attempt, error });
    }

    /// Adopt a fresh connection: resubscribe every stream from its cursor
    /// and re-issue every call in flight.
    fn attach(&mut self, client: V8Client) {
        self.reconnect_at = None;
        self.reconnect_attempt = 0;
        self.shell_subscription = Some(client.subscribe::<stream::Shell>(
            ShellSpec {
                location: ShellLocation::Active,
                project_id: None,
            },
            SubscribeOptions {
                after: self.shell.cursor.clone(),
                window: None,
            },
        ));
        for (id, thread) in &mut self.threads {
            thread.subscription = Some(subscribe_thread(&client, *id, &thread.state));
        }
        for (id, watch) in &mut self.outputs {
            if watch.state.running {
                watch.subscription = Some(client.subscribe::<stream::CommandOutput>(
                    CommandOutputSpec {
                        thread_id: watch.thread_id,
                        item_id: *id,
                    },
                    SubscribeOptions::default(),
                ));
            }
        }
        for op in &mut self.pending {
            restart_call(&client, op);
        }
        *self.shared.client.lock() = Some(client.clone());
        self.client = Some(client);
        self.publish_connection(ConnectionState::Connected);
    }

    fn lost_connection(&mut self) {
        if self.reconnect_at.is_some() {
            return;
        }
        let disconnected = self.client.as_ref().is_none_or(V8Client::is_disconnected);
        if !disconnected {
            return;
        }
        self.client = None;
        *self.shared.client.lock() = None;
        self.shell_subscription = None;
        for thread in self.threads.values_mut() {
            thread.subscription = None;
        }
        for watch in self.outputs.values_mut() {
            watch.subscription = None;
        }
        for op in &mut self.pending {
            match op {
                PendingOp::Dispatch { call, .. } => *call = None,
                PendingOp::History { call, .. } => *call = None,
                PendingOp::Item { call, .. } => *call = None,
            }
        }
        self.reconnect_attempt += 1;
        self.schedule_reconnect("the daemon connection closed".into());
    }

    // -- control ---------------------------------------------------------

    fn on_control(&mut self, message: Control) {
        match message {
            Control::OpenThread(id) => self.open_thread(id),
            Control::CloseThread(id) => self.close_thread(id, true),
            Control::LoadOlder(id) => self.load_older(id),
            Control::WatchOutput(thread_id, item_id) => self.watch_output(thread_id, item_id),
            Control::UnwatchOutput(item_id) => {
                if let Some(watch) = self.outputs.remove(&item_id) {
                    if let Some(subscription) = watch.subscription {
                        subscription.unsubscribe();
                    }
                    self.output_order.retain(|id| *id != item_id);
                    self.mark(watch.thread_id, ThreadChange {
                        rows: true,
                        ..ThreadChange::default()
                    });
                }
            }
            Control::FetchItem(thread_id, item_id) => {
                if self.threads.contains_key(&thread_id)
                    && !self.pending.iter().any(|op| {
                        matches!(op, PendingOp::Item { item_id: pending, .. } if *pending == item_id)
                    })
                {
                    let mut op = PendingOp::Item {
                        thread_id,
                        item_id,
                        call: None,
                    };
                    if let Some(client) = &self.client {
                        restart_call(client, &mut op);
                    }
                    self.pending.push(op);
                }
            }
            Control::Dispatch(queued) => self.start_dispatch(queued),
            Control::Shutdown => self.stopped = true,
        }
    }

    fn start_dispatch(&mut self, queued: Queued) {
        let target = queued.command.target();
        let busy = self.pending.iter().any(|op| {
            matches!(op, PendingOp::Dispatch { queued, .. } if queued.command.target() == target)
        });
        if busy {
            self.dispatch_waiting
                .entry(target)
                .or_default()
                .push_back(queued);
            return;
        }
        let mut op = PendingOp::Dispatch { queued, call: None };
        if let Some(client) = &self.client {
            restart_call(client, &mut op);
        }
        self.pending.push(op);
    }

    fn open_thread(&mut self, id: ThreadId) {
        self.lru.retain(|open| *open != id);
        self.lru.push_back(id);
        if !self.threads.contains_key(&id) {
            let mut thread = OpenThread::new();
            if let Some(client) = &self.client {
                thread.subscription = Some(subscribe_thread(client, id, &thread.state));
            }
            self.threads.insert(id, thread);
        } else if let Some(thread) = self.threads.get_mut(&id)
            && thread.published
        {
            // Already open: republish so a UI that dropped its copy gets one.
            thread.dirty = true;
            self.publish_at = Some(Instant::now());
        }
        while self.lru.len() > self.config.max_open_threads.max(1) {
            let Some(evicted) = self.lru.pop_front() else {
                break;
            };
            self.close_thread(evicted, true);
        }
    }

    fn close_thread(&mut self, id: ThreadId, publish: bool) {
        self.lru.retain(|open| *open != id);
        let Some(thread) = self.threads.remove(&id) else {
            return;
        };
        if let Some(subscription) = thread.subscription {
            subscription.unsubscribe();
        }
        let watched = self
            .outputs
            .iter()
            .filter(|(_, watch)| watch.thread_id == id)
            .map(|(item, _)| *item)
            .collect::<Vec<_>>();
        for item in watched {
            if let Some(watch) = self.outputs.remove(&item)
                && let Some(subscription) = watch.subscription
            {
                subscription.unsubscribe();
            }
            self.output_order.retain(|id| *id != item);
        }
        self.pending
            .retain(|op| !matches!(op, PendingOp::History { thread_id, .. } | PendingOp::Item { thread_id, .. } if *thread_id == id));
        self.last_publish.remove(&id);
        self.shared.ready.lock().threads.remove(&id.0);
        if publish {
            self.shared.updates.lock().merge_thread(
                id.0,
                ThreadUpdate {
                    view: None,
                    change: ThreadChange {
                        state: true,
                        ..ThreadChange::default()
                    },
                },
            );
            (self.shared.wake)();
        }
    }

    fn load_older(&mut self, id: ThreadId) {
        let Some(thread) = self.threads.get_mut(&id) else {
            return;
        };
        if thread.history_loading || !thread.state.has_more_history {
            return;
        }
        let Some(cursor) = thread.state.history.clone() else {
            return;
        };
        thread.history_loading = true;
        thread.dirty = true;
        let mut op = PendingOp::History {
            thread_id: id,
            cursor,
            call: None,
        };
        if let Some(client) = &self.client {
            restart_call(client, &mut op);
        }
        self.pending.push(op);
        self.schedule_publish(id);
    }

    fn watch_output(&mut self, thread_id: ThreadId, item_id: TurnItemId) {
        if !self.threads.contains_key(&thread_id) {
            return;
        }
        if self.outputs.contains_key(&item_id) {
            return;
        }
        while self.outputs.len() >= self.config.max_output_watches.max(1) {
            let Some(oldest) = self.output_order.pop_front() else {
                break;
            };
            if let Some(watch) = self.outputs.remove(&oldest)
                && let Some(subscription) = watch.subscription
            {
                subscription.unsubscribe();
            }
        }
        let subscription = self.client.as_ref().map(|client| {
            client.subscribe::<stream::CommandOutput>(
                CommandOutputSpec { thread_id, item_id },
                SubscribeOptions::default(),
            )
        });
        self.outputs.insert(
            item_id,
            OutputWatch {
                thread_id,
                subscription,
                state: CommandOutputState::default(),
            },
        );
        self.output_order.push_back(item_id);
    }

    // -- streams ---------------------------------------------------------

    fn on_shell(&mut self, event: Result<StreamEvent, RecvError>) {
        match event {
            Ok(StreamEvent::Item(StreamItem::Shell(item))) => {
                let applied = self.shell.apply_item(item);
                if applied.changed() {
                    self.shell_change.projects |= applied.projects;
                    self.shell_change.threads |= applied.threads;
                    self.shell_dirty = true;
                    self.publish_at.get_or_insert_with(Instant::now);
                }
                if applied.synchronized && self.shell.complete {
                    // Publish at once: the first live shell unblocks start-up.
                    self.shell_dirty = true;
                    self.publish_at = Some(Instant::now());
                }
                // Drain whatever else is queued before publishing.
                self.drain_shell();
            }
            Ok(StreamEvent::End(end)) => {
                self.shell_subscription = None;
                if !matches!(end.reason, EndReason::Unsubscribed) {
                    self.resubscribe_shell_later();
                }
            }
            Ok(_) => {}
            Err(RecvError) => {
                self.shell_subscription = None;
                self.lost_connection();
            }
        }
    }

    fn drain_shell(&mut self) {
        while let Some(event) = self
            .shell_subscription
            .as_ref()
            .and_then(|subscription| subscription.events().try_recv().ok())
        {
            match event {
                StreamEvent::Item(StreamItem::Shell(item)) => {
                    let applied = self.shell.apply_item(item);
                    self.shell_change.projects |= applied.projects;
                    self.shell_change.threads |= applied.threads;
                    self.shell_dirty |= applied.changed() || applied.synchronized;
                }
                other => {
                    self.on_shell(Ok(other));
                    return;
                }
            }
        }
    }

    fn resubscribe_shell_later(&mut self) {
        if let Some(client) = &self.client
            && !client.is_disconnected()
        {
            // The daemon ended the stream without closing the connection
            // (shutting down, an error): subscribe again from the cursor.
            self.shell_subscription = Some(client.subscribe::<stream::Shell>(
                ShellSpec::default(),
                SubscribeOptions {
                    after: self.shell.cursor.clone(),
                    window: None,
                },
            ));
        } else {
            self.lost_connection();
        }
    }

    fn on_thread(&mut self, id: ThreadId, event: Result<StreamEvent, RecvError>) {
        let Some(thread) = self.threads.get_mut(&id) else {
            return;
        };
        let mut events = vec![event];
        // Fold everything already queued into one publish.
        if let Some(subscription) = &thread.subscription {
            events.extend(subscription.events().try_iter().map(Ok));
        }
        for event in events {
            let Some(thread) = self.threads.get_mut(&id) else {
                return;
            };
            match event {
                Ok(StreamEvent::Item(StreamItem::Thread(item))) => {
                    let change = classify(&item);
                    let applied = thread.state.apply_item(item);
                    if applied.changed() {
                        thread.change = thread.change.union(change);
                        thread.dirty = true;
                    }
                    if applied.synchronized {
                        thread.dirty = true;
                        // The first view of a thread goes out at once.
                        if !thread.published {
                            self.publish_at = Some(Instant::now());
                        }
                    }
                }
                Ok(StreamEvent::End(end)) => {
                    thread.subscription = None;
                    match end.reason {
                        EndReason::NotFound | EndReason::Forbidden => {
                            let creating = self.pending.iter().any(|op| {
                                matches!(op, PendingOp::Dispatch { queued, .. }
                                    if queued.creates(id))
                            });
                            if creating {
                                thread.awaiting_dispatch = true;
                            } else {
                                self.close_thread(id, true);
                                return;
                            }
                        }
                        EndReason::Unsubscribed => {}
                        _ => {
                            if let Some(client) = &self.client
                                && !client.is_disconnected()
                            {
                                thread.subscription =
                                    Some(subscribe_thread(client, id, &thread.state));
                            } else {
                                self.lost_connection();
                                return;
                            }
                        }
                    }
                }
                Ok(_) => {}
                Err(RecvError) => {
                    thread.subscription = None;
                    self.lost_connection();
                    return;
                }
            }
        }
        self.schedule_publish(id);
    }

    fn on_output(&mut self, item_id: TurnItemId, event: Result<StreamEvent, RecvError>) {
        let Some(watch) = self.outputs.get_mut(&item_id) else {
            return;
        };
        let mut changed = false;
        let mut events = vec![event];
        if let Some(subscription) = &watch.subscription {
            events.extend(subscription.events().try_iter().map(Ok));
        }
        for event in events {
            match event {
                Ok(StreamEvent::Item(StreamItem::CommandOutput(item))) => {
                    changed |= watch.state.apply_item(item);
                }
                Ok(StreamEvent::End(end)) => {
                    watch.subscription = None;
                    if !matches!(end.reason, EndReason::Completed | EndReason::Unsubscribed) {
                        watch.state.running = false;
                    }
                    changed = true;
                }
                Ok(_) => {}
                Err(RecvError) => {
                    watch.subscription = None;
                    break;
                }
            }
        }
        let thread_id = watch.thread_id;
        if changed {
            self.mark(
                thread_id,
                ThreadChange {
                    rows: true,
                    tools: true,
                    ..ThreadChange::default()
                },
            );
        }
        if self.client.as_ref().is_some_and(V8Client::is_disconnected) {
            self.lost_connection();
        }
    }

    fn on_reply(
        &mut self,
        op: PendingOp,
        raw: Result<Result<crate::v8::MethodResult, CallError>, RecvError>,
    ) {
        match op {
            PendingOp::Dispatch { queued, call } => {
                let result = PendingCall::<method::OrchestrationDispatch>::decode(raw);
                drop(call);
                if matches!(result, Err(CallError::Disconnected)) {
                    // Retried with the same command id after the reconnect.
                    self.pending
                        .push(PendingOp::Dispatch { queued, call: None });
                    self.lost_connection();
                    return;
                }
                let Queued {
                    ticket,
                    command,
                    then,
                    ..
                } = queued;
                let failed = result.is_err();
                let mut notices = Vec::new();
                match result {
                    Ok(result) => {
                        notices.push(StoreNotice::Dispatched { ticket, result });
                        for next in then {
                            self.start_dispatch(next);
                        }
                    }
                    Err(error) => {
                        notices.push(StoreNotice::DispatchFailed {
                            ticket,
                            command_type: command.command_type(),
                            error: error.clone(),
                        });
                        let mut rest = then;
                        while let Some(next) = rest.pop() {
                            notices.push(StoreNotice::DispatchFailed {
                                ticket: next.ticket,
                                command_type: next.command.command_type(),
                                error: error.clone(),
                            });
                            rest.extend(next.then);
                        }
                    }
                }
                // A thread that was not found while this command created it
                // can be subscribed now.
                let waiting = self
                    .threads
                    .iter()
                    .filter(|(id, thread)| {
                        thread.awaiting_dispatch && command_creates(&command, **id)
                    })
                    .map(|(id, _)| *id)
                    .collect::<Vec<_>>();
                for id in waiting {
                    if failed {
                        self.close_thread(id, true);
                    } else if let Some(client) = &self.client
                        && let Some(thread) = self.threads.get_mut(&id)
                    {
                        thread.awaiting_dispatch = false;
                        thread.subscription = Some(subscribe_thread(client, id, &thread.state));
                    }
                }
                self.shared.updates.lock().notices.extend(notices);
                (self.shared.wake)();
                let target = command.target();
                if let Some(next) = self
                    .dispatch_waiting
                    .get_mut(&target)
                    .and_then(VecDeque::pop_front)
                {
                    self.start_dispatch(next);
                }
                if self
                    .dispatch_waiting
                    .get(&target)
                    .is_some_and(VecDeque::is_empty)
                {
                    self.dispatch_waiting.remove(&target);
                }
            }
            PendingOp::History {
                thread_id,
                cursor,
                call: _,
            } => {
                let result = PendingCall::<method::ThreadsHistoryPage>::decode(raw);
                if matches!(result, Err(CallError::Disconnected)) {
                    self.pending.push(PendingOp::History {
                        thread_id,
                        cursor,
                        call: None,
                    });
                    self.lost_connection();
                    return;
                }
                let Some(thread) = self.threads.get_mut(&thread_id) else {
                    return;
                };
                thread.history_loading = false;
                thread.dirty = true;
                match result {
                    Ok(page) => {
                        // A snapshot that arrived meanwhile restarted the
                        // window; a page for its old cursor still merges
                        // harmlessly (upserts that never replace newer rows).
                        merge_page(&mut thread.state, page);
                        thread.change.history = true;
                        thread.change.rows = true;
                    }
                    Err(error) => {
                        self.shared
                            .updates
                            .lock()
                            .notices
                            .push(StoreNotice::HistoryFailed {
                                thread_id: thread_id.0,
                                error,
                            });
                    }
                }
                self.schedule_publish(thread_id);
            }
            PendingOp::Item {
                thread_id,
                item_id,
                call: _,
            } => {
                let result = PendingCall::<method::ThreadsTurnItem>::decode(raw);
                if matches!(result, Err(CallError::Disconnected)) {
                    self.pending.push(PendingOp::Item {
                        thread_id,
                        item_id,
                        call: None,
                    });
                    self.lost_connection();
                    return;
                }
                let max = self.config.max_full_items.max(1);
                let Some(thread) = self.threads.get_mut(&thread_id) else {
                    return;
                };
                if let Ok(item) = result {
                    thread.full_order.retain(|id| *id != item_id);
                    thread.full_order.push_back(item_id);
                    thread.full_items.insert(item_id, item);
                    while thread.full_order.len() > max {
                        if let Some(oldest) = thread.full_order.pop_front() {
                            thread.full_items.remove(&oldest);
                        }
                    }
                    thread.change.rows = true;
                    thread.change.tools = true;
                    thread.dirty = true;
                    self.schedule_publish(thread_id);
                }
            }
        }
    }

    // -- publishing ------------------------------------------------------

    fn mark(&mut self, id: ThreadId, change: ThreadChange) {
        if let Some(thread) = self.threads.get_mut(&id) {
            thread.change = thread.change.union(change);
            thread.dirty = true;
            self.schedule_publish(id);
        }
    }

    fn schedule_publish(&mut self, id: ThreadId) {
        let interval = self.config.publish_interval;
        let at = self
            .last_publish
            .get(&id)
            .map_or_else(Instant::now, |last| *last + interval);
        self.publish_at = Some(self.publish_at.map_or(at, |current| current.min(at)));
    }

    fn publish_due(&mut self) {
        let Some(at) = self.publish_at else {
            return;
        };
        let now = Instant::now();
        if at > now {
            return;
        }
        self.publish_at = None;
        let interval = self.config.publish_interval;
        let mut updates = Vec::new();
        let mut ready_threads = Vec::new();
        let mut next = None::<Instant>;
        for (id, thread) in &mut self.threads {
            if !thread.dirty {
                continue;
            }
            if let Some(last) = self.last_publish.get(id)
                && *last + interval > now
                && thread.published
            {
                let due = *last + interval;
                next = Some(next.map_or(due, |next| next.min(due)));
                continue;
            }
            // Nothing to show before the first snapshot.
            if thread.state.thread.is_none() {
                continue;
            }
            let outputs = self
                .outputs
                .iter()
                .filter(|(_, watch)| watch.thread_id == *id)
                .map(|(item, watch)| (*item, watch.state.clone()))
                .collect::<HashMap<_, _>>();
            let extras = ViewExtras {
                outputs: Some(&outputs),
                full_items: Some(&thread.full_items),
                history_loading: thread.history_loading,
            };
            let Some(view) = thread.cache.build(&thread.state, &extras) else {
                continue;
            };
            let mut change = std::mem::take(&mut thread.change);
            if !thread.published {
                change.snapshot = true;
                change.state = true;
            }
            if thread.state.synchronized {
                ready_threads.push(id.0);
            }
            thread.dirty = false;
            thread.published = true;
            self.last_publish.insert(*id, now);
            updates.push((
                id.0,
                ThreadUpdate {
                    view: Some(Arc::new(view)),
                    change,
                },
            ));
        }
        let shell = self.shell_dirty.then(|| {
            self.shell_dirty = false;
            Arc::new(self.shell_cache.build(&self.shell))
        });
        self.publish_at = next;
        if updates.is_empty() && shell.is_none() {
            return;
        }
        {
            let mut pending = self.shared.updates.lock();
            for (id, update) in updates {
                pending.merge_thread(id, update);
            }
            if let Some(shell) = &shell {
                pending.shell = Some(shell.clone());
                pending.shell_change.projects |= self.shell_change.projects;
                pending.shell_change.threads |= self.shell_change.threads;
                self.shell_change = ShellChange::default();
            }
        }
        {
            let mut ready = self.shared.ready.lock();
            if shell.is_some_and(|shell| shell.synchronized) {
                ready.shell = true;
            }
            ready.threads.extend(ready_threads);
        }
        self.shared.ready_changed.notify_all();
        (self.shared.wake)();
    }

    fn publish_connection(&mut self, state: ConnectionState) {
        self.shared.updates.lock().connection = Some(state);
        (self.shared.wake)();
    }
}

fn subscribe_thread(client: &V8Client, id: ThreadId, state: &ThreadState) -> Subscription {
    client.subscribe::<stream::Thread>(
        ThreadSpec {
            thread_id: id,
            compact_turn_items: true,
        },
        SubscribeOptions {
            after: state.cursor.clone(),
            window: None,
        },
    )
}

/// (Re)send the call behind `op` on `client`.
fn restart_call(client: &V8Client, op: &mut PendingOp) {
    match op {
        PendingOp::Dispatch { queued, call } => {
            *call = client
                .start::<method::OrchestrationDispatch>(DispatchParams {
                    command_id: queued.command_id.clone(),
                    command: DispatchCommand((*queued.command).clone()),
                })
                .ok();
        }
        PendingOp::History {
            thread_id,
            cursor,
            call,
        } => {
            *call = client
                .start::<method::ThreadsHistoryPage>(HistoryPageParams {
                    thread_id: *thread_id,
                    cursor: cursor.clone(),
                })
                .ok();
        }
        PendingOp::Item {
            thread_id,
            item_id,
            call,
        } => {
            *call = client
                .start::<method::ThreadsTurnItem>(TurnItemParams {
                    thread_id: *thread_id,
                    item_id: *item_id,
                })
                .ok();
        }
    }
}

fn merge_page(state: &mut ThreadState, page: HistoryPage) {
    state.merge_history_page(page);
}

impl Queued {
    /// Whether this command, or one chained after it, creates `thread_id`.
    fn creates(&self, thread_id: ThreadId) -> bool {
        command_creates(&self.command, thread_id)
            || self.then.iter().any(|next| next.creates(thread_id))
    }
}

/// Whether `command` brings `thread_id` into existence.
fn command_creates(command: &Command, thread_id: ThreadId) -> bool {
    match command {
        Command::ThreadCreate { thread_id: id, .. } => *id == thread_id,
        Command::ThreadFork {
            target_thread_id, ..
        } => *target_thread_id == thread_id,
        _ => false,
    }
}

/// How the UI should redraw for one `thread` item.
pub fn classify(item: &ThreadItem) -> ThreadChange {
    match item {
        ThreadItem::Snapshot(_) => ThreadChange {
            snapshot: true,
            rows: true,
            state: true,
            ..ThreadChange::default()
        },
        ThreadItem::Delta { .. } => ThreadChange {
            text: true,
            ..ThreadChange::default()
        },
        ThreadItem::Event { event, .. } => match event {
            DomainEvent::TurnItemUpdated(item) => match &item.kind {
                // A streaming text item only grows; settling it reshapes
                // the turn (folds, footers), which is a state change.
                TurnItemKind::AssistantMessage { streaming, .. }
                | TurnItemKind::Reasoning { streaming, .. }
                | TurnItemKind::ProposedPlan { streaming, .. }
                    if *streaming =>
                {
                    ThreadChange {
                        text: true,
                        ..ThreadChange::default()
                    }
                }
                TurnItemKind::CommandExecution { .. }
                | TurnItemKind::FileChange { .. }
                | TurnItemKind::FileSearch { .. }
                | TurnItemKind::WebSearch { .. }
                | TurnItemKind::DynamicTool { .. }
                | TurnItemKind::Subagent { .. } => ThreadChange {
                    rows: true,
                    tools: true,
                    ..ThreadChange::default()
                },
                TurnItemKind::Reasoning { .. }
                | TurnItemKind::TodoList { .. }
                | TurnItemKind::ProposedPlan { .. } => ThreadChange {
                    rows: true,
                    ..ThreadChange::default()
                },
                _ => ThreadChange {
                    rows: true,
                    state: true,
                    ..ThreadChange::default()
                },
            },
            DomainEvent::MessageUpdated(_)
            | DomainEvent::PlanUpdated(_)
            | DomainEvent::NodeUpdated(_)
            | DomainEvent::ProviderTurnUpdated(_)
            | DomainEvent::SubagentUpdated(_) => ThreadChange {
                rows: true,
                ..ThreadChange::default()
            },
            _ => ThreadChange {
                state: true,
                rows: true,
                ..ThreadChange::default()
            },
        },
        ThreadItem::Synchronized { .. } | ThreadItem::Unknown => ThreadChange::default(),
    }
}

/// A failed dispatch's message for the user.
pub fn describe_error(error: &CallError) -> String {
    match error {
        CallError::Remote(Error { message, .. }) => message.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use waku_protocol::orchestration::{ItemStatus, MessageId, ThreadId as Tid};

    #[test]
    fn deltas_and_streaming_snapshots_are_text_only() {
        let delta = ThreadItem::Delta {
            item_id: TurnItemId(Uuid::nil()),
            channel: waku_protocol::v8::orchestration::DeltaChannel::Text,
            rev: 1,
            text: "a".into(),
        };
        assert!(classify(&delta).text_only());
        let item = |streaming| TurnItem {
            id: TurnItemId(Uuid::nil()),
            thread_id: Tid(Uuid::nil()),
            run_id: None,
            node_id: None,
            provider_thread_id: None,
            provider_turn_id: None,
            native_item_ref: None,
            parent_item_id: None,
            position: 1,
            status: ItemStatus::Running,
            title: None,
            tool: None,
            stream_rev: 0,
            started_at: None,
            completed_at: None,
            updated_at: 0,
            kind: TurnItemKind::AssistantMessage {
                message_id: MessageId(Uuid::nil()),
                text: "a".into(),
                streaming,
            },
        };
        let event = |streaming| ThreadItem::Event {
            seq: 1,
            event: DomainEvent::TurnItemUpdated(item(streaming)),
        };
        assert!(classify(&event(true)).text_only());
        let settled = classify(&event(false));
        assert!(settled.state && settled.rows && !settled.text_only());
    }
}
