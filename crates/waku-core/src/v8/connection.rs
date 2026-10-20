//! One v8 connection (RFC 0003 §2): handshake, the reader loop, and the
//! writer with its priority queues, per-subscription budgets and DATA credit.
//!
//! The reader runs in the connection's task and never waits on the writer.
//! The writer is its own task draining three queues in order — control
//! (`pong`, `ping`, `goodbye`), messages (replies, `end`, stream items), then
//! at most one DATA chunk before it checks the others again. Nothing polls:
//! the writer sleeps on a `Notify` with the keepalive interval as its only
//! timeout, and the reader's only timer is the dead-peer timeout.
//!
//! Stream items are encoded once and queued per subscription. A subscription
//! whose queue passes its live budget is ended with `end{overflow}` and the
//! last cursor actually written, so the client resubscribes from there; the
//! other streams on the connection are unaffected (kick-and-resume).

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde::Serialize;
use subtle::ConstantTimeEq as _;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Notify;
use tokio::task::AbortHandle;
use waku_protocol::v8::codec::{self, encode_items_envelope};
use waku_protocol::v8::frame::FrameLimits;
use waku_protocol::v8::{
    ClientKind, ClientMessage, Credential, DEFAULT_DATA_WINDOW, Encoding, End, EndReason, Error,
    ErrorCode, ErrorDetails, GoodbyeCode, Hello, LIVE_BUDGET_BYTES, LIVE_BUDGET_ITEMS, Limits,
    Method, MethodResult, ProtocolVersion, RejectCode, Rejected, Reply, Scope, ServerMessage,
    SessionInfo, StreamCursor, StreamSpec, Subscribe, WRITE_STALL_MS, Welcome,
};

use super::orchestration::HubMessage;
use super::transport::{FrameReader, FrameWriter, Incoming, Outgoing, ReadError};
use super::{V8Context, methods, orchestration, streams};

/// Frames written before the writer flushes even with more queued.
const MAX_FRAMES_PER_FLUSH: usize = 64;
/// How long a closing connection waits for its writer to send a `goodbye`.
const CLOSE_GRACE: Duration = Duration::from_secs(1);

/// Queue caps of one connection (RFC 0003 §2.4); smaller in tests.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Budget {
    /// Queued items per subscription.
    pub(crate) items: usize,
    /// Queued encoded item bytes per subscription.
    pub(crate) item_bytes: usize,
    /// Everything queued for the writer.
    pub(crate) connection_bytes: usize,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            items: LIVE_BUDGET_ITEMS as usize,
            item_bytes: LIVE_BUDGET_BYTES as usize,
            connection_bytes: waku_protocol::v8::CONNECTION_QUEUE_BYTES as usize,
        }
    }
}

pub(crate) struct Conn {
    pub(crate) encoding: Encoding,
    pub(crate) limits: Limits,
    pub(crate) scopes: Vec<Scope>,
    /// From `hello`; the creation source of dispatched commands.
    pub(crate) client_kind: ClientKind,
    budget: Budget,
    state: Mutex<State>,
    wake: Notify,
}

#[derive(Default)]
struct State {
    control: VecDeque<Vec<u8>>,
    messages: VecDeque<Queued>,
    data: VecDeque<DataEntry>,
    /// Bytes of replies, items and DATA waiting for the writer.
    queued_bytes: usize,
    channels: HashMap<u32, Channel>,
    /// In-flight calls; `None` until the call's task is registered.
    calls: HashMap<u32, Option<AbortHandle>>,
    /// A `goodbye` is queued; the writer stops once it is written.
    closing: bool,
    /// The connection is over; the writer stops at once.
    stopped: bool,
    next_nonce: u64,
}

enum Queued {
    Encoded(Vec<u8>),
    /// Items of this subscription are queued; written as one `items`.
    Channel(u32),
}

enum DataEntry {
    Chunk {
        channel: u32,
        fin: bool,
        body: Vec<u8>,
    },
    /// An `end` that must follow the stream's DATA.
    Message(Vec<u8>),
}

struct Channel {
    items: VecDeque<(Arc<[u8]>, Option<StreamCursor>)>,
    item_bytes: usize,
    token_queued: bool,
    /// The cursor of the last item handed to the writer.
    delivered: Option<StreamCursor>,
    credit: u64,
    credit_wake: Arc<Notify>,
    /// DATA chunks of this channel still in the data queue.
    pending_data: usize,
    task: Option<AbortHandle>,
    /// The hub queue of a `thread` or `shell` stream, for read-your-writes
    /// barriers.
    barrier: Option<tokio::sync::mpsc::WeakSender<HubMessage>>,
}

pub(crate) enum Next {
    Write(Outgoing),
    /// The `goodbye`: write it, flush and stop.
    Last(Outgoing),
    Idle,
    Stop,
}

pub(crate) enum CallRejected {
    Busy,
    Duplicate,
}

pub(crate) enum OpenRejected {
    InUse,
    TooMany,
}

impl Conn {
    pub(crate) fn new(
        encoding: Encoding,
        limits: Limits,
        scopes: Vec<Scope>,
        client_kind: ClientKind,
        budget: Budget,
    ) -> Self {
        Self {
            encoding,
            limits,
            scopes,
            client_kind,
            budget,
            state: Mutex::new(State {
                next_nonce: 1,
                ..State::default()
            }),
            wake: Notify::new(),
        }
    }

    pub(crate) fn encode<T: Serialize>(&self, message: &T) -> Vec<u8> {
        codec::to_vec(self.encoding, message).unwrap_or_else(|error| {
            // Our own types always encode; this is a bug, not a peer error.
            panic!("could not encode a v8 message: {error}")
        })
    }

    pub(crate) fn push_control(&self, message: &ServerMessage) {
        let bytes = self.encode(message);
        let mut state = self.state.lock();
        if state.closing || state.stopped {
            return;
        }
        state.control.push_back(bytes);
        drop(state);
        self.wake.notify_one();
    }

    pub(crate) fn ping(&self) {
        let nonce = {
            let mut state = self.state.lock();
            let nonce = state.next_nonce;
            state.next_nonce += 1;
            nonce
        };
        self.push_control(&ServerMessage::Ping { nonce });
    }

    /// Queue a `goodbye` as the last thing the writer sends.
    pub(crate) fn goodbye(&self, code: GoodbyeCode, message: impl Into<String>, reconnect: bool) {
        let bytes = self.encode(&ServerMessage::Goodbye {
            code,
            message: message.into(),
            reconnect,
        });
        let mut state = self.state.lock();
        if state.closing || state.stopped {
            return;
        }
        state.control.push_back(bytes);
        state.closing = true;
        drop(state);
        self.wake.notify_one();
    }

    /// End the connection: abort every call and stream. The writer stops at
    /// once unless a `goodbye` is still to be written.
    pub(crate) fn stop(&self) {
        let mut state = self.state.lock();
        for call in state.calls.drain().filter_map(|(_, task)| task) {
            call.abort();
        }
        for channel in state.channels.values_mut() {
            if let Some(task) = channel.task.take() {
                task.abort();
            }
        }
        if !state.closing {
            state.stopped = true;
        }
        drop(state);
        self.wake.notify_one();
    }

    /// Queue a reply, end or other message. Over the connection budget the
    /// client is not reading, so it is told `overloaded` and dropped.
    pub(crate) fn push_message(&self, message: &ServerMessage) {
        let bytes = self.encode(message);
        let mut state = self.state.lock();
        if state.closing || state.stopped {
            return;
        }
        if state.queued_bytes + bytes.len() > self.budget.connection_bytes {
            drop(state);
            self.goodbye(
                GoodbyeCode::Overloaded,
                "the client is not reading its replies",
                true,
            );
            return;
        }
        state.queued_bytes += bytes.len();
        state.messages.push_back(Queued::Encoded(bytes));
        drop(state);
        self.wake.notify_one();
    }

    pub(crate) fn push_reply(&self, reply: Reply) {
        let id = reply.id;
        let message = ServerMessage::Reply(reply);
        let encoded = self.encode(&message);
        if encoded.len() > self.limits.max_message_bytes as usize {
            self.push_message(&ServerMessage::Reply(Reply::error(
                id,
                Error::new(
                    ErrorCode::PayloadTooLarge,
                    format!(
                        "the result is {} bytes, over the message limit",
                        encoded.len()
                    ),
                )
                .with_details(ErrorDetails::Limit {
                    limit: u64::from(self.limits.max_message_bytes),
                }),
            )));
            return;
        }
        self.push_message(&message);
    }

    pub(crate) fn begin_call(&self, id: u32) -> Result<(), CallRejected> {
        let mut state = self.state.lock();
        if state.calls.contains_key(&id) {
            return Err(CallRejected::Duplicate);
        }
        if state.calls.len() >= self.limits.max_in_flight_calls as usize {
            return Err(CallRejected::Busy);
        }
        state.calls.insert(id, None);
        Ok(())
    }

    pub(crate) fn set_call_task(&self, id: u32, task: AbortHandle) {
        if let Some(slot) = self.state.lock().calls.get_mut(&id) {
            *slot = Some(task);
        }
    }

    /// Whether the call was still in flight (not cancelled): only then is
    /// its reply sent.
    pub(crate) fn finish_call(&self, id: u32) -> bool {
        self.state.lock().calls.remove(&id).is_some()
    }

    pub(crate) fn cancel_call(&self, id: u32) -> bool {
        let Some(task) = self.state.lock().calls.remove(&id) else {
            return false;
        };
        if let Some(task) = task {
            task.abort();
        }
        true
    }

    pub(crate) fn open_channel(&self, channel: u32, window: u32) -> Result<(), OpenRejected> {
        let mut state = self.state.lock();
        if state.channels.contains_key(&channel) {
            return Err(OpenRejected::InUse);
        }
        if state.channels.len() >= self.limits.max_subscriptions as usize {
            return Err(OpenRejected::TooMany);
        }
        state.channels.insert(
            channel,
            Channel {
                items: VecDeque::new(),
                item_bytes: 0,
                token_queued: false,
                delivered: None,
                credit: u64::from(window),
                credit_wake: Arc::new(Notify::new()),
                pending_data: 0,
                task: None,
                barrier: None,
            },
        );
        Ok(())
    }

    /// The hub queues of this connection's `thread` and `shell` streams.
    pub(crate) fn barriers(&self) -> Vec<tokio::sync::mpsc::WeakSender<HubMessage>> {
        self.state
            .lock()
            .channels
            .values()
            .filter_map(|channel| channel.barrier.clone())
            .collect()
    }

    pub(crate) fn set_channel_task(&self, channel: u32, task: AbortHandle) {
        // A stream that already ended has no channel left.
        if let Some(open) = self.state.lock().channels.get_mut(&channel) {
            open.task = Some(task);
        }
    }

    /// `unsubscribe`: drop what is queued and end the stream.
    pub(crate) fn unsubscribe(&self, channel: u32) {
        let mut state = self.state.lock();
        let Some(removed) = remove_channel(&mut state, channel, true) else {
            return;
        };
        if let Some(task) = removed.task {
            task.abort();
        }
        drop(state);
        self.push_message(&ServerMessage::End(End {
            channel,
            reason: EndReason::Unsubscribed,
            resume_after: removed.delivered,
        }));
    }

    pub(crate) fn add_credit(&self, channel: u32, bytes: u32) {
        if let Some(open) = self.state.lock().channels.get_mut(&channel) {
            open.credit = open.credit.saturating_add(u64::from(bytes));
            open.credit_wake.notify_one();
        }
    }

    /// The writer's next frame, by priority.
    pub(crate) fn next(&self) -> Next {
        let mut guard = self.state.lock();
        let state = &mut *guard;
        if state.stopped {
            return Next::Stop;
        }
        if let Some(bytes) = state.control.pop_front() {
            if state.closing && state.control.is_empty() {
                return Next::Last(Outgoing::Message(bytes));
            }
            return Next::Write(Outgoing::Message(bytes));
        }
        if state.closing {
            return Next::Stop;
        }
        while let Some(entry) = state.messages.pop_front() {
            match entry {
                Queued::Encoded(bytes) => {
                    state.queued_bytes -= bytes.len();
                    return Next::Write(Outgoing::Message(bytes));
                }
                Queued::Channel(channel) => {
                    if let Some(envelope) = self.take_items(state, channel) {
                        return Next::Write(Outgoing::Message(envelope));
                    }
                }
            }
        }
        match state.data.pop_front() {
            Some(DataEntry::Chunk { channel, fin, body }) => {
                state.queued_bytes -= body.len();
                if let Some(open) = state.channels.get_mut(&channel) {
                    open.pending_data -= 1;
                }
                Next::Write(Outgoing::Data { channel, fin, body })
            }
            Some(DataEntry::Message(bytes)) => Next::Write(Outgoing::Message(bytes)),
            None => Next::Idle,
        }
    }

    /// One `items` message with as many of `channel`'s queued items as fit.
    fn take_items(&self, state: &mut State, channel: u32) -> Option<Vec<u8>> {
        let open = state.channels.get_mut(&channel)?;
        open.token_queued = false;
        let limit = self.limits.max_message_bytes as usize;
        let mut taken = Vec::new();
        let mut size = 0;
        while let Some((bytes, _)) = open.items.front() {
            // 32 bytes covers the envelope around the items.
            if !taken.is_empty() && size + bytes.len() + 32 > limit {
                break;
            }
            let (bytes, cursor) = open.items.pop_front().expect("front exists");
            size += bytes.len();
            if cursor.is_some() {
                open.delivered = cursor;
            }
            taken.push(bytes);
        }
        open.item_bytes -= size;
        if !open.items.is_empty() {
            open.token_queued = true;
            state.messages.push_back(Queued::Channel(channel));
        }
        state.queued_bytes -= size;
        (!taken.is_empty()).then(|| encode_items_envelope(self.encoding, channel, &taken))
    }
}

/// Remove `channel`; `drop_data` also discards its queued DATA.
fn remove_channel(state: &mut State, channel: u32, drop_data: bool) -> Option<Channel> {
    let removed = state.channels.remove(&channel)?;
    state.queued_bytes -= removed.item_bytes;
    if drop_data && removed.pending_data > 0 {
        let mut freed = 0;
        state.data.retain(|entry| match entry {
            DataEntry::Chunk {
                channel: owner,
                body,
                ..
            } if *owner == channel => {
                freed += body.len();
                false
            }
            _ => true,
        });
        state.queued_bytes -= freed;
    }
    Some(removed)
}

/// The subscription ended or was kicked; its task should return.
pub(crate) struct Ended;

/// A stream task's handle on its subscription.
pub(crate) struct StreamSink {
    conn: Arc<Conn>,
    channel: u32,
}

impl StreamSink {
    pub(crate) fn encoding(&self) -> Encoding {
        self.conn.encoding
    }

    /// Lets `orchestration.dispatch` on this connection wait for this
    /// stream (read-your-writes).
    pub(crate) fn register_barrier(&self, sender: tokio::sync::mpsc::WeakSender<HubMessage>) {
        if let Some(open) = self.conn.state.lock().channels.get_mut(&self.channel) {
            open.barrier = Some(sender);
        }
    }

    pub(crate) fn max_data_chunk(&self) -> usize {
        self.conn.limits.max_data_chunk_bytes as usize
    }

    pub(crate) fn push<T: Serialize>(
        &self,
        item: &T,
        cursor: Option<StreamCursor>,
    ) -> Result<(), Ended> {
        self.push_encoded(self.conn.encode(item).into(), cursor)
    }

    /// Queue an item encoded once and shared by every subscriber.
    pub(crate) fn push_encoded(
        &self,
        bytes: Arc<[u8]>,
        cursor: Option<StreamCursor>,
    ) -> Result<(), Ended> {
        let conn = &self.conn;
        let mut guard = conn.state.lock();
        let state = &mut *guard;
        if state.closing || state.stopped {
            return Err(Ended);
        }
        let Some(open) = state.channels.get_mut(&self.channel) else {
            return Err(Ended);
        };
        let budget = conn.budget;
        let overflow = open.items.len() >= budget.items
            || open.item_bytes + bytes.len() > budget.item_bytes
            || state.queued_bytes + bytes.len() > budget.connection_bytes;
        if overflow {
            let removed = remove_channel(state, self.channel, false).expect("channel exists");
            drop(guard);
            conn.push_message(&ServerMessage::End(End {
                channel: self.channel,
                reason: EndReason::Overflow,
                resume_after: removed.delivered,
            }));
            return Err(Ended);
        }
        open.item_bytes += bytes.len();
        state.queued_bytes += bytes.len();
        open.items.push_back((bytes, cursor));
        if !open.token_queued {
            open.token_queued = true;
            state.messages.push_back(Queued::Channel(self.channel));
        }
        drop(guard);
        conn.wake.notify_one();
        Ok(())
    }

    /// Wait until the client has granted DATA credit; returns the credit.
    pub(crate) async fn credit(&self) -> Result<u64, Ended> {
        loop {
            let wake = {
                let state = self.conn.state.lock();
                let open = state.channels.get(&self.channel).ok_or(Ended)?;
                if open.credit > 0 {
                    return Ok(open.credit);
                }
                open.credit_wake.clone()
            };
            wake.notified().await;
        }
    }

    /// Queue a DATA chunk within the credit [`Self::credit`] returned.
    pub(crate) fn data(&self, fin: bool, body: Vec<u8>) -> Result<(), Ended> {
        let mut guard = self.conn.state.lock();
        let state = &mut *guard;
        if state.closing || state.stopped {
            return Err(Ended);
        }
        let open = state.channels.get_mut(&self.channel).ok_or(Ended)?;
        open.credit = open.credit.saturating_sub(body.len() as u64);
        open.pending_data += 1;
        state.queued_bytes += body.len();
        state.data.push_back(DataEntry::Chunk {
            channel: self.channel,
            fin,
            body,
        });
        drop(guard);
        self.conn.wake.notify_one();
        Ok(())
    }

    /// End the subscription after everything it queued.
    pub(crate) fn end(&self, reason: EndReason) {
        let conn = &self.conn;
        let mut guard = conn.state.lock();
        let state = &mut *guard;
        if !state.channels.contains_key(&self.channel) {
            return;
        }
        // Items still queued go out now, ahead of the `end`.
        let mut envelopes = Vec::new();
        while let Some(envelope) = conn.take_items(state, self.channel) {
            envelopes.push(envelope);
        }
        let Some(removed) = remove_channel(state, self.channel, false) else {
            return;
        };
        let end = conn.encode(&ServerMessage::End(End {
            channel: self.channel,
            reason,
            resume_after: removed.delivered,
        }));
        for envelope in envelopes {
            state.queued_bytes += envelope.len();
            state.messages.push_back(Queued::Encoded(envelope));
        }
        if removed.pending_data > 0 {
            state.data.push_back(DataEntry::Message(end));
        } else {
            state.queued_bytes += end.len();
            state.messages.push_back(Queued::Encoded(end));
        }
        drop(guard);
        conn.wake.notify_one();
    }
}

/// Drains the queues onto the socket. The only timer is the keepalive: after
/// `pingIntervalMs` with nothing written it sends a `ping`.
pub(crate) async fn run_writer<S>(
    conn: Arc<Conn>,
    mut writer: FrameWriter<S>,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let interval = Duration::from_millis(u64::from(conn.limits.ping_interval_ms));
    let stall = Duration::from_millis(u64::from(WRITE_STALL_MS));
    let mut last_write = Instant::now();
    let mut unflushed = 0;
    loop {
        match conn.next() {
            Next::Write(outgoing) => {
                within(stall, writer.feed(outgoing)).await?;
                last_write = Instant::now();
                unflushed += 1;
                if unflushed >= MAX_FRAMES_PER_FLUSH {
                    within(stall, writer.flush()).await?;
                    unflushed = 0;
                }
            }
            Next::Last(outgoing) => {
                within(stall, writer.feed(outgoing)).await?;
                within(stall, writer.flush()).await?;
                writer.close().await;
                return Ok(());
            }
            Next::Stop => {
                if unflushed > 0 {
                    let _ = within(stall, writer.flush()).await;
                }
                writer.close().await;
                return Ok(());
            }
            Next::Idle => {
                if unflushed > 0 {
                    within(stall, writer.flush()).await?;
                    unflushed = 0;
                }
                let deadline = tokio::time::Instant::from_std(last_write + interval);
                if tokio::time::timeout_at(deadline, conn.wake.notified())
                    .await
                    .is_err()
                {
                    conn.ping();
                }
            }
        }
    }
}

async fn within<T>(
    stall: Duration,
    write: impl Future<Output = std::io::Result<T>>,
) -> std::io::Result<T> {
    tokio::time::timeout(stall, write).await.map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "the client stopped reading (writeStalled)",
        )
    })?
}

/// Write one message during the handshake, before the writer task exists.
async fn send_now<S: AsyncRead + AsyncWrite + Unpin>(
    writer: &mut FrameWriter<S>,
    encoding: Encoding,
    message: &ServerMessage,
) {
    let Ok(bytes) = codec::to_vec(encoding, message) else {
        return;
    };
    let _ = writer.feed(Outgoing::Message(bytes)).await;
    let _ = writer.flush().await;
}

async fn refuse<S: AsyncRead + AsyncWrite + Unpin>(
    writer: &mut FrameWriter<S>,
    encoding: Encoding,
    code: GoodbyeCode,
    message: String,
) {
    send_now(
        writer,
        encoding,
        &ServerMessage::Goodbye {
            code,
            message,
            reconnect: false,
        },
    )
    .await;
    writer.close().await;
}

fn authenticate(context: &V8Context, hello: &Hello) -> Result<(), Rejected> {
    hello.check_version()?;
    let unauthenticated = |message: &str| Rejected {
        code: RejectCode::Unauthenticated,
        message: message.into(),
        retry_after_ms: None,
    };
    match &hello.credential {
        Credential::Local { token }
            if bool::from(context.token.as_bytes().ct_eq(token.as_bytes())) =>
        {
            Ok(())
        }
        Credential::Local { .. } => Err(unauthenticated("authentication failed")),
        // Pairing and session tokens arrive with remote access (Phase 7).
        _ => Err(unauthenticated(
            "only the local credential is accepted by this daemon",
        )),
    }
}

/// Serve one connection whose transport is set up (prefaces exchanged or
/// WebSocket upgraded). `authenticated` runs once `hello` checks out.
pub(crate) async fn serve<S>(
    mut reader: FrameReader<S>,
    mut writer: FrameWriter<S>,
    context: Arc<V8Context>,
    authenticated: impl FnOnce(),
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let first = tokio::time::timeout(context.hello_deadline, reader.next()).await;
    let (body, encoding) = match first {
        Ok(Ok(Some(Incoming::Message(body, encoding)))) => (body, encoding),
        Ok(Err(ReadError::Frame(error))) => {
            refuse(
                &mut writer,
                Encoding::MessagePack,
                error.goodbye_code(),
                error.to_string(),
            )
            .await;
            return;
        }
        Ok(Ok(Some(Incoming::Data { .. }))) => {
            refuse(
                &mut writer,
                Encoding::MessagePack,
                GoodbyeCode::ProtocolError,
                "the first message must be hello".into(),
            )
            .await;
            return;
        }
        // No hello in time, a read error, or the client left.
        _ => return,
    };
    writer.set_encoding(encoding);
    let hello = match codec::from_slice::<ClientMessage>(encoding, &body) {
        Ok(ClientMessage::Hello(hello)) => hello,
        _ => {
            refuse(
                &mut writer,
                encoding,
                GoodbyeCode::ProtocolError,
                "the first message must be hello".into(),
            )
            .await;
            return;
        }
    };
    if let Err(rejected) = authenticate(&context, &hello) {
        send_now(&mut writer, encoding, &ServerMessage::Rejected(rejected)).await;
        writer.close().await;
        return;
    }
    authenticated();
    let mut limits = context.limits;
    if let Some(client_cap) = hello.max_message_bytes {
        limits.max_message_bytes = limits.max_message_bytes.min(client_cap);
    }
    reader.set_limits(FrameLimits::session(context.limits.max_message_bytes));
    // The local socket and the daemon token both grant everything (§6.1).
    let scopes = vec![Scope::Admin];
    send_now(
        &mut writer,
        encoding,
        &ServerMessage::Welcome(Welcome {
            protocol: ProtocolVersion::CURRENT,
            daemon_version: env!("CARGO_PKG_VERSION").into(),
            environment: context.environment.clone(),
            session: SessionInfo {
                id: uuid::Uuid::new_v4(),
                scopes: scopes.clone(),
                expires_at: None,
            },
            issued_credential: None,
            limits,
            compression: None,
            features: Vec::new(),
        }),
    )
    .await;

    let conn = Arc::new(Conn::new(
        encoding,
        limits,
        scopes,
        hello.client.kind.clone(),
        context.budget,
    ));
    let mut writer_task = tokio::spawn(run_writer(conn.clone(), writer));
    tokio::select! {
        () = read_loop(&mut reader, &conn, &context) => {}
        () = context.shutdown.triggered() => {
            conn.goodbye(GoodbyeCode::ShuttingDown, "the daemon is shutting down", true);
        }
        // The writer failed (a write stall or a dead socket).
        _ = &mut writer_task => {
            conn.stop();
            return;
        }
    }
    conn.stop();
    if tokio::time::timeout(CLOSE_GRACE, &mut writer_task)
        .await
        .is_err()
    {
        writer_task.abort();
    }
}

async fn read_loop<S>(reader: &mut FrameReader<S>, conn: &Arc<Conn>, context: &Arc<V8Context>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let idle = Duration::from_millis(u64::from(conn.limits.ping_timeout_ms));
    loop {
        let next = match tokio::time::timeout(idle, reader.next()).await {
            Ok(Ok(Some(next))) => next,
            Ok(Ok(None)) => return,
            Ok(Err(ReadError::Frame(error))) => {
                conn.goodbye(error.goodbye_code(), error.to_string(), false);
                return;
            }
            Ok(Err(_)) => return,
            Err(_) => {
                conn.goodbye(
                    GoodbyeCode::IdleTimeout,
                    format!("nothing received for {} ms", idle.as_millis()),
                    true,
                );
                return;
            }
        };
        match next {
            // Uploads (`blobs.put`) are not served yet. DATA on a channel
            // without a transfer is discarded (RFC 0003 §5.6).
            Incoming::Data { .. } => {}
            Incoming::Message(body, encoding) => {
                if encoding != conn.encoding {
                    conn.goodbye(
                        GoodbyeCode::ProtocolError,
                        "message encodings are mixed on one connection",
                        false,
                    );
                    return;
                }
                let message = match codec::from_slice::<ClientMessage>(encoding, &body) {
                    Ok(message) => message,
                    Err(error) => {
                        if !reject_malformed_call(conn, encoding, &body, &error) {
                            conn.goodbye(GoodbyeCode::ProtocolError, error.to_string(), false);
                            return;
                        }
                        continue;
                    }
                };
                if !handle_message(conn, context, message) {
                    return;
                }
            }
        }
    }
}

/// A call whose params do not decode is answered `badRequest` instead of
/// closing the connection.
fn reject_malformed_call(
    conn: &Conn,
    encoding: Encoding,
    body: &[u8],
    error: &codec::CodecError,
) -> bool {
    #[derive(serde::Deserialize)]
    struct CallProbe {
        #[serde(rename = "type")]
        kind: String,
        id: u32,
    }
    match codec::from_slice::<CallProbe>(encoding, body) {
        Ok(probe) if probe.kind == "call" => {
            conn.push_reply(Reply::error(
                probe.id,
                Error::new(ErrorCode::BadRequest, error.to_string()),
            ));
            true
        }
        _ => false,
    }
}

/// Act on one client message; `false` ends the connection.
fn handle_message(conn: &Arc<Conn>, context: &Arc<V8Context>, message: ClientMessage) -> bool {
    match message {
        ClientMessage::Hello(_) => {
            conn.goodbye(GoodbyeCode::ProtocolError, "hello was already sent", false);
            false
        }
        ClientMessage::Call { id, method } => {
            handle_call(conn, context, id, method);
            true
        }
        ClientMessage::Cancel { id } => {
            if conn.cancel_call(id) {
                conn.push_reply(Reply::error(
                    id,
                    Error::new(ErrorCode::Cancelled, "the call was cancelled"),
                ));
            }
            true
        }
        ClientMessage::Subscribe(subscribe) => handle_subscribe(conn, context, subscribe),
        ClientMessage::Unsubscribe { channel } => {
            conn.unsubscribe(channel);
            true
        }
        ClientMessage::Credit { channel, bytes } => {
            conn.add_credit(channel, bytes);
            true
        }
        ClientMessage::Ping { nonce } => {
            conn.push_control(&ServerMessage::Pong { nonce });
            true
        }
        ClientMessage::Pong { .. } | ClientMessage::Unknown => true,
        ClientMessage::Goodbye { .. } => false,
    }
}

fn handle_call(conn: &Arc<Conn>, context: &Arc<V8Context>, id: u32, method: Method) {
    let Some(scope) = method.scope() else {
        conn.push_reply(Reply::error(
            id,
            Error::new(
                ErrorCode::UnknownMethod,
                format!("unknown method {}", method.name()),
            ),
        ));
        return;
    };
    if !scope.granted_by(&conn.scopes) {
        conn.push_reply(Reply::error(
            id,
            Error::new(
                ErrorCode::Forbidden,
                format!("{} needs {scope}", method.name()),
            )
            .with_details(ErrorDetails::MissingScope { required: scope }),
        ));
        return;
    }
    match conn.begin_call(id) {
        Ok(()) => {}
        Err(CallRejected::Busy) => {
            conn.push_reply(Reply::error(
                id,
                Error::new(ErrorCode::Busy, "too many calls in flight")
                    .retryable()
                    .with_details(ErrorDetails::Limit {
                        limit: u64::from(conn.limits.max_in_flight_calls),
                    }),
            ));
            return;
        }
        Err(CallRejected::Duplicate) => {
            conn.push_reply(Reply::error(
                id,
                Error::new(ErrorCode::BadRequest, format!("call id {id} is in flight")),
            ));
            return;
        }
    }
    let task_conn = conn.clone();
    let task_context = context.clone();
    let task = tokio::spawn(async move {
        let reply = match method {
            // Async: it waits for this connection's streams after the commit.
            Method::OrchestrationDispatch(params) => {
                match orchestration::dispatch(&task_conn, &task_context, params).await {
                    Ok(result) => Reply::ok(id, MethodResult::OrchestrationDispatch(result)),
                    Err(error) => Reply::error(id, error),
                }
            }
            method => {
                let outcome =
                    tokio::task::spawn_blocking(move || methods::handle(&task_context, method))
                        .await;
                match outcome {
                    Ok(Ok(result)) => Reply::ok(id, result),
                    Ok(Err(error)) => Reply::error(id, error),
                    Err(_) => Reply::error(id, Error::new(ErrorCode::Internal, "the call failed")),
                }
            }
        };
        if task_conn.finish_call(id) {
            task_conn.push_reply(reply);
        }
    });
    conn.set_call_task(id, task.abort_handle());
}

fn handle_subscribe(conn: &Arc<Conn>, context: &Arc<V8Context>, subscribe: Subscribe) -> bool {
    let Subscribe {
        channel,
        stream,
        after,
        window,
    } = subscribe;
    if channel == 0 {
        conn.goodbye(GoodbyeCode::ProtocolError, "channel 0 is reserved", false);
        return false;
    }
    match conn.open_channel(channel, window.unwrap_or(DEFAULT_DATA_WINDOW)) {
        Ok(()) => {}
        Err(OpenRejected::InUse) => {
            conn.goodbye(
                GoodbyeCode::ProtocolError,
                format!("channel {channel} is in use"),
                false,
            );
            return false;
        }
        Err(OpenRejected::TooMany) => {
            conn.push_message(&ServerMessage::End(End {
                channel,
                reason: EndReason::Error {
                    error: Error::new(ErrorCode::Busy, "too many subscriptions").with_details(
                        ErrorDetails::Limit {
                            limit: u64::from(conn.limits.max_subscriptions),
                        },
                    ),
                },
                resume_after: None,
            }));
            return true;
        }
    }
    let sink = StreamSink {
        conn: conn.clone(),
        channel,
    };
    let Some(scope) = stream.scope() else {
        sink.end(EndReason::Error {
            error: Error::new(ErrorCode::Unsupported, "unknown stream"),
        });
        return true;
    };
    if !scope.granted_by(&conn.scopes) {
        sink.end(EndReason::Forbidden);
        return true;
    }
    let context = context.clone();
    let task = match stream {
        StreamSpec::Settings(_) => tokio::spawn(streams::settings(sink, context, after)),
        StreamSpec::BlobDownload(spec) => tokio::spawn(streams::blob_download(sink, context, spec)),
        StreamSpec::Shell(spec) => {
            tokio::spawn(orchestration::shell_stream(sink, context, spec, after))
        }
        StreamSpec::Thread(spec) => {
            tokio::spawn(orchestration::thread_stream(sink, context, spec, after))
        }
        StreamSpec::CommandOutput(spec) => {
            tokio::spawn(orchestration::command_output_stream(sink, context, spec))
        }
        StreamSpec::Unknown => return true,
    };
    conn.set_channel_task(channel, task.abort_handle());
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use waku_protocol::v8::message::Routes;
    use waku_protocol::v8::{ServerMessageSeed, SettingsItem, StreamItem};

    fn conn(budget: Budget) -> Arc<Conn> {
        Arc::new(Conn::new(
            Encoding::MessagePack,
            Limits::default(),
            vec![Scope::Admin],
            ClientKind::Desktop,
            budget,
        ))
    }

    fn decode(outgoing: Outgoing) -> ServerMessage {
        let Outgoing::Message(bytes) = outgoing else {
            panic!("expected a message");
        };
        let routes = Routes {
            calls: Default::default(),
            channels: [(5, "settings"), (6, "settings")].into(),
        };
        codec::from_slice_seed(Encoding::MessagePack, &bytes, ServerMessageSeed(&routes)).unwrap()
    }

    fn item(revision: u64) -> SettingsItem {
        SettingsItem::Synchronized {
            cursor: StreamCursor::Revision { revision },
        }
    }

    fn cursor(revision: u64) -> Option<StreamCursor> {
        Some(StreamCursor::Revision { revision })
    }

    #[test]
    fn an_overflowing_subscription_is_kicked_with_its_last_delivered_cursor() {
        let conn = conn(Budget {
            items: 3,
            ..Budget::default()
        });
        conn.open_channel(5, 0).ok().unwrap();
        conn.open_channel(6, 0).ok().unwrap();
        let sink = StreamSink {
            conn: conn.clone(),
            channel: 5,
        };
        let other = StreamSink {
            conn: conn.clone(),
            channel: 6,
        };
        sink.push(&item(1), cursor(1)).ok().unwrap();
        sink.push(&item(2), cursor(2)).ok().unwrap();
        other.push(&item(10), cursor(10)).ok().unwrap();
        // The writer takes channel 5's two items: delivered = revision 2.
        let Next::Write(first) = conn.next() else {
            panic!("expected items");
        };
        assert_eq!(
            decode(first),
            ServerMessage::Items {
                channel: 5,
                items: vec![StreamItem::Settings(item(1)), StreamItem::Settings(item(2))]
            }
        );
        // The client stops reading; channel 5 fills its budget of 3.
        for revision in 3..6 {
            sink.push(&item(revision), cursor(revision)).ok().unwrap();
        }
        assert!(
            sink.push(&item(6), cursor(6)).is_err(),
            "the fourth queued item overflows"
        );
        assert!(
            sink.push(&item(7), cursor(7)).is_err(),
            "the stream stays ended"
        );
        // Channel 6 is unaffected, and channel 5 ends with the cursor it
        // actually delivered, not the ones it dropped.
        let mut rest = Vec::new();
        while let Next::Write(outgoing) = conn.next() {
            rest.push(decode(outgoing));
        }
        assert_eq!(
            rest,
            vec![
                ServerMessage::Items {
                    channel: 6,
                    items: vec![StreamItem::Settings(item(10))]
                },
                ServerMessage::End(End {
                    channel: 5,
                    reason: EndReason::Overflow,
                    resume_after: cursor(2),
                }),
            ]
        );
        assert!(other.push(&item(11), cursor(11)).is_ok());
        let channel_bytes = other_bytes(&conn);
        assert_eq!(conn.state.lock().queued_bytes, channel_bytes);
    }

    fn other_bytes(conn: &Conn) -> usize {
        // Bytes of every subscription's queued items.
        conn.state
            .lock()
            .channels
            .values()
            .map(|channel| channel.item_bytes)
            .sum()
    }

    #[test]
    fn data_is_written_after_messages_and_before_its_end() {
        let conn = conn(Budget::default());
        conn.open_channel(5, 8).ok().unwrap();
        let sink = StreamSink {
            conn: conn.clone(),
            channel: 5,
        };
        sink.data(false, b"abcd".to_vec()).ok().unwrap();
        sink.data(true, b"efgh".to_vec()).ok().unwrap();
        sink.end(EndReason::Completed);
        conn.push_message(&ServerMessage::Pong { nonce: 1 });
        conn.push_control(&ServerMessage::Ping { nonce: 2 });
        let mut order = Vec::new();
        loop {
            match conn.next() {
                Next::Write(Outgoing::Data { fin, body, .. }) => {
                    order.push(format!("data {} {fin}", String::from_utf8(body).unwrap()));
                }
                Next::Write(message) => order.push(format!("{:?}", decode(message))),
                _ => break,
            }
        }
        assert_eq!(
            order,
            [
                "Ping { nonce: 2 }",
                "Pong { nonce: 1 }",
                "data abcd false",
                "data efgh true",
                "End(End { channel: 5, reason: Completed, resume_after: None })",
            ]
        );
        assert_eq!(conn.state.lock().queued_bytes, 0);
    }

    #[test]
    fn credit_is_spent_and_returned() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let conn = conn(Budget::default());
        conn.open_channel(5, 4).ok().unwrap();
        let sink = StreamSink {
            conn: conn.clone(),
            channel: 5,
        };
        runtime.block_on(async {
            assert_eq!(sink.credit().await.ok(), Some(4));
            sink.data(false, vec![0; 4]).ok().unwrap();
            let waiting = tokio::time::timeout(Duration::from_millis(20), sink.credit()).await;
            assert!(waiting.is_err(), "no credit left");
            conn.add_credit(5, 10);
            assert_eq!(sink.credit().await.ok(), Some(10));
        });
        conn.unsubscribe(5);
        assert!(sink.data(true, Vec::new()).is_err());
        // The dropped DATA no longer counts; only the `end` is queued.
        let end = end_bytes(&conn);
        assert_eq!(conn.state.lock().queued_bytes, end);
    }

    fn end_bytes(conn: &Conn) -> usize {
        conn.state
            .lock()
            .messages
            .iter()
            .map(|entry| match entry {
                Queued::Encoded(bytes) => bytes.len(),
                Queued::Channel(_) => 0,
            })
            .sum()
    }

    #[test]
    fn a_goodbye_is_the_last_frame() {
        let conn = conn(Budget::default());
        conn.push_message(&ServerMessage::Pong { nonce: 1 });
        conn.goodbye(GoodbyeCode::ShuttingDown, "bye", true);
        conn.push_control(&ServerMessage::Ping { nonce: 3 });
        assert!(matches!(conn.next(), Next::Last(_)));
        assert!(matches!(conn.next(), Next::Stop));
    }
}
