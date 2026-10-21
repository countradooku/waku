//! The desktop's connection to `waku-daemon`.
//!
//! Each [`DaemonClient`] owns one thread that runs a current-thread tokio
//! runtime (#18): a reader blocked on the socket and a writer blocked on the
//! outgoing queue, plus the keepalive timers. Nothing polls, so a request
//! goes out the moment it is queued and an idle connection costs a keepalive
//! ping every few seconds. Callers never see the runtime: requests block on
//! crossbeam channels and events arrive on crossbeam receivers, so GUI and
//! CLI code stay runtime-agnostic.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context as _, anyhow, bail};
use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt as _, StreamExt as _};
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use uuid::Uuid;

use waku_protocol::{
    ClientMessage, Command, KEEPALIVE_INTERVAL, KEEPALIVE_TIMEOUT, MAX_WIRE_MESSAGE_BYTES,
    PROTOCOL_VERSION, ReplayCursor, Request, ResponseOutcome, ResponsePayload, RpcError,
    SequencedEvent, ServerMessage, WireDriverEvent,
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
/// Connecting, the WebSocket upgrade (and TLS for `wss://`) and the hello
/// exchange must finish within this.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_BUFFERED_EVENTS_PER_RUNTIME: usize = 4096;
/// Queued requests written before one flush.
const MAX_MESSAGES_PER_FLUSH: usize = 64;

enum Outgoing {
    Message(ClientMessage),
    Shutdown,
    Disconnect,
}

struct ClientInner {
    outgoing: mpsc::UnboundedSender<Outgoing>,
    pending: Mutex<HashMap<Uuid, Sender<Result<ResponsePayload, RpcError>>>>,
    sessions: Mutex<HashMap<(Uuid, Uuid), Sender<SequencedEvent>>>,
    pending_events: Mutex<HashMap<(Uuid, Uuid), VecDeque<SequencedEvent>>>,
    task_state_subscribers: Mutex<Vec<Sender<u64>>>,
    last_sequences: Mutex<HashMap<(Uuid, Uuid), LastSequence>>,
    disconnected: AtomicBool,
    /// Run once when the connection closes (see [`DaemonClient::on_disconnect`]).
    disconnect_callbacks: Mutex<Vec<DisconnectCallback>>,
}

type DisconnectCallback = Box<dyn FnOnce() + Send>;

#[derive(Clone, Copy)]
struct LastSequence {
    epoch: Uuid,
    sequence: u64,
}

#[derive(Clone)]
pub struct DaemonClient {
    inner: Arc<ClientInner>,
}

impl DaemonClient {
    pub fn connect(address: &str, token: String) -> anyhow::Result<Self> {
        Self::connect_with_resume(address, token, Vec::new())
    }

    pub fn connect_with_resume(
        address: &str,
        token: String,
        resume_from: Vec<ReplayCursor>,
    ) -> anyhow::Result<Self> {
        let last_sequences = resume_from
            .iter()
            .map(|cursor| {
                (
                    (cursor.session_id, cursor.runtime_id),
                    LastSequence {
                        epoch: cursor.epoch,
                        sequence: cursor.sequence,
                    },
                )
            })
            .collect();
        let endpoint = Endpoint::parse(address)?;
        let (outgoing, outgoing_rx) = mpsc::unbounded_channel();
        let inner = Arc::new(ClientInner {
            outgoing,
            pending: Mutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
            pending_events: Mutex::new(HashMap::new()),
            task_state_subscribers: Mutex::new(Vec::new()),
            last_sequences: Mutex::new(last_sequences),
            disconnected: AtomicBool::new(false),
            disconnect_callbacks: Mutex::new(Vec::new()),
        });
        let hello = ClientMessage::Hello {
            protocol_version: PROTOCOL_VERSION,
            token,
            client_id: Uuid::new_v4(),
            resume_from,
            binary_frames: Some(true),
        };
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let thread_inner = inner.clone();
        std::thread::Builder::new()
            .name("waku-daemon-client".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready_tx.send(Err(anyhow!(error)));
                        return;
                    }
                };
                runtime.block_on(connect_and_run(
                    endpoint,
                    hello,
                    ready_tx,
                    outgoing_rx,
                    &thread_inner,
                ));
                disconnected(&thread_inner);
            })
            .context("could not start Waku daemon client thread")?;
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self { inner }),
            Ok(Err(error)) => Err(error),
            Err(_) => bail!("Waku daemon client thread exited during the handshake"),
        }
    }

    pub fn subscribe(&self, session_id: Uuid, runtime_id: Uuid) -> Receiver<SequencedEvent> {
        let (events, receiver) = unbounded();
        let key = (session_id, runtime_id);
        let mut sessions = self.inner.sessions.lock();
        sessions.insert(key, events.clone());
        // Keep the subscription lock while draining the pre-subscription
        // replay queue. The socket thread takes these locks in the same order,
        // so a new live event cannot overtake older replayed events here.
        if let Some(buffered) = self.inner.pending_events.lock().remove(&key) {
            for event in buffered {
                let _ = events.send(event);
            }
        }
        receiver
    }

    /// Whether two handles send through the same WebSocket connection.
    ///
    /// The daemon supervisor publishes replacement clients after a managed
    /// restart. Runtime adapters use this identity check to ignore the
    /// subscription's initial snapshot and wait for an actual replacement.
    pub fn same_connection(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub fn is_disconnected(&self) -> bool {
        self.inner.disconnected.load(Ordering::Acquire)
    }

    /// Run `callback` once this connection closes, or right away when it
    /// already has. This is the event a supervisor waits on instead of
    /// polling [`Self::is_disconnected`] (#178). The callback runs on the
    /// connection thread as it exits, so it must not block.
    pub fn on_disconnect(&self, callback: impl FnOnce() + Send + 'static) {
        let mut callbacks = self.inner.disconnect_callbacks.lock();
        // `disconnected` sets the flag before it takes this lock, so either
        // the flag is visible here or the callback is queued in time.
        if !self.is_disconnected() {
            callbacks.push(Box::new(callback));
            return;
        }
        drop(callbacks);
        callback();
    }

    pub fn unsubscribe(&self, session_id: Uuid, runtime_id: Uuid) {
        self.inner.sessions.lock().remove(&(session_id, runtime_id));
    }

    pub fn subscribe_task_state(&self) -> Receiver<u64> {
        let (events, receiver) = unbounded();
        self.inner.task_state_subscribers.lock().push(events);
        receiver
    }

    pub fn request(
        &self,
        session_id: Uuid,
        runtime_id: Uuid,
        command: Command,
    ) -> anyhow::Result<ResponsePayload> {
        if self.inner.disconnected.load(Ordering::Acquire) {
            bail!("Waku daemon is disconnected");
        }
        let request_id = Uuid::new_v4();
        let (response, response_rx) = bounded(1);
        self.inner.pending.lock().insert(request_id, response);
        let message = ClientMessage::Request(Request {
            request_id,
            session_id,
            runtime_id,
            command,
        });
        if self
            .inner
            .outgoing
            .send(Outgoing::Message(message))
            .is_err()
        {
            self.inner.pending.lock().remove(&request_id);
            bail!("Waku daemon connection is closed");
        }
        match response_rx.recv_timeout(REQUEST_TIMEOUT) {
            Ok(Ok(payload)) => Ok(payload),
            Ok(Err(error)) => Err(anyhow!(error.message)),
            Err(error) => {
                self.inner.pending.lock().remove(&request_id);
                Err(anyhow!("timed out waiting for Waku daemon: {error}"))
            }
        }
    }

    pub fn notify(
        &self,
        session_id: Uuid,
        runtime_id: Uuid,
        command: Command,
    ) -> anyhow::Result<()> {
        if self.inner.disconnected.load(Ordering::Acquire) {
            bail!("Waku daemon is disconnected");
        }
        self.inner
            .outgoing
            .send(Outgoing::Message(ClientMessage::Request(Request {
                // The nil request id is reserved for fire-and-forget controls;
                // the daemon executes them in the runtime mailbox but does
                // not allocate or send a response.
                request_id: Uuid::nil(),
                session_id,
                runtime_id,
                command,
            })))
            .map_err(|_| anyhow!("Waku daemon connection is closed"))
    }

    pub fn last_sequences(&self) -> Vec<ReplayCursor> {
        self.inner
            .last_sequences
            .lock()
            .iter()
            .map(|(&(session_id, runtime_id), cursor)| ReplayCursor {
                session_id,
                runtime_id,
                epoch: cursor.epoch,
                sequence: cursor.sequence,
            })
            .collect()
    }

    pub fn shutdown(&self) {
        let _ = self.inner.outgoing.send(Outgoing::Shutdown);
    }

    /// Close this connection without asking the daemon to stop. Runtimes are
    /// daemon-owned, so anything this client started keeps running and other
    /// clients stay attached; the daemon just stops writing to this socket.
    pub fn disconnect(&self) {
        let _ = self.inner.outgoing.send(Outgoing::Disconnect);
    }
}

/// The address form of a daemon's local endpoint, as [`DaemonClient::connect`]
/// accepts it: `unix:<path>` for a Unix socket, `pipe:<name>` for a Windows
/// named pipe.
pub fn local_endpoint_address(socket: &str) -> String {
    if cfg!(windows) {
        format!("pipe:{socket}")
    } else {
        format!("unix:{socket}")
    }
}

/// Where a [`DaemonClient`] connects.
#[derive(Debug, PartialEq)]
enum Endpoint {
    /// `host:port`, `ws://â¦` or `wss://â¦`, normalized to the `/v1` URL.
    WebSocket(String),
    /// `unix:/path/to/daemon.sock`
    Unix(std::path::PathBuf),
    /// `pipe:\\.\pipe\name`, or a bare `\\.\pipe\name`
    Pipe(String),
}

impl Endpoint {
    fn parse(address: &str) -> anyhow::Result<Self> {
        if let Some(path) = address.strip_prefix("unix:") {
            if path.is_empty() {
                bail!("Waku daemon socket path is empty");
            }
            return Ok(Self::Unix(path.into()));
        }
        if let Some(name) = address.strip_prefix("pipe:") {
            return Ok(Self::Pipe(name.to_owned()));
        }
        if address.starts_with(r"\\.\pipe\") {
            return Ok(Self::Pipe(address.to_owned()));
        }
        daemon_url(address).map(Self::WebSocket)
    }
}

fn daemon_url(address: &str) -> anyhow::Result<String> {
    let normalized = if address.starts_with("ws://") || address.starts_with("wss://") {
        address.to_owned()
    } else {
        format!("ws://{address}")
    };
    let mut url = url::Url::parse(&normalized).context("Waku daemon address is invalid")?;
    url.set_path("/v1");
    url.set_query(None);
    url.set_fragment(None);
    Ok(url.into())
}

/// The WebSocket URL sent over a local socket. Only the path is checked.
#[cfg(any(unix, windows))]
const LOCAL_WEBSOCKET_URL: &str = "ws://localhost/v1";

fn websocket_config() -> WebSocketConfig {
    WebSocketConfig::default()
        .max_message_size(Some(MAX_WIRE_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_WIRE_MESSAGE_BYTES))
}

type Ready = std::sync::mpsc::SyncSender<anyhow::Result<()>>;

/// Connects, says hello, reports the outcome through `ready`, and then runs
/// the connection until either side closes it.
async fn connect_and_run(
    endpoint: Endpoint,
    hello: ClientMessage,
    ready: Ready,
    outgoing: mpsc::UnboundedReceiver<Outgoing>,
    inner: &ClientInner,
) {
    match endpoint {
        Endpoint::WebSocket(url) => {
            let connect = async {
                let (socket, _) = tokio_tungstenite::connect_async_with_config(
                    url,
                    Some(websocket_config()),
                    true,
                )
                .await
                .context("could not connect to Waku daemon")?;
                handshake(socket, &hello).await
            };
            run_connected(connect, ready, outgoing, inner).await;
        }
        #[cfg(unix)]
        Endpoint::Unix(path) => {
            let connect = async {
                let stream = tokio::net::UnixStream::connect(&path)
                    .await
                    .with_context(|| {
                        format!("could not connect to Waku daemon at {}", path.display())
                    })?;
                let (socket, _) = tokio_tungstenite::client_async_with_config(
                    LOCAL_WEBSOCKET_URL,
                    stream,
                    Some(websocket_config()),
                )
                .await?;
                handshake(socket, &hello).await
            };
            run_connected(connect, ready, outgoing, inner).await;
        }
        #[cfg(windows)]
        Endpoint::Pipe(name) => {
            let connect = async {
                let stream = open_pipe(&name)
                    .await
                    .with_context(|| format!("could not connect to Waku daemon at {name}"))?;
                let (socket, _) = tokio_tungstenite::client_async_with_config(
                    LOCAL_WEBSOCKET_URL,
                    stream,
                    Some(websocket_config()),
                )
                .await?;
                handshake(socket, &hello).await
            };
            run_connected(connect, ready, outgoing, inner).await;
        }
        #[allow(unreachable_patterns)]
        other => {
            let _ = ready.send(Err(anyhow!("{other:?} is not supported on this platform")));
        }
    }
}

/// A named pipe whose server has not created an instance yet, or whose
/// instances are all busy, is retried until the connect timeout.
#[cfg(windows)]
async fn open_pipe(
    name: &str,
) -> std::io::Result<tokio::net::windows::named_pipe::NamedPipeClient> {
    const ERROR_PIPE_BUSY: i32 = 231;
    loop {
        match tokio::net::windows::named_pipe::ClientOptions::new().open(name) {
            Ok(client) => return Ok(client),
            Err(error)
                if error.raw_os_error() == Some(ERROR_PIPE_BUSY)
                    || error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn run_connected<S>(
    connect: impl Future<Output = anyhow::Result<(WebSocketStream<S>, bool)>>,
    ready: Ready,
    outgoing: mpsc::UnboundedReceiver<Outgoing>,
    inner: &ClientInner,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (socket, binary_frames) = match tokio::time::timeout(CONNECT_TIMEOUT, connect).await {
        Ok(Ok(connected)) => connected,
        Ok(Err(error)) => {
            let _ = ready.send(Err(error));
            return;
        }
        Err(_) => {
            let _ = ready.send(Err(anyhow!("timed out connecting to Waku daemon")));
            return;
        }
    };
    let _ = ready.send(Ok(()));
    run_client(socket, binary_frames, outgoing, inner).await;
}

async fn handshake<S>(
    mut socket: WebSocketStream<S>,
    hello: &ClientMessage,
) -> anyhow::Result<(WebSocketStream<S>, bool)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    socket.send(text_message(hello)?).await?;
    let response = loop {
        match socket.next().await {
            Some(Ok(Message::Text(text))) => {
                break serde_json::from_str::<ServerMessage>(text.as_str())?;
            }
            Some(Ok(Message::Close(_))) | None => bail!("Waku daemon closed during handshake"),
            Some(Ok(_)) => {}
            Some(Err(error)) => return Err(error.into()),
        }
    };
    match response {
        ServerMessage::Hello {
            protocol_version,
            binary_frames,
            ..
        } if protocol_version == PROTOCOL_VERSION => Ok((socket, binary_frames == Some(true))),
        ServerMessage::Hello {
            protocol_version, ..
        } => bail!(
            "daemon protocol {protocol_version} does not match desktop protocol {PROTOCOL_VERSION}"
        ),
        ServerMessage::Rejected { message } => bail!("daemon rejected connection: {message}"),
        other => bail!("daemon sent an invalid handshake response: {other:?}"),
    }
}

async fn run_client<S>(
    socket: WebSocketStream<S>,
    binary_frames: bool,
    outgoing: mpsc::UnboundedReceiver<Outgoing>,
    inner: &ClientInner,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (sink, stream) = socket.split();
    tokio::select! {
        () = write_outgoing(sink, outgoing, binary_frames) => {}
        () = read_incoming(stream, inner) => {}
    }
}

/// Writes queued requests as soon as they arrive. After `KEEPALIVE_INTERVAL`
/// without anything to send it writes a ping instead, which keeps the
/// daemon's dead-connection timer fed and lets this side notice a hung
/// daemon through the missing pong.
async fn write_outgoing<S>(
    mut sink: SplitSink<WebSocketStream<S>, Message>,
    mut outgoing: mpsc::UnboundedReceiver<Outgoing>,
    binary_frames: bool,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let next = match tokio::time::timeout(KEEPALIVE_INTERVAL, outgoing.recv()).await {
            Ok(Some(next)) => next,
            Ok(None) => return,
            Err(_) => {
                if sink.send(Message::Ping(Default::default())).await.is_err() {
                    return;
                }
                continue;
            }
        };
        let mut next = Some(next);
        let mut batched = 0;
        while let Some(item) = next.take() {
            match item {
                Outgoing::Message(message) => {
                    let Ok(frame) = outgoing_message(message, binary_frames) else {
                        continue;
                    };
                    if sink.feed(frame).await.is_err() {
                        return;
                    }
                    batched += 1;
                    if batched < MAX_MESSAGES_PER_FLUSH {
                        next = outgoing.try_recv().ok();
                    }
                }
                Outgoing::Shutdown => {
                    let _ = sink
                        .feed(Message::Text(
                            serde_json::to_string(&ClientMessage::Shutdown)
                                .unwrap_or_default()
                                .into(),
                        ))
                        .await;
                    let _ = sink.flush().await;
                    return;
                }
                Outgoing::Disconnect => {
                    let _ = sink.close().await;
                    return;
                }
            }
        }
        if sink.flush().await.is_err() {
            return;
        }
    }
}

/// Delivers daemon messages until the daemon closes the connection, says it
/// is shutting down, or goes silent for `KEEPALIVE_TIMEOUT` â the daemon
/// pings when it has nothing to say, so silence means it is hung or gone.
async fn read_incoming<S>(mut stream: SplitStream<WebSocketStream<S>>, inner: &ClientInner)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let Ok(next) = tokio::time::timeout(KEEPALIVE_TIMEOUT, stream.next()).await else {
            return;
        };
        match next {
            Some(Ok(Message::Text(text))) => {
                let Ok(message) = serde_json::from_str::<ServerMessage>(text.as_str()) else {
                    continue;
                };
                if !deliver(message, inner) {
                    return;
                }
            }
            // Blob data with its bytes raw after a JSON header (#22).
            Some(Ok(Message::Binary(frame))) => {
                let Some((header, body)) = waku_protocol::binary_frame::split(&frame) else {
                    continue;
                };
                let Ok(mut message) = serde_json::from_slice::<ServerMessage>(header) else {
                    continue;
                };
                message.put_bulk(body.to_vec());
                if !deliver(message, inner) {
                    return;
                }
            }
            Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
            // Pings are answered by the WebSocket layer.
            Some(Ok(_)) => {}
        }
    }
}

/// Routes one daemon message. Returns `false` when the connection should end.
fn deliver(message: ServerMessage, inner: &ClientInner) -> bool {
    match message {
        ServerMessage::Response {
            request_id,
            outcome,
        } => {
            if let Some(pending) = inner.pending.lock().remove(&request_id) {
                let result = match outcome {
                    ResponseOutcome::Ok { payload } => Ok(payload),
                    ResponseOutcome::Error { error } => Err(error),
                };
                let _ = pending.send(result);
            }
        }
        ServerMessage::Event(event) => {
            let should_deliver = {
                let mut sequences = inner.last_sequences.lock();
                let previous = sequences
                    .entry((event.session_id, event.runtime_id))
                    .or_insert(LastSequence {
                        epoch: event.epoch,
                        sequence: 0,
                    });
                if previous.epoch == event.epoch && event.sequence <= previous.sequence {
                    false
                } else {
                    previous.epoch = event.epoch;
                    previous.sequence = event.sequence;
                    true
                }
            };
            if should_deliver {
                let key = (event.session_id, event.runtime_id);
                let sessions = inner.sessions.lock();
                if let Some(events) = sessions.get(&key) {
                    let _ = events.send(event);
                } else if !matches!(event.event, WireDriverEvent::TerminalOutput(_)) {
                    // Every client receives every terminal's output. Nobody
                    // here is attached to this one, and an attach replays the
                    // daemon's retained tail, so buffering it would only hold
                    // up to 4,096 chunks of someone else's terminal.
                    let mut pending = inner.pending_events.lock();
                    let buffered = pending.entry(key).or_default();
                    buffered.push_back(event);
                    while buffered.len() > MAX_BUFFERED_EVENTS_PER_RUNTIME {
                        buffered.pop_front();
                    }
                }
            }
        }
        ServerMessage::TaskStateChanged { revision } => {
            inner
                .task_state_subscribers
                .lock()
                .retain(|subscriber| subscriber.send(revision).is_ok());
        }
        ServerMessage::ShuttingDown => return false,
        ServerMessage::Hello { .. } | ServerMessage::Rejected { .. } => {}
    }
    true
}

fn disconnected(inner: &ClientInner) {
    inner.disconnected.store(true, Ordering::Release);
    let pending = std::mem::take(&mut *inner.pending.lock());
    for (_, response) in pending {
        let _ = response.send(Err(RpcError {
            message: "Waku daemon disconnected".into(),
        }));
    }
    // Closing the desktop transport is not evidence that a daemon-owned
    // provider exited. Drop the subscription senders so runtime adapters can
    // hand off to a replacement client and ask the daemon whether the same
    // runtime still exists. Real provider exits arrive through the replayable
    // `processExited` event emitted by the daemon.
    drop(std::mem::take(&mut *inner.sessions.lock()));
    inner.task_state_subscribers.lock().clear();
    let callbacks = std::mem::take(&mut *inner.disconnect_callbacks.lock());
    for callback in callbacks {
        callback();
    }
}

/// A request as a WebSocket message: binary with its bulk bytes raw when
/// the daemon agreed to binary frames and it has any, JSON text otherwise.
fn outgoing_message(mut message: ClientMessage, binary_frames: bool) -> anyhow::Result<Message> {
    if binary_frames && let Some(body) = message.take_bulk() {
        let header = serde_json::to_vec(&message)?;
        return Ok(Message::Binary(
            waku_protocol::binary_frame::encode(&header, &body).into(),
        ));
    }
    text_message(&message)
}

fn text_message<T: serde::Serialize>(value: &T) -> anyhow::Result<Message> {
    Ok(Message::Text(serde_json::to_string(value)?.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daemon_endpoint_accepts_addresses_and_secure_urls() {
        assert_eq!(
            daemon_url("127.0.0.1:4312").unwrap(),
            "ws://127.0.0.1:4312/v1"
        );
        assert_eq!(
            daemon_url("wss://waku.example.test/old?ignored=1").unwrap(),
            "wss://waku.example.test/v1"
        );
    }

    #[test]
    fn daemon_endpoint_accepts_local_sockets_and_pipes() {
        assert_eq!(
            Endpoint::parse("unix:/tmp/waku/daemon.sock").unwrap(),
            Endpoint::Unix("/tmp/waku/daemon.sock".into())
        );
        assert!(Endpoint::parse("unix:").is_err());
        assert_eq!(
            Endpoint::parse(r"pipe:\\.\pipe\waku-fork-me-daemon").unwrap(),
            Endpoint::Pipe(r"\\.\pipe\waku-fork-me-daemon".into())
        );
        assert_eq!(
            Endpoint::parse(r"\\.\pipe\waku-fork-me-daemon").unwrap(),
            Endpoint::Pipe(r"\\.\pipe\waku-fork-me-daemon".into())
        );
        assert_eq!(
            Endpoint::parse("127.0.0.1:4312").unwrap(),
            Endpoint::WebSocket("ws://127.0.0.1:4312/v1".into())
        );
        let local = local_endpoint_address("/tmp/waku/daemon.sock");
        assert!(Endpoint::parse(&local).is_ok());
    }
}
