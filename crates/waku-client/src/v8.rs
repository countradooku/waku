//! The protocol v8 client (RFC 0003 §2.7), beside the legacy [`DaemonClient`]
//! until the desktop switches over (#53).
//!
//! Like the legacy client, a [`V8Client`] owns one thread running a
//! current-thread tokio runtime: a reader blocked on the socket and a writer
//! blocked on its queue, with keepalive as a timeout around the writer's
//! wait. Callers never see the runtime: calls block on crossbeam channels and
//! stream events arrive on crossbeam receivers.
//!
//! The local socket carries MessagePack frames after the `WAKU` preface;
//! `ws://` and `wss://` addresses use the `/v8` WebSocket with binary
//! MessagePack frames. A subscription that the daemon ends with `overflow` is
//! resubscribed from its last delivered cursor without the caller noticing
//! anything but a [`StreamEvent::Resumed`].
//!
//! [`DaemonClient`]: crate::DaemonClient

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use anyhow::{Context as _, anyhow, bail};
use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt as _, StreamExt as _};
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, BufWriter};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::{self, Message};
use uuid::Uuid;

use waku_protocol::v8::frame::{PrefaceCheck, check_preface};
use waku_protocol::v8::orchestration::{ShellItem, ThreadItem};
pub use waku_protocol::v8::*;

const CALL_TIMEOUT: Duration = Duration::from_secs(120);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_FRAMES_PER_FLUSH: usize = 64;

/// Why a call failed.
#[derive(Debug, Clone, PartialEq)]
pub enum CallError {
    /// The daemon's `reply.error`.
    Remote(Error),
    /// The connection closed before the reply.
    Disconnected,
    TimedOut,
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Remote(error) => error.fmt(f),
            Self::Disconnected => f.write_str("the daemon connection closed"),
            Self::TimedOut => f.write_str("the daemon did not reply in time"),
        }
    }
}

impl std::error::Error for CallError {}

/// The daemon refused the `hello`.
#[derive(Debug, Clone, PartialEq)]
pub struct RejectedError(pub Rejected);

impl std::fmt::Display for RejectedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the daemon rejected the connection ({}): {}",
            self.0.code, self.0.message
        )
    }
}

impl std::error::Error for RejectedError {}

/// One event of a subscription, in stream order.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamEvent {
    Item(StreamItem),
    /// DATA bytes; return credit with [`Subscription::consumed`].
    Data {
        bytes: Vec<u8>,
        fin: bool,
    },
    /// The daemon kicked the subscription for overflowing its budget and it
    /// was resubscribed from `after`.
    Resumed {
        after: Option<StreamCursor>,
    },
    /// The subscription is over.
    End(End),
}

#[derive(Clone, Debug, Default)]
pub struct SubscribeOptions {
    pub after: Option<StreamCursor>,
    pub window: Option<u32>,
}

enum Outgoing {
    Message(Vec<u8>),
    /// DATA: upload bytes or terminal input.
    Data {
        channel: u32,
        fin: bool,
        body: Vec<u8>,
    },
    Close,
}

struct SubscriptionState {
    stream: &'static str,
    spec: StreamSpec,
    window: Option<u32>,
    cursor: Option<StreamCursor>,
    /// DATA bytes still to come that replay retained output (a terminal's
    /// `attached`/`reset`); they do not advance the offset cursor.
    replay_left: u64,
    events: Sender<StreamEvent>,
    channel: Arc<AtomicU32>,
}

#[derive(Default)]
struct Routing {
    next_call: u32,
    next_channel: u32,
    next_nonce: u64,
    calls: HashMap<u32, (&'static str, Sender<Result<MethodResult, CallError>>)>,
    subscriptions: HashMap<u32, SubscriptionState>,
    /// Channels of uploads in progress; they share the channel space.
    uploads: std::collections::HashSet<u32>,
}

impl DecodeContext for Routing {
    fn call_method(&self, id: u32) -> Option<&'static str> {
        self.calls.get(&id).map(|(method, _)| *method)
    }

    fn channel_stream(&self, channel: u32) -> Option<&'static str> {
        self.subscriptions.get(&channel).map(|state| state.stream)
    }
}

impl Routing {
    fn allocate_call(&mut self) -> u32 {
        loop {
            self.next_call = self.next_call.wrapping_add(1).max(1);
            if !self.calls.contains_key(&self.next_call) {
                return self.next_call;
            }
        }
    }

    fn allocate_channel(&mut self) -> u32 {
        loop {
            self.next_channel = self.next_channel.wrapping_add(1).max(1);
            if !self.subscriptions.contains_key(&self.next_channel)
                && !self.uploads.contains(&self.next_channel)
            {
                return self.next_channel;
            }
        }
    }
}

struct Inner {
    outgoing: mpsc::UnboundedSender<Outgoing>,
    routing: Mutex<Routing>,
    welcome: Welcome,
    disconnected: AtomicBool,
    disconnect_callbacks: Mutex<Vec<Box<dyn FnOnce() + Send>>>,
}

impl Inner {
    fn send(&self, message: &ClientMessage) {
        match codec::to_vec(Encoding::MessagePack, message) {
            Ok(bytes) => {
                let _ = self.outgoing.send(Outgoing::Message(bytes));
            }
            Err(error) => eprintln!("waku v8 client could not encode a message: {error}"),
        }
    }
}

/// A protocol v8 connection to the daemon.
#[derive(Clone)]
pub struct V8Client {
    inner: Arc<Inner>,
}

/// Where a [`V8Client`] connects.
#[derive(Debug, PartialEq)]
enum Endpoint {
    WebSocket(String),
    Unix(std::path::PathBuf),
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
        let normalized = if address.starts_with("ws://") || address.starts_with("wss://") {
            address.to_owned()
        } else {
            format!("ws://{address}")
        };
        let mut url = url::Url::parse(&normalized).context("Waku daemon address is invalid")?;
        url.set_path(WEBSOCKET_PATH);
        url.set_query(None);
        url.set_fragment(None);
        Ok(Self::WebSocket(url.into()))
    }
}

impl V8Client {
    /// Connect as the desktop with the daemon's local token. `address` is
    /// `unix:<path>`, `pipe:<name>`, `host:port`, `ws://…` or `wss://…`.
    pub fn connect(address: &str, token: impl Into<String>) -> anyhow::Result<Self> {
        Self::connect_as(
            address,
            Credential::Local {
                token: token.into(),
            },
            ClientInfo {
                kind: ClientKind::Desktop,
                app_version: env!("CARGO_PKG_VERSION").into(),
                os: std::env::consts::OS.into(),
                device_label: None,
                instance_id: Uuid::new_v4(),
            },
        )
    }

    /// Connect with an explicit credential and client description. A refused
    /// `hello` fails with a [`RejectedError`].
    pub fn connect_as(
        address: &str,
        credential: Credential,
        client: ClientInfo,
    ) -> anyhow::Result<Self> {
        let endpoint = Endpoint::parse(address)?;
        let hello = ClientMessage::Hello(Hello {
            protocol: ProtocolVersion::CURRENT,
            client,
            credential,
            compression: Vec::new(),
            features: Vec::new(),
            max_message_bytes: Some(MAX_MESSAGE_BYTES),
        });
        let (ready_tx, ready_rx) = bounded::<anyhow::Result<V8Client>>(1);
        std::thread::Builder::new()
            .name("waku-daemon-v8-client".into())
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
                runtime.block_on(connect_and_run(endpoint, hello, ready_tx));
            })
            .context("could not start the daemon client thread")?;
        ready_rx
            .recv()
            .map_err(|_| anyhow!("the daemon client thread stopped"))?
    }

    pub fn welcome(&self) -> &Welcome {
        &self.inner.welcome
    }

    pub fn is_disconnected(&self) -> bool {
        self.inner.disconnected.load(Ordering::Acquire)
    }

    /// Run `callback` once when the connection closes (at once if it has).
    pub fn on_disconnect(&self, callback: impl FnOnce() + Send + 'static) {
        let mut callbacks = self.inner.disconnect_callbacks.lock();
        if self.is_disconnected() {
            drop(callbacks);
            callback();
            return;
        }
        callbacks.push(Box::new(callback));
    }

    /// Call `M` and wait for its reply.
    pub fn call<M: MethodDef>(&self, params: M::Params) -> Result<M::Result, CallError> {
        self.call_timeout::<M>(params, CALL_TIMEOUT)
    }

    pub fn call_timeout<M: MethodDef>(
        &self,
        params: M::Params,
        timeout: Duration,
    ) -> Result<M::Result, CallError> {
        let (id, reply) = self.start_call::<M>(params)?;
        self.wait_call::<M>(id, reply, timeout)
    }

    /// Send a call `M` without waiting: the reply arrives on
    /// [`PendingCall::receiver`], so one thread can select over many calls
    /// and streams at once. A connection that closes first answers
    /// [`CallError::Disconnected`].
    pub fn start<M: MethodDef>(&self, params: M::Params) -> Result<PendingCall<M>, CallError> {
        let (id, reply) = self.start_call::<M>(params)?;
        Ok(PendingCall {
            id,
            reply,
            _method: std::marker::PhantomData,
        })
    }

    fn start_call<M: MethodDef>(
        &self,
        params: M::Params,
    ) -> Result<(u32, Receiver<Result<MethodResult, CallError>>), CallError> {
        if self.is_disconnected() {
            return Err(CallError::Disconnected);
        }
        let (reply_tx, reply_rx) = bounded(1);
        let id = {
            let mut routing = self.inner.routing.lock();
            let id = routing.allocate_call();
            routing.calls.insert(id, (M::NAME, reply_tx));
            id
        };
        self.inner.send(&ClientMessage::Call {
            id,
            method: M::wrap(params),
        });
        Ok((id, reply_rx))
    }

    fn wait_call<M: MethodDef>(
        &self,
        id: u32,
        reply_rx: Receiver<Result<MethodResult, CallError>>,
        timeout: Duration,
    ) -> Result<M::Result, CallError> {
        match reply_rx.recv_timeout(timeout) {
            Ok(Ok(result)) => M::unwrap(result).ok_or(CallError::Disconnected),
            Ok(Err(error)) => Err(error),
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                self.inner.routing.lock().calls.remove(&id);
                self.inner.send(&ClientMessage::Cancel { id });
                Err(CallError::TimedOut)
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => Err(CallError::Disconnected),
        }
    }

    /// Upload `bytes` as a blob (`blobs.put`, RFC 0003 §5.6): the call, then
    /// the bytes as DATA on a fresh channel, `FIN` on the last chunk. Pass
    /// `sha256` (lowercase hex) to let the daemon skip a blob it holds.
    pub fn put_blob(
        &self,
        bytes: &[u8],
        mime: &str,
        name: Option<&str>,
        sha256: Option<&str>,
    ) -> Result<BlobPut, CallError> {
        let channel = {
            let mut routing = self.inner.routing.lock();
            let channel = routing.allocate_channel();
            routing.uploads.insert(channel);
            channel
        };
        let started = self.start_call::<method::BlobsPut>(BlobsPutParams {
            channel,
            size: bytes.len() as u64,
            mime: mime.to_owned(),
            name: name.map(str::to_owned),
            sha256: sha256.map(str::to_owned),
        });
        let outcome = started.and_then(|(id, reply)| {
            let chunk = self.inner.welcome.limits.max_data_chunk_bytes.max(1) as usize;
            let mut chunks = bytes.chunks(chunk).peekable();
            if chunks.peek().is_none() {
                self.send_data(channel, true, Vec::new());
            }
            while let Some(body) = chunks.next() {
                // A deduplicated or refused upload is answered early; stop
                // sending and close the transfer.
                if !reply.is_empty() {
                    self.send_data(channel, true, Vec::new());
                    break;
                }
                self.send_data(channel, chunks.peek().is_none(), body.to_vec());
            }
            self.wait_call::<method::BlobsPut>(id, reply, CALL_TIMEOUT)
        });
        self.inner.routing.lock().uploads.remove(&channel);
        outcome
    }

    /// Download a blob, or with `path` a file inside an attachment, through
    /// the `blobDownload` stream; returns its `meta` and bytes.
    pub fn download_blob(
        &self,
        reference: &str,
        path: Option<&str>,
    ) -> Result<(BlobDownloadItem, Vec<u8>), CallError> {
        let subscription = self.subscribe::<stream::BlobDownload>(
            BlobDownloadSpec {
                reference: reference.to_owned(),
                offset: None,
                path: path.map(str::to_owned),
            },
            SubscribeOptions::default(),
        );
        let mut meta = None;
        let mut bytes = Vec::new();
        loop {
            let event = subscription
                .events()
                .recv_timeout(CALL_TIMEOUT)
                .map_err(|_| CallError::TimedOut)?;
            match event {
                StreamEvent::Item(StreamItem::BlobDownload(item)) => meta = Some(item),
                StreamEvent::Data { bytes: body, .. } => {
                    subscription.consumed(u32::try_from(body.len()).unwrap_or(u32::MAX));
                    bytes.extend_from_slice(&body);
                }
                StreamEvent::End(end) => {
                    return match end.reason {
                        EndReason::Completed => meta
                            .map(|meta| (meta, bytes))
                            .ok_or(CallError::Disconnected),
                        EndReason::Error { error } => Err(CallError::Remote(error)),
                        other => Err(CallError::Remote(Error::new(
                            ErrorCode::NotFound,
                            format!("the download ended: {other:?}"),
                        ))),
                    };
                }
                _ => {}
            }
        }
    }

    fn send_data(&self, channel: u32, fin: bool, body: Vec<u8>) {
        let _ = self
            .inner
            .outgoing
            .send(Outgoing::Data { channel, fin, body });
    }

    /// Subscribe to `S`. Events arrive on [`Subscription::events`].
    pub fn subscribe<S: StreamDef>(
        &self,
        spec: S::Spec,
        options: SubscribeOptions,
    ) -> Subscription {
        let (events_tx, events) = unbounded();
        let channel = Arc::new(AtomicU32::new(0));
        let spec = S::wrap(spec);
        let id = {
            let mut routing = self.inner.routing.lock();
            let id = routing.allocate_channel();
            channel.store(id, Ordering::Release);
            routing.subscriptions.insert(
                id,
                SubscriptionState {
                    stream: S::NAME,
                    spec: spec.clone(),
                    window: options.window,
                    cursor: options.after.clone(),
                    replay_left: 0,
                    events: events_tx,
                    channel: channel.clone(),
                },
            );
            id
        };
        self.inner.send(&ClientMessage::Subscribe(Subscribe {
            channel: id,
            stream: spec,
            after: options.after,
            window: options.window,
        }));
        Subscription {
            client: self.clone(),
            channel,
            events,
        }
    }

    /// Say goodbye and close the connection.
    pub fn close(&self) {
        self.inner.send(&ClientMessage::Goodbye {
            code: GoodbyeCode::ShuttingDown,
            message: "client closed".into(),
        });
        let _ = self.inner.outgoing.send(Outgoing::Close);
    }
}

/// A call sent with [`V8Client::start`] whose reply has not been taken.
pub struct PendingCall<M: MethodDef> {
    id: u32,
    reply: Receiver<Result<MethodResult, CallError>>,
    _method: std::marker::PhantomData<fn() -> M>,
}

impl<M: MethodDef> PendingCall<M> {
    /// The call id on its connection.
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Ready once the reply (or the disconnect) arrived; pass what it
    /// yields to [`Self::decode`]. For `crossbeam_channel::Select`.
    pub fn receiver(&self) -> &Receiver<Result<MethodResult, CallError>> {
        &self.reply
    }

    /// The typed result of a raw reply from [`Self::receiver`].
    pub fn decode(
        raw: Result<Result<MethodResult, CallError>, crossbeam_channel::RecvError>,
    ) -> Result<M::Result, CallError> {
        match raw {
            Ok(Ok(result)) => M::unwrap(result).ok_or(CallError::Disconnected),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(CallError::Disconnected),
        }
    }

    /// The reply, if it has arrived.
    pub fn try_take(&self) -> Option<Result<M::Result, CallError>> {
        match self.reply.try_recv() {
            Ok(raw) => Some(Self::decode(Ok(raw))),
            Err(crossbeam_channel::TryRecvError::Empty) => None,
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                Some(Err(CallError::Disconnected))
            }
        }
    }
}

/// A live subscription. Dropping it does not unsubscribe; call
/// [`Self::unsubscribe`].
pub struct Subscription {
    client: V8Client,
    channel: Arc<AtomicU32>,
    events: Receiver<StreamEvent>,
}

impl Subscription {
    pub fn events(&self) -> &Receiver<StreamEvent> {
        &self.events
    }

    /// The channel of the current subscription (it changes on resume).
    pub fn channel(&self) -> u32 {
        self.channel.load(Ordering::Acquire)
    }

    /// Return DATA credit for bytes the caller has consumed.
    pub fn consumed(&self, bytes: u32) {
        self.client.inner.send(&ClientMessage::Credit {
            channel: self.channel(),
            bytes,
        });
    }

    pub fn unsubscribe(&self) {
        self.client.inner.send(&ClientMessage::Unsubscribe {
            channel: self.channel(),
        });
    }

    /// Send DATA on this subscription's channel: input of an interactive
    /// terminal.
    pub fn send_data(&self, bytes: Vec<u8>) {
        self.client.send_data(self.channel(), false, bytes);
    }
}

type Ready = Sender<anyhow::Result<V8Client>>;

async fn connect_and_run(endpoint: Endpoint, hello: ClientMessage, ready: Ready) {
    match endpoint {
        Endpoint::WebSocket(url) => {
            let connect = async {
                let mut request = url.as_str().into_client_request()?;
                request.headers_mut().insert(
                    "Sec-WebSocket-Protocol",
                    HeaderValue::from_static(WEBSOCKET_SUBPROTOCOL),
                );
                let (socket, _) = tokio_tungstenite::connect_async(request)
                    .await
                    .context("could not connect to the Waku daemon")?;
                anyhow::Ok(websocket(socket))
            };
            run(connect, hello, ready).await;
        }
        #[cfg(unix)]
        Endpoint::Unix(path) => {
            let connect = async {
                let stream = tokio::net::UnixStream::connect(&path)
                    .await
                    .with_context(|| format!("could not connect to {}", path.display()))?;
                local(stream).await
            };
            run(connect, hello, ready).await;
        }
        #[cfg(windows)]
        Endpoint::Pipe(name) => {
            let connect = async {
                let stream = tokio::net::windows::named_pipe::ClientOptions::new()
                    .open(&name)
                    .with_context(|| format!("could not connect to {name}"))?;
                local(stream).await
            };
            run(connect, hello, ready).await;
        }
        #[allow(unreachable_patterns)]
        other => {
            let _ = ready.send(Err(anyhow!("{other:?} is not supported on this platform")));
        }
    }
}

/// Exchange prefaces on a local stream.
async fn local<S>(mut stream: S) -> anyhow::Result<(Reader<S>, Writer<S>)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream.write_all(&PREFACE).await?;
    stream.flush().await?;
    let mut preface = [0; 8];
    stream
        .read_exact(&mut preface)
        .await
        .context("the daemon closed the connection before its preface")?;
    match check_preface(&preface) {
        PrefaceCheck::Match => {}
        PrefaceCheck::OtherMajor(major) => {
            return Err(RejectedError(Rejected {
                code: RejectCode::VersionUnsupported,
                message: format!(
                    "the daemon speaks protocol {major}, this client {PROTOCOL_MAJOR}"
                ),
                retry_after_ms: None,
            })
            .into());
        }
        PrefaceCheck::NotWaku => bail!("the daemon socket did not answer with a Waku preface"),
    }
    let (read, write) = tokio::io::split(stream);
    Ok((
        Reader::Local {
            read,
            decoder: FrameDecoder::new(frame::FrameLimits::default()),
            chunk: vec![0; 64 * 1024].into_boxed_slice(),
        },
        Writer::Local(BufWriter::new(write)),
    ))
}

fn websocket<S>(socket: WebSocketStream<S>) -> (Reader<S>, Writer<S>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (sink, stream) = socket.split();
    (Reader::WebSocket(stream), Writer::WebSocket(sink))
}

enum Reader<S> {
    Local {
        read: ReadHalf<S>,
        decoder: FrameDecoder,
        chunk: Box<[u8]>,
    },
    WebSocket(SplitStream<WebSocketStream<S>>),
}

enum Writer<S> {
    Local(BufWriter<WriteHalf<S>>),
    WebSocket(SplitSink<WebSocketStream<S>, Message>),
}

impl<S: AsyncRead + AsyncWrite + Unpin> Reader<S> {
    async fn next(&mut self) -> anyhow::Result<Option<frame::Frame>> {
        match self {
            Self::Local {
                read,
                decoder,
                chunk,
            } => loop {
                if let Some(frame) = decoder.next_frame()? {
                    return Ok(Some(frame));
                }
                let count = read.read(chunk).await?;
                if count == 0 {
                    return Ok(None);
                }
                decoder.extend(&chunk[..count]);
            },
            Self::WebSocket(stream) => loop {
                match stream.next().await {
                    None
                    | Some(Err(
                        tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed,
                    )) => return Ok(None),
                    Some(Err(error)) => return Err(error.into()),
                    Some(Ok(Message::Binary(bytes))) => {
                        if let Some(frame) =
                            frame::parse_message(&bytes, &frame::FrameLimits::default())?
                        {
                            return Ok(Some(frame));
                        }
                    }
                    Some(Ok(Message::Close(_))) => return Ok(None),
                    Some(Ok(_)) => {}
                }
            },
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Writer<S> {
    async fn feed(&mut self, body: &[u8]) -> anyhow::Result<()> {
        match self {
            Self::Local(write) => {
                write
                    .write_all(&frame::FrameHeader::msg(body.len()).encode())
                    .await?;
                write.write_all(body).await?;
            }
            Self::WebSocket(sink) => {
                sink.feed(Message::Binary(frame::encode_msg(body).into()))
                    .await?
            }
        }
        Ok(())
    }

    async fn feed_data(&mut self, channel: u32, fin: bool, body: &[u8]) -> anyhow::Result<()> {
        match self {
            Self::Local(write) => {
                write
                    .write_all(&frame::FrameHeader::data(channel, body.len(), fin).encode())
                    .await?;
                write.write_all(body).await?;
            }
            Self::WebSocket(sink) => {
                sink.feed(Message::Binary(
                    frame::encode_data(channel, fin, body).into(),
                ))
                .await?
            }
        }
        Ok(())
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        match self {
            Self::Local(write) => write.flush().await?,
            Self::WebSocket(sink) => sink.flush().await?,
        }
        Ok(())
    }

    async fn close(&mut self) {
        match self {
            Self::Local(write) => {
                let _ = write.shutdown().await;
            }
            Self::WebSocket(sink) => {
                let _ = sink.close().await;
            }
        }
    }
}

async fn run<S>(
    connect: impl Future<Output = anyhow::Result<(Reader<S>, Writer<S>)>>,
    hello: ClientMessage,
    ready: Ready,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let handshake = async {
        let (mut reader, mut writer) = connect.await?;
        writer
            .feed(&codec::to_vec(Encoding::MessagePack, &hello)?)
            .await?;
        writer.flush().await?;
        loop {
            let Some(frame) = reader.next().await? else {
                bail!("the daemon closed the connection during the handshake");
            };
            let frame::Frame::Msg(body) = frame else {
                continue;
            };
            match codec::from_slice_seed(
                Encoding::MessagePack,
                &body,
                ServerMessageSeed(&Routing::default()),
            )? {
                ServerMessage::Welcome(welcome) => return Ok((reader, writer, welcome)),
                ServerMessage::Rejected(rejected) => return Err(RejectedError(rejected).into()),
                ServerMessage::Goodbye { code, message, .. } => {
                    bail!("the daemon closed the connection ({code}): {message}")
                }
                _ => {}
            }
        }
    };
    let (reader, writer, welcome) = match tokio::time::timeout(CONNECT_TIMEOUT, handshake).await {
        Ok(Ok(connected)) => connected,
        Ok(Err(error)) => {
            let _ = ready.send(Err(error));
            return;
        }
        Err(_) => {
            let _ = ready.send(Err(anyhow!("the daemon handshake timed out")));
            return;
        }
    };
    let (outgoing, outgoing_rx) = mpsc::unbounded_channel();
    let inner = Arc::new(Inner {
        outgoing,
        routing: Mutex::new(Routing::default()),
        welcome,
        disconnected: AtomicBool::new(false),
        disconnect_callbacks: Mutex::new(Vec::new()),
    });
    if ready
        .send(Ok(V8Client {
            inner: inner.clone(),
        }))
        .is_err()
    {
        return;
    }
    tokio::select! {
        () = read_frames(reader, &inner) => {}
        () = write_frames(writer, outgoing_rx, &inner) => {}
    }
    disconnected(&inner);
}

/// Drains the outgoing queue. Keepalive is a timeout around the wait: after
/// `pingIntervalMs` with nothing to send it sends a `ping`.
async fn write_frames<S>(
    mut writer: Writer<S>,
    mut outgoing: mpsc::UnboundedReceiver<Outgoing>,
    inner: &Inner,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let interval = Duration::from_millis(u64::from(inner.welcome.limits.ping_interval_ms));
    loop {
        let next = match tokio::time::timeout(interval, outgoing.recv()).await {
            Ok(Some(next)) => next,
            Ok(None) => return,
            Err(_) => {
                let nonce = {
                    let mut routing = inner.routing.lock();
                    routing.next_nonce += 1;
                    routing.next_nonce
                };
                let ping = codec::to_vec(Encoding::MessagePack, &ClientMessage::Ping { nonce })
                    .expect("a ping encodes");
                Outgoing::Message(ping)
            }
        };
        let mut next = Some(next);
        let mut written = 0;
        while let Some(item) = next.take() {
            match item {
                Outgoing::Message(bytes) => {
                    if writer.feed(&bytes).await.is_err() {
                        return;
                    }
                }
                Outgoing::Data { channel, fin, body } => {
                    if writer.feed_data(channel, fin, &body).await.is_err() {
                        return;
                    }
                }
                Outgoing::Close => {
                    let _ = writer.flush().await;
                    writer.close().await;
                    return;
                }
            }
            written += 1;
            if written < MAX_FRAMES_PER_FLUSH {
                next = outgoing.try_recv().ok();
            }
        }
        if writer.flush().await.is_err() {
            return;
        }
    }
}

/// Reads until the connection ends. Nothing at all for `pingTimeoutMs` means
/// the daemon is gone.
async fn read_frames<S>(mut reader: Reader<S>, inner: &Inner)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let timeout = Duration::from_millis(u64::from(inner.welcome.limits.ping_timeout_ms));
    loop {
        let frame = match tokio::time::timeout(timeout, reader.next()).await {
            Ok(Ok(Some(frame))) => frame,
            Ok(Ok(None) | Err(_)) | Err(_) => return,
        };
        match frame {
            frame::Frame::Data { channel, fin, body } => {
                if let Some(state) = inner.routing.lock().subscriptions.get_mut(&channel) {
                    advance_offset(state, body.len() as u64);
                    let _ = state.events.send(StreamEvent::Data { bytes: body, fin });
                }
            }
            frame::Frame::Msg(body) => {
                let message = {
                    let routing = inner.routing.lock();
                    codec::from_slice_seed(
                        Encoding::MessagePack,
                        &body,
                        ServerMessageSeed(&*routing),
                    )
                };
                match message {
                    Ok(message) => {
                        if !deliver(message, inner) {
                            return;
                        }
                    }
                    Err(error) => {
                        eprintln!("waku v8 client ignored an undecodable message: {error}");
                    }
                }
            }
        }
    }
}

/// DATA of an offset stream moves its cursor, past any replay first.
fn advance_offset(state: &mut SubscriptionState, bytes: u64) {
    let live = bytes.saturating_sub(state.replay_left);
    state.replay_left = state.replay_left.saturating_sub(bytes);
    if live > 0
        && let Some(StreamCursor::Offset { offset, .. }) = &mut state.cursor
    {
        *offset += live;
    }
}

/// The cursor an item advances its subscription to.
fn item_cursor(item: &StreamItem, previous: Option<&StreamCursor>) -> Option<StreamCursor> {
    match item {
        StreamItem::Drafts(DraftsItem::Snapshot { revision, .. })
        | StreamItem::Drafts(DraftsItem::Changed { revision, .. })
        | StreamItem::Providers(ProvidersItem::Snapshot { revision, .. })
        | StreamItem::Providers(ProvidersItem::StatusUpdated { revision, .. }) => {
            Some(StreamCursor::Revision {
                revision: *revision,
            })
        }
        StreamItem::Drafts(DraftsItem::Synchronized { cursor })
        | StreamItem::Providers(ProvidersItem::Synchronized { cursor }) => Some(cursor.clone()),
        StreamItem::Terminal(
            TerminalItem::Attached {
                generation, offset, ..
            }
            | TerminalItem::Reset {
                generation, offset, ..
            },
        ) => Some(StreamCursor::Offset {
            generation: *generation,
            offset: *offset,
        }),
        StreamItem::Settings(SettingsItem::Snapshot { revision, .. })
        | StreamItem::Settings(SettingsItem::Updated { revision, .. }) => {
            Some(StreamCursor::Revision {
                revision: *revision,
            })
        }
        StreamItem::Settings(SettingsItem::Synchronized { cursor }) => Some(cursor.clone()),
        // Shell and thread items carry the sequence of the event store whose
        // id the snapshot or `synchronized` cursor named.
        StreamItem::Thread(ThreadItem::Snapshot(snapshot)) => Some(snapshot.cursor.clone()),
        StreamItem::Thread(ThreadItem::Synchronized { cursor })
        | StreamItem::Shell(ShellItem::Synchronized { cursor })
        | StreamItem::Shell(ShellItem::Snapshot { cursor, .. }) => Some(cursor.clone()),
        StreamItem::Thread(ThreadItem::Event { seq, .. })
        | StreamItem::Shell(
            ShellItem::ProjectUpdated { seq, .. }
            | ShellItem::ProjectRemoved { seq, .. }
            | ShellItem::ThreadUpdated { seq, .. }
            | ShellItem::ThreadRemoved { seq, .. },
        ) => {
            let StreamCursor::Sequence { store, .. } = previous? else {
                return None;
            };
            Some(StreamCursor::Sequence {
                store: *store,
                seq: *seq,
            })
        }
        _ => None,
    }
}

fn deliver(message: ServerMessage, inner: &Inner) -> bool {
    match message {
        ServerMessage::Reply(reply) => {
            let Some((_, sender)) = inner.routing.lock().calls.remove(&reply.id) else {
                return true;
            };
            let outcome = match (reply.ok, reply.error) {
                (_, Some(error)) => Err(CallError::Remote(error)),
                (Some(result), None) => Ok(result),
                (None, None) => Err(CallError::Remote(Error::new(
                    ErrorCode::Internal,
                    "a reply without a result",
                ))),
            };
            let _ = sender.send(outcome);
        }
        ServerMessage::Items { channel, items } => {
            let mut routing = inner.routing.lock();
            if let Some(state) = routing.subscriptions.get_mut(&channel) {
                for item in items {
                    if let Some(cursor) = item_cursor(&item, state.cursor.as_ref()) {
                        state.cursor = Some(cursor);
                    }
                    if let StreamItem::Terminal(
                        TerminalItem::Attached { replay, .. } | TerminalItem::Reset { replay, .. },
                    ) = &item
                    {
                        state.replay_left = *replay;
                    }
                    let _ = state.events.send(StreamEvent::Item(item));
                }
            }
        }
        ServerMessage::End(end) => {
            let mut routing = inner.routing.lock();
            let Some(mut state) = routing.subscriptions.remove(&end.channel) else {
                return true;
            };
            if end.reason == EndReason::Overflow {
                // Kick-and-resume: only this subscription overflowed.
                let after = end.resume_after.clone().or(state.cursor.take());
                let channel = routing.allocate_channel();
                state.channel.store(channel, Ordering::Release);
                state.cursor = after.clone();
                let _ = state.events.send(StreamEvent::Resumed {
                    after: after.clone(),
                });
                let subscribe = ClientMessage::Subscribe(Subscribe {
                    channel,
                    stream: state.spec.clone(),
                    after,
                    window: state.window,
                });
                routing.subscriptions.insert(channel, state);
                drop(routing);
                inner.send(&subscribe);
            } else {
                let _ = state.events.send(StreamEvent::End(end));
            }
        }
        ServerMessage::Ping { nonce } => inner.send(&ClientMessage::Pong { nonce }),
        ServerMessage::Goodbye { .. } => return false,
        ServerMessage::Pong { .. }
        | ServerMessage::Notice { .. }
        | ServerMessage::Welcome(_)
        | ServerMessage::Rejected(_)
        | ServerMessage::Unknown => {}
    }
    true
}

fn disconnected(inner: &Inner) {
    inner.disconnected.store(true, Ordering::Release);
    let mut routing = inner.routing.lock();
    for (_, (_, sender)) in routing.calls.drain() {
        let _ = sender.send(Err(CallError::Disconnected));
    }
    routing.subscriptions.clear();
    drop(routing);
    let callbacks = std::mem::take(&mut *inner.disconnect_callbacks.lock());
    for callback in callbacks {
        callback();
    }
}

#[cfg(all(test, unix))]
mod tests;
