//! The daemon's listeners and connection admission.
//!
//! The transport is evented (#18). One small multi-thread tokio runtime owns
//! the accept loops and every connection's tasks; nothing polls. Backend
//! calls are synchronous (git, filesystem, provider probes) and run on that
//! runtime's blocking pool, which reuses a bounded set of threads instead of
//! spawning one per call.
//!
//! Every connection speaks protocol v8 (RFC 0003; decision D6 removed the
//! legacy protocol): on TCP as a WebSocket upgrade to `/v8` with the
//! `waku.v8` subprotocol, on the local socket with the 8-byte `WAKU`
//! preface. Anything else is refused before it is read further.

use std::collections::HashSet;
use std::io;
use std::net::TcpListener;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use anyhow::{Context as _, bail};
use parking_lot::{Condvar, Mutex};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, ReadBuf};
use tokio::sync::Notify;
use tokio_tungstenite::tungstenite::handshake::server::{
    ErrorResponse, Request as HandshakeRequest, Response as HandshakeResponse,
};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::http::header::{HeaderValue, ORIGIN, SEC_WEBSOCKET_PROTOCOL};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use uuid::Uuid;

pub use crate::backend::Backend;
use crate::local_socket::LocalListener;

/// The WebSocket upgrade and the client's `hello` must both arrive within
/// this.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Bytes a TCP connection may send before it has authenticated: the HTTP
/// upgrade request plus a v8 `hello`. Anything beyond that is refused unread,
/// so an unauthenticated peer cannot make the daemon buffer a large message.
const MAX_PRE_AUTH_BYTES: usize = waku_protocol::v8::HANDSHAKE_MESSAGE_BYTES as usize + 16 * 1024;
/// The WebSocket layer's own message cap. It sits above every v8 limit, so
/// an oversized v8 message is answered with v8's own error rather than a
/// dropped socket.
const MAX_WEBSOCKET_MESSAGE_BYTES: usize = 48 * 1024 * 1024;
const MAX_CONNECTIONS: usize = 64;
const NATIVE_CLIENT_HEADER: &str = "x-waku-client";
const NATIVE_CLIENT_HEADER_VALUE: &str = "native";
/// Async workers only move frames between sockets and queues; two keep one
/// connection's burst from delaying another's reply.
const TRANSPORT_WORKERS: usize = 2;
/// Cap on threads running blocking backend work at once (git, filesystem,
/// SQLite, provider probes). Further calls queue for a free thread.
const MAX_BLOCKING_THREADS: usize = 64;
/// An idle blocking thread exits after this, so a burst of calls does not
/// leave a crowd of parked threads behind.
const BLOCKING_THREAD_KEEP_ALIVE: Duration = Duration::from_secs(30);
/// How long [`serve`] waits for in-flight work when it stops.
const RUNTIME_SHUTDOWN_GRACE: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, Default)]
pub struct ServerOptions {
    /// Browser WebSocket handshakes carry an Origin header. Most native
    /// clients do not; a native client that does identifies itself with
    /// `x-waku-client`. An empty set therefore still permits native clients
    /// only.
    pub allowed_origins: HashSet<String>,
    /// Only a daemon owned by the desktop process accepts
    /// `daemon.shutdown`. Service-managed daemons keep running when an
    /// authenticated client disconnects.
    pub allow_shutdown: bool,
    /// The event store behind protocol v8's orchestration methods and
    /// streams. The daemon always serves one ([`crate::daemon_host`]); a
    /// server without it answers them `unsupported`.
    pub orchestration: Option<Arc<crate::Orchestration>>,
    /// This daemon's persistent environment id (RFC 0003 `EnvironmentInfo`);
    /// a per-process id when absent.
    pub environment_id: Option<Uuid>,
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

/// Everything a connection task needs from the server.
pub(crate) struct ServerContext {
    pub(crate) shutdown: ShutdownSignal,
    allowed_origins: HashSet<String>,
    active_connections: Arc<AtomicUsize>,
    v8: Arc<crate::v8::V8Context>,
}

impl ServerContext {
    fn spawn_tcp_connection<S>(self: &Arc<Self>, stream: S)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let Some(permit) = ConnectionPermit::acquire(&self.active_connections) else {
            return;
        };
        let context = self.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(error) = handle_websocket(stream, &context).await {
                eprintln!("waku-daemon connection ended: {error:#}");
            }
        });
    }

    /// A connection on the local socket. A v8 client opens with the `WAKU`
    /// preface; anything else is closed.
    pub(crate) fn spawn_local_connection<S>(self: &Arc<Self>, mut stream: S)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let Some(permit) = ConnectionPermit::acquire(&self.active_connections) else {
            return;
        };
        let context = self.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let mut magic = [0; 4];
            let read = tokio::time::timeout(HANDSHAKE_TIMEOUT, stream.read_exact(&mut magic)).await;
            if matches!(read, Ok(Ok(_))) && magic[..] == waku_protocol::v8::PREFACE[..4] {
                crate::v8::serve_local(stream, magic, context.v8.clone()).await;
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
    let v8 = Arc::new(
        crate::v8::V8Context::new(
            token,
            backend.clone(),
            crate::v8::SettingsBus::default(),
            shutdown.clone(),
        )
        .with_environment_id(options.environment_id)
        .with_orchestration(options.orchestration.clone())
        .with_allow_shutdown(options.allow_shutdown),
    );
    let context = Arc::new(ServerContext {
        shutdown,
        allowed_origins: options.allowed_origins,
        active_connections: Arc::new(AtomicUsize::new(0)),
        v8,
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
                context.spawn_tcp_connection(stream);
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
        // the daemon (and every live thread) up.
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

/// Upgrade a TCP connection to the `/v8` WebSocket and serve it.
async fn handle_websocket<S>(stream: S, context: &ServerContext) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let budget = Arc::new(AtomicUsize::new(MAX_PRE_AUTH_BYTES));
    let stream = PreAuthLimit {
        inner: stream,
        remaining: budget.clone(),
    };
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_WEBSOCKET_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_WEBSOCKET_MESSAGE_BYTES));
    let allowed_origins = context.allowed_origins.clone();
    let socket = tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        tokio_tungstenite::accept_hdr_async_with_config(
            stream,
            move |request: &HandshakeRequest, response: HandshakeResponse| {
                validate_handshake(request, response, &allowed_origins)
            },
            Some(config),
        ),
    )
    .await
    .context("daemon handshake timed out")?
    .context("WebSocket handshake failed")?;
    // v8 runs its own handshake; the pre-auth cap lifts once its hello
    // checks out.
    crate::v8::serve_websocket(socket, context.v8.clone(), move || {
        budget.store(usize::MAX, Ordering::Release);
    })
    .await;
    Ok(())
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
    mut response: HandshakeResponse,
    allowed_origins: &HashSet<String>,
) -> Result<HandshakeResponse, ErrorResponse> {
    if request.uri().path() != waku_protocol::v8::WEBSOCKET_PATH {
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
    // RFC 0003 §1.4: a v8 upgrade must offer the `waku.v8` subprotocol.
    let offered = request
        .headers()
        .get_all(SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|protocol| protocol.trim() == waku_protocol::v8::WEBSOCKET_SUBPROTOCOL);
    if !offered {
        return Err(handshake_error(
            StatusCode::UPGRADE_REQUIRED,
            "the waku.v8 subprotocol is required",
        ));
    }
    response.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static(waku_protocol::v8::WEBSOCKET_SUBPROTOCOL),
    );
    Ok(response)
}

fn handshake_error(status: StatusCode, message: &str) -> ErrorResponse {
    tokio_tungstenite::tungstenite::http::Response::builder()
        .status(status)
        .body(Some(message.to_owned()))
        .expect("static WebSocket rejection is valid")
}

#[cfg(test)]
mod tests {
    use crossbeam_channel::bounded;
    use waku_client::V8Client;

    use super::*;
    use crate::settings::DaemonSettings;

    /// Answers `settings.get` and nothing else.
    struct SettingsBackend;

    impl Backend for SettingsBackend {
        fn settings(&self) -> anyhow::Result<DaemonSettings> {
            Ok(DaemonSettings::default())
        }
    }

    fn v8_request(path: &str) -> tokio_tungstenite::tungstenite::http::request::Builder {
        HandshakeRequest::builder().uri(path).header(
            SEC_WEBSOCKET_PROTOCOL,
            waku_protocol::v8::WEBSOCKET_SUBPROTOCOL,
        )
    }

    #[test]
    fn only_the_v8_path_with_its_subprotocol_is_served() {
        for path in ["/", "/v7", "/v9", "/socket"] {
            let request = v8_request(path).body(()).unwrap();
            assert_eq!(
                validate_handshake(&request, HandshakeResponse::new(()), &HashSet::new())
                    .unwrap_err()
                    .status(),
                StatusCode::NOT_FOUND,
                "{path}"
            );
        }
        let bare = HandshakeRequest::builder().uri("/v8").body(()).unwrap();
        assert_eq!(
            validate_handshake(&bare, HandshakeResponse::new(()), &HashSet::new())
                .unwrap_err()
                .status(),
            StatusCode::UPGRADE_REQUIRED
        );
        let accepted = validate_handshake(
            &v8_request("/v8").body(()).unwrap(),
            HandshakeResponse::new(()),
            &HashSet::new(),
        )
        .unwrap();
        assert_eq!(
            accepted.headers().get(SEC_WEBSOCKET_PROTOCOL).unwrap(),
            waku_protocol::v8::WEBSOCKET_SUBPROTOCOL
        );
    }

    #[test]
    fn browser_origins_are_denied_unless_explicitly_allowed() {
        let request = v8_request("/v8")
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
        let native = v8_request("/v8").body(()).unwrap();
        assert!(validate_handshake(&native, HandshakeResponse::new(()), &HashSet::new()).is_ok());

        let declared_native = v8_request("/v8")
            .header(ORIGIN, "http://192.168.0.114:34125")
            .header(NATIVE_CLIENT_HEADER, NATIVE_CLIENT_HEADER_VALUE)
            .body(())
            .unwrap();
        assert!(
            validate_handshake(
                &declared_native,
                HandshakeResponse::new(()),
                &HashSet::new()
            )
            .is_ok()
        );

        let forged_native = v8_request("/v8")
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
        let address = listener.local_addr().unwrap().to_string();
        let shutdown = ShutdownSignal::new();
        let server_shutdown = shutdown.clone();
        let server = std::thread::spawn(move || {
            serve(
                listener,
                "secret".into(),
                Arc::new(SettingsBackend),
                server_shutdown,
                ServerOptions::default(),
            )
            .unwrap()
        });

        // A hello far larger than the pre-authentication budget is refused
        // before the daemon buffers it, so the handshake fails.
        let oversized = "x".repeat(MAX_PRE_AUTH_BYTES * 2);
        assert!(V8Client::connect(&address, oversized).is_err());
        assert!(V8Client::connect(&address, "wrong").is_err());
        // An ordinary client is unaffected.
        let client = V8Client::connect(&address, "secret").unwrap();
        client
            .call::<waku_client::v8::method::SettingsGet>(waku_client::v8::NoParams {})
            .unwrap();

        shutdown.trigger();
        server.join().unwrap();
    }

    #[test]
    fn a_client_reports_a_closed_connection_through_its_disconnect_callback() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let shutdown = ShutdownSignal::new();
        let server = {
            let shutdown = shutdown.clone();
            std::thread::spawn(move || {
                serve(
                    listener,
                    "secret".into(),
                    Arc::new(SettingsBackend),
                    shutdown,
                    ServerOptions::default(),
                )
                .unwrap()
            })
        };
        let client = V8Client::connect(&address, "secret").unwrap();
        let (closed, on_closed) = bounded(2);
        let first = closed.clone();
        client.on_disconnect(move || {
            let _ = first.send("registered while connected");
        });
        assert!(on_closed.recv_timeout(Duration::from_millis(100)).is_err());

        shutdown.trigger();
        server.join().unwrap();
        assert_eq!(
            on_closed.recv_timeout(Duration::from_secs(3)),
            Ok("registered while connected")
        );
        assert!(client.is_disconnected());
        // A callback registered after the close runs at once.
        client.on_disconnect(move || {
            let _ = closed.send("registered after the close");
        });
        assert_eq!(on_closed.try_recv(), Ok("registered after the close"));
    }

    #[cfg(unix)]
    #[test]
    fn local_socket_is_private_and_serves_without_tcp() {
        use std::os::unix::fs::PermissionsExt as _;
        use std::path::PathBuf;

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
                Arc::new(SettingsBackend),
                server_shutdown,
                ServerOptions::default(),
            )
            .unwrap()
        });
        let address = format!("unix:{}", path.display());
        assert!(V8Client::connect(&address, "wrong").is_err());
        let client = V8Client::connect(&address, "secret").unwrap();
        client
            .call::<waku_client::v8::method::SettingsGet>(waku_client::v8::NoParams {})
            .unwrap();
        // A client that does not open with the v8 preface is closed.
        {
            use std::io::{Read as _, Write as _};
            let mut legacy = std::os::unix::net::UnixStream::connect(&path).unwrap();
            legacy
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            legacy
                .write_all(b"GET /v1 HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .unwrap();
            let mut reply = Vec::new();
            let _ = legacy.read_to_end(&mut reply);
            assert!(reply.is_empty(), "the daemon answered a non-v8 client");
        }

        shutdown.trigger();
        server.join().unwrap();
        assert!(
            !path.exists(),
            "the daemon removes its socket when it stops"
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
