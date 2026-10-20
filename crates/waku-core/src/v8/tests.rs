//! The v8 endpoint end to end: the Rust v8 client and raw protocol probes
//! against a real daemon server, beside the legacy protocol.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use uuid::Uuid;
use waku_client::v8::{
    CallError, RejectedError, StreamEvent, SubscribeOptions, V8Client, method, stream,
};
use waku_protocol::v8::frame::{FrameDecoder, FrameHeader, FrameLimits, encode_msg};
use waku_protocol::v8::message::Routes;
use waku_protocol::v8::{
    BlobDownloadItem, BlobDownloadSpec, ClientInfo, ClientKind, ClientMessage, Credential,
    EndReason, ErrorCode, FileContent, FilesReadParams, GoodbyeCode, HANDSHAKE_MESSAGE_BYTES,
    Hello, Method, NoParams, PREFACE, ProtocolVersion, RejectCode, RootRef, Scope, ServerMessage,
    ServerMessageSeed, SettingsItem, SettingsSpec, StreamCursor, StreamItem, codec,
};

use super::{BackendHandle, SettingsBus, V8Context, settings_revision, wire_settings};
use crate::model::Project;
use crate::protocol::{Command, Request, ResponsePayload};
use crate::server::{
    Backend, EventSink, Listeners, ServerOptions, ShutdownSignal, serve_listeners,
};
use crate::settings::DaemonSettings;

const TOKEN: &str = "secret";
const BLOB: &str = "waku-blob:abc.txt";

struct TestBackend {
    settings: Mutex<DaemonSettings>,
    project_root: PathBuf,
    blob: Vec<u8>,
}

impl Backend for TestBackend {
    fn handle(&self, request: Request, _events: EventSink) -> anyhow::Result<ResponsePayload> {
        match request.command {
            Command::GetSettings => Ok(ResponsePayload::Settings {
                settings: self.settings.lock().clone(),
            }),
            Command::UpdateSettings { settings } => {
                *self.settings.lock() = settings;
                Ok(ResponsePayload::Ack)
            }
            Command::LoadTaskState => Ok(ResponsePayload::TaskState {
                projects: vec![Project {
                    id: project_id(),
                    name: "demo".into(),
                    path: self.project_root.clone(),
                    created_at: 1_700_000_000,
                }],
                sessions: Vec::new(),
                default_cwd: PathBuf::from("/"),
                projectless_root: None,
            }),
            Command::ReadBlob { reference } if reference == BLOB => Ok(ResponsePayload::BlobData {
                bytes: self.blob.clone(),
            }),
            Command::ReadBlob { .. } => anyhow::bail!("no such blob"),
            _ => anyhow::bail!("not supported by the test backend"),
        }
    }
}

fn project_id() -> Uuid {
    Uuid::from_u128(0x5eed)
}

fn blob_bytes() -> Vec<u8> {
    (0..600_000u32).map(|index| (index % 251) as u8).collect()
}

fn backend(root: &Path) -> Arc<TestBackend> {
    Arc::new(TestBackend {
        settings: Mutex::new(DaemonSettings::default()),
        project_root: root.join("project"),
        blob: blob_bytes(),
    })
}

/// A daemon on a fresh local socket and a loopback TCP port.
struct Daemon {
    root: PathBuf,
    socket: PathBuf,
    tcp: std::net::SocketAddr,
    shutdown: ShutdownSignal,
    server: Option<std::thread::JoinHandle<()>>,
}

impl Daemon {
    fn start() -> Self {
        // Short, so the path fits in `sun_path` on every runner.
        let root = PathBuf::from("/tmp")
            .join(format!("wk8-{}", &Uuid::new_v4().simple().to_string()[..8]));
        std::fs::create_dir_all(root.join("project/src")).unwrap();
        std::fs::write(root.join("project/src/main.rs"), "fn main() {}\n").unwrap();
        std::fs::write(root.join("project/image.bin"), [0u8, 159, 146, 150]).unwrap();
        let socket = root.join("run").join("d.sock");
        let local = crate::LocalListener::bind(&socket).unwrap();
        let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = tcp.local_addr().unwrap();
        let shutdown = ShutdownSignal::new();
        let server_shutdown = shutdown.clone();
        let backend = backend(&root);
        let server = std::thread::spawn(move || {
            serve_listeners(
                Listeners {
                    tcp: Some(tcp),
                    local: Some(local),
                },
                TOKEN.into(),
                backend,
                server_shutdown,
                ServerOptions::default(),
            )
            .unwrap();
        });
        Self {
            root,
            socket,
            tcp: address,
            shutdown,
            server: Some(server),
        }
    }

    fn local_address(&self) -> String {
        format!("unix:{}", self.socket.display())
    }

    fn connect(&self) -> V8Client {
        V8Client::connect(&self.local_address(), TOKEN).unwrap()
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        self.shutdown.trigger();
        if let Some(server) = self.server.take() {
            let _ = server.join();
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn next_event(subscription: &waku_client::v8::Subscription) -> StreamEvent {
    subscription
        .events()
        .recv_timeout(Duration::from_secs(5))
        .expect("a stream event")
}

fn read_file(
    client: &V8Client,
    root: RootRef,
    path: &str,
    max_bytes: Option<u32>,
) -> Result<FileContent, CallError> {
    client.call::<method::FilesRead>(FilesReadParams {
        root,
        path: path.into(),
        max_bytes,
    })
}

fn error_code(result: Result<FileContent, CallError>) -> ErrorCode {
    match result {
        Err(CallError::Remote(error)) => error.code,
        other => panic!("expected a remote error, got {other:?}"),
    }
}

#[test]
fn v8_methods_run_on_the_local_socket_beside_the_legacy_protocol() {
    let daemon = Daemon::start();
    let client = daemon.connect();
    assert_eq!(client.welcome().session.scopes, vec![Scope::Admin]);
    assert_eq!(client.welcome().protocol, ProtocolVersion::CURRENT);

    let snapshot = client.call::<method::SettingsGet>(NoParams {}).unwrap();
    assert_eq!(
        snapshot.revision,
        settings_revision(&wire_settings(&DaemonSettings::default()))
    );

    let projects = client.call::<method::ProjectsList>(NoParams {}).unwrap();
    assert_eq!(projects.len(), 1);
    assert_eq!(projects[0].id, project_id());
    assert_eq!(projects[0].created_at, 1_700_000_000_000);

    let project = RootRef::Project {
        project_id: project_id(),
    };
    let file = read_file(&client, project.clone(), "src/main.rs", None).unwrap();
    assert_eq!(file.text.as_deref(), Some("fn main() {}\n"));
    assert!(!file.binary);
    assert_eq!(file.size, 13);
    assert!(file.revision.starts_with("sha256:"));
    let binary = read_file(&client, project.clone(), "image.bin", None).unwrap();
    assert!(binary.binary && binary.text.is_none());
    let root = RootRef::Path {
        path: daemon.root.join("project").display().to_string(),
    };
    assert_eq!(
        error_code(read_file(&client, root.clone(), "missing.rs", None)),
        ErrorCode::NotFound
    );
    assert_eq!(
        error_code(read_file(&client, root.clone(), "../escape", None)),
        ErrorCode::BadRequest
    );
    assert_eq!(
        error_code(read_file(&client, root, "src/main.rs", Some(4))),
        ErrorCode::PayloadTooLarge
    );
    assert_eq!(
        error_code(read_file(
            &client,
            RootRef::Project {
                project_id: Uuid::nil()
            },
            "src/main.rs",
            None
        )),
        ErrorCode::NotFound
    );

    // The legacy protocol still answers on the same socket.
    let legacy = waku_client::DaemonClient::connect(&daemon.local_address(), TOKEN.into()).unwrap();
    assert!(matches!(
        legacy
            .request(Uuid::nil(), Uuid::nil(), Command::GetSettings)
            .unwrap(),
        ResponsePayload::Settings { .. }
    ));

    // And a wrong token is refused with a typed reason.
    let error = V8Client::connect(&daemon.local_address(), "wrong")
        .err()
        .unwrap();
    assert_eq!(
        error.downcast_ref::<RejectedError>().unwrap().0.code,
        RejectCode::Unauthenticated
    );
}

#[test]
fn the_settings_stream_follows_legacy_updates_and_resumes_from_its_revision() {
    let daemon = Daemon::start();
    let client = daemon.connect();
    let subscription =
        client.subscribe::<stream::Settings>(SettingsSpec {}, SubscribeOptions::default());
    let StreamEvent::Item(StreamItem::Settings(SettingsItem::Snapshot { revision, settings })) =
        next_event(&subscription)
    else {
        panic!("expected a snapshot");
    };
    assert!(!settings.computer_use_enabled);
    let cursor = StreamCursor::Revision { revision };
    assert_eq!(
        next_event(&subscription),
        StreamEvent::Item(StreamItem::Settings(SettingsItem::Synchronized {
            cursor: cursor.clone()
        }))
    );

    // A change made by a legacy client reaches the v8 subscriber.
    let legacy = waku_client::DaemonClient::connect(&daemon.local_address(), TOKEN.into()).unwrap();
    let changed = DaemonSettings {
        computer_use_enabled: true,
        ..DaemonSettings::default()
    };
    legacy
        .request(
            Uuid::nil(),
            Uuid::nil(),
            Command::UpdateSettings {
                settings: changed.clone(),
            },
        )
        .unwrap();
    let StreamEvent::Item(StreamItem::Settings(SettingsItem::Updated {
        revision: updated,
        settings,
    })) = next_event(&subscription)
    else {
        panic!("expected an update");
    };
    assert_ne!(updated, revision);
    assert_eq!(updated, settings_revision(&wire_settings(&changed)));
    assert!(settings.computer_use_enabled);

    // Resuming at the current revision skips the snapshot.
    let resumed = client.subscribe::<stream::Settings>(
        SettingsSpec {},
        SubscribeOptions {
            after: Some(StreamCursor::Revision { revision: updated }),
            window: None,
        },
    );
    assert_eq!(
        next_event(&resumed),
        StreamEvent::Item(StreamItem::Settings(SettingsItem::Synchronized {
            cursor: StreamCursor::Revision { revision: updated }
        }))
    );
    subscription.unsubscribe();
    let StreamEvent::End(end) = next_event(&subscription) else {
        panic!("expected the end");
    };
    assert_eq!(end.reason, EndReason::Unsubscribed);
    assert_eq!(
        end.resume_after,
        Some(StreamCursor::Revision { revision: updated })
    );
}

#[test]
fn blob_downloads_stay_within_their_credit() {
    let daemon = Daemon::start();
    let client = daemon.connect();
    let blob = blob_bytes();
    let window = 300_000;
    let download = client.subscribe::<stream::BlobDownload>(
        BlobDownloadSpec {
            reference: BLOB.into(),
            offset: None,
        },
        SubscribeOptions {
            after: None,
            window: Some(window),
        },
    );
    assert_eq!(
        next_event(&download),
        StreamEvent::Item(StreamItem::BlobDownload(BlobDownloadItem::Meta {
            size: blob.len() as u64,
            mime: "text/plain".into(),
            sha256: format!("{:x}", Sha256::digest(&blob)),
        }))
    );
    let mut received = Vec::new();
    while received.len() < window as usize {
        let StreamEvent::Data { bytes, fin } = next_event(&download) else {
            panic!("expected data");
        };
        assert!(!fin);
        assert!(bytes.len() <= waku_protocol::v8::MAX_DATA_CHUNK_BYTES as usize);
        received.extend(bytes);
    }
    assert_eq!(
        received.len(),
        window as usize,
        "never more than the credit"
    );
    assert!(
        download
            .events()
            .recv_timeout(Duration::from_millis(200))
            .is_err(),
        "the daemon waits for credit"
    );
    download.consumed(window);
    let mut fin = false;
    while !fin {
        match next_event(&download) {
            StreamEvent::Data { bytes, fin: last } => {
                received.extend(bytes);
                fin = last;
            }
            other => panic!("expected data, got {other:?}"),
        }
    }
    assert_eq!(received, blob);
    let StreamEvent::End(end) = next_event(&download) else {
        panic!("expected the end");
    };
    assert_eq!(end.reason, EndReason::Completed);

    // Resume from an offset, and a missing blob ends with an error.
    let tail = client.subscribe::<stream::BlobDownload>(
        BlobDownloadSpec {
            reference: BLOB.into(),
            offset: Some(blob.len() as u64 - 10),
        },
        SubscribeOptions::default(),
    );
    assert!(matches!(next_event(&tail), StreamEvent::Item(_)));
    assert_eq!(
        next_event(&tail),
        StreamEvent::Data {
            bytes: blob[blob.len() - 10..].to_vec(),
            fin: true
        }
    );
    let missing = client.subscribe::<stream::BlobDownload>(
        BlobDownloadSpec {
            reference: "waku-blob:missing.txt".into(),
            offset: None,
        },
        SubscribeOptions::default(),
    );
    let StreamEvent::End(end) = next_event(&missing) else {
        panic!("expected the end");
    };
    let EndReason::Error { error } = end.reason else {
        panic!("expected an error end");
    };
    assert_eq!(error.code, ErrorCode::NotFound);
}

/// A hand-driven v8 client for the paths the real client never takes.
struct Raw<S> {
    stream: S,
    decoder: FrameDecoder,
    routes: Routes,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Raw<S> {
    /// Exchange prefaces; returns the daemon's.
    async fn open(mut stream: S, preface: [u8; 8]) -> (Self, Option<[u8; 8]>) {
        stream.write_all(&preface).await.unwrap();
        let mut theirs = [0; 8];
        let answered = stream.read_exact(&mut theirs).await.ok().map(|_| theirs);
        let raw = Self {
            stream,
            decoder: FrameDecoder::new(FrameLimits::default()),
            routes: Routes::default(),
        };
        (raw, answered)
    }

    async fn send(&mut self, message: &ClientMessage) {
        let body = codec::to_vec(codec::Encoding::MessagePack, message).unwrap();
        self.stream.write_all(&encode_msg(&body)).await.unwrap();
    }

    async fn hello(&mut self, token: &str, major: u16) {
        self.send(&ClientMessage::Hello(Hello {
            protocol: ProtocolVersion { major, minor: 0 },
            client: ClientInfo {
                kind: ClientKind::Cli,
                app_version: "test".into(),
                os: "test".into(),
                device_label: None,
                instance_id: Uuid::new_v4(),
            },
            credential: Credential::Local {
                token: token.into(),
            },
            compression: Vec::new(),
            features: Vec::new(),
            max_message_bytes: None,
        }))
        .await;
    }

    /// The next message, or `None` once the daemon closed the connection.
    async fn recv(&mut self) -> Option<ServerMessage> {
        let mut chunk = [0; 4096];
        loop {
            if let Some(frame) = self.decoder.next_frame().unwrap() {
                let waku_protocol::v8::Frame::Msg(body) = frame else {
                    continue;
                };
                return Some(
                    codec::from_slice_seed(
                        codec::Encoding::MessagePack,
                        &body,
                        ServerMessageSeed(&self.routes),
                    )
                    .unwrap(),
                );
            }
            let count = self.stream.read(&mut chunk).await.unwrap_or(0);
            if count == 0 {
                return None;
            }
            self.decoder.extend(&chunk[..count]);
        }
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn handshakes_are_refused_with_a_reason() {
    let daemon = Daemon::start();
    let socket = daemon.socket.clone();
    runtime().block_on(async move {
        let connect = || tokio::net::UnixStream::connect(&socket);

        let (mut raw, preface) = Raw::open(connect().await.unwrap(), PREFACE).await;
        assert_eq!(preface, Some(PREFACE));
        raw.hello("wrong", 8).await;
        let Some(ServerMessage::Rejected(rejected)) = raw.recv().await else {
            panic!("expected a rejection");
        };
        assert_eq!(rejected.code, RejectCode::Unauthenticated);
        assert!(raw.recv().await.is_none(), "then the daemon closes");

        let (mut raw, _) = Raw::open(connect().await.unwrap(), PREFACE).await;
        raw.hello(TOKEN, 9).await;
        let Some(ServerMessage::Rejected(rejected)) = raw.recv().await else {
            panic!("expected a rejection");
        };
        assert_eq!(rejected.code, RejectCode::VersionUnsupported);

        // A preface of another major is answered with ours, then closed.
        let mut other = PREFACE;
        other[5] = 9;
        let (mut raw, preface) = Raw::open(connect().await.unwrap(), other).await;
        assert_eq!(preface, Some(PREFACE));
        assert!(raw.recv().await.is_none());

        // The first message must be a hello.
        let (mut raw, _) = Raw::open(connect().await.unwrap(), PREFACE).await;
        raw.send(&ClientMessage::Ping { nonce: 1 }).await;
        let Some(ServerMessage::Goodbye { code, .. }) = raw.recv().await else {
            panic!("expected a goodbye");
        };
        assert_eq!(code, GoodbyeCode::ProtocolError);

        // A hello frame over the 64 KiB handshake cap is refused from its
        // header alone.
        let (mut raw, _) = Raw::open(connect().await.unwrap(), PREFACE).await;
        let header = FrameHeader::msg(HANDSHAKE_MESSAGE_BYTES as usize + 1).encode();
        raw.stream.write_all(&header).await.unwrap();
        let Some(ServerMessage::Goodbye { code, .. }) = raw.recv().await else {
            panic!("expected a goodbye");
        };
        assert_eq!(code, GoodbyeCode::FrameTooLarge);

        // Unknown methods are answered, not fatal.
        let (mut raw, _) = Raw::open(connect().await.unwrap(), PREFACE).await;
        raw.hello(TOKEN, 8).await;
        assert!(matches!(raw.recv().await, Some(ServerMessage::Welcome(_))));
        raw.send(&ClientMessage::Call {
            id: 4,
            method: Method::Unknown {
                name: "future.method".into(),
            },
        })
        .await;
        let Some(ServerMessage::Reply(reply)) = raw.recv().await else {
            panic!("expected a reply");
        };
        assert_eq!(
            (reply.id, reply.error.unwrap().code),
            (4, ErrorCode::UnknownMethod)
        );
        // A ping is answered from the control queue.
        raw.send(&ClientMessage::Ping { nonce: 77 }).await;
        assert_eq!(raw.recv().await, Some(ServerMessage::Pong { nonce: 77 }));
    });
}

fn test_context(limits: waku_protocol::v8::Limits, hello_deadline: Duration) -> Arc<V8Context> {
    let root = std::env::temp_dir();
    let mut context = V8Context::new(
        TOKEN.into(),
        BackendHandle::new(backend(&root), crate::server::test_event_sink()),
        SettingsBus::default(),
        ShutdownSignal::new(),
    );
    context.limits = limits;
    context.hello_deadline = hello_deadline;
    Arc::new(context)
}

#[test]
fn keepalive_pings_an_idle_client_and_drops_a_dead_one() {
    let limits = waku_protocol::v8::Limits {
        ping_interval_ms: 40,
        ping_timeout_ms: 300,
        ..Default::default()
    };
    let context = test_context(limits, Duration::from_millis(200));
    runtime().block_on(async move {
        // No hello within the deadline: closed without a word.
        let (client, server) = tokio::io::duplex(64 * 1024);
        let serving = tokio::spawn(super::serve_local(server, *b"WAKU", context.clone()));
        let mut client = client;
        // The server already "read" the magic; send the rest of the preface.
        client.write_all(&PREFACE[4..]).await.unwrap();
        let mut preface = [0; 8];
        client.read_exact(&mut preface).await.unwrap();
        let started = std::time::Instant::now();
        let mut rest = Vec::new();
        client.read_to_end(&mut rest).await.unwrap();
        assert!(rest.is_empty());
        assert!(started.elapsed() >= Duration::from_millis(150));
        serving.await.unwrap();

        // An idle client is pinged, then dropped with `idleTimeout`.
        let (client, server) = tokio::io::duplex(64 * 1024);
        let serving = tokio::spawn(super::serve_local(server, *b"WAKU", context.clone()));
        let mut client = client;
        client.write_all(&PREFACE[4..]).await.unwrap();
        let mut raw = Raw {
            stream: client,
            decoder: FrameDecoder::new(FrameLimits::default()),
            routes: Routes::default(),
        };
        let mut preface = [0; 8];
        raw.stream.read_exact(&mut preface).await.unwrap();
        raw.hello(TOKEN, 8).await;
        let Some(ServerMessage::Welcome(welcome)) = raw.recv().await else {
            panic!("expected the welcome");
        };
        assert_eq!(welcome.limits.ping_timeout_ms, 300);
        let started = std::time::Instant::now();
        let mut pings = 0;
        let goodbye = loop {
            match raw.recv().await {
                Some(ServerMessage::Ping { .. }) => pings += 1,
                Some(ServerMessage::Goodbye { code, .. }) => break code,
                other => panic!("unexpected {other:?}"),
            }
        };
        assert_eq!(goodbye, GoodbyeCode::IdleTimeout);
        assert!(pings >= 2, "{pings} pings");
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(250) && elapsed < Duration::from_secs(3),
            "{elapsed:?}"
        );
        assert!(raw.recv().await.is_none());
        serving.await.unwrap();
    });
}

#[test]
fn the_websocket_endpoint_needs_the_subprotocol_and_speaks_json() {
    use futures_util::{SinkExt as _, StreamExt as _};
    use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest as _};

    let daemon = Daemon::start();
    let address = daemon.tcp;
    runtime().block_on(async move {
        let url = format!("ws://{address}/v8");
        let tcp = tokio::net::TcpStream::connect(address).await.unwrap();
        let refused = tokio_tungstenite::client_async(url.as_str(), tcp).await;
        match refused {
            Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                assert_eq!(response.status(), 426);
            }
            other => panic!("expected HTTP 426, got {:?}", other.map(|_| ())),
        }

        let mut request = url.as_str().into_client_request().unwrap();
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            waku_protocol::v8::WEBSOCKET_SUBPROTOCOL.parse().unwrap(),
        );
        let tcp = tokio::net::TcpStream::connect(address).await.unwrap();
        let (mut socket, response) = tokio_tungstenite::client_async(request, tcp).await.unwrap();
        assert_eq!(
            response.headers()["sec-websocket-protocol"],
            waku_protocol::v8::WEBSOCKET_SUBPROTOCOL
        );
        let hello = serde_json::json!({
            "type": "hello",
            "protocol": {"major": 8, "minor": 0},
            "client": {"kind": "web", "appVersion": "test", "os": "test", "instanceId": Uuid::new_v4()},
            "credential": {"type": "local", "token": TOKEN},
        });
        socket.send(Message::Text(hello.to_string().into())).await.unwrap();
        let Some(Ok(Message::Text(welcome))) = socket.next().await else {
            panic!("expected a text welcome");
        };
        let welcome: serde_json::Value = serde_json::from_str(welcome.as_str()).unwrap();
        assert_eq!(welcome["type"], "welcome");
        socket
            .send(Message::Text(
                r#"{"type":"call","id":1,"method":"settings.get","params":{}}"#.into(),
            ))
            .await
            .unwrap();
        let Some(Ok(Message::Text(reply))) = socket.next().await else {
            panic!("expected a text reply");
        };
        let reply: serde_json::Value = serde_json::from_str(reply.as_str()).unwrap();
        assert_eq!(reply["type"], "reply");
        assert_eq!(reply["id"], 1);
        assert!(reply["ok"]["revision"].is_u64());
        // A MessagePack frame on a JSON connection mixes encodings.
        let body = codec::to_vec(codec::Encoding::MessagePack, &ClientMessage::Ping { nonce: 1 }).unwrap();
        socket.send(Message::Binary(encode_msg(&body).into())).await.unwrap();
        let Some(Ok(Message::Text(goodbye))) = socket.next().await else {
            panic!("expected a goodbye");
        };
        let goodbye: serde_json::Value = serde_json::from_str(goodbye.as_str()).unwrap();
        assert_eq!(goodbye["code"], "protocolError");
    });

    // The Rust client speaks MessagePack frames over the same endpoint.
    let client = V8Client::connect(&format!("ws://{address}"), TOKEN).unwrap();
    assert!(client.call::<method::SettingsGet>(NoParams {}).is_ok());
}

/// The TypeScript client against the real daemon over JSON WebSocket. Needs
/// `bun`; skipped without it unless `WAKU_REQUIRE_BUN=1`.
#[test]
fn the_typescript_client_talks_to_the_daemon() {
    let daemon = Daemon::start();
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = std::process::Command::new("bun")
        .arg(workspace.join("packages/waku-client/src/v8/tools/smoke.ts"))
        .arg(format!("ws://{}", daemon.tcp))
        .arg(TOKEN)
        .current_dir(&workspace)
        .output();
    let output = match output {
        Ok(output) => output,
        Err(error) if std::env::var_os("WAKU_REQUIRE_BUN").is_none() => {
            eprintln!("skipping the TypeScript client test: bun is unavailable ({error})");
            return;
        }
        Err(error) => panic!("bun is required: {error}"),
    };
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let revision = settings_revision(&wire_settings(&DaemonSettings::default()));
    assert_eq!(report["settingsRevision"], revision);
    assert_eq!(report["projects"], 1);
    assert_eq!(
        report["streamItems"],
        serde_json::json!(["snapshot", "synchronized"])
    );
    assert_eq!(report["missingFileError"], "notFound");
}

#[test]
fn a_slow_reader_is_kicked_from_one_stream_and_resumes_from_its_cursor() {
    let mut context = test_context(waku_protocol::v8::Limits::default(), Duration::from_secs(5));
    Arc::get_mut(&mut context).unwrap().budget = super::Budget {
        items: 3,
        ..Default::default()
    };
    let bus = context.settings.clone();
    runtime().block_on(async move {
        // A tiny pipe: the daemon's writer blocks after a few items while the
        // client is not reading.
        let (client, server) = tokio::io::duplex(512);
        let serving = tokio::spawn(super::serve_local(server, *b"WAKU", context));
        let mut raw = Raw {
            stream: client,
            decoder: FrameDecoder::new(FrameLimits::default()),
            routes: Routes::default(),
        };
        raw.stream.write_all(&PREFACE[4..]).await.unwrap();
        let mut preface = [0; 8];
        raw.stream.read_exact(&mut preface).await.unwrap();
        raw.hello(TOKEN, 8).await;
        assert!(matches!(raw.recv().await, Some(ServerMessage::Welcome(_))));
        raw.routes.channels.insert(1, "settings");
        raw.routes.channels.insert(2, "settings");
        raw.send(&ClientMessage::Subscribe(waku_protocol::v8::Subscribe {
            channel: 1,
            stream: waku_protocol::v8::StreamSpec::Settings(SettingsSpec {}),
            after: None,
            window: None,
        }))
        .await;
        // Many changes while the client reads nothing.
        let mut last = 0;
        for index in 0..60 {
            let state = bus.publish(waku_protocol::v8::Settings {
                disabled_providers: vec![format!("provider-{index}")],
                ..Default::default()
            });
            last = state.revision;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        // Now read: items, then `end{overflow}` with the cursor of the last
        // item actually delivered.
        let mut delivered = None;
        let end = loop {
            match raw.recv().await.expect("the connection stays up") {
                ServerMessage::Items { channel: 1, items } => {
                    for item in items {
                        match item {
                            StreamItem::Settings(
                                SettingsItem::Snapshot { revision, .. }
                                | SettingsItem::Updated { revision, .. },
                            ) => delivered = Some(StreamCursor::Revision { revision }),
                            StreamItem::Settings(SettingsItem::Synchronized { cursor }) => {
                                delivered = Some(cursor);
                            }
                            other => panic!("unexpected {other:?}"),
                        }
                    }
                }
                ServerMessage::End(end) => break end,
                ServerMessage::Ping { .. } => {}
                other => panic!("unexpected {other:?}"),
            }
        };
        assert_eq!(end.channel, 1);
        assert_eq!(end.reason, EndReason::Overflow);
        assert!(delivered.is_some());
        assert_eq!(end.resume_after, delivered);
        // Resubscribe from there: the daemon is past it, so a fresh snapshot
        // of the latest revision follows, then `synchronized`.
        raw.send(&ClientMessage::Subscribe(waku_protocol::v8::Subscribe {
            channel: 2,
            stream: waku_protocol::v8::StreamSpec::Settings(SettingsSpec {}),
            after: end.resume_after,
            window: None,
        }))
        .await;
        let items = loop {
            match raw.recv().await.expect("the connection stays up") {
                ServerMessage::Items { channel: 2, items } => break items,
                ServerMessage::Ping { .. } => {}
                other => panic!("unexpected {other:?}"),
            }
        };
        let StreamItem::Settings(SettingsItem::Snapshot { revision, .. }) = &items[0] else {
            panic!("expected a snapshot, got {items:?}");
        };
        assert_eq!(*revision, last);
        raw.send(&ClientMessage::Goodbye {
            code: GoodbyeCode::ShuttingDown,
            message: String::new(),
        })
        .await;
        serving.await.unwrap();
    });
}
