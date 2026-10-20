//! The v8 client against a scripted daemon on a Unix socket.

use std::path::PathBuf;
use std::time::Instant;

use tokio::net::UnixListener;
use waku_protocol::v8::frame::encode_msg;

use super::*;

/// A scripted daemon: reads frames from one client and answers through
/// `script`, which returns the messages to send.
struct FakeDaemon {
    path: PathBuf,
    received: Receiver<ClientMessage>,
    _directory: TempDirectory,
}

struct TempDirectory(PathBuf);

impl Drop for TempDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn welcome(limits: Limits) -> ServerMessage {
    ServerMessage::Welcome(Welcome {
        protocol: ProtocolVersion::CURRENT,
        daemon_version: "test".into(),
        environment: EnvironmentInfo {
            id: Uuid::nil(),
            name: "test".into(),
            host_os: "test".into(),
            host_arch: "test".into(),
            store_id: Uuid::nil(),
        },
        session: SessionInfo {
            id: Uuid::nil(),
            scopes: vec![Scope::Admin],
            expires_at: None,
        },
        issued_credential: None,
        limits,
        compression: None,
        features: Vec::new(),
    })
}

fn fake_daemon(
    preface: [u8; 8],
    limits: Limits,
    script: impl FnMut(&ClientMessage) -> Vec<ServerMessage> + Send + 'static,
) -> FakeDaemon {
    let directory =
        std::env::temp_dir().join(format!("waku-v8-client-{}", Uuid::new_v4().simple()));
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("d.sock");
    let (received_tx, received) = unbounded();
    let (bound_tx, bound_rx) = bounded(1);
    let socket = path.clone();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let listener = UnixListener::bind(&socket).unwrap();
            bound_tx.send(()).unwrap();
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut theirs = [0; 8];
            stream.read_exact(&mut theirs).await.unwrap();
            stream.write_all(&preface).await.unwrap();
            if preface != PREFACE {
                return;
            }
            let mut script = script;
            let mut decoder = FrameDecoder::new(frame::FrameLimits::default());
            let mut chunk = vec![0; 4096];
            let mut welcomed = false;
            loop {
                let count = match stream.read(&mut chunk).await {
                    Ok(0) | Err(_) => return,
                    Ok(count) => count,
                };
                decoder.extend(&chunk[..count]);
                while let Ok(Some(frame::Frame::Msg(body))) = decoder.next_frame() {
                    let message: ClientMessage =
                        codec::from_slice(Encoding::MessagePack, &body).unwrap();
                    let replies = if welcomed {
                        script(&message)
                    } else {
                        welcomed = true;
                        vec![welcome(limits)]
                    };
                    let _ = received_tx.send(message);
                    for reply in replies {
                        let body = codec::to_vec(Encoding::MessagePack, &reply).unwrap();
                        if stream.write_all(&encode_msg(&body)).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });
    });
    bound_rx.recv().unwrap();
    FakeDaemon {
        path,
        received,
        _directory: TempDirectory(directory),
    }
}

fn connect(daemon: &FakeDaemon) -> anyhow::Result<V8Client> {
    V8Client::connect(&format!("unix:{}", daemon.path.display()), "token")
}

fn next_event(subscription: &Subscription) -> StreamEvent {
    subscription
        .events()
        .recv_timeout(Duration::from_secs(5))
        .expect("a stream event")
}

#[test]
fn a_silent_daemon_is_declared_dead_after_the_ping_timeout() {
    let limits = Limits {
        ping_interval_ms: 40,
        ping_timeout_ms: 300,
        ..Limits::default()
    };
    let daemon = fake_daemon(PREFACE, limits, |_| Vec::new());
    let client = connect(&daemon).unwrap();
    let started = Instant::now();
    let (gone_tx, gone_rx) = bounded(1);
    client.on_disconnect(move || {
        let _ = gone_tx.send(());
    });
    gone_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the client gives up on a silent daemon");
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_millis(250), "{elapsed:?}");
    assert!(client.is_disconnected());
    // It kept pinging while it waited.
    let pings = daemon
        .received
        .try_iter()
        .filter(|message| matches!(message, ClientMessage::Ping { .. }))
        .count();
    assert!(pings >= 2, "{pings} pings");
    assert_eq!(
        client.call::<method::SettingsGet>(NoParams {}),
        Err(CallError::Disconnected)
    );
}

#[test]
fn an_overflowed_subscription_resumes_from_its_cursor() {
    let daemon = fake_daemon(PREFACE, Limits::default(), |message| {
        let ClientMessage::Subscribe(subscribe) = message else {
            return Vec::new();
        };
        let channel = subscribe.channel;
        match &subscribe.after {
            None => vec![
                ServerMessage::Items {
                    channel,
                    items: vec![
                        StreamItem::Settings(SettingsItem::Snapshot {
                            revision: 3,
                            settings: Settings::default(),
                        }),
                        StreamItem::Settings(SettingsItem::Synchronized {
                            cursor: StreamCursor::Revision { revision: 3 },
                        }),
                    ],
                },
                ServerMessage::End(End {
                    channel,
                    reason: EndReason::Overflow,
                    resume_after: Some(StreamCursor::Revision { revision: 3 }),
                }),
            ],
            Some(after) => vec![ServerMessage::Items {
                channel,
                items: vec![StreamItem::Settings(SettingsItem::Synchronized {
                    cursor: after.clone(),
                })],
            }],
        }
    });
    let client = connect(&daemon).unwrap();
    let subscription =
        client.subscribe::<stream::Settings>(SettingsSpec {}, SubscribeOptions::default());
    let first_channel = subscription.channel();
    assert!(matches!(
        next_event(&subscription),
        StreamEvent::Item(StreamItem::Settings(SettingsItem::Snapshot {
            revision: 3,
            ..
        }))
    ));
    assert!(matches!(next_event(&subscription), StreamEvent::Item(_)));
    let cursor = StreamCursor::Revision { revision: 3 };
    assert_eq!(
        next_event(&subscription),
        StreamEvent::Resumed {
            after: Some(cursor.clone())
        }
    );
    assert_eq!(
        next_event(&subscription),
        StreamEvent::Item(StreamItem::Settings(SettingsItem::Synchronized {
            cursor: cursor.clone()
        }))
    );
    assert_ne!(subscription.channel(), first_channel);
    let subscribes = daemon
        .received
        .try_iter()
        .filter_map(|message| match message {
            ClientMessage::Subscribe(subscribe) => Some(subscribe),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(subscribes.len(), 2);
    assert_eq!(subscribes[1].after, Some(cursor));
    assert_eq!(subscribes[1].channel, subscription.channel());
    client.close();
}

#[test]
fn a_daemon_of_another_major_is_reported() {
    let mut preface = PREFACE;
    preface[5] = 9;
    let daemon = fake_daemon(preface, Limits::default(), |_| Vec::new());
    let error = connect(&daemon).err().expect("the connect fails");
    let rejected = error
        .downcast_ref::<RejectedError>()
        .expect("a version rejection");
    assert_eq!(rejected.0.code, RejectCode::VersionUnsupported);
}

#[test]
fn endpoints_parse_like_the_legacy_client() {
    assert_eq!(
        Endpoint::parse("127.0.0.1:4100").unwrap(),
        Endpoint::WebSocket("ws://127.0.0.1:4100/v8".into())
    );
    assert_eq!(
        Endpoint::parse("wss://host.example/v1?x=1").unwrap(),
        Endpoint::WebSocket("wss://host.example/v8".into())
    );
    assert_eq!(
        Endpoint::parse("unix:/tmp/d.sock").unwrap(),
        Endpoint::Unix("/tmp/d.sock".into())
    );
}
