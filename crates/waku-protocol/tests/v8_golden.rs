//! Golden encodings of protocol v8 (RFC 0003 §11).
//!
//! Every sample below is encoded with both encodings and compared byte for
//! byte with `tests/v8/<name>.json` and `tests/v8/<name>.msgpack.hex`; each
//! golden file must also decode back to its sample. The fixtures are the
//! cross-language contract: the TypeScript client's tests load the same
//! files, and [`typescript_round_trip`] has the TypeScript codec re-encode
//! them and checks that Rust decodes the result to the same values.
//!
//! Regenerate after an intentional wire change with
//! `WAKU_UPDATE_GOLDEN=1 cargo test -p waku-protocol --test v8_golden`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use uuid::Uuid;
use waku_protocol::v8::codec::{Encoding, from_slice, from_slice_seed, to_vec};
use waku_protocol::v8::frame;
use waku_protocol::orchestration::{
    AppThread, Command, CommandId, DomainEvent, Project, Run, ThreadId, TurnItem, TurnItemId,
};
use waku_protocol::v8::message::Routes;
use waku_protocol::v8::orchestration::{
    CommandOutputItem, CommandOutputSpec, DeltaChannel, DispatchCommand, DispatchParams,
    DispatchResult, HistoryPage, HistoryPageParams, ShellItem, ShellLocation, ShellSpec,
    ThreadActivity, ThreadItem, ThreadProjection, ThreadRow, ThreadSnapshot, ThreadSpec,
};
use waku_protocol::v8::*;

enum Sample {
    Client(ClientMessage),
    Server(ServerMessage),
}

const INSTANCE: Uuid = Uuid::from_u128(0x1111_2222_3333_4444_5555_6666_7777_8888);
const ENVIRONMENT: Uuid = Uuid::from_u128(0x0e0e_0e0e_0e0e_4e0e_8e0e_0e0e_0e0e_0e0e);
const STORE: Uuid = Uuid::from_u128(0x5705_e000_0000_4000_8000_0000_0000_0001);
const SESSION: Uuid = Uuid::from_u128(0x5e55_1000_0000_4000_8000_0000_0000_0002);
const PROJECT: Uuid = Uuid::from_u128(0x9a0e_c700_0000_4000_8000_0000_0000_0003);
const BLOB: &str = "waku-blob:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08.txt";

/// The call ids and channels the samples use, for decoding replies and items.
fn routes() -> Routes {
    Routes {
        calls: [
            (1, "settings.get"),
            (2, "projects.list"),
            (3, "files.read"),
            (4, "orchestration.dispatch"),
            (5, "threads.historyPage"),
        ]
        .into(),
        channels: [
            (1, "settings"),
            (2, "blobDownload"),
            (3, "thread"),
            (4, "shell"),
            (5, "commandOutput"),
        ]
        .into(),
    }
}

fn settings() -> Settings {
    Settings {
        computer_use_enabled: true,
        computer_use_allowed_apps: vec![AppGrant {
            bundle_id: "com.apple.Safari".into(),
            app_name: "Safari".into(),
        }],
        disabled_providers: vec!["grok".into()],
        provider_binary_overrides: BTreeMap::from([
            ("claude".into(), "/opt/claude/bin/claude".into()),
            ("codex".into(), "/usr/local/bin/codex".into()),
        ]),
    }
}

const THREAD: Uuid = Uuid::from_u128(0x7e8e_ad00_0000_4000_8000_0000_0000_0004);
const ITEM: Uuid = Uuid::from_u128(0x17e3_0000_0000_4000_8000_0000_0000_0005);

fn thread() -> AppThread {
    serde_json::from_value(serde_json::json!({
        "id": THREAD,
        "projectId": PROJECT,
        "createdBy": "user",
        "creationSource": "desktop",
        "title": "Fix the flaky test",
        "titleSource": "user",
        "modelSelection": { "instance": "codex", "model": "gpt-5" },
        "runtimeMode": "full_access",
        "interactionMode": "default",
        "historyOrigin": "native",
        "lineage": { "rootThreadId": THREAD },
        "createdAt": 1_760_000_000_000_i64,
        "updatedAt": 1_760_000_100_000_i64,
        "lastReplyAt": 1_760_000_100_000_i64,
    }))
    .unwrap()
}

fn run() -> Run {
    serde_json::from_value(serde_json::json!({
        "id": Uuid::from_u128(0x2),
        "threadId": THREAD,
        "ordinal": 3,
        "modelSelection": { "instance": "codex" },
        "userMessageId": Uuid::from_u128(0x3),
        "status": "completed",
        "requestedAt": 1_760_000_000_000_i64,
        "completedAt": 1_760_000_090_000_i64,
    }))
    .unwrap()
}

/// A command execution as streams carry it: output omitted.
fn command_item() -> TurnItem {
    serde_json::from_value(serde_json::json!({
        "id": ITEM,
        "threadId": THREAD,
        "runId": Uuid::from_u128(0x2),
        "position": (3_u64 << 24) | 1,
        "status": "completed",
        "updatedAt": 1_760_000_090_000_i64,
        "kind": {
            "type": "command_execution",
            "input": "cargo test",
            "outputOmitted": true,
            "outputIndicatesFailure": true,
            "exitCode": 101,
        },
    }))
    .unwrap()
}

fn answer_item() -> TurnItem {
    serde_json::from_value(serde_json::json!({
        "id": Uuid::from_u128(0x6),
        "threadId": THREAD,
        "runId": Uuid::from_u128(0x2),
        "position": (3_u64 << 24) | 2,
        "status": "running",
        "streamRev": 4,
        "updatedAt": 1_760_000_090_000_i64,
        "kind": {
            "type": "assistant_message",
            "messageId": Uuid::from_u128(0x7),
            "text": "The test races on",
            "streaming": true,
        },
    }))
    .unwrap()
}

fn sequence(seq: u64) -> StreamCursor {
    StreamCursor::Sequence { store: STORE, seq }
}

fn orchestration_samples() -> Vec<(&'static str, Sample)> {
    use Sample::{Client, Server};
    let row = ThreadRow {
        thread: thread(),
        activity: ThreadActivity::Working,
        pending_request_count: 0,
        queued_run_count: 1,
    };
    let project: Project = serde_json::from_value(serde_json::json!({
        "id": PROJECT,
        "name": "waku",
        "path": "/Users/dev/waku",
        "orderKey": "a0",
        "createdAt": 1_760_000_000_000_i64,
        "updatedAt": 1_760_000_000_000_i64,
    }))
    .unwrap();
    let mut projection = ThreadProjection::new(thread());
    projection.runs = vec![run()];
    projection.turn_items = vec![command_item(), answer_item()];
    vec![
        (
            "client_call_orchestration_dispatch",
            Client(ClientMessage::Call {
                id: 4,
                method: Method::OrchestrationDispatch(DispatchParams {
                    command_id: CommandId::new("rename:7e8ead00").unwrap(),
                    command: DispatchCommand(Command::ThreadRename {
                        thread_id: ThreadId(THREAD),
                        title: "Fix the flaky test".into(),
                    }),
                }),
            }),
        ),
        (
            "server_reply_orchestration_dispatch",
            Server(ServerMessage::Reply(Reply::ok(
                4,
                MethodResult::OrchestrationDispatch(DispatchResult {
                    sequence: 1_042,
                    duplicate: false,
                }),
            ))),
        ),
        (
            "client_call_threads_history_page",
            Client(ClientMessage::Call {
                id: 5,
                method: Method::ThreadsHistoryPage(HistoryPageParams {
                    thread_id: ThreadId(THREAD),
                    cursor: "h1.7e8ead0000004000800000000000004.3000001".into(),
                }),
            }),
        ),
        (
            "server_reply_threads_history_page",
            Server(ServerMessage::Reply(Reply::ok(
                5,
                MethodResult::ThreadsHistoryPage(HistoryPage {
                    items: vec![command_item()],
                    runs: vec![run()],
                    next: None,
                }),
            ))),
        ),
        (
            "client_subscribe_thread",
            Client(ClientMessage::Subscribe(Subscribe {
                channel: 3,
                stream: StreamSpec::Thread(ThreadSpec {
                    thread_id: ThreadId(THREAD),
                    compact_turn_items: true,
                }),
                after: Some(sequence(1_000)),
                window: None,
            })),
        ),
        (
            "server_items_thread",
            Server(ServerMessage::Items {
                channel: 3,
                items: vec![
                    StreamItem::Thread(ThreadItem::Snapshot(ThreadSnapshot {
                        cursor: sequence(1_040),
                        projection,
                        history: Some("h1.7e8ead0000004000800000000000004.3000001".into()),
                        has_more_history: true,
                        budget_exceeded: false,
                    })),
                    StreamItem::Thread(ThreadItem::Synchronized {
                        cursor: sequence(1_040),
                    }),
                    StreamItem::Thread(ThreadItem::Event {
                        seq: 1_041,
                        event: DomainEvent::TurnItemUpdated(answer_item()),
                    }),
                    StreamItem::Thread(ThreadItem::Delta {
                        item_id: TurnItemId(Uuid::from_u128(0x6)),
                        channel: DeltaChannel::Text,
                        rev: 5,
                        text: " the shared temp dir.".into(),
                    }),
                ],
            }),
        ),
        (
            "client_subscribe_shell",
            Client(ClientMessage::Subscribe(Subscribe {
                channel: 4,
                stream: StreamSpec::Shell(ShellSpec::default()),
                after: None,
                window: None,
            })),
        ),
        (
            "server_items_shell",
            Server(ServerMessage::Items {
                channel: 4,
                items: vec![
                    StreamItem::Shell(ShellItem::Snapshot {
                        cursor: sequence(1_040),
                        projects: vec![project.clone()],
                        threads: vec![row.clone()],
                        complete: true,
                    }),
                    StreamItem::Shell(ShellItem::Synchronized {
                        cursor: sequence(1_040),
                    }),
                    StreamItem::Shell(ShellItem::ThreadUpdated {
                        seq: 1_042,
                        row,
                        location: ShellLocation::Archived,
                    }),
                    StreamItem::Shell(ShellItem::ProjectUpdated {
                        seq: 1_043,
                        row: project,
                    }),
                    StreamItem::Shell(ShellItem::ThreadRemoved {
                        seq: 1_044,
                        thread_id: ThreadId(THREAD),
                    }),
                ],
            }),
        ),
        (
            "client_subscribe_command_output",
            Client(ClientMessage::Subscribe(Subscribe {
                channel: 5,
                stream: StreamSpec::CommandOutput(CommandOutputSpec {
                    thread_id: ThreadId(THREAD),
                    item_id: TurnItemId(ITEM),
                }),
                after: None,
                window: None,
            })),
        ),
        (
            "server_items_command_output",
            Server(ServerMessage::Items {
                channel: 5,
                items: vec![
                    StreamItem::CommandOutput(CommandOutputItem::Replace {
                        text: "running 3 tests\n".into(),
                        truncated: false,
                        running: true,
                    }),
                    StreamItem::CommandOutput(CommandOutputItem::Append {
                        text: "test a ... ok\n".into(),
                    }),
                    StreamItem::CommandOutput(CommandOutputItem::Finished {
                        exit_code: Some(101),
                        truncated: false,
                    }),
                ],
            }),
        ),
    ]
}

fn samples() -> Vec<(&'static str, Sample)> {
    let mut samples = base_samples();
    samples.extend(orchestration_samples());
    samples
}

fn base_samples() -> Vec<(&'static str, Sample)> {
    use Sample::{Client, Server};
    vec![
        (
            "client_hello",
            Client(ClientMessage::Hello(Hello {
                protocol: ProtocolVersion::CURRENT,
                client: ClientInfo {
                    kind: ClientKind::Desktop,
                    app_version: "0.1.20".into(),
                    os: "macos".into(),
                    device_label: Some("Studio".into()),
                    instance_id: INSTANCE,
                },
                credential: Credential::Local {
                    token: "local-token".into(),
                },
                compression: Vec::new(),
                features: vec!["compactTurnItems".into()],
                max_message_bytes: Some(MAX_MESSAGE_BYTES),
            })),
        ),
        (
            "client_call_settings_get",
            Client(ClientMessage::Call {
                id: 1,
                method: Method::SettingsGet(NoParams {}),
            }),
        ),
        (
            "client_call_projects_list",
            Client(ClientMessage::Call {
                id: 2,
                method: Method::ProjectsList(NoParams {}),
            }),
        ),
        (
            "client_call_files_read",
            Client(ClientMessage::Call {
                id: 3,
                method: Method::FilesRead(FilesReadParams {
                    root: RootRef::Project {
                        project_id: PROJECT,
                    },
                    path: "src/main.rs".into(),
                    max_bytes: Some(65_536),
                }),
            }),
        ),
        ("client_cancel", Client(ClientMessage::Cancel { id: 3 })),
        (
            "client_subscribe_settings",
            Client(ClientMessage::Subscribe(Subscribe {
                channel: 1,
                stream: StreamSpec::Settings(SettingsSpec {}),
                after: Some(StreamCursor::Revision { revision: 42 }),
                window: None,
            })),
        ),
        (
            "client_subscribe_blob_download",
            Client(ClientMessage::Subscribe(Subscribe {
                channel: 2,
                stream: StreamSpec::BlobDownload(BlobDownloadSpec {
                    reference: BLOB.into(),
                    offset: Some(1024),
                }),
                after: None,
                window: Some(DEFAULT_DATA_WINDOW),
            })),
        ),
        (
            "client_unsubscribe",
            Client(ClientMessage::Unsubscribe { channel: 1 }),
        ),
        (
            "client_credit",
            Client(ClientMessage::Credit {
                channel: 2,
                bytes: MAX_DATA_CHUNK_BYTES,
            }),
        ),
        ("client_ping", Client(ClientMessage::Ping { nonce: 7 })),
        (
            "client_pong",
            Client(ClientMessage::Pong {
                nonce: MAX_SAFE_INTEGER,
            }),
        ),
        (
            "client_goodbye",
            Client(ClientMessage::Goodbye {
                code: GoodbyeCode::ShuttingDown,
                message: "window closed".into(),
            }),
        ),
        (
            "server_welcome",
            Server(ServerMessage::Welcome(Welcome {
                protocol: ProtocolVersion::CURRENT,
                daemon_version: "0.1.20".into(),
                environment: EnvironmentInfo {
                    id: ENVIRONMENT,
                    name: "studio.local".into(),
                    host_os: "macos".into(),
                    host_arch: "aarch64".into(),
                    store_id: STORE,
                },
                session: SessionInfo {
                    id: SESSION,
                    scopes: vec![Scope::Admin],
                    expires_at: None,
                },
                issued_credential: None,
                limits: Limits::default(),
                compression: None,
                features: Vec::new(),
            })),
        ),
        (
            "server_rejected",
            Server(ServerMessage::Rejected(Rejected {
                code: RejectCode::VersionUnsupported,
                message: "protocol 9.0 is unsupported; this daemon speaks 8".into(),
                retry_after_ms: None,
            })),
        ),
        (
            "server_reply_settings_get",
            Server(ServerMessage::Reply(Reply::ok(
                1,
                MethodResult::SettingsGet(SettingsSnapshot {
                    revision: 4_503_599_627_370_495,
                    settings: settings(),
                }),
            ))),
        ),
        (
            "server_reply_projects_list",
            Server(ServerMessage::Reply(Reply::ok(
                2,
                MethodResult::ProjectsList(vec![ProjectInfo {
                    id: PROJECT,
                    name: "waku".into(),
                    path: "/Users/dev/waku".into(),
                    created_at: 1_760_000_000_000,
                }]),
            ))),
        ),
        (
            "server_reply_files_read",
            Server(ServerMessage::Reply(Reply::ok(
                3,
                MethodResult::FilesRead(FileContent {
                    text: Some("fn main() {\n    println!(\"héllo\");\n}\n".into()),
                    blob: None,
                    binary: false,
                    size: 38,
                    revision: "sha256:4a1f".into(),
                }),
            ))),
        ),
        (
            "server_reply_error",
            Server(ServerMessage::Reply(Reply::error(
                3,
                Error::new(ErrorCode::NotFound, "src/main.rs does not exist").with_details(
                    ErrorDetails::NotFound {
                        resource: "file".into(),
                        id: "src/main.rs".into(),
                    },
                ),
            ))),
        ),
        (
            "server_items_settings",
            Server(ServerMessage::Items {
                channel: 1,
                items: vec![
                    StreamItem::Settings(SettingsItem::Snapshot {
                        revision: 43,
                        settings: settings(),
                    }),
                    StreamItem::Settings(SettingsItem::Synchronized {
                        cursor: StreamCursor::Revision { revision: 43 },
                    }),
                    StreamItem::Settings(SettingsItem::Updated {
                        revision: 44,
                        settings: Settings::default(),
                    }),
                ],
            }),
        ),
        (
            "server_items_blob_download",
            Server(ServerMessage::Items {
                channel: 2,
                items: vec![StreamItem::BlobDownload(BlobDownloadItem::Meta {
                    size: 300_000,
                    mime: "text/plain".into(),
                    sha256: "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
                        .into(),
                })],
            }),
        ),
        (
            "server_end_overflow",
            Server(ServerMessage::End(End {
                channel: 1,
                reason: EndReason::Overflow,
                resume_after: Some(StreamCursor::Revision { revision: 43 }),
            })),
        ),
        (
            "server_end_error",
            Server(ServerMessage::End(End {
                channel: 2,
                reason: EndReason::Error {
                    error: Error::new(ErrorCode::Busy, "too many subscriptions")
                        .retryable()
                        .with_details(ErrorDetails::Limit { limit: 256 }),
                },
                resume_after: None,
            })),
        ),
        (
            "server_end_completed",
            Server(ServerMessage::End(End {
                channel: 2,
                reason: EndReason::Completed,
                resume_after: None,
            })),
        ),
        (
            "server_notice",
            Server(ServerMessage::Notice {
                notice: Notice::UpdateAvailable {
                    version: "0.2.0".into(),
                },
            }),
        ),
        ("server_ping", Server(ServerMessage::Ping { nonce: 1 })),
        ("server_pong", Server(ServerMessage::Pong { nonce: 7 })),
        (
            "server_goodbye",
            Server(ServerMessage::Goodbye {
                code: GoodbyeCode::IdleTimeout,
                message: "nothing received for 15s".into(),
                reconnect: true,
            }),
        ),
    ]
}

/// Raw frames, golden as hex: the local preface, and DATA chunks of a
/// download (the MSG frame header around a message body).
fn frame_samples() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("frame_preface", PREFACE.to_vec()),
        ("frame_msg_ping", frame::encode_msg(&golden_ping())),
        ("frame_data_chunk", frame::encode_data(2, false, b"chunk-1")),
        ("frame_data_fin", frame::encode_data(2, true, b"")),
    ]
}

fn golden_ping() -> Vec<u8> {
    to_vec(Encoding::MessagePack, &ClientMessage::Ping { nonce: 7 }).unwrap()
}

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/v8")
}

fn updating() -> bool {
    std::env::var_os("WAKU_UPDATE_GOLDEN").is_some_and(|value| value == "1")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn unhex(text: &str) -> Vec<u8> {
    let text = text.trim();
    (0..text.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&text[index..index + 2], 16).unwrap())
        .collect()
}

fn encode(sample: &Sample, encoding: Encoding) -> Vec<u8> {
    match sample {
        Sample::Client(message) => to_vec(encoding, message).unwrap(),
        Sample::Server(message) => to_vec(encoding, message).unwrap(),
    }
}

/// Decode `bytes` as the same direction as `sample` and compare.
fn assert_decodes(name: &str, sample: &Sample, encoding: Encoding, bytes: &[u8], origin: &str) {
    match sample {
        Sample::Client(expected) => {
            let decoded: ClientMessage = from_slice(encoding, bytes)
                .unwrap_or_else(|error| panic!("{origin} {name} ({encoding:?}): {error}"));
            assert_eq!(&decoded, expected, "{origin} {name} ({encoding:?})");
        }
        Sample::Server(expected) => {
            let decoded = from_slice_seed(encoding, bytes, ServerMessageSeed(&routes()))
                .unwrap_or_else(|error| panic!("{origin} {name} ({encoding:?}): {error}"));
            assert_eq!(&decoded, expected, "{origin} {name} ({encoding:?})");
        }
    }
}

fn check_or_write(path: &Path, actual: &str, stale: &mut Vec<String>) {
    if updating() {
        std::fs::write(path, actual).unwrap();
        return;
    }
    match std::fs::read_to_string(path) {
        Ok(expected) if expected == actual => {}
        Ok(_) => stale.push(format!("{} differs", path.display())),
        Err(_) => stale.push(format!("{} is missing", path.display())),
    }
}

/// `index.json`: every fixture with its direction, plus the routes needed to
/// decode replies and items, for the TypeScript tests.
fn index() -> String {
    let routes = routes();
    let fixtures = samples()
        .iter()
        .map(|(name, sample)| {
            let direction = match sample {
                Sample::Client(_) => "client",
                Sample::Server(_) => "server",
            };
            serde_json::json!({ "direction": direction, "name": name })
        })
        .collect::<Vec<_>>();
    let frames = frame_samples()
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>();
    // Keys in sorted order, so the file is the same whether or not another
    // crate in the build turns on serde_json's `preserve_order`.
    let document = serde_json::json!({
        "frames": frames,
        "messages": fixtures,
        "routes": {
            "calls": routes.calls.iter().map(|(id, method)| (id.to_string(), *method)).collect::<BTreeMap<_, _>>(),
            "channels": routes.channels.iter().map(|(channel, stream)| (channel.to_string(), *stream)).collect::<BTreeMap<_, _>>(),
        },
    });
    serde_json::to_string_pretty(&document).unwrap() + "\n"
}

#[test]
fn golden_encodings_are_byte_exact_and_decode_back() {
    let directory = fixtures();
    if updating() {
        std::fs::create_dir_all(&directory).unwrap();
    }
    let mut stale = Vec::new();
    for (name, sample) in samples() {
        let json = encode(&sample, Encoding::Json);
        let msgpack = encode(&sample, Encoding::MessagePack);
        check_or_write(
            &directory.join(format!("{name}.json")),
            &(String::from_utf8(json).unwrap() + "\n"),
            &mut stale,
        );
        check_or_write(
            &directory.join(format!("{name}.msgpack.hex")),
            &(hex(&msgpack) + "\n"),
            &mut stale,
        );
    }
    for (name, bytes) in frame_samples() {
        check_or_write(
            &directory.join(format!("{name}.hex")),
            &(hex(&bytes) + "\n"),
            &mut stale,
        );
    }
    check_or_write(&directory.join("index.json"), &index(), &mut stale);
    assert!(
        stale.is_empty(),
        "golden fixtures are stale (run with WAKU_UPDATE_GOLDEN=1 after an intentional wire change):\n{}",
        stale.join("\n")
    );

    // The committed files decode back to the samples.
    for (name, sample) in samples() {
        let json = std::fs::read(directory.join(format!("{name}.json"))).unwrap();
        assert_decodes(name, &sample, Encoding::Json, &json, "golden");
        let msgpack =
            std::fs::read_to_string(directory.join(format!("{name}.msgpack.hex"))).unwrap();
        assert_decodes(
            name,
            &sample,
            Encoding::MessagePack,
            &unhex(&msgpack),
            "golden",
        );
    }
}

#[test]
fn frames_decode_from_their_golden_bytes() {
    let mut decoder = FrameDecoder::new(FrameLimits::default());
    for (name, bytes) in frame_samples().into_iter().skip(1) {
        decoder.extend(&bytes);
        let frame = decoder
            .next_frame()
            .unwrap()
            .unwrap_or_else(|| panic!("{name}"));
        match (name, frame) {
            ("frame_msg_ping", Frame::Msg(body)) => assert_eq!(
                from_slice::<ClientMessage>(Encoding::MessagePack, &body).unwrap(),
                ClientMessage::Ping { nonce: 7 }
            ),
            ("frame_data_chunk", Frame::Data { channel, fin, body }) => {
                assert_eq!((channel, fin, body.as_slice()), (2, false, &b"chunk-1"[..]));
            }
            ("frame_data_fin", Frame::Data { channel, fin, body }) => {
                assert_eq!((channel, fin, body.len()), (2, true, 0));
            }
            (name, frame) => panic!("{name}: unexpected {frame:?}"),
        }
    }
}

/// The TypeScript codec decodes every fixture and re-encodes it in both
/// encodings; Rust must decode those bytes to the same values, and the
/// MessagePack bytes must equal Rust's (both write minimal integers in
/// declaration order). Needs `bun`; skipped without it unless
/// `WAKU_REQUIRE_BUN=1`.
#[test]
fn typescript_round_trip() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let script = root.join("packages/waku-client/src/v8/tools/reencode-golden.ts");
    let output = std::env::temp_dir().join(format!("waku-v8-golden-{}", Uuid::new_v4().simple()));
    std::fs::create_dir_all(&output).unwrap();
    let status = std::process::Command::new("bun")
        .arg(&script)
        .arg(fixtures())
        .arg(&output)
        .current_dir(&root)
        .status();
    let status = match status {
        Ok(status) => status,
        Err(error) if std::env::var_os("WAKU_REQUIRE_BUN").is_none() => {
            eprintln!("skipping the TypeScript round trip: bun is unavailable ({error})");
            return;
        }
        Err(error) => panic!("bun is required: {error}"),
    };
    assert!(status.success(), "the TypeScript re-encoder failed");
    for (name, sample) in samples() {
        let json = std::fs::read(output.join(format!("{name}.json"))).unwrap();
        assert_decodes(name, &sample, Encoding::Json, &json, "typescript");
        let msgpack =
            unhex(&std::fs::read_to_string(output.join(format!("{name}.msgpack.hex"))).unwrap());
        assert_decodes(name, &sample, Encoding::MessagePack, &msgpack, "typescript");
        assert_eq!(
            hex(&msgpack),
            hex(&encode(&sample, Encoding::MessagePack)),
            "TypeScript MessagePack of {name}"
        );
    }
    let _ = std::fs::remove_dir_all(&output);
}
