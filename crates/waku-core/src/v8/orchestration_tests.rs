//! Orchestration over v8 end to end (#54, #56, #57): the Rust v8 client
//! against a daemon connection served from a real orchestrator.
//!
//! The reducer fixture the TypeScript client tests replay
//! (`packages/waku-client/src/v8/fixtures/reducers.json`) is recorded here
//! from a deterministic scenario; regenerate it with
//! `WAKU_UPDATE_GOLDEN=1 cargo test -p waku-core --lib reducer_fixture`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncReadExt as _;
use uuid::Uuid;
use waku_client::v8::{CallError, StreamEvent, SubscribeOptions, V8Client, method, stream};
use waku_orchestrator::streams::LiveDelta;
use waku_orchestrator::streams::seed::{SeedOptions, seed_history, seed_thread, turn_events};
use waku_orchestrator::{
    Decision, Dependencies, ManualClock, NoCapabilities, Orchestrator, OrchestratorConfig,
    SequentialIds, WriteGuard,
};
use waku_protocol::orchestration::*;
use waku_protocol::v8::orchestration::{
    ArchivedParams, CommandOutputItem, CommandOutputSpec, DeltaChannel, DispatchCommand,
    DispatchParams, HistoryPageParams, ShellItem, ShellSpec, ThreadItem, ThreadSnapshotParams,
    ThreadSpec, TurnItemParams,
};
use waku_protocol::v8::{EndReason, ErrorCode, StreamCursor, StreamItem, codec};

use super::orchestration::Orchestration;
use super::{SettingsBus, V8Context};
use crate::server::{Backend, ShutdownSignal};

const TOKEN: &str = "orchestration-secret";
const START: Timestamp = 1_700_000_000_000;

struct BlobBackend;

impl Backend for BlobBackend {
    fn store_blob(&self, _mime_type: &str, bytes: &[u8]) -> anyhow::Result<(String, PathBuf)> {
        Ok((
            format!("waku-blob:{}.png", bytes.len()),
            PathBuf::from("/blobs"),
        ))
    }
}

/// A v8 endpoint on a fresh socket, served from a fresh orchestrator with a
/// manual clock and sequential ids.
struct Served {
    root: PathBuf,
    socket: PathBuf,
    clock: Arc<ManualClock>,
    orchestration: Arc<Orchestration>,
    runtime: Option<tokio::runtime::Runtime>,
}

impl Served {
    fn start() -> Self {
        Self::start_with(true)
    }

    fn start_with(orchestrated: bool) -> Self {
        let root = PathBuf::from("/tmp").join(format!(
            "wk8o-{}",
            &Uuid::new_v4().simple().to_string()[..8]
        ));
        std::fs::create_dir_all(&root).unwrap();
        let clock = Arc::new(ManualClock::new(START));
        let orchestrator = Orchestrator::open(
            OrchestratorConfig::new(root.join("state.sqlite")),
            Dependencies {
                clock: clock.clone(),
                ids: Box::new(SequentialIds::default()),
                capabilities: Arc::new(NoCapabilities),
            },
        )
        .unwrap();
        let orchestration = Orchestration::new(orchestrator, Default::default());
        let context = Arc::new(
            V8Context::new(
                TOKEN.into(),
                Arc::new(BlobBackend),
                SettingsBus::default(),
                ShutdownSignal::new(),
            )
            .with_orchestration(orchestrated.then(|| orchestration.clone())),
        );
        let socket = root.join("o.sock");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let listener = {
            let _guard = runtime.enter();
            tokio::net::UnixListener::bind(&socket).unwrap()
        };
        runtime.spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let context = context.clone();
                tokio::spawn(async move {
                    let mut magic = [0; 4];
                    if stream.read_exact(&mut magic).await.is_ok() {
                        super::serve_local(stream, magic, context).await;
                    }
                });
            }
        });
        Self {
            root,
            socket,
            clock,
            orchestration,
            runtime: Some(runtime),
        }
    }

    fn connect(&self) -> V8Client {
        V8Client::connect(&format!("unix:{}", self.socket.display()), TOKEN).unwrap()
    }

    fn orchestrator(&self) -> &Arc<Orchestrator> {
        &self.orchestration.orchestrator
    }

    fn store(&self) -> Uuid {
        self.orchestration.store_id()
    }

    fn cursor(&self) -> StreamCursor {
        StreamCursor::Sequence {
            store: self.store(),
            seq: self.orchestrator().last_sequence(),
        }
    }

    fn events(&self) -> i64 {
        self.orchestrator()
            .reader()
            .with(|connection| {
                connection.query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            })
            .unwrap()
    }
}

impl Drop for Served {
    fn drop(&mut self) {
        self.orchestration.shutdown();
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_timeout(Duration::from_secs(1));
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn next(subscription: &waku_client::v8::Subscription) -> StreamEvent {
    subscription
        .events()
        .recv_timeout(Duration::from_secs(10))
        .expect("a stream event")
}

fn thread_items(subscription: &waku_client::v8::Subscription) -> Vec<ThreadItem> {
    let mut items = Vec::new();
    loop {
        match next(subscription) {
            StreamEvent::Item(StreamItem::Thread(item)) => {
                let done = matches!(item, ThreadItem::Synchronized { .. });
                items.push(item);
                if done {
                    return items;
                }
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}

/// Appends thread items to `seen` up to and including the first that
/// satisfies `last`.
fn collect_thread_until(
    subscription: &waku_client::v8::Subscription,
    seen: &mut Vec<ThreadItem>,
    last: impl Fn(&ThreadItem) -> bool,
) {
    loop {
        let StreamEvent::Item(StreamItem::Thread(item)) = next(subscription) else {
            panic!();
        };
        let done = last(&item);
        seen.push(item);
        if done {
            return;
        }
    }
}

fn subscribe_thread(
    client: &V8Client,
    thread_id: ThreadId,
    after: Option<StreamCursor>,
) -> waku_client::v8::Subscription {
    client.subscribe::<stream::Thread>(
        ThreadSpec {
            thread_id,
            compact_turn_items: false,
        },
        SubscribeOptions {
            after,
            window: None,
        },
    )
}

fn running_tool(
    thread_id: ThreadId,
    id: TurnItemId,
    position: u64,
    status: ItemStatus,
    output: &str,
) -> DomainEvent {
    DomainEvent::TurnItemUpdated(TurnItem {
        id,
        thread_id,
        run_id: None,
        node_id: None,
        provider_thread_id: None,
        provider_turn_id: None,
        native_item_ref: None,
        parent_item_id: None,
        position,
        status,
        title: None,
        tool: None,
        stream_rev: 0,
        started_at: Some(START),
        completed_at: None,
        updated_at: START,
        kind: TurnItemKind::CommandExecution {
            input: "cargo build".into(),
            output: Some(output.into()),
            output_truncated: false,
            output_omitted: false,
            output_indicates_failure: false,
            exit_code: status.is_terminal().then_some(0),
        },
    })
}

fn ingest(served: &Served, thread_id: ThreadId, events: Vec<DomainEvent>) {
    served
        .orchestrator()
        .ingest_decision(
            thread_id,
            Decision {
                events,
                ..Decision::default()
            },
            WriteGuard::None,
        )
        .unwrap();
}

#[test]
fn a_5000_message_thread_opens_bounded_and_resumes_with_a_replay() {
    let served = Served::start();
    let (_, thread_id) = seed_thread(served.orchestrator(), "long").unwrap();
    seed_history(
        served.orchestrator(),
        thread_id,
        SeedOptions {
            turns: 2_500,
            batch: 250,
            ..SeedOptions::default()
        },
    )
    .unwrap();
    let client = served.connect();
    let subscription = subscribe_thread(&client, thread_id, None);
    let items = thread_items(&subscription);
    let [
        ThreadItem::Snapshot(snapshot),
        ThreadItem::Synchronized { cursor },
    ] = items.as_slice()
    else {
        panic!("{items:?}");
    };
    let bytes = codec::to_vec(
        codec::Encoding::MessagePack,
        &ThreadItem::Snapshot(snapshot.clone()),
    )
    .unwrap()
    .len();
    eprintln!("opening a 5,000-message thread transferred {bytes} B");
    assert!(bytes <= 1024 * 1024, "{bytes}");
    assert!(snapshot.has_more_history);
    assert_eq!(cursor, &served.cursor());

    // Ten seconds offline: the client drops, a few turns commit meanwhile.
    subscription.unsubscribe();
    drop(client);
    served.clock.advance(10_000);
    seed_history(
        served.orchestrator(),
        thread_id,
        SeedOptions {
            turns: 4,
            first_ordinal: 2_501,
            batch: 1,
            ..SeedOptions::default()
        },
    )
    .unwrap();
    let client = served.connect();
    let subscription = subscribe_thread(&client, thread_id, Some(cursor.clone()));
    let items = thread_items(&subscription);
    // The gap replays as events; no snapshot.
    assert_eq!(items.len(), 4 * 5 + 1, "{items:?}");
    assert!(
        items[..20]
            .iter()
            .all(|item| matches!(item, ThreadItem::Event { .. }))
    );
    assert_eq!(
        items.last(),
        Some(&ThreadItem::Synchronized {
            cursor: served.cursor()
        })
    );

    // A gap over 128 events gets a fresh bounded snapshot instead.
    let cursor = served.cursor();
    subscription.unsubscribe();
    seed_history(
        served.orchestrator(),
        thread_id,
        SeedOptions {
            turns: 30,
            first_ordinal: 2_505,
            ..SeedOptions::default()
        },
    )
    .unwrap();
    let subscription = subscribe_thread(&client, thread_id, Some(cursor));
    let items = thread_items(&subscription);
    assert!(matches!(items[0], ThreadItem::Snapshot(_)), "{items:?}");
    assert_eq!(items.len(), 2);
}

#[test]
fn dispatch_replies_after_its_events_are_queued_on_the_same_connection() {
    let served = Served::start();
    let (_, thread_id) = seed_thread(served.orchestrator(), "ryw").unwrap();
    let client = served.connect();
    let thread = subscribe_thread(&client, thread_id, None);
    thread_items(&thread);
    let shell =
        client.subscribe::<stream::Shell>(ShellSpec::default(), SubscribeOptions::default());
    loop {
        if let StreamEvent::Item(StreamItem::Shell(ShellItem::Synchronized { .. })) = next(&shell) {
            break;
        }
    }
    let result = client
        .call::<method::OrchestrationDispatch>(DispatchParams {
            command_id: CommandId::new("rename-1").unwrap(),
            command: DispatchCommand(Command::ThreadRename {
                thread_id,
                title: "Renamed".into(),
            }),
        })
        .unwrap();
    assert!(!result.duplicate);
    // Already queued when the reply arrived: no waiting.
    let thread_seen = thread.events().try_iter().any(|event| {
        matches!(
            event,
            StreamEvent::Item(StreamItem::Thread(ThreadItem::Event { seq, .. })) if seq == result.sequence
        )
    });
    assert!(thread_seen);
    let shell_seen = shell.events().try_iter().any(|event| {
        matches!(
            event,
            StreamEvent::Item(StreamItem::Shell(ShellItem::ThreadUpdated { seq, ref row, .. }))
                if seq == result.sequence && row.thread.title == "Renamed"
        )
    });
    assert!(shell_seen);
    // A retry with the same command id is a duplicate.
    let again = client
        .call::<method::OrchestrationDispatch>(DispatchParams {
            command_id: CommandId::new("rename-1").unwrap(),
            command: DispatchCommand(Command::ThreadRename {
                thread_id,
                title: "Renamed".into(),
            }),
        })
        .unwrap();
    assert!(again.duplicate);
    assert_eq!(again.sequence, result.sequence);
    // A rejected command is a call error with the command's reason.
    let missing = client.call::<method::OrchestrationDispatch>(DispatchParams {
        command_id: CommandId::new("archive-missing").unwrap(),
        command: DispatchCommand(Command::ThreadArchive {
            thread_id: ThreadId(Uuid::from_u128(404)),
        }),
    });
    assert!(matches!(missing, Err(CallError::Remote(error)) if error.code == ErrorCode::NotFound));
}

#[test]
fn running_tool_updates_coalesce_and_deltas_stream_live() {
    let served = Served::start();
    let (_, thread_id) = seed_thread(served.orchestrator(), "live").unwrap();
    let client = served.connect();
    let subscription = subscribe_thread(&client, thread_id, None);
    thread_items(&subscription);
    let tool: TurnItemId = TurnItemId(Uuid::from_u128(77));
    for n in 0..30 {
        ingest(
            &served,
            thread_id,
            vec![running_tool(
                thread_id,
                tool,
                5,
                ItemStatus::Running,
                &format!("line {n}"),
            )],
        );
    }
    ingest(
        &served,
        thread_id,
        vec![running_tool(
            thread_id,
            tool,
            5,
            ItemStatus::Completed,
            "done",
        )],
    );
    let mut updates = Vec::new();
    loop {
        let StreamEvent::Item(StreamItem::Thread(ThreadItem::Event { seq, event })) =
            next(&subscription)
        else {
            panic!("expected events");
        };
        let DomainEvent::TurnItemUpdated(item) = event else {
            continue;
        };
        updates.push((seq, item.status));
        if item.status == ItemStatus::Completed {
            break;
        }
    }
    eprintln!("31 tool updates arrived as {}", updates.len());
    assert!(updates.len() < 31, "{updates:?}");
    assert!(updates.windows(2).all(|pair| pair[0].0 < pair[1].0));
    // Outputs never ride on the stream.
    served.orchestration.live.deltas.publish(LiveDelta {
        thread_id,
        item_id: tool,
        channel: DeltaChannel::Text,
        rev: 1,
        text: "Hel".into(),
    });
    let StreamEvent::Item(StreamItem::Thread(ThreadItem::Delta { rev, text, .. })) =
        next(&subscription)
    else {
        panic!("expected a delta");
    };
    assert_eq!((rev, text.as_str()), (1, "Hel"));
}

#[test]
fn command_output_streams_from_the_hub_and_never_becomes_events() {
    let served = Served::start();
    let (_, thread_id) = seed_thread(served.orchestrator(), "output").unwrap();
    let tool = TurnItemId(Uuid::from_u128(88));
    ingest(
        &served,
        thread_id,
        vec![running_tool(thread_id, tool, 9, ItemStatus::Running, "")],
    );
    let events_before = served.events();
    let client = served.connect();
    let subscription = client.subscribe::<stream::CommandOutput>(
        CommandOutputSpec {
            thread_id,
            item_id: tool,
        },
        SubscribeOptions::default(),
    );
    let hub = served.orchestration.live.output.clone();
    hub.append(thread_id, tool, "hello\n");
    let StreamEvent::Item(StreamItem::CommandOutput(first)) = next(&subscription) else {
        panic!("expected output");
    };
    assert_eq!(
        first,
        CommandOutputItem::Replace {
            text: "hello\n".into(),
            truncated: false,
            running: true
        }
    );
    // A flood: replaced by the bounded tail, paced, and no events.
    let chunk = "y\n".repeat(8 * 1024);
    for _ in 0..2_000 {
        hub.append(thread_id, tool, &chunk);
    }
    hub.finish(tool, Some(0));
    let mut frames = Vec::new();
    let end = loop {
        match next(&subscription) {
            StreamEvent::Item(StreamItem::CommandOutput(frame)) => frames.push(frame),
            StreamEvent::End(end) => break end,
            other => panic!("unexpected {other:?}"),
        }
    };
    assert_eq!(end.reason, EndReason::Completed);
    assert!(matches!(
        frames.last(),
        Some(CommandOutputItem::Finished {
            exit_code: Some(0),
            truncated: true
        })
    ));
    let streamed: usize = frames
        .iter()
        .map(|frame| match frame {
            CommandOutputItem::Replace { text, .. } | CommandOutputItem::Append { text } => {
                text.len()
            }
            _ => 0,
        })
        .sum();
    eprintln!(
        "32 MiB of output streamed as {streamed} B in {} frames",
        frames.len()
    );
    assert!(streamed < 2 * 1024 * 1024, "{streamed}");
    assert_eq!(served.events(), events_before);

    // A finished command the hub no longer has: its stored output.
    let stored = TurnItemId(Uuid::from_u128(89));
    ingest(
        &served,
        thread_id,
        vec![running_tool(
            thread_id,
            stored,
            10,
            ItemStatus::Completed,
            "stored output",
        )],
    );
    let subscription = client.subscribe::<stream::CommandOutput>(
        CommandOutputSpec {
            thread_id,
            item_id: stored,
        },
        SubscribeOptions::default(),
    );
    let StreamEvent::Item(StreamItem::CommandOutput(CommandOutputItem::Replace {
        text,
        running,
        ..
    })) = next(&subscription)
    else {
        panic!("expected the stored output");
    };
    assert_eq!((text.as_str(), running), ("stored output", false));
    // An unknown item ends `notFound`.
    let missing = client.subscribe::<stream::CommandOutput>(
        CommandOutputSpec {
            thread_id,
            item_id: TurnItemId(Uuid::from_u128(1)),
        },
        SubscribeOptions::default(),
    );
    let StreamEvent::End(end) = next(&missing) else {
        panic!("expected the end");
    };
    assert_eq!(end.reason, EndReason::NotFound);
}

#[test]
fn history_pages_turn_items_and_archives_over_calls() {
    let served = Served::start();
    let (_, thread_id) = seed_thread(served.orchestrator(), "calls").unwrap();
    seed_history(
        served.orchestrator(),
        thread_id,
        SeedOptions {
            turns: 60,
            tool_calls_per_turn: 2,
            tool_output_bytes: 2_000,
            ..SeedOptions::default()
        },
    )
    .unwrap();
    let client = served.connect();
    let snapshot = client
        .call::<method::ThreadsSnapshot>(ThreadSnapshotParams {
            thread_id,
            compact_turn_items: true,
        })
        .unwrap();
    let mut total = snapshot.projection.turn_items.len();
    let mut cursor = snapshot.history.clone();
    while let Some(next) = cursor {
        let page = client
            .call::<method::ThreadsHistoryPage>(HistoryPageParams {
                thread_id,
                cursor: next,
            })
            .unwrap();
        total += page.items.len();
        cursor = page.next;
    }
    assert_eq!(total, 60 * 4);
    let tool = snapshot
        .projection
        .turn_items
        .iter()
        .find(|item| matches!(item.kind, TurnItemKind::CommandExecution { .. }))
        .unwrap();
    let full = client
        .call::<method::ThreadsTurnItem>(TurnItemParams {
            thread_id,
            item_id: tool.id,
        })
        .unwrap();
    assert!(matches!(
        &full.kind,
        TurnItemKind::CommandExecution { output: Some(output), .. } if output.len() == 2_000
    ));
    let bad = client.call::<method::ThreadsHistoryPage>(HistoryPageParams {
        thread_id,
        cursor: "nonsense".into(),
    });
    assert!(matches!(bad, Err(CallError::Remote(error)) if error.code == ErrorCode::BadRequest));
    client
        .call::<method::OrchestrationDispatch>(DispatchParams {
            command_id: CommandId::new("archive").unwrap(),
            command: DispatchCommand(Command::ThreadArchive { thread_id }),
        })
        .unwrap();
    let archived = client
        .call::<method::ThreadsArchived>(ArchivedParams::default())
        .unwrap();
    assert_eq!(archived.rows.len(), 1);
}

#[test]
fn without_an_orchestrator_orchestration_is_unsupported() {
    let served = Served::start_with(false);
    let client = served.connect();
    let result = client.call::<method::ThreadsSnapshot>(ThreadSnapshotParams {
        thread_id: ThreadId(Uuid::nil()),
        compact_turn_items: false,
    });
    assert!(
        matches!(result, Err(CallError::Remote(error)) if error.code == ErrorCode::Unsupported)
    );
    let subscription = subscribe_thread(&client, ThreadId(Uuid::nil()), None);
    let StreamEvent::End(end) = next(&subscription) else {
        panic!("expected the end");
    };
    assert!(
        matches!(end.reason, EndReason::Error { error } if error.code == ErrorCode::Unsupported)
    );
}

fn fixture_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../packages/waku-client/src/v8/fixtures/reducers.json")
}

/// `value` with the random store id replaced by the nil UUID, so the
/// fixture is deterministic.
fn normalize(value: serde_json::Value, store: Uuid) -> serde_json::Value {
    let text = serde_json::to_string(&value)
        .unwrap()
        .replace(&store.to_string(), &Uuid::nil().to_string());
    serde_json::from_str(&text).unwrap()
}

/// Records what a client sees of a thread and a shell while things change,
/// and what a fresh snapshot says afterwards. The TypeScript reducers must
/// turn the first into the second.
#[test]
fn reducer_fixture() {
    let served = Served::start();
    let (project_id, thread_id) = seed_thread(served.orchestrator(), "fixture").unwrap();
    seed_history(
        served.orchestrator(),
        thread_id,
        SeedOptions {
            turns: 3,
            tool_calls_per_turn: 1,
            tool_output_bytes: 300,
            ..SeedOptions::default()
        },
    )
    .unwrap();
    let client = served.connect();
    let thread = subscribe_thread(&client, thread_id, None);
    let mut thread_seen: Vec<ThreadItem> = thread_items(&thread);
    let shell =
        client.subscribe::<stream::Shell>(ShellSpec::default(), SubscribeOptions::default());
    let mut shell_seen: Vec<ShellItem> = Vec::new();
    loop {
        let StreamEvent::Item(StreamItem::Shell(item)) = next(&shell) else {
            panic!();
        };
        let done = matches!(item, ShellItem::Synchronized { .. });
        shell_seen.push(item);
        if done {
            break;
        }
    }

    let dispatch = |id: &str, command| {
        client
            .call::<method::OrchestrationDispatch>(DispatchParams {
                command_id: CommandId::new(id).unwrap(),
                command: DispatchCommand(command),
            })
            .unwrap()
    };
    // A streaming answer: snapshot at rev 2, deltas 3 and 4, a gap (6),
    // then the persisted snapshot at rev 5.
    let answer = TurnItemId(Uuid::from_u128(0xa11));
    let streaming = |rev: u32, text: &str, status: ItemStatus| {
        DomainEvent::TurnItemUpdated(TurnItem {
            id: answer,
            thread_id,
            run_id: None,
            node_id: None,
            provider_thread_id: None,
            provider_turn_id: None,
            native_item_ref: None,
            parent_item_id: None,
            position: TurnItem::position_of(4, 1),
            status,
            title: None,
            tool: None,
            stream_rev: rev,
            started_at: Some(START),
            completed_at: None,
            updated_at: START,
            kind: TurnItemKind::AssistantMessage {
                message_id: MessageId(Uuid::from_u128(0xa12)),
                text: text.into(),
                streaming: !status.is_terminal(),
            },
        })
    };
    let mut events = turn_events(
        thread_id,
        4,
        &SeedOptions {
            tool_calls_per_turn: 1,
            tool_output_bytes: 300,
            ..SeedOptions::default()
        },
    );
    events.truncate(3);
    ingest(&served, thread_id, events);
    ingest(
        &served,
        thread_id,
        vec![streaming(2, "He", ItemStatus::Running)],
    );
    dispatch(
        "fixture-rename",
        Command::ThreadRename {
            thread_id,
            title: "Fixture, renamed".into(),
        },
    );
    // Deltas travel apart from events, so pin the order the fixture records:
    // every event so far, then the deltas, then the persisted snapshot.
    let last_own = |served: &Served| {
        served
            .orchestrator()
            .reader()
            .with(|connection| {
                connection.query_row(
                    "SELECT MAX(sequence) FROM events WHERE thread_id = ?1",
                    [thread_id.to_string()],
                    |row| row.get::<_, i64>(0),
                )
            })
            .unwrap() as u64
    };
    let renamed = last_own(&served);
    collect_thread_until(
        &thread,
        &mut thread_seen,
        |item| matches!(item, ThreadItem::Event { seq, .. } if *seq == renamed),
    );
    for (rev, text) in [(3, "ll"), (4, "o,"), (6, " skipped")] {
        served.orchestration.live.deltas.publish(LiveDelta {
            thread_id,
            item_id: answer,
            channel: DeltaChannel::Text,
            rev,
            text: text.into(),
        });
    }
    collect_thread_until(&thread, &mut thread_seen, |item| {
        matches!(item, ThreadItem::Delta { rev: 6, .. })
    });
    ingest(
        &served,
        thread_id,
        vec![streaming(5, "Hello, world", ItemStatus::Completed)],
    );
    let second = ThreadId(waku_orchestrator::ids::derived_id("fixture", "second"));
    dispatch(
        "fixture-second",
        Command::ThreadCreate {
            thread_id: second,
            project_id,
            title: Some("Second".into()),
            model_selection: ModelSelection::new("codex"),
            runtime_mode: RuntimeMode::FullAccess,
            interaction_mode: InteractionMode::Default,
            workspace: WorkspaceStrategy::Root { branch: None },
            imported_native_thread: None,
        },
    );
    dispatch(
        "fixture-archive",
        Command::ThreadArchive { thread_id: second },
    );
    let head = served.orchestrator().last_sequence();

    // Collect until each stream delivered its last event.
    let last_own = last_own(&served);
    collect_thread_until(
        &thread,
        &mut thread_seen,
        |item| matches!(item, ThreadItem::Event { seq, .. } if *seq == last_own),
    );
    loop {
        let StreamEvent::Item(StreamItem::Shell(item)) = next(&shell) else {
            panic!();
        };
        let done = matches!(&item, ShellItem::ThreadUpdated { seq, .. } if *seq == head);
        shell_seen.push(item);
        if done {
            break;
        }
    }

    let expected_thread = client
        .call::<method::ThreadsSnapshot>(ThreadSnapshotParams {
            thread_id,
            compact_turn_items: false,
        })
        .unwrap();
    let expected_shell: Vec<_> = served
        .orchestrator()
        .reader()
        .with(|connection| {
            waku_orchestrator::streams::shell::shell_rows(
                connection,
                served.store(),
                &ShellSpec::default(),
            )
        })
        .unwrap()
        .threads;
    let store = served.store();
    let fixture = normalize(
        serde_json::json!({
            "thread": { "items": thread_seen, "expected": expected_thread.projection },
            "shell": { "items": shell_seen, "expectedThreads": expected_shell },
        }),
        store,
    );
    let text = serde_json::to_string_pretty(&fixture).unwrap() + "\n";
    let path = fixture_path();
    if std::env::var_os("WAKU_UPDATE_GOLDEN").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &text).unwrap();
        return;
    }
    let committed = std::fs::read_to_string(&path).unwrap_or_default();
    assert!(
        committed == text,
        "{} is stale; rerun with WAKU_UPDATE_GOLDEN=1",
        path.display()
    );
}
