//! The real daemon with the orchestrator as its single source of truth
//! (#55): `WakuBackend` plus `DaemonHost` (orchestrator, effect workers,
//! provider session manager, lifecycle service), served over protocol v8,
//! driving the fake Codex and Claude CLIs.
//!
//! A turn runs, streams and persists inside the daemon whether or not a
//! client is attached; a second client sees the same transcript; a restart
//! keeps it.
#![cfg(unix)]

mod common;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{FakeDir, sample_timeline};
use uuid::Uuid;
use waku_client::v8::{StreamEvent, SubscribeOptions, V8Client, method, stream};
use waku_core::daemon::WakuBackend;
use waku_core::daemon_host::{DaemonHost, HostOptions};
use waku_core::model::ProviderKind;
use waku_core::{DaemonSettings, DaemonSettingsStore, ServerOptions};
use waku_protocol::orchestration::*;
use waku_protocol::v8::orchestration::{
    DeltaChannel, DispatchCommand, DispatchParams, ItemsAround, SearchSource, ThreadFindParams,
    ThreadItem, ThreadSearchParams, ThreadSnapshotParams, ThreadSpec,
};
use waku_protocol::v8::{NoParams, StreamItem};

const TOKEN: &str = "orchestrated-secret";
/// The sample timeline spans 12 ms; at this rate a turn takes 2.4 s, long
/// enough to stream deltas and to drop a client mid-turn.
const RATE: &str = "0.005";

/// A daemon composed as `waku-daemon`'s `main` composes it, on a local
/// socket, over the data directory `root`.
struct Daemon {
    socket: PathBuf,
    shutdown: waku_core::ShutdownSignal,
    server: Option<std::thread::JoinHandle<()>>,
}

impl Daemon {
    fn start(root: &Path, overrides: &[(ProviderKind, &Path)]) -> Self {
        let settings = DaemonSettingsStore::open(root.join("settings.json")).unwrap();
        let mut document = DaemonSettings::default();
        for (provider, binary) in overrides {
            document
                .provider_binary_overrides
                .insert(*provider, binary.display().to_string());
        }
        settings.replace(document).unwrap();
        let backend = Arc::new(WakuBackend::new(settings, root.to_path_buf()).unwrap());
        let host = DaemonHost::start(&backend, HostOptions::default()).unwrap();
        let socket = root.join("d.sock");
        let _ = std::fs::remove_file(&socket);
        let local = waku_core::LocalListener::bind(&socket).unwrap();
        let shutdown = waku_core::ShutdownSignal::new();
        let server_shutdown = shutdown.clone();
        let server = std::thread::spawn(move || {
            let result = waku_core::serve_listeners(
                waku_core::Listeners {
                    tcp: None,
                    local: Some(local),
                },
                TOKEN.into(),
                backend,
                server_shutdown,
                ServerOptions {
                    orchestration: Some(host.orchestration()),
                    environment_id: Some(host.environment_id()),
                    ..ServerOptions::default()
                },
            );
            host.shutdown();
            result.unwrap();
        });
        Self {
            socket,
            shutdown,
            server: Some(server),
        }
    }

    fn connect(&self) -> V8Client {
        V8Client::connect(&format!("unix:{}", self.socket.display()), TOKEN).unwrap()
    }

    fn stop(mut self) {
        self.halt();
    }

    fn halt(&mut self) {
        self.shutdown.trigger();
        if let Some(server) = self.server.take() {
            server.join().unwrap();
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        self.halt();
    }
}

/// A short data directory (the socket path must fit `sun_path`) with a git
/// project in it.
struct Scratch {
    root: PathBuf,
}

impl Scratch {
    fn new() -> Self {
        let root = PathBuf::from("/tmp").join(format!(
            "wk55-{}",
            &Uuid::new_v4().simple().to_string()[..8]
        ));
        let project = root.join("project");
        std::fs::create_dir_all(&project).unwrap();
        std::fs::write(project.join("README.md"), "e2e\n").unwrap();
        for args in [
            &["init", "-q"][..],
            &["add", "."],
            &[
                "-c",
                "user.name=Waku",
                "-c",
                "user.email=waku@example.invalid",
                "commit",
                "-q",
                "-m",
                "init",
            ],
        ] {
            let status = std::process::Command::new("git")
                .args(args)
                .current_dir(&project)
                .status()
                .unwrap();
            assert!(status.success(), "git {args:?}");
        }
        Self { root }
    }

    fn project(&self) -> PathBuf {
        self.root.join("project")
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn dispatch(client: &V8Client, command: Command) -> u64 {
    client
        .call::<method::OrchestrationDispatch>(DispatchParams {
            command_id: CommandId::new(format!("e2e:{}", Uuid::new_v4().simple())).unwrap(),
            command: DispatchCommand(command),
        })
        .unwrap_or_else(|error| panic!("dispatch failed: {error:?}"))
        .sequence
}

/// A project and a thread on `instance`, created over v8.
fn create_thread(client: &V8Client, project: &Path, instance: &str) -> (ProjectId, ThreadId) {
    let project_id = ProjectId(Uuid::new_v4());
    let thread_id = ThreadId(Uuid::new_v4());
    dispatch(
        client,
        Command::ProjectCreate {
            project_id,
            name: "e2e".into(),
            path: project.to_owned(),
            order_key: None,
        },
    );
    dispatch(
        client,
        Command::ThreadCreate {
            thread_id,
            project_id,
            // A user title: no title generation runs against the fakes.
            title: Some(format!("{instance} e2e")),
            model_selection: ModelSelection::new(instance),
            runtime_mode: RuntimeMode::FullAccess,
            interaction_mode: InteractionMode::Default,
            workspace: WorkspaceStrategy::Root { branch: None },
            imported_native_thread: None,
        },
    );
    (project_id, thread_id)
}

fn send_message(client: &V8Client, thread_id: ThreadId, text: &str) -> MessageId {
    let message_id = MessageId(Uuid::new_v4());
    dispatch(
        client,
        Command::MessageDispatch {
            thread_id,
            message_id,
            text: text.into(),
            display_text: None,
            attachments: Vec::new(),
            context: Vec::new(),
            model_selection: None,
            mode: DispatchMode::Auto,
            title_seed: None,
            source_plan: None,
        },
    );
    message_id
}

fn snapshot(
    client: &V8Client,
    thread_id: ThreadId,
) -> waku_protocol::v8::orchestration::ThreadSnapshot {
    client
        .call::<method::ThreadsSnapshot>(ThreadSnapshotParams {
            thread_id,
            compact_turn_items: false,
        })
        .unwrap()
}

/// Waits (polling the daemon, not a stream) until every run of the thread
/// is terminal and there is at least `runs` of them.
fn wait_settled(
    client: &V8Client,
    thread_id: ThreadId,
    runs: usize,
) -> waku_protocol::v8::orchestration::ThreadSnapshot {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let snapshot = snapshot(client, thread_id);
        let projection = &snapshot.projection;
        if projection.runs.len() >= runs
            && projection.runs.iter().all(|run| run.status.is_terminal())
        {
            return snapshot;
        }
        assert!(
            Instant::now() < deadline,
            "the turn never settled: {:#?}",
            projection.runs
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The assistant text of `items`, in transcript order.
fn assistant_text(items: &[TurnItem]) -> String {
    let mut items = items.iter().collect::<Vec<_>>();
    items.sort_by_key(|item| item.position);
    items
        .iter()
        .filter_map(|item| match &item.kind {
            TurnItemKind::AssistantMessage { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

/// What a client folds from a `thread` stream: each streaming item's text
/// from its snapshots plus the live deltas newer than them.
#[derive(Default)]
struct Folded {
    text: HashMap<TurnItemId, (u32, String)>,
    deltas_applied: usize,
    completed_runs: usize,
}

impl Folded {
    fn apply(&mut self, item: ThreadItem) {
        match item {
            ThreadItem::Event {
                event: DomainEvent::TurnItemUpdated(item),
                ..
            } => {
                if let TurnItemKind::AssistantMessage { text, .. } = &item.kind {
                    self.text.insert(item.id, (item.stream_rev, text.clone()));
                }
            }
            ThreadItem::Event {
                event: DomainEvent::RunUpdated(run),
                ..
            } if run.status == RunStatus::Completed => self.completed_runs += 1,
            ThreadItem::Delta {
                item_id,
                channel: DeltaChannel::Text,
                rev,
                text,
            } => {
                let (current, folded) = self.text.entry(item_id).or_default();
                if rev == *current + 1 {
                    folded.push_str(&text);
                    *current = rev;
                    self.deltas_applied += 1;
                }
            }
            _ => {}
        }
    }
}

/// Reads `subscription` until a run completes, folding what arrives.
fn follow_until_completed(subscription: &waku_client::v8::Subscription) -> Folded {
    let mut folded = Folded::default();
    let deadline = Instant::now() + Duration::from_secs(90);
    while folded.completed_runs == 0 {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match subscription.events().recv_timeout(remaining) {
            Ok(StreamEvent::Item(StreamItem::Thread(item))) => folded.apply(item),
            Ok(StreamEvent::Item(_) | StreamEvent::Resumed { .. }) => {}
            Ok(other) => panic!("the thread stream ended: {other:?}"),
            Err(_) => panic!("no completed run within 90 s"),
        }
    }
    folded
}

fn subscribe_thread(client: &V8Client, thread_id: ThreadId) -> waku_client::v8::Subscription {
    let subscription = client.subscribe::<stream::Thread>(
        ThreadSpec {
            thread_id,
            compact_turn_items: false,
        },
        SubscribeOptions::default(),
    );
    // The snapshot, then `synchronized`.
    loop {
        match subscription
            .events()
            .recv_timeout(Duration::from_secs(10))
            .expect("a thread snapshot")
        {
            StreamEvent::Item(StreamItem::Thread(ThreadItem::Synchronized { .. })) => {
                return subscription;
            }
            StreamEvent::Item(StreamItem::Thread(_)) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
}

fn same_transcript(left: &[TurnItem], right: &[TurnItem]) {
    let shape = |items: &[TurnItem]| {
        let mut items = items
            .iter()
            .map(|item| (item.position, item.id, item.status, item.kind.clone()))
            .collect::<Vec<_>>();
        items.sort_by_key(|item| item.0);
        items
    };
    assert_eq!(shape(left), shape(right));
}

/// One provider through the whole life of a thread.
fn thread_survives_clients_and_restarts(provider: ProviderKind, binary: &str) {
    let scratch = Scratch::new();
    let timeline = sample_timeline();
    let fake = FakeDir::new(provider.command(), binary, &timeline, RATE);
    let overrides = [(provider, fake.binary.as_path())];
    let daemon = Daemon::start(&scratch.root, &overrides);
    let instance = provider.id();

    // A turn streams to its subscriber and completes.
    let first = daemon.connect();
    let environment = first.welcome().environment.id;
    let (project_id, thread_id) = create_thread(&first, &scratch.project(), instance);
    let live = subscribe_thread(&first, thread_id);
    send_message(&first, thread_id, "go");
    let folded = follow_until_completed(&live);
    let settled = wait_settled(&first, thread_id, 1);
    let items = settled.projection.turn_items.clone();
    assert_eq!(
        assistant_text(&items),
        timeline.assistant_text(),
        "{instance}: the stored transcript"
    );
    assert!(
        folded.deltas_applied > 0,
        "{instance}: no live delta reached the subscriber"
    );
    // What the subscriber folded from snapshots and deltas is the stored
    // text, item by item.
    for item in &items {
        if let TurnItemKind::AssistantMessage { text, .. } = &item.kind {
            assert_eq!(
                folded.text.get(&item.id).map(|(_, text)| text.as_str()),
                Some(text.as_str()),
                "{instance}: folded text of {}",
                item.id
            );
        }
    }
    assert_eq!(settled.projection.runs[0].status, RunStatus::Completed);
    assert!(
        items.iter().any(|item| matches!(
            &item.kind,
            TurnItemKind::UserMessage { text, .. } if text == "go"
        )),
        "{instance}: the prompt is in the transcript"
    );

    // A second client sees the same transcript.
    let second = daemon.connect();
    same_transcript(&snapshot(&second, thread_id).projection.turn_items, &items);

    // Kill the client mid-turn, once the answer streams: the turn
    // finishes and persists without it.
    send_message(&first, thread_id, "again");
    let started = Instant::now();
    loop {
        let projection = snapshot(&second, thread_id).projection;
        let streaming = projection.runs.iter().find(|run| {
            !run.status.is_terminal()
                && projection.turn_items.iter().any(|item| {
                    item.run_id == Some(run.id)
                        && matches!(item.kind, TurnItemKind::AssistantMessage { .. })
                })
        });
        if streaming.is_some() {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "{instance}: the second turn never streamed: {:#?}",
            projection.runs
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    drop(live);
    drop(first);
    drop(second);
    let third = daemon.connect();
    let settled = wait_settled(&third, thread_id, 2);
    assert!(
        settled
            .projection
            .runs
            .iter()
            .all(|run| run.status == RunStatus::Completed),
        "{instance}: {:#?}",
        settled.projection.runs
    );
    let items = settled.projection.turn_items.clone();
    assert_eq!(
        assistant_text(&items),
        timeline.assistant_text().repeat(2),
        "{instance}: both turns are stored"
    );

    // Search finds the thread and the match inside it.
    let hits = third
        .call::<method::ThreadsSearch>(ThreadSearchParams {
            query: "done".into(),
            ..ThreadSearchParams::default()
        })
        .unwrap();
    assert_eq!(hits.len(), 1, "{instance}: {hits:#?}");
    assert_eq!(hits[0].thread_id, thread_id);
    assert_eq!(hits[0].source, SearchSource::Message);
    let found = third
        .call::<method::ThreadsFind>(ThreadFindParams {
            thread_id,
            query: "done".into(),
            anchor: None,
            window: None,
            items_around: Some(ItemsAround {
                before: 1,
                after: 1,
            }),
        })
        .unwrap();
    assert_eq!(found.matches.len(), 2, "{instance}: {found:#?}");
    let current = found.current.expect("a current match") as usize;
    assert!(
        found
            .items
            .iter()
            .any(|item| item.position == found.matches[current].position),
        "{instance}: the items around the match hold it"
    );
    drop(third);

    // A restart keeps the thread, the project and the environment.
    daemon.stop();
    let daemon = Daemon::start(&scratch.root, &overrides);
    let client = daemon.connect();
    assert_eq!(client.welcome().environment.id, environment);
    let projects = client.call::<method::ProjectsList>(NoParams {}).unwrap();
    assert_eq!(
        projects
            .iter()
            .map(|project| project.id)
            .collect::<Vec<_>>(),
        [project_id]
    );
    let restored = snapshot(&client, thread_id);
    same_transcript(&restored.projection.turn_items, &items);
    assert_eq!(restored.projection.runs.len(), 2);

    // The thread keeps working after the restart: the next turn resumes
    // the provider's conversation.
    send_message(&client, thread_id, "third");
    let settled = wait_settled(&client, thread_id, 3);
    assert_eq!(
        assistant_text(&settled.projection.turn_items),
        timeline.assistant_text().repeat(3),
        "{instance}: the turn after the restart: {:#?}",
        settled.projection.runs.last()
    );
    drop(client);
    daemon.stop();
}

#[test]
fn a_codex_thread_lives_in_the_daemon() {
    thread_survives_clients_and_restarts(ProviderKind::Codex, env!("CARGO_BIN_EXE_fake-codex"));
}

#[test]
fn a_claude_thread_lives_in_the_daemon() {
    thread_survives_clients_and_restarts(ProviderKind::Claude, env!("CARGO_BIN_EXE_fake-claude"));
}
