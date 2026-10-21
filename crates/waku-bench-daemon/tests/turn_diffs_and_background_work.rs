//! The v8 gaps the clients needed (#180), end to end: the real daemon
//! (`WakuBackend` + `DaemonHost` over protocol v8) driving the fake Codex
//! and Claude CLIs.
//!
//! - a file-changing turn has a turn diff, and the thread a full diff;
//! - rewinding the first message restores the files from before run 1;
//! - a Claude session's `system/init` slash commands reach the thread;
//! - a background process streams its output and stops;
//! - every scratch task gets a folder of its own.
#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{FakeDir, sample_timeline};
use uuid::Uuid;
use waku_client::thread_view::{ViewCache, ViewExtras};
use waku_client::v8::{StreamEvent, SubscribeOptions, V8Client, method, stream};
use waku_core::daemon::WakuBackend;
use waku_core::daemon_host::{DaemonHost, HostOptions};
use waku_core::model::ProviderKind;
use waku_core::persistence::StateStore;
use waku_core::{DaemonSettings, DaemonSettingsStore, ServerOptions};
use waku_protocol::orchestration::*;
use waku_protocol::v8::orchestration::{
    CommandOutputItem, CommandOutputSpec, DispatchCommand, DispatchParams, FullDiffParams,
    ThreadSnapshot, ThreadSnapshotParams, TurnDiffParams,
};
use waku_protocol::v8::{NoParams, ReviewDiff, ReviewDiffSource, StreamItem, TextPayload};

const TOKEN: &str = "gaps-secret";
const RATE: &str = "1";

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
        let backend =
            Arc::new(WakuBackend::new(settings, StateStore::daemon(root.join("app.db"))).unwrap());
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
}

impl Drop for Daemon {
    fn drop(&mut self) {
        self.shutdown.trigger();
        if let Some(server) = self.server.take() {
            server.join().unwrap();
        }
    }
}

/// A short data directory with a committed git project in it.
struct Scratch {
    root: PathBuf,
}

impl Scratch {
    fn new() -> Self {
        let root = PathBuf::from("/tmp").join(format!(
            "wk180-{}",
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

fn dispatch_as(client: &V8Client, command_id: &str, command: Command) -> u64 {
    client
        .call::<method::OrchestrationDispatch>(DispatchParams {
            command_id: CommandId::new(command_id).unwrap(),
            command: DispatchCommand(command),
        })
        .unwrap_or_else(|error| panic!("dispatch failed: {error:?}"))
        .sequence
}

fn dispatch(client: &V8Client, command: Command) -> u64 {
    dispatch_as(client, &format!("e2e:{}", Uuid::new_v4().simple()), command)
}

fn create_thread(client: &V8Client, project: &Path, instance: &str) -> ThreadId {
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
            title: Some(format!("{instance} e2e")),
            model_selection: ModelSelection::new(instance),
            runtime_mode: RuntimeMode::FullAccess,
            interaction_mode: InteractionMode::Default,
            workspace: WorkspaceStrategy::Root { branch: None },
            imported_native_thread: None,
        },
    );
    thread_id
}

fn send_message(client: &V8Client, thread_id: ThreadId, text: &str) -> RunId {
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
    // `decide` derives a run's id from its message (RFC 0001 amendments).
    snapshot(client, thread_id)
        .projection
        .runs
        .iter()
        .find(|run| run.user_message_id == message_id)
        .map(|run| run.id)
        .expect("the message made a run")
}

fn snapshot(client: &V8Client, thread_id: ThreadId) -> ThreadSnapshot {
    client
        .call::<method::ThreadsSnapshot>(ThreadSnapshotParams {
            thread_id,
            compact_turn_items: false,
        })
        .unwrap()
}

/// Polls until `done` holds for the thread's snapshot.
fn wait_for(
    client: &V8Client,
    thread_id: ThreadId,
    what: &str,
    done: impl Fn(&ThreadSnapshot) -> bool,
) -> ThreadSnapshot {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let snapshot = snapshot(client, thread_id);
        if done(&snapshot) {
            return snapshot;
        }
        assert!(
            Instant::now() < deadline,
            "{what}: never happened: {:#?}",
            snapshot.projection.runs
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn wait_settled(client: &V8Client, thread_id: ThreadId, runs: usize) -> ThreadSnapshot {
    wait_for(client, thread_id, "the turns settled", |snapshot| {
        snapshot.projection.runs.len() >= runs
            && snapshot
                .projection
                .runs
                .iter()
                .all(|run| run.status.is_terminal())
    })
}

fn inline_patch(diff: &ReviewDiff) -> &str {
    match &diff.patch {
        TextPayload::Inline { value } => value,
        other => panic!("a small diff comes inline: {other:?}"),
    }
}

fn file_changes(diff: &ReviewDiff, path: &str) -> (Option<u64>, Option<u64>) {
    let file = diff
        .files
        .iter()
        .find(|file| file.path == path)
        .unwrap_or_else(|| panic!("{path} is in the diff: {:?}", diff.files));
    (file.additions, file.deletions)
}

#[test]
fn turn_diffs_follow_the_files_and_the_first_message_rewinds() {
    let scratch = Scratch::new();
    let fake = FakeDir::new(
        ProviderKind::Codex.command(),
        env!("CARGO_BIN_EXE_fake-codex"),
        &sample_timeline(),
        RATE,
    );
    let daemon = Daemon::start(
        &scratch.root,
        &[(ProviderKind::Codex, fake.binary.as_path())],
    );
    let client = daemon.connect();
    let thread_id = create_thread(&client, &scratch.project(), "codex");
    let notes = scratch.project().join("notes.txt");

    let first = send_message(
        &client,
        thread_id,
        "write the notes\nwaku-fake:write notes.txt hello\\n",
    );
    wait_settled(&client, thread_id, 1);
    assert_eq!(std::fs::read_to_string(&notes).unwrap(), "hello\n");
    let turn = client
        .call::<method::ThreadsTurnDiff>(TurnDiffParams {
            thread_id,
            run_id: first,
            ignore_whitespace: false,
        })
        .unwrap();
    assert_eq!(turn.source, ReviewDiffSource::Turn { run_id: first });
    assert_eq!(file_changes(&turn, "notes.txt"), (Some(1), Some(0)));
    assert!(
        inline_patch(&turn).contains("+hello"),
        "{}",
        inline_patch(&turn)
    );
    assert!(
        !turn.files.iter().any(|file| file.path == "README.md"),
        "only what the turn changed"
    );

    let second = send_message(
        &client,
        thread_id,
        "rewrite the notes\nwaku-fake:write notes.txt second\\n",
    );
    let settled = wait_settled(&client, thread_id, 2);
    let turn = client
        .call::<method::ThreadsTurnDiff>(TurnDiffParams {
            thread_id,
            run_id: second,
            ignore_whitespace: false,
        })
        .unwrap();
    assert_eq!(file_changes(&turn, "notes.txt"), (Some(1), Some(1)));
    assert!(inline_patch(&turn).contains("-hello"));
    assert!(inline_patch(&turn).contains("+second"));
    let full = client
        .call::<method::ThreadsFullDiff>(FullDiffParams {
            thread_id,
            ignore_whitespace: false,
        })
        .unwrap();
    assert_eq!(full.source, ReviewDiffSource::Thread);
    assert_eq!(file_changes(&full, "notes.txt"), (Some(1), Some(0)));
    assert!(inline_patch(&full).contains("+second"));
    assert!(!inline_patch(&full).contains("hello"));

    // Rewind the first message the way the desktop does: its view's rewind
    // point is the turn-start checkpoint of run 1.
    let mut state = waku_client::projection::ThreadState::new();
    state.apply_snapshot(settled);
    let view = ViewCache::default()
        .build(&state, &ViewExtras::default())
        .unwrap();
    let rewind = waku_client::commands::rewind(&view, first.0).expect("the first message rewinds");
    let Command::CheckpointRollback { checkpoint_id, .. } = &rewind else {
        panic!("a rollback: {rewind:?}");
    };
    let target = state.checkpoints[checkpoint_id].clone();
    assert_eq!(target.run_id, Some(first));
    assert_eq!(target.app_run_ordinal, Some(0));
    dispatch(&client, rewind);
    let rewound = wait_for(&client, thread_id, "the rewind applied", |snapshot| {
        snapshot.projection.thread.rollback.is_none()
            && snapshot
                .projection
                .runs
                .iter()
                .all(|run| run.status == RunStatus::RolledBack)
    });
    assert!(rewound.projection.thread.rollback.is_none());
    assert!(!notes.exists(), "the files are as they were before run 1");
    assert_eq!(
        std::fs::read_to_string(scratch.project().join("README.md")).unwrap(),
        "e2e\n"
    );
}

#[test]
fn a_claude_session_reports_its_slash_commands() {
    let scratch = Scratch::new();
    let fake = FakeDir::new(
        ProviderKind::Claude.command(),
        env!("CARGO_BIN_EXE_fake-claude"),
        &sample_timeline(),
        RATE,
    );
    let daemon = Daemon::start(
        &scratch.root,
        &[(ProviderKind::Claude, fake.binary.as_path())],
    );
    let client = daemon.connect();
    let thread_id = create_thread(&client, &scratch.project(), "claude");
    send_message(&client, thread_id, "go");
    let snapshot = wait_for(&client, thread_id, "the session's commands", |snapshot| {
        snapshot
            .projection
            .provider_sessions
            .iter()
            .any(|session| !session.available_commands.is_empty())
    });
    let names = snapshot
        .projection
        .provider_sessions
        .iter()
        .flat_map(|session| &session.available_commands)
        .map(|command| command.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, waku_bench_daemon::native::claude::SLASH_COMMANDS);

    let mut state = waku_client::projection::ThreadState::new();
    state.apply_snapshot(snapshot);
    let view = ViewCache::default()
        .build(&state, &ViewExtras::default())
        .unwrap();
    let reported = view
        .available_commands
        .iter()
        .map(|command| command.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(reported, waku_bench_daemon::native::claude::SLASH_COMMANDS);
}

#[test]
fn a_background_process_streams_its_output_and_stops() {
    let scratch = Scratch::new();
    let fake = FakeDir::new(
        ProviderKind::Codex.command(),
        env!("CARGO_BIN_EXE_fake-codex"),
        &sample_timeline(),
        RATE,
    );
    let daemon = Daemon::start(
        &scratch.root,
        &[(ProviderKind::Codex, fake.binary.as_path())],
    );
    let client = daemon.connect();
    let thread_id = create_thread(&client, &scratch.project(), "codex");
    send_message(
        &client,
        thread_id,
        "start the server\nwaku-fake:background npm run dev",
    );
    let pending = |snapshot: &ThreadSnapshot| {
        snapshot
            .projection
            .provider_threads
            .iter()
            .flat_map(|provider_thread| provider_thread.pending_background_tasks.clone())
            .collect::<Vec<_>>()
    };
    let snapshot = wait_for(
        &client,
        thread_id,
        "the process went to the background",
        |snapshot| {
            snapshot
                .projection
                .runs
                .iter()
                .all(|run| run.status.is_terminal())
                && !pending(snapshot).is_empty()
        },
    );
    let task = pending(&snapshot).remove(0);
    assert_eq!(task.kind, PendingTaskKind::Command);
    assert_eq!(task.description.as_deref(), Some("npm run dev"));
    let item_id = task.item_id.expect("the task names its command item");

    // Its output, including what it printed after the turn ended.
    let output = client.subscribe::<stream::CommandOutput>(
        CommandOutputSpec { thread_id, item_id },
        SubscribeOptions::default(),
    );
    let mut text = String::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while !text.contains("still running") {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match output.events().recv_timeout(remaining) {
            Ok(StreamEvent::Item(StreamItem::CommandOutput(item))) => match item {
                CommandOutputItem::Replace { text: whole, .. } => text = whole,
                CommandOutputItem::Append { text: more } => text.push_str(&more),
                _ => {}
            },
            Ok(StreamEvent::Item(_) | StreamEvent::Resumed { .. }) => {}
            Ok(other) => panic!("the output stream ended early: {other:?} after {text:?}"),
            Err(_) => panic!("no background output within 30 s: {text:?}"),
        }
    }
    assert!(text.contains("npm run dev: started"), "{text:?}");

    // The client's view offers a stop that the daemon routes to the session.
    let mut state = waku_client::projection::ThreadState::new();
    state.apply_snapshot(snapshot);
    let view = ViewCache::default()
        .build(&state, &ViewExtras::default())
        .unwrap();
    let work = view
        .background_work
        .iter()
        .find(|item| item.key.provider_id == task.task_id)
        .expect("the process is background work");
    assert!(work.can_stop);
    let stop = waku_client::commands::stop_background_work(
        thread_id.0,
        work.key.kind,
        work.control_id.as_deref().unwrap(),
    )
    .unwrap();
    dispatch(&client, stop);
    wait_for(&client, thread_id, "the process stopped", |snapshot| {
        pending(snapshot).is_empty()
    });

    // A refresh with nothing running changes nothing.
    dispatch(
        &client,
        waku_client::commands::refresh_background_work(thread_id.0),
    );
    std::thread::sleep(Duration::from_millis(300));
    assert!(pending(&snapshot(&client, thread_id)).is_empty());
}

#[test]
fn every_scratch_task_gets_a_folder_of_its_own() {
    let scratch = Scratch::new();
    let projectless = scratch.root.join("projects");
    waku_core::projectless::set_workspace_root(Some(projectless.clone()));
    let daemon = Daemon::start(&scratch.root, &[]);
    let client = daemon.connect();
    let first = ProjectId(Uuid::new_v4());
    let second = ProjectId(Uuid::new_v4());
    let scratch_project = |project_id| Command::ProjectCreateScratch {
        project_id,
        name: None,
        prompt: Some("Fix the login page".into()),
    };
    dispatch_as(&client, "scratch:1", scratch_project(first));
    // A retry of the same command creates nothing new.
    dispatch_as(&client, "scratch:1", scratch_project(first));
    dispatch_as(&client, "scratch:2", scratch_project(second));
    let projects = client
        .call::<method::ProjectsList>(NoParams {})
        .unwrap()
        .into_iter()
        .filter(|project| project.id == first || project.id == second)
        .collect::<Vec<_>>();
    assert_eq!(projects.len(), 2);
    assert_ne!(projects[0].path, projects[1].path);
    for project in &projects {
        assert!(project.path.starts_with(&projectless), "{project:?}");
        assert!(project.path.is_dir());
        assert_eq!(project.name, "No project");
        assert!(
            project
                .path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("fix-the-login-page"),
            "{project:?}"
        );
    }
    let date_dirs = std::fs::read_dir(&projectless)
        .unwrap()
        .flat_map(|entry| std::fs::read_dir(entry.unwrap().path()).unwrap())
        .count();
    assert_eq!(date_dirs, 2, "one folder per task, the retry made none");
}
