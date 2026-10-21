//! The desktop's client layer against the real daemon (#55): the
//! projection store, the render views and the command builders the desktop
//! uses, with no window. The daemon is composed as `waku-daemon` composes
//! it (orchestrator, effect workers, provider session manager) and drives
//! the fake Codex CLI.
//!
//! What the desktop shows must come from the shell and thread streams
//! alone: a thread created and prompted through `orchestration.dispatch`
//! streams into the view (live deltas folded in order), a second client sees
//! the same view, a client killed mid-turn finds the finished turn when it
//! comes back, a daemon restart is survived by resubscribing from cursors,
//! and older history pages in on demand.
#![cfg(unix)]

mod common;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{FakeDir, sample_timeline};
use crossbeam_channel::{Receiver, unbounded};
use uuid::Uuid;
use waku_client::commands::{self, NewThread, Outgoing, Selection, SendIntent};
use waku_client::store::{ProjectionStore, StoreConfig, StoreNotice, StoreUpdates, ThreadChange};
use waku_client::thread_view::{ShellView, ThreadView};
use waku_client::v8::V8Client;
use waku_core::daemon::WakuBackend;
use waku_core::daemon_host::{DaemonHost, HostOptions};
use waku_core::model::{ActivityKind, MessageRole, ProviderKind, SessionStatus, TurnStatus};
use waku_core::persistence::StateStore;
use waku_core::{DaemonSettings, DaemonSettingsStore, ServerOptions};

const TOKEN: &str = "desktop-client-secret";
/// The sample timeline spans 12 ms; at this rate a turn takes 2.4 s, long
/// enough to stream deltas and to drop a client mid-turn.
const RATE: &str = "0.005";

struct Daemon {
    socket: PathBuf,
    shutdown: waku_core::ShutdownSignal,
    server: Option<std::thread::JoinHandle<()>>,
}

impl Daemon {
    fn start(root: &Path, codex: &Path) -> Self {
        let settings = DaemonSettingsStore::open(root.join("settings.json")).unwrap();
        let mut document = DaemonSettings::default();
        document
            .provider_binary_overrides
            .insert(ProviderKind::Codex, codex.display().to_string());
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

struct Scratch {
    root: PathBuf,
}

impl Scratch {
    fn new() -> Self {
        let root = PathBuf::from("/tmp").join(format!(
            "wk55d-{}",
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

/// What the desktop keeps of the store: the newest shell and thread views,
/// the change flags of every drain, and the dispatch outcomes.
struct Desktop {
    store: ProjectionStore,
    wake: Receiver<()>,
    shell: Arc<ShellView>,
    views: HashMap<Uuid, Arc<ThreadView>>,
    changes: Vec<(Uuid, ThreadChange)>,
    notices: Vec<StoreNotice>,
}

impl Desktop {
    /// Connect the way the desktop does: through a connector the store
    /// calls again after every disconnect.
    fn connect(socket: &Path) -> Self {
        let (wake_tx, wake) = unbounded();
        let address = format!("unix:{}", socket.display());
        let store = ProjectionStore::start(
            Box::new(move || V8Client::connect(&address, TOKEN)),
            move || {
                let _ = wake_tx.try_send(());
            },
            StoreConfig::default(),
        );
        assert!(
            store.wait_ready(&[], Duration::from_secs(10)),
            "the shell never synchronized"
        );
        let mut desktop = Self {
            store,
            wake,
            shell: Arc::default(),
            views: HashMap::new(),
            changes: Vec::new(),
            notices: Vec::new(),
        };
        desktop.drain();
        desktop
    }

    fn drain(&mut self) {
        let StoreUpdates {
            shell,
            threads,
            notices,
            ..
        } = self.store.take_updates();
        if let Some(shell) = shell {
            self.shell = shell;
        }
        for (id, update) in threads {
            self.changes.push((id, update.change));
            match update.view {
                Some(view) => {
                    self.views.insert(id, view);
                }
                None => {
                    self.views.remove(&id);
                }
            }
        }
        for notice in notices {
            if let StoreNotice::DispatchFailed {
                command_type,
                error,
                ..
            } = &notice
            {
                panic!("{command_type} failed: {error}");
            }
            self.notices.push(notice);
        }
    }

    /// Drain until `done` holds, like the desktop's event pump.
    fn until(&mut self, what: &str, done: impl Fn(&Self) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            self.drain();
            if done(self) {
                return;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "timed out waiting for {what}: {:#?}",
                self.views
                    .values()
                    .map(|view| (
                        view.status,
                        view.turns
                            .iter()
                            .map(|turn| (turn.turn_count, turn.status))
                            .collect::<Vec<_>>(),
                        view.queued_messages.len(),
                        view.last_error.clone(),
                        prompts(view),
                    ))
                    .collect::<Vec<_>>()
            );
            let _ = self
                .wake
                .recv_timeout(remaining.min(Duration::from_millis(200)));
        }
    }

    fn view(&self, id: Uuid) -> &ThreadView {
        self.views.get(&id).expect("an open thread view")
    }

    fn settled(&self, id: Uuid, turns: usize) -> bool {
        self.views.get(&id).is_some_and(|view| {
            view.turns.len() >= turns
                && view
                    .turns
                    .iter()
                    .all(|turn| turn.status != TurnStatus::Running)
                && view.status == SessionStatus::Idle
                && view.queued_messages.is_empty()
        })
    }
}

fn answers(view: &ThreadView) -> String {
    view.messages
        .iter()
        .filter(|message| message.role == MessageRole::Assistant)
        .map(|message| message.content.as_str())
        .collect()
}

fn prompts(view: &ThreadView) -> Vec<String> {
    view.messages
        .iter()
        .filter(|message| message.role == MessageRole::User)
        .map(|message| message.content.clone())
        .collect()
}

/// The rows a view renders, for comparing two clients.
fn rows(view: &ThreadView) -> Vec<String> {
    let mut rows = view
        .messages
        .iter()
        .map(|message| format!("{:?}:{}", message.role, message.content))
        .collect::<Vec<_>>();
    for block in &view.transcript_blocks {
        for activity in &block.activities {
            rows.push(format!(
                "{}:{:?}:{}",
                block.after_message, activity.kind, activity.complete
            ));
        }
    }
    rows
}

fn send(desktop: &Desktop, thread_id: Uuid, text: &str, intent: SendIntent) {
    let view = desktop.view(thread_id);
    let outgoing = Outgoing {
        text: text.into(),
        ..Outgoing::default()
    };
    desktop.store.dispatch(commands::send_message(
        thread_id,
        &outgoing,
        commands::dispatch_mode(view, intent),
        None,
    ));
}

/// Create a project and a thread, sending its first prompt, the way the
/// desktop's new-task page does.
fn start_task(desktop: &mut Desktop, project: &Path, prompt: &str) -> Uuid {
    let project_id = Uuid::new_v4();
    desktop.store.dispatch(commands::create_project(
        project_id,
        project.to_owned(),
        "e2e".into(),
    ));
    desktop.until("the project row", |desktop| {
        desktop
            .shell
            .projects
            .iter()
            .any(|row| row.id == project_id)
    });
    let thread_id = Uuid::new_v4();
    let new = NewThread {
        thread_id,
        project_id,
        selection: Selection {
            provider: ProviderKind::Codex,
            ..Selection::default()
        },
        runtime_mode: waku_core::model::RuntimeMode::FullAccess,
        workspace: waku_core::model::SessionWorkspace::Local,
    };
    let outgoing = Outgoing {
        text: prompt.into(),
        ..Outgoing::default()
    };
    // Opened before the daemon knows it: the store waits for the creation.
    desktop.store.open_thread(thread_id);
    desktop
        .store
        .dispatch_all(commands::start_thread(&new, &outgoing).into());
    thread_id
}

#[test]
fn the_desktop_renders_threads_from_the_streams() {
    let scratch = Scratch::new();
    let timeline = sample_timeline();
    let fake = FakeDir::new("codex", env!("CARGO_BIN_EXE_fake-codex"), &timeline, RATE);
    let daemon = Daemon::start(&scratch.root, &fake.binary);

    // A new task streams into its view and settles.
    let mut first = Desktop::connect(&daemon.socket);
    let thread_id = start_task(&mut first, &scratch.project(), "go");
    first.until("the first turn", |desktop| desktop.settled(thread_id, 1));
    let view = first.view(thread_id).clone();
    assert_eq!(prompts(&view), ["go"]);
    assert_eq!(answers(&view), timeline.assistant_text());
    assert_eq!(view.turns[0].status, TurnStatus::Completed);
    let activities = view
        .transcript_blocks
        .iter()
        .flat_map(|block| block.activities.iter().map(|activity| activity.kind))
        .collect::<Vec<_>>();
    for kind in [
        ActivityKind::Reasoning,
        ActivityKind::Command,
        ActivityKind::FileChange,
    ] {
        assert!(activities.contains(&kind), "{kind:?} in {activities:?}");
    }
    assert!(
        first
            .changes
            .iter()
            .any(|(id, change)| *id == thread_id && change.text_only()),
        "live text arrived as text-only updates"
    );
    let row = first.shell.thread(thread_id).expect("a sidebar row");
    assert_eq!(row.status, SessionStatus::Idle);
    assert!(row.last_reply_at.is_some());

    // A second desktop renders the same rows.
    let mut second = Desktop::connect(&daemon.socket);
    second.store.open_thread(thread_id);
    second.until("the second client's view", |desktop| {
        desktop.settled(thread_id, 1)
    });
    assert_eq!(rows(second.view(thread_id)), rows(first.view(thread_id)));

    // Kill the first desktop mid-turn: the turn finishes without it, and a
    // desktop that comes back renders the whole result.
    send(&first, thread_id, "again", SendIntent::Submit);
    first.until("the second turn to stream", |desktop| {
        desktop.views.get(&thread_id).is_some_and(|view| {
            view.is_busy()
                && view
                    .messages
                    .last()
                    .is_some_and(|message| message.role == MessageRole::Assistant)
        })
    });
    first.store.shutdown();
    drop(first);
    second.until("the second client to see the second turn", |desktop| {
        desktop.settled(thread_id, 2)
    });
    let mut third = Desktop::connect(&daemon.socket);
    third.store.open_thread(thread_id);
    third.until("the returning client's view", |desktop| {
        desktop.settled(thread_id, 2)
    });
    assert_eq!(prompts(third.view(thread_id)), ["go", "again"]);
    assert_eq!(
        answers(third.view(thread_id)),
        timeline.assistant_text().repeat(2)
    );
    assert_eq!(rows(third.view(thread_id)), rows(second.view(thread_id)));

    // Queued messages wait in the view's queue, then run in order.
    send(&third, thread_id, "three", SendIntent::Submit);
    third.until("the third turn to start", |desktop| {
        desktop.view(thread_id).is_busy()
    });
    send(&third, thread_id, "four", SendIntent::Submit);
    third.until("the queued fourth message", |desktop| {
        desktop.view(thread_id).queued_messages.len() == 1
    });
    assert_eq!(third.view(thread_id).queued_messages[0].content, "four");
    third.until("both queued turns", |desktop| desktop.settled(thread_id, 4));
    assert_eq!(
        prompts(third.view(thread_id)),
        ["go", "again", "three", "four"]
    );

    // Renames and archives reach every client through the shell.
    second
        .store
        .dispatch(commands::rename(thread_id, "Renamed").unwrap());
    third.until("the renamed row", |desktop| {
        desktop
            .shell
            .thread(thread_id)
            .is_some_and(|row| row.display_title() == "Renamed")
    });

    // A daemon restart: the store reconnects and resubscribes from its
    // cursors, and the thread keeps working.
    drop(second);
    daemon.stop();
    let daemon = Daemon::start(&scratch.root, &fake.binary);
    send(&third, thread_id, "five", SendIntent::Submit);
    third.until("a turn after the restart", |desktop| {
        desktop.settled(thread_id, 5)
    });
    assert_eq!(
        answers(third.view(thread_id)),
        timeline.assistant_text().repeat(5)
    );

    third.store.dispatch(commands::archive(thread_id));
    third.until("the archived row to leave the active shell", |desktop| {
        desktop.shell.thread(thread_id).is_none()
    });
    third.store.shutdown();
    daemon.stop();
}

#[test]
fn older_history_pages_in_on_demand() {
    let scratch = Scratch::new();
    let timeline = sample_timeline();
    // Fast turns: a dozen of them fit in a few seconds.
    let fake = FakeDir::new("codex", env!("CARGO_BIN_EXE_fake-codex"), &timeline, "0.05");
    let daemon = Daemon::start(&scratch.root, &fake.binary);
    let mut writer = Desktop::connect(&daemon.socket);
    let thread_id = start_task(&mut writer, &scratch.project(), "turn 0");
    writer.until("the first turn", |desktop| desktop.settled(thread_id, 1));
    const TURNS: usize = 12;
    for turn in 1..TURNS {
        send(
            &writer,
            thread_id,
            &format!("turn {turn}"),
            SendIntent::Queue,
        );
    }
    writer.until("every queued turn", |desktop| {
        desktop.settled(thread_id, TURNS)
    });
    writer.store.shutdown();

    // A fresh desktop opens a bounded window and pages the rest in.
    let mut reader = Desktop::connect(&daemon.socket);
    reader.store.open_thread(thread_id);
    reader.until("the bounded snapshot", |desktop| {
        desktop
            .views
            .get(&thread_id)
            .is_some_and(|view| view.synchronized)
    });
    let opened = prompts(reader.view(thread_id));
    assert!(
        opened.len() < TURNS,
        "the snapshot is bounded: {}",
        opened.len()
    );
    assert!(reader.view(thread_id).has_more_history);
    while reader.view(thread_id).has_more_history {
        let before = prompts(reader.view(thread_id)).len();
        reader.store.load_older(thread_id);
        reader.until("an older page", |desktop| {
            let view = desktop.view(thread_id);
            !view.history_loading && (prompts(view).len() > before || !view.has_more_history)
        });
    }
    let expected = (0..TURNS)
        .map(|turn| format!("turn {turn}"))
        .collect::<Vec<_>>();
    assert_eq!(prompts(reader.view(thread_id)), expected);
    assert_eq!(reader.view(thread_id).turns.len(), TURNS);
    reader.store.shutdown();
    daemon.stop();
}
