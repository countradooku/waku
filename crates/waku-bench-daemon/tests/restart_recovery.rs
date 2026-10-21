//! Restart recovery end to end (#59): the orchestrated daemon runs as its
//! own process, is killed with SIGKILL while a turn streams from a fake
//! provider CLI, and starts again on the same data directory. The run is
//! interrupted, continued once with a visible "resumed" row, and the
//! continuation resumes the provider's conversation; refused cases leave it
//! interrupted.
#![cfg(unix)]

mod common;

use std::io::{BufRead as _, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use common::{FakeDir, sample_timeline};
use uuid::Uuid;
use waku_client::v8::{V8Client, method};
use waku_core::model::ProviderKind;
use waku_orchestrator::recovery::{CONTINUE_TEXT, PROCESS_LOSS, RESUMED_NOTICE};
use waku_protocol::orchestration::*;
use waku_protocol::v8::orchestration::{DispatchCommand, DispatchParams, ThreadSnapshotParams};

const TOKEN: &str = "restart-secret";
/// The sample timeline at this rate takes about 3 s, long enough to kill
/// the daemon while the answer streams.
const RATE: &str = "0.004";

/// A short data directory (the socket path must fit `sun_path`) with a git
/// project in it.
struct Scratch {
    root: PathBuf,
}

impl Scratch {
    fn new() -> Self {
        let root = PathBuf::from("/tmp").join(format!(
            "wk59-{}",
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
            let status = Command::new("git")
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

    fn socket(&self) -> PathBuf {
        self.root.join("d.sock")
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// The daemon process and what its ready line said.
struct Daemon {
    child: Child,
    recovered: usize,
    continuations: usize,
}

impl Daemon {
    fn start(scratch: &Scratch, provider: ProviderKind, binary: &Path, continue_: bool) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_orchestrated-daemon"));
        command
            .arg("--root")
            .arg(&scratch.root)
            .arg("--socket")
            .arg(scratch.socket())
            .arg("--token")
            .arg(TOKEN)
            .arg("--provider")
            .arg(format!("{}={}", provider.id(), binary.display()))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        if !continue_ {
            command.arg("--no-continue");
        }
        let mut child = command.spawn().unwrap();
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let ready: serde_json::Value =
            serde_json::from_str(&line).unwrap_or_else(|_| panic!("ready line: {line:?}"));
        Self {
            child,
            recovered: ready["recovered"].as_array().map_or(0, Vec::len),
            continuations: ready["continuations"].as_u64().unwrap_or(0) as usize,
        }
    }

    fn connect(&self, scratch: &Scratch) -> V8Client {
        V8Client::connect(&format!("unix:{}", scratch.socket().display()), TOKEN).unwrap()
    }

    /// `kill -9`.
    fn kill(mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn dispatch(client: &V8Client, command: Command_) -> u64 {
    client
        .call::<method::OrchestrationDispatch>(DispatchParams {
            command_id: CommandId::new(format!("e2e:{}", Uuid::new_v4().simple())).unwrap(),
            command: DispatchCommand(command),
        })
        .unwrap_or_else(|error| panic!("dispatch failed: {error:?}"))
        .sequence
}

type Command_ = waku_protocol::orchestration::Command;

fn create_thread(client: &V8Client, project: &Path, instance: &str) -> ThreadId {
    let project_id = ProjectId(Uuid::new_v4());
    let thread_id = ThreadId(Uuid::new_v4());
    dispatch(
        client,
        Command_::ProjectCreate {
            project_id,
            name: "e2e".into(),
            path: project.to_owned(),
            order_key: None,
        },
    );
    dispatch(
        client,
        Command_::ThreadCreate {
            thread_id,
            project_id,
            title: Some(format!("{instance} restart")),
            model_selection: ModelSelection::new(instance),
            runtime_mode: RuntimeMode::FullAccess,
            interaction_mode: InteractionMode::Default,
            workspace: WorkspaceStrategy::Root { branch: None },
            imported_native_thread: None,
        },
    );
    thread_id
}

fn send_message(client: &V8Client, thread_id: ThreadId, text: &str) {
    dispatch(
        client,
        Command_::MessageDispatch {
            thread_id,
            message_id: MessageId(Uuid::new_v4()),
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
}

fn projection(
    client: &V8Client,
    thread_id: ThreadId,
) -> waku_protocol::v8::orchestration::ThreadProjection {
    client
        .call::<method::ThreadsSnapshot>(ThreadSnapshotParams {
            thread_id,
            compact_turn_items: false,
        })
        .unwrap()
        .projection
}

/// Waits until a run streams its answer.
fn wait_streaming(client: &V8Client, thread_id: ThreadId) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let projection = projection(client, thread_id);
        let streaming = projection.runs.iter().any(|run| {
            !run.status.is_terminal()
                && projection.turn_items.iter().any(|item| {
                    item.run_id == Some(run.id)
                        && matches!(item.kind, TurnItemKind::AssistantMessage { .. })
                })
        });
        if streaming {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the turn never streamed: {:#?}",
            projection.runs
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Waits until every run is terminal and there are at least `runs`.
fn wait_settled(
    client: &V8Client,
    thread_id: ThreadId,
    runs: usize,
) -> waku_protocol::v8::orchestration::ThreadProjection {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let projection = projection(client, thread_id);
        if projection.runs.len() >= runs
            && projection.runs.iter().all(|run| run.status.is_terminal())
        {
            return projection;
        }
        assert!(
            Instant::now() < deadline,
            "the thread never settled: {:#?}",
            projection.runs
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn lifecycle_rows(items: &[TurnItem], lifecycle: LifecycleNotice) -> Vec<&TurnItem> {
    items
        .iter()
        .filter(|item| {
            matches!(
                &item.kind,
                TurnItemKind::SystemNotice { lifecycle: Some(kind), .. } if *kind == lifecycle
            )
        })
        .collect()
}

fn killed_mid_turn_and_continued_once(provider: ProviderKind, binary: &str) {
    let scratch = Scratch::new();
    let timeline = sample_timeline();
    let fake = FakeDir::new(provider.command(), binary, &timeline, RATE);
    let instance = provider.id();

    let daemon = Daemon::start(&scratch, provider, &fake.binary, true);
    let client = daemon.connect(&scratch);
    let thread_id = create_thread(&client, &scratch.project(), instance);
    send_message(&client, thread_id, "go");
    wait_streaming(&client, thread_id);
    drop(client);
    daemon.kill();

    let daemon = Daemon::start(&scratch, provider, &fake.binary, true);
    assert!(daemon.recovered >= 1, "{instance}: nothing was recovered");
    assert_eq!(daemon.continuations, 1, "{instance}");
    let client = daemon.connect(&scratch);
    let settled = wait_settled(&client, thread_id, 2);
    let mut runs = settled.runs.clone();
    runs.sort_by_key(|run| run.ordinal);
    assert_eq!(runs.len(), 2, "{instance}: continued once: {runs:#?}");
    let (interrupted, continued) = (&runs[0], &runs[1]);
    assert_eq!(interrupted.status, RunStatus::Interrupted, "{instance}");
    assert_eq!(
        interrupted
            .failure
            .as_ref()
            .and_then(|failure| failure.code.as_deref()),
        Some(PROCESS_LOSS)
    );
    assert_eq!(continued.status, RunStatus::Completed, "{instance}");
    assert_eq!(
        continued.continuation,
        Some(RunContinuation {
            kind: ContinuationKind::Restart,
            of_run_id: interrupted.id,
        })
    );
    assert_eq!(
        continued.provider_thread_id, interrupted.provider_thread_id,
        "{instance}: the provider conversation is resumed"
    );
    let items = &settled.turn_items;
    assert_eq!(
        lifecycle_rows(items, LifecycleNotice::RestartInterrupted).len(),
        1
    );
    let resumed = lifecycle_rows(items, LifecycleNotice::RestartResumed);
    assert_eq!(resumed.len(), 1, "{instance}: one resumed row");
    assert_eq!(resumed[0].run_id, Some(continued.id));
    assert!(matches!(
        &resumed[0].kind,
        TurnItemKind::SystemNotice { message, .. } if message == RESUMED_NOTICE
    ));
    assert!(
        items.iter().any(|item| item.run_id == Some(continued.id)
            && matches!(
                &item.kind,
                TurnItemKind::UserMessage { text, created_by: Actor::System, .. }
                    if text == CONTINUE_TEXT
            )),
        "{instance}: the continuation message"
    );
    assert!(
        items.iter().any(|item| item.run_id == Some(continued.id)
            && matches!(&item.kind, TurnItemKind::AssistantMessage { text, .. } if !text.is_empty())),
        "{instance}: the continuation answered"
    );
    drop(client);

    // Another crash after the continuation finished continues nothing.
    daemon.kill();
    let daemon = Daemon::start(&scratch, provider, &fake.binary, true);
    assert_eq!(daemon.continuations, 0);
    let client = daemon.connect(&scratch);
    assert_eq!(projection(&client, thread_id).runs.len(), 2);
}

#[test]
fn a_codex_turn_killed_with_the_daemon_is_continued_once() {
    killed_mid_turn_and_continued_once(ProviderKind::Codex, env!("CARGO_BIN_EXE_fake-codex"));
}

#[test]
fn a_claude_turn_killed_with_the_daemon_is_continued_once() {
    killed_mid_turn_and_continued_once(ProviderKind::Claude, env!("CARGO_BIN_EXE_fake-claude"));
}

/// Kills the daemon mid-turn after `prepare` and restarts it; the run must
/// end interrupted with no continuation, and the notice says why.
fn refused(prepare: impl FnOnce(&V8Client, ThreadId), continue_: bool, reason: &str) {
    let scratch = Scratch::new();
    let timeline = sample_timeline();
    let provider = ProviderKind::Codex;
    let fake = FakeDir::new(
        provider.command(),
        env!("CARGO_BIN_EXE_fake-codex"),
        &timeline,
        RATE,
    );
    let daemon = Daemon::start(&scratch, provider, &fake.binary, true);
    let client = daemon.connect(&scratch);
    let thread_id = create_thread(&client, &scratch.project(), provider.id());
    send_message(&client, thread_id, "go");
    wait_streaming(&client, thread_id);
    prepare(&client, thread_id);
    drop(client);
    daemon.kill();

    let daemon = Daemon::start(&scratch, provider, &fake.binary, continue_);
    assert_eq!(daemon.continuations, 0, "{reason}");
    let client = daemon.connect(&scratch);
    let settled = wait_settled(&client, thread_id, 1);
    assert_eq!(settled.runs.len(), 1, "{reason}: {:#?}", settled.runs);
    assert_eq!(settled.runs[0].status, RunStatus::Interrupted);
    let notices = lifecycle_rows(&settled.turn_items, LifecycleNotice::RestartInterrupted);
    assert_eq!(notices.len(), 1);
    assert!(
        matches!(&notices[0].kind, TurnItemKind::SystemNotice { message, .. } if message.contains(reason)),
        "{:?}",
        notices[0].kind
    );
    assert!(lifecycle_rows(&settled.turn_items, LifecycleNotice::RestartResumed).is_empty());
}

#[test]
fn a_restart_does_not_continue_when_the_setting_is_off() {
    refused(|_, _| {}, false, "turned off");
}

#[test]
fn a_restart_does_not_continue_a_snoozed_thread() {
    refused(
        |client, thread_id| {
            let until = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64
                + 3_600_000;
            dispatch(client, Command_::ThreadSnooze { thread_id, until });
        },
        true,
        "snoozed",
    );
}

#[test]
fn a_restart_does_not_continue_an_archived_thread() {
    refused(
        |client, thread_id| {
            dispatch(client, Command_::ThreadArchive { thread_id });
        },
        true,
        "archived",
    );
}

/// #58: the desktop attaches to a daemon already running on the local
/// socket (the background service) instead of spawning one, and quitting
/// the desktop mid-turn leaves the turn running: a later launch attaches
/// again and finds it completed.
#[test]
fn the_desktop_attaches_to_a_running_service_and_a_turn_outlives_it() {
    let scratch = Scratch::new();
    let timeline = sample_timeline();
    let provider = ProviderKind::Codex;
    let fake = FakeDir::new(
        provider.command(),
        env!("CARGO_BIN_EXE_fake-codex"),
        &timeline,
        RATE,
    );
    let mut daemon = Daemon::start(&scratch, provider, &fake.binary, true);
    // What `waku-daemon --service` keeps next to its socket.
    std::fs::write(scratch.root.join("local.token"), TOKEN).unwrap();
    // SAFETY: only this test reads the variable (through the supervisor's
    // identity paths); the other tests pass their socket explicitly.
    unsafe { std::env::set_var(waku_protocol::DAEMON_SOCKET_ENV, scratch.socket()) };
    assert_eq!(
        waku_protocol::identity::local_token_path(),
        scratch.root.join("local.token")
    );
    // The executable fails to start without its arguments: attaching must
    // not spawn it.
    let executable = PathBuf::from(env!("CARGO_BIN_EXE_orchestrated-daemon"));
    let desktop = waku_client::DaemonSupervisor::spawn_configured(
        &executable,
        false,
        waku_client::DaemonExposureSettings::default(),
    )
    .unwrap();
    assert!(desktop.is_attached_service());

    let client = daemon.connect(&scratch);
    let thread_id = create_thread(&client, &scratch.project(), provider.id());
    send_message(&client, thread_id, "go");
    wait_streaming(&client, thread_id);
    // Quit the desktop mid-turn.
    drop(client);
    drop(desktop);
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        daemon.child.try_wait().unwrap().is_none(),
        "the service outlives the desktop"
    );

    // Reopen: attach again and find the turn completed.
    let desktop = waku_client::DaemonSupervisor::spawn_configured(
        &executable,
        false,
        waku_client::DaemonExposureSettings::default(),
    )
    .unwrap();
    assert!(desktop.is_attached_service());
    let client = daemon.connect(&scratch);
    let settled = wait_settled(&client, thread_id, 1);
    assert_eq!(settled.runs.len(), 1);
    assert_eq!(settled.runs[0].status, RunStatus::Completed);
    assert!(settled.turn_items.iter().any(|item| matches!(
        &item.kind,
        TurnItemKind::AssistantMessage { text, .. } if !text.is_empty()
    )));
    drop(client);
    drop(desktop);
    unsafe { std::env::remove_var(waku_protocol::DAEMON_SOCKET_ENV) };
}
