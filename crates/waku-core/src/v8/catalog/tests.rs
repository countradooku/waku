//! The catalog end to end: the Rust v8 client against a real daemon — the
//! real [`WakuBackend`] over a temporary state directory, a git repository
//! with a bare remote, and a scripted terminal shell.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use uuid::Uuid;
use waku_client::v8::{
    CallError, StreamEvent, SubscribeOptions, Subscription, V8Client, method, stream,
};
use waku_protocol::orchestration::ModelSelection;
use waku_protocol::v8::*;

use crate::daemon::WakuBackend;
use crate::server::{Listeners, ServerOptions, ShutdownSignal, serve_listeners};
use crate::settings::DaemonSettingsStore;

const TOKEN: &str = "catalog-secret";
const WAIT: Duration = Duration::from_secs(10);

fn git(cwd: &Path, args: &[&str]) -> String {
    let output = crate::git_workflow::git(cwd, args)
        .unwrap_or_else(|error| panic!("git {args:?} in {}: {error:#}", cwd.display()));
    String::from_utf8_lossy(&output).trim().to_owned()
}

fn configure_identity(repository: &Path) {
    git(repository, &["config", "user.name", "Waku Tests"]);
    git(repository, &["config", "user.email", "waku@example.com"]);
    git(repository, &["config", "commit.gpgsign", "false"]);
}

/// An orchestrator in `root` holding the project `demo` at `path` and one
/// thread in it: the daemon's source of projects and threads (#55).
fn orchestration(
    root: &Path,
    project_id: Uuid,
    path: &Path,
    thread_id: Uuid,
) -> Arc<crate::Orchestration> {
    use waku_protocol::orchestration::{
        Command as OrchestrationCommand, InteractionMode, ProjectId, RuntimeMode, ThreadId,
        WorkspaceStrategy,
    };
    let orchestrator = waku_orchestrator::Orchestrator::open(
        waku_orchestrator::OrchestratorConfig::new(root.join("state.sqlite")),
        waku_orchestrator::Dependencies {
            clock: Arc::new(waku_orchestrator::ManualClock::new(1_700_000_000_000)),
            ids: Box::new(waku_orchestrator::SequentialIds::default()),
            capabilities: Arc::new(waku_orchestrator::NoCapabilities),
        },
    )
    .unwrap();
    let dispatch = |id: &str, command| dispatch(&orchestrator, id, command);
    dispatch(
        "project-demo",
        OrchestrationCommand::ProjectCreate {
            project_id: ProjectId(project_id),
            name: "demo".into(),
            path: path.to_owned(),
            order_key: None,
        },
    );
    dispatch(
        "thread-demo",
        OrchestrationCommand::ThreadCreate {
            thread_id: ThreadId(thread_id),
            project_id: ProjectId(project_id),
            title: None,
            model_selection: ModelSelection::new("codex"),
            runtime_mode: RuntimeMode::FullAccess,
            interaction_mode: InteractionMode::Default,
            workspace: WorkspaceStrategy::Root { branch: None },
            imported_native_thread: None,
        },
    );
    crate::Orchestration::new(orchestrator, Default::default())
}

/// Dispatches `command` as the user.
fn dispatch(
    orchestrator: &waku_orchestrator::Orchestrator,
    id: &str,
    command: waku_protocol::orchestration::Command,
) {
    use waku_protocol::orchestration::{Actor, CommandEnvelope, CommandId, CreationSource};
    orchestrator
        .dispatch(CommandEnvelope {
            command_id: CommandId::new(id).unwrap(),
            issued_by: Actor::User,
            source: CreationSource::Desktop,
            command,
        })
        .unwrap();
}

struct Daemon {
    root: PathBuf,
    project: PathBuf,
    project_id: Uuid,
    /// A thread of the project, in the orchestrator.
    thread_id: Uuid,
    socket: PathBuf,
    tcp: std::net::SocketAddr,
    shutdown: ShutdownSignal,
    server: Option<std::thread::JoinHandle<()>>,
}

impl Daemon {
    fn start() -> Self {
        // Short, so the socket path fits in `sun_path`.
        let root = PathBuf::from("/tmp").join(format!(
            "wkc-{}",
            &Uuid::new_v4().simple().to_string()[..10]
        ));
        let project = root.join("project");
        let remote = root.join("remote.git");
        std::fs::create_dir_all(project.join("src")).unwrap();
        std::fs::create_dir_all(project.join(".waku/commands")).unwrap();
        std::fs::create_dir_all(project.join(".claude/skills/demo")).unwrap();
        std::fs::write(project.join("README.md"), "# demo\n").unwrap();
        std::fs::write(
            project.join("src/main.rs"),
            "fn main() {\n    println!(\"needle in a haystack\");\n}\n",
        )
        .unwrap();
        std::fs::write(
            project.join(".waku/commands/greet.md"),
            "---\ndescription: Say hello\n---\nSay hello to $ARGUMENTS\n",
        )
        .unwrap();
        std::fs::write(
            project.join(".claude/skills/demo/SKILL.md"),
            "---\nname: demo\ndescription: A demo skill\n---\nDo the demo.\n",
        )
        .unwrap();
        git(&root, &["init", "--bare", "-b", "main", "remote.git"]);
        git(&project, &["init", "-b", "main"]);
        configure_identity(&project);
        git(&project, &["add", "-A", "--", "."]);
        git(&project, &["commit", "-m", "initial"]);
        git(
            &project,
            &["remote", "add", "origin", remote.to_str().unwrap()],
        );
        git(&project, &["push", "-u", "origin", "main"]);

        let shell = alacritty_terminal::tty::Shell::new(
            "/bin/sh".into(),
            vec!["-c".into(), "printf 'ready\\n'; exec cat".into()],
        );
        let backend = WakuBackend::new(
            DaemonSettingsStore::open(root.join("settings.json")).unwrap(),
            root.to_path_buf(),
        )
        .unwrap()
        .with_terminal_shell(shell);
        let project_id = Uuid::new_v4();
        let thread_id = Uuid::new_v4();
        let orchestration = orchestration(&root, project_id, &project, thread_id);
        let socket = root.join("d.sock");
        let local = crate::LocalListener::bind(&socket).unwrap();
        let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = tcp.local_addr().unwrap();
        let shutdown = ShutdownSignal::new();
        let server_shutdown = shutdown.clone();
        let server = std::thread::spawn(move || {
            serve_listeners(
                Listeners {
                    tcp: Some(tcp),
                    local: Some(local),
                },
                TOKEN.into(),
                Arc::new(backend),
                server_shutdown,
                ServerOptions {
                    allow_shutdown: true,
                    orchestration: Some(orchestration.clone()),
                    ..ServerOptions::default()
                },
            )
            .unwrap();
            orchestration.shutdown();
        });
        Self {
            root,
            project,
            project_id,
            thread_id,
            socket,
            tcp: address,
            shutdown,
            server: Some(server),
        }
    }

    fn connect(&self) -> V8Client {
        V8Client::connect(&format!("unix:{}", self.socket.display()), TOKEN).unwrap()
    }

    fn connect_tcp(&self) -> V8Client {
        V8Client::connect(&self.tcp.to_string(), TOKEN).unwrap()
    }

    fn root(&self) -> RootRef {
        RootRef::Project {
            project_id: self.project_id,
        }
    }

    /// Point the managed worktree folder into the test directory.
    fn use_test_worktrees(&self, client: &V8Client) {
        let storage = StorageSettings {
            worktrees_directory: Some(self.root.join("worktrees").display().to_string()),
            ..StorageSettings::default()
        };
        client
            .call::<method::SettingsUpdate>(SettingsUpdateParams {
                base_revision: None,
                patch: SettingsPatch {
                    storage: Some(storage),
                    ..SettingsPatch::default()
                },
                op_id: Uuid::new_v4(),
            })
            .unwrap();
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

fn op() -> Uuid {
    Uuid::new_v4()
}

fn remote_code<T: std::fmt::Debug>(result: Result<T, CallError>) -> Error {
    match result {
        Err(CallError::Remote(error)) => error,
        other => panic!("expected a remote error, got {other:?}"),
    }
}

fn next_event(subscription: &Subscription) -> StreamEvent {
    subscription
        .events()
        .recv_timeout(WAIT)
        .expect("a stream event")
}

fn next_item(subscription: &Subscription) -> StreamItem {
    loop {
        match next_event(subscription) {
            StreamEvent::Item(item) => return item,
            StreamEvent::Data { .. } => {}
            other => panic!("expected an item, got {other:?}"),
        }
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

#[test]
fn files_tree_search_write_and_browse() {
    let daemon = Daemon::start();
    let client = daemon.connect();
    let root = daemon.root();

    let tree = client
        .call::<method::FilesTree>(FilesTreeParams {
            root: root.clone(),
            expanded: vec!["src".into()],
        })
        .unwrap();
    let main = tree
        .iter()
        .find(|entry| entry.path == "src/main.rs")
        .unwrap();
    assert_eq!((main.depth, main.is_dir), (1, false));
    assert!(
        tree.iter()
            .any(|entry| entry.path == "src" && entry.expanded)
    );

    let entries = client
        .call::<method::FilesSearchEntries>(FilesSearchEntriesParams {
            root: root.clone(),
            query: "mainrs".into(),
            limit: Some(5),
        })
        .unwrap();
    assert_eq!(entries[0].path, "src/main.rs");

    let hits = client
        .call::<method::FilesSearchContents>(FilesSearchContentsParams {
            root: root.clone(),
            query: "NEEDLE".into(),
            regex: false,
            case_sensitive: false,
            limit: None,
        })
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(
        (hits[0].path.as_str(), hits[0].line, hits[0].column),
        ("src/main.rs", 2, 15)
    );
    let regex = client
        .call::<method::FilesSearchContents>(FilesSearchContentsParams {
            root: root.clone(),
            query: r"fn \w+\(".into(),
            regex: true,
            case_sensitive: true,
            limit: None,
        })
        .unwrap();
    assert_eq!(regex.len(), 1);
    let invalid = client.call::<method::FilesSearchContents>(FilesSearchContentsParams {
        root: root.clone(),
        query: "(".into(),
        regex: true,
        case_sensitive: true,
        limit: None,
    });
    assert_eq!(remote_code(invalid).code, ErrorCode::BadRequest);

    // A write over the revision it read; a stale revision conflicts.
    let read = client
        .call::<method::FilesRead>(FilesReadParams {
            root: root.clone(),
            path: "README.md".into(),
            max_bytes: None,
        })
        .unwrap();
    let written = client
        .call::<method::FilesWrite>(FilesWriteParams {
            root: root.clone(),
            path: "README.md".into(),
            content: "# changed\n".into(),
            base_revision: Some(read.revision.clone()),
            op_id: op(),
        })
        .unwrap();
    assert_ne!(written.revision, read.revision);
    let stale = client.call::<method::FilesWrite>(FilesWriteParams {
        root: root.clone(),
        path: "README.md".into(),
        content: "# lost\n".into(),
        base_revision: Some(read.revision),
        op_id: op(),
    });
    let error = remote_code(stale);
    assert_eq!(error.code, ErrorCode::Conflict);
    assert_eq!(
        error.details,
        Some(ErrorDetails::RevisionConflict {
            current: written.revision.clone()
        })
    );
    // `""` creates only a new file.
    client
        .call::<method::FilesWrite>(FilesWriteParams {
            root: root.clone(),
            path: "NEW.md".into(),
            content: "new\n".into(),
            base_revision: Some(String::new()),
            op_id: op(),
        })
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(daemon.project.join("NEW.md")).unwrap(),
        "new\n"
    );
    let escape = client.call::<method::FilesWrite>(FilesWriteParams {
        root: root.clone(),
        path: "../outside".into(),
        content: String::new(),
        base_revision: None,
        op_id: op(),
    });
    assert_eq!(remote_code(escape).code, ErrorCode::BadRequest);

    // A file over the inline limit arrives as a blob.
    let large = "x".repeat(MAX_INLINE_TEXT_BYTES as usize + 10);
    std::fs::write(daemon.project.join("large.txt"), &large).unwrap();
    let content = client
        .call::<method::FilesRead>(FilesReadParams {
            root: root.clone(),
            path: "large.txt".into(),
            max_bytes: None,
        })
        .unwrap();
    assert_eq!(content.text, None);
    let (_, bytes) = client
        .download_blob(content.blob.as_deref().unwrap(), None)
        .unwrap();
    assert_eq!(bytes, large.as_bytes());

    let directory = client
        .call::<method::FsBrowse>(FsBrowseParams {
            path: Some(daemon.project.display().to_string()),
        })
        .unwrap();
    assert!(
        directory
            .entries
            .iter()
            .any(|entry| entry.name == "src" && entry.is_dir)
    );
    assert!(directory.parent.is_some());
    let relative = client.call::<method::FsBrowse>(FsBrowseParams {
        path: Some("relative".into()),
    });
    assert_eq!(remote_code(relative).code, ErrorCode::BadRequest);
}

#[test]
fn an_op_id_runs_its_mutation_once() {
    let daemon = Daemon::start();
    let client = daemon.connect();
    let first = op();
    let write = |content: &str, op_id| {
        client
            .call::<method::FilesWrite>(FilesWriteParams {
                root: daemon.root(),
                path: "README.md".into(),
                content: content.into(),
                base_revision: None,
                op_id,
            })
            .unwrap()
    };
    let a = write("a\n", first);
    let b = write("b\n", op());
    // The retry of the first write returns its receipt and writes nothing.
    assert_eq!(write("a\n", first), a);
    assert_ne!(a, b);
    assert_eq!(
        std::fs::read_to_string(daemon.project.join("README.md")).unwrap(),
        "b\n"
    );
}

#[test]
fn vcs_status_stage_commit_push_pull_and_worktrees() {
    let daemon = Daemon::start();
    let client = daemon.connect();
    let root = daemon.root();
    let params = || RootParams { root: root.clone() };

    let watch = client.subscribe::<stream::VcsStatusWatch>(
        VcsStatusSpec { root: root.clone() },
        SubscribeOptions::default(),
    );
    let StreamItem::VcsStatusWatch(VcsStatusItem::Status { status, .. }) = next_item(&watch) else {
        panic!("expected a status");
    };
    assert!(status.files.is_empty());
    assert_eq!(status.branch.as_deref(), Some("main"));

    std::fs::write(daemon.project.join("README.md"), "# edited\n").unwrap();
    let status = client.call::<method::VcsStatus>(params()).unwrap();
    assert_eq!(status.files[0].path, "README.md");
    assert_eq!(status.files[0].worktree_status, "M");

    let staged = client
        .call::<method::VcsStage>(VcsStageParams {
            root: root.clone(),
            paths: vec!["README.md".into()],
            stage: true,
            op_id: op(),
        })
        .unwrap();
    assert_eq!(staged.files[0].index_status, "M");
    // The daemon's own change reaches the watcher at once.
    let StreamItem::VcsStatusWatch(VcsStatusItem::Status { status, .. }) = next_item(&watch) else {
        panic!("expected a status");
    };
    assert_eq!(status.files[0].index_status, "M");

    let preview = client.call::<method::VcsCommitPreview>(params()).unwrap();
    assert!(preview.has_staged);
    let diff = client
        .call::<method::ReviewDiffGet>(ReviewDiffParams {
            root: root.clone(),
            source: ReviewDiffSource::Staged,
            ignore_whitespace: false,
        })
        .unwrap();
    assert_eq!(diff.files[0].path, "README.md");
    assert_eq!(diff.files[0].additions, Some(1));
    let TextPayload::Inline { value: patch } = &diff.patch else {
        panic!("a small patch is inline");
    };
    assert!(patch.contains("+# edited"));

    let commit = client
        .call::<method::VcsCommit>(VcsCommitParams {
            root: root.clone(),
            message: "Edit the readme".into(),
            include_unstaged: false,
            push: true,
            op_id: op(),
        })
        .unwrap();
    assert!(commit.pushed);
    assert_eq!(commit.sha, git(&daemon.project, &["rev-parse", "HEAD"]));
    assert_eq!(
        git(&daemon.root.join("remote.git"), &["rev-parse", "main"]),
        commit.sha
    );

    // Someone else pushes; pulling moves HEAD.
    let other = daemon.root.join("other");
    git(
        &daemon.root,
        &[
            "clone",
            daemon.root.join("remote.git").to_str().unwrap(),
            "other",
        ],
    );
    configure_identity(&other);
    std::fs::write(other.join("CHANGELOG.md"), "1\n").unwrap();
    git(&other, &["add", "-A", "--", "."]);
    git(&other, &["commit", "-m", "changelog"]);
    git(&other, &["push", "origin", "main"]);
    let pulled = client
        .call::<method::VcsPull>(RootOpParams {
            root: root.clone(),
            op_id: op(),
        })
        .unwrap();
    assert!(pulled.updated);
    assert!(pulled.conflicts.is_empty());
    assert!(daemon.project.join("CHANGELOG.md").exists());
    client
        .call::<method::VcsPush>(RootOpParams {
            root: root.clone(),
            op_id: op(),
        })
        .unwrap();

    let branch = client
        .call::<method::VcsSwitchRef>(VcsSwitchRefParams {
            root: root.clone(),
            name: "feature".into(),
            create: true,
            op_id: op(),
        })
        .unwrap();
    assert_eq!(branch.current.as_deref(), Some("feature"));
    let refs = client.call::<method::VcsRefs>(params()).unwrap().unwrap();
    assert!(refs.branches.iter().any(|branch| branch.name == "main"));
    let missing = client.call::<method::VcsSwitchRef>(VcsSwitchRefParams {
        root: root.clone(),
        name: "no-such-branch".into(),
        create: false,
        op_id: op(),
    });
    assert_eq!(remote_code(missing).code, ErrorCode::Vcs);

    daemon.use_test_worktrees(&client);
    // A worktree is named for an orchestration thread.
    let unknown = client.call::<method::VcsCreateWorktree>(VcsCreateWorktreeParams {
        project_id: daemon.project_id,
        base_ref: Some("main".into()),
        branch_hint: "try the idea".into(),
        thread_id: Some(Uuid::new_v4()),
        op_id: op(),
    });
    assert_eq!(remote_code(unknown).code, ErrorCode::NotFound);
    let worktree = client
        .call::<method::VcsCreateWorktree>(VcsCreateWorktreeParams {
            project_id: daemon.project_id,
            base_ref: Some("main".into()),
            branch_hint: "try the idea".into(),
            thread_id: Some(daemon.thread_id),
            op_id: op(),
        })
        .unwrap();
    assert!(worktree.branch.starts_with("waku/"));
    assert!(Path::new(&worktree.path).starts_with(daemon.root.join("worktrees")));
    let worktrees = client.call::<method::VcsWorktrees>(params()).unwrap();
    assert_eq!(worktrees.len(), 2);
    client
        .call::<method::VcsRemoveWorktree>(VcsRemoveWorktreeParams {
            path: worktree.path.clone(),
            force: true,
            op_id: op(),
        })
        .unwrap();
    assert!(!Path::new(&worktree.path).exists());
    assert_eq!(
        client.call::<method::VcsWorktrees>(params()).unwrap().len(),
        1
    );

    // Outside a repository.
    let plain = daemon.root.join("plain");
    std::fs::create_dir_all(&plain).unwrap();
    let outside = client.call::<method::VcsStatus>(RootParams {
        root: RootRef::Path {
            path: plain.display().to_string(),
        },
    });
    let error = remote_code(outside);
    assert_eq!(error.code, ErrorCode::Vcs);
    assert_eq!(
        error.details,
        Some(ErrorDetails::Vcs {
            kind: error::VcsFailureKind::NotARepository
        })
    );
    watch.unsubscribe();
}

fn occurrences(haystack: &[u8], needle: &[u8]) -> usize {
    haystack
        .windows(needle.len())
        .filter(|window| *window == needle)
        .count()
}

fn data_until(subscription: &Subscription, marker: &[u8]) -> (Vec<u8>, Vec<StreamItem>) {
    data_until_count(subscription, marker, 1)
}

/// DATA until `marker` was seen `count` times (a PTY echoes input, and
/// `cat` then prints it again).
fn data_until_count(
    subscription: &Subscription,
    marker: &[u8],
    count: usize,
) -> (Vec<u8>, Vec<StreamItem>) {
    let deadline = Instant::now() + WAIT;
    let mut bytes = Vec::new();
    let mut items = Vec::new();
    while occurrences(&bytes, marker) < count {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match subscription.events().recv_timeout(remaining) {
            Ok(StreamEvent::Data { bytes: body, .. }) => {
                subscription.consumed(body.len() as u32);
                bytes.extend_from_slice(&body);
            }
            Ok(StreamEvent::Item(item)) => items.push(item),
            Ok(other) => panic!("unexpected {other:?}"),
            Err(_) => panic!(
                "no {:?} in {:?}",
                String::from_utf8_lossy(marker),
                String::from_utf8_lossy(&bytes)
            ),
        }
    }
    (bytes, items)
}

/// The next `reset`, skipping other items and output.
fn next_reset(subscription: &Subscription) -> (Uuid, u64) {
    loop {
        if let TerminalItem::Reset {
            generation, offset, ..
        } = terminal_item(subscription)
        {
            return (generation, offset);
        }
    }
}

fn terminal_item(subscription: &Subscription) -> TerminalItem {
    loop {
        match next_event(subscription) {
            StreamEvent::Item(StreamItem::Terminal(item)) => return item,
            StreamEvent::Data { bytes, .. } => subscription.consumed(bytes.len() as u32),
            other => panic!("expected a terminal item, got {other:?}"),
        }
    }
}

#[test]
fn terminals_open_attach_write_resize_clear_restart_and_close() {
    let daemon = Daemon::start();
    let client = daemon.connect();
    let list = client.subscribe::<stream::TerminalList>(
        TerminalListSpec::default(),
        SubscribeOptions::default(),
    );
    assert_eq!(
        next_item(&list),
        StreamItem::TerminalList(TerminalListItem::Snapshot {
            terminals: Vec::new()
        })
    );
    let opened = client
        .call::<method::TerminalOpen>(TerminalOpenParams {
            root: daemon.root(),
            cols: 80,
            rows: 24,
            title: Some("shell".into()),
            op_id: op(),
        })
        .unwrap();
    let StreamItem::TerminalList(TerminalListItem::Upserted { terminal }) = next_item(&list) else {
        panic!("expected an upsert");
    };
    assert_eq!(terminal.terminal_id, opened.terminal_id);
    assert_eq!(terminal.cwd, daemon.project.display().to_string());

    let terminal = client.subscribe::<stream::Terminal>(
        TerminalSpec {
            terminal_id: opened.terminal_id,
            interactive: true,
        },
        SubscribeOptions::default(),
    );
    let TerminalItem::Attached {
        generation,
        cols,
        title,
        ..
    } = terminal_item(&terminal)
    else {
        panic!("attached first");
    };
    assert_eq!(
        (generation, cols, title.as_deref()),
        (opened.generation, 80, Some("shell"))
    );
    data_until(&terminal, b"ready");

    // Input travels as DATA on the subscription's channel.
    terminal.send_data(b"ping-42\n".to_vec());
    data_until_count(&terminal, b"ping-42", 2);

    client
        .call::<method::TerminalResize>(TerminalResizeParams {
            terminal_id: opened.terminal_id,
            cols: 100,
            rows: 30,
        })
        .unwrap();
    assert_eq!(
        terminal_item(&terminal),
        TerminalItem::Resized {
            cols: 100,
            rows: 30
        }
    );

    // A second, read-only subscriber replays the retained tail.
    let viewer = client.subscribe::<stream::Terminal>(
        TerminalSpec {
            terminal_id: opened.terminal_id,
            interactive: false,
        },
        SubscribeOptions::default(),
    );
    let TerminalItem::Attached { replay, offset, .. } = terminal_item(&viewer) else {
        panic!("attached first");
    };
    assert!(replay > 0);
    let (replayed, _) = data_until(&viewer, b"ping-42");
    assert!(contains(&replayed, b"ready"));
    // Resuming from the current cursor replays nothing.
    let resumed = client.subscribe::<stream::Terminal>(
        TerminalSpec {
            terminal_id: opened.terminal_id,
            interactive: false,
        },
        SubscribeOptions {
            after: Some(StreamCursor::Offset { generation, offset }),
            window: None,
        },
    );
    let TerminalItem::Attached { replay, .. } = terminal_item(&resumed) else {
        panic!("attached first");
    };
    assert_eq!(replay, 0);

    client
        .call::<method::TerminalClear>(TerminalParams {
            terminal_id: opened.terminal_id,
        })
        .unwrap();
    let (cleared, _) = next_reset(&terminal);
    assert_eq!(cleared, generation);

    let restarted = client
        .call::<method::TerminalRestart>(TerminalParams {
            terminal_id: opened.terminal_id,
        })
        .unwrap();
    assert_ne!(restarted.generation, generation);
    let (fresh, _) = next_reset(&terminal);
    assert_eq!(fresh, restarted.generation);
    data_until(&terminal, b"ready");

    client
        .call::<method::TerminalClose>(TerminalParams {
            terminal_id: opened.terminal_id,
        })
        .unwrap();
    loop {
        match next_event(&terminal) {
            StreamEvent::End(end) => {
                assert_eq!(end.reason, EndReason::Completed);
                break;
            }
            StreamEvent::Data { bytes, .. } => terminal.consumed(bytes.len() as u32),
            StreamEvent::Item(_) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    loop {
        if let StreamItem::TerminalList(TerminalListItem::Removed { terminal_id }) =
            next_item(&list)
        {
            assert_eq!(terminal_id, opened.terminal_id);
            break;
        }
    }
    let gone = client.call::<method::TerminalResize>(TerminalResizeParams {
        terminal_id: opened.terminal_id,
        cols: 10,
        rows: 10,
    });
    assert_eq!(remote_code(gone).code, ErrorCode::NotFound);
    let missing = client.subscribe::<stream::Terminal>(
        TerminalSpec {
            terminal_id: opened.terminal_id,
            interactive: false,
        },
        SubscribeOptions::default(),
    );
    assert!(matches!(
        next_event(&missing),
        StreamEvent::End(End {
            reason: EndReason::NotFound,
            ..
        })
    ));
}

#[test]
fn a_terminal_subscriber_without_credit_is_reset_with_a_fresh_tail() {
    let daemon = Daemon::start();
    let client = daemon.connect();
    let opened = client
        .call::<method::TerminalOpen>(TerminalOpenParams {
            root: daemon.root(),
            cols: 80,
            rows: 24,
            title: None,
            op_id: op(),
        })
        .unwrap();
    let writer = client.subscribe::<stream::Terminal>(
        TerminalSpec {
            terminal_id: opened.terminal_id,
            interactive: true,
        },
        SubscribeOptions::default(),
    );
    data_until(&writer, b"ready");
    // A subscriber with a tiny window that never returns credit.
    let slow = client.subscribe::<stream::Terminal>(
        TerminalSpec {
            terminal_id: opened.terminal_id,
            interactive: false,
        },
        SubscribeOptions {
            after: None,
            window: Some(16),
        },
    );
    let TerminalItem::Attached { replay, .. } = terminal_item(&slow) else {
        panic!("attached first");
    };
    assert!(replay <= 16);
    // Well over what the daemon holds for a subscriber without credit.
    let line = format!("{}\n", "y".repeat(1000));
    for _ in 0..1200 {
        writer.send_data(line.as_bytes().to_vec());
    }
    writer.send_data(b"done-marker\n".to_vec());
    data_until(&writer, b"done-marker");
    // Credit returns: the subscriber starts over from the retained tail.
    slow.consumed(4 * 1024 * 1024);
    let deadline = Instant::now() + WAIT;
    loop {
        assert!(Instant::now() < deadline, "no reset");
        match next_event(&slow) {
            StreamEvent::Item(StreamItem::Terminal(TerminalItem::Reset { replay, .. })) => {
                assert!(replay > 0);
                break;
            }
            StreamEvent::Data { .. } | StreamEvent::Item(_) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    let (tail, _) = data_until(&slow, b"done-marker");
    assert!(tail.len() <= 4 * 1024 * 1024);
}

#[test]
fn blobs_upload_stat_download_and_attachments() {
    let daemon = Daemon::start();
    let client = daemon.connect();
    let bytes = (0..700_000u32)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let put = client
        .put_blob(&bytes, "image/png", Some("big.png"), None)
        .unwrap();
    assert!(!put.deduplicated);
    assert_eq!(put.size, bytes.len() as u64);
    let stat = client
        .call::<method::BlobsStat>(BlobsStatParams {
            reference: put.reference.clone(),
            path: None,
        })
        .unwrap();
    assert_eq!(stat.sha256, put.sha256);
    assert_eq!(stat.mime, "image/png");
    // The local socket gets the file's path; a remote client does not.
    assert_eq!(std::fs::read(stat.local_path.unwrap()).unwrap(), bytes);
    let remote = daemon.connect_tcp();
    let remote_stat = remote
        .call::<method::BlobsStat>(BlobsStatParams {
            reference: put.reference.clone(),
            path: None,
        })
        .unwrap();
    assert_eq!(remote_stat.local_path, None);
    let (meta, downloaded) = remote.download_blob(&put.reference, None).unwrap();
    assert_eq!(downloaded, bytes);
    assert!(matches!(meta, BlobDownloadItem::Meta { size, .. } if size == bytes.len() as u64));

    // Known content is deduplicated by its digest.
    let again = client
        .put_blob(&bytes, "image/png", None, Some(&put.sha256))
        .unwrap();
    assert!(again.deduplicated);
    assert_eq!(again.reference, put.reference);
    let wrong = client.put_blob(b"abc", "text/plain", None, Some(&"0".repeat(64)));
    assert_eq!(remote_code(wrong).code, ErrorCode::BadRequest);
    let empty = client.put_blob(b"", "text/plain", None, None).unwrap();
    assert_eq!(empty.size, 0);

    // A directory attachment from two uploads.
    let first = client.put_blob(b"one\n", "text/plain", None, None).unwrap();
    let second = client.put_blob(b"two\n", "text/plain", None, None).unwrap();
    let attachment = client
        .call::<method::AttachmentsImport>(AttachmentsImportParams {
            name: "notes".into(),
            kind: AttachmentKind::Directory,
            parts: vec![
                AttachmentPart {
                    relative_path: "a.txt".into(),
                    blob: first.reference,
                },
                AttachmentPart {
                    relative_path: "nested/b.txt".into(),
                    blob: second.reference,
                },
            ],
            op_id: op(),
        })
        .unwrap();
    assert!(attachment.is_dir);
    let file = Path::new(&attachment.path).join("nested/b.txt");
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "two\n");
    let (_, inside) = remote
        .download_blob(&attachment.reference, Some(file.to_str().unwrap()))
        .unwrap();
    assert_eq!(inside, b"two\n");

    let copied = client
        .call::<method::AttachmentsImportPath>(AttachmentsImportPathParams {
            path: daemon.project.join("README.md").display().to_string(),
            op_id: op(),
        })
        .unwrap();
    assert_eq!(copied.name, "README.md");
    assert!(!copied.is_dir);
    let missing = client.call::<method::BlobsStat>(BlobsStatParams {
        reference: "waku-blob:0000000000000000.png".into(),
        path: None,
    });
    assert_eq!(remote_code(missing).code, ErrorCode::NotFound);
}

#[test]
fn drafts_apply_with_revisions_and_stream_changes() {
    let daemon = Daemon::start();
    let client = daemon.connect();
    let initial = client.call::<method::DraftsGet>(NoParams {}).unwrap();
    assert_eq!(initial.drafts, ComposerDrafts::default());
    let drafts = client.subscribe::<stream::Drafts>(DraftsSpec {}, SubscribeOptions::default());
    assert!(matches!(
        next_item(&drafts),
        StreamItem::Drafts(DraftsItem::Snapshot { revision, .. }) if revision == initial.revision
    ));
    assert!(matches!(
        next_item(&drafts),
        StreamItem::Drafts(DraftsItem::Synchronized { .. })
    ));

    let thread = daemon.thread_id;
    let change = DraftChange {
        target: DraftTarget::Thread { thread_id: thread },
        draft: Some(ComposerDraft {
            text: "half a thought".into(),
            attachments: Vec::new(),
        }),
    };
    let applied = client
        .call::<method::DraftsApply>(DraftsApplyParams {
            base_revision: Some(initial.revision),
            changes: vec![change.clone()],
            op_id: op(),
        })
        .unwrap();
    assert_ne!(applied.revision, initial.revision);
    assert_eq!(
        next_item(&drafts),
        StreamItem::Drafts(DraftsItem::Changed {
            revision: applied.revision,
            changes: vec![change],
        })
    );
    let stale = client.call::<method::DraftsApply>(DraftsApplyParams {
        base_revision: Some(initial.revision),
        changes: Vec::new(),
        op_id: op(),
    });
    assert_eq!(remote_code(stale).code, ErrorCode::Conflict);
    let current = client.call::<method::DraftsGet>(NoParams {}).unwrap();
    assert_eq!(current.revision, applied.revision);
    assert_eq!(current.drafts.threads[&thread].text, "half a thought");

    // Resuming at the current revision skips the snapshot.
    let resumed = client.subscribe::<stream::Drafts>(
        DraftsSpec {},
        SubscribeOptions {
            after: Some(StreamCursor::Revision {
                revision: applied.revision,
            }),
            window: None,
        },
    );
    assert!(matches!(
        next_item(&resumed),
        StreamItem::Drafts(DraftsItem::Synchronized { .. })
    ));
    // An empty draft removes it.
    client
        .call::<method::DraftsApply>(DraftsApplyParams {
            base_revision: None,
            changes: vec![DraftChange {
                target: DraftTarget::Thread { thread_id: thread },
                draft: Some(ComposerDraft::default()),
            }],
            op_id: op(),
        })
        .unwrap();
    let StreamItem::Drafts(DraftsItem::Changed { changes, .. }) = next_item(&resumed) else {
        panic!("a change");
    };
    assert_eq!(changes[0].draft, None);
    assert!(
        client
            .call::<method::DraftsGet>(NoParams {})
            .unwrap()
            .drafts
            .threads
            .is_empty()
    );

    // Keys are orchestration ids: only a live project or thread gets a
    // draft (D6: no legacy session ids).
    let text = |text: &str| {
        Some(ComposerDraft {
            text: text.into(),
            attachments: Vec::new(),
        })
    };
    let unknown = client.call::<method::DraftsApply>(DraftsApplyParams {
        base_revision: None,
        changes: vec![DraftChange {
            target: DraftTarget::Thread {
                thread_id: Uuid::new_v4(),
            },
            draft: text("lost"),
        }],
        op_id: op(),
    });
    assert_eq!(remote_code(unknown).code, ErrorCode::NotFound);
    client
        .call::<method::DraftsApply>(DraftsApplyParams {
            base_revision: None,
            changes: vec![
                DraftChange {
                    target: DraftTarget::NewThread {
                        project_id: daemon.project_id,
                    },
                    draft: text("next task"),
                },
                DraftChange {
                    target: DraftTarget::Thread { thread_id: thread },
                    draft: text("follow-up"),
                },
            ],
            op_id: op(),
        })
        .unwrap();
    // A deleted thread's draft is gone.
    client
        .call::<method::OrchestrationDispatch>(waku_protocol::v8::orchestration::DispatchParams {
            command_id: waku_protocol::orchestration::CommandId::new("delete-thread").unwrap(),
            command: waku_protocol::v8::orchestration::DispatchCommand(
                waku_protocol::orchestration::Command::ThreadDelete {
                    thread_id: waku_protocol::orchestration::ThreadId(thread),
                },
            ),
        })
        .unwrap();
    let current = client.call::<method::DraftsGet>(NoParams {}).unwrap();
    assert!(current.drafts.threads.is_empty());
    assert_eq!(
        current.drafts.new_threads[&daemon.project_id].text,
        "next task"
    );
}

#[test]
fn provider_sessions_leave_out_conversations_a_live_thread_holds() {
    use waku_protocol::orchestration::{
        Command as OrchestrationCommand, DriverKind, InteractionMode, ProjectId, ProviderRef,
        RuntimeMode, ThreadId, WorkspaceStrategy,
    };
    let root = tempfile::tempdir().unwrap();
    let project_id = Uuid::new_v4();
    let orchestration = orchestration(root.path(), project_id, root.path(), Uuid::new_v4());
    let orchestrator = &orchestration.orchestrator;
    let imported = ThreadId(Uuid::new_v4());
    dispatch(
        orchestrator,
        "import",
        OrchestrationCommand::ThreadCreate {
            thread_id: imported,
            project_id: ProjectId(project_id),
            title: None,
            model_selection: ModelSelection::new("codex"),
            runtime_mode: RuntimeMode::FullAccess,
            interaction_mode: InteractionMode::Default,
            workspace: WorkspaceStrategy::Root { branch: None },
            imported_native_thread: Some(ProviderRef::strong(DriverKind::Codex, "native-1")),
        },
    );
    let backend = WakuBackend::new(
        DaemonSettingsStore::open(root.path().join("settings.json")).unwrap(),
        root.path().to_path_buf(),
    )
    .unwrap();
    let context = crate::v8::V8Context::new(
        TOKEN.into(),
        Arc::new(backend),
        crate::v8::SettingsBus::default(),
        ShutdownSignal::new(),
    )
    .with_orchestration(Some(orchestration.clone()));
    let codex = crate::model::ProviderKind::Codex;
    assert_eq!(
        super::providers::imported_native_ids(&context, codex).unwrap(),
        std::collections::HashSet::from(["native-1".to_owned()])
    );
    assert!(
        super::providers::imported_native_ids(&context, crate::model::ProviderKind::Claude)
            .unwrap()
            .is_empty()
    );
    // Deleting the thread offers the conversation again.
    dispatch(
        orchestrator,
        "delete",
        OrchestrationCommand::ThreadDelete {
            thread_id: imported,
        },
    );
    assert!(
        super::providers::imported_native_ids(&context, codex)
            .unwrap()
            .is_empty()
    );
    orchestration.shutdown();
}

#[test]
fn settings_update_streams_and_storage_cleanup() {
    let daemon = Daemon::start();
    let client = daemon.connect();
    let settings =
        client.subscribe::<stream::Settings>(SettingsSpec {}, SubscribeOptions::default());
    let StreamItem::Settings(SettingsItem::Snapshot { revision, .. }) = next_item(&settings) else {
        panic!("a snapshot");
    };
    next_item(&settings);
    let providers =
        client.subscribe::<stream::Providers>(ProvidersSpec {}, SubscribeOptions::default());
    let StreamItem::Providers(ProvidersItem::Snapshot { statuses, .. }) = next_item(&providers)
    else {
        panic!("a snapshot");
    };
    assert!(statuses.iter().all(|status| status.enabled));
    next_item(&providers);

    let updated = client
        .call::<method::SettingsUpdate>(SettingsUpdateParams {
            base_revision: Some(revision),
            patch: SettingsPatch {
                computer_use_enabled: Some(true),
                disabled_providers: Some(vec!["grok".into()]),
                ..SettingsPatch::default()
            },
            op_id: op(),
        })
        .unwrap();
    assert!(updated.settings.computer_use_enabled);
    assert_eq!(updated.settings.disabled_providers, ["grok"]);
    assert!(matches!(
        next_item(&settings),
        StreamItem::Settings(SettingsItem::Updated { revision, .. }) if revision == updated.revision
    ));
    let StreamItem::Providers(ProvidersItem::StatusUpdated { status, .. }) = next_item(&providers)
    else {
        panic!("a status update");
    };
    assert_eq!(
        (status.instance_id.as_str(), status.enabled),
        ("grok", false)
    );

    let stale = client.call::<method::SettingsUpdate>(SettingsUpdateParams {
        base_revision: Some(revision),
        patch: SettingsPatch::default(),
        op_id: op(),
    });
    let error = remote_code(stale);
    assert_eq!(error.code, ErrorCode::Conflict);
    let unknown = client.call::<method::SettingsUpdate>(SettingsUpdateParams {
        base_revision: None,
        patch: SettingsPatch {
            disabled_providers: Some(vec!["nebula".into()]),
            ..SettingsPatch::default()
        },
        op_id: op(),
    });
    assert_eq!(remote_code(unknown).code, ErrorCode::BadRequest);

    daemon.use_test_worktrees(&client);
    let current = client.call::<method::SettingsGet>(NoParams {}).unwrap();
    assert_eq!(
        current.settings.storage.worktrees_directory,
        Some(daemon.root.join("worktrees").display().to_string())
    );
    let relative = client.call::<method::SettingsUpdate>(SettingsUpdateParams {
        base_revision: None,
        patch: SettingsPatch {
            storage: Some(StorageSettings {
                worktrees_directory: Some("relative/dir".into()),
                ..StorageSettings::default()
            }),
            ..SettingsPatch::default()
        },
        op_id: op(),
    });
    assert_eq!(remote_code(relative).code, ErrorCode::BadRequest);

    let status = client
        .call::<method::StorageWorktreeCleanupStatus>(NoParams {})
        .unwrap();
    assert!(!status.running);
    let report = client
        .call::<method::StorageRunWorktreeCleanup>(StorageRunCleanupParams { op_id: op() })
        .unwrap();
    assert_eq!(report.trigger, CleanupTrigger::Manual);
    assert_eq!(report.removed, 0);
    let status = client
        .call::<method::StorageWorktreeCleanupStatus>(NoParams {})
        .unwrap();
    assert!(status.last_run_at.is_some());
}

#[test]
fn providers_models_probe_usage_and_skills() {
    let daemon = Daemon::start();
    let client = daemon.connect();
    let statuses = client.call::<method::ProvidersList>(NoParams {}).unwrap();
    assert_eq!(statuses.len(), crate::model::ProviderKind::ALL.len());
    assert!(
        statuses
            .iter()
            .any(|status| status.instance_id == "codex" && status.plan_usage)
    );

    let probe = client
        .call::<method::ProvidersProbe>(ProvidersProbeParams {
            instance_id: "amp".into(),
            discover_models: false,
            probe_version: false,
        })
        .unwrap();
    assert_eq!(probe.probe.instance_id, "amp");
    let models = client
        .call::<method::ProvidersModels>(ProvidersModelsParams {
            instance_id: "codex".into(),
            refresh: false,
        })
        .unwrap();
    assert!(!models.models.is_empty());
    assert_ne!(models.source, ModelCatalogSource::Discovered);
    assert_eq!(
        client
            .call::<method::ProvidersPlanUsage>(InstanceParams {
                instance_id: "amp".into()
            })
            .unwrap(),
        None
    );
    let unknown = client.call::<method::ProvidersModels>(ProvidersModelsParams {
        instance_id: "nebula".into(),
        refresh: false,
    });
    assert_eq!(remote_code(unknown).code, ErrorCode::NotFound);
    // A disabled provider lists no sessions (and its CLI is not asked).
    client
        .call::<method::SettingsUpdate>(SettingsUpdateParams {
            base_revision: None,
            patch: SettingsPatch {
                disabled_providers: Some(vec!["codex".into()]),
                ..SettingsPatch::default()
            },
            op_id: op(),
        })
        .unwrap();
    let sessions = client
        .call::<method::ProviderSessionsList>(ProviderSessionsListParams {
            instance_id: "codex".into(),
            limit: Some(10),
        })
        .unwrap();
    assert!(sessions.is_empty());

    let history = client
        .call::<method::UsageHistoryGet>(UsageHistoryParams {
            window: UsageWindow::TrailingDays { days: 7 },
            project_ids: vec![daemon.project_id],
        })
        .unwrap();
    assert_eq!(history.window, UsageWindow::TrailingDays { days: 7 });
    assert!(history.since_day <= history.until_day);

    let catalog = client
        .call::<method::SkillsList>(SkillsListParams {
            project_ids: vec![daemon.project_id],
        })
        .unwrap();
    let skill = catalog
        .skills
        .iter()
        .find(|skill| skill.name == "demo" && skill.scope == SkillScope::Project)
        .unwrap();
    assert!(skill.enabled);
    assert_eq!(skill.installs[0].source, "claude");
    let dir = skill.installs[0].dir.clone();
    client
        .call::<method::SkillsSetEnabled>(SkillsSetEnabledParams {
            dirs: vec![dir.clone()],
            enabled: false,
            op_id: op(),
        })
        .unwrap();
    let catalog = client
        .call::<method::SkillsList>(SkillsListParams {
            project_ids: vec![daemon.project_id],
        })
        .unwrap();
    assert!(
        !catalog
            .skills
            .iter()
            .find(|skill| skill.name == "demo")
            .unwrap()
            .enabled
    );
    let relative = client.call::<method::SkillsTrash>(SkillsTrashParams {
        dirs: vec!["relative".into()],
        op_id: op(),
    });
    assert_eq!(remote_code(relative).code, ErrorCode::BadRequest);

    let commands = client
        .call::<method::ComposerSlashCommands>(ComposerSlashCommandsParams {
            root: daemon.root(),
            instance_id: Some("amp".into()),
            thread_id: None,
        })
        .unwrap();
    let greet = commands
        .iter()
        .find(|command| command.name == "greet")
        .unwrap();
    assert_eq!(greet.description, "Say hello");
    let without = client.call::<method::ComposerSlashCommands>(ComposerSlashCommandsParams {
        root: daemon.root(),
        instance_id: None,
        thread_id: None,
    });
    assert_eq!(remote_code(without).code, ErrorCode::BadRequest);

    // Asking for the commit message generator of an uninstalled provider.
    let generated =
        client.call::<method::VcsGenerateCommitMessage>(VcsGenerateCommitMessageParams {
            root: daemon.root(),
            include_unstaged: true,
            selection: ModelSelection::new("nebula"),
        });
    assert_eq!(remote_code(generated).code, ErrorCode::NotFound);
}

#[test]
fn daemon_info_local_only_prompts_and_shutdown() {
    let daemon = Daemon::start();
    let client = daemon.connect();
    let info = client.call::<method::DaemonInfoGet>(NoParams {}).unwrap();
    assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(info.environment, client.welcome().environment);
    assert!(!info.default_cwd.is_empty());

    // Permission prompts open on the daemon host: never for a remote client.
    let remote = daemon.connect_tcp();
    let prompt = remote
        .call::<method::ComputerUsePermissions>(ComputerUsePermissionsParams { prompt: true });
    assert_eq!(remote_code(prompt).code, ErrorCode::Forbidden);

    client
        .call::<method::DaemonShutdown>(DaemonShutdownParams {
            reason: "test".into(),
        })
        .unwrap();
    let deadline = Instant::now() + WAIT;
    while !client.is_disconnected() {
        assert!(Instant::now() < deadline, "the daemon did not shut down");
        std::thread::sleep(Duration::from_millis(20));
    }
}
