//! The desktop's protocol v8 surface helpers (`waku_client::surfaces` and
//! the v8 terminal link) against the real daemon (#53, #55): the same
//! `WakuBackend` plus `DaemonHost` that `waku-daemon` composes, on a local
//! socket. Files, version control, terminal attach and replay across a
//! reconnect, drafts, blobs and settings.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, unbounded};
use uuid::Uuid;
use waku_client::persistence::{ComposerDraft, ComposerDraftKey, ComposerDrafts};
use waku_client::surfaces::drafts::{DraftStore, RemoteDraftChange};
use waku_client::surfaces::settings::SettingsSync;
use waku_client::surfaces::{DaemonV8, Endpoint, blobs, files, settings, vcs};
use waku_client::terminal::{TerminalLink, TerminalSink, TerminalUpdate};
use waku_client::v8::{
    ReviewDiffSource, StreamEvent, StreamItem, SubscribeOptions, TerminalListItem,
    TerminalListSpec, V8Client, method, stream,
};
use waku_core::daemon::WakuBackend;
use waku_core::daemon_host::{DaemonHost, HostOptions};
use waku_core::model::ProviderKind;
use waku_core::{DaemonSettings, DaemonSettingsStore, ServerOptions};
use waku_protocol::orchestration::*;
use waku_protocol::v8::orchestration::{DispatchCommand, DispatchParams};

const TOKEN: &str = "desktop-surfaces-secret";
const TIMEOUT: Duration = Duration::from_secs(30);

/// A daemon composed as `waku-daemon`'s `main` composes it, on a local
/// socket under `root`, with a git repository in `root/project`.
struct Daemon {
    root: PathBuf,
    socket: PathBuf,
    shutdown: waku_core::ShutdownSignal,
    server: Option<std::thread::JoinHandle<()>>,
}

fn git(project: &Path, args: &[&str]) {
    let status = std::process::Command::new("git")
        .args([
            "-c",
            "user.name=Waku",
            "-c",
            "user.email=waku@example.invalid",
        ])
        .args(args)
        .current_dir(project)
        .status()
        .unwrap();
    assert!(status.success(), "git {args:?}");
}

impl Daemon {
    fn start() -> Self {
        // The socket path must fit `sun_path`.
        let root = PathBuf::from("/tmp").join(format!(
            "wkds-{}",
            &Uuid::new_v4().simple().to_string()[..8]
        ));
        let project = root.join("project");
        std::fs::create_dir_all(project.join("src")).unwrap();
        std::fs::write(project.join("README.md"), "surfaces\n").unwrap();
        std::fs::write(project.join("src/main.rs"), "fn main() {}\n").unwrap();
        git(&project, &["init", "-q", "-b", "main"]);
        // The daemon commits through its own Git, which has no identity on CI.
        git(&project, &["config", "user.name", "Waku"]);
        git(&project, &["config", "user.email", "waku@example.invalid"]);
        git(&project, &["add", "."]);
        git(&project, &["commit", "-q", "-m", "init"]);

        let settings = DaemonSettingsStore::open(root.join("settings.json")).unwrap();
        settings.replace(DaemonSettings::default()).unwrap();
        let backend = Arc::new(WakuBackend::new(settings, root.to_path_buf()).unwrap());
        let host = DaemonHost::start(&backend, HostOptions::default()).unwrap();
        let socket = root.join("d.sock");
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
            root,
            socket,
            shutdown,
            server: Some(server),
        }
    }

    fn project(&self) -> PathBuf {
        self.root.join("project")
    }

    fn endpoint(&self) -> Endpoint {
        Endpoint {
            address: format!("unix:{}", self.socket.display()),
            token: TOKEN.into(),
        }
    }

    /// A desktop handle, as `src/daemon_v8.rs` builds one.
    fn desktop(&self) -> DaemonV8 {
        DaemonV8::fixed(self.endpoint())
    }

    fn raw(&self) -> V8Client {
        V8Client::connect(&self.endpoint().address, TOKEN).unwrap()
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

fn dispatch(client: &V8Client, command: Command) {
    client
        .call::<method::OrchestrationDispatch>(DispatchParams {
            command_id: CommandId::new(format!("surfaces:{}", Uuid::new_v4().simple())).unwrap(),
            command: DispatchCommand(command),
        })
        .unwrap_or_else(|error| panic!("dispatch failed: {error:?}"));
}

/// An orchestrator project at `path` with one thread.
fn create_thread(client: &V8Client, path: &Path) -> (ProjectId, ThreadId) {
    let project_id = ProjectId(Uuid::new_v4());
    let thread_id = ThreadId(Uuid::new_v4());
    dispatch(
        client,
        Command::ProjectCreate {
            project_id,
            name: "surfaces".into(),
            path: path.to_owned(),
            order_key: None,
        },
    );
    dispatch(
        client,
        Command::ThreadCreate {
            thread_id,
            project_id,
            title: Some("surfaces".into()),
            model_selection: ModelSelection::new("codex"),
            runtime_mode: RuntimeMode::FullAccess,
            interaction_mode: InteractionMode::Default,
            workspace: WorkspaceStrategy::Root { branch: None },
            imported_native_thread: None,
        },
    );
    (project_id, thread_id)
}

#[test]
fn files_and_version_control() {
    let daemon = Daemon::start();
    let desktop = daemon.desktop();
    let project = daemon.project();
    assert!(desktop.is_local());

    // The right panel's tree, with `src` expanded by its absolute path.
    let rows = files::tree(&desktop, &project, &[project.join("src")]).unwrap();
    let main = rows
        .iter()
        .find(|row| row.relative_path == "src/main.rs")
        .expect("an expanded directory lists its children");
    assert_eq!(main.absolute_path, project.join("src/main.rs"));
    assert_eq!(main.depth, 1);
    assert!(
        rows.iter()
            .any(|row| row.name == "README.md" && !row.is_dir)
    );

    // The editor reads and saves.
    assert_eq!(
        files::read_text(&desktop, &project, "README.md").unwrap(),
        "surfaces\n"
    );
    files::write_text(&desktop, &project, "README.md", "edited\n".into()).unwrap();
    assert_eq!(
        std::fs::read_to_string(project.join("README.md")).unwrap(),
        "edited\n"
    );
    assert!(files::read_text(&desktop, &project, "missing.txt").is_err());

    // The remote folder picker and `@` mentions.
    let listing = files::browse(&desktop, Some(&project)).unwrap();
    // The daemon reports canonical paths (`/tmp` is `/private/tmp` on macOS).
    assert!(
        listing.entries.iter().any(|entry| entry.is_dir
            && entry.name == "src"
            && entry.path.ends_with("/project/src")),
        "{listing:?}"
    );
    let mentions = files::mention_index(&desktop, &project, 100).unwrap();
    assert!(mentions.iter().any(|entry| entry.path == "src/main.rs"));

    // Branches, status, staging, the commit preview, a commit and a diff.
    let refs = vcs::refs(&desktop, &project)
        .unwrap()
        .expect("a repository");
    assert_eq!(vcs::display_branch(&refs), Some("main"));
    let status = vcs::status(&desktop, &project).unwrap();
    assert!(status.files.iter().any(|file| file.path == "README.md"));
    let preview = vcs::commit_preview(&desktop, &project).unwrap();
    assert!(preview.has_unstaged && !preview.has_staged);
    let staged = vcs::stage(&desktop, &project, vec!["README.md".into()], true).unwrap();
    assert!(
        staged
            .files
            .iter()
            .any(|file| file.path == "README.md" && file.index_status != " ")
    );
    let diff = vcs::review_diff(&desktop, &project, ReviewDiffSource::Staged, false).unwrap();
    assert!(diff.numstat.contains("README.md"), "{diff:?}");
    assert!(diff.patch.contains("+edited"), "{diff:?}");
    let commit = vcs::commit(&desktop, &project, "Edit".into(), false, false).unwrap();
    assert!(!commit.sha.is_empty() && !commit.pushed);
    assert!(vcs::status(&desktop, &project).unwrap().files.is_empty());
    let switched = vcs::switch_ref(&desktop, &project, "feature".into(), true).unwrap();
    assert_eq!(switched.current.as_deref(), Some("feature"));
    // Nothing to pull from: the daemon's error reaches the desktop as text.
    let error = vcs::pull(&desktop, &project).unwrap_err();
    assert!(!error.to_string().is_empty());
}

/// Collects a terminal link's updates for the test thread.
struct ChannelSink(Sender<TerminalUpdate>);

impl TerminalSink for ChannelSink {
    fn update(&mut self, update: TerminalUpdate) {
        let _ = self.0.send(update);
    }
}

/// Updates until the output holds `marker`.
fn output_until(updates: &Receiver<TerminalUpdate>, marker: &str) -> Vec<TerminalUpdate> {
    let deadline = Instant::now() + TIMEOUT;
    let mut seen = Vec::new();
    let mut output = Vec::new();
    while !String::from_utf8_lossy(&output).contains(marker) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let update = updates
            .recv_timeout(remaining)
            .unwrap_or_else(|_| panic!("the link never delivered {marker:?}; saw {seen:?}"));
        if let TerminalUpdate::Output(bytes) = &update {
            output.extend_from_slice(bytes);
        }
        seen.push(update);
    }
    seen
}

fn open_terminals(client: &V8Client) -> usize {
    let list = client.subscribe::<stream::TerminalList>(
        TerminalListSpec { root: None },
        SubscribeOptions::default(),
    );
    let count = loop {
        match list.events().recv_timeout(TIMEOUT) {
            Ok(StreamEvent::Item(StreamItem::TerminalList(TerminalListItem::Snapshot {
                terminals,
            }))) => break terminals.iter().filter(|terminal| !terminal.exited).count(),
            Ok(_) => {}
            Err(_) => panic!("no terminal list snapshot"),
        }
    };
    list.unsubscribe();
    count
}

#[test]
fn terminal_attach_input_and_replay_after_a_reconnect() {
    let daemon = Daemon::start();
    let desktop = daemon.desktop();
    let (updates_tx, updates) = unbounded();
    let link = TerminalLink::open(
        desktop.clone(),
        daemon.project(),
        80,
        24,
        ChannelSink(updates_tx),
    )
    .unwrap();
    // Input goes out on the attached subscription: wait for the shell.
    updates
        .recv_timeout(TIMEOUT)
        .expect("the shell never wrote anything");
    // The shell computes the marker, so the echo of the input cannot match.
    link.write(b"echo $((40+2))ok\r".to_vec());
    output_until(&updates, "42ok");
    link.resize(100, 30);

    // Drop the connection under the link: it reconnects through the same
    // handle and resubscribes from the offset on screen. Either the daemon
    // continues the stream, or it resets the screen and replays its tail.
    desktop.close();
    let observer = daemon.raw();
    assert_eq!(
        open_terminals(&observer),
        1,
        "the terminal outlives the connection"
    );
    link.write(b"echo $((50+5))ok\r".to_vec());
    let after = output_until(&updates, "55ok");
    if let Some(reset) = after
        .iter()
        .position(|update| *update == TerminalUpdate::Reset)
    {
        let replay = after[reset..]
            .iter()
            .filter_map(|update| match update {
                TerminalUpdate::Output(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
                _ => None,
            })
            .collect::<String>();
        assert!(replay.contains("42ok"), "the replay lost earlier output");
    }

    // The daemon's own terminal clear resets the screen.
    link.clear();
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let update = updates
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .expect("a clear resets the screen");
        if update == TerminalUpdate::Reset {
            break;
        }
    }

    // Dropping the link closes the daemon's terminal.
    drop(link);
    let deadline = Instant::now() + TIMEOUT;
    while open_terminals(&observer) != 0 {
        assert!(
            Instant::now() < deadline,
            "dropping the link left the terminal open"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn draft(text: &str) -> ComposerDraft {
    ComposerDraft {
        text: text.into(),
        attachments: Vec::new(),
    }
}

#[test]
fn drafts_save_load_and_follow_other_clients() {
    let daemon = Daemon::start();
    let (project_id, thread_id) = create_thread(&daemon.raw(), &daemon.project());
    let new_task = ComposerDraftKey::NewSession(project_id.0);
    let task = ComposerDraftKey::Session(thread_id.0);

    let store = DraftStore::new(daemon.desktop());
    assert_eq!(store.load().unwrap(), ComposerDrafts::default());
    let (changes_tx, changes) = unbounded::<Vec<RemoteDraftChange>>();
    store
        .follow(move |batch| changes_tx.send(batch).is_ok())
        .unwrap();

    let mut drafts = ComposerDrafts::default();
    drafts.set(new_task, draft("plan the work"));
    drafts.set(task, draft("continue"));
    // A draft for an id the orchestrator does not know is skipped, not
    // fatal: the session layer may still hand out legacy ids.
    drafts.set(ComposerDraftKey::Session(Uuid::new_v4()), draft("orphan"));
    store.save(drafts, 1).unwrap();

    let mut expected = ComposerDrafts::default();
    expected.set(new_task, draft("plan the work"));
    expected.set(task, draft("continue"));
    let other = DraftStore::new(daemon.desktop());
    assert_eq!(other.load().unwrap(), expected);

    // Another client clears the thread's draft; the first one hears it.
    let mut edited = expected.clone();
    edited.remove(task);
    other.save(edited, 1).unwrap();
    let deadline = Instant::now() + TIMEOUT;
    let change = loop {
        let batch = changes
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .expect("the drafts stream never reported the other client's change");
        if let Some(change) = batch.into_iter().find(|change| change.key == task) {
            break change;
        }
    };
    assert_eq!(change.previous, Some(draft("continue")));
    assert_eq!(change.draft, None);
}

#[test]
fn blobs_and_attachments() {
    let daemon = Daemon::start();
    let desktop = daemon.desktop();
    let png = (0..300_000u32).map(|byte| byte as u8).collect::<Vec<_>>();

    // A pasted image becomes a one-file attachment with a daemon path.
    let image = blobs::store_image(&desktop, &png, "image/png", "image.png").unwrap();
    assert!(image.reference.starts_with("waku-attachment:"));
    assert!(image.path.ends_with("image.png"));
    let path = PathBuf::from(&image.path);
    assert_eq!(
        blobs::read_reference(&desktop, &image.reference, Some(&path)).unwrap(),
        png
    );
    // What a remote desktop reads instead: the `blobDownload` stream.
    let (_, downloaded) = desktop
        .client()
        .unwrap()
        .download_blob(&image.reference, Some(&image.path))
        .unwrap();
    assert_eq!(downloaded, png);

    // A dropped directory: one upload per file, then the manifest.
    let folder = blobs::upload_attachment(
        &desktop,
        "notes",
        blobs::AttachmentSource::Directory(vec![
            ("a.md".into(), b"alpha".to_vec()),
            ("deep/b.md".into(), b"beta".to_vec()),
        ]),
    )
    .unwrap();
    assert!(folder.is_dir);
    let nested = PathBuf::from(&folder.path).join("deep/b.md");
    assert_eq!(
        blobs::read_reference(&desktop, &folder.reference, Some(&nested)).unwrap(),
        b"beta"
    );
    assert!(blobs::read_reference(&desktop, &folder.reference, None).is_err());
}

#[test]
fn settings_write_and_follow() {
    let daemon = Daemon::start();
    let desktop = daemon.desktop();
    let initial = settings::settings_from_wire(&settings::get(&desktop).unwrap().settings);
    let sync = SettingsSync::start(desktop.clone(), Some(initial.clone())).unwrap();
    let (remote_tx, remote) = unbounded();
    sync.follow(move |settings| remote_tx.send(settings).is_ok())
        .unwrap();

    let mut edited = initial.clone();
    edited.disabled_providers = vec![ProviderKind::DeepSeek];
    edited.storage.worktree_cleanup.merged = true;
    sync.update(edited.clone());
    assert!(
        sync.wait_settled(TIMEOUT),
        "the edit never reached the daemon"
    );
    let stored = settings::settings_from_wire(&settings::get(&desktop).unwrap().settings);
    assert_eq!(stored.disabled_providers, [ProviderKind::DeepSeek]);
    assert!(stored.storage.worktree_cleanup.merged);

    // Another client's edit reaches this desktop; its own did not. (Not
    // Computer Use: the daemon turns it off where it is unavailable.)
    let mut other = edited.clone();
    other.storage.worktree_cleanup.merged = !other.storage.worktree_cleanup.merged;
    settings::update(&daemon.desktop(), &other).unwrap();
    let heard = remote.recv_timeout(TIMEOUT).expect("no remote settings");
    assert_eq!(
        heard.storage.worktree_cleanup.merged,
        other.storage.worktree_cleanup.merged
    );
    assert!(remote.try_recv().is_err());

    // Storage status reads through the same surface.
    let status = settings::worktree_cleanup_status(&desktop).unwrap();
    assert!(!status.running);
}
