//! The ports against real Git repositories, and title generation through
//! fake backends.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use uuid::Uuid;
use waku_orchestrator::command::PreparePhase;
use waku_orchestrator::{CheckpointPort, CleanupPort, PrepareRequest, TitlePort, WorkspacePort};
use waku_protocol::orchestration::*;

use super::*;
use crate::attachments::AttachmentStore;

struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!("waku-ports-{label}-{}", Uuid::new_v4()));
        fs::create_dir_all(&path).unwrap();
        Self(dunce::canonicalize(path).unwrap())
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn git(cwd: &Path, args: &[&str]) -> String {
    let output = crate::command_env::plain_command("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

/// A repository on `main` with one commit and `packages/app/README.md`.
fn repository(root: &Path) -> PathBuf {
    let repository = root.join("repository");
    fs::create_dir_all(repository.join("packages/app")).unwrap();
    git(&repository, &["init", "-q", "-b", "main"]);
    git(&repository, &["config", "core.autocrlf", "false"]);
    git(&repository, &["config", "user.name", "Waku Tests"]);
    git(&repository, &["config", "user.email", "tests@waku.invalid"]);
    fs::write(repository.join("packages/app/README.md"), "one\n").unwrap();
    fs::write(repository.join("tracked.txt"), "base\n").unwrap();
    git(&repository, &["add", "."]);
    git(&repository, &["commit", "-q", "-m", "initial"]);
    repository
}

fn thread_ref(thread: ThreadId, scope: CheckpointScopeId, ordinal: u32) -> String {
    waku_orchestrator::decide::checkpoint_ref(thread, scope, ordinal)
}

#[test]
fn checkpoints_capture_and_restore_hidden_refs() {
    let temp = TempDir::new("checkpoints");
    let repository = repository(&temp.0);
    let thread = ThreadId(Uuid::new_v4());
    let scope = CheckpointScopeId(Uuid::new_v4());
    let port = GitCheckpointPort;

    // The user's own staged state must survive every capture untouched.
    fs::write(repository.join("tracked.txt"), "base\nstaged\n").unwrap();
    git(&repository, &["add", "tracked.txt"]);
    fs::write(repository.join("new.txt"), "untracked\n").unwrap();
    let status_before = git(&repository, &["status", "--porcelain"]);

    let first = thread_ref(thread, scope, 0);
    assert!(first.starts_with(&format!("refs/waku/t/{thread}/{scope}/")));
    let captured = port.capture(&repository, &first, None).unwrap();
    assert_eq!(captured.git_ref, first);
    let mut files = captured
        .files
        .iter()
        .map(|file| (file.path.as_str(), file.kind.as_str(), file.additions))
        .collect::<Vec<_>>();
    files.sort();
    assert_eq!(
        files,
        vec![("new.txt", "added", 1), ("tracked.txt", "modified", 1)]
    );
    assert_eq!(git(&repository, &["status", "--porcelain"]), status_before);
    let first_commit = git(&repository, &["rev-parse", &first]);

    // Capturing an existing ref returns it without moving it.
    fs::write(repository.join("later.txt"), "later\n").unwrap();
    let again = port.capture(&repository, &first, None).unwrap();
    assert_eq!(git(&repository, &["rev-parse", &first]), first_commit);
    assert_eq!(again.files.len(), 2);

    // The next checkpoint diffs against the previous one.
    let second = thread_ref(thread, scope, 1);
    let next = port.capture(&repository, &second, Some(&first)).unwrap();
    assert_eq!(
        next.files
            .iter()
            .map(|file| (file.path.as_str(), file.kind.as_str()))
            .collect::<Vec<_>>(),
        vec![("later.txt", "added")]
    );
    // Both live under the thread's namespace.
    let listed = git(
        &repository,
        &[
            "for-each-ref",
            "--format=%(refname)",
            &format!("refs/waku/t/{thread}"),
        ],
    );
    assert_eq!(listed.lines().count(), 2);

    // Restoring the first checkpoint brings its files back exactly.
    fs::write(repository.join("tracked.txt"), "scribbled\n").unwrap();
    port.restore(&repository, &first).unwrap();
    assert_eq!(
        fs::read_to_string(repository.join("tracked.txt")).unwrap(),
        "base\nstaged\n"
    );
    assert!(repository.join("new.txt").exists());
    assert!(!repository.join("later.txt").exists());

    // Refs outside refs/waku/, missing refs and non-repositories fail for good.
    let error = port
        .capture(&repository, "refs/heads/main", None)
        .unwrap_err();
    assert!(!error.retryable);
    let error = port
        .restore(&repository, &thread_ref(thread, scope, 9))
        .unwrap_err();
    assert!(!error.retryable);
    let plain = temp.0.join("plain");
    fs::create_dir_all(&plain).unwrap();
    assert!(!port.capture(&plain, &first, None).unwrap_err().retryable);
}

fn new_worktree(base: &str, branch: Option<&str>) -> WorkspaceStrategy {
    WorkspaceStrategy::NewWorktree {
        base_ref: base.to_owned(),
        branch: branch.map(str::to_owned),
        start_from_origin: false,
    }
}

#[test]
fn worktrees_are_created_once_per_thread_and_removed_safely() {
    let temp = TempDir::new("worktrees");
    let repository = repository(&temp.0);
    let project = repository.join("packages/app");
    let root = temp.0.join("managed");
    let port = GitWorkspacePort::with_root(root.clone());
    let project_id = ProjectId(Uuid::new_v4());
    let thread_id = ThreadId(Uuid::new_v4());
    let strategy = new_worktree("main", None);
    let request = PrepareRequest {
        project_id,
        thread_id,
        project_root: &project,
        strategy: &strategy,
        name_hint: "Fix the parser spans!",
    };
    let phases = Mutex::new(Vec::new());
    let progress = |phase: PreparePhase| phases.lock().push(phase);

    let prepared = port.prepare(&request, &progress).unwrap();
    assert_eq!(*phases.lock(), vec![PreparePhase::Worktree]);
    let short = &thread_id.0.simple().to_string()[..8];
    let name = format!("fix-the-parser-spans-{short}");
    assert_eq!(
        prepared.branch.as_deref(),
        Some(format!("waku/{name}").as_str())
    );
    assert!(prepared.owned);
    let worktree = root.join(project_id.to_string()).join(&name);
    assert_eq!(prepared.cwd, worktree.join("packages/app"));
    assert_eq!(prepared.worktree_path.as_ref(), Some(&prepared.cwd));
    assert_eq!(
        fs::read_to_string(prepared.cwd.join("README.md")).unwrap(),
        "one\n"
    );
    assert!(git(&repository, &["worktree", "list"]).contains(&name));

    // A retried preparation finds the same worktree.
    assert_eq!(port.prepare(&request, &progress).unwrap(), prepared);

    // Uncommitted work keeps the worktree.
    fs::write(worktree.join("wip.txt"), "work in progress\n").unwrap();
    let branch = prepared.branch.clone().unwrap();
    let error = port.remove_worktree(&worktree, Some(&branch)).unwrap_err();
    assert!(!error.retryable, "{error:?}");
    assert!(error.message.contains("UncommittedChanges"), "{error:?}");
    assert!(worktree.exists());

    // Clean, it goes, with its branch; removing again is a no-op.
    fs::remove_file(worktree.join("wip.txt")).unwrap();
    port.remove_worktree(&worktree, Some(&branch)).unwrap();
    assert!(!worktree.exists());
    assert!(!git(&repository, &["branch", "--list", &branch]).contains(&branch));
    port.remove_worktree(&worktree, Some(&branch)).unwrap();

    // A named branch is honoured; a hostile one is refused.
    let named = new_worktree("main", Some("feature/spans"));
    let other_thread = ThreadId(Uuid::new_v4());
    let prepared = port
        .prepare(
            &PrepareRequest {
                thread_id: other_thread,
                strategy: &named,
                ..request
            },
            &progress,
        )
        .unwrap();
    assert_eq!(prepared.branch.as_deref(), Some("feature/spans"));
    let hostile = new_worktree("main", Some("--upload-pack=evil"));
    assert!(
        port.prepare(
            &PrepareRequest {
                thread_id: ThreadId(Uuid::new_v4()),
                strategy: &hostile,
                ..request
            },
            &progress,
        )
        .is_err()
    );
    // A committed but unpushed branch keeps its worktree by default.
    let feature_dir = prepared.worktree_path.clone().unwrap();
    let feature_root = PathBuf::from(git(&feature_dir, &["rev-parse", "--show-toplevel"]));
    fs::write(feature_root.join("feature.txt"), "feature\n").unwrap();
    git(&feature_root, &["add", "."]);
    git(&feature_root, &["commit", "-q", "-m", "feature"]);
    let error = port
        .remove_worktree(&feature_root, Some("feature/spans"))
        .unwrap_err();
    assert!(error.message.contains("UnpushedCommits"), "{error:?}");
}

#[test]
fn root_and_existing_workspaces_are_used_as_they_are() {
    let temp = TempDir::new("workspaces");
    let repository = repository(&temp.0);
    let port = GitWorkspacePort::with_root(temp.0.join("managed"));
    let request = |strategy| PrepareRequest {
        project_id: ProjectId(Uuid::new_v4()),
        thread_id: ThreadId(Uuid::new_v4()),
        project_root: &repository,
        strategy,
        name_hint: "x",
    };
    let root = WorkspaceStrategy::Root { branch: None };
    let prepared = port.prepare(&request(&root), &|_| {}).unwrap();
    assert_eq!(prepared.cwd, repository);
    assert_eq!(prepared.branch.as_deref(), Some("main"));
    assert!(!prepared.owned && prepared.worktree_path.is_none());

    let missing = WorkspaceStrategy::ExistingWorktree {
        path: temp.0.join("nowhere"),
        branch: None,
    };
    assert!(
        !port
            .prepare(&request(&missing), &|_| {})
            .unwrap_err()
            .retryable
    );
}

/// Records prompts and answers from a script.
struct FakeTitles {
    prompts: Mutex<Vec<(Option<String>, String)>>,
    answer: anyhow::Result<Option<String>>,
}

impl FakeTitles {
    fn answering(answer: Option<&str>) -> Arc<Self> {
        Arc::new(Self {
            prompts: Mutex::new(Vec::new()),
            answer: Ok(answer.map(str::to_owned)),
        })
    }
}

impl TitleBackend for FakeTitles {
    fn generate(&self, model: Option<&str>, prompt: &str) -> anyhow::Result<Option<String>> {
        self.prompts
            .lock()
            .push((model.map(str::to_owned), prompt.to_owned()));
        match &self.answer {
            Ok(answer) => Ok(answer.clone()),
            Err(error) => Err(anyhow::anyhow!("{error}")),
        }
    }
}

#[test]
fn titles_route_by_driver_and_are_normalized() {
    let codex = FakeTitles::answering(Some("  \"Fix parser spans.\"\nsecond line"));
    let fallback = FakeTitles::answering(None);
    let port = ProviderTitlePort::new()
        .route(DriverKind::Codex, codex.clone())
        .fallback(fallback.clone());
    let selection = ModelSelection {
        model: Some("gpt-5.6-luna".into()),
        ..ModelSelection::new("codex")
    };
    let long_prompt = "word ".repeat(2_000);
    assert_eq!(
        port.generate(&selection, &long_prompt).unwrap().as_deref(),
        Some("Fix parser spans")
    );
    let (model, prompt) = codex.prompts.lock()[0].clone();
    assert_eq!(model.as_deref(), Some("gpt-5.6-luna"));
    assert_eq!(prompt.chars().count(), titles::MAX_PROMPT_CHARS);

    // Other drivers use the fallback; without one there is no title.
    assert_eq!(
        port.generate(&ModelSelection::new("claude"), "hi").unwrap(),
        None
    );
    assert_eq!(fallback.prompts.lock().len(), 1);
    let bare = ProviderTitlePort::new();
    assert_eq!(bare.generate(&selection, "hi").unwrap(), None);
    // Blank prompts never reach a backend.
    assert_eq!(port.generate(&selection, "   ").unwrap(), None);
    assert_eq!(codex.prompts.lock().len(), 1);

    // Backend errors are retried; a missing CLI is simply no title.
    let failing = Arc::new(FakeTitles {
        prompts: Mutex::new(Vec::new()),
        answer: Err(anyhow::anyhow!("rate limited")),
    });
    let port = ProviderTitlePort::new().route(DriverKind::Codex, failing);
    assert!(port.generate(&selection, "hi").unwrap_err().retryable);
    let missing = ProviderTitlePort::new().route(
        DriverKind::Codex,
        Arc::new(CodexTitleBackend::with_binary(std::env::temp_dir(), || {
            None
        })),
    );
    assert_eq!(missing.generate(&selection, "hi").unwrap(), None);
    assert_eq!(titles::normalize_title("\n  \n"), None);
    assert_eq!(
        titles::normalize_title(&"x".repeat(200)).unwrap().len(),
        titles::MAX_TITLE_CHARS
    );
}

#[derive(Default)]
struct RecordingTerminals(Mutex<Vec<Uuid>>);

impl ThreadTerminals for RecordingTerminals {
    fn close_thread(&self, thread_id: Uuid) -> usize {
        self.0.lock().push(thread_id);
        1
    }
}

#[test]
fn cleanup_closes_terminals_and_removes_only_attachment_trees() {
    let temp = TempDir::new("cleanup");
    let store_root = temp.0.join("attachments");
    let store = AttachmentStore::new(store_root.clone());
    let source = temp.0.join("note.txt");
    fs::write(&source, "hello").unwrap();
    let stored = store.import_path(&source).unwrap();
    assert!(store.path_for(&stored.reference).unwrap().exists());

    let terminals = Arc::new(RecordingTerminals::default());
    let port = CoreCleanupPort::new(terminals.clone(), AttachmentStore::new(store_root));
    let thread_id = ThreadId(Uuid::new_v4());
    port.terminals(thread_id).unwrap();
    assert_eq!(*terminals.0.lock(), vec![thread_id.0]);

    port.attachments(&[
        stored.reference.clone(),
        "waku-blob:abcdef.png".to_owned(),
        "waku-attachment:not-a-uuid".to_owned(),
    ])
    .unwrap();
    assert!(!store.path_for(&stored.reference).unwrap().exists());
    // Idempotent.
    port.attachments(&[stored.reference]).unwrap();

    let map = TerminalMap::default();
    assert_eq!(map.close_thread(thread_id.0), 0);
}

/// The ports wired into the orchestrator's default executor: a first message
/// generates the thread's title through the title port.
#[test]
fn the_default_executor_runs_core_ports() {
    use waku_orchestrator::fakes::ScriptedExecutor;
    use waku_orchestrator::{
        DefaultExecutor, Dependencies, EffectWorkerPool, NoCapabilities, Orchestrator,
        OrchestratorConfig, SequentialIds, SystemClock, WorkerConfig,
    };

    let temp = TempDir::new("executor");
    let repository = repository(&temp.0);
    let orchestrator = Orchestrator::open(
        OrchestratorConfig::new(temp.0.join("state.sqlite")),
        Dependencies {
            clock: Arc::new(SystemClock),
            ids: Box::new(SequentialIds::new(7)),
            capabilities: Arc::new(NoCapabilities),
        },
    )
    .unwrap();
    let titles = FakeTitles::answering(Some("Generated title"));
    let ports = core_ports(
        GitWorkspacePort::with_root(temp.0.join("managed")),
        ProviderTitlePort::new().route(DriverKind::Codex, titles.clone()),
        CoreCleanupPort::new(
            Arc::new(RecordingTerminals::default()),
            AttachmentStore::new(temp.0.join("attachments")),
        ),
    );
    let pool = EffectWorkerPool::start(
        orchestrator.clone(),
        Arc::new(DefaultExecutor {
            provider: Arc::new(ScriptedExecutor::default()),
            ports,
        }),
        WorkerConfig {
            workers: 2,
            ..WorkerConfig::default()
        },
    );
    let envelope = |id: &str, command| CommandEnvelope {
        command_id: CommandId::new(id).unwrap(),
        issued_by: Actor::User,
        source: CreationSource::Desktop,
        command,
    };
    let project_id = ProjectId(Uuid::new_v4());
    let thread_id = ThreadId(Uuid::new_v4());
    orchestrator
        .dispatch(envelope(
            "project",
            Command::ProjectCreate {
                project_id,
                name: "repo".into(),
                path: repository.clone(),
                order_key: None,
            },
        ))
        .unwrap();
    orchestrator
        .dispatch(envelope(
            "thread",
            Command::ThreadCreate {
                thread_id,
                project_id,
                title: None,
                model_selection: ModelSelection::new("codex"),
                runtime_mode: RuntimeMode::FullAccess,
                interaction_mode: InteractionMode::Default,
                workspace: WorkspaceStrategy::Root { branch: None },
                imported_native_thread: None,
            },
        ))
        .unwrap();
    orchestrator
        .dispatch(envelope(
            "message",
            Command::MessageDispatch {
                thread_id,
                message_id: MessageId(Uuid::new_v4()),
                text: "Please fix the parser".into(),
                display_text: None,
                attachments: Vec::new(),
                context: Vec::new(),
                model_selection: None,
                mode: DispatchMode::Auto,
                title_seed: None,
                source_plan: None,
            },
        ))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while orchestrator.thread(thread_id).unwrap().thread.title != "Generated title" {
        assert!(Instant::now() < deadline, "the title was never generated");
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(titles.prompts.lock()[0].1, "Please fix the parser");
    pool.shutdown(Duration::from_secs(1));
}
