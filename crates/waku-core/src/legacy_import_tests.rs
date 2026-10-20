//! Importing Waku `app.db` files into the orchestrator (#63), against
//! fixture databases written by this crate's own persistence code and by
//! hand at every upstream migration (0000–0002).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rusqlite::{Connection, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;
use waku_orchestrator::import::legacy::{
    self, LegacySource, NOTICE_INTERRUPTED, TranscriptOutcome, import_shells,
};
use waku_orchestrator::search::ThreadFindQuery;
use waku_orchestrator::{
    Dependencies, NoCapabilities, Orchestrator, OrchestratorConfig, SystemClock, UuidV7Ids,
};
use waku_protocol::orchestration::{self as orch, *};

use crate::model::{
    ActivityFileChange, ActivityFileChangeStatus, ActivityItem, ActivityKind, AgentSession,
    AgentTurn, Checkpoint as LegacyCheckpoint, CheckpointFile as LegacyCheckpointFile,
    CheckpointStatus as LegacyCheckpointStatus, ContextUsage as LegacyContextUsage,
    Message as LegacyMessage, MessageAttachment, MessageRole as LegacyRole, ProviderKind,
    ProviderResumeCursor, QueuedMessage, ReasoningBlock, RuntimeMode as LegacyRuntimeMode,
    SessionStatus as LegacySessionStatus, SessionWorkspace, ThreadGoal as LegacyGoal,
    ThreadGoalStatus, TranscriptBlock, TurnStatus,
};
use crate::persistence::{PersistedState, StateStore};

struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!("waku-import-{label}-{}", Uuid::new_v4()));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn orchestrator(dir: &Path) -> Arc<Orchestrator> {
    Orchestrator::open(
        OrchestratorConfig::new(dir.join("state.sqlite")),
        Dependencies {
            clock: Arc::new(SystemClock),
            ids: Box::new(UuidV7Ids),
            capabilities: Arc::new(NoCapabilities),
        },
    )
    .unwrap()
}

/// SHA-256 of every SQLite file of a database (main, WAL, shared memory).
fn database_hash(path: &Path) -> Vec<(String, String)> {
    ["", "-wal", "-shm"]
        .iter()
        .filter_map(|suffix| {
            let file = PathBuf::from(format!("{}{suffix}", path.display()));
            let bytes = fs::read(&file).ok()?;
            Some((
                suffix.to_string(),
                Sha256::digest(&bytes)
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect(),
            ))
        })
        .collect()
}

const STARTED: u64 = 1_700_000_000;

struct Fixture {
    path: PathBuf,
    project: Uuid,
    other_project: Uuid,
    rich: Uuid,
    plain: Uuid,
    turns: [Uuid; 3],
    messages: Vec<Uuid>,
    queued: Uuid,
    activities: Vec<Uuid>,
}

/// One rich session (a worktree, a Codex cursor, three turns with a
/// checkpoint, activities, attachments, a queue, busy when the app quit) and
/// one plain session in another project, saved by `StateStore`.
fn fork_fixture(dir: &Path) -> Fixture {
    let path = dir.join("app.db");
    let store = StateStore::new(path.clone());
    let mut state = PersistedState::empty();
    let project = crate::model::Project {
        id: Uuid::new_v4(),
        name: "Parser".into(),
        path: dir.join("parser"),
        created_at: STARTED - 100,
    };
    let other = crate::model::Project {
        id: Uuid::new_v4(),
        name: "Docs".into(),
        path: dir.join("docs"),
        created_at: STARTED - 50,
    };
    let (project_id, other_id) = (project.id, other.id);
    state.projects = vec![project, other];

    let mut rich = AgentSession::new(project_id, ProviderKind::Codex);
    rich.auto_title = Some("Parser spans".into());
    rich.workspace = SessionWorkspace::Worktree {
        path: dir.join("worktrees/parser-spans"),
        branch: "waku/parser-spans".into(),
    };
    rich.model = Some("gpt-5.6".into());
    rich.reasoning_effort = Some("high".into());
    rich.service_tier = Some("flex".into());
    rich.context_window = Some("1m".into());
    rich.runtime_mode = LegacyRuntimeMode::AutoAcceptEdits;
    rich.provider_cursor = Some(ProviderResumeCursor::Codex {
        thread_id: "codex-thread-1".into(),
    });
    rich.thread_goal = Some(LegacyGoal {
        objective: "Spans everywhere".into(),
        status: ThreadGoalStatus::Active,
        token_budget: Some(10_000),
        tokens_used: 1_200,
        time_used_seconds: 30,
    });
    rich.context_usage = Some(LegacyContextUsage {
        tokens: 4_000,
        window: Some(200_000),
    });
    rich.status = LegacySessionStatus::Working;
    rich.created_at = STARTED;
    rich.updated_at = STARTED + 500;
    rich.last_reply_at = Some(STARTED + 400);
    let turns = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    rich.turns = vec![
        AgentTurn {
            id: turns[0],
            turn_count: 1,
            status: TurnStatus::Completed,
            provider_turn_started: true,
            provider_resume_at: Some("resp-1".into()),
            started_at: STARTED + 10,
            completed_at: Some(STARTED + 20),
            checkpoint: Some(LegacyCheckpoint {
                turn_count: 1,
                git_ref: format!("refs/waku/session-{}-turn-1", Uuid::nil()),
                status: LegacyCheckpointStatus::Ready,
                files: vec![LegacyCheckpointFile {
                    path: "src/lexer.rs".into(),
                    additions: 12,
                    deletions: 3,
                }],
                additions: 12,
                deletions: 3,
                created_at: STARTED + 21,
            }),
        },
        AgentTurn {
            id: turns[1],
            turn_count: 2,
            status: TurnStatus::Failed,
            provider_turn_started: true,
            provider_resume_at: None,
            started_at: STARTED + 100,
            completed_at: Some(STARTED + 110),
            checkpoint: None,
        },
        AgentTurn {
            id: turns[2],
            turn_count: 3,
            status: TurnStatus::Running,
            provider_turn_started: true,
            provider_resume_at: None,
            started_at: STARTED + 400,
            completed_at: None,
            checkpoint: None,
        },
    ];
    let attachment = MessageAttachment {
        path: dir.join("note.md"),
        mention: "@note.md".into(),
        name: "note.md".into(),
        is_dir: false,
        is_image: false,
        blob_reference: Some(format!("waku-attachment:{}", Uuid::new_v4())),
    };
    let message = |turn: Option<Uuid>, role, content: &str, at: u64| LegacyMessage {
        id: Uuid::new_v4(),
        turn_id: turn,
        role,
        content: content.into(),
        display_content: None,
        attachments: Vec::new(),
        created_at: at,
        streaming: false,
    };
    let mut first = message(
        Some(turns[0]),
        LegacyRole::User,
        "Make tokens carry spans @note.md",
        STARTED + 10,
    );
    first.display_content = Some("Make tokens carry spans".into());
    first.attachments = vec![attachment];
    let mut streaming = message(
        Some(turns[1]),
        LegacyRole::Assistant,
        "Halfway thro",
        STARTED + 105,
    );
    streaming.streaming = true;
    let messages = vec![
        first,
        message(
            Some(turns[0]),
            LegacyRole::Assistant,
            "Tokens now carry a byte span.",
            STARTED + 19,
        ),
        message(
            Some(turns[1]),
            LegacyRole::User,
            "Now the parser",
            STARTED + 100,
        ),
        streaming,
        message(None, LegacyRole::System, "Context compacted", STARTED + 300),
        message(
            Some(turns[2]),
            LegacyRole::User,
            "Keep going",
            STARTED + 400,
        ),
    ];
    let message_ids = messages.iter().map(|message| message.id).collect();
    rich.messages = messages.into();

    let reasoning = ActivityItem::from_reasoning(
        ReasoningBlock {
            content: "Spans need byte offsets.".into(),
            started_at_ms: (STARTED + 11) * 1_000,
            finished_at_ms: (STARTED + 12) * 1_000,
        },
        true,
    );
    let mut command = ActivityItem::new(
        Some("call-1".into()),
        ActivityKind::Command,
        "Run tests",
        None,
        true,
    );
    command.display_target = Some("cargo test".into());
    command.output = Some("1 failed".into());
    command.failed = true;
    let mut edit = ActivityItem::new(
        Some("call-2".into()),
        ActivityKind::FileChange,
        "Edit lexer",
        None,
        true,
    );
    edit.file_changes = vec![ActivityFileChange {
        path: "src/lexer.rs".into(),
        additions: Some(12),
        deletions: Some(3),
        status: Some(ActivityFileChangeStatus::Modified),
        diff: Some("@@\n-old\n+new\n".into()),
    }];
    let mut search = ActivityItem::new(
        Some("call-3".into()),
        ActivityKind::Search,
        "Search the web",
        None,
        true,
    );
    search.display_target = Some("rust byte spans".into());
    let mut read = ActivityItem::new(
        Some("call-4".into()),
        ActivityKind::FileRead,
        "Read lexer",
        None,
        false,
    );
    read.tool_name = Some("Read".into());
    read.arguments = Some("{\"path\":\"src/lexer.rs\"}".into());
    let activities = [&reasoning, &command, &edit, &search, &read]
        .iter()
        .map(|activity| activity.id)
        .collect();
    rich.transcript_blocks = vec![TranscriptBlock {
        after_message: 1,
        turn_id: Some(turns[0]),
        activities: vec![reasoning, command, edit, search, read],
    }]
    .into();
    let queued = QueuedMessage::with_presentation("Then document it", None, Vec::new());
    let queued_id = queued.id;
    rich.queued_messages = vec![queued];
    let rich_id = rich.id;

    let mut plain = AgentSession::new(other_id, ProviderKind::Claude);
    plain.title = "My own title".into();
    plain.created_at = STARTED + 1_000;
    plain.updated_at = STARTED + 1_000;
    plain.last_reply_at = Some(STARTED + 1_000);
    plain.begin_turn("hello there");
    plain.push_message(LegacyRole::Assistant, "general kenobi");
    plain.finish_active_turn(TurnStatus::Completed);
    let plain_id = plain.id;

    state.push_session(rich);
    state.push_session(plain);
    store.save(&mut state).unwrap();
    drop(store);
    Fixture {
        path,
        project: project_id,
        other_project: other_id,
        rich: rich_id,
        plain: plain_id,
        turns,
        messages: message_ids,
        queued: queued_id,
        activities,
    }
}

#[test]
fn an_upstream_database_imports_read_only_and_every_field_survives() {
    let source = TempDir::new("source");
    let target = TempDir::new("target");
    let fixture = fork_fixture(&source.0);
    let before = database_hash(&fixture.path);
    let orchestrator = orchestrator(&target.0);

    let report = import_shells(&orchestrator, &LegacySource::upstream(&fixture.path)).unwrap();
    assert_eq!((report.projects, report.threads, report.skipped), (2, 2, 0));
    assert!(report.failed.is_empty(), "{:?}", report.failed);

    // Projects keep ids, names, paths, order and creation time.
    let projects = orchestrator.projects();
    assert_eq!(
        projects
            .iter()
            .map(|project| project.id.0)
            .collect::<Vec<_>>(),
        vec![fixture.project, fixture.other_project]
    );
    assert_eq!(projects[0].name, "Parser");
    assert_eq!(projects[0].created_at, (STARTED as i64 - 100) * 1_000);

    // The shell.
    let rich_id = ThreadId(fixture.rich);
    let shell = orchestrator.thread(rich_id).unwrap();
    let thread = &shell.thread;
    assert_eq!(thread.project_id.0, fixture.project);
    assert_eq!(thread.title, "Parser spans");
    assert_eq!(thread.title_source, TitleSource::Generated);
    assert_eq!(thread.history_origin, HistoryOrigin::WakuImport);
    assert_eq!(thread.creation_source, CreationSource::Import);
    assert_eq!(thread.model_selection.instance.as_str(), "codex");
    assert_eq!(thread.model_selection.model.as_deref(), Some("gpt-5.6"));
    assert_eq!(
        thread.model_selection.reasoning_effort.as_deref(),
        Some("high")
    );
    assert_eq!(thread.model_selection.service_tier.as_deref(), Some("flex"));
    assert_eq!(thread.model_selection.context_window.as_deref(), Some("1m"));
    assert_eq!(thread.runtime_mode, orch::RuntimeMode::AutoAcceptEdits);
    assert_eq!(thread.branch.as_deref(), Some("waku/parser-spans"));
    assert_eq!(
        thread.worktree_path.as_deref(),
        Some(source.0.join("worktrees/parser-spans").as_path())
    );
    assert!(thread.worktree_owned);
    assert_eq!(thread.created_at, STARTED as i64 * 1_000);
    assert_eq!(thread.updated_at, (STARTED as i64 + 500) * 1_000);
    assert_eq!(thread.last_reply_at, Some((STARTED as i64 + 400) * 1_000));
    // The provider cursor is a strong native ref with the original cursor
    // kept for native resume (R23).
    let provider_thread =
        &shell.provider_threads[&thread.active_provider_thread_id.expect("native thread")];
    assert_eq!(
        provider_thread.native_thread_ref,
        Some(ProviderRef::strong(DriverKind::Codex, "codex-thread-1"))
    );
    assert_eq!(provider_thread.status, ProviderThreadStatus::NotLoaded);
    let cursor: ProviderResumeCursor = serde_json::from_str(
        &provider_thread
            .native_metadata
            .as_ref()
            .and_then(|metadata| metadata.resume_cursor.clone())
            .unwrap()
            .0,
    )
    .unwrap();
    assert_eq!(
        cursor,
        ProviderResumeCursor::Codex {
            thread_id: "codex-thread-1".into()
        }
    );
    assert_eq!(
        provider_thread
            .goal
            .as_ref()
            .map(|goal| goal.objective.as_str()),
        Some("Spans everywhere")
    );
    assert_eq!(
        provider_thread.context_usage.map(|usage| usage.tokens),
        Some(4_000)
    );
    // No history yet: transcripts come lazily.
    assert!(shell.runs.is_empty());
    let mut pending = orchestrator.pending_transcripts();
    pending.sort();
    let mut expected = vec![rich_id, ThreadId(fixture.plain)];
    expected.sort();
    assert_eq!(pending, expected);

    let plain = orchestrator.thread(ThreadId(fixture.plain)).unwrap();
    assert_eq!(plain.thread.title, "My own title");
    assert_eq!(plain.thread.title_source, TitleSource::User);
    assert!(plain.thread.active_provider_thread_id.is_none());

    // A find over the imported thread imports its transcript first.
    let found = orchestrator
        .find_in_thread(&ThreadFindQuery::new(rich_id, "byte span"))
        .unwrap();
    assert_eq!(found.matches.len(), 1);
    assert!(!orchestrator.pending_transcripts().contains(&rich_id));
    let projection = orchestrator.thread(rich_id).unwrap();

    // Runs: the turn ids, their ordinals and statuses.
    let mut runs = projection.runs.values().collect::<Vec<_>>();
    runs.sort_by_key(|run| (run.ordinal.is_none(), run.ordinal));
    assert_eq!(
        runs.iter()
            .map(|run| (run.id.0, run.ordinal, run.status))
            .collect::<Vec<_>>(),
        vec![
            (fixture.turns[0], Some(1), RunStatus::Completed),
            (fixture.turns[1], Some(2), RunStatus::Failed),
            (fixture.turns[2], Some(3), RunStatus::Interrupted),
            (
                waku_orchestrator::ids::derived_id("run", &fixture.queued.to_string()),
                None,
                RunStatus::Queued
            ),
        ]
    );
    let first = runs[0];
    assert_eq!(first.user_message_id.0, fixture.messages[0]);
    assert_eq!(first.started_at, Some((STARTED as i64 + 10) * 1_000));
    assert_eq!(first.completed_at, Some((STARTED as i64 + 20) * 1_000));
    let queued = runs[3];
    assert!(queued.queue_held);
    assert_eq!(queued.queue_position, Some(1));
    // Attempts, root nodes and provider turns, with the weak native ref.
    let attempt = &projection.attempts[&first.active_attempt_id.unwrap()];
    assert_eq!(attempt.reason, AttemptReason::Initial);
    assert_eq!(attempt.native_thread_id.as_deref(), Some("codex-thread-1"));
    let node = &projection.nodes[&first.root_node_id.unwrap()];
    assert_eq!(node.kind, NodeKind::RootTurn);
    let turn = &projection.provider_turns[&attempt.provider_turn_id.unwrap()];
    assert_eq!(turn.ordinal, 1);
    assert_eq!(
        turn.native_turn_ref
            .as_ref()
            .map(|reference| (reference.native_id.as_deref(), reference.strength)),
        Some((Some("resp-1"), RefStrength::Weak))
    );
    let coverage = &projection.provider_threads[&provider_thread.id];
    assert_eq!(
        (coverage.first_run_ordinal, coverage.last_run_ordinal),
        (Some(1), Some(3))
    );
    // The checkpoint keeps its ref name.
    let checkpoint = &projection.checkpoints[&first.checkpoint_id.unwrap()];
    assert_eq!(
        checkpoint.git_ref,
        format!("refs/waku/session-{}-turn-1", Uuid::nil())
    );
    assert_eq!(checkpoint.status, CheckpointStatus::Ready);
    assert_eq!(checkpoint.app_run_ordinal, Some(1));
    assert_eq!(
        checkpoint
            .files
            .iter()
            .map(|file| (file.path.as_str(), file.additions, file.deletions))
            .collect::<Vec<_>>(),
        vec![("src/lexer.rs", 12, 3)]
    );
    let scope = &projection.checkpoint_scopes[&checkpoint.scope_id];
    assert_eq!(scope.kind, ScopeKind::RootRun);
    assert_eq!(
        scope.cwd,
        source.0.join("worktrees/parser-spans"),
        "a worktree thread's checkpoints live in its worktree"
    );

    // Messages keep ids, text, display text and attachments.
    let messages = orchestrator.reader().with(|connection| {
        let mut statement = connection
            .prepare("SELECT payload_json FROM p_messages WHERE thread_id = ?1")
            .unwrap();
        statement
            .query_map([rich_id.to_string()], |row| row.get::<_, String>(0))
            .unwrap()
            .map(|json| serde_json::from_str::<Message>(&json.unwrap()).unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(messages.len(), 7);
    let prompt = messages
        .iter()
        .find(|message| message.id.0 == fixture.messages[0])
        .unwrap();
    assert_eq!(prompt.text, "Make tokens carry spans @note.md");
    assert_eq!(
        prompt.display_text.as_deref(),
        Some("Make tokens carry spans")
    );
    assert_eq!(prompt.attachments[0].mention, "@note.md");
    assert!(prompt.attachments[0].blob_reference.is_some());
    assert_eq!(prompt.run_id, Some(RunId(fixture.turns[0])));
    assert_eq!(prompt.created_at, (STARTED as i64 + 10) * 1_000);

    // Turn items, in transcript order.
    let items = orchestrator.items_around(rich_id, 0, 0, 1_000).unwrap();
    let summary = items
        .iter()
        .map(|item| {
            let detail = match &item.kind {
                TurnItemKind::UserMessage { text, .. } => text.clone(),
                TurnItemKind::AssistantMessage { text, .. } => text.clone(),
                TurnItemKind::Reasoning { text, .. } => text.clone(),
                TurnItemKind::CommandExecution { input, output, .. } => {
                    format!("{input} -> {}", output.clone().unwrap_or_default())
                }
                TurnItemKind::FileChange {
                    file_name,
                    additions,
                    deletions,
                    diff,
                    ..
                } => format!(
                    "{file_name} +{additions:?} -{deletions:?} {}",
                    diff.is_some()
                ),
                TurnItemKind::WebSearch { patterns, .. } => patterns.join(","),
                TurnItemKind::DynamicTool { tool_name, .. } => {
                    tool_name.clone().unwrap_or_default()
                }
                TurnItemKind::Checkpoint { files, .. } => format!("{} files", files.len()),
                TurnItemKind::SystemNotice { message } => message.clone(),
                other => other.type_name().to_owned(),
            };
            (item.block(), item.kind.type_name(), item.status, detail)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        summary,
        vec![
            (
                1,
                "user_message",
                ItemStatus::Completed,
                "Make tokens carry spans @note.md".into()
            ),
            (
                1,
                "reasoning",
                ItemStatus::Completed,
                "Spans need byte offsets.".into()
            ),
            (
                1,
                "command_execution",
                ItemStatus::Failed,
                "cargo test -> 1 failed".into()
            ),
            (
                1,
                "file_change",
                ItemStatus::Completed,
                "src/lexer.rs +Some(12) -Some(3) true".into()
            ),
            (
                1,
                "web_search",
                ItemStatus::Completed,
                "rust byte spans".into()
            ),
            (1, "dynamic_tool", ItemStatus::Interrupted, "Read".into()),
            (
                1,
                "assistant_message",
                ItemStatus::Completed,
                "Tokens now carry a byte span.".into()
            ),
            (1, "checkpoint", ItemStatus::Completed, "1 files".into()),
            (
                2,
                "user_message",
                ItemStatus::Completed,
                "Now the parser".into()
            ),
            (
                2,
                "assistant_message",
                ItemStatus::Interrupted,
                "Halfway thro".into()
            ),
            (
                2,
                "system_notice",
                ItemStatus::Completed,
                "Context compacted".into()
            ),
            (
                3,
                "user_message",
                ItemStatus::Completed,
                "Keep going".into()
            ),
            (
                3,
                "system_notice",
                ItemStatus::Completed,
                NOTICE_INTERRUPTED.into()
            ),
        ]
    );
    // Activity items keep their ids' identity and the original JSON.
    let tool = items
        .iter()
        .find(|item| item.kind.type_name() == "dynamic_tool")
        .unwrap();
    let TurnItemKind::DynamicTool { input, .. } = &tool.kind else {
        unreachable!()
    };
    let original: ActivityItem = serde_json::from_str(&input.0).unwrap();
    assert_eq!(original.id, fixture.activities[4]);
    assert_eq!(
        original.arguments.as_deref(),
        Some("{\"path\":\"src/lexer.rs\"}")
    );
    assert_eq!(
        items[1].id,
        TurnItemId(waku_orchestrator::ids::derived_id(
            "imported_activity",
            &fixture.activities[0].to_string()
        ))
    );

    // The source was never written.
    assert_eq!(database_hash(&fixture.path), before);
    // Re-importing changes nothing.
    let sequence = orchestrator.last_sequence();
    let again = import_shells(&orchestrator, &LegacySource::upstream(&fixture.path)).unwrap();
    assert_eq!((again.threads, again.skipped), (0, 2));
    assert_eq!(orchestrator.last_sequence(), sequence);
    assert_eq!(orchestrator.ensure_transcript(rich_id), None);
    let report = orchestrator.verify().unwrap();
    assert!(report.is_clean(), "{report:#?}");
}

#[test]
fn the_fork_database_imports_lazily_on_first_dispatch_and_after_restart() {
    let source = TempDir::new("fork-source");
    let target = TempDir::new("fork-target");
    let fixture = fork_fixture(&source.0);
    let plain = ThreadId(fixture.plain);
    {
        let orchestrator = orchestrator(&target.0);
        let report = legacy::import_fork_app_db(&orchestrator, &fixture.path)
            .unwrap()
            .unwrap();
        assert_eq!(report.threads, 2);
        assert!(
            legacy::import_fork_app_db(&orchestrator, &target.0.join("missing.db"))
                .unwrap()
                .is_none()
        );
    }
    // After a restart the pending set comes back from `legacy_imports`.
    let orchestrator = orchestrator(&target.0);
    assert_eq!(orchestrator.pending_transcripts().len(), 2);
    // A new message: the old turn is imported first, the new run follows it.
    orchestrator
        .dispatch(CommandEnvelope {
            command_id: CommandId::new("first-new-message").unwrap(),
            issued_by: Actor::User,
            source: CreationSource::Desktop,
            command: Command::MessageDispatch {
                thread_id: plain,
                message_id: MessageId(Uuid::new_v4()),
                text: "and now?".into(),
                display_text: None,
                attachments: Vec::new(),
                context: Vec::new(),
                model_selection: None,
                mode: DispatchMode::QueueAfterActive,
                title_seed: None,
                source_plan: None,
            },
        })
        .unwrap();
    let projection = orchestrator.thread(plain).unwrap();
    let mut ordinals = projection
        .runs
        .values()
        .map(|run| run.ordinal)
        .collect::<Vec<_>>();
    ordinals.sort();
    assert_eq!(ordinals, vec![Some(1), Some(2)]);
    let items = orchestrator.items_around(plain, 0, 0, 100).unwrap();
    assert_eq!(items[0].kind.type_name(), "user_message");
    assert_eq!(items[1].kind.type_name(), "assistant_message");
    assert_eq!(items.last().unwrap().block(), 2);
    let (imported_at, error): (Option<i64>, Option<String>) =
        orchestrator.reader().with(|connection| {
            connection
            .query_row(
                "SELECT transcript_imported_at, error FROM legacy_imports WHERE session_id = ?1",
                [plain.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
        });
    assert!(imported_at.is_some() && error.is_none());
    assert!(orchestrator.verify().unwrap().is_clean());
}

#[test]
fn a_missing_source_is_recorded_and_the_thread_stays_usable() {
    let source = TempDir::new("missing-source");
    let target = TempDir::new("missing-target");
    let fixture = fork_fixture(&source.0);
    let orchestrator = orchestrator(&target.0);
    import_shells(&orchestrator, &LegacySource::upstream(&fixture.path)).unwrap();
    drop(source);
    let outcome = orchestrator.ensure_transcript(ThreadId(fixture.rich));
    assert!(
        matches!(outcome, Some(TranscriptOutcome::Failed(_))),
        "{outcome:?}"
    );
    assert!(
        orchestrator
            .ensure_transcript(ThreadId(fixture.rich))
            .is_none()
    );
    // The shell is still there and takes new work.
    orchestrator
        .dispatch(CommandEnvelope {
            command_id: CommandId::new("after-failure").unwrap(),
            issued_by: Actor::User,
            source: CreationSource::Desktop,
            command: Command::ThreadRename {
                thread_id: ThreadId(fixture.rich),
                title: "Still here".into(),
            },
        })
        .unwrap();
}

/// A database at an upstream migration, written by hand: `0000` has no
/// `auto_title`, `0001` no `display_content`/`attachments` on messages.
fn upstream_fixture(dir: &Path, migrations: usize) -> (PathBuf, Uuid, Uuid) {
    const MIGRATIONS: [&str; 3] = [
        include_str!("../../../db/migrations/0000_init.sql"),
        include_str!("../../../db/migrations/0001_clear_sugar_man.sql"),
        include_str!("../../../db/migrations/0002_cute_miracleman.sql"),
    ];
    let path = dir.join(format!("app-{migrations}.db"));
    let connection = Connection::open(&path).unwrap();
    for sql in &MIGRATIONS[..migrations] {
        for statement in sql.split("--> statement-breakpoint") {
            connection.execute_batch(statement).unwrap();
        }
    }
    let project = Uuid::new_v4();
    let session = Uuid::new_v4();
    let turn = Uuid::new_v4();
    connection
        .execute(
            "INSERT INTO projects (id, name, path, position, created_at) VALUES (?1, 'Old', '/old', 0, ?2)",
            params![project.to_string(), STARTED as i64],
        )
        .unwrap();
    let title = if migrations >= 2 {
        "New task"
    } else {
        "Old title"
    };
    connection
        .execute(
            "INSERT INTO sessions (id, project_id, title, provider, model, status, created_at,
                                   updated_at, last_reply_at)
             VALUES (?1, ?2, ?3, 'claude', 'opus', 'idle', ?4, ?4, ?4)",
            params![
                session.to_string(),
                project.to_string(),
                title,
                STARTED as i64
            ],
        )
        .unwrap();
    if migrations >= 2 {
        connection
            .execute(
                "UPDATE sessions SET auto_title = 'Upgraded title' WHERE id = ?1",
                [session.to_string()],
            )
            .unwrap();
    }
    let data = serde_json::json!({
        "id": session, "title": title, "project_id": project, "provider": "claude",
        "model": "opus", "runtime_mode": "ask", "status": "idle",
        "created_at": STARTED, "updated_at": STARTED, "last_reply_at": STARTED,
        "provider_cursor": {"provider": "claude", "sessionId": "claude-session-9"},
        "turns": [{"id": turn, "turn_count": 1, "status": "completed",
                   "started_at": STARTED, "completed_at": STARTED + 5}],
        "transcript_blocks": [],
    });
    connection
        .execute(
            "INSERT INTO session_details (session_id, data) VALUES (?1, ?2)",
            params![session.to_string(), data.to_string()],
        )
        .unwrap();
    for (position, (role, content)) in [("user", "old question"), ("assistant", "old answer")]
        .into_iter()
        .enumerate()
    {
        connection
            .execute(
                "INSERT INTO messages (id, session_id, turn_id, position, role, content,
                                       created_at, streaming)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0)",
                params![
                    Uuid::new_v4().to_string(),
                    session.to_string(),
                    turn.to_string(),
                    position as i64,
                    role,
                    content,
                    STARTED as i64
                ],
            )
            .unwrap();
    }
    (path, session, project)
}

/// An app still running holds committed rows in its WAL; they import too,
/// and the reader leaves every file as it found it.
#[test]
fn a_live_database_is_read_through_its_wal() {
    let source = TempDir::new("live");
    let target = TempDir::new("live-target");
    let (path, ..) = upstream_fixture(&source.0, 3);
    let live = Connection::open(&path).unwrap();
    live.query_row("PRAGMA journal_mode = WAL", [], |_| Ok(()))
        .unwrap();
    live.query_row("PRAGMA wal_autocheckpoint = 0", [], |_| Ok(()))
        .unwrap();
    let late = Uuid::new_v4();
    live.execute(
        "INSERT INTO projects (id, name, path, position, created_at) VALUES (?1, 'Late', '/late', 1, 1)",
        [late.to_string()],
    )
    .unwrap();
    let before = database_hash(&path);
    assert_eq!(before.len(), 3, "the WAL holds the late row");
    let orchestrator = orchestrator(&target.0);
    let report = import_shells(&orchestrator, &LegacySource::upstream(&path)).unwrap();
    assert_eq!(report.projects, 2);
    assert!(orchestrator.project(ProjectId(late)).is_some());
    // The database and its WAL are untouched. The `-shm` file is SQLite's
    // shared-memory index, where every reader of a live WAL database takes
    // its read lock; it holds no data.
    let data_files = |hashes: Vec<(String, String)>| {
        hashes
            .into_iter()
            .filter(|(suffix, _)| suffix != "-shm")
            .collect::<Vec<_>>()
    };
    assert_eq!(data_files(database_hash(&path)), data_files(before));
    drop(live);
}

#[test]
fn databases_at_every_upstream_migration_import() {
    for migrations in 1..=3 {
        let source = TempDir::new("migrations");
        let target = TempDir::new("migrations-target");
        let (path, session, project) = upstream_fixture(&source.0, migrations);
        let before = database_hash(&path);
        let orchestrator = orchestrator(&target.0);
        let report = import_shells(&orchestrator, &LegacySource::upstream(&path)).unwrap();
        assert_eq!((report.projects, report.threads), (1, 1), "{migrations}");
        let thread = orchestrator.thread(ThreadId(session)).unwrap();
        assert_eq!(thread.thread.project_id.0, project);
        let expected_title = if migrations >= 2 {
            "Upgraded title"
        } else {
            "Old title"
        };
        assert_eq!(thread.thread.title, expected_title, "{migrations}");
        let native = &thread.provider_threads[&thread.thread.active_provider_thread_id.unwrap()];
        assert_eq!(
            native.native_thread_ref,
            Some(ProviderRef::strong(DriverKind::Claude, "claude-session-9"))
        );
        assert!(matches!(
            orchestrator.ensure_transcript(ThreadId(session)),
            Some(TranscriptOutcome::Imported { runs: 1, items: 2 })
        ));
        let items = orchestrator
            .items_around(ThreadId(session), 0, 0, 10)
            .unwrap();
        assert_eq!(items.len(), 2, "{migrations}");
        assert_eq!(database_hash(&path), before, "{migrations}");
    }
}
