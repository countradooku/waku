//! Row mapping per turn item kind, and the view rules around it.

use super::*;
use serde_json::{Value, json};

const THREAD: &str = "44a520e9-3b5c-57a3-8735-2b450dd6e7ad";
const PROJECT: &str = "61cd4b0a-5f81-50ca-9956-74f86d38b966";
const RUN_1: &str = "00000000-0000-0000-0000-0000000000a1";
const RUN_2: &str = "00000000-0000-0000-0000-0000000000a2";
const RUN_3: &str = "00000000-0000-0000-0000-0000000000a3";
const SCOPE: &str = "00000000-0000-0000-0000-0000000000c1";
const CHECKPOINT: &str = "00000000-0000-0000-0000-0000000000c2";
const REQUEST: &str = "00000000-0000-0000-0000-0000000000d1";
const QUESTION: &str = "00000000-0000-0000-0000-0000000000d2";

fn id(n: u128) -> String {
    Uuid::from_u128(n).to_string()
}

fn thread() -> AppThread {
    serde_json::from_value(json!({
        "id": THREAD,
        "projectId": PROJECT,
        "createdBy": "user",
        "creationSource": "desktop",
        "title": "Fixture",
        "titleSource": "user",
        "modelSelection": {"instance": "claude", "model": "opus"},
        "runtimeMode": "auto_accept_edits",
        "interactionMode": "default",
        "historyOrigin": "native",
        "lineage": {"rootThreadId": THREAD},
        "createdAt": 1_700_000_000_000_i64,
        "updatedAt": 1_700_000_005_000_i64,
        "lastReplyAt": 1_700_000_004_000_i64,
        "lastVisitedAt": 1_700_000_000_000_i64,
        "worktreePath": "/work/tree",
        "branch": "waku/fixture",
    }))
    .unwrap()
}

fn run(run_id: &str, ordinal: Option<u32>, status: &str, message: u128) -> Run {
    let mut value = json!({
        "id": run_id,
        "threadId": THREAD,
        "modelSelection": {"instance": "claude"},
        "userMessageId": id(message),
        "status": status,
        "requestedAt": 1_700_000_001_000_i64,
        "startedAt": 1_700_000_001_000_i64,
    });
    if let Some(ordinal) = ordinal {
        value["ordinal"] = json!(ordinal);
    }
    if status == "completed" {
        value["completedAt"] = json!(1_700_000_003_000_i64);
    }
    serde_json::from_value(value).unwrap()
}

fn item(n: u128, run_id: &str, index: u32, status: &str, kind: Value) -> TurnItem {
    let block = if run_id == RUN_2 { 2 } else { 1 };
    serde_json::from_value(json!({
        "id": id(n),
        "threadId": THREAD,
        "runId": run_id,
        "position": TurnItem::position_of(block, index),
        "status": status,
        "startedAt": 1_700_000_002_000_i64,
        "updatedAt": 1_700_000_002_000_i64,
        "kind": kind,
    }))
    .unwrap()
}

fn prompt(n: u128, run_id: &str, message: u128, text: &str) -> TurnItem {
    item(
        n,
        run_id,
        0,
        "completed",
        json!({
            "type": "user_message", "messageId": id(message), "inputIntent": "turn_start",
            "text": text, "createdBy": "user",
        }),
    )
}

fn state(items: Vec<TurnItem>, runs: Vec<Run>) -> ThreadState {
    let mut state = ThreadState::new();
    state.thread = Some(thread());
    state.synchronized = true;
    state.runs = runs.into_iter().map(|run| (run.id, run)).collect();
    state.turn_items = items.into_iter().map(|item| (item.id, item)).collect();
    state
}

fn every_kind() -> Vec<TurnItem> {
    vec![
        item(
            1,
            RUN_1,
            0,
            "completed",
            json!({
                "type": "user_message", "messageId": id(101), "inputIntent": "turn_start",
                "text": "fix it", "displayText": "Fix it", "createdBy": "user",
                "attachments": [{"path": "/a.png", "mention": "a.png", "name": "a.png",
                    "isDir": false, "isImage": true, "blobReference": "waku-blob:1.png"}],
            }),
        ),
        item(
            2,
            RUN_1,
            1,
            "completed",
            json!({"type": "reasoning", "text": "thinking"}),
        ),
        item(
            3,
            RUN_1,
            2,
            "completed",
            json!({"type": "command_execution", "input": "cargo test", "outputOmitted": true,
                "exitCode": 1}),
        ),
        item(
            4,
            RUN_1,
            3,
            "completed",
            json!({"type": "file_change", "fileName": "src/lib.rs", "additions": 2,
                "deletions": 1, "diff": "@@\n-a\n+b\n+c\n"}),
        ),
        item(
            5,
            RUN_1,
            4,
            "completed",
            json!({"type": "file_search", "pattern": "*.rs",
                "results": [{"fileName": "src/lib.rs", "line": 3}]}),
        ),
        item(
            6,
            RUN_1,
            5,
            "completed",
            json!({"type": "web_search", "patterns": ["gpui list"],
                "results": [{"url": "https://zed.dev"}]}),
        ),
        item(
            7,
            RUN_1,
            6,
            "completed",
            json!({"type": "dynamic_tool", "toolName": "mcp__docs__lookup",
                "input": "{\"q\":\"x\"}", "output": "\"found\""}),
        ),
        item(
            8,
            RUN_1,
            7,
            "completed",
            json!({"type": "subagent", "subagentId": id(201), "origin": "provider_native",
                "driver": "claude", "prompt": "explore the repo", "result": "done"}),
        ),
        item(
            9,
            RUN_1,
            8,
            "completed",
            json!({"type": "todo_list", "planId": id(301),
                "steps": [{"id": "1", "text": "read", "status": "completed"},
                          {"id": "2", "text": "write", "status": "pending"}]}),
        ),
        item(
            10,
            RUN_1,
            9,
            "completed",
            json!({"type": "proposed_plan", "planId": id(302), "markdown": "# Plan"}),
        ),
        item(
            11,
            RUN_1,
            10,
            "completed",
            json!({"type": "approval_request", "requestId": id(401),
                "requestKind": "command_approval", "prompt": "Run rm?",
                "options": [{"id": "y", "label": "Yes", "allow": true}]}),
        ),
        item(
            12,
            RUN_1,
            11,
            "completed",
            json!({"type": "user_input_request", "requestId": id(402),
                "questions": [{"id": "q", "header": "Pick", "question": "Which?",
                    "options": [{"label": "A"}], "multiSelect": false}],
                "answers": [{"questionId": "q", "answers": ["A"]}]}),
        ),
        item(
            13,
            RUN_1,
            12,
            "completed",
            json!({"type": "assistant_message", "messageId": id(102), "text": "Done."}),
        ),
        item(
            14,
            RUN_1,
            13,
            "completed",
            json!({"type": "notification", "source": {"type": "command"},
                "outcome": "completed", "summary": "Build finished"}),
        ),
        item(
            15,
            RUN_1,
            14,
            "completed",
            json!({"type": "system_notice", "message": "Restarted",
                "lifecycle": "restart_resumed"}),
        ),
        item(
            16,
            RUN_1,
            15,
            "completed",
            json!({"type": "compaction", "summary": "Shorter"}),
        ),
        item(
            17,
            RUN_1,
            16,
            "completed",
            json!({"type": "handoff", "contextHandoffId": id(501),
                "toProviderInstanceId": "codex", "strategy": "summary"}),
        ),
        item(
            18,
            RUN_1,
            17,
            "completed",
            json!({"type": "fork", "source": {"type": "run", "threadId": THREAD, "runId": RUN_1},
                "targetThreadId": id(601)}),
        ),
        item(
            19,
            RUN_1,
            18,
            "completed",
            json!({"type": "thread_created", "targetThreadId": id(602),
                "targetProviderInstanceId": "codex", "targetModel": "gpt"}),
        ),
        item(
            20,
            RUN_1,
            19,
            "completed",
            json!({"type": "secret_request", "label": "Token", "reason": "needed",
                "secretStatus": "pending"}),
        ),
        item(
            21,
            RUN_1,
            20,
            "failed",
            json!({"type": "error",
                "failure": {"class": "provider_error", "message": "rate limited"}}),
        ),
        item(
            22,
            RUN_1,
            21,
            "completed",
            json!({"type": "checkpoint", "checkpointId": CHECKPOINT, "scopeId": SCOPE}),
        ),
        item(
            23,
            RUN_1,
            22,
            "completed",
            json!({"type": "run_interrupt_result", "message": "x"}),
        ),
        item(24, RUN_1, 23, "completed", json!({"type": "hologram"})),
        item(
            25,
            RUN_1,
            24,
            "completed",
            json!({"type": "system_notice", "message": "",
                "lifecycle": "restart_interrupted"}),
        ),
    ]
}

fn build(state: &ThreadState) -> ThreadView {
    ViewCache::default()
        .build(state, &ViewExtras::default())
        .unwrap()
}

#[test]
fn every_turn_item_kind_maps_onto_a_row() {
    let mut run_1 = run(RUN_1, Some(1), "completed", 101);
    run_1.checkpoint_id = Some(CheckpointId(CHECKPOINT.parse().unwrap()));
    let mut state = state(every_kind(), vec![run_1]);
    let checkpoint: Checkpoint = serde_json::from_value(json!({
        "id": CHECKPOINT, "threadId": THREAD, "scopeId": SCOPE, "runId": RUN_1,
        "nodeId": id(701), "ordinalWithinScope": 0, "appRunOrdinal": 1,
        "gitRef": "refs/waku/t/x", "status": "ready", "capturedAt": 1_700_000_003_000_i64,
        "files": [{"path": "src/lib.rs", "kind": "modified", "additions": 2, "deletions": 1}],
    }))
    .unwrap();
    state.checkpoints.insert(checkpoint.id, checkpoint);
    let view = build(&state);

    let roles = view
        .messages
        .iter()
        .map(|message| (message.role, message.content.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(
        roles,
        [
            (MessageRole::User, "fix it"),
            (MessageRole::Assistant, "Done."),
            (MessageRole::System, "Build finished"),
            (MessageRole::System, "Restarted"),
            (MessageRole::System, "Context compacted: Shorter"),
            (MessageRole::System, "Handed off to codex"),
            (MessageRole::System, "Forked"),
            (MessageRole::System, "Continued with codex gpt"),
            (MessageRole::System, "Token: needed"),
            (MessageRole::Assistant, "rate limited"),
            (MessageRole::System, "Interrupted when the daemon restarted"),
        ]
    );
    let user = &view.messages[0];
    assert_eq!(user.id, Uuid::from_u128(101));
    assert_eq!(user.display_content.as_deref(), Some("Fix it"));
    assert_eq!(
        user.attachments[0].blob_reference.as_deref(),
        Some("waku-blob:1.png")
    );
    let run_id = RUN_1.parse::<Uuid>().unwrap();
    assert!(
        view.messages
            .iter()
            .all(|message| message.turn_id == Some(run_id))
    );

    assert_eq!(view.transcript_blocks.len(), 1);
    let block = &view.transcript_blocks[0];
    assert_eq!(block.after_message, 1);
    assert_eq!(block.turn_id, Some(run_id));
    let kinds = block
        .activities
        .iter()
        .map(|activity| activity.kind)
        .collect::<Vec<_>>();
    assert_eq!(
        kinds,
        [
            ActivityKind::Reasoning,
            ActivityKind::Command,
            ActivityKind::FileChange,
            ActivityKind::FileSearch,
            ActivityKind::Search,
            ActivityKind::Tool,
            ActivityKind::Tool,
            ActivityKind::Plan,
            ActivityKind::Plan,
            ActivityKind::Tool,
            ActivityKind::Tool,
        ]
    );
    let activity = |n: u128| {
        block
            .activities
            .iter()
            .find(|activity| activity.id == Uuid::from_u128(n))
            .unwrap()
    };
    assert_eq!(
        activity(2)
            .reasoning
            .as_ref()
            .map(|reasoning| reasoning.content.as_str()),
        Some("thinking")
    );
    assert!(activity(2).complete);
    assert_eq!(activity(3).arguments.as_deref(), Some("cargo test"));
    assert!(activity(3).failed, "a non-zero exit fails the row");
    assert_eq!(activity(4).file_changes[0].path, "src/lib.rs");
    assert_eq!(activity(4).file_changes[0].additions, Some(2));
    assert_eq!(
        activity(4).file_changes[0].diff.as_deref(),
        Some("@@\n-a\n+b\n+c\n")
    );
    assert_eq!(activity(5).display_target.as_deref(), Some("*.rs"));
    assert_eq!(activity(6).output.as_deref(), Some("https://zed.dev"));
    assert_eq!(activity(7).tool_name.as_deref(), Some("lookup"));
    assert_eq!(activity(7).mcp_server.as_deref(), Some("docs"));
    assert_eq!(activity(8).output.as_deref(), Some("done"));
    assert!(
        activity(9)
            .detail
            .as_deref()
            .unwrap()
            .contains("- [x] read")
    );
    assert_eq!(activity(10).detail.as_deref(), Some("# Plan"));
    assert_eq!(activity(11).display_target.as_deref(), Some("Run rm?"));
    assert!(
        activity(12)
            .detail
            .as_deref()
            .unwrap()
            .contains("Which? — A")
    );

    assert_eq!(view.turns.len(), 1);
    let turn = &view.turns[0];
    assert_eq!(turn.id, run_id);
    assert_eq!(turn.turn_count, 1);
    assert_eq!(turn.status, TurnStatus::Completed);
    let checkpoint = turn.checkpoint.as_ref().unwrap();
    assert_eq!((checkpoint.additions, checkpoint.deletions), (2, 1));
    assert_eq!(view.run_checkpoints.len(), 1);
    assert_eq!(view.status, SessionStatus::Idle);
    assert_eq!(view.provider, ProviderKind::Claude);
    assert_eq!(view.model.as_deref(), Some("opus"));
    assert_eq!(view.runtime_mode, model::RuntimeMode::AutoAcceptEdits);
    assert!(matches!(
        &view.workspace,
        SessionWorkspace::Worktree { branch, .. } if branch == "waku/fixture"
    ));
    assert!(view.unread);
}

#[test]
fn pending_requests_are_prompts_not_rows() {
    let items = vec![
        prompt(1, RUN_1, 101, "go"),
        item(
            2,
            RUN_1,
            1,
            "pending",
            json!({"type": "approval_request", "requestId": REQUEST,
                "requestKind": "command_approval", "prompt": "Run `rm -rf build`?\nrm -rf build",
                "options": [{"id": "allow", "label": "Allow", "allow": true}]}),
        ),
    ];
    let mut state = state(items, vec![run(RUN_1, Some(1), "waiting", 101)]);
    for (request_id, kind, questions) in [
        (REQUEST, "command_approval", json!([])),
        (
            QUESTION,
            "user_input",
            json!([{"id": "q", "header": "H", "question": "Which?", "options": [],
                "multiSelect": false}]),
        ),
    ] {
        let request: RuntimeRequest = serde_json::from_value(json!({
            "id": request_id, "threadId": THREAD, "nodeId": id(801), "kind": kind,
            "status": "pending", "responseCapability": {"type": "message"},
            "options": [{"id": "allow", "label": "Allow", "allow": true}],
            "questions": questions, "createdAt": 1_700_000_002_000_i64,
        }))
        .unwrap();
        state.runtime_requests.insert(request.id, request);
    }
    let view = build(&state);
    assert!(
        view.transcript_blocks.is_empty(),
        "the pending row is the prompt"
    );
    let permission = view.pending_permission.as_ref().unwrap();
    assert_eq!(permission.request_id, REQUEST);
    assert_eq!(permission.title, "Run `rm -rf build`?");
    assert_eq!(permission.detail, "rm -rf build");
    assert_eq!(permission.options[0].id, "allow");
    let input = view.pending_user_input.as_ref().unwrap();
    assert_eq!(input.request_id, QUESTION);
    assert_eq!(input.questions[0].question, "Which?");
    assert_eq!(view.status, SessionStatus::Waiting);
    assert_eq!(view.active_run_id, Some(RunId(RUN_1.parse().unwrap())));
    assert_eq!(view.pending_request_count, 2);
}

#[test]
fn rolled_back_runs_disappear_and_queued_runs_wait_in_the_queue() {
    let items = vec![
        prompt(1, RUN_1, 101, "first"),
        prompt(2, RUN_2, 102, "second"),
    ];
    let mut state = state(
        items,
        vec![
            run(RUN_1, Some(1), "completed", 101),
            run(RUN_2, Some(2), "rolled_back", 102),
            run(RUN_3, None, "queued", 103),
        ],
    );
    let queued: Message = serde_json::from_value(json!({
        "id": id(103), "threadId": THREAD, "runId": RUN_3, "role": "user",
        "createdBy": "user", "creationSource": "desktop", "text": "third",
        "createdAt": 1_700_000_001_000_i64, "updatedAt": 1_700_000_001_000_i64,
    }))
    .unwrap();
    state.messages.insert(queued.id, queued);
    let view = build(&state);
    assert_eq!(
        view.messages
            .iter()
            .map(|message| message.content.as_str())
            .collect::<Vec<_>>(),
        ["first"]
    );
    assert_eq!(view.turns.len(), 1);
    assert_eq!(view.queued_messages.len(), 1);
    assert_eq!(view.queued_messages[0].content, "third");
    assert_eq!(view.queued_messages[0].id, RUN_3.parse::<Uuid>().unwrap());
}

#[test]
fn rewinding_a_run_rolls_back_to_the_previous_runs_checkpoint() {
    let mut view = build(&state(Vec::new(), Vec::new()));
    let point = |ordinal, ready| RunCheckpoint {
        run_id: RunId(Uuid::from_u128(u128::from(ordinal))),
        ordinal,
        scope_id: CheckpointScopeId(Uuid::from_u128(10 + u128::from(ordinal))),
        checkpoint_id: CheckpointId(Uuid::from_u128(20 + u128::from(ordinal))),
        ready,
    };
    view.run_checkpoints = vec![point(1, true), point(2, false), point(3, true)];
    assert_eq!(view.rewind_point(1), None, "nothing precedes the first run");
    assert_eq!(view.rewind_point(2), Some(point(1, true)));
    assert_eq!(
        view.rewind_point(3),
        Some(point(1, true)),
        "a checkpoint that failed is skipped"
    );
    assert_eq!(view.rewind_point(4), Some(point(3, true)));
}

fn checkpoint(
    n: u128,
    run_id: &str,
    ordinal_within_scope: u32,
    app_run_ordinal: u32,
) -> Checkpoint {
    serde_json::from_value(json!({
        "id": id(n),
        "threadId": THREAD,
        "scopeId": SCOPE,
        "runId": run_id,
        "nodeId": id(701),
        "ordinalWithinScope": ordinal_within_scope,
        "appRunOrdinal": app_run_ordinal,
        "gitRef": format!("refs/waku/t/{THREAD}/{SCOPE}/{ordinal_within_scope}"),
        "status": "ready",
        "capturedAt": 1_700_000_003_000_i64,
    }))
    .unwrap()
}

#[test]
fn the_first_message_rewinds_to_its_turn_start_checkpoint() {
    let mut first = run(RUN_1, Some(1), "completed", 101);
    first.checkpoint_id = Some(CheckpointId(id(902).parse().unwrap()));
    let mut state = state(vec![prompt(1, RUN_1, 101, "first")], vec![first]);
    state.checkpoint_scopes.insert(
        CheckpointScopeId(SCOPE.parse().unwrap()),
        serde_json::from_value(json!({
            "id": SCOPE, "threadId": THREAD, "runId": RUN_1, "nodeId": id(701),
            "kind": "root_run", "ordinalWithinParent": 1, "advancesAppRunCount": true,
            "cwd": "/work/tree", "createdAt": 1_700_000_001_000_i64,
        }))
        .unwrap(),
    );
    for checkpoint in [checkpoint(901, RUN_1, 0, 0), checkpoint(902, RUN_1, 1, 1)] {
        state.checkpoints.insert(checkpoint.id, checkpoint);
    }
    let view = build(&state);
    let point = view.rewind_point(1).expect("the first message rewinds");
    assert_eq!(point.checkpoint_id.0, id(901).parse::<Uuid>().unwrap());
    assert_eq!(point.scope_id.0, SCOPE.parse::<Uuid>().unwrap());
    // The run's own checkpoint is what a later run would fall back to.
    assert_eq!(view.run_checkpoints.len(), 1);
    assert_eq!(
        view.run_checkpoints[0].checkpoint_id.0,
        id(902).parse::<Uuid>().unwrap()
    );
}

fn session(status: &str, instance: &str, updated_at: i64, commands: Value) -> ProviderSession {
    serde_json::from_value(json!({
        "id": Uuid::new_v4(),
        "driver": instance,
        "providerInstanceId": instance,
        "status": status,
        "cwd": "/work/tree",
        "createdAt": 1_700_000_000_000_i64,
        "updatedAt": updated_at,
        "availableCommands": commands,
    }))
    .unwrap()
}

#[test]
fn open_sessions_of_the_threads_provider_report_slash_commands() {
    let mut state = state(Vec::new(), Vec::new());
    for session in [
        session(
            "ready",
            "claude",
            2,
            json!([{"name": "review", "description": "Review"}, {"name": "compact"}]),
        ),
        session(
            "ready",
            "claude",
            1,
            json!([{"name": "review"}, {"name": "cost"}]),
        ),
        session("stopped", "claude", 3, json!([{"name": "gone"}])),
        session("ready", "codex", 4, json!([{"name": "other"}])),
    ] {
        state.provider_sessions.insert(session.id, session);
    }
    let view = build(&state);
    let names = view
        .available_commands
        .iter()
        .map(|command| (command.name.as_str(), command.description.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        [("review", "Review"), ("compact", ""), ("cost", "")],
        "newest session first, one per name, closed and other sessions left out"
    );
}

#[test]
fn background_processes_stop_through_their_provider_thread_and_show_their_output() {
    let provider_thread = id(801);
    let command_item = id(802);
    let mut thread = thread();
    thread.active_provider_thread_id = Some(ProviderThreadId(provider_thread.parse().unwrap()));
    let mut state = state(Vec::new(), Vec::new());
    state.thread = Some(thread);
    let row: ProviderThread = serde_json::from_value(json!({
        "id": provider_thread,
        "driver": "codex",
        "providerInstanceId": "claude",
        "status": "idle",
        "createdAt": 1,
        "updatedAt": 1,
        "pendingBackgroundTasks": [
            {"kind": "command", "taskId": "proc-7", "description": "npm run dev",
             "itemId": command_item},
            {"kind": "monitor", "taskId": "watch-1"},
        ],
    }))
    .unwrap();
    state.provider_threads.insert(row.id, row);
    let mut outputs = HashMap::new();
    outputs.insert(
        TurnItemId(command_item.parse().unwrap()),
        CommandOutputState {
            text: "listening on 3000\n".into(),
            truncated: false,
            running: true,
            exit_code: None,
        },
    );
    let view = ViewCache::default()
        .build(
            &state,
            &ViewExtras {
                outputs: Some(&outputs),
                ..ViewExtras::default()
            },
        )
        .unwrap();
    assert_eq!(view.background_work.len(), 2);
    let process = &view.background_work[0];
    assert_eq!(process.key.kind, model::BackgroundWorkKind::Process);
    assert!(process.can_stop);
    assert_eq!(process.output.as_deref(), Some("listening on 3000\n"));
    assert_eq!(
        process.origin_activity_id.as_deref(),
        Some(command_item.as_str())
    );
    let control = process.control_id.as_deref().unwrap();
    assert_eq!(
        background_control(process.key.kind, control),
        Some(BackgroundControl::Process {
            provider_thread_id: ProviderThreadId(provider_thread.parse().unwrap()),
            task_id: "proc-7".into(),
        })
    );
    let monitor = &view.background_work[1];
    assert_eq!(monitor.key.kind, model::BackgroundWorkKind::Monitor);
    assert_eq!(monitor.status, model::BackgroundWorkStatus::Monitoring);
    assert_eq!(
        background_control(model::BackgroundWorkKind::Subagent, control),
        None,
        "a process control is not a subagent's"
    );
}

#[test]
fn streaming_text_rebuilds_only_its_own_row() {
    let answer = |text: &str, rev: u32| {
        let mut item = item(
            2,
            RUN_1,
            1,
            "running",
            json!({"type": "assistant_message", "messageId": id(102), "text": text,
                "streaming": true}),
        );
        item.stream_rev = rev;
        item
    };
    let mut state = state(
        vec![prompt(1, RUN_1, 101, "go"), answer("He", 1)],
        vec![run(RUN_1, Some(1), "running", 101)],
    );
    let mut cache = ViewCache::default();
    let first = cache.build(&state, &ViewExtras::default()).unwrap();
    assert!(first.messages[1].streaming);
    assert_eq!(first.status, SessionStatus::Working);
    assert_eq!(first.active_turn_id(), Some(RUN_1.parse().unwrap()));
    assert!(state.apply_delta(
        TurnItemId(Uuid::from_u128(2)),
        &waku_protocol::v8::orchestration::DeltaChannel::Text,
        2,
        "llo",
    ));
    let second = cache.build(&state, &ViewExtras::default()).unwrap();
    assert!(Arc::ptr_eq(&first.messages[0], &second.messages[0]));
    assert!(!Arc::ptr_eq(&first.messages[1], &second.messages[1]));
    assert_eq!(second.messages[1].content, "Hello");
}

#[test]
fn watched_output_and_fetched_bodies_fill_omitted_rows() {
    let command = item(
        3,
        RUN_1,
        0,
        "running",
        json!({"type": "command_execution", "input": "ls", "outputOmitted": true}),
    );
    let change = item(
        4,
        RUN_1,
        1,
        "completed",
        json!({"type": "file_change", "fileName": "a.rs", "bodyOmitted": true}),
    );
    let state = state(
        vec![command, change.clone()],
        vec![run(RUN_1, Some(1), "running", 101)],
    );
    let outputs = HashMap::from([(
        TurnItemId(Uuid::from_u128(3)),
        CommandOutputState {
            text: "a.rs\n".into(),
            ..CommandOutputState::default()
        },
    )]);
    let mut full = change;
    full.kind = serde_json::from_value(json!({"type": "file_change", "fileName": "a.rs",
        "diff": "@@\n+x\n"}))
    .unwrap();
    let full_items = HashMap::from([(full.id, full)]);
    let view = ViewCache::default()
        .build(
            &state,
            &ViewExtras {
                outputs: Some(&outputs),
                full_items: Some(&full_items),
                history_loading: false,
            },
        )
        .unwrap();
    let block = &view.transcript_blocks[0];
    assert_eq!(block.activities[0].output.as_deref(), Some("a.rs"));
    assert_eq!(
        block.activities[1].file_changes[0].diff.as_deref(),
        Some("@@\n+x\n")
    );
}

#[test]
fn shell_rows_are_list_views_reused_while_unchanged() {
    let row = ThreadRow {
        thread: thread(),
        activity: ThreadActivity::Working,
        pending_request_count: 0,
        queued_run_count: 2,
    };
    let mut shell = ShellState::new(waku_protocol::v8::orchestration::ShellLocation::Active);
    shell.threads.insert(row.thread.id, row);
    let mut cache = ShellCache::default();
    let first = cache.build(&shell);
    let second = cache.build(&shell);
    assert!(Arc::ptr_eq(&first.threads[0], &second.threads[0]));
    let view = &first.threads[0];
    assert!(!view.detail_loaded);
    assert_eq!(view.status, SessionStatus::Working);
    assert_eq!(view.queued_run_count, 2);
    assert_eq!(view.display_title(), "Fixture");
    assert_eq!(view.last_reply_at, Some(1_700_000_004));
}
