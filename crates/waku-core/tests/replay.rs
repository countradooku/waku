//! Replay-backed integration tests (RFC 0002 "Replay fixtures", #44, #45).
//!
//! Each fixture under `fixtures/replay/<scenario>/<provider>.ndjson` replays
//! through the real orchestrator, sink, projections, effect workers,
//! checkpoints and `ProviderSessionManager`; only the provider transport is
//! replaced. The fake driver's fixtures are the orchestrator suite run
//! through the `DriverV2` trait: start, streaming, approvals, structured
//! questions, native and interrupt-restart steering, interrupts, rollback,
//! native and portable forks, usage limits, child turns, session resume,
//! idle release, in-session model switches and compaction.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::Value;
use waku_core::driver::claude::{
    CLAUDE_PROVIDER, ClaudeDriver, claude_capabilities, claude_instance,
};
use waku_core::driver::codex::{CODEX_PROVIDER, CodexDriver, codex_capabilities, codex_instance};
use waku_core::driver::deepseek::{DEEPSEEK_PROVIDER, DeepSeekDriver, deepseek_replay_instance};
use waku_core::driver::opencode::{OPENCODE_PROVIDER, OpenCodeDriver, opencode_replay_instance};
use waku_core::driver::v2::fake::{
    FAKE_PROVIDER, FakeDriver, FakeProfile, fake_capabilities, fake_instance,
};
use waku_core::driver::v2::fake_agent::{FakeAgentFactory, USAGE_LIMIT_RESET_MS};
use waku_core::driver::v2::manager::ForkResult;
use waku_core::driver::v2::replay::record::{RecordRequest, instance_for, record_scenario};
use waku_core::driver::v2::replay::{
    ReplayEntry, ReplayError, ReplayHarness, ReplayProfile, ReplayTranscript, ScenarioInput,
    ScenarioResult, read_transcript,
};
use waku_core::driver::v2::{AdapterEvent, DriverV2, InstanceConfig, TurnOutcome};
use waku_orchestrator::ThreadProjection;
use waku_protocol::orchestration::*;

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/replay")
}

fn input(scenario: &str) -> ScenarioInput {
    ScenarioInput::load(&fixtures().join(scenario), FAKE_PROVIDER).unwrap()
}

fn driver_for(provider: &str) -> Arc<dyn DriverV2> {
    match provider {
        FAKE_PROVIDER => Arc::new(FakeDriver),
        // The ACP family (#48); its fixtures' assertions are in `replay_acp.rs`.
        "acp" | "cursor" | "grok" | "kimi" | "fx" => {
            Arc::new(waku_core::driver::acp::AcpDriver::new())
        }
        CODEX_PROVIDER => Arc::new(CodexDriver::new()),
        CLAUDE_PROVIDER => Arc::new(ClaudeDriver),
        OPENCODE_PROVIDER => Arc::new(OpenCodeDriver),
        DEEPSEEK_PROVIDER => Arc::new(DeepSeekDriver),
        other => panic!("no DriverV2 for {other} yet"),
    }
}

/// The ACP family (#48) replays through `replay_acp.rs`, which builds each
/// flavor's instance and holds its assertions.
fn is_acp(provider: &str) -> bool {
    matches!(provider, "acp" | "cursor" | "grok" | "kimi" | "fx")
}

fn instance_of(provider: &str) -> InstanceConfig {
    match provider {
        FAKE_PROVIDER => fake_instance(FakeProfile::default()),
        CODEX_PROVIDER => codex_instance(),
        CLAUDE_PROVIDER => claude_instance(),
        // HTTP drivers get a fixed service URL instead of discovering (or
        // starting) a real service.
        OPENCODE_PROVIDER => opencode_replay_instance(),
        DEEPSEEK_PROVIDER => deepseek_replay_instance(),
        other => panic!("no DriverV2 instance for {other} here"),
    }
}

fn run_transcript(
    scenario: &str,
    transcript: ReplayTranscript,
) -> Result<ScenarioResult, ReplayError> {
    let provider = transcript.header.provider.clone();
    let input = ScenarioInput::load(&fixtures().join(scenario), &provider)?;
    let instance = instance_for(instance_of(&provider), &input);
    let protocol = transcript.header.protocol.clone();
    let mut harness = ReplayHarness::replay(
        driver_for(&transcript.header.provider),
        instance,
        transcript,
        Some(fixtures().join(scenario)),
        ReplayProfile::new(protocol),
    )?;
    harness.run(&input)
}

fn replay(scenario: &str) -> ScenarioResult {
    replay_as(scenario, FAKE_PROVIDER)
}

fn replay_as(scenario: &str, provider: &str) -> ScenarioResult {
    let transcript =
        read_transcript(&fixtures().join(scenario).join(format!("{provider}.ndjson"))).unwrap();
    run_transcript(scenario, transcript)
        .unwrap_or_else(|error| panic!("{scenario}/{provider}: {error}"))
}

/// Every `(scenario, provider)` fixture, sorted.
fn all_fixtures() -> Vec<(String, String)> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(fixtures()).unwrap() {
        let entry = entry.unwrap();
        if !entry.path().is_dir() {
            continue;
        }
        let scenario = entry.file_name().to_string_lossy().into_owned();
        for file in std::fs::read_dir(entry.path()).unwrap() {
            let name = file.unwrap().file_name().to_string_lossy().into_owned();
            if let Some(provider) = name.strip_suffix(".ndjson") {
                found.push((scenario.clone(), provider.to_owned()));
            }
        }
    }
    found.sort();
    found
}

// -- helpers ---------------------------------------------------------------------

fn runs(projection: &ThreadProjection) -> Vec<&Run> {
    let mut runs = projection
        .runs
        .values()
        .filter(|run| run.ordinal.is_some())
        .collect::<Vec<_>>();
    runs.sort_by_key(|run| run.ordinal);
    runs
}

fn statuses(projection: &ThreadProjection) -> Vec<RunStatus> {
    runs(projection).iter().map(|run| run.status).collect()
}

/// Final assistant text per message of `thread`, in order of first appearance.
fn assistant_texts(result: &ScenarioResult, thread: ThreadId) -> Vec<String> {
    let mut order = Vec::new();
    let mut texts = std::collections::HashMap::new();
    for event in &result.events {
        if let DomainEvent::MessageUpdated(message) = event
            && message.thread_id == thread
            && message.role == MessageRole::Assistant
        {
            if !texts.contains_key(&message.id) {
                order.push(message.id);
            }
            texts.insert(message.id, message.text.clone());
        }
    }
    order.into_iter().map(|id| texts[&id].clone()).collect()
}

/// The final state of every turn item of `thread`.
fn items(result: &ScenarioResult, thread: ThreadId) -> Vec<TurnItem> {
    let mut latest = std::collections::BTreeMap::new();
    for event in &result.events {
        if let DomainEvent::TurnItemUpdated(item) = event
            && item.thread_id == thread
        {
            latest.insert(item.position, item.clone());
        }
    }
    latest.into_values().collect()
}

fn main_id(result: &ScenarioResult) -> ThreadId {
    result.threads["main"].thread.id
}

fn terminals(result: &ScenarioResult) -> Vec<TurnOutcome> {
    result
        .adapter_events
        .iter()
        .filter_map(|event| match event {
            AdapterEvent::TurnTerminal { terminal } => Some(terminal.outcome.clone()),
            _ => None,
        })
        .collect()
}

fn provider_turns(projection: &ThreadProjection) -> Vec<&ProviderTurn> {
    let mut turns = projection
        .provider_turns
        .values()
        .filter(|turn| turn.run_attempt_id.is_some())
        .collect::<Vec<_>>();
    turns.sort_by_key(|turn| (turn.provider_thread_id, turn.ordinal));
    turns
}

// -- the orchestrator suite through the trait ------------------------------------

#[test]
fn simple() {
    let result = replay("simple");
    let main = &result.projection;
    assert_eq!(statuses(main), [RunStatus::Completed]);
    let run = runs(main)[0];
    let checkpoint = &main.checkpoints[&run.checkpoint_id.expect("captured")];
    assert_eq!(checkpoint.status, CheckpointStatus::Ready);
    assert_eq!(checkpoint.app_run_ordinal, Some(1));
    assert_eq!(
        assistant_texts(&result, main_id(&result)),
        ["Echo: Respond with the following text: fixture simple ok"]
    );
    let turns = provider_turns(main);
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].status, ProviderTurnStatus::Completed);
    assert_eq!(
        turns[0]
            .native_turn_ref
            .as_ref()
            .and_then(|reference| reference.native_id.as_deref()),
        Some("u-2")
    );
    let provider_thread = &main.provider_threads[&run.provider_thread_id.unwrap()];
    assert_eq!(
        provider_thread
            .native_thread_ref
            .as_ref()
            .map(|reference| (reference.native_id.as_deref(), reference.strength)),
        Some((Some("t-1"), RefStrength::Strong))
    );
    // The session carries the negotiated capabilities and was closed at the end.
    let session = main.provider_sessions.values().next().unwrap();
    assert_eq!(
        session.capabilities,
        fake_capabilities(FakeProfile::default())
    );
    assert_eq!(session.status, SessionStatus::Stopped);
    assert_eq!(result.capabilities, session.capabilities);
    assert_eq!(terminals(&result), [TurnOutcome::Completed]);
    // Streaming text was persisted as a final, non-streaming row.
    let rows = items(&result, main_id(&result));
    assert!(rows.iter().any(|item| matches!(
        &item.kind,
        TurnItemKind::AssistantMessage { streaming: false, text, .. } if text.starts_with("Echo:")
    ) && item.status == ItemStatus::Completed));
}

#[test]
fn multi_turn() {
    let result = replay("multi_turn");
    let main = &result.projection;
    assert_eq!(statuses(main), [RunStatus::Completed, RunStatus::Completed]);
    assert_eq!(
        assistant_texts(&result, main_id(&result)),
        ["Echo: first", "Echo: second"]
    );
    let turns = provider_turns(main);
    assert_eq!(
        turns.iter().map(|turn| turn.ordinal).collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(turns[0].provider_thread_id, turns[1].provider_thread_id);
    assert_eq!(
        main.provider_sessions.len(),
        1,
        "one session serves both turns"
    );
}

#[test]
fn queued_turn() {
    let result = replay("queued_turn");
    let main = &result.projection;
    assert_eq!(statuses(main), [RunStatus::Completed, RunStatus::Completed]);
    let second = runs(main)[1];
    assert_eq!(second.ordinal, Some(2));
    assert!(second.requested_at < runs(main)[0].completed_at.unwrap());
    assert!(main.queue.is_empty());
    assert_eq!(
        assistant_texts(&result, main_id(&result)),
        ["Listed the files.", "Echo: queued after"]
    );
}

#[test]
fn tool_call_approval() {
    let result = replay("tool_call_approval");
    let main = &result.projection;
    assert_eq!(statuses(main), [RunStatus::Completed]);
    let request = main.runtime_requests.values().next().unwrap();
    assert_eq!(request.kind, RuntimeRequestKind::CommandApproval);
    assert_eq!(request.status, RequestStatus::Resolved);
    assert_eq!(
        request.response,
        Some(RuntimeResponse::Approval {
            option_id: "allow".into()
        })
    );
    // Native option ids survive normalization.
    assert_eq!(
        request
            .options
            .iter()
            .map(|option| option.id.as_str())
            .collect::<Vec<_>>(),
        ["allow", "deny"]
    );
    let rows = items(&result, main_id(&result));
    assert!(rows.iter().any(|item| matches!(
        &item.kind,
        TurnItemKind::ApprovalRequest { prompt: Some(prompt), .. } if prompt == "ls"
    ) && item.status == ItemStatus::Completed));
    assert!(rows.iter().any(|item| matches!(
        &item.kind,
        TurnItemKind::CommandExecution { output: Some(output), exit_code: Some(0), .. }
            if output == "README.md\n"
    )));
    assert!(
        main.nodes
            .values()
            .any(|node| node.kind == NodeKind::ToolCall && node.status == NodeStatus::Completed)
    );
    // Tool output went to the output hub, never to the log.
    assert!(
        result
            .adapter_events
            .iter()
            .any(|event| matches!(event, AdapterEvent::CommandOutputDelta { .. }))
    );
    assert_eq!(
        assistant_texts(&result, main_id(&result)),
        ["Listed the files."]
    );
}

#[test]
fn plan_questions() {
    let result = replay("plan_questions");
    let main = &result.projection;
    assert_eq!(statuses(main), [RunStatus::Completed]);
    let plan = main.plans.values().next().unwrap();
    assert!(matches!(&plan.kind, PlanKind::TodoList { steps, .. } if steps.len() == 2));
    let request = main.runtime_requests.values().next().unwrap();
    assert_eq!(request.kind, RuntimeRequestKind::UserInput);
    assert_eq!(request.status, RequestStatus::Resolved);
    assert_eq!(request.questions[0].id, "q1");
    assert_eq!(
        assistant_texts(&result, main_id(&result)),
        ["You chose Blue."]
    );
    let rows = items(&result, main_id(&result));
    assert!(
        rows.iter()
            .any(|item| matches!(item.kind, TurnItemKind::TodoList { .. }))
    );
    assert!(rows.iter().any(|item| matches!(
        &item.kind,
        TurnItemKind::UserInputRequest { answers: Some(answers), .. } if answers[0].answers == ["Blue"]
    )));
}

#[test]
fn turn_interrupt() {
    let result = replay("turn_interrupt");
    let main = &result.projection;
    assert_eq!(statuses(main), [RunStatus::Interrupted]);
    let rows = items(&result, main_id(&result));
    assert!(
        rows.iter()
            .any(|item| matches!(item.kind, TurnItemKind::RunInterruptRequest { .. }))
    );
    assert!(
        rows.iter()
            .any(|item| matches!(item.kind, TurnItemKind::RunInterruptResult { .. }))
    );
    // The partial text stopped streaming.
    assert!(rows.iter().any(|item| matches!(
        &item.kind,
        TurnItemKind::AssistantMessage { streaming: false, text, .. } if text == "Working"
    ) && item.status == ItemStatus::Interrupted));
    assert_eq!(
        provider_turns(main)[0].status,
        ProviderTurnStatus::Interrupted
    );
    assert_eq!(terminals(&result), [TurnOutcome::Interrupted]);
    assert!(runs(main)[0].checkpoint_id.is_none());
}

#[test]
fn message_steering_is_native_when_declared() {
    let result = replay("message_steering");
    let main = &result.projection;
    assert_eq!(statuses(main), [RunStatus::Completed]);
    let run = runs(main)[0];
    let attempts = main
        .attempts
        .values()
        .filter(|attempt| attempt.run_id == run.id)
        .count();
    assert_eq!(attempts, 1, "native steering keeps one attempt");
    let rows = items(&result, main_id(&result));
    assert!(rows.iter().any(|item| matches!(
        &item.kind,
        TurnItemKind::UserMessage { input_intent: InputIntent::Steer, text, .. } if text == "use tabs"
    )));
    // The partial reply before the steer, then the steered one.
    assert_eq!(
        assistant_texts(&result, main_id(&result)),
        ["Working", "Steered: use tabs"]
    );
}

#[test]
fn steering_restarts_the_turn_without_native_steering() {
    let result = replay("steering_restart");
    let main = &result.projection;
    assert_eq!(statuses(main), [RunStatus::Completed]);
    let run = runs(main)[0];
    let mut attempts = main
        .attempts
        .values()
        .filter(|attempt| attempt.run_id == run.id)
        .collect::<Vec<_>>();
    attempts.sort_by_key(|attempt| attempt.attempt_ordinal);
    assert_eq!(
        attempts
            .iter()
            .map(|attempt| (attempt.reason, attempt.status))
            .collect::<Vec<_>>(),
        [
            (AttemptReason::Initial, AttemptStatus::Superseded),
            (AttemptReason::SteeringRestart, AttemptStatus::Completed),
        ]
    );
    let turns = provider_turns(main);
    assert_eq!(
        turns.iter().map(|turn| turn.status).collect::<Vec<_>>(),
        [
            ProviderTurnStatus::Interrupted,
            ProviderTurnStatus::Completed
        ]
    );
    // The superseded attempt's terminal did not end the run.
    assert_eq!(
        terminals(&result),
        [TurnOutcome::Interrupted, TurnOutcome::Completed]
    );
    assert!(assistant_texts(&result, main_id(&result)).contains(&"Echo: use tabs".to_owned()));
}

#[test]
fn usage_limit_fails_with_the_reset_time() {
    let result = replay("usage_limit");
    let main = &result.projection;
    assert_eq!(statuses(main), [RunStatus::Failed]);
    let failure = runs(main)[0].failure.clone().unwrap();
    assert_eq!(failure.class, FailureClass::UsageLimit);
    assert_eq!(failure.reset_at, Some(USAGE_LIMIT_RESET_MS));
    let recovery = main.thread.limit_recovery.clone().unwrap();
    assert_eq!(recovery.reset_at, Some(USAGE_LIMIT_RESET_MS));
    let rows = items(&result, main_id(&result));
    assert!(rows.iter().any(|item| matches!(
        &item.kind,
        TurnItemKind::Error { failure, .. } if failure.class == FailureClass::UsageLimit
    )));
}

#[test]
fn child_turns_do_not_close_the_parent() {
    let result = replay("subagent");
    let main = &result.projection;
    assert_eq!(statuses(main), [RunStatus::Completed]);
    let subagent = main.subagents.values().next().unwrap();
    assert_eq!(subagent.status, SubagentStatus::Completed);
    assert_eq!(subagent.result.as_deref(), Some("Found it."));
    let child_thread = main
        .provider_threads
        .values()
        .find(|thread| thread.owner_node_id == Some(subagent.id))
        .expect("the subagent's provider thread");
    let child_turn = main
        .provider_turns
        .values()
        .find(|turn| turn.provider_thread_id == child_thread.id)
        .unwrap();
    assert_eq!(child_turn.status, ProviderTurnStatus::Completed);
    // Two terminals: the child's, then the parent's. The parent kept
    // streaming after the child finished.
    assert_eq!(
        terminals(&result),
        [TurnOutcome::Completed, TurnOutcome::Completed]
    );
    assert_eq!(
        assistant_texts(&result, main_id(&result)),
        ["Echo: [subagent] delegate the search"]
    );
    assert_eq!(
        main.thread.active_provider_thread_id,
        runs(main)[0].provider_thread_id
    );
}

#[test]
fn rollback_is_native_in_place() {
    let result = replay("thread_rollback");
    let main = &result.projection;
    assert_eq!(
        statuses(main),
        [
            RunStatus::Completed,
            RunStatus::RolledBack,
            RunStatus::Completed
        ]
    );
    assert!(main.thread.rollback.is_none());
    let provider_threads = runs(main)
        .iter()
        .map(|run| run.provider_thread_id)
        .collect::<BTreeSet<_>>();
    assert_eq!(provider_threads.len(), 1, "rolled back in place");
    assert!(
        result
            .checkpoint_calls
            .iter()
            .any(|call| call.starts_with("restore refs/waku/t/")),
        "{:?}",
        result.checkpoint_calls
    );
}

#[test]
fn rollback_without_support_starts_a_fresh_thread_with_a_handoff() {
    let result = replay("thread_rollback_portable");
    let main = &result.projection;
    assert_eq!(
        statuses(main),
        [
            RunStatus::Completed,
            RunStatus::RolledBack,
            RunStatus::Completed
        ]
    );
    let all = runs(main);
    assert_ne!(all[0].provider_thread_id, all[2].provider_thread_id);
    let retired = &main.provider_threads[&all[0].provider_thread_id.unwrap()];
    assert_eq!(retired.status, ProviderThreadStatus::Closed);
    assert_eq!(
        assistant_texts(&result, main_id(&result))
            .last()
            .map(String::as_str),
        Some("I have the earlier context.")
    );
}

#[test]
fn fork_is_native_when_declared() {
    let result = replay("thread_fork_native");
    assert!(matches!(result.forks[..], [ForkResult::Native { .. }]));
    let main = &result.projection;
    let fork = &result.threads["fork-a"];
    let source_thread = main.thread.active_provider_thread_id.unwrap();
    let forked = &fork.provider_threads[&fork.thread.active_provider_thread_id.unwrap()];
    assert_eq!(
        forked
            .forked_from
            .as_ref()
            .map(|from| from.provider_thread_id),
        Some(source_thread)
    );
    assert!(
        forked
            .forked_from
            .as_ref()
            .unwrap()
            .provider_turn_id
            .is_some()
    );
    assert_eq!(statuses(fork), [RunStatus::Completed]);
    assert_eq!(
        assistant_texts(&result, fork.thread.id),
        ["Echo: in the fork"]
    );
}

#[test]
fn fork_falls_back_to_a_portable_handoff() {
    let result = replay("thread_fork_portable");
    assert_eq!(result.forks, [ForkResult::Portable]);
    let fork = &result.threads["fork-a"];
    assert_eq!(statuses(fork), [RunStatus::Completed]);
    let provider_thread = &fork.provider_threads[&fork.thread.active_provider_thread_id.unwrap()];
    assert!(provider_thread.forked_from.is_none());
    assert_eq!(
        assistant_texts(&result, fork.thread.id),
        ["I have the earlier context."]
    );
}

#[test]
fn unloaded_threads_resume_natively() {
    let result = replay("provider_thread_resume");
    let main = &result.projection;
    assert_eq!(statuses(main), [RunStatus::Completed, RunStatus::Completed]);
    assert_eq!(
        main.provider_sessions.len(),
        2,
        "a second session resumed the thread"
    );
    assert!(main.provider_sessions.values().any(|session| {
        session.status == SessionStatus::Stopped
            && session.detach_reason.as_deref() == Some("unloaded")
    }));
    let all = runs(main);
    assert_eq!(all[0].provider_thread_id, all[1].provider_thread_id);
}

#[test]
fn idle_sessions_are_released() {
    let result = replay("idle_release");
    let main = &result.projection;
    assert_eq!(result.released, [main.thread.id]);
    assert!(
        main.provider_sessions
            .values()
            .any(|session| { session.detach_reason.as_deref() == Some("idle") })
    );
    assert_eq!(statuses(main), [RunStatus::Completed, RunStatus::Completed]);
}

#[test]
fn background_work_pins_an_idle_session_for_up_to_four_hours() {
    let result = replay("idle_release_pinned");
    let main = &result.projection;
    // Not released after 31 minutes, released after 4 hours and 1 minute.
    assert_eq!(result.released, [main.thread.id]);
    let released = main
        .provider_sessions
        .values()
        .find(|session| session.detach_reason.as_deref() == Some("idle"))
        .expect("an idle release");
    let pinned_for = released.detached_at.unwrap() - released.created_at;
    assert!(
        pinned_for >= 4 * 60 * 60 * 1000,
        "released after {pinned_for} ms"
    );
    assert_eq!(statuses(main), [RunStatus::Completed, RunStatus::Completed]);
}

#[test]
fn model_changes_switch_in_session() {
    let result = replay("model_switch");
    let main = &result.projection;
    assert_eq!(statuses(main), [RunStatus::Completed, RunStatus::Completed]);
    assert_eq!(
        runs(main)[1].model_selection.model.as_deref(),
        Some("fake-large")
    );
    assert_eq!(
        main.provider_sessions.len(),
        1,
        "no restart for a model switch"
    );
}

#[test]
fn compaction_reaches_the_live_session() {
    let result = replay("compact");
    assert_eq!(statuses(&result.projection), [RunStatus::Completed]);
    let transcript = read_transcript(&fixtures().join("compact/fake.ndjson")).unwrap();
    assert!(transcript.entries.iter().any(|entry| matches!(
        entry,
        ReplayEntry::ExpectOutbound { frame, .. } if frame["method"] == "thread/compact"
    )));
}

// -- the harness itself ----------------------------------------------------------

#[test]
fn every_fixture_has_assertions() {
    let fake = [
        "simple",
        "multi_turn",
        "queued_turn",
        "tool_call_approval",
        "plan_questions",
        "turn_interrupt",
        "message_steering",
        "steering_restart",
        "usage_limit",
        "subagent",
        "thread_rollback",
        "thread_rollback_portable",
        "thread_fork_native",
        "thread_fork_portable",
        "provider_thread_resume",
        "idle_release",
        "idle_release_pinned",
        "model_switch",
        "compact",
    ];
    let covered = |provider: &str| -> &[&str] {
        match provider {
            FAKE_PROVIDER => &fake,
            CODEX_PROVIDER => &codex::COVERED,
            CLAUDE_PROVIDER => &claude::COVERED,
            OPENCODE_PROVIDER => opencode::COVERED,
            DEEPSEEK_PROVIDER => deepseek::COVERED,
            other => panic!("{other} fixtures have no assertions"),
        }
    };
    let recorded = all_fixtures();
    for (scenario, provider) in &recorded {
        if is_acp(provider) {
            // The ACP family's assertions and determinism check are in `replay_acp.rs`.
            continue;
        }
        assert!(
            covered(provider).contains(&scenario.as_str()),
            "{scenario}/{provider} has no assertions"
        );
    }
    for entry in std::fs::read_dir(fixtures()).unwrap() {
        let entry = entry.unwrap();
        if !entry.path().is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        assert!(
            recorded.iter().any(|(scenario, _)| scenario == &name),
            "{name} has no recorded fixture"
        );
    }
    for provider in [
        FAKE_PROVIDER,
        CODEX_PROVIDER,
        CLAUDE_PROVIDER,
        OPENCODE_PROVIDER,
        DEEPSEEK_PROVIDER,
    ] {
        for scenario in covered(provider) {
            assert!(
                recorded.contains(&(scenario.to_string(), provider.to_owned())),
                "{scenario}/{provider} is asserted but not recorded"
            );
        }
    }
}

#[test]
fn replay_is_deterministic() {
    for (scenario, provider) in all_fixtures()
        .into_iter()
        .filter(|(_, provider)| !is_acp(provider))
    {
        let first = replay_as(&scenario, &provider);
        let second = replay_as(&scenario, &provider);
        // Only the temporary workspace path differs between two runs.
        let normalize = |result: &ScenarioResult| -> Vec<String> {
            let workspace = result.workspace.display().to_string();
            result
                .events
                .iter()
                .map(|event| {
                    serde_json::to_string(event)
                        .unwrap()
                        .replace(&workspace, "<workspace>")
                })
                .collect()
        };
        let (first, second) = (normalize(&first), normalize(&second));
        let differs = first
            .iter()
            .zip(&second)
            .position(|(left, right)| left != right);
        assert!(
            differs.is_none() && first.len() == second.len(),
            "{scenario}/{provider}: event logs differ at {differs:?} ({} vs {} events):\n{:?}\n{:?}",
            first.len(),
            second.len(),
            differs.map(|index| &first[index]),
            differs.map(|index| &second[index]),
        );
    }
}

fn mutate(transcript: &mut ReplayTranscript, method: &str, edit: impl Fn(&mut Value)) {
    for entry in &mut transcript.entries {
        if let ReplayEntry::ExpectOutbound { frame, .. } = entry
            && frame["method"] == method
        {
            edit(frame);
            return;
        }
    }
    panic!("no {method} frame");
}

#[test]
fn fixture_drift_fails_with_a_readable_frame_diff() {
    let path = fixtures().join("simple/fake.ndjson");
    let mut transcript = read_transcript(&path).unwrap();
    mutate(&mut transcript, "turn/start", |frame| {
        frame["params"]["text"] = Value::from("Respond with the following text: fixture simple OK");
    });
    let error = run_transcript("simple", transcript)
        .err()
        .expect("drift must fail");
    let ReplayError::Mismatch {
        scenario,
        line,
        label,
        diff,
        ..
    } = &error
    else {
        panic!("expected a mismatch, got {error}");
    };
    assert_eq!(scenario, "simple");
    assert_eq!(*line, 7);
    assert_eq!(label.as_deref(), Some("turn/start"));
    assert!(diff.contains("--- expected\n+++ actual"), "{diff}");
    assert!(
        diff.contains("-    \"text\": \"Respond with the following text: fixture simple OK\""),
        "{diff}"
    );
    assert!(
        diff.contains("+    \"text\": \"Respond with the following text: fixture simple ok\""),
        "{diff}"
    );
    let shown = error.to_string();
    assert!(shown.starts_with("replay simple: outbound frame does not match line 7 (turn/start)"));
}

#[test]
fn leftover_and_unexpected_frames_fail() {
    let path = fixtures().join("simple/fake.ndjson");
    // An entry the driver never reaches.
    let mut transcript = read_transcript(&path).unwrap();
    transcript.entries.push(ReplayEntry::ExpectOutbound {
        label: Some("never".into()),
        channel: None,
        frame: serde_json::json!({ "method": "never" }),
    });
    transcript.lines.push(99);
    let error = run_transcript("simple", transcript).err().unwrap();
    assert!(matches!(error, ReplayError::NotExhausted { .. }), "{error}");
    // A transcript that ends early.
    let mut transcript = read_transcript(&path).unwrap();
    let cut = transcript
        .entries
        .iter()
        .position(|entry| matches!(entry, ReplayEntry::ExpectOutbound { frame, .. } if frame["method"] == "turn/start"))
        .unwrap();
    transcript.entries.truncate(cut);
    transcript.lines.truncate(cut);
    let error = run_transcript("simple", transcript).err().unwrap();
    assert!(
        matches!(error, ReplayError::UnexpectedOutbound { .. }),
        "{error}"
    );
}

#[test]
fn the_recorder_reproduces_the_committed_fixture() {
    let dir = tempfile::tempdir().unwrap();
    let committed = read_transcript(&fixtures().join("simple/fake.ndjson")).unwrap();
    let out = dir.path().join("simple/fake.ndjson");
    let recorded = record_scenario(RecordRequest {
        driver: Arc::new(FakeDriver),
        instance: fake_instance(FakeProfile::default()),
        live: Arc::new(FakeAgentFactory::new(dir.path().join("agent-home"))),
        header: committed.header.clone(),
        input: input("simple"),
        out: out.clone(),
        renumber_ids: false,
    })
    .unwrap();
    assert!(Path::new(&out).exists());
    assert_eq!(recorded.entries, committed.entries);
}

/// The native id of the provider thread a run used.
fn native_thread(projection: &ThreadProjection, run: &Run) -> Option<String> {
    projection.provider_threads[&run.provider_thread_id.unwrap()]
        .native_thread_ref
        .as_ref()
        .and_then(|reference| reference.native_id.clone())
}

// -- Codex (#46) ---------------------------------------------------------------------

/// The Codex app-server fixtures (`<scenario>/codex.ndjson`). Their
/// sources are listed in `fixtures/replay/CODEX.md`.
mod codex {
    use super::*;

    pub const COVERED: [&str; 16] = [
        "simple",
        "multi_turn",
        "tool_call_read_only",
        "tool_call_approval",
        "plan_questions",
        "todo_list",
        "turn_interrupt",
        "turn_interrupt_mid_tool",
        "message_steering",
        "provider_thread_resume",
        "thread_rollback",
        "thread_fork_native",
        "subagent",
        "usage_limit",
        "runtime_exit_unexpected",
        "background_work",
    ];

    fn replay(scenario: &str) -> ScenarioResult {
        replay_as(scenario, CODEX_PROVIDER)
    }

    fn native_id(reference: Option<&ProviderRef>) -> Option<(&str, RefStrength)> {
        reference.and_then(|reference| Some((reference.native_id.as_deref()?, reference.strength)))
    }

    #[test]
    fn simple() {
        let result = replay("simple");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed]);
        assert_eq!(
            assistant_texts(&result, main_id(&result)),
            ["fixture simple ok"]
        );
        let turns = provider_turns(main);
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].status, ProviderTurnStatus::Completed);
        assert_eq!(
            native_id(turns[0].native_turn_ref.as_ref()),
            Some(("00000000-0000-4000-8000-000000000002", RefStrength::Strong))
        );
        // The context meter: the newest call's total and the window.
        let usage = turns[0].token_usage.clone().expect("usage");
        assert_eq!((usage.used, usage.max), (22737, Some(258400)));
        let run = runs(main)[0];
        let provider_thread = &main.provider_threads[&run.provider_thread_id.unwrap()];
        assert_eq!(
            native_id(provider_thread.native_thread_ref.as_ref()),
            Some(("00000000-0000-4000-8000-000000000001", RefStrength::Strong))
        );
        let session = main.provider_sessions.values().next().unwrap();
        assert_eq!(session.capabilities, codex_capabilities(&codex_instance()));
        assert_eq!(session.status, SessionStatus::Stopped);
        assert_eq!(terminals(&result), [TurnOutcome::Completed]);
    }

    #[test]
    fn multi_turn() {
        let result = replay("multi_turn");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed, RunStatus::Completed]);
        assert_eq!(
            assistant_texts(&result, main_id(&result)),
            [
                "first fixture turn complete",
                "second fixture turn complete"
            ]
        );
        let turns = provider_turns(main);
        assert_eq!(
            turns.iter().map(|turn| turn.ordinal).collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(main.provider_sessions.len(), 1);
    }

    #[test]
    fn tool_call_read_only_streams_command_output() {
        let result = replay("tool_call_read_only");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed]);
        let rows = items(&result, main_id(&result));
        assert!(rows.iter().any(|item| matches!(
            &item.kind,
            TurnItemKind::CommandExecution { output: Some(output), exit_code: Some(0), .. }
                if output.contains("stream line 4")
        ) && item.status == ItemStatus::Completed));
        let deltas = result
            .adapter_events
            .iter()
            .filter(|event| matches!(event, AdapterEvent::CommandOutputDelta { .. }))
            .count();
        assert!(deltas >= 4, "{deltas} output deltas");
        assert!(
            main.nodes
                .values()
                .any(|node| node.kind == NodeKind::ToolCall && node.status == NodeStatus::Completed)
        );
        assert!(
            assistant_texts(&result, main_id(&result))
                .contains(&"command output fixture complete".to_owned())
        );
        // Rate-limit snapshots without windows show no meter.
        assert!(
            !result
                .adapter_events
                .iter()
                .any(|event| matches!(event, AdapterEvent::AccountUsageUpdated { .. }))
        );
    }

    #[test]
    fn tool_call_approval_passes_native_decisions_through() {
        let result = replay("tool_call_approval");
        let main = &result.projection;
        assert_eq!(main.thread.runtime_mode, RuntimeMode::Ask);
        assert_eq!(statuses(main), [RunStatus::Completed]);
        let request = main.runtime_requests.values().next().unwrap();
        assert_eq!(request.kind, RuntimeRequestKind::CommandApproval);
        assert_eq!(request.status, RequestStatus::Resolved);
        assert_eq!(
            request.response,
            Some(RuntimeResponse::Approval {
                option_id: "accept".into()
            })
        );
        assert_eq!(
            request
                .options
                .iter()
                .map(|option| (option.id.as_str(), option.allow))
                .collect::<Vec<_>>(),
            [
                ("accept", true),
                ("acceptWithExecpolicyAmendment", true),
                ("cancel", false)
            ]
        );
        let rows = items(&result, main_id(&result));
        assert!(rows.iter().any(|item| matches!(
            &item.kind,
            TurnItemKind::ApprovalRequest { prompt: Some(prompt), .. } if prompt.contains(".codex-probe-write-action.txt")
        )));
    }

    #[test]
    fn plan_questions_answer_the_native_question() {
        let result = replay("plan_questions");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed]);
        let request = main.runtime_requests.values().next().unwrap();
        assert_eq!(request.kind, RuntimeRequestKind::UserInput);
        assert_eq!(request.status, RequestStatus::Resolved);
        assert_eq!(request.questions[0].id, "schema_preference");
        assert_eq!(request.questions[0].options.len(), 2);
        let rows = items(&result, main_id(&result));
        assert!(rows.iter().any(|item| matches!(
            &item.kind,
            TurnItemKind::UserInputRequest { answers: Some(answers), .. }
                if answers[0].answers == ["Strict schemas (Recommended)"]
        )));
    }

    #[test]
    fn todo_list_maps_turn_plan_updates() {
        let result = replay("todo_list");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed]);
        let plan = main.plans.values().next().unwrap();
        let PlanKind::TodoList { steps, .. } = &plan.kind else {
            panic!("a todo list");
        };
        assert_eq!(
            steps
                .iter()
                .map(|step| step.text.as_str())
                .collect::<Vec<_>>(),
            [
                "Inspect package.json",
                "Inspect tsconfig.json",
                "Report completion"
            ]
        );
        assert!(
            steps
                .iter()
                .all(|step| step.status == PlanStepStatus::Completed)
        );
        assert_eq!(plan.status, PlanStatus::Completed);
        let plan_updates = result
            .adapter_events
            .iter()
            .filter(|event| matches!(event, AdapterEvent::PlanUpdated { .. }))
            .count();
        assert_eq!(plan_updates, 2);
    }

    #[test]
    fn turn_interrupt_keeps_the_session() {
        let result = replay("turn_interrupt");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Interrupted]);
        assert_eq!(terminals(&result), [TurnOutcome::Interrupted]);
        assert_eq!(
            provider_turns(main)[0].status,
            ProviderTurnStatus::Interrupted
        );
        let rows = items(&result, main_id(&result));
        assert!(
            rows.iter()
                .any(|item| matches!(item.kind, TurnItemKind::RunInterruptResult { .. }))
        );
        // The session ended only at shutdown, not with the interrupt.
        let session = main.provider_sessions.values().next().unwrap();
        assert_ne!(session.detach_reason.as_deref(), Some("session lost"));
    }

    #[test]
    fn turn_interrupt_mid_tool_closes_the_running_command() {
        let result = replay("turn_interrupt_mid_tool");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Interrupted]);
        assert_eq!(terminals(&result), [TurnOutcome::Interrupted]);
        let rows = items(&result, main_id(&result));
        assert!(rows.iter().any(
            |item| matches!(item.kind, TurnItemKind::CommandExecution { .. })
                && item.status == ItemStatus::Interrupted
        ));
        assert!(
            !main
                .nodes
                .values()
                .any(|node| node.kind == NodeKind::ToolCall && node.status == NodeStatus::Running)
        );
    }

    #[test]
    fn message_steering_is_native() {
        let result = replay("message_steering");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed]);
        let run = runs(main)[0];
        let attempts = main
            .attempts
            .values()
            .filter(|attempt| attempt.run_id == run.id)
            .count();
        assert_eq!(attempts, 1, "native steering keeps one attempt");
        let rows = items(&result, main_id(&result));
        assert!(rows.iter().any(|item| matches!(
            &item.kind,
            TurnItemKind::UserMessage { input_intent: InputIntent::Steer, text, .. }
                if text == "Actually, respond with exactly: steering fixture observed"
        )));
        assert_eq!(
            assistant_texts(&result, main_id(&result)),
            [
                "steering fixture initial response",
                "steering fixture observed"
            ]
        );
    }

    #[test]
    fn provider_thread_resume_reopens_the_native_thread() {
        let result = replay("provider_thread_resume");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed, RunStatus::Completed]);
        assert_eq!(main.provider_sessions.len(), 2);
        let all = runs(main);
        assert_eq!(all[0].provider_thread_id, all[1].provider_thread_id);
        let transcript =
            read_transcript(&fixtures().join("provider_thread_resume/codex.ndjson")).unwrap();
        assert!(transcript.entries.iter().any(|entry| matches!(
            entry,
            ReplayEntry::ExpectOutbound { frame, channel: Some(channel), .. }
                if frame["method"] == "thread/resume" && channel == "main#2"
        )));
    }

    #[test]
    fn thread_rollback_reverts_in_place() {
        let result = replay("thread_rollback");
        let main = &result.projection;
        assert_eq!(
            statuses(main),
            [
                RunStatus::Completed,
                RunStatus::RolledBack,
                RunStatus::Completed
            ]
        );
        let provider_threads = runs(main)
            .iter()
            .map(|run| run.provider_thread_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(provider_threads.len(), 1, "reverted in place");
        assert!(
            result
                .checkpoint_calls
                .iter()
                .any(|call| call.starts_with("restore refs/waku/t/"))
        );
        // `beforeTurnId` names the first dropped turn: the second one.
        let transcript = read_transcript(&fixtures().join("thread_rollback/codex.ndjson")).unwrap();
        let second_turn = provider_turns(main)
            .iter()
            .find(|turn| turn.ordinal == 2)
            .and_then(|turn| turn.native_turn_ref.as_ref())
            .and_then(|reference| reference.native_id.clone())
            .unwrap();
        assert!(transcript.entries.iter().any(|entry| matches!(
            entry,
            ReplayEntry::ExpectOutbound { frame, .. }
                if frame["method"] == "thread/revert" && frame["params"]["beforeTurnId"] == second_turn.as_str()
        )));
    }

    #[test]
    fn thread_fork_native_runs_the_fork_in_its_own_session() {
        let result = replay("thread_fork_native");
        assert!(matches!(result.forks[..], [ForkResult::Native { .. }]));
        let main = &result.projection;
        let fork = &result.threads["fork-a"];
        let source_thread = main.thread.active_provider_thread_id.unwrap();
        let forked = &fork.provider_threads[&fork.thread.active_provider_thread_id.unwrap()];
        let from = forked.forked_from.as_ref().unwrap();
        assert_eq!(from.provider_thread_id, source_thread);
        assert!(from.provider_turn_id.is_some());
        assert_eq!(statuses(fork), [RunStatus::Completed, RunStatus::Completed]);
        assert_eq!(
            assistant_texts(&result, fork.thread.id),
            ["fork marker stored", "source-marker-7Q9V|fork-marker-2K4M"]
        );
    }

    #[test]
    fn subagent_turns_do_not_close_the_parent() {
        let result = replay("subagent");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed]);
        let subagent = main.subagents.values().next().unwrap();
        assert_eq!(subagent.status, SubagentStatus::Completed);
        assert_eq!(subagent.result.as_deref(), Some("Hello."));
        assert_eq!(subagent.title.as_deref(), Some("/root/say_hello"));
        let child_thread = main
            .provider_threads
            .values()
            .find(|thread| thread.owner_node_id == Some(subagent.id))
            .expect("the subagent's provider thread");
        assert!(child_thread.native_thread_ref.is_some());
        let child_turn = main
            .provider_turns
            .values()
            .find(|turn| turn.provider_thread_id == child_thread.id)
            .unwrap();
        assert_eq!(child_turn.status, ProviderTurnStatus::Completed);
        assert_eq!(
            terminals(&result),
            [TurnOutcome::Completed, TurnOutcome::Completed]
        );
        assert_eq!(assistant_texts(&result, main_id(&result)), ["Hello."]);
    }

    #[test]
    fn usage_limit_takes_the_reset_time_from_the_rate_limits() {
        let result = replay("usage_limit");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Failed]);
        let failure = runs(main)[0].failure.clone().unwrap();
        assert_eq!(failure.class, FailureClass::UsageLimit);
        assert_eq!(failure.code.as_deref(), Some("usageLimitExceeded"));
        assert_eq!(failure.reset_at, Some(2_000_100_000_000));
        let recovery = main.thread.limit_recovery.clone().unwrap();
        assert_eq!(recovery.reset_at, Some(2_000_100_000_000));
        // The account meters reached the instance.
        let usage = result
            .adapter_events
            .iter()
            .find_map(|event| match event {
                AdapterEvent::AccountUsageUpdated { usage } => Some(usage.clone()),
                _ => None,
            })
            .expect("account usage");
        assert_eq!(usage.windows[0].percent, 100.0);
        assert_eq!(usage.windows[0].resets_at, Some(2_000_100_000));
    }

    #[test]
    fn runtime_exit_unexpected_fails_the_turn_once() {
        let result = replay("runtime_exit_unexpected");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Failed]);
        let failure = runs(main)[0].failure.clone().unwrap();
        assert_eq!(failure.class, FailureClass::TransportError);
        assert_eq!(terminals(&result).len(), 1);
    }

    #[test]
    fn background_work_outlives_the_turn() {
        let result = replay("background_work");
        assert_eq!(statuses(&result.projection), [RunStatus::Completed]);
        let work = result
            .adapter_events
            .iter()
            .filter_map(|event| match event {
                AdapterEvent::BackgroundWorkUpdated { work } => {
                    Some((work.task.task_id.clone(), work.live))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            work,
            [("26567".to_owned(), true), ("26567".to_owned(), false)]
        );
    }
}

// -- Claude Code (#47) -------------------------------------------------------------

/// The Claude Code fixtures (`<scenario>/claude.ndjson`), hand-authored from
/// the stream-json protocol (see `fixtures/replay/README.md`).
mod claude {
    use super::*;

    pub const COVERED: [&str; 16] = [
        "simple",
        "multi_turn",
        "tool_call_read_only",
        "tool_call_approval",
        "plan_questions",
        "turn_interrupt",
        "turn_interrupt_mid_tool",
        "message_steering",
        "provider_thread_resume",
        "thread_rollback",
        "thread_fork_native",
        "usage_limit",
        "runtime_exit_unexpected",
        "background_work",
        "subagent",
        "model_switch",
    ];

    fn claude(scenario: &str) -> ScenarioResult {
        replay_as(scenario, CLAUDE_PROVIDER)
    }

    #[test]
    fn claude_fixtures_replay() {
        let mut failures = Vec::new();
        for scenario in COVERED {
            let transcript =
                read_transcript(&fixtures().join(scenario).join("claude.ndjson")).unwrap();
            if let Err(error) = run_transcript(scenario, transcript) {
                failures.push(format!("{scenario}: {error}"));
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n\n"));
    }

    fn native_turn_ids(projection: &ThreadProjection) -> Vec<(u32, Option<String>, RefStrength)> {
        provider_turns(projection)
            .iter()
            .map(|turn| {
                let reference = turn.native_turn_ref.as_ref();
                (
                    turn.ordinal,
                    reference.and_then(|reference| reference.native_id.clone()),
                    reference.map_or(RefStrength::None, |reference| reference.strength),
                )
            })
            .collect()
    }

    fn resume_cursor(thread: &ProviderThread) -> Option<String> {
        thread
            .native_metadata
            .as_ref()
            .and_then(|metadata| metadata.resume_cursor.as_ref())
            .map(|cursor| cursor.0.clone())
    }

    #[test]
    fn claude_simple() {
        let result = claude("simple");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed]);
        assert_eq!(
            assistant_texts(&result, main_id(&result)),
            ["fixture simple ok"]
        );
        let rows = items(&result, main_id(&result));
        assert!(rows.iter().any(|item| matches!(
            &item.kind,
            TurnItemKind::Reasoning { text, streaming: false } if text == "The user wants an echo."
        )));
        // The turn's weak native id is the assistant uuid that ended it.
        assert_eq!(
            native_turn_ids(main),
            [(
                1,
                Some("00000000-0000-4000-8000-000000000001".to_owned()),
                RefStrength::Weak
            )]
        );
        let run = runs(main)[0];
        let thread = &main.provider_threads[&run.provider_thread_id.unwrap()];
        assert_eq!(
            thread
                .native_thread_ref
                .as_ref()
                .map(|reference| (reference.native_id.as_deref(), reference.strength)),
            Some((
                Some("00000000-0000-7000-8000-000000000002"),
                RefStrength::Strong
            ))
        );
        // The cursor keeps the resume point (today's `resume_at`).
        assert_eq!(
            resume_cursor(thread).as_deref(),
            Some(
                "{\"provider\":\"claude\",\"sessionId\":\"00000000-0000-7000-8000-000000000002\",\"resumeAt\":\"00000000-0000-4000-8000-000000000001\"}"
            )
        );
        let session = main.provider_sessions.values().next().unwrap();
        assert_eq!(session.capabilities, claude_capabilities());
        assert_eq!(terminals(&result), [TurnOutcome::Completed]);
        // Usage: the last call's context and the model's window from `result`.
        let turn = provider_turns(main)[0];
        assert_eq!(
            turn.token_usage
                .as_ref()
                .map(|usage| (usage.used, usage.max)),
            Some((1512, Some(200_000)))
        );
    }

    #[test]
    fn claude_multi_turn_keeps_one_process() {
        let result = claude("multi_turn");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed, RunStatus::Completed]);
        assert_eq!(
            assistant_texts(&result, main_id(&result)),
            ["Echo: first", "Echo: second"]
        );
        assert_eq!(main.provider_sessions.len(), 1);
        let turns = provider_turns(main);
        assert_eq!(turns.len(), 2);
        assert_eq!(turns[0].provider_thread_id, turns[1].provider_thread_id);
    }

    #[test]
    fn claude_read_only_tool() {
        let result = claude("tool_call_read_only");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed]);
        let rows = items(&result, main_id(&result));
        let command = rows
            .iter()
            .find(|item| matches!(item.kind, TurnItemKind::CommandExecution { .. }))
            .expect("a command row");
        assert_eq!(command.status, ItemStatus::Completed);
        assert!(matches!(
            &command.kind,
            TurnItemKind::CommandExecution { input, output: Some(output), .. }
                if input == "ls" && output == "README.md\n"
        ));
        assert_eq!(
            command
                .native_item_ref
                .as_ref()
                .and_then(|reference| reference.native_id.as_deref()),
            Some("toolu_ls")
        );
        assert!(main.runtime_requests.is_empty(), "Full Access asks nothing");
    }

    #[test]
    fn claude_tool_call_approval() {
        let result = claude("tool_call_approval");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed]);
        let request = main.runtime_requests.values().next().unwrap();
        assert_eq!(request.kind, RuntimeRequestKind::CommandApproval);
        assert_eq!(request.status, RequestStatus::Resolved);
        assert_eq!(
            request
                .native_request_ref
                .as_ref()
                .and_then(|reference| reference.native_id.as_deref()),
            Some("perm-1")
        );
        assert_eq!(
            request
                .options
                .iter()
                .map(|option| option.id.as_str())
                .collect::<Vec<_>>(),
            ["allow", "deny"]
        );
        let rows = items(&result, main_id(&result));
        assert!(rows.iter().any(|item| matches!(
            &item.kind,
            TurnItemKind::ApprovalRequest { prompt: Some(prompt), .. } if prompt == "ls"
        )));
        assert_eq!(
            assistant_texts(&result, main_id(&result)),
            ["Listed the files."]
        );
    }

    #[test]
    fn claude_plan_and_structured_questions() {
        let result = claude("plan_questions");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed]);
        let mut kinds = main
            .plans
            .values()
            .map(|plan| plan.kind.type_name())
            .collect::<Vec<_>>();
        kinds.sort();
        assert_eq!(kinds, ["proposed_plan", "todo_list"]);
        assert!(main.plans.values().any(|plan| matches!(
            &plan.kind,
            PlanKind::TodoList { steps, .. } if steps.len() == 2 && steps[0].status == PlanStepStatus::Running
        )));
        let request = main.runtime_requests.values().next().unwrap();
        assert_eq!(request.kind, RuntimeRequestKind::UserInput);
        assert_eq!(request.status, RequestStatus::Resolved);
        assert_eq!(request.questions[0].id, "Which color should the button be?");
        assert_eq!(request.questions[0].options.len(), 2);
        assert_eq!(
            assistant_texts(&result, main_id(&result)),
            ["You chose Blue."]
        );
    }

    #[test]
    fn claude_turn_interrupt() {
        let result = claude("turn_interrupt");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Interrupted]);
        assert_eq!(terminals(&result), [TurnOutcome::Interrupted]);
        let rows = items(&result, main_id(&result));
        assert!(rows.iter().any(|item| matches!(
            &item.kind,
            TurnItemKind::AssistantMessage { streaming: false, text, .. } if text == "Working"
        ) && item.status == ItemStatus::Interrupted));
        assert_eq!(
            provider_turns(main)[0].status,
            ProviderTurnStatus::Interrupted
        );
    }

    #[test]
    fn claude_turn_interrupt_mid_tool() {
        let result = claude("turn_interrupt_mid_tool");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Interrupted]);
        assert_eq!(terminals(&result), [TurnOutcome::Interrupted]);
        let rows = items(&result, main_id(&result));
        let command = rows
            .iter()
            .find(|item| matches!(item.kind, TurnItemKind::CommandExecution { .. }))
            .expect("the running command");
        assert_eq!(command.status, ItemStatus::Interrupted);
    }

    #[test]
    fn claude_message_steering_is_native() {
        let result = claude("message_steering");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed]);
        let run = runs(main)[0];
        let attempts = main
            .attempts
            .values()
            .filter(|attempt| attempt.run_id == run.id)
            .count();
        assert_eq!(attempts, 1, "native steering keeps one attempt");
        assert_eq!(terminals(&result), [TurnOutcome::Completed]);
        assert_eq!(
            assistant_texts(&result, main_id(&result)),
            ["Working", "Steered: use tabs"]
        );
    }

    #[test]
    fn claude_resumes_an_unloaded_thread() {
        let result = claude("provider_thread_resume");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed, RunStatus::Completed]);
        assert_eq!(main.provider_sessions.len(), 2);
        let all = runs(main);
        assert_eq!(all[0].provider_thread_id, all[1].provider_thread_id);
        let transcript =
            read_transcript(&fixtures().join("provider_thread_resume/claude.ndjson")).unwrap();
        let spawns = transcript
            .entries
            .iter()
            .filter_map(|entry| match entry {
                ReplayEntry::ExpectOutbound { frame, .. } if frame["type"] == "process_start" => {
                    Some(frame["args"].clone())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            spawns[0]
                .as_array()
                .unwrap()
                .iter()
                .any(|arg| arg == "--session-id")
        );
        assert!(
            spawns[1]
                .as_array()
                .unwrap()
                .iter()
                .any(|arg| arg == "--resume")
        );
    }

    #[test]
    fn claude_rollback_replaces_the_thread() {
        let result = claude("thread_rollback");
        let main = &result.projection;
        assert_eq!(
            statuses(main),
            [
                RunStatus::Completed,
                RunStatus::RolledBack,
                RunStatus::Completed
            ]
        );
        let all = runs(main);
        assert_ne!(all[0].provider_thread_id, all[2].provider_thread_id);
        let replacement = &main.provider_threads[&all[2].provider_thread_id.unwrap()];
        assert_eq!(
            replacement
                .forked_from
                .as_ref()
                .map(|fork| fork.provider_thread_id),
            all[0].provider_thread_id
        );
        assert_eq!(
            replacement
                .native_thread_ref
                .as_ref()
                .and_then(|reference| reference.native_id.as_deref()),
            Some("00000000-0000-7000-8000-000000000003")
        );
        assert_eq!(
            main.thread.active_provider_thread_id,
            all[2].provider_thread_id
        );
    }

    #[test]
    fn claude_fork_copies_the_conversation() {
        let result = claude("thread_fork_native");
        assert!(matches!(result.forks[..], [ForkResult::Native { .. }]));
        let main = &result.projection;
        let fork = &result.threads["fork-a"];
        let forked = &fork.provider_threads[&fork.thread.active_provider_thread_id.unwrap()];
        assert_eq!(
            forked
                .forked_from
                .as_ref()
                .map(|from| from.provider_thread_id),
            main.thread.active_provider_thread_id
        );
        assert_eq!(statuses(fork), [RunStatus::Completed]);
        assert_eq!(
            assistant_texts(&result, fork.thread.id),
            ["Echo: in the fork"]
        );
    }

    #[test]
    fn claude_usage_limit_carries_the_reset_time() {
        let result = claude("usage_limit");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Failed]);
        let failure = runs(main)[0].failure.clone().unwrap();
        assert_eq!(failure.class, FailureClass::UsageLimit);
        // `resetsAt` is epoch seconds on the wire.
        assert_eq!(failure.reset_at, Some(1_767_240_000_000));
        assert_eq!(
            main.thread.limit_recovery.clone().unwrap().reset_at,
            Some(1_767_240_000_000)
        );
    }

    #[test]
    fn claude_unexpected_exit_fails_the_turn() {
        let result = claude("runtime_exit_unexpected");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Failed]);
        let failure = runs(main)[0].failure.clone().unwrap();
        assert_eq!(failure.class, FailureClass::TransportError);
        assert_eq!(failure.code.as_deref(), Some("process_exit"));
        assert!(matches!(
            terminals(&result)[..],
            [TurnOutcome::Failed { .. }]
        ));
    }

    #[test]
    fn claude_background_work_parks_the_turn() {
        let result = claude("background_work");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed, RunStatus::Completed]);
        let parked = result.adapter_events.iter().find_map(|event| match event {
            AdapterEvent::TurnTerminal { terminal } => Some(terminal.background_pending),
            _ => None,
        });
        assert_eq!(parked, Some(true), "the reply with a live task parks");
        let work = result
            .adapter_events
            .iter()
            .filter_map(|event| match event {
                AdapterEvent::BackgroundWorkUpdated { work } => {
                    Some((work.task.task_id.clone(), work.task.kind, work.live))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            work,
            [
                ("bkbuild1".to_owned(), PendingTaskKind::Command, true),
                ("bkbuild1".to_owned(), PendingTaskKind::Command, false),
            ]
        );
        // The task settled during the next turn and left the thread.
        assert_eq!(terminals(&result).len(), 2);
        let thread = &main.provider_threads[&runs(main)[0].provider_thread_id.unwrap()];
        assert!(thread.pending_background_tasks.is_empty());
    }

    #[test]
    fn claude_subagents_are_child_turns() {
        let result = claude("subagent");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed]);
        let subagent = main.subagents.values().next().unwrap();
        assert_eq!(subagent.status, SubagentStatus::Completed);
        assert_eq!(subagent.result.as_deref(), Some("Found it."));
        assert_eq!(subagent.title.as_deref(), Some("Search the repo"));
        assert_eq!(
            subagent
                .native_task_ref
                .as_ref()
                .and_then(|reference| reference.native_id.as_deref()),
            Some("agent-1")
        );
        let child_thread = main
            .provider_threads
            .values()
            .find(|thread| thread.owner_node_id == Some(subagent.id))
            .expect("the subagent's provider thread");
        let child_turn = main
            .provider_turns
            .values()
            .find(|turn| turn.provider_thread_id == child_thread.id)
            .unwrap();
        assert_eq!(child_turn.status, ProviderTurnStatus::Completed);
        assert_eq!(
            terminals(&result),
            [TurnOutcome::Completed, TurnOutcome::Completed]
        );
        // The subagent's own frames never reach the parent's transcript.
        assert_eq!(
            assistant_texts(&result, main_id(&result)),
            ["Echo: [subagent] delegate the search"]
        );
    }

    #[test]
    fn claude_model_switch_is_in_session() {
        let result = claude("model_switch");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed, RunStatus::Completed]);
        assert_eq!(
            runs(main)[1].model_selection.model.as_deref(),
            Some("claude-sonnet-5")
        );
        assert_eq!(main.provider_sessions.len(), 1, "set_model, no restart");
    }

    #[test]
    fn claude_capabilities_match_the_snapshot() {
        let path = fixtures().join("../capabilities_claude.json");
        let expected: Capabilities =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(claude_capabilities(), expected);
        assert_eq!(
            ClaudeDriver.capabilities(&claude_instance()),
            claude_capabilities()
        );
    }
}

// -- OpenCode (#49): HTTP + SSE against `opencode serve` ---------------------------

/// The OpenCode fixtures (`<scenario>/opencode.ndjson`), hand-derived from
/// the protocol (see `fixtures/replay/README.md`).
mod opencode {
    use super::*;

    pub const COVERED: &[&str] = &[
        "simple",
        "multi_turn",
        "tool_call_read_only",
        "tool_call_approval",
        "plan_questions",
        "turn_interrupt",
        "turn_interrupt_mid_tool",
        "message_steering",
        "provider_thread_resume",
        "thread_rollback",
        "thread_fork_native",
        "usage_limit",
        "stream_resync",
        "background_work",
    ];

    fn opencode(scenario: &str) -> ScenarioResult {
        replay_as(scenario, OPENCODE_PROVIDER)
    }

    #[test]
    fn opencode_simple() {
        let result = opencode("simple");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed]);
        assert_eq!(
            assistant_texts(&result, main_id(&result)),
            ["fixture simple ok"]
        );
        let run = runs(main)[0];
        assert_eq!(native_thread(main, run).as_deref(), Some("ses_1"));
        let turns = provider_turns(main);
        assert_eq!(turns.len(), 1);
        let native = turns[0].native_turn_ref.as_ref().unwrap();
        // The turn's native id is the user message that opens it (weak).
        assert_eq!(native.native_id.as_deref(), Some("msg_1"));
        assert_eq!(native.strength, RefStrength::Weak);
        // Usage: the step's context occupancy against the model's window.
        let usage = turns[0].token_usage.clone().expect("usage");
        assert_eq!((usage.used, usage.max), (1240, Some(200_000)));
        assert_eq!(terminals(&result), [TurnOutcome::Completed]);
        let session = main.provider_sessions.values().next().unwrap();
        assert_eq!(
            session.capabilities,
            waku_core::driver::opencode::opencode_capabilities(&opencode_replay_instance())
        );
    }

    #[test]
    fn opencode_multi_turn() {
        let result = opencode("multi_turn");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed, RunStatus::Completed]);
        assert_eq!(
            assistant_texts(&result, main_id(&result)),
            ["Echo: first", "Echo: second"]
        );
        assert_eq!(main.provider_sessions.len(), 1);
    }

    #[test]
    fn opencode_tool_call_read_only() {
        let result = opencode("tool_call_read_only");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed]);
        let rows = items(&result, main_id(&result));
        let read = rows
            .iter()
            .find(|item| {
                matches!(&item.kind, TurnItemKind::DynamicTool { tool_name: Some(name), .. } if name == "read")
            })
            .expect("a read tool row");
        assert_eq!(read.status, ItemStatus::Completed);
        // Provider control markers never reach a transcript.
        let TurnItemKind::DynamicTool { input, .. } = &read.kind else {
            unreachable!()
        };
        assert!(!input.as_str().contains("providerState"), "{input:?}");
        assert!(main.runtime_requests.is_empty());
        assert_eq!(
            assistant_texts(&result, main_id(&result)),
            ["The README is a heading."]
        );
    }

    #[test]
    fn opencode_tool_call_approval() {
        let result = opencode("tool_call_approval");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed]);
        let request = main.runtime_requests.values().next().unwrap();
        assert_eq!(request.kind, RuntimeRequestKind::CommandApproval);
        assert_eq!(request.status, RequestStatus::Resolved);
        assert_eq!(
            request
                .options
                .iter()
                .map(|option| option.id.as_str())
                .collect::<Vec<_>>(),
            ["once", "always", "reject"]
        );
        assert_eq!(
            request
                .native_request_ref
                .as_ref()
                .and_then(|reference| reference.native_id.as_deref()),
            Some("per_1")
        );
        let rows = items(&result, main_id(&result));
        assert!(rows.iter().any(|item| matches!(
            &item.kind,
            TurnItemKind::CommandExecution { input, .. } if input == "ls"
        ) && item.status == ItemStatus::Completed));
        assert!(
            result
                .adapter_events
                .iter()
                .any(|event| matches!(event, AdapterEvent::CommandOutputDelta { .. }))
        );
    }

    #[test]
    fn opencode_plan_questions() {
        let result = opencode("plan_questions");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed]);
        let request = main.runtime_requests.values().next().unwrap();
        assert_eq!(request.kind, RuntimeRequestKind::UserInput);
        assert_eq!(request.status, RequestStatus::Resolved);
        assert_eq!(request.questions[0].id, "q1");
        assert_eq!(request.questions[0].options[0].label, "Blue");
        assert_eq!(
            assistant_texts(&result, main_id(&result)),
            ["You chose Blue."]
        );
    }

    #[test]
    fn opencode_turn_interrupt() {
        let result = opencode("turn_interrupt");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Interrupted]);
        assert_eq!(terminals(&result), [TurnOutcome::Interrupted]);
        assert_eq!(
            provider_turns(main)[0].status,
            ProviderTurnStatus::Interrupted
        );
    }

    #[test]
    fn opencode_turn_interrupt_mid_tool() {
        let result = opencode("turn_interrupt_mid_tool");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Interrupted]);
        assert_eq!(terminals(&result), [TurnOutcome::Interrupted]);
        let rows = items(&result, main_id(&result));
        assert!(rows.iter().any(|item| matches!(
            &item.kind,
            TurnItemKind::CommandExecution { input, .. } if input == "sleep 600"
        ) && item.status.is_terminal()));
    }

    #[test]
    fn opencode_message_steering_is_native() {
        let result = opencode("message_steering");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed]);
        let run = runs(main)[0];
        let attempts = main
            .attempts
            .values()
            .filter(|attempt| attempt.run_id == run.id)
            .count();
        assert_eq!(attempts, 1, "native steering keeps one attempt");
        assert_eq!(terminals(&result), [TurnOutcome::Completed]);
        assert_eq!(
            assistant_texts(&result, main_id(&result)),
            ["Working", "Steered: use tabs"]
        );
    }

    #[test]
    fn opencode_provider_thread_resume() {
        let result = opencode("provider_thread_resume");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed, RunStatus::Completed]);
        assert_eq!(main.provider_sessions.len(), 2);
        let all = runs(main);
        assert_eq!(all[0].provider_thread_id, all[1].provider_thread_id);
    }

    #[test]
    fn opencode_rollback_forks_a_replacement_session() {
        let result = opencode("thread_rollback");
        let main = &result.projection;
        assert_eq!(
            statuses(main),
            [
                RunStatus::Completed,
                RunStatus::RolledBack,
                RunStatus::Completed
            ]
        );
        let all = runs(main);
        assert_ne!(all[0].provider_thread_id, all[2].provider_thread_id);
        assert_eq!(native_thread(main, all[2]).as_deref(), Some("ses_2"));
        let replacement = &main.provider_threads[&all[2].provider_thread_id.unwrap()];
        assert_eq!(
            replacement
                .forked_from
                .as_ref()
                .map(|fork| fork.provider_thread_id),
            all[0].provider_thread_id
        );
        assert_eq!(
            assistant_texts(&result, main_id(&result))
                .last()
                .map(String::as_str),
            Some("Echo: three")
        );
    }

    #[test]
    fn opencode_fork_is_native() {
        let result = opencode("thread_fork_native");
        assert!(matches!(result.forks[..], [ForkResult::Native { .. }]));
        let fork = &result.threads["fork-a"];
        assert_eq!(statuses(fork), [RunStatus::Completed]);
        let forked = &fork.provider_threads[&fork.thread.active_provider_thread_id.unwrap()];
        assert_eq!(
            forked
                .native_thread_ref
                .as_ref()
                .and_then(|reference| reference.native_id.as_deref()),
            Some("ses_2")
        );
        assert_eq!(
            assistant_texts(&result, fork.thread.id),
            ["Echo: in the fork"]
        );
    }

    #[test]
    fn opencode_usage_limit() {
        let result = opencode("usage_limit");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Failed]);
        let failure = runs(main)[0].failure.clone().unwrap();
        assert_eq!(failure.class, FailureClass::UsageLimit);
        assert_eq!(failure.code.as_deref(), Some("provider.quota"));
        // OpenCode reports no structured reset time.
        assert_eq!(failure.reset_at, None);
    }

    #[test]
    fn opencode_stream_resync_is_not_a_lost_session() {
        let result = opencode("stream_resync");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed]);
        assert_eq!(main.provider_sessions.len(), 1);
        assert_eq!(
            assistant_texts(&result, main_id(&result)),
            ["Working still"]
        );
    }

    #[test]
    fn opencode_background_shells_outlive_their_turn() {
        let result = opencode("background_work");
        assert_eq!(statuses(&result.projection), [RunStatus::Completed]);
        let work = result
            .adapter_events
            .iter()
            .filter_map(|event| match event {
                AdapterEvent::BackgroundWorkUpdated { work } => {
                    Some((work.task.task_id.clone(), work.live))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            work,
            [("shl_1".to_owned(), true), ("shl_1".to_owned(), false)]
        );
        // The shell ended after the turn's terminal.
        let terminal = result
            .adapter_events
            .iter()
            .position(|event| matches!(event, AdapterEvent::TurnTerminal { .. }))
            .unwrap();
        let ended = result
            .adapter_events
            .iter()
            .rposition(|event| matches!(event, AdapterEvent::BackgroundWorkUpdated { .. }))
            .unwrap();
        assert!(ended > terminal);
    }
}

// -- DeepSeek Harness (#52): `dsh web` RPC + WebSocket downlinks -------------------

/// The DeepSeek Harness fixtures (`<scenario>/deepseek.ndjson`),
/// hand-derived from the protocol (see `fixtures/replay/README.md`).
mod deepseek {
    use super::*;

    pub const COVERED: &[&str] = &[
        "simple",
        "multi_turn",
        "tool_call_read_only",
        "tool_call_approval",
        "plan_questions",
        "turn_interrupt",
        "turn_interrupt_mid_tool",
        "message_steering",
        "provider_thread_resume",
        "thread_rollback",
        "thread_fork_native",
        "usage_limit",
        "runtime_exit_unexpected",
        "background_work",
        "subagent",
    ];

    const DEEPSEEK_SESSION: &str = "6f1c0b9e-4d2a-4c3b-9a8e-1f2d3c4b5a60";
    const DEEPSEEK_FORK: &str = "7a2d1c0f-5e3b-4d4c-8b9f-2e3f4d5c6b71";

    fn deepseek(scenario: &str) -> ScenarioResult {
        replay_as(scenario, DEEPSEEK_PROVIDER)
    }

    #[test]
    fn deepseek_simple() {
        let result = deepseek("simple");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed]);
        assert_eq!(
            assistant_texts(&result, main_id(&result)),
            ["fixture simple ok"]
        );
        assert_eq!(
            native_thread(main, runs(main)[0]).as_deref(),
            Some(DEEPSEEK_SESSION)
        );
        let turns = provider_turns(main);
        let native = turns[0].native_turn_ref.as_ref().unwrap();
        // The `turn/start` seq is the strong native turn id.
        assert_eq!(native.native_id.as_deref(), Some("2"));
        assert_eq!(native.strength, RefStrength::Strong);
        assert_eq!(
            turns[0].token_usage.as_ref().map(|usage| usage.used),
            Some(930)
        );
        assert_eq!(terminals(&result), [TurnOutcome::Completed]);
        let session = main.provider_sessions.values().next().unwrap();
        assert_eq!(
            session.capabilities,
            waku_core::driver::deepseek::deepseek_capabilities(&deepseek_replay_instance())
        );
    }

    #[test]
    fn deepseek_multi_turn() {
        let result = deepseek("multi_turn");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed, RunStatus::Completed]);
        assert_eq!(
            assistant_texts(&result, main_id(&result)),
            ["Echo: first", "Echo: second"]
        );
    }

    #[test]
    fn deepseek_tool_call_read_only() {
        let result = deepseek("tool_call_read_only");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed]);
        let rows = items(&result, main_id(&result));
        let read = rows
            .iter()
            .find(|item| item.title.as_deref() == Some("README.md"))
            .expect("the read row, titled by the Host's view");
        assert_eq!(read.status, ItemStatus::Completed);
        assert!(main.runtime_requests.is_empty());
    }

    #[test]
    fn deepseek_tool_call_approval_passes_outcomes_through() {
        let result = deepseek("tool_call_approval");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed]);
        let request = main.runtime_requests.values().next().unwrap();
        assert_eq!(request.kind, RuntimeRequestKind::CommandApproval);
        assert_eq!(request.status, RequestStatus::Resolved);
        assert_eq!(
            request
                .options
                .iter()
                .map(|option| option.id.as_str())
                .collect::<Vec<_>>(),
            ["allowed-once", "rejected"]
        );
        let rows = items(&result, main_id(&result));
        assert!(rows.iter().any(|item| matches!(
            &item.kind,
            TurnItemKind::CommandExecution { input, .. } if input == "ls"
        ) && item.status == ItemStatus::Completed));
    }

    #[test]
    fn deepseek_plan_questions() {
        let result = deepseek("plan_questions");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed]);
        let plan = main.plans.values().next().unwrap();
        assert!(matches!(&plan.kind, PlanKind::TodoList { steps, .. } if steps.len() == 2));
        let request = main.runtime_requests.values().next().unwrap();
        assert_eq!(request.kind, RuntimeRequestKind::UserInput);
        assert_eq!(request.status, RequestStatus::Resolved);
        assert_eq!(
            assistant_texts(&result, main_id(&result)),
            ["You chose Blue."]
        );
    }

    #[test]
    fn deepseek_turn_interrupt() {
        let result = deepseek("turn_interrupt");
        assert_eq!(statuses(&result.projection), [RunStatus::Interrupted]);
        assert_eq!(terminals(&result), [TurnOutcome::Interrupted]);
    }

    #[test]
    fn deepseek_turn_interrupt_mid_tool() {
        let result = deepseek("turn_interrupt_mid_tool");
        assert_eq!(statuses(&result.projection), [RunStatus::Interrupted]);
        assert_eq!(terminals(&result), [TurnOutcome::Interrupted]);
        let rows = items(&result, main_id(&result));
        assert!(rows.iter().any(|item| matches!(
            &item.kind,
            TurnItemKind::CommandExecution { input, .. } if input == "sleep 600"
        ) && item.status.is_terminal()));
    }

    #[test]
    fn deepseek_message_steering_is_native() {
        let result = deepseek("message_steering");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed]);
        let run = runs(main)[0];
        assert_eq!(
            main.attempts
                .values()
                .filter(|attempt| attempt.run_id == run.id)
                .count(),
            1
        );
        assert_eq!(
            assistant_texts(&result, main_id(&result)),
            ["Working", "Steered: use tabs"]
        );
    }

    #[test]
    fn deepseek_provider_thread_resume() {
        let result = deepseek("provider_thread_resume");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed, RunStatus::Completed]);
        assert_eq!(main.provider_sessions.len(), 2);
        let all = runs(main);
        assert_eq!(all[0].provider_thread_id, all[1].provider_thread_id);
    }

    #[test]
    fn deepseek_rollback_forks_at_the_kept_turn() {
        let result = deepseek("thread_rollback");
        let main = &result.projection;
        assert_eq!(
            statuses(main),
            [
                RunStatus::Completed,
                RunStatus::RolledBack,
                RunStatus::Completed
            ]
        );
        let all = runs(main);
        assert_ne!(all[0].provider_thread_id, all[2].provider_thread_id);
        assert_eq!(native_thread(main, all[2]).as_deref(), Some(DEEPSEEK_FORK));
    }

    #[test]
    fn deepseek_fork_is_native() {
        let result = deepseek("thread_fork_native");
        assert!(matches!(result.forks[..], [ForkResult::Native { .. }]));
        let fork = &result.threads["fork-a"];
        assert_eq!(statuses(fork), [RunStatus::Completed]);
        assert_eq!(
            assistant_texts(&result, fork.thread.id),
            ["Echo: in the fork"]
        );
    }

    #[test]
    fn deepseek_usage_limit_takes_the_structured_reset_time() {
        let result = deepseek("usage_limit");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Failed]);
        let failure = runs(main)[0].failure.clone().unwrap();
        assert_eq!(failure.class, FailureClass::UsageLimit);
        assert_eq!(failure.reset_at, Some(1_767_229_200_000));
        assert_eq!(
            main.thread
                .limit_recovery
                .as_ref()
                .and_then(|recovery| recovery.reset_at),
            Some(1_767_229_200_000)
        );
    }

    #[test]
    fn deepseek_host_exit_fails_the_turn_and_loses_the_session() {
        let result = deepseek("runtime_exit_unexpected");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Failed]);
        let failure = runs(main)[0].failure.clone().unwrap();
        assert_eq!(failure.class, FailureClass::TransportError);
        assert_eq!(terminals(&result).len(), 1);
    }

    #[test]
    fn deepseek_background_jobs_outlive_their_turn() {
        let result = deepseek("background_work");
        assert_eq!(statuses(&result.projection), [RunStatus::Completed]);
        let work = result
            .adapter_events
            .iter()
            .filter_map(|event| match event {
                AdapterEvent::BackgroundWorkUpdated { work } => {
                    Some((work.task.task_id.clone(), work.live))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            work,
            [("job_1".to_owned(), true), ("job_1".to_owned(), false)]
        );
    }

    #[test]
    fn deepseek_subagent_jobs_are_subagents() {
        let result = deepseek("subagent");
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed]);
        let subagent = main.subagents.values().next().unwrap();
        assert_eq!(subagent.status, SubagentStatus::Completed);
        assert_eq!(terminals(&result), [TurnOutcome::Completed]);
    }
}
