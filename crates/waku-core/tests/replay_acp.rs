//! Replay tests for the ACP family's `DriverV2` (#48): every
//! `fixtures/replay/<scenario>/<flavor>.ndjson` for `acp` (a generic ACP
//! agent), `grok`, `cursor`, `kimi` and `fx` runs through the real
//! orchestrator, projections, effect workers and `ProviderSessionManager`;
//! only the agent's stdio is replayed. Sources: `fixtures/replay/ACP_FIXTURES.md`.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use waku_core::driver::acp::{AcpDriver, Flavor, instance_capabilities};
use waku_core::driver::v2::manager::ForkResult;
use waku_core::driver::v2::replay::{
    ReplayHarness, ReplayProfile, ScenarioInput, ScenarioResult, read_transcript,
};
use waku_core::driver::v2::{AdapterEvent, InstanceConfig, TurnOutcome};
use waku_orchestrator::ThreadProjection;
use waku_protocol::orchestration::*;

const PROVIDERS: [&str; 5] = ["acp", "grok", "cursor", "kimi", "fx"];
const GROK_SESSION: &str = "00000000-0000-4000-8000-000000000001";
const FORK_SESSION: &str = "00000000-0000-4000-8000-000000000002";

/// Every ACP scenario with assertions below; the R rows of RFC 0002's
/// "Required scenarios" plus the S rows the flavors declare.
const COVERED: [&str; 13] = [
    "simple",
    "multi_turn",
    "tool_call_read_only",
    "tool_call_approval",
    "plan_questions",
    "turn_interrupt",
    "turn_interrupt_mid_tool",
    "steering_restart",
    "provider_thread_resume",
    "usage_limit",
    "runtime_exit_unexpected",
    "thread_rollback",
    "thread_fork_native",
];

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/replay")
}

/// `input.<provider>.json` when a scenario needs the agent's own ids or
/// Supervised mode, else the shared `input.json`.
fn input(scenario: &str, provider: &str) -> ScenarioInput {
    let dir = fixtures().join(scenario);
    let path = [format!("input.{provider}.json"), "input.json".to_owned()]
        .into_iter()
        .map(|name| dir.join(name))
        .find(|path| path.exists())
        .unwrap_or_else(|| panic!("{scenario} has no input"));
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn instance(provider: &str) -> InstanceConfig {
    let flavor = Flavor::from_id(provider).unwrap();
    let mut cfg = InstanceConfig::new(provider, flavor.driver_kind());
    cfg.binary = match flavor {
        Flavor::Generic => "acp-agent",
        Flavor::Cursor => "cursor-agent",
        Flavor::Grok => "grok",
        Flavor::Kimi => "kimi",
        Flavor::Fx => "fx",
    }
    .into();
    cfg
}

fn has_fixture(scenario: &str, provider: &str) -> bool {
    fixtures()
        .join(scenario)
        .join(format!("{provider}.ndjson"))
        .exists()
}

fn replay(scenario: &str, provider: &str) -> ScenarioResult {
    let path = fixtures().join(scenario).join(format!("{provider}.ndjson"));
    let transcript = read_transcript(&path).unwrap();
    let mut harness = ReplayHarness::replay(
        Arc::new(AcpDriver::new()),
        instance(provider),
        transcript,
        Some(fixtures().join(scenario)),
        ReplayProfile::new("acp.stdio"),
    )
    .unwrap_or_else(|error| panic!("{scenario}/{provider}: {error}"));
    harness
        .run(&input(scenario, provider))
        .unwrap_or_else(|error| panic!("{scenario}/{provider}: {error}"))
}

/// Runs `check` for every flavor that has a fixture of `scenario`.
fn each(scenario: &str, check: impl Fn(&str, ScenarioResult)) {
    let mut ran = 0;
    for provider in PROVIDERS {
        if has_fixture(scenario, provider) {
            check(provider, replay(scenario, provider));
            ran += 1;
        }
    }
    assert!(ran > 0, "{scenario} has no ACP fixture");
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

fn native_thread(projection: &ThreadProjection, id: ProviderThreadId) -> Option<String> {
    projection.provider_threads[&id]
        .native_thread_ref
        .as_ref()
        .and_then(|reference| reference.native_id.clone())
}

fn session_id(provider: &str) -> &'static str {
    if provider == "grok" {
        GROK_SESSION
    } else {
        "acp-session-1"
    }
}

// -- scenarios -------------------------------------------------------------------

#[test]
fn simple() {
    each("simple", |provider, result| {
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed], "{provider}");
        assert_eq!(terminals(&result), [TurnOutcome::Completed], "{provider}");
        let texts = assistant_texts(&result, main_id(&result));
        assert_eq!(
            texts.last().map(String::as_str),
            Some("fixture simple ok"),
            "{provider}"
        );
        let run = runs(main)[0];
        let thread = run.provider_thread_id.unwrap();
        assert_eq!(
            native_thread(main, thread).as_deref(),
            Some(session_id(provider))
        );
        assert_eq!(
            main.provider_threads[&thread]
                .native_thread_ref
                .as_ref()
                .unwrap()
                .strength,
            RefStrength::Strong
        );
        // The session's capabilities were negotiated: every recorded agent
        // advertises `loadSession`, so they equal the instance's claim.
        let session = main.provider_sessions.values().next().unwrap();
        assert_eq!(
            session.capabilities,
            instance_capabilities(&instance(provider)),
            "{provider}"
        );
        assert_eq!(session.status, SessionStatus::Stopped);
        // Text was persisted final, never streaming; Grok also thought.
        let rows = items(&result, main_id(&result));
        assert!(rows.iter().any(|item| matches!(
            &item.kind,
            TurnItemKind::AssistantMessage {
                streaming: false,
                ..
            }
        ) && item.status == ItemStatus::Completed));
        if provider == "grok" {
            assert!(rows.iter().any(|item| matches!(
                &item.kind,
                TurnItemKind::Reasoning { text, streaming: false } if text.starts_with("The user wants")
            )));
        }
    });
}

#[test]
fn multi_turn() {
    each("multi_turn", |provider, result| {
        let main = &result.projection;
        assert_eq!(
            statuses(main),
            [RunStatus::Completed, RunStatus::Completed],
            "{provider}"
        );
        assert_eq!(main.provider_sessions.len(), 1, "{provider}: one session");
        let all = runs(main);
        assert_eq!(all[0].provider_thread_id, all[1].provider_thread_id);
        let turns = main
            .provider_turns
            .values()
            .map(|turn| turn.ordinal)
            .collect::<BTreeSet<_>>();
        assert_eq!(turns, BTreeSet::from([1, 2]), "{provider}");
        assert_eq!(
            assistant_texts(&result, main_id(&result)).len(),
            2,
            "{provider}"
        );
    });
}

#[test]
fn tool_call_read_only() {
    each("tool_call_read_only", |provider, result| {
        assert_eq!(
            statuses(&result.projection),
            [RunStatus::Completed],
            "{provider}"
        );
        let rows = items(&result, main_id(&result));
        let reads = rows
            .iter()
            .filter(|item| {
                matches!(&item.kind, TurnItemKind::DynamicTool { tool_name: Some(name), output: Some(_), .. } if name == "read")
                    && item.status == ItemStatus::Completed
            })
            .count();
        assert_eq!(reads, 2, "{provider}");
        assert!(
            result
                .projection
                .nodes
                .values()
                .filter(|node| node.kind == NodeKind::ToolCall)
                .all(|node| node.status == NodeStatus::Completed)
        );
        assert_eq!(
            assistant_texts(&result, main_id(&result)),
            [
                "Reading both files now.\n",
                "read only tool fixture complete"
            ],
            "{provider}"
        );
    });
}

#[test]
fn tool_call_approval() {
    each("tool_call_approval", |provider, result| {
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed], "{provider}");
        let request = main.runtime_requests.values().next().unwrap();
        // An `edit` tool call asks for a file-change approval.
        assert_eq!(request.kind, RuntimeRequestKind::FileChangeApproval);
        assert_eq!(request.status, RequestStatus::Resolved);
        // The agent's own option ids survive normalization.
        assert_eq!(
            request
                .options
                .iter()
                .map(|option| (option.id.as_str(), option.allow))
                .collect::<Vec<_>>(),
            [
                ("allow-edits-session", true),
                ("allow-once", true),
                ("reject-once", false)
            ]
        );
        assert_eq!(
            request.response,
            Some(RuntimeResponse::Approval {
                option_id: "allow-once".into()
            })
        );
        let rows = items(&result, main_id(&result));
        assert!(rows.iter().any(|item| matches!(
            &item.kind,
            TurnItemKind::ApprovalRequest { prompt: Some(prompt), .. }
                if prompt.starts_with("Write `")
        )));
    });
}

#[test]
fn plan_questions() {
    each("plan_questions", |provider, result| {
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed], "{provider}");
        let request = main.runtime_requests.values().next().unwrap();
        assert_eq!(request.kind, RuntimeRequestKind::UserInput);
        assert_eq!(request.status, RequestStatus::Resolved);
        assert_eq!(request.questions[0].id, "schema_vs_ui_flexibility");
        assert_eq!(
            request.questions[0].options[0].label,
            "Strict schemas (Recommended)"
        );
        assert_eq!(
            assistant_texts(&result, main_id(&result))
                .last()
                .map(String::as_str),
            Some("plan questions fixture complete")
        );
        if provider == "cursor" {
            let plan = main.plans.values().next().unwrap();
            assert_eq!(plan.status, PlanStatus::Completed);
            assert!(matches!(&plan.kind, PlanKind::TodoList { steps, .. } if steps.len() == 2));
        }
    });
}

#[test]
fn turn_interrupt() {
    each("turn_interrupt", |provider, result| {
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Interrupted], "{provider}");
        assert_eq!(terminals(&result), [TurnOutcome::Interrupted], "{provider}");
        let turn = main.provider_turns.values().next().unwrap();
        assert_eq!(turn.status, ProviderTurnStatus::Interrupted);
        assert!(runs(main)[0].checkpoint_id.is_none());
    });
}

#[test]
fn turn_interrupt_mid_tool() {
    each("turn_interrupt_mid_tool", |provider, result| {
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Interrupted], "{provider}");
        assert_eq!(terminals(&result), [TurnOutcome::Interrupted], "{provider}");
        let rows = items(&result, main_id(&result));
        assert!(rows.iter().any(|item| matches!(
            &item.kind,
            TurnItemKind::CommandExecution { input, output_indicates_failure: true, .. } if input == "sleep 30"
        ) && item.status == ItemStatus::Failed), "{provider}");
    });
}

#[test]
fn steering_restarts_the_turn() {
    each("steering_restart", |provider, result| {
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Completed], "{provider}");
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
            ],
            "{provider}"
        );
        assert_eq!(
            terminals(&result),
            [TurnOutcome::Interrupted, TurnOutcome::Completed],
            "{provider}"
        );
        assert_eq!(
            assistant_texts(&result, main_id(&result))
                .last()
                .map(String::as_str),
            Some("steering fixture observed")
        );
    });
}

#[test]
fn unloaded_threads_resume_natively() {
    each("provider_thread_resume", |provider, result| {
        let main = &result.projection;
        assert_eq!(
            statuses(main),
            [RunStatus::Completed, RunStatus::Completed],
            "{provider}"
        );
        assert_eq!(main.provider_sessions.len(), 2, "{provider}");
        let all = runs(main);
        assert_eq!(all[0].provider_thread_id, all[1].provider_thread_id);
        // `session/load` replays history; none of it becomes a live message.
        assert_eq!(
            assistant_texts(&result, main_id(&result)),
            ["first answer", "second answer"],
            "{provider}"
        );
    });
}

#[test]
fn usage_limit_is_classified() {
    each("usage_limit", |provider, result| {
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Failed], "{provider}");
        let failure = runs(main)[0].failure.clone().unwrap();
        assert_eq!(failure.class, FailureClass::UsageLimit, "{provider}");
        assert!(failure.message.contains("usage limit"), "{provider}");
        // Only the generic agent's error names a reset time; Grok's
        // `rate_limit` completion carries none (as in T3).
        let expected = (provider != "grok").then_some(1_767_243_600_000);
        assert_eq!(failure.reset_at, expected, "{provider}");
        assert!(main.thread.limit_recovery.is_some());
    });
}

#[test]
fn an_agent_crash_fails_the_turn() {
    each("runtime_exit_unexpected", |provider, result| {
        let main = &result.projection;
        assert_eq!(statuses(main), [RunStatus::Failed], "{provider}");
        let failure = runs(main)[0].failure.clone().unwrap();
        assert_eq!(failure.class, FailureClass::TransportError, "{provider}");
        assert_eq!(
            terminals(&result).len(),
            1,
            "{provider}: exactly one terminal"
        );
    });
}

#[test]
fn grok_rollback_replaces_the_native_session() {
    let result = replay("thread_rollback", "grok");
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
    let (old, new) = (
        all[0].provider_thread_id.unwrap(),
        all[2].provider_thread_id.unwrap(),
    );
    assert_ne!(old, new, "rollback replaced the provider thread");
    assert_eq!(native_thread(main, old).as_deref(), Some(GROK_SESSION));
    assert_eq!(native_thread(main, new).as_deref(), Some(FORK_SESSION));
    assert_eq!(
        main.provider_threads[&new]
            .forked_from
            .as_ref()
            .map(|from| from.provider_thread_id),
        Some(old)
    );
    assert_eq!(main.thread.active_provider_thread_id, Some(new));
    assert!(
        result
            .checkpoint_calls
            .iter()
            .any(|call| call.starts_with("restore refs/waku/t/"))
    );
}

#[test]
fn grok_fork_is_native() {
    let result = replay("thread_fork_native", "grok");
    assert!(matches!(result.forks[..], [ForkResult::Native { .. }]));
    let main = &result.projection;
    let fork = &result.threads["fork-a"];
    let forked = fork.thread.active_provider_thread_id.unwrap();
    assert_eq!(native_thread(fork, forked).as_deref(), Some(FORK_SESSION));
    assert_eq!(
        fork.provider_threads[&forked]
            .forked_from
            .as_ref()
            .map(|from| from.provider_thread_id),
        main.thread.active_provider_thread_id
    );
    assert_eq!(statuses(fork), [RunStatus::Completed]);
    assert_eq!(assistant_texts(&result, fork.thread.id), ["forked answer"]);
}

// -- coverage --------------------------------------------------------------------

/// RFC 0002 "Required scenarios": every R row for the generic agent and
/// Grok, and Grok's S rows; every ACP fixture on disk has assertions.
#[test]
fn every_acp_fixture_is_covered_and_required_rows_exist() {
    let required = [
        "simple",
        "multi_turn",
        "tool_call_read_only",
        "turn_interrupt",
        "turn_interrupt_mid_tool",
        "steering_restart",
        "provider_thread_resume",
        "usage_limit",
        "runtime_exit_unexpected",
        // S: approvals are declared by every flavor.
        "tool_call_approval",
    ];
    for provider in ["acp", "grok"] {
        for scenario in required {
            assert!(
                has_fixture(scenario, provider),
                "{scenario}/{provider}.ndjson is required"
            );
        }
    }
    for scenario in ["plan_questions", "thread_rollback", "thread_fork_native"] {
        assert!(has_fixture(scenario, "grok"), "{scenario}/grok.ndjson");
    }
    for entry in std::fs::read_dir(fixtures()).unwrap() {
        let entry = entry.unwrap();
        if !entry.path().is_dir() {
            continue;
        }
        let scenario = entry.file_name().to_string_lossy().into_owned();
        for provider in PROVIDERS {
            if has_fixture(&scenario, provider) {
                assert!(
                    COVERED.contains(&scenario.as_str()),
                    "{scenario}/{provider}.ndjson has no assertions"
                );
            }
        }
    }
}

#[test]
fn acp_replay_is_deterministic() {
    for scenario in ["simple", "steering_restart", "tool_call_approval"] {
        for provider in ["acp", "grok"] {
            let normalize = |result: &ScenarioResult| -> Vec<String> {
                // A Windows path's backslashes are escaped once per JSON
                // level; the most escaped spelling is replaced first.
                let mut spellings = vec![result.workspace.display().to_string()];
                for _ in 0..3 {
                    let escaped = serde_json::to_string(spellings.last().unwrap()).unwrap();
                    spellings.push(escaped[1..escaped.len() - 1].to_owned());
                }
                spellings.dedup();
                result
                    .events
                    .iter()
                    .map(|event| {
                        spellings
                            .iter()
                            .rev()
                            .fold(serde_json::to_string(event).unwrap(), |json, spelling| {
                                json.replace(spelling, "<workspace>")
                            })
                    })
                    .collect()
            };
            let first = normalize(&replay(scenario, provider));
            let second = normalize(&replay(scenario, provider));
            assert_eq!(first, second, "{scenario}/{provider}");
        }
    }
}
