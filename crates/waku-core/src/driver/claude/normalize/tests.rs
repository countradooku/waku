//! The normalizer's paths without a process: exactly one terminal per
//! accepted turn, automatic approvals, wakes and the frames that must stay
//! out of the parent transcript. Shapes are the live ones the v1 driver's
//! tests quoted (CLI 2.1.223–2.1.258).

use serde_json::json;
use waku_orchestrator::ManualClock;

use super::*;
use crate::driver::v2::SeqIdAllocator;

fn normalizer(auto_approve: bool) -> Normalizer {
    Normalizer::new(
        Scope {
            session_id: ProviderSessionId(uuid::Uuid::from_u128(1)),
            thread_id: ThreadId(uuid::Uuid::from_u128(2)),
            instance: ProviderInstanceId::new("claude"),
            ids: Arc::new(SeqIdAllocator::new()),
            clock: Arc::new(ManualClock::new(0)),
        },
        auto_approve,
    )
}

fn thread() -> ProviderThreadRef {
    ProviderThreadRef {
        provider_thread_id: ProviderThreadId(uuid::Uuid::from_u128(3)),
        native: NativeRef::strong("session"),
        cursor: None,
    }
}

fn run(n: u128) -> RunRef {
    RunRef {
        thread_id: ThreadId(uuid::Uuid::from_u128(2)),
        run_id: RunId(uuid::Uuid::from_u128(10 + n)),
        attempt_id: RunAttemptId(uuid::Uuid::from_u128(20 + n)),
        root_node_id: NodeId(uuid::Uuid::from_u128(30 + n)),
    }
}

/// A normalizer with one turn started and echoed.
fn running(auto_approve: bool) -> Normalizer {
    let mut normalizer = normalizer(auto_approve);
    normalizer.reset_thread(thread());
    normalizer.begin_turn(run(1), &thread(), 1);
    let echo = normalizer
        .handle(&json!({"type":"user","isReplay":true,"message":{"role":"user","content":[{"type":"text","text":"go"}]}}))
        .unwrap();
    assert!(echo.echoed);
    normalizer
}

fn feed(normalizer: &mut Normalizer, frames: &[Value]) -> Output {
    let mut all = Output::default();
    for frame in frames {
        if let Some(output) = normalizer.handle(frame) {
            all.events.extend(output.events);
            all.writes.extend(output.writes);
            all.echoed |= output.echoed;
            all.turn_ended |= output.turn_ended;
        }
    }
    all.events.extend(normalizer.flush());
    all
}

fn terminals(events: &[AdapterEvent]) -> Vec<TurnTerminal> {
    events
        .iter()
        .filter_map(|event| match event {
            AdapterEvent::TurnTerminal { terminal } => Some(terminal.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn the_result_settles_the_turn_exactly_once() {
    let mut normalizer = running(true);
    let result = json!({"type":"result","subtype":"success","is_error":false});
    let first = feed(&mut normalizer, std::slice::from_ref(&result));
    assert_eq!(terminals(&first.events).len(), 1);
    assert!(first.turn_ended);
    let second = feed(&mut normalizer, &[result]);
    assert!(terminals(&second.events).is_empty());
}

#[test]
fn an_error_result_fails_and_an_interrupted_one_is_interrupted() {
    let mut normalizer = running(true);
    let output = feed(
        &mut normalizer,
        &[
            json!({"type":"result","subtype":"error_max_turns","is_error":true,"errors":["max turns reached"]}),
        ],
    );
    assert!(matches!(
        &terminals(&output.events)[0].outcome,
        TurnOutcome::Failed { failure } if failure.class == FailureClass::Unknown
    ));

    let mut normalizer = running(true);
    assert!(normalizer.mark_interrupt(run(1).attempt_id));
    let output = feed(
        &mut normalizer,
        &[json!({"type":"result","subtype":"success","is_error":false})],
    );
    assert_eq!(
        terminals(&output.events)[0].outcome,
        TurnOutcome::Interrupted
    );
}

#[test]
fn an_interrupt_before_the_echo_still_gets_its_one_terminal() {
    let mut normalizer = normalizer(true);
    normalizer.reset_thread(thread());
    normalizer.begin_turn(run(1), &thread(), 1);
    assert!(normalizer.mark_interrupt(run(1).attempt_id));
    let output = feed(
        &mut normalizer,
        &[
            json!({"type":"result","subtype":"error_during_execution","is_error":true,"errors":["Request interrupted by user"]}),
        ],
    );
    let terminals = terminals(&output.events);
    assert_eq!(terminals.len(), 1);
    assert_eq!(terminals[0].outcome, TurnOutcome::Interrupted);
    assert!(output.turn_ended, "start_turn's waiter is released");
}

#[test]
fn a_turn_the_cli_never_took_has_no_terminal() {
    let mut normalizer = normalizer(true);
    normalizer.reset_thread(thread());
    normalizer.begin_turn(run(1), &thread(), 1);
    normalizer.abandon_turn(run(1).attempt_id);
    assert!(normalizer.turn.is_none());
    let output = feed(
        &mut normalizer,
        &[json!({"type":"result","subtype":"success","is_error":false})],
    );
    assert!(terminals(&output.events).is_empty());
}

#[test]
fn supervised_mode_asks_and_auto_modes_answer_themselves() {
    let request = json!({
        "type": "control_request",
        "request_id": "fa01120e",
        "request": {
            "subtype": "can_use_tool",
            "tool_name": "Bash",
            "input": {"command": "echo hi"},
            "description": "Write probe file",
            "tool_use_id": "toolu_1"
        }
    });
    let mut supervised = running(false);
    let output = feed(&mut supervised, std::slice::from_ref(&request));
    assert!(output.writes.is_empty());
    assert!(output.events.iter().any(|event| matches!(
        event,
        AdapterEvent::RuntimeRequestUpdated { request } if request.kind == RuntimeRequestKind::CommandApproval
    )));
    let (reply, _) = supervised
        .respond(
            "fa01120e",
            &RuntimeReply::Decision {
                option_id: DENY.to_owned(),
            },
        )
        .unwrap();
    assert_eq!(reply["response"]["response"]["behavior"], "deny");

    let mut automatic = running(true);
    let output = feed(&mut automatic, &[request]);
    assert_eq!(
        output.writes,
        [
            json!({"type":"control_response","response":{"subtype":"success","request_id":"fa01120e","response":{"behavior":"allow"}}})
        ]
    );
    assert!(output.events.is_empty());
}

#[test]
fn ask_user_question_is_never_answered_automatically() {
    let mut normalizer = running(true);
    let output = feed(
        &mut normalizer,
        &[json!({
            "type": "control_request",
            "request_id": "ask-1",
            "request": {
                "subtype": "can_use_tool",
                "tool_name": "AskUserQuestion",
                "input": {"questions": [{
                    "header": "Environment",
                    "question": "Where should this deploy?",
                    "options": [{"label": "Preview", "description": "A preview deployment"}],
                    "multiSelect": false
                }]}
            }
        })],
    );
    assert!(output.writes.is_empty());
    let request = output
        .events
        .iter()
        .find_map(|event| match event {
            AdapterEvent::RuntimeRequestUpdated { request } => Some(request.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(request.kind, RuntimeRequestKind::UserInput);
    assert_eq!(request.questions[0].id, "Where should this deploy?");
}

#[test]
fn subagent_frames_stay_out_of_the_parent_transcript() {
    let mut normalizer = running(true);
    let output = feed(
        &mut normalizer,
        &[
            json!({"type":"stream_event","parent_tool_use_id":"toolu-agent","event":{"type":"message_start","message":{"id":"msg_sub"}}}),
            json!({"type":"stream_event","parent_tool_use_id":"toolu-agent","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"sub text"}}}),
            json!({"type":"assistant","parent_tool_use_id":"toolu-agent","message":{"id":"msg_sub","content":[{"type":"text","text":"sub text"}],"usage":{"input_tokens":999999}}}),
        ],
    );
    assert!(output.events.is_empty(), "{:?}", output.events);
}

#[test]
fn subagent_output_between_turns_does_not_open_a_turn() {
    let mut normalizer = running(true);
    feed(
        &mut normalizer,
        &[json!({"type":"result","subtype":"success","is_error":false})],
    );
    let output = feed(
        &mut normalizer,
        &[
            json!({"type":"assistant","parent_tool_use_id":"toolu_agent","message":{"content":[{"type":"text","text":"still going"}]}}),
        ],
    );
    assert!(output.events.is_empty());
    assert!(normalizer.turn.is_none());
}

#[test]
fn a_wake_after_detached_work_continues_the_last_run() {
    let mut normalizer = running(true);
    let parked = feed(
        &mut normalizer,
        &[
            json!({"type":"system","subtype":"background_tasks_changed","tasks":[{"task_id":"b1","task_type":"local_bash","description":"sleep"}]}),
            json!({"type":"result","subtype":"success","is_error":false}),
        ],
    );
    assert!(terminals(&parked.events)[0].background_pending);
    assert!(normalizer.has_live_tasks());
    let woken = feed(
        &mut normalizer,
        &[
            json!({"type":"system","subtype":"background_tasks_changed","tasks":[]}),
            json!({"type":"system","subtype":"init","model":"claude-opus-5"}),
            json!({"type":"stream_event","event":{"type":"message_start","message":{"id":"msg_wake"}}}),
            json!({"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"It finished."}}}),
            json!({"type":"result","subtype":"success","is_error":false}),
        ],
    );
    let terminals = terminals(&woken.events);
    assert_eq!(terminals.len(), 1);
    assert_eq!(terminals[0].run, run(1));
    assert_eq!(terminals[0].provider_turn.as_ref().unwrap().ordinal, 2);
    assert!(!terminals[0].background_pending);
    assert!(!normalizer.has_live_tasks());
}

#[test]
fn ambient_housekeeping_tasks_stay_out_of_background_work() {
    let mut normalizer = running(true);
    let output = feed(
        &mut normalizer,
        &[
            json!({"type":"system","subtype":"task_started","task_id":"w","task_type":"local_bash","is_backgrounded":true,"ambient":true}),
            json!({"type":"system","subtype":"background_tasks_changed","tasks":[
                {"task_id":"w","task_type":"local_bash","ambient":true},
                {"task_id":"b1","task_type":"local_bash","description":"build"}]}),
        ],
    );
    let work = output
        .events
        .iter()
        .filter_map(|event| match event {
            AdapterEvent::BackgroundWorkUpdated { work } => Some(work.task.task_id.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(work, ["b1"]);
}

#[test]
fn edits_carry_the_hunks_claude_applied() {
    let mut normalizer = running(true);
    let output = feed(
        &mut normalizer,
        &[
            json!({"type":"assistant","uuid":"a1","message":{"id":"msg_1","content":[
                {"type":"tool_use","id":"toolu_edit","name":"Edit","input":{"file_path":"/w/src/a.rs","old_string":"fn old() {}","new_string":"fn new() {}"}}]}}),
            json!({"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"toolu_edit","content":"The file has been updated."}]},
                   "tool_use_result":{"filePath":"/w/src/a.rs","structuredPatch":[{"oldStart":1,"oldLines":1,"newStart":1,"newLines":1,"lines":["-fn old() {}","+fn new() {}"]}]}}),
        ],
    );
    let diff = output
        .events
        .iter()
        .find_map(|event| match event {
            AdapterEvent::TurnItemUpdated { item } if item.status == ItemStatus::Completed => {
                match &item.kind {
                    TurnItemKind::FileChange { diff, .. } => diff.clone(),
                    _ => None,
                }
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(diff, "@@ -1,1 +1,1 @@\n-fn old() {}\n+fn new() {}\n");
}

#[test]
fn rate_limit_evidence_classifies_the_failed_result() {
    let mut normalizer = running(true);
    let output = feed(
        &mut normalizer,
        &[
            json!({"type":"rate_limit_event","rate_limit_info":{"status":"rejected","resetsAt":1000,"rateLimitType":"seven_day"}}),
            json!({"type":"result","subtype":"success","is_error":true,"result":"limit"}),
        ],
    );
    assert!(matches!(
        &terminals(&output.events)[0].outcome,
        TurnOutcome::Failed { failure }
            if failure.class == FailureClass::UsageLimit && failure.reset_at == Some(1_000_000)
    ));
}
