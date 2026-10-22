use serde_json::json;
use waku_orchestrator::ManualClock;

use super::*;
use crate::driver::v2::SeqIdAllocator;

fn normalizer() -> Normalizer {
    Normalizer::new(Scope {
        ids: Arc::new(SeqIdAllocator::new()),
        clock: Arc::new(ManualClock::new(0)),
    })
}

fn run() -> RunRef {
    RunRef {
        thread_id: ThreadId(uuid::Uuid::from_u128(2)),
        run_id: RunId(uuid::Uuid::from_u128(3)),
        attempt_id: RunAttemptId(uuid::Uuid::from_u128(4)),
        root_node_id: NodeId(uuid::Uuid::from_u128(5)),
    }
}

fn thread() -> ProviderThreadRef {
    ProviderThreadRef {
        provider_thread_id: ProviderThreadId(uuid::Uuid::from_u128(6)),
        native: NativeRef::strong("T-abc"),
        cursor: None,
    }
}

fn events(normalizer: &mut Normalizer, frames: &[Value]) -> Vec<AdapterEvent> {
    frames
        .iter()
        .flat_map(|frame| normalizer.handle(frame).unwrap_or_default().events)
        .collect()
}

fn terminals(events: &[AdapterEvent]) -> Vec<TurnOutcome> {
    events
        .iter()
        .filter_map(|event| match event {
            AdapterEvent::TurnTerminal { terminal } => Some(terminal.outcome.clone()),
            _ => None,
        })
        .collect()
}

/// Payloads copied from a live `--stream-json-input` session (the v1
/// driver's test).
#[test]
fn streams_thinking_text_and_tools_then_settles_on_end_turn() {
    let mut normalizer = normalizer();
    let (_, started) = normalizer.begin_turn(run(), &thread(), 1);
    assert!(matches!(
        started[..],
        [AdapterEvent::ProviderTurnUpdated { ref turn }] if turn.status == ProviderTurnStatus::Running
    ));
    let seen = events(
        &mut normalizer,
        &[
            json!({"type":"system","subtype":"init","session_id":"T-abc","tools":[]}),
            json!({"type":"assistant","message":{"content":[
                {"type":"thinking","thinking":"pondering"},
                {"type":"tool_use","id":"toolu_1","name":"Bash","input":{"cmd":"ls"}}
            ],"stop_reason":"tool_use"}}),
            json!({"type":"user","message":{"content":[
                {"type":"tool_result","tool_use_id":"toolu_1","content":"a.txt","is_error":false}
            ]}}),
            json!({"type":"assistant","message":{"content":[
                {"type":"text","text":"BANANA."}
            ],"stop_reason":"end_turn"}}),
        ],
    );
    assert_eq!(terminals(&seen), [TurnOutcome::Completed]);
    let commands = seen
        .iter()
        .filter_map(|event| match event {
            AdapterEvent::TurnItemUpdated { item } => match &item.kind {
                TurnItemKind::CommandExecution { input, output, .. } => {
                    Some((input.clone(), output.clone(), item.status))
                }
                _ => None,
            },
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        commands,
        [
            ("ls".to_owned(), None, ItemStatus::Running),
            (
                "ls".to_owned(),
                Some("a.txt".to_owned()),
                ItemStatus::Completed
            ),
        ]
    );
    assert!(seen.iter().any(|event| matches!(
        event,
        AdapterEvent::MessageUpdated { message } if message.text == "BANANA." && !message.streaming
    )));
    assert!(seen.iter().any(|event| matches!(
        event,
        AdapterEvent::TurnItemUpdated { item }
            if matches!(&item.kind, TurnItemKind::Reasoning { text, .. } if text == "pondering")
    )));
    assert!(normalizer.turn.is_none());
}

#[test]
fn a_tool_using_message_does_not_end_the_turn() {
    let mut normalizer = normalizer();
    normalizer.begin_turn(run(), &thread(), 1);
    let seen = events(
        &mut normalizer,
        &[json!({"type":"assistant","message":{"content":[],"stop_reason":"tool_use"}})],
    );
    assert!(terminals(&seen).is_empty());
    assert!(normalizer.turn.is_some());
}

#[test]
fn subagent_frames_never_reach_the_parent() {
    let mut normalizer = normalizer();
    normalizer.begin_turn(run(), &thread(), 1);
    let seen = events(
        &mut normalizer,
        &[
            json!({"type":"assistant","parent_tool_use_id":"toolu_9","message":{"content":[
            {"type":"text","text":"inner"}
        ],"stop_reason":"end_turn"}}),
        ],
    );
    assert!(seen.is_empty());
    assert!(normalizer.turn.is_some());
}

#[test]
fn an_error_result_fails_the_turn_and_an_interrupt_wins() {
    let mut normalizer = normalizer();
    normalizer.begin_turn(run(), &thread(), 1);
    let seen = events(
        &mut normalizer,
        &[
            json!({"type":"result","subtype":"error_during_execution","is_error":true,"error":"Out of credits","api_error_status":429,"resetsAt":1767240000}),
        ],
    );
    let outcomes = terminals(&seen);
    let [TurnOutcome::Failed { failure }] = &outcomes[..] else {
        panic!("{outcomes:?}")
    };
    assert_eq!(failure.class, FailureClass::UsageLimit);
    assert_eq!(failure.reset_at, Some(1_767_240_000_000));

    let mut normalizer = self::normalizer();
    normalizer.begin_turn(run(), &thread(), 1);
    assert!(normalizer.mark_interrupt(run().attempt_id));
    let seen = events(
        &mut normalizer,
        &[
            json!({"type":"result","subtype":"error_during_execution","is_error":true,"error":"interrupted"}),
        ],
    );
    assert_eq!(terminals(&seen), [TurnOutcome::Interrupted]);
}
