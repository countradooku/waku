use serde_json::json;
use waku_orchestrator::ManualClock;

use super::*;
use crate::driver::v2::SeqIdAllocator;

fn normalizer(flavor: PiFlavor) -> Normalizer {
    Normalizer::new(Scope {
        flavor,
        session_id: ProviderSessionId(uuid::Uuid::from_u128(1)),
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
        native: NativeRef::strong("session-1"),
        cursor: None,
    }
}

fn feed(normalizer: &mut Normalizer, frames: &[Value]) -> Vec<Output> {
    frames
        .iter()
        .map(|frame| normalizer.handle(frame).unwrap_or_default())
        .collect()
}

fn terminals(outputs: &[Output]) -> Vec<TurnOutcome> {
    outputs
        .iter()
        .flat_map(|output| &output.events)
        .filter_map(|event| match event {
            AdapterEvent::TurnTerminal { terminal } => Some(terminal.outcome.clone()),
            _ => None,
        })
        .collect()
}

fn texts(outputs: &[Output]) -> Vec<String> {
    outputs
        .iter()
        .flat_map(|output| &output.events)
        .filter_map(|event| match event {
            AdapterEvent::MessageUpdated { message } if !message.streaming => {
                Some(message.text.clone())
            }
            _ => None,
        })
        .collect()
}

#[test]
fn a_pi_turn_streams_and_settles_once_on_agent_settled() {
    let mut normalizer = normalizer(PiFlavor::Pi);
    normalizer.begin_turn(run(), &thread(), 1, "waku-3".into());
    let outputs = feed(
        &mut normalizer,
        &[
            json!({"type": "response", "id": "waku-3", "command": "prompt", "success": true}),
            json!({"type": "agent_start"}),
            json!({"type": "turn_start"}),
            json!({"type": "message_start", "message": {"role": "assistant", "content": []}}),
            json!({"type": "message_update", "assistantMessageEvent": {"type": "thinking_delta", "delta": "hm"}}),
            json!({"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "delta": "done"}}),
            json!({"type": "message_end", "message": {"role": "assistant", "content": [{"type": "text", "text": "done"}], "usage": {"totalTokens": 42}, "stopReason": "stop"}}),
            json!({"type": "agent_end", "messages": []}),
            json!({"type": "agent_settled"}),
        ],
    );
    assert_eq!(outputs[0].accepted, Some(Ok(())));
    assert_eq!(terminals(&outputs), [TurnOutcome::Completed]);
    assert_eq!(texts(&outputs), ["done"]);
    assert!(normalizer.turn.is_none());
}

#[test]
fn oh_my_pi_settles_on_the_terminal_agent_end_only() {
    let mut normalizer = normalizer(PiFlavor::OhMyPi);
    normalizer.begin_turn(run(), &thread(), 1, "waku-3".into());
    let outputs = feed(
        &mut normalizer,
        &[
            json!({"type": "agent_start"}),
            json!({"type": "agent_end", "isTerminal": false}),
            json!({"type": "agent_settled"}),
            json!({"type": "agent_end", "messages": []}),
        ],
    );
    // Agent activity accepts the prompt before its response arrives.
    assert_eq!(outputs[0].accepted, Some(Ok(())));
    assert_eq!(terminals(&outputs), [TurnOutcome::Completed]);
    assert!(outputs[3].turn_ended);
}

#[test]
fn a_local_command_completes_with_its_response() {
    let mut normalizer = normalizer(PiFlavor::OhMyPi);
    normalizer.begin_turn(run(), &thread(), 1, "waku-3".into());
    let outputs = feed(
        &mut normalizer,
        &[
            json!({"type": "response", "id": "waku-3", "command": "prompt", "success": true}),
            json!({"type": "command_output", "text": "Available commands\n"}),
            json!({"type": "command_output", "text": "/compact"}),
            json!({"type": "response", "id": "waku-3", "command": "prompt", "success": true, "data": {"agentInvoked": false}}),
        ],
    );
    assert_eq!(terminals(&outputs), [TurnOutcome::Completed]);
    assert_eq!(texts(&outputs), ["Available commands\n\n/compact"]);
}

#[test]
fn a_refused_prompt_fails_start_turn_without_a_terminal() {
    let mut normalizer = normalizer(PiFlavor::Pi);
    normalizer.begin_turn(run(), &thread(), 1, "waku-3".into());
    let outputs = feed(
        &mut normalizer,
        &[json!({"type": "response", "id": "waku-3", "success": false, "error": "no model"})],
    );
    assert_eq!(outputs[0].accepted, Some(Err("no model".to_owned())));
    assert!(terminals(&outputs).is_empty());
    assert!(normalizer.turn.is_none());
}

#[test]
fn an_error_after_acceptance_fails_the_turn_once() {
    let mut normalizer = normalizer(PiFlavor::Pi);
    normalizer.begin_turn(run(), &thread(), 1, "waku-3".into());
    let outputs = feed(
        &mut normalizer,
        &[
            json!({"type": "agent_start"}),
            json!({"type": "message_start", "message": {"role": "assistant", "content": []}}),
            json!({"type": "message_end", "message": {"role": "assistant", "content": [], "stopReason": "error", "errorMessage": "boom"}}),
            json!({"type": "agent_settled"}),
            json!({"type": "agent_settled"}),
        ],
    );
    let outcomes = terminals(&outputs);
    assert_eq!(outcomes.len(), 1);
    assert!(matches!(
        &outcomes[0],
        TurnOutcome::Failed { failure } if failure.message == "boom" && failure.class == FailureClass::Unknown
    ));
}

#[test]
fn a_usage_limit_takes_the_reset_from_the_retry_wait() {
    let mut normalizer = normalizer(PiFlavor::OhMyPi);
    normalizer.begin_turn(run(), &thread(), 1, "waku-3".into());
    let outputs = feed(
        &mut normalizer,
        &[
            json!({"type": "agent_start"}),
            json!({"type": "auto_retry_start", "attempt": 1, "maxAttempts": 1, "delayMs": 3_600_000, "errorMessage": "limit", "errorId": 0x0008_1000}),
            json!({"type": "auto_retry_end", "success": false, "attempt": 1, "finalError": "Usage limit reached"}),
            json!({"type": "agent_end"}),
        ],
    );
    let outcomes = terminals(&outputs);
    let TurnOutcome::Failed { failure } = &outcomes[0] else {
        panic!("{outcomes:?}")
    };
    assert_eq!(failure.class, FailureClass::UsageLimit);
    assert_eq!(failure.reset_at, Some(3_600_000));
}

#[test]
fn a_recovered_retry_completes_the_turn() {
    let mut normalizer = normalizer(PiFlavor::Pi);
    normalizer.begin_turn(run(), &thread(), 1, "waku-3".into());
    let outputs = feed(
        &mut normalizer,
        &[
            json!({"type": "agent_start"}),
            json!({"type": "message_end", "message": {"role": "assistant", "content": [], "stopReason": "error", "errorMessage": "overloaded"}}),
            json!({"type": "auto_retry_end", "success": true, "attempt": 1}),
            json!({"type": "agent_settled"}),
        ],
    );
    assert_eq!(terminals(&outputs), [TurnOutcome::Completed]);
}

#[test]
fn an_interrupted_tool_closes_as_interrupted() {
    let mut normalizer = normalizer(PiFlavor::Pi);
    normalizer.begin_turn(run(), &thread(), 1, "waku-3".into());
    let mut outputs = feed(
        &mut normalizer,
        &[
            json!({"type": "agent_start"}),
            json!({"type": "tool_execution_start", "toolCallId": "call-1", "toolName": "bash", "args": {"command": "sleep 30"}}),
        ],
    );
    assert!(normalizer.mark_interrupt(run().attempt_id));
    outputs.extend(feed(
        &mut normalizer,
        &[
            json!({"type": "message_end", "message": {"role": "assistant", "content": [], "stopReason": "error", "errorMessage": "This operation was aborted"}}),
            json!({"type": "agent_settled"}),
        ],
    ));
    assert_eq!(terminals(&outputs), [TurnOutcome::Interrupted]);
    let command = outputs
        .iter()
        .flat_map(|output| &output.events)
        .filter_map(|event| match event {
            AdapterEvent::TurnItemUpdated { item }
                if matches!(item.kind, TurnItemKind::CommandExecution { .. }) =>
            {
                Some(item.status)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(command, [ItemStatus::Running, ItemStatus::Interrupted]);
}

#[test]
fn approvals_pass_native_ids_and_options_through() {
    // Pi: Waku's extension asks through `confirm`.
    let mut pi = normalizer(PiFlavor::Pi);
    pi.begin_turn(run(), &thread(), 1, "waku-3".into());
    let outputs = feed(
        &mut pi,
        &[
            json!({"type": "agent_start"}),
            json!({"type": "extension_ui_request", "id": "dlg-1", "method": "confirm", "title": "Allow bash?", "message": "{\n  \"command\": \"ls\"\n}"}),
        ],
    );
    let request = outputs[1]
        .events
        .iter()
        .find_map(|event| match event {
            AdapterEvent::RuntimeRequestUpdated { request } => Some(request.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(request.kind, RuntimeRequestKind::CommandApproval);
    assert_eq!(
        request
            .options
            .iter()
            .map(|option| option.id.as_str())
            .collect::<Vec<_>>(),
        [ALLOW, DENY]
    );
    let (frame, _) = pi
        .respond(
            "dlg-1",
            &RuntimeReply::Decision {
                option_id: ALLOW.into(),
            },
        )
        .unwrap();
    assert_eq!(
        frame,
        json!({"type": "extension_ui_response", "id": "dlg-1", "confirmed": true})
    );

    // Oh My Pi: its own approval `select`.
    let mut omp = normalizer(PiFlavor::OhMyPi);
    omp.begin_turn(run(), &thread(), 1, "waku-3".into());
    let outputs = feed(
        &mut omp,
        &[
            json!({"type": "extension_ui_request", "id": "dlg-2", "method": "select", "title": "Allow tool: edit\nPath: src/main.rs", "options": ["Approve", "Deny"]}),
        ],
    );
    let request = outputs[0]
        .events
        .iter()
        .find_map(|event| match event {
            AdapterEvent::RuntimeRequestUpdated { request } => Some(request.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(request.kind, RuntimeRequestKind::FileChangeApproval);
    let (frame, _) = omp
        .respond(
            "dlg-2",
            &RuntimeReply::Decision {
                option_id: "Deny".into(),
            },
        )
        .unwrap();
    assert_eq!(
        frame,
        json!({"type": "extension_ui_response", "id": "dlg-2", "value": "Deny"})
    );
}

#[test]
fn extension_dialogs_become_structured_questions() {
    let mut normalizer = normalizer(PiFlavor::Pi);
    normalizer.begin_turn(run(), &thread(), 1, "waku-3".into());
    let outputs = feed(
        &mut normalizer,
        &[
            json!({"type": "extension_ui_request", "id": "dlg-3", "method": "select", "title": "Pick a color", "options": ["Red", "Blue"]}),
        ],
    );
    let request = outputs[0]
        .events
        .iter()
        .find_map(|event| match event {
            AdapterEvent::RuntimeRequestUpdated { request } => Some(request.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(request.kind, RuntimeRequestKind::UserInput);
    assert_eq!(request.questions[0].id, "dlg-3");
    assert_eq!(request.questions[0].options.len(), 2);
    let (frame, events) = normalizer
        .respond(
            "dlg-3",
            &RuntimeReply::Answers(vec![UserInputAnswer {
                question_id: "dlg-3".into(),
                answers: vec!["Blue".into()],
            }]),
        )
        .unwrap();
    assert_eq!(
        frame,
        json!({"type": "extension_ui_response", "id": "dlg-3", "value": "Blue"})
    );
    assert!(events.iter().any(|event| matches!(
        event,
        AdapterEvent::RuntimeRequestUpdated { request } if request.status == RequestStatus::Resolved
    )));
    // A dialog with no turn to show it in is cancelled, as Pi does headless.
    let mut idle = self::normalizer(PiFlavor::Pi);
    let output = idle
        .handle(&json!({"type": "extension_ui_request", "id": "dlg-4", "method": "input", "title": "Name"}))
        .unwrap();
    assert_eq!(
        output.writes,
        [json!({"type": "extension_ui_response", "id": "dlg-4", "cancelled": true})]
    );
    assert!(output.events.is_empty());
}
