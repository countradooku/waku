use std::sync::Arc;

use serde_json::json;
use waku_orchestrator::ManualClock;

use super::*;
use crate::driver::v2::replay::{ReplayProfile, ReplayTranscript, ReplayTransportFactory};
use crate::driver::v2::{
    ContextBlock, NullOutput, NullRawLog, RunRef, SeqIdAllocator, Shutdown, TurnIntent, TurnMessage,
};

#[test]
fn selection_transitions_follow_the_rfc_table() {
    let driver = ClaudeDriver;
    let caps = claude_capabilities();
    let base = Selection::default();
    let plan = |to: Selection| driver.plan_selection_transition(&caps, &base, &to);
    assert_eq!(
        plan(Selection {
            model: Some("claude-sonnet-5".into()),
            ..base.clone()
        }),
        TransitionPlan::ApplyInSession
    );
    assert_eq!(
        plan(Selection {
            context_window: Some("1m".into()),
            ..base.clone()
        }),
        TransitionPlan::ApplyInSession
    );
    assert_eq!(
        plan(Selection {
            reasoning_effort: Some("high".into()),
            ..base.clone()
        }),
        TransitionPlan::RestartSession
    );
    assert_eq!(
        plan(Selection {
            service_tier: Some("fast".into()),
            ..base.clone()
        }),
        TransitionPlan::ApplyOnNextTurn
    );
    assert_eq!(
        plan(Selection {
            runtime_mode: RuntimeMode::Ask,
            ..base.clone()
        }),
        TransitionPlan::RestartSession
    );
    assert!(matches!(
        plan(Selection {
            interaction_mode: InteractionMode::Plan,
            ..base.clone()
        }),
        TransitionPlan::Reject { .. }
    ));
}

#[test]
fn system_blocks_ride_the_launch_and_the_rest_the_envelope() {
    let request = TurnStart {
        run: RunRef {
            thread_id: ThreadId(uuid::Uuid::nil()),
            run_id: RunId(uuid::Uuid::nil()),
            attempt_id: RunAttemptId(uuid::Uuid::nil()),
            root_node_id: NodeId(uuid::Uuid::nil()),
        },
        run_ordinal: 1,
        provider_turn_ordinal: 1,
        restart_continuation_of: None,
        thread: ProviderThreadRef::unbound(ProviderThreadId(uuid::Uuid::nil())),
        message: TurnMessage {
            message_id: MessageId(uuid::Uuid::nil()),
            text: "go".into(),
            attachments: Vec::new(),
        },
        selection: Selection::default(),
        app_context: vec![
            ContextBlock {
                key: "rules".into(),
                kind: ContextKind::System,
                text: "Be brief.".into(),
            },
            ContextBlock {
                key: "fork".into(),
                kind: ContextKind::SyntheticUser,
                text: "earlier".into(),
            },
        ],
        intent: TurnIntent::Prompt,
    };
    assert_eq!(system_prompt(&request).as_deref(), Some("Be brief."));
    let launched = turn_text(&request, true);
    assert!(launched.contains("<context key=\"fork\">") && !launched.contains("Be brief."));
    // A process that already runs cannot take a system prompt any more.
    assert!(turn_text(&request, false).contains("Be brief."));
}

/// A process that exits before echoing the prompt: `start_turn` fails with
/// `SessionLost` and no terminal is sent (only accepted turns get one).
#[test]
fn an_exit_before_the_echo_loses_the_session_without_a_terminal() {
    let transcript = ReplayTranscript::parse(
        &[
            json!({"type":"transcript_start","provider":"claude","protocol":"claude.stream-json","version":"2.1.304","scenario":"exit_before_echo"}),
            json!({"type":"expect_outbound","frame":{"type":"process_start","program":"claude","args":"<any>","env":{}}}),
            json!({"type":"expect_outbound","frame":{"type":"user","message":{"role":"user","content":[{"type":"text","text":"go"}]},"parent_tool_use_id":null}}),
            json!({"type":"runtime_exit","status":"error","exitCode":1}),
        ]
        .iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n"),
    )
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let factory = ReplayTransportFactory::new(
        transcript,
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
        ReplayProfile::new(CLAUDE_PROTOCOL),
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let ids: Arc<dyn IdAllocator> = Arc::new(SeqIdAllocator::new());
        let opened = ClaudeDriver
            .open_session(SessionContext {
                provider_session_id: ProviderSessionId(uuid::Uuid::from_u128(1)),
                thread_id: ThreadId(uuid::Uuid::from_u128(2)),
                instance: claude_instance(),
                cwd: dir.path().to_path_buf(),
                selection: Selection::default(),
                resume: None,
                ids: ids.clone(),
                clock: Arc::new(ManualClock::new(0)),
                transports: Arc::new(factory.clone()),
                mcp: None,
                output: Arc::new(NullOutput),
                raw_log: Arc::new(NullRawLog),
                shutdown: Shutdown::new(),
            })
            .await
            .unwrap();
        let mut events = opened.events;
        let session = opened.session;
        let thread = session
            .ensure_thread(EnsureThread {
                thread_id: ThreadId(uuid::Uuid::from_u128(2)),
                title: None,
                selection: Selection::default(),
                existing: None,
            })
            .await
            .unwrap();
        let started = session
            .start_turn(TurnStart {
                run: RunRef {
                    thread_id: ThreadId(uuid::Uuid::from_u128(2)),
                    run_id: RunId(uuid::Uuid::from_u128(3)),
                    attempt_id: RunAttemptId(uuid::Uuid::from_u128(4)),
                    root_node_id: NodeId(uuid::Uuid::from_u128(5)),
                },
                run_ordinal: 1,
                provider_turn_ordinal: 1,
                restart_continuation_of: None,
                thread,
                message: TurnMessage {
                    message_id: MessageId(uuid::Uuid::from_u128(6)),
                    text: "go".into(),
                    attachments: Vec::new(),
                },
                selection: Selection::default(),
                app_context: Vec::new(),
                intent: TurnIntent::Prompt,
            })
            .await;
        assert!(matches!(started, Err(DriverError::SessionLost { .. })));
        let mut seen = Vec::new();
        while let Some(event) = events.recv().await {
            seen.push(event);
        }
        assert!(
            !seen
                .iter()
                .any(|event| matches!(event, AdapterEvent::TurnTerminal { .. })),
            "{seen:?}"
        );
    });
    factory.assert_exhausted().unwrap();
}
