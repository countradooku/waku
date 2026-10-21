use super::*;

#[test]
fn selection_transitions_follow_the_rfc_table() {
    let driver = AmpDriver::new();
    let caps = amp_capabilities();
    let base = Selection::default();
    let plan = |to: Selection| driver.plan_selection_transition(&caps, &base, &to);
    for to in [
        Selection {
            model: Some("smart".into()),
            ..base.clone()
        },
        Selection {
            reasoning_effort: Some("high".into()),
            ..base.clone()
        },
        Selection {
            service_tier: Some("fast".into()),
            ..base.clone()
        },
        Selection {
            context_window: Some("1m".into()),
            ..base.clone()
        },
        Selection {
            agent_preset: Some("rush".into()),
            ..base.clone()
        },
    ] {
        assert_eq!(plan(to), TransitionPlan::RestartSession);
    }
    assert!(matches!(
        plan(Selection {
            runtime_mode: RuntimeMode::Ask,
            ..base.clone()
        }),
        TransitionPlan::Reject { .. }
    ));
    assert!(matches!(
        plan(Selection {
            interaction_mode: InteractionMode::Plan,
            ..base.clone()
        }),
        TransitionPlan::Reject { .. }
    ));
}

#[test]
fn launch_arguments_stream_json_and_resume_the_exact_thread() {
    let selection = Selection {
        model: Some("smart".into()),
        reasoning_effort: Some("high".into()),
        service_tier: Some("fast".into()),
        ..Selection::default()
    };
    let args = protocol::launch_args(&selection, "T-123");
    assert_eq!(
        args,
        [
            "threads",
            "continue",
            "T-123",
            "--execute",
            "--stream-json-thinking",
            "--stream-json-input",
            "--dangerously-allow-all",
            "--mode",
            "smart",
            "--effort",
            "high",
            "--fast"
        ]
    );
    // The agent preset is Amp's mode when set.
    let preset = Selection {
        model: Some("smart".into()),
        agent_preset: Some("rush".into()),
        ..Selection::default()
    };
    assert!(
        protocol::launch_args(&preset, "T-1")
            .windows(2)
            .any(|pair| pair == ["--mode", "rush"])
    );
}

#[test]
fn steering_marks_the_message() {
    assert_eq!(
        protocol::user_message("go", true),
        serde_json::json!({"type": "user", "steer": true, "message": {"role": "user", "content": [{"type": "text", "text": "go"}]}})
    );
    assert!(protocol::user_message("go", false).get("steer").is_none());
}

#[test]
fn thread_ids_are_read_from_plain_or_quoted_lines() {
    assert_eq!(
        protocol::thread_id_from_line("T-abc-123\n").as_deref(),
        Some("T-abc-123")
    );
    assert_eq!(
        protocol::thread_id_from_line("\"T-abc\"").as_deref(),
        Some("T-abc")
    );
    assert_eq!(protocol::thread_id_from_line("Created thread T-abc"), None);
}

/// SIGINT ends the run and the process; the turn ends `Interrupted` with a
/// reusable thread, and the next turn reopens it with `threads continue`.
#[test]
fn an_interrupt_ends_the_process_and_the_next_turn_continues_the_thread() {
    use waku_orchestrator::ManualClock;

    use crate::driver::v2::replay::{ReplayProfile, ReplayTranscript, ReplayTransportFactory};
    use crate::driver::v2::{
        NullOutput, NullRawLog, RunRef, SeqIdAllocator, Shutdown, TurnIntent, TurnMessage,
    };

    let launch = r#"["threads","continue","T-1","--execute","--stream-json-thinking","--stream-json-input","--dangerously-allow-all"]"#;
    let lines = [
        r#"{"type":"transcript_start","provider":"amp","protocol":"amp.stream-json","version":"1","scenario":"unit"}"#.to_owned(),
        r#"{"type":"expect_outbound","channel":"threads-new","frame":{"type":"process_start","program":"amp","args":["threads","new"],"env":{}}}"#.to_owned(),
        r#"{"type":"emit_inbound","channel":"threads-new","frame":"T-1"}"#.to_owned(),
        r#"{"type":"runtime_exit","channel":"threads-new","status":"success"}"#.to_owned(),
        format!(r#"{{"type":"expect_outbound","frame":{{"type":"process_start","program":"amp","args":{launch},"env":{{}}}}}}"#),
        r#"{"type":"expect_outbound","frame":{"type":"user","message":{"role":"user","content":[{"type":"text","text":"one"}]}}}"#.to_owned(),
        r#"{"type":"emit_inbound","frame":{"type":"assistant","message":{"content":[{"type":"text","text":"Working"}]}}}"#.to_owned(),
        r#"{"type":"expect_outbound","frame":{"type":"signal","signal":"SIGINT"}}"#.to_owned(),
        r#"{"type":"runtime_exit","status":"error","signal":2}"#.to_owned(),
        format!(r#"{{"type":"expect_outbound","channel":"main#2","frame":{{"type":"process_start","program":"amp","args":{launch},"env":{{}}}}}}"#),
        r#"{"type":"expect_outbound","channel":"main#2","frame":{"type":"user","message":{"role":"user","content":[{"type":"text","text":"two"}]}}}"#.to_owned(),
        r#"{"type":"emit_inbound","channel":"main#2","frame":{"type":"assistant","message":{"content":[{"type":"text","text":"Done"}],"stop_reason":"end_turn"}}}"#.to_owned(),
        r#"{"type":"runtime_exit","channel":"main#2","status":"success"}"#.to_owned(),
    ];
    let factory = ReplayTransportFactory::new(
        ReplayTranscript::parse(&lines.join("\n")).unwrap(),
        std::env::temp_dir(),
        std::env::temp_dir(),
        ReplayProfile::new(AMP_PROTOCOL),
    );
    let run = |n: u128| RunRef {
        thread_id: ThreadId(uuid::Uuid::from_u128(2)),
        run_id: RunId(uuid::Uuid::from_u128(10 + n)),
        attempt_id: RunAttemptId(uuid::Uuid::from_u128(20 + n)),
        root_node_id: NodeId(uuid::Uuid::from_u128(30 + n)),
    };
    let turn = |n: u128, ordinal: u32, text: &str, thread: &ProviderThreadRef| TurnStart {
        run: run(n),
        run_ordinal: ordinal,
        provider_turn_ordinal: ordinal,
        restart_continuation_of: None,
        thread: thread.clone(),
        message: TurnMessage {
            message_id: MessageId(uuid::Uuid::from_u128(40 + n)),
            text: text.into(),
            attachments: Vec::new(),
        },
        selection: Selection::default(),
        app_context: Vec::new(),
        intent: TurnIntent::Prompt,
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let opened = AmpDriver::new()
            .open_session(SessionContext {
                provider_session_id: ProviderSessionId(uuid::Uuid::from_u128(1)),
                thread_id: ThreadId(uuid::Uuid::from_u128(2)),
                instance: amp_instance(),
                cwd: std::env::temp_dir(),
                selection: Selection::default(),
                resume: None,
                ids: Arc::new(SeqIdAllocator::new()),
                clock: Arc::new(ManualClock::new(0)),
                transports: Arc::new(factory.clone()),
                mcp: None,
                output: Arc::new(NullOutput),
                raw_log: Arc::new(NullRawLog),
                shutdown: Shutdown::new(),
            })
            .await
            .unwrap();
        let (session, mut events) = (opened.session, opened.events);
        let thread = session
            .ensure_thread(EnsureThread {
                thread_id: ThreadId(uuid::Uuid::from_u128(2)),
                title: None,
                selection: Selection::default(),
                existing: None,
            })
            .await
            .unwrap();
        assert_eq!(thread.native.id, "T-1");
        session
            .start_turn(turn(1, 1, "one", &thread))
            .await
            .unwrap();
        // Interrupt once Amp answered: the signal must follow the prompt
        // on the wire, which a write still in the pipe would not.
        loop {
            match events.recv().await {
                Some(AdapterEvent::MessageUpdated { .. }) => break,
                Some(_) => {}
                None => panic!("the stream ended"),
            }
        }
        let mut next_terminal = async || loop {
            match events.recv().await {
                Some(AdapterEvent::TurnTerminal { terminal }) => {
                    return Some((terminal.outcome, terminal.thread_disposition));
                }
                Some(_) => {}
                None => return None,
            }
        };
        session
            .interrupt_turn(Interrupt {
                run: run(1),
                thread: thread.clone(),
                provider_turn: None,
                restart_follows: false,
            })
            .await
            .unwrap();
        assert_eq!(
            next_terminal().await,
            Some((
                TurnOutcome::Interrupted,
                crate::driver::v2::ThreadDisposition::Reusable
            ))
        );
        session
            .start_turn(turn(2, 2, "two", &thread))
            .await
            .unwrap();
        assert_eq!(
            next_terminal().await.map(|(outcome, _)| outcome),
            Some(TurnOutcome::Completed)
        );
        session.close().await.unwrap();
        assert_eq!(next_terminal().await, None);
    });
    factory.assert_exhausted().unwrap();
}

#[test]
fn context_blocks_ride_the_shared_envelope() {
    use crate::driver::v2::{ContextBlock, ContextKind, RunRef, TurnIntent, TurnMessage};
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
        app_context: vec![ContextBlock {
            key: "fork".into(),
            kind: ContextKind::SyntheticUser,
            text: "earlier".into(),
        }],
        intent: TurnIntent::Prompt,
    };
    let text = turn_text(&request);
    assert!(text.starts_with("<WAKU_CONTEXT_V1>"));
    assert!(text.ends_with("go"));
}
