use super::*;

#[test]
fn selection_transitions_follow_the_rfc_table() {
    let driver = PiDriver::new();
    let caps = pi_capabilities(PiFlavor::Pi);
    let base = Selection::default();
    let plan = |to: Selection| driver.plan_selection_transition(&caps, &base, &to);
    assert_eq!(
        plan(Selection {
            model: Some("anthropic/claude-opus-5".into()),
            ..base.clone()
        }),
        TransitionPlan::ApplyInSession
    );
    assert_eq!(
        plan(Selection {
            reasoning_effort: Some("high".into()),
            ..base.clone()
        }),
        TransitionPlan::ApplyInSession
    );
    for to in [
        Selection {
            service_tier: Some("fast".into()),
            ..base.clone()
        },
        Selection {
            context_window: Some("1m".into()),
            ..base.clone()
        },
        Selection {
            agent_preset: Some("plan".into()),
            ..base.clone()
        },
    ] {
        assert_eq!(plan(to), TransitionPlan::ApplyOnNextTurn);
    }
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
fn launch_arguments_follow_the_runtime_mode() {
    let pi = pi_instance();
    let omp = ohmypi_instance();
    let extension = std::path::Path::new("/ext/waku-approvals-v1.ts");
    let (args, env) = protocol::launch(PiFlavor::Pi, Supervision::FullAccess, None, &pi);
    assert_eq!(args, ["--mode", "rpc", "--approve"]);
    assert_eq!(env, [("PI_SKIP_VERSION_CHECK".to_owned(), "1".to_owned())]);
    let (args, env) = protocol::launch(PiFlavor::Pi, Supervision::Ask, Some(extension), &pi);
    assert_eq!(
        args,
        [
            "--mode",
            "rpc",
            "--approve",
            "--extension",
            "/ext/waku-approvals-v1.ts"
        ]
    );
    assert!(env.contains(&(protocol::RUNTIME_MODE_ENV.to_owned(), "ask".to_owned())));
    // Oh My Pi enforces its own modes; Full access keeps `--yolo`.
    assert_eq!(
        protocol::launch(PiFlavor::OhMyPi, Supervision::FullAccess, None, &omp).0,
        ["--mode", "rpc", "--yolo"]
    );
    assert_eq!(
        protocol::launch(PiFlavor::OhMyPi, Supervision::Ask, None, &omp).0,
        ["--mode", "rpc", "--approval-mode", "always-ask"]
    );
    assert_eq!(
        protocol::launch(PiFlavor::OhMyPi, Supervision::AutoAcceptEdits, None, &omp).0,
        ["--mode", "rpc", "--approval-mode", "write"]
    );
    // `Auto` is not enforced by either flavor: it runs supervised.
    assert_eq!(Supervision::of(RuntimeMode::Auto), Supervision::Ask);
    let (args, _) =
        protocol::fork_launch(PiFlavor::OhMyPi, std::path::Path::new("/s/one.jsonl"), &omp);
    assert_eq!(args, ["--mode", "rpc", "--yolo", "--fork", "/s/one.jsonl"]);
}

#[test]
fn branch_requests_keep_all_but_the_removed_user_turns() {
    let messages = [
        json!({"entryId": "turn-1"}),
        json!({"entryId": "turn-2"}),
        json!({"entryId": "turn-3"}),
    ];
    assert_eq!(
        protocol::branch_request(PiFlavor::Pi, &messages, 0),
        Ok(None)
    );
    assert_eq!(
        protocol::branch_request(PiFlavor::Pi, &messages, 2),
        Ok(Some(json!({"type": "fork", "entryId": "turn-2"})))
    );
    assert_eq!(
        protocol::branch_request(PiFlavor::OhMyPi, &messages, 3),
        Ok(Some(json!({"type": "branch", "entryId": "turn-1"})))
    );
    assert!(protocol::branch_request(PiFlavor::Pi, &messages, 4).is_err());
}

#[test]
fn chunked_frames_reassemble_and_reject_broken_runs() {
    use base64::Engine as _;
    let encode = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);
    let payload = json!({"type": "response", "id": "waku-1", "success": true});
    let bytes = serde_json::to_vec(&payload).unwrap();
    let (first, second) = bytes.split_at(bytes.len() / 2);
    let chunk = |index: u64, data: &[u8]| {
        json!({
            "type": "rpc_chunk",
            "chunkId": "rpc-1",
            "index": index,
            "count": 2,
            "byteLength": bytes.len(),
            "data": encode(data),
        })
    };
    let mut assembly = ChunkAssembly::default();
    assert_eq!(assembly.accept(chunk(0, first)).unwrap(), None);
    assert_eq!(assembly.accept(chunk(1, second)).unwrap(), Some(payload));
    let plain = json!({"type": "agent_start"});
    let mut assembly = ChunkAssembly::default();
    assert_eq!(assembly.accept(plain.clone()).unwrap(), Some(plain.clone()));
    assert_eq!(assembly.accept(chunk(0, first)).unwrap(), None);
    assert!(assembly.accept(plain).is_err());
    let mut assembly = ChunkAssembly::default();
    assert!(assembly.accept(chunk(1, second)).is_err());
}

#[test]
fn usage_limits_are_classified_from_structured_fields_only() {
    assert!(protocol::is_usage_limit(&json!({"errorId": 0x0008_1000})));
    assert!(protocol::is_usage_limit(&json!({"errorStatus": 429})));
    assert!(!protocol::is_usage_limit(
        &json!({"errorMessage": "usage limit reached", "errorStatus": 500})
    ));
}

#[test]
fn the_approval_extension_is_written_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = protocol::materialize_extension(dir.path()).unwrap();
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        protocol::APPROVAL_EXTENSION_SOURCE
    );
    assert_eq!(protocol::materialize_extension(dir.path()).unwrap(), path);
}

mod sessions {
    use std::sync::Arc;

    use waku_orchestrator::ManualClock;

    use super::*;
    use crate::driver::v2::replay::{ReplayProfile, ReplayTranscript, ReplayTransportFactory};
    use crate::driver::v2::{
        AdapterEventReceiver, NullOutput, NullRawLog, RunRef, SeqIdAllocator, Shutdown, TurnIntent,
        TurnMessage,
    };

    const STATE: &str = r#"{"type":"emit_inbound","frame":{"type":"response","id":"waku-1","command":"get_state","success":true,"data":{"sessionId":"s-1","sessionFile":"/pi/s-1.jsonl","model":{"contextWindow":1000}}}}"#;

    fn transcript(lines: &[&str]) -> ReplayTranscript {
        let mut all = vec![
            r#"{"type":"transcript_start","provider":"pi","protocol":"pi.rpc-jsonl","version":"1.0.0","scenario":"unit"}"#,
            r#"{"type":"expect_outbound","frame":{"type":"process_start","program":"pi","args":"<any>","env":"<any>"}}"#,
            r#"{"type":"expect_outbound","frame":{"type":"get_state","id":"waku-1"}}"#,
            STATE,
        ];
        all.extend_from_slice(lines);
        ReplayTranscript::parse(&all.join("\n")).unwrap()
    }

    fn run() -> RunRef {
        RunRef {
            thread_id: ThreadId(uuid::Uuid::from_u128(2)),
            run_id: RunId(uuid::Uuid::from_u128(3)),
            attempt_id: RunAttemptId(uuid::Uuid::from_u128(4)),
            root_node_id: NodeId(uuid::Uuid::from_u128(5)),
        }
    }

    async fn open(
        factory: &Arc<ReplayTransportFactory>,
    ) -> (
        Arc<dyn DriverSession>,
        AdapterEventReceiver,
        ProviderThreadRef,
    ) {
        let opened = PiDriver::new()
            .open_session(SessionContext {
                provider_session_id: ProviderSessionId(uuid::Uuid::from_u128(1)),
                thread_id: ThreadId(uuid::Uuid::from_u128(2)),
                instance: pi_instance(),
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
        let thread = opened
            .session
            .ensure_thread(EnsureThread {
                thread_id: ThreadId(uuid::Uuid::from_u128(2)),
                title: None,
                selection: Selection::default(),
                existing: None,
            })
            .await
            .unwrap();
        assert_eq!(thread.native.id, "s-1");
        (opened.session, opened.events, thread)
    }

    fn turn(thread: ProviderThreadRef) -> TurnStart {
        TurnStart {
            run: run(),
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
        }
    }

    fn factory(lines: &[&str]) -> Arc<ReplayTransportFactory> {
        ReplayTransportFactory::new(
            transcript(lines),
            std::env::temp_dir(),
            std::env::temp_dir(),
            ReplayProfile::new(PI_PROTOCOL),
        )
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    }

    async fn terminals(mut events: AdapterEventReceiver) -> Vec<TurnOutcome> {
        let mut outcomes = Vec::new();
        while let Some(event) = events.recv().await {
            if let AdapterEvent::TurnTerminal { terminal } = event {
                outcomes.push(terminal.outcome);
            }
        }
        outcomes
    }

    /// An agent that exits before taking the prompt: `start_turn` fails
    /// with `SessionLost` and no terminal is sent.
    #[test]
    fn an_exit_before_the_prompt_is_taken_loses_the_session_without_a_terminal() {
        let factory = factory(&[
            r#"{"type":"expect_outbound","frame":{"type":"prompt","id":"waku-2","message":"go"}}"#,
            r#"{"type":"runtime_exit","status":"error","exitCode":1}"#,
        ]);
        runtime().block_on(async {
            let (session, events, thread) = open(&factory).await;
            let started = session.start_turn(turn(thread)).await;
            assert!(matches!(started, Err(DriverError::SessionLost { .. })));
            drop(session);
            assert!(terminals(events).await.is_empty());
        });
        factory.assert_exhausted().unwrap();
    }

    /// A prompt the agent accepted but never started: the abort is answered
    /// and the turn ends `Cancelled`, once.
    #[test]
    fn an_interrupt_before_the_run_starts_cancels_the_turn() {
        let factory = factory(&[
            r#"{"type":"expect_outbound","frame":{"type":"prompt","id":"waku-2","message":"go"}}"#,
            r#"{"type":"emit_inbound","frame":{"type":"response","id":"waku-2","command":"prompt","success":true}}"#,
            r#"{"type":"expect_outbound","frame":{"type":"abort","id":"waku-3"}}"#,
            r#"{"type":"emit_inbound","frame":{"type":"response","id":"waku-3","command":"abort","success":true}}"#,
            r#"{"type":"runtime_exit","status":"success"}"#,
        ]);
        runtime().block_on(async {
            let (session, mut events, thread) = open(&factory).await;
            session.start_turn(turn(thread.clone())).await.unwrap();
            session
                .interrupt_turn(Interrupt {
                    run: run(),
                    thread,
                    provider_turn: None,
                    restart_follows: false,
                })
                .await
                .unwrap();
            let mut outcome = None;
            while let Some(event) = events.recv().await {
                if let AdapterEvent::TurnTerminal { terminal } = event {
                    outcome = Some(terminal.outcome);
                    break;
                }
            }
            assert_eq!(outcome, Some(TurnOutcome::Cancelled));
            session.close().await.unwrap();
            drop(session);
            assert!(terminals(events).await.is_empty());
        });
        factory.assert_exhausted().unwrap();
    }

    /// A process exit mid-turn fails the open turn once and ends the
    /// stream (the manager then reports the session lost).
    #[test]
    fn an_exit_mid_turn_fails_the_turn_once() {
        let factory = factory(&[
            r#"{"type":"expect_outbound","frame":{"type":"prompt","id":"waku-2","message":"go"}}"#,
            r#"{"type":"emit_inbound","frame":{"type":"response","id":"waku-2","command":"prompt","success":true}}"#,
            r#"{"type":"emit_inbound","frame":{"type":"agent_start"}}"#,
            r#"{"type":"runtime_exit","status":"error","exitCode":1}"#,
        ]);
        runtime().block_on(async {
            let (session, events, thread) = open(&factory).await;
            session.start_turn(turn(thread)).await.unwrap();
            let outcomes = terminals(events).await;
            assert!(
                matches!(&outcomes[..], [TurnOutcome::Failed { failure }] if failure.class == FailureClass::TransportError),
                "{outcomes:?}"
            );
            assert!(matches!(
                session.start_turn(turn(ProviderThreadRef::unbound(ProviderThreadId(
                    uuid::Uuid::nil()
                ))))
                .await,
                Err(DriverError::SessionLost { .. })
            ));
        });
        factory.assert_exhausted().unwrap();
    }
}
