//! Forward-compatibility and round-trip tests for the orchestration types
//! (#43 acceptance: an unknown variant yields a skippable record).

use std::path::PathBuf;

use uuid::Uuid;

use super::*;

fn id<T: From<Uuid>>(n: u128) -> T {
    T::from(Uuid::from_u128(n))
}

fn base_item(kind: TurnItemKind) -> TurnItem {
    TurnItem {
        id: id(1),
        thread_id: id(2),
        run_id: Some(id(3)),
        node_id: None,
        provider_thread_id: None,
        provider_turn_id: None,
        native_item_ref: None,
        parent_item_id: None,
        position: TurnItem::position_of(1, 2),
        status: ItemStatus::Completed,
        title: Some("title".into()),
        tool: Some(ToolPresentation {
            surface: Some(ToolSurface::Browser),
            icon: Some(ToolIcon::Website {
                page_url: "https://example.com".into(),
                favicon_url: None,
                favicon_url_dark: None,
            }),
            source: Some(ToolSource {
                key: "browser".into(),
                name: "Browser".into(),
                kind: ToolSourceKind::Browser,
                icon: None,
            }),
            non_execution_kind: None,
        }),
        stream_rev: 3,
        started_at: Some(10),
        completed_at: Some(20),
        updated_at: 20,
        kind,
    }
}

fn failure() -> ProviderFailure {
    ProviderFailure::new(FailureClass::ProviderError, "boom").with_code("x")
}

/// One sample of every known kind, in `TYPE_NAMES` order.
fn every_kind() -> Vec<TurnItemKind> {
    vec![
        TurnItemKind::Notification(Notification {
            source: NotificationSource::DelegatedTask {
                task_ids: vec![id(9)],
                child_thread_id: Some(id(8)),
            },
            outcome: NotificationOutcome::Completed,
            summary: "done".into(),
            detail: None,
        }),
        TurnItemKind::UserMessage {
            message_id: id(4),
            input_intent: InputIntent::TurnStart,
            text: "hi".into(),
            display_text: None,
            attachments: vec![Attachment {
                path: PathBuf::from("/tmp/a.png"),
                mention: "a.png".into(),
                name: "a.png".into(),
                is_dir: false,
                is_image: true,
                blob_reference: Some("waku-blob:abc.png".into()),
            }],
            created_by: Actor::User,
            sender_thread_id: None,
        },
        TurnItemKind::AssistantMessage {
            message_id: id(5),
            text: "hello".into(),
            streaming: true,
        },
        TurnItemKind::Reasoning {
            text: "thinking".into(),
            streaming: false,
        },
        TurnItemKind::ProposedPlan {
            plan_id: id(6),
            markdown: "# plan".into(),
            streaming: false,
        },
        TurnItemKind::TodoList {
            plan_id: id(6),
            steps: vec![PlanStep {
                id: "1".into(),
                text: "step".into(),
                status: PlanStepStatus::Running,
            }],
            explanation: Some("why".into()),
        },
        TurnItemKind::UserInputRequest {
            request_id: id(7),
            questions: vec![UserInputQuestion {
                id: "q".into(),
                header: "H".into(),
                question: "Q?".into(),
                options: vec![UserInputOption {
                    label: "yes".into(),
                    description: None,
                }],
                multi_select: false,
            }],
            answers: None,
            response_mode: Some(ResponseMode::Message),
        },
        TurnItemKind::FileChange {
            file_name: "a.rs".into(),
            additions: Some(1),
            deletions: Some(2),
            diff: Some("@@".into()),
            old_text: None,
            new_text: None,
            changes: vec![FileChangeDetail {
                operation: "update".into(),
                path: "a.rs".into(),
                old_path: None,
                file_type: None,
                mime_type: None,
            }],
            body_omitted: false,
        },
        TurnItemKind::CommandExecution {
            input: "ls".into(),
            output: Some("a".into()),
            output_truncated: false,
            output_omitted: false,
            output_indicates_failure: false,
            exit_code: Some(0),
        },
        TurnItemKind::FileSearch {
            pattern: Some("*.rs".into()),
            results: vec![FileSearchResult {
                file_name: "a.rs".into(),
                line: Some(1),
                column: None,
                preview: None,
            }],
        },
        TurnItemKind::WebSearch {
            patterns: vec!["rust".into()],
            results: vec![WebSearchResult {
                title: Some("Rust".into()),
                url: Some("https://rust-lang.org".into()),
                snippet: None,
            }],
        },
        TurnItemKind::ApprovalRequest {
            request_id: id(7),
            request_kind: RuntimeRequestKind::CommandApproval,
            prompt: Some("run?".into()),
            app_name: None,
            options: vec![PermissionOption {
                id: "allow_once".into(),
                label: "Allow".into(),
                allow: true,
            }],
        },
        TurnItemKind::Checkpoint {
            checkpoint_id: id(10),
            scope_id: id(11),
            files: vec![CheckpointFile {
                path: "a.rs".into(),
                kind: "modified".into(),
                additions: 1,
                deletions: 0,
            }],
        },
        TurnItemKind::RunInterruptRequest {
            message: "stop".into(),
        },
        TurnItemKind::RunInterruptResult {
            message: "stopped".into(),
        },
        TurnItemKind::SystemNotice {
            message: "notice".into(),
        },
        TurnItemKind::Error {
            failure: failure(),
            retry: Some(ProviderRetry {
                attempt: 1,
                max_attempts: Some(3),
                retry_delay_ms: None,
            }),
        },
        TurnItemKind::Compaction {
            driver: Some(DriverKind::Codex),
            summary: None,
            before_tokens: Some(100),
            after_tokens: Some(10),
        },
        TurnItemKind::Handoff {
            context_handoff_id: id(12),
            from_provider_instance_ids: vec![ProviderInstanceId::new("codex")],
            to_provider_instance_id: ProviderInstanceId::new("claude"),
            strategy: HandoffStrategy::FullThreadSummary,
            summary: None,
        },
        TurnItemKind::Fork {
            source: ForkOrigin::Run {
                thread_id: id(2),
                run_id: id(3),
            },
            target_thread_id: id(13),
        },
        TurnItemKind::ThreadCreated {
            target_thread_id: id(13),
            target_run_id: None,
            target_provider_instance_id: ProviderInstanceId::new("codex"),
            target_model: "gpt".into(),
        },
        TurnItemKind::SecretRequest {
            label: "TOKEN".into(),
            reason: "needed".into(),
            placeholder: None,
            secret_status: SecretStatus::Pending,
        },
        TurnItemKind::Subagent {
            subagent_id: id(14),
            origin: SubagentOrigin::ProviderNative,
            driver: DriverKind::Claude,
            child_thread_id: None,
            prompt: "do".into(),
            progress: None,
            result: None,
        },
        TurnItemKind::DynamicTool {
            tool_name: Some("tool".into()),
            input: JsonText::new(r#"{"a":1}"#),
            output: None,
            output_omitted: false,
            viewed_image_path: None,
        },
    ]
}

#[test]
fn every_turn_item_kind_round_trips_with_its_tag() {
    let kinds = every_kind();
    assert_eq!(kinds.len(), TurnItemKind::TYPE_NAMES.len());
    for (kind, name) in kinds.into_iter().zip(TurnItemKind::TYPE_NAMES) {
        assert_eq!(kind.type_name(), name);
        let item = base_item(kind);
        let json = serde_json::to_string(&item).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["kind"]["type"], name, "{json}");
        let decoded: TurnItem = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, item);
    }
}

#[test]
fn unknown_turn_item_kind_is_a_skippable_row() {
    let known = serde_json::to_value(base_item(TurnItemKind::SystemNotice {
        message: "ok".into(),
    }))
    .unwrap();
    let mut unknown = known.clone();
    unknown["id"] = serde_json::json!(Uuid::from_u128(99).to_string());
    unknown["position"] = serde_json::json!(TurnItem::position_of(1, 3));
    unknown["kind"] = serde_json::json!({
        "type": "hologram",
        "frames": [{"nested": {"deep": [1, 2, 3]}}],
        "text": "from the future",
    });
    unknown["futureField"] = serde_json::json!({"x": 1});
    let rows = serde_json::to_string(&vec![known, unknown]).unwrap();

    let decoded: Vec<TurnItem> = serde_json::from_str(&rows).unwrap();
    assert_eq!(decoded.len(), 2);
    assert!(matches!(decoded[0].kind, TurnItemKind::SystemNotice { .. }));
    assert!(decoded[1].kind.is_unknown());
    // The cursor still advances: the row keeps its id and position.
    assert_eq!(decoded[1].id, id(99));
    assert_eq!(decoded[1].position, TurnItem::position_of(1, 3));
}

#[test]
fn unknown_domain_event_skips_its_payload_and_keeps_the_sequence() {
    let thread_id: ThreadId = id(2);
    let frames = serde_json::json!([
        {
            "sequence": 41,
            "eventId": Uuid::from_u128(1).to_string(),
            "threadId": thread_id.to_string(),
            "occurredAt": 5,
            "event": {"type": "galaxy.updated", "payload": {"stars": [1, 2]}, "extra": true},
        },
        {
            "sequence": 42,
            "eventId": Uuid::from_u128(2).to_string(),
            "threadId": thread_id.to_string(),
            "occurredAt": 6,
            "event": serde_json::to_value(DomainEvent::TurnItemUpdated(base_item(
                TurnItemKind::Reasoning { text: "r".into(), streaming: false }
            ))).unwrap(),
        },
    ]);
    let decoded: Vec<StoredEvent> = serde_json::from_value(frames).unwrap();
    assert_eq!(decoded[0].sequence, 41);
    assert_eq!(decoded[0].event, DomainEvent::Unknown);
    assert_eq!(decoded[1].sequence, 42);
    assert_eq!(decoded[1].event.event_type(), "turn_item.updated");

    assert_eq!(
        DomainEvent::from_parts("galaxy.updated", "{not even json").unwrap(),
        DomainEvent::Unknown
    );
}

#[test]
fn stored_payload_round_trips_through_from_parts() {
    let event = DomainEvent::TurnItemUpdated(base_item(TurnItemKind::DynamicTool {
        tool_name: None,
        input: JsonText::new("[1,2]"),
        output: Some(JsonText::new("null")),
        output_omitted: false,
        viewed_image_path: None,
    }));
    let payload = event.payload_json().unwrap();
    assert!(!payload.contains("turn_item.updated"));
    let decoded = DomainEvent::from_parts(event.event_type(), &payload).unwrap();
    assert_eq!(decoded, event);
    assert_eq!(event.entity_id(), Uuid::from_u128(1).to_string());
    assert_eq!(event.run_id(), Some(id(3)));
}

#[test]
fn unknown_enum_values_decode_to_unknown_or_the_weakest_value() {
    assert_eq!(
        serde_json::from_str::<RunStatus>("\"teleporting\"").unwrap(),
        RunStatus::Unknown
    );
    assert_eq!(
        serde_json::from_str::<RunStatus>("\"rolled_back\"").unwrap(),
        RunStatus::RolledBack
    );
    assert_eq!(
        serde_json::from_str::<RefStrength>("\"quantum\"").unwrap(),
        RefStrength::None
    );
    assert_eq!(
        serde_json::from_str::<Quality>("\"great\"").unwrap(),
        Quality::None
    );
    assert_eq!(
        serde_json::from_str::<Enforcement>("\"hard\"").unwrap(),
        Enforcement::ClientBoundary
    );
    assert_eq!(
        serde_json::from_str::<NotificationSource>(r#"{"type":"cron","every":"1h"}"#).unwrap(),
        NotificationSource::BackgroundTask
    );
    assert_eq!(
        serde_json::from_str::<WorkspaceStrategy>(r#"{"type":"cloud","region":"x"}"#).unwrap(),
        WorkspaceStrategy::Unknown
    );
    assert_eq!(
        serde_json::from_str::<ThreadChange>("\"teleported\"").unwrap(),
        ThreadChange::Unknown
    );
    for status in RunStatus::ALL {
        let json = serde_json::to_string(status).unwrap();
        assert_eq!(json, format!("\"{}\"", status.as_str()));
    }
}

#[test]
fn capabilities_decode_missing_groups_as_unsupported() {
    let caps: Capabilities =
        serde_json::from_str(r#"{"turns":{"active_steering":true,"future":1},"x":{}}"#).unwrap();
    assert!(caps.turns.active_steering);
    assert!(!caps.turns.interrupt);
    assert_eq!(caps.identity.thread_ids, RefStrength::None);
}

#[test]
fn command_ids_are_validated_on_decode() {
    assert!(CommandId::new("effect:12:0").is_ok());
    assert!(CommandId::new("limit-resume:a.b_c").is_ok());
    assert!(CommandId::new("").is_err());
    assert!(CommandId::new("has space").is_err());
    assert!(CommandId::new("x".repeat(257)).is_err());
    assert!(serde_json::from_str::<CommandId>("\"bad/slash\"").is_err());
    assert_eq!(
        serde_json::from_str::<CommandId>("\"ok-1\"")
            .unwrap()
            .as_str(),
        "ok-1"
    );
}

#[test]
fn commands_decode_by_tag_and_reject_unknown_ones() {
    let envelope = CommandEnvelope {
        command_id: CommandId::new("c1").unwrap(),
        issued_by: Actor::User,
        source: CreationSource::Desktop,
        command: Command::MessageDispatch {
            thread_id: id(1),
            message_id: id(2),
            text: "hi".into(),
            display_text: None,
            attachments: Vec::new(),
            context: Vec::new(),
            model_selection: None,
            mode: DispatchMode::SteerActive {
                target_run_id: id(3),
            },
            title_seed: None,
            source_plan: None,
        },
    };
    let json = serde_json::to_string(&envelope).unwrap();
    assert!(json.contains(r#""type":"message.dispatch""#), "{json}");
    assert!(json.contains(r#""targetRunId""#), "{json}");
    assert_eq!(
        serde_json::from_str::<CommandEnvelope>(&json).unwrap(),
        envelope
    );
    assert!(serde_json::from_str::<Command>(r#"{"type":"thread.stop","threadId":"x"}"#).is_err());
}

#[test]
fn driver_kind_resolves_legacy_and_instance_tags() {
    assert_eq!(DriverKind::from_tag("deepSeek"), DriverKind::DeepSeek);
    assert_eq!(DriverKind::from_tag("open_code"), DriverKind::OpenCode);
    assert_eq!(
        DriverKind::from_instance(&ProviderInstanceId::new("codex:work")),
        DriverKind::Codex
    );
    assert_eq!(DriverKind::from_tag("nope"), DriverKind::Unknown);
}

/// The TypeScript side of the forward-compatibility rule: every open union
/// keeps an `unknown` member a client can skip.
#[cfg(feature = "ts")]
#[test]
fn typescript_declarations_keep_the_unknown_members() {
    use ts_rs::TS;

    let config = ts_rs::Config::new();
    let item = TurnItemKind::decl(&config);
    assert!(item.contains("\"unknown\""), "{item}");
    let event = DomainEvent::decl(&config);
    assert!(event.contains("\"unknown\""), "{event}");
    let status = RunStatus::decl(&config);
    assert!(
        status.contains("\"rolled_back\"") && status.contains("\"unknown\""),
        "{status}"
    );
}
