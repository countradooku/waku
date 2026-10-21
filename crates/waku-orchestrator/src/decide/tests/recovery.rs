//! `RecoverThread` and `ContinueRun` (#59): pure decisions over hand-built
//! projections.

use super::*;
use crate::command::InternalCommand;
use crate::decide::recovery::{
    CONTINUE_TEXT, INTERRUPTED_NOTICE, PROCESS_LOSS, RESUMED_NOTICE, Refusal,
};

const BOOT: Uuid = Uuid::from_u128(0xb007);
const SESSION: u128 = 600;
const TURN: u128 = 500;

impl Fixture {
    fn internal(&mut self, command: InternalCommand) -> Result<Decision, CommandError> {
        self.now += 1;
        let mut cx = DecideContext {
            now: self.now,
            ids: &mut self.ids,
            capabilities: &self.capabilities,
            settings: &self.settings,
            thread: self.projection.as_ref(),
            related: None,
            project: Some(&self.project),
        };
        decide(
            &mut cx,
            &AnyCommand::Internal {
                command_id: CommandId::new("internal").unwrap(),
                command,
            },
        )
    }

    /// Decides an internal command and applies it.
    fn run_internal(&mut self, command: InternalCommand) -> Decision {
        let decision = self.internal(command).expect("accepted");
        self.apply(&decision.events);
        decision
    }

    fn apply(&mut self, events: &[DomainEvent]) {
        let projection = self.projection.as_mut().unwrap();
        for event in events {
            projection.apply(event);
        }
    }

    fn recover(&mut self) -> Decision {
        self.run_internal(InternalCommand::RecoverThread {
            thread_id: id(10),
            boot_id: BOOT,
        })
    }

    /// The driver accepted `run`'s turn on a live session and streams an
    /// answer; with `request`, it then asked the user a question.
    fn make_live(&mut self, run: RunId, request: bool, survive: bool) {
        let projection = self.thread().clone();
        let run = projection.runs[&run].clone();
        let attempt = projection.attempts[&run.active_attempt_id.unwrap()].clone();
        let provider_thread = projection.provider_threads[&attempt.provider_thread_id].clone();
        let mut capabilities = Capabilities::default();
        capabilities.sessions.pending_requests_survive_restart = survive;
        let mut events = vec![
            DomainEvent::ProviderSessionUpdated(ProviderSession {
                id: id(SESSION),
                driver: DriverKind::Codex,
                provider_instance_id: ProviderInstanceId::new("codex"),
                status: SessionStatus::Running,
                cwd: PathBuf::from("/repo"),
                model: None,
                capabilities,
                created_at: NOW,
                updated_at: NOW,
                last_error: None,
                detached_at: None,
                detach_reason: None,
            }),
            DomainEvent::ProviderThreadUpdated(ProviderThread {
                provider_session_id: Some(id(SESSION)),
                status: ProviderThreadStatus::Active,
                native_thread_ref: Some(ProviderRef {
                    driver: DriverKind::Codex,
                    native_id: Some("t-1".into()),
                    strength: RefStrength::Strong,
                    fingerprint: None,
                    ordinal: None,
                }),
                pending_background_tasks: vec![PendingBackgroundTask {
                    kind: PendingTaskKind::Command,
                    task_id: "bg-1".into(),
                    description: Some("npm run dev".into()),
                    parent_task_id: None,
                    child_thread_id: None,
                }],
                ..provider_thread.clone()
            }),
            DomainEvent::ProviderTurnUpdated(ProviderTurn {
                id: id(TURN),
                provider_thread_id: provider_thread.id,
                node_id: attempt.root_node_id,
                run_attempt_id: Some(attempt.id),
                native_turn_ref: None,
                ordinal: 1,
                status: ProviderTurnStatus::Running,
                started_at: Some(NOW),
                completed_at: None,
                token_usage: None,
                turn_token_usage: None,
            }),
            DomainEvent::RunAttemptUpdated(RunAttempt {
                provider_turn_id: Some(id(TURN)),
                status: AttemptStatus::Running,
                ..attempt.clone()
            }),
            DomainEvent::RunUpdated(Run {
                status: if request {
                    RunStatus::Waiting
                } else {
                    RunStatus::Running
                },
                ..run.clone()
            }),
            DomainEvent::TurnItemUpdated(TurnItem {
                id: id(700),
                thread_id: id(10),
                run_id: Some(run.id),
                node_id: run.root_node_id,
                provider_thread_id: Some(provider_thread.id),
                provider_turn_id: Some(id(TURN)),
                native_item_ref: None,
                parent_item_id: None,
                position: TurnItem::position_of(run.ordinal.unwrap(), 5),
                status: ItemStatus::Running,
                title: None,
                tool: None,
                stream_rev: 3,
                started_at: Some(NOW),
                completed_at: None,
                updated_at: NOW,
                kind: TurnItemKind::AssistantMessage {
                    message_id: id(701),
                    text: "Halfway".into(),
                    streaming: true,
                },
            }),
        ];
        if request {
            events.push(DomainEvent::RuntimeRequestUpdated(RuntimeRequest {
                id: id(800),
                thread_id: id(10),
                node_id: attempt.root_node_id,
                provider_turn_id: Some(id(TURN)),
                native_request_ref: None,
                kind: RuntimeRequestKind::CommandApproval,
                status: RequestStatus::Pending,
                response_capability: ResponseCapability::Live {
                    provider_session_id: id(SESSION),
                },
                options: Vec::new(),
                questions: Vec::new(),
                response: None,
                created_at: NOW,
                resolved_at: None,
            }));
        }
        self.apply(&events);
    }

    fn notices(&self, lifecycle: LifecycleNotice) -> Vec<TurnItem> {
        let mut notices = self
            .thread()
            .hot_items
            .values()
            .filter(|item| {
                matches!(
                    &item.kind,
                    TurnItemKind::SystemNotice { lifecycle: Some(kind), .. } if *kind == lifecycle
                )
            })
            .cloned()
            .collect::<Vec<_>>();
        notices.sort_by_key(|item| item.position);
        notices
    }
}

/// A thread whose first run is live on the provider.
fn live_thread() -> (Fixture, RunId) {
    let mut fixture = Fixture::new(Capabilities::default());
    fixture.run(create());
    let run = run_of(&fixture.run(send(100, DispatchMode::Auto)))[0].id;
    fixture.make_live(run, false, false);
    (fixture, run)
}

fn continue_effect(decision: &Decision) -> Option<(RunId, u8)> {
    decision.effects.iter().find_map(|effect| match effect {
        Effect::ProviderRuntimeContinue {
            source_run_id,
            chain_length,
        } => Some((*source_run_id, *chain_length)),
        _ => None,
    })
}

fn notice_text(item: &TurnItem) -> &str {
    match &item.kind {
        TurnItemKind::SystemNotice { message, .. } => message,
        _ => panic!("not a notice"),
    }
}

/// The `ContinueRun` the effect would dispatch for `source`.
fn continuation(fixture: &Fixture, source: RunId) -> InternalCommand {
    crate::recovery::continuation_command(id(10), &fixture.thread().runs[&source]).1
}

#[test]
fn a_run_interrupted_by_a_restart_records_process_loss_and_asks_to_continue() {
    let (mut fixture, run) = live_thread();
    let decision = fixture.recover();
    let projection = fixture.thread();

    // The dead process's session stopped; its thread lost the session and
    // the background work, the open provider turn was interrupted.
    let session = &projection.provider_sessions[&id(SESSION)];
    assert_eq!(session.status, SessionStatus::Stopped);
    assert_eq!(session.detach_reason.as_deref(), Some(PROCESS_LOSS));
    let provider_thread = projection.provider_threads.values().next().unwrap();
    assert_eq!(provider_thread.provider_session_id, None);
    assert_eq!(provider_thread.status, ProviderThreadStatus::Idle);
    assert!(provider_thread.pending_background_tasks.is_empty());
    assert!(
        provider_thread.native_thread_ref.is_some(),
        "the native thread is kept for the resume"
    );
    assert_eq!(
        projection.provider_turns[&id(TURN)].status,
        ProviderTurnStatus::Interrupted
    );

    // The run ended `interrupted` with a transport failure.
    let interrupted = &projection.runs[&run];
    assert_eq!(interrupted.status, RunStatus::Interrupted);
    let failure = interrupted.failure.as_ref().unwrap();
    assert_eq!(failure.class, FailureClass::TransportError);
    assert_eq!(failure.code.as_deref(), Some(PROCESS_LOSS));
    assert_eq!(
        interrupted.restart_cancelled_background_work,
        [CancelledBackgroundWork {
            kind: BackgroundWorkKind::Shell,
            label: "npm run dev".into(),
            id: "bg-1".into(),
        }]
    );
    let attempt = &projection.attempts[&interrupted.active_attempt_id.unwrap()];
    assert_eq!(attempt.status, AttemptStatus::Interrupted);

    // The streaming answer stopped streaming; the notice is in the run's
    // block after it.
    let answer = projection
        .hot_items
        .values()
        .find(|item| item.id == id(700));
    assert!(
        answer.is_none(),
        "the run is terminal, its rows left the hot set"
    );
    let events = decision
        .events
        .iter()
        .filter_map(|event| match event {
            DomainEvent::TurnItemUpdated(item) => Some(item),
            _ => None,
        })
        .collect::<Vec<_>>();
    let answer = events.iter().find(|item| item.id == id(700)).unwrap();
    assert_eq!(answer.status, ItemStatus::Interrupted);
    assert!(matches!(
        answer.kind,
        TurnItemKind::AssistantMessage {
            streaming: false,
            ..
        }
    ));
    let notice = events
        .iter()
        .find(|item| {
            matches!(
                item.kind,
                TurnItemKind::SystemNotice {
                    lifecycle: Some(LifecycleNotice::RestartInterrupted),
                    ..
                }
            )
        })
        .unwrap();
    assert_eq!(notice_text(notice), INTERRUPTED_NOTICE);
    assert_eq!(notice.run_id, Some(run));
    assert_eq!(notice.block(), 1);

    // Continuation intent is recorded as the outbox effect.
    assert_eq!(continue_effect(&decision), Some((run, 1)));
}

#[test]
fn the_continuation_is_a_new_run_with_a_resumed_row_and_runs_once() {
    let (mut fixture, run) = live_thread();
    fixture.recover();
    let command = continuation(&fixture, run);
    let decision = fixture.run_internal(command.clone());
    let projection = fixture.thread();
    let continued = projection
        .runs
        .values()
        .find(|candidate| candidate.id != run)
        .unwrap()
        .clone();
    assert_eq!(
        continued.continuation,
        Some(RunContinuation {
            kind: ContinuationKind::Restart,
            of_run_id: run,
        })
    );
    assert_eq!(continued.status, RunStatus::Starting);
    assert_eq!(continued.ordinal, Some(2));
    assert_eq!(
        continued.provider_thread_id, projection.runs[&run].provider_thread_id,
        "the provider thread is resumed, not replaced"
    );
    assert_eq!(
        continued.restart_cancelled_background_work.len(),
        1,
        "the cancelled work follows the continuation"
    );
    let message = &projection.hot_messages[&continued.user_message_id];
    assert_eq!(message.created_by, Actor::System);
    assert!(message.text.starts_with(CONTINUE_TEXT));
    assert!(message.text.contains("npm run dev"));
    assert!(matches!(
        decision.effects.as_slice(),
        [Effect::ProviderTurnStart { run_id, .. }] if *run_id == continued.id
    ));

    // The resumed row opens the block, before the system message.
    let resumed = fixture.notices(LifecycleNotice::RestartResumed);
    assert_eq!(resumed.len(), 1);
    assert_eq!(notice_text(&resumed[0]), RESUMED_NOTICE);
    assert_eq!(resumed[0].position, TurnItem::position_of(2, 0));
    assert_eq!(resumed[0].run_id, Some(continued.id));
    assert_eq!(resumed[0].node_id, continued.root_node_id);
    let prompt = fixture
        .thread()
        .hot_items
        .values()
        .find(|item| matches!(item.kind, TurnItemKind::UserMessage { .. }))
        .unwrap();
    assert_eq!(prompt.position, TurnItem::position_of(2, 1));

    // Continued once: the same continuation again is refused.
    assert!(matches!(
        fixture.internal(command),
        Err(CommandError::Conflict { .. })
    ));
}

#[test]
fn recovery_refuses_to_continue_and_says_why() {
    type Setup = fn(&mut Fixture, RunId);
    let cases: [(&str, Setup, Refusal); 6] = [
        (
            "setting off",
            |fixture, _| fixture.settings.continue_after_restart = false,
            Refusal::Disabled,
        ),
        (
            "archived",
            |fixture, _| {
                fixture.run(Command::ThreadArchive { thread_id: id(10) });
            },
            Refusal::Archived,
        ),
        (
            "settled",
            |fixture, _| {
                fixture.run(Command::ThreadSettle { thread_id: id(10) });
            },
            Refusal::Settled,
        ),
        (
            "snoozed",
            |fixture, _| {
                fixture.run(Command::ThreadSnooze {
                    thread_id: id(10),
                    until: NOW + 1_000_000,
                });
            },
            Refusal::Snoozed,
        ),
        (
            "compact",
            |fixture, run| {
                let projection = fixture.projection.as_mut().unwrap();
                let message = projection.runs[&run].user_message_id;
                projection.hot_messages.get_mut(&message).unwrap().text = "/compact".into();
            },
            Refusal::Compact,
        ),
        (
            "chain limit",
            |fixture, run| {
                // Three restart continuations already lead to this run.
                let projection = fixture.projection.as_mut().unwrap();
                let mut previous = None;
                for n in 0..3u128 {
                    let id = RunId(Uuid::from_u128(900 + n));
                    let mut earlier = projection.runs[&run].clone();
                    earlier.id = id;
                    earlier.ordinal = None;
                    earlier.status = RunStatus::Interrupted;
                    earlier.continuation = previous.map(|of_run_id| RunContinuation {
                        kind: ContinuationKind::Restart,
                        of_run_id,
                    });
                    projection.apply(&DomainEvent::RunUpdated(earlier));
                    previous = Some(id);
                }
                let mut current = projection.runs[&run].clone();
                current.continuation = Some(RunContinuation {
                    kind: ContinuationKind::Restart,
                    of_run_id: previous.unwrap(),
                });
                projection.apply(&DomainEvent::RunUpdated(current));
            },
            Refusal::ChainLimit,
        ),
    ];
    for (name, setup, refusal) in cases {
        let (mut fixture, run) = live_thread();
        setup(&mut fixture, run);
        let decision = fixture.recover();
        assert_eq!(continue_effect(&decision), None, "{name}");
        assert_eq!(
            fixture.thread().runs[&run].status,
            RunStatus::Interrupted,
            "{name}"
        );
        let notices = fixture.notices(LifecycleNotice::RestartInterrupted);
        assert!(
            notices.is_empty(),
            "{name}: the interrupted run's rows left the hot set"
        );
        let notice = decision
            .events
            .iter()
            .find_map(|event| match event {
                DomainEvent::TurnItemUpdated(item)
                    if matches!(
                        item.kind,
                        TurnItemKind::SystemNotice {
                            lifecycle: Some(LifecycleNotice::RestartInterrupted),
                            ..
                        }
                    ) =>
                {
                    Some(item.clone())
                }
                _ => None,
            })
            .unwrap_or_else(|| panic!("{name}: no notice"));
        assert!(
            notice_text(&notice).contains(refusal.reason()),
            "{name}: {}",
            notice_text(&notice)
        );
    }
}

#[test]
fn a_chain_of_two_is_continued_a_third_time() {
    let (mut fixture, run) = live_thread();
    let projection = fixture.projection.as_mut().unwrap();
    let mut previous = None;
    for n in 0..2u128 {
        let id = RunId(Uuid::from_u128(900 + n));
        let mut earlier = projection.runs[&run].clone();
        earlier.id = id;
        earlier.ordinal = None;
        earlier.status = RunStatus::Interrupted;
        earlier.continuation = previous.map(|of_run_id| RunContinuation {
            kind: ContinuationKind::Restart,
            of_run_id,
        });
        projection.apply(&DomainEvent::RunUpdated(earlier));
        previous = Some(id);
    }
    let mut current = projection.runs[&run].clone();
    current.continuation = Some(RunContinuation {
        kind: ContinuationKind::Restart,
        of_run_id: previous.unwrap(),
    });
    projection.apply(&DomainEvent::RunUpdated(current));
    assert_eq!(continue_effect(&fixture.recover()), Some((run, 3)));
}

#[test]
fn a_run_waiting_on_the_user_is_not_continued_and_its_request_expires() {
    let mut fixture = Fixture::new(Capabilities::default());
    fixture.run(create());
    let run = run_of(&fixture.run(send(100, DispatchMode::Auto)))[0].id;
    fixture.make_live(run, true, false);
    let decision = fixture.recover();
    assert_eq!(continue_effect(&decision), None);
    let projection = fixture.thread();
    assert_eq!(projection.runs[&run].status, RunStatus::Interrupted);
    assert_eq!(
        projection.runtime_requests[&id(800)].status,
        RequestStatus::Expired
    );
}

#[test]
fn a_request_that_survives_restarts_keeps_the_run_waiting() {
    let mut fixture = Fixture::new(Capabilities::default());
    fixture.run(create());
    let run = run_of(&fixture.run(send(100, DispatchMode::Auto)))[0].id;
    fixture.make_live(run, true, true);
    let decision = fixture.recover();
    assert_eq!(continue_effect(&decision), None);
    let projection = fixture.thread();
    assert_eq!(projection.runs[&run].status, RunStatus::Waiting);
    assert_eq!(
        projection.runtime_requests[&id(800)].status,
        RequestStatus::Pending
    );
    assert_eq!(
        projection.provider_sessions[&id(SESSION)].status,
        SessionStatus::Stopped
    );
}

#[test]
fn newer_queued_work_runs_instead_of_a_continuation() {
    let (mut fixture, run) = live_thread();
    let queued = run_of(&fixture.run(send(101, DispatchMode::Auto)))[0].id;
    assert_eq!(fixture.thread().runs[&queued].status, RunStatus::Queued);
    let decision = fixture.recover();
    assert_eq!(continue_effect(&decision), None);
    let projection = fixture.thread();
    assert_eq!(projection.runs[&run].status, RunStatus::Interrupted);
    assert_eq!(projection.runs[&queued].status, RunStatus::Starting);
    assert!(decision.effects.iter().any(|effect| matches!(
        effect,
        Effect::ProviderTurnStart { run_id, .. } if *run_id == queued
    )));
    // A continuation that arrives anyway is refused.
    let command = continuation(&fixture, run);
    assert!(matches!(
        fixture.internal(command),
        Err(CommandError::InvalidState { .. })
    ));
}

#[test]
fn a_held_queue_stays_held_and_an_idle_queue_is_released() {
    // Held: nothing starts.
    let mut fixture = Fixture::new(Capabilities::default());
    fixture.run(create());
    let first = run_of(&fixture.run(send(100, DispatchMode::Auto)))[0].id;
    let second = run_of(&fixture.run(send(101, DispatchMode::Auto)))[0].id;
    fixture.run(Command::RunInterrupt {
        thread_id: id(10),
        run_id: first,
        reason: None,
        hold_queue: true,
    });
    let decision = fixture.recover();
    assert!(decision.effects.is_empty());
    assert_eq!(fixture.thread().runs[&second].status, RunStatus::Queued);

    // Released by hand later; an idle thread with a waiting head starts it.
    let mut fixture = Fixture::new(Capabilities::default());
    fixture.run(create());
    let first = run_of(&fixture.run(send(100, DispatchMode::Auto)))[0].clone();
    let second = run_of(&fixture.run(send(101, DispatchMode::Auto)))[0].id;
    // The first run ended while the daemon died before the queue moved.
    fixture.apply(&[DomainEvent::RunUpdated(Run {
        status: RunStatus::Completed,
        completed_at: Some(NOW),
        ..first
    })]);
    let decision = fixture.recover();
    assert!(matches!(
        decision.effects.as_slice(),
        [Effect::ProviderTurnStart { run_id, .. }] if *run_id == second
    ));
}

#[test]
fn a_run_the_provider_never_accepted_retries_its_attempt() {
    let mut fixture = Fixture::new(Capabilities::default());
    fixture.run(create());
    let run = run_of(&fixture.run(send(100, DispatchMode::Auto)))[0].clone();
    let first_attempt = run.active_attempt_id.unwrap();
    let mut attempts = vec![first_attempt];
    for round in 1..=3 {
        let decision = fixture.recover();
        let current = fixture.thread().runs[&run.id].clone();
        assert_eq!(current.status, RunStatus::Starting, "round {round}");
        let attempt = &fixture.thread().attempts[&current.active_attempt_id.unwrap()];
        assert_eq!(attempt.reason, AttemptReason::ProviderRecovery);
        assert_eq!(attempt.attempt_ordinal, round + 1);
        assert!(matches!(
            decision.effects.as_slice(),
            [Effect::ProviderTurnStart { attempt_id, .. }] if *attempt_id == attempt.id
        ));
        attempts.push(attempt.id);
        // Each recovery is a new boot.
        fixture.ids = SequentialIds::new(100 + round as u64);
    }
    assert_eq!(
        fixture.thread().attempts[&first_attempt].status,
        AttemptStatus::Interrupted
    );
    // A fourth restart gives up.
    let decision = fixture.recover();
    assert!(decision.effects.is_empty());
    assert_eq!(
        fixture.thread().runs[&run.id].status,
        RunStatus::Interrupted
    );
}

#[test]
fn finalizing_runs_and_quiet_threads_need_nothing() {
    let (mut fixture, run) = live_thread();
    let current = fixture.thread().runs[&run].clone();
    fixture.apply(&[DomainEvent::RunUpdated(Run {
        status: RunStatus::Finalizing,
        ..current
    })]);
    let decision = fixture.recover();
    assert!(decision.effects.is_empty());
    assert_eq!(fixture.thread().runs[&run].status, RunStatus::Finalizing);
    // The dead session still stops.
    assert_eq!(
        fixture.thread().provider_sessions[&id(SESSION)].status,
        SessionStatus::Stopped
    );
    // Recovering again changes nothing.
    assert!(fixture.recover().is_empty());
}

#[test]
fn continue_run_is_refused_when_the_thread_moved_on() {
    let (mut fixture, run) = live_thread();
    fixture.recover();
    let command = continuation(&fixture, run);
    fixture.run(Command::ThreadArchive { thread_id: id(10) });
    assert!(matches!(
        fixture.internal(command),
        Err(CommandError::InvalidState { .. })
    ));
}
