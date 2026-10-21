//! Unit tests of `decide` with hand-built contexts: no database, no clock,
//! no threads (#39 acceptance: decide is pure).

use std::path::PathBuf;

use uuid::Uuid;

use super::*;
use crate::command::{AnyCommand, CommandError};
use crate::effects::{Effect, EffectType};
use crate::fakes::StaticCapabilities;
use crate::ids::SequentialIds;
use crate::projection::ThreadProjection;

/// RFC 0001 "Testing": the `decide` module does not import storage,
/// filesystem, network, threads or the orchestrator. Every `use` line of
/// every file in `src/decide/` is checked.
#[test]
fn decide_reaches_no_io() {
    const SOURCES: &[(&str, &str)] = &[
        ("mod.rs", include_str!("mod.rs")),
        ("draft.rs", include_str!("draft.rs")),
        ("internal.rs", include_str!("internal.rs")),
        ("project.rs", include_str!("project.rs")),
        ("recovery.rs", include_str!("recovery.rs")),
        ("request.rs", include_str!("request.rs")),
        ("run.rs", include_str!("run.rs")),
        ("thread.rs", include_str!("thread.rs")),
    ];
    const FORBIDDEN: &[&str] = &[
        "rusqlite",
        "std::fs",
        "std::net",
        "std::io",
        "std::process",
        "std::thread",
        "std::time::SystemTime",
        "std::time::Instant",
        "crate::store",
        "crate::orchestrator",
        "crate::feed",
        "crate::clock",
        "crate::effects::worker",
        "crate::effects::executor",
        "crossbeam",
        "parking_lot",
    ];
    for (file, source) in SOURCES {
        for line in source.lines().map(str::trim) {
            if !(line.starts_with("use ")
                || line.starts_with("pub use ")
                || line.starts_with("pub(crate) use "))
            {
                continue;
            }
            for forbidden in FORBIDDEN {
                assert!(
                    !line.contains(forbidden),
                    "src/decide/{file} imports {forbidden}: {line}"
                );
            }
        }
        // Nothing reaches I/O through a fully qualified path either.
        for forbidden in ["std::fs::", "std::net::", "rusqlite::", "std::thread::"] {
            assert!(
                !source.contains(forbidden),
                "src/decide/{file} mentions {forbidden}"
            );
        }
    }
}

const NOW: Timestamp = 1_000;

fn id<T: From<Uuid>>(n: u128) -> T {
    T::from(Uuid::from_u128(n))
}

fn project() -> Project {
    Project {
        id: id(1),
        name: "P".into(),
        path: PathBuf::from("/repo"),
        order_key: OrderKey::new("a"),
        created_at: 0,
        updated_at: 0,
        removed_at: None,
    }
}

fn envelope(command: Command) -> AnyCommand {
    AnyCommand::Client(CommandEnvelope {
        command_id: CommandId::new("c").unwrap(),
        issued_by: Actor::User,
        source: CreationSource::Desktop,
        command,
    })
}

struct Fixture {
    projection: Option<ThreadProjection>,
    ids: SequentialIds,
    capabilities: StaticCapabilities,
    settings: OrchestratorSettings,
    project: Project,
    now: Timestamp,
}

impl Fixture {
    fn new(capabilities: Capabilities) -> Self {
        Self {
            projection: None,
            ids: SequentialIds::new(9),
            capabilities: StaticCapabilities::with("codex", capabilities),
            settings: OrchestratorSettings::default(),
            project: project(),
            now: NOW,
        }
    }

    fn decide(&mut self, command: Command) -> Result<Decision, CommandError> {
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
        decide(&mut cx, &envelope(command))
    }

    /// Decides and applies, like a commit would.
    fn run(&mut self, command: Command) -> Decision {
        let decision = self.decide(command).expect("accepted");
        for event in &decision.events {
            if let DomainEvent::ThreadUpdated(update) = event
                && self.projection.is_none()
            {
                self.projection = Some(ThreadProjection::new(update.thread.clone()));
            }
            if let Some(projection) = self.projection.as_mut() {
                projection.apply(event);
            }
        }
        decision
    }

    fn thread(&self) -> &ThreadProjection {
        self.projection.as_ref().unwrap()
    }
}

fn create() -> Command {
    Command::ThreadCreate {
        thread_id: id(10),
        project_id: id(1),
        title: None,
        model_selection: ModelSelection::new("codex"),
        runtime_mode: RuntimeMode::FullAccess,
        interaction_mode: InteractionMode::Default,
        workspace: WorkspaceStrategy::Root { branch: None },
        imported_native_thread: None,
    }
}

fn send(message: u128, mode: DispatchMode) -> Command {
    Command::MessageDispatch {
        thread_id: id(10),
        message_id: id(message),
        text: "Fix the bug\nplease".into(),
        display_text: None,
        attachments: Vec::new(),
        context: Vec::new(),
        model_selection: None,
        mode,
        title_seed: None,
        source_plan: None,
    }
}

fn run_of(decision: &Decision) -> Vec<&Run> {
    decision
        .events
        .iter()
        .filter_map(|event| match event {
            DomainEvent::RunUpdated(run) => Some(run),
            _ => None,
        })
        .collect()
}

#[test]
fn the_first_message_starts_a_run_with_ordinal_one_and_a_title() {
    let mut fixture = Fixture::new(Capabilities::default());
    fixture.run(create());
    let decision = fixture.run(send(100, DispatchMode::Auto));

    let types = decision
        .events
        .iter()
        .map(DomainEvent::event_type)
        .collect::<Vec<_>>();
    assert_eq!(
        types,
        [
            "thread.updated",
            "provider_thread.updated",
            "run.updated",
            "run_attempt.updated",
            "node.updated",
            "checkpoint_scope.updated",
            "message.updated",
            "turn_item.updated",
        ]
    );
    let run = run_of(&decision)[0];
    assert_eq!(run.status, RunStatus::Starting);
    assert_eq!(run.ordinal, Some(1));
    let item = fixture.thread().hot_items.values().next().unwrap();
    assert_eq!(item.position, TurnItem::position_of(1, 0), "R15");
    assert!(matches!(
        item.kind,
        TurnItemKind::UserMessage {
            input_intent: InputIntent::TurnStart,
            ..
        }
    ));
    assert_eq!(fixture.thread().thread.title, "Fix the bug");
    assert_eq!(fixture.thread().thread.title_source, TitleSource::Prompt);
    let effects = decision
        .effects
        .iter()
        .map(Effect::effect_type)
        .collect::<Vec<_>>();
    assert_eq!(
        effects,
        [
            EffectType::ProviderTurnStart,
            EffectType::ThreadTitleGenerate
        ]
    );
}

#[test]
fn decide_is_deterministic_for_equal_inputs() {
    let decide_twice = || {
        let mut fixture = Fixture::new(Capabilities::default());
        fixture.run(create());
        fixture.run(send(100, DispatchMode::Auto));
        fixture.run(send(101, DispatchMode::Auto))
    };
    assert_eq!(decide_twice(), decide_twice());
}

#[test]
fn a_busy_thread_queues_and_the_queue_advances_when_the_run_ends() {
    let mut fixture = Fixture::new(Capabilities::default());
    fixture.run(create());
    let first = run_of(&fixture.run(send(100, DispatchMode::Auto)))[0].id;
    let queued = fixture.run(send(101, DispatchMode::Auto));
    let second = run_of(&queued)[0].clone();
    assert_eq!(second.status, RunStatus::Queued);
    assert_eq!(second.ordinal, None, "R14: no ordinal while queued");
    assert_eq!(second.queue_position, Some(1));
    assert!(queued.effects.is_empty());
    let third = run_of(&fixture.run(send(102, DispatchMode::QueueAfterActive)))[0].clone();
    assert_eq!(third.queue_position, Some(2));

    // The provider never accepted run 1: the interrupt ends it directly,
    // cancels its pending start and starts the head of the queue.
    let decision = fixture.run(Command::RunInterrupt {
        thread_id: id(10),
        run_id: first,
        reason: None,
        hold_queue: false,
    });
    assert_eq!(
        decision.cancel_unsettled[0].effect_types,
        [
            EffectType::ProviderTurnStart,
            EffectType::ProviderTurnRestart
        ]
    );
    let runs = fixture.thread().runs.clone();
    assert_eq!(runs[&first].status, RunStatus::Interrupted);
    assert_eq!(runs[&second.id].status, RunStatus::Starting);
    assert_eq!(runs[&second.id].ordinal, Some(2));
    assert_eq!(runs[&third.id].status, RunStatus::Queued);
    assert!(matches!(
        decision.effects.as_slice(),
        [Effect::ProviderTurnStart { run_id, .. }] if *run_id == second.id
    ));
    // The queued user message became a row in run 2's block.
    assert!(fixture.thread().hot_items.values().any(|item| item.position
        == TurnItem::position_of(2, 0)
        && matches!(
            item.kind,
            TurnItemKind::UserMessage {
                input_intent: InputIntent::QueuedTurn,
                ..
            }
        )));
}

#[test]
fn a_held_queue_waits_for_resume() {
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
    assert_eq!(fixture.thread().runs[&second].status, RunStatus::Queued);
    assert!(fixture.thread().runs[&second].queue_held);
    fixture.run(Command::QueueResume { thread_id: id(10) });
    assert_eq!(fixture.thread().runs[&second].status, RunStatus::Starting);
}

#[test]
fn steering_follows_capabilities() {
    let mut capabilities = Capabilities::default();
    capabilities.turns.active_steering = true;
    let mut fixture = Fixture::new(capabilities);
    fixture.run(create());
    let run = run_of(&fixture.run(send(100, DispatchMode::Auto)))[0].clone();
    // Before the provider accepted the turn, a steer waits in the queue.
    let early = fixture.run(send(
        101,
        DispatchMode::SteerActive {
            target_run_id: run.id,
        },
    ));
    assert_eq!(run_of(&early)[0].status, RunStatus::Queued);

    // Simulate the driver: a live turn on a live session.
    let projection = fixture.projection.as_mut().unwrap();
    let attempt_id = run.active_attempt_id.unwrap();
    let attempt = projection.attempts.get_mut(&attempt_id).unwrap();
    attempt.provider_turn_id = Some(id(500));
    let provider_thread = projection
        .provider_threads
        .get_mut(&attempt.provider_thread_id)
        .unwrap();
    provider_thread.provider_session_id = Some(id(600));
    projection.runs.get_mut(&run.id).unwrap().status = RunStatus::Running;

    let steer = fixture.run(send(
        102,
        DispatchMode::SteerActive {
            target_run_id: run.id,
        },
    ));
    assert!(matches!(
        steer.effects.as_slice(),
        [Effect::ProviderTurnSteer { provider_turn_id, .. }] if *provider_turn_id == id(500)
    ));
    assert!(run_of(&steer).is_empty(), "a steer creates no run");
    // Auto steers too when the provider supports it.
    let auto = fixture.run(send(103, DispatchMode::Auto));
    assert_eq!(auto.effects[0].effect_type(), EffectType::ProviderTurnSteer);

    // Without native steering, the run restarts with a new attempt.
    fixture
        .capabilities
        .0
        .lock()
        .get_mut(&ProviderInstanceId::new("codex"))
        .unwrap()
        .turns
        .active_steering = false;
    fixture
        .capabilities
        .0
        .lock()
        .get_mut(&ProviderInstanceId::new("codex"))
        .unwrap()
        .turns
        .interrupt = true;
    let restart = fixture.run(send(
        104,
        DispatchMode::SteerActive {
            target_run_id: run.id,
        },
    ));
    let Effect::ProviderTurnRestart {
        interrupted_attempt_id,
        next_attempt_id,
        ..
    } = &restart.effects[0]
    else {
        panic!("expected a restart: {:?}", restart.effects);
    };
    assert_eq!(*interrupted_attempt_id, attempt_id);
    let attempts = &fixture.thread().attempts;
    assert_eq!(attempts[&attempt_id].status, AttemptStatus::Superseded);
    assert_eq!(
        attempts[next_attempt_id].reason,
        AttemptReason::SteeringRestart
    );
    assert_eq!(attempts[next_attempt_id].attempt_ordinal, 2);
}

#[test]
fn invalid_commands_are_rejected_without_events() {
    let mut fixture = Fixture::new(Capabilities::default());
    assert!(matches!(
        fixture.decide(send(100, DispatchMode::Auto)),
        Err(CommandError::NotFound { .. })
    ));
    fixture.run(create());
    assert!(matches!(
        fixture.decide(create()),
        Err(CommandError::Conflict { .. })
    ));
    fixture.run(send(100, DispatchMode::Auto));
    assert!(matches!(
        fixture.decide(send(100, DispatchMode::Auto)),
        Err(CommandError::Conflict { .. })
    ));
    assert!(matches!(
        fixture.decide(send(101, DispatchMode::StartImmediately)),
        Err(CommandError::InvalidState { .. })
    ));
    assert!(matches!(
        fixture.decide(Command::QueuedRunReorder {
            thread_id: id(10),
            run_id: id(5),
            before_run_id: None,
        }),
        Err(CommandError::Unsupported { .. })
    ));
    let empty = Command::MessageDispatch {
        thread_id: id(10),
        message_id: id(200),
        text: "   ".into(),
        display_text: None,
        attachments: Vec::new(),
        context: Vec::new(),
        model_selection: None,
        mode: DispatchMode::Auto,
        title_seed: None,
        source_plan: None,
    };
    assert!(matches!(
        fixture.decide(empty),
        Err(CommandError::Invalid { .. })
    ));
}

#[test]
fn ids_are_derived_from_stable_inputs() {
    let mut first = Fixture::new(Capabilities::default());
    first.run(create());
    let a = first.run(send(100, DispatchMode::Auto));
    // A different id source changes only fresh ids (the provider thread),
    // never the ids derived from the message id.
    let mut second = Fixture::new(Capabilities::default());
    second.ids = SequentialIds::new(77);
    second.run(create());
    let b = second.run(send(100, DispatchMode::Auto));
    assert_eq!(run_of(&a)[0].id, run_of(&b)[0].id);
    assert_eq!(
        run_of(&a)[0].active_attempt_id,
        run_of(&b)[0].active_attempt_id
    );
    assert_ne!(
        run_of(&a)[0].provider_thread_id,
        run_of(&b)[0].provider_thread_id
    );
}

#[test]
fn lifecycle_commands_are_idempotent_in_effect() {
    let mut fixture = Fixture::new(Capabilities::default());
    fixture.run(create());
    let archive = fixture.run(Command::ThreadArchive { thread_id: id(10) });
    assert_eq!(archive.events.len(), 1);
    assert_eq!(archive.effects, [Effect::TerminalCleanup]);
    let again = fixture.run(Command::ThreadArchive { thread_id: id(10) });
    assert!(again.is_empty(), "archiving twice changes nothing");
    let visit = fixture.run(Command::ThreadVisit {
        thread_id: id(10),
        visited_at: 5,
    });
    assert!(visit.is_empty(), "visits are monotonic");
    let rename = fixture.run(Command::ThreadRename {
        thread_id: id(10),
        title: "  Mine  ".into(),
    });
    let DomainEvent::ThreadUpdated(update) = &rename.events[0] else {
        panic!()
    };
    assert_eq!(update.change, ThreadChange::Renamed);
    assert_eq!(update.thread.title, "Mine");
    assert_eq!(update.thread.title_source, TitleSource::User);
}

mod recovery;
