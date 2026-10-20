//! Dispatch + commit throughput of the orchestrator core (RFC 0001).
//!
//! - `dispatch_commit/message_queue_10k`: 10,000 `message.dispatch`
//!   commands on one thread through the whole path (receipt lookup, key
//!   lock, decide, one SQLite transaction with events, projections, receipt
//!   and outbox, cache apply, feed publish). Every message after the first
//!   queues, so each command commits a message, a run and a thread snapshot.
//! - `dispatch_commit/text_snapshot_10k`: 10,000 streamed assistant-text
//!   snapshots committed through `ingest_decision` with a `RunCurrent` guard
//!   (the R12 persist path #44 will drive).
//! - `decide/message_dispatch`: `decide` alone against a thread holding
//!   1,000 runs, no storage.
//!
//! Numbers are per batch; divide by 10,000 for one command.

use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use uuid::Uuid;
use waku_orchestrator::{
    AnyCommand, DecideContext, Decision, Dependencies, NoCapabilities, Orchestrator,
    OrchestratorConfig, OrchestratorSettings, SequentialIds, SystemClock, ThreadProjection,
    WriteGuard, decide,
};
use waku_protocol::orchestration::*;

const BATCH: u128 = 10_000;

struct Fixture {
    _dir: tempfile::TempDir,
    orchestrator: Arc<Orchestrator>,
    thread: ThreadId,
}

fn envelope(id: String, command: Command) -> CommandEnvelope {
    CommandEnvelope {
        command_id: CommandId::new(id).unwrap(),
        issued_by: Actor::User,
        source: CreationSource::Desktop,
        command,
    }
}

fn message(thread: ThreadId, message: u128) -> Command {
    Command::MessageDispatch {
        thread_id: thread,
        message_id: MessageId(Uuid::from_u128(message)),
        text: "Refactor the parser so tokens carry spans; keep the public API.".into(),
        display_text: None,
        attachments: Vec::new(),
        context: Vec::new(),
        model_selection: None,
        mode: DispatchMode::QueueAfterActive,
        title_seed: None,
        source_plan: None,
    }
}

fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let orchestrator = Orchestrator::open(
        OrchestratorConfig::new(dir.path().join("state.sqlite")),
        Dependencies {
            clock: Arc::new(SystemClock),
            ids: Box::new(SequentialIds::new(1)),
            capabilities: Arc::new(NoCapabilities),
        },
    )
    .unwrap();
    let project = ProjectId(Uuid::from_u128(1));
    let thread = ThreadId(Uuid::from_u128(2));
    orchestrator
        .dispatch(envelope(
            "project".into(),
            Command::ProjectCreate {
                project_id: project,
                name: "bench".into(),
                path: PathBuf::from("/repo"),
                order_key: None,
            },
        ))
        .unwrap();
    orchestrator
        .dispatch(envelope(
            "thread".into(),
            Command::ThreadCreate {
                thread_id: thread,
                project_id: project,
                title: Some("bench".into()),
                model_selection: ModelSelection::new("codex"),
                runtime_mode: RuntimeMode::FullAccess,
                interaction_mode: InteractionMode::Default,
                workspace: WorkspaceStrategy::Root { branch: None },
                imported_native_thread: None,
            },
        ))
        .unwrap();
    Fixture {
        _dir: dir,
        orchestrator,
        thread,
    }
}

fn message_queue(iterations: u64) -> Duration {
    let mut total = Duration::ZERO;
    for _ in 0..iterations {
        let fixture = fixture();
        let start = Instant::now();
        for n in 0..BATCH {
            fixture
                .orchestrator
                .dispatch(envelope(
                    format!("m{n}"),
                    message(fixture.thread, 1_000 + n),
                ))
                .unwrap();
        }
        total += start.elapsed();
    }
    total
}

fn text_snapshots(iterations: u64) -> Duration {
    let mut total = Duration::ZERO;
    for _ in 0..iterations {
        let fixture = fixture();
        fixture
            .orchestrator
            .dispatch(envelope("first".into(), message(fixture.thread, 1)))
            .unwrap();
        let projection = fixture.orchestrator.thread(fixture.thread).unwrap();
        let run = projection.active_run().unwrap().clone();
        let attempt = run.active_attempt_id.unwrap();
        let block = run.ordinal.unwrap();
        let position = TurnItem::position_of(block, projection.next_position[&block]);
        let message_id = MessageId(Uuid::from_u128(77));
        let mut text = String::new();
        let start = Instant::now();
        for n in 0..BATCH {
            text.push_str("word ");
            if n % 16 == 0 {
                text.push('\n');
            }
            let decision = Decision {
                events: vec![
                    DomainEvent::MessageUpdated(Message {
                        id: message_id,
                        thread_id: fixture.thread,
                        run_id: Some(run.id),
                        node_id: run.root_node_id,
                        role: MessageRole::Assistant,
                        created_by: Actor::Agent,
                        creation_source: CreationSource::Provider,
                        text: text.clone(),
                        display_text: None,
                        attachments: Vec::new(),
                        context: Vec::new(),
                        notification: None,
                        sender_thread_id: None,
                        streaming: true,
                        created_at: 0,
                        updated_at: n as i64,
                    }),
                    DomainEvent::TurnItemUpdated(TurnItem {
                        id: TurnItemId(Uuid::from_u128(78)),
                        thread_id: fixture.thread,
                        run_id: Some(run.id),
                        node_id: run.root_node_id,
                        provider_thread_id: run.provider_thread_id,
                        provider_turn_id: None,
                        native_item_ref: None,
                        parent_item_id: None,
                        position,
                        status: ItemStatus::Running,
                        title: None,
                        tool: None,
                        stream_rev: n as u32,
                        started_at: Some(0),
                        completed_at: None,
                        updated_at: n as i64,
                        kind: TurnItemKind::AssistantMessage {
                            message_id,
                            text: text.clone(),
                            streaming: true,
                        },
                    }),
                ],
                ..Decision::default()
            };
            fixture
                .orchestrator
                .ingest_decision(
                    fixture.thread,
                    decision,
                    WriteGuard::RunCurrent {
                        run_id: run.id,
                        attempt_id: attempt,
                    },
                )
                .unwrap();
            // Keep the snapshot bounded like R12's block-boundary persists.
            if text.len() > 4_096 {
                text.clear();
            }
        }
        total += start.elapsed();
    }
    total
}

fn decide_only(criterion: &mut Criterion) {
    // A thread with 1,000 queued runs, decided against in memory.
    let fixture = fixture();
    for n in 0..1_000 {
        fixture
            .orchestrator
            .dispatch(envelope(
                format!("seed{n}"),
                message(fixture.thread, 50_000 + n),
            ))
            .unwrap();
    }
    let projection: Arc<ThreadProjection> = fixture.orchestrator.thread(fixture.thread).unwrap();
    let project = fixture.orchestrator.projects().remove(0);
    let settings = OrchestratorSettings::default();
    let mut ids = SequentialIds::new(5);
    let mut n = 0u128;
    criterion.bench_function("decide/message_dispatch", |bencher| {
        bencher.iter(|| {
            n += 1;
            let command = AnyCommand::Client(envelope(
                "decide".into(),
                message(fixture.thread, 900_000 + n),
            ));
            let mut cx = DecideContext {
                now: 1,
                ids: &mut ids,
                capabilities: &NoCapabilities,
                settings: &settings,
                thread: Some(&projection),
                related: None,
                project: Some(&project),
            };
            black_box(decide(&mut cx, &command).unwrap())
        })
    });
}

fn dispatch_commit(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("dispatch_commit");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));
    group.bench_function("message_queue_10k", |bencher| {
        bencher.iter_custom(message_queue)
    });
    group.bench_function("text_snapshot_10k", |bencher| {
        bencher.iter_custom(text_snapshots)
    });
    group.finish();
}

criterion_group!(benches, dispatch_commit, decide_only);
criterion_main!(benches);
