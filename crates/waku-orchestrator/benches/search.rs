//! Thread search over the FTS5 index (#65): the bench corpus is 10,000
//! threads in 10 projects, each with a title, four user messages, four
//! assistant replies and a todo list (100,000 documents, ~5.5 M words drawn
//! from a 4,000-word Zipf vocabulary), committed through the sink so the
//! index is the one `apply_sql` maintains.
//!
//! - `search/threads_10k/<query>`: `search_threads` with the default limit
//!   (50), snippets included.
//! - `search/find_5k_items/<query>`: `find_in_thread` on one thread holding
//!   5,000 messages, none of them in memory.
//!
//! The acceptance budget is < 50 ms per search.

use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use criterion::{Criterion, criterion_group, criterion_main};
use uuid::Uuid;
use waku_orchestrator::search::{SearchThreadsQuery, ThreadFindQuery};
use waku_orchestrator::{
    Decision, Dependencies, NoCapabilities, Orchestrator, OrchestratorConfig, SequentialIds,
    SystemClock, WriteGuard,
};
use waku_protocol::orchestration::*;

const THREADS: u128 = 10_000;
const PROJECTS: u128 = 10;
const VOCABULARY: usize = 4_000;
const FIND_ITEMS: u32 = 5_000;

/// A deterministic xorshift generator.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// Pronounceable synthetic words; index 0 is the most frequent.
fn vocabulary() -> Vec<String> {
    const SYLLABLES: [&str; 20] = [
        "ka", "lo", "mi", "ten", "ra", "vu", "sel", "po", "dan", "qui", "ber", "xo", "ny", "fa",
        "gor", "hel", "ji", "mon", "pra", "zu",
    ];
    (0..VOCABULARY)
        .map(|index| {
            let mut word = String::new();
            let mut n = index + 20;
            while n > 0 {
                word.push_str(SYLLABLES[n % SYLLABLES.len()]);
                n /= SYLLABLES.len();
            }
            word
        })
        .collect()
}

/// Zipf(1) sampling by inverse CDF over a precomputed table.
struct Words {
    words: Vec<String>,
    cdf: Vec<f64>,
}

impl Words {
    fn new() -> Self {
        let words = vocabulary();
        let total: f64 = (1..=words.len()).map(|rank| 1.0 / rank as f64).sum();
        let mut running = 0.0;
        let cdf = (1..=words.len())
            .map(|rank| {
                running += 1.0 / rank as f64 / total;
                running
            })
            .collect();
        Self { words, cdf }
    }

    fn sentence(&self, rng: &mut Rng, length: usize) -> String {
        let mut text = String::new();
        for n in 0..length {
            let pick = rng.unit();
            let index = self.cdf.partition_point(|value| *value < pick);
            if n > 0 {
                text.push(' ');
            }
            text.push_str(&self.words[index.min(self.words.len() - 1)]);
        }
        text
    }
}

fn envelope(id: String, command: Command) -> CommandEnvelope {
    CommandEnvelope {
        command_id: CommandId::new(id).unwrap(),
        issued_by: Actor::User,
        source: CreationSource::Desktop,
        command,
    }
}

fn item(thread_id: ThreadId, index: u32, kind: TurnItemKind) -> TurnItem {
    TurnItem {
        id: TurnItemId(Uuid::new_v4()),
        thread_id,
        run_id: None,
        node_id: None,
        provider_thread_id: None,
        provider_turn_id: None,
        native_item_ref: None,
        parent_item_id: None,
        position: TurnItem::position_of(0, index),
        status: ItemStatus::Completed,
        title: None,
        tool: None,
        stream_rev: 0,
        started_at: None,
        completed_at: None,
        updated_at: 1,
        kind,
    }
}

fn user(thread_id: ThreadId, index: u32, text: String) -> DomainEvent {
    DomainEvent::TurnItemUpdated(item(
        thread_id,
        index,
        TurnItemKind::UserMessage {
            message_id: MessageId(Uuid::new_v4()),
            input_intent: InputIntent::TurnStart,
            text,
            display_text: None,
            attachments: Vec::new(),
            created_by: Actor::User,
            sender_thread_id: None,
        },
    ))
}

fn assistant(thread_id: ThreadId, index: u32, text: String) -> DomainEvent {
    DomainEvent::TurnItemUpdated(item(
        thread_id,
        index,
        TurnItemKind::AssistantMessage {
            message_id: MessageId(Uuid::new_v4()),
            text,
            streaming: false,
        },
    ))
}

struct Corpus {
    _dir: tempfile::TempDir,
    orchestrator: Arc<Orchestrator>,
    long_thread: ThreadId,
}

fn corpus() -> Corpus {
    let started = Instant::now();
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
    let words = Words::new();
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    for project in 0..PROJECTS {
        orchestrator
            .dispatch(envelope(
                format!("project-{project}"),
                Command::ProjectCreate {
                    project_id: ProjectId(Uuid::from_u128(1 + project)),
                    name: format!("project {project}"),
                    path: PathBuf::from(format!("/repo/{project}")),
                    order_key: None,
                },
            ))
            .unwrap();
    }
    for thread in 0..THREADS {
        let thread_id = ThreadId(Uuid::from_u128(1_000 + thread));
        orchestrator
            .dispatch(envelope(
                format!("thread-{thread}"),
                Command::ThreadCreate {
                    thread_id,
                    project_id: ProjectId(Uuid::from_u128(1 + thread % PROJECTS)),
                    title: Some(words.sentence(&mut rng, 5)),
                    model_selection: ModelSelection::new("codex"),
                    runtime_mode: RuntimeMode::FullAccess,
                    interaction_mode: InteractionMode::Default,
                    workspace: WorkspaceStrategy::Root { branch: None },
                    imported_native_thread: None,
                },
            ))
            .unwrap();
        let mut events = Vec::with_capacity(9);
        for turn in 0..4u32 {
            events.push(user(thread_id, turn * 2, words.sentence(&mut rng, 25)));
            events.push(assistant(
                thread_id,
                turn * 2 + 1,
                words.sentence(&mut rng, 110),
            ));
        }
        events.push(DomainEvent::TurnItemUpdated(item(
            thread_id,
            8,
            TurnItemKind::TodoList {
                plan_id: PlanId(Uuid::new_v4()),
                steps: (0..4)
                    .map(|step| PlanStep {
                        id: step.to_string(),
                        text: words.sentence(&mut rng, 8),
                        status: PlanStepStatus::Pending,
                    })
                    .collect(),
                explanation: None,
            },
        )));
        orchestrator
            .ingest_decision(
                thread_id,
                Decision {
                    events,
                    ..Decision::default()
                },
                WriteGuard::None,
            )
            .unwrap();
    }
    // One long thread for in-thread find.
    let long_thread = ThreadId(Uuid::from_u128(1_000 + THREADS));
    orchestrator
        .dispatch(envelope(
            "thread-long".into(),
            Command::ThreadCreate {
                thread_id: long_thread,
                project_id: ProjectId(Uuid::from_u128(1)),
                title: Some("long".into()),
                model_selection: ModelSelection::new("codex"),
                runtime_mode: RuntimeMode::FullAccess,
                interaction_mode: InteractionMode::Default,
                workspace: WorkspaceStrategy::Root { branch: None },
                imported_native_thread: None,
            },
        ))
        .unwrap();
    for chunk in 0..FIND_ITEMS / 500 {
        let events = (0..500)
            .map(|n| {
                let index = chunk * 500 + n;
                assistant(long_thread, index, words.sentence(&mut rng, 60))
            })
            .collect();
        orchestrator
            .ingest_decision(
                long_thread,
                Decision {
                    events,
                    ..Decision::default()
                },
                WriteGuard::None,
            )
            .unwrap();
    }
    let documents: i64 = orchestrator.reader().with(|connection| {
        connection
            .query_row("SELECT COUNT(*) FROM search_docs", [], |row| row.get(0))
            .unwrap()
    });
    eprintln!(
        "search corpus: {THREADS} threads, {documents} documents, built in {:.1?}",
        started.elapsed()
    );
    Corpus {
        _dir: dir,
        orchestrator,
        long_thread,
    }
}

fn bench(c: &mut Criterion) {
    let corpus = corpus();
    let words = vocabulary();
    // Frequent (rank 2), mid-frequency (rank 200), rare (rank 3,500), two
    // words, and a two-letter prefix.
    let queries = [
        ("frequent", words[2].clone()),
        ("medium", words[200].clone()),
        ("rare", words[3_500].clone()),
        ("two_words", format!("{} {}", words[50], words[120])),
        ("prefix", words[300][..2].to_owned()),
    ];
    let mut group = c.benchmark_group("search");
    for (name, text) in &queries {
        let query = SearchThreadsQuery::new(text.clone());
        let found = corpus.orchestrator.search_threads(&query).unwrap().len();
        eprintln!("threads_10k/{name}: {text:?} -> {found} threads");
        group.bench_function(format!("threads_10k/{name}"), |b| {
            b.iter(|| black_box(corpus.orchestrator.search_threads(&query).unwrap()))
        });
    }
    for (name, text) in &queries {
        let query = ThreadFindQuery::new(corpus.long_thread, text.clone());
        let found = corpus.orchestrator.find_in_thread(&query).unwrap();
        eprintln!(
            "find_5k_items/{name}: {text:?} -> {} matches",
            found.matches.len()
        );
        group.bench_function(format!("find_5k_items/{name}"), |b| {
            b.iter(|| black_box(corpus.orchestrator.find_in_thread(&query).unwrap()))
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20);
    targets = bench
}
criterion_main!(benches);
