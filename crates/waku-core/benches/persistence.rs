//! The daemon's `SaveTaskState` write: `StateStore::save` for one dirty
//! session holding a 2,000-message transcript, against a real SQLite file in
//! the system temp directory.
//!
//! - `initial` writes the whole transcript into an already-open, empty store
//!   (the first save after a session is adopted).
//! - `streaming` is the steady state while a turn streams: the last message
//!   changed, so the fingerprint skip should keep the write to one message row
//!   plus the session detail.

use std::hint::black_box;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use uuid::Uuid;
use waku_core::model::{AgentSession, AgentTurn, Message, MessageRole, ProviderKind, TurnStatus};
use waku_core::persistence::{PersistedState, StateStore};

const MESSAGES: usize = 2_000;

/// A scratch directory removed on drop, so no run leaves a database behind.
struct ScratchDir(PathBuf);

impl ScratchDir {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "waku-bench-persistence-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create bench scratch directory");
        Self(path)
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// One project and one started session with `MESSAGES` messages across
/// `MESSAGES / 2` completed turns of realistic Markdown length.
fn state_with_long_session(cwd: PathBuf) -> (PersistedState, Uuid) {
    let mut state = PersistedState::fresh(cwd);
    let project_id = state.projects[0].id;
    let mut session = AgentSession::new(project_id, ProviderKind::Codex);
    session.title = "Benchmark session".to_owned();
    let paragraph = "The parser streams each delta into a settled prefix and reparses only \
                     the tail, so a frame stays within budget while the agent writes. ";
    for turn_count in 0..MESSAGES / 2 {
        let turn_id = Uuid::new_v4();
        session.turns.push(AgentTurn {
            id: turn_id,
            turn_count,
            status: TurnStatus::Completed,
            provider_turn_started: true,
            provider_resume_at: None,
            started_at: 1_700_000_000 + turn_count as u64 * 60,
            completed_at: Some(1_700_000_030 + turn_count as u64 * 60),
            checkpoint: None,
        });
        session.messages.push(Message::new_for_turn(
            MessageRole::User,
            format!("Please refactor module {turn_count} and keep the tests green."),
            turn_id,
        ));
        session.messages.push(Message::new_for_turn(
            MessageRole::Assistant,
            paragraph.repeat(2 + turn_count % 12),
            turn_id,
        ));
    }
    let session_id = session.id;
    state.sessions = Vec::new();
    state.push_session(session);
    (state, session_id)
}

fn persistence(c: &mut Criterion) {
    let mut group = c.benchmark_group("persistence");
    group.sample_size(10);

    group.bench_function("save_task_state_2k_messages_initial", |b| {
        b.iter_batched(
            || {
                let dir = ScratchDir::new();
                let store = StateStore::daemon(dir.0.join("app.db"));
                // Open the connection and run migrations outside the timing,
                // so the figure is the transcript write itself.
                store
                    .save(&mut PersistedState::fresh(dir.0.clone()))
                    .expect("open bench store");
                let (state, _) = state_with_long_session(dir.0.clone());
                (dir, store, state)
            },
            |(dir, store, mut state)| {
                store.save(&mut state).expect("save task state");
                // Dropped after timing: closing SQLite and removing the file.
                (dir, store, state)
            },
            BatchSize::PerIteration,
        );
    });

    let dir = ScratchDir::new();
    let store = StateStore::daemon(dir.0.join("app.db"));
    let (mut state, session_id) = state_with_long_session(dir.0.clone());
    store.save(&mut state).expect("seed task state");
    let mut flip = false;
    group.bench_function("save_task_state_2k_messages_streaming", |b| {
        b.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                // A streamed delta changed the newest message; alternate between
                // two bodies so the transcript size stays constant.
                flip = !flip;
                let session = state.session_mut(session_id).expect("bench session");
                let last = session.messages.last_mut().expect("bench message");
                last.content
                    .truncate(last.content.trim_end_matches(" …").len());
                if flip {
                    last.content.push_str(" …");
                }
                let start = Instant::now();
                store.save(black_box(&mut state)).expect("save task state");
                elapsed += start.elapsed();
            }
            elapsed
        });
    });
    group.finish();
    drop(store);
    drop(dir);
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .noise_threshold(0.03);
    targets = persistence
}
criterion_main!(benches);
