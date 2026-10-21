//! Daemon-scheduled sweeps of the blob and attachment stores (#53, #55).
//!
//! v7 clients asked for a sweep (`SweepBlobs`) after deleting a session. The
//! daemon now schedules sweeps itself, and a sweep keeps everything that is
//! still referenced:
//!
//! - the orchestrator's turn items and messages of threads that are not
//!   deleted ([`orchestrator_references`]);
//! - the composer drafts, keyed by orchestration project and thread ids;
//! - until v7 is deleted (D6), the legacy sessions of `app.db`, which the
//!   desktop still writes ([`crate::persistence::AssetSweep`]).
//!
//! Scheduling is cheap: one thread sleeps on a condition variable until a
//! sweep is due, and never wakes otherwise. A sweep is requested
//!
//! - once after startup ([`SweepTimes::startup`]);
//! - after a blob or attachment is stored, once its grace period
//!   ([`crate::persistence::ASSET_SWEEP_GRACE_PERIOD`]) has passed, so an
//!   upload that nothing came to reference is reclaimed
//!   ([`SweepTimes::after_store`]);
//! - after something released references: a thread deleted or rolled back,
//!   or a v7 `SweepBlobs` ([`SweepTimes::after_release`]).
//!
//! Draft edits request nothing: a draft is cleared on every send, and the
//! attachments a discarded draft held were stored first, so the sweep after
//! their store reclaims them.
//!
//! Requests coalesce into one due time (the earliest), and sweeps are at
//! least [`SweepTimes::min_interval`] apart, so a burst of requests costs one
//! sweep.

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use waku_orchestrator::Orchestrator;

use crate::persistence::{ASSET_SWEEP_GRACE_PERIOD, AssetReferences};

/// When sweeps run, relative to what asked for them.
#[derive(Clone, Copy, Debug)]
pub struct SweepTimes {
    /// The first sweep after the daemon starts.
    pub startup: Duration,
    /// After a store: past the grace period, so the new blob can go.
    pub after_store: Duration,
    /// After references were released.
    pub after_release: Duration,
    /// The least time between two sweeps.
    pub min_interval: Duration,
}

impl Default for SweepTimes {
    fn default() -> Self {
        Self {
            startup: Duration::from_secs(5 * 60),
            after_store: ASSET_SWEEP_GRACE_PERIOD + Duration::from_secs(60),
            after_release: Duration::from_secs(60),
            min_interval: Duration::from_secs(10 * 60),
        }
    }
}

/// Why a sweep is requested.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SweepReason {
    /// A blob or attachment was stored.
    Stored,
    /// References were released.
    Released,
}

#[derive(Default)]
struct State {
    due: Option<Instant>,
    last_run: Option<Instant>,
    stopped: bool,
}

struct Shared {
    state: Mutex<State>,
    wake: Condvar,
    times: SweepTimes,
}

impl Shared {
    fn request_at(&self, due: Instant) {
        let mut state = self.state.lock();
        if state.stopped {
            return;
        }
        let due = match state.last_run {
            Some(last) => due.max(last + self.times.min_interval),
            None => due,
        };
        if state.due.is_none_or(|current| due < current) {
            state.due = Some(due);
            self.wake.notify_one();
        }
    }
}

/// Asks the [`BlobSweeper`] for a sweep. Cloned into whatever stores or
/// releases references.
#[derive(Clone)]
pub struct SweepHandle(Arc<Shared>);

impl SweepHandle {
    pub fn request(&self, reason: SweepReason) {
        let delay = match reason {
            SweepReason::Stored => self.0.times.after_store,
            SweepReason::Released => self.0.times.after_release,
        };
        self.0.request_at(Instant::now() + delay);
    }
}

impl std::fmt::Debug for SweepHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SweepHandle")
            .finish_non_exhaustive()
    }
}

/// The thread that runs sweeps when they are due.
pub struct BlobSweeper {
    shared: Arc<Shared>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl BlobSweeper {
    /// Starts the sweeper with the first sweep [`SweepTimes::startup`] from
    /// now. `sweep` runs on the sweeper's thread.
    pub fn start(times: SweepTimes, mut sweep: impl FnMut() + Send + 'static) -> Self {
        let shared = Arc::new(Shared {
            state: Mutex::new(State::default()),
            wake: Condvar::new(),
            times,
        });
        shared.request_at(Instant::now() + times.startup);
        let worker = shared.clone();
        let thread = std::thread::Builder::new()
            .name("waku-blob-sweep".into())
            .spawn(move || {
                let mut state = worker.state.lock();
                loop {
                    if state.stopped {
                        return;
                    }
                    match state.due {
                        None => worker.wake.wait(&mut state),
                        Some(due) if due <= Instant::now() => {
                            state.due = None;
                            state.last_run = Some(Instant::now());
                            drop(state);
                            sweep();
                            state = worker.state.lock();
                        }
                        Some(due) => {
                            worker.wake.wait_until(&mut state, due);
                        }
                    }
                }
            })
            .expect("the blob sweep thread starts");
        Self {
            shared,
            thread: Some(thread),
        }
    }

    pub fn handle(&self) -> SweepHandle {
        SweepHandle(self.shared.clone())
    }

    /// Stops the thread; a sweep in progress finishes first.
    pub fn shutdown(mut self) {
        self.stop();
    }

    fn stop(&mut self) {
        self.shared.state.lock().stopped = true;
        self.shared.wake.notify_one();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for BlobSweeper {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Every blob and attachment reference in the turn items and messages of
/// threads that are not deleted. Reads raw payloads, so a reference counts
/// wherever it is nested.
pub fn orchestrator_references(orchestrator: &Orchestrator) -> anyhow::Result<AssetReferences> {
    const QUERY: &str = "SELECT i.payload_json FROM p_turn_items i
           JOIN p_threads t ON t.thread_id = i.thread_id
          WHERE t.deleted_at IS NULL
            AND (instr(i.payload_json, 'waku-blob:') > 0
                 OR instr(i.payload_json, 'waku-attachment:') > 0)
         UNION ALL
         SELECT m.payload_json FROM p_messages m
           JOIN p_threads t ON t.thread_id = m.thread_id
          WHERE t.deleted_at IS NULL
            AND (instr(m.payload_json, 'waku-blob:') > 0
                 OR instr(m.payload_json, 'waku-attachment:') > 0)";
    let references: rusqlite::Result<AssetReferences> = orchestrator.reader().with(|connection| {
        let mut references = AssetReferences::default();
        let mut statement = connection.prepare(QUERY)?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            references.collect(&row.get::<_, String>(0)?);
        }
        Ok(references)
    });
    Ok(references?)
}

/// Requests a sweep whenever the orchestrator commits an event that
/// releases references (a thread deleted or rolled back), until it shuts
/// down. Blocks; run it on its own thread.
pub fn watch_releases(orchestrator: &Orchestrator, sweeps: &SweepHandle) {
    use waku_protocol::orchestration::{DomainEvent, ThreadChange};
    loop {
        let feed = orchestrator.subscribe();
        loop {
            match feed.recv() {
                Ok(event) => {
                    if let DomainEvent::ThreadUpdated(update) = &event.event
                        && matches!(
                            update.change,
                            ThreadChange::Deleted | ThreadChange::RollbackCompleted
                        )
                    {
                        sweeps.request(SweepReason::Released);
                    }
                }
                // Fell behind: whatever was missed may have released
                // something.
                Err(waku_orchestrator::feed::FeedEnded::Overflowed) => {
                    sweeps.request(SweepReason::Released);
                    break;
                }
                Err(waku_orchestrator::feed::FeedEnded::Closed) => return,
            }
        }
        if orchestrator.is_shutting_down() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn times(startup: u64, release: u64, interval: u64) -> SweepTimes {
        SweepTimes {
            startup: Duration::from_millis(startup),
            after_store: Duration::from_secs(3600),
            after_release: Duration::from_millis(release),
            min_interval: Duration::from_millis(interval),
        }
    }

    fn wait_for(count: &AtomicUsize, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while count.load(Ordering::SeqCst) < expected {
            assert!(Instant::now() < deadline, "no sweep {expected}");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn sweeps_run_once_at_startup_and_then_only_when_requested() {
        let count = Arc::new(AtomicUsize::new(0));
        let counted = count.clone();
        let sweeper = BlobSweeper::start(times(10, 10, 0), move || {
            counted.fetch_add(1, Ordering::SeqCst);
        });
        wait_for(&count, 1);
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(count.load(Ordering::SeqCst), 1, "no sweep unasked");
        sweeper.handle().request(SweepReason::Released);
        wait_for(&count, 2);
        // A store waits out the grace period.
        sweeper.handle().request(SweepReason::Stored);
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(count.load(Ordering::SeqCst), 2);
        sweeper.shutdown();
    }

    /// A blob old enough for a sweep to take.
    fn aged_blob(store: &crate::persistence::StateStore, byte: u8) -> (String, std::path::PathBuf) {
        let reference = store
            .blobs()
            .store_image_bytes("image/png", &[byte; 64])
            .unwrap();
        let path = store.blobs().path_for(&reference).unwrap();
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(
                std::time::SystemTime::now() - ASSET_SWEEP_GRACE_PERIOD - Duration::from_secs(60),
            )
            .unwrap();
        (reference, path)
    }

    #[test]
    fn a_sweep_keeps_what_live_threads_reference() {
        use waku_protocol::orchestration::{
            Actor, Attachment, Command, CommandEnvelope, CommandId, CreationSource, DispatchMode,
            InteractionMode, MessageId, ModelSelection, ProjectId, RuntimeMode, ThreadId,
            WorkspaceStrategy,
        };
        let root = tempfile::tempdir().unwrap();
        let store = crate::persistence::StateStore::daemon(root.path().join("app.db"));
        store.load().unwrap();
        let (kept, kept_path) = aged_blob(&store, 1);
        let (_, orphan_path) = aged_blob(&store, 2);

        let orchestrator = Orchestrator::open(
            waku_orchestrator::OrchestratorConfig::new(root.path().join("state.sqlite")),
            waku_orchestrator::Dependencies {
                clock: Arc::new(waku_orchestrator::SystemClock),
                ids: Box::new(waku_orchestrator::SequentialIds::default()),
                capabilities: Arc::new(waku_orchestrator::NoCapabilities),
            },
        )
        .unwrap();
        let dispatch = |id: &str, command| {
            orchestrator
                .dispatch(CommandEnvelope {
                    command_id: CommandId::new(id).unwrap(),
                    issued_by: Actor::User,
                    source: CreationSource::Desktop,
                    command,
                })
                .unwrap();
        };
        let project_id = ProjectId(uuid::Uuid::new_v4());
        let thread_id = ThreadId(uuid::Uuid::new_v4());
        dispatch(
            "project",
            Command::ProjectCreate {
                project_id,
                name: "demo".into(),
                path: root.path().to_owned(),
                order_key: None,
            },
        );
        dispatch(
            "thread",
            Command::ThreadCreate {
                thread_id,
                project_id,
                title: None,
                model_selection: ModelSelection::new("codex"),
                runtime_mode: RuntimeMode::FullAccess,
                interaction_mode: InteractionMode::Default,
                workspace: WorkspaceStrategy::Root { branch: None },
                imported_native_thread: None,
            },
        );
        dispatch(
            "message",
            Command::MessageDispatch {
                thread_id,
                message_id: MessageId(uuid::Uuid::new_v4()),
                text: "What is in this screenshot?".into(),
                display_text: None,
                attachments: vec![Attachment {
                    path: kept_path.clone(),
                    mention: "screenshot.png".into(),
                    name: "screenshot.png".into(),
                    is_dir: false,
                    is_image: true,
                    blob_reference: Some(kept.clone()),
                }],
                context: Vec::new(),
                model_selection: None,
                mode: DispatchMode::Auto,
                title_seed: None,
                source_plan: None,
            },
        );

        let references = orchestrator_references(&orchestrator).unwrap();
        assert!(references.blobs.contains(&kept));
        store.asset_sweep().run(&references);
        assert!(kept_path.exists(), "a live thread's attachment stays");
        assert!(!orphan_path.exists(), "an aged orphan goes");

        // Deleting the thread releases it.
        dispatch("delete", Command::ThreadDelete { thread_id });
        let references = orchestrator_references(&orchestrator).unwrap();
        assert!(references.blobs.is_empty());
        store.asset_sweep().run(&references);
        assert!(!kept_path.exists(), "a deleted thread's attachment goes");
        orchestrator.shutdown();
    }

    #[test]
    fn requests_coalesce_and_respect_the_interval() {
        let count = Arc::new(AtomicUsize::new(0));
        let counted = count.clone();
        let sweeper = BlobSweeper::start(times(0, 0, 300), move || {
            counted.fetch_add(1, Ordering::SeqCst);
        });
        wait_for(&count, 1);
        let handle = sweeper.handle();
        for _ in 0..10 {
            handle.request(SweepReason::Released);
        }
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(count.load(Ordering::SeqCst), 1, "held by the interval");
        wait_for(&count, 2);
        std::thread::sleep(Duration::from_millis(400));
        assert_eq!(count.load(Ordering::SeqCst), 2, "ten requests, one sweep");
        sweeper.shutdown();
    }
}
