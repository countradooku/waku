//! `EffectWorkerPool`: OS threads that claim outbox rows with a lease, run
//! them through an [`EffectExecutor`] and settle them (#41).
//!
//! - Claims are FIFO per (thread, lane); the title lane only blocks titles.
//! - The post-commit waker is the fast path; a liveness poll (default 30 s)
//!   returns expired leases to `pending` and prunes settled rows.
//! - Retries back off exponentially (`base · 2^(attempt-1)`, capped) up to
//!   `max_attempts`; `Permanent` errors fail at once.
//! - A cancel (`Decision::cancel_unsettled`) marks the row `cancelled` in the
//!   same transaction and signals the effect's [`CancelToken`]; the worker
//!   re-reads the row after claiming, so a cancel that races the claim wins,
//!   and a settle only lands while the worker still holds the lease.

use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, bounded};
use parking_lot::Mutex;
use uuid::Uuid;
use waku_protocol::orchestration::{EffectId, Timestamp};

use super::executor::{CancelToken, ClaimedEffect, EffectContext, EffectError, EffectExecutor};
use crate::orchestrator::Orchestrator;
use crate::store::outbox::{self, EffectStatus, OutboxRow, ReconcileReport, Settle};
use crate::store::receipts;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerConfig {
    pub workers: usize,
    pub lease: Duration,
    pub max_attempts: u32,
    pub backoff_base: Duration,
    pub backoff_cap: Duration,
    /// The slow path; the in-process notify is the fast one.
    pub liveness_poll: Duration,
    /// Succeeded and cancelled rows (R3: 7 days). Failed rows are kept.
    pub settled_retention: Duration,
    /// Command receipts (R3: 30 days).
    pub receipt_retention: Duration,
    pub prune_batch: usize,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            workers: 4,
            lease: Duration::from_secs(30),
            max_attempts: 5,
            backoff_base: Duration::from_millis(100),
            backoff_cap: Duration::from_secs(30),
            liveness_poll: Duration::from_secs(30),
            settled_retention: Duration::from_secs(7 * 24 * 60 * 60),
            receipt_retention: Duration::from_secs(30 * 24 * 60 * 60),
            prune_batch: 500,
        }
    }
}

impl WorkerConfig {
    /// `min(cap, base · 2^(attempt-1))` for a 1-based attempt.
    pub fn backoff(&self, attempt: u32) -> Duration {
        let factor = 1u32
            .checked_shl(attempt.saturating_sub(1))
            .unwrap_or(u32::MAX);
        self.backoff_base
            .checked_mul(factor)
            .unwrap_or(self.backoff_cap)
            .min(self.backoff_cap)
    }
}

fn millis(duration: Duration) -> i64 {
    duration.as_millis().min(i64::MAX as u128) as i64
}

struct Shared {
    orchestrator: Arc<Orchestrator>,
    executor: Arc<dyn EffectExecutor>,
    config: WorkerConfig,
    boot: Uuid,
    notify_tx: Sender<()>,
    notify_rx: Receiver<()>,
    done_tx: Sender<usize>,
    stopping: AtomicBool,
    tokens: Mutex<HashMap<EffectId, Arc<CancelToken>>>,
    last_maintenance: Mutex<Option<Instant>>,
}

impl Shared {
    fn notify(&self, available: usize) {
        for _ in 0..available.clamp(1, self.config.workers.max(1)) {
            // A full channel already guarantees a wakeup.
            let _ = self.notify_tx.try_send(());
        }
    }

    fn signal_cancelled(&self, ids: &[EffectId]) {
        let tokens = self.tokens.lock();
        for id in ids {
            if let Some(token) = tokens.get(id) {
                token.cancel();
            }
        }
    }
}

pub struct EffectWorkerPool {
    shared: Arc<Shared>,
    handles: Vec<JoinHandle<()>>,
    done_rx: Receiver<usize>,
}

impl EffectWorkerPool {
    /// Run once at startup, before `start` and before the socket accepts
    /// work: cancels pending and running process-bound effects (their
    /// provider process is gone) and requeues running replay-safe ones.
    pub fn reconcile_after_process_loss(
        orchestrator: &Orchestrator,
    ) -> anyhow::Result<ReconcileReport> {
        let now = orchestrator.clock().now();
        let writer = orchestrator.store().writer();
        Ok(outbox::reconcile_after_process_loss(&writer, now)?)
    }

    pub fn start(
        orchestrator: Arc<Orchestrator>,
        executor: Arc<dyn EffectExecutor>,
        config: WorkerConfig,
    ) -> Self {
        let workers = config.workers.max(1);
        let (notify_tx, notify_rx) = bounded(workers * 4);
        let (done_tx, done_rx) = bounded(workers);
        let shared = Arc::new(Shared {
            orchestrator: orchestrator.clone(),
            executor,
            config,
            boot: Uuid::now_v7(),
            notify_tx,
            notify_rx,
            done_tx,
            stopping: AtomicBool::new(false),
            tokens: Mutex::new(HashMap::new()),
            last_maintenance: Mutex::new(None),
        });
        let weak: Weak<Shared> = Arc::downgrade(&shared);
        orchestrator.set_effect_waker(Arc::new(move |commit| {
            if let Some(shared) = weak.upgrade() {
                if !commit.cancelled.is_empty() {
                    shared.signal_cancelled(&commit.cancelled);
                }
                if !commit.enqueued.is_empty() {
                    shared.notify(commit.enqueued.len());
                }
            }
        }));
        let handles = (0..workers)
            .map(|index| {
                let shared = shared.clone();
                std::thread::Builder::new()
                    .name(format!("waku-effect-{index}"))
                    .spawn(move || worker_loop(&shared, index))
                    .expect("spawn effect worker")
            })
            .collect();
        // Rows committed before the pool started.
        shared.notify(workers);
        Self {
            shared,
            handles,
            done_rx,
        }
    }

    /// Wakes up to `available` idle workers.
    pub fn notify(&self, available: usize) {
        self.shared.notify(available);
    }

    /// Signals the cancel tokens of running effects.
    pub fn signal_cancelled(&self, ids: &[EffectId]) {
        self.shared.signal_cancelled(ids);
    }

    /// Effects currently executing.
    pub fn running(&self) -> usize {
        self.shared.tokens.lock().len()
    }

    /// Stops claiming, waits up to `grace` for running effects, then cancels
    /// what is still running. Leftover rows are reconciled on the next start.
    pub fn shutdown(self, grace: Duration) {
        let shared = self.shared;
        shared.stopping.store(true, Ordering::Release);
        shared.notify(self.handles.len());
        let deadline = Instant::now() + grace;
        let mut finished = 0;
        while finished < self.handles.len() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match self.done_rx.recv_timeout(remaining) {
                Ok(_) => finished += 1,
                Err(_) => break,
            }
        }
        if finished < self.handles.len() {
            for token in shared.tokens.lock().values() {
                token.cancel();
            }
        }
        for handle in self.handles {
            if handle.is_finished() {
                let _ = handle.join();
            }
        }
    }
}

fn worker_loop(shared: &Arc<Shared>, index: usize) {
    let worker_id = format!("{}:{index}", shared.boot);
    let lease_ms = millis(shared.config.lease);
    while !shared.stopping.load(Ordering::Acquire) {
        if index == 0 {
            maintenance(shared);
        }
        let now = shared.orchestrator.clock().now();
        let claimed = {
            let writer = shared.orchestrator.store().writer();
            outbox::claim(&writer, &worker_id, now, lease_ms)
        };
        match claimed {
            Ok(Some(row)) => run_one(shared, &worker_id, row),
            Ok(None) => {
                let next = {
                    let writer = shared.orchestrator.store().writer();
                    outbox::next_available(&writer).ok().flatten()
                };
                // A row due now is blocked behind its lane; the settle that
                // unblocks it notifies. Otherwise sleep until the next
                // backoff expires.
                let mut wait = shared.config.liveness_poll;
                if let Some(next) = next.filter(|next| *next > now) {
                    wait = wait.min(Duration::from_millis((next - now) as u64));
                }
                let _ = shared.notify_rx.recv_timeout(wait);
            }
            Err(_) => {
                let _ = shared.notify_rx.recv_timeout(shared.config.backoff_base);
            }
        }
    }
    let _ = shared.done_tx.send(index);
}

/// The liveness poll: expired leases back to pending, then pruning.
fn maintenance(shared: &Shared) {
    let mut last = shared.last_maintenance.lock();
    if last.is_some_and(|last| last.elapsed() < shared.config.liveness_poll) {
        return;
    }
    *last = Some(Instant::now());
    drop(last);
    let now = shared.orchestrator.clock().now();
    let writer = shared.orchestrator.store().writer();
    let _ = outbox::expire_leases(&writer, now);
    let pruned = outbox::prune(
        &writer,
        now - millis(shared.config.settled_retention),
        shared.config.prune_batch,
    )
    .unwrap_or(0)
        + receipts::prune(
            &writer,
            now - millis(shared.config.receipt_retention),
            shared.config.prune_batch,
        )
        .unwrap_or(0);
    if pruned > 0 {
        let _ = writer.execute_batch("PRAGMA incremental_vacuum(2048);");
    }
}

fn settle(shared: &Shared, worker_id: &str, effect_id: EffectId, outcome: &Settle) {
    let now: Timestamp = shared.orchestrator.clock().now();
    let writer = shared.orchestrator.store().writer();
    let _ = outbox::settle(&writer, effect_id, worker_id, outcome, now);
}

fn run_one(shared: &Arc<Shared>, worker_id: &str, row: OutboxRow) {
    let token = Arc::new(CancelToken::new());
    shared.tokens.lock().insert(row.effect_id, token.clone());
    let finish = |outcome: Option<Settle>| {
        if let Some(outcome) = outcome {
            settle(shared, worker_id, row.effect_id, &outcome);
        }
        shared.tokens.lock().remove(&row.effect_id);
        // The lane's next row (or a retry's new due time) is claimable now.
        shared.notify(1);
    };

    // A cancel that raced the claim wins.
    let status = {
        let writer = shared.orchestrator.store().writer();
        outbox::status(&writer, row.effect_id)
    };
    if !matches!(status, Ok(Some(EffectStatus::Running))) {
        return finish(None);
    }
    let effect = match &row.effect {
        Ok(effect) => effect.clone(),
        Err(_) => {
            return finish(Some(Settle::Failed {
                error: format!("unknown effect type {}", row.effect_type),
            }));
        }
    };
    let config = &shared.config;
    let claimed = ClaimedEffect {
        id: row.effect_id,
        command_id: row.command_id.clone(),
        thread_id: row.thread_id,
        attempt: row.attempt_count,
        will_retry: row.attempt_count < config.max_attempts,
        effect,
    };
    let now = shared.orchestrator.clock().now();
    let cx = EffectContext {
        orchestrator: &shared.orchestrator,
        cancel: &token,
        now,
        effect_id: row.effect_id,
    };
    let result = if token.is_cancelled() {
        Err(EffectError::Cancelled)
    } else {
        catch_unwind(AssertUnwindSafe(|| shared.executor.execute(&claimed, &cx)))
            .unwrap_or_else(|_| Err(EffectError::Permanent("effect panicked".to_owned())))
    };
    let outcome = match result {
        // Cut off by shutdown: leave the row running, so the next start
        // reconciles it like a crash (one recovery path).
        Err(EffectError::Cancelled) if shared.stopping.load(Ordering::Acquire) => {
            return finish(None);
        }
        Ok(()) => Settle::Succeeded,
        Err(EffectError::Cancelled) => Settle::Cancelled {
            reason: "cancelled".to_owned(),
        },
        Err(EffectError::Retryable(error)) if claimed.will_retry => Settle::Retry {
            available_at: shared.orchestrator.clock().now()
                + millis(config.backoff(claimed.attempt)),
            error,
        },
        Err(EffectError::Retryable(error) | EffectError::Permanent(error)) => {
            Settle::Failed { error }
        }
    };
    finish(Some(outcome));
}
