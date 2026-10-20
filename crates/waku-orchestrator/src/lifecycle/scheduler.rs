//! Deadlines for auto-settle and snooze wake, kept in memory and refreshed
//! only for threads whose rows changed (#64).
//!
//! Each tracked thread has at most two deadlines: when its snooze ends (a
//! `SnoozeWake` fires then) and when the auto-settle rule must look at it
//! again (its inactivity window elapses, or its snooze ends). Nothing is
//! scanned on a timer: [`Scheduler::reload`] recomputes one thread from its
//! projection rows when a committed event touches it, and
//! [`Scheduler::poll`] pops what is due.

use std::collections::{BTreeSet, HashMap};

use rusqlite::{Connection, OptionalExtension, params};
use waku_protocol::orchestration::*;

use super::policy::{AutoSettleSettings, PullRequestState, SettlementFacts, Verdict, evaluate};
use crate::store::{StoreResult, decode};

/// Where the auto-settle rule learns about pull requests. Implementations
/// answer from memory (a cache the PR service keeps); the scheduler thread
/// never waits on the network.
pub trait PullRequestSource: Send + Sync {
    fn pull_request(&self, thread: &AppThread) -> Option<PullRequestState>;
}

/// No pull request integration (Phase 2): only inactivity settles threads.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoPullRequests;

impl PullRequestSource for NoPullRequests {
    fn pull_request(&self, _thread: &AppThread) -> Option<PullRequestState> {
        None
    }
}

/// A command the scheduler wants dispatched.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Fire {
    Settle {
        thread_id: ThreadId,
        snapshot_at: Timestamp,
    },
    Wake {
        thread_id: ThreadId,
        at: Timestamp,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Due {
    Wake,
    Check,
}

#[derive(Clone, Copy, Debug, Default)]
struct Entry {
    wake: Option<Timestamp>,
    check: Option<Timestamp>,
}

/// Every column the facts need, for one thread (`?1`) or for every thread
/// that can still settle or wake.
const FACTS: &str =
    "SELECT t.payload_json, t.activity, t.pending_request_count, t.queued_run_count,
        (SELECT MAX(r.requested_at) FROM p_runs AS r WHERE r.thread_id = t.thread_id),
        (SELECT MAX(r.started_at) FROM p_runs AS r WHERE r.thread_id = t.thread_id),
        (SELECT MAX(r.completed_at) FROM p_runs AS r WHERE r.thread_id = t.thread_id),
        EXISTS (SELECT 1 FROM p_provider_threads AS p
                WHERE p.thread_id = t.thread_id
                  AND json_array_length(p.payload_json, '$.pendingBackgroundTasks') > 0)
     FROM p_threads AS t";

fn facts_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<FactsRow> {
    Ok(FactsRow {
        thread_json: row.get(0)?,
        activity: row.get(1)?,
        pending_requests: row.get(2)?,
        queued_runs: row.get(3)?,
        requested_at: row.get(4)?,
        started_at: row.get(5)?,
        completed_at: row.get(6)?,
        background_work: row.get(7)?,
    })
}

struct FactsRow {
    thread_json: String,
    activity: String,
    pending_requests: i64,
    queued_runs: i64,
    requested_at: Option<Timestamp>,
    started_at: Option<Timestamp>,
    completed_at: Option<Timestamp>,
    background_work: bool,
}

impl FactsRow {
    fn into_facts(self) -> StoreResult<(AppThread, SettlementFacts)> {
        let thread: AppThread = decode("p_threads", &self.thread_json)?;
        let activity_at = [
            thread.last_reply_at,
            self.requested_at,
            self.started_at,
            self.completed_at,
        ]
        .into_iter()
        .flatten()
        .max();
        let facts = SettlementFacts {
            thread_id: thread.id,
            project_id: thread.project_id,
            created_at: thread.created_at,
            updated_at: thread.updated_at,
            archived: thread.archived_at.is_some(),
            deleted: thread.deleted_at.is_some(),
            settled: thread.settled_at.is_some(),
            settled_override: thread.settled_override.is_some(),
            pinned: thread.pinned_at.is_some(),
            auto_settle_disabled: thread.auto_settle_disabled_at.is_some(),
            busy: !matches!(self.activity.as_str(), "idle" | "failed") || self.queued_runs > 0,
            pending_requests: self.pending_requests > 0,
            background_work: self.background_work,
            failed: self.activity == "failed",
            snoozed_until: thread.snoozed_until,
            snoozed_at: thread.snoozed_at,
            last_snooze_wake_at: thread.last_snooze_wake_at,
            latest_user_message_at: self.requested_at,
            latest_run_completed_at: self.completed_at,
            activity_at,
        };
        Ok((thread, facts))
    }
}

/// Loads one thread's settlement facts.
pub fn load_facts(
    connection: &Connection,
    thread_id: ThreadId,
) -> StoreResult<Option<(AppThread, SettlementFacts)>> {
    use std::sync::LazyLock;
    static ONE: LazyLock<String> = LazyLock::new(|| format!("{FACTS} WHERE t.thread_id = ?1"));
    connection
        .prepare_cached(&ONE)?
        .query_row(params![thread_id.to_string()], facts_row)
        .optional()?
        .map(FactsRow::into_facts)
        .transpose()
}

/// The in-memory deadline set.
pub struct Scheduler {
    settings: AutoSettleSettings,
    queue: BTreeSet<(Timestamp, ThreadId, Due)>,
    threads: HashMap<ThreadId, Entry>,
    ready: Vec<Fire>,
}

impl Scheduler {
    pub fn new(settings: AutoSettleSettings) -> Self {
        Self {
            settings,
            queue: BTreeSet::new(),
            threads: HashMap::new(),
            ready: Vec::new(),
        }
    }

    pub fn settings(&self) -> &AutoSettleSettings {
        &self.settings
    }

    /// Replaces the settings; the caller reloads every thread.
    pub fn set_settings(&mut self, settings: AutoSettleSettings) {
        self.settings = settings;
    }

    /// Threads with a pending deadline.
    pub fn tracked(&self) -> usize {
        self.threads.len()
    }

    /// The earliest deadline, if any (ready fires count as now).
    pub fn next_due(&self) -> Option<Timestamp> {
        if !self.ready.is_empty() {
            return Some(Timestamp::MIN);
        }
        self.queue.first().map(|(at, ..)| *at)
    }

    /// Recomputes every thread that can still settle or wake.
    pub fn reload_all(
        &mut self,
        connection: &Connection,
        pull_requests: &dyn PullRequestSource,
        now: Timestamp,
    ) -> StoreResult<()> {
        use std::sync::LazyLock;
        static ALL: LazyLock<String> = LazyLock::new(|| {
            format!(
                "{FACTS} WHERE t.deleted_at IS NULL
                   AND (t.snoozed_until IS NOT NULL
                        OR (t.archived_at IS NULL AND t.settled_at IS NULL))"
            )
        });
        self.queue.clear();
        self.threads.clear();
        self.ready.clear();
        let mut statement = connection.prepare_cached(&ALL)?;
        let rows = statement
            .query_map([], facts_row)?
            .collect::<Result<Vec<_>, _>>()?;
        for row in rows {
            let (thread, facts) = row.into_facts()?;
            self.schedule(&thread, &facts, pull_requests, now);
        }
        Ok(())
    }

    /// Recomputes one thread after its rows changed.
    pub fn reload(
        &mut self,
        connection: &Connection,
        pull_requests: &dyn PullRequestSource,
        thread_id: ThreadId,
        now: Timestamp,
    ) -> StoreResult<()> {
        self.forget(thread_id);
        if let Some((thread, facts)) = load_facts(connection, thread_id)? {
            self.schedule(&thread, &facts, pull_requests, now);
        }
        Ok(())
    }

    /// Pops what is due at `now`: wakes fire as they are, settle checks
    /// re-read their thread first.
    pub fn poll(
        &mut self,
        connection: &Connection,
        pull_requests: &dyn PullRequestSource,
        now: Timestamp,
    ) -> StoreResult<Vec<Fire>> {
        let mut checks = Vec::new();
        let mut woken = Vec::new();
        while let Some(&(at, thread_id, due)) = self.queue.first() {
            if at > now {
                break;
            }
            self.queue.pop_first();
            let entry = self.threads.entry(thread_id).or_default();
            match due {
                Due::Wake => {
                    entry.wake = None;
                    woken.push(thread_id);
                    self.ready.push(Fire::Wake { thread_id, at });
                }
                Due::Check => {
                    entry.check = None;
                    checks.push(thread_id);
                }
            }
            if entry.wake.is_none() && entry.check.is_none() {
                self.threads.remove(&thread_id);
            }
        }
        for thread_id in checks {
            self.reload(connection, pull_requests, thread_id, now)?;
            // The rows still show the snooze whose wake just fired; the
            // wake's own commit reloads the thread again.
            if woken.contains(&thread_id) {
                self.clear_wake(thread_id);
            }
        }
        let mut fires = std::mem::take(&mut self.ready);
        let mut seen = std::collections::HashSet::new();
        fires.retain(|fire| seen.insert(*fire));
        Ok(fires)
    }

    fn clear_wake(&mut self, thread_id: ThreadId) {
        if let Some(entry) = self.threads.get_mut(&thread_id)
            && let Some(at) = entry.wake.take()
        {
            self.queue.remove(&(at, thread_id, Due::Wake));
            if entry.check.is_none() {
                self.threads.remove(&thread_id);
            }
        }
    }

    fn schedule(
        &mut self,
        thread: &AppThread,
        facts: &SettlementFacts,
        pull_requests: &dyn PullRequestSource,
        now: Timestamp,
    ) {
        if facts.deleted {
            return;
        }
        if let Some(until) = facts.snoozed_until {
            self.insert(facts.thread_id, Due::Wake, until);
        }
        let policy = self.settings.for_project(facts.project_id);
        if !policy.enabled() {
            return;
        }
        let pull_request = pull_requests.pull_request(thread);
        match evaluate(facts, pull_request, &policy, now) {
            Verdict::Never => {}
            Verdict::At(at) => self.insert(facts.thread_id, Due::Check, at),
            Verdict::SettleNow => self.ready.push(Fire::Settle {
                thread_id: facts.thread_id,
                snapshot_at: facts.updated_at,
            }),
        }
    }

    fn insert(&mut self, thread_id: ThreadId, due: Due, at: Timestamp) {
        let entry = self.threads.entry(thread_id).or_default();
        let slot = match due {
            Due::Wake => &mut entry.wake,
            Due::Check => &mut entry.check,
        };
        if let Some(previous) = slot.replace(at) {
            self.queue.remove(&(previous, thread_id, due));
        }
        self.queue.insert((at, thread_id, due));
    }

    fn forget(&mut self, thread_id: ThreadId) {
        if let Some(entry) = self.threads.remove(&thread_id) {
            if let Some(at) = entry.wake {
                self.queue.remove(&(at, thread_id, Due::Wake));
            }
            if let Some(at) = entry.check {
                self.queue.remove(&(at, thread_id, Due::Check));
            }
        }
    }
}
