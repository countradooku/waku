//! `EventSink`: events, projections, receipt and outbox in one SQLite
//! transaction (#40, RFC 0001 "Public APIs").
//!
//! `commit` runs `BEGIN IMMEDIATE` and then, in order:
//! 1. rechecks the receipt (a concurrent duplicate returns the stored one);
//! 2. checks the write guard against projection rows;
//! 3. appends the events (global sequence);
//! 4. applies them to the projection tables (`apply_sql`) and records the
//!    projection's last sequence;
//! 5. writes the receipt;
//! 6. cancels unsettled effects;
//! 7. enqueues the decision's effects;
//!
//! then `COMMIT`. Subscribers see the events only after `COMMIT` returns, in
//! sequence order, because publishing happens while the writer is still held.
//! A failure at any step rolls the whole transaction back, so the projection
//! can never get ahead of the log and no effect exists without its events.

use std::sync::Arc;

use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use waku_protocol::orchestration::*;

use super::receipts::{self, NewReceipt, Receipt};
use super::{GRAPH_PROJECTION, Store, StoreError, StoreResult, max_sequence};
use crate::command::{CommandError, LockKey};
use crate::decide::Decision;
use crate::effects::Effect;
use crate::feed::FeedPublisher;
use crate::ids::IdSource;
use crate::projection::sql::apply_sql;

/// A provider-ingestion precondition, checked inside the transaction (rule 5).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteGuard {
    /// The run is non-terminal and the attempt is still its active one.
    RunCurrent {
        run_id: RunId,
        attempt_id: RunAttemptId,
    },
    /// The provider thread is still owned by this thread (and node).
    ProviderThreadOwner {
        provider_thread_id: ProviderThreadId,
        thread_id: ThreadId,
        owner_node_id: Option<NodeId>,
    },
    /// The provider thread row has not changed since it was read.
    ProviderThreadUnchanged {
        provider_thread_id: ProviderThreadId,
        updated_at: Timestamp,
    },
    /// No other thread owns this native thread.
    UnlessNativeThreadOwned {
        driver: DriverKind,
        native_id: String,
        thread_id: ThreadId,
    },
    None,
}

/// Who is committing and under which key.
#[derive(Clone, Copy, Debug)]
pub struct CommitContext<'a> {
    /// `None` for provider ingestion.
    pub command_id: Option<&'a CommandId>,
    pub command_type: &'a str,
    /// Gives `events.thread_id` / `events.project_id`.
    pub key: LockKey,
    /// The thread's project, recorded on thread events for the shell stream.
    pub project_id: Option<ProjectId>,
    pub guard: Option<&'a WriteGuard>,
    pub occurred_at: Timestamp,
}

#[derive(Clone, Debug, Default)]
pub struct CommitResult {
    pub last_sequence: Sequence,
    pub events: Vec<Arc<StoredEvent>>,
    pub enqueued: Vec<EffectId>,
    pub cancelled: Vec<EffectId>,
    /// The command id already had a receipt; nothing was written.
    pub duplicate: bool,
    /// The stored receipt when `duplicate`.
    pub receipt: Option<Receipt>,
}

/// The outcome of recording a rejection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RejectionOutcome {
    Recorded(Sequence),
    Duplicate(Receipt),
}

/// The seven steps of `commit`; crash-injection tests abort after each.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultPoint {
    AfterReceiptCheck,
    AfterGuard,
    AfterEvents,
    AfterProjections,
    AfterReceipt,
    AfterCancel,
    AfterOutbox,
}

impl FaultPoint {
    pub const ALL: [Self; 7] = [
        Self::AfterReceiptCheck,
        Self::AfterGuard,
        Self::AfterEvents,
        Self::AfterProjections,
        Self::AfterReceipt,
        Self::AfterCancel,
        Self::AfterOutbox,
    ];
}

#[inline]
fn fault(_point: FaultPoint) -> StoreResult<()> {
    #[cfg(test)]
    super::faults::check(_point)?;
    Ok(())
}

pub struct EventSink {
    store: Arc<Store>,
    ids: Arc<Mutex<Box<dyn IdSource>>>,
    feed: Arc<FeedPublisher>,
}

impl EventSink {
    pub fn new(
        store: Arc<Store>,
        ids: Arc<Mutex<Box<dyn IdSource>>>,
        feed: Arc<FeedPublisher>,
    ) -> Self {
        Self { store, ids, feed }
    }

    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }

    /// Commits a decision atomically and publishes its events after commit.
    pub fn commit(&self, cx: CommitContext<'_>, decision: &Decision) -> StoreResult<CommitResult> {
        let mut writer = self.store.writer();
        let transaction = writer.transaction_with_behavior(TransactionBehavior::Immediate)?;

        // 1. A concurrent duplicate may have committed since the fast path.
        if let Some(command_id) = cx.command_id
            && let Some(receipt) = receipts::lookup(&transaction, command_id)?
        {
            return Ok(CommitResult {
                last_sequence: receipt.result_sequence,
                duplicate: true,
                receipt: Some(receipt),
                ..CommitResult::default()
            });
        }
        fault(FaultPoint::AfterReceiptCheck)?;

        // 2.
        if let Some(guard) = cx.guard {
            check_guard(&transaction, guard)?;
        }
        fault(FaultPoint::AfterGuard)?;

        // 3.
        let event_ids = {
            let mut ids = self.ids.lock();
            (0..decision.events.len())
                .map(|_| EventId(ids.new_id()))
                .collect::<Vec<_>>()
        };
        let thread_id = cx.key.thread_id();
        let project_id = cx.key.project_id().or(cx.project_id);
        let mut stored = Vec::with_capacity(decision.events.len());
        let mut payloads = Vec::with_capacity(decision.events.len());
        for (event, event_id) in decision.events.iter().zip(event_ids) {
            let payload = event.payload_json()?;
            let sequence = insert_event(
                &transaction,
                event_id,
                cx.command_id,
                project_id,
                thread_id,
                cx.occurred_at,
                event,
                &payload,
            )?;
            stored.push(Arc::new(StoredEvent {
                sequence,
                event_id,
                command_id: cx.command_id.cloned(),
                project_id,
                thread_id,
                occurred_at: cx.occurred_at,
                event: event.clone(),
            }));
            payloads.push(payload);
        }
        fault(FaultPoint::AfterEvents)?;

        // 4.
        for (event, payload) in stored.iter().zip(&payloads) {
            apply_sql(&transaction, event, payload)?;
        }
        let last_sequence = match stored.last() {
            Some(event) => {
                set_projection_sequence(&transaction, event.sequence, cx.occurred_at)?;
                event.sequence
            }
            None => max_sequence(&transaction)?,
        };
        fault(FaultPoint::AfterProjections)?;

        // 5.
        if let Some(command_id) = cx.command_id {
            receipts::insert(
                &transaction,
                NewReceipt {
                    command_id,
                    command_type: cx.command_type,
                    project_id: project_id.map(|id| id.to_string()),
                    thread_id: thread_id.map(|id| id.to_string()),
                    result_sequence: last_sequence,
                    error: None,
                    accepted_at: cx.occurred_at,
                },
            )?;
        }
        fault(FaultPoint::AfterReceipt)?;

        // 6.
        let mut cancelled = Vec::new();
        for cancel in &decision.cancel_unsettled {
            for effect_type in &cancel.effect_types {
                cancelled.extend(super::outbox::cancel_unsettled(
                    &transaction,
                    cancel.thread_id,
                    *effect_type,
                    &cancel.reason,
                    cx.occurred_at,
                )?);
            }
        }
        fault(FaultPoint::AfterCancel)?;

        // 7.
        let lane_key = match cx.key {
            LockKey::Thread(id) => id.to_string(),
            LockKey::Project(id) => format!("project:{id}"),
        };
        let mut enqueued = Vec::with_capacity(decision.effects.len());
        for effect in &decision.effects {
            enqueued.push(insert_effect(
                &transaction,
                cx.command_id,
                &lane_key,
                thread_id,
                effect,
                cx.occurred_at,
            )?);
        }
        fault(FaultPoint::AfterOutbox)?;

        transaction.commit()?;
        // Still under the writer lock: subscribers see commits in order.
        self.feed.publish(&stored);
        self.store.note_commit();
        Ok(CommitResult {
            last_sequence,
            events: stored,
            enqueued,
            cancelled,
            duplicate: false,
            receipt: None,
        })
    }

    /// Records a rejected command (R16), unless the id already has a receipt.
    pub fn commit_rejection(
        &self,
        cx: CommitContext<'_>,
        error: &CommandError,
    ) -> StoreResult<RejectionOutcome> {
        let Some(command_id) = cx.command_id else {
            return Ok(RejectionOutcome::Recorded(max_sequence(
                &self.store.writer(),
            )?));
        };
        let mut writer = self.store.writer();
        let transaction = writer.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(receipt) = receipts::lookup(&transaction, command_id)? {
            return Ok(RejectionOutcome::Duplicate(receipt));
        }
        let sequence = max_sequence(&transaction)?;
        receipts::insert(
            &transaction,
            NewReceipt {
                command_id,
                command_type: cx.command_type,
                project_id: cx
                    .key
                    .project_id()
                    .or(cx.project_id)
                    .map(|id| id.to_string()),
                thread_id: cx.key.thread_id().map(|id| id.to_string()),
                result_sequence: sequence,
                error: Some(error),
                accepted_at: cx.occurred_at,
            },
        )?;
        transaction.commit()?;
        Ok(RejectionOutcome::Recorded(sequence))
    }
}

#[allow(clippy::too_many_arguments)]
fn insert_event(
    connection: &Connection,
    event_id: EventId,
    command_id: Option<&CommandId>,
    project_id: Option<ProjectId>,
    thread_id: Option<ThreadId>,
    occurred_at: Timestamp,
    event: &DomainEvent,
    payload: &str,
) -> StoreResult<Sequence> {
    connection
        .prepare_cached(
            "INSERT INTO events (event_id, command_id, project_id, thread_id, run_id, node_id,
                                 entity_id, driver, event_type, occurred_at, payload_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        )?
        .execute(params![
            event_id.to_string(),
            command_id.map(CommandId::as_str),
            project_id.map(|id| id.to_string()),
            thread_id.map(|id| id.to_string()),
            event.run_id().map(|id| id.to_string()),
            event.node_id().map(|id| id.to_string()),
            event.entity_id(),
            event.driver().map(DriverKind::as_str),
            event.event_type(),
            occurred_at,
            payload,
        ])?;
    Ok(connection.last_insert_rowid() as Sequence)
}

fn insert_effect(
    connection: &Connection,
    command_id: Option<&CommandId>,
    lane_key: &str,
    thread_id: Option<ThreadId>,
    effect: &Effect,
    now: Timestamp,
) -> StoreResult<EffectId> {
    let payload = serde_json::to_string(effect)?;
    connection
        .prepare_cached(
            "INSERT INTO effect_outbox (command_id, lane_key, thread_id, effect_type, lane,
                                        process_bound, status, available_at, payload_json,
                                        created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', ?7, ?8, ?7, ?7)",
        )?
        .execute(params![
            command_id.map(CommandId::as_str),
            lane_key,
            thread_id.map(|id| id.to_string()),
            effect.effect_type().as_str(),
            effect.lane().as_str(),
            effect.process_bound(),
            now,
            payload,
        ])?;
    Ok(connection.last_insert_rowid())
}

pub(crate) fn set_projection_sequence(
    connection: &Connection,
    sequence: Sequence,
    now: Timestamp,
) -> StoreResult<()> {
    connection
        .prepare_cached(
            "INSERT INTO projection_metadata (name, schema_version, last_sequence, updated_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(name) DO UPDATE SET last_sequence = excluded.last_sequence,
                                             updated_at = excluded.updated_at",
        )?
        .execute(params![
            GRAPH_PROJECTION,
            super::GRAPH_SCHEMA_VERSION,
            sequence as i64,
            now
        ])?;
    Ok(())
}

fn check_guard(connection: &Connection, guard: &WriteGuard) -> StoreResult<()> {
    let holds = match guard {
        WriteGuard::None => true,
        WriteGuard::RunCurrent { run_id, attempt_id } => connection
            .prepare_cached(
                "SELECT status, json_extract(payload_json, '$.activeAttemptId') FROM p_runs
                 WHERE run_id = ?1",
            )?
            .query_row([run_id.to_string()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
            })
            .optional()?
            .is_some_and(|(status, active)| {
                let status = RunStatus::ALL
                    .iter()
                    .copied()
                    .find(|known| known.as_str() == status)
                    .unwrap_or(RunStatus::Unknown);
                !status.is_terminal() && active == Some(attempt_id.to_string())
            }),
        WriteGuard::ProviderThreadOwner {
            provider_thread_id,
            thread_id,
            owner_node_id,
        } => connection
            .prepare_cached(
                "SELECT thread_id, owner_node_id FROM p_provider_threads
                 WHERE provider_thread_id = ?1",
            )?
            .query_row([provider_thread_id.to_string()], |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                ))
            })
            .optional()?
            .is_some_and(|(owner_thread, owner_node)| {
                owner_thread == Some(thread_id.to_string())
                    && owner_node == owner_node_id.map(|id| id.to_string())
            }),
        WriteGuard::ProviderThreadUnchanged {
            provider_thread_id,
            updated_at,
        } => {
            connection
                .prepare_cached(
                    "SELECT updated_at FROM p_provider_threads WHERE provider_thread_id = ?1",
                )?
                .query_row([provider_thread_id.to_string()], |row| row.get::<_, i64>(0))
                .optional()?
                == Some(*updated_at)
        }
        WriteGuard::UnlessNativeThreadOwned {
            driver,
            native_id,
            thread_id,
        } => connection
            .prepare_cached(
                "SELECT 1 FROM p_provider_threads
                 WHERE driver = ?1 AND native_id = ?2 AND thread_id IS NOT NULL AND thread_id <> ?3
                 LIMIT 1",
            )?
            .query_row(
                params![driver.as_str(), native_id, thread_id.to_string()],
                |_| Ok(()),
            )
            .optional()?
            .is_none(),
    };
    if holds {
        Ok(())
    } else {
        Err(StoreError::GuardFailed)
    }
}
