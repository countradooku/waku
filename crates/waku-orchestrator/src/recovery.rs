//! The startup half of restart recovery (#59, RFC 0001 "Recovery").
//!
//! [`recover_after_restart`] runs after
//! `EffectWorkerPool::reconcile_after_process_loss` and before the effect
//! workers start: it selects candidate threads from the projection tables
//! and dispatches `RecoverThread` for each with command id
//! `recover:<thread>:<boot_id>`, so running it twice in one boot is a
//! receipt hit. What each thread needs is decided in `decide::recovery`.
//!
//! The `provider_runtime.continue` effect a recovery enqueues dispatches
//! [`continuation_command`] (command id `restart-continue:<thread>:<run>`),
//! whose decision creates the continuation run.

use rusqlite::Connection;
use uuid::Uuid;
use waku_protocol::orchestration::*;

use crate::command::InternalCommand;
pub use crate::decide::recovery::{
    CONTINUE_TEXT, INTERRUPTED_NOTICE, PROCESS_LOSS, RESUMED_NOTICE, Refusal,
};
use crate::ids::derived_id;
use crate::orchestrator::{DispatchError, Orchestrator};
use crate::store::StoreResult;

/// What one startup recovery did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    pub boot_id: Uuid,
    /// Threads `RecoverThread` was dispatched for, in id order.
    pub threads: Vec<ThreadId>,
    /// Threads whose recovery was rejected or failed, with the error.
    pub failed: Vec<(ThreadId, String)>,
    /// `provider_runtime.continue` effects this boot's recovery enqueued.
    pub continuations: usize,
}

/// Threads recovery has to look at, from the projection tables (each
/// branch reads a partial index): runs that were not terminal, pending
/// runtime requests, sessions that were not stopped, provider threads
/// holding background work for a session, and subagents still running.
pub fn candidate_threads(connection: &Connection) -> StoreResult<Vec<ThreadId>> {
    let mut statement = connection.prepare(
        "SELECT thread_id FROM p_runs
           WHERE status IN ('preparing', 'queued', 'starting', 'running', 'waiting', 'finalizing')
         UNION
         SELECT thread_id FROM p_runtime_requests WHERE status = 'pending'
         UNION
         SELECT bindings.thread_id FROM p_provider_sessions AS sessions
           JOIN p_provider_session_bindings AS bindings
             ON bindings.provider_session_id = sessions.provider_session_id
           WHERE sessions.status NOT IN ('stopped', 'error')
         UNION
         SELECT thread_id FROM p_provider_threads
           WHERE provider_session_id IS NOT NULL AND thread_id IS NOT NULL
             AND json_extract(payload_json, '$.pendingBackgroundTasks') IS NOT NULL
         UNION
         SELECT thread_id FROM p_subagents
           WHERE status IN ('idle', 'pending', 'running', 'waiting')
         ORDER BY 1",
    )?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows
        .into_iter()
        .filter_map(|id| Uuid::parse_str(&id).ok().map(ThreadId))
        .collect())
}

/// `recover:<thread>:<boot_id>`.
pub fn recover_command_id(thread_id: ThreadId, boot_id: Uuid) -> CommandId {
    CommandId::new(format!("recover:{thread_id}:{boot_id}")).expect("short enough")
}

/// Dispatches `RecoverThread` for every candidate thread. Call it once per
/// process start, after the effect reconciliation and before the effect
/// workers start.
pub fn recover_after_restart(
    orchestrator: &Orchestrator,
    boot_id: Uuid,
) -> StoreResult<RecoveryReport> {
    let threads = orchestrator.reader().with(candidate_threads)?;
    let mut failed = Vec::new();
    for &thread_id in &threads {
        match orchestrator.dispatch_internal(
            recover_command_id(thread_id, boot_id),
            InternalCommand::RecoverThread { thread_id, boot_id },
        ) {
            Ok(_) => {}
            Err(DispatchError::Rejected(error)) => failed.push((thread_id, error.to_string())),
            Err(error) => failed.push((thread_id, error.to_string())),
        }
    }
    let continuations = orchestrator.reader().with(|connection| {
        connection
            .query_row(
                "SELECT COUNT(*) FROM effect_outbox
                 WHERE effect_type = 'provider_runtime.continue' AND command_id LIKE ?1",
                [format!("recover:%:{boot_id}")],
                |row| row.get::<_, i64>(0),
            )
            .map_err(crate::store::StoreError::from)
    })?;
    Ok(RecoveryReport {
        boot_id,
        threads,
        failed,
        continuations: usize::try_from(continuations).unwrap_or_default(),
    })
}

/// The `ContinueRun` a `provider_runtime.continue` effect dispatches for
/// `source` (the interrupted run), with its stable command id
/// `restart-continue:<thread>:<run>` and a message id derived from the run.
/// Background work the restart cancelled is named in the message, so the
/// agent can start it again (T3's restart background note).
pub fn continuation_command(thread_id: ThreadId, source: &Run) -> (CommandId, InternalCommand) {
    let command_id = CommandId::new(format!("restart-continue:{thread_id}:{}", source.id))
        .expect("short enough");
    let message_id = MessageId(derived_id(
        "restart_continue_message",
        &source.id.to_string(),
    ));
    let mut text = CONTINUE_TEXT.to_owned();
    if !source.restart_cancelled_background_work.is_empty() {
        let work = source
            .restart_cancelled_background_work
            .iter()
            .map(|work| format!("{} ({})", work.label, work.kind.as_str()))
            .collect::<Vec<_>>()
            .join(", ");
        text.push_str(&format!(
            "\n\nWaku restarted, which stopped background work this turn had started: {work}. \
             Start it again if it is still needed."
        ));
    }
    (
        command_id,
        InternalCommand::ContinueRun {
            thread_id,
            of_run_id: source.id,
            kind: ContinuationKind::Restart,
            message_id,
            text,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn continuation_ids_are_stable_and_name_cancelled_work() {
        let thread_id = ThreadId(Uuid::from_u128(1));
        let mut source = Run {
            id: RunId(Uuid::from_u128(7)),
            thread_id,
            ordinal: Some(1),
            model_selection: ModelSelection::new("codex"),
            provider_thread_id: None,
            user_message_id: MessageId(Uuid::from_u128(8)),
            root_node_id: None,
            active_attempt_id: None,
            status: RunStatus::Interrupted,
            queue_position: None,
            queue_held: false,
            requested_at: 0,
            started_at: None,
            provider_completed_at: None,
            completed_at: Some(1),
            failure: None,
            checkpoint_id: None,
            context_handoff_id: None,
            continuation: None,
            workspace_preparation: None,
            restart_cancelled_background_work: Vec::new(),
            source_plan: None,
            delegated_task_id: None,
            delegated_completion: None,
        };
        let (first_id, first) = continuation_command(thread_id, &source);
        let (again_id, again) = continuation_command(thread_id, &source);
        assert_eq!(first_id, again_id);
        assert_eq!(first, again);
        assert_eq!(
            first_id.as_str(),
            format!("restart-continue:{thread_id}:{}", source.id)
        );
        let InternalCommand::ContinueRun { text, .. } = &first else {
            panic!()
        };
        assert_eq!(text, CONTINUE_TEXT);
        source
            .restart_cancelled_background_work
            .push(CancelledBackgroundWork {
                kind: BackgroundWorkKind::Shell,
                label: "npm run dev".into(),
                id: "t1".into(),
            });
        let (_, noted) = continuation_command(thread_id, &source);
        let InternalCommand::ContinueRun { text, .. } = noted else {
            panic!()
        };
        assert!(text.starts_with(CONTINUE_TEXT));
        assert!(text.contains("npm run dev (shell)"), "{text}");
    }
}
