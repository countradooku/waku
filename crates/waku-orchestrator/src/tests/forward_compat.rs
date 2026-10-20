//! #43: rows written by a newer version decode to skippable records instead
//! of failing a load, a rebuild or a worker.

use rusqlite::params;
use waku_protocol::orchestration::*;

use super::common::*;

#[test]
fn unknown_events_in_the_log_are_skipped_by_rebuild_and_catch_up() {
    let mut env = Env::new();
    let project = env.project();
    let thread = env.thread(project);
    env.send(thread, "before", DispatchMode::Auto);
    {
        let writer = env.orchestrator.store().writer();
        writer
            .execute(
                "INSERT INTO events (event_id, thread_id, entity_id, event_type, occurred_at,
                                     payload_json)
                 VALUES (?1, ?2, 'x', 'hologram.updated', 1, '{\"frames\":[1,2]}')",
                params![fresh::<EventId>().to_string(), thread.to_string()],
            )
            .unwrap();
        // A newer binary would have advanced the projection with it.
        writer
            .execute(
                "UPDATE projection_metadata SET last_sequence = (SELECT MAX(sequence) FROM events)",
                [],
            )
            .unwrap();
    }
    env.assert_verified();
    // A rebuild and a catch-up both step over it.
    env.orchestrator.rebuild_projections().unwrap();
    env.assert_verified();
    env.orchestrator
        .store()
        .writer()
        .execute("UPDATE projection_metadata SET last_sequence = 0", [])
        .unwrap();
    env.reopen();
    env.assert_verified();
    env.send(thread, "after", DispatchMode::QueueAfterActive);
    env.assert_verified();
}

#[test]
fn unknown_values_inside_stored_entities_decode_to_unknown() {
    let env = Env::new();
    let project = env.project();
    let thread = env.thread(project);
    let run = env.send(thread, "go", DispatchMode::Auto);
    {
        let writer = env.orchestrator.store().writer();
        writer
            .execute(
                "UPDATE p_runs SET payload_json = json_set(payload_json, '$.status', 'teleporting',
                                                           '$.futureField', 'x')
                 WHERE run_id = ?1",
                [run.to_string()],
            )
            .unwrap();
    }
    let loaded = crate::projection::sql::load_thread(&env.orchestrator.store().writer(), thread)
        .unwrap()
        .unwrap();
    assert_eq!(loaded.runs[&run].status, RunStatus::Unknown);
}
