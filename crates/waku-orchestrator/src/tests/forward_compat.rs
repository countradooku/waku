//! #43: rows written by a newer version decode to skippable records instead
//! of failing a load, a rebuild or a worker.

use std::sync::Arc;
use std::time::Duration;

use rusqlite::params;
use waku_protocol::orchestration::*;

use super::common::*;
use crate::effects::EffectType;
use crate::effects::worker::{EffectWorkerPool, WorkerConfig};
use crate::fakes::ScriptedExecutor;
use crate::store::outbox::EffectStatus;

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

#[test]
fn an_unknown_effect_type_fails_its_row_and_frees_the_lane() {
    let env = Arc::new(Env::new());
    let project = env.project();
    let thread = env.thread(project);
    {
        let writer = env.orchestrator.store().writer();
        writer
            .execute(
                "INSERT INTO effect_outbox (lane_key, thread_id, effect_type, lane, process_bound,
                   status, available_at, payload_json, created_at, updated_at)
                 VALUES (?1, ?1, 'teleport.start', 'thread', 0, 'pending', 0,
                         '{\"type\":\"teleport.start\"}', 0, 0)",
                [thread.to_string()],
            )
            .unwrap();
    }
    env.send(thread, "go", DispatchMode::Auto);
    let executor = Arc::new(ScriptedExecutor::default());
    let pool = EffectWorkerPool::start(
        env.orchestrator.clone(),
        executor.clone(),
        WorkerConfig {
            workers: 2,
            ..WorkerConfig::default()
        },
    );
    // The turn start behind the unknown row still runs.
    assert!(executor.wait_started(EffectType::ProviderTurnStart, Duration::from_secs(5), None));
    pool.shutdown(Duration::from_secs(1));
    let outbox = env.outbox();
    assert_eq!(outbox[0].1, "teleport.start");
    assert_eq!(outbox[0].2, EffectStatus::Failed);
}
