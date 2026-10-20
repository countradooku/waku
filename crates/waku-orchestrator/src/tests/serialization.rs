//! #39: commands are serialized per thread; different threads proceed in
//! parallel; the in-memory projection follows every commit.

use std::collections::HashSet;
use std::sync::Arc;

use waku_protocol::orchestration::*;

use super::common::*;
use crate::command::LockKey;
use crate::orchestrator::KeyedLocks;

#[test]
fn concurrent_dispatches_on_one_thread_are_serialized() {
    let env = Arc::new(Env::new());
    let project = env.project();
    let thread = env.thread(project);
    let handles = (0..8)
        .map(|worker| {
            let env = env.clone();
            std::thread::spawn(move || {
                for n in 0..25 {
                    let message = fresh();
                    env.dispatch(
                        &format!("w{worker}-{n}"),
                        Env::send_command(thread, message, "hi", DispatchMode::QueueAfterActive),
                    )
                    .unwrap();
                }
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().unwrap();
    }
    let projection = env.projection(thread);
    assert_eq!(projection.runs.len(), 200);
    let positions = projection
        .runs
        .values()
        .filter_map(|run| run.queue_position)
        .collect::<Vec<_>>();
    assert_eq!(positions.len(), 199, "one run starts, the rest queue");
    assert_eq!(
        positions.iter().collect::<HashSet<_>>().len(),
        199,
        "queue positions are unique"
    );
    assert_eq!(
        projection.active_run().map(|run| run.ordinal),
        Some(Some(1))
    );
    // Sequences of this thread are gap-free in commit order.
    assert_eq!(projection.last_sequence, env.orchestrator.last_sequence());
    env.assert_verified();
    let reloaded = crate::projection::sql::load_thread(&env.orchestrator.store().writer(), thread)
        .unwrap()
        .unwrap();
    assert_eq!(reloaded, *projection);
}

#[test]
fn threads_do_not_block_each_other() {
    let locks = Arc::new(KeyedLocks::default());
    let a = LockKey::Thread(fresh());
    let b = LockKey::Thread(fresh());
    let held = locks.lock(&[a]);
    // Another thread's key is free while `a` is held.
    let other = {
        let locks = locks.clone();
        std::thread::spawn(move || {
            let _guard = locks.lock(&[b]);
            true
        })
    };
    assert!(other.join().unwrap());
    // The same key blocks until released.
    let (sender, receiver) = crossbeam_channel::bounded(1);
    let waiter = {
        let locks = locks.clone();
        std::thread::spawn(move || {
            let _guard = locks.lock(&[a]);
            sender.send(()).unwrap();
        })
    };
    assert!(
        receiver
            .recv_timeout(std::time::Duration::from_millis(50))
            .is_err()
    );
    drop(held);
    receiver
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    waiter.join().unwrap();
    // Unused keys are dropped.
    assert!(locks.is_empty());
}

#[test]
fn multi_key_locks_are_taken_in_sorted_order() {
    let locks = Arc::new(KeyedLocks::default());
    let a = LockKey::Thread(fresh());
    let b = LockKey::Thread(fresh());
    // Opposite argument orders would deadlock without sorting.
    let handles = (0..2)
        .map(|direction| {
            let locks = locks.clone();
            std::thread::spawn(move || {
                for _ in 0..500 {
                    let keys = if direction == 0 { [a, b] } else { [b, a] };
                    let _guard = locks.lock(&keys);
                }
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn cached_projection_matches_storage_after_every_command() {
    let env = Env::with_capabilities(steering_capabilities());
    let project = env.project();
    let thread = env.thread(project);
    let check = |env: &Env| {
        let live = env.projection(thread);
        let loaded =
            crate::projection::sql::load_thread(&env.orchestrator.store().writer(), thread)
                .unwrap()
                .unwrap();
        assert_eq!(*live, loaded);
    };
    let first = env.send(thread, "one", DispatchMode::Auto);
    check(&env);
    env.send(thread, "two", DispatchMode::Auto);
    check(&env);
    go_running(&env, thread, first);
    assistant_text(&env, thread, first, "Hello");
    assistant_text(&env, thread, first, "Hello, world");
    check(&env);
    env.send(
        thread,
        "steer",
        DispatchMode::SteerActive {
            target_run_id: first,
        },
    );
    check(&env);
    provider_done(&env, thread, first);
    check(&env);
}
