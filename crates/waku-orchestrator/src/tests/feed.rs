//! #40: subscribers see committed events only, in sequence order; a slow
//! subscriber is ended instead of growing memory.

use std::sync::Arc;
use std::time::Duration;

use waku_protocol::orchestration::*;

use super::common::*;
use crate::feed::FeedEnded;
use crate::store::faults;
use crate::store::sink::FaultPoint;
use crate::store::sink::WriteGuard;

#[test]
fn subscribers_receive_committed_events_in_order() {
    let env = Arc::new(Env::new());
    let feed = env.orchestrator.subscribe();
    let project = env.project();
    let threads = (0..4).map(|_| env.thread(project)).collect::<Vec<_>>();
    let handles = threads
        .iter()
        .map(|thread| {
            let env = env.clone();
            let thread = *thread;
            std::thread::spawn(move || {
                for _ in 0..10 {
                    env.send(thread, "hi", DispatchMode::QueueAfterActive);
                }
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().unwrap();
    }
    let events = feed.drain();
    assert_eq!(events.len() as i64, env.count("events"));
    assert!(
        events
            .windows(2)
            .all(|pair| pair[1].sequence == pair[0].sequence + 1),
        "feed is gap-free and ordered"
    );
    assert_eq!(
        events.last().unwrap().sequence,
        env.orchestrator.last_sequence()
    );
}

#[test]
fn aborted_commits_publish_nothing() {
    let env = Env::new();
    let project = env.project();
    let thread = env.thread(project);
    let feed = env.orchestrator.subscribe();
    faults::arm(FaultPoint::AfterOutbox);
    assert!(
        env.try_ok(Env::send_command(thread, fresh(), "x", DispatchMode::Auto))
            .is_err()
    );
    assert!(feed.try_recv().is_none());
    // A failed guard publishes nothing either.
    let outcome = env.ingest(
        thread,
        crate::decide::Decision {
            events: vec![DomainEvent::ThreadUpdated(ThreadUpdate {
                change: ThreadChange::Activity,
                thread: env.projection(thread).thread.clone(),
            })],
            ..Default::default()
        },
        WriteGuard::RunCurrent {
            run_id: fresh(),
            attempt_id: fresh(),
        },
    );
    assert_eq!(outcome, crate::orchestrator::IngestOutcome::Stale);
    assert!(feed.try_recv().is_none());
}

#[test]
fn a_slow_subscriber_is_ended_with_overflow() {
    let env = Env::new();
    let project = env.project();
    let thread = env.thread(project);
    let feed = env.orchestrator.subscribe_with_capacity(4);
    let healthy = env.orchestrator.subscribe();
    for _ in 0..5 {
        env.send(thread, "hi", DispatchMode::QueueAfterActive);
    }
    let mut received = 0;
    loop {
        match feed.recv_timeout(Duration::from_millis(10)) {
            Ok(Some(_)) => received += 1,
            Ok(None) => panic!("an overflowed feed must end"),
            Err(ended) => {
                assert_eq!(ended, FeedEnded::Overflowed);
                break;
            }
        }
    }
    assert!(received <= 4);
    assert!(feed.overflowed());
    // Others are unaffected.
    assert_eq!(healthy.drain().len() as i64, env.count("events") - 2);
}
