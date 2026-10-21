//! `--autoplay`: the bench daemon prompts its timeline thread only once a
//! client subscribes to that thread, after the configured delay, and the
//! subscriber receives the whole run through the orchestrator path.
#![cfg(unix)]

mod common;

use std::time::{Duration, Instant};

use common::{Bench, run_text, sample_timeline, snapshot, wait_settled};
use waku_client::v8::{StreamEvent, SubscribeOptions, stream};
use waku_protocol::orchestration::{DomainEvent, RunStatus, ThreadId, TurnItemKind};
use waku_protocol::v8::StreamItem;
use waku_protocol::v8::orchestration::{ThreadItem, ThreadSpec};

#[test]
fn autoplay_waits_for_a_subscription_then_streams_the_timeline_to_it() {
    let delay = Duration::from_millis(400);
    let timeline = sample_timeline();
    let (mut options, root) = Bench::options(&timeline, "1");
    options.autoplay = Some(delay);
    options.history_turns = 2;
    options.history_sessions = 3;
    let bench = Bench::start(options, root);
    let thread_id = bench.server().default_thread;
    assert_eq!(bench.server().autoplay_thread, Some(thread_id));
    assert_eq!(bench.server().history_threads.len(), 3);

    // Nothing plays until a client subscribes, however long that takes;
    // reads do not count.
    let client = bench.connect();
    std::thread::sleep(delay * 2);
    assert_eq!(snapshot(&client, thread_id).projection.runs.len(), 1);
    assert!(bench.server().reports().is_empty(), "autoplay must wait");

    // The desktop's launch path: subscribe to the selected thread.
    let subscribed = Instant::now();
    let subscription = client.subscribe::<stream::Thread>(
        ThreadSpec {
            thread_id: ThreadId(thread_id),
            compact_turn_items: true,
        },
        SubscribeOptions::default(),
    );
    let mut prompt_at = None;
    let mut prompt = None;
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match subscription
            .events()
            .recv_timeout(remaining)
            .expect("the autoplayed run never completed")
        {
            StreamEvent::Item(StreamItem::Thread(ThreadItem::Event { event, .. })) => match event {
                DomainEvent::TurnItemUpdated(item) => {
                    if let TurnItemKind::UserMessage { text, .. } = &item.kind
                        && prompt.is_none()
                    {
                        prompt_at = Some(subscribed.elapsed());
                        prompt = Some(text.clone());
                    }
                }
                DomainEvent::RunUpdated(run) if run.status == RunStatus::Completed => break,
                _ => {}
            },
            StreamEvent::End(end) => panic!("the thread stream ended: {end:?}"),
            _ => {}
        }
    }
    assert!(
        prompt_at.expect("a user message streamed") >= delay,
        "the prompt waits for the configured delay"
    );
    assert_eq!(prompt.as_deref(), Some(timeline.name.as_str()));

    let settled = wait_settled(&client, thread_id, 2);
    let run = settled
        .projection
        .runs
        .iter()
        .find(|run| run.ordinal == Some(2))
        .expect("the autoplayed run");
    assert_eq!(run.status, RunStatus::Completed);
    assert_eq!(run_text(&settled, run.id), timeline.assistant_text());

    // The daemon reports the run with the fake's replay.
    let deadline = Instant::now() + Duration::from_secs(10);
    while bench.server().reports().is_empty() {
        assert!(Instant::now() < deadline, "the run was never reported");
        std::thread::sleep(Duration::from_millis(20));
    }
    let reports = bench.server().reports();
    let replay = reports[0].replay.as_ref().expect("the fake's replay");
    assert_eq!(replay.timeline, timeline.name);
    assert_eq!(replay.events, timeline.events.len() as u64);
    assert!(!replay.canceled);
    assert_eq!(reports[0].status, RunStatus::Completed);

    // A second subscription does not prompt again.
    drop(subscription);
    let _again = client.subscribe::<stream::Thread>(
        ThreadSpec {
            thread_id: ThreadId(thread_id),
            compact_turn_items: true,
        },
        SubscribeOptions::default(),
    );
    std::thread::sleep(delay * 2);
    assert_eq!(snapshot(&client, thread_id).projection.runs.len(), 2);
    assert_eq!(bench.server().reports().len(), 1);
}
