//! `--autoplay`: a client that only attaches, as the desktop does for its
//! selected session, receives the prompt and the whole timeline without
//! sending a prompt itself.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use uuid::Uuid;
use waku_bench_daemon::fixture;
use waku_bench_daemon::server::{BenchOptions, BenchServer};
use waku_bench_daemon::{Rate, TimedEvent, Timeline};
use waku_client::{Command, DaemonClient, ResponsePayload};

fn timeline() -> Timeline {
    Timeline {
        name: "autoplayed".into(),
        description: "test".into(),
        events: vec![
            TimedEvent::new(0, "turnStarted", Value::Null),
            TimedEvent::new(1_000, "textDelta", json!("Hello")),
            TimedEvent::new(2_000, "textDelta", json!(", world")),
            TimedEvent::new(
                3_000,
                "turnFinished",
                json!({"success": true, "summary": null}),
            ),
        ],
    }
}

fn start(delay: Duration) -> BenchServer {
    let timeline = timeline();
    BenchServer::start(BenchOptions {
        listen: "127.0.0.1:0".into(),
        token: "secret".into(),
        default_timeline: timeline.name.clone(),
        timelines: vec![Arc::new(timeline)],
        rate: Rate::Factor(1.0),
        loops: 1,
        provider: waku_core::model::ProviderKind::Codex,
        history_turns: 2,
        history_sessions: 3,
        exit_after: None,
        autoplay: Some(delay),
        state_dir: std::env::temp_dir().join(format!("waku-bench-autoplay-{}", Uuid::new_v4())),
        project_dir: None,
        allow_shutdown: true,
    })
    .unwrap()
}

#[test]
fn autoplay_waits_for_an_attach_then_streams_the_timeline_to_it() {
    let delay = Duration::from_millis(200);
    let server = start(delay);
    let session_id = fixture::session_id("autoplayed");
    assert_eq!(server.autoplay_session, Some(session_id));

    // Nothing plays until a client attaches, however long that takes.
    std::thread::sleep(delay * 3);
    assert!(
        server.stats().is_empty(),
        "autoplay must wait for an attach"
    );

    // The desktop's startup path: load the catalog, then attach the
    // selected session. The extra history sessions are listed too.
    let client = DaemonClient::connect(&server.address.to_string(), "secret".into()).unwrap();
    let ResponsePayload::TaskState { sessions, .. } = client
        .request(Uuid::nil(), Uuid::nil(), Command::LoadTaskState)
        .unwrap()
    else {
        panic!("expected task state");
    };
    assert_eq!(
        sessions
            .iter()
            .filter(|session| session.title.contains("history"))
            .count(),
        3
    );
    let attached_at = Instant::now();
    let ResponsePayload::SessionRuntime {
        runtime_id: Some(runtime_id),
        ..
    } = client
        .request(session_id, Uuid::nil(), Command::AttachSession)
        .unwrap()
    else {
        panic!("the autoplay session must already have a runtime to attach to");
    };
    let events = client.subscribe(session_id, runtime_id);

    let mut received = Vec::new();
    loop {
        let event = events
            .recv_timeout(Duration::from_secs(10))
            .expect("timed out waiting for the autoplayed turn");
        let finished = event.event.kind() == "turnFinished";
        received.push(event);
        if finished {
            break;
        }
    }
    assert!(
        attached_at.elapsed() >= delay,
        "the prompt must wait for the configured delay"
    );
    let mut kinds = received
        .iter()
        .map(|event| event.event.kind())
        .collect::<Vec<_>>();
    // The daemon may merge the two adjacent text deltas into one event,
    // depending on how far apart they arrive.
    kinds.dedup();
    // `connected` was journaled before this client existed and is replayed
    // to it on subscribe; the prompt and the replay follow.
    assert_eq!(
        kinds,
        [
            "connected",
            "promptSubmitted",
            "turnStarted",
            "textDelta",
            "turnFinished"
        ]
    );
    let prompt = waku_client::event_from_wire(received[1].event.clone()).unwrap();
    let waku_core::model::DriverEvent::PromptSubmitted { message, .. } = prompt else {
        panic!("expected promptSubmitted, got {prompt:?}");
    };
    assert_eq!(message, "autoplayed");

    // A second attach does not replay again.
    client
        .request(session_id, Uuid::nil(), Command::AttachSession)
        .unwrap();
    std::thread::sleep(delay * 2);
    let stats = server.stop().unwrap();
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].events, 4);
    assert!(!stats[0].canceled);
}
