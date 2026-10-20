//! A real `waku-client` connection drives the bench daemon exactly as the
//! desktop does and must receive every replayed event, in order.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use uuid::Uuid;
use waku_bench_daemon::fixture;
use waku_bench_daemon::server::{BenchOptions, BenchServer};
use waku_bench_daemon::{Rate, TimedEvent, Timeline};
use waku_client::{Command, DaemonClient, ResponsePayload, SequencedEvent, WireDriverStartOptions};

fn small_timeline() -> Timeline {
    Timeline {
        name: "roundtrip".into(),
        description: "test".into(),
        events: vec![
            TimedEvent::new(0, "turnStarted", Value::Null),
            TimedEvent::new(1_000, "reasoningDelta", json!("Thinking")),
            TimedEvent::new(2_000, "textDelta", json!("Hello")),
            TimedEvent::new(
                3_000,
                "activity",
                json!({"id": "a1", "kind": "command", "title": "ls", "detail": null, "complete": true}),
            ),
            TimedEvent::new(4_000, "textDelta", json!(", world")),
            TimedEvent::new(
                5_000,
                "usageUpdated",
                json!({"contextTokens": 9, "contextWindow": 100}),
            ),
            TimedEvent::new(
                5_000,
                "turnFinished",
                json!({"success": true, "summary": null}),
            ),
        ],
    }
}

fn start(timelines: Vec<Timeline>, rate: Rate, loops: u32) -> BenchServer {
    let state_dir = std::env::temp_dir().join(format!("waku-bench-test-{}", Uuid::new_v4()));
    let default_timeline = timelines[0].name.clone();
    BenchServer::start(BenchOptions {
        listen: "127.0.0.1:0".into(),
        token: "secret".into(),
        timelines: timelines.into_iter().map(Arc::new).collect(),
        default_timeline,
        rate,
        loops,
        provider: waku_core::model::ProviderKind::Codex,
        history_turns: 3,
        history_sessions: 1,
        exit_after: None,
        autoplay: None,
        state_dir,
        project_dir: None,
        allow_shutdown: true,
    })
    .unwrap()
}

fn start_options(provider: &str) -> WireDriverStartOptions {
    WireDriverStartOptions {
        provider: provider.into(),
        binary: "codex".into(),
        cwd: std::env::temp_dir(),
        mode: "fullAccess".into(),
        model: None,
        reasoning_effort: None,
        service_tier: None,
        context_window: None,
        agent_preset: None,
        computer_use_enabled: false,
        provider_cursor: None,
    }
}

/// `(kind, payload)` of each received event, adjacent deltas merged (see
/// [`coalesced`]).
fn wire_parts(events: &[SequencedEvent]) -> Vec<(String, Value)> {
    coalesced(
        &events
            .iter()
            .map(|event| TimedEvent::new(0, event.event.kind(), event.event.payload()))
            .collect::<Vec<_>>(),
    )
}

/// Events with adjacent text (or reasoning) deltas merged. The daemon merges
/// deltas that arrive within its coalescing window, so how a run groups them
/// depends on timing; comparing both sides in this form checks that every
/// event arrived, in order and unaltered, whatever the grouping.
fn coalesced(events: &[TimedEvent]) -> Vec<(String, Value)> {
    let mut merged: Vec<(String, Value)> = Vec::new();
    for event in events {
        let delta = matches!(event.kind.as_str(), "textDelta" | "reasoningDelta");
        if delta
            && let Some((kind, Value::String(text))) = merged.last_mut()
            && *kind == event.kind
        {
            text.push_str(event.payload.as_str().unwrap_or_default());
            continue;
        }
        merged.push((event.kind.clone(), event.payload.clone()));
    }
    merged
}

/// Start a runtime, prompt it, and collect events through `turnFinished`.
fn run_turn(client: &DaemonClient, session_id: Uuid, prompt: &str) -> Vec<SequencedEvent> {
    let runtime_id = Uuid::new_v4();
    let response = client
        .request(
            session_id,
            runtime_id,
            Command::Start {
                options: start_options("codex"),
            },
        )
        .unwrap();
    assert!(matches!(response, ResponsePayload::Started { .. }));
    let events = client.subscribe(session_id, runtime_id);
    client
        .notify(
            session_id,
            runtime_id,
            Command::Prompt {
                prompt: prompt.into(),
                turn_id: None,
                message_id: None,
            },
        )
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut received = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let event = events
            .recv_timeout(remaining)
            .expect("timed out waiting for the replay to finish");
        let finished = event.event.kind() == "turnFinished";
        received.push(event);
        if finished {
            break;
        }
    }
    received
}

#[test]
fn desktop_handshake_task_state_and_replay_round_trip() {
    let timeline = small_timeline();
    let server = start(vec![timeline.clone()], Rate::Factor(1.0), 1);
    let address = server.address.to_string();

    assert!(
        DaemonClient::connect(&address, "wrong".into()).is_err(),
        "a wrong token must be rejected like the real daemon does"
    );
    let client = DaemonClient::connect(&address, "secret".into()).unwrap();

    let ResponsePayload::TaskState {
        projects, sessions, ..
    } = client
        .request(Uuid::nil(), Uuid::nil(), Command::LoadTaskState)
        .unwrap()
    else {
        panic!("expected task state");
    };
    assert_eq!(projects.len(), 1);
    assert_eq!(projects[0].id, fixture::PROJECT_ID);
    let mut titles = sessions
        .iter()
        .map(|s| s.title.as_str())
        .collect::<Vec<_>>();
    titles.sort_unstable();
    assert_eq!(titles, ["Bench · history (3 turns)", "Bench · roundtrip"]);
    let history_id = sessions
        .iter()
        .find(|session| session.title.contains("history"))
        .unwrap()
        .id;

    // Hydration goes through the real persistence backend.
    let ResponsePayload::Session {
        session: Some(history),
    } = client
        .request(
            Uuid::nil(),
            Uuid::nil(),
            Command::HydrateSession {
                session_id: history_id,
            },
        )
        .unwrap()
    else {
        panic!("expected a hydrated session");
    };
    assert_eq!(history.messages.len(), 6);

    let ResponsePayload::ProviderProbe { probe, .. } = client
        .request(
            Uuid::nil(),
            Uuid::nil(),
            Command::ProbeProvider {
                provider: waku_core::model::ProviderKind::Codex,
                binary_override: None,
                discover_models: false,
                probe_version: true,
            },
        )
        .unwrap()
    else {
        panic!("expected a probe");
    };
    assert!(probe.installed);

    let session_id = fixture::session_id("roundtrip");
    assert!(sessions.iter().any(|session| session.id == session_id));
    let received = run_turn(&client, session_id, "do the thing");
    let kinds = received
        .iter()
        .map(|event| event.event.kind())
        .collect::<Vec<_>>();
    assert_eq!(kinds[0], "connected");
    assert_eq!(kinds[1], "promptSubmitted");
    assert_eq!(received[1].event.payload()["message"], "do the thing");
    let replayed = &received[2..];
    for got in replayed {
        // Every event must decode with the desktop's own decoder.
        waku_client::event_from_wire(got.event.clone()).unwrap();
    }
    assert_eq!(wire_parts(replayed), coalesced(&timeline.events));
    assert!(
        received
            .windows(2)
            .all(|pair| pair[1].sequence == pair[0].sequence + 1),
        "sequence numbers must be contiguous"
    );

    let stats = server.stop().unwrap();
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].events, timeline.events.len() as u64);
    assert!(!stats[0].canceled);
}

#[test]
fn builtin_timelines_arrive_complete_and_in_order_at_max_rate() {
    let burst = Timeline::builtin("burst").unwrap();
    let images = Timeline::builtin("images").unwrap();
    let server = start(vec![burst.clone(), images.clone()], Rate::Max, 1);
    let client = DaemonClient::connect(&server.address.to_string(), "secret".into()).unwrap();

    // The prompt text selects a timeline by name, whatever the session.
    for timeline in [&burst, &images] {
        let received = run_turn(&client, fixture::session_id("burst"), &timeline.name);
        let replayed = &received[2..];
        assert!(
            wire_parts(replayed) == coalesced(&timeline.events),
            "{} arrived out of order or altered",
            timeline.name
        );
    }
    server.stop().unwrap();
}

#[test]
fn loops_and_cancel() {
    let server = start(vec![small_timeline()], Rate::Max, 3);
    let client = DaemonClient::connect(&server.address.to_string(), "secret".into()).unwrap();
    let received = run_turn(&client, fixture::session_id("roundtrip"), "go");
    let text = received
        .iter()
        .filter(|event| event.event.kind() == "textDelta")
        .filter_map(|event| match &event.event {
            waku_protocol::WireDriverEvent::TextDelta(text) => Some(text.as_str()),
            _ => None,
        })
        .collect::<String>();
    assert_eq!(text, "Hello, world".repeat(3));
    assert_eq!(
        received
            .iter()
            .filter(|event| event.event.kind() == "turnStarted")
            .count(),
        1
    );
    server.stop().unwrap();

    // A slow replay canceled mid-turn finishes the turn unsuccessfully.
    let server = start(
        vec![Timeline::builtin("prose").unwrap()],
        Rate::Factor(1.0),
        1,
    );
    let client = DaemonClient::connect(&server.address.to_string(), "secret".into()).unwrap();
    let session_id = fixture::session_id("prose");
    let runtime_id = Uuid::new_v4();
    client
        .request(
            session_id,
            runtime_id,
            Command::Start {
                options: start_options("claude"),
            },
        )
        .unwrap();
    let events = client.subscribe(session_id, runtime_id);
    client
        .notify(
            session_id,
            runtime_id,
            Command::Prompt {
                prompt: "write".into(),
                turn_id: None,
                message_id: None,
            },
        )
        .unwrap();
    // Wait for a few deltas, then cancel.
    let mut deltas = 0;
    while deltas < 3 {
        let event = events.recv_timeout(Duration::from_secs(10)).unwrap();
        if event.event.kind() == "textDelta" {
            deltas += 1;
        }
    }
    client
        .notify(session_id, runtime_id, Command::Cancel)
        .unwrap();
    let finished = loop {
        let event = events.recv_timeout(Duration::from_secs(10)).unwrap();
        if event.event.kind() == "turnFinished" {
            break event;
        }
    };
    assert_eq!(finished.event.payload()["success"], false);
    let stats = server.stop().unwrap();
    assert!(stats[0].canceled);
}

/// Wire cost of the `burst` timeline at its recorded rate, as a client
/// receives it: events and JSON bytes. Not a pass/fail test; run with
/// `cargo test -p waku-bench-daemon --release --test protocol_roundtrip --
/// --ignored --nocapture burst_wire_cost`.
#[test]
#[ignore = "measurement; takes the timeline's full replay time"]
fn burst_wire_cost() {
    let burst = Timeline::builtin("burst").unwrap();
    let server = start(vec![burst.clone()], Rate::Factor(1.0), 1);
    let client = DaemonClient::connect(&server.address.to_string(), "secret".into()).unwrap();
    let started = Instant::now();
    let received = run_turn(&client, fixture::session_id("burst"), &burst.name);
    let elapsed = started.elapsed().as_secs_f64();
    let bytes = received
        .iter()
        .map(|event| {
            serde_json::to_string(&waku_client::ServerMessage::Event(event.clone()))
                .unwrap()
                .len()
        })
        .sum::<usize>();
    // What the same replay costs one event per provider chunk, as before
    // daemon-side coalescing.
    let template = &received[0];
    let uncoalesced_bytes = burst
        .events
        .iter()
        .enumerate()
        .map(|(index, event)| {
            let mut sequenced = template.clone();
            sequenced.sequence = index as u64 + 3;
            sequenced.event = event.to_wire();
            serde_json::to_string(&waku_client::ServerMessage::Event(sequenced))
                .unwrap()
                .len()
        })
        .sum::<usize>();
    println!(
        "burst: {} timeline events ({} JSON bytes one event per chunk) -> {} wire events, {} JSON bytes, over {elapsed:.2}s ({:.0} events/s)",
        burst.events.len(),
        uncoalesced_bytes,
        received.len(),
        bytes,
        received.len() as f64 / elapsed,
    );
    assert_eq!(wire_parts(&received[2..]), coalesced(&burst.events));
    server.stop().unwrap();
}
