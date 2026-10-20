//! The real daemon (`WakuBackend` with Waku's own Codex and Claude drivers)
//! spawns the fakes and must turn their native streams back into the
//! timeline's transcript.

mod common;

use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{FakeDir, png_url, sample_timeline};
use uuid::Uuid;
use waku_client::{Command, DaemonClient, ResponsePayload, WireDriverStartOptions};
use waku_core::daemon::WakuBackend;
use waku_core::model::{ActivityItem, ActivityKind, DriverEvent, ProviderResumeCursor};
use waku_core::persistence::StateStore;
use waku_core::{DaemonSettingsStore, ServerOptions};

struct RealDaemon {
    address: String,
    shutdown: waku_core::ShutdownSignal,
    server: Option<std::thread::JoinHandle<()>>,
    root: std::path::PathBuf,
}

impl RealDaemon {
    fn start() -> Self {
        let root = std::env::temp_dir().join(format!("waku-real-daemon-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let backend = WakuBackend::new(
            DaemonSettingsStore::open(root.join("settings.json")).unwrap(),
            StateStore::daemon(root.join("app.db")),
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let shutdown = waku_core::ShutdownSignal::new();
        let server_shutdown = shutdown.clone();
        let server = std::thread::spawn(move || {
            waku_core::serve(
                listener,
                "secret".into(),
                Arc::new(backend),
                server_shutdown,
                ServerOptions::default(),
            )
            .unwrap();
        });
        Self {
            address,
            shutdown,
            server: Some(server),
            root,
        }
    }
}

impl Drop for RealDaemon {
    fn drop(&mut self) {
        self.shutdown.trigger();
        if let Some(server) = self.server.take() {
            let _ = server.join();
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// Start `provider` on the fake and run one prompt through the real daemon.
fn run_turn(provider: &str, fake: &FakeDir) -> Vec<DriverEvent> {
    let daemon = RealDaemon::start();
    let client = DaemonClient::connect(&daemon.address, "secret".into()).unwrap();
    let session_id = Uuid::new_v4();
    let runtime_id = Uuid::new_v4();
    let started = client
        .request(
            session_id,
            runtime_id,
            Command::Start {
                options: WireDriverStartOptions {
                    provider: provider.into(),
                    binary: fake.binary.clone(),
                    cwd: fake.dir.clone(),
                    mode: "fullAccess".into(),
                    model: None,
                    reasoning_effort: None,
                    service_tier: None,
                    context_window: None,
                    agent_preset: None,
                    computer_use_enabled: false,
                    provider_cursor: None,
                },
            },
        )
        .unwrap();
    assert!(matches!(started, ResponsePayload::Started { .. }));
    let events = client.subscribe(session_id, runtime_id);
    client
        .notify(
            session_id,
            runtime_id,
            Command::Prompt {
                prompt: "go".into(),
                turn_id: None,
                message_id: None,
            },
        )
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut received = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let event = events
            .recv_timeout(remaining)
            .unwrap_or_else(|_| panic!("{provider}: no turnFinished; got {received:#?}"));
        let event = waku_client::event_from_wire(event.event).unwrap();
        let finished = matches!(event, DriverEvent::TurnFinished { .. });
        received.push(event);
        if finished {
            break;
        }
    }
    client
        .request(session_id, runtime_id, Command::CloseSession)
        .unwrap();
    received
}

fn text(events: &[DriverEvent]) -> String {
    events
        .iter()
        .filter_map(|event| match event {
            DriverEvent::TextDelta(text) => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

fn reasoning(events: &[DriverEvent]) -> String {
    events
        .iter()
        .filter_map(|event| match event {
            DriverEvent::ReasoningDelta(text) => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

/// The final form of each activity, keyed by provider id, in first-seen order.
fn activities(events: &[DriverEvent]) -> Vec<ActivityItem> {
    let mut out: Vec<ActivityItem> = Vec::new();
    for event in events {
        if let DriverEvent::RichActivity(item) = event {
            match out
                .iter_mut()
                .find(|existing| existing.source_id == item.source_id)
            {
                Some(existing) => *existing = item.clone(),
                None => out.push(item.clone()),
            }
        }
    }
    out
}

fn assert_common_transcript(provider: &str, events: &[DriverEvent]) {
    let timeline = sample_timeline();
    assert_eq!(text(events), timeline.assistant_text(), "{provider} text");
    assert_eq!(
        reasoning(events),
        timeline.reasoning_text(),
        "{provider} reasoning"
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, DriverEvent::TurnStarted)),
        "{provider}: no TurnStarted"
    );
    assert!(
        matches!(
            events.last(),
            Some(DriverEvent::TurnFinished { success: true, .. })
        ),
        "{provider}: turn did not finish successfully"
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, DriverEvent::Error(_))),
        "{provider}: unexpected error event in {events:#?}"
    );

    let activities = activities(events);
    assert_eq!(activities.len(), 3, "{provider}: {activities:#?}");
    assert!(activities.iter().all(|item| item.complete));
    let command = &activities[0];
    assert_eq!(command.kind, ActivityKind::Command);
    assert!(
        command
            .output
            .as_deref()
            .is_some_and(|output| output.contains("test result: ok")),
        "{provider}: {command:#?}"
    );
    let edit = &activities[1];
    assert_eq!(edit.kind, ActivityKind::FileChange, "{provider}: {edit:#?}");
    let change = edit.file_changes.first().expect("an edit carries a change");
    assert!(change.path.ends_with("src/a.rs"), "{provider}: {change:#?}");
    let diff = change.diff.as_deref().unwrap_or_default();
    assert!(
        diff.contains("+fn new() {}") && diff.contains("-fn old() {}"),
        "{provider}: diff {diff:?}"
    );
    assert_eq!(activities[2].image_urls, vec![png_url()], "{provider}");
}

/// Whole built-in timelines keep their exact text through both real
/// drivers, which is what "the same visible transcript" rests on.
#[test]
fn builtin_timelines_survive_both_real_drivers() {
    for name in ["burst", "reasoning-fast", "code-wall"] {
        let timeline = waku_bench_daemon::Timeline::builtin(name).unwrap();
        for (provider, binary) in [
            ("codex", env!("CARGO_BIN_EXE_fake-codex")),
            ("claude", env!("CARGO_BIN_EXE_fake-claude")),
        ] {
            let fake = FakeDir::new(provider, binary, &timeline, "max");
            let events = run_turn(provider, &fake);
            assert!(
                text(&events) == timeline.assistant_text(),
                "{provider}/{name}: text differs"
            );
            assert!(
                reasoning(&events) == timeline.reasoning_text(),
                "{provider}/{name}: reasoning differs"
            );
        }
    }
}

#[test]
fn waku_codex_driver_renders_the_fake_codex_stream() {
    let fake = FakeDir::new(
        "codex",
        env!("CARGO_BIN_EXE_fake-codex"),
        &sample_timeline(),
        "max",
    );
    let events = run_turn("codex", &fake);
    // Codex connects once `thread/start` answers, which can be after the
    // daemon has already published the prompt.
    assert!(
        events.iter().any(|event| matches!(
            event,
            DriverEvent::Connected {
                provider_cursor: Some(ProviderResumeCursor::Codex { .. })
            }
        )),
        "{events:#?}"
    );
    assert_common_transcript("codex", &events);
    assert!(events.iter().any(|event| matches!(
        event,
        DriverEvent::UsageUpdated {
            context_tokens: Some(1234),
            context_window: Some(272_000)
        }
    )));
}

#[test]
fn waku_claude_driver_renders_the_fake_claude_stream() {
    let fake = FakeDir::new(
        "claude",
        env!("CARGO_BIN_EXE_fake-claude"),
        &sample_timeline(),
        "max",
    );
    let events = run_turn("claude", &fake);
    assert!(
        matches!(
            events.first(),
            Some(DriverEvent::Connected {
                provider_cursor: Some(ProviderResumeCursor::Claude { .. })
            })
        ),
        "{events:#?}"
    );
    assert_common_transcript("claude", &events);
    assert!(events.iter().any(|event| matches!(
        event,
        DriverEvent::UsageUpdated {
            context_window: Some(272_000),
            ..
        }
    )));
}
