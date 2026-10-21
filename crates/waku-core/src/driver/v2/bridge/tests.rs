//! The bridge serves a `DriverV2` driver behind the old interface.
//!
//! Amp, the simplest driver, runs against a scripted `amp` through
//! `V1Bridge` and real processes: the legacy event stream must tell the
//! story of two turns on one process. (Amp itself is not served through the
//! bridge in the daemon: decision D6.)

#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Duration;

use crossbeam_channel::Receiver;

use super::*;
use crate::driver::amp::{AmpDriver, amp_instance};
use crate::driver::test_event_channel;
use waku_protocol::model::RuntimeMode as LegacyMode;

const AMP_SCRIPT: &str = r#"#!/bin/sh
if [ "$1 $2" = "threads new" ]; then echo 'T-fake-1'; exit 0; fi
n=0
while IFS= read -r line; do
  n=$((n+1))
  if [ $n -eq 1 ]; then echo '{"type":"system","subtype":"init","session_id":"T-fake-1"}'; fi
  echo '{"type":"assistant","message":{"content":[{"type":"thinking","thinking":"Thinking it over."},{"type":"tool_use","id":"tool-'$n'","name":"Bash","input":{"cmd":"ls"}}]}}'
  echo '{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"tool-'$n'","content":"README.md"}]}}'
  echo '{"type":"assistant","message":{"content":[{"type":"text","text":"Hello "},{"type":"text","text":"from amp '$n'"}],"stop_reason":"end_turn"}}'
done
"#;

fn write_script(dir: &Path) -> std::path::PathBuf {
    let path = dir.join("amp");
    std::fs::write(&path, AMP_SCRIPT).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

fn options(binary: &Path, cwd: &Path) -> DriverStartOptions {
    DriverStartOptions {
        binary: binary.to_owned(),
        cwd: cwd.to_owned(),
        mode: LegacyMode::FullAccess,
        model: None,
        reasoning_effort: None,
        service_tier: None,
        context_window: None,
        agent_preset: None,
        computer_use_enabled: false,
        provider_cursor: None,
    }
}

/// What a client can observe, with deltas merged and asynchronous title
/// refreshes left out.
#[derive(Debug, PartialEq, Eq)]
enum Observed {
    Connected(Option<String>),
    TurnStarted,
    Text(String),
    Reasoning(String),
    Activity { id: Option<String>, complete: bool },
    TurnFinished(bool),
    Error(String),
    ProcessExited,
}

fn observe(events: Vec<DriverEvent>) -> Vec<Observed> {
    let mut observed = Vec::new();
    for event in events {
        let next = match event {
            DriverEvent::Connected { provider_cursor } => Observed::Connected(
                provider_cursor.map(|cursor| serde_json::to_string(&cursor).unwrap()),
            ),
            DriverEvent::TurnStarted => Observed::TurnStarted,
            DriverEvent::TextDelta(text) => {
                if let Some(Observed::Text(previous)) = observed.last_mut() {
                    previous.push_str(&text);
                    continue;
                }
                Observed::Text(text)
            }
            DriverEvent::ReasoningDelta(text) => {
                if let Some(Observed::Reasoning(previous)) = observed.last_mut() {
                    previous.push_str(&text);
                    continue;
                }
                Observed::Reasoning(text)
            }
            DriverEvent::Activity { id, complete, .. } => Observed::Activity { id, complete },
            DriverEvent::RichActivity(item) => Observed::Activity {
                id: item.source_id,
                complete: item.complete,
            },
            DriverEvent::TurnFinished { success, .. } => Observed::TurnFinished(success),
            DriverEvent::Error(error) => Observed::Error(error),
            DriverEvent::ProcessExited => Observed::ProcessExited,
            _ => continue,
        };
        observed.push(next);
    }
    observed
}

/// Two turns, then the handle is dropped; returns every event until the
/// process exited.
fn drive(control: Box<dyn DriverControl>, events: Receiver<DriverEvent>) -> Vec<DriverEvent> {
    let mut seen = Vec::new();
    let wait_for = |seen: &mut Vec<DriverEvent>, done: &dyn Fn(&DriverEvent) -> bool| loop {
        let event = events
            .recv_timeout(Duration::from_secs(10))
            .expect("the driver went quiet");
        let finished = done(&event);
        seen.push(event);
        if finished {
            return;
        }
    };
    for prompt in ["one", "two"] {
        control.prompt(prompt.to_owned());
        wait_for(&mut seen, &|event| {
            matches!(event, DriverEvent::TurnFinished { .. })
        });
    }
    drop(control);
    wait_for(&mut seen, &|event| {
        matches!(event, DriverEvent::ProcessExited)
    });
    seen
}

#[test]
fn the_bridge_serves_amp_behind_the_old_interface() {
    let dir = tempfile::tempdir().unwrap();
    let binary = write_script(dir.path());
    let cwd = dir.path().canonicalize().unwrap();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (events, received) = test_event_channel();
    let mut instance = amp_instance();
    instance.binary = binary.clone();
    let bridged = V1Bridge::start(
        Arc::new(AmpDriver::new()),
        instance,
        options(&binary, &cwd),
        events,
        Arc::new(ProcessTransportFactory),
        runtime.handle().clone(),
    )
    .unwrap();
    assert!(bridged.supports_steer());
    let observed = observe(drive(Box::new(bridged), received));
    let turn = |n: u32| {
        vec![
            Observed::TurnStarted,
            Observed::Reasoning("Thinking it over.".to_owned()),
            Observed::Activity {
                id: Some(format!("tool-{n}")),
                complete: false,
            },
            Observed::Activity {
                id: Some(format!("tool-{n}")),
                complete: true,
            },
            Observed::Text(format!("Hello from amp {n}")),
            Observed::TurnFinished(true),
        ]
    };
    let story = observed
        .into_iter()
        .filter(|event| !matches!(event, Observed::Connected(_)))
        .collect::<Vec<_>>();
    let mut expected = turn(1);
    expected.extend(turn(2));
    expected.push(Observed::ProcessExited);
    assert_eq!(story, expected);
}
