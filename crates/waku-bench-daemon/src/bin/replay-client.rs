//! `replay-client`: the desktop's launch-and-stream path without a window.
//!
//! Connects with `waku-client`, loads the task state as the desktop's launch
//! does, attaches `--session` and drains that session's events until
//! `turnFinished`, decoding every one with `event_from_wire`, the desktop's
//! own decoder. Against `waku-bench-daemon --autoplay` the attach starts the
//! session's timeline, so one run replays one timeline end to end through the
//! real server, journal and client with no GUI. `scripts/bench/pgo.sh` uses
//! it as PGO training for the daemon and client code, and it gives daemon
//! CPU numbers without the app (#30).
//!
//! ```text
//! replay-client [--address ADDR] [--token TOKEN] --session ID [--timeout SECS]
//! ```
//!
//! The address and token default to `$WAKU_DAEMON_ADDRESS` and
//! `$WAKU_DAEMON_TOKEN`. Prints one JSON line: events, decoded payload bytes
//! and wall time.

use std::time::{Duration, Instant};

use anyhow::{Context as _, anyhow, bail};
use serde_json::json;
use uuid::Uuid;
use waku_client::{Command, DaemonClient, ResponsePayload};
use waku_protocol::{DAEMON_ADDRESS_ENV, DAEMON_TOKEN_ENV};

const USAGE: &str = "\
usage: replay-client [--address ADDR] [--token TOKEN] --session ID [--timeout SECS]

  --address ADDR   daemon address (default: $WAKU_DAEMON_ADDRESS)
  --token TOKEN    daemon token (default: $WAKU_DAEMON_TOKEN)
  --session ID     session to attach; its turn must finish within the timeout
  --timeout SECS   give up after this long without an event (default: 30)";

fn main() -> anyhow::Result<()> {
    let mut address = std::env::var(DAEMON_ADDRESS_ENV).unwrap_or_default();
    let mut token = std::env::var(DAEMON_TOKEN_ENV).unwrap_or_default();
    let mut session = None;
    let mut timeout = Duration::from_secs(30);
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        let mut value = || {
            arguments
                .next()
                .ok_or_else(|| anyhow!("{argument} requires a value"))
        };
        match argument.as_str() {
            "--address" => address = value()?,
            "--token" => token = value()?,
            "--session" => session = Some(value()?.parse::<Uuid>().context("--session")?),
            "--timeout" => timeout = Duration::from_secs(value()?.parse().context("--timeout")?),
            "--help" | "-h" => {
                println!("{USAGE}");
                return Ok(());
            }
            other => bail!("unknown argument {other:?}\n\n{USAGE}"),
        }
    }
    let Some(session_id) = session else {
        bail!("--session is required\n\n{USAGE}");
    };
    if address.trim().is_empty() || token.is_empty() {
        bail!("a daemon address and token are required\n\n{USAGE}");
    }

    let started = Instant::now();
    let client = DaemonClient::connect(address.trim(), token)?;
    let ResponsePayload::TaskState { sessions, .. } =
        client.request(Uuid::nil(), Uuid::nil(), Command::LoadTaskState)?
    else {
        bail!("the daemon returned an invalid task-state response");
    };
    if !sessions.iter().any(|session| session.id == session_id) {
        bail!("session {session_id} is not in the daemon's task state");
    }
    let ResponsePayload::SessionRuntime {
        runtime_id: Some(runtime_id),
        ..
    } = client.request(session_id, Uuid::nil(), Command::AttachSession)?
    else {
        bail!(
            "session {session_id} has no runtime to attach to (start the daemon with --autoplay)"
        );
    };
    let events = client.subscribe(session_id, runtime_id);
    let mut count = 0_u64;
    let mut payload_bytes = 0_u64;
    loop {
        let event = events
            .recv_timeout(timeout)
            .map_err(|_| anyhow!("no event for {timeout:?}; the turn did not finish"))?;
        count += 1;
        payload_bytes += serde_json::to_vec(&event.event).map_or(0, |bytes| bytes.len()) as u64;
        let finished = event.event.kind() == "turnFinished";
        waku_client::event_from_wire(event.event)?;
        if finished {
            break;
        }
    }
    println!(
        "{}",
        json!({
            "session": session_id,
            "events": count,
            "payload_bytes": payload_bytes,
            "elapsed_ms": started.elapsed().as_secs_f64() * 1e3,
        })
    );
    client.shutdown();
    Ok(())
}
