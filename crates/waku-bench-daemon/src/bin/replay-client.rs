//! `replay-client`: the desktop's launch-and-stream path without a window.
//!
//! Connects over protocol v8 through the desktop's own client layer
//! (`waku_client::store::ProjectionStore`: the shell and thread streams, the
//! projection reducers and the render views), opens `--thread` as the
//! desktop opens its selected thread at launch, and follows it until a new
//! turn settles. Against `waku-bench-daemon --autoplay` that subscription
//! starts the thread's timeline, so one run replays one timeline end to end
//! through the orchestrator, the provider session manager, a fake provider
//! CLI, the server and the client with no GUI. With `--prompt` it sends the
//! message itself, as the composer would. `scripts/bench/pgo.sh` uses it as
//! PGO training for the daemon and client code, and it gives daemon CPU
//! numbers without the app (#30).
//!
//! ```text
//! replay-client [--address ADDR] [--token TOKEN] --thread ID [--prompt TEXT]
//!               [--timeout SECS]
//! ```
//!
//! The address and token default to `$WAKU_DAEMON_ADDRESS` and
//! `$WAKU_DAEMON_TOKEN`, the thread to `$WAKU_BENCH_THREAD_ID`. Prints one
//! JSON line: view updates, the new turns' assistant bytes and wall times.

use std::time::{Duration, Instant};

use anyhow::{Context as _, anyhow, bail};
use serde_json::json;
use uuid::Uuid;
use waku_client::commands::{self, Outgoing, SendIntent};
use waku_client::store::{ProjectionStore, StoreConfig, StoreNotice};
use waku_client::thread_view::ThreadView;
use waku_client::v8::V8Client;
use waku_protocol::model::{MessageRole, SessionStatus, TurnStatus};

const USAGE: &str = "\
usage: replay-client [--address ADDR] [--token TOKEN] --thread ID [--prompt TEXT]
                     [--timeout SECS]

  --address ADDR   daemon address: host:port or unix:PATH (default: $WAKU_DAEMON_ADDRESS)
  --token TOKEN    daemon token (default: $WAKU_DAEMON_TOKEN)
  --thread ID      thread to open (default: $WAKU_BENCH_THREAD_ID); a new turn
                   must settle in it within the timeout
  --prompt TEXT    send TEXT to the thread once it is open (without it, the
                   daemon must prompt it: waku-bench-daemon --autoplay)
  --timeout SECS   give up after this long without a view update (default: 30)";

/// Whether `view` holds a turn beyond the first `turns` and every turn has
/// settled.
fn settled(view: &ThreadView, turns: usize) -> bool {
    view.turns.len() > turns
        && view
            .turns
            .iter()
            .all(|turn| turn.status != TurnStatus::Running)
        && view.status == SessionStatus::Idle
        && view.queued_messages.is_empty()
}

fn assistant_bytes(view: &ThreadView) -> usize {
    view.messages
        .iter()
        .filter(|message| message.role == MessageRole::Assistant)
        .map(|message| message.content.len())
        .sum()
}

fn main() -> anyhow::Result<()> {
    let mut address = std::env::var("WAKU_DAEMON_ADDRESS").unwrap_or_default();
    let mut token = std::env::var("WAKU_DAEMON_TOKEN").unwrap_or_default();
    let mut thread = std::env::var("WAKU_BENCH_THREAD_ID").ok();
    let mut prompt = None;
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
            "--thread" => thread = Some(value()?),
            "--prompt" => prompt = Some(value()?),
            "--timeout" => timeout = Duration::from_secs(value()?.parse().context("--timeout")?),
            "--help" | "-h" => {
                println!("{USAGE}");
                return Ok(());
            }
            other => bail!("unknown argument {other:?}\n\n{USAGE}"),
        }
    }
    let Some(thread) = thread.filter(|thread| !thread.is_empty()) else {
        bail!("--thread is required\n\n{USAGE}");
    };
    let thread_id = thread.parse::<Uuid>().context("--thread")?;
    let address = address.trim().to_owned();
    if address.is_empty() || token.is_empty() {
        bail!("a daemon address and token are required\n\n{USAGE}");
    }

    let started = Instant::now();
    let (wake_tx, wake) = crossbeam_channel::bounded(1);
    let connect_address = address.clone();
    let store = ProjectionStore::start(
        Box::new(move || V8Client::connect(&connect_address, token.clone())),
        move || {
            let _ = wake_tx.try_send(());
        },
        StoreConfig::default(),
    );
    store.open_thread(thread_id);
    if !store.wait_ready(&[thread_id], timeout) {
        bail!("thread {thread_id} did not load within {timeout:?}");
    }
    let opened_ms = started.elapsed().as_secs_f64() * 1e3;

    let mut view: Option<std::sync::Arc<ThreadView>> = None;
    let mut initial_turns = None;
    let mut updates = 0_u64;
    let mut prompted = prompt.is_none();
    let mut first_new_turn_ms = None;
    loop {
        let drained = store.take_updates();
        for notice in drained.notices {
            if let StoreNotice::DispatchFailed {
                command_type,
                error,
                ..
            } = notice
            {
                bail!("{command_type} failed: {error:?}");
            }
        }
        for (id, update) in drained.threads {
            if id == thread_id {
                updates += 1;
                view = update.view;
            }
        }
        if let Some(current) = view.as_deref() {
            let turns = *initial_turns.get_or_insert(current.turns.len());
            if !prompted {
                let text = prompt.as_deref().unwrap_or_default();
                store.dispatch(commands::send_message(
                    thread_id,
                    &Outgoing {
                        text: text.to_owned(),
                        ..Outgoing::default()
                    },
                    commands::dispatch_mode(current, SendIntent::Submit),
                    None,
                ));
                prompted = true;
            }
            if first_new_turn_ms.is_none() && current.turns.len() > turns {
                first_new_turn_ms = Some(started.elapsed().as_secs_f64() * 1e3);
            }
            if settled(current, turns) {
                break;
            }
        }
        if wake.recv_timeout(timeout).is_err() {
            bail!("no update for {timeout:?}; the turn did not settle");
        }
    }
    let view = view.expect("a settled view");
    let initial_turns = initial_turns.unwrap_or_default();
    println!(
        "{}",
        json!({
            "thread": thread_id,
            "address": address,
            "turns": view.turns.len() - initial_turns,
            "status": format!("{:?}", view.turns.last().map(|turn| turn.status)),
            "view_updates": updates,
            "assistant_bytes": assistant_bytes(&view),
            "opened_ms": opened_ms,
            "first_turn_ms": first_new_turn_ms,
            "elapsed_ms": started.elapsed().as_secs_f64() * 1e3,
        })
    );
    store.shutdown();
    Ok(())
}
