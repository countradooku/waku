//! `--autoplay`: replay a timeline into the desktop without touching its UI.
//!
//! The bench daemon plays the part of a second client, the way the web client
//! or a second desktop drives a shared daemon:
//!
//! 1. Before the address is printed, an internal [`DaemonClient`] connects to
//!    this daemon and sends `start` for the autoplay session. The hub records
//!    the runtime exactly as for a desktop-started one.
//! 2. The desktop launches with that session selected (its own `state.json`)
//!    and asks `attachSession` for it, as it does for every selected session
//!    that has started. The answer names the running runtime, so the desktop
//!    subscribes to it through the ordinary attach path (`attach_remote`).
//! 3. That `attachSession` is the signal that the desktop can render the
//!    stream. After `delay`, the internal client sends `prompt`. The backend
//!    publishes `promptSubmitted` first, which the desktop adopts as a user
//!    message and a running turn, then replays the timeline. The desktop
//!    renders the stream through the same event pump, rows and commits as for
//!    a prompt typed into its own composer; only the composer itself is not
//!    exercised.
//! 4. The internal client then disconnects, so the daemon writes the stream
//!    to the desktop's socket only and its CPU is not doubled.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context as _, bail};
use uuid::Uuid;
use waku_client::{Command, DaemonClient, ResponsePayload, WireDriverStartOptions};
use waku_core::model::ProviderKind;
use waku_protocol::encode_enum;

use crate::backend::BenchBackend;

/// Delay between the desktop attaching and the prompt when `--autoplay` has
/// no value: long enough for the attach to land on the UI thread and the
/// first frames of the selected transcript to settle.
pub const DEFAULT_DELAY: Duration = Duration::from_millis(1_500);

#[derive(Clone, Debug)]
pub struct AutoplayOptions {
    pub session_id: Uuid,
    /// The prompt text; a timeline name selects that timeline.
    pub prompt: String,
    pub provider: ProviderKind,
    pub cwd: PathBuf,
    pub delay: Duration,
}

pub(crate) struct Autoplay {
    pub thread: JoinHandle<()>,
}

impl Autoplay {
    /// Start the autoplay runtime, then wait in the background for the
    /// desktop to attach. Returns once the runtime exists, so a desktop
    /// launched afterwards always finds it.
    pub(crate) fn start(
        address: &str,
        token: String,
        backend: &BenchBackend,
        shutdown: Arc<AtomicBool>,
        options: AutoplayOptions,
    ) -> anyhow::Result<Self> {
        let client = DaemonClient::connect(address, token)
            .context("autoplay could not connect to the bench daemon")?;
        let attached = backend.watch_attach(options.session_id);
        let runtime_id = Uuid::new_v4();
        let response = client.request(
            options.session_id,
            runtime_id,
            Command::Start {
                options: WireDriverStartOptions {
                    provider: encode_enum(options.provider)?,
                    binary: PathBuf::from(options.provider.command()),
                    cwd: options.cwd.clone(),
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
        )?;
        if !matches!(response, ResponsePayload::Started { .. }) {
            bail!("autoplay start returned {response:?}");
        }
        let thread = std::thread::Builder::new()
            .name("waku-bench-autoplay".into())
            .spawn(move || {
                let waited = Instant::now();
                loop {
                    if shutdown.load(Ordering::Acquire) {
                        client.disconnect();
                        return;
                    }
                    match attached.recv_timeout(Duration::from_millis(100)) {
                        Ok(()) => break,
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                            client.disconnect();
                            return;
                        }
                    }
                }
                eprintln!(
                    "waku-bench-daemon: autoplay: a client attached to {} after {:.1}s; prompting {:?} in {} ms",
                    options.session_id,
                    waited.elapsed().as_secs_f64(),
                    options.prompt,
                    options.delay.as_millis(),
                );
                let deadline = Instant::now() + options.delay;
                while Instant::now() < deadline {
                    if shutdown.load(Ordering::Acquire) {
                        client.disconnect();
                        return;
                    }
                    std::thread::sleep(
                        deadline
                            .saturating_duration_since(Instant::now())
                            .min(Duration::from_millis(50)),
                    );
                }
                // A machine-readable marker for runners timing the replay.
                eprintln!("waku-bench-daemon: autoplay prompt sent");
                let result = client.request(
                    options.session_id,
                    runtime_id,
                    Command::Prompt {
                        prompt: options.prompt.clone(),
                        turn_id: Some(Uuid::new_v4()),
                        message_id: Some(Uuid::new_v4()),
                    },
                );
                if let Err(error) = result {
                    eprintln!("waku-bench-daemon: autoplay prompt failed: {error:#}");
                }
                client.disconnect();
            })
            .context("could not start the autoplay thread")?;
        Ok(Self { thread })
    }
}

/// Parse `--autoplay` / `--autoplay=MS`.
pub fn parse_flag(argument: &str) -> anyhow::Result<Option<Duration>> {
    if argument == "--autoplay" {
        return Ok(Some(DEFAULT_DELAY));
    }
    let Some(value) = argument.strip_prefix("--autoplay=") else {
        return Ok(None);
    };
    let millis = value
        .trim()
        .trim_end_matches("ms")
        .parse::<u64>()
        .with_context(|| format!("--autoplay delay {value:?} is not a number of milliseconds"))?;
    Ok(Some(Duration::from_millis(millis)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flag_parses_with_and_without_a_delay() {
        assert_eq!(parse_flag("--autoplay").unwrap(), Some(DEFAULT_DELAY));
        assert_eq!(
            parse_flag("--autoplay=250").unwrap(),
            Some(Duration::from_millis(250))
        );
        assert_eq!(parse_flag("--autoplay=0ms").unwrap(), Some(Duration::ZERO));
        assert!(parse_flag("--autoplay=soon").is_err());
        assert_eq!(parse_flag("--rate").unwrap(), None);
    }
}
