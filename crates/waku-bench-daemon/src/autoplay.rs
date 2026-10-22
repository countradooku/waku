//! `--autoplay`: replay a timeline into the desktop without touching its UI.
//!
//! 1. The desktop launches with the timeline's thread selected (its own
//!    `state.json`, `selected_session`) and subscribes to that thread's
//!    `thread` stream, as it does for the selected thread at every launch.
//! 2. That subscription is the signal that the desktop can render the
//!    stream: the stream registers with the daemon's delta bus before it
//!    reads its snapshot, and the bench daemon watches the bus for it.
//! 3. After `delay`, the bench daemon dispatches `message.dispatch` with the
//!    timeline's name into the thread, through the daemon's own
//!    orchestrator, as a second client's send would. The orchestrator starts
//!    a run, the provider session manager starts the fake provider CLI,
//!    which replays the timeline, and the desktop renders the run through
//!    its ordinary thread stream, projection store and rows. Only the
//!    composer itself is not exercised.

use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context as _, anyhow};
use uuid::Uuid;
use waku_protocol::orchestration::{
    Actor, Command, CommandEnvelope, CommandId, CreationSource, DispatchMode, MessageId, ThreadId,
};

/// Delay between the desktop subscribing and the prompt when `--autoplay`
/// has no value: long enough for the snapshot to land on the UI thread and
/// the first frames of the selected transcript to settle.
pub const DEFAULT_DELAY: Duration = Duration::from_millis(1_500);
/// How often the subscription is looked for, until it arrives.
const SUBSCRIPTION_POLL: Duration = Duration::from_millis(50);

#[derive(Clone, Debug)]
pub struct AutoplayOptions {
    pub thread_id: Uuid,
    /// The message text; a timeline name selects that timeline.
    pub prompt: String,
    pub delay: Duration,
}

/// The message `--autoplay` sends, as the desktop's composer would send it.
pub fn message_dispatch(thread_id: Uuid, text: &str) -> anyhow::Result<CommandEnvelope> {
    let message_id = Uuid::new_v4();
    Ok(CommandEnvelope {
        command_id: CommandId::new(format!("bench:autoplay:{}", message_id.simple()))
            .map_err(|error| anyhow!("{error:?}"))?,
        issued_by: Actor::User,
        source: CreationSource::Desktop,
        command: Command::MessageDispatch {
            thread_id: ThreadId(thread_id),
            message_id: MessageId(message_id),
            text: text.to_owned(),
            display_text: None,
            attachments: Vec::new(),
            context: Vec::new(),
            model_selection: None,
            mode: DispatchMode::QueueAfterActive,
            title_seed: None,
            source_plan: None,
        },
    })
}

/// Wait in the background for a `thread` subscription to the autoplay
/// thread, then prompt it.
pub(crate) fn start(
    orchestration: Arc<waku_core::Orchestration>,
    shutdown: waku_core::ShutdownSignal,
    options: AutoplayOptions,
) -> anyhow::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("waku-bench-autoplay".into())
        .spawn(move || {
            let thread_id = ThreadId(options.thread_id);
            let waited = Instant::now();
            while !orchestration.live.deltas.has_subscribers(thread_id) {
                if shutdown.wait_timeout(SUBSCRIPTION_POLL) {
                    return;
                }
            }
            eprintln!(
                "waku-bench-daemon: autoplay: a client subscribed to {} after {:.1}s; prompting {:?} in {} ms",
                options.thread_id,
                waited.elapsed().as_secs_f64(),
                options.prompt,
                options.delay.as_millis(),
            );
            if !options.delay.is_zero() && shutdown.wait_timeout(options.delay) {
                return;
            }
            // A machine-readable marker for runners timing the replay.
            eprintln!("waku-bench-daemon: autoplay prompt sent");
            let result = message_dispatch(options.thread_id, &options.prompt).and_then(|envelope| {
                orchestration
                    .orchestrator
                    .dispatch(envelope)
                    .map_err(|error| anyhow!("{error}"))
            });
            if let Err(error) = result {
                eprintln!("waku-bench-daemon: autoplay prompt failed: {error:#}");
            }
        })
        .context("could not start the autoplay thread")
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
