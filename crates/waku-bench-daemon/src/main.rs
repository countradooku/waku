//! `waku-bench-daemon`: a deterministic stand-in for `waku-daemon`.
//!
//! ```text
//! waku-bench-daemon [serve] [--timeline NAME|PATH] [--rate FACTOR|max] [--loops N]
//!                   [--listen ADDR] [--provider codex|claude] [--history-turns N]
//!                   [--history-sessions N] [--autoplay[=DELAY_MS]]
//!                   [--exit-after N] [--state-dir DIR] [--project DIR]
//! waku-bench-daemon generate [--out DIR] [--check]
//! waku-bench-daemon list
//! ```

use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context as _, anyhow, bail};
use waku_bench_daemon::replay::{Rate, ReplayStats};
use waku_bench_daemon::server::{BenchOptions, BenchServer};
use waku_bench_daemon::timeline::{BUILTIN_NAMES, Timeline};
use waku_bench_daemon::{autoplay, generate};
use waku_protocol::{DAEMON_ADDRESS_ENV, DAEMON_TOKEN_ENV, decode_enum};

const USAGE: &str = "\
usage:
  waku-bench-daemon [serve] [options]   replay timelines to a connected Waku desktop
  waku-bench-daemon generate [--out DIR] [--check]
                                        rewrite (or verify) the checked-in timelines
  waku-bench-daemon list                describe the built-in timelines

serve options:
  --timeline NAME|PATH  timeline a prompt replays by default (default: prose).
                        Built-ins: prose, burst, reasoning-fast, code-wall,
                        tool-storm, images. A path loads a .jsonl/.jsonl.gz file.
  --rate FACTOR|max     playback speed relative to the timeline (default: 1)
  --loops N             repeat the timeline body N times per prompt (default: 1)
  --listen ADDR         address to listen on (default: 127.0.0.1:0)
  --provider NAME       provider of the fixture sessions: codex or claude (default: codex)
  --history-turns N     add a settled session with N turns of history (default: 0)
  --history-sessions N  add N such sessions instead of one (default: 1)
  --autoplay[=MS]       start the --timeline session's runtime now and replay the
                        timeline MS ms (default 1500) after a desktop attaches
                        to it, as a second client's prompt would. The desktop
                        must have that session selected (its id is printed
                        as WAKU_BENCH_SESSION_ID).
  --exit-after N        stop after N replays and print the summary
  --state-dir DIR       throwaway database location (default: a temp directory)
  --project DIR         project directory listed in the fixture

The token comes from $WAKU_DAEMON_TOKEN, or is generated. Every built-in
timeline gets its own fixture session; a prompt replays that session's
timeline, or the timeline whose name is the prompt's whole text.";

fn main() -> anyhow::Result<()> {
    let mut arguments = std::env::args().skip(1).peekable();
    match arguments.peek().map(String::as_str) {
        Some("generate") => {
            arguments.next();
            generate_command(arguments)
        }
        Some("list") => {
            arguments.next();
            list_command()
        }
        Some("serve") => {
            arguments.next();
            serve_command(arguments)
        }
        Some("--help" | "-h" | "help") => {
            println!("{USAGE}");
            Ok(())
        }
        _ => serve_command(arguments),
    }
}

fn value(arguments: &mut impl Iterator<Item = String>, flag: &str) -> anyhow::Result<String> {
    arguments
        .next()
        .ok_or_else(|| anyhow!("{flag} requires a value"))
}

fn generate_command(mut arguments: impl Iterator<Item = String>) -> anyhow::Result<()> {
    let mut out = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("timelines");
    let mut check = false;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--out" => out = PathBuf::from(value(&mut arguments, "--out")?),
            "--check" => check = true,
            other => bail!("unknown argument {other:?}\n\n{USAGE}"),
        }
    }
    if check {
        let mut stale = Vec::new();
        for timeline in generate::generate_all()? {
            let path = out.join(format!("{}.jsonl.gz", timeline.name));
            match Timeline::load(&path) {
                Ok(committed) if committed == timeline => {}
                _ => stale.push(path.display().to_string()),
            }
        }
        if !stale.is_empty() {
            bail!(
                "timelines differ from the generator; run `cargo run -p waku-bench-daemon -- generate`:\n  {}",
                stale.join("\n  ")
            );
        }
        println!("all {} timelines match the generator", BUILTIN_NAMES.len());
        return Ok(());
    }
    for (timeline, bytes) in generate::write_all(&out)? {
        let stats = timeline.stats();
        println!(
            "{:<15} {:>6} events  {:>7.1} s  {:>8} KB json  {:>7} KB gz",
            timeline.name,
            stats.events,
            stats.duration.as_secs_f64(),
            timeline.to_jsonl().len() / 1024,
            bytes / 1024,
        );
    }
    println!("wrote {}", out.display());
    Ok(())
}

fn list_command() -> anyhow::Result<()> {
    for name in BUILTIN_NAMES {
        let timeline = Timeline::builtin(name)?;
        let stats = timeline.stats();
        println!(
            "{name:<15} {:>6.1} s  {:>6} events  {:>6} text / {:>6} reasoning deltas  {:>4} activities  {:>6} KB payload\n                {}",
            stats.duration.as_secs_f64(),
            stats.events,
            stats.text_deltas,
            stats.reasoning_deltas,
            stats.activities,
            stats.payload_bytes / 1024,
            timeline.description,
        );
    }
    Ok(())
}

fn serve_command(mut arguments: impl Iterator<Item = String>) -> anyhow::Result<()> {
    let mut default_timeline = "prose".to_owned();
    let mut rate = Rate::default();
    let mut loops = 1;
    let mut listen = "127.0.0.1:0".to_owned();
    let mut provider = "codex".to_owned();
    let mut history_turns = 0;
    let mut history_sessions = 1;
    let mut autoplay = None;
    let mut exit_after = None;
    let mut state_dir = None;
    let mut project_dir = None;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--timeline" => default_timeline = value(&mut arguments, "--timeline")?,
            "--rate" => rate = value(&mut arguments, "--rate")?.parse()?,
            "--loops" => {
                loops = value(&mut arguments, "--loops")?
                    .parse()
                    .context("--loops must be a positive integer")?;
                if loops == 0 {
                    bail!("--loops must be at least 1");
                }
            }
            "--listen" => listen = value(&mut arguments, "--listen")?,
            "--provider" => provider = value(&mut arguments, "--provider")?,
            "--history-turns" => {
                history_turns = value(&mut arguments, "--history-turns")?
                    .parse()
                    .context("--history-turns must be an integer")?;
            }
            "--history-sessions" => {
                history_sessions = value(&mut arguments, "--history-sessions")?
                    .parse()
                    .context("--history-sessions must be an integer")?;
            }
            flag if flag == "--autoplay" || flag.starts_with("--autoplay=") => {
                autoplay = autoplay::parse_flag(flag)?;
            }
            "--exit-after" => {
                exit_after = Some(
                    value(&mut arguments, "--exit-after")?
                        .parse()
                        .context("--exit-after must be an integer")?,
                );
            }
            "--state-dir" => state_dir = Some(PathBuf::from(value(&mut arguments, "--state-dir")?)),
            "--project" => project_dir = Some(PathBuf::from(value(&mut arguments, "--project")?)),
            "--help" | "-h" => {
                println!("{USAGE}");
                return Ok(());
            }
            other => bail!("unknown argument {other:?}\n\n{USAGE}"),
        }
    }

    let mut timelines = Timeline::builtins()?;
    if !BUILTIN_NAMES.contains(&default_timeline.as_str()) {
        let custom = Timeline::resolve(&default_timeline)?;
        default_timeline = custom.name.clone();
        timelines.retain(|timeline| timeline.name != custom.name);
        timelines.insert(0, Arc::new(custom));
    }
    let provider =
        decode_enum(&provider).with_context(|| format!("unknown provider {provider:?}"))?;
    let token = std::env::var(DAEMON_TOKEN_ENV)
        .ok()
        .filter(|token| !token.is_empty())
        .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());
    let state_dir = state_dir.unwrap_or_else(|| {
        std::env::temp_dir().join(format!("waku-bench-daemon-{}", std::process::id()))
    });

    let server = BenchServer::start(BenchOptions {
        listen,
        token,
        timelines: timelines.clone(),
        default_timeline: default_timeline.clone(),
        rate,
        loops,
        provider,
        history_turns,
        history_sessions,
        exit_after,
        autoplay,
        state_dir,
        project_dir,
        allow_shutdown: true,
    })?;
    install_signal_handlers(server.shutdown_flag());

    // Machine-readable lines first, for runners that read stdout.
    println!("{DAEMON_ADDRESS_ENV}={}", server.address);
    println!("{DAEMON_TOKEN_ENV}={}", server.token);
    // Ids a runner needs to preselect sessions in the desktop's state.json.
    println!("WAKU_BENCH_PROJECT_ID={}", server.project_id);
    println!("WAKU_BENCH_SESSION_ID={}", server.default_session);
    if let Some(session_id) = server.history_session {
        println!("WAKU_BENCH_HISTORY_SESSION_ID={session_id}");
    }
    std::io::stdout().flush()?;
    eprintln!(
        "waku-bench-daemon listening on {} · default timeline {default_timeline} · rate {rate} · loops {loops}",
        server.address
    );
    eprintln!("  state: {}", server.state_dir.display());
    eprintln!(
        "  sessions: {}",
        timelines
            .iter()
            .map(|timeline| format!("Bench · {}", timeline.name))
            .collect::<Vec<_>>()
            .join(", ")
    );
    eprintln!(
        "  run the desktop with: {DAEMON_ADDRESS_ENV}={} {DAEMON_TOKEN_ENV}={} <waku>",
        server.address, server.token
    );

    let stats = server.wait()?;
    print_summary(&stats);
    Ok(())
}

fn print_summary(stats: &[ReplayStats]) {
    println!();
    println!("waku-bench-daemon summary: {} replay(s)", stats.len());
    if stats.is_empty() {
        return;
    }
    println!(
        "{:<16} {:>8} {:>10} {:>10} {:>10} {:>10} {:>10}  status",
        "timeline", "events", "payload", "scheduled", "elapsed", "events/s", "max lag"
    );
    for replay in stats {
        println!(
            "{:<16} {:>8} {:>8} KB {:>9.2}s {:>9.2}s {:>10.0} {:>8.1}ms  {}",
            replay.timeline,
            replay.events,
            replay.payload_bytes / 1024,
            replay.scheduled.as_secs_f64(),
            replay.elapsed.as_secs_f64(),
            replay.events_per_second(),
            replay.max_lag.as_secs_f64() * 1_000.0,
            if replay.canceled {
                "canceled"
            } else {
                "finished"
            },
        );
    }
    let events = stats.iter().map(|replay| replay.events).sum::<u64>();
    let elapsed = stats.iter().map(|replay| replay.elapsed).sum::<Duration>();
    println!(
        "total: {events} events over {:.2}s of replay",
        elapsed.as_secs_f64()
    );
}

static SIGNALED: AtomicBool = AtomicBool::new(false);

#[cfg(unix)]
extern "C" fn on_signal(_: libc::c_int) {
    SIGNALED.store(true, Ordering::Release);
}

/// Ctrl-C and SIGTERM stop the server cleanly so the summary still prints.
fn install_signal_handlers(shutdown: Arc<AtomicBool>) {
    #[cfg(unix)]
    unsafe {
        let handler = on_signal as extern "C" fn(libc::c_int) as libc::sighandler_t;
        libc::signal(libc::SIGINT, handler);
        libc::signal(libc::SIGTERM, handler);
    }
    let _ = std::thread::Builder::new()
        .name("waku-bench-daemon-signals".into())
        .spawn(move || {
            while !shutdown.load(Ordering::Acquire) {
                if SIGNALED.load(Ordering::Acquire) {
                    shutdown.store(true, Ordering::Release);
                    break;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        });
}
