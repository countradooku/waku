//! `waku-bench-daemon`: `waku-daemon` with deterministic fixtures and the
//! fake provider CLIs.
//!
//! ```text
//! waku-bench-daemon [serve] [--timeline NAME|PATH] [--rate FACTOR|max] [--loops N]
//!                   [--listen ADDR] [--socket PATH] [--provider codex|claude]
//!                   [--history-turns N] [--history-sessions N] [--autoplay[=DELAY_MS]]
//!                   [--exit-after N] [--state-dir DIR] [--project DIR] [--fakes DIR]
//! waku-bench-daemon generate [--out DIR] [--check]
//! waku-bench-daemon list
//! ```

use std::io::Write as _;
use std::path::PathBuf;
#[cfg(unix)]
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

use anyhow::{Context as _, anyhow, bail};
use waku_bench_daemon::server::{BenchOptions, BenchServer, RunReport};
use waku_bench_daemon::timeline::{BUILTIN_NAMES, Timeline};
use waku_bench_daemon::{autoplay, generate};
use waku_protocol::model::ProviderKind;

/// The real daemon's allocator (`crates/waku-daemon/src/main.rs`), so the
/// bench daemon's CPU and memory stay comparable to it.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const ADDRESS_ENV: &str = "WAKU_DAEMON_ADDRESS";
const TOKEN_ENV: &str = "WAKU_DAEMON_TOKEN";

const USAGE: &str = "\
usage:
  waku-bench-daemon [serve] [options]   serve the fixture threads; prompts replay timelines
  waku-bench-daemon generate [--out DIR] [--check]
                                        rewrite (or verify) the checked-in timelines
  waku-bench-daemon list                describe the built-in timelines

serve options:
  --timeline NAME|PATH  timeline a prompt replays by default (default: prose).
                        Built-ins: prose, burst, reasoning-fast, code-wall,
                        tool-storm, images. A path loads a .jsonl/.jsonl.gz file.
  --rate FACTOR|max     playback speed relative to the timeline (default: 1)
  --loops N             repeat the timeline body N times per prompt (default: 1)
  --listen ADDR         TCP address to listen on (default: 127.0.0.1:0 unless
                        --socket is given)
  --socket PATH         local socket to listen on
  --provider NAME       provider of the fixture threads: codex or claude (default: codex)
  --history-turns N     add a settled thread with N turns of history (default: 0)
  --history-sessions N  add N such threads instead of one (default: 1)
  --autoplay[=MS]       prompt the --timeline thread with its timeline MS ms
                        (default 1500) after a client subscribes to that
                        thread. The desktop must have it selected (its id is
                        printed as WAKU_BENCH_THREAD_ID).
  --exit-after N        stop after N replays and print the summary
  --state-dir DIR       throwaway data directory (default: a temp directory)
  --project DIR         project directory of the fixture
  --fakes DIR           where fake-codex and fake-claude are (default: beside
                        this executable)

The token comes from $WAKU_DAEMON_TOKEN, or is generated. Every built-in
timeline gets its own fixture thread; a prompt whose whole text is a
timeline's name replays that timeline, any other prompt the --timeline one.";

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

fn parse_count(arguments: &mut impl Iterator<Item = String>, flag: &str) -> anyhow::Result<usize> {
    value(arguments, flag)?
        .parse()
        .with_context(|| format!("{flag} must be an integer"))
}

fn serve_command(mut arguments: impl Iterator<Item = String>) -> anyhow::Result<()> {
    let mut options = BenchOptions::new(PathBuf::new(), String::new());
    let mut state_dir = None;
    let mut provider = "codex".to_owned();
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--timeline" => options.timeline = value(&mut arguments, "--timeline")?,
            "--rate" => options.rate = value(&mut arguments, "--rate")?.parse()?,
            "--loops" => {
                options.loops = value(&mut arguments, "--loops")?
                    .parse()
                    .context("--loops must be a positive integer")?;
                if options.loops == 0 {
                    bail!("--loops must be at least 1");
                }
            }
            "--listen" => options.listen = Some(value(&mut arguments, "--listen")?),
            "--socket" => options.socket = Some(PathBuf::from(value(&mut arguments, "--socket")?)),
            "--provider" => provider = value(&mut arguments, "--provider")?,
            "--history-turns" => {
                options.history_turns = parse_count(&mut arguments, "--history-turns")?;
            }
            "--history-sessions" => {
                options.history_sessions = parse_count(&mut arguments, "--history-sessions")?;
            }
            flag if flag == "--autoplay" || flag.starts_with("--autoplay=") => {
                options.autoplay = autoplay::parse_flag(flag)?;
            }
            "--exit-after" => {
                options.exit_after = Some(parse_count(&mut arguments, "--exit-after")?);
            }
            "--state-dir" => state_dir = Some(PathBuf::from(value(&mut arguments, "--state-dir")?)),
            "--project" => {
                options.project_dir = Some(PathBuf::from(value(&mut arguments, "--project")?));
            }
            "--fakes" => options.fakes_dir = Some(PathBuf::from(value(&mut arguments, "--fakes")?)),
            "--help" | "-h" => {
                println!("{USAGE}");
                return Ok(());
            }
            other => bail!("unknown argument {other:?}\n\n{USAGE}"),
        }
    }
    options.provider = ProviderKind::ALL
        .into_iter()
        .find(|candidate| candidate.id() == provider)
        .filter(|candidate| waku_bench_daemon::server::BENCH_PROVIDERS.contains(candidate))
        .with_context(|| format!("--provider must be codex or claude, not {provider:?}"))?;
    options.token = std::env::var(TOKEN_ENV)
        .ok()
        .filter(|token| !token.is_empty())
        .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());
    // As `waku-daemon` does: the bearer capability belongs to this process
    // only, never to the provider CLIs it starts. Their configuration is the
    // `waku-bench.json` the server writes, not the environment.
    unsafe {
        std::env::remove_var(TOKEN_ENV);
        for name in [
            "WAKU_BENCH_TIMELINE",
            "WAKU_BENCH_RATE",
            "WAKU_BENCH_LOOPS",
            "WAKU_BENCH_LOG",
            "WAKU_BENCH_STATS",
        ] {
            std::env::remove_var(name);
        }
    }
    options.state_dir = state_dir.unwrap_or_else(|| {
        std::env::temp_dir().join(format!("waku-bench-daemon-{}", std::process::id()))
    });
    let (rate, loops) = (options.rate, options.loops);

    let server = BenchServer::start(options)?;
    install_signal_handlers(server.shutdown_flag());

    // Machine-readable lines first, for runners that read stdout.
    println!("{ADDRESS_ENV}={}", server.client_address());
    println!("{TOKEN_ENV}={}", server.token);
    // Ids a runner needs to preselect a thread in the desktop's state.json.
    println!("WAKU_BENCH_PROJECT_ID={}", server.project_id);
    println!("WAKU_BENCH_THREAD_ID={}", server.default_thread);
    if let Some(thread_id) = server.history_threads.first() {
        println!("WAKU_BENCH_HISTORY_THREAD_ID={thread_id}");
    }
    std::io::stdout().flush()?;
    eprintln!(
        "waku-bench-daemon listening on {} · default timeline {} · rate {rate} · loops {loops}",
        server.client_address(),
        server.timeline,
    );
    eprintln!("  state: {}", server.state_dir.display());
    eprintln!(
        "  threads: {}{}",
        server
            .timeline_threads
            .iter()
            .map(|(timeline, _)| format!("Bench · {timeline}"))
            .collect::<Vec<_>>()
            .join(", "),
        match server.history_threads.len() {
            0 => String::new(),
            count => format!(" and {count} history thread(s)"),
        }
    );
    eprintln!(
        "  run the desktop with: {ADDRESS_ENV}={} {TOKEN_ENV}={} <waku>",
        server.client_address(),
        server.token
    );

    let reports = server.wait()?;
    print_summary(&reports);
    Ok(())
}

fn print_summary(reports: &[RunReport]) {
    let replays = reports
        .iter()
        .filter_map(|report| report.replay.as_ref().map(|replay| (report, replay)))
        .collect::<Vec<_>>();
    println!();
    println!("waku-bench-daemon summary: {} replay(s)", replays.len());
    if replays.is_empty() {
        return;
    }
    println!(
        "{:<16} {:>8} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}  status",
        "timeline", "events", "payload", "scheduled", "elapsed", "events/s", "max lag", "run"
    );
    for (report, replay) in &replays {
        println!(
            "{:<16} {:>8} {:>8} KB {:>9.2}s {:>9.2}s {:>10.0} {:>8.1}ms {:>8}ms  {}",
            replay.timeline,
            replay.events,
            replay.payload_bytes / 1024,
            replay.scheduled.as_secs_f64(),
            replay.elapsed.as_secs_f64(),
            replay.events_per_second(),
            replay.max_lag.as_secs_f64() * 1_000.0,
            report.run_ms.unwrap_or_default(),
            report.status.as_str(),
        );
    }
    let events = replays.iter().map(|(_, replay)| replay.events).sum::<u64>();
    let elapsed = replays
        .iter()
        .map(|(_, replay)| replay.elapsed)
        .sum::<Duration>();
    println!(
        "total: {events} events over {:.2}s of replay",
        elapsed.as_secs_f64()
    );
}

/// The write end of the signal pipe; the handler only writes one byte.
#[cfg(unix)]
static SIGNAL_PIPE: AtomicI32 = AtomicI32::new(-1);

#[cfg(unix)]
extern "C" fn on_signal(_: libc::c_int) {
    let fd = SIGNAL_PIPE.load(Ordering::Acquire);
    if fd >= 0 {
        // `write` is async-signal-safe; a full pipe already holds a wakeup.
        unsafe { libc::write(fd, [1_u8].as_ptr().cast(), 1) };
    }
}

/// Ctrl-C and SIGTERM stop the server cleanly so the summary still prints.
/// The watcher thread blocks on a pipe the handler writes to, so an idle
/// bench daemon has no timer wakeups of its own.
fn install_signal_handlers(shutdown: waku_core::ShutdownSignal) {
    #[cfg(unix)]
    {
        let mut fds = [0 as libc::c_int; 2];
        if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
            eprintln!("waku-bench-daemon: no signal pipe; Ctrl-C will not print the summary");
            return;
        }
        let [read_end, write_end] = fds;
        SIGNAL_PIPE.store(write_end, Ordering::Release);
        unsafe {
            let handler = on_signal as extern "C" fn(libc::c_int) as libc::sighandler_t;
            libc::signal(libc::SIGINT, handler);
            libc::signal(libc::SIGTERM, handler);
        }
        let _ = std::thread::Builder::new()
            .name("waku-bench-daemon-signals".into())
            .spawn(move || {
                let mut byte = 0_u8;
                loop {
                    let read = unsafe { libc::read(read_end, (&raw mut byte).cast(), 1) };
                    if read > 0 {
                        shutdown.trigger();
                        return;
                    }
                    if read == 0
                        || std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted
                    {
                        return;
                    }
                }
            });
    }
    #[cfg(not(unix))]
    let _ = shutdown;
}
