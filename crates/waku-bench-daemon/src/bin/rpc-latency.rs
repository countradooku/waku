//! `rpc-latency`: client-side round-trip latency of protocol v8 calls.
//!
//! Connects with `waku_client::v8::V8Client`, exactly as the desktop does,
//! creates one project over a generated source tree with
//! `orchestration.dispatch` (`project.create`, and `thread.create` unless
//! `--thread` names one), then times sequential calls (one in flight at a
//! time):
//!
//! | Name | Call |
//! | --- | --- |
//! | `settings_get` | `settings.get`: the smallest round trip, the transport floor |
//! | `threads_snapshot` | `threads.snapshot` (compact, as the desktop subscribes) of `--thread`, else of the created empty thread |
//! | `files_search_entries` | `files.searchEntries` over the project (the composer's `@` file index), every entry |
//! | `threads_search` | `threads.search` (sidebar search) |
//!
//! ```text
//! rpc-latency [--address ADDR] [--token TOKEN] [--thread ID] [--count N] [--warmup N]
//!             [--files N] [--project DIR] [--label TEXT] [--out FILE]
//!             [--hold SECONDS] [--terminal] [--supervise DAEMON]
//! ```
//!
//! Against `waku-bench-daemon`, pass its `WAKU_BENCH_THREAD_ID` as
//! `--thread`: that thread holds one settled two-message turn.
//!
//! `--hold` keeps the connection open and idle after the timed calls, so an
//! outside sampler (`scripts/bench/harness.py measure --pid`) can count the
//! idle wakeups of a connected daemon and client pair without a GUI.
//! `--terminal` keeps a daemon terminal open (its shell at the prompt,
//! subscribed as the desktop's terminal pane is) for the hold.
//! `--supervise DAEMON` launches that `waku-daemon` binary through the
//! desktop's `DaemonSupervisor`, as a release app does, instead of
//! connecting to `--address`, so the hold also covers the supervisor
//! (#178). Give it a private `HOME` and `WAKU_DAEMON_SOCKET`.
//!
//! The address and token default to `$WAKU_DAEMON_ADDRESS` and
//! `$WAKU_DAEMON_TOKEN`. Point it only at a disposable daemon: it adds a
//! project (and a thread) to the daemon's database.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context as _, anyhow, bail};
use serde_json::{Value, json};
use uuid::Uuid;
use waku_client::DaemonSupervisor;
use waku_client::v8::files::FilesSearchEntriesParams;
use waku_client::v8::orchestration::{
    DispatchCommand, DispatchParams, ThreadSearchParams, ThreadSnapshotParams,
};
use waku_client::v8::terminal::{TerminalItem, TerminalOpenParams, TerminalParams, TerminalSpec};
use waku_client::v8::{
    MethodDef, NoParams, RootRef, StreamEvent, StreamItem, SubscribeOptions, Subscription,
    V8Client, method, stream,
};
use waku_protocol::orchestration::{
    Command, CommandId, InteractionMode, ModelSelection, ProjectId, RuntimeMode, ThreadId,
    WorkspaceStrategy,
};

const USAGE: &str = "\
usage: rpc-latency [--address ADDR] [--token TOKEN] [--thread ID] [--count N] [--warmup N]
                   [--files N] [--project DIR] [--label TEXT] [--out FILE]
                   [--hold SECONDS] [--terminal] [--supervise DAEMON]

  --address ADDR   daemon address: host:port or unix:PATH (default: $WAKU_DAEMON_ADDRESS)
  --token TOKEN    daemon token (default: $WAKU_DAEMON_TOKEN)
  --thread ID      thread to snapshot (waku-bench-daemon's WAKU_BENCH_THREAD_ID);
                   default: a thread created here, which has no turns
  --count N        timed calls per kind (default: 1000)
  --warmup N       untimed calls per kind first (default: 50)
  --files N        files to create in the seeded project (default: 500)
  --project DIR    project directory to create and seed (default: a temp dir)
  --label TEXT     recorded in the JSON (e.g. waku-bench-daemon)
  --out FILE       also write the JSON report here
  --hold SECONDS   stay connected and idle this long before disconnecting
  --terminal       keep a daemon terminal open during the hold
  --supervise BIN  launch this waku-daemon through DaemonSupervisor instead
                   of connecting to --address (set HOME and WAKU_DAEMON_SOCKET
                   to private paths)

Creates a project (and a thread) through orchestration.dispatch: use a
disposable daemon.";

struct Options {
    address: String,
    token: String,
    thread: Option<Uuid>,
    count: usize,
    warmup: usize,
    files: usize,
    project: Option<PathBuf>,
    label: Option<String>,
    out: Option<PathBuf>,
    hold: Option<Duration>,
    terminal: bool,
    supervise: Option<PathBuf>,
}

fn parse() -> anyhow::Result<Options> {
    let mut options = Options {
        address: std::env::var("WAKU_DAEMON_ADDRESS").unwrap_or_default(),
        token: std::env::var("WAKU_DAEMON_TOKEN").unwrap_or_default(),
        thread: None,
        count: 1_000,
        warmup: 50,
        files: 500,
        project: None,
        label: None,
        out: None,
        hold: None,
        terminal: false,
        supervise: None,
    };
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        let mut value = || {
            arguments
                .next()
                .ok_or_else(|| anyhow!("{argument} requires a value"))
        };
        match argument.as_str() {
            "--address" => options.address = value()?,
            "--token" => options.token = value()?,
            "--thread" => options.thread = Some(value()?.parse().context("--thread")?),
            "--count" => options.count = value()?.parse().context("--count")?,
            "--warmup" => options.warmup = value()?.parse().context("--warmup")?,
            "--files" => options.files = value()?.parse().context("--files")?,
            "--project" => options.project = Some(PathBuf::from(value()?)),
            "--label" => options.label = Some(value()?),
            "--out" => options.out = Some(PathBuf::from(value()?)),
            "--hold" => {
                options.hold = Some(Duration::from_secs_f64(value()?.parse().context("--hold")?));
            }
            "--terminal" => options.terminal = true,
            "--supervise" => options.supervise = Some(PathBuf::from(value()?)),
            "--help" | "-h" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            other => bail!("unknown argument {other:?}\n\n{USAGE}"),
        }
    }
    if options.supervise.is_none()
        && (options.address.trim().is_empty() || options.token.is_empty())
    {
        bail!("a daemon address and token are required\n\n{USAGE}");
    }
    if options.count == 0 {
        bail!("--count must be at least 1");
    }
    Ok(options)
}

fn dispatch(client: &V8Client, command: Command) -> anyhow::Result<()> {
    client
        .call::<method::OrchestrationDispatch>(DispatchParams {
            command_id: CommandId::new(format!("rpc-latency:{}", Uuid::new_v4().simple()))
                .map_err(|error| anyhow!("{error:?}"))?,
            command: DispatchCommand(command),
        })
        .map(drop)
        .map_err(|error| anyhow!("orchestration.dispatch failed: {error:?}"))
}

/// Open a daemon terminal in `root` and subscribe to it as the desktop's
/// terminal pane does, then wait until its shell has settled at the prompt:
/// no output for half a second. Its output is drained on a thread for the
/// rest of the run.
fn open_terminal(client: &V8Client, root: RootRef) -> anyhow::Result<(Uuid, Subscription)> {
    let opened = client
        .call::<method::TerminalOpen>(TerminalOpenParams {
            root,
            cols: 80,
            rows: 24,
            title: None,
            op_id: Uuid::new_v4(),
        })
        .map_err(|error| anyhow!("terminal.open failed: {error:?}"))?;
    let subscription = client.subscribe::<stream::Terminal>(
        TerminalSpec {
            terminal_id: opened.terminal_id,
            interactive: true,
        },
        SubscribeOptions::default(),
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut output = 0;
    while Instant::now() < deadline {
        match subscription
            .events()
            .recv_timeout(Duration::from_millis(500))
        {
            Ok(StreamEvent::Data { bytes, .. }) => {
                subscription.consumed(bytes.len() as u32);
                output += bytes.len();
            }
            Ok(StreamEvent::Item(StreamItem::Terminal(TerminalItem::Error { message }))) => {
                bail!("the daemon terminal failed: {message}");
            }
            Ok(StreamEvent::End(end)) => bail!("the terminal stream ended: {end:?}"),
            Ok(_) => {}
            Err(_) if output > 0 => break,
            Err(_) => {}
        }
    }
    if output == 0 {
        bail!("the daemon terminal produced no output");
    }
    Ok((opened.terminal_id, subscription))
}

/// A small source tree: `files` files spread over nested directories.
fn create_project(dir: &Path, files: usize) -> anyhow::Result<()> {
    std::fs::create_dir_all(dir)?;
    std::fs::write(dir.join("README.md"), "# rpc-latency project\n")?;
    for index in 0..files {
        let sub = dir
            .join("src")
            .join(format!("module_{}", index % 20))
            .join(format!("part_{}", index % 7));
        std::fs::create_dir_all(&sub)?;
        std::fs::write(
            sub.join(format!("file_{index}.rs")),
            format!("pub fn f{index}() -> usize {{ {index} }}\n"),
        )?;
    }
    Ok(())
}

#[derive(Default)]
struct Samples(Vec<Duration>);

impl Samples {
    fn summary(&mut self) -> Value {
        self.0.sort_unstable();
        let micros = |duration: Duration| duration.as_secs_f64() * 1e6;
        let percentile = |p: f64| {
            // Nearest rank.
            let rank = ((p / 100.0) * self.0.len() as f64).ceil() as usize;
            micros(self.0[rank.clamp(1, self.0.len()) - 1])
        };
        let total = self.0.iter().sum::<Duration>();
        json!({
            "count": self.0.len(),
            "p50_us": percentile(50.0),
            "p95_us": percentile(95.0),
            "p99_us": percentile(99.0),
            "mean_us": micros(total) / self.0.len() as f64,
            "min_us": micros(self.0[0]),
            "max_us": micros(self.0[self.0.len() - 1]),
        })
    }
}

fn time_calls<M: MethodDef>(
    client: &V8Client,
    warmup: usize,
    count: usize,
    params: impl Fn() -> M::Params,
    check: impl Fn(&M::Result) -> anyhow::Result<()>,
) -> anyhow::Result<Samples> {
    let mut samples = Samples(Vec::with_capacity(count));
    for index in 0..warmup + count {
        let started = Instant::now();
        let result = client
            .call::<M>(params())
            .map_err(|error| anyhow!("{} failed: {error:?}", M::NAME))?;
        let elapsed = started.elapsed();
        check(&result)?;
        if index >= warmup {
            samples.0.push(elapsed);
        }
    }
    Ok(samples)
}

fn main() -> anyhow::Result<()> {
    let options = parse()?;
    let temporary = options.project.is_none();
    let project_dir = options.project.clone().unwrap_or_else(|| {
        std::env::temp_dir().join(format!("waku-rpc-latency-{}", std::process::id()))
    });
    create_project(&project_dir, options.files)?;
    let project_dir = std::path::absolute(&project_dir)?;

    // Dropped at the end of `main`, which stops a supervised daemon.
    let supervisor = options
        .supervise
        .as_deref()
        .map(|daemon| DaemonSupervisor::spawn(daemon, false))
        .transpose()?;
    let (address, token) = match &supervisor {
        Some(supervisor) => supervisor
            .endpoint()
            .context("the supervised daemon has no endpoint")?,
        None => (options.address.trim().to_owned(), options.token.clone()),
    };
    let client = V8Client::connect(&address, token)?;

    // A project over the generated tree, and a thread in it unless one was
    // named, created the way the desktop creates them.
    let project_id = Uuid::new_v4();
    dispatch(
        &client,
        Command::ProjectCreate {
            project_id: ProjectId(project_id),
            name: "rpc-latency".into(),
            path: project_dir.clone(),
            order_key: None,
        },
    )?;
    let thread_id = match options.thread {
        Some(thread_id) => thread_id,
        None => {
            let thread_id = Uuid::new_v4();
            dispatch(
                &client,
                Command::ThreadCreate {
                    thread_id: ThreadId(thread_id),
                    project_id: ProjectId(project_id),
                    title: Some("rpc-latency".into()),
                    model_selection: ModelSelection::new("codex"),
                    runtime_mode: RuntimeMode::FullAccess,
                    interaction_mode: InteractionMode::Default,
                    workspace: WorkspaceStrategy::Root { branch: None },
                    imported_native_thread: None,
                },
            )?;
            thread_id
        }
    };
    let snapshot_items = client
        .call::<method::ThreadsSnapshot>(ThreadSnapshotParams {
            thread_id: ThreadId(thread_id),
            compact_turn_items: true,
        })
        .map_err(|error| anyhow!("threads.snapshot of {thread_id} failed: {error:?}"))?
        .projection
        .turn_items
        .len();

    let mut results = serde_json::Map::new();
    let mut run = |name: &str, samples: anyhow::Result<Samples>| -> anyhow::Result<()> {
        let summary = samples.with_context(|| format!("{name} failed"))?.summary();
        println!(
            "{name:<24} p50 {:>9.1} us  p95 {:>9.1} us  p99 {:>9.1} us  mean {:>9.1} us  max {:>9.1} us",
            summary["p50_us"].as_f64().unwrap_or_default(),
            summary["p95_us"].as_f64().unwrap_or_default(),
            summary["p99_us"].as_f64().unwrap_or_default(),
            summary["mean_us"].as_f64().unwrap_or_default(),
            summary["max_us"].as_f64().unwrap_or_default(),
        );
        results.insert(name.to_owned(), summary);
        Ok(())
    };
    let (warmup, count) = (options.warmup, options.count);
    run(
        "settings_get",
        time_calls::<method::SettingsGet>(&client, warmup, count, || NoParams {}, |_| Ok(())),
    )?;
    run(
        "threads_snapshot",
        time_calls::<method::ThreadsSnapshot>(
            &client,
            warmup,
            count,
            || ThreadSnapshotParams {
                thread_id: ThreadId(thread_id),
                compact_turn_items: true,
            },
            |snapshot| {
                if snapshot.projection.turn_items.len() == snapshot_items {
                    Ok(())
                } else {
                    bail!("the snapshot changed while it was timed")
                }
            },
        ),
    )?;
    let files = options.files;
    run(
        "files_search_entries",
        time_calls::<method::FilesSearchEntries>(
            &client,
            warmup,
            count,
            || FilesSearchEntriesParams {
                root: RootRef::Project { project_id },
                query: String::new(),
                limit: Some(10_000),
            },
            |entries| {
                if entries.len() >= files {
                    Ok(())
                } else {
                    bail!(
                        "files.searchEntries found {} of {files} files",
                        entries.len()
                    )
                }
            },
        ),
    )?;
    run(
        "threads_search",
        time_calls::<method::ThreadsSearch>(
            &client,
            warmup,
            count,
            || ThreadSearchParams {
                query: "timeline".into(),
                project_id: None,
                include_archived: false,
                limit: Some(50),
            },
            |_| Ok(()),
        ),
    )?;
    if let Some(hold) = options.hold {
        let terminal = options
            .terminal
            .then(|| open_terminal(&client, RootRef::Project { project_id }))
            .transpose()?;
        let drain = terminal.map(|(terminal_id, subscription)| {
            let drain = std::thread::spawn(move || {
                while let Ok(event) = subscription.events().recv() {
                    match event {
                        StreamEvent::Data { bytes, .. } => {
                            subscription.consumed(bytes.len() as u32);
                        }
                        StreamEvent::End(_) => break,
                        _ => {}
                    }
                }
            });
            (terminal_id, drain)
        });
        eprintln!(
            "rpc-latency: holding the idle connection{} for {:.0}s (pid {})",
            if drain.is_some() {
                " with a terminal open"
            } else {
                ""
            },
            hold.as_secs_f64(),
            std::process::id()
        );
        std::thread::sleep(hold);
        if let Some((terminal_id, drain)) = drain {
            client
                .call::<method::TerminalClose>(TerminalParams { terminal_id })
                .map_err(|error| anyhow!("terminal.close failed: {error:?}"))?;
            client.close();
            let _ = drain.join();
        }
    }
    client.close();

    let report = json!({
        "schema": "waku.rpc-latency/2",
        "label": options.label,
        "address": address,
        "thread": thread_id,
        "thread_snapshot_items": snapshot_items,
        "count": options.count,
        "warmup": options.warmup,
        "project_files": options.files,
        "requests": results,
    });
    if let Some(out) = &options.out {
        std::fs::write(out, serde_json::to_vec_pretty(&report)?)
            .with_context(|| format!("could not write {}", out.display()))?;
    }
    drop(supervisor);
    if temporary {
        let _ = std::fs::remove_dir_all(&project_dir);
    }
    Ok(())
}
