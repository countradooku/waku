//! `rpc-latency`: client-side round-trip latency of daemon requests.
//!
//! Connects with `waku-client`, exactly as the desktop does, seeds one small
//! project and session through the ordinary `saveTaskState`, then times
//! sequential requests (one in flight at a time):
//!
//! | Name | Request |
//! | --- | --- |
//! | `get_settings` | `getSettings`: the smallest round trip, the transport floor |
//! | `hydrate_session` | `hydrateSession` of a two-message session |
//! | `list_project_files` | `workspace.listProjectFiles` over the seeded project (the composer's `@` file search) |
//! | `search_session_messages` | `searchSessionMessages` (sidebar search) |
//!
//! ```text
//! rpc-latency [--address ADDR] [--token TOKEN] [--count N] [--warmup N]
//!             [--files N] [--project DIR] [--label TEXT] [--out FILE]
//! ```
//!
//! The address and token default to `$WAKU_DAEMON_ADDRESS` and
//! `$WAKU_DAEMON_TOKEN`. Point it only at a disposable daemon: it adds a
//! project and a session to the daemon's database.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context as _, anyhow, bail};
use serde_json::{Value, json};
use uuid::Uuid;
use waku_bench_daemon::fixture;
use waku_client::{Command, DaemonClient, ResponsePayload, WorkspaceOperation, WorkspaceResult};
use waku_core::model::ProviderKind;
use waku_protocol::{DAEMON_ADDRESS_ENV, DAEMON_TOKEN_ENV};

const USAGE: &str = "\
usage: rpc-latency [--address ADDR] [--token TOKEN] [--count N] [--warmup N]
                   [--files N] [--project DIR] [--label TEXT] [--out FILE]

  --address ADDR   daemon address (default: $WAKU_DAEMON_ADDRESS)
  --token TOKEN    daemon token (default: $WAKU_DAEMON_TOKEN)
  --count N        timed requests per kind (default: 1000)
  --warmup N       untimed requests per kind first (default: 50)
  --files N        files to create in the seeded project (default: 500)
  --project DIR    project directory to create and seed (default: a temp dir)
  --label TEXT     recorded in the JSON (e.g. waku-daemon)
  --out FILE       also write the JSON report here

Seeds a project and a session through saveTaskState: use a disposable daemon.";

/// Fixed so repeated runs against one daemon reuse the same rows.
const RPC_PROJECT_ID: Uuid = Uuid::from_u128(0xbe4c_0000_0000_4000_8000_0000_0000_0f0f);

struct Options {
    address: String,
    token: String,
    count: usize,
    warmup: usize,
    files: usize,
    project: Option<PathBuf>,
    label: Option<String>,
    out: Option<PathBuf>,
}

fn parse() -> anyhow::Result<Options> {
    let mut options = Options {
        address: std::env::var(DAEMON_ADDRESS_ENV).unwrap_or_default(),
        token: std::env::var(DAEMON_TOKEN_ENV).unwrap_or_default(),
        count: 1_000,
        warmup: 50,
        files: 500,
        project: None,
        label: None,
        out: None,
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
            "--count" => options.count = value()?.parse().context("--count")?,
            "--warmup" => options.warmup = value()?.parse().context("--warmup")?,
            "--files" => options.files = value()?.parse().context("--files")?,
            "--project" => options.project = Some(PathBuf::from(value()?)),
            "--label" => options.label = Some(value()?),
            "--out" => options.out = Some(PathBuf::from(value()?)),
            "--help" | "-h" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            other => bail!("unknown argument {other:?}\n\n{USAGE}"),
        }
    }
    if options.address.trim().is_empty() || options.token.is_empty() {
        bail!("a daemon address and token are required\n\n{USAGE}");
    }
    if options.count == 0 {
        bail!("--count must be at least 1");
    }
    Ok(options)
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

fn time_requests(
    client: &DaemonClient,
    warmup: usize,
    count: usize,
    command: impl Fn() -> Command,
    check: impl Fn(&ResponsePayload) -> bool,
) -> anyhow::Result<Samples> {
    let mut samples = Samples(Vec::with_capacity(count));
    for index in 0..warmup + count {
        let started = Instant::now();
        let response = client.request(Uuid::nil(), Uuid::nil(), command())?;
        let elapsed = started.elapsed();
        if !check(&response) {
            bail!("unexpected response: {response:?}");
        }
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

    let client = DaemonClient::connect(options.address.trim(), options.token.clone())?;

    // One project with one settled two-message session, saved the way the
    // desktop saves.
    let mut seeded = fixture::build(
        project_dir.clone(),
        &["rpc".to_owned()],
        ProviderKind::Codex,
        0,
        0,
    );
    seeded.project.id = RPC_PROJECT_ID;
    for session in &mut seeded.sessions {
        session.project_id = RPC_PROJECT_ID;
    }
    let session_id = seeded.sessions[0].id;
    let saved = client.request(
        Uuid::nil(),
        Uuid::nil(),
        Command::SaveTaskState {
            projects: vec![seeded.project.clone()],
            live_session_ids: Vec::new(),
            sessions: seeded.sessions.clone(),
        },
    )?;
    if !matches!(saved, ResponsePayload::TaskStateSaved { .. }) {
        bail!("saveTaskState returned {saved:?}");
    }

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
    run(
        "get_settings",
        time_requests(
            &client,
            options.warmup,
            options.count,
            || Command::GetSettings,
            |response| matches!(response, ResponsePayload::Settings { .. }),
        ),
    )?;
    run(
        "hydrate_session",
        time_requests(
            &client,
            options.warmup,
            options.count,
            || Command::HydrateSession { session_id },
            |response| {
                matches!(
                    response,
                    ResponsePayload::Session {
                        session: Some(session)
                    } if session.messages.len() == 2
                )
            },
        ),
    )?;
    let files = options.files;
    run(
        "list_project_files",
        time_requests(
            &client,
            options.warmup,
            options.count,
            || Command::Workspace {
                operation: WorkspaceOperation::ListProjectFiles {
                    root: project_dir.clone(),
                    cap: 10_000,
                },
            },
            |response| {
                matches!(
                    response,
                    ResponsePayload::Workspace {
                        result: WorkspaceResult::ProjectFiles { entries }
                    } if entries.len() >= files
                )
            },
        ),
    )?;
    run(
        "search_session_messages",
        time_requests(
            &client,
            options.warmup,
            options.count,
            || Command::SearchSessionMessages {
                query: "timeline".into(),
                limit: 50,
            },
            |response| matches!(response, ResponsePayload::SessionMessageMatches { .. }),
        ),
    )?;
    client.disconnect();

    let report = json!({
        "schema": "waku.rpc-latency/1",
        "label": options.label,
        "address": options.address.trim(),
        "count": options.count,
        "warmup": options.warmup,
        "project_files": options.files,
        "requests": results,
    });
    if let Some(out) = &options.out {
        std::fs::write(out, serde_json::to_vec_pretty(&report)?)
            .with_context(|| format!("could not write {}", out.display()))?;
    }
    if temporary {
        let _ = std::fs::remove_dir_all(&project_dir);
    }
    Ok(())
}
