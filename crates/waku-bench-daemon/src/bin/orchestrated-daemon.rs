//! The orchestrated daemon as `waku-daemon`'s `main` composes it
//! (`WakuBackend`, `DaemonHost`, protocol v8 on a local socket), over a
//! data directory of the caller's choosing, so a test can `kill -9` it and
//! start it again (#59, `tests/restart_recovery.rs`).
//!
//! ```text
//! orchestrated-daemon --root DIR --socket PATH --token TOKEN
//!     [--provider ID=BINARY]... [--no-continue]
//! ```
//!
//! It prints one JSON line once it serves:
//! `{"pid": …, "socket": …, "recovered": [thread ids], "continuations": n}`.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context as _, anyhow, bail};
use waku_core::daemon::WakuBackend;
use waku_core::daemon_host::{DaemonHost, HostOptions};
use waku_core::model::ProviderKind;
use waku_core::{DaemonSettingsStore, ServerOptions};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

struct Arguments {
    root: PathBuf,
    socket: PathBuf,
    token: String,
    providers: Vec<(ProviderKind, String)>,
    continue_after_restart: bool,
}

fn parse() -> anyhow::Result<Arguments> {
    let mut root = None;
    let mut socket = None;
    let mut token = None;
    let mut providers = Vec::new();
    let mut continue_after_restart = true;
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        let mut value = || {
            arguments
                .next()
                .ok_or_else(|| anyhow!("{argument} needs a value"))
        };
        match argument.as_str() {
            "--root" => root = Some(PathBuf::from(value()?)),
            "--socket" => socket = Some(PathBuf::from(value()?)),
            "--token" => token = Some(value()?),
            "--provider" => {
                let spec = value()?;
                let (id, binary) = spec
                    .split_once('=')
                    .ok_or_else(|| anyhow!("--provider takes ID=BINARY"))?;
                let provider = ProviderKind::ALL
                    .into_iter()
                    .find(|provider| provider.id() == id)
                    .ok_or_else(|| anyhow!("unknown provider {id}"))?;
                providers.push((provider, binary.to_owned()));
            }
            "--no-continue" => continue_after_restart = false,
            other => bail!("unknown argument {other:?}"),
        }
    }
    Ok(Arguments {
        root: root.context("--root is required")?,
        socket: socket.context("--socket is required")?,
        token: token.context("--token is required")?,
        providers,
        continue_after_restart,
    })
}

fn main() -> anyhow::Result<()> {
    let arguments = parse()?;
    std::fs::create_dir_all(&arguments.root)?;
    let settings = DaemonSettingsStore::open(arguments.root.join("settings.json"))?;
    let mut document = settings.get();
    for (provider, binary) in &arguments.providers {
        document
            .provider_binary_overrides
            .insert(*provider, binary.clone());
    }
    document.set_continue_threads_after_restart(arguments.continue_after_restart);
    settings.replace(document)?;
    let backend = Arc::new(WakuBackend::new(settings, arguments.root.to_path_buf())?);
    let host = DaemonHost::start(&backend, HostOptions::default())?;
    let local = waku_core::LocalListener::bind(&arguments.socket)?;
    let ready = serde_json::json!({
        "pid": std::process::id(),
        "socket": local.path(),
        "recovered": host.recovery().threads,
        "continuations": host.recovery().continuations,
    });
    println!("{ready}");
    let shutdown = waku_core::ShutdownSignal::new();
    let result = waku_core::serve_listeners(
        waku_core::Listeners {
            tcp: None,
            local: Some(local),
        },
        arguments.token,
        backend,
        shutdown,
        ServerOptions {
            orchestration: Some(host.orchestration()),
            environment_id: Some(host.environment_id()),
            ..ServerOptions::default()
        },
    );
    host.shutdown();
    result
}
