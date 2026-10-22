//! Records a replay fixture from a live session (RFC 0002 "Recorder", #45).
//!
//! ```text
//! cargo run -p waku-core --bin waku-replay-record -- \
//!   --provider fake --scenario simple [--fixtures DIR] [--out PATH] [--renumber-ids]
//!   [--binary PATH] [--script TRANSCRIPT] [--source TEXT] [--synthetic]
//!   [--cli-version V] [--service-url URL]
//! ```
//!
//! Runs `<fixtures>/<scenario>/input.<provider>.json` (or the shared
//! `input.json`) against the provider through its `DriverV2`, writes
//! `<fixtures>/<scenario>/<provider>.ndjson` sanitized, fails closed on any
//! leaked secret, then replays the file and keeps it only if replay passes.
//! Only drivers ported to `DriverV2` can record: the in-repo `fake` provider,
//! `codex` (#46), `claude` (#47), `pi` / `ohmypi` (#50) and `amp` (#51),
//! each against the real CLI (`--binary`, default `claude`, `pi`, `omp` or
//! `amp` on the search path). `--script` plays a transcript's inbound
//! frames to the driver instead of the live CLI (Codex only; see
//! `driver::codex::script`). The HTTP drivers `opencode` (#49) and `deepseek`
//! (#52) record against a running service given by `--service-url`
//! (OpenCode: `http://opencode:<password>@127.0.0.1:<port>`; DeepSeek: a
//! `dsh web` URL), so neither the recording nor its replay check discovers
//! or starts one.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use serde_json::{Map, Value};
use waku_core::driver::amp::{AMP_PROTOCOL, AMP_PROVIDER, AmpDriver, amp_instance};
use waku_core::driver::claude::{CLAUDE_PROTOCOL, CLAUDE_PROVIDER, ClaudeDriver, claude_instance};
use waku_core::driver::codex::script::ScriptedAppServer;
use waku_core::driver::codex::{
    CODEX_FIXTURE_VERSION, CODEX_PROTOCOL, CODEX_PROVIDER, CodexDriver, codex_instance,
};
use waku_core::driver::deepseek::{DEEPSEEK_PROTOCOL, DEEPSEEK_PROVIDER, DeepSeekDriver};
use waku_core::driver::opencode::{OPENCODE_PROTOCOL, OPENCODE_PROVIDER, OpenCodeDriver};
use waku_core::driver::pi::{
    OHMYPI_PROVIDER, PI_PROTOCOL, PI_PROVIDER, PiDriver, PiFlavor, pi_replay_instance,
};
use waku_core::driver::v2::fake::{
    FAKE_PROTOCOL, FAKE_PROVIDER, FakeDriver, FakeProfile, fake_instance,
};
use waku_core::driver::v2::fake_agent::FakeAgentFactory;
use waku_core::driver::v2::replay::record::{RecordRequest, record_scenario};
use waku_core::driver::v2::replay::{ScenarioInput, TranscriptHeader, read_transcript};
use waku_core::driver::v2::{DriverV2, InstanceConfig, ProcessTransportFactory, TransportFactory};
use waku_protocol::orchestration::DriverKind;

struct Args {
    provider: String,
    scenario: String,
    fixtures: PathBuf,
    out: Option<PathBuf>,
    renumber_ids: bool,
    binary: Option<PathBuf>,
    script: Option<PathBuf>,
    source: Option<String>,
    synthetic: bool,
    /// HTTP drivers: the running service to record against
    /// (`http://user:password@127.0.0.1:port`). Replay re-runs the same
    /// instance, so the driver never discovers or starts a service itself.
    service_url: Option<String>,
    cli_version: Option<String>,
}

fn parse() -> Result<Args, String> {
    let mut provider = None;
    let mut scenario = None;
    let mut fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/replay");
    let mut out = None;
    let mut renumber_ids = false;
    let (mut binary, mut script, mut source, mut synthetic) = (None, None, None, false);
    let (mut service_url, mut cli_version) = (None, None);
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = |name: &str| args.next().ok_or_else(|| format!("{name} needs a value"));
        match arg.as_str() {
            "--provider" => provider = Some(value("--provider")?),
            "--scenario" => scenario = Some(value("--scenario")?),
            "--fixtures" => fixtures = PathBuf::from(value("--fixtures")?),
            "--out" => out = Some(PathBuf::from(value("--out")?)),
            "--renumber-ids" => renumber_ids = true,
            "--binary" => binary = Some(PathBuf::from(value("--binary")?)),
            "--script" => script = Some(PathBuf::from(value("--script")?)),
            "--source" => source = Some(value("--source")?),
            "--synthetic" => synthetic = true,
            "--service-url" => service_url = Some(value("--service-url")?),
            "--cli-version" => cli_version = Some(value("--cli-version")?),
            // Accepted for parity with RFC 0002; no recorded scenario is
            // timing-sensitive yet.
            "--keep-timing" => {}
            "--model" => {
                value(&arg)?;
            }
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(Args {
        provider: provider.ok_or("--provider is required")?,
        scenario: scenario.ok_or("--scenario is required")?,
        fixtures,
        out,
        renumber_ids,
        binary,
        script,
        source,
        synthetic,
        service_url,
        cli_version,
    })
}

fn main() -> ExitCode {
    let args = match parse() {
        Ok(args) => args,
        Err(error) => {
            eprintln!("waku-replay-record: {error}");
            return ExitCode::from(2);
        }
    };
    let scenario_dir = args.fixtures.join(&args.scenario);
    let input = match ScenarioInput::load(&scenario_dir, &args.provider) {
        Ok(input) => input,
        Err(error) => {
            eprintln!("waku-replay-record: {error}");
            return ExitCode::from(2);
        }
    };
    let out = args
        .out
        .clone()
        .unwrap_or_else(|| scenario_dir.join(format!("{}.ndjson", args.provider)));
    let home = std::env::temp_dir().join("waku-replay-record-home");
    let mut metadata = Map::new();
    metadata.insert("recordedBy".to_owned(), Value::from("waku-replay-record"));
    if let Some(source) = &args.source {
        metadata.insert("source".to_owned(), Value::from(source.clone()));
    }
    let (driver, instance, live, protocol): (
        Arc<dyn DriverV2>,
        InstanceConfig,
        Arc<dyn TransportFactory>,
        &str,
    ) = match args.provider.as_str() {
        FAKE_PROVIDER => {
            metadata.insert("minCliVersion".to_owned(), Value::from("1.0.0"));
            (
                Arc::new(FakeDriver),
                fake_instance(FakeProfile::default()),
                Arc::new(FakeAgentFactory::new(home)),
                FAKE_PROTOCOL,
            )
        }
        CODEX_PROVIDER => {
            let mut instance = codex_instance();
            if let Some(binary) = &args.binary {
                instance.binary = binary.clone();
            }
            metadata.insert(
                "minCliVersion".to_owned(),
                Value::from(CODEX_FIXTURE_VERSION),
            );
            // Waku's own version in `initialize` changes with every release.
            metadata.insert(
                "ignore".to_owned(),
                serde_json::json!(["/params/clientInfo/version"]),
            );
            let live: Arc<dyn TransportFactory> = match &args.script {
                Some(script) => match read_transcript(script) {
                    Ok(script) => ScriptedAppServer::new(script, home),
                    Err(error) => {
                        eprintln!("waku-replay-record: {}: {error}", script.display());
                        return ExitCode::from(2);
                    }
                },
                None => Arc::new(ProcessTransportFactory),
            };
            (Arc::new(CodexDriver::new()), instance, live, CODEX_PROTOCOL)
        }
        CLAUDE_PROVIDER => {
            if args.script.is_some() {
                eprintln!("waku-replay-record: --script is Codex only");
                return ExitCode::from(2);
            }
            let mut instance = claude_instance();
            if let Some(binary) = &args.binary {
                instance.binary = binary.clone();
            }
            metadata.insert("minCliVersion".to_owned(), Value::from("1.0.0"));
            (
                Arc::new(ClaudeDriver),
                instance,
                Arc::new(ProcessTransportFactory),
                CLAUDE_PROTOCOL,
            )
        }
        AMP_PROVIDER => {
            if args.script.is_some() {
                eprintln!("waku-replay-record: --script is Codex only");
                return ExitCode::from(2);
            }
            let mut instance = amp_instance();
            if let Some(binary) = &args.binary {
                instance.binary = binary.clone();
            }
            instance.cli_version = args.cli_version.clone();
            metadata.insert(
                "minCliVersion".to_owned(),
                Value::from(args.cli_version.clone().unwrap_or_default()),
            );
            (
                Arc::new(AmpDriver::new()),
                instance,
                Arc::new(ProcessTransportFactory),
                AMP_PROTOCOL,
            )
        }
        PI_PROVIDER | OHMYPI_PROVIDER => {
            if args.script.is_some() {
                eprintln!("waku-replay-record: --script is Codex only");
                return ExitCode::from(2);
            }
            let flavor = if args.provider == PI_PROVIDER {
                PiFlavor::Pi
            } else {
                PiFlavor::OhMyPi
            };
            // The same instance replay uses: the approval extension goes
            // under the temp directory.
            let mut instance = pi_replay_instance(flavor);
            if let Some(binary) = &args.binary {
                instance.binary = binary.clone();
            }
            instance.cli_version = args.cli_version.clone();
            metadata.insert(
                "minCliVersion".to_owned(),
                Value::from(args.cli_version.clone().unwrap_or_default()),
            );
            (
                Arc::new(PiDriver::new()),
                instance,
                Arc::new(ProcessTransportFactory),
                PI_PROTOCOL,
            )
        }
        OPENCODE_PROVIDER | DEEPSEEK_PROVIDER => {
            let Some(service_url) = args.service_url.clone() else {
                eprintln!(
                    "waku-replay-record: {} records against a running service; pass --service-url",
                    args.provider
                );
                return ExitCode::from(2);
            };
            let opencode = args.provider == OPENCODE_PROVIDER;
            let mut instance = if opencode {
                InstanceConfig::new(OPENCODE_PROVIDER, DriverKind::OpenCode)
            } else {
                InstanceConfig::new(DEEPSEEK_PROVIDER, DriverKind::DeepSeek)
            };
            instance.binary = args
                .binary
                .clone()
                .unwrap_or_else(|| if opencode { "opencode" } else { "dsh" }.into());
            instance.cli_version = args.cli_version.clone();
            instance.extra = if opencode {
                serde_json::json!({ "serviceUrl": service_url })
            } else {
                serde_json::json!({ "baseUrl": service_url })
            };
            metadata.insert(
                "minCliVersion".to_owned(),
                Value::from(args.cli_version.clone().unwrap_or_default()),
            );
            let (driver, protocol): (Arc<dyn DriverV2>, &str) = if opencode {
                (Arc::new(OpenCodeDriver), OPENCODE_PROTOCOL)
            } else {
                (Arc::new(DeepSeekDriver), DEEPSEEK_PROTOCOL)
            };
            (
                driver,
                instance,
                Arc::new(ProcessTransportFactory),
                protocol,
            )
        }
        other => {
            eprintln!("waku-replay-record: {other} cannot be recorded by this tool");
            return ExitCode::from(2);
        }
    };
    metadata.insert(
        "synthetic".to_owned(),
        Value::from(args.synthetic || args.script.is_some()),
    );
    let header = TranscriptHeader {
        provider: args.provider.clone(),
        protocol: protocol.to_owned(),
        version: instance.cli_version.clone().unwrap_or_default(),
        scenario: args.scenario.clone(),
        metadata,
    };
    let result = record_scenario(RecordRequest {
        driver,
        instance,
        live,
        header,
        input,
        out: out.clone(),
        renumber_ids: args.renumber_ids,
        // A live model can think for minutes per turn.
        patience: Some(std::time::Duration::from_secs(300)),
    });
    match result {
        Ok(transcript) => {
            println!(
                "recorded {} ({} entries) and replayed it green",
                out.display(),
                transcript.entries.len()
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("waku-replay-record: {error}");
            ExitCode::FAILURE
        }
    }
}
