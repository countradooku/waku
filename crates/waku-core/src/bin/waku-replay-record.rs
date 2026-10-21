//! Records a replay fixture from a live session (RFC 0002 "Recorder", #45).
//!
//! ```text
//! cargo run -p waku-core --bin waku-replay-record -- \
//!   --provider fake --scenario simple [--fixtures DIR] [--out PATH] [--renumber-ids]
//! ```
//!
//! Runs `<fixtures>/<scenario>/input.json` against the provider through its
//! `DriverV2`, writes `<fixtures>/<scenario>/<provider>.ndjson` sanitized,
//! fails closed on any leaked secret, then replays the file and keeps it only
//! if replay passes. Only drivers ported to `DriverV2` can record; today that
//! is the in-repo `fake` provider, and each port (#46–#52) adds its own.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use serde_json::{Map, Value};
use waku_core::driver::opencode::{OPENCODE_PROTOCOL, OPENCODE_PROVIDER, OpenCodeDriver};
use waku_core::driver::v2::fake::{
    FAKE_PROTOCOL, FAKE_PROVIDER, FakeDriver, FakeProfile, fake_instance,
};
use waku_core::driver::v2::fake_agent::FakeAgentFactory;
use waku_core::driver::v2::replay::record::{RecordRequest, record_scenario};
use waku_core::driver::v2::replay::{ScenarioInput, TranscriptHeader};
use waku_core::driver::v2::{DriverV2, InstanceConfig, ProcessTransportFactory, TransportFactory};
use waku_protocol::orchestration::DriverKind;

struct Args {
    provider: String,
    scenario: String,
    fixtures: PathBuf,
    out: Option<PathBuf>,
    renumber_ids: bool,
    binary: Option<PathBuf>,
    /// HTTP drivers: the running service to record against
    /// (`http://user:password@127.0.0.1:port`). Replay re-runs the same
    /// instance, so the driver never discovers or starts a service itself.
    service_url: Option<String>,
    cli_version: Option<String>,
}

/// What records a provider: its driver, the instance, the protocol name and
/// the live transports.
struct Target {
    driver: Arc<dyn DriverV2>,
    instance: InstanceConfig,
    protocol: &'static str,
    live: Arc<dyn TransportFactory>,
}

fn target(args: &Args) -> Option<Target> {
    let service = |provider: &str| -> Option<String> {
        let url = args.service_url.clone();
        if url.is_none() {
            eprintln!(
                "waku-replay-record: {provider} records against a running service; pass --service-url"
            );
        }
        url
    };
    let target = match args.provider.as_str() {
        FAKE_PROVIDER => Target {
            driver: Arc::new(FakeDriver),
            instance: fake_instance(FakeProfile::default()),
            protocol: FAKE_PROTOCOL,
            live: Arc::new(FakeAgentFactory::new(
                std::env::temp_dir().join("waku-replay-record-home"),
            )),
        },
        OPENCODE_PROVIDER => {
            let mut instance = InstanceConfig::new(OPENCODE_PROVIDER, DriverKind::OpenCode);
            instance.binary = args.binary.clone().unwrap_or_else(|| "opencode".into());
            instance.cli_version = args.cli_version.clone();
            instance.extra = serde_json::json!({ "serviceUrl": service(OPENCODE_PROVIDER)? });
            Target {
                driver: Arc::new(OpenCodeDriver),
                instance,
                protocol: OPENCODE_PROTOCOL,
                live: Arc::new(ProcessTransportFactory),
            }
        }
        other => {
            eprintln!(
                "waku-replay-record: {other} has no DriverV2 port yet; recording arrives with its port (#46-#52)"
            );
            return None;
        }
    };
    Some(target)
}

fn parse() -> Result<Args, String> {
    let mut provider = None;
    let mut scenario = None;
    let mut fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/replay");
    let mut out = None;
    let mut renumber_ids = false;
    let mut binary = None;
    let mut service_url = None;
    let mut cli_version = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = |name: &str| args.next().ok_or_else(|| format!("{name} needs a value"));
        match arg.as_str() {
            "--provider" => provider = Some(value("--provider")?),
            "--scenario" => scenario = Some(value("--scenario")?),
            "--fixtures" => fixtures = PathBuf::from(value("--fixtures")?),
            "--out" => out = Some(PathBuf::from(value("--out")?)),
            "--renumber-ids" => renumber_ids = true,
            // Accepted for parity with RFC 0002; the fake provider has no
            // timing-sensitive scenario yet.
            "--keep-timing" => {}
            "--binary" => binary = Some(PathBuf::from(value("--binary")?)),
            "--service-url" => service_url = Some(value("--service-url")?),
            "--cli-version" => cli_version = Some(value("--cli-version")?),
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
    let own_input = scenario_dir.join(format!("input.{}.json", args.provider));
    let input_path = if own_input.exists() {
        own_input
    } else {
        scenario_dir.join("input.json")
    };
    let input: ScenarioInput = match std::fs::read_to_string(&input_path)
        .map_err(|error| error.to_string())
        .and_then(|text| serde_json::from_str(&text).map_err(|error| error.to_string()))
    {
        Ok(input) => input,
        Err(error) => {
            eprintln!("waku-replay-record: {}: {error}", input_path.display());
            return ExitCode::from(2);
        }
    };
    let out = args
        .out
        .clone()
        .unwrap_or_else(|| scenario_dir.join(format!("{}.ndjson", args.provider)));
    let Some(target) = target(&args) else {
        return ExitCode::from(2);
    };
    let instance = target.instance;
    let mut metadata = Map::new();
    metadata.insert("recordedBy".to_owned(), Value::from("waku-replay-record"));
    metadata.insert(
        "minCliVersion".to_owned(),
        Value::from(instance.cli_version.clone().unwrap_or_default()),
    );
    metadata.insert("synthetic".to_owned(), Value::from(false));
    let header = TranscriptHeader {
        provider: args.provider.clone(),
        protocol: target.protocol.to_owned(),
        version: instance.cli_version.clone().unwrap_or_default(),
        scenario: args.scenario.clone(),
        metadata,
    };
    let result = record_scenario(RecordRequest {
        driver: target.driver,
        instance,
        live: target.live,
        header,
        input,
        out: out.clone(),
        renumber_ids: args.renumber_ids,
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
