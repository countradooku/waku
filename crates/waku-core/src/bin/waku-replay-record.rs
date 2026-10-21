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
//! is the in-repo `fake` provider and Claude Code (#47); each port (#46–#52)
//! adds its own.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use serde_json::{Map, Value};
use waku_core::driver::claude::{CLAUDE_PROTOCOL, CLAUDE_PROVIDER, ClaudeDriver, claude_instance};
use waku_core::driver::v2::fake::{
    FAKE_PROTOCOL, FAKE_PROVIDER, FakeDriver, FakeProfile, fake_instance,
};
use waku_core::driver::v2::fake_agent::FakeAgentFactory;
use waku_core::driver::v2::replay::record::{RecordRequest, record_scenario};
use waku_core::driver::v2::replay::{ScenarioInput, TranscriptHeader};
use waku_core::driver::v2::{DriverV2, InstanceConfig, ProcessTransportFactory, TransportFactory};

struct Args {
    provider: String,
    scenario: String,
    fixtures: PathBuf,
    out: Option<PathBuf>,
    renumber_ids: bool,
    binary: Option<PathBuf>,
}

fn parse() -> Result<Args, String> {
    let mut provider = None;
    let mut scenario = None;
    let mut fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/replay");
    let mut out = None;
    let mut renumber_ids = false;
    let mut binary = None;
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
            // Accepted for parity with RFC 0002; scenarios pick models.
            "--model" => {
                value("--model")?;
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
    if args.provider != FAKE_PROVIDER && args.provider != CLAUDE_PROVIDER {
        eprintln!(
            "waku-replay-record: {} has no DriverV2 port yet; recording arrives with its port (#46-#52)",
            args.provider
        );
        return ExitCode::from(2);
    }
    let scenario_dir = args.fixtures.join(&args.scenario);
    let input: ScenarioInput = match std::fs::read_to_string(scenario_dir.join("input.json"))
        .map_err(|error| error.to_string())
        .and_then(|text| serde_json::from_str(&text).map_err(|error| error.to_string()))
    {
        Ok(input) => input,
        Err(error) => {
            eprintln!(
                "waku-replay-record: {}/input.json: {error}",
                scenario_dir.display()
            );
            return ExitCode::from(2);
        }
    };
    let out = args
        .out
        .unwrap_or_else(|| scenario_dir.join(format!("{}.ndjson", args.provider)));
    // Claude records against the real CLI (`--binary`, default `claude` on
    // the search path) through the real process transport.
    let (instance, protocol, driver, live): (
        InstanceConfig,
        &str,
        Arc<dyn DriverV2>,
        Arc<dyn TransportFactory>,
    ) = if args.provider == CLAUDE_PROVIDER {
        let mut instance = claude_instance();
        if let Some(binary) = args.binary.clone() {
            instance.binary = binary;
        }
        (
            instance,
            CLAUDE_PROTOCOL,
            Arc::new(ClaudeDriver),
            Arc::new(ProcessTransportFactory),
        )
    } else {
        let home = std::env::temp_dir().join("waku-replay-record-home");
        (
            fake_instance(FakeProfile::default()),
            FAKE_PROTOCOL,
            Arc::new(FakeDriver),
            Arc::new(FakeAgentFactory::new(home)),
        )
    };
    let mut metadata = Map::new();
    metadata.insert("recordedBy".to_owned(), Value::from("waku-replay-record"));
    metadata.insert("minCliVersion".to_owned(), Value::from("1.0.0"));
    metadata.insert("synthetic".to_owned(), Value::from(false));
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
