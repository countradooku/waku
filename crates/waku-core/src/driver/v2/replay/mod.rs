//! Replay fixtures (RFC 0002 "Replay fixtures", D-8, #45).
//!
//! A fixture is an NDJSON transcript of one provider session in T3's record
//! shapes: a `transcript_start` header, then `expect_outbound`,
//! `emit_inbound` and `runtime_exit` entries, plus Waku's optional `channel`.
//! [`ReplayTransportFactory`] stands in for the provider's processes: it
//! checks every frame a driver writes against the next expected one and
//! feeds the recorded inbound frames back. [`harness::ReplayHarness`] runs
//! the real orchestrator, sink, projections, effect workers, checkpoints and
//! session manager on top, so only the transport is replaced.

pub mod harness;
pub mod matcher;
pub mod record;
pub mod sanitize;
pub mod transport;

use std::fmt;
use std::io::{self, Write as _};
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub use self::harness::{ReplayHarness, ScenarioInput, ScenarioResult, ScenarioStep};
pub use self::record::RecordingTransportFactory;
pub use self::sanitize::{SanitizeError, Sanitizer};
pub use self::transport::ReplayTransportFactory;

/// The transcript header (`transcript_start`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TranscriptHeader {
    /// `ProviderKind::id()`, or `fake` for the test driver.
    pub provider: String,
    pub protocol: String,
    /// The CLI version recorded.
    pub version: String,
    pub scenario: String,
    /// Free-form; `ignore`, `minCliVersion`, `synthetic` and
    /// `recorderOnlyKeys` are reserved.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub metadata: Map<String, Value>,
}

impl TranscriptHeader {
    /// JSON Pointers removed from both sides before outbound frames compare.
    pub fn ignored_pointers(&self) -> Vec<String> {
        self.metadata
            .get("ignore")
            .and_then(Value::as_array)
            .map(|pointers| {
                pointers
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default()
    }
}

fn is_main(channel: &Option<String>) -> bool {
    channel.as_deref().is_none_or(|channel| channel == "main")
}

/// The status a recorded process ended with.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExitStatusKind {
    Success,
    Error,
    Cancelled,
}

/// One transcript entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum ReplayEntry {
    ExpectOutbound {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        label: Option<String>,
        #[serde(default, skip_serializing_if = "is_main")]
        channel: Option<String>,
        frame: Value,
    },
    EmitInbound {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        label: Option<String>,
        #[serde(default, skip_serializing_if = "is_main")]
        channel: Option<String>,
        frame: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        after_ms: Option<u64>,
    },
    RuntimeExit {
        #[serde(default, skip_serializing_if = "is_main")]
        channel: Option<String>,
        status: ExitStatusKind,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signal: Option<i32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<Value>,
    },
}

impl ReplayEntry {
    /// The channel the entry belongs to (`"main"` by default).
    pub fn channel(&self) -> &str {
        let channel = match self {
            Self::ExpectOutbound { channel, .. }
            | Self::EmitInbound { channel, .. }
            | Self::RuntimeExit { channel, .. } => channel,
        };
        channel.as_deref().unwrap_or("main")
    }

    pub fn label(&self) -> Option<&str> {
        match self {
            Self::ExpectOutbound { label, .. } | Self::EmitInbound { label, .. } => {
                label.as_deref()
            }
            Self::RuntimeExit { .. } => None,
        }
    }

    /// A one-line description for error messages.
    pub fn describe(&self) -> String {
        let kind = match self {
            Self::ExpectOutbound { .. } => "expect_outbound",
            Self::EmitInbound { .. } => "emit_inbound",
            Self::RuntimeExit { .. } => "runtime_exit",
        };
        match self.label() {
            Some(label) => format!("{kind} {label} ({})", self.channel()),
            None => format!("{kind} ({})", self.channel()),
        }
    }
}

/// A parsed transcript; `lines[i]` is the 1-based file line of `entries[i]`.
#[derive(Clone, Debug, PartialEq)]
pub struct ReplayTranscript {
    pub header: TranscriptHeader,
    pub entries: Vec<ReplayEntry>,
    pub lines: Vec<usize>,
}

impl ReplayTranscript {
    pub fn new(header: TranscriptHeader, entries: Vec<ReplayEntry>) -> Self {
        let lines = (0..entries.len()).map(|index| index + 2).collect();
        Self {
            header,
            entries,
            lines,
        }
    }

    pub fn parse(text: &str) -> Result<Self, ReplayError> {
        let mut records = text
            .lines()
            .enumerate()
            .map(|(index, line)| (index + 1, line.trim()))
            .filter(|(_, line)| !line.is_empty());
        let Some((first_line, first)) = records.next() else {
            return Err(ReplayError::Parse {
                line: 0,
                message: "the transcript is empty".to_owned(),
            });
        };
        let parse_error = |line: usize, message: String| ReplayError::Parse { line, message };
        let header: Value = serde_json::from_str(first)
            .map_err(|error| parse_error(first_line, error.to_string()))?;
        if header.get("type").and_then(Value::as_str) != Some("transcript_start") {
            return Err(parse_error(
                first_line,
                "the first record must be transcript_start".to_owned(),
            ));
        }
        let header: TranscriptHeader = serde_json::from_value(header)
            .map_err(|error| parse_error(first_line, error.to_string()))?;
        let mut entries = Vec::new();
        let mut lines = Vec::new();
        for (line, record) in records {
            let value: Value = serde_json::from_str(record)
                .map_err(|error| parse_error(line, error.to_string()))?;
            if value.get("type").and_then(Value::as_str) == Some("transcript_start") {
                return Err(parse_error(
                    line,
                    "transcript_start is only valid as the first record".to_owned(),
                ));
            }
            let entry: ReplayEntry = serde_json::from_value(value)
                .map_err(|error| parse_error(line, error.to_string()))?;
            entries.push(entry);
            lines.push(line);
        }
        Ok(Self {
            header,
            entries,
            lines,
        })
    }

    pub fn to_ndjson(&self) -> String {
        let mut out = String::new();
        let mut header = serde_json::to_value(&self.header).expect("header serializes");
        if let Value::Object(map) = &mut header {
            let mut ordered = Map::new();
            ordered.insert(
                "type".to_owned(),
                Value::String("transcript_start".to_owned()),
            );
            ordered.extend(std::mem::take(map));
            *map = ordered;
        }
        out.push_str(&header.to_string());
        out.push('\n');
        for entry in &self.entries {
            out.push_str(&serde_json::to_string(entry).expect("entries serialize"));
            out.push('\n');
        }
        out
    }
}

pub fn read_transcript(path: &Path) -> Result<ReplayTranscript, ReplayError> {
    let text = std::fs::read_to_string(path).map_err(|error| ReplayError::Io(error.to_string()))?;
    ReplayTranscript::parse(&text)
}

pub fn write_transcript(path: &Path, transcript: &ReplayTranscript) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = std::fs::File::create(path)?;
    file.write_all(transcript.to_ndjson().as_bytes())?;
    file.sync_all()
}

/// Why a replay failed. `Display` is written for a failing test's output.
#[derive(Clone, Debug, PartialEq)]
pub enum ReplayError {
    /// An outbound frame differed from the expected one.
    Mismatch {
        scenario: String,
        line: usize,
        label: Option<String>,
        expected: Value,
        actual: Value,
        /// A unified diff of the two pretty-printed canonical frames.
        diff: String,
    },
    /// An outbound frame while the cursor is at `runtime_exit` or past the end.
    UnexpectedOutbound {
        scenario: String,
        channel: String,
        frame: Value,
    },
    /// Entries left once the scenario is idle.
    NotExhausted {
        scenario: String,
        remaining: Vec<String>,
    },
    Parse {
        line: usize,
        message: String,
    },
    Io(String),
    /// The harness itself failed (a step, a timeout, a verify).
    Harness(String),
}

impl fmt::Display for ReplayError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mismatch {
                scenario,
                line,
                label,
                diff,
                ..
            } => write!(
                formatter,
                "replay {scenario}: outbound frame does not match line {line}{}\n{diff}",
                label
                    .as_deref()
                    .map(|label| format!(" ({label})"))
                    .unwrap_or_default()
            ),
            Self::UnexpectedOutbound {
                scenario,
                channel,
                frame,
            } => write!(
                formatter,
                "replay {scenario}: unexpected outbound frame on {channel} after the transcript ended:\n{}",
                serde_json::to_string_pretty(frame).unwrap_or_default()
            ),
            Self::NotExhausted {
                scenario,
                remaining,
            } => write!(
                formatter,
                "replay {scenario}: {} entries were never reached:\n  {}",
                remaining.len(),
                remaining.join("\n  ")
            ),
            Self::Parse { line, message } => {
                write!(formatter, "replay transcript line {line}: {message}")
            }
            Self::Io(message) => write!(formatter, "replay i/o: {message}"),
            Self::Harness(message) => write!(formatter, "replay harness: {message}"),
        }
    }
}

impl std::error::Error for ReplayError {}

/// What differs per driver when replaying (`ReplayProfile::ignored_pointers`,
/// for example a CLI's `clientInfo.version`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReplayProfile {
    pub protocol: String,
    pub ignored_pointers: Vec<String>,
}

impl ReplayProfile {
    pub fn new(protocol: impl Into<String>) -> Self {
        Self {
            protocol: protocol.into(),
            ignored_pointers: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transcripts_round_trip_in_t3_record_shapes() {
        let text = r#"{"type":"transcript_start","provider":"fake","protocol":"fake.jsonl","version":"1.0.0","scenario":"simple","metadata":{"ignore":["/params/client"]}}
{"type":"expect_outbound","label":"initialize","frame":{"id":1,"method":"initialize"}}

{"type":"emit_inbound","frame":{"id":1,"result":{}},"afterMs":40}
{"type":"expect_outbound","channel":"fork-1","frame":{"id":1}}
{"type":"runtime_exit","status":"success"}
"#;
        let transcript = ReplayTranscript::parse(text).unwrap();
        assert_eq!(transcript.entries.len(), 4);
        assert_eq!(transcript.lines, vec![2, 4, 5, 6]);
        assert_eq!(transcript.header.ignored_pointers(), vec!["/params/client"]);
        assert_eq!(transcript.entries[2].channel(), "fork-1");
        assert!(matches!(
            transcript.entries[1],
            ReplayEntry::EmitInbound {
                after_ms: Some(40),
                ..
            }
        ));
        let again = ReplayTranscript::parse(&transcript.to_ndjson()).unwrap();
        assert_eq!(again.entries, transcript.entries);
        assert_eq!(again.header, transcript.header);
    }

    #[test]
    fn a_missing_header_or_a_bad_line_is_a_parse_error_with_its_line() {
        let error =
            ReplayTranscript::parse("{\"type\":\"expect_outbound\",\"frame\":{}}").unwrap_err();
        assert!(matches!(error, ReplayError::Parse { line: 1, .. }));
        let error = ReplayTranscript::parse(
            "{\"type\":\"transcript_start\",\"provider\":\"p\",\"protocol\":\"x\",\"version\":\"1\",\"scenario\":\"s\"}\nnot json",
        )
        .unwrap_err();
        assert!(matches!(error, ReplayError::Parse { line: 2, .. }));
    }
}
