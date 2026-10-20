//! The timeline fixture format.
//!
//! A timeline is JSON Lines: one header object, then one object per driver
//! event with its offset from the start of the turn in microseconds.
//!
//! ```text
//! {"timeline":"prose","format":1,"description":"…","events":20003,"durationUs":200060000}
//! {"atUs":0,"kind":"turnStarted","payload":null}
//! {"atUs":50000,"kind":"textDelta","payload":"### Root"}
//! ```
//!
//! `kind` and `payload` are exactly the daemon's `WireDriverEvent`, so the
//! bench daemon sends them verbatim and the fake provider CLIs translate them
//! into provider-native messages. Committed timelines are gzip-compressed
//! (`*.jsonl.gz`); plain `*.jsonl` files load too, for hand-written ones.

use std::io::{Read as _, Write as _};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, bail, ensure};
use flate2::Compression;
use flate2::GzBuilder;
use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use waku_protocol::WireDriverEvent;

pub const FORMAT_VERSION: u32 = 1;

/// Every checked-in timeline, in the order the fixture lists its sessions.
pub const BUILTIN_NAMES: [&str; 6] = [
    "prose",
    "burst",
    "reasoning-fast",
    "code-wall",
    "tool-storm",
    "images",
];

const BUILTIN_BYTES: [&[u8]; 6] = [
    include_bytes!("../timelines/prose.jsonl.gz"),
    include_bytes!("../timelines/burst.jsonl.gz"),
    include_bytes!("../timelines/reasoning-fast.jsonl.gz"),
    include_bytes!("../timelines/code-wall.jsonl.gz"),
    include_bytes!("../timelines/tool-storm.jsonl.gz"),
    include_bytes!("../timelines/images.jsonl.gz"),
];

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Header {
    timeline: String,
    format: u32,
    description: String,
    events: usize,
    duration_us: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TimedEvent {
    /// Offset from the start of the replay, in microseconds.
    pub at_us: u64,
    pub kind: String,
    #[serde(default)]
    pub payload: Value,
}

impl TimedEvent {
    pub fn new(at_us: u64, kind: impl Into<String>, payload: Value) -> Self {
        Self {
            at_us,
            kind: kind.into(),
            payload,
        }
    }

    pub fn to_wire(&self) -> WireDriverEvent {
        WireDriverEvent::new(self.kind.clone(), self.payload.clone())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Timeline {
    pub name: String,
    pub description: String,
    pub events: Vec<TimedEvent>,
}

/// Aggregate shape of a timeline, for listings and summaries.
#[derive(Clone, Copy, Debug, Default)]
pub struct TimelineStats {
    pub events: usize,
    pub text_deltas: usize,
    pub reasoning_deltas: usize,
    pub activities: usize,
    pub text_bytes: usize,
    pub reasoning_bytes: usize,
    pub payload_bytes: usize,
    pub duration: Duration,
}

impl Timeline {
    pub fn duration(&self) -> Duration {
        Duration::from_micros(self.events.last().map_or(0, |event| event.at_us))
    }

    /// The checked-in timeline called `name`.
    pub fn builtin(name: &str) -> anyhow::Result<Self> {
        let index = BUILTIN_NAMES
            .iter()
            .position(|candidate| *candidate == name)
            .with_context(|| {
                format!(
                    "unknown timeline {name:?}; expected one of {}",
                    BUILTIN_NAMES.join(", ")
                )
            })?;
        Self::parse(BUILTIN_BYTES[index])
            .with_context(|| format!("checked-in timeline {name} is invalid"))
    }

    pub fn builtins() -> anyhow::Result<Vec<Arc<Self>>> {
        BUILTIN_NAMES
            .iter()
            .map(|name| Self::builtin(name).map(Arc::new))
            .collect()
    }

    /// A built-in name, or a path to a `.jsonl` / `.jsonl.gz` file.
    pub fn resolve(spec: &str) -> anyhow::Result<Self> {
        if BUILTIN_NAMES.contains(&spec) {
            return Self::builtin(spec);
        }
        let path = Path::new(spec);
        if path.exists() {
            return Self::load(path);
        }
        bail!(
            "timeline {spec:?} is neither a built-in ({}) nor an existing file",
            BUILTIN_NAMES.join(", ")
        )
    }

    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let bytes =
            std::fs::read(path).with_context(|| format!("could not read {}", path.display()))?;
        Self::parse(&bytes).with_context(|| format!("invalid timeline {}", path.display()))
    }

    /// Parse plain or gzip-compressed JSON Lines and validate every event
    /// against the daemon's own wire decoder.
    pub fn parse(bytes: &[u8]) -> anyhow::Result<Self> {
        let text = if bytes.starts_with(&[0x1f, 0x8b]) {
            let mut text = String::new();
            GzDecoder::new(bytes)
                .read_to_string(&mut text)
                .context("could not decompress timeline")?;
            text
        } else {
            String::from_utf8(bytes.to_vec()).context("timeline is not UTF-8")?
        };
        let mut lines = text
            .lines()
            .enumerate()
            .filter(|(_, line)| !line.trim().is_empty());
        let (_, header) = lines.next().context("timeline is empty")?;
        let header: Header = serde_json::from_str(header).context("invalid timeline header")?;
        ensure!(
            header.format == FORMAT_VERSION,
            "timeline format {} is unsupported; expected {FORMAT_VERSION}",
            header.format
        );
        let mut events = Vec::with_capacity(header.events);
        let mut previous = 0;
        for (index, line) in lines {
            let event: TimedEvent = serde_json::from_str(line)
                .with_context(|| format!("line {}: invalid timeline event", index + 1))?;
            ensure!(
                event.at_us >= previous,
                "line {}: timestamps must not go backwards",
                index + 1
            );
            previous = event.at_us;
            waku_protocol::event_from_wire(event.to_wire())
                .with_context(|| format!("line {}: not a valid driver event", index + 1))?;
            events.push(event);
        }
        ensure!(
            events.len() == header.events,
            "header promises {} events but the file has {}",
            header.events,
            events.len()
        );
        let timeline = Self {
            name: header.timeline,
            description: header.description,
            events,
        };
        ensure!(
            timeline.duration().as_micros() == u128::from(header.duration_us),
            "header duration does not match the last event"
        );
        Ok(timeline)
    }

    pub fn to_jsonl(&self) -> String {
        let header = Header {
            timeline: self.name.clone(),
            format: FORMAT_VERSION,
            description: self.description.clone(),
            events: self.events.len(),
            duration_us: self.duration().as_micros() as u64,
        };
        let mut out = serde_json::to_string(&header).expect("header serializes");
        out.push('\n');
        for event in &self.events {
            out.push_str(&serde_json::to_string(event).expect("events serialize"));
            out.push('\n');
        }
        out
    }

    /// Deterministic gzip: no file name and a zero modification time, so the
    /// same timeline always compresses to the same bytes.
    pub fn to_gzip(&self) -> Vec<u8> {
        let mut encoder = GzBuilder::new()
            .mtime(0)
            .operating_system(255)
            .write(Vec::new(), Compression::best());
        encoder
            .write_all(self.to_jsonl().as_bytes())
            .expect("writing to a Vec cannot fail");
        encoder.finish().expect("writing to a Vec cannot fail")
    }

    pub fn stats(&self) -> TimelineStats {
        let mut stats = TimelineStats {
            events: self.events.len(),
            duration: self.duration(),
            ..TimelineStats::default()
        };
        for event in &self.events {
            stats.payload_bytes += payload_len(&event.payload);
            match event.kind.as_str() {
                "textDelta" => {
                    stats.text_deltas += 1;
                    stats.text_bytes += event.payload.as_str().map_or(0, str::len);
                }
                "reasoningDelta" => {
                    stats.reasoning_deltas += 1;
                    stats.reasoning_bytes += event.payload.as_str().map_or(0, str::len);
                }
                "richActivity" | "activity" => stats.activities += 1,
                _ => {}
            }
        }
        stats
    }

    /// All `textDelta` payloads joined: the assistant message a client ends
    /// up showing.
    pub fn assistant_text(&self) -> String {
        self.joined("textDelta")
    }

    pub fn reasoning_text(&self) -> String {
        self.joined("reasoningDelta")
    }

    fn joined(&self, kind: &str) -> String {
        self.events
            .iter()
            .filter(|event| event.kind == kind)
            .filter_map(|event| event.payload.as_str())
            .collect()
    }
}

/// Serialized payload size without allocating the serialization.
pub fn payload_len(value: &Value) -> usize {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 += buf.len();
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    let _ = serde_json::to_writer(&mut counter, value);
    counter.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample() -> Timeline {
        Timeline {
            name: "sample".into(),
            description: "two deltas".into(),
            events: vec![
                TimedEvent::new(0, "turnStarted", Value::Null),
                TimedEvent::new(10, "textDelta", json!("Hello")),
                TimedEvent::new(20, "textDelta", json!(", world")),
                TimedEvent::new(
                    30,
                    "turnFinished",
                    json!({"success": true, "summary": null}),
                ),
            ],
        }
    }

    #[test]
    fn plain_and_gzip_round_trip() {
        let timeline = sample();
        assert_eq!(
            Timeline::parse(timeline.to_jsonl().as_bytes()).unwrap(),
            timeline
        );
        assert_eq!(Timeline::parse(&timeline.to_gzip()).unwrap(), timeline);
        assert_eq!(timeline.to_gzip(), timeline.to_gzip());
        assert_eq!(timeline.assistant_text(), "Hello, world");
    }

    #[test]
    fn rejects_backwards_time_unknown_kinds_and_bad_counts() {
        let mut backwards = sample();
        backwards.events[2].at_us = 5;
        assert!(Timeline::parse(backwards.to_jsonl().as_bytes()).is_err());

        let mut unknown = sample();
        unknown.events[1].kind = "notAnEvent".into();
        assert!(Timeline::parse(unknown.to_jsonl().as_bytes()).is_err());

        let mut bad_payload = sample();
        bad_payload.events[3].payload = json!({"success": "yes"});
        assert!(Timeline::parse(bad_payload.to_jsonl().as_bytes()).is_err());

        let truncated = sample().to_jsonl();
        let truncated = truncated.lines().take(3).collect::<Vec<_>>().join("\n");
        assert!(Timeline::parse(truncated.as_bytes()).is_err());
    }

    #[test]
    fn every_builtin_parses() {
        for name in BUILTIN_NAMES {
            let timeline = Timeline::builtin(name).unwrap();
            assert_eq!(timeline.name, name);
            assert_eq!(timeline.events.first().unwrap().kind, "turnStarted");
            assert_eq!(timeline.events.last().unwrap().kind, "turnFinished");
        }
    }
}
