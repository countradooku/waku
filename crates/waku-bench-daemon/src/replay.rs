//! Paced replay of a timeline, shared by the bench daemon and the fake CLIs.

use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context as _, bail};
use serde_json::Value;
use uuid::Uuid;

use crate::timeline::{TimedEvent, Timeline, payload_len};

/// How fast a timeline plays relative to its own timestamps.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Rate {
    /// Multiply playback speed by this factor (`1.0` = as recorded).
    Factor(f64),
    /// Ignore timestamps and emit as fast as the transport accepts.
    Max,
}

impl Default for Rate {
    fn default() -> Self {
        Self::Factor(1.0)
    }
}

impl FromStr for Rate {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> anyhow::Result<Self> {
        let value = value.trim();
        if value.eq_ignore_ascii_case("max") {
            return Ok(Self::Max);
        }
        let factor: f64 = value
            .trim_end_matches(['x', 'X'])
            .parse()
            .with_context(|| format!("rate {value:?} is not a number or \"max\""))?;
        if !factor.is_finite() || factor < 0.0 {
            bail!("rate must be a positive factor or \"max\"");
        }
        Ok(if factor == 0.0 {
            Self::Max
        } else {
            Self::Factor(factor)
        })
    }
}

impl fmt::Display for Rate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Factor(factor) => write!(f, "{factor}x"),
            Self::Max => f.write_str("max"),
        }
    }
}

/// In `max` mode, pause briefly after this many events. The daemon's socket
/// writer drains its queue between 25 ms read polls, and a subscriber whose
/// queue overflows is disconnected; this keeps an unpaced replay well under
/// that bound while still being far faster than any real provider.
const MAX_RATE_BATCH: usize = 1_024;
const MAX_RATE_PAUSE: Duration = Duration::from_millis(5);

/// What one replay did, for the exit summary.
#[derive(Clone, Debug, Default)]
pub struct ReplayStats {
    pub timeline: String,
    pub events: u64,
    pub payload_bytes: u64,
    pub text_deltas: u64,
    pub reasoning_deltas: u64,
    pub activities: u64,
    /// Wall time the timeline asked for at this rate.
    pub scheduled: Duration,
    pub elapsed: Duration,
    /// Worst delay between an event's scheduled time and its emission.
    pub max_lag: Duration,
    pub canceled: bool,
}

impl ReplayStats {
    pub fn events_per_second(&self) -> f64 {
        self.events as f64 / self.elapsed.as_secs_f64().max(1e-9)
    }

    /// One line of a fake's stats file (`waku-bench.json` `stats`), which the
    /// bench daemon reads when the run that replayed it ends.
    pub fn to_json(&self) -> Value {
        serde_json::json!({
            "timeline": self.timeline,
            "events": self.events,
            "payloadBytes": self.payload_bytes,
            "textDeltas": self.text_deltas,
            "reasoningDeltas": self.reasoning_deltas,
            "activities": self.activities,
            "scheduledUs": self.scheduled.as_micros() as u64,
            "elapsedUs": self.elapsed.as_micros() as u64,
            "maxLagUs": self.max_lag.as_micros() as u64,
            "canceled": self.canceled,
        })
    }

    pub fn from_json(value: &Value) -> Option<Self> {
        let number = |key: &str| value.get(key).and_then(Value::as_u64);
        let micros = |key: &str| number(key).map(Duration::from_micros);
        Some(Self {
            timeline: value.get("timeline")?.as_str()?.to_owned(),
            events: number("events")?,
            payload_bytes: number("payloadBytes").unwrap_or_default(),
            text_deltas: number("textDeltas").unwrap_or_default(),
            reasoning_deltas: number("reasoningDeltas").unwrap_or_default(),
            activities: number("activities").unwrap_or_default(),
            scheduled: micros("scheduledUs").unwrap_or_default(),
            elapsed: micros("elapsedUs")?,
            max_lag: micros("maxLagUs").unwrap_or_default(),
            canceled: value.get("canceled").and_then(Value::as_bool)?,
        })
    }
}

/// The events of `timeline` expanded for `loops` repetitions.
///
/// The leading `turnStarted` and the trailing usage/finish events play once;
/// everything between them repeats back to back inside the same turn. Repeats
/// get fresh activity ids so a client renders new rows instead of updating
/// the first loop's.
pub fn expand(timeline: &Timeline, loops: u32) -> Vec<TimedEvent> {
    let loops = loops.max(1);
    let events = &timeline.events;
    let prologue = events
        .iter()
        .take_while(|event| event.kind == "turnStarted")
        .count();
    let epilogue = events[prologue..]
        .iter()
        .rev()
        .take_while(|event| matches!(event.kind.as_str(), "turnFinished" | "usageUpdated"))
        .count();
    let body = &events[prologue..events.len() - epilogue];
    if loops == 1 || body.is_empty() {
        return events.clone();
    }
    let body_start = body.first().map_or(0, |event| event.at_us);
    let body_end = body.last().map_or(0, |event| event.at_us);
    // The gap a loop leaves before the next one starts.
    let period = body_end - body_start + 100_000;
    let mut out = Vec::with_capacity(prologue + body.len() * loops as usize + epilogue);
    out.extend_from_slice(&events[..prologue]);
    for repeat in 0..loops {
        let offset = u64::from(repeat) * period;
        for event in body {
            let mut event = event.clone();
            event.at_us += offset;
            if repeat > 0 {
                remap_ids(&mut event, repeat);
            }
            out.push(event);
        }
    }
    let tail_offset = u64::from(loops - 1) * period;
    for event in &events[events.len() - epilogue..] {
        let mut event = event.clone();
        event.at_us += tail_offset;
        out.push(event);
    }
    out
}

fn remap_ids(event: &mut TimedEvent, repeat: u32) {
    match event.kind.as_str() {
        "richActivity" => {
            if let Some(id) = event
                .payload
                .get("id")
                .and_then(Value::as_str)
                .and_then(|id| Uuid::parse_str(id).ok())
            {
                let remapped = Uuid::from_u128(id.as_u128() ^ u128::from(repeat));
                event.payload["id"] = Value::String(remapped.to_string());
            }
            if let Some(source) = event.payload.get("source_id").and_then(Value::as_str) {
                event.payload["source_id"] = Value::String(format!("{source}-loop{repeat}"));
            }
        }
        "activity" => {
            if let Some(id) = event.payload.get("id").and_then(Value::as_str) {
                event.payload["id"] = Value::String(format!("{id}-loop{repeat}"));
            }
        }
        _ => {}
    }
}

/// Emit `events` on schedule, stopping early when `cancel` is set.
///
/// `emit` returning an error stops the replay too (the consumer went away).
pub fn play(
    name: &str,
    events: &[TimedEvent],
    rate: Rate,
    cancel: &AtomicBool,
    mut emit: impl FnMut(&TimedEvent) -> anyhow::Result<()>,
) -> ReplayStats {
    let start = Instant::now();
    let mut stats = ReplayStats {
        timeline: name.to_owned(),
        scheduled: match rate {
            Rate::Factor(factor) => Duration::from_micros(
                (events.last().map_or(0, |event| event.at_us) as f64 / factor) as u64,
            ),
            Rate::Max => Duration::ZERO,
        },
        ..ReplayStats::default()
    };
    for (index, event) in events.iter().enumerate() {
        if cancel.load(Ordering::Acquire) {
            stats.canceled = true;
            break;
        }
        match rate {
            Rate::Factor(factor) => {
                let target = start + Duration::from_micros((event.at_us as f64 / factor) as u64);
                let now = Instant::now();
                if target > now {
                    // Sleep in short slices so a cancel lands promptly even
                    // inside a long pause.
                    let mut remaining = target - now;
                    while remaining > Duration::ZERO {
                        if cancel.load(Ordering::Acquire) {
                            stats.canceled = true;
                            break;
                        }
                        let slice = remaining.min(Duration::from_millis(50));
                        std::thread::sleep(slice);
                        remaining = target.saturating_duration_since(Instant::now());
                    }
                    if stats.canceled {
                        break;
                    }
                } else {
                    stats.max_lag = stats.max_lag.max(now - target);
                }
            }
            Rate::Max => {
                if index > 0 && index % MAX_RATE_BATCH == 0 {
                    std::thread::sleep(MAX_RATE_PAUSE);
                }
            }
        }
        if emit(event).is_err() {
            stats.canceled = true;
            break;
        }
        stats.events += 1;
        stats.payload_bytes += payload_len(&event.payload) as u64;
        match event.kind.as_str() {
            "textDelta" => stats.text_deltas += 1,
            "reasoningDelta" => stats.reasoning_deltas += 1,
            "richActivity" | "activity" => stats.activities += 1,
            _ => {}
        }
    }
    stats.elapsed = start.elapsed();
    stats
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn timeline() -> Timeline {
        Timeline {
            name: "t".into(),
            description: String::new(),
            events: vec![
                TimedEvent::new(0, "turnStarted", Value::Null),
                TimedEvent::new(1_000, "textDelta", json!("a")),
                TimedEvent::new(
                    2_000,
                    "richActivity",
                    json!({"id": Uuid::from_u128(8).to_string(), "source_id": "call_1"}),
                ),
                TimedEvent::new(
                    3_000,
                    "usageUpdated",
                    json!({"contextTokens": 1, "contextWindow": 2}),
                ),
                TimedEvent::new(
                    3_000,
                    "turnFinished",
                    json!({"success": true, "summary": null}),
                ),
            ],
        }
    }

    #[test]
    fn stats_round_trip_through_json() {
        let stats = play(
            "t",
            &timeline().events,
            Rate::Max,
            &AtomicBool::new(false),
            |_| Ok(()),
        );
        let back = ReplayStats::from_json(&stats.to_json()).unwrap();
        assert_eq!(back.timeline, "t");
        assert_eq!(back.events, stats.events);
        assert_eq!(back.text_deltas, 1);
        assert_eq!(back.elapsed.as_micros(), stats.elapsed.as_micros());
        assert!(!back.canceled);
    }

    #[test]
    fn rates_parse() {
        assert_eq!("max".parse::<Rate>().unwrap(), Rate::Max);
        assert_eq!("0".parse::<Rate>().unwrap(), Rate::Max);
        assert_eq!("2x".parse::<Rate>().unwrap(), Rate::Factor(2.0));
        assert!("-1".parse::<Rate>().is_err());
        assert!("fast".parse::<Rate>().is_err());
    }

    #[test]
    fn loops_repeat_the_body_once_per_loop_inside_one_turn() {
        let expanded = expand(&timeline(), 3);
        let kinds = expanded
            .iter()
            .map(|event| event.kind.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            [
                "turnStarted",
                "textDelta",
                "richActivity",
                "textDelta",
                "richActivity",
                "textDelta",
                "richActivity",
                "usageUpdated",
                "turnFinished"
            ]
        );
        let ids = expanded
            .iter()
            .filter(|event| event.kind == "richActivity")
            .map(|event| event.payload["id"].as_str().unwrap().to_owned())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(ids.len(), 3, "each loop needs its own activity rows");
        assert!(
            expanded
                .windows(2)
                .all(|pair| pair[0].at_us <= pair[1].at_us)
        );
        assert_eq!(expand(&timeline(), 1), timeline().events);
    }

    #[test]
    fn play_emits_in_order_and_honors_cancel() {
        let events = timeline().events;
        let mut seen = Vec::new();
        let stats = play("t", &events, Rate::Max, &AtomicBool::new(false), |event| {
            seen.push(event.kind.clone());
            Ok(())
        });
        assert_eq!(seen.len(), events.len());
        assert_eq!(stats.events, events.len() as u64);
        assert_eq!(stats.text_deltas, 1);
        assert!(!stats.canceled);

        let canceled = play("t", &events, Rate::Max, &AtomicBool::new(true), |_| Ok(()));
        assert!(canceled.canceled);
        assert_eq!(canceled.events, 0);
    }

    #[test]
    fn factor_rate_paces_events() {
        let events = timeline().events;
        let stats = play(
            "t",
            &events,
            Rate::Factor(1.0),
            &AtomicBool::new(false),
            |_| Ok(()),
        );
        assert!(stats.elapsed >= Duration::from_millis(3));
    }
}
