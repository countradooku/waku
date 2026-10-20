//! Fake provider CLIs that stream the bench timelines through each provider's
//! own protocol (issue #8).
//!
//! `fake-codex` speaks `codex app-server` JSON-RPC and `fake-claude` speaks
//! Claude Code's stream-json control protocol. Both the real Waku daemon and
//! T3 Code can drive them with no network. The translation is deliberately
//! the one a real provider would produce for the same transcript, so the
//! visible result matches the bench daemon's direct replay; README.md lists
//! what is approximated.
//!
//! Configuration, highest precedence first:
//! 1. `WAKU_BENCH_TIMELINE`, `WAKU_BENCH_RATE`, `WAKU_BENCH_LOOPS` and
//!    `WAKU_BENCH_LOG` in the environment;
//! 2. `waku-bench.json` beside the invoked executable, which
//!    `scripts/bench/fake-path.sh` writes into the private PATH directory
//!    (`{"timeline": "burst", "rate": "1", "loops": 1, "log": "/tmp/x.jsonl"}`);
//! 3. timeline `prose` at its recorded rate, once.
//!
//! A prompt whose whole text is a built-in timeline name replays that
//! timeline instead.

pub mod claude;
pub mod codex;

use std::collections::HashMap;
use std::io::{BufRead as _, BufReader, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context as _;
use crossbeam_channel::{Receiver, unbounded};
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::replay::{self, Rate};
use crate::timeline::{BUILTIN_NAMES, TimedEvent, Timeline};

pub const CONFIG_FILE: &str = "waku-bench.json";

#[derive(Default, Deserialize)]
struct ConfigFile {
    timeline: Option<String>,
    rate: Option<Value>,
    loops: Option<u32>,
    log: Option<PathBuf>,
}

#[derive(Clone)]
pub struct FakeConfig {
    pub default_timeline: Arc<Timeline>,
    pub rate: Rate,
    pub loops: u32,
    pub log: Option<PathBuf>,
    builtins: Arc<Mutex<HashMap<String, Arc<Timeline>>>>,
}

impl FakeConfig {
    /// Resolve the configuration for an executable invoked as `argv0`.
    pub fn resolve(argv0: &str) -> anyhow::Result<Self> {
        let file = executable_dir(argv0)
            .map(|dir| dir.join(CONFIG_FILE))
            .filter(|path| path.is_file())
            .map(|path| -> anyhow::Result<ConfigFile> {
                let text = std::fs::read_to_string(&path)?;
                serde_json::from_str(&text).with_context(|| format!("invalid {}", path.display()))
            })
            .transpose()?
            .unwrap_or_default();
        let env = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
        let timeline = env("WAKU_BENCH_TIMELINE")
            .or(file.timeline)
            .unwrap_or_else(|| "prose".into());
        let rate = match env("WAKU_BENCH_RATE") {
            Some(rate) => rate.parse()?,
            None => match file.rate {
                Some(Value::String(rate)) => rate.parse()?,
                Some(Value::Number(rate)) => rate.to_string().parse()?,
                Some(other) => anyhow::bail!("invalid rate {other}"),
                None => Rate::default(),
            },
        };
        let loops = match env("WAKU_BENCH_LOOPS") {
            Some(loops) => loops
                .parse()
                .context("WAKU_BENCH_LOOPS must be an integer")?,
            None => file.loops.unwrap_or(1),
        };
        let log = env("WAKU_BENCH_LOG").map(PathBuf::from).or(file.log);
        Ok(Self {
            default_timeline: Arc::new(Timeline::resolve(&timeline)?),
            rate,
            loops: loops.max(1),
            log,
            builtins: Arc::default(),
        })
    }

    pub fn new(default_timeline: Timeline, rate: Rate, loops: u32) -> Self {
        Self {
            default_timeline: Arc::new(default_timeline),
            rate,
            loops: loops.max(1),
            log: None,
            builtins: Arc::default(),
        }
    }

    /// The timeline a prompt replays.
    pub fn timeline_for(&self, prompt: &str) -> Arc<Timeline> {
        let name = prompt.trim();
        if BUILTIN_NAMES.contains(&name) && name != self.default_timeline.name {
            let mut builtins = self.builtins.lock();
            if let Some(timeline) = builtins.get(name) {
                return timeline.clone();
            }
            if let Ok(timeline) = Timeline::builtin(name) {
                let timeline = Arc::new(timeline);
                builtins.insert(name.to_owned(), timeline.clone());
                return timeline;
            }
        }
        self.default_timeline.clone()
    }
}

/// The directory an executable was launched from, before symlinks are
/// resolved: the private PATH directory holds the config, not the target.
fn executable_dir(argv0: &str) -> Option<PathBuf> {
    let path = Path::new(argv0);
    if path.components().count() > 1 {
        return path.parent().map(Path::to_path_buf);
    }
    std::env::split_paths(&std::env::var_os("PATH")?).find(|dir| dir.join(argv0).is_file())
}

/// Newline-delimited JSON on stdout, one complete line per write.
///
/// Both protocols treat stdout as the transport: nothing else may ever be
/// printed there.
#[derive(Clone)]
pub struct Output {
    writer: Arc<Mutex<Box<dyn std::io::Write + Send>>>,
    log: Option<Arc<Mutex<std::fs::File>>>,
}

impl Output {
    pub fn stdout(log: Option<&Path>) -> Self {
        Self::new(Box::new(std::io::stdout()), log)
    }

    pub fn new(writer: Box<dyn std::io::Write + Send>, log: Option<&Path>) -> Self {
        let log = log.and_then(|path| {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .ok()
                .map(|file| Arc::new(Mutex::new(file)))
        });
        Self {
            writer: Arc::new(Mutex::new(writer)),
            log,
        }
    }

    pub fn send(&self, value: &Value) -> anyhow::Result<()> {
        let mut line = serde_json::to_vec(value)?;
        line.push(b'\n');
        let mut writer = self.writer.lock();
        writer.write_all(&line)?;
        writer.flush()?;
        Ok(())
    }

    /// Record a protocol message or a note for the run log, if enabled.
    pub fn log(&self, direction: &str, value: &Value) {
        if let Some(log) = &self.log {
            let entry = json!({"at": now_ms(), "dir": direction, "msg": value});
            let _ = writeln!(log.lock(), "{entry}");
        }
    }
}

/// Lines of stdin parsed as JSON, on a reader thread. Unparsable lines are
/// skipped; the channel closes at end of input.
pub fn stdin_messages() -> Receiver<Value> {
    let (sender, receiver) = unbounded();
    std::thread::Builder::new()
        .name("fake-provider-stdin".into())
        .spawn(move || {
            for line in BufReader::new(std::io::stdin()).lines() {
                let Ok(line) = line else { break };
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                if let Ok(value) = serde_json::from_str::<Value>(line)
                    && sender.send(value).is_err()
                {
                    break;
                }
            }
        })
        .expect("could not start stdin reader");
    receiver
}

/// Serializes turns: a prompt that arrives during a replay waits for it, as a
/// provider queues it.
#[derive(Default)]
pub struct TurnRunner {
    cancel: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl TurnRunner {
    /// Replay `timeline` through `emit`, then call `finish(canceled)`.
    pub fn start(
        &mut self,
        timeline: Arc<Timeline>,
        config: &FakeConfig,
        mut emit: impl FnMut(&TimedEvent) -> anyhow::Result<()> + Send + 'static,
        finish: impl FnOnce(bool) + Send + 'static,
    ) {
        let previous = self.worker.take();
        let cancel = Arc::new(AtomicBool::new(false));
        self.cancel = cancel.clone();
        let rate = config.rate;
        let loops = config.loops;
        self.worker = Some(
            std::thread::Builder::new()
                .name("fake-provider-turn".into())
                .spawn(move || {
                    if let Some(previous) = previous {
                        let _ = previous.join();
                    }
                    let events = replay::expand(&timeline, loops);
                    let stats =
                        replay::play(&timeline.name, &events, rate, &cancel, |event| emit(event));
                    finish(stats.canceled);
                })
                .expect("could not start turn thread"),
        );
    }

    pub fn cancel(&self) {
        self.cancel.store(true, Ordering::Release);
    }

    /// Wait for the running turn, if any.
    pub fn join(&mut self) {
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

pub fn now_secs() -> u64 {
    now_ms() / 1_000
}

/// A value satisfying a simple JSON schema: the fakes answer structured
/// requests such as title generation with it.
pub fn value_for_schema(schema: &Value, label: &str) -> Value {
    match schema.get("type").and_then(Value::as_str) {
        Some("object") => {
            let mut object = serde_json::Map::new();
            if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
                for (key, property) in properties {
                    object.insert(key.clone(), value_for_schema(property, label));
                }
            }
            Value::Object(object)
        }
        Some("array") => json!([]),
        Some("boolean") => json!(false),
        Some("integer" | "number") => json!(0),
        Some("null") => Value::Null,
        _ => json!(label),
    }
}

/// The parts of a timeline activity the fakes translate.
pub struct ActivityView<'a> {
    pub id: String,
    pub kind: &'a str,
    pub title: &'a str,
    pub tool_name: Option<&'a str>,
    pub arguments: Option<Value>,
    pub output: Option<&'a str>,
    pub image_urls: Vec<&'a str>,
    pub file_changes: Vec<(&'a str, &'a str)>,
    pub complete: bool,
    pub failed: bool,
}

impl<'a> ActivityView<'a> {
    /// Read a `richActivity` or `activity` payload.
    pub fn parse(kind: &str, payload: &'a Value) -> Option<Self> {
        let str_at = |key: &str| payload.get(key).and_then(Value::as_str);
        match kind {
            "richActivity" => Some(Self {
                id: str_at("source_id")
                    .or_else(|| str_at("id"))
                    .unwrap_or("item")
                    .to_owned(),
                kind: str_at("kind").unwrap_or("tool"),
                title: str_at("title").unwrap_or("Tool"),
                tool_name: str_at("tool_name"),
                arguments: str_at("arguments").map(|arguments| {
                    serde_json::from_str(arguments)
                        .unwrap_or_else(|_| Value::String(arguments.to_owned()))
                }),
                output: str_at("output"),
                image_urls: payload
                    .get("image_urls")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .collect(),
                file_changes: payload
                    .get("file_changes")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|change| {
                        Some((
                            change.get("path")?.as_str()?,
                            change.get("diff").and_then(Value::as_str).unwrap_or(""),
                        ))
                    })
                    .collect(),
                complete: payload.get("complete").and_then(Value::as_bool) == Some(true),
                failed: payload.get("failed").and_then(Value::as_bool) == Some(true),
            }),
            "activity" => Some(Self {
                id: str_at("id").unwrap_or("item").to_owned(),
                kind: str_at("kind").unwrap_or("tool"),
                title: str_at("title").unwrap_or("Tool"),
                tool_name: None,
                arguments: None,
                output: str_at("detail"),
                image_urls: Vec::new(),
                file_changes: Vec::new(),
                complete: payload.get("complete").and_then(Value::as_bool) == Some(true),
                failed: false,
            }),
            _ => None,
        }
    }

    /// The shell command this activity ran, for command-shaped activities.
    pub fn command(&self) -> String {
        match &self.arguments {
            Some(Value::String(command)) => command.clone(),
            Some(value) => value
                .get("command")
                .and_then(Value::as_str)
                .unwrap_or(self.title)
                .to_owned(),
            None => self.title.to_owned(),
        }
    }

    pub fn argument(&self, key: &str) -> Option<&str> {
        self.arguments.as_ref()?.get(key)?.as_str()
    }
}

/// Split a `data:<mime>;base64,<data>` URL.
pub fn split_data_url(url: &str) -> Option<(&str, &str)> {
    let rest = url.strip_prefix("data:")?;
    let (mime, data) = rest.split_once(";base64,")?;
    Some((mime, data))
}

/// Before/after text recovered from a unified diff, for edit tools that
/// carry strings rather than patches.
pub fn diff_sides(diff: &str) -> (String, String) {
    let mut old = Vec::new();
    let mut new = Vec::new();
    for line in diff.lines() {
        if line.starts_with("diff --git")
            || line.starts_with("--- ")
            || line.starts_with("+++ ")
            || line.starts_with("@@")
        {
            continue;
        }
        if let Some(line) = line.strip_prefix('-') {
            old.push(line);
        } else if let Some(line) = line.strip_prefix('+') {
            new.push(line);
        } else {
            let line = line.strip_prefix(' ').unwrap_or(line);
            old.push(line);
            new.push(line);
        }
    }
    (old.join("\n"), new.join("\n"))
}

/// Claude Code's `structuredPatch`: the hunks of a unified diff with their
/// positions, which is what its edit tools report alongside a result.
pub fn structured_patch(diff: &str) -> Vec<Value> {
    let mut hunks = Vec::new();
    let mut current: Option<(Value, Vec<String>)> = None;
    for line in diff.lines() {
        if let Some(header) = line.strip_prefix("@@ ") {
            if let Some((mut hunk, lines)) = current.take() {
                hunk["lines"] = json!(lines);
                hunks.push(hunk);
            }
            let range = |prefix: char| {
                header
                    .split_whitespace()
                    .find_map(|part| part.strip_prefix(prefix))
                    .map(|range| {
                        let (start, count) = range.split_once(',').unwrap_or((range, "1"));
                        (
                            start.parse::<u64>().unwrap_or(1),
                            count.parse::<u64>().unwrap_or(1),
                        )
                    })
                    .unwrap_or((1, 0))
            };
            let (old_start, old_lines) = range('-');
            let (new_start, new_lines) = range('+');
            current = Some((
                json!({"oldStart": old_start, "oldLines": old_lines,
                       "newStart": new_start, "newLines": new_lines}),
                Vec::new(),
            ));
        } else if let Some((_, lines)) = current.as_mut()
            && !line.starts_with("diff --git")
        {
            lines.push(line.to_owned());
        }
    }
    if let Some((mut hunk, lines)) = current {
        hunk["lines"] = json!(lines);
        hunks.push(hunk);
    }
    hunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structured_patches_keep_hunk_positions() {
        let diff =
            "--- a/x\n+++ b/x\n@@ -3,2 +3,2 @@\n keep\n-old\n+new\n@@ -40,1 +40,2 @@\n a\n+b";
        assert_eq!(
            structured_patch(diff),
            vec![
                json!({"oldStart": 3, "oldLines": 2, "newStart": 3, "newLines": 2,
                       "lines": [" keep", "-old", "+new"]}),
                json!({"oldStart": 40, "oldLines": 1, "newStart": 40, "newLines": 2,
                       "lines": [" a", "+b"]}),
            ]
        );
    }

    #[test]
    fn schema_values_fill_every_property() {
        let schema = json!({
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "needsRefinement": {"type": "boolean"}
            }
        });
        assert_eq!(
            value_for_schema(&schema, "Bench"),
            json!({"title": "Bench", "needsRefinement": false})
        );
    }

    #[test]
    fn diff_sides_rebuild_before_and_after() {
        let diff = "--- a/x\n+++ b/x\n@@ -1,2 +1,2 @@\n keep\n-old\n+new";
        assert_eq!(diff_sides(diff), ("keep\nold".into(), "keep\nnew".into()));
    }

    #[test]
    fn data_urls_split() {
        assert_eq!(
            split_data_url("data:image/png;base64,AAAA"),
            Some(("image/png", "AAAA"))
        );
        assert_eq!(split_data_url("https://x"), None);
    }
}
