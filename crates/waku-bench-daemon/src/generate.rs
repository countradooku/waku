//! Deterministic generators for the checked-in timelines.
//!
//! `cargo run -p waku-bench-daemon -- generate` rewrites
//! `crates/waku-bench-daemon/timelines/` from these functions. Every input is a
//! fixed seed, so the output is byte-identical on every machine; a test keeps
//! the committed files and the generators in step.

use std::path::Path;

use anyhow::Context as _;
use base64::Engine as _;
use serde_json::{Value, json};
use waku_protocol::model::{
    ActivityFileChange, ActivityFileChangeStatus, ActivityItem, ActivityKind,
};

use crate::content;
use crate::png;
use crate::rng::Rng;
use crate::timeline::{BUILTIN_NAMES, TimedEvent, Timeline};

/// Working directory the generated tool calls pretend to run in.
pub const FIXTURE_CWD: &str = "/bench/project";
/// Context window reported by every timeline's closing usage update.
const CONTEXT_WINDOW: u64 = 272_000;

const MS: u64 = 1_000;
const SECOND: u64 = 1_000_000;

/// Generate one built-in timeline by name.
pub fn generate(name: &str) -> anyhow::Result<Timeline> {
    Ok(match name {
        "prose" => prose(),
        "burst" => burst(),
        "reasoning-fast" => reasoning_fast(),
        "code-wall" => code_wall(),
        "tool-storm" => tool_storm(),
        "images" => images(),
        other => anyhow::bail!("no generator for timeline {other:?}"),
    })
}

pub fn generate_all() -> anyhow::Result<Vec<Timeline>> {
    BUILTIN_NAMES.iter().map(|name| generate(name)).collect()
}

/// Write every timeline as `<dir>/<name>.jsonl.gz`.
pub fn write_all(dir: &Path) -> anyhow::Result<Vec<(Timeline, u64)>> {
    std::fs::create_dir_all(dir).with_context(|| format!("could not create {}", dir.display()))?;
    let mut written = Vec::new();
    for timeline in generate_all()? {
        let path = dir.join(format!("{}.jsonl.gz", timeline.name));
        let bytes = timeline.to_gzip();
        std::fs::write(&path, &bytes)
            .with_context(|| format!("could not write {}", path.display()))?;
        written.push((timeline, bytes.len() as u64));
    }
    Ok(written)
}

/// Accumulates events on a monotonic clock.
struct Builder {
    events: Vec<TimedEvent>,
    now_us: u64,
}

impl Builder {
    fn new() -> Self {
        let mut builder = Self {
            events: Vec::new(),
            now_us: 0,
        };
        builder.push("turnStarted", Value::Null);
        builder.advance(50 * MS);
        builder
    }

    fn push(&mut self, kind: &str, payload: Value) {
        self.events
            .push(TimedEvent::new(self.now_us, kind, payload));
    }

    fn advance(&mut self, micros: u64) {
        self.now_us += micros;
    }

    /// Stream `tokens` as one delta event each at `tokens_per_second`.
    fn stream(&mut self, kind: &str, tokens: &[&str], tokens_per_second: u64) {
        let interval = SECOND / tokens_per_second;
        for token in tokens {
            self.push(kind, Value::String((*token).to_owned()));
            self.advance(interval);
        }
    }

    fn stream_text(&mut self, kind: &str, text: &str, tokens_per_second: u64) -> usize {
        let tokens = content::tokenize(text);
        self.stream(kind, &tokens, tokens_per_second);
        tokens.len()
    }

    fn activity(&mut self, item: &ActivityItem) {
        self.push(
            "richActivity",
            serde_json::to_value(item).expect("activities serialize"),
        );
    }

    fn finish(mut self, name: &str, description: String, context_tokens: u64) -> Timeline {
        self.push(
            "usageUpdated",
            json!({"contextTokens": context_tokens, "contextWindow": CONTEXT_WINDOW}),
        );
        self.advance(20 * MS);
        self.push("turnFinished", json!({"success": true, "summary": null}));
        Timeline {
            name: name.to_owned(),
            description,
            events: self.events,
        }
    }
}

/// Markdown text of at least `tokens` tokens, cut to exactly that many.
fn markdown_tokens(rng: &mut Rng, tokens: usize) -> String {
    let mut text = String::new();
    while content::tokenize(&text).len() < tokens {
        if !text.is_empty() {
            text.push_str("\n\n");
        }
        text.push_str(&content::markdown_answer(rng, 6_000));
    }
    content::tokenize(&text)[..tokens].concat()
}

fn prose() -> Timeline {
    const TOKENS: usize = 20_000;
    const RATE: u64 = 100;
    let mut rng = Rng::new(0x7072_6f73);
    let text = markdown_tokens(&mut rng, TOKENS);
    let mut builder = Builder::new();
    builder.stream_text("textDelta", &text, RATE);
    builder.finish(
        "prose",
        format!("Markdown answer, {TOKENS} tokens streamed one per event at {RATE} tok/s"),
        TOKENS as u64 + 1_200,
    )
}

fn burst() -> Timeline {
    const TOKENS: usize = 20_000;
    const RATE: u64 = 2_000;
    let mut rng = Rng::new(0x6275_7273);
    let text = markdown_tokens(&mut rng, TOKENS);
    let mut builder = Builder::new();
    builder.stream_text("textDelta", &text, RATE);
    builder.finish(
        "burst",
        format!("Markdown answer, {TOKENS} tokens at {RATE} tok/s (10 s)"),
        TOKENS as u64 + 1_200,
    )
}

fn reasoning_fast() -> Timeline {
    const REASONING_TOKENS: usize = 12_000;
    const REASONING_RATE: u64 = 1_500;
    const ANSWER_TOKENS: usize = 2_000;
    const ANSWER_RATE: u64 = 500;
    let mut rng = Rng::new(0x7265_6173);
    let mut reasoning = String::new();
    while content::tokenize(&reasoning).len() < REASONING_TOKENS {
        if !reasoning.is_empty() {
            reasoning.push_str("\n\n");
        }
        reasoning.push_str(&content::reasoning_text(&mut rng, 4_000));
    }
    let reasoning = content::tokenize(&reasoning)[..REASONING_TOKENS].concat();
    let answer = markdown_tokens(&mut rng, ANSWER_TOKENS);
    let mut builder = Builder::new();
    builder.stream_text("reasoningDelta", &reasoning, REASONING_RATE);
    builder.advance(100 * MS);
    builder.stream_text("textDelta", &answer, ANSWER_RATE);
    builder.finish(
        "reasoning-fast",
        format!(
            "{REASONING_TOKENS} reasoning tokens at {REASONING_RATE} tok/s, then a \
             {ANSWER_TOKENS}-token answer at {ANSWER_RATE} tok/s"
        ),
        (REASONING_TOKENS + ANSWER_TOKENS) as u64 + 1_200,
    )
}

fn code_wall() -> Timeline {
    const LINES: usize = 3_000;
    const RATE: u64 = 1_000;
    let mut rng = Rng::new(0x636f_6465);
    let samples = content::CODE_SAMPLES
        .iter()
        .filter(|sample| sample.language == "rust")
        .collect::<Vec<_>>();
    let mut lines = Vec::with_capacity(LINES);
    let mut repeat = 0;
    while lines.len() < LINES {
        let sample = samples[repeat as usize % samples.len()];
        lines.push(format!("// region {repeat}"));
        for line in sample.lines {
            lines.push(content::shift_numbers(line, repeat));
        }
        lines.push(String::new());
        repeat += 1;
    }
    lines.truncate(LINES);
    let intro = format!(
        "Here is the whole module after the change. {}\n\n",
        content::paragraph(&mut rng, Some(2))
    );
    let block = format!("```rust\n{}\n```\n\n", lines.join("\n"));
    let outro = content::paragraph(&mut rng, Some(3));

    let mut builder = Builder::new();
    let mut tokens = builder.stream_text("textDelta", &intro, 200);
    tokens += builder.stream_text("textDelta", &block, RATE);
    tokens += builder.stream_text("textDelta", &outro, 200);
    builder.finish(
        "code-wall",
        format!("One {LINES}-line fenced Rust block streamed at {RATE} tok/s"),
        tokens as u64 + 1_200,
    )
}

/// Grow `make` output until it reaches `target_bytes`.
fn sized(target_bytes: usize, mut make: impl FnMut() -> String) -> String {
    let mut out = String::new();
    while out.len() < target_bytes {
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&make());
    }
    out
}

/// One tool call of the storm: its in-progress and completed forms.
fn storm_activity(
    rng: &mut Rng,
    index: usize,
    output_bytes: usize,
) -> (ActivityItem, ActivityItem) {
    let id = rng.uuid();
    let call_id = rng.call_id();
    let roll = rng.next_f64();
    let (started, completed) = if roll < 0.4 {
        let command = (*rng.pick(content::COMMANDS)).to_owned();
        let arguments = json!({"command": command, "cwd": FIXTURE_CWD}).to_string();
        let output = sized(output_bytes, || content::build_log(rng, 200));
        let started = ActivityItem::new(
            Some(call_id.clone()),
            ActivityKind::Command,
            command.clone(),
            None,
            false,
        )
        .with_tool_name(Some("Bash"))
        .with_arguments(Some(arguments));
        let completed = started.clone().with_output(Some(output));
        (started, completed)
    } else if roll < 0.7 {
        let path = (*rng.pick(content::FILES)).to_owned();
        let mut additions = 0;
        let mut deletions = 0;
        let diff = sized(output_bytes, || {
            let (diff, added, deleted) = content::unified_diff(rng, &path, 8);
            additions += added;
            deletions += deleted;
            diff
        });
        let arguments = json!({"file_path": format!("{FIXTURE_CWD}/{path}")}).to_string();
        let started = ActivityItem::new(
            Some(call_id.clone()),
            ActivityKind::FileChange,
            format!("Edit {path}"),
            None,
            false,
        )
        .with_tool_name(Some("Edit"))
        .with_arguments(Some(arguments));
        let mut completed = started.clone();
        completed.file_changes = vec![ActivityFileChange {
            path: path.clone(),
            additions: Some(additions),
            deletions: Some(deletions),
            status: Some(ActivityFileChangeStatus::Modified),
            diff: Some(diff),
        }];
        (started, completed)
    } else if roll < 0.85 {
        let pattern = (*rng.pick(&[
            "transcript row",
            "fingerprint",
            "ListState",
            "hydrate",
            "measure",
        ]))
        .to_owned();
        let arguments = json!({"pattern": pattern, "path": "src"}).to_string();
        let output = sized(output_bytes, || content::search_results(rng, 100));
        let started = ActivityItem::new(
            Some(call_id.clone()),
            ActivityKind::FileSearch,
            format!("Search {pattern}"),
            None,
            false,
        )
        .with_tool_name(Some("Grep"))
        .with_arguments(Some(arguments));
        let completed = started.clone().with_output(Some(output));
        (started, completed)
    } else {
        let path = (*rng.pick(content::FILES)).to_owned();
        let arguments = json!({"file_path": format!("{FIXTURE_CWD}/{path}")}).to_string();
        let mut line = 0;
        let output = sized(output_bytes, || {
            let sample = rng.pick(content::CODE_SAMPLES);
            sample
                .lines
                .iter()
                .map(|text| {
                    line += 1;
                    format!("{line:>6}\t{text}")
                })
                .collect::<Vec<_>>()
                .join("\n")
        });
        let started = ActivityItem::new(
            Some(call_id.clone()),
            ActivityKind::FileRead,
            format!("Read {path}"),
            None,
            false,
        )
        .with_tool_name(Some("Read"))
        .with_arguments(Some(arguments));
        let completed = started.clone().with_output(Some(output));
        (started, completed)
    };
    let mut started = started;
    let mut completed = completed;
    started.id = id;
    completed.id = id;
    completed.complete = true;
    // Every twentieth call fails, as real storms include red rows.
    completed.failed = index % 20 == 19;
    (started, completed)
}

fn tool_storm() -> Timeline {
    const ACTIVITIES: usize = 300;
    const OUTPUT_BYTES: usize = 50 * 1024;
    let mut rng = Rng::new(0x746f_6f6c);
    let mut builder = Builder::new();
    let intro = "I'll run the full sweep: build, tests, every search and each edit.\n\n";
    let mut tokens = builder.stream_text("textDelta", intro, 200);
    for index in 0..ACTIVITIES {
        let (started, completed) = storm_activity(&mut rng, index, OUTPUT_BYTES);
        builder.activity(&started);
        builder.advance(40 * MS);
        builder.activity(&completed);
        builder.advance(60 * MS);
    }
    let outro = format!(
        "\n\nAll {ACTIVITIES} steps finished. {}",
        content::paragraph(&mut rng, Some(3))
    );
    tokens += builder.stream_text("textDelta", &outro, 200);
    builder.finish(
        "tool-storm",
        format!(
            "{ACTIVITIES} tool calls (commands, edits, searches, reads), each with a \
             ~{} KB output or diff, one every 100 ms",
            OUTPUT_BYTES / 1024
        ),
        tokens as u64 + 90_000,
    )
}

pub const IMAGE_COUNT: u32 = 20;
pub const IMAGE_WIDTH: u32 = 320;
pub const IMAGE_HEIGHT: u32 = 200;

fn images() -> Timeline {
    let mut rng = Rng::new(0x696d_6167);
    let mut builder = Builder::new();
    let mut tokens = builder.stream_text(
        "textDelta",
        "Capturing each state of the transcript so we can compare them side by side.\n\n",
        200,
    );
    for index in 0..IMAGE_COUNT {
        let name = format!("screenshot-{:02}.png", index + 1);
        let path = format!("{FIXTURE_CWD}/temp/{name}");
        let png = png::test_card(index, IMAGE_WIDTH, IMAGE_HEIGHT);
        let url = format!(
            "data:image/png;base64,{}",
            base64::engine::general_purpose::STANDARD.encode(png)
        );
        let id = rng.uuid();
        let call_id = rng.call_id();
        let mut started = ActivityItem::new(
            Some(call_id),
            ActivityKind::Tool,
            format!("View {name}"),
            None,
            false,
        )
        .with_tool_name(Some("view_image"))
        .with_arguments(Some(json!({"path": path}).to_string()));
        started.id = id;
        let mut completed = started.clone().with_image_urls(vec![url]);
        completed.complete = true;
        builder.activity(&started);
        builder.advance(150 * MS);
        builder.activity(&completed);
        builder.advance(100 * MS);
        let caption = format!(
            "Screenshot {} of {IMAGE_COUNT}: {}\n\n",
            index + 1,
            content::paragraph(&mut rng, Some(1))
        );
        tokens += builder.stream_text("textDelta", &caption, 400);
    }
    builder.finish(
        "images",
        format!(
            "{IMAGE_COUNT} tool results each carrying a {IMAGE_WIDTH}x{IMAGE_HEIGHT} PNG data URL, \
             with captions between them"
        ),
        tokens as u64 + 20_000,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_is_deterministic() {
        for name in ["prose", "code-wall", "images"] {
            let first = generate(name).unwrap();
            let second = generate(name).unwrap();
            assert_eq!(first, second, "{name} differs between runs");
            assert_eq!(first.to_gzip(), second.to_gzip());
        }
    }

    #[test]
    fn checked_in_timelines_match_the_generator() {
        for name in BUILTIN_NAMES {
            assert!(
                crate::Timeline::builtin(name).unwrap() == generate(name).unwrap(),
                "timelines/{name}.jsonl.gz is stale; run `cargo run -p waku-bench-daemon -- generate`"
            );
        }
    }

    #[test]
    fn prose_and_burst_have_the_promised_shape() {
        let prose = generate("prose").unwrap().stats();
        assert_eq!(prose.text_deltas, 20_000);
        // 100 tok/s for 20k tokens is 200 s.
        assert!((199..=202).contains(&prose.duration.as_secs()));

        let burst = generate("burst").unwrap().stats();
        assert_eq!(burst.text_deltas, 20_000);
        assert!((9..=11).contains(&burst.duration.as_secs()));
    }

    #[test]
    fn code_wall_is_one_three_thousand_line_fence() {
        let timeline = generate("code-wall").unwrap();
        let text = timeline.assistant_text();
        assert_eq!(text.matches("```").count(), 2);
        let start = text.find("```rust\n").unwrap() + "```rust\n".len();
        let end = start + text[start..].find("\n```").unwrap();
        assert_eq!(text[start..end].lines().count(), 3_000);
    }

    #[test]
    fn tool_storm_has_three_hundred_large_activities() {
        let timeline = generate("tool-storm").unwrap();
        let completed = timeline
            .events
            .iter()
            .filter(|event| event.kind == "richActivity" && event.payload["complete"] == true)
            .collect::<Vec<_>>();
        assert_eq!(completed.len(), 300);
        for event in completed {
            let output = event.payload["output"].as_str().map_or(0, str::len);
            let diff = event.payload["file_changes"][0]["diff"]
                .as_str()
                .map_or(0, str::len);
            assert!(output.max(diff) >= 50 * 1024, "small activity: {event:?}");
        }
    }

    #[test]
    fn images_carry_twenty_png_data_urls() {
        let timeline = generate("images").unwrap();
        let urls = timeline
            .events
            .iter()
            .filter_map(|event| event.payload["image_urls"].as_array())
            .flatten()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>();
        assert_eq!(urls.len(), IMAGE_COUNT as usize);
        assert!(
            urls.iter()
                .all(|url| url.starts_with("data:image/png;base64,iVBORw0KGgo"))
        );
    }
}
