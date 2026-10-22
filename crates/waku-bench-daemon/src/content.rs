//! Realistic transcript material for timelines and fixture sessions.
//!
//! The banks and generators were ported from the former
//! `scripts/seed-mock-sessions.ts`, so the bench exercises the same shapes as
//! its seeded corpora: Markdown with
//! tables, fenced code, nested lists and quotes; build logs; unified diffs;
//! search hits. What is measured is parsing, highlighting, shaping and
//! wrapping, so the text has to look like what an agent actually produces.

use crate::rng::Rng;

pub const PROMPTS: &[&str] = &[
    "the transcript stutters when I scroll fast through a long session, can you profile it",
    "make the sidebar remember which project was selected across restarts",
    "why does hydrating a session take so long on startup?",
    "add a keyboard shortcut to collapse every turn in the transcript",
    "the composer loses focus after a turn finishes, fix that",
    "reasoning blocks should keep their scroll position when a turn is expanded",
    "audit the render path for anything touching the filesystem",
    "tool output over a few thousand lines should virtualize instead of laying out everything",
    "the diff view wraps long lines in a way that breaks alignment, take a look",
    "can we cache the markdown parse between frames instead of reparsing",
    "session switching drops a frame or two, find out where the time goes",
    "make the activity disclosure animate open without janking the list",
    "the checkpoint row runs git on the ui thread somewhere, track it down",
    "add a setting for the transcript font size and make it live-update",
    "long code blocks inside assistant messages should get a copy button",
    "measure how much time we spend in text shaping for a 2000 message session",
    "the turn fold summary should say how many tools ran, not just the duration",
    "scrolling to the bottom on a new message should be instant, not animated",
    "check whether we re-measure every row when only the last one changed",
    "images in tool output are decoded on the ui thread, move that off",
];

pub const HEADINGS: &[&str] = &[
    "What I found",
    "Root cause",
    "The fix",
    "Measurements",
    "Why it was slow",
    "Trade-offs",
    "What changed",
    "Follow-ups",
    "How it works now",
    "Verification",
];

pub const SENTENCES: &[&str] = &[
    "The row builder was rebuilding whole-session state for every visible item, so the cost scaled with history rather than with what is on screen.",
    "Each frame walked the full transcript to compute anchors, which is fine at fifty messages and ruinous at five thousand.",
    "I hoisted that into a cache refreshed once per frame and keyed it on the fingerprint the fold already computes.",
    "The measurement pass shapes text twice: once for the height query and once for paint, and neither result was retained.",
    "Nothing here needs the filesystem, but the checkpoint affordance was reaching for `git` before the background prefetch had landed.",
    "Because the list is virtualized, only the visible window matters — the trap is any per-frame work that iterates the whole collection anyway.",
    "Deserializing the detail blob dominates session switching once a transcript passes a few megabytes.",
    "I kept the allocation out of the hot path by reusing the buffer between rows rather than collecting into a fresh vector.",
    "The generation counter guards against a superseded background pass overwriting newer state when two switches land close together.",
    "Wrapping is the expensive part: every soft break costs a shaping run, and code blocks defeat the fast path entirely.",
    "Syntax highlighting now runs once per code block and the result is stored beside the parsed block rather than recomputed on scroll.",
    "The regression only shows on a high-refresh display, where the frame budget is under seven milliseconds.",
    "I left the synchronous path in place for one-shot actions, where freshness is worth more than latency.",
    "Interleaving the blocks at message boundaries keeps ordering stable even when a provider emits events out of order.",
    "That turned a linear scan per row into a single hash map lookup, which is what made the difference under a long transcript.",
];

pub const INLINE_SNIPPETS: &[&str] = &[
    "`transcript_rows_fingerprint`",
    "`folded_transcript_row_kinds`",
    "`ListState::splice`",
    "`cx.background_executor().spawn`",
    "`AgentSession::hydrate`",
    "`session_details.data`",
    "`render` → `measure` → `paint`",
    "`Window::request_animation_frame`",
];

pub const FILES: &[&str] = &[
    "src/app/transcript.rs",
    "src/app/transcript_view.rs",
    "src/app/components.rs",
    "src/app/streaming.rs",
    "src/app/sessions.rs",
    "src/md/render.rs",
    "src/md/parser.rs",
    "src/md/highlight.rs",
    "src/persistence.rs",
    "src/model.rs",
    "src/driver/codex.rs",
    "src/ui/scrollbar.rs",
];

pub struct CodeSample {
    pub language: &'static str,
    pub lines: &'static [&'static str],
}

pub const CODE_SAMPLES: &[CodeSample] = &[
    CodeSample {
        language: "rust",
        lines: &[
            "pub(super) fn visible_rows(&self, window: &Window) -> Range<usize> {",
            "    let viewport = window.viewport_size().height;",
            "    let first = self.offsets.partition_point(|offset| *offset < self.scroll_top);",
            "    let last = self.offsets.partition_point(|offset| *offset < self.scroll_top + viewport);",
            "    first..last.min(self.rows.len())",
            "}",
            "",
            "impl TranscriptView {",
            "    fn remeasure(&mut self, kind: TranscriptRowKind, cx: &mut Context<Self>) {",
            "        let Some(index) = self.rows.iter().position(|row| *row == kind) else {",
            "            return;",
            "        };",
            "        self.list.splice(index..index + 1, 1);",
            "        cx.notify();",
            "    }",
            "}",
        ],
    },
    CodeSample {
        language: "rust",
        lines: &[
            "let cached = self.row_cache.borrow();",
            "if cached.fingerprint == fingerprint {",
            "    return cached.rows.clone();",
            "}",
            "drop(cached);",
            "",
            "let rows = folded_transcript_row_kinds(session, &self.expanded_turns);",
            "*self.row_cache.borrow_mut() = RowCache {",
            "    fingerprint,",
            "    rows: rows.clone(),",
            "};",
            "rows",
        ],
    },
    CodeSample {
        language: "typescript",
        lines: &[
            "const rows = useMemo(() => foldTranscript(session, expanded), [session, expanded]);",
            "",
            "useLayoutEffect(() => {",
            "  if (!listRef.current) return;",
            "  listRef.current.scrollToIndex(rows.length - 1, { align: 'end' });",
            "}, [rows.length]);",
            "",
            "export function foldTranscript(session: Session, expanded: Set<string>) {",
            "  const anchors = session.blocks.map((block) => block.afterMessage);",
            "  return interleave(session.messages.length, anchors).filter((row) =>",
            "    expanded.has(row.turnId) || !row.hidden,",
            "  );",
            "}",
        ],
    },
    CodeSample {
        language: "bash",
        lines: &[
            "cargo build --profile dev 2>&1 | tail -20",
            "hyperfine --warmup 3 'target/debug/waku --bench transcript'",
            "rg -n 'fn render' src/app | wc -l",
            "sqlite3 temp/app.db 'EXPLAIN QUERY PLAN SELECT * FROM sessions ORDER BY updated_at'",
        ],
    },
    CodeSample {
        language: "json",
        lines: &[
            "{",
            "  \"session\": \"long-history\",",
            "  \"messages\": 4021,",
            "  \"blocks\": 6144,",
            "  \"frame_ms\": { \"p50\": 4.1, \"p95\": 11.7, \"max\": 38.2 },",
            "  \"hydrate_ms\": 182.4",
            "}",
        ],
    },
    CodeSample {
        language: "diff",
        lines: &[
            "-    let rows = folded_transcript_row_kinds(session, &self.expanded_turns);",
            "+    let fingerprint = transcript_rows_fingerprint(session, &self.expanded_turns);",
            "+    let rows = self.cached_rows(session, fingerprint);",
            "     for row in &rows {",
            "-        self.measure(row, window, cx);",
            "+        self.measure_if_dirty(row, window, cx);",
            "     }",
        ],
    },
];

pub const COMMANDS: &[&str] = &[
    "cargo build",
    "cargo test transcript",
    "cargo clippy --all-targets",
    "rg -n 'transcript_rows_fingerprint' src",
    "git diff --stat",
    "sqlite3 temp/app.db 'select count(*) from messages'",
    "wc -l src/app/*.rs",
    "bun ./scripts/dev.ts",
];

pub fn paragraph(rng: &mut Rng, sentences: Option<u64>) -> String {
    let count = sentences.unwrap_or_else(|| rng.int(2, 5));
    let mut parts = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let mut sentence = (*rng.pick(SENTENCES)).to_owned();
        if rng.chance(0.3) && sentence.ends_with('.') {
            sentence.pop();
            sentence.push_str(&format!(" — see {}.", rng.pick(INLINE_SNIPPETS)));
        }
        parts.push(sentence);
    }
    parts.join(" ")
}

/// Shift every standalone number in `line` by `delta`, so repeated code never
/// presents identical input to the highlighter or the wrapper.
pub fn shift_numbers(line: &str, delta: u64) -> String {
    if delta == 0 {
        return line.to_owned();
    }
    let bytes = line.as_bytes();
    let mut out = String::with_capacity(line.len() + 4);
    let mut index = 0;
    while index < bytes.len() {
        let is_word = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_';
        if bytes[index].is_ascii_digit() && (index == 0 || !is_word(bytes[index - 1])) {
            let start = index;
            while index < bytes.len() && bytes[index].is_ascii_digit() {
                index += 1;
            }
            let ends_word = index == bytes.len() || !is_word(bytes[index]);
            let digits = &line[start..index];
            match digits.parse::<u64>() {
                Ok(value) if ends_word => out.push_str(&(value + delta).to_string()),
                _ => out.push_str(digits),
            }
            continue;
        }
        let character = line[index..]
            .chars()
            .next()
            .expect("index is a char boundary");
        out.push(character);
        index += character.len_utf8();
    }
    out
}

pub fn code_fence(rng: &mut Rng, min_lines: usize) -> String {
    let sample = rng.pick(CODE_SAMPLES);
    let mut lines = Vec::new();
    let mut repeat = 0;
    while lines.len() < min_lines {
        for line in sample.lines {
            lines.push(shift_numbers(line, repeat));
        }
        repeat += 1;
        if repeat > 40 {
            break;
        }
    }
    let mut out = format!("```{}\n", sample.language);
    out.push_str(&lines.join("\n"));
    out.push_str("\n```");
    out
}

pub fn table(rng: &mut Rng, rows: Option<u64>) -> String {
    let rows = rows.unwrap_or_else(|| rng.int(3, 9));
    let mut lines = vec![
        "| Path | Rows | p50 (ms) | p95 (ms) |".to_owned(),
        "| --- | ---: | ---: | ---: |".to_owned(),
    ];
    for _ in 0..rows {
        let path = rng.pick(FILES);
        let count = rng.int(40, 9000);
        let p50 = rng.next_f64() * 6.0;
        let p95 = rng.next_f64() * 24.0;
        lines.push(format!("| `{path}` | {count} | {p50:.2} | {p95:.2} |"));
    }
    lines.join("\n")
}

fn truncate_chars(text: &str, limit: usize) -> &str {
    match text.char_indices().nth(limit) {
        Some((index, _)) => &text[..index],
        None => text,
    }
}

pub fn bullet_list(rng: &mut Rng) -> String {
    let items = rng.int(3, 7);
    (0..items)
        .map(|_| {
            let nested = if rng.chance(0.35) {
                format!("\n  - {}", truncate_chars(rng.pick(SENTENCES), 90))
            } else {
                String::new()
            };
            format!("- {}{nested}", paragraph(rng, Some(1)))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn task_list(rng: &mut Rng, items: Option<u64>) -> String {
    let items = items.unwrap_or_else(|| rng.int(3, 6));
    (0..items)
        .map(|_| {
            let done = if rng.chance(0.6) { "x" } else { " " };
            let text = paragraph(rng, Some(1));
            format!("- [{done}] {}", truncate_chars(&text, 110))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn numbered_list(rng: &mut Rng) -> String {
    let items = rng.int(3, 6);
    (0..items)
        .map(|index| {
            let heading = rng.pick(HEADINGS);
            format!("{}. **{heading}** — {}", index + 1, paragraph(rng, Some(1)))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// A Markdown answer of roughly `target_bytes`, mixing every construct the
/// renderer supports.
pub fn markdown_answer(rng: &mut Rng, target_bytes: usize) -> String {
    let mut parts = vec![format!("### {}", rng.pick(HEADINGS)), paragraph(rng, None)];
    let mut size = parts.iter().map(|part| part.len() + 2).sum::<usize>();
    while size < target_bytes {
        let roll = rng.next_f64();
        let part = if roll < 0.3 {
            paragraph(rng, None)
        } else if roll < 0.5 {
            let lines = rng.int(6, 40) as usize;
            code_fence(rng, lines)
        } else if roll < 0.63 {
            bullet_list(rng)
        } else if roll < 0.72 {
            numbered_list(rng)
        } else if roll < 0.79 {
            task_list(rng, None)
        } else if roll < 0.86 {
            table(rng, None)
        } else if roll < 0.93 {
            format!("> {}", paragraph(rng, Some(2)))
        } else {
            format!("#### {}", rng.pick(HEADINGS))
        };
        size += part.len() + 2;
        parts.push(part);
    }
    parts.join("\n\n")
}

pub fn build_log(rng: &mut Rng, lines: u64) -> String {
    let mut out = Vec::new();
    for index in 0..lines {
        let roll = rng.next_f64();
        if roll < 0.08 {
            out.push(format!(
                "warning: unused variable: `{}`",
                rng.pick(&["index", "cx", "window", "session"])
            ));
            let file = rng.pick(FILES);
            let line = rng.int(20, 1800);
            let column = rng.int(4, 60);
            out.push(format!("  --> {file}:{line}:{column}"));
            out.push("   |".into());
            let number = rng.int(20, 1800);
            let name = rng.pick(&["rows", "anchors", "cache"]);
            out.push(format!(
                "{number:>4} |     let {name} = session.transcript_blocks.len();"
            ));
            out.push(
                "   |         ^^^^ help: if this is intentional, prefix it with an underscore"
                    .into(),
            );
        } else if roll < 0.2 {
            let krate = rng.pick(&["gpui", "waku", "rusqlite", "pulldown-cmark", "smol"]);
            let (major, minor, patch) = (rng.int(0, 3), rng.int(0, 40), rng.int(0, 9));
            out.push(format!("   Compiling {krate} v{major}.{minor}.{patch}"));
        } else {
            let module = rng.pick(&["app::tests", "md::parser::tests", "persistence::tests"]);
            let test = rng.pick(&[
                "folds_settled_turns",
                "keeps_scroll_anchor",
                "hydrates_one_session",
                "writes_only_dirty_rows",
            ]);
            out.push(format!("test {module}::{test}_{index} ... ok"));
        }
    }
    out.push(String::new());
    let seconds = rng.next_f64() * 30.0;
    out.push(format!(
        "test result: ok. {lines} passed; 0 failed; 0 ignored; finished in {seconds:.2}s"
    ));
    out.join("\n")
}

/// A unified diff for `path` with `hunks` hunks.
pub fn unified_diff(rng: &mut Rng, path: &str, hunks: u64) -> (String, u64, u64) {
    let mut out = vec![
        format!("diff --git a/{path} b/{path}"),
        format!("--- a/{path}"),
        format!("+++ b/{path}"),
    ];
    let (mut additions, mut deletions) = (0, 0);
    for hunk in 0..hunks {
        let start = 20 + hunk * 40 + rng.int(0, 30);
        let sample = rng.pick(CODE_SAMPLES);
        let mut body = Vec::with_capacity(sample.lines.len());
        let (mut old, mut new) = (0, 0);
        for line in sample.lines {
            let line = shift_numbers(line, hunk);
            let roll = rng.next_f64();
            if roll < 0.25 {
                body.push(format!("-{line}"));
                old += 1;
                deletions += 1;
            } else if roll < 0.6 {
                body.push(format!("+{line}"));
                new += 1;
                additions += 1;
            } else {
                body.push(format!(" {line}"));
                old += 1;
                new += 1;
            }
        }
        out.push(format!("@@ -{start},{old} +{start},{new} @@"));
        out.extend(body);
    }
    (out.join("\n"), additions, deletions)
}

pub fn search_results(rng: &mut Rng, hits: u64) -> String {
    let mut out = Vec::with_capacity(hits as usize);
    for _ in 0..hits {
        let sample = rng.pick(CODE_SAMPLES);
        let file = rng.pick(FILES);
        let line = rng.int(10, 1900);
        out.push(format!("{file}:{line}:{}", rng.pick(sample.lines).trim()));
    }
    out.join("\n")
}

/// Reasoning prose with bold section headers, the way Codex summaries read.
pub fn reasoning_text(rng: &mut Rng, target_bytes: usize) -> String {
    let mut parts = Vec::new();
    let mut size = 0;
    while size < target_bytes {
        let part = if rng.chance(0.2) {
            format!("**{}**", rng.pick(HEADINGS))
        } else {
            let sentences = rng.int(2, 6);
            paragraph(rng, Some(sentences))
        };
        size += part.len() + 2;
        parts.push(part);
    }
    parts.join("\n\n")
}

/// Split text into model-sized tokens of about four bytes each.
///
/// Real tokenizers emit a word together with its leading space and break long
/// identifiers into pieces. This keeps every piece's text intact, so the
/// concatenation of the tokens is exactly the input.
pub fn tokenize(text: &str) -> Vec<&str> {
    /// Visible characters per token before a long word is split.
    const WORD_PIECE: usize = 4;
    let mut tokens = Vec::new();
    let mut start = 0;
    // Visible (non-whitespace) characters in the token being built.
    let mut visible = 0;
    for (index, character) in text.char_indices() {
        if character == '\n' {
            // A newline is its own token, as in real streams.
            if index > start {
                tokens.push(&text[start..index]);
            }
            tokens.push(&text[index..index + 1]);
            start = index + 1;
            visible = 0;
            continue;
        }
        let whitespace = character.is_whitespace();
        // Whitespace opens the next token (words carry their leading space);
        // a long run of visible characters is split into word pieces.
        if (whitespace && visible > 0) || (!whitespace && visible >= WORD_PIECE) {
            tokens.push(&text[start..index]);
            start = index;
            visible = 0;
        }
        if !whitespace {
            visible += 1;
        }
    }
    if start < text.len() {
        tokens.push(&text[start..]);
    }
    tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_round_trips_and_averages_a_few_bytes() {
        let mut rng = Rng::new(1);
        let text = markdown_answer(&mut rng, 20_000);
        let tokens = tokenize(&text);
        assert_eq!(tokens.concat(), text);
        let average = text.len() as f64 / tokens.len() as f64;
        assert!((2.5..=6.0).contains(&average), "average token {average}");
        assert!(tokens.iter().all(|token| !token.is_empty()));
    }

    #[test]
    fn shift_numbers_only_touches_standalone_numbers() {
        assert_eq!(shift_numbers("index + 1, x2 = 10", 3), "index + 4, x2 = 13");
        assert_eq!(shift_numbers("no digits", 9), "no digits");
    }

    #[test]
    fn diffs_count_their_lines() {
        let mut rng = Rng::new(3);
        let (diff, additions, deletions) = unified_diff(&mut rng, "src/a.rs", 3);
        assert_eq!(
            diff.lines()
                .filter(|line| line.starts_with('+') && !line.starts_with("+++"))
                .count() as u64,
            additions
        );
        assert_eq!(
            diff.lines()
                .filter(|line| line.starts_with('-') && !line.starts_with("---"))
                .count() as u64,
            deletions
        );
    }
}
