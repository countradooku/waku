//! Deterministic inputs for `benches/desktop.rs` and `tests/md_stream_linear.rs`.
//!
//! Everything is generated from a fixed seed, so a benchmark measures the same
//! bytes on every machine and every run, and nothing is read from disk.

#![allow(dead_code)]

/// xorshift64*: tiny, dependency-free and stable across platforms.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform in `low..=high`.
    pub fn range(&mut self, low: usize, high: usize) -> usize {
        low + (self.next() % (high - low + 1) as u64) as usize
    }

    pub fn pick<'a>(&mut self, items: &[&'a str]) -> &'a str {
        items[self.range(0, items.len() - 1)]
    }
}

const WORDS: &[&str] = &[
    "the", "parser", "streams", "each", "delta", "into", "a", "settled", "prefix", "and",
    "reparses", "only", "tail", "so", "frame", "stays", "within", "budget", "while", "agent",
    "writes", "code", "review", "diff", "terminal", "session", "daemon", "event", "cache", "bound",
    "render", "layout", "shaping", "text", "run", "block", "list", "table", "heading", "quote",
    "function", "returns", "value", "error", "handles", "commit", "branch", "worktree", "for",
    "with", "without", "before", "after", "because", "every", "visible", "row", "update",
];

const RUST_LINES: &[&str] = &[
    "    let value = compute(&input, 42)?;",
    "    // Settle the prefix before the next delta arrives.",
    "    if buffer.len() > MAX_BYTES {",
    "        return Err(anyhow::anyhow!(\"buffer overflow: {}\", buffer.len()));",
    "    }",
    "    for (index, line) in source.lines().enumerate() {",
    "        tokens.push(Token { range: 0..line.len(), class: TokenClass::Keyword });",
    "    match state {",
    "        State::Idle => {}",
    "        State::Streaming { offset } => cursor = offset + 0x1f,",
    "    }",
    "    let name: &'static str = \"waku\";",
    "    /* block comment spanning",
    "       two lines */",
    "    self.cache.retain(|key, _| *key < boundary);",
    "    Ok(())",
];

/// Streamed assistant Markdown, split the way a provider emits it: roughly one
/// word (with its leading space) or line break per token.
///
/// Blocks stay small and varied, as in a real response — headings, styled
/// paragraphs, lists, short fenced code, tables and quotes — so total parse
/// work should grow linearly with the token count.
pub fn markdown_stream_tokens(count: usize) -> Vec<String> {
    let mut rng = Rng::new(0x5EED_0001);
    let mut tokens = Vec::with_capacity(count + 64);
    let mut section = 0usize;
    while tokens.len() < count {
        let mut block = String::new();
        match rng.range(0, 9) {
            0 => {
                section += 1;
                block.push_str(&format!(
                    "## Section {section}: {}\n\n",
                    sentence(&mut rng, 4)
                ));
            }
            1 | 2 => {
                for _ in 0..rng.range(3, 6) {
                    block.push_str("- ");
                    block.push_str(&styled_sentence(&mut rng, 10));
                    block.push('\n');
                }
                block.push('\n');
            }
            3 => {
                block.push_str("```rust\nfn example() -> anyhow::Result<()> {\n");
                for _ in 0..rng.range(6, 14) {
                    block.push_str(RUST_LINES[rng.range(0, RUST_LINES.len() - 1)]);
                    block.push('\n');
                }
                block.push_str("}\n```\n\n");
            }
            4 => {
                block.push_str("| Metric | Before | After |\n| --- | ---: | ---: |\n");
                for _ in 0..rng.range(3, 6) {
                    block.push_str(&format!(
                        "| {} | {} ms | {} ms |\n",
                        rng.pick(WORDS),
                        rng.range(10, 900),
                        rng.range(1, 90)
                    ));
                }
                block.push('\n');
            }
            5 => {
                block.push_str("> ");
                block.push_str(&styled_sentence(&mut rng, 24));
                block.push_str("\n\n");
            }
            _ => {
                for _ in 0..rng.range(2, 5) {
                    block.push_str(&styled_sentence(&mut rng, 14));
                    block.push(' ');
                }
                block.push_str("\n\n");
            }
        }
        tokenize_into(&block, &mut tokens);
    }
    tokens.truncate(count);
    tokens
}

/// Split text into provider-sized tokens: whitespace attaches to the following
/// word, newlines stand alone and long words split into 4-byte pieces.
fn tokenize_into(text: &str, tokens: &mut Vec<String>) {
    let mut current = String::new();
    for ch in text.chars() {
        if ch == '\n' {
            if !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
            tokens.push("\n".to_owned());
        } else if ch == ' ' && !current.trim().is_empty() {
            tokens.push(std::mem::take(&mut current));
            current.push(ch);
        } else {
            current.push(ch);
            if current.len() >= 6 && !current.ends_with(' ') {
                tokens.push(std::mem::take(&mut current));
            }
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
}

fn sentence(rng: &mut Rng, words: usize) -> String {
    (0..words)
        .map(|_| rng.pick(WORDS))
        .collect::<Vec<_>>()
        .join(" ")
}

/// A sentence with the inline styles the flatten pass has to split runs for.
fn styled_sentence(rng: &mut Rng, words: usize) -> String {
    let mut out = String::new();
    for index in 0..words {
        if index > 0 {
            out.push(' ');
        }
        let word = rng.pick(WORDS);
        match rng.range(0, 11) {
            0 => out.push_str(&format!("**{word}**")),
            1 => out.push_str(&format!("*{word}*")),
            2 => out.push_str(&format!("`{word}()`")),
            3 => out.push_str(&format!("[{word}](https://example.com/{word}/{index})")),
            4 => out.push_str(&format!("~~{word}~~")),
            _ => out.push_str(word),
        }
    }
    out.push('.');
    out
}

/// One paragraph of `lines` soft-wrapped lines with mixed inline styles.
pub fn styled_paragraph(lines: usize) -> String {
    let mut rng = Rng::new(0x5EED_0002);
    (0..lines)
        .map(|_| styled_sentence(&mut rng, 10))
        .collect::<Vec<_>>()
        .join("\n")
}

/// A Rust source block of `lines` lines with comments, strings and numbers.
pub fn rust_code(lines: usize) -> String {
    let mut rng = Rng::new(0x5EED_0003);
    let mut out = String::with_capacity(lines * 48);
    let mut line = 0;
    while line < lines {
        out.push_str(&format!(
            "pub fn handler_{line}(input: &str) -> Result<usize> {{\n"
        ));
        line += 1;
        for _ in 0..rng.range(6, 18) {
            if line >= lines {
                break;
            }
            out.push_str(RUST_LINES[rng.range(0, RUST_LINES.len() - 1)]);
            out.push('\n');
            line += 1;
        }
        if line < lines {
            out.push_str("}\n");
            line += 1;
        }
    }
    out.pop();
    out
}

/// A unified Git patch of about `lines` lines across Rust, TypeScript and
/// Python files, with the matching `git diff --numstat` output.
pub fn unified_patch(lines: usize) -> (String, String) {
    let mut rng = Rng::new(0x5EED_0004);
    let mut patch = String::with_capacity(lines * 48);
    let mut numstat = String::new();
    let mut emitted = 0;
    let mut file = 0;
    let ts = [
        "  const value = await client.request({ id, method: \"session.load\" });",
        "  // Keep the viewport pinned while rows stream in.",
        "  if (rows.length > MAX_ROWS) return rows.slice(-MAX_ROWS);",
        "  export function render(row: Row): string { return `${row.id}`; }",
    ];
    let py = [
        "    value = compute(source, limit=42)",
        "    # Settle the prefix before the next delta arrives.",
        "    if len(buffer) > MAX_BYTES: raise ValueError(\"overflow\")",
        "    return {\"id\": row.id, \"text\": row.text}",
    ];
    while emitted < lines {
        let (path, body): (String, &[&str]) = match file % 3 {
            0 => (format!("src/module_{file}.rs"), RUST_LINES),
            1 => (format!("web/src/view_{file}.ts"), &ts),
            _ => (format!("tools/script_{file}.py"), &py),
        };
        patch.push_str(&format!(
            "diff --git a/{path} b/{path}\nindex 1234567..89abcde 100644\n--- a/{path}\n+++ b/{path}\n"
        ));
        emitted += 4;
        let (mut additions, mut deletions) = (0, 0);
        let mut old_start = 1;
        for _ in 0..rng.range(4, 10) {
            let old_start_here = old_start + rng.range(5, 60);
            let context = rng.range(3, 6);
            let removed = rng.range(1, 12);
            let added = rng.range(1, 16);
            patch.push_str(&format!(
                "@@ -{old_start_here},{} +{old_start_here},{} @@ fn context()\n",
                context * 2 + removed,
                context * 2 + added
            ));
            emitted += 1;
            for (kind, count) in [(' ', context), ('-', removed), ('+', added), (' ', context)] {
                for _ in 0..count {
                    patch.push(kind);
                    patch.push_str(body[rng.range(0, body.len() - 1)]);
                    patch.push('\n');
                    emitted += 1;
                }
            }
            additions += added;
            deletions += removed;
            old_start = old_start_here + context * 2 + removed;
        }
        numstat.push_str(&format!("{additions}\t{deletions}\t{path}\n"));
        file += 1;
    }
    (numstat, patch)
}

/// Colored terminal output — a build log, an `ls --color` listing and
/// 256-color/truecolor spans — wrapped at `columns`, `lines` lines long, so a
/// grid shows a full, scrolled screen of mixed styles.
pub fn terminal_output(columns: usize, lines: usize) -> Vec<u8> {
    let mut rng = Rng::new(0x5EED_0005);
    let mut out = String::new();
    for line in 0..lines {
        let mut visible = 0;
        match line % 4 {
            0 => {
                let crate_name = rng.pick(WORDS);
                let text = format!("Compiling {crate_name} v0.{}.{}", rng.range(1, 9), line);
                out.push_str(&format!("\x1b[1;32m{text}\x1b[0m "));
                visible += text.len() + 1;
            }
            1 => {
                let text = format!("warning: unused variable `{}`", rng.pick(WORDS));
                out.push_str(&format!("\x1b[1;33m{text}\x1b[0m "));
                visible += text.len() + 1;
            }
            2 => {
                let text = format!("error[E0{}]: mismatched types", rng.range(100, 999));
                out.push_str(&format!("\x1b[1;31;4m{text}\x1b[0m "));
                visible += text.len() + 1;
            }
            _ => {}
        }
        while visible + 14 < columns {
            let word = rng.pick(WORDS);
            let style = match rng.range(0, 5) {
                0 => "\x1b[34;1m".to_owned(),
                1 => format!("\x1b[38;5;{}m", rng.range(16, 231)),
                2 => format!(
                    "\x1b[38;2;{};{};{}m",
                    rng.range(0, 255),
                    rng.range(0, 255),
                    rng.range(0, 255)
                ),
                3 => format!("\x1b[48;5;{}m", rng.range(232, 255)),
                _ => String::new(),
            };
            out.push_str(&format!("{style}{word:<12}\x1b[0m"));
            visible += 12;
        }
        out.push_str("\r\n");
    }
    out.into_bytes()
}
