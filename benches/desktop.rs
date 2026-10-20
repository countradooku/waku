//! Desktop hot paths that run on or feed the UI thread: streaming Markdown
//! parse, inline flatten, code highlighting, review-diff parse and the
//! terminal frame snapshot. All of them are window-free.
//!
//! Run with `cargo bench -p waku --bench desktop`; compare two revisions with
//! `scripts/bench/compare.sh`.

use std::hint::black_box;
use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use waku::bench_support::{self as support, Block, IncrementalParser, Lang, ReviewDiffSource};

#[path = "support/fixtures.rs"]
mod fixtures;

const STREAM_TOKENS: usize = 20_000;
const CODE_LINES: usize = 3_000;
const PATCH_LINES: usize = 10_000;
const TERMINAL_COLUMNS: usize = 200;
const TERMINAL_ROWS: usize = 60;

fn markdown(c: &mut Criterion) {
    let tokens = fixtures::markdown_stream_tokens(STREAM_TOKENS);
    let bytes = tokens.iter().map(String::len).sum::<usize>() as u64;
    let mut group = c.benchmark_group("md");
    group.throughput(Throughput::Bytes(bytes));
    group.sample_size(10);
    group.bench_function("incremental_append_20k_tokens", |b| {
        b.iter(|| {
            let mut parser = IncrementalParser::new();
            for token in &tokens {
                parser.append(black_box(token));
            }
            parser.tree().blocks.len()
        });
    });
    group.finish();

    let paragraph = fixtures::styled_paragraph(CODE_LINES);
    let runs = match support::parse(&paragraph).blocks.into_iter().next() {
        Some(support::TopBlock {
            block: Block::Paragraph { runs },
            ..
        }) => runs,
        _ => panic!("styled paragraph fixture did not parse as one paragraph"),
    };
    let code = fixtures::rust_code(CODE_LINES);

    let mut group = c.benchmark_group("md");
    group.sample_size(50);
    group.throughput(Throughput::Bytes(paragraph.len() as u64));
    group.bench_function("flatten_3000_line_paragraph", |b| {
        b.iter(|| support::flatten(black_box(&runs)));
    });
    group.throughput(Throughput::Bytes(code.len() as u64));
    group.bench_function("code_runs_3000_line_block", |b| {
        b.iter(|| support::code_runs(black_box(&code), Some(Lang::Rust)));
    });
    group.bench_function("highlight_tokenize_3000_line_block", |b| {
        b.iter(|| support::tokenize(Lang::Rust, black_box(&code)));
    });
    group.finish();
}

fn review_diff(c: &mut Criterion) {
    let (numstat, patch) = fixtures::unified_patch(PATCH_LINES);
    let mut group = c.benchmark_group("review_diff");
    group.throughput(Throughput::Bytes(patch.len() as u64));
    group.sample_size(20);
    group.bench_function("parse_10k_line_patch", |b| {
        b.iter_batched(
            || (),
            |()| {
                support::parse_collected(
                    ReviewDiffSource::Uncommitted,
                    black_box(&numstat),
                    black_box(&patch),
                    false,
                )
            },
            // Dropping a 10k-line snapshot is part of replacing one, but
            // keep it out of the parse figure.
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn terminal(c: &mut Criterion) {
    let output = fixtures::terminal_output(TERMINAL_COLUMNS, TERMINAL_ROWS * 3);
    let mut grid = support::TerminalSnapshotBench::new(TERMINAL_COLUMNS, TERMINAL_ROWS, &output);
    let mut group = c.benchmark_group("terminal");
    group.throughput(Throughput::Elements(
        (TERMINAL_COLUMNS * TERMINAL_ROWS) as u64,
    ));
    // Every row rebuilt: a frame after a scroll, resize or theme change.
    group.bench_function("snapshot_200x60", |b| {
        b.iter(|| black_box(&mut grid).snapshot());
    });
    // Output rewrote one row in place: parse the edit, rebuild that row only.
    group.throughput(Throughput::Elements(TERMINAL_COLUMNS as u64));
    group.bench_function("incremental_1_row_200x60", |b| {
        b.iter(|| black_box(&mut grid).incremental());
    });
    group.finish();
}

criterion_group! {
    name = benches;
    // Short windows keep the A/B gate affordable; the gate compares the
    // median of several alternating rounds rather than one long run.
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .noise_threshold(0.03);
    targets = markdown, review_diff, terminal
}
criterion_main!(benches);
