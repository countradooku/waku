//! Streaming Markdown must cost O(total tokens), not O(tokens²).
//!
//! `IncrementalParser::append` reparses only from the last stable block
//! boundary. If that boundary ever stops advancing — or an append starts
//! copying or rescanning the whole document — every token pays for the full
//! response so far and a long answer goes quadratic.
//!
//! Timing is too noisy to assert on in CI, so this counts heap allocations
//! instead. Parsing allocates in proportion to the bytes it parses, and the
//! counts are exact and repeatable for a fixed input: streaming 2N tokens must
//! cost about twice what N tokens cost (4x would mean quadratic).
// The `perf-counters` feature installs its own global allocator.
#![cfg(not(feature = "perf-counters"))]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use waku::bench_support::IncrementalParser;

#[path = "../benches/support/fixtures.rs"]
mod fixtures;

/// Counts allocations made by the current thread only, so the harness and
/// any other thread cannot perturb the measurement.
struct CountingAllocator;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
    static ALLOCATED_BYTES: Cell<u64> = const { Cell::new(0) };
}

fn record(bytes: usize) {
    let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
    let _ = ALLOCATED_BYTES.try_with(|total| total.set(total.get() + bytes as u64));
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record(new_size);
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

/// (allocations, allocated bytes) to stream `tokens` through a fresh parser.
fn stream_cost(tokens: &[String]) -> (u64, u64) {
    let before = (ALLOCATIONS.get(), ALLOCATED_BYTES.get());
    let mut parser = IncrementalParser::new();
    for token in tokens {
        parser.append(token);
    }
    let after = (ALLOCATIONS.get(), ALLOCATED_BYTES.get());
    assert!(!parser.tree().blocks.is_empty());
    drop(parser);
    (after.0 - before.0, after.1 - before.1)
}

#[test]
fn incremental_append_work_grows_linearly_with_tokens() {
    const TOKENS: usize = 20_000;
    const MAX_RATIO: f64 = 2.5;

    let tokens = fixtures::markdown_stream_tokens(TOKENS);
    let (half_allocations, half_bytes) = stream_cost(&tokens[..TOKENS / 2]);
    let (full_allocations, full_bytes) = stream_cost(&tokens);

    let allocation_ratio = full_allocations as f64 / half_allocations as f64;
    let byte_ratio = full_bytes as f64 / half_bytes as f64;
    eprintln!(
        "IncrementalParser::append: {} tokens -> {half_allocations} allocations / \
         {half_bytes} B; {TOKENS} tokens -> {full_allocations} allocations / {full_bytes} B \
         (x{allocation_ratio:.2} allocations, x{byte_ratio:.2} bytes)",
        TOKENS / 2
    );
    assert!(
        allocation_ratio < MAX_RATIO,
        "doubling the stream multiplied allocations by {allocation_ratio:.2} \
         (limit {MAX_RATIO}); append work is no longer linear"
    );
    assert!(
        byte_ratio < MAX_RATIO,
        "doubling the stream multiplied allocated bytes by {byte_ratio:.2} \
         (limit {MAX_RATIO}); append work is no longer linear"
    );
}
