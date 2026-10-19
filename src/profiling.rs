//! `profiling` feature: route the app's `tracing` spans to Tracy.
//!
//! Spans are entered with the crate-local `profile_span!` macro, which
//! compiles to nothing without this feature, so shipping builds carry neither
//! the spans nor a subscriber. With it, a `tracing-tracy` layer records every
//! span for a Tracy profiler that attaches on demand, and `WAKU_TRACE` adds a
//! stderr layer that prints each matching span's busy/idle time when it
//! closes. `WAKU_TRACE` takes a `tracing_subscriber` target filter such as
//! `waku::app=debug,waku::md=trace`; `1`, `true` or `all` mean everything.
//! The daemon installs the same pair in `crates/waku-daemon/src/main.rs`.

use tracing_subscriber::Layer as _;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

pub fn init() {
    let stderr = std::env::var("WAKU_TRACE")
        .ok()
        .filter(|filter| !filter.trim().is_empty())
        .map(|filter| {
            let everything = Targets::new().with_default(tracing::Level::TRACE);
            let targets = match filter.trim() {
                "1" | "true" | "all" => everything,
                filter => filter.parse().unwrap_or_else(|error| {
                    eprintln!("ignoring unparsable WAKU_TRACE={filter:?}: {error}");
                    everything
                }),
            };
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_span_events(FmtSpan::CLOSE)
                .with_filter(targets)
        });
    let _ = tracing_subscriber::registry()
        .with(tracing_tracy::TracyLayer::default())
        .with(stderr)
        .try_init();
}
