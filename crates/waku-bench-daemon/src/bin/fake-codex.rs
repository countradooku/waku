//! A fake `codex` CLI for benchmarks. See `waku_bench_daemon::native::codex`.

fn main() {
    std::process::exit(waku_bench_daemon::native::codex::main(
        std::env::args().collect(),
    ));
}
