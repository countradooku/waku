//! A fake `claude` CLI for benchmarks. See `waku_bench_daemon::native::claude`.

fn main() {
    std::process::exit(waku_bench_daemon::native::claude::main(
        std::env::args().collect(),
    ));
}
