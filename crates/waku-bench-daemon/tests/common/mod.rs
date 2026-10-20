//! Shared helpers for the fake-provider tests.
#![allow(dead_code)]

use std::io::{BufRead as _, BufReader, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::time::{Duration, Instant};

use base64::Engine as _;
use crossbeam_channel::{Receiver, unbounded};
use serde_json::{Value, json};
use uuid::Uuid;
use waku_bench_daemon::{TimedEvent, Timeline};
use waku_protocol::model::{
    ActivityFileChange, ActivityFileChangeStatus, ActivityItem, ActivityKind,
};

pub const DIFF: &str = "diff --git a/src/a.rs b/src/a.rs\n--- a/src/a.rs\n+++ b/src/a.rs\n@@ -1,2 +1,2 @@\n fn keep() {}\n-fn old() {}\n+fn new() {}";

pub fn png_url() -> String {
    let png = waku_bench_daemon::png::test_card(1, 16, 12);
    format!(
        "data:image/png;base64,{}",
        base64::engine::general_purpose::STANDARD.encode(png)
    )
}

/// One of every event shape the fakes translate.
pub fn sample_timeline() -> Timeline {
    let command_id = Uuid::from_u128(1);
    let mut command = ActivityItem::new(
        Some("call_cmd".into()),
        ActivityKind::Command,
        "cargo test",
        None,
        false,
    )
    .with_tool_name(Some("Bash"))
    .with_arguments(Some(
        json!({"command": "cargo test", "cwd": "/bench"}).to_string(),
    ));
    command.id = command_id;
    let mut command_done = command.clone().with_output(Some("test result: ok".into()));
    command_done.complete = true;

    let mut edit = ActivityItem::new(
        Some("call_edit".into()),
        ActivityKind::FileChange,
        "Edit src/a.rs",
        None,
        true,
    );
    edit.id = Uuid::from_u128(2);
    edit.file_changes = vec![ActivityFileChange {
        path: "src/a.rs".into(),
        additions: Some(1),
        deletions: Some(1),
        status: Some(ActivityFileChangeStatus::Modified),
        diff: Some(DIFF.into()),
    }];

    let mut image = ActivityItem::new(
        Some("call_img".into()),
        ActivityKind::Tool,
        "View shot.png",
        None,
        true,
    )
    .with_tool_name(Some("view_image"))
    .with_arguments(Some(json!({"path": "/bench/shot.png"}).to_string()))
    .with_image_urls(vec![png_url()]);
    image.id = Uuid::from_u128(3);

    let activity = |at, item: &ActivityItem| {
        TimedEvent::new(at, "richActivity", serde_json::to_value(item).unwrap())
    };
    Timeline {
        name: "sample".into(),
        description: "every event shape".into(),
        events: vec![
            TimedEvent::new(0, "turnStarted", Value::Null),
            TimedEvent::new(1_000, "reasoningDelta", json!("Let me ")),
            TimedEvent::new(2_000, "reasoningDelta", json!("think.")),
            TimedEvent::new(3_000, "textDelta", json!("Running ")),
            TimedEvent::new(4_000, "textDelta", json!("tests.")),
            activity(5_000, &command),
            activity(6_000, &command_done),
            activity(7_000, &edit),
            activity(8_000, &image),
            TimedEvent::new(9_000, "textDelta", json!("All ")),
            TimedEvent::new(10_000, "textDelta", json!("done.")),
            TimedEvent::new(
                11_000,
                "usageUpdated",
                json!({"contextTokens": 1234, "contextWindow": 272000}),
            ),
            TimedEvent::new(
                12_000,
                "turnFinished",
                json!({"success": true, "summary": null}),
            ),
        ],
    }
}

/// A private PATH-style directory: `<name>` symlinked to the fake binary
/// plus a `waku-bench.json` naming `timeline`.
pub struct FakeDir {
    pub dir: PathBuf,
    pub binary: PathBuf,
}

impl FakeDir {
    pub fn new(name: &str, target: &str, timeline: &Timeline, rate: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("waku-fake-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let timeline_path = dir.join("timeline.jsonl");
        std::fs::write(&timeline_path, timeline.to_jsonl()).unwrap();
        std::fs::write(
            dir.join("waku-bench.json"),
            json!({"timeline": timeline_path, "rate": rate, "log": dir.join("log.jsonl")})
                .to_string(),
        )
        .unwrap();
        let binary = dir.join(name);
        #[cfg(unix)]
        std::os::unix::fs::symlink(target, &binary).unwrap();
        #[cfg(not(unix))]
        std::fs::copy(target, &binary).unwrap();
        Self { dir, binary }
    }
}

impl Drop for FakeDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A fake CLI process driven over its stdin/stdout.
pub struct Driven {
    pub child: Child,
    pub stdin: Option<ChildStdin>,
    pub lines: Receiver<Value>,
}

impl Driven {
    pub fn spawn(binary: &Path, args: &[&str], cwd: &Path) -> Self {
        let mut child = Command::new(binary)
            .args(args)
            .current_dir(cwd)
            .env_remove("WAKU_BENCH_TIMELINE")
            .env_remove("WAKU_BENCH_RATE")
            .env_remove("WAKU_BENCH_LOOPS")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (sender, lines) = unbounded();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let line = line.unwrap();
                let value: Value = serde_json::from_str(&line)
                    .unwrap_or_else(|_| panic!("stdout carried a non-JSON line: {line:?}"));
                if sender.send(value).is_err() {
                    break;
                }
            }
        });
        Self {
            stdin: child.stdin.take(),
            child,
            lines,
        }
    }

    pub fn send(&mut self, value: Value) {
        let stdin = self.stdin.as_mut().unwrap();
        writeln!(stdin, "{value}").unwrap();
        stdin.flush().unwrap();
    }

    pub fn next(&self) -> Value {
        self.lines
            .recv_timeout(Duration::from_secs(20))
            .expect("the fake stopped answering")
    }

    /// Collect messages until `done` accepts one (inclusive).
    pub fn until(&self, done: impl Fn(&Value) -> bool) -> Vec<Value> {
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut out = Vec::new();
        loop {
            assert!(Instant::now() < deadline, "timed out; got {out:#?}");
            let value = self.next();
            let finished = done(&value);
            out.push(value);
            if finished {
                return out;
            }
        }
    }

    pub fn close(mut self) -> std::process::ExitStatus {
        drop(self.stdin.take());
        self.child.wait().unwrap()
    }
}
