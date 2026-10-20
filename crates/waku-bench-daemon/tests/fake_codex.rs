//! Drive `fake-codex app-server` over JSON-RPC and check what it streams.

mod common;

use std::process::Command;

use common::{DIFF, Driven, FakeDir, sample_timeline};
use serde_json::{Value, json};

const FAKE: &str = env!("CARGO_BIN_EXE_fake-codex");

fn method(value: &Value) -> &str {
    value.get("method").and_then(Value::as_str).unwrap_or("")
}

fn handshake(driven: &mut Driven) -> String {
    driven.send(json!({"id": 0, "method": "initialize", "params": {
        "clientInfo": {"name": "test", "title": "Test", "version": "1"},
        "capabilities": {"experimentalApi": true}
    }}));
    let initialized = driven.next();
    assert_eq!(initialized["id"], 0);
    let user_agent = initialized["result"]["userAgent"].as_str().unwrap();
    assert!(user_agent.contains("/0.160.0"), "{user_agent}");
    for key in ["codexHome", "platformFamily", "platformOs"] {
        assert!(initialized["result"][key].is_string(), "missing {key}");
    }
    driven.send(json!({"method": "initialized", "params": {}}));
    driven.send(json!({"id": 1, "method": "thread/start", "params": {
        "cwd": "/tmp/project", "approvalPolicy": "never", "sandbox": "danger-full-access"
    }}));
    let thread = driven.next();
    assert_eq!(thread["id"], 1);
    for key in [
        "id",
        "cliVersion",
        "createdAt",
        "updatedAt",
        "cwd",
        "ephemeral",
        "modelProvider",
        "preview",
        "projectId",
        "sessionId",
        "source",
        "status",
        "turns",
    ] {
        assert!(
            thread["result"]["thread"].get(key).is_some(),
            "thread is missing {key}"
        );
    }
    thread["result"]["thread"]["id"]
        .as_str()
        .unwrap()
        .to_owned()
}

#[test]
fn app_server_streams_a_turn_as_codex_items() {
    let timeline = sample_timeline();
    let fake = FakeDir::new("codex", FAKE, &timeline, "max");
    let mut driven = Driven::spawn(&fake.binary, &["app-server", "--stdio"], &fake.dir);
    let thread_id = handshake(&mut driven);

    driven.send(json!({"id": 7, "method": "turn/start", "params": {
        "threadId": thread_id, "input": [{"type": "text", "text": "go"}]
    }}));
    let messages = driven.until(|value| method(value) == "turn/completed");

    // The response precedes every notification of the turn it starts.
    assert_eq!(messages[0]["id"], 7);
    let turn_id = messages[0]["result"]["turn"]["id"].as_str().unwrap();
    assert_eq!(method(&messages[1]), "turn/started");
    assert_eq!(messages[1]["params"]["turn"]["id"], turn_id);
    for message in &messages[1..] {
        assert_eq!(message["params"]["threadId"], thread_id.as_str());
        if method(message).starts_with("item/") {
            assert_eq!(message["params"]["turnId"], turn_id);
        }
    }
    for message in messages.iter().filter(|m| method(m) == "item/started") {
        assert!(message["params"]["startedAtMs"].is_u64());
    }
    for message in messages.iter().filter(|m| method(m) == "item/completed") {
        assert!(message["params"]["completedAtMs"].is_u64());
    }

    let joined = |name: &str| {
        messages
            .iter()
            .filter(|m| method(m) == name)
            .filter_map(|m| m["params"]["delta"].as_str())
            .collect::<String>()
    };
    assert_eq!(joined("item/agentMessage/delta"), timeline.assistant_text());
    assert_eq!(
        joined("item/reasoning/summaryTextDelta"),
        timeline.reasoning_text()
    );

    let completed = messages
        .iter()
        .filter(|m| method(m) == "item/completed")
        .map(|m| &m["params"]["item"])
        .collect::<Vec<_>>();
    let types = completed
        .iter()
        .map(|item| item["type"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        types,
        [
            "reasoning",
            "agentMessage",
            "commandExecution",
            "fileChange",
            "mcpToolCall",
            "agentMessage"
        ]
    );
    assert_eq!(completed[1]["text"], "Running tests.");
    assert_eq!(completed[2]["command"], "cargo test");
    assert_eq!(completed[2]["aggregatedOutput"], "test result: ok");
    assert_eq!(completed[2]["exitCode"], 0);
    assert_eq!(completed[3]["changes"][0]["path"], "src/a.rs");
    assert_eq!(completed[3]["changes"][0]["diff"], DIFF);
    let image = &completed[4]["result"]["content"][0];
    assert_eq!(image["type"], "image");
    assert_eq!(image["mimeType"], "image/png");
    assert!(
        common::png_url().ends_with(image["data"].as_str().unwrap()),
        "image bytes must survive the translation"
    );

    let usage = messages
        .iter()
        .find(|m| method(m) == "thread/tokenUsage/updated")
        .unwrap();
    assert_eq!(usage["params"]["tokenUsage"]["last"]["totalTokens"], 1234);
    assert_eq!(usage["params"]["tokenUsage"]["modelContextWindow"], 272000);
    let done = messages.last().unwrap();
    assert_eq!(done["params"]["turn"]["status"], "completed");
    assert_eq!(done["params"]["turn"]["id"], turn_id);

    // Unknown methods fail like an older app-server; nothing else is printed.
    driven.send(json!({"id": 9, "method": "thread/goal/get", "params": {"threadId": thread_id}}));
    let error = driven.next();
    assert_eq!(error["id"], 9);
    assert_eq!(error["error"]["code"], -32601);
    assert!(driven.close().success());
}

#[test]
fn structured_turns_answer_title_generation() {
    let timeline = sample_timeline();
    let fake = FakeDir::new("codex", FAKE, &timeline, "1");
    let mut driven = Driven::spawn(&fake.binary, &["app-server", "--stdio"], &fake.dir);
    let thread_id = handshake(&mut driven);
    driven.send(json!({"id": 2, "method": "turn/start", "params": {
        "threadId": thread_id,
        "input": [{"type": "text", "text": "name this"}],
        "outputSchema": {"type": "object", "properties": {"title": {"type": "string"}}}
    }}));
    let messages = driven.until(|value| method(value) == "turn/completed");
    let message = messages
        .iter()
        .find(|m| method(m) == "item/completed")
        .unwrap();
    let title: Value =
        serde_json::from_str(message["params"]["item"]["text"].as_str().unwrap()).unwrap();
    assert_eq!(title, json!({"title": "Bench task"}));
    driven.close();
}

#[test]
fn interrupt_completes_the_turn_as_interrupted() {
    let fake = FakeDir::new(
        "codex",
        FAKE,
        &waku_bench_daemon::Timeline::builtin("prose").unwrap(),
        "1",
    );
    let mut driven = Driven::spawn(&fake.binary, &["app-server"], &fake.dir);
    let thread_id = handshake(&mut driven);
    driven.send(json!({"id": 3, "method": "turn/start", "params": {
        "threadId": thread_id, "input": [{"type": "text", "text": "go"}]
    }}));
    let mut deltas = 0;
    let mut turn_id = None;
    while deltas < 3 {
        let message = driven.next();
        if message["id"] == 3 {
            turn_id = message["result"]["turn"]["id"].as_str().map(str::to_owned);
        }
        if method(&message) == "item/agentMessage/delta" {
            deltas += 1;
        }
    }
    driven.send(json!({"id": 4, "method": "turn/interrupt", "params": {
        "threadId": thread_id, "turnId": turn_id
    }}));
    let messages = driven.until(|value| method(value) == "turn/completed");
    assert!(messages.iter().any(|m| m["id"] == 4));
    assert_eq!(
        messages.last().unwrap()["params"]["turn"]["status"],
        "interrupted"
    );
    driven.close();
}

#[test]
fn cli_probes_and_exec() {
    let version = Command::new(FAKE).arg("--version").output().unwrap();
    assert!(version.status.success());
    assert_eq!(
        String::from_utf8_lossy(&version.stdout).trim(),
        "codex-cli 0.160.0"
    );

    let help = Command::new(FAKE)
        .args(["app-server", "--help"])
        .output()
        .unwrap();
    assert!(help.status.success());

    let mcp = Command::new(FAKE)
        .args([
            "--config",
            "features.plugins=false",
            "mcp",
            "list",
            "--json",
        ])
        .output()
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&mcp.stdout).trim(), "[]");

    let dir = std::env::temp_dir().join(format!("fake-codex-exec-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let schema = dir.join("schema.json");
    std::fs::write(
        &schema,
        r#"{"type":"object","properties":{"title":{"type":"string"},"needsRefinement":{"type":"boolean"}}}"#,
    )
    .unwrap();
    let last = dir.join("last.json");
    let mut exec = Command::new(FAKE)
        .args(["exec", "--ephemeral", "-s", "read-only", "--output-schema"])
        .arg(&schema)
        .arg("--output-last-message")
        .arg(&last)
        .arg("-")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    use std::io::Write as _;
    exec.stdin.take().unwrap().write_all(b"title this").unwrap();
    assert!(exec.wait().unwrap().success());
    let answer: Value = serde_json::from_str(&std::fs::read_to_string(&last).unwrap()).unwrap();
    assert_eq!(
        answer,
        json!({"title": "Bench task", "needsRefinement": false})
    );
    std::fs::remove_dir_all(dir).unwrap();
}
