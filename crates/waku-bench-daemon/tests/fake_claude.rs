//! Drive `fake-claude` over stream-json, as the Agent SDK and Waku do.

mod common;

use std::io::Write as _;
use std::process::{Command, Stdio};

use common::{Driven, FakeDir, sample_timeline};
use serde_json::{Value, json};

const FAKE: &str = env!("CARGO_BIN_EXE_fake-claude");

/// The SDK's argv, including the duplicate and inline flags it passes.
const SDK_ARGS: &[&str] = &[
    "--output-format",
    "stream-json",
    "--verbose",
    "--input-format",
    "stream-json",
    "--thinking-display",
    "summarized",
    "--permission-prompt-tool",
    "stdio",
    "--setting-sources=user,project,local",
    "--session-id",
    "11111111-2222-4333-8444-555555555555",
    "--include-partial-messages",
    "--replay-user-messages",
    "--thinking-display",
    "summarized",
];

fn kind(value: &Value) -> &str {
    value.get("type").and_then(Value::as_str).unwrap_or("")
}

fn user(text: &str) -> Value {
    json!({
        "type": "user",
        "message": {"role": "user", "content": [{"type": "text", "text": text}]},
        "parent_tool_use_id": null
    })
}

#[test]
fn stream_json_turn_matches_claude_code_framing() {
    let timeline = sample_timeline();
    let fake = FakeDir::new("claude", FAKE, &timeline, "max");
    let mut driven = Driven::spawn(&fake.binary, SDK_ARGS, &fake.dir);

    driven.send(json!({
        "type": "control_request", "request_id": "init-1",
        "request": {"subtype": "initialize", "hooks": null}
    }));
    let init = driven.next();
    assert_eq!(kind(&init), "control_response");
    assert_eq!(init["response"]["subtype"], "success");
    assert_eq!(init["response"]["request_id"], "init-1");
    let account = &init["response"]["response"]["account"];
    assert!(account["apiKeySource"].is_string());
    assert!(
        account.get("subscriptionType").is_none(),
        "a subscription type would make T3 call Anthropic's usage API"
    );
    assert!(
        !init["response"]["response"]["models"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    driven.send(json!({
        "type": "control_request", "request_id": "mcp-1",
        "request": {"subtype": "mcp_set_servers", "servers": {}}
    }));
    assert_eq!(
        driven.next()["response"]["response"],
        json!({"added": [], "removed": [], "errors": {}})
    );

    driven.send(user("go"));
    let frames = driven.until(|value| kind(value) == "result");

    assert_eq!(kind(&frames[0]), "user");
    assert_eq!(frames[0]["isReplay"], true);
    assert_eq!(kind(&frames[1]), "system");
    assert_eq!(frames[1]["subtype"], "init");
    for frame in &frames[1..] {
        assert_eq!(frame["session_id"], "11111111-2222-4333-8444-555555555555");
        if matches!(kind(frame), "assistant" | "user" | "stream_event") {
            assert!(
                frame.get("parent_tool_use_id").is_some_and(Value::is_null),
                "T3 drops frames without an explicit null parent_tool_use_id"
            );
        }
    }

    let deltas = |delta_type: &str, field: &str| {
        frames
            .iter()
            .filter(|f| kind(f) == "stream_event")
            .filter(|f| f["event"]["delta"]["type"] == delta_type)
            .filter_map(|f| f["event"]["delta"][field].as_str())
            .collect::<String>()
    };
    assert_eq!(deltas("text_delta", "text"), timeline.assistant_text());
    assert_eq!(
        deltas("thinking_delta", "thinking"),
        timeline.reasoning_text()
    );

    let blocks = frames
        .iter()
        .filter(|f| kind(f) == "assistant")
        .map(|f| &f["message"]["content"][0])
        .collect::<Vec<_>>();
    let block_types = blocks
        .iter()
        .map(|block| block["type"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        block_types,
        [
            "thinking", "text", "tool_use", "tool_use", "tool_use", "text"
        ]
    );
    assert_eq!(blocks[1]["text"], "Running tests.");
    assert_eq!(blocks[2]["name"], "Bash");
    assert_eq!(blocks[2]["input"]["command"], "cargo test");
    assert_eq!(blocks[3]["name"], "Edit");
    assert_eq!(
        blocks[3]["input"]["old_string"],
        "fn keep() {}\nfn old() {}"
    );
    assert_eq!(
        blocks[3]["input"]["new_string"],
        "fn keep() {}\nfn new() {}"
    );
    assert_eq!(blocks[5]["text"], "All done.");

    let results = frames
        .iter()
        .filter(|f| kind(f) == "user" && f.get("isReplay").is_none())
        .map(|f| &f["message"]["content"][0])
        .collect::<Vec<_>>();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0]["tool_use_id"], blocks[2]["id"]);
    assert_eq!(results[0]["content"], "test result: ok");
    assert_eq!(results[2]["content"][0]["type"], "image");
    assert_eq!(
        results[2]["content"][0]["source"]["media_type"],
        "image/png"
    );

    let result = frames.last().unwrap();
    assert_eq!(result["subtype"], "success");
    assert_eq!(result["is_error"], false);
    assert_eq!(result["result"], "All done.");
    let model_usage = result["modelUsage"].as_object().unwrap();
    assert_eq!(
        model_usage.values().next().unwrap()["contextWindow"],
        272000
    );
    assert!(driven.close().success());
}

#[test]
fn interrupt_ends_the_turn_with_an_error_result() {
    let fake = FakeDir::new(
        "claude",
        FAKE,
        &waku_bench_daemon::Timeline::builtin("prose").unwrap(),
        "1",
    );
    let mut driven = Driven::spawn(&fake.binary, SDK_ARGS, &fake.dir);
    driven.send(user("go"));
    let mut deltas = 0;
    while deltas < 3 {
        if driven.next()["event"]["delta"]["type"] == "text_delta" {
            deltas += 1;
        }
    }
    driven.send(json!({
        "type": "control_request", "request_id": "stop",
        "request": {"subtype": "interrupt"}
    }));
    let frames = driven.until(|value| kind(value) == "result");
    assert!(
        frames
            .iter()
            .any(|f| kind(f) == "control_response" && f["response"]["request_id"] == "stop")
    );
    let result = frames.last().unwrap();
    assert_eq!(result["is_error"], true);
    assert!(result["errors"].is_array());
    driven.close();
}

#[test]
fn prompt_text_selects_a_builtin_timeline() {
    let fake = FakeDir::new("claude", FAKE, &sample_timeline(), "max");
    let mut driven = Driven::spawn(&fake.binary, SDK_ARGS, &fake.dir);
    driven.send(user("images"));
    let frames = driven.until(|value| kind(value) == "result");
    let images = frames
        .iter()
        .filter(|f| kind(f) == "user" && f.get("isReplay").is_none())
        .filter(|f| f["message"]["content"][0]["content"][0]["type"] == "image")
        .count();
    assert_eq!(images, 20);
    driven.close();
}

#[test]
fn version_and_one_shot_structured_output() {
    let version = Command::new(FAKE).arg("--version").output().unwrap();
    assert!(version.status.success());
    assert_eq!(
        String::from_utf8_lossy(&version.stdout).trim(),
        "2.1.300 (Claude Code)"
    );

    let mut child = Command::new(FAKE)
        .args([
            "-p",
            "--output-format",
            "json",
            "--json-schema",
            r#"{"type":"object","properties":{"title":{"type":"string"}}}"#,
            "--tools",
            "",
            "--permission-mode",
            "dontAsk",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"title this")
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["structured_output"], json!({"title": "Bench task"}));
}
