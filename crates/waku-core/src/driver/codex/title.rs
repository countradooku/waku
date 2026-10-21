//! One-shot Codex title generation for the orchestrator's `TitlePort`
//! (RFC 0001).
//!
//! Codex's app-server persists a thread name but does not generate one, so,
//! as Codex Desktop does, an isolated ephemeral turn on a short-lived
//! app-server writes it.

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::Stdio;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context as _, anyhow};
use crossbeam_channel::unbounded;
use serde_json::{Value, json};

fn write_json_line(writer: &mut impl Write, value: &Value) -> std::io::Result<()> {
    serde_json::to_writer(&mut *writer, value)?;
    writer.write_all(b"\n")?;
    writer.flush()
}

const CODEX_TITLE_INSTRUCTIONS: &str = "Generate a concise title for the user's coding task. Return only a plain title of at most six words. Do not use tools, quotes, markdown, labels, or ending punctuation.";
const CODEX_TITLE_MODEL: &str = "gpt-5.6-luna";
const CODEX_TITLE_TIMEOUT: Duration = Duration::from_secs(60);

fn codex_title_thread_params(cwd: &Path) -> Value {
    json!({
        "cwd": cwd.display().to_string(),
        "approvalPolicy": "never",
        "approvalsReviewer": "user",
        "sandbox": "read-only",
        "model": CODEX_TITLE_MODEL,
        "serviceName": "waku-title",
        "baseInstructions": CODEX_TITLE_INSTRUCTIONS,
        "developerInstructions": CODEX_TITLE_INSTRUCTIONS,
        "ephemeral": true
    })
}

fn codex_title_turn_params(thread_id: &str, user_message: &str) -> Value {
    json!({
        "threadId": thread_id,
        "input": [{"type": "text", "text": user_message}],
        "approvalPolicy": "never",
        "approvalsReviewer": "user",
        "sandboxPolicy": {"type": "readOnly"},
        "effort": "low",
        "summary": "none",
        "outputSchema": {
            "type": "object",
            "properties": {
                "title": {"type": "string", "minLength": 1, "maxLength": 80}
            },
            "required": ["title"],
            "additionalProperties": false
        }
    })
}

/// Codex app-server can persist a thread name but does not generate one. Match
/// Codex Desktop's client-owned behavior with an isolated ephemeral turn, then
/// hand the result back to the main writer for `thread/name/set`.
pub(crate) fn generate_codex_title(
    binary: &Path,
    cwd: &Path,
    prompt: &str,
) -> anyhow::Result<String> {
    let mut command = crate::command_env::command(binary);
    let command = command
        .args(["app-server", "--stdio"])
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut child =
        crate::command_env::spawn(command).context("failed to start Codex title generation")?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("Codex title generation stdin is unavailable"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("Codex title generation stdout is unavailable"))?;
    let (messages, message_rx) = unbounded::<Value>();
    let reader = thread::Builder::new()
        .name("waku-codex-title-reader".into())
        .spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if let Ok(value) = serde_json::from_str::<Value>(&line) {
                    let _ = messages.send(value);
                }
            }
        })?;

    let result = (|| -> anyhow::Result<String> {
        write_json_line(
            &mut stdin,
            &json!({
                "method": "initialize",
                "id": 0,
                "params": {
                    "clientInfo": {
                        "name": "waku-title",
                        "title": "Waku Title",
                        "version": env!("CARGO_PKG_VERSION")
                    },
                    "capabilities": {"experimentalApi": true}
                }
            }),
        )?;
        write_json_line(&mut stdin, &json!({"method": "initialized", "params": {}}))?;

        write_json_line(
            &mut stdin,
            &json!({
                "method": "thread/start",
                "id": 1,
                "params": codex_title_thread_params(cwd)
            }),
        )?;

        let deadline = Instant::now() + CODEX_TITLE_TIMEOUT;
        let mut title_thread_id = None::<String>;
        let mut generated = None::<String>;
        loop {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| anyhow!("Codex title generation timed out"))?;
            let value = message_rx
                .recv_timeout(remaining)
                .context("Codex title generation stopped before completion")?;

            if value.get("id").and_then(Value::as_u64) == Some(1) && value.get("method").is_none() {
                if let Some(error) = value.pointer("/error/message").and_then(Value::as_str) {
                    return Err(anyhow!("Codex could not open a title thread: {error}"));
                }
                let thread_id = value
                    .pointer("/result/thread/id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("Codex returned no title thread ID"))?
                    .to_owned();
                title_thread_id = Some(thread_id.clone());
                write_json_line(
                    &mut stdin,
                    &json!({
                        "method": "turn/start",
                        "id": 2,
                        "params": codex_title_turn_params(&thread_id, prompt)
                    }),
                )?;
                continue;
            }

            if value.get("method").and_then(Value::as_str) == Some("item/completed")
                && value.pointer("/params/threadId").and_then(Value::as_str)
                    == title_thread_id.as_deref()
                && value.pointer("/params/item/type").and_then(Value::as_str)
                    == Some("agentMessage")
                && let Some(text) = value.pointer("/params/item/text").and_then(Value::as_str)
            {
                generated = Some(text.to_owned());
                continue;
            }

            if value.get("method").and_then(Value::as_str) == Some("turn/completed")
                && value.pointer("/params/threadId").and_then(Value::as_str)
                    == title_thread_id.as_deref()
            {
                let status = value
                    .pointer("/params/turn/status")
                    .and_then(Value::as_str)
                    .unwrap_or("failed");
                if status != "completed" {
                    let message = value
                        .pointer("/params/turn/error/message")
                        .and_then(Value::as_str)
                        .unwrap_or("the title turn did not complete");
                    return Err(anyhow!("Codex title generation failed: {message}"));
                }
                return generated
                    .as_deref()
                    .and_then(normalize_codex_title)
                    .ok_or_else(|| anyhow!("Codex returned an empty task title"));
            }
        }
    })();

    drop(stdin);
    let _ = child.kill();
    let _ = child.wait();
    let _ = reader.join();
    result
}

fn normalize_codex_title(raw: &str) -> Option<String> {
    let raw = raw.trim();
    let decoded = serde_json::from_str::<Value>(raw)
        .ok()
        .and_then(|value| match value {
            Value::String(title) => Some(title),
            Value::Object(object) => object
                .get("title")
                .and_then(Value::as_str)
                .map(str::to_owned),
            _ => None,
        });
    let candidate = decoded.as_deref().unwrap_or(raw);
    let line = candidate
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with("```") && *line != "json")?;
    let line = line
        .strip_prefix("Title:")
        .or_else(|| line.strip_prefix("title:"))
        .unwrap_or(line)
        .trim();
    let title = line
        .trim_matches(|character| matches!(character, '"' | '\'' | '`' | '#' | '*' | '_'))
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let title = title.trim_end_matches(['.', ',', ':', ';']).trim();
    if title.is_empty() {
        return None;
    }
    let title = title.chars().take(80).collect::<String>();
    let title = title.trim_end().to_owned();
    (!title.is_empty()).then_some(title)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_structured_and_plain_codex_titles() {
        assert_eq!(
            normalize_codex_title("\"Fix Provider Task Titles\"").as_deref(),
            Some("Fix Provider Task Titles")
        );
        assert_eq!(
            normalize_codex_title("Title: **Repair title updates.**").as_deref(),
            Some("Repair title updates")
        );
        assert_eq!(normalize_codex_title("```\n\n```"), None);
    }

    #[test]
    fn title_generation_uses_luna_low_and_only_the_user_message() {
        let thread = codex_title_thread_params(Path::new("/tmp/project"));
        let turn = codex_title_turn_params("title-thread", "Fix task titles");

        assert_eq!(thread["model"], "gpt-5.6-luna");
        assert_eq!(turn["effort"], "low");
        assert_eq!(
            turn["input"],
            json!([{"type":"text","text":"Fix task titles"}])
        );
    }

    #[test]
    #[ignore = "requires an installed, authenticated codex"]
    fn codex_title_generation_against_the_real_cli() {
        let binary = crate::command_env::find_executable("codex").expect("codex is not installed");
        let title = generate_codex_title(
            &binary,
            &std::env::temp_dir(),
            "Find why provider-generated task titles never replace the first prompt and fix it.",
        )
        .expect("Codex should generate a task title");
        assert!(!title.is_empty());
        assert!(title.chars().count() <= 80);
    }
}
