//! Codex CLI session discovery, transcript import, and detached forks.
//!
//! The app-server owns Codex's state database and rollout migration rules, so
//! use its public thread APIs instead of reimplementing `codex resume` by
//! walking private SQLite tables or guessing rollout filenames. These probes
//! are bounded, short-lived, and called only from the daemon's background RPC
//! path — never from a render frame.

use std::io::{BufRead as _, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use anyhow::{Context as _, anyhow, bail};
use serde_json::{Value, json};

use crate::model::{ProviderResumeCursor, ProviderSessionSummary};

const RPC_TIMEOUT: Duration = Duration::from_secs(10);

fn write_json_line(writer: &mut impl Write, value: &Value) -> std::io::Result<()> {
    serde_json::to_writer(&mut *writer, value)?;
    writer.write_all(b"\n")?;
    writer.flush()
}

fn app_server_request(binary: &Path, request: Value) -> anyhow::Result<Value> {
    let cwd = crate::acp_session::catalog_working_directory()?;
    app_server_request_in(binary, &cwd, request)
}

fn app_server_request_in(binary: &Path, cwd: &Path, request: Value) -> anyhow::Result<Value> {
    let mut command = crate::command_env::command(binary);
    let command = command
        .args(["app-server", "--stdio"])
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut child = crate::command_env::spawn(command)
        .with_context(|| format!("could not start {} app-server", binary.display()))?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("Codex app-server stdin is unavailable"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("Codex app-server stdout is unavailable"))?;
    let (tx, rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Ok(value) = serde_json::from_str::<Value>(&line)
                && tx.send(value).is_err()
            {
                break;
            }
        }
    });

    let deadline = Instant::now() + RPC_TIMEOUT;
    let receive_response = |id| -> anyhow::Result<Value> {
        loop {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| anyhow!("Codex app-server session request timed out"))?;
            let value = rx
                .recv_timeout(remaining)
                .map_err(|_| anyhow!("Codex app-server session request timed out"))?;
            if value.get("method").is_none() && value.get("id").and_then(Value::as_u64) == Some(id)
            {
                if let Some(error) = value.pointer("/error/message").and_then(Value::as_str) {
                    bail!("Codex rejected the session request: {error}");
                }
                return Ok(value);
            }
        }
    };
    let response = (|| -> anyhow::Result<Value> {
        write_json_line(
            &mut stdin,
            &json!({
                "method": "initialize",
                "id": 0,
                "params": {
                    "clientInfo": {
                        "name": "waku",
                        "title": "Waku",
                        "version": env!("CARGO_PKG_VERSION")
                    },
                    "capabilities": { "experimentalApi": true }
                }
            }),
        )?;
        receive_response(0)?;
        write_json_line(&mut stdin, &json!({"method": "initialized", "params": {}}))?;
        write_json_line(&mut stdin, &request)?;
        receive_response(1)
    })();

    // A fork response still leaves a live writer in this app-server. Let EOF
    // flush and close it before handing the new thread to another process.
    // Also wait for stdout to close: the configured CLI may wrap Codex in Node.
    drop(stdin);
    let shutdown_deadline = Instant::now() + RPC_TIMEOUT;
    let shutdown = (|| -> anyhow::Result<()> {
        loop {
            if let Some(status) = child.try_wait()? {
                if !status.success() {
                    bail!("Codex app-server exited with {status}");
                }
                if reader.is_finished() {
                    return Ok(());
                }
            }
            if Instant::now() >= shutdown_deadline {
                bail!("Codex app-server did not finish closing its session");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    })();
    if shutdown.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    if reader.is_finished() {
        let _ = reader.join();
    }
    let response = response?;
    shutdown?;
    Ok(response)
}

fn title_from_prompt(prompt: &str) -> Option<String> {
    let mut title = prompt
        .split_whitespace()
        .take(7)
        .collect::<Vec<_>>()
        .join(" ");
    if title.is_empty() {
        return None;
    }
    if title.chars().count() > 54 {
        title = format!("{}…", title.chars().take(53).collect::<String>());
    }
    Some(title)
}

fn parse_session_summaries(response: &Value, limit: usize) -> Vec<ProviderSessionSummary> {
    response
        .pointer("/result/data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|thread| {
            thread.get("ephemeral").and_then(Value::as_bool) != Some(true)
                && thread.get("parentThreadId").is_none_or(Value::is_null)
        })
        .filter_map(|thread| {
            let id = thread.get("id").and_then(Value::as_str)?.trim();
            if id.is_empty() {
                return None;
            }
            let cwd = PathBuf::from(thread.get("cwd").and_then(Value::as_str)?);
            if !cwd.is_absolute() || !cwd.is_dir() {
                return None;
            }
            let title = thread
                .get("name")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|title| !title.is_empty())
                .map(str::to_owned)
                .or_else(|| {
                    thread
                        .get("preview")
                        .and_then(Value::as_str)
                        .and_then(title_from_prompt)
                })?;
            let created_at = thread
                .get("createdAt")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let updated_at = thread
                .get("recencyAt")
                .and_then(Value::as_u64)
                .or_else(|| thread.get("updatedAt").and_then(Value::as_u64))
                .unwrap_or(created_at)
                .max(created_at);
            Some(ProviderSessionSummary {
                cursor: ProviderResumeCursor::Codex {
                    thread_id: id.to_owned(),
                },
                title,
                cwd,
                created_at,
                updated_at,
            })
        })
        .take(limit)
        .collect()
}

/// Ask Codex for CLI-owned threads, excluding exec, editor, app-server and
/// sub-agent rollouts. Waku-created app-server threads are filtered again by
/// native cursor in the daemon catalog.
pub fn list_provider_sessions(
    binary: &Path,
    limit: usize,
) -> anyhow::Result<Vec<ProviderSessionSummary>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let response = app_server_request(
        binary,
        json!({
            "method": "thread/list",
            "id": 1,
            "params": {
                "limit": limit,
                "sortKey": "updated_at",
                "sortDirection": "desc",
                "sourceKinds": ["cli"]
            }
        }),
    )?;
    Ok(parse_session_summaries(&response, limit))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_thread_catalog_metadata_and_filters_subagents() {
        let root = std::env::temp_dir();
        let response = json!({"result": {"data": [
            {
                "id": "01900000-0000-7000-8000-000000000001",
                "name": "Resume this thread",
                "preview": "ignored fallback",
                "cwd": root,
                "createdAt": 10,
                "updatedAt": 20,
                "recencyAt": 25,
                "ephemeral": false,
                "parentThreadId": null
            },
            {
                "id": "01900000-0000-7000-8000-000000000002",
                "preview": "sub-agent",
                "cwd": root,
                "createdAt": 10,
                "updatedAt": 20,
                "ephemeral": false,
                "parentThreadId": "01900000-0000-7000-8000-000000000001"
            }
        ]}});

        let sessions = parse_session_summaries(&response, 10);
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].title, "Resume this thread");
        assert_eq!(sessions[0].updated_at, 25);
    }
}
