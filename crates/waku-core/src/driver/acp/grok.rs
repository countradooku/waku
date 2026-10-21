//! Grok's native fork and rollback (RFC 0002 "ACP flavors"): Grok has no
//! turn-aware fork over ACP, so Waku asks a short-lived second agent process
//! (channel `fork-1`) for `_x.ai/session/fork`, which copies the whole
//! session on disk, then truncates the copy's history files to the retained
//! turns. The result replaces the provider thread (`replaced: true`).

use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, anyhow, bail};
use chrono::{SecondsFormat, Utc};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader as AsyncBufReader};
use uuid::Uuid;

use super::protocol::{Connection, Inbound, decode};
use crate::driver::v2::{
    DriverError, DriverResult, NullRawLog, ProcessSpec, ProtocolReason, RawFrameLog,
    TransportFactory,
};

/// The replay channel of the side process.
pub const FORK_CHANNEL: &str = "fork-1";
const RPC_TIMEOUT: Duration = Duration::from_secs(30);

/// Grok's home under a provider home: `GROK_HOME`, else `<home>/.grok`.
pub fn grok_home(provider_home: &Path, configured: bool) -> PathBuf {
    if configured {
        return provider_home.to_owned();
    }
    std::env::var_os("GROK_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| provider_home.join(".grok"))
}

fn fork_failed(detail: impl Into<String>) -> DriverError {
    DriverError::Protocol {
        reason: ProtocolReason::UnexpectedFrame,
        detail: detail.into(),
    }
}

/// Forks `source` into a new Grok session that keeps its first
/// `retained_turns` prompts (`None` keeps all of them). Returns the new
/// session id.
pub async fn fork_session(
    transports: &Arc<dyn TransportFactory>,
    binary: &Path,
    cwd: &Path,
    home: &Path,
    source: &str,
    retained_turns: Option<usize>,
    raw_log: Arc<dyn RawFrameLog>,
) -> DriverResult<String> {
    Uuid::parse_str(source)
        .map_err(|_| fork_failed("Grok returned an invalid source session ID"))?;
    let source_dir = {
        let (home, source) = (home.to_owned(), source.to_owned());
        tokio::task::spawn_blocking(move || find_session_directory_in(&home, &source))
            .await
            .map_err(|error| fork_failed(error.to_string()))?
            .map_err(|error| fork_failed(error.to_string()))?
    };
    let fork_id = tokio::time::timeout(
        RPC_TIMEOUT,
        native_fork(transports, binary, cwd, source, raw_log),
    )
    .await
    .map_err(|_| DriverError::Protocol {
        reason: ProtocolReason::Timeout,
        detail: "timed out waiting for Grok's session fork".to_owned(),
    })??;
    let Some(retained) = retained_turns else {
        return Ok(fork_id);
    };
    let fork_dir = source_dir
        .parent()
        .ok_or_else(|| fork_failed("Grok's source session directory has no parent"))?
        .join(&fork_id);
    tokio::task::spawn_blocking(move || {
        let result = truncate_fork(&fork_dir, retained);
        if result.is_err() {
            let _ = fs::remove_dir_all(&fork_dir);
        }
        result
    })
    .await
    .map_err(|error| fork_failed(error.to_string()))?
    .map_err(|error| fork_failed(error.to_string()))?;
    Ok(fork_id)
}

async fn native_fork(
    transports: &Arc<dyn TransportFactory>,
    binary: &Path,
    cwd: &Path,
    source: &str,
    raw_log: Arc<dyn RawFrameLog>,
) -> DriverResult<String> {
    let transport = transports
        .spawn_stdio(
            FORK_CHANNEL,
            ProcessSpec {
                program: binary.to_owned(),
                args: ["agent", "--always-approve", "--no-leader", "stdio"]
                    .map(str::to_owned)
                    .to_vec(),
                env: Vec::new(),
                cwd: cwd.to_owned(),
            },
        )
        .await?;
    let connection = Arc::new(Connection::new(FORK_CHANNEL, transport.stdin, raw_log));
    let reader = connection.clone();
    let stdout = transport.stdout;
    let read = tokio::spawn(async move {
        let mut lines = AsyncBufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let Ok(frame) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            if let Some(Inbound::Response { id, result }) = decode(&frame) {
                reader.resolve(id, result);
            }
        }
        reader.fail_pending();
    });
    let result = async {
        connection
            .request(
                "initialize",
                json!({
                    "protocolVersion": 1,
                    "clientCapabilities": {
                        "fs": {"readTextFile": true, "writeTextFile": true},
                        "terminal": true
                    }
                }),
            )
            .await
            .map_err(|error| fork_failed(format!("Grok initialize failed: {}", error.detail())))?;
        let response = connection
            .request(
                "_x.ai/session/fork",
                json!({ "sourceSessionId": source, "sourceCwd": cwd, "newCwd": cwd }),
            )
            .await
            .map_err(|error| {
                fork_failed(format!("Grok session fork failed: {}", error.detail()))
            })?;
        let session_id = response
            .get("newSessionId")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| fork_failed("Grok returned no forked session ID"))?;
        Uuid::parse_str(session_id)
            .map_err(|_| fork_failed("Grok returned an invalid forked session ID"))?;
        Ok(session_id.to_owned())
    }
    .await;
    // Closing stdin is the side process's shutdown signal.
    connection.close().await;
    let _ = read.await;
    let _ = transport.exit.await;
    result
}

/// The synchronous entry the legacy daemon fork and rewind paths use until
/// the orchestrator cutover: the same fork, on real processes.
pub fn fork_session_at_turn_blocking(
    binary: &Path,
    cwd: &Path,
    source: &str,
    retained_turns: usize,
) -> anyhow::Result<String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("could not start the Grok fork runtime")?;
    let transports: Arc<dyn TransportFactory> =
        Arc::new(crate::driver::v2::ProcessTransportFactory);
    let home = grok_home(
        &dirs::home_dir().ok_or_else(|| anyhow!("Grok's home directory could not be located"))?,
        false,
    );
    runtime
        .block_on(fork_session(
            &transports,
            binary,
            cwd,
            &home,
            source,
            Some(retained_turns),
            Arc::new(NullRawLog),
        ))
        .map_err(|error| match error {
            DriverError::Protocol { detail, .. } => anyhow!(detail),
            other => anyhow!(other.to_string()),
        })
}

pub fn find_session_directory_in(grok_home: &Path, session_id: &str) -> anyhow::Result<PathBuf> {
    let sessions = grok_home.join("sessions");
    for entry in fs::read_dir(&sessions)
        .with_context(|| format!("could not read Grok sessions at {}", sessions.display()))?
    {
        let Ok(entry) = entry else {
            continue;
        };
        let candidate = entry.path().join(session_id);
        if candidate.join("summary.json").is_file() {
            return Ok(candidate);
        }
    }
    bail!("Grok session {session_id} was not found on disk")
}

fn truncate_fork(session_dir: &Path, retained_turns: usize) -> anyhow::Result<()> {
    let chat_path = session_dir.join("chat_history.jsonl");
    let updates_path = session_dir.join("updates.jsonl");
    let chat = read_json_lines(&chat_path)?;
    let updates = read_json_lines(&updates_path)?;
    let chat = truncate_at_turn(&chat, retained_turns, is_chat_prompt)?;
    let updates = truncate_at_turn(&updates, retained_turns, is_update_prompt)?;
    write_json_lines(&chat_path, &chat)?;
    write_json_lines(&updates_path, &updates)?;

    let summary_path = session_dir.join("summary.json");
    let mut summary: Value = serde_json::from_slice(
        &fs::read(&summary_path)
            .with_context(|| format!("could not read {}", summary_path.display()))?,
    )
    .context("Grok's fork summary is invalid JSON")?;
    summary["num_messages"] = json!(updates.len());
    summary["num_chat_messages"] = json!(chat.len());
    summary["updated_at"] = json!(Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true));
    write_json(&summary_path, &summary)
}

fn truncate_at_turn(
    values: &[Value],
    retained_turns: usize,
    is_prompt: fn(&Value) -> bool,
) -> anyhow::Result<Vec<Value>> {
    let mut turns = 0;
    let cutoff = values.iter().position(|value| {
        if !is_prompt(value) {
            return false;
        }
        if turns == retained_turns {
            true
        } else {
            turns += 1;
            false
        }
    });
    if cutoff.is_none() {
        turns = values.iter().filter(|value| is_prompt(value)).count();
        if turns < retained_turns {
            bail!("Grok has only {turns} native turns, but Waku needs {retained_turns}");
        }
    }
    Ok(values[..cutoff.unwrap_or(values.len())].to_vec())
}

fn is_chat_prompt(value: &Value) -> bool {
    value.get("type").and_then(Value::as_str) == Some("user")
        && value.get("prompt_index").and_then(Value::as_u64).is_some()
}

fn is_update_prompt(value: &Value) -> bool {
    value
        .pointer("/params/update/sessionUpdate")
        .and_then(Value::as_str)
        == Some("user_message_chunk")
}

fn read_json_lines(path: &Path) -> anyhow::Result<Vec<Value>> {
    let file = fs::File::open(path)
        .with_context(|| format!("could not open Grok history at {}", path.display()))?;
    BufReader::new(file)
        .lines()
        .map(|line| {
            let line = line?;
            serde_json::from_str(&line).context("Grok's history contains invalid JSON")
        })
        .collect()
}

fn write_json_lines(path: &Path, values: &[Value]) -> anyhow::Result<()> {
    write_atomic(path, |writer| {
        for value in values {
            serde_json::to_writer(&mut *writer, value)?;
            writer.write_all(b"\n")?;
        }
        Ok(())
    })
}

fn write_json(path: &Path, value: &Value) -> anyhow::Result<()> {
    write_atomic(path, |writer| {
        serde_json::to_writer_pretty(&mut *writer, value)?;
        writer.write_all(b"\n")?;
        Ok(())
    })
}

fn write_atomic(
    path: &Path,
    write: impl FnOnce(&mut BufWriter<fs::File>) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("Grok history path has no parent"))?;
    let temp = parent.join(format!(".waku-{}.tmp", Uuid::new_v4()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let file = options
        .open(&temp)
        .with_context(|| format!("could not create {}", temp.display()))?;
    let mut writer = BufWriter::new(file);
    if let Err(error) = write(&mut writer).and_then(|()| {
        writer.flush()?;
        writer.get_ref().sync_all()?;
        Ok(())
    }) {
        let _ = fs::remove_file(&temp);
        return Err(error);
    }
    fs::rename(&temp, path)
        .with_context(|| format!("could not replace Grok history at {}", path.display()))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn truncates_chat_history_before_the_first_excluded_prompt() {
        let history = vec![
            json!({"type":"system"}),
            json!({"type":"user","prompt_index":0}),
            json!({"type":"assistant"}),
            json!({"type":"user","synthetic_reason":"reminder"}),
            json!({"type":"user","prompt_index":1}),
            json!({"type":"assistant"}),
        ];
        let truncated = truncate_at_turn(&history, 1, is_chat_prompt).unwrap();
        assert_eq!(truncated.len(), 4);
        assert_eq!(truncated.last().unwrap()["synthetic_reason"], "reminder");
    }

    #[test]
    fn truncates_native_updates_at_the_matching_user_chunk() {
        let update = |kind| json!({"params":{"update":{"sessionUpdate":kind}}});
        let history = vec![
            update("hook_execution"),
            update("user_message_chunk"),
            update("agent_message_chunk"),
            update("turn_completed"),
            update("user_message_chunk"),
            update("agent_message_chunk"),
        ];
        let truncated = truncate_at_turn(&history, 1, is_update_prompt).unwrap();
        assert_eq!(truncated.len(), 4);
    }

    #[test]
    fn rejects_a_checkpoint_beyond_groks_history() {
        let error = truncate_at_turn(
            &[json!({"type":"user","prompt_index":0})],
            2,
            is_chat_prompt,
        )
        .unwrap_err();
        assert!(error.to_string().contains("only 1 native turns"));
    }
}
