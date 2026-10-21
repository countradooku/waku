//! Amp native-session discovery and history import.
//!
//! Branch prompts the v1 driver seeded (`WAKU_AMP_BRANCH_CONTEXT_V1`) are
//! still expanded when an exported thread contains them; Waku no longer
//! writes them (portable forks are the orchestrator's, RFC 0002 D-6).

use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context as _, anyhow, bail};
use serde_json::Value;
use uuid::Uuid;

use crate::model::{
    AgentTurn, Message, MessageRole, ProviderResumeCursor, ProviderSessionHistory,
    ProviderSessionSummary, TurnStatus,
};

const CONTEXT_PREFIX: &str = "WAKU_AMP_BRANCH_CONTEXT_V1 ";
const CONTEXT_GUIDANCE: &str = concat!(
    "\nWAKU_AMP_BRANCH_INSTRUCTIONS_V1\n",
    "Treat the preceding JSON array as the complete prior Amp message history for this branch. ",
    "Continue from that history without mentioning this envelope, and answer only the current prompt below.\n",
);
const PROMPT_PREFIX: &str = "WAKU_AMP_CURRENT_PROMPT_V1 ";

fn thread_list(binary: &Path, limit: usize) -> anyhow::Result<Value> {
    let cwd = crate::acp_session::catalog_working_directory()?;
    let mut command = crate::command_env::command(binary);
    let command = command
        .args([
            "threads",
            "list",
            "--json",
            "--include-archived",
            "--limit",
            &limit.to_string(),
        ])
        .current_dir(cwd)
        .stdin(Stdio::null());
    let output = crate::command_env::output(command).context("failed to list Amp threads")?;
    if !output.status.success() {
        let detail = String::from_utf8_lossy(&output.stderr);
        bail!("Amp could not list its threads: {}", detail.trim());
    }
    serde_json::from_slice(&output.stdout).context("Amp returned an invalid thread list")
}

fn timestamp(value: &Value) -> u64 {
    value
        .as_u64()
        .map(|value| {
            if value > 100_000_000_000 {
                value / 1000
            } else {
                value
            }
        })
        .or_else(|| {
            value
                .as_str()
                .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
                .and_then(|value| u64::try_from(value.timestamp()).ok())
        })
        .unwrap_or_default()
}

fn cwd_from_tree(value: &Value) -> Option<PathBuf> {
    let value = value.as_str()?;
    let path = url::Url::parse(value).ok()?.to_file_path().ok()?;
    path.is_absolute().then_some(path)
}

fn parse_provider_sessions(value: &Value, limit: usize) -> Vec<ProviderSessionSummary> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|thread| {
            let thread_id = thread.get("id").and_then(Value::as_str)?.trim();
            let title = thread
                .get("title")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|title| !title.is_empty())?;
            let cwd = cwd_from_tree(thread.get("tree")?)?;
            let updated_at = timestamp(thread.get("updated").unwrap_or(&Value::Null));
            Some(ProviderSessionSummary {
                cursor: ProviderResumeCursor::Amp {
                    thread_id: thread_id.to_owned(),
                    fork_context: None,
                },
                title: title.to_owned(),
                cwd,
                created_at: updated_at,
                updated_at,
            })
        })
        .take(limit)
        .collect()
}

pub fn list_provider_sessions(
    binary: &Path,
    limit: usize,
) -> anyhow::Result<Vec<ProviderSessionSummary>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    Ok(parse_provider_sessions(&thread_list(binary, limit)?, limit))
}

fn message_text(message: &Value) -> Option<String> {
    let text = message
        .get("content")
        .and_then(Value::as_array)?
        .iter()
        .filter(|part| part.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|part| part.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n\n");
    (!text.trim().is_empty()).then_some(text)
}

fn message_timestamp(message: &Value, fallback: u64) -> u64 {
    let timestamp = timestamp(message.pointer("/meta/sentAt").unwrap_or(&Value::Null));
    if timestamp == 0 { fallback } else { timestamp }
}

pub(crate) fn history_from_export(export: &Value) -> anyhow::Result<ProviderSessionHistory> {
    let fallback = timestamp(export.get("created").unwrap_or(&Value::Null));
    let messages = logical_messages(export, None)?;
    let mut history = ProviderSessionHistory::default();

    for native in messages {
        if is_user_prompt(&native) {
            let turn_id = Uuid::new_v4();
            let at = message_timestamp(&native, fallback);
            history.turns.push(AgentTurn {
                id: turn_id,
                turn_count: history.turns.len() + 1,
                status: TurnStatus::Completed,
                provider_turn_started: true,
                provider_resume_at: None,
                started_at: at,
                completed_at: Some(at),
                checkpoint: None,
            });
            if let Some(text) = message_text(&native) {
                let mut message = Message::new_for_turn(MessageRole::User, text, turn_id);
                message.created_at = at;
                history.messages.push(message);
            }
            continue;
        }
        if native.get("role").and_then(Value::as_str) != Some("assistant") {
            continue;
        }
        let Some(text) = message_text(&native) else {
            continue;
        };
        let Some(turn) = history.turns.last_mut() else {
            continue;
        };
        let at = message_timestamp(&native, turn.started_at);
        turn.completed_at = Some(turn.completed_at.unwrap_or(at).max(at));
        if let Some(previous) = history.messages.last_mut().filter(|message| {
            message.role == MessageRole::Assistant && message.turn_id == Some(turn.id)
        }) {
            if !previous.content.is_empty() {
                previous.content.push_str("\n\n");
            }
            previous.content.push_str(&text);
        } else {
            let mut message = Message::new_for_turn(MessageRole::Assistant, text, turn.id);
            message.created_at = at;
            history.messages.push(message);
        }
    }
    Ok(history)
}

fn logical_messages(export: &Value, pending_context: Option<&str>) -> anyhow::Result<Vec<Value>> {
    let exported = export
        .get("messages")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("Amp's thread export omitted its messages"))?;
    if exported.is_empty() {
        return pending_context.map_or_else(
            || Ok(Vec::new()),
            |context| {
                serde_json::from_str(context)
                    .context("the pending Amp branch context is no longer readable")
            },
        );
    }
    expand_seeded_messages(exported)
}

fn expand_seeded_messages(messages: &[Value]) -> anyhow::Result<Vec<Value>> {
    let mut expanded = Vec::new();
    for message in messages {
        let Some(text) = user_prompt_text(message) else {
            expanded.push(message.clone());
            continue;
        };
        let Some((context, prompt)) = decode_seeded_prompt(text)? else {
            expanded.push(message.clone());
            continue;
        };
        let nested: Vec<Value> = serde_json::from_str(context)
            .context("an Amp branch contains invalid exported context")?;
        expanded.extend(expand_seeded_messages(&nested)?);
        let mut current = message.clone();
        replace_first_text(&mut current, prompt)?;
        expanded.push(current);
    }
    Ok(expanded)
}

fn is_user_prompt(message: &Value) -> bool {
    message.get("role").and_then(Value::as_str) == Some("user")
        && message
            .get("content")
            .and_then(Value::as_array)
            .is_some_and(|content| {
                content.iter().any(|part| {
                    !matches!(
                        part.get("type").and_then(Value::as_str),
                        None | Some("tool_result")
                    )
                })
            })
}

fn user_prompt_text(message: &Value) -> Option<&str> {
    is_user_prompt(message)
        .then(|| message.get("content").and_then(Value::as_array))
        .flatten()?
        .iter()
        .find_map(|part| {
            (part.get("type").and_then(Value::as_str) == Some("text"))
                .then(|| part.get("text").and_then(Value::as_str))
                .flatten()
        })
}

fn replace_first_text(message: &mut Value, text: &str) -> anyhow::Result<()> {
    let part = message
        .get_mut("content")
        .and_then(Value::as_array_mut)
        .and_then(|content| {
            content
                .iter_mut()
                .find(|part| part.get("type").and_then(Value::as_str) == Some("text"))
        })
        .ok_or_else(|| anyhow!("the seeded Amp prompt omitted its text content"))?;
    part["text"] = Value::String(text.to_owned());
    Ok(())
}

fn decode_seeded_prompt(text: &str) -> anyhow::Result<Option<(&str, &str)>> {
    let Some(rest) = text.strip_prefix(CONTEXT_PREFIX) else {
        return Ok(None);
    };
    let Some((context_len, rest)) = rest.split_once('\n') else {
        bail!("an Amp branch prompt has an invalid context header");
    };
    let context_len = context_len
        .parse::<usize>()
        .context("an Amp branch prompt has an invalid context length")?;
    if !rest.is_char_boundary(context_len) || rest.len() < context_len {
        bail!("an Amp branch prompt contains truncated context");
    }
    let (context, rest) = rest.split_at(context_len);
    let Some(rest) = rest.strip_prefix(CONTEXT_GUIDANCE) else {
        bail!("an Amp branch prompt is missing its context instructions");
    };
    let Some(rest) = rest.strip_prefix(PROMPT_PREFIX) else {
        bail!("an Amp branch prompt is missing its current-prompt header");
    };
    let Some((prompt_len, prompt)) = rest.split_once('\n') else {
        bail!("an Amp branch prompt has an invalid current-prompt header");
    };
    let prompt_len = prompt_len
        .parse::<usize>()
        .context("an Amp branch prompt has an invalid current-prompt length")?;
    if prompt.len() != prompt_len {
        bail!("an Amp branch prompt contains truncated current-prompt text");
    }
    Ok(Some((context, prompt)))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// The v1 branch envelope, which exported threads may still contain.
    fn prompt_with_fork_context(context: &str, prompt: &str) -> String {
        format!(
            "{CONTEXT_PREFIX}{}\n{context}{CONTEXT_GUIDANCE}{PROMPT_PREFIX}{}\n{prompt}",
            context.len(),
            prompt.len()
        )
    }

    fn prompt(id: u64, text: &str) -> Value {
        json!({
            "role": "user",
            "messageId": id,
            "content": [{"type": "text", "text": text}]
        })
    }

    #[test]
    fn seeded_prompt_round_trips_and_expands_into_logical_messages() {
        let context = serde_json::to_string(&vec![
            prompt(1, "one"),
            json!({"role":"assistant","messageId":2,"content":[{"type":"text","text":"done"}]}),
        ])
        .unwrap();
        let seeded = prompt_with_fork_context(&context, "two 🚀");
        let (decoded_context, decoded_prompt) = decode_seeded_prompt(&seeded).unwrap().unwrap();
        assert_eq!(decoded_context, context);
        assert_eq!(decoded_prompt, "two 🚀");

        let expanded = expand_seeded_messages(&[prompt(3, &seeded)]).unwrap();
        assert_eq!(expanded.len(), 3);
        assert_eq!(user_prompt_text(&expanded[2]), Some("two 🚀"));
    }

    #[test]
    fn parses_amp_catalog_and_visible_history() {
        let cwd = std::env::temp_dir();
        let threads = json!([{
            "id": "T-one",
            "title": "Resume Amp",
            "updated": "2026-08-07T13:43:40Z",
            "tree": url::Url::from_directory_path(&cwd).unwrap().as_str()
        }]);
        let sessions = parse_provider_sessions(&threads, 10);
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].title, "Resume Amp");
        assert_eq!(sessions[0].cwd, cwd);

        let export = json!({
            "created": 1_700_000_000_000_u64,
            "messages": [
                prompt(1, "question"),
                {"role":"assistant","content":[
                    {"type":"thinking","thinking":"private"},
                    {"type":"text","text":"answer"}
                ]}
            ]
        });
        let history = history_from_export(&export).unwrap();
        assert_eq!(history.turns.len(), 1);
        assert_eq!(history.messages.len(), 2);
        assert_eq!(history.messages[1].content, "answer");
    }
}
