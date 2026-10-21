//! Grok native-session metadata: the session catalog and generated titles.
//! Fork and rollback moved into the ACP driver (`driver::acp::grok`, #48).

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, anyhow, bail};
use serde_json::Value;
use uuid::Uuid;

use crate::model::{ProviderResumeCursor, ProviderSessionSummary};

fn summary_timestamp(value: Option<&Value>) -> u64 {
    value
        .and_then(Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .and_then(|value| u64::try_from(value.timestamp()).ok())
        .unwrap_or_default()
}

fn provider_summary(value: &Value) -> Option<ProviderSessionSummary> {
    let kind = value.get("session_kind").and_then(Value::as_str);
    let hidden = value
        .get("hidden")
        .and_then(Value::as_bool)
        .unwrap_or_else(|| kind.is_some_and(|kind| kind.starts_with("subagent")));
    if hidden || kind == Some("headless") {
        return None;
    }
    let session_id = value
        .pointer("/info/id")
        .and_then(Value::as_str)
        .or_else(|| value.get("id").and_then(Value::as_str))?
        .trim();
    Uuid::parse_str(session_id).ok()?;
    let cwd = PathBuf::from(value.pointer("/info/cwd").and_then(Value::as_str)?);
    if !cwd.is_absolute() {
        return None;
    }
    let title = ["generated_title", "session_summary", "title"]
        .into_iter()
        .find_map(|key| {
            value
                .get(key)
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|title| !title.is_empty())
        })
        .map(str::to_owned)
        .unwrap_or_else(|| {
            cwd.file_name()
                .and_then(|name| name.to_str())
                .filter(|name| !name.is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| format!("Grok session {}", &session_id[..8]))
        });
    let created_at = summary_timestamp(value.get("created_at"));
    let updated_at = ["last_active_at", "updated_at", "created_at"]
        .into_iter()
        .filter_map(|key| value.get(key))
        .map(|value| summary_timestamp(Some(value)))
        .max()
        .unwrap_or(created_at)
        .max(created_at);
    Some(ProviderSessionSummary {
        cursor: ProviderResumeCursor::Grok {
            session_id: session_id.to_owned(),
        },
        title,
        cwd,
        created_at,
        updated_at,
    })
}

pub fn list_provider_sessions(limit: usize) -> anyhow::Result<Vec<ProviderSessionSummary>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let root = grok_home_directory()?.join("sessions");
    let mut sessions = Vec::new();
    let entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error).with_context(|| format!("could not read {}", root.display()));
        }
    };
    for workspace in entries.flatten() {
        let Ok(children) = fs::read_dir(workspace.path()) else {
            continue;
        };
        for child in children.flatten() {
            let path = child.path().join("summary.json");
            let Ok(bytes) = fs::read(path) else {
                continue;
            };
            let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
                continue;
            };
            if let Some(summary) = provider_summary(&value) {
                sessions.push(summary);
            }
        }
    }
    sessions.sort_by_key(|session| std::cmp::Reverse(session.updated_at));
    sessions.truncate(limit);
    Ok(sessions)
}

pub fn generated_title(session_id: &str) -> anyhow::Result<Option<String>> {
    generated_title_in(&grok_home_directory()?, session_id)
}

pub fn generated_title_in(grok_home: &Path, session_id: &str) -> anyhow::Result<Option<String>> {
    Uuid::parse_str(session_id).context("Grok returned an invalid session ID")?;
    let summary_path = find_session_directory_in(grok_home, session_id)?.join("summary.json");
    let summary: Value = serde_json::from_slice(
        &fs::read(&summary_path)
            .with_context(|| format!("could not read {}", summary_path.display()))?,
    )
    .context("Grok's session summary is invalid JSON")?;
    Ok(summary
        .get("generated_title")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|title| !title.is_empty())
        .map(str::to_owned))
}

fn grok_home_directory() -> anyhow::Result<PathBuf> {
    std::env::var_os("GROK_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| dirs::home_dir().map(|home| home.join(".grok")))
        .ok_or_else(|| anyhow!("Grok's home directory could not be located"))
}

fn find_session_directory_in(grok_home: &Path, session_id: &str) -> anyhow::Result<PathBuf> {
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn reads_groks_generated_title_from_native_metadata() {
        let root = std::env::temp_dir().join(format!("waku-grok-title-{}", Uuid::new_v4()));
        let session_id = Uuid::new_v4().to_string();
        let session = root
            .join("sessions")
            .join("%2Ftmp%2Fproject")
            .join(&session_id);
        fs::create_dir_all(&session).unwrap();
        fs::write(
            session.join("summary.json"),
            serde_json::to_vec(&json!({
                "generated_title": "  Fix provider task titles  ",
                "session_summary": "A much longer summary"
            }))
            .unwrap(),
        )
        .unwrap();

        assert_eq!(
            generated_title_in(&root, &session_id).unwrap().as_deref(),
            Some("Fix provider task titles")
        );
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn maps_visible_grok_summaries_and_hides_internal_sessions() {
        let session_id = Uuid::new_v4().to_string();
        let cwd = std::env::temp_dir().join("project");
        let summary = provider_summary(&json!({
            "info": {"id": session_id, "cwd": cwd},
            "generated_title": "Resume Grok",
            "session_summary": "fallback",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:01Z",
            "last_active_at": "2026-01-01T00:00:02Z"
        }))
        .unwrap();
        assert_eq!(summary.title, "Resume Grok");
        assert_eq!(summary.cwd, cwd);
        assert!(summary.updated_at > summary.created_at);

        for value in [
            json!({"info":{"id":Uuid::new_v4(),"cwd":std::env::temp_dir()},"session_kind":"headless"}),
            json!({"info":{"id":Uuid::new_v4(),"cwd":std::env::temp_dir()},"session_kind":"subagent_fork"}),
            json!({"info":{"id":Uuid::new_v4(),"cwd":std::env::temp_dir()},"hidden":true}),
        ] {
            assert!(provider_summary(&value).is_none());
        }
    }
}
