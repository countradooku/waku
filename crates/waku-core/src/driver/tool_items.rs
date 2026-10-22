//! Tool calls as turn items (RFC 0002: `Activity` / `RichActivity` →
//! `TurnItemUpdated` with the tool variant from `activity.rs`).
//!
//! Drivers ported to `DriverV2` describe a tool by its native name, input
//! and output. The shared activity normalizer formats those once (argument
//! JSON, output text, file-change diffs, the compact display target), and
//! this module picks the turn-item variant that carries them, so every port
//! classifies tools the same way the v1 transcript did.

use serde_json::Value;
use waku_protocol::model::{ActivityFileChangeStatus, ActivityKind};
use waku_protocol::orchestration::{FileChangeDetail, JsonText, TurnItemKind};

use super::activity;

/// A normalized tool row: its title and turn-item kind.
pub(crate) struct ToolItem {
    pub title: String,
    pub kind: TurnItemKind,
}

/// Describes one tool call. `output` is `None` while the call runs.
pub(crate) fn tool_item(
    name: &str,
    title: Option<String>,
    input: Option<&Value>,
    output: Option<&Value>,
    failed: bool,
    exit_code: Option<i32>,
) -> ToolItem {
    let kind = ActivityKind::from_tool_name(name);
    let title = title
        .or_else(|| activity::input_title(input))
        .unwrap_or_else(|| name.to_owned());
    let item = activity::tool_activity(
        None,
        kind,
        title,
        input,
        output,
        None,
        failed,
        output.is_some(),
    );
    let text_input = || {
        input
            .and_then(|input| {
                ["command", "cmd", "script"]
                    .iter()
                    .find_map(|key| input.get(*key).and_then(Value::as_str))
            })
            .map(str::to_owned)
            .or_else(|| item.display_target.clone())
            .unwrap_or_else(|| item.title.clone())
    };
    let kind = match kind {
        ActivityKind::Command => TurnItemKind::CommandExecution {
            input: text_input(),
            output: item.output.clone(),
            output_truncated: false,
            output_omitted: false,
            output_indicates_failure: failed || exit_code.is_some_and(|code| code != 0),
            exit_code,
        },
        ActivityKind::FileChange
            if !item.file_changes.is_empty() || item.display_target.is_some() =>
        {
            let first = item.file_changes.first();
            let diff = item
                .file_changes
                .iter()
                .filter_map(|change| change.diff.as_deref())
                .collect::<Vec<_>>()
                .join("\n");
            TurnItemKind::FileChange {
                file_name: first
                    .map(|change| change.path.clone())
                    .or_else(|| item.display_target.clone())
                    .unwrap_or_else(|| item.title.clone()),
                additions: first
                    .and_then(|change| change.additions)
                    .and_then(|count| u32::try_from(count).ok()),
                deletions: first
                    .and_then(|change| change.deletions)
                    .and_then(|count| u32::try_from(count).ok()),
                diff: (!diff.is_empty()).then_some(diff),
                old_text: None,
                new_text: None,
                changes: item
                    .file_changes
                    .iter()
                    .map(|change| FileChangeDetail {
                        operation: match change.status {
                            Some(ActivityFileChangeStatus::Added) => "add",
                            Some(ActivityFileChangeStatus::Deleted) => "delete",
                            _ => "update",
                        }
                        .to_owned(),
                        path: change.path.clone(),
                        old_path: None,
                        file_type: None,
                        mime_type: None,
                    })
                    .collect(),
                body_omitted: false,
            }
        }
        ActivityKind::FileSearch | ActivityKind::FileList => TurnItemKind::FileSearch {
            pattern: item.display_target.clone(),
            results: Vec::new(),
        },
        ActivityKind::Search => TurnItemKind::WebSearch {
            patterns: item.display_target.clone().into_iter().collect(),
            results: Vec::new(),
        },
        _ => TurnItemKind::DynamicTool {
            tool_name: Some(name.to_owned()),
            input: JsonText::new(
                input
                    .filter(|input| !input.is_null())
                    .map(Value::to_string)
                    .unwrap_or_else(|| "{}".to_owned()),
            ),
            output: item
                .output
                .as_ref()
                .map(|output| JsonText::new(Value::String(output.clone()).to_string())),
            output_omitted: false,
            viewed_image_path: None,
        },
    };
    ToolItem {
        title: item.title,
        kind,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn tools_map_to_the_turn_item_their_name_implies() {
        let bash = tool_item(
            "bash",
            None,
            Some(&json!({"command": "ls"})),
            Some(&json!("README.md\n")),
            false,
            Some(0),
        );
        assert!(matches!(
            bash.kind,
            TurnItemKind::CommandExecution { ref input, ref output, exit_code: Some(0), .. }
                if input == "ls" && output.as_deref() == Some("README.md")
        ));
        let read = tool_item(
            "read",
            None,
            Some(&json!({"filePath": "/w/a.rs"})),
            None,
            false,
            None,
        );
        assert!(matches!(read.kind, TurnItemKind::DynamicTool { .. }));
        let grep = tool_item(
            "grep",
            None,
            Some(&json!({"pattern": "fn main"})),
            None,
            false,
            None,
        );
        assert!(matches!(grep.kind, TurnItemKind::FileSearch { .. }));
    }
}
