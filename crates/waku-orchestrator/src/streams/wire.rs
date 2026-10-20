//! Wire projection (#57, RFC 0003 §5.4): what a turn item looks like on a
//! stream, in a snapshot or in a history page.
//!
//! Large bodies stay off the streams: command output, dynamic-tool results
//! and inline file-change bodies are omitted (`outputOmitted`,
//! `bodyOmitted`) even when small, so stream bytes do not depend on tool
//! output size; dynamic values over [`WIRE_VALUE_BYTES`] become a summary of
//! at most [`WIRE_SUMMARY_CHARS`] characters; the handoff summary is
//! stripped. Persisted events stay complete; `threads.turnItem` serves the
//! full item, with images inside tool output moved to the blob store
//! ([`externalize_images`]).

use std::borrow::Cow;

use waku_protocol::orchestration::{DomainEvent, ItemStatus, JsonText, TurnItem, TurnItemKind};
use waku_protocol::v8::orchestration::{WIRE_SUMMARY_CHARS, WIRE_VALUE_BYTES};

/// The first line of `text`, at most [`WIRE_SUMMARY_CHARS`] characters, with
/// `…` when anything was cut.
pub fn summary(text: &str) -> String {
    let first = text
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("");
    let first = first.trim();
    let mut out: String = first.chars().take(WIRE_SUMMARY_CHARS - 1).collect();
    if out.len() < text.trim().len() {
        out.push('…');
    }
    out
}

fn summarize_long(value: &mut String) -> bool {
    if value.len() > WIRE_VALUE_BYTES {
        *value = summary(value);
        true
    } else {
        false
    }
}

fn summarize_long_option(value: &mut Option<String>) {
    if let Some(text) = value {
        summarize_long(text);
    }
}

/// `item` as streams carry it.
pub fn project_turn_item(item: &TurnItem) -> TurnItem {
    let mut item = item.clone();
    let status = item.status;
    match &mut item.kind {
        TurnItemKind::CommandExecution {
            input,
            output,
            output_omitted,
            output_indicates_failure,
            exit_code,
            ..
        } => {
            if output.take().is_some() {
                *output_omitted = true;
            }
            *output_indicates_failure |=
                exit_code.is_some_and(|code| code != 0) || status == ItemStatus::Failed;
            if summarize_long(input) {
                *output_omitted = true;
            }
        }
        TurnItemKind::DynamicTool {
            input,
            output,
            output_omitted,
            ..
        } => {
            if output.take().is_some() {
                *output_omitted = true;
            }
            if input.0.len() > WIRE_VALUE_BYTES {
                // Still valid JSON: a string holding the summary.
                *input = JsonText(
                    serde_json::to_string(&summary(&input.0)).expect("a string serializes"),
                );
                *output_omitted = true;
            }
        }
        TurnItemKind::FileChange {
            diff,
            old_text,
            new_text,
            body_omitted,
            ..
        } => {
            let had_body = diff.take().is_some() | old_text.take().is_some();
            if had_body | new_text.take().is_some() {
                *body_omitted = true;
            }
        }
        TurnItemKind::Handoff { summary, .. } => *summary = None,
        TurnItemKind::Subagent {
            progress, result, ..
        } => {
            summarize_long_option(progress);
            summarize_long_option(result);
        }
        TurnItemKind::Compaction { summary, .. } => summarize_long_option(summary),
        TurnItemKind::Notification(notification) => {
            summarize_long_option(&mut notification.detail);
        }
        _ => {}
    }
    item
}

/// `event` as streams carry it: only turn items change.
pub fn project_event(event: &DomainEvent) -> Cow<'_, DomainEvent> {
    match event {
        DomainEvent::TurnItemUpdated(item) => {
            Cow::Owned(DomainEvent::TurnItemUpdated(project_turn_item(item)))
        }
        other => Cow::Borrowed(other),
    }
}

/// Moves base64 images out of a tool's JSON output. `store(bytes, mime)`
/// puts one image in the blob store and returns its `waku-blob:` reference;
/// each image becomes that reference, so the result never carries a data
/// URL. Returns `None` when nothing changed (or the text is not JSON).
///
/// Recognized shapes: MCP image content (`{"type": "image", "data": <base64>,
/// "mimeType": …}`, which gains `"blob"` and loses `"data"`) and `data:image/…;
/// base64,…` strings anywhere.
pub fn externalize_images(
    json: &str,
    store: &mut dyn FnMut(&[u8], &str) -> Option<String>,
) -> Option<String> {
    use base64::Engine as _;
    use serde_json::Value;

    fn decode(data: &str) -> Option<Vec<u8>> {
        base64::engine::general_purpose::STANDARD
            .decode(data.trim())
            .ok()
    }

    fn walk(value: &mut Value, store: &mut dyn FnMut(&[u8], &str) -> Option<String>) -> bool {
        match value {
            Value::String(text) => {
                let Some(rest) = text.strip_prefix("data:image/") else {
                    return false;
                };
                let Some((subtype, data)) = rest.split_once(";base64,") else {
                    return false;
                };
                let mime = format!("image/{subtype}");
                let Some(reference) = decode(data).and_then(|bytes| store(&bytes, &mime)) else {
                    return false;
                };
                *text = reference;
                true
            }
            Value::Array(items) => {
                let mut changed = false;
                for item in items {
                    changed |= walk(item, store);
                }
                changed
            }
            Value::Object(map) => {
                let is_image = map.get("type").and_then(Value::as_str) == Some("image");
                let mut changed = false;
                if is_image && let Some(Value::String(data)) = map.get("data") {
                    let mime = map
                        .get("mimeType")
                        .or_else(|| map.get("mime_type"))
                        .and_then(Value::as_str)
                        .unwrap_or("image/png")
                        .to_owned();
                    if let Some(reference) = decode(data).and_then(|bytes| store(&bytes, &mime)) {
                        map.remove("data");
                        map.insert("blob".into(), Value::String(reference));
                        changed = true;
                    }
                }
                for item in map.values_mut() {
                    changed |= walk(item, store);
                }
                changed
            }
            _ => false,
        }
    }

    let mut value: Value = serde_json::from_str(json).ok()?;
    walk(&mut value, store).then(|| value.to_string())
}

/// `item` with the images in its tool output moved to the blob store (see
/// [`externalize_images`]).
pub fn externalize_item_images(
    item: &mut TurnItem,
    store: &mut dyn FnMut(&[u8], &str) -> Option<String>,
) {
    if let TurnItemKind::DynamicTool {
        output: Some(output),
        ..
    } = &mut item.kind
        && let Some(rewritten) = externalize_images(&output.0, store)
    {
        output.0 = rewritten;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use waku_protocol::orchestration::{ThreadId, TurnItemId};

    fn item(kind: TurnItemKind, status: ItemStatus) -> TurnItem {
        TurnItem {
            id: TurnItemId(uuid::Uuid::from_u128(1)),
            thread_id: ThreadId(uuid::Uuid::from_u128(2)),
            run_id: None,
            node_id: None,
            provider_thread_id: None,
            provider_turn_id: None,
            native_item_ref: None,
            parent_item_id: None,
            position: 7,
            status,
            title: None,
            tool: None,
            stream_rev: 0,
            started_at: None,
            completed_at: None,
            updated_at: 1,
            kind,
        }
    }

    #[test]
    fn outputs_and_bodies_are_omitted_even_when_small() {
        let command = item(
            TurnItemKind::CommandExecution {
                input: "ls".into(),
                output: Some("a\nb".into()),
                output_truncated: false,
                output_omitted: false,
                output_indicates_failure: false,
                exit_code: Some(2),
            },
            ItemStatus::Completed,
        );
        let TurnItemKind::CommandExecution {
            input,
            output,
            output_omitted,
            output_indicates_failure,
            ..
        } = project_turn_item(&command).kind
        else {
            unreachable!()
        };
        assert_eq!(
            (
                input.as_str(),
                output,
                output_omitted,
                output_indicates_failure
            ),
            ("ls", None, true, true)
        );

        let change = item(
            TurnItemKind::FileChange {
                file_name: "a.rs".into(),
                additions: Some(1),
                deletions: None,
                diff: Some("+x".into()),
                old_text: None,
                new_text: Some("x".into()),
                changes: Vec::new(),
                body_omitted: false,
            },
            ItemStatus::Completed,
        );
        let TurnItemKind::FileChange {
            diff,
            new_text,
            body_omitted,
            additions,
            ..
        } = project_turn_item(&change).kind
        else {
            unreachable!()
        };
        assert_eq!(
            (diff, new_text, body_omitted, additions),
            (None, None, true, Some(1))
        );

        let tool = item(
            TurnItemKind::DynamicTool {
                tool_name: Some("search".into()),
                input: JsonText(format!("{{\"q\": \"{}\"}}", "x".repeat(10_000))),
                output: Some(JsonText("{}".into())),
                output_omitted: false,
                viewed_image_path: None,
            },
            ItemStatus::Running,
        );
        let TurnItemKind::DynamicTool {
            input,
            output,
            output_omitted,
            ..
        } = project_turn_item(&tool).kind
        else {
            unreachable!()
        };
        assert!(output.is_none() && output_omitted);
        let summary: String = serde_json::from_str(&input.0).unwrap();
        assert_eq!(summary.chars().count(), WIRE_SUMMARY_CHARS);
        assert!(summary.ends_with('…'));
    }

    #[test]
    fn untouched_kinds_stay_equal() {
        let text = item(
            TurnItemKind::AssistantMessage {
                message_id: waku_protocol::orchestration::MessageId(uuid::Uuid::nil()),
                text: "x".repeat(20_000),
                streaming: false,
            },
            ItemStatus::Completed,
        );
        assert_eq!(project_turn_item(&text), text);
        let event = DomainEvent::TurnItemUpdated(text);
        assert!(matches!(project_event(&event), Cow::Owned(_)));
    }

    #[test]
    fn images_move_to_the_blob_store() {
        let png = base64::engine::general_purpose::STANDARD.encode(b"\x89PNG");
        use base64::Engine as _;
        let json = format!(
            r#"{{"content":[{{"type":"image","data":"{png}","mimeType":"image/png"}},{{"type":"text","text":"data:image/jpeg;base64,{png}"}}]}}"#
        );
        let mut stored = Vec::new();
        let rewritten = externalize_images(&json, &mut |bytes, mime| {
            stored.push((bytes.to_vec(), mime.to_owned()));
            Some(format!("waku-blob:{}.png", stored.len()))
        })
        .unwrap();
        assert!(!rewritten.contains(&png), "{rewritten}");
        assert!(
            rewritten.contains(r#""blob":"waku-blob:1.png""#),
            "{rewritten}"
        );
        assert!(
            rewritten.contains(r#""text":"waku-blob:2.png""#),
            "{rewritten}"
        );
        assert_eq!(stored[0], (b"\x89PNG".to_vec(), "image/png".to_owned()));
        assert_eq!(stored[1].1, "image/jpeg");
        assert_eq!(externalize_images("{\"a\": 1}", &mut |_, _| None), None);
    }
}
