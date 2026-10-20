//! Outbound frame matching (RFC 0002 "Matching").
//!
//! Frames compare as canonical JSON (sorted keys) after removing ignored
//! JSON Pointers from both sides and materializing placeholders:
//! `<workspace>` and `<home>` become the harness paths, `"<any>"` as a whole
//! value matches anything, and a string containing `<any>` is a glob. A
//! mismatch carries a unified diff of the two pretty-printed frames.

use std::path::Path;

use serde_json::{Map, Value};
use similar::TextDiff;

pub const ANY: &str = "<any>";
pub const WORKSPACE: &str = "<workspace>";
pub const HOME: &str = "<home>";

/// Paths that placeholders stand for.
#[derive(Clone, Debug)]
pub struct Placeholders {
    pub workspace: String,
    pub home: String,
}

impl Placeholders {
    pub fn new(workspace: &Path, home: &Path) -> Self {
        Self {
            workspace: workspace.display().to_string(),
            home: home.display().to_string(),
        }
    }

    /// Replaces `<workspace>` and `<home>` inside every string.
    pub fn materialize(&self, value: &Value) -> Value {
        match value {
            Value::String(text) => Value::String(
                text.replace(WORKSPACE, &self.workspace)
                    .replace(HOME, &self.home),
            ),
            Value::Array(items) => {
                Value::Array(items.iter().map(|item| self.materialize(item)).collect())
            }
            Value::Object(map) => Value::Object(
                map.iter()
                    .map(|(key, item)| (key.clone(), self.materialize(item)))
                    .collect(),
            ),
            other => other.clone(),
        }
    }
}

/// Removes the value a JSON Pointer names, if present.
pub fn remove_pointer(value: &mut Value, pointer: &str) {
    let Some((parent, last)) = pointer.rsplit_once('/') else {
        return;
    };
    let last = last.replace("~1", "/").replace("~0", "~");
    let target = if parent.is_empty() {
        Some(value)
    } else {
        value.pointer_mut(parent)
    };
    match target {
        Some(Value::Object(map)) => {
            map.remove(&last);
        }
        Some(Value::Array(items)) => {
            if let Ok(index) = last.parse::<usize>()
                && index < items.len()
            {
                items.remove(index);
            }
        }
        _ => {}
    }
}

/// Sorted keys at every level.
pub fn canonical(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort();
            let mut sorted = Map::new();
            for key in keys {
                sorted.insert(key.clone(), canonical(&map[key]));
            }
            Value::Object(sorted)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonical).collect()),
        other => other.clone(),
    }
}

/// Whether `actual` matches `expected` (placeholders already materialized).
pub fn matches(expected: &Value, actual: &Value) -> bool {
    match (expected, actual) {
        (Value::String(pattern), _) if pattern == ANY => true,
        (Value::String(pattern), Value::String(text)) if pattern.contains(ANY) => {
            glob(pattern, text)
        }
        (Value::Object(expected), Value::Object(actual)) => {
            expected.len() == actual.len()
                && expected
                    .iter()
                    .all(|(key, value)| actual.get(key).is_some_and(|other| matches(value, other)))
        }
        (Value::Array(expected), Value::Array(actual)) => {
            expected.len() == actual.len()
                && expected
                    .iter()
                    .zip(actual)
                    .all(|(expected, actual)| matches(expected, actual))
        }
        _ => expected == actual,
    }
}

/// `<any>` segments match any substring.
fn glob(pattern: &str, text: &str) -> bool {
    let parts = pattern.split(ANY).collect::<Vec<_>>();
    let mut rest = text;
    for (index, part) in parts.iter().enumerate() {
        if index == 0 {
            let Some(stripped) = rest.strip_prefix(part) else {
                return false;
            };
            rest = stripped;
        } else if index == parts.len() - 1 {
            return rest.ends_with(part);
        } else {
            let Some(found) = rest.find(part) else {
                return false;
            };
            rest = &rest[found + part.len()..];
        }
    }
    true
}

/// Prepares both sides for comparison: placeholders, ignored pointers,
/// canonical key order.
pub fn prepare(
    expected: &Value,
    actual: &Value,
    placeholders: &Placeholders,
    ignored: &[String],
) -> (Value, Value) {
    let mut expected = placeholders.materialize(expected);
    let mut actual = actual.clone();
    for pointer in ignored {
        remove_pointer(&mut expected, pointer);
        remove_pointer(&mut actual, pointer);
    }
    (canonical(&expected), canonical(&actual))
}

/// A unified diff of two pretty-printed canonical frames.
pub fn frame_diff(expected: &Value, actual: &Value) -> String {
    let expected = serde_json::to_string_pretty(&canonical(expected)).unwrap_or_default() + "\n";
    let actual = serde_json::to_string_pretty(&canonical(actual)).unwrap_or_default() + "\n";
    TextDiff::from_lines(&expected, &actual)
        .unified_diff()
        .context_radius(3)
        .header("expected", "actual")
        .to_string()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn placeholders_ignores_and_globs() {
        let placeholders = Placeholders::new(Path::new("/tmp/ws"), Path::new("/tmp/home"));
        let expected = json!({"params": {"cwd": "<workspace>/src", "token": "<any>", "id": "msg-<any>-x",
                                         "client": {"version": "1"}}, "id": 1});
        let actual = json!({"id": 1, "params": {"id": "msg-123-x", "token": {"a": 1}, "cwd": "/tmp/ws/src",
                                                "client": {"version": "2"}}});
        let (expected, actual) = prepare(
            &expected,
            &actual,
            &placeholders,
            &["/params/client/version".to_owned()],
        );
        assert!(matches(&expected, &actual));
        assert!(!matches(&json!({"id": 2}), &json!({"id": 1})));
        assert!(!matches(&json!({"a": "x-<any>"}), &json!({"a": "y-1"})));
        assert!(!matches(&json!({"a": 1}), &json!({"a": 1, "b": 2})));
    }

    #[test]
    fn a_mismatch_reads_as_a_unified_diff() {
        let diff = frame_diff(
            &json!({"method": "turn/start", "params": {"text": "hello"}}),
            &json!({"method": "turn/start", "params": {"text": "hullo"}}),
        );
        assert!(diff.contains("--- expected"), "{diff}");
        assert!(diff.contains("+++ actual"), "{diff}");
        assert!(diff.contains("-    \"text\": \"hello\""), "{diff}");
        assert!(diff.contains("+    \"text\": \"hullo\""), "{diff}");
    }
}
