//! Helpers shared by the provider drivers: stderr triage, tool-name
//! classification and permission memory.

use std::collections::{HashMap, HashSet};

pub(super) fn provider_stderr_error(lines: Vec<String>) -> Option<String> {
    let first_error = lines
        .iter()
        .find(|line| line.to_ascii_lowercase().contains("error"))?
        .trim();

    // CLI parsers can echo a rejected multi-line argument in full. The first
    // diagnostic already identifies the failure; forwarding the rest would
    // turn provider stderr into an enormous assistant message.
    if first_error.to_ascii_lowercase().starts_with("error:") {
        return Some(truncate_error(first_error, 400));
    }

    let mut message = String::new();
    let first_error_index = lines
        .iter()
        .position(|line| line.trim() == first_error)
        .unwrap_or_default();
    for line in lines.iter().skip(first_error_index).take(6) {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if !message.is_empty() {
            message.push('\n');
        }
        message.push_str(line);
        if message.chars().count() >= 800 {
            break;
        }
    }
    Some(truncate_error(&message, 800))
}

fn truncate_error(message: &str, max_chars: usize) -> String {
    if message.chars().count() <= max_chars {
        return message.to_owned();
    }
    let mut truncated = message.chars().take(max_chars).collect::<String>();
    truncated.push('…');
    truncated
}

/// OpenCode's permission policy.
///
/// `permission_responses` translates every durable "always" choice into a
/// one-shot provider reply and keeps the rule in driver-local state. An
/// `always` reply would write into `/api/permission/saved`, a GLOBAL store
/// shared with the user's own terminal, so a Full Access Waku task would
/// silently disarm prompts in every other workspace and in the user's TUI.
/// `always` is never put on the wire.
#[derive(Clone, Debug)]
pub(super) struct OpenCodePermissionRequest {
    pub(super) permission: String,
    pub(super) patterns: Vec<String>,
    pub(super) always: Vec<String>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct OpenCodePermissionRule {
    permission: String,
    pattern: String,
}

#[derive(Default)]
pub(super) struct OpenCodePermissionState {
    pub(super) pending: HashMap<String, OpenCodePermissionRequest>,
    pub(super) responding: HashSet<String>,
    pub(super) approved: HashSet<OpenCodePermissionRule>,
}

impl OpenCodePermissionState {
    pub(super) fn is_approved(&self, request: &OpenCodePermissionRequest) -> bool {
        !request.patterns.is_empty()
            && request.patterns.iter().all(|pattern| {
                self.approved.iter().any(|rule| {
                    opencode_wildcard_matches(&request.permission, &rule.permission)
                        && opencode_wildcard_matches(pattern, &rule.pattern)
                })
            })
    }

    pub(super) fn remember(&mut self, request: &OpenCodePermissionRequest) {
        // Mirror OpenCode's own `always` handling exactly: only provider-
        // supplied reusable patterns become rules. An empty list deliberately
        // resolves the current request without broadening future access.
        self.approved
            .extend(request.always.iter().map(|pattern| OpenCodePermissionRule {
                permission: request.permission.clone(),
                pattern: pattern.clone(),
            }));
    }
}

fn opencode_wildcard_matches(input: &str, pattern: &str) -> bool {
    let input = input.replace('\\', "/");
    let pattern = pattern.replace('\\', "/");
    if pattern
        .strip_suffix(" *")
        .is_some_and(|prefix| input == prefix)
    {
        return true;
    }

    let input = input.chars().collect::<Vec<_>>();
    let mut previous = vec![false; input.len() + 1];
    previous[0] = true;
    for token in pattern.chars() {
        let mut current = vec![false; input.len() + 1];
        if token == '*' {
            current[0] = previous[0];
        }
        for index in 1..=input.len() {
            current[index] = match token {
                '*' => previous[index] || current[index - 1],
                '?' => previous[index - 1],
                literal => previous[index - 1] && literal == input[index - 1],
            };
        }
        previous = current;
    }
    previous[input.len()]
}

/// The provider replies one answer to a permission card produces. The driver
/// runs commands and events on one worker, so its permission state is
/// thread-local and there is nothing to serialize against.
pub(super) fn permission_responses(
    permissions: &mut OpenCodePermissionState,
    request_id: &str,
    option_id: &str,
) -> Vec<(String, String)> {
    let request = permissions.pending.remove(request_id);
    if option_id != "always" {
        permissions.responding.insert(request_id.to_owned());
        return vec![(request_id.to_owned(), option_id.to_owned())];
    }

    if let Some(request) = request.as_ref() {
        permissions.remember(request);
    }
    // OpenCode normally applies an `always` reply to other matching requests
    // already pending in the same session. Preserve that behavior locally,
    // but send every provider reply as one-shot so the shared server's cache
    // remains untouched.
    let additional = permissions
        .pending
        .iter()
        .filter(|(_, request)| permissions.is_approved(request))
        .map(|(request_id, _)| request_id.clone())
        .collect::<Vec<_>>();
    for request_id in &additional {
        permissions.pending.remove(request_id);
    }

    let responses = std::iter::once((request_id.to_owned(), "once".into()))
        .chain(
            additional
                .into_iter()
                .map(|request_id| (request_id, "once".into())),
        )
        .collect::<Vec<_>>();
    permissions
        .responding
        .extend(responses.iter().map(|(request_id, _)| request_id.clone()));
    responses
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ActivityKind;

    #[test]
    fn todo_tools_are_plans_not_file_writes() {
        assert_eq!(
            ActivityKind::from_tool_name("TodoWrite"),
            ActivityKind::Plan
        );
        assert_eq!(
            ActivityKind::from_tool_name("todo_write"),
            ActivityKind::Plan
        );
        assert_eq!(
            ActivityKind::from_tool_name("apply_patch"),
            ActivityKind::FileChange
        );
        assert_eq!(ActivityKind::from_tool_name("read"), ActivityKind::FileRead);
        assert_eq!(
            ActivityKind::from_tool_name("ReadFile"),
            ActivityKind::FileRead
        );
        assert_eq!(
            ActivityKind::from_tool_name("grep"),
            ActivityKind::FileSearch
        );
        assert_eq!(
            ActivityKind::from_tool_name("glob"),
            ActivityKind::FileSearch
        );
        assert_eq!(ActivityKind::from_tool_name("ls"), ActivityKind::FileList);
        assert_eq!(
            ActivityKind::from_tool_name("websearch"),
            ActivityKind::Search
        );
        assert_eq!(
            ActivityKind::from_tool_name("create_thread"),
            ActivityKind::Tool
        );
        assert_eq!(
            ActivityKind::from_tool_name("read_mcp_resource"),
            ActivityKind::Tool
        );
        assert_eq!(
            ActivityKind::from_tool_name("list_threads"),
            ActivityKind::Tool
        );
    }

    #[test]
    fn provider_stderr_keeps_cli_argument_errors_compact() {
        let message = provider_stderr_error(vec![
            "error: unexpected argument '---".into(),
            "name: waku-computer-use".into(),
            "description: a very long bundled skill".into(),
            "---' found".into(),
            "tip: to pass it as a value, use '-- ---'".into(),
        ]);

        assert_eq!(message.as_deref(), Some("error: unexpected argument '---"));
    }

    #[test]
    fn provider_stderr_ignores_non_error_diagnostics() {
        assert_eq!(
            provider_stderr_error(vec!["warning: optional integration unavailable".into()]),
            None
        );
    }
    #[test]
    fn always_without_provider_rules_does_not_broaden_future_access() {
        let mut permissions = OpenCodePermissionState::default();
        permissions.pending.insert(
            "per_once".into(),
            OpenCodePermissionRequest {
                permission: "bash".into(),
                patterns: vec!["cargo test".into()],
                always: Vec::new(),
            },
        );

        assert_eq!(
            permission_responses(&mut permissions, "per_once", "always"),
            [("per_once".into(), "once".into())]
        );
        assert!(permissions.approved.is_empty());
    }

    #[test]
    fn always_resolves_matching_requests_that_are_already_pending() {
        let mut permissions = OpenCodePermissionState::default();
        let request = |patterns: &[&str]| OpenCodePermissionRequest {
            permission: "bash".into(),
            patterns: patterns.iter().map(|pattern| (*pattern).into()).collect(),
            always: vec!["cargo *".into()],
        };
        permissions
            .pending
            .insert("per_first".into(), request(&["cargo test"]));
        permissions
            .pending
            .insert("per_matching".into(), request(&["cargo check"]));
        permissions
            .pending
            .insert("per_other".into(), request(&["git status"]));

        assert_eq!(
            permission_responses(&mut permissions, "per_first", "always"),
            [
                ("per_first".into(), "once".into()),
                ("per_matching".into(), "once".into()),
            ]
        );
        assert!(!permissions.pending.contains_key("per_matching"));
        assert!(permissions.pending.contains_key("per_other"));
    }
}
