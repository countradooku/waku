//! Model, reasoning-effort, service-tier, context-window and access-mode
//! selection for ACP sessions. ACP leaves option ids to the agent, so each
//! flavor's quirks live here: Cursor's parameterized model picker, Fx's
//! provider-scoped model option, Grok's `session/set_model` with
//! `_meta.reasoningEffort`, and Kimi's `thinking` config option.

use agent_client_protocol::schema::v1::{
    SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory,
    SessionConfigSelectOptions, SessionModeState,
};
use serde_json::{Value, json};
use waku_protocol::model_catalog::{
    CursorModelSelection, cursor_suffix_has, normalize_cursor_reasoning_effort,
    resolve_cursor_model,
};
use waku_protocol::orchestration::RuntimeMode;

use super::flavor::Flavor;
use super::protocol::{Connection, RpcError};

/// The selection fields an ACP session applies itself.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ModelChoice {
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub service_tier: Option<String>,
    pub context_window: Option<String>,
}

impl ModelChoice {
    pub fn from_selection(selection: &crate::driver::v2::Selection) -> Self {
        Self {
            model: selection.model.clone(),
            reasoning_effort: selection.reasoning_effort.clone(),
            service_tier: selection.service_tier.clone(),
            context_window: selection.context_window.clone(),
        }
    }
}

/// The access mode to switch a fresh or loaded session to, if any.
pub fn desired_access_mode(
    flavor: Flavor,
    modes: Option<&SessionModeState>,
    mode: RuntimeMode,
) -> Option<String> {
    let modes = modes?;
    let desired = if flavor == Flavor::Fx {
        let desired = if mode == RuntimeMode::Ask {
            "ask"
        } else {
            "code"
        };
        modes
            .available_modes
            .iter()
            .find(|mode| mode.id.to_string().eq_ignore_ascii_case(desired))?
            .id
            .to_string()
    } else {
        // Sessions created before the interaction toggle was removed may
        // retain the provider's read-only mode. Return only those sessions to
        // the provider's ordinary execution mode; otherwise leave externally
        // selected native modes untouched.
        if !modes
            .current_mode_id
            .to_string()
            .eq_ignore_ascii_case("plan")
        {
            return None;
        }
        modes
            .available_modes
            .iter()
            .find(|mode| {
                let id = mode.id.to_string();
                id.eq_ignore_ascii_case("agent") || id.eq_ignore_ascii_case("default")
            })?
            .id
            .to_string()
    };
    (modes.current_mode_id.to_string() != desired).then_some(desired)
}

/// Which session config option carries reasoning effort. ACP leaves the id to
/// the agent: Kimi Code exposes it as its `thinking` level, while the other
/// agents Waku drives keep it on `mode`. Grok does not use this path: its
/// effort rides on `session/set_model` as `_meta.reasoningEffort`.
fn reasoning_effort_config_id(flavor: Flavor) -> &'static str {
    match flavor {
        Flavor::Kimi => "thinking",
        _ => "mode",
    }
}

pub fn set_model_params(
    session_id: &str,
    model: &str,
    reasoning_effort: Option<&str>,
    flavor: Flavor,
) -> Value {
    let mut params = json!({"sessionId": session_id, "modelId": model});
    if flavor == Flavor::Grok
        && let Some(effort) = reasoning_effort.filter(|effort| !effort.is_empty())
    {
        params["_meta"] = json!({"reasoningEffort": effort});
    }
    params
}

fn config_options_of(response: &Value) -> Vec<SessionConfigOption> {
    response
        .get("configOptions")
        .cloned()
        .and_then(|options| serde_json::from_value(options).ok())
        .unwrap_or_default()
}

async fn set_config_option(
    connection: &Connection,
    session_id: &str,
    config_id: &str,
    value: Value,
) -> Result<Vec<SessionConfigOption>, RpcError> {
    // A boolean travels as `{"type":"boolean","value":…}`, a select value id
    // as a bare `value` (the schema's flattened `SessionConfigOptionValue`).
    let mut params = json!({"sessionId": session_id, "configId": config_id, "value": value});
    if params["value"].is_boolean() {
        params["type"] = json!("boolean");
    }
    let response = connection
        .request("session/set_config_option", params)
        .await?;
    Ok(config_options_of(&response))
}

fn flag_request_value(option: &SessionConfigOption, enabled: bool) -> Option<Value> {
    match &option.kind {
        SessionConfigKind::Boolean(_) => Some(Value::Bool(enabled)),
        SessionConfigKind::Select(_) => {
            let value = if enabled { "true" } else { "false" };
            session_config_select_values(option)
                .contains(&value)
                .then(|| Value::String(value.to_owned()))
        }
        _ => None,
    }
}

async fn apply_cursor_variant_configs(
    connection: &Connection,
    session_id: &str,
    mut options: Vec<SessionConfigOption>,
    selection: &CursorModelSelection,
    choice: &ModelChoice,
) -> Result<(), RpcError> {
    let reasoning_effort = choice.reasoning_effort.as_deref();
    // Thinking can reveal a thought-level option, so apply it first and use
    // each response's refreshed option set for the next selection.
    for target in ["thinking", "effort", "context", "fast"] {
        let Some(option) = (match target {
            "thinking" => options
                .iter()
                .find(|option| is_cursor_thinking_option(option)),
            "effort" => find_cursor_effort_option(&options),
            "context" => options
                .iter()
                .find(|option| is_cursor_context_option(option)),
            "fast" => options.iter().find(|option| is_cursor_fast_option(option)),
            _ => None,
        })
        .cloned() else {
            continue;
        };
        let value = match target {
            "thinking" => {
                let Some(enabled) = cursor_desired_thinking(selection, reasoning_effort) else {
                    continue;
                };
                if cursor_flag_matches(&option, enabled) {
                    continue;
                }
                let Some(value) = flag_request_value(&option, enabled) else {
                    continue;
                };
                value
            }
            "effort" => {
                let Some(value) = cursor_desired_effort_value(&option, selection, reasoning_effort)
                else {
                    continue;
                };
                if session_config_current_value(&option) == Some(value.as_str()) {
                    continue;
                }
                Value::String(value)
            }
            "context" => {
                let Some(value) =
                    cursor_desired_context_value(&option, choice.context_window.as_deref())
                else {
                    continue;
                };
                if session_config_current_value(&option) == Some(value.as_str()) {
                    continue;
                }
                Value::String(value)
            }
            "fast" => {
                let Some(enabled) = cursor_desired_fast(selection, choice.service_tier.as_deref())
                else {
                    continue;
                };
                if cursor_flag_matches(&option, enabled) {
                    continue;
                }
                let Some(value) = flag_request_value(&option, enabled) else {
                    continue;
                };
                value
            }
            _ => continue,
        };
        options = set_config_option(connection, session_id, &option.id.to_string(), value).await?;
    }
    Ok(())
}

/// Applies a model choice to a live session. `Err` carries a user-safe reason.
pub async fn apply_model(
    connection: &Connection,
    flavor: Flavor,
    session_id: &str,
    config_options: &[SessionConfigOption],
    choice: &ModelChoice,
) -> Result<(), String> {
    let Some(model) = choice.model.as_deref() else {
        return Ok(());
    };
    let describe = |error: RpcError| error.detail();
    let cursor_model_option = (flavor == Flavor::Cursor)
        .then(|| find_config_option(config_options, SessionConfigOptionCategory::Model))
        .flatten();
    if let Some(option) = cursor_model_option
        && let Some(selection) = cursor_model_selection(option, model)
    {
        let options = set_config_option(
            connection,
            session_id,
            &option.id.to_string(),
            Value::String(selection.value.clone()),
        )
        .await
        .map_err(describe)?;
        return apply_cursor_variant_configs(connection, session_id, options, &selection, choice)
            .await
            .map_err(describe);
    }

    if flavor == Flavor::Fx {
        let mut options = config_options.to_vec();
        if let Some((provider_option, value)) = fx_model_provider_switch(&options, model) {
            let config_id = provider_option.id.to_string();
            options = set_config_option(
                connection,
                session_id,
                &config_id,
                Value::String(value.to_owned()),
            )
            .await
            .map_err(describe)?;
        }
        let Some(option) = fx_model_option(&options) else {
            return Err("Fx did not advertise its model configuration".to_owned());
        };
        if !session_config_select_values(option).contains(&model) {
            return Err(format!("Fx did not advertise model {model}"));
        }
        set_config_option(
            connection,
            session_id,
            &option.id.to_string(),
            Value::String(model.to_owned()),
        )
        .await
        .map_err(describe)?;
        return Ok(());
    }

    // Grok, Kimi, generic agents and Cursor agents that do not advertise a
    // model config option use `session/set_model`. Fx intentionally stays on
    // session/set_config_option, its documented model API.
    connection
        .request(
            "session/set_model",
            set_model_params(
                session_id,
                model,
                choice.reasoning_effort.as_deref(),
                flavor,
            ),
        )
        .await
        .map_err(describe)?;
    if flavor != Flavor::Grok
        && let Some(effort) = choice.reasoning_effort.as_deref()
    {
        // Reasoning effort is an optional config extension and is deliberately
        // non-fatal when an agent does not expose it.
        let _ = set_config_option(
            connection,
            session_id,
            reasoning_effort_config_id(flavor),
            Value::String(effort.to_owned()),
        )
        .await;
    }
    Ok(())
}

fn session_config_select_values(option: &SessionConfigOption) -> Vec<&str> {
    let SessionConfigKind::Select(select) = &option.kind else {
        return Vec::new();
    };
    match &select.options {
        SessionConfigSelectOptions::Ungrouped(options) => options
            .iter()
            .map(|option| option.value.0.as_ref())
            .collect(),
        SessionConfigSelectOptions::Grouped(groups) => groups
            .iter()
            .flat_map(|group| group.options.iter())
            .map(|option| option.value.0.as_ref())
            .collect(),
        _ => Vec::new(),
    }
}

fn cursor_model_selection(
    option: &SessionConfigOption,
    requested: &str,
) -> Option<CursorModelSelection> {
    resolve_cursor_model(session_config_select_values(option), requested)
}

fn cursor_option_id(option: &SessionConfigOption) -> String {
    option.id.to_string().to_ascii_lowercase()
}

fn cursor_option_name(option: &SessionConfigOption) -> String {
    option.name.to_ascii_lowercase()
}

fn is_cursor_thinking_option(option: &SessionConfigOption) -> bool {
    option.category == Some(SessionConfigOptionCategory::ModelConfig) && {
        let id = cursor_option_id(option);
        let name = cursor_option_name(option);
        id == "thinking" || name.contains("thinking")
    }
}

fn is_cursor_fast_option(option: &SessionConfigOption) -> bool {
    option.category == Some(SessionConfigOptionCategory::ModelConfig) && {
        let id = cursor_option_id(option);
        let name = cursor_option_name(option);
        id == "fast" || name == "fast" || name.contains("fast mode")
    }
}

fn is_cursor_context_option(option: &SessionConfigOption) -> bool {
    option.category == Some(SessionConfigOptionCategory::ModelConfig) && {
        let id = cursor_option_id(option);
        let name = cursor_option_name(option);
        id == "context" || id == "context_size" || name.contains("context")
    }
}

fn is_cursor_effort_option(option: &SessionConfigOption) -> bool {
    if !matches!(option.kind, SessionConfigKind::Select(_)) {
        return false;
    }
    let id = cursor_option_id(option);
    let name = cursor_option_name(option);
    id == "effort"
        || id == "reasoning"
        || name == "effort"
        || name == "reasoning"
        || name.contains("effort")
        || name.contains("reasoning")
}

fn find_cursor_effort_option(options: &[SessionConfigOption]) -> Option<&SessionConfigOption> {
    let candidates: Vec<&SessionConfigOption> = options
        .iter()
        .filter(|option| is_cursor_effort_option(option))
        .collect();
    candidates
        .iter()
        .copied()
        .find(|option| {
            matches!(
                option.category.as_ref(),
                Some(SessionConfigOptionCategory::Other(value))
                    if value.eq_ignore_ascii_case("model_option")
            )
        })
        .or_else(|| {
            candidates
                .iter()
                .copied()
                .find(|option| option.id.to_string().eq_ignore_ascii_case("effort"))
        })
        .or_else(|| {
            candidates
                .iter()
                .copied()
                .find(|option| option.category == Some(SessionConfigOptionCategory::ThoughtLevel))
        })
        .or_else(|| candidates.first().copied())
}

fn cursor_matching_select_value<'a>(values: &[&'a str], requested: &str) -> Option<&'a str> {
    let normalized = normalize_cursor_reasoning_effort(requested);
    values
        .iter()
        .find(|value| normalize_cursor_reasoning_effort(value) == normalized)
        .copied()
}

fn cursor_desired_effort_value(
    option: &SessionConfigOption,
    selection: &CursorModelSelection,
    reasoning_effort: Option<&str>,
) -> Option<String> {
    let values = session_config_select_values(option);
    if let Some(effort) = reasoning_effort.filter(|effort| !effort.is_empty())
        && let Some(value) = cursor_matching_select_value(&values, effort)
    {
        return Some(value.to_owned());
    }
    if selection.suffix.contains("extra-high")
        && let Some(value) = values
            .iter()
            .find(|value| normalize_cursor_reasoning_effort(value) == "xhigh")
    {
        return Some((*value).to_owned());
    }
    values
        .iter()
        .find(|value| cursor_suffix_has(&selection.suffix, value))
        .map(|value| (*value).to_owned())
}

fn cursor_desired_thinking(
    selection: &CursorModelSelection,
    reasoning_effort: Option<&str>,
) -> Option<bool> {
    if let Some(effort) = reasoning_effort {
        return Some(normalize_cursor_reasoning_effort(effort) != "none");
    }
    cursor_suffix_has(&selection.suffix, "thinking").then_some(true)
}

fn cursor_desired_fast(
    selection: &CursorModelSelection,
    service_tier: Option<&str>,
) -> Option<bool> {
    match service_tier {
        Some("fast") => Some(true),
        Some(_) => Some(false),
        None if cursor_suffix_has(&selection.suffix, "fast") => Some(true),
        None => None,
    }
}

fn cursor_desired_context_value(
    option: &SessionConfigOption,
    context_window: Option<&str>,
) -> Option<String> {
    let requested = context_window.filter(|value| !value.is_empty())?;
    let values = session_config_select_values(option);
    let normalized = requested.replace(['_', ' '], "-");
    values
        .iter()
        .find(|value| {
            value.eq_ignore_ascii_case(requested)
                || value
                    .replace(['_', ' '], "-")
                    .eq_ignore_ascii_case(&normalized)
        })
        .map(|value| (*value).to_owned())
}

fn cursor_flag_matches(option: &SessionConfigOption, enabled: bool) -> bool {
    match &option.kind {
        SessionConfigKind::Boolean(boolean) => boolean.current_value == enabled,
        SessionConfigKind::Select(_) => {
            let value = if enabled { "true" } else { "false" };
            session_config_current_value(option) == Some(value)
        }
        _ => false,
    }
}

fn session_config_current_value(option: &SessionConfigOption) -> Option<&str> {
    let SessionConfigKind::Select(select) = &option.kind else {
        return None;
    };
    Some(select.current_value.0.as_ref())
}

fn find_config_option(
    config_options: &[SessionConfigOption],
    category: SessionConfigOptionCategory,
) -> Option<&SessionConfigOption> {
    config_options
        .iter()
        .find(|option| option.category.as_ref() == Some(&category))
}

fn fx_model_option(config_options: &[SessionConfigOption]) -> Option<&SessionConfigOption> {
    config_options.iter().find(|option| {
        option.category == Some(SessionConfigOptionCategory::Model)
            && option.id.to_string().eq_ignore_ascii_case("model")
    })
}

fn fx_model_provider_switch<'a>(
    config_options: &'a [SessionConfigOption],
    model: &str,
) -> Option<(&'a SessionConfigOption, &'static str)> {
    if fx_model_option(config_options)
        .is_some_and(|option| session_config_select_values(option).contains(&model))
    {
        return None;
    }
    // Fx scopes model options to the selected account route. AI Gateway IDs
    // are provider/model pairs, while subscription IDs are flat. Selecting the
    // Gateway route returns a refreshed model option that contains these IDs.
    if !model.contains('/') {
        return None;
    }
    let provider = config_options.iter().find(|option| {
        option.category == Some(SessionConfigOptionCategory::Model)
            && option.id.to_string().eq_ignore_ascii_case("provider")
    })?;
    (session_config_current_value(provider) != Some("gateway")
        && session_config_select_values(provider).contains(&"gateway"))
    .then_some((provider, "gateway"))
}

#[cfg(test)]
mod tests {
    use agent_client_protocol::schema::v1::{SessionConfigSelectOption, SessionMode};

    use super::*;

    fn select_config_option(
        id: &str,
        category: SessionConfigOptionCategory,
        current: &str,
        values: &[&str],
    ) -> SessionConfigOption {
        SessionConfigOption::select(
            id.to_owned(),
            id.to_owned(),
            current.to_owned(),
            values
                .iter()
                .map(|value| SessionConfigSelectOption::new((*value).to_owned(), *value))
                .collect::<Vec<_>>(),
        )
        .category(category)
    }

    #[test]
    fn legacy_read_only_sessions_return_to_the_advertised_agent_mode() {
        let modes = SessionModeState::new(
            "plan",
            vec![
                SessionMode::new("agent", "Agent"),
                SessionMode::new("plan", "Plan"),
            ],
        );

        assert_eq!(
            desired_access_mode(Flavor::Cursor, Some(&modes), RuntimeMode::FullAccess),
            Some("agent".to_owned())
        );
    }

    #[test]
    fn fx_access_mode_selects_ask_or_code() {
        let modes = SessionModeState::new(
            "code",
            vec![
                SessionMode::new("ask", "Ask before sensitive actions"),
                SessionMode::new("code", "Review sensitive actions automatically"),
            ],
        );
        assert_eq!(
            desired_access_mode(Flavor::Fx, Some(&modes), RuntimeMode::Ask),
            Some("ask".to_owned())
        );
        assert!(desired_access_mode(Flavor::Fx, Some(&modes), RuntimeMode::FullAccess).is_none());
    }

    #[test]
    fn fx_model_option_ignores_provider_selector_in_same_category() {
        let provider = select_config_option(
            "provider",
            SessionConfigOptionCategory::Model,
            "gateway",
            &["gateway", "codex", "grok"],
        );
        let model = select_config_option(
            "model",
            SessionConfigOptionCategory::Model,
            "openai/gpt-5.6-sol",
            &["openai/gpt-5.6-sol", "anthropic/claude-sonnet-5"],
        );

        assert_eq!(
            fx_model_option(&[provider, model]).map(|option| option.id.to_string()),
            Some("model".to_owned())
        );
    }

    #[test]
    fn fx_gateway_model_selects_the_gateway_route_first() {
        let provider = select_config_option(
            "provider",
            SessionConfigOptionCategory::Model,
            "codex",
            &["gateway", "codex", "grok"],
        );
        let model = select_config_option(
            "model",
            SessionConfigOptionCategory::Model,
            "gpt-5.6-luna",
            &["gpt-5.6-sol", "gpt-5.6-luna"],
        );
        let options = [provider, model];

        let (option, value) =
            fx_model_provider_switch(&options, "openai/gpt-5.6-luna-fast").unwrap();
        assert_eq!(option.id.to_string(), "provider");
        assert_eq!(value, "gateway");
    }

    #[test]
    fn cursor_model_aliases_resolve_to_advertised_parameterized_picker_values() {
        let option = select_config_option(
            "model",
            SessionConfigOptionCategory::Model,
            "default",
            &["default", "grok-4.6", "composer-2.5", "claude-sonnet-4-6"],
        );

        assert_eq!(
            cursor_model_selection(&option, "auto"),
            Some(CursorModelSelection {
                value: "default".into(),
                suffix: String::new(),
            })
        );
        assert_eq!(
            cursor_model_selection(&option, "composer-2.5"),
            Some(CursorModelSelection {
                value: "composer-2.5".into(),
                suffix: String::new(),
            })
        );
        assert_eq!(
            cursor_model_selection(&option, "cursor-grok-4.6-xhigh-fast"),
            Some(CursorModelSelection {
                value: "grok-4.6".into(),
                suffix: "xhigh-fast".into(),
            })
        );
        assert_eq!(
            cursor_model_selection(&option, "claude-4.6-sonnet-medium-thinking"),
            Some(CursorModelSelection {
                value: "claude-sonnet-4-6".into(),
                suffix: "medium-thinking".into(),
            })
        );
    }

    #[test]
    fn cursor_model_suffix_selects_dynamic_effort_thinking_and_fast_options() {
        let selection = CursorModelSelection {
            value: "claude-opus-5".into(),
            suffix: "thinking-extra-high-fast".into(),
        };
        let effort = select_config_option(
            "effort",
            SessionConfigOptionCategory::ThoughtLevel,
            "high",
            &["low", "medium", "high", "xhigh"],
        );
        let extra_high = select_config_option(
            "reasoning",
            SessionConfigOptionCategory::ThoughtLevel,
            "high",
            &["low", "medium", "high", "extra-high"],
        );
        let context = select_config_option(
            "context",
            SessionConfigOptionCategory::ModelConfig,
            "272k",
            &["272k", "1m"],
        );

        assert_eq!(
            cursor_desired_effort_value(&effort, &selection, None).as_deref(),
            Some("xhigh")
        );
        assert_eq!(
            cursor_desired_effort_value(&extra_high, &selection, Some("xhigh")).as_deref(),
            Some("extra-high")
        );
        assert_eq!(cursor_desired_thinking(&selection, None), Some(true));
        assert_eq!(cursor_desired_fast(&selection, None), Some(true));
        assert_eq!(
            cursor_desired_effort_value(&effort, &selection, Some("low")).as_deref(),
            Some("low")
        );
        assert_eq!(
            cursor_desired_fast(&selection, Some("default")),
            Some(false)
        );
        assert_eq!(
            cursor_desired_context_value(&context, Some("1m")).as_deref(),
            Some("1m")
        );
        assert_eq!(
            cursor_desired_thinking(&selection, Some("none")),
            Some(false)
        );
    }

    #[test]
    fn cursor_prefers_model_option_effort_over_thought_level() {
        let thought = select_config_option(
            "reasoning",
            SessionConfigOptionCategory::ThoughtLevel,
            "high",
            &["low", "medium", "high"],
        );
        let effort = select_config_option(
            "effort",
            SessionConfigOptionCategory::Other("model_option".into()),
            "max",
            &["low", "medium", "high", "max"],
        );
        let options = [thought, effort];
        let selected = find_cursor_effort_option(&options).unwrap();
        assert_eq!(selected.id.to_string(), "effort");
    }

    #[test]
    fn grok_set_model_includes_reasoning_effort_meta() {
        let params = set_model_params("sess", "grok-4.6", Some("xhigh"), Flavor::Grok);
        assert_eq!(params["modelId"], "grok-4.6");
        assert_eq!(params["_meta"]["reasoningEffort"], "xhigh");
        assert!(
            set_model_params("sess", "k2", Some("high"), Flavor::Kimi)
                .get("_meta")
                .is_none()
        );
    }
}
