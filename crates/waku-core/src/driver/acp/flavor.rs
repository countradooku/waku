//! ACP flavors: what differs between Cursor, Grok, Kimi, Fx and a generic
//! ACP agent. Everything after the launch is the Agent Client Protocol; the
//! flavor only decides the launch line, the extension methods Waku answers,
//! how a model choice is applied and what the driver claims to support.

use serde_json::Value;
use waku_protocol::model::ProviderKind;
use waku_protocol::orchestration::{
    self as orch, Capabilities, Enforcement, FailureClass, ProviderFailure, Quality, RefStrength,
    RuntimeMode,
};

use super::protocol::RpcError;
use crate::driver::v2::{InstanceConfig, Selection, TransitionPlan};

/// Which agent the ACP driver talks to (`InstanceConfig::provider`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Flavor {
    /// Any spec-conforming ACP agent; no extension methods.
    Generic,
    Cursor,
    Grok,
    Kimi,
    Fx,
}

impl Flavor {
    pub const ALL: [Self; 5] = [
        Self::Generic,
        Self::Cursor,
        Self::Grok,
        Self::Kimi,
        Self::Fx,
    ];

    pub fn from_driver(kind: orch::DriverKind) -> Self {
        match kind {
            orch::DriverKind::Cursor => Self::Cursor,
            orch::DriverKind::Grok => Self::Grok,
            orch::DriverKind::Kimi => Self::Kimi,
            orch::DriverKind::Fx => Self::Fx,
            _ => Self::Generic,
        }
    }

    pub fn from_provider(kind: ProviderKind) -> Option<Self> {
        match kind {
            ProviderKind::Cursor => Some(Self::Cursor),
            ProviderKind::Grok => Some(Self::Grok),
            ProviderKind::Kimi => Some(Self::Kimi),
            ProviderKind::Fx => Some(Self::Fx),
            _ => None,
        }
    }

    /// The replay provider id (`<scenario>/<id>.ndjson`).
    pub const fn id(self) -> &'static str {
        match self {
            Self::Generic => "acp",
            Self::Cursor => "cursor",
            Self::Grok => "grok",
            Self::Kimi => "kimi",
            Self::Fx => "fx",
        }
    }

    pub fn from_id(id: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|flavor| flavor.id() == id)
    }

    pub const fn driver_kind(self) -> orch::DriverKind {
        match self {
            Self::Generic => orch::DriverKind::Unknown,
            Self::Cursor => orch::DriverKind::Cursor,
            Self::Grok => orch::DriverKind::Grok,
            Self::Kimi => orch::DriverKind::Kimi,
            Self::Fx => orch::DriverKind::Fx,
        }
    }

    /// The legacy provider whose resume cursor carries this flavor's
    /// sessions. A generic agent has none: the native id is its cursor.
    pub const fn provider_kind(self) -> Option<ProviderKind> {
        match self {
            Self::Generic => None,
            Self::Cursor => Some(ProviderKind::Cursor),
            Self::Grok => Some(ProviderKind::Grok),
            Self::Kimi => Some(ProviderKind::Kimi),
            Self::Fx => Some(ProviderKind::Fx),
        }
    }

    pub fn display_name(self) -> &'static str {
        self.provider_kind()
            .map_or("ACP agent", ProviderKind::display_name)
    }

    /// Arguments after the binary. Grok takes its reasoning effort at launch.
    pub fn launch_args(self, reasoning_effort: Option<&str>, extra: &Value) -> Vec<String> {
        match self {
            Self::Cursor | Self::Kimi | Self::Fx => vec!["acp".to_owned()],
            Self::Grok => {
                let mut args = vec!["agent".to_owned()];
                if let Some(effort) = reasoning_effort.filter(|effort| !effort.is_empty()) {
                    args.push("--reasoning-effort".to_owned());
                    args.push(effort.to_owned());
                }
                args.push("stdio".to_owned());
                args
            }
            // A generic agent names its own arguments in the instance's
            // `extra.args` (most ACP agents need none).
            Self::Generic => extra
                .get("args")
                .and_then(Value::as_array)
                .map(|args| {
                    args.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_default(),
        }
    }

    /// Environment the driver sets on top of the inherited one.
    pub fn launch_env(self) -> Vec<(String, String)> {
        match self {
            Self::Grok => vec![("GROK_OAUTH2_REFERRER".to_owned(), "waku".to_owned())],
            _ => Vec::new(),
        }
    }

    /// Cursor's and Grok's structured-question extensions.
    pub const fn structured_questions(self) -> bool {
        matches!(self, Self::Cursor | Self::Grok)
    }

    /// Grok's native fork (`_x.ai/session/fork` on a side process, then
    /// on-disk truncation) backs rollback and fork.
    pub const fn native_fork(self) -> bool {
        matches!(self, Self::Grok)
    }

    /// Modes the agent enforces itself (`acp::desired_access_mode`).
    pub fn supported_modes(self) -> Vec<RuntimeMode> {
        match self {
            Self::Fx => vec![RuntimeMode::Ask, RuntimeMode::FullAccess],
            _ => vec![
                RuntimeMode::Ask,
                RuntimeMode::AutoAcceptEdits,
                RuntimeMode::Auto,
                RuntimeMode::FullAccess,
            ],
        }
    }

    /// Whether a model can change inside a live session. Decided per
    /// flavor, because the option set arrives only with `session/new`, after
    /// the capabilities are fixed: Cursor and Fx advertise a model config
    /// option, Grok and Kimi take `session/set_model`. A generic agent may do
    /// neither, so it restarts on a model change.
    pub const fn model_switch_in_session(self) -> bool {
        !matches!(self, Self::Generic)
    }
}

/// What `initialize` told us.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AgentInfo {
    pub load_session: bool,
    pub resume: bool,
    pub fork: bool,
    pub list: bool,
    pub close: bool,
    pub image: bool,
}

impl AgentInfo {
    pub fn from_initialize(result: &Value) -> Self {
        let caps = result.get("agentCapabilities").unwrap_or(&Value::Null);
        let session = caps.get("sessionCapabilities").unwrap_or(&Value::Null);
        let present = |key: &str| session.get(key).is_some_and(|value| !value.is_null());
        Self {
            load_session: caps
                .get("loadSession")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            resume: present("resume"),
            fork: present("fork"),
            list: present("list"),
            close: present("close"),
            image: caps
                .pointer("/promptCapabilities/image")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        }
    }

    /// The most an agent can advertise; the instance-level claim.
    pub fn everything() -> Self {
        Self {
            load_session: true,
            resume: true,
            fork: true,
            list: true,
            close: true,
            image: true,
        }
    }
}

/// The capabilities an ACP session declares (RFC 0002 matrix, ACP column).
/// `agent` is what `initialize` returned; [`AgentInfo::everything`] gives the
/// instance-level claim before any session exists.
pub fn capabilities(flavor: Flavor, _cfg: &InstanceConfig, agent: &AgentInfo) -> Capabilities {
    let mut caps = Capabilities::default();
    caps.sessions.model_switch_in_session = flavor.model_switch_in_session();
    caps.sessions.provider_switching_via_handoff = true;
    caps.threads.create_empty_thread = true;
    caps.threads.native_thread_id = true;
    // The snapshot is the `session/load` replay.
    caps.threads.read_snapshot = agent.load_session;
    // Only Grok rolls back and forks, by replacing the native session; every
    // other flavor takes the orchestrator's portable handoff (D-6).
    let native_fork = flavor.native_fork();
    caps.threads.rollback = native_fork;
    caps.threads.fork = native_fork;
    caps.threads.fork_from_turn = native_fork;
    caps.turns.emits_turn_completed = true;
    caps.turns.interrupt = true;
    // D-7: no native steering; the orchestrator interrupts and restarts.
    caps.turns.steering_by_interrupt_restart = true;
    caps.turns.queued_messages = true;
    caps.turns.terminal_status_quality = Quality::Strong;
    caps.streaming.assistant_text = true;
    caps.streaming.reasoning = true;
    caps.tools.tool_item_ids = true;
    caps.tools.emits_tool_started = true;
    caps.tools.emits_tool_completed = true;
    caps.tools.emits_tool_output = true;
    caps.tools.mcp_tools = true;
    caps.approvals.command = true;
    caps.approvals.file_read = true;
    caps.approvals.file_change = true;
    caps.approvals.native_request_ids = true;
    caps.approvals.callbacks_are_live_only = true;
    caps.planning.emits_plan_updated = true;
    caps.planning.emits_todo_list = true;
    caps.planning.structured_questions = flavor.structured_questions();
    caps.context.accepts_synthetic_user_context = true;
    caps.context.can_consume_handoff_summaries = true;
    caps.context.delta_handoff = true;
    caps.context.full_thread_handoff = true;
    caps.checkpointing.app_can_checkpoint_filesystem = true;
    caps.checkpointing.nested_scopes = true;
    caps.checkpointing.provider_can_rollback_conversation = caps.threads.rollback;
    caps.checkpointing.provider_can_read_conversation_snapshot = caps.threads.read_snapshot;
    caps.identity.thread_ids = RefStrength::Strong;
    caps.identity.turn_ids = RefStrength::None;
    caps.identity.item_ids = RefStrength::Weak;
    caps.identity.request_ids = RefStrength::Strong;
    caps.runtime_policy.enforcement = Enforcement::Native;
    caps.runtime_policy.supported_modes = flavor.supported_modes();
    // Grok's Computer Use runtime is not wired to v8 yet (#181), so no
    // flavor offers it.
    caps.extras.computer_use = false;
    caps.extras.model_discovery = true;
    caps
}

/// RFC 0002 "Selection transitions", ACP column.
pub fn plan_selection_transition(
    flavor: Flavor,
    session_caps: &Capabilities,
    from: &Selection,
    to: &Selection,
) -> TransitionPlan {
    let in_session = session_caps.sessions.model_switch_in_session;
    let mut plans = Vec::new();
    if from.model != to.model {
        plans.push(if in_session {
            TransitionPlan::ApplyInSession
        } else {
            TransitionPlan::RestartSession
        });
    }
    if from.reasoning_effort != to.reasoning_effort {
        // Grok takes its effort as a launch argument.
        plans.push(if flavor == Flavor::Grok {
            TransitionPlan::RestartSession
        } else {
            TransitionPlan::ApplyInSession
        });
    }
    if from.service_tier != to.service_tier {
        plans.push(TransitionPlan::ApplyOnNextTurn);
    }
    if from.context_window != to.context_window {
        plans.push(if in_session {
            TransitionPlan::ApplyInSession
        } else {
            TransitionPlan::RestartSession
        });
    }
    if from.runtime_mode != to.runtime_mode {
        plans.push(TransitionPlan::RestartSession);
    }
    if from.agent_preset != to.agent_preset {
        plans.push(TransitionPlan::ApplyOnNextTurn);
    }
    if from.interaction_mode != to.interaction_mode {
        plans.push(TransitionPlan::Reject {
            reason: "ACP agents have no plan mode in Waku yet".to_owned(),
        });
    }
    TransitionPlan::combine(plans)
}

// -- failures ---------------------------------------------------------------------

/// Grok's JSON-RPC code for a rate or usage limit (T3 `xaiAcpExtension`).
pub const XAI_RATE_LIMIT_CODE: i64 = -32003;
/// ACP's `auth_required` error.
const AUTH_REQUIRED_CODE: i64 = -32000;

fn timestamp_ms(value: &Value) -> Option<i64> {
    if let Some(number) = value.as_i64() {
        // Seconds before 2286, milliseconds after.
        return Some(if number < 10_000_000_000 {
            number * 1000
        } else {
            number
        });
    }
    let text = value.as_str()?;
    chrono::DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|time| time.timestamp_millis())
}

/// When a usage limit reopens, from the provider's own structured fields
/// only (port step 9): an absolute `resetAt`/`resetsAt`/`reset_at`, or a
/// relative `retryAfterSeconds`/`retry_after` against `now`.
fn reset_at(data: &Value, now: i64) -> Option<i64> {
    ["resetAt", "resetsAt", "reset_at", "resets_at"]
        .iter()
        .find_map(|key| data.get(*key).and_then(timestamp_ms))
        .or_else(|| {
            ["retryAfterSeconds", "retry_after_seconds", "retry_after"]
                .iter()
                .find_map(|key| data.get(*key).and_then(Value::as_i64))
                .map(|seconds| now + seconds * 1000)
        })
}

fn is_limit_marker(text: &str) -> bool {
    let text = text.to_ascii_lowercase();
    [
        "rate_limit",
        "usage_limit",
        "rate_limited",
        "quota",
        "too_many_requests",
    ]
    .iter()
    .any(|marker| text.contains(marker))
}

/// Classifies a JSON-RPC error that ended a turn (or a request).
pub fn classify_rpc_failure(flavor: Flavor, error: &RpcError, now: i64) -> ProviderFailure {
    if error.is_closed() {
        return ProviderFailure::new(FailureClass::TransportError, "The agent exited.")
            .with_code("process_exit");
    }
    let data = error.data.clone().unwrap_or(Value::Null);
    let http_status = ["http_status", "httpStatus", "status"]
        .iter()
        .find_map(|key| data.get(*key).and_then(Value::as_u64));
    let data_code = ["code", "type", "errorType", "error_type"]
        .iter()
        .find_map(|key| data.get(*key).and_then(Value::as_str))
        .unwrap_or_default();
    let usage_limit = (flavor == Flavor::Grok && error.code == XAI_RATE_LIMIT_CODE)
        || http_status == Some(429)
        || is_limit_marker(data_code);
    let class = if usage_limit {
        FailureClass::UsageLimit
    } else if error.code == AUTH_REQUIRED_CODE || matches!(http_status, Some(401 | 403)) {
        FailureClass::PermissionError
    } else if error.code == -32602 {
        FailureClass::ValidationError
    } else if http_status.is_some_and(|status| status >= 400) {
        FailureClass::ProviderError
    } else {
        FailureClass::Unknown
    };
    let mut failure =
        ProviderFailure::new(class, error.detail()).with_code(if data_code.is_empty() {
            format!("acp:{}", error.code)
        } else {
            data_code.to_owned()
        });
    if usage_limit {
        failure.reset_at = reset_at(&data, now);
        failure.retryable = Some(true);
    }
    failure
}

/// Grok's `_x.ai/session/prompt_complete` stop reasons that end a turn in
/// failure (T3 `xaiAcpExtension`): `rate_limit` and `error`.
pub fn xai_completion_failure(
    stop_reason: &str,
    agent_result: Option<&str>,
) -> Option<ProviderFailure> {
    match stop_reason {
        "rate_limit" => {
            let mut failure = ProviderFailure::new(
                FailureClass::UsageLimit,
                agent_result.unwrap_or("Grok reached its usage limit."),
            )
            .with_code("rate_limit");
            failure.retryable = Some(true);
            Some(failure)
        }
        "error" => Some(
            ProviderFailure::new(
                FailureClass::ProviderError,
                agent_result.unwrap_or("Grok reported an error."),
            )
            .with_code("error"),
        ),
        _ => None,
    }
}

/// A failure Kimi recorded only in its wire log (`kimi_session`).
pub fn kimi_failure(code: Option<&str>, message: &str) -> ProviderFailure {
    let class = if code.is_some_and(is_limit_marker) || message.contains("429") {
        FailureClass::UsageLimit
    } else {
        FailureClass::ProviderError
    };
    let failure = ProviderFailure::new(class, message);
    match code {
        Some(code) => failure.with_code(code),
        None => failure,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn grok_launches_with_its_effort_and_referrer() {
        assert_eq!(
            Flavor::Grok.launch_args(Some("xhigh"), &Value::Null),
            ["agent", "--reasoning-effort", "xhigh", "stdio"]
        );
        assert_eq!(
            Flavor::Grok.launch_args(None, &Value::Null),
            ["agent", "stdio"]
        );
        assert_eq!(
            Flavor::Grok.launch_env(),
            [("GROK_OAUTH2_REFERRER".to_owned(), "waku".to_owned())]
        );
        for flavor in [Flavor::Cursor, Flavor::Kimi, Flavor::Fx] {
            assert_eq!(flavor.launch_args(None, &Value::Null), ["acp"]);
            assert!(flavor.launch_env().is_empty());
        }
        assert_eq!(
            Flavor::Generic.launch_args(None, &json!({"args": ["--acp", "-q"]})),
            ["--acp", "-q"]
        );
    }

    #[test]
    fn usage_limits_are_classified_from_structured_fields() {
        let now = 1_000_000;
        let grok = RpcError {
            code: XAI_RATE_LIMIT_CODE,
            message: "Rate limited".into(),
            data: Some(
                json!({"message": "weekly limit reached", "resetAt": "2026-01-02T00:00:00Z"}),
            ),
        };
        let failure = classify_rpc_failure(Flavor::Grok, &grok, now);
        assert_eq!(failure.class, FailureClass::UsageLimit);
        assert_eq!(failure.message, "weekly limit reached");
        assert_eq!(failure.reset_at, Some(1_767_312_000_000));
        // -32003 is Grok's code only; elsewhere a 429 or a limit code counts.
        assert_eq!(
            classify_rpc_failure(Flavor::Generic, &grok, now).class,
            FailureClass::Unknown
        );
        let generic = RpcError {
            code: -32603,
            message: "Internal error".into(),
            data: Some(json!({"http_status": 429, "retry_after": 60})),
        };
        let failure = classify_rpc_failure(Flavor::Generic, &generic, now);
        assert_eq!(failure.class, FailureClass::UsageLimit);
        assert_eq!(failure.reset_at, Some(now + 60_000));
        let unclassified = RpcError {
            code: -32603,
            message: "Internal error".into(),
            data: None,
        };
        assert_eq!(
            classify_rpc_failure(Flavor::Kimi, &unclassified, now).class,
            FailureClass::Unknown
        );
        assert_eq!(
            xai_completion_failure("rate_limit", None).unwrap().class,
            FailureClass::UsageLimit
        );
        assert_eq!(
            xai_completion_failure("error", Some("boom"))
                .unwrap()
                .message,
            "boom"
        );
        assert!(xai_completion_failure("end_turn", None).is_none());
        assert_eq!(
            kimi_failure(Some("provider.rate_limit"), "slow down").class,
            FailureClass::UsageLimit
        );
        assert_eq!(
            kimi_failure(Some("provider.api_error"), "402 membership inactive").class,
            FailureClass::ProviderError
        );
    }

    fn selection() -> Selection {
        Selection {
            model: Some("a".into()),
            ..Selection::default()
        }
    }

    #[test]
    fn selection_transitions_follow_the_rfc_table() {
        let cfg = InstanceConfig::new("grok", orch::DriverKind::Grok);
        let grok = capabilities(Flavor::Grok, &cfg, &AgentInfo::everything());
        let generic = capabilities(Flavor::Generic, &cfg, &AgentInfo::everything());
        let from = selection();
        let change = |edit: fn(&mut Selection)| {
            let mut to = from.clone();
            edit(&mut to);
            to
        };
        let model = change(|to| to.model = Some("b".into()));
        assert_eq!(
            plan_selection_transition(Flavor::Grok, &grok, &from, &model),
            TransitionPlan::ApplyInSession
        );
        assert_eq!(
            plan_selection_transition(Flavor::Generic, &generic, &from, &model),
            TransitionPlan::RestartSession
        );
        let effort = change(|to| to.reasoning_effort = Some("high".into()));
        assert_eq!(
            plan_selection_transition(Flavor::Grok, &grok, &from, &effort),
            TransitionPlan::RestartSession
        );
        assert_eq!(
            plan_selection_transition(Flavor::Kimi, &grok, &from, &effort),
            TransitionPlan::ApplyInSession
        );
        let tier = change(|to| to.service_tier = Some("fast".into()));
        assert_eq!(
            plan_selection_transition(Flavor::Cursor, &grok, &from, &tier),
            TransitionPlan::ApplyOnNextTurn
        );
        let mode = change(|to| to.runtime_mode = RuntimeMode::Ask);
        assert_eq!(
            plan_selection_transition(Flavor::Cursor, &grok, &from, &mode),
            TransitionPlan::RestartSession
        );
        let preset = change(|to| to.agent_preset = Some("plan".into()));
        assert_eq!(
            plan_selection_transition(Flavor::Cursor, &grok, &from, &preset),
            TransitionPlan::ApplyOnNextTurn
        );
        let plan = change(|to| to.interaction_mode = orch::InteractionMode::Plan);
        assert!(matches!(
            plan_selection_transition(Flavor::Cursor, &grok, &from, &plan),
            TransitionPlan::Reject { .. }
        ));
    }
}
