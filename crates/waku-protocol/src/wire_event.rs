//! Typed runtime events on the wire (#21).
//!
//! Each event is `{"kind": "...", "payload": ...}`, exactly the JSON the
//! earlier `{kind: String, payload: Value}` struct produced, so web and
//! mobile clients read it unchanged. Being a typed, adjacently tagged enum
//! means the daemon serializes a [`crate::model::DriverEvent`] straight to
//! JSON and the client deserializes straight into the variant, with no
//! intermediate `serde_json::Value` tree on either side.
//!
//! Variants without data (`turnStarted`, `turnParked`, `processExited`,
//! `terminalExited`) carry no `payload`; an explicit `"payload": null` from
//! an older daemon or a recorded timeline is accepted.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;
use uuid::Uuid;

use crate::computer_use::ComputerUseState;
use crate::model::{
    ActivityItem, ActivityKind, BackgroundWorkEvent, PermissionOption, ProviderResumeCursor,
    ReportedCommand, ThreadGoal, UserInputQuestion,
};
use crate::usage::PlanUsage;

#[derive(Clone, Debug, Deserialize, Serialize, TS)]
#[serde(tag = "kind", content = "payload", rename_all = "camelCase")]
pub enum WireDriverEvent {
    Connected(Option<ProviderResumeCursor>),
    AgentPresetSelected(Option<String>),
    AutoTitleUpdated(Option<String>),
    AvailableCommands(Vec<ReportedCommand>),
    PromptSubmitted(PromptSubmittedPayload),
    TurnStarted,
    TurnParked,
    TextDelta(String),
    ReasoningDelta(String),
    Activity(ActivityPayload),
    RichActivity(ActivityItem),
    BackgroundWork(BackgroundWorkEvent),
    Permission(PermissionPayload),
    UserInputRequested(UserInputRequestPayload),
    ComputerUseUpdated(ComputerUseState),
    SteerAccepted(SteerAcceptedPayload),
    SteerRejected(SteerRejectedPayload),
    UsageUpdated(UsagePayload),
    PlanUsageUpdated(PlanUsage),
    GoalUpdated(Option<ThreadGoal>),
    TurnFinished(TurnFinishedPayload),
    Error(String),
    ProcessExited,
    /// Raw PTY output of a daemon terminal, base64 encoded.
    TerminalOutput(TerminalOutputPayload),
    TerminalExited,
    TerminalError(String),
}

#[derive(Clone, Debug, Deserialize, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct PromptSubmittedPayload {
    pub message: String,
    pub turn_id: Uuid,
    pub message_id: Uuid,
}

#[derive(Clone, Debug, Deserialize, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ActivityPayload {
    pub id: Option<String>,
    pub kind: ActivityKind,
    pub title: String,
    pub detail: Option<String>,
    pub complete: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct PermissionPayload {
    pub request_id: String,
    pub title: String,
    pub detail: String,
    pub options: Vec<PermissionOption>,
}

#[derive(Clone, Debug, Deserialize, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct UserInputRequestPayload {
    pub request_id: String,
    pub questions: Vec<UserInputQuestion>,
}

#[derive(Clone, Debug, Deserialize, Serialize, TS)]
pub struct SteerAcceptedPayload {
    pub message: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, TS)]
pub struct SteerRejectedPayload {
    pub message: String,
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct UsagePayload {
    pub context_tokens: Option<u64>,
    pub context_window: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, TS)]
pub struct TurnFinishedPayload {
    pub success: bool,
    pub summary: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, TS)]
pub struct TerminalOutputPayload {
    /// Base64 of the raw bytes.
    pub data: String,
}

impl WireDriverEvent {
    /// The `kind` tag this event carries on the wire.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Connected(_) => "connected",
            Self::AgentPresetSelected(_) => "agentPresetSelected",
            Self::AutoTitleUpdated(_) => "autoTitleUpdated",
            Self::AvailableCommands(_) => "availableCommands",
            Self::PromptSubmitted(_) => "promptSubmitted",
            Self::TurnStarted => "turnStarted",
            Self::TurnParked => "turnParked",
            Self::TextDelta(_) => "textDelta",
            Self::ReasoningDelta(_) => "reasoningDelta",
            Self::Activity(_) => "activity",
            Self::RichActivity(_) => "richActivity",
            Self::BackgroundWork(_) => "backgroundWork",
            Self::Permission(_) => "permission",
            Self::UserInputRequested(_) => "userInputRequested",
            Self::ComputerUseUpdated(_) => "computerUseUpdated",
            Self::SteerAccepted(_) => "steerAccepted",
            Self::SteerRejected(_) => "steerRejected",
            Self::UsageUpdated(_) => "usageUpdated",
            Self::PlanUsageUpdated(_) => "planUsageUpdated",
            Self::GoalUpdated(_) => "goalUpdated",
            Self::TurnFinished(_) => "turnFinished",
            Self::Error(_) => "error",
            Self::ProcessExited => "processExited",
            Self::TerminalOutput(_) => "terminalOutput",
            Self::TerminalExited => "terminalExited",
            Self::TerminalError(_) => "terminalError",
        }
    }

    /// The event's payload as JSON (`null` for variants without one). For
    /// tests and tools that compare against recorded timelines; the
    /// transport never builds this tree.
    pub fn payload(&self) -> Value {
        serde_json::to_value(self)
            .ok()
            .and_then(|mut value| value.get_mut("payload").map(Value::take))
            .unwrap_or(Value::Null)
    }

    /// Build an event from a recorded `kind` and `payload`.
    pub fn from_parts(kind: &str, payload: Value) -> serde_json::Result<Self> {
        serde_json::from_value(serde_json::json!({ "kind": kind, "payload": payload }))
    }

    /// Whether this is streamed text that the daemon may merge with the
    /// next event of the same kind (see the hub's delta coalescing).
    pub fn is_delta(&self) -> bool {
        matches!(self, Self::TextDelta(_) | Self::ReasoningDelta(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn wire_shape_matches_the_untyped_envelope() {
        let delta = serde_json::to_value(WireDriverEvent::TextDelta("hi".into())).unwrap();
        assert_eq!(delta, json!({ "kind": "textDelta", "payload": "hi" }));
        let started = serde_json::to_value(WireDriverEvent::TurnStarted).unwrap();
        assert_eq!(started, json!({ "kind": "turnStarted" }));
        let usage = serde_json::to_value(WireDriverEvent::UsageUpdated(UsagePayload {
            context_tokens: Some(9),
            context_window: None,
        }))
        .unwrap();
        assert_eq!(
            usage,
            json!({ "kind": "usageUpdated", "payload": { "contextTokens": 9, "contextWindow": null } })
        );
    }

    #[test]
    fn explicit_null_payloads_still_decode() {
        for kind in [
            "turnStarted",
            "turnParked",
            "processExited",
            "terminalExited",
        ] {
            let event = WireDriverEvent::from_parts(kind, Value::Null).unwrap();
            assert_eq!(event.kind(), kind);
            let missing: WireDriverEvent = serde_json::from_value(json!({ "kind": kind })).unwrap();
            assert_eq!(missing.kind(), kind);
        }
        let connected = WireDriverEvent::from_parts("connected", Value::Null).unwrap();
        assert!(matches!(connected, WireDriverEvent::Connected(None)));
    }

    #[test]
    fn kind_and_payload_round_trip_through_from_parts() {
        let event = WireDriverEvent::Activity(ActivityPayload {
            id: Some("a1".into()),
            kind: ActivityKind::Command,
            title: "ls".into(),
            detail: None,
            complete: true,
        });
        let rebuilt = WireDriverEvent::from_parts(event.kind(), event.payload()).unwrap();
        assert_eq!(
            serde_json::to_value(&rebuilt).unwrap(),
            serde_json::to_value(&event).unwrap()
        );
        assert!(WireDriverEvent::from_parts("noSuchKind", Value::Null).is_err());
    }
}
