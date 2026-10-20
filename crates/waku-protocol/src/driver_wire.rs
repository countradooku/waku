use anyhow::{Context as _, anyhow, bail};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::model::DriverEvent;
use crate::wire_event::{
    ActivityPayload, PermissionPayload, PromptSubmittedPayload, SteerAcceptedPayload,
    SteerRejectedPayload, TurnFinishedPayload, UsagePayload, UserInputRequestPayload,
    WireDriverEvent,
};

pub fn decode_enum<T: DeserializeOwned>(value: &str) -> anyhow::Result<T> {
    serde_json::from_value(Value::String(value.to_owned()))
        .with_context(|| format!("invalid protocol enum value {value:?}"))
}

pub fn encode_enum<T: Serialize>(value: T) -> anyhow::Result<String> {
    serde_json::to_value(value)?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("protocol enum did not serialize as a string"))
}

/// The wire form of a provider event. Only the client-only cursor variant
/// has none.
pub fn event_to_wire(event: DriverEvent) -> anyhow::Result<WireDriverEvent> {
    Ok(match event {
        DriverEvent::RuntimeEventCursorAdvanced(_) => {
            bail!("client-only runtime cursors cannot be sent by the daemon")
        }
        DriverEvent::Connected { provider_cursor } => WireDriverEvent::Connected(provider_cursor),
        DriverEvent::AgentPresetSelected(preset) => WireDriverEvent::AgentPresetSelected(preset),
        DriverEvent::AutoTitleUpdated(title) => WireDriverEvent::AutoTitleUpdated(title),
        DriverEvent::AvailableCommands(commands) => WireDriverEvent::AvailableCommands(commands),
        DriverEvent::TurnStarted => WireDriverEvent::TurnStarted,
        DriverEvent::TurnParked => WireDriverEvent::TurnParked,
        DriverEvent::TextDelta(text) => WireDriverEvent::TextDelta(text),
        DriverEvent::ReasoningDelta(text) => WireDriverEvent::ReasoningDelta(text),
        DriverEvent::Activity {
            id,
            kind,
            title,
            detail,
            complete,
        } => WireDriverEvent::Activity(ActivityPayload {
            id,
            kind,
            title,
            detail,
            complete,
        }),
        DriverEvent::RichActivity(activity) => WireDriverEvent::RichActivity(activity),
        DriverEvent::BackgroundWork(work) => WireDriverEvent::BackgroundWork(work),
        DriverEvent::Permission {
            request_id,
            title,
            detail,
            options,
        } => WireDriverEvent::Permission(PermissionPayload {
            request_id,
            title,
            detail,
            options,
        }),
        DriverEvent::UserInputRequested {
            request_id,
            questions,
        } => WireDriverEvent::UserInputRequested(UserInputRequestPayload {
            request_id,
            questions,
        }),
        DriverEvent::ComputerUseUpdated(state) => WireDriverEvent::ComputerUseUpdated(state),
        DriverEvent::PromptSubmitted {
            message,
            turn_id,
            message_id,
        } => WireDriverEvent::PromptSubmitted(PromptSubmittedPayload {
            message,
            turn_id,
            message_id,
        }),
        DriverEvent::SteerAccepted { message } => {
            WireDriverEvent::SteerAccepted(SteerAcceptedPayload { message })
        }
        DriverEvent::SteerRejected { message, reason } => {
            WireDriverEvent::SteerRejected(SteerRejectedPayload { message, reason })
        }
        DriverEvent::UsageUpdated {
            context_tokens,
            context_window,
        } => WireDriverEvent::UsageUpdated(UsagePayload {
            context_tokens,
            context_window,
        }),
        DriverEvent::PlanUsageUpdated(usage) => WireDriverEvent::PlanUsageUpdated(usage),
        DriverEvent::GoalUpdated(goal) => WireDriverEvent::GoalUpdated(goal),
        DriverEvent::TurnFinished { success, summary } => {
            WireDriverEvent::TurnFinished(TurnFinishedPayload { success, summary })
        }
        DriverEvent::Error(error) => WireDriverEvent::Error(error),
        DriverEvent::ProcessExited => WireDriverEvent::ProcessExited,
    })
}

/// The provider event a wire event carries. Terminal events belong to a
/// terminal emulator, not a provider session, and are refused.
pub fn event_from_wire(event: WireDriverEvent) -> anyhow::Result<DriverEvent> {
    Ok(match event {
        WireDriverEvent::Connected(provider_cursor) => DriverEvent::Connected { provider_cursor },
        WireDriverEvent::AgentPresetSelected(preset) => DriverEvent::AgentPresetSelected(preset),
        WireDriverEvent::AutoTitleUpdated(title) => DriverEvent::AutoTitleUpdated(title),
        WireDriverEvent::AvailableCommands(commands) => DriverEvent::AvailableCommands(commands),
        WireDriverEvent::TurnStarted => DriverEvent::TurnStarted,
        WireDriverEvent::TurnParked => DriverEvent::TurnParked,
        WireDriverEvent::TextDelta(text) => DriverEvent::TextDelta(text),
        WireDriverEvent::ReasoningDelta(text) => DriverEvent::ReasoningDelta(text),
        WireDriverEvent::Activity(activity) => DriverEvent::Activity {
            id: activity.id,
            kind: activity.kind,
            title: activity.title,
            detail: activity.detail,
            complete: activity.complete,
        },
        WireDriverEvent::RichActivity(activity) => DriverEvent::RichActivity(activity),
        WireDriverEvent::BackgroundWork(work) => DriverEvent::BackgroundWork(work),
        WireDriverEvent::Permission(permission) => DriverEvent::Permission {
            request_id: permission.request_id,
            title: permission.title,
            detail: permission.detail,
            options: permission.options,
        },
        WireDriverEvent::UserInputRequested(request) => DriverEvent::UserInputRequested {
            request_id: request.request_id,
            questions: request.questions,
        },
        WireDriverEvent::ComputerUseUpdated(state) => DriverEvent::ComputerUseUpdated(state),
        WireDriverEvent::PromptSubmitted(submitted) => DriverEvent::PromptSubmitted {
            message: submitted.message,
            turn_id: submitted.turn_id,
            message_id: submitted.message_id,
        },
        WireDriverEvent::SteerAccepted(steer) => DriverEvent::SteerAccepted {
            message: steer.message,
        },
        WireDriverEvent::SteerRejected(steer) => DriverEvent::SteerRejected {
            message: steer.message,
            reason: steer.reason,
        },
        WireDriverEvent::UsageUpdated(usage) => DriverEvent::UsageUpdated {
            context_tokens: usage.context_tokens,
            context_window: usage.context_window,
        },
        WireDriverEvent::PlanUsageUpdated(usage) => DriverEvent::PlanUsageUpdated(usage),
        WireDriverEvent::GoalUpdated(goal) => DriverEvent::GoalUpdated(goal),
        WireDriverEvent::TurnFinished(finished) => DriverEvent::TurnFinished {
            success: finished.success,
            summary: finished.summary,
        },
        WireDriverEvent::Error(error) => DriverEvent::Error(error),
        WireDriverEvent::ProcessExited => DriverEvent::ProcessExited,
        event @ (WireDriverEvent::TerminalOutput(_)
        | WireDriverEvent::TerminalExited
        | WireDriverEvent::TerminalError(_)
        | WireDriverEvent::TerminalReset(_)) => {
            bail!("{} is a terminal event, not a provider event", event.kind())
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ThreadGoal, ThreadGoalStatus, UserInputOption, UserInputQuestion};

    #[test]
    fn goal_updates_round_trip_through_the_daemon_wire() {
        let wire = event_to_wire(DriverEvent::GoalUpdated(Some(ThreadGoal {
            objective: "Ship the feature".into(),
            status: ThreadGoalStatus::UsageLimited,
            token_budget: Some(50_000),
            tokens_used: 12_500,
            time_used_seconds: 90,
        })))
        .unwrap();
        assert_eq!(wire.kind(), "goalUpdated");
        // The status spelling is Codex's own camelCase vocabulary.
        assert_eq!(wire.payload()["status"], "usageLimited");

        let DriverEvent::GoalUpdated(Some(goal)) = event_from_wire(wire).unwrap() else {
            panic!("the event changed variants during its wire round trip");
        };
        assert_eq!(goal.objective, "Ship the feature");
        assert_eq!(goal.status, ThreadGoalStatus::UsageLimited);
        assert_eq!(goal.token_budget, Some(50_000));

        let cleared = event_to_wire(DriverEvent::GoalUpdated(None)).unwrap();
        assert!(matches!(
            event_from_wire(cleared).unwrap(),
            DriverEvent::GoalUpdated(None)
        ));
    }

    #[test]
    fn structured_user_input_round_trips_through_the_daemon_wire() {
        let wire = event_to_wire(DriverEvent::UserInputRequested {
            request_id: "request-1".into(),
            questions: vec![UserInputQuestion {
                id: "deployment".into(),
                header: "Environment".into(),
                question: "Where should this deploy?".into(),
                options: vec![UserInputOption {
                    label: "Preview".into(),
                    description: Some("Create a preview deployment".into()),
                }],
                multi_select: false,
            }],
        })
        .unwrap();
        assert_eq!(wire.kind(), "userInputRequested");

        let DriverEvent::UserInputRequested {
            request_id,
            questions,
        } = event_from_wire(wire).unwrap()
        else {
            panic!("the event changed variants during its wire round trip");
        };
        assert_eq!(request_id, "request-1");
        assert_eq!(questions[0].id, "deployment");
        assert_eq!(questions[0].options[0].label, "Preview");
    }
}
