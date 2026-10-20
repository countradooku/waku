//! Capability-driven policies (RFC 0002 "Degradation policies", "Selection
//! transitions"). Pure functions of a session's [`Capabilities`]: behaviour
//! follows what a provider declares, never its name.

use serde::{Deserialize, Serialize};
use waku_protocol::orchestration::*;

/// The orchestrator's default handoff budget when a provider does not
/// recommend one (`ContextCaps::max_recommended_handoff_chars`).
pub const DEFAULT_HANDOFF_CHARS: usize = 100_000;

/// How a message reaches a running turn.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SteeringPolicy {
    /// `steer_turn` into the running turn (`turns.active_steering`).
    Native,
    /// Interrupt, then restart within the same run as a new
    /// `RunAttempt { steering_restart }`.
    InterruptRestart,
    /// Neither: the message waits for the turn to end.
    Queue,
}

pub fn steering_policy(capabilities: &Capabilities) -> SteeringPolicy {
    let turns = &capabilities.turns;
    if turns.active_steering {
        SteeringPolicy::Native
    } else if turns.steering_by_interrupt_restart || turns.interrupt {
        SteeringPolicy::InterruptRestart
    } else {
        SteeringPolicy::Queue
    }
}

/// Where a fork starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForkPointKind {
    Tip,
    AfterTurn,
    SubagentThread,
}

/// How a fork is made.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForkStrategy {
    /// The provider forks its own thread.
    Native,
    /// A fresh provider thread whose first run carries a portable context
    /// handoff as `ContextKind::SyntheticUser` (D-6).
    Portable,
}

pub fn fork_strategy(capabilities: &Capabilities, point: ForkPointKind) -> ForkStrategy {
    let threads = &capabilities.threads;
    let native = threads.fork
        && match point {
            ForkPointKind::Tip => true,
            ForkPointKind::AfterTurn => threads.fork_from_turn,
            ForkPointKind::SubagentThread => threads.fork_from_subagent_thread,
        };
    if native {
        ForkStrategy::Native
    } else {
        ForkStrategy::Portable
    }
}

/// How a checkpoint rollback reconciles the provider conversation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RollbackStrategy {
    /// The provider rolls back (in place, or by replacing the thread).
    Native,
    /// A fresh provider thread plus a checkpoint-summary handoff, marked
    /// divergent.
    FreshThread,
}

pub fn rollback_strategy(capabilities: &Capabilities) -> RollbackStrategy {
    if capabilities.threads.rollback {
        RollbackStrategy::Native
    } else {
        RollbackStrategy::FreshThread
    }
}

/// A driver's answer to "how does this selection change reach a live
/// session" (`DriverV2::plan_selection_transition`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TransitionPlan {
    /// Per-turn parameters; the next `start_turn` carries the change.
    ApplyOnNextTurn,
    /// Call `apply_selection` on the live session, then continue.
    ApplyInSession,
    /// Close the session and reopen it on the same native thread.
    RestartSession,
    /// The native thread cannot carry the change: a new provider thread with
    /// a context handoff.
    CreateWithHandoff,
    Reject {
        reason: String,
    },
}

impl TransitionPlan {
    const fn rank(&self) -> u8 {
        match self {
            Self::ApplyOnNextTurn => 0,
            Self::ApplyInSession => 1,
            Self::RestartSession => 2,
            Self::CreateWithHandoff => 3,
            Self::Reject { .. } => 4,
        }
    }

    /// With several changed fields the strongest plan wins: `Reject` >
    /// `CreateWithHandoff` > `RestartSession` > `ApplyInSession` >
    /// `ApplyOnNextTurn`.
    pub fn strongest(self, other: Self) -> Self {
        if other.rank() > self.rank() {
            other
        } else {
            self
        }
    }

    /// Folds per-field plans into one.
    pub fn combine(plans: impl IntoIterator<Item = Self>) -> Self {
        plans
            .into_iter()
            .fold(Self::ApplyOnNextTurn, Self::strongest)
    }
}

/// What the session manager does about a selection change on a live session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionTransitionAction {
    /// Nothing to do now; the next turn carries it.
    Reuse,
    /// Switch the model (or other option) inside the live session.
    SwitchInSession,
    /// Close and reopen the session, resuming the same native thread.
    RestartAndResume,
    /// Start a new provider thread with a context handoff.
    NewWithHandoff,
    Reject {
        reason: String,
    },
}

/// Turns a driver's plan into an action, degrading where the session's
/// capabilities do not back the plan: an in-session switch without
/// `sessions.model_switch_in_session` restarts instead, and a handoff without
/// `sessions.provider_switching_via_handoff` is refused.
pub fn session_transition(
    capabilities: &Capabilities,
    plan: &TransitionPlan,
) -> SessionTransitionAction {
    match plan {
        TransitionPlan::ApplyOnNextTurn => SessionTransitionAction::Reuse,
        TransitionPlan::ApplyInSession if capabilities.sessions.model_switch_in_session => {
            SessionTransitionAction::SwitchInSession
        }
        TransitionPlan::ApplyInSession | TransitionPlan::RestartSession => {
            SessionTransitionAction::RestartAndResume
        }
        TransitionPlan::CreateWithHandoff
            if capabilities.sessions.provider_switching_via_handoff =>
        {
            SessionTransitionAction::NewWithHandoff
        }
        TransitionPlan::CreateWithHandoff => SessionTransitionAction::Reject {
            reason: "the provider cannot take over this thread with a context handoff".to_owned(),
        },
        TransitionPlan::Reject { reason } => SessionTransitionAction::Reject {
            reason: reason.clone(),
        },
    }
}

/// The runtime mode a run actually gets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeModeDecision {
    Run(RuntimeMode),
    /// The provider enforces only modes that do not include the request and
    /// cannot fall back to Supervised (a FullAccess-only provider).
    Refuse {
        reason: String,
    },
}

/// RFC 0002 "Degradation policies": offer only `supported_modes`; anything
/// else runs as Supervised (`Ask`), or is refused when the provider only
/// supports FullAccess. An empty set means the driver did not declare modes,
/// which the weakest-claim default treats like "enforces nothing natively":
/// the request runs at the client boundary unchanged.
pub fn effective_runtime_mode(
    capabilities: &Capabilities,
    requested: RuntimeMode,
) -> RuntimeModeDecision {
    let supported = &capabilities.runtime_policy.supported_modes;
    if supported.is_empty() || supported.contains(&requested) {
        return RuntimeModeDecision::Run(requested);
    }
    if supported.contains(&RuntimeMode::Ask) {
        return RuntimeModeDecision::Run(RuntimeMode::Ask);
    }
    RuntimeModeDecision::Refuse {
        reason: format!(
            "this provider only runs in {} mode",
            supported
                .iter()
                .map(|mode| mode.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// The handoff budget for a target provider.
pub fn handoff_budget(capabilities: &Capabilities) -> usize {
    capabilities
        .context
        .max_recommended_handoff_chars
        .map_or(DEFAULT_HANDOFF_CHARS, |chars| chars as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn steering_prefers_native_then_restart_then_queue() {
        let mut caps = Capabilities::default();
        assert_eq!(steering_policy(&caps), SteeringPolicy::Queue);
        caps.turns.interrupt = true;
        assert_eq!(steering_policy(&caps), SteeringPolicy::InterruptRestart);
        caps.turns.active_steering = true;
        assert_eq!(steering_policy(&caps), SteeringPolicy::Native);
    }

    #[test]
    fn forks_degrade_to_a_portable_handoff() {
        let mut caps = Capabilities::default();
        assert_eq!(
            fork_strategy(&caps, ForkPointKind::Tip),
            ForkStrategy::Portable
        );
        caps.threads.fork = true;
        assert_eq!(
            fork_strategy(&caps, ForkPointKind::Tip),
            ForkStrategy::Native
        );
        assert_eq!(
            fork_strategy(&caps, ForkPointKind::AfterTurn),
            ForkStrategy::Portable
        );
        caps.threads.fork_from_turn = true;
        assert_eq!(
            fork_strategy(&caps, ForkPointKind::AfterTurn),
            ForkStrategy::Native
        );
        assert_eq!(
            fork_strategy(&caps, ForkPointKind::SubagentThread),
            ForkStrategy::Portable
        );
    }

    #[test]
    fn the_strongest_transition_plan_wins() {
        let plan = TransitionPlan::combine([
            TransitionPlan::ApplyInSession,
            TransitionPlan::ApplyOnNextTurn,
            TransitionPlan::RestartSession,
        ]);
        assert_eq!(plan, TransitionPlan::RestartSession);
        let rejected = TransitionPlan::combine([
            TransitionPlan::CreateWithHandoff,
            TransitionPlan::Reject {
                reason: "no".into(),
            },
        ]);
        assert!(matches!(rejected, TransitionPlan::Reject { .. }));
    }

    #[test]
    fn session_transitions_follow_capabilities() {
        let mut caps = Capabilities::default();
        assert_eq!(
            session_transition(&caps, &TransitionPlan::ApplyOnNextTurn),
            SessionTransitionAction::Reuse
        );
        assert_eq!(
            session_transition(&caps, &TransitionPlan::ApplyInSession),
            SessionTransitionAction::RestartAndResume
        );
        caps.sessions.model_switch_in_session = true;
        assert_eq!(
            session_transition(&caps, &TransitionPlan::ApplyInSession),
            SessionTransitionAction::SwitchInSession
        );
        assert!(matches!(
            session_transition(&caps, &TransitionPlan::CreateWithHandoff),
            SessionTransitionAction::Reject { .. }
        ));
        caps.sessions.provider_switching_via_handoff = true;
        assert_eq!(
            session_transition(&caps, &TransitionPlan::CreateWithHandoff),
            SessionTransitionAction::NewWithHandoff
        );
    }

    #[test]
    fn unsupported_runtime_modes_run_supervised_or_are_refused() {
        let mut caps = Capabilities::default();
        assert_eq!(
            effective_runtime_mode(&caps, RuntimeMode::Auto),
            RuntimeModeDecision::Run(RuntimeMode::Auto)
        );
        caps.runtime_policy.supported_modes = vec![RuntimeMode::Ask, RuntimeMode::FullAccess];
        assert_eq!(
            effective_runtime_mode(&caps, RuntimeMode::Auto),
            RuntimeModeDecision::Run(RuntimeMode::Ask)
        );
        caps.runtime_policy.supported_modes = vec![RuntimeMode::FullAccess];
        assert!(matches!(
            effective_runtime_mode(&caps, RuntimeMode::Ask),
            RuntimeModeDecision::Refuse { .. }
        ));
    }
}
