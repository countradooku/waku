//! DeepSeek Harness driver unit tests: the capability snapshot (RFC 0002
//! port step 5) and selection transitions (step 6).

use super::*;

const CAPABILITIES: &str = include_str!("capabilities_deepseek.json");

#[test]
fn capabilities_match_the_rfc_matrix_row() {
    let declared =
        serde_json::to_value(deepseek_capabilities(&deepseek_replay_instance())).unwrap();
    let expected: Value = serde_json::from_str(CAPABILITIES).unwrap();
    assert_eq!(
        declared,
        expected,
        "update capabilities_deepseek.json and RFC 0002's matrix together:\n{}",
        serde_json::to_string_pretty(&declared).unwrap()
    );
}

#[test]
fn selection_transitions_follow_the_rfc_table() {
    let from = Selection::default();
    let plan = |to: Selection| deepseek_transition(&from, &to);
    for to in [
        Selection {
            model: Some("deepseek/deepseek-v4".to_owned()),
            ..Selection::default()
        },
        Selection {
            reasoning_effort: Some("high".to_owned()),
            ..Selection::default()
        },
        Selection {
            context_window: Some("1m".to_owned()),
            ..Selection::default()
        },
        Selection {
            runtime_mode: RuntimeMode::Ask,
            ..Selection::default()
        },
    ] {
        assert_eq!(plan(to), TransitionPlan::ApplyInSession);
    }
    assert_eq!(
        plan(Selection {
            service_tier: Some("fast".to_owned()),
            ..Selection::default()
        }),
        TransitionPlan::ApplyOnNextTurn
    );
    // Agent presets are fixed at `session.create`.
    assert_eq!(
        plan(Selection {
            agent_preset: Some("reviewer".to_owned()),
            model: Some("deepseek/deepseek-v4".to_owned()),
            ..Selection::default()
        }),
        TransitionPlan::CreateWithHandoff
    );
    assert!(matches!(
        plan(Selection {
            interaction_mode: InteractionMode::Plan,
            ..Selection::default()
        }),
        TransitionPlan::Reject { .. }
    ));
}

#[test]
fn full_access_is_harness_danger_mode() {
    assert_eq!(
        permission_for(RuntimeMode::FullAccess),
        "danger-full-access"
    );
    assert_eq!(permission_for(RuntimeMode::Ask), "workspace-write");
    assert_eq!(
        permission_for(RuntimeMode::AutoAcceptEdits),
        "workspace-write"
    );
}
