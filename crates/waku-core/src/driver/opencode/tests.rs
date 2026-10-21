//! OpenCode driver unit tests: the capability snapshot (RFC 0002 port step
//! 5), selection transitions (step 6), routing and answer shapes.

use serde_json::json;

use super::*;

const CAPABILITIES: &str = include_str!("capabilities_opencode.json");

#[test]
fn capabilities_match_the_rfc_matrix_row() {
    let declared =
        serde_json::to_value(opencode_capabilities(&opencode_replay_instance())).unwrap();
    let expected: Value = serde_json::from_str(CAPABILITIES).unwrap();
    assert_eq!(
        declared,
        expected,
        "update capabilities_opencode.json and RFC 0002's matrix together:\n{}",
        serde_json::to_string_pretty(&declared).unwrap()
    );
}

fn selection() -> Selection {
    Selection {
        model: Some("opencode/big-pickle".to_owned()),
        ..Selection::default()
    }
}

#[test]
fn selection_transitions_follow_the_rfc_table() {
    let from = selection();
    let plan = |to: Selection| opencode_transition(&from, &to);
    assert_eq!(
        plan(Selection {
            model: Some("anthropic/claude".to_owned()),
            ..selection()
        }),
        TransitionPlan::ApplyInSession
    );
    assert_eq!(
        plan(Selection {
            runtime_mode: RuntimeMode::Ask,
            ..selection()
        }),
        TransitionPlan::ApplyInSession
    );
    for to in [
        Selection {
            reasoning_effort: Some("high".to_owned()),
            ..selection()
        },
        Selection {
            service_tier: Some("fast".to_owned()),
            ..selection()
        },
        Selection {
            context_window: Some("1m".to_owned()),
            ..selection()
        },
        Selection {
            agent_preset: Some("review".to_owned()),
            ..selection()
        },
        Selection {
            interaction_mode: InteractionMode::Plan,
            ..selection()
        },
    ] {
        assert_eq!(plan(to), TransitionPlan::ApplyOnNextTurn);
    }
    assert_eq!(plan(selection()), TransitionPlan::ApplyOnNextTurn);
}

#[test]
fn models_carry_the_reasoning_variant_and_plan_mode_is_the_plan_agent() {
    let model = model_ref(&Selection {
        reasoning_effort: Some("high".to_owned()),
        ..selection()
    })
    .unwrap();
    assert_eq!(
        (
            model.provider_id.as_str(),
            model.id.as_str(),
            model.variant.as_deref()
        ),
        ("opencode", "big-pickle", Some("high"))
    );
    assert!(
        model_ref(&Selection {
            model: Some("no-provider".to_owned()),
            ..Selection::default()
        })
        .is_none()
    );
    assert_eq!(agent_for(&Selection::default()), "build");
    assert_eq!(
        agent_for(&Selection {
            interaction_mode: InteractionMode::Plan,
            agent_preset: Some("review".to_owned()),
            ..Selection::default()
        }),
        "plan"
    );
}

/// Demultiplexing is a security boundary: the stream carries the user's own
/// TUI sessions and every other Waku task.
#[test]
fn events_route_by_their_session_and_tui_commands_never_do() {
    let session = |envelope: Value| routed_session(&envelope).map(str::to_owned);
    assert_eq!(
        session(json!({"type": "session.text.delta", "data": {"sessionID": "ses_a"}})),
        Some("ses_a".to_owned())
    );
    for kind in [
        "tui.prompt.append",
        "tui.command.execute",
        "tui.session.select",
    ] {
        assert_eq!(
            session(json!({"type": kind, "data": {"sessionID": "ses_a"}})),
            None
        );
    }
    // `form.created` is the one event whose session id is nested.
    assert_eq!(
        session(
            json!({"type": "form.created", "data": {"form": {"id": "frm_1", "sessionID": "ses_a"}}})
        ),
        Some("ses_a".to_owned())
    );
    assert_eq!(
        session(
            json!({"type": "form.created", "data": {"sessionID": "ses_a", "form": {"id": "frm_1"}}})
        ),
        None
    );
    assert_eq!(session(json!({"type": "model.updated", "data": {}})), None);
}

#[test]
fn form_answers_round_trip_field_keys_and_option_values() {
    let fields: Vec<FormField> = serde_json::from_value(json!([
        {"type": "string", "key": "color", "options": [{"value": "blue", "label": "Blue"}]},
        {"type": "multiselect", "key": "tags", "options": [{"value": "a", "label": "Alpha"}, {"value": "b", "label": "Beta"}]},
        {"type": "boolean", "key": "ok"},
        {"type": "number", "key": "n"},
    ]))
    .unwrap();
    let answers = vec![
        UserInputAnswer {
            question_id: "color".to_owned(),
            answers: vec!["Blue".to_owned()],
        },
        UserInputAnswer {
            question_id: "tags".to_owned(),
            answers: vec!["Alpha".to_owned(), "custom".to_owned()],
        },
        UserInputAnswer {
            question_id: "ok".to_owned(),
            answers: vec!["yes".to_owned()],
        },
        UserInputAnswer {
            question_id: "n".to_owned(),
            answers: vec!["3".to_owned()],
        },
    ];
    assert_eq!(
        serde_json::to_value(form_answer(&fields, &answers)).unwrap(),
        json!({"color": "blue", "tags": ["a", "custom"], "ok": true, "n": 3.0})
    );
}

#[test]
fn slash_invocations_split_name_and_arguments() {
    assert_eq!(
        slash_invocation("/review main"),
        Some(("review".to_owned(), "main".to_owned()))
    );
    assert_eq!(
        slash_invocation("/init"),
        Some(("init".to_owned(), String::new()))
    );
    assert_eq!(slash_invocation("no command"), None);
    assert_eq!(slash_invocation("/"), None);
}
