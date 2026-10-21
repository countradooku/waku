//! Capability snapshots per flavor (port step 5) and negotiation.

use std::path::PathBuf;

use waku_protocol::orchestration::{self as orch, RuntimeMode};

use super::*;

fn snapshot_path(flavor: Flavor) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src/driver/acp/snapshots")
        .join(format!("capabilities_{}.json", flavor.id()))
}

fn instance(flavor: Flavor) -> InstanceConfig {
    let mut cfg = InstanceConfig::new(flavor.id(), flavor.driver_kind());
    // Computer Use is a debug-build Grok extension; the snapshot shows it on.
    cfg.computer_use_enabled = flavor == Flavor::Grok;
    cfg
}

/// Each flavor's instance-level capabilities equal the committed snapshot,
/// which is RFC 0002's ACP matrix column for that flavor. Regenerate with
/// `WAKU_UPDATE_SNAPSHOTS=1` after an intentional change, and update the RFC.
#[test]
fn capabilities_match_the_snapshot_per_flavor() {
    for flavor in Flavor::ALL {
        let caps = instance_capabilities(&instance(flavor));
        let actual = serde_json::to_string_pretty(&caps).unwrap() + "\n";
        let path = snapshot_path(flavor);
        if std::env::var_os("WAKU_UPDATE_SNAPSHOTS").is_some() {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, &actual).unwrap();
            continue;
        }
        let expected = std::fs::read_to_string(&path)
            .unwrap_or_else(|_| panic!("missing snapshot {}", path.display()));
        assert_eq!(
            serde_json::from_str::<orch::Capabilities>(&expected).unwrap(),
            caps,
            "{} capabilities drifted from {}",
            flavor.id(),
            path.display()
        );
    }
}

#[test]
fn the_rfc_matrix_rows_that_differ_by_flavor() {
    let caps = |flavor| instance_capabilities(&instance(flavor));
    for flavor in Flavor::ALL {
        let caps = caps(flavor);
        // D-7: no flavor steers natively.
        assert!(!caps.turns.active_steering);
        assert!(caps.turns.steering_by_interrupt_restart);
        let grok = flavor == Flavor::Grok;
        assert_eq!(caps.threads.rollback, grok);
        assert_eq!(caps.threads.fork, grok);
        assert_eq!(caps.threads.fork_from_turn, grok);
        assert_eq!(caps.extras.computer_use, grok);
        assert_eq!(
            caps.planning.structured_questions,
            matches!(flavor, Flavor::Cursor | Flavor::Grok)
        );
        assert_eq!(
            caps.runtime_policy.supported_modes.len(),
            if flavor == Flavor::Fx { 2 } else { 4 }
        );
    }
    assert_eq!(
        caps(Flavor::Fx).runtime_policy.supported_modes,
        [RuntimeMode::Ask, RuntimeMode::FullAccess]
    );
}

#[test]
fn read_snapshot_is_negotiated_from_load_session() {
    let cfg = instance(Flavor::Kimi);
    let with = negotiated_capabilities(
        &cfg,
        &AgentInfo::from_initialize(&json!({"agentCapabilities": {"loadSession": true}})),
    );
    assert!(with.threads.read_snapshot);
    assert!(with.checkpointing.provider_can_read_conversation_snapshot);
    let without = negotiated_capabilities(
        &cfg,
        &AgentInfo::from_initialize(&json!({"agentCapabilities": {}})),
    );
    assert!(!without.threads.read_snapshot);
    // A negotiated session never claims more than its instance.
    let most = instance_capabilities(&cfg);
    assert!(most.threads.read_snapshot);
    assert_eq!(
        Capabilities {
            threads: most.threads.clone(),
            checkpointing: most.checkpointing.clone(),
            ..without.clone()
        },
        most
    );
}

#[test]
fn initialize_reports_session_capabilities() {
    let info = AgentInfo::from_initialize(&json!({
        "protocolVersion": 1,
        "agentCapabilities": {
            "loadSession": true,
            "promptCapabilities": {"image": false},
            "sessionCapabilities": {"list": {}, "resume": {}, "close": {}}
        }
    }));
    assert_eq!(
        info,
        AgentInfo {
            load_session: true,
            resume: true,
            fork: false,
            list: true,
            close: true,
            image: false,
        }
    );
}

#[test]
fn cursor_opts_into_its_parameterized_model_picker() {
    assert_eq!(
        initialize_params(Flavor::Cursor)["clientCapabilities"]["_meta"]["parameterizedModelPicker"],
        true
    );
    assert!(
        initialize_params(Flavor::Grok)["clientCapabilities"]
            .get("_meta")
            .is_none()
    );
}
