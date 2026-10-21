//! Provider-thread snapshots from native history imports, shared by the
//! drivers whose `read_snapshot` parses a session file or an export.

use std::collections::HashMap;

use waku_protocol::orchestration::{self as orch, *};

use crate::driver::v2::{
    EntityKind, IdAllocator, InstanceConfig, ProviderThreadRef, ThreadSnapshot,
};

/// A provider-thread snapshot from an imported native history (a Pi
/// session file, an Amp thread export). Turns and messages bind by ordinal
/// under the provider thread.
pub(crate) fn snapshot_from_history(
    ids: &dyn IdAllocator,
    now: Timestamp,
    driver: orch::DriverKind,
    instance: &InstanceConfig,
    thread: &ProviderThreadRef,
    history: &waku_protocol::model::ProviderSessionHistory,
) -> ThreadSnapshot {
    let scope = thread.provider_thread_id.0;
    let mut row = ProviderThread {
        id: thread.provider_thread_id,
        driver,
        provider_instance_id: instance.instance_id.clone(),
        provider_session_id: None,
        app_thread_id: None,
        owner_node_id: None,
        native_thread_ref: None,
        native_conversation_head_ref: None,
        status: ProviderThreadStatus::Idle,
        first_run_ordinal: None,
        last_run_ordinal: None,
        handoff_ids: Vec::new(),
        forked_from: None,
        pending_background_tasks: Vec::new(),
        context_usage: None,
        goal: None,
        native_metadata: None,
        created_at: now,
        updated_at: now,
    };
    thread.apply_to(&mut row);
    let thread_id = ThreadId(ids.bind(scope, EntityKind::Node, "snapshot"));
    let mut turns = Vec::new();
    let mut ordinals = HashMap::new();
    for turn in &history.turns {
        let ordinal = turn.turn_count as u32;
        ordinals.insert(turn.id, ordinal);
        turns.push(ProviderTurn {
            id: ProviderTurnId(ids.bind_ordinal(
                scope,
                EntityKind::ProviderTurn,
                u64::from(ordinal),
            )),
            provider_thread_id: thread.provider_thread_id,
            node_id: NodeId(ids.bind_ordinal(scope, EntityKind::Node, u64::from(ordinal))),
            run_attempt_id: None,
            native_turn_ref: None,
            ordinal,
            status: match turn.status {
                waku_protocol::model::TurnStatus::Failed => ProviderTurnStatus::Failed,
                waku_protocol::model::TurnStatus::Interrupted => ProviderTurnStatus::Interrupted,
                _ => ProviderTurnStatus::Completed,
            },
            started_at: Some(turn.started_at as i64 * 1000),
            completed_at: turn.completed_at.map(|at| at as i64 * 1000),
            token_usage: None,
            turn_token_usage: None,
        });
    }
    let messages = history
        .messages
        .iter()
        .enumerate()
        .map(|(index, message)| {
            let ordinal = message
                .turn_id
                .and_then(|turn| ordinals.get(&turn).copied())
                .unwrap_or(0);
            let user = message.role == waku_protocol::model::MessageRole::User;
            Message {
                id: MessageId(ids.bind_ordinal(scope, EntityKind::Message, index as u64)),
                thread_id,
                run_id: None,
                node_id: (ordinal > 0)
                    .then(|| NodeId(ids.bind_ordinal(scope, EntityKind::Node, u64::from(ordinal)))),
                role: if user {
                    MessageRole::User
                } else {
                    MessageRole::Assistant
                },
                created_by: if user { Actor::User } else { Actor::Agent },
                creation_source: CreationSource::Provider,
                text: message.content.clone(),
                display_text: None,
                attachments: Vec::new(),
                context: Vec::new(),
                notification: None,
                sender_thread_id: None,
                streaming: false,
                created_at: message.created_at as i64 * 1000,
                updated_at: message.created_at as i64 * 1000,
            }
        })
        .collect();
    ThreadSnapshot {
        thread: row,
        turns,
        messages,
        pending_requests: Vec::new(),
    }
}
