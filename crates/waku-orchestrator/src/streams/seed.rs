//! Synthetic thread histories for stream tests and benchmarks: completed
//! runs with a user message, tool calls and an assistant answer each,
//! committed through [`Orchestrator::ingest_decision`] in large batches.
//!
//! Not used by the daemon; it exists so the "open a 5,000-message thread"
//! and "tool storm" checks of #54 and #57 run against real projection rows
//! without a provider.

use std::path::PathBuf;

use waku_protocol::orchestration::*;

use crate::decide::Decision;
use crate::ids::derived_id;
use crate::orchestrator::{DispatchError, Orchestrator};
use crate::store::sink::WriteGuard;

/// What each synthetic turn holds.
#[derive(Clone, Copy, Debug)]
pub struct SeedOptions {
    pub turns: usize,
    /// Ordinal of the first seeded run; later calls continue the history.
    pub first_ordinal: u32,
    pub user_text_bytes: usize,
    pub assistant_text_bytes: usize,
    pub tool_calls_per_turn: usize,
    pub tool_output_bytes: usize,
    /// Turns committed per transaction.
    pub batch: usize,
    pub now: Timestamp,
}

impl Default for SeedOptions {
    fn default() -> Self {
        Self {
            turns: 10,
            first_ordinal: 1,
            user_text_bytes: 120,
            assistant_text_bytes: 600,
            tool_calls_per_turn: 0,
            tool_output_bytes: 0,
            batch: 100,
            now: 1_700_000_000_000,
        }
    }
}

fn text_of(bytes: usize, seed: &str) -> String {
    let unit = format!("{seed} lorem ipsum dolor sit amet. ");
    let mut text = unit.repeat(bytes / unit.len() + 1);
    text.truncate(bytes);
    text
}

/// Creates a project and a thread with fixed command ids.
pub fn seed_thread(
    orchestrator: &Orchestrator,
    name: &str,
) -> Result<(ProjectId, ThreadId), DispatchError> {
    let project_id = ProjectId(derived_id("seed-project", name));
    let thread_id = ThreadId(derived_id("seed-thread", name));
    let envelope = |id: &str, command| CommandEnvelope {
        command_id: CommandId::new(format!("seed-{id}-{}", project_id.0.simple()))
            .expect("a valid command id"),
        issued_by: Actor::User,
        source: CreationSource::Desktop,
        command,
    };
    orchestrator.dispatch(envelope(
        "project",
        Command::ProjectCreate {
            project_id,
            name: name.to_owned(),
            path: PathBuf::from(format!("/seed/{name}")),
            order_key: None,
        },
    ))?;
    orchestrator.dispatch(envelope(
        "thread",
        Command::ThreadCreate {
            thread_id,
            project_id,
            title: Some(name.to_owned()),
            model_selection: ModelSelection::new("codex"),
            runtime_mode: RuntimeMode::FullAccess,
            interaction_mode: InteractionMode::Default,
            workspace: WorkspaceStrategy::Root { branch: None },
            imported_native_thread: None,
        },
    ))?;
    Ok((project_id, thread_id))
}

fn item(
    thread_id: ThreadId,
    run_id: RunId,
    id: TurnItemId,
    position: u64,
    at: Timestamp,
    kind: TurnItemKind,
) -> TurnItem {
    TurnItem {
        id,
        thread_id,
        run_id: Some(run_id),
        node_id: None,
        provider_thread_id: None,
        provider_turn_id: None,
        native_item_ref: None,
        parent_item_id: None,
        position,
        status: ItemStatus::Completed,
        title: None,
        tool: None,
        stream_rev: 0,
        started_at: Some(at),
        completed_at: Some(at),
        updated_at: at,
        kind,
    }
}

/// The events of one completed synthetic turn.
pub fn turn_events(thread_id: ThreadId, ordinal: u32, options: &SeedOptions) -> Vec<DomainEvent> {
    let key = format!("{}:{ordinal}", thread_id.0.simple());
    let at = options.now + i64::from(ordinal) * 1_000;
    let user_id = MessageId(derived_id("seed-user", &key));
    let assistant_id = MessageId(derived_id("seed-assistant", &key));
    let run_id = RunId(derived_id("seed-run", &key));
    let message = |id, role, text: String| Message {
        id,
        thread_id,
        run_id: Some(run_id),
        node_id: None,
        role,
        created_by: if role == MessageRole::User {
            Actor::User
        } else {
            Actor::Agent
        },
        creation_source: CreationSource::Desktop,
        text,
        display_text: None,
        attachments: Vec::new(),
        context: Vec::new(),
        notification: None,
        sender_thread_id: None,
        streaming: false,
        created_at: at,
        updated_at: at,
    };
    let user_text = text_of(options.user_text_bytes, &format!("question {ordinal}"));
    let assistant_text = text_of(options.assistant_text_bytes, &format!("answer {ordinal}"));
    let mut events = vec![
        DomainEvent::RunUpdated(Run {
            id: run_id,
            thread_id,
            ordinal: Some(ordinal),
            model_selection: ModelSelection::new("codex"),
            provider_thread_id: None,
            user_message_id: user_id,
            root_node_id: None,
            active_attempt_id: None,
            status: RunStatus::Completed,
            queue_position: None,
            queue_held: false,
            requested_at: at,
            started_at: Some(at),
            provider_completed_at: Some(at),
            completed_at: Some(at),
            failure: None,
            checkpoint_id: None,
            context_handoff_id: None,
            continuation: None,
            workspace_preparation: None,
            restart_cancelled_background_work: Vec::new(),
            source_plan: None,
            delegated_task_id: None,
            delegated_completion: None,
        }),
        DomainEvent::MessageUpdated(message(user_id, MessageRole::User, user_text.clone())),
        DomainEvent::TurnItemUpdated(item(
            thread_id,
            run_id,
            TurnItemId(derived_id("seed-user-item", &key)),
            TurnItem::position_of(ordinal, 0),
            at,
            TurnItemKind::UserMessage {
                message_id: user_id,
                input_intent: InputIntent::TurnStart,
                text: user_text,
                display_text: None,
                attachments: Vec::new(),
                created_by: Actor::User,
                sender_thread_id: None,
            },
        )),
    ];
    for tool in 0..options.tool_calls_per_turn {
        events.push(DomainEvent::TurnItemUpdated(item(
            thread_id,
            run_id,
            TurnItemId(derived_id("seed-tool-item", &format!("{key}:{tool}"))),
            TurnItem::position_of(ordinal, 1 + tool as u32),
            at,
            TurnItemKind::CommandExecution {
                input: format!("cargo test --package seed-{tool}"),
                output: Some(text_of(options.tool_output_bytes, "test result")),
                output_truncated: false,
                output_omitted: false,
                output_indicates_failure: false,
                exit_code: Some(0),
            },
        )));
    }
    events.push(DomainEvent::MessageUpdated(message(
        assistant_id,
        MessageRole::Assistant,
        assistant_text.clone(),
    )));
    events.push(DomainEvent::TurnItemUpdated(item(
        thread_id,
        run_id,
        TurnItemId(derived_id("seed-assistant-item", &key)),
        TurnItem::position_of(ordinal, 1 + options.tool_calls_per_turn as u32),
        at,
        TurnItemKind::AssistantMessage {
            message_id: assistant_id,
            text: assistant_text,
            streaming: false,
        },
    )));
    events
}

/// Commits `options.turns` completed turns to `thread_id`; returns the last
/// committed sequence.
pub fn seed_history(
    orchestrator: &Orchestrator,
    thread_id: ThreadId,
    options: SeedOptions,
) -> Result<Sequence, DispatchError> {
    let mut last = orchestrator.last_sequence();
    let ordinals = (0..options.turns as u32).map(|n| options.first_ordinal + n);
    let ordinals = ordinals.collect::<Vec<_>>();
    for batch in ordinals.chunks(options.batch.max(1)) {
        let events = batch
            .iter()
            .flat_map(|ordinal| turn_events(thread_id, *ordinal, &options))
            .collect();
        let decision = Decision {
            events,
            ..Decision::default()
        };
        if let crate::orchestrator::IngestOutcome::Committed(sequence) =
            orchestrator.ingest_decision(thread_id, decision, WriteGuard::None)?
        {
            last = sequence;
        }
    }
    Ok(last)
}
