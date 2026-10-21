//! The threads the bench daemon serves: one project, one thread per timeline
//! and optional long history threads, written through the orchestrator
//! before the server accepts a connection.
//!
//! Projects and threads are created with `project.create` and
//! `thread.create`, exactly as a client creates them. History is committed
//! with [`Orchestrator::ingest_decision`] as completed runs (a run, its user
//! and assistant messages, and the turn items a provider turn leaves:
//! user message, tool calls, assistant message), in the same shapes a real
//! provider turn stores, so the shell and thread streams, history paging and
//! search serve real projection rows without running a provider for every
//! turn. Every id, timestamp and text is derived from fixed seeds.

use std::path::PathBuf;

use anyhow::{Context as _, anyhow};
use uuid::Uuid;
use waku_orchestrator::{Decision, IngestOutcome, Orchestrator, WriteGuard};
use waku_protocol::model::ProviderKind;
use waku_protocol::orchestration::*;

use crate::content;
use crate::rng::Rng;

/// Fixed so a client's selection and a runner's scripts survive restarts.
pub const PROJECT_ID: Uuid = Uuid::from_u128(0xbe4c_0000_0000_4000_8000_0000_0000_0001);
/// Threads are stamped from here (seconds) so ordering never depends on the
/// clock.
const BASE_TIME: i64 = 1_790_000_000;
/// History turns committed per transaction.
const HISTORY_BATCH: usize = 100;

/// The id of the thread bound to `timeline`.
pub fn thread_id(timeline: &str) -> Uuid {
    let seed = timeline.bytes().fold(0x811c_9dc5_u32, |hash, byte| {
        (hash ^ u32::from(byte)).wrapping_mul(0x0100_0193)
    });
    Rng::new(seed).uuid()
}

/// The id of the `index`th history thread (`--history-sessions`).
pub fn history_thread_id(index: usize) -> Uuid {
    Rng::new(0x6869_7374_u32.wrapping_add(index as u32)).uuid()
}

pub struct FixtureOptions {
    pub project_path: PathBuf,
    /// One thread per timeline, newest first in the sidebar.
    pub timelines: Vec<String>,
    pub provider: ProviderKind,
    pub history_turns: usize,
    /// How many history threads of `history_turns` turns (at least one when
    /// `history_turns` is non-zero).
    pub history_sessions: usize,
}

/// What [`seed`] created.
#[derive(Clone, Debug)]
pub struct Seeded {
    pub project_id: Uuid,
    /// `(timeline, thread id)`, in `FixtureOptions::timelines` order.
    pub timeline_threads: Vec<(String, Uuid)>,
    pub history_threads: Vec<Uuid>,
}

impl Seeded {
    pub fn thread_of(&self, timeline: &str) -> Option<Uuid> {
        self.timeline_threads
            .iter()
            .find(|(name, _)| name == timeline)
            .map(|(_, id)| *id)
    }
}

/// The model selection fixture threads start with: the provider's instance
/// and its default model.
pub fn model_selection(provider: ProviderKind) -> ModelSelection {
    let mut selection = ModelSelection::new(provider.id());
    selection.model = waku_protocol::model_catalog::fallback_models(provider)
        .into_iter()
        .find(|model| model.is_default)
        .map(|model| model.id);
    selection
}

fn envelope(id: String, command: Command) -> anyhow::Result<CommandEnvelope> {
    Ok(CommandEnvelope {
        command_id: CommandId::new(id).map_err(|error| anyhow!("{error:?}"))?,
        issued_by: Actor::User,
        source: CreationSource::Desktop,
        command,
    })
}

fn dispatch(orchestrator: &Orchestrator, id: String, command: Command) -> anyhow::Result<()> {
    orchestrator
        .dispatch(envelope(id, command)?)
        .map(drop)
        .map_err(|error| anyhow!("{error}"))
}

fn create_thread(
    orchestrator: &Orchestrator,
    project_id: ProjectId,
    thread_id: Uuid,
    title: String,
    provider: ProviderKind,
) -> anyhow::Result<()> {
    dispatch(
        orchestrator,
        format!("bench:thread:{}", thread_id.simple()),
        Command::ThreadCreate {
            thread_id: ThreadId(thread_id),
            project_id,
            // A user title, so no title generation runs.
            title: Some(title),
            model_selection: model_selection(provider),
            runtime_mode: RuntimeMode::FullAccess,
            interaction_mode: InteractionMode::Default,
            workspace: WorkspaceStrategy::Root { branch: None },
            imported_native_thread: None,
        },
    )
}

fn commit(
    orchestrator: &Orchestrator,
    thread_id: Uuid,
    events: Vec<DomainEvent>,
) -> anyhow::Result<()> {
    let decision = Decision {
        events,
        ..Decision::default()
    };
    match orchestrator.ingest_decision(ThreadId(thread_id), decision, WriteGuard::None) {
        Ok(IngestOutcome::Committed(_)) => Ok(()),
        Ok(other) => Err(anyhow!("the history commit was not stored: {other:?}")),
        Err(error) => Err(anyhow!("{error}")),
    }
}

/// Stamps `last_reply_at` from the thread's newest run request, as
/// `message.dispatch` does: history committed directly would otherwise leave
/// it unset, and clients list only threads that have one.
fn stamp_reply(orchestrator: &Orchestrator, thread_id: Uuid) -> anyhow::Result<()> {
    let projection = orchestrator
        .thread(ThreadId(thread_id))
        .map_err(|error| anyhow!("{error}"))?;
    let Some(at) = projection.runs.values().map(|run| run.requested_at).max() else {
        return Ok(());
    };
    let mut thread = projection.thread.clone();
    thread.last_reply_at = Some(at);
    thread.updated_at = thread.updated_at.max(at);
    commit(
        orchestrator,
        thread_id,
        vec![DomainEvent::ThreadUpdated(ThreadUpdate {
            change: ThreadChange::Activity,
            thread,
        })],
    )
}

/// Creates the project and every thread, and commits their history.
pub fn seed(orchestrator: &Orchestrator, options: &FixtureOptions) -> anyhow::Result<Seeded> {
    let project_id = ProjectId(PROJECT_ID);
    dispatch(
        orchestrator,
        format!("bench:project:{}", PROJECT_ID.simple()),
        Command::ProjectCreate {
            project_id,
            name: options
                .project_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("bench-project")
                .to_owned(),
            path: options.project_path.clone(),
            order_key: None,
        },
    )
    .context("could not create the bench project")?;

    let mut timeline_threads = Vec::new();
    let count = options.timelines.len() as i64 + 1;
    for (index, timeline) in options.timelines.iter().enumerate() {
        let id = thread_id(timeline);
        create_thread(
            orchestrator,
            project_id,
            id,
            format!("Bench · {timeline}"),
            options.provider,
        )
        .with_context(|| format!("could not create the {timeline} thread"))?;
        // Newest first: the first timeline gets the latest stamp.
        let at = (BASE_TIME + (count - index as i64) * 60) * 1_000;
        commit(
            orchestrator,
            id,
            intro_turn(id, timeline, options.provider, at),
        )?;
        stamp_reply(orchestrator, id)?;
        timeline_threads.push((timeline.clone(), id));
    }

    let mut history_threads = Vec::new();
    if options.history_turns > 0 {
        for index in 0..options.history_sessions.max(1) {
            let id = history_thread_id(index);
            let title = if index == 0 {
                format!("Bench · history ({} turns)", options.history_turns)
            } else {
                format!(
                    "Bench · history {} ({} turns)",
                    index + 1,
                    options.history_turns
                )
            };
            create_thread(orchestrator, project_id, id, title, options.provider)?;
            let turns = history_turns(id, options.provider, options.history_turns, index);
            for batch in turns.chunks(HISTORY_BATCH) {
                commit(orchestrator, id, batch.concat())?;
            }
            stamp_reply(orchestrator, id)?;
            history_threads.push(id);
        }
    }
    Ok(Seeded {
        project_id: PROJECT_ID,
        timeline_threads,
        history_threads,
    })
}

/// One completed run: the run, its messages and its turn items.
struct Turn {
    thread_id: Uuid,
    provider: ProviderKind,
    ordinal: u32,
    run_id: Uuid,
    user: (Uuid, Uuid, String),
    assistant: (Uuid, Uuid, String),
    /// `(item id, kind)` between the user and the assistant message.
    tools: Vec<(Uuid, TurnItemKind)>,
    started_at: Timestamp,
    completed_at: Timestamp,
}

impl Turn {
    fn events(self) -> Vec<DomainEvent> {
        let thread_id = ThreadId(self.thread_id);
        let run_id = RunId(self.run_id);
        let message = |id: Uuid, role, text: &str, at| Message {
            id: MessageId(id),
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
            text: text.to_owned(),
            display_text: None,
            attachments: Vec::new(),
            context: Vec::new(),
            notification: None,
            sender_thread_id: None,
            streaming: false,
            created_at: at,
            updated_at: at,
        };
        let item = |id: Uuid, index: u32, at, kind| {
            DomainEvent::TurnItemUpdated(TurnItem {
                id: TurnItemId(id),
                thread_id,
                run_id: Some(run_id),
                node_id: None,
                provider_thread_id: None,
                provider_turn_id: None,
                native_item_ref: None,
                parent_item_id: None,
                position: TurnItem::position_of(self.ordinal, index),
                status: ItemStatus::Completed,
                title: None,
                tool: None,
                stream_rev: 0,
                started_at: Some(at),
                completed_at: Some(at),
                updated_at: at,
                kind,
            })
        };
        let (user_message, user_item, user_text) = self.user;
        let (assistant_message, assistant_item, assistant_text) = self.assistant;
        let mut events = vec![
            DomainEvent::RunUpdated(Run {
                id: run_id,
                thread_id,
                ordinal: Some(self.ordinal),
                model_selection: model_selection(self.provider),
                provider_thread_id: None,
                user_message_id: MessageId(user_message),
                root_node_id: None,
                active_attempt_id: None,
                status: RunStatus::Completed,
                queue_position: None,
                queue_held: false,
                requested_at: self.started_at,
                started_at: Some(self.started_at),
                provider_completed_at: Some(self.completed_at),
                completed_at: Some(self.completed_at),
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
            DomainEvent::MessageUpdated(message(
                user_message,
                MessageRole::User,
                &user_text,
                self.started_at,
            )),
            item(
                user_item,
                0,
                self.started_at,
                TurnItemKind::UserMessage {
                    message_id: MessageId(user_message),
                    input_intent: InputIntent::TurnStart,
                    text: user_text,
                    display_text: None,
                    attachments: Vec::new(),
                    created_by: Actor::User,
                    sender_thread_id: None,
                },
            ),
        ];
        let tools = self.tools.len() as u32;
        for (index, (id, kind)) in self.tools.into_iter().enumerate() {
            events.push(item(id, 1 + index as u32, self.started_at, kind));
        }
        events.push(DomainEvent::MessageUpdated(message(
            assistant_message,
            MessageRole::Assistant,
            &assistant_text,
            self.completed_at,
        )));
        events.push(item(
            assistant_item,
            1 + tools,
            self.completed_at,
            TurnItemKind::AssistantMessage {
                message_id: MessageId(assistant_message),
                text: assistant_text,
                streaming: false,
            },
        ));
        events
    }
}

/// The settled first turn of a timeline thread, saying what a prompt in it
/// replays.
fn intro_turn(
    thread_id: Uuid,
    timeline: &str,
    provider: ProviderKind,
    at: Timestamp,
) -> Vec<DomainEvent> {
    let mut rng = Rng::new(thread_id.as_u128() as u32);
    Turn {
        thread_id,
        provider,
        ordinal: 1,
        run_id: rng.uuid(),
        user: (rng.uuid(), rng.uuid(), format!("Bench: {timeline}")),
        assistant: (
            rng.uuid(),
            rng.uuid(),
            format!(
                "Send `{timeline}` in this thread to replay the `{timeline}` timeline. A \
                 prompt whose whole text is another timeline's name (prose, burst, \
                 reasoning-fast, code-wall, tool-storm, images) replays that one; any other \
                 prompt replays the daemon's `--timeline`."
            ),
        ),
        tools: Vec::new(),
        started_at: at,
        completed_at: at,
    }
    .events()
}

/// A long, settled conversation in the seed script's shapes, for transcripts
/// that must already be long when a timeline starts streaming. Thread 0 keeps
/// the id and content it always had; each later one gets its own id, content
/// seed and an older timestamp. One entry per turn.
fn history_turns(
    thread_id: Uuid,
    provider: ProviderKind,
    turns: usize,
    index: usize,
) -> Vec<Vec<DomainEvent>> {
    let mut rng = Rng::new(20_260_806_u32.wrapping_add(index as u32));
    let mut clock = BASE_TIME - turns as i64 * 60 - index as i64 * 86_400;
    let mut out = Vec::with_capacity(turns);
    for turn_index in 0..turns {
        let run_id = rng.uuid();
        let started_at = clock * 1_000;
        let prompt = (*rng.pick(content::PROMPTS)).to_owned();
        let user = (rng.uuid(), rng.uuid(), prompt);
        clock += rng.int(2, 12) as i64;
        let activity_count = rng.int(1, 3);
        let tools = (0..activity_count)
            .map(|_| history_tool(&mut rng))
            .collect::<Vec<_>>();
        clock += rng.int(5, 40) as i64;
        let bytes = rng.int(300, 1_400) as usize;
        let answer = content::markdown_answer(&mut rng, bytes);
        let assistant = (rng.uuid(), rng.uuid(), answer);
        clock += rng.int(1, 8) as i64;
        let completed_at = clock * 1_000;
        out.push(
            Turn {
                thread_id,
                provider,
                ordinal: turn_index as u32 + 1,
                run_id,
                user,
                assistant,
                tools,
                started_at,
                completed_at,
            }
            .events(),
        );
        clock += rng.int(5, 30) as i64;
    }
    out
}

fn history_tool(rng: &mut Rng) -> (Uuid, TurnItemKind) {
    let id = rng.uuid();
    // The call id the legacy rows carried; drawn to keep the content stream.
    let _ = rng.call_id();
    let kind = if rng.chance(0.5) {
        let command = (*rng.pick(content::COMMANDS)).to_owned();
        let lines = rng.int(6, 30);
        TurnItemKind::CommandExecution {
            input: command,
            output: Some(content::build_log(rng, lines)),
            output_truncated: false,
            output_omitted: false,
            output_indicates_failure: false,
            exit_code: Some(0),
        }
    } else {
        let hits = rng.int(4, 12);
        let results = content::search_results(rng, hits)
            .lines()
            .map(|line| {
                let mut parts = line.splitn(3, ':');
                let file_name = parts.next().unwrap_or_default().to_owned();
                let line_number = parts.next().and_then(|line| line.parse().ok());
                FileSearchResult {
                    file_name,
                    line: line_number,
                    column: None,
                    preview: parts.next().map(str::to_owned),
                }
            })
            .collect();
        TurnItemKind::FileSearch {
            pattern: None,
            results,
        }
    };
    (id, kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_and_history_are_stable() {
        assert_ne!(thread_id("prose"), thread_id("burst"));
        assert_eq!(thread_id("prose"), thread_id("prose"));
        assert_ne!(history_thread_id(0), history_thread_id(1));
        let id = history_thread_id(0);
        let a = history_turns(id, ProviderKind::Codex, 5, 0);
        let b = history_turns(id, ProviderKind::Codex, 5, 0);
        assert_eq!(a.len(), 5);
        assert_eq!(
            serde_json::to_string(&a).unwrap(),
            serde_json::to_string(&b).unwrap()
        );
        // A different history thread has its own content.
        let other = history_turns(history_thread_id(1), ProviderKind::Codex, 5, 1);
        assert_ne!(
            serde_json::to_string(&a[0][2]).unwrap(),
            serde_json::to_string(&other[0][2]).unwrap()
        );
    }

    #[test]
    fn a_turn_is_a_completed_run_with_ordered_items() {
        let id = history_thread_id(0);
        let turn = &history_turns(id, ProviderKind::Claude, 3, 0)[2];
        let DomainEvent::RunUpdated(run) = &turn[0] else {
            panic!("a run first");
        };
        assert_eq!(run.ordinal, Some(3));
        assert_eq!(run.status, RunStatus::Completed);
        assert_eq!(run.model_selection.instance.as_str(), "claude");
        let positions = turn
            .iter()
            .filter_map(|event| match event {
                DomainEvent::TurnItemUpdated(item) => Some(item.position),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(positions.len() >= 3, "user, tools and the answer");
        assert!(positions.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(positions[0], TurnItem::position_of(3, 0));
    }
}
