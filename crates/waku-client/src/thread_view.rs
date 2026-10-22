//! Read-only render views of the v8 projections (#55).
//!
//! The desktop's transcript renderers (virtualized rows, incremental
//! markdown, fold and footer caches) consume messages, ordered activity
//! blocks and turns. [`ThreadView`] is that shape, derived — never edited,
//! never persisted, never sent back — from a [`ThreadState`] the `thread`
//! stream maintains, and [`ShellView`] is the sidebar's list derived from the
//! `shell` stream. Every [`TurnItemKind`] maps onto one existing row type:
//!
//! | Turn item | Row |
//! |---|---|
//! | `user_message` | user [`Message`] (opens the run's turn) |
//! | `assistant_message`, `error` | assistant [`Message`] |
//! | `reasoning` | reasoning activity |
//! | `command_execution` | command activity (output from `commandOutput` or `threads.turnItem` while expanded) |
//! | `file_change` | file-change activity (body from `threads.turnItem` while expanded) |
//! | `file_search`, `web_search` | file-search / search activity |
//! | `dynamic_tool`, `subagent` | tool activity |
//! | `proposed_plan`, `todo_list` | plan activity |
//! | `approval_request`, `user_input_request` | tool activity once answered; the pending request drives the prompt |
//! | `notification`, `system_notice`, `compaction`, `handoff`, `fork`, `thread_created`, `secret_request` | system notice [`Message`] |
//! | `checkpoint` | the turn's [`Checkpoint`] (changed-files row) |
//! | `run_interrupt_request`, `run_interrupt_result` | the turn's `Interrupted` status |
//!
//! Runs map onto [`AgentTurn`]s by ordinal; rolled-back runs and their items
//! disappear. Queued runs become [`QueuedMessage`]s. Building a view reuses
//! the rows of unchanged items ([`ViewCache`]), so a streaming publish costs
//! one rebuilt row plus pointer copies.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use uuid::Uuid;
use waku_protocol::model::{
    self, ActivityFileChange, ActivityFileChangeStatus, ActivityItem, ActivityKind, AgentTurn,
    MessageAttachment, MessageRole, PendingPermission, ProviderKind, QueuedMessage, ReasoningBlock,
    SessionStatus, SessionWorkspace, SharedVec, TranscriptBlock, TurnStatus,
};
use waku_protocol::orchestration::*;
use waku_protocol::v8::orchestration::{ThreadActivity, ThreadRow};

use crate::projection::{CommandOutputState, ShellState, ThreadState};

/// The title a thread shows before it has one.
pub const DEFAULT_TITLE: &str = "New task";

/// A thread as the desktop renders it: list columns always, the transcript
/// once the thread is open (`detail_loaded`).
#[derive(Clone, Debug)]
pub struct ThreadView {
    pub id: Uuid,
    pub project_id: Uuid,
    pub title: String,
    pub title_source: TitleSource,
    pub workspace: SessionWorkspace,
    pub instance: ProviderInstanceId,
    pub provider: ProviderKind,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub service_tier: Option<String>,
    pub context_window: Option<String>,
    pub agent_preset: Option<String>,
    pub runtime_mode: model::RuntimeMode,
    pub interaction_mode: InteractionMode,
    pub status: SessionStatus,
    /// Unix seconds.
    pub created_at: u64,
    pub updated_at: u64,
    pub last_reply_at: Option<u64>,
    pub pinned: bool,
    pub archived: bool,
    /// A reply arrived after the user last looked at the thread.
    pub unread: bool,
    pub pending_request_count: u32,
    pub queued_run_count: u32,
    /// The transcript below was built from an open `thread` stream; a shell
    /// row leaves it empty.
    pub detail_loaded: bool,
    pub messages: SharedVec<model::Message>,
    pub transcript_blocks: SharedVec<TranscriptBlock>,
    pub turns: Vec<AgentTurn>,
    pub queued_messages: Vec<QueuedMessage>,
    /// Slash commands the thread's open provider session reported
    /// (`ProviderSession.availableCommands`); the composer merges them with
    /// `composer.slashCommands`.
    pub available_commands: Vec<model::ReportedCommand>,
    pub pending_permission: Option<PendingPermission>,
    pub pending_user_input: Option<PendingUserInput>,
    pub thread_goal: Option<model::ThreadGoal>,
    pub context_usage: Option<model::ContextUsage>,
    /// The run a steer or an interrupt addresses.
    pub active_run_id: Option<RunId>,
    /// The live session accepts a message into a running turn (natively or
    /// by restarting it).
    pub can_steer: bool,
    /// The checkpoint that ends each completed run.
    pub run_checkpoints: Vec<RunCheckpoint>,
    /// Rewind points: the checkpoint each run started from (its turn start,
    /// captured before the provider turn), keyed by the run's ordinal.
    pub turn_starts: Vec<RunCheckpoint>,
    pub rollback: Option<RollbackState>,
    /// The failure of the newest run, when it failed.
    pub last_error: Option<String>,
    /// Subagents and detached provider work, for the background-work
    /// surfaces. A stoppable subagent's `control_id` is `<run>/<node>`.
    pub background_work: Vec<model::BackgroundWorkItem>,
    /// The provider's own id of the active conversation.
    pub provider_native_id: Option<String>,
    pub has_more_history: bool,
    pub history_loading: bool,
    /// The stream delivered its snapshot or replay and is live.
    pub synchronized: bool,
}

/// A pending structured question, as the composer asks it.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingUserInput {
    pub request_id: String,
    pub questions: Vec<model::UserInputQuestion>,
}

/// The checkpoint a completed run ended with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunCheckpoint {
    pub run_id: RunId,
    pub ordinal: u32,
    pub scope_id: CheckpointScopeId,
    pub checkpoint_id: CheckpointId,
    pub ready: bool,
}

impl ThreadView {
    /// A task the user is composing but has not sent: UI-local until its
    /// first message dispatches `thread.create`, and replaced by the
    /// stream's view as soon as that has started.
    pub fn draft(
        id: Uuid,
        project_id: Uuid,
        provider: ProviderKind,
        runtime_mode: model::RuntimeMode,
        now_seconds: u64,
    ) -> Self {
        Self {
            id,
            project_id,
            title: String::new(),
            title_source: TitleSource::Placeholder,
            workspace: SessionWorkspace::Local,
            instance: instance_of(provider),
            provider,
            model: None,
            reasoning_effort: None,
            service_tier: None,
            context_window: None,
            agent_preset: None,
            runtime_mode,
            interaction_mode: InteractionMode::Default,
            status: SessionStatus::Idle,
            created_at: now_seconds,
            updated_at: now_seconds,
            last_reply_at: None,
            pinned: false,
            archived: false,
            unread: false,
            pending_request_count: 0,
            queued_run_count: 0,
            detail_loaded: true,
            messages: SharedVec::new(),
            transcript_blocks: SharedVec::new(),
            turns: Vec::new(),
            queued_messages: Vec::new(),
            available_commands: Vec::new(),
            pending_permission: None,
            pending_user_input: None,
            thread_goal: None,
            context_usage: None,
            active_run_id: None,
            can_steer: false,
            run_checkpoints: Vec::new(),
            turn_starts: Vec::new(),
            rollback: None,
            last_error: None,
            background_work: Vec::new(),
            provider_native_id: None,
            has_more_history: false,
            history_loading: false,
            synchronized: true,
        }
    }

    /// A list row from the shell (no transcript).
    pub fn from_row(row: &ThreadRow) -> Self {
        let mut view = Self::from_thread(&row.thread);
        view.status = status_from_activity(&row.activity);
        view.pending_request_count = row.pending_request_count;
        view.queued_run_count = row.queued_run_count;
        view
    }

    fn from_thread(thread: &AppThread) -> Self {
        let selection = &thread.model_selection;
        Self {
            id: thread.id.0,
            project_id: thread.project_id.0,
            title: thread.title.clone(),
            title_source: thread.title_source,
            workspace: workspace_of(thread),
            instance: selection.instance.clone(),
            provider: provider_of(&selection.instance),
            model: selection.model.clone(),
            reasoning_effort: selection.reasoning_effort.clone(),
            service_tier: selection.service_tier.clone(),
            context_window: selection.context_window.clone(),
            agent_preset: selection.agent_preset.clone(),
            runtime_mode: legacy_runtime_mode(&thread.runtime_mode),
            interaction_mode: thread.interaction_mode,
            status: SessionStatus::Idle,
            created_at: seconds(thread.created_at),
            updated_at: seconds(thread.updated_at),
            last_reply_at: thread.last_reply_at.map(seconds),
            pinned: thread.pinned_at.is_some(),
            archived: thread.archived_at.is_some(),
            unread: thread
                .last_reply_at
                .is_some_and(|reply| thread.last_visited_at.is_none_or(|visit| visit < reply)),
            pending_request_count: 0,
            queued_run_count: 0,
            detail_loaded: false,
            messages: SharedVec::new(),
            transcript_blocks: SharedVec::new(),
            turns: Vec::new(),
            queued_messages: Vec::new(),
            available_commands: Vec::new(),
            pending_permission: None,
            pending_user_input: None,
            thread_goal: None,
            context_usage: None,
            active_run_id: None,
            can_steer: false,
            run_checkpoints: Vec::new(),
            turn_starts: Vec::new(),
            rollback: thread.rollback.clone(),
            last_error: None,
            background_work: Vec::new(),
            provider_native_id: None,
            has_more_history: false,
            history_loading: false,
            synchronized: false,
        }
    }

    /// The provider's own id of the active conversation.
    pub fn provider_native_id(&self) -> Option<&str> {
        self.provider_native_id.as_deref()
    }

    pub fn display_title(&self) -> &str {
        if self.title_source == TitleSource::Placeholder || self.title.trim().is_empty() {
            DEFAULT_TITLE
        } else {
            &self.title
        }
    }

    pub fn is_busy(&self) -> bool {
        self.status.is_busy()
    }

    /// Whether anything was ever sent: a placeholder thread with no reply is
    /// still a draft.
    pub fn has_started(&self) -> bool {
        self.last_reply_at.is_some() || !self.messages.is_empty() || !self.turns.is_empty()
    }

    pub fn active_turn_id(&self) -> Option<Uuid> {
        self.turns
            .last()
            .filter(|turn| turn.status == TurnStatus::Running)
            .map(|turn| turn.id)
    }

    /// The model picker may switch providers only before the first message.
    pub fn can_choose_model(&self, provider: ProviderKind) -> bool {
        !self.status.is_busy() && (!self.has_started() || self.provider == provider)
    }

    /// The checkpoint to roll back to so that run `ordinal` and everything
    /// after it disappear: the one the run started from (for the first run,
    /// the state before anything ran), else the one the previous run ended
    /// with.
    pub fn rewind_point(&self, ordinal: u32) -> Option<RunCheckpoint> {
        self.turn_starts
            .iter()
            .find(|checkpoint| checkpoint.ordinal == ordinal && checkpoint.ready)
            .or_else(|| {
                self.run_checkpoints
                    .iter()
                    .filter(|checkpoint| checkpoint.ordinal < ordinal && checkpoint.ready)
                    .max_by_key(|checkpoint| checkpoint.ordinal)
            })
            .copied()
    }

    /// The ordinal of the run whose turn id is `turn_id`.
    pub fn turn_ordinal(&self, turn_id: Uuid) -> Option<u32> {
        self.turns
            .iter()
            .find(|turn| turn.id == turn_id)
            .and_then(|turn| u32::try_from(turn.turn_count).ok())
    }
}

/// The sidebar's projects and thread rows.
#[derive(Clone, Debug, Default)]
pub struct ShellView {
    /// In `order_key` order.
    pub projects: Vec<model::Project>,
    pub threads: Vec<Arc<ThreadView>>,
    /// The first snapshot arrived whole and the stream is live.
    pub synchronized: bool,
}

impl ShellView {
    pub fn thread(&self, id: Uuid) -> Option<&Arc<ThreadView>> {
        self.threads.iter().find(|thread| thread.id == id)
    }
}

/// Rows of the last shell build, reused while their row is unchanged.
#[derive(Default)]
pub struct ShellCache {
    rows: HashMap<ThreadId, (ThreadRow, Arc<ThreadView>)>,
}

impl ShellCache {
    pub fn build(&mut self, shell: &ShellState) -> ShellView {
        let mut projects = shell
            .projects
            .values()
            .filter(|project| project.removed_at.is_none())
            .collect::<Vec<_>>();
        projects.sort_by(|left, right| {
            (&left.order_key, left.created_at, left.id).cmp(&(
                &right.order_key,
                right.created_at,
                right.id,
            ))
        });
        let projects = projects.into_iter().map(legacy_project).collect();
        let mut rows = HashMap::with_capacity(shell.threads.len());
        let mut threads = Vec::with_capacity(shell.threads.len());
        for (id, row) in &shell.threads {
            let view = match self.rows.remove(id) {
                Some((cached, view)) if &cached == row => view,
                _ => Arc::new(ThreadView::from_row(row)),
            };
            threads.push(view.clone());
            rows.insert(*id, (row.clone(), view));
        }
        self.rows = rows;
        threads.sort_by(|left, right| {
            right
                .last_reply_at
                .unwrap_or(right.updated_at)
                .cmp(&left.last_reply_at.unwrap_or(left.updated_at))
                .then(left.id.cmp(&right.id))
        });
        ShellView {
            projects,
            threads,
            synchronized: shell.synchronized && shell.complete,
        }
    }
}

/// What a view build may add beyond the stream: bodies the wire projection
/// omitted, fetched for rows the user expanded.
#[derive(Default)]
pub struct ViewExtras<'a> {
    /// Live or finished output of watched commands.
    pub outputs: Option<&'a HashMap<TurnItemId, CommandOutputState>>,
    /// Unprojected items from `threads.turnItem`.
    pub full_items: Option<&'a HashMap<TurnItemId, TurnItem>>,
    pub history_loading: bool,
}

#[derive(Clone, PartialEq)]
struct ItemKey {
    updated_at: Timestamp,
    stream_rev: u32,
    status: ItemStatus,
    text_len: usize,
    extra: u64,
}

enum Row {
    Message(Arc<model::Message>),
    Activity(ActivityItem),
    Checkpoint,
    Skip,
}

/// Rows of the last view build, reused while their item is unchanged.
#[derive(Default)]
pub struct ViewCache {
    messages: HashMap<TurnItemId, (ItemKey, Arc<model::Message>)>,
    activities: HashMap<TurnItemId, (ItemKey, ActivityItem)>,
    blocks: Vec<Arc<TranscriptBlock>>,
}

impl ViewCache {
    /// Builds the render view of an open thread.
    pub fn build(&mut self, state: &ThreadState, extras: &ViewExtras<'_>) -> Option<ThreadView> {
        let thread = state.thread.as_ref()?;
        let mut view = ThreadView::from_thread(thread);
        view.detail_loaded = true;
        view.synchronized = state.synchronized;
        view.has_more_history = state.has_more_history;
        view.history_loading = extras.history_loading;

        let mut runs = state.runs.values().collect::<Vec<_>>();
        runs.sort_by_key(|run| (run.ordinal.unwrap_or(u32::MAX), run.requested_at, run.id));
        let rolled_back = runs
            .iter()
            .filter(|run| run.status == RunStatus::RolledBack)
            .map(|run| run.id)
            .collect::<std::collections::HashSet<_>>();

        // Turns: every run that took an ordinal and was not rolled back.
        for run in &runs {
            let Some(ordinal) = run.ordinal else {
                continue;
            };
            if run.status == RunStatus::RolledBack
                || matches!(run.status, RunStatus::Queued | RunStatus::Preparing)
                    && run.started_at.is_none()
            {
                continue;
            }
            let checkpoint = run
                .checkpoint_id
                .and_then(|id| state.checkpoints.get(&id))
                .map(|checkpoint| legacy_checkpoint(checkpoint, ordinal));
            if let Some(start) = turn_start(state, run) {
                view.turn_starts.push(RunCheckpoint {
                    run_id: run.id,
                    ordinal,
                    scope_id: start.scope_id,
                    checkpoint_id: start.id,
                    ready: true,
                });
            }
            if let Some(checkpoint) = run.checkpoint_id.and_then(|id| state.checkpoints.get(&id)) {
                view.run_checkpoints.push(RunCheckpoint {
                    run_id: run.id,
                    ordinal,
                    scope_id: checkpoint.scope_id,
                    checkpoint_id: checkpoint.id,
                    ready: checkpoint.status == CheckpointStatus::Ready,
                });
            }
            view.turns.push(AgentTurn {
                id: run.id.0,
                turn_count: ordinal as usize,
                status: turn_status(&run.status),
                provider_turn_started: matches!(
                    run.status,
                    RunStatus::Running | RunStatus::Waiting | RunStatus::Finalizing
                ) || run.status.is_terminal(),
                provider_resume_at: None,
                started_at: seconds(run.started_at.unwrap_or(run.requested_at)),
                completed_at: run.completed_at.map(seconds),
                checkpoint,
            });
        }

        // Queued messages: queued runs, in queue order, with their prompt.
        let mut queued = runs
            .iter()
            .filter(|run| run.status == RunStatus::Queued)
            .collect::<Vec<_>>();
        queued.sort_by_key(|run| (run.queue_position.unwrap_or(u32::MAX), run.requested_at));
        for run in queued {
            let message = state.messages.get(&run.user_message_id);
            view.queued_messages.push(QueuedMessage {
                id: run.id.0,
                content: message
                    .map(|message| message.text.clone())
                    .unwrap_or_default(),
                display_content: message.and_then(|message| message.display_text.clone()),
                attachments: message
                    .map(|message| message.attachments.iter().map(legacy_attachment).collect())
                    .unwrap_or_default(),
                created_at: seconds(run.requested_at),
            });
        }

        let active = runs.iter().find(|run| run.status.is_active());
        view.active_run_id = active.map(|run| run.id);
        view.status = status_from_runs(&runs, state);
        view.last_error = runs
            .iter()
            .rev()
            .find(|run| run.status.is_terminal() && run.status != RunStatus::RolledBack)
            .and_then(|run| run.failure.as_ref())
            .map(|failure| failure.message.clone());
        view.can_steer = state.provider_sessions.values().any(|session| {
            session.detached_at.is_none()
                && session.provider_instance_id == thread.model_selection.instance
                && (session.capabilities.turns.active_steering
                    || session.capabilities.turns.steering_by_interrupt_restart)
        });
        view.available_commands = session_commands(state, &thread.model_selection.instance);
        if let Some(provider_thread) = thread
            .active_provider_thread_id
            .and_then(|id| state.provider_threads.get(&id))
        {
            view.thread_goal = provider_thread.goal.as_ref().map(legacy_goal);
            view.provider_native_id = provider_thread
                .native_thread_ref
                .as_ref()
                .and_then(|reference| reference.native_id.clone());
            for task in &provider_thread.pending_background_tasks {
                if task.kind == PendingTaskKind::Subagent {
                    // Its subagent row below says more.
                    continue;
                }
                let kind = match task.kind {
                    PendingTaskKind::Monitor => model::BackgroundWorkKind::Monitor,
                    _ => model::BackgroundWorkKind::Process,
                };
                view.background_work.push(process_work(
                    provider_thread.id,
                    task,
                    kind,
                    state,
                    extras.outputs,
                ));
            }
            view.context_usage =
                provider_thread
                    .context_usage
                    .as_ref()
                    .map(|usage| model::ContextUsage {
                        tokens: usage.tokens,
                        window: usage.window,
                    });
        }
        let mut subagents = state.subagents.values().collect::<Vec<_>>();
        subagents.sort_by_key(|subagent| (subagent.started_at, subagent.id));
        for subagent in subagents {
            let status = match subagent.status {
                SubagentStatus::Completed => model::BackgroundWorkStatus::Completed,
                SubagentStatus::Failed => model::BackgroundWorkStatus::Failed,
                SubagentStatus::Cancelled | SubagentStatus::Interrupted => {
                    model::BackgroundWorkStatus::Stopped
                }
                SubagentStatus::Pending | SubagentStatus::Idle => {
                    model::BackgroundWorkStatus::Starting
                }
                _ => model::BackgroundWorkStatus::Running,
            };
            let mut item = model::BackgroundWorkItem::new(
                model::BackgroundWorkKind::Subagent,
                subagent.id.to_string(),
                subagent
                    .title
                    .clone()
                    .unwrap_or_else(|| first_line(&subagent.prompt)),
                status,
            );
            item.detail = subagent
                .result
                .clone()
                .or_else(|| subagent.progress.clone());
            item.model = subagent.model.clone();
            item.background = true;
            item.started_at_ms = subagent.started_at.unwrap_or(subagent.updated_at).max(0) as u64;
            item.updated_at_ms = subagent.updated_at.max(0) as u64;
            if let Some(run_id) = subagent.run_id
                && status.is_stoppable()
            {
                item.can_stop = true;
                item.control_id = Some(format!("{run_id}/{}", subagent.id));
            }
            view.background_work.push(item);
        }
        let (permission, user_input) = pending_requests(state);
        view.pending_permission = permission;
        view.pending_user_input = user_input;
        view.pending_request_count = state
            .runtime_requests
            .values()
            .filter(|request| request.status == RequestStatus::Pending)
            .count() as u32;
        view.queued_run_count = view.queued_messages.len() as u32;

        // Rows, in position order.
        let mut messages = SharedVec::new();
        let mut blocks: Vec<TranscriptBlock> = Vec::new();
        let mut messages_seen = HashMap::new();
        let mut activities_seen = HashMap::new();
        for item in state.ordered_turn_items() {
            if item.run_id.is_some_and(|run| rolled_back.contains(&run)) {
                continue;
            }
            let full = extras
                .full_items
                .and_then(|items| items.get(&item.id))
                .filter(|full| full.updated_at >= item.updated_at);
            let output = extras.outputs.and_then(|outputs| outputs.get(&item.id));
            let source = full.unwrap_or(item);
            let key = ItemKey {
                updated_at: source.updated_at,
                stream_rev: source.stream_rev,
                status: source.status,
                text_len: text_len(&source.kind),
                extra: extra_key(full.is_some(), output),
            };
            let row = match self.messages.remove(&item.id) {
                Some((cached, message)) if cached == key => Row::Message(message),
                _ => match self.activities.remove(&item.id) {
                    Some((cached, activity)) if cached == key => Row::Activity(activity),
                    _ => map_item(source, output),
                },
            };
            let turn_id = item.run_id.map(|run| run.0);
            match row {
                Row::Message(message) => {
                    messages_seen.insert(item.id, (key, message.clone()));
                    messages.push_shared(message);
                }
                Row::Activity(activity) => {
                    activities_seen.insert(item.id, (key, activity.clone()));
                    let after_message = messages.len();
                    match blocks.last_mut() {
                        Some(block)
                            if block.after_message == after_message && block.turn_id == turn_id =>
                        {
                            block.activities.push(activity);
                        }
                        _ => blocks.push(TranscriptBlock {
                            after_message,
                            turn_id,
                            activities: vec![activity],
                        }),
                    }
                }
                Row::Checkpoint | Row::Skip => {}
            }
        }
        self.messages = messages_seen;
        self.activities = activities_seen;

        // Reuse an unchanged block's allocation so the renderer's shared
        // rows stay shared.
        let mut previous = std::mem::take(&mut self.blocks)
            .into_iter()
            .filter_map(|block| Some(((block.after_message, block.activities.first()?.id), block)))
            .collect::<HashMap<_, _>>();
        let mut shared = SharedVec::with_capacity(blocks.len());
        for block in blocks {
            let key = (
                block.after_message,
                block.activities.first().map(|activity| activity.id),
            );
            let reuse = key
                .1
                .and_then(|first| previous.remove(&(key.0, first)))
                .filter(|old| same_block(old, &block));
            let block = reuse.unwrap_or_else(|| Arc::new(block));
            self.blocks.push(block.clone());
            shared.push_shared(block);
        }
        view.messages = messages;
        view.transcript_blocks = shared;
        Some(view)
    }
}

/// The ready checkpoint `run` started from: ordinal 0 of its root scope,
/// captured before its provider turn (the daemon's `checkpoints::turn_start`).
fn turn_start<'a>(state: &'a ThreadState, run: &Run) -> Option<&'a Checkpoint> {
    state.checkpoints.values().find(|checkpoint| {
        checkpoint.run_id == Some(run.id)
            && Some(checkpoint.id) != run.checkpoint_id
            && checkpoint.ordinal_within_scope == 0
            && checkpoint.app_run_ordinal.is_some()
            && checkpoint.status == CheckpointStatus::Ready
            && state
                .checkpoint_scopes
                .get(&checkpoint.scope_id)
                .is_none_or(|scope| scope.kind == ScopeKind::RootRun)
    })
}

/// The slash commands the thread's open sessions of its provider instance
/// reported, newest session first, one per name.
fn session_commands(
    state: &ThreadState,
    instance: &ProviderInstanceId,
) -> Vec<model::ReportedCommand> {
    let mut sessions = state
        .provider_sessions
        .values()
        .filter(|session| {
            session.detached_at.is_none()
                && &session.provider_instance_id == instance
                && !matches!(
                    session.status,
                    waku_protocol::orchestration::SessionStatus::Stopped
                        | waku_protocol::orchestration::SessionStatus::Error
                )
        })
        .collect::<Vec<_>>();
    sessions.sort_by_key(|session| std::cmp::Reverse((session.updated_at, session.id)));
    let mut seen = std::collections::HashSet::new();
    let mut commands = Vec::new();
    for command in sessions
        .into_iter()
        .flat_map(|session| &session.available_commands)
    {
        if seen.insert(command.name.clone()) {
            commands.push(model::ReportedCommand {
                name: command.name.clone(),
                description: command.description.clone().unwrap_or_default(),
            });
        }
    }
    commands
}

/// What a background-work item's `control_id` stops.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BackgroundControl {
    /// `subagent.stop`.
    Subagent { run_id: RunId, node_id: NodeId },
    /// `background_work.stop`.
    Process {
        provider_thread_id: ProviderThreadId,
        task_id: String,
    },
}

/// Parses the `control_id` a view gave a background-work item of `kind`:
/// `<run>/<node>` for a subagent, `<provider thread>/<task>` for a process
/// or monitor.
pub fn background_control(
    kind: model::BackgroundWorkKind,
    control_id: &str,
) -> Option<BackgroundControl> {
    let (owner, rest) = control_id.split_once('/')?;
    let owner = owner.parse::<Uuid>().ok()?;
    match kind {
        model::BackgroundWorkKind::Subagent => Some(BackgroundControl::Subagent {
            run_id: RunId(owner),
            node_id: NodeId(rest.parse().ok()?),
        }),
        model::BackgroundWorkKind::Process | model::BackgroundWorkKind::Monitor => {
            (!rest.is_empty()).then(|| BackgroundControl::Process {
                provider_thread_id: ProviderThreadId(owner),
                task_id: rest.to_owned(),
            })
        }
    }
}

/// A process or monitor a provider thread reports: stoppable through
/// `background_work.stop`, its output from the `commandOutput` stream of the
/// command item that started it (while watched).
fn process_work(
    provider_thread_id: ProviderThreadId,
    task: &PendingBackgroundTask,
    kind: model::BackgroundWorkKind,
    state: &ThreadState,
    outputs: Option<&HashMap<TurnItemId, CommandOutputState>>,
) -> model::BackgroundWorkItem {
    let status = match kind {
        model::BackgroundWorkKind::Monitor => model::BackgroundWorkStatus::Monitoring,
        _ => model::BackgroundWorkStatus::Running,
    };
    let mut item = model::BackgroundWorkItem::new(
        kind,
        task.task_id.clone(),
        task.description
            .clone()
            .unwrap_or_else(|| task.task_id.clone()),
        status,
    );
    item.background = true;
    item.can_stop = true;
    item.control_id = Some(format!("{provider_thread_id}/{}", task.task_id));
    item.command = task.description.clone();
    if let Some(item_id) = task.item_id {
        item.origin_activity_id = Some(item_id.0.to_string());
        if let Some(started) = state
            .turn_items
            .get(&item_id)
            .and_then(|turn_item| turn_item.started_at)
        {
            item.started_at_ms = started.max(0) as u64;
        }
        if let Some(output) = outputs.and_then(|outputs| outputs.get(&item_id)) {
            item.output = Some(output.text.clone());
            item.output_truncated = output.truncated;
            item.exit_code = output.exit_code;
        }
    }
    item
}

fn extra_key(full: bool, output: Option<&CommandOutputState>) -> u64 {
    let output = output.map_or(0, |output| {
        (output.text.len() as u64) << 3
            | u64::from(output.running) << 2
            | u64::from(output.exit_code.is_some()) << 1
            | 1
    });
    output << 1 | u64::from(full)
}

fn same_block(left: &TranscriptBlock, right: &TranscriptBlock) -> bool {
    left.after_message == right.after_message
        && left.turn_id == right.turn_id
        && left.activities.len() == right.activities.len()
        && left
            .activities
            .iter()
            .zip(&right.activities)
            .all(|(left, right)| same_activity(left, right))
}

fn same_activity(left: &ActivityItem, right: &ActivityItem) -> bool {
    left.id == right.id
        && left.complete == right.complete
        && left.failed == right.failed
        && left.title == right.title
        && left.output.as_ref().map(String::len) == right.output.as_ref().map(String::len)
        && left.detail.as_ref().map(String::len) == right.detail.as_ref().map(String::len)
        && left.reasoning.as_ref().map(|block| block.content.len())
            == right.reasoning.as_ref().map(|block| block.content.len())
        && left.file_changes.len() == right.file_changes.len()
        && left
            .file_changes
            .iter()
            .zip(&right.file_changes)
            .all(|(left, right)| {
                left.diff.as_ref().map(String::len) == right.diff.as_ref().map(String::len)
            })
}

fn text_len(kind: &TurnItemKind) -> usize {
    match kind {
        TurnItemKind::AssistantMessage { text, .. }
        | TurnItemKind::Reasoning { text, .. }
        | TurnItemKind::UserMessage { text, .. } => text.len(),
        TurnItemKind::ProposedPlan { markdown, .. } => markdown.len(),
        TurnItemKind::CommandExecution { output, .. } => output.as_ref().map_or(0, String::len),
        TurnItemKind::Subagent {
            progress, result, ..
        } => progress.as_ref().map_or(0, String::len) + result.as_ref().map_or(0, String::len),
        _ => 0,
    }
}

/// Maps one turn item onto the row the desktop renders for it.
fn map_item(item: &TurnItem, output: Option<&CommandOutputState>) -> Row {
    let id = item.id.0;
    let turn_id = item.run_id.map(|run| run.0);
    let created_at = seconds(item.started_at.unwrap_or(item.updated_at));
    let complete = item.status.is_terminal();
    let failed = matches!(item.status, ItemStatus::Failed);
    let title = item.title.clone();
    let activity = |kind: ActivityKind, fallback: &str| {
        let mut activity = ActivityItem::new(
            None,
            kind,
            title.clone().unwrap_or_else(|| fallback.to_owned()),
            None,
            complete,
        );
        activity.id = id;
        activity.failed = failed;
        activity
    };
    let message = |role: MessageRole, content: String, streaming: bool| {
        Row::Message(Arc::new(model::Message {
            id,
            turn_id,
            role,
            content,
            display_content: None,
            attachments: Vec::new(),
            created_at,
            streaming,
        }))
    };
    match &item.kind {
        TurnItemKind::UserMessage {
            message_id,
            text,
            display_text,
            attachments,
            ..
        } => Row::Message(Arc::new(model::Message {
            id: message_id.0,
            turn_id,
            role: MessageRole::User,
            content: text.clone(),
            display_content: display_text.clone(),
            attachments: attachments.iter().map(legacy_attachment).collect(),
            created_at,
            streaming: false,
        })),
        TurnItemKind::AssistantMessage {
            message_id,
            text,
            streaming,
        } => Row::Message(Arc::new(model::Message {
            id: message_id.0,
            turn_id,
            role: MessageRole::Assistant,
            content: text.clone(),
            display_content: None,
            attachments: Vec::new(),
            created_at,
            streaming: *streaming,
        })),
        TurnItemKind::Error { failure, .. } => {
            message(MessageRole::Assistant, failure.message.clone(), false)
        }
        TurnItemKind::Reasoning { text, streaming } => {
            let started = item.started_at.unwrap_or(item.updated_at).max(0) as u64;
            let finished = item.completed_at.unwrap_or(item.updated_at).max(0) as u64;
            let mut activity = ActivityItem::from_reasoning(
                ReasoningBlock {
                    content: text.clone(),
                    started_at_ms: started,
                    finished_at_ms: finished,
                },
                !*streaming && complete,
            );
            activity.id = id;
            Row::Activity(activity)
        }
        TurnItemKind::CommandExecution {
            input,
            output: stored,
            output_indicates_failure,
            exit_code,
            ..
        } => {
            let mut activity = activity(ActivityKind::Command, "Command")
                .with_arguments(Some(input.clone()))
                .with_output(
                    output
                        .map(|output| output.text.clone())
                        .or_else(|| stored.clone()),
                );
            activity.id = id;
            activity.failed = failed
                || *output_indicates_failure
                || exit_code.is_some_and(|code| code != 0)
                || output.is_some_and(|output| output.exit_code.is_some_and(|code| code != 0));
            Row::Activity(activity)
        }
        TurnItemKind::FileChange {
            file_name,
            additions,
            deletions,
            diff,
            old_text,
            new_text,
            changes,
            ..
        } => {
            let mut activity = activity(ActivityKind::FileChange, file_name);
            let body = diff
                .clone()
                .or_else(|| synthesize_diff(old_text.as_deref(), new_text.as_deref()));
            activity.file_changes = if changes.is_empty() {
                vec![ActivityFileChange {
                    path: file_name.clone(),
                    additions: additions.map(u64::from),
                    deletions: deletions.map(u64::from),
                    status: None,
                    diff: body,
                }]
            } else {
                let single = changes.len() == 1;
                changes
                    .iter()
                    .map(|change| ActivityFileChange {
                        path: change.path.clone(),
                        additions: single.then(|| additions.map(u64::from)).flatten(),
                        deletions: single.then(|| deletions.map(u64::from)).flatten(),
                        status: match change.operation.as_str() {
                            "add" | "added" | "create" => Some(ActivityFileChangeStatus::Added),
                            "delete" | "deleted" | "remove" => {
                                Some(ActivityFileChangeStatus::Deleted)
                            }
                            _ => Some(ActivityFileChangeStatus::Modified),
                        },
                        diff: single.then(|| body.clone()).flatten(),
                    })
                    .collect()
            };
            activity.display_target = None;
            Row::Activity(activity)
        }
        TurnItemKind::FileSearch { pattern, results } => {
            let mut activity = activity(ActivityKind::FileSearch, "Search files");
            activity.display_target = pattern.clone();
            if !results.is_empty() {
                activity.output = Some(
                    results
                        .iter()
                        .map(|result| match result.line {
                            Some(line) => format!("{}:{line}", result.file_name),
                            None => result.file_name.clone(),
                        })
                        .collect::<Vec<_>>()
                        .join("\n"),
                );
            }
            Row::Activity(activity)
        }
        TurnItemKind::WebSearch { patterns, results } => {
            let mut activity = activity(ActivityKind::Search, "Web search");
            activity.display_target = patterns.first().cloned();
            if !results.is_empty() {
                activity.output = Some(
                    results
                        .iter()
                        .filter_map(|result| result.url.clone().or_else(|| result.title.clone()))
                        .collect::<Vec<_>>()
                        .join("\n"),
                );
            }
            Row::Activity(activity)
        }
        TurnItemKind::DynamicTool {
            tool_name,
            input,
            output,
            ..
        } => {
            let name = tool_name.as_deref().unwrap_or("Tool");
            let kind = ActivityKind::from_tool_name(name);
            let mut activity = activity(
                if kind == ActivityKind::Plan {
                    ActivityKind::Tool
                } else {
                    kind
                },
                name,
            )
            .with_tool_name(Some(name))
            .with_arguments(Some(input.0.clone()))
            .with_output(output.as_ref().map(|output| output.0.clone()));
            activity.id = id;
            activity.failed = failed;
            Row::Activity(activity)
        }
        TurnItemKind::Subagent {
            prompt,
            progress,
            result,
            ..
        } => {
            let mut activity = activity(ActivityKind::Tool, "Subagent");
            activity.tool_name = Some("subagent".into());
            activity.display_target = Some(first_line(prompt));
            activity.arguments = Some(prompt.clone());
            activity.output = result.clone().or_else(|| progress.clone());
            Row::Activity(activity)
        }
        TurnItemKind::ProposedPlan { markdown, .. } => {
            let mut activity = activity(ActivityKind::Plan, "Plan");
            activity.detail = Some(markdown.clone());
            Row::Activity(activity)
        }
        TurnItemKind::TodoList {
            steps, explanation, ..
        } => {
            let mut activity = activity(ActivityKind::Plan, "Plan");
            let mut detail = explanation.clone().unwrap_or_default();
            for step in steps {
                if !detail.is_empty() {
                    detail.push('\n');
                }
                let mark = match step.status {
                    PlanStepStatus::Completed => "[x]",
                    PlanStepStatus::Running => "[~]",
                    _ => "[ ]",
                };
                detail.push_str(&format!("- {mark} {}", step.text));
            }
            activity.detail = Some(detail);
            Row::Activity(activity)
        }
        TurnItemKind::ApprovalRequest {
            prompt, options, ..
        } => {
            // A pending request is asked by the composer; once answered it
            // stays in the transcript as a compact row.
            if !complete {
                return Row::Skip;
            }
            let mut activity = activity(ActivityKind::Tool, "Approval");
            activity.display_target = prompt.as_deref().map(first_line);
            activity.detail = prompt.clone();
            activity.output = (!options.is_empty()).then(|| {
                options
                    .iter()
                    .map(|option| option.label.clone())
                    .collect::<Vec<_>>()
                    .join(" · ")
            });
            Row::Activity(activity)
        }
        TurnItemKind::UserInputRequest {
            questions, answers, ..
        } => {
            if !complete {
                return Row::Skip;
            }
            let mut activity = activity(ActivityKind::Tool, "Question");
            activity.display_target = questions.first().map(|question| question.header.clone());
            let mut detail = String::new();
            for question in questions {
                if !detail.is_empty() {
                    detail.push('\n');
                }
                detail.push_str(&question.question);
                if let Some(answer) = answers
                    .iter()
                    .flatten()
                    .find(|answer| answer.question_id == question.id)
                {
                    detail.push_str(&format!(" — {}", answer.answers.join(", ")));
                }
            }
            activity.detail = Some(detail);
            Row::Activity(activity)
        }
        TurnItemKind::Notification(notification) => message(
            MessageRole::System,
            notification.detail.as_ref().map_or_else(
                || notification.summary.clone(),
                |detail| format!("{}: {detail}", notification.summary),
            ),
            false,
        ),
        // Lifecycle rows (restart recovery, #59) render as the same pill;
        // their message reads on its own, with a fallback for an empty one.
        TurnItemKind::SystemNotice {
            message: text,
            lifecycle,
        } => {
            let text = match lifecycle {
                _ if !text.trim().is_empty() => text.clone(),
                Some(LifecycleNotice::RestartInterrupted) => {
                    "Interrupted when the daemon restarted".to_owned()
                }
                Some(LifecycleNotice::RestartResumed) => {
                    "Resumed after the daemon restarted".to_owned()
                }
                _ => text.clone(),
            };
            message(MessageRole::System, text, false)
        }
        TurnItemKind::Compaction { summary, .. } => message(
            MessageRole::System,
            summary.as_deref().map_or_else(
                || "Context compacted".to_owned(),
                |summary| format!("Context compacted: {}", first_line(summary)),
            ),
            false,
        ),
        TurnItemKind::Handoff {
            to_provider_instance_id,
            ..
        } => message(
            MessageRole::System,
            format!("Handed off to {to_provider_instance_id}"),
            false,
        ),
        TurnItemKind::Fork { .. } => message(MessageRole::System, "Forked".into(), false),
        TurnItemKind::ThreadCreated {
            target_provider_instance_id,
            target_model,
            ..
        } => message(
            MessageRole::System,
            format!("Continued with {target_provider_instance_id} {target_model}"),
            false,
        ),
        TurnItemKind::SecretRequest { label, reason, .. } => {
            message(MessageRole::System, format!("{label}: {reason}"), false)
        }
        TurnItemKind::Checkpoint { .. } => Row::Checkpoint,
        TurnItemKind::RunInterruptRequest { .. }
        | TurnItemKind::RunInterruptResult { .. }
        | TurnItemKind::Unknown => Row::Skip,
    }
}

fn synthesize_diff(old: Option<&str>, new: Option<&str>) -> Option<String> {
    if old.is_none() && new.is_none() {
        return None;
    }
    let mut diff = String::from("@@\n");
    for line in old.unwrap_or_default().lines() {
        diff.push('-');
        diff.push_str(line);
        diff.push('\n');
    }
    for line in new.unwrap_or_default().lines() {
        diff.push('+');
        diff.push_str(line);
        diff.push('\n');
    }
    Some(diff)
}

fn first_line(text: &str) -> String {
    let line = text.lines().next().unwrap_or_default().trim();
    match line.char_indices().nth(120) {
        Some((end, _)) => format!("{}…", &line[..end]),
        None => line.to_owned(),
    }
}

/// The oldest pending request the user must answer.
fn pending_requests(state: &ThreadState) -> (Option<PendingPermission>, Option<PendingUserInput>) {
    let mut pending = state
        .runtime_requests
        .values()
        .filter(|request| request.status == RequestStatus::Pending && request.response.is_none())
        .collect::<Vec<_>>();
    pending.sort_by_key(|request| (request.created_at, request.id));
    let mut permission = None;
    let mut user_input = None;
    for request in pending {
        if request.kind == RuntimeRequestKind::UserInput {
            if user_input.is_none() && !request.questions.is_empty() {
                user_input = Some(PendingUserInput {
                    request_id: request.id.to_string(),
                    questions: request.questions.iter().map(legacy_question).collect(),
                });
            }
            continue;
        }
        if permission.is_some() {
            continue;
        }
        let prompt = state.turn_items.values().find_map(|item| match &item.kind {
            TurnItemKind::ApprovalRequest {
                request_id, prompt, ..
            } if *request_id == request.id => prompt.clone(),
            _ => None,
        });
        let prompt = prompt.unwrap_or_default();
        let (title, detail) = match prompt.split_once('\n') {
            Some((title, detail)) => (title.trim().to_owned(), detail.trim().to_owned()),
            None => (request_title(&request.kind).to_owned(), prompt),
        };
        permission = Some(PendingPermission {
            request_id: request.id.to_string(),
            title,
            detail,
            options: request
                .options
                .iter()
                .map(|option| model::PermissionOption {
                    id: option.id.clone(),
                    label: option.label.clone(),
                    allow: option.allow,
                })
                .collect(),
        });
    }
    (permission, user_input)
}

fn request_title(kind: &RuntimeRequestKind) -> &'static str {
    match kind {
        RuntimeRequestKind::CommandApproval => "Run this command?",
        RuntimeRequestKind::FileReadApproval => "Read this file?",
        RuntimeRequestKind::FileChangeApproval | RuntimeRequestKind::ApplyPatchApproval => {
            "Apply these changes?"
        }
        RuntimeRequestKind::DynamicToolCall => "Allow this tool?",
        _ => "Allow this?",
    }
}

fn status_from_runs(runs: &[&Run], state: &ThreadState) -> SessionStatus {
    if let Some(active) = runs.iter().find(|run| run.status.is_active()) {
        return match active.status {
            RunStatus::Preparing | RunStatus::Starting => SessionStatus::Connecting,
            RunStatus::Waiting => SessionStatus::Waiting,
            _ => SessionStatus::Working,
        };
    }
    if state
        .runtime_requests
        .values()
        .any(|request| request.status == RequestStatus::Pending)
    {
        return SessionStatus::Waiting;
    }
    match runs
        .iter()
        .rev()
        .find(|run| run.status.is_terminal() && run.status != RunStatus::RolledBack)
    {
        Some(run) if run.status == RunStatus::Failed => SessionStatus::Failed,
        _ => SessionStatus::Idle,
    }
}

/// The sidebar status of a shell row.
pub fn status_from_activity(activity: &ThreadActivity) -> SessionStatus {
    match activity {
        ThreadActivity::Preparing => SessionStatus::Connecting,
        ThreadActivity::Working | ThreadActivity::Finalizing => SessionStatus::Working,
        ThreadActivity::Waiting => SessionStatus::Waiting,
        ThreadActivity::Failed => SessionStatus::Failed,
        _ => SessionStatus::Idle,
    }
}

fn turn_status(status: &RunStatus) -> TurnStatus {
    match status {
        RunStatus::Completed => TurnStatus::Completed,
        RunStatus::Failed => TurnStatus::Failed,
        RunStatus::Interrupted | RunStatus::Cancelled => TurnStatus::Interrupted,
        _ => TurnStatus::Running,
    }
}

fn legacy_checkpoint(checkpoint: &Checkpoint, ordinal: u32) -> model::Checkpoint {
    let files = checkpoint
        .files
        .iter()
        .map(|file| model::CheckpointFile {
            path: file.path.clone(),
            additions: u64::from(file.additions),
            deletions: u64::from(file.deletions),
        })
        .collect::<Vec<_>>();
    let mut legacy = model::Checkpoint {
        turn_count: ordinal as usize,
        git_ref: checkpoint.git_ref.clone(),
        status: match checkpoint.status {
            CheckpointStatus::Ready => model::CheckpointStatus::Ready,
            CheckpointStatus::Error => model::CheckpointStatus::Error,
            _ => model::CheckpointStatus::Unavailable,
        },
        files,
        additions: 0,
        deletions: 0,
        created_at: seconds(checkpoint.captured_at),
    };
    legacy.refresh_totals();
    legacy
}

fn legacy_goal(goal: &ThreadGoal) -> model::ThreadGoal {
    model::ThreadGoal {
        objective: goal.objective.clone(),
        status: match goal.status {
            ThreadGoalStatus::Paused => model::ThreadGoalStatus::Paused,
            ThreadGoalStatus::Blocked => model::ThreadGoalStatus::Blocked,
            ThreadGoalStatus::UsageLimited => model::ThreadGoalStatus::UsageLimited,
            ThreadGoalStatus::BudgetLimited => model::ThreadGoalStatus::BudgetLimited,
            ThreadGoalStatus::Complete => model::ThreadGoalStatus::Complete,
            _ => model::ThreadGoalStatus::Active,
        },
        token_budget: goal.token_budget,
        tokens_used: goal.tokens_used,
        time_used_seconds: goal.time_used_seconds,
    }
}

fn legacy_question(question: &UserInputQuestion) -> model::UserInputQuestion {
    model::UserInputQuestion {
        id: question.id.clone(),
        header: question.header.clone(),
        question: question.question.clone(),
        options: question
            .options
            .iter()
            .map(|option| model::UserInputOption {
                label: option.label.clone(),
                description: option.description.clone(),
            })
            .collect(),
        multi_select: question.multi_select,
    }
}

/// An orchestration attachment as the transcript's attachment chips show it.
pub fn legacy_attachment(attachment: &Attachment) -> MessageAttachment {
    MessageAttachment {
        path: attachment.path.clone(),
        mention: attachment.mention.clone(),
        name: attachment.name.clone(),
        is_dir: attachment.is_dir,
        is_image: attachment.is_image,
        blob_reference: attachment.blob_reference.clone(),
    }
}

/// An orchestration project in the shape the desktop's project lists use.
pub fn legacy_project(project: &Project) -> model::Project {
    model::Project {
        id: project.id.0,
        name: project.name.clone(),
        path: project.path.clone(),
        created_at: seconds(project.created_at),
    }
}

fn workspace_of(thread: &AppThread) -> SessionWorkspace {
    match &thread.worktree_path {
        Some(path) => SessionWorkspace::Worktree {
            path: path.clone(),
            branch: thread.branch.clone().unwrap_or_default(),
        },
        None => SessionWorkspace::Local,
    }
}

/// The workspace strategy a new thread's first run uses.
pub fn workspace_strategy(workspace: &SessionWorkspace) -> WorkspaceStrategy {
    match workspace {
        SessionWorkspace::Local => WorkspaceStrategy::Root { branch: None },
        SessionWorkspace::NewWorktree { base_branch } => WorkspaceStrategy::NewWorktree {
            base_ref: base_branch.clone().unwrap_or_else(|| "HEAD".into()),
            branch: None,
            start_from_origin: false,
        },
        SessionWorkspace::Worktree { path, branch } => WorkspaceStrategy::ExistingWorktree {
            path: PathBuf::from(path),
            branch: (!branch.is_empty()).then(|| branch.clone()),
        },
    }
}

/// The provider behind an instance id (`codex`, `claude:work`, …).
pub fn provider_of(instance: &ProviderInstanceId) -> ProviderKind {
    let tag = instance.driver_tag();
    ProviderKind::ALL
        .into_iter()
        .find(|provider| provider.id() == tag)
        .unwrap_or_else(|| match DriverKind::from_tag(tag) {
            DriverKind::DeepSeek => ProviderKind::DeepSeek,
            DriverKind::OpenCode => ProviderKind::OpenCode,
            DriverKind::OhMyPi => ProviderKind::OhMyPi,
            _ => ProviderKind::default(),
        })
}

/// The instance a provider runs as until instances are configurable.
pub fn instance_of(provider: ProviderKind) -> ProviderInstanceId {
    ProviderInstanceId::new(provider.id())
}

pub fn legacy_runtime_mode(mode: &RuntimeMode) -> model::RuntimeMode {
    match mode {
        RuntimeMode::Ask => model::RuntimeMode::Ask,
        RuntimeMode::AutoAcceptEdits => model::RuntimeMode::AutoAcceptEdits,
        RuntimeMode::Auto => model::RuntimeMode::Auto,
        RuntimeMode::FullAccess => model::RuntimeMode::FullAccess,
        // A mode this build does not know is treated as the most careful.
        _ => model::RuntimeMode::Ask,
    }
}

/// Milliseconds since the epoch to seconds.
pub fn seconds(milliseconds: Timestamp) -> u64 {
    (milliseconds.max(0) / 1000) as u64
}

#[cfg(test)]
mod tests;
