//! `V1Bridge`: runs any ported `DriverSession` behind the old
//! `DriverControl` interface until the orchestrator cutover (RFC 0002 D-9).
//!
//! It inverts the `DriverEvent` → `AdapterEvent` table: entity snapshots
//! become the deltas and lifecycle events the desktop interprets today.
//! `DriverControl` methods are synchronous and may be called from any
//! thread, so they only enqueue commands; one actor task on the runtime runs
//! them in order, which keeps the old single-writer behaviour. The few
//! methods that return a value (`apply_options`, `rollback`, `fork`) wait on
//! the actor's reply.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::mpsc as std_mpsc;

use anyhow::anyhow;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use waku_protocol::model::{
    ActivityFileChange, ActivityItem, ActivityKind, BackgroundWorkEvent,
    BackgroundWorkItem as LegacyWork, BackgroundWorkKey, BackgroundWorkKind, BackgroundWorkStatus,
    DriverEvent, GoalOperation, PermissionOption as LegacyOption, ProviderResumeCursor,
    ThreadGoal as LegacyGoal, ThreadGoalStatus as LegacyGoalStatus,
    UserInputAnswer as LegacyAnswer, UserInputOption as LegacyInputOption,
    UserInputQuestion as LegacyQuestion,
};
use waku_protocol::orchestration::{self as orch, *};

use super::RuntimeResponse;
use super::*;
use crate::computer_use::ComputerToolRequest;
use crate::driver::{DriverControl, DriverEventSender, DriverStartOptions, SessionOptions};

enum BridgeCommand {
    Prompt(String),
    Steer(String),
    Cancel,
    Respond {
        native: String,
        option_id: String,
    },
    RespondUserInput {
        native: String,
        answers: Vec<LegacyAnswer>,
    },
    Goal(GoalOperation),
    ApplyOptions(Selection, std_mpsc::Sender<bool>),
    Rollback(
        usize,
        std_mpsc::Sender<anyhow::Result<Option<ProviderResumeCursor>>>,
    ),
    Fork(
        usize,
        std_mpsc::Sender<anyhow::Result<ProviderResumeCursor>>,
    ),
    StopBackgroundWork {
        key: BackgroundWorkKey,
        control_id: String,
    },
}

/// What the event translator and the actor share.
#[derive(Default)]
struct Shared {
    thread: Option<ProviderThreadRef>,
    current: Option<RunRef>,
    /// Native request id → app request (for replies).
    requests: HashMap<String, RuntimeRequestId>,
    /// Provider turns of this session, in order (rollback counts them).
    turns: Vec<ProviderTurnRef>,
}

pub struct V1Bridge {
    commands: mpsc::UnboundedSender<BridgeCommand>,
    session: Arc<dyn DriverSession>,
    supports_steer: bool,
}

/// The selection a legacy start or option change describes.
pub fn selection_from_options(options: &SessionOptions, agent_preset: Option<String>) -> Selection {
    Selection {
        model: options
            .model
            .clone()
            .filter(|model| !model.is_empty() && model != "default"),
        reasoning_effort: options.reasoning_effort.clone(),
        service_tier: options.service_tier.clone(),
        context_window: options.context_window.clone(),
        agent_preset,
        runtime_mode: options.mode.into(),
        interaction_mode: InteractionMode::Default,
    }
}

impl V1Bridge {
    /// Opens a session of `driver` for `instance` and serves it as a
    /// `DriverControl`. Must not be called from inside the runtime.
    pub fn start(
        driver: Arc<dyn DriverV2>,
        instance: InstanceConfig,
        options: DriverStartOptions,
        events: DriverEventSender,
        transports: Arc<dyn TransportFactory>,
        runtime: tokio::runtime::Handle,
    ) -> anyhow::Result<Self> {
        let ids: Arc<dyn IdAllocator> = Arc::new(UuidIdAllocator);
        let clock: Arc<dyn Clock> = Arc::new(waku_orchestrator::SystemClock);
        let thread_id = ThreadId(ids.fresh(EntityKind::Node));
        let provider_session_id = ProviderSessionId(ids.fresh(EntityKind::Session));
        let selection = selection_from_options(
            &SessionOptions {
                mode: options.mode,
                model: options.model.clone(),
                reasoning_effort: options.reasoning_effort.clone(),
                service_tier: options.service_tier.clone(),
                context_window: options.context_window.clone(),
            },
            options.agent_preset.clone(),
        );
        let mut instance = instance;
        instance.binary = options.binary.clone();
        instance.computer_use_enabled = options.computer_use_enabled;
        let opened = runtime.block_on(driver.open_session(SessionContext {
            provider_session_id,
            thread_id,
            instance,
            cwd: options.cwd.clone(),
            selection: selection.clone(),
            resume: None,
            ids: ids.clone(),
            clock: clock.clone(),
            transports,
            mcp: None,
            output: Arc::new(NullOutput),
            raw_log: Arc::new(NullRawLog),
            shutdown: Shutdown::new(),
        }))?;
        let session = opened.session;
        let capabilities = opened.capabilities;
        let shared = Arc::new(Mutex::new(Shared::default()));
        // A resumed thread is bound at once; a new one on the first prompt.
        if let Some(cursor) = options.provider_cursor.clone() {
            let reference = ProviderThreadRef {
                provider_thread_id: ProviderThreadId(ids.fresh(EntityKind::ProviderThread)),
                native: NativeRef::strong(cursor_native_id(&cursor)),
                cursor: Some(cursor),
            };
            let resumed = runtime.block_on(session.resume_thread(ResumeThread {
                thread: reference,
                selection: selection.clone(),
            }))?;
            shared.lock().thread = Some(resumed);
        }
        runtime.spawn(translate(opened.events, events.clone(), shared.clone()));
        let (commands, receiver) = mpsc::unbounded_channel();
        runtime.spawn(actor(BridgeActor {
            driver,
            session: session.clone(),
            capabilities: capabilities.clone(),
            selection,
            thread_id,
            ids,
            shared,
            events,
            receiver,
        }));
        Ok(Self {
            commands,
            session,
            supports_steer: capabilities.turns.active_steering,
        })
    }
}

fn cursor_native_id(cursor: &ProviderResumeCursor) -> String {
    serde_json::to_value(cursor)
        .ok()
        .and_then(|value| {
            ["threadId", "sessionId"]
                .iter()
                .find_map(|key| value.get(key).and_then(|id| id.as_str()).map(str::to_owned))
        })
        .unwrap_or_default()
}

struct BridgeActor {
    driver: Arc<dyn DriverV2>,
    session: Arc<dyn DriverSession>,
    capabilities: Capabilities,
    selection: Selection,
    thread_id: ThreadId,
    ids: Arc<dyn IdAllocator>,
    shared: Arc<Mutex<Shared>>,
    events: DriverEventSender,
    receiver: mpsc::UnboundedReceiver<BridgeCommand>,
}

impl BridgeActor {
    fn emit(&self, event: DriverEvent) {
        let _ = self.events.send(event);
    }

    async fn thread(&self) -> DriverResult<ProviderThreadRef> {
        if let Some(thread) = self.shared.lock().thread.clone() {
            return Ok(thread);
        }
        let provider_thread_id = ProviderThreadId(self.ids.fresh(EntityKind::ProviderThread));
        let thread = self
            .session
            .ensure_thread(EnsureThread {
                thread_id: self.thread_id,
                title: None,
                selection: self.selection.clone(),
                existing: Some(ProviderThreadRef::unbound(provider_thread_id)),
            })
            .await?;
        self.shared.lock().thread = Some(thread.clone());
        Ok(thread)
    }

    async fn prompt(&self, text: String) {
        let thread = match self.thread().await {
            Ok(thread) => thread,
            Err(error) => {
                self.emit(DriverEvent::Error(error.to_string()));
                return;
            }
        };
        let run = RunRef {
            thread_id: self.thread_id,
            run_id: RunId(self.ids.fresh(EntityKind::Node)),
            attempt_id: RunAttemptId(self.ids.fresh(EntityKind::Node)),
            root_node_id: NodeId(self.ids.fresh(EntityKind::Node)),
        };
        let ordinal = self.shared.lock().turns.len() as u32 + 1;
        self.shared.lock().current = Some(run);
        let started = self
            .session
            .start_turn(TurnStart {
                run,
                run_ordinal: ordinal,
                provider_turn_ordinal: ordinal,
                restart_continuation_of: None,
                thread,
                message: TurnMessage {
                    message_id: MessageId(self.ids.fresh(EntityKind::Message)),
                    text,
                    attachments: Vec::new(),
                },
                selection: self.selection.clone(),
                app_context: Vec::new(),
                intent: TurnIntent::Prompt,
            })
            .await;
        match started {
            Ok(accepted) => {
                if let Some(turn) = accepted.provider_turn {
                    let mut shared = self.shared.lock();
                    if !shared
                        .turns
                        .iter()
                        .any(|known| known.provider_turn_id == turn.provider_turn_id)
                    {
                        shared.turns.push(turn);
                    }
                }
            }
            Err(error) => {
                self.shared.lock().current = None;
                self.emit(DriverEvent::Error(error.to_string()));
                self.emit(DriverEvent::TurnFinished {
                    success: false,
                    summary: Some(error.to_string()),
                });
            }
        }
    }

    async fn steer(&self, text: String) {
        let (current, thread) = {
            let shared = self.shared.lock();
            (shared.current, shared.thread.clone())
        };
        let (Some(run), Some(thread)) = (current, thread) else {
            self.emit(DriverEvent::SteerRejected {
                message: text,
                reason: "No turn is running.".to_owned(),
            });
            return;
        };
        let result = self
            .session
            .steer_turn(Steer {
                run,
                thread,
                provider_turn: None,
                message: TurnMessage {
                    message_id: MessageId(self.ids.fresh(EntityKind::Message)),
                    text: text.clone(),
                    attachments: Vec::new(),
                },
            })
            .await;
        match result {
            Ok(()) => self.emit(DriverEvent::SteerAccepted { message: text }),
            Err(error) => self.emit(DriverEvent::SteerRejected {
                message: text,
                reason: error.to_string(),
            }),
        }
    }

    async fn rollback(&self, turns: usize) -> anyhow::Result<Option<ProviderResumeCursor>> {
        let thread = self.thread().await?;
        let known = self.shared.lock().turns.clone();
        let keep = known.len().saturating_sub(turns);
        let outcome = self
            .session
            .rollback(Rollback {
                thread,
                target: match keep {
                    0 => RollbackTarget::ThreadStart,
                    keep => RollbackTarget::AfterProviderTurn(known[keep - 1].clone()),
                },
                turns: known.clone(),
                turns_to_remove: turns as u32,
            })
            .await?;
        let mut shared = self.shared.lock();
        shared.turns.truncate(keep);
        shared.thread = Some(outcome.thread.clone());
        Ok(outcome.thread.cursor)
    }

    async fn fork(&self, turns_to_remove: usize) -> anyhow::Result<ProviderResumeCursor> {
        let thread = self.thread().await?;
        let known = self.shared.lock().turns.clone();
        let keep = known.len().saturating_sub(turns_to_remove);
        let at = match keep {
            keep if keep == known.len() || keep == 0 => ForkPoint::Tip,
            keep => ForkPoint::AfterProviderTurn {
                turn: known[keep - 1].clone(),
                turns_to_remove: turns_to_remove as u32,
            },
        };
        let forked = self
            .session
            .fork(ForkRequest {
                source: thread,
                at,
                target_thread_id: ThreadId(self.ids.fresh(EntityKind::Node)),
                owner_node: None,
                selection: None,
            })
            .await?;
        forked
            .cursor
            .ok_or_else(|| anyhow!("the provider returned no resume cursor for the fork"))
    }

    async fn run(mut self) {
        while let Some(command) = self.receiver.recv().await {
            match command {
                BridgeCommand::Prompt(text) => self.prompt(text).await,
                BridgeCommand::Steer(text) => self.steer(text).await,
                BridgeCommand::Cancel => {
                    let (current, thread) = {
                        let shared = self.shared.lock();
                        (shared.current, shared.thread.clone())
                    };
                    if let (Some(run), Some(thread)) = (current, thread) {
                        let _ = self
                            .session
                            .interrupt_turn(Interrupt {
                                run,
                                thread,
                                provider_turn: None,
                                restart_follows: false,
                            })
                            .await;
                    }
                }
                BridgeCommand::Respond { native, option_id } => {
                    self.respond(native, RuntimeReply::Decision { option_id })
                        .await;
                }
                BridgeCommand::RespondUserInput { native, answers } => {
                    let answers = answers.iter().map(orch::UserInputAnswer::from).collect();
                    self.respond(native, RuntimeReply::Answers(answers)).await;
                }
                BridgeCommand::Goal(operation) => {
                    let request = match operation {
                        GoalOperation::Refresh => GoalRequest::Get,
                        GoalOperation::Clear => GoalRequest::Clear,
                        GoalOperation::Set {
                            objective, status, ..
                        } => GoalRequest::Set(ThreadGoal {
                            objective: objective.unwrap_or_default(),
                            status: status.map_or(ThreadGoalStatus::Active, Into::into),
                            token_budget: None,
                            tokens_used: 0,
                            time_used_seconds: 0,
                        }),
                    };
                    if let Err(error) = self.session.goal(request).await {
                        self.emit(DriverEvent::Error(error.to_string()));
                    }
                }
                BridgeCommand::ApplyOptions(selection, reply) => {
                    let plan = self.driver.plan_selection_transition(
                        &self.capabilities,
                        &self.selection,
                        &selection,
                    );
                    let applied = match plan {
                        TransitionPlan::ApplyOnNextTurn => true,
                        TransitionPlan::ApplyInSession => {
                            self.session.apply_selection(&selection).await.is_ok()
                        }
                        _ => false,
                    };
                    if applied {
                        self.selection = selection;
                    }
                    let _ = reply.send(applied);
                }
                BridgeCommand::Rollback(turns, reply) => {
                    let _ = reply.send(self.rollback(turns).await);
                }
                BridgeCommand::Fork(turns, reply) => {
                    let _ = reply.send(self.fork(turns).await);
                }
                BridgeCommand::StopBackgroundWork { key, control_id } => {
                    self.stop_background_work(key, control_id).await;
                }
            }
        }
        // Every `DriverControl` handle is gone: shut the session down.
        let _ = self.session.close().await;
    }

    async fn stop_background_work(&self, key: BackgroundWorkKey, control_id: String) {
        let result = if key.kind == BackgroundWorkKind::Subagent {
            self.session
                .stop_subagent(SubagentRef {
                    node_id: NodeId(uuid::Uuid::nil()),
                    native_task_id: control_id,
                })
                .await
                .and_then(|outcome| match outcome {
                    SubagentStopOutcome::Stopped => Ok(()),
                    SubagentStopOutcome::AlreadyFinished => Err(DriverError::Protocol {
                        reason: ProtocolReason::UnexpectedFrame,
                        detail: "the task is no longer running".to_owned(),
                    }),
                })
        } else {
            let provider_thread_id = self
                .shared
                .lock()
                .thread
                .as_ref()
                .map_or(ProviderThreadId(uuid::Uuid::nil()), |thread| {
                    thread.provider_thread_id
                });
            self.session
                .stop_background_work(BackgroundWorkRef {
                    provider_thread_id,
                    task_id: control_id,
                })
                .await
        };
        if let Err(error) = result {
            self.emit(DriverEvent::BackgroundWork(
                BackgroundWorkEvent::StopFailed {
                    key,
                    message: error.to_string(),
                },
            ));
        }
    }

    async fn respond(&self, native: String, reply: RuntimeReply) {
        let Some(request_id) = self.shared.lock().requests.get(&native).copied() else {
            return;
        };
        let result = self
            .session
            .respond(RuntimeResponse {
                request_id,
                native: NativeRef::strong(native),
                reply,
            })
            .await;
        if let Err(error) = result {
            self.emit(DriverEvent::Error(error.to_string()));
        }
    }
}

async fn actor(actor: BridgeActor) {
    actor.run().await;
}

fn legacy_goal(goal: &ThreadGoal) -> LegacyGoal {
    LegacyGoal {
        objective: goal.objective.clone(),
        status: match goal.status {
            ThreadGoalStatus::Paused => LegacyGoalStatus::Paused,
            ThreadGoalStatus::Blocked => LegacyGoalStatus::Blocked,
            ThreadGoalStatus::UsageLimited => LegacyGoalStatus::UsageLimited,
            ThreadGoalStatus::BudgetLimited => LegacyGoalStatus::BudgetLimited,
            ThreadGoalStatus::Complete => LegacyGoalStatus::Complete,
            _ => LegacyGoalStatus::Active,
        },
        token_budget: goal.token_budget,
        tokens_used: goal.tokens_used,
        time_used_seconds: goal.time_used_seconds,
    }
}

/// The legacy rich activity a tool row stands for: the same `tool_activity`
/// normalization the v1 drivers ran on native tool input and output,
/// rebuilt from the row.
fn rich_activity(item: &TurnItem) -> Option<ActivityItem> {
    use serde_json::{Value, json};
    let complete = item.status.is_terminal();
    let failed = matches!(item.status, ItemStatus::Failed);
    let source_id = item
        .native_item_ref
        .as_ref()
        .and_then(|reference| reference.native_id.clone())
        .or_else(|| Some(item.id.to_string()));
    let parse = |text: &str| {
        serde_json::from_str::<Value>(text).unwrap_or_else(|_| Value::String(text.to_owned()))
    };
    let (kind, tool_name, arguments, output, change) = match &item.kind {
        TurnItemKind::CommandExecution { input, output, .. } => (
            ActivityKind::Command,
            None,
            json!({ "command": input }),
            output.clone().map(Value::String),
            None,
        ),
        TurnItemKind::FileChange {
            file_name,
            old_text,
            new_text,
            diff,
            ..
        } => (
            ActivityKind::FileChange,
            None,
            json!({ "file_path": file_name, "old_string": old_text, "new_string": new_text }),
            None,
            diff.clone().map(|diff| (file_name.clone(), diff)),
        ),
        TurnItemKind::FileSearch { pattern, .. } => (
            ActivityKind::FileSearch,
            None,
            json!({ "pattern": pattern }),
            None,
            None,
        ),
        TurnItemKind::WebSearch { patterns, .. } => (
            ActivityKind::Search,
            None,
            json!({ "query": patterns.join(", ") }),
            None,
            None,
        ),
        TurnItemKind::DynamicTool {
            tool_name,
            input,
            output,
            ..
        } => (
            tool_name
                .as_deref()
                .map_or(ActivityKind::Tool, ActivityKind::from_tool_name),
            tool_name.clone(),
            parse(&input.0),
            output.as_ref().map(|output| parse(&output.0)),
            None,
        ),
        _ => return None,
    };
    let title = item
        .title
        .clone()
        .or_else(|| tool_name.clone())
        .unwrap_or_else(|| format!("{kind:?}"));
    let mut activity = crate::driver::activity::tool_activity(
        source_id,
        kind,
        title,
        (!complete).then_some(&arguments),
        output.as_ref(),
        output.as_ref(),
        failed,
        complete,
    )
    .with_tool_name(tool_name.as_deref());
    if let Some((path, diff)) = change {
        activity.file_changes = vec![ActivityFileChange {
            path,
            additions: None,
            deletions: None,
            status: None,
            diff: Some(diff),
        }];
    }
    Some(activity)
}

/// The new part of a growing snapshot, or the whole text after a rewrite.
fn suffix(previous: &str, text: &str) -> Option<String> {
    let added = text.strip_prefix(previous).unwrap_or(text);
    (!added.is_empty()).then(|| added.to_owned())
}

#[derive(Default)]
struct Translator {
    texts: HashMap<MessageId, String>,
    reasoning: HashMap<TurnItemId, String>,
    activities: HashMap<TurnItemId, bool>,
    turns_started: std::collections::HashSet<ProviderTurnId>,
    usage: HashMap<ProviderTurnId, (u64, Option<u64>)>,
    cursor: Option<String>,
    title: Option<String>,
    prompts: HashMap<RuntimeRequestId, String>,
    raised: std::collections::HashSet<RuntimeRequestId>,
}

impl Translator {
    fn translate(&mut self, event: AdapterEvent, shared: &Mutex<Shared>) -> Vec<DriverEvent> {
        let mut out = Vec::new();
        match event {
            AdapterEvent::SessionUpdated { session } => {
                if session.status == SessionStatus::Error
                    && let Some(error) = session.last_error
                {
                    out.push(DriverEvent::Error(error));
                }
            }
            AdapterEvent::ProviderThreadUpdated { thread, .. } => {
                if thread.owner_node_id.is_some() {
                    return out;
                }
                let cursor = thread
                    .native_metadata
                    .as_ref()
                    .and_then(|metadata| metadata.resume_cursor.as_ref())
                    .map(|cursor| cursor.0.clone());
                if cursor.is_some() && cursor != self.cursor {
                    self.cursor = cursor.clone();
                    let provider_cursor = cursor.as_deref().and_then(|cursor| {
                        serde_json::from_str::<ProviderResumeCursor>(cursor).ok()
                    });
                    {
                        let mut shared = shared.lock();
                        let reference = ProviderThreadRef::from_row(&thread);
                        shared.thread = Some(reference);
                    }
                    out.push(DriverEvent::Connected { provider_cursor });
                }
                let title = thread
                    .native_metadata
                    .as_ref()
                    .and_then(|metadata| metadata.title.clone());
                // A snapshot without a title leaves the known one alone.
                if title.is_some() && title != self.title {
                    self.title = title.clone();
                    out.push(DriverEvent::AutoTitleUpdated(title));
                }
            }
            AdapterEvent::ProviderTurnUpdated { turn } => {
                if turn.run_attempt_id.is_none() {
                    return out;
                }
                if turn.status == ProviderTurnStatus::Running && self.turns_started.insert(turn.id)
                {
                    let mut shared = shared.lock();
                    if !shared
                        .turns
                        .iter()
                        .any(|known| known.provider_turn_id == turn.id)
                    {
                        let native = turn
                            .native_turn_ref
                            .as_ref()
                            .and_then(|reference| reference.native_id.clone())
                            .unwrap_or_default();
                        shared.turns.push(ProviderTurnRef {
                            provider_turn_id: turn.id,
                            native: NativeRef::strong(native),
                            ordinal: turn.ordinal,
                        });
                    }
                    out.push(DriverEvent::TurnStarted);
                }
                if let Some(usage) = &turn.token_usage {
                    let value = (usage.used, usage.max);
                    if self.usage.insert(turn.id, value) != Some(value) {
                        out.push(DriverEvent::UsageUpdated {
                            context_tokens: Some(usage.used),
                            context_window: usage.max,
                        });
                    }
                }
            }
            AdapterEvent::MessageUpdated { message } => {
                if message.role != MessageRole::Assistant {
                    return out;
                }
                let previous = self.texts.entry(message.id).or_default();
                if let Some(added) = suffix(previous, &message.text) {
                    out.push(DriverEvent::TextDelta(added));
                }
                *previous = message.text;
            }
            AdapterEvent::TurnItemUpdated { item } => {
                if let TurnItemKind::Reasoning { text, .. } = &item.kind {
                    let previous = self.reasoning.entry(item.id).or_default();
                    if let Some(added) = suffix(previous, text) {
                        out.push(DriverEvent::ReasoningDelta(added));
                    }
                    *previous = text.clone();
                } else if let TurnItemKind::ApprovalRequest {
                    request_id,
                    prompt: Some(prompt),
                    ..
                } = &item.kind
                {
                    self.prompts.insert(*request_id, prompt.clone());
                } else if let Some(activity) = rich_activity(&item) {
                    let complete = item.status.is_terminal();
                    if self.activities.insert(item.id, complete) != Some(complete) {
                        out.push(DriverEvent::RichActivity(activity));
                    }
                }
            }
            AdapterEvent::RuntimeRequestUpdated { request } => {
                if request.status != RequestStatus::Pending || !self.raised.insert(request.id) {
                    return out;
                }
                let native = request
                    .native_request_ref
                    .as_ref()
                    .and_then(|reference| reference.native_id.clone())
                    .unwrap_or_else(|| request.id.to_string());
                shared.lock().requests.insert(native.clone(), request.id);
                if request.kind == RuntimeRequestKind::UserInput {
                    out.push(DriverEvent::UserInputRequested {
                        request_id: native,
                        questions: request
                            .questions
                            .iter()
                            .map(|question| LegacyQuestion {
                                id: question.id.clone(),
                                header: question.header.clone(),
                                question: question.question.clone(),
                                options: question
                                    .options
                                    .iter()
                                    .map(|option| LegacyInputOption {
                                        label: option.label.clone(),
                                        description: option.description.clone(),
                                    })
                                    .collect(),
                                multi_select: question.multi_select,
                            })
                            .collect(),
                    });
                } else {
                    out.push(DriverEvent::Permission {
                        request_id: native,
                        title: self
                            .prompts
                            .get(&request.id)
                            .cloned()
                            .unwrap_or_else(|| request.kind.as_str().replace('_', " ")),
                        detail: String::new(),
                        options: request
                            .options
                            .iter()
                            .map(|option| LegacyOption {
                                id: option.id.clone(),
                                label: option.label.clone(),
                                allow: option.allow,
                            })
                            .collect(),
                    });
                }
            }
            AdapterEvent::GoalUpdated { goal } => {
                out.push(DriverEvent::GoalUpdated(goal.as_ref().map(legacy_goal)));
            }
            AdapterEvent::AccountUsageUpdated { usage } => {
                out.push(DriverEvent::PlanUsageUpdated(usage));
            }
            AdapterEvent::TurnTerminal { terminal } => {
                // A child turn never finishes the session's turn.
                let child = terminal.provider_turn.as_ref().is_some_and(|turn| {
                    !shared
                        .lock()
                        .turns
                        .iter()
                        .any(|known| known.provider_turn_id == turn.provider_turn_id)
                });
                if child {
                    return out;
                }
                {
                    let mut shared = shared.lock();
                    if shared
                        .current
                        .is_some_and(|run| run.attempt_id == terminal.run.attempt_id)
                    {
                        shared.current = None;
                    }
                }
                match terminal.outcome {
                    TurnOutcome::Completed if terminal.background_pending => {
                        out.push(DriverEvent::TurnParked);
                    }
                    TurnOutcome::Completed => out.push(DriverEvent::TurnFinished {
                        success: true,
                        summary: terminal.summary,
                    }),
                    TurnOutcome::Interrupted | TurnOutcome::Cancelled => {
                        out.push(DriverEvent::TurnFinished {
                            success: false,
                            summary: terminal.summary,
                        });
                    }
                    TurnOutcome::Failed { failure } => {
                        out.push(DriverEvent::Error(failure.message.clone()));
                        out.push(DriverEvent::TurnFinished {
                            success: false,
                            summary: Some(failure.message),
                        });
                    }
                }
            }
            // Detached processes and monitors (#47). A subagent's row comes
            // from `SubagentUpdated`, so its level entry is not repeated.
            AdapterEvent::BackgroundWorkUpdated { work } => {
                let kind = match work.task.kind {
                    PendingTaskKind::Monitor => BackgroundWorkKind::Monitor,
                    PendingTaskKind::Subagent => return out,
                    _ => BackgroundWorkKind::Process,
                };
                let status = match (work.live, kind) {
                    (true, BackgroundWorkKind::Monitor) => BackgroundWorkStatus::Monitoring,
                    (true, _) => BackgroundWorkStatus::Running,
                    (false, _) => BackgroundWorkStatus::Completed,
                };
                let mut item = LegacyWork::new(
                    kind,
                    work.task.task_id.clone(),
                    work.task.description.clone().unwrap_or_default(),
                    status,
                );
                item.background = true;
                item.can_stop = work.live;
                item.control_id = Some(work.task.task_id);
                out.push(DriverEvent::BackgroundWork(BackgroundWorkEvent::Upsert(
                    item,
                )));
            }
            AdapterEvent::SubagentUpdated { subagent } => {
                let Some(task_id) = subagent
                    .native_task_ref
                    .as_ref()
                    .and_then(|reference| reference.native_id.clone())
                else {
                    return out;
                };
                let status = match subagent.status {
                    SubagentStatus::Completed => BackgroundWorkStatus::Completed,
                    SubagentStatus::Failed => BackgroundWorkStatus::Failed,
                    SubagentStatus::Cancelled | SubagentStatus::Interrupted => {
                        BackgroundWorkStatus::Stopped
                    }
                    _ => BackgroundWorkStatus::Running,
                };
                let mut item = LegacyWork::new(
                    BackgroundWorkKind::Subagent,
                    task_id.clone(),
                    subagent.title.clone().unwrap_or_default(),
                    status,
                );
                item.can_stop = status.is_live();
                item.control_id = Some(task_id);
                item.command = (!subagent.prompt.is_empty()).then(|| subagent.prompt.clone());
                item.detail = subagent.progress.clone();
                item.output = subagent.result.clone();
                item.model = subagent.model.clone();
                out.push(DriverEvent::BackgroundWork(BackgroundWorkEvent::Upsert(
                    item,
                )));
            }
            AdapterEvent::NodeUpdated { .. }
            | AdapterEvent::PlanUpdated { .. }
            | AdapterEvent::CommandOutputDelta { .. } => {}
        }
        out
    }
}

async fn translate(
    mut events: AdapterEventReceiver,
    sink: DriverEventSender,
    shared: Arc<Mutex<Shared>>,
) {
    let mut translator = Translator::default();
    while let Some(event) = events.recv().await {
        for event in translator.translate(event, &shared) {
            let _ = sink.send(event);
        }
    }
    let _ = sink.send(DriverEvent::ProcessExited);
}

impl DriverControl for V1Bridge {
    fn prompt(&self, prompt: String) {
        let _ = self.commands.send(BridgeCommand::Prompt(prompt));
    }

    fn supports_steer(&self) -> bool {
        self.supports_steer
    }

    fn steer(&self, prompt: String) {
        let _ = self.commands.send(BridgeCommand::Steer(prompt));
    }

    fn cancel(&self) {
        let _ = self.commands.send(BridgeCommand::Cancel);
    }

    fn cancel_computer_use(&self) {
        if let Some(control) = self.session.computer_use() {
            control.cancel_computer_use();
        }
    }

    fn stop_background_work(&self, key: BackgroundWorkKey, control_id: String) {
        let _ = self
            .commands
            .send(BridgeCommand::StopBackgroundWork { key, control_id });
    }

    fn respond(&self, request_id: String, option_id: String) {
        let _ = self.commands.send(BridgeCommand::Respond {
            native: request_id,
            option_id,
        });
    }

    fn respond_user_input(&self, request_id: String, answers: Vec<LegacyAnswer>) {
        let _ = self.commands.send(BridgeCommand::RespondUserInput {
            native: request_id,
            answers,
        });
    }

    fn goal(&self, operation: GoalOperation) {
        let _ = self.commands.send(BridgeCommand::Goal(operation));
    }

    fn run_computer_tool(&self, request: ComputerToolRequest) {
        if let Some(control) = self.session.computer_use() {
            control.run_computer_tool(request);
        }
    }

    fn reject_computer_tool(&self, request: ComputerToolRequest, reason: String) {
        if let Some(control) = self.session.computer_use() {
            control.reject_computer_tool(request, reason);
        }
    }

    fn apply_options(&self, options: SessionOptions) -> bool {
        let (reply, receiver) = std_mpsc::channel();
        let selection = selection_from_options(&options, None);
        if self
            .commands
            .send(BridgeCommand::ApplyOptions(selection, reply))
            .is_err()
        {
            return false;
        }
        receiver.recv().unwrap_or(false)
    }

    fn rollback(&self, turns: usize) -> anyhow::Result<Option<ProviderResumeCursor>> {
        let (reply, receiver) = std_mpsc::channel();
        self.commands
            .send(BridgeCommand::Rollback(turns, reply))
            .map_err(|_| anyhow!("the provider session ended"))?;
        receiver
            .recv()
            .map_err(|_| anyhow!("the provider session ended"))?
    }

    fn fork(&self, turns_to_remove: usize) -> anyhow::Result<ProviderResumeCursor> {
        let (reply, receiver) = std_mpsc::channel();
        self.commands
            .send(BridgeCommand::Fork(turns_to_remove, reply))
            .map_err(|_| anyhow!("the provider session ended"))?;
        receiver
            .recv()
            .map_err(|_| anyhow!("the provider session ended"))?
    }
}

#[cfg(test)]
mod tests;
