//! The bridge drives an existing driver behind the old interface unchanged.
//!
//! Amp, the simplest driver, runs twice against a scripted `amp`: once
//! directly through `DriverControl`, once wrapped as a `DriverSession` (a
//! test-only shim that applies RFC 0002's `DriverEvent` → `AdapterEvent`
//! table) and served back through `V1Bridge`. The two event streams must
//! tell the same story.

#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Duration;

use crossbeam_channel::Receiver;
use serde_json::Value;

use super::*;
use crate::driver::amp::AmpDriver;
use crate::driver::test_event_channel;
use waku_protocol::model::RuntimeMode as LegacyMode;

const AMP_SCRIPT: &str = r#"#!/bin/sh
if [ "$1" = "threads" ]; then echo '[]'; exit 0; fi
n=0
while IFS= read -r line; do
  n=$((n+1))
  if [ $n -eq 1 ]; then echo '{"type":"system","subtype":"init","session_id":"T-fake-1"}'; fi
  echo '{"type":"assistant","message":{"content":[{"type":"thinking","thinking":"Thinking it over."},{"type":"tool_use","id":"tool-'$n'","name":"Bash","input":{"cmd":"ls"}}]}}'
  echo '{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"tool-'$n'","content":"README.md"}]}}'
  echo '{"type":"assistant","message":{"content":[{"type":"text","text":"Hello "},{"type":"text","text":"from amp '$n'"}],"stop_reason":"end_turn"}}'
done
"#;

fn write_script(dir: &Path) -> std::path::PathBuf {
    let path = dir.join("amp");
    std::fs::write(&path, AMP_SCRIPT).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

fn options(binary: &Path, cwd: &Path) -> DriverStartOptions {
    DriverStartOptions {
        binary: binary.to_owned(),
        cwd: cwd.to_owned(),
        mode: LegacyMode::FullAccess,
        model: None,
        reasoning_effort: None,
        service_tier: None,
        context_window: None,
        agent_preset: None,
        computer_use_enabled: false,
        provider_cursor: None,
    }
}

/// What a client can observe, with deltas merged and asynchronous title
/// refreshes left out.
#[derive(Debug, PartialEq, Eq)]
enum Observed {
    Connected(Option<String>),
    TurnStarted,
    Text(String),
    Reasoning(String),
    Activity { id: Option<String>, complete: bool },
    TurnFinished(bool),
    Error(String),
    ProcessExited,
}

fn observe(events: Vec<DriverEvent>) -> Vec<Observed> {
    let mut observed = Vec::new();
    for event in events {
        let next = match event {
            DriverEvent::Connected { provider_cursor } => Observed::Connected(
                provider_cursor.map(|cursor| serde_json::to_string(&cursor).unwrap()),
            ),
            DriverEvent::TurnStarted => Observed::TurnStarted,
            DriverEvent::TextDelta(text) => {
                if let Some(Observed::Text(previous)) = observed.last_mut() {
                    previous.push_str(&text);
                    continue;
                }
                Observed::Text(text)
            }
            DriverEvent::ReasoningDelta(text) => {
                if let Some(Observed::Reasoning(previous)) = observed.last_mut() {
                    previous.push_str(&text);
                    continue;
                }
                Observed::Reasoning(text)
            }
            DriverEvent::Activity { id, complete, .. } => Observed::Activity { id, complete },
            DriverEvent::RichActivity(item) => Observed::Activity {
                id: item.source_id,
                complete: item.complete,
            },
            DriverEvent::TurnFinished { success, .. } => Observed::TurnFinished(success),
            DriverEvent::Error(error) => Observed::Error(error),
            DriverEvent::ProcessExited => Observed::ProcessExited,
            _ => continue,
        };
        observed.push(next);
    }
    observed
}

/// Two turns, then the handle is dropped; returns every event until the
/// process exited.
fn drive(control: Box<dyn DriverControl>, events: Receiver<DriverEvent>) -> Vec<DriverEvent> {
    let mut seen = Vec::new();
    let wait_for = |seen: &mut Vec<DriverEvent>, done: &dyn Fn(&DriverEvent) -> bool| loop {
        let event = events
            .recv_timeout(Duration::from_secs(10))
            .expect("the driver went quiet");
        let finished = done(&event);
        seen.push(event);
        if finished {
            return;
        }
    };
    for prompt in ["one", "two"] {
        control.prompt(prompt.to_owned());
        wait_for(&mut seen, &|event| {
            matches!(event, DriverEvent::TurnFinished { .. })
        });
    }
    drop(control);
    wait_for(&mut seen, &|event| {
        matches!(event, DriverEvent::ProcessExited)
    });
    seen
}

// -- the shim: an unported driver as a DriverSession --------------------------------

#[derive(Default)]
struct ShimState {
    run: Option<RunRef>,
    thread: Option<ProviderThreadRef>,
    turn: Option<ProviderTurnRef>,
    text: String,
    reasoning: String,
    ordinal: u32,
}

struct AmpShim;

struct ShimSession {
    control: Mutex<Option<Arc<dyn DriverControl>>>,
    state: Arc<Mutex<ShimState>>,
    capabilities: Capabilities,
}

fn shim_capabilities() -> Capabilities {
    let mut caps = Capabilities::default();
    caps.turns.active_steering = true;
    caps.turns.interrupt = true;
    caps.turns.emits_turn_started = true;
    caps
}

fn template_row(thread: &ProviderThreadRef, thread_id: ThreadId) -> ProviderThread {
    ProviderThread {
        id: thread.provider_thread_id,
        driver: orch::DriverKind::Amp,
        provider_instance_id: ProviderInstanceId::new("amp"),
        provider_session_id: None,
        app_thread_id: Some(thread_id),
        owner_node_id: None,
        native_thread_ref: None,
        native_conversation_head_ref: None,
        status: ProviderThreadStatus::Active,
        first_run_ordinal: None,
        last_run_ordinal: None,
        handoff_ids: Vec::new(),
        forked_from: None,
        pending_background_tasks: Vec::new(),
        context_usage: None,
        goal: None,
        native_metadata: None,
        created_at: 0,
        updated_at: 0,
    }
}

/// RFC 0002's `DriverEvent` → `AdapterEvent` table, for the shim.
fn forward(event: DriverEvent, state: &Mutex<ShimState>, thread_id: ThreadId) -> Vec<AdapterEvent> {
    let mut state = state.lock();
    let item = |id: TurnItemId, run: &RunRef, status: ItemStatus, kind: TurnItemKind| TurnItem {
        id,
        thread_id,
        run_id: Some(run.run_id),
        node_id: Some(run.root_node_id),
        provider_thread_id: None,
        provider_turn_id: None,
        native_item_ref: None,
        parent_item_id: None,
        position: 0,
        status,
        title: None,
        tool: None,
        stream_rev: 0,
        started_at: None,
        completed_at: None,
        updated_at: 0,
        kind,
    };
    match event {
        DriverEvent::Connected { provider_cursor } => {
            let Some(thread) = state.thread.clone() else {
                return Vec::new();
            };
            let mut reference = thread;
            if let Some(cursor) = provider_cursor {
                reference.native = NativeRef::strong(cursor_native_id(&cursor));
                reference.cursor = Some(cursor);
            }
            state.thread = Some(reference.clone());
            let mut row = template_row(&reference, thread_id);
            reference.apply_to(&mut row);
            vec![AdapterEvent::ProviderThreadUpdated {
                thread: row,
                retained_native_turn_ids: None,
            }]
        }
        DriverEvent::TurnStarted => {
            let (Some(run), Some(thread)) = (state.run, state.thread.clone()) else {
                return Vec::new();
            };
            state.ordinal += 1;
            state.text.clear();
            state.reasoning.clear();
            let id = ProviderTurnId(uuid::Uuid::new_v5(
                &thread.provider_thread_id.0,
                format!("turn:{}", state.ordinal).as_bytes(),
            ));
            let reference = ProviderTurnRef {
                provider_turn_id: id,
                native: NativeRef::weak(state.ordinal.to_string()),
                ordinal: state.ordinal,
            };
            state.turn = Some(reference.clone());
            vec![AdapterEvent::ProviderTurnUpdated {
                turn: ProviderTurn {
                    id,
                    provider_thread_id: thread.provider_thread_id,
                    node_id: run.root_node_id,
                    run_attempt_id: Some(run.attempt_id),
                    native_turn_ref: Some(reference.native.to_provider_ref(orch::DriverKind::Amp)),
                    ordinal: reference.ordinal,
                    status: ProviderTurnStatus::Running,
                    started_at: None,
                    completed_at: None,
                    token_usage: None,
                    turn_token_usage: None,
                },
            }]
        }
        DriverEvent::TextDelta(text) => {
            let Some(run) = state.run else {
                return Vec::new();
            };
            state.text.push_str(&text);
            vec![AdapterEvent::MessageUpdated {
                message: Message {
                    id: MessageId(uuid::Uuid::new_v5(&run.attempt_id.0, b"assistant")),
                    thread_id,
                    run_id: Some(run.run_id),
                    node_id: Some(run.root_node_id),
                    role: MessageRole::Assistant,
                    created_by: Actor::Agent,
                    creation_source: CreationSource::Provider,
                    text: state.text.clone(),
                    display_text: None,
                    attachments: Vec::new(),
                    context: Vec::new(),
                    notification: None,
                    sender_thread_id: None,
                    streaming: true,
                    created_at: 0,
                    updated_at: 0,
                },
            }]
        }
        DriverEvent::ReasoningDelta(text) => {
            let Some(run) = state.run else {
                return Vec::new();
            };
            state.reasoning.push_str(&text);
            vec![AdapterEvent::TurnItemUpdated {
                item: item(
                    TurnItemId(uuid::Uuid::new_v5(&run.attempt_id.0, b"reasoning")),
                    &run,
                    ItemStatus::Running,
                    TurnItemKind::Reasoning {
                        text: state.reasoning.clone(),
                        streaming: true,
                    },
                ),
            }]
        }
        DriverEvent::RichActivity(activity) => {
            let Some(run) = state.run else {
                return Vec::new();
            };
            let source = activity.source_id.clone().unwrap_or_default();
            let mut row = item(
                TurnItemId(uuid::Uuid::new_v5(&run.attempt_id.0, source.as_bytes())),
                &run,
                if activity.complete {
                    ItemStatus::Completed
                } else {
                    ItemStatus::Running
                },
                TurnItemKind::DynamicTool {
                    tool_name: activity.tool_name.clone().or(Some(activity.title.clone())),
                    input: JsonText(activity.arguments.clone().unwrap_or_default()),
                    output: activity.output.clone().map(JsonText),
                    output_omitted: false,
                    viewed_image_path: None,
                },
            );
            row.title = Some(activity.title);
            row.native_item_ref = Some(ProviderRef::strong(orch::DriverKind::Amp, source));
            vec![AdapterEvent::TurnItemUpdated { item: row }]
        }
        DriverEvent::TurnFinished { success, summary } => {
            let Some(run) = state.run.take() else {
                return Vec::new();
            };
            vec![AdapterEvent::TurnTerminal {
                terminal: TurnTerminal {
                    run,
                    provider_turn: state.turn.take(),
                    outcome: if success {
                        TurnOutcome::Completed
                    } else {
                        TurnOutcome::Failed {
                            failure: ProviderFailure::new(
                                FailureClass::Unknown,
                                summary.clone().unwrap_or_default(),
                            ),
                        }
                    },
                    thread_disposition: ThreadDisposition::Reusable,
                    child_turns_continue: false,
                    background_pending: false,
                    summary,
                },
            }]
        }
        _ => Vec::new(),
    }
}

#[async_trait::async_trait]
impl DriverV2 for AmpShim {
    fn kind(&self) -> DriverFamily {
        DriverFamily::Amp
    }

    fn capabilities(&self, _cfg: &InstanceConfig) -> Capabilities {
        shim_capabilities()
    }

    fn plan_selection_transition(
        &self,
        _caps: &Capabilities,
        _from: &Selection,
        _to: &Selection,
    ) -> TransitionPlan {
        TransitionPlan::RestartSession
    }

    async fn open_session(&self, ctx: SessionContext) -> DriverResult<OpenedSession> {
        let (legacy, legacy_events) = test_event_channel();
        let control: Arc<dyn DriverControl> = Arc::new(
            AmpDriver::start(options(&ctx.instance.binary, &ctx.cwd), legacy)
                .map_err(|error| DriverError::Io(std::io::Error::other(error.to_string())))?,
        );
        let (events, receiver) = adapter_channel();
        let state = Arc::new(Mutex::new(ShimState::default()));
        let pump_state = state.clone();
        let thread_id = ctx.thread_id;
        std::thread::spawn(move || {
            while let Ok(event) = legacy_events.recv() {
                if matches!(event, DriverEvent::ProcessExited) {
                    return;
                }
                for event in forward(event, &pump_state, thread_id) {
                    if events.blocking_send(event).is_err() {
                        return;
                    }
                }
            }
        });
        Ok(OpenedSession {
            session: Arc::new(ShimSession {
                control: Mutex::new(Some(control)),
                state,
                capabilities: shim_capabilities(),
            }),
            events: receiver,
            capabilities: shim_capabilities(),
        })
    }
}

#[async_trait::async_trait]
impl DriverSession for ShimSession {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    async fn ensure_thread(&self, req: EnsureThread) -> DriverResult<ProviderThreadRef> {
        // Amp creates its thread with the first message.
        let thread = req
            .existing
            .unwrap_or_else(|| ProviderThreadRef::unbound(ProviderThreadId(uuid::Uuid::nil())));
        self.state.lock().thread = Some(thread.clone());
        Ok(thread)
    }

    async fn resume_thread(&self, req: ResumeThread) -> DriverResult<ProviderThreadRef> {
        self.state.lock().thread = Some(req.thread.clone());
        Ok(req.thread)
    }

    async fn start_turn(&self, req: TurnStart) -> DriverResult<TurnAccepted> {
        self.state.lock().run = Some(req.run);
        let control = self.control.lock().clone();
        match control {
            Some(control) => control.prompt(req.message.text),
            None => {
                return Err(DriverError::SessionLost {
                    detail: "closed".to_owned(),
                });
            }
        }
        Ok(TurnAccepted::default())
    }

    async fn steer_turn(&self, req: Steer) -> DriverResult<()> {
        if let Some(control) = self.control.lock().clone() {
            control.steer(req.message.text);
        }
        Ok(())
    }

    async fn interrupt_turn(&self, _req: Interrupt) -> DriverResult<()> {
        if let Some(control) = self.control.lock().clone() {
            control.cancel();
        }
        Ok(())
    }

    async fn respond(&self, _req: RuntimeResponse) -> DriverResult<()> {
        Ok(())
    }

    async fn read_snapshot(&self, _thread: &ProviderThreadRef) -> DriverResult<ThreadSnapshot> {
        Err(DriverError::Unsupported {
            operation: "read_snapshot",
        })
    }

    async fn rollback(&self, _req: Rollback) -> DriverResult<RollbackOutcome> {
        Err(DriverError::Unsupported {
            operation: "rollback",
        })
    }

    async fn fork(&self, _req: ForkRequest) -> DriverResult<ProviderThreadRef> {
        Err(DriverError::Unsupported { operation: "fork" })
    }

    async fn close(&self) -> DriverResult<()> {
        // Dropping the last handle shuts Amp's stdin.
        self.control.lock().take();
        Ok(())
    }
}

#[test]
fn the_bridge_serves_amp_behind_the_old_interface_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let binary = write_script(dir.path());
    let cwd = dir.path().canonicalize().unwrap();

    let (events, received) = test_event_channel();
    let direct = AmpDriver::start(options(&binary, &cwd), events).unwrap();
    let direct = observe(drive(Box::new(direct), received));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (events, received) = test_event_channel();
    let mut instance = InstanceConfig::new("amp", orch::DriverKind::Amp);
    instance.extra = Value::Null;
    let bridged = V1Bridge::start(
        Arc::new(AmpShim),
        instance,
        options(&binary, &cwd),
        events,
        Arc::new(ProcessTransportFactory),
        runtime.handle().clone(),
    )
    .unwrap();
    assert!(bridged.supports_steer());
    let bridged = observe(drive(Box::new(bridged), received));

    assert_eq!(
        bridged, direct,
        "the bridged stream must match the direct one"
    );
    // And the story is the expected one.
    assert_eq!(
        direct,
        vec![
            Observed::TurnStarted,
            Observed::Connected(Some(
                "{\"provider\":\"amp\",\"threadId\":\"T-fake-1\"}".to_owned()
            )),
            Observed::Reasoning("Thinking it over.".to_owned()),
            Observed::Activity {
                id: Some("tool-1".to_owned()),
                complete: false
            },
            Observed::Activity {
                id: Some("tool-1".to_owned()),
                complete: true
            },
            Observed::Text("Hello from amp 1".to_owned()),
            Observed::TurnFinished(true),
            Observed::TurnStarted,
            Observed::Reasoning("Thinking it over.".to_owned()),
            Observed::Activity {
                id: Some("tool-2".to_owned()),
                complete: false
            },
            Observed::Activity {
                id: Some("tool-2".to_owned()),
                complete: true
            },
            Observed::Text("Hello from amp 2".to_owned()),
            Observed::TurnFinished(true),
            Observed::ProcessExited,
        ]
    );
}
