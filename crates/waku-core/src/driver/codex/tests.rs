//! Unit tests for the Codex driver: the capability row, the selection
//! transitions, and the exactly-one-terminal rule on the paths the replay
//! fixtures cannot stage (an interrupt racing `turn/start`, a stream that
//! ends mid-turn, a provider error, a refused steer).

use std::io;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, Lines};
use tokio::sync::{mpsc, oneshot};
use url::Url;
use waku_orchestrator::ManualClock;

use super::*;
use crate::driver::v2::transport::HttpTransport;

#[test]
fn capabilities_match_the_rfc_matrix_row() {
    let actual = serde_json::to_value(codex_capabilities(&codex_instance())).unwrap();
    let expected: Value = serde_json::from_str(include_str!("capabilities_codex.json")).unwrap();
    assert_eq!(
        actual,
        expected,
        "{}",
        serde_json::to_string_pretty(&actual).unwrap()
    );
    // Computer Use is declared only where it is switched on.
    let mut cfg = codex_instance();
    cfg.computer_use_enabled = true;
    assert!(codex_capabilities(&cfg).extras.computer_use);
}

#[test]
fn selection_transitions_follow_the_rfc_table() {
    let driver = CodexDriver::new();
    let caps = codex_capabilities(&codex_instance());
    let base = Selection::default();
    let plan = |change: fn(&mut Selection)| {
        let mut to = base.clone();
        change(&mut to);
        driver.plan_selection_transition(&caps, &base, &to)
    };
    assert_eq!(
        plan(|s| s.model = Some("gpt-6-luna".into())),
        TransitionPlan::ApplyOnNextTurn
    );
    assert_eq!(
        plan(|s| s.reasoning_effort = Some("high".into())),
        TransitionPlan::ApplyOnNextTurn
    );
    assert_eq!(
        plan(|s| s.service_tier = Some("fast".into())),
        TransitionPlan::ApplyOnNextTurn
    );
    assert_eq!(
        plan(|s| s.context_window = Some("1m".into())),
        TransitionPlan::ApplyOnNextTurn
    );
    assert_eq!(
        plan(|s| s.agent_preset = Some("plan".into())),
        TransitionPlan::ApplyOnNextTurn
    );
    assert_eq!(
        plan(|s| s.runtime_mode = RuntimeMode::Ask),
        TransitionPlan::RestartSession
    );
    assert!(matches!(
        plan(|s| s.interaction_mode = InteractionMode::Plan),
        TransitionPlan::Reject { .. }
    ));
    // The strongest plan wins.
    assert_eq!(
        plan(|s| {
            s.model = Some("gpt-6-luna".into());
            s.runtime_mode = RuntimeMode::Ask;
        }),
        TransitionPlan::RestartSession
    );
}

// -- a hand-driven app-server ---------------------------------------------------------

struct Server {
    lines: Lines<BufReader<DuplexStream>>,
    stdout: Option<DuplexStream>,
    exit: Option<oneshot::Sender<ProcessExit>>,
}

impl Server {
    async fn read(&mut self) -> Value {
        let line = tokio::time::timeout(Duration::from_secs(5), self.lines.next_line())
            .await
            .expect("the driver wrote nothing")
            .unwrap()
            .expect("stdin closed");
        serde_json::from_str(&line).unwrap()
    }

    async fn expect(&mut self, method: &str) -> Value {
        let frame = self.read().await;
        assert_eq!(frame["method"], method, "{frame}");
        frame
    }

    async fn send(&mut self, frame: Value) {
        let stdout = self.stdout.as_mut().unwrap();
        stdout
            .write_all(format!("{frame}\n").as_bytes())
            .await
            .unwrap();
        stdout.flush().await.unwrap();
    }

    async fn reply(&mut self, request: &Value, result: Value) {
        self.send(json!({ "id": request["id"], "result": result }))
            .await;
    }

    /// The process dies.
    fn crash(&mut self) {
        self.stdout.take();
        if let Some(exit) = self.exit.take() {
            let _ = exit.send(ProcessExit {
                success: false,
                code: Some(1),
                signal: None,
            });
        }
    }
}

struct Spawner(mpsc::UnboundedSender<Server>);

#[async_trait]
impl TransportFactory for Spawner {
    async fn spawn_stdio(&self, _channel: &str, _spec: ProcessSpec) -> io::Result<StdioTransport> {
        let (driver_stdin, server_stdin) = tokio::io::duplex(1 << 16);
        let (server_stdout, driver_stdout) = tokio::io::duplex(1 << 16);
        let (_, driver_stderr) = tokio::io::duplex(64);
        let (exit, exited) = oneshot::channel();
        let _ = self.0.send(Server {
            lines: BufReader::new(server_stdin).lines(),
            stdout: Some(server_stdout),
            exit: Some(exit),
        });
        Ok(StdioTransport {
            stdin: Box::new(driver_stdin),
            stdout: Box::new(driver_stdout),
            stderr: Box::new(driver_stderr),
            exit: Box::pin(async move { exited.await.map_err(io::Error::other) }),
            pid: None,
        })
    }

    async fn http(&self, _channel: &str, _base: Url) -> io::Result<Arc<dyn HttpTransport>> {
        Err(io::Error::from(io::ErrorKind::Unsupported))
    }

    fn provider_home(&self, _cfg: &InstanceConfig) -> std::path::PathBuf {
        std::env::temp_dir()
    }
}

struct Harness {
    session: Arc<dyn DriverSession>,
    events: AdapterEventReceiver,
    server: Server,
    thread: ProviderThreadRef,
    ids: Arc<SeqIdAllocator>,
}

impl Harness {
    async fn open() -> Self {
        let (spawned, mut servers) = mpsc::unbounded_channel();
        let ids = Arc::new(SeqIdAllocator::new());
        let ctx = SessionContext {
            provider_session_id: ProviderSessionId(ids.fresh(EntityKind::Session)),
            thread_id: ThreadId(ids.fresh(EntityKind::Node)),
            instance: codex_instance(),
            cwd: std::env::temp_dir(),
            selection: Selection::default(),
            resume: None,
            ids: ids.clone(),
            clock: Arc::new(ManualClock::new(1_767_225_600_000)),
            transports: Arc::new(Spawner(spawned)),
            mcp: None,
            output: Arc::new(NullOutput),
            raw_log: Arc::new(NullRawLog),
            shutdown: Shutdown::new(),
        };
        let opening = tokio::spawn(async move { CodexDriver::new().open_session(ctx).await });
        let mut server = servers.recv().await.unwrap();
        let initialize = server.expect("initialize").await;
        server.reply(&initialize, json!({})).await;
        server.expect("initialized").await;
        let opened = opening.await.unwrap().unwrap();
        let session = opened.session;
        let ensuring = {
            let session = session.clone();
            let provider_thread_id = ProviderThreadId(ids.fresh(EntityKind::ProviderThread));
            tokio::spawn(async move {
                session
                    .ensure_thread(EnsureThread {
                        thread_id: ThreadId(uuid::Uuid::nil()),
                        title: None,
                        selection: Selection::default(),
                        existing: Some(ProviderThreadRef::unbound(provider_thread_id)),
                    })
                    .await
            })
        };
        let start = server.expect("thread/start").await;
        server
            .reply(
                &start,
                json!({ "thread": { "id": "thread-1", "turns": [] } }),
            )
            .await;
        let thread = ensuring.await.unwrap().unwrap();
        Self {
            session,
            events: opened.events,
            server,
            thread,
            ids,
        }
    }

    fn run(&self) -> RunRef {
        RunRef {
            thread_id: ThreadId(self.ids.fresh(EntityKind::Node)),
            run_id: RunId(self.ids.fresh(EntityKind::Node)),
            attempt_id: RunAttemptId(self.ids.fresh(EntityKind::Node)),
            root_node_id: NodeId(self.ids.fresh(EntityKind::Node)),
        }
    }

    fn start(&self, run: RunRef) -> tokio::task::JoinHandle<DriverResult<TurnAccepted>> {
        let session = self.session.clone();
        let thread = self.thread.clone();
        tokio::spawn(async move {
            session
                .start_turn(TurnStart {
                    run,
                    run_ordinal: 1,
                    provider_turn_ordinal: 1,
                    restart_continuation_of: None,
                    thread,
                    message: TurnMessage {
                        message_id: MessageId(uuid::Uuid::nil()),
                        text: "go".into(),
                        attachments: Vec::new(),
                    },
                    selection: Selection::default(),
                    app_context: Vec::new(),
                    intent: TurnIntent::Prompt,
                })
                .await
        })
    }

    /// Every terminal until the stream goes quiet.
    async fn terminals(&mut self) -> Vec<TurnTerminal> {
        let mut out = Vec::new();
        while let Ok(Some(event)) =
            tokio::time::timeout(Duration::from_millis(300), self.events.recv()).await
        {
            if let AdapterEvent::TurnTerminal { terminal } = event {
                out.push(terminal);
            }
        }
        out
    }
}

#[tokio::test]
async fn an_interrupt_before_codex_names_the_turn_still_interrupts_it_once() {
    let mut h = Harness::open().await;
    let run = h.run();
    let started = h.start(run);
    let request = h.server.expect("turn/start").await;
    // The user stops before the response arrives.
    h.session
        .interrupt_turn(Interrupt {
            run,
            thread: h.thread.clone(),
            provider_turn: None,
            restart_follows: false,
        })
        .await
        .unwrap();
    h.server
        .reply(
            &request,
            json!({ "turn": { "id": "turn-1", "status": "inProgress", "items": [] } }),
        )
        .await;
    let interrupt = h.server.expect("turn/interrupt").await;
    assert_eq!(interrupt["params"]["turnId"], "turn-1");
    h.server.reply(&interrupt, json!({})).await;
    h.server
        .send(
            json!({ "method": "turn/completed", "params": { "threadId": "thread-1",
            "turn": { "id": "turn-1", "status": "interrupted", "items": [], "error": null } } }),
        )
        .await;
    let accepted = started.await.unwrap().unwrap();
    assert_eq!(accepted.provider_turn.unwrap().native.id, "turn-1");
    let terminals = h.terminals().await;
    assert_eq!(terminals.len(), 1);
    assert_eq!(terminals[0].outcome, TurnOutcome::Interrupted);
    assert_eq!(terminals[0].thread_disposition, ThreadDisposition::Reusable);
}

#[tokio::test]
async fn a_stream_that_ends_mid_turn_fails_the_turn_once_and_ends_the_events() {
    let mut h = Harness::open().await;
    let run = h.run();
    let started = h.start(run);
    let request = h.server.expect("turn/start").await;
    h.server
        .reply(&request, json!({ "turn": { "id": "turn-1" } }))
        .await;
    started.await.unwrap().unwrap();
    h.server.crash();
    let mut terminals = Vec::new();
    while let Ok(Some(event)) = tokio::time::timeout(Duration::from_secs(5), h.events.recv()).await
    {
        if let AdapterEvent::TurnTerminal { terminal } = event {
            terminals.push(terminal);
        }
    }
    assert_eq!(terminals.len(), 1, "the stream ended after one terminal");
    let TurnOutcome::Failed { failure } = &terminals[0].outcome else {
        panic!("{:?}", terminals[0].outcome);
    };
    assert_eq!(failure.class, FailureClass::TransportError);
    // A request in flight when the process died fails as a lost session.
    let error = h
        .session
        .goal(GoalRequest::Get)
        .await
        .expect_err("the process is gone");
    assert!(matches!(error, DriverError::SessionLost { .. }), "{error}");
}

#[tokio::test]
async fn provider_errors_are_classified_and_steers_after_the_turn_are_refused() {
    let mut h = Harness::open().await;
    let run = h.run();
    let started = h.start(run);
    let request = h.server.expect("turn/start").await;
    h.server
        .reply(&request, json!({ "turn": { "id": "turn-1" } }))
        .await;
    started.await.unwrap().unwrap();
    // A steer Codex refuses because the turn just ended.
    let steering = {
        let session = h.session.clone();
        let thread = h.thread.clone();
        tokio::spawn(async move {
            session
                .steer_turn(Steer {
                    run,
                    thread,
                    provider_turn: None,
                    message: TurnMessage {
                        message_id: MessageId(uuid::Uuid::nil()),
                        text: "faster".into(),
                        attachments: Vec::new(),
                    },
                })
                .await
        })
    };
    let steer = h.server.expect("turn/steer").await;
    assert_eq!(steer["params"]["expectedTurnId"], "turn-1");
    h.server
        .send(json!({ "id": steer["id"], "error": { "code": -32600, "message": "no active turn to steer" } }))
        .await;
    assert!(matches!(
        steering.await.unwrap(),
        Err(DriverError::SteerRejected {
            reason: SteerRejectReason::TurnSettled
        })
    ));
    h.server
        .send(json!({ "method": "error", "params": { "threadId": "thread-1", "turnId": "turn-1",
            "willRetry": false, "error": { "message": "Context window exceeded.", "codexErrorInfo": "contextWindowExceeded" } } }))
        .await;
    h.server
        .send(json!({ "method": "turn/completed", "params": { "threadId": "thread-1",
            "turn": { "id": "turn-1", "status": "failed", "error": { "message": "Context window exceeded." } } } }))
        .await;
    let terminals = h.terminals().await;
    assert_eq!(terminals.len(), 1);
    let TurnOutcome::Failed { failure } = &terminals[0].outcome else {
        panic!("{:?}", terminals[0].outcome);
    };
    assert_eq!(failure.class, FailureClass::ProviderError);
    assert_eq!(failure.code.as_deref(), Some("contextWindowExceeded"));
}

#[tokio::test]
async fn goal_turns_codex_starts_itself_are_not_counted_but_are_reverted() {
    let mut h = Harness::open().await;
    let run = h.run();
    let started = h.start(run);
    let request = h.server.expect("turn/start").await;
    h.server
        .reply(&request, json!({ "turn": { "id": "turn-1" } }))
        .await;
    let accepted = started.await.unwrap().unwrap().provider_turn.unwrap();
    h.server
        .send(
            json!({ "method": "turn/completed", "params": { "threadId": "thread-1",
            "turn": { "id": "turn-1", "status": "completed", "items": [] } } }),
        )
        .await;
    // A `/goal` continuation Codex starts on its own.
    h.server
        .send(
            json!({ "method": "turn/started", "params": { "threadId": "thread-1",
            "turn": { "id": "goal-turn", "status": "inProgress" } } }),
        )
        .await;
    h.server
        .send(
            json!({ "method": "turn/completed", "params": { "threadId": "thread-1",
            "turn": { "id": "goal-turn", "status": "completed", "items": [] } } }),
        )
        .await;
    let mut goal_turn = None;
    let mut terminals = 0;
    while let Ok(Some(event)) =
        tokio::time::timeout(Duration::from_millis(300), h.events.recv()).await
    {
        match event {
            AdapterEvent::ProviderTurnUpdated { turn }
                if turn
                    .native_turn_ref
                    .as_ref()
                    .and_then(|r| r.native_id.as_deref())
                    == Some("goal-turn") =>
            {
                goal_turn = Some(turn);
            }
            AdapterEvent::TurnTerminal { .. } => terminals += 1,
            _ => {}
        }
    }
    let goal_turn = goal_turn.expect("the goal turn is reported");
    assert_eq!(goal_turn.run_attempt_id, None, "it belongs to no run");
    assert_eq!(terminals, 1, "only the run's own turn has a terminal");
    // Rolling back to the first turn drops the goal turn as well.
    let rolling = {
        let session = h.session.clone();
        let thread = h.thread.clone();
        tokio::spawn(async move {
            session
                .rollback(Rollback {
                    thread,
                    target: RollbackTarget::AfterProviderTurn(accepted.clone()),
                    turns: vec![accepted],
                    turns_to_remove: 0,
                })
                .await
        })
    };
    let list = h.server.expect("thread/turns/list").await;
    assert_eq!(list["params"]["limit"], 1);
    h.server
        .reply(
            &list,
            json!({ "data": [{ "id": "goal-turn" }], "nextCursor": null }),
        )
        .await;
    let revert = h.server.expect("thread/revert").await;
    assert_eq!(revert["params"]["beforeTurnId"], "goal-turn");
    h.server
        .reply(&revert, json!({ "thread": { "id": "thread-1" } }))
        .await;
    let outcome = rolling.await.unwrap().unwrap();
    assert!(!outcome.replaced);
    assert_eq!(
        outcome.retained_native_turn_ids,
        Some(vec!["turn-1".to_owned()])
    );
}
