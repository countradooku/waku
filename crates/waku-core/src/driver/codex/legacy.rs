//! The Codex session behind the old `DriverControl` interface, until the
//! orchestrator cutover (RFC 0002 D-9).
//!
//! `start_local` opens the `DriverV2` session through `V1Bridge::start` and
//! adds what the bridge does not translate yet, so the desktop keeps its
//! Codex behaviour:
//! - rich activities: the [`Sidecar`] sees every raw frame before it is
//!   normalized and keeps the old presentation (`RichActivity`) per native
//!   item; the bridge's plain `Activity` for that item is replaced in order;
//! - background terminals and subagents as `BackgroundWork` events, plus
//!   the refresh and stop commands;
//! - the first prompt's generated title, persisted with `thread/name/set`;
//! - visible stderr notices and MCP start-up failures as `Error`;
//! - Stop for a turn Codex started on its own (a `/goal` continuation);
//! - the Computer Use preview monitor.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use anyhow::anyhow;
use parking_lot::Mutex;
use serde_json::Value;
use waku_protocol::orchestration as orch;

use super::{CodexDriver, CodexSession, Shared};
use crate::computer_use;
use crate::computer_use::ComputerToolRequest;
use crate::driver::computer_use as computer_use_runtime;
use crate::driver::v2::bridge::V1Bridge;
use crate::driver::v2::{
    BackgroundWorkRef, ComputerUseControl, DriverSession, GoalRequest, InstanceConfig, ProcessExit,
    ProcessTransportFactory,
};
use crate::driver::{DriverControl, DriverEventSender, DriverStartOptions, SessionOptions};
use crate::model::{
    ActivityItem, ActivityKind, BackgroundWorkEvent, BackgroundWorkItem, BackgroundWorkKey,
    BackgroundWorkKind, BackgroundWorkStatus, DriverEvent, GoalOperation, ProviderResumeCursor,
    UserInputAnswer, unix_time_millis,
};

const DISABLE_EXTERNAL_COMPUTER_USE_PLUGIN: &str =
    "plugins.computer-use@openai-bundled.enabled=false";
// Codex 0.146 only resolves plugin enablement from user/profile config layers, so a
// process-local `-c` plugin override does not yet suppress its bundled capabilities.
const DISABLE_EXTERNAL_COMPUTER_USE_MCP_COMMAND: &str =
    "mcp_servers.computer-use.command=\"/usr/bin/true\"";
const DISABLE_EXTERNAL_COMPUTER_USE_MCP: &str = "mcp_servers.computer-use.enabled=false";
const DISABLE_EXTERNAL_COMPUTER_USE_SKILL: &str =
    r#"skills.config=[{name="computer-use:computer-use",enabled=false}]"#;
// Codex validates MCP transport fields even for disabled servers, so the
// override needs a valid stdio command before disabling the bundled server.
const DISABLE_CODEX_NODE_REPL_COMMAND: &str = "mcp_servers.node_repl.command=\"/usr/bin/true\"";
const DISABLE_CODEX_NODE_REPL: &str = "mcp_servers.node_repl.enabled=false";

fn toml_string(value: &str) -> String {
    serde_json::to_string(value).expect("a filesystem path is always valid JSON")
}

/// Computer Use (debug builds): Waku's long-lived QuickJS MCP server with the
/// raw native helper kept private behind its built-in `cua` object, so Codex
/// sees only the compact `js` / `js_reset` surface.
pub(super) struct ComputerUse {
    server_path: PathBuf,
    server: String,
    repl: String,
    skill_root: PathBuf,
    process_directory: PathBuf,
    process_directory_config: String,
}

impl ComputerUse {
    pub(super) fn load() -> anyhow::Result<Self> {
        let server_path = computer_use::mcp_server_command()?;
        let server = toml_string(&server_path.display().to_string());
        let repl_path = computer_use::js_repl_server_path()?;
        let repl = toml_string(&repl_path.display().to_string());
        let skill_root = computer_use::skill_root_path()?;
        let process_directory = computer_use_runtime::create_process_directory()?;
        let process_directory_config = toml_string(&process_directory.display().to_string());
        Ok(Self {
            server_path,
            server,
            repl,
            skill_root,
            process_directory,
            process_directory_config,
        })
    }

    pub(super) fn skill_root(&self) -> String {
        self.skill_root.display().to_string()
    }

    /// The `-c` overrides and environment the app-server starts with.
    pub(super) fn configure(&self, args: &mut Vec<String>, env: &mut Vec<(String, String)>) {
        for setting in [
            DISABLE_EXTERNAL_COMPUTER_USE_PLUGIN.to_owned(),
            DISABLE_EXTERNAL_COMPUTER_USE_MCP_COMMAND.to_owned(),
            DISABLE_EXTERNAL_COMPUTER_USE_MCP.to_owned(),
            DISABLE_EXTERNAL_COMPUTER_USE_SKILL.to_owned(),
            DISABLE_CODEX_NODE_REPL_COMMAND.to_owned(),
            DISABLE_CODEX_NODE_REPL.to_owned(),
            format!("mcp_servers.waku_js_repl.command={}", self.repl),
            "mcp_servers.waku_js_repl.args=[]".to_owned(),
            format!(
                "mcp_servers.waku_js_repl.env.WAKU_COMPUTER_USE_SERVER={}",
                self.server
            ),
            format!(
                "mcp_servers.waku_js_repl.env.WAKU_COMPUTER_USE_PROCESS_DIRECTORY={}",
                self.process_directory_config
            ),
        ] {
            args.push("-c".to_owned());
            args.push(setting);
        }
        env.push((
            "WAKU_COMPUTER_USE_SERVER".to_owned(),
            self.server_path.display().to_string(),
        ));
        env.push((
            "WAKU_COMPUTER_USE_PROCESS_DIRECTORY".to_owned(),
            self.process_directory.display().to_string(),
        ));
    }
}

impl ComputerUseControl for ComputerUse {
    fn run_computer_tool(&self, _request: ComputerToolRequest) {}

    fn reject_computer_tool(&self, _request: ComputerToolRequest, _reason: String) {}

    fn cancel_computer_use(&self) {
        computer_use_runtime::stop_registered_processes(&self.process_directory, &self.server_path);
    }
}

impl Drop for ComputerUse {
    fn drop(&mut self) {
        self.cancel_computer_use();
        let _ = fs::remove_dir_all(&self.process_directory);
    }
}

/// The bridge's runtime: drivers are async (RFC 0002 D-2) and the daemon
/// that calls `start_local` is not.
fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("waku-codex")
            .enable_all()
            .build()
            .expect("the Codex driver runtime starts")
    })
}

/// What the raw-frame observer and the `DriverControl` wrapper share.
pub(super) struct Sidecar {
    events: DriverEventSender,
    shared: OnceLock<Arc<Shared>>,
    /// The old presentation of each native item, started and completed.
    rich: Mutex<HashMap<(String, bool), ActivityItem>>,
    visible_stderr: Mutex<bool>,
}

impl Sidecar {
    fn new(events: DriverEventSender) -> Self {
        Self {
            events,
            shared: OnceLock::new(),
            rich: Mutex::new(HashMap::new()),
            visible_stderr: Mutex::new(false),
        }
    }

    pub(super) fn attach(&self, shared: Arc<Shared>) {
        let _ = self.shared.set(shared);
    }

    fn session(&self) -> Option<CodexSession> {
        self.shared.get().map(|shared| CodexSession {
            shared: shared.clone(),
        })
    }

    fn send(&self, event: DriverEvent) {
        let _ = self.events.send(event);
    }

    /// Sees a raw frame before it is normalized.
    pub(super) fn observe(&self, frame: &Value) {
        let Some(method) = frame.get("method").and_then(Value::as_str) else {
            return;
        };
        let params = frame.get("params").cloned().unwrap_or(Value::Null);
        match method {
            "item/started" | "item/completed" => {
                let Some(item) = params.get("item") else {
                    return;
                };
                let complete = method == "item/completed";
                if let Some(work) = codex_command_work(item, complete) {
                    self.send(DriverEvent::BackgroundWork(BackgroundWorkEvent::Upsert(
                        work,
                    )));
                }
                for work in codex_subagent_work(item) {
                    self.send(DriverEvent::BackgroundWork(BackgroundWorkEvent::Upsert(
                        work,
                    )));
                }
                if let Some(activity) = rich_activity(item, complete) {
                    let id = item
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned();
                    self.rich.lock().insert((id, complete), activity);
                }
            }
            "item/commandExecution/outputDelta" => {
                if let (Some(item_id), Some(delta)) = (
                    params.get("itemId").and_then(Value::as_str),
                    params.get("delta").and_then(Value::as_str),
                ) && !delta.is_empty()
                {
                    self.send(DriverEvent::BackgroundWork(
                        BackgroundWorkEvent::OutputDelta {
                            key: BackgroundWorkKey::new(BackgroundWorkKind::Process, item_id),
                            delta: delta.to_owned(),
                        },
                    ));
                }
            }
            "mcpServer/startupStatus/updated"
                if params.get("status").and_then(Value::as_str) == Some("failed") =>
            {
                if let Some(message) = params.get("error").and_then(Value::as_str) {
                    let name = params.get("name").and_then(Value::as_str).unwrap_or("MCP");
                    self.send(DriverEvent::Error(format!("{name}: {message}")));
                }
            }
            "error" => {
                if params.get("willRetry").and_then(Value::as_bool) != Some(true) {
                    return;
                }
                if let Some(message) = params.pointer("/error/message").and_then(Value::as_str) {
                    self.send(DriverEvent::Error(message.to_owned()));
                }
            }
            _ => {}
        }
    }

    pub(super) fn stderr(&self, line: &str) {
        if is_visible_stderr_notice(line) {
            *self.visible_stderr.lock() = true;
            self.send(DriverEvent::Error(clean_stderr(line)));
        }
    }

    pub(super) fn exited(&self, status: &std::io::Result<ProcessExit>) {
        match status {
            Ok(exit) if !exit.success && !*self.visible_stderr.lock() => {
                self.send(DriverEvent::Error(tr!(
                    "errors.provider_exited",
                    provider = "Codex app-server",
                    status = exit
                        .code
                        .map_or_else(|| "signal".to_owned(), |code| code.to_string())
                )));
            }
            Err(error) => self.send(DriverEvent::Error(tr!(
                "errors.read_provider_exit_status",
                provider = "Codex app-server",
                error = error
            ))),
            _ => {}
        }
    }

    /// The bridge's plain activity, replaced by the old presentation of the
    /// same item. Items the old driver did not present stay hidden.
    fn rewrite(&self, event: DriverEvent) -> Option<DriverEvent> {
        match event {
            DriverEvent::Activity {
                id: Some(id),
                complete,
                ..
            } => self
                .rich
                .lock()
                .get(&(id, complete))
                .cloned()
                .map(DriverEvent::RichActivity),
            DriverEvent::Activity { id: None, .. } => None,
            other => Some(other),
        }
    }
}

struct TitleState {
    enabled: bool,
    launched: bool,
    pending: Option<String>,
}

/// The Codex session served as a `DriverControl`.
pub(super) struct CodexV1 {
    bridge: V1Bridge,
    sidecar: Arc<Sidecar>,
    title: Arc<Mutex<TitleState>>,
    preview: Option<computer_use_runtime::ComputerUsePreviewMonitor>,
}

/// Starts Codex for the desktop: a `DriverV2` session behind `V1Bridge`.
pub(crate) fn start_local(
    options: DriverStartOptions,
    events: DriverEventSender,
) -> anyhow::Result<Arc<dyn DriverControl>> {
    if let Some(cursor) = &options.provider_cursor
        && !matches!(cursor, ProviderResumeCursor::Codex { .. })
    {
        return Err(anyhow!(
            "cannot resume Codex from a {} cursor",
            cursor.provider().display_name()
        ));
    }
    let resumed = options.provider_cursor.is_some();
    let binary = options.binary.clone();
    let cwd = options.cwd.clone();
    let sidecar = Arc::new(Sidecar::new(events.clone()));
    let (wake, _wakes) = smol::channel::bounded(1);
    let (bridge_events, bridged) = crate::driver::event_channel(wake);
    let driver = Arc::new(CodexDriver {
        legacy: Some(sidecar.clone()),
    });
    let mut instance = InstanceConfig::new(super::CODEX_PROVIDER, orch::DriverKind::Codex);
    instance.extra = Value::Null;
    let runtime = runtime().handle().clone();
    // `V1Bridge::start` blocks on the runtime; the caller may itself run on
    // another runtime's blocking pool.
    let bridge = {
        let runtime = runtime.clone();
        std::thread::Builder::new()
            .name("waku-codex-start".into())
            .spawn(move || {
                V1Bridge::start(
                    driver,
                    instance,
                    options,
                    bridge_events,
                    Arc::new(ProcessTransportFactory),
                    runtime,
                )
            })?
            .join()
            .map_err(|_| anyhow!("the Codex driver failed to start"))??
    };
    let title = Arc::new(Mutex::new(TitleState {
        enabled: !resumed,
        launched: false,
        pending: None,
    }));
    let preview = sidecar
        .shared
        .get()
        .and_then(|shared| shared.computer_use.as_ref())
        .map(|computer_use| {
            computer_use_runtime::ComputerUsePreviewMonitor::start(
                computer_use.process_directory.clone(),
                events.clone(),
            )
        })
        .transpose()?;
    {
        let sidecar = sidecar.clone();
        let title = title.clone();
        let runtime = runtime.clone();
        std::thread::Builder::new()
            .name("waku-codex-events".into())
            .spawn(move || forward(bridged, sidecar, title, binary, cwd, runtime))?;
    }
    if resumed && let Some(session) = sidecar.session() {
        // A resumed thread may carry a goal from an earlier run; probe once,
        // quietly (older app-servers have no goal API).
        runtime.spawn(async move {
            let _ = session.goal(GoalRequest::Get).await;
        });
    }
    Ok(Arc::new(CodexV1 {
        bridge,
        sidecar,
        title,
        preview,
    }))
}

/// Forwards the bridge's events in order, with the old presentation of
/// activities, and starts the title once the first turn runs.
fn forward(
    bridged: crossbeam_channel::Receiver<DriverEvent>,
    sidecar: Arc<Sidecar>,
    title: Arc<Mutex<TitleState>>,
    binary: PathBuf,
    cwd: PathBuf,
    runtime: tokio::runtime::Handle,
) {
    while let Ok(event) = bridged.recv() {
        if matches!(event, DriverEvent::TurnStarted) {
            let prompt = {
                let mut title = title.lock();
                if title.enabled && !title.launched {
                    let prompt = title.pending.take();
                    title.launched = prompt.is_some();
                    prompt
                } else {
                    None
                }
            };
            if let Some(prompt) = prompt {
                let (binary, cwd, sidecar, runtime) = (
                    binary.clone(),
                    cwd.clone(),
                    sidecar.clone(),
                    runtime.clone(),
                );
                let _ = std::thread::Builder::new()
                    .name("waku-codex-title".into())
                    .spawn(move || {
                        let Ok(name) = super::title::generate_codex_title(&binary, &cwd, &prompt)
                        else {
                            return;
                        };
                        // Update Waku even if persisting the name back to
                        // Codex fails; Codex echoes the name when it lands.
                        sidecar.send(DriverEvent::AutoTitleUpdated(Some(name.clone())));
                        if let Some(session) = sidecar.session() {
                            runtime.spawn(async move {
                                let _ = session.set_thread_name(&name).await;
                            });
                        }
                    });
            }
        }
        if let Some(event) = sidecar.rewrite(event) {
            sidecar.send(event);
        }
    }
}

impl CodexV1 {
    fn spawn(&self, task: impl std::future::Future<Output = ()> + Send + 'static) {
        runtime().spawn(task);
    }
}

impl DriverControl for CodexV1 {
    fn prompt(&self, prompt: String) {
        {
            let mut title = self.title.lock();
            if title.enabled && !title.launched && title.pending.is_none() {
                title.pending = Some(prompt.clone());
            }
        }
        self.bridge.prompt(prompt);
    }

    fn supports_steer(&self) -> bool {
        self.bridge.supports_steer()
    }

    fn steer(&self, prompt: String) {
        self.bridge.steer(prompt);
    }

    fn cancel(&self) {
        // A turn Codex started itself (a `/goal` continuation) belongs to no
        // bridge run; Stop reaches it directly.
        let active = self
            .sidecar
            .shared
            .get()
            .is_some_and(|shared| shared.state.lock().active_root_turn().is_some());
        match self.sidecar.session().filter(|_| active) {
            Some(session) => self.spawn(async move {
                let _ = session.interrupt_active().await;
            }),
            None => self.bridge.cancel(),
        }
    }

    fn cancel_computer_use(&self) {
        self.bridge.cancel_computer_use();
    }

    fn refresh_background_work(&self) {
        let Some(session) = self.sidecar.session() else {
            return;
        };
        let sidecar = self.sidecar.clone();
        self.spawn(async move {
            if let Ok(terminals) = session.background_terminals().await {
                let items = terminals
                    .iter()
                    .filter_map(codex_background_terminal)
                    .collect();
                sidecar.send(DriverEvent::BackgroundWork(
                    BackgroundWorkEvent::ReconcileProcesses { items },
                ));
            }
        });
    }

    fn stop_background_work(&self, key: BackgroundWorkKey, control_id: String) {
        let Some(session) = self.sidecar.session() else {
            return;
        };
        let sidecar = self.sidecar.clone();
        self.spawn(async move {
            let provider_thread_id = session
                .shared
                .state
                .lock()
                .root
                .as_ref()
                .map(|root| root.id)
                .unwrap_or(orch::ProviderThreadId(uuid::Uuid::nil()));
            let result = session
                .stop_background_work(BackgroundWorkRef {
                    provider_thread_id,
                    task_id: control_id,
                })
                .await;
            let event = match result {
                Ok(()) => {
                    let mut item = BackgroundWorkItem::new(
                        key.kind,
                        key.provider_id.clone(),
                        "",
                        BackgroundWorkStatus::Stopped,
                    );
                    item.key = key;
                    item.background = true;
                    item.updated_at_ms = unix_time_millis();
                    BackgroundWorkEvent::Upsert(item)
                }
                Err(error) => BackgroundWorkEvent::StopFailed {
                    key,
                    message: error.to_string(),
                },
            };
            sidecar.send(DriverEvent::BackgroundWork(event));
        });
    }

    fn respond(&self, request_id: String, option_id: String) {
        self.bridge.respond(request_id, option_id);
    }

    fn respond_user_input(&self, request_id: String, answers: Vec<UserInputAnswer>) {
        self.bridge.respond_user_input(request_id, answers);
    }

    fn goal(&self, operation: GoalOperation) {
        self.bridge.goal(operation);
    }

    fn run_computer_tool(&self, request: ComputerToolRequest) {
        self.bridge.run_computer_tool(request);
    }

    fn reject_computer_tool(&self, request: ComputerToolRequest, reason: String) {
        self.bridge.reject_computer_tool(request, reason);
    }

    fn apply_options(&self, options: SessionOptions) -> bool {
        self.bridge.apply_options(options)
    }

    fn rollback(&self, turns: usize) -> anyhow::Result<Option<ProviderResumeCursor>> {
        if turns == 0 {
            return Ok(None);
        }
        self.bridge.rollback(turns)
    }

    fn fork(&self, turns_to_remove: usize) -> anyhow::Result<ProviderResumeCursor> {
        self.bridge.fork(turns_to_remove)
    }
}

impl Drop for CodexV1 {
    fn drop(&mut self) {
        self.bridge.cancel_computer_use();
        drop(self.preview.take());
    }
}

/// The old presentation of an app-server item.
fn rich_activity(item: &Value, complete: bool) -> Option<ActivityItem> {
    let kind = codex_activity_kind(item)?;
    let title = codex_item_title(item);
    let output = codex_item_output(item);
    let image_urls = codex_item_image_urls(item);
    let detail = codex_item_detail(item, output.as_deref());
    Some(
        ActivityItem::new(
            item.get("id").and_then(Value::as_str).map(str::to_owned),
            kind,
            title,
            detail,
            complete,
        )
        .with_tool_name(item.get("tool").and_then(Value::as_str))
        .with_mcp_server(item.get("server").and_then(Value::as_str))
        .with_arguments(codex_item_arguments(item))
        .with_activity_source(Some(item))
        .with_output(output)
        .with_image_urls(image_urls)
        .with_failed(codex_item_failed(item)),
    )
}

fn split_camel_case(value: &str) -> String {
    super::protocol::split_camel_case(value)
}

fn json_id(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn codex_background_terminal(item: &Value) -> Option<BackgroundWorkItem> {
    let item_id = json_id(item.get("itemId").or_else(|| item.get("id")))?;
    let process_id = json_id(item.get("processId"))?;
    let command = item
        .get("command")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| tr!("background.process"));
    let mut work = BackgroundWorkItem::new(
        BackgroundWorkKind::Process,
        item_id,
        command.clone(),
        BackgroundWorkStatus::Running,
    );
    work.command = Some(command);
    work.cwd = item.get("cwd").and_then(Value::as_str).map(str::to_owned);
    let mut telemetry = Vec::new();
    if let Some(pid) = item.get("osPid").and_then(Value::as_u64) {
        telemetry.push(format!("PID {pid}"));
    }
    if let Some(cpu) = item.get("cpuPercent").and_then(Value::as_f64) {
        telemetry.push(format!("CPU {cpu:.1}%"));
    }
    if let Some(rss_kb) = item.get("rssKb").and_then(Value::as_u64) {
        telemetry.push(format!("{:.1} MB", (rss_kb as f64 / 1024.0).max(0.1)));
    }
    if !telemetry.is_empty() {
        work.detail = Some(telemetry.join("  ·  "));
    }
    work.background = true;
    work.can_stop = true;
    work.control_id = Some(process_id);
    work.updated_at_ms = unix_time_millis();
    Some(work)
}

fn codex_command_work(item: &Value, complete: bool) -> Option<BackgroundWorkItem> {
    if item.get("type").and_then(Value::as_str) != Some("commandExecution") {
        return None;
    }
    let item_id = item.get("id").and_then(Value::as_str)?.to_owned();
    let command = item
        .get("command")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| tr!("background.command"));
    let exit_code = item
        .get("exitCode")
        .and_then(Value::as_i64)
        .and_then(|code| i32::try_from(code).ok());
    let status = if !complete {
        BackgroundWorkStatus::Running
    } else if codex_item_failed(item) || exit_code.is_some_and(|code| code != 0) {
        BackgroundWorkStatus::Failed
    } else {
        BackgroundWorkStatus::Completed
    };
    let mut work = BackgroundWorkItem::new(
        BackgroundWorkKind::Process,
        item_id.clone(),
        command.clone(),
        status,
    );
    work.command = Some(command);
    work.cwd = item.get("cwd").and_then(Value::as_str).map(str::to_owned);
    work.output = item
        .get("aggregatedOutput")
        .and_then(Value::as_str)
        .filter(|output| !output.is_empty())
        .map(str::to_owned);
    work.duration_ms = item
        .get("durationMs")
        .or_else(|| item.get("duration_ms"))
        .and_then(Value::as_u64);
    work.exit_code = exit_code;
    work.control_id = json_id(item.get("processId"));
    work.origin_activity_id = Some(item_id);
    work.updated_at_ms = unix_time_millis();
    Some(work)
}

fn codex_agent_status(status: &str) -> BackgroundWorkStatus {
    match status {
        "pendingInit" => BackgroundWorkStatus::Starting,
        "running" => BackgroundWorkStatus::Running,
        "completed" | "shutdown" => BackgroundWorkStatus::Completed,
        "interrupted" => BackgroundWorkStatus::Stopped,
        "errored" | "notFound" => BackgroundWorkStatus::Failed,
        _ => BackgroundWorkStatus::Running,
    }
}

fn codex_subagent_work(item: &Value) -> Vec<BackgroundWorkItem> {
    if item.get("type").and_then(Value::as_str) != Some("collabAgentToolCall") {
        return Vec::new();
    }
    let origin = item.get("id").and_then(Value::as_str).map(str::to_owned);
    let prompt = item
        .get("prompt")
        .and_then(Value::as_str)
        .and_then(|prompt| prompt.lines().find(|line| !line.trim().is_empty()))
        .map(str::trim)
        .filter(|prompt| !prompt.is_empty())
        .map(|prompt| {
            let mut title = prompt.chars().take(96).collect::<String>();
            if prompt.chars().count() > 96 {
                title.push('…');
            }
            title
        })
        .unwrap_or_else(|| tr!("background.subagent"));
    let model = item.get("model").and_then(Value::as_str).map(str::to_owned);
    let parent_id = item
        .get("senderThreadId")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let role = item
        .get("agentType")
        .or_else(|| item.get("role"))
        .or_else(|| item.get("tool"))
        .and_then(Value::as_str)
        .map(split_camel_case);
    item.get("agentsStates")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
        .map(|(thread_id, state)| {
            let status = codex_agent_status(
                state
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("running"),
            );
            let mut work = BackgroundWorkItem::new(
                BackgroundWorkKind::Subagent,
                thread_id,
                prompt.clone(),
                status,
            );
            work.detail = state
                .get("message")
                .and_then(Value::as_str)
                .filter(|message| !message.is_empty())
                .map(str::to_owned);
            work.background = true;
            work.origin_activity_id = origin.clone();
            work.role = role.clone();
            work.model = model.clone();
            work.parent_id = parent_id.clone();
            work.updated_at_ms = unix_time_millis();
            work
        })
        .collect()
}

fn codex_activity_kind(item: &Value) -> Option<ActivityKind> {
    let item_type = item
        .get("type")
        .and_then(Value::as_str)?
        .to_ascii_lowercase();
    if item_type.contains("command") {
        Some(
            codex_command_action_presentation(item)
                .map(|(kind, _)| kind)
                .unwrap_or(ActivityKind::Command),
        )
    } else if item_type.contains("filechange") || item_type.contains("patch") {
        Some(ActivityKind::FileChange)
    } else if item_type.contains("websearch") {
        Some(ActivityKind::Search)
    } else if item_type.contains("plan") || item_type.contains("todo") {
        Some(ActivityKind::Plan)
    } else if item_type.contains("tool") || item_type.contains("collab") {
        Some(
            item.get("tool")
                .or_else(|| item.get("name"))
                .and_then(Value::as_str)
                .map(ActivityKind::from_tool_name)
                .unwrap_or(ActivityKind::Tool),
        )
    } else {
        None
    }
}

fn codex_item_title(item: &Value) -> String {
    if let Some((_, title)) = codex_command_action_presentation(item) {
        return title;
    }
    if let Some(command) = item.get("command").and_then(Value::as_str) {
        return command.to_owned();
    }
    if let Some(query) = non_empty_string(item.get("query")) {
        return tr!("activity.search_for", query = query);
    }
    if item.get("type").and_then(Value::as_str) == Some("webSearch") {
        return codex_web_search_title(item);
    }
    if item.get("type").and_then(Value::as_str) == Some("mcpToolCall")
        && let Some(title) = item
            .pointer("/arguments/title")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|title| !title.is_empty())
    {
        return title.to_owned();
    }
    if let Some(name) = item.get("tool").and_then(Value::as_str) {
        return split_camel_case(name);
    }
    item.get("type")
        .and_then(Value::as_str)
        .map(split_camel_case)
        .unwrap_or_else(|| tr!("activity.activity"))
}

fn codex_web_search_title(item: &Value) -> String {
    let Some(action) = item.get("action") else {
        return tr!("activity.searched_web");
    };

    match action.get("type").and_then(Value::as_str) {
        Some("search") => {
            if let Some(query) = non_empty_string(action.get("query")) {
                return tr!("activity.search_for", query = query);
            }
            if let Some(query) =
                action
                    .get("queries")
                    .and_then(Value::as_array)
                    .and_then(|queries| {
                        queries
                            .iter()
                            .find_map(|query| non_empty_string(Some(query)))
                    })
            {
                return tr!("activity.search_for", query = query);
            }
            tr!("activity.searched_web")
        }
        Some("openPage") => non_empty_string(action.get("url"))
            .map(|url| tr!("activity.open_url", url = url))
            .unwrap_or_else(|| tr!("activity.opened_web_page")),
        Some("findInPage") => match (
            non_empty_string(action.get("pattern")),
            non_empty_string(action.get("url")),
        ) {
            (Some(pattern), Some(url)) => {
                tr!("activity.find_in_url", pattern = pattern, url = url)
            }
            (Some(pattern), None) => tr!("activity.find_on_page", pattern = pattern),
            (None, Some(url)) => tr!("activity.search_within_url", url = url),
            (None, None) => tr!("activity.searched_within_page"),
        },
        _ => item
            .get("results")
            .and_then(Value::as_array)
            .filter(|results| !results.is_empty())
            .map(|results| {
                if results.len() == 1 {
                    tr!("activity.browsed_page", count = results.len())
                } else {
                    tr!("activity.browsed_pages", count = results.len())
                }
            })
            .unwrap_or_else(|| tr!("activity.browsed_web")),
    }
}

fn non_empty_string(value: Option<&Value>) -> Option<&str> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn codex_item_detail(item: &Value, output: Option<&str>) -> Option<String> {
    if codex_item_failed(item)
        && let Some(first_line) =
            output.and_then(|output| output.lines().map(str::trim).find(|line| !line.is_empty()))
    {
        return Some(first_line.to_owned());
    }
    if item.get("type").and_then(Value::as_str) == Some("fileChange") {
        return None;
    }
    item.get("cwd")
        .and_then(Value::as_str)
        .or_else(|| item.get("path").and_then(Value::as_str))
        .or_else(|| item.get("status").and_then(Value::as_str))
        .map(str::to_owned)
}

fn codex_item_arguments(item: &Value) -> Option<String> {
    match item.get("type").and_then(Value::as_str) {
        Some("mcpToolCall") => item
            .get("arguments")
            .filter(|value| !value.is_null())
            .and_then(format_activity_json),
        Some("fileChange") => item
            .get("changes")
            .filter(|value| !value.is_null())
            .and_then(format_activity_json),
        Some("commandExecution") => {
            let mut arguments = serde_json::Map::new();
            if let Some(command) = item.get("command") {
                arguments.insert("command".into(), command.clone());
            }
            if let Some(cwd) = item.get("cwd") {
                arguments.insert("cwd".into(), cwd.clone());
            }
            if let Some(action) = codex_single_presentable_command_action(item) {
                arguments.insert("action".into(), action.clone());
            }
            (!arguments.is_empty())
                .then(|| Value::Object(arguments))
                .as_ref()
                .and_then(format_activity_json)
        }
        _ => None,
    }
}

fn codex_single_presentable_command_action(item: &Value) -> Option<&Value> {
    if item.get("type").and_then(Value::as_str) != Some("commandExecution") {
        return None;
    }
    let [action] = item.get("commandActions")?.as_array()?.as_slice() else {
        return None;
    };
    matches!(
        action.get("type").and_then(Value::as_str),
        Some("read" | "listFiles" | "search")
    )
    .then_some(action)
}

fn codex_command_action_presentation(item: &Value) -> Option<(ActivityKind, String)> {
    match codex_single_presentable_command_action(item)?
        .get("type")
        .and_then(Value::as_str)?
    {
        "read" => Some((ActivityKind::FileRead, tr!("activity.read_file"))),
        "listFiles" => Some((ActivityKind::FileList, tr!("activity.list_files"))),
        "search" => Some((ActivityKind::FileSearch, tr!("activity.search_files"))),
        _ => None,
    }
}

fn codex_item_output(item: &Value) -> Option<String> {
    match item.get("type").and_then(Value::as_str) {
        Some("mcpToolCall") => {
            if let Some(message) = item.pointer("/error/message").and_then(Value::as_str) {
                return non_empty_activity_text(message.to_owned());
            }
            let result = item.get("result").filter(|value| !value.is_null())?;
            if let Some(structured) = result
                .get("structuredContent")
                .filter(|value| !value.is_null())
            {
                return format_activity_json(structured);
            }
            result
                .get("content")
                .filter(|value| !value.is_null())
                .and_then(|content| {
                    let text_items = content
                        .as_array()?
                        .iter()
                        .filter(|item| !is_image_content_item(item));
                    let output = text_items
                        .filter_map(|item| {
                            if item.get("type").and_then(Value::as_str) == Some("text") {
                                item.get("text").and_then(Value::as_str).map(str::to_owned)
                            } else {
                                format_activity_json(item)
                            }
                        })
                        .collect::<Vec<_>>()
                        .join("\n\n");
                    non_empty_activity_text(output)
                })
        }
        Some("commandExecution") => {
            let output = item
                .get("aggregatedOutput")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let exit = item.get("exitCode").and_then(Value::as_i64);
            let text = match (output.trim().is_empty(), exit) {
                (false, Some(exit)) => tr!(
                    "activity.output_with_exit_code",
                    output = output.trim_end(),
                    code = exit
                ),
                (false, None) => output.trim_end().to_owned(),
                (true, Some(exit)) => tr!("activity.exit_code", code = exit),
                (true, None) => return None,
            };
            non_empty_activity_text(text)
        }
        _ => None,
    }
}

fn codex_item_image_urls(item: &Value) -> Vec<String> {
    let content = match item.get("type").and_then(Value::as_str) {
        Some("mcpToolCall") => item.pointer("/result/content"),
        _ => None,
    };
    content
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(image_content_url)
        .collect()
}

fn image_content_url(item: &Value) -> Option<String> {
    let item_type = item.get("type").and_then(Value::as_str);
    if !matches!(item_type, Some("inputImage" | "image")) {
        return None;
    }
    item.get("imageUrl")
        .or_else(|| item.get("image_url"))
        .or_else(|| item.get("url"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            let data = item.get("data").and_then(Value::as_str)?;
            let mime_type = item
                .get("mimeType")
                .or_else(|| item.get("mime_type"))
                .and_then(Value::as_str)
                .unwrap_or("image/png");
            Some(format!("data:{mime_type};base64,{data}"))
        })
}

fn is_image_content_item(item: &Value) -> bool {
    matches!(
        item.get("type").and_then(Value::as_str),
        Some("inputImage" | "image")
    )
}

fn codex_item_failed(item: &Value) -> bool {
    item.get("status").and_then(Value::as_str) == Some("failed")
        || item.get("success").and_then(Value::as_bool) == Some(false)
        || item.get("error").is_some_and(|error| !error.is_null())
        || item
            .get("exitCode")
            .and_then(Value::as_i64)
            .is_some_and(|code| code != 0)
}

fn format_activity_json(value: &Value) -> Option<String> {
    serde_json::to_string_pretty(value)
        .ok()
        .and_then(non_empty_activity_text)
}

fn non_empty_activity_text(value: String) -> Option<String> {
    const MAX_CHARS: usize = 16_000;
    let value = value.trim().to_owned();
    if value.is_empty() {
        return None;
    }
    if value.chars().count() <= MAX_CHARS {
        return Some(value);
    }
    let mut truncated = value.chars().take(MAX_CHARS).collect::<String>();
    truncated.push_str("\n\n… output truncated");
    Some(truncated)
}

fn clean_stderr(line: &str) -> String {
    line.split_once(": ")
        .map(|(_, message)| message.to_owned())
        .unwrap_or_else(|| line.to_owned())
}

fn is_visible_stderr_notice(line: &str) -> bool {
    let lowercase = line.to_ascii_lowercase();
    if lowercase.contains("transport channel closed")
        || lowercase.contains("missing authorization header")
    {
        return false;
    }
    line.contains(" ERROR ")
        || lowercase.starts_with("error:")
        || line.contains('⚠')
        || lowercase.contains("fatal")
        || lowercase.contains("warning")
        || lowercase.contains("no such file or directory")
        || lowercase.contains("mcp startup incomplete")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn missing_launcher_dependencies_are_visible() {
        assert!(is_visible_stderr_notice(
            "env: node: No such file or directory"
        ));
    }

    #[test]
    fn startup_configuration_errors_are_visible() {
        assert!(is_visible_stderr_notice(
            "Error: error loading default config after config error: invalid transport"
        ));
    }

    #[test]
    fn computer_use_command_configuration_follows_the_setting() {
        let config = ComputerUse {
            server_path: PathBuf::from("/tmp/waku-computer-use-server"),
            server: toml_string("/tmp/waku-computer-use-server"),
            repl: toml_string("/tmp/waku"),
            skill_root: PathBuf::from("/tmp/waku-computer-use-skill"),
            process_directory: PathBuf::from("/tmp/waku-computer-use-processes-test"),
            process_directory_config: toml_string("/tmp/waku-computer-use-processes-test"),
        };
        let (mut args, mut env) = (Vec::new(), Vec::new());
        config.configure(&mut args, &mut env);
        // The raw helper must never be registered as a Codex MCP server: the
        // Waku REPL owns it and exposes only `cua` inside JavaScript.
        assert!(
            !args
                .iter()
                .any(|argument| argument.contains("mcp_servers.waku_computer_use"))
        );
        assert!(
            args.iter()
                .any(|argument| argument.contains("mcp_servers.waku_js_repl.command"))
        );
        assert!(
            args.iter()
                .any(|argument| argument == "mcp_servers.waku_js_repl.args=[]")
        );
        for setting in [
            DISABLE_EXTERNAL_COMPUTER_USE_PLUGIN,
            DISABLE_EXTERNAL_COMPUTER_USE_MCP,
            DISABLE_EXTERNAL_COMPUTER_USE_SKILL,
            DISABLE_CODEX_NODE_REPL,
            DISABLE_CODEX_NODE_REPL_COMMAND,
        ] {
            assert!(args.iter().any(|argument| argument == setting), "{setting}");
        }
        assert!(
            env.iter()
                .any(|(name, _)| name == "WAKU_COMPUTER_USE_SERVER")
        );
        std::mem::forget(config);
    }

    #[test]
    fn computer_use_process_registry_accepts_only_pid_files() {
        let directory = std::env::temp_dir().join(format!(
            "waku-computer-use-process-test-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(directory.join("456")).unwrap();
        fs::write(directory.join("123"), []).unwrap();
        fs::write(directory.join("not-a-pid"), []).unwrap();

        let processes = computer_use_runtime::registered_processes(&directory);
        assert_eq!(processes.len(), 1);
        assert_eq!(processes[0].0, 123);
        assert_eq!(processes[0].1, directory.join("123"));

        fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn computer_use_cleanup_verifies_the_registered_executable() {
        let current = fs::canonicalize(std::env::current_exe().unwrap()).unwrap();
        assert_eq!(
            computer_use_runtime::process_executable(std::process::id() as i32),
            Some(current)
        );
    }

    #[test]
    fn mcp_activity_keeps_native_server_and_tool_names() {
        let (events, received) = crate::driver::test_event_channel();
        let sidecar = Sidecar::new(events);
        for method in ["item/started", "item/completed"] {
            sidecar.observe(&json!({
                "method": method, "params": {
                    "threadId": "thread-1", "turnId": "turn-9", "item": {
                        "id": "cua-call", "type": "mcpToolCall", "server": "waku_js_repl", "tool": "js",
                        "arguments": {"title": "List running apps via CUA", "code": "cua.list_apps()"}
                    }
                }
            }));
            let complete = method == "item/completed";
            let Some(DriverEvent::RichActivity(item)) = sidecar.rewrite(DriverEvent::Activity {
                id: Some("cua-call".into()),
                kind: ActivityKind::Tool,
                title: String::new(),
                detail: None,
                complete,
            }) else {
                panic!("expected a tool activity");
            };
            assert_eq!(item.title, "List running apps via CUA");
            assert_eq!(item.tool_name.as_deref(), Some("js"));
            assert_eq!(item.mcp_server.as_deref(), Some("waku_js_repl"));
            assert_eq!(item.complete, complete);
        }
        // An item the old driver never presented stays hidden.
        assert!(
            sidecar
                .rewrite(DriverEvent::Activity {
                    id: Some("unknown".into()),
                    kind: ActivityKind::Tool,
                    title: String::new(),
                    detail: None,
                    complete: true,
                })
                .is_none()
        );
        assert!(received.try_recv().is_err());
    }

    #[test]
    fn command_items_and_output_become_background_work() {
        let (events, received) = crate::driver::test_event_channel();
        let sidecar = Sidecar::new(events);
        sidecar.observe(&json!({"method": "item/started", "params": {"item": {
            "type": "commandExecution", "id": "exec-1", "command": "ls", "status": "inProgress"
        }}}));
        sidecar.observe(
            &json!({"method": "item/commandExecution/outputDelta", "params": {
                "itemId": "exec-1", "delta": "README.md\n"
            }}),
        );
        assert!(matches!(
            received.try_recv().unwrap(),
            DriverEvent::BackgroundWork(BackgroundWorkEvent::Upsert(work)) if work.key.provider_id == "exec-1"
        ));
        assert!(matches!(
            received.try_recv().unwrap(),
            DriverEvent::BackgroundWork(BackgroundWorkEvent::OutputDelta { delta, .. }) if delta == "README.md\n"
        ));
    }

    #[test]
    fn background_terminal_snapshot_preserves_native_control_ids() {
        let item = codex_background_terminal(&json!({
            "itemId": "item-7",
            "processId": "process-9",
            "command": "bun run dev",
            "cwd": "/tmp/project",
            "osPid": 123,
            "cpuPercent": 1.5,
            "rssKb": 4096
        }))
        .unwrap();
        assert_eq!(item.key.provider_id, "item-7");
        assert_eq!(item.control_id.as_deref(), Some("process-9"));
        assert_eq!(item.status, BackgroundWorkStatus::Running);
        assert!(item.background && item.can_stop);
    }

    #[test]
    fn collab_agent_states_become_subagent_work() {
        let items = codex_subagent_work(&json!({
            "type": "collabAgentToolCall",
            "id": "collab-1",
            "tool": "spawnAgent",
            "prompt": "Inspect the parser\nand report back",
            "senderThreadId": "root-thread",
            "agentsStates": {
                "agent-thread": {"status": "running", "message": "Reading source"}
            }
        }));
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].key.kind, BackgroundWorkKind::Subagent);
        assert_eq!(items[0].key.provider_id, "agent-thread");
        assert_eq!(items[0].origin_activity_id.as_deref(), Some("collab-1"));
        assert_eq!(items[0].status, BackgroundWorkStatus::Running);
    }

    #[test]
    fn web_search_titles_never_end_with_an_empty_query() {
        let batch_open = json!({
            "type": "webSearch",
            "query": "",
            "action": { "type": "other" },
            "results": [{ "url": "https://openai.com" }, { "url": "https://deepseek.com" }]
        });
        let nested_query = json!({
            "type": "webSearch",
            "query": "",
            "action": { "type": "search", "queries": ["GPT-5.6 Luna official"] }
        });

        assert_eq!(codex_item_title(&batch_open), "Browsed 2 pages");
        assert_eq!(
            codex_item_title(&nested_query),
            "Search for GPT-5.6 Luna official"
        );
    }

    #[test]
    fn web_search_titles_describe_open_and_find_actions() {
        let open = json!({
            "type": "webSearch",
            "query": "",
            "action": { "type": "openPage", "url": "https://openai.com" }
        });
        let find = json!({
            "type": "webSearch",
            "query": "",
            "action": {
                "type": "findInPage",
                "pattern": "pricing",
                "url": "https://openai.com"
            }
        });

        assert_eq!(codex_item_title(&open), "Open https://openai.com");
        assert_eq!(
            codex_item_title(&find),
            "Find pricing in https://openai.com"
        );
    }

    #[test]
    fn mcp_tool_title_prefers_the_human_facing_argument() {
        let titled = json!({
            "type": "mcpToolCall",
            "server": "waku_js_repl",
            "tool": "js",
            "arguments": {
                "title": "Inspect Helium browser",
                "code": "cua.list_windows({})"
            }
        });
        let untitled = json!({
            "type": "mcpToolCall",
            "server": "waku_js_repl",
            "tool": "js",
            "arguments": { "code": "cua.list_apps()" }
        });

        assert_eq!(codex_item_title(&titled), "Inspect Helium browser");
        assert_eq!(codex_item_title(&untitled), "Js");
    }

    #[test]
    fn mcp_file_tools_use_the_shared_file_presentation_metadata() {
        let item = json!({
            "id": "read-1",
            "type": "mcpToolCall",
            "server": "filesystem",
            "tool": "read_file",
            "arguments": {"path": "/tmp/waku/src/model.rs"},
            "status": "inProgress"
        });

        let kind = codex_activity_kind(&item).expect("MCP call should be an activity");
        let activity = ActivityItem::new(
            Some("read-1".into()),
            kind,
            codex_item_title(&item),
            None,
            false,
        )
        .with_arguments(codex_item_arguments(&item))
        .with_activity_source(Some(&item));

        assert_eq!(activity.kind, ActivityKind::FileRead);
        assert_eq!(
            activity.display_target.as_deref(),
            Some("/tmp/waku/src/model.rs")
        );
    }

    #[test]
    fn command_actions_use_semantic_file_presentation() {
        for (action, expected_kind, expected_title, expected_target) in [
            (
                json!({
                    "type": "read",
                    "command": "sed -n '12,20p' crates/waku-core/src/driver/codex.rs",
                    "name": "codex.rs",
                    "path": "/tmp/waku/crates/waku-core/src/driver/codex.rs"
                }),
                ActivityKind::FileRead,
                tr!("activity.read_file"),
                "/tmp/waku/crates/waku-core/src/driver/codex.rs",
            ),
            (
                json!({
                    "type": "listFiles",
                    "command": "rg --files crates/waku-core/src",
                    "path": "crates/waku-core/src"
                }),
                ActivityKind::FileList,
                tr!("activity.list_files"),
                "crates/waku-core/src",
            ),
            (
                json!({
                    "type": "search",
                    "command": "rg commandActions crates/waku-core/src",
                    "query": "commandActions",
                    "path": "crates/waku-core/src"
                }),
                ActivityKind::FileSearch,
                tr!("activity.search_files"),
                "commandActions",
            ),
        ] {
            let item = json!({
                "id": "command-1",
                "type": "commandExecution",
                "command": action["command"],
                "cwd": "/tmp/waku",
                "commandActions": [action],
                "status": "completed"
            });

            let kind = codex_activity_kind(&item).expect("command should be an activity");
            let activity = ActivityItem::new(
                Some("command-1".into()),
                kind,
                codex_item_title(&item),
                None,
                true,
            )
            .with_arguments(codex_item_arguments(&item))
            .with_activity_source(Some(&item));

            assert_eq!(activity.kind, expected_kind);
            assert_eq!(activity.title, expected_title);
            assert_eq!(activity.display_target.as_deref(), Some(expected_target));
        }
    }

    #[test]
    fn unknown_or_compound_command_actions_keep_the_raw_command() {
        for command_actions in [
            json!([{"type": "unknown", "command": "sed -i '' file.txt"}]),
            json!([
                {
                    "type": "search",
                    "command": "rg needle src",
                    "query": "needle",
                    "path": "src"
                },
                {
                    "type": "read",
                    "command": "cat src/main.rs",
                    "name": "main.rs",
                    "path": "/tmp/waku/src/main.rs"
                }
            ]),
        ] {
            let item = json!({
                "type": "commandExecution",
                "command": "inspect files",
                "cwd": "/tmp/waku",
                "commandActions": command_actions
            });

            assert_eq!(codex_activity_kind(&item), Some(ActivityKind::Command));
            assert_eq!(codex_item_title(&item), "inspect files");
        }
    }

    #[test]
    fn file_change_arguments_preserve_provider_diff_metadata() {
        let item = json!({
            "id": "patch-1",
            "type": "fileChange",
            "changes": [{
                "path": "src/app.rs",
                "kind": {"type": "update"},
                "diff": "@@ -1 +1,2 @@\n-old\n+new\n+more"
            }],
            "status": "completed"
        });

        let arguments = codex_item_arguments(&item).expect("file changes should be retained");
        let activity = ActivityItem::new(
            Some("patch-1".into()),
            ActivityKind::FileChange,
            codex_item_title(&item),
            None,
            true,
        )
        .with_arguments(Some(arguments));

        assert_eq!(activity.file_changes.len(), 1);
        assert_eq!(activity.file_changes[0].path, "src/app.rs");
        assert_eq!(activity.file_changes[0].additions, Some(2));
        assert_eq!(activity.file_changes[0].deletions, Some(1));
        assert_eq!(codex_item_detail(&item, None), None);
    }

    #[test]
    fn mcp_image_content_is_removed_from_text_and_kept_as_an_image() {
        let item = json!({
            "type": "mcpToolCall",
            "result": {
                "content": [
                    {"type": "text", "text": "Screenshot captured."},
                    {"type": "image", "mimeType": "image/png", "data": "aGVsbG8="}
                ]
            }
        });

        assert_eq!(
            codex_item_output(&item).as_deref(),
            Some("Screenshot captured.")
        );
        assert_eq!(
            codex_item_image_urls(&item),
            vec!["data:image/png;base64,aGVsbG8=".to_owned()]
        );
    }
}
