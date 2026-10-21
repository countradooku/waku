//! Pi RPC frames, launch arguments and the flavor differences between Pi
//! and Oh My Pi (RFC 0002 "Pi and Oh My Pi", #50).
//!
//! Oh My Pi is a fork of Pi that kept the newline-delimited RPC transport but
//! renamed part of the surface: forking is `branch`, a run settles on
//! `agent_end` instead of `agent_settled`, titles arrive on
//! `session_info_update`, oversized frames are chunked once protocol v2 is
//! negotiated, and it has its own approval modes.

use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use waku_protocol::model::ProviderResumeCursor;
use waku_protocol::orchestration::{
    DriverKind, FailureClass, ProviderFailure, RuntimeMode, Timestamp,
};

use crate::driver::v2::InstanceConfig;

/// `ProviderKind::id()` of Pi.
pub const PI_PROVIDER: &str = "pi";
/// `ProviderKind::id()` of Oh My Pi.
pub const OHMYPI_PROVIDER: &str = "ohmypi";
/// The replay protocol name of both flavors.
pub const PI_PROTOCOL: &str = "pi.rpc-jsonl";

/// Option ids of an approval raised by Waku's Pi extension (a `confirm`
/// dialog has no options of its own).
pub const ALLOW: &str = "allow";
pub const DENY: &str = "deny";

/// Oh My Pi's own approval dialog: a `select` with exactly these options.
pub const OMP_APPROVE: &str = "Approve";
pub const OMP_DENY: &str = "Deny";
const OMP_APPROVAL_TITLE: &str = "Allow tool: ";

/// The environment variable Waku's Pi extension reads its policy from.
pub const RUNTIME_MODE_ENV: &str = "WAKU_PI_RUNTIME_MODE";
/// The extension's file name; the version changes with its source.
pub const APPROVAL_EXTENSION_FILE: &str = "waku-approvals-v1.ts";

/// Oh My Pi refuses to reassemble beyond this, so neither should Waku.
const MAX_REASSEMBLED_FRAME_BYTES: usize = 64 * 1024 * 1024;

/// Bit of Oh My Pi's `errorId` classifier marking a usage limit
/// (`pi-ai/src/error/flags.ts`, `Flag.UsageLimit`).
const USAGE_LIMIT_FLAG: u64 = 0x0008_0000;

/// Which dialect of the Pi RPC protocol a session speaks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PiFlavor {
    Pi,
    OhMyPi,
}

impl PiFlavor {
    pub fn from_instance(instance: &InstanceConfig) -> Self {
        match instance.provider {
            DriverKind::OhMyPi => Self::OhMyPi,
            _ => Self::Pi,
        }
    }

    pub fn driver_kind(self) -> DriverKind {
        match self {
            Self::Pi => DriverKind::Pi,
            Self::OhMyPi => DriverKind::OhMyPi,
        }
    }

    pub fn display_name(self) -> &'static str {
        match self {
            Self::Pi => "Pi",
            Self::OhMyPi => "Oh My Pi",
        }
    }

    pub fn provider_id(self) -> &'static str {
        match self {
            Self::Pi => PI_PROVIDER,
            Self::OhMyPi => OHMYPI_PROVIDER,
        }
    }

    /// The event that means the run is over and nothing more is scheduled.
    pub fn settled_event(self) -> &'static str {
        match self {
            Self::Pi => "agent_settled",
            Self::OhMyPi => "agent_end",
        }
    }

    pub fn session_info_event(self) -> &'static str {
        match self {
            Self::Pi => "session_info_changed",
            Self::OhMyPi => "session_info_update",
        }
    }

    pub fn session_info_title_field(self) -> &'static str {
        match self {
            Self::Pi => "name",
            Self::OhMyPi => "title",
        }
    }

    pub fn branch_messages_command(self) -> &'static str {
        match self {
            Self::Pi => "get_fork_messages",
            Self::OhMyPi => "get_branch_messages",
        }
    }

    pub fn branch_command(self) -> &'static str {
        match self {
            Self::Pi => "fork",
            Self::OhMyPi => "branch",
        }
    }

    /// Only Oh My Pi chunks oversized frames, and only after it is asked to.
    pub fn negotiates_protocol_v2(self) -> bool {
        matches!(self, Self::OhMyPi)
    }

    pub fn cursor(self, session_id: &str, session_file: Option<PathBuf>) -> ProviderResumeCursor {
        match self {
            Self::Pi => ProviderResumeCursor::Pi {
                session_id: session_id.to_owned(),
                session_file,
            },
            Self::OhMyPi => ProviderResumeCursor::OhMyPi {
                session_id: session_id.to_owned(),
                session_file,
            },
        }
    }

    /// The session file a cursor of this flavor names.
    pub fn session_file(self, cursor: Option<&ProviderResumeCursor>) -> Option<PathBuf> {
        match (self, cursor?) {
            (Self::Pi, ProviderResumeCursor::Pi { session_file, .. })
            | (Self::OhMyPi, ProviderResumeCursor::OhMyPi { session_file, .. }) => {
                session_file.clone()
            }
            _ => None,
        }
    }
}

/// How a runtime mode reaches the agent. Pi has no permission system, so
/// Waku's extension hooks `tool_call` and asks through a `confirm` dialog;
/// Oh My Pi has its own approval modes and asks through a `select`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Supervision {
    FullAccess,
    AutoAcceptEdits,
    Ask,
}

impl Supervision {
    /// `Auto` is not a mode either flavor enforces; it runs supervised.
    pub fn of(mode: RuntimeMode) -> Self {
        match mode {
            RuntimeMode::FullAccess => Self::FullAccess,
            RuntimeMode::AutoAcceptEdits => Self::AutoAcceptEdits,
            _ => Self::Ask,
        }
    }

    fn env_value(self) -> &'static str {
        match self {
            Self::FullAccess => "full_access",
            Self::AutoAcceptEdits => "auto_accept_edits",
            Self::Ask => "ask",
        }
    }
}

/// The launch arguments and environment of the session's agent.
pub fn launch(
    flavor: PiFlavor,
    supervision: Supervision,
    extension: Option<&Path>,
    instance: &InstanceConfig,
) -> (Vec<String>, Vec<(String, String)>) {
    let mut args = vec!["--mode".to_owned(), "rpc".to_owned()];
    let mut env = instance.env.clone();
    match flavor {
        PiFlavor::Pi => {
            // Pi has no permission system; `--approve` trusts project files.
            args.push("--approve".to_owned());
            if supervision != Supervision::FullAccess
                && let Some(extension) = extension
            {
                args.push("--extension".to_owned());
                args.push(extension.display().to_string());
                env.push((
                    RUNTIME_MODE_ENV.to_owned(),
                    supervision.env_value().to_owned(),
                ));
            }
            env.push(("PI_SKIP_VERSION_CHECK".to_owned(), "1".to_owned()));
        }
        PiFlavor::OhMyPi => match supervision {
            Supervision::FullAccess => args.push("--yolo".to_owned()),
            Supervision::AutoAcceptEdits => {
                args.extend(["--approval-mode".to_owned(), "write".to_owned()]);
            }
            Supervision::Ask => {
                args.extend(["--approval-mode".to_owned(), "always-ask".to_owned()]);
            }
        },
    }
    if let Some(home) = &instance.provider_home
        && !env.iter().any(|(key, _)| key == "PI_CODING_AGENT_DIR")
    {
        env.push(("PI_CODING_AGENT_DIR".to_owned(), home.display().to_string()));
    }
    (args, env)
}

/// A throwaway agent that copies `session_file` (`--fork`) in `cwd`: the
/// only full-session copy either flavor offers, and it sets the copy's
/// working directory.
pub fn fork_launch(
    flavor: PiFlavor,
    session_file: &Path,
    instance: &InstanceConfig,
) -> (Vec<String>, Vec<(String, String)>) {
    let (mut args, env) = launch(flavor, Supervision::FullAccess, None, instance);
    args.push("--fork".to_owned());
    args.push(session_file.display().to_string());
    (args, env)
}

/// The TypeScript source of Waku's Pi approval extension. Pi leaves
/// permission policy to extensions: this one blocks a tool call the user
/// declines through Pi's public `tool_call` hook, the semantics of T3's
/// injected bridge (`provider-pi/src/server/mcpExtensionSource.ts`).
pub const APPROVAL_EXTENSION_SOURCE: &str = r#"// Waku approvals for Pi (RFC 0002, #50). Loaded only in supervised modes.
const MODE = process.env.WAKU_PI_RUNTIME_MODE ?? "full_access";
const READ_ONLY = new Set(["read", "grep", "find", "ls"]);
const FILE_CHANGES = new Set(["edit", "write"]);

function summary(input: unknown): string {
  try {
    return JSON.stringify(input, null, 2).slice(0, 4000);
  } catch {
    return String(input).slice(0, 4000);
  }
}

export default function (pi: any) {
  pi.on("tool_call", async (event: any, ctx: any) => {
    if (MODE === "full_access") return;
    if (READ_ONLY.has(event.toolName)) return;
    if (MODE === "auto_accept_edits" && FILE_CHANGES.has(event.toolName)) return;
    const approved = await ctx.ui.confirm(`Allow ${event.toolName}?`, summary(event.input), {
      signal: ctx.signal,
    });
    if (!approved) {
      return { block: true, reason: `${event.toolName} was declined in Waku.` };
    }
  });
}
"#;

/// Where the extension is written: `extra.extensionDir` when set (tests,
/// the recorder), else the fork's own home.
pub fn extension_dir(instance: &InstanceConfig) -> PathBuf {
    instance
        .extra
        .get("extensionDir")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .unwrap_or_else(|| waku_protocol::identity::home_directory().join("pi"))
}

/// Writes the extension when it is missing or stale. Blocking.
pub fn materialize_extension(dir: &Path) -> std::io::Result<PathBuf> {
    let path = dir.join(APPROVAL_EXTENSION_FILE);
    if std::fs::read_to_string(&path).ok().as_deref() == Some(APPROVAL_EXTENSION_SOURCE) {
        return Ok(path);
    }
    std::fs::create_dir_all(dir)?;
    let staging = dir.join(format!(".{APPROVAL_EXTENSION_FILE}.tmp"));
    std::fs::write(&staging, APPROVAL_EXTENSION_SOURCE)?;
    std::fs::rename(&staging, &path)?;
    Ok(path)
}

/// `provider/model` → (`provider`, `modelId`).
pub fn parse_model_slug(model: &str) -> Option<(&str, &str)> {
    let (provider, model_id) = model.trim().split_once('/')?;
    (!provider.is_empty() && !model_id.is_empty()).then_some((provider, model_id))
}

pub fn set_model(model: &str) -> Option<Value> {
    let (provider, model_id) = parse_model_slug(model)?;
    Some(json!({"type": "set_model", "provider": provider, "modelId": model_id}))
}

/// The native session a `get_state` response reports.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SessionState {
    pub session_id: String,
    pub session_file: Option<PathBuf>,
    pub context_window: Option<u64>,
    pub title: Option<String>,
}

pub fn session_state(response: &Value) -> Option<SessionState> {
    let data = response.get("data")?;
    let session_id = data
        .get("sessionId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())?;
    Some(SessionState {
        session_id: session_id.to_owned(),
        session_file: data
            .get("sessionFile")
            .and_then(Value::as_str)
            .map(PathBuf::from),
        context_window: data
            .pointer("/model/contextWindow")
            .and_then(Value::as_u64)
            .filter(|window| *window > 0),
        title: data
            .get("sessionName")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|title| !title.is_empty())
            .map(str::to_owned),
    })
}

/// The fork or branch request that keeps all but the last
/// `turns_to_remove` user entries of `messages` (Pi's fork-message list:
/// one entry per user message of the active branch).
pub fn branch_request(
    flavor: PiFlavor,
    messages: &[Value],
    turns_to_remove: usize,
) -> Result<Option<Value>, String> {
    if turns_to_remove == 0 {
        return Ok(None);
    }
    if turns_to_remove > messages.len() {
        return Err(format!(
            "{} has only {} native turns, but {turns_to_remove} must be removed",
            flavor.display_name(),
            messages.len()
        ));
    }
    let entry_id = messages
        .get(messages.len() - turns_to_remove)
        .and_then(|message| message.get("entryId"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            format!(
                "{} returned a fork message without an entry id",
                flavor.display_name()
            )
        })?;
    Ok(Some(
        json!({"type": flavor.branch_command(), "entryId": entry_id}),
    ))
}

/// Pi already computes context occupancy for its own footer. Pi's providers
/// normally fill `totalTokens`, but Pi falls back to the four component
/// counters when a provider leaves it zero.
pub fn message_context_tokens(message: &Value) -> Option<u64> {
    let usage = message.get("usage")?;
    usage
        .get("totalTokens")
        .and_then(Value::as_u64)
        .filter(|tokens| *tokens > 0)
        .or_else(|| {
            let total = ["input", "output", "cacheRead", "cacheWrite"]
                .into_iter()
                .filter_map(|field| usage.get(field).and_then(Value::as_u64))
                .fold(0_u64, u64::saturating_add);
            (total > 0).then_some(total)
        })
}

/// Whether a failed assistant message or retry notice is a usage limit:
/// Oh My Pi's `errorId` flag, or HTTP 429 in `errorStatus`. Never the text.
pub fn is_usage_limit(value: &Value) -> bool {
    value
        .get("errorId")
        .and_then(Value::as_u64)
        .is_some_and(|id| id & USAGE_LIMIT_FLAG != 0)
        || value.get("errorStatus").and_then(Value::as_u64) == Some(429)
}

/// The failure an errored assistant message (or a final retry notice)
/// reports. `reset_at` is the end of the provider-stated wait the agent's
/// own retry announced (`auto_retry_start.delayMs`), when there was one.
pub fn failure(
    flavor: PiFlavor,
    message: Option<&str>,
    usage_limit: bool,
    reset_at: Option<Timestamp>,
) -> ProviderFailure {
    let text = message
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| format!("{} reported an error.", flavor.display_name()));
    if usage_limit {
        let mut failure =
            ProviderFailure::new(FailureClass::UsageLimit, text).with_code("usage_limit");
        failure.retryable = Some(true);
        failure.reset_at = reset_at;
        failure
    } else {
        ProviderFailure::new(FailureClass::Unknown, text).with_code("provider_error")
    }
}

/// An approval dialog: Waku's extension's `confirm`, or Oh My Pi's own
/// `select` between `Approve` and `Deny`. Returns the tool name.
pub fn approval_tool(flavor: PiFlavor, request: &Value) -> Option<String> {
    let method = request.get("method").and_then(Value::as_str)?;
    let title = request.get("title").and_then(Value::as_str).unwrap_or("");
    match method {
        "confirm" => Some(
            title
                .strip_prefix("Allow ")
                .and_then(|rest| rest.strip_suffix('?'))
                .filter(|tool| !tool.is_empty() && !tool.contains(char::is_whitespace))
                .unwrap_or("tool")
                .to_owned(),
        ),
        "select" if flavor == PiFlavor::OhMyPi => {
            let options = request.get("options").and_then(Value::as_array)?;
            let labels = options.iter().filter_map(Value::as_str).collect::<Vec<_>>();
            let tool = title.strip_prefix(OMP_APPROVAL_TITLE)?;
            (labels == [OMP_APPROVE, OMP_DENY])
                .then(|| tool.lines().next().unwrap_or("tool").trim().to_owned())
        }
        _ => None,
    }
}

/// Tools whose approval is a file change.
pub fn is_file_change_tool(tool: &str) -> bool {
    matches!(tool, "edit" | "write" | "ast_edit" | "multi_edit")
}

/// Reassembles the `rpc_chunk` runs Oh My Pi emits for frames over its 1 MiB
/// stdout ceiling. Without this a large tool result degrades to an error
/// frame and the row renders empty.
#[derive(Default)]
pub struct ChunkAssembly {
    active: Option<PendingChunks>,
}

struct PendingChunks {
    chunk_id: String,
    count: u64,
    next_index: u64,
    byte_length: usize,
    data: Vec<u8>,
}

impl ChunkAssembly {
    /// Returns the logical message to dispatch, or `None` while a chunked
    /// frame is still arriving.
    pub fn accept(&mut self, value: Value) -> Result<Option<Value>, String> {
        if value.get("type").and_then(Value::as_str) != Some("rpc_chunk") {
            // The run must be uninterrupted, so anything else invalidates a
            // partial frame rather than silently splicing around it.
            if self.active.take().is_some() {
                return Err("chunked frame was interrupted".to_owned());
            }
            return Ok(Some(value));
        }
        let (chunk_id, index, count, byte_length, data) = (|| {
            Some((
                value.get("chunkId").and_then(Value::as_str)?,
                value.get("index").and_then(Value::as_u64)?,
                value.get("count").and_then(Value::as_u64)?,
                value.get("byteLength").and_then(Value::as_u64)?,
                value.get("data").and_then(Value::as_str)?,
            ))
        })()
        .ok_or_else(|| "chunk frame was malformed".to_owned())?;
        let byte_length = usize::try_from(byte_length)
            .map_err(|_| "chunked frame exceeds the reassembly limit".to_owned())?;
        if count == 0 || index >= count {
            self.active = None;
            return Err("chunk frame was malformed".to_owned());
        }
        if byte_length > MAX_REASSEMBLED_FRAME_BYTES {
            self.active = None;
            return Err("chunked frame exceeds the reassembly limit".to_owned());
        }
        let decoded = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, data)
            .map_err(|error| format!("chunk payload was not valid base64: {error}"))?;
        let mut pending = match self.active.take() {
            Some(pending)
                if pending.chunk_id == chunk_id
                    && pending.count == count
                    && pending.byte_length == byte_length
                    && pending.next_index == index =>
            {
                pending
            }
            Some(_) => return Err("chunked frame was interrupted".to_owned()),
            None if index == 0 => PendingChunks {
                chunk_id: chunk_id.to_owned(),
                count,
                next_index: 0,
                byte_length,
                data: Vec::with_capacity(byte_length),
            },
            None => return Err("chunked frame started mid-sequence".to_owned()),
        };
        pending.data.extend_from_slice(&decoded);
        pending.next_index += 1;
        if pending.data.len() > pending.byte_length {
            return Err("chunked frame overran its declared length".to_owned());
        }
        if pending.next_index < pending.count {
            self.active = Some(pending);
            return Ok(None);
        }
        if pending.data.len() != pending.byte_length {
            return Err("chunked frame did not match its declared length".to_owned());
        }
        let text = String::from_utf8(pending.data)
            .map_err(|_| "chunked frame was not valid UTF-8".to_owned())?;
        serde_json::from_str(&text)
            .map(Some)
            .map_err(|error| format!("chunked frame was not valid JSON: {error}"))
    }
}

/// The text of a tool result (`{content: [{type: "text", text}]}`).
pub fn tool_result_text(result: Option<&Value>) -> Option<String> {
    let result = result?;
    let text = match result.get("content").unwrap_or(result) {
        Value::String(text) => text.clone(),
        Value::Array(blocks) => blocks
            .iter()
            .filter_map(|block| {
                (block.get("type").and_then(Value::as_str) == Some("text"))
                    .then(|| block.get("text").and_then(Value::as_str))
                    .flatten()
            })
            .collect::<Vec<_>>()
            .join("\n"),
        Value::Null => return None,
        other => other.to_string(),
    };
    (!text.is_empty()).then_some(text)
}
