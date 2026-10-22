//! Cold-path OpenCode helpers: the resume catalog, transcript import,
//! conversation forking, and the model/agent catalog.
//!
//! Everything here goes through [`opencode_service::attached`], never
//! [`opencode_service::shared`]. Opening the Resume picker or refreshing the
//! model catalog must not start the user's background daemon, so an absent
//! service is an empty catalog — not a spawn, and not a 20s start poll.
//!
//! Every call blocks on a socket, so every caller must already be off the UI
//! thread; one of these is several frames of budget.
//!
//! Three traps are encoded here rather than left to callers:
//!
//! * `GET …/export?sanitize=true` is a SHARE sanitizer, not a state stripper.
//!   Verified against a live service: it replaces the user's prompt, the
//!   assistant's prose, reasoning, tool input, tool output and tool metadata
//!   alike with `[redacted:…:msg_…]` placeholders, so importing a sanitized
//!   export yields a transcript of nothing but placeholders. Waku exports
//!   unsanitized and drops the private parts itself — see
//!   [`strip_provider_state`].
//! * One agent STEP is one assistant message. A three-tool turn is four
//!   assistant messages (three `finish: "tool-calls"` then one
//!   `finish: "stop"`), so the import folds a turn's assistant messages into
//!   a single visual reply.
//! * One user message is not one turn. A steer is stored as an ordinary
//!   `user` message inside the turn it steered, so turns are counted by
//!   [`TurnBoundaries`], never by user messages.

use std::path::{Path, PathBuf};

use anyhow::Context as _;

use crate::model::{
    ProviderAgentPreset, ProviderKind, ProviderModel, ProviderResumeCursor, ProviderSessionSummary,
};
use crate::opencode_api::{self, AgentInfo, AgentMode, MessageInfo, ModelInfo, Order, SessionInfo};
use crate::opencode_service;

/// Matches `acp_session`: a catalog that needs more than this many pages is
/// either enormous or looping, and both deserve the same bound.
const MAX_PAGES: usize = 20;
/// The service caps nothing, so the page size is Waku's choice. Large enough
/// that an ordinary catalog is one round trip.
const PAGE_SIZE: usize = 100;
/// `GET /api/session` lists subagent children too. The literal string `null`
/// is how the route asks for roots only; it is echoed back inside the opaque
/// cursor as `"parentID":"null"`, so it survives pagination.
const ROOT_SESSIONS_ONLY: &str = "null";
/// OpenCode's own default primary agent. A session created without one comes
/// back with `agent: "build"`, and the roster carries no `isDefault` marker.
#[allow(dead_code)]
const DEFAULT_AGENT: &str = "build";

/// Lists the adopted service's root sessions across every workspace, newest
/// first.
///
/// Returns an empty catalog when no service is registered or healthy. That is
/// the whole point of the attach-only rule: the Resume picker must never be
/// the thing that starts the user's daemon.
pub fn list_provider_sessions(
    binary: &Path,
    limit: usize,
) -> anyhow::Result<Vec<ProviderSessionSummary>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let Some(service) = opencode_service::attached(binary)? else {
        return Ok(Vec::new());
    };
    let endpoint = service.endpoint();
    let mut summaries = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..MAX_PAGES {
        let (sessions, next) = opencode_api::list_sessions(
            &endpoint,
            None,
            Some(ROOT_SESSIONS_ONLY),
            Order::Desc,
            PAGE_SIZE.min(limit.max(1)),
            cursor.as_deref(),
        )
        .context("OpenCode could not list its sessions")?;
        summaries.extend(sessions.iter().filter_map(session_summary));
        if summaries.len() >= limit {
            break;
        }
        // `cursor.next` is minted on every page including the last; the api
        // layer already collapses the empty page into an absent cursor.
        let Some(next) = next else { break };
        cursor = Some(next);
    }
    summaries.truncate(limit);
    Ok(summaries)
}

fn session_summary(session: &SessionInfo) -> Option<ProviderSessionSummary> {
    let session_id = session.id.trim();
    if session_id.is_empty() {
        return None;
    }
    let cwd = PathBuf::from(session.location.directory.trim());
    if !cwd.is_absolute() {
        return None;
    }
    let created_at = unix_seconds(session.time.created);
    Some(ProviderSessionSummary {
        cursor: ProviderResumeCursor::OpenCode {
            session_id: session_id.to_owned(),
            // Kept verbatim: the list filter and `location.directory` on
            // create are compared by exact string equality, so a resume must
            // reuse the very string the service stored.
            directory: Some(session.location.directory.clone()),
        },
        title: crate::acp_session::session_title(
            ProviderKind::OpenCode,
            session.title.as_deref(),
            session_id,
        ),
        cwd,
        created_at,
        updated_at: unix_seconds(session.time.updated).max(created_at),
    })
}

/// OpenCode stamps time in Unix milliseconds as a JSON number; the catalog
/// sorts in seconds.
fn unix_seconds(millis: f64) -> u64 {
    if millis.is_finite() && millis > 0.0 {
        (millis / 1000.0) as u64
    } else {
        0
    }
}

/// Finds the user message that opens each native turn, fed every stored
/// message in conversation order.
///
/// Steering stores an ordinary `user` message in the running turn, and Waku
/// folds a steer into that turn rather than starting one, so counting user
/// messages forks at the wrong boundary once any turn was steered. The rule
/// is OpenCode's own (`session/diff.ts`): a turn opens with the first user
/// message after an `idle` marker and runs to the next one.
///
/// History migrated from OpenCode 1 carries no markers. There a message sent
/// mid-turn joined the running loop, which Waku's OpenCode 1 driver showed as
/// a steer too: the loop ran on past a `tool-calls` or `unknown` step unless
/// the step failed, and took in any message written before its latest step
/// completed. A message written later opened a turn, including one that
/// directly follows a prompt that failed before its first step. A migrated
/// step never records `time.streamed`, which a native step records once its
/// provider stream ends, so the fallback cannot split a native turn steered
/// before its first marker.
#[derive(Debug, Default)]
pub(crate) struct TurnBoundaries {
    /// A user message opened a turn no marker has closed yet.
    open: bool,
    /// A marker was seen, so everything after it is native history.
    marked: bool,
    /// The latest step, when it is migrated history.
    migrated_step: Option<MigratedStep>,
}

#[derive(Clone, Copy, Debug)]
struct MigratedStep {
    /// OpenCode 1 went on to run another step.
    handed_back: bool,
    completed: Option<f64>,
}

impl TurnBoundaries {
    /// Feeds the next message, returning its id when it opens a turn.
    pub(crate) fn observe<'a>(&mut self, message: &'a MessageInfo) -> Option<&'a str> {
        match message {
            MessageInfo::Idle { .. } => {
                self.open = false;
                self.marked = true;
                None
            }
            MessageInfo::Assistant {
                time,
                finish,
                error,
                ..
            } => {
                self.migrated_step =
                    (!self.marked && time.streamed.is_none()).then(|| MigratedStep {
                        handed_back: error.is_none()
                            && matches!(finish.as_deref(), Some("tool-calls" | "unknown")),
                        completed: time.completed,
                    });
                None
            }
            MessageInfo::User { id, time, .. } => {
                let migrated_turn_ended = self.migrated_step.is_some_and(|step| {
                    !step.handed_back
                        && step
                            .completed
                            .is_none_or(|completed| time.created >= completed)
                });
                let opens = !self.open || migrated_turn_ended;
                self.open = true;
                opens.then_some(id.as_str())
            }
            _ => None,
        }
    }
}

/// The service's model and primary-agent catalogs.
///
/// Attach-only, so a launch with no service running answers empties and
/// `model_catalog` falls back to its disk cache rather than starting the
/// user's daemon just to refresh a picker.
pub(crate) fn discover_catalog(
    binary: &Path,
) -> (Vec<ProviderModel>, Option<Vec<ProviderAgentPreset>>) {
    let Ok(Some(service)) = opencode_service::attached(binary) else {
        return (Vec::new(), None);
    };
    let endpoint = service.endpoint();
    // No directory: both catalogs are global, and scoping them would only pin
    // them to whichever workspace asked first.
    let models = opencode_api::list_models(&endpoint, None)
        .map(|models| catalog_models(&models))
        .unwrap_or_default();
    let presets = opencode_api::list_agents(&endpoint, None)
        .ok()
        .map(|agents| agent_presets(&agents));
    (models, presets)
}

fn catalog_models(models: &[ModelInfo]) -> Vec<ProviderModel> {
    models
        .iter()
        .filter(|model| model.enabled)
        .filter_map(|model| {
            let provider = model.provider_id.trim();
            let id = model.id.trim();
            if provider.is_empty() || id.is_empty() {
                return None;
            }
            let name = model.name.trim();
            let name = if name.is_empty() { id } else { name };
            let catalog = ProviderModel::new(format!("{provider}/{id}"), name)
                .sub_provider(crate::model_catalog::display_name_from_slug(provider));
            // A model's variants ARE its reasoning-effort ladder: the ids are
            // `low`/`medium`/`high`/`max`/`minimal`/`xhigh`/`none`/`thinking`,
            // and the chosen one rides on `ModelRef::variant`. Dropping them
            // left the effort control empty for every OpenCode model.
            Some(crate::model_catalog::with_variant_efforts(
                catalog,
                model.variants.iter().map(|variant| variant.id.as_str()),
            ))
        })
        .collect()
}

/// Only agents a session can actually be STARTED as.
///
/// `subagent` entries are dispatch targets rather than session compositions,
/// and the hidden primaries (`title`, `summary`, `compaction`) are the
/// service's own internal agents.
fn agent_presets(agents: &[AgentInfo]) -> Vec<ProviderAgentPreset> {
    agents
        .iter()
        .filter(|agent| matches!(agent.mode, AgentMode::Primary | AgentMode::All) && !agent.hidden)
        .filter_map(|agent| {
            let id = agent.id.trim();
            if id.is_empty() {
                return None;
            }
            let name = agent.name.trim();
            let mut preset = ProviderAgentPreset::new(id, if name.is_empty() { id } else { name });
            preset.is_default = id == DEFAULT_AGENT;
            preset.description = agent
                .description
                .as_deref()
                .map(str::trim)
                .filter(|description| !description.is_empty())
                .map(str::to_owned);
            Some(preset)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    fn sessions(value: Value) -> Vec<SessionInfo> {
        serde_json::from_value(value).expect("the session list should decode")
    }

    fn messages(value: Value) -> Vec<MessageInfo> {
        serde_json::from_value(value).expect("the message list should decode")
    }

    #[test]
    fn summaries_keep_the_stored_directory_and_convert_millis_to_seconds() {
        let project_dir = std::env::temp_dir().join("waku-opencode-project");
        let untitled_dir = std::env::temp_dir().join("waku-opencode-untitled");
        let listed = sessions(json!([
            {
                "id": "ses_waku",
                "projectID": "prj",
                "cost": 0,
                "tokens": {"input": 0, "output": 0, "reasoning": 0, "cache": {"read": 0, "write": 0}},
                "time": {"created": 1_788_689_510_000_u64, "updated": 1_788_689_514_982_u64},
                "title": "Review and merge Waku PR #113",
                "location": {"directory": project_dir}
            },
            {
                "id": " ses_untitled ",
                "projectID": "global",
                "cost": 0,
                "tokens": {"input": 0, "output": 0, "reasoning": 0, "cache": {"read": 0, "write": 0}},
                "time": {"created": 1_786_109_016_231_u64, "updated": 0},
                "location": {"directory": untitled_dir}
            },
            {
                "id": "ses_relative",
                "projectID": "prj",
                "cost": 0,
                "tokens": {"input": 0, "output": 0, "reasoning": 0, "cache": {"read": 0, "write": 0}},
                "time": {"created": 1_786_109_016_231_u64, "updated": 0},
                "location": {"directory": "relative/dir"}
            }
        ]));

        let summaries = listed
            .iter()
            .filter_map(session_summary)
            .collect::<Vec<_>>();

        assert_eq!(summaries.len(), 2, "{summaries:#?}");
        assert_eq!(
            summaries[0].cursor,
            ProviderResumeCursor::OpenCode {
                session_id: "ses_waku".into(),
                directory: Some(project_dir.to_string_lossy().into_owned()),
            }
        );
        assert_eq!(summaries[0].cwd, project_dir);
        assert_eq!(summaries[1].cwd, untitled_dir);
        assert_eq!(summaries[0].created_at, 1_788_689_510);
        assert_eq!(summaries[0].updated_at, 1_788_689_514);
        // A session the service never updated must not sort before its own
        // creation.
        assert_eq!(summaries[1].title, "OpenCode session ses_unti");
        assert_eq!(summaries[1].updated_at, summaries[1].created_at);
    }

    /// A prompt or steer, written at `created` milliseconds.
    fn prompt(id: &str, created: u64) -> Value {
        json!({"id": id, "type": "user", "time": {"created": created}, "text": id})
    }

    /// One agent step that ran over `ran` milliseconds. Only a native step
    /// records `streamed`; migrated OpenCode 1 history never does.
    fn step(id: &str, finish: &str, native: bool, ran: std::ops::Range<u64>) -> Value {
        let mut time = json!({"created": ran.start, "completed": ran.end});
        if native {
            time["streamed"] = json!(ran.end);
        }
        json!({
            "id": id,
            "type": "assistant",
            "time": time,
            "agent": "build",
            "model": {"id": "big-pickle", "providerID": "opencode"},
            "content": [],
            "finish": finish,
        })
    }

    fn idle(id: &str) -> Value {
        json!({"id": id, "type": "idle", "time": {"created": 1}, "outcome": "succeeded"})
    }

    fn turn_openers(transcript: Value) -> Vec<String> {
        let mut boundaries = TurnBoundaries::default();
        messages(transcript)
            .iter()
            .filter_map(|message| boundaries.observe(message))
            .map(str::to_owned)
            .collect()
    }

    /// The 2.0.22 shape of a steered turn, then a plain one: steers land
    /// after a `tool-calls` step or after the final `stop` step, and the turn
    /// still ends at one marker.
    #[test]
    fn steers_join_the_turn_their_idle_marker_closes() {
        let openers = turn_openers(json!([
            prompt("msg_first", 1),
            step("msg_a", "tool-calls", true, 2..10),
            prompt("msg_steer", 11),
            step("msg_b", "stop", true, 12..20),
            prompt("msg_late_steer", 21),
            step("msg_c", "stop", true, 22..30),
            idle("msg_idle_1"),
            prompt("msg_second", 40),
            step("msg_d", "stop", true, 41..50),
            idle("msg_idle_2"),
        ]));

        assert_eq!(openers, ["msg_first", "msg_second"]);
    }

    /// OpenCode 1 history has no markers; its own loop decided which messages
    /// joined a running turn. Messages sort by when they were written, so one
    /// written mid-step follows that step.
    #[test]
    fn migrated_history_opens_a_turn_per_finished_loop() {
        let openers = turn_openers(json!([
            prompt("msg_first", 1),
            step("msg_a", "tool-calls", false, 2..10),
            // The loop ran on past `tool-calls`.
            prompt("msg_steer", 11),
            step("msg_b", "stop", false, 12..20),
            // Written while the final step was still running.
            prompt("msg_late_steer", 15),
            step("msg_c", "stop", false, 21..30),
            prompt("msg_second", 31),
            {
                "id": "msg_d", "type": "assistant",
                "time": {"created": 32, "completed": 40},
                "agent": "build", "model": {"id": "big-pickle", "providerID": "opencode"},
                "content": [], "finish": "tool-calls",
                "error": {"type": "aborted", "message": "Aborted"},
            },
            // Written before the abort, so it joined the loop the abort ended.
            prompt("msg_aborted_steer", 35),
            // The abort ended that loop despite its `tool-calls`.
            prompt("msg_third", 41),
            // `msg_third` failed before its first step, so this is a retry.
            prompt("msg_retry", 50),
            step("msg_e", "stop", false, 51..60),
        ]));

        assert_eq!(
            openers,
            ["msg_first", "msg_second", "msg_third", "msg_retry"]
        );
    }

    /// A migrated session continued under 2.0: the fallback covers only the
    /// history before the first marker, and native steps never split a turn.
    #[test]
    fn migrated_history_hands_over_to_idle_markers() {
        let openers = turn_openers(json!([
            prompt("msg_v1", 1),
            step("msg_a", "stop", false, 2..10),
            prompt("msg_v2", 1_000),
            step("msg_b", "tool-calls", true, 1_001..1_010),
            prompt("msg_v2_steer", 1_011),
            step("msg_c", "stop", true, 1_012..1_020),
            idle("msg_idle_1"),
            prompt("msg_v2_again", 2_000),
            // Past the first marker even an unstreamed step cannot end a
            // turn; only the next marker does.
            step("msg_d", "stop", false, 2_001..2_010),
            prompt("msg_v2_again_steer", 3_000),
            idle("msg_idle_2"),
        ]));

        assert_eq!(openers, ["msg_v1", "msg_v2", "msg_v2_again"]);
    }

    #[test]
    fn bookkeeping_messages_never_open_turns() {
        let openers = turn_openers(json!([
            prompt("msg_user", 1),
            {"id": "msg_synth", "type": "synthetic", "time": {"created": 1.0}, "text": "continue"},
            {"id": "msg_sys", "type": "system", "time": {"created": 1.0}, "text": "prompt"},
            {"id": "msg_skill", "type": "skill", "time": {"created": 1.0}, "skill": "s", "name": "S", "text": "t"},
            {"id": "msg_shell", "type": "shell", "time": {"created": 1.0}, "shellID": "sh",
             "command": "ls", "status": "exited"},
            {"id": "msg_compact", "type": "compaction", "time": {"created": 1.0},
             "status": "completed", "reason": "auto"},
            {"id": "msg_agent", "type": "agent-switched", "time": {"created": 1.0}, "agent": "plan"},
            {"id": "msg_model", "type": "model-switched", "time": {"created": 1.0},
             "model": {"id": "m", "providerID": "p"}},
            {"id": "msg_loc", "type": "location-switched", "time": {"created": 1.0},
             "location": {"directory": "/tmp"}},
            step("msg_assist", "stop", true, 2..10),
            idle("msg_idle"),
            prompt("msg_user2", 20),
        ]));

        assert_eq!(openers, ["msg_user", "msg_user2"]);
    }

    #[test]
    fn catalog_models_are_addressed_by_provider_and_id() {
        let models: Vec<ModelInfo> = serde_json::from_value(json!([
            {
                "id": "gemini-3.8-flash",
                "modelID": "gemini-3.8-flash",
                "providerID": "github-copilot",
                "name": "Gemini 3.8 Flash",
                "status": "active",
                "enabled": true,
                "limit": {"context": 1_048_576, "output": 65_536}
            },
            {
                "id": "deepseek-v4-flash-vision-exp",
                "modelID": "deepseek-v4-flash-vision-exp",
                "providerID": "deepseek",
                "name": "DeepSeek V4 Flash Vision Exp",
                "status": "beta",
                "enabled": true,
                "limit": {"context": 128_000, "output": 8_000}
            },
            {
                "id": "retired",
                "modelID": "retired",
                "providerID": "opencode",
                "name": "Retired",
                "status": "active",
                "enabled": false,
                "limit": {"context": 0, "output": 0}
            }
        ]))
        .expect("the model catalogue should decode");

        let models = catalog_models(&models);

        assert_eq!(models.len(), 2, "{models:#?}");
        assert_eq!(models[0].id, "github-copilot/gemini-3.8-flash");
        assert_eq!(models[0].name, "Gemini 3.8 Flash");
        assert_eq!(models[0].sub_provider.as_deref(), Some("Github Copilot"));
        // A beta model is still selectable; only a disabled one is not.
        assert_eq!(models[1].id, "deepseek/deepseek-v4-flash-vision-exp");
    }

    #[test]
    fn agent_presets_keep_only_startable_primaries() {
        let agents: Vec<AgentInfo> = serde_json::from_value(json!([
            {"id": "build", "name": "Build", "mode": "primary", "hidden": false,
             "description": "The default agent."},
            {"id": "plan", "name": "Plan", "mode": "primary", "hidden": false},
            {"id": "general", "name": "General", "mode": "subagent", "hidden": false},
            {"id": "title", "name": "Title", "mode": "primary", "hidden": true},
            {"id": "review", "name": "Review", "mode": "all", "hidden": false}
        ]))
        .expect("the agent roster should decode");

        let presets = agent_presets(&agents);

        assert_eq!(
            presets
                .iter()
                .map(|preset| preset.id.as_str())
                .collect::<Vec<_>>(),
            vec!["build", "plan", "review"]
        );
        assert!(presets[0].is_default);
        assert_eq!(
            presets[0].description.as_deref(),
            Some("The default agent.")
        );
        assert!(!presets[1].is_default);
    }

    /// Exercises the real catalog against the user's own service. Read-only:
    /// it lists sessions and never creates, prompts or deletes.
    #[test]
    #[ignore = "requires a running OpenCode background service"]
    fn lists_real_root_sessions_across_workspaces() {
        let binary =
            crate::command_env::find_executable("opencode").expect("opencode is not installed");
        let sessions = list_provider_sessions(&binary, 50).expect("the catalog should load");
        assert!(sessions.iter().all(|session| session.cwd.is_absolute()));
        assert!(
            sessions
                .windows(2)
                .all(|pair| pair[0].updated_at >= pair[1].updated_at),
            "OpenCode lists newest first"
        );
    }

    /// The service is the only source of the catalog, and both halves have
    /// to arrive together or the picker degrades to the disk cache.
    #[test]
    #[ignore = "requires a running OpenCode background service"]
    fn discovers_the_real_model_and_agent_catalog() {
        let binary =
            crate::command_env::find_executable("opencode").expect("opencode is not installed");
        let (models, presets) = discover_catalog(&binary);
        assert!(!models.is_empty(), "the service should expose models");
        assert!(models.iter().all(|model| model.id.contains('/')));
        let presets = presets.expect("the service should expose agents");
        assert!(presets.iter().any(|preset| preset.id == DEFAULT_AGENT));
    }
}

#[cfg(test)]
mod discovery_smoke {
    /// Proves the picker is actually populated against a live service, which a
    /// unit test over canned JSON cannot.
    #[test]
    #[ignore = "requires a running OpenCode background service"]
    fn discovers_models_from_the_adopted_service() {
        let (models, presets) = super::discover_catalog(std::path::Path::new("opencode"));
        println!(
            "models={} presets={:?}",
            models.len(),
            presets.as_ref().map(Vec::len)
        );
        let with_efforts: Vec<_> = models
            .iter()
            .filter(|model| !model.reasoning_efforts.is_empty())
            .collect();
        println!("with efforts={}", with_efforts.len());
        for model in with_efforts.iter().take(4) {
            println!(
                "  {} -> {:?} (default {:?})",
                model.id,
                model
                    .reasoning_efforts
                    .iter()
                    .map(|effort| effort.id.as_str())
                    .collect::<Vec<_>>(),
                model.default_reasoning_effort
            );
        }
        assert!(!models.is_empty(), "expected a non-empty model catalog");
        assert!(
            !with_efforts.is_empty(),
            "expected some models to expose variants"
        );
    }
}
