//! The ACP family (Cursor, Grok, Kimi Code, Fx and any generic Agent Client
//! Protocol agent) as one `DriverV2` (RFC 0002, #48).
//!
//! ```text
//! AcpDriver::open_session ─ spawn (ctx.transports, channel "main") ─ initialize
//!   └─ AcpSession (session.rs): session/new | session/resume | session/load,
//!      session/prompt → TurnTerminal, session/cancel, request_permission,
//!      Cursor and Grok questions, Grok fork/rollback on channel "fork-1"
//! ```
//!
//! - One driver, flavor from `InstanceConfig::provider` (`flavor.rs`).
//! - Capabilities come from the flavor and the `initialize` response
//!   (`loadSession`); see `flavor::capabilities` and the snapshots in
//!   `snapshots/capabilities_<flavor>.json`.
//! - No native steering (D-7): the superseding second `session/prompt` the v1
//!   driver sent is gone; the orchestrator interrupts and restarts.
//! - JSON-RPC is spoken directly (`protocol.rs`) so replay matches request
//!   ids; the SDK's schema types still read typed payloads, and its
//!   connection still serves the session catalog (`catalog_agent`).
//! - Until the orchestrator cutover, [`start`] runs the driver behind
//!   `V1Bridge` for the desktop.

mod flavor;
mod grok;
mod protocol;
mod questions;
mod selection;
mod session;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol::{AcpAgent, AcpAgentConfig, LineDirection};
use anyhow::anyhow;
use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::{Value, json};
use waku_protocol::model::{ProviderKind, ProviderResumeCursor};
use waku_protocol::orchestration::Capabilities;

pub use self::flavor::{AgentInfo, Flavor};
pub use self::grok::fork_session_at_turn_blocking as grok_fork_session_at_turn;
use self::protocol::Connection;
use self::session::{AcpSession, Homes, Setup, Shared};
use crate::driver::v2::bridge::V1Bridge;
use crate::driver::v2::{
    ContextBlock, ContextKind, DriverError, DriverFamily, DriverResult, DriverV2, InstanceConfig,
    OpenedSession, ProcessSpec, ProcessTransportFactory, ProtocolReason, Selection, SessionContext,
    TransitionPlan, adapter_channel,
};
use crate::driver::{DriverEventSender, DriverStartOptions};

const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(60);

/// What the desktop's v1 start hands the driver (only behind `V1Bridge`).
struct LegacyStart {
    events: DriverEventSender,
    /// Create the ACP session at startup, as the v1 driver did, so commands
    /// and the resume cursor arrive before the first prompt.
    eager: bool,
    /// A Cursor branch's retained conversation, sent with the first prompt.
    fork_context: Option<String>,
}

/// The ACP family's `DriverV2`.
#[derive(Default)]
pub struct AcpDriver {
    legacy: Mutex<Option<LegacyStart>>,
}

impl AcpDriver {
    pub fn new() -> Self {
        Self::default()
    }

    fn legacy(start: LegacyStart) -> Self {
        Self {
            legacy: Mutex::new(Some(start)),
        }
    }
}

/// The instance-level capabilities of a flavor: the most any of its sessions
/// can negotiate.
pub fn instance_capabilities(cfg: &InstanceConfig) -> Capabilities {
    flavor::capabilities(
        Flavor::from_driver(cfg.provider),
        cfg,
        &AgentInfo::everything(),
    )
}

/// The capabilities a session negotiates from `initialize`.
pub fn negotiated_capabilities(cfg: &InstanceConfig, agent: &AgentInfo) -> Capabilities {
    flavor::capabilities(Flavor::from_driver(cfg.provider), cfg, agent)
}

fn kimi_home(provider_home: &Path, configured: bool) -> PathBuf {
    if configured {
        return provider_home.to_owned();
    }
    std::env::var_os("KIMI_CODE_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| provider_home.join(".kimi-code"))
}

fn initialize_params(flavor: Flavor) -> Value {
    let mut client_capabilities = json!({
        "fs": { "readTextFile": false, "writeTextFile": false },
        "terminal": false,
    });
    if flavor == Flavor::Cursor {
        // Cursor only exposes its parameterized model controls to clients
        // that opt in. Waku applies the returned config option ids rather
        // than assuming Cursor's private ids stay stable.
        client_capabilities["_meta"] = json!({ "parameterizedModelPicker": true });
    }
    json!({
        "protocolVersion": 1,
        "clientCapabilities": client_capabilities,
        "clientInfo": { "name": "waku", "version": env!("CARGO_PKG_VERSION") },
    })
}

#[async_trait]
impl DriverV2 for AcpDriver {
    fn kind(&self) -> DriverFamily {
        DriverFamily::Acp
    }

    fn capabilities(&self, cfg: &InstanceConfig) -> Capabilities {
        instance_capabilities(cfg)
    }

    fn plan_selection_transition(
        &self,
        session_caps: &Capabilities,
        from: &Selection,
        to: &Selection,
    ) -> TransitionPlan {
        // The flavor matters only for reasoning effort, which Grok takes at
        // launch; native fork is Grok's alone, so it names the flavor.
        let flavor = if session_caps.threads.fork {
            Flavor::Grok
        } else {
            Flavor::Generic
        };
        flavor::plan_selection_transition(flavor, session_caps, from, to)
    }

    async fn open_session(&self, ctx: SessionContext) -> DriverResult<OpenedSession> {
        let flavor = Flavor::from_driver(ctx.instance.provider);
        let legacy = self.legacy.lock().take();
        let computer_use = match (&legacy, flavor) {
            (Some(legacy), Flavor::Grok) if ctx.instance.computer_use_enabled => Some(
                super::support::HeadlessComputerUseRuntime::start(
                    ProviderKind::Grok,
                    legacy.events.clone(),
                )
                .map_err(|error| DriverError::Protocol {
                    reason: ProtocolReason::UnexpectedFrame,
                    detail: error.to_string(),
                })?,
            ),
            _ => None,
        };
        let (computer_args, computer_env) = super::support::grok_computer_use_launch_configuration(
            computer_use.as_ref().map(|runtime| &runtime.config),
        );
        let mut args = flavor.launch_args(
            ctx.selection.reasoning_effort.as_deref(),
            &ctx.instance.extra,
        );
        args.extend(computer_args);
        let mut env = flavor.launch_env();
        env.extend(computer_env);
        env.extend(ctx.instance.env.iter().cloned());
        let transport = ctx
            .transports
            .spawn_stdio(
                "main",
                ProcessSpec {
                    program: ctx.instance.binary.clone(),
                    args,
                    env,
                    cwd: ctx.cwd.clone(),
                },
            )
            .await?;
        let provider_home = ctx.transports.provider_home(&ctx.instance);
        let configured = ctx.instance.provider_home.is_some();
        let homes = Homes {
            grok: computer_use
                .as_ref()
                .and_then(super::support::HeadlessComputerUseRuntime::grok_home)
                .map(Path::to_owned)
                .unwrap_or_else(|| grok::grok_home(&provider_home, configured)),
            kimi: kimi_home(&provider_home, configured),
        };
        let connection = Connection::new("main", transport.stdin, ctx.raw_log.clone());
        let (events, receiver) = adapter_channel();
        let pending_context = legacy
            .as_ref()
            .and_then(|legacy| legacy.fork_context.clone())
            .map(|text| ContextBlock {
                key: "fork".to_owned(),
                kind: ContextKind::SyntheticUser,
                text,
            })
            .into_iter()
            .collect();
        let setup = Setup {
            flavor,
            provider_session_id: ctx.provider_session_id,
            thread_id: ctx.thread_id,
            instance_id: ctx.instance.instance_id.clone(),
            cwd: ctx.cwd.clone(),
            binary: ctx.instance.binary.clone(),
            homes,
            transports: ctx.transports.clone(),
            ids: ctx.ids.clone(),
            clock: ctx.clock.clone(),
            raw_log: ctx.raw_log.clone(),
            legacy: legacy.as_ref().map(|legacy| legacy.events.clone()),
            computer_use,
        };
        let shared = Arc::new(Shared::new(
            setup,
            connection,
            events,
            ctx.selection.clone(),
            pending_context,
        ));
        tokio::spawn(session::read_loop(
            shared.clone(),
            transport.stdout,
            transport.exit,
        ));
        tokio::spawn(session::stderr_loop(shared.clone(), transport.stderr));
        let initialized = tokio::time::timeout(
            INITIALIZE_TIMEOUT,
            shared
                .connection()
                .request("initialize", initialize_params(flavor)),
        )
        .await;
        let result = match initialized {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => {
                shared.connection().close().await;
                return Err(if error.is_closed() {
                    DriverError::SessionLost {
                        detail: format!(
                            "{}: the agent exited during initialize",
                            flavor.display_name()
                        ),
                    }
                } else {
                    DriverError::Provider(flavor::classify_rpc_failure(
                        flavor,
                        &error,
                        ctx.clock.now(),
                    ))
                });
            }
            Err(_) => {
                shared.connection().close().await;
                return Err(DriverError::Protocol {
                    reason: ProtocolReason::Timeout,
                    detail: format!("{} did not answer initialize", flavor.display_name()),
                });
            }
        };
        let agent = AgentInfo::from_initialize(&result);
        let capabilities = flavor::capabilities(flavor, &ctx.instance, &agent);
        shared.negotiated(agent, capabilities.clone());
        if legacy.as_ref().is_some_and(|legacy| legacy.eager)
            && let Err(error) = shared.eager_session().await
        {
            shared.connection().close().await;
            return Err(error);
        }
        Ok(OpenedSession {
            session: Arc::new(AcpSession { shared }),
            events: receiver,
            capabilities,
        })
    }
}

// -- the v1 desktop -----------------------------------------------------------------

/// The runtime ACP sessions run on until the daemon's tokio runtime (#18)
/// hosts drivers: the one shared by every bridged driver, whose threads
/// unblock SIGCHLD so agents can reap children.
fn runtime() -> &'static tokio::runtime::Runtime {
    crate::driver::bridge_runtime()
}

/// Starts an ACP session for the desktop: the `DriverV2` behind `V1Bridge`.
pub(crate) fn start(
    provider: ProviderKind,
    options: DriverStartOptions,
    events: DriverEventSender,
) -> anyhow::Result<V1Bridge> {
    if Flavor::from_provider(provider).is_none() {
        return Err(anyhow!(
            "{} does not speak the Agent Client Protocol",
            provider.display_name()
        ));
    }
    let (eager, fork_context) = match &options.provider_cursor {
        Some(cursor) if cursor.provider() != provider => {
            return Err(anyhow!(
                "cannot resume {} from a {} cursor",
                provider.display_name(),
                cursor.provider().display_name()
            ));
        }
        Some(ProviderResumeCursor::Cursor {
            session_id,
            fork_context,
        }) => (session_id.is_empty(), fork_context.clone()),
        Some(cursor) => (cursor.native_id().is_empty(), None),
        None => (true, None),
    };
    let driver = Arc::new(AcpDriver::legacy(LegacyStart {
        events: events.clone(),
        eager,
        fork_context,
    }));
    let instance = InstanceConfig::new(provider.id(), provider.into());
    V1Bridge::start(
        driver,
        instance,
        options,
        events,
        Arc::new(ProcessTransportFactory),
        runtime().handle().clone(),
    )
}

// -- session catalog (SDK connection) -----------------------------------------------

/// Builds a short-lived ACP process for session discovery or history replay.
///
/// Catalog work runs on the daemon request thread, never a render path. It
/// shares the production launch arguments so provider argv and environment
/// quirks cannot drift between a resumed task and the picker that found it.
pub(crate) fn catalog_agent(
    provider: ProviderKind,
    binary: &Path,
    cwd: &Path,
) -> anyhow::Result<AcpAgent> {
    let flavor = Flavor::from_provider(provider).ok_or_else(|| {
        anyhow!(
            "{} does not speak the Agent Client Protocol",
            provider.display_name()
        )
    })?;
    let binary = binary
        .to_str()
        .ok_or_else(|| anyhow!("the ACP executable path is not valid UTF-8"))?;
    let cwd = cwd
        .to_str()
        .ok_or_else(|| anyhow!("the ACP working directory is not valid UTF-8"))?;
    let mut environment = crate::command_env::shell_environment()
        .into_iter()
        .map(|(name, value)| {
            (
                name.to_string_lossy().into_owned(),
                value.to_string_lossy().into_owned(),
            )
        })
        .collect::<Vec<_>>();
    environment.extend(flavor.launch_env());
    // `AcpAgentConfig` carries only argv and environment. macOS `env -C`
    // supplies the cwd without a shell, preserving argument boundaries.
    let mut args = vec!["-C".to_owned(), cwd.to_owned(), binary.to_owned()];
    args.extend(flavor.launch_args(None, &Value::Null));
    let config = AcpAgentConfig::new("/usr/bin/env")
        .args(args)
        .envs(environment);
    Ok(AcpAgent::new(config).with_debug(|_line, _direction: LineDirection| {}))
}

#[cfg(test)]
mod tests;
