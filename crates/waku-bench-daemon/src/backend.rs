//! The daemon backend: Waku's real backend with the provider runtime replaced
//! by timeline replay.
//!
//! Persistence, settings, drafts, blobs, hydration and search all go to the
//! real `WakuBackend` over a throwaway database, so the desktop sees exactly
//! the daemon it always talks to. Only the commands that would reach a
//! provider process — start, prompt, cancel, probes, usage — are answered
//! here.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::JoinHandle;

use anyhow::{Context as _, anyhow, bail};
use parking_lot::Mutex;
use uuid::Uuid;
use waku_core::daemon::WakuBackend;
use waku_core::model::{DriverEvent, ProviderKind, ProviderProbe, ProviderResumeCursor};
use waku_core::{Backend, Command, EventSink, Request, ResponsePayload};
use waku_protocol::{WorkspaceOperation, WorkspaceResult, decode_enum, event_to_wire};

use crate::replay::{self, Rate, ReplayStats};
use crate::timeline::Timeline;

/// Providers the bench reports as installed. Their composer, model picker
/// and session flow are what the benchmarks exercise; every other provider
/// reports "not installed" so the UI does not offer it.
pub const BENCH_PROVIDERS: [ProviderKind; 2] = [ProviderKind::Codex, ProviderKind::Claude];

pub struct BenchSettings {
    /// Every timeline a prompt may select by name.
    pub timelines: HashMap<String, Arc<Timeline>>,
    /// Replayed when neither the prompt nor the session names one.
    pub default_timeline: String,
    pub session_timelines: HashMap<Uuid, String>,
    pub rate: Rate,
    pub loops: u32,
    /// Stop serving after this many replays have finished.
    pub exit_after: Option<usize>,
    /// Reported as the provider binary location in probes.
    pub binary_dir: PathBuf,
}

struct Runtime {
    runtime_id: Uuid,
    cancel: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

pub struct BenchBackend {
    inner: WakuBackend,
    settings: BenchSettings,
    runtimes: Mutex<HashMap<Uuid, Runtime>>,
    /// `--autoplay`: told once when a client attaches to this session's
    /// running runtime, i.e. when the desktop starts mirroring it.
    attach_watch: Mutex<Option<(Uuid, crossbeam_channel::Sender<()>)>>,
    stats: Arc<Mutex<Vec<ReplayStats>>>,
    finished: Arc<AtomicUsize>,
    shutdown: waku_core::ShutdownSignal,
}

impl BenchBackend {
    pub fn new(
        inner: WakuBackend,
        settings: BenchSettings,
        shutdown: waku_core::ShutdownSignal,
    ) -> Self {
        Self {
            inner,
            settings,
            runtimes: Mutex::new(HashMap::new()),
            attach_watch: Mutex::new(None),
            stats: Arc::new(Mutex::new(Vec::new())),
            finished: Arc::new(AtomicUsize::new(0)),
            shutdown,
        }
    }

    /// A channel that fires once, the first time a client attaches to
    /// `session_id`'s running runtime. Replaces any earlier watch.
    pub fn watch_attach(&self, session_id: Uuid) -> crossbeam_channel::Receiver<()> {
        let (sender, receiver) = crossbeam_channel::bounded(1);
        *self.attach_watch.lock() = Some((session_id, sender));
        receiver
    }

    /// Statistics for every replay so far, in completion order.
    pub fn stats(&self) -> Vec<ReplayStats> {
        self.stats.lock().clone()
    }

    fn select_timeline(&self, session_id: Uuid, prompt: &str) -> anyhow::Result<Arc<Timeline>> {
        let name = Some(prompt.trim())
            .filter(|name| self.settings.timelines.contains_key(*name))
            .or_else(|| {
                self.settings
                    .session_timelines
                    .get(&session_id)
                    .map(String::as_str)
            })
            .unwrap_or(&self.settings.default_timeline);
        self.settings
            .timelines
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow!("timeline {name:?} is not loaded"))
    }

    fn stop_runtime(runtime: Runtime) {
        runtime.cancel.store(true, Ordering::Release);
        if let Some(worker) = runtime.worker {
            let _ = worker.join();
        }
    }

    fn start(&self, request: &Request, provider: &str, events: &EventSink) -> anyhow::Result<()> {
        let provider: ProviderKind = decode_enum(provider)?;
        let previous = self.runtimes.lock().remove(&request.session_id);
        if let Some(previous) = previous {
            Self::stop_runtime(previous);
        }
        self.runtimes.lock().insert(
            request.session_id,
            Runtime {
                runtime_id: request.runtime_id,
                cancel: Arc::new(AtomicBool::new(false)),
                worker: None,
            },
        );
        let native_id = format!("bench-{}", request.session_id);
        let provider_cursor = match provider {
            ProviderKind::Claude => ProviderResumeCursor::Claude {
                session_id: request.session_id.to_string(),
                resume_at: None,
            },
            _ => ProviderResumeCursor::Codex {
                thread_id: native_id,
            },
        };
        events.send(event_to_wire(DriverEvent::Connected {
            provider_cursor: Some(provider_cursor),
        })?)
    }

    fn prompt(
        &self,
        request: &Request,
        prompt: &str,
        turn_id: Option<Uuid>,
        message_id: Option<Uuid>,
        events: EventSink,
    ) -> anyhow::Result<()> {
        let mut runtimes = self.runtimes.lock();
        let runtime = runtimes
            .get_mut(&request.session_id)
            .filter(|runtime| runtime.runtime_id == request.runtime_id)
            .ok_or_else(|| anyhow!("daemon session {} is not running", request.session_id))?;
        // Published before the turn starts, exactly as the real daemon does,
        // so every attached client mirrors the user message.
        events.send(event_to_wire(DriverEvent::PromptSubmitted {
            message: prompt.to_owned(),
            turn_id: turn_id.unwrap_or_else(Uuid::new_v4),
            message_id: message_id.unwrap_or_else(Uuid::new_v4),
        })?)?;
        let timeline = self.select_timeline(request.session_id, prompt)?;
        let rate = self.settings.rate;
        let loops = self.settings.loops;
        // A new prompt gets a fresh cancel flag; one that arrives during a
        // replay waits for it, as a provider would queue it.
        runtime.cancel = Arc::new(AtomicBool::new(false));
        let cancel = runtime.cancel.clone();
        let previous = runtime.worker.take();
        let stats = self.stats.clone();
        let finished = self.finished.clone();
        let exit_after = self.settings.exit_after;
        let shutdown = self.shutdown.clone();
        let worker = std::thread::Builder::new()
            .name(format!("waku-bench-replay-{}", request.session_id))
            .spawn(move || {
                if let Some(previous) = previous {
                    let _ = previous.join();
                }
                let expanded = replay::expand(&timeline, loops);
                let replayed = replay::play(&timeline.name, &expanded, rate, &cancel, |event| {
                    events.send(event.to_wire())
                });
                if replayed.canceled
                    && let Ok(wire) = event_to_wire(DriverEvent::TurnFinished {
                        success: false,
                        summary: None,
                    })
                {
                    let _ = events.send(wire);
                }
                eprintln!(
                    "waku-bench-daemon: {} replay {} · {} events in {:.2}s ({:.0}/s, max lag {:.1} ms)",
                    replayed.timeline,
                    if replayed.canceled { "canceled" } else { "finished" },
                    replayed.events,
                    replayed.elapsed.as_secs_f64(),
                    replayed.events_per_second(),
                    replayed.max_lag.as_secs_f64() * 1_000.0,
                );
                stats.lock().push(replayed);
                let done = finished.fetch_add(1, Ordering::AcqRel) + 1;
                if exit_after.is_some_and(|limit| done >= limit) {
                    shutdown.trigger();
                }
            })
            .context("could not start replay thread")?;
        runtime.worker = Some(worker);
        Ok(())
    }

    fn probe(&self, provider: ProviderKind) -> ProviderProbe {
        let installed = BENCH_PROVIDERS.contains(&provider);
        ProviderProbe {
            provider,
            installed,
            path: installed.then(|| self.settings.binary_dir.join(provider.command())),
            models: waku_protocol::model_catalog::fallback_models(provider),
            agent_presets: waku_protocol::model_catalog::fallback_agent_presets(provider),
        }
    }
}

impl Backend for BenchBackend {
    fn handle(&self, request: Request, events: EventSink) -> anyhow::Result<ResponsePayload> {
        let session_id = request.session_id;
        match &request.command {
            Command::Start { options } => {
                self.start(&request, &options.provider, &events)?;
                Ok(ResponsePayload::Started {
                    supports_steer: false,
                })
            }
            Command::AttachSession => {
                let runtime_id = self
                    .runtimes
                    .lock()
                    .get(&session_id)
                    .map(|runtime| runtime.runtime_id);
                if runtime_id.is_some() {
                    let mut watch = self.attach_watch.lock();
                    if watch
                        .as_ref()
                        .is_some_and(|(watched, _)| *watched == session_id)
                        && let Some((_, attached)) = watch.take()
                    {
                        let _ = attached.try_send(());
                    }
                }
                Ok(ResponsePayload::SessionRuntime {
                    runtime_id,
                    supports_steer: false,
                })
            }
            Command::Prompt {
                prompt,
                turn_id,
                message_id,
            } => {
                self.prompt(&request, prompt, *turn_id, *message_id, events)?;
                Ok(ResponsePayload::Ack)
            }
            Command::Steer { prompt } => {
                events.send(event_to_wire(DriverEvent::SteerRejected {
                    message: prompt.clone(),
                    reason: "the bench daemon replays fixed timelines and cannot steer".into(),
                })?)?;
                Ok(ResponsePayload::Ack)
            }
            Command::Cancel => {
                if let Some(runtime) = self.runtimes.lock().get(&session_id) {
                    runtime.cancel.store(true, Ordering::Release);
                }
                Ok(ResponsePayload::Ack)
            }
            Command::CloseSession => {
                let removed = {
                    let mut runtimes = self.runtimes.lock();
                    runtimes
                        .get(&session_id)
                        .is_some_and(|runtime| runtime.runtime_id == request.runtime_id)
                        .then(|| runtimes.remove(&session_id))
                        .flatten()
                };
                if let Some(runtime) = removed {
                    Self::stop_runtime(runtime);
                }
                Ok(ResponsePayload::Ack)
            }
            Command::RemoveSession => {
                let removed = self.runtimes.lock().remove(&session_id);
                if let Some(runtime) = removed {
                    Self::stop_runtime(runtime);
                }
                self.inner.handle(request, events)
            }
            Command::ApplyOptions { .. } => Ok(ResponsePayload::OptionsApplied { applied: true }),
            Command::Rollback { .. } | Command::Fork { .. } => {
                bail!("the bench daemon does not keep provider conversations to rewind or fork")
            }
            Command::Respond { .. }
            | Command::RespondUserInput { .. }
            | Command::Goal { .. }
            | Command::RunComputerTool { .. }
            | Command::RejectComputerTool { .. }
            | Command::CancelComputerUse
            | Command::RefreshBackgroundWork
            | Command::StopBackgroundWork { .. } => Ok(ResponsePayload::Ack),
            Command::ProbeProvider {
                provider,
                probe_version,
                ..
            } => Ok(ResponsePayload::ProviderProbe {
                probe: self.probe(*provider),
                version: (*probe_version && BENCH_PROVIDERS.contains(provider))
                    .then(|| format!("{}-bench", env!("CARGO_PKG_VERSION"))),
            }),
            Command::FetchPlanUsage { .. } => Ok(ResponsePayload::PlanUsage { usage: None }),
            Command::ListProviderSessions { .. } => Ok(ResponsePayload::ProviderSessions {
                sessions: Vec::new(),
            }),
            Command::LoadProviderSession { .. } | Command::ForkProviderSession { .. } => {
                bail!("the bench daemon has no provider-native conversations")
            }
            Command::LoadUsageHistory { .. } => {
                bail!("usage history is not scanned by the bench daemon")
            }
            Command::ProbeComputerPermissions { .. } => Ok(ResponsePayload::ComputerPermissions {
                permissions: Default::default(),
            }),
            Command::LoadSkills { .. } => Ok(ResponsePayload::SkillsCatalog {
                catalog: Default::default(),
            }),
            Command::Workspace {
                operation:
                    WorkspaceOperation::DiscoverSlashCommands {
                        provider,
                        project_root,
                        ..
                    },
            } => {
                // The real lookup runs the provider CLI found on the login
                // PATH (`codex app-server` for its skill list), which would
                // start a real provider. Point it at the reported binary,
                // which never exists, so only the file-based layers load.
                let absent = self.settings.binary_dir.join(provider.command());
                Ok(ResponsePayload::Workspace {
                    result: WorkspaceResult::SlashCommands {
                        commands: waku_core::composer_complete::discover_slash_commands(
                            *provider,
                            project_root,
                            Some(&absent.to_string_lossy()),
                        ),
                    },
                })
            }
            _ => self.inner.handle(request, events),
        }
    }

    fn shutdown(&self) {
        let runtimes = std::mem::take(&mut *self.runtimes.lock());
        for runtime in runtimes.into_values() {
            Self::stop_runtime(runtime);
        }
        self.inner.shutdown();
    }
}
