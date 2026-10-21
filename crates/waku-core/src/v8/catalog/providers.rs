//! `providers.*`, `usage.history`, `computerUse.permissions` and the
//! `providers` stream.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::broadcast;
use waku_protocol::v8::{
    AgentPreset, ComputerPermissions, ComputerUsePermissionsParams, CostQuality, EndReason, Error,
    ErrorCode, ErrorDetails, InstanceParams, ModelCatalogSource, ModelCost, ModelOption, PlanUsage,
    PlanWindow, PricingStatus, ProviderModel, ProviderModels, ProviderProbe, ProviderProbeResult,
    ProviderStatus, ProvidersItem, ProvidersModelsParams, ProvidersProbeParams, Scope,
    StreamCursor, TokenTotals, UsageDay, UsageHistory, UsageHistoryParams, UsageModelSlice,
    UsageMonth, UsageProject, UsageProviderPart, UsageProviderSlice, UsageWindow,
};

use super::{
    Domain, call, content_revision, display_path, provider_kind, seconds_to_ms, unexpected,
};
use crate::model::ProviderKind;
use crate::protocol::{Command, ResponsePayload};
use crate::settings::DaemonSettings;
use crate::v8::V8Context;
use crate::v8::connection::StreamSink;
use crate::v8::orchestration::projects;
use waku_protocol::orchestration::JsonText;
use waku_protocol::v8::{
    MAX_PROVIDER_SESSIONS, ProviderSessionSummary, ProviderSessionsListParams,
};

/// Provider statuses and plan usage for the `providers` stream. Statuses
/// change through settings and probes; nothing here runs on a timer.
pub(crate) struct ProvidersBus {
    state: Mutex<ProvidersState>,
    sender: broadcast::Sender<ProvidersEvent>,
}

#[derive(Default)]
struct ProvidersState {
    statuses: BTreeMap<String, ProviderStatus>,
    /// CLI versions seen by probes, by instance id.
    versions: HashMap<String, String>,
    loaded: bool,
}

#[derive(Clone)]
enum ProvidersEvent {
    Status {
        revision: u64,
        status: ProviderStatus,
    },
    PlanUsage {
        instance_id: String,
        usage: Option<PlanUsage>,
    },
}

impl Default for ProvidersBus {
    fn default() -> Self {
        Self {
            state: Mutex::new(ProvidersState::default()),
            sender: broadcast::channel(256).0,
        }
    }
}

fn revision_of(statuses: &BTreeMap<String, ProviderStatus>) -> u64 {
    content_revision(statuses)
}

impl ProvidersBus {
    fn publish(&self, status: ProviderStatus) {
        let mut state = self.state.lock();
        if state.statuses.get(&status.instance_id) == Some(&status) {
            return;
        }
        state
            .statuses
            .insert(status.instance_id.clone(), status.clone());
        let revision = revision_of(&state.statuses);
        drop(state);
        let _ = self
            .sender
            .send(ProvidersEvent::Status { revision, status });
    }

    fn publish_all(&self, statuses: Vec<ProviderStatus>) {
        {
            // The first load is everyone's snapshot, not a change.
            let mut state = self.state.lock();
            if !state.loaded {
                state.loaded = true;
                for status in statuses {
                    state.statuses.insert(status.instance_id.clone(), status);
                }
                return;
            }
        }
        for status in statuses {
            self.publish(status);
        }
    }

    fn snapshot(&self) -> Option<(u64, Vec<ProviderStatus>)> {
        let state = self.state.lock();
        state.loaded.then(|| {
            (
                revision_of(&state.statuses),
                state.statuses.values().cloned().collect(),
            )
        })
    }

    fn version(&self, instance_id: &str) -> Option<String> {
        self.state.lock().versions.get(instance_id).cloned()
    }

    fn remember_version(&self, instance_id: &str, version: &str) {
        self.state
            .lock()
            .versions
            .insert(instance_id.to_owned(), version.to_owned());
    }
}

pub(super) fn daemon_settings(context: &V8Context) -> Result<DaemonSettings, Error> {
    match call(context, Command::GetSettings, Domain::General)? {
        ResponsePayload::Settings { settings } => Ok(settings),
        other => Err(unexpected(&other)),
    }
}

/// The binary a provider instance runs: its override in settings, or the
/// command found on the daemon's `PATH`.
pub(super) fn provider_binary(context: &V8Context, kind: ProviderKind) -> Result<PathBuf, Error> {
    let settings = daemon_settings(context)?;
    let probe = crate::model::provider_probe(
        kind,
        settings
            .provider_binary_overrides
            .get(&kind)
            .map(String::as_str),
    );
    probe.path.ok_or_else(|| {
        Error::new(
            ErrorCode::Provider,
            format!(
                "{} is not installed on the daemon host",
                kind.display_name()
            ),
        )
        .with_details(ErrorDetails::Provider {
            class: waku_protocol::v8::error::ProviderFailureClass::ValidationError,
            retryable: false,
            reset_at: None,
            code: Some("notInstalled".into()),
        })
    })
}

fn supports_plan_usage(kind: ProviderKind) -> bool {
    matches!(
        kind,
        ProviderKind::Claude | ProviderKind::Codex | ProviderKind::OpenCode | ProviderKind::Grok
    )
}

fn status_of(
    kind: ProviderKind,
    settings: &DaemonSettings,
    probe: &crate::model::ProviderProbe,
    version: Option<String>,
) -> ProviderStatus {
    ProviderStatus {
        instance_id: kind.id().into(),
        kind: kind.id().into(),
        display_name: kind.display_name().into(),
        enabled: !settings.disabled_providers.contains(&kind),
        binary_override: settings.provider_binary_overrides.get(&kind).cloned(),
        installed: probe.installed,
        path: probe.path.as_deref().map(display_path),
        version,
        plan_usage: supports_plan_usage(kind),
    }
}

fn legacy_probe(
    context: &V8Context,
    kind: ProviderKind,
    settings: &DaemonSettings,
    discover_models: bool,
    probe_version: bool,
) -> Result<(crate::model::ProviderProbe, Option<String>), Error> {
    match call(
        context,
        Command::ProbeProvider {
            provider: kind,
            binary_override: settings.provider_binary_overrides.get(&kind).cloned(),
            discover_models,
            probe_version,
        },
        Domain::Provider,
    )? {
        ResponsePayload::ProviderProbe { probe, version } => Ok((probe, version)),
        other => Err(unexpected(&other)),
    }
}

/// Every provider's status from settings and the cached probe (a `PATH`
/// lookup, no provider process).
pub(super) fn list(context: &V8Context) -> Result<Vec<ProviderStatus>, Error> {
    let settings = daemon_settings(context)?;
    let bus = &context.catalog.providers;
    let statuses = ProviderKind::ALL
        .into_iter()
        .map(|kind| {
            let (probe, _) = legacy_probe(context, kind, &settings, false, false)?;
            Ok(status_of(kind, &settings, &probe, bus.version(kind.id())))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    bus.publish_all(statuses.clone());
    Ok(statuses)
}

/// Settings changed: re-derive statuses for `providers` subscribers.
pub(super) fn settings_changed(context: &V8Context) {
    if context.catalog.providers.sender.receiver_count() == 0
        && !context.catalog.providers.state.lock().loaded
    {
        return;
    }
    if let Err(error) = list(context) {
        eprintln!("waku-daemon could not refresh provider statuses: {error}");
    }
}

fn wire_option(option: crate::model::ProviderModelOption) -> ModelOption {
    ModelOption {
        id: option.id,
        label: option.label,
        description: option.description,
    }
}

fn wire_model(model: crate::model::ProviderModel) -> ProviderModel {
    ProviderModel {
        id: model.id,
        name: model.name,
        sub_provider: model.sub_provider,
        is_default: model.is_default,
        reasoning_efforts: model
            .reasoning_efforts
            .into_iter()
            .map(wire_option)
            .collect(),
        default_reasoning_effort: model.default_reasoning_effort,
        service_tiers: model.service_tiers.into_iter().map(wire_option).collect(),
        default_service_tier: model.default_service_tier,
        context_windows: model.context_windows.into_iter().map(wire_option).collect(),
        default_context_window: model.default_context_window,
    }
}

fn wire_preset(preset: crate::model::ProviderAgentPreset) -> AgentPreset {
    AgentPreset {
        id: preset.id,
        name: preset.name,
        description: preset.description,
        is_default: preset.is_default,
        is_custom: preset.is_custom,
    }
}

pub(super) fn probe(
    context: &V8Context,
    params: ProvidersProbeParams,
) -> Result<ProviderProbeResult, Error> {
    let kind = provider_kind(&params.instance_id)?;
    let settings = daemon_settings(context)?;
    let (probe, version) = legacy_probe(
        context,
        kind,
        &settings,
        params.discover_models,
        params.probe_version,
    )?;
    let bus = &context.catalog.providers;
    if let Some(version) = &version {
        bus.remember_version(kind.id(), version);
    }
    bus.publish(status_of(kind, &settings, &probe, bus.version(kind.id())));
    Ok(ProviderProbeResult {
        probe: ProviderProbe {
            instance_id: kind.id().into(),
            installed: probe.installed,
            path: probe.path.as_deref().map(display_path),
            models: probe.models.into_iter().map(wire_model).collect(),
            agent_presets: probe.agent_presets.into_iter().map(wire_preset).collect(),
        },
        version,
    })
}

pub(super) fn models(
    context: &V8Context,
    params: ProvidersModelsParams,
) -> Result<ProviderModels, Error> {
    let kind = provider_kind(&params.instance_id)?;
    if params.refresh && kind.supports_model_discovery() {
        let settings = daemon_settings(context)?;
        let (probe, _) = legacy_probe(context, kind, &settings, true, false)?;
        return Ok(ProviderModels {
            models: probe.models.into_iter().map(wire_model).collect(),
            agent_presets: probe.agent_presets.into_iter().map(wire_preset).collect(),
            source: ModelCatalogSource::Discovered,
        });
    }
    let (models, source) = match crate::model_catalog::cached_models(kind) {
        Some(models) if !models.is_empty() => (models, ModelCatalogSource::Cached),
        _ => (
            crate::model_catalog::fallback_models(kind),
            ModelCatalogSource::Fallback,
        ),
    };
    Ok(ProviderModels {
        models: models.into_iter().map(wire_model).collect(),
        agent_presets: crate::model_catalog::fallback_agent_presets(kind)
            .into_iter()
            .map(wire_preset)
            .collect(),
        source,
    })
}

pub(super) fn plan_usage(
    context: &V8Context,
    params: InstanceParams,
) -> Result<Option<PlanUsage>, Error> {
    let kind = provider_kind(&params.instance_id)?;
    if !supports_plan_usage(kind) {
        return Ok(None);
    }
    let settings = daemon_settings(context)?;
    let usage = match call(
        context,
        Command::FetchPlanUsage {
            provider: kind,
            binary_override: settings.provider_binary_overrides.get(&kind).cloned(),
            cli_version: context.catalog.providers.version(kind.id()),
        },
        Domain::Provider,
    )? {
        ResponsePayload::PlanUsage { usage } => usage.map(|usage| PlanUsage {
            plan_label: usage.plan_label,
            windows: usage
                .windows
                .into_iter()
                .map(|window| PlanWindow {
                    label: window.label,
                    percent: window.percent,
                    resets_at: window.resets_at.map(|seconds| seconds.saturating_mul(1000)),
                })
                .collect(),
        }),
        other => return Err(unexpected(&other)),
    };
    let _ = context
        .catalog
        .providers
        .sender
        .send(ProvidersEvent::PlanUsage {
            instance_id: kind.id().into(),
            usage: usage.clone(),
        });
    Ok(usage)
}

/// `providers`: a snapshot (skipped when `after` is current),
/// `synchronized`, then status and plan usage changes.
pub(super) async fn providers_stream(
    sink: StreamSink,
    context: Arc<V8Context>,
    after: Option<StreamCursor>,
) {
    let mut events = context.catalog.providers.sender.subscribe();
    let mut first = true;
    loop {
        let snapshot = match context.catalog.providers.snapshot() {
            Some(snapshot) => snapshot,
            None => {
                let task_context = context.clone();
                match tokio::task::spawn_blocking(move || list(&task_context)).await {
                    Ok(Ok(_)) => continue,
                    Ok(Err(error)) => {
                        sink.end(EndReason::Error { error });
                        return;
                    }
                    Err(_) => return,
                }
            }
        };
        let (revision, statuses) = snapshot;
        let cursor = StreamCursor::Revision { revision };
        if !(first && after.as_ref() == Some(&cursor)) {
            let item = ProvidersItem::Snapshot { revision, statuses };
            if sink.push(&item, Some(cursor.clone())).is_err() {
                return;
            }
        }
        first = false;
        let synchronized = ProvidersItem::Synchronized {
            cursor: cursor.clone(),
        };
        if sink.push(&synchronized, Some(cursor)).is_err() {
            return;
        }
        loop {
            let item = match events.recv().await {
                Ok(ProvidersEvent::Status { revision, status }) => (
                    ProvidersItem::StatusUpdated { revision, status },
                    Some(StreamCursor::Revision { revision }),
                ),
                Ok(ProvidersEvent::PlanUsage { instance_id, usage }) => {
                    (ProvidersItem::PlanUsageUpdated { instance_id, usage }, None)
                }
                // Fell behind: send a fresh snapshot.
                Err(broadcast::error::RecvError::Lagged(_)) => break,
                Err(broadcast::error::RecvError::Closed) => return,
            };
            if sink.push(&item.0, item.1).is_err() {
                return;
            }
        }
    }
}

pub(super) fn provider_sessions(
    context: &V8Context,
    params: ProviderSessionsListParams,
) -> Result<Vec<ProviderSessionSummary>, Error> {
    let kind = provider_kind(&params.instance_id)?;
    let limit = params
        .limit
        .unwrap_or(MAX_PROVIDER_SESSIONS)
        .min(MAX_PROVIDER_SESSIONS) as usize;
    let sessions = match call(
        context,
        Command::ListProviderSessions {
            provider: kind,
            limit,
        },
        Domain::Provider,
    )? {
        ResponsePayload::ProviderSessions { sessions } => sessions,
        other => return Err(unexpected(&other)),
    };
    sessions
        .into_iter()
        .map(|session| {
            Ok(ProviderSessionSummary {
                instance_id: kind.id().into(),
                native_id: session.cursor.native_id().to_owned(),
                resume_cursor: JsonText(
                    serde_json::to_string(&session.cursor).map_err(super::internal)?,
                ),
                title: session.title,
                cwd: display_path(&session.cwd),
                created_at: seconds_to_ms(session.created_at),
                updated_at: seconds_to_ms(session.updated_at),
            })
        })
        .collect()
}

fn legacy_window(window: UsageWindow) -> Result<crate::usage_history::UsageWindow, Error> {
    use crate::usage_history::UsageWindow as Legacy;
    Ok(match window {
        UsageWindow::TrailingDays { days } => Legacy::TrailingDays(days),
        UsageWindow::Months { months } => Legacy::Months(months),
        UsageWindow::ThisMonth => Legacy::ThisMonth,
        UsageWindow::LastMonth => Legacy::LastMonth,
        UsageWindow::Unknown => return Err(super::bad_request("window", "unknown usage window")),
    })
}

fn wire_window(window: crate::usage_history::UsageWindow) -> UsageWindow {
    use crate::usage_history::UsageWindow as Legacy;
    match window {
        Legacy::TrailingDays(days) => UsageWindow::TrailingDays { days },
        Legacy::Months(months) => UsageWindow::Months { months },
        Legacy::ThisMonth => UsageWindow::ThisMonth,
        Legacy::LastMonth => UsageWindow::LastMonth,
    }
}

fn usage_provider(provider: crate::usage_history::UsageProvider) -> String {
    match provider {
        crate::usage_history::UsageProvider::Claude => "claude".into(),
        crate::usage_history::UsageProvider::Codex => "codex".into(),
    }
}

fn by_provider(days: &[crate::usage_history::ProviderDay; 2]) -> Vec<UsageProviderPart> {
    crate::usage_history::UsageProvider::ALL
        .into_iter()
        .map(|provider| {
            let day = days[provider.index()];
            UsageProviderPart {
                provider: usage_provider(provider),
                cost_usd: day.cost_usd,
                total_tokens: day.total_tokens,
            }
        })
        .collect()
}

fn model_costs(models: Vec<(String, f64)>) -> Vec<ModelCost> {
    models
        .into_iter()
        .map(|(model, cost_usd)| ModelCost { model, cost_usd })
        .collect()
}

pub(super) fn usage(
    context: &V8Context,
    params: UsageHistoryParams,
) -> Result<UsageHistory, Error> {
    let window = legacy_window(params.window)?;
    let project_roots = if params.project_ids.is_empty() {
        Vec::new()
    } else {
        projects(context)?
            .into_iter()
            .filter(|project| params.project_ids.contains(&project.id.0))
            .map(|project| project.path)
            .collect()
    };
    let history = match call(
        context,
        Command::LoadUsageHistory {
            window,
            project_roots,
        },
        Domain::General,
    )? {
        ResponsePayload::UsageHistory { history } => history,
        other => return Err(unexpected(&other)),
    };
    use crate::usage_history::PricingStatus as Pricing;
    Ok(UsageHistory {
        window: wire_window(history.window),
        since_day: history.since_day.to_string(),
        until_day: history.until_day.to_string(),
        totals: TokenTotals {
            uncached_input: history.totals.uncached_input,
            cached_input: history.totals.cached_input,
            cache_creation: history.totals.cache_creation,
            output: history.totals.output,
            reasoning: history.totals.reasoning,
        },
        total_tokens: history.total_tokens,
        cost_usd: history.cost_usd,
        records: history.records,
        sessions: history.sessions,
        providers: history
            .providers
            .into_iter()
            .map(|slice| UsageProviderSlice {
                provider: usage_provider(slice.provider),
                cost_usd: slice.cost_usd,
                total_tokens: slice.total_tokens,
                cost_share: slice.cost_share,
                token_share: slice.token_share,
            })
            .collect(),
        models: history
            .models
            .into_iter()
            .map(|slice| UsageModelSlice {
                provider: usage_provider(slice.provider),
                model: slice.model,
                cost_usd: slice.cost_usd,
                total_tokens: slice.total_tokens,
                cost_share: slice.cost_share,
            })
            .collect(),
        daily: history
            .daily
            .into_iter()
            .map(|day| UsageDay {
                day: day.day.to_string(),
                cost_usd: day.cost_usd,
                total_tokens: day.total_tokens,
                by_provider: by_provider(&day.by_provider),
            })
            .collect(),
        months: history
            .months
            .into_iter()
            .map(|month| UsageMonth {
                first_day: month.first_day.to_string(),
                cost_usd: month.cost_usd,
                total_tokens: month.total_tokens,
                by_provider: by_provider(&month.by_provider),
                sessions: month.sessions,
                active_days: month.active_days,
                top_models: model_costs(month.top_models),
            })
            .collect(),
        projects: history
            .projects
            .into_iter()
            .map(|project| UsageProject {
                path: project.path,
                cost_usd: project.cost_usd,
                total_tokens: project.total_tokens,
                by_provider: by_provider(&project.by_provider),
                sessions: project.sessions,
                cost_share: project.cost_share,
                last_day: project.last_day.map(|day| day.to_string()),
                top_models: model_costs(project.top_models),
            })
            .collect(),
        quality: CostQuality {
            provider_reported_share: history.quality.provider_reported_share,
            model_priced_share: history.quality.model_priced_share,
            unpriced_share: history.quality.unpriced_share,
            cache_savings_usd: history.quality.cache_savings_usd,
        },
        pricing: match history.pricing {
            Pricing::Fresh => PricingStatus::Fresh,
            Pricing::Cached => PricingStatus::Cached,
            Pricing::Unavailable => PricingStatus::Unavailable,
        },
        scanned_files: history.scanned_files as u64,
        skipped_files: history.skipped_files as u64,
        errors: history.errors,
        scan_duration_ms: u64::try_from(history.scan_duration.as_millis()).unwrap_or(u64::MAX),
    })
}

pub(super) fn computer_permissions(
    context: &V8Context,
    local: bool,
    params: ComputerUsePermissionsParams,
) -> Result<ComputerPermissions, Error> {
    if params.prompt && !local {
        return Err(Error::new(
            ErrorCode::Forbidden,
            "permission prompts open on the daemon host; ask over the local socket",
        )
        .with_details(ErrorDetails::MissingScope {
            required: Scope::Operate,
        }));
    }
    match call(
        context,
        Command::ProbeComputerPermissions {
            prompt: params.prompt,
        },
        Domain::General,
    ) {
        Ok(ResponsePayload::ComputerPermissions { permissions }) => Ok(ComputerPermissions {
            screen_recording: permissions.screen_recording,
            accessibility: permissions.accessibility,
        }),
        Ok(other) => Err(unexpected(&other)),
        Err(error) => Err(Error::new(ErrorCode::Unsupported, error.message)),
    }
}
