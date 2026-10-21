//! Providers, usage and Computer Use: `providers.probe`, `providers.models`,
//! `providers.planUsage`, `usage.history`, `computerUse.permissions` and
//! `projects.list` (to name projects by path).
//!
//! Results are converted to the desktop's domain types, which its pages and
//! model pickers already render.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context as _, bail};
use chrono::NaiveDate;
use uuid::Uuid;

use super::DaemonV8;
use crate::v8::{
    self, ComputerUsePermissionsParams, InstanceParams, NoParams, ProvidersModelsParams,
    ProvidersProbeParams, UsageHistoryParams, method,
};
use waku_protocol::computer_use::ComputerPermissions;
use waku_protocol::model::{
    ProviderAgentPreset, ProviderKind, ProviderModel, ProviderModelOption, ProviderProbe,
};
use waku_protocol::usage::{PlanUsage, PlanWindow};
use waku_protocol::usage_history as legacy;

/// A provider's instance id: its kind's id until instances are configured.
pub fn instance_id(provider: ProviderKind) -> String {
    provider.id().to_owned()
}

/// The provider kind an instance id names.
pub fn provider_kind(instance_id: &str) -> Option<ProviderKind> {
    ProviderKind::ALL
        .into_iter()
        .find(|kind| kind.id() == instance_id)
}

fn model_option(option: v8::ModelOption) -> ProviderModelOption {
    ProviderModelOption {
        id: option.id,
        label: option.label,
        description: option.description,
    }
}

pub fn provider_model(model: v8::ProviderModel) -> ProviderModel {
    ProviderModel {
        id: model.id,
        name: model.name,
        sub_provider: model.sub_provider,
        is_default: model.is_default,
        reasoning_efforts: model
            .reasoning_efforts
            .into_iter()
            .map(model_option)
            .collect(),
        default_reasoning_effort: model.default_reasoning_effort,
        service_tiers: model.service_tiers.into_iter().map(model_option).collect(),
        default_service_tier: model.default_service_tier,
        context_windows: model
            .context_windows
            .into_iter()
            .map(model_option)
            .collect(),
        default_context_window: model.default_context_window,
    }
}

pub fn agent_preset(preset: v8::AgentPreset) -> ProviderAgentPreset {
    ProviderAgentPreset {
        id: preset.id,
        name: preset.name,
        description: preset.description,
        is_default: preset.is_default,
        is_custom: preset.is_custom,
    }
}

/// A probe result as the desktop's model pickers read it.
pub fn provider_probe(provider: ProviderKind, probe: v8::ProviderProbe) -> ProviderProbe {
    ProviderProbe {
        provider,
        installed: probe.installed,
        path: probe.path.map(PathBuf::from),
        models: probe.models.into_iter().map(provider_model).collect(),
        agent_presets: probe.agent_presets.into_iter().map(agent_preset).collect(),
    }
}

/// Detect `provider`'s CLI (with the binary the daemon's settings name) and
/// optionally its version.
pub fn probe(
    daemon: &DaemonV8,
    provider: ProviderKind,
    probe_version: bool,
) -> anyhow::Result<(ProviderProbe, Option<String>)> {
    let result = daemon.call::<method::ProvidersProbe>(ProvidersProbeParams {
        instance_id: instance_id(provider),
        discover_models: false,
        probe_version,
    })?;
    Ok((provider_probe(provider, result.probe), result.version))
}

/// Ask the provider for its live model catalog (`providers.models` with
/// `refresh`), keeping what `known` says about the installation.
pub fn discover_models(daemon: &DaemonV8, known: ProviderProbe) -> anyhow::Result<ProviderProbe> {
    let models = daemon.call::<method::ProvidersModels>(ProvidersModelsParams {
        instance_id: instance_id(known.provider),
        refresh: true,
    })?;
    Ok(with_models(known, models))
}

/// `known` with the catalog of `models`.
pub fn with_models(mut known: ProviderProbe, models: v8::ProviderModels) -> ProviderProbe {
    known.models = models.models.into_iter().map(provider_model).collect();
    known.agent_presets = models.agent_presets.into_iter().map(agent_preset).collect();
    known
}

/// Plan usage as the usage meter shows it (`resetsAt` in seconds).
pub fn plan_usage_from_wire(usage: v8::PlanUsage) -> PlanUsage {
    PlanUsage {
        plan_label: usage.plan_label,
        windows: usage
            .windows
            .into_iter()
            .map(|window| PlanWindow {
                label: window.label,
                percent: window.percent,
                resets_at: window.resets_at.map(|ms| ms.div_euclid(1000)),
            })
            .collect(),
    }
}

pub fn plan_usage(daemon: &DaemonV8, provider: ProviderKind) -> anyhow::Result<Option<PlanUsage>> {
    Ok(daemon
        .call::<method::ProvidersPlanUsage>(InstanceParams {
            instance_id: instance_id(provider),
        })?
        .map(plan_usage_from_wire))
}

pub fn computer_permissions(
    daemon: &DaemonV8,
    prompt: bool,
) -> anyhow::Result<ComputerPermissions> {
    let permissions =
        daemon.call::<method::ComputerUsePermissions>(ComputerUsePermissionsParams { prompt })?;
    Ok(ComputerPermissions {
        screen_recording: permissions.screen_recording,
        accessibility: permissions.accessibility,
    })
}

/// The orchestrator's ids of the projects at `paths`. The desktop still
/// lists projects by path; matching on it bridges to the daemon's project
/// ids until the session layer hands those out itself (#55).
pub fn project_ids_for_paths(daemon: &DaemonV8, paths: &[PathBuf]) -> anyhow::Result<Vec<Uuid>> {
    if paths.is_empty() {
        return Ok(Vec::new());
    }
    let projects = daemon.call::<method::ProjectsList>(NoParams {})?;
    Ok(projects
        .into_iter()
        .filter(|project| paths.iter().any(|path| same_path(path, &project.path)))
        .map(|project| project.id.0)
        .collect())
}

fn same_path(left: &Path, right: &Path) -> bool {
    left.components().eq(right.components())
}

fn wire_window(window: legacy::UsageWindow) -> v8::UsageWindow {
    match window {
        legacy::UsageWindow::TrailingDays(days) => v8::UsageWindow::TrailingDays { days },
        legacy::UsageWindow::Months(months) => v8::UsageWindow::Months { months },
        legacy::UsageWindow::ThisMonth => v8::UsageWindow::ThisMonth,
        legacy::UsageWindow::LastMonth => v8::UsageWindow::LastMonth,
    }
}

fn legacy_window(window: v8::UsageWindow) -> anyhow::Result<legacy::UsageWindow> {
    Ok(match window {
        v8::UsageWindow::TrailingDays { days } => legacy::UsageWindow::TrailingDays(days),
        v8::UsageWindow::Months { months } => legacy::UsageWindow::Months(months),
        v8::UsageWindow::ThisMonth => legacy::UsageWindow::ThisMonth,
        v8::UsageWindow::LastMonth => legacy::UsageWindow::LastMonth,
        v8::UsageWindow::Unknown => bail!("the daemon sent an unknown usage window"),
    })
}

fn usage_provider(provider: &str) -> Option<legacy::UsageProvider> {
    match provider {
        "claude" => Some(legacy::UsageProvider::Claude),
        "codex" => Some(legacy::UsageProvider::Codex),
        _ => None,
    }
}

fn day(value: &str) -> anyhow::Result<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .with_context(|| format!("the daemon sent an invalid day {value:?}"))
}

fn by_provider(parts: &[v8::UsageProviderPart]) -> [legacy::ProviderDay; 2] {
    let mut days = [legacy::ProviderDay::default(); 2];
    for part in parts {
        if let Some(provider) = usage_provider(&part.provider) {
            days[provider.index()] = legacy::ProviderDay {
                cost_usd: part.cost_usd,
                total_tokens: part.total_tokens,
            };
        }
    }
    days
}

fn model_costs(models: Vec<v8::ModelCost>) -> Vec<(String, f64)> {
    models
        .into_iter()
        .map(|model| (model.model, model.cost_usd))
        .collect()
}

/// A `usage.history` report as the usage page renders it.
pub fn usage_history_from_wire(history: v8::UsageHistory) -> anyhow::Result<legacy::UsageHistory> {
    Ok(legacy::UsageHistory {
        window: legacy_window(history.window)?,
        since_day: day(&history.since_day)?,
        until_day: day(&history.until_day)?,
        totals: legacy::TokenTotals {
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
            .filter_map(|slice| {
                Some(legacy::ProviderSlice {
                    provider: usage_provider(&slice.provider)?,
                    cost_usd: slice.cost_usd,
                    total_tokens: slice.total_tokens,
                    cost_share: slice.cost_share,
                    token_share: slice.token_share,
                })
            })
            .collect(),
        models: history
            .models
            .into_iter()
            .filter_map(|slice| {
                Some(legacy::ModelSlice {
                    provider: usage_provider(&slice.provider)?,
                    model: slice.model,
                    cost_usd: slice.cost_usd,
                    total_tokens: slice.total_tokens,
                    cost_share: slice.cost_share,
                })
            })
            .collect(),
        daily: history
            .daily
            .into_iter()
            .map(|slice| {
                Ok(legacy::DaySlice {
                    day: day(&slice.day)?,
                    cost_usd: slice.cost_usd,
                    total_tokens: slice.total_tokens,
                    by_provider: by_provider(&slice.by_provider),
                })
            })
            .collect::<anyhow::Result<_>>()?,
        months: history
            .months
            .into_iter()
            .map(|month| {
                Ok(legacy::MonthSlice {
                    first_day: day(&month.first_day)?,
                    cost_usd: month.cost_usd,
                    total_tokens: month.total_tokens,
                    by_provider: by_provider(&month.by_provider),
                    sessions: month.sessions,
                    active_days: month.active_days,
                    top_models: model_costs(month.top_models),
                })
            })
            .collect::<anyhow::Result<_>>()?,
        projects: history
            .projects
            .into_iter()
            .map(|project| {
                Ok(legacy::ProjectSlice {
                    path: project.path,
                    cost_usd: project.cost_usd,
                    total_tokens: project.total_tokens,
                    by_provider: by_provider(&project.by_provider),
                    sessions: project.sessions,
                    cost_share: project.cost_share,
                    last_day: project.last_day.as_deref().map(day).transpose()?,
                    top_models: model_costs(project.top_models),
                })
            })
            .collect::<anyhow::Result<_>>()?,
        quality: legacy::CostQuality {
            provider_reported_share: history.quality.provider_reported_share,
            model_priced_share: history.quality.model_priced_share,
            unpriced_share: history.quality.unpriced_share,
            cache_savings_usd: history.quality.cache_savings_usd,
        },
        pricing: match history.pricing {
            v8::PricingStatus::Fresh => legacy::PricingStatus::Fresh,
            v8::PricingStatus::Cached => legacy::PricingStatus::Cached,
            _ => legacy::PricingStatus::Unavailable,
        },
        scanned_files: usize::try_from(history.scanned_files).unwrap_or(usize::MAX),
        skipped_files: usize::try_from(history.skipped_files).unwrap_or(usize::MAX),
        errors: history.errors,
        scan_duration: Duration::from_millis(history.scan_duration_ms),
    })
}

/// The usage report for `window`, broken out for the projects at
/// `project_paths`.
pub fn usage_history(
    daemon: &DaemonV8,
    window: legacy::UsageWindow,
    project_paths: &[PathBuf],
) -> anyhow::Result<legacy::UsageHistory> {
    let project_ids = project_ids_for_paths(daemon, project_paths)?;
    let history = daemon.call::<method::UsageHistoryGet>(UsageHistoryParams {
        window: wire_window(window),
        project_ids,
    })?;
    usage_history_from_wire(history)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_ids_round_trip_every_provider() {
        for provider in ProviderKind::ALL {
            assert_eq!(provider_kind(&instance_id(provider)), Some(provider));
        }
        assert_eq!(provider_kind("nebula"), None);
    }

    #[test]
    fn plan_usage_reset_times_return_to_seconds() {
        let usage = plan_usage_from_wire(v8::PlanUsage {
            plan_label: Some("Max".into()),
            windows: vec![v8::PlanWindow {
                label: "5h".into(),
                percent: 42.5,
                resets_at: Some(1_700_000_000_999),
            }],
        });
        assert_eq!(usage.plan_label.as_deref(), Some("Max"));
        assert_eq!(usage.windows[0].resets_at, Some(1_700_000_000));
        assert_eq!(usage.windows[0].percent, 42.5);
    }

    #[test]
    fn a_discovered_catalog_keeps_the_installation() {
        let known = ProviderProbe {
            provider: ProviderKind::Codex,
            installed: true,
            path: Some("/usr/bin/codex".into()),
            models: Vec::new(),
            agent_presets: Vec::new(),
        };
        let probe = with_models(
            known,
            v8::ProviderModels {
                models: vec![v8::ProviderModel {
                    id: "gpt-5".into(),
                    name: "GPT-5".into(),
                    sub_provider: None,
                    is_default: true,
                    reasoning_efforts: vec![v8::ModelOption {
                        id: "high".into(),
                        label: "High".into(),
                        description: None,
                    }],
                    default_reasoning_effort: Some("high".into()),
                    service_tiers: Vec::new(),
                    default_service_tier: None,
                    context_windows: Vec::new(),
                    default_context_window: None,
                }],
                agent_presets: Vec::new(),
                source: v8::ModelCatalogSource::Discovered,
            },
        );
        assert!(probe.installed);
        assert_eq!(probe.path, Some(PathBuf::from("/usr/bin/codex")));
        assert_eq!(probe.preferred_model().unwrap().id, "gpt-5");
        assert_eq!(probe.models[0].reasoning_efforts[0].label, "High");
    }

    #[test]
    fn a_usage_report_converts_days_and_provider_columns() {
        let part = |provider: &str, cost_usd| v8::UsageProviderPart {
            provider: provider.into(),
            cost_usd,
            total_tokens: 10,
        };
        let history = usage_history_from_wire(v8::UsageHistory {
            window: v8::UsageWindow::TrailingDays { days: 7 },
            since_day: "2026-10-15".into(),
            until_day: "2026-10-21".into(),
            totals: v8::TokenTotals::default(),
            total_tokens: 20,
            cost_usd: 3.0,
            records: 2,
            sessions: 1,
            providers: vec![v8::UsageProviderSlice {
                provider: "codex".into(),
                cost_usd: 1.0,
                total_tokens: 10,
                cost_share: 0.5,
                token_share: 0.5,
            }],
            models: Vec::new(),
            daily: vec![v8::UsageDay {
                day: "2026-10-20".into(),
                cost_usd: 3.0,
                total_tokens: 20,
                by_provider: vec![part("codex", 1.0), part("claude", 2.0), part("pi", 9.0)],
            }],
            months: Vec::new(),
            projects: vec![v8::UsageProject {
                path: "/srv/app".into(),
                cost_usd: 3.0,
                total_tokens: 20,
                by_provider: Vec::new(),
                sessions: 1,
                cost_share: 1.0,
                last_day: Some("2026-10-20".into()),
                top_models: vec![v8::ModelCost {
                    model: "gpt-5".into(),
                    cost_usd: 3.0,
                }],
            }],
            quality: v8::CostQuality::default(),
            pricing: v8::PricingStatus::Cached,
            scanned_files: 4,
            skipped_files: 1,
            errors: Vec::new(),
            scan_duration_ms: 1_500,
        })
        .unwrap();
        assert_eq!(history.window, legacy::UsageWindow::TrailingDays(7));
        assert_eq!(
            history.since_day,
            NaiveDate::from_ymd_opt(2026, 10, 15).unwrap()
        );
        let day = &history.daily[0];
        assert_eq!(
            day.by_provider[legacy::UsageProvider::Claude.index()].cost_usd,
            2.0
        );
        assert_eq!(
            day.by_provider[legacy::UsageProvider::Codex.index()].cost_usd,
            1.0
        );
        assert_eq!(history.providers[0].provider, legacy::UsageProvider::Codex);
        assert_eq!(history.projects[0].top_models, [("gpt-5".to_owned(), 3.0)]);
        assert_eq!(history.pricing, legacy::PricingStatus::Cached);
        assert_eq!(history.scan_duration, Duration::from_millis(1_500));
        assert!(
            usage_history_from_wire(v8::UsageHistory {
                since_day: "yesterday".into(),
                ..sample_unknown()
            })
            .is_err()
        );
    }

    fn sample_unknown() -> v8::UsageHistory {
        v8::UsageHistory {
            window: v8::UsageWindow::ThisMonth,
            since_day: "2026-10-01".into(),
            until_day: "2026-10-21".into(),
            totals: v8::TokenTotals::default(),
            total_tokens: 0,
            cost_usd: 0.0,
            records: 0,
            sessions: 0,
            providers: Vec::new(),
            models: Vec::new(),
            daily: Vec::new(),
            months: Vec::new(),
            projects: Vec::new(),
            quality: v8::CostQuality::default(),
            pricing: v8::PricingStatus::Fresh,
            scanned_files: 0,
            skipped_files: 0,
            errors: Vec::new(),
            scan_duration_ms: 0,
        }
    }
}
