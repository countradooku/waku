//! Providers and usage (RFC 0003 §5.2 "Providers and usage", §5.4
//! `providers`).
//!
//! A provider instance is named by its id (`"codex"`, `"claude"`, …). Until
//! provider instances are configured in settings, an instance id is the
//! provider kind's id. Binaries come from settings
//! (`providerBinaryOverrides`), never from a request.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::StreamCursor;

/// A provider instance as the providers list shows it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ProviderStatus {
    pub instance_id: String,
    /// The driver kind (`codex`, `claude`, …).
    pub kind: String,
    pub display_name: String,
    /// Not listed in `disabledProviders`.
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binary_override: Option<String>,
    /// Whether the binary was found, once a probe has run.
    pub installed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// The CLI version, once a probe asked for it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Whether this provider reports plan usage (`providers.planUsage`).
    pub plan_usage: bool,
}

/// `providers.probe` params.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ProvidersProbeParams {
    pub instance_id: String,
    /// Ask the CLI for its model catalog (slow; spawns the provider).
    #[serde(default)]
    pub discover_models: bool,
    /// Ask the CLI for its version.
    #[serde(default)]
    pub probe_version: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ProviderProbeResult {
    pub probe: ProviderProbe,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ProviderProbe {
    pub instance_id: String,
    pub installed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default)]
    pub models: Vec<ProviderModel>,
    #[serde(default)]
    pub agent_presets: Vec<AgentPreset>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ProviderModel {
    pub id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_provider: Option<String>,
    #[serde(default)]
    pub is_default: bool,
    #[serde(default)]
    pub reasoning_efforts: Vec<ModelOption>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_reasoning_effort: Option<String>,
    #[serde(default)]
    pub service_tiers: Vec<ModelOption>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_service_tier: Option<String>,
    /// Context window sizes offered as a per-session choice.
    #[serde(default)]
    pub context_windows: Vec<ModelOption>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_context_window: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ModelOption {
    pub id: String,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// A provider-owned agent composition (DeepSeek Harness presets).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct AgentPreset {
    pub id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub is_default: bool,
    #[serde(default)]
    pub is_custom: bool,
}

/// `providers.models` params.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ProvidersModelsParams {
    pub instance_id: String,
    /// Ask the CLI for a fresh catalog instead of the cached one.
    #[serde(default)]
    pub refresh: bool,
}

open_string_enum! {
    /// Where a model catalog came from.
    pub enum ModelCatalogSource {
        /// Asked from the CLI just now.
        Discovered = "discovered",
        /// The catalog the CLI reported last time.
        Cached = "cached",
        /// Waku's built-in list; the CLI was not asked.
        Fallback = "fallback",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ProviderModels {
    pub models: Vec<ProviderModel>,
    pub agent_presets: Vec<AgentPreset>,
    pub source: ModelCatalogSource,
}

/// The params of a read about one provider instance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct InstanceParams {
    pub instance_id: String,
}

/// An account's plan usage windows.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct PlanUsage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_label: Option<String>,
    pub windows: Vec<PlanWindow>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct PlanWindow {
    pub label: String,
    /// 0–100.
    pub percent: f64,
    /// Milliseconds since the Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resets_at: Option<i64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ProvidersSpec {}

/// `providers` items. Statuses change when settings enable, disable or
/// re-point a provider and when a probe runs; plan usage when
/// `providers.planUsage` fetches it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ProvidersItem {
    Snapshot {
        revision: u64,
        statuses: Vec<ProviderStatus>,
    },
    StatusUpdated {
        revision: u64,
        status: ProviderStatus,
    },
    PlanUsageUpdated {
        instance_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        usage: Option<PlanUsage>,
    },
    Synchronized {
        cursor: StreamCursor,
    },
    #[serde(other)]
    Unknown,
}

// -- usage.history ----------------------------------------------------------

/// A usage reporting window.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum UsageWindow {
    /// The last `days` days, today included.
    TrailingDays {
        days: u32,
    },
    /// The last `months` calendar months, this one included.
    Months {
        months: u32,
    },
    ThisMonth,
    LastMonth,
    #[serde(other)]
    Unknown,
}

/// `usage.history` params.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct UsageHistoryParams {
    pub window: UsageWindow,
    /// Projects whose usage is broken out per project.
    #[serde(default)]
    pub project_ids: Vec<Uuid>,
}

open_string_enum! {
    pub enum PricingStatus {
        Fresh = "fresh",
        Cached = "cached",
        Unavailable = "unavailable",
    }
}

/// Token and cost history read from the providers' local logs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct UsageHistory {
    pub window: UsageWindow,
    /// `YYYY-MM-DD`.
    pub since_day: String,
    pub until_day: String,
    pub totals: TokenTotals,
    pub total_tokens: u64,
    pub cost_usd: f64,
    pub records: u64,
    pub sessions: u64,
    pub providers: Vec<UsageProviderSlice>,
    pub models: Vec<UsageModelSlice>,
    pub daily: Vec<UsageDay>,
    pub months: Vec<UsageMonth>,
    pub projects: Vec<UsageProject>,
    pub quality: CostQuality,
    pub pricing: PricingStatus,
    pub scanned_files: u64,
    pub skipped_files: u64,
    pub errors: Vec<String>,
    pub scan_duration_ms: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct TokenTotals {
    pub uncached_input: u64,
    pub cached_input: u64,
    pub cache_creation: u64,
    pub output: u64,
    pub reasoning: u64,
}

/// One provider's share of a window. `provider` is a provider id.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct UsageProviderSlice {
    pub provider: String,
    pub cost_usd: f64,
    pub total_tokens: u64,
    pub cost_share: f64,
    pub token_share: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct UsageModelSlice {
    pub provider: String,
    pub model: String,
    pub cost_usd: f64,
    pub total_tokens: u64,
    pub cost_share: f64,
}

/// One provider's part of a day, month or project.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct UsageProviderPart {
    pub provider: String,
    pub cost_usd: f64,
    pub total_tokens: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct UsageDay {
    /// `YYYY-MM-DD`.
    pub day: String,
    pub cost_usd: f64,
    pub total_tokens: u64,
    pub by_provider: Vec<UsageProviderPart>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ModelCost {
    pub model: String,
    pub cost_usd: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct UsageMonth {
    /// `YYYY-MM-DD`, the first day of the month.
    pub first_day: String,
    pub cost_usd: f64,
    pub total_tokens: u64,
    pub by_provider: Vec<UsageProviderPart>,
    pub sessions: u64,
    pub active_days: u32,
    pub top_models: Vec<ModelCost>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct UsageProject {
    pub path: String,
    pub cost_usd: f64,
    pub total_tokens: u64,
    pub by_provider: Vec<UsageProviderPart>,
    pub sessions: u64,
    pub cost_share: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_day: Option<String>,
    pub top_models: Vec<ModelCost>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct CostQuality {
    pub provider_reported_share: f64,
    pub model_priced_share: f64,
    pub unpriced_share: f64,
    pub cache_savings_usd: f64,
}

// -- computer use -----------------------------------------------------------

/// `computerUse.permissions` params. `prompt: true` asks macOS to show its
/// permission prompts; it needs `operate` and the local socket.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ComputerUsePermissionsParams {
    #[serde(default)]
    pub prompt: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ComputerPermissions {
    pub screen_recording: bool,
    pub accessibility: bool,
}
