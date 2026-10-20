use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use crate::ts::TS;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::computer_use::ComputerAppGrant;
use crate::model::ProviderKind;
use crate::storage::StorageSettings;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, TS)]
#[serde(default)]
#[derive(Default)]
pub struct DaemonSettings {
    pub computer_use_enabled: bool,
    pub computer_use_allowed_apps: Vec<ComputerAppGrant>,
    pub disabled_providers: Vec<ProviderKind>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub provider_binary_overrides: HashMap<ProviderKind, String>,
    /// Managed worktree folder and its cleanup rules (#62).
    #[serde(skip_serializing_if = "StorageSettings::is_default")]
    #[ts(as = "Option<StorageSettings>", optional)]
    pub storage: StorageSettings,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl DaemonSettings {
    pub fn default_path() -> PathBuf {
        crate::identity::home_directory().join("settings.json")
    }

    pub fn discard_legacy_app_keys(&mut self) {
        for key in ["analytics_enabled", "favorite_models", "theme", "language"] {
            self.extra.remove(key);
        }
    }
}
