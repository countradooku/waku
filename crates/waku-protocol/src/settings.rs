use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::computer_use::ComputerAppGrant;
use crate::model::ProviderKind;
use crate::storage::StorageSettings;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
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
    pub storage: StorageSettings,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Settings key of "continue threads after restarts" (#59). It lives in the
/// open part of the document, which every client round-trips (as
/// `provider_homes` does), so a client that does not know it keeps it.
pub const CONTINUE_THREADS_AFTER_RESTART_KEY: &str = "continue_threads_after_restart";

impl DaemonSettings {
    /// Whether threads interrupted by a daemon restart are continued (#59).
    /// On unless set to `false`.
    pub fn continue_threads_after_restart(&self) -> bool {
        self.extra
            .get(CONTINUE_THREADS_AFTER_RESTART_KEY)
            .and_then(Value::as_bool)
            .unwrap_or(true)
    }

    pub fn set_continue_threads_after_restart(&mut self, enabled: bool) {
        self.extra.insert(
            CONTINUE_THREADS_AFTER_RESTART_KEY.to_owned(),
            Value::Bool(enabled),
        );
    }

    pub fn default_path() -> PathBuf {
        crate::identity::home_directory().join("settings.json")
    }

    pub fn discard_legacy_app_keys(&mut self) {
        for key in ["analytics_enabled", "favorite_models", "theme", "language"] {
            self.extra.remove(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn continuing_after_restarts_is_on_unless_turned_off() {
        let mut settings = DaemonSettings::default();
        assert!(settings.continue_threads_after_restart());
        settings.set_continue_threads_after_restart(false);
        assert!(!settings.continue_threads_after_restart());
        let json = serde_json::to_string(&settings).unwrap();
        assert!(
            json.contains("\"continue_threads_after_restart\":false"),
            "{json}"
        );
        let decoded: DaemonSettings = serde_json::from_str(&json).unwrap();
        assert!(!decoded.continue_threads_after_restart());
    }
}
