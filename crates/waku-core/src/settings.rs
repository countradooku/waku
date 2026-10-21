//! Daemon-owned, user-editable configuration.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use uuid::Uuid;
pub use waku_protocol::settings::DaemonSettings;

pub struct DaemonSettingsStore {
    path: PathBuf,
    settings: Mutex<DaemonSettings>,
}

impl DaemonSettingsStore {
    /// Open the daemon document; a missing one means the defaults (D6: no
    /// settings are imported from another layout).
    pub fn open(path: PathBuf) -> io::Result<Self> {
        let (mut settings, write_current) = match fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice(&bytes) {
                Ok(settings) => (settings, false),
                Err(error) => {
                    let backup = quarantine_corrupt_settings(&path)?;
                    eprintln!(
                        "Waku daemon moved invalid settings to {}: {error}",
                        backup.display()
                    );
                    (DaemonSettings::default(), true)
                }
            },
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                (DaemonSettings::default(), false)
            }
            Err(error) => return Err(error),
        };
        settings.discard_legacy_app_keys();
        normalize(&mut settings);
        if write_current {
            write_atomic(&path, &settings)?;
        }
        Ok(Self {
            path,
            settings: Mutex::new(settings),
        })
    }

    pub fn get(&self) -> DaemonSettings {
        self.settings.lock().clone()
    }

    pub fn replace(&self, mut settings: DaemonSettings) -> io::Result<()> {
        settings.discard_legacy_app_keys();
        normalize(&mut settings);
        let mut current = self.settings.lock();
        write_atomic(&self.path, &settings)?;
        *current = settings;
        Ok(())
    }
}

/// Clamp storage rules and drop worktree locations that could never be
/// valid, whether they came from a client or a hand-edited file.
fn normalize(settings: &mut DaemonSettings) {
    settings.storage =
        std::mem::take(&mut settings.storage).normalized(dirs::home_dir().as_deref());
}

fn quarantine_corrupt_settings(path: &Path) -> io::Result<PathBuf> {
    let extension = format!("json.corrupt-{}", Uuid::new_v4().simple());
    let backup = path.with_extension(extension);
    fs::rename(path, &backup)?;
    Ok(backup)
}

fn write_atomic(path: &Path, settings: &DaemonSettings) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let data = serde_json::to_vec_pretty(settings).map_err(to_io_error)?;
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, data)?;
    fs::rename(temporary, path)
}

fn to_io_error(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use uuid::Uuid;

    #[test]
    fn legacy_combined_settings_keep_only_daemon_fields() {
        let path = std::env::temp_dir().join(format!("waku-settings-{}.json", Uuid::new_v4()));
        fs::write(
            &path,
            r#"{"theme":"dark","analytics_enabled":false,"computer_use_enabled":true,"future":42}"#,
        )
        .unwrap();

        let store = DaemonSettingsStore::open(path.clone()).unwrap();
        let settings = store.get();
        assert!(settings.computer_use_enabled);
        assert_eq!(settings.extra.get("future"), Some(&Value::from(42)));
        assert!(!settings.extra.contains_key("theme"));
        store.replace(settings).unwrap();

        let value: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert!(value.get("theme").is_none());
        assert!(value.get("analytics_enabled").is_none());
        assert_eq!(value["future"], 42);
        fs::remove_file(path).ok();
    }

    #[test]
    fn corrupt_current_settings_are_quarantined_and_replaced() {
        let directory = std::env::temp_dir().join(format!("waku-settings-{}", Uuid::new_v4()));
        let path = directory.join("settings.json");
        fs::create_dir_all(&directory).unwrap();
        fs::write(&path, b"{ definitely not json").unwrap();

        let store = DaemonSettingsStore::open(path.clone()).unwrap();

        assert_eq!(store.get(), DaemonSettings::default());
        assert!(serde_json::from_slice::<DaemonSettings>(&fs::read(&path).unwrap()).is_ok());
        assert!(fs::read_dir(&directory).unwrap().flatten().any(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("settings.json.corrupt-")
        }));
        fs::remove_dir_all(directory).ok();
    }

    #[test]
    fn storage_settings_migrate_to_safe_defaults_and_are_normalized() {
        let directory = std::env::temp_dir().join(format!("waku-settings-{}", Uuid::new_v4()));
        let path = directory.join("settings.json");
        fs::create_dir_all(&directory).unwrap();
        // A document written before storage settings existed.
        fs::write(&path, r#"{"computer_use_enabled":false,"future":1}"#).unwrap();
        let store = DaemonSettingsStore::open(path.clone()).unwrap();
        let storage = store.get().storage;
        assert_eq!(storage, waku_protocol::storage::StorageSettings::default());
        assert!(!storage.worktree_cleanup.any_rule_enabled());

        // Hand-edited values are clamped and invalid locations dropped.
        fs::write(
            &path,
            r#"{"storage":{"worktrees_directory":"relative","worktree_cleanup":{"inactive_after_days":0,"interval_hours":0}}}"#,
        )
        .unwrap();
        let store = DaemonSettingsStore::open(path.clone()).unwrap();
        let storage = store.get().storage;
        assert_eq!(storage.worktrees_directory, None);
        assert_eq!(storage.worktree_cleanup.inactive_after_days, Some(1));
        assert_eq!(storage.worktree_cleanup.interval_hours, 1);
        fs::remove_dir_all(directory).ok();
    }
}
