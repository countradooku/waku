//! The sidebar the previous launch showed, cached so the next launch can
//! paint its first frame before the daemon has answered (#29).
//!
//! The snapshot is display-only. It never reaches `StateStore`, `Waku::save`
//! or the daemon: the shell paints it, and the real view replaces it as soon
//! as `LoadTaskState` lands. A stale or wrong snapshot can therefore cost one
//! misleading frame, never data. Even so it is invalidated whenever it could
//! describe a different sidebar:
//!
//! - its format version or the app version differs from this build;
//! - it was written for another daemon (the managed local daemon versus a
//!   remote one named by `WAKU_DAEMON_ADDRESS`, or another remote address);
//! - it does not parse, is larger than [`MAX_FILE_BYTES`], or holds a
//!   non-finite size.
//!
//! It is bounded: at most [`MAX_ROWS`] rows of at most [`MAX_LABEL_CHARS`]
//! characters each, which is more than a tall window shows, and the reader
//! re-applies both bounds so a hand-edited file cannot grow the shell.
//! Writes go to a temporary file renamed over the old one, so a crash
//! mid-write leaves the previous snapshot or none, never a torn one.

use std::fs;
use std::io::{self, Read as _};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::i18n::AppLanguage;
use crate::theme::ThemePreference;

/// Bumped whenever the layout below changes meaning.
pub const SNAPSHOT_VERSION: u32 = 1;
/// More rows than the tallest window can show at once.
pub const MAX_ROWS: usize = 96;
pub const MAX_LABEL_CHARS: usize = 160;
/// A full snapshot is about 15 KB; anything far larger is not one of ours.
pub const MAX_FILE_BYTES: u64 = 64 * 1024;
const FILE_NAME: &str = "startup-sidebar.json";

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SidebarSnapshot {
    pub version: u32,
    pub app_version: String,
    /// Which daemon the rows came from; see [`daemon_key`].
    pub daemon: String,
    pub theme: ThemePreference,
    pub language: AppLanguage,
    pub ui_font_size: f32,
    pub sidebar_visible: bool,
    pub sidebar_width: f32,
    pub rows: Vec<SnapshotRow>,
}

/// One sidebar row, already localized and laid out in display order.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SnapshotRow {
    Header { label: String },
    Session { title: String, selected: bool },
    Spacer,
}

/// Display settings the shell needs besides the rows.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ShellSettings {
    pub theme: ThemePreference,
    pub language: AppLanguage,
    pub ui_font_size: f32,
    pub sidebar_visible: bool,
    pub sidebar_width: f32,
}

impl SidebarSnapshot {
    pub fn new(daemon: String, settings: ShellSettings, rows: Vec<SnapshotRow>) -> Self {
        let mut snapshot = Self {
            version: SNAPSHOT_VERSION,
            app_version: env!("CARGO_PKG_VERSION").to_owned(),
            daemon,
            theme: settings.theme,
            language: settings.language,
            ui_font_size: settings.ui_font_size,
            sidebar_visible: settings.sidebar_visible,
            sidebar_width: settings.sidebar_width,
            rows,
        };
        snapshot.bound();
        snapshot
    }

    pub fn settings(&self) -> ShellSettings {
        ShellSettings {
            theme: self.theme,
            language: self.language,
            ui_font_size: self.ui_font_size,
            sidebar_visible: self.sidebar_visible,
            sidebar_width: self.sidebar_width,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Parse a snapshot written for `daemon` by this build, or `None` when it
    /// must not be shown.
    pub fn decode(bytes: &[u8], daemon: &str) -> Option<Self> {
        if bytes.len() as u64 > MAX_FILE_BYTES {
            return None;
        }
        let mut snapshot = serde_json::from_slice::<Self>(bytes).ok()?;
        let current = snapshot.version == SNAPSHOT_VERSION
            && snapshot.app_version == env!("CARGO_PKG_VERSION")
            && snapshot.daemon == daemon
            && snapshot.ui_font_size.is_finite()
            && snapshot.sidebar_width.is_finite();
        if !current {
            return None;
        }
        snapshot.bound();
        Some(snapshot)
    }

    fn bound(&mut self) {
        self.rows.truncate(MAX_ROWS);
        for row in &mut self.rows {
            match row {
                SnapshotRow::Header { label } => truncate_chars(label, MAX_LABEL_CHARS),
                SnapshotRow::Session { title, .. } => truncate_chars(title, MAX_LABEL_CHARS),
                SnapshotRow::Spacer => {}
            }
        }
    }
}

fn truncate_chars(text: &mut String, limit: usize) {
    if let Some((index, _)) = text.char_indices().nth(limit) {
        text.truncate(index);
    }
}

/// Identifies the daemon a snapshot describes. The desktop either manages its
/// own local daemon or connects to the one `WAKU_DAEMON_ADDRESS` names; their
/// task lists have nothing in common.
pub fn daemon_key() -> String {
    match std::env::var(waku_client::DAEMON_ADDRESS_ENV)
        .ok()
        .map(|address| address.trim().to_owned())
        .filter(|address| !address.is_empty())
    {
        Some(address) => format!("remote:{address}"),
        None => "local".to_owned(),
    }
}

/// Beside the desktop's `state.json`, so the bench harness's isolated HOME
/// isolates it too.
pub fn default_path() -> PathBuf {
    crate::persistence::StateStore::default_dir().join(FILE_NAME)
}

/// Read the snapshot for `daemon`. Any failure means "no snapshot".
pub fn read(path: &Path, daemon: &str) -> Option<SidebarSnapshot> {
    let file = fs::File::open(path).ok()?;
    if file.metadata().ok()?.len() > MAX_FILE_BYTES {
        return None;
    }
    let mut bytes = Vec::new();
    file.take(MAX_FILE_BYTES + 1).read_to_end(&mut bytes).ok()?;
    SidebarSnapshot::decode(&bytes, daemon)
}

/// Atomically replace the snapshot at `path`.
pub fn write(path: &Path, snapshot: &SidebarSnapshot) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension(format!("json.{}.tmp", std::process::id()));
    fs::write(&temporary, snapshot.encode())?;
    fs::rename(&temporary, path).inspect_err(|_| {
        let _ = fs::remove_file(&temporary);
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings() -> ShellSettings {
        ShellSettings {
            theme: ThemePreference::Dark,
            language: AppLanguage::English,
            ui_font_size: 14.0,
            sidebar_visible: true,
            sidebar_width: 280.0,
        }
    }

    fn sample() -> SidebarSnapshot {
        SidebarSnapshot::new(
            "local".into(),
            settings(),
            vec![
                SnapshotRow::Header {
                    label: "Today".into(),
                },
                SnapshotRow::Session {
                    title: "Fix the flaky test".into(),
                    selected: true,
                },
                SnapshotRow::Spacer,
            ],
        )
    }

    #[test]
    fn round_trips_through_its_file_format() {
        let snapshot = sample();
        let bytes = snapshot.encode();
        assert_eq!(SidebarSnapshot::decode(&bytes, "local"), Some(snapshot));
        // The format is plain tagged JSON so a stale file is easy to inspect.
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["version"], SNAPSHOT_VERSION);
        assert_eq!(json["rows"][0]["kind"], "header");
        assert_eq!(json["rows"][1]["kind"], "session");
        assert_eq!(json["rows"][1]["selected"], true);
        assert_eq!(json["rows"][2]["kind"], "spacer");
        assert_eq!(json["theme"], "dark");
    }

    #[test]
    fn another_daemon_invalidates_the_snapshot() {
        let bytes = sample().encode();
        assert!(SidebarSnapshot::decode(&bytes, "remote:10.0.0.2:34123").is_none());
        let remote = SidebarSnapshot::new("remote:10.0.0.2:34123".into(), settings(), Vec::new());
        assert!(SidebarSnapshot::decode(&remote.encode(), "local").is_none());
        assert!(SidebarSnapshot::decode(&remote.encode(), "remote:10.0.0.2:34123").is_some());
    }

    #[test]
    fn another_format_or_app_version_invalidates_the_snapshot() {
        let mut older_format = sample();
        older_format.version = SNAPSHOT_VERSION + 1;
        assert!(SidebarSnapshot::decode(&older_format.encode(), "local").is_none());

        let mut other_build = sample();
        other_build.app_version = "0.0.0-other".into();
        assert!(SidebarSnapshot::decode(&other_build.encode(), "local").is_none());
    }

    #[test]
    fn malformed_or_oversized_files_are_ignored() {
        assert!(SidebarSnapshot::decode(b"", "local").is_none());
        assert!(SidebarSnapshot::decode(b"{\"version\":1", "local").is_none());
        assert!(SidebarSnapshot::decode(b"[]", "local").is_none());

        let mut huge = sample().encode();
        huge.resize(MAX_FILE_BYTES as usize + 1, b' ');
        assert!(SidebarSnapshot::decode(&huge, "local").is_none());

        let mut json: serde_json::Value = serde_json::from_slice(&sample().encode()).unwrap();
        json["sidebar_width"] = serde_json::Value::Null;
        assert!(SidebarSnapshot::decode(json.to_string().as_bytes(), "local").is_none());
    }

    #[test]
    fn rows_and_labels_stay_bounded_on_write_and_on_read() {
        let long = "界".repeat(MAX_LABEL_CHARS * 2);
        let rows = (0..MAX_ROWS * 2)
            .map(|_| SnapshotRow::Session {
                title: long.clone(),
                selected: false,
            })
            .collect::<Vec<_>>();
        let snapshot = SidebarSnapshot::new("local".into(), settings(), rows);
        assert_eq!(snapshot.rows.len(), MAX_ROWS);
        let SnapshotRow::Session { title, .. } = &snapshot.rows[0] else {
            panic!("expected a session row");
        };
        assert_eq!(title.chars().count(), MAX_LABEL_CHARS);
        assert!(snapshot.encode().len() as u64 <= MAX_FILE_BYTES);

        // A file edited by hand past the bounds is cut back when read.
        let mut edited = snapshot.clone();
        edited.rows = vec![SnapshotRow::Spacer; MAX_ROWS + 5];
        edited.rows[0] = SnapshotRow::Header {
            label: "x".repeat(MAX_LABEL_CHARS + 5),
        };
        let decoded = SidebarSnapshot::decode(&serde_json::to_vec(&edited).unwrap(), "local")
            .expect("an over-long snapshot is cut back, not rejected");
        assert_eq!(decoded.rows.len(), MAX_ROWS);
        assert_eq!(
            decoded.rows[0],
            SnapshotRow::Header {
                label: "x".repeat(MAX_LABEL_CHARS)
            }
        );
    }

    #[test]
    fn writes_replace_the_file_atomically_and_reads_check_the_daemon() {
        let directory =
            std::env::temp_dir().join(format!("waku-startup-snapshot-{}", uuid::Uuid::new_v4()));
        let path = directory.join(FILE_NAME);
        assert!(
            read(&path, "local").is_none(),
            "a missing file is no snapshot"
        );

        write(&path, &sample()).unwrap();
        assert_eq!(read(&path, "local"), Some(sample()));
        assert!(read(&path, "remote:example:1").is_none());

        let mut replacement = sample();
        replacement.rows.clear();
        write(&path, &replacement).unwrap();
        assert_eq!(read(&path, "local"), Some(replacement));
        let leftovers = fs::read_dir(&directory)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name() != FILE_NAME)
            .count();
        assert_eq!(leftovers, 0, "no temporary file may outlive a write");

        fs::write(&path, b"not json").unwrap();
        assert!(read(&path, "local").is_none());
        fs::remove_dir_all(&directory).unwrap();
    }
}
