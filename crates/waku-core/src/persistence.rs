//! The daemon's small file-backed stores beside the orchestrator's
//! `state.sqlite`: the data directory itself, composer drafts
//! (`composer-drafts.json`), and the sweep that reclaims blobs and
//! attachment trees nothing references any more.
//!
//! Projects, threads and transcripts live only in the orchestrator's event
//! store (RFC 0001). Daemon preferences are [`crate::settings`]'s.

use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use parking_lot::Mutex;

use crate::blob_store::BlobStore;
use crate::identity::DATA_DIRECTORY_NAME;
pub use waku_protocol::persistence::{
    ComposerDraft, ComposerDraftAttachment, ComposerDraftChange, ComposerDraftKey,
    ComposerDraftTarget, ComposerDrafts,
};

const COMPOSER_DRAFTS_FILENAME: &str = "composer-drafts.json";
/// Uploading a blob and committing the reference are separate operations. A
/// sweep may reclaim abandoned payloads, but never ones a client could still
/// be handing off to a draft or a turn.
pub const ASSET_SWEEP_GRACE_PERIOD: Duration = Duration::from_secs(60 * 60);

/// Where the daemon keeps its data: the orchestrator's `state.sqlite`,
/// `blobs/`, `attachments/`, the composer drafts and the cleanup state.
///
/// Debug builds keep it in the checkout's gitignored `temp/`, so development
/// never touches the installed app's data and a bad state is thrown away by
/// deleting one directory. Release builds use the usual per-user
/// application support directory.
pub fn default_data_dir() -> PathBuf {
    if cfg!(debug_assertions) {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")))
            .join("temp")
    } else {
        dirs::data_local_dir()
            .unwrap_or_else(std::env::temp_dir)
            .join(DATA_DIRECTORY_NAME)
    }
}

/// Small, independently persisted composer state: one atomic JSON
/// document, written only with the keys a client changed.
#[derive(Clone)]
pub struct ComposerDraftStore {
    path: PathBuf,
    latest_write: Arc<Mutex<u64>>,
}

impl ComposerDraftStore {
    pub fn in_dir(data_dir: &Path) -> Self {
        Self {
            path: data_dir.join(COMPOSER_DRAFTS_FILENAME),
            latest_write: Arc::new(Mutex::new(0)),
        }
    }

    pub fn load(&self) -> io::Result<ComposerDrafts> {
        let bytes = match fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(ComposerDrafts::default());
            }
            Err(error) => return Err(error),
        };
        serde_json::from_slice(&bytes).map_err(to_io_error)
    }

    /// Write a complete snapshot atomically. Older writes become no-ops if
    /// a newer generation reached the store first.
    pub fn save(&self, drafts: ComposerDrafts, generation: u64) -> io::Result<()> {
        let data = serde_json::to_vec_pretty(&drafts).map_err(to_io_error)?;
        let mut latest_write = self.latest_write.lock();
        if generation < *latest_write {
            return Ok(());
        }
        self.replace(&data)?;
        *latest_write = generation;
        Ok(())
    }

    /// Apply only the drafts a client actually changed. The mutation and
    /// the atomic file replacement share one lock, so concurrent clients
    /// cannot clobber unrelated draft keys.
    pub fn apply_changes(&self, changes: Vec<ComposerDraftChange>) -> io::Result<()> {
        if changes.is_empty() {
            return Ok(());
        }
        let mut latest_write = self.latest_write.lock();
        let mut drafts = self.load()?;
        let mut changed = false;
        for change in changes {
            let key = ComposerDraftKey::from(change.target);
            changed |= match change.draft {
                Some(draft) => drafts.set(key, draft),
                None => drafts.remove(key),
            };
        }
        if !changed {
            return Ok(());
        }
        let data = serde_json::to_vec_pretty(&drafts).map_err(to_io_error)?;
        self.replace(&data)?;
        *latest_write = latest_write.saturating_add(1);
        Ok(())
    }

    fn replace(&self, data: &[u8]) -> io::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = self.path.with_extension("json.tmp");
        fs::write(&temporary, data)?;
        fs::rename(temporary, &self.path)
    }
}

/// `waku-blob:` and `waku-attachment:` references something still holds.
#[derive(Debug, Default)]
pub struct AssetReferences {
    pub blobs: HashSet<String>,
    pub attachments: HashSet<String>,
}

impl AssetReferences {
    /// Adds every reference named in `text` (JSON or any other text that
    /// ends a reference with `"`).
    pub fn collect(&mut self, text: &str) {
        collect_references(text, crate::blob_store::BLOB_SCHEME, &mut self.blobs);
        collect_references(
            text,
            crate::attachments::ATTACHMENT_SCHEME,
            &mut self.attachments,
        );
    }
}

/// Deletes blobs and attachment trees older than
/// [`ASSET_SWEEP_GRACE_PERIOD`] that nothing references: not the composer
/// drafts, not the caller's references (the orchestrator's, #55). A source
/// that cannot be read stops the sweep rather than counting as empty.
pub struct AssetSweep {
    blobs: Arc<BlobStore>,
    attachments_root: PathBuf,
    drafts: PathBuf,
}

impl AssetSweep {
    /// The sweep of the stores in `data_dir`.
    pub fn in_dir(data_dir: &Path, blobs: Arc<BlobStore>) -> Self {
        Self {
            blobs,
            attachments_root: data_dir.join("attachments"),
            drafts: data_dir.join(COMPOSER_DRAFTS_FILENAME),
        }
    }

    pub fn run(&self, extra: &AssetReferences) {
        let cutoff = SystemTime::now()
            .checked_sub(ASSET_SWEEP_GRACE_PERIOD)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let drafts = match fs::read_to_string(&self.drafts) {
            Ok(drafts) => drafts,
            Err(error) if error.kind() == io::ErrorKind::NotFound => String::new(),
            Err(_) => return,
        };
        let mut live = AssetReferences::default();
        live.collect(&drafts);
        live.blobs.extend(extra.blobs.iter().cloned());
        live.attachments.extend(extra.attachments.iter().cloned());
        let _ = self
            .blobs
            .retain_unreferenced_older_than(&live.blobs, cutoff);
        let _ = crate::attachments::AttachmentStore::new(self.attachments_root.clone())
            .retain_unreferenced_older_than(&live.attachments, cutoff);
    }
}

/// Scanning raw text keeps retention independent of how deeply a reference
/// is nested in a turn item or a draft.
fn collect_references(data: &str, scheme: &str, references: &mut HashSet<String>) {
    let mut rest = data;
    while let Some(start) = rest.find(scheme) {
        rest = &rest[start..];
        let end = rest.find('"').unwrap_or(rest.len());
        references.insert(rest[..end].to_owned());
        rest = &rest[end..];
    }
}

fn to_io_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn temporary_directory() -> PathBuf {
        std::env::temp_dir().join(format!("waku-state-{}", Uuid::new_v4()))
    }

    fn text_draft(text: &str) -> ComposerDraft {
        ComposerDraft {
            text: text.to_owned(),
            attachments: Vec::new(),
        }
    }

    #[test]
    fn composer_project_change_moves_a_draft_only_to_an_empty_destination() {
        let source = ComposerDraftKey::NewSession(Uuid::new_v4());
        let destination = ComposerDraftKey::NewSession(Uuid::new_v4());
        let draft = text_draft("keep this prompt");
        let mut drafts = ComposerDrafts::default();
        drafts.set(source, draft.clone());

        assert!(drafts.move_to_empty(source, destination));
        assert!(drafts.get(source).is_none());
        assert_eq!(drafts.get(destination), Some(&draft));

        let occupied = ComposerDraftKey::NewSession(Uuid::new_v4());
        let parked = text_draft("already parked here");
        drafts.set(occupied, parked.clone());
        assert!(!drafts.move_to_empty(destination, occupied));
        assert_eq!(drafts.get(destination), Some(&draft));
        assert_eq!(drafts.get(occupied), Some(&parked));
    }

    #[test]
    fn composer_drafts_round_trip_text_and_attachment_metadata() {
        let directory = temporary_directory();
        let store = ComposerDraftStore::in_dir(&directory);
        let project_id = Uuid::new_v4();
        let draft = ComposerDraft {
            text: "compare these".to_owned(),
            attachments: vec![ComposerDraftAttachment {
                path: PathBuf::from("/tmp/reference image.png"),
                mention: "/tmp/reference image.png".to_owned(),
                name: "reference image.png".to_owned(),
                is_dir: false,
                is_image: true,
                blob_reference: None,
            }],
        };
        let mut drafts = ComposerDrafts::default();
        drafts.set(ComposerDraftKey::NewSession(project_id), draft.clone());

        store.save(drafts, 1).unwrap();
        let restored = store.load().unwrap();
        assert_eq!(
            restored.get(ComposerDraftKey::NewSession(project_id)),
            Some(&draft)
        );
        fs::remove_dir_all(directory).ok();
    }

    #[test]
    fn composer_draft_changes_preserve_unrelated_client_keys() {
        let directory = temporary_directory();
        let store = ComposerDraftStore::in_dir(&directory);
        let desktop_thread_id = Uuid::new_v4();
        let web_project_id = Uuid::new_v4();
        let desktop_draft = text_draft("desktop follow-up");
        let web_draft = text_draft("web new task");
        let mut initial = ComposerDrafts::default();
        initial.set(
            ComposerDraftKey::Session(desktop_thread_id),
            desktop_draft.clone(),
        );
        store.save(initial, 1).unwrap();

        store
            .apply_changes(vec![ComposerDraftChange {
                target: ComposerDraftTarget::NewSession {
                    project_id: web_project_id,
                },
                draft: Some(web_draft.clone()),
            }])
            .unwrap();

        let restored = store.load().unwrap();
        assert_eq!(
            restored.get(ComposerDraftKey::Session(desktop_thread_id)),
            Some(&desktop_draft)
        );
        assert_eq!(
            restored.get(ComposerDraftKey::NewSession(web_project_id)),
            Some(&web_draft)
        );
        fs::remove_dir_all(directory).ok();
    }

    #[test]
    fn empty_and_older_composer_drafts_cannot_resurface() {
        let directory = temporary_directory();
        let store = ComposerDraftStore::in_dir(&directory);
        let key = ComposerDraftKey::Session(Uuid::new_v4());
        let mut latest = ComposerDrafts::default();
        latest.set(key, text_draft("latest"));
        store.save(latest, 2).unwrap();

        let mut stale = ComposerDrafts::default();
        stale.set(key, text_draft("stale"));
        store.save(stale, 1).unwrap();
        assert_eq!(store.load().unwrap().get(key), Some(&text_draft("latest")));

        let mut removed = store.load().unwrap();
        assert!(removed.set(key, ComposerDraft::default()));
        assert!(!removed.set(key, ComposerDraft::default()));
        store.save(removed, 3).unwrap();
        assert!(store.load().unwrap().get(key).is_none());
        fs::remove_dir_all(directory).ok();
    }

    #[test]
    fn references_are_collected_from_any_text() {
        let mut references = AssetReferences::default();
        references.collect(
            r#"{"image":"waku-blob:0123456789abcdef.png","file":"waku-attachment:abc","n":"waku-blob:fedcba9876543210.jpg"}"#,
        );
        assert_eq!(
            references.blobs,
            HashSet::from([
                "waku-blob:0123456789abcdef.png".to_owned(),
                "waku-blob:fedcba9876543210.jpg".to_owned(),
            ])
        );
        assert_eq!(
            references.attachments,
            HashSet::from(["waku-attachment:abc".to_owned()])
        );
    }

    #[test]
    fn default_data_dir_is_build_specific() {
        let directory = default_data_dir();
        // Debug data stays inside the checkout so development cannot read or
        // write the installed app's.
        #[cfg(debug_assertions)]
        {
            assert_eq!(directory.file_name(), Some(std::ffi::OsStr::new("temp")));
            let checkout = Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .and_then(Path::parent)
                .unwrap();
            assert!(directory.starts_with(checkout));
        }
        #[cfg(not(debug_assertions))]
        assert_eq!(
            directory.file_name(),
            Some(std::ffi::OsStr::new(DATA_DIRECTORY_NAME))
        );
    }
}
