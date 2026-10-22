//! What protocol v8's non-orchestration methods call into: settings,
//! providers, usage, skills, composer drafts, blobs and attachments,
//! workspaces and the PTY host.
//!
//! [`crate::daemon::WakuBackend`] is the daemon's. Every method is
//! synchronous (git, filesystem, provider probes) and runs on the server's
//! blocking pool. Tests substitute small fakes, so every method has a
//! default that answers "not supported".

use std::path::{Path, PathBuf};

use uuid::Uuid;
use waku_protocol::storage::{WorktreeCleanupReport, WorktreeCleanupStatus};

use crate::attachments::{AttachmentUpload, StoredAttachment};
use crate::computer_use::ComputerPermissions;
use crate::model::{ProviderKind, ProviderProbe, ProviderSessionSummary};
use crate::persistence::{ComposerDraftChange, ComposerDrafts};
use crate::settings::DaemonSettings;
use crate::skills::SkillsCatalog;
use crate::terminal::{TerminalEvents, TerminalSnapshot};
use crate::usage::PlanUsage;
use crate::usage_history::{UsageHistory, UsageWindow};
use crate::workspace::{WorkspaceOperation, WorkspaceResult};

fn unsupported() -> anyhow::Error {
    anyhow::anyhow!("not supported by this daemon backend")
}

pub trait Backend: Send + Sync + 'static {
    /// The daemon settings as they are now.
    fn settings(&self) -> anyhow::Result<DaemonSettings> {
        Err(unsupported())
    }

    /// Replace the daemon settings.
    fn update_settings(&self, _settings: DaemonSettings) -> anyhow::Result<()> {
        Err(unsupported())
    }

    /// The daemon's working directory, where a client without a project
    /// starts.
    fn default_cwd(&self) -> anyhow::Result<PathBuf> {
        Err(unsupported())
    }

    fn worktree_cleanup_status(&self) -> anyhow::Result<WorktreeCleanupStatus> {
        Err(unsupported())
    }

    /// Run worktree cleanup now with the saved rules ("Delete now"). Waits
    /// for a run already in progress first.
    fn run_worktree_cleanup(&self) -> anyhow::Result<WorktreeCleanupReport> {
        Err(unsupported())
    }

    /// Probe `provider`'s CLI; `discover_models` also asks it for its
    /// models, `probe_version` for its version.
    fn probe_provider(
        &self,
        _provider: ProviderKind,
        _binary_override: Option<String>,
        _discover_models: bool,
        _probe_version: bool,
    ) -> anyhow::Result<(ProviderProbe, Option<String>)> {
        Err(unsupported())
    }

    fn plan_usage(
        &self,
        _provider: ProviderKind,
        _binary_override: Option<String>,
        _cli_version: Option<String>,
    ) -> anyhow::Result<Option<PlanUsage>> {
        Err(unsupported())
    }

    fn computer_permissions(&self, _prompt: bool) -> anyhow::Result<ComputerPermissions> {
        Err(unsupported())
    }

    fn usage_history(
        &self,
        _window: UsageWindow,
        _project_roots: Vec<PathBuf>,
    ) -> anyhow::Result<UsageHistory> {
        Err(unsupported())
    }

    /// The skills of the user and of `projects` (name, root).
    fn skills(&self, _projects: Vec<(String, PathBuf)>) -> anyhow::Result<SkillsCatalog> {
        Err(unsupported())
    }

    fn set_skills_enabled(&self, _dirs: Vec<PathBuf>, _enabled: bool) -> anyhow::Result<()> {
        Err(unsupported())
    }

    fn trash_skills(&self, _dirs: Vec<PathBuf>) -> anyhow::Result<()> {
        Err(unsupported())
    }

    /// One provider's resumable CLI conversations on the daemon host, at
    /// most `limit`, newest first.
    fn provider_sessions(
        &self,
        _provider: ProviderKind,
        _limit: usize,
    ) -> anyhow::Result<Vec<ProviderSessionSummary>> {
        Err(unsupported())
    }

    fn composer_drafts(&self) -> anyhow::Result<ComposerDrafts> {
        Err(unsupported())
    }

    fn apply_composer_draft_changes(
        &self,
        _changes: Vec<ComposerDraftChange>,
    ) -> anyhow::Result<()> {
        Err(unsupported())
    }

    /// Store an image blob; returns its reference and its path on the
    /// daemon host.
    fn store_blob(&self, _mime_type: &str, _bytes: &[u8]) -> anyhow::Result<(String, PathBuf)> {
        Err(unsupported())
    }

    fn read_blob(&self, _reference: &str) -> anyhow::Result<Vec<u8>> {
        Err(unsupported())
    }

    /// Where a blob lives on the daemon host; fails when it does not exist.
    fn locate_blob(&self, _reference: &str) -> anyhow::Result<PathBuf> {
        Err(unsupported())
    }

    fn import_attachment(
        &self,
        _name: &str,
        _upload: AttachmentUpload,
    ) -> anyhow::Result<StoredAttachment> {
        Err(unsupported())
    }

    fn import_path_attachment(&self, _path: &Path) -> anyhow::Result<StoredAttachment> {
        Err(unsupported())
    }

    /// A file inside a stored attachment.
    fn read_attachment(&self, _reference: &str, _path: &Path) -> anyhow::Result<Vec<u8>> {
        Err(unsupported())
    }

    /// Where a file inside a stored attachment lives on the daemon host.
    fn locate_attachment(&self, _reference: &str, _path: &Path) -> anyhow::Result<PathBuf> {
        Err(unsupported())
    }

    fn workspace(&self, _operation: WorkspaceOperation) -> anyhow::Result<WorkspaceResult> {
        Err(unsupported())
    }

    /// Open terminal `id` in `cwd`; its output, resets, exit and errors go
    /// to `events`. Replaces a terminal already open under `id`.
    fn open_terminal(
        &self,
        _id: Uuid,
        _cwd: &Path,
        _cols: u16,
        _rows: u16,
        _events: TerminalEvents,
    ) -> anyhow::Result<()> {
        Err(unsupported())
    }

    fn write_terminal(&self, _id: Uuid, _data: Vec<u8>) -> anyhow::Result<()> {
        Err(unsupported())
    }

    fn resize_terminal(&self, _id: Uuid, _cols: u16, _rows: u16) -> anyhow::Result<()> {
        Err(unsupported())
    }

    /// Terminal `id`'s state and a replay of at most `max_replay_bytes` of
    /// its retained output, after resizing it when both `cols` and `rows`
    /// are given.
    fn attach_terminal(
        &self,
        _id: Uuid,
        _cols: Option<u16>,
        _rows: Option<u16>,
        _max_replay_bytes: Option<u32>,
    ) -> anyhow::Result<TerminalSnapshot> {
        Err(unsupported())
    }

    /// Drop the terminal's retained output; its events get a reset.
    fn clear_terminal(&self, _id: Uuid) -> anyhow::Result<()> {
        Err(unsupported())
    }

    /// Replace the terminal's shell with a fresh one in the same directory.
    fn restart_terminal(&self, _id: Uuid) -> anyhow::Result<TerminalSnapshot> {
        Err(unsupported())
    }

    /// Close terminal `id`; closing one that is not open succeeds.
    fn close_terminal(&self, _id: Uuid) -> anyhow::Result<()> {
        Err(unsupported())
    }

    /// The server is stopping: release what the backend holds open.
    fn shutdown(&self) {}
}
