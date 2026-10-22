//! The daemon's [`Backend`]: settings, providers, usage, skills, composer
//! drafts, blobs and attachments, workspaces and terminals, for protocol
//! v8's non-orchestration methods; and the worktree cleanup scheduler.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use anyhow::{anyhow, bail};
use parking_lot::Mutex;
use uuid::Uuid;
use waku_protocol::storage::{
    StorageSettings, WorktreeCleanupReason, WorktreeCleanupReport, WorktreeCleanupStatus,
    WorktreeCleanupTrigger,
};

use crate::Backend;
use crate::attachments::{AttachmentStore, AttachmentUpload, StoredAttachment};
use crate::blob_store::BlobStore;
use crate::computer_use::ComputerPermissions;
use crate::model::{ProviderKind, ProviderProbe, ProviderSessionSummary};
use crate::persistence::{AssetSweep, ComposerDraftChange, ComposerDraftStore, ComposerDrafts};
use crate::settings::{DaemonSettings, DaemonSettingsStore};
use crate::skills::SkillsCatalog;
use crate::terminal::{DaemonTerminal, TerminalEvents, TerminalSnapshot};
use crate::usage::PlanUsage;
use crate::usage_history::{UsageHistory, UsageWindow};
use crate::workspace::{WorkspaceOperation, WorkspaceResult};
use crate::worktree_cleanup::{TaskWorktree, WorktreeCleanup};

/// The most provider conversations one `providerSessions.list` returns.
const MAX_PROVIDER_SESSIONS: usize = 500;

/// Called with the new settings after a client changed them.
pub type SettingsListener = Box<dyn Fn(&DaemonSettings) + Send + Sync>;

pub struct WakuBackend {
    /// Shared with the orchestrator's cleanup port, which closes a thread's
    /// terminals when it is archived, deleted or settled.
    terminals: Arc<crate::orchestrator_ports::TerminalMap>,
    #[cfg(all(test, unix))]
    terminal_shell: Option<alacritty_terminal::tty::Shell>,
    settings: DaemonSettingsStore,
    data_dir: PathBuf,
    blobs: Arc<BlobStore>,
    composer_drafts: ComposerDraftStore,
    attachments: AttachmentStore,
    usage_scan_cache: Mutex<crate::usage_history::ScanCache>,
    default_cwd: PathBuf,
    worktree_cleanup: Arc<WorktreeCleanup>,
    settings_listener: Mutex<Option<SettingsListener>>,
    /// The daemon host's blob sweeper (#55): stores request a sweep through
    /// it once one is attached.
    blob_sweeps: Mutex<Option<crate::blob_sweep::SweepHandle>>,
}

impl WakuBackend {
    /// The backend of the data in `data_dir` (see
    /// [`crate::persistence::default_data_dir`]).
    pub fn new(settings: DaemonSettingsStore, data_dir: PathBuf) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&data_dir)?;
        Ok(Self {
            terminals: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(all(test, unix))]
            terminal_shell: None,
            settings,
            blobs: Arc::new(BlobStore::new(data_dir.join("blobs"))),
            composer_drafts: ComposerDraftStore::in_dir(&data_dir),
            attachments: AttachmentStore::new(data_dir.join("attachments")),
            usage_scan_cache: Mutex::new(HashMap::new()),
            default_cwd: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            worktree_cleanup: Arc::new(WorktreeCleanup::open(
                data_dir.join("worktree-cleanup.json"),
            )),
            settings_listener: Mutex::new(None),
            blob_sweeps: Mutex::new(None),
            data_dir,
        })
    }

    /// The directory the daemon keeps its data in: the orchestrator's
    /// `state.sqlite`, `blobs/`, `attachments/` and the drafts.
    pub fn data_dir(&self) -> PathBuf {
        self.data_dir.clone()
    }

    /// The daemon settings as they are now.
    pub fn daemon_settings(&self) -> DaemonSettings {
        self.settings.get()
    }

    /// The open terminals, for the orchestrator's cleanup port.
    pub fn terminal_map(&self) -> Arc<crate::orchestrator_ports::TerminalMap> {
        self.terminals.clone()
    }

    /// Sweeps go through `sweeps` from now on: stores request them.
    pub fn set_blob_sweeps(&self, sweeps: crate::blob_sweep::SweepHandle) {
        *self.blob_sweeps.lock() = Some(sweeps);
    }

    /// The blob and attachment stores' sweep, for the daemon host's
    /// sweeper, which adds the orchestrator's references.
    pub fn asset_sweep(&self) -> AssetSweep {
        AssetSweep::in_dir(&self.data_dir, self.blobs.clone())
    }

    fn request_sweep(&self, reason: crate::blob_sweep::SweepReason) {
        if let Some(sweeps) = &*self.blob_sweeps.lock() {
            sweeps.request(reason);
        }
    }

    /// Runs `listener` after every settings change (the orchestrator host
    /// re-registers provider instances). Replaces an earlier listener.
    pub fn on_settings_changed(&self, listener: SettingsListener) {
        *self.settings_listener.lock() = Some(listener);
    }

    #[cfg(all(test, unix))]
    pub(crate) fn with_terminal_shell(mut self, shell: alacritty_terminal::tty::Shell) -> Self {
        self.terminal_shell = Some(shell);
        self
    }

    /// The open terminal `id`. The terminal is shared out of the map so slow
    /// work on it (a restart waiting out a shell's exit) never holds up the
    /// other terminals.
    fn terminal(&self, id: Uuid) -> anyhow::Result<Arc<DaemonTerminal>> {
        self.terminals
            .lock()
            .get(&id)
            .cloned()
            .ok_or_else(|| anyhow!("daemon terminal {id} is not running"))
    }

    fn spawn_terminal(
        &self,
        cwd: &Path,
        cols: u16,
        rows: u16,
        events: TerminalEvents,
    ) -> anyhow::Result<DaemonTerminal> {
        #[cfg(all(test, unix))]
        if let Some(shell) = &self.terminal_shell {
            return DaemonTerminal::open_with_shell(cwd, cols, rows, events, shell.clone());
        }
        ensure_shell_environment();
        DaemonTerminal::open(cwd, cols, rows, events)
    }

    /// Apply the saved cleanup rules once and return the report.
    pub fn run_worktree_cleanup(&self, trigger: WorktreeCleanupTrigger) -> WorktreeCleanupReport {
        let storage = self.settings.get().storage;
        self.worktree_cleanup.run(self, &storage, trigger)
    }
}

impl Backend for WakuBackend {
    fn settings(&self) -> anyhow::Result<DaemonSettings> {
        Ok(self.settings.get())
    }

    fn update_settings(&self, mut settings: DaemonSettings) -> anyhow::Result<()> {
        let previous = self.settings.get();
        settings.storage = merge_storage_settings(&previous.storage, settings.storage);
        let storage_changed = previous.storage != settings.storage;
        self.settings.replace(settings)?;
        if storage_changed {
            self.worktree_cleanup.wake_scheduler();
        }
        if let Some(listener) = &*self.settings_listener.lock() {
            listener(&self.settings.get());
        }
        Ok(())
    }

    fn default_cwd(&self) -> anyhow::Result<PathBuf> {
        Ok(self.default_cwd.clone())
    }

    fn worktree_cleanup_status(&self) -> anyhow::Result<WorktreeCleanupStatus> {
        Ok(self.worktree_cleanup.status(&self.settings.get().storage))
    }

    fn run_worktree_cleanup(&self) -> anyhow::Result<WorktreeCleanupReport> {
        Ok(WakuBackend::run_worktree_cleanup(
            self,
            WorktreeCleanupTrigger::Manual,
        ))
    }

    fn probe_provider(
        &self,
        provider: ProviderKind,
        binary_override: Option<String>,
        discover_models: bool,
        probe_version: bool,
    ) -> anyhow::Result<(ProviderProbe, Option<String>)> {
        ensure_shell_environment();
        let mut probe = if discover_models || probe_version {
            crate::model::provider_probe(provider, binary_override.as_deref())
        } else {
            crate::model::cached_provider_probe(provider, binary_override.as_deref())
        };
        let version = probe_version
            .then(|| {
                probe
                    .path
                    .as_deref()
                    .and_then(crate::model::probe_provider_version)
            })
            .flatten();
        if discover_models {
            probe = crate::model::discover_provider_models(probe);
        }
        Ok((probe, version))
    }

    fn plan_usage(
        &self,
        provider: ProviderKind,
        binary_override: Option<String>,
        cli_version: Option<String>,
    ) -> anyhow::Result<Option<PlanUsage>> {
        Ok(match provider {
            ProviderKind::Claude => Some(crate::usage::fetch_claude_plan_usage(
                cli_version.as_deref(),
            )?),
            ProviderKind::Codex => Some(crate::usage::fetch_codex_plan_usage()?),
            ProviderKind::OpenCode => crate::usage::fetch_opencode_go_plan_usage()?,
            ProviderKind::Grok => {
                ensure_shell_environment();
                let probe = crate::model::provider_probe(provider, binary_override.as_deref());
                let binary = probe.path.ok_or_else(|| anyhow!("grok is not installed"))?;
                Some(crate::usage::fetch_grok_plan_usage(&binary)?)
            }
            _ => bail!("provider has no plan usage fetcher"),
        })
    }

    fn computer_permissions(&self, prompt: bool) -> anyhow::Result<ComputerPermissions> {
        crate::computer_use::probe_permissions(prompt)
    }

    fn usage_history(
        &self,
        window: UsageWindow,
        project_roots: Vec<PathBuf>,
    ) -> anyhow::Result<UsageHistory> {
        let rates = crate::usage_history::load_rate_table(&self.data_dir);
        Ok(crate::usage_history::scan(
            &mut self.usage_scan_cache.lock(),
            &rates,
            window,
            &project_roots,
        ))
    }

    fn skills(&self, projects: Vec<(String, PathBuf)>) -> anyhow::Result<SkillsCatalog> {
        let locations = crate::skills::skill_locations(&projects);
        Ok(crate::skills::scan_skills(&locations))
    }

    fn set_skills_enabled(&self, dirs: Vec<PathBuf>, enabled: bool) -> anyhow::Result<()> {
        for dir in dirs {
            crate::skills::set_skill_enabled(&dir, enabled).map_err(|error| anyhow!(error))?;
        }
        Ok(())
    }

    fn trash_skills(&self, dirs: Vec<PathBuf>) -> anyhow::Result<()> {
        crate::skills::trash_skills(&dirs).map_err(|error| anyhow!(error))
    }

    fn provider_sessions(
        &self,
        provider: ProviderKind,
        limit: usize,
    ) -> anyhow::Result<Vec<ProviderSessionSummary>> {
        let limit = limit.min(MAX_PROVIDER_SESSIONS);
        if limit == 0 {
            return Ok(Vec::new());
        }
        ensure_shell_environment();
        let settings = self.settings.get();
        if settings.disabled_providers.contains(&provider) {
            return Ok(Vec::new());
        }
        let binary_override = settings
            .provider_binary_overrides
            .get(&provider)
            .map(String::as_str);
        let Some(binary) = crate::model::provider_probe(provider, binary_override).path else {
            return Ok(Vec::new());
        };
        // Discovery is deliberately provider-scoped. Opening Resume must not
        // start every installed agent CLI, and another provider is queried
        // only after the user explicitly picks it.
        let mut sessions = match provider {
            ProviderKind::Amp => crate::amp_session::list_provider_sessions(&binary, limit)?,
            ProviderKind::Claude => crate::claude_session::list_provider_sessions(limit)?,
            ProviderKind::Codex => crate::codex_session::list_provider_sessions(&binary, limit)?,
            ProviderKind::Cursor | ProviderKind::Fx => {
                crate::acp_session::list_provider_sessions(provider, &binary, &[], limit)?
            }
            ProviderKind::OpenCode => {
                crate::opencode_session::list_provider_sessions(&binary, limit)?
            }
            ProviderKind::DeepSeek => {
                crate::deepseek_session::list_provider_sessions(&binary, limit)?
            }
            ProviderKind::Grok => crate::grok_session::list_provider_sessions(limit)?,
            ProviderKind::Kimi => crate::kimi_session::list_provider_sessions(limit)?,
            ProviderKind::OhMyPi | ProviderKind::Pi => {
                crate::pi_session::list_provider_sessions(provider, limit)?
            }
        };
        sessions.sort_by(|a, b| {
            b.updated_at
                .cmp(&a.updated_at)
                .then_with(|| a.title.cmp(&b.title))
        });
        sessions.truncate(limit);
        Ok(sessions)
    }

    fn composer_drafts(&self) -> anyhow::Result<ComposerDrafts> {
        Ok(self.composer_drafts.load()?)
    }

    fn apply_composer_draft_changes(
        &self,
        changes: Vec<ComposerDraftChange>,
    ) -> anyhow::Result<()> {
        Ok(self.composer_drafts.apply_changes(changes)?)
    }

    fn store_blob(&self, mime_type: &str, bytes: &[u8]) -> anyhow::Result<(String, PathBuf)> {
        let reference = self.blobs.store_image_bytes(mime_type, bytes)?;
        let path = self
            .blobs
            .path_for(&reference)
            .ok_or_else(|| anyhow!("stored blob has no daemon path"))?;
        self.request_sweep(crate::blob_sweep::SweepReason::Stored);
        Ok((reference, path))
    }

    fn read_blob(&self, reference: &str) -> anyhow::Result<Vec<u8>> {
        let path = self
            .blobs
            .path_for(reference)
            .ok_or_else(|| anyhow!("invalid blob reference"))?;
        Ok(std::fs::read(path)?)
    }

    fn locate_blob(&self, reference: &str) -> anyhow::Result<PathBuf> {
        let path = self
            .blobs
            .path_for(reference)
            .ok_or_else(|| anyhow!("invalid blob reference"))?;
        if !path.is_file() {
            bail!("blob {reference} does not exist");
        }
        Ok(path)
    }

    fn import_attachment(
        &self,
        name: &str,
        upload: AttachmentUpload,
    ) -> anyhow::Result<StoredAttachment> {
        let attachment = self.attachments.import(name, upload)?;
        self.request_sweep(crate::blob_sweep::SweepReason::Stored);
        Ok(attachment)
    }

    fn import_path_attachment(&self, path: &Path) -> anyhow::Result<StoredAttachment> {
        let attachment = self.attachments.import_path(path)?;
        self.request_sweep(crate::blob_sweep::SweepReason::Stored);
        Ok(attachment)
    }

    fn read_attachment(&self, reference: &str, path: &Path) -> anyhow::Result<Vec<u8>> {
        Ok(self.attachments.read_file(reference, path)?)
    }

    fn locate_attachment(&self, reference: &str, path: &Path) -> anyhow::Result<PathBuf> {
        Ok(self.attachments.locate_file(reference, path)?)
    }

    fn workspace(&self, operation: WorkspaceOperation) -> anyhow::Result<WorkspaceResult> {
        if let WorkspaceOperation::CreateWorktree { .. } = &operation {
            let root = crate::worktree_cleanup::worktrees_directory(&self.settings.get().storage)?;
            return crate::workspace::execute_in(operation, Some(&root));
        }
        crate::workspace::execute(operation)
    }

    fn open_terminal(
        &self,
        id: Uuid,
        cwd: &Path,
        cols: u16,
        rows: u16,
        events: TerminalEvents,
    ) -> anyhow::Result<()> {
        {
            let terminals = self.terminals.lock();
            if terminals.len() >= crate::terminal::MAX_TERMINALS && !terminals.contains_key(&id) {
                bail!(
                    "the daemon already has {} terminals open; close one first",
                    crate::terminal::MAX_TERMINALS
                );
            }
        }
        let terminal = Arc::new(self.spawn_terminal(cwd, cols, rows, events)?);
        // Bound so a replaced terminal shuts down after the lock is released.
        let _previous = self.terminals.lock().insert(id, terminal);
        Ok(())
    }

    fn write_terminal(&self, id: Uuid, data: Vec<u8>) -> anyhow::Result<()> {
        self.terminal(id)?.write(data)
    }

    fn resize_terminal(&self, id: Uuid, cols: u16, rows: u16) -> anyhow::Result<()> {
        self.terminal(id)?.resize(cols, rows);
        Ok(())
    }

    fn attach_terminal(
        &self,
        id: Uuid,
        cols: Option<u16>,
        rows: Option<u16>,
        max_replay_bytes: Option<u32>,
    ) -> anyhow::Result<TerminalSnapshot> {
        let terminal = self.terminal(id)?;
        if let (Some(cols), Some(rows)) = (cols, rows) {
            terminal.resize(cols, rows);
        }
        Ok(terminal.snapshot(max_replay_bytes))
    }

    fn clear_terminal(&self, id: Uuid) -> anyhow::Result<()> {
        self.terminal(id)?.clear();
        Ok(())
    }

    fn restart_terminal(&self, id: Uuid) -> anyhow::Result<TerminalSnapshot> {
        self.terminal(id)?.restart()
    }

    fn close_terminal(&self, id: Uuid) -> anyhow::Result<()> {
        // Dropped outside the lock: closing waits out the shell's exit.
        let _removed = self.terminals.lock().remove(&id);
        Ok(())
    }

    fn shutdown(&self) {
        let terminals = std::mem::take(&mut *self.terminals.lock());
        drop(terminals);
    }
}

/// Orchestrator threads remove the worktree they own when they are deleted
/// (RFC 0001), under the same safety rules (`GitWorkspacePort`), so this
/// scheduled pass has no tasks of its own to consider yet.
impl crate::worktree_cleanup::CleanupHost for WakuBackend {
    fn tasks(&self) -> anyhow::Result<Vec<TaskWorktree>> {
        Ok(Vec::new())
    }

    fn blocked(&self, _task_ids: &[Uuid]) -> Option<WorktreeCleanupReason> {
        None
    }
}

/// Start the thread that runs worktree cleanup on daemon start and then at
/// most every `interval_hours`. It sleeps until the next run is due, or
/// until a settings change wakes it while no rule is enabled, so an idle
/// daemon is not woken for it. The thread holds only the cleanup state and
/// a weak backend reference, and exits once the backend is gone.
pub fn start_worktree_cleanup_scheduler(backend: &Arc<WakuBackend>) -> std::io::Result<()> {
    let weak = Arc::downgrade(backend);
    let cleanup = backend.worktree_cleanup.clone();
    std::thread::Builder::new()
        .name("waku-worktree-cleanup".into())
        .spawn(move || {
            loop {
                let seen = cleanup.wake_generation();
                let Some(storage) = weak.upgrade().map(|backend| backend.settings.get().storage)
                else {
                    return;
                };
                let delay =
                    cleanup.automatic_delay(&storage, crate::worktree_cleanup::now_seconds());
                if cleanup.wait(seen, delay) {
                    continue;
                }
                let Some(backend) = weak.upgrade() else {
                    return;
                };
                let report = backend.run_worktree_cleanup(WorktreeCleanupTrigger::Automatic);
                if report.removed > 0 || report.failed > 0 {
                    eprintln!(
                        "Waku worktree cleanup removed {}, kept {}, failed {}",
                        report.removed, report.kept, report.failed
                    );
                }
            }
        })?;
    Ok(())
}

/// Apply a client's storage settings without losing earlier worktree
/// locations: a client may send a stale list, and the location it moves
/// away from must stay managed so cleanup can still reach its worktrees.
fn merge_storage_settings(previous: &StorageSettings, next: StorageSettings) -> StorageSettings {
    let mut locations = Vec::new();
    if next.worktrees_directory != previous.worktrees_directory
        && let Some(old) = &previous.worktrees_directory
    {
        locations.push(old.clone());
    }
    locations.extend(next.previous_worktrees_directories.iter().cloned());
    locations.extend(previous.previous_worktrees_directories.iter().cloned());
    StorageSettings {
        previous_worktrees_directories: locations,
        ..next
    }
    .normalized(dirs::home_dir().as_deref())
}

/// Loads the login shell's environment once (provider CLIs run with it).
pub(crate) fn ensure_shell_environment() {
    static REFRESHED: OnceLock<()> = OnceLock::new();
    REFRESHED.get_or_init(|| {
        crate::command_env::refresh_from_default_shell();
    });
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::workspace::WorkspaceResult;

    fn git(cwd: &Path, args: &[&str]) {
        crate::git_workflow::git(cwd, args).unwrap();
    }

    #[test]
    fn storage_settings_keep_earlier_worktree_locations() {
        let previous = StorageSettings {
            worktrees_directory: Some(PathBuf::from("/first")),
            previous_worktrees_directories: vec![PathBuf::from("/zero")],
            ..StorageSettings::default()
        };
        // A client with a stale list moves the folder.
        let next = StorageSettings {
            worktrees_directory: Some(PathBuf::from("/second")),
            ..StorageSettings::default()
        };
        let merged = merge_storage_settings(&previous, next);
        assert_eq!(merged.worktrees_directory, Some(PathBuf::from("/second")));
        assert_eq!(
            merged.previous_worktrees_directories,
            vec![PathBuf::from("/first"), PathBuf::from("/zero")]
        );
        // Moving back drops the current location from the earlier ones.
        let back = merge_storage_settings(
            &merged,
            StorageSettings {
                worktrees_directory: Some(PathBuf::from("/first")),
                ..StorageSettings::default()
            },
        );
        assert_eq!(
            back.previous_worktrees_directories,
            vec![PathBuf::from("/second"), PathBuf::from("/zero")]
        );
    }

    #[test]
    fn worktrees_go_to_the_configured_folder_and_cleanup_reports_its_runs() {
        let root = std::env::temp_dir().join(format!("waku-daemon-cleanup-{}", Uuid::new_v4()));
        let repository = root.join("repository");
        std::fs::create_dir_all(&repository).unwrap();
        git(&repository, &["init", "-b", "main"]);
        std::fs::write(repository.join("README.md"), "main\n").unwrap();
        git(&repository, &["add", "-A", "--", "."]);
        git(
            &repository,
            &[
                "-c",
                "user.name=Waku Tests",
                "-c",
                "user.email=waku@example.com",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-m",
                "initial",
            ],
        );
        let backend = WakuBackend::new(
            DaemonSettingsStore::open(root.join("settings.json")).unwrap(),
            root.join("data"),
        )
        .unwrap();

        let mut settings = Backend::settings(&backend).unwrap();
        let trees = root.join("trees");
        settings.storage.worktrees_directory = Some(trees.clone());
        settings.storage.worktree_cleanup.deleted_tasks = true;
        backend.update_settings(settings).unwrap();

        let WorkspaceResult::WorktreeCreated { worktree } = backend
            .workspace(WorkspaceOperation::CreateWorktree {
                project_path: repository.clone(),
                project_id: Uuid::new_v4(),
                session_id: Uuid::new_v4(),
                prompt: "Clean me up".into(),
                base_branch: None,
            })
            .unwrap()
        else {
            panic!("expected a created worktree");
        };
        assert!(worktree.path.starts_with(&trees), "{worktree:?}");

        let report = Backend::run_worktree_cleanup(&backend).unwrap();
        assert_eq!(report.removed, 0, "{report:#?}");
        // A live thread's worktree is never cleanup's to take.
        assert!(worktree.path.exists());

        let status = backend.worktree_cleanup_status().unwrap();
        assert_eq!(status.last_report, Some(report));
        assert!(status.last_run_at.is_some());
        assert!(status.next_run_at.is_some());
        assert!(!status.running);
        std::fs::remove_dir_all(root).unwrap();
    }
}
