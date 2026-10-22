//! Daemon settings and storage: `settings.get`, `settings.update`, the
//! `settings` stream, `storage.*` and `daemon.info`.
//!
//! The desktop edits its daemon settings as the domain [`DaemonSettings`]
//! inside its own state; this module converts them to and from the wire
//! shape, writes them without ever blocking the UI thread, and reports
//! changes made by other clients.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, unbounded};
use parking_lot::{Condvar, Mutex};
use uuid::Uuid;

use super::DaemonV8;
use crate::v8::{
    self, AppGrant, DaemonInfo, NoParams, SettingsItem, SettingsPatch, SettingsSnapshot,
    SettingsSpec, SettingsUpdateParams, StorageRunCleanupParams, StreamEvent, StreamItem,
    SubscribeOptions, method, stream,
};
use waku_protocol::DaemonSettings;
use waku_protocol::computer_use::ComputerAppGrant;
use waku_protocol::model::ProviderKind;
use waku_protocol::storage as legacy;

/// Failed writes are retried after this.
const RETRY_INTERVAL: Duration = Duration::from_millis(500);
/// A lost settings stream is resubscribed after this.
const RESUBSCRIBE_INTERVAL: Duration = Duration::from_secs(1);
/// Confirmed writes whose echo the settings stream has not delivered yet.
const MAX_UNECHOED: usize = 16;

/// The provider a settings key names: its instance id, or the camelCase
/// name the daemon's settings snapshot still uses for a few kinds.
fn settings_provider(key: &str) -> Option<ProviderKind> {
    ProviderKind::ALL.into_iter().find(|kind| {
        kind.id() == key
            || serde_json::to_value(kind)
                .ok()
                .is_some_and(|name| name.as_str() == Some(key))
    })
}

fn keep_when_to_wire(value: legacy::WorktreeKeepWhen) -> v8::WorktreeKeepWhen {
    match value {
        legacy::WorktreeKeepWhen::AnyLocalFiles => v8::WorktreeKeepWhen::AnyLocalFiles,
        legacy::WorktreeKeepWhen::UncommittedChanges => v8::WorktreeKeepWhen::UncommittedChanges,
        legacy::WorktreeKeepWhen::TrackedChanges => v8::WorktreeKeepWhen::TrackedChanges,
    }
}

fn keep_when_from_wire(value: &v8::WorktreeKeepWhen) -> legacy::WorktreeKeepWhen {
    match value {
        v8::WorktreeKeepWhen::AnyLocalFiles => legacy::WorktreeKeepWhen::AnyLocalFiles,
        v8::WorktreeKeepWhen::TrackedChanges => legacy::WorktreeKeepWhen::TrackedChanges,
        _ => legacy::WorktreeKeepWhen::UncommittedChanges,
    }
}

pub fn storage_to_wire(storage: &legacy::StorageSettings) -> v8::StorageSettings {
    let cleanup = &storage.worktree_cleanup;
    v8::StorageSettings {
        worktrees_directory: storage
            .worktrees_directory
            .as_ref()
            .map(|path| path.to_string_lossy().into_owned()),
        previous_worktrees_directories: storage
            .previous_worktrees_directories
            .iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect(),
        worktree_cleanup: v8::WorktreeCleanupSettings {
            inactive_after_days: cleanup.inactive_after_days,
            finished_after_days: cleanup.finished_after_days,
            merged: cleanup.merged,
            unchanged: cleanup.unchanged,
            deleted_tasks: cleanup.deleted_tasks,
            keep_when: keep_when_to_wire(cleanup.keep_when),
            remove_unpushed_commits: cleanup.remove_unpushed_commits,
            interval_hours: cleanup.interval_hours,
        },
    }
}

pub fn storage_from_wire(storage: &v8::StorageSettings) -> legacy::StorageSettings {
    let cleanup = &storage.worktree_cleanup;
    legacy::StorageSettings {
        worktrees_directory: storage.worktrees_directory.as_ref().map(PathBuf::from),
        previous_worktrees_directories: storage
            .previous_worktrees_directories
            .iter()
            .map(PathBuf::from)
            .collect(),
        worktree_cleanup: legacy::WorktreeCleanupSettings {
            inactive_after_days: cleanup.inactive_after_days,
            finished_after_days: cleanup.finished_after_days,
            merged: cleanup.merged,
            unchanged: cleanup.unchanged,
            deleted_tasks: cleanup.deleted_tasks,
            keep_when: keep_when_from_wire(&cleanup.keep_when),
            remove_unpushed_commits: cleanup.remove_unpushed_commits,
            interval_hours: cleanup.interval_hours,
        },
    }
}

/// The daemon's settings as the desktop's state holds them.
pub fn settings_from_wire(settings: &v8::Settings) -> DaemonSettings {
    DaemonSettings {
        computer_use_enabled: settings.computer_use_enabled,
        computer_use_allowed_apps: settings
            .computer_use_allowed_apps
            .iter()
            .map(|grant| ComputerAppGrant {
                bundle_id: grant.bundle_id.clone(),
                app_name: grant.app_name.clone(),
            })
            .collect(),
        disabled_providers: settings
            .disabled_providers
            .iter()
            .filter_map(|key| settings_provider(key))
            .collect(),
        provider_binary_overrides: settings
            .provider_binary_overrides
            .iter()
            .filter_map(|(key, binary)| Some((settings_provider(key)?, binary.clone())))
            .collect::<HashMap<_, _>>(),
        storage: storage_from_wire(&settings.storage),
        extra: BTreeMap::new(),
    }
}

/// A patch that sets every field the desktop edits to `settings`.
pub fn settings_patch(settings: &DaemonSettings) -> SettingsPatch {
    let mut disabled = settings
        .disabled_providers
        .iter()
        .map(|provider| provider.id().to_owned())
        .collect::<Vec<_>>();
    disabled.sort();
    disabled.dedup();
    SettingsPatch {
        // Not a desktop setting; left as the daemon has it.
        continue_threads_after_restart: None,
        computer_use_enabled: Some(settings.computer_use_enabled),
        computer_use_allowed_apps: Some(
            settings
                .computer_use_allowed_apps
                .iter()
                .map(|grant| AppGrant {
                    bundle_id: grant.bundle_id.clone(),
                    app_name: grant.app_name.clone(),
                })
                .collect(),
        ),
        disabled_providers: Some(disabled),
        provider_binary_overrides: Some(
            settings
                .provider_binary_overrides
                .iter()
                .map(|(provider, binary)| (provider.id().to_owned(), binary.clone()))
                .collect(),
        ),
        storage: Some(storage_to_wire(&settings.storage)),
    }
}

/// Whether two settings documents agree on everything the wire carries.
fn same_settings(left: &DaemonSettings, right: &DaemonSettings) -> bool {
    settings_patch(left) == settings_patch(right)
}

pub fn get(daemon: &DaemonV8) -> anyhow::Result<SettingsSnapshot> {
    daemon.call::<method::SettingsGet>(NoParams {})
}

pub fn update(daemon: &DaemonV8, settings: &DaemonSettings) -> anyhow::Result<SettingsSnapshot> {
    daemon.call::<method::SettingsUpdate>(SettingsUpdateParams {
        base_revision: None,
        patch: settings_patch(settings),
        op_id: Uuid::new_v4(),
    })
}

pub fn daemon_info(daemon: &DaemonV8) -> anyhow::Result<DaemonInfo> {
    daemon.call::<method::DaemonInfoGet>(NoParams {})
}

// -- storage ----------------------------------------------------------------

fn seconds(ms: i64) -> u64 {
    u64::try_from(ms.div_euclid(1000)).unwrap_or(0)
}

fn cleanup_reason(reason: v8::CleanupReason) -> legacy::WorktreeCleanupReason {
    use legacy::WorktreeCleanupReason as Legacy;
    use v8::CleanupReason as Wire;
    match reason {
        Wire::TaskDeleted => Legacy::TaskDeleted,
        Wire::Inactive { days } => Legacy::Inactive { days },
        Wire::Finished { days } => Legacy::Finished { days },
        Wire::Merged { base } => Legacy::Merged { base },
        Wire::Unchanged { base } => Legacy::Unchanged { base },
        Wire::NoRuleApplies => Legacy::NoRuleApplies,
        Wire::TaskRunning => Legacy::TaskRunning,
        Wire::LiveRuntime => Legacy::LiveRuntime,
        Wire::OpenTerminal => Legacy::OpenTerminal,
        Wire::OutsideManagedFolder => Legacy::OutsideManagedFolder,
        Wire::NotWakuBranch => Legacy::NotWakuBranch,
        Wire::NotLinkedWorktree => Legacy::NotLinkedWorktree,
        Wire::BranchChanged => Legacy::BranchChanged,
        Wire::UncommittedChanges { files } => Legacy::UncommittedChanges { files },
        Wire::IgnoredFiles { example } => Legacy::IgnoredFiles { example },
        Wire::UnpushedCommits { commits } => Legacy::UnpushedCommits { commits },
        Wire::InactiveFor { days, of } => Legacy::InactiveFor { days, of },
        Wire::FinishedFor { days, of } => Legacy::FinishedFor { days, of },
        Wire::NotFinished => Legacy::NotFinished,
        Wire::NotMerged { base } => Legacy::NotMerged { base },
        Wire::HasCommits { commits, base } => Legacy::HasCommits { commits, base },
        Wire::BaseUnavailable => Legacy::BaseUnavailable,
        Wire::ChangedDuringCleanup => Legacy::ChangedDuringCleanup,
        Wire::Error { message } => Legacy::Error { message },
        Wire::Unknown => Legacy::Error {
            message: "an unknown reason".into(),
        },
    }
}

/// A cleanup report as the storage page renders it (times in seconds).
pub fn cleanup_report_from_wire(
    report: v8::WorktreeCleanupReport,
) -> legacy::WorktreeCleanupReport {
    legacy::WorktreeCleanupReport {
        trigger: match report.trigger {
            v8::CleanupTrigger::Manual => legacy::WorktreeCleanupTrigger::Manual,
            _ => legacy::WorktreeCleanupTrigger::Automatic,
        },
        started_at: seconds(report.started_at),
        finished_at: seconds(report.finished_at),
        entries: report
            .entries
            .into_iter()
            .map(|entry| legacy::WorktreeCleanupEntry {
                path: PathBuf::from(entry.path),
                branch: entry.branch,
                task_id: entry.thread_id,
                task_title: entry.thread_title,
                outcome: match entry.outcome {
                    v8::CleanupOutcome::Removed => legacy::WorktreeCleanupOutcome::Removed,
                    v8::CleanupOutcome::Kept => legacy::WorktreeCleanupOutcome::Kept,
                    _ => legacy::WorktreeCleanupOutcome::Failed,
                },
                reason: cleanup_reason(entry.reason),
                branch_deleted: entry.branch_deleted,
            })
            .collect(),
        removed: report.removed,
        kept: report.kept,
        failed: report.failed,
        omitted: report.omitted,
    }
}

pub fn cleanup_status_from_wire(
    status: v8::WorktreeCleanupStatus,
) -> legacy::WorktreeCleanupStatus {
    legacy::WorktreeCleanupStatus {
        last_run_at: status.last_run_at.map(seconds),
        last_report: status.last_report.map(cleanup_report_from_wire),
        next_run_at: status.next_run_at.map(seconds),
        running: status.running,
    }
}

pub fn worktree_cleanup_status(daemon: &DaemonV8) -> anyhow::Result<legacy::WorktreeCleanupStatus> {
    Ok(cleanup_status_from_wire(
        daemon.call::<method::StorageWorktreeCleanupStatus>(NoParams {})?,
    ))
}

pub fn run_worktree_cleanup(daemon: &DaemonV8) -> anyhow::Result<legacy::WorktreeCleanupReport> {
    Ok(cleanup_report_from_wire(
        daemon.call::<method::StorageRunWorktreeCleanup>(StorageRunCleanupParams {
            op_id: Uuid::new_v4(),
        })?,
    ))
}

// -- the writer and the stream ------------------------------------------------

#[derive(Default)]
struct WriterState {
    /// What the desktop wants the daemon to hold.
    desired: Option<DaemonSettings>,
    /// What the daemon last confirmed holding.
    persisted: Option<DaemonSettings>,
    /// Documents the daemon confirmed for this writer, oldest first, until
    /// the settings stream delivers them; the stream can lag behind the
    /// writer, so `persisted` alone cannot tell an echo from a remote edit.
    unechoed: VecDeque<DaemonSettings>,
    stopped: bool,
}

struct WriterInner {
    daemon: DaemonV8,
    state: Mutex<WriterState>,
    idle: Condvar,
    wake: Sender<()>,
}

impl WriterInner {
    fn settled(state: &WriterState) -> bool {
        match (&state.desired, &state.persisted) {
            (None, _) => true,
            (Some(desired), Some(persisted)) => same_settings(desired, persisted),
            (Some(_), None) => false,
        }
    }
}

/// Writes the desktop's daemon settings with `settings.update` from its own
/// thread, coalescing bursts of edits into the latest document and retrying
/// failures, and follows the `settings` stream so edits from other clients
/// reach the desktop.
#[derive(Clone)]
pub struct SettingsSync {
    inner: Arc<WriterInner>,
}

/// A settings document another client wrote.
pub type RemoteSettings = DaemonSettings;

impl SettingsSync {
    /// Start the writer. `persisted` is what the daemon is known to hold
    /// (its startup snapshot); an equal update is never sent.
    pub fn start(daemon: DaemonV8, persisted: Option<DaemonSettings>) -> anyhow::Result<Self> {
        let (wake, wakes) = unbounded();
        let inner = Arc::new(WriterInner {
            daemon,
            state: Mutex::new(WriterState {
                desired: None,
                persisted,
                unechoed: VecDeque::new(),
                stopped: false,
            }),
            idle: Condvar::new(),
            wake,
        });
        let weak = Arc::downgrade(&inner);
        std::thread::Builder::new()
            .name("waku-daemon-settings".into())
            .spawn(move || write_settings(&weak, &wakes))?;
        Ok(Self { inner })
    }

    /// Queue `settings` for the daemon. Never blocks on the daemon.
    pub fn update(&self, settings: DaemonSettings) {
        let mut state = self.inner.state.lock();
        if state
            .persisted
            .as_ref()
            .is_some_and(|persisted| same_settings(persisted, &settings))
            && state.desired.is_none()
        {
            return;
        }
        state.desired = Some(settings);
        drop(state);
        let _ = self.inner.wake.send(());
    }

    /// Whether an edit is still on its way to the daemon.
    pub fn is_pending(&self) -> bool {
        !WriterInner::settled(&self.inner.state.lock())
    }

    /// Wait up to `timeout` for every queued edit to reach the daemon;
    /// whether it did. Blocks: for worker threads that must see the daemon
    /// act on the latest settings (a probe after a binary override changed).
    pub fn wait_settled(&self, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        let mut state = self.inner.state.lock();
        while !WriterInner::settled(&state) {
            if self.inner.idle.wait_until(&mut state, deadline).timed_out() {
                return WriterInner::settled(&state);
            }
        }
        true
    }

    /// Follow the `settings` stream from a worker thread: `on_change` gets
    /// each document another client wrote (never an echo of this writer's
    /// own edits). The thread stops once `on_change` returns `false`.
    pub fn follow(
        &self,
        on_change: impl FnMut(RemoteSettings) -> bool + Send + 'static,
    ) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        std::thread::Builder::new()
            .name("waku-daemon-settings-stream".into())
            .spawn(move || follow_settings(&inner, on_change))?;
        Ok(())
    }

    /// Stop the writer thread once its queue is written.
    pub fn stop(&self) {
        self.inner.state.lock().stopped = true;
        let _ = self.inner.wake.send(());
    }
}

fn write_settings(weak: &std::sync::Weak<WriterInner>, wakes: &Receiver<()>) {
    while wakes.recv().is_ok() {
        while wakes.try_recv().is_ok() {}
        loop {
            let Some(inner) = weak.upgrade() else {
                return;
            };
            let desired = {
                let state = inner.state.lock();
                if WriterInner::settled(&state) {
                    inner.idle.notify_all();
                    if state.stopped {
                        return;
                    }
                    break;
                }
                state.desired.clone()
            };
            let Some(desired) = desired else {
                break;
            };
            match update(&inner.daemon, &desired) {
                Ok(snapshot) => {
                    let mut state = inner.state.lock();
                    let confirmed = settings_from_wire(&snapshot.settings);
                    if state.unechoed.len() == MAX_UNECHOED {
                        state.unechoed.pop_front();
                    }
                    state.unechoed.push_back(confirmed.clone());
                    state.persisted = Some(confirmed);
                    // The daemon normalizes some fields (paths, cleanup
                    // bounds); what it confirmed is what it holds, and the
                    // desire is met once it was sent unchanged.
                    if state
                        .desired
                        .as_ref()
                        .is_some_and(|current| same_settings(current, &desired))
                    {
                        state.desired = None;
                    }
                    if WriterInner::settled(&state) {
                        inner.idle.notify_all();
                    }
                }
                Err(error) => {
                    eprintln!("could not persist Waku daemon settings: {error:#}");
                    drop(inner);
                    if wakes.recv_timeout(RETRY_INTERVAL).is_ok() {
                        while wakes.try_recv().is_ok() {}
                    }
                }
            }
        }
    }
}

fn follow_settings(inner: &WriterInner, mut on_change: impl FnMut(RemoteSettings) -> bool) {
    let mut after = None;
    // The last document the stream delivered. A subscription's snapshot can
    // predate this writer's latest confirmation, so items are compared with
    // the stream's own history, not with `persisted`.
    let mut seen = inner.state.lock().persisted.clone();
    loop {
        let Ok(client) = inner.daemon.client() else {
            std::thread::sleep(RESUBSCRIBE_INTERVAL);
            continue;
        };
        let subscription = client.subscribe::<stream::Settings>(
            SettingsSpec {},
            SubscribeOptions {
                after: after.clone(),
                window: None,
            },
        );
        // Until the connection closes; then resume from the last revision.
        while let Ok(event) = subscription.events().recv() {
            let settings = match event {
                StreamEvent::Item(StreamItem::Settings(
                    SettingsItem::Snapshot { revision, settings }
                    | SettingsItem::Updated { revision, settings },
                )) => {
                    after = Some(v8::StreamCursor::Revision { revision });
                    settings
                }
                StreamEvent::End(_) => break,
                _ => continue,
            };
            let settings = settings_from_wire(&settings);
            if seen
                .as_ref()
                .is_some_and(|seen| same_settings(seen, &settings))
            {
                continue;
            }
            seen = Some(settings.clone());
            let mut state = inner.state.lock();
            // An echo of this writer's own edit, and of nothing after it.
            if let Some(echo) = state
                .unechoed
                .iter()
                .position(|own| same_settings(own, &settings))
            {
                state.unechoed.drain(..=echo);
                continue;
            }
            // While this desktop's own edit is in flight, the daemon's
            // document is about to change again; the writer's confirmation
            // settles it.
            if !WriterInner::settled(&state) {
                continue;
            }
            // Another client's edit supersedes every earlier write; an echo
            // still owed was coalesced away by the stream.
            state.unechoed.clear();
            state.persisted = Some(settings.clone());
            drop(state);
            if !on_change(settings) {
                subscription.unsubscribe();
                return;
            }
        }
        std::thread::sleep(RESUBSCRIBE_INTERVAL);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> DaemonSettings {
        let mut settings = DaemonSettings {
            computer_use_enabled: true,
            computer_use_allowed_apps: vec![ComputerAppGrant {
                bundle_id: "com.apple.Safari".into(),
                app_name: "Safari".into(),
            }],
            disabled_providers: vec![ProviderKind::DeepSeek, ProviderKind::Amp],
            ..DaemonSettings::default()
        };
        settings
            .provider_binary_overrides
            .insert(ProviderKind::OpenCode, "/opt/opencode".into());
        settings.storage.worktrees_directory = Some("/srv/trees".into());
        settings.storage.worktree_cleanup.merged = true;
        settings.storage.worktree_cleanup.keep_when = legacy::WorktreeKeepWhen::TrackedChanges;
        settings
    }

    #[test]
    fn a_patch_names_providers_by_instance_id() {
        let patch = settings_patch(&sample());
        assert_eq!(
            patch.disabled_providers.as_deref(),
            Some(&["amp".to_owned(), "deepseek".to_owned()][..])
        );
        assert_eq!(
            patch.provider_binary_overrides.as_ref().unwrap()["opencode"],
            "/opt/opencode"
        );
        assert!(patch.touches_providers_or_all());
    }

    trait AllFields {
        fn touches_providers_or_all(&self) -> bool;
    }

    impl AllFields for SettingsPatch {
        fn touches_providers_or_all(&self) -> bool {
            self.computer_use_enabled.is_some()
                && self.computer_use_allowed_apps.is_some()
                && self.storage.is_some()
                && self.touches_providers()
        }
    }

    #[test]
    fn settings_round_trip_through_the_wire_shape() {
        let settings = sample();
        let patch = settings_patch(&settings);
        let wire = v8::Settings {
            computer_use_enabled: patch.computer_use_enabled.unwrap(),
            computer_use_allowed_apps: patch.computer_use_allowed_apps.unwrap(),
            disabled_providers: patch.disabled_providers.unwrap(),
            provider_binary_overrides: patch.provider_binary_overrides.unwrap(),
            storage: patch.storage.unwrap(),
            continue_threads_after_restart: true,
        };
        let back = settings_from_wire(&wire);
        assert!(same_settings(&settings, &back));
        assert_eq!(
            back.storage.worktrees_directory,
            Some(PathBuf::from("/srv/trees"))
        );
        assert_eq!(
            back.storage.worktree_cleanup.keep_when,
            legacy::WorktreeKeepWhen::TrackedChanges
        );
    }

    #[test]
    fn the_snapshots_camel_case_provider_names_are_understood() {
        let wire = v8::Settings {
            disabled_providers: vec!["deepSeek".into(), "ohmypi".into(), "nebula".into()],
            ..v8::Settings::default()
        };
        assert_eq!(
            settings_from_wire(&wire).disabled_providers,
            [ProviderKind::DeepSeek, ProviderKind::OhMyPi]
        );
    }

    #[test]
    fn cleanup_times_return_to_seconds_and_threads_to_tasks() {
        let thread = Uuid::from_u128(5);
        let status = cleanup_status_from_wire(v8::WorktreeCleanupStatus {
            last_run_at: Some(1_700_000_000_000),
            last_report: Some(v8::WorktreeCleanupReport {
                trigger: v8::CleanupTrigger::Manual,
                started_at: 1_700_000_000_000,
                finished_at: 1_700_000_001_500,
                entries: vec![v8::WorktreeCleanupEntry {
                    path: "/srv/trees/a".into(),
                    branch: Some("waku/a".into()),
                    thread_id: Some(thread),
                    thread_title: Some("Fix".into()),
                    outcome: v8::CleanupOutcome::Kept,
                    reason: v8::CleanupReason::UncommittedChanges { files: 3 },
                    branch_deleted: false,
                }],
                removed: 0,
                kept: 1,
                failed: 0,
                omitted: 0,
            }),
            next_run_at: None,
            running: false,
        });
        assert_eq!(status.last_run_at, Some(1_700_000_000));
        let report = status.last_report.unwrap();
        assert_eq!(report.finished_at, 1_700_000_001);
        assert_eq!(report.trigger, legacy::WorktreeCleanupTrigger::Manual);
        let entry = &report.entries[0];
        assert_eq!(entry.task_id, Some(thread));
        assert_eq!(entry.outcome, legacy::WorktreeCleanupOutcome::Kept);
        assert_eq!(
            entry.reason,
            legacy::WorktreeCleanupReason::UncommittedChanges { files: 3 }
        );
    }
}
