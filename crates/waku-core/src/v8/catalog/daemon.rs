//! `daemon.info`, `daemon.shutdown`, `settings.update` and `storage.*`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use waku_protocol::storage as legacy;
use waku_protocol::v8::{
    Ack, CleanupOutcome, CleanupReason, CleanupTrigger, DaemonInfo, DaemonShutdownParams, Error,
    ErrorCode, ErrorDetails, MethodResult, SettingsSnapshot, SettingsUpdateParams, StorageSettings,
    WorktreeCleanupEntry, WorktreeCleanupReport, WorktreeCleanupSettings, WorktreeCleanupStatus,
    WorktreeKeepWhen,
};

use super::{Domain, bad_request, call, display_path, provider_kind, seconds_to_ms, unexpected};
use crate::computer_use::ComputerAppGrant;
use crate::protocol::{Command, ResponsePayload};
use crate::v8::{V8Context, settings_revision, wire_settings};

/// How long a shutdown waits so its reply reaches the client first.
const SHUTDOWN_GRACE: Duration = Duration::from_millis(100);

pub(super) fn info(context: &V8Context) -> Result<DaemonInfo, Error> {
    let (default_cwd, projectless_root) =
        match call(context, Command::LoadTaskState, Domain::General)? {
            ResponsePayload::TaskState {
                default_cwd,
                projectless_root,
                ..
            } => (default_cwd, projectless_root),
            other => return Err(unexpected(&other)),
        };
    Ok(DaemonInfo {
        version: env!("CARGO_PKG_VERSION").into(),
        environment: context.environment.clone(),
        features: Vec::new(),
        uptime_ms: u64::try_from(context.catalog.started_at.elapsed().as_millis())
            .unwrap_or(u64::MAX),
        default_cwd: display_path(&default_cwd),
        projectless_root: projectless_root.as_deref().map(display_path),
        home: dirs::home_dir().as_deref().map(display_path),
    })
}

pub(super) fn shutdown(
    context: &std::sync::Arc<V8Context>,
    _params: DaemonShutdownParams,
) -> Result<MethodResult, Error> {
    if !context.allow_shutdown {
        return Err(Error::new(
            ErrorCode::Forbidden,
            "daemon shutdown is managed by its service owner",
        ));
    }
    let signal = context.shutdown.clone();
    tokio::spawn(async move {
        tokio::time::sleep(SHUTDOWN_GRACE).await;
        signal.trigger();
    });
    Ok(MethodResult::DaemonShutdown(Ack {}))
}

// -- settings ---------------------------------------------------------------

fn keep_when(value: legacy::WorktreeKeepWhen) -> WorktreeKeepWhen {
    match value {
        legacy::WorktreeKeepWhen::AnyLocalFiles => WorktreeKeepWhen::AnyLocalFiles,
        legacy::WorktreeKeepWhen::UncommittedChanges => WorktreeKeepWhen::UncommittedChanges,
        legacy::WorktreeKeepWhen::TrackedChanges => WorktreeKeepWhen::TrackedChanges,
    }
}

/// The legacy storage settings as v8 clients see them.
pub(crate) fn wire_storage(storage: &legacy::StorageSettings) -> StorageSettings {
    let cleanup = &storage.worktree_cleanup;
    StorageSettings {
        worktrees_directory: storage.worktrees_directory.as_deref().map(display_path),
        previous_worktrees_directories: storage
            .previous_worktrees_directories
            .iter()
            .map(|path| display_path(path))
            .collect(),
        worktree_cleanup: WorktreeCleanupSettings {
            inactive_after_days: cleanup.inactive_after_days,
            finished_after_days: cleanup.finished_after_days,
            merged: cleanup.merged,
            unchanged: cleanup.unchanged,
            deleted_tasks: cleanup.deleted_tasks,
            keep_when: keep_when(cleanup.keep_when),
            remove_unpushed_commits: cleanup.remove_unpushed_commits,
            interval_hours: cleanup.interval_hours,
        },
    }
}

fn legacy_storage(storage: StorageSettings) -> Result<legacy::StorageSettings, Error> {
    let home = dirs::home_dir();
    let directory = |field: &str, input: &str| {
        legacy::validate_worktrees_directory(input, home.as_deref()).map_err(|error| {
            bad_request(
                field,
                match error {
                    legacy::WorktreeDirectoryError::Empty => "empty",
                    legacy::WorktreeDirectoryError::NotAbsolute => "not an absolute path",
                    legacy::WorktreeDirectoryError::FilesystemRoot => "the filesystem root",
                },
            )
        })
    };
    let cleanup = storage.worktree_cleanup;
    Ok(legacy::StorageSettings {
        worktrees_directory: storage
            .worktrees_directory
            .as_deref()
            .map(|input| directory("patch.storage.worktreesDirectory", input))
            .transpose()?,
        previous_worktrees_directories: storage
            .previous_worktrees_directories
            .iter()
            .map(|input| directory("patch.storage.previousWorktreesDirectories", input))
            .collect::<Result<Vec<PathBuf>, Error>>()?,
        worktree_cleanup: legacy::WorktreeCleanupSettings {
            inactive_after_days: cleanup.inactive_after_days,
            finished_after_days: cleanup.finished_after_days,
            merged: cleanup.merged,
            unchanged: cleanup.unchanged,
            deleted_tasks: cleanup.deleted_tasks,
            keep_when: match cleanup.keep_when {
                WorktreeKeepWhen::AnyLocalFiles => legacy::WorktreeKeepWhen::AnyLocalFiles,
                WorktreeKeepWhen::UncommittedChanges => {
                    legacy::WorktreeKeepWhen::UncommittedChanges
                }
                WorktreeKeepWhen::TrackedChanges => legacy::WorktreeKeepWhen::TrackedChanges,
                WorktreeKeepWhen::Unknown(value) => {
                    return Err(bad_request(
                        "patch.storage.worktreeCleanup.keepWhen",
                        format!("unknown value {value}"),
                    ));
                }
            },
            remove_unpushed_commits: cleanup.remove_unpushed_commits,
            interval_hours: cleanup.interval_hours,
        }
        .normalized(),
    })
}

pub(super) fn update_settings(
    context: &V8Context,
    params: SettingsUpdateParams,
) -> Result<SettingsSnapshot, Error> {
    let _update = context.catalog.settings_update.lock();
    let mut settings = super::providers::daemon_settings(context)?;
    let current = settings_revision(&wire_settings(&settings));
    if let Some(base) = params.base_revision
        && base != current
    {
        return Err(Error::new(
            ErrorCode::Conflict,
            format!("the settings changed since revision {base}"),
        )
        .with_details(ErrorDetails::RevisionConflict {
            current: current.to_string(),
        }));
    }
    let patch = params.patch;
    let touches_providers = patch.touches_providers();
    if let Some(enabled) = patch.computer_use_enabled {
        settings.computer_use_enabled = enabled;
    }
    if let Some(apps) = patch.computer_use_allowed_apps {
        settings.computer_use_allowed_apps = apps
            .into_iter()
            .map(|grant| ComputerAppGrant {
                bundle_id: grant.bundle_id,
                app_name: grant.app_name,
            })
            .collect();
    }
    if let Some(disabled) = patch.disabled_providers {
        settings.disabled_providers = disabled
            .iter()
            .map(|id| {
                provider_kind(id).map_err(|_| {
                    bad_request(
                        "patch.disabledProviders",
                        format!("unknown provider instance {id}"),
                    )
                })
            })
            .collect::<Result<_, _>>()?;
    }
    if let Some(overrides) = patch.provider_binary_overrides {
        settings.provider_binary_overrides = overrides
            .into_iter()
            .map(|(id, binary)| {
                let kind = provider_kind(&id).map_err(|_| {
                    bad_request(
                        "patch.providerBinaryOverrides",
                        format!("unknown provider instance {id}"),
                    )
                })?;
                Ok((kind, binary))
            })
            .collect::<Result<HashMap<_, _>, Error>>()?;
    }
    if let Some(storage) = patch.storage {
        settings.storage = legacy_storage(storage)?;
    }
    if let Some(enabled) = patch.continue_threads_after_restart {
        settings.set_continue_threads_after_restart(enabled);
    }
    call(
        context,
        Command::UpdateSettings { settings },
        Domain::General,
    )?;
    let state = context
        .backend
        .load_settings(&context.settings)
        .map_err(|error| super::backend_error(error, Domain::General))?;
    if touches_providers {
        super::providers::settings_changed(context);
    }
    Ok(SettingsSnapshot {
        revision: state.revision,
        settings: state.settings.clone(),
    })
}

// -- storage ----------------------------------------------------------------

fn reason(reason: legacy::WorktreeCleanupReason) -> CleanupReason {
    use legacy::WorktreeCleanupReason as Legacy;
    match reason {
        Legacy::TaskDeleted => CleanupReason::TaskDeleted,
        Legacy::Inactive { days } => CleanupReason::Inactive { days },
        Legacy::Finished { days } => CleanupReason::Finished { days },
        Legacy::Merged { base } => CleanupReason::Merged { base },
        Legacy::Unchanged { base } => CleanupReason::Unchanged { base },
        Legacy::NoRuleApplies => CleanupReason::NoRuleApplies,
        Legacy::TaskRunning => CleanupReason::TaskRunning,
        Legacy::LiveRuntime => CleanupReason::LiveRuntime,
        Legacy::OpenTerminal => CleanupReason::OpenTerminal,
        Legacy::OutsideManagedFolder => CleanupReason::OutsideManagedFolder,
        Legacy::NotWakuBranch => CleanupReason::NotWakuBranch,
        Legacy::NotLinkedWorktree => CleanupReason::NotLinkedWorktree,
        Legacy::BranchChanged => CleanupReason::BranchChanged,
        Legacy::UncommittedChanges { files } => CleanupReason::UncommittedChanges { files },
        Legacy::IgnoredFiles { example } => CleanupReason::IgnoredFiles { example },
        Legacy::UnpushedCommits { commits } => CleanupReason::UnpushedCommits { commits },
        Legacy::InactiveFor { days, of } => CleanupReason::InactiveFor { days, of },
        Legacy::FinishedFor { days, of } => CleanupReason::FinishedFor { days, of },
        Legacy::NotFinished => CleanupReason::NotFinished,
        Legacy::NotMerged { base } => CleanupReason::NotMerged { base },
        Legacy::HasCommits { commits, base } => CleanupReason::HasCommits { commits, base },
        Legacy::BaseUnavailable => CleanupReason::BaseUnavailable,
        Legacy::ChangedDuringCleanup => CleanupReason::ChangedDuringCleanup,
        Legacy::Error { message } => CleanupReason::Error { message },
    }
}

fn report(report: legacy::WorktreeCleanupReport) -> WorktreeCleanupReport {
    WorktreeCleanupReport {
        trigger: match report.trigger {
            legacy::WorktreeCleanupTrigger::Automatic => CleanupTrigger::Automatic,
            legacy::WorktreeCleanupTrigger::Manual => CleanupTrigger::Manual,
        },
        started_at: seconds_to_ms(report.started_at),
        finished_at: seconds_to_ms(report.finished_at),
        entries: report
            .entries
            .into_iter()
            .map(|entry| WorktreeCleanupEntry {
                path: display_path(&entry.path),
                branch: entry.branch,
                thread_id: entry.task_id,
                thread_title: entry.task_title,
                outcome: match entry.outcome {
                    legacy::WorktreeCleanupOutcome::Removed => CleanupOutcome::Removed,
                    legacy::WorktreeCleanupOutcome::Kept => CleanupOutcome::Kept,
                    legacy::WorktreeCleanupOutcome::Failed => CleanupOutcome::Failed,
                },
                reason: reason(entry.reason),
                branch_deleted: entry.branch_deleted,
            })
            .collect(),
        removed: report.removed,
        kept: report.kept,
        failed: report.failed,
        omitted: report.omitted,
    }
}

pub(super) fn cleanup_status(context: &V8Context) -> Result<WorktreeCleanupStatus, Error> {
    match call(context, Command::GetWorktreeCleanupStatus, Domain::General)? {
        ResponsePayload::WorktreeCleanupStatus { status } => Ok(WorktreeCleanupStatus {
            last_run_at: status.last_run_at.map(seconds_to_ms),
            last_report: status.last_report.map(report),
            next_run_at: status.next_run_at.map(seconds_to_ms),
            running: status.running,
        }),
        other => Err(unexpected(&other)),
    }
}

pub(super) fn run_cleanup(context: &V8Context) -> Result<WorktreeCleanupReport, Error> {
    let result = call(context, Command::RunWorktreeCleanup, Domain::Vcs);
    context.catalog.vcs.changed();
    match result? {
        ResponsePayload::WorktreeCleanupReport { report: done } => Ok(report(done)),
        other => Err(unexpected(&other)),
    }
}
