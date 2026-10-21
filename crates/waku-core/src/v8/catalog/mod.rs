//! The non-orchestration v8 catalog (#53 part 2, RFC 0003 §5.2–§5.6): files,
//! version control and review, terminals, providers and usage, composer
//! completion, skills, drafts, settings and storage, blobs and attachments,
//! and the daemon itself.
//!
//! Every method is an adapter over the same backend the legacy protocol
//! uses ([`BackendHandle`](super::BackendHandle) and the backend functions
//! behind it), so both protocols see one daemon state while clients move
//! over. Mutations are idempotent by `opId` through [`receipts`].

mod blobs;
mod composer;
mod daemon;
mod files;
mod providers;
pub(crate) mod receipts;
mod terminal;
mod vcs;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::mpsc;
use waku_protocol::v8::error::VcsFailureKind;
use waku_protocol::v8::{Error, ErrorCode, ErrorDetails, Method, MethodResult};

use super::V8Context;
use super::connection::{Conn, DataChunk, StreamSink};
use crate::protocol::{Command, ResponsePayload};
use crate::workspace::{WorkspaceOperation, WorkspaceResult};

pub(crate) use blobs::store_blob;
pub(crate) use composer::DraftsBus;
pub(crate) use daemon::wire_storage;
pub(crate) use files::file_revision;
pub(crate) use providers::ProvidersBus;
pub(crate) use terminal::{TerminalHub, TerminalInput};
pub(crate) use vcs::VcsBus;

/// Shared state of the catalog, one per daemon.
pub(crate) struct Catalog {
    pub(crate) receipts: receipts::Receipts,
    pub(crate) drafts: DraftsBus,
    pub(crate) providers: ProvidersBus,
    pub(crate) terminals: Arc<TerminalHub>,
    pub(crate) vcs: VcsBus,
    pub(crate) blobs: blobs::BlobIndex,
    /// Serializes `settings.update`, so a `baseRevision` check and the write
    /// after it see the same settings.
    pub(crate) settings_update: parking_lot::Mutex<()>,
    pub(crate) started_at: std::time::Instant,
}

impl Default for Catalog {
    fn default() -> Self {
        Self {
            receipts: receipts::Receipts::default(),
            drafts: DraftsBus::default(),
            providers: ProvidersBus::default(),
            terminals: Arc::new(TerminalHub::default()),
            vcs: VcsBus::default(),
            blobs: blobs::BlobIndex::default(),
            settings_update: parking_lot::Mutex::new(()),
            started_at: std::time::Instant::now(),
        }
    }
}

/// What a call knows about its connection.
pub(crate) struct CallContext {
    pub(crate) conn: Arc<Conn>,
    /// The DATA of a `blobs.put`, registered before its task started.
    pub(crate) upload: Option<mpsc::Receiver<DataChunk>>,
}

/// Run a non-orchestration method: from its receipt when its `opId` was
/// seen, otherwise on the blocking pool (or, for uploads, as a task reading
/// the call's DATA).
pub(crate) async fn run(
    context: Arc<V8Context>,
    call: CallContext,
    method: Method,
) -> Result<MethodResult, Error> {
    let Some(op_id) = method.op_id() else {
        return execute(context, call, method).await;
    };
    let receipts = &context.catalog.receipts;
    let ticket = match receipts.begin(op_id).await {
        receipts::Begin::Done(result) => return Ok(result),
        receipts::Begin::Run(ticket) => ticket,
    };
    let outcome = execute(context.clone(), call, method).await;
    context.catalog.receipts.finish(ticket, &outcome);
    outcome
}

async fn execute(
    context: Arc<V8Context>,
    call: CallContext,
    method: Method,
) -> Result<MethodResult, Error> {
    match method {
        Method::BlobsPut(params) => blobs::put(&context, call, params)
            .await
            .map(MethodResult::BlobsPut),
        Method::DaemonShutdown(params) => daemon::shutdown(&context, params),
        method => {
            let local = call.conn.local;
            tokio::task::spawn_blocking(move || super::methods::handle(&context, local, method))
                .await
                .unwrap_or_else(|_| Err(Error::new(ErrorCode::Internal, "the call failed")))
        }
    }
}

/// A catalog method on the blocking pool. `local` is whether the call came
/// over the local socket.
pub(crate) fn handle(
    context: &V8Context,
    local: bool,
    method: Method,
) -> Result<MethodResult, Error> {
    use MethodResult as R;
    Ok(match method {
        Method::DaemonInfoGet(_) => R::DaemonInfoGet(daemon::info(context)?),
        Method::FilesTree(params) => R::FilesTree(files::tree(context, params)?),
        Method::FilesSearchEntries(params) => {
            R::FilesSearchEntries(files::search_entries(context, params)?)
        }
        Method::FilesSearchContents(params) => {
            R::FilesSearchContents(files::search_contents(context, params)?)
        }
        Method::FilesWrite(params) => R::FilesWrite(files::write(context, params)?),
        Method::FsBrowse(params) => R::FsBrowse(files::browse(context, params)?),
        Method::ComposerSlashCommands(params) => {
            R::ComposerSlashCommands(composer::slash_commands(context, params)?)
        }
        Method::SkillsList(params) => R::SkillsList(composer::skills(context, params)?),
        Method::SkillsSetEnabled(params) => {
            R::SkillsSetEnabled(composer::set_skills_enabled(context, params)?)
        }
        Method::SkillsTrash(params) => R::SkillsTrash(composer::trash_skills(context, params)?),
        Method::DraftsGet(_) => R::DraftsGet(composer::drafts(context)?),
        Method::DraftsApply(params) => R::DraftsApply(composer::apply_drafts(context, params)?),
        Method::BlobsStat(params) => R::BlobsStat(blobs::stat(context, local, params)?),
        Method::AttachmentsImport(params) => {
            R::AttachmentsImport(blobs::import_attachment(context, params)?)
        }
        Method::AttachmentsImportPath(params) => {
            R::AttachmentsImportPath(blobs::import_path(context, params)?)
        }
        Method::VcsStatus(params) => R::VcsStatus(vcs::status(context, params)?),
        Method::VcsRefs(params) => R::VcsRefs(vcs::refs(context, params)?),
        Method::VcsSwitchRef(params) => R::VcsSwitchRef(vcs::switch_ref(context, params)?),
        Method::VcsWorktrees(params) => R::VcsWorktrees(vcs::worktrees(context, params)?),
        Method::VcsCreateWorktree(params) => {
            R::VcsCreateWorktree(vcs::create_worktree(context, params)?)
        }
        Method::VcsRemoveWorktree(params) => {
            R::VcsRemoveWorktree(vcs::remove_worktree(context, params)?)
        }
        Method::VcsCommitPreview(params) => {
            R::VcsCommitPreview(vcs::commit_preview(context, params)?)
        }
        Method::VcsStage(params) => R::VcsStage(vcs::stage(context, params)?),
        Method::VcsGenerateCommitMessage(params) => {
            R::VcsGenerateCommitMessage(vcs::generate_commit_message(context, params)?)
        }
        Method::VcsCommit(params) => R::VcsCommit(vcs::commit(context, params)?),
        Method::VcsPush(params) => R::VcsPush(vcs::push(context, params)?),
        Method::VcsPull(params) => R::VcsPull(vcs::pull(context, params)?),
        Method::ReviewDiffGet(params) => R::ReviewDiffGet(vcs::review_diff(context, params)?),
        Method::TerminalOpen(params) => R::TerminalOpen(terminal::open(context, params)?),
        Method::TerminalResize(params) => R::TerminalResize(terminal::resize(context, params)?),
        Method::TerminalClear(params) => R::TerminalClear(terminal::clear(context, params)?),
        Method::TerminalRestart(params) => R::TerminalRestart(terminal::restart(context, params)?),
        Method::TerminalClose(params) => R::TerminalClose(terminal::close(context, params)?),
        Method::ProvidersList(_) => R::ProvidersList(providers::list(context)?),
        Method::ProvidersProbe(params) => R::ProvidersProbe(providers::probe(context, params)?),
        Method::ProvidersModels(params) => R::ProvidersModels(providers::models(context, params)?),
        Method::ProvidersPlanUsage(params) => {
            R::ProvidersPlanUsage(providers::plan_usage(context, params)?)
        }
        Method::UsageHistoryGet(params) => R::UsageHistoryGet(providers::usage(context, params)?),
        Method::ComputerUsePermissions(params) => {
            R::ComputerUsePermissions(providers::computer_permissions(context, local, params)?)
        }
        Method::SettingsUpdate(params) => {
            R::SettingsUpdate(daemon::update_settings(context, params)?)
        }
        Method::StorageWorktreeCleanupStatus(_) => {
            R::StorageWorktreeCleanupStatus(daemon::cleanup_status(context)?)
        }
        Method::StorageRunWorktreeCleanup(_) => {
            R::StorageRunWorktreeCleanup(daemon::run_cleanup(context)?)
        }
        other => {
            return Err(Error::new(
                ErrorCode::Internal,
                format!("{} is not a catalog method", other.name()),
            ));
        }
    })
}

/// Start a catalog stream, or return the spec when it is not one.
pub(crate) fn subscribe(
    sink: StreamSink,
    context: Arc<V8Context>,
    spec: waku_protocol::v8::StreamSpec,
    after: Option<waku_protocol::v8::StreamCursor>,
    input: Option<mpsc::Receiver<TerminalInput>>,
) -> Result<tokio::task::JoinHandle<()>, (StreamSink, waku_protocol::v8::StreamSpec)> {
    use waku_protocol::v8::StreamSpec;
    Ok(match spec {
        StreamSpec::Drafts(_) => tokio::spawn(composer::drafts_stream(sink, context, after)),
        StreamSpec::Providers(_) => tokio::spawn(providers::providers_stream(sink, context, after)),
        StreamSpec::Terminal(spec) => {
            tokio::spawn(terminal::terminal_stream(sink, context, spec, after, input))
        }
        StreamSpec::TerminalList(spec) => {
            tokio::spawn(terminal::terminal_list_stream(sink, context, spec))
        }
        StreamSpec::VcsStatusWatch(spec) => tokio::spawn(vcs::status_stream(sink, context, spec)),
        other => return Err((sink, other)),
    })
}

// -- shared helpers ---------------------------------------------------------

pub(super) fn internal(error: impl std::fmt::Display) -> Error {
    Error::new(ErrorCode::Internal, error.to_string())
}

fn unexpected(payload: &ResponsePayload) -> Error {
    Error::new(
        ErrorCode::Internal,
        format!(
            "unexpected backend response {}",
            super::payload_name(payload)
        ),
    )
}

pub(super) fn bad_request(field: &str, reason: impl Into<String>) -> Error {
    let reason = reason.into();
    Error::new(ErrorCode::BadRequest, format!("{field}: {reason}")).with_details(
        ErrorDetails::Validation {
            field: field.into(),
            reason,
        },
    )
}

pub(super) fn not_found(resource: &str, id: impl Into<String>) -> Error {
    let id = id.into();
    Error::new(ErrorCode::NotFound, format!("no {resource} {id}")).with_details(
        ErrorDetails::NotFound {
            resource: resource.into(),
            id,
        },
    )
}

/// Which error code a backend failure maps to when nothing more specific is
/// recognized.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Domain {
    General,
    Vcs,
    Provider,
}

/// A backend failure as a typed error: filesystem errors by their kind, git
/// failures by what git said, everything else by `domain`.
pub(super) fn backend_error(error: anyhow::Error, domain: Domain) -> Error {
    let message = format!("{error:#}");
    if let Some(io) = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<std::io::Error>())
    {
        let code = match io.kind() {
            std::io::ErrorKind::NotFound => ErrorCode::NotFound,
            std::io::ErrorKind::InvalidInput => ErrorCode::BadRequest,
            std::io::ErrorKind::AlreadyExists => ErrorCode::AlreadyExists,
            std::io::ErrorKind::PermissionDenied => ErrorCode::Forbidden,
            _ => ErrorCode::Io,
        };
        if domain != Domain::Vcs || code != ErrorCode::Io {
            return Error::new(code, message);
        }
    }
    let lower = message.to_ascii_lowercase();
    let vcs = [
        ("not a git repository", VcsFailureKind::NotARepository),
        ("conflict", VcsFailureKind::MergeConflict),
        ("non-fast-forward", VcsFailureKind::NonFastForward),
        ("[rejected]", VcsFailureKind::NonFastForward),
        ("authentication failed", VcsFailureKind::AuthRequired),
        ("could not read username", VcsFailureKind::AuthRequired),
        (
            "permission denied (publickey)",
            VcsFailureKind::AuthRequired,
        ),
        ("did not match any", VcsFailureKind::RefNotFound),
        ("invalid reference", VcsFailureKind::RefNotFound),
        ("unknown revision", VcsFailureKind::RefNotFound),
        ("would be overwritten", VcsFailureKind::DirtyWorktree),
        (
            "contains modified or untracked files",
            VcsFailureKind::DirtyWorktree,
        ),
        ("uncommitted changes", VcsFailureKind::DirtyWorktree),
    ]
    .into_iter()
    .find(|(needle, _)| lower.contains(needle));
    if let Some((_, kind)) = vcs
        && domain == Domain::Vcs
    {
        return Error::new(ErrorCode::Vcs, message).with_details(ErrorDetails::Vcs { kind });
    }
    let code = match domain {
        Domain::General => ErrorCode::Internal,
        Domain::Vcs => ErrorCode::Vcs,
        Domain::Provider => ErrorCode::Provider,
    };
    Error::new(code, message)
}

/// Run a legacy command on the backend, with no session.
pub(super) fn call(
    context: &V8Context,
    command: Command,
    domain: Domain,
) -> Result<ResponsePayload, Error> {
    context
        .backend
        .call(command)
        .map_err(|error| backend_error(error, domain))
}

/// Run a workspace operation on the backend.
pub(super) fn workspace(
    context: &V8Context,
    operation: WorkspaceOperation,
    domain: Domain,
) -> Result<WorkspaceResult, Error> {
    match call(context, Command::Workspace { operation }, domain)? {
        ResponsePayload::Workspace { result } => Ok(result),
        other => Err(unexpected(&other)),
    }
}

pub(super) fn path_string(path: &Path) -> Result<String, Error> {
    path.to_str().map(str::to_owned).ok_or_else(|| {
        Error::new(
            ErrorCode::Io,
            format!("{} is not valid UTF-8", path.display()),
        )
    })
}

/// A lossless string for paths the daemon produced itself; a non-UTF-8 one
/// is reported lossily rather than failing a whole listing.
pub(super) fn display_path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

/// An absolute daemon-host path from a request.
pub(super) fn absolute_path(field: &str, path: &str) -> Result<PathBuf, Error> {
    let path = PathBuf::from(path);
    if !path.is_absolute() {
        return Err(bad_request(field, "not an absolute path"));
    }
    Ok(path)
}

pub(super) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
        })
}

/// Unix seconds as milliseconds.
pub(super) fn seconds_to_ms(seconds: u64) -> i64 {
    i64::try_from(seconds)
        .unwrap_or(i64::MAX / 1000)
        .saturating_mul(1000)
}

/// A 53-bit FNV-1a content hash of `value`'s MessagePack encoding: equal
/// revisions mean equal content, across daemon restarts too.
pub(super) fn content_revision<T: serde::Serialize>(value: &T) -> u64 {
    let bytes = waku_protocol::v8::codec::to_vec(waku_protocol::v8::Encoding::MessagePack, value)
        .expect("catalog values encode");
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash & waku_protocol::v8::MAX_SAFE_INTEGER
}

/// Resolve a provider instance id to its driver kind.
pub(super) fn provider_kind(instance_id: &str) -> Result<crate::model::ProviderKind, Error> {
    crate::model::ProviderKind::ALL
        .into_iter()
        .find(|kind| kind.id() == instance_id)
        .ok_or_else(|| not_found("provider instance", instance_id))
}

#[cfg(all(test, unix))]
mod tests;
