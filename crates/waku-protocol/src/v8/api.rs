//! The v8 methods and streams (RFC 0003 §5), declared with [`methods!`] and
//! [`streams!`].
//!
//! #53 declared the methods that already had a v7 backend; #54, #56 and #57
//! add orchestration: `orchestration.dispatch`, the `threads.*` reads, and
//! the `shell`, `thread` and `commandOutput` streams, whose envelopes live in
//! [`super::orchestration`]. Everything else in RFC 0003 §5.2–§5.4 is added
//! here as its backend lands.
//!
//! TODO(#55): `projects.list` returns the orchestration `Project` entity
//! instead of [`ProjectInfo`] once the orchestrator is the daemon's source of
//! truth.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use uuid::Uuid;

use super::blobs::*;
use super::composer::*;
use super::daemon::*;
use super::files::*;
use super::orchestration::{
    ArchivedPage, ArchivedParams, CommandOutputItem, CommandOutputSpec, DispatchParams,
    DispatchResult, HistoryPage, HistoryPageParams, ShellItem, ShellSpec, ThreadItem,
    ThreadSnapshot, ThreadSnapshotParams, ThreadSpec, TurnItemParams,
};
use super::providers::*;
use super::terminal::*;
use super::vcs::*;
use super::{Scope, StreamCursor};
use crate::orchestration::TurnItem;

/// A method's static description; implemented by the marker types in
/// [`method`].
pub trait MethodDef {
    const NAME: &'static str;
    const SCOPE: Scope;
    /// Whether the method mutates and carries an `opId`.
    const OP_ID: bool;
    type Params: Serialize + DeserializeOwned;
    type Result: Serialize + DeserializeOwned;

    fn wrap(params: Self::Params) -> Method;
    fn unwrap(result: MethodResult) -> Option<Self::Result>;
}

/// A stream's static description; implemented by the marker types in
/// [`stream`].
pub trait StreamDef {
    const NAME: &'static str;
    const SCOPE: Scope;
    const CURSOR: CursorKind;
    /// Whether the stream also carries DATA frames under credit.
    const DATA: bool;
    type Spec: Serialize + DeserializeOwned;
    type Item: Serialize + DeserializeOwned;

    fn wrap(spec: Self::Spec) -> StreamSpec;
    fn unwrap(item: StreamItem) -> Option<Self::Item>;
}

/// Which [`StreamCursor`] variant a stream resumes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorKind {
    None,
    Sequence,
    Revision,
    Offset,
}

impl CursorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Sequence => "sequence",
            Self::Revision => "revision",
            Self::Offset => "offset",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MethodInfo {
    pub name: &'static str,
    pub scope: Scope,
    pub op_id: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StreamInfo {
    pub name: &'static str,
    pub scope: Scope,
    pub cursor: CursorKind,
    pub data: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CommandInfo {
    pub name: &'static str,
    pub scope: Scope,
}

methods! {
    /// The daemon's settings, with secrets redacted.
    "settings.get" => SettingsGet(NoParams) -> SettingsSnapshot, scope = Read;
    /// The project catalog.
    "projects.list" => ProjectsList(NoParams) -> Vec<ProjectInfo>, scope = Read;
    /// A text file under a root. Text is inline up to
    /// [`MAX_INLINE_TEXT_BYTES`].
    "files.read" => FilesRead(FilesReadParams) -> FileContent, scope = FilesRead;
    /// Dispatch a command (RFC 0001), idempotent by `commandId`. The scope
    /// declared here is the floor; each command adds its own
    /// ([`super::orchestration::command_scopes`]).
    "orchestration.dispatch" => OrchestrationDispatch(DispatchParams) -> DispatchResult, scope = Read;
    /// A bounded snapshot of a thread, the same shape as the `thread`
    /// stream's first item.
    "threads.snapshot" => ThreadsSnapshot(ThreadSnapshotParams) -> ThreadSnapshot, scope = Read;
    /// Older turn items, bounded like the snapshot window.
    "threads.historyPage" => ThreadsHistoryPage(HistoryPageParams) -> HistoryPage, scope = Read;
    /// One turn item as stored, with the bodies the wire projection omits.
    "threads.turnItem" => ThreadsTurnItem(TurnItemParams) -> TurnItem, scope = Read;
    /// Archived threads, paged; the active shell never carries them.
    "threads.archived" => ThreadsArchived(ArchivedParams) -> ArchivedPage, scope = Read;

    // -- #53 part 2: the non-orchestration catalog (RFC 0003 §5.2) --------

    /// The daemon's version, environment and default locations.
    "daemon.info" => DaemonInfoGet(NoParams) -> DaemonInfo, scope = Read;
    /// Stop the daemon; only a desktop-owned daemon accepts it.
    "daemon.shutdown" => DaemonShutdown(DaemonShutdownParams) -> Ack, scope = Admin;
    /// A working tree, with the `expanded` directories' children.
    "files.tree" => FilesTree(FilesTreeParams) -> Vec<TreeEntry>, scope = FilesRead;
    /// Files and directories for `@` mentions, fuzzy-ranked.
    "files.searchEntries" => FilesSearchEntries(FilesSearchEntriesParams) -> Vec<FileEntry>, scope = FilesRead;
    /// Lines matching a literal or a regular expression.
    "files.searchContents" => FilesSearchContents(FilesSearchContentsParams) -> Vec<ContentHit>, scope = FilesRead;
    /// Write a text file, optionally only over `baseRevision`.
    "files.write" => FilesWrite(FilesWriteParams) -> FileRevision, scope = FilesWrite, op_id;
    /// A directory on the daemon host, for project pickers.
    "fs.browse" => FsBrowse(FsBrowseParams) -> Directory, scope = FilesRead;
    /// The slash commands of a provider instance in a project.
    "composer.slashCommands" => ComposerSlashCommands(ComposerSlashCommandsParams) -> Vec<SlashCommand>, scope = Read;
    /// User skills and those of the given projects.
    "skills.list" => SkillsList(SkillsListParams) -> SkillsCatalog, scope = Read;
    "skills.setEnabled" => SkillsSetEnabled(SkillsSetEnabledParams) -> Ack, scope = SettingsWrite, op_id;
    "skills.trash" => SkillsTrash(SkillsTrashParams) -> Ack, scope = SettingsWrite, op_id;
    /// Every composer draft, with the revision the `drafts` stream resumes
    /// from.
    "drafts.get" => DraftsGet(NoParams) -> DraftsSnapshot, scope = Read;
    "drafts.apply" => DraftsApply(DraftsApplyParams) -> DraftsRevision, scope = Operate, op_id;
    /// Upload a blob: the bytes follow as DATA on `channel`.
    "blobs.put" => BlobsPut(BlobsPutParams) -> BlobPut, scope = Operate;
    "blobs.stat" => BlobsStat(BlobsStatParams) -> BlobStat, scope = Read;
    /// An attachment from uploaded blobs.
    "attachments.import" => AttachmentsImport(AttachmentsImportParams) -> StoredAttachment, scope = Operate, op_id;
    /// An attachment copied from a daemon-host path (also `files:read`).
    "attachments.importPath" => AttachmentsImportPath(AttachmentsImportPathParams) -> StoredAttachment, scope = Operate, op_id;
    "vcs.status" => VcsStatus(RootParams) -> GitStatus, scope = Read;
    /// Branches; absent outside a repository.
    "vcs.refs" => VcsRefs(RootParams) -> Option<BranchSnapshot>, scope = Read;
    "vcs.switchRef" => VcsSwitchRef(VcsSwitchRefParams) -> BranchSnapshot, scope = VcsWrite, op_id;
    "vcs.worktrees" => VcsWorktrees(RootParams) -> Vec<GitWorktree>, scope = Read;
    "vcs.createWorktree" => VcsCreateWorktree(VcsCreateWorktreeParams) -> CreatedWorktree, scope = VcsWrite, op_id;
    "vcs.removeWorktree" => VcsRemoveWorktree(VcsRemoveWorktreeParams) -> Ack, scope = VcsWrite, op_id;
    "vcs.commitPreview" => VcsCommitPreview(RootParams) -> CommitSnapshot, scope = Read;
    "vcs.stage" => VcsStage(VcsStageParams) -> GitStatus, scope = VcsWrite, op_id;
    /// Ask a provider to write a commit message for the staged (or all)
    /// changes.
    "vcs.generateCommitMessage" => VcsGenerateCommitMessage(VcsGenerateCommitMessageParams) -> CommitMessage, scope = Operate;
    "vcs.commit" => VcsCommit(VcsCommitParams) -> CommitResult, scope = VcsWrite, op_id;
    "vcs.push" => VcsPush(RootOpParams) -> Ack, scope = VcsWrite, op_id;
    "vcs.pull" => VcsPull(RootOpParams) -> PullResult, scope = VcsWrite, op_id;
    "review.diff" => ReviewDiffGet(ReviewDiffParams) -> ReviewDiff, scope = Read;
    /// Start a shell; attach with the `terminal` stream.
    "terminal.open" => TerminalOpen(TerminalOpenParams) -> TerminalOpened, scope = Terminal, op_id;
    "terminal.resize" => TerminalResize(TerminalResizeParams) -> Ack, scope = Terminal;
    /// Drop the retained output; subscribers get `reset`.
    "terminal.clear" => TerminalClear(TerminalParams) -> Ack, scope = Terminal;
    /// Replace the shell with a fresh one; subscribers get `reset`.
    "terminal.restart" => TerminalRestart(TerminalParams) -> TerminalGeneration, scope = Terminal;
    "terminal.close" => TerminalClose(TerminalParams) -> Ack, scope = Terminal;
    /// Every provider instance with its status.
    "providers.list" => ProvidersList(NoParams) -> Vec<ProviderStatus>, scope = Read;
    "providers.probe" => ProvidersProbe(ProvidersProbeParams) -> ProviderProbeResult, scope = Read;
    /// A provider instance's model catalog.
    "providers.models" => ProvidersModels(ProvidersModelsParams) -> ProviderModels, scope = Read;
    /// The account's plan usage; absent when the provider reports none.
    "providers.planUsage" => ProvidersPlanUsage(InstanceParams) -> Option<PlanUsage>, scope = Read;
    /// One provider's resumable CLI conversations, for `threadImport`.
    "providerSessions.list" => ProviderSessionsList(ProviderSessionsListParams) -> Vec<ProviderSessionSummary>, scope = Read;
    "usage.history" => UsageHistoryGet(UsageHistoryParams) -> UsageHistory, scope = Read;
    "settings.update" => SettingsUpdate(SettingsUpdateParams) -> SettingsSnapshot, scope = SettingsWrite, op_id;
    "storage.worktreeCleanupStatus" => StorageWorktreeCleanupStatus(NoParams) -> WorktreeCleanupStatus, scope = Read;
    /// Run worktree cleanup now with the saved rules.
    "storage.runWorktreeCleanup" => StorageRunWorktreeCleanup(StorageRunCleanupParams) -> WorktreeCleanupReport, scope = SettingsWrite, op_id;
    "computerUse.permissions" => ComputerUsePermissions(ComputerUsePermissionsParams) -> ComputerPermissions, scope = Read;
}

streams! {
    /// `snapshot`, then `updated` on every change; `synchronized` after the
    /// snapshot (or alone when `after` is the current revision).
    "settings" => Settings(SettingsSpec) -> SettingsItem, cursor = Revision, scope = Read;
    /// `meta`, then the blob's bytes as DATA within the credit window with
    /// `FIN` on the last chunk, then `end{completed}`.
    "blobDownload" => BlobDownload(BlobDownloadSpec) -> BlobDownloadItem, cursor = Offset, scope = Read, data;
    /// Navigation rows: a snapshot (or the deltas since `after`), then
    /// `synchronized`, then a delta per changed row.
    "shell" => Shell(ShellSpec) -> ShellItem, cursor = Sequence, scope = Read;
    /// One thread: a bounded snapshot or the replayed gap, `synchronized`,
    /// then its events and live `delta`s.
    "thread" => Thread(ThreadSpec) -> ThreadItem, cursor = Sequence, scope = Read;
    /// The live output of one command, while its row is expanded.
    "commandOutput" => CommandOutput(CommandOutputSpec) -> CommandOutputItem, cursor = None, scope = Read;
    /// Composer drafts: a snapshot, `synchronized`, then every change.
    "drafts" => Drafts(DraftsSpec) -> DraftsItem, cursor = Revision, scope = Read;
    /// Provider statuses and plan usage.
    "providers" => Providers(ProvidersSpec) -> ProvidersItem, cursor = Revision, scope = Read;
    /// One terminal: `attached`, then output as DATA under credit; input as
    /// DATA from the client when `interactive` (which needs `terminal`).
    "terminal" => Terminal(TerminalSpec) -> TerminalItem, cursor = Offset, scope = Read, data;
    /// The open terminals.
    "terminalList" => TerminalList(TerminalListSpec) -> TerminalListItem, cursor = None, scope = Read;
    /// A repository's status while subscribed, at most once a second.
    "vcsStatus" => VcsStatusWatch(VcsStatusSpec) -> VcsStatusItem, cursor = None, scope = Read;
}

/// The params of a method declared with `op_id`.
pub trait OpIdParams {
    fn op_id(&self) -> Uuid;
}

macro_rules! op_id_params {
    ($($params:ty),* $(,)?) => {
        $(impl OpIdParams for $params {
            fn op_id(&self) -> Uuid {
                self.op_id
            }
        })*
    };
}

op_id_params!(
    FilesWriteParams,
    SkillsSetEnabledParams,
    SkillsTrashParams,
    DraftsApplyParams,
    AttachmentsImportParams,
    AttachmentsImportPathParams,
    VcsSwitchRefParams,
    VcsCreateWorktreeParams,
    VcsRemoveWorktreeParams,
    VcsStageParams,
    VcsCommitParams,
    RootOpParams,
    TerminalOpenParams,
    SettingsUpdateParams,
    StorageRunCleanupParams,
);

impl Method {
    /// Every scope the call needs: the method's static scope plus the
    /// payload-dependent ones (RFC 0003 §6.3). `None` for an unknown method.
    pub fn required_scopes(&self) -> Option<Vec<Scope>> {
        let mut scopes = vec![self.scope()?];
        match self {
            Self::SettingsUpdate(params) if params.patch.touches_providers() => {
                scopes.push(Scope::ProvidersManage);
            }
            Self::AttachmentsImportPath(_) => scopes.push(Scope::FilesRead),
            Self::ComputerUsePermissions(params) if params.prompt => scopes.push(Scope::Operate),
            _ => {}
        }
        Some(scopes)
    }
}

impl StreamSpec {
    /// Every scope the subscription needs, payload-dependent ones included.
    pub fn required_scopes(&self) -> Option<Vec<Scope>> {
        let mut scopes = vec![self.scope()?];
        if let Self::Terminal(spec) = self
            && spec.interactive
        {
            scopes.push(Scope::Terminal);
        }
        Some(scopes)
    }
}

/// The params of a method that takes none: an empty map.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct NoParams {}

/// The result of a method that only acknowledges: an empty map.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Ack {}

/// Text that may be too large for a message (RFC 0003 §1.1 `Payload<T>`):
/// inline, or a blob reference to download.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum TextPayload {
    Inline {
        value: String,
    },
    Blob {
        reference: String,
        size: u64,
    },
    #[serde(other)]
    Unknown,
}

/// Where a files, vcs or terminal call operates.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum RootRef {
    Project {
        project_id: Uuid,
    },
    /// The thread's worktree, or its project's root. TODO(#54): resolved
    /// through the orchestrator; answered `unsupported` until then.
    Thread {
        thread_id: Uuid,
    },
    /// An absolute path on the daemon host.
    Path {
        path: String,
    },
    #[serde(other)]
    Unknown,
}

/// Text above this is not sent inline (RFC 0003: `files.read`).
pub const MAX_INLINE_TEXT_BYTES: u64 = 2 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct FilesReadParams {
    pub root: RootRef,
    /// Relative to `root`, with `/` separators.
    pub path: String,
    /// Refuse files larger than this instead of reading them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct FileContent {
    /// The content, when it is text and small enough to send inline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// A blob reference to download the content from, otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob: Option<String>,
    pub binary: bool,
    pub size: u64,
    /// A content hash, the base revision for a later `files.write`.
    pub revision: String,
}

/// A project of the catalog.
///
/// TODO(#54): replaced by the orchestration `Project` entity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ProjectInfo {
    pub id: Uuid,
    pub name: String,
    pub path: String,
    /// Milliseconds since the Unix epoch.
    pub created_at: i64,
}

/// `settings.get` result and settings snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct SettingsSnapshot {
    /// Equal revisions mean equal settings; compare for equality only.
    pub revision: u64,
    pub settings: Settings,
}

/// The daemon's settings as clients see them.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(default, rename_all = "camelCase")]
pub struct Settings {
    pub computer_use_enabled: bool,
    pub computer_use_allowed_apps: Vec<AppGrant>,
    /// Provider instance ids that are switched off.
    pub disabled_providers: Vec<String>,
    /// Binary path per provider instance id.
    pub provider_binary_overrides: BTreeMap<String, String>,
    /// The managed worktree folder and its cleanup rules.
    pub storage: StorageSettings,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(default, rename_all = "camelCase")]
pub struct AppGrant {
    pub bundle_id: String,
    pub app_name: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SettingsSpec {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum SettingsItem {
    Snapshot {
        revision: u64,
        settings: Settings,
    },
    Updated {
        revision: u64,
        settings: Settings,
    },
    /// Everything up to `cursor` has been delivered: data is fresh.
    Synchronized {
        cursor: StreamCursor,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct BlobDownloadSpec {
    /// `waku-blob:<sha256>.<ext>`.
    pub reference: String,
    /// Resume from this byte.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    /// With a `waku-attachment:` reference, the file inside it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum BlobDownloadItem {
    Meta {
        size: u64,
        mime: String,
        /// Lowercase hex.
        sha256: String,
    },
    #[serde(other)]
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;

    commands! {
        /// A stand-in command set for the macro's tests; #54 declares the
        /// real one.
        enum TestCommand;
        "threadArchive" => ThreadArchive(TestThread), scope = Operate;
        "threadVisit" => ThreadVisit(TestThread), scope = Read;
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
    #[serde(rename_all = "camelCase")]
    struct TestThread {
        thread_id: Uuid,
    }

    #[test]
    fn commands_are_an_open_union_with_scopes() {
        let command = TestCommand::ThreadArchive(TestThread {
            thread_id: Uuid::nil(),
        });
        let json = serde_json::to_value(&command).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"type": "threadArchive", "threadId": Uuid::nil()})
        );
        assert_eq!(
            serde_json::from_value::<TestCommand>(json).unwrap(),
            command
        );
        assert_eq!(command.scope(), Some(Scope::Operate));
        assert_eq!(command.static_name(), Some("threadArchive"));
        let internal = serde_json::json!({"type": "thread.stop", "threadId": Uuid::nil()});
        let decoded = serde_json::from_value::<TestCommand>(internal).unwrap();
        assert_eq!(decoded, TestCommand::Unknown);
        assert_eq!(decoded.scope(), None);
        assert_eq!(
            TestCommand::manifest()
                .iter()
                .map(|info| (info.name, info.scope.as_str()))
                .collect::<Vec<_>>(),
            [("threadArchive", "operate"), ("threadVisit", "read")]
        );
    }

    #[test]
    fn method_and_stream_tables_match_the_declarations() {
        use crate::v8::api::{method, stream};
        assert_eq!(<method::FilesRead as MethodDef>::SCOPE, Scope::FilesRead);
        assert_eq!(<method::SettingsGet as MethodDef>::NAME, "settings.get");
        const { assert!(!<method::SettingsGet as MethodDef>::OP_ID) };
        const { assert!(<stream::BlobDownload as StreamDef>::DATA) };
        assert_eq!(
            <stream::Settings as StreamDef>::CURSOR,
            CursorKind::Revision
        );
        let names = methods().iter().map(|info| info.name).collect::<Vec<_>>();
        assert_eq!(
            names[..8],
            [
                "settings.get",
                "projects.list",
                "files.read",
                "orchestration.dispatch",
                "threads.snapshot",
                "threads.historyPage",
                "threads.turnItem",
                "threads.archived",
            ]
        );
        let mut unique = names.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), names.len(), "method names are unique");
        let spec = <stream::Settings as StreamDef>::wrap(SettingsSpec {});
        assert_eq!(
            serde_json::to_value(&spec).unwrap(),
            serde_json::json!({"type": "settings"})
        );
        assert_eq!(
            serde_json::from_value::<StreamSpec>(serde_json::json!({"type": "nebula", "x": 1}))
                .unwrap(),
            StreamSpec::Unknown
        );
    }

    #[test]
    fn mutations_carry_op_ids_and_payloads_add_scopes() {
        use crate::v8::api::{method, stream};
        let op = Uuid::from_u128(7);
        let write = Method::FilesWrite(FilesWriteParams {
            root: RootRef::Path { path: "/r".into() },
            path: "a".into(),
            content: String::new(),
            base_revision: None,
            op_id: op,
        });
        assert_eq!(write.op_id(), Some(op));
        assert_eq!(Method::SettingsGet(NoParams {}).op_id(), None);
        // Every file and repository mutation is retried safely.
        for info in methods() {
            if matches!(info.scope, Scope::FilesWrite | Scope::VcsWrite) {
                assert!(info.op_id, "{} carries no opId", info.name);
            }
        }
        let update = |patch: SettingsPatch| {
            Method::SettingsUpdate(SettingsUpdateParams {
                base_revision: None,
                patch,
                op_id: op,
            })
            .required_scopes()
            .unwrap()
        };
        assert_eq!(update(SettingsPatch::default()), [Scope::SettingsWrite]);
        assert_eq!(
            update(SettingsPatch {
                disabled_providers: Some(Vec::new()),
                ..SettingsPatch::default()
            }),
            [Scope::SettingsWrite, Scope::ProvidersManage]
        );
        let import = Method::AttachmentsImportPath(AttachmentsImportPathParams {
            path: "/a".into(),
            op_id: op,
        });
        assert_eq!(
            import.required_scopes().unwrap(),
            [Scope::Operate, Scope::FilesRead]
        );
        let prompt = Method::ComputerUsePermissions(ComputerUsePermissionsParams { prompt: true });
        assert_eq!(
            prompt.required_scopes().unwrap(),
            [Scope::Read, Scope::Operate]
        );
        assert_eq!(Method::Unknown { name: "x".into() }.required_scopes(), None);
        let spec = |interactive| {
            <stream::Terminal as StreamDef>::wrap(TerminalSpec {
                terminal_id: op,
                interactive,
            })
            .required_scopes()
            .unwrap()
        };
        assert_eq!(spec(false), [Scope::Read]);
        assert_eq!(spec(true), [Scope::Read, Scope::Terminal]);
        const { assert!(<method::TerminalOpen as MethodDef>::OP_ID) };
        const { assert!(<stream::Terminal as StreamDef>::DATA) };
    }
}
