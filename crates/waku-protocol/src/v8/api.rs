//! The v8 methods and streams (RFC 0003 §5), declared with [`methods!`] and
//! [`streams!`].
//!
//! This is the first vertical slice of #53: methods that already have a v7
//! backend and need no orchestration. Everything else in RFC 0003 §5.2–§5.4
//! is added here as its backend lands.
//!
//! TODO(#54): `orchestration.dispatch` (with the `Command` union declared by
//! `commands!` over `crate::orchestration`), the `threads.*` reads, and the
//! `shell` and `thread` streams (`DomainEvent`, `TurnItem`) plug in here once
//! `waku_protocol::orchestration` is merged. `projects.list` then returns the
//! orchestration `Project` entity instead of [`ProjectInfo`].

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use uuid::Uuid;

use super::{Scope, StreamCursor};

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
}

streams! {
    /// `snapshot`, then `updated` on every change; `synchronized` after the
    /// snapshot (or alone when `after` is the current revision).
    "settings" => Settings(SettingsSpec) -> SettingsItem, cursor = Revision, scope = Read;
    /// `meta`, then the blob's bytes as DATA within the credit window with
    /// `FIN` on the last chunk, then `end{completed}`.
    "blobDownload" => BlobDownload(BlobDownloadSpec) -> BlobDownloadItem, cursor = Offset, scope = Read, data;
}

/// The params of a method that takes none: an empty map.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct NoParams {}

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
        assert_eq!(names, ["settings.get", "projects.list", "files.read"]);
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
}
