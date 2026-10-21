//! Composer completion, skills and drafts (RFC 0003 §5.2 "Composer, skills
//! and drafts", §5.4 `drafts`).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{RootRef, StreamCursor};

/// `composer.slashCommands` params.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ComposerSlashCommandsParams {
    /// The project the commands are discovered in.
    pub root: RootRef,
    /// The provider instance whose commands are listed. Required until a
    /// thread's own instance can stand in for it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<Uuid>,
}

open_string_enum! {
    /// Where a slash command comes from.
    pub enum CommandScope {
        Project = "project",
        User = "user",
        Skill = "skill",
        Builtin = "builtin",
        Waku = "waku",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct SlashCommand {
    pub name: String,
    pub description: String,
    pub scope: CommandScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub argument_hint: Option<String>,
    /// The prompt template a Waku command expands to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
}

// -- skills -----------------------------------------------------------------

/// `skills.list` params: user skills plus those of these projects.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct SkillsListParams {
    #[serde(default)]
    pub project_ids: Vec<Uuid>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct SkillsCatalog {
    pub skills: Vec<Skill>,
}

open_string_enum! {
    pub enum SkillScope {
        User = "user",
        Project = "project",
    }
}

/// One skill, merged across the places it is installed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct Skill {
    pub name: String,
    pub description: String,
    pub scope: SkillScope,
    /// The project's name, for project skills.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    /// The first install is the primary one.
    pub installs: Vec<SkillInstall>,
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_tools: Option<String>,
    pub body: String,
    pub supporting_files: u64,
    pub total_bytes: u64,
    /// Milliseconds since the Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_at: Option<i64>,
    pub duplicates: u64,
    /// A stable key for list rows (53 bits).
    pub row_key: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct SkillInstall {
    /// `shared` (`~/.agents/skills` and the like) or the provider id whose
    /// folder holds it.
    pub source: String,
    /// The skill directory on the daemon host.
    pub dir: String,
    pub skill_file: String,
    pub enabled: bool,
}

/// `skills.setEnabled` params.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct SkillsSetEnabledParams {
    /// Skill directories (`SkillInstall.dir`).
    pub dirs: Vec<String>,
    pub enabled: bool,
    pub op_id: Uuid,
}

/// `skills.trash` params: move skill directories to the trash.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct SkillsTrashParams {
    pub dirs: Vec<String>,
    pub op_id: Uuid,
}

// -- drafts -----------------------------------------------------------------

/// Unsent composer text, per project (a thread not started yet) and per
/// thread.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(default, rename_all = "camelCase")]
pub struct ComposerDrafts {
    /// By project id.
    pub new_threads: BTreeMap<Uuid, ComposerDraft>,
    /// By thread id.
    pub threads: BTreeMap<Uuid, ComposerDraft>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(default, rename_all = "camelCase")]
pub struct ComposerDraft {
    pub text: String,
    pub attachments: Vec<DraftAttachment>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct DraftAttachment {
    /// Daemon-host path.
    pub path: String,
    /// The `@` mention text in the draft.
    pub mention: String,
    pub name: String,
    pub is_dir: bool,
    pub is_image: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob_reference: Option<String>,
}

/// Which draft a change applies to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum DraftTarget {
    /// The composer of a project's next thread.
    NewThread {
        project_id: Uuid,
    },
    Thread {
        thread_id: Uuid,
    },
    #[serde(other)]
    Unknown,
}

/// Set one draft, or remove it with `draft` absent (an empty draft is a
/// removal too).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct DraftChange {
    pub target: DraftTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub draft: Option<ComposerDraft>,
}

/// `drafts.get` result. `revision` is a content hash: compare for equality
/// only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct DraftsSnapshot {
    pub revision: u64,
    pub drafts: ComposerDrafts,
}

/// `drafts.apply` params. Changes are keyed, so concurrent clients only
/// conflict on the same draft; `baseRevision` makes the whole apply
/// conditional (`conflict` with `revisionConflict` otherwise).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct DraftsApplyParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_revision: Option<u64>,
    pub changes: Vec<DraftChange>,
    pub op_id: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct DraftsRevision {
    pub revision: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DraftsSpec {}

/// `drafts` items: a snapshot (skipped when `after` is the current
/// revision), `synchronized`, then the changes of every apply.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum DraftsItem {
    Snapshot {
        revision: u64,
        drafts: ComposerDrafts,
    },
    Changed {
        revision: u64,
        changes: Vec<DraftChange>,
    },
    Synchronized {
        cursor: StreamCursor,
    },
    #[serde(other)]
    Unknown,
}
