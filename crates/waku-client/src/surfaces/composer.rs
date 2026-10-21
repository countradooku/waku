//! Composer sources and skills: `composer.slashCommands` and `skills.*`.
//!
//! Results are converted to the desktop's domain types, which the composer's
//! completion ranking and the skills page already work on.

use std::path::{Path, PathBuf};

use uuid::Uuid;

use super::DaemonV8;
use super::files::path_root;
use super::providers::{instance_id, project_ids_for_paths, provider_kind};
use crate::v8::{
    self, ComposerSlashCommandsParams, SkillsListParams, SkillsSetEnabledParams, SkillsTrashParams,
    method,
};
use waku_protocol::composer::{CommandScope, SlashCommand};
use waku_protocol::model::ProviderKind;
use waku_protocol::skills::{SkillEntry, SkillInstall, SkillScope, SkillSource, SkillsCatalog};

pub fn slash_command(command: v8::SlashCommand) -> Option<SlashCommand> {
    Some(SlashCommand {
        name: command.name,
        description: command.description,
        scope: match command.scope {
            v8::CommandScope::Project => CommandScope::Project,
            v8::CommandScope::User => CommandScope::User,
            v8::CommandScope::Skill => CommandScope::Skill,
            v8::CommandScope::Builtin => CommandScope::Builtin,
            v8::CommandScope::Waku => CommandScope::Waku,
            // A scope this build does not know is not offered.
            _ => return None,
        },
        argument_hint: command.argument_hint,
        template: command.template,
    })
}

/// The slash commands `provider` offers in the project at `root`, with the
/// binary the daemon's settings name.
pub fn slash_commands(
    daemon: &DaemonV8,
    provider: ProviderKind,
    root: &Path,
) -> anyhow::Result<Vec<SlashCommand>> {
    let commands = daemon.call::<method::ComposerSlashCommands>(ComposerSlashCommandsParams {
        root: path_root(root),
        instance_id: Some(instance_id(provider)),
        thread_id: None,
    })?;
    Ok(commands.into_iter().filter_map(slash_command).collect())
}

fn skill_source(source: &str) -> SkillSource {
    provider_kind(source).map_or(SkillSource::Shared, SkillSource::Provider)
}

pub fn skill_entry(skill: v8::Skill) -> SkillEntry {
    SkillEntry {
        name: skill.name,
        description: skill.description,
        scope: match skill.scope {
            v8::SkillScope::Project => SkillScope::Project,
            _ => SkillScope::User,
        },
        project: skill.project,
        installs: skill
            .installs
            .into_iter()
            .map(|install| SkillInstall {
                source: skill_source(&install.source),
                dir: PathBuf::from(install.dir),
                skill_file: PathBuf::from(install.skill_file),
                enabled: install.enabled,
            })
            .collect(),
        enabled: skill.enabled,
        allowed_tools: skill.allowed_tools,
        body: skill.body,
        supporting_files: usize::try_from(skill.supporting_files).unwrap_or(usize::MAX),
        total_bytes: skill.total_bytes,
        // The page shows seconds; the wire carries milliseconds.
        modified_at: skill
            .modified_at
            .and_then(|ms| u64::try_from(ms.div_euclid(1000)).ok()),
        duplicates: usize::try_from(skill.duplicates).unwrap_or(usize::MAX),
        row_key: skill.row_key,
    }
}

/// User skills plus those of the projects at `project_paths`. A skill
/// without an install has nothing to show and is skipped.
pub fn skills(daemon: &DaemonV8, project_paths: &[PathBuf]) -> anyhow::Result<SkillsCatalog> {
    let project_ids = project_ids_for_paths(daemon, project_paths)?;
    let catalog = daemon.call::<method::SkillsList>(SkillsListParams { project_ids })?;
    Ok(SkillsCatalog {
        skills: catalog
            .skills
            .into_iter()
            .filter(|skill| !skill.installs.is_empty())
            .map(skill_entry)
            .collect(),
    })
}

fn dir_strings(dirs: &[PathBuf]) -> Vec<String> {
    dirs.iter()
        .map(|dir| dir.to_string_lossy().into_owned())
        .collect()
}

pub fn set_skills_enabled(
    daemon: &DaemonV8,
    dirs: &[PathBuf],
    enabled: bool,
) -> anyhow::Result<()> {
    daemon.call::<method::SkillsSetEnabled>(SkillsSetEnabledParams {
        dirs: dir_strings(dirs),
        enabled,
        op_id: Uuid::new_v4(),
    })?;
    Ok(())
}

pub fn trash_skills(daemon: &DaemonV8, dirs: &[PathBuf]) -> anyhow::Result<()> {
    daemon.call::<method::SkillsTrash>(SkillsTrashParams {
        dirs: dir_strings(dirs),
        op_id: Uuid::new_v4(),
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_command_scopes_are_not_offered() {
        let command = |scope| v8::SlashCommand {
            name: "review".into(),
            description: "Review".into(),
            scope,
            argument_hint: Some("<path>".into()),
            template: None,
        };
        let converted = slash_command(command(v8::CommandScope::Project)).unwrap();
        assert_eq!(converted.scope, CommandScope::Project);
        assert_eq!(converted.argument_hint.as_deref(), Some("<path>"));
        assert!(slash_command(command(v8::CommandScope::Unknown("nebula".into()))).is_none());
    }

    #[test]
    fn skill_installs_regain_their_sources_and_seconds() {
        let entry = skill_entry(v8::Skill {
            name: "lint".into(),
            description: "Lint".into(),
            scope: v8::SkillScope::Project,
            project: Some("app".into()),
            installs: vec![
                v8::SkillInstall {
                    source: "claude".into(),
                    dir: "/srv/app/.claude/skills/lint".into(),
                    skill_file: "/srv/app/.claude/skills/lint/SKILL.md".into(),
                    enabled: true,
                },
                v8::SkillInstall {
                    source: "shared".into(),
                    dir: "/home/u/.agents/skills/lint".into(),
                    skill_file: "/home/u/.agents/skills/lint/SKILL.md".into(),
                    enabled: true,
                },
            ],
            enabled: true,
            allowed_tools: None,
            body: "Run the linter.".into(),
            supporting_files: 2,
            total_bytes: 120,
            modified_at: Some(1_700_000_000_500),
            duplicates: 1,
            row_key: 9,
        });
        assert_eq!(entry.scope, SkillScope::Project);
        assert_eq!(
            entry.primary().source,
            SkillSource::Provider(ProviderKind::Claude)
        );
        assert_eq!(entry.installs[1].source, SkillSource::Shared);
        assert_eq!(
            entry.primary().dir,
            PathBuf::from("/srv/app/.claude/skills/lint")
        );
        assert_eq!(entry.modified_at, Some(1_700_000_000));
        assert_eq!((entry.supporting_files, entry.duplicates), (2, 1));
    }
}
