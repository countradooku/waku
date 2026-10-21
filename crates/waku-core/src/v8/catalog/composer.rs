//! `composer.slashCommands`, `skills.*`, `drafts.*` and the `drafts`
//! stream.

use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::broadcast;
use waku_protocol::v8::{
    Ack, CommandScope, ComposerDraft, ComposerDrafts, ComposerSlashCommandsParams, DraftAttachment,
    DraftChange, DraftTarget, DraftsApplyParams, DraftsItem, DraftsRevision, DraftsSnapshot,
    EndReason, Error, ErrorCode, ErrorDetails, MAX_SAFE_INTEGER, Skill, SkillInstall, SkillScope,
    SkillsCatalog, SkillsListParams, SkillsSetEnabledParams, SkillsTrashParams, SlashCommand,
    StreamCursor,
};

use super::{
    Domain, absolute_path, bad_request, call, content_revision, display_path, provider_kind,
    seconds_to_ms, unexpected, workspace,
};
use crate::persistence::{
    ComposerDraft as LegacyDraft, ComposerDraftAttachment, ComposerDraftChange,
    ComposerDraftTarget, ComposerDrafts as LegacyDrafts,
};
use crate::protocol::{Command, ResponsePayload};
use crate::v8::V8Context;
use crate::v8::connection::StreamSink;
use crate::v8::methods::root_path;
use crate::v8::orchestration::projects;
use crate::workspace::{WorkspaceOperation, WorkspaceResult};

pub(super) fn slash_commands(
    context: &V8Context,
    params: ComposerSlashCommandsParams,
) -> Result<Vec<SlashCommand>, Error> {
    let project_root = root_path(context, &params.root)?;
    let instance = params
        .instance_id
        .ok_or_else(|| bad_request("instanceId", "required"))?;
    let provider = provider_kind(&instance)?;
    let settings = super::providers::daemon_settings(context)?;
    let WorkspaceResult::SlashCommands { commands } = workspace(
        context,
        WorkspaceOperation::DiscoverSlashCommands {
            provider,
            project_root,
            binary_override: settings.provider_binary_overrides.get(&provider).cloned(),
        },
        Domain::Provider,
    )?
    else {
        return Err(super::internal("unexpected slash command result"));
    };
    use waku_protocol::composer::CommandScope as Legacy;
    Ok(commands
        .into_iter()
        .map(|command| SlashCommand {
            name: command.name,
            description: command.description,
            scope: match command.scope {
                Legacy::Project => CommandScope::Project,
                Legacy::User => CommandScope::User,
                Legacy::Skill => CommandScope::Skill,
                Legacy::Builtin => CommandScope::Builtin,
                Legacy::Waku => CommandScope::Waku,
            },
            argument_hint: command.argument_hint,
            template: command.template,
        })
        .collect())
}

// -- skills -----------------------------------------------------------------

fn skill_source(source: crate::skills::SkillSource) -> String {
    match source {
        crate::skills::SkillSource::Shared => "shared".into(),
        crate::skills::SkillSource::Provider(provider) => provider.id().into(),
    }
}

fn wire_skill(skill: crate::skills::SkillEntry) -> Skill {
    Skill {
        name: skill.name,
        description: skill.description,
        scope: match skill.scope {
            crate::skills::SkillScope::User => SkillScope::User,
            crate::skills::SkillScope::Project => SkillScope::Project,
        },
        project: skill.project,
        installs: skill
            .installs
            .into_iter()
            .map(|install| SkillInstall {
                source: skill_source(install.source),
                dir: display_path(&install.dir),
                skill_file: display_path(&install.skill_file),
                enabled: install.enabled,
            })
            .collect(),
        enabled: skill.enabled,
        allowed_tools: skill.allowed_tools,
        body: skill.body,
        supporting_files: skill.supporting_files as u64,
        total_bytes: skill.total_bytes,
        modified_at: skill.modified_at.map(seconds_to_ms),
        duplicates: skill.duplicates as u64,
        row_key: skill.row_key & MAX_SAFE_INTEGER,
    }
}

pub(super) fn skills(
    context: &V8Context,
    params: SkillsListParams,
) -> Result<SkillsCatalog, Error> {
    let projects = if params.project_ids.is_empty() {
        Vec::new()
    } else {
        projects(context)?
            .into_iter()
            .filter(|project| params.project_ids.contains(&project.id.0))
            .map(|project| (project.name, project.path))
            .collect()
    };
    match call(context, Command::LoadSkills { projects }, Domain::General)? {
        ResponsePayload::SkillsCatalog { catalog } => Ok(SkillsCatalog {
            skills: catalog.skills.into_iter().map(wire_skill).collect(),
        }),
        other => Err(unexpected(&other)),
    }
}

fn skill_dirs(dirs: &[String]) -> Result<Vec<PathBuf>, Error> {
    dirs.iter().map(|dir| absolute_path("dirs", dir)).collect()
}

pub(super) fn set_skills_enabled(
    context: &V8Context,
    params: SkillsSetEnabledParams,
) -> Result<Ack, Error> {
    let dirs = skill_dirs(&params.dirs)?;
    call(
        context,
        Command::SetSkillsEnabled {
            dirs,
            enabled: params.enabled,
        },
        Domain::General,
    )?;
    Ok(Ack {})
}

pub(super) fn trash_skills(context: &V8Context, params: SkillsTrashParams) -> Result<Ack, Error> {
    let dirs = skill_dirs(&params.dirs)?;
    call(context, Command::TrashSkills { dirs }, Domain::General)?;
    Ok(Ack {})
}

// -- drafts -----------------------------------------------------------------

/// Applied draft changes for `drafts` subscribers.
pub(crate) struct DraftsBus {
    sender: broadcast::Sender<(u64, Arc<Vec<DraftChange>>)>,
    /// Serializes `drafts.apply`, so its `baseRevision` check, the write and
    /// the revision it reports belong together.
    apply: Mutex<()>,
}

impl Default for DraftsBus {
    fn default() -> Self {
        Self {
            sender: broadcast::channel(256).0,
            apply: Mutex::new(()),
        }
    }
}

fn wire_draft(draft: LegacyDraft) -> ComposerDraft {
    ComposerDraft {
        text: draft.text,
        attachments: draft
            .attachments
            .into_iter()
            .map(|attachment| DraftAttachment {
                path: display_path(&attachment.path),
                mention: attachment.mention,
                name: attachment.name,
                is_dir: attachment.is_dir,
                is_image: attachment.is_image,
                blob_reference: attachment.blob_reference,
            })
            .collect(),
    }
}

fn legacy_draft(draft: ComposerDraft) -> LegacyDraft {
    LegacyDraft {
        text: draft.text,
        attachments: draft
            .attachments
            .into_iter()
            .map(|attachment| ComposerDraftAttachment {
                path: PathBuf::from(attachment.path),
                mention: attachment.mention,
                name: attachment.name,
                is_dir: attachment.is_dir,
                is_image: attachment.is_image,
                blob_reference: attachment.blob_reference,
            })
            .collect(),
    }
}

fn wire_drafts(drafts: LegacyDrafts) -> ComposerDrafts {
    ComposerDrafts {
        new_threads: drafts
            .new_sessions
            .into_iter()
            .map(|(id, draft)| (id, wire_draft(draft)))
            .collect(),
        threads: drafts
            .sessions
            .into_iter()
            .map(|(id, draft)| (id, wire_draft(draft)))
            .collect(),
    }
}

fn load_drafts(context: &V8Context) -> Result<DraftsSnapshot, Error> {
    match call(context, Command::LoadComposerDrafts, Domain::General)? {
        ResponsePayload::ComposerDrafts { drafts } => {
            let drafts = wire_drafts(drafts);
            Ok(DraftsSnapshot {
                revision: content_revision(&drafts),
                drafts,
            })
        }
        other => Err(unexpected(&other)),
    }
}

pub(super) fn drafts(context: &V8Context) -> Result<DraftsSnapshot, Error> {
    load_drafts(context)
}

pub(super) fn apply_drafts(
    context: &V8Context,
    params: DraftsApplyParams,
) -> Result<DraftsRevision, Error> {
    let bus = &context.catalog.drafts;
    let _apply = bus.apply.lock();
    let current = load_drafts(context)?.revision;
    if let Some(base) = params.base_revision
        && base != current
    {
        return Err(Error::new(
            ErrorCode::Conflict,
            format!("the drafts changed since revision {base}"),
        )
        .with_details(ErrorDetails::RevisionConflict {
            current: current.to_string(),
        }));
    }
    let mut legacy = Vec::with_capacity(params.changes.len());
    let mut changes = Vec::with_capacity(params.changes.len());
    for change in params.changes {
        let target = match change.target {
            DraftTarget::NewThread { project_id } => ComposerDraftTarget::NewSession { project_id },
            DraftTarget::Thread { thread_id } => ComposerDraftTarget::Session {
                session_id: thread_id,
            },
            DraftTarget::Unknown => return Err(bad_request("changes.target", "unknown target")),
        };
        // An empty draft is a removal.
        let draft = change
            .draft
            .filter(|draft| !draft.text.is_empty() || !draft.attachments.is_empty());
        legacy.push(ComposerDraftChange {
            target,
            draft: draft.clone().map(legacy_draft),
        });
        changes.push(DraftChange {
            target: change.target,
            draft,
        });
    }
    call(
        context,
        Command::ApplyComposerDraftChanges { changes: legacy },
        Domain::General,
    )?;
    let revision = load_drafts(context)?.revision;
    if revision != current {
        let _ = bus.sender.send((revision, Arc::new(changes)));
    }
    Ok(DraftsRevision { revision })
}

/// `drafts`: a snapshot (skipped when `after` is current), `synchronized`,
/// then the changes of every apply.
pub(super) async fn drafts_stream(
    sink: StreamSink,
    context: Arc<V8Context>,
    after: Option<StreamCursor>,
) {
    let mut changes = context.catalog.drafts.sender.subscribe();
    let mut after = after;
    loop {
        let task_context = context.clone();
        let snapshot = match tokio::task::spawn_blocking(move || load_drafts(&task_context)).await {
            Ok(Ok(snapshot)) => snapshot,
            Ok(Err(error)) => {
                sink.end(EndReason::Error { error });
                return;
            }
            Err(_) => return,
        };
        let mut revision = snapshot.revision;
        let cursor = StreamCursor::Revision { revision };
        if after.take().as_ref() != Some(&cursor) {
            let item = DraftsItem::Snapshot {
                revision,
                drafts: snapshot.drafts,
            };
            if sink.push(&item, Some(cursor.clone())).is_err() {
                return;
            }
        }
        if sink
            .push(
                &DraftsItem::Synchronized {
                    cursor: cursor.clone(),
                },
                Some(cursor),
            )
            .is_err()
        {
            return;
        }
        loop {
            match changes.recv().await {
                Ok((next, applied)) => {
                    // Already part of the snapshot.
                    if next == revision {
                        continue;
                    }
                    revision = next;
                    let item = DraftsItem::Changed {
                        revision,
                        changes: applied.as_ref().clone(),
                    };
                    if sink
                        .push(&item, Some(StreamCursor::Revision { revision }))
                        .is_err()
                    {
                        return;
                    }
                }
                // Fell behind: start over from a snapshot.
                Err(broadcast::error::RecvError::Lagged(_)) => break,
                Err(broadcast::error::RecvError::Closed) => return,
            }
        }
    }
}
