//! `composer.slashCommands`, `skills.*`, `drafts.*` and the `drafts`
//! stream.

use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::broadcast;
use uuid::Uuid;
use waku_protocol::v8::{
    Ack, CommandScope, ComposerDraft, ComposerDrafts, ComposerSlashCommandsParams, DraftAttachment,
    DraftChange, DraftTarget, DraftsApplyParams, DraftsItem, DraftsRevision, DraftsSnapshot,
    EndReason, Error, ErrorCode, ErrorDetails, MAX_SAFE_INTEGER, Skill, SkillInstall, SkillScope,
    SkillsCatalog, SkillsListParams, SkillsSetEnabledParams, SkillsTrashParams, SlashCommand,
    StreamCursor,
};

use super::{
    Domain, absolute_path, bad_request, call, content_revision, display_path, not_found,
    provider_kind, seconds_to_ms, workspace,
};
use crate::persistence::{
    ComposerDraft as StoredDraft, ComposerDraftAttachment, ComposerDraftChange,
    ComposerDraftTarget, ComposerDrafts as StoredDrafts,
};
use crate::v8::V8Context;
use crate::v8::connection::StreamSink;
use crate::v8::methods::root_path;
use crate::v8::orchestration::{live_project, live_threads, projects};
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
    use waku_protocol::composer::CommandScope as Scope;
    Ok(commands
        .into_iter()
        .map(|command| SlashCommand {
            name: command.name,
            description: command.description,
            scope: match command.scope {
                Scope::Project => CommandScope::Project,
                Scope::User => CommandScope::User,
                Scope::Skill => CommandScope::Skill,
                Scope::Builtin => CommandScope::Builtin,
                Scope::Waku => CommandScope::Waku,
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
    let catalog = call(context.backend.skills(projects), Domain::General)?;
    Ok(SkillsCatalog {
        skills: catalog.skills.into_iter().map(wire_skill).collect(),
    })
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
        context.backend.set_skills_enabled(dirs, params.enabled),
        Domain::General,
    )?;
    Ok(Ack {})
}

pub(super) fn trash_skills(context: &V8Context, params: SkillsTrashParams) -> Result<Ack, Error> {
    let dirs = skill_dirs(&params.dirs)?;
    call(context.backend.trash_skills(dirs), Domain::General)?;
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

fn wire_draft(draft: StoredDraft) -> ComposerDraft {
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

fn stored_draft(draft: ComposerDraft) -> StoredDraft {
    StoredDraft {
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

fn wire_drafts(drafts: StoredDrafts) -> ComposerDrafts {
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

fn stored_drafts(context: &V8Context) -> Result<StoredDrafts, Error> {
    call(context.backend.composer_drafts(), Domain::General)
}

/// The keys of `drafts` that name no live orchestration project or thread
/// (D6: drafts are keyed by orchestration ids, never legacy session ids):
/// projects first, then threads.
fn stale_keys(context: &V8Context, drafts: &StoredDrafts) -> Result<(Vec<Uuid>, Vec<Uuid>), Error> {
    let mut projects = Vec::new();
    for &project_id in drafts.new_sessions.keys() {
        if !live_project(context, project_id)? {
            projects.push(project_id);
        }
    }
    let live = live_threads(context, drafts.sessions.keys().copied())?;
    let threads = drafts
        .sessions
        .keys()
        .filter(|thread_id| !live.contains(thread_id))
        .copied()
        .collect();
    Ok((projects, threads))
}

/// The drafts of live projects and threads; stale ones are pruned by the
/// next `drafts.apply`.
fn load_drafts(context: &V8Context) -> Result<DraftsSnapshot, Error> {
    let mut stored = stored_drafts(context)?;
    let (projects, threads) = stale_keys(context, &stored)?;
    for project_id in projects {
        stored.new_sessions.remove(&project_id);
    }
    for thread_id in threads {
        stored.sessions.remove(&thread_id);
    }
    let drafts = wire_drafts(stored);
    Ok(DraftsSnapshot {
        revision: content_revision(&drafts),
        drafts,
    })
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
    let mut stored = Vec::with_capacity(params.changes.len());
    let mut changes = Vec::with_capacity(params.changes.len());
    for change in params.changes {
        // An empty draft is a removal.
        let draft = change
            .draft
            .filter(|draft| !draft.text.is_empty() || !draft.attachments.is_empty());
        // The stored document names its keys sessions; they are orchestration
        // ids. Only a live project or thread gets a draft; removing one of
        // anything else is a no-op.
        let target = match change.target {
            DraftTarget::NewThread { project_id } => {
                if draft.is_some() && !live_project(context, project_id)? {
                    return Err(not_found("project", project_id.to_string()));
                }
                ComposerDraftTarget::NewSession { project_id }
            }
            DraftTarget::Thread { thread_id } => {
                if draft.is_some() && live_threads(context, [thread_id])?.is_empty() {
                    return Err(not_found("thread", thread_id.to_string()));
                }
                ComposerDraftTarget::Session {
                    session_id: thread_id,
                }
            }
            DraftTarget::Unknown => return Err(bad_request("changes.target", "unknown target")),
        };
        stored.push(ComposerDraftChange {
            target,
            draft: draft.clone().map(stored_draft),
        });
        changes.push(DraftChange {
            target: change.target,
            draft,
        });
    }
    // Prune the drafts of removed projects and deleted threads (and any
    // legacy session ids) on the way.
    let (stale_projects, stale_threads) = stale_keys(context, &stored_drafts(context)?)?;
    stored.extend(
        stale_projects
            .into_iter()
            .map(|project_id| ComposerDraftTarget::NewSession { project_id })
            .chain(
                stale_threads
                    .into_iter()
                    .map(|session_id| ComposerDraftTarget::Session { session_id }),
            )
            .map(|target| ComposerDraftChange {
                target,
                draft: None,
            }),
    );
    call(
        context.backend.apply_composer_draft_changes(stored),
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
