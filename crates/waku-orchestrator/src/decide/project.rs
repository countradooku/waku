//! Project commands (R8).

use waku_protocol::orchestration::*;

use super::{DecideContext, Decision};
use crate::command::CommandError;

fn required_name(name: &str) -> Result<String, CommandError> {
    let name = name.trim();
    if name.is_empty() {
        return Err(CommandError::invalid("name", "must not be empty"));
    }
    Ok(name.to_owned())
}

fn live_project<'a>(
    cx: &DecideContext<'a>,
    project_id: ProjectId,
) -> Result<&'a Project, CommandError> {
    cx.project
        .filter(|project| project.id == project_id && project.removed_at.is_none())
        .ok_or_else(|| CommandError::not_found("project", project_id))
}

/// A default order key that sorts by creation time.
pub(super) fn time_order_key(now: Timestamp) -> OrderKey {
    OrderKey::new(format!("t{now:016}"))
}

pub(super) fn decide(
    cx: &mut DecideContext<'_>,
    command: &Command,
) -> Result<Decision, CommandError> {
    let now = cx.now;
    let project = match command {
        Command::ProjectCreate {
            project_id,
            name,
            path,
            order_key,
        } => {
            if cx.project.is_some_and(|project| project.id == *project_id) {
                return Err(CommandError::conflict(format!(
                    "project {project_id} already exists"
                )));
            }
            if path.as_os_str().is_empty() {
                return Err(CommandError::invalid("path", "must not be empty"));
            }
            Project {
                id: *project_id,
                name: required_name(name)?,
                path: path.clone(),
                order_key: order_key.clone().unwrap_or_else(|| time_order_key(now)),
                created_at: now,
                updated_at: now,
                removed_at: None,
            }
        }
        Command::ProjectRename { project_id, name } => Project {
            name: required_name(name)?,
            updated_at: now,
            ..live_project(cx, *project_id)?.clone()
        },
        Command::ProjectReorder {
            project_id,
            order_key,
        } => Project {
            order_key: order_key.clone(),
            updated_at: now,
            ..live_project(cx, *project_id)?.clone()
        },
        Command::ProjectRemove { project_id } => Project {
            removed_at: Some(now),
            updated_at: now,
            ..live_project(cx, *project_id)?.clone()
        },
        _ => unreachable!("routed by decide_client"),
    };
    Ok(Decision {
        events: vec![DomainEvent::ProjectUpdated(project)],
        ..Decision::default()
    })
}
