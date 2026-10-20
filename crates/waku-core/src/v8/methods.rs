//! v8 method handlers. They run on the blocking pool.

use std::path::{Component, Path, PathBuf};

use sha2::{Digest as _, Sha256};
use waku_protocol::v8::{
    Error, ErrorCode, ErrorDetails, FileContent, FilesReadParams, MAX_INLINE_TEXT_BYTES, Method,
    MethodResult, ProjectInfo, RootRef, SettingsSnapshot,
};

use super::V8Context;
use crate::model::Project;
use crate::protocol::{Command, ResponsePayload};

/// Bytes inspected to tell text from binary.
const BINARY_SNIFF_BYTES: usize = 8 * 1024;

pub(crate) fn handle(context: &V8Context, method: Method) -> Result<MethodResult, Error> {
    match method {
        Method::SettingsGet(_) => {
            let state = context
                .backend
                .load_settings(&context.settings)
                .map_err(internal)?;
            Ok(MethodResult::SettingsGet(SettingsSnapshot {
                revision: state.revision,
                settings: state.settings.clone(),
            }))
        }
        Method::ProjectsList(_) => Ok(MethodResult::ProjectsList(
            projects(context)?.iter().map(project_info).collect(),
        )),
        Method::FilesRead(params) => files_read(context, params).map(MethodResult::FilesRead),
        method @ (Method::OrchestrationDispatch(_)
        | Method::ThreadsSnapshot(_)
        | Method::ThreadsHistoryPage(_)
        | Method::ThreadsTurnItem(_)
        | Method::ThreadsArchived(_)) => {
            super::orchestration::handle(context, method).expect("an orchestration method")
        }
        Method::Unknown { name } => Err(Error::new(
            ErrorCode::UnknownMethod,
            format!("unknown method {name}"),
        )),
    }
}

fn internal(error: anyhow::Error) -> Error {
    Error::new(ErrorCode::Internal, format!("{error:#}"))
}

/// TODO(#54): the orchestrator's project entities; until then the legacy
/// task state.
fn projects(context: &V8Context) -> Result<Vec<Project>, Error> {
    match context
        .backend
        .call(Command::LoadTaskState)
        .map_err(internal)?
    {
        ResponsePayload::TaskState { projects, .. } => Ok(projects),
        _ => Err(Error::new(
            ErrorCode::Internal,
            "unexpected task state response",
        )),
    }
}

fn project_info(project: &Project) -> ProjectInfo {
    ProjectInfo {
        id: project.id,
        name: project.name.clone(),
        path: project.path.to_string_lossy().into_owned(),
        created_at: i64::try_from(project.created_at)
            .unwrap_or(i64::MAX / 1000)
            .saturating_mul(1000),
    }
}

fn root_path(context: &V8Context, root: &RootRef) -> Result<PathBuf, Error> {
    match root {
        RootRef::Path { path } => {
            let path = PathBuf::from(path);
            if !path.is_absolute() {
                return Err(
                    Error::new(ErrorCode::BadRequest, "a path root must be absolute").with_details(
                        ErrorDetails::Validation {
                            field: "root.path".into(),
                            reason: "not absolute".into(),
                        },
                    ),
                );
            }
            Ok(path)
        }
        RootRef::Project { project_id } => projects(context)?
            .into_iter()
            .find(|project| project.id == *project_id)
            .map(|project| project.path)
            .ok_or_else(|| {
                Error::new(ErrorCode::NotFound, "no such project").with_details(
                    ErrorDetails::NotFound {
                        resource: "project".into(),
                        id: project_id.to_string(),
                    },
                )
            }),
        RootRef::Thread { thread_id } => super::orchestration::thread_root(
            context,
            waku_protocol::orchestration::ThreadId(*thread_id),
        ),
        RootRef::Unknown => Err(Error::new(ErrorCode::BadRequest, "unknown root kind")),
    }
}

/// `root` joined with a relative, `/`-separated path that cannot leave it.
fn resolve(root: &Path, relative: &str) -> Result<PathBuf, Error> {
    let relative = Path::new(relative);
    let valid = !relative.as_os_str().is_empty()
        && relative
            .components()
            .all(|component| matches!(component, Component::Normal(_)));
    if !valid {
        return Err(Error::new(
            ErrorCode::BadRequest,
            "path must be a non-empty relative path",
        )
        .with_details(ErrorDetails::Validation {
            field: "path".into(),
            reason: "not a plain relative path".into(),
        }));
    }
    Ok(root.join(relative))
}

fn io_error(error: &std::io::Error, path: &str) -> Error {
    if error.kind() == std::io::ErrorKind::NotFound {
        return Error::new(ErrorCode::NotFound, format!("{path} does not exist")).with_details(
            ErrorDetails::NotFound {
                resource: "file".into(),
                id: path.into(),
            },
        );
    }
    Error::new(ErrorCode::Io, format!("{path}: {error}"))
}

fn files_read(context: &V8Context, params: FilesReadParams) -> Result<FileContent, Error> {
    let root = root_path(context, &params.root)?;
    let path = resolve(&root, &params.path)?;
    let metadata = std::fs::metadata(&path).map_err(|error| io_error(&error, &params.path))?;
    if !metadata.is_file() {
        return Err(Error::new(
            ErrorCode::BadRequest,
            format!("{} is not a file", params.path),
        ));
    }
    let size = metadata.len();
    let cap = params.max_bytes.map_or(MAX_INLINE_TEXT_BYTES, |max| {
        u64::from(max).min(MAX_INLINE_TEXT_BYTES)
    });
    if size > cap {
        // TODO(#53): larger files go through the blob store as `blob`.
        return Err(Error::new(
            ErrorCode::PayloadTooLarge,
            format!("{} is {size} bytes", params.path),
        )
        .with_details(ErrorDetails::Limit { limit: cap }));
    }
    let bytes = std::fs::read(&path).map_err(|error| io_error(&error, &params.path))?;
    let revision = format!("sha256:{:x}", Sha256::digest(&bytes));
    let size = bytes.len() as u64;
    let binary = bytes[..bytes.len().min(BINARY_SNIFF_BYTES)].contains(&0);
    let text = if binary {
        None
    } else {
        String::from_utf8(bytes).ok()
    };
    Ok(FileContent {
        binary: text.is_none(),
        text,
        blob: None,
        size,
        revision,
    })
}
