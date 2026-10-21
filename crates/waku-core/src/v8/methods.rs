//! v8 method handlers. They run on the blocking pool.

use std::path::{Component, Path, PathBuf};

use waku_protocol::v8::{
    Error, ErrorCode, ErrorDetails, FileContent, FilesReadParams, MAX_BLOB_BYTES,
    MAX_INLINE_TEXT_BYTES, Method, MethodResult, RootRef, SettingsSnapshot,
};

use super::V8Context;

/// Bytes inspected to tell text from binary.
const BINARY_SNIFF_BYTES: usize = 8 * 1024;

/// `local` is whether the call came over the local socket.
pub(crate) fn handle(
    context: &V8Context,
    local: bool,
    method: Method,
) -> Result<MethodResult, Error> {
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
        Method::ProjectsList(_) => {
            super::orchestration::projects(context).map(MethodResult::ProjectsList)
        }
        Method::FilesRead(params) => files_read(context, params).map(MethodResult::FilesRead),
        method @ (Method::OrchestrationDispatch(_)
        | Method::ThreadsSnapshot(_)
        | Method::ThreadsHistoryPage(_)
        | Method::ThreadsTurnItem(_)
        | Method::ThreadsArchived(_)
        | Method::ThreadsSearch(_)
        | Method::ThreadsFind(_)) => {
            super::orchestration::handle(context, method).expect("an orchestration method")
        }
        Method::Unknown { name } => Err(Error::new(
            ErrorCode::UnknownMethod,
            format!("unknown method {name}"),
        )),
        method => super::catalog::handle(context, local, method),
    }
}

fn internal(error: anyhow::Error) -> Error {
    Error::new(ErrorCode::Internal, format!("{error:#}"))
}

pub(crate) fn project_not_found(project_id: uuid::Uuid) -> Error {
    Error::new(ErrorCode::NotFound, "no such project").with_details(ErrorDetails::NotFound {
        resource: "project".into(),
        id: project_id.to_string(),
    })
}

pub(crate) fn root_path(context: &V8Context, root: &RootRef) -> Result<PathBuf, Error> {
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
        RootRef::Project { project_id } => super::orchestration::projects(context)?
            .into_iter()
            .find(|project| project.id.0 == *project_id)
            .map(|project| project.path)
            .ok_or_else(|| project_not_found(*project_id)),
        RootRef::Thread { thread_id } => super::orchestration::thread_root(
            context,
            waku_protocol::orchestration::ThreadId(*thread_id),
        ),
        RootRef::Unknown => Err(Error::new(ErrorCode::BadRequest, "unknown root kind")),
    }
}

/// `root` joined with a relative, `/`-separated path that cannot leave it.
pub(crate) fn resolve(root: &Path, relative: &str) -> Result<PathBuf, Error> {
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

fn too_large(path: &str, size: u64, limit: u64) -> Error {
    Error::new(
        ErrorCode::PayloadTooLarge,
        format!("{path} is {size} bytes"),
    )
    .with_details(ErrorDetails::Limit { limit })
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
    if let Some(max) = params.max_bytes
        && size > u64::from(max)
    {
        return Err(too_large(&params.path, size, u64::from(max)));
    }
    if size > MAX_BLOB_BYTES {
        return Err(too_large(&params.path, size, MAX_BLOB_BYTES));
    }
    let bytes = std::fs::read(&path).map_err(|error| io_error(&error, &params.path))?;
    let revision = super::catalog::file_revision(&bytes);
    let size = bytes.len() as u64;
    let binary = bytes[..bytes.len().min(BINARY_SNIFF_BYTES)].contains(&0)
        || std::str::from_utf8(&bytes).is_err();
    if size > MAX_INLINE_TEXT_BYTES {
        // Too large for a message: the client downloads it.
        let mime = if binary {
            "application/octet-stream"
        } else {
            "text/plain"
        };
        let reference = super::catalog::store_blob(context, mime, bytes)?.0;
        return Ok(FileContent {
            text: None,
            blob: Some(reference),
            binary,
            size,
            revision,
        });
    }
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
