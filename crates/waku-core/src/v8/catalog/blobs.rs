//! `blobs.put` (an upload streamed as DATA), `blobs.stat`,
//! `attachments.import` and `attachments.importPath` (RFC 0003 §5.6).

use std::collections::{HashMap, VecDeque};
use std::io::Read as _;
use std::path::PathBuf;
use std::sync::Arc;

use base64::Engine as _;
use parking_lot::Mutex;
use sha2::{Digest as _, Sha256};
use waku_protocol::v8::{
    AttachmentKind, AttachmentsImportParams, AttachmentsImportPathParams, BlobPut, BlobStat,
    BlobsPutParams, BlobsStatParams, Error, ErrorCode, ErrorDetails, MAX_ATTACHMENT_BYTES,
    MAX_ATTACHMENT_PARTS, MAX_BLOB_BYTES, StoredAttachment,
};

use super::{CallContext, Domain, bad_request, call, display_path, not_found, unexpected};
use crate::attachments::{AttachmentUpload, AttachmentUploadEntry};
use crate::protocol::{Command, ResponsePayload};
use crate::v8::V8Context;
use crate::v8::connection::Conn;

/// Uploaded blobs remembered by digest, for `deduplicated` replies.
const MAX_INDEXED_BLOBS: usize = 4_096;

/// sha256 → reference of blobs this daemon stored or hashed.
///
/// The blob store names blobs by a fast content hash, not by sha256, so a
/// `blobs.put` with `sha256` deduplicates against the blobs this daemon has
/// seen since it started.
#[derive(Default)]
pub(crate) struct BlobIndex {
    state: Mutex<(HashMap<String, String>, VecDeque<String>)>,
}

impl BlobIndex {
    fn get(&self, sha256: &str) -> Option<String> {
        self.state.lock().0.get(sha256).cloned()
    }

    fn insert(&self, sha256: String, reference: String) {
        let mut state = self.state.lock();
        let (map, order) = &mut *state;
        if map.insert(sha256.clone(), reference).is_none() {
            order.push_back(sha256);
        }
        while order.len() > MAX_INDEXED_BLOBS {
            if let Some(oldest) = order.pop_front() {
                map.remove(&oldest);
            }
        }
    }
}

/// Frees an upload's channel when the call ends, however it ends; later
/// DATA on it is discarded.
struct UploadChannel {
    conn: Arc<Conn>,
    channel: u32,
}

impl Drop for UploadChannel {
    fn drop(&mut self) {
        self.conn.close_upload(self.channel);
    }
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn too_large(what: &str, limit: u64) -> Error {
    Error::new(
        ErrorCode::PayloadTooLarge,
        format!("{what} is over {limit} bytes"),
    )
    .with_details(ErrorDetails::Limit { limit })
}

/// Store bytes in the blob store: the reference and the file.
pub(crate) fn store_blob(
    context: &V8Context,
    mime: &str,
    bytes: Vec<u8>,
) -> Result<(String, PathBuf), Error> {
    match call(
        context,
        Command::StoreBlob {
            mime_type: mime.to_owned(),
            bytes,
        },
        Domain::General,
    )? {
        ResponsePayload::BlobStored { reference, path } => Ok((reference, path)),
        other => Err(unexpected(&other)),
    }
}

pub(super) async fn put(
    context: &Arc<V8Context>,
    call: CallContext,
    params: BlobsPutParams,
) -> Result<BlobPut, Error> {
    let _channel = UploadChannel {
        conn: call.conn.clone(),
        channel: params.channel,
    };
    let Some(mut upload) = call.upload else {
        return Err(Error::new(ErrorCode::Internal, "the upload has no channel"));
    };
    if params.size > MAX_BLOB_BYTES {
        return Err(too_large("the blob", MAX_BLOB_BYTES));
    }
    if let Some(sha256) = &params.sha256 {
        if !valid_sha256(sha256) {
            return Err(bad_request("sha256", "not 64 lowercase hex digits"));
        }
        if let Some(reference) = context.catalog.blobs.get(sha256)
            && blob_exists(context, &reference).await
        {
            return Ok(BlobPut {
                reference,
                size: params.size,
                sha256: sha256.clone(),
                deduplicated: true,
            });
        }
    }
    let size = usize::try_from(params.size).unwrap_or(usize::MAX);
    let mut bytes = Vec::with_capacity(size.min(8 * 1024 * 1024));
    let mut hasher = Sha256::new();
    loop {
        let Some(chunk) = upload.recv().await else {
            return Err(Error::new(
                ErrorCode::Cancelled,
                "the connection closed during the upload",
            ));
        };
        if bytes.len() + chunk.body.len() > size {
            return Err(bad_request(
                "size",
                "more DATA arrived than `size` announced",
            ));
        }
        hasher.update(&chunk.body);
        bytes.extend_from_slice(&chunk.body);
        if chunk.fin {
            break;
        }
    }
    if bytes.len() != size {
        return Err(bad_request(
            "size",
            format!("{} bytes arrived, {} were announced", bytes.len(), size),
        ));
    }
    let sha256 = format!("{:x}", hasher.finalize());
    if let Some(expected) = &params.sha256
        && *expected != sha256
    {
        return Err(bad_request("sha256", "the bytes have a different digest"));
    }
    let task_context = context.clone();
    let mime = params.mime.clone();
    let (reference, _) =
        tokio::task::spawn_blocking(move || store_blob(&task_context, &mime, bytes))
            .await
            .map_err(super::internal)??;
    context
        .catalog
        .blobs
        .insert(sha256.clone(), reference.clone());
    Ok(BlobPut {
        reference,
        size: params.size,
        sha256,
        deduplicated: false,
    })
}

async fn blob_exists(context: &Arc<V8Context>, reference: &str) -> bool {
    let task_context = context.clone();
    let reference = reference.to_owned();
    tokio::task::spawn_blocking(move || {
        matches!(
            task_context.backend.call(Command::LocateBlob { reference }),
            Ok(ResponsePayload::BlobLocation { .. })
        )
    })
    .await
    .unwrap_or(false)
}

/// Where a blob, or a file inside an attachment, lives on the daemon host.
pub(crate) fn locate(
    context: &V8Context,
    reference: &str,
    path: Option<&str>,
) -> Result<PathBuf, Error> {
    let command = if reference.starts_with(crate::attachments::ATTACHMENT_SCHEME) {
        let path =
            path.ok_or_else(|| bad_request("path", "an attachment needs the file's path"))?;
        Command::LocateAttachment {
            reference: reference.to_owned(),
            path: super::absolute_path("path", path)?,
        }
    } else {
        Command::LocateBlob {
            reference: reference.to_owned(),
        }
    };
    match context.backend.call(command) {
        Ok(ResponsePayload::BlobLocation { path }) => Ok(path),
        Ok(other) => Err(unexpected(&other)),
        Err(_) => Err(not_found("blob", reference)),
    }
}

pub(super) fn stat(
    context: &V8Context,
    local: bool,
    params: BlobsStatParams,
) -> Result<BlobStat, Error> {
    let path = locate(context, &params.reference, params.path.as_deref())?;
    let mut file = std::fs::File::open(&path).map_err(|_| not_found("blob", &params.reference))?;
    let size = file.metadata().map_err(super::internal)?.len();
    let mut hasher = Sha256::new();
    let mut buffer = vec![0; 256 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|error| Error::new(ErrorCode::Io, error.to_string()))?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    let sha256 = format!("{:x}", hasher.finalize());
    if !params
        .reference
        .starts_with(crate::attachments::ATTACHMENT_SCHEME)
    {
        context
            .catalog
            .blobs
            .insert(sha256.clone(), params.reference.clone());
    }
    let name = params.path.as_deref().unwrap_or(&params.reference);
    Ok(BlobStat {
        size,
        mime: crate::v8::streams::mime_type(name).into(),
        sha256,
        local_path: local.then(|| display_path(&path)),
    })
}

fn read_blob(context: &V8Context, reference: &str) -> Result<Vec<u8>, Error> {
    match context.backend.call(Command::ReadBlob {
        reference: reference.to_owned(),
    }) {
        Ok(ResponsePayload::BlobData { bytes }) => Ok(bytes),
        Ok(other) => Err(unexpected(&other)),
        Err(_) => Err(not_found("blob", reference)),
    }
}

fn stored(attachment: crate::attachments::StoredAttachment) -> StoredAttachment {
    StoredAttachment {
        reference: attachment.reference,
        path: display_path(&attachment.path),
        name: attachment.name,
        is_dir: attachment.is_dir,
    }
}

pub(super) fn import_attachment(
    context: &V8Context,
    params: AttachmentsImportParams,
) -> Result<StoredAttachment, Error> {
    if params.parts.len() > MAX_ATTACHMENT_PARTS as usize {
        return Err(too_large(
            "the attachment's file count",
            u64::from(MAX_ATTACHMENT_PARTS),
        ));
    }
    let engine = base64::engine::general_purpose::STANDARD;
    let mut total = 0u64;
    let mut read = |reference: &str| -> Result<String, Error> {
        let bytes = read_blob(context, reference)?;
        total += bytes.len() as u64;
        if total > MAX_ATTACHMENT_BYTES {
            return Err(too_large("the attachment", MAX_ATTACHMENT_BYTES));
        }
        Ok(engine.encode(bytes))
    };
    let upload = match params.kind {
        AttachmentKind::File => {
            let [part] = params.parts.as_slice() else {
                return Err(bad_request(
                    "parts",
                    "a file attachment has exactly one part",
                ));
            };
            AttachmentUpload::File {
                data_base64: read(&part.blob)?,
            }
        }
        AttachmentKind::Directory => AttachmentUpload::Directory {
            entries: params
                .parts
                .iter()
                .map(|part| {
                    Ok(AttachmentUploadEntry {
                        relative_path: PathBuf::from(&part.relative_path),
                        data_base64: read(&part.blob)?,
                    })
                })
                .collect::<Result<_, Error>>()?,
        },
        AttachmentKind::Unknown(kind) => {
            return Err(bad_request(
                "kind",
                format!("unknown attachment kind {kind}"),
            ));
        }
    };
    match call(
        context,
        Command::ImportAttachment {
            name: params.name,
            upload,
        },
        Domain::General,
    )? {
        ResponsePayload::AttachmentStored { attachment } => Ok(stored(attachment)),
        other => Err(unexpected(&other)),
    }
}

pub(super) fn import_path(
    context: &V8Context,
    params: AttachmentsImportPathParams,
) -> Result<StoredAttachment, Error> {
    let path = super::absolute_path("path", &params.path)?;
    match call(
        context,
        Command::ImportPathAttachment { path },
        Domain::General,
    )? {
        ResponsePayload::AttachmentStored { attachment } => Ok(stored(attachment)),
        other => Err(unexpected(&other)),
    }
}
