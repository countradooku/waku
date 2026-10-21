//! v8 stream producers: one task per subscription.

use std::sync::Arc;

use sha2::{Digest as _, Sha256};
use waku_protocol::v8::{
    BlobDownloadItem, BlobDownloadSpec, EndReason, Error, ErrorCode, ErrorDetails, MAX_BLOB_BYTES,
    SettingsItem, StreamCursor,
};

use super::V8Context;
use super::connection::StreamSink;
use crate::protocol::{Command, ResponsePayload};

/// `settings`: a snapshot (skipped when `after` is the current revision),
/// `synchronized`, then `updated` on every change.
pub(crate) async fn settings(
    sink: StreamSink,
    context: Arc<V8Context>,
    after: Option<StreamCursor>,
) {
    // Subscribe to changes before reading the current revision, so a change
    // in between is not lost.
    let mut changes = context.settings.subscribe();
    let current = match context.settings.current() {
        Some(current) => current,
        None => {
            let backend = context.backend.clone();
            let bus = context.settings.clone();
            match tokio::task::spawn_blocking(move || backend.load_settings(&bus)).await {
                Ok(Ok(current)) => current,
                Ok(Err(error)) => {
                    sink.end(EndReason::Error {
                        error: Error::new(ErrorCode::Internal, format!("{error:#}")),
                    });
                    return;
                }
                Err(_) => return,
            }
        }
    };
    let mut revision = current.revision;
    let cursor = StreamCursor::Revision { revision };
    if after.as_ref() != Some(&cursor) {
        let snapshot = SettingsItem::Snapshot {
            revision,
            settings: current.settings.clone(),
        };
        if sink.push(&snapshot, Some(cursor.clone())).is_err() {
            return;
        }
    }
    if sink
        .push(
            &SettingsItem::Synchronized {
                cursor: cursor.clone(),
            },
            Some(cursor),
        )
        .is_err()
    {
        return;
    }
    while changes.changed().await.is_ok() {
        let Some(state) = changes.borrow_and_update().clone() else {
            continue;
        };
        if state.revision == revision {
            continue;
        }
        revision = state.revision;
        let item = state.updated_item(sink.encoding());
        if sink
            .push_encoded(item, Some(StreamCursor::Revision { revision }))
            .is_err()
        {
            return;
        }
    }
}

/// `blobDownload`: `meta`, the bytes as DATA within the credit window with
/// `FIN` on the last chunk, then `end{completed}`.
pub(crate) async fn blob_download(
    sink: StreamSink,
    context: Arc<V8Context>,
    spec: BlobDownloadSpec,
) {
    let backend = context.backend.clone();
    let reference = spec.reference.clone();
    // A file inside an attachment, or a blob.
    let command = match spec.path.clone() {
        Some(path) => Command::ReadAttachment {
            reference,
            path: path.into(),
        },
        None => Command::ReadBlob { reference },
    };
    let bytes = match tokio::task::spawn_blocking(move || backend.call(command)).await {
        Ok(Ok(ResponsePayload::BlobData { bytes })) => bytes,
        Ok(Ok(_)) => {
            sink.end(EndReason::Error {
                error: Error::new(ErrorCode::Internal, "unexpected blob response"),
            });
            return;
        }
        Ok(Err(error)) => {
            sink.end(EndReason::Error {
                error: Error::new(ErrorCode::NotFound, format!("{error:#}")).with_details(
                    ErrorDetails::NotFound {
                        resource: "blob".into(),
                        id: spec.reference,
                    },
                ),
            });
            return;
        }
        Err(_) => return,
    };
    if bytes.len() as u64 > MAX_BLOB_BYTES {
        sink.end(EndReason::Error {
            error: Error::new(
                ErrorCode::PayloadTooLarge,
                "the blob is over the size limit",
            )
            .with_details(ErrorDetails::Limit {
                limit: MAX_BLOB_BYTES,
            }),
        });
        return;
    }
    let meta = BlobDownloadItem::Meta {
        size: bytes.len() as u64,
        mime: mime_type(spec.path.as_deref().unwrap_or(&spec.reference)).into(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
    };
    if sink.push(&meta, None).is_err() {
        return;
    }
    let mut offset = usize::try_from(spec.offset.unwrap_or(0))
        .unwrap_or(usize::MAX)
        .min(bytes.len());
    // Nothing left to send: an empty chunk carries the FIN.
    if offset == bytes.len() && sink.data(true, Vec::new()).is_err() {
        return;
    }
    while offset < bytes.len() {
        let Ok(credit) = sink.credit().await else {
            return;
        };
        let length = (bytes.len() - offset)
            .min(sink.max_data_chunk())
            .min(usize::try_from(credit).unwrap_or(usize::MAX));
        let fin = offset + length == bytes.len();
        if sink
            .data(fin, bytes[offset..offset + length].to_vec())
            .is_err()
        {
            return;
        }
        offset += length;
    }
    sink.end(EndReason::Completed);
}

/// The MIME type of a `waku-blob:<sha256>.<ext>` reference.
pub(crate) fn mime_type(reference: &str) -> &'static str {
    let extension = reference
        .rsplit_once('.')
        .map(|(_, extension)| extension.to_ascii_lowercase())
        .unwrap_or_default();
    match extension.as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        "pdf" => "application/pdf",
        "txt" | "log" => "text/plain",
        "md" => "text/markdown",
        "json" => "application/json",
        _ => "application/octet-stream",
    }
}
