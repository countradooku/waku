//! Blobs and attachments: `blobs.put`, `blobs.stat`, the `blobDownload`
//! stream, `attachments.import` and `attachments.importPath` (RFC 0003
//! §5.6).

use std::path::Path;

use anyhow::{Context as _, bail};
use uuid::Uuid;

use super::DaemonV8;
use crate::v8::{
    AttachmentKind, AttachmentPart, AttachmentsImportParams, AttachmentsImportPathParams,
    BlobsStatParams, StoredAttachment, method,
};

/// What a client uploads for an attachment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttachmentSource {
    File(Vec<u8>),
    /// `(relative path with / separators, bytes)` per file.
    Directory(Vec<(String, Vec<u8>)>),
}

/// The MIME type a file name suggests, for the blob store's extension.
pub fn mime_for_name(name: &str) -> &'static str {
    let extension = name
        .rsplit_once('.')
        .map(|(_, extension)| extension.to_ascii_lowercase());
    match extension.as_deref() {
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("svg") => "image/svg+xml",
        Some("bmp") => "image/bmp",
        Some("tif" | "tiff") => "image/tiff",
        Some("pdf") => "application/pdf",
        Some("json") => "application/json",
        Some("md" | "txt" | "rs" | "ts" | "js" | "py" | "toml" | "yaml" | "yml") => "text/plain",
        _ => "application/octet-stream",
    }
}

/// The file extension the blob store gives an image of `mime`.
pub fn image_extension(mime: &str) -> &'static str {
    match mime {
        "image/jpeg" => "jpg",
        "image/gif" => "gif",
        "image/webp" => "webp",
        "image/svg+xml" => "svg",
        "image/bmp" => "bmp",
        "image/tiff" => "tiff",
        _ => "png",
    }
}

/// The `attachments.import` params for `name` whose files were stored as
/// `parts` (`(relative path, blob reference)`).
pub fn import_params(
    name: String,
    directory: bool,
    parts: Vec<(String, String)>,
) -> AttachmentsImportParams {
    AttachmentsImportParams {
        name,
        kind: if directory {
            AttachmentKind::Directory
        } else {
            AttachmentKind::File
        },
        parts: parts
            .into_iter()
            .map(|(relative_path, blob)| AttachmentPart {
                relative_path,
                blob,
            })
            .collect(),
        op_id: Uuid::new_v4(),
    }
}

/// Upload `source` and store it as the attachment `name`: one `blobs.put`
/// per file, then `attachments.import` with the manifest.
pub fn upload_attachment(
    daemon: &DaemonV8,
    name: &str,
    source: AttachmentSource,
) -> anyhow::Result<StoredAttachment> {
    let client = daemon.client()?;
    let put = |relative: &str, bytes: &[u8]| {
        client
            .put_blob(bytes, mime_for_name(relative), Some(relative), None)
            .map(|stored| stored.reference)
            .map_err(super::call_error)
    };
    let (directory, parts) = match source {
        AttachmentSource::File(bytes) => (false, vec![(name.to_owned(), put(name, &bytes)?)]),
        AttachmentSource::Directory(files) => (
            true,
            files
                .into_iter()
                .map(|(relative, bytes)| Ok((relative.clone(), put(&relative, &bytes)?)))
                .collect::<anyhow::Result<Vec<_>>>()?,
        ),
    };
    daemon.call::<method::AttachmentsImport>(import_params(name.to_owned(), directory, parts))
}

/// Store the attachment at `path`, a daemon-host path, without sending its
/// bytes: only for a daemon that shares this machine's filesystem.
pub fn import_path(daemon: &DaemonV8, path: &Path) -> anyhow::Result<StoredAttachment> {
    daemon.call::<method::AttachmentsImportPath>(AttachmentsImportPathParams {
        path: path.to_string_lossy().into_owned(),
        op_id: Uuid::new_v4(),
    })
}

/// Store pasted image bytes as a one-file attachment named `name`, which
/// gives them a daemon-host path a provider can be pointed at.
pub fn store_image(
    daemon: &DaemonV8,
    bytes: &[u8],
    mime: &str,
    name: &str,
) -> anyhow::Result<StoredAttachment> {
    let reference = daemon
        .client()?
        .put_blob(bytes, mime, Some(name), None)
        .map_err(super::call_error)?
        .reference;
    daemon.call::<method::AttachmentsImport>(import_params(
        name.to_owned(),
        false,
        vec![(name.to_owned(), reference)],
    ))
}

/// Whether `reference` names something the daemon stores.
pub fn is_daemon_reference(reference: &str) -> bool {
    waku_protocol::blob::is_reference(reference)
        || reference.starts_with(waku_protocol::attachments::ATTACHMENT_SCHEME)
}

/// Read a blob, or with `daemon_path` a file inside an attachment. A local
/// daemon names the file (`blobs.stat.localPath`) and this process reads it;
/// otherwise the bytes arrive through `blobDownload`. Blocks.
pub fn read_reference(
    daemon: &DaemonV8,
    reference: &str,
    daemon_path: Option<&Path>,
) -> anyhow::Result<Vec<u8>> {
    if !is_daemon_reference(reference) {
        bail!("{reference:?} is not a daemon blob or attachment reference");
    }
    let path = daemon_path.map(|path| path.to_string_lossy().into_owned());
    if reference.starts_with(waku_protocol::attachments::ATTACHMENT_SCHEME) && path.is_none() {
        bail!("an attachment read needs the file's daemon path");
    }
    if daemon.is_local()
        && let Ok(stat) = daemon.call::<method::BlobsStat>(BlobsStatParams {
            reference: reference.to_owned(),
            path: path.clone(),
        })
        && let Some(local) = stat.local_path
        && let Ok(bytes) = std::fs::read(local)
    {
        return Ok(bytes);
    }
    let (_, bytes) = daemon
        .client()?
        .download_blob(reference, path.as_deref())
        .map_err(super::call_error)
        .with_context(|| format!("could not read {reference}"))?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_file_attachment_has_one_part_and_a_directory_many() {
        let file = import_params(
            "shot.png".into(),
            false,
            vec![("shot.png".into(), "waku-blob:1.png".into())],
        );
        assert_eq!(file.kind, AttachmentKind::File);
        assert_eq!(file.parts[0].blob, "waku-blob:1.png");
        let directory = import_params(
            "src".into(),
            true,
            vec![
                ("a.rs".into(), "waku-blob:a.txt".into()),
                ("ui/b.rs".into(), "waku-blob:b.txt".into()),
            ],
        );
        assert_eq!(directory.kind, AttachmentKind::Directory);
        assert_eq!(directory.parts[1].relative_path, "ui/b.rs");
        assert_ne!(file.op_id, directory.op_id);
    }

    #[test]
    fn names_and_mime_types_agree() {
        assert_eq!(mime_for_name("Shot.PNG"), "image/png");
        assert_eq!(mime_for_name("notes"), "application/octet-stream");
        assert_eq!(image_extension("image/jpeg"), "jpg");
        assert_eq!(image_extension("image/unknown"), "png");
        assert!(is_daemon_reference("waku-attachment:abc"));
        assert!(!is_daemon_reference("data:image/png;base64,AA=="));
    }
}
