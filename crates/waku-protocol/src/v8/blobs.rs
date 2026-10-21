//! Blobs and attachments (RFC 0003 §5.2 "Blobs and attachments", §5.6).
//!
//! Uploads: `blobs.put` names a free channel, and the client streams the
//! bytes as DATA frames on it right after the call, `FIN` on the last one.
//! Downloads: the `blobDownload` stream.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Files in one directory attachment.
pub const MAX_ATTACHMENT_PARTS: u32 = 4_096;
/// Bytes of one attachment (a file, or a directory's files together).
pub const MAX_ATTACHMENT_BYTES: u64 = 32 * 1024 * 1024;

/// `blobs.put` params. `channel` must not be in use by a subscription or
/// another upload; it is free again once the reply arrives.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct BlobsPutParams {
    pub channel: u32,
    /// Exactly the bytes that follow, at most `maxBlobBytes`.
    pub size: u64,
    pub mime: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Lowercase hex. Verified when given, and lets the daemon answer at once
    /// for a blob it already holds (`deduplicated`); the client then stops
    /// and sends an empty DATA frame with `FIN`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct BlobPut {
    /// `waku-blob:<name>`.
    pub reference: String,
    pub size: u64,
    /// Lowercase hex.
    pub sha256: String,
    pub deduplicated: bool,
}

/// `blobs.stat` params: a `waku-blob:` reference, or a
/// `waku-attachment:` reference with the `path` of a file inside it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct BlobsStatParams {
    pub reference: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct BlobStat {
    pub size: u64,
    pub mime: String,
    /// Lowercase hex.
    pub sha256: String,
    /// Where the file is on the daemon host; only on the local socket, where
    /// the client may read it directly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_path: Option<String>,
}

open_string_enum! {
    pub enum AttachmentKind {
        File = "file",
        Directory = "directory",
    }
}

/// `attachments.import` params: an attachment assembled from uploaded
/// blobs. A file attachment has one part (its `relativePath` is ignored); a
/// directory has one part per file.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct AttachmentsImportParams {
    pub name: String,
    pub kind: AttachmentKind,
    pub parts: Vec<AttachmentPart>,
    pub op_id: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct AttachmentPart {
    /// `/`-separated, inside the directory.
    pub relative_path: String,
    /// A `waku-blob:` reference from `blobs.put`.
    pub blob: String,
}

/// `attachments.importPath` params: copy a file or directory on the daemon
/// host into the attachment store.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct AttachmentsImportPathParams {
    /// Absolute.
    pub path: String,
    pub op_id: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct StoredAttachment {
    /// `waku-attachment:<id>`.
    pub reference: String,
    /// The materialized file or directory on the daemon host.
    pub path: String,
    pub name: String,
    pub is_dir: bool,
}
