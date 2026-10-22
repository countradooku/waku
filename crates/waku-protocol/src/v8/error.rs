//! The error model (RFC 0003 §3).
//!
//! Errors of calls and streams carry a machine-readable [`ErrorCode`] and
//! optional typed [`ErrorDetails`], so clients branch on the cause instead of
//! parsing English. Connection-level failures are `rejected` (handshake) or
//! `goodbye` (after it), never an [`Error`].

use serde::{Deserialize, Serialize};

use super::Scope;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct Error {
    pub code: ErrorCode,
    /// English, for logs. Clients localize by `code`.
    pub message: String,
    #[serde(default)]
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<ErrorDetails>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
}

impl Error {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: false,
            retry_after_ms: None,
            details: None,
            trace_id: None,
        }
    }

    pub fn with_details(mut self, details: ErrorDetails) -> Self {
        self.details = Some(details);
        self
    }

    pub fn retryable(mut self) -> Self {
        self.retryable = true;
        self
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for Error {}

open_string_enum! {
    pub enum ErrorCode {
        BadRequest = "badRequest",
        UnknownMethod = "unknownMethod",
        Unauthenticated = "unauthenticated",
        Forbidden = "forbidden",
        NotFound = "notFound",
        Conflict = "conflict",
        AlreadyExists = "alreadyExists",
        Unsupported = "unsupported",
        NotReady = "notReady",
        Busy = "busy",
        RateLimited = "rateLimited",
        PayloadTooLarge = "payloadTooLarge",
        Cancelled = "cancelled",
        Timeout = "timeout",
        Provider = "provider",
        Vcs = "vcs",
        Io = "io",
        Internal = "internal",
    }
}

/// Typed detail for an [`Error`]; an open union.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ErrorDetails {
    MissingScope {
        required: Scope,
    },
    Validation {
        field: String,
        reason: String,
    },
    NotFound {
        resource: String,
        id: String,
    },
    /// `files.write`, `settings.update` and `drafts.apply` against a stale
    /// base revision.
    RevisionConflict {
        current: String,
    },
    /// RFC 0002 `Unsupported`.
    Capability {
        capability: String,
    },
    Provider {
        class: ProviderFailureClass,
        retryable: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reset_at: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        code: Option<String>,
    },
    Vcs {
        kind: VcsFailureKind,
    },
    /// The limit that `payloadTooLarge` or `busy` hit.
    Limit {
        limit: u64,
    },
    #[serde(other)]
    Unknown,
}

open_string_enum! {
    /// RFC 0002's provider failure classes.
    pub enum ProviderFailureClass {
        UsageLimit = "usageLimit",
        ProviderError = "providerError",
        TransportError = "transportError",
        PermissionError = "permissionError",
        ValidationError = "validationError",
        Unclassified = "unknown",
    }
}

open_string_enum! {
    pub enum VcsFailureKind {
        NotARepository = "notARepository",
        DirtyWorktree = "dirtyWorktree",
        MergeConflict = "mergeConflict",
        AuthRequired = "authRequired",
        NonFastForward = "nonFastForward",
        RefNotFound = "refNotFound",
    }
}
