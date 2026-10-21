//! `CleanupPort`: a thread's terminals and attachment trees.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use uuid::Uuid;
use waku_orchestrator::{CleanupPort, PortError};
use waku_protocol::attachments::ATTACHMENT_SCHEME;
use waku_protocol::orchestration::ThreadId;

use crate::attachments::AttachmentStore;
use crate::terminal::DaemonTerminal;

/// The daemon's open terminals by id.
pub type TerminalMap = Mutex<HashMap<Uuid, Arc<DaemonTerminal>>>;

/// Closes the terminals of a thread.
pub trait ThreadTerminals: Send + Sync {
    /// Closes every terminal of `thread_id`; returns how many closed.
    fn close_thread(&self, thread_id: Uuid) -> usize;
}

impl ThreadTerminals for TerminalMap {
    fn close_thread(&self, thread_id: Uuid) -> usize {
        // Dropped outside the lock: closing waits out the shell's exit.
        let removed = self.lock().remove(&thread_id);
        usize::from(removed.is_some())
    }
}

/// Closes terminals and removes attachment trees.
///
/// Only `waku-attachment:` trees are removed: each belongs to one upload.
/// `waku-blob:` blobs are content-addressed and may be shared by other
/// messages, so they are left to the blob store's retention sweep, which
/// knows every live reference.
pub struct CoreCleanupPort {
    terminals: Arc<dyn ThreadTerminals>,
    attachments: AttachmentStore,
}

impl CoreCleanupPort {
    pub fn new(terminals: Arc<dyn ThreadTerminals>, attachments: AttachmentStore) -> Self {
        Self {
            terminals,
            attachments,
        }
    }
}

impl CleanupPort for CoreCleanupPort {
    fn terminals(&self, thread_id: ThreadId) -> Result<(), PortError> {
        self.terminals.close_thread(thread_id.0);
        Ok(())
    }

    fn attachments(&self, ids: &[String]) -> Result<(), PortError> {
        for reference in ids
            .iter()
            .filter(|reference| reference.starts_with(ATTACHMENT_SCHEME))
        {
            self.attachments
                .remove(reference)
                .map_err(|error| PortError::retryable(format!("removing {reference}: {error}")))?;
        }
        Ok(())
    }
}
