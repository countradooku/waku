//! `waku-core`'s implementations of the orchestrator's effect ports (RFC 0001
//! "Public APIs" and "Amendments": `Ports`).
//!
//! - [`GitWorkspacePort`]: task worktrees under the managed folder, created
//!   idempotently per thread, and removed only when the worktree-cleanup
//!   safety rules allow it.
//! - [`GitCheckpointPort`]: hidden refs (`refs/waku/t/<thread>/<scope>/<n>`)
//!   captured through the isolated-index snapshot of [`crate::checkpoint`],
//!   and restored from.
//! - [`ProviderTitlePort`]: one-shot title generation routed by driver.
//! - [`CoreCleanupPort`]: a thread's terminals and its attachment trees.
//!
//! Every method is idempotent, which is what lets the outbox requeue these
//! effects after a crash. They run on effect-worker threads, never on a
//! frame.

pub mod checkpoints;
pub mod cleanup;
pub mod titles;
pub mod workspace;

use std::sync::Arc;

use waku_orchestrator::Ports;

pub use checkpoints::GitCheckpointPort;
pub use cleanup::{CoreCleanupPort, TerminalMap, ThreadTerminals};
pub use titles::{CodexTitleBackend, ProviderTitlePort, TitleBackend};
pub use workspace::GitWorkspacePort;

/// The four ports, ready for `DefaultExecutor`.
pub fn core_ports(
    workspace: GitWorkspacePort,
    titles: ProviderTitlePort,
    cleanup: CoreCleanupPort,
) -> Ports {
    Ports {
        workspace: Arc::new(workspace),
        checkpoints: Arc::new(GitCheckpointPort),
        titles: Arc::new(titles),
        cleanup: Arc::new(cleanup),
    }
}

#[cfg(test)]
mod tests;
