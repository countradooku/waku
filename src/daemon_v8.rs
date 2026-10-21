//! The desktop's protocol v8 connection for every non-session surface
//! (#53, #55): files, version control, terminals, providers, usage, skills,
//! settings, storage, attachments, composer sources and drafts.
//!
//! It is opened at startup beside the supervisor's own connection, which
//! the session layer still uses, and reopened on demand after the daemon
//! restarts. The request and response handling lives in
//! [`waku_client::surfaces`]; this module only ties the handle to the
//! supervised daemon. When the session layer gains its own v8 handle the two
//! are meant to merge into this one.

pub use waku_client::surfaces::DaemonV8;

/// A handle that follows `daemon` across restarts: every reconnect asks the
/// supervisor where the current daemon listens.
pub fn for_supervisor(daemon: &waku_client::DaemonSupervisor) -> DaemonV8 {
    let daemon = daemon.clone();
    DaemonV8::new(move || {
        let (address, token) = daemon
            .endpoint()
            .ok_or_else(|| anyhow::anyhow!("the Waku daemon is restarting"))?;
        Ok(waku_client::surfaces::Endpoint { address, token })
    })
}
