//! The daemon's per-user local endpoint (#19): a Unix domain socket, or a
//! named pipe on Windows.
//!
//! The managed daemon listens here instead of on TCP when remote exposure is
//! off, so the desktop reaches it without the TCP stack and without a port
//! anyone else on the machine can connect to. Legacy clients speak the
//! WebSocket protocol on top; protocol v8 clients open with the `WAKU`
//! preface and RFC 0003's binary framing instead, on the same socket.
//!
//! Unix: the socket lives in a 0700 directory owned by the daemon's user and
//! is itself 0600. Every accepted peer must run as the same effective uid
//! (`getpeereid` / `SO_PEERCRED`), checked before a byte is read. A sibling
//! `.lock` file held with `flock` makes removing a leftover socket safe: only
//! the lock holder unlinks and rebinds the default path. A second daemon for
//! the same user binds a pid-suffixed path instead and reports it in its
//! ready line.
//!
//! Windows: the pipe is created with `FILE_FLAG_FIRST_PIPE_INSTANCE` and
//! rejects remote clients. It keeps the default pipe DACL; peer SID checks are
//! not implemented yet, so the bearer token is the authentication there.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context as _;

use crate::server::ServerContext;

pub struct LocalListener {
    path: PathBuf,
    #[cfg(unix)]
    listener: Option<std::os::unix::net::UnixListener>,
    /// Held for the daemon's lifetime; see the module docs.
    #[cfg(unix)]
    _lock: Option<std::fs::File>,
}

impl LocalListener {
    /// Where clients connect: the socket path, or the pipe name on Windows.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(unix)]
impl LocalListener {
    /// Bind `requested`, creating its private directory if needed.
    pub fn bind(requested: &Path) -> anyhow::Result<Self> {
        use std::os::unix::ffi::OsStrExt as _;
        use std::os::unix::fs::OpenOptionsExt as _;

        let directory = requested
            .parent()
            .filter(|directory| !directory.as_os_str().is_empty())
            .context("the daemon socket path has no directory")?;
        prepare_private_directory(directory)?;
        let mut lock_name = requested.as_os_str().to_owned();
        lock_name.push(".lock");
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .mode(0o600)
            .open(PathBuf::from(&lock_name))
            .with_context(|| format!("could not open {}", PathBuf::from(&lock_name).display()))?;
        // SAFETY: `flock` on an open descriptor has no other preconditions.
        let locked = unsafe {
            libc::flock(
                std::os::fd::AsRawFd::as_raw_fd(&lock),
                libc::LOCK_EX | libc::LOCK_NB,
            )
        } == 0;
        let (path, lock) = if locked {
            (requested.to_owned(), Some(lock))
        } else {
            // Another live daemon of this user owns the default path. A pid
            // is unique among live processes, so a file at this name can only
            // be a leftover of a dead one.
            let stem = requested
                .file_stem()
                .map(|stem| stem.to_string_lossy().into_owned())
                .unwrap_or_else(|| "daemon".into());
            (
                directory.join(format!("{stem}-{}.sock", std::process::id())),
                None,
            )
        };
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("could not remove stale {}", path.display()));
            }
        }
        if path.as_os_str().as_bytes().len() >= 104 {
            anyhow::bail!(
                "daemon socket path is too long for a Unix socket: {}",
                path.display()
            );
        }
        // The socket is created with the process umask and tightened to 0600
        // right after. In between it is already unreachable for other users:
        // its directory is 0700. (A process-wide `umask` would race with
        // other threads creating files, so it is not narrowed here.)
        let listener = std::os::unix::net::UnixListener::bind(&path)
            .with_context(|| format!("could not bind the daemon socket {}", path.display()))?;
        std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            path,
            listener: Some(listener),
            _lock: lock,
        })
    }
}

/// Create `directory` 0700 if missing, and refuse one that another user owns
/// or that is not a real directory. A directory that is ours but too open is
/// tightened to 0700.
#[cfg(unix)]
fn prepare_private_directory(directory: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, PermissionsExt as _};

    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(directory)
        .with_context(|| format!("could not create {}", directory.display()))?;
    let metadata = std::fs::symlink_metadata(directory)?;
    if !metadata.file_type().is_dir() {
        anyhow::bail!("{} is not a directory", directory.display());
    }
    // SAFETY: `geteuid` has no preconditions and cannot fail.
    let uid = unsafe { libc::geteuid() };
    if metadata.uid() != uid {
        anyhow::bail!(
            "{} belongs to uid {}, not this user ({uid})",
            directory.display(),
            metadata.uid()
        );
    }
    if metadata.mode() & 0o077 != 0 {
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

#[cfg(unix)]
impl Drop for LocalListener {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(unix)]
pub(crate) async fn accept_local(
    mut listener: LocalListener,
    context: Arc<ServerContext>,
) -> anyhow::Result<()> {
    let std_listener = listener
        .listener
        .take()
        .context("the daemon socket was already served")?;
    let accepting = tokio::net::UnixListener::from_std(std_listener)
        .context("could not register the daemon socket")?;
    // SAFETY: `geteuid` has no preconditions and cannot fail.
    let uid = unsafe { libc::geteuid() };
    loop {
        let accepted = tokio::select! {
            () = context.shutdown.triggered() => return Ok(()),
            accepted = accepting.accept() => accepted,
        };
        match accepted {
            Ok((stream, _)) => {
                // Only this user's processes may talk to the daemon, checked
                // before the handshake reads anything.
                if stream.peer_cred().is_ok_and(|peer| peer.uid() == uid) {
                    context.spawn_local_connection(stream);
                }
            }
            Err(error) => crate::server::accept_failed(error).await?,
        }
    }
}

#[cfg(windows)]
impl LocalListener {
    /// Pipes are created by [`accept_local`] on the server's runtime; this
    /// only records the name. Clients retry briefly while the first instance
    /// does not exist yet.
    pub fn bind(requested: &Path) -> anyhow::Result<Self> {
        let name = requested.to_string_lossy();
        if !name.starts_with(r"\\.\pipe\") {
            anyhow::bail!("daemon pipe name must start with \\\\.\\pipe\\: {name}");
        }
        Ok(Self {
            path: requested.to_owned(),
        })
    }
}

#[cfg(windows)]
pub(crate) async fn accept_local(
    listener: LocalListener,
    context: Arc<ServerContext>,
) -> anyhow::Result<()> {
    use tokio::net::windows::named_pipe::ServerOptions;

    let name = listener.path.as_os_str().to_owned();
    let mut server = ServerOptions::new()
        .first_pipe_instance(true)
        .reject_remote_clients(true)
        .create(&name)
        .with_context(|| {
            format!(
                "could not create the daemon pipe {}",
                listener.path.display()
            )
        })?;
    loop {
        tokio::select! {
            () = context.shutdown.triggered() => return Ok(()),
            connected = server.connect() => connected.context("daemon pipe failed")?,
        }
        // Open the next instance before handing this one off, so a client
        // never finds the pipe missing between connections.
        let next = ServerOptions::new()
            .reject_remote_clients(true)
            .create(&name)
            .context("could not create the next daemon pipe instance")?;
        let connected = std::mem::replace(&mut server, next);
        context.spawn_local_connection(connected);
    }
}

#[cfg(not(any(unix, windows)))]
impl LocalListener {
    pub fn bind(_requested: &Path) -> anyhow::Result<Self> {
        anyhow::bail!("local daemon sockets are not supported on this platform")
    }
}

#[cfg(not(any(unix, windows)))]
pub(crate) async fn accept_local(
    _listener: LocalListener,
    _context: Arc<ServerContext>,
) -> anyhow::Result<()> {
    anyhow::bail!("local daemon sockets are not supported on this platform")
}
