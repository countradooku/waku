//! Discovery and adoption of the shared OpenCode service.
//!
//! OpenCode is not a per-workspace server. One background process, started by
//! whoever needed it first — usually the user's own TUI — serves every
//! workspace, because a session carries its own `location.directory` and the
//! service itself sits in `$HOME`. Waku therefore *finds* that process
//! through its registration file instead of owning one. Sessions talk to it
//! through the async transport (`driver::opencode`, `driver::v2::http`), each
//! on its own `GET /api/event` downlink filtered by session id.
//!
//! Two rules make that safe, and both are load-bearing:
//!
//! * **An adopted service is never killed.** Waku never signals the pid, never
//!   calls the service's own stop route, and never runs `opencode service
//!   stop` — the process it found may be driving the user's terminal, and a
//!   process Waku started with `serve --service` is indistinguishable from one
//!   it found.
//! * **The registration file is opened read-only, always.** The service polls
//!   its own descriptor every 5s and self-terminates when the contents change,
//!   so any write — including a well-meaning repair of a stale descriptor —
//!   kills the user's daemon within 5s.
//!
//! Everything here blocks — file reads, HTTP, process probes — so every
//! caller must already be off the UI thread (the async driver calls it on the
//! blocking pool).

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Condvar, Mutex as StdMutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context as _, anyhow, bail};
use parking_lot::RwLock;
use serde::Deserialize;

use crate::http_wire::Endpoint;
use crate::opencode_api::{self, ServerInfo};

/// The Basic username the service demands. Any other username answers 401.
const SERVICE_USER: &str = "opencode";
const REGISTRATION_FILE: &str = "service.json";

/// `--version` only prints a constant, so anything slower is a broken binary
/// rather than a busy one.
const VERSION_TIMEOUT: Duration = Duration::from_secs(5);
const SERVICE_START_BUDGET: Duration = Duration::from_secs(20);
const SERVICE_START_POLL: Duration = Duration::from_millis(100);

/// The descriptor the service writes for its clients.
///
/// `version` is the service's own build id. Waku surfaces it rather than
/// comparing it with anything of its own: the CLI's incumbency check compares
/// against *its* build, and Waku is not an OpenCode build, so equality there
/// would reject every perfectly healthy service.
#[derive(Clone, Deserialize)]
pub(crate) struct ServiceRegistration {
    pub id: String,
    #[serde(default)]
    pub version: Option<String>,
    pub url: String,
    pub pid: u32,
    pub password: String,
}

impl std::fmt::Debug for ServiceRegistration {
    /// Hand-written so the credential cannot reach a log, an error string or a
    /// panic message through a derived `Debug`.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServiceRegistration")
            .field("id", &self.id)
            .field("version", &self.version)
            .field("url", &self.url)
            .field("pid", &self.pid)
            .field("password", &"<redacted>")
            .finish()
    }
}

/// `${XDG_STATE_HOME:-$HOME/.local/state}/opencode/service.json`.
pub(crate) fn registration_path() -> PathBuf {
    state_directory().join(REGISTRATION_FILE)
}

fn state_directory() -> PathBuf {
    std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
        .or_else(|| dirs::home_dir().map(|home| home.join(".local").join("state")))
        .unwrap_or_else(|| PathBuf::from(".local").join("state"))
        .join("opencode")
}

/// Reads the descriptor of whatever service is registered, if any.
///
/// Read-only by contract; see the module doc. Absence is an ordinary answer —
/// the user simply has no service running — so this returns `None` rather than
/// an error.
pub(crate) fn read_registration() -> Option<ServiceRegistration> {
    read_registration_file(&registration_path()).or_else(channel_registration)
}

/// A non-mainstream channel registers as `service-<channel>.json`. The
/// mainstream name is probed first so a stale channel descriptor can never
/// shadow the service the user is actually running.
fn channel_registration() -> Option<ServiceRegistration> {
    let mut candidates: Vec<PathBuf> = std::fs::read_dir(state_directory())
        .ok()?
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("service-") && name.ends_with(".json"))
        })
        .collect();
    candidates.sort();
    candidates
        .iter()
        .find_map(|path| read_registration_file(path))
}

fn read_registration_file(path: &Path) -> Option<ServiceRegistration> {
    let contents = std::fs::read(path).ok()?;
    serde_json::from_slice(&contents).ok()
}

/// Confirms a descriptor still names a live, ready service, and returns the
/// endpoint to talk to it with.
///
/// `/api/info` answers 200 only once the service is ready — 503 while it is
/// starting or stopping, 500 when it failed — so a service that is still
/// coming up fails here and is simply polled again.
pub(crate) fn probe(registration: &ServiceRegistration) -> anyhow::Result<Endpoint> {
    let endpoint = Endpoint::basic(&registration.url, SERVICE_USER, &registration.password)?;
    let info = opencode_api::server_info(&endpoint)
        .context("the OpenCode service did not answer its info route")?;
    accept_info(registration, &info)?;
    Ok(endpoint)
}

fn accept_info(registration: &ServiceRegistration, info: &ServerInfo) -> anyhow::Result<()> {
    // The pid comparison is the whole point of the probe: a descriptor left
    // behind by a service that died still parses, and its port can already
    // have been reused by an unrelated listener that answers 200.
    if info.pid != registration.pid {
        bail!(
            "the OpenCode service on {} is not the process its registration names",
            registration.url
        );
    }
    // The same staleness check OpenCode's own client makes: a recycled pid
    // can belong to a different build than the one that registered.
    if let Some(version) = registration.version.as_deref()
        && info.version != version
    {
        bail!(
            "the OpenCode service on {} is not the build its registration names",
            registration.url
        );
    }
    Ok(())
}

/// How the service Waku is talking to came to exist.
///
/// `Adopted` covers both a service Waku found and one it started with
/// `serve --service`; the two are indistinguishable by construction, and
/// neither is ever signalled. `Private` is reserved for the future Computer
/// Use path (`serve --stdio --port 0` with a Waku-generated password, which
/// prints one JSON line and exits when stdin closes) and is unreachable today;
/// wiring it up will also need the guardian-script plus `process_group(0)`
/// wrapper from `deepseek_session`, because the dev watcher SIGTERMs Waku
/// without running destructors.
#[allow(dead_code)]
pub(crate) enum Ownership {
    Adopted { pid: u32 },
    Private(StdMutex<std::process::Child>),
}

/// The one adopted service.
pub(crate) struct OpenCodeService {
    /// Swapped in place when a CLI upgrade replaces the service: the upgrade
    /// version-checks incumbency and takes over with a new url, pid and
    /// password, which must not look like a dead provider to live sessions.
    endpoint: RwLock<Endpoint>,
    registration: RwLock<ServiceRegistration>,
    ownership: Ownership,
    binary: PathBuf,
}

impl OpenCodeService {
    fn adopt(binary: &Path, registration: ServiceRegistration, endpoint: Endpoint) -> Arc<Self> {
        Arc::new(Self {
            endpoint: RwLock::new(endpoint),
            ownership: Ownership::Adopted {
                pid: registration.pid,
            },
            registration: RwLock::new(registration),
            binary: binary.to_path_buf(),
        })
    }

    /// The service's URL with its Basic credential in the user info, for the
    /// async transport (`driver::v2::http`). Never log it.
    pub(crate) fn base_url(&self) -> anyhow::Result<url::Url> {
        let registration = self.registration.read();
        let mut url = url::Url::parse(&registration.url)
            .with_context(|| format!("invalid OpenCode service URL {}", registration.url))?;
        url.set_username(SERVICE_USER)
            .and_then(|()| url.set_password(Some(&registration.password)))
            .map_err(|()| anyhow!("the OpenCode service URL cannot carry a credential"))?;
        Ok(url)
    }

    pub(crate) fn endpoint(&self) -> Endpoint {
        self.endpoint.read().clone()
    }

    /// The pid of the adopted service, for reporting only. Waku never signals
    /// it: see the module doc.
    #[allow(dead_code)]
    pub(crate) fn pid(&self) -> Option<u32> {
        match self.ownership {
            Ownership::Adopted { pid } => Some(pid),
            Ownership::Private(_) => None,
        }
    }

    fn adopt_registration(&self, registration: ServiceRegistration, endpoint: Endpoint) {
        *self.endpoint.write() = endpoint;
        *self.registration.write() = registration;
    }
}

enum SlotState {
    Vacant,
    Starting,
    Running(Arc<OpenCodeService>),
}

/// There is exactly one service, so the slot is keyed on nothing. `Running`
/// holds a STRONG handle: the service object is permanent, which is what keeps
/// the two hazards this repo has already hit — a drop-before-start bouncing
/// the shared connection, and a shutdown running under the slot mutex —
/// structurally impossible here. The `Starting` + condvar discipline exists
/// only so N concurrent driver starts do not each run the 20s spawn poll.
fn slot() -> &'static (StdMutex<SlotState>, Condvar) {
    static SERVICE: OnceLock<(StdMutex<SlotState>, Condvar)> = OnceLock::new();
    SERVICE.get_or_init(|| (StdMutex::new(SlotState::Vacant), Condvar::new()))
}

/// Returns the shared service, starting one if none is registered.
///
/// The only entry point allowed to spawn, and reached only from driver start.
/// Blocking: discovery, an HTTP probe, and up to a 20s start poll.
pub(crate) fn shared(binary: &Path) -> anyhow::Result<Arc<OpenCodeService>> {
    acquire(binary, true)?.ok_or_else(|| anyhow!("the OpenCode background service is not running"))
}

/// Returns the shared service only if one is already healthy, NEVER starting
/// it. Opening the Resume picker or refreshing the model catalog must not
/// start the user's background daemon.
pub(crate) fn attached(binary: &Path) -> anyhow::Result<Option<Arc<OpenCodeService>>> {
    acquire(binary, false)
}

fn acquire(binary: &Path, may_spawn: bool) -> anyhow::Result<Option<Arc<OpenCodeService>>> {
    let (state, changed) = slot();
    loop {
        let mut slot = state.lock().unwrap();
        match &*slot {
            SlotState::Running(service) => {
                let service = Arc::clone(service);
                // Revalidation blocks on HTTP, so it happens outside the slot
                // lock: an acquire must never wait behind another acquire's
                // network round trip.
                drop(slot);
                return match revalidate(&service, may_spawn) {
                    Ok(true) => Ok(Some(service)),
                    Ok(false) => Ok(None),
                    Err(error) => Err(error),
                };
            }
            SlotState::Vacant => {
                *slot = SlotState::Starting;
                break;
            }
            SlotState::Starting => {
                let _unused = changed.wait(slot).unwrap();
            }
        }
    }

    let connected = connect(binary, may_spawn);
    let mut slot = state.lock().unwrap();
    match connected {
        Ok(Some(service)) => {
            *slot = SlotState::Running(Arc::clone(&service));
            changed.notify_all();
            Ok(Some(service))
        }
        // A failed or declined start resets to `Vacant` so the next session
        // simply retries.
        Ok(None) => {
            *slot = SlotState::Vacant;
            changed.notify_all();
            Ok(None)
        }
        Err(error) => {
            *slot = SlotState::Vacant;
            changed.notify_all();
            Err(error)
        }
    }
}

fn connect(binary: &Path, may_spawn: bool) -> anyhow::Result<Option<Arc<OpenCodeService>>> {
    if let Some(registration) = read_registration()
        && let Ok(endpoint) = probe(&registration)
    {
        return Ok(Some(OpenCodeService::adopt(binary, registration, endpoint)));
    }
    if !may_spawn {
        return Ok(None);
    }
    let (registration, endpoint) = spawn_service(binary)?;
    Ok(Some(OpenCodeService::adopt(binary, registration, endpoint)))
}

/// Re-checks the service the slot has been holding.
///
/// A CLI upgrade replaces the running service with a new url, pid and
/// password, and the user can stop it outright between two Waku tasks. Both
/// are absorbed by refreshing the endpoint IN PLACE — minting a second service
/// object would put a second reader on the same process and orphan every live
/// subscription.
fn revalidate(service: &Arc<OpenCodeService>, may_spawn: bool) -> anyhow::Result<bool> {
    if let Some(registration) = read_registration()
        && let Ok(endpoint) = probe(&registration)
    {
        service.adopt_registration(registration, endpoint);
        return Ok(true);
    }
    if !may_spawn {
        return Ok(false);
    }
    let (registration, endpoint) = spawn_service(&service.binary)?;
    service.adopt_registration(registration, endpoint);
    Ok(true)
}

/// Starts a background service and waits for it to register.
///
/// `serve --service` short-circuits on a healthy incumbent, so it is
/// idempotent and safe to race — including against a second Waku daemon, which
/// happens routinely when a release build and `Waku Debug.app` run side by
/// side. In that race the in-process condvar does nothing at all, and the
/// pid-checked rediscovery below is the only thing that converges both
/// processes on one service.
fn spawn_service(binary: &Path) -> anyhow::Result<(ServiceRegistration, Endpoint)> {
    ensure_service_cli(binary)?;
    let mut command = crate::command_env::command(binary);
    command
        .args(["serve", "--service"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut child = crate::command_env::spawn(&mut command)
        .context("failed to start the OpenCode background service")?;

    let deadline = Instant::now() + SERVICE_START_BUDGET;
    let mut exit = None;
    loop {
        if let Some(registration) = read_registration()
            && let Ok(endpoint) = probe(&registration)
        {
            // Reap the launcher if it has already detached. The service
            // itself is never signalled, and dropping a `Child` does not
            // kill it, so an attached service simply outlives this handle.
            let _unused = child.try_wait();
            return Ok((registration, endpoint));
        }
        if exit.is_none() {
            exit = child.try_wait().ok().flatten();
        }
        if Instant::now() >= deadline {
            // A non-zero exit is deliberately NOT fatal on its own: losing the
            // race to another Waku daemon looks exactly like that, and the
            // winner's service can still be seconds away from healthy.
            match exit {
                Some(status) => bail!(
                    "`opencode serve --service` exited ({status}) without registering a healthy service"
                ),
                None => bail!("timed out starting the OpenCode background service"),
            }
        }
        thread::sleep(SERVICE_START_POLL);
    }
}

/// Refuses to start a service with an OpenCode 1 binary.
///
/// OpenCode 1 has no background service. Its `serve` ignores `--service`,
/// starts an ordinary foreground server that never registers, and would
/// outlive the 20s start poll as an orphan Waku never stops. OpenCode 1 still
/// installs as `opencode` from some channels, so this is the one place where
/// that binary reaches a spawn, and it is caught before anything runs.
fn ensure_service_cli(binary: &Path) -> anyhow::Result<()> {
    let Some(output) = version_output(binary) else {
        // A probe that cannot run says nothing about the major; the start
        // poll below still bounds whatever happens next.
        return Ok(());
    };
    match opencode_one_version(&output) {
        Some(version) => bail!(
            "Waku needs OpenCode 2, but {} is OpenCode {version}. Install OpenCode 2 with `npm install -g @opencode/cli`, or choose its binary in Settings → Providers.",
            binary.display()
        ),
        None => Ok(()),
    }
}

fn version_output(binary: &Path) -> Option<String> {
    let mut command = crate::command_env::command(binary);
    command
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = crate::command_env::spawn(&mut command).ok()?;
    let deadline = Instant::now() + VERSION_TIMEOUT;
    while child.try_wait().ok()?.is_none() {
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        thread::sleep(Duration::from_millis(20));
    }
    let output = child.wait_with_output().ok()?;
    Some(format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    ))
}

/// OpenCode 1 prints its version bare (`1.18.32`), including its `0.0.0-…`
/// dev and snapshot builds. OpenCode 2 names itself (`opencode v2.0.22`), as
/// its prerelease channels did (`opencode2 v0.0.0-beta-19192`).
fn opencode_one_version(output: &str) -> Option<&str> {
    let line = output
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())?;
    if line.split_whitespace().count() != 1 {
        return None;
    }
    let major = line.split('.').next()?.parse::<u64>().ok()?;
    (major <= 1).then_some(line)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn registration(pid: u32) -> ServiceRegistration {
        ServiceRegistration {
            id: "796c624d-c94e-4086-b245-0a9560a14019".to_owned(),
            version: Some("2.0.22".to_owned()),
            url: "http://127.0.0.1:49374".to_owned(),
            pid,
            password: "secret".to_owned(),
        }
    }

    #[test]
    fn info_is_rejected_when_it_names_a_different_process_or_build() {
        // Decoded from the route's full answer, so a reshaped payload fails
        // here rather than at the first probe.
        let info = |version: &str, pid: u32| -> ServerInfo {
            serde_json::from_value(json!({
                "version": version,
                "pid": pid,
                "urls": ["http://127.0.0.1:49374"],
                "paths": {"tmp": "/tmp/opencode"}
            }))
            .unwrap()
        };
        // A stale descriptor still parses, and its port can already have been
        // reused by a ready listener that is not this service.
        assert!(accept_info(&registration(58286), &info("2.0.22", 99999)).is_err());
        assert!(accept_info(&registration(58286), &info("2.0.21", 58286)).is_err());
        assert!(accept_info(&registration(58286), &info("2.0.22", 58286)).is_ok());
        // A descriptor without a version is checked on the pid alone.
        let mut unversioned = registration(58286);
        unversioned.version = None;
        assert!(accept_info(&unversioned, &info("2.0.21", 58286)).is_ok());
    }

    #[test]
    fn only_a_bare_opencode_one_version_blocks_a_service_start() {
        for output in ["1.18.32\n", "\n0.15.31\n", "0.0.0-dev-202610020321"] {
            assert!(
                opencode_one_version(output).is_some(),
                "{output:?} is an OpenCode 1 build"
            );
        }
        for output in [
            "opencode v2.0.22\n",
            "opencode2 v0.0.0-beta-19192\n",
            "opencode v0.0.0-dev-20438",
            "2.1.0",
            "",
            "error: something went wrong",
        ] {
            assert!(
                opencode_one_version(output).is_none(),
                "{output:?} must not be mistaken for OpenCode 1"
            );
        }
    }

    /// OpenCode 1's `serve` ignores `--service` and would start a foreground
    /// server that never registers. The binary is refused before `serve` ever
    /// runs, so nothing is left behind.
    #[cfg(unix)]
    #[test]
    fn an_opencode_one_binary_never_reaches_serve() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory =
            std::env::temp_dir().join(format!("waku-opencode-one-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let served = directory.join("served");
        let binary = directory.join("opencode");
        std::fs::write(
            &binary,
            format!(
                "#!/bin/sh\nif [ \"$1\" = --version ]; then echo 1.18.32; exit 0; fi\ntouch '{}'\n",
                served.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();

        let error = spawn_service(&binary).unwrap_err().to_string();
        assert!(error.contains("OpenCode 1.18.32"), "{error}");
        assert!(error.contains("@opencode/cli"), "{error}");
        assert!(
            !served.exists(),
            "an OpenCode 1 binary must never run `serve`"
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn registration_debug_never_prints_the_password() {
        let rendered = format!("{:?}", registration(58286));
        assert!(!rendered.contains("secret"), "{rendered}");
        assert!(rendered.contains("<redacted>"));
    }

    #[test]
    fn the_base_url_carries_the_service_credential() {
        let service = OpenCodeService::adopt(
            Path::new("opencode"),
            registration(58286),
            Endpoint::basic("http://127.0.0.1:49374", SERVICE_USER, "secret").unwrap(),
        );
        let url = service.base_url().unwrap();
        assert_eq!(url.username(), "opencode");
        assert_eq!(url.password(), Some("secret"));
        assert_eq!(url.port(), Some(49374));
    }

    #[test]
    #[ignore = "requires the user's running OpenCode service"]
    fn adopts_the_registered_service() {
        let registration = read_registration().expect("no OpenCode service is registered");
        let endpoint = probe(&registration).expect("the registered service is not healthy");
        assert_eq!(endpoint.host, "127.0.0.1");
        assert!(endpoint.auth.is_some());
        assert!(registration.version.is_some());
    }
}
