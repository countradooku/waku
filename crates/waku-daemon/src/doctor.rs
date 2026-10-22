//! `waku-daemon doctor [--json]` (#58): checks the daemon's data directory,
//! its orchestrator database (integrity, migrations, the projection
//! `verify`, the effect outbox), the configured providers' binaries, the
//! local socket (a v8 handshake with the local token) and the background
//! service.
//!
//! The checks take their inputs explicitly ([`Inputs`]) so tests run them
//! against temporary directories. Opening the database applies migrations
//! the binary knows and the file lacks, as a daemon start would; it never
//! rebuilds projections.

use std::path::{Path, PathBuf};

use serde::Serialize;
use waku_core::DaemonSettings;
use waku_orchestrator::projection::maintenance;
use waku_orchestrator::store::{Store, max_sequence};

use crate::service::{Runner, ServiceSpec, ServiceState};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Level {
    Ok,
    Warn,
    Fail,
}

impl Level {
    const fn tag(self) -> &'static str {
        match self {
            Self::Ok => "ok  ",
            Self::Warn => "warn",
            Self::Fail => "FAIL",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Check {
    pub name: String,
    pub level: Level,
    pub detail: String,
}

impl Check {
    fn new(name: &str, level: Level, detail: impl Into<String>) -> Self {
        Self {
            name: name.to_owned(),
            level,
            detail: detail.into(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Report {
    pub checks: Vec<Check>,
}

impl Report {
    pub fn failed(&self) -> bool {
        self.checks.iter().any(|check| check.level == Level::Fail)
    }

    #[cfg(test)]
    pub fn check(&self, name: &str) -> Option<&Check> {
        self.checks.iter().find(|check| check.name == name)
    }

    /// One line per check: `ok   database  …`.
    pub fn render(&self) -> String {
        let width = self
            .checks
            .iter()
            .map(|check| check.name.len())
            .max()
            .unwrap_or(0);
        let mut out = String::new();
        for check in &self.checks {
            out.push_str(&format!(
                "{}  {:width$}  {}\n",
                check.level.tag(),
                check.name,
                check.detail
            ));
        }
        out
    }
}

/// Where to look.
pub struct Inputs<'a> {
    pub data_dir: PathBuf,
    pub settings: DaemonSettings,
    pub socket: PathBuf,
    pub token: PathBuf,
    pub log: PathBuf,
    /// Resolves a provider binary (the login shell's search path in the
    /// real doctor).
    pub find_binary: &'a dyn Fn(&Path) -> Option<PathBuf>,
    /// The background service, if it should be checked.
    pub service: Option<(ServiceSpec, &'a dyn Runner)>,
}

pub fn run(inputs: &Inputs<'_>) -> Report {
    let mut report = Report::default();
    report.checks.push(if inputs.data_dir.is_dir() {
        Check::new(
            "data directory",
            Level::Ok,
            inputs.data_dir.display().to_string(),
        )
    } else {
        Check::new(
            "data directory",
            Level::Warn,
            format!(
                "{} does not exist yet (the daemon creates it)",
                inputs.data_dir.display()
            ),
        )
    });
    database_checks(
        &inputs.data_dir.join(waku_core::daemon_host::STATE_DATABASE),
        &mut report,
    );
    provider_checks(inputs, &mut report);
    report
        .checks
        .push(socket_check(&inputs.socket, &inputs.token));
    if let Some((spec, runner)) = &inputs.service {
        let state = crate::service::status(spec, *runner);
        let level = match &state {
            ServiceState::Running { .. } | ServiceState::NotInstalled => Level::Ok,
            ServiceState::Stopped | ServiceState::Unknown(_) => Level::Warn,
        };
        report.checks.push(Check::new(
            "service",
            level,
            format!(
                "{} {}: {}",
                spec.platform.name(),
                spec.label,
                state.describe()
            ),
        ));
    }
    report.checks.push(Check::new(
        "logs",
        Level::Ok,
        inputs.log.display().to_string(),
    ));
    report
}

fn database_checks(path: &Path, report: &mut Report) {
    if !path.exists() {
        report.checks.push(Check::new(
            "database",
            Level::Warn,
            format!("{} does not exist yet", path.display()),
        ));
        return;
    }
    let store = match Store::open(path, 1) {
        Ok(store) => store,
        Err(error) => {
            report.checks.push(Check::new(
                "database",
                Level::Fail,
                format!("{} does not open: {error}", path.display()),
            ));
            return;
        }
    };
    let facts = store.reader().with(|connection| {
        let integrity: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
        let migrations: i64 =
            connection.query_row("SELECT COUNT(*) FROM orchestrator_migrations", [], |row| {
                row.get(0)
            })?;
        let threads: i64 = connection.query_row(
            "SELECT COUNT(*) FROM p_threads WHERE deleted_at IS NULL",
            [],
            |row| row.get(0),
        )?;
        let outbox = connection
            .prepare("SELECT status, COUNT(*) FROM effect_outbox GROUP BY status")?
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let head = max_sequence(connection)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(error.to_string().into()))?;
        Ok::<_, rusqlite::Error>((integrity, migrations, threads, outbox, head))
    });
    let (integrity, migrations, threads, outbox, head) = match facts {
        Ok(facts) => facts,
        Err(error) => {
            report.checks.push(Check::new(
                "database",
                Level::Fail,
                format!("could not read {}: {error}", path.display()),
            ));
            return;
        }
    };
    report.checks.push(if integrity == "ok" {
        Check::new(
            "database",
            Level::Ok,
            format!(
                "{}: integrity ok, {migrations} migrations, {threads} threads, {head} events",
                path.display()
            ),
        )
    } else {
        Check::new(
            "database",
            Level::Fail,
            format!("{}: quick_check says {integrity}", path.display()),
        )
    });
    let count = |status: &str| {
        outbox
            .iter()
            .find(|(name, _)| name == status)
            .map_or(0, |(_, count)| *count)
    };
    let (pending, running, failed) = (count("pending"), count("running"), count("failed"));
    report.checks.push(Check::new(
        "effects",
        if failed > 0 { Level::Warn } else { Level::Ok },
        format!("{pending} pending, {running} running, {failed} failed"),
    ));
    report.checks.push(match maintenance::verify(&store) {
        Ok(verified) if verified.is_clean() => Check::new(
            "projections",
            Level::Ok,
            format!(
                "verify: {} rows match a rebuild of {} events",
                verified.rows_checked, verified.log_sequence
            ),
        ),
        Ok(verified) => Check::new(
            "projections",
            Level::Fail,
            format!(
                "verify: projections at {} of {} events, {} mismatches, {} dangling references, {} effects without events",
                verified.projection_sequence,
                verified.log_sequence,
                verified.mismatches.len(),
                verified.dangling_references.len(),
                verified.effects_without_events.len()
            ),
        ),
        Err(error) => Check::new("projections", Level::Fail, format!("verify failed: {error}")),
    });
}

fn provider_checks(inputs: &Inputs<'_>, report: &mut Report) {
    let instances = waku_core::daemon_host::provider_instances(&inputs.settings);
    let mut found = Vec::new();
    let mut missing = Vec::new();
    for instance in &instances {
        let id = instance.config.instance_id.as_str().to_owned();
        match (inputs.find_binary)(&instance.config.binary) {
            Some(path) => found.push(format!("{id} ({})", path.display())),
            None => missing.push(format!("{id} ({})", instance.config.binary.display())),
        }
    }
    report.checks.push(Check::new(
        "providers",
        if found.is_empty() {
            Level::Warn
        } else {
            Level::Ok
        },
        if found.is_empty() {
            "no provider CLI was found".to_owned()
        } else {
            found.join(", ")
        },
    ));
    if !missing.is_empty() {
        report.checks.push(Check::new(
            "providers missing",
            Level::Warn,
            missing.join(", "),
        ));
    }
}

fn socket_check(socket: &Path, token_path: &Path) -> Check {
    let present = if cfg!(windows) { true } else { socket.exists() };
    if !present {
        return Check::new(
            "socket",
            Level::Warn,
            format!("no daemon listens on {}", socket.display()),
        );
    }
    let Ok(token) = std::fs::read_to_string(token_path) else {
        return Check::new(
            "socket",
            Level::Warn,
            format!(
                "{} exists, but there is no local token at {} (a desktop-owned daemon)",
                socket.display(),
                token_path.display()
            ),
        );
    };
    let address = waku_client::local_endpoint_address(&socket.to_string_lossy());
    match waku_client::V8Client::connect(&address, token.trim()) {
        Ok(client) => {
            let environment = client.welcome().environment.id;
            client.close();
            Check::new(
                "socket",
                Level::Ok,
                format!("{} answers (environment {environment})", socket.display()),
            )
        }
        Err(error) if cfg!(windows) => Check::new(
            "socket",
            Level::Warn,
            format!("no daemon answers on {}: {error:#}", socket.display()),
        ),
        Err(error) => Check::new(
            "socket",
            Level::Fail,
            format!("{} does not answer: {error:#}", socket.display()),
        ),
    }
}

/// `waku-daemon doctor [--json]`: exits 1 when a check fails.
pub fn cli(arguments: &[String]) -> anyhow::Result<bool> {
    let json = match arguments {
        [] => false,
        [flag] if flag == "--json" => true,
        _ => anyhow::bail!("usage: waku-daemon doctor [--json]"),
    };
    let data_dir = waku_core::persistence::default_data_dir();
    let settings = waku_core::DaemonSettingsStore::open(DaemonSettings::default_path())
        .map(|store| store.get())
        .unwrap_or_default();
    let executable = std::env::current_exe()?;
    let find = |binary: &Path| {
        if binary.is_absolute() {
            binary.is_file().then(|| binary.to_path_buf())
        } else {
            waku_core::command_env::find_executable(&binary.to_string_lossy())
        }
    };
    let runner = crate::service::SystemRunner;
    let report = run(&Inputs {
        data_dir,
        settings,
        socket: waku_protocol::identity::daemon_socket_path(),
        token: waku_protocol::identity::local_token_path(),
        log: waku_protocol::identity::daemon_log_path(),
        find_binary: &find,
        service: ServiceSpec::current(executable)
            .ok()
            .map(|spec| (spec, &runner as &dyn Runner)),
    });
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print!("{}", report.render());
    }
    Ok(!report.failed())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use waku_orchestrator::{
        Dependencies, NoCapabilities, Orchestrator, OrchestratorConfig, SystemClock, UuidV7Ids,
    };
    use waku_protocol::orchestration::*;

    use super::*;

    fn inputs<'a>(root: &Path, find: &'a dyn Fn(&Path) -> Option<PathBuf>) -> Inputs<'a> {
        Inputs {
            data_dir: root.join("data"),
            settings: DaemonSettings::default(),
            socket: root.join("run/daemon.sock"),
            token: root.join("run/local.token"),
            log: root.join("logs/daemon.log"),
            find_binary: find,
            service: None,
        }
    }

    fn only_codex(binary: &Path) -> Option<PathBuf> {
        (binary == Path::new("codex")).then(|| PathBuf::from("/opt/bin/codex"))
    }

    #[test]
    fn a_fresh_machine_warns_but_does_not_fail() {
        let root = tempfile::tempdir().unwrap();
        let report = run(&inputs(root.path(), &only_codex));
        assert!(!report.failed(), "{}", report.render());
        assert_eq!(report.check("data directory").unwrap().level, Level::Warn);
        assert_eq!(report.check("database").unwrap().level, Level::Warn);
        assert_eq!(report.check("socket").unwrap().level, Level::Warn);
        let providers = report.check("providers").unwrap();
        assert_eq!(providers.level, Level::Ok);
        assert_eq!(providers.detail, "codex (/opt/bin/codex)");
        assert!(
            report
                .check("providers missing")
                .unwrap()
                .detail
                .contains("claude (claude)")
        );
        let text = report.render();
        assert!(text.contains("warn  database"), "{text}");
        assert!(text.contains("ok    providers"), "{text}");
        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["checks"][0]["level"], "warn");
    }

    fn open(path: &Path) -> Arc<Orchestrator> {
        Orchestrator::open(
            OrchestratorConfig::new(path),
            Dependencies {
                clock: Arc::new(SystemClock),
                ids: Box::new(UuidV7Ids),
                capabilities: Arc::new(NoCapabilities),
            },
        )
        .unwrap()
    }

    #[test]
    fn a_healthy_database_passes_and_a_damaged_projection_fails() {
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        let database = data.join(waku_core::daemon_host::STATE_DATABASE);
        let orchestrator = open(&database);
        orchestrator
            .dispatch(CommandEnvelope {
                command_id: CommandId::new("doctor:project").unwrap(),
                issued_by: Actor::User,
                source: CreationSource::Desktop,
                command: Command::ProjectCreate {
                    project_id: ProjectId(uuid::Uuid::from_u128(1)),
                    name: "P".into(),
                    path: PathBuf::from("/repo"),
                    order_key: None,
                },
            })
            .unwrap();
        orchestrator.shutdown();
        drop(orchestrator);

        let report = run(&inputs(root.path(), &only_codex));
        assert!(!report.failed(), "{}", report.render());
        let database_check = report.check("database").unwrap();
        assert_eq!(database_check.level, Level::Ok);
        assert!(
            database_check.detail.contains("integrity ok"),
            "{}",
            database_check.detail
        );
        assert!(
            database_check.detail.contains("1 events"),
            "{}",
            database_check.detail
        );
        assert_eq!(report.check("projections").unwrap().level, Level::Ok);
        assert_eq!(
            report.check("effects").unwrap().detail,
            "0 pending, 0 running, 0 failed"
        );

        // A projection row that the log does not explain.
        let connection = rusqlite::Connection::open(&database).unwrap();
        connection
            .execute("UPDATE p_projects SET name = 'tampered'", [])
            .unwrap();
        drop(connection);
        let report = run(&inputs(root.path(), &only_codex));
        assert!(report.failed());
        let projections = report.check("projections").unwrap();
        assert_eq!(projections.level, Level::Fail);
        assert!(
            projections.detail.contains("1 mismatches"),
            "{}",
            projections.detail
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_socket_nobody_answers_fails() {
        let root = tempfile::tempdir().unwrap();
        let run_dir = root.path().join("run");
        std::fs::create_dir_all(&run_dir).unwrap();
        // A socket file left behind by a dead daemon.
        let socket = run_dir.join("daemon.sock");
        drop(std::os::unix::net::UnixListener::bind(&socket).unwrap());
        let report = run(&inputs(root.path(), &only_codex));
        assert_eq!(
            report.check("socket").unwrap().level,
            Level::Warn,
            "no token yet"
        );
        std::fs::write(run_dir.join("local.token"), "secret").unwrap();
        let report = run(&inputs(root.path(), &only_codex));
        assert_eq!(report.check("socket").unwrap().level, Level::Fail);
    }
}
