//! `waku-daemon service install|uninstall|status` (#58): the daemon as a
//! per-user background service that outlives the desktop.
//!
//! - macOS: a launchd user agent, `~/Library/LaunchAgents/<label>.plist`,
//!   loaded with `launchctl bootstrap gui/<uid>`.
//! - Linux: a systemd user unit, `~/.config/systemd/user/<unit>.service`,
//!   enabled with `systemctl --user enable --now`.
//! - Windows: a scheduled task that starts at logon (`schtasks`). This path
//!   is written from the documentation and has not run on Windows.
//!
//! Every definition runs a stable launcher (`~/.waku-fork/bin/
//! waku-fork-daemon-service`, T3's service launcher) that execs the daemon
//! binary which installed the service with `--service`, so an in-place app
//! update keeps the service valid and a reinstall points it elsewhere.
//! Output goes to `~/.waku-fork/logs/waku-fork-daemon.log`. Debug builds use
//! `waku-fork-dev-*` names and their own label, so both can be installed. launchd and systemd restart the daemon
//! when it dies (not when it exits cleanly), and the restarted daemon
//! recovers the turns the crash interrupted (#59).
//!
//! Everything is a [`Plan`] first, built by pure functions, so the
//! generated files are snapshot-tested and `--dry-run` prints what would
//! happen; [`apply`] writes the files and runs the commands through a
//! [`Runner`].

use std::io;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, bail};

/// The service manager the definitions are for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Platform {
    Launchd,
    Systemd,
    WindowsTask,
}

impl Platform {
    pub const fn current() -> Self {
        if cfg!(target_os = "macos") {
            Self::Launchd
        } else if cfg!(windows) {
            Self::WindowsTask
        } else {
            Self::Systemd
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Launchd => "launchd user agent",
            Self::Systemd => "systemd user unit",
            Self::WindowsTask => "scheduled task",
        }
    }
}

/// Everything a definition names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceSpec {
    pub platform: Platform,
    /// launchd label, systemd unit stem, Windows task name.
    pub label: String,
    /// The daemon binary the launcher runs.
    pub executable: PathBuf,
    /// The stable launcher the definition runs.
    pub launcher: PathBuf,
    /// Where stdout and stderr go.
    pub log: PathBuf,
    /// The user's home directory (definitions live under it).
    pub home: PathBuf,
    /// The user id launchd domains are named by (`gui/<uid>`).
    pub uid: u32,
}

impl ServiceSpec {
    /// The spec for this user and build, running `executable`.
    pub fn current(executable: PathBuf) -> anyhow::Result<Self> {
        let home = dirs::home_dir().context("this user has no home directory")?;
        Ok(Self {
            platform: Platform::current(),
            label: waku_protocol::identity::service_label(),
            executable,
            launcher: waku_protocol::identity::service_launcher_path(),
            log: waku_protocol::identity::daemon_log_path(),
            home,
            uid: current_uid(),
        })
    }

    /// The service definition file (the Windows task lives in the task
    /// scheduler; its definition path is the launcher).
    pub fn definition_path(&self) -> PathBuf {
        match self.platform {
            Platform::Launchd => self
                .home
                .join("Library/LaunchAgents")
                .join(format!("{}.plist", self.label)),
            Platform::Systemd => self
                .home
                .join(".config/systemd/user")
                .join(self.unit_name()),
            Platform::WindowsTask => self.launcher.clone(),
        }
    }

    /// `<label with dashes>.service`.
    pub fn unit_name(&self) -> String {
        format!("{}.service", self.label.replace('.', "-"))
    }

    fn launchd_target(&self) -> String {
        format!("gui/{}/{}", self.uid, self.label)
    }
}

#[cfg(unix)]
fn current_uid() -> u32 {
    // SAFETY: `geteuid` has no preconditions and cannot fail.
    unsafe { libc::geteuid() }
}

#[cfg(not(unix))]
fn current_uid() -> u32 {
    0
}

// -- generated files -------------------------------------------------------------

fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// A double-quoted POSIX shell word.
fn shell_quote(text: &str) -> String {
    let mut quoted = String::with_capacity(text.len() + 2);
    quoted.push('"');
    for character in text.chars() {
        if matches!(character, '"' | '\\' | '$' | '`') {
            quoted.push('\\');
        }
        quoted.push(character);
    }
    quoted.push('"');
    quoted
}

/// The launcher script (`sh` on Unix, `cmd` on Windows).
pub fn launcher_script(spec: &ServiceSpec) -> String {
    let executable = spec.executable.display().to_string();
    match spec.platform {
        Platform::WindowsTask => format!(
            "@echo off\r\nrem Generated by `waku-daemon service install` (#58). It runs the daemon\r\n\
             rem that installed the service; reinstall to point it at another build.\r\n\
             \"{executable}\" --service >> \"{}\" 2>&1\r\n",
            spec.log.display()
        ),
        Platform::Launchd | Platform::Systemd => format!(
            "#!/bin/sh\n# Generated by `waku-daemon service install` (#58). It runs the daemon\n\
             # that installed the service; reinstall to point it at another build.\n\
             exec {} --service \"$@\"\n",
            shell_quote(&executable)
        ),
    }
}

/// The launchd user agent.
pub fn launchd_plist(spec: &ServiceSpec) -> String {
    let launcher = xml_escape(&spec.launcher.display().to_string());
    let log = xml_escape(&spec.log.display().to_string());
    let home = xml_escape(&spec.home.display().to_string());
    let label = xml_escape(&spec.label);
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>{label}</string>
	<key>ProgramArguments</key>
	<array>
		<string>{launcher}</string>
	</array>
	<key>RunAtLoad</key>
	<true/>
	<key>KeepAlive</key>
	<dict>
		<key>SuccessfulExit</key>
		<false/>
	</dict>
	<key>ProcessType</key>
	<string>Interactive</string>
	<key>WorkingDirectory</key>
	<string>{home}</string>
	<key>StandardOutPath</key>
	<string>{log}</string>
	<key>StandardErrorPath</key>
	<string>{log}</string>
</dict>
</plist>
"#
    )
}

/// The systemd user unit.
pub fn systemd_unit(spec: &ServiceSpec) -> String {
    let launcher = spec.launcher.display().to_string().replace('"', "\\\"");
    let log = spec.log.display();
    format!(
        "[Unit]\n\
         Description=Waku daemon (background service, #58)\n\
         \n\
         [Service]\n\
         Type=simple\n\
         ExecStart=\"{launcher}\"\n\
         Restart=on-failure\n\
         RestartSec=2\n\
         StandardOutput=append:{log}\n\
         StandardError=append:{log}\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n"
    )
}

// -- plans -----------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedFile {
    pub path: PathBuf,
    pub contents: String,
    /// Mode 0755 instead of 0644 (the launcher).
    pub executable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedCommand {
    pub program: String,
    pub args: Vec<String>,
    /// A failure is expected sometimes (unloading what is not loaded).
    pub allow_failure: bool,
}

impl PlannedCommand {
    fn new(program: &str, args: &[&str]) -> Self {
        Self {
            program: program.to_owned(),
            args: args.iter().map(|arg| (*arg).to_owned()).collect(),
            allow_failure: false,
        }
    }

    fn tolerant(mut self) -> Self {
        self.allow_failure = true;
        self
    }

    pub fn display(&self) -> String {
        std::iter::once(self.program.as_str())
            .chain(self.args.iter().map(String::as_str))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// What an install or uninstall does, in order: create `directories`,
/// write `files`, run `commands`, remove `remove`, run `after_remove`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Plan {
    pub directories: Vec<PathBuf>,
    pub files: Vec<PlannedFile>,
    pub commands: Vec<PlannedCommand>,
    pub remove: Vec<PathBuf>,
    pub after_remove: Vec<PlannedCommand>,
}

pub fn install_plan(spec: &ServiceSpec) -> Plan {
    let mut plan = Plan {
        directories: vec![
            spec.launcher
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_default(),
            spec.log.parent().map(Path::to_path_buf).unwrap_or_default(),
        ],
        files: vec![PlannedFile {
            path: spec.launcher.clone(),
            contents: launcher_script(spec),
            executable: true,
        }],
        ..Plan::default()
    };
    let definition = spec.definition_path();
    match spec.platform {
        Platform::Launchd => {
            plan.files.push(PlannedFile {
                path: definition.clone(),
                contents: launchd_plist(spec),
                executable: false,
            });
            let target = spec.launchd_target();
            let domain = format!("gui/{}", spec.uid);
            let definition = definition.display().to_string();
            plan.commands = vec![
                // A reinstall replaces a loaded agent.
                PlannedCommand::new("launchctl", &["bootout", &target]).tolerant(),
                PlannedCommand::new("launchctl", &["bootstrap", &domain, &definition]),
            ];
        }
        Platform::Systemd => {
            plan.files.push(PlannedFile {
                path: definition,
                contents: systemd_unit(spec),
                executable: false,
            });
            let unit = spec.unit_name();
            plan.commands = vec![
                PlannedCommand::new("systemctl", &["--user", "daemon-reload"]),
                PlannedCommand::new("systemctl", &["--user", "enable", "--now", &unit]),
            ];
        }
        Platform::WindowsTask => {
            let launcher = format!("\"{}\"", spec.launcher.display());
            plan.commands = vec![
                PlannedCommand::new(
                    "schtasks",
                    &[
                        "/Create",
                        "/TN",
                        &spec.label,
                        "/TR",
                        &launcher,
                        "/SC",
                        "ONLOGON",
                        "/RL",
                        "LIMITED",
                        "/F",
                    ],
                ),
                PlannedCommand::new("schtasks", &["/Run", "/TN", &spec.label]),
            ];
        }
    }
    if let Some(parent) = spec.definition_path().parent()
        && !plan.directories.iter().any(|directory| directory == parent)
    {
        plan.directories.push(parent.to_path_buf());
    }
    plan
}

pub fn uninstall_plan(spec: &ServiceSpec) -> Plan {
    let mut plan = Plan::default();
    match spec.platform {
        Platform::Launchd => {
            plan.commands = vec![
                PlannedCommand::new("launchctl", &["bootout", &spec.launchd_target()]).tolerant(),
            ];
            plan.remove.push(spec.definition_path());
        }
        Platform::Systemd => {
            let unit = spec.unit_name();
            plan.commands = vec![
                PlannedCommand::new("systemctl", &["--user", "disable", "--now", &unit]).tolerant(),
            ];
            plan.remove.push(spec.definition_path());
            plan.after_remove = vec![PlannedCommand::new(
                "systemctl",
                &["--user", "daemon-reload"],
            )];
        }
        Platform::WindowsTask => {
            plan.commands = vec![
                PlannedCommand::new("schtasks", &["/End", "/TN", &spec.label]).tolerant(),
                PlannedCommand::new("schtasks", &["/Delete", "/TN", &spec.label, "/F"]).tolerant(),
            ];
        }
    }
    plan.remove.push(spec.launcher.clone());
    plan
}

// -- running plans ---------------------------------------------------------------

/// What a command printed and whether it succeeded.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CommandOutput {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}

/// Runs service-manager commands; tests record them instead.
pub trait Runner {
    fn run(&self, program: &str, args: &[String]) -> io::Result<CommandOutput>;
}

/// The real `launchctl`, `systemctl` and `schtasks`.
pub struct SystemRunner;

impl Runner for SystemRunner {
    fn run(&self, program: &str, args: &[String]) -> io::Result<CommandOutput> {
        let output = std::process::Command::new(program).args(args).output()?;
        Ok(CommandOutput {
            success: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

/// Describes `plan` line by line, as `--dry-run` prints it.
pub fn describe(plan: &Plan) -> Vec<String> {
    let mut lines = Vec::new();
    for directory in &plan.directories {
        if !directory.as_os_str().is_empty() {
            lines.push(format!("mkdir -p {}", directory.display()));
        }
    }
    for file in &plan.files {
        lines.push(format!(
            "write {} ({} bytes{})",
            file.path.display(),
            file.contents.len(),
            if file.executable { ", executable" } else { "" }
        ));
    }
    for command in &plan.commands {
        lines.push(format!("run {}", command.display()));
    }
    for path in &plan.remove {
        lines.push(format!("remove {}", path.display()));
    }
    for command in &plan.after_remove {
        lines.push(format!("run {}", command.display()));
    }
    lines
}

/// Carries out `plan` in its order. A command that fails stops the plan
/// unless it is allowed to fail.
pub fn apply(plan: &Plan, runner: &dyn Runner) -> anyhow::Result<()> {
    for directory in &plan.directories {
        if !directory.as_os_str().is_empty() {
            std::fs::create_dir_all(directory)
                .with_context(|| format!("could not create {}", directory.display()))?;
        }
    }
    for file in &plan.files {
        write_file(file)?;
    }
    run_commands(&plan.commands, runner)?;
    for path in &plan.remove {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| format!("could not remove {}", path.display()));
            }
        }
    }
    run_commands(&plan.after_remove, runner)
}

fn run_commands(commands: &[PlannedCommand], runner: &dyn Runner) -> anyhow::Result<()> {
    for command in commands {
        let output = runner
            .run(&command.program, &command.args)
            .with_context(|| format!("could not run {}", command.program))?;
        if !output.success && !command.allow_failure {
            bail!(
                "{} failed: {}",
                command.display(),
                output.stderr.trim().lines().last().unwrap_or("no output")
            );
        }
    }
    Ok(())
}

fn write_file(file: &PlannedFile) -> anyhow::Result<()> {
    let temporary = file.path.with_extension("tmp");
    std::fs::write(&temporary, &file.contents)
        .with_context(|| format!("could not write {}", temporary.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = if file.executable { 0o755 } else { 0o644 };
        std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(mode))?;
    }
    std::fs::rename(&temporary, &file.path)
        .with_context(|| format!("could not write {}", file.path.display()))
}

// -- status ----------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServiceState {
    NotInstalled,
    /// Installed but not running.
    Stopped,
    Running {
        pid: Option<u32>,
    },
    /// Installed; the service manager's answer could not be read.
    Unknown(String),
}

impl ServiceState {
    pub fn describe(&self) -> String {
        match self {
            Self::NotInstalled => "not installed".to_owned(),
            Self::Stopped => "installed, not running".to_owned(),
            Self::Running { pid: Some(pid) } => format!("running (pid {pid})"),
            Self::Running { pid: None } => "running".to_owned(),
            Self::Unknown(detail) => format!("installed, state unknown: {detail}"),
        }
    }
}

fn field<'a>(text: &'a str, key: &str) -> Option<&'a str> {
    text.lines().find_map(|line| {
        let (name, value) = line.split_once(['=', ':'])?;
        (name.trim().eq_ignore_ascii_case(key)).then(|| value.trim())
    })
}

/// The service's state, from its definition and the service manager.
pub fn status(spec: &ServiceSpec, runner: &dyn Runner) -> ServiceState {
    let installed = match spec.platform {
        Platform::WindowsTask => {
            let query = runner.run(
                "schtasks",
                &["/Query", "/TN", &spec.label, "/FO", "LIST"].map(str::to_owned),
            );
            match query {
                Ok(output) if output.success => {
                    return match field(&output.stdout, "Status") {
                        Some(status) if status.eq_ignore_ascii_case("Running") => {
                            ServiceState::Running { pid: None }
                        }
                        Some(_) => ServiceState::Stopped,
                        None => ServiceState::Unknown(output.stdout.trim().to_owned()),
                    };
                }
                Ok(_) => false,
                Err(error) => return ServiceState::Unknown(error.to_string()),
            }
        }
        _ => spec.definition_path().exists(),
    };
    if !installed {
        return ServiceState::NotInstalled;
    }
    match spec.platform {
        Platform::Launchd => {
            match runner.run("launchctl", &["print".to_owned(), spec.launchd_target()]) {
                // Not loaded: installed but not running.
                Ok(output) if !output.success => ServiceState::Stopped,
                Ok(output) => match field(&output.stdout, "state") {
                    Some("running") => ServiceState::Running {
                        pid: field(&output.stdout, "pid").and_then(|pid| pid.parse().ok()),
                    },
                    Some(_) => ServiceState::Stopped,
                    None => ServiceState::Unknown("launchctl printed no state".to_owned()),
                },
                Err(error) => ServiceState::Unknown(error.to_string()),
            }
        }
        Platform::Systemd => {
            let unit = spec.unit_name();
            let active = runner.run(
                "systemctl",
                &["--user", "is-active", &unit].map(str::to_owned),
            );
            match active {
                Ok(output) if output.stdout.trim() == "active" => {
                    let pid = runner
                        .run(
                            "systemctl",
                            &["--user", "show", "-p", "MainPID", "--value", &unit]
                                .map(str::to_owned),
                        )
                        .ok()
                        .and_then(|output| output.stdout.trim().parse::<u32>().ok())
                        .filter(|pid| *pid != 0);
                    ServiceState::Running { pid }
                }
                Ok(_) => ServiceState::Stopped,
                Err(error) => ServiceState::Unknown(error.to_string()),
            }
        }
        Platform::WindowsTask => unreachable!("answered above"),
    }
}

// -- command line ----------------------------------------------------------------

/// `waku-daemon service <install|uninstall|status> [--dry-run]`.
pub fn cli(arguments: &[String]) -> anyhow::Result<()> {
    let mut action = None;
    let mut dry_run = false;
    for argument in arguments {
        match argument.as_str() {
            "--dry-run" => dry_run = true,
            "install" | "uninstall" | "status" if action.is_none() => {
                action = Some(argument.as_str());
            }
            "--help" | "-h" => {
                println!("usage: waku-daemon service <install|uninstall|status> [--dry-run]");
                return Ok(());
            }
            other => bail!("unknown service argument {other:?}"),
        }
    }
    let executable = std::env::current_exe()
        .and_then(|path| path.canonicalize())
        .context("could not locate the waku-daemon binary")?;
    let spec = ServiceSpec::current(executable)?;
    match action {
        Some("install") => run_plan(&install_plan(&spec), dry_run, || {
            println!(
                "installed the {} {}; logs: {}",
                spec.platform.name(),
                spec.label,
                spec.log.display()
            );
        }),
        Some("uninstall") => run_plan(&uninstall_plan(&spec), dry_run, || {
            println!("uninstalled the {} {}", spec.platform.name(), spec.label);
        }),
        Some("status") => {
            let state = status(&spec, &SystemRunner);
            println!(
                "{} {}: {}",
                spec.platform.name(),
                spec.label,
                state.describe()
            );
            println!("definition: {}", spec.definition_path().display());
            println!("launcher:   {}", spec.launcher.display());
            println!("logs:       {}", spec.log.display());
            Ok(())
        }
        _ => bail!("usage: waku-daemon service <install|uninstall|status> [--dry-run]"),
    }
}

fn run_plan(plan: &Plan, dry_run: bool, done: impl FnOnce()) -> anyhow::Result<()> {
    if dry_run {
        for line in describe(plan) {
            println!("{line}");
        }
        return Ok(());
    }
    apply(plan, &SystemRunner)?;
    done();
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    /// Records commands and answers them from a script.
    #[derive(Default)]
    struct FakeRunner {
        calls: RefCell<Vec<String>>,
        answers: Vec<(&'static str, CommandOutput)>,
    }

    impl Runner for FakeRunner {
        fn run(&self, program: &str, args: &[String]) -> io::Result<CommandOutput> {
            let line = std::iter::once(program.to_owned())
                .chain(args.iter().cloned())
                .collect::<Vec<_>>()
                .join(" ");
            self.calls.borrow_mut().push(line.clone());
            Ok(self
                .answers
                .iter()
                .find(|(prefix, _)| line.starts_with(prefix))
                .map(|(_, output)| output.clone())
                .unwrap_or(CommandOutput {
                    success: true,
                    ..CommandOutput::default()
                }))
        }
    }

    fn spec(platform: Platform, root: &Path) -> ServiceSpec {
        ServiceSpec {
            platform,
            label: "io.github.countradooku.waku.daemon".into(),
            executable: PathBuf::from("/Applications/Waku.app/Contents/MacOS/waku-daemon"),
            launcher: root.join(".waku-fork/bin/waku-daemon-service"),
            log: root.join(".waku-fork/logs/daemon.log"),
            home: root.to_path_buf(),
            uid: 501,
        }
    }

    /// Compares with `src/snapshots/<name>`; `WAKU_UPDATE_SNAPSHOTS=1`
    /// rewrites it.
    fn snapshot(name: &str, actual: &str) {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/snapshots")
            .join(name);
        if std::env::var_os("WAKU_UPDATE_SNAPSHOTS").is_some() {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, actual).unwrap();
        }
        let expected = std::fs::read_to_string(&path)
            .unwrap_or_else(|_| panic!("{} is missing", path.display()));
        assert_eq!(actual, expected, "{name} changed (WAKU_UPDATE_SNAPSHOTS=1)");
    }

    #[test]
    fn generated_definitions_match_their_snapshots() {
        let root = Path::new("/Users/dev");
        snapshot(
            "launchd.plist",
            &launchd_plist(&spec(Platform::Launchd, root)),
        );
        snapshot(
            "systemd.service",
            &systemd_unit(&spec(Platform::Systemd, Path::new("/home/dev"))),
        );
        snapshot(
            "launcher.sh",
            &launcher_script(&spec(Platform::Launchd, root)),
        );
        let mut windows = spec(Platform::WindowsTask, Path::new(r"C:\Users\dev"));
        windows.executable = PathBuf::from(r"C:\Program Files\Waku\waku-daemon.exe");
        windows.launcher = PathBuf::from(r"C:\Users\dev\.waku-fork\bin\waku-daemon-service.cmd");
        windows.log = PathBuf::from(r"C:\Users\dev\.waku-fork\logs\daemon.log");
        snapshot("launcher.cmd", &launcher_script(&windows));
        assert_eq!(
            describe(&install_plan(&windows))
                .into_iter()
                .filter(|line| line.starts_with("run "))
                .collect::<Vec<_>>(),
            [
                r#"run schtasks /Create /TN io.github.countradooku.waku.daemon /TR "C:\Users\dev\.waku-fork\bin\waku-daemon-service.cmd" /SC ONLOGON /RL LIMITED /F"#,
                "run schtasks /Run /TN io.github.countradooku.waku.daemon",
            ]
        );
    }

    #[test]
    fn paths_are_escaped_in_definitions() {
        let mut odd = spec(Platform::Launchd, Path::new("/Users/a&b"));
        odd.executable = PathBuf::from("/opt/my \"waku\"/$bin");
        assert!(launchd_plist(&odd).contains("<string>/Users/a&amp;b</string>"));
        assert!(launcher_script(&odd).contains(r#"exec "/opt/my \"waku\"/\$bin" --service "$@""#));
    }

    #[test]
    fn install_writes_the_launcher_and_agent_then_bootstraps_it() {
        let root = tempfile::tempdir().unwrap();
        let spec = spec(Platform::Launchd, root.path());
        let runner = FakeRunner {
            answers: vec![(
                "launchctl bootout",
                CommandOutput {
                    success: false,
                    stderr: "Boot-out failed: 3: No such process".into(),
                    ..CommandOutput::default()
                },
            )],
            ..FakeRunner::default()
        };
        apply(&install_plan(&spec), &runner).unwrap();
        let plist = spec.definition_path();
        assert!(plist.ends_with("Library/LaunchAgents/io.github.countradooku.waku.daemon.plist"));
        assert_eq!(
            std::fs::read_to_string(&plist).unwrap(),
            launchd_plist(&spec)
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&spec.launcher)
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o755);
        }
        assert!(spec.log.parent().unwrap().is_dir());
        assert_eq!(
            *runner.calls.borrow(),
            [
                "launchctl bootout gui/501/io.github.countradooku.waku.daemon".to_owned(),
                format!("launchctl bootstrap gui/501 {}", plist.display()),
            ]
        );

        // Status reads launchctl's answer.
        let running = FakeRunner {
            answers: vec![(
                "launchctl print",
                CommandOutput {
                    success: true,
                    stdout: "gui/501/io.github.countradooku.waku.daemon = {\n\tstate = running\n\tpid = 4242\n}\n".into(),
                    ..CommandOutput::default()
                },
            )],
            ..FakeRunner::default()
        };
        assert_eq!(
            status(&spec, &running),
            ServiceState::Running { pid: Some(4242) }
        );

        // Uninstall boots it out and removes both files.
        let runner = FakeRunner::default();
        apply(&uninstall_plan(&spec), &runner).unwrap();
        assert!(!plist.exists() && !spec.launcher.exists());
        assert_eq!(status(&spec, &runner), ServiceState::NotInstalled);
    }

    #[test]
    fn systemd_install_enables_the_unit_and_status_reads_it() {
        let root = tempfile::tempdir().unwrap();
        let spec = spec(Platform::Systemd, root.path());
        let runner = FakeRunner::default();
        apply(&install_plan(&spec), &runner).unwrap();
        assert!(
            spec.definition_path()
                .ends_with(".config/systemd/user/io-github-countradooku-waku-daemon.service")
        );
        assert_eq!(
            *runner.calls.borrow(),
            [
                "systemctl --user daemon-reload",
                "systemctl --user enable --now io-github-countradooku-waku-daemon.service",
            ]
        );
        let active = FakeRunner {
            answers: vec![
                (
                    "systemctl --user is-active",
                    CommandOutput {
                        success: true,
                        stdout: "active\n".into(),
                        ..CommandOutput::default()
                    },
                ),
                (
                    "systemctl --user show",
                    CommandOutput {
                        success: true,
                        stdout: "777\n".into(),
                        ..CommandOutput::default()
                    },
                ),
            ],
            ..FakeRunner::default()
        };
        assert_eq!(
            status(&spec, &active),
            ServiceState::Running { pid: Some(777) }
        );
        let inactive = FakeRunner {
            answers: vec![(
                "systemctl --user is-active",
                CommandOutput {
                    success: false,
                    stdout: "inactive\n".into(),
                    ..CommandOutput::default()
                },
            )],
            ..FakeRunner::default()
        };
        assert_eq!(status(&spec, &inactive), ServiceState::Stopped);
    }

    #[test]
    fn a_dry_run_describes_and_changes_nothing() {
        let root = tempfile::tempdir().unwrap();
        let spec = spec(Platform::Systemd, root.path());
        let lines = describe(&install_plan(&spec));
        assert!(lines.iter().any(|line| line.starts_with("write ")
            && line.contains("waku-daemon-service")
            && line.ends_with("executable)")));
        assert!(
            lines
                .iter()
                .any(|line| line
                    .ends_with("enable --now io-github-countradooku-waku-daemon.service"))
        );
        assert!(!spec.launcher.exists());
        assert!(!spec.definition_path().exists());
    }

    #[test]
    fn a_failing_required_command_fails_the_install() {
        let root = tempfile::tempdir().unwrap();
        let spec = spec(Platform::Systemd, root.path());
        let runner = FakeRunner {
            answers: vec![(
                "systemctl --user enable",
                CommandOutput {
                    success: false,
                    stderr: "Failed to connect to bus".into(),
                    ..CommandOutput::default()
                },
            )],
            ..FakeRunner::default()
        };
        let error = apply(&install_plan(&spec), &runner).unwrap_err();
        assert!(
            error.to_string().contains("Failed to connect to bus"),
            "{error}"
        );
    }

    #[test]
    fn windows_status_reads_the_task_list() {
        let root = tempfile::tempdir().unwrap();
        let spec = spec(Platform::WindowsTask, root.path());
        let missing = FakeRunner {
            answers: vec![(
                "schtasks /Query",
                CommandOutput {
                    success: false,
                    ..CommandOutput::default()
                },
            )],
            ..FakeRunner::default()
        };
        assert_eq!(status(&spec, &missing), ServiceState::NotInstalled);
        let running = FakeRunner {
            answers: vec![(
                "schtasks /Query",
                CommandOutput {
                    success: true,
                    stdout: "TaskName:      \\io.github.countradooku.waku.daemon\nStatus:        Running\n".into(),
                    ..CommandOutput::default()
                },
            )],
            ..FakeRunner::default()
        };
        assert_eq!(status(&spec, &running), ServiceState::Running { pid: None });
    }
}
