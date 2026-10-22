//! Resident `dsh web` Host RPC process and its loopback transport.
//!
//! DeepSeek Harness exposes client-grade unary RPC over HTTP and two
//! downlink-only WebSockets. One process hosts every Harness session; callers
//! share it through `deepseek_pool`, while this module owns startup, blocking
//! typed RPC (session listing and history import) and process-tree teardown.
//! Live sessions talk to the Host through the async driver
//! (`driver::deepseek`), which opens its own downlinks.

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context as _, anyhow, bail};
use parking_lot::Mutex;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::model::{ProviderResumeCursor, ProviderSessionSummary};

const SERVER_START_TIMEOUT: Duration = Duration::from_secs(20);
const RPC_TIMEOUT: Duration = Duration::from_secs(30);

fn provider_timestamp(value: &Value) -> u64 {
    value
        .as_u64()
        .map(|value| {
            if value > 100_000_000_000 {
                value / 1000
            } else {
                value
            }
        })
        .unwrap_or_default()
}

fn parse_provider_summaries(value: &Value, limit: usize) -> Vec<ProviderSessionSummary> {
    value
        .get("items")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|item| {
            item.get("blank").and_then(Value::as_bool) != Some(true)
                && item.get("parentSessionId").is_none_or(Value::is_null)
                && item.get("origin").and_then(Value::as_str) != Some("subagent")
        })
        .filter_map(|item| {
            let session_id = item.get("sessionId").and_then(Value::as_str)?.trim();
            if session_id.is_empty() {
                return None;
            }
            let cwd = std::path::PathBuf::from(item.get("cwd").and_then(Value::as_str)?);
            if !cwd.is_absolute() {
                return None;
            }
            let updated_at = provider_timestamp(item.get("updatedAt").unwrap_or(&Value::Null));
            let title = item
                .pointer("/projections/values/title")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|title| !title.is_empty())
                .map(str::to_owned)
                .or_else(|| {
                    cwd.file_name()
                        .and_then(|name| name.to_str())
                        .filter(|name| !name.is_empty())
                        .map(str::to_owned)
                })
                .unwrap_or_else(|| {
                    format!(
                        "DeepSeek session {}",
                        session_id.chars().take(8).collect::<String>()
                    )
                });
            Some(ProviderSessionSummary {
                cursor: ProviderResumeCursor::DeepSeek {
                    session_id: session_id.to_owned(),
                },
                title,
                cwd,
                created_at: updated_at,
                updated_at,
            })
        })
        .take(limit)
        .collect()
}

pub fn list_provider_sessions(
    binary: &Path,
    limit: usize,
) -> anyhow::Result<Vec<ProviderSessionSummary>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let server = crate::deepseek_pool::acquire(binary)?;
    let value = server.rpc("session.list", json!({}))?;
    Ok(parse_provider_summaries(&value, limit))
}

// The development watcher terminates the app with SIGTERM, which does not run
// Rust destructors. Keep one pipe open in Waku and let this wrapper terminate
// the resident Host when that pipe closes, so a rebuild cannot orphan `dsh
// web`. The second watcher makes a spontaneous Host exit observable through
// the wrapper Child as well as through the ordinary process monitor.
#[cfg(unix)]
const DSH_GUARDIAN_SCRIPT: &str = r#"
parent=$$
child=
watcher=
cleanup() {
  trap - EXIT HUP INT TERM
  if [ -n "$watcher" ]; then kill -TERM "$watcher" 2>/dev/null || true; fi
  if [ -n "$child" ]; then
    kill -TERM "$child" 2>/dev/null || true
    wait "$child" 2>/dev/null || true
  fi
}
trap cleanup EXIT HUP INT TERM
"$@" </dev/null &
child=$!
(
  while kill -0 "$child" 2>/dev/null; do sleep 1; done
  kill -TERM "$parent" 2>/dev/null || true
) &
watcher=$!
while IFS= read -r _; do :; done
"#;

#[cfg(unix)]
use std::os::unix::process::CommandExt as _;

pub(crate) struct DeepSeekServer {
    child: Arc<Mutex<Child>>,
    pub(crate) port: u16,
}

impl DeepSeekServer {
    pub(crate) fn start(binary: &Path) -> anyhow::Result<Self> {
        Self::start_with_dsh_home(binary, None)
    }

    fn start_with_dsh_home(binary: &Path, dsh_home: Option<&Path>) -> anyhow::Result<Self> {
        let supports_no_open = web_supports_no_open(binary, dsh_home);
        let catalog_cwd = crate::acp_session::catalog_working_directory()?;
        #[cfg(unix)]
        let mut command = {
            let mut command = crate::command_env::command("/bin/sh");
            command
                .args(["-c", DSH_GUARDIAN_SCRIPT, "waku-dsh-guardian"])
                .arg(binary)
                .args(["web", "--host", "127.0.0.1", "--port", "0"]);
            if supports_no_open {
                command.arg("--no-open");
            }
            command
        };
        #[cfg(not(unix))]
        let mut command = {
            let mut command = crate::command_env::command(binary);
            command.args(["web", "--host", "127.0.0.1", "--port", "0"]);
            if supports_no_open {
                command.arg("--no-open");
            }
            command
        };
        command
            .current_dir(&catalog_cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(dsh_home) = dsh_home {
            command.env("DSH_HOME", dsh_home);
        }
        #[cfg(unix)]
        command.process_group(0);

        let mut child = crate::command_env::spawn(&mut command)
            .context("failed to start `dsh web --host 127.0.0.1 --port 0`")?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("DeepSeek Harness provided no startup output"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow!("DeepSeek Harness provided no diagnostic output"))?;
        let (stdout_tx, stdout_rx) = std::sync::mpsc::channel();
        let (stderr_tx, stderr_rx) = std::sync::mpsc::channel();
        thread::Builder::new()
            .name("waku-deepseek-stdout".into())
            .spawn(move || {
                for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                    let _ = stdout_tx.send(line);
                }
            })?;
        thread::Builder::new()
            .name("waku-deepseek-stderr".into())
            .spawn(move || {
                for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                    let _ = stderr_tx.send(line);
                }
            })?;

        let started_at = Instant::now();
        let mut diagnostics = Vec::new();
        let port = 'startup: loop {
            while let Ok(line) = stdout_rx.try_recv() {
                if let Some(port) = parse_ready_port(&line) {
                    break 'startup port;
                }
                if !line.trim().is_empty() {
                    diagnostics.push(line);
                }
            }
            while let Ok(line) = stderr_rx.try_recv() {
                if !line.trim().is_empty() {
                    diagnostics.push(line);
                }
            }
            if let Some(status) = child.try_wait()? {
                let detail = diagnostics.join("\n");
                bail!(
                    "DeepSeek Harness exited during startup ({status}){}",
                    if detail.is_empty() {
                        String::new()
                    } else {
                        format!(": {detail}")
                    }
                );
            }
            if started_at.elapsed() >= SERVER_START_TIMEOUT {
                terminate_child(&mut child, Duration::from_secs(2));
                let detail = diagnostics.join("\n");
                bail!(
                    "timed out starting DeepSeek Harness{}",
                    if detail.is_empty() {
                        String::new()
                    } else {
                        format!(": {detail}")
                    }
                );
            }
            thread::sleep(Duration::from_millis(20));
        };

        let child = Arc::new(Mutex::new(child));
        Ok(Self { child, port })
    }

    pub(crate) fn rpc(&self, method: &str, payload: Value) -> anyhow::Result<Value> {
        let rpc_id = format!("waku-{}", Uuid::new_v4());
        let body = json!({
            "type": "client-request",
            "rpcId": rpc_id,
            "method": method,
            "payload": payload,
        });
        let response = crate::http_wire::request_json(
            &crate::http_wire::Endpoint::local(self.port),
            "POST",
            &format!("/api/{method}"),
            Some(&body),
            RPC_TIMEOUT,
        )?;
        if response.get("type").and_then(Value::as_str) != Some("server-response")
            || response.get("rpcId").and_then(Value::as_str) != Some(rpc_id.as_str())
        {
            bail!("DeepSeek Harness returned an invalid {method} response");
        }
        rpc_result_value(method, &response)
    }

    pub(crate) fn is_alive(&self) -> bool {
        self.child
            .lock()
            .try_wait()
            .is_ok_and(|status| status.is_none())
    }

    pub(crate) fn shutdown(&self, timeout: Duration) {
        terminate_child(&mut self.child.lock(), timeout);
    }
}

impl Drop for DeepSeekServer {
    fn drop(&mut self) {
        self.shutdown(Duration::from_secs(5));
    }
}

fn rpc_result_value(method: &str, response: &Value) -> anyhow::Result<Value> {
    let result = response
        .get("result")
        .ok_or_else(|| anyhow!("DeepSeek Harness returned no {method} result"))?;
    if result.get("ok").and_then(Value::as_bool) == Some(true) {
        return Ok(result.get("value").cloned().unwrap_or(Value::Null));
    }
    let error = result.get("error").unwrap_or(&Value::Null);
    let code = error
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("unknown error");
    bail!("DeepSeek Harness {method} failed ({code}): {message}")
}

fn parse_ready_port(line: &str) -> Option<u16> {
    let url = line.trim().strip_prefix("dsh web: ")?;
    let url = url::Url::parse(url).ok()?;
    (url.scheme() == "http" && url.host_str() == Some("127.0.0.1"))
        .then(|| url.port())
        .flatten()
}

fn web_supports_no_open(binary: &Path, dsh_home: Option<&Path>) -> bool {
    let mut command = crate::command_env::command(binary);
    command.args(["web", "--help"]);
    if let Ok(cwd) = crate::acp_session::catalog_working_directory() {
        command.current_dir(cwd);
    }
    if let Some(dsh_home) = dsh_home {
        command.env("DSH_HOME", dsh_home);
    }
    crate::command_env::output(&mut command)
        .ok()
        .is_some_and(|output| web_help_supports_no_open(&output.stdout))
}

fn web_help_supports_no_open(output: &[u8]) -> bool {
    String::from_utf8_lossy(output)
        .lines()
        .any(|line| line.contains("--no-open"))
}

fn terminate_child(child: &mut Child, timeout: Duration) {
    if child.try_wait().is_ok_and(|status| status.is_some()) {
        return;
    }
    #[cfg(unix)]
    unsafe {
        let _ = libc::kill(-(child.id() as libc::pid_t), libc::SIGTERM);
    }
    #[cfg(not(unix))]
    let _ = child.kill();

    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => thread::sleep(Duration::from_millis(20)),
            Err(_) => break,
        }
    }
    #[cfg(unix)]
    unsafe {
        let _ = libc::kill(-(child.id() as libc::pid_t), libc::SIGKILL);
    }
    let _ = child.kill();
    let _ = child.wait();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_line_accepts_only_the_private_loopback_url() {
        assert_eq!(
            parse_ready_port("dsh web: http://127.0.0.1:59258"),
            Some(59258)
        );
        assert_eq!(parse_ready_port("dsh web: http://localhost:59258"), None);
        assert_eq!(parse_ready_port("listening on 59258"), None);
    }

    #[test]
    fn web_no_open_flag_is_capability_gated() {
        assert!(web_help_supports_no_open(
            b"  --no-open  do not open the Web UI in the default browser\n"
        ));
        assert!(!web_help_supports_no_open(
            b"  --port <port>  listen port\n"
        ));
    }

    /// Exercises Waku's HTTP envelope, WebSocket handshakes, and process-tree
    /// lifecycle against the locally installed Harness without creating a
    /// session or making a model request.
    #[test]
    #[ignore = "requires an installed DeepSeek Harness"]
    fn installed_harness_host_answers_rpc() {
        let binary =
            crate::command_env::find_executable("dsh").expect("DeepSeek Harness is not installed");
        let server = DeepSeekServer::start(&binary).expect("Harness Host should start");
        let listed = server
            .rpc("session.list", json!({}))
            .expect("Harness Host should answer a typed RPC");
        assert!(listed.get("items").and_then(Value::as_array).is_some());
        #[cfg(unix)]
        {
            // Simulate an abruptly terminated Waku process: the OS closes its
            // pipe without giving DeepSeekServer a chance to run Drop.
            drop(server.child.lock().stdin.take());
            let deadline = Instant::now() + Duration::from_secs(5);
            while server.is_alive() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(20));
            }
            assert!(
                !server.is_alive(),
                "the Harness guardian should stop after its parent pipe closes"
            );
        }
        server.shutdown(Duration::from_secs(5));
    }

    /// Probes the session payloads used by the custom driver in an isolated
    /// Harness store. Commands are local configuration operations, so this
    /// needs neither credentials nor a model request.
    #[test]
    #[ignore = "requires an installed DeepSeek Harness"]
    fn installed_harness_session_contracts_round_trip() {
        struct TempHarnessHome(std::path::PathBuf);

        impl Drop for TempHarnessHome {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }

        let binary =
            crate::command_env::find_executable("dsh").expect("DeepSeek Harness is not installed");
        let root =
            TempHarnessHome(std::env::temp_dir().join(format!("waku-dsh-test-{}", Uuid::new_v4())));
        std::fs::create_dir_all(&root.0).unwrap();
        {
            let server = DeepSeekServer::start_with_dsh_home(&binary, Some(&root.0))
                .expect("Harness Host should start");
            let session_id = format!("waku-test-{}", Uuid::new_v4());
            let created = server
                .rpc(
                    "session.create",
                    json!({"cwd": root.0.to_string_lossy(), "sessionId": session_id}),
                )
                .expect("session.create should accept Waku's payload");
            assert_eq!(
                created.get("sessionId").and_then(Value::as_str),
                Some(session_id.as_str())
            );
            let history = server
                .rpc(
                    "session.history",
                    json!({"sessionId": session_id, "maxMessages": 200}),
                )
                .expect("session.history should accept Waku's payload");
            assert!(history.get("events").and_then(Value::as_array).is_some());
            assert!(
                server
                    .rpc("session.models", json!({"sessionId": session_id}))
                    .expect("session.models should return the session catalog")
                    .get("current")
                    .is_some()
            );
            let commands = server
                .rpc("commands/list", json!({"args": {"agentId": session_id}}))
                .expect("commands/list should return native commands");
            assert!(commands.as_array().is_some_and(|commands| {
                commands
                    .iter()
                    .any(|command| command.get("name").and_then(Value::as_str) == Some("plan"))
            }));
            for command in ["/permission workspace-write", "/plan off"] {
                let response = server
                    .rpc(
                        "commands/execute",
                        json!({"args": {"agentId": session_id, "line": command, "images": []}}),
                    )
                    .expect("native configuration command should succeed");
                assert_eq!(
                    response.pointer("/result/kind").and_then(Value::as_str),
                    Some("success")
                );
            }
            server.shutdown(Duration::from_secs(5));
        }
    }
}
