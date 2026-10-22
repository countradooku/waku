//! Computer Use for Codex sessions (debug builds): Waku's QuickJS MCP
//! server replaces Codex's bundled computer-use plugin, MCP server, skill
//! and Node REPL for the app-server it configures.

use std::fs;
use std::path::PathBuf;

use crate::computer_use;
use crate::computer_use::ComputerToolRequest;
use crate::driver::computer_use as computer_use_runtime;
use crate::driver::v2::ComputerUseControl;

const DISABLE_EXTERNAL_COMPUTER_USE_PLUGIN: &str =
    "plugins.computer-use@openai-bundled.enabled=false";
// Codex 0.146 only resolves plugin enablement from user/profile config layers, so a
// process-local `-c` plugin override does not yet suppress its bundled capabilities.
const DISABLE_EXTERNAL_COMPUTER_USE_MCP_COMMAND: &str =
    "mcp_servers.computer-use.command=\"/usr/bin/true\"";
const DISABLE_EXTERNAL_COMPUTER_USE_MCP: &str = "mcp_servers.computer-use.enabled=false";
const DISABLE_EXTERNAL_COMPUTER_USE_SKILL: &str =
    r#"skills.config=[{name="computer-use:computer-use",enabled=false}]"#;
// Codex validates MCP transport fields even for disabled servers, so the
// override needs a valid stdio command before disabling the bundled server.
const DISABLE_CODEX_NODE_REPL_COMMAND: &str = "mcp_servers.node_repl.command=\"/usr/bin/true\"";
const DISABLE_CODEX_NODE_REPL: &str = "mcp_servers.node_repl.enabled=false";

fn toml_string(value: &str) -> String {
    serde_json::to_string(value).expect("a filesystem path is always valid JSON")
}

/// Computer Use (debug builds): Waku's long-lived QuickJS MCP server with the
/// raw native helper kept private behind its built-in `cua` object, so Codex
/// sees only the compact `js` / `js_reset` surface.
pub(super) struct ComputerUse {
    server_path: PathBuf,
    server: String,
    repl: String,
    skill_root: PathBuf,
    process_directory: PathBuf,
    process_directory_config: String,
}

impl ComputerUse {
    pub(super) fn load() -> anyhow::Result<Self> {
        let server_path = computer_use::mcp_server_command()?;
        let server = toml_string(&server_path.display().to_string());
        let repl_path = computer_use::js_repl_server_path()?;
        let repl = toml_string(&repl_path.display().to_string());
        let skill_root = computer_use::skill_root_path()?;
        let process_directory = computer_use_runtime::create_process_directory()?;
        let process_directory_config = toml_string(&process_directory.display().to_string());
        Ok(Self {
            server_path,
            server,
            repl,
            skill_root,
            process_directory,
            process_directory_config,
        })
    }

    pub(super) fn skill_root(&self) -> String {
        self.skill_root.display().to_string()
    }

    /// The `-c` overrides and environment the app-server starts with.
    pub(super) fn configure(&self, args: &mut Vec<String>, env: &mut Vec<(String, String)>) {
        for setting in [
            DISABLE_EXTERNAL_COMPUTER_USE_PLUGIN.to_owned(),
            DISABLE_EXTERNAL_COMPUTER_USE_MCP_COMMAND.to_owned(),
            DISABLE_EXTERNAL_COMPUTER_USE_MCP.to_owned(),
            DISABLE_EXTERNAL_COMPUTER_USE_SKILL.to_owned(),
            DISABLE_CODEX_NODE_REPL_COMMAND.to_owned(),
            DISABLE_CODEX_NODE_REPL.to_owned(),
            format!("mcp_servers.waku_js_repl.command={}", self.repl),
            "mcp_servers.waku_js_repl.args=[]".to_owned(),
            format!(
                "mcp_servers.waku_js_repl.env.WAKU_COMPUTER_USE_SERVER={}",
                self.server
            ),
            format!(
                "mcp_servers.waku_js_repl.env.WAKU_COMPUTER_USE_PROCESS_DIRECTORY={}",
                self.process_directory_config
            ),
        ] {
            args.push("-c".to_owned());
            args.push(setting);
        }
        env.push((
            "WAKU_COMPUTER_USE_SERVER".to_owned(),
            self.server_path.display().to_string(),
        ));
        env.push((
            "WAKU_COMPUTER_USE_PROCESS_DIRECTORY".to_owned(),
            self.process_directory.display().to_string(),
        ));
    }
}

impl ComputerUseControl for ComputerUse {
    fn run_computer_tool(&self, _request: ComputerToolRequest) {}

    fn reject_computer_tool(&self, _request: ComputerToolRequest, _reason: String) {}

    fn cancel_computer_use(&self) {
        computer_use_runtime::stop_registered_processes(&self.process_directory, &self.server_path);
    }
}

impl Drop for ComputerUse {
    fn drop(&mut self) {
        self.cancel_computer_use();
        let _ = fs::remove_dir_all(&self.process_directory);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computer_use_command_configuration_follows_the_setting() {
        let config = ComputerUse {
            server_path: PathBuf::from("/tmp/waku-computer-use-server"),
            server: toml_string("/tmp/waku-computer-use-server"),
            repl: toml_string("/tmp/waku"),
            skill_root: PathBuf::from("/tmp/waku-computer-use-skill"),
            process_directory: PathBuf::from("/tmp/waku-computer-use-processes-test"),
            process_directory_config: toml_string("/tmp/waku-computer-use-processes-test"),
        };
        let (mut args, mut env) = (Vec::new(), Vec::new());
        config.configure(&mut args, &mut env);
        // The raw helper must never be registered as a Codex MCP server: the
        // Waku REPL owns it and exposes only `cua` inside JavaScript.
        assert!(
            !args
                .iter()
                .any(|argument| argument.contains("mcp_servers.waku_computer_use"))
        );
        assert!(
            args.iter()
                .any(|argument| argument.contains("mcp_servers.waku_js_repl.command"))
        );
        assert!(
            args.iter()
                .any(|argument| argument == "mcp_servers.waku_js_repl.args=[]")
        );
        for setting in [
            DISABLE_EXTERNAL_COMPUTER_USE_PLUGIN,
            DISABLE_EXTERNAL_COMPUTER_USE_MCP,
            DISABLE_EXTERNAL_COMPUTER_USE_SKILL,
            DISABLE_CODEX_NODE_REPL,
            DISABLE_CODEX_NODE_REPL_COMMAND,
        ] {
            assert!(args.iter().any(|argument| argument == setting), "{setting}");
        }
        assert!(
            env.iter()
                .any(|(name, _)| name == "WAKU_COMPUTER_USE_SERVER")
        );
        std::mem::forget(config);
    }

    #[test]
    fn computer_use_process_registry_accepts_only_pid_files() {
        let directory = std::env::temp_dir().join(format!(
            "waku-computer-use-process-test-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(directory.join("456")).unwrap();
        fs::write(directory.join("123"), []).unwrap();
        fs::write(directory.join("not-a-pid"), []).unwrap();

        let processes = computer_use_runtime::registered_processes(&directory);
        assert_eq!(processes.len(), 1);
        assert_eq!(processes[0].0, 123);
        assert_eq!(processes[0].1, directory.join("123"));

        fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn computer_use_cleanup_verifies_the_registered_executable() {
        let current = fs::canonicalize(std::env::current_exe().unwrap()).unwrap();
        assert_eq!(
            computer_use_runtime::process_executable(std::process::id() as i32),
            Some(current)
        );
    }
}
