//! `TitlePort`: one-shot thread titles from the provider.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use waku_orchestrator::{PortError, TitlePort};
use waku_protocol::orchestration::{DriverKind, ModelSelection};

/// The most prompt characters sent for a title.
pub const MAX_PROMPT_CHARS: usize = 4_000;
/// The longest title kept (the orchestrator caps at 120).
pub const MAX_TITLE_CHARS: usize = 80;

/// Generates a title for one prompt. `Ok(None)` means "no title" (the
/// provider is not installed, or answered nothing); an error is retried.
pub trait TitleBackend: Send + Sync {
    fn generate(&self, model: Option<&str>, prompt: &str) -> anyhow::Result<Option<String>>;
}

/// Codex's app-server, as the Codex driver already uses it for titles.
pub struct CodexTitleBackend {
    binary: Arc<dyn Fn() -> Option<PathBuf> + Send + Sync>,
    cwd: PathBuf,
}

impl CodexTitleBackend {
    /// Finds `codex` on the search path at each call; runs in `cwd` (any
    /// directory works; the title thread is ephemeral).
    pub fn new(cwd: PathBuf) -> Self {
        Self::with_binary(cwd, || crate::command_env::find_executable("codex"))
    }

    pub fn with_binary(
        cwd: PathBuf,
        binary: impl Fn() -> Option<PathBuf> + Send + Sync + 'static,
    ) -> Self {
        Self {
            binary: Arc::new(binary),
            cwd,
        }
    }
}

impl TitleBackend for CodexTitleBackend {
    fn generate(&self, _model: Option<&str>, prompt: &str) -> anyhow::Result<Option<String>> {
        let Some(binary) = (self.binary)() else {
            return Ok(None);
        };
        crate::driver::generate_codex_title(&binary, &self.cwd, prompt).map(Some)
    }
}

/// Routes title generation by the thread's driver, with an optional
/// fallback backend for drivers without one. Drivers whose provider names
/// threads itself (Claude, OpenCode, Amp, …) report titles through their
/// native-title refresh instead; for them this answers `None`.
#[derive(Clone, Default)]
pub struct ProviderTitlePort {
    backends: HashMap<DriverKind, Arc<dyn TitleBackend>>,
    fallback: Option<Arc<dyn TitleBackend>>,
}

impl ProviderTitlePort {
    pub fn new() -> Self {
        Self::default()
    }

    /// Codex generates titles for Codex threads.
    pub fn with_codex(cwd: PathBuf) -> Self {
        Self::new().route(DriverKind::Codex, Arc::new(CodexTitleBackend::new(cwd)))
    }

    pub fn route(mut self, driver: DriverKind, backend: Arc<dyn TitleBackend>) -> Self {
        self.backends.insert(driver, backend);
        self
    }

    pub fn fallback(mut self, backend: Arc<dyn TitleBackend>) -> Self {
        self.fallback = Some(backend);
        self
    }
}

/// The first non-empty line, trimmed of quotes and a trailing period, at
/// most [`MAX_TITLE_CHARS`] characters.
pub fn normalize_title(raw: &str) -> Option<String> {
    let line = raw.lines().map(str::trim).find(|line| !line.is_empty())?;
    let line = line
        .trim_start_matches(['"', '\'', '“', '‘', '`', '#', ' '])
        .trim_end_matches(['"', '\'', '”', '’', '`', '.', ' ']);
    let title = line.chars().take(MAX_TITLE_CHARS).collect::<String>();
    let title = title.trim().to_owned();
    (!title.is_empty()).then_some(title)
}

impl TitlePort for ProviderTitlePort {
    fn generate(
        &self,
        selection: &ModelSelection,
        prompt: &str,
    ) -> Result<Option<String>, PortError> {
        let driver = DriverKind::from_instance(&selection.instance);
        let Some(backend) = self.backends.get(&driver).or(self.fallback.as_ref()) else {
            return Ok(None);
        };
        let prompt = prompt.trim();
        if prompt.is_empty() {
            return Ok(None);
        }
        let prompt = match prompt.char_indices().nth(MAX_PROMPT_CHARS) {
            Some((end, _)) => &prompt[..end],
            None => prompt,
        };
        backend
            .generate(selection.model.as_deref(), prompt)
            .map(|title| title.as_deref().and_then(normalize_title))
            .map_err(|error| PortError::retryable(format!("{error:#}")))
    }
}
