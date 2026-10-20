//! The recorder's sanitizer (RFC 0002 "Sanitization"). Applied in order
//! before a fixture is written, and failing closed afterwards: if the
//! written file still contains the real home directory, the user name or a
//! deny-listed secret, the file is deleted and recording fails.

use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};

use regex::Regex;
use serde_json::{Map, Value};

use super::ReplayEntry;

/// Environment variables whose values must never reach a fixture.
pub const DENY_LIST: [&str; 8] = [
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "XAI_API_KEY",
    "DEEPSEEK_API_KEY",
    "OPENROUTER_API_KEY",
    "GITHUB_TOKEN",
    "GH_TOKEN",
];

pub const REDACTED: &str = "<redacted>";
pub const REPLAY_HOME: &str = "/home/replay-user";
pub const REPLAY_HOST: &str = "replay-host";
pub const REPLAY_EMAIL: &str = "user@example.invalid";
pub const INSTALLATION_ID: &str = "00000000-0000-4000-8000-000000000000";
/// Single strings over this size are truncated (unless the scenario streams
/// output).
pub const MAX_STRING_BYTES: usize = 64 * 1024;

/// What the fail-closed scan found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SanitizeError {
    /// The written file still contains this kind of secret; it was deleted.
    Leak {
        kind: String,
        path: PathBuf,
    },
    Io(String),
}

impl fmt::Display for SanitizeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Leak { kind, path } => write!(
                formatter,
                "{} still contains {kind}; the fixture was deleted",
                path.display()
            ),
            Self::Io(message) => write!(formatter, "sanitizer i/o: {message}"),
        }
    }
}

impl std::error::Error for SanitizeError {}

/// The sanitizer's inputs. `from_env` reads the real machine; tests build
/// one by hand.
#[derive(Clone, Debug)]
pub struct Sanitizer {
    pub workspace: Option<String>,
    pub provider_home: Option<String>,
    pub home: Option<String>,
    pub user: Option<String>,
    pub hostname: Option<String>,
    /// Values of deny-listed environment variables.
    pub secrets: Vec<(String, String)>,
    pub renumber_ids: bool,
    pub keep_long_strings: bool,
    /// Keys whose values are installation or telemetry ids.
    pub installation_keys: Vec<String>,
}

impl Sanitizer {
    pub fn from_env(workspace: &Path, provider_home: Option<&Path>) -> Self {
        let secrets = DENY_LIST
            .iter()
            .filter_map(|name| {
                let value = std::env::var(name).ok()?;
                (!value.trim().is_empty()).then(|| ((*name).to_owned(), value))
            })
            .collect();
        Self {
            workspace: Some(workspace.display().to_string()),
            provider_home: provider_home.map(|home| home.display().to_string()),
            home: dirs::home_dir().map(|home| home.display().to_string()),
            user: std::env::var("USER")
                .or_else(|_| std::env::var("USERNAME"))
                .ok()
                .filter(|user| user.len() >= 3),
            hostname: std::env::var("HOSTNAME")
                .ok()
                .filter(|host| !host.is_empty()),
            secrets,
            renumber_ids: false,
            keep_long_strings: false,
            installation_keys: vec![
                "installationId".to_owned(),
                "installation_id".to_owned(),
                "telemetryId".to_owned(),
                "deviceId".to_owned(),
            ],
        }
    }

    /// Sanitizes every frame of a transcript, consistently across channels.
    pub fn sanitize(&self, entries: &mut [ReplayEntry]) {
        let mut state = Pass::new(self);
        for entry in entries.iter_mut() {
            match entry {
                ReplayEntry::ExpectOutbound { frame, .. }
                | ReplayEntry::EmitInbound { frame, .. } => {
                    *frame = state.value(frame, None);
                }
                ReplayEntry::RuntimeExit { error, .. } => {
                    if let Some(error) = error {
                        *error = state.value(error, None);
                    }
                }
            }
        }
    }

    /// The fail-closed scan of a written file. On a hit the file is deleted.
    pub fn verify_file(&self, path: &Path) -> Result<(), SanitizeError> {
        let text =
            std::fs::read_to_string(path).map_err(|error| SanitizeError::Io(error.to_string()))?;
        if let Some(kind) = self.leak(&text) {
            let _ = std::fs::remove_file(path);
            return Err(SanitizeError::Leak {
                kind,
                path: path.to_owned(),
            });
        }
        Ok(())
    }

    /// What kind of secret `text` still contains, if any.
    pub fn leak(&self, text: &str) -> Option<String> {
        if let Some(home) = self.home.as_deref().filter(|home| home.len() > 1)
            && text.contains(home)
        {
            return Some("the home directory".to_owned());
        }
        if let Some(user) = &self.user
            && Regex::new(&format!(r"\b{}\b", regex::escape(user)))
                .ok()
                .is_some_and(|pattern| pattern.is_match(text))
        {
            return Some("the user name".to_owned());
        }
        for (name, value) in &self.secrets {
            if text.contains(value.as_str()) {
                return Some(format!("the value of {name}"));
            }
        }
        None
    }
}

/// One sanitizing pass; keeps UUID renumbering consistent across the file.
struct Pass<'a> {
    rules: &'a Sanitizer,
    secret_key: Regex,
    secret_shapes: Vec<Regex>,
    email: Regex,
    uuid: Regex,
    uuids: HashMap<String, String>,
    native_ids: Regex,
    natives: HashMap<String, String>,
}

impl<'a> Pass<'a> {
    fn new(rules: &'a Sanitizer) -> Self {
        Self {
            rules,
            secret_key: Regex::new(
                r"(?i)token|secret|password|api[_-]?key|authorization|cookie|bearer|refresh",
            )
            .expect("valid regex"),
            secret_shapes: [
                r"sk-[A-Za-z0-9_-]{20,}",
                r"gh[pousr]_[A-Za-z0-9]{20,}",
                r"xox[abpr]-[A-Za-z0-9-]+",
                r"AKIA[0-9A-Z]{16}",
                r"eyJ[\w-]+\.[\w-]+\.[\w-]+",
            ]
            .into_iter()
            .map(|pattern| Regex::new(pattern).expect("valid regex"))
            .collect(),
            email: Regex::new(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
                .expect("valid regex"),
            uuid: Regex::new(
                r"(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
            )
            .expect("valid regex"),
            uuids: HashMap::new(),
            native_ids: Regex::new(r"\b(T-|ses_|msg_)[A-Za-z0-9-]+").expect("valid regex"),
            natives: HashMap::new(),
        }
    }

    fn value(&mut self, value: &Value, key: Option<&str>) -> Value {
        if let Some(key) = key {
            if self.secret_key.is_match(key) && !value.is_object() && !value.is_array() {
                return Value::String(REDACTED.to_owned());
            }
            if self
                .rules
                .installation_keys
                .iter()
                .any(|known| known == key)
                && value.is_string()
            {
                return Value::String(INSTALLATION_ID.to_owned());
            }
        }
        match value {
            Value::String(text) => Value::String(self.string(text)),
            Value::Array(items) => {
                Value::Array(items.iter().map(|item| self.value(item, None)).collect())
            }
            Value::Object(map) => {
                let mut out = Map::new();
                for (key, item) in map {
                    out.insert(key.clone(), self.value(item, Some(key)));
                }
                Value::Object(out)
            }
            other => other.clone(),
        }
    }

    fn string(&mut self, text: &str) -> String {
        let mut text = text.to_owned();
        // 1. Paths: workspace, provider home, then any remaining $HOME.
        if let Some(workspace) = self
            .rules
            .workspace
            .as_deref()
            .filter(|path| path.len() > 1)
        {
            text = text.replace(workspace, super::matcher::WORKSPACE);
            // macOS resolves `/var` to `/private/var` behind the driver's back.
            text = text.replace(&format!("/private{workspace}"), super::matcher::WORKSPACE);
        }
        if let Some(home) = self
            .rules
            .provider_home
            .as_deref()
            .filter(|path| path.len() > 1)
        {
            text = text.replace(home, super::matcher::HOME);
        }
        if let Some(home) = self.rules.home.as_deref().filter(|path| path.len() > 1) {
            text = text.replace(home, REPLAY_HOME);
        }
        // 3. Secret shapes, emails and the hostname.
        for shape in &self.secret_shapes {
            text = shape.replace_all(&text, REDACTED).into_owned();
        }
        for (_, secret) in &self.rules.secrets {
            text = text.replace(secret.as_str(), REDACTED);
        }
        text = self.email.replace_all(&text, REPLAY_EMAIL).into_owned();
        if let Some(host) = &self.rules.hostname {
            text = text.replace(host.as_str(), REPLAY_HOST);
        }
        if let Some(user) = &self.rules.user {
            text = Regex::new(&format!(r"\b{}\b", regex::escape(user)))
                .map(|pattern| pattern.replace_all(&text, "replay-user").into_owned())
                .unwrap_or(text);
        }
        // 4. UUIDs renumbered in first-seen order; other native ids only on
        // request.
        let uuids = &mut self.uuids;
        text = self
            .uuid
            .replace_all(&text, |captures: &regex::Captures<'_>| {
                let original = captures[0].to_ascii_lowercase();
                if original == INSTALLATION_ID {
                    return original;
                }
                let next = uuids.len() + 1;
                uuids
                    .entry(original)
                    .or_insert_with(|| format!("00000000-0000-4000-8000-{next:012}"))
                    .clone()
            })
            .into_owned();
        if self.rules.renumber_ids {
            let natives = &mut self.natives;
            text = self
                .native_ids
                .replace_all(&text, |captures: &regex::Captures<'_>| {
                    let next = natives.len() + 1;
                    let prefix = captures[1].to_owned();
                    natives
                        .entry(captures[0].to_owned())
                        .or_insert_with(|| format!("{prefix}{next:06}"))
                        .clone()
                })
                .into_owned();
        }
        // 7. Size.
        if !self.rules.keep_long_strings && text.len() > MAX_STRING_BYTES {
            let mut end = MAX_STRING_BYTES;
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            let dropped = text.len() - end;
            text.truncate(end);
            text.push_str(&format!("…<truncated {dropped} bytes>"));
        }
        text
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn rules() -> Sanitizer {
        Sanitizer {
            workspace: Some("/tmp/ws-123".to_owned()),
            provider_home: Some("/Users/alice/.fake".to_owned()),
            home: Some("/Users/alice".to_owned()),
            user: Some("alice".to_owned()),
            hostname: Some("alice-mbp.local".to_owned()),
            secrets: vec![("OPENAI_API_KEY".to_owned(), "opensesame-123".to_owned())],
            renumber_ids: false,
            keep_long_strings: false,
            installation_keys: vec!["installationId".to_owned()],
        }
    }

    fn sanitized(frame: Value) -> Value {
        let mut entries = vec![ReplayEntry::EmitInbound {
            label: None,
            channel: None,
            frame,
            after_ms: None,
        }];
        rules().sanitize(&mut entries);
        match &entries[0] {
            ReplayEntry::EmitInbound { frame, .. } => frame.clone(),
            _ => unreachable!(),
        }
    }

    #[test]
    fn paths_keys_shapes_and_uuids_are_rewritten() {
        let frame = sanitized(json!({
            "cwd": "/tmp/ws-123/src",
            "config": "/Users/alice/.fake/config.toml",
            "log": "/Users/alice/Library/Logs/x",
            "apiKey": "anything",
            "headers": {"Authorization": "Bearer abc"},
            "text": "use sk-abcdefghijklmnopqrstuvwxyz and mail alice@corp.example from alice-mbp.local",
            "ids": ["0F0E0D0C-0000-1111-2222-333344445555", "0f0e0d0c-0000-1111-2222-333344445555",
                    "aaaaaaaa-0000-1111-2222-333344445555"],
            "installationId": "abc",
            "env": "opensesame-123",
        }));
        assert_eq!(frame["cwd"], "<workspace>/src");
        assert_eq!(frame["config"], "<home>/config.toml");
        assert_eq!(frame["log"], "/home/replay-user/Library/Logs/x");
        assert_eq!(frame["apiKey"], REDACTED);
        assert_eq!(frame["headers"]["Authorization"], REDACTED);
        assert_eq!(
            frame["text"],
            "use <redacted> and mail user@example.invalid from replay-host"
        );
        assert_eq!(frame["ids"][0], "00000000-0000-4000-8000-000000000001");
        assert_eq!(frame["ids"][1], "00000000-0000-4000-8000-000000000001");
        assert_eq!(frame["ids"][2], "00000000-0000-4000-8000-000000000002");
        assert_eq!(frame["installationId"], INSTALLATION_ID);
        assert_eq!(frame["env"], REDACTED);
    }

    #[test]
    fn long_strings_are_truncated() {
        let frame = sanitized(json!({ "output": "x".repeat(MAX_STRING_BYTES + 10) }));
        let output = frame["output"].as_str().unwrap();
        assert!(output.ends_with("…<truncated 10 bytes>"));
    }

    #[test]
    fn the_scan_fails_closed_and_deletes_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let rules = rules();
        for (leak, kind) in [
            ("/Users/alice/secret", "the home directory"),
            ("hello alice", "the user name"),
            ("key=opensesame-123", "the value of OPENAI_API_KEY"),
        ] {
            let path = dir.path().join("fake.ndjson");
            std::fs::write(&path, format!("{{\"text\":\"{leak}\"}}\n")).unwrap();
            let error = rules.verify_file(&path).unwrap_err();
            assert_eq!(
                error,
                SanitizeError::Leak {
                    kind: kind.to_owned(),
                    path: path.clone()
                }
            );
            assert!(!path.exists(), "{kind}: the file must be deleted");
        }
        let path = dir.path().join("clean.ndjson");
        std::fs::write(&path, "{\"text\":\"<workspace>\"}\n").unwrap();
        rules.verify_file(&path).unwrap();
        assert!(path.exists());
    }
}
