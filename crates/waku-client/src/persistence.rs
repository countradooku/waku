//! Desktop-owned preferences and RPC proxies for daemon-owned task state.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Command, DaemonExposureSettings, DaemonSettings, DaemonSupervisor, ResponsePayload};
use waku_protocol::computer_use::ComputerAppGrant;
use waku_protocol::i18n::AppLanguage;
use waku_protocol::identity::DATA_DIRECTORY_NAME;
use waku_protocol::model::{
    AgentSession, FavoriteModel, Project, ProviderKind, ProviderResumeCursor,
    ProviderSessionHistory, ProviderSessionSummary, RuntimeMode,
};
use waku_protocol::theme::ThemePreference;

pub use waku_protocol::persistence::{
    ComposerDraft, ComposerDraftAttachment, ComposerDraftChange, ComposerDraftKey,
    ComposerDraftTarget, ComposerDrafts, SessionMessageMatch,
};

const STATE_VERSION: u32 = 5;
const APP_STATE_VERSION: u32 = 1;

pub const DEFAULT_SIDEBAR_WIDTH: f32 = 252.0;
pub const DEFAULT_RIGHT_PANEL_WIDTH: f32 = 460.0;

/// How the desktop groups task history in the sidebar.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SidebarGrouping {
    Project,
    #[default]
    Updated,
}

/// Direction of task history inside the sidebar's current grouping.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SidebarOrdering {
    #[default]
    Newest,
    Oldest,
}

fn default_sidebar_visibility() -> bool {
    true
}

fn default_right_panel_visibility() -> bool {
    false
}

fn default_computer_use_enabled() -> bool {
    false
}

fn default_ui_font_size() -> f32 {
    DEFAULT_UI_FONT_SIZE
}

fn default_code_font_size() -> f32 {
    DEFAULT_CODE_FONT_SIZE
}

fn default_render_math() -> bool {
    true
}

/// The fork ships no analytics endpoint, so sharing is opt-in.
fn default_analytics_enabled() -> bool {
    false
}

fn default_provider() -> ProviderKind {
    ProviderKind::Codex
}

fn default_sidebar_width() -> f32 {
    DEFAULT_SIDEBAR_WIDTH
}

fn default_right_panel_width() -> f32 {
    DEFAULT_RIGHT_PANEL_WIDTH
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RememberedModelTraits {
    provider: ProviderKind,
    model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reasoning_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    service_tier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context_window: Option<String>,
}

/// Remote composer-draft proxy. Draft bytes and attachments remain owned by
/// the daemon even though the desktop keeps an in-memory editing snapshot.
#[derive(Clone)]
pub struct ComposerDraftStore {
    daemon: DaemonSupervisor,
    save_state: Arc<Mutex<ComposerDraftSaveState>>,
}

#[derive(Default)]
struct ComposerDraftSaveState {
    snapshot: ComposerDrafts,
    latest_generation: u64,
}

impl ComposerDraftStore {
    pub fn remote(daemon: DaemonSupervisor) -> Self {
        Self {
            daemon,
            save_state: Arc::new(Mutex::new(ComposerDraftSaveState::default())),
        }
    }

    pub fn load(&self) -> io::Result<ComposerDrafts> {
        match self
            .daemon
            .client()
            .request(Uuid::nil(), Uuid::nil(), Command::LoadComposerDrafts)
            .map_err(to_io_error)?
        {
            ResponsePayload::ComposerDrafts { drafts } => {
                self.save_state.lock().snapshot = drafts.clone();
                Ok(drafts)
            }
            _ => Err(io::Error::other(
                "Waku daemon returned an invalid composer-drafts response",
            )),
        }
    }

    pub fn save(&self, drafts: ComposerDrafts, generation: u64) -> io::Result<()> {
        // Serialize this client's detached background saves and compare only
        // against its last local snapshot. A second client may add or remove
        // other keys without those changes being interpreted as ours.
        let mut state = self.save_state.lock();
        if generation < state.latest_generation {
            return Ok(());
        }
        let changes = composer_draft_changes(&state.snapshot, &drafts);
        if changes.is_empty() {
            state.latest_generation = generation;
            return Ok(());
        }
        match self
            .daemon
            .client()
            .request(
                Uuid::nil(),
                Uuid::nil(),
                Command::ApplyComposerDraftChanges { changes },
            )
            .map_err(to_io_error)?
        {
            ResponsePayload::Ack => {
                state.snapshot = drafts;
                state.latest_generation = generation;
                Ok(())
            }
            _ => Err(io::Error::other(
                "Waku daemon returned an invalid composer-drafts save response",
            )),
        }
    }
}

fn composer_draft_changes(
    previous: &ComposerDrafts,
    next: &ComposerDrafts,
) -> Vec<ComposerDraftChange> {
    let mut changes = Vec::new();
    collect_composer_draft_changes(
        &previous.new_sessions,
        &next.new_sessions,
        |project_id| ComposerDraftTarget::NewSession { project_id },
        &mut changes,
    );
    collect_composer_draft_changes(
        &previous.sessions,
        &next.sessions,
        |session_id| ComposerDraftTarget::Session { session_id },
        &mut changes,
    );
    changes
}

fn collect_composer_draft_changes(
    previous: &HashMap<Uuid, ComposerDraft>,
    next: &HashMap<Uuid, ComposerDraft>,
    target: impl Fn(Uuid) -> ComposerDraftTarget,
    changes: &mut Vec<ComposerDraftChange>,
) {
    for (id, draft) in next {
        if previous.get(id) != Some(draft) {
            changes.push(ComposerDraftChange {
                target: target(*id),
                draft: Some(draft.clone()),
            });
        }
    }
    for id in previous.keys() {
        if !next.contains_key(id) {
            changes.push(ComposerDraftChange {
                target: target(*id),
                draft: None,
            });
        }
    }
}

/// Last observed main-window frame in logical pixels. GPUI window bounds are
/// relative to the display the window sits on, so the frame only means
/// something together with `display` — the stable display UUID (Zed persists
/// the same pair; display *ids* renumber across reboots and replugs). While
/// the window is maximized or fullscreen these bounds keep the floating frame
/// the window returns to, not the screen-filling one.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub struct PersistedWindowState {
    pub x: f32,
    pub y: f32,
    pub width: f32,
    pub height: f32,
    #[serde(default)]
    pub maximized: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display: Option<Uuid>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct AppSettings {
    pub analytics_enabled: bool,
    pub favorite_models: Vec<FavoriteModel>,
    pub theme: ThemePreference,
    pub language: AppLanguage,
    /// Base text size for the interface, in pixels: chrome and prose are
    /// authored against the 14px default and scale from it. Hand-edited
    /// values are clamped when applied.
    pub ui_font_size: f32,
    /// Text size for code surfaces — the file editor, diffs, code blocks,
    /// and tool output — in pixels. Hand-edited values are clamped when
    /// applied.
    pub code_font_size: f32,
    pub render_math: bool,
    /// Slide the sidebar and right panel open and shut. Off by default:
    /// toggling a panel is instant, the way T3 Code's panels are.
    pub animate_panels: bool,
    pub daemon_exposure: DaemonExposureSettings,
    /// Preferred target of the header's "open project in app" control, by
    /// catalog id. `None` — and an id no longer installed — fall back to the
    /// platform file manager.
    pub open_in_app: Option<String>,
}

impl Default for AppSettings {
    fn default() -> Self {
        Self {
            analytics_enabled: default_analytics_enabled(),
            favorite_models: Vec::new(),
            theme: ThemePreference::System,
            language: AppLanguage::default(),
            ui_font_size: DEFAULT_UI_FONT_SIZE,
            code_font_size: DEFAULT_CODE_FONT_SIZE,
            render_math: true,
            animate_panels: false,
            daemon_exposure: DaemonExposureSettings::default(),
            open_in_app: None,
        }
    }
}

pub const DEFAULT_UI_FONT_SIZE: f32 = 14.0;
pub const DEFAULT_CODE_FONT_SIZE: f32 = 13.0;

/// Bounds a possibly hand-edited font size to something the layout survives.
fn sanitized_font_size(size: f32, fallback: f32) -> f32 {
    if size.is_finite() {
        size.clamp(9.0, 24.0)
    } else {
        fallback
    }
}

pub fn sanitized_ui_font_size(size: f32) -> f32 {
    sanitized_font_size(size, DEFAULT_UI_FONT_SIZE)
}

pub fn sanitized_code_font_size(size: f32) -> f32 {
    sanitized_font_size(size, DEFAULT_CODE_FONT_SIZE)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct AppState {
    app_state_version: u32,
    #[serde(default = "Uuid::new_v4")]
    analytics_id: Uuid,
    #[serde(default)]
    selected_project: Option<Uuid>,
    #[serde(default)]
    selected_session: Option<Uuid>,
    #[serde(default = "default_provider")]
    last_provider: ProviderKind,
    #[serde(default)]
    last_runtime_mode: RuntimeMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_reasoning_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_service_tier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_context_window: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    remembered_model_traits: Vec<RememberedModelTraits>,
    #[serde(default = "default_sidebar_visibility")]
    sidebar_visible: bool,
    #[serde(default = "default_right_panel_visibility")]
    right_panel_visible: bool,
    #[serde(default = "default_sidebar_width")]
    sidebar_width: f32,
    #[serde(default)]
    sidebar_grouping: SidebarGrouping,
    #[serde(default)]
    sidebar_ordering: SidebarOrdering,
    #[serde(default = "default_right_panel_width")]
    right_panel_width: f32,
    /// Whether markdown files in the right panel open as a rendered preview
    /// instead of source. One global mode, not per file.
    #[serde(default)]
    markdown_preview: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    window_state: Option<PersistedWindowState>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PersistedState {
    pub version: u32,
    #[serde(default = "Uuid::new_v4")]
    pub analytics_id: Uuid,
    #[serde(default = "default_analytics_enabled")]
    pub analytics_enabled: bool,
    pub projects: Vec<Project>,
    pub sessions: Vec<AgentSession>,
    pub selected_project: Option<Uuid>,
    pub selected_session: Option<Uuid>,
    pub last_provider: ProviderKind,
    #[serde(default)]
    pub last_runtime_mode: RuntimeMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reasoning_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_service_tier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_context_window: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remembered_model_traits: Vec<RememberedModelTraits>,
    #[serde(default)]
    pub favorite_models: Vec<FavoriteModel>,
    #[serde(default)]
    pub theme: ThemePreference,
    #[serde(default)]
    pub language: AppLanguage,
    #[serde(default = "default_ui_font_size")]
    pub ui_font_size: f32,
    #[serde(default = "default_code_font_size")]
    pub code_font_size: f32,
    #[serde(default = "default_render_math")]
    pub render_math: bool,
    #[serde(default)]
    pub animate_panels: bool,
    #[serde(default)]
    pub daemon_exposure: DaemonExposureSettings,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_in_app: Option<String>,
    #[serde(default = "default_sidebar_visibility")]
    pub sidebar_visible: bool,
    #[serde(default = "default_right_panel_visibility")]
    pub right_panel_visible: bool,
    #[serde(default = "default_sidebar_width")]
    pub sidebar_width: f32,
    #[serde(default)]
    pub sidebar_grouping: SidebarGrouping,
    #[serde(default)]
    pub sidebar_ordering: SidebarOrdering,
    #[serde(default = "default_right_panel_width")]
    pub right_panel_width: f32,
    /// Whether markdown files in the right panel open as a rendered preview
    /// instead of source. One global mode, not per file.
    #[serde(default)]
    pub markdown_preview: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_state: Option<PersistedWindowState>,
    #[serde(default = "default_computer_use_enabled")]
    pub computer_use_enabled: bool,
    #[serde(default)]
    pub computer_use_allowed_apps: Vec<ComputerAppGrant>,
    #[serde(default)]
    pub disabled_providers: Vec<ProviderKind>,
    #[serde(default)]
    pub provider_binary_overrides: HashMap<ProviderKind, String>,
    #[serde(skip)]
    daemon_settings_extra: BTreeMap<String, serde_json::Value>,
    #[serde(skip)]
    dirty_sessions: HashSet<Uuid>,
}

impl PersistedState {
    pub fn session_mut(&mut self, id: Uuid) -> Option<&mut AgentSession> {
        let session = self.sessions.iter_mut().find(|session| session.id == id)?;
        self.dirty_sessions.insert(id);
        Some(session)
    }

    pub fn mark_session_dirty(&mut self, id: Uuid) {
        self.dirty_sessions.insert(id);
    }

    /// Whether `id` has changes the next save has yet to send the daemon.
    pub fn is_session_dirty(&self, id: Uuid) -> bool {
        self.dirty_sessions.contains(&id)
    }

    /// Snapshots of the dirty sessions for `SaveTaskState`. Cheap: their
    /// transcripts are shared, not copied.
    ///
    /// A skeleton (`detail_loaded == false`: never hydrated, or released by
    /// dehydration) is left out. The wire cannot say "not loaded", so the
    /// daemon would take its empty transcript as the real one.
    fn dirty_session_snapshots(&self) -> Vec<AgentSession> {
        self.sessions
            .iter()
            .filter(|session| session.detail_loaded && self.dirty_sessions.contains(&session.id))
            .cloned()
            .collect()
    }

    pub fn push_session(&mut self, session: AgentSession) {
        self.dirty_sessions.insert(session.id);
        self.sessions.push(session);
    }

    pub fn empty() -> Self {
        Self {
            version: STATE_VERSION,
            analytics_id: Uuid::new_v4(),
            analytics_enabled: default_analytics_enabled(),
            projects: Vec::new(),
            sessions: Vec::new(),
            selected_project: None,
            selected_session: None,
            last_provider: ProviderKind::Codex,
            last_runtime_mode: RuntimeMode::default(),
            last_model: None,
            last_reasoning_effort: None,
            last_service_tier: None,
            last_context_window: None,
            remembered_model_traits: Vec::new(),
            favorite_models: Vec::new(),
            theme: ThemePreference::System,
            language: AppLanguage::default(),
            ui_font_size: DEFAULT_UI_FONT_SIZE,
            code_font_size: DEFAULT_CODE_FONT_SIZE,
            render_math: true,
            animate_panels: false,
            daemon_exposure: DaemonExposureSettings::default(),
            open_in_app: None,
            sidebar_visible: true,
            right_panel_visible: false,
            sidebar_width: DEFAULT_SIDEBAR_WIDTH,
            sidebar_grouping: SidebarGrouping::Updated,
            sidebar_ordering: SidebarOrdering::Newest,
            right_panel_width: DEFAULT_RIGHT_PANEL_WIDTH,
            markdown_preview: false,
            window_state: None,
            computer_use_enabled: false,
            computer_use_allowed_apps: Vec::new(),
            disabled_providers: Vec::new(),
            provider_binary_overrides: HashMap::new(),
            daemon_settings_extra: BTreeMap::new(),
            dirty_sessions: HashSet::new(),
        }
    }

    pub fn fresh(cwd: PathBuf) -> Self {
        let project = Project::from_path(cwd);
        let session = AgentSession::new(project.id, ProviderKind::Codex);
        Self {
            selected_project: Some(project.id),
            selected_session: Some(session.id),
            projects: vec![project],
            sessions: vec![session],
            ..Self::empty()
        }
    }

    pub fn new_session(&self, project_id: Uuid, provider: ProviderKind) -> AgentSession {
        let mut session = AgentSession::new(project_id, provider);
        session.runtime_mode = self.last_runtime_mode;
        if provider == self.last_provider {
            session.model.clone_from(&self.last_model);
            session
                .reasoning_effort
                .clone_from(&self.last_reasoning_effort);
            session.service_tier.clone_from(&self.last_service_tier);
            session.context_window.clone_from(&self.last_context_window);
        }
        session
    }

    pub fn remember_model_traits(
        &mut self,
        provider: ProviderKind,
        model: &str,
        reasoning_effort: Option<String>,
        service_tier: Option<String>,
        context_window: Option<String>,
    ) {
        let existing = self
            .remembered_model_traits
            .iter()
            .position(|traits| traits.provider == provider && traits.model == model);
        if reasoning_effort.is_none() && service_tier.is_none() && context_window.is_none() {
            if let Some(index) = existing {
                self.remembered_model_traits.remove(index);
            }
            return;
        }
        if let Some(index) = existing {
            let traits = &mut self.remembered_model_traits[index];
            traits.reasoning_effort = reasoning_effort;
            traits.service_tier = service_tier;
            traits.context_window = context_window;
        } else {
            self.remembered_model_traits.push(RememberedModelTraits {
                provider,
                model: model.to_owned(),
                reasoning_effort,
                service_tier,
                context_window,
            });
        }
    }

    pub fn model_traits_for(
        &self,
        provider: ProviderKind,
        model: &str,
    ) -> (Option<String>, Option<String>, Option<String>) {
        self.remembered_model_traits
            .iter()
            .find(|traits| traits.provider == provider && traits.model == model)
            .map(|traits| {
                (
                    traits.reasoning_effort.clone(),
                    traits.service_tier.clone(),
                    traits.context_window.clone(),
                )
            })
            .unwrap_or_default()
    }

    pub fn daemon_settings(&self) -> DaemonSettings {
        DaemonSettings {
            computer_use_enabled: self.computer_use_enabled,
            computer_use_allowed_apps: self.computer_use_allowed_apps.clone(),
            disabled_providers: self.disabled_providers.clone(),
            provider_binary_overrides: self.provider_binary_overrides.clone(),
            extra: self.daemon_settings_extra.clone(),
        }
    }

    pub fn apply_daemon_settings(&mut self, settings: DaemonSettings) {
        // Computer Use is experimental, so a release build must not let a
        // setting written by a development build leave this client believing
        // it is on.
        self.computer_use_enabled =
            crate::computer_use::resolve_enabled(settings.computer_use_enabled);
        self.computer_use_allowed_apps = settings.computer_use_allowed_apps;
        self.disabled_providers = settings.disabled_providers;
        self.provider_binary_overrides = settings.provider_binary_overrides;
        self.daemon_settings_extra = settings.extra;
    }

    fn app_settings(&self) -> AppSettings {
        AppSettings {
            analytics_enabled: self.analytics_enabled,
            favorite_models: self.favorite_models.clone(),
            theme: self.theme,
            language: self.language,
            ui_font_size: self.ui_font_size,
            code_font_size: self.code_font_size,
            render_math: self.render_math,
            animate_panels: self.animate_panels,
            daemon_exposure: self.daemon_exposure.clone(),
            open_in_app: self.open_in_app.clone(),
        }
    }

    fn app_state(&self) -> AppState {
        AppState {
            app_state_version: APP_STATE_VERSION,
            analytics_id: self.analytics_id,
            selected_project: self.selected_project,
            selected_session: self.persistable_selected_session(),
            last_provider: self.last_provider,
            last_runtime_mode: self.last_runtime_mode,
            last_model: self.last_model.clone(),
            last_reasoning_effort: self.last_reasoning_effort.clone(),
            last_service_tier: self.last_service_tier.clone(),
            last_context_window: self.last_context_window.clone(),
            remembered_model_traits: self.remembered_model_traits.clone(),
            sidebar_visible: self.sidebar_visible,
            right_panel_visible: self.right_panel_visible,
            sidebar_width: self.sidebar_width,
            sidebar_grouping: self.sidebar_grouping,
            sidebar_ordering: self.sidebar_ordering,
            right_panel_width: self.right_panel_width,
            markdown_preview: self.markdown_preview,
            window_state: self.window_state,
        }
    }

    fn apply_app_settings(&mut self, settings: AppSettings) {
        self.analytics_enabled = settings.analytics_enabled;
        self.favorite_models = settings.favorite_models;
        self.theme = settings.theme;
        self.language = settings.language;
        self.ui_font_size = sanitized_ui_font_size(settings.ui_font_size);
        self.code_font_size = sanitized_code_font_size(settings.code_font_size);
        self.render_math = settings.render_math;
        self.animate_panels = settings.animate_panels;
        self.daemon_exposure = settings.daemon_exposure;
        self.open_in_app = settings.open_in_app;
    }

    fn apply_app_state(&mut self, app_state: AppState) {
        self.analytics_id = app_state.analytics_id;
        self.selected_project = app_state.selected_project;
        self.selected_session = app_state.selected_session;
        self.last_provider = app_state.last_provider;
        self.last_runtime_mode = app_state.last_runtime_mode;
        self.last_model = app_state.last_model;
        self.last_reasoning_effort = app_state.last_reasoning_effort;
        self.last_service_tier = app_state.last_service_tier;
        self.last_context_window = app_state.last_context_window;
        self.remembered_model_traits = app_state.remembered_model_traits;
        self.sidebar_visible = app_state.sidebar_visible;
        self.right_panel_visible = app_state.right_panel_visible;
        self.sidebar_width = app_state.sidebar_width;
        self.sidebar_grouping = app_state.sidebar_grouping;
        self.sidebar_ordering = app_state.sidebar_ordering;
        self.right_panel_width = app_state.right_panel_width;
        self.markdown_preview = app_state.markdown_preview;
        self.window_state = app_state.window_state;
    }

    fn persistable_selected_session(&self) -> Option<Uuid> {
        self.selected_session.filter(|selected| {
            self.sessions
                .iter()
                .any(|session| session.id == *selected && session.has_started())
        })
    }

    fn ensure_runtime_session(&mut self) {
        if self
            .selected_session
            .is_some_and(|selected| self.sessions.iter().any(|session| session.id == selected))
        {
            return;
        }
        self.selected_session = None;
        let Some(project_id) = self
            .selected_project
            .filter(|selected| self.projects.iter().any(|project| project.id == *selected))
        else {
            return;
        };
        let session = self.new_session(project_id, self.last_provider);
        self.selected_session = Some(session.id);
        self.sessions.push(session);
    }

    fn migrate_loaded(&mut self) {
        for session in &mut self.sessions {
            let checkpoint_totals_current = session.turns.iter().all(|turn| {
                turn.checkpoint
                    .as_ref()
                    .is_none_or(waku_protocol::model::Checkpoint::totals_are_current)
            });
            let before = (
                session.turns.len(),
                session.last_reply_at,
                session.provider_cursor.is_some(),
            );
            session.migrate_legacy_state();
            session.backfill_last_reply_at();
            if !checkpoint_totals_current
                || before
                    != (
                        session.turns.len(),
                        session.last_reply_at,
                        session.provider_cursor.is_some(),
                    )
            {
                self.dirty_sessions.insert(session.id);
            }
        }
        self.version = STATE_VERSION;
        normalize_computer_app_grants(&mut self.computer_use_allowed_apps);
        self.backfill_remembered_selection();
    }

    fn backfill_remembered_selection(&mut self) {
        let Some(session) = self
            .selected_session
            .and_then(|selected| self.sessions.iter().find(|session| session.id == selected))
            .cloned()
        else {
            return;
        };
        if self.last_model.is_none() {
            self.last_model = session.model;
        }
        if self.last_reasoning_effort.is_none() {
            self.last_reasoning_effort = session.reasoning_effort;
        }
        if self.last_service_tier.is_none() {
            self.last_service_tier = session.service_tier;
        }
        if self.last_context_window.is_none() {
            self.last_context_window = session.context_window;
        }
    }
}

fn configuration_directory() -> PathBuf {
    waku_protocol::identity::home_directory()
}

fn default_app_settings_path() -> PathBuf {
    if cfg!(debug_assertions) {
        StateStore::default_path().with_file_name("app.json")
    } else {
        configuration_directory().join("app.json")
    }
}

fn default_app_state_path() -> PathBuf {
    StateStore::default_path().with_file_name("state.json")
}

fn read_app_state_file(path: &Path) -> Option<AppState> {
    let bytes = fs::read(path).ok()?;
    let app_state = serde_json::from_slice::<AppState>(&bytes).ok()?;
    (app_state.app_state_version == APP_STATE_VERSION).then_some(app_state)
}

/// Read the last persisted window frame ahead of the full state load. The
/// main window opens before the daemon connection and `StateStore` exist, and
/// `WindowOptions` needs its frame at `open_window` time.
pub fn load_window_state() -> Option<PersistedWindowState> {
    read_app_state_file(&default_app_state_path())?.window_state
}

fn default_legacy_settings_paths() -> Vec<PathBuf> {
    if cfg!(debug_assertions) {
        vec![StateStore::default_path().with_file_name("settings.json")]
    } else {
        vec![configuration_directory().join("settings.json")]
    }
}

fn read_app_settings_source(
    app_settings_path: &Path,
    legacy_settings_paths: &[PathBuf],
) -> io::Result<Option<(Vec<u8>, bool)>> {
    match fs::read(app_settings_path) {
        Ok(bytes) => return Ok(Some((bytes, true))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    for path in legacy_settings_paths {
        match fs::read(path) {
            Ok(bytes) => return Ok(Some((bytes, false))),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(None)
}

/// Load the app-owned launch settings before the managed daemon starts.
/// Missing settings are initialized immediately so its bearer token remains
/// stable between this process launch and the later UI state load.
pub fn load_or_create_app_settings() -> io::Result<AppSettings> {
    let path = default_app_settings_path();
    let source = read_app_settings_source(&path, &default_legacy_settings_paths())?;
    let loaded_from_primary = source.as_ref().is_some_and(|(_, primary)| *primary);
    let token_was_persisted = source
        .as_ref()
        .and_then(|(bytes, _)| serde_json::from_slice::<serde_json::Value>(bytes).ok())
        .and_then(|value| {
            value
                .get("daemon_exposure")
                .and_then(|daemon| daemon.get("token"))
                .and_then(serde_json::Value::as_str)
                .map(|token| !token.trim().is_empty())
        })
        .unwrap_or(false);
    let mut settings: AppSettings = source
        .map(|(bytes, _)| serde_json::from_slice::<AppSettings>(&bytes).map_err(to_io_error))
        .transpose()?
        .unwrap_or_default();
    let generated_token = settings.daemon_exposure.ensure_token();
    if !loaded_from_primary || !token_was_persisted || generated_token {
        write_json_atomically(&path, &settings)?;
    }
    Ok(settings)
}

/// Desktop state store: app files stay local, task data crosses RPC.
pub struct StateStore {
    path: PathBuf,
    app_state_path: PathBuf,
    app_settings_path: PathBuf,
    legacy_settings_paths: Vec<PathBuf>,
    daemon: DaemonSupervisor,
    remote_default_cwd: Mutex<Option<PathBuf>>,
    /// A task snapshot may only be written after this client has successfully
    /// loaded the daemon's authoritative state. Falling back to an empty UI
    /// after a transient RPC failure must never turn the next ordinary save
    /// into a destructive replacement of the daemon database.
    task_state_loaded: AtomicBool,
    /// Writes `app.json` and `state.json` off the UI thread.
    app_files: AppFileWriter,
}

impl StateStore {
    pub fn default_path() -> PathBuf {
        if cfg!(debug_assertions) {
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .and_then(Path::parent)
                .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")))
                .join("temp")
                .join("app.db")
        } else {
            dirs::data_local_dir()
                .unwrap_or_else(std::env::temp_dir)
                .join(DATA_DIRECTORY_NAME)
                .join("app.db")
        }
    }

    pub fn remote(daemon: DaemonSupervisor) -> Self {
        let app_state_path = default_app_state_path();
        let app_settings_path = default_app_settings_path();
        Self {
            app_files: AppFileWriter::spawn(
                app_settings_path.clone(),
                app_state_path.clone(),
                APP_FILE_WRITE_DEBOUNCE,
            ),
            app_state_path,
            app_settings_path,
            legacy_settings_paths: default_legacy_settings_paths(),
            path: Self::default_path(),
            daemon,
            remote_default_cwd: Mutex::new(None),
            task_state_loaded: AtomicBool::new(false),
        }
    }

    pub fn session_message_search(
        &self,
        query: String,
        limit: usize,
    ) -> impl FnOnce() -> io::Result<Vec<SessionMessageMatch>> + Send + 'static {
        let daemon = self.daemon.clone();
        move || match daemon
            .client()
            .request(
                Uuid::nil(),
                Uuid::nil(),
                Command::SearchSessionMessages { query, limit },
            )
            .map_err(to_io_error)?
        {
            ResponsePayload::SessionMessageMatches { matches } => Ok(matches),
            _ => Err(io::Error::other(
                "Waku daemon returned an invalid message-search response",
            )),
        }
    }

    pub fn provider_sessions(
        &self,
        provider: ProviderKind,
        limit: usize,
    ) -> impl FnOnce() -> io::Result<Vec<ProviderSessionSummary>> + Send + 'static {
        let daemon = self.daemon.clone();
        move || match daemon
            .client()
            .request(
                Uuid::nil(),
                Uuid::nil(),
                Command::ListProviderSessions { provider, limit },
            )
            .map_err(to_io_error)?
        {
            ResponsePayload::ProviderSessions { sessions } => Ok(sessions),
            _ => Err(io::Error::other(
                "Waku daemon returned an invalid provider-session response",
            )),
        }
    }

    pub fn provider_session_history(
        &self,
        cursor: ProviderResumeCursor,
        cwd: PathBuf,
    ) -> impl FnOnce() -> io::Result<ProviderSessionHistory> + Send + 'static {
        let daemon = self.daemon.clone();
        move || match daemon
            .client()
            .request(
                Uuid::nil(),
                Uuid::nil(),
                Command::LoadProviderSession { cursor, cwd },
            )
            .map_err(to_io_error)?
        {
            ResponsePayload::ProviderSessionHistory { history } => Ok(history),
            _ => Err(io::Error::other(
                "Waku daemon returned an invalid provider-session history response",
            )),
        }
    }

    pub fn load_or_fresh(&self, cwd: PathBuf) -> PersistedState {
        let mut state = match self.load() {
            Ok(state) => {
                self.task_state_loaded.store(true, Ordering::Release);
                state
            }
            Err(_) if cwd.parent().is_none() => PersistedState::empty(),
            Err(_) => PersistedState::fresh(cwd),
        };
        if state.projects.is_empty()
            && let Some(cwd) = self.remote_default_cwd.lock().clone()
            && cwd.parent().is_some()
        {
            let project = Project::from_path(cwd);
            let session = state.new_session(project.id, state.last_provider);
            state.selected_project = Some(project.id);
            state.selected_session = Some(session.id);
            state.projects.push(project);
            state.push_session(session);
        }
        state.ensure_runtime_session();
        if let Some(selected) = state.selected_session
            && let Some(session) = state
                .sessions
                .iter_mut()
                .find(|session| session.id == selected)
        {
            let _ = self.hydrate(session);
        }
        state
    }

    pub fn load(&self) -> io::Result<PersistedState> {
        let (projects, mut sessions, default_cwd) = match self
            .daemon
            .client()
            .request(Uuid::nil(), Uuid::nil(), Command::LoadTaskState)
            .map_err(to_io_error)?
        {
            ResponsePayload::TaskState {
                projects,
                sessions,
                default_cwd,
                projectless_root,
            } => {
                waku_protocol::projectless::set_workspace_root(projectless_root);
                (projects, sessions, default_cwd)
            }
            _ => {
                return Err(io::Error::other(
                    "Waku daemon returned an invalid task-state response",
                ));
            }
        };
        restore_task_state_skeletons(&mut sessions);
        *self.remote_default_cwd.lock() = Some(default_cwd);
        let mut state = PersistedState::empty();
        state.projects = projects;
        state.sessions = sessions;
        let app_settings_missing = !self.app_settings_path.is_file();
        if let Some(settings) = self.read_app_settings()? {
            state.apply_app_settings(settings);
        }
        let app_state = read_app_state_file(&self.app_state_path);
        let app_state_missing = app_state.is_none();
        if let Some(app_state) = app_state {
            state.apply_app_state(app_state);
        }
        state.migrate_loaded();
        if app_settings_missing {
            let _ = self.write_app_settings(&state.app_settings());
        }
        if app_state_missing {
            let _ = self.write_app_state(&state.app_state());
        }
        Ok(state)
    }

    pub fn hydrate(&self, session: &mut AgentSession) -> io::Result<()> {
        if session.detail_loaded {
            return Ok(());
        }
        match hydrate_session(&self.daemon, session.id)? {
            Some(stored) => {
                *session = stored;
                Ok(())
            }
            None => {
                session.detail_loaded = true;
                Ok(())
            }
        }
    }

    /// Persists the desktop's state without file I/O or serialization on the
    /// calling (UI) thread.
    ///
    /// - The app files go to the background [`AppFileWriter`], which
    ///   coalesces saves into one write per [`APP_FILE_WRITE_DEBOUNCE`] and
    ///   skips files whose bytes did not change. A failed write surfaces from
    ///   the next call.
    /// - Dirty sessions are snapshotted for the daemon. Their transcripts are
    ///   [`waku_protocol::shared::SharedVec`]s, so the snapshot shares message
    ///   and block bodies instead of copying them, and the socket thread
    ///   serializes the command.
    pub fn save(&self, state: &mut PersistedState) -> io::Result<()> {
        self.app_files.submit(AppFiles {
            settings: state.app_settings(),
            state: state.app_state(),
        });
        let write_error = self.app_files.take_error();
        if !self.task_state_loaded.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "task state was not loaded; refusing to overwrite daemon data",
            ));
        }
        let sessions = state.dirty_session_snapshots();
        let live_session_ids = state.sessions.iter().map(|session| session.id).collect();
        self.daemon
            .client()
            .notify(
                Uuid::nil(),
                Uuid::nil(),
                Command::SaveTaskState {
                    projects: state.projects.clone(),
                    live_session_ids,
                    sessions,
                },
            )
            .map_err(to_io_error)?;
        state.dirty_sessions.clear();
        write_error.map_or(Ok(()), Err)
    }

    /// A blocking flush of the app files, for the background executor: it
    /// writes whatever is still inside the debounce window now, with
    /// `durability`. Quit uses [`Durability::Full`].
    pub fn app_files_flush(
        &self,
        durability: Durability,
    ) -> impl FnOnce() -> io::Result<()> + Send + 'static {
        self.app_files.flush_handle(durability)
    }

    pub fn remove_session(&self, session_id: Uuid) -> io::Result<()> {
        self.daemon
            .client()
            .notify(session_id, Uuid::nil(), Command::RemoveSession)
            .map_err(to_io_error)
    }

    pub fn blob_sweep(&self) -> impl FnOnce() + Send + 'static {
        let daemon = self.daemon.clone();
        move || {
            let _ = daemon
                .client()
                .request(Uuid::nil(), Uuid::nil(), Command::SweepBlobs);
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn read_app_settings(&self) -> io::Result<Option<AppSettings>> {
        let source =
            read_app_settings_source(&self.app_settings_path, &self.legacy_settings_paths)?;
        source
            .map(|(bytes, _)| serde_json::from_slice(&bytes).map_err(to_io_error))
            .transpose()
    }

    fn write_app_settings(&self, settings: &AppSettings) -> io::Result<()> {
        write_json_atomically(&self.app_settings_path, settings)
    }

    fn write_app_state(&self, state: &AppState) -> io::Result<()> {
        write_json_atomically(&self.app_state_path, state)
    }
}

/// Fetches one daemon-owned session. Remote image materialization is kept
/// separate so presentation code can show the transcript before large blobs
/// finish crossing the daemon boundary.
pub fn hydrate_session(
    daemon: &DaemonSupervisor,
    session_id: Uuid,
) -> io::Result<Option<AgentSession>> {
    match daemon
        .client()
        .request(
            Uuid::nil(),
            Uuid::nil(),
            Command::HydrateSession { session_id },
        )
        .map_err(to_io_error)?
    {
        ResponsePayload::Session { session } => Ok(session),
        _ => Err(io::Error::other(
            "Waku daemon returned an invalid session-hydration response",
        )),
    }
}

fn write_json_atomically(path: &Path, value: &impl Serialize) -> io::Result<()> {
    let data = serde_json::to_vec(value).map_err(to_io_error)?;
    write_bytes_atomically(path, &data, Durability::Full)
}

/// How far a write must reach before it counts as done.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Durability {
    /// Plain `fsync(2)`: the bytes reach the drive, which may still hold them
    /// in its cache. Enough for the debounced writes: tmp+rename already
    /// means a crash leaves the previous file or the new one, never a torn
    /// one.
    Fsync,
    /// `File::sync_all`, which on macOS is `F_FULLFSYNC` and also flushes the
    /// drive's cache (several ms). Reserved for startup and quit.
    Full,
}

fn sync_file(file: &fs::File, durability: Durability) -> io::Result<()> {
    match durability {
        Durability::Full => file.sync_all(),
        #[cfg(unix)]
        Durability::Fsync => {
            use std::os::fd::AsRawFd as _;
            // SAFETY: `fsync` only reads the descriptor, which `file` keeps
            // open for the duration of the call.
            if unsafe { libc::fsync(file.as_raw_fd()) } == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        }
        #[cfg(not(unix))]
        Durability::Fsync => file.sync_data(),
    }
}

/// Crash-safe replacement: write a sibling temporary file, sync it, then
/// rename it over `path`, so a crash leaves either the old or the new file.
fn write_bytes_atomically(path: &Path, data: &[u8], durability: Durability) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("json.tmp");
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(&temporary)?;
    file.write_all(data)?;
    sync_file(&file, durability)?;
    fs::rename(&temporary, path)?;
    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

/// Saves of the app files closer together than this coalesce into one write
/// of the newest content.
pub const APP_FILE_WRITE_DEBOUNCE: Duration = Duration::from_millis(500);

/// One save's worth of desktop-owned files.
struct AppFiles {
    settings: AppSettings,
    state: AppState,
}

enum AppFileCommand {
    Write(Box<AppFiles>),
    Flush(Durability, Sender<()>),
    Shutdown,
}

#[derive(Default)]
struct AppFileWriterShared {
    /// The newest failed write, reported by the next `StateStore::save`.
    error: Mutex<Option<io::Error>>,
    /// Debounce windows (and flushes) that had content to write.
    windows: AtomicU64,
    /// Files actually rewritten.
    file_writes: AtomicU64,
    /// Files skipped because their bytes matched the previous write.
    file_skips: AtomicU64,
}

/// Background writer for `app.json` and `state.json`.
///
/// The UI thread only moves two small structs into a channel. The writer
/// thread serializes them as compact JSON, skips a file whose bytes match
/// what it last wrote, and otherwise replaces it with tmp+rename and a plain
/// `fsync`. Writes are trailing-edge debounced: the first save after a quiet
/// period opens a window of `debounce`, later saves inside it replace its
/// content, and the end of the window writes the newest content once. The
/// window does not restart on every save, so a steady stream of saves still
/// lands within `debounce` of the first one, which bounds what a `kill -9`
/// can lose.
struct AppFileWriter {
    commands: Sender<AppFileCommand>,
    shared: Arc<AppFileWriterShared>,
    thread: Option<JoinHandle<()>>,
}

impl AppFileWriter {
    fn spawn(settings_path: PathBuf, state_path: PathBuf, debounce: Duration) -> Self {
        let (commands, receiver) = crossbeam_channel::unbounded();
        let shared = Arc::new(AppFileWriterShared::default());
        let thread = std::thread::Builder::new()
            .name("waku-app-files".to_owned())
            .spawn({
                let shared = shared.clone();
                move || {
                    AppFileWriterThread {
                        paths: [settings_path, state_path],
                        fingerprints: [None, None],
                        needs_full_sync: [false, false],
                        shared,
                    }
                    .run(&receiver, debounce);
                }
            })
            .ok();
        Self {
            commands,
            shared,
            thread,
        }
    }

    fn submit(&self, files: AppFiles) {
        let _ = self.commands.send(AppFileCommand::Write(Box::new(files)));
    }

    fn take_error(&self) -> Option<io::Error> {
        self.shared.error.lock().take()
    }

    fn flush_handle(
        &self,
        durability: Durability,
    ) -> impl FnOnce() -> io::Result<()> + Send + 'static {
        let commands = self.commands.clone();
        let shared = self.shared.clone();
        move || {
            let (done, finished) = crossbeam_channel::bounded(1);
            commands
                .send(AppFileCommand::Flush(durability, done))
                .map_err(|_| io::Error::other("the app file writer has stopped"))?;
            let _ = finished.recv();
            shared.error.lock().take().map_or(Ok(()), Err)
        }
    }
}

impl Drop for AppFileWriter {
    fn drop(&mut self) {
        let _ = self.commands.send(AppFileCommand::Shutdown);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

struct AppFileWriterThread {
    paths: [PathBuf; 2],
    /// FNV-1a of the bytes last written to each path, so an unchanged file
    /// costs one hash instead of a write and an fsync.
    fingerprints: [Option<u64>; 2],
    /// Paths whose newest write only had a plain `fsync`.
    needs_full_sync: [bool; 2],
    shared: Arc<AppFileWriterShared>,
}

impl AppFileWriterThread {
    fn run(mut self, commands: &Receiver<AppFileCommand>, debounce: Duration) {
        let mut pending: Option<Box<AppFiles>> = None;
        let mut deadline: Option<Instant> = None;
        loop {
            let command = match deadline {
                Some(deadline) => match commands.recv_deadline(deadline) {
                    Ok(command) => Some(command),
                    Err(RecvTimeoutError::Timeout) => None,
                    Err(RecvTimeoutError::Disconnected) => Some(AppFileCommand::Shutdown),
                },
                None => Some(commands.recv().unwrap_or(AppFileCommand::Shutdown)),
            };
            match command {
                None => {
                    deadline = None;
                    self.write(pending.take(), Durability::Fsync);
                }
                Some(AppFileCommand::Write(files)) => {
                    pending = Some(files);
                    deadline.get_or_insert_with(|| Instant::now() + debounce);
                }
                Some(AppFileCommand::Flush(durability, done)) => {
                    deadline = None;
                    self.write(pending.take(), durability);
                    let _ = done.send(());
                }
                Some(AppFileCommand::Shutdown) => {
                    self.write(pending.take(), Durability::Full);
                    return;
                }
            }
        }
    }

    fn write(&mut self, files: Option<Box<AppFiles>>, durability: Durability) {
        let mut result = Ok(());
        if let Some(files) = files {
            self.shared.windows.fetch_add(1, Ordering::Relaxed);
            let encoded = [
                serde_json::to_vec(&files.settings),
                serde_json::to_vec(&files.state),
            ];
            for (index, bytes) in encoded.into_iter().enumerate() {
                let written = bytes
                    .map_err(to_io_error)
                    .and_then(|bytes| self.write_file(index, &bytes, durability));
                result = result.and(written);
            }
        }
        if durability == Durability::Full {
            // Content an earlier window wrote with a plain fsync gets the full
            // sync it skipped, so quit leaves everything past the drive cache.
            for index in 0..self.paths.len() {
                if std::mem::take(&mut self.needs_full_sync[index]) {
                    let synced =
                        fs::File::open(&self.paths[index]).and_then(|file| file.sync_all());
                    result = result.and(synced);
                }
            }
        }
        if let Err(error) = result {
            *self.shared.error.lock() = Some(error);
        }
    }

    fn write_file(&mut self, index: usize, bytes: &[u8], durability: Durability) -> io::Result<()> {
        let fingerprint = fnv1a(bytes);
        if self.fingerprints[index] == Some(fingerprint) {
            self.shared.file_skips.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        // Forget the old fingerprint first: if this write fails, the next
        // window must retry even when its content is unchanged.
        self.fingerprints[index] = None;
        write_bytes_atomically(&self.paths[index], bytes, durability)?;
        self.fingerprints[index] = Some(fingerprint);
        self.needs_full_sync[index] = durability == Durability::Fsync;
        self.shared.file_writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

fn restore_task_state_skeletons(sessions: &mut [AgentSession]) {
    for session in sessions {
        session.detail_loaded = false;
    }
}

/// Reads one daemon-owned binary payload. Clients decide how to present the
/// bytes; this transport layer deliberately creates no client-side files.
pub fn read_remote_reference(
    reference: &str,
    daemon_path: Option<&Path>,
    daemon: &DaemonSupervisor,
) -> Option<Vec<u8>> {
    let command = if waku_protocol::blob::is_reference(reference) {
        Command::ReadBlob {
            reference: reference.to_owned(),
        }
    } else if reference.starts_with(waku_protocol::attachments::ATTACHMENT_SCHEME) {
        let path = daemon_path?;
        Command::ReadAttachment {
            reference: reference.to_owned(),
            path: path.to_owned(),
        }
    } else {
        return None;
    };
    let Ok(ResponsePayload::BlobData { bytes }) =
        daemon.client().request(Uuid::nil(), Uuid::nil(), command)
    else {
        return None;
    };
    Some(bytes)
}

fn normalize_computer_app_grants(grants: &mut Vec<ComputerAppGrant>) {
    let mut seen_bundle_ids = HashSet::new();
    grants.retain(|grant| {
        !grant.bundle_id.trim().is_empty() && seen_bundle_ids.insert(grant.bundle_id.clone())
    });
}

fn to_io_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn math_rendering_defaults_on_and_persists_as_an_app_preference() {
        let defaults: AppSettings = serde_json::from_str("{}").unwrap();
        assert!(defaults.render_math);
        let mut state = PersistedState::empty();
        assert!(state.render_math);
        state.render_math = false;
        let settings = serde_json::to_value(state.app_settings()).unwrap();
        assert_eq!(settings["render_math"], false);
        assert!(
            serde_json::to_value(state.app_state())
                .unwrap()
                .get("render_math")
                .is_none()
        );
        let mut restored = PersistedState::empty();
        restored.apply_app_settings(serde_json::from_value(settings).unwrap());
        assert!(!restored.render_math);
    }

    fn temporary_directory(name: &str) -> PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "waku-app-files-{name}-{}-{}",
            std::process::id(),
            Uuid::new_v4()
        ));
        fs::create_dir_all(&directory).unwrap();
        directory
    }

    fn app_files(state: &PersistedState) -> AppFiles {
        AppFiles {
            settings: state.app_settings(),
            state: state.app_state(),
        }
    }

    fn writer_in(directory: &Path, debounce: Duration) -> AppFileWriter {
        AppFileWriter::spawn(
            directory.join("app.json"),
            directory.join("state.json"),
            debounce,
        )
    }

    fn counts(writer: &AppFileWriter) -> (u64, u64, u64) {
        (
            writer.shared.windows.load(Ordering::Relaxed),
            writer.shared.file_writes.load(Ordering::Relaxed),
            writer.shared.file_skips.load(Ordering::Relaxed),
        )
    }

    #[test]
    fn a_burst_of_saves_is_one_write_per_debounce_window() {
        let directory = temporary_directory("burst");
        let debounce = Duration::from_millis(150);
        let writer = writer_in(&directory, debounce);
        let mut state = PersistedState::empty();

        for width in 0..50 {
            state.sidebar_width = 200.0 + width as f32;
            writer.submit(app_files(&state));
        }
        assert_eq!(counts(&writer).0, 0, "nothing is written inside the window");
        std::thread::sleep(debounce * 4);

        let (windows, file_writes, _) = counts(&writer);
        assert_eq!(windows, 1, "fifty saves inside one window coalesce");
        assert_eq!(file_writes, 2, "one write each for app.json and state.json");
        let stored: AppState =
            serde_json::from_slice(&fs::read(directory.join("state.json")).unwrap()).unwrap();
        assert_eq!(
            stored.sidebar_width, 249.0,
            "the window writes the newest save"
        );
        assert!(
            !directory.join("state.json.tmp").exists(),
            "the temporary file is renamed into place"
        );

        // A save stream that outlasts several windows still writes at most
        // once per window.
        let started = Instant::now();
        let mut saves = 0;
        while started.elapsed() < debounce * 4 {
            state.sidebar_width += 1.0;
            writer.submit(app_files(&state));
            saves += 1;
            std::thread::sleep(Duration::from_millis(5));
        }
        writer.flush_handle(Durability::Fsync)().unwrap();
        let windows = counts(&writer).0 - 1;
        assert!(
            windows <= 6,
            "{saves} saves over four windows wrote {windows} times"
        );
        let stored: AppState =
            serde_json::from_slice(&fs::read(directory.join("state.json")).unwrap()).unwrap();
        assert_eq!(stored.sidebar_width, state.sidebar_width);
        drop(writer);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn unchanged_app_files_are_not_rewritten() {
        let directory = temporary_directory("unchanged");
        let writer = writer_in(&directory, Duration::from_secs(60));
        let mut state = PersistedState::empty();

        writer.submit(app_files(&state));
        writer.flush_handle(Durability::Fsync)().unwrap();
        assert_eq!(counts(&writer), (1, 2, 0));

        writer.submit(app_files(&state));
        writer.flush_handle(Durability::Fsync)().unwrap();
        assert_eq!(counts(&writer), (2, 2, 2), "identical content is skipped");

        // Only the file whose bytes changed is written.
        state.right_panel_visible = !state.right_panel_visible;
        writer.submit(app_files(&state));
        writer.flush_handle(Durability::Full)().unwrap();
        assert_eq!(counts(&writer), (3, 3, 3));
        let stored: AppState =
            serde_json::from_slice(&fs::read(directory.join("state.json")).unwrap()).unwrap();
        assert_eq!(stored.right_panel_visible, state.right_panel_visible);

        // Shutdown writes what is still pending.
        state.sidebar_visible = !state.sidebar_visible;
        writer.submit(app_files(&state));
        drop(writer);
        let stored: AppState =
            serde_json::from_slice(&fs::read(directory.join("state.json")).unwrap()).unwrap();
        assert_eq!(stored.sidebar_visible, state.sidebar_visible);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_failed_write_is_reported_and_retried() {
        let directory = temporary_directory("failed");
        // A directory where the file should be makes the rename fail.
        fs::create_dir_all(directory.join("state.json")).unwrap();
        let writer = writer_in(&directory, Duration::from_secs(60));
        let state = PersistedState::empty();

        writer.submit(app_files(&state));
        assert!(writer.flush_handle(Durability::Fsync)().is_err());
        assert!(
            writer.take_error().is_none(),
            "the flush consumed the error"
        );

        fs::remove_dir_all(directory.join("state.json")).unwrap();
        writer.submit(app_files(&state));
        writer.flush_handle(Durability::Fsync)().unwrap();
        assert!(
            directory.join("state.json").is_file(),
            "unchanged content is retried"
        );
        drop(writer);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn save_snapshots_share_transcripts_and_never_send_skeletons() {
        let mut state = PersistedState::fresh(PathBuf::from("/tmp/project"));
        let id = state.sessions[0].id;
        state.sessions[0].begin_turn("a prompt");
        state.mark_session_dirty(id);
        let mut skeleton = AgentSession::new(state.projects[0].id, ProviderKind::Codex);
        skeleton.release_transcript();
        state.push_session(skeleton);

        let snapshots = state.dirty_session_snapshots();

        assert_eq!(snapshots.len(), 1, "the skeleton is not sent");
        assert_eq!(snapshots[0].id, id);
        assert!(
            snapshots[0].messages.ptr_eq(&state.sessions[0].messages),
            "a snapshot shares message bodies instead of copying them"
        );
    }

    #[test]
    fn desktop_settings_paths_are_build_specific() {
        let app_settings_path = default_app_settings_path();
        let legacy_settings_paths = default_legacy_settings_paths();

        #[cfg(debug_assertions)]
        {
            let state_path = StateStore::default_path();
            assert_eq!(app_settings_path, state_path.with_file_name("app.json"));
            assert_eq!(
                legacy_settings_paths,
                [state_path.with_file_name("settings.json")]
            );
        }

        #[cfg(not(debug_assertions))]
        {
            assert_eq!(
                app_settings_path,
                configuration_directory().join("app.json")
            );
            assert_eq!(
                legacy_settings_paths,
                [configuration_directory().join("settings.json")]
            );
        }
    }

    #[test]
    fn legacy_app_state_defaults_sidebar_presentation() {
        let state: AppState = serde_json::from_str(r#"{"app_state_version":1}"#).unwrap();

        assert_eq!(state.sidebar_grouping, SidebarGrouping::Updated);
        assert_eq!(state.sidebar_ordering, SidebarOrdering::Newest);
        assert_eq!(state.last_runtime_mode, RuntimeMode::FullAccess);
    }

    #[test]
    fn new_tasks_inherit_the_remembered_access_mode() {
        let mut state = PersistedState::fresh(PathBuf::from("/tmp/project"));
        state.last_runtime_mode = RuntimeMode::Ask;

        let session = state.new_session(state.projects[0].id, ProviderKind::OpenCode);

        assert_eq!(session.runtime_mode, RuntimeMode::Ask);
        assert_eq!(state.app_state().last_runtime_mode, RuntimeMode::Ask);
    }

    #[test]
    fn daemon_task_state_becomes_list_only_after_crossing_the_client_boundary() {
        let mut session = AgentSession::new(Uuid::new_v4(), ProviderKind::Codex);
        session.detail_loaded = false;
        assert!(
            session.has_started(),
            "a stored skeleton is visible history"
        );

        let encoded = serde_json::to_vec(&ResponsePayload::TaskState {
            projects: Vec::new(),
            sessions: vec![session],
            default_cwd: PathBuf::from("/daemon/project"),
            projectless_root: None,
        })
        .unwrap();
        let ResponsePayload::TaskState { mut sessions, .. } =
            serde_json::from_slice(&encoded).unwrap()
        else {
            panic!("task state response changed shape");
        };

        assert!(
            !sessions[0].has_started(),
            "serde cannot carry the process-local skeleton marker"
        );
        restore_task_state_skeletons(&mut sessions);
        assert!(!sessions[0].detail_loaded);
        assert!(sessions[0].has_started());
    }
}
