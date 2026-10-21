use super::*;
use waku_client::commands::{self, Selection};

fn new_task_runtime_mode(current: Option<&ThreadView>, remembered: RuntimeMode) -> RuntimeMode {
    current
        .map(|session| session.runtime_mode)
        .unwrap_or(remembered)
}

impl Waku {
    pub(crate) fn open_task_from_notification(&mut self, session_id: Uuid, cx: &mut Context<Self>) {
        self.select_session(session_id, cx);
    }

    pub(super) fn select_project(&mut self, project_id: Uuid, cx: &mut Context<Self>) {
        self.state.selected_project = Some(project_id);
        self.create_session_for(project_id, self.state.last_provider, cx);
    }

    pub(super) fn select_session(&mut self, session_id: Uuid, cx: &mut Context<Self>) {
        self.request_session_activation(session_id, SessionActivationTransition::Visit, cx);
    }

    /// Bench harness hook, compiled only with `perf-counters`: when
    /// `WAKU_PERF_VISIT_SESSIONS_MS` is set, select every daemon thread
    /// once, in catalog order, one per interval, through the same path as a
    /// sidebar click. The memory scenarios use it to visit N long sessions
    /// without UI input. Each visit bumps `totals.session_visits`. It waits
    /// for the shell stream to list threads before it can run out of them.
    #[cfg(feature = "perf-counters")]
    pub(super) fn start_perf_session_visits(&mut self, cx: &mut Context<Self>) {
        let Some(interval) = std::env::var("WAKU_PERF_VISIT_SESSIONS_MS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .map(Duration::from_millis)
        else {
            return;
        };
        cx.spawn(async move |waku, cx| {
            let mut visited = HashSet::new();
            loop {
                cx.background_executor().timer(interval).await;
                let next = waku.update(cx, |waku, cx| {
                    let next = waku
                        .threads
                        .shell_ids()
                        .find(|id| !visited.contains(id));
                    if let Some(session_id) = next {
                        waku.select_session(session_id, cx);
                        crate::perf_counters::session_visit();
                    }
                    next
                });
                match next {
                    Ok(Some(session_id)) => {
                        visited.insert(session_id);
                    }
                    // The thread list has not arrived yet.
                    Ok(None) if visited.is_empty() => {}
                    _ => break,
                }
            }
        })
        .detach();
    }

    fn request_session_activation(
        &mut self,
        session_id: Uuid,
        transition: SessionActivationTransition,
        cx: &mut Context<Self>,
    ) {
        let Some(view) = self.thread(session_id) else {
            return;
        };
        let needs_stream = !view.detail_loaded;
        self.reveal_sidebar_session(session_id);
        if needs_stream {
            // Keep the current transcript visible until the store has the
            // thread's first view, but acknowledge the click in the sidebar
            // at once.
            self.pending_session_activation = Some(PendingSessionActivation {
                session_id,
                transition,
            });
            self.threads.store.open_thread(session_id);
            cx.notify();
            return;
        }
        self.pending_session_activation = None;
        self.finish_session_activation(session_id, transition, cx);
    }

    pub(super) fn finish_session_activation(
        &mut self,
        session_id: Uuid,
        transition: SessionActivationTransition,
        cx: &mut Context<Self>,
    ) {
        match transition {
            SessionActivationTransition::Visit => self
                .session_navigation
                .visit(self.state.selected_session, session_id),
            SessionActivationTransition::Back { from } => {
                if self.state.selected_session != Some(from)
                    || self.session_navigation.back_target() != Some(session_id)
                {
                    return;
                }
                let _ = self.session_navigation.go_back(from);
            }
            SessionActivationTransition::Forward { from } => {
                if self.state.selected_session != Some(from)
                    || self.session_navigation.forward_target() != Some(session_id)
                {
                    return;
                }
                let _ = self.session_navigation.go_forward(from);
            }
        }
        self.activate_session(session_id, cx);
    }

    fn activate_session(&mut self, session_id: Uuid, cx: &mut Context<Self>) {
        let session_changed = self.state.selected_session != Some(session_id);
        if session_changed {
            self.capture_and_save_current_composer_draft(cx);
            self.store_selected_right_panel_state();
        }
        self.state.selected_session = Some(session_id);
        self.task_switcher.record_access(session_id);
        if !self.threads.drafts.contains_key(&session_id) {
            // Open (or touch) its stream: the store keeps the most recently
            // opened threads subscribed and closes the rest.
            self.threads.store.open_thread(session_id);
        }
        if let Some((
            project_id,
            provider,
            runtime_mode,
            model,
            reasoning_effort,
            service_tier,
            context_window,
        )) = self.selected_session().map(|session| {
            (
                session.project_id,
                session.provider,
                session.runtime_mode,
                session.model.clone(),
                session.reasoning_effort.clone(),
                session.service_tier.clone(),
                session.context_window.clone(),
            )
        }) {
            self.state.selected_project = Some(project_id);
            self.state.last_provider = provider;
            self.state.last_runtime_mode = runtime_mode;
            self.state.last_model = model;
            self.state.last_reasoning_effort = reasoning_effort;
            self.state.last_service_tier = service_tier;
            self.state.last_context_window = context_window;
        }
        if self
            .selected_session()
            .is_some_and(|session| !session.has_started())
        {
            self.session_navigation.remember_new_task(session_id);
        }
        self.threads.user_input = self
            .selected_session()
            .and_then(|session| session.pending_user_input.as_ref())
            .map(|input| PendingUserInput::new(input.request_id.clone(), input.questions.clone()));
        if session_changed {
            self.restore_selected_composer_draft(cx);
            self.sync_user_input_answer(cx);
            self.restore_right_panel_state(session_id, cx);
        } else {
            self.ensure_right_panel_terminals(cx);
        }
        self.reset_visible_state();
        if session_changed {
            // Each materialized worktree has its own cache entry. A task that
            // finished while another session was selected could otherwise
            // retain the clean snapshot captured before its agent made edits.
            self.refresh_selected_branch_snapshot(cx);
        }
        self.refresh_composer_sources(cx);
        self.reset_transcript_rows(self.transcript_row_count());
        self.mark_visited(session_id);
        self.save();
        cx.notify();
    }

    /// Drops cached answers about the workspace on disk.
    ///
    /// These queries cache to keep `git` and directory walks out of frames, but
    /// nothing tells us when the working tree changes underneath. Rather than
    /// expire on a timer, they are dropped at the moments the answer plausibly
    /// moved — coming back to the window, or a turn finishing.
    pub(super) fn invalidate_workspace_queries(&mut self, cx: &mut Context<Self>) {
        let Some(workspace_path) = self
            .selected_workspace_path()
            .map(std::path::Path::to_path_buf)
        else {
            return;
        };
        self.branch_snapshots.invalidate(&workspace_path);
        self.sidebar_branch_scan_fingerprint.set(None);
        self.sidebar_branch_scan_generation
            .set(self.sidebar_branch_scan_generation.get().wrapping_add(1));
        self.refresh_workspace_surfaces(cx);
        self.invalidate_composer_sources(cx);
    }

    pub(super) fn create_session_for(
        &mut self,
        project_id: Uuid,
        provider: ProviderKind,
        cx: &mut Context<Self>,
    ) {
        if let Some(draft_id) = self
            .threads
            .drafts
            .values()
            .find(|draft| draft.project_id == project_id && !draft.has_started())
            .map(|draft| draft.id)
        {
            self.select_session(draft_id, cx);
            return;
        }
        // A task opened from the current task carries its working access mode.
        // `last_runtime_mode` covers launch and the few creation paths without
        // a selected source task.
        let runtime_mode =
            new_task_runtime_mode(self.selected_session(), self.state.last_runtime_mode);
        let mut draft = ThreadView::draft(
            Uuid::new_v4(),
            project_id,
            provider,
            runtime_mode,
            unix_time(),
        );
        if provider == self.state.last_provider {
            draft.model.clone_from(&self.state.last_model);
            draft
                .reasoning_effort
                .clone_from(&self.state.last_reasoning_effort);
            draft.service_tier.clone_from(&self.state.last_service_tier);
            draft
                .context_window
                .clone_from(&self.state.last_context_window);
        }
        let id = draft.id;
        self.threads.drafts.insert(id, draft);
        self.select_session(id, cx);
    }

    pub(super) fn select_workspace(&mut self, workspace: SessionWorkspace, cx: &mut Context<Self>) {
        // Only a task that has not been sent chooses where it runs.
        let Some(draft) = self
            .state
            .selected_session
            .and_then(|id| self.threads.drafts.get_mut(&id))
        else {
            return;
        };
        if draft.has_started() || draft.workspace == workspace {
            return;
        }
        draft.workspace = workspace;
        cx.notify();
    }

    /// `thread.delete`. The row leaves the sidebar when the shell says so;
    /// an unsent draft simply goes.
    pub(super) fn remove_session(&mut self, session_id: Uuid, cx: &mut Context<Self>) {
        let Some(view) = self.thread(session_id) else {
            return;
        };
        let project_id = view.project_id;
        let composer_draft_key = threads::draft_key(view);
        self.remove_composer_draft(composer_draft_key, cx);
        if self.threads.drafts.remove(&session_id).is_some()
            && self.threads.shell.thread(session_id).is_none()
        {
            self.session_navigation.remove(session_id);
            self.task_switcher.remove(session_id);
            if self.state.selected_session == Some(session_id) {
                self.select_neighbour(project_id, cx);
            }
            cx.notify();
            return;
        }
        self.dispatch(commands::delete(session_id), ThreadAction::Other);
        cx.notify();
    }

    /// `thread.archive`: the row leaves the active sidebar when the shell
    /// moves it.
    pub(super) fn archive_session(&mut self, session_id: Uuid, cx: &mut Context<Self>) {
        if self.threads.drafts.contains_key(&session_id) {
            return;
        }
        self.dispatch(commands::archive(session_id), ThreadAction::Other);
        cx.notify();
    }

    pub(super) fn new_session_action(
        &mut self,
        _: &NewSession,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.settings_page = None;
        let current_project = self
            .selected_project()
            .map(|project| (project.id, project.is_projectless()));
        match current_project {
            Some((_, true)) => self.create_projectless_session(cx),
            Some((project_id, false)) => {
                if let Some(session_id) = self
                    .session_navigation
                    .remembered_new_task(self.threads.drafts.values(), project_id)
                {
                    self.select_session(session_id, cx);
                } else {
                    self.create_session_for(project_id, self.state.last_provider, cx);
                }
            }
            None => self.create_projectless_session(cx),
        }
        let focus_handle = self.composer_focus(cx);
        window.focus(&focus_handle, cx);
    }

    pub(super) fn new_project_action(
        &mut self,
        _: &NewProject,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.add_project(cx);
    }

    pub(super) fn open_settings_action(
        &mut self,
        _: &OpenSettings,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.settings_page = Some(SettingsPage::General);
        self.settings_scroll.set_offset(gpui::Point::default());
        // Sparkle owns this value and its consent prompt can flip it outside
        // the settings UI, so re-mirror it each time settings opens.
        self.automatic_updates_enabled = cx
            .try_global::<crate::updater::UpdaterState>()
            .and_then(|updater| updater.0.as_ref())
            .is_some_and(|updater| updater.automatically_checks_for_updates());
        // Warm the Usage page's transcript scan while the user is still on
        // General, so clicking Usage lands on data instead of a spinner.
        self.ensure_usage_history(false, cx);
        window.focus(&self.settings_focus, cx);
        cx.notify();
    }

    pub(super) fn toggle_sidebar_action(
        &mut self,
        _: &ToggleSidebar,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.set_sidebar_visible(!self.sidebar_visible, cx);
    }

    pub(super) fn toggle_right_panel_action(
        &mut self,
        _: &ToggleRightPanel,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.set_right_panel_visible(!self.right_panel_visible, cx);
    }

    pub(super) fn toggle_fps_counter_action(
        &mut self,
        _: &ToggleFpsCounter,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.fps_counter_visible = !self.fps_counter_visible;
        cx.notify();
    }

    pub(super) fn set_sidebar_visible(&mut self, visible: bool, cx: &mut Context<Self>) {
        if self.sidebar_visible == visible {
            return;
        }
        self.sidebar_visible = visible;
        self.sidebar_slide = self.begin_panel_slide(self.sidebar_rendered_width, cx);
        self.persist_panel_layout();
        cx.notify();
    }

    /// A toggle's slide, starting from the width the panel currently occupies
    /// so an interrupted one reverses from where its edge actually is.
    /// Panels are instant unless the user turned on **Animate panels**, and
    /// reduce-motion overrides that: `None` means the panel simply appears at
    /// its new width and no frames are scheduled for it.
    fn begin_panel_slide(&self, from: f32, cx: &App) -> Option<motion::WidthTween> {
        (self.state.animate_panels && !cx.reduce_motion()).then(|| motion::WidthTween::new(from))
    }

    pub(super) fn set_right_panel_visible(&mut self, visible: bool, cx: &mut Context<Self>) {
        if visible {
            self.request_active_terminal_focus();
        } else {
            self.right_panel_pending_terminal_focus = None;
        }
        if self.right_panel_visible == visible {
            return;
        }
        self.right_panel_visible = visible;
        self.right_panel_slide = self.begin_panel_slide(self.right_panel_rendered_width, cx);
        if visible {
            self.analytics
                .track(crate::analytics::Event::RightPanelOpened);
        }
        self.persist_panel_layout();
        cx.notify();
    }

    pub(super) fn persist_panel_layout(&mut self) {
        self.state.sidebar_visible = self.sidebar_visible;
        self.state.right_panel_visible = self.right_panel_visible;
        self.state.sidebar_width = self.sidebar_width;
        self.state.right_panel_width = self.right_panel_width;
        self.save();
    }

    /// Mirror the live window frame into persisted state; disk waits for the
    /// app-quit save (any other `save` carries the frame along for free).
    /// macOS reports a zoomed window as `Windowed` with screen-filling bounds,
    /// so while maximized (and while fullscreen) the last floating frame is
    /// kept as the restore size — and the display it was captured on — and
    /// only the flag advances.
    pub(super) fn capture_window_state(&mut self, window: &Window, cx: &App) {
        // Bounds also change when the OS relocates the window — a monitor
        // unplugged, a display asleep. Those moves are not the user's; like
        // Zed, only capture while the window is the active one.
        if !window.is_window_active() {
            return;
        }
        let previous = self.state.window_state;
        let display = window.display(cx).and_then(|display| display.uuid().ok());
        self.state.window_state = Some(match window.window_bounds() {
            WindowBounds::Fullscreen(restore) => {
                previous.unwrap_or_else(|| persisted_window_state(restore, false, display))
            }
            WindowBounds::Maximized(restore) => persisted_window_state(restore, true, display),
            WindowBounds::Windowed(bounds) if window.is_maximized() => PersistedWindowState {
                maximized: true,
                ..previous.unwrap_or_else(|| persisted_window_state(bounds, true, display))
            },
            WindowBounds::Windowed(bounds) => persisted_window_state(bounds, false, display),
        });
    }

    /// The width each panel lays its content out at. A panel mid-slide counts
    /// as on screen and keeps its full width here: the slide narrows the
    /// container that clips it, so nothing inside reflows on the way out.
    /// What the panel actually occupies this frame is
    /// [`Waku::sidebar_rendered_width`] / [`Waku::right_panel_rendered_width`].
    pub(super) fn effective_panel_widths(&self, window: &Window) -> (f32, f32) {
        fitted_panel_widths(
            f32::from(window.viewport_size().width),
            self.sidebar_visible || self.sidebar_slide.is_some(),
            self.right_panel_visible || self.right_panel_slide.is_some(),
            self.sidebar_width,
            self.right_panel_width,
        )
    }

    pub(super) fn begin_panel_resize(
        &mut self,
        target: PanelResizeTarget,
        event: &MouseDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let (sidebar_width, right_panel_width) = self.effective_panel_widths(window);
        // A drag tracks the pointer directly; whatever slide was still
        // finishing would fight it for the same edge.
        let start_width = match target {
            PanelResizeTarget::Sidebar => {
                self.sidebar_slide = None;
                self.sidebar_width = sidebar_width;
                crate::platform::set_sidebar_material_width(window, sidebar_width);
                sidebar_width
            }
            PanelResizeTarget::RightPanel => {
                self.right_panel_slide = None;
                self.right_panel_width = right_panel_width;
                right_panel_width
            }
            PanelResizeTarget::FileTree => {
                let width =
                    fitted_file_tree_width(right_panel_width, self.right_panel_file_tree_width);
                self.right_panel_file_tree_width = width;
                width
            }
        };
        self.panel_resize_drag = Some(PanelResizeDrag {
            target,
            start_mouse_x: f32::from(event.position.x),
            start_width,
        });
        cx.stop_propagation();
        cx.notify();
    }

    pub(super) fn resize_panel_mouse_move(
        &mut self,
        event: &MouseMoveEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(drag) = self.panel_resize_drag else {
            return;
        };
        let viewport_width = f32::from(window.viewport_size().width);
        let (sidebar_width, right_panel_width) = self.effective_panel_widths(window);
        let delta = f32::from(event.position.x) - drag.start_mouse_x;
        match drag.target {
            PanelResizeTarget::Sidebar => {
                let maximum = SIDEBAR_MAX_WIDTH
                    .min(viewport_width - MAIN_PANEL_MIN_WIDTH - right_panel_width)
                    .max(SIDEBAR_MIN_WIDTH);
                let width = (drag.start_width + delta).clamp(SIDEBAR_MIN_WIDTH, maximum);
                if (self.sidebar_width - width).abs() < 0.5 {
                    return;
                }
                self.sidebar_width = width;
                crate::platform::set_sidebar_material_width(window, width);
            }
            PanelResizeTarget::RightPanel => {
                let maximum = RIGHT_PANEL_MAX_WIDTH
                    .min(viewport_width - MAIN_PANEL_MIN_WIDTH - sidebar_width)
                    .max(RIGHT_PANEL_MIN_WIDTH);
                let width = (drag.start_width - delta).clamp(RIGHT_PANEL_MIN_WIDTH, maximum);
                if (self.right_panel_width - width).abs() < 0.5 {
                    return;
                }
                self.right_panel_width = width;
            }
            PanelResizeTarget::FileTree => {
                let maximum = FILE_TREE_MAX_WIDTH
                    .min(right_panel_width - FILE_EDITOR_MIN_WIDTH)
                    .max(FILE_TREE_MIN_WIDTH);
                let width = (drag.start_width - delta).clamp(FILE_TREE_MIN_WIDTH, maximum);
                if (self.right_panel_file_tree_width - width).abs() < 0.5 {
                    return;
                }
                self.right_panel_file_tree_width = width;
            }
        }
        cx.notify();
    }

    pub(super) fn finish_panel_resize(
        &mut self,
        event: &MouseUpEvent,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if event.button == MouseButton::Left
            && let Some(drag) = self.panel_resize_drag.take()
        {
            if drag.target != PanelResizeTarget::FileTree {
                self.persist_panel_layout();
            }
            cx.notify();
        }
    }

    pub(super) fn navigate_back_action(
        &mut self,
        _: &NavigateBack,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.settings_page.take().is_some() {
            let focus_handle = self.composer_focus(cx);
            window.focus(&focus_handle, cx);
            cx.notify();
            return;
        }

        let Some(current) = self.state.selected_session else {
            return;
        };
        if let Some(target) = self.session_navigation.back_target() {
            self.settings_page = None;
            self.request_session_activation(
                target,
                SessionActivationTransition::Back { from: current },
                cx,
            );
        }
    }

    pub(super) fn navigate_forward_action(
        &mut self,
        _: &NavigateForward,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.settings_page.is_some() {
            return;
        }

        let Some(current) = self.state.selected_session else {
            return;
        };
        if let Some(target) = self.session_navigation.forward_target() {
            self.settings_page = None;
            self.request_session_activation(
                target,
                SessionActivationTransition::Forward { from: current },
                cx,
            );
        }
    }

    pub(super) fn navigation_mouse_down(
        &mut self,
        event: &MouseDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event.button {
            MouseButton::Navigate(NavigationDirection::Back) => {
                cx.stop_propagation();
                self.navigate_back_action(&NavigateBack, window, cx);
            }
            MouseButton::Navigate(NavigationDirection::Forward) => {
                cx.stop_propagation();
                self.navigate_forward_action(&NavigateForward, window, cx);
            }
            _ => {}
        }
    }

    pub(super) fn focus_composer_action(
        &mut self,
        _: &FocusComposer,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.settings_page = None;
        let focus_handle = self.composer_focus(cx);
        window.focus(&focus_handle, cx);
        cx.notify();
    }

    pub(super) fn cancel_turn_action(
        &mut self,
        _: &CancelTurn,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // The switcher focus lands after its deferred overlay is painted.
        // Route the root Escape action here too so an immediate press always
        // cancels the provisional selection instead of reaching the session.
        if self.task_switcher.is_open() {
            self.cancel_task_switcher(window, cx);
            return;
        }
        if self.settings_page.take().is_some() {
            let focus_handle = self.composer_focus(cx);
            window.focus(&focus_handle, cx);
            cx.notify();
            return;
        }
        if self.message_edit.is_some() {
            self.cancel_message_edit(window, cx);
            return;
        }
        let Some(target) = self.selected_escape_stop_target() else {
            self.cancel_turn(cx);
            return;
        };
        match self.escape_stop_confirmation.press(target, Instant::now()) {
            EscapeStopPress::Stop => self.cancel_turn(cx),
            EscapeStopPress::Arm(arm) => {
                cx.notify();
                cx.spawn(async move |this, cx| {
                    cx.background_executor()
                        .timer(ESCAPE_STOP_CONFIRMATION_TIMEOUT)
                        .await;
                    let _ = this.update(cx, |this, cx| {
                        if this.escape_stop_confirmation.expire(arm) {
                            cx.notify();
                        }
                    });
                })
                .detach();
            }
        }
    }

    fn selected_escape_stop_target(&self) -> Option<EscapeStopTarget> {
        let session = self.selected_session()?;
        session
            .status
            .is_busy()
            .then(|| EscapeStopTarget::for_session(session))
    }

    pub(super) fn reset_visible_state(&mut self) {
        self.activities_expanded.clear();
        self.expanded_activity_items.clear();
        self.expanded_turns.clear();
        self.expanded_changed_files.clear();
        self.transcript_control_focuses.borrow_mut().clear();
        self.user_message_viewports.borrow_mut().clear();
        self.hovered_response_row = None;
        // Selection belongs to the session being left.
        self.transcript_selection.selection.borrow_mut().clear();
        self.transcript_selection.registry.borrow_mut().clear();
        self.reset_transcript_search_for_session();
        let (streaming_messages, live_reasoning) = self.selected_session().map_or_else(
            || (Vec::new(), Vec::new()),
            |session| {
                let messages = session
                    .messages
                    .iter()
                    .filter(|message| message.role == MessageRole::Assistant && message.streaming)
                    .map(|message| message.id)
                    .collect();
                let reasoning = session
                    .transcript_blocks
                    .iter()
                    .flat_map(|block| &block.activities)
                    .filter(|activity| activity.reasoning.is_some() && !activity.complete)
                    .map(|activity| activity.id)
                    .collect();
                (messages, reasoning)
            },
        );
        // Parsed messages are keyed by message id, which is unique across
        // sessions, so they stay cached — switching back to a recent session
        // then costs no re-parse. The LRU evicts what was drawn longest ago
        // once the cache passes its byte budget, rather than dropping
        // everything.
        let mut message_markdown = self.message_markdown.borrow_mut();
        message_markdown.trim();
        for id in streaming_messages {
            message_markdown
                .get_or_insert_with(id, MarkdownView::default)
                .seed_streaming_baseline();
        }
        drop(message_markdown);
        // Reasoning parses are keyed by (session, activity), so they survive a
        // switch too. A live block restarts from a seeded view, because its
        // byte window (`reasoning_window_starts`) is reset below.
        let mut activity_markdown = self.activity_markdown.borrow_mut();
        activity_markdown.trim();
        if let Some(session_id) = self.state.selected_session {
            for id in live_reasoning {
                activity_markdown.insert((session_id, id), MarkdownView::seeded());
            }
        }
        drop(activity_markdown);
        self.reasoning_window_starts.borrow_mut().clear();
        self.activity_scroll_viewports.borrow_mut().clear();
        self.menus.borrow_mut().clear();
        self.message_edit = None;
        self.hide_toast();
        self.navigation_rail_reset_generation
            .set(self.navigation_rail_reset_generation.get().wrapping_add(1));
        self.transcript_anchor.set(None);
        self.transcript_anchor_end_space.set(Pixels::ZERO);
        self.transcript_anchor_following.set(false);
    }

    fn remember_selected_model_traits(&mut self) {
        let Some((provider, model, reasoning_effort, service_tier, context_window)) =
            self.selected_session().and_then(|session| {
                Some((
                    session.provider,
                    self.model_for_session(session)?.to_owned(),
                    session.reasoning_effort.clone(),
                    session.service_tier.clone(),
                    session.context_window.clone(),
                ))
            })
        else {
            return;
        };
        self.state.remember_model_traits(
            provider,
            &model,
            reasoning_effort,
            service_tier,
            context_window,
        );
    }

    pub(super) fn choose_model(
        &mut self,
        provider: ProviderKind,
        model: String,
        cx: &mut Context<Self>,
    ) {
        let Some(provider_changed) = self
            .selected_session()
            .filter(|session| {
                session.can_choose_model(provider)
                    && (session.provider != provider
                        || session.model.as_deref() != Some(model.as_str()))
            })
            .map(|session| session.provider != provider)
        else {
            return;
        };
        self.remember_selected_model_traits();
        let (reasoning_effort, service_tier, context_window) =
            self.state.model_traits_for(provider, &model);
        self.state.last_provider = provider;
        self.state.last_model = Some(model.clone());
        self.state.last_reasoning_effort = reasoning_effort.clone();
        self.state.last_service_tier = service_tier.clone();
        self.state.last_context_window = context_window.clone();
        self.model_picker_tab = ModelPickerTab::Provider(provider);
        self.update_selection(
            move |selection| {
                selection.provider = provider;
                selection.model = Some(model);
                if provider_changed {
                    selection.agent_preset = None;
                }
                selection.reasoning_effort = reasoning_effort;
                selection.service_tier = service_tier;
                selection.context_window = context_window;
            },
            cx,
        );
        if provider_changed {
            // A different provider is also a different command registry.
            self.refresh_composer_sources(cx);
        }
        self.save();
        cx.notify();
    }

    /// Change the selected thread's provider, model or per-model options: a
    /// draft edits itself; a thread dispatches
    /// `thread.model_selection.set`, and the next run uses it.
    fn update_selection(&mut self, change: impl FnOnce(&mut Selection), cx: &mut Context<Self>) {
        let Some(session_id) = self.state.selected_session else {
            return;
        };
        if let Some(draft) = self.threads.drafts.get_mut(&session_id) {
            let mut selection = Selection::of(draft);
            change(&mut selection);
            draft.provider = selection.provider;
            draft.instance = waku_client::thread_view::instance_of(selection.provider);
            draft.model = selection.model;
            draft.reasoning_effort = selection.reasoning_effort;
            draft.service_tier = selection.service_tier;
            draft.context_window = selection.context_window;
            draft.agent_preset = selection.agent_preset;
            cx.notify();
            return;
        }
        let Some(view) = self.thread(session_id) else {
            return;
        };
        let mut selection = Selection::of(view);
        change(&mut selection);
        if selection == Selection::of(view) {
            return;
        }
        self.dispatch(
            commands::set_selection(session_id, &selection),
            ThreadAction::Other,
        );
    }

    /// Primary modifier + /: toggle the composer's model picker as if its chip were clicked.
    pub(super) fn toggle_model_picker_action(
        &mut self,
        _: &ToggleModelPicker,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.settings_page.is_some() {
            return;
        }
        if !self
            .selected_session()
            .is_some_and(|session| session.can_choose_model(session.provider))
        {
            return;
        }
        let menus = self.menus.borrow();
        let Some(handle) = menus.get(MODEL_PICKER_MENU_ID).cloned() else {
            return;
        };
        // A keyboard toggle produces no mouse-down for another open menu's
        // dismiss-on-down-out to see, so close the rest here.
        let other_open: Vec<_> = menus
            .iter()
            .filter(|(id, other)| id.as_ref() != MODEL_PICKER_MENU_ID && other.is_open())
            .map(|(_, other)| other.clone())
            .collect();
        drop(menus);
        // The picker's toggle observers update this entity, so the toggle has
        // to run after this listener releases it.
        window.defer(cx, move |window, cx| {
            for menu in other_open {
                menu.close(window, cx);
            }
            crate::ui::menu::toggle_popover(&handle, MenuAlign::AboveLeft, window, cx);
        });
    }

    /// Discovery is not requested here: launch already requested it for every
    /// installed provider, so tabs only ever switch between loaded lists.
    pub(super) fn select_model_picker_tab(&mut self, tab: ModelPickerTab, cx: &mut Context<Self>) {
        if self.model_picker_tab != tab {
            self.model_picker_tab = tab;
            if let ModelPickerTab::Provider(provider) = tab {
                // Selecting a rail re-runs that provider's catalog discovery,
                // so each tab is fresh when viewed without probing every
                // provider on open.
                self.refresh_provider_model_discovery(provider);
            }
            // A different tab renumbers the rows under the keyboard cursor,
            // and would otherwise inherit the old tab's scroll offset.
            self.model_picker_highlight = None;
            self.reveal_selected_picker_model();
            cx.notify();
        }
    }

    /// A sidebar rail click is an explicit "show me this tab": it exits search
    /// mode even when the clicked tab is already selected. A live query spans
    /// every provider and hides which tab is selected, so a click that left
    /// the query in place would visibly do nothing — `select_model_picker_tab`
    /// also bails when the tab is unchanged, which is exactly the state that
    /// query leaves it in. Clearing the field first emits an edit, which
    /// resets the keyboard highlight and re-reveals the current model under
    /// the now-unfiltered list.
    pub(super) fn select_model_picker_tab_from_rail(
        &mut self,
        tab: ModelPickerTab,
        cx: &mut Context<Self>,
    ) {
        self.model_search.update(cx, |input, cx| input.clear(cx));
        self.select_model_picker_tab(tab, cx);
    }

    pub(super) fn toggle_favorite_model(
        &mut self,
        provider: ProviderKind,
        model: String,
        cx: &mut Context<Self>,
    ) {
        if let Some(index) = self
            .state
            .favorite_models
            .iter()
            .position(|favorite| favorite.provider == provider && favorite.model == model)
        {
            self.state.favorite_models.remove(index);
        } else {
            self.state
                .favorite_models
                .push(FavoriteModel { provider, model });
        }
        self.save();
        cx.notify();
    }

    pub(super) fn set_runtime_mode(&mut self, mode: RuntimeMode, cx: &mut Context<Self>) {
        let Some((session_id, session_changed)) = self
            .selected_session()
            .map(|session| (session.id, session.runtime_mode != mode))
        else {
            return;
        };
        let remembered_changed = self.state.last_runtime_mode != mode;
        if session_changed {
            if let Some(draft) = self.threads.drafts.get_mut(&session_id) {
                draft.runtime_mode = mode;
            } else {
                self.dispatch(
                    commands::set_runtime_mode(session_id, mode),
                    ThreadAction::Other,
                );
            }
        }
        if session_changed || remembered_changed {
            self.state.last_runtime_mode = mode;
            self.save();
            cx.notify();
        }
    }

    pub(super) fn set_reasoning_effort(&mut self, effort: String, cx: &mut Context<Self>) {
        if self
            .selected_session()
            .is_none_or(|session| session.reasoning_effort.as_deref() == Some(effort.as_str()))
        {
            return;
        }
        self.state.last_reasoning_effort = Some(effort.clone());
        self.update_selection(|selection| selection.reasoning_effort = Some(effort), cx);
        self.remember_selected_model_traits();
        self.save();
        cx.notify();
    }

    pub(super) fn set_service_tier(&mut self, tier: String, cx: &mut Context<Self>) {
        if self
            .selected_session()
            .is_none_or(|session| session.service_tier.as_deref() == Some(tier.as_str()))
        {
            return;
        }
        self.state.last_service_tier = Some(tier.clone());
        self.update_selection(|selection| selection.service_tier = Some(tier), cx);
        self.remember_selected_model_traits();
        self.save();
        cx.notify();
    }

    pub(super) fn set_context_window(&mut self, window: String, cx: &mut Context<Self>) {
        if self
            .selected_session()
            .is_none_or(|session| session.context_window.as_deref() == Some(window.as_str()))
        {
            return;
        }
        self.state.last_context_window = Some(window.clone());
        self.update_selection(|selection| selection.context_window = Some(window), cx);
        self.remember_selected_model_traits();
        self.save();
        cx.notify();
    }

    pub(super) fn set_agent_preset(&mut self, agent_preset: String, cx: &mut Context<Self>) {
        let selectable = self
            .provider_probe(ProviderKind::DeepSeek)
            .is_some_and(|probe| {
                probe
                    .agent_presets
                    .iter()
                    .any(|preset| preset.id == agent_preset)
            });
        if !selectable
            || !self.selected_session().is_some_and(|session| {
                session.provider == ProviderKind::DeepSeek
                    && !session.has_started()
                    && session.agent_preset.as_deref() != Some(agent_preset.as_str())
            })
        {
            return;
        }
        self.update_selection(|selection| selection.agent_preset = Some(agent_preset), cx);
        cx.notify();
    }

    /// `run.interrupt` of the selected thread's active run. The daemon
    /// settles the turn; the view reports it.
    pub(super) fn cancel_turn(&mut self, cx: &mut Context<Self>) {
        self.escape_stop_confirmation.clear();
        let Some(command) = self.selected_session().and_then(commands::interrupt) else {
            return;
        };
        if let Some(view) = self.selected_session() {
            let provider = view.provider.id();
            if let Some(turn) = view.turns.last() {
                self.analytics.track(crate::analytics::Event::TurnFinished {
                    provider,
                    turn_number: turn.turn_count,
                    outcome: crate::analytics::TurnOutcome::Cancelled,
                    duration_seconds: unix_time().saturating_sub(turn.started_at),
                });
            }
        }
        self.dispatch(command, ThreadAction::Other);
        cx.notify();
    }

    pub(super) fn respond_permission(
        &mut self,
        request_id: String,
        option_id: String,
        cx: &mut Context<Self>,
    ) {
        let Some(view) = self.selected_session() else {
            return;
        };
        let thread_id = view.id;
        let provider = view.provider.id();
        let decision = view
            .pending_permission
            .as_ref()
            .and_then(|permission| {
                permission
                    .options
                    .iter()
                    .find(|option| option.id == option_id)
            })
            .map_or(
                "other",
                |option| if option.allow { "allow" } else { "deny" },
            );
        let Some(command) = commands::respond_approval(thread_id, &request_id, &option_id) else {
            return;
        };
        self.dispatch(command, ThreadAction::Other);
        self.analytics
            .track(crate::analytics::Event::PermissionResponded {
                provider,
                kind: "provider",
                decision,
            });
        cx.notify();
    }

    pub(super) fn sync_user_input_answer(&mut self, cx: &mut Context<Self>) {
        let answer = self
            .threads
            .user_input
            .as_ref()
            .and_then(|pending| {
                pending
                    .current_question()
                    .map(|question| (pending, question))
            })
            .and_then(|(pending, question)| pending.custom_answers.get(&question.id))
            .cloned()
            .unwrap_or_default();
        self.user_input_answer
            .update(cx, |input, cx| input.set_content(answer, cx));
    }

    pub(super) fn update_user_input_custom_answer(
        &mut self,
        answer: impl AsRef<str>,
        cx: &mut Context<Self>,
    ) {
        let Some(pending) = self.threads.user_input.as_mut() else {
            return;
        };
        let Some(question_id) = pending
            .current_question()
            .map(|question| question.id.clone())
        else {
            return;
        };
        let answer = answer.as_ref().to_owned();
        if answer.trim().is_empty() {
            pending.custom_answers.remove(&question_id);
        } else {
            pending.custom_answers.insert(question_id.clone(), answer);
            pending.selections.remove(&question_id);
        }
        cx.notify();
    }

    pub(super) fn submit_user_input_custom_answer(
        &mut self,
        answer: String,
        cx: &mut Context<Self>,
    ) {
        if answer.trim().is_empty() {
            return;
        }
        self.update_user_input_custom_answer(answer, cx);
        self.advance_user_input(cx);
    }

    pub(super) fn select_user_input_option(&mut self, label: String, cx: &mut Context<Self>) {
        let Some(pending) = self.threads.user_input.as_mut() else {
            return;
        };
        let Some((question_id, multi_select)) = pending
            .current_question()
            .map(|question| (question.id.clone(), question.multi_select))
        else {
            return;
        };
        let selected = pending.selections.entry(question_id.clone()).or_default();
        if multi_select {
            if let Some(index) = selected.iter().position(|answer| answer == &label) {
                selected.remove(index);
            } else {
                selected.push(label);
            }
        } else {
            selected.clear();
            selected.push(label);
        }
        if selected.is_empty() {
            pending.selections.remove(&question_id);
        }
        pending.custom_answers.remove(&question_id);
        self.user_input_answer
            .update(cx, |input, cx| input.clear(cx));
        cx.notify();
    }

    pub(super) fn previous_user_input(&mut self, cx: &mut Context<Self>) {
        let Some(pending) = self.threads.user_input.as_mut() else {
            return;
        };
        if pending.question_index == 0 {
            return;
        }
        pending.question_index -= 1;
        self.sync_user_input_answer(cx);
        cx.notify();
    }

    pub(super) fn advance_user_input(&mut self, cx: &mut Context<Self>) {
        let should_submit = {
            let Some(pending) = self.threads.user_input.as_mut() else {
                return;
            };
            let Some(question) = pending.current_question() else {
                return;
            };
            let answered = pending
                .custom_answers
                .get(&question.id)
                .is_some_and(|answer| !answer.trim().is_empty())
                || pending
                    .selections
                    .get(&question.id)
                    .is_some_and(|answers| !answers.is_empty());
            if !answered {
                return;
            }
            if pending.question_index + 1 < pending.questions.len() {
                pending.question_index += 1;
                false
            } else {
                true
            }
        };

        if should_submit {
            let Some(thread_id) = self.state.selected_session else {
                return;
            };
            let Some(pending) = self.threads.user_input.take() else {
                return;
            };
            let answers = pending.answers();
            if let Some(command) =
                commands::respond_user_input(thread_id, &pending.request_id, &answers)
            {
                self.dispatch(command, ThreadAction::Other);
            }
            self.user_input_answer
                .update(cx, |input, cx| input.clear(cx));
        } else {
            self.sync_user_input_answer(cx);
        }
        cx.notify();
    }

    pub(super) fn add_project(&mut self, cx: &mut Context<Self>) {
        if self.daemon.is_remote() {
            // The local folder dialog would show this machine's disk; browse
            // the daemon host's instead (#61).
            self.open_remote_folder_picker(cx);
            return;
        }
        let receiver = cx.prompt_for_paths(PathPromptOptions {
            files: false,
            directories: true,
            multiple: false,
            prompt: Some(tr!("project.add_project").into()),
        });
        cx.spawn(async move |this, cx| {
            if let Ok(Ok(Some(paths))) = receiver.await
                && let Some(path) = paths.into_iter().next()
            {
                let _ = this.update(cx, |this, cx| this.add_project_at(path, cx));
            }
        })
        .detach();
    }

    /// Select the project at `path` (a daemon-host path), adding it with
    /// `project.create` and starting a task in it when it is new.
    pub(super) fn add_project_at(&mut self, path: PathBuf, cx: &mut Context<Self>) {
        if let Some(existing) = self.projects().iter().find(|p| p.path == path) {
            self.select_project(existing.id, cx);
            return;
        }
        let project = Project::from_path(path);
        let project_id = project.id;
        self.dispatch(
            commands::create_project(project_id, project.path, project.name),
            ThreadAction::Other,
        );
        self.analytics.track(crate::analytics::Event::ProjectAdded);
        self.state.selected_project = Some(project_id);
        self.create_session_for(project_id, self.state.last_provider, cx);
    }

    pub(super) fn create_projectless_session(&mut self, cx: &mut Context<Self>) {
        if let Some(draft_id) = self
            .threads
            .drafts
            .values()
            .find(|draft| {
                !draft.has_started()
                    && self.projects().iter().any(|project| {
                        project.id == draft.project_id
                            && project.is_projectless()
                            && !crate::projectless::is_legacy_root_path(&project.path)
                    })
            })
            .map(|draft| draft.id)
        {
            self.select_session(draft_id, cx);
            return;
        }

        // Every projectless task gets a folder of its own: the daemon makes
        // it under the projectless root and records the project
        // (`project.create_scratch`). The project row arrives with the
        // shell, like `add_project_at`'s.
        let project_id = Uuid::new_v4();
        self.dispatch(
            commands::create_scratch_project(project_id, None),
            ThreadAction::Other,
        );
        self.state.selected_project = Some(project_id);
        self.create_session_for(project_id, self.state.last_provider, cx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn draft(project_id: Uuid, provider: ProviderKind) -> ThreadView {
        ThreadView::draft(
            Uuid::new_v4(),
            project_id,
            provider,
            RuntimeMode::FullAccess,
            0,
        )
    }

    #[test]
    fn new_task_carries_the_current_tasks_access_mode() {
        let mut current = draft(Uuid::new_v4(), ProviderKind::OpenCode);
        current.runtime_mode = RuntimeMode::Ask;

        assert_eq!(
            new_task_runtime_mode(Some(&current), RuntimeMode::FullAccess),
            RuntimeMode::Ask
        );
        assert_eq!(
            new_task_runtime_mode(None, RuntimeMode::AutoAcceptEdits),
            RuntimeMode::AutoAcceptEdits
        );
    }

    #[test]
    fn new_task_navigation_reuses_a_draft_from_the_current_project() {
        let project_id = Uuid::new_v4();
        let draft = draft(project_id, ProviderKind::Codex);
        let mut started = self::draft(project_id, ProviderKind::Claude);
        started.last_reply_at = Some(1);
        let mut navigation = SessionNavigation::default();

        navigation.remember_new_task(draft.id);
        navigation.visit(Some(draft.id), started.id);

        assert_eq!(
            navigation.remembered_new_task([&draft, &started], project_id),
            Some(draft.id)
        );
    }

    #[test]
    fn new_task_navigation_does_not_reopen_a_draft_from_another_project() {
        let draft = draft(Uuid::new_v4(), ProviderKind::Codex);
        let current_project_id = Uuid::new_v4();
        let mut navigation = SessionNavigation::default();

        navigation.remember_new_task(draft.id);

        assert_eq!(
            navigation.remembered_new_task([&draft], current_project_id),
            None
        );
    }

    #[test]
    fn new_task_navigation_does_not_reopen_a_started_or_removed_draft() {
        let project_id = Uuid::new_v4();
        let mut draft = draft(project_id, ProviderKind::Codex);
        let mut navigation = SessionNavigation::default();
        navigation.remember_new_task(draft.id);

        draft.last_reply_at = Some(1);
        assert_eq!(navigation.remembered_new_task([&draft], project_id), None);

        navigation.remove(draft.id);
        assert_eq!(navigation.new_task, None);
    }
}
