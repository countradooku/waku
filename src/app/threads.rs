//! The desktop's window onto daemon-owned threads (#55).
//!
//! Projects, threads and transcripts live in the daemon. The desktop holds
//! only what [`ProjectionStore`] publishes — the shell's rows and the render
//! views of the threads it has open — plus UI-local state: unsent task
//! drafts, the answers being typed into a question, and which dispatched
//! action a failure belongs to. Every mutation is an `orchestration.dispatch`.
//!
//! The event pump drains the store at most once per commit interval while
//! text streams ([`Waku::drain_store`]), so a provider's chunk rate never
//! becomes the redraw rate; each update's [`ThreadChange`] picks the islands
//! to repaint.

use super::*;
use waku_client::commands;
use waku_client::store::{
    ConnectionState, DispatchTicket, ProjectionStore, StoreNotice, StoreUpdates, ThreadChange,
    describe_error,
};
use waku_client::thread_view::ShellView;

/// What a dispatch was for, so its failure can be undone where the user
/// can see it.
pub(super) enum ThreadAction {
    /// A message typed in the composer: restored there if it fails.
    Send {
        thread_id: Uuid,
        submission: ComposerSubmission,
    },
    /// The `thread.create` of a new task; its draft stays until it works.
    Create { thread_id: Uuid },
    /// The rollback half of a message edit.
    Rewind { thread_id: Uuid },
    /// Anything else: a failure is shown as a toast.
    Other,
}

/// An edited past message waiting for its rollback to apply.
pub(super) struct PendingRewind {
    submission: ComposerSubmission,
    /// The edited turn: it and everything after it are rolled back.
    turn_id: Uuid,
}

pub(super) struct Threads {
    pub(super) store: ProjectionStore,
    pub(super) shell: Arc<ShellView>,
    /// Render views of the threads the store keeps open.
    pub(super) open: HashMap<Uuid, Arc<ThreadView>>,
    /// Tasks being composed, not yet created in the daemon.
    pub(super) drafts: HashMap<Uuid, ThreadView>,
    /// Drafts standing in for imported provider conversations: they go as
    /// soon as the stream shows the thread, history or not.
    pub(super) imports: HashSet<Uuid>,
    actions: HashMap<DispatchTicket, ThreadAction>,
    rewinds: HashMap<Uuid, PendingRewind>,
    pub(super) connection: ConnectionState,
    /// Answers being chosen for the selected thread's pending question.
    pub(super) user_input: Option<PendingUserInput>,
    /// Set by the transcript's scroll handler when the reader nears the top,
    /// so the next frame asks for older history.
    pub(super) near_top: Rc<Cell<bool>>,
}

impl Threads {
    pub(super) fn new(store: ProjectionStore) -> Self {
        Self {
            store,
            shell: Arc::default(),
            open: HashMap::new(),
            drafts: HashMap::new(),
            imports: HashSet::new(),
            actions: HashMap::new(),
            rewinds: HashMap::new(),
            connection: ConnectionState::Connected,
            user_input: None,
            near_top: Rc::new(Cell::new(false)),
        }
    }

    /// The best view of `id`: its open view, its draft, else its shell row.
    pub(super) fn get(&self, id: Uuid) -> Option<&ThreadView> {
        self.drafts
            .get(&id)
            .filter(|_| {
                // A sent draft stands in only until the stream shows it.
                !self
                    .open
                    .get(&id)
                    .is_some_and(|view| view.has_started() || self.imports.contains(&id))
            })
            .or_else(|| self.open.get(&id).map(Arc::as_ref))
            .or_else(|| self.shell.thread(id).map(Arc::as_ref))
    }

    pub(super) fn actions_insert(&mut self, ticket: DispatchTicket, action: ThreadAction) {
        self.actions.insert(ticket, action);
    }

    /// Every thread the sidebar can list: the shell's rows, then drafts.
    /// The daemon's threads, in catalog order (no local drafts).
    #[cfg(feature = "perf-counters")]
    pub(super) fn shell_ids(&self) -> impl Iterator<Item = Uuid> + '_ {
        self.shell.threads.iter().map(|thread| thread.id)
    }

    pub(super) fn listed(&self) -> impl Iterator<Item = &ThreadView> {
        self.shell.threads.iter().map(Arc::as_ref).chain(
            self.drafts
                .values()
                .filter(|draft| self.shell.thread(draft.id).is_none()),
        )
    }
}

/// The composer-draft key of a task: its own once it has started, else its
/// project's new-task draft.
pub(super) fn draft_key(view: &ThreadView) -> crate::persistence::ComposerDraftKey {
    crate::persistence::ComposerDraftKey::for_task(view.has_started(), view.id, view.project_id)
}

/// The windows-free part of applying one thread update: which islands it
/// dirties for the thread that is selected or not, and whether it was a
/// stream commit (live text).
pub(super) fn thread_update_dirty(change: ThreadChange, selected: bool) -> DirtySet {
    if !selected {
        // A background thread's transcript is drawn nowhere; its sidebar
        // row moves through the shell.
        return DirtySet::NONE;
    }
    if change.state || change.snapshot || change.history {
        return DirtySet::ALL;
    }
    DirtySet {
        root: change.text || change.rows || change.tools,
        transcript: change.text || change.rows || change.tools,
        right_panel: change.tools,
        sidebar: false,
    }
}

impl Waku {
    pub(super) fn thread(&self, id: Uuid) -> Option<&ThreadView> {
        self.threads.get(id)
    }

    /// The selected task while it is still an unsent draft: the only kind of
    /// task the desktop edits itself.
    pub(super) fn selected_draft_mut(&mut self) -> Option<&mut ThreadView> {
        let id = self.state.selected_session?;
        self.threads
            .drafts
            .get_mut(&id)
            .filter(|draft| !draft.has_started())
    }

    pub(super) fn projects(&self) -> &[Project] {
        &self.threads.shell.projects
    }

    pub(super) fn project(&self, id: Uuid) -> Option<&Project> {
        self.projects().iter().find(|project| project.id == id)
    }

    /// Dispatch `command`, remembering what to undo if it fails.
    pub(super) fn dispatch(&mut self, command: Command, action: ThreadAction) -> DispatchTicket {
        let ticket = self.threads.store.dispatch(command);
        self.threads.actions.insert(ticket, action);
        ticket
    }

    /// Ask for the selected thread's older history when the reader scrolled
    /// near its top. Called from render; only sends a message.
    pub(super) fn page_older_history(&self) {
        let near_top = self.threads.near_top.replace(false);
        let Some(view) = self
            .selected_session()
            .filter(|view| view.detail_loaded && view.has_more_history && !view.history_loading)
        else {
            return;
        };
        // A window that does not fill the viewport cannot be scrolled up,
        // so it asks too.
        let rows = self.active_transcript_rows();
        let short = rows.item_count() > 0 && rows.max_offset_for_scrollbar().y < px(1.0);
        if near_top || short {
            self.threads.store.load_older(view.id);
        }
    }

    /// Apply what the store published since the last drain. Returns the
    /// islands to repaint and whether live text moved in the selected thread
    /// (a stream commit, which paces the pump).
    pub(super) fn drain_store(&mut self, cx: &mut Context<Self>) -> (DirtySet, bool) {
        let StoreUpdates {
            shell,
            shell_change,
            threads,
            notices,
            connection,
        } = self.threads.store.take_updates();
        let mut dirty = DirtySet::NONE;
        let mut streamed = false;
        let selected = self.state.selected_session;

        if let Some(shell) = shell {
            let previous = std::mem::replace(&mut self.threads.shell, shell);
            self.notify_settled_threads(&previous, cx);
            if shell_change.projects {
                dirty = DirtySet::ALL;
            } else if shell_change.threads {
                dirty.sidebar = true;
                dirty.root = true;
            }
            self.forget_removed_threads(&previous, cx);
        }

        for (id, update) in threads {
            let is_selected = selected == Some(id);
            let previous_kinds = (is_selected && (update.change.state || update.change.snapshot))
                .then(|| self.snapshot_selected_transcript_rows(id))
                .flatten();
            let rows_before = is_selected.then(|| self.transcript_row_count());
            let previous = match update.view {
                Some(view) => self.threads.open.insert(id, view),
                None => self.threads.open.remove(&id),
            };
            let current = self.threads.open.get(&id).cloned();
            if let Some(view) = &current {
                if self.threads.drafts.contains_key(&id)
                    && (view.has_started() || self.threads.imports.remove(&id))
                {
                    self.threads.drafts.remove(&id);
                }
                self.sync_background_work(view);
                self.continue_rewind(view, cx);
            }
            if let Some(pending) = self
                .pending_session_activation
                .filter(|pending| pending.session_id == id)
            {
                self.pending_session_activation = None;
                if current.is_some() {
                    self.finish_session_activation(id, pending.transition, cx);
                    dirty = DirtySet::ALL;
                    continue;
                }
            }
            if !is_selected {
                continue;
            }
            dirty = dirty.union(thread_update_dirty(update.change, true));
            streamed |= update.change.text;
            let was_busy = previous.as_ref().is_some_and(|view| view.is_busy());
            let now_busy = current.as_ref().is_some_and(|view| view.is_busy());
            if was_busy
                && !now_busy
                && let Some(view) = &current
                && let Some(turn) = view.turns.last()
            {
                self.analytics.track(crate::analytics::Event::TurnFinished {
                    provider: view.provider.id(),
                    turn_number: turn.turn_count,
                    outcome: match turn.status {
                        TurnStatus::Failed => crate::analytics::TurnOutcome::Failed,
                        TurnStatus::Interrupted => crate::analytics::TurnOutcome::Cancelled,
                        _ => crate::analytics::TurnOutcome::Completed,
                    },
                    duration_seconds: turn
                        .completed_at
                        .unwrap_or_else(unix_time)
                        .saturating_sub(turn.started_at),
                });
            }
            if was_busy && !now_busy {
                // The agent may have edited files or switched branches.
                self.workspace_queries_stale = true;
                if let Some(view) = &current
                    && cx.active_window().is_some()
                {
                    self.mark_visited(view.id);
                }
            }
            let question_changed = previous
                .as_ref()
                .and_then(|view| view.pending_user_input.as_ref())
                .map(|input| &input.request_id)
                != current
                    .as_ref()
                    .and_then(|view| view.pending_user_input.as_ref())
                    .map(|input| &input.request_id);
            if question_changed {
                self.threads.user_input = current
                    .as_ref()
                    .and_then(|view| view.pending_user_input.as_ref())
                    .map(|input| {
                        PendingUserInput::new(input.request_id.clone(), input.questions.clone())
                    });
                self.user_input_answer
                    .update(cx, |input, cx| input.clear(cx));
            }
            match (&current, rows_before) {
                (None, _) => {
                    // Closed or gone: show the shell row until reopened.
                    self.reset_visible_state();
                    self.reset_transcript_rows(self.transcript_row_count());
                }
                (Some(_), Some(before)) if update.change.history => {
                    // Older rows arrived above the reader: keep their place.
                    let after = self.transcript_row_count();
                    if after > before {
                        self.transcript_rows.splice(0..0, after - before);
                        if self.transcript_anchor.get().is_some() {
                            self.anchored_transcript_rows.splice(0..0, after - before);
                        }
                    } else {
                        self.sync_transcript_rows();
                    }
                }
                (Some(_), _)
                    if update.change.snapshot
                        && previous.as_ref().is_none_or(|old| !old.detail_loaded) =>
                {
                    // The first view of the thread: a document replacement.
                    self.reset_transcript_rows(self.transcript_row_count());
                }
                (Some(_), _) => {
                    if let Some(previous_kinds) = previous_kinds.as_deref() {
                        self.splice_active_transcript_rows_after_visibility_change(previous_kinds);
                    }
                    if update.change.text || update.change.rows {
                        self.remeasure_transcript_tail();
                    } else {
                        self.sync_transcript_rows();
                    }
                }
            }
            // Slash commands and `@` mentions follow the provider, the
            // workspace and the commands the live session reported, which a
            // thread update rarely moves.
            let sources_moved =
                previous
                    .as_ref()
                    .zip(current.as_ref())
                    .is_some_and(|(old, new)| {
                        old.provider != new.provider
                            || old.workspace != new.workspace
                            || old.available_commands != new.available_commands
                    });
            if sources_moved {
                self.refresh_composer_sources(cx);
            }
        }

        for notice in notices {
            self.apply_store_notice(notice, cx);
            dirty = dirty.union(DirtySet {
                root: true,
                ..DirtySet::NONE
            });
        }
        if let Some(connection) = connection {
            if matches!(connection, ConnectionState::Reconnecting { attempt: 1, .. }) {
                self.show_toast(tr!("errors.daemon_reconnecting"));
            }
            self.threads.connection = connection;
            dirty = dirty.union(DirtySet {
                root: true,
                ..DirtySet::NONE
            });
        }
        perf_count!(stream_drain(streamed));
        (dirty, streamed)
    }

    fn apply_store_notice(&mut self, notice: StoreNotice, cx: &mut Context<Self>) {
        match notice {
            StoreNotice::Dispatched { ticket, .. } => {
                if let Some(ThreadAction::Rewind { thread_id }) =
                    self.threads.actions.remove(&ticket)
                    && let Some(view) = self.threads.open.get(&thread_id).cloned()
                {
                    self.continue_rewind(&view, cx);
                }
            }
            StoreNotice::DispatchFailed {
                ticket,
                command_type,
                error,
            } => {
                let message = describe_error(&error);
                match self.threads.actions.remove(&ticket) {
                    Some(ThreadAction::Send {
                        thread_id,
                        submission,
                    }) => {
                        if let Some(draft) = self.threads.drafts.get_mut(&thread_id) {
                            draft.messages = Default::default();
                            draft.turns.clear();
                            draft.status = SessionStatus::Idle;
                            draft.last_reply_at = None;
                        }
                        if self.state.selected_session == Some(thread_id) {
                            self.restore_composer_submission(submission, cx);
                            self.reset_transcript_rows(self.transcript_row_count());
                        }
                        self.show_toast(tr!("errors.send_message", error = message));
                    }
                    Some(ThreadAction::Rewind { thread_id }) => {
                        if let Some(rewind) = self.threads.rewinds.remove(&thread_id) {
                            self.restore_composer_submission(rewind.submission, cx);
                        }
                        self.show_toast(tr!("errors.rewind_task", error = message));
                    }
                    Some(ThreadAction::Create { thread_id }) => {
                        // An import placeholder has nothing to come back to.
                        if self.threads.imports.remove(&thread_id) {
                            self.threads.drafts.remove(&thread_id);
                            self.threads.store.close_thread(thread_id);
                        }
                        self.show_toast(tr!("errors.create_task", error = message));
                    }
                    Some(ThreadAction::Other) | None => {
                        self.show_toast(tr!(
                            "errors.daemon_command",
                            command = command_type,
                            error = message
                        ));
                    }
                }
            }
            StoreNotice::HistoryFailed { error, .. } => {
                self.show_toast(tr!("errors.load_history", error = describe_error(&error)));
            }
        }
    }

    /// A shell row that stopped working while the window was in the
    /// background raises the task notification the provider's finish used
    /// to.
    fn notify_settled_threads(&mut self, previous: &ShellView, cx: &mut Context<Self>) {
        if cx.active_window().is_some() {
            return;
        }
        let settled = self
            .threads
            .shell
            .threads
            .iter()
            .filter(|row| !row.is_busy())
            .filter(|row| {
                previous
                    .thread(row.id)
                    .is_some_and(|before| before.is_busy())
            })
            .map(|row| {
                (
                    row.id,
                    row.provider,
                    row.display_title().to_owned(),
                    row.status == SessionStatus::Failed,
                )
            })
            .collect::<Vec<_>>();
        for (id, provider, title, failed) in settled {
            if usage_meter::PLAN_USAGE_PROVIDERS.contains(&provider) {
                self.plan_usage_stale.insert(provider);
            }
            let title = if title == waku_client::thread_view::DEFAULT_TITLE {
                tr!("session.new_task")
            } else {
                title
            };
            let body = if failed {
                tr!("session.stopped")
            } else {
                tr!("session.turn_completed")
            };
            crate::platform::show_task_notification(&task_notification_tag(id), &title, &body, cx);
        }
    }

    /// Threads that left the shell (deleted, archived elsewhere) leave every
    /// UI-local table, and a selection on one moves to a neighbour.
    fn forget_removed_threads(&mut self, previous: &ShellView, cx: &mut Context<Self>) {
        let removed = previous
            .threads
            .iter()
            .filter(|row| self.threads.shell.thread(row.id).is_none())
            .map(|row| (row.id, row.project_id))
            .collect::<Vec<_>>();
        for (id, project_id) in removed {
            self.threads.open.remove(&id);
            self.threads.store.close_thread(id);
            self.background_work.remove(&id);
            self.remove_right_panel_session_state(id);
            self.task_switcher.remove(id);
            self.session_navigation.remove(id);
            if self.state.selected_session == Some(id) {
                self.select_neighbour(project_id, cx);
            }
        }
    }

    /// Select the newest thread of `project_id`, else a fresh draft there.
    pub(super) fn select_neighbour(&mut self, project_id: Uuid, cx: &mut Context<Self>) {
        self.state.selected_session = None;
        let next = self
            .threads
            .shell
            .threads
            .iter()
            .filter(|row| row.project_id == project_id)
            .max_by_key(|row| row.last_reply_at.unwrap_or(row.updated_at))
            .map(|row| row.id);
        match next {
            Some(id) => self.select_session(id, cx),
            None if self.project(project_id).is_some() => {
                self.create_session_for(project_id, self.state.last_provider, cx)
            }
            None => {
                if let Some(project_id) = self.projects().first().map(|project| project.id) {
                    self.create_session_for(project_id, self.state.last_provider, cx);
                }
            }
        }
    }

    /// `thread.visit`: the user has seen the thread's latest reply.
    pub(super) fn mark_visited(&mut self, thread_id: Uuid) {
        if self.threads.drafts.contains_key(&thread_id)
            || !self
                .threads
                .shell
                .thread(thread_id)
                .is_some_and(|row| row.unread)
        {
            return;
        }
        let now = chrono::Utc::now().timestamp_millis();
        self.dispatch(commands::visit(thread_id, now), ThreadAction::Other);
    }

    /// Mirror the view's subagents and detached work into the background
    /// work registry the header and right panel draw.
    fn sync_background_work(&mut self, view: &ThreadView) {
        if view.background_work.is_empty() && !self.background_work.contains_key(&view.id) {
            return;
        }
        self.background_work
            .entry(view.id)
            .or_default()
            .reconcile_view(view.background_work.clone());
    }

    /// Start an edited past message: roll the thread back to before the
    /// edited turn, then (once the rollback applied) send the new text.
    pub(super) fn start_rewind(
        &mut self,
        thread_id: Uuid,
        turn_id: Uuid,
        submission: ComposerSubmission,
        cx: &mut Context<Self>,
    ) {
        let Some(view) = self.thread(thread_id) else {
            return;
        };
        let provider = view.provider.id();
        let rolled_back = view.turn_ordinal(turn_id).map_or(0, |ordinal| {
            view.turns.len().saturating_sub(ordinal as usize - 1)
        });
        match commands::rewind(view, turn_id) {
            Ok(command) => {
                self.analytics
                    .track(crate::analytics::Event::ConversationRolledBack {
                        provider,
                        turns: rolled_back,
                    });
                self.threads.rewinds.insert(
                    thread_id,
                    PendingRewind {
                        submission,
                        turn_id,
                    },
                );
                self.dispatch(command, ThreadAction::Rewind { thread_id });
            }
            Err(commands::RewindError::Busy) => {
                self.restore_composer_submission(submission, cx);
                self.show_toast(tr!("session.stop_before_rewind"));
            }
            Err(commands::RewindError::NoCheckpoint) => {
                self.restore_composer_submission(submission, cx);
                self.show_toast(tr!("session.rewind_unavailable"));
            }
        }
        cx.notify();
    }

    /// Send a rewound message once its rollback has applied: the edited
    /// turn is gone from the view and no rollback is in flight.
    fn continue_rewind(&mut self, view: &ThreadView, cx: &mut Context<Self>) {
        let Some(rewind) = self.threads.rewinds.get(&view.id) else {
            return;
        };
        if let Some(failure) = view
            .rollback
            .as_ref()
            .and_then(|state| state.failure.clone())
        {
            let rewind = self.threads.rewinds.remove(&view.id).expect("present");
            self.restore_composer_submission(rewind.submission, cx);
            self.show_toast(tr!("errors.rewind_task", error = failure));
            return;
        }
        let applied =
            view.rollback.is_none() && !view.turns.iter().any(|turn| turn.id == rewind.turn_id);
        if !applied {
            return;
        }
        let rewind = self.threads.rewinds.remove(&view.id).expect("present");
        let command = commands::send_message(
            view.id,
            &rewind.submission.outgoing(),
            DispatchMode::QueueAfterActive,
            None,
        );
        self.dispatch(
            command,
            ThreadAction::Send {
                thread_id: view.id,
                submission: rewind.submission,
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn change(apply: impl FnOnce(&mut ThreadChange)) -> ThreadChange {
        let mut change = ThreadChange::default();
        apply(&mut change);
        change
    }

    #[test]
    fn streaming_text_dirties_only_the_transcript_of_the_selected_thread() {
        let text = change(|change| change.text = true);
        assert_eq!(
            thread_update_dirty(text, true),
            DirtySet {
                root: true,
                transcript: true,
                ..DirtySet::NONE
            }
        );
        assert!(thread_update_dirty(text, false).is_empty());
    }

    #[test]
    fn tool_activity_also_dirties_the_right_panel() {
        let tools = change(|change| {
            change.rows = true;
            change.tools = true;
        });
        assert_eq!(
            thread_update_dirty(tools, true),
            DirtySet {
                root: true,
                transcript: true,
                right_panel: true,
                sidebar: false,
            }
        );
    }

    #[test]
    fn new_rows_without_tools_leave_the_right_panel_alone() {
        let rows = change(|change| change.rows = true);
        let dirty = thread_update_dirty(rows, true);
        assert!(dirty.transcript && !dirty.right_panel && !dirty.sidebar);
    }

    #[test]
    fn run_state_snapshots_and_history_repaint_the_window() {
        for change in [
            change(|change| change.state = true),
            change(|change| change.snapshot = true),
            change(|change| change.history = true),
        ] {
            assert_eq!(thread_update_dirty(change, true), DirtySet::ALL);
            assert!(thread_update_dirty(change, false).is_empty());
        }
    }
}
