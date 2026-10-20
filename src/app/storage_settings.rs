//! Settings → Storage: the managed worktree folder, automatic cleanup rules,
//! and "Delete now" with the last run's report (#62).
//!
//! Every value lives in the daemon's settings document; the page edits the
//! client's mirror and saves it like any other daemon setting. Cleanup runs
//! and status reads go to the daemon off the UI thread.

use super::*;
use crate::ui::ActivationExt as _;
use waku_client::storage::{
    StorageSettings, WorktreeCleanupEntry, WorktreeCleanupOutcome, WorktreeCleanupReason,
    WorktreeCleanupSettings, WorktreeCleanupStatus, WorktreeDirectoryError, WorktreeKeepWhen,
    validate_worktrees_directory,
};

const INACTIVE_DAYS: [u32; 7] = [1, 3, 7, 14, 30, 60, 90];
const FINISHED_DAYS: [u32; 6] = [0, 1, 3, 7, 14, 30];
const INTERVAL_HOURS: [u32; 6] = [1, 6, 12, 24, 72, 168];
/// Report rows drawn at once; the daemon already bounds the report itself.
const VISIBLE_REPORT_ROWS: usize = 50;

/// Page state the Storage settings keep between frames.
pub(super) struct StorageSettingsUi {
    pub(super) location_input: Entity<TextInput>,
    pub(super) location_error: Option<String>,
    /// The daemon's cleanup status; `None` until the first read lands.
    pub(super) status: Option<WorktreeCleanupStatus>,
    pub(super) running: bool,
    pub(super) error: Option<String>,
    /// Bumped per status read so a superseded read cannot land late.
    pub(super) generation: u64,
}

impl StorageSettingsUi {
    pub(super) fn new(input: Entity<TextInput>) -> Self {
        Self {
            location_input: input,
            location_error: None,
            status: None,
            running: false,
            error: None,
            generation: 0,
        }
    }
}

fn days_label(days: u32) -> String {
    if days == 1 {
        tr!("storage.one_day")
    } else {
        tr!("storage.days", count = days)
    }
}

fn days_label_u64(days: u64) -> String {
    u32::try_from(days)
        .map(days_label)
        .unwrap_or_else(|_| tr!("storage.days", count = days))
}

fn interval_label(hours: u32) -> String {
    if hours.is_multiple_of(24) {
        days_label(hours / 24)
    } else if hours == 1 {
        tr!("storage.one_hour")
    } else {
        tr!("storage.hours", count = hours)
    }
}

fn inactive_label(days: Option<u32>) -> String {
    days.map_or_else(|| tr!("storage.off"), days_label)
}

fn finished_label(days: Option<u32>) -> String {
    match days {
        None => tr!("storage.off"),
        Some(0) => tr!("storage.immediately"),
        Some(days) => days_label(days),
    }
}

fn keep_when_label(keep_when: WorktreeKeepWhen) -> String {
    match keep_when {
        WorktreeKeepWhen::AnyLocalFiles => tr!("storage.keep_any_local_files"),
        WorktreeKeepWhen::UncommittedChanges => tr!("storage.keep_uncommitted_changes"),
        WorktreeKeepWhen::TrackedChanges => tr!("storage.keep_tracked_changes"),
    }
}

fn location_error_label(error: WorktreeDirectoryError) -> String {
    match error {
        WorktreeDirectoryError::Empty => tr!("storage.location_empty"),
        WorktreeDirectoryError::NotAbsolute => tr!("storage.location_not_absolute"),
        WorktreeDirectoryError::FilesystemRoot => tr!("storage.location_root"),
    }
}

pub(super) fn cleanup_reason_label(reason: &WorktreeCleanupReason) -> String {
    use WorktreeCleanupReason as Reason;
    match reason {
        Reason::TaskDeleted => tr!("storage.reason_task_deleted"),
        Reason::Inactive { days } => {
            tr!("storage.reason_inactive", duration = days_label_u64(*days))
        }
        Reason::Finished { days } => {
            tr!("storage.reason_finished", duration = days_label_u64(*days))
        }
        Reason::Merged { base } => tr!("storage.reason_merged", base = base),
        Reason::Unchanged { base } => tr!("storage.reason_unchanged", base = base),
        Reason::NoRuleApplies => tr!("storage.reason_no_rule"),
        Reason::TaskRunning => tr!("storage.reason_task_running"),
        Reason::LiveRuntime => tr!("storage.reason_live_runtime"),
        Reason::OpenTerminal => tr!("storage.reason_open_terminal"),
        Reason::OutsideManagedFolder => tr!("storage.reason_outside"),
        Reason::NotWakuBranch => tr!("storage.reason_not_waku_branch"),
        Reason::NotLinkedWorktree => tr!("storage.reason_not_linked"),
        Reason::BranchChanged => tr!("storage.reason_branch_changed"),
        Reason::UncommittedChanges { files } => {
            tr!("storage.reason_uncommitted", count = files)
        }
        Reason::IgnoredFiles { example } => tr!("storage.reason_ignored", example = example),
        Reason::UnpushedCommits { commits } => {
            tr!("storage.reason_unpushed", count = commits)
        }
        Reason::InactiveFor { days, of } => tr!(
            "storage.reason_inactive_for",
            duration = days_label_u64(*days),
            limit = days_label(*of)
        ),
        Reason::FinishedFor { days, of } => tr!(
            "storage.reason_finished_for",
            duration = days_label_u64(*days),
            limit = days_label(*of)
        ),
        Reason::NotFinished => tr!("storage.reason_not_finished"),
        Reason::NotMerged { base } => tr!("storage.reason_not_merged", base = base),
        Reason::HasCommits { commits, base } => {
            tr!("storage.reason_has_commits", count = commits, base = base)
        }
        Reason::BaseUnavailable => tr!("storage.reason_base_unavailable"),
        Reason::ChangedDuringCleanup => tr!("storage.reason_changed"),
        Reason::Error { message } => message.clone(),
    }
}

/// One labelled settings row: title and description on the left, the
/// control on the right.
fn setting_row(theme: Theme, title: String, description: String, control: impl IntoElement) -> Div {
    div()
        .w_full()
        .min_h(px(60.0))
        .px(px(20.0))
        .py(px(12.0))
        .flex()
        .items_center()
        .gap(px(24.0))
        .child(
            div()
                .flex_1()
                .min_w_0()
                .child(
                    div()
                        .text_size(sp(13.5))
                        .font_weight(FontWeight::MEDIUM)
                        .text_color(theme.text)
                        .child(title),
                )
                .child(
                    div()
                        .mt(px(5.0))
                        .whitespace_normal()
                        .text_size(sp(12.5))
                        .line_height(sp(18.0))
                        .text_color(theme.text_secondary)
                        .child(description),
                ),
        )
        .child(control)
}

fn divider(theme: Theme) -> Div {
    div().mx(px(20.0)).h(px(1.0)).bg(theme.border)
}

fn card(theme: Theme) -> Div {
    div()
        .mt(px(15.0))
        .w_full()
        .flex()
        .flex_col()
        .rounded(px(13.0))
        .overflow_hidden()
        .bg(theme.raised)
}

fn section_title(theme: Theme, title: String) -> Div {
    div()
        .mt(px(24.0))
        .px(px(2.0))
        .text_size(sp(12.5))
        .font_weight(FontWeight::MEDIUM)
        .text_color(theme.text_tertiary)
        .child(title)
}

/// A bordered settings button, operable by click, Enter and Space.
fn settings_button<E: 'static>(
    id: &'static str,
    label: String,
    disabled: bool,
    theme: Theme,
    cx: &mut Context<E>,
    activate: impl Fn(&mut E, &mut Window, &mut Context<E>) + 'static,
) -> Stateful<Div> {
    let button = div()
        .id(id)
        .tab_index(0)
        .h(px(29.0))
        .px(px(11.0))
        .flex_none()
        .rounded(px(7.0))
        .border_1()
        .border_color(theme.border_strong)
        .flex()
        .items_center()
        .justify_center()
        .cursor_default()
        .text_size(sp(12.5))
        .text_color(theme.text_secondary)
        .opacity(if disabled { 0.55 } else { 1.0 })
        .focus_visible(|style| style.border_color(theme.accent))
        .child(label);
    if disabled {
        button
    } else {
        button
            .hover(|element| element.bg(theme.overlay))
            .on_activation(cx, activate)
    }
}

impl Waku {
    fn worktree_cleanup_rules(&self) -> &WorktreeCleanupSettings {
        &self.state.daemon_storage.worktree_cleanup
    }

    fn update_storage_settings(
        &mut self,
        edit: impl FnOnce(&mut StorageSettings),
        cx: &mut Context<Self>,
    ) {
        let before = self.state.daemon_storage.clone();
        edit(&mut self.state.daemon_storage);
        if self.state.daemon_storage == before {
            return;
        }
        self.save();
        // The next automatic run may have moved.
        self.ensure_worktree_cleanup_status(cx);
        cx.notify();
    }

    fn update_cleanup_rules(
        &mut self,
        edit: impl FnOnce(&mut WorktreeCleanupSettings),
        cx: &mut Context<Self>,
    ) {
        self.update_storage_settings(|storage| edit(&mut storage.worktree_cleanup), cx);
    }

    /// Validate and save the typed worktree location. An empty field returns
    /// to the default folder.
    pub(super) fn apply_worktree_location(&mut self, cx: &mut Context<Self>) {
        let typed = self
            .storage_settings
            .location_input
            .read(cx)
            .content()
            .to_owned();
        let location = if typed.trim().is_empty() {
            None
        } else {
            match validate_worktrees_directory(&typed, dirs::home_dir().as_deref()) {
                Ok(path) => Some(path),
                Err(error) => {
                    self.storage_settings.location_error = Some(location_error_label(error));
                    cx.notify();
                    return;
                }
            }
        };
        self.storage_settings.location_error = None;
        self.update_storage_settings(|storage| storage.worktrees_directory = location, cx);
        cx.notify();
    }

    fn reset_worktree_location(&mut self, cx: &mut Context<Self>) {
        self.storage_settings
            .location_input
            .update(cx, |input, cx| input.set_content("", cx));
        self.storage_settings.location_error = None;
        self.update_storage_settings(|storage| storage.worktrees_directory = None, cx);
        cx.notify();
    }

    /// Mirror the saved location into the field when the page opens.
    pub(super) fn sync_worktree_location_input(&mut self, cx: &mut Context<Self>) {
        let saved = self
            .state
            .daemon_storage
            .worktrees_directory
            .as_ref()
            .map(|path| path.to_string_lossy().into_owned())
            .unwrap_or_default();
        self.storage_settings.location_error = None;
        self.storage_settings
            .location_input
            .update(cx, |input, cx| input.set_content(saved, cx));
    }

    fn worktree_location_dirty(&self, cx: &App) -> bool {
        let typed = self
            .storage_settings
            .location_input
            .read(cx)
            .content()
            .trim();
        let saved = self
            .state
            .daemon_storage
            .worktrees_directory
            .as_ref()
            .map(|path| path.to_string_lossy().into_owned())
            .unwrap_or_default();
        typed != saved
    }

    /// Read the daemon's cleanup status in the background.
    pub(super) fn ensure_worktree_cleanup_status(&mut self, cx: &mut Context<Self>) {
        self.storage_settings.generation += 1;
        let generation = self.storage_settings.generation;
        let daemon = self.daemon.client();
        cx.spawn(async move |this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move {
                    match daemon.request(
                        Uuid::nil(),
                        Uuid::nil(),
                        waku_client::Command::GetWorktreeCleanupStatus,
                    )? {
                        waku_client::ResponsePayload::WorktreeCleanupStatus { status } => {
                            Ok(status)
                        }
                        _ => anyhow::bail!("the daemon returned an invalid cleanup status"),
                    }
                })
                .await;
            let _ = this.update(cx, |this, cx| {
                if this.storage_settings.generation != generation {
                    return;
                }
                match result {
                    Ok(status) => {
                        this.storage_settings.running = status.running;
                        this.storage_settings.status = Some(status);
                    }
                    Err(error) => {
                        this.storage_settings.error =
                            Some(tr!("storage.status_failed", error = error.to_string()));
                    }
                }
                cx.notify();
            });
        })
        .detach();
    }

    /// "Delete now": run cleanup with the saved rules and show its report.
    fn run_worktree_cleanup_now(&mut self, cx: &mut Context<Self>) {
        if self.storage_settings.running {
            return;
        }
        self.storage_settings.running = true;
        self.storage_settings.error = None;
        // Unsaved rule edits do not exist: every control saves on change.
        let daemon = self.daemon.client();
        cx.spawn(async move |this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move {
                    match daemon.request(
                        Uuid::nil(),
                        Uuid::nil(),
                        waku_client::Command::RunWorktreeCleanup,
                    )? {
                        waku_client::ResponsePayload::WorktreeCleanupReport { report } => {
                            Ok(report)
                        }
                        _ => anyhow::bail!("the daemon returned an invalid cleanup report"),
                    }
                })
                .await;
            let _ = this.update(cx, |this, cx| {
                this.storage_settings.running = false;
                match result {
                    Ok(report) => {
                        let status = this
                            .storage_settings
                            .status
                            .get_or_insert_with(WorktreeCleanupStatus::default);
                        status.last_run_at = Some(report.finished_at);
                        status.last_report = Some(report);
                        status.running = false;
                    }
                    Err(error) => {
                        this.storage_settings.error =
                            Some(tr!("storage.run_failed", error = error.to_string()));
                    }
                }
                // Pick up the next automatic run time.
                this.ensure_worktree_cleanup_status(cx);
                cx.notify();
            });
        })
        .detach();
        cx.notify();
    }

    pub(super) fn render_storage_settings(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = Theme::current(cx);
        div()
            .child(section_title(theme, tr!("storage.worktrees_title")))
            .child(self.render_worktree_location(theme, cx))
            .child(section_title(theme, tr!("storage.cleanup_title")))
            .child(
                div()
                    .mt(px(6.0))
                    .px(px(2.0))
                    .whitespace_normal()
                    .text_size(sp(12.5))
                    .line_height(sp(18.0))
                    .text_color(theme.text_secondary)
                    .child(tr!("storage.cleanup_description")),
            )
            .child(self.render_cleanup_rules(theme, cx))
            .child(self.render_cleanup_safety(theme, cx))
            .child(section_title(theme, tr!("storage.run_title")))
            .child(self.render_cleanup_run(theme, cx))
            .into_any_element()
    }

    fn render_worktree_location(&self, theme: Theme, cx: &mut Context<Self>) -> Div {
        let dirty = self.worktree_location_dirty(cx);
        let is_default = self.state.daemon_storage.worktrees_directory.is_none();
        let apply = settings_button(
            "apply-worktree-location",
            tr!("storage.location_apply"),
            !dirty,
            theme,
            cx,
            |this, _, cx| this.apply_worktree_location(cx),
        );
        let reset = settings_button(
            "reset-worktree-location",
            tr!("storage.location_reset"),
            is_default,
            theme,
            cx,
            |this, _, cx| this.reset_worktree_location(cx),
        );
        card(theme).child(
            div()
                .px(px(20.0))
                .py(px(14.0))
                .child(
                    div()
                        .text_size(sp(13.5))
                        .font_weight(FontWeight::MEDIUM)
                        .text_color(theme.text)
                        .child(tr!("storage.location")),
                )
                .child(
                    div()
                        .mt(px(5.0))
                        .whitespace_normal()
                        .text_size(sp(12.5))
                        .line_height(sp(18.0))
                        .text_color(theme.text_secondary)
                        .child(tr!("storage.location_description")),
                )
                .child(
                    div()
                        .mt(px(12.0))
                        .flex()
                        .items_center()
                        .gap(px(8.0))
                        .child(
                            div().flex_1().min_w_0().child(
                                TextField::new(
                                    "worktree-location-field",
                                    self.storage_settings.location_input.clone(),
                                )
                                .w_full(),
                            ),
                        )
                        .child(apply)
                        .child(reset),
                )
                .children(self.storage_settings.location_error.clone().map(|error| {
                    div()
                        .mt(px(8.0))
                        .flex()
                        .items_center()
                        .gap(px(6.0))
                        .text_size(sp(12.5))
                        .text_color(theme.danger)
                        .child(icon("icons/alert.svg", 12.0, theme.danger))
                        .child(error)
                })),
        )
    }

    fn render_cleanup_rules(&self, theme: Theme, cx: &mut Context<Self>) -> Div {
        let rules = self.worktree_cleanup_rules().clone();
        let weak = cx.entity().downgrade();

        let inactive_handle = self.menu_handle("storage-inactive-selector", cx);
        let inactive_selector = {
            let weak = weak.clone();
            let selected = rules.inactive_after_days;
            dropdown_menu(
                MenuChip::new("storage-inactive-selector")
                    .label(inactive_label(selected))
                    .outlined()
                    .selected(inactive_handle.is_open())
                    .w(px(116.0))
                    .justify_between(),
                "storage-inactive-selector-menu",
                &inactive_handle,
                MenuAlign::BelowRight,
                move |_| {
                    std::iter::once(None)
                        .chain(INACTIVE_DAYS.into_iter().map(Some))
                        .map(|days| {
                            let weak = weak.clone();
                            MenuItem::new(inactive_label(days), move |_, cx| {
                                let _ = weak.update(cx, |this, cx| {
                                    this.update_cleanup_rules(
                                        |rules| rules.inactive_after_days = days,
                                        cx,
                                    );
                                });
                            })
                            .selected(days == selected)
                        })
                        .collect()
                },
            )
        };

        let finished_handle = self.menu_handle("storage-finished-selector", cx);
        let finished_selector = {
            let weak = weak.clone();
            let selected = rules.finished_after_days;
            dropdown_menu(
                MenuChip::new("storage-finished-selector")
                    .label(finished_label(selected))
                    .outlined()
                    .selected(finished_handle.is_open())
                    .w(px(116.0))
                    .justify_between(),
                "storage-finished-selector-menu",
                &finished_handle,
                MenuAlign::BelowRight,
                move |_| {
                    std::iter::once(None)
                        .chain(FINISHED_DAYS.into_iter().map(Some))
                        .map(|days| {
                            let weak = weak.clone();
                            MenuItem::new(finished_label(days), move |_, cx| {
                                let _ = weak.update(cx, |this, cx| {
                                    this.update_cleanup_rules(
                                        |rules| rules.finished_after_days = days,
                                        cx,
                                    );
                                });
                            })
                            .selected(days == selected)
                        })
                        .collect()
                },
            )
        };

        let toggle = |id: &'static str,
                      on: bool,
                      cx: &mut Context<Self>,
                      set: fn(&mut WorktreeCleanupSettings, bool)| {
            toggle_switch(id, on, false, theme, cx, move |this, _, cx| {
                this.update_cleanup_rules(|rules| set(rules, !on), cx);
            })
        };
        let merged_toggle = toggle("storage-merged-toggle", rules.merged, cx, |rules, on| {
            rules.merged = on
        });
        let unchanged_toggle = toggle(
            "storage-unchanged-toggle",
            rules.unchanged,
            cx,
            |rules, on| rules.unchanged = on,
        );
        let deleted_toggle = toggle(
            "storage-deleted-toggle",
            rules.deleted_tasks,
            cx,
            |rules, on| rules.deleted_tasks = on,
        );

        card(theme)
            .child(setting_row(
                theme,
                tr!("storage.inactive"),
                tr!("storage.inactive_description"),
                inactive_selector,
            ))
            .child(divider(theme))
            .child(setting_row(
                theme,
                tr!("storage.finished"),
                tr!("storage.finished_description"),
                finished_selector,
            ))
            .child(divider(theme))
            .child(setting_row(
                theme,
                tr!("storage.merged"),
                tr!("storage.merged_description"),
                merged_toggle,
            ))
            .child(divider(theme))
            .child(setting_row(
                theme,
                tr!("storage.unchanged"),
                tr!("storage.unchanged_description"),
                unchanged_toggle,
            ))
            .child(divider(theme))
            .child(setting_row(
                theme,
                tr!("storage.deleted"),
                tr!("storage.deleted_description"),
                deleted_toggle,
            ))
    }

    fn render_cleanup_safety(&self, theme: Theme, cx: &mut Context<Self>) -> Div {
        let rules = self.worktree_cleanup_rules().clone();
        let weak = cx.entity().downgrade();

        let keep_handle = self.menu_handle("storage-keep-selector", cx);
        let keep_selector = {
            let weak = weak.clone();
            let selected = rules.keep_when;
            dropdown_menu(
                MenuChip::new("storage-keep-selector")
                    .label(keep_when_label(selected))
                    .outlined()
                    .selected(keep_handle.is_open())
                    .w(px(176.0))
                    .justify_between(),
                "storage-keep-selector-menu",
                &keep_handle,
                MenuAlign::BelowRight,
                move |_| {
                    WorktreeKeepWhen::ALL
                        .into_iter()
                        .map(|keep_when| {
                            let weak = weak.clone();
                            MenuItem::new(keep_when_label(keep_when), move |_, cx| {
                                let _ = weak.update(cx, |this, cx| {
                                    this.update_cleanup_rules(
                                        |rules| rules.keep_when = keep_when,
                                        cx,
                                    );
                                });
                            })
                            .selected(keep_when == selected)
                        })
                        .collect()
                },
            )
        };

        let interval_handle = self.menu_handle("storage-interval-selector", cx);
        let interval_selector = {
            let weak = weak.clone();
            let selected = rules.interval_hours;
            dropdown_menu(
                MenuChip::new("storage-interval-selector")
                    .label(interval_label(selected))
                    .outlined()
                    .selected(interval_handle.is_open())
                    .w(px(116.0))
                    .justify_between(),
                "storage-interval-selector-menu",
                &interval_handle,
                MenuAlign::BelowRight,
                move |_| {
                    INTERVAL_HOURS
                        .into_iter()
                        .map(|hours| {
                            let weak = weak.clone();
                            MenuItem::new(interval_label(hours), move |_, cx| {
                                let _ = weak.update(cx, |this, cx| {
                                    this.update_cleanup_rules(
                                        |rules| rules.interval_hours = hours,
                                        cx,
                                    );
                                });
                            })
                            .selected(hours == selected)
                        })
                        .collect()
                },
            )
        };

        let unpushed = rules.remove_unpushed_commits;
        let unpushed_toggle = toggle_switch(
            "storage-unpushed-toggle",
            unpushed,
            false,
            theme,
            cx,
            move |this, _, cx| {
                this.update_cleanup_rules(|rules| rules.remove_unpushed_commits = !unpushed, cx);
            },
        );

        card(theme)
            .child(setting_row(
                theme,
                tr!("storage.keep_when"),
                tr!("storage.keep_when_description"),
                keep_selector,
            ))
            .child(divider(theme))
            .child(setting_row(
                theme,
                tr!("storage.unpushed"),
                tr!("storage.unpushed_description"),
                unpushed_toggle,
            ))
            .child(divider(theme))
            .child(setting_row(
                theme,
                tr!("storage.interval"),
                tr!("storage.interval_description"),
                interval_selector,
            ))
    }

    fn render_cleanup_run(&self, theme: Theme, cx: &mut Context<Self>) -> Div {
        let ui = &self.storage_settings;
        let running = ui.running;
        let delete_now = settings_button(
            "storage-delete-now",
            if running {
                tr!("storage.deleting")
            } else {
                tr!("storage.delete_now")
            },
            running,
            theme,
            cx,
            |this, _, cx| this.run_worktree_cleanup_now(cx),
        );
        let status = ui.status.as_ref();
        let last_run = match status.and_then(|status| status.last_run_at) {
            Some(at) => tr!(
                "storage.last_run",
                time = super::components::format_message_time(at)
            ),
            None => tr!("storage.never_run"),
        };
        let next_run = match status {
            _ if !self.worktree_cleanup_rules().any_rule_enabled() => {
                tr!("storage.automatic_off")
            }
            Some(WorktreeCleanupStatus {
                next_run_at: Some(at),
                ..
            }) if *at > unix_time() => tr!(
                "storage.next_run",
                time = super::components::format_message_time(*at)
            ),
            _ => tr!("storage.next_run_soon"),
        };

        let mut column = card(theme).child(
            div()
                .w_full()
                .min_h(px(60.0))
                .px(px(20.0))
                .py(px(12.0))
                .flex()
                .items_center()
                .gap(px(24.0))
                .child(
                    div()
                        .flex_1()
                        .min_w_0()
                        .child(
                            div()
                                .text_size(sp(13.5))
                                .font_weight(FontWeight::MEDIUM)
                                .text_color(theme.text)
                                .child(last_run),
                        )
                        .child(
                            div()
                                .mt(px(5.0))
                                .whitespace_normal()
                                .text_size(sp(12.5))
                                .line_height(sp(18.0))
                                .text_color(theme.text_secondary)
                                .child(next_run),
                        ),
                )
                .child(delete_now),
        );
        if let Some(error) = ui.error.clone() {
            column = column.child(divider(theme)).child(
                div()
                    .px(px(20.0))
                    .py(px(10.0))
                    .flex()
                    .items_center()
                    .gap(px(6.0))
                    .whitespace_normal()
                    .text_size(sp(12.5))
                    .text_color(theme.danger)
                    .child(icon("icons/alert.svg", 12.0, theme.danger))
                    .child(error),
            );
        }
        let Some(report) = status.and_then(|status| status.last_report.as_ref()) else {
            return column;
        };
        column = column.child(divider(theme)).child(
            div()
                .px(px(20.0))
                .py(px(10.0))
                .text_size(sp(12.5))
                .text_color(theme.text_secondary)
                .child(tr!(
                    "storage.summary",
                    removed = report.removed,
                    kept = report.kept,
                    failed = report.failed
                )),
        );
        let home = dirs::home_dir();
        for entry in report.entries.iter().take(VISIBLE_REPORT_ROWS) {
            column = column
                .child(divider(theme))
                .child(report_row(theme, entry, home.as_deref()));
        }
        let hidden =
            report.entries.len().saturating_sub(VISIBLE_REPORT_ROWS) as u32 + report.omitted;
        if hidden > 0 {
            column = column.child(divider(theme)).child(
                div()
                    .px(px(20.0))
                    .py(px(10.0))
                    .text_size(sp(12.5))
                    .text_color(theme.text_tertiary)
                    .child(tr!("storage.omitted", count = hidden)),
            );
        }
        column
    }
}

/// One report row: outcome (icon and word, never color alone), the task and
/// its folder, and why.
fn report_row(theme: Theme, entry: &WorktreeCleanupEntry, home: Option<&std::path::Path>) -> Div {
    let (outcome_icon, outcome_color, outcome_label) = match entry.outcome {
        WorktreeCleanupOutcome::Removed => (
            "icons/trash.svg",
            theme.success,
            tr!("storage.outcome_removed"),
        ),
        WorktreeCleanupOutcome::Kept => (
            "icons/lock.svg",
            theme.text_tertiary,
            tr!("storage.outcome_kept"),
        ),
        WorktreeCleanupOutcome::Failed => (
            "icons/alert.svg",
            theme.danger,
            tr!("storage.outcome_failed"),
        ),
    };
    let path = home
        .and_then(|home| entry.path.strip_prefix(home).ok())
        .map(|relative| format!("~/{}", relative.display()))
        .unwrap_or_else(|| entry.path.display().to_string());
    let mut reason = cleanup_reason_label(&entry.reason);
    if entry.branch_deleted {
        reason = tr!("storage.reason_with_branch", reason = reason);
    }
    div()
        .px(px(20.0))
        .py(px(10.0))
        .flex()
        .items_start()
        .gap(px(12.0))
        .child(
            div()
                .w(px(86.0))
                .flex_none()
                .flex()
                .items_center()
                .gap(px(6.0))
                .text_size(sp(12.5))
                .text_color(outcome_color)
                .child(icon(outcome_icon, 12.0, outcome_color))
                .child(outcome_label),
        )
        .child(
            div()
                .flex_1()
                .min_w_0()
                .child(
                    div()
                        .text_size(sp(12.5))
                        .text_color(theme.text)
                        .truncate()
                        .child(entry.task_title.clone().unwrap_or_else(|| path.clone())),
                )
                .child(
                    div()
                        .mt(px(2.0))
                        .text_size(sp(12.0))
                        .text_color(theme.text_tertiary)
                        .truncate()
                        .child(path),
                )
                .child(
                    div()
                        .mt(px(2.0))
                        .whitespace_normal()
                        .text_size(sp(12.5))
                        .line_height(sp(17.0))
                        .text_color(theme.text_secondary)
                        .child(reason),
                ),
        )
}
