//! New project against a remote daemon (#61): a folder picker over the daemon
//! host's filesystem.
//!
//! The local folder dialog would show this machine's disk, and a path picked
//! there means nothing to a daemon elsewhere. This modal browses the daemon's
//! own directories through `BrowseDirectory` instead. Every listing is one
//! background request; frames read only the listing already in memory, and a
//! generation counter keeps a slow, superseded response from replacing the
//! folder the user has since moved to.

use gpui::{KeyBinding, actions};
use waku_client::workspace::WorkingTreeEntry;
use waku_client::{WorkspaceOperation, WorkspaceResult};

use super::*;

actions!(
    waku_remote_folder_picker,
    [
        DismissRemoteFolderPicker,
        RemoteFolderPickerNext,
        RemoteFolderPickerPrevious,
        RemoteFolderPickerOpen,
        RemoteFolderPickerChoose,
        RemoteFolderPickerParent,
    ]
);

const PICKER_CONTEXT: &str = "RemoteFolderPicker";
const ROW_HEIGHT: f32 = 28.0;
const LIST_HEIGHT: f32 = 300.0;

pub fn init(cx: &mut App) {
    cx.bind_keys([
        KeyBinding::new("escape", DismissRemoteFolderPicker, Some(PICKER_CONTEXT)),
        KeyBinding::new("down", RemoteFolderPickerNext, Some(PICKER_CONTEXT)),
        KeyBinding::new("up", RemoteFolderPickerPrevious, Some(PICKER_CONTEXT)),
        KeyBinding::new("enter", RemoteFolderPickerOpen, Some(PICKER_CONTEXT)),
        KeyBinding::new(
            "secondary-enter",
            RemoteFolderPickerChoose,
            Some(PICKER_CONTEXT),
        ),
        KeyBinding::new("backspace", RemoteFolderPickerParent, Some(PICKER_CONTEXT)),
        KeyBinding::new(
            "secondary-up",
            RemoteFolderPickerParent,
            Some(PICKER_CONTEXT),
        ),
    ]);
}

/// One folder row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct FolderRow {
    pub name: SharedString,
    pub path: PathBuf,
}

/// A directory on the daemon host, as the daemon resolved it.
#[derive(Clone, Debug)]
struct Listing {
    path: PathBuf,
    parent: Option<PathBuf>,
    home: PathBuf,
    folders: Rc<Vec<FolderRow>>,
}

pub(super) struct RemoteFolderPicker {
    listing: Option<Listing>,
    loading: bool,
    error: Option<String>,
    /// Keyboard cursor over the folder rows.
    highlight: Option<usize>,
    /// Bumped by every navigation; a response for an older one is dropped.
    generation: u64,
    list_state: ListState,
    scrollbar: Rc<ScrollbarState>,
    focus: FocusHandle,
    choose_focus: FocusHandle,
    cancel_focus: FocusHandle,
    /// Focus moves into the modal on its first frame, which has a window.
    focus_pending: bool,
}

/// The folders of a listing, in the daemon's order (case-insensitive by
/// name), with hidden folders after the rest: they are rarely projects but
/// must stay reachable. Files are not shown; a project is a folder.
pub(super) fn folder_rows(entries: Vec<WorkingTreeEntry>) -> Vec<FolderRow> {
    let (hidden, visible): (Vec<_>, Vec<_>) = entries
        .into_iter()
        .filter(|entry| entry.is_dir)
        .partition(|entry| entry.name.starts_with('.'));
    visible
        .into_iter()
        .chain(hidden)
        .map(|entry| FolderRow {
            name: entry.name.into(),
            path: entry.absolute_path,
        })
        .collect()
}

/// Where the picker opens: beside the selected project when there is one on
/// the daemon, else the daemon user's home.
pub(super) fn initial_folder(selected_project: Option<&Project>) -> Option<PathBuf> {
    selected_project
        .filter(|project| !project.is_projectless())
        .and_then(|project| project.path.parent())
        .map(Path::to_path_buf)
}

/// Move a cursor over `count` rows by one step.
pub(super) fn step_highlight(current: Option<usize>, count: usize, down: bool) -> Option<usize> {
    if count == 0 {
        return None;
    }
    Some(match (current, down) {
        (None, true) => 0,
        (None, false) => count - 1,
        (Some(index), true) => (index + 1).min(count - 1),
        (Some(index), false) => index.saturating_sub(1),
    })
}

impl Waku {
    /// New project while the daemon runs elsewhere: browse its filesystem.
    pub(super) fn open_remote_folder_picker(&mut self, cx: &mut Context<Self>) {
        let start = initial_folder(self.selected_project());
        self.remote_folder_picker = Some(RemoteFolderPicker {
            listing: None,
            loading: true,
            error: None,
            highlight: None,
            generation: 0,
            list_state: ListState::new(0, ListAlignment::Top, px(ROW_HEIGHT * 4.0)),
            scrollbar: ScrollbarState::new(),
            focus: cx.focus_handle(),
            choose_focus: cx.focus_handle(),
            cancel_focus: cx.focus_handle(),
            focus_pending: true,
        });
        self.browse_remote_folder(start, cx);
    }

    /// List `path` on the daemon host, or its home for `None`.
    fn browse_remote_folder(&mut self, path: Option<PathBuf>, cx: &mut Context<Self>) {
        let Some(picker) = self.remote_folder_picker.as_mut() else {
            return;
        };
        picker.generation += 1;
        picker.loading = true;
        let generation = picker.generation;
        let workspace = waku_client::WorkspaceClient::new(self.daemon.client());
        cx.spawn(async move |waku, cx| {
            let result = cx
                .background_executor()
                .spawn(
                    async move { workspace.request(WorkspaceOperation::BrowseDirectory { path }) },
                )
                .await;
            let _ = waku.update(cx, |waku, cx| {
                let Some(picker) = waku.remote_folder_picker.as_mut() else {
                    return;
                };
                if picker.generation != generation {
                    return;
                }
                picker.loading = false;
                match result {
                    Ok(WorkspaceResult::Directory {
                        path,
                        parent,
                        home,
                        entries,
                        ..
                    }) => {
                        let folders = folder_rows(entries);
                        picker
                            .list_state
                            .reset_with_uniform_height(folders.len(), px(ROW_HEIGHT));
                        picker.listing = Some(Listing {
                            path,
                            parent,
                            home,
                            folders: Rc::new(folders),
                        });
                        picker.highlight = None;
                        picker.error = None;
                    }
                    Ok(_) => {
                        picker.error =
                            Some("Waku daemon returned an invalid directory listing".into());
                    }
                    // The previous folder stays listed under the error.
                    Err(error) => picker.error = Some(error.to_string()),
                }
                cx.notify();
            });
        })
        .detach();
        cx.notify();
    }

    fn close_remote_folder_picker(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.remote_folder_picker.take().is_none() {
            return;
        }
        let focus = self.composer_focus(cx);
        window.focus(&focus, cx);
        cx.notify();
    }

    /// Add the folder currently shown as a project.
    fn choose_remote_folder(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(path) = self
            .remote_folder_picker
            .as_ref()
            .and_then(|picker| picker.listing.as_ref())
            .map(|listing| listing.path.clone())
        else {
            return;
        };
        self.close_remote_folder_picker(window, cx);
        self.add_project_at(path, cx);
    }

    fn remote_folder_picker_parent(&mut self, cx: &mut Context<Self>) {
        let parent = self
            .remote_folder_picker
            .as_ref()
            .and_then(|picker| picker.listing.as_ref())
            .and_then(|listing| listing.parent.clone());
        if let Some(parent) = parent {
            self.browse_remote_folder(Some(parent), cx);
        }
    }

    fn remote_folder_picker_home(&mut self, cx: &mut Context<Self>) {
        let home = self
            .remote_folder_picker
            .as_ref()
            .and_then(|picker| picker.listing.as_ref())
            .map(|listing| listing.home.clone());
        self.browse_remote_folder(home, cx);
    }

    fn move_remote_folder_highlight(&mut self, down: bool, cx: &mut Context<Self>) {
        let Some(picker) = self.remote_folder_picker.as_mut() else {
            return;
        };
        let count = picker
            .listing
            .as_ref()
            .map_or(0, |listing| listing.folders.len());
        picker.highlight = step_highlight(picker.highlight, count, down);
        if let Some(index) = picker.highlight {
            picker.list_state.scroll_to_reveal_item(index);
        }
        cx.notify();
    }

    /// Enter: open the highlighted folder, or choose the shown one when the
    /// keyboard has not moved onto a row.
    fn open_remote_folder_highlight(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let target = self.remote_folder_picker.as_ref().and_then(|picker| {
            let index = picker.highlight?;
            picker
                .listing
                .as_ref()?
                .folders
                .get(index)
                .map(|row| row.path.clone())
        });
        match target {
            Some(path) => self.browse_remote_folder(Some(path), cx),
            None => self.choose_remote_folder(window, cx),
        }
    }

    pub(super) fn render_remote_folder_picker(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        let picker = self.remote_folder_picker.as_mut()?;
        if std::mem::take(&mut picker.focus_pending) {
            // The modal joins the dispatch tree only after it has drawn.
            let focus = picker.focus.clone();
            window.on_next_frame(move |window, _| {
                window.on_next_frame(move |window, cx| window.focus(&focus, cx));
            });
        }
        let picker = self.remote_folder_picker.as_ref()?;
        let theme = Theme::current(cx);
        let weak = cx.entity().downgrade();
        let listing = picker.listing.clone();
        let current_path = listing
            .as_ref()
            .map(|listing| listing.path.display().to_string());
        let can_go_up = listing
            .as_ref()
            .is_some_and(|listing| listing.parent.is_some());
        let can_choose = listing.is_some();
        let highlight = picker.highlight;

        let body = match &listing {
            Some(listing) if !listing.folders.is_empty() => {
                let folders = listing.folders.clone();
                let row_weak = weak.clone();
                div()
                    .id("remote-folder-list")
                    .relative()
                    .w_full()
                    .h(px(LIST_HEIGHT))
                    .flex_none()
                    .px(px(6.0))
                    .child(
                        list(picker.list_state.clone(), move |index, _window, _cx| {
                            let Some(row) = folders.get(index) else {
                                return div().into_any_element();
                            };
                            let path = row.path.clone();
                            let open_weak = row_weak.clone();
                            div()
                                .id(("remote-folder-row", index))
                                .h(px(ROW_HEIGHT))
                                .w_full()
                                .px(px(10.0))
                                .rounded(px(7.0))
                                .flex()
                                .items_center()
                                .gap(px(9.0))
                                .cursor_default()
                                .text_size(sp(13.0))
                                .text_color(theme.text)
                                .when(highlight == Some(index), |row| row.bg(theme.overlay_strong))
                                .hover(|row| row.bg(theme.overlay))
                                .child(icon("icons/folder.svg", 13.0, theme.text_secondary))
                                .child(div().min_w_0().flex_1().truncate().child(row.name.clone()))
                                .child(icon("icons/chevron-right.svg", 11.0, theme.text_ghost))
                                .on_click(move |_, _, cx| {
                                    let path = path.clone();
                                    let _ = open_weak.update(cx, |waku, cx| {
                                        waku.browse_remote_folder(Some(path), cx);
                                    });
                                })
                                .into_any_element()
                        })
                        .size_full(),
                    )
                    .child(scrollbar::edge_fade(
                        picker.list_state.clone(),
                        scrollbar::FadeEdge::Top,
                        theme.composer,
                    ))
                    .child(scrollbar::edge_fade(
                        picker.list_state.clone(),
                        scrollbar::FadeEdge::Bottom,
                        theme.composer,
                    ))
                    .child(scrollbar::vertical(&picker.list_state, &picker.scrollbar))
                    .into_any_element()
            }
            _ => div()
                .h(px(LIST_HEIGHT))
                .flex_none()
                .flex()
                .items_center()
                .justify_center()
                .text_size(sp(13.0))
                .text_color(theme.text_tertiary)
                .child(if picker.loading {
                    tr!("project.remote_picker_loading")
                } else if listing.is_some() {
                    tr!("project.remote_picker_empty")
                } else {
                    String::new()
                })
                .into_any_element(),
        };

        let nav_button = |id: &'static str, icon_path: &'static str, label: String, enabled| {
            div()
                .id(id)
                .h(px(26.0))
                .px(px(8.0))
                .flex_none()
                .rounded(px(7.0))
                .flex()
                .items_center()
                .gap(px(6.0))
                .cursor_default()
                .text_size(sp(12.5))
                .text_color(if enabled {
                    theme.text_secondary
                } else {
                    theme.text_ghost
                })
                .when(enabled, |button| {
                    button
                        .hover(|button| button.bg(theme.overlay))
                        .active(|button| button.bg(theme.overlay_strong))
                })
                .child(icon(
                    icon_path,
                    12.0,
                    if enabled {
                        theme.text_secondary
                    } else {
                        theme.text_ghost
                    },
                ))
                .child(label)
        };
        let up_weak = weak.clone();
        let home_weak = weak.clone();
        let path_bar = div()
            .h(px(40.0))
            .px(px(10.0))
            .flex_none()
            .flex()
            .items_center()
            .gap(px(4.0))
            .border_b_1()
            .border_color(theme.border)
            .child(
                nav_button(
                    "remote-folder-up",
                    "icons/arrow-up.svg",
                    tr!("project.remote_picker_up"),
                    can_go_up,
                )
                .on_click(move |_, _, cx| {
                    let _ = up_weak.update(cx, |waku, cx| waku.remote_folder_picker_parent(cx));
                }),
            )
            .child(
                nav_button(
                    "remote-folder-home",
                    "icons/server.svg",
                    tr!("project.remote_picker_home"),
                    listing.is_some(),
                )
                .on_click(move |_, _, cx| {
                    let _ = home_weak.update(cx, |waku, cx| waku.remote_folder_picker_home(cx));
                }),
            )
            .child(
                div()
                    .min_w_0()
                    .flex_1()
                    .pl(px(6.0))
                    .truncate()
                    .text_size(sp(12.5))
                    .text_color(theme.text_secondary)
                    .child(current_path.unwrap_or_default()),
            );

        let footer_button =
            |id: &'static str,
             focus: &FocusHandle,
             label: String,
             primary: bool,
             enabled: bool,
             activate: fn(&mut Waku, &mut Window, &mut Context<Waku>)| {
                let click_weak = weak.clone();
                let key_weak = weak.clone();
                div()
                    .id(id)
                    .track_focus(focus)
                    .when(enabled, |button| button.tab_index(0))
                    .h(px(30.0))
                    .px(px(14.0))
                    .rounded(px(9.0))
                    .flex()
                    .items_center()
                    .cursor_default()
                    .text_size(sp(13.0))
                    .when(primary, |button| {
                        button
                            .bg(if enabled { theme.text } else { theme.overlay })
                            .text_color(if enabled {
                                theme.composer
                            } else {
                                theme.text_ghost
                            })
                    })
                    .when(!primary, |button| {
                        button
                            .text_color(theme.text_secondary)
                            .hover(|button| button.bg(theme.overlay))
                    })
                    .focus_visible(|style| style.border_1().border_color(theme.accent))
                    .child(label)
                    .when(enabled, |button| {
                        button
                            .on_click(move |_, window, cx| {
                                let _ =
                                    click_weak.update(cx, |waku, cx| activate(waku, window, cx));
                            })
                            .on_key_down(move |event: &KeyDownEvent, window, cx| {
                                if !event.keystroke.modifiers.modified()
                                    && matches!(event.keystroke.key.as_str(), "enter" | "space")
                                {
                                    let _ =
                                        key_weak.update(cx, |waku, cx| activate(waku, window, cx));
                                    cx.stop_propagation();
                                }
                            })
                    })
            };

        let footer = div()
            .h(px(52.0))
            .px(px(14.0))
            .flex_none()
            .flex()
            .items_center()
            .gap(px(8.0))
            .border_t_1()
            .border_color(theme.border)
            .child(
                div()
                    .min_w_0()
                    .flex_1()
                    .truncate()
                    .text_size(sp(12.0))
                    .text_color(if picker.error.is_some() {
                        theme.danger
                    } else {
                        theme.text_tertiary
                    })
                    .child(
                        picker
                            .error
                            .clone()
                            .unwrap_or_else(|| tr!("project.remote_picker_hint")),
                    ),
            )
            .child(footer_button(
                "remote-folder-cancel",
                &picker.cancel_focus,
                tr!("common.cancel"),
                false,
                true,
                |waku, window, cx| waku.close_remote_folder_picker(window, cx),
            ))
            .child(footer_button(
                "remote-folder-choose",
                &picker.choose_focus,
                tr!("project.remote_picker_open"),
                true,
                can_choose,
                |waku, window, cx| waku.choose_remote_folder(window, cx),
            ));

        let card = div()
            .id("remote-folder-picker")
            .key_context(PICKER_CONTEXT)
            .track_focus(&picker.focus)
            .on_action(
                cx.listener(|waku, _: &DismissRemoteFolderPicker, window, cx| {
                    waku.close_remote_folder_picker(window, cx);
                }),
            )
            .on_action(cx.listener(|waku, _: &RemoteFolderPickerNext, _, cx| {
                waku.move_remote_folder_highlight(true, cx);
            }))
            .on_action(cx.listener(|waku, _: &RemoteFolderPickerPrevious, _, cx| {
                waku.move_remote_folder_highlight(false, cx);
            }))
            .on_action(cx.listener(|waku, _: &RemoteFolderPickerOpen, window, cx| {
                waku.open_remote_folder_highlight(window, cx);
            }))
            .on_action(
                cx.listener(|waku, _: &RemoteFolderPickerChoose, window, cx| {
                    waku.choose_remote_folder(window, cx);
                }),
            )
            .on_action(cx.listener(|waku, _: &RemoteFolderPickerParent, _, cx| {
                waku.remote_folder_picker_parent(cx);
            }))
            .tab_group()
            .w_full()
            .max_w(px(560.0))
            .overflow_hidden()
            .rounded(px(16.0))
            .bg(theme.composer)
            .shadow_xl()
            .flex()
            .flex_col()
            .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
            .child(
                div()
                    .h(px(46.0))
                    .px(px(16.0))
                    .flex_none()
                    .flex()
                    .items_center()
                    .gap(px(9.0))
                    .text_size(sp(14.0))
                    .text_color(theme.text)
                    .child(icon("icons/folder-open.svg", 15.0, theme.text))
                    .child(div().child(tr!("project.remote_picker_title"))),
            )
            .child(path_bar)
            .child(body)
            .child(footer);

        let scrim = if theme.is_dark {
            gpui::hsla(0.0, 0.0, 0.0, 0.34)
        } else {
            gpui::hsla(0.0, 0.0, 0.0, 0.16)
        };
        let layer = div()
            .id("remote-folder-picker-layer")
            .absolute()
            .inset_0()
            .occlude()
            .bg(scrim)
            .p(px(24.0))
            .flex()
            .items_center()
            .justify_center()
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|waku, _, window, cx| waku.close_remote_folder_picker(window, cx)),
            )
            .child(card);
        Some(gpui::deferred(layer).with_priority(4).into_any_element())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(name: &str, is_dir: bool) -> WorkingTreeEntry {
        WorkingTreeEntry {
            relative_path: name.into(),
            absolute_path: PathBuf::from("/srv").join(name),
            name: name.into(),
            is_dir,
            expanded: false,
            depth: 0,
        }
    }

    #[test]
    fn folder_rows_list_folders_with_hidden_ones_last() {
        let rows = folder_rows(vec![
            entry(".config", true),
            entry("api", true),
            entry("Web", true),
            entry("notes.txt", false),
        ]);
        assert_eq!(
            rows.iter().map(|row| row.name.as_ref()).collect::<Vec<_>>(),
            vec!["api", "Web", ".config"]
        );
        assert_eq!(rows[0].path, PathBuf::from("/srv/api"));
    }

    #[test]
    fn the_picker_opens_beside_the_selected_project() {
        let project = Project::from_path(PathBuf::from("/srv/code/waku"));
        assert_eq!(
            initial_folder(Some(&project)),
            Some(PathBuf::from("/srv/code"))
        );
        assert_eq!(initial_folder(None), None);
    }

    #[test]
    fn the_keyboard_cursor_stays_on_the_rows() {
        assert_eq!(step_highlight(None, 0, true), None);
        assert_eq!(step_highlight(None, 3, true), Some(0));
        assert_eq!(step_highlight(None, 3, false), Some(2));
        assert_eq!(step_highlight(Some(2), 3, true), Some(2));
        assert_eq!(step_highlight(Some(0), 3, false), Some(0));
        assert_eq!(step_highlight(Some(1), 3, true), Some(2));
    }
}
