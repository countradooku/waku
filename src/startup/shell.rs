//! The launch shell: the window's first frames while the daemon starts.
//!
//! It paints the sidebar from the startup snapshot with the same geometry as
//! the real one (titlebar, action rows, group headers, session cards), so the
//! swap to the app view moves nothing. It has no controls: the shell lasts
//! only until the bootstrap lands, and the global key bindings (quit, close)
//! already work. When the daemon fails to start, the main area explains why.

use gpui::{
    AnyElement, App, FontWeight, InteractiveElement as _, IntoElement, MouseButton,
    ParentElement as _, Styled as _, div, prelude::FluentBuilder as _, px,
};

use super::snapshot::{ShellSettings, SidebarSnapshot, SnapshotRow};
use crate::persistence::DEFAULT_SIDEBAR_WIDTH;
use crate::theme::{Theme, ThemePreference, sp};

// Mirrors of the sidebar's layout constants (`src/app/sidebar.rs`).
const TITLEBAR_HEIGHT: f32 = 48.0;
/// "New task" plus the search row and its gap.
const ACTION_ROWS_HEIGHT: f32 = 32.0 + 32.0 + 10.0;
const GROUP_HEADER_HEIGHT: f32 = 28.0;
const GROUP_HEADER_BOTTOM_GAP: f32 = 2.0;
const SESSION_CARD_HEIGHT: f32 = 51.0;
const SESSION_ROW_GAP: f32 = 1.0;
const GROUP_SPACER_HEIGHT: f32 = 10.0;
const GROUP_CHILD_PADDING: f32 = 28.0;

/// The shell's settings when there is no snapshot (first launch).
pub(super) fn default_settings() -> ShellSettings {
    ShellSettings {
        theme: ThemePreference::System,
        language: crate::i18n::AppLanguage::System,
        ui_font_size: waku_client::persistence::DEFAULT_UI_FONT_SIZE,
        sidebar_visible: true,
        sidebar_width: DEFAULT_SIDEBAR_WIDTH,
    }
}

pub(super) fn sidebar_width(settings: ShellSettings) -> f32 {
    if settings.sidebar_width > 0.0 {
        settings.sidebar_width
    } else {
        DEFAULT_SIDEBAR_WIDTH
    }
}

pub(super) fn render(
    snapshot: Option<&SidebarSnapshot>,
    failure: Option<&str>,
    cx: &App,
) -> AnyElement {
    let theme = Theme::current(cx);
    let settings = snapshot.map_or_else(default_settings, SidebarSnapshot::settings);
    let rows = snapshot.map_or(&[][..], |snapshot| snapshot.rows.as_slice());
    let sidebar = settings.sidebar_visible.then(|| {
        div()
            .w(px(sidebar_width(settings)))
            .h_full()
            .flex_none()
            .flex()
            .flex_col()
            .overflow_hidden()
            .bg(theme.sidebar)
            .child(titlebar())
            .child(div().h(px(ACTION_ROWS_HEIGHT)).flex_none())
            .child(
                div()
                    .px(px(10.0))
                    .flex()
                    .flex_col()
                    .children(rows.iter().map(|row| sidebar_row(row, &theme))),
            )
    });
    let main = div()
        .flex_1()
        .h_full()
        .min_w_0()
        .flex()
        .flex_col()
        .bg(theme.surface)
        .when(sidebar.is_some(), |main| {
            main.border_l_1().border_color(theme.sidebar_border)
        })
        .when(sidebar.is_none(), |main| main.child(titlebar()))
        .when_some(failure, |main, message| {
            main.child(
                div()
                    .flex_1()
                    .flex()
                    .flex_col()
                    .items_center()
                    .justify_center()
                    .gap(px(8.0))
                    .px(px(32.0))
                    .child(
                        div()
                            .text_size(sp(15.0))
                            .font_weight(FontWeight::MEDIUM)
                            .text_color(theme.text)
                            .child(tr!("startup.daemon_failed")),
                    )
                    .child(
                        div()
                            .max_w(px(560.0))
                            .text_size(sp(13.0))
                            .text_color(theme.text_secondary)
                            .child(message.to_owned()),
                    )
                    .child(
                        div()
                            .text_size(sp(12.0))
                            .text_color(theme.text_tertiary)
                            .child(tr!("startup.daemon_failed_hint")),
                    ),
            )
        });
    div()
        .size_full()
        .flex()
        .text_color(theme.text)
        .font_family(".SystemUIFont")
        .children(sidebar)
        .child(main)
        .into_any_element()
}

/// The titlebar strip, draggable like the real one so a failed launch can
/// still be moved out of the way.
fn titlebar() -> impl IntoElement {
    let strip = div()
        .id("startup-shell-titlebar")
        .h(px(TITLEBAR_HEIGHT))
        .w_full()
        .flex_none();
    #[cfg(target_os = "windows")]
    let strip = strip.window_control_area(gpui::WindowControlArea::Drag);
    strip.on_mouse_down(MouseButton::Left, |_, window, _| {
        crate::platform::start_window_move(window);
    })
}

fn sidebar_row(row: &SnapshotRow, theme: &Theme) -> AnyElement {
    match row {
        SnapshotRow::Header { label } => div()
            .h(px(GROUP_HEADER_HEIGHT))
            .mb(px(GROUP_HEADER_BOTTOM_GAP))
            .flex_none()
            .px(px(8.0))
            .flex()
            .items_center()
            .text_size(sp(13.0))
            .font_weight(FontWeight::MEDIUM)
            .text_color(theme.text_secondary)
            .overflow_hidden()
            .child(div().truncate().child(label.clone()))
            .into_any_element(),
        SnapshotRow::Session { title, selected } => div()
            .h(px(SESSION_CARD_HEIGHT))
            .mb(px(SESSION_ROW_GAP))
            .flex_none()
            .pl(px(GROUP_CHILD_PADDING))
            .pr(px(8.0))
            .rounded(px(6.0))
            .when(*selected, |row| row.bg(theme.sidebar_item_background))
            .flex()
            .items_center()
            .text_size(sp(13.0))
            .text_color(theme.text)
            .overflow_hidden()
            .child(div().truncate().child(title.clone()))
            .into_any_element(),
        SnapshotRow::Spacer => div()
            .h(px(GROUP_SPACER_HEIGHT))
            .flex_none()
            .into_any_element(),
    }
}
