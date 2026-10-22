//! Targeted invalidation for the root view and its pane islands.
//!
//! A root `cx.notify()` fans out to every [`super::WakuPane`] through their
//! root observers, so one stream commit used to rebuild the sidebar, the
//! transcript and the right panel together. Producers that know what they
//! changed — the event pump (from each store update's `ThreadChange`), the
//! time-label wake — record it in a
//! [`DirtySet`] instead, and [`Waku::notify_dirty`] notifies exactly those
//! islands. Everything else keeps the conservative root notify.
//!
//! The root view itself renders on every window draw, so a dirty pane already
//! brings the header, composer and footer up to date. Root-only changes take
//! the root-frame path (see [`RootFrame`]): a frame without the pane fan-out.

use super::*;

/// Which regions of the window a change has to repaint.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct DirtySet {
    /// State the root view draws itself: header, composer, footer, toasts,
    /// overlays.
    pub(super) root: bool,
    pub(super) sidebar: bool,
    pub(super) transcript: bool,
    pub(super) right_panel: bool,
}

impl DirtySet {
    pub(super) const NONE: Self = Self {
        root: false,
        sidebar: false,
        transcript: false,
        right_panel: false,
    };

    pub(super) const ALL: Self = Self {
        root: true,
        sidebar: true,
        transcript: true,
        right_panel: true,
    };

    #[cfg(test)]
    pub(super) fn is_empty(self) -> bool {
        self == Self::NONE
    }

    pub(super) fn union(self, other: Self) -> Self {
        Self {
            root: self.root || other.root,
            sidebar: self.sidebar || other.sidebar,
            transcript: self.transcript || other.transcript,
            right_panel: self.right_panel || other.right_panel,
        }
    }

    /// How [`Waku::notify_dirty`] delivers this set.
    pub(super) fn route(self) -> DirtyRoute {
        if self.sidebar && self.transcript && self.right_panel {
            // Every island rebuilds either way; one root notify is one
            // effect instead of three.
            return DirtyRoute::FanOut;
        }
        if self.sidebar || self.transcript || self.right_panel {
            return DirtyRoute::Panes {
                sidebar: self.sidebar,
                transcript: self.transcript,
                right_panel: self.right_panel,
            };
        }
        if self.root {
            DirtyRoute::RootOnly
        } else {
            DirtyRoute::Nothing
        }
    }
}

/// The notifications one [`DirtySet`] turns into.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DirtyRoute {
    Nothing,
    /// A frame for the root view alone: every island replays its cache.
    RootOnly,
    /// Notify these islands directly. The root renders in the same frame
    /// because it renders on every draw, so root state rides along.
    Panes {
        sidebar: bool,
        transcript: bool,
        right_panel: bool,
    },
    /// A plain root notify: the pane observers rebuild every island.
    FanOut,
}

/// A zero-size child of the root view whose only job is to be notified.
///
/// gpui marks a notified view and its ancestors dirty, and the root renders
/// on every window draw anyway, so notifying this view buys a frame that
/// re-renders the root without the pane observers firing: every island
/// replays its cached subtree. Root-hosted pulses (the header's background
/// work spinner, the composer's preparing spinner) and root-only state
/// changes use it instead of a root notify.
pub(super) struct RootFrame;

impl Render for RootFrame {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        div().absolute().size_0()
    }
}

impl Waku {
    /// Deliver a [`DirtySet`]: notify only the islands it names.
    pub(super) fn notify_dirty(&self, dirty: DirtySet, cx: &mut Context<Self>) {
        match dirty.route() {
            DirtyRoute::Nothing => {}
            DirtyRoute::FanOut => cx.notify(),
            DirtyRoute::RootOnly => self.notify_root_only(cx),
            DirtyRoute::Panes {
                sidebar,
                transcript,
                right_panel,
            } => {
                for (dirty, pane) in [
                    (sidebar, &self.sidebar_pane),
                    (transcript, &self.transcript_pane),
                    (right_panel, &self.right_panel_pane),
                ] {
                    if dirty {
                        pane.update(cx, |_, cx| cx.notify());
                    }
                }
            }
        }
    }

    /// A frame that re-renders the root view and replays every island.
    pub(super) fn notify_root_only(&self, cx: &mut App) {
        self.root_frame.update(cx, |_, cx| cx.notify());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stream_commit_of_the_selected_thread_notifies_the_transcript_island_alone() {
        let change = waku_client::store::ThreadChange {
            text: true,
            ..Default::default()
        };
        assert_eq!(
            super::super::threads::thread_update_dirty(change, true).route(),
            DirtyRoute::Panes {
                sidebar: false,
                transcript: true,
                right_panel: false,
            }
        );
        assert_eq!(
            super::super::threads::thread_update_dirty(change, false).route(),
            DirtyRoute::Nothing
        );
    }

    #[test]
    fn routes_pick_the_cheapest_equivalent_notify() {
        assert_eq!(DirtySet::NONE.route(), DirtyRoute::Nothing);
        assert_eq!(DirtySet::ALL.route(), DirtyRoute::FanOut);
        assert_eq!(
            DirtySet {
                root: true,
                ..DirtySet::NONE
            }
            .route(),
            DirtyRoute::RootOnly
        );
        // All three islands without the root flag is still a fan-out.
        assert_eq!(
            DirtySet {
                root: false,
                ..DirtySet::ALL
            }
            .route(),
            DirtyRoute::FanOut
        );
        assert_eq!(
            DirtySet {
                sidebar: true,
                ..DirtySet::NONE
            }
            .union(DirtySet {
                right_panel: true,
                ..DirtySet::NONE
            })
            .route(),
            DirtyRoute::Panes {
                sidebar: true,
                transcript: false,
                right_panel: true,
            }
        );
    }
}
