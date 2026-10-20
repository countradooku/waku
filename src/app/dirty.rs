//! Targeted invalidation for the root view and its pane islands.
//!
//! A root `cx.notify()` fans out to every [`super::WakuPane`] through their
//! root observers, so one stream commit used to rebuild the sidebar, the
//! transcript and the right panel together. Producers that know what they
//! changed — the event pump, the time-label wake — record it in a
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

/// What one provider event can change on screen, before knowing whether its
/// session is the selected one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DriverEventImpact {
    /// Bookkeeping nothing draws (the replay cursor) or output that has its
    /// own repaint cadence (background log deltas, batched at 10 Hz).
    Nothing,
    /// Turn content: text and reasoning. Only the transcript draws it; the
    /// sidebar row may still change, which the caller checks by fingerprint.
    Transcript,
    /// Tool activity and background work: drawn by the transcript and by
    /// right-panel surfaces (background work, terminal-backed commands).
    TranscriptAndRightPanel,
    /// Everything else — status, permissions, errors, turn boundaries,
    /// usage, titles. Rare, so it keeps the whole-window notify.
    All,
}

pub(super) fn driver_event_impact(event: &DriverEvent) -> DriverEventImpact {
    match event {
        DriverEvent::RuntimeEventCursorAdvanced(_)
        | DriverEvent::BackgroundWork(BackgroundWorkEvent::OutputDelta { .. }) => {
            DriverEventImpact::Nothing
        }
        DriverEvent::TextDelta(_) | DriverEvent::ReasoningDelta(_) => DriverEventImpact::Transcript,
        DriverEvent::Activity { .. }
        | DriverEvent::RichActivity(_)
        | DriverEvent::BackgroundWork(_) => DriverEventImpact::TranscriptAndRightPanel,
        _ => DriverEventImpact::All,
    }
}

/// The regions one drained session dirtied. `sidebar_changed` is whether the
/// session's sidebar fingerprint moved across the drain.
pub(super) fn session_drain_dirty(
    impact: Option<DriverEventImpact>,
    selected: bool,
    sidebar_changed: bool,
) -> DirtySet {
    let mut dirty = match impact {
        None | Some(DriverEventImpact::Nothing) => DirtySet::NONE,
        Some(DriverEventImpact::All) => DirtySet::ALL,
        Some(DriverEventImpact::Transcript) if selected => DirtySet {
            root: true,
            transcript: true,
            ..DirtySet::NONE
        },
        Some(DriverEventImpact::TranscriptAndRightPanel) if selected => DirtySet {
            root: true,
            transcript: true,
            right_panel: true,
            ..DirtySet::NONE
        },
        // A background session's content is drawn nowhere but its sidebar
        // row, and only through the fingerprinted fields.
        Some(DriverEventImpact::Transcript | DriverEventImpact::TranscriptAndRightPanel) => {
            DirtySet::NONE
        }
    };
    dirty.sidebar |= sidebar_changed;
    dirty
}

/// [`sidebar_session_fingerprint`] of one session, `None` once it is gone.
pub(super) fn session_sidebar_fingerprint(sessions: &[AgentSession], id: Uuid) -> Option<u64> {
    sessions
        .iter()
        .find(|session| session.id == id)
        .map(sidebar_session_fingerprint)
}

/// The widest impact of two events drained together.
pub(super) fn widest_impact(
    current: Option<DriverEventImpact>,
    next: DriverEventImpact,
) -> Option<DriverEventImpact> {
    fn rank(impact: DriverEventImpact) -> u8 {
        match impact {
            DriverEventImpact::Nothing => 0,
            DriverEventImpact::Transcript => 1,
            DriverEventImpact::TranscriptAndRightPanel => 2,
            DriverEventImpact::All => 3,
        }
    }
    Some(match current {
        Some(current) if rank(current) >= rank(next) => current,
        _ => next,
    })
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
    use crate::model::RuntimeEventCursor;

    fn text() -> DriverEvent {
        DriverEvent::TextDelta("hello".into())
    }

    #[test]
    fn streaming_text_dirties_only_the_transcript_of_the_selected_session() {
        let impact = widest_impact(None, driver_event_impact(&text()));
        assert_eq!(
            session_drain_dirty(impact, true, false),
            DirtySet {
                root: true,
                transcript: true,
                ..DirtySet::NONE
            }
        );
        assert_eq!(
            session_drain_dirty(impact, true, false).route(),
            DirtyRoute::Panes {
                sidebar: false,
                transcript: true,
                right_panel: false,
            }
        );
    }

    #[test]
    fn a_background_session_streams_without_a_frame_unless_its_row_changed() {
        let impact = widest_impact(None, driver_event_impact(&text()));
        assert!(session_drain_dirty(impact, false, false).is_empty());
        assert_eq!(
            session_drain_dirty(impact, false, true).route(),
            DirtyRoute::Panes {
                sidebar: true,
                transcript: false,
                right_panel: false,
            }
        );
    }

    #[test]
    fn a_sidebar_visible_change_adds_the_sidebar_and_nothing_else() {
        let impact = widest_impact(None, driver_event_impact(&text()));
        assert_eq!(
            session_drain_dirty(impact, true, true),
            DirtySet {
                root: true,
                sidebar: true,
                transcript: true,
                right_panel: false,
            }
        );
    }

    #[test]
    fn activities_also_dirty_the_right_panel() {
        let activity = DriverEvent::Activity {
            id: None,
            kind: crate::model::ActivityKind::Command,
            title: "ls".into(),
            detail: None,
            complete: false,
        };
        let impact = widest_impact(
            widest_impact(None, driver_event_impact(&text())),
            driver_event_impact(&activity),
        );
        assert_eq!(impact, Some(DriverEventImpact::TranscriptAndRightPanel));
        let dirty = session_drain_dirty(impact, true, false);
        assert!(dirty.transcript && dirty.right_panel && !dirty.sidebar);
    }

    #[test]
    fn turn_boundaries_and_errors_keep_the_whole_window_notify() {
        for event in [
            DriverEvent::TurnStarted,
            DriverEvent::TurnFinished {
                success: true,
                summary: None,
            },
            DriverEvent::Error("boom".into()),
            DriverEvent::ProcessExited,
            DriverEvent::AutoTitleUpdated(Some("Title".into())),
        ] {
            let impact = widest_impact(
                widest_impact(None, driver_event_impact(&text())),
                driver_event_impact(&event),
            );
            assert_eq!(impact, Some(DriverEventImpact::All), "{event:?}");
            for selected in [true, false] {
                assert_eq!(
                    session_drain_dirty(impact, selected, false).route(),
                    DirtyRoute::FanOut
                );
            }
        }
    }

    #[test]
    fn bookkeeping_events_cause_no_frame() {
        let impact = widest_impact(
            None,
            driver_event_impact(&DriverEvent::RuntimeEventCursorAdvanced(
                RuntimeEventCursor {
                    runtime_id: Uuid::nil(),
                    epoch: Uuid::nil(),
                    sequence: 1,
                },
            )),
        );
        assert!(session_drain_dirty(impact, true, false).is_empty());
        assert!(session_drain_dirty(None, true, false).is_empty());
    }

    #[test]
    fn streamed_text_leaves_the_sidebar_fingerprint_alone() {
        let mut session = AgentSession::new(Uuid::new_v4(), ProviderKind::Codex);
        session.title = "Fix the bug".into();
        let id = session.id;
        let mut sessions = vec![session];
        append_text_delta_to_session(&mut sessions, id, false, "Hello".into());
        let before = session_sidebar_fingerprint(&sessions, id);
        assert!(before.is_some());

        append_text_delta_to_session(&mut sessions, id, true, " world".into());
        sessions[0].updated_at += 5;
        assert_eq!(session_sidebar_fingerprint(&sessions, id), before);

        sessions[0].status = SessionStatus::Working;
        let working = session_sidebar_fingerprint(&sessions, id);
        assert_ne!(working, before, "status shows as the row's spinner");

        sessions[0].set_auto_title(Some("Renamed".into()));
        sessions[0].title = AgentSession::DEFAULT_TITLE.into();
        assert_ne!(session_sidebar_fingerprint(&sessions, id), working);

        assert_eq!(session_sidebar_fingerprint(&sessions, Uuid::new_v4()), None);
    }

    #[test]
    fn widest_impact_never_narrows() {
        let all = widest_impact(None, DriverEventImpact::All);
        assert_eq!(
            widest_impact(all, DriverEventImpact::Transcript),
            Some(DriverEventImpact::All)
        );
        assert_eq!(
            widest_impact(
                Some(DriverEventImpact::Nothing),
                DriverEventImpact::Transcript
            ),
            Some(DriverEventImpact::Transcript)
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
