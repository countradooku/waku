//! Daemon state other clients change, followed over protocol v8 (#55): the
//! `settings` stream and the `drafts` stream.
//!
//! Each stream is read on its own worker thread (`waku_client::surfaces`),
//! which hands changes to a GPUI task through a channel; the task applies a
//! burst at once and notifies. Nothing here polls, and nothing blocks the UI
//! thread.

use waku_client::surfaces::drafts::{DraftStore, RemoteDraftChange};
use waku_client::surfaces::settings::SettingsSync;

use super::*;

impl Waku {
    /// Start following the daemon's settings and drafts. Called once while
    /// the app entity is built.
    pub(super) fn follow_daemon_streams(
        settings_sync: &SettingsSync,
        drafts: &DraftStore,
        cx: &mut Context<Self>,
    ) {
        let (settings_tx, settings_rx) = smol::channel::unbounded();
        if let Err(error) =
            settings_sync.follow(move |settings| settings_tx.send_blocking(settings).is_ok())
        {
            eprintln!("could not follow the daemon settings: {error:#}");
        }
        cx.spawn(async move |this, cx| {
            while let Ok(mut settings) = settings_rx.recv().await {
                // A burst of edits elsewhere lands as its newest document.
                while let Ok(newer) = settings_rx.try_recv() {
                    settings = newer;
                }
                if this
                    .update(cx, |this, cx| {
                        this.state.apply_daemon_settings(settings);
                        cx.notify();
                    })
                    .is_err()
                {
                    break;
                }
            }
        })
        .detach();

        let (drafts_tx, drafts_rx) = smol::channel::unbounded();
        if let Err(error) = drafts.follow(move |changes| drafts_tx.send_blocking(changes).is_ok()) {
            eprintln!("could not follow the composer drafts: {error:#}");
        }
        cx.spawn(async move |this, cx| {
            while let Ok(mut changes) = drafts_rx.recv().await {
                while let Ok(more) = drafts_rx.try_recv() {
                    changes.extend(more);
                }
                if this
                    .update(cx, |this, cx| {
                        this.apply_remote_composer_drafts(changes, cx)
                    })
                    .is_err()
                {
                    break;
                }
            }
        })
        .detach();
    }

    /// Fold drafts another client changed into the editing copy. A draft
    /// edited here since the daemon last saw it keeps the local text, which
    /// the next save sends.
    fn apply_remote_composer_drafts(
        &mut self,
        changes: Vec<RemoteDraftChange>,
        cx: &mut Context<Self>,
    ) {
        let selected = self.selected_composer_draft_key();
        self.capture_current_composer_draft(cx);
        let outcome = apply_remote_drafts(&mut self.composer_drafts, changes, selected);
        if outcome.restore_selected {
            self.restore_selected_composer_draft(cx);
        }
        if outcome.local_kept {
            self.schedule_composer_draft_save(cx);
        }
        cx.notify();
    }
}

/// What folding remote draft changes did.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct RemoteDraftsOutcome {
    /// The selected composer's draft changed and must be shown.
    pub restore_selected: bool,
    /// A local edit won over a remote one and still has to be sent.
    pub local_kept: bool,
}

pub(super) fn apply_remote_drafts(
    drafts: &mut crate::persistence::ComposerDrafts,
    changes: Vec<RemoteDraftChange>,
    selected: Option<crate::persistence::ComposerDraftKey>,
) -> RemoteDraftsOutcome {
    let mut outcome = RemoteDraftsOutcome::default();
    for change in changes {
        let local = drafts.get(change.key).cloned();
        if local == change.draft {
            continue;
        }
        if local != change.previous {
            outcome.local_kept = true;
            continue;
        }
        match change.draft {
            Some(draft) => {
                drafts.set(change.key, draft);
            }
            None => {
                drafts.remove(change.key);
            }
        }
        outcome.restore_selected |= Some(change.key) == selected;
    }
    outcome
}
