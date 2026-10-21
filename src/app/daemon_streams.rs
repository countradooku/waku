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

    /// Read the daemon's version for the Daemon settings page.
    pub(super) fn load_daemon_info(daemon_v8: &crate::daemon_v8::DaemonV8, cx: &mut Context<Self>) {
        let daemon_v8 = daemon_v8.clone();
        cx.spawn(async move |this, cx| {
            let info = cx
                .background_executor()
                .spawn(async move { waku_client::surfaces::settings::daemon_info(&daemon_v8) })
                .await;
            match info {
                Ok(info) => {
                    let _ = this.update(cx, |this, cx| {
                        this.daemon_version = Some(info.version);
                        cx.notify();
                    });
                }
                Err(error) => eprintln!("could not read the daemon info: {error:#}"),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::{ComposerDraft, ComposerDraftKey, ComposerDrafts};

    fn draft(text: &str) -> ComposerDraft {
        ComposerDraft {
            text: text.into(),
            attachments: Vec::new(),
        }
    }

    #[test]
    fn remote_drafts_replace_unedited_copies_and_yield_to_local_edits() {
        let selected = ComposerDraftKey::Session(Uuid::from_u128(1));
        let other = ComposerDraftKey::NewSession(Uuid::from_u128(2));
        let edited = ComposerDraftKey::Session(Uuid::from_u128(3));
        let mut drafts = ComposerDrafts::default();
        drafts.set(selected, draft("hello"));
        drafts.set(edited, draft("typed here"));
        let outcome = apply_remote_drafts(
            &mut drafts,
            vec![
                RemoteDraftChange {
                    key: selected,
                    previous: Some(draft("hello")),
                    draft: Some(draft("hello from the web")),
                },
                RemoteDraftChange {
                    key: other,
                    previous: None,
                    draft: Some(draft("new task")),
                },
                RemoteDraftChange {
                    key: edited,
                    previous: Some(draft("typed")),
                    draft: None,
                },
            ],
            Some(selected),
        );
        assert_eq!(
            outcome,
            RemoteDraftsOutcome {
                restore_selected: true,
                local_kept: true,
            }
        );
        assert_eq!(drafts.get(selected), Some(&draft("hello from the web")));
        assert_eq!(drafts.get(other), Some(&draft("new task")));
        assert_eq!(drafts.get(edited), Some(&draft("typed here")));
    }

    #[test]
    fn a_remote_change_already_held_changes_nothing() {
        let key = ComposerDraftKey::Session(Uuid::from_u128(1));
        let mut drafts = ComposerDrafts::default();
        drafts.set(key, draft("same"));
        let outcome = apply_remote_drafts(
            &mut drafts,
            vec![RemoteDraftChange {
                key,
                previous: None,
                draft: Some(draft("same")),
            }],
            Some(key),
        );
        assert_eq!(outcome, RemoteDraftsOutcome::default());
    }
}
