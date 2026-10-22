//! Composer drafts: `drafts.get`, `drafts.apply` and the `drafts` stream.
//!
//! The desktop edits drafts as the domain [`ComposerDrafts`], keyed by
//! [`ComposerDraftKey`]. On the wire a `NewSession(project)` key is a
//! `newThread{projectId}` target and a `Session(id)` key a
//! `thread{threadId}` target: the ids are whatever the session layer
//! passes, and the daemon accepts only orchestration project and thread ids
//! (RFC 0003 §5.2, "Changed by merging the catalog into the #55 cut-over").

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use uuid::Uuid;

use super::{DaemonV8, remote_code};
use crate::v8::{
    self, DraftChange, DraftTarget, DraftsApplyParams, DraftsItem, DraftsSpec, ErrorCode, NoParams,
    StreamEvent, StreamItem, SubscribeOptions, method, stream,
};
use waku_protocol::persistence::{
    ComposerDraft, ComposerDraftAttachment, ComposerDraftKey, ComposerDrafts,
};

/// A lost drafts stream is resubscribed after this.
const RESUBSCRIBE_INTERVAL: Duration = Duration::from_secs(1);
/// Revisions this client applied whose echo the stream has not delivered.
const OWN_REVISIONS: usize = 32;

/// One draft set (`Some`) or removed (`None`).
pub type DraftEdit = (ComposerDraftKey, Option<ComposerDraft>);

/// One draft another client changed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteDraftChange {
    pub key: ComposerDraftKey,
    /// What the daemon held before: an editing copy that still equals it
    /// has no local edits and takes the change.
    pub previous: Option<ComposerDraft>,
    /// `None` removes the draft.
    pub draft: Option<ComposerDraft>,
}

fn wire_attachment(attachment: &ComposerDraftAttachment) -> v8::DraftAttachment {
    v8::DraftAttachment {
        path: attachment.path.to_string_lossy().into_owned(),
        mention: attachment.mention.clone(),
        name: attachment.name.clone(),
        is_dir: attachment.is_dir,
        is_image: attachment.is_image,
        blob_reference: attachment.blob_reference.clone(),
    }
}

fn domain_attachment(attachment: v8::DraftAttachment) -> ComposerDraftAttachment {
    ComposerDraftAttachment {
        path: PathBuf::from(attachment.path),
        mention: attachment.mention,
        name: attachment.name,
        is_dir: attachment.is_dir,
        is_image: attachment.is_image,
        blob_reference: attachment.blob_reference,
    }
}

pub fn wire_draft(draft: &ComposerDraft) -> v8::ComposerDraft {
    v8::ComposerDraft {
        text: draft.text.clone(),
        attachments: draft.attachments.iter().map(wire_attachment).collect(),
    }
}

pub fn domain_draft(draft: v8::ComposerDraft) -> ComposerDraft {
    ComposerDraft {
        text: draft.text,
        attachments: draft
            .attachments
            .into_iter()
            .map(domain_attachment)
            .collect(),
    }
}

pub fn domain_drafts(drafts: v8::ComposerDrafts) -> ComposerDrafts {
    ComposerDrafts {
        new_sessions: drafts
            .new_threads
            .into_iter()
            .map(|(id, draft)| (id, domain_draft(draft)))
            .collect(),
        sessions: drafts
            .threads
            .into_iter()
            .map(|(id, draft)| (id, domain_draft(draft)))
            .collect(),
    }
}

pub fn wire_target(key: ComposerDraftKey) -> DraftTarget {
    match key {
        ComposerDraftKey::NewSession(project_id) => DraftTarget::NewThread { project_id },
        ComposerDraftKey::Session(thread_id) => DraftTarget::Thread { thread_id },
    }
}

pub fn domain_key(target: DraftTarget) -> Option<ComposerDraftKey> {
    match target {
        DraftTarget::NewThread { project_id } => Some(ComposerDraftKey::NewSession(project_id)),
        DraftTarget::Thread { thread_id } => Some(ComposerDraftKey::Session(thread_id)),
        DraftTarget::Unknown => None,
    }
}

fn diff_map(
    previous: &HashMap<Uuid, ComposerDraft>,
    next: &HashMap<Uuid, ComposerDraft>,
    key: impl Fn(Uuid) -> ComposerDraftKey,
    changes: &mut Vec<DraftEdit>,
) {
    let mut ids = next
        .iter()
        .filter(|(id, draft)| previous.get(id) != Some(draft))
        .map(|(id, _)| *id)
        .collect::<Vec<_>>();
    ids.sort();
    for id in ids {
        let draft = next[&id].clone();
        changes.push((key(id), (!draft.is_empty()).then_some(draft)));
    }
    let mut removed = previous
        .keys()
        .filter(|id| !next.contains_key(id))
        .copied()
        .collect::<Vec<_>>();
    removed.sort();
    changes.extend(removed.into_iter().map(|id| (key(id), None)));
}

/// What turns `previous` into `next`, keyed draft by draft.
pub fn diff_drafts(previous: &ComposerDrafts, next: &ComposerDrafts) -> Vec<DraftEdit> {
    let mut changes = Vec::new();
    diff_map(
        &previous.new_sessions,
        &next.new_sessions,
        ComposerDraftKey::NewSession,
        &mut changes,
    );
    diff_map(
        &previous.sessions,
        &next.sessions,
        ComposerDraftKey::Session,
        &mut changes,
    );
    changes
}

fn wire_change((key, draft): &DraftEdit) -> DraftChange {
    DraftChange {
        target: wire_target(*key),
        draft: draft.as_ref().map(wire_draft),
    }
}

fn apply_change(drafts: &mut ComposerDrafts, (key, draft): DraftEdit) {
    match draft {
        Some(draft) => {
            drafts.set(key, draft);
        }
        None => {
            drafts.remove(key);
        }
    }
}

#[derive(Default)]
struct StoreState {
    /// The drafts as the daemon holds them, as far as this client knows.
    snapshot: ComposerDrafts,
    latest_generation: u64,
    /// Revisions of this client's own applies the stream has not echoed
    /// yet, oldest first. A revision is a content hash, so another client
    /// can produce one this client produced earlier: each is matched once,
    /// in order.
    own_revisions: VecDeque<u64>,
}

/// The desktop's composer drafts on the daemon: one baseline snapshot to
/// diff saves against, kept current by the `drafts` stream.
#[derive(Clone)]
pub struct DraftStore {
    daemon: DaemonV8,
    state: Arc<Mutex<StoreState>>,
}

impl DraftStore {
    pub fn new(daemon: DaemonV8) -> Self {
        Self {
            daemon,
            state: Arc::new(Mutex::new(StoreState::default())),
        }
    }

    /// Every draft (`drafts.get`), which becomes the baseline. Blocks.
    pub fn load(&self) -> anyhow::Result<ComposerDrafts> {
        let snapshot = self.daemon.call::<method::DraftsGet>(NoParams {})?;
        let drafts = domain_drafts(snapshot.drafts);
        self.state.lock().snapshot = drafts.clone();
        Ok(drafts)
    }

    /// Send what changed between the baseline and `drafts`. A save older
    /// than one already sent (`generation`) is dropped. A draft whose
    /// project or thread the daemon does not know is skipped rather than
    /// failing the rest. Blocks.
    pub fn save(&self, drafts: ComposerDrafts, generation: u64) -> anyhow::Result<()> {
        // Holding the state through the round trip serializes this client's
        // saves and lets the stream recognize the echo of this one.
        let mut state = self.state.lock();
        if generation < state.latest_generation {
            return Ok(());
        }
        let changes = diff_drafts(&state.snapshot, &drafts);
        if changes.is_empty() {
            state.latest_generation = generation;
            return Ok(());
        }
        let applied = match self.apply(changes.iter().map(wire_change).collect()) {
            Ok(revision) => {
                state.own_revisions.push_back(revision);
                changes
            }
            Err(error) if remote_code(&error) == Some(ErrorCode::NotFound) => {
                let mut applied = Vec::new();
                for change in changes {
                    match self.apply(vec![wire_change(&change)]) {
                        Ok(revision) => {
                            state.own_revisions.push_back(revision);
                            applied.push(change);
                        }
                        Err(error) if remote_code(&error) == Some(ErrorCode::NotFound) => {}
                        Err(error) => return Err(error),
                    }
                }
                applied
            }
            Err(error) => return Err(error),
        };
        while state.own_revisions.len() > OWN_REVISIONS {
            state.own_revisions.pop_front();
        }
        for change in applied {
            apply_change(&mut state.snapshot, change);
        }
        state.latest_generation = generation;
        Ok(())
    }

    fn apply(&self, changes: Vec<DraftChange>) -> anyhow::Result<u64> {
        Ok(self
            .daemon
            .call::<method::DraftsApply>(DraftsApplyParams {
                base_revision: None,
                changes,
                op_id: Uuid::new_v4(),
            })?
            .revision)
    }

    /// Follow the `drafts` stream from a worker thread: `on_change` gets
    /// the changes other clients make, already folded into the baseline
    /// (apply them to the editing copy too, or the next save reverts them).
    /// The thread stops once `on_change` returns `false`.
    pub fn follow(
        &self,
        on_change: impl FnMut(Vec<RemoteDraftChange>) -> bool + Send + 'static,
    ) -> anyhow::Result<()> {
        let store = self.clone();
        std::thread::Builder::new()
            .name("waku-drafts-stream".into())
            .spawn(move || store.follow_drafts(on_change))?;
        Ok(())
    }

    fn follow_drafts(&self, mut on_change: impl FnMut(Vec<RemoteDraftChange>) -> bool) {
        let mut after = None;
        loop {
            let Ok(client) = self.daemon.client() else {
                std::thread::sleep(RESUBSCRIBE_INTERVAL);
                continue;
            };
            let subscription = client.subscribe::<stream::Drafts>(
                DraftsSpec {},
                SubscribeOptions {
                    after: after.clone(),
                    window: None,
                },
            );
            while let Ok(event) = subscription.events().recv() {
                let changes = match event {
                    StreamEvent::Item(StreamItem::Drafts(item)) => match item {
                        DraftsItem::Snapshot { revision, drafts } => {
                            after = Some(v8::StreamCursor::Revision { revision });
                            self.remote_snapshot(revision, domain_drafts(drafts))
                        }
                        DraftsItem::Changed { revision, changes } => {
                            after = Some(v8::StreamCursor::Revision { revision });
                            self.remote_changes(revision, changes)
                        }
                        _ => continue,
                    },
                    StreamEvent::End(_) => break,
                    _ => continue,
                };
                if !changes.is_empty() && !on_change(changes) {
                    subscription.unsubscribe();
                    return;
                }
            }
            std::thread::sleep(RESUBSCRIBE_INTERVAL);
        }
    }

    fn remote_snapshot(&self, revision: u64, drafts: ComposerDrafts) -> Vec<RemoteDraftChange> {
        let mut state = self.state.lock();
        if let Some(echo) = state.own_revisions.iter().position(|own| *own == revision) {
            state.own_revisions.drain(..=echo);
        } else if !state.own_revisions.is_empty() {
            // Taken before this client's latest applies, whose echoes follow;
            // the baseline is already newer.
            return Vec::new();
        }
        let changes = diff_drafts(&state.snapshot, &drafts)
            .into_iter()
            .map(|(key, draft)| RemoteDraftChange {
                key,
                previous: state.snapshot.get(key).cloned(),
                draft,
            })
            .collect();
        state.snapshot = drafts;
        changes
    }

    fn remote_changes(&self, revision: u64, changes: Vec<DraftChange>) -> Vec<RemoteDraftChange> {
        let mut state = self.state.lock();
        if let Some(echo) = state.own_revisions.iter().position(|own| *own == revision) {
            state.own_revisions.drain(..=echo);
            return Vec::new();
        }
        let changes = changes
            .into_iter()
            .filter_map(|change| {
                let key = domain_key(change.target)?;
                let draft = change
                    .draft
                    .map(domain_draft)
                    .filter(|draft| !draft.is_empty());
                Some((key, draft))
            })
            .filter(|(key, draft)| state.snapshot.get(*key) != draft.as_ref())
            .collect::<Vec<_>>();
        changes
            .into_iter()
            .map(|(key, draft)| {
                let previous = state.snapshot.get(key).cloned();
                apply_change(&mut state.snapshot, (key, draft.clone()));
                RemoteDraftChange {
                    key,
                    previous,
                    draft,
                }
            })
            .collect()
    }
}

/// The wire drafts for `drafts`, for tests and tools.
pub fn wire_drafts(drafts: &ComposerDrafts) -> v8::ComposerDrafts {
    v8::ComposerDrafts {
        new_threads: drafts
            .new_sessions
            .iter()
            .map(|(id, draft)| (*id, wire_draft(draft)))
            .collect::<BTreeMap<_, _>>(),
        threads: drafts
            .sessions
            .iter()
            .map(|(id, draft)| (*id, wire_draft(draft)))
            .collect::<BTreeMap<_, _>>(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn draft(text: &str) -> ComposerDraft {
        ComposerDraft {
            text: text.into(),
            attachments: vec![ComposerDraftAttachment {
                path: "/srv/app/shot.png".into(),
                mention: "shot.png".into(),
                name: "shot.png".into(),
                is_dir: false,
                is_image: true,
                blob_reference: Some("waku-attachment:1".into()),
            }],
        }
    }

    #[test]
    fn a_diff_sets_changed_drafts_and_removes_missing_ones() {
        let project = Uuid::from_u128(1);
        let thread = Uuid::from_u128(2);
        let gone = Uuid::from_u128(3);
        let mut previous = ComposerDrafts::default();
        previous.set(ComposerDraftKey::Session(gone), draft("old"));
        previous.set(ComposerDraftKey::Session(thread), draft("same"));
        let mut next = previous.clone();
        next.remove(ComposerDraftKey::Session(gone));
        next.set(ComposerDraftKey::NewSession(project), draft("new"));
        let changes = diff_drafts(&previous, &next);
        assert_eq!(
            changes,
            vec![
                (ComposerDraftKey::NewSession(project), Some(draft("new"))),
                (ComposerDraftKey::Session(gone), None),
            ]
        );
        let wire = changes.iter().map(wire_change).collect::<Vec<_>>();
        assert_eq!(
            wire[0].target,
            DraftTarget::NewThread {
                project_id: project
            }
        );
        assert_eq!(
            wire[0].draft.as_ref().unwrap().attachments[0].path,
            "/srv/app/shot.png"
        );
        assert_eq!(wire[1].target, DraftTarget::Thread { thread_id: gone });
        assert_eq!(wire[1].draft, None);
    }

    #[test]
    fn drafts_round_trip_through_the_wire_shape() {
        let mut drafts = ComposerDrafts::default();
        drafts.set(
            ComposerDraftKey::NewSession(Uuid::from_u128(7)),
            draft("hi"),
        );
        drafts.set(
            ComposerDraftKey::Session(Uuid::from_u128(8)),
            draft("there"),
        );
        assert_eq!(domain_drafts(wire_drafts(&drafts)), drafts);
    }

    #[test]
    fn own_echoes_are_dropped_and_remote_changes_folded_in() {
        let store = DraftStore::new(DaemonV8::fixed(super::super::Endpoint {
            address: "unix:/nonexistent".into(),
            token: String::new(),
        }));
        let thread = Uuid::from_u128(9);
        store.state.lock().own_revisions.push_back(41);
        let change = DraftChange {
            target: DraftTarget::Thread { thread_id: thread },
            draft: Some(wire_draft(&draft("remote"))),
        };
        assert!(store.remote_changes(41, vec![change.clone()]).is_empty());
        let changes = store.remote_changes(42, vec![change.clone()]);
        assert_eq!(
            changes,
            vec![RemoteDraftChange {
                key: ComposerDraftKey::Session(thread),
                previous: None,
                draft: Some(draft("remote")),
            }]
        );
        // Now part of the baseline: a repeat changes nothing.
        assert!(store.remote_changes(43, vec![change]).is_empty());
        // A snapshot without it removes it.
        assert_eq!(
            store.remote_snapshot(44, ComposerDrafts::default()),
            vec![RemoteDraftChange {
                key: ComposerDraftKey::Session(thread),
                previous: Some(draft("remote")),
                draft: None,
            }]
        );
    }

    #[test]
    fn an_own_revision_echoes_once() {
        let store = DraftStore::new(DaemonV8::fixed(super::super::Endpoint {
            address: "unix:/nonexistent".into(),
            token: String::new(),
        }));
        let thread = Uuid::from_u128(9);
        let set = DraftChange {
            target: DraftTarget::Thread { thread_id: thread },
            draft: Some(wire_draft(&draft("mine"))),
        };
        let clear = DraftChange {
            target: DraftTarget::Thread { thread_id: thread },
            draft: None,
        };
        store
            .state
            .lock()
            .snapshot
            .set(ComposerDraftKey::Session(thread), draft("mine"));
        store.state.lock().own_revisions.extend([1, 2]);
        // A snapshot from before both applies is older than the baseline.
        assert!(
            store
                .remote_snapshot(0, ComposerDrafts::default())
                .is_empty()
        );
        assert!(store.remote_changes(1, vec![]).is_empty());
        assert!(store.remote_changes(2, vec![set]).is_empty());
        // Another client returns the drafts to revision 1: its content hash
        // matches an echo already consumed, so it is remote.
        assert_eq!(
            store.remote_changes(1, vec![clear]),
            vec![RemoteDraftChange {
                key: ComposerDraftKey::Session(thread),
                previous: Some(draft("mine")),
                draft: None,
            }]
        );
    }
}
