//! The task state the bench daemon serves: one project and deterministic
//! sessions, written into a fresh daemon database before the server starts.

use std::path::{Path, PathBuf};

use anyhow::Context as _;
use serde_json::json;
use uuid::Uuid;
use waku_core::model::{
    ActivityItem, ActivityKind, AgentSession, AgentTurn, Message, MessageRole, Project,
    ProviderKind, TranscriptBlock, TurnStatus,
};
use waku_core::persistence::{PersistedState, StateStore};

use crate::content;
use crate::rng::Rng;

/// Fixed so a client's selection and a runner's scripts survive restarts.
pub const PROJECT_ID: Uuid = Uuid::from_u128(0xbe4c_0000_0000_4000_8000_0000_0000_0001);
/// Sessions are stamped from here so ordering never depends on the clock.
const BASE_TIME: u64 = 1_790_000_000;

pub struct Fixture {
    pub project: Project,
    pub sessions: Vec<AgentSession>,
    /// Which timeline a prompt in each session replays.
    pub bindings: Vec<(Uuid, String)>,
}

/// The id of the session bound to `timeline`.
pub fn session_id(timeline: &str) -> Uuid {
    let seed = timeline.bytes().fold(0x811c_9dc5_u32, |hash, byte| {
        (hash ^ u32::from(byte)).wrapping_mul(0x0100_0193)
    });
    Rng::new(seed).uuid()
}

pub fn build(
    project_path: PathBuf,
    timelines: &[String],
    provider: ProviderKind,
    history_turns: usize,
) -> Fixture {
    let project = Project {
        id: PROJECT_ID,
        name: project_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("bench-project")
            .to_owned(),
        path: project_path,
        created_at: BASE_TIME,
    };
    let model = waku_protocol::model_catalog::fallback_models(provider)
        .into_iter()
        .find(|model| model.is_default)
        .map(|model| model.id);
    let mut sessions = Vec::new();
    let mut bindings = Vec::new();
    // Newest first in the list: the first timeline gets the latest stamp.
    let count = timelines.len() as u64 + 1;
    for (index, timeline) in timelines.iter().enumerate() {
        let mut session = AgentSession::new(project.id, provider);
        session.id = session_id(timeline);
        session.title = format!("Bench · {timeline}");
        session.model.clone_from(&model);
        session.created_at = BASE_TIME + (count - index as u64) * 60;
        // The store keeps only sessions that have started, so each timeline
        // session opens with one settled turn saying what a prompt replays.
        let mut rng = Rng::new(session.id.as_u128() as u32);
        let turn_id = rng.uuid();
        let at = session.created_at;
        let intro = format!(
            "Any prompt in this session replays the `{timeline}` timeline. A prompt whose \
             whole text is another timeline's name (prose, burst, reasoning-fast, code-wall, \
             tool-storm, images) replays that one instead."
        );
        session.messages.push(message(
            &mut rng,
            MessageRole::User,
            format!("Bench: {timeline}"),
            turn_id,
            at,
        ));
        session.messages.push(message(
            &mut rng,
            MessageRole::Assistant,
            intro,
            turn_id,
            at,
        ));
        session.turns.push(AgentTurn {
            id: turn_id,
            turn_count: 1,
            status: TurnStatus::Completed,
            provider_turn_started: true,
            provider_resume_at: None,
            started_at: at,
            completed_at: Some(at),
            checkpoint: None,
        });
        session.updated_at = at;
        session.last_reply_at = Some(at);
        bindings.push((session.id, timeline.clone()));
        sessions.push(session);
    }
    if history_turns > 0 {
        let mut session = history_session(project.id, provider, history_turns);
        session.model.clone_from(&model);
        sessions.push(session);
    }
    Fixture {
        project,
        sessions,
        bindings,
    }
}

/// A long, settled conversation in the seed script's shapes, for transcripts
/// that must already be long when a timeline starts streaming.
fn history_session(project_id: Uuid, provider: ProviderKind, turns: usize) -> AgentSession {
    let mut rng = Rng::new(20_260_806);
    let mut session = AgentSession::new(project_id, provider);
    session.id = Rng::new(0x6869_7374).uuid();
    session.title = format!("Bench · history ({turns} turns)");
    let mut clock = BASE_TIME - turns as u64 * 60;
    session.created_at = clock;
    for turn_index in 0..turns {
        let turn_id = rng.uuid();
        let started_at = clock;
        let prompt = (*rng.pick(content::PROMPTS)).to_owned();
        session
            .messages
            .push(message(&mut rng, MessageRole::User, prompt, turn_id, clock));
        clock += rng.int(2, 12);

        let activity_count = rng.int(1, 3);
        let activities = (0..activity_count)
            .map(|_| history_activity(&mut rng))
            .collect::<Vec<_>>();
        session.transcript_blocks.push(TranscriptBlock {
            after_message: session.messages.len(),
            turn_id: Some(turn_id),
            activities,
        });
        clock += rng.int(5, 40);

        let bytes = rng.int(300, 1_400) as usize;
        let answer = content::markdown_answer(&mut rng, bytes);
        session.messages.push(message(
            &mut rng,
            MessageRole::Assistant,
            answer,
            turn_id,
            clock,
        ));
        clock += rng.int(1, 8);
        session.turns.push(AgentTurn {
            id: turn_id,
            turn_count: turn_index + 1,
            status: TurnStatus::Completed,
            provider_turn_started: true,
            provider_resume_at: None,
            started_at,
            completed_at: Some(clock),
            checkpoint: None,
        });
        clock += rng.int(5, 30);
    }
    session.updated_at = clock;
    session.last_reply_at = Some(clock);
    session
}

fn message(rng: &mut Rng, role: MessageRole, content: String, turn_id: Uuid, at: u64) -> Message {
    let mut message = Message::new_for_turn(role, content, turn_id);
    message.id = rng.uuid();
    message.created_at = at;
    message
}

fn history_activity(rng: &mut Rng) -> ActivityItem {
    let id = rng.uuid();
    let call_id = rng.call_id();
    let mut item = if rng.chance(0.5) {
        let command = (*rng.pick(content::COMMANDS)).to_owned();
        let lines = rng.int(6, 30);
        ActivityItem::new(
            Some(call_id),
            ActivityKind::Command,
            command.clone(),
            None,
            true,
        )
        .with_tool_name(Some("Bash"))
        .with_arguments(Some(json!({"command": command}).to_string()))
        .with_output(Some(content::build_log(rng, lines)))
    } else {
        let hits = rng.int(4, 12);
        ActivityItem::new(
            Some(call_id),
            ActivityKind::FileSearch,
            "Search",
            None,
            true,
        )
        .with_tool_name(Some("Grep"))
        .with_output(Some(content::search_results(rng, hits)))
    };
    item.id = id;
    item
}

/// Write the fixture into a fresh daemon database at `state_path`.
pub fn seed(state_path: &Path, fixture: &Fixture) -> anyhow::Result<()> {
    let store = StateStore::daemon(state_path.to_owned());
    let mut state = PersistedState::empty();
    state.projects.push(fixture.project.clone());
    state.selected_project = Some(fixture.project.id);
    state.selected_session = fixture.sessions.first().map(|session| session.id);
    for session in &fixture.sessions {
        state.push_session(session.clone());
    }
    store
        .save(&mut state)
        .with_context(|| format!("could not seed {}", state_path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_ids_and_content_are_stable() {
        let names = vec!["prose".to_owned(), "burst".to_owned()];
        let a = build(PathBuf::from("/tmp/p"), &names, ProviderKind::Codex, 5);
        let b = build(PathBuf::from("/tmp/p"), &names, ProviderKind::Codex, 5);
        assert_eq!(a.sessions.len(), 3);
        for (left, right) in a.sessions.iter().zip(&b.sessions) {
            assert_eq!(left.id, right.id);
            assert_eq!(left.created_at, right.created_at);
            assert_eq!(left.messages.len(), right.messages.len());
            for (l, r) in left.messages.iter().zip(&right.messages) {
                assert_eq!((l.id, &l.content), (r.id, &r.content));
            }
        }
        assert_eq!(a.bindings[0], (session_id("prose"), "prose".to_owned()));
        assert_ne!(session_id("prose"), session_id("burst"));
        assert_eq!(a.sessions[2].turns.len(), 5);
        assert_eq!(a.sessions[2].messages.len(), 10);
    }
}
