//! `ImportLegacyProject`, `ImportLegacyThread` and `ImportLegacyTranscript`
//! in `decide` (#63). The reader and the conversion are tested against real
//! `app.db` fixtures in `waku-core` (`legacy_import_tests`).

use std::path::PathBuf;

use waku_protocol::model as legacy;
use waku_protocol::orchestration::*;

use super::common::*;
use crate::command::{CommandError, InternalCommand};
use crate::import::legacy::{shell_from_session, transcript_from_session};
use crate::orchestrator::DispatchError;

fn internal(env: &Env, id: &str, command: InternalCommand) -> Result<(), DispatchError> {
    env.tick();
    env.orchestrator
        .dispatch_internal(cid(id), command)
        .map(|_| ())
}

#[test]
fn transcripts_import_once_and_only_into_their_own_imported_thread() {
    let env = Env::new();
    let project = Project {
        id: fresh(),
        name: "Old".into(),
        path: PathBuf::from("/old"),
        order_key: OrderKey::new("00000000000"),
        created_at: 1_000,
        updated_at: 1_000,
        removed_at: None,
    };
    internal(
        &env,
        "import-project",
        InternalCommand::ImportLegacyProject {
            project: project.clone(),
        },
    )
    .unwrap();
    // A second import of the same project (another source) is a no-op.
    let sequence = env.orchestrator.last_sequence();
    internal(
        &env,
        "import-project-again",
        InternalCommand::ImportLegacyProject {
            project: Project {
                name: "Renamed elsewhere".into(),
                ..project.clone()
            },
        },
    )
    .unwrap();
    assert_eq!(env.orchestrator.last_sequence(), sequence);
    assert_eq!(
        env.orchestrator.project(project.id).unwrap().created_at,
        1_000
    );

    let mut session = legacy::AgentSession::new(project.id.0, legacy::ProviderKind::Codex);
    session.begin_turn("old question");
    session.push_message(legacy::MessageRole::Assistant, "old answer");
    session.finish_active_turn(legacy::TurnStatus::Completed);
    let shell = shell_from_session(&session);
    let thread_id = shell.thread.id;
    internal(
        &env,
        "import-thread",
        InternalCommand::ImportLegacyThread { shell },
    )
    .unwrap();
    let projection = env.projection(thread_id);
    let messages = session
        .messages
        .iter()
        .map(|message| (**message).clone())
        .collect::<Vec<_>>();
    let transcript =
        transcript_from_session(&projection.thread, None, &project.path, &session, &messages);
    assert_eq!(transcript.runs.len(), 1);
    assert_eq!(transcript.items.len(), 2);

    // Rows of another thread are refused.
    let mut foreign = transcript.clone();
    foreign.items[0].thread_id = fresh();
    let error = internal(
        &env,
        "foreign",
        InternalCommand::ImportLegacyTranscript {
            thread_id,
            transcript: foreign,
        },
    )
    .unwrap_err();
    assert!(matches!(
        error,
        DispatchError::Rejected(CommandError::Invalid { .. })
    ));
    // A native thread never takes one.
    let native = env.thread(project.id);
    let error = internal(
        &env,
        "native",
        InternalCommand::ImportLegacyTranscript {
            thread_id: native,
            transcript: transcript.clone(),
        },
    )
    .unwrap_err();
    assert!(matches!(
        error,
        DispatchError::Rejected(CommandError::InvalidState { .. })
    ));

    internal(
        &env,
        "transcript",
        InternalCommand::ImportLegacyTranscript {
            thread_id,
            transcript: transcript.clone(),
        },
    )
    .unwrap();
    let projection = env.projection(thread_id);
    assert_eq!(projection.max_ordinal, 1);
    // Terminal runs keep their rows in SQLite only.
    assert!(projection.hot_items.is_empty());
    // Importing again (another command id) changes nothing.
    let sequence = env.orchestrator.last_sequence();
    internal(
        &env,
        "transcript-again",
        InternalCommand::ImportLegacyTranscript {
            thread_id,
            transcript,
        },
    )
    .unwrap();
    assert_eq!(env.orchestrator.last_sequence(), sequence);
    // The next native run continues the ordinals.
    let run = env.send(thread_id, "new question", DispatchMode::Auto);
    assert_eq!(env.run(thread_id, run).ordinal, Some(2));
    env.assert_verified();
}
