//! Builders for transcript fixtures in the desktop's window-free tests.
//!
//! Production code never edits a [`ThreadView`]: views come from the
//! projection store (#55). Tests still need transcripts of a given shape to
//! exercise the row fold, the fingerprints and the footers, so these build
//! one in place, the way a stream's view would look.

use super::*;

/// An empty task in `project_id`, as a fixture.
pub(super) fn test_session(project_id: Uuid, provider: ProviderKind) -> ThreadView {
    ThreadView::draft(
        Uuid::new_v4(),
        project_id,
        provider,
        RuntimeMode::FullAccess,
        unix_time(),
    )
}

pub(super) trait TranscriptFixture {
    /// Open a running turn with its prompt; returns the turn id.
    fn begin_turn(&mut self, prompt: impl Into<String>) -> Uuid;
    /// Append a message to the running turn (or to no turn).
    fn push_message(&mut self, role: MessageRole, content: impl Into<String>) -> Uuid;
    fn push_user_message_with_presentation(
        &mut self,
        content: impl Into<String>,
        display_content: Option<String>,
        attachments: Vec<MessageAttachment>,
    ) -> Uuid;
    /// Settle the running turn.
    fn finish_active_turn(&mut self, status: TurnStatus) -> Option<(Uuid, usize)>;
}

impl TranscriptFixture for ThreadView {
    fn begin_turn(&mut self, prompt: impl Into<String>) -> Uuid {
        let id = Uuid::new_v4();
        let now = unix_time();
        self.turns.push(AgentTurn {
            id,
            turn_count: self.turns.len() + 1,
            status: TurnStatus::Running,
            provider_turn_started: false,
            provider_resume_at: None,
            started_at: now,
            completed_at: None,
            checkpoint: None,
        });
        self.messages
            .push(Message::new_for_turn(MessageRole::User, prompt, id));
        self.last_reply_at = Some(now);
        id
    }

    fn push_message(&mut self, role: MessageRole, content: impl Into<String>) -> Uuid {
        let mut message = Message::new(role, content);
        message.turn_id = self.active_turn_id();
        let id = message.id;
        self.messages.push(message);
        id
    }

    fn push_user_message_with_presentation(
        &mut self,
        content: impl Into<String>,
        display_content: Option<String>,
        attachments: Vec<MessageAttachment>,
    ) -> Uuid {
        let mut message = Message::new(MessageRole::User, content)
            .with_presentation(display_content, attachments);
        message.turn_id = self.active_turn_id();
        let id = message.id;
        self.messages.push(message);
        id
    }

    fn finish_active_turn(&mut self, status: TurnStatus) -> Option<(Uuid, usize)> {
        let turn = self
            .turns
            .last_mut()
            .filter(|turn| turn.status == TurnStatus::Running)?;
        turn.status = status;
        turn.completed_at = Some(unix_time());
        Some((turn.id, turn.turn_count))
    }
}
