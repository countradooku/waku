//! Conversions from the legacy v7 types in [`crate::model`], for the
//! importer (#63) and the cutover (#55).

use super::ids::JsonText;
use super::values::*;
use crate::model;

impl From<model::RuntimeMode> for RuntimeMode {
    fn from(mode: model::RuntimeMode) -> Self {
        match mode {
            model::RuntimeMode::Ask => Self::Ask,
            model::RuntimeMode::AutoAcceptEdits => Self::AutoAcceptEdits,
            model::RuntimeMode::Auto => Self::Auto,
            model::RuntimeMode::FullAccess => Self::FullAccess,
        }
    }
}

impl From<model::ProviderKind> for DriverKind {
    fn from(kind: model::ProviderKind) -> Self {
        match kind {
            model::ProviderKind::Amp => Self::Amp,
            model::ProviderKind::Claude => Self::Claude,
            model::ProviderKind::Codex => Self::Codex,
            model::ProviderKind::Cursor => Self::Cursor,
            model::ProviderKind::DeepSeek => Self::DeepSeek,
            model::ProviderKind::Fx => Self::Fx,
            model::ProviderKind::OpenCode => Self::OpenCode,
            model::ProviderKind::Grok => Self::Grok,
            model::ProviderKind::Kimi => Self::Kimi,
            model::ProviderKind::OhMyPi => Self::OhMyPi,
            model::ProviderKind::Pi => Self::Pi,
        }
    }
}

impl From<&model::MessageAttachment> for Attachment {
    fn from(attachment: &model::MessageAttachment) -> Self {
        Self {
            path: attachment.path.clone(),
            mention: attachment.mention.clone(),
            name: attachment.name.clone(),
            is_dir: attachment.is_dir,
            is_image: attachment.is_image,
            blob_reference: attachment.blob_reference.clone(),
        }
    }
}

impl From<&model::PermissionOption> for PermissionOption {
    fn from(option: &model::PermissionOption) -> Self {
        Self {
            id: option.id.clone(),
            label: option.label.clone(),
            allow: option.allow,
        }
    }
}

impl From<&model::UserInputQuestion> for UserInputQuestion {
    fn from(question: &model::UserInputQuestion) -> Self {
        Self {
            id: question.id.clone(),
            header: question.header.clone(),
            question: question.question.clone(),
            options: question
                .options
                .iter()
                .map(|option| UserInputOption {
                    label: option.label.clone(),
                    description: option.description.clone(),
                })
                .collect(),
            multi_select: question.multi_select,
        }
    }
}

impl From<&model::UserInputAnswer> for UserInputAnswer {
    fn from(answer: &model::UserInputAnswer) -> Self {
        Self {
            question_id: answer.question_id.clone(),
            answers: answer.answers.clone(),
        }
    }
}

impl From<model::ThreadGoalStatus> for ThreadGoalStatus {
    fn from(status: model::ThreadGoalStatus) -> Self {
        match status {
            model::ThreadGoalStatus::Active => Self::Active,
            model::ThreadGoalStatus::Paused => Self::Paused,
            model::ThreadGoalStatus::Blocked => Self::Blocked,
            model::ThreadGoalStatus::UsageLimited => Self::UsageLimited,
            model::ThreadGoalStatus::BudgetLimited => Self::BudgetLimited,
            model::ThreadGoalStatus::Complete => Self::Complete,
        }
    }
}

impl From<&model::ThreadGoal> for ThreadGoal {
    fn from(goal: &model::ThreadGoal) -> Self {
        Self {
            objective: goal.objective.clone(),
            status: goal.status.into(),
            token_budget: goal.token_budget,
            tokens_used: goal.tokens_used,
            time_used_seconds: goal.time_used_seconds,
        }
    }
}

impl From<model::ContextUsage> for ContextUsage {
    fn from(usage: model::ContextUsage) -> Self {
        Self {
            tokens: usage.tokens,
            window: usage.window,
        }
    }
}

/// A legacy resume cursor as a strong native thread ref plus the cursor
/// itself as opaque JSON (R23).
pub fn resume_cursor_ref(
    cursor: &model::ProviderResumeCursor,
) -> serde_json::Result<(ProviderRef, JsonText)> {
    let reference = ProviderRef::strong(cursor.provider().into(), cursor.native_id());
    Ok((reference, JsonText(serde_json::to_string(cursor)?)))
}
