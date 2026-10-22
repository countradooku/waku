use super::*;
use waku_client::commands::{self, NewThread, Outgoing, Selection, SendIntent};

fn numbered_title_suffix(title: &str) -> Option<(&str, usize)> {
    let (base, suffix) = title.rsplit_once(" (")?;
    let number = suffix.strip_suffix(')')?.parse().ok()?;
    (!base.is_empty() && number >= 2).then_some((base, number))
}

fn next_response_fork_title<'a>(
    source_title: &str,
    existing_titles: impl IntoIterator<Item = &'a str>,
) -> String {
    let existing_titles = existing_titles.into_iter().collect::<Vec<_>>();
    let base = numbered_title_suffix(source_title)
        .filter(|(base, _)| existing_titles.iter().any(|title| title == base))
        .map_or(source_title, |(base, _)| base);
    let highest_number = existing_titles
        .iter()
        .filter_map(|title| {
            if *title == base {
                Some(1)
            } else {
                numbered_title_suffix(title)
                    .filter(|(candidate_base, _)| *candidate_base == base)
                    .map(|(_, number)| number)
            }
        })
        .max()
        .unwrap_or(1);
    format!("{base} ({})", highest_number.saturating_add(1).max(2))
}

impl Waku {
    pub fn composer_focus(&self, cx: &App) -> FocusHandle {
        self.composer.read(cx).focus()
    }

    pub(super) fn selected_project(&self) -> Option<&Project> {
        self.project(self.state.selected_project?)
    }

    pub(super) fn selected_session(&self) -> Option<&ThreadView> {
        self.thread(self.state.selected_session?)
    }

    /// The directory every filesystem and provider operation for `session`
    /// must use. A task that has not run yet reads the local checkout until
    /// its first run prepares a worktree.
    pub(super) fn workspace_path_for_session<'a>(
        &'a self,
        session: &'a ThreadView,
    ) -> Option<&'a std::path::Path> {
        let project = self.project(session.project_id)?;
        Some(session.workspace.path().unwrap_or(&project.path))
    }

    pub(super) fn selected_workspace_path(&self) -> Option<&std::path::Path> {
        let session = self.selected_session()?;
        self.workspace_path_for_session(session)
    }

    pub(super) fn provider_probe(&self, provider: ProviderKind) -> Option<&ProviderProbe> {
        self.probes.iter().find(|probe| probe.provider == provider)
    }

    pub(super) fn request_provider_model_discovery(&mut self, provider: ProviderKind) {
        if !provider.supports_model_discovery()
            || self.provider_model_discoveries.contains(&provider)
        {
            return;
        }
        let Some(probe) = self
            .provider_probe(provider)
            .filter(|probe| probe.installed)
            .cloned()
        else {
            return;
        };
        self.provider_model_discoveries.insert(provider);
        self.provider_model_discoveries_pending.insert(provider);
        let provider_probe_tx = self.provider_probe_tx.clone();
        let event_wake = self.event_wake_tx.clone();
        let daemon_v8 = self.daemon_v8.clone();
        let settings_sync = self.settings_sync.clone();
        if std::thread::Builder::new()
            .name(format!("waku-{}-model-discovery", provider.id()))
            .spawn(move || {
                // The daemon probes the binary its settings name; let a
                // just-edited override land first.
                settings_sync.wait_settled(PROVIDER_SETTINGS_SETTLE);
                let discovered =
                    waku_client::surfaces::providers::discover_models(&daemon_v8, probe.clone())
                        .unwrap_or(probe);
                if provider_probe_tx.send(discovered).is_ok() {
                    signal_event_pump(&event_wake);
                }
            })
            .is_err()
        {
            self.provider_model_discoveries.remove(&provider);
            self.provider_model_discoveries_pending.remove(&provider);
        }
    }

    /// Re-run one provider's model-owned catalog discovery, for selectors whose
    /// contents can change while Waku stays open — models the user just
    /// authored in a provider's config, or DeepSeek's custom agent presets.
    /// The stale catalog stays on screen until the fresh probe lands, so an
    /// open menu never blanks into a loading state while it refreshes.
    pub(super) fn refresh_provider_model_discovery(&mut self, provider: ProviderKind) {
        if self.provider_model_discoveries_pending.contains(&provider) {
            return;
        }
        self.provider_model_discoveries.remove(&provider);
        self.request_provider_model_discovery(provider);
    }

    /// Ask every installed CLI for its version, one short-lived subprocess per
    /// provider on its own thread. Answers land in `provider_versions` through
    /// the drain loop; render reads only that map.
    pub(super) fn request_provider_version_probes(&mut self) {
        let targets = self
            .probes
            .iter()
            .filter(|probe| probe.installed)
            .map(|probe| probe.provider)
            .collect::<Vec<_>>();
        for provider in targets {
            if !self.provider_version_probes_pending.insert(provider) {
                continue;
            }
            let provider_version_tx = self.provider_version_tx.clone();
            let event_wake = self.event_wake_tx.clone();
            let daemon_v8 = self.daemon_v8.clone();
            let settings_sync = self.settings_sync.clone();
            if std::thread::Builder::new()
                .name(format!("waku-{}-version-probe", provider.id()))
                .spawn(move || {
                    settings_sync.wait_settled(PROVIDER_SETTINGS_SETTLE);
                    let version =
                        waku_client::surfaces::providers::probe(&daemon_v8, provider, true)
                            .ok()
                            .and_then(|(_, version)| version);
                    if provider_version_tx.send((provider, version)).is_ok() {
                        signal_event_pump(&event_wake);
                    }
                })
                .is_err()
            {
                self.provider_version_probes_pending.remove(&provider);
            }
        }
    }

    pub(super) fn drain_provider_version_events(&mut self) -> bool {
        let mut changed = false;
        while let Ok((provider, version)) = self.provider_version_events.try_recv() {
            self.provider_version_probes_pending.remove(&provider);
            self.provider_versions.insert(provider, version);
            changed = true;
        }
        changed
    }

    /// Re-detect provider CLIs off-thread — every provider for the Providers
    /// page's refresh, or one whose binary override just changed. Also re-runs
    /// model discovery and version probes for whatever the detection finds
    /// installed.
    pub(super) fn refresh_provider_detection(&mut self, scope: Option<ProviderKind>) {
        if self.provider_detection_remaining > 0 {
            return;
        }
        let providers = match scope {
            Some(provider) => vec![provider],
            None => ProviderKind::ALL.to_vec(),
        };
        self.provider_detection_remaining = providers.len();
        let provider_detection_tx = self.provider_detection_tx.clone();
        let event_wake = self.event_wake_tx.clone();
        let detect_providers = providers.clone();
        let daemon_v8 = self.daemon_v8.clone();
        let settings_sync = self.settings_sync.clone();
        if std::thread::Builder::new()
            .name("waku-provider-detection".into())
            .spawn(move || {
                // Detection follows a binary override edit: probe what the
                // daemon will run, not what it ran before the edit.
                settings_sync.wait_settled(PROVIDER_SETTINGS_SETTLE);
                for provider in detect_providers {
                    let probe = match waku_client::surfaces::providers::probe(
                        &daemon_v8, provider, false,
                    ) {
                        Ok((probe, _)) => probe,
                        Err(_) => ProviderProbe {
                            provider,
                            installed: false,
                            path: None,
                            models: crate::model_catalog::fallback_models(provider),
                            agent_presets: crate::model_catalog::fallback_agent_presets(provider),
                        },
                    };
                    if provider_detection_tx.send(probe).is_ok() {
                        signal_event_pump(&event_wake);
                    }
                }
            })
            .is_err()
        {
            self.provider_detection_remaining = 0;
            return;
        }
        // A refresh means "re-check everything about these providers":
        // clearing the per-launch guard lets each one's catalog discovery run
        // again as its detection lands below.
        for provider in providers {
            self.provider_model_discoveries.remove(&provider);
        }
    }

    pub(super) fn drain_provider_detection_events(&mut self) -> bool {
        let mut changed = false;
        let mut installed_providers = Vec::new();
        while let Ok(probe) = self.provider_detection_events.try_recv() {
            let provider = probe.provider;
            let installed = probe.installed;
            self.provider_detection_remaining = self.provider_detection_remaining.saturating_sub(1);
            if self.provider_detection_remaining == 0 {
                self.provider_detection_checked_at = Some(Instant::now());
            }
            if let Some(existing) = self
                .probes
                .iter_mut()
                .find(|existing| existing.provider == provider)
            {
                if self.provider_model_discoveries_pending.contains(&provider) {
                    // A manual refresh may overlap an older live discovery.
                    // Keep that newer catalog while still accepting PATH
                    // detection from this response.
                    existing.installed = probe.installed;
                    existing.path = probe.path;
                } else {
                    *existing = probe;
                }
            } else {
                self.probes.push(probe);
            }
            if installed {
                installed_providers.push(provider);
            } else {
                self.provider_versions.remove(&provider);
            }
            changed = true;
        }
        for provider in installed_providers {
            self.request_provider_model_discovery(provider);
        }
        if changed {
            self.request_provider_version_probes();
        }
        changed
    }

    /// Whether the provider can back a new session: installed and not switched
    /// off in the Providers settings.
    pub(super) fn provider_enabled(&self, provider: ProviderKind) -> bool {
        !self.state.disabled_providers.contains(&provider)
            && self
                .provider_probe(provider)
                .is_some_and(|probe| probe.installed)
    }

    /// Whether the model picker has no provider left to offer — nothing
    /// detected on this machine, or everything switched off — so the
    /// composer's trigger, the picker panel, and the send button all swap to
    /// their unavailable state.
    pub(super) fn model_picker_has_no_providers(&self) -> bool {
        let locked_provider = self
            .selected_session()
            .filter(|session| !session.messages.is_empty())
            .map(|session| session.provider);
        super::composer::picker_has_no_providers(
            &self.probes,
            &self.state.disabled_providers,
            locked_provider,
            self.provider_detection_checked_at.is_some(),
        )
    }

    pub(super) fn model_for_session<'a>(&'a self, session: &'a ThreadView) -> Option<&'a str> {
        session.model.as_deref().or_else(|| {
            self.provider_probe(session.provider)
                .and_then(ProviderProbe::preferred_model)
                .map(|model| model.id.as_str())
        })
    }

    pub(super) fn model_display_name(&self, provider: ProviderKind, model: Option<&str>) -> String {
        let Some(model) = model else {
            return provider.short_name().to_owned();
        };
        self.provider_probe(provider)
            .and_then(|probe| probe.model(model))
            .map(|candidate| candidate.name.clone())
            .unwrap_or_else(|| model.to_owned())
    }

    pub(super) fn model_metadata_for_session(
        &self,
        session: &ThreadView,
    ) -> Option<&ProviderModel> {
        let model = self.model_for_session(session)?;
        self.provider_probe(session.provider)?.model(model)
    }

    pub(super) fn catalog_model_id_for_session<'a>(
        &'a self,
        session: &'a ThreadView,
    ) -> Option<&'a str> {
        self.model_metadata_for_session(session)
            .map(|model| model.id.as_str())
            .or_else(|| self.model_for_session(session))
    }

    pub(super) fn selected_transcript_blocks(&self) -> &[Arc<TranscriptBlock>] {
        self.selected_session()
            .map(|session| &session.transcript_blocks[..])
            .unwrap_or(&[])
    }

    /// Persist the desktop's own files (preferences, layout, selection) and
    /// the daemon settings. Task data is the daemon's and is never saved
    /// from here.
    pub(super) fn save(&mut self) {
        profile_span!(DEBUG, "Waku::save");
        perf_scope!(save_scope);
        // Queued for the settings writer's thread; never waits on the daemon.
        self.settings_sync.update(self.state.daemon_settings());
        let app_error = self
            .store
            .save(&self.state)
            .err()
            .map(|error| error.to_string());
        if let Some(error) = app_error {
            self.show_toast(tr!("errors.save_local_state", error = error));
        }
    }

    /// The provider, model and options a thread's next run uses: the
    /// thread's own choices, with the provider's preferred model and only
    /// the options that model offers.
    pub(super) fn selection_for(&self, session: &ThreadView) -> Selection {
        let model = session.model.clone().or_else(|| {
            self.provider_probe(session.provider)
                .and_then(ProviderProbe::preferred_model)
                .map(|model| model.id.clone())
        });
        let model_metadata = self.model_metadata_for_session(session);
        let mut reasoning_effort = session.reasoning_effort.clone().filter(|effort| {
            model_metadata.is_some_and(|model| {
                model
                    .reasoning_efforts
                    .iter()
                    .any(|option| option.id == *effort)
            })
        });
        let mut service_tier = session.service_tier.clone().filter(|tier| {
            tier == "default"
                || model_metadata.is_some_and(|model| {
                    model.service_tiers.iter().any(|option| option.id == *tier)
                })
        });
        let context_window = session.context_window.clone().filter(|window| {
            model_metadata.is_some_and(|model| {
                model
                    .context_windows
                    .iter()
                    .any(|option| option.id == *window)
            })
        });
        if session.provider == ProviderKind::Cursor
            && let Some(requested) = model.as_deref()
            && let Some(probe) = self.provider_probe(session.provider)
            && let Some(matched) =
                waku_protocol::model_catalog::cursor_catalog_model(&probe.models, requested)
        {
            if reasoning_effort.is_none() {
                reasoning_effort = waku_protocol::model_catalog::cursor_suffix_reasoning_effort(
                    &matched.suffix,
                    &matched.model.reasoning_efforts,
                );
            }
            if service_tier.is_none() {
                service_tier = waku_protocol::model_catalog::cursor_suffix_service_tier(
                    &matched.suffix,
                    &matched.model.service_tiers,
                );
            }
        }
        Selection {
            provider: session.provider,
            model,
            reasoning_effort,
            service_tier,
            context_window,
            agent_preset: self.agent_preset_for_session(session),
        }
    }

    pub(super) fn agent_preset_for_session(&self, session: &ThreadView) -> Option<String> {
        if session.provider != ProviderKind::DeepSeek {
            return None;
        }
        session.agent_preset.clone().or_else(|| {
            self.provider_probe(session.provider)
                .and_then(ProviderProbe::preferred_agent_preset)
                .map(|preset| preset.id.clone())
        })
    }

    pub(super) fn agent_preset_label_for_session(&self, session: &ThreadView) -> Option<String> {
        let id = self.agent_preset_for_session(session)?;
        Some(
            self.provider_probe(session.provider)
                .and_then(|probe| probe.agent_presets.iter().find(|preset| preset.id == id))
                .map(|preset| preset.display_name())
                .unwrap_or(id),
        )
    }

    /// Send what the composer submitted: the first message of a draft
    /// creates its thread; otherwise it starts a turn or queues behind the
    /// running one.
    pub(super) fn submit_composer_submission(
        &mut self,
        submission: ComposerSubmission,
        cx: &mut Context<Self>,
    ) {
        self.send_submission(submission, SendIntent::Submit, cx);
    }

    /// Deliver a steering message into the running turn; a session that
    /// cannot take one queues it instead.
    pub(super) fn steer_composer_submission(
        &mut self,
        submission: ComposerSubmission,
        cx: &mut Context<Self>,
    ) {
        self.send_submission(submission, SendIntent::Steer, cx);
    }

    pub(super) fn session_can_steer(&self, session: &ThreadView) -> bool {
        session.active_run_id.is_some() && session.can_steer
    }

    fn send_submission(
        &mut self,
        mut submission: ComposerSubmission,
        intent: SendIntent,
        cx: &mut Context<Self>,
    ) {
        let Some(view) = self.selected_session() else {
            return;
        };
        let thread_id = view.id;
        let provider = view.provider;
        submission.prompt = self.resolve_provider_submission(provider, &submission.prompt);
        if submission.prompt.trim().is_empty() && submission.attachments.is_empty() {
            return;
        }
        self.analytics
            .track(crate::analytics::Event::TurnSubmitted {
                provider: provider.id(),
                model: self
                    .selection_for(view)
                    .model
                    .unwrap_or_else(|| "default".into()),
                turn_number: view.turns.len() + 1,
                workspace: if view.workspace.is_worktree() {
                    "worktree"
                } else {
                    "local"
                },
                projectless: self
                    .project(view.project_id)
                    .is_some_and(Project::is_projectless),
                attachment_count: submission.attachments.len(),
                has_input: !submission.prompt.trim().is_empty(),
            });
        let message_id = Uuid::new_v4();
        let mut outgoing = submission.outgoing();
        outgoing.message_id = Some(message_id);
        if self
            .threads
            .drafts
            .get(&thread_id)
            .is_some_and(|draft| !draft.has_started())
        {
            self.start_drafted_thread(thread_id, &outgoing, submission, cx);
        } else {
            let mode = commands::dispatch_mode(view, intent);
            let command = commands::send_message(thread_id, &outgoing, mode, None);
            self.dispatch(
                command,
                ThreadAction::Send {
                    thread_id,
                    submission,
                },
            );
        }
        self.anchor_sent_message(thread_id, message_id);
        cx.notify();
    }

    /// `thread.create` then the first `message.dispatch`. The draft shows
    /// the prompt and a working turn until the stream has the real ones.
    fn start_drafted_thread(
        &mut self,
        thread_id: Uuid,
        outgoing: &Outgoing,
        submission: ComposerSubmission,
        cx: &mut Context<Self>,
    ) {
        let Some(draft) = self.threads.drafts.get(&thread_id) else {
            return;
        };
        let new = NewThread {
            thread_id,
            project_id: draft.project_id,
            selection: self.selection_for(draft),
            runtime_mode: draft.runtime_mode,
            workspace: draft.workspace.clone(),
        };
        let now = unix_time();
        let turn_id = Uuid::new_v4();
        if let Some(draft) = self.threads.drafts.get_mut(&thread_id) {
            let mut message = Message::new_for_turn(MessageRole::User, &submission.prompt, turn_id)
                .with_presentation(
                    submission.display_content.clone(),
                    submission.attachments.clone(),
                );
            message.id = outgoing.message_id.unwrap_or(message.id);
            draft.messages.push(message);
            draft.turns.push(AgentTurn {
                id: turn_id,
                turn_count: 1,
                status: TurnStatus::Running,
                provider_turn_started: false,
                provider_resume_at: None,
                started_at: now,
                completed_at: None,
                checkpoint: None,
            });
            draft.status = SessionStatus::Connecting;
            draft.last_reply_at = Some(now);
        }
        // Subscribe before the daemon knows the thread: the store waits for
        // the creation and then streams it.
        self.threads.store.open_thread(thread_id);
        let commands = commands::start_thread(&new, outgoing);
        let tickets = self.threads.store.dispatch_all(commands.into());
        self.threads
            .actions_insert(tickets[0], ThreadAction::Create { thread_id });
        self.threads.actions_insert(
            tickets[1],
            ThreadAction::Send {
                thread_id,
                submission,
            },
        );
        let _ = cx;
    }

    /// Hold the sent message at the top of the viewport while its reply
    /// streams below it.
    fn anchor_sent_message(&mut self, thread_id: Uuid, message_id: Uuid) {
        if self.state.selected_session != Some(thread_id) {
            return;
        }
        self.activities_expanded.clear();
        self.expanded_activity_items.clear();
        self.expanded_turns.clear();
        self.expanded_changed_files.clear();
        self.transcript_control_focuses.borrow_mut().clear();
        self.message_edit = None;
        self.hide_toast();
        self.transcript_anchor.set(Some(TranscriptAnchor {
            session_id: thread_id,
            message_id,
        }));
        // Provisional reservation: the anchored list has no measured bounds
        // until its first paint, and a zero end space cannot hold the sent
        // row at the viewport top. Seed a full viewport of end space; the
        // first measured frame trues it up.
        let mut provisional = self.transcript_rows.viewport_bounds().size.height;
        if provisional <= Pixels::ZERO {
            provisional = self.anchored_transcript_rows.viewport_bounds().size.height;
        }
        self.transcript_anchor_end_space.set(provisional);
        self.transcript_anchor_following.set(true);
        self.sync_transcript_rows();
        self.scroll_transcript_to_anchor();
    }

    /// Resolve presentation-preserving composer syntax immediately before a
    /// prompt crosses into a provider transport.
    fn resolve_provider_submission(&self, provider: ProviderKind, prompt: &str) -> String {
        crate::composer_complete::resolved_submission(provider, prompt, &self.slash_command_index)
            .unwrap_or_else(|| prompt.to_owned())
    }

    /// `queued_run.cancel`: the queued message leaves the queue.
    pub(super) fn remove_queued_message(
        &mut self,
        session_id: Uuid,
        message_id: Uuid,
        cx: &mut Context<Self>,
    ) {
        self.dispatch(
            commands::cancel_queued(session_id, message_id),
            ThreadAction::Other,
        );
        cx.notify();
    }

    /// Pop a queued message back into the composer so the user can edit and
    /// resubmit it: the queued run is cancelled.
    pub(super) fn edit_queued_message(
        &mut self,
        session_id: Uuid,
        message_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(message) = self.thread(session_id).and_then(|view| {
            view.queued_messages
                .iter()
                .find(|message| message.id == message_id)
                .cloned()
        }) else {
            return;
        };
        self.dispatch(
            commands::cancel_queued(session_id, message_id),
            ThreadAction::Other,
        );
        self.restore_composer_submission(ComposerSubmission::from_queued_message(message), cx);
        let focus_handle = self.composer_focus(cx);
        window.focus(&focus_handle, cx);
        cx.notify();
    }

    /// Deliver a queued follow-up into the running turn now: the queued run
    /// is cancelled and its message sent as a steer (the daemon has no
    /// promote command yet, #55 notes).
    pub(super) fn steer_queued_message(
        &mut self,
        session_id: Uuid,
        message_id: Uuid,
        cx: &mut Context<Self>,
    ) {
        let Some((message, mode)) = self.thread(session_id).and_then(|view| {
            let message = view
                .queued_messages
                .iter()
                .find(|message| message.id == message_id)
                .cloned()?;
            Some((message, commands::dispatch_mode(view, SendIntent::Steer)))
        }) else {
            return;
        };
        if matches!(mode, DispatchMode::QueueAfterActive) {
            // Nothing to steer into: it already waits in the queue.
            return;
        }
        let submission = ComposerSubmission::from_queued_message(message);
        self.dispatch(
            commands::cancel_queued(session_id, message_id),
            ThreadAction::Other,
        );
        let command = commands::send_message(session_id, &submission.outgoing(), mode, None);
        self.dispatch(
            command,
            ThreadAction::Send {
                thread_id: session_id,
                submission,
            },
        );
        cx.notify();
    }

    /// Activate the same action as the oldest queued row's Steer control.
    pub(super) fn steer_oldest_queued_message(&mut self, cx: &mut Context<Self>) {
        let Some((session_id, message_id)) = self.selected_session().and_then(|session| {
            if !self.session_can_steer(session) {
                return None;
            }
            Some((session.id, session.queued_messages.first()?.id))
        }) else {
            return;
        };
        self.steer_queued_message(session_id, message_id, cx);
    }

    /// `thread.fork` through the turn `turn_count` into a new thread, which
    /// is selected once the shell lists it.
    pub(super) fn fork_session_from_response(
        &mut self,
        session_id: Uuid,
        turn_count: usize,
        cx: &mut Context<Self>,
    ) {
        let Some(view) = self.thread(session_id) else {
            self.show_toast(tr!("session.response_unavailable"));
            cx.notify();
            return;
        };
        if view.is_busy() {
            self.show_toast(tr!("session.response_cannot_fork"));
            cx.notify();
            return;
        }
        let Some(turn_id) = view
            .turns
            .iter()
            .find(|turn| turn.turn_count == turn_count)
            .map(|turn| turn.id)
        else {
            self.show_toast(tr!("session.response_unavailable"));
            cx.notify();
            return;
        };
        let title = next_response_fork_title(
            view.display_title(),
            self.threads.listed().map(ThreadView::display_title),
        );
        let target = Uuid::new_v4();
        self.analytics
            .track(crate::analytics::Event::ResponseForked {
                provider: view.provider.id(),
                turn_number: turn_count,
            });
        let command = commands::fork(view, turn_id, target, Some(title));
        self.dispatch(command, ThreadAction::Other);
        self.threads.store.open_thread(target);
        cx.notify();
    }

    pub(super) fn begin_message_edit(
        &mut self,
        action: UserMessageAction,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let UserMessageAction {
            session_id,
            message_id,
            turn_count,
        } = action;
        let Some((message_index, initial_message, attachments)) = self
            .thread(session_id)
            .filter(|session| matches!(session.status, SessionStatus::Idle | SessionStatus::Failed))
            .and_then(|session| {
                let turn = session
                    .turns
                    .iter()
                    .find(|turn| turn.turn_count == turn_count)?;
                session
                    .messages
                    .iter()
                    .enumerate()
                    .find_map(|(index, message)| {
                        (message.id == message_id
                            && message.turn_id == Some(turn.id)
                            && message.role == MessageRole::User)
                            .then(|| {
                                (
                                    index,
                                    message.visible_content().to_owned(),
                                    message.attachments.clone(),
                                )
                            })
                    })
            })
        else {
            self.show_toast(tr!("session.message_not_editable"));
            cx.notify();
            return;
        };

        let input = cx.new(|cx| ComposerInput::new(window, cx).padding_x(px(12.0), cx));
        input.update(cx, |input, cx| input.set_content(initial_message, cx));
        cx.subscribe(
            &input,
            |this: &mut Self, _, event: &ComposerEvent, cx| match event {
                ComposerEvent::Submit(prompt) => {
                    this.submit_message_edit_prompt(prompt.clone(), cx)
                }
                // An edited past message resubmits from that point; there is
                // no running turn for it to steer.
                ComposerEvent::SubmitSteer(prompt) => {
                    this.submit_message_edit_prompt(prompt.clone(), cx)
                }
                ComposerEvent::SteerQueued => {}
                ComposerEvent::Edited => cx.notify(),
                ComposerEvent::Focus => {}
                ComposerEvent::BackspaceOnEmpty => {}
            },
        )
        .detach();
        self.message_edit = Some(MessageEdit {
            session_id,
            message_id,
            turn_count,
            input: input.clone(),
            attachments,
        });
        self.hide_toast();
        self.remeasure_transcript_message(message_index);
        let focus_handle = input.read(cx).focus();
        window.focus(&focus_handle, cx);
        cx.notify();
    }

    pub(super) fn cancel_message_edit(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(edit) = self.message_edit.take() else {
            return;
        };
        let message_index = self.selected_session().and_then(|session| {
            session
                .messages
                .iter()
                .position(|message| message.id == edit.message_id)
        });
        if let Some(message_index) = message_index {
            self.remeasure_transcript_message(message_index);
        }
        let focus_handle = self.composer_focus(cx);
        window.focus(&focus_handle, cx);
        cx.notify();
    }

    pub(super) fn submit_message_edit(&mut self, cx: &mut Context<Self>) {
        let prompt = self
            .message_edit
            .as_ref()
            .map(|edit| edit.input.read(cx).content(cx).to_owned())
            .unwrap_or_default();
        self.submit_message_edit_prompt(prompt, cx);
    }

    /// Resend an edited past message: `checkpoint.rollback` to before its
    /// turn (files included), then the edited text once that applied.
    fn submit_message_edit_prompt(&mut self, prompt: String, cx: &mut Context<Self>) {
        let Some(edit) = self.message_edit.clone() else {
            return;
        };
        let prompt = prompt.trim().to_owned();
        if prompt.is_empty() && edit.attachments.is_empty() {
            self.show_toast(tr!("session.edited_message_empty"));
            cx.notify();
            return;
        }
        let Some(turn_id) = self.thread(edit.session_id).and_then(|view| {
            view.turns
                .iter()
                .find(|turn| turn.turn_count == edit.turn_count)
                .map(|turn| turn.id)
        }) else {
            return;
        };
        let mentions = edit
            .attachments
            .iter()
            .map(|attachment| attachment.mention.clone())
            .collect::<Vec<_>>();
        let provider_prompt = composer::merged_submission(&prompt, &mentions)
            .expect("edited text or retained attachments always form a submission");
        let display_content = (!edit.attachments.is_empty()).then_some(prompt);
        self.message_edit = None;
        self.start_rewind(
            edit.session_id,
            turn_id,
            ComposerSubmission {
                prompt: provider_prompt,
                display_content,
                attachments: edit.attachments,
            },
            cx,
        );
    }

    pub(super) fn drain_event_pump(&mut self, cx: &mut Context<Self>) -> EventPumpSchedule {
        profile_span!(DEBUG, "drain_event_pump");
        perf_scope!(pump_scope);
        // The store says what each update changed, so a stream commit
        // rebuilds the transcript island alone instead of fanning a root
        // notify out to every pane (`DirtySet`). The other result queues are
        // rare and keep the whole-window notify.
        let (mut dirty, streamed) = self.drain_store(cx);
        // `|` on purpose: a busy stream must not starve the other result
        // queues just because its own drain reported a change first.
        if self.drain_provider_probe_events()
            | self.drain_provider_version_events()
            | self.drain_provider_detection_events()
            | self.drain_computer_permission_events()
            | self.drain_plan_usage_events()
        {
            dirty = DirtySet::ALL;
        }
        self.notify_dirty(dirty, cx);
        if std::mem::take(&mut self.workspace_queries_stale) {
            self.invalidate_workspace_queries(cx);
        }
        if std::mem::take(&mut self.composer_sources_stale) {
            self.refresh_composer_sources(cx);
        }
        self.maybe_refresh_background_work(cx);

        if streamed {
            // Live text: the next drain waits a full commit interval, so the
            // provider's chunk rate never becomes the redraw rate.
            EventPumpSchedule::StreamFrame
        } else if let Some(delay) = self.background_output_refresh_delay() {
            EventPumpSchedule::BackgroundOutput(delay)
        } else {
            EventPumpSchedule::Idle
        }
    }

    pub(super) fn drain_provider_probe_events(&mut self) -> bool {
        let mut changed = false;
        while let Ok(probe) = self.provider_probe_events.try_recv() {
            self.provider_model_discoveries_pending
                .remove(&probe.provider);
            if let Some(existing) = self
                .probes
                .iter_mut()
                .find(|existing| existing.provider == probe.provider)
            {
                *existing = probe;
            } else {
                self.probes.push(probe);
            }
            changed = true;
        }
        changed
    }

    pub(super) fn drain_computer_permission_events(&mut self) -> bool {
        let mut changed = false;
        while let Ok(result) = self.computer_permission_events.try_recv() {
            self.computer_permission_request_pending = false;
            match result {
                Ok(permissions) => self.computer_permissions = permissions,
                Err(error) => self.show_toast(error),
            }
            changed = true;
        }
        changed
    }
}

#[cfg(test)]
mod response_fork_title_tests {
    use super::next_response_fork_title;

    #[test]
    fn response_fork_titles_advance_one_numbered_sequence() {
        assert_eq!(
            next_response_fork_title("Fix the bug", ["Fix the bug"]),
            "Fix the bug (2)"
        );
        assert_eq!(
            next_response_fork_title(
                "Fix the bug",
                ["Fix the bug", "Fix the bug (2)", "Fix the bug (4)"]
            ),
            "Fix the bug (5)"
        );
        assert_eq!(
            next_response_fork_title("Fix the bug (2)", ["Fix the bug", "Fix the bug (2)"]),
            "Fix the bug (3)"
        );
        assert_eq!(
            next_response_fork_title("Plan (2026)", ["Plan (2026)"]),
            "Plan (2026) (2)"
        );
    }
}

#[cfg(test)]
mod version_tests {
    use crate::model::parse_cli_version;

    #[test]
    fn parses_common_cli_version_banners() {
        assert_eq!(
            parse_cli_version("codex-cli 0.45.0\n"),
            Some("0.45.0".to_owned())
        );
        assert_eq!(
            parse_cli_version("2.1.24 (Claude Code)\n"),
            Some("2.1.24".to_owned())
        );
        assert_eq!(
            parse_cli_version("v1.3.0-beta.2"),
            Some("1.3.0-beta.2".to_owned())
        );
        assert_eq!(
            parse_cli_version("\nAmp CLI version 0.9.12\n"),
            Some("0.9.12".to_owned())
        );
        assert_eq!(parse_cli_version("not a version"), None);
        assert_eq!(parse_cli_version(""), None);
    }

    #[test]
    fn version_requires_a_dotted_number_not_a_bare_digit() {
        // "2024" alone or a hash must not read as a version.
        assert_eq!(parse_cli_version("build 2024 f3a9c1"), None);
        assert_eq!(
            parse_cli_version("cursor-agent 2025.09.12-4f8d8e2"),
            Some("2025.09.12-4f8d8e2".to_owned())
        );
    }
}
