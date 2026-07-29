use super::*;
use codex_protocol::config_types::MultiAgentMode;

pub(super) const THREAD_UNLOADING_DELAY: Duration = Duration::from_secs(30 * 60);
const SPINE_UI_TERMINAL_BARRIER_TIMEOUT: Duration = Duration::from_secs(10);
const THREAD_UNLOADING_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub(super) struct ListenerTaskContext {
    pub(super) thread_manager: Arc<ThreadManager>,
    pub(super) thread_store: Arc<dyn ThreadStore>,
    pub(super) thread_state_manager: ThreadStateManager,
    pub(super) outgoing: Arc<OutgoingMessageSender>,
    pub(super) pending_thread_unloads: Arc<Mutex<HashSet<ThreadId>>>,
    pub(super) thread_watch_manager: ThreadWatchManager,
    pub(super) thread_list_state_permit: Arc<Semaphore>,
    pub(super) fallback_model_provider: String,
    pub(super) codex_home: PathBuf,
    pub(super) skills_watcher: Arc<SkillsWatcher>,
}

struct UnloadingState {
    delay: Duration,
    has_subscribers_rx: watch::Receiver<bool>,
    has_subscribers: (bool, Instant),
    thread_status_rx: watch::Receiver<ThreadStatus>,
    is_active: (bool, Instant),
    retry_not_before: Option<Instant>,
}

impl UnloadingState {
    async fn new(
        listener_task_context: &ListenerTaskContext,
        thread_id: ThreadId,
        delay: Duration,
    ) -> Option<Self> {
        let has_subscribers_rx = listener_task_context
            .thread_state_manager
            .subscribe_to_has_connections(thread_id)
            .await?;
        let thread_status_rx = listener_task_context
            .thread_watch_manager
            .subscribe(thread_id)
            .await?;
        let has_subscribers = (*has_subscribers_rx.borrow(), Instant::now());
        let is_active = (
            matches!(*thread_status_rx.borrow(), ThreadStatus::Active { .. }),
            Instant::now(),
        );
        Some(Self {
            delay,
            has_subscribers_rx,
            has_subscribers,
            thread_status_rx,
            is_active,
            retry_not_before: None,
        })
    }

    fn unloading_target(&self) -> Option<Instant> {
        match (self.has_subscribers, self.is_active) {
            ((false, has_no_subscribers_since), (false, is_inactive_since)) => {
                let target =
                    std::cmp::max(has_no_subscribers_since, is_inactive_since) + self.delay;
                Some(
                    self.retry_not_before
                        .map_or(target, |retry| target.max(retry)),
                )
            }
            _ => None,
        }
    }

    fn sync_receiver_values(&mut self) {
        let has_subscribers = *self.has_subscribers_rx.borrow();
        if self.has_subscribers.0 != has_subscribers {
            self.has_subscribers = (has_subscribers, Instant::now());
            self.retry_not_before = None;
        }

        let is_active = matches!(*self.thread_status_rx.borrow(), ThreadStatus::Active { .. });
        if self.is_active.0 != is_active {
            self.is_active = (is_active, Instant::now());
            self.retry_not_before = None;
        }
    }

    fn should_unload_now(&mut self) -> bool {
        self.sync_receiver_values();
        self.unloading_target()
            .is_some_and(|target| target <= Instant::now())
    }

    fn note_thread_activity_observed(&mut self) {
        self.retry_not_before = None;
        if !self.is_active.0 {
            self.is_active = (false, Instant::now());
        }
    }

    fn retry_unload_soon(&mut self) {
        self.retry_not_before = Some(Instant::now() + THREAD_UNLOADING_RETRY_DELAY);
    }

    async fn wait_for_unloading_trigger(&mut self) -> bool {
        loop {
            self.sync_receiver_values();
            let unloading_target = self.unloading_target();
            if let Some(target) = unloading_target
                && target <= Instant::now()
            {
                return true;
            }
            let unloading_sleep = async {
                if let Some(target) = unloading_target {
                    tokio::time::sleep_until(target.into()).await;
                } else {
                    futures::future::pending::<()>().await;
                }
            };
            tokio::select! {
                _ = unloading_sleep => return true,
                changed = self.has_subscribers_rx.changed() => {
                    if changed.is_err() {
                        return false;
                    }
                    self.sync_receiver_values();
                },
                changed = self.thread_status_rx.changed() => {
                    if changed.is_err() {
                        return false;
                    }
                    self.sync_receiver_values();
                },
            }
        }
    }
}

pub(super) enum ThreadShutdownResult {
    Complete,
    SubmitFailed,
    TimedOut,
}

pub(super) enum EnsureConversationListenerResult {
    Attached,
    ConnectionClosed,
}

#[expect(
    clippy::await_holding_invalid_type,
    reason = "listener subscription must be serialized against pending unloads"
)]
pub(super) async fn ensure_conversation_listener(
    listener_task_context: ListenerTaskContext,
    conversation_id: ThreadId,
    connection_id: ConnectionId,
    raw_events_enabled: bool,
) -> Result<EnsureConversationListenerResult, JSONRPCErrorError> {
    let conversation = match listener_task_context
        .thread_manager
        .get_thread(conversation_id)
        .await
    {
        Ok(conv) => conv,
        Err(_) => {
            return Err(invalid_request(format!(
                "thread not found: {conversation_id}"
            )));
        }
    };
    let thread_state = {
        let pending_thread_unloads = listener_task_context.pending_thread_unloads.lock().await;
        if pending_thread_unloads.contains(&conversation_id) {
            return Err(invalid_request(format!(
                "thread {conversation_id} is closing; retry after the thread is closed"
            )));
        }
        let Some(thread_state) = listener_task_context
            .thread_state_manager
            .try_ensure_connection_subscribed(conversation_id, connection_id, raw_events_enabled)
            .await
        else {
            return Ok(EnsureConversationListenerResult::ConnectionClosed);
        };
        thread_state
    };
    if let Err(error) = ensure_listener_task_running(
        listener_task_context.clone(),
        conversation_id,
        conversation,
        thread_state,
    )
    .await
    {
        let _ = listener_task_context
            .thread_state_manager
            .unsubscribe_connection_from_thread(conversation_id, connection_id)
            .await;
        return Err(error);
    }
    Ok(EnsureConversationListenerResult::Attached)
}

pub(super) fn log_listener_attach_result(
    result: Result<EnsureConversationListenerResult, JSONRPCErrorError>,
    thread_id: ThreadId,
    connection_id: ConnectionId,
    thread_kind: &'static str,
) {
    match result {
        Ok(EnsureConversationListenerResult::Attached) => {}
        Ok(EnsureConversationListenerResult::ConnectionClosed) => {
            tracing::debug!(
                thread_id = %thread_id,
                connection_id = ?connection_id,
                "skipping auto-attach for closed connection"
            );
        }
        Err(err) => {
            tracing::warn!(
                "failed to attach listener for {thread_kind} {thread_id}: {message}",
                message = err.message
            );
        }
    }
}

pub(super) async fn ensure_listener_task_running(
    listener_task_context: ListenerTaskContext,
    conversation_id: ThreadId,
    conversation: Arc<CodexThread>,
    thread_state: Arc<Mutex<ThreadState>>,
) -> Result<(), JSONRPCErrorError> {
    let (cancel_tx, mut cancel_rx) = oneshot::channel();
    let Some(mut unloading_state) = UnloadingState::new(
        &listener_task_context,
        conversation_id,
        THREAD_UNLOADING_DELAY,
    )
    .await
    else {
        return Err(invalid_request(format!(
            "thread {conversation_id} is closing; retry after the thread is closed"
        )));
    };
    let config = conversation.config().await;
    let environments = conversation.environment_selections().await;
    let watch_registration = listener_task_context
        .skills_watcher
        .register_thread_config(
            config.as_ref(),
            listener_task_context.thread_manager.as_ref(),
            &environments,
        )
        .await;
    let thread_settings_baseline =
        thread_settings_from_config_snapshot(&conversation.config_snapshot().await);
    let (mut listener_command_rx, listener_generation) = {
        let mut thread_state = thread_state.lock().await;
        if thread_state.listener_matches(&conversation) {
            return Ok(());
        }
        let (listener_command_rx, listener_generation) = thread_state.set_listener(
            cancel_tx,
            &conversation,
            watch_registration,
            thread_settings_baseline,
        );
        let Some(listener_command_tx) = thread_state.listener_command_tx() else {
            tracing::warn!(
                "thread listener command sender missing immediately after listener registration"
            );
            return Ok(());
        };
        listener_task_context
            .thread_state_manager
            .register_listener_command_tx(conversation_id, listener_command_tx);
        (listener_command_rx, listener_generation)
    };
    listener_task_context
        .thread_state_manager
        .note_spine_ui_listener_generation(conversation_id, listener_generation)
        .await;
    let ListenerTaskContext {
        outgoing,
        thread_manager,
        thread_store,
        thread_state_manager,
        pending_thread_unloads,
        thread_watch_manager,
        thread_list_state_permit,
        fallback_model_provider,
        codex_home,
        ..
    } = listener_task_context;
    let outgoing_for_task = Arc::clone(&outgoing);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = &mut cancel_rx => {
                    // Listener was superseded or the thread is being torn down.
                    break;
                }
                listener_command = listener_command_rx.recv() => {
                    let Some(listener_command) = listener_command else {
                        break;
                    };
                    handle_thread_listener_command(
                        conversation_id,
                        &conversation,
                        codex_home.as_path(),
                        thread_manager.as_ref(),
                        thread_store.as_ref(),
                        &thread_state_manager,
                        &thread_state,
                        &thread_watch_manager,
                        &thread_list_state_permit,
                        &outgoing_for_task,
                        &pending_thread_unloads,
                        listener_command,
                    )
                    .await;
                }
                event = conversation.next_event() => {
                    let event = match event {
                        Ok(event) => event,
                        Err(err) => {
                            tracing::warn!("thread.next_event() failed with: {err}");
                            break;
                        }
                    };

                    if matches!(&event.msg, EventMsg::TurnStarted(_)) {
                        thread_state_manager
                            .note_spine_ui_agent_turn_started(
                                conversation_id,
                                listener_generation,
                            )
                            .await;
                    }

                    if crate::spine_ui::is_enabled()
                        && matches!(&event.msg, EventMsg::TurnComplete(_) | EventMsg::TurnAborted(_))
                    {
                        let (agent_states, timed_out) = thread_state_manager
                            .wait_for_spine_ui_terminal_children(
                                conversation_id,
                                &event.id,
                                SPINE_UI_TERMINAL_BARRIER_TIMEOUT,
                            )
                            .await;
                        if !timed_out.is_empty() {
                            let child_thread_ids = timed_out
                                .iter()
                                .map(|(thread_id, _)| *thread_id)
                                .collect::<Vec<_>>();
                            tracing::warn!(
                                thread_id = %conversation_id,
                                turn_id = %event.id,
                                child_thread_ids = ?child_thread_ids,
                                "Spine UI terminal barrier timed out; using latest child states"
                            );
                        }
                        let mut state = thread_state.lock().await;
                        if state.live_spine_ui(&event.id).is_some() {
                            for (child_thread_id, generation, child_state) in agent_states {
                                if let Some(child_state) = child_state {
                                    state.record_spine_ui_agent_state(
                                        child_thread_id,
                                        generation,
                                        child_state,
                                    );
                                } else {
                                    state.invalidate_spine_ui_agent_state(
                                        child_thread_id,
                                        generation,
                                    );
                                }
                            }
                            for (child_thread_id, generation) in timed_out {
                                state.mark_spine_ui_agent_sync_timeout(
                                    child_thread_id,
                                    generation,
                                );
                            }
                        }
                    }

                    // Track the event before emitting any typed translations
                    // so thread-local state such as raw event opt-in stays
                    // synchronized with the conversation.
                    let raw_events_enabled = {
                        let mut thread_state = thread_state.lock().await;
                        thread_state.track_current_turn_event(&event.id, &event.msg);
                        thread_state.experimental_raw_events
                    };
                    if crate::spine_ui::is_enabled()
                        && let EventMsg::SpineSpawnProgress(progress) = &event.msg
                    {
                        thread_state_manager
                            .register_spine_ui_spawn_progress(
                                conversation_id,
                                &event.id,
                                progress,
                            )
                            .await;
                    }
                    if crate::spine_ui::is_enabled()
                        && matches!(&event.msg, EventMsg::ThreadRolledBack(_))
                    {
                        restore_persisted_spine_ui_ancestors(
                            conversation_id,
                            conversation.config_snapshot().await.parent_thread_id,
                            thread_store.as_ref(),
                            &thread_state_manager,
                        )
                        .await;
                        thread_state.lock().await.reset_spine_ui_after_rollback();
                        let completed_updates = thread_state_manager
                            .clear_all_spine_ui_routes_for_thread(conversation_id)
                            .await;
                        persist_spine_ui_completed_updates(
                            completed_updates,
                            thread_manager.as_ref(),
                            thread_store.as_ref(),
                            &thread_state_manager,
                            &outgoing_for_task,
                            &thread_list_state_permit,
                        )
                        .await;
                    }
                    if matches!(&event.msg, EventMsg::RawResponseItem(_)) && !raw_events_enabled {
                        continue;
                    }
                    let subscribed_connection_ids = thread_state_manager
                        .subscribed_connection_ids(conversation_id)
                        .await;
                    let thread_outgoing = ThreadScopedOutgoingMessageSender::new(
                        outgoing_for_task.clone(),
                        subscribed_connection_ids,
                        conversation_id,
                    );

                    apply_bespoke_event_handling(
                        event.clone(),
                        conversation_id,
                        conversation.clone(),
                        thread_manager.clone(),
                        thread_outgoing,
                        thread_state.clone(),
                        thread_watch_manager.clone(),
                        thread_list_state_permit.clone(),
                        fallback_model_provider.clone(),
                    )
                    .await;
                    if matches!(
                        &event.msg,
                        EventMsg::TurnStarted(_)
                            | EventMsg::SpineTreeUpdate(_)
                            | EventMsg::SpineSpawnProgress(_)
                            | EventMsg::TurnComplete(_)
                            | EventMsg::TurnAborted(_)
                    ) {
                        thread_state_manager
                            .queue_spine_ui_agent_state(conversation_id)
                            .await;
                    }
                    if matches!(&event.msg, EventMsg::TurnComplete(_) | EventMsg::TurnAborted(_)) {
                        thread_state_manager
                            .acknowledge_spine_ui_agent_terminal(
                                conversation_id,
                                listener_generation,
                                &event.id,
                            )
                            .await;
                        thread_state_manager
                            .clear_spine_ui_parent_routes(conversation_id, &event.id)
                            .await;
                    }
                }
                unloading_watchers_open = unloading_state.wait_for_unloading_trigger() => {
                    if !unloading_watchers_open {
                        break;
                    }
                    if !unloading_state.should_unload_now() {
                        continue;
                    }
                    if thread_state_manager
                        .has_timed_out_spine_ui_children(conversation_id)
                        .await
                    {
                        unloading_state.note_thread_activity_observed();
                        continue;
                    }
                    if matches!(conversation.agent_status().await, AgentStatus::Running) {
                        unloading_state.note_thread_activity_observed();
                        continue;
                    }
                    let Ok(thread_list_state_permit) = thread_list_state_permit
                        .clone()
                        .try_acquire_owned()
                    else {
                        unloading_state.retry_unload_soon();
                        continue;
                    };
                    {
                        let mut pending_thread_unloads = pending_thread_unloads.lock().await;
                        if pending_thread_unloads.contains(&conversation_id) {
                            continue;
                        }
                        if !unloading_state.should_unload_now() {
                            continue;
                        }
                        pending_thread_unloads.insert(conversation_id);
                    }
                    unload_thread_without_subscribers(
                        thread_manager.clone(),
                        outgoing_for_task.clone(),
                        pending_thread_unloads.clone(),
                        thread_state_manager.clone(),
                        thread_watch_manager.clone(),
                        conversation_id,
                        conversation.clone(),
                        thread_list_state_permit,
                    )
                    .await;
                    break;
                }
            }
        }

        let mut thread_state = thread_state.lock().await;
        if thread_state.listener_generation == listener_generation {
            thread_state_manager.unregister_listener_command_tx(conversation_id);
            thread_state.clear_listener();
        }
    });
    Ok(())
}

pub(super) async fn wait_for_thread_shutdown(thread: &Arc<CodexThread>) -> ThreadShutdownResult {
    match tokio::time::timeout(Duration::from_secs(10), thread.shutdown_and_wait()).await {
        Ok(Ok(())) => ThreadShutdownResult::Complete,
        Ok(Err(_)) => ThreadShutdownResult::SubmitFailed,
        Err(_) => ThreadShutdownResult::TimedOut,
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn unload_thread_without_subscribers(
    thread_manager: Arc<ThreadManager>,
    outgoing: Arc<OutgoingMessageSender>,
    pending_thread_unloads: Arc<Mutex<HashSet<ThreadId>>>,
    thread_state_manager: ThreadStateManager,
    thread_watch_manager: ThreadWatchManager,
    thread_id: ThreadId,
    thread: Arc<CodexThread>,
    thread_list_state_permit: tokio::sync::OwnedSemaphorePermit,
) {
    info!("thread {thread_id} has no subscribers and is idle; shutting down");

    // Any pending app-server -> client requests for this thread can no longer be
    // answered; cancel their callbacks before shutdown/unload.
    outgoing
        .cancel_requests_for_thread(thread_id, /*error*/ None)
        .await;
    thread_state_manager.remove_thread_state(thread_id).await;

    tokio::spawn(async move {
        let thread_list_state_permit = thread_list_state_permit;
        match wait_for_thread_shutdown(&thread).await {
            ThreadShutdownResult::Complete => {
                let was_loaded = thread_manager.remove_thread(&thread_id).await.is_some();
                pending_thread_unloads.lock().await.remove(&thread_id);
                drop(thread_list_state_permit);
                if !was_loaded {
                    info!("thread {thread_id} was already removed before teardown finalized");
                    thread_watch_manager
                        .remove_thread(&thread_id.to_string())
                        .await;
                    return;
                }
                thread_watch_manager
                    .remove_thread(&thread_id.to_string())
                    .await;
                let notification = ThreadClosedNotification {
                    thread_id: thread_id.to_string(),
                };
                outgoing
                    .send_server_notification(ServerNotification::ThreadClosed(notification))
                    .await;
            }
            ThreadShutdownResult::SubmitFailed => {
                pending_thread_unloads.lock().await.remove(&thread_id);
                drop(thread_list_state_permit);
                warn!("failed to submit Shutdown to thread {thread_id}");
            }
            ThreadShutdownResult::TimedOut => {
                pending_thread_unloads.lock().await.remove(&thread_id);
                drop(thread_list_state_permit);
                warn!("thread {thread_id} shutdown timed out; leaving thread loaded");
            }
        }
    });
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_thread_listener_command(
    conversation_id: ThreadId,
    conversation: &Arc<CodexThread>,
    codex_home: &Path,
    thread_manager: &ThreadManager,
    thread_store: &dyn ThreadStore,
    thread_state_manager: &ThreadStateManager,
    thread_state: &Arc<Mutex<ThreadState>>,
    thread_watch_manager: &ThreadWatchManager,
    thread_list_state_permit: &Arc<Semaphore>,
    outgoing: &Arc<OutgoingMessageSender>,
    pending_thread_unloads: &Arc<Mutex<HashSet<ThreadId>>>,
    listener_command: ThreadListenerCommand,
) {
    match listener_command {
        ThreadListenerCommand::SendThreadResumeResponse(resume_request) => {
            handle_pending_thread_resume_request(
                conversation_id,
                conversation,
                codex_home,
                thread_state_manager,
                thread_state,
                thread_watch_manager,
                thread_store,
                thread_list_state_permit,
                outgoing,
                pending_thread_unloads,
                *resume_request,
            )
            .await;
        }
        ThreadListenerCommand::EmitThreadGoalUpdated { turn_id, goal } => {
            outgoing
                .send_server_notification(ServerNotification::ThreadGoalUpdated(
                    ThreadGoalUpdatedNotification {
                        thread_id: conversation_id.to_string(),
                        turn_id,
                        goal,
                    },
                ))
                .await;
        }
        ThreadListenerCommand::EmitThreadGoalCleared => {
            outgoing
                .send_server_notification(ServerNotification::ThreadGoalCleared(
                    ThreadGoalClearedNotification {
                        thread_id: conversation_id.to_string(),
                    },
                ))
                .await;
        }
        ThreadListenerCommand::EmitThreadGoalSnapshot { state_db } => {
            send_thread_goal_snapshot_notification(outgoing, conversation_id, &state_db).await;
        }
        ThreadListenerCommand::ResolveServerRequest {
            request_id,
            completion_tx,
        } => {
            resolve_pending_server_request(
                conversation_id,
                thread_state_manager,
                outgoing,
                request_id,
            )
            .await;
            let _ = completion_tx.send(());
        }
        ThreadListenerCommand::ForwardSpineUiAgentState {
            child_thread_id,
            parent_turn_id,
            generation,
            state: child_state,
            terminal,
        } => {
            if !thread_state_manager
                .spine_ui_route_is_current(
                    child_thread_id,
                    conversation_id,
                    &parent_turn_id,
                    generation,
                )
                .await
            {
                return;
            }
            let (spine_ui, late_terminal_refresh) = {
                let mut state = thread_state.lock().await;
                if state.live_spine_ui(&parent_turn_id).is_some() {
                    let changed = child_state.is_some_and(|child_state| {
                        state.record_spine_ui_agent_state(child_thread_id, generation, child_state)
                    });
                    (
                        changed
                            .then(|| state.live_spine_ui(&parent_turn_id).cloned())
                            .flatten(),
                        None,
                    )
                } else if terminal {
                    (
                        None,
                        Some(state.record_completed_spine_ui_agent_terminal(
                            &parent_turn_id,
                            child_thread_id,
                            generation,
                            child_state,
                        )),
                    )
                } else {
                    (None, None)
                }
            };
            if let Some(spine_ui) = spine_ui {
                let connection_ids = thread_state_manager
                    .subscribed_connection_ids(conversation_id)
                    .await;
                let outgoing = ThreadScopedOutgoingMessageSender::new(
                    outgoing.clone(),
                    connection_ids,
                    conversation_id,
                );
                if let Some(notification) = crate::spine_ui::snapshot_started_notification(
                    &conversation_id.to_string(),
                    &parent_turn_id,
                    &spine_ui,
                ) {
                    outgoing
                        .send_server_notification(ServerNotification::ItemStarted(notification))
                        .await;
                }
                thread_state_manager
                    .queue_spine_ui_agent_state(conversation_id)
                    .await;
            }
            let mut completed_state = None;
            if let Some(refresh) = late_terminal_refresh {
                completed_state = refresh.completed.iter().find_map(|(turn_id, state)| {
                    (turn_id == &parent_turn_id).then_some(state.clone())
                });
                if let Some((turn_id, state)) = refresh.active
                    && let Some(notification) = crate::spine_ui::snapshot_started_notification(
                        &conversation_id.to_string(),
                        &turn_id,
                        &state,
                    )
                {
                    let connection_ids = thread_state_manager
                        .subscribed_connection_ids(conversation_id)
                        .await;
                    let outgoing = ThreadScopedOutgoingMessageSender::new(
                        outgoing.clone(),
                        connection_ids,
                        conversation_id,
                    );
                    outgoing
                        .send_server_notification(ServerNotification::ItemStarted(notification))
                        .await;
                }
                persist_spine_ui_completed_updates(
                    refresh
                        .completed
                        .into_iter()
                        .map(
                            |(turn_id, state)| crate::thread_state::SpineUiCompletedUpdate {
                                thread_id: conversation_id,
                                turn_id,
                                state,
                            },
                        )
                        .collect(),
                    thread_manager,
                    thread_store,
                    thread_state_manager,
                    outgoing,
                    thread_list_state_permit,
                )
                .await;
                if refresh.forward.is_some() {
                    thread_state_manager
                        .queue_spine_ui_agent_state(conversation_id)
                        .await;
                }
            }
            if terminal {
                if let Some(completed_state) = completed_state {
                    let completed_updates = thread_state_manager
                        .propagate_spine_ui_completed_agent_state(conversation_id, completed_state)
                        .await;
                    persist_spine_ui_completed_updates(
                        completed_updates,
                        thread_manager,
                        thread_store,
                        thread_state_manager,
                        outgoing,
                        thread_list_state_permit,
                    )
                    .await;
                }
                thread_state_manager
                    .complete_spine_ui_late_terminal(
                        child_thread_id,
                        conversation_id,
                        &parent_turn_id,
                        generation,
                    )
                    .await;
                thread_state_manager
                    .clear_spine_ui_parent_routes(conversation_id, &parent_turn_id)
                    .await;
            }
        }
        ThreadListenerCommand::EmitSpineUiInvalidation { turn_id, state } => {
            if let Some(notification) = crate::spine_ui::snapshot_started_notification(
                &conversation_id.to_string(),
                &turn_id,
                &state,
            ) {
                let connection_ids = thread_state_manager
                    .subscribed_connection_ids(conversation_id)
                    .await;
                ThreadScopedOutgoingMessageSender::new(
                    outgoing.clone(),
                    connection_ids,
                    conversation_id,
                )
                .send_server_notification(ServerNotification::ItemStarted(notification))
                .await;
            }
        }
    }
}

async fn restore_persisted_spine_ui_ancestors(
    child_thread_id: ThreadId,
    mut parent_thread_id: Option<ThreadId>,
    thread_store: &dyn ThreadStore,
    thread_state_manager: &ThreadStateManager,
) {
    let mut child_thread_id = child_thread_id;
    let mut visited = HashSet::new();
    while let Some(parent_id) = parent_thread_id {
        if !visited.insert(parent_id) {
            warn!("stopping cyclic Spine UI parent recovery at thread {parent_id}");
            break;
        }
        let stored = match thread_store
            .read_thread(StoreReadThreadParams {
                thread_id: parent_id,
                include_archived: true,
                include_history: true,
            })
            .await
        {
            Ok(stored) => stored,
            Err(err) => {
                warn!("failed to read parent thread {parent_id} for Spine UI rollback: {err}");
                break;
            }
        };
        if let Some(history) = stored.history.as_ref() {
            for (turn_id, state) in crate::spine_ui::persisted_completed_states_for_child(
                &history.items,
                child_thread_id,
            ) {
                thread_state_manager
                    .restore_spine_ui_completed_state(parent_id, turn_id, state)
                    .await;
            }
        }
        child_thread_id = parent_id;
        parent_thread_id = stored.parent_thread_id;
    }
}

async fn persist_spine_ui_completed_updates(
    updates: Vec<crate::thread_state::SpineUiCompletedUpdate>,
    thread_manager: &ThreadManager,
    thread_store: &dyn ThreadStore,
    thread_state_manager: &ThreadStateManager,
    outgoing: &Arc<OutgoingMessageSender>,
    thread_list_state_permit: &Arc<Semaphore>,
) {
    for update in updates {
        let Some(event) = crate::spine_ui::snapshot_completed_event(
            update.thread_id,
            &update.turn_id,
            &update.state,
        ) else {
            continue;
        };
        let Ok(thread_list_state_permit) = thread_list_state_permit.acquire().await else {
            warn!("failed to acquire thread list state permit for Spine UI completed update");
            return;
        };
        let rollout_item = RolloutItem::EventMsg(EventMsg::ItemCompleted(event.clone()));
        if let Ok(conversation) = thread_manager.get_thread(update.thread_id).await {
            if let Err(err) = conversation
                .append_rollout_items(std::slice::from_ref(&rollout_item))
                .await
            {
                warn!(
                    "failed to persist Spine UI completed update through loaded parent thread {}: {err}",
                    update.thread_id
                );
            }
        } else {
            persist_spine_ui_completed_update_to_unloaded_thread(&update, event, thread_store)
                .await;
        }
        let connection_ids = thread_state_manager
            .subscribed_connection_ids(update.thread_id)
            .await;
        drop(thread_list_state_permit);
        let scoped_outgoing = ThreadScopedOutgoingMessageSender::new(
            Arc::clone(outgoing),
            connection_ids,
            update.thread_id,
        );
        if let Some(notification) = crate::spine_ui::snapshot_completed_notification(
            &update.thread_id.to_string(),
            &update.turn_id,
            &update.state,
        ) {
            scoped_outgoing
                .send_server_notification(ServerNotification::ItemCompleted(notification))
                .await;
        }
    }
}

async fn persist_spine_ui_completed_update_to_unloaded_thread(
    update: &crate::thread_state::SpineUiCompletedUpdate,
    event: codex_protocol::protocol::ItemCompletedEvent,
    thread_store: &dyn ThreadStore,
) {
    let stored = match thread_store
        .read_thread(StoreReadThreadParams {
            thread_id: update.thread_id,
            include_archived: true,
            include_history: false,
        })
        .await
    {
        Ok(stored) => stored,
        Err(err) => {
            warn!(
                "failed to read unloaded parent thread {} for Spine UI rollback persistence: {err}",
                update.thread_id
            );
            return;
        }
    };
    let Some(rollout_path) = stored.rollout_path else {
        warn!(
            "unloaded parent thread {} has no local rollout path for Spine UI rollback persistence",
            update.thread_id
        );
        return;
    };
    if let Err(err) = codex_rollout::append_rollout_item_to_path(
        &rollout_path,
        &RolloutItem::EventMsg(EventMsg::ItemCompleted(event)),
    )
    .await
    {
        warn!(
            "failed to persist Spine UI rollback in unloaded parent thread {}: {err}",
            update.thread_id
        );
    }
}

#[allow(clippy::too_many_arguments)]
#[expect(
    clippy::await_holding_invalid_type,
    reason = "running-thread resume subscription must be serialized against pending unloads"
)]
pub(super) async fn handle_pending_thread_resume_request(
    conversation_id: ThreadId,
    conversation: &Arc<CodexThread>,
    _codex_home: &Path,
    thread_state_manager: &ThreadStateManager,
    thread_state: &Arc<Mutex<ThreadState>>,
    thread_watch_manager: &ThreadWatchManager,
    thread_store: &dyn ThreadStore,
    thread_list_state_permit: &Arc<Semaphore>,
    outgoing: &Arc<OutgoingMessageSender>,
    pending_thread_unloads: &Arc<Mutex<HashSet<ThreadId>>>,
    mut pending: crate::thread_state::PendingThreadResumeRequest,
) {
    let request_id = pending.request_id.clone();
    let Ok(thread_list_state_permit) = thread_list_state_permit.acquire().await else {
        outgoing
            .send_error(
                request_id,
                internal_error(
                    "failed to acquire thread list state permit for running thread resume",
                ),
            )
            .await;
        return;
    };
    let stored = match thread_store
        .read_thread(StoreReadThreadParams {
            thread_id: conversation_id,
            include_archived: true,
            include_history: true,
        })
        .await
    {
        Ok(stored) => stored,
        Err(err) => {
            outgoing
                .send_error(
                    request_id,
                    internal_error(format!(
                        "failed to refresh running thread history for resume: {err}"
                    )),
                )
                .await;
            return;
        }
    };
    let Some(history) = stored.history else {
        outgoing
            .send_error(
                request_id,
                internal_error("running thread resume did not include persisted history"),
            )
            .await;
        return;
    };
    pending.history_items = history.items;
    let active_turn = {
        let state = thread_state.lock().await;
        state.active_turn_snapshot()
    };
    tracing::debug!(
        thread_id = %conversation_id,
        request_id = ?pending.request_id,
        active_turn_present = active_turn.is_some(),
        active_turn_id = ?active_turn.as_ref().map(|turn| turn.id.as_str()),
        active_turn_status = ?active_turn.as_ref().map(|turn| &turn.status),
        "composing running thread resume response"
    );
    let has_live_in_progress_turn =
        matches!(conversation.agent_status().await, AgentStatus::Running)
            || active_turn
                .as_ref()
                .is_some_and(|turn| matches!(turn.status, TurnStatus::InProgress));

    let request_id = pending.request_id;
    let connection_id = request_id.connection_id;
    let mut thread = pending.thread_summary;
    if pending.include_turns {
        populate_thread_turns_from_history(
            &mut thread,
            &pending.history_items,
            active_turn.as_ref(),
        );
    }

    let thread_status = thread_watch_manager
        .loaded_status_for_thread(&thread.id)
        .await;

    set_thread_status_and_interrupt_stale_turns(
        &mut thread,
        thread_status,
        has_live_in_progress_turn,
    );
    let token_usage_thread = pending.include_turns.then(|| thread.clone());
    let mut initial_turns_page = if let Some(params) = pending.initial_turns_page.as_ref() {
        match super::thread_processor::build_thread_resume_initial_turns_page(
            &pending.history_items,
            thread.status.clone(),
            has_live_in_progress_turn,
            active_turn,
            params,
        ) {
            Ok(page) => Some(page),
            Err(error) => {
                outgoing.send_error(request_id, error).await;
                return;
            }
        }
    } else {
        None
    };
    if pending.redact_resume_payloads {
        redact_thread_resume_payloads(&mut thread.turns);
        if let Some(initial_turns_page) = initial_turns_page.as_mut() {
            redact_thread_resume_payloads(&mut initial_turns_page.data);
        }
    }

    {
        let pending_thread_unloads = pending_thread_unloads.lock().await;
        if pending_thread_unloads.contains(&conversation_id) {
            drop(pending_thread_unloads);
            outgoing
                .send_error(
                    request_id,
                    invalid_request(format!(
                        "thread {conversation_id} is closing; retry thread/resume after the thread is closed"
                    )),
                )
                .await;
            return;
        }
        if !thread_state_manager
            .try_add_connection_to_thread(conversation_id, connection_id)
            .await
        {
            tracing::debug!(
                thread_id = %conversation_id,
                connection_id = ?connection_id,
                "skipping running thread resume for closed connection"
            );
            return;
        }
    }

    let config_snapshot = pending.config_snapshot;
    let cwd = config_snapshot.cwd().clone();
    let ThreadConfigSnapshot {
        model,
        model_provider_id,
        service_tier,
        approval_policy,
        approvals_reviewer,
        permission_profile,
        active_permission_profile,
        workspace_roots,
        reasoning_effort,
        originator,
        ..
    } = config_snapshot;
    let instruction_sources = pending.instruction_sources;
    let sandbox = thread_response_sandbox_policy(&permission_profile, cwd.as_path());
    let active_permission_profile =
        thread_response_active_permission_profile(active_permission_profile);
    let session_id = conversation.session_configured().session_id.to_string();
    thread.session_id = session_id;

    let response = ThreadResumeResponse {
        thread,
        model,
        model_provider: model_provider_id,
        service_tier,
        cwd,
        runtime_workspace_roots: workspace_roots,
        instruction_sources,
        approval_policy: approval_policy.into(),
        approvals_reviewer: approvals_reviewer.into(),
        sandbox,
        active_permission_profile,
        reasoning_effort,
        multi_agent_mode: MultiAgentMode::ExplicitRequestOnly,
        initial_turns_page,
    };
    outgoing
        .send_response_with_thread_originator(request_id, response, originator)
        .await;
    drop(thread_list_state_permit);
    // Match cold resume: metadata-only resume should attach the listener without
    // paying the cost of turn reconstruction for historical usage replay.
    if let Some(token_usage_thread) = token_usage_thread {
        let token_usage_turn_id = latest_token_usage_turn_id_from_rollout_items(
            &pending.history_items,
            token_usage_thread.turns.as_slice(),
        );
        // Rejoining a loaded thread has the same UI contract as a cold resume, but
        // uses the live conversation state instead of reconstructing a new session.
        send_thread_token_usage_update_to_connection(
            outgoing,
            connection_id,
            conversation_id,
            &token_usage_thread,
            conversation.as_ref(),
            token_usage_turn_id,
        )
        .await;
    }
    if pending.emit_thread_goal_update {
        if let Some(state_db) = pending.thread_goal_state_db {
            send_thread_goal_snapshot_notification(outgoing, conversation_id, &state_db).await;
        } else {
            tracing::warn!(
                thread_id = %conversation_id,
                "state db unavailable when reading thread goal for running thread resume"
            );
        }
    }
    outgoing
        .replay_requests_to_connection_for_thread(connection_id, conversation_id)
        .await;
    // App-server owns resume response and snapshot ordering, so wait until
    // replay completes before letting extensions react to the idle thread.
    if pending.emit_thread_goal_update {
        conversation.emit_thread_idle_lifecycle_if_idle().await;
    }
}

pub(super) async fn send_thread_goal_snapshot_notification(
    outgoing: &Arc<OutgoingMessageSender>,
    thread_id: ThreadId,
    state_db: &StateDbHandle,
) {
    match state_db.thread_goals().get_thread_goal(thread_id).await {
        Ok(Some(goal)) => {
            outgoing
                .send_server_notification(ServerNotification::ThreadGoalUpdated(
                    ThreadGoalUpdatedNotification {
                        thread_id: thread_id.to_string(),
                        turn_id: None,
                        goal: api_thread_goal_from_state(goal),
                    },
                ))
                .await;
        }
        Ok(None) => {
            outgoing
                .send_server_notification(ServerNotification::ThreadGoalCleared(
                    ThreadGoalClearedNotification {
                        thread_id: thread_id.to_string(),
                    },
                ))
                .await;
        }
        Err(err) => {
            tracing::warn!(
                thread_id = %thread_id,
                "failed to read thread goal for resume snapshot: {err}"
            );
        }
    }
}

pub(crate) fn populate_thread_turns_from_history(
    thread: &mut Thread,
    items: &[RolloutItem],
    active_turn: Option<&Turn>,
) {
    let mut turns = build_legacy_api_turns_from_rollout_items(items);
    if let Some(active_turn) = active_turn {
        merge_turn_history_with_active_turn(&mut turns, active_turn.clone());
    }
    thread.turns = turns;
}

pub(super) async fn resolve_pending_server_request(
    conversation_id: ThreadId,
    thread_state_manager: &ThreadStateManager,
    outgoing: &Arc<OutgoingMessageSender>,
    request_id: RequestId,
) {
    let thread_id = conversation_id.to_string();
    let subscribed_connection_ids = thread_state_manager
        .subscribed_connection_ids(conversation_id)
        .await;
    let outgoing = ThreadScopedOutgoingMessageSender::new(
        outgoing.clone(),
        subscribed_connection_ids,
        conversation_id,
    );
    outgoing
        .send_server_notification(ServerNotification::ServerRequestResolved(
            ServerRequestResolvedNotification {
                thread_id,
                request_id,
            },
        ))
        .await;
}

pub(super) fn merge_turn_history_with_active_turn(turns: &mut Vec<Turn>, active_turn: Turn) {
    turns.retain(|turn| turn.id != active_turn.id);
    turns.push(active_turn);
    crate::spine_ui::hide_internal_history_items_when_disabled(turns);
}

pub(super) fn set_thread_status_and_interrupt_stale_turns(
    thread: &mut Thread,
    loaded_status: ThreadStatus,
    has_live_in_progress_turn: bool,
) {
    let status = resolve_thread_status(loaded_status, has_live_in_progress_turn);
    if !matches!(status, ThreadStatus::Active { .. }) {
        for turn in &mut thread.turns {
            if matches!(turn.status, TurnStatus::InProgress) {
                turn.status = TurnStatus::Interrupted;
            }
        }
    }
    thread.status = status;
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;

    use codex_app_server_protocol::ServerNotification;
    use codex_app_server_protocol::ThreadHistoryBuilder;
    use codex_app_server_protocol::ThreadItem;
    use codex_app_server_protocol::ThreadStatus;
    use codex_core::NewThread;
    use codex_login::CodexAuth;
    use codex_protocol::AgentPath;
    use codex_protocol::ThreadId;
    use codex_protocol::items::TurnItem as CoreTurnItem;
    use codex_protocol::models::BaseInstructions;
    use codex_protocol::models::ResponseItem;
    use codex_protocol::protocol::AgentStatus;
    use codex_protocol::protocol::EventMsg;
    use codex_protocol::protocol::RawResponseItemEvent;
    use codex_protocol::protocol::RolloutItem;
    use codex_protocol::protocol::SessionSource;
    use codex_protocol::protocol::SpineSpawnProgressEvent;
    use codex_protocol::protocol::SpineSpawnTaskProgress;
    use codex_protocol::protocol::SpineTreeNodeKind;
    use codex_protocol::protocol::SpineTreeNodeSnapshot;
    use codex_protocol::protocol::SpineTreeNodeStatus;
    use codex_protocol::protocol::SpineTreeUpdateEvent;
    use codex_protocol::protocol::ThreadHistoryMode;
    use codex_protocol::protocol::ThreadMemoryMode;
    use codex_protocol::protocol::TurnCompleteEvent;
    use codex_protocol::protocol::TurnStartedEvent;
    use codex_thread_store::CreateThreadParams;
    use codex_thread_store::LiveThread;
    use codex_thread_store::LocalThreadStore;
    use codex_thread_store::LocalThreadStoreConfig;
    use codex_thread_store::ReadThreadParams;
    use codex_thread_store::ThreadPersistenceMetadata;
    use codex_thread_store::ThreadStore;
    use core_test_support::load_default_config_for_test;
    use tempfile::TempDir;
    use tokio::sync::Mutex;
    use tokio::sync::mpsc;
    use tokio::sync::watch;

    use super::THREAD_UNLOADING_RETRY_DELAY;
    use super::UnloadingState;
    use super::handle_thread_listener_command;
    use super::persist_spine_ui_completed_update_to_unloaded_thread;
    use super::persist_spine_ui_completed_updates;
    use super::restore_persisted_spine_ui_ancestors;
    use crate::outgoing_message::ConnectionId;
    use crate::outgoing_message::OutgoingEnvelope;
    use crate::outgoing_message::OutgoingMessage;
    use crate::outgoing_message::OutgoingMessageSender;
    use crate::spine_ui::SpineUiState;
    use crate::thread_state::ConnectionCapabilities;
    use crate::thread_state::ThreadListenerCommand;
    use crate::thread_state::ThreadStateManager;
    use crate::thread_status::ThreadWatchManager;

    #[test]
    fn unloading_permit_contention_retries_without_resetting_the_idle_delay() {
        let (_connections_tx, connections_rx) = watch::channel(false);
        let (_status_tx, status_rx) = watch::channel(ThreadStatus::Idle);
        let now = Instant::now();
        let mut state = UnloadingState {
            delay: Duration::ZERO,
            has_subscribers_rx: connections_rx,
            has_subscribers: (false, now),
            thread_status_rx: status_rx,
            is_active: (false, now),
            retry_not_before: None,
        };

        assert!(state.should_unload_now());
        state.retry_unload_soon();
        assert!(!state.should_unload_now());
        let retry_target = state.unloading_target().expect("retry target");
        assert!(retry_target > Instant::now());
        assert!(retry_target <= Instant::now() + THREAD_UNLOADING_RETRY_DELAY);
    }

    #[tokio::test]
    async fn late_terminal_listener_refreshes_and_persists_the_timed_out_card() {
        let home = TempDir::new().expect("temporary Codex home");
        let config = load_default_config_for_test(&home).await;
        let thread_store = codex_core::thread_store_from_config(&config, None);
        let thread_manager = Arc::new(
            codex_core::test_support::thread_manager_with_models_provider_and_home(
                CodexAuth::create_dummy_chatgpt_auth_for_testing(),
                config.model_provider.clone(),
                config.codex_home.to_path_buf(),
                Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
            ),
        );
        let NewThread {
            thread_id: parent_thread_id,
            thread: parent_thread,
            ..
        } = thread_manager
            .start_thread(config)
            .await
            .expect("start parent thread");
        let child_thread_id = ThreadId::new();
        let manager = ThreadStateManager::new();
        let parent_state = manager.thread_state(parent_thread_id).await;
        let child_state = manager.thread_state(child_thread_id).await;
        let (listener_tx, mut listener_rx) = mpsc::unbounded_channel();
        manager.register_listener_command_tx(parent_thread_id, listener_tx);

        let progress = SpineSpawnProgressEvent {
            call_id: "spawn-child".to_string(),
            tasks: vec![SpineSpawnTaskProgress {
                ordinal: 0,
                summary: "Child".to_string(),
                thread_id: child_thread_id,
                agent_path: AgentPath::try_from("/root/agent_0").ok(),
                status: AgentStatus::Running,
            }],
        };
        {
            let mut state = parent_state.lock().await;
            state.track_current_turn_event("turn-1", &turn_started("turn-1"));
            state.track_current_turn_event("turn-1", &spine_function_call("spawn"));
            state.record_spine_ui_snapshot(
                spine_ui_state(1)
                    .latest_snapshot()
                    .expect("parent snapshot")
                    .clone(),
            );
            state.record_spine_ui_spawn_progress(progress.clone());
        }
        {
            let mut state = child_state.lock().await;
            state.track_current_turn_event("child-turn", &turn_started("child-turn"));
            state.track_current_turn_event("child-turn", &spine_function_call("open"));
            state.record_spine_ui_snapshot(
                spine_ui_state(2)
                    .latest_snapshot()
                    .expect("child snapshot")
                    .clone(),
            );
        }
        manager
            .register_spine_ui_spawn_progress(parent_thread_id, "turn-1", &progress)
            .await;
        let (states, timed_out) = manager
            .wait_for_spine_ui_terminal_children(parent_thread_id, "turn-1", Duration::ZERO)
            .await;
        let [(forwarded_child_id, generation, child_snapshot)] = states.as_slice() else {
            panic!("expected one timed-out child state");
        };
        assert_eq!(*forwarded_child_id, child_thread_id);
        assert_eq!(timed_out, vec![(child_thread_id, *generation)]);

        let timed_out_parent = {
            let mut state = parent_state.lock().await;
            assert!(state.record_spine_ui_agent_state(
                child_thread_id,
                *generation,
                child_snapshot.clone().expect("latest child state"),
            ));
            assert!(state.mark_spine_ui_agent_sync_timeout(child_thread_id, *generation));
            state.track_current_turn_event("turn-1", &turn_complete("turn-1"));
            state.take_turn_summary().spine_ui
        };
        parent_thread
            .persist_client_item_completed(
                crate::spine_ui::snapshot_completed_event(
                    parent_thread_id,
                    "turn-1",
                    &timed_out_parent,
                )
                .expect("timed-out completed event"),
            )
            .await;
        manager
            .clear_spine_ui_parent_routes(parent_thread_id, "turn-1")
            .await;
        parent_state
            .lock()
            .await
            .track_current_turn_event("turn-2", &turn_started("turn-2"));

        {
            let mut state = child_state.lock().await;
            state.track_current_turn_event("child-turn", &turn_complete("child-turn"));
            state.take_turn_summary();
        }
        manager
            .acknowledge_spine_ui_agent_terminal(child_thread_id, 0, "child-turn")
            .await;
        let command = loop {
            let command = listener_rx.recv().await.expect("late terminal command");
            if matches!(
                command,
                ThreadListenerCommand::ForwardSpineUiAgentState { terminal: true, .. }
            ) {
                break command;
            }
        };

        let connection_id = ConnectionId(7);
        manager
            .connection_initialized(connection_id, ConnectionCapabilities::default())
            .await;
        assert!(
            manager
                .try_add_connection_to_thread(parent_thread_id, connection_id)
                .await
        );
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel(8);
        let outgoing = Arc::new(OutgoingMessageSender::new(
            outgoing_tx,
            codex_analytics::AnalyticsEventsClient::disabled(),
        ));
        let thread_list_state_permit = Arc::new(tokio::sync::Semaphore::new(1));
        handle_thread_listener_command(
            parent_thread_id,
            &parent_thread,
            home.path(),
            thread_manager.as_ref(),
            thread_store.as_ref(),
            &manager,
            &parent_state,
            &ThreadWatchManager::new(),
            &thread_list_state_permit,
            &outgoing,
            &Arc::new(Mutex::new(HashSet::new())),
            command,
        )
        .await;

        let envelope = outgoing_rx
            .recv()
            .await
            .expect("completed card notification");
        let OutgoingEnvelope::ToConnection {
            connection_id: notified_connection_id,
            message:
                OutgoingMessage::AppServerNotification(ServerNotification::ItemCompleted(notification)),
            ..
        } = envelope
        else {
            panic!("expected targeted item/completed notification: {envelope:?}");
        };
        assert_eq!(notified_connection_id, connection_id);
        assert_eq!(notification.turn_id, "turn-1");
        assert_eq!(notification.item.id(), "spine-ui-turn-1");
        assert_card_has_no_sync_timeout(&notification.item);

        parent_thread
            .flush_rollout()
            .await
            .expect("flush parent rollout");
        let rollout_path = parent_thread.rollout_path().expect("parent rollout path");
        let (items, _, parse_errors) =
            codex_rollout::RolloutRecorder::load_rollout_items(&rollout_path)
                .await
                .expect("read parent rollout");
        assert_eq!(parse_errors, 0);
        let completed_cards = items
            .iter()
            .filter_map(|item| {
                let RolloutItem::EventMsg(EventMsg::ItemCompleted(event)) = item else {
                    return None;
                };
                let CoreTurnItem::McpToolCall(call) = &event.item else {
                    return None;
                };
                (call.id == "spine-ui-turn-1").then_some(call)
            })
            .collect::<Vec<_>>();
        assert_eq!(completed_cards.len(), 2);
        let latest = completed_cards.last().expect("latest completed card");
        assert_eq!(latest.id, "spine-ui-turn-1");
        assert!(
            latest
                .result
                .as_ref()
                .and_then(|result| result.structured_content.as_ref())
                .is_some_and(|content| {
                    content["spawnCalls"][0]["tasks"][0]["status"] == "running"
                })
        );
    }

    #[tokio::test]
    async fn completed_update_only_direct_appends_after_manager_removal() {
        let home = TempDir::new().expect("temporary Codex home");
        let config = load_default_config_for_test(&home).await;
        let thread_store = codex_core::thread_store_from_config(&config, None);
        let thread_manager = Arc::new(
            codex_core::test_support::thread_manager_with_models_provider_and_home(
                CodexAuth::create_dummy_chatgpt_auth_for_testing(),
                config.model_provider.clone(),
                config.codex_home.to_path_buf(),
                Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
            ),
        );
        let NewThread {
            thread_id, thread, ..
        } = thread_manager
            .start_thread(config)
            .await
            .expect("start parent thread");
        thread.ensure_rollout_materialized().await;
        let rollout_path = thread.rollout_path().expect("parent rollout path");
        let update = crate::thread_state::SpineUiCompletedUpdate {
            thread_id,
            turn_id: "turn-1".to_string(),
            state: spine_ui_state(7),
        };
        let manager = ThreadStateManager::new();
        let (outgoing_tx, _outgoing_rx) = mpsc::channel(8);
        let outgoing = Arc::new(OutgoingMessageSender::new(
            outgoing_tx,
            codex_analytics::AnalyticsEventsClient::disabled(),
        ));
        let thread_list_state_permit = Arc::new(tokio::sync::Semaphore::new(1));
        let transition_permit = thread_list_state_permit
            .clone()
            .acquire_owned()
            .await
            .expect("thread list state permit");
        let persistence = {
            let thread_manager = thread_manager.clone();
            let thread_store = thread_store.clone();
            let outgoing = outgoing.clone();
            let thread_list_state_permit = thread_list_state_permit.clone();
            tokio::spawn(async move {
                persist_spine_ui_completed_updates(
                    vec![update],
                    thread_manager.as_ref(),
                    thread_store.as_ref(),
                    &manager,
                    &outgoing,
                    &thread_list_state_permit,
                )
                .await;
            })
        };
        tokio::task::yield_now().await;
        assert!(!persistence.is_finished());

        thread
            .shutdown_and_wait()
            .await
            .expect("shutdown live rollout writer");
        assert!(thread_manager.get_thread(thread_id).await.is_ok());
        drop(transition_permit);
        persistence
            .await
            .expect("completed update persistence task");

        let (items, _, parse_errors) =
            codex_rollout::RolloutRecorder::load_rollout_items(&rollout_path)
                .await
                .expect("read parent rollout");
        assert_eq!(parse_errors, 0);
        let persisted = items.iter().rev().find_map(|item| {
            let RolloutItem::EventMsg(EventMsg::ItemCompleted(event)) = item else {
                return None;
            };
            let CoreTurnItem::McpToolCall(call) = &event.item else {
                return None;
            };
            (call.id == "spine-ui-turn-1").then_some(call)
        });
        assert!(persisted.is_none());

        thread_manager.remove_thread(&thread_id).await;
        persist_spine_ui_completed_updates(
            vec![crate::thread_state::SpineUiCompletedUpdate {
                thread_id,
                turn_id: "turn-1".to_string(),
                state: spine_ui_state(8),
            }],
            thread_manager.as_ref(),
            thread_store.as_ref(),
            &ThreadStateManager::new(),
            &outgoing,
            &thread_list_state_permit,
        )
        .await;
        let (items, _, parse_errors) =
            codex_rollout::RolloutRecorder::load_rollout_items(&rollout_path)
                .await
                .expect("read parent rollout after manager removal");
        assert_eq!(parse_errors, 0);
        let content = items
            .iter()
            .rev()
            .find_map(|item| {
                let RolloutItem::EventMsg(EventMsg::ItemCompleted(event)) = item else {
                    return None;
                };
                let CoreTurnItem::McpToolCall(call) = &event.item else {
                    return None;
                };
                if call.id != "spine-ui-turn-1" {
                    return None;
                }
                call.result
                    .as_ref()
                    .and_then(|result| result.structured_content.as_ref())
            })
            .expect("cold-appended Spine UI content");
        assert_eq!(content["snapshot"]["snapshotSeq"], serde_json::json!(8));
    }

    #[tokio::test]
    async fn cold_rollback_updates_unloaded_parent_and_ancestor_rollouts() {
        let home = TempDir::new().expect("temporary Codex home");
        let config = LocalThreadStoreConfig {
            codex_home: home.path().to_path_buf(),
            sqlite_home: home.path().to_path_buf(),
            default_model_provider_id: "test-provider".to_string(),
        };
        let state_db = codex_state::StateRuntime::init(
            config.sqlite_home.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("initialize state DB");
        let initial_store = Arc::new(LocalThreadStore::new(
            config.clone(),
            Some(state_db.clone()),
        ));
        let root_thread_id = ThreadId::new();
        let parent_thread_id = ThreadId::new();
        let child_thread_id = ThreadId::new();

        let mut child_ui = spine_ui_state(3);
        child_ui.set_revision(3);
        let parent_ui = spine_ui_state_with_agent(2, child_thread_id, 7, child_ui);
        let root_ui = spine_ui_state_with_agent(1, parent_thread_id, 9, parent_ui.clone());
        persist_completed_thread(
            initial_store.clone(),
            root_thread_id,
            None,
            "root-turn",
            &root_ui,
        )
        .await;
        persist_completed_thread(
            initial_store,
            parent_thread_id,
            Some(root_thread_id),
            "parent-turn",
            &parent_ui,
        )
        .await;

        let restarted_store = LocalThreadStore::new(config.clone(), Some(state_db.clone()));
        let restarted_manager = ThreadStateManager::new();
        restore_persisted_spine_ui_ancestors(
            child_thread_id,
            Some(parent_thread_id),
            &restarted_store,
            &restarted_manager,
        )
        .await;
        let updates = restarted_manager
            .clear_all_spine_ui_routes_for_thread(child_thread_id)
            .await;
        assert_eq!(updates.len(), 2);
        for update in updates {
            let event = crate::spine_ui::snapshot_completed_event(
                update.thread_id,
                &update.turn_id,
                &update.state,
            )
            .expect("updated completed event");
            persist_spine_ui_completed_update_to_unloaded_thread(&update, event, &restarted_store)
                .await;
        }

        let verified_store = LocalThreadStore::new(config, Some(state_db));
        assert_persisted_card_has_no_agent(
            &verified_store,
            parent_thread_id,
            "parent-turn",
            child_thread_id,
        )
        .await;
        assert_persisted_card_has_no_agent(
            &verified_store,
            root_thread_id,
            "root-turn",
            parent_thread_id,
        )
        .await;
    }

    async fn persist_completed_thread(
        store: Arc<LocalThreadStore>,
        thread_id: ThreadId,
        parent_thread_id: Option<ThreadId>,
        turn_id: &str,
        state: &SpineUiState,
    ) {
        let live_thread = LiveThread::create(
            store,
            CreateThreadParams {
                session_id: thread_id.into(),
                thread_id,
                extra_config: None,
                forked_from_id: None,
                parent_thread_id,
                source: SessionSource::Exec,
                thread_source: None,
                originator: "test-originator".to_string(),
                base_instructions: BaseInstructions::default(),
                dynamic_tools: Vec::new(),
                selected_capability_roots: Vec::new(),
                multi_agent_version: None,
                history_mode: ThreadHistoryMode::Legacy,
                initial_window_id: format!("window-{thread_id}"),
                metadata: ThreadPersistenceMetadata {
                    cwd: Some(std::env::current_dir().expect("current directory")),
                    model_provider: "test-provider".to_string(),
                    memory_mode: ThreadMemoryMode::Enabled,
                },
            },
        )
        .await
        .expect("create persisted thread");
        let completed = crate::spine_ui::snapshot_completed_event(thread_id, turn_id, state)
            .expect("completed Spine UI event");
        live_thread
            .append_items(&[
                RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: turn_id.to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                })),
                RolloutItem::EventMsg(EventMsg::ItemCompleted(completed)),
                RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
                    turn_id: turn_id.to_string(),
                    last_agent_message: None,
                    completed_at: None,
                    duration_ms: None,
                    time_to_first_token_ms: None,
                })),
            ])
            .await
            .expect("append completed turn");
        live_thread
            .shutdown()
            .await
            .expect("shutdown persisted thread");
    }

    async fn assert_persisted_card_has_no_agent(
        store: &dyn ThreadStore,
        thread_id: ThreadId,
        turn_id: &str,
        child_thread_id: ThreadId,
    ) {
        let stored = store
            .read_thread(ReadThreadParams {
                thread_id,
                include_archived: true,
                include_history: true,
            })
            .await
            .expect("read persisted thread");
        let history = stored.history.expect("persisted history");
        assert!(
            crate::spine_ui::persisted_completed_states_for_child(&history.items, child_thread_id,)
                .is_empty()
        );
        let mut builder = ThreadHistoryBuilder::new();
        for item in &history.items {
            builder.handle_rollout_item(item);
        }
        let item_id = format!("spine-ui-{turn_id}");
        let turns = builder.finish();
        let item = turns
            .iter()
            .flat_map(|turn| &turn.items)
            .find(|item| item.id() == item_id)
            .expect("rebuilt Spine UI card");
        let ThreadItem::McpToolCall {
            result: Some(result),
            ..
        } = item
        else {
            panic!("expected completed Spine UI MCP card");
        };
        let content = result
            .structured_content
            .as_ref()
            .expect("Spine UI structured content");
        assert!(
            content["agentGenerations"]
                .as_array()
                .expect("agent generations")
                .iter()
                .all(|entry| entry["threadId"] != child_thread_id.to_string())
        );
    }

    fn turn_started(turn_id: &str) -> EventMsg {
        EventMsg::TurnStarted(TurnStartedEvent {
            turn_id: turn_id.to_string(),
            trace_id: None,
            started_at: None,
            model_context_window: None,
            collaboration_mode_kind: Default::default(),
        })
    }

    fn turn_complete(turn_id: &str) -> EventMsg {
        EventMsg::TurnComplete(TurnCompleteEvent {
            turn_id: turn_id.to_string(),
            last_agent_message: None,
            completed_at: None,
            duration_ms: None,
            time_to_first_token_ms: None,
        })
    }

    fn spine_function_call(name: &str) -> EventMsg {
        EventMsg::RawResponseItem(RawResponseItemEvent {
            item: ResponseItem::FunctionCall {
                id: None,
                name: name.to_string(),
                namespace: Some("spine".to_string()),
                arguments: "{}".to_string(),
                call_id: format!("call-{name}"),
                internal_chat_message_metadata_passthrough: None,
            },
        })
    }

    fn assert_card_has_no_sync_timeout(item: &ThreadItem) {
        let ThreadItem::McpToolCall {
            result: Some(result),
            ..
        } = item
        else {
            panic!("expected completed Spine UI MCP card");
        };
        assert_eq!(
            result
                .structured_content
                .as_ref()
                .expect("Spine UI structured content")["spawnCalls"][0]["tasks"][0]["status"],
            serde_json::json!("running")
        );
    }

    fn spine_ui_state_with_agent(
        sequence: u64,
        child_thread_id: ThreadId,
        generation: u64,
        child_state: SpineUiState,
    ) -> SpineUiState {
        let mut state = spine_ui_state(sequence);
        state.record_spawn_progress(SpineSpawnProgressEvent {
            call_id: format!("spawn-{child_thread_id}"),
            tasks: vec![SpineSpawnTaskProgress {
                ordinal: 0,
                summary: "Child".to_string(),
                thread_id: child_thread_id,
                agent_path: AgentPath::try_from("/root/agent_0").ok(),
                status: AgentStatus::Completed(None),
            }],
        });
        assert!(state.record_agent_state(child_thread_id, generation, child_state));
        state
    }

    fn spine_ui_state(sequence: u64) -> SpineUiState {
        let mut state = SpineUiState::default();
        state.record_snapshot(SpineTreeUpdateEvent {
            snapshot_seq: sequence,
            active_node_id: "1.1".to_string(),
            settled_spawn_call_ids: Vec::new(),
            nodes: vec![
                SpineTreeNodeSnapshot {
                    node_id: "1".to_string(),
                    parent_id: None,
                    kind: SpineTreeNodeKind::RootEpoch,
                    status: SpineTreeNodeStatus::Opened,
                    summary: None,
                    memory_summary: None,
                    spawn_outcome: None,
                    start: 0,
                    end: None,
                    context_pressure: None,
                },
                SpineTreeNodeSnapshot {
                    node_id: "1.1".to_string(),
                    parent_id: Some("1".to_string()),
                    kind: SpineTreeNodeKind::Task,
                    status: SpineTreeNodeStatus::Closed,
                    summary: Some("Work".to_string()),
                    memory_summary: None,
                    spawn_outcome: None,
                    start: 1,
                    end: Some(2),
                    context_pressure: None,
                },
            ],
        });
        state
    }
}
