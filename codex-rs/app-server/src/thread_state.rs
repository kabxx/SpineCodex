use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::ConnectionRequestId;
use crate::spine_ui::SpineUiState;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadGoal;
use codex_app_server_protocol::ThreadHistoryBuilder;
use codex_app_server_protocol::ThreadSettings;
use codex_app_server_protocol::Turn;
use codex_app_server_protocol::TurnError;
use codex_app_server_protocol::TurnStatus;
use codex_core::CodexThread;
use codex_core::ThreadConfigSnapshot;
use codex_file_watcher::WatchRegistration;
use codex_protocol::ThreadId;
#[cfg(test)]
use codex_protocol::config_types::MultiAgentMode;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::SpineSpawnProgressEvent;
use codex_protocol::protocol::SpineTreeUpdateEvent;
use codex_rollout::state_db::StateDbHandle;
use codex_utils_path_uri::LegacyAppPathString;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::Weak;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::time::Instant;
use tracing::error;

type PendingInterruptQueue = Vec<ConnectionRequestId>;
const SPINE_UI_TIMED_OUT_ROUTE_RETENTION: Duration = Duration::from_secs(30 * 60);

pub(crate) struct PendingThreadResumeRequest {
    pub(crate) request_id: ConnectionRequestId,
    pub(crate) history_items: Vec<RolloutItem>,
    pub(crate) config_snapshot: ThreadConfigSnapshot,
    pub(crate) instruction_sources: Vec<LegacyAppPathString>,
    pub(crate) thread_summary: codex_app_server_protocol::Thread,
    pub(crate) emit_thread_goal_update: bool,
    pub(crate) thread_goal_state_db: Option<StateDbHandle>,
    pub(crate) include_turns: bool,
    pub(crate) initial_turns_page:
        Option<codex_app_server_protocol::ThreadResumeInitialTurnsPageParams>,
    pub(crate) redact_resume_payloads: bool,
}

// ThreadListenerCommand is used to perform operations in the context of the thread listener, for serialization purposes.
pub(crate) enum ThreadListenerCommand {
    // SendThreadResumeResponse is used to resume an already running thread by sending the thread's history to the client and atomically subscribing for new updates.
    SendThreadResumeResponse(Box<PendingThreadResumeRequest>),
    // EmitThreadGoalUpdated is used to order goal updates with running-thread resume responses and goal clears.
    EmitThreadGoalUpdated {
        turn_id: Option<String>,
        goal: ThreadGoal,
    },
    // EmitThreadGoalCleared is used to order app-server goal clears with running-thread resume responses.
    EmitThreadGoalCleared,
    // EmitThreadGoalSnapshot is used to read and emit the latest goal state in the listener order.
    EmitThreadGoalSnapshot {
        state_db: StateDbHandle,
    },
    // ResolveServerRequest is used to notify the client that the request has been resolved.
    // It is executed in the thread listener's context to ensure that the resolved notification is ordered with regard to the request itself.
    ResolveServerRequest {
        request_id: RequestId,
        completion_tx: oneshot::Sender<()>,
    },
    ForwardSpineUiAgentState {
        child_thread_id: ThreadId,
        parent_turn_id: String,
        generation: u64,
        state: Option<SpineUiState>,
        terminal: bool,
    },
    EmitSpineUiInvalidation {
        turn_id: String,
        state: SpineUiState,
    },
}

/// Per-conversation accumulation of the latest states e.g. error message while a turn runs.
#[derive(Default, Clone)]
pub(crate) struct TurnSummary {
    pub(crate) started_at: Option<i64>,
    pub(crate) command_execution_started: HashSet<String>,
    pub(crate) last_error: Option<TurnError>,
    pub(crate) spine_ui: SpineUiState,
    spine_ui_turn_id: Option<String>,
}

impl TurnSummary {
    pub(crate) fn active_spine_ui(&self, turn_id: &str) -> Option<&SpineUiState> {
        (self.spine_ui_turn_id.as_deref() == Some(turn_id)).then_some(&self.spine_ui)
    }
}

#[derive(Default)]
pub(crate) struct ThreadState {
    pub(crate) pending_interrupts: PendingInterruptQueue,
    pub(crate) pending_rollbacks: Option<ConnectionRequestId>,
    pub(crate) turn_summary: TurnSummary,
    spine_ui_carry: SpineUiState,
    spine_ui_last_completed: SpineUiState,
    spine_ui_last_completed_turn_id: Option<String>,
    spine_ui_restored_completed: HashMap<String, SpineUiState>,
    next_spine_ui_revision: u64,
    pub(crate) last_terminal_turn_id: Option<String>,
    pub(crate) cancel_tx: Option<oneshot::Sender<()>>,
    pub(crate) experimental_raw_events: bool,
    pub(crate) listener_generation: u64,
    last_thread_settings: Option<ThreadSettings>,
    listener_command_tx: Option<mpsc::UnboundedSender<ThreadListenerCommand>>,
    current_turn_history: ThreadHistoryBuilder,
    listener_thread: Option<Weak<CodexThread>>,
    watch_registration: WatchRegistration,
}

#[derive(Clone)]
pub(crate) struct SpineUiCompletedUpdate {
    pub(crate) thread_id: ThreadId,
    pub(crate) turn_id: String,
    pub(crate) state: SpineUiState,
}

#[derive(Default)]
pub(crate) struct SpineUiAgentRefresh {
    pub(crate) active: Option<(String, SpineUiState)>,
    pub(crate) completed: Vec<(String, SpineUiState)>,
    pub(crate) forward: Option<SpineUiState>,
}

impl ThreadState {
    pub(crate) fn listener_matches(&self, conversation: &Arc<CodexThread>) -> bool {
        self.listener_thread
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some_and(|existing| Arc::ptr_eq(&existing, conversation))
    }

    pub(crate) fn set_listener(
        &mut self,
        cancel_tx: oneshot::Sender<()>,
        conversation: &Arc<CodexThread>,
        watch_registration: WatchRegistration,
        thread_settings_baseline: ThreadSettings,
    ) -> (mpsc::UnboundedReceiver<ThreadListenerCommand>, u64) {
        if let Some(previous) = self.cancel_tx.replace(cancel_tx) {
            let _ = previous.send(());
        }
        self.listener_generation = self.listener_generation.wrapping_add(1);
        self.last_thread_settings = Some(thread_settings_baseline);
        let (listener_command_tx, listener_command_rx) = mpsc::unbounded_channel();
        self.listener_command_tx = Some(listener_command_tx);
        self.listener_thread = Some(Arc::downgrade(conversation));
        self.watch_registration = watch_registration;
        (listener_command_rx, self.listener_generation)
    }

    pub(crate) fn clear_listener(&mut self) {
        if let Some(cancel_tx) = self.cancel_tx.take() {
            let _ = cancel_tx.send(());
        }
        self.listener_command_tx = None;
        self.current_turn_history.reset();
        self.listener_thread = None;
        self.watch_registration = WatchRegistration::default();
    }

    pub(crate) fn set_experimental_raw_events(&mut self, enabled: bool) {
        self.experimental_raw_events = enabled;
    }

    pub(crate) fn listener_command_tx(
        &self,
    ) -> Option<mpsc::UnboundedSender<ThreadListenerCommand>> {
        self.listener_command_tx.clone()
    }

    pub(crate) fn active_turn_snapshot(&self) -> Option<Turn> {
        let mut turn = self.current_turn_history.active_turn_snapshot()?;
        if let Some(spine_ui) = self.turn_summary.active_spine_ui(&turn.id)
            && let Some(item) = crate::spine_ui::snapshot_thread_item(&turn.id, spine_ui)
        {
            if let Some(existing) = turn
                .items
                .iter_mut()
                .find(|existing| existing.id() == item.id())
            {
                *existing = item;
            } else {
                turn.items.push(item);
            }
        }
        Some(turn)
    }

    pub(crate) fn has_active_turn(&self, turn_id: &str) -> bool {
        self.current_turn_history
            .active_turn_snapshot()
            .is_some_and(|turn| turn.id == turn_id && turn.status == TurnStatus::InProgress)
    }

    pub(crate) fn live_spine_ui(&self, turn_id: &str) -> Option<&SpineUiState> {
        self.has_active_turn(turn_id)
            .then(|| self.turn_summary.active_spine_ui(turn_id))
            .flatten()
    }

    fn current_spine_ui_for_forward(&self) -> Option<&SpineUiState> {
        self.current_turn_history
            .active_turn_snapshot()
            .and_then(|turn| self.turn_summary.active_spine_ui(&turn.id))
            .or_else(|| {
                self.spine_ui_last_completed
                    .latest_snapshot()
                    .is_some()
                    .then_some(&self.spine_ui_last_completed)
            })
    }

    pub(crate) fn active_spine_ui_snapshot(&self) -> Option<(String, SpineUiState)> {
        let turn = self.current_turn_history.active_turn_snapshot()?;
        self.turn_summary
            .active_spine_ui(&turn.id)
            .cloned()
            .map(|state| (turn.id, state))
    }

    fn completed_spine_ui_snapshot(&self) -> Option<(String, SpineUiState)> {
        self.spine_ui_last_completed.latest_snapshot()?;
        Some((
            self.spine_ui_last_completed_turn_id.clone()?,
            self.spine_ui_last_completed.clone(),
        ))
    }

    pub(crate) fn record_spine_ui_snapshot(&mut self, snapshot: SpineTreeUpdateEvent) -> bool {
        let changed = self.turn_summary.spine_ui.record_snapshot(snapshot);
        if changed {
            self.assign_turn_summary_spine_ui_revision();
        }
        changed
    }

    pub(crate) fn record_spine_ui_spawn_progress(
        &mut self,
        progress: SpineSpawnProgressEvent,
    ) -> bool {
        let changed = self.turn_summary.spine_ui.record_spawn_progress(progress);
        if changed {
            self.assign_turn_summary_spine_ui_revision();
        }
        changed
    }

    pub(crate) fn record_spine_ui_agent_state(
        &mut self,
        child_thread_id: ThreadId,
        generation: u64,
        child_state: SpineUiState,
    ) -> bool {
        let changed =
            self.turn_summary
                .spine_ui
                .record_agent_state(child_thread_id, generation, child_state);
        if changed {
            self.assign_turn_summary_spine_ui_revision();
        }
        changed
    }

    pub(crate) fn mark_spine_ui_agent_sync_timeout(
        &mut self,
        child_thread_id: ThreadId,
        generation: u64,
    ) -> bool {
        let changed = self
            .turn_summary
            .spine_ui
            .mark_agent_sync_timeout(child_thread_id, generation);
        if changed {
            self.assign_turn_summary_spine_ui_revision();
        }
        changed
    }

    pub(crate) fn record_completed_spine_ui_agent_terminal(
        &mut self,
        parent_turn_id: &str,
        child_thread_id: ThreadId,
        generation: u64,
        child_state: Option<SpineUiState>,
    ) -> SpineUiAgentRefresh {
        let has_completed_card = self.spine_ui_last_completed_turn_id.as_deref()
            == Some(parent_turn_id)
            || self
                .spine_ui_restored_completed
                .contains_key(parent_turn_id);
        if !has_completed_card {
            return SpineUiAgentRefresh::default();
        }

        let refresh = self.refresh_spine_ui_agent_state_inner(
            child_thread_id,
            generation,
            child_state.as_ref(),
            true,
        );
        let target_updated = refresh
            .completed
            .iter()
            .any(|(turn_id, _)| turn_id == parent_turn_id);
        if target_updated
            && self.spine_ui_last_completed_turn_id.as_deref() != Some(parent_turn_id)
            && self
                .spine_ui_restored_completed
                .get(parent_turn_id)
                .is_some_and(|state| !state.has_agent_sync_timeouts())
        {
            self.spine_ui_restored_completed.remove(parent_turn_id);
        }
        refresh
    }

    fn refresh_spine_ui_agent_state(
        &mut self,
        child_thread_id: ThreadId,
        generation: u64,
        child_state: SpineUiState,
    ) -> SpineUiAgentRefresh {
        self.refresh_spine_ui_agent_state_inner(
            child_thread_id,
            generation,
            Some(&child_state),
            false,
        )
    }

    fn refresh_spine_ui_agent_state_inner(
        &mut self,
        child_thread_id: ThreadId,
        generation: u64,
        child_state: Option<&SpineUiState>,
        clear_timeout: bool,
    ) -> SpineUiAgentRefresh {
        let update_state = |state: &mut SpineUiState| {
            let tracks_generation = if clear_timeout {
                state.tracks_agent_or_timeout_generation(child_thread_id, generation)
            } else {
                state.tracks_agent_generation(child_thread_id, generation)
            };
            if !tracks_generation {
                return false;
            }
            if clear_timeout && child_state.is_none() {
                return state.remove_agent_state(child_thread_id, generation);
            }
            let state_changed = child_state.is_some_and(|child_state| {
                state.record_agent_state(child_thread_id, generation, child_state.clone())
            });
            let timeout_cleared =
                clear_timeout && state.clear_agent_sync_timeout(child_thread_id, generation);
            state_changed || timeout_cleared
        };
        let turn_changed = update_state(&mut self.turn_summary.spine_ui);
        let carry_changed = update_state(&mut self.spine_ui_carry);
        let completed_changed = update_state(&mut self.spine_ui_last_completed);

        let restored_turn_ids = self
            .spine_ui_restored_completed
            .iter()
            .filter_map(|(turn_id, state)| {
                let tracks_generation = if clear_timeout {
                    state.tracks_agent_or_timeout_generation(child_thread_id, generation)
                } else {
                    state.tracks_agent_generation(child_thread_id, generation)
                };
                tracks_generation.then_some(turn_id.clone())
            })
            .collect::<Vec<_>>();
        let mut restored_changed = Vec::new();
        for turn_id in restored_turn_ids {
            let Some(mut state) = self.spine_ui_restored_completed.remove(&turn_id) else {
                continue;
            };
            if update_state(&mut state) {
                restored_changed.push((turn_id, state));
            } else {
                self.spine_ui_restored_completed.insert(turn_id, state);
            }
        }

        if !turn_changed && !carry_changed && !completed_changed && restored_changed.is_empty() {
            return SpineUiAgentRefresh::default();
        }

        let mut candidate = self
            .turn_summary
            .spine_ui
            .revision()
            .max(self.spine_ui_carry.revision())
            .max(self.spine_ui_last_completed.revision());
        for (_, state) in &restored_changed {
            candidate = candidate.max(state.revision());
        }
        let revision = self.allocate_spine_ui_revision(candidate);
        if turn_changed {
            self.turn_summary.spine_ui.set_revision(revision);
        }
        if carry_changed {
            self.spine_ui_carry.set_revision(revision);
        }
        if completed_changed {
            self.spine_ui_last_completed.set_revision(revision);
        }

        let active = turn_changed
            .then(|| self.active_spine_ui_snapshot())
            .flatten();
        let mut completed = completed_changed
            .then(|| self.completed_spine_ui_snapshot())
            .flatten()
            .into_iter()
            .collect::<Vec<_>>();
        for (turn_id, mut state) in restored_changed {
            state.set_revision(revision);
            completed.push((turn_id.clone(), state.clone()));
            if state.has_agent_sync_timeouts() {
                self.spine_ui_restored_completed.insert(turn_id, state);
            }
        }
        let forward = (turn_changed || carry_changed || completed_changed)
            .then(|| self.current_spine_ui_for_forward().cloned())
            .flatten();
        SpineUiAgentRefresh {
            active,
            completed,
            forward,
        }
    }

    pub(crate) fn invalidate_spine_ui_agent_state(
        &mut self,
        child_thread_id: ThreadId,
        generation: u64,
    ) -> bool {
        let turn_changed = self
            .turn_summary
            .spine_ui
            .remove_agent_state(child_thread_id, generation);
        let carry_changed = self
            .spine_ui_carry
            .remove_agent_state(child_thread_id, generation);
        let completed_changed = self
            .spine_ui_last_completed
            .remove_agent_state(child_thread_id, generation);
        if !turn_changed && !carry_changed && !completed_changed {
            return false;
        }

        let candidate = self
            .turn_summary
            .spine_ui
            .revision()
            .max(self.spine_ui_carry.revision())
            .max(self.spine_ui_last_completed.revision());
        let revision = self.allocate_spine_ui_revision(candidate);
        if turn_changed {
            self.turn_summary.spine_ui.set_revision(revision);
        }
        if carry_changed {
            self.spine_ui_carry.set_revision(revision);
        }
        if completed_changed {
            self.spine_ui_last_completed.set_revision(revision);
        }
        true
    }

    pub(crate) fn spine_ui_carry_agent_generations(&self) -> HashMap<ThreadId, u64> {
        self.spine_ui_carry.tracked_agent_generations()
    }

    fn assign_turn_summary_spine_ui_revision(&mut self) {
        let candidate = self.turn_summary.spine_ui.revision();
        let revision = self.allocate_spine_ui_revision(candidate);
        self.turn_summary.spine_ui.set_revision(revision);
    }

    fn allocate_spine_ui_revision(&mut self, candidate: u64) -> u64 {
        let revision = self.next_spine_ui_revision.saturating_add(1).max(candidate);
        self.next_spine_ui_revision = revision;
        revision
    }

    pub(crate) fn reset_spine_ui_after_rollback(&mut self) {
        self.turn_summary.spine_ui = SpineUiState::default();
        self.turn_summary.spine_ui_turn_id = None;
        self.spine_ui_carry = SpineUiState::default();
        self.spine_ui_last_completed = SpineUiState::default();
        self.spine_ui_last_completed_turn_id = None;
        self.spine_ui_restored_completed.clear();
        self.last_terminal_turn_id = None;
    }

    fn restore_spine_ui_completed_state(&mut self, turn_id: String, mut state: SpineUiState) {
        self.next_spine_ui_revision = self.next_spine_ui_revision.max(state.revision());
        state.set_revision(self.next_spine_ui_revision);
        if self.spine_ui_last_completed_turn_id.as_deref() == Some(turn_id.as_str())
            && self.spine_ui_last_completed.latest_snapshot().is_some()
        {
            return;
        }
        if self.spine_ui_last_completed.latest_snapshot().is_none()
            && self.turn_summary.spine_ui.latest_snapshot().is_none()
        {
            self.spine_ui_last_completed = state.clone();
            self.spine_ui_carry = state.carry_forward();
            self.spine_ui_last_completed_turn_id = Some(turn_id.clone());
            self.last_terminal_turn_id = Some(turn_id);
            return;
        }
        self.spine_ui_restored_completed.insert(turn_id, state);
    }

    fn invalidate_restored_spine_ui_agent_states(
        &mut self,
        child_thread_id: ThreadId,
        generation: u64,
    ) -> Vec<(String, SpineUiState)> {
        let turn_ids = self
            .spine_ui_restored_completed
            .iter()
            .filter_map(|(turn_id, state)| {
                state
                    .tracks_agent_or_timeout_generation(child_thread_id, generation)
                    .then_some(turn_id.clone())
            })
            .collect::<Vec<_>>();
        let mut updates = Vec::with_capacity(turn_ids.len());
        for turn_id in turn_ids {
            let Some(mut state) = self.spine_ui_restored_completed.remove(&turn_id) else {
                continue;
            };
            if state.remove_agent_state(child_thread_id, generation) {
                let revision = self.allocate_spine_ui_revision(state.revision());
                state.set_revision(revision);
                updates.push((turn_id.clone(), state.clone()));
            }
            if state.has_agent_sync_timeouts() {
                self.spine_ui_restored_completed.insert(turn_id, state);
            }
        }
        updates
    }

    pub(crate) fn take_turn_summary(&mut self) -> TurnSummary {
        let turn_summary = std::mem::take(&mut self.turn_summary);
        if let Some(turn_id) = turn_summary.spine_ui_turn_id.as_ref() {
            if self.spine_ui_last_completed_turn_id.as_deref() != Some(turn_id)
                && self.spine_ui_last_completed.has_agent_sync_timeouts()
                && let Some(previous_turn_id) = self.spine_ui_last_completed_turn_id.take()
            {
                self.spine_ui_restored_completed
                    .insert(previous_turn_id, self.spine_ui_last_completed.clone());
            }
            self.spine_ui_last_completed = turn_summary.spine_ui.clone();
            self.spine_ui_carry = turn_summary.spine_ui.carry_forward();
            self.spine_ui_last_completed_turn_id = Some(turn_id.clone());
        }
        turn_summary
    }

    pub(crate) fn track_current_turn_event(&mut self, event_turn_id: &str, event: &EventMsg) {
        if let EventMsg::TurnStarted(payload) = event {
            self.turn_summary.started_at = payload.started_at;
            self.last_terminal_turn_id = None;
        }
        self.current_turn_history.handle_event(event);
        if self.has_active_turn(event_turn_id)
            && let EventMsg::RawResponseItem(payload) = event
            && crate::spine_ui::is_tree_tool_call(&payload.item)
        {
            if self.turn_summary.spine_ui_turn_id.as_deref() != Some(event_turn_id) {
                self.turn_summary.spine_ui = self.spine_ui_carry.carry_forward();
            }
            self.turn_summary.spine_ui_turn_id = Some(event_turn_id.to_string());
        }
        if matches!(event, EventMsg::TurnAborted(_) | EventMsg::TurnComplete(_))
            && !self.current_turn_history.has_active_turn()
        {
            self.last_terminal_turn_id = Some(event_turn_id.to_string());
            self.current_turn_history.reset();
        }
    }

    pub(crate) fn note_thread_settings(&mut self, thread_settings: ThreadSettings) -> bool {
        let changed = self.last_thread_settings.as_ref() != Some(&thread_settings);
        self.last_thread_settings = Some(thread_settings);
        changed
    }
}

pub(crate) async fn resolve_server_request_on_thread_listener(
    thread_state: &Arc<Mutex<ThreadState>>,
    request_id: RequestId,
) {
    let (completion_tx, completion_rx) = oneshot::channel();
    let listener_command_tx = {
        let state = thread_state.lock().await;
        state.listener_command_tx()
    };
    let Some(listener_command_tx) = listener_command_tx else {
        error!("failed to remove pending client request: thread listener is not running");
        return;
    };

    if listener_command_tx
        .send(ThreadListenerCommand::ResolveServerRequest {
            request_id,
            completion_tx,
        })
        .is_err()
    {
        error!(
            "failed to remove pending client request: thread listener command channel is closed"
        );
        return;
    }

    if let Err(err) = completion_rx.await {
        error!("failed to remove pending client request: {err}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_app_server_protocol::ApprovalsReviewer;
    use codex_app_server_protocol::AskForApproval;
    use codex_app_server_protocol::SandboxPolicy;
    use codex_app_server_protocol::ThreadItem;
    use codex_protocol::AgentPath;
    use codex_protocol::config_types::CollaborationMode;
    use codex_protocol::config_types::ModeKind;
    use codex_protocol::config_types::Settings;
    use codex_protocol::models::ResponseItem;
    use codex_protocol::protocol::AgentStatus;
    use codex_protocol::protocol::RawResponseItemEvent;
    use codex_protocol::protocol::SpineSpawnTaskProgress;
    use codex_protocol::protocol::SpineTreeNodeKind;
    use codex_protocol::protocol::SpineTreeNodeSnapshot;
    use codex_protocol::protocol::SpineTreeNodeStatus;
    use codex_protocol::protocol::TurnCompleteEvent;
    use codex_protocol::protocol::TurnStartedEvent;
    use codex_utils_absolute_path::AbsolutePathBuf;
    use pretty_assertions::assert_eq;

    #[test]
    fn note_thread_settings_reports_only_effective_changes() {
        let mut state = ThreadState::default();
        let initial = thread_settings("mock-model");
        let updated = thread_settings("mock-model-2");

        let results = vec![
            state.note_thread_settings(initial.clone()),
            state.note_thread_settings(initial),
            state.note_thread_settings(updated.clone()),
            state.note_thread_settings(updated),
        ];

        assert_eq!(results, vec![true, false, true, false]);
    }

    #[test]
    fn active_turn_requires_a_real_matching_turn_lifecycle() {
        let mut state = ThreadState::default();
        assert!(!state.has_active_turn("turn-1"));

        state.track_current_turn_event(
            "turn-1",
            &EventMsg::TurnStarted(TurnStartedEvent {
                turn_id: "turn-1".to_string(),
                trace_id: None,
                started_at: None,
                model_context_window: None,
                collaboration_mode_kind: Default::default(),
            }),
        );
        assert!(state.has_active_turn("turn-1"));
        assert!(!state.has_active_turn("turn-2"));

        state.track_current_turn_event(
            "turn-1",
            &EventMsg::TurnComplete(TurnCompleteEvent {
                turn_id: "turn-1".to_string(),
                last_agent_message: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            }),
        );
        assert!(!state.has_active_turn("turn-1"));
    }

    #[test]
    fn spine_ui_requires_an_explicit_spine_call_in_the_active_turn() {
        let mut state = ThreadState::default();
        state.track_current_turn_event(
            "turn-1",
            &EventMsg::TurnStarted(TurnStartedEvent {
                turn_id: "turn-1".to_string(),
                trace_id: None,
                started_at: None,
                model_context_window: None,
                collaboration_mode_kind: Default::default(),
            }),
        );

        assert!(state.live_spine_ui("turn-1").is_none());
        state.track_current_turn_event("turn-1", &function_call("shell", None));
        assert!(state.live_spine_ui("turn-1").is_none());
        state.track_current_turn_event("turn-1", &function_call("trim", Some("spine")));
        assert!(state.live_spine_ui("turn-1").is_none());

        state.track_current_turn_event("turn-1", &function_call("open", Some("spine")));
        assert!(state.live_spine_ui("turn-1").is_some());
        assert!(state.live_spine_ui("turn-2").is_none());
        state
            .turn_summary
            .spine_ui
            .record_snapshot(spine_snapshot(1, "1.1"));
        assert!(
            state
                .active_turn_snapshot()
                .is_some_and(|turn| turn.items.iter().any(|item| matches!(
                    item,
                    ThreadItem::McpToolCall { id, .. } if id == "spine-ui-turn-1"
                )))
        );

        state.track_current_turn_event(
            "turn-1",
            &EventMsg::TurnComplete(TurnCompleteEvent {
                turn_id: "turn-1".to_string(),
                last_agent_message: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            }),
        );
        assert!(state.live_spine_ui("turn-1").is_none());
        assert!(state.turn_summary.active_spine_ui("turn-1").is_some());

        state.reset_spine_ui_after_rollback();
        assert!(state.turn_summary.active_spine_ui("turn-1").is_none());
        assert!(state.spine_ui_carry.latest_snapshot().is_none());
    }

    #[test]
    fn spine_ui_revision_remains_monotonic_across_rollback() {
        let mut state = ThreadState::default();
        state.track_current_turn_event(
            "turn-1",
            &EventMsg::TurnStarted(TurnStartedEvent {
                turn_id: "turn-1".to_string(),
                trace_id: None,
                started_at: None,
                model_context_window: None,
                collaboration_mode_kind: Default::default(),
            }),
        );
        state.track_current_turn_event("turn-1", &function_call("open", Some("spine")));
        state.record_spine_ui_snapshot(spine_snapshot(1, "1.1"));
        state.record_spine_ui_snapshot(spine_snapshot(2, "1.1"));
        let previous_revision = state
            .turn_summary
            .spine_ui
            .structured_content()
            .and_then(|content| content["uiRevision"].as_u64())
            .expect("first turn UI revision");

        state.reset_spine_ui_after_rollback();
        state.track_current_turn_event(
            "turn-2",
            &EventMsg::TurnStarted(TurnStartedEvent {
                turn_id: "turn-2".to_string(),
                trace_id: None,
                started_at: None,
                model_context_window: None,
                collaboration_mode_kind: Default::default(),
            }),
        );
        state.track_current_turn_event("turn-2", &function_call("open", Some("spine")));
        state.record_spine_ui_snapshot(spine_snapshot(1, "1.1"));

        let next_revision = state
            .turn_summary
            .spine_ui
            .structured_content()
            .and_then(|content| content["uiRevision"].as_u64())
            .expect("second turn UI revision");
        assert!(next_revision > previous_revision);
    }

    #[tokio::test]
    async fn spine_ui_concurrent_agent_states_queue_to_the_parent_listener() {
        let manager = ThreadStateManager::new();
        let parent_thread_id = ThreadId::new();
        let child_a = ThreadId::new();
        let child_b = ThreadId::new();
        let parent_state = manager.thread_state(parent_thread_id).await;
        let (listener_tx, mut listener_rx) = mpsc::unbounded_channel();
        manager.register_listener_command_tx(parent_thread_id, listener_tx);
        let progress = SpineSpawnProgressEvent {
            call_id: "spawn-1".to_string(),
            tasks: vec![
                agent_task(0, "Agent A", child_a),
                agent_task(1, "Agent B", child_b),
            ],
        };
        {
            let mut state = parent_state.lock().await;
            state.track_current_turn_event(
                "turn-1",
                &EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: "turn-1".to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                }),
            );
            state.track_current_turn_event("turn-1", &function_call("spawn", Some("spine")));
            state.record_spine_ui_snapshot(spine_snapshot(1, "1.1"));
            state.record_spine_ui_spawn_progress(progress.clone());
        }
        manager
            .register_spine_ui_spawn_progress(parent_thread_id, "turn-1", &progress)
            .await;

        for (child_id, sequence) in [(child_a, 4), (child_b, 7)] {
            let child_state = manager.thread_state(child_id).await;
            {
                let mut child_state = child_state.lock().await;
                child_state.track_current_turn_event(
                    "child-turn",
                    &EventMsg::TurnStarted(TurnStartedEvent {
                        turn_id: "child-turn".to_string(),
                        trace_id: None,
                        started_at: None,
                        model_context_window: None,
                        collaboration_mode_kind: Default::default(),
                    }),
                );
                child_state
                    .track_current_turn_event("child-turn", &function_call("open", Some("spine")));
                child_state.record_spine_ui_snapshot(spine_snapshot(sequence, "1.1"));
            }
            manager.queue_spine_ui_agent_state(child_id).await;
        }

        let mut queued = HashSet::new();
        for _ in 0..2 {
            let ThreadListenerCommand::ForwardSpineUiAgentState {
                child_thread_id,
                parent_turn_id,
                generation,
                state,
                terminal,
            } = listener_rx.recv().await.expect("queued agent state")
            else {
                panic!("expected forwarded Spine UI state");
            };
            assert_eq!(parent_turn_id, "turn-1");
            assert!(generation > 0);
            assert!(!terminal);
            assert!(
                state
                    .as_ref()
                    .and_then(SpineUiState::latest_snapshot)
                    .is_some()
            );
            queued.insert(child_thread_id);
        }
        assert_eq!(queued, HashSet::from([child_a, child_b]));

        manager
            .clear_spine_ui_parent_routes(parent_thread_id, "turn-1")
            .await;
        manager.queue_spine_ui_agent_state(child_a).await;
        assert!(listener_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn spine_ui_parent_refresh_reads_the_child_final_state() {
        let manager = ThreadStateManager::new();
        let parent_thread_id = ThreadId::new();
        let child_thread_id = ThreadId::new();
        let parent_state = manager.thread_state(parent_thread_id).await;
        let progress = SpineSpawnProgressEvent {
            call_id: "spawn-1".to_string(),
            tasks: vec![agent_task(0, "Agent", child_thread_id)],
        };
        {
            let mut state = parent_state.lock().await;
            state.track_current_turn_event(
                "turn-1",
                &EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: "turn-1".to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                }),
            );
            state.track_current_turn_event("turn-1", &function_call("spawn", Some("spine")));
            state
                .turn_summary
                .spine_ui
                .record_snapshot(spine_snapshot(1, "1.1"));
            state
                .turn_summary
                .spine_ui
                .record_spawn_progress(progress.clone());
        }
        manager
            .register_spine_ui_spawn_progress(parent_thread_id, "turn-1", &progress)
            .await;

        let child_state = manager.thread_state(child_thread_id).await;
        let manager_for_wait = manager.clone();
        let waiter = tokio::spawn(async move {
            manager_for_wait
                .wait_for_spine_ui_terminal_children(
                    parent_thread_id,
                    "turn-1",
                    Duration::from_secs(2),
                )
                .await
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        {
            let mut state = child_state.lock().await;
            state.track_current_turn_event(
                "child-turn",
                &EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: "child-turn".to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                }),
            );
            state.track_current_turn_event("child-turn", &function_call("open", Some("spine")));
            state
                .turn_summary
                .spine_ui
                .record_snapshot(spine_snapshot(9, "1.1"));
            state.track_current_turn_event(
                "child-turn",
                &EventMsg::TurnComplete(TurnCompleteEvent {
                    turn_id: "child-turn".to_string(),
                    last_agent_message: None,
                    completed_at: None,
                    duration_ms: None,
                    time_to_first_token_ms: None,
                }),
            );
            state.take_turn_summary();
        }
        manager
            .acknowledge_spine_ui_agent_terminal(child_thread_id, 0, "child-turn")
            .await;

        let (refreshed, timed_out) = waiter.await.expect("terminal barrier task");

        assert!(timed_out.is_empty());
        assert_eq!(refreshed.len(), 1);
        assert_eq!(refreshed[0].0, child_thread_id);
        assert_eq!(
            refreshed[0]
                .2
                .as_ref()
                .and_then(SpineUiState::latest_snapshot)
                .map(|snapshot| snapshot.snapshot_seq),
            Some(9)
        );
    }

    #[tokio::test]
    async fn spine_ui_terminal_barrier_is_bottom_up_for_nested_agents() {
        let manager = ThreadStateManager::new();
        let root_thread_id = ThreadId::new();
        let child_thread_id = ThreadId::new();
        let grandchild_thread_id = ThreadId::new();
        let root_state = manager.thread_state(root_thread_id).await;
        let child_state = manager.thread_state(child_thread_id).await;
        let grandchild_state = manager.thread_state(grandchild_thread_id).await;
        let root_progress = SpineSpawnProgressEvent {
            call_id: "root-spawn".to_string(),
            tasks: vec![agent_task(0, "Child", child_thread_id)],
        };
        let child_progress = SpineSpawnProgressEvent {
            call_id: "child-spawn".to_string(),
            tasks: vec![agent_task(0, "Grandchild", grandchild_thread_id)],
        };
        {
            let mut state = root_state.lock().await;
            state.track_current_turn_event(
                "root-turn",
                &EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: "root-turn".to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                }),
            );
            state.track_current_turn_event("root-turn", &function_call("spawn", Some("spine")));
            state.record_spine_ui_snapshot(spine_snapshot(1, "1.1"));
            state.record_spine_ui_spawn_progress(root_progress.clone());
        }
        {
            let mut state = child_state.lock().await;
            state.track_current_turn_event(
                "child-turn",
                &EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: "child-turn".to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                }),
            );
            state.track_current_turn_event("child-turn", &function_call("spawn", Some("spine")));
            state.record_spine_ui_snapshot(spine_snapshot(2, "1.1"));
            state.record_spine_ui_spawn_progress(child_progress.clone());
        }
        {
            let mut state = grandchild_state.lock().await;
            state.track_current_turn_event(
                "grandchild-turn",
                &EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: "grandchild-turn".to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                }),
            );
            state
                .track_current_turn_event("grandchild-turn", &function_call("open", Some("spine")));
            state.record_spine_ui_snapshot(spine_snapshot(9, "1.1"));
        }
        manager
            .register_spine_ui_spawn_progress(root_thread_id, "root-turn", &root_progress)
            .await;
        manager
            .register_spine_ui_spawn_progress(child_thread_id, "child-turn", &child_progress)
            .await;

        let root_manager = manager.clone();
        let root_waiter = tokio::spawn(async move {
            root_manager
                .wait_for_spine_ui_terminal_children(
                    root_thread_id,
                    "root-turn",
                    Duration::from_secs(2),
                )
                .await
        });
        let child_manager = manager.clone();
        let child_waiter = tokio::spawn(async move {
            child_manager
                .wait_for_spine_ui_terminal_children(
                    child_thread_id,
                    "child-turn",
                    Duration::from_secs(2),
                )
                .await
        });
        tokio::task::yield_now().await;
        assert!(!root_waiter.is_finished());
        assert!(!child_waiter.is_finished());

        {
            let mut state = grandchild_state.lock().await;
            state.track_current_turn_event(
                "grandchild-turn",
                &EventMsg::TurnComplete(TurnCompleteEvent {
                    turn_id: "grandchild-turn".to_string(),
                    last_agent_message: None,
                    completed_at: None,
                    duration_ms: None,
                    time_to_first_token_ms: None,
                }),
            );
            state.take_turn_summary();
        }
        manager
            .acknowledge_spine_ui_agent_terminal(grandchild_thread_id, 0, "grandchild-turn")
            .await;
        let (grandchild_states, child_timed_out) =
            child_waiter.await.expect("child terminal barrier task");
        assert!(child_timed_out.is_empty());
        {
            let mut state = child_state.lock().await;
            for (thread_id, generation, agent_state) in grandchild_states {
                state.record_spine_ui_agent_state(
                    thread_id,
                    generation,
                    agent_state.expect("grandchild terminal state"),
                );
            }
            state.track_current_turn_event(
                "child-turn",
                &EventMsg::TurnComplete(TurnCompleteEvent {
                    turn_id: "child-turn".to_string(),
                    last_agent_message: None,
                    completed_at: None,
                    duration_ms: None,
                    time_to_first_token_ms: None,
                }),
            );
            state.take_turn_summary();
        }
        manager
            .acknowledge_spine_ui_agent_terminal(child_thread_id, 0, "child-turn")
            .await;

        let (child_states, root_timed_out) = root_waiter.await.expect("root terminal barrier task");
        assert!(root_timed_out.is_empty());
        let child_ui = child_states[0]
            .2
            .as_ref()
            .expect("child terminal state")
            .structured_content()
            .expect("child structured content");
        assert_eq!(
            child_ui["agentSubtrees"][0]["threadId"],
            grandchild_thread_id.to_string()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn spine_ui_terminal_barrier_timeout_returns_latest_state() {
        let manager = ThreadStateManager::new();
        let parent_thread_id = ThreadId::new();
        let child_thread_id = ThreadId::new();
        let parent_state = manager.thread_state(parent_thread_id).await;
        let child_state = manager.thread_state(child_thread_id).await;
        let progress = SpineSpawnProgressEvent {
            call_id: "spawn-1".to_string(),
            tasks: vec![agent_task(0, "Agent", child_thread_id)],
        };
        {
            let mut state = parent_state.lock().await;
            state.track_current_turn_event(
                "turn-1",
                &EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: "turn-1".to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                }),
            );
            state.track_current_turn_event("turn-1", &function_call("spawn", Some("spine")));
            state.record_spine_ui_snapshot(spine_snapshot(1, "1.1"));
            state.record_spine_ui_spawn_progress(progress.clone());
        }
        {
            let mut state = child_state.lock().await;
            state.track_current_turn_event(
                "child-turn",
                &EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: "child-turn".to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                }),
            );
            state.track_current_turn_event("child-turn", &function_call("open", Some("spine")));
            state.record_spine_ui_snapshot(spine_snapshot(5, "1.1"));
        }
        manager
            .register_spine_ui_spawn_progress(parent_thread_id, "turn-1", &progress)
            .await;

        let (listener_tx, mut listener_rx) = mpsc::unbounded_channel();
        manager.register_listener_command_tx(parent_thread_id, listener_tx);
        let (states, timed_out) = manager
            .wait_for_spine_ui_terminal_children(parent_thread_id, "turn-1", Duration::ZERO)
            .await;

        let generation = states[0].1;
        assert_eq!(timed_out, vec![(child_thread_id, generation)]);
        assert!(
            manager
                .has_timed_out_spine_ui_children(parent_thread_id)
                .await
        );
        assert_eq!(
            states[0]
                .2
                .as_ref()
                .and_then(SpineUiState::latest_snapshot)
                .map(|snapshot| snapshot.snapshot_seq),
            Some(5)
        );

        {
            let mut state = parent_state.lock().await;
            assert!(state.mark_spine_ui_agent_sync_timeout(child_thread_id, generation));
            state.track_current_turn_event(
                "turn-1",
                &EventMsg::TurnComplete(TurnCompleteEvent {
                    turn_id: "turn-1".to_string(),
                    last_agent_message: None,
                    completed_at: None,
                    duration_ms: None,
                    time_to_first_token_ms: None,
                }),
            );
            state.take_turn_summary();
        }
        manager
            .clear_spine_ui_parent_routes(parent_thread_id, "turn-1")
            .await;
        assert!(
            manager
                .spine_ui_route_is_current(child_thread_id, parent_thread_id, "turn-1", generation,)
                .await
        );

        tokio::time::advance(SPINE_UI_TIMED_OUT_ROUTE_RETENTION - Duration::from_secs(1)).await;

        {
            let mut state = child_state.lock().await;
            state.track_current_turn_event(
                "child-turn",
                &EventMsg::TurnComplete(TurnCompleteEvent {
                    turn_id: "child-turn".to_string(),
                    last_agent_message: None,
                    completed_at: None,
                    duration_ms: None,
                    time_to_first_token_ms: None,
                }),
            );
            state.take_turn_summary();
        }
        manager
            .acknowledge_spine_ui_agent_terminal(child_thread_id, 0, "child-turn")
            .await;
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(
            manager
                .has_timed_out_spine_ui_children(parent_thread_id)
                .await
        );
        let ThreadListenerCommand::ForwardSpineUiAgentState {
            child_thread_id: forwarded_child,
            parent_turn_id,
            generation: forwarded_generation,
            state: terminal_state,
            terminal,
            ..
        } = listener_rx.recv().await.expect("late terminal forward")
        else {
            panic!("expected late terminal forward");
        };
        assert_eq!(forwarded_child, child_thread_id);
        assert_eq!(parent_turn_id, "turn-1");
        assert_eq!(forwarded_generation, generation);
        assert!(terminal);
        let refresh = parent_state
            .lock()
            .await
            .record_completed_spine_ui_agent_terminal(
                "turn-1",
                child_thread_id,
                generation,
                terminal_state,
            );
        let completed = refresh
            .completed
            .into_iter()
            .find_map(|(turn_id, state)| (turn_id == "turn-1").then_some(state))
            .expect("updated completed card");
        assert_eq!(
            completed.structured_content().expect("completed content")["spawnCalls"][0]["tasks"][0]
                ["status"],
            serde_json::json!("running")
        );
        manager
            .complete_spine_ui_late_terminal(
                child_thread_id,
                parent_thread_id,
                "turn-1",
                generation,
            )
            .await;
        manager
            .clear_spine_ui_parent_routes(parent_thread_id, "turn-1")
            .await;
        assert!(
            !manager
                .spine_ui_route_is_current(child_thread_id, parent_thread_id, "turn-1", generation,)
                .await
        );
        assert!(
            !manager
                .has_timed_out_spine_ui_children(parent_thread_id)
                .await
        );
    }

    #[tokio::test(start_paused = true)]
    async fn expired_timed_out_route_does_not_block_parent_cleanup() {
        let manager = ThreadStateManager::new();
        let parent_thread_id = ThreadId::new();
        let child_thread_id = ThreadId::new();
        let (terminal_tx, _) = watch::channel(SpineUiRouteTerminalState::TimedOut);
        manager.state.lock().await.spine_ui_parent_by_child.insert(
            child_thread_id,
            SpineUiParentRoute {
                parent_thread_id,
                parent_turn_id: "turn-1".to_string(),
                baseline_node_ids: HashSet::new(),
                generation: 1,
                timed_out_at: Some(Instant::now()),
                terminal_tx,
            },
        );

        assert!(
            manager
                .has_timed_out_spine_ui_children(parent_thread_id)
                .await
        );
        tokio::time::advance(SPINE_UI_TIMED_OUT_ROUTE_RETENTION + Duration::from_secs(1)).await;
        assert!(
            !manager
                .has_timed_out_spine_ui_children(parent_thread_id)
                .await
        );
        assert!(
            !manager
                .spine_ui_route_is_current(child_thread_id, parent_thread_id, "turn-1", 1)
                .await
        );
    }

    #[test]
    fn spine_ui_late_terminal_updates_completed_card_after_new_turn_starts() {
        let child_thread_id = ThreadId::new();
        let generation = 7;
        let mut state = completed_timed_out_parent_state(child_thread_id, generation);

        state.track_current_turn_event(
            "turn-2",
            &EventMsg::TurnStarted(TurnStartedEvent {
                turn_id: "turn-2".to_string(),
                trace_id: None,
                started_at: None,
                model_context_window: None,
                collaboration_mode_kind: Default::default(),
            }),
        );

        let mut final_child_ui = SpineUiState::default();
        final_child_ui.record_snapshot(spine_snapshot(3, "1.1"));
        final_child_ui.set_revision(2);
        let refresh = state.record_completed_spine_ui_agent_terminal(
            "turn-1",
            child_thread_id,
            generation,
            Some(final_child_ui),
        );
        let completed = refresh
            .completed
            .iter()
            .find_map(|(turn_id, state)| (turn_id == "turn-1").then_some(state))
            .expect("late terminal should still update turn-1");

        assert_eq!(
            completed.structured_content().expect("completed content")["agentSyncTimeoutGenerations"],
            serde_json::json!([])
        );
    }

    #[test]
    fn spine_ui_late_terminal_without_state_removes_older_generation() {
        let child_thread_id = ThreadId::new();
        let mut state = completed_timed_out_parent_state(child_thread_id, 7);
        assert!(
            state
                .spine_ui_carry
                .mark_agent_sync_timeout(child_thread_id, 8)
        );
        assert!(
            state
                .spine_ui_last_completed
                .mark_agent_sync_timeout(child_thread_id, 8)
        );

        let refresh =
            state.record_completed_spine_ui_agent_terminal("turn-1", child_thread_id, 8, None);
        let completed = refresh
            .completed
            .iter()
            .find_map(|(turn_id, state)| (turn_id == "turn-1").then_some(state))
            .expect("late terminal should clear the stale subtree");

        assert!(completed.tracked_agent_generations().is_empty());
        assert!(state.spine_ui_carry.tracked_agent_generations().is_empty());
    }

    #[test]
    fn spine_ui_late_terminal_updates_historical_card_after_new_spine_turn_completes() {
        let child_thread_id = ThreadId::new();
        let generation = 7;
        let mut state = completed_timed_out_parent_state(child_thread_id, generation);

        state.track_current_turn_event(
            "turn-2",
            &EventMsg::TurnStarted(TurnStartedEvent {
                turn_id: "turn-2".to_string(),
                trace_id: None,
                started_at: None,
                model_context_window: None,
                collaboration_mode_kind: Default::default(),
            }),
        );
        state.track_current_turn_event("turn-2", &function_call("next", Some("spine")));
        state.record_spine_ui_snapshot(spine_snapshot(2, "1.1"));
        state.track_current_turn_event(
            "turn-2",
            &EventMsg::TurnComplete(TurnCompleteEvent {
                turn_id: "turn-2".to_string(),
                last_agent_message: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            }),
        );
        state.take_turn_summary();

        let mut final_child_ui = SpineUiState::default();
        final_child_ui.record_snapshot(spine_snapshot(3, "1.1"));
        final_child_ui.set_revision(2);
        let refresh = state.record_completed_spine_ui_agent_terminal(
            "turn-1",
            child_thread_id,
            generation,
            Some(final_child_ui),
        );
        let historical = refresh
            .completed
            .iter()
            .find_map(|(turn_id, state)| (turn_id == "turn-1").then_some(state))
            .expect("late terminal should update the retained turn-1 card");
        assert_eq!(
            historical.structured_content().expect("historical content")["agentSyncTimeoutGenerations"],
            serde_json::json!([])
        );
        assert!(
            refresh
                .completed
                .iter()
                .any(|(turn_id, _)| turn_id == "turn-2"),
            "the newer carried card should be refreshed too"
        );
        assert!(
            !state.spine_ui_restored_completed.contains_key("turn-1"),
            "resolved runtime history should not accumulate"
        );
    }

    #[tokio::test]
    async fn spine_ui_terminal_barrier_observes_ack_before_route_registration() {
        let manager = ThreadStateManager::new();
        let parent_thread_id = ThreadId::new();
        let child_thread_id = ThreadId::new();
        let child_state = manager.thread_state(child_thread_id).await;
        {
            let mut state = child_state.lock().await;
            state.track_current_turn_event(
                "child-turn",
                &EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: "child-turn".to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                }),
            );
            state.track_current_turn_event("child-turn", &function_call("open", Some("spine")));
            state.record_spine_ui_snapshot(spine_snapshot(7, "1.1"));
            state.track_current_turn_event(
                "child-turn",
                &EventMsg::TurnComplete(TurnCompleteEvent {
                    turn_id: "child-turn".to_string(),
                    last_agent_message: None,
                    completed_at: None,
                    duration_ms: None,
                    time_to_first_token_ms: None,
                }),
            );
            state.take_turn_summary();
        }
        manager
            .acknowledge_spine_ui_agent_terminal(child_thread_id, 0, "child-turn")
            .await;
        register_test_spine_ui_route(&manager, parent_thread_id, "parent-turn", child_thread_id)
            .await;

        let (states, timed_out) = manager
            .wait_for_spine_ui_terminal_children(parent_thread_id, "parent-turn", Duration::ZERO)
            .await;

        assert!(timed_out.is_empty());
        assert_eq!(
            states[0]
                .2
                .as_ref()
                .and_then(SpineUiState::latest_snapshot)
                .map(|snapshot| snapshot.snapshot_seq),
            Some(7)
        );

        manager
            .note_spine_ui_agent_turn_started(child_thread_id, 0)
            .await;
        let (_, timed_out) = manager
            .wait_for_spine_ui_terminal_children(parent_thread_id, "parent-turn", Duration::ZERO)
            .await;
        assert_eq!(
            timed_out
                .iter()
                .map(|(thread_id, _)| *thread_id)
                .collect::<Vec<_>>(),
            vec![child_thread_id]
        );
    }

    #[tokio::test]
    async fn spine_ui_terminal_barrier_does_not_treat_received_as_acknowledged() {
        let manager = ThreadStateManager::new();
        let parent_thread_id = ThreadId::new();
        let child_thread_id = ThreadId::new();
        let child_state = manager.thread_state(child_thread_id).await;
        {
            let mut state = child_state.lock().await;
            state.track_current_turn_event(
                "child-turn",
                &EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: "child-turn".to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                }),
            );
            state.track_current_turn_event("child-turn", &function_call("open", Some("spine")));
            state.record_spine_ui_snapshot(spine_snapshot(3, "1.1"));
            state.track_current_turn_event(
                "child-turn",
                &EventMsg::TurnComplete(TurnCompleteEvent {
                    turn_id: "child-turn".to_string(),
                    last_agent_message: None,
                    completed_at: None,
                    duration_ms: None,
                    time_to_first_token_ms: None,
                }),
            );
        }
        register_test_spine_ui_route(&manager, parent_thread_id, "parent-turn", child_thread_id)
            .await;

        let (_, timed_out) = manager
            .wait_for_spine_ui_terminal_children(parent_thread_id, "parent-turn", Duration::ZERO)
            .await;

        assert_eq!(
            timed_out
                .iter()
                .map(|(thread_id, _)| *thread_id)
                .collect::<Vec<_>>(),
            vec![child_thread_id]
        );
    }

    #[tokio::test]
    async fn spine_ui_terminal_barrier_tracks_child_before_listener_registration() {
        let manager = ThreadStateManager::new();
        let parent_thread_id = ThreadId::new();
        let child_thread_id = ThreadId::new();
        register_test_spine_ui_route(&manager, parent_thread_id, "parent-turn", child_thread_id)
            .await;

        let (states, timed_out) = manager
            .wait_for_spine_ui_terminal_children(parent_thread_id, "parent-turn", Duration::ZERO)
            .await;

        assert_eq!(
            timed_out
                .iter()
                .map(|(thread_id, _)| *thread_id)
                .collect::<Vec<_>>(),
            vec![child_thread_id]
        );
        assert_eq!(states.len(), 1);
        assert!(states[0].2.is_none());
    }

    #[tokio::test]
    async fn spine_ui_terminal_barrier_rejects_a_stale_listener_ack() {
        let manager = ThreadStateManager::new();
        let parent_thread_id = ThreadId::new();
        let child_thread_id = ThreadId::new();
        let child_state = manager.thread_state(child_thread_id).await;
        {
            let mut state = child_state.lock().await;
            state.listener_generation = 2;
            state.track_current_turn_event(
                "child-turn",
                &EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: "child-turn".to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                }),
            );
            state.track_current_turn_event("child-turn", &function_call("open", Some("spine")));
            state.record_spine_ui_snapshot(spine_snapshot(4, "1.1"));
        }
        manager
            .note_spine_ui_listener_generation(child_thread_id, 2)
            .await;
        register_test_spine_ui_route(&manager, parent_thread_id, "parent-turn", child_thread_id)
            .await;
        {
            let mut state = child_state.lock().await;
            state.track_current_turn_event(
                "child-turn",
                &EventMsg::TurnComplete(TurnCompleteEvent {
                    turn_id: "child-turn".to_string(),
                    last_agent_message: None,
                    completed_at: None,
                    duration_ms: None,
                    time_to_first_token_ms: None,
                }),
            );
            state.take_turn_summary();
        }

        manager
            .acknowledge_spine_ui_agent_terminal(child_thread_id, 1, "child-turn")
            .await;
        let (_, timed_out) = manager
            .wait_for_spine_ui_terminal_children(parent_thread_id, "parent-turn", Duration::ZERO)
            .await;
        assert_eq!(
            timed_out
                .iter()
                .map(|(thread_id, _)| *thread_id)
                .collect::<Vec<_>>(),
            vec![child_thread_id]
        );

        manager
            .acknowledge_spine_ui_agent_terminal(child_thread_id, 2, "child-turn")
            .await;
        let (_, timed_out) = manager
            .wait_for_spine_ui_terminal_children(parent_thread_id, "parent-turn", Duration::ZERO)
            .await;
        assert!(timed_out.is_empty());
    }

    #[tokio::test]
    async fn spine_ui_route_does_not_reuse_ack_from_a_superseded_listener() {
        let manager = ThreadStateManager::new();
        let parent_thread_id = ThreadId::new();
        let child_thread_id = ThreadId::new();
        let child_state = manager.thread_state(child_thread_id).await;
        {
            let mut state = child_state.lock().await;
            state.listener_generation = 1;
            state.track_current_turn_event(
                "child-turn",
                &EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: "child-turn".to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                }),
            );
            state.track_current_turn_event("child-turn", &function_call("open", Some("spine")));
            state.record_spine_ui_snapshot(spine_snapshot(4, "1.1"));
            state.track_current_turn_event(
                "child-turn",
                &EventMsg::TurnComplete(TurnCompleteEvent {
                    turn_id: "child-turn".to_string(),
                    last_agent_message: None,
                    completed_at: None,
                    duration_ms: None,
                    time_to_first_token_ms: None,
                }),
            );
            state.take_turn_summary();
        }
        manager
            .note_spine_ui_listener_generation(child_thread_id, 1)
            .await;
        manager
            .acknowledge_spine_ui_agent_terminal(child_thread_id, 1, "child-turn")
            .await;

        child_state.lock().await.listener_generation = 2;
        manager
            .note_spine_ui_listener_generation(child_thread_id, 2)
            .await;
        register_test_spine_ui_route(&manager, parent_thread_id, "parent-turn", child_thread_id)
            .await;

        let (_, timed_out) = manager
            .wait_for_spine_ui_terminal_children(parent_thread_id, "parent-turn", Duration::ZERO)
            .await;
        assert_eq!(
            timed_out
                .iter()
                .map(|(thread_id, _)| *thread_id)
                .collect::<Vec<_>>(),
            vec![child_thread_id]
        );
    }

    #[tokio::test]
    async fn spine_ui_child_rollback_invalidates_parent_carry_after_terminal() {
        let manager = ThreadStateManager::new();
        let parent_thread_id = ThreadId::new();
        let child_thread_id = ThreadId::new();
        let parent_state = manager.thread_state(parent_thread_id).await;
        let child_state = manager.thread_state(child_thread_id).await;
        let progress = SpineSpawnProgressEvent {
            call_id: "spawn-1".to_string(),
            tasks: vec![agent_task(0, "Agent", child_thread_id)],
        };
        {
            let mut state = parent_state.lock().await;
            state.track_current_turn_event(
                "turn-1",
                &EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: "turn-1".to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                }),
            );
            state.track_current_turn_event("turn-1", &function_call("spawn", Some("spine")));
            state
                .turn_summary
                .spine_ui
                .record_snapshot(spine_snapshot(1, "1.1"));
            state
                .turn_summary
                .spine_ui
                .record_spawn_progress(progress.clone());
        }
        {
            let mut state = child_state.lock().await;
            state.track_current_turn_event(
                "child-turn",
                &EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: "child-turn".to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                }),
            );
            state.track_current_turn_event("child-turn", &function_call("open", Some("spine")));
            state
                .turn_summary
                .spine_ui
                .record_snapshot(spine_snapshot(3, "1.1"));
        }
        manager
            .register_spine_ui_spawn_progress(parent_thread_id, "turn-1", &progress)
            .await;
        let (mut agent_states, _) = manager
            .wait_for_spine_ui_terminal_children(parent_thread_id, "turn-1", Duration::ZERO)
            .await;
        let (_, generation, child_ui) = agent_states.pop().expect("child route state");
        let child_ui = child_ui.expect("latest child UI");
        {
            let mut state = parent_state.lock().await;
            assert!(state.record_spine_ui_agent_state(child_thread_id, generation, child_ui,));
            state.track_current_turn_event(
                "turn-1",
                &EventMsg::TurnComplete(TurnCompleteEvent {
                    turn_id: "turn-1".to_string(),
                    last_agent_message: None,
                    completed_at: None,
                    duration_ms: None,
                    time_to_first_token_ms: None,
                }),
            );
            state.take_turn_summary();
        }
        manager
            .clear_spine_ui_parent_routes(parent_thread_id, "turn-1")
            .await;

        manager
            .clear_all_spine_ui_routes_for_thread(child_thread_id)
            .await;

        {
            let mut state = parent_state.lock().await;
            assert!(state.spine_ui_carry_agent_generations().is_empty());
            state.track_current_turn_event(
                "turn-2",
                &EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: "turn-2".to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                }),
            );
            state.track_current_turn_event("turn-2", &function_call("open", Some("spine")));
            state.record_spine_ui_snapshot(spine_snapshot(2, "1.1"));
            let structured = state
                .turn_summary
                .spine_ui
                .structured_content()
                .expect("carried Spine UI");
            assert_eq!(structured["agentSubtrees"], serde_json::json!([]));
        }
    }

    #[tokio::test]
    async fn spine_ui_child_rollback_invalidates_nested_idle_ancestor_carries() {
        let manager = ThreadStateManager::new();
        let root_thread_id = ThreadId::new();
        let parent_thread_id = ThreadId::new();
        let child_thread_id = ThreadId::new();
        let root_state = manager.thread_state(root_thread_id).await;
        let parent_state = manager.thread_state(parent_thread_id).await;
        let child_state = manager.thread_state(child_thread_id).await;
        let parent_progress = SpineSpawnProgressEvent {
            call_id: "spawn-child".to_string(),
            tasks: vec![agent_task(0, "Child", child_thread_id)],
        };
        let root_progress = SpineSpawnProgressEvent {
            call_id: "spawn-parent".to_string(),
            tasks: vec![agent_task(0, "Parent", parent_thread_id)],
        };

        {
            let mut state = child_state.lock().await;
            state.track_current_turn_event(
                "child-turn",
                &EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: "child-turn".to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                }),
            );
            state.track_current_turn_event("child-turn", &function_call("open", Some("spine")));
            state.record_spine_ui_snapshot(spine_snapshot(7, "1.1"));
        }
        {
            let mut state = parent_state.lock().await;
            state.track_current_turn_event(
                "parent-turn",
                &EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: "parent-turn".to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                }),
            );
            state.track_current_turn_event("parent-turn", &function_call("spawn", Some("spine")));
            state.record_spine_ui_snapshot(spine_snapshot(2, "1.1"));
            state.record_spine_ui_spawn_progress(parent_progress.clone());
        }
        manager
            .register_spine_ui_spawn_progress(parent_thread_id, "parent-turn", &parent_progress)
            .await;
        let (mut child_states, _) = manager
            .wait_for_spine_ui_terminal_children(parent_thread_id, "parent-turn", Duration::ZERO)
            .await;
        let (_, child_generation, child_ui) = child_states.pop().expect("child state");
        {
            let mut state = parent_state.lock().await;
            state.record_spine_ui_agent_state(
                child_thread_id,
                child_generation,
                child_ui.expect("child UI"),
            );
            state.track_current_turn_event(
                "parent-turn",
                &EventMsg::TurnComplete(TurnCompleteEvent {
                    turn_id: "parent-turn".to_string(),
                    last_agent_message: None,
                    completed_at: None,
                    duration_ms: None,
                    time_to_first_token_ms: None,
                }),
            );
            state.take_turn_summary();
        }
        manager
            .clear_spine_ui_parent_routes(parent_thread_id, "parent-turn")
            .await;

        {
            let mut state = root_state.lock().await;
            state.track_current_turn_event(
                "root-turn",
                &EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: "root-turn".to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                }),
            );
            state.track_current_turn_event("root-turn", &function_call("spawn", Some("spine")));
            state.record_spine_ui_snapshot(spine_snapshot(1, "1.1"));
            state.record_spine_ui_spawn_progress(root_progress.clone());
        }
        manager
            .register_spine_ui_spawn_progress(root_thread_id, "root-turn", &root_progress)
            .await;
        let (mut parent_states, _) = manager
            .wait_for_spine_ui_terminal_children(root_thread_id, "root-turn", Duration::ZERO)
            .await;
        let (_, parent_generation, parent_ui) = parent_states.pop().expect("parent state");
        {
            let mut state = root_state.lock().await;
            state.record_spine_ui_agent_state(
                parent_thread_id,
                parent_generation,
                parent_ui.expect("parent UI"),
            );
            state.track_current_turn_event(
                "root-turn",
                &EventMsg::TurnComplete(TurnCompleteEvent {
                    turn_id: "root-turn".to_string(),
                    last_agent_message: None,
                    completed_at: None,
                    duration_ms: None,
                    time_to_first_token_ms: None,
                }),
            );
            state.take_turn_summary();
        }
        manager
            .clear_spine_ui_parent_routes(root_thread_id, "root-turn")
            .await;

        manager
            .clear_all_spine_ui_routes_for_thread(child_thread_id)
            .await;

        assert!(
            parent_state
                .lock()
                .await
                .spine_ui_carry_agent_generations()
                .is_empty()
        );
        assert!(
            root_state
                .lock()
                .await
                .spine_ui_carry_agent_generations()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn spine_ui_cold_restored_lineage_invalidates_completed_ancestor_cards() {
        let manager = ThreadStateManager::new();
        let root_thread_id = ThreadId::new();
        let parent_thread_id = ThreadId::new();
        let child_thread_id = ThreadId::new();

        let mut child_ui = SpineUiState::default();
        child_ui.record_snapshot(spine_snapshot(3, "1.1"));
        let mut parent_ui = SpineUiState::default();
        parent_ui.record_snapshot(spine_snapshot(2, "1.1"));
        parent_ui.record_spawn_progress(SpineSpawnProgressEvent {
            call_id: "spawn-child".to_string(),
            tasks: vec![agent_task(0, "Child", child_thread_id)],
        });
        assert!(parent_ui.record_agent_state(child_thread_id, 7, child_ui));

        let mut root_ui = SpineUiState::default();
        root_ui.record_snapshot(spine_snapshot(1, "1.1"));
        root_ui.record_spawn_progress(SpineSpawnProgressEvent {
            call_id: "spawn-parent".to_string(),
            tasks: vec![agent_task(0, "Parent", parent_thread_id)],
        });
        assert!(root_ui.record_agent_state(parent_thread_id, 9, parent_ui.clone()));

        manager
            .restore_spine_ui_completed_state(
                parent_thread_id,
                "parent-turn".to_string(),
                parent_ui,
            )
            .await;
        manager
            .restore_spine_ui_completed_state(root_thread_id, "root-turn".to_string(), root_ui)
            .await;

        let updates = manager
            .clear_all_spine_ui_routes_for_thread(child_thread_id)
            .await;

        assert_eq!(updates.len(), 2);
        assert!(updates.iter().any(|update| {
            update.thread_id == parent_thread_id && update.turn_id == "parent-turn"
        }));
        assert!(
            updates.iter().any(|update| {
                update.thread_id == root_thread_id && update.turn_id == "root-turn"
            })
        );
        assert!(
            manager
                .thread_state(parent_thread_id)
                .await
                .lock()
                .await
                .spine_ui_carry_agent_generations()
                .is_empty()
        );
        assert!(
            manager
                .thread_state(root_thread_id)
                .await
                .lock()
                .await
                .spine_ui_carry_agent_generations()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn spine_ui_timeout_only_cold_restore_can_be_invalidated() {
        let manager = ThreadStateManager::new();
        let parent_thread_id = ThreadId::new();
        let child_thread_id = ThreadId::new();
        let mut parent_ui = SpineUiState::default();
        parent_ui.record_snapshot(spine_snapshot(1, "1.1"));
        parent_ui.record_spawn_progress(SpineSpawnProgressEvent {
            call_id: "spawn-child".to_string(),
            tasks: vec![agent_task(0, "Child", child_thread_id)],
        });
        assert!(parent_ui.mark_agent_sync_timeout(child_thread_id, 7));

        manager
            .restore_spine_ui_completed_state(
                parent_thread_id,
                "parent-turn".to_string(),
                parent_ui,
            )
            .await;
        let updates = manager
            .clear_all_spine_ui_routes_for_thread(child_thread_id)
            .await;

        let [update] = updates.as_slice() else {
            panic!("expected one timeout-only parent update");
        };
        assert_eq!(update.thread_id, parent_thread_id);
        assert_eq!(update.turn_id, "parent-turn");
        assert!(
            !update
                .state
                .tracks_agent_or_timeout_generation(child_thread_id, 7)
        );
    }

    #[tokio::test]
    async fn spine_ui_late_terminal_refreshes_completed_ancestor_cards() {
        let manager = ThreadStateManager::new();
        let root_thread_id = ThreadId::new();
        let parent_thread_id = ThreadId::new();
        let child_thread_id = ThreadId::new();

        let mut initial_child_ui = SpineUiState::default();
        initial_child_ui.record_snapshot(spine_snapshot(3, "1.1"));
        initial_child_ui.set_revision(1);
        let mut parent_ui = SpineUiState::default();
        parent_ui.record_snapshot(spine_snapshot(2, "1.1"));
        let mut child_task = agent_task(0, "Child", child_thread_id);
        child_task.status = AgentStatus::Completed(None);
        parent_ui.record_spawn_progress(SpineSpawnProgressEvent {
            call_id: "spawn-child".to_string(),
            tasks: vec![child_task],
        });
        assert!(parent_ui.record_agent_state(child_thread_id, 7, initial_child_ui));
        assert!(parent_ui.mark_agent_sync_timeout(child_thread_id, 7));

        let mut root_ui = SpineUiState::default();
        root_ui.record_snapshot(spine_snapshot(1, "1.1"));
        root_ui.record_spawn_progress(SpineSpawnProgressEvent {
            call_id: "spawn-parent".to_string(),
            tasks: vec![agent_task(0, "Parent", parent_thread_id)],
        });
        assert!(root_ui.record_agent_state(parent_thread_id, 9, parent_ui.clone()));
        manager
            .restore_spine_ui_completed_state(
                parent_thread_id,
                "parent-turn".to_string(),
                parent_ui,
            )
            .await;
        manager
            .restore_spine_ui_completed_state(root_thread_id, "root-turn".to_string(), root_ui)
            .await;

        let mut final_child_ui = SpineUiState::default();
        final_child_ui.record_snapshot(spine_snapshot(4, "1.2"));
        final_child_ui.set_revision(2);
        let parent_state = manager.thread_state(parent_thread_id).await;
        let refresh = parent_state
            .lock()
            .await
            .record_completed_spine_ui_agent_terminal(
                "parent-turn",
                child_thread_id,
                7,
                Some(final_child_ui),
            );
        let completed_parent = refresh
            .completed
            .into_iter()
            .find_map(|(turn_id, state)| (turn_id == "parent-turn").then_some(state))
            .expect("late child terminal should refresh the parent card");

        let updates = manager
            .propagate_spine_ui_completed_agent_state(parent_thread_id, completed_parent)
            .await;

        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].thread_id, root_thread_id);
        assert_eq!(updates[0].turn_id, "root-turn");
        assert!(updates[0].state.revision() > 0);
        let structured = updates[0]
            .state
            .structured_content()
            .expect("updated root structured content");
        assert_eq!(
            structured["agentSubtrees"][0]["agentSubtrees"][0]["snapshot"]["snapshotSeq"],
            4
        );
        assert_eq!(
            structured["agentSubtrees"][0]["spawnCalls"][0]["tasks"][0]["status"],
            serde_json::json!({ "completed": null })
        );
        assert_eq!(
            structured["agentSubtrees"][0]["agentSyncTimeoutGenerations"],
            serde_json::json!([])
        );
    }

    #[test]
    fn spine_ui_stale_completed_refresh_does_not_replace_a_newer_generation() {
        let child_thread_id = ThreadId::new();
        let mut parent = ThreadState::default();
        parent.track_current_turn_event(
            "turn-1",
            &EventMsg::TurnStarted(TurnStartedEvent {
                turn_id: "turn-1".to_string(),
                trace_id: None,
                started_at: None,
                model_context_window: None,
                collaboration_mode_kind: Default::default(),
            }),
        );
        parent.track_current_turn_event("turn-1", &function_call("spawn", Some("spine")));
        parent.record_spine_ui_snapshot(spine_snapshot(1, "1.1"));
        parent.record_spine_ui_spawn_progress(SpineSpawnProgressEvent {
            call_id: "spawn-child".to_string(),
            tasks: vec![agent_task(0, "Child", child_thread_id)],
        });
        let mut current_child = SpineUiState::default();
        current_child.record_snapshot(spine_snapshot(9, "1.2"));
        assert!(parent.record_spine_ui_agent_state(child_thread_id, 2, current_child));

        let revision = parent.turn_summary.spine_ui.revision();
        let mut stale_child = SpineUiState::default();
        stale_child.record_snapshot(spine_snapshot(4, "1.1"));
        let refresh = parent.refresh_spine_ui_agent_state(child_thread_id, 1, stale_child);

        assert!(refresh.active.is_none());
        assert!(refresh.completed.is_empty());
        assert!(refresh.forward.is_none());
        assert_eq!(parent.turn_summary.spine_ui.revision(), revision);
        let structured = parent
            .turn_summary
            .spine_ui
            .structured_content()
            .expect("current parent structured content");
        assert_eq!(structured["agentSubtrees"][0]["snapshot"]["snapshotSeq"], 9);
    }

    #[tokio::test]
    async fn spine_ui_cold_restore_invalidates_every_carried_completed_card() {
        let manager = ThreadStateManager::new();
        let parent_thread_id = ThreadId::new();
        let child_thread_id = ThreadId::new();
        let mut child_ui = SpineUiState::default();
        child_ui.record_snapshot(spine_snapshot(3, "1.1"));
        let mut first_parent_ui = SpineUiState::default();
        first_parent_ui.record_snapshot(spine_snapshot(1, "1.1"));
        first_parent_ui.record_spawn_progress(SpineSpawnProgressEvent {
            call_id: "spawn-child".to_string(),
            tasks: vec![agent_task(0, "Child", child_thread_id)],
        });
        assert!(first_parent_ui.record_agent_state(child_thread_id, 7, child_ui));
        let mut second_parent_ui = first_parent_ui.clone();
        second_parent_ui.record_snapshot(spine_snapshot(2, "1.1"));

        manager
            .restore_spine_ui_completed_state(
                parent_thread_id,
                "parent-turn-2".to_string(),
                second_parent_ui,
            )
            .await;
        manager
            .restore_spine_ui_completed_state(
                parent_thread_id,
                "parent-turn-1".to_string(),
                first_parent_ui,
            )
            .await;

        let updates = manager
            .clear_all_spine_ui_routes_for_thread(child_thread_id)
            .await;

        assert_eq!(updates.len(), 2);
        assert!(updates.iter().all(|update| {
            update.thread_id == parent_thread_id
                && !update
                    .state
                    .agent_generations()
                    .contains_key(&child_thread_id)
        }));
        assert_eq!(
            updates
                .iter()
                .map(|update| update.turn_id.as_str())
                .collect::<HashSet<_>>(),
            HashSet::from(["parent-turn-1", "parent-turn-2"])
        );
    }

    #[tokio::test]
    async fn spine_ui_cold_restore_does_not_replace_an_active_card() {
        let manager = ThreadStateManager::new();
        let parent_thread_id = ThreadId::new();
        let child_thread_id = ThreadId::new();
        let parent_state = manager.thread_state(parent_thread_id).await;
        {
            let mut state = parent_state.lock().await;
            state.track_current_turn_event(
                "active-turn",
                &EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: "active-turn".to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                }),
            );
            state.track_current_turn_event("active-turn", &function_call("open", Some("spine")));
            state.record_spine_ui_snapshot(spine_snapshot(99, "1.1"));
        }

        let mut child_ui = SpineUiState::default();
        child_ui.record_snapshot(spine_snapshot(3, "1.1"));
        let mut restored_ui = SpineUiState::default();
        restored_ui.record_snapshot(spine_snapshot(2, "1.1"));
        restored_ui.record_spawn_progress(SpineSpawnProgressEvent {
            call_id: "spawn-child".to_string(),
            tasks: vec![agent_task(0, "Child", child_thread_id)],
        });
        assert!(restored_ui.record_agent_state(child_thread_id, 7, child_ui));
        manager
            .restore_spine_ui_completed_state(
                parent_thread_id,
                "historical-turn".to_string(),
                restored_ui,
            )
            .await;

        let updates = manager
            .clear_all_spine_ui_routes_for_thread(child_thread_id)
            .await;

        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].turn_id, "historical-turn");
        assert!(
            !updates[0]
                .state
                .agent_generations()
                .contains_key(&child_thread_id)
        );
        let active = parent_state
            .lock()
            .await
            .active_spine_ui_snapshot()
            .expect("active Spine UI card");
        assert_eq!(active.0, "active-turn");
        assert_eq!(
            active
                .1
                .latest_snapshot()
                .map(|snapshot| snapshot.snapshot_seq),
            Some(99)
        );
    }

    #[tokio::test]
    async fn spine_ui_cold_restore_does_not_replace_newer_in_memory_lineage() {
        let manager = ThreadStateManager::new();
        let parent_thread_id = ThreadId::new();
        let child_thread_id = ThreadId::new();
        let mut child_ui = SpineUiState::default();
        child_ui.record_snapshot(spine_snapshot(3, "1.1"));
        let mut current_parent_ui = SpineUiState::default();
        current_parent_ui.record_snapshot(spine_snapshot(2, "1.1"));
        current_parent_ui.record_spawn_progress(SpineSpawnProgressEvent {
            call_id: "spawn-current-child".to_string(),
            tasks: vec![agent_task(0, "Current child", child_thread_id)],
        });
        assert!(current_parent_ui.record_agent_state(child_thread_id, 2, child_ui.clone()));
        manager
            .restore_spine_ui_completed_state(
                parent_thread_id,
                "current-turn".to_string(),
                current_parent_ui,
            )
            .await;

        let mut stale_parent_ui = SpineUiState::default();
        stale_parent_ui.record_snapshot(spine_snapshot(1, "1.1"));
        stale_parent_ui.record_spawn_progress(SpineSpawnProgressEvent {
            call_id: "spawn-stale-child".to_string(),
            tasks: vec![agent_task(0, "Stale child", child_thread_id)],
        });
        assert!(stale_parent_ui.record_agent_state(child_thread_id, 1, child_ui));
        manager
            .restore_spine_ui_completed_state(
                parent_thread_id,
                "stale-turn".to_string(),
                stale_parent_ui,
            )
            .await;

        let updates = manager
            .clear_all_spine_ui_routes_for_thread(child_thread_id)
            .await;

        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].turn_id, "current-turn");
        assert!(
            manager
                .thread_state(parent_thread_id)
                .await
                .lock()
                .await
                .spine_ui_carry_agent_generations()
                .is_empty()
        );
    }

    #[test]
    fn spine_ui_stale_generation_invalidation_keeps_replacement_agent() {
        let child_thread_id = ThreadId::new();
        let mut parent = ThreadState::default();
        parent.track_current_turn_event(
            "turn-1",
            &EventMsg::TurnStarted(TurnStartedEvent {
                turn_id: "turn-1".to_string(),
                trace_id: None,
                started_at: None,
                model_context_window: None,
                collaboration_mode_kind: Default::default(),
            }),
        );
        parent.track_current_turn_event("turn-1", &function_call("spawn", Some("spine")));
        parent.record_spine_ui_snapshot(spine_snapshot(1, "1.1"));
        parent.record_spine_ui_spawn_progress(SpineSpawnProgressEvent {
            call_id: "spawn-1".to_string(),
            tasks: vec![agent_task(0, "Agent", child_thread_id)],
        });
        let mut child = SpineUiState::default();
        child.record_snapshot(spine_snapshot(2, "1.1"));
        assert!(parent.record_spine_ui_agent_state(child_thread_id, 2, child));

        assert!(!parent.invalidate_spine_ui_agent_state(child_thread_id, 1));
        assert_eq!(
            parent
                .turn_summary
                .spine_ui
                .agent_generations()
                .get(&child_thread_id),
            Some(&2)
        );
    }

    #[test]
    fn spine_ui_invalidated_generation_cannot_be_reinserted() {
        let child_thread_id = ThreadId::new();
        let mut parent = ThreadState::default();
        parent.track_current_turn_event(
            "turn-1",
            &EventMsg::TurnStarted(TurnStartedEvent {
                turn_id: "turn-1".to_string(),
                trace_id: None,
                started_at: None,
                model_context_window: None,
                collaboration_mode_kind: Default::default(),
            }),
        );
        parent.track_current_turn_event("turn-1", &function_call("spawn", Some("spine")));
        parent.record_spine_ui_snapshot(spine_snapshot(1, "1.1"));
        parent.record_spine_ui_spawn_progress(SpineSpawnProgressEvent {
            call_id: "spawn-1".to_string(),
            tasks: vec![agent_task(0, "Agent", child_thread_id)],
        });
        let mut child = SpineUiState::default();
        child.record_snapshot(spine_snapshot(2, "1.1"));

        assert!(parent.record_spine_ui_agent_state(child_thread_id, 1, child.clone()));
        assert!(parent.invalidate_spine_ui_agent_state(child_thread_id, 1));
        assert!(!parent.record_spine_ui_agent_state(child_thread_id, 1, child.clone()));
        assert!(parent.record_spine_ui_agent_state(child_thread_id, 2, child));
    }

    #[tokio::test]
    async fn spine_ui_rollback_invalidates_queued_route_generation() {
        let manager = ThreadStateManager::new();
        let parent_thread_id = ThreadId::new();
        let child_thread_id = ThreadId::new();
        let parent_state = manager.thread_state(parent_thread_id).await;
        let child_state = manager.thread_state(child_thread_id).await;
        let (listener_tx, mut listener_rx) = mpsc::unbounded_channel();
        manager.register_listener_command_tx(parent_thread_id, listener_tx);
        let progress = SpineSpawnProgressEvent {
            call_id: "spawn-1".to_string(),
            tasks: vec![agent_task(0, "Agent", child_thread_id)],
        };
        {
            let mut state = parent_state.lock().await;
            state.track_current_turn_event(
                "turn-1",
                &EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: "turn-1".to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                }),
            );
            state.track_current_turn_event("turn-1", &function_call("spawn", Some("spine")));
            state
                .turn_summary
                .spine_ui
                .record_snapshot(spine_snapshot(1, "1.1"));
            state
                .turn_summary
                .spine_ui
                .record_spawn_progress(progress.clone());
        }
        {
            let mut state = child_state.lock().await;
            state.track_current_turn_event(
                "child-turn",
                &EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: "child-turn".to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                }),
            );
            state.track_current_turn_event("child-turn", &function_call("open", Some("spine")));
            state
                .turn_summary
                .spine_ui
                .record_snapshot(spine_snapshot(3, "1.1"));
        }
        manager
            .register_spine_ui_spawn_progress(parent_thread_id, "turn-1", &progress)
            .await;
        let ThreadListenerCommand::ForwardSpineUiAgentState {
            generation: first_generation,
            state: Some(child_ui),
            ..
        } = listener_rx.recv().await.expect("initial forward")
        else {
            panic!("expected initial forward");
        };
        assert!(parent_state.lock().await.record_spine_ui_agent_state(
            child_thread_id,
            first_generation,
            child_ui,
        ));

        let manager_for_wait = manager.clone();
        let waiter = tokio::spawn(async move {
            manager_for_wait
                .wait_for_spine_ui_terminal_children(
                    parent_thread_id,
                    "turn-1",
                    Duration::from_secs(2),
                )
                .await
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        manager
            .clear_all_spine_ui_routes_for_thread(child_thread_id)
            .await;
        let (terminal_states, timed_out) = waiter.await.expect("terminal barrier task");
        assert!(timed_out.is_empty());
        assert_eq!(terminal_states.len(), 1);
        assert_eq!(terminal_states[0].0, child_thread_id);
        assert_eq!(terminal_states[0].1, first_generation);
        assert!(terminal_states[0].2.is_none());
        let ThreadListenerCommand::EmitSpineUiInvalidation { .. } =
            listener_rx.recv().await.expect("rollback removal")
        else {
            panic!("expected rollback removal");
        };
        assert!(
            parent_state
                .lock()
                .await
                .turn_summary
                .spine_ui
                .agent_generations()
                .is_empty()
        );
        assert!(
            !manager
                .spine_ui_route_is_current(
                    child_thread_id,
                    parent_thread_id,
                    "turn-1",
                    first_generation,
                )
                .await
        );

        manager
            .register_spine_ui_spawn_progress(parent_thread_id, "turn-1", &progress)
            .await;
        let ThreadListenerCommand::ForwardSpineUiAgentState {
            generation: second_generation,
            ..
        } = listener_rx.recv().await.expect("replacement forward")
        else {
            panic!("expected replacement forward");
        };
        assert!(second_generation > first_generation);
    }

    async fn register_test_spine_ui_route(
        manager: &ThreadStateManager,
        parent_thread_id: ThreadId,
        parent_turn_id: &str,
        child_thread_id: ThreadId,
    ) {
        let parent_state = manager.thread_state(parent_thread_id).await;
        let progress = SpineSpawnProgressEvent {
            call_id: format!("spawn-{parent_turn_id}"),
            tasks: vec![agent_task(0, "Agent", child_thread_id)],
        };
        {
            let mut state = parent_state.lock().await;
            state.track_current_turn_event(
                parent_turn_id,
                &EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: parent_turn_id.to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                }),
            );
            state.track_current_turn_event(parent_turn_id, &function_call("spawn", Some("spine")));
            state.record_spine_ui_snapshot(spine_snapshot(1, "1.1"));
            state.record_spine_ui_spawn_progress(progress.clone());
        }
        manager
            .register_spine_ui_spawn_progress(parent_thread_id, parent_turn_id, &progress)
            .await;
    }

    fn agent_task(ordinal: u32, summary: &str, thread_id: ThreadId) -> SpineSpawnTaskProgress {
        SpineSpawnTaskProgress {
            ordinal,
            summary: summary.to_string(),
            thread_id,
            agent_path: Some(
                AgentPath::try_from(format!("/root/agent_{ordinal}")).expect("agent path"),
            ),
            status: AgentStatus::Running,
        }
    }

    fn completed_timed_out_parent_state(child_thread_id: ThreadId, generation: u64) -> ThreadState {
        let mut state = ThreadState::default();
        state.track_current_turn_event(
            "turn-1",
            &EventMsg::TurnStarted(TurnStartedEvent {
                turn_id: "turn-1".to_string(),
                trace_id: None,
                started_at: None,
                model_context_window: None,
                collaboration_mode_kind: Default::default(),
            }),
        );
        state.track_current_turn_event("turn-1", &function_call("spawn", Some("spine")));
        state.record_spine_ui_snapshot(spine_snapshot(1, "1.1"));
        state.record_spine_ui_spawn_progress(SpineSpawnProgressEvent {
            call_id: "spawn-child".to_string(),
            tasks: vec![agent_task(0, "Child", child_thread_id)],
        });
        let mut child_ui = SpineUiState::default();
        child_ui.record_snapshot(spine_snapshot(2, "1.1"));
        assert!(state.record_spine_ui_agent_state(child_thread_id, generation, child_ui));
        assert!(state.mark_spine_ui_agent_sync_timeout(child_thread_id, generation));
        state.track_current_turn_event(
            "turn-1",
            &EventMsg::TurnComplete(TurnCompleteEvent {
                turn_id: "turn-1".to_string(),
                last_agent_message: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            }),
        );
        state.take_turn_summary();
        state
    }

    #[test]
    fn late_terminal_prunes_every_restored_card_without_remaining_timeouts() {
        let child_thread_id = ThreadId::new();
        let generation = 7;
        let mut state = completed_timed_out_parent_state(child_thread_id, generation);
        let timed_out_card = state.spine_ui_last_completed.clone();
        state
            .spine_ui_restored_completed
            .insert("turn-1".to_string(), timed_out_card.clone());
        state
            .spine_ui_restored_completed
            .insert("turn-2".to_string(), timed_out_card);
        state.spine_ui_last_completed_turn_id = Some("turn-3".to_string());
        let mut child_state = SpineUiState::default();
        child_state.record_snapshot(spine_snapshot(3, "1.1"));

        let refresh = state.record_completed_spine_ui_agent_terminal(
            "turn-1",
            child_thread_id,
            generation,
            Some(child_state),
        );

        assert_eq!(refresh.completed.len(), 3);
        assert!(state.spine_ui_restored_completed.is_empty());
    }

    fn spine_snapshot(sequence: u64, active_node_id: &str) -> SpineTreeUpdateEvent {
        SpineTreeUpdateEvent {
            snapshot_seq: sequence,
            active_node_id: active_node_id.to_string(),
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
                    status: SpineTreeNodeStatus::Live,
                    summary: Some("Work".to_string()),
                    memory_summary: None,
                    spawn_outcome: None,
                    start: 1,
                    end: None,
                    context_pressure: None,
                },
            ],
        }
    }

    fn function_call(name: &str, namespace: Option<&str>) -> EventMsg {
        EventMsg::RawResponseItem(RawResponseItemEvent {
            item: ResponseItem::FunctionCall {
                id: None,
                name: name.to_string(),
                namespace: namespace.map(str::to_string),
                arguments: "{}".to_string(),
                call_id: format!("call-{name}"),
                internal_chat_message_metadata_passthrough: None,
            },
        })
    }

    fn thread_settings(model: &str) -> ThreadSettings {
        ThreadSettings {
            cwd: AbsolutePathBuf::from_absolute_path("/tmp").expect("absolute path"),
            approval_policy: AskForApproval::OnRequest,
            approvals_reviewer: ApprovalsReviewer::User,
            sandbox_policy: SandboxPolicy::ReadOnly {
                network_access: false,
            },
            active_permission_profile: None,
            model: model.to_string(),
            model_provider: "mock_provider".to_string(),
            service_tier: None,
            effort: None,
            summary: None,
            collaboration_mode: CollaborationMode {
                mode: ModeKind::Default,
                settings: Settings {
                    model: model.to_string(),
                    reasoning_effort: None,
                    developer_instructions: None,
                },
            },
            multi_agent_mode: MultiAgentMode::ExplicitRequestOnly,
            personality: None,
        }
    }
}

struct ThreadEntry {
    state: Arc<Mutex<ThreadState>>,
    connection_ids: HashSet<ConnectionId>,
    has_connections_watcher: watch::Sender<bool>,
    listener_generation: u64,
    spine_ui_terminal_ack: Option<SpineUiTerminalAck>,
}

#[derive(Clone)]
struct SpineUiTerminalAck {
    listener_generation: u64,
    state: Option<SpineUiState>,
}

#[derive(Clone)]
struct SpineUiParentRoute {
    parent_thread_id: ThreadId,
    parent_turn_id: String,
    baseline_node_ids: HashSet<String>,
    generation: u64,
    timed_out_at: Option<Instant>,
    terminal_tx: watch::Sender<SpineUiRouteTerminalState>,
}

#[derive(Clone, Debug, Default)]
enum SpineUiRouteTerminalState {
    #[default]
    Pending,
    TimedOut,
    Settled(Option<Box<SpineUiState>>),
    Invalidated,
}

#[derive(Clone)]
struct SpineUiCarryLineage {
    parent_thread_id: ThreadId,
    generation: u64,
    baseline_node_ids: HashSet<String>,
}

impl Default for ThreadEntry {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(ThreadState::default())),
            connection_ids: HashSet::new(),
            has_connections_watcher: watch::channel(false).0,
            listener_generation: 0,
            spine_ui_terminal_ack: None,
        }
    }
}

impl ThreadEntry {
    fn update_has_connections(&self) {
        let _ = self.has_connections_watcher.send_if_modified(|current| {
            let prev = *current;
            *current = !self.connection_ids.is_empty();
            prev != *current
        });
    }
}

#[derive(Default)]
struct ThreadStateManagerInner {
    live_connections: HashMap<ConnectionId, ConnectionCapabilities>,
    threads: HashMap<ThreadId, ThreadEntry>,
    thread_ids_by_connection: HashMap<ConnectionId, HashSet<ThreadId>>,
    spine_ui_parent_by_child: HashMap<ThreadId, SpineUiParentRoute>,
    spine_ui_carry_parent_by_child: HashMap<ThreadId, SpineUiCarryLineage>,
    spine_ui_invalidated_generation_by_child: HashMap<ThreadId, u64>,
    next_spine_ui_route_generation: u64,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct ConnectionCapabilities {
    pub(crate) request_attestation: bool,
}

#[derive(Clone, Default)]
pub(crate) struct ThreadStateManager {
    state: Arc<Mutex<ThreadStateManagerInner>>,
    // Extension event sinks are synchronous, so they need an await-free way to
    // enqueue work on the active per-thread listener.
    listener_commands:
        Arc<StdMutex<HashMap<ThreadId, mpsc::UnboundedSender<ThreadListenerCommand>>>>,
}

impl ThreadStateManager {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) async fn connection_initialized(
        &self,
        connection_id: ConnectionId,
        capabilities: ConnectionCapabilities,
    ) {
        self.state
            .lock()
            .await
            .live_connections
            .insert(connection_id, capabilities);
    }

    pub(crate) async fn first_attestation_capable_connection_for_thread(
        &self,
        thread_id: ThreadId,
    ) -> Option<ConnectionId> {
        let state = self.state.lock().await;
        state
            .threads
            .get(&thread_id)?
            .connection_ids
            .iter()
            .filter_map(|connection_id| {
                state
                    .live_connections
                    .get(connection_id)?
                    .request_attestation
                    .then_some(*connection_id)
            })
            .min_by_key(|connection_id| connection_id.0)
    }

    pub(crate) async fn wait_for_thread_subscriber(&self, thread_id: ThreadId) {
        let mut has_connections = {
            let mut state = self.state.lock().await;
            state
                .threads
                .entry(thread_id)
                .or_default()
                .has_connections_watcher
                .subscribe()
        };
        while !*has_connections.borrow_and_update() {
            if has_connections.changed().await.is_err() {
                break;
            }
        }
    }

    pub(crate) async fn subscribed_connection_ids(&self, thread_id: ThreadId) -> Vec<ConnectionId> {
        let state = self.state.lock().await;
        state
            .threads
            .get(&thread_id)
            .map(|thread_entry| thread_entry.connection_ids.iter().copied().collect())
            .unwrap_or_default()
    }

    pub(crate) async fn thread_state(&self, thread_id: ThreadId) -> Arc<Mutex<ThreadState>> {
        let mut state = self.state.lock().await;
        state.threads.entry(thread_id).or_default().state.clone()
    }

    pub(crate) async fn note_spine_ui_listener_generation(
        &self,
        thread_id: ThreadId,
        listener_generation: u64,
    ) {
        let mut state = self.state.lock().await;
        let generation_changed = {
            let entry = state.threads.entry(thread_id).or_default();
            let generation_changed = entry.listener_generation != listener_generation;
            if generation_changed {
                entry.spine_ui_terminal_ack = None;
            }
            entry.listener_generation = listener_generation;
            generation_changed
        };
        if generation_changed && let Some(route) = state.spine_ui_parent_by_child.get(&thread_id) {
            route
                .terminal_tx
                .send_replace(SpineUiRouteTerminalState::Pending);
        }
    }

    pub(crate) async fn note_spine_ui_agent_turn_started(
        &self,
        thread_id: ThreadId,
        listener_generation: u64,
    ) {
        let mut state = self.state.lock().await;
        let Some(entry) = state.threads.get_mut(&thread_id) else {
            return;
        };
        if entry.listener_generation != listener_generation {
            return;
        }
        entry.spine_ui_terminal_ack = None;
        if let Some(route) = state.spine_ui_parent_by_child.get(&thread_id) {
            route
                .terminal_tx
                .send_replace(SpineUiRouteTerminalState::Pending);
        }
    }

    pub(crate) async fn spine_ui_state_for_thread(
        &self,
        thread_id: ThreadId,
    ) -> Option<SpineUiState> {
        let thread_state = self
            .state
            .lock()
            .await
            .threads
            .get(&thread_id)
            .map(|entry| entry.state.clone())?;
        let thread_state = thread_state.lock().await;
        thread_state.current_spine_ui_for_forward().cloned()
    }

    pub(crate) async fn restore_spine_ui_completed_state(
        &self,
        parent_thread_id: ThreadId,
        turn_id: String,
        state: SpineUiState,
    ) {
        let generations = state.tracked_agent_generations();
        let max_generation = generations.values().copied().max().unwrap_or_default();
        let parent_state = self.thread_state(parent_thread_id).await;
        parent_state
            .lock()
            .await
            .restore_spine_ui_completed_state(turn_id, state);
        let mut manager = self.state.lock().await;
        manager.next_spine_ui_route_generation =
            manager.next_spine_ui_route_generation.max(max_generation);
        for (child_thread_id, generation) in generations {
            if manager
                .spine_ui_invalidated_generation_by_child
                .get(&child_thread_id)
                .is_some_and(|invalidated| *invalidated >= generation)
            {
                continue;
            }
            let restored_lineage = SpineUiCarryLineage {
                parent_thread_id,
                generation,
                baseline_node_ids: HashSet::new(),
            };
            manager
                .spine_ui_carry_parent_by_child
                .entry(child_thread_id)
                .and_modify(|current| {
                    if generation > current.generation {
                        *current = restored_lineage.clone();
                    }
                })
                .or_insert(restored_lineage);
        }
    }

    pub(crate) async fn register_spine_ui_spawn_progress(
        &self,
        parent_thread_id: ThreadId,
        parent_turn_id: &str,
        progress: &SpineSpawnProgressEvent,
    ) {
        let parent_state = self.thread_state(parent_thread_id).await;
        let baseline_node_ids: HashSet<String> = {
            let parent_state = parent_state.lock().await;
            if parent_state.live_spine_ui(parent_turn_id).is_none() {
                return;
            }
            parent_state
                .turn_summary
                .spine_ui
                .latest_snapshot()
                .map(|snapshot| {
                    snapshot
                        .nodes
                        .iter()
                        .map(|node| node.node_id.clone())
                        .collect()
                })
                .unwrap_or_default()
        };

        {
            let mut state = self.state.lock().await;
            for task in &progress.tasks {
                let entry = state.threads.entry(task.thread_id).or_default();
                let terminal_ack = entry
                    .spine_ui_terminal_ack
                    .clone()
                    .filter(|ack| entry.listener_generation == ack.listener_generation);
                let existing = state.spine_ui_parent_by_child.get(&task.thread_id).cloned();
                if existing.is_some_and(|route| {
                    route.parent_thread_id == parent_thread_id
                        && route.parent_turn_id == parent_turn_id
                }) {
                    continue;
                }
                if let Some(previous) = state.spine_ui_parent_by_child.get(&task.thread_id) {
                    previous
                        .terminal_tx
                        .send_replace(SpineUiRouteTerminalState::Invalidated);
                }
                state.next_spine_ui_route_generation =
                    state.next_spine_ui_route_generation.saturating_add(1);
                let generation = state.next_spine_ui_route_generation;
                let terminal_state =
                    terminal_ack.map_or(SpineUiRouteTerminalState::Pending, |ack| {
                        SpineUiRouteTerminalState::Settled(
                            ack.state.map(|state| {
                                Box::new(state.filtered_for_parent(&baseline_node_ids))
                            }),
                        )
                    });
                let terminal_tx = watch::channel(terminal_state).0;
                state.spine_ui_parent_by_child.insert(
                    task.thread_id,
                    SpineUiParentRoute {
                        parent_thread_id,
                        parent_turn_id: parent_turn_id.to_string(),
                        baseline_node_ids: baseline_node_ids.clone(),
                        generation,
                        timed_out_at: None,
                        terminal_tx,
                    },
                );
            }
        }
        for task in &progress.tasks {
            self.queue_spine_ui_agent_state(task.thread_id).await;
        }
    }

    pub(crate) async fn queue_spine_ui_agent_state(&self, child_thread_id: ThreadId) {
        let (route, child_state) = {
            let state = self.state.lock().await;
            let Some(route) = state
                .spine_ui_parent_by_child
                .get(&child_thread_id)
                .cloned()
            else {
                return;
            };
            let Some(child_state) = state
                .threads
                .get(&child_thread_id)
                .map(|entry| entry.state.clone())
            else {
                return;
            };
            (route, child_state)
        };
        let state = {
            let child_state = child_state.lock().await;
            let Some(state) = child_state.current_spine_ui_for_forward() else {
                return;
            };
            state.filtered_for_parent(&route.baseline_node_ids)
        };
        let Some(tx) = self.current_listener_command_tx(route.parent_thread_id) else {
            return;
        };
        let _ = tx.send(ThreadListenerCommand::ForwardSpineUiAgentState {
            child_thread_id,
            parent_turn_id: route.parent_turn_id,
            generation: route.generation,
            state: Some(state),
            terminal: false,
        });
    }

    pub(crate) async fn acknowledge_spine_ui_agent_terminal(
        &self,
        child_thread_id: ThreadId,
        listener_generation: u64,
        terminal_turn_id: &str,
    ) {
        let (route, child_state) = {
            let state = self.state.lock().await;
            let Some(entry) = state.threads.get(&child_thread_id) else {
                return;
            };
            if entry.listener_generation != listener_generation {
                return;
            }
            (
                state
                    .spine_ui_parent_by_child
                    .get(&child_thread_id)
                    .cloned(),
                entry.state.clone(),
            )
        };
        let terminal_state = {
            let child_state = child_state.lock().await;
            if child_state.current_turn_history.has_active_turn()
                || child_state.listener_generation != listener_generation
                || child_state.last_terminal_turn_id.as_deref() != Some(terminal_turn_id)
            {
                return;
            }
            child_state.current_spine_ui_for_forward().cloned()
        };

        let mut state = self.state.lock().await;
        let Some(entry) = state.threads.get_mut(&child_thread_id) else {
            return;
        };
        if entry.listener_generation != listener_generation {
            return;
        }
        entry.spine_ui_terminal_ack = Some(SpineUiTerminalAck {
            listener_generation,
            state: terminal_state.clone(),
        });
        let late_terminal = if let Some(route) = route
            && let Some(current) = state.spine_ui_parent_by_child.get_mut(&child_thread_id)
            && current.generation == route.generation
        {
            let was_timed_out = current.timed_out_at.is_some();
            if was_timed_out {
                current.timed_out_at = Some(Instant::now());
            }
            let filtered_state = terminal_state
                .map(|state| Box::new(state.filtered_for_parent(&route.baseline_node_ids)));
            current
                .terminal_tx
                .send_replace(SpineUiRouteTerminalState::Settled(filtered_state.clone()));
            was_timed_out.then_some((route, filtered_state.map(|state| *state)))
        } else {
            None
        };
        drop(state);

        if let Some((route, state)) = late_terminal
            && let Some(tx) = self.current_listener_command_tx(route.parent_thread_id)
        {
            let _ = tx.send(ThreadListenerCommand::ForwardSpineUiAgentState {
                child_thread_id,
                parent_turn_id: route.parent_turn_id,
                generation: route.generation,
                state,
                terminal: true,
            });
        }
    }

    pub(crate) async fn wait_for_spine_ui_terminal_children(
        &self,
        parent_thread_id: ThreadId,
        parent_turn_id: &str,
        timeout: Duration,
    ) -> (
        Vec<(ThreadId, u64, Option<SpineUiState>)>,
        Vec<(ThreadId, u64)>,
    ) {
        let candidates = {
            let state = self.state.lock().await;
            state
                .spine_ui_parent_by_child
                .iter()
                .filter(|(_, route)| {
                    route.parent_thread_id == parent_thread_id
                        && route.parent_turn_id == parent_turn_id
                })
                .filter_map(|(child_thread_id, route)| {
                    let child_state = state.threads.get(child_thread_id)?.state.clone();
                    Some((
                        *child_thread_id,
                        route.generation,
                        route.baseline_node_ids.clone(),
                        route.terminal_tx.subscribe(),
                        child_state,
                    ))
                })
                .collect::<Vec<_>>()
        };

        let deadline = tokio::time::Instant::now() + timeout;
        let mut states = Vec::with_capacity(candidates.len());
        let mut timed_out = Vec::new();
        for (child_thread_id, generation, baseline_node_ids, mut terminal_rx, child_state) in
            candidates
        {
            let current = terminal_rx.borrow_and_update().clone();
            let terminal = if matches!(current, SpineUiRouteTerminalState::Pending) {
                tokio::time::timeout_at(deadline, async {
                    loop {
                        if terminal_rx.changed().await.is_err() {
                            break SpineUiRouteTerminalState::Invalidated;
                        }
                        let state = terminal_rx.borrow_and_update().clone();
                        if !matches!(state, SpineUiRouteTerminalState::Pending) {
                            break state;
                        }
                    }
                })
                .await
                .ok()
            } else {
                Some(current)
            };

            match terminal {
                Some(SpineUiRouteTerminalState::Settled(state)) => {
                    states.push((child_thread_id, generation, state.map(|state| *state)));
                }
                Some(SpineUiRouteTerminalState::Invalidated) => {
                    states.push((child_thread_id, generation, None));
                }
                Some(SpineUiRouteTerminalState::TimedOut) => {
                    timed_out.push((child_thread_id, generation));
                    let latest = {
                        let child_state = child_state.lock().await;
                        child_state
                            .current_spine_ui_for_forward()
                            .map(|state| state.filtered_for_parent(&baseline_node_ids))
                    };
                    states.push((child_thread_id, generation, latest));
                }
                Some(SpineUiRouteTerminalState::Pending) => unreachable!(),
                None => {
                    let latest = {
                        let child_state = child_state.lock().await;
                        child_state
                            .current_spine_ui_for_forward()
                            .map(|state| state.filtered_for_parent(&baseline_node_ids))
                    };
                    match self
                        .mark_spine_ui_route_timed_out(
                            child_thread_id,
                            parent_thread_id,
                            parent_turn_id,
                            generation,
                        )
                        .await
                    {
                        SpineUiRouteTerminalState::Settled(state) => {
                            states.push((child_thread_id, generation, state.map(|state| *state)));
                        }
                        SpineUiRouteTerminalState::Invalidated => {
                            states.push((child_thread_id, generation, None));
                        }
                        SpineUiRouteTerminalState::TimedOut => {
                            timed_out.push((child_thread_id, generation));
                            states.push((child_thread_id, generation, latest));
                        }
                        SpineUiRouteTerminalState::Pending => unreachable!(),
                    }
                }
            }
        }
        (states, timed_out)
    }

    async fn mark_spine_ui_route_timed_out(
        &self,
        child_thread_id: ThreadId,
        parent_thread_id: ThreadId,
        parent_turn_id: &str,
        generation: u64,
    ) -> SpineUiRouteTerminalState {
        let mut state = self.state.lock().await;
        let Some(route) = state.spine_ui_parent_by_child.get_mut(&child_thread_id) else {
            return SpineUiRouteTerminalState::Invalidated;
        };
        if route.parent_thread_id != parent_thread_id
            || route.parent_turn_id != parent_turn_id
            || route.generation != generation
        {
            return SpineUiRouteTerminalState::Invalidated;
        }
        let terminal = route.terminal_tx.borrow().clone();
        if matches!(terminal, SpineUiRouteTerminalState::Pending) {
            route.timed_out_at = Some(Instant::now());
            route
                .terminal_tx
                .send_replace(SpineUiRouteTerminalState::TimedOut);
            SpineUiRouteTerminalState::TimedOut
        } else {
            terminal
        }
    }

    pub(crate) async fn complete_spine_ui_late_terminal(
        &self,
        child_thread_id: ThreadId,
        parent_thread_id: ThreadId,
        parent_turn_id: &str,
        generation: u64,
    ) {
        let mut state = self.state.lock().await;
        if let Some(route) = state.spine_ui_parent_by_child.get_mut(&child_thread_id)
            && route.parent_thread_id == parent_thread_id
            && route.parent_turn_id == parent_turn_id
            && route.generation == generation
            && matches!(
                *route.terminal_tx.borrow(),
                SpineUiRouteTerminalState::Settled(_)
            )
        {
            route.timed_out_at = None;
        }
    }

    pub(crate) async fn spine_ui_route_is_current(
        &self,
        child_thread_id: ThreadId,
        parent_thread_id: ThreadId,
        parent_turn_id: &str,
        generation: u64,
    ) -> bool {
        self.state
            .lock()
            .await
            .spine_ui_parent_by_child
            .get(&child_thread_id)
            .is_some_and(|route| {
                route.parent_thread_id == parent_thread_id
                    && route.parent_turn_id == parent_turn_id
                    && route.generation == generation
            })
    }

    pub(crate) async fn has_timed_out_spine_ui_children(&self, parent_thread_id: ThreadId) -> bool {
        let now = Instant::now();
        let mut state = self.state.lock().await;
        state.spine_ui_parent_by_child.retain(|_, route| {
            let expired = route.parent_thread_id == parent_thread_id
                && route.timed_out_at.is_some_and(|timed_out_at| {
                    now.saturating_duration_since(timed_out_at)
                        >= SPINE_UI_TIMED_OUT_ROUTE_RETENTION
                });
            if expired {
                route
                    .terminal_tx
                    .send_replace(SpineUiRouteTerminalState::Invalidated);
            }
            !expired
        });
        state
            .spine_ui_parent_by_child
            .values()
            .any(|route| route.parent_thread_id == parent_thread_id && route.timed_out_at.is_some())
    }

    pub(crate) async fn clear_spine_ui_parent_routes(
        &self,
        parent_thread_id: ThreadId,
        parent_turn_id: &str,
    ) {
        let parent_state = {
            let state = self.state.lock().await;
            state
                .threads
                .get(&parent_thread_id)
                .map(|entry| entry.state.clone())
        };
        let carried_generations = if let Some(parent_state) = parent_state {
            parent_state.lock().await.spine_ui_carry_agent_generations()
        } else {
            HashMap::new()
        };

        let mut state = self.state.lock().await;
        let removed_routes = state
            .spine_ui_parent_by_child
            .iter()
            .filter(|(_, route)| {
                route.parent_thread_id == parent_thread_id
                    && route.parent_turn_id == parent_turn_id
                    && route.timed_out_at.is_none()
            })
            .map(|(child_thread_id, route)| (*child_thread_id, route.clone()))
            .collect::<Vec<_>>();
        for (child_thread_id, route) in &removed_routes {
            route
                .terminal_tx
                .send_replace(SpineUiRouteTerminalState::Invalidated);
            state.spine_ui_parent_by_child.remove(child_thread_id);
        }

        let invalidated_generations = state.spine_ui_invalidated_generation_by_child.clone();
        state
            .spine_ui_carry_parent_by_child
            .retain(|child_thread_id, lineage| {
                lineage.parent_thread_id != parent_thread_id
                    || (carried_generations.get(child_thread_id) == Some(&lineage.generation)
                        && invalidated_generations
                            .get(child_thread_id)
                            .is_none_or(|invalidated| *invalidated < lineage.generation))
            });
        for (child_thread_id, route) in removed_routes {
            if carried_generations.get(&child_thread_id) != Some(&route.generation)
                || state
                    .spine_ui_invalidated_generation_by_child
                    .get(&child_thread_id)
                    .is_some_and(|invalidated| *invalidated >= route.generation)
            {
                continue;
            }
            state.spine_ui_carry_parent_by_child.insert(
                child_thread_id,
                SpineUiCarryLineage {
                    parent_thread_id,
                    generation: route.generation,
                    baseline_node_ids: route.baseline_node_ids,
                },
            );
        }
    }

    pub(crate) async fn propagate_spine_ui_completed_agent_state(
        &self,
        child_thread_id: ThreadId,
        child_state: SpineUiState,
    ) -> Vec<SpineUiCompletedUpdate> {
        let mut pending = self
            .spine_ui_parent_refresh_targets(child_thread_id)
            .await
            .into_iter()
            .map(|(parent_thread_id, generation, baseline_node_ids)| {
                (
                    child_thread_id,
                    child_state.clone(),
                    parent_thread_id,
                    generation,
                    baseline_node_ids,
                )
            })
            .collect::<VecDeque<_>>();
        let mut visited = HashSet::new();
        let mut completed_updates = Vec::new();
        while let Some((
            child_thread_id,
            child_state,
            parent_thread_id,
            generation,
            baseline_node_ids,
        )) = pending.pop_front()
        {
            if !visited.insert((child_thread_id, parent_thread_id, generation)) {
                continue;
            }
            let parent_state = {
                let state = self.state.lock().await;
                state
                    .threads
                    .get(&parent_thread_id)
                    .map(|entry| entry.state.clone())
            };
            let Some(parent_state) = parent_state else {
                continue;
            };
            let refresh = parent_state.lock().await.refresh_spine_ui_agent_state(
                child_thread_id,
                generation,
                child_state.filtered_for_parent(&baseline_node_ids),
            );
            if let Some((turn_id, state)) = refresh.active
                && let Some(tx) = self.current_listener_command_tx(parent_thread_id)
            {
                let _ = tx.send(ThreadListenerCommand::EmitSpineUiInvalidation { turn_id, state });
            }
            for (turn_id, state) in refresh.completed {
                completed_updates.push(SpineUiCompletedUpdate {
                    thread_id: parent_thread_id,
                    turn_id,
                    state,
                });
            }
            let Some(parent_state) = refresh.forward else {
                continue;
            };
            pending.extend(
                self.spine_ui_parent_refresh_targets(parent_thread_id)
                    .await
                    .into_iter()
                    .map(
                        |(ancestor_thread_id, ancestor_generation, baseline_node_ids)| {
                            (
                                parent_thread_id,
                                parent_state.clone(),
                                ancestor_thread_id,
                                ancestor_generation,
                                baseline_node_ids,
                            )
                        },
                    ),
            );
        }
        completed_updates
    }

    async fn spine_ui_parent_refresh_targets(
        &self,
        child_thread_id: ThreadId,
    ) -> Vec<(ThreadId, u64, HashSet<String>)> {
        let state = self.state.lock().await;
        let mut targets = Vec::new();
        if let Some(route) = state.spine_ui_parent_by_child.get(&child_thread_id) {
            targets.push((
                route.parent_thread_id,
                route.generation,
                route.baseline_node_ids.clone(),
            ));
        }
        if let Some(lineage) = state.spine_ui_carry_parent_by_child.get(&child_thread_id)
            && !targets.iter().any(|(parent_thread_id, generation, _)| {
                *parent_thread_id == lineage.parent_thread_id && *generation == lineage.generation
            })
        {
            targets.push((
                lineage.parent_thread_id,
                lineage.generation,
                lineage.baseline_node_ids.clone(),
            ));
        }
        targets
    }

    async fn take_spine_ui_parent_invalidation_targets(
        &self,
        child_thread_id: ThreadId,
    ) -> Vec<(ThreadId, u64)> {
        let mut state = self.state.lock().await;
        let live_route = state.spine_ui_parent_by_child.remove(&child_thread_id);
        if let Some(route) = &live_route {
            route
                .terminal_tx
                .send_replace(SpineUiRouteTerminalState::Invalidated);
        }
        let carry_lineage = state
            .spine_ui_carry_parent_by_child
            .remove(&child_thread_id);
        let targets = Self::spine_ui_invalidation_targets(live_route, carry_lineage);
        Self::record_spine_ui_invalidated_generation(&mut state, child_thread_id, &targets);
        targets
    }

    pub(crate) async fn clear_all_spine_ui_routes_for_thread(
        &self,
        thread_id: ThreadId,
    ) -> Vec<SpineUiCompletedUpdate> {
        let targets = {
            let mut state = self.state.lock().await;
            if let Some(entry) = state.threads.get_mut(&thread_id) {
                entry.spine_ui_terminal_ack = None;
            }
            let live_route = state.spine_ui_parent_by_child.remove(&thread_id);
            if let Some(route) = &live_route {
                route
                    .terminal_tx
                    .send_replace(SpineUiRouteTerminalState::Invalidated);
            }
            let carry_lineage = state.spine_ui_carry_parent_by_child.remove(&thread_id);
            state.spine_ui_parent_by_child.retain(|_, route| {
                let remove = route.parent_thread_id == thread_id;
                if remove {
                    route
                        .terminal_tx
                        .send_replace(SpineUiRouteTerminalState::Invalidated);
                }
                !remove
            });
            state
                .spine_ui_carry_parent_by_child
                .retain(|_, lineage| lineage.parent_thread_id != thread_id);
            let targets = Self::spine_ui_invalidation_targets(live_route, carry_lineage);
            Self::record_spine_ui_invalidated_generation(&mut state, thread_id, &targets);
            targets
        };
        self.invalidate_spine_ui_targets(thread_id, targets).await
    }

    fn record_spine_ui_invalidated_generation(
        state: &mut ThreadStateManagerInner,
        child_thread_id: ThreadId,
        targets: &[(ThreadId, u64)],
    ) {
        let Some(generation) = targets.iter().map(|(_, generation)| *generation).max() else {
            return;
        };
        state
            .spine_ui_invalidated_generation_by_child
            .entry(child_thread_id)
            .and_modify(|current| *current = (*current).max(generation))
            .or_insert(generation);
    }

    fn spine_ui_invalidation_targets(
        live_route: Option<SpineUiParentRoute>,
        carry_lineage: Option<SpineUiCarryLineage>,
    ) -> Vec<(ThreadId, u64)> {
        let mut targets = Vec::new();
        if let Some(route) = live_route {
            targets.push((route.parent_thread_id, route.generation));
        }
        if let Some(lineage) = carry_lineage {
            let target = (lineage.parent_thread_id, lineage.generation);
            if !targets.contains(&target) {
                targets.push(target);
            }
        }
        targets
    }

    async fn invalidate_spine_ui_targets(
        &self,
        child_thread_id: ThreadId,
        targets: Vec<(ThreadId, u64)>,
    ) -> Vec<SpineUiCompletedUpdate> {
        let mut pending = targets
            .into_iter()
            .map(|(parent_thread_id, generation)| (child_thread_id, parent_thread_id, generation))
            .collect::<VecDeque<_>>();
        let mut visited = HashSet::new();
        let mut completed_updates = Vec::new();
        while let Some((child_thread_id, parent_thread_id, generation)) = pending.pop_front() {
            if !visited.insert((child_thread_id, parent_thread_id, generation)) {
                continue;
            }
            let parent_state = {
                let state = self.state.lock().await;
                state
                    .threads
                    .get(&parent_thread_id)
                    .map(|entry| entry.state.clone())
            };
            let Some(parent_state) = parent_state else {
                continue;
            };
            let (changed, active_spine_ui, completed_spine_ui) = {
                let mut parent_state = parent_state.lock().await;
                let live_state_changed =
                    parent_state.invalidate_spine_ui_agent_state(child_thread_id, generation);
                let active_spine_ui = live_state_changed
                    .then(|| parent_state.active_spine_ui_snapshot())
                    .flatten();
                let mut completed_spine_ui = live_state_changed
                    .then(|| parent_state.completed_spine_ui_snapshot())
                    .flatten()
                    .into_iter()
                    .collect::<Vec<_>>();
                let restored_updates = parent_state
                    .invalidate_restored_spine_ui_agent_states(child_thread_id, generation);
                let changed = live_state_changed || !restored_updates.is_empty();
                completed_spine_ui.extend(restored_updates);
                (changed, active_spine_ui, completed_spine_ui)
            };
            if changed {
                if let Some((turn_id, state)) = active_spine_ui
                    && let Some(tx) = self.current_listener_command_tx(parent_thread_id)
                {
                    let _ =
                        tx.send(ThreadListenerCommand::EmitSpineUiInvalidation { turn_id, state });
                }
                for (turn_id, state) in completed_spine_ui {
                    completed_updates.push(SpineUiCompletedUpdate {
                        thread_id: parent_thread_id,
                        turn_id,
                        state,
                    });
                }
                let ancestor_targets = self
                    .take_spine_ui_parent_invalidation_targets(parent_thread_id)
                    .await;
                pending.extend(ancestor_targets.into_iter().map(
                    |(ancestor_thread_id, ancestor_generation)| {
                        (parent_thread_id, ancestor_thread_id, ancestor_generation)
                    },
                ));
            }
        }
        completed_updates
    }

    pub(crate) fn current_listener_command_tx(
        &self,
        thread_id: ThreadId,
    ) -> Option<mpsc::UnboundedSender<ThreadListenerCommand>> {
        self.listener_commands
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&thread_id)
            .cloned()
    }

    pub(crate) fn register_listener_command_tx(
        &self,
        thread_id: ThreadId,
        tx: mpsc::UnboundedSender<ThreadListenerCommand>,
    ) {
        self.listener_commands
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(thread_id, tx);
    }

    pub(crate) fn unregister_listener_command_tx(&self, thread_id: ThreadId) {
        self.listener_commands
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&thread_id);
    }

    pub(crate) async fn remove_thread_state(&self, thread_id: ThreadId) {
        let thread_state = {
            let mut state = self.state.lock().await;
            let thread_state = state
                .threads
                .remove(&thread_id)
                .map(|thread_entry| thread_entry.state);
            state.thread_ids_by_connection.retain(|_, thread_ids| {
                thread_ids.remove(&thread_id);
                !thread_ids.is_empty()
            });
            state
                .spine_ui_parent_by_child
                .retain(|child_thread_id, route| {
                    let remove =
                        *child_thread_id == thread_id || route.parent_thread_id == thread_id;
                    if remove {
                        route
                            .terminal_tx
                            .send_replace(SpineUiRouteTerminalState::Invalidated);
                    }
                    !remove
                });
            state
                .spine_ui_carry_parent_by_child
                .retain(|child_thread_id, lineage| {
                    *child_thread_id != thread_id && lineage.parent_thread_id != thread_id
                });
            state
                .spine_ui_invalidated_generation_by_child
                .remove(&thread_id);
            thread_state
        };
        self.unregister_listener_command_tx(thread_id);

        if let Some(thread_state) = thread_state {
            let mut thread_state = thread_state.lock().await;
            tracing::debug!(
                thread_id = %thread_id,
                listener_generation = thread_state.listener_generation,
                had_listener = thread_state.cancel_tx.is_some(),
                had_active_turn = thread_state.active_turn_snapshot().is_some(),
                "clearing thread listener during thread-state teardown"
            );
            thread_state.clear_listener();
        }
    }

    pub(crate) async fn clear_all_listeners(&self) {
        let thread_states = {
            let mut state = self.state.lock().await;
            for route in state.spine_ui_parent_by_child.values() {
                route
                    .terminal_tx
                    .send_replace(SpineUiRouteTerminalState::Invalidated);
            }
            state.spine_ui_parent_by_child.clear();
            state.spine_ui_carry_parent_by_child.clear();
            state.spine_ui_invalidated_generation_by_child.clear();
            state
                .threads
                .iter()
                .map(|(thread_id, thread_entry)| (*thread_id, thread_entry.state.clone()))
                .collect::<Vec<_>>()
        };

        for (thread_id, thread_state) in thread_states {
            self.unregister_listener_command_tx(thread_id);
            let mut thread_state = thread_state.lock().await;
            tracing::debug!(
                thread_id = %thread_id,
                listener_generation = thread_state.listener_generation,
                had_listener = thread_state.cancel_tx.is_some(),
                had_active_turn = thread_state.active_turn_snapshot().is_some(),
                "clearing thread listener during app-server shutdown"
            );
            thread_state.clear_listener();
        }
    }

    pub(crate) async fn unsubscribe_connection_from_thread(
        &self,
        thread_id: ThreadId,
        connection_id: ConnectionId,
    ) -> bool {
        {
            let mut state = self.state.lock().await;
            if !state.threads.contains_key(&thread_id) {
                return false;
            }

            if !state
                .thread_ids_by_connection
                .get(&connection_id)
                .is_some_and(|thread_ids| thread_ids.contains(&thread_id))
            {
                return false;
            }

            if let Some(thread_ids) = state.thread_ids_by_connection.get_mut(&connection_id) {
                thread_ids.remove(&thread_id);
                if thread_ids.is_empty() {
                    state.thread_ids_by_connection.remove(&connection_id);
                }
            }
            if let Some(thread_entry) = state.threads.get_mut(&thread_id) {
                thread_entry.connection_ids.remove(&connection_id);
                thread_entry.update_has_connections();
            }
        };

        true
    }

    #[cfg(test)]
    pub(crate) async fn has_subscribers(&self, thread_id: ThreadId) -> bool {
        self.state
            .lock()
            .await
            .threads
            .get(&thread_id)
            .is_some_and(|thread_entry| !thread_entry.connection_ids.is_empty())
    }

    pub(crate) async fn try_ensure_connection_subscribed(
        &self,
        thread_id: ThreadId,
        connection_id: ConnectionId,
        experimental_raw_events: bool,
    ) -> Option<Arc<Mutex<ThreadState>>> {
        let thread_state = {
            let mut state = self.state.lock().await;
            if !state.live_connections.contains_key(&connection_id) {
                return None;
            }
            state
                .thread_ids_by_connection
                .entry(connection_id)
                .or_default()
                .insert(thread_id);
            let thread_entry = state.threads.entry(thread_id).or_default();
            thread_entry.connection_ids.insert(connection_id);
            thread_entry.update_has_connections();
            thread_entry.state.clone()
        };
        {
            let mut thread_state_guard = thread_state.lock().await;
            if experimental_raw_events {
                thread_state_guard.set_experimental_raw_events(/*enabled*/ true);
            }
        }
        Some(thread_state)
    }

    pub(crate) async fn try_add_connection_to_thread(
        &self,
        thread_id: ThreadId,
        connection_id: ConnectionId,
    ) -> bool {
        let mut state = self.state.lock().await;
        if !state.live_connections.contains_key(&connection_id) {
            return false;
        }
        state
            .thread_ids_by_connection
            .entry(connection_id)
            .or_default()
            .insert(thread_id);
        let thread_entry = state.threads.entry(thread_id).or_default();
        thread_entry.connection_ids.insert(connection_id);
        thread_entry.update_has_connections();
        true
    }

    pub(crate) async fn remove_connection(&self, connection_id: ConnectionId) -> Vec<ThreadId> {
        {
            let mut state = self.state.lock().await;
            state.live_connections.remove(&connection_id);
            let thread_ids = state
                .thread_ids_by_connection
                .remove(&connection_id)
                .unwrap_or_default();
            for thread_id in &thread_ids {
                if let Some(thread_entry) = state.threads.get_mut(thread_id) {
                    thread_entry.connection_ids.remove(&connection_id);
                    thread_entry.update_has_connections();
                }
            }
            thread_ids
                .into_iter()
                .filter(|thread_id| {
                    state
                        .threads
                        .get(thread_id)
                        .is_some_and(|thread_entry| thread_entry.connection_ids.is_empty())
                })
                .collect::<Vec<_>>()
        }
    }

    pub(crate) async fn subscribe_to_has_connections(
        &self,
        thread_id: ThreadId,
    ) -> Option<watch::Receiver<bool>> {
        let state = self.state.lock().await;
        state
            .threads
            .get(&thread_id)
            .map(|thread_entry| thread_entry.has_connections_watcher.subscribe())
    }
}
