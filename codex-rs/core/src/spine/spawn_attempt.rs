use super::AgentPath;
use super::AgentStatus;
use super::Arc;
use super::CONTINUE_AFTER_FAILURE_MESSAGE;
use super::CancellationToken;
use super::Duration;
use super::HashMap;
use super::HashSet;
use super::Session;
use super::SpawnBatchCall;
use super::SpawnOutcome;
use super::SpawnResult;
use super::SpawnTask;
use super::ThreadId;
use super::TurnContext;
use super::UserInput;
use super::batch_progress_event;
use super::correct_intermediate_messages;
use super::emit_all_batch_progress;
use super::error_result;
use super::join_all;
use super::result_from_status;
use super::result_status;
use super::sync_progress_results;
use super::wait_for_terminal;
use super::wait_for_terminal_after_resume;
use super::watch;

pub(super) struct AttemptWait {
    pub(super) ordinal: usize,
    pub(super) thread_id: ThreadId,
    resume_status: Option<watch::Receiver<AgentStatus>>,
}

impl AttemptWait {
    pub(super) fn initial(ordinal: usize, thread_id: ThreadId) -> Self {
        Self {
            ordinal,
            thread_id,
            resume_status: None,
        }
    }

    fn resumed(
        ordinal: usize,
        thread_id: ThreadId,
        resume_status: watch::Receiver<AgentStatus>,
    ) -> Self {
        Self {
            ordinal,
            thread_id,
            resume_status: Some(resume_status),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn continue_failed_branches(
    session: &Arc<Session>,
    turn: &Arc<TurnContext>,
    failed_ordinals: &[usize],
    current_thread_ids: &[Option<ThreadId>],
    results: &mut [Option<SpawnResult>],
    progress_calls: &Arc<Vec<SpawnBatchCall>>,
    progress_thread_ids: &Arc<tokio::sync::Mutex<Vec<ThreadId>>>,
    progress_paths: &Arc<Vec<AgentPath>>,
    progress_statuses: &Arc<tokio::sync::Mutex<Vec<AgentStatus>>>,
    additional_guidance: Option<&str>,
) -> Vec<AttemptWait> {
    let control = &session.services.agent_control;
    let mut pending = Vec::with_capacity(failed_ordinals.len());
    for ordinal in failed_ordinals {
        let Some(thread_id) = current_thread_ids[*ordinal] else {
            results[*ordinal] = Some(error_result(
                *ordinal,
                SpawnOutcome::Errored,
                "child cannot continue because its thread is no longer available".to_string(),
                /*execution_ref*/ None,
            ));
            continue;
        };
        match control.subscribe_status(thread_id).await {
            Ok(mut status_rx) => {
                status_rx.borrow_and_update();
                pending.push((*ordinal, thread_id, status_rx));
            }
            Err(error) => {
                results[*ordinal] = Some(error_result(
                    *ordinal,
                    SpawnOutcome::Errored,
                    format!("child cannot continue: {error}"),
                    Some(thread_id.to_string()),
                ));
            }
        }
    }

    let reservations = match control.reserve_spine_spawn_slots(pending.len()) {
        Ok(reservations) => reservations,
        Err(error) => {
            let diagnostic = format!("child continuation admission failed: {error}");
            for (ordinal, thread_id, _) in &pending {
                results[*ordinal] = Some(error_result(
                    *ordinal,
                    SpawnOutcome::Errored,
                    diagnostic.clone(),
                    Some(thread_id.to_string()),
                ));
            }
            sync_progress_results(progress_statuses, results).await;
            emit_all_batch_progress(
                session,
                turn.as_ref(),
                progress_calls.as_ref(),
                progress_thread_ids.as_ref(),
                progress_paths.as_ref(),
                progress_statuses.as_ref(),
            )
            .await;
            return Vec::new();
        }
    };
    for (reservation, (_, thread_id, _)) in reservations.into_iter().zip(&pending) {
        reservation.commit(*thread_id);
    }

    let mut waits = Vec::with_capacity(pending.len());
    for (ordinal, thread_id, status_rx) in pending {
        let send_result = control
            .send_spine_spawn_continuation(
                thread_id,
                vec![UserInput::Text {
                    text: append_failure_guidance(
                        CONTINUE_AFTER_FAILURE_MESSAGE.to_string(),
                        additional_guidance,
                    ),
                    text_elements: Vec::new(),
                }],
            )
            .await;
        match send_result {
            Ok(_) => {
                results[ordinal] = None;
                waits.push(AttemptWait::resumed(ordinal, thread_id, status_rx));
            }
            Err(error) => {
                results[ordinal] = Some(error_result(
                    ordinal,
                    SpawnOutcome::Errored,
                    format!("child continuation failed to start: {error}"),
                    Some(thread_id.to_string()),
                ));
            }
        }
    }

    {
        let mut statuses = progress_statuses.lock().await;
        for ordinal in failed_ordinals {
            statuses[*ordinal] = results[*ordinal]
                .as_ref()
                .map_or(AgentStatus::Running, result_status);
        }
    }
    emit_all_batch_progress(
        session,
        turn.as_ref(),
        progress_calls.as_ref(),
        progress_thread_ids.as_ref(),
        progress_paths.as_ref(),
        progress_statuses.as_ref(),
    )
    .await;
    waits
}

pub(super) fn append_failure_guidance(
    mut message: String,
    additional_guidance: Option<&str>,
) -> String {
    if let Some(additional_guidance) = additional_guidance {
        message.push_str("\n\nAdditional user guidance:\n");
        message.push_str(additional_guidance);
    }
    message
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn wait_for_attempts(
    session: &Arc<Session>,
    turn: &Arc<TurnContext>,
    cancellation_token: &CancellationToken,
    parent_path: &AgentPath,
    child_paths: &[AgentPath],
    child_by_path: &HashMap<AgentPath, ThreadId>,
    corrected_ids: &mut HashSet<String>,
    flat_tasks: &[(usize, usize, SpawnTask)],
    progress_calls: &Arc<Vec<SpawnBatchCall>>,
    progress_thread_ids: &Arc<tokio::sync::Mutex<Vec<ThreadId>>>,
    progress_paths: &Arc<Vec<AgentPath>>,
    progress_statuses: &Arc<tokio::sync::Mutex<Vec<AgentStatus>>>,
    waits: Vec<AttemptWait>,
) -> Option<Vec<(usize, SpawnResult)>> {
    let waits = waits.into_iter().map(|wait| {
        let control = session.services.agent_control.clone();
        let session = Arc::clone(session);
        let turn = Arc::clone(turn);
        let progress_calls = Arc::clone(progress_calls);
        let progress_thread_ids = Arc::clone(progress_thread_ids);
        let progress_paths = Arc::clone(progress_paths);
        let progress_statuses = Arc::clone(progress_statuses);
        let call_ordinal = flat_tasks[wait.ordinal].0;
        async move {
            let mut status = match wait.resume_status {
                Some(status_rx) => {
                    wait_for_terminal_after_resume(&control, wait.thread_id, status_rx).await
                }
                None => wait_for_terminal(&control, wait.thread_id).await,
            };
            if control
                .wait_for_spine_spawn_turn_idle(wait.thread_id)
                .await
                .is_ok()
            {
                status = control.get_status(wait.thread_id).await;
            }
            let result = result_from_status(wait.ordinal, wait.thread_id, status);
            let event = {
                let thread_ids = progress_thread_ids.lock().await;
                let mut statuses = progress_statuses.lock().await;
                statuses[wait.ordinal] = result_status(&result);
                batch_progress_event(
                    progress_calls.as_ref(),
                    call_ordinal,
                    &thread_ids,
                    progress_paths.as_ref(),
                    &statuses,
                )
            };
            session
                .emit_spine_spawn_progress(turn.as_ref(), event)
                .await;
            (wait.ordinal, result)
        }
    });
    let wait_all = join_all(waits);
    tokio::pin!(wait_all);
    let mut interval = tokio::time::interval(Duration::from_millis(25));
    let terminal = loop {
        tokio::select! {
            statuses = &mut wait_all => break Some(statuses),
            _ = cancellation_token.cancelled() => break None,
            _ = interval.tick() => {
                correct_intermediate_messages(
                    session,
                    parent_path,
                    child_paths,
                    child_by_path,
                    corrected_ids,
                ).await;
            }
        }
    };
    correct_intermediate_messages(
        session,
        parent_path,
        child_paths,
        child_by_path,
        corrected_ids,
    )
    .await;
    terminal
}
