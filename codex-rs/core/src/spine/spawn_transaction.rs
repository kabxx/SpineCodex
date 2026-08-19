use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::user_input::UserInput;
use codex_spine_core::SpawnOutcome;
use codex_spine_core::SpawnReceipt;
use codex_spine_core::SpawnResult;
use codex_spine_core::SpawnTask;
use futures::future::join_all;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::agent::AgentStatus;
use crate::agent::control::SpawnAgentBatchRequest;
use crate::agent::control::SpawnAgentForkMode;
use crate::agent::control::SpawnAgentOptions;
use crate::agent::next_thread_spawn_depth;
use crate::session::session::Session;
use crate::session::turn_context::TurnContext;
use crate::spine::spawn_gate::SpawnFailureAction;
use crate::spine::spawn_gate::request_spawn_failure_action;
use crate::tools::handlers::multi_agents_common::build_agent_spawn_config;
use crate::tools::handlers::multi_agents_common::thread_spawn_source;

use super::CONTINUE_AFTER_FAILURE_MESSAGE;
use super::SpawnBatchCall;
use super::StartPhase;
use super::batch_progress_event;
use super::capacity_rejection_receipts;
use super::classify_start_results;
use super::correct_intermediate_messages;
use super::error_result;
use super::finish_batch_receipts;
use super::is_spawn_terminal;
use super::normalized_progress_status;
use super::quiesce_transaction_messages;
use super::result_from_status;
use super::result_status;
use super::should_show_failure_gate;
use super::task_envelope;
use super::teardown_transaction_children_with_correction;
use super::transaction_task_name;
use super::wait_for_terminal;
use super::wait_for_terminal_after_resume;

#[path = "spawn_attempt.rs"]
mod attempt;

use attempt::AttemptWait;
use attempt::append_failure_guidance;
use attempt::continue_failed_branches;
use attempt::wait_for_attempts;

async fn emit_all_batch_progress(
    session: &Session,
    turn: &TurnContext,
    calls: &[SpawnBatchCall],
    thread_ids: &tokio::sync::Mutex<Vec<ThreadId>>,
    paths: &[AgentPath],
    statuses: &tokio::sync::Mutex<Vec<AgentStatus>>,
) {
    let events = {
        let thread_ids = thread_ids.lock().await;
        let statuses = statuses.lock().await;
        (0..calls.len())
            .map(|call_ordinal| {
                batch_progress_event(calls, call_ordinal, &thread_ids, paths, &statuses)
            })
            .collect::<Vec<_>>()
    };
    for event in events {
        session.emit_spine_spawn_progress(turn, event).await;
    }
}

async fn sync_progress_results(
    progress_statuses: &tokio::sync::Mutex<Vec<AgentStatus>>,
    results: &[Option<SpawnResult>],
) {
    let mut statuses = progress_statuses.lock().await;
    for (ordinal, result) in results.iter().enumerate() {
        if let Some(result) = result {
            statuses[ordinal] = result_status(result);
        }
    }
}

pub(super) async fn execute_batch_transaction(
    session: Arc<Session>,
    turn: Arc<TurnContext>,
    cancellation_token: CancellationToken,
    calls: Vec<SpawnBatchCall>,
    show_failure_gate: bool,
) -> Result<HashMap<String, SpawnReceipt>, String> {
    let transaction_guard = session.spine_spawn_lifecycle.try_enter().ok_or_else(|| {
        "spine.spawn cannot start while another transaction is active or aborting".to_string()
    })?;
    let calls = calls.as_slice();
    if cancellation_token.is_cancelled() {
        return Err("spine.spawn was cancelled before child creation".to_string());
    }

    let config = build_agent_spawn_config(&session.get_base_instructions().await, turn.as_ref())
        .map_err(|error| error.to_string())?;
    let child_depth = next_thread_spawn_depth(&turn.session_source);
    let parent_path = turn
        .session_source
        .get_agent_path()
        .unwrap_or_else(AgentPath::root);
    let failure_gate_enabled = should_show_failure_gate(show_failure_gate, &parent_path);
    let task_count = calls.iter().map(|call| call.tasks.len()).sum();
    let mut child_paths = Vec::with_capacity(task_count);
    let mut requests = Vec::with_capacity(task_count);
    let mut flat_tasks = Vec::with_capacity(task_count);
    for (call_ordinal, call) in calls.iter().enumerate() {
        for (task_ordinal, task) in call.tasks.iter().enumerate() {
            let task_name = transaction_task_name(&call.call_id, task_ordinal);
            let source = thread_spawn_source(
                session.thread_id,
                &turn.session_source,
                child_depth,
                /*agent_role*/ None,
                Some(task_name),
            )
            .map_err(|error| error.to_string())?;
            let child_path = source
                .get_agent_path()
                .ok_or_else(|| "spine.spawn child is missing an agent path".to_string())?;
            child_paths.push(child_path);
            flat_tasks.push((call_ordinal, task_ordinal, task.clone()));
            // TODO(spine-spawn-context): Verify complete effective parent-context inheritance for
            // spawned children using native fork_turns="all". Compare the parent pre-spawn
            // effective context with each child's first model request, including inherited Spine
            // memory and first-turn cached_tokens, before strengthening this contract.
            requests.push(
                SpawnAgentBatchRequest::new(
                    source,
                    SpawnAgentOptions {
                        fork_parent_spawn_call_id: Some(call.fork_parent_call_id.clone()),
                        fork_mode: Some(SpawnAgentForkMode::FullHistoryTrimToolCallSuffix),
                        parent_thread_id: Some(session.thread_id),
                        environments: Some(turn.environments.to_selections()),
                    },
                )
                .suppress_parent_completion_notification(),
            );
        }
    }

    let prepared = match session
        .services
        .agent_control
        .prepare_agent_spawn_batch(config.clone(), requests)
        .await
    {
        Ok(prepared) => prepared,
        Err(CodexErr::AgentLimitReached { max_threads }) => {
            return capacity_rejection_receipts(calls, task_count, max_threads);
        }
        Err(error) => return Err(format!("spine.spawn admission failed: {error}")),
    };
    if cancellation_token.is_cancelled() {
        drop(prepared);
        return Err("spine.spawn was cancelled before child creation".to_string());
    }

    let starts =
        prepared
            .into_iter()
            .zip(flat_tasks.iter())
            .map(|(prepared, (call_ordinal, _, task))| {
                session
                    .services
                    .agent_control
                    .spawn_prepared_agent_with_metadata(
                        prepared,
                        vec![UserInput::Text {
                            text: task_envelope(task, &calls[*call_ordinal].tasks),
                            text_elements: Vec::new(),
                        }],
                    )
            });
    let start_results = join_all(starts)
        .await
        .into_iter()
        .map(|result| result.map(|agent| agent.thread_id));
    let StartPhase {
        live,
        mut results,
        failed: start_failed,
    } = classify_start_results(&child_paths, start_results);
    let mut child_by_path = live
        .iter()
        .map(|(_, thread_id, path)| (path.clone(), *thread_id))
        .collect::<HashMap<_, _>>();
    let mut current_thread_ids = vec![None; task_count];
    for (ordinal, thread_id, _) in &live {
        current_thread_ids[*ordinal] = Some(*thread_id);
    }
    let mailbox_cancellation = session
        .input_queue
        .mailbox_submission_cancellation(&child_paths);
    transaction_guard.install_mailbox_cancellation(mailbox_cancellation.clone());
    if cancellation_token.is_cancelled() {
        mailbox_cancellation.activate();
    }

    if start_failed {
        let child_thread_ids = live
            .iter()
            .map(|(_, thread_id, _)| *thread_id)
            .collect::<Vec<_>>();
        let mut corrected_ids = HashSet::new();
        let teardown_result = teardown_transaction_children_with_correction(
            &session,
            &parent_path,
            &child_thread_ids,
            &child_paths,
            &child_by_path,
            &mut corrected_ids,
        )
        .await;
        quiesce_transaction_messages(
            &session,
            &parent_path,
            &child_paths,
            &child_by_path,
            &mut corrected_ids,
        )
        .await;
        teardown_result?;
        for (ordinal, thread_id, _) in &live {
            let diagnostic =
                "child aborted because another transaction child failed to start".to_string();
            results[*ordinal] = Some(error_result(
                *ordinal,
                SpawnOutcome::Aborted,
                diagnostic,
                Some(thread_id.to_string()),
            ));
        }
        return finish_batch_receipts(calls, results);
    }

    let progress_calls = Arc::new(calls.to_vec());
    let progress_paths = Arc::new(child_paths.clone());
    let mut progress_thread_ids = vec![None; task_count];
    for (ordinal, thread_id, _) in &live {
        progress_thread_ids[*ordinal] = Some(*thread_id);
    }
    let progress_thread_ids = Arc::new(tokio::sync::Mutex::new(
        progress_thread_ids
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| "spine.spawn live child identity is incomplete".to_string())?,
    ));
    let initial_statuses = join_all(
        live.iter()
            .map(|(_, thread_id, _)| session.services.agent_control.get_status(*thread_id)),
    )
    .await;
    let mut progress_statuses = vec![AgentStatus::PendingInit; task_count];
    for ((ordinal, thread_id, _), status) in live.iter().zip(initial_statuses) {
        progress_statuses[*ordinal] = normalized_progress_status(*ordinal, *thread_id, status);
    }
    let progress_statuses = Arc::new(tokio::sync::Mutex::new(progress_statuses));
    emit_all_batch_progress(
        &session,
        turn.as_ref(),
        progress_calls.as_ref(),
        progress_thread_ids.as_ref(),
        progress_paths.as_ref(),
        progress_statuses.as_ref(),
    )
    .await;

    let mut corrected_ids = HashSet::new();
    let mut waits = live
        .iter()
        .map(|(ordinal, thread_id, _)| AttemptWait::initial(*ordinal, *thread_id))
        .collect::<Vec<_>>();
    let mut cancelled = false;
    let mut gate_cancelled = false;
    let mut gate_round = 1_u32;
    let mut fatal_error = None;

    'attempts: loop {
        let attempted = waits
            .iter()
            .map(|wait| (wait.ordinal, wait.thread_id))
            .collect::<Vec<_>>();
        let terminal = wait_for_attempts(
            &session,
            &turn,
            &cancellation_token,
            &parent_path,
            &child_paths,
            &child_by_path,
            &mut corrected_ids,
            &flat_tasks,
            &progress_calls,
            &progress_thread_ids,
            &progress_paths,
            &progress_statuses,
            waits,
        )
        .await;
        let Some(completed_results) = terminal else {
            cancelled = true;
            for (ordinal, thread_id) in attempted {
                let status = session.services.agent_control.get_status(thread_id).await;
                results[ordinal] = Some(if is_spawn_terminal(&status) {
                    result_from_status(ordinal, thread_id, status)
                } else {
                    error_result(
                        ordinal,
                        SpawnOutcome::Aborted,
                        "branch aborted because the originating spine.spawn transaction was cancelled"
                            .to_string(),
                        Some(thread_id.to_string()),
                    )
                });
            }
            break;
        };
        for (ordinal, result) in completed_results {
            results[ordinal] = Some(result);
        }

        let failed_ordinals = results
            .iter()
            .enumerate()
            .filter_map(|(ordinal, result)| {
                result
                    .as_ref()
                    .is_some_and(|result| result.outcome != SpawnOutcome::Completed)
                    .then_some(ordinal)
            })
            .collect::<Vec<_>>();
        if failed_ordinals.is_empty() || !failure_gate_enabled {
            break;
        }

        let gate_call_id = format!("{}:failure_gate:{gate_round}", calls[0].call_id);
        let decision = tokio::select! {
            decision = request_spawn_failure_action(
                &session,
                &turn,
                &gate_call_id,
                failed_ordinals.len(),
                task_count,
            ) => decision,
            _ = cancellation_token.cancelled() => {
                cancelled = true;
                None
            }
        };
        let Some(decision) = decision else {
            if !cancelled {
                gate_cancelled = true;
            }
            break;
        };
        gate_round = gate_round.saturating_add(1);

        let additional_guidance = decision.note.as_deref();
        match decision.action {
            SpawnFailureAction::Abandon => break,
            SpawnFailureAction::Continue => {
                waits = continue_failed_branches(
                    &session,
                    &turn,
                    &failed_ordinals,
                    &current_thread_ids,
                    &mut results,
                    &progress_calls,
                    &progress_thread_ids,
                    &progress_paths,
                    &progress_statuses,
                    additional_guidance,
                )
                .await;
            }
            SpawnFailureAction::Retry => {
                let retry_thread_ids = failed_ordinals
                    .iter()
                    .filter_map(|ordinal| current_thread_ids[*ordinal])
                    .collect::<Vec<_>>();
                let retry_paths = failed_ordinals
                    .iter()
                    .map(|ordinal| child_paths[*ordinal].clone())
                    .collect::<Vec<_>>();
                let teardown_result = teardown_transaction_children_with_correction(
                    &session,
                    &parent_path,
                    &retry_thread_ids,
                    &child_paths,
                    &child_by_path,
                    &mut corrected_ids,
                )
                .await;
                quiesce_transaction_messages(
                    &session,
                    &parent_path,
                    &child_paths,
                    &child_by_path,
                    &mut corrected_ids,
                )
                .await;
                if let Err(error) = teardown_result {
                    fatal_error = Some(error);
                    break 'attempts;
                }
                for ordinal in &failed_ordinals {
                    current_thread_ids[*ordinal] = None;
                    child_by_path.remove(&child_paths[*ordinal]);
                }
                if cancellation_token.is_cancelled() {
                    cancelled = true;
                    break;
                }

                let retry_requests = (|| -> Result<Vec<_>, String> {
                    let mut requests = Vec::with_capacity(failed_ordinals.len());
                    for ordinal in &failed_ordinals {
                        let (call_ordinal, task_ordinal, _) = &flat_tasks[*ordinal];
                        let call = &calls[*call_ordinal];
                        let source = thread_spawn_source(
                            session.thread_id,
                            &turn.session_source,
                            child_depth,
                            /*agent_role*/ None,
                            Some(transaction_task_name(&call.call_id, *task_ordinal)),
                        )
                        .map_err(|error| error.to_string())?;
                        let retry_path = source.get_agent_path().ok_or_else(|| {
                            "spine.spawn retry child is missing an agent path".to_string()
                        })?;
                        if retry_path != child_paths[*ordinal] {
                            return Err("spine.spawn retry changed a child agent path".to_string());
                        }
                        requests.push(
                            SpawnAgentBatchRequest::new(
                                source,
                                SpawnAgentOptions {
                                    fork_parent_spawn_call_id: Some(
                                        call.fork_parent_call_id.clone(),
                                    ),
                                    fork_mode: Some(
                                        SpawnAgentForkMode::FullHistoryTrimToolCallSuffix,
                                    ),
                                    parent_thread_id: Some(session.thread_id),
                                    environments: Some(turn.environments.to_selections()),
                                },
                            )
                            .suppress_parent_completion_notification(),
                        );
                    }
                    Ok(requests)
                })();
                let retry_requests = match retry_requests {
                    Ok(requests) => requests,
                    Err(error) => {
                        fatal_error = Some(error);
                        break 'attempts;
                    }
                };

                let prepared = match session
                    .services
                    .agent_control
                    .prepare_agent_spawn_batch(config.clone(), retry_requests)
                    .await
                {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        let diagnostic = format!("child retry admission failed: {error}");
                        for ordinal in &failed_ordinals {
                            results[*ordinal] = Some(error_result(
                                *ordinal,
                                SpawnOutcome::Errored,
                                diagnostic.clone(),
                                /*execution_ref*/ None,
                            ));
                        }
                        sync_progress_results(&progress_statuses, &results).await;
                        emit_all_batch_progress(
                            &session,
                            turn.as_ref(),
                            progress_calls.as_ref(),
                            progress_thread_ids.as_ref(),
                            progress_paths.as_ref(),
                            progress_statuses.as_ref(),
                        )
                        .await;
                        break;
                    }
                };
                if cancellation_token.is_cancelled() {
                    drop(prepared);
                    cancelled = true;
                    break;
                }

                let starts =
                    prepared
                        .into_iter()
                        .zip(failed_ordinals.iter())
                        .map(|(prepared, ordinal)| {
                            let (call_ordinal, _, task) = &flat_tasks[*ordinal];
                            session
                                .services
                                .agent_control
                                .spawn_prepared_agent_with_metadata(
                                    prepared,
                                    vec![UserInput::Text {
                                        text: append_failure_guidance(
                                            task_envelope(task, &calls[*call_ordinal].tasks),
                                            additional_guidance,
                                        ),
                                        text_elements: Vec::new(),
                                    }],
                                )
                        });
                let start_results = join_all(starts).await;
                let mut retry_live = Vec::with_capacity(failed_ordinals.len());
                let mut retry_start_failed = false;
                for ((ordinal, path), start_result) in failed_ordinals
                    .iter()
                    .copied()
                    .zip(retry_paths)
                    .zip(start_results)
                {
                    match start_result {
                        Ok(agent) => retry_live.push((ordinal, agent.thread_id, path)),
                        Err(error) => {
                            retry_start_failed = true;
                            results[ordinal] = Some(error_result(
                                ordinal,
                                SpawnOutcome::Errored,
                                format!("child retry failed to start: {error}"),
                                /*execution_ref*/ None,
                            ));
                        }
                    }
                }
                for (ordinal, thread_id, path) in &retry_live {
                    current_thread_ids[*ordinal] = Some(*thread_id);
                    child_by_path.insert(path.clone(), *thread_id);
                    progress_thread_ids.lock().await[*ordinal] = *thread_id;
                }

                if retry_start_failed {
                    let retry_live_ids = retry_live
                        .iter()
                        .map(|(_, thread_id, _)| *thread_id)
                        .collect::<Vec<_>>();
                    let teardown_result = teardown_transaction_children_with_correction(
                        &session,
                        &parent_path,
                        &retry_live_ids,
                        &child_paths,
                        &child_by_path,
                        &mut corrected_ids,
                    )
                    .await;
                    quiesce_transaction_messages(
                        &session,
                        &parent_path,
                        &child_paths,
                        &child_by_path,
                        &mut corrected_ids,
                    )
                    .await;
                    if let Err(error) = teardown_result {
                        fatal_error = Some(error);
                        break 'attempts;
                    }
                    for (ordinal, thread_id, path) in retry_live {
                        results[ordinal] = Some(error_result(
                            ordinal,
                            SpawnOutcome::Aborted,
                            "child retry aborted because another retry child failed to start"
                                .to_string(),
                            Some(thread_id.to_string()),
                        ));
                        current_thread_ids[ordinal] = None;
                        child_by_path.remove(&path);
                    }
                    sync_progress_results(&progress_statuses, &results).await;
                    emit_all_batch_progress(
                        &session,
                        turn.as_ref(),
                        progress_calls.as_ref(),
                        progress_thread_ids.as_ref(),
                        progress_paths.as_ref(),
                        progress_statuses.as_ref(),
                    )
                    .await;
                    break;
                }

                let retry_statuses = join_all(retry_live.iter().map(|(_, thread_id, _)| {
                    session.services.agent_control.get_status(*thread_id)
                }))
                .await;
                {
                    let mut statuses = progress_statuses.lock().await;
                    for ((ordinal, thread_id, _), status) in retry_live.iter().zip(retry_statuses) {
                        results[*ordinal] = None;
                        statuses[*ordinal] =
                            normalized_progress_status(*ordinal, *thread_id, status);
                    }
                }
                emit_all_batch_progress(
                    &session,
                    turn.as_ref(),
                    progress_calls.as_ref(),
                    progress_thread_ids.as_ref(),
                    progress_paths.as_ref(),
                    progress_statuses.as_ref(),
                )
                .await;
                waits = retry_live
                    .into_iter()
                    .map(|(ordinal, thread_id, _)| AttemptWait::initial(ordinal, thread_id))
                    .collect();
            }
        }
    }

    if cancelled {
        mailbox_cancellation.activate();
        for (ordinal, thread_id) in current_thread_ids.iter().enumerate() {
            if results[ordinal].is_some() {
                continue;
            }
            results[ordinal] = Some(error_result(
                ordinal,
                SpawnOutcome::Aborted,
                "branch aborted because the originating spine.spawn transaction was cancelled"
                    .to_string(),
                thread_id.map(|thread_id| thread_id.to_string()),
            ));
        }
        sync_progress_results(&progress_statuses, &results).await;
        emit_all_batch_progress(
            &session,
            turn.as_ref(),
            progress_calls.as_ref(),
            progress_thread_ids.as_ref(),
            progress_paths.as_ref(),
            progress_statuses.as_ref(),
        )
        .await;
    }

    let child_thread_ids = current_thread_ids
        .iter()
        .flatten()
        .copied()
        .collect::<Vec<_>>();
    let teardown_result = teardown_transaction_children_with_correction(
        &session,
        &parent_path,
        &child_thread_ids,
        &child_paths,
        &child_by_path,
        &mut corrected_ids,
    )
    .await;
    quiesce_transaction_messages(
        &session,
        &parent_path,
        &child_paths,
        &child_by_path,
        &mut corrected_ids,
    )
    .await;
    if let Some(error) = fatal_error {
        return match teardown_result {
            Ok(()) => Err(error),
            Err(cleanup_error) => Err(format!("{error}; cleanup failed: {cleanup_error}")),
        };
    }
    teardown_result?;

    if gate_cancelled {
        return Err("spine.spawn failure gate was cancelled without a selection".to_string());
    }
    finish_batch_receipts(calls, results)
}
