use crate::agent::AgentStatus;
use crate::agent_communication::AgentCommunicationContext;
use crate::agent_communication::AgentCommunicationKind;
use crate::session::MailboxSubmissionCancellation;
use crate::session::session::Session;
use crate::session::turn_context::TurnContext;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::SpineSpawnProgressEvent;
use codex_protocol::protocol::SpineSpawnTaskProgress;
use codex_spine_core::SPINE_SPAWN_RESULT_SCHEMA;
use codex_spine_core::SpawnOutcome;
use codex_spine_core::SpawnReceipt;
use codex_spine_core::SpawnResult;
use codex_spine_core::SpawnTask;
use serde::Deserialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Display;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

const CORRECTION_MESSAGE: &str = concat!(
    "This spawned execution branch remains active. Continue exactly the declared\n",
    "assignment and use its declared shared blackboard to collaborate with peer\n",
    "branches. When the assignment is complete or precisely bounded, return exactly\n",
    "one non-empty,\n",
    "tool-free assistant final response containing terminal memory. That response\n",
    "ends this branch execution."
);
const CONTINUE_AFTER_FAILURE_MESSAGE: &str = concat!(
    "Continue the same assignment from this branch's existing context. Preserve useful progress ",
    "from the failed turn, finish the remaining work, and return the required terminal memory."
);
pub(crate) const MIN_SPAWN_TASKS: usize = 2;

#[path = "spawn_transaction.rs"]
mod transaction;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SpawnArgs {
    tasks: Vec<SpawnTask>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SpawnBatchCall {
    pub(crate) call_id: String,
    pub(crate) fork_parent_call_id: String,
    pub(crate) tasks: Vec<SpawnTask>,
}

#[derive(Default)]
pub(crate) struct SpawnBatchCoordinator {
    completed: HashMap<String, Result<SpawnReceipt, String>>,
}

#[derive(Clone)]
pub(crate) struct SpawnLifecycle {
    shared: Arc<SpawnLifecycleShared>,
}

struct SpawnLifecycleShared {
    state: StdMutex<SpawnLifecycleState>,
}

#[derive(Default)]
struct SpawnLifecycleState {
    next_transaction_id: u64,
    active_transactions: HashMap<u64, Option<MailboxSubmissionCancellation>>,
    abort_barriers: usize,
}

pub(crate) struct SpawnTransactionGuard {
    shared: Arc<SpawnLifecycleShared>,
    transaction_id: u64,
}

pub(crate) struct SpawnAbortBarrier {
    shared: Arc<SpawnLifecycleShared>,
    had_active_transactions: bool,
}

impl Default for SpawnLifecycle {
    fn default() -> Self {
        Self {
            shared: Arc::new(SpawnLifecycleShared {
                state: StdMutex::new(SpawnLifecycleState::default()),
            }),
        }
    }
}

impl SpawnLifecycle {
    pub(crate) fn try_enter(&self) -> Option<SpawnTransactionGuard> {
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.abort_barriers > 0 || !state.active_transactions.is_empty() {
            return None;
        }
        let transaction_id = state.next_transaction_id;
        state.next_transaction_id = state.next_transaction_id.wrapping_add(1);
        state.active_transactions.insert(transaction_id, None);
        Some(SpawnTransactionGuard {
            shared: Arc::clone(&self.shared),
            transaction_id,
        })
    }

    pub(crate) fn begin_abort(&self) -> SpawnAbortBarrier {
        let (had_active_transactions, mailbox_cancellations) = {
            let mut state = self
                .shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.abort_barriers += 1;
            (
                !state.active_transactions.is_empty(),
                state
                    .active_transactions
                    .values()
                    .filter_map(Clone::clone)
                    .collect::<Vec<_>>(),
            )
        };
        for cancellation in mailbox_cancellations {
            cancellation.activate();
        }
        SpawnAbortBarrier {
            shared: Arc::clone(&self.shared),
            had_active_transactions,
        }
    }
}

impl SpawnAbortBarrier {
    pub(crate) fn had_active_transactions(&self) -> bool {
        self.had_active_transactions
    }
}

impl Drop for SpawnTransactionGuard {
    fn drop(&mut self) {
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active_transactions.remove(&self.transaction_id);
    }
}

impl SpawnTransactionGuard {
    fn install_mailbox_cancellation(&self, cancellation: MailboxSubmissionCancellation) {
        let should_activate = {
            let mut state = self
                .shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let should_activate = state.abort_barriers > 0;
            if let Some(slot) = state.active_transactions.get_mut(&self.transaction_id) {
                *slot = Some(cancellation.clone());
            }
            should_activate
        };
        if should_activate {
            cancellation.activate();
        }
    }
}

impl Drop for SpawnAbortBarrier {
    fn drop(&mut self) {
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.abort_barriers = state.abort_barriers.saturating_sub(1);
    }
}

pub(crate) fn parse_tasks(arguments: &str) -> Result<Vec<SpawnTask>, String> {
    let tasks = serde_json::from_str::<SpawnArgs>(arguments)
        .map_err(|error| format!("invalid spine.spawn arguments: {error}"))?
        .tasks;
    if tasks.len() < MIN_SPAWN_TASKS {
        return Err(format!(
            "spine.spawn requires at least {MIN_SPAWN_TASKS} tasks"
        ));
    }
    let mut summaries = HashSet::with_capacity(tasks.len());
    for (ordinal, task) in tasks.iter().enumerate() {
        let summary = task.summary.trim();
        if summary.is_empty() {
            return Err(format!(
                "spine.spawn task {ordinal} requires a non-empty summary"
            ));
        }
        if !summaries.insert(summary) {
            return Err(format!(
                "spine.spawn task {ordinal} has duplicate summary `{summary}`"
            ));
        }
        if task.prompt.trim().is_empty() {
            return Err(format!(
                "spine.spawn task {ordinal} requires a non-empty prompt"
            ));
        }
    }
    Ok(tasks)
}

pub(crate) fn encode_receipt(receipt: &SpawnReceipt) -> Result<String, serde_json::Error> {
    serde_json::to_string(receipt)
}

pub(crate) fn decode_receipt(body: &str) -> Result<SpawnReceipt, serde_json::Error> {
    serde_json::from_str(body)
}

pub(crate) fn calls_in_response_group(
    rollout: &[RolloutItem],
    call_id: &str,
) -> Result<Vec<SpawnBatchCall>, String> {
    let effective = super::effective_rollout(rollout);
    let mut index = 0;
    while index < effective.len() {
        let Some((group, consumed)) = super::completed_tool_group(&effective, index, true) else {
            index += 1;
            continue;
        };
        if group.calls.iter().any(|call| call.call_id == call_id) {
            if group.calls.iter().any(|call| {
                matches!(
                    call.name.as_str(),
                    "spine.open" | "spine.close" | "spine.next"
                )
            }) {
                return Err(
                    "spine.spawn cannot be mixed with spine.open, spine.close, or spine.next"
                        .to_string(),
                );
            }

            let spawn_calls = group
                .calls
                .iter()
                .filter(|call| call.name == "spine.spawn")
                .collect::<Vec<_>>();
            if spawn_calls.len() > 1 {
                return Err(
                    "spine.spawn may be called at most once in one model response".to_string(),
                );
            }

            let mut calls = Vec::new();
            for call in spawn_calls {
                match parse_tasks(&call.arguments) {
                    Ok(tasks) => calls.push(SpawnBatchCall {
                        call_id: call.call_id.clone(),
                        fork_parent_call_id: call.call_id.clone(),
                        tasks,
                    }),
                    Err(error) if call.call_id == call_id => return Err(error),
                    Err(_) => {}
                }
            }
            if calls.iter().any(|call| call.call_id == call_id) {
                return Ok(calls);
            }
            return Err(format!(
                "spine.spawn call `{call_id}` is missing valid tasks from its response group"
            ));
        }
        index += consumed;
    }
    Err(format!(
        "spine.spawn call `{call_id}` is missing from the current rollout"
    ))
}

pub(crate) async fn execute(
    session: Arc<Session>,
    turn: Arc<TurnContext>,
    call_id: String,
    cancellation_token: CancellationToken,
    tasks: Vec<SpawnTask>,
) -> Result<SpawnReceipt, String> {
    let mut coordinator = session.spine_spawn_batch_coordinator.lock().await;
    if let Some(result) = coordinator.completed.remove(&call_id) {
        return result;
    }

    let calls = session
        .spine_spawn_calls_in_response_group(&call_id)
        .await?;
    let Some(current) = calls.iter().find(|call| call.call_id == call_id) else {
        return Err(format!(
            "spine.spawn call `{call_id}` is missing from its response group"
        ));
    };
    if current.tasks != tasks {
        return Err(format!(
            "spine.spawn call `{call_id}` arguments changed during group admission"
        ));
    }

    let batch_result = execute_batch(
        Arc::clone(&session),
        turn,
        cancellation_token,
        &calls,
        /*show_failure_gate*/ true,
    )
    .await;
    match batch_result {
        Ok(receipts) => coordinator.completed.extend(
            receipts
                .into_iter()
                .map(|(call_id, receipt)| (call_id, Ok(receipt))),
        ),
        Err(error) => coordinator.completed.extend(
            calls
                .iter()
                .map(|call| (call.call_id.clone(), Err(error.clone()))),
        ),
    }
    coordinator.completed.remove(&call_id).unwrap_or_else(|| {
        Err(format!(
            "spine.spawn batch did not produce a result for call `{call_id}`"
        ))
    })
}

pub(crate) async fn execute_nested(
    session: Arc<Session>,
    turn: Arc<TurnContext>,
    outer_exec_call_id: String,
    invocation_ordinal: u64,
    cancellation_token: CancellationToken,
    tasks: Vec<SpawnTask>,
) -> Result<SpawnReceipt, String> {
    let call_id = format!("{outer_exec_call_id}:spine:{invocation_ordinal}");
    let calls = [SpawnBatchCall {
        call_id: call_id.clone(),
        fork_parent_call_id: outer_exec_call_id,
        tasks,
    }];
    execute_batch(
        session,
        turn,
        cancellation_token,
        &calls,
        /*show_failure_gate*/ false,
    )
    .await?
    .remove(&call_id)
    .ok_or_else(|| {
        format!("nested spine.spawn batch did not produce a result for call `{call_id}`")
    })
}

async fn execute_batch(
    session: Arc<Session>,
    turn: Arc<TurnContext>,
    cancellation_token: CancellationToken,
    calls: &[SpawnBatchCall],
    show_failure_gate: bool,
) -> Result<HashMap<String, SpawnReceipt>, String> {
    let calls = calls.to_vec();
    tokio::spawn(transaction::execute_batch_transaction(
        session,
        turn,
        cancellation_token,
        calls,
        show_failure_gate,
    ))
    .await
    .map_err(|error| format!("spine.spawn transaction task failed: {error}"))?
}

async fn teardown_transaction_children(
    session: &Session,
    transaction_root_thread_ids: &[ThreadId],
) -> Result<(), String> {
    let result = session
        .services
        .agent_control
        .shutdown_spine_spawn_subtrees(transaction_root_thread_ids)
        .await
        .map_err(|error| format!("spine.spawn child teardown failed: {error}"));
    // A continuation may be accepted after its Spine slot is committed but before the child
    // claims the execution guard. Releasing a claimed or already released slot is a no-op.
    for thread_id in transaction_root_thread_ids {
        session
            .services
            .agent_control
            .release_execution_reservation(*thread_id);
    }
    result
}

async fn teardown_transaction_children_with_correction(
    session: &Arc<Session>,
    parent_path: &AgentPath,
    transaction_root_thread_ids: &[ThreadId],
    transaction_roots: &[AgentPath],
    child_by_path: &HashMap<AgentPath, ThreadId>,
    corrected_ids: &mut HashSet<String>,
) -> Result<(), String> {
    let teardown = teardown_transaction_children(session.as_ref(), transaction_root_thread_ids);
    tokio::pin!(teardown);
    let mut interval = tokio::time::interval(Duration::from_millis(25));
    loop {
        tokio::select! {
            result = &mut teardown => break result,
            _ = interval.tick() => {
                correct_intermediate_messages(
                    session,
                    parent_path,
                    transaction_roots,
                    child_by_path,
                    corrected_ids,
                ).await;
            }
        }
    }
}

async fn quiesce_transaction_messages(
    session: &Arc<Session>,
    parent_path: &AgentPath,
    transaction_roots: &[AgentPath],
    child_by_path: &HashMap<AgentPath, ThreadId>,
    corrected_ids: &mut HashSet<String>,
) {
    let quiescence = session.input_queue.wait_for_mailbox_submissions(|author| {
        author_is_in_transaction_subtree(author, transaction_roots)
    });
    tokio::pin!(quiescence);
    let mut interval = tokio::time::interval(Duration::from_millis(25));
    loop {
        tokio::select! {
            _ = &mut quiescence => break,
            _ = interval.tick() => {
                correct_intermediate_messages(
                    session,
                    parent_path,
                    transaction_roots,
                    child_by_path,
                    corrected_ids,
                ).await;
            }
        }
    }
    correct_intermediate_messages(
        session,
        parent_path,
        transaction_roots,
        child_by_path,
        corrected_ids,
    )
    .await;
}

fn batch_progress_event(
    calls: &[SpawnBatchCall],
    call_ordinal: usize,
    thread_ids: &[ThreadId],
    paths: &[AgentPath],
    statuses: &[AgentStatus],
) -> SpineSpawnProgressEvent {
    let start = calls
        .iter()
        .take(call_ordinal)
        .map(|call| call.tasks.len())
        .sum::<usize>();
    let call = &calls[call_ordinal];
    let end = start + call.tasks.len();
    spawn_progress_event(
        &call.call_id,
        &call.tasks,
        &thread_ids[start..end],
        &paths[start..end],
        &statuses[start..end],
    )
}

fn finish_batch_receipts(
    calls: &[SpawnBatchCall],
    results: Vec<Option<SpawnResult>>,
) -> Result<HashMap<String, SpawnReceipt>, String> {
    let mut receipts = HashMap::with_capacity(calls.len());
    let mut results = results.into_iter();
    for call in calls {
        let call_results = (0..call.tasks.len())
            .map(|task_ordinal| {
                results.next().flatten().map(|mut result| {
                    result.ordinal = u32::try_from(task_ordinal).unwrap_or(u32::MAX);
                    result
                })
            })
            .collect();
        receipts.insert(
            call.call_id.clone(),
            finish_receipt(&call.tasks, call_results)?,
        );
    }
    debug_assert!(results.next().is_none());
    Ok(receipts)
}

fn capacity_rejection_receipts(
    calls: &[SpawnBatchCall],
    task_count: usize,
    max_threads: usize,
) -> Result<HashMap<String, SpawnReceipt>, String> {
    let mut receipts = HashMap::with_capacity(calls.len());
    let mut batch_ordinal = 0usize;
    for call in calls {
        let results = call
            .tasks
            .iter()
            .enumerate()
            .map(|(task_ordinal, task)| {
                batch_ordinal = batch_ordinal.saturating_add(1);
                let diagnostic = format!(
                    "spine.spawn task {batch_ordinal}/{task_count} (`{}`) was not started: \
                     aggregate admission requested {task_count} child agents, but shared capacity \
                     was unavailable under the configured limit of {max_threads} concurrent child \
                     agents (existing agents also consume this capacity). Admission is \
                     all-or-nothing, so no child agents from this batch were created. Retry \
                     spine.spawn with fewer tasks after capacity is available, or increase \
                     spine_spawn.max_concurrent_threads_per_session.",
                    task.summary
                );
                Some(error_result(
                    task_ordinal,
                    SpawnOutcome::Errored,
                    diagnostic,
                    /*execution_ref*/ None,
                ))
            })
            .collect();
        receipts.insert(call.call_id.clone(), finish_receipt(&call.tasks, results)?);
    }
    Ok(receipts)
}

fn spawn_progress_event(
    call_id: &str,
    tasks: &[SpawnTask],
    thread_ids: &[ThreadId],
    paths: &[AgentPath],
    statuses: &[AgentStatus],
) -> SpineSpawnProgressEvent {
    SpineSpawnProgressEvent {
        call_id: call_id.to_string(),
        tasks: tasks
            .iter()
            .zip(thread_ids)
            .zip(paths)
            .zip(statuses)
            .enumerate()
            .map(
                |(ordinal, (((task, thread_id), path), status))| SpineSpawnTaskProgress {
                    ordinal: ordinal as u32,
                    summary: task.summary.clone(),
                    thread_id: *thread_id,
                    agent_path: Some(path.clone()),
                    status: status.clone(),
                },
            )
            .collect(),
    }
}

fn result_status(result: &SpawnResult) -> AgentStatus {
    match result.outcome {
        SpawnOutcome::Completed => AgentStatus::Completed(None),
        SpawnOutcome::Errored => AgentStatus::Errored(
            result
                .diagnostic
                .clone()
                .unwrap_or_else(|| result.memory_body.clone()),
        ),
        SpawnOutcome::Aborted => AgentStatus::Shutdown,
    }
}

fn normalized_progress_status(
    ordinal: usize,
    thread_id: ThreadId,
    status: AgentStatus,
) -> AgentStatus {
    if is_spawn_terminal(&status) {
        result_status(&result_from_status(ordinal, thread_id, status))
    } else {
        status
    }
}

struct StartPhase {
    live: Vec<(usize, ThreadId, AgentPath)>,
    results: Vec<Option<SpawnResult>>,
    failed: bool,
}

fn classify_start_results<E: Display>(
    child_paths: &[AgentPath],
    start_results: impl IntoIterator<Item = Result<ThreadId, E>>,
) -> StartPhase {
    let mut live = Vec::with_capacity(child_paths.len());
    let mut results = vec![None; child_paths.len()];
    let mut failed = false;
    for (ordinal, start_result) in start_results.into_iter().enumerate() {
        match start_result {
            Ok(thread_id) => live.push((ordinal, thread_id, child_paths[ordinal].clone())),
            Err(error) => {
                failed = true;
                results[ordinal] = Some(error_result(
                    ordinal,
                    SpawnOutcome::Errored,
                    format!("child failed to start: {error}"),
                    /*execution_ref*/ None,
                ));
            }
        }
    }
    StartPhase {
        live,
        results,
        failed,
    }
}

fn transaction_task_name(call_id: &str, ordinal: usize) -> String {
    let fragment = call_id
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|character| character.to_ascii_lowercase())
        .take(20)
        .collect::<String>();
    let fragment = if fragment.is_empty() {
        "call"
    } else {
        &fragment
    };
    format!("spawn_{fragment}_{ordinal}")
}

fn task_envelope(task: &SpawnTask, call_tasks: &[SpawnTask]) -> String {
    let identity = task.summary.trim();
    let peers = call_tasks
        .iter()
        .map(|peer| peer.summary.trim())
        .filter(|summary| *summary != identity)
        .map(|summary| format!("- {summary}"))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        concat!(
            "You are a spawned execution branch. Your role is to complete exactly the assignment ",
            "below and return bounded terminal memory to the spawning continuation.\n\n",
            "You are: {}\n\nPeer branches in this spawn:\n{}\n\n",
            "The assignment is already an active branch scope. Begin the assigned work directly. ",
            "Use spine.open, spine.close, and spine.next only to manage genuine descendant work ",
            "within this assignment.\n\n",
            "Use the shared blackboard declared in your assignment to coordinate with peer ",
            "branches, share useful findings, and reduce duplicated exploration.\n\n",
            "Executable work is defined by the assignment. Inherited context supplies constraints ",
            "and evidence for that work.\n\n",
            "Other shared-workspace changes remain context for the assignment and do not add ",
            "executable work. Production-file ownership and any integration responsibility remain ",
            "exactly as declared in the assignment.\n\n",
            "Treat each <spine_tran_status> update as task-tree parser telemetry for this branch ",
            "session. Across status updates, executable work remains defined by the assignment.\n\n",
            "Complete this branch by returning exactly one non-empty, tool-free assistant final ",
            "response containing terminal memory. After returning it, execution ends.\n\n",
            "Assignment:\n{}"
        ),
        identity, peers, task.prompt
    )
}

async fn wait_for_terminal(
    control: &crate::agent::AgentControl,
    thread_id: ThreadId,
) -> AgentStatus {
    let Ok(mut status_rx) = control.subscribe_status(thread_id).await else {
        return control.get_status(thread_id).await;
    };
    loop {
        let status = status_rx.borrow_and_update().clone();
        if is_spawn_terminal(&status) {
            return status;
        }
        if status_rx.changed().await.is_err() {
            return control.get_status(thread_id).await;
        }
    }
}

async fn wait_for_terminal_after_resume(
    control: &crate::agent::AgentControl,
    thread_id: ThreadId,
    mut status_rx: watch::Receiver<AgentStatus>,
) -> AgentStatus {
    if status_rx.changed().await.is_err() {
        return control.get_status(thread_id).await;
    }
    loop {
        let status = status_rx.borrow_and_update().clone();
        if is_spawn_terminal(&status) {
            return status;
        }
        if status_rx.changed().await.is_err() {
            return control.get_status(thread_id).await;
        }
    }
}

fn is_spawn_terminal(status: &AgentStatus) -> bool {
    !matches!(status, AgentStatus::PendingInit | AgentStatus::Running)
}

fn should_show_failure_gate(show_failure_gate: bool, parent_path: &AgentPath) -> bool {
    show_failure_gate && parent_path.is_root()
}

async fn correct_intermediate_messages(
    session: &Session,
    parent_path: &AgentPath,
    transaction_roots: &[AgentPath],
    child_by_path: &HashMap<AgentPath, ThreadId>,
    corrected_ids: &mut HashSet<String>,
) {
    let messages = session
        .input_queue
        .extract_mailbox_communications(|mail| {
            author_is_in_transaction_subtree(&mail.author, transaction_roots)
        })
        .await;
    for message in messages {
        if message
            .id
            .as_ref()
            .is_some_and(|identity| !corrected_ids.insert(identity.clone()))
        {
            continue;
        }
        let Some(thread_id) = child_by_path.get(&message.author).copied().or_else(|| {
            session
                .services
                .agent_control
                .agent_id_for_path(&message.author)
        }) else {
            continue;
        };
        let correction = InterAgentCommunication::new(
            parent_path.clone(),
            message.author,
            Vec::new(),
            CORRECTION_MESSAGE.to_string(),
            /*trigger_turn*/ false,
        );
        let context =
            AgentCommunicationContext::new(AgentCommunicationKind::Message, session.thread_id);
        let _ = session
            .services
            .agent_control
            .send_inter_agent_communication(thread_id, correction, context)
            .await;
    }
}

fn author_is_in_transaction_subtree(author: &AgentPath, transaction_roots: &[AgentPath]) -> bool {
    transaction_roots
        .iter()
        .any(|root| path_is_in_subtree(author, root))
}

fn path_is_in_subtree(candidate: &AgentPath, root: &AgentPath) -> bool {
    candidate == root
        || candidate
            .as_str()
            .strip_prefix(root.as_str())
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn result_from_status(ordinal: usize, thread_id: ThreadId, status: AgentStatus) -> SpawnResult {
    match status {
        AgentStatus::Completed(Some(memory)) if !memory.trim().is_empty() => SpawnResult {
            ordinal: ordinal as u32,
            outcome: SpawnOutcome::Completed,
            memory_body: memory,
            diagnostic: None,
            execution_ref: Some(thread_id.to_string()),
        },
        AgentStatus::Completed(_) => error_result(
            ordinal,
            SpawnOutcome::Errored,
            "child completed without a non-empty final memory".to_string(),
            Some(thread_id.to_string()),
        ),
        AgentStatus::Errored(error) => error_result(
            ordinal,
            SpawnOutcome::Errored,
            format!("child errored: {error}"),
            Some(thread_id.to_string()),
        ),
        AgentStatus::Shutdown => error_result(
            ordinal,
            SpawnOutcome::Aborted,
            "child shut down before returning final memory".to_string(),
            Some(thread_id.to_string()),
        ),
        AgentStatus::NotFound => error_result(
            ordinal,
            SpawnOutcome::Errored,
            "child was not found before returning final memory".to_string(),
            Some(thread_id.to_string()),
        ),
        AgentStatus::PendingInit | AgentStatus::Running | AgentStatus::Interrupted => error_result(
            ordinal,
            SpawnOutcome::Aborted,
            format!("child did not reach a terminal status: {status:?}"),
            Some(thread_id.to_string()),
        ),
    }
}

fn error_result(
    ordinal: usize,
    outcome: SpawnOutcome,
    diagnostic: String,
    execution_ref: Option<String>,
) -> SpawnResult {
    SpawnResult {
        ordinal: ordinal as u32,
        outcome,
        memory_body: diagnostic.clone(),
        diagnostic: Some(diagnostic),
        execution_ref,
    }
}

fn finish_receipt(
    tasks: &[SpawnTask],
    results: Vec<Option<SpawnResult>>,
) -> Result<SpawnReceipt, String> {
    let receipt = SpawnReceipt {
        schema: SPINE_SPAWN_RESULT_SCHEMA.to_string(),
        results: results
            .into_iter()
            .enumerate()
            .map(|(ordinal, result)| {
                result.unwrap_or_else(|| {
                    error_result(
                        ordinal,
                        SpawnOutcome::Errored,
                        "coordinator lost the child terminal result".to_string(),
                        None,
                    )
                })
            })
            .collect(),
    };
    receipt
        .validate_for(tasks)
        .map_err(|error| format!("spine.spawn produced an invalid receipt: {error}"))?;
    Ok(receipt)
}

#[cfg(test)]
#[path = "spawn_tests.rs"]
mod tests;
