use codex_app_server_protocol::ItemCompletedNotification;
use codex_app_server_protocol::ItemStartedNotification;
use codex_app_server_protocol::McpAuthStatus;
use codex_app_server_protocol::McpResourceContent;
use codex_app_server_protocol::McpResourceReadResponse;
use codex_app_server_protocol::McpServerStatus;
use codex_app_server_protocol::McpServerToolCallResponse;
use codex_app_server_protocol::McpToolCallStatus;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::Turn;
use codex_protocol::ThreadId;
use codex_protocol::items::McpToolCallItem;
use codex_protocol::items::McpToolCallStatus as CoreMcpToolCallStatus;
use codex_protocol::items::TurnItem as CoreTurnItem;
use codex_protocol::mcp::CallToolResult;
use codex_protocol::mcp::McpServerInfo;
use codex_protocol::mcp::Resource;
use codex_protocol::mcp::Tool;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::SpineSpawnOutcome;
use codex_protocol::protocol::SpineSpawnProgressEvent;
use codex_protocol::protocol::SpineSpawnTaskProgress;
use codex_protocol::protocol::SpineTreeUpdateEvent;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use std::collections::HashMap;
use std::collections::HashSet;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

pub(crate) const ENABLE_ENV: &str = "CODEX_SPINE_APP_UI";
pub(crate) const SERVER_NAME: &str = "__codex_internal_spine_tree_ui__";
pub(crate) const TOOL_NAME: &str = "spine_tree";
pub(crate) const RESOURCE_URI: &str = "ui://spine/tree.html";
pub(crate) const AGENT_SYNC_TIMEOUT_MESSAGE: &str =
    "Final status synchronization timed out after 10 seconds";

const ITEM_ID_PREFIX: &str = "spine-ui-";
const RESOURCE_MIME_TYPE: &str = "text/html;profile=mcp-app";
const RESOURCE_HTML: &str = include_str!("spine_ui/tree.html");

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct SpineUiSpawnTask {
    #[serde(flatten)]
    progress: SpineSpawnTaskProgress,
    result_node_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct SpineUiSpawnCall {
    call_id: String,
    parent_node_id: Option<String>,
    tasks: Vec<SpineUiSpawnTask>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct SpineUiAgentSubtree {
    thread_id: ThreadId,
    #[serde(flatten)]
    content: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct SpineUiAgentGeneration {
    thread_id: ThreadId,
    generation: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistedSpineUiState {
    schema_version: u8,
    ui_revision: u64,
    snapshot: SpineTreeUpdateEvent,
    spawn_calls: Vec<SpineUiSpawnCall>,
    agent_subtrees: Vec<SpineUiAgentSubtree>,
    agent_generations: Vec<SpineUiAgentGeneration>,
    invalidated_agent_generations: Vec<SpineUiAgentGeneration>,
    agent_sync_timeout_generations: Vec<SpineUiAgentGeneration>,
}

#[derive(Clone, Debug)]
struct SpineUiAgentState {
    generation: u64,
    state: SpineUiState,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SpineUiState {
    revision: u64,
    snapshot: Option<SpineTreeUpdateEvent>,
    spawn_calls: Vec<SpineUiSpawnCall>,
    settled_spawn_call_ids: HashSet<String>,
    agent_subtrees: HashMap<ThreadId, SpineUiAgentState>,
    invalidated_agent_generations: HashMap<ThreadId, u64>,
    agent_sync_timeout_generations: HashMap<ThreadId, u64>,
}

impl SpineUiState {
    fn from_structured_content(content: serde_json::Value) -> Option<Self> {
        let persisted: PersistedSpineUiState = serde_json::from_value(content).ok()?;
        if persisted.schema_version != 1 {
            return None;
        }
        let generations = persisted
            .agent_generations
            .into_iter()
            .map(|entry| (entry.thread_id, entry.generation))
            .collect::<HashMap<_, _>>();
        let mut agent_subtrees = HashMap::new();
        for subtree in persisted.agent_subtrees {
            let generation = *generations.get(&subtree.thread_id)?;
            let state = Self::from_structured_content(subtree.content)?;
            if agent_subtrees
                .insert(subtree.thread_id, SpineUiAgentState { generation, state })
                .is_some()
            {
                return None;
            }
        }
        if agent_subtrees.len() != generations.len() {
            return None;
        }
        let invalidated_agent_generations = persisted
            .invalidated_agent_generations
            .into_iter()
            .map(|entry| (entry.thread_id, entry.generation))
            .collect();
        let agent_sync_timeout_generations = persisted
            .agent_sync_timeout_generations
            .into_iter()
            .map(|entry| (entry.thread_id, entry.generation))
            .collect();
        let settled_spawn_call_ids = persisted
            .snapshot
            .settled_spawn_call_ids
            .iter()
            .cloned()
            .collect();
        Some(Self {
            revision: persisted.ui_revision,
            snapshot: Some(persisted.snapshot),
            spawn_calls: persisted.spawn_calls,
            settled_spawn_call_ids,
            agent_subtrees,
            invalidated_agent_generations,
            agent_sync_timeout_generations,
        })
    }

    pub(crate) fn record_snapshot(&mut self, snapshot: SpineTreeUpdateEvent) -> bool {
        if let Some(current) = self.snapshot.as_ref()
            && (snapshot.snapshot_seq < current.snapshot_seq
                || (snapshot.snapshot_seq == current.snapshot_seq && snapshot == *current))
        {
            return false;
        }
        for call in &mut self.spawn_calls {
            if call.parent_node_id.is_none() {
                call.parent_node_id = Some(snapshot.active_node_id.clone());
            }
        }
        let visible_node_ids = snapshot
            .nodes
            .iter()
            .map(|node| node.node_id.as_str())
            .collect::<HashSet<_>>();
        self.spawn_calls.retain(|call| {
            call.parent_node_id
                .as_deref()
                .is_none_or(|parent_node_id| visible_node_ids.contains(parent_node_id))
        });
        self.settled_spawn_call_ids
            .extend(snapshot.settled_spawn_call_ids.iter().cloned());
        self.reconcile_spawn_result_nodes(&snapshot);
        let visible_agent_thread_ids = self
            .spawn_calls
            .iter()
            .flat_map(|call| call.tasks.iter().map(|task| task.progress.thread_id))
            .collect::<HashSet<_>>();
        self.agent_subtrees
            .retain(|thread_id, _| visible_agent_thread_ids.contains(thread_id));
        self.invalidated_agent_generations
            .retain(|thread_id, _| visible_agent_thread_ids.contains(thread_id));
        self.agent_sync_timeout_generations
            .retain(|thread_id, _| visible_agent_thread_ids.contains(thread_id));
        self.settled_spawn_call_ids
            .retain(|call_id| self.spawn_calls.iter().any(|call| &call.call_id == call_id));
        self.snapshot = Some(snapshot);
        self.bump_revision();
        true
    }

    pub(crate) fn record_spawn_progress(&mut self, progress: SpineSpawnProgressEvent) -> bool {
        if self.settled_spawn_call_ids.contains(&progress.call_id) {
            return false;
        }
        let parent_node_id = self
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.active_node_id.clone());
        if let Some(existing) = self
            .spawn_calls
            .iter_mut()
            .find(|call| call.call_id == progress.call_id)
        {
            let result_node_ids = existing
                .tasks
                .iter()
                .map(|task| (task.progress.ordinal, task.result_node_id.clone()))
                .collect::<HashMap<_, _>>();
            let previous = existing.tasks.clone();
            existing.tasks = progress
                .tasks
                .into_iter()
                .map(|mut progress| {
                    if let Some(old) = previous
                        .iter()
                        .find(|task| task.progress.ordinal == progress.ordinal)
                        && (status_rank(&old.progress.status) >= 2
                            || status_rank(&old.progress.status) > status_rank(&progress.status))
                    {
                        progress.status = old.progress.status.clone();
                    }
                    SpineUiSpawnTask {
                        result_node_id: result_node_ids.get(&progress.ordinal).cloned().flatten(),
                        progress,
                    }
                })
                .collect();
            if existing.parent_node_id.is_none() {
                existing.parent_node_id = parent_node_id;
            }
        } else {
            self.spawn_calls.push(SpineUiSpawnCall {
                call_id: progress.call_id,
                parent_node_id,
                tasks: progress
                    .tasks
                    .into_iter()
                    .map(|progress| SpineUiSpawnTask {
                        progress,
                        result_node_id: None,
                    })
                    .collect(),
            });
        }
        self.bump_revision();
        true
    }

    pub(crate) fn record_agent_state(
        &mut self,
        thread_id: ThreadId,
        generation: u64,
        state: SpineUiState,
    ) -> bool {
        let is_known_agent = self.spawn_calls.iter().any(|call| {
            call.tasks
                .iter()
                .any(|task| task.progress.thread_id == thread_id)
        });
        if !is_known_agent
            || self
                .invalidated_agent_generations
                .get(&thread_id)
                .is_some_and(|invalidated| generation <= *invalidated)
            || self.agent_subtrees.get(&thread_id).is_some_and(|current| {
                generation < current.generation
                    || (generation == current.generation
                        && state.revision <= current.state.revision)
            })
        {
            return false;
        }
        for call in &mut self.spawn_calls {
            if let Some(task) = call
                .tasks
                .iter_mut()
                .find(|task| task.progress.thread_id == thread_id)
                && matches!(
                    task.progress.status,
                    codex_protocol::protocol::AgentStatus::PendingInit
                )
            {
                task.progress.status = codex_protocol::protocol::AgentStatus::Running;
            }
        }
        self.agent_subtrees
            .insert(thread_id, SpineUiAgentState { generation, state });
        if self
            .agent_sync_timeout_generations
            .get(&thread_id)
            .is_some_and(|timed_out| generation > *timed_out)
        {
            self.agent_sync_timeout_generations.remove(&thread_id);
        }
        self.bump_revision();
        true
    }

    pub(crate) fn mark_agent_sync_timeout(&mut self, thread_id: ThreadId, generation: u64) -> bool {
        let is_known_agent = self.spawn_calls.iter().any(|call| {
            call.tasks
                .iter()
                .any(|task| task.progress.thread_id == thread_id)
        });
        if !is_known_agent
            || self
                .invalidated_agent_generations
                .get(&thread_id)
                .is_some_and(|invalidated| generation <= *invalidated)
            || self
                .agent_subtrees
                .get(&thread_id)
                .is_some_and(|current| generation < current.generation)
            || self.agent_sync_timeout_generations.get(&thread_id) == Some(&generation)
        {
            return false;
        }
        self.agent_sync_timeout_generations
            .insert(thread_id, generation);
        self.bump_revision();
        true
    }

    pub(crate) fn clear_agent_sync_timeout(
        &mut self,
        thread_id: ThreadId,
        generation: u64,
    ) -> bool {
        if self.agent_sync_timeout_generations.get(&thread_id) != Some(&generation) {
            return false;
        }
        self.agent_sync_timeout_generations.remove(&thread_id);
        self.bump_revision();
        true
    }

    pub(crate) fn remove_agent_state(&mut self, thread_id: ThreadId, generation: u64) -> bool {
        self.invalidated_agent_generations
            .entry(thread_id)
            .and_modify(|invalidated| *invalidated = (*invalidated).max(generation))
            .or_insert(generation);
        let should_remove_subtree = self
            .agent_subtrees
            .get(&thread_id)
            .is_some_and(|current| current.generation <= generation);
        let should_remove_timeout = self
            .agent_sync_timeout_generations
            .get(&thread_id)
            .is_some_and(|current| *current <= generation);
        if !should_remove_subtree && !should_remove_timeout {
            return false;
        }
        if should_remove_subtree {
            self.agent_subtrees.remove(&thread_id);
        }
        if should_remove_timeout {
            self.agent_sync_timeout_generations.remove(&thread_id);
        }
        self.bump_revision();
        true
    }

    pub(crate) fn carry_forward(&self) -> Self {
        Self {
            revision: self.revision,
            snapshot: None,
            spawn_calls: self.spawn_calls.clone(),
            settled_spawn_call_ids: self.settled_spawn_call_ids.clone(),
            agent_subtrees: self.agent_subtrees.clone(),
            invalidated_agent_generations: self.invalidated_agent_generations.clone(),
            agent_sync_timeout_generations: self.agent_sync_timeout_generations.clone(),
        }
    }

    pub(crate) fn latest_snapshot(&self) -> Option<&SpineTreeUpdateEvent> {
        self.snapshot.as_ref()
    }

    pub(crate) fn revision(&self) -> u64 {
        self.revision
    }

    pub(crate) fn set_revision(&mut self, revision: u64) {
        self.revision = revision;
    }

    #[cfg(test)]
    pub(crate) fn agent_generations(&self) -> HashMap<ThreadId, u64> {
        self.agent_subtrees
            .iter()
            .map(|(thread_id, state)| (*thread_id, state.generation))
            .collect()
    }

    pub(crate) fn tracked_agent_generations(&self) -> HashMap<ThreadId, u64> {
        let mut generations = self
            .agent_subtrees
            .iter()
            .filter_map(|(thread_id, state)| {
                let is_invalidated = self
                    .invalidated_agent_generations
                    .get(thread_id)
                    .is_some_and(|invalidated| state.generation <= *invalidated);
                (!is_invalidated).then_some((*thread_id, state.generation))
            })
            .collect::<HashMap<_, _>>();
        for (thread_id, generation) in &self.agent_sync_timeout_generations {
            if self
                .invalidated_agent_generations
                .get(thread_id)
                .is_some_and(|invalidated| generation <= invalidated)
            {
                continue;
            }
            generations
                .entry(*thread_id)
                .and_modify(|current| *current = (*current).max(*generation))
                .or_insert(*generation);
        }
        generations
    }

    pub(crate) fn tracks_agent_generation(&self, thread_id: ThreadId, generation: u64) -> bool {
        self.agent_subtrees
            .get(&thread_id)
            .is_some_and(|state| state.generation == generation)
    }

    pub(crate) fn tracks_agent_or_timeout_generation(
        &self,
        thread_id: ThreadId,
        generation: u64,
    ) -> bool {
        self.tracks_agent_generation(thread_id, generation)
            || self.agent_sync_timeout_generations.get(&thread_id) == Some(&generation)
    }

    pub(crate) fn has_agent_sync_timeouts(&self) -> bool {
        !self.agent_sync_timeout_generations.is_empty()
    }

    pub(crate) fn filtered_for_parent(&self, baseline_node_ids: &HashSet<String>) -> Self {
        let mut filtered = self.clone();
        if let Some(snapshot) = filtered.snapshot.as_mut() {
            snapshot
                .nodes
                .retain(|node| !baseline_node_ids.contains(&node.node_id));
            if !snapshot
                .nodes
                .iter()
                .any(|node| node.node_id == snapshot.active_node_id)
                && let Some(node) = snapshot.nodes.last()
            {
                snapshot.active_node_id = node.node_id.clone();
            }
        }
        for call in &mut filtered.spawn_calls {
            if call
                .parent_node_id
                .as_ref()
                .is_some_and(|node_id| baseline_node_ids.contains(node_id))
            {
                call.parent_node_id = None;
            }
        }
        filtered
    }

    pub(crate) fn structured_content(&self) -> Option<serde_json::Value> {
        let snapshot = self.snapshot.as_ref()?;
        let suppressed_node_ids = self
            .spawn_calls
            .iter()
            .flat_map(|call| call.tasks.iter())
            .filter_map(|task| task.result_node_id.as_deref())
            .collect::<Vec<_>>();
        let mut seen_agent_threads = HashSet::new();
        let agent_subtrees = self
            .spawn_calls
            .iter()
            .flat_map(|call| call.tasks.iter())
            .filter_map(|task| {
                let thread_id = task.progress.thread_id;
                let state = &self.agent_subtrees.get(&thread_id)?.state;
                let content = state.structured_content()?;
                seen_agent_threads
                    .insert(thread_id)
                    .then_some(SpineUiAgentSubtree { thread_id, content })
            })
            .collect::<Vec<_>>();
        let mut spawn_calls = self.spawn_calls.clone();
        for call in &mut spawn_calls {
            for task in &mut call.tasks {
                if self
                    .agent_sync_timeout_generations
                    .contains_key(&task.progress.thread_id)
                {
                    task.progress.status = codex_protocol::protocol::AgentStatus::Errored(
                        AGENT_SYNC_TIMEOUT_MESSAGE.to_string(),
                    );
                }
            }
        }
        let agent_generations = generation_entries(
            self.agent_subtrees
                .iter()
                .map(|(thread_id, state)| (*thread_id, state.generation)),
        );
        let invalidated_agent_generations = generation_entries(
            self.invalidated_agent_generations
                .iter()
                .map(|(thread_id, generation)| (*thread_id, *generation)),
        );
        let agent_sync_timeout_generations = generation_entries(
            self.agent_sync_timeout_generations
                .iter()
                .map(|(thread_id, generation)| (*thread_id, *generation)),
        );
        Some(json!({
            "schemaVersion": 1,
            "uiRevision": self.revision,
            "snapshot": snapshot,
            "spawnCalls": spawn_calls,
            "agentSubtrees": agent_subtrees,
            "agentGenerations": agent_generations,
            "invalidatedAgentGenerations": invalidated_agent_generations,
            "agentSyncTimeoutGenerations": agent_sync_timeout_generations,
            "suppressedNodeIds": suppressed_node_ids,
        }))
    }

    fn reconcile_spawn_result_nodes(&mut self, snapshot: &SpineTreeUpdateEvent) {
        let mut claimed_node_ids = self
            .spawn_calls
            .iter()
            .flat_map(|call| call.tasks.iter())
            .filter_map(|task| task.result_node_id.clone())
            .collect::<HashSet<_>>();

        for call in &mut self.spawn_calls {
            if !self.settled_spawn_call_ids.contains(&call.call_id) {
                continue;
            }
            for task in &mut call.tasks {
                if task.result_node_id.is_some() {
                    continue;
                }
                let Some(node) = snapshot.nodes.iter().find(|node| {
                    node.spawn_outcome.is_some()
                        && node.parent_id == call.parent_node_id
                        && node.summary.as_deref() == Some(task.progress.summary.as_str())
                        && !claimed_node_ids.contains(&node.node_id)
                }) else {
                    continue;
                };
                task.progress.status = match node.spawn_outcome {
                    Some(SpineSpawnOutcome::Completed) => {
                        codex_protocol::protocol::AgentStatus::Completed(None)
                    }
                    Some(SpineSpawnOutcome::Errored) => {
                        codex_protocol::protocol::AgentStatus::Errored(
                            node.memory_summary
                                .clone()
                                .unwrap_or_else(|| "Agent failed".to_string()),
                        )
                    }
                    Some(SpineSpawnOutcome::Aborted) => {
                        codex_protocol::protocol::AgentStatus::Shutdown
                    }
                    None => continue,
                };
                task.result_node_id = Some(node.node_id.clone());
                claimed_node_ids.insert(node.node_id.clone());
            }
        }
    }

    fn bump_revision(&mut self) {
        self.revision = self.revision.saturating_add(1);
    }
}

fn generation_entries(
    entries: impl IntoIterator<Item = (ThreadId, u64)>,
) -> Vec<SpineUiAgentGeneration> {
    let mut entries = entries
        .into_iter()
        .map(|(thread_id, generation)| SpineUiAgentGeneration {
            thread_id,
            generation,
        })
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.thread_id.to_string());
    entries
}

fn status_rank(status: &codex_protocol::protocol::AgentStatus) -> u8 {
    match status {
        codex_protocol::protocol::AgentStatus::PendingInit => 0,
        codex_protocol::protocol::AgentStatus::Running => 1,
        codex_protocol::protocol::AgentStatus::Interrupted => 2,
        codex_protocol::protocol::AgentStatus::Completed(_)
        | codex_protocol::protocol::AgentStatus::Errored(_)
        | codex_protocol::protocol::AgentStatus::Shutdown
        | codex_protocol::protocol::AgentStatus::NotFound => 3,
    }
}

pub(crate) fn is_enabled() -> bool {
    enabled_from_env_value(std::env::var(ENABLE_ENV).ok().as_deref())
}

fn enabled_from_env_value(value: Option<&str>) -> bool {
    value.is_some_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "on"
        )
    })
}

pub(crate) fn is_tree_tool_call(item: &ResponseItem) -> bool {
    let ResponseItem::FunctionCall {
        name, namespace, ..
    } = item
    else {
        return false;
    };
    let tool = match namespace.as_deref() {
        Some("spine") => name.as_str(),
        None => name.strip_prefix("spine.").unwrap_or_default(),
        Some(_) => return false,
    };
    matches!(tool, "open" | "next" | "close" | "spawn")
}

pub(crate) fn read_resource(server: &str, uri: &str) -> Option<McpResourceReadResponse> {
    (server == SERVER_NAME && uri == RESOURCE_URI).then(|| McpResourceReadResponse {
        contents: vec![McpResourceContent::Text {
            uri: uri.to_string(),
            mime_type: Some(RESOURCE_MIME_TYPE.to_string()),
            text: RESOURCE_HTML.to_string(),
            meta: Some(json!({
                "ui": {
                    "prefersBorder": true,
                    "csp": {
                        "connectDomains": [],
                        "resourceDomains": []
                    }
                },
                "openai/widgetHeightHint": 1,
                "openai/widgetMinFrameHeight": 1
            })),
        }],
    })
}

pub(crate) fn is_internal_tool(server: &str, tool: &str) -> bool {
    server == SERVER_NAME && tool == TOOL_NAME
}

pub(crate) fn is_internal_history_item(
    turn_id: &str,
    id: &str,
    server: &str,
    tool: &str,
    resource_uri: Option<&str>,
) -> bool {
    id == format!("{ITEM_ID_PREFIX}{turn_id}")
        && is_internal_tool(server, tool)
        && resource_uri == Some(RESOURCE_URI)
}

pub(crate) fn hide_internal_history_items_when_disabled(turns: &mut [Turn]) {
    filter_internal_history_items(turns, is_enabled());
}

fn filter_internal_history_items(turns: &mut [Turn], enabled: bool) {
    if enabled {
        return;
    }
    for turn in turns {
        turn.items
            .retain(|item| !is_internal_thread_item(&turn.id, item));
    }
}

pub(crate) fn is_internal_thread_item(turn_id: &str, item: &ThreadItem) -> bool {
    let ThreadItem::McpToolCall {
        id,
        server,
        tool,
        mcp_app_resource_uri,
        ..
    } = item
    else {
        return false;
    };
    is_internal_history_item(turn_id, id, server, tool, mcp_app_resource_uri.as_deref())
}

pub(crate) fn persisted_completed_states_for_child(
    rollout_items: &[RolloutItem],
    child_thread_id: ThreadId,
) -> Vec<(String, SpineUiState)> {
    let mut states_by_item_id = HashMap::new();
    for (index, item) in rollout_items.iter().enumerate() {
        let RolloutItem::EventMsg(EventMsg::ItemCompleted(event)) = item else {
            continue;
        };
        let CoreTurnItem::McpToolCall(item) = &event.item else {
            continue;
        };
        if !is_internal_history_item(
            &event.turn_id,
            &item.id,
            &item.server,
            &item.tool,
            item.mcp_app_resource_uri.as_deref(),
        ) {
            continue;
        }
        let Some(content) = item
            .result
            .as_ref()
            .and_then(|result| result.structured_content.clone())
        else {
            continue;
        };
        let Some(state) = SpineUiState::from_structured_content(content) else {
            continue;
        };
        let should_replace = states_by_item_id.get(&item.id).is_none_or(
            |(existing_index, _, existing_state): &(usize, String, SpineUiState)| {
                state.revision > existing_state.revision
                    || (state.revision == existing_state.revision && index > *existing_index)
            },
        );
        if should_replace {
            states_by_item_id.insert(item.id.clone(), (index, event.turn_id.clone(), state));
        }
    }
    let mut states = states_by_item_id
        .into_values()
        .filter_map(|(index, turn_id, state)| {
            let generation = *state.tracked_agent_generations().get(&child_thread_id)?;
            Some((index, turn_id, state, generation))
        })
        .collect::<Vec<_>>();
    let Some(generation) = states.iter().map(|(_, _, _, generation)| *generation).max() else {
        return Vec::new();
    };
    states.retain(|(_, _, _, candidate)| *candidate == generation);
    states.sort_by_key(|state| std::cmp::Reverse(state.0));
    states
        .into_iter()
        .map(|(_, turn_id, state, _)| (turn_id, state))
        .collect()
}

pub(crate) fn tool_call_response(
    thread_id: &str,
    state: Option<&SpineUiState>,
) -> McpServerToolCallResponse {
    let Some(structured_content) = state.and_then(SpineUiState::structured_content) else {
        return McpServerToolCallResponse {
            content: vec![json!({
                "type": "text",
                "text": "No Spine Tree is active for this thread."
            })],
            structured_content: None,
            is_error: Some(true),
            meta: None,
        };
    };
    McpServerToolCallResponse {
        content: vec![json!({
            "type": "text",
            "text": "Spine Tree"
        })],
        structured_content: Some(structured_content),
        is_error: Some(false),
        meta: Some(json!({
            "openai/widgetSessionId": format!("spine-ui-tool-{thread_id}")
        })),
    }
}

pub(crate) fn server_status(include_resources: bool) -> McpServerStatus {
    let tool = Tool {
        name: TOOL_NAME.to_string(),
        title: Some("Spine Tree".to_string()),
        description: Some(
            "Host-managed read-only view of the current Spine task tree.".to_string(),
        ),
        input_schema: json!({"type": "object", "properties": {}, "additionalProperties": false}),
        output_schema: None,
        annotations: Some(json!({
            "readOnlyHint": true,
            "destructiveHint": false,
            "openWorldHint": false
        })),
        icons: None,
        meta: Some(json!({"ui": {"resourceUri": RESOURCE_URI}})),
    };
    McpServerStatus {
        name: SERVER_NAME.to_string(),
        server_info: Some(McpServerInfo {
            name: SERVER_NAME.to_string(),
            title: Some("Spine UI".to_string()),
            version: "1".to_string(),
            description: Some("Read-only Spine task tree UI.".to_string()),
            icons: None,
            website_url: None,
        }),
        tools: HashMap::from([(tool.name.clone(), tool)]),
        resources: include_resources
            .then(|| registered_resource(RESOURCE_URI, "spine-tree", "Spine Tree"))
            .into_iter()
            .collect(),
        resource_templates: Vec::new(),
        auth_status: McpAuthStatus::Unsupported,
    }
}

fn registered_resource(uri: &str, name: &str, title: &str) -> Resource {
    Resource {
        annotations: None,
        description: Some("Spine task tree component".to_string()),
        mime_type: Some(RESOURCE_MIME_TYPE.to_string()),
        name: name.to_string(),
        size: None,
        title: Some(title.to_string()),
        uri: uri.to_string(),
        icons: None,
        meta: None,
    }
}

pub(crate) fn snapshot_started_notification(
    thread_id: &str,
    turn_id: &str,
    state: &SpineUiState,
) -> Option<ItemStartedNotification> {
    Some(ItemStartedNotification {
        item: snapshot_item(turn_id, state, McpToolCallStatus::InProgress)?,
        thread_id: thread_id.to_string(),
        turn_id: turn_id.to_string(),
        started_at_ms: now_unix_timestamp_ms(),
    })
}

pub(crate) fn snapshot_thread_item(turn_id: &str, state: &SpineUiState) -> Option<ThreadItem> {
    snapshot_item(turn_id, state, McpToolCallStatus::InProgress)
}

pub(crate) fn snapshot_completed_notification(
    thread_id: &str,
    turn_id: &str,
    state: &SpineUiState,
) -> Option<ItemCompletedNotification> {
    Some(ItemCompletedNotification {
        item: snapshot_item(turn_id, state, McpToolCallStatus::Completed)?,
        thread_id: thread_id.to_string(),
        turn_id: turn_id.to_string(),
        completed_at_ms: now_unix_timestamp_ms(),
    })
}

fn snapshot_item(
    turn_id: &str,
    state: &SpineUiState,
    status: McpToolCallStatus,
) -> Option<ThreadItem> {
    Some(ThreadItem::from(snapshot_core_item(
        turn_id,
        state,
        match status {
            McpToolCallStatus::InProgress => CoreMcpToolCallStatus::InProgress,
            McpToolCallStatus::Completed => CoreMcpToolCallStatus::Completed,
            McpToolCallStatus::Failed => CoreMcpToolCallStatus::Failed,
        },
    )?))
}

pub(crate) fn snapshot_completed_event(
    thread_id: ThreadId,
    turn_id: &str,
    state: &SpineUiState,
) -> Option<ItemCompletedEvent> {
    Some(ItemCompletedEvent {
        thread_id,
        turn_id: turn_id.to_string(),
        item: snapshot_core_item(turn_id, state, CoreMcpToolCallStatus::Completed)?,
        completed_at_ms: now_unix_timestamp_ms(),
    })
}

fn snapshot_core_item(
    turn_id: &str,
    state: &SpineUiState,
    status: CoreMcpToolCallStatus,
) -> Option<CoreTurnItem> {
    let structured_content = state.structured_content()?;
    let snapshot_seq = state.snapshot.as_ref()?.snapshot_seq;
    // Codex supersedes older widgets that share a server/resource unless the host
    // gives each independently mounted turn a stable widget session.
    let item_id = format!("{ITEM_ID_PREFIX}{turn_id}");
    Some(CoreTurnItem::McpToolCall(McpToolCallItem {
        id: item_id.clone(),
        server: SERVER_NAME.to_string(),
        tool: TOOL_NAME.to_string(),
        status,
        arguments: json!({}),
        connector_id: Some(SERVER_NAME.to_string()),
        mcp_app_resource_uri: Some(RESOURCE_URI.to_string()),
        link_id: None,
        app_name: Some("Spine UI".to_string()),
        template_id: None,
        action_name: Some(TOOL_NAME.to_string()),
        plugin_id: None,
        result: Some(CallToolResult {
            content: vec![json!({
                "type": "text",
                "text": format!("Spine snapshot {snapshot_seq}"),
            })],
            structured_content: Some(structured_content),
            is_error: Some(false),
            meta: Some(json!({
                "ui/resourceUri": RESOURCE_URI,
                "openai/widgetSessionId": item_id,
            })),
        }),
        error: None,
        duration: None,
    }))
}

fn now_unix_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
#[path = "spine_ui_tests.rs"]
mod tests;
