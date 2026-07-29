use super::*;
use codex_app_server_protocol::McpToolCallAppContext;
use codex_app_server_protocol::McpToolCallStatus;
use codex_app_server_protocol::ThreadHistoryBuilder;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::SpineTreeNodeKind;
use codex_protocol::protocol::SpineTreeNodeSnapshot;
use codex_protocol::protocol::SpineTreeNodeStatus;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnStartedEvent;
use pretty_assertions::assert_eq;

fn snapshot(sequence: u64, active_node_id: &str) -> SpineTreeUpdateEvent {
    SpineTreeUpdateEvent {
        snapshot_seq: sequence,
        active_node_id: active_node_id.to_string(),
        settled_spawn_call_ids: Vec::new(),
        nodes: vec![
            node(
                "1",
                None,
                SpineTreeNodeKind::RootEpoch,
                SpineTreeNodeStatus::Opened,
                None,
            ),
            node(
                "1.1",
                Some("1"),
                SpineTreeNodeKind::Task,
                if active_node_id == "1.1" {
                    SpineTreeNodeStatus::Live
                } else {
                    SpineTreeNodeStatus::Closed
                },
                Some("Render the Spine tree"),
            ),
            node(
                "1.2",
                Some("1"),
                SpineTreeNodeKind::Task,
                if active_node_id == "1.2" {
                    SpineTreeNodeStatus::Live
                } else {
                    SpineTreeNodeStatus::Opened
                },
                Some("Verify the result"),
            ),
        ],
    }
}

fn node(
    node_id: &str,
    parent_id: Option<&str>,
    kind: SpineTreeNodeKind,
    status: SpineTreeNodeStatus,
    summary: Option<&str>,
) -> SpineTreeNodeSnapshot {
    SpineTreeNodeSnapshot {
        node_id: node_id.to_string(),
        parent_id: parent_id.map(str::to_string),
        kind,
        status,
        summary: summary.map(str::to_string),
        memory_summary: None,
        spawn_outcome: None,
        start: 0,
        end: None,
        context_pressure: None,
    }
}

fn spawn_progress(call_id: &str, status: AgentStatus) -> SpineSpawnProgressEvent {
    spawn_progress_for_thread(call_id, ThreadId::new(), status)
}

fn spawn_progress_for_thread(
    call_id: &str,
    thread_id: ThreadId,
    status: AgentStatus,
) -> SpineSpawnProgressEvent {
    SpineSpawnProgressEvent {
        call_id: call_id.to_string(),
        tasks: vec![SpineSpawnTaskProgress {
            ordinal: 0,
            summary: format!("Run {call_id}"),
            thread_id,
            agent_path: Some(
                AgentPath::try_from(format!("/root/{call_id}")).expect("valid agent path"),
            ),
            status,
        }],
    }
}

#[test]
fn app_ui_is_default_off_with_an_explicit_opt_in() {
    assert!(enabled_from_env_value(Some("1")));
    assert!(enabled_from_env_value(Some("true")));
    assert!(enabled_from_env_value(Some(" ON ")));
    assert!(!enabled_from_env_value(None));
    assert!(!enabled_from_env_value(Some("")));
    assert!(!enabled_from_env_value(Some("0")));
    assert!(!enabled_from_env_value(Some(" FALSE ")));
    assert!(!enabled_from_env_value(Some("off")));
    assert!(!enabled_from_env_value(Some("enabled")));
}

#[test]
fn disabled_app_ui_hides_internal_history_items_without_mutating_enabled_history() {
    let internal_item = ThreadItem::McpToolCall {
        id: "spine-ui-turn-1".to_string(),
        server: SERVER_NAME.to_string(),
        tool: TOOL_NAME.to_string(),
        status: McpToolCallStatus::Completed,
        arguments: serde_json::json!({}),
        app_context: None,
        mcp_app_resource_uri: Some(RESOURCE_URI.to_string()),
        plugin_id: None,
        result: None,
        error: None,
        duration_ms: None,
    };
    let regular_item = ThreadItem::Plan {
        id: "plan-1".to_string(),
        text: "Keep regular history".to_string(),
    };
    let turns = vec![Turn {
        id: "turn-1".to_string(),
        items: vec![internal_item, regular_item.clone()],
        items_view: codex_app_server_protocol::TurnItemsView::Full,
        status: codex_app_server_protocol::TurnStatus::Completed,
        error: None,
        started_at: None,
        completed_at: None,
        duration_ms: None,
    }];

    let mut disabled_turns = turns.clone();
    filter_internal_history_items(&mut disabled_turns, false);
    assert_eq!(disabled_turns[0].items, vec![regular_item]);

    let mut enabled_turns = turns.clone();
    filter_internal_history_items(&mut enabled_turns, true);
    assert_eq!(enabled_turns, turns);
}

#[test]
fn only_tree_affecting_spine_calls_activate_the_ui() {
    for tool in ["open", "next", "close", "spawn"] {
        assert!(is_tree_tool_call(&function_call(tool, Some("spine"))));
        assert!(is_tree_tool_call(&function_call(
            &format!("spine.{tool}"),
            None
        )));
    }
    assert!(!is_tree_tool_call(&function_call("trim", Some("spine"))));
    assert!(!is_tree_tool_call(&function_call("open", Some("other"))));
    assert!(!is_tree_tool_call(&function_call("shell", None)));
}

fn function_call(name: &str, namespace: Option<&str>) -> ResponseItem {
    ResponseItem::FunctionCall {
        id: None,
        name: name.to_string(),
        namespace: namespace.map(str::to_string),
        arguments: "{}".to_string(),
        call_id: format!("call-{name}"),
        internal_chat_message_metadata_passthrough: None,
    }
}

#[test]
fn resource_is_scoped_and_self_contained() {
    let response = read_resource(SERVER_NAME, RESOURCE_URI).expect("Spine UI resource");
    assert_eq!(response.contents.len(), 1);
    assert!(read_resource("other", RESOURCE_URI).is_none());
    assert!(read_resource(SERVER_NAME, "ui://spine/other.html").is_none());
    assert!(RESOURCE_HTML.contains("default-src 'none'"));
    assert!(RESOURCE_HTML.contains("ResizeObserver"));
    assert!(RESOURCE_HTML.contains("body { min-height: 0; padding: 0; }"));
    assert!(RESOURCE_HTML.contains("max-height: 720px;"));
    assert!(RESOURCE_HTML.contains("overflow-y: auto;"));
    assert!(RESOURCE_HTML.contains(".card-body { padding: 6px; }"));
    assert!(RESOURCE_HTML.contains(".path-children > .drawer-control-item::before"));
    assert!(RESOURCE_HTML.contains("count === 1 ? \"leaf\" : \"leaves\""));
    assert!(RESOURCE_HTML.contains("function validTreePayload(value)"));
    assert!(RESOURCE_HTML.contains("typeof node.summary !== \"string\""));
    assert!(RESOURCE_HTML.contains("function spawnOutcomeState(outcome)"));
    assert!(RESOURCE_HTML.contains("spawn-result-row"));
    assert!(!RESOURCE_HTML.contains("agent-path"));
    assert!(RESOURCE_HTML.contains("notifyBackgroundColor?.(\"transparent\")"));
    assert!(!RESOURCE_HTML.contains("background: var(--surface)"));
    assert!(!RESOURCE_HTML.contains("no-disclosure-slot"));
    assert!(!RESOURCE_HTML.contains("<script src="));
    assert!(!RESOURCE_HTML.contains("<link rel=\"stylesheet\""));
    assert!(!RESOURCE_HTML.contains("https://"));

    let status = server_status(true);
    let tool = status.tools.get("spine_tree").expect("Spine Tree tool");
    assert_eq!(tool.title.as_deref(), Some("Spine Tree"));
    assert_eq!(status.name, SERVER_NAME);
    assert_eq!(status.resources[0].uri, RESOURCE_URI);
    assert_eq!(status.resources.len(), 1);
    assert_eq!(
        tool.meta.as_ref().expect("tool metadata")["ui"]["resourceUri"],
        RESOURCE_URI
    );
}

#[test]
fn advertised_tool_returns_a_well_formed_read_only_result() {
    assert!(is_internal_tool(SERVER_NAME, TOOL_NAME));
    assert!(!is_internal_tool("other", TOOL_NAME));

    let inactive = tool_call_response("thread-1", None);
    assert_eq!(inactive.is_error, Some(true));
    assert!(inactive.structured_content.is_none());

    let mut state = SpineUiState::default();
    state.record_snapshot(snapshot(5, "1.1"));
    let active = tool_call_response("thread-1", Some(&state));
    assert_eq!(active.is_error, Some(false));
    assert_eq!(
        active
            .structured_content
            .as_ref()
            .expect("structured content")["snapshot"]["snapshotSeq"],
        5
    );
    assert_eq!(
        active.meta.expect("tool metadata")["openai/widgetSessionId"],
        "spine-ui-tool-thread-1"
    );
}

#[test]
fn completed_snapshot_reconstructs_as_the_same_mcp_app_item() {
    let thread_id = ThreadId::new();
    let child_thread_id = ThreadId::new();
    let mut state = SpineUiState::default();
    state.record_snapshot(snapshot(7, "1.1"));
    state.record_spawn_progress(spawn_progress_for_thread(
        "review",
        child_thread_id,
        AgentStatus::Running,
    ));
    let mut child_state = SpineUiState::default();
    child_state.record_snapshot(snapshot(3, "1.2"));
    state.record_agent_state(child_thread_id, 1, child_state);
    let completed =
        snapshot_completed_event(thread_id, "turn-1", &state).expect("completed Spine UI event");

    let mut builder = ThreadHistoryBuilder::new();
    builder.handle_rollout_item(&RolloutItem::EventMsg(EventMsg::TurnStarted(
        TurnStartedEvent {
            turn_id: "turn-1".to_string(),
            trace_id: None,
            started_at: None,
            model_context_window: None,
            collaboration_mode_kind: Default::default(),
        },
    )));
    builder.handle_rollout_item(&RolloutItem::EventMsg(EventMsg::TurnComplete(
        TurnCompleteEvent {
            turn_id: "turn-1".to_string(),
            last_agent_message: None,
            completed_at: None,
            duration_ms: None,
            time_to_first_token_ms: None,
        },
    )));
    let mut regular_mcp = completed.clone();
    let CoreTurnItem::McpToolCall(regular_mcp_item) = &mut regular_mcp.item else {
        panic!("expected canonical MCP tool call");
    };
    regular_mcp_item.id = "regular-mcp".to_string();
    regular_mcp_item.server = "another_server".to_string();
    regular_mcp_item.tool = "another_tool".to_string();
    builder.handle_rollout_item(&RolloutItem::EventMsg(EventMsg::ItemCompleted(regular_mcp)));
    builder.handle_rollout_item(&RolloutItem::EventMsg(EventMsg::ItemCompleted(completed)));

    let turns = builder.finish();
    assert_eq!(turns[0].items.len(), 1);
    let ThreadItem::McpToolCall {
        id,
        status,
        app_context,
        mcp_app_resource_uri,
        result,
        ..
    } = &turns[0].items[0]
    else {
        panic!("expected reconstructed MCP App item");
    };
    assert_eq!(id, "spine-ui-turn-1");
    assert_eq!(*status, McpToolCallStatus::Completed);
    assert_eq!(
        app_context,
        &Some(McpToolCallAppContext {
            connector_id: SERVER_NAME.to_string(),
            link_id: None,
            resource_uri: Some(RESOURCE_URI.to_string()),
            app_name: Some("Spine UI".to_string()),
            template_id: None,
            action_name: Some("spine_tree".to_string()),
        })
    );
    assert_eq!(mcp_app_resource_uri.as_deref(), Some(RESOURCE_URI));
    assert_eq!(
        result
            .as_ref()
            .and_then(|result| result.structured_content.as_ref())
            .expect("structured content")["spawnCalls"][0]["callId"],
        "review"
    );
    assert_eq!(
        result
            .as_ref()
            .and_then(|result| result.structured_content.as_ref())
            .expect("structured content")["agentSubtrees"][0]["threadId"],
        child_thread_id.to_string()
    );
    assert_eq!(
        result
            .as_ref()
            .and_then(|result| result.meta.as_ref())
            .expect("result metadata")["openai/widgetSessionId"],
        "spine-ui-turn-1"
    );
}

#[test]
fn spawn_calls_keep_creation_order_and_original_parent() {
    let mut state = SpineUiState::default();
    state.record_snapshot(snapshot(1, "1.1"));
    state.record_spawn_progress(spawn_progress("first", AgentStatus::Running));
    state.record_snapshot(snapshot(2, "1.2"));
    state.record_spawn_progress(spawn_progress("second", AgentStatus::Running));
    state.record_spawn_progress(spawn_progress("first", AgentStatus::Completed(None)));

    let content = state.structured_content().expect("structured content");
    assert_eq!(content["spawnCalls"][0]["callId"], "first");
    assert_eq!(content["spawnCalls"][0]["parentNodeId"], "1.1");
    assert_eq!(
        content["spawnCalls"][0]["tasks"][0]["status"],
        json!({ "completed": null })
    );
    assert_eq!(content["spawnCalls"][1]["callId"], "second");
    assert_eq!(content["spawnCalls"][1]["parentNodeId"], "1.2");
}

#[test]
fn changed_snapshot_at_the_same_boundary_advances_the_ui_revision() {
    let mut state = SpineUiState::default();
    state.record_snapshot(snapshot(7, "1.1"));
    let first_revision = state.revision;

    state.record_snapshot(snapshot(7, "1.2"));
    assert!(state.revision > first_revision);
    assert_eq!(
        state
            .latest_snapshot()
            .map(|snapshot| snapshot.active_node_id.as_str()),
        Some("1.2")
    );

    let second_revision = state.revision;
    state.record_snapshot(snapshot(7, "1.2"));
    assert_eq!(state.revision, second_revision);
}

#[test]
fn filtered_agent_state_reanchors_direct_nested_spawns() {
    let grandchild_thread_id = ThreadId::new();
    let mut child_state = SpineUiState::default();
    child_state.record_snapshot(snapshot(4, "1.2"));
    child_state.record_spawn_progress(spawn_progress_for_thread(
        "nested",
        grandchild_thread_id,
        AgentStatus::Running,
    ));
    let baseline = child_state
        .latest_snapshot()
        .expect("child snapshot")
        .nodes
        .iter()
        .map(|node| node.node_id.clone())
        .collect();

    let filtered = child_state.filtered_for_parent(&baseline);
    let content = filtered.structured_content().expect("filtered content");

    assert_eq!(content["snapshot"]["nodes"], json!([]));
    assert_eq!(content["spawnCalls"][0]["parentNodeId"], json!(null));
    assert_eq!(
        content["spawnCalls"][0]["tasks"][0]["threadId"],
        grandchild_thread_id.to_string()
    );
}

#[test]
fn agent_route_generation_replaces_reset_state_and_guards_removal() {
    let child_thread_id = ThreadId::new();
    let mut state = SpineUiState::default();
    state.record_snapshot(snapshot(1, "1.1"));
    state.record_spawn_progress(spawn_progress_for_thread(
        "child",
        child_thread_id,
        AgentStatus::Running,
    ));

    let mut first = SpineUiState::default();
    first.record_snapshot(snapshot(8, "1.2"));
    assert!(state.record_agent_state(child_thread_id, 1, first));

    let mut reset = SpineUiState::default();
    reset.record_snapshot(snapshot(1, "1.1"));
    assert!(state.record_agent_state(child_thread_id, 2, reset));
    assert!(!state.remove_agent_state(child_thread_id, 1));
    assert!(state.remove_agent_state(child_thread_id, 2));
    assert!(state.agent_subtrees.is_empty());
}

#[test]
fn invalidating_new_timeout_removes_older_subtree_and_tracking() {
    let child_thread_id = ThreadId::new();
    let mut state = SpineUiState::default();
    state.record_snapshot(snapshot(1, "1.1"));
    state.record_spawn_progress(spawn_progress_for_thread(
        "child",
        child_thread_id,
        AgentStatus::Running,
    ));

    let mut child = SpineUiState::default();
    child.record_snapshot(snapshot(2, "1.2"));
    assert!(state.record_agent_state(child_thread_id, 7, child));
    assert!(state.mark_agent_sync_timeout(child_thread_id, 8));
    assert_eq!(
        state.tracked_agent_generations().get(&child_thread_id),
        Some(&8)
    );

    let mut stale_persisted_state = state.clone();
    stale_persisted_state
        .invalidated_agent_generations
        .insert(child_thread_id, 8);
    assert!(
        !stale_persisted_state
            .tracked_agent_generations()
            .contains_key(&child_thread_id)
    );

    assert!(state.remove_agent_state(child_thread_id, 8));
    assert!(state.agent_subtrees.is_empty());
    assert!(state.agent_sync_timeout_generations.is_empty());
    assert!(state.tracked_agent_generations().is_empty());
}

#[test]
fn agent_sync_timeout_is_visible_until_the_matching_terminal_state_arrives() {
    let child_thread_id = ThreadId::new();
    let mut state = SpineUiState::default();
    state.record_snapshot(snapshot(1, "1.1"));
    state.record_spawn_progress(spawn_progress_for_thread(
        "child",
        child_thread_id,
        AgentStatus::Running,
    ));

    assert!(state.mark_agent_sync_timeout(child_thread_id, 7));
    assert_eq!(
        state.structured_content().expect("timed out content")["spawnCalls"][0]["tasks"][0]["status"],
        json!({ "errored": "Final status synchronization timed out after 10 seconds" })
    );
    assert!(!state.clear_agent_sync_timeout(child_thread_id, 6));
    assert!(state.clear_agent_sync_timeout(child_thread_id, 7));
    assert_eq!(
        state.structured_content().expect("recovered content")["spawnCalls"][0]["tasks"][0]["status"],
        json!("running")
    );
}

#[test]
fn completed_state_round_trips_with_agent_generations_for_cold_rollback() {
    let child_thread_id = ThreadId::new();
    let parent_thread_id = ThreadId::new();
    let mut child = SpineUiState::default();
    child.record_snapshot(snapshot(2, "1.2"));
    let mut parent = SpineUiState::default();
    parent.record_snapshot(snapshot(1, "1.1"));
    parent.record_spawn_progress(spawn_progress_for_thread(
        "child",
        child_thread_id,
        AgentStatus::Completed(None),
    ));
    assert!(parent.record_agent_state(child_thread_id, 7, child));
    let event = snapshot_completed_event(parent_thread_id, "turn-1", &parent)
        .expect("completed item event");

    let restored_states = persisted_completed_states_for_child(
        &[RolloutItem::EventMsg(EventMsg::ItemCompleted(event))],
        child_thread_id,
    );
    let [(turn_id, restored)] = restored_states.as_slice() else {
        panic!("expected one restored state");
    };

    assert_eq!(turn_id, "turn-1");
    assert_eq!(restored.structured_content(), parent.structured_content());
    assert!(restored.tracks_agent_generation(child_thread_id, 7));
}

#[test]
fn timeout_only_state_round_trips_for_cold_rollback() {
    let child_thread_id = ThreadId::new();
    let parent_thread_id = ThreadId::new();
    let mut parent = SpineUiState::default();
    parent.record_snapshot(snapshot(1, "1.1"));
    parent.record_spawn_progress(spawn_progress_for_thread(
        "child",
        child_thread_id,
        AgentStatus::Completed(None),
    ));
    assert!(parent.mark_agent_sync_timeout(child_thread_id, 7));
    let event = snapshot_completed_event(parent_thread_id, "turn-1", &parent)
        .expect("completed item event");

    let restored_states = persisted_completed_states_for_child(
        &[RolloutItem::EventMsg(EventMsg::ItemCompleted(event))],
        child_thread_id,
    );
    let [(turn_id, restored)] = restored_states.as_slice() else {
        panic!("expected one restored timeout-only state");
    };

    assert_eq!(turn_id, "turn-1");
    assert!(restored.tracks_agent_or_timeout_generation(child_thread_id, 7));
}

#[test]
fn cold_rollback_uses_latest_internal_state_for_the_same_card() {
    let child_thread_id = ThreadId::new();
    let parent_thread_id = ThreadId::new();
    let mut original = SpineUiState::default();
    original.record_snapshot(snapshot(1, "1.1"));
    original.record_spawn_progress(spawn_progress_for_thread(
        "child",
        child_thread_id,
        AgentStatus::Completed(None),
    ));
    let mut child = SpineUiState::default();
    child.record_snapshot(snapshot(2, "1.2"));
    assert!(original.record_agent_state(child_thread_id, 7, child));
    let original_event = snapshot_completed_event(parent_thread_id, "turn-1", &original)
        .expect("original completed item event");

    let mut updated = original.clone();
    assert!(updated.remove_agent_state(child_thread_id, 7));
    let updated_event = snapshot_completed_event(parent_thread_id, "turn-1", &updated)
        .expect("updated completed item event");

    assert!(
        persisted_completed_states_for_child(
            &[
                RolloutItem::EventMsg(EventMsg::ItemCompleted(original_event)),
                RolloutItem::EventMsg(EventMsg::ItemCompleted(updated_event)),
            ],
            child_thread_id,
        )
        .is_empty()
    );

    let mut collision_event = snapshot_completed_event(parent_thread_id, "turn-1", &updated)
        .expect("collision completed item event");
    let CoreTurnItem::McpToolCall(collision_item) = &mut collision_event.item else {
        panic!("expected MCP tool call");
    };
    collision_item.server = "configured-real-server".to_string();
    let states = persisted_completed_states_for_child(
        &[
            RolloutItem::EventMsg(EventMsg::ItemCompleted(
                snapshot_completed_event(parent_thread_id, "turn-1", &original)
                    .expect("original completed item event"),
            )),
            RolloutItem::EventMsg(EventMsg::ItemCompleted(collision_event)),
        ],
        child_thread_id,
    );
    let [(_, restored)] = states.as_slice() else {
        panic!("expected the previous internal state to remain authoritative");
    };
    assert_eq!(restored.agent_generations().get(&child_thread_id), Some(&7));
}

#[test]
fn cold_rollback_restores_every_card_for_the_latest_agent_generation() {
    let child_thread_id = ThreadId::new();
    let parent_thread_id = ThreadId::new();
    let mut child = SpineUiState::default();
    child.record_snapshot(snapshot(3, "1.2"));
    let mut first = SpineUiState::default();
    first.record_snapshot(snapshot(1, "1.1"));
    first.record_spawn_progress(spawn_progress_for_thread(
        "child",
        child_thread_id,
        AgentStatus::Completed(None),
    ));
    assert!(first.record_agent_state(child_thread_id, 7, child.clone()));
    let mut second = first.clone();
    second.record_snapshot(snapshot(2, "1.2"));

    let states = persisted_completed_states_for_child(
        &[
            RolloutItem::EventMsg(EventMsg::ItemCompleted(
                snapshot_completed_event(parent_thread_id, "turn-1", &first)
                    .expect("first completed item event"),
            )),
            RolloutItem::EventMsg(EventMsg::ItemCompleted(
                snapshot_completed_event(parent_thread_id, "turn-2", &second)
                    .expect("second completed item event"),
            )),
        ],
        child_thread_id,
    );

    assert_eq!(
        states
            .iter()
            .map(|(turn_id, _)| turn_id.as_str())
            .collect::<Vec<_>>(),
        vec!["turn-2", "turn-1"]
    );
}

#[test]
fn cold_rollback_keeps_the_highest_revision_when_writes_arrive_out_of_order() {
    let child_thread_id = ThreadId::new();
    let parent_thread_id = ThreadId::new();
    let mut child = SpineUiState::default();
    child.record_snapshot(snapshot(3, "1.2"));
    let mut older = SpineUiState::default();
    older.record_snapshot(snapshot(1, "1.1"));
    older.record_spawn_progress(spawn_progress_for_thread(
        "child",
        child_thread_id,
        AgentStatus::Completed(None),
    ));
    assert!(older.record_agent_state(child_thread_id, 7, child));
    older.revision = 7;
    let mut newer = older.clone();
    newer.record_snapshot(snapshot(2, "1.2"));
    newer.revision = 8;

    let states = persisted_completed_states_for_child(
        &[
            RolloutItem::EventMsg(EventMsg::ItemCompleted(
                snapshot_completed_event(parent_thread_id, "turn-1", &newer)
                    .expect("newer completed item event"),
            )),
            RolloutItem::EventMsg(EventMsg::ItemCompleted(
                snapshot_completed_event(parent_thread_id, "turn-1", &older)
                    .expect("late older completed item event"),
            )),
        ],
        child_thread_id,
    );

    let [(_, restored)] = states.as_slice() else {
        panic!("expected one restored state");
    };
    assert_eq!(
        restored.structured_content().expect("structured content")["uiRevision"],
        8
    );
}

#[test]
fn unmatched_spawn_result_node_remains_visible_for_outcome_rendering() {
    let mut state = SpineUiState::default();
    let mut committed = snapshot(1, "1.1");
    let mut result_node = node(
        "1.1.1",
        Some("1.1"),
        SpineTreeNodeKind::Task,
        SpineTreeNodeStatus::Closed,
        Some("Rejected agent"),
    );
    result_node.spawn_outcome = Some(SpineSpawnOutcome::Errored);
    committed.nodes.push(result_node);
    state.record_snapshot(committed);

    let content = state.structured_content().expect("structured content");
    assert_eq!(content["spawnCalls"], json!([]));
    assert_eq!(content["suppressedNodeIds"], json!([]));
    assert_eq!(content["snapshot"]["nodes"][3]["nodeId"], "1.1.1");
    assert_eq!(content["snapshot"]["nodes"][3]["spawnOutcome"], "errored");
}

#[test]
fn settled_spawn_call_keeps_one_agent_row_and_suppresses_its_result_node() {
    let mut state = SpineUiState::default();
    state.record_snapshot(snapshot(1, "1.1"));
    state.record_spawn_progress(spawn_progress("settled", AgentStatus::Running));

    let mut committed = snapshot(2, "1.1");
    committed.settled_spawn_call_ids = vec!["settled".to_string()];
    let mut result_node = node(
        "1.1.1",
        Some("1.1"),
        SpineTreeNodeKind::Task,
        SpineTreeNodeStatus::Closed,
        Some("Run settled"),
    );
    result_node.spawn_outcome = Some(SpineSpawnOutcome::Completed);
    committed.nodes.push(result_node);
    state.record_snapshot(committed.clone());
    state.record_spawn_progress(spawn_progress("settled", AgentStatus::Completed(None)));
    committed.snapshot_seq = 3;
    committed.settled_spawn_call_ids.clear();
    state.record_snapshot(committed);

    let content = state.structured_content().expect("structured content");
    assert_eq!(content["spawnCalls"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        content["spawnCalls"][0]["tasks"][0]["resultNodeId"],
        "1.1.1"
    );
    assert_eq!(
        content["spawnCalls"][0]["tasks"][0]["status"],
        json!({ "completed": null })
    );
    assert_eq!(content["suppressedNodeIds"], json!(["1.1.1"]));
    assert_eq!(content["snapshot"]["nodes"][3]["nodeId"], "1.1.1");
}

#[test]
fn agent_subtree_is_ordered_with_its_agent_and_survives_carry_forward() {
    let child_thread_id = ThreadId::new();
    let mut state = SpineUiState::default();
    state.record_snapshot(snapshot(1, "1.1"));
    state.record_spawn_progress(spawn_progress_for_thread(
        "child",
        child_thread_id,
        AgentStatus::Running,
    ));
    let mut child_state = SpineUiState::default();
    child_state.record_snapshot(snapshot(4, "1.2"));
    state.record_agent_state(child_thread_id, 1, child_state);
    let mut stale_child_state = SpineUiState::default();
    stale_child_state.record_snapshot(snapshot(3, "1.1"));
    state.record_agent_state(child_thread_id, 1, stale_child_state);

    let mut later_turn = state.carry_forward();
    later_turn.record_snapshot(snapshot(2, "1.2"));
    let content = later_turn.structured_content().expect("structured content");

    assert_eq!(
        content["agentSubtrees"][0]["threadId"],
        child_thread_id.to_string()
    );
    assert_eq!(content["agentSubtrees"][0]["snapshot"]["snapshotSeq"], 4);
    assert_eq!(
        content["agentSubtrees"][0]["snapshot"]["activeNodeId"],
        "1.2"
    );
}

#[test]
fn unsettled_terminal_agents_carry_forward_with_their_parent() {
    let mut state = SpineUiState::default();
    state.record_snapshot(snapshot(1, "1.1"));
    state.record_spawn_progress(spawn_progress("interrupted", AgentStatus::Completed(None)));

    let mut later_turn = state.carry_forward();
    later_turn.record_snapshot(snapshot(2, "1.2"));

    let content = later_turn.structured_content().expect("structured content");
    assert_eq!(content["spawnCalls"][0]["callId"], "interrupted");
    assert_eq!(content["spawnCalls"][0]["parentNodeId"], "1.1");
    assert_eq!(
        content["spawnCalls"][0]["tasks"][0]["status"],
        json!({ "completed": null })
    );
}

#[test]
fn snapshot_projects_to_a_stable_mcp_app_item_lifecycle() {
    let mut state = SpineUiState::default();
    state.record_snapshot(snapshot(7, "1.1"));
    let started =
        snapshot_started_notification("thread-1", "turn-1", &state).expect("started notification");
    let completed = snapshot_completed_notification("thread-1", "turn-1", &state)
        .expect("completed notification");
    let other_turn = snapshot_completed_notification("thread-1", "turn-2", &state)
        .expect("second completed notification");

    let ThreadItem::McpToolCall {
        id: started_id,
        status: started_status,
        mcp_app_resource_uri,
        result: started_result,
        ..
    } = started.item
    else {
        panic!("expected MCP tool call item");
    };
    let ThreadItem::McpToolCall {
        id: completed_id,
        status: completed_status,
        result: completed_result,
        ..
    } = completed.item
    else {
        panic!("expected MCP tool call item");
    };

    assert_eq!(started_id, completed_id);
    assert_eq!(started_status, McpToolCallStatus::InProgress);
    assert_eq!(completed_status, McpToolCallStatus::Completed);
    assert_eq!(mcp_app_resource_uri.as_deref(), Some(RESOURCE_URI));
    let started_result = started_result.expect("started result");
    let completed_result = completed_result.expect("completed result");
    assert_eq!(started_result.meta, completed_result.meta);
    assert_eq!(
        started_result.meta.as_ref().expect("result metadata")["openai/widgetSessionId"],
        "spine-ui-turn-1"
    );
    let ThreadItem::McpToolCall {
        result: Some(other_result),
        ..
    } = other_turn.item
    else {
        panic!("expected second MCP tool call result");
    };
    assert_ne!(started_result.meta, other_result.meta);
    let content = started_result
        .structured_content
        .expect("structured content");
    assert_eq!(content["schemaVersion"], 1);
    assert_eq!(content["snapshot"]["snapshotSeq"], 7);
    assert_eq!(content["spawnCalls"], json!([]));
}
