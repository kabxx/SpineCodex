use codex_protocol::ThreadId;
use codex_protocol::items::McpToolCallItem;
use codex_protocol::items::McpToolCallStatus;
use codex_protocol::items::TurnItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::ThreadHistoryMode;
use pretty_assertions::assert_eq;
use serde_json::json;

use super::persisted_rollout_items;

fn completed_mcp_item(
    id: &str,
    server: &str,
    tool: &str,
    resource_uri: Option<&str>,
) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
        thread_id: ThreadId::default(),
        turn_id: "turn".to_string(),
        item: TurnItem::McpToolCall(McpToolCallItem {
            id: id.to_string(),
            server: server.to_string(),
            tool: tool.to_string(),
            arguments: json!({}),
            connector_id: None,
            mcp_app_resource_uri: resource_uri.map(str::to_string),
            link_id: None,
            app_name: None,
            template_id: None,
            action_name: None,
            plugin_id: None,
            status: McpToolCallStatus::Completed,
            result: None,
            error: None,
            duration: None,
        }),
        completed_at_ms: 0,
    }))
}

#[test]
fn legacy_history_keeps_only_the_spine_ui_completed_mcp_item() {
    let spine = completed_mcp_item(
        "spine-ui-turn",
        "__codex_internal_spine_tree_ui__",
        "spine_tree",
        Some("ui://spine/tree.html"),
    );
    let configured_collision = completed_mcp_item(
        "spine-ui-wrong-turn",
        "__codex_internal_spine_tree_ui__",
        "spine_tree",
        Some("ui://spine/tree.html"),
    );
    let old_internal_id = completed_mcp_item(
        "spine-ui-old",
        "__codex_spine_ui",
        "spine_tree",
        Some("ui://spine/tree.html"),
    );
    let items = vec![spine.clone(), configured_collision, old_internal_id];

    let legacy = persisted_rollout_items(&items, ThreadHistoryMode::Legacy);
    assert_eq!(
        serde_json::to_value(legacy).expect("serialize legacy items"),
        serde_json::to_value([spine]).expect("serialize expected legacy items")
    );

    let paginated = persisted_rollout_items(&items, ThreadHistoryMode::Paginated);
    assert_eq!(
        serde_json::to_value(paginated).expect("serialize paginated items"),
        serde_json::to_value(items).expect("serialize expected paginated items")
    );
}
