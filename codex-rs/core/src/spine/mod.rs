use crate::context_manager::ContextManager;
use crate::context_manager::is_user_turn_boundary;
use crate::event_mapping::is_contextual_dev_message_content;
use crate::event_mapping::is_contextual_user_message_content;
use crate::tools::code_mode::is_exec_tool_name;
use crate::tools::code_mode::spine_bridge::CODE_MODE_SPINE_CARRIER_MARKER;
use crate::tools::code_mode::spine_bridge::NestedSpineCallV1;
use crate::tools::code_mode::spine_bridge::NestedSpineToolName;
use crate::tools::code_mode::spine_bridge::decode_marked_body;
use codex_protocol::models::ContentItem;
use codex_protocol::models::FunctionCallOutputBody;
use codex_protocol::models::FunctionCallOutputPayload;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::RolloutItem;
use codex_spine_core::ContextItem;
use codex_spine_core::MemorySlot;
use codex_spine_core::Message;
use codex_spine_core::MessageRole;
use codex_spine_core::NativeItemRef;
use codex_spine_core::NodeStatus;
use codex_spine_core::RawBoundary;
use codex_spine_core::RolloutEvent;
use codex_spine_core::SpineProjection;
use codex_spine_core::SpineReducer;
use codex_spine_core::ToolCallGroup;
use codex_spine_core::ToolOutcome;
use codex_spine_core::ToolUse;
use codex_spine_core::TrimEdit;
use codex_spine_core::TrimProjection;
use codex_spine_core::TrimRequest;
use serde::Deserialize;

pub(crate) mod instructions;
pub(crate) mod memory_projection;
pub(crate) mod pressure;
pub(crate) mod rollout_debug;
pub(crate) mod spawn;
pub(crate) mod spawn_gate;
pub(crate) mod status;
pub(crate) mod tool_response;

pub(crate) const TOOL_RESULT_CLEARED_MESSAGE: &str = "[Old tool result content cleared]";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SpineControlKind {
    Open,
    Close,
    Next,
}

impl SpineControlKind {
    pub(crate) fn requires_task(self) -> bool {
        matches!(self, Self::Close | Self::Next)
    }

    pub(crate) fn from_tool_name(tool_name: &codex_tools::ToolName) -> Option<Self> {
        if tool_name.namespace.as_deref()
            != Some(crate::tools::handlers::spine_spec::SPINE_NAMESPACE)
        {
            return None;
        }
        match tool_name.name.as_str() {
            crate::tools::handlers::spine_spec::SPINE_OPEN => Some(Self::Open),
            crate::tools::handlers::spine_spec::SPINE_CLOSE => Some(Self::Close),
            crate::tools::handlers::spine_spec::SPINE_NEXT => Some(Self::Next),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CodexSpineProjection {
    pub(crate) spine: SpineProjection,
    pub(crate) context: Vec<ResponseItem>,
}

pub(crate) fn closed_memory_projection_entries(
    rollout: &[RolloutItem],
    spawn_enabled: bool,
) -> Vec<memory_projection::SpinetreeMemoryProjectionEntry> {
    derive_from_rollout_with_features(rollout, true, false, spawn_enabled)
        .spine
        .nodes
        .into_iter()
        .filter_map(|node| {
            if node.kind != codex_spine_core::NodeKind::Task || node.status != NodeStatus::Closed {
                return None;
            }
            let node_id = node.id;
            let body = node.memory?.into_iter().find_map(|slot| match slot {
                MemorySlot::Summary {
                    owner_node, body, ..
                } if owner_node == node_id => Some(body),
                _ => None,
            })?;
            let node_id = node_id.to_string();
            Some(memory_projection::SpinetreeMemoryProjectionEntry {
                summary: node.summary.unwrap_or_else(|| "node".to_string()),
                body: render_memory_artifact(&node_id, &body),
                node_id,
            })
        })
        .collect()
}

pub(crate) fn user_message_projection_entries(
    rollout: &[RolloutItem],
) -> Vec<memory_projection::SpinetreeUserMessageProjectionEntry> {
    let mut next_anchor = 1;
    effective_rollout(rollout)
        .into_iter()
        .filter_map(|(raw_index, item)| {
            let RolloutItem::ResponseItem(item) = item else {
                return None;
            };
            let message = message_from_response_item(raw_index, item);
            if message.role != MessageRole::User {
                return None;
            }
            let entry = memory_projection::SpinetreeUserMessageProjectionEntry {
                anchor: next_anchor,
                body: message.content,
            };
            next_anchor += 1;
            Some(entry)
        })
        .collect()
}

pub(crate) fn derive_from_rollout(rollout: &[RolloutItem]) -> CodexSpineProjection {
    derive_from_rollout_with_features(rollout, true, false, true)
}

pub(crate) fn derive_from_rollout_with_features(
    rollout: &[RolloutItem],
    jit_enabled: bool,
    trim_enabled: bool,
    spawn_enabled: bool,
) -> CodexSpineProjection {
    let effective = effective_rollout(rollout);
    projection_from_effective_rollout(
        &effective,
        rollout,
        jit_enabled,
        trim_enabled,
        spawn_enabled,
        None,
    )
}

pub(crate) fn derive_from_rollout_with_host_history(
    rollout: &[RolloutItem],
    jit_enabled: bool,
    trim_enabled: bool,
    spawn_enabled: bool,
    host_history: &ContextManager,
) -> CodexSpineProjection {
    let effective = effective_rollout(rollout);
    projection_from_effective_rollout(
        &effective,
        rollout,
        jit_enabled,
        trim_enabled,
        spawn_enabled,
        Some(host_history),
    )
}

fn projection_from_effective_rollout(
    effective: &[(usize, &RolloutItem)],
    rollout: &[RolloutItem],
    jit_enabled: bool,
    trim_enabled: bool,
    spawn_enabled: bool,
    host_history: Option<&ContextManager>,
) -> CodexSpineProjection {
    let events = lex_rollout(effective, spawn_enabled);
    let trim = trim_enabled.then(|| TrimProjection::derive(&events));
    let spine = if jit_enabled {
        SpineReducer::derive(&events)
    } else {
        SpineReducer::derive(&[])
    };
    let context = if jit_enabled {
        materialize_context(
            &spine.visible_context,
            rollout,
            trim.as_ref(),
            host_history,
            spawn_enabled,
        )
    } else {
        materialize_trim_only_context(effective, &events, rollout, trim.as_ref(), host_history)
    };
    CodexSpineProjection { spine, context }
}

pub(crate) fn validate_trim_request(
    rollout: &[RolloutItem],
    current_call_id: &str,
    request: &TrimRequest,
) -> Result<(), String> {
    let effective = effective_rollout(rollout);
    let events = lex_rollout(&effective, true);
    // The current trim request is already staged in the rollout, but has no
    // response yet. Its predecessor is the only eligible trim window.
    let current_group = events.iter().rposition(|event| {
        let RolloutEvent::ToolCall(group) = event else {
            return false;
        };
        group
            .calls
            .iter()
            .any(|call| call.call_id == current_call_id && call.name == "spine.trim")
    });
    let Some(current_group) = current_group else {
        return Err("spine.trim failed: current toolcall is unavailable; do not retry".to_string());
    };
    let events_before_current = &events[..current_group];
    let previous_completed_group = events_before_current
        .iter()
        .rev()
        .find(|event| matches!(event, RolloutEvent::ToolCall(group) if group.is_complete()));
    let projection = previous_completed_group
        .map(|event| TrimProjection::derive(std::slice::from_ref(event)))
        .unwrap_or_default();
    projection.validate(request)
}

pub(crate) fn validate_nested_trim_request(
    rollout: &[RolloutItem],
    outer_exec_call_id: &str,
    request: &TrimRequest,
) -> Result<(), String> {
    let effective = effective_rollout(rollout);
    let Some(outer_exec_index) = effective.iter().rposition(|(_, item)| {
        matches!(
            item,
            RolloutItem::ResponseItem(
                item @ ResponseItem::CustomToolCall { call_id, .. }
            ) if call_id == outer_exec_call_id && is_registered_code_mode_exec_request(item)
        )
    }) else {
        return Err(
            "spine.trim failed: outer exec toolcall is unavailable; do not retry".to_string(),
        );
    };
    let events = lex_rollout(&effective[..outer_exec_index], true);
    let previous_completed_group = events
        .iter()
        .rev()
        .find(|event| matches!(event, RolloutEvent::ToolCall(group) if group.is_complete()));
    let projection = previous_completed_group
        .map(|event| TrimProjection::derive(std::slice::from_ref(event)))
        .unwrap_or_default();
    projection.validate(request)
}

pub(crate) fn validate_code_mode_spine_outer_exec(
    rollout: &[RolloutItem],
    call_id: &str,
) -> Result<(), String> {
    let effective = effective_rollout(rollout);
    let Some(index) = effective.iter().rposition(|(_, item)| {
        matches!(
            item,
            RolloutItem::ResponseItem(
                item @ ResponseItem::CustomToolCall {
                    call_id: candidate,
                    ..
                }
            ) if candidate == call_id && is_registered_code_mode_exec_request(item)
        )
    }) else {
        return Err(format!(
            "Code Mode outer exec `{call_id}` is unavailable from the native rollout"
        ));
    };

    let mut first_call = index;
    while first_call > 0
        && matches!(
            effective[first_call - 1].1,
            RolloutItem::ResponseItem(item) if normalized_tool_request(item).is_some()
        )
    {
        first_call -= 1;
    }
    let mut call_end = index + 1;
    while call_end < effective.len()
        && matches!(
            effective[call_end].1,
            RolloutItem::ResponseItem(item) if normalized_tool_request(item).is_some()
        )
    {
        call_end += 1;
    }
    if first_call != index || call_end != index + 1 {
        return Err(
            "Code Mode nested Spine calls require outer exec to be the sole callable item"
                .to_string(),
        );
    }
    Ok(())
}

fn effective_rollout(rollout: &[RolloutItem]) -> Vec<(usize, &RolloutItem)> {
    let mut effective: Vec<(usize, &RolloutItem)> = Vec::new();
    let mut response_ordinal = 0;
    for item in rollout {
        if let RolloutItem::EventMsg(EventMsg::ThreadRolledBack(rollback)) = item {
            let turns = usize::try_from(rollback.num_turns).unwrap_or(usize::MAX);
            if turns == 0 {
                continue;
            }
            let user_boundaries: Vec<_> = effective
                .iter()
                .enumerate()
                .filter_map(|(effective_index, (_, item))| match item {
                    RolloutItem::ResponseItem(item) if is_user_turn_boundary(item) => {
                        Some(effective_index)
                    }
                    RolloutItem::InterAgentCommunication(_) => Some(effective_index),
                    _ => None,
                })
                .collect();
            if let Some(cut) = user_boundaries
                .len()
                .checked_sub(turns)
                .and_then(|position| user_boundaries.get(position))
                .copied()
                .or_else(|| user_boundaries.first().copied())
            {
                let first_user_boundary = user_boundaries.first().copied().unwrap_or(cut);
                effective.truncate(cut);
                // Native rollback trims contextual updates immediately above the removed
                // user-turn boundary. Keep the Spine selected prefix identical to that host
                // boundary so projection cannot reintroduce settings that rollback removed.
                let mut scan = effective.len();
                while scan > first_user_boundary {
                    let Some((_, item)) = effective.get(scan - 1) else {
                        break;
                    };
                    let trim = match item {
                        RolloutItem::ResponseItem(ResponseItem::Message {
                            role, content, ..
                        }) if role == "developer" => is_contextual_dev_message_content(content),
                        RolloutItem::ResponseItem(ResponseItem::Message {
                            role, content, ..
                        }) if role == "user" => is_contextual_user_message_content(content),
                        RolloutItem::EventMsg(EventMsg::TokenCount(_)) => {
                            scan -= 1;
                            continue;
                        }
                        _ => false,
                    };
                    if !trim {
                        break;
                    }
                    effective.remove(scan - 1);
                    scan -= 1;
                }
            }
            continue;
        }
        if is_spine_source_item(item) {
            effective.push((response_ordinal, item));
            response_ordinal += 1;
        } else if matches!(item, RolloutItem::EventMsg(EventMsg::TokenCount(_))) {
            effective.push((response_ordinal, item));
        }
    }
    effective
}

fn is_spine_source_item(item: &RolloutItem) -> bool {
    matches!(
        item,
        RolloutItem::ResponseItem(_)
            | RolloutItem::InterAgentCommunication(_)
            | RolloutItem::Compacted(_)
    )
}

fn lex_rollout(effective: &[(usize, &RolloutItem)], spawn_enabled: bool) -> Vec<RolloutEvent> {
    let mut events = Vec::new();
    let mut index = 0;
    while index < effective.len() {
        let (raw_index, item) = effective[index];
        match item {
            RolloutItem::ResponseItem(response_item) => {
                if let Some((group, consumed)) =
                    completed_tool_group(effective, index, spawn_enabled)
                {
                    events.push(RolloutEvent::ToolCall(group));
                    index += consumed;
                    continue;
                }
                events.push(RolloutEvent::Message(message_from_response_item(
                    raw_index,
                    response_item,
                )));
            }
            RolloutItem::InterAgentCommunication(communication) => {
                events.push(RolloutEvent::Message(message_from_response_item(
                    raw_index,
                    &communication.to_model_input_item(),
                )));
            }
            RolloutItem::Compacted(compacted) => {
                let replacement_history = compacted
                    .replacement_history
                    .as_ref()
                    .map(|items| {
                        items
                            .iter()
                            .enumerate()
                            .map(|(replacement_index, _)| ContextItem::Native {
                                source: NativeItemRef::CompactReplacement {
                                    compact_boundary: RawBoundary(raw_index as u64),
                                    index: u32::try_from(replacement_index).unwrap_or(u32::MAX),
                                },
                            })
                            .collect()
                    })
                    .unwrap_or_else(|| {
                        vec![ContextItem::Message {
                            message: Message {
                                boundary: RawBoundary(raw_index as u64),
                                role: MessageRole::Assistant,
                                content: compacted.message.clone(),
                            },
                            user_anchor: None,
                        }]
                    });
                events.push(RolloutEvent::Compact {
                    boundary: RawBoundary(raw_index as u64),
                    replacement_history,
                });
            }
            RolloutItem::SessionMeta(_)
            | RolloutItem::InterAgentCommunicationMetadata { .. }
            | RolloutItem::TurnContext(_)
            | RolloutItem::WorldState(_)
            | RolloutItem::EventMsg(_) => {}
        }
        index += 1;
    }
    events
}

fn completed_tool_group(
    effective: &[(usize, &RolloutItem)],
    start: usize,
    spawn_enabled: bool,
) -> Option<(ToolCallGroup, usize)> {
    let mut cursor = start;
    let mut leading = Vec::new();
    while let Some((raw_index, RolloutItem::ResponseItem(item))) = effective.get(cursor).copied() {
        if !is_leading_assistant_item(item) {
            break;
        }
        leading.push(message_from_response_item(raw_index, item));
        cursor += 1;
    }

    let first_call = cursor;
    let mut calls = Vec::new();
    while let Some((_, RolloutItem::ResponseItem(item))) = effective.get(cursor).copied() {
        let Some(call) = normalized_tool_request(item) else {
            break;
        };
        calls.push(call);
        cursor += 1;
    }
    if cursor == first_call {
        return None;
    }

    let request_end = cursor;
    let response_start = cursor;
    let mut last_group_index = cursor.saturating_sub(1);
    while let Some((_, RolloutItem::ResponseItem(item))) = effective.get(cursor).copied() {
        let Some(response) = normalized_tool_response(item) else {
            break;
        };
        if !calls.iter().any(|call| call.call_id == response.call_id) {
            break;
        }
        last_group_index = cursor;
        cursor += 1;
    }

    let request_items = effective[first_call..request_end]
        .iter()
        .filter_map(|entry| match entry.1 {
            RolloutItem::ResponseItem(item) => Some(item),
            _ => None,
        })
        .collect::<Vec<_>>();
    let response_items = effective[response_start..cursor]
        .iter()
        .filter_map(|entry| match entry.1 {
            RolloutItem::ResponseItem(item) => Some(item),
            _ => None,
        })
        .collect::<Vec<_>>();

    match code_mode_carrier_verdict(&request_items, &response_items) {
        CarrierGroupVerdict::Absent => {
            for (raw_index, item) in effective[response_start..cursor].iter().copied() {
                let RolloutItem::ResponseItem(item) = item else {
                    unreachable!("response range was prevalidated")
                };
                let response = normalized_tool_response(item).expect("prevalidated response");
                let call_index = calls
                    .iter()
                    .position(|call| call.call_id == response.call_id)
                    .expect("response call id was prevalidated");
                let output_boundary = RawBoundary(raw_index as u64);
                let call = &mut calls[call_index];
                call.outcome = Some(classify_tool_outcome(call, response.output, spawn_enabled));
                call.output = Some(response.output.body.to_text().unwrap_or_default());
                call.output_boundary = Some(output_boundary);
            }
        }
        CarrierGroupVerdict::Valid(analysis) => {
            let (raw_index, _) = effective[response_start];
            let output_boundary = RawBoundary(raw_index as u64);
            let AnalyzedCodeModeCarrier {
                outer_call_id,
                visible_output,
                nested_calls,
            } = analysis;
            let nested = normalize_code_mode_carrier_calls(
                &outer_call_id,
                nested_calls,
                output_boundary,
                spawn_enabled,
            );
            let call = &mut calls[0];
            call.outcome = Some(classify_tool_outcome(call, &visible_output, spawn_enabled));
            call.output = Some(visible_output.body.to_text().unwrap_or_default());
            call.output_boundary = Some(output_boundary);
            calls.extend(nested);
        }
        CarrierGroupVerdict::Corrupt(error) => {
            tracing::error!(%error, "invalid Code Mode Spine carrier group");
            for (raw_index, item) in effective[response_start..cursor].iter().copied() {
                let RolloutItem::ResponseItem(item) = item else {
                    unreachable!("response range was prevalidated")
                };
                let response = normalized_tool_response(item).expect("prevalidated response");
                let call_index = calls
                    .iter()
                    .position(|call| call.call_id == response.call_id)
                    .expect("response call id was prevalidated");
                let call = &mut calls[call_index];
                call.outcome = Some(ToolOutcome::Failed);
                call.output = Some(String::new());
                call.output_boundary = Some(RawBoundary(raw_index as u64));
            }
        }
    }

    let raw_start = effective[start].0;
    let raw_end = effective[last_group_index].0;
    Some((
        ToolCallGroup {
            start: RawBoundary(raw_start as u64),
            end: RawBoundary(raw_end as u64),
            leading_assistant_messages: leading,
            calls,
        },
        last_group_index - start + 1,
    ))
}

// Carrier differences end at the rollout adapter; reducers consume one toolcall model.
fn normalized_tool_request(item: &ResponseItem) -> Option<ToolUse> {
    let (name, namespace, arguments, call_id) = match item {
        ResponseItem::FunctionCall {
            name,
            namespace,
            arguments,
            call_id,
            ..
        } => (name, namespace.as_deref(), arguments, call_id),
        ResponseItem::CustomToolCall {
            name,
            namespace,
            input,
            call_id,
            ..
        } => (name, namespace.as_deref(), input, call_id),
        _ => return None,
    };
    Some(ToolUse {
        call_id: call_id.clone(),
        name: qualified_tool_name(namespace, name),
        arguments: arguments.clone(),
        call_ordinal: None,
        outcome: None,
        output: None,
        output_boundary: None,
    })
}

fn normalized_tool_response(item: &ResponseItem) -> Option<NormalizedToolResponse<'_>> {
    match item {
        ResponseItem::FunctionCallOutput {
            call_id, output, ..
        } => Some(NormalizedToolResponse {
            call_id,
            output,
            output_name: None,
        }),
        ResponseItem::CustomToolCallOutput {
            call_id,
            name,
            output,
            ..
        } => Some(NormalizedToolResponse {
            call_id,
            output,
            output_name: name.as_deref(),
        }),
        _ => None,
    }
}

struct NormalizedToolResponse<'a> {
    call_id: &'a str,
    output: &'a FunctionCallOutputPayload,
    output_name: Option<&'a str>,
}

enum CarrierGroupVerdict {
    Absent,
    Valid(AnalyzedCodeModeCarrier),
    Corrupt(String),
}

struct AnalyzedCodeModeCarrier {
    outer_call_id: String,
    visible_output: FunctionCallOutputPayload,
    nested_calls: Vec<AnalyzedNestedSpineCall>,
}

struct AnalyzedNestedSpineCall {
    invocation_ordinal: u64,
    tool_name: &'static str,
    arguments: String,
    output: String,
    success: bool,
}

fn code_mode_carrier_verdict(
    request_items: &[&ResponseItem],
    response_items: &[&ResponseItem],
) -> CarrierGroupVerdict {
    if !response_items
        .iter()
        .any(|item| is_marked_code_mode_carrier_output(item))
    {
        return CarrierGroupVerdict::Absent;
    }
    if request_items.len() != 1 || !is_registered_code_mode_exec_request(request_items[0]) {
        return CarrierGroupVerdict::Corrupt(
            "marked carrier requires the sole outer exec request".to_string(),
        );
    }
    if response_items.len() != 1 {
        return CarrierGroupVerdict::Corrupt(
            "marked carrier requires exactly one matching output".to_string(),
        );
    }
    let Some(request) = normalized_tool_request(request_items[0]) else {
        return CarrierGroupVerdict::Corrupt("invalid outer exec request".to_string());
    };
    let Some(response) = normalized_tool_response(response_items[0]) else {
        return CarrierGroupVerdict::Corrupt("invalid outer exec output".to_string());
    };
    if request.call_id != response.call_id
        || response.output_name != Some(CODE_MODE_SPINE_CARRIER_MARKER)
    {
        return CarrierGroupVerdict::Corrupt(
            "marked carrier request/output pairing is ambiguous".to_string(),
        );
    }
    match analyze_code_mode_carrier(request.call_id, response.output_name, &response.output.body) {
        Ok(analysis) => CarrierGroupVerdict::Valid(analysis),
        Err(error) => CarrierGroupVerdict::Corrupt(error),
    }
}

fn is_registered_code_mode_exec_request(item: &ResponseItem) -> bool {
    matches!(
        item,
        ResponseItem::CustomToolCall {
            name,
            namespace,
            ..
        } if is_exec_tool_name(&codex_tools::ToolName {
            namespace: namespace.clone(),
            name: name.clone(),
        })
    )
}

fn normalize_code_mode_carrier_calls(
    outer_call_id: &str,
    nested_calls: Vec<AnalyzedNestedSpineCall>,
    output_boundary: RawBoundary,
    spawn_enabled: bool,
) -> Vec<ToolUse> {
    nested_calls
        .into_iter()
        .map(|call| {
            let outcome = if !call.success {
                ToolOutcome::Failed
            } else if call.tool_name == "spine.spawn" && !spawn_enabled {
                ToolOutcome::Unknown
            } else {
                ToolOutcome::Succeeded
            };
            ToolUse {
                call_id: format!("{outer_call_id}:spine:{}", call.invocation_ordinal),
                name: call.tool_name.to_string(),
                arguments: call.arguments,
                call_ordinal: Some(call.invocation_ordinal),
                outcome: Some(outcome),
                output: Some(call.output),
                output_boundary: Some(output_boundary),
            }
        })
        .collect()
}

fn analyze_code_mode_carrier(
    outer_call_id: String,
    output_name: Option<&str>,
    body: &FunctionCallOutputBody,
) -> Result<AnalyzedCodeModeCarrier, String> {
    let carrier = decode_marked_body(output_name, body)?
        .ok_or_else(|| "missing Code Mode Spine carrier".to_string())?;
    let nested_calls = carrier
        .nested_spine_calls
        .into_iter()
        .map(analyze_nested_spine_call)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AnalyzedCodeModeCarrier {
        outer_call_id,
        visible_output: FunctionCallOutputPayload {
            body: carrier.visible_body,
            success: carrier.outer_success,
        },
        nested_calls,
    })
}

fn nested_spine_tool_name(name: NestedSpineToolName) -> &'static str {
    match name {
        NestedSpineToolName::Open => "spine.open",
        NestedSpineToolName::Close => "spine.close",
        NestedSpineToolName::Next => "spine.next",
        NestedSpineToolName::Trim => "spine.trim",
        NestedSpineToolName::Spawn => "spine.spawn",
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NestedOpenArgs {
    goal: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NestedCloseArgs {
    memory: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NestedNextArgs {
    goal: String,
    memory: String,
}

fn analyze_nested_spine_call(call: NestedSpineCallV1) -> Result<AnalyzedNestedSpineCall, String> {
    let expected_success = match call.name {
        NestedSpineToolName::Open => {
            let args: NestedOpenArgs =
                serde_json::from_str(&call.arguments).map_err(|error| error.to_string())?;
            require_non_empty(&args.goal, "spine.open goal")?;
            Some(tool_response::SpineToolResponse::Open)
        }
        NestedSpineToolName::Close => {
            let args: NestedCloseArgs =
                serde_json::from_str(&call.arguments).map_err(|error| error.to_string())?;
            require_non_empty(&args.memory, "spine.close memory")?;
            Some(tool_response::SpineToolResponse::Close)
        }
        NestedSpineToolName::Next => {
            let args: NestedNextArgs =
                serde_json::from_str(&call.arguments).map_err(|error| error.to_string())?;
            require_non_empty(&args.goal, "spine.next goal")?;
            require_non_empty(&args.memory, "spine.next memory")?;
            Some(tool_response::SpineToolResponse::Next)
        }
        NestedSpineToolName::Trim => {
            TrimRequest::parse(&call.arguments)?;
            Some(tool_response::SpineToolResponse::Trim)
        }
        NestedSpineToolName::Spawn => {
            let tasks = spawn::parse_tasks(&call.arguments)?;
            if call.output.success {
                let receipt = spawn::decode_receipt(&call.output.body)
                    .map_err(|error| format!("invalid nested spine.spawn receipt: {error}"))?;
                receipt
                    .validate_for(&tasks)
                    .map_err(|error| error.to_string())?;
            }
            None
        }
    };
    if call.output.success
        && expected_success.is_some_and(|response| !response.is_success_carrier(&call.output.body))
    {
        return Err(format!(
            "invalid nested {} success output",
            nested_spine_tool_name(call.name)
        ));
    }
    Ok(AnalyzedNestedSpineCall {
        invocation_ordinal: call.invocation_ordinal,
        tool_name: nested_spine_tool_name(call.name),
        arguments: call.arguments,
        output: call.output.body,
        success: call.output.success,
    })
}

fn require_non_empty(value: &str, field: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        Err(format!("{field} must not be empty"))
    } else {
        Ok(())
    }
}

fn classify_tool_outcome(
    call: &ToolUse,
    output: &codex_protocol::models::FunctionCallOutputPayload,
    spawn_enabled: bool,
) -> ToolOutcome {
    if call.name == "spine.spawn" {
        if output.success == Some(false) {
            return ToolOutcome::Failed;
        }
        return if spawn_enabled && is_valid_spawn_success_carrier(call, &output.body) {
            ToolOutcome::Succeeded
        } else {
            ToolOutcome::Unknown
        };
    }
    tool_response::SpineToolResponse::outcome(&call.name, output)
}

fn is_valid_spawn_success_carrier(call: &ToolUse, body: &FunctionCallOutputBody) -> bool {
    let FunctionCallOutputBody::Text(body) = body else {
        return false;
    };
    let Ok(tasks) = spawn::parse_tasks(&call.arguments) else {
        return false;
    };
    let Ok(receipt) = spawn::decode_receipt(body) else {
        return false;
    };
    receipt.validate_for(&tasks).is_ok()
}

fn is_leading_assistant_item(item: &ResponseItem) -> bool {
    matches!(
        item,
        ResponseItem::Message { role, .. } if role == "assistant"
    ) || matches!(item, ResponseItem::Reasoning { .. })
}

fn qualified_tool_name(namespace: Option<&str>, name: &str) -> String {
    match namespace {
        Some(namespace) if !namespace.is_empty() => format!("{namespace}.{name}"),
        _ => name.to_string(),
    }
}

fn message_from_response_item(raw_index: usize, item: &ResponseItem) -> Message {
    let (role, content) = match item {
        ResponseItem::Message { role, content, .. } => (
            match role.as_str() {
                "user" if is_contextual_user_message_content(content) => {
                    MessageRole::ContextualUser
                }
                "user" => MessageRole::User,
                "developer" => MessageRole::Developer,
                "system" => MessageRole::System,
                _ => MessageRole::Assistant,
            },
            content
                .iter()
                .filter_map(content_text)
                .collect::<Vec<_>>()
                .join("\n"),
        ),
        _ => (
            MessageRole::Assistant,
            serde_json::to_string(item).unwrap_or_default(),
        ),
    };
    Message {
        boundary: RawBoundary(raw_index as u64),
        role,
        content,
    }
}

fn content_text(item: &ContentItem) -> Option<String> {
    match item {
        ContentItem::InputText { text } | ContentItem::OutputText { text } => Some(text.clone()),
        ContentItem::InputImage { .. } => Some("<image>".to_string()),
    }
}

fn materialize_context(
    context: &[ContextItem],
    rollout: &[RolloutItem],
    trim: Option<&TrimProjection>,
    host_history: Option<&ContextManager>,
    spawn_enabled: bool,
) -> Vec<ResponseItem> {
    let mut materialized = Vec::new();
    for item in context {
        match item {
            ContextItem::Message {
                message,
                user_anchor,
            } => {
                if let Some(mut item) = response_item_at(rollout, message.boundary, host_history) {
                    if is_marked_code_mode_carrier_output(&item) {
                        item = project_code_mode_carrier_item(item, None, rollout);
                    }
                    if let Some(anchor) = user_anchor {
                        tag_user_message(&mut item, *anchor);
                    }
                    materialized.push(item);
                } else {
                    materialized.push(text_message(message.role, message.content.clone()));
                }
            }
            ContextItem::ToolCall(group) => {
                for raw_index in group.start.0..=group.end.0 {
                    if let Some(item) = response_item_at(rollout, RawBoundary(raw_index), None) {
                        materialized.push(project_toolcall_item(
                            item,
                            group,
                            usize::try_from(raw_index).unwrap_or(usize::MAX),
                            trim,
                            spawn_enabled,
                            host_history,
                            rollout,
                        ));
                    }
                }
            }
            ContextItem::SyntheticNode {
                node_id,
                summary,
                status,
            } => materialized.push(text_message(
                MessageRole::Developer,
                format!(
                    "<spine_node id=\"{node_id}\" summary=\"{}\" status=\"{}\" />",
                    escape_attribute(summary),
                    status_name(*status),
                ),
            )),
            ContextItem::MemorySlot(slot) => match slot {
                MemorySlot::User {
                    message, anchor, ..
                } => {
                    // The reducer created this slot from the same immutable rollout.
                    let mut item = response_item_at(rollout, message.boundary, host_history)
                        .unwrap_or_else(|| {
                            panic!(
                                "memory user slot at raw boundary {} has no rollout source",
                                message.boundary.0
                            )
                        });
                    assert!(
                        matches!(&item, ResponseItem::Message { role, .. } if role == "user"),
                        "memory user slot at raw boundary {} resolved to a non-user item",
                        message.boundary.0
                    );
                    tag_user_message(&mut item, *anchor);
                    materialized.push(item);
                }
                MemorySlot::Summary {
                    owner_node, body, ..
                } => materialized.push(text_message(
                    MessageRole::ContextualUser,
                    format!("<spine_memory node_id=\"{owner_node}\">\n{body}\n</spine_memory>"),
                )),
                MemorySlot::SpawnEvidence {
                    owner_node,
                    task,
                    outcome,
                    diagnostic,
                    execution_ref,
                    ..
                } => materialized.push(text_message(
                    MessageRole::ContextualUser,
                    render_spawn_evidence(
                        owner_node,
                        task,
                        *outcome,
                        diagnostic.as_deref(),
                        execution_ref.as_deref(),
                    ),
                )),
            },
            ContextItem::Native { source } => match source {
                NativeItemRef::CompactReplacement {
                    compact_boundary,
                    index,
                } => {
                    if let Some(item) = compact_replacement_at(rollout, *compact_boundary, *index) {
                        materialized.push(item);
                    }
                }
            },
        }
    }
    materialized
}

fn project_toolcall_item(
    item: ResponseItem,
    group: &ToolCallGroup,
    raw_ordinal: usize,
    trim: Option<&TrimProjection>,
    spawn_enabled: bool,
    host_history: Option<&ContextManager>,
    rollout: &[RolloutItem],
) -> ResponseItem {
    let was_code_mode_carrier = is_marked_code_mode_carrier_output(&item);
    let item = project_code_mode_carrier_item(item, Some(group), rollout);
    let mut item = if was_code_mode_carrier {
        item
    } else {
        host_history
            .map(|history| history.canonical_projected_item(&item))
            .unwrap_or(item)
    };
    if spawn_enabled && group.is_complete() {
        if let ResponseItem::FunctionCallOutput {
            call_id, output, ..
        } = &mut item
        {
            if let Some(call) = group
                .calls
                .iter()
                .find(|call| call.call_id == *call_id && call.name == "spine.spawn")
            {
                let conflicting = group.calls.iter().any(|call| {
                    matches!(
                        call.name.as_str(),
                        "spine.open" | "spine.close" | "spine.next"
                    )
                });
                let status = if !conflicting && call.outcome == Some(ToolOutcome::Succeeded) {
                    "success"
                } else {
                    "failure"
                };
                output.body =
                    FunctionCallOutputBody::Text(serde_json::json!({"status": status}).to_string());
                output.success = Some(status == "success");
                return item;
            }
        }
    }

    project_trim_item(item, raw_ordinal, trim)
}

fn materialize_trim_only_context(
    effective: &[(usize, &RolloutItem)],
    events: &[RolloutEvent],
    rollout: &[RolloutItem],
    trim: Option<&TrimProjection>,
    host_history: Option<&ContextManager>,
) -> Vec<ResponseItem> {
    let start = effective
        .iter()
        .rposition(|(_, item)| matches!(item, RolloutItem::Compacted(_)))
        .unwrap_or(0);
    let mut context = Vec::new();
    for (raw_index, item) in effective.iter().skip(start) {
        match item {
            RolloutItem::ResponseItem(item) => {
                let group = events.iter().find_map(|event| {
                    let RolloutEvent::ToolCall(group) = event else {
                        return None;
                    };
                    (group.start.0 <= *raw_index as u64 && *raw_index as u64 <= group.end.0)
                        .then_some(group)
                });
                let was_code_mode_carrier = is_marked_code_mode_carrier_output(item);
                let item = project_code_mode_carrier_item(item.clone(), group, rollout);
                let item = if was_code_mode_carrier {
                    item
                } else {
                    host_history
                        .map(|history| history.canonical_projected_item(&item))
                        .unwrap_or(item)
                };
                context.push(project_trim_item(item, *raw_index, trim))
            }
            RolloutItem::InterAgentCommunication(communication) => {
                context.push(communication.to_model_input_item())
            }
            RolloutItem::Compacted(compacted) => {
                if let Some(replacement) = &compacted.replacement_history {
                    context.extend(replacement.iter().map(|item| {
                        host_history
                            .map(|history| history.canonical_projected_item(item))
                            .unwrap_or_else(|| item.clone())
                    }));
                } else {
                    context.push(text_message(
                        MessageRole::Assistant,
                        compacted.message.clone(),
                    ));
                }
            }
            _ => {}
        }
    }
    if context.is_empty() && !rollout.is_empty() {
        context.extend(
            rollout
                .iter()
                .filter_map(|item| match item {
                    RolloutItem::ResponseItem(item) => Some(
                        host_history
                            .map(|history| history.canonical_projected_item(item))
                            .unwrap_or_else(|| item.clone()),
                    ),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        );
    }
    context
}

fn is_marked_code_mode_carrier_output(item: &ResponseItem) -> bool {
    matches!(
        item,
        ResponseItem::CustomToolCallOutput { name, .. }
            if name.as_deref() == Some(CODE_MODE_SPINE_CARRIER_MARKER)
    )
}

pub(crate) fn is_code_mode_spine_carrier_rollout_item(item: &RolloutItem) -> bool {
    matches!(
        item,
        RolloutItem::ResponseItem(ResponseItem::CustomToolCallOutput { name, .. })
            if name.as_deref() == Some(CODE_MODE_SPINE_CARRIER_MARKER)
    )
}

fn project_code_mode_carrier_item(
    mut item: ResponseItem,
    group: Option<&ToolCallGroup>,
    rollout: &[RolloutItem],
) -> ResponseItem {
    let ResponseItem::CustomToolCallOutput {
        call_id,
        name,
        output,
        ..
    } = &mut item
    else {
        return item;
    };
    if name.as_deref() != Some(CODE_MODE_SPINE_CARRIER_MARKER) {
        return item;
    }
    let verdict = group
        .map(|group| code_mode_carrier_verdict_for_rollout_group(group, rollout))
        .unwrap_or_else(|| {
            CarrierGroupVerdict::Corrupt(
                "marked carrier is outside a completed tool group".to_string(),
            )
        });
    match verdict {
        CarrierGroupVerdict::Valid(analysis) => {
            *output = analysis.visible_output;
            *name = None;
        }
        CarrierGroupVerdict::Absent => {
            tracing::error!(
                call_id = call_id.as_str(),
                "marked carrier was absent from its completed tool group"
            );
            output.body = FunctionCallOutputBody::Text(
                "Code Mode Spine evidence is invalid and was not applied.".to_string(),
            );
            output.success = Some(false);
            *name = None;
        }
        CarrierGroupVerdict::Corrupt(error) => {
            tracing::error!(
                call_id = call_id.as_str(),
                %error,
                "failed to project Code Mode Spine carrier"
            );
            output.body = FunctionCallOutputBody::Text(
                "Code Mode Spine evidence is invalid and was not applied.".to_string(),
            );
            output.success = Some(false);
            *name = None;
        }
    }
    item
}

fn code_mode_carrier_verdict_for_rollout_group(
    group: &ToolCallGroup,
    rollout: &[RolloutItem],
) -> CarrierGroupVerdict {
    let items = (group.start.0..=group.end.0)
        .filter_map(|raw_index| response_item_at(rollout, RawBoundary(raw_index), None))
        .collect::<Vec<_>>();
    let request_items = items
        .iter()
        .filter(|item| normalized_tool_request(item).is_some())
        .collect::<Vec<_>>();
    let response_items = items
        .iter()
        .filter(|item| normalized_tool_response(item).is_some())
        .collect::<Vec<_>>();
    code_mode_carrier_verdict(&request_items, &response_items)
}

fn project_trim_item(
    mut item: ResponseItem,
    raw_ordinal: usize,
    trim: Option<&TrimProjection>,
) -> ResponseItem {
    let (call_id, body) = match &mut item {
        ResponseItem::FunctionCallOutput {
            call_id, output, ..
        } => (call_id, &mut output.body),
        ResponseItem::CustomToolCallOutput {
            call_id, output, ..
        } => (call_id, &mut output.body),
        _ => return item,
    };
    let Some(edit) =
        trim.and_then(|projection| projection.edit(RawBoundary(raw_ordinal as u64), call_id))
    else {
        return item;
    };
    let visible_body = match edit {
        TrimEdit::Tagged { trim_id, body, .. } => format!("[TRIM_ID: {trim_id}]\n{body}"),
        TrimEdit::Snipped => TOOL_RESULT_CLEARED_MESSAGE.to_string(),
        TrimEdit::Sliced(value) => value.clone(),
    };
    *body = FunctionCallOutputBody::Text(visible_body);
    item
}

fn response_item_at(
    rollout: &[RolloutItem],
    boundary: RawBoundary,
    host_history: Option<&ContextManager>,
) -> Option<ResponseItem> {
    let index = usize::try_from(boundary.0).ok()?;
    match rollout
        .iter()
        .filter(|item| is_spine_source_item(item))
        .nth(index)?
    {
        RolloutItem::ResponseItem(item) => Some(
            host_history
                .map(|history| history.canonical_projected_item(item))
                .unwrap_or_else(|| item.clone()),
        ),
        RolloutItem::InterAgentCommunication(communication) => {
            Some(communication.to_model_input_item())
        }
        RolloutItem::Compacted(compacted) => Some(text_message(
            MessageRole::Assistant,
            compacted.message.clone(),
        )),
        _ => None,
    }
}

fn compact_replacement_at(
    rollout: &[RolloutItem],
    boundary: RawBoundary,
    replacement_index: u32,
) -> Option<ResponseItem> {
    let raw_index = usize::try_from(boundary.0).ok()?;
    let replacement_index = usize::try_from(replacement_index).ok()?;
    let RolloutItem::Compacted(compacted) = rollout
        .iter()
        .filter(|item| is_spine_source_item(item))
        .nth(raw_index)?
    else {
        return None;
    };
    compacted
        .replacement_history
        .as_ref()?
        .get(replacement_index)
        .cloned()
}

fn tag_user_message(item: &mut ResponseItem, anchor: u64) {
    let ResponseItem::Message { role, content, .. } = item else {
        return;
    };
    if role != "user" {
        return;
    }
    let prefix = format!("[U{anchor}]\n");
    if let Some(ContentItem::InputText { text }) = content
        .iter_mut()
        .find(|item| matches!(item, ContentItem::InputText { .. }))
    {
        text.insert_str(0, &prefix);
    } else {
        content.insert(0, ContentItem::InputText { text: prefix });
    }
}

fn text_message(role: MessageRole, text: String) -> ResponseItem {
    ResponseItem::Message {
        id: None,
        role: match role {
            MessageRole::User => "user",
            MessageRole::ContextualUser => "user",
            MessageRole::Assistant => "assistant",
            MessageRole::Developer => "developer",
            MessageRole::System => "system",
        }
        .to_string(),
        content: vec![ContentItem::InputText { text }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    }
}

fn render_memory_artifact(node_id: &str, body: &str) -> String {
    format!("# Spine Memory {node_id}\n\n## Node Memory\n{body}")
}

fn render_spawn_evidence(
    owner_node: &codex_spine_core::NodeId,
    task: &codex_spine_core::SpawnTask,
    outcome: codex_spine_core::SpawnOutcome,
    diagnostic: Option<&str>,
    execution_ref: Option<&str>,
) -> String {
    format!(
        "<spine_spawn_evidence node_id=\"{owner_node}\">\n{}\n</spine_spawn_evidence>",
        render_spawn_evidence_body(task, outcome, diagnostic, execution_ref)
    )
}

fn render_spawn_evidence_body(
    task: &codex_spine_core::SpawnTask,
    outcome: codex_spine_core::SpawnOutcome,
    diagnostic: Option<&str>,
    execution_ref: Option<&str>,
) -> String {
    serde_json::to_string_pretty(&serde_json::json!({
        "summary": task.summary,
        "prompt": task.prompt,
        "outcome": outcome,
        "diagnostic": diagnostic,
        "execution_ref": execution_ref,
    }))
    .expect("spawn evidence fields serialize")
}

fn status_name(status: NodeStatus) -> &'static str {
    match status {
        NodeStatus::Live => "live",
        NodeStatus::Opened => "opened",
        NodeStatus::Closed => "closed",
        NodeStatus::Compacted => "compacted",
    }
}

fn escape_attribute(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[cfg(test)]
mod tests;
