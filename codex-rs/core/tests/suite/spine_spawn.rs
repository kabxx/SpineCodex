use anyhow::Context;
use anyhow::Result;
use codex_core::test_support::submit_interrupt_then_mailbox_for_test;
use codex_features::Feature;
use codex_protocol::AgentPath;
use codex_protocol::models::ResponseItem;
use codex_protocol::openai_models::ToolMode;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::MULTI_AGENT_MODE_OPEN_TAG;
use codex_protocol::protocol::MultiAgentVersion;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutLine;
use codex_protocol::request_user_input::RequestUserInputAnswer;
use codex_protocol::request_user_input::RequestUserInputResponse;
use codex_protocol::user_input::UserInput;
use codex_spine_core::SPINE_SPAWN_RESULT_SCHEMA;
use codex_spine_core::SpawnReceipt;
use core_test_support::responses::ResponseMock;
use core_test_support::responses::ResponsesRequest;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_custom_tool_call;
use core_test_support::responses::ev_function_call;
use core_test_support::responses::ev_function_call_with_namespace;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::ev_shell_command_call;
use core_test_support::responses::mount_response_once_match;
use core_test_support::responses::mount_sse_once_match;
use core_test_support::responses::sse;
use core_test_support::responses::sse_response;
use core_test_support::responses::start_mock_server;
use core_test_support::test_codex::TestCodex;
use core_test_support::test_codex::TestCodexBuilder;
use core_test_support::test_codex::spine_test_codex;
use core_test_support::wait_for_event;
use core_test_support::wait_for_event_match;
use serde_json::Value;
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::Instant;
use tokio::time::sleep;
use wiremock::ResponseTemplate;

const SPAWN_NAMESPACE: &str = "spine";
const SPAWN_TOOL: &str = "spawn";
const SPAWN_CALL_ID: &str = "spawn-lifecycle-call";
const FIRST_PARENT_PROMPT: &str = "run the lifecycle spawn batch";
const SECOND_PARENT_PROMPT: &str = "run the replacement spawn batch";
const BRANCH_PROMPT_MARKER: &str = "You are a spawned execution branch.";
const CONTINUE_AFTER_FAILURE_MESSAGE: &str = concat!(
    "Continue the same assignment from this branch's existing context. Preserve useful progress ",
    "from the failed turn, finish the remaining work, and return the required terminal memory."
);
const CORRECTION_MESSAGE: &str = concat!(
    "This spawned execution branch remains active. Continue exactly the declared\n",
    "assignment and use its declared shared blackboard to collaborate with peer\n",
    "branches. When the assignment is complete or precisely bounded, return exactly\n",
    "one non-empty,\n",
    "tool-free assistant final response containing terminal memory. That response\n",
    "ends this branch execution."
);
const CODE_MODE_SPINE_CARRIER_MARKER: &str = "spine.code_mode.output.v1";

fn body_contains(request: &wiremock::Request, text: &str) -> bool {
    decoded_body(request)
        .and_then(|body| serde_json::from_slice::<Value>(&body).ok())
        .is_some_and(|body| body.to_string().contains(text))
}

fn body_contains_json_text(request: &wiremock::Request, text: &str) -> bool {
    let json_fragment = serde_json::to_string(text)
        .expect("serialize text to JSON")
        .trim_matches('"')
        .to_string();
    decoded_body(request)
        .and_then(|body| serde_json::from_slice::<Value>(&body).ok())
        .is_some_and(|body| body.to_string().contains(&json_fragment))
}

fn child_task_marker(request: &wiremock::Request, marker: &str) -> bool {
    decoded_body(request)
        .and_then(|body| serde_json::from_slice::<Value>(&body).ok())
        .is_some_and(|body| body_has_child_task_marker(&body, marker))
}

fn body_has_child_task_marker(body: &Value, marker: &str) -> bool {
    body.get("input")
        .and_then(Value::as_array)
        .is_some_and(|items| {
            items.iter().any(|item| {
                item.get("type").and_then(Value::as_str) == Some("message")
                    && item.get("role").and_then(Value::as_str) == Some("user")
                    && item
                        .get("content")
                        .and_then(Value::as_array)
                        .is_some_and(|content| {
                            content.iter().any(|part| {
                                part.get("text")
                                    .and_then(Value::as_str)
                                    .is_some_and(|text| {
                                        text.contains(BRANCH_PROMPT_MARKER) && text.contains(marker)
                                    })
                            })
                        })
            })
        })
}

fn has_function_call_output(request: &wiremock::Request, call_id: &str) -> bool {
    decoded_body(request)
        .and_then(|body| serde_json::from_slice::<Value>(&body).ok())
        .is_some_and(|body| {
            body.get("input")
                .and_then(Value::as_array)
                .is_some_and(|items| {
                    items.iter().any(|item| {
                        item.get("type").and_then(Value::as_str) == Some("function_call_output")
                            && item.get("call_id").and_then(Value::as_str) == Some(call_id)
                    })
                })
        })
}

fn is_parent_spawn_request(request: &wiremock::Request) -> bool {
    body_contains(request, FIRST_PARENT_PROMPT)
        && !body_contains(request, BRANCH_PROMPT_MARKER)
        && !has_function_call_output(request, SPAWN_CALL_ID)
}

fn decoded_body(request: &wiremock::Request) -> Option<Vec<u8>> {
    let is_zstd = request
        .headers
        .get("content-encoding")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|entry| entry.trim().eq_ignore_ascii_case("zstd"))
        });
    if is_zstd {
        zstd::stream::decode_all(std::io::Cursor::new(&request.body)).ok()
    } else {
        Some(request.body.clone())
    }
}

fn persisted_function_call_output(test: &TestCodex, call_id: &str) -> Result<String> {
    let path = test
        .codex
        .rollout_path()
        .context("test thread is missing its rollout path")?;
    let rollout = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read rollout {}", path.display()))?;
    rollout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(serde_json::from_str::<RolloutLine>)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .find_map(|line| match line.item {
            RolloutItem::ResponseItem(ResponseItem::FunctionCallOutput {
                call_id: output_call_id,
                output,
                ..
            }) if output_call_id == call_id => output.body.to_text(),
            _ => None,
        })
        .with_context(|| format!("rollout is missing function output for `{call_id}`"))
}

fn spawn_args_for(tasks: &[(&str, &str)]) -> String {
    let tasks = tasks
        .iter()
        .map(|(summary, prompt)| json!({"summary": summary, "prompt": prompt}))
        .collect::<Vec<_>>();
    json!({"tasks": tasks}).to_string()
}

fn spawn_args(first_marker: &str, second_marker: &str) -> String {
    spawn_args_for(&[("first", first_marker), ("second", second_marker)])
}

fn spine_builder() -> TestCodexBuilder {
    spine_test_codex()
        .with_spine_spawn()
        .with_model("koffing")
        .with_config(|config| {
            config.spine_spawn.max_concurrent_threads_per_session = 3;
            config.multi_agent_v2.max_concurrent_threads_per_session = 17;
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(0);
            config.model_provider.supports_websockets = false;
        })
}

fn metadata_v2_spine_builder() -> TestCodexBuilder {
    spine_builder()
        .with_model("gpt-5.6-sol")
        .with_model_info_override("gpt-5.6-sol", |model_info| {
            model_info.multi_agent_version = Some(MultiAgentVersion::V2);
        })
        .with_config(|config| {
            config.multi_agent_v2.root_agent_usage_hint_text =
                Some("metadata-v2-root-usage-hint".to_string());
            config.multi_agent_v2.subagent_usage_hint_text =
                Some("metadata-v2-subagent-usage-hint".to_string());
        })
}

fn multi_agent_v2_spine_builder() -> TestCodexBuilder {
    metadata_v2_spine_builder().with_config(|config| {
        config
            .features
            .enable(Feature::MultiAgentV2)
            .expect("enable MultiAgentV2");
    })
}

fn nested_spawn_builder() -> TestCodexBuilder {
    metadata_v2_spine_builder()
        .with_model_info_override("gpt-5.6-sol", |model_info| {
            model_info.use_responses_lite = true;
            model_info.tool_mode = Some(ToolMode::CodeModeOnly);
            model_info
                .experimental_supported_tools
                .push("test_sync_tool".to_string());
        })
        .with_config(|config| {
            config
                .features
                .enable(Feature::CodeMode)
                .expect("enable CodeMode");
        })
}

async fn wait_for_request(
    mock_response: &ResponseMock,
    label: &str,
    predicate: impl Fn(&core_test_support::responses::ResponsesRequest) -> bool,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if mock_response.requests().iter().any(&predicate) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for mocked Responses request `{label}`");
        }
        sleep(Duration::from_millis(10)).await;
    }
}

async fn choose_spawn_failure_action(test: &TestCodex, answers: &[&str]) -> Result<()> {
    let request = wait_for_event_match(&test.codex, |event| match event {
        EventMsg::RequestUserInput(request) if request.call_id.contains(":failure_gate:") => {
            Some(request.clone())
        }
        _ => None,
    })
    .await;
    assert_eq!(request.questions.len(), 1);
    let question = &request.questions[0];
    assert_eq!(question.id, "spine_spawn_failure_action");
    assert!(question.question.contains("spawned branches failed"));
    assert_eq!(
        question
            .options
            .as_ref()
            .expect("spawn failure gate options")
            .iter()
            .map(|option| option.label.as_str())
            .collect::<Vec<_>>(),
        vec!["Continue", "Retry", "Abandon"]
    );

    let response = RequestUserInputResponse {
        answers: HashMap::from([(
            question.id.clone(),
            RequestUserInputAnswer {
                answers: answers.iter().map(ToString::to_string).collect(),
            },
        )]),
    };
    test.codex
        .submit(Op::UserInputAnswer {
            id: request.turn_id,
            response,
        })
        .await?;
    Ok(())
}

async fn submit_turn_with_spawn_failure_action(
    test: &TestCodex,
    prompt: &str,
    answers: &[&str],
) -> Result<bool> {
    test.codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: prompt.to_string(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: Default::default(),
        })
        .await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnStarted(_))
    })
    .await;
    choose_spawn_failure_action(test, answers).await?;
    let followup_gate = wait_for_event_match(&test.codex, |event| match event {
        EventMsg::TurnComplete(_) => Some(None),
        EventMsg::RequestUserInput(request) if request.call_id.contains(":failure_gate:") => {
            Some(Some(request.clone()))
        }
        _ => None,
    })
    .await;
    if let Some(request) = followup_gate {
        let question = &request.questions[0];
        test.codex
            .submit(Op::UserInputAnswer {
                id: request.turn_id,
                response: RequestUserInputResponse {
                    answers: HashMap::from([(
                        question.id.clone(),
                        RequestUserInputAnswer {
                            answers: vec!["Abandon".to_string()],
                        },
                    )]),
                },
            })
            .await?;
        wait_for_event(&test.codex, |event| {
            matches!(event, EventMsg::TurnComplete(_))
        })
        .await;
        return Ok(false);
    }
    Ok(true)
}

async fn wait_for_code_mode_first_output(test: &TestCodex, outer_exec_call_id: &str) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if codex_core::test_support::code_mode_is_waiting_for_first_output(
            &test.codex,
            outer_exec_call_id,
        ) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for Code Mode first-output settlement `{outer_exec_call_id}`"
            );
        }
        sleep(Duration::from_millis(10)).await;
    }
}

fn parent_projection_request(
    mock_response: &ResponseMock,
    first_memory: &str,
    second_memory: &str,
) -> core_test_support::responses::ResponsesRequest {
    mock_response
        .requests()
        .into_iter()
        .find(|request| {
            request.body_contains_text(first_memory)
                && request.body_contains_text(second_memory)
                && !request.body_contains_text(BRANCH_PROMPT_MARKER)
        })
        .expect("parent follow-up should contain the completed spawn projection")
}

fn unique_matching_request(
    mock_response: &ResponseMock,
    label: &str,
    predicate: impl Fn(&ResponsesRequest) -> bool,
) -> ResponsesRequest {
    let mut matches = mock_response
        .requests()
        .into_iter()
        .filter(predicate)
        .collect::<Vec<_>>();
    assert_eq!(
        matches.len(),
        1,
        "unique mocked request `{label}`: {}",
        matches
            .iter()
            .map(|request| request.body_json().to_string())
            .collect::<Vec<_>>()
            .join("\n---\n")
    );
    matches.remove(0)
}

fn first_matching_request(
    mock_response: &ResponseMock,
    predicate: impl Fn(&ResponsesRequest) -> bool,
) -> ResponsesRequest {
    mock_response
        .requests()
        .into_iter()
        .find(predicate)
        .expect("matching mocked request")
}

fn has_namespace(request: &ResponsesRequest, namespace: &str) -> bool {
    request
        .body_json()
        .get("tools")
        .and_then(Value::as_array)
        .is_some_and(|tools| {
            tools.iter().any(|tool| {
                tool.get("type").and_then(Value::as_str) == Some("namespace")
                    && tool.get("name").and_then(Value::as_str) == Some(namespace)
            })
        })
}

async fn build_reverse_completion_fixture(
    first_delay: Duration,
    second_delay: Duration,
) -> Result<(
    wiremock::MockServer,
    TestCodex,
    ResponseMock,
    ResponseMock,
    ResponseMock,
    ResponseMock,
)> {
    let server = start_mock_server().await;
    let parent_spawn = mount_sse_once_match(
        &server,
        is_parent_spawn_request,
        sse(vec![
            ev_response_created("parent-spawn-response"),
            ev_function_call_with_namespace(
                SPAWN_CALL_ID,
                SPAWN_NAMESPACE,
                SPAWN_TOOL,
                &spawn_args("first-child-marker", "second-child-marker"),
            ),
            ev_completed("parent-spawn-response"),
        ]),
    )
    .await;
    let first_child = mount_response_once_match(
        &server,
        |request: &wiremock::Request| child_task_marker(request, "first-child-marker"),
        sse_response(sse(vec![
            ev_response_created("first-child-response"),
            ev_assistant_message("first-child-message", "first memory"),
            ev_completed("first-child-response"),
        ]))
        .set_delay(first_delay),
    )
    .await;
    let second_child = mount_response_once_match(
        &server,
        |request: &wiremock::Request| child_task_marker(request, "second-child-marker"),
        sse_response(sse(vec![
            ev_response_created("second-child-response"),
            ev_assistant_message("second-child-message", "second memory"),
            ev_completed("second-child-response"),
        ]))
        .set_delay(second_delay),
    )
    .await;
    let parent_followup = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            body_contains(request, "first memory")
                && body_contains(request, "second memory")
                && !body_contains(request, BRANCH_PROMPT_MARKER)
        },
        sse(vec![
            ev_response_created("parent-followup-response"),
            ev_assistant_message("parent-followup-message", "parent done"),
            ev_completed("parent-followup-response"),
        ]),
    )
    .await;
    let test = metadata_v2_spine_builder().build(&server).await?;
    assert!(test.config.features.enabled(Feature::SpineSpawn));
    assert!(!test.config.features.enabled(Feature::MultiAgentV2));
    let selected_model = test
        .config
        .model_catalog
        .as_ref()
        .and_then(|catalog| {
            catalog
                .models
                .iter()
                .find(|model| model.slug == "gpt-5.6-sol")
        })
        .expect("selected model metadata should be present in the test model catalog");
    assert_eq!(
        selected_model.multi_agent_version,
        Some(MultiAgentVersion::V2)
    );
    assert_eq!(
        parent_spawn.requests().len(),
        0,
        "fixture must not issue a request before submit_turn"
    );
    Ok((
        server,
        test,
        parent_spawn,
        first_child,
        second_child,
        parent_followup,
    ))
}

#[test]
fn spine_spawn_respects_metadata_v2_when_multi_agent_feature_is_off() -> Result<()> {
    const TEST_STACK_SIZE_BYTES: usize = 16 * 1024 * 1024;

    let handle = std::thread::Builder::new()
        .name("spine_spawn_prefix_trim".to_string())
        .stack_size(TEST_STACK_SIZE_BYTES)
        .spawn(|| -> Result<()> {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(TEST_STACK_SIZE_BYTES)
                .enable_all()
                .build()?;
            runtime.block_on(spawn_starts_batch_concurrently_and_orders_reverse_completion_impl())
        })?;

    match handle.join() {
        Ok(result) => result,
        Err(_) => Err(anyhow::anyhow!("spine.spawn prefix test thread panicked")),
    }
}

async fn spawn_starts_batch_concurrently_and_orders_reverse_completion_impl() -> Result<()> {
    let (server, test, parent_spawn, first_child, second_child, parent_followup) =
        build_reverse_completion_fixture(Duration::from_millis(500), Duration::from_millis(100))
            .await?;

    let observe_overlap = async {
        if let Err(error) = wait_for_request(&first_child, "first child", |request| {
            request.body_contains_text("first-child-marker")
                && request.body_contains_text(BRANCH_PROMPT_MARKER)
        })
        .await
        {
            let requests = server.received_requests().await.unwrap_or_default();
            let request_bodies = requests
                .iter()
                .filter_map(decoded_body)
                .filter_map(|body| String::from_utf8(body).ok())
                .collect::<Vec<_>>();
            anyhow::bail!(
                "{error}; parent requests: {}; parent tool output: {:?}; received: {} {:?}",
                parent_spawn.requests().len(),
                parent_followup.function_call_output_text(SPAWN_CALL_ID),
                requests.len(),
                request_bodies,
            );
        }
        wait_for_request(&second_child, "second child", |request| {
            request.body_contains_text("second-child-marker")
                && request.body_contains_text(BRANCH_PROMPT_MARKER)
        })
        .await?;
        assert!(
            parent_followup
                .requests()
                .iter()
                .all(|request| !request.body_contains_text("first memory")),
            "parent must not publish a receipt while the slower child is running"
        );
        assert_eq!(
            test.thread_manager.list_thread_ids().await.len(),
            3,
            "root plus both transaction children must be live together"
        );
        Result::<()>::Ok(())
    };
    tokio::try_join!(test.submit_turn(FIRST_PARENT_PROMPT), observe_overlap)?;

    let parent_request =
        parent_projection_request(&parent_followup, "first memory", "second memory");
    let rendered = parent_request.body_json().to_string();
    let first_position = rendered.find("first memory");
    let second_position = rendered.find("second memory");
    assert!(
        first_position < second_position,
        "parent projection must preserve task ordinal order: first={first_position:?}, second={second_position:?}, request={rendered}"
    );

    let parent_first_request =
        unique_matching_request(&parent_spawn, "initial parent", |request| {
            request.body_contains_text(FIRST_PARENT_PROMPT)
                && !request.body_contains_text(BRANCH_PROMPT_MARKER)
                && request.function_call_output_text(SPAWN_CALL_ID).is_none()
        });
    let child_first_request = first_matching_request(&first_child, |request| {
        request.body_contains_text("first-child-marker")
            && request.body_contains_text(BRANCH_PROMPT_MARKER)
    });
    let parent_first_body = parent_first_request.body_json();
    let child_first_body = child_first_request.body_json();
    assert!(!has_namespace(&parent_first_request, "collaboration"));
    assert!(!has_namespace(&child_first_request, "collaboration"));
    assert!(parent_first_request.body_contains_text("metadata-v2-root-usage-hint"));
    assert!(!parent_first_request.body_contains_text("metadata-v2-subagent-usage-hint"));
    assert!(child_first_request.body_contains_text("metadata-v2-root-usage-hint"));
    assert!(child_first_request.body_contains_text("metadata-v2-subagent-usage-hint"));
    for request in [&parent_first_request, &child_first_request] {
        assert!(request.body_contains_text(MULTI_AGENT_MODE_OPEN_TAG));
    }
    assert!(
        child_first_request.body_contains_text(FIRST_PARENT_PROMPT),
        "FullHistory child must retain semantic access to the parent turn"
    );
    assert!(
        child_first_request.body_contains_text("first-child-marker"),
        "child task envelope must be appended to the inherited history"
    );
    let child_input = child_first_body["input"]
        .as_array()
        .expect("child request input must be an array");
    assert!(
        !child_input
            .iter()
            .any(|item| { item.get("call_id").and_then(Value::as_str) == Some(SPAWN_CALL_ID) }),
        "child must not inherit the current spine.spawn request or synthetic output"
    );
    let parent_input = parent_first_body["input"]
        .as_array()
        .expect("parent request input must be an array");
    let exact_lcp = parent_input
        .iter()
        .zip(child_input)
        .take_while(|(parent, child)| parent == child)
        .count();
    assert_eq!(
        exact_lcp,
        parent_input.len(),
        "child must preserve the complete parent request input as an exact prefix"
    );
    let parent_cache_key = parent_first_body["prompt_cache_key"]
        .as_str()
        .expect("parent request must expose prompt_cache_key")
        .to_string();
    let child_cache_key = child_first_body["prompt_cache_key"]
        .as_str()
        .expect("child request must expose prompt_cache_key")
        .to_string();
    assert_eq!(
        parent_cache_key, child_cache_key,
        "Spine spawn child must share the parent's prompt cache affinity"
    );
    eprintln!(
        "SPINE_SPAWN_CONTEXT_DIAGNOSTIC {}",
        json!({
            "semantic_parent_prompt": true,
            "parent_input_items": parent_input.len(),
            "child_input_items": child_input.len(),
            "exact_lcp_items": exact_lcp,
            "parent_prompt_cache_key": parent_cache_key,
            "child_prompt_cache_key": child_cache_key,
            "cache_affinity_shared": parent_cache_key == child_cache_key,
            "parent_request_prefix_exact": exact_lcp == parent_input.len(),
            "inherited_in_flight_spawn_call": false,
            "provider_cache_hit_claim": false,
        })
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_child_abandon_returns_diagnostic_without_salvage() -> Result<()> {
    let server = start_mock_server().await;
    let parent_prompt = "run a spawn batch and abandon failed branches";
    mount_sse_once_match(
        &server,
        move |request: &wiremock::Request| {
            body_contains(request, parent_prompt) && !body_contains(request, BRANCH_PROMPT_MARKER)
        },
        sse(vec![
            ev_response_created("gate-parent-response"),
            ev_function_call_with_namespace(
                SPAWN_CALL_ID,
                SPAWN_NAMESPACE,
                SPAWN_TOOL,
                &spawn_args("gate-first-child-marker", "gate-second-child-marker"),
            ),
            ev_completed("gate-parent-response"),
        ]),
    )
    .await;
    let _failed_child = mount_response_once_match(
        &server,
        |request: &wiremock::Request| child_task_marker(request, "gate-first-child-marker"),
        ResponseTemplate::new(503).set_body_json(json!({
            "error": {
                "code": "server_is_overloaded",
                "message": "selected model is at capacity"
            }
        })),
    )
    .await;
    let _completed_child = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| child_task_marker(request, "gate-second-child-marker"),
        sse(vec![
            ev_response_created("gate-second-response"),
            ev_assistant_message("gate-second-message", "second child completed"),
            ev_completed("gate-second-response"),
        ]),
    )
    .await;
    let parent_followup = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            !body_contains(request, BRANCH_PROMPT_MARKER)
                && body_contains(request, "child errored")
                && body_contains(request, "second child completed")
        },
        sse(vec![
            ev_response_created("gate-parent-followup"),
            ev_assistant_message("gate-parent-final", "abandoned failure observed"),
            ev_completed("gate-parent-followup"),
        ]),
    )
    .await;

    let test = spine_builder().build(&server).await?;
    assert!(submit_turn_with_spawn_failure_action(&test, parent_prompt, &["Abandon"]).await?);

    let requests = server.received_requests().await.unwrap_or_default();
    assert_eq!(
        requests
            .iter()
            .filter(|request| child_task_marker(request, "gate-first-child-marker"))
            .count(),
        1,
        "Abandon must not issue a salvage or continuation request"
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| child_task_marker(request, "gate-second-child-marker"))
            .count(),
        1
    );
    assert_eq!(parent_followup.requests().len(), 1);
    let receipt: SpawnReceipt =
        serde_json::from_str(&persisted_function_call_output(&test, SPAWN_CALL_ID)?)?;
    assert_eq!(receipt.results.len(), 2);
    assert_eq!(
        receipt.results[0].outcome,
        codex_spine_core::SpawnOutcome::Errored
    );
    assert!(
        receipt.results[0].memory_body.contains("child errored"),
        "unexpected failed receipt: {:?}",
        receipt.results[0]
    );
    assert_eq!(
        receipt.results[1].outcome,
        codex_spine_core::SpawnOutcome::Completed
    );
    assert_eq!(receipt.results[1].memory_body, "second child completed");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_failure_gate_waits_for_every_branch_to_settle() -> Result<()> {
    let server = start_mock_server().await;
    let parent_prompt = "wait for every branch before showing the failure gate";
    mount_sse_once_match(
        &server,
        move |request: &wiremock::Request| {
            body_contains(request, parent_prompt) && !body_contains(request, BRANCH_PROMPT_MARKER)
        },
        sse(vec![
            ev_response_created("settlement-gate-parent"),
            ev_function_call_with_namespace(
                SPAWN_CALL_ID,
                SPAWN_NAMESPACE,
                SPAWN_TOOL,
                &spawn_args("settlement-fast-failure", "settlement-delayed-success"),
            ),
            ev_completed("settlement-gate-parent"),
        ]),
    )
    .await;
    mount_response_once_match(
        &server,
        |request: &wiremock::Request| child_task_marker(request, "settlement-fast-failure"),
        ResponseTemplate::new(503).set_body_json(json!({
            "error": {
                "code": "server_is_overloaded",
                "message": "fast branch failed"
            }
        })),
    )
    .await;

    let delayed_arrived = std::sync::Arc::new(tokio::sync::Notify::new());
    let responder_arrived = std::sync::Arc::clone(&delayed_arrived);
    let delayed_release =
        std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let responder_release = std::sync::Arc::clone(&delayed_release);
    let delayed_success = sse_response(sse(vec![
        ev_response_created("settlement-delayed-response"),
        ev_assistant_message("settlement-delayed-message", "delayed branch completed"),
        ev_completed("settlement-delayed-response"),
    ]));
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path_regex(".*/responses$"))
        .and(|request: &wiremock::Request| child_task_marker(request, "settlement-delayed-success"))
        .respond_with(move |_: &wiremock::Request| {
            responder_arrived.notify_one();
            let (released, release_signal) = &*responder_release;
            let guard = released.lock().expect("delayed response release lock");
            let guard = release_signal
                .wait_while(guard, |released| !*released)
                .expect("delayed response release wait");
            drop(guard);
            delayed_success.clone()
        })
        .up_to_n_times(1)
        .mount(&server)
        .await;
    let parent_followup = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            !body_contains(request, BRANCH_PROMPT_MARKER)
                && body_contains(request, "child errored")
                && body_contains(request, "delayed branch completed")
        },
        sse(vec![
            ev_response_created("settlement-parent-followup"),
            ev_assistant_message("settlement-parent-final", "settled gate observed"),
            ev_completed("settlement-parent-followup"),
        ]),
    )
    .await;

    let test = spine_builder().build(&server).await?;
    test.codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: parent_prompt.to_string(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: Default::default(),
        })
        .await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnStarted(_))
    })
    .await;
    tokio::time::timeout(Duration::from_secs(5), delayed_arrived.notified())
        .await
        .context("delayed branch never reached its response gate")?;

    let premature_gate = tokio::time::timeout(
        Duration::from_millis(200),
        wait_for_event_match(&test.codex, |event| match event {
            EventMsg::RequestUserInput(request) if request.call_id.contains(":failure_gate:") => {
                Some(request.clone())
            }
            _ => None,
        }),
    )
    .await;
    let gate_was_premature = premature_gate.is_ok();
    {
        let (released, release_signal) = &*delayed_release;
        let mut released = released.lock().expect("delayed response release lock");
        *released = true;
        release_signal.notify_all();
    }
    assert!(
        !gate_was_premature,
        "the Gate appeared before the delayed success branch settled"
    );
    choose_spawn_failure_action(&test, &["Abandon"]).await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;

    assert_eq!(parent_followup.requests().len(), 1);
    let receipt: SpawnReceipt =
        serde_json::from_str(&persisted_function_call_output(&test, SPAWN_CALL_ID)?)?;
    assert_eq!(
        receipt.results[0].outcome,
        codex_spine_core::SpawnOutcome::Errored
    );
    assert_eq!(receipt.results[1].memory_body, "delayed branch completed");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupted_child_enters_the_failure_gate_without_interrupting_parent() -> Result<()> {
    let server = start_mock_server().await;
    let parent_prompt = "interrupt one spawned branch and abandon it at the gate";
    mount_sse_once_match(
        &server,
        move |request: &wiremock::Request| {
            body_contains(request, parent_prompt) && !body_contains(request, BRANCH_PROMPT_MARKER)
        },
        sse(vec![
            ev_response_created("child-interrupt-parent"),
            ev_function_call_with_namespace(
                SPAWN_CALL_ID,
                SPAWN_NAMESPACE,
                SPAWN_TOOL,
                &spawn_args("child-interrupt-target", "child-interrupt-success"),
            ),
            ev_completed("child-interrupt-parent"),
        ]),
    )
    .await;
    let child_shell_args = json!({
        "command": "sleep 60",
        "timeout_ms": 60_000
    })
    .to_string();
    let interrupted_child = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            child_task_marker(request, "child-interrupt-target")
                && body_contains(request, "You are: first")
        },
        sse(vec![
            ev_response_created("child-interrupt-pause-response"),
            ev_function_call(
                "child-interrupt-pause-call",
                "shell_command",
                &child_shell_args,
            ),
            ev_completed("child-interrupt-pause-response"),
        ]),
    )
    .await;
    let completed_child = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            child_task_marker(request, "child-interrupt-success")
                && body_contains(request, "You are: second")
        },
        sse(vec![
            ev_response_created("child-interrupt-success-response"),
            ev_assistant_message(
                "child-interrupt-success-message",
                "sibling completed before gate",
            ),
            ev_completed("child-interrupt-success-response"),
        ]),
    )
    .await;
    let parent_followup = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            !body_contains(request, BRANCH_PROMPT_MARKER)
                && body_contains(request, "child interrupted")
                && body_contains(request, "sibling completed before gate")
        },
        sse(vec![
            ev_response_created("child-interrupt-parent-followup"),
            ev_assistant_message(
                "child-interrupt-parent-final",
                "child interruption observed",
            ),
            ev_completed("child-interrupt-parent-followup"),
        ]),
    )
    .await;

    let test = spine_builder().build(&server).await?;
    test.codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: parent_prompt.to_string(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: Default::default(),
        })
        .await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnStarted(_))
    })
    .await;
    wait_for_request(&interrupted_child, "child to interrupt", |request| {
        body_has_child_task_marker(&request.body_json(), "child-interrupt-target")
            && request.body_contains_text("You are: first")
    })
    .await?;
    let interrupted_request = interrupted_child
        .requests()
        .into_iter()
        .find(|request| {
            body_has_child_task_marker(&request.body_json(), "child-interrupt-target")
                && request.body_contains_text("You are: first")
        })
        .context("interrupted child request")?;
    let interrupted_thread_id = codex_protocol::ThreadId::from_string(
        interrupted_request.body_json()["client_metadata"]["thread_id"]
            .as_str()
            .context("interrupted child request thread id")?,
    )?;
    let child_thread = test
        .thread_manager
        .get_thread(interrupted_thread_id)
        .await?;
    wait_for_event(&child_thread, |event| {
        matches!(event, EventMsg::ExecCommandBegin(_))
    })
    .await;
    child_thread.submit(Op::Interrupt).await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if child_thread.agent_status().await == AgentStatus::Interrupted {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("interrupted child did not reach its terminal status")?;
    wait_for_request(&completed_child, "sibling to complete", |request| {
        request.body_contains_text("You are: second")
    })
    .await?;
    let completed_request = completed_child
        .requests()
        .into_iter()
        .find(|request| request.body_contains_text("You are: second"))
        .context("completed sibling request")?;
    let completed_thread_id = codex_protocol::ThreadId::from_string(
        completed_request.body_json()["client_metadata"]["thread_id"]
            .as_str()
            .context("completed sibling request thread id")?,
    )?;
    let completed_thread = test.thread_manager.get_thread(completed_thread_id).await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if matches!(
                completed_thread.agent_status().await,
                AgentStatus::Completed(_)
            ) {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("successful sibling did not reach its terminal status")?;

    choose_spawn_failure_action(&test, &["Abandon"]).await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;

    assert_eq!(parent_followup.requests().len(), 1);
    assert_ne!(test.codex.agent_status().await, AgentStatus::Interrupted);
    let receipt: SpawnReceipt =
        serde_json::from_str(&persisted_function_call_output(&test, SPAWN_CALL_ID)?)?;
    assert_eq!(
        receipt.results[0].outcome,
        codex_spine_core::SpawnOutcome::Aborted
    );
    assert_eq!(
        receipt.results[1].memory_body,
        "sibling completed before gate"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupting_the_failure_gate_tears_down_every_child() -> Result<()> {
    let server = start_mock_server().await;
    let parent_prompt = "run a spawn batch and interrupt its failure gate";
    mount_sse_once_match(
        &server,
        move |request: &wiremock::Request| {
            body_contains(request, parent_prompt) && !body_contains(request, BRANCH_PROMPT_MARKER)
        },
        sse(vec![
            ev_response_created("gate-interrupt-parent"),
            ev_function_call_with_namespace(
                SPAWN_CALL_ID,
                SPAWN_NAMESPACE,
                SPAWN_TOOL,
                &spawn_args("gate-interrupt-failed", "gate-interrupt-completed"),
            ),
            ev_completed("gate-interrupt-parent"),
        ]),
    )
    .await;
    mount_response_once_match(
        &server,
        |request: &wiremock::Request| child_task_marker(request, "gate-interrupt-failed"),
        ResponseTemplate::new(503).set_body_json(json!({
            "error": {
                "code": "server_is_overloaded",
                "message": "selected model is at capacity"
            }
        })),
    )
    .await;
    mount_sse_once_match(
        &server,
        |request: &wiremock::Request| child_task_marker(request, "gate-interrupt-completed"),
        sse(vec![
            ev_response_created("gate-interrupt-child"),
            ev_assistant_message("gate-interrupt-message", "completed before the gate"),
            ev_completed("gate-interrupt-child"),
        ]),
    )
    .await;

    let test = spine_builder().build(&server).await?;
    test.codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: parent_prompt.to_string(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: Default::default(),
        })
        .await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnStarted(_))
    })
    .await;
    wait_for_event(&test.codex, |event| {
        matches!(
            event,
            EventMsg::RequestUserInput(request)
                if request.call_id.contains(":failure_gate:")
        )
    })
    .await;
    assert_eq!(test.thread_manager.list_thread_ids().await.len(), 3);

    test.codex.submit(Op::Interrupt).await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnAborted(_))
    })
    .await;
    assert_eq!(
        test.thread_manager.list_thread_ids().await.len(),
        1,
        "TurnAborted must follow complete failure-gate teardown"
    );
    assert_eq!(test.codex.agent_status().await, AgentStatus::Interrupted);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_child_continue_resumes_the_same_thread() -> Result<()> {
    let server = start_mock_server().await;
    let parent_prompt = "run a spawn batch and continue the failed branch";
    let user_guidance = "preserve the partial analysis from the failed turn";
    mount_sse_once_match(
        &server,
        move |request: &wiremock::Request| {
            body_contains(request, parent_prompt) && !body_contains(request, BRANCH_PROMPT_MARKER)
        },
        sse(vec![
            ev_response_created("continue-parent-response"),
            ev_function_call_with_namespace(
                SPAWN_CALL_ID,
                SPAWN_NAMESPACE,
                SPAWN_TOOL,
                &spawn_args("continue-first-marker", "continue-second-marker"),
            ),
            ev_completed("continue-parent-response"),
        ]),
    )
    .await;
    let failed_child = mount_response_once_match(
        &server,
        |request: &wiremock::Request| {
            child_task_marker(request, "continue-first-marker")
                && !body_contains(request, CONTINUE_AFTER_FAILURE_MESSAGE)
        },
        ResponseTemplate::new(503).set_body_json(json!({
            "error": {
                "code": "server_is_overloaded",
                "message": "selected model is at capacity"
            }
        })),
    )
    .await;
    let continued_child = mount_sse_once_match(
        &server,
        move |request: &wiremock::Request| {
            body_contains(request, CONTINUE_AFTER_FAILURE_MESSAGE)
                && body_contains(request, user_guidance)
        },
        sse(vec![
            ev_response_created("continued-child-response"),
            ev_assistant_message("continued-child-message", "continued branch memory"),
            ev_completed("continued-child-response"),
        ]),
    )
    .await;
    let completed_child = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| child_task_marker(request, "continue-second-marker"),
        sse(vec![
            ev_response_created("continue-second-response"),
            ev_assistant_message("continue-second-message", "untouched success memory"),
            ev_completed("continue-second-response"),
        ]),
    )
    .await;
    let parent_followup = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            !body_contains(request, BRANCH_PROMPT_MARKER)
                && body_contains(request, "continued branch memory")
                && body_contains(request, "untouched success memory")
        },
        sse(vec![
            ev_response_created("continue-parent-followup"),
            ev_assistant_message("continue-parent-final", "continued failure observed"),
            ev_completed("continue-parent-followup"),
        ]),
    )
    .await;

    let test = spine_builder().build(&server).await?;
    let completed_after_continue = submit_turn_with_spawn_failure_action(
        &test,
        parent_prompt,
        &["Continue", &format!("user_note: {user_guidance}")],
    )
    .await?;

    if !completed_after_continue {
        let requests = server.received_requests().await.unwrap_or_default();
        eprintln!(
            "requests after failed Continue:\n{}",
            requests
                .iter()
                .filter_map(decoded_body)
                .filter_map(|body| serde_json::from_slice::<Value>(&body).ok())
                .map(|body| {
                    json!({
                        "prompt_cache_key": body["prompt_cache_key"],
                        "has_branch_marker": body.to_string().contains(BRANCH_PROMPT_MARKER),
                        "has_continue_marker": body.to_string().contains("Continue the same assignment"),
                        "input": body["input"],
                    })
                    .to_string()
                })
                .collect::<Vec<_>>()
                .join("\n---\n")
        );
        eprintln!(
            "failed={} continued={} completed={} parent={}",
            failed_child.requests().len(),
            continued_child.requests().len(),
            completed_child.requests().len(),
            parent_followup.requests().len()
        );
    }
    assert!(
        completed_after_continue,
        "Continue should resolve the failed branch"
    );

    let requests = server.received_requests().await.unwrap_or_default();
    let initial_requests = requests
        .iter()
        .filter(|request| {
            child_task_marker(request, "continue-first-marker")
                && !body_contains(request, CONTINUE_AFTER_FAILURE_MESSAGE)
        })
        .collect::<Vec<_>>();
    let continued_requests = requests
        .iter()
        .filter(|request| body_contains(request, CONTINUE_AFTER_FAILURE_MESSAGE))
        .collect::<Vec<_>>();
    assert_eq!(initial_requests.len(), 1);
    assert_eq!(continued_requests.len(), 1);
    assert!(body_contains(continued_requests[0], user_guidance));
    assert_eq!(
        requests
            .iter()
            .filter(|request| child_task_marker(request, "continue-second-marker"))
            .count(),
        1,
        "the successful branch must not run again"
    );
    assert_eq!(parent_followup.requests().len(), 1);
    let initial_body: Value = serde_json::from_slice(
        &decoded_body(initial_requests[0]).expect("initial child request body"),
    )?;
    let continued_body: Value = serde_json::from_slice(
        &decoded_body(continued_requests[0]).expect("continued child request body"),
    )?;
    assert_eq!(
        initial_body["prompt_cache_key"], continued_body["prompt_cache_key"],
        "Continue must reuse the failed child thread and cache affinity"
    );
    assert_eq!(
        initial_body["client_metadata"]["thread_id"],
        continued_body["client_metadata"]["thread_id"],
        "Continue must submit a new turn to the same failed child thread"
    );
    let receipt: SpawnReceipt =
        serde_json::from_str(&persisted_function_call_output(&test, SPAWN_CALL_ID)?)?;
    assert!(
        receipt
            .results
            .iter()
            .all(|result| result.outcome == codex_spine_core::SpawnOutcome::Completed)
    );
    assert_eq!(receipt.results[0].memory_body, "continued branch memory");
    assert_eq!(receipt.results[1].memory_body, "untouched success memory");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_continue_returns_to_the_gate_for_the_remaining_failure() -> Result<()> {
    let server = start_mock_server().await;
    let parent_prompt = "continue a failed branch that fails again";
    mount_sse_once_match(
        &server,
        move |request: &wiremock::Request| {
            body_contains(request, parent_prompt) && !body_contains(request, BRANCH_PROMPT_MARKER)
        },
        sse(vec![
            ev_response_created("repeat-gate-parent"),
            ev_function_call_with_namespace(
                SPAWN_CALL_ID,
                SPAWN_NAMESPACE,
                SPAWN_TOOL,
                &spawn_args("repeat-gate-first", "repeat-gate-second"),
            ),
            ev_completed("repeat-gate-parent"),
        ]),
    )
    .await;
    mount_response_once_match(
        &server,
        |request: &wiremock::Request| {
            child_task_marker(request, "repeat-gate-first")
                && !body_contains(request, CONTINUE_AFTER_FAILURE_MESSAGE)
        },
        ResponseTemplate::new(503).set_body_json(json!({
            "error": {"code": "server_is_overloaded", "message": "first failure"}
        })),
    )
    .await;
    mount_response_once_match(
        &server,
        |request: &wiremock::Request| body_contains(request, CONTINUE_AFTER_FAILURE_MESSAGE),
        ResponseTemplate::new(503).set_body_json(json!({
            "error": {"code": "server_is_overloaded", "message": "second failure"}
        })),
    )
    .await;
    mount_sse_once_match(
        &server,
        |request: &wiremock::Request| child_task_marker(request, "repeat-gate-second"),
        sse(vec![
            ev_response_created("repeat-gate-success"),
            ev_assistant_message("repeat-gate-success-message", "stable success memory"),
            ev_completed("repeat-gate-success"),
        ]),
    )
    .await;
    mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            !body_contains(request, BRANCH_PROMPT_MARKER)
                && has_function_call_output(request, SPAWN_CALL_ID)
        },
        sse(vec![
            ev_response_created("repeat-gate-parent-followup"),
            ev_assistant_message("repeat-gate-parent-final", "repeated gate observed"),
            ev_completed("repeat-gate-parent-followup"),
        ]),
    )
    .await;

    let test = spine_builder().build(&server).await?;
    assert!(
        !submit_turn_with_spawn_failure_action(&test, parent_prompt, &["Continue"]).await?,
        "a second failure must reopen the Gate before the test abandons it"
    );
    let receipt: SpawnReceipt =
        serde_json::from_str(&persisted_function_call_output(&test, SPAWN_CALL_ID)?)?;
    assert_eq!(
        receipt.results[0].outcome,
        codex_spine_core::SpawnOutcome::Errored
    );
    assert_eq!(receipt.results[1].memory_body, "stable success memory");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_child_retry_starts_a_fresh_branch() -> Result<()> {
    let server = start_mock_server().await;
    let parent_prompt = "run a spawn batch and retry the failed branch";
    let user_guidance = "use the fallback source on this retry";
    mount_sse_once_match(
        &server,
        move |request: &wiremock::Request| {
            body_contains(request, parent_prompt) && !body_contains(request, BRANCH_PROMPT_MARKER)
        },
        sse(vec![
            ev_response_created("retry-parent-response"),
            ev_function_call_with_namespace(
                SPAWN_CALL_ID,
                SPAWN_NAMESPACE,
                SPAWN_TOOL,
                &spawn_args("retry-first-marker", "retry-second-marker"),
            ),
            ev_completed("retry-parent-response"),
        ]),
    )
    .await;

    let attempt = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let responder_attempt = std::sync::Arc::clone(&attempt);
    let retry_success = sse_response(sse(vec![
        ev_response_created("retried-child-response"),
        ev_assistant_message("retried-child-message", "retried branch memory"),
        ev_completed("retried-child-response"),
    ]));
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path_regex(".*/responses$"))
        .and(|request: &wiremock::Request| child_task_marker(request, "retry-first-marker"))
        .respond_with(move |_: &wiremock::Request| {
            if responder_attempt.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                ResponseTemplate::new(503).set_body_json(json!({
                    "error": {
                        "code": "server_is_overloaded",
                        "message": "selected model is at capacity"
                    }
                }))
            } else {
                retry_success.clone()
            }
        })
        .expect(2)
        .mount(&server)
        .await;
    mount_sse_once_match(
        &server,
        |request: &wiremock::Request| child_task_marker(request, "retry-second-marker"),
        sse(vec![
            ev_response_created("retry-second-response"),
            ev_assistant_message("retry-second-message", "retry untouched success"),
            ev_completed("retry-second-response"),
        ]),
    )
    .await;
    let parent_followup = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            !body_contains(request, BRANCH_PROMPT_MARKER)
                && body_contains(request, "retried branch memory")
                && body_contains(request, "retry untouched success")
        },
        sse(vec![
            ev_response_created("retry-parent-followup"),
            ev_assistant_message("retry-parent-final", "retried failure observed"),
            ev_completed("retry-parent-followup"),
        ]),
    )
    .await;

    let test = spine_builder().build(&server).await?;
    assert!(
        submit_turn_with_spawn_failure_action(
            &test,
            parent_prompt,
            &["Retry", &format!("user_note: {user_guidance}")],
        )
        .await?
    );

    let requests = server.received_requests().await.unwrap_or_default();
    let retry_requests = requests
        .iter()
        .filter(|request| child_task_marker(request, "retry-first-marker"))
        .collect::<Vec<_>>();
    assert_eq!(retry_requests.len(), 2);
    assert_eq!(
        retry_requests
            .iter()
            .filter(|request| body_contains(request, user_guidance))
            .count(),
        1,
        "Retry guidance must apply only to the fresh attempt"
    );
    assert!(
        retry_requests
            .iter()
            .all(|request| !body_contains(request, CONTINUE_AFTER_FAILURE_MESSAGE)),
        "Retry must replay the original assignment rather than continuing the old thread"
    );
    let retry_thread_ids = retry_requests
        .iter()
        .map(|request| {
            decoded_body(request)
                .and_then(|body| serde_json::from_slice::<Value>(&body).ok())
                .and_then(|body| {
                    body["client_metadata"]["thread_id"]
                        .as_str()
                        .map(str::to_string)
                })
                .expect("retry request thread id")
        })
        .collect::<Vec<_>>();
    assert_ne!(
        retry_thread_ids[0], retry_thread_ids[1],
        "Retry must create a fresh child thread"
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| child_task_marker(request, "retry-second-marker"))
            .count(),
        1,
        "the successful branch must not run again"
    );
    assert_eq!(parent_followup.requests().len(), 1);
    let receipt: SpawnReceipt =
        serde_json::from_str(&persisted_function_call_output(&test, SPAWN_CALL_ID)?)?;
    assert!(
        receipt
            .results
            .iter()
            .all(|result| result.outcome == codex_spine_core::SpawnOutcome::Completed)
    );
    assert_eq!(receipt.results[0].memory_body, "retried branch memory");
    assert_eq!(receipt.results[1].memory_body, "retry untouched success");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_failed_children_share_one_abandon_gate() -> Result<()> {
    let server = start_mock_server().await;
    let parent_prompt = "run a spawn batch where every branch fails";
    mount_sse_once_match(
        &server,
        move |request: &wiremock::Request| {
            body_contains(request, parent_prompt) && !body_contains(request, BRANCH_PROMPT_MARKER)
        },
        sse(vec![
            ev_response_created("all-failed-parent-response"),
            ev_function_call_with_namespace(
                SPAWN_CALL_ID,
                SPAWN_NAMESPACE,
                SPAWN_TOOL,
                &spawn_args("all-failed-first", "all-failed-second"),
            ),
            ev_completed("all-failed-parent-response"),
        ]),
    )
    .await;
    for marker in ["all-failed-first", "all-failed-second"] {
        mount_response_once_match(
            &server,
            move |request: &wiremock::Request| child_task_marker(request, marker),
            ResponseTemplate::new(503).set_body_json(json!({
                "error": {
                    "code": "server_is_overloaded",
                    "message": format!("{marker} is at capacity")
                }
            })),
        )
        .await;
    }
    let parent_followup = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            !body_contains(request, BRANCH_PROMPT_MARKER)
                && has_function_call_output(request, SPAWN_CALL_ID)
        },
        sse(vec![
            ev_response_created("all-failed-parent-followup"),
            ev_assistant_message("all-failed-parent-final", "all failures observed"),
            ev_completed("all-failed-parent-followup"),
        ]),
    )
    .await;

    let test = spine_builder().build(&server).await?;
    assert!(submit_turn_with_spawn_failure_action(&test, parent_prompt, &["Abandon"]).await?);

    assert_eq!(parent_followup.requests().len(), 1);
    let receipt: SpawnReceipt =
        serde_json::from_str(&persisted_function_call_output(&test, SPAWN_CALL_ID)?)?;
    assert_eq!(receipt.results.len(), 2);
    assert!(
        receipt
            .results
            .iter()
            .all(|result| result.outcome == codex_spine_core::SpawnOutcome::Errored)
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn responses_lite_exec_batches_ordinary_tools_with_nested_spine_spawn() -> Result<()> {
    let server = start_mock_server().await;
    let parent_prompt = "batch ordinary tools with nested Spine spawn";
    let code = r#"// @exec: {"yield_time_ms": 30000}
const syncArgs = () => ({
  sleep_after_ms: 50,
  barrier: {
    id: "spine-code-mode-parallel-spawn",
    participants: 2,
    timeout_ms: 10_000,
  },
});
const [left, right, spawned] = await Promise.all([
  tools.test_sync_tool(syncArgs()),
  tools.test_sync_tool(syncArgs()),
  tools.spine__spawn({
    tasks: [
      {summary: "first", prompt: "nested-first-child-marker"},
      {summary: "second", prompt: "nested-second-child-marker"},
    ],
  }),
]);
text(JSON.stringify({left, right, spawned}));
"#;
    let parent_exec = mount_sse_once_match(
        &server,
        move |request: &wiremock::Request| {
            body_contains(request, parent_prompt)
                && !body_contains(request, BRANCH_PROMPT_MARKER)
                && !body_contains(request, "exec-nested-spawn")
        },
        sse(vec![
            ev_response_created("nested-spawn-parent-response"),
            ev_custom_tool_call("exec-nested-spawn", "exec", code),
            ev_completed("nested-spawn-parent-response"),
        ]),
    )
    .await;
    let first_child = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            child_task_marker(request, "nested-first-child-marker")
                && !body_contains(request, "exec-nested-spawn")
        },
        sse(vec![
            ev_response_created("nested-spawn-first-response"),
            ev_assistant_message("nested-spawn-first-message", "nested first memory"),
            ev_completed("nested-spawn-first-response"),
        ]),
    )
    .await;
    let second_child = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            child_task_marker(request, "nested-second-child-marker")
                && !body_contains(request, "exec-nested-spawn")
        },
        sse(vec![
            ev_response_created("nested-spawn-second-response"),
            ev_assistant_message("nested-spawn-second-message", "nested second memory"),
            ev_completed("nested-spawn-second-response"),
        ]),
    )
    .await;
    let parent_followup = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            body_contains(request, "nested first memory")
                && body_contains(request, "nested second memory")
                && !body_contains(request, BRANCH_PROMPT_MARKER)
        },
        sse(vec![
            ev_response_created("nested-spawn-followup-response"),
            ev_assistant_message("nested-spawn-followup-message", "nested parent done"),
            ev_completed("nested-spawn-followup-response"),
        ]),
    )
    .await;

    let mut builder = nested_spawn_builder();
    let test = builder.build(&server).await?;

    test.submit_turn(parent_prompt).await?;
    test.codex.flush_rollout().await?;

    assert_eq!(parent_exec.requests().len(), 1);
    let first_child_requests = first_child
        .requests()
        .into_iter()
        .filter(|request| {
            body_has_child_task_marker(&request.body_json(), "nested-first-child-marker")
        })
        .collect::<Vec<_>>();
    let first_child_request_ids = first_child_requests
        .iter()
        .map(|request| {
            let body = request.body_json();
            (
                body["client_metadata"]["thread_id"].clone(),
                body["client_metadata"]["turn_id"].clone(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        first_child_requests.len(),
        1,
        "unexpected first child requests: {first_child_request_ids:?}"
    );
    let second_child_requests = second_child
        .requests()
        .into_iter()
        .filter(|request| {
            body_has_child_task_marker(&request.body_json(), "nested-second-child-marker")
        })
        .collect::<Vec<_>>();
    assert_eq!(second_child_requests.len(), 1);
    assert_eq!(parent_followup.requests().len(), 1);

    for child_request in first_child_requests.iter().chain(&second_child_requests) {
        assert!(child_request.body_contains_text(parent_prompt));
        assert!(
            !child_request.body_contains_text("spine-code-mode-parallel-spawn"),
            "child history must stop before the outer exec request: {}",
            child_request.body_json()
        );
    }

    let followup = parent_followup.single_request();
    let visible_output = followup
        .custom_tool_call_output("exec-nested-spawn")
        .get("output")
        .and_then(Value::as_array)
        .context("exec output should preserve content items")?
        .iter()
        .filter_map(|item| item.get("text").and_then(Value::as_str))
        .collect::<String>();
    assert!(
        visible_output.contains(r#""left":"ok""#),
        "{visible_output}"
    );
    assert!(
        visible_output.contains(r#""right":"ok""#),
        "{visible_output}"
    );
    assert!(
        visible_output.contains(r#""spawned":"Spine spawn accepted.""#),
        "{visible_output}"
    );
    let followup_body = followup.body_json().to_string();
    assert!(!followup_body.contains(CODE_MODE_SPINE_CARRIER_MARKER));
    assert!(!followup_body.contains(SPINE_SPAWN_RESULT_SCHEMA));
    assert!(followup_body.contains("nested first memory"));
    assert!(followup_body.contains("nested second memory"));

    let rollout_path = test.codex.rollout_path().context("rollout path")?;
    let rollout = std::fs::read_to_string(&rollout_path)?;
    let carrier = rollout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(serde_json::from_str::<RolloutLine>)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .find_map(|line| match line.item {
            RolloutItem::ResponseItem(ResponseItem::CustomToolCallOutput {
                call_id,
                name,
                output,
                ..
            }) if call_id == "exec-nested-spawn"
                && name.as_deref() == Some(CODE_MODE_SPINE_CARRIER_MARKER) =>
            {
                output.body.to_text()
            }
            _ => None,
        })
        .context("raw outer exec output should contain the marked carrier")?;
    let carrier: Value = serde_json::from_str(&carrier)?;
    let calls = carrier["nested_spine_calls"]
        .as_array()
        .context("carrier nested calls")?;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0]["invocation_ordinal"], 0);
    assert_eq!(calls[0]["name"], "spawn");
    assert_eq!(calls[0]["output"]["success"], true);
    let receipt: SpawnReceipt = serde_json::from_str(
        calls[0]["output"]["body"]
            .as_str()
            .context("nested spawn receipt body")?,
    )?;
    assert_eq!(receipt.schema, SPINE_SPAWN_RESULT_SCHEMA);
    assert_eq!(receipt.results.len(), 2);
    assert_eq!(receipt.results[0].memory_body, "nested first memory");
    assert_eq!(receipt.results[1].memory_body, "nested second memory");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_nested_spawn_returns_to_its_parent_without_a_user_gate() -> Result<()> {
    let server = start_mock_server().await;
    let parent_prompt = "run a nested Spine spawn with one failed child";
    let code = r#"// @exec: {"yield_time_ms": 30000}
const spawned = await tools.spine__spawn({
  tasks: [
    {summary: "failed", prompt: "nested-failure-child-marker"},
    {summary: "success", prompt: "nested-failure-success-marker"},
  ],
});
text(String(spawned));
"#;
    mount_sse_once_match(
        &server,
        move |request: &wiremock::Request| {
            body_contains(request, parent_prompt)
                && !body_contains(request, BRANCH_PROMPT_MARKER)
                && !body_contains(request, "exec-nested-failure")
        },
        sse(vec![
            ev_response_created("nested-failure-parent-response"),
            ev_custom_tool_call("exec-nested-failure", "exec", code),
            ev_completed("nested-failure-parent-response"),
        ]),
    )
    .await;
    let failed_child = mount_response_once_match(
        &server,
        |request: &wiremock::Request| child_task_marker(request, "nested-failure-child-marker"),
        ResponseTemplate::new(503).set_body_json(json!({
            "error": {
                "code": "server_is_overloaded",
                "message": "nested child forced failure"
            }
        })),
    )
    .await;
    let successful_child = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| child_task_marker(request, "nested-failure-success-marker"),
        sse(vec![
            ev_response_created("nested-failure-success-response"),
            ev_assistant_message(
                "nested-failure-success-message",
                "nested sibling success memory",
            ),
            ev_completed("nested-failure-success-response"),
        ]),
    )
    .await;
    let parent_followup = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            !body_contains(request, BRANCH_PROMPT_MARKER)
                && body_contains(request, "child errored")
                && body_contains(request, "nested sibling success memory")
        },
        sse(vec![
            ev_response_created("nested-failure-parent-followup"),
            ev_assistant_message("nested-failure-parent-final", "nested failure handled"),
            ev_completed("nested-failure-parent-followup"),
        ]),
    )
    .await;

    let mut builder = nested_spawn_builder();
    let test = builder.build(&server).await?;
    test.codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: parent_prompt.to_string(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: Default::default(),
        })
        .await?;

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = test.codex.next_event().await?;
            match event.msg {
                EventMsg::RequestUserInput(request)
                    if request.call_id.contains(":failure_gate:") =>
                {
                    anyhow::bail!("nested spawn unexpectedly requested a user failure action")
                }
                EventMsg::TurnComplete(_) => return Ok::<_, anyhow::Error>(()),
                _ => {}
            }
        }
    })
    .await
    .context("nested failure turn did not complete without a Gate")??;
    test.codex.flush_rollout().await?;

    assert_eq!(failed_child.requests().len(), 1);
    assert_eq!(successful_child.requests().len(), 1);
    assert_eq!(parent_followup.requests().len(), 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupting_nested_spawn_tears_down_children() -> Result<()> {
    let server = start_mock_server().await;
    let parent_prompt = "interrupt nested Spine spawn";
    let code = r#"// @exec: {"yield_time_ms": 100}
await tools.spine__spawn({
  tasks: [
    {summary: "first", prompt: "nested-cancel-first-marker"},
    {summary: "second", prompt: "nested-cancel-second-marker"},
  ],
});
"#;
    mount_sse_once_match(
        &server,
        move |request: &wiremock::Request| {
            body_contains(request, parent_prompt) && !body_contains(request, BRANCH_PROMPT_MARKER)
        },
        sse(vec![
            ev_response_created("nested-cancel-parent-response"),
            ev_custom_tool_call("exec-nested-cancel", "exec", code),
            ev_completed("nested-cancel-parent-response"),
        ]),
    )
    .await;
    let first_child = mount_response_once_match(
        &server,
        |request: &wiremock::Request| child_task_marker(request, "nested-cancel-first-marker"),
        sse_response(sse(vec![
            ev_response_created("nested-cancel-first-response"),
            ev_assistant_message("nested-cancel-first-message", "too late"),
            ev_completed("nested-cancel-first-response"),
        ]))
        .set_delay(Duration::from_secs(30)),
    )
    .await;
    let second_child = mount_response_once_match(
        &server,
        |request: &wiremock::Request| child_task_marker(request, "nested-cancel-second-marker"),
        sse_response(sse(vec![
            ev_response_created("nested-cancel-second-response"),
            ev_assistant_message("nested-cancel-second-message", "too late"),
            ev_completed("nested-cancel-second-response"),
        ]))
        .set_delay(Duration::from_secs(30)),
    )
    .await;
    let mut builder = nested_spawn_builder();
    let test = builder.build(&server).await?;

    test.codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: parent_prompt.to_string(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: Default::default(),
        })
        .await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnStarted(_))
    })
    .await;
    wait_for_request(&first_child, "nested cancel first child", |request| {
        body_has_child_task_marker(&request.body_json(), "nested-cancel-first-marker")
    })
    .await?;
    wait_for_request(&second_child, "nested cancel second child", |request| {
        body_has_child_task_marker(&request.body_json(), "nested-cancel-second-marker")
    })
    .await?;
    wait_for_code_mode_first_output(&test, "exec-nested-cancel").await?;
    assert_eq!(
        test.thread_manager.list_thread_ids().await.len(),
        3,
        "both nested spawn children must remain active after the exec yield deadline"
    );
    test.codex.submit(Op::Interrupt).await?;

    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnAborted(_))
    })
    .await;
    let ids = test.thread_manager.list_thread_ids().await;
    assert_eq!(
        ids.len(),
        1,
        "parent emitted TurnAborted before nested spawn teardown completed: {ids:?}"
    );
    assert_eq!(
        test.codex.agent_status().await,
        AgentStatus::Interrupted,
        "TurnAborted must leave the parent Interrupted"
    );

    test.codex.flush_rollout().await?;
    let rollout_path = test.codex.rollout_path().context("rollout path")?;
    let rollout = std::fs::read_to_string(&rollout_path)?;
    let persisted_carrier = rollout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(serde_json::from_str::<RolloutLine>)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .any(|line| {
            matches!(
                line.item,
                RolloutItem::ResponseItem(ResponseItem::CustomToolCallOutput {
                    call_id,
                    name,
                    ..
                }) if call_id == "exec-nested-cancel"
                    && name.as_deref() == Some(CODE_MODE_SPINE_CARRIER_MARKER)
            )
        });
    assert!(
        !persisted_carrier,
        "interrupted yielded exec must not persist a Spine carrier"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn intermediate_message_is_corrected_once_and_never_reaches_parent_model() -> Result<()> {
    let server = start_mock_server().await;
    mount_sse_once_match(
        &server,
        is_parent_spawn_request,
        sse(vec![
            ev_response_created("parent-spawn-response"),
            ev_function_call_with_namespace(
                SPAWN_CALL_ID,
                SPAWN_NAMESPACE,
                SPAWN_TOOL,
                &spawn_args("corrected-child-marker", "ordinary-child-marker"),
            ),
            ev_completed("parent-spawn-response"),
        ]),
    )
    .await;
    let corrected_child = mount_response_once_match(
        &server,
        |request: &wiremock::Request| child_task_marker(request, "corrected-child-marker"),
        sse_response(sse(vec![
            ev_response_created("corrected-child-first-response"),
            ev_shell_command_call("child-yield-call", "true"),
            ev_completed("corrected-child-first-response"),
        ]))
        .set_delay(Duration::from_millis(300)),
    )
    .await;
    let corrected_child_followup = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            has_function_call_output(request, "child-yield-call")
                && body_contains_json_text(request, CORRECTION_MESSAGE)
        },
        sse(vec![
            ev_response_created("corrected-child-final-response"),
            ev_assistant_message("corrected-child-final-message", "corrected child memory"),
            ev_completed("corrected-child-final-response"),
        ]),
    )
    .await;
    mount_response_once_match(
        &server,
        |request: &wiremock::Request| child_task_marker(request, "ordinary-child-marker"),
        sse_response(sse(vec![
            ev_response_created("ordinary-child-response"),
            ev_assistant_message("ordinary-child-message", "ordinary child memory"),
            ev_completed("ordinary-child-response"),
        ]))
        .set_delay(Duration::from_millis(450)),
    )
    .await;
    let parent_followup = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            body_contains(request, "corrected child memory")
                && body_contains(request, "ordinary child memory")
                && !body_contains(request, BRANCH_PROMPT_MARKER)
        },
        sse(vec![
            ev_response_created("parent-followup-response"),
            ev_assistant_message("parent-followup-message", "parent done"),
            ev_completed("parent-followup-response"),
        ]),
    )
    .await;
    let test = spine_builder().build(&server).await?;

    let inject_intermediate = async {
        wait_for_request(&corrected_child, "corrected child first turn", |request| {
            request.body_contains_text("corrected-child-marker")
                && request.body_contains_text(BRANCH_PROMPT_MARKER)
        })
        .await?;
        test.codex
            .submit(Op::InterAgentCommunication {
                communication: InterAgentCommunication::new(
                    AgentPath::try_from("/root/spawn_spawnlifecyclecall_0")
                        .expect("transaction child path should be valid"),
                    AgentPath::root(),
                    Vec::new(),
                    "intermediate-secret".to_string(),
                    /*trigger_turn*/ false,
                ),
            })
            .await?;
        wait_for_request(
            &corrected_child_followup,
            "corrected child follow-up",
            |request| {
                request.body_contains_text(CORRECTION_MESSAGE)
                    && request.input().iter().any(|item| {
                        item.get("call_id").and_then(Value::as_str) == Some("child-yield-call")
                    })
            },
        )
        .await?;
        Result::<()>::Ok(())
    };
    tokio::try_join!(test.submit_turn(FIRST_PARENT_PROMPT), inject_intermediate)?;

    assert_eq!(
        corrected_child_followup
            .requests()
            .iter()
            .filter(|request| {
                request.body_contains_text(CORRECTION_MESSAGE)
                    && request.input().iter().any(|item| {
                        item.get("call_id").and_then(Value::as_str) == Some("child-yield-call")
                    })
            })
            .count(),
        1
    );
    let parent_request = parent_projection_request(
        &parent_followup,
        "corrected child memory",
        "ordinary child memory",
    );
    assert!(!parent_request.body_contains_text("intermediate-secret"));
    assert!(!parent_request.body_contains_text(CORRECTION_MESSAGE));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn descendant_root_message_is_corrected_while_branch_internal_message_is_delivered()
-> Result<()> {
    const B0_SPAWN_D0_CALL_ID: &str = "b0-spawn-d0";
    const B0_WAIT_CALL_ID: &str = "b0-wait-for-descendant";
    const B0_PATH: &str = "/root/spawn_spawnlifecyclecall_0";
    const D0_SEND_BRANCH_CALL_ID: &str = "d0-send-branch";
    const D0_SEND_ROOT_CALL_ID: &str = "d0-send-root";
    const D0_WAIT_CALL_ID: &str = "d0-wait-for-correction";
    const D0_SECRET: &str = "descendant-intermediate-secret";
    const D0_BRANCH_MESSAGE: &str = "descendant-branch-internal-message";

    let server = start_mock_server().await;
    mount_sse_once_match(
        &server,
        is_parent_spawn_request,
        sse(vec![
            ev_response_created("parent-descendant-spawn-response"),
            ev_function_call_with_namespace(
                SPAWN_CALL_ID,
                SPAWN_NAMESPACE,
                SPAWN_TOOL,
                &spawn_args("descendant-parent-marker", "ordinary-sibling-marker"),
            ),
            ev_completed("parent-descendant-spawn-response"),
        ]),
    )
    .await;
    let descendant_parent_spawn = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            child_task_marker(request, "descendant-parent-marker")
                && !has_function_call_output(request, B0_SPAWN_D0_CALL_ID)
        },
        sse(vec![
            ev_response_created("b0-spawn-d0-response"),
            ev_function_call_with_namespace(
                B0_SPAWN_D0_CALL_ID,
                "collaboration",
                "spawn_agent",
                &json!({
                    "message": "descendant-worker-marker",
                    "task_name": "worker",
                    "fork_turns": "all",
                })
                .to_string(),
            ),
            ev_completed("b0-spawn-d0-response"),
        ]),
    )
    .await;
    mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            has_function_call_output(request, B0_SPAWN_D0_CALL_ID)
                && !has_function_call_output(request, B0_WAIT_CALL_ID)
        },
        sse(vec![
            ev_response_created("b0-wait-response"),
            ev_shell_command_call(B0_WAIT_CALL_ID, "sleep 0.3"),
            ev_completed("b0-wait-response"),
        ]),
    )
    .await;
    mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            has_function_call_output(request, B0_WAIT_CALL_ID)
                && body_contains(request, D0_BRANCH_MESSAGE)
        },
        sse(vec![
            ev_response_created("b0-final-response"),
            ev_assistant_message("b0-final-message", "descendant branch memory"),
            ev_completed("b0-final-response"),
        ]),
    )
    .await;
    let descendant_first_request = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            body_contains(request, "descendant-worker-marker")
                && body_contains(request, "\"type\":\"agent_message\"")
                && !has_function_call_output(request, D0_SEND_ROOT_CALL_ID)
        },
        sse(vec![
            ev_response_created("d0-send-response"),
            ev_function_call_with_namespace(
                D0_SEND_BRANCH_CALL_ID,
                "collaboration",
                "send_message",
                &json!({
                    "target": B0_PATH,
                    "message": D0_BRANCH_MESSAGE,
                })
                .to_string(),
            ),
            ev_function_call_with_namespace(
                D0_SEND_ROOT_CALL_ID,
                "collaboration",
                "send_message",
                &json!({
                    "target": "/root",
                    "message": D0_SECRET,
                })
                .to_string(),
            ),
            ev_shell_command_call(D0_WAIT_CALL_ID, "sleep 0.3"),
            ev_completed("d0-send-response"),
        ]),
    )
    .await;
    mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            has_function_call_output(request, D0_SEND_BRANCH_CALL_ID)
                && has_function_call_output(request, D0_SEND_ROOT_CALL_ID)
                && has_function_call_output(request, D0_WAIT_CALL_ID)
                && body_contains_json_text(request, CORRECTION_MESSAGE)
        },
        sse(vec![
            ev_response_created("d0-corrected-response"),
            ev_assistant_message("d0-corrected-message", "descendant terminal memory"),
            ev_completed("d0-corrected-response"),
        ]),
    )
    .await;
    mount_response_once_match(
        &server,
        |request: &wiremock::Request| child_task_marker(request, "ordinary-sibling-marker"),
        sse_response(sse(vec![
            ev_response_created("ordinary-sibling-response"),
            ev_assistant_message("ordinary-sibling-message", "ordinary sibling memory"),
            ev_completed("ordinary-sibling-response"),
        ]))
        .set_delay(Duration::from_millis(700)),
    )
    .await;
    let parent_followup = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            body_contains(request, "descendant branch memory")
                && body_contains(request, "ordinary sibling memory")
                && !body_contains(request, BRANCH_PROMPT_MARKER)
        },
        sse(vec![
            ev_response_created("parent-descendant-followup-response"),
            ev_assistant_message("parent-descendant-followup-message", "parent done"),
            ev_completed("parent-descendant-followup-response"),
        ]),
    )
    .await;
    let test = multi_agent_v2_spine_builder().build(&server).await?;
    let mut created_threads = test.thread_manager.subscribe_thread_created();

    test.submit_turn(FIRST_PARENT_PROMPT).await?;

    for _ in 0..3 {
        let thread_id =
            tokio::time::timeout(Duration::from_secs(5), created_threads.recv()).await??;
        assert!(
            test.thread_manager.get_thread(thread_id).await.is_err(),
            "Spawn must remove every direct branch and descendant before returning"
        );
    }
    let ordinary_spawn_parent_request = unique_matching_request(
        &descendant_parent_spawn,
        "ordinary V2 spawn parent",
        |request| body_has_child_task_marker(&request.body_json(), "descendant-parent-marker"),
    );
    let ordinary_spawn_child_request = unique_matching_request(
        &descendant_first_request,
        "ordinary V2 spawn child",
        |request| {
            request.body_contains_text("descendant-worker-marker")
                && !request.input().iter().any(|item| {
                    item.get("call_id").and_then(Value::as_str) == Some(B0_SPAWN_D0_CALL_ID)
                })
        },
    );
    let ordinary_spawn_parent_body = ordinary_spawn_parent_request.body_json();
    let ordinary_spawn_child_body = ordinary_spawn_child_request.body_json();
    let ordinary_spawn_parent_input = ordinary_spawn_parent_body["input"]
        .as_array()
        .expect("ordinary V2 spawn parent input must be an array");
    let ordinary_spawn_child_input = ordinary_spawn_child_body["input"]
        .as_array()
        .expect("ordinary V2 spawn child input must be an array");
    let exact_lcp = ordinary_spawn_parent_input
        .iter()
        .zip(ordinary_spawn_child_input)
        .take_while(|(parent, child)| parent == child)
        .count();
    assert_eq!(
        exact_lcp,
        ordinary_spawn_parent_input.len(),
        "SpineJIT V2 fork_turns=all must preserve the complete parent request input as an exact prefix"
    );
    assert_eq!(
        ordinary_spawn_parent_body["prompt_cache_key"],
        ordinary_spawn_child_body["prompt_cache_key"],
        "SpineJIT V2 parent and child must share session prompt-cache affinity"
    );
    let parent_request = parent_projection_request(
        &parent_followup,
        "descendant branch memory",
        "ordinary sibling memory",
    );
    assert!(!parent_request.body_contains_text(D0_SECRET));
    assert!(!parent_request.body_contains_text(D0_BRANCH_MESSAGE));
    assert!(!parent_request.body_contains_text(CORRECTION_MESSAGE));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupt_tears_down_children_drops_late_mail_and_releases_batch_capacity() -> Result<()> {
    const CANCEL_SPAWN_DESCENDANT_CALL_ID: &str = "cancel-spawn-descendant";
    const LATE_DESCENDANT_MESSAGE: &str = "late-descendant-message-must-not-reach-root";
    let server = start_mock_server().await;
    mount_sse_once_match(
        &server,
        is_parent_spawn_request,
        sse(vec![
            ev_response_created("cancel-parent-response"),
            ev_function_call_with_namespace(
                SPAWN_CALL_ID,
                SPAWN_NAMESPACE,
                SPAWN_TOOL,
                &spawn_args("cancel-first-marker", "cancel-second-marker"),
            ),
            ev_completed("cancel-parent-response"),
        ]),
    )
    .await;
    let cancel_first = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            child_task_marker(request, "cancel-first-marker")
                && !has_function_call_output(request, CANCEL_SPAWN_DESCENDANT_CALL_ID)
        },
        sse(vec![
            ev_response_created("cancel-first-response"),
            ev_function_call_with_namespace(
                CANCEL_SPAWN_DESCENDANT_CALL_ID,
                "collaboration",
                "spawn_agent",
                &json!({
                    "message": "cancel-descendant-marker",
                    "task_name": "worker",
                    "fork_turns": "all",
                })
                .to_string(),
            ),
            ev_completed("cancel-first-response"),
        ]),
    )
    .await;
    let cancel_first_followup = mount_response_once_match(
        &server,
        |request: &wiremock::Request| {
            has_function_call_output(request, CANCEL_SPAWN_DESCENDANT_CALL_ID)
        },
        sse_response(sse(vec![
            ev_response_created("cancel-first-followup-response"),
            ev_assistant_message("cancel-first-followup-message", "too late"),
            ev_completed("cancel-first-followup-response"),
        ]))
        .set_delay(Duration::from_secs(5)),
    )
    .await;
    let cancel_descendant = mount_response_once_match(
        &server,
        |request: &wiremock::Request| {
            body_contains(request, "cancel-descendant-marker")
                && body_contains(request, "\"type\":\"agent_message\"")
        },
        sse_response(sse(vec![
            ev_response_created("cancel-descendant-response"),
            ev_assistant_message("cancel-descendant-message", "too late"),
            ev_completed("cancel-descendant-response"),
        ]))
        .set_delay(Duration::from_secs(5)),
    )
    .await;
    let cancel_second = mount_response_once_match(
        &server,
        |request: &wiremock::Request| child_task_marker(request, "cancel-second-marker"),
        sse_response(sse(vec![
            ev_response_created("cancel-second-response"),
            ev_assistant_message("cancel-second-message", "too late"),
            ev_completed("cancel-second-response"),
        ]))
        .set_delay(Duration::from_secs(5)),
    )
    .await;

    let replacement_call_id = "spawn-replacement-call";
    mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            body_contains(request, SECOND_PARENT_PROMPT)
                && !body_contains(request, LATE_DESCENDANT_MESSAGE)
        },
        sse(vec![
            ev_response_created("replacement-parent-response"),
            ev_function_call_with_namespace(
                replacement_call_id,
                SPAWN_NAMESPACE,
                SPAWN_TOOL,
                &spawn_args("replacement-first-marker", "replacement-second-marker"),
            ),
            ev_completed("replacement-parent-response"),
        ]),
    )
    .await;
    let mut replacement_children = Vec::new();
    for (marker, response, message, memory) in [
        (
            "replacement-first-marker",
            "replacement-first-response",
            "replacement-first-message",
            "replacement first memory",
        ),
        (
            "replacement-second-marker",
            "replacement-second-response",
            "replacement-second-message",
            "replacement second memory",
        ),
    ] {
        replacement_children.push(
            mount_response_once_match(
                &server,
                move |request: &wiremock::Request| child_task_marker(request, marker),
                sse_response(sse(vec![
                    ev_response_created(response),
                    ev_assistant_message(message, memory),
                    ev_completed(response),
                ]))
                .set_delay(Duration::from_secs(5)),
            )
            .await,
        );
    }
    let test = multi_agent_v2_spine_builder().build(&server).await?;
    let mut created_threads = test.thread_manager.subscribe_thread_created();

    test.codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: FIRST_PARENT_PROMPT.to_string(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: Default::default(),
        })
        .await?;
    wait_for_request(&cancel_first, "cancel first child", |request| {
        request.body_contains_text("cancel-first-marker")
            && request.body_contains_text(BRANCH_PROMPT_MARKER)
    })
    .await?;
    wait_for_request(&cancel_second, "cancel second child", |request| {
        request.body_contains_text("cancel-second-marker")
            && request.body_contains_text(BRANCH_PROMPT_MARKER)
    })
    .await?;
    wait_for_request(
        &cancel_first_followup,
        "cancel first child after descendant spawn",
        |request| {
            request.input().iter().any(|item| {
                item.get("call_id").and_then(Value::as_str) == Some(CANCEL_SPAWN_DESCENDANT_CALL_ID)
            })
        },
    )
    .await?;
    wait_for_request(&cancel_descendant, "cancel descendant", |request| {
        request.body_contains_text("cancel-descendant-marker")
    })
    .await?;
    let mut transaction_thread_ids = Vec::new();
    for _ in 0..3 {
        transaction_thread_ids
            .push(tokio::time::timeout(Duration::from_secs(5), created_threads.recv()).await??);
    }
    submit_interrupt_then_mailbox_for_test(
        test.codex.as_ref(),
        InterAgentCommunication::new(
            AgentPath::try_from("/root/spawn_spawnlifecyclecall_0/worker")
                .expect("descendant path should be valid"),
            AgentPath::root(),
            Vec::new(),
            LATE_DESCENDANT_MESSAGE.to_string(),
            /*trigger_turn*/ false,
        ),
    )
    .await?;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if matches!(test.codex.next_event().await?.msg, EventMsg::TurnAborted(_)) {
                return Result::<()>::Ok(());
            }
        }
    })
    .await
    .context("Interrupt did not complete within the native hard-abort bound")??;

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if test.thread_manager.list_thread_ids().await.len() == 1
            && test.codex.agent_status().await == AgentStatus::Interrupted
        {
            break;
        }
        if Instant::now() >= deadline {
            anyhow::bail!("cancelled transaction children remained loaded");
        }
        sleep(Duration::from_millis(10)).await;
    }
    for thread_id in transaction_thread_ids {
        assert!(
            test.thread_manager.get_thread(thread_id).await.is_err(),
            "Interrupt must remove every direct branch and recursive descendant"
        );
    }

    test.codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: SECOND_PARENT_PROMPT.to_string(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: Default::default(),
        })
        .await?;
    for (mock_response, marker) in replacement_children
        .iter()
        .zip(["replacement-first-marker", "replacement-second-marker"])
    {
        wait_for_request(mock_response, marker, |request| {
            request.body_contains_text(marker) && request.body_contains_text(BRANCH_PROMPT_MARKER)
        })
        .await?;
    }
    assert_eq!(test.thread_manager.list_thread_ids().await.len(), 3);
    test.codex.submit(Op::Interrupt).await?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if test.thread_manager.list_thread_ids().await.len() == 1
            && test.codex.agent_status().await == AgentStatus::Interrupted
        {
            break;
        }
        if Instant::now() >= deadline {
            anyhow::bail!("replacement transaction children remained loaded after cleanup");
        }
        sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn successful_batches_release_transaction_children_for_immediate_reuse() -> Result<()> {
    let server = start_mock_server().await;
    let first_call_id = "spawn-first-success-call";
    mount_sse_once_match(
        &server,
        move |request: &wiremock::Request| {
            body_contains(request, FIRST_PARENT_PROMPT)
                && !body_contains(request, SECOND_PARENT_PROMPT)
                && !body_contains(request, BRANCH_PROMPT_MARKER)
                && !has_function_call_output(request, first_call_id)
        },
        sse(vec![
            ev_response_created("first-success-parent-response"),
            ev_function_call_with_namespace(
                first_call_id,
                SPAWN_NAMESPACE,
                SPAWN_TOOL,
                &spawn_args("first-success-a-marker", "first-success-b-marker"),
            ),
            ev_completed("first-success-parent-response"),
        ]),
    )
    .await;
    for (marker, response, message, memory) in [
        (
            "first-success-a-marker",
            "first-success-a-response",
            "first-success-a-message",
            "first batch memory one",
        ),
        (
            "first-success-b-marker",
            "first-success-b-response",
            "first-success-b-message",
            "first batch memory two",
        ),
    ] {
        mount_response_once_match(
            &server,
            move |request: &wiremock::Request| child_task_marker(request, marker),
            sse_response(sse(vec![
                ev_response_created(response),
                ev_assistant_message(message, memory),
                ev_completed(response),
            ])),
        )
        .await;
    }
    let first_followup = mount_sse_once_match(
        &server,
        move |request: &wiremock::Request| {
            body_contains(request, "first batch memory one")
                && body_contains(request, "first batch memory two")
                && !body_contains(request, SECOND_PARENT_PROMPT)
                && has_function_call_output(request, first_call_id)
                && !body_contains(request, BRANCH_PROMPT_MARKER)
        },
        sse(vec![
            ev_response_created("first-success-followup-response"),
            ev_assistant_message("first-success-followup-message", "first batch done"),
            ev_completed("first-success-followup-response"),
        ]),
    )
    .await;

    let second_call_id = "spawn-second-success-call";
    mount_sse_once_match(
        &server,
        move |request: &wiremock::Request| {
            body_contains(request, SECOND_PARENT_PROMPT)
                && !body_contains(request, BRANCH_PROMPT_MARKER)
                && !has_function_call_output(request, second_call_id)
        },
        sse(vec![
            ev_response_created("second-success-parent-response"),
            ev_function_call_with_namespace(
                second_call_id,
                SPAWN_NAMESPACE,
                SPAWN_TOOL,
                &spawn_args("second-success-a-marker", "second-success-b-marker"),
            ),
            ev_completed("second-success-parent-response"),
        ]),
    )
    .await;
    for (marker, response, message, memory) in [
        (
            "second-success-a-marker",
            "second-success-a-response",
            "second-success-a-message",
            "second batch memory one",
        ),
        (
            "second-success-b-marker",
            "second-success-b-response",
            "second-success-b-message",
            "second batch memory two",
        ),
    ] {
        mount_response_once_match(
            &server,
            move |request: &wiremock::Request| child_task_marker(request, marker),
            sse_response(sse(vec![
                ev_response_created(response),
                ev_assistant_message(message, memory),
                ev_completed(response),
            ])),
        )
        .await;
    }
    let second_followup = mount_sse_once_match(
        &server,
        move |request: &wiremock::Request| {
            body_contains(request, "second batch memory one")
                && body_contains(request, "second batch memory two")
                && has_function_call_output(request, second_call_id)
                && !body_contains(request, BRANCH_PROMPT_MARKER)
        },
        sse(vec![
            ev_response_created("second-success-followup-response"),
            ev_assistant_message("second-success-followup-message", "second batch done"),
            ev_completed("second-success-followup-response"),
        ]),
    )
    .await;

    let test = spine_builder().build(&server).await?;
    assert!(!test.config.features.enabled(Feature::MultiAgentV2));

    test.submit_turn(FIRST_PARENT_PROMPT).await?;
    assert_eq!(
        test.thread_manager.list_thread_ids().await.len(),
        1,
        "completed Spine transaction children must be removed before returning the receipt"
    );
    assert!(
        first_followup
            .function_call_output_text(first_call_id)
            .is_some()
    );

    test.submit_turn(SECOND_PARENT_PROMPT).await?;
    assert_eq!(
        test.thread_manager.list_thread_ids().await.len(),
        1,
        "the replacement transaction must release its children too"
    );
    assert!(
        second_followup
            .function_call_output_text(second_call_id)
            .is_some()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multiple_spawn_calls_are_rejected_before_child_creation() -> Result<()> {
    const PROMPT: &str = "attempt two spine spawn calls in one response";
    const FIRST_CALL_ID: &str = "duplicate-spawn-first";
    const SECOND_CALL_ID: &str = "duplicate-spawn-second";

    let server = start_mock_server().await;
    mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            body_contains(request, PROMPT)
                && !has_function_call_output(request, FIRST_CALL_ID)
                && !has_function_call_output(request, SECOND_CALL_ID)
        },
        sse(vec![
            ev_response_created("duplicate-spawn-parent-response"),
            ev_function_call_with_namespace(
                FIRST_CALL_ID,
                SPAWN_NAMESPACE,
                SPAWN_TOOL,
                &spawn_args("duplicate-first-a", "duplicate-first-b"),
            ),
            ev_function_call_with_namespace(
                SECOND_CALL_ID,
                SPAWN_NAMESPACE,
                SPAWN_TOOL,
                &spawn_args("duplicate-second-a", "duplicate-second-b"),
            ),
            ev_completed("duplicate-spawn-parent-response"),
        ]),
    )
    .await;
    let followup = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            has_function_call_output(request, FIRST_CALL_ID)
                && has_function_call_output(request, SECOND_CALL_ID)
        },
        sse(vec![
            ev_response_created("duplicate-spawn-followup-response"),
            ev_assistant_message("duplicate-spawn-followup-message", "duplicate rejected"),
            ev_completed("duplicate-spawn-followup-response"),
        ]),
    )
    .await;
    let test = spine_builder().build(&server).await?;

    test.submit_turn(PROMPT).await?;

    assert_eq!(
        test.thread_manager.list_thread_ids().await.len(),
        1,
        "duplicate spine.spawn calls must fail before creating children"
    );
    for call_id in [FIRST_CALL_ID, SECOND_CALL_ID] {
        let provider_output = followup
            .function_call_output_text(call_id)
            .with_context(|| format!("missing failure output for `{call_id}`"))?;
        assert_eq!(provider_output, r#"{"status":"failure"}"#);
        let output = persisted_function_call_output(&test, call_id)?;
        assert!(
            output.contains("spine.spawn may be called at most once in one model response"),
            "unexpected durable failure output for `{call_id}`: {output}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_per_call_bound_is_model_visible_and_rejects_oversized_batches() -> Result<()> {
    const CALL_ID: &str = "spawn-over-limit-call";
    const PROMPT: &str = "run a spine spawn batch beyond the configured per-call bound";
    let tasks = [
        ("first", "first-marker"),
        ("second", "second-marker"),
        ("third", "third-marker"),
    ];
    let server = start_mock_server().await;
    let first_request = mount_sse_once_match(
        &server,
        move |request: &wiremock::Request| {
            body_contains(request, PROMPT)
                && !body_contains(request, BRANCH_PROMPT_MARKER)
                && !has_function_call_output(request, CALL_ID)
        },
        sse(vec![
            ev_response_created("over-limit-parent-response"),
            ev_function_call_with_namespace(
                CALL_ID,
                SPAWN_NAMESPACE,
                SPAWN_TOOL,
                &spawn_args_for(&tasks),
            ),
            ev_completed("over-limit-parent-response"),
        ]),
    )
    .await;
    let parent_followup = mount_sse_once_match(
        &server,
        move |request: &wiremock::Request| {
            has_function_call_output(request, CALL_ID)
                && !body_contains(request, BRANCH_PROMPT_MARKER)
        },
        sse(vec![
            ev_response_created("over-limit-followup-response"),
            ev_assistant_message("over-limit-followup-message", "limit handled"),
            ev_completed("over-limit-followup-response"),
        ]),
    )
    .await;
    let test = spine_builder().build(&server).await?;

    test.submit_turn(PROMPT).await?;

    let request_body = first_request.single_request().body_json();
    let spawn = request_body["tools"]
        .as_array()
        .and_then(|tools| {
            tools
                .iter()
                .find(|tool| tool["type"] == "namespace" && tool["name"] == SPAWN_NAMESPACE)
        })
        .and_then(|namespace| namespace["tools"].as_array())
        .and_then(|tools| tools.iter().find(|tool| tool["name"] == SPAWN_TOOL))
        .context("model request is missing spine.spawn")?;
    assert!(
        spawn["description"]
            .as_str()
            .is_some_and(|description| description.ends_with(
                "The tasks array must contain at least 2 and at most 2 task assignments."
            )),
        "configured task bound must be visible in the tool description"
    );
    assert_eq!(
        spawn["parameters"]["properties"]["tasks"].get("minItems"),
        None
    );
    assert_eq!(
        spawn["parameters"]["properties"]["tasks"].get("maxItems"),
        None
    );
    assert_eq!(
        test.thread_manager.list_thread_ids().await.len(),
        1,
        "per-call validation must run before child creation"
    );
    let provider_output = parent_followup
        .function_call_output_text(CALL_ID)
        .expect("parent follow-up must receive the spine.spawn failure carrier");
    assert_eq!(provider_output, r#"{"status":"failure"}"#);

    let output = persisted_function_call_output(&test, CALL_ID)?;
    assert!(
        output.contains("spine.spawn accepts at most 2 tasks"),
        "unexpected durable failure output: {output}"
    );
    Ok(())
}
