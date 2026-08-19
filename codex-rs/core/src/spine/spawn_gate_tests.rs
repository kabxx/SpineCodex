use std::collections::HashMap;

use codex_protocol::request_user_input::RequestUserInputAnswer;
use pretty_assertions::assert_eq;

use super::*;

fn response(answers: Vec<&str>) -> RequestUserInputResponse {
    RequestUserInputResponse {
        answers: HashMap::from([(
            FAILURE_ACTION_QUESTION_ID.to_string(),
            RequestUserInputAnswer {
                answers: answers.into_iter().map(str::to_string).collect(),
            },
        )]),
    }
}

#[test]
fn parses_each_failure_action() {
    assert_eq!(
        parse_failure_decision(response(vec![CONTINUE_LABEL])),
        Some(SpawnFailureDecision {
            action: SpawnFailureAction::Continue,
            note: None,
        })
    );
    assert_eq!(
        parse_failure_decision(response(vec![RETRY_LABEL])),
        Some(SpawnFailureDecision {
            action: SpawnFailureAction::Retry,
            note: None,
        })
    );
    assert_eq!(
        parse_failure_decision(response(vec![ABANDON_LABEL])),
        Some(SpawnFailureDecision {
            action: SpawnFailureAction::Abandon,
            note: None,
        })
    );
}

#[test]
fn extracts_and_bounds_generic_options_notes() {
    assert_eq!(
        parse_failure_decision(response(vec![
            CONTINUE_LABEL,
            "user_note: keep going",
            "user_note: preserve the partial result",
        ])),
        Some(SpawnFailureDecision {
            action: SpawnFailureAction::Continue,
            note: Some("keep going\npreserve the partial result".to_string()),
        })
    );

    let long_note = "x".repeat(MAX_FAILURE_GUIDANCE_CHARS + 1);
    assert_eq!(
        parse_failure_decision(RequestUserInputResponse {
            answers: HashMap::from([(
                FAILURE_ACTION_QUESTION_ID.to_string(),
                RequestUserInputAnswer {
                    answers: vec![RETRY_LABEL.to_string(), format!("user_note: {long_note}")],
                },
            )]),
        }),
        Some(SpawnFailureDecision {
            action: SpawnFailureAction::Retry,
            note: Some("x".repeat(MAX_FAILURE_GUIDANCE_CHARS)),
        })
    );
}

#[test]
fn rejects_empty_unknown_or_malformed_answers() {
    assert_eq!(parse_failure_decision(response(Vec::new())), None);
    assert_eq!(parse_failure_decision(response(vec!["Unknown"])), None);
    assert_eq!(
        parse_failure_decision(response(vec![CONTINUE_LABEL, "unexpected"])),
        None
    );
}
