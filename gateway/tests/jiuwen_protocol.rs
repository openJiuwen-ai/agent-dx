#![cfg(feature = "agent-api")]

use data_plane_gateway::ingress::jiuwen::protocol::{Request, MAX_FRAME_BYTES};
use serde_json::{json, Value};

fn request(method: &str, params: Value) -> Request {
    Request::parse(
        &serde_json::to_vec(&json!({"type":"req","id":"client-1","method":method,"params":params}))
            .unwrap(),
    )
    .unwrap()
}

#[test]
fn chat_keeps_client_ack_separate_from_e2a_and_preserves_steering_fields() {
    let prepared = request("chat.send", json!({"session_id":"session-1","content":"hello","mode":"agent.work.normal","input_mode":"steer","expected_execution_id":"exec-1","user_id":"forged"}))
        .prepare("verified-user", "backend-1").unwrap();
    assert_eq!(prepared.envelope["request_id"], "backend-1");
    assert_eq!(prepared.envelope["user_id"], "verified-user");
    assert_eq!(prepared.envelope["channel"], "web");
    assert_eq!(prepared.envelope["params"]["query"], "hello");
    assert_eq!(
        prepared.envelope["params"]["expected_execution_id"],
        "exec-1"
    );
    assert!(prepared.envelope["params"].get("user_id").is_none());
    assert_eq!(prepared.envelope["is_stream"], true);
    assert_eq!(
        prepared.acknowledgement.unwrap(),
        json!({"type":"res","id":"client-1","ok":true,"payload":{"accepted":true,"session_id":"session-1"}})
    );
}

#[test]
fn create_drops_session_and_preserves_or_generates_an_idempotency_token() {
    let prepared = request("session.create", json!({"session_id":"unrelated","mode":"team.code.plan","work_mode":"code","create_token":"from-client"}))
        .prepare("user", "backend-1").unwrap();
    assert!(prepared.envelope["session_id"].is_null());
    assert!(prepared.envelope["params"].get("session_id").is_none());
    assert_eq!(prepared.envelope["params"]["create_token"], "from-client");
    assert_eq!(prepared.envelope["is_stream"], false);
    assert!(prepared.acknowledgement.is_none());
    let generated = request("session.create", json!({}))
        .prepare("user", "backend-2")
        .unwrap();
    assert_eq!(
        generated.envelope["params"]["create_token"]
            .as_str()
            .unwrap()
            .len(),
        32
    );
}

#[test]
fn history_preserves_both_pagination_contracts_and_null_cursor() {
    let prepared = request(
        "history.get",
        json!({"session_id":"s","cursor":null,"page_idx":1,"limit":50}),
    )
    .prepare("user", "backend-1")
    .unwrap();
    assert_eq!(prepared.envelope["params"]["cursor"], Value::Null);
    assert_eq!(prepared.envelope["is_stream"], true);
    assert_eq!(
        prepared.acknowledgement.unwrap()["payload"],
        json!({"accepted":true,"session_id":"s","cursor":null,"page_idx":1,"limit":50})
    );
}

#[test]
fn replies_keep_the_original_interaction_request_id() {
    for method in ["chat.send", "chat.user_answer"] {
        let prepared = request(method, json!({"session_id":"s","request_id":"question-1","query":"","answers":[{"card_id":"card-1","selected_options":["approve"]}]}))
            .prepare("user", "backend-1").unwrap();
        assert_eq!(prepared.envelope["params"]["request_id"], "question-1");
        assert_eq!(prepared.envelope["params"]["query"], "");
        assert_eq!(
            prepared.envelope["params"]["answers"][0]["card_id"],
            "card-1"
        );
    }
    let interrupt = request(
        "chat.interrupt",
        json!({"session_id":"s","intent":"cancel"}),
    )
    .prepare("user", "b")
    .unwrap();
    assert_eq!(
        interrupt.acknowledgement.unwrap()["payload"]["intent"],
        "cancel"
    );
    assert_eq!(interrupt.envelope["is_stream"], false);
    let flow = request(
        "chat.swarmflow_reply",
        json!({"session_id":"s","run_id":"flow","correlation_id":"step","answer":"yes"}),
    )
    .prepare("user", "b")
    .unwrap();
    assert_eq!(flow.envelope["params"]["correlation_id"], "step");
}

#[test]
fn unary_queries_have_no_early_ack_and_require_session_where_applicable() {
    for (method, params) in [
        ("session.list", json!({"limit":30,"offset":0})),
        ("session.get_metadata", json!({"session_id":"s"})),
    ] {
        let prepared = request(method, params).prepare("user", "b").unwrap();
        assert!(prepared.acknowledgement.is_none());
        assert_eq!(prepared.envelope["is_stream"], false);
    }
    assert!(
        Request::parse(br#"{"type":"req","id":"r","method":"history.get","params":{}}"#).is_err()
    );
}

#[test]
fn forwards_agentserver_methods_without_a_gateway_method_list() {
    for (method, params) in [
        ("project.list", json!({"filter":"all","user_id":"forged"})),
        ("project.get_sessions", json!({"project_id":"project-1"})),
        ("config.set", json!({"key":"theme","value":"dark"})),
        ("future.method", json!({"content":"raw"})),
    ] {
        let prepared = request(method, params)
            .prepare("verified-user", "backend-1")
            .unwrap();
        assert_eq!(prepared.envelope["method"], method);
        assert_eq!(prepared.envelope["user_id"], "verified-user");
        assert!(prepared.envelope["session_id"].is_null());
        assert_eq!(prepared.envelope["is_stream"], false);
        assert!(prepared.acknowledgement.is_none());
        assert!(prepared.envelope["params"].get("user_id").is_none());
        assert!(prepared.envelope["params"].get("query").is_none());
    }
}

#[test]
fn forwards_explicit_generic_stream_without_a_synthetic_chat_ack() {
    let raw = json!({
        "type":"req",
        "id":"client-1",
        "method":"command.goal",
        "params":{"goal_id":"goal-1"},
        "is_stream":true
    });
    let request = Request::parse(&serde_json::to_vec(&raw).unwrap()).unwrap();
    let prepared = request.prepare("verified-user", "backend-1").unwrap();
    assert_eq!(prepared.envelope["method"], "command.goal");
    assert_eq!(prepared.envelope["is_stream"], true);
    assert!(prepared.acknowledgement.is_none());
}

#[test]
fn rejects_invalid_requests_sessions_and_oversized_frames() {
    for params in [
        json!({"session_id":"../other"}),
        json!({"session_id":"a/other"}),
        json!({"session_id":"x".repeat(81)}),
    ] {
        let raw = serde_json::to_vec(
            &json!({"type":"req","id":"r","method":"chat.send","params":params}),
        )
        .unwrap();
        assert!(Request::parse(&raw).is_err());
    }
    for raw in [
        br#"[]"#.as_slice(),
        br#"{"type":"event","id":"r","method":"chat.send","params":{}}"#,
        br#"{"type":"req","id":"r","method":"","params":{}}"#,
        br#"{"type":"req","id":"r","method":"bad\nmethod","params":{}}"#,
    ] {
        assert!(Request::parse(raw).is_err());
    }
    assert!(Request::parse(&vec![b' '; MAX_FRAME_BYTES + 1]).is_err());
}

#[test]
fn optional_ack_identifiers_follow_web_handler_string_rules() {
    for (method, field) in [
        ("chat.interrupt", "intent"),
        ("chat.user_answer", "request_id"),
    ] {
        for value in [
            Value::Null,
            json!(""),
            json!(42),
            json!({"unexpected":true}),
        ] {
            let mut params = json!({"session_id":"s"});
            params[field] = value.clone();
            let prepared = request(method, params).prepare("user", "backend").unwrap();
            assert!(prepared.acknowledgement.unwrap()["payload"]
                .get(field)
                .is_none());
            // Business validation belongs to AgentServer; ACK projection must not mutate input.
            assert_eq!(prepared.envelope["params"][field], value);
        }
    }
}

#[test]
fn explicit_empty_query_and_identity_boundary_are_preserved() {
    let req = request(
        "chat.send",
        json!({"session_id":"s","content":"ignored","query":"","answers":[]}),
    );
    assert_eq!(
        req.prepare("user", "backend").unwrap().envelope["params"]["query"],
        ""
    );
    for identity in ["", " ", "user\nforged"] {
        assert!(req.prepare(identity, "backend").is_err());
        assert!(req.prepare("user", identity).is_err());
    }
}
