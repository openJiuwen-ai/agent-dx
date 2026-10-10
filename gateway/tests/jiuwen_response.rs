#![cfg(feature = "agent-api")]
use data_plane_gateway::ingress::jiuwen::{protocol::Request, response::Response};
use serde_json::{json, Value};

fn request(method: &str) -> Request {
    Request::parse(
        &serde_json::to_vec(
            &json!({"type":"req","id":"client","method":method,"params":{"session_id":"session"}}),
        )
        .unwrap(),
    )
    .unwrap()
}
fn response(kind: &str, body: Value) -> Response {
    Response::parse(&serde_json::to_vec(&json!({"protocol_version":"1.0","request_id":"backend","sequence":0,"is_final":kind!="e2a.chunk","status":if kind=="e2a.error" {"failed"} else if kind=="e2a.chunk" {"in_progress"} else {"succeeded"},"response_kind":kind,"body":body})).unwrap()).unwrap()
}
fn project(method: &str, kind: &str, body: Value) -> Option<Value> {
    response(kind, body)
        .project(&request(method), "backend", "user")
        .unwrap()
        .frame
}

#[test]
fn unary_result_and_error_use_client_request_id() {
    assert_eq!(
        project(
            "session.list",
            "e2a.complete",
            json!({"result":{"sessions":[],"total":0}})
        )
        .unwrap(),
        json!({"type":"res","id":"client","ok":true,"payload":{"sessions":[],"total":0}})
    );
    assert_eq!(project("session.create", "e2a.error",json!({"code":"E2A.AGENT_ERROR","message":"wrapper","details":{"error":"bad mode","code":"BAD_REQUEST"}})).unwrap(),json!({"type":"res","id":"client","ok":false,"error":"bad mode","code":"BAD_REQUEST"}));
    let error = project(
        "session.create",
        "e2a.error",
        json!({"details":{"error":"failed"}}),
    )
    .unwrap();
    assert_eq!(error["code"], "SESSION_CREATE_FAILED");
}

#[test]
fn generic_agentserver_unary_result_and_error_reach_client() {
    let request = Request::parse(
        br#"{"type":"req","id":"client","method":"project.list","params":{"filter":"all"}}"#,
    )
    .unwrap();
    let success = response(
        "e2a.complete",
        json!({"result":{"projects":[{"project_id":"project-1"}]}}),
    )
    .project(&request, "backend", "user")
    .unwrap();
    assert_eq!(
        success.frame.unwrap(),
        json!({"type":"res","id":"client","ok":true,"payload":{"projects":[{"project_id":"project-1"}]}})
    );
    let failed = response(
        "e2a.error",
        json!({"details":{"error":"bad filter","code":"BAD_REQUEST"}}),
    )
    .project(&request, "backend", "user")
    .unwrap();
    assert_eq!(
        failed.frame.unwrap(),
        json!({"type":"res","id":"client","ok":false,"error":"bad filter","code":"BAD_REQUEST"})
    );
}

#[test]
fn generic_stream_uses_its_event_and_final_result_without_chat_labels() {
    let request = Request::parse(
        br#"{"type":"req","id":"client","method":"command.goal","params":{},"is_stream":true}"#,
    )
    .unwrap();
    let progress = response(
        "e2a.chunk",
        json!({"delta_kind":"custom","delta":{"event_type":"goal.progress","step":1}}),
    )
    .project(&request, "backend", "user")
    .unwrap();
    assert_eq!(progress.frame.unwrap()["event"], "goal.progress");
    let result = response("e2a.complete", json!({"result":{"status":"done"}}))
        .project(&request, "backend", "user")
        .unwrap();
    assert_eq!(
        result.frame.unwrap(),
        json!({"type":"res","id":"client","ok":true,"payload":{"status":"done"}})
    );
    let empty = response("e2a.complete", json!({"result":{}}))
        .project(&request, "backend", "user")
        .unwrap();
    assert_eq!(
        empty.frame.unwrap(),
        json!({"type":"res","id":"client","ok":true,"payload":{}})
    );
}

#[test]
fn terminal_marker_is_not_a_business_final_or_history_completion() {
    for method in ["chat.send", "history.get"] {
        let result = response("e2a.complete", json!({"result":{}}))
            .project(&request(method), "backend", "user")
            .unwrap();
        assert!(result.complete);
        assert!(result.frame.is_none());
    }
    let event=project("chat.send","e2a.complete",json!({"result":{"event_type":"runtime.accepted","request_id":"backend","execution_id":"run"}})).unwrap();
    assert_eq!(event["event"], "runtime.accepted");
    assert_eq!(event["payload"]["request_id"], "client");
}

#[test]
fn text_and_rich_events_preserve_execution_and_business_identifiers() {
    let text=project("chat.send","e2a.chunk",json!({"delta_kind":"text","event_type":"chat.delta","delta":"hello","rid":"member-round","turn_request_id":"backend","execution_id":"run","output_order":3})).unwrap();
    assert_eq!(text["payload"]["content"], "hello");
    assert_eq!(text["payload"]["request_id"], "client");
    assert_eq!(text["payload"]["turn_request_id"], "client");
    assert_eq!(text["payload"]["rid"], "member-round");
    for event in [
        "chat.ask_user_question",
        "chat.tool_result",
        "chat.subtask_update",
        "chat.file",
        "chat.reasoning",
    ] {
        let frame=project("chat.send","e2a.chunk",json!({"delta_kind":"custom","event_type":event,"delta":{"request_id":"question-id","turn_request_id":"backend","content":"data","files":[{"download_url":"/file-api/download?token=signed"}],"answers":[{"request_id":"nested-id"}]}})).unwrap();
        assert_eq!(frame["event"], event);
        assert_eq!(frame["payload"]["request_id"], "question-id");
        assert_eq!(frame["payload"]["turn_request_id"], "client");
        assert_eq!(frame["payload"]["answers"][0]["request_id"], "nested-id");
        assert_eq!(
            frame["payload"]["files"][0]["download_url"],
            "/file-api/download?token=signed"
        );
    }
}

#[test]
fn history_keeps_pagination_fragment_and_terminal_business_fields() {
    let payload = json!({"event_type":"history.message","request_id":"backend","cursor":null,"page_idx":1,"message":{"_part":{"record_id":"r","part_idx":0,"total_parts":2},"content":"one"}});
    let frame = project(
        "history.get",
        "e2a.chunk",
        json!({"delta_kind":"custom","delta":payload}),
    )
    .unwrap();
    assert_eq!(frame["event"], "history.message");
    assert_eq!(frame["payload"]["request_id"], "client");
    assert_eq!(frame["payload"]["cursor"], Value::Null);
    assert_eq!(frame["payload"]["message"]["_part"]["total_parts"], 2);
    let done=project("history.get","e2a.complete",json!({"result":{"event_type":"history.message","status":"done","has_more":false,"next_cursor":null,"snapshot_id":"snapshot"}})).unwrap();
    assert_eq!(done["event"], "history.message");
    assert_eq!(done["payload"]["has_more"], false);
}

#[test]
fn interrupt_confirmation_and_runtime_errors_remain_separate() {
    let event = project(
        "chat.interrupt",
        "e2a.complete",
        json!({"result":{"event_type":"chat.interrupt_result","intent":"cancel","success":true}}),
    )
    .unwrap();
    assert_eq!(event["event"], "chat.interrupt_result");
    let error=project("chat.send","e2a.error",json!({"details":{"event_type":"runtime.error","error":"failed","code":"SESSION_INPUT_DELIVERY_UNKNOWN"}})).unwrap();
    assert_eq!(error["event"], "chat.error");
    assert_eq!(error["payload"]["code"], "SESSION_INPUT_DELIVERY_UNKNOWN");
    assert_eq!(error["payload"]["request_id"], "client");
    assert_eq!(
        project(
            "chat.interrupt",
            "e2a.error",
            json!({"details":{"error":"not running","code":"NOT_FOUND"}})
        )
        .unwrap()["type"],
        "res"
    );
}

#[test]
fn reject_misrouted_malformed_and_push_as_request_reply() {
    let req = request("chat.send");
    let good = json!({"protocol_version":"1.0","request_id":"backend","sequence":0,"is_final":true,"status":"succeeded","response_kind":"e2a.complete","body":{"result":{}}});
    for (field, value) in [
        ("request_id", json!("someone-else")),
        ("user_id", json!("other-user")),
        ("channel", json!("tui")),
        ("metadata", json!({"_jiuwenswarm_server_push":true})),
    ] {
        let mut raw = good.clone();
        raw[field] = value;
        let frame = Response::parse(&serde_json::to_vec(&raw).unwrap()).unwrap();
        assert!(frame.project(&req, "backend", "user").is_err());
    }
    for (field, value) in [
        ("protocol_version", json!("2.0")),
        ("is_final", json!(false)),
        ("sequence", json!(-1)),
        ("body", json!([])),
        ("response_kind", json!("unknown")),
    ] {
        let mut raw = good.clone();
        raw[field] = value;
        assert!(Response::parse(&serde_json::to_vec(&raw).unwrap()).is_err());
    }
    assert!(response("e2a.chunk", json!({"delta":"wrong unary frame"}))
        .project(&request("session.list"), "backend", "user")
        .is_err());
}

#[test]
fn plan_approval_is_a_business_event_with_its_content() {
    let frame=project("chat.send","plan.approval_required",json!({"plan_content":"Review this plan","plan_slug":"plan-1","plan_path":"/plans/plan-1.md"})).unwrap();
    assert_eq!(frame["event"], "plan.approval_required");
    assert_eq!(frame["payload"]["plan_content"], "Review this plan");
    assert_eq!(frame["payload"]["plan_slug"], "plan-1");
    assert_eq!(frame["payload"]["request_id"], "backend");
    assert_eq!(frame["payload"]["turn_request_id"], "client");
}

#[test]
fn negative_keepalive_is_transport_activity_without_business_output() {
    let raw = json!({"protocol_version":"1.0","request_id":"backend","sequence":-1,"is_final":false,"status":"in_progress","response_kind":"e2a.chunk","body":{"delta_kind":"custom","event_type":"keepalive","delta":{"event_type":"keepalive"}}});
    let parsed = Response::parse(&serde_json::to_vec(&raw).unwrap()).unwrap();
    let projected = parsed
        .project(&request("chat.send"), "backend", "user")
        .unwrap();
    assert!(!projected.complete);
    assert!(projected.frame.is_none());
}

#[test]
fn busy_state_follows_processing_and_interrupt_events_not_transport_final() {
    use data_plane_gateway::ingress::jiuwen::response::{Projection, SessionState};
    let mut state = SessionState::new(2).unwrap();
    let event = |name: &str, payload: Value| Projection {
        frame: Some(json!({"type":"event","event":name,"payload":payload})),
        complete: false,
    };
    state
        .observe(&event(
            "chat.processing_status",
            json!({"session_id":"session","is_processing":true}),
        ))
        .unwrap();
    state
        .observe(&Projection {
            frame: None,
            complete: true,
        })
        .unwrap();
    state
        .observe(&event(
            "chat.final",
            json!({"session_id":"session","content":"segment"}),
        ))
        .unwrap();
    let metadata = || {
        response(
            "e2a.complete",
            json!({"result":{"title":"session","is_processing":false}}),
        )
        .project(&request("session.get_metadata"), "backend", "user")
        .unwrap()
    };
    let mut value = metadata();
    state.augment_metadata(&request("session.get_metadata"), &mut value);
    assert_eq!(value.frame.unwrap()["payload"]["is_processing"], true);
    state
        .observe(&event(
            "chat.interrupt_result",
            json!({"session_id":"session","intent":"pause"}),
        ))
        .unwrap();
    let mut value = metadata();
    state.augment_metadata(&request("session.get_metadata"), &mut value);
    assert_eq!(value.frame.unwrap()["payload"]["is_processing"], true);
    state
        .observe(&event(
            "chat.interrupt_result",
            json!({"session_id":"session","intent":"cancel"}),
        ))
        .unwrap();
    let mut value = metadata();
    state.augment_metadata(&request("session.get_metadata"), &mut value);
    assert_eq!(value.frame.unwrap()["payload"]["is_processing"], false);
}

#[test]
fn busy_state_is_bounded_without_silently_evicting_active_sessions() {
    use data_plane_gateway::ingress::jiuwen::response::{Projection, SessionState};
    assert!(SessionState::new(0).is_err());
    let mut state = SessionState::new(1).unwrap();
    let event = |id: &str, busy: bool| Projection {
        frame: Some(
            json!({"type":"event","event":"chat.processing_status","payload":{"session_id":id,"is_processing":busy}}),
        ),
        complete: false,
    };
    state.observe(&event("session", true)).unwrap();
    assert!(state.observe(&event("other", true)).is_err());
    state.observe(&event("session", false)).unwrap();
    state.observe(&event("other", true)).unwrap();
}
