#![cfg(feature = "agent-api")]
use data_plane_gateway::ingress::jiuwen::{
    protocol::Request,
    response::Response,
    session::{Session, SessionLimits},
};
use serde_json::{json, Value};
fn request(id: &str, method: &str) -> Request {
    Request::parse(
        &serde_json::to_vec(
            &json!({"type":"req","id":id,"method":method,"params":{"session_id":"s"}}),
        )
        .unwrap(),
    )
    .unwrap()
}
fn wire(id: &str, push: bool, final_frame: bool, payload: Value) -> Response {
    Response::parse(&serde_json::to_vec(&json!({"protocol_version":"1.0","request_id":id,"sequence":0,"is_final":final_frame,"status":if final_frame {"succeeded"}else{"in_progress"},"response_kind":if final_frame{"e2a.complete"}else{"e2a.chunk"},"body":if final_frame{json!({"result":payload})}else{json!({"delta_kind":"custom","delta":payload})},"session_id":"s","metadata":{"_jiuwenswarm_server_push":push}})).unwrap()).unwrap()
}
fn session() -> Session {
    Session::new(
        "user".into(),
        SessionLimits {
            pending: 2,
            recent: 2,
            busy: 2,
        },
    )
    .unwrap()
}
#[test]
fn requests_have_unique_backend_ids_and_stale_replies_cannot_complete_new_requests() {
    let mut session = session();
    let a = session
        .submit(request("a", "session.get_metadata"))
        .unwrap();
    let b = session
        .submit(request("b", "session.get_metadata"))
        .unwrap();
    let aid = a.envelope["request_id"].as_str().unwrap();
    let bid = b.envelope["request_id"].as_str().unwrap();
    assert_ne!(aid, bid);
    assert!(session.submit(request("a", "chat.send")).is_err());
    assert!(session.submit(request("c", "chat.send")).is_err());
    let frame = session
        .receive(&wire(aid, false, true, json!({"title":"A"})))
        .unwrap()
        .unwrap();
    assert_eq!(frame["id"], "a");
    assert_eq!(frame["payload"]["is_processing"], false);
    assert_eq!(session.pending_len(), 1);
    assert!(session
        .receive(&wire(aid, false, true, json!({"title":"late"})))
        .unwrap()
        .is_none());
    assert!(
        session.submit(request("a", "chat.send")).is_err(),
        "recent client IDs cannot be reused while late push aliases remain"
    );
    assert!(session.submit(request("c", "chat.send")).is_ok());
}
#[test]
fn push_does_not_complete_an_rpc_and_preserves_interaction_ids_even_on_collision() {
    let mut session = session();
    let prepared = session.submit(request("client", "chat.send")).unwrap();
    let id = prepared.envelope["request_id"].as_str().unwrap();
    let push = wire(
        id,
        true,
        true,
        json!({"event_type":"chat.ask_user_question","request_id":id,"turn_request_id":id,"content":"approve?"}),
    );
    let frame = session.receive(&push).unwrap().unwrap();
    assert_eq!(
        frame["payload"]["request_id"], id,
        "interaction identity is not a transport alias"
    );
    assert_eq!(frame["payload"]["turn_request_id"], "client");
    assert_eq!(session.pending_len(), 1);
    session.receive(&wire(id, false, true, json!({}))).unwrap();
}
#[test]
fn recent_turn_aliases_apply_to_followup_input_and_unrelated_push_is_session_scoped() {
    let mut session = session();
    let first = session.submit(request("first", "chat.send")).unwrap();
    let old = first.envelope["request_id"].as_str().unwrap();
    session.receive(&wire(old, false, true, json!({}))).unwrap();
    let follow = session.submit(request("follow", "chat.send")).unwrap();
    let new = follow.envelope["request_id"].as_str().unwrap();
    let frame=session.receive(&wire(new,false,false,json!({"event_type":"chat.input_received","turn_request_id":old,"input_request_id":new,"content":"extra"}))).unwrap().unwrap();
    assert_eq!(frame["payload"]["turn_request_id"], "first");
    assert_eq!(frame["payload"]["input_request_id"], "follow");
    let frame = session
        .receive(&wire(
            "background",
            true,
            false,
            json!({"event_type":"chat.processing_status","is_processing":true}),
        ))
        .unwrap()
        .unwrap();
    assert_eq!(frame["payload"]["session_id"], "s");
    assert_eq!(frame["payload"]["request_id"], "background");
}
#[test]
fn push_owner_mismatch_is_rejected_and_terminal_push_only_retires_matching_stream() {
    let mut session = session();
    let stream = session.submit(request("chat", "chat.send")).unwrap();
    let unary = session
        .submit(request("meta", "session.get_metadata"))
        .unwrap();
    let stream_id = stream.envelope["request_id"].as_str().unwrap();
    let unary_id = unary.envelope["request_id"].as_str().unwrap();
    let raw = json!({"protocol_version":"1.0","request_id":stream_id,"sequence":0,"is_final":false,"status":"in_progress","response_kind":"e2a.chunk","body":{"delta_kind":"custom","delta":{"event_type":"chat.file"}},"user_id":"other","session_id":"s","metadata":{"_jiuwenswarm_server_push":true}});
    assert!(session
        .receive(&Response::parse(&serde_json::to_vec(&raw).unwrap()).unwrap())
        .is_err());
    session
        .receive(&wire(unary_id, true, true, json!({})))
        .unwrap();
    assert_eq!(session.pending_len(), 2);
    session
        .receive(&wire(stream_id, true, true, json!({})))
        .unwrap();
    assert_eq!(session.pending_len(), 1);
}

#[test]
fn client_id_that_equals_another_backend_id_is_not_remapped_twice() {
    let mut session = session();
    let first = session.submit(request("first", "chat.send")).unwrap();
    let old = first.envelope["request_id"].as_str().unwrap();
    let second = session.submit(request(old, "chat.send")).unwrap();
    let new = second.envelope["request_id"].as_str().unwrap();
    let frame = session
        .receive(&wire(
            new,
            false,
            false,
            json!({"event_type":"runtime.accepted","request_id":new}),
        ))
        .unwrap()
        .unwrap();
    assert_eq!(frame["payload"]["request_id"], old);
}
