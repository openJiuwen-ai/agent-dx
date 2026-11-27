use adx_apiserver::contract::create_spec;
use adx_protocol::control as pb;
use serde_json::{json, Value};

fn execution(body: Value) -> Value {
    let caller = pb::CallerContext {
        tenant_id: "tenant".into(),
        administrator: false,
    };
    let spec = create_spec(body, &caller).unwrap();
    let options: adx_core::execution::ExecutionOptions =
        spec.execution.unwrap().try_into().unwrap();
    serde_json::to_value(options).unwrap()
}

#[test]
fn sandboxd_start_options_preserve_defaults_and_explicit_root() {
    let defaults = execution(json!({"image":"image"}));
    assert!(defaults.get("prefer_image_path").is_none());
    assert!(defaults.get("process_user").is_none());
    for (prefer, uid, gid) in [(true, 1001, 1002), (false, 0, 0)] {
        let options = execution(json!({"image":"image", "preferImagePath":prefer,
            "processUser":{"uid":uid,"gid":gid}, "env":{"PATH":"/caller/bin"}}));
        assert_eq!(options["prefer_image_path"], prefer);
        assert_eq!(options["process_user"], json!({"uid":uid,"gid":gid}));
        assert!(options.get("user").is_none());
        assert!(options.get("group").is_none());
    }
}

#[test]
fn sandboxd_start_options_validate_numeric_identity_and_backend_support() {
    let caller = pb::CallerContext {
        tenant_id: "tenant".into(),
        administrator: false,
    };
    for user in [
        json!({"uid":-1,"gid":0}),
        json!({"uid":0,"gid":4294967295u64}),
        json!({"uid":"1000","gid":0}),
        json!({"uid":true,"gid":0}),
        json!({"uid":0}),
    ] {
        assert!(create_spec(json!({"image":"image","processUser":user}), &caller).is_err());
    }
    assert!(create_spec(json!({"image":"image","preferImagePath":"true"}), &caller).is_err());
    for runtime in ["conch", "supervisor"] {
        assert!(create_spec(
            json!({"image":"image","runtime":runtime,"preferImagePath":true}),
            &caller
        )
        .is_err());
        assert!(create_spec(
            json!({"image":"image","runtime":runtime,"processUser":{"uid":0,"gid":0}}),
            &caller
        )
        .is_err());
        assert!(create_spec(
            json!({"image":"image","runtime":runtime,"preferImagePath":false}),
            &caller
        )
        .is_ok());
    }
}

#[test]
fn sandboxd_accepts_numeric_tool_identity() {
    for runtime in ["runsc", "runc", "kata"] {
        let options = execution(json!({"image":"image", "runtime":runtime,
            "user":"1001", "group":"1002"}));
        assert_eq!(options["user"], "1001");
        assert_eq!(options["group"], "1002");
    }
    let options =
        execution(json!({"image":"image", "runtime":"conch", "user":"alice", "group":"staff"}));
    assert_eq!(options["user"], "alice");
    assert_eq!(options["group"], "staff");
    let caller = pb::CallerContext {
        tenant_id: "tenant".into(),
        administrator: false,
    };
    assert!(create_spec(
        json!({"image":"image", "user":"1001", "group":"1002",
        "processUser":{"uid":0,"gid":0}}),
        &caller
    )
    .is_err());
}

#[test]
fn restore_accepts_explicit_target_configuration() {
    let options = execution(json!({"snapshotId":"source", "preferImagePath":false,
        "processUser":{"uid":1000,"gid":1001}}));
    assert_eq!(options["prefer_image_path"], false);
    assert_eq!(options["process_user"], json!({"uid":1000,"gid":1001}));
}
