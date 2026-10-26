use adx_apiserver::contract::create_spec;
use adx_protocol::control::CallerContext;
use serde_json::json;

#[test]
fn environment_creation_preserves_explicit_workload_process_through_rpc() {
    let process =
        json!({"commands":[["/bin/sh","-c","exec app"]],"user":"10001:10001","cwd":"/workspace"});
    let spec = create_spec(
        json!({
            "name":"process","image":"app:1","runtime":"runc",
            "inheritEntrypoint":true,"process":process
        }),
        &CallerContext {
            tenant_id: "tenant".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let spec = adx_core::EnvironmentSpec::try_from(spec).unwrap();
    assert_eq!(
        serde_json::to_value(spec.execution).unwrap()["process"],
        process
    );
}

#[test]
fn environment_creation_rejects_invalid_workload_process_before_scheduling() {
    for process in [
        json!({"commands":[]}),
        json!({"commands":[[]]}),
        json!({"commands":[[""]]}),
        json!({"commands":[["/bin/app","bad\u{0000}arg"]]}),
        json!({"user":""}),
        json!({"cwd":"../escape"}),
    ] {
        assert!(create_spec(
            json!({
                "name":"invalid","image":"app:1","runtime":"runc",
                "inheritEntrypoint":true,"process":process
            }),
            &CallerContext {
                tenant_id: "tenant".into(),
                ..Default::default()
            }
        )
        .is_err());
    }
}
