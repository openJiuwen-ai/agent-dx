use adx_apiserver::contract::create_spec;
use adx_protocol::control::CallerContext;
use serde_json::json;

#[test]
fn default_local_rootfs_runs_explicit_process_and_readiness_without_image_inheritance() {
    let profile = serde_json::from_value(json!({
        "rootfs":{"type":"local","path":"/opt/adx/runtime/adx-runtime-rootfs.img","runtime_class":"runsc"},
        "bootstrap":{"type":"erofs","root":"/opt/adx/runtime/adx-runtime-rootfs.img",
            "target":"/__adx","entrypoint":["/__adx/usr/local/bin/adx-execd"]}
    })).unwrap();
    let process = json!({"commands":[["/opt/python/bin/python","-m","agentserver"]],
        "cwd":"/workspace"});
    let spec = adx_apiserver::contract::create_spec_with_environment(
        json!({"name":"local-workload","process":process,
            "readinessProbe":{"tcpSocket":{"port":18092},"periodSeconds":1,"timeoutSeconds":1}}),
        &CallerContext {
            tenant_id: "tenant".into(),
            ..Default::default()
        },
        Some(&profile),
    )
    .unwrap();
    let spec = adx_core::EnvironmentSpec::try_from(spec).unwrap();
    assert!(spec.image.is_empty());
    assert_eq!(spec.runtime_class, "runsc");
    assert_eq!(spec.runtime_profile, Some(profile));
    assert!(spec.execution.rootfs.is_none());
    assert!(!spec.execution.inherit_entrypoint);
    assert_eq!(
        serde_json::to_value(spec.execution.process).unwrap(),
        process
    );
    assert!(spec.execution.readiness_probe.is_some());
}

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
