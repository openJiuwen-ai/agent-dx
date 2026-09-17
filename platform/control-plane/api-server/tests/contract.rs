use adx_api_server::contract::create_spec;
use adx_protocol::control as pb;
use serde_json::json;
fn caller() -> pb::CallerContext {
    pb::CallerContext {
        tenant_id: "verified".into(),
        administrator: false,
    }
}
#[test]
fn public_create_maps_directly_to_instance_and_uses_verified_identity() {
    let spec=create_spec(json!({"name":"case","namespace":"ns","tenant":"spoofed","image":"image","cpu":125,"memory":32,"storageMb":4,"env":{"USER_VALUE":"ok"},"idleTimeoutSeconds":15}), &caller()).unwrap();
    assert_eq!(spec.id, "ns-case");
    assert_eq!(spec.tenant_id, "verified");
    assert_eq!(spec.runtime, "runsc");
    let resources = spec.resources.unwrap();
    assert_eq!(
        (
            resources.cpu_millis,
            resources.memory_bytes,
            resources.disk_bytes
        ),
        (125, 32 * 1048576, 4 * 1048576)
    );
    assert_eq!(spec.env["USER_VALUE"], "ok");
    assert_eq!(spec.lifecycle.unwrap().idle_timeout_seconds, 15);
}
#[test]
fn clone_preserves_omitted_geometry_and_does_not_invent_image() {
    let spec = create_spec(json!({"name":"clone","snapshotId":"snapshot-1"}), &caller()).unwrap();
    assert_eq!(spec.snapshot_id.as_deref(), Some("snapshot-1"));
    assert!(spec.image.is_empty());
    assert!(spec.runtime.is_empty());
    assert_eq!(spec.resources.unwrap().cpu_millis, 0);
}
#[test]
fn unsupported_fields_are_explicit_not_silently_ignored() {
    for body in [
        json!({"image":"img","failover":true}),
        json!({"image":"img","mounts":[{"source":"/tmp"}]}),
        json!({"image":"img","ports":["8080"]}),
    ] {
        assert_eq!(
            create_spec(body, &caller()).unwrap_err().code(),
            tonic::Code::Unimplemented
        );
    }
    for body in [
        json!({"image":"img","cpu":-1}),
        json!({"image":"img","env":{"ADX_INSTANCE_ID":"fake"}}),
        json!({"image":"img","storageMb":0}),
        json!({"image":"img","scheduleAffinities":[{"weight":-1}]}),
    ] {
        assert_eq!(
            create_spec(body, &caller()).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
}
#[test]
fn instance_affinity_alternatives_stay_in_one_or_group() {
    let spec = create_spec(
        json!({"image":"img","name":"peer","scheduleAffinities":[
 {"kind":1,"affinity":2,"labelOps":[{"type":0,"labelKey":"app","labelValues":["a"]}]},
 {"kind":1,"affinity":2,"labelOps":[{"type":0,"labelKey":"app","labelValues":["b"]}]}]}),
        &caller(),
    )
    .unwrap();
    let groups = spec.scheduling.unwrap().placement_groups;
    assert_eq!(groups.len(), 1);
    assert!(groups[0].required);
    assert_eq!(groups[0].terms.len(), 2);
}

#[test]
fn runtime_conflicts_and_restart_ranges_are_rejected_before_rpc() {
    for input in [
        json!({"image":"img","runtime":"runc","rootfs":{"runtime":"fc"}}),
        json!({"image":"img","restartPolicy":{"maxAttempts":0,"initialBackoffSeconds":1,"maxBackoffSeconds":1}}),
        json!({"image":"img","createTimeoutSeconds":-1}),
    ] {
        assert_eq!(
            create_spec(input, &caller()).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
}
#[test]
fn required_node_anti_affinity_preserves_missing_label_semantics() {
    let spec=create_spec(json!({"image":"img","scheduleAffinities":[{"kind":0,"affinity":3,"labelOps":[{"type":1,"labelKey":"zone","labelValues":["a"]}]}]}),&caller()).unwrap();
    let p = spec.scheduling.unwrap();
    let g = &p.placement_groups[0];
    assert!(g.required && g.anti);
    let terms = &g.terms[0].selector.as_ref().unwrap().expressions;
    assert_eq!(terms[0].op, pb::SelectorOp::Exists as i32);
    assert_eq!(terms[1].op, pb::SelectorOp::NotIn as i32);
}

#[test]
fn create_timeout_compatibility_and_invalid_reserves() {
    use adx_api_server::contract::resolve_create_timeout as budget;
    assert_eq!(budget(0, 20, 0).unwrap(), 80);
    assert_eq!(budget(60, 30, 0).unwrap(), 90);
    assert_eq!(budget(120, 0, 10).unwrap(), 130);
    assert_eq!(budget(120, 20, 30).unwrap(), 120);
    for (c, s, i) in [(20, 0, 0), (60, 50, 0), (20, 30, 0), (0, 1, i64::MAX)] {
        assert!(budget(c, s, i).is_err());
    }
}
