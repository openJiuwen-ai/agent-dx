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
fn sandboxd_options_are_typed_and_preserved() {
    let spec = create_spec(json!({
        "image":"registry.example/app:v1",
        "cpu":500,"cpu_limit":1500,"memory":256,"mem_limit":512,
        "storageMb":32,"storage_limit_mb":64,
        "failover":true,"inheritEntrypoint":true,
        "ports":["8080"],
        "mounts":[{"type":"bind","target":"/models","options":["ro"],"image_url":"registry.example/models:v2"}],
        "network":{"schemaVersion":2,"traffic":{"ingressDefaultAction":"deny","egressDefaultAction":"allow","mode":"stateful","rules":[{"action":"allow","direction":"ingress","protocol":"tcp","sandboxPort":8080,"priority":100}]}},
        "dataPlane":{"tunnelSecurityMode":"tls","portForwardSecurityMode":"tls-token"},
        "extra_config":{"networkStack":"netstack"}
    }), &caller()).unwrap();
    let options = spec.sandbox.unwrap();
    assert!(options.failover && options.inherit_entrypoint);
    assert_eq!(options.ports, vec![8080]);
    assert_eq!(options.mounts.len(), 1);
    assert!(options.network.unwrap().traffic.is_some());
    assert_eq!(
        options.data_plane.unwrap().tunnel,
        pb::DataPlaneSecurityMode::DataPlaneSecurityTls as i32
    );
    let limits = options.limits.unwrap();
    assert_eq!(
        (limits.cpu_millis, limits.memory_bytes, limits.disk_bytes),
        (1500, 512 << 20, 64 << 20)
    );
    assert_eq!(options.extra_config, r#"{"networkStack":"netstack"}"#);
}

#[test]
fn s3_rootfs_and_mount_credentials_follow_sandboxd_contract() {
    let spec = create_spec(json!({
        "rootfs":{"runtime":"runsc","type":"s3","storageInfo":{"endpoint":"https://s3.example","bucket":"rootfs","object":"base.erofs","accessKey":"key","secretKey":"secret"}},
        "mounts":[{"type":"erofs","target":"/weights","options":["ro"],"s3_config":{"endpoint":"https://s3.example","bucket":"models","object":"weights.erofs"}}]
    }), &caller()).unwrap();
    let options = spec.sandbox.unwrap();
    assert!(matches!(
        options.rootfs.unwrap().source.unwrap().source.unwrap(),
        pb::storage_source::Source::S3(_)
    ));
    assert!(matches!(
        options.mounts[0]
            .source
            .as_ref()
            .unwrap()
            .source
            .as_ref()
            .unwrap(),
        pb::storage_source::Source::S3(_)
    ));
}

#[test]
fn unsafe_or_inconsistent_sandbox_options_are_rejected() {
    for body in [
        json!({"image":"img","mounts":[{"type":"bind","target":"/data","hostPath":"/host"}]}),
        json!({"rootfs":{"runtime":"runsc","type":"local","path":"/host/root"}}),
        json!({"image":"img","cpu":1000,"cpu_limit":500}),
    ] {
        assert_eq!(
            create_spec(body, &caller()).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
    for body in [
        json!({"image":"img","cpu":-1}),
        json!({"image":"img","env":{"ADX_INSTANCE_ID":"fake"}}),
        json!({"image":"img","storageMb":0}),
        json!({"image":"img","ports":["0"]}),
        json!({"image":"img","ports":["8080", "8080"]}),
        json!({"image":"img","scheduleAffinities":[{"weight":-1}]}),
    ] {
        assert_eq!(
            create_spec(body, &caller()).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
}

#[test]
fn rootfs_overlay_rejects_ambiguous_or_incomplete_sources_before_rpc() {
    let s3 = json!({
        "endpoint":"https://s3.example",
        "bucket":"rootfs",
        "object":"base.erofs"
    });
    for body in [
        json!({"rootfs":{"runtime":""}}),
        json!({"rootfs":{"runtime":false}}),
        json!({"rootfs":{"readonly":"true"}}),
        json!({"rootfs":{"imageurl":"image:v1"}}),
        json!({"rootfs":{"path":"/unexpected"}}),
        json!({"rootfs":{"storageInfo":s3.clone()}}),
        json!({"rootfs":{"type":""}}),
        json!({"rootfs":{"type":false}}),
        json!({"rootfs":{"type":"image"}}),
        json!({"rootfs":{"type":"image","imageurl":false}}),
        json!({"rootfs":{"type":"image","imageurl":"image:v1","path":"/unexpected"}}),
        json!({"rootfs":{"type":"image","imageurl":"image:v1","storageInfo":s3.clone()}}),
        json!({"rootfs":{"type":"s3"}}),
        json!({"rootfs":{"type":"s3","storageInfo":"invalid"}}),
        json!({"rootfs":{"type":"s3","storageInfo":{"bucket":"rootfs","object":"base.erofs"}}}),
        json!({"rootfs":{"type":"s3","storageInfo":{"endpoint":"https://s3.example","object":"base.erofs"}}}),
        json!({"rootfs":{"type":"s3","storageInfo":{"endpoint":"https://s3.example","bucket":"rootfs"}}}),
        json!({"rootfs":{"type":"s3","storageInfo":{"endpoint":"https://s3.example","bucket":"rootfs","object":"base.erofs","accessKey":false}}}),
        json!({"rootfs":{"type":"s3","storageInfo":{"endpoint":"https://s3.example","bucket":"rootfs","object":"base.erofs","accessKey":"key"}}}),
        json!({"rootfs":{"type":"s3","storageInfo":s3.clone(),"imageurl":"image:v1"}}),
        json!({"rootfs":{"type":"s3","storageInfo":s3.clone(),"path":"/unexpected"}}),
        json!({"rootfs":{"type":"local"}}),
        json!({"rootfs":{"type":"local","path":false}}),
        json!({"rootfs":{"type":"local","path":"/unexpected","imageurl":"image:v1"}}),
        json!({"rootfs":{"type":"local","path":"/unexpected","storageInfo":s3.clone()}}),
        json!({"rootfs":{"type":"unknown"}}),
        json!({"image":"image:v1","rootfs":{"type":"s3","storageInfo":s3.clone()}}),
    ] {
        assert_eq!(
            create_spec(body, &caller()).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
}

#[test]
fn declared_forwarded_ports_are_validated_and_accepted() {
    let spec = create_spec(json!({"image":"img","ports":["8080", "65535"]}), &caller()).unwrap();
    assert_eq!(spec.image, "img");
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

#[test]
fn create_and_central_queue_timeouts_are_distinct() {
    let timeouts = adx_api_server::contract::create_timeouts(&json!({
        "image": "img",
        "createTimeoutSeconds": 120,
        "scheduleTimeoutSeconds": 20,
        "initCallTimeoutSeconds": 30
    }))
    .unwrap();
    assert_eq!(timeouts.create_seconds, 120);
    assert_eq!(timeouts.schedule_seconds, 20);

    let defaults = adx_api_server::contract::create_timeouts(&json!({"image": "img"})).unwrap();
    assert_eq!(defaults.schedule_seconds, 30);
}

#[test]
fn deployment_environment_supplies_default_root_and_runtime_but_snapshot_inherits_source() {
    let environment = serde_json::from_value(json!({
        "rootfs":{"runtime":"runc","type":"local","path":"/opt/adx/runtime/rootfs.img","readonly":false},
        "bootstrap":{"type":"erofs","root":"/opt/adx/runtime/rootfs.img","target":"/__adx",
          "entrypoint":["/__adx/usr/local/bin/rrt-runtime"]}
    })).unwrap();
    let create = |body| {
        adx_api_server::contract::create_spec_with_environment(body, &caller(), Some(&environment))
            .unwrap()
    };
    let default = create(json!({"name":"default"}));
    assert!(default.image.is_empty());
    assert_eq!(default.runtime, "runc");
    assert!(default.runtime_environment.is_some());
    let runtime_only = create(json!({"rootfs":{"runtime":"firecracker"}}));
    assert!(runtime_only.image.is_empty());
    assert_eq!(runtime_only.runtime, "firecracker");
    let runtime_environment = runtime_only.runtime_environment.unwrap();
    assert_eq!(runtime_environment.rootfs.unwrap().runtime, "firecracker");
    assert!(runtime_only.sandbox.unwrap().rootfs.is_none());

    let readonly_only = create(json!({"rootfs":{"readonly":true}}));
    assert!(readonly_only.image.is_empty());
    assert!(
        readonly_only
            .runtime_environment
            .unwrap()
            .rootfs
            .unwrap()
            .readonly
    );
    assert!(readonly_only.sandbox.unwrap().rootfs.is_none());

    let custom = create(json!({"image":"ubuntu:24.04"}));
    assert_eq!(custom.image, "ubuntu:24.04");
    assert_eq!(custom.runtime_environment, default.runtime_environment);
    let clone = create(json!({"snapshotId":"saved"}));
    assert!(clone.runtime_environment.is_none());
    assert!(clone.runtime.is_empty());
}

#[test]
fn rootfs_source_replacement_inherits_default_readonly_unless_explicitly_overridden() {
    let environment = serde_json::from_value(json!({
        "rootfs":{"runtime":"runc","type":"local","path":"/opt/adx/runtime/rootfs.img","readonly":true},
        "bootstrap":{"type":"erofs","root":"/opt/adx/runtime/rootfs.img","target":"/__adx",
          "entrypoint":["/__adx/usr/local/bin/rrt-runtime"]}
    }))
    .unwrap();
    let create = |rootfs| {
        adx_api_server::contract::create_spec_with_environment(
            json!({"rootfs":rootfs}),
            &caller(),
            Some(&environment),
        )
        .unwrap()
    };

    let inherited = create(json!({"type":"image","imageurl":"app:v1"}));
    assert!(inherited.sandbox.unwrap().rootfs.unwrap().readonly);

    let writable = create(json!({
        "type":"image",
        "imageurl":"app:v1",
        "readonly":false
    }));
    assert!(!writable.sandbox.unwrap().rootfs.unwrap().readonly);
}
