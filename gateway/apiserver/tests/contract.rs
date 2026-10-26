use adx_apiserver::contract::create_spec;
use adx_protocol::control as pb;
use serde_json::json;
fn caller() -> pb::CallerContext {
    pb::CallerContext {
        tenant_id: "verified".into(),
        administrator: false,
    }
}
#[test]
fn explicit_forever_pause_retention_is_accepted() {
    let result = create_spec(
        json!({"image":"image","idleTimeoutSeconds":30,"idleAction":"pause",
        "pauseOptions":{"retention":{"kind":"forever"},"storage":"shared"}}),
        &caller(),
    );
    assert!(result.is_ok(), "forever retention rejected: {result:?}");
}
#[test]
fn idle_pause_options_are_typed_and_validated() {
    let spec = create_spec(json!({"image":"image", "idleTimeoutSeconds":30,"idleAction":"pause",
        "pauseOptions":{"retention":{"kind":"ttl","ttlSeconds":3600},"timeoutSeconds":120,"storage":"shared"}}), &caller()).unwrap();
    let policy = spec.lifecycle.unwrap();
    assert_eq!(policy.idle_action, pb::IdleAction::Pause as i32);
    assert_eq!(
        policy.pause.unwrap().storage,
        pb::CheckpointStorage::Shared as i32
    );
    for value in [
        json!({"image":"image","idleAction":"pause"}),
        json!({"image":"image","pauseOptions":{"retention":{"kind":"ttl","ttlSeconds":0}}}),
        json!({"image":"image","pauseOptions":{"storage":"missing"}}),
    ] {
        assert!(create_spec(value.clone(), &caller()).is_err(), "{value}");
    }
}
#[test]
fn public_create_maps_directly_to_instance_and_uses_verified_identity() {
    let spec=create_spec(json!({"name":"case","namespace":"ns","tenant":"spoofed","image":"image","cpu":125,"memory":32,"storageMb":4,"env":{"USER_VALUE":"ok"},"idleTimeoutSeconds":15}), &caller()).unwrap();
    assert_eq!(spec.id, "ns-case");
    assert_eq!(spec.tenant_id, "verified");
    assert_eq!(spec.runtime_class, "runsc");
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
    assert!(spec.runtime_class.is_empty());
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
        "network":{"schemaVersion":2,"traffic":{"ingressDefaultAction":"deny","egressDefaultAction":"allow","mode":"stateful","rules":[{"action":"allow","direction":"ingress","protocol":"tcp","environmentPort":8080,"priority":100}]}},
        "dataPlane":{"tunnelSecurityMode":"tls","portForwardSecurityMode":"tls-token"},
        "extra_config":{"networkStack":"netstack"}
    }), &caller()).unwrap();
    let options = spec.execution.unwrap();
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
    let options = spec.execution.unwrap();
    assert!(matches!(
        options.rootfs.unwrap().source.unwrap().source.unwrap(),
        pb::storage_source::Source::S3(_)
    ));
    assert!(matches!(
        options.mounts[0].mount_source.as_ref(),
        Some(pb::mount_spec::MountSource::Source(pb::StorageSource {
            source: Some(pb::storage_source::Source::S3(_))
        }))
    ));
}

#[test]
fn unsafe_or_inconsistent_environment_options_are_rejected() {
    for body in [
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
        json!({"image":"img","env":{"ADX_ENVIRONMENT_ID":"fake"}}),
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
    use adx_apiserver::contract::resolve_create_timeout as budget;
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
    let timeouts = adx_apiserver::contract::create_timeouts(&json!({
        "image": "img",
        "createTimeoutSeconds": 120,
        "scheduleTimeoutSeconds": 20,
        "initCallTimeoutSeconds": 30
    }))
    .unwrap();
    assert_eq!(timeouts.create_seconds, 120);
    assert_eq!(timeouts.schedule_seconds, 20);

    let defaults = adx_apiserver::contract::create_timeouts(&json!({"image": "img"})).unwrap();
    assert_eq!(defaults.schedule_seconds, 30);
}

#[test]
fn deployment_environment_supplies_default_root_and_runtime_but_snapshot_inherits_source() {
    let environment = serde_json::from_value(json!({
        "rootfs":{"runtime_class":"runc","type":"local","path":"/opt/adx/runtime/rootfs.img","readonly":false},
        "bootstrap":{"type":"erofs","root":"/opt/adx/runtime/rootfs.img","target":"/__adx",
          "entrypoint":["/__adx/usr/local/bin/adx-execd"]}
    })).unwrap();
    let create = |body| {
        adx_apiserver::contract::create_spec_with_environment(body, &caller(), Some(&environment))
            .unwrap()
    };
    let default = create(json!({"name":"default"}));
    assert!(default.image.is_empty());
    assert_eq!(default.runtime_class, "runc");
    assert!(default.runtime_profile.is_some());
    let runtime_only = create(json!({"rootfs":{"runtime":"firecracker"}}));
    assert!(runtime_only.image.is_empty());
    assert_eq!(runtime_only.runtime_class, "firecracker");
    let environment = runtime_only.runtime_profile.unwrap();
    assert_eq!(environment.rootfs.unwrap().runtime_class, "firecracker");
    assert!(runtime_only.execution.unwrap().rootfs.is_none());

    let readonly_only = create(json!({"rootfs":{"readonly":true}}));
    assert!(readonly_only.image.is_empty());
    assert!(
        readonly_only
            .runtime_profile
            .unwrap()
            .rootfs
            .unwrap()
            .readonly
    );
    assert!(readonly_only.execution.unwrap().rootfs.is_none());

    let custom = create(json!({"image":"ubuntu:24.04"}));
    assert_eq!(custom.image, "ubuntu:24.04");
    assert_eq!(custom.runtime_profile, default.runtime_profile);
    let clone = create(json!({"snapshotId":"saved"}));
    assert!(clone.runtime_profile.is_none());
    assert!(clone.runtime_class.is_empty());
}

#[test]
fn rootfs_source_replacement_inherits_default_readonly_unless_explicitly_overridden() {
    let environment = serde_json::from_value(json!({
        "rootfs":{"runtime_class":"runc","type":"local","path":"/opt/adx/runtime/rootfs.img","readonly":true},
        "bootstrap":{"type":"erofs","root":"/opt/adx/runtime/rootfs.img","target":"/__adx",
          "entrypoint":["/__adx/usr/local/bin/adx-execd"]}
    }))
    .unwrap();
    let create = |rootfs| {
        adx_apiserver::contract::create_spec_with_environment(
            json!({"rootfs":rootfs}),
            &caller(),
            Some(&environment),
        )
        .unwrap()
    };

    let inherited = create(json!({"type":"image","imageurl":"app:v1"}));
    assert!(inherited.execution.unwrap().rootfs.unwrap().readonly);

    let writable = create(json!({
        "type":"image",
        "imageurl":"app:v1",
        "readonly":false
    }));
    assert!(!writable.execution.unwrap().rootfs.unwrap().readonly);
}

#[test]
fn workload_probes_preserve_actions_defaults_and_initialization_budget() {
    let spec = create_spec(
        json!({"image":"server", "inheritEntrypoint":true,
            "initCallTimeoutSeconds":120,
            "readinessProbe":{"httpGet":{"port":8080,"path":"/ready"}},
            "healthcheckProbe":{"exec":{"command":["/app/check"]},"failureThreshold":4}
        }),
        &caller(),
    )
    .unwrap();
    let options: adx_core::execution::ExecutionOptions =
        spec.execution.unwrap().try_into().unwrap();
    assert_eq!(options.init_timeout_seconds, 120);
    let readiness = options.readiness_probe.unwrap();
    assert_eq!(
        readiness.action,
        adx_core::probes::ProbeAction::HttpGet {
            port: 8080,
            path: "/ready".into()
        }
    );
    assert_eq!(
        (
            readiness.period_seconds,
            readiness.timeout_seconds,
            readiness.failure_threshold
        ),
        (2, 1, 1)
    );
    let healthcheck = options.healthcheck_probe.unwrap();
    assert_eq!(
        (
            healthcheck.period_seconds,
            healthcheck.timeout_seconds,
            healthcheck.failure_threshold
        ),
        (10, 2, 4)
    );
}

#[test]
fn malformed_or_inapplicable_probes_are_rejected_before_assignment() {
    for body in [
        json!({"image":"server","readinessProbe":{"tcpSocket":{"port":80}}}),
        json!({"image":"server","inheritEntrypoint":true,"readinessProbe":{"tcpSocket":{"port":80},"failureThreshold":1}}),
        json!({"image":"server","inheritEntrypoint":true,"healthcheckProbe":{"tcpSocket":{"port":80},"exec":{"command":["true"]}}}),
        json!({"image":"server","inheritEntrypoint":true,"readinessProbe":{"httpGet":{"port":80,"path":"/ready\r\nX: injected"}}}),
        json!({"image":"server","inheritEntrypoint":true,"healthcheckProbe":{"exec":{"command":[]}}}),
        json!({"image":"server","inheritEntrypoint":true,"healthcheckProbe":{"tcpSocket":{"port":65536}}}),
        json!({"image":"server","initCallTimeoutSeconds":86401}),
    ] {
        assert_eq!(
            create_spec(body, &caller()).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
}

#[test]
fn constructor_cwd_is_preserved_and_invalid_paths_are_rejected() {
    let spec = create_spec(
        json!({"name":"cwd", "image":"image", "cwd":"/workspace/nested"}),
        &caller(),
    )
    .unwrap();
    let model: adx_core::EnvironmentSpec = spec.try_into().unwrap();
    assert_eq!(model.execution.cwd.as_deref(), Some("/workspace/nested"));
    for cwd in [
        "",
        "relative/path",
        "/workspace\ninvalid",
        "/workspace\0invalid",
    ] {
        let error = create_spec(json!({"image":"image", "cwd":cwd}), &caller()).unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
    assert!(create_spec(json!({"image":"image"}), &caller())
        .unwrap()
        .execution
        .unwrap()
        .cwd
        .is_none());
}

#[test]
fn local_mount_accepts_both_spellings_and_preserves_rw() {
    for field in ["host_path", "hostPath"] {
        let mut mount = json!({"type":"bind","target":"/home/jiuwen","options":["rw"],"create_if_missing":true,"create_options":{"uid":10001,"gid":10001,"mode":"0700"}});
        mount[field] = json!("/mnt/sfs/workspaces/user-a");
        let spec = create_spec(json!({"image":"app", "mounts":[mount]}), &caller()).unwrap();
        let mounts = spec.execution.unwrap().mounts;
        assert_eq!(mounts[0].options, ["rw"]);
        assert!(mounts[0].create_if_missing);
        let creation = mounts[0].create_options.as_ref().unwrap();
        assert_eq!(
            (creation.uid, creation.gid, creation.mode.as_str()),
            (10001, 10001, "0700")
        );
        assert!(matches!(mounts[0].mount_source.as_ref(),
            Some(pb::mount_spec::MountSource::Source(pb::StorageSource { source: Some(pb::storage_source::Source::Local(path)) })) if path == "/mnt/sfs/workspaces/user-a"));
    }
}

#[test]
fn mount_requires_exactly_one_source_for_every_combination() {
    for image in [false, true] {
        for s3 in [false, true] {
            for local in [false, true] {
                let mut mount = json!({"type":"bind","target":"/data","options":["rw"]});
                if image {
                    mount["image_url"] = json!("registry/models:v1");
                }
                if s3 {
                    mount["s3_config"] = json!({"endpoint":"https://s3.example","bucket":"data","object":"data.erofs"});
                }
                if local {
                    mount["host_path"] = json!("/mnt/sfs/user");
                }
                let result = create_spec(json!({"image":"app", "mounts":[mount]}), &caller());
                assert_eq!(
                    result.is_ok(),
                    [image, s3, local].into_iter().filter(|v| *v).count() == 1,
                    "image={image}, s3={s3}, local={local}: {result:?}"
                );
            }
        }
    }
}

#[test]
fn local_mount_rejects_relative_traversal_and_control_characters() {
    for path in [
        "relative",
        "/data/../etc",
        "/data/./user",
        "/data/\u{0}user",
        "/data/\nuser",
    ] {
        let result = create_spec(
            json!({"image":"app", "mounts":[{"type":"bind","target":"/data","host_path":path}]}),
            &caller(),
        );
        assert_eq!(
            result.unwrap_err().code(),
            tonic::Code::InvalidArgument,
            "{path:?}"
        );
    }
}

#[test]
fn directory_creation_rejects_invalid_sources_and_attributes() {
    for mount in [
        json!({"type":"bind","target":"/data","image_url":"app","create_if_missing":true}),
        json!({"type":"erofs","target":"/data","host_path":"/mnt/sfs/data","create_if_missing":true}),
        json!({"type":"bind","target":"/data","host_path":"/mnt/sfs/data","create_options":{"uid":10001,"gid":10001,"mode":"0700"}}),
    ] {
        assert!(create_spec(json!({"image":"app","mounts":[mount]}), &caller()).is_err());
    }
    for options in [
        json!({"uid":-1,"gid":10001,"mode":"0700"}),
        json!({"uid":4294967295u64,"gid":10001,"mode":"0700"}),
        json!({"uid":10001,"gid":4294967295u64,"mode":"0700"}),
        json!({"uid":10001,"gid":10001,"mode":"755"}),
        json!({"uid":10001,"gid":10001,"mode":"0788"}),
        json!({"uid":10001,"gid":10001,"mode":"1777"}),
        json!({"uid":10001,"mode":"0700"}),
    ] {
        let mount = json!({"type":"bind","target":"/data","host_path":"/mnt/sfs/data","create_if_missing":true,"create_options":options});
        assert_eq!(
            create_spec(json!({"image":"app","mounts":[mount]}), &caller())
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
}

#[test]
fn tunnel_listener_configuration_cannot_be_supplied_through_user_environment() {
    for key in ["EXECD_TUNNEL_WS_PORT", "EXECD_TUNNEL_HTTP_PORT"] {
        assert!(
            create_spec(json!({"image":"image", "env":{key:"12345"}}), &caller()).is_err(),
            "accepted {key}"
        );
    }
}

#[test]
fn tunnel_configuration_is_not_stored_in_untrusted_environment() {
    let spec = create_spec(
        json!({"image":"image", "tunnel":{"enabled":true,"proxyPort":18766}}),
        &caller(),
    )
    .unwrap();
    assert!(!spec.env.contains_key("EXECD_TUNNEL_WS_PORT"));
    assert!(!spec.env.contains_key("EXECD_TUNNEL_HTTP_PORT"));
}

#[test]
fn tunnel_listeners_roundtrip_with_default_and_custom_ports() {
    for (input, expected) in [(0, 8766), (18766, 18766)] {
        let spec = create_spec(json!({"image":"image", "tunnel":{"enabled":true,"proxyPort":input},"network":{"blockNetwork":true}}), &caller()).unwrap();
        let options: adx_core::execution::ExecutionOptions =
            spec.execution.unwrap().try_into().unwrap();
        assert_eq!(options.tunnel_http_port, Some(expected));
        let wire: pb::ExecutionOptions = options.clone().into();
        let restored: adx_core::execution::ExecutionOptions = wire.try_into().unwrap();
        assert_eq!(restored, options);
    }
    for request in [
        json!({"image":"image","tunnel":{"enabled":true,"proxyPort":65536}}),
        json!({"image":"image","tunnel":{"enabled":true,"proxyPort":50090}}),
        json!({"image":"image","tunnel":{"enabled":true,"proxyPort":50091}}),
        json!({"image":"image","tunnel":{"enabled":true},"ports":["8765"]}),
        json!({"image":"image","tunnel":{"enabled":true},"network":{"schemaVersion":2,"traffic":{"ingressDefaultAction":"deny","egressDefaultAction":"deny","mode":"stateless","rules":[]}}}),
    ] {
        assert!(
            create_spec(request.clone(), &caller()).is_err(),
            "accepted {request}"
        );
    }
}

#[test]
fn tool_identity_is_preserved_and_invalid_pairs_are_rejected() {
    let spec = create_spec(
        json!({"image":"tools:1","runtime":"conch","user":"1001","group":"1002"}),
        &caller(),
    )
    .unwrap();
    let execution = spec.execution.unwrap();
    assert_eq!(execution.user.as_deref(), Some("1001"));
    assert_eq!(execution.group.as_deref(), Some("1002"));
    for body in [
        json!({"image":"tools:1","user":"1001"}),
        json!({"image":"tools:1","group":"1002"}),
        json!({"image":"tools:1","user":"","group":"1002"}),
        json!({"image":"tools:1","user":"a:b","group":"1002"}),
        json!({"image":"tools:1","user":"4294967295","group":"1002"}),
        json!({"image":"tools:1","user":"1001","group":"1002","inheritEntrypoint":true}),
    ] {
        assert!(create_spec(body.clone(), &caller()).is_err(), "{body}");
    }
}

#[test]
fn volume_mounts_are_exclusive_with_inline_sources_and_keep_fixed_uid() {
    let mut input = json!({"namespace":"test","name":"vol","image":"app","runtime":"runc","cpu":1000,"memory":128,
        "mounts":[{"type":"bind","target":"/workspace","volume_id":"workspace","volume_uid":"fixed-uid","options":["rw"]}]});
    let spec = create_spec(input.clone(), &caller()).unwrap();
    let spec: adx_core::EnvironmentSpec = spec.try_into().unwrap();
    assert!(
        matches!(&spec.execution.mounts[0].source, adx_core::execution::MountSource::Volume(reference)
        if reference.id == "workspace" && reference.uid.as_deref()==Some("fixed-uid"))
    );
    input["mounts"][0]["host_path"] = json!("/mnt/data");
    assert!(create_spec(input.clone(), &caller()).is_err());
    input["mounts"][0]
        .as_object_mut()
        .unwrap()
        .remove("host_path");
    input["mounts"][0]["create_if_missing"] = json!(true);
    assert!(create_spec(input, &caller()).is_err());
}
