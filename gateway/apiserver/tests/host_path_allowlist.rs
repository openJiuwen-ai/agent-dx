//! Deployment-owned source authorization shared by Environment creation paths.
use adx_apiserver::{
    clients::Clients, config::Config, contract, environment_service::EnvironmentService,
};
use adx_protocol::control as pb;
use serde_json::{json, Value};
use tonic::Code;

fn service(allowed: Option<Value>) -> std::sync::Arc<EnvironmentService> {
    let mut settings = json!({
        "listen": "127.0.0.1:0",
        "coordinator_address": "http://127.0.0.1:1",
        "internal_security": "network",
        "ingress_mode": "standalone",
        "rpc_timeout_seconds": 1,
        "cache_entries": 16,
        "auth_cache_ttl_seconds": 1,
    });
    if let Some(allowed) = allowed {
        settings["allowed_host_paths"] = allowed;
    }
    let config: Config = serde_json::from_value(settings).unwrap();
    config.validate().unwrap();
    EnvironmentService::new(Clients::new(config).unwrap())
}

fn caller() -> pb::CallerContext {
    pb::CallerContext {
        tenant_id: "tenant".into(),
        administrator: false,
    }
}

fn request(source: &str) -> Value {
    json!({
        "image": "app:v1",
        "mounts": [{"type": "bind", "host_path": source, "target": "/workspace", "options": ["rw"]}]
    })
}

#[tokio::test]
async fn omitted_and_empty_host_path_policies_deny_local_mounts() {
    for allowed in [None, Some(json!([]))] {
        let service = service(allowed);
        let error = service
            .prepare_create(request("/etc"), &caller())
            .unwrap_err();
        assert_eq!(error.code(), Code::PermissionDenied);
        assert!(service
            .prepare_create(json!({"image": "app:v1"}), &caller())
            .is_ok());
    }
}

#[tokio::test]
async fn wildcard_allows_valid_sources_and_preserves_mount_options() {
    for allowed in [json!(["*"]), json!(["/mnt/workspaces", "*"])] {
        let service = service(Some(allowed));
        for field in ["host_path", "hostPath"] {
            for path in [
                "/",
                "/etc",
                "/opt/adx/config/tls/ca.pem",
                "/srv/other/project",
            ] {
                let mut mount = json!({"type":"bind", "target":"/workspace", "options":["ro"]});
                mount[field] = json!(path);
                let spec = service
                    .prepare_create(json!({"image":"app:v1", "mounts":[mount]}), &caller())
                    .unwrap();
                assert_eq!(spec.execution.unwrap().mounts[0].options, ["ro"]);
            }
        }
    }
}

#[tokio::test]
async fn wildcard_keeps_path_syntax_validation_on_typed_creation() {
    let service = service(Some(json!(["*"])));
    for source in ["relative", "/mnt/../etc", "/mnt/\nuser"] {
        let mut spec = contract::create_spec(request("/mnt/workspaces/alice"), &caller()).unwrap();
        spec.execution.as_mut().unwrap().mounts[0].mount_source =
            Some(pb::mount_spec::MountSource::Source(pb::StorageSource {
                source: Some(pb::storage_source::Source::Local(source.into())),
            }));
        let error = service
            .create(spec, json!({"e2b":true}), "create-test", &caller())
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
    }
}

#[tokio::test]
async fn empty_host_path_allowlist_denies_local_sources_only() {
    let service = service(Some(json!([])));
    let error = service
        .prepare_create(request("/mnt/workspaces/alice"), &caller())
        .unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied);
    assert!(error.message().contains("allowed_host_paths"));
    for source in [
        json!({"image_url": "registry.example/data:v1"}),
        json!({"s3_config": {"endpoint": "https://s3.example", "bucket": "data", "object": "data.erofs"}}),
        json!({"volume_id": "workspace-alice"}),
    ] {
        let mut mount = source;
        mount["type"] = json!("bind");
        mount["target"] = json!("/workspace");
        assert!(service
            .prepare_create(json!({"image": "app:v1", "mounts": [mount]}), &caller())
            .is_ok());
    }
}

#[tokio::test]
async fn allowed_paths_cover_exact_sources_descendants_and_both_aliases() {
    let service = service(Some(json!(["/mnt/workspaces", "/etc/agentos/ca.pem"])));
    for runtime in ["runc", "runsc", "supervisor", "conch"] {
        for field in ["host_path", "hostPath"] {
            for path in [
                "/mnt/workspaces",
                "/mnt/workspaces/alice/project",
                "/etc/agentos/ca.pem",
            ] {
                let mut mount = json!({"type": "bind", "target": "/workspace", "options": ["ro"]});
                mount[field] = json!(path);
                let spec = service
                    .prepare_create(
                        json!({"image": "app:v1", "runtime": runtime, "mounts": [mount]}),
                        &caller(),
                    )
                    .unwrap();
                assert_eq!(spec.execution.unwrap().mounts[0].options, ["ro"]);
            }
        }
    }
}

#[tokio::test]
async fn denied_sources_cannot_use_prefix_collisions_or_mount_targets() {
    let service = service(Some(json!(["/mnt/workspaces", "/etc/agentos/ca.pem"])));
    for administrator in [false, true] {
        let caller = pb::CallerContext {
            administrator,
            ..caller()
        };
        for path in [
            "/",
            "/etc",
            "/mnt",
            "/mnt/workspaces-backup/alice",
            "/etc/agentos/ca.pem.backup",
        ] {
            let mut input = request(path);
            input["mounts"][0]["target"] = json!("/mnt/workspaces");
            let error = service.prepare_create(input, &caller).unwrap_err();
            assert_eq!(error.code(), Code::PermissionDenied, "{path}");
        }
    }
}

#[tokio::test]
async fn directory_initialization_keeps_its_options_and_requires_authorization() {
    let service = service(Some(json!(["/mnt/workspaces"])));
    for (source, allowed) in [("/mnt/workspaces/alice", true), ("/etc/alice", false)] {
        let mut input = request(source);
        input["mounts"][0]["create_if_missing"] = json!(true);
        input["mounts"][0]["create_options"] = json!({"uid": 1000, "gid": 1000, "mode": "0700"});
        let result = service.prepare_create(input, &caller());
        if allowed {
            let mount = result.unwrap().execution.unwrap().mounts.remove(0);
            assert!(mount.create_if_missing);
            assert_eq!(mount.options, ["rw"]);
            assert_eq!(mount.create_options.unwrap().mode, "0700");
        } else {
            assert_eq!(result.unwrap_err().code(), Code::PermissionDenied);
        }
    }
}

#[tokio::test]
async fn typed_create_cannot_bypass_host_path_admission() {
    let service = service(Some(json!(["/mnt/workspaces"])));
    for (source, code) in [
        ("/etc", Code::PermissionDenied),
        ("/mnt/workspaces/../etc", Code::InvalidArgument),
    ] {
        // Snapshot/template creation can supply an already-built specification.
        let mut spec = contract::create_spec(request("/mnt/workspaces/alice"), &caller()).unwrap();
        let mount = &mut spec.execution.as_mut().unwrap().mounts[0];
        mount.mount_source = Some(pb::mount_spec::MountSource::Source(pb::StorageSource {
            source: Some(pb::storage_source::Source::Local(source.into())),
        }));
        let error = service
            .create(spec, json!({"e2b": true}), "create-test", &caller())
            .await
            .unwrap_err();
        assert_eq!(error.code(), code);
    }
    let metrics = service.metrics();
    for reason in ["permission_denied", "invalid_argument"] {
        assert!(metrics.contains(&format!(
            "adx_environment_create_rejections_total{{reason=\"{reason}\"}} 1\n"
        )));
    }
}
