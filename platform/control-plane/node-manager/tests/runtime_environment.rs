use adx_core::{environment::RuntimeEnvironment, InstanceSpec};
use adx_node_manager::sandboxd::{proto, start_request, Config};
use serde_json::json;

fn environment() -> RuntimeEnvironment {
    serde_json::from_value(json!({
        "rootfs":{"runtime":"runsc","type":"local","path":"/opt/adx/runtime/rootfs.img","readonly":false},
        "bootstrap":{"type":"erofs","root":"/opt/adx/runtime/rootfs.img","target":"/__adx",
            "entrypoint":["/__adx/usr/local/bin/rrt-runtime"]},
        "env":{"PLATFORM_VALUE":"configured"}
    })).unwrap()
}
fn spec(image: &str) -> InstanceSpec {
    serde_json::from_value(
        json!({"id":"i","tenant_id":"t","image":image,"runtime":"runc",
        "resources":{"cpu_millis":100,"memory_bytes":1048576,"disk_bytes":0},"priority":0,
        "runtime_environment":environment()}),
    )
    .unwrap()
}

#[test]
fn default_rootfs_uses_local_artifact_without_bootstrap_mount() {
    let config = Config {
        runtime_environment: Some(environment()),
        ..Default::default()
    };
    let r = start_request(&spec(""), "i-1", 1, &[], &config).unwrap();
    assert_eq!(r.runtime, "runc"); // runtime-only override retains the default source
    let root = r.rootfs.unwrap();
    assert_eq!(root.r#type, proto::RootfsSrcType::Local as i32);
    assert_eq!(
        root.source,
        Some(proto::rootfs_config::Source::Path(
            "/opt/adx/runtime/rootfs.img".into()
        ))
    );
    assert!(!root.readonly);
    assert!(r.mounts.is_empty());
    assert_eq!(r.command, environment().bootstrap.entrypoint);
}

#[test]
fn custom_image_mounts_the_same_local_environment_read_only() {
    let config = Config {
        runtime_environment: Some(environment()),
        ..Default::default()
    };
    let r = start_request(&spec("ubuntu:24.04"), "i-1", 1, &[], &config).unwrap();
    assert_eq!(
        r.rootfs.unwrap().source,
        Some(proto::rootfs_config::Source::ImageUrl(
            "ubuntu:24.04".into()
        ))
    );
    assert_eq!(r.mounts.len(), 1);
    let mount = &r.mounts[0];
    assert_eq!(mount.r#type, "erofs");
    assert_eq!(mount.target, "/__adx");
    assert_eq!(mount.options, ["ro"]);
    assert_eq!(
        mount.source,
        Some(proto::mount::Source::HostPath(environment().bootstrap.root))
    );
    assert_eq!(r.envs["PLATFORM_VALUE"], "configured");
}

#[test]
fn old_or_unconfigured_environment_cannot_mount_arbitrary_host_files() {
    let mut wanted = spec("custom");
    let config = Config {
        runtime_environment: Some(environment()),
        ..Default::default()
    };
    wanted.runtime_environment.as_mut().unwrap().bootstrap.root = "/etc/other.img".into();
    assert!(start_request(&wanted, "i-1", 1, &[], &config).is_err());
    assert!(start_request(&spec("custom"), "i-1", 1, &[], &Config::default()).is_err());
}
