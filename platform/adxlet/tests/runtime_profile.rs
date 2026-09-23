use adx_core::{
    runtime_profile::RuntimeProfile,
    sandbox::{Rootfs, StorageSource},
    EnvironmentSpec,
};
use adxlet::sandboxd::{proto, start_request, Config};
use serde_json::json;

fn runtime_profile() -> RuntimeProfile {
    serde_json::from_value(json!({
        "rootfs":{"runtime_class":"runsc","type":"local","path":"/opt/adx/runtime/rootfs.img","readonly":false},
        "bootstrap":{"type":"erofs","root":"/opt/adx/runtime/rootfs.img","target":"/__adx",
            "entrypoint":["/__adx/usr/local/bin/adx-execd"],
            "image_process_config":"/etc/adx/custom-image-process.json"},
        "env":{"PLATFORM_VALUE":"configured"}
    })).unwrap()
}
fn image_environment() -> RuntimeProfile {
    serde_json::from_value(json!({
        "rootfs":{"runtime_class":"runc","type":"image","image":"registry.local/adx-runtime@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","readonly":false},
        "bootstrap":{"type":"image","image":"registry.local/adx-runtime@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","target":"/__adx",
            "entrypoint":["/__adx/usr/local/bin/adx-execd"],
            "image_process_config":"/etc/adx/custom-image-process.json"},
        "env":{"PLATFORM_VALUE":"configured"}
    })).unwrap()
}
fn spec(image: &str) -> EnvironmentSpec {
    serde_json::from_value(
        json!({"id":"i","tenant_id":"t","image":image,"runtime_class":"runc",
        "resources":{"cpu_millis":100,"memory_bytes":1048576,"disk_bytes":0},"priority":0,
        "runtime_profile":runtime_profile()}),
    )
    .unwrap()
}

#[test]
fn default_rootfs_uses_local_artifact_without_bootstrap_mount() {
    let config = Config {
        runtime_profile: Some(runtime_profile()),
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
    assert_eq!(r.command, runtime_profile().bootstrap.entrypoint);
}

#[test]
fn runtime_and_readonly_overlay_reuses_the_deployment_root_without_bootstrap_mount() {
    let config = Config {
        runtime_profile: Some(runtime_profile()),
        ..Default::default()
    };
    let mut wanted = spec("");
    wanted.runtime_class = "firecracker".into();
    let rootfs = &mut wanted.runtime_profile.as_mut().unwrap().rootfs;
    rootfs.runtime_class = "firecracker".into();
    rootfs.readonly = true;

    let request = start_request(&wanted, "i-1", 1, &[], &config).unwrap();
    assert_eq!(request.runtime, "firecracker");
    let rootfs = request.rootfs.unwrap();
    assert!(rootfs.readonly);
    assert_eq!(
        rootfs.source,
        Some(proto::rootfs_config::Source::Path(
            "/opt/adx/runtime/rootfs.img".into()
        ))
    );
    assert!(request.mounts.is_empty());
}

#[test]
fn custom_image_mounts_the_same_local_environment_read_only() {
    let config = Config {
        runtime_profile: Some(runtime_profile()),
        ..Default::default()
    };
    let mut wanted = spec("ubuntu:24.04");
    wanted.image.clear();
    wanted.sandbox.rootfs = Some(Rootfs {
        readonly: false,
        source: StorageSource::Image("ubuntu:24.04".into()),
    });
    wanted.sandbox.inherit_entrypoint = true;
    let r = start_request(&wanted, "i-1", 1, &[], &config).unwrap();
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
        Some(proto::mount::Source::HostPath(
            runtime_profile().bootstrap.root
        ))
    );
    assert_eq!(r.envs["PLATFORM_VALUE"], "configured");
    assert_eq!(
        r.envs["ADX_IMAGE_PROCESS_CONFIG"],
        "/etc/adx/custom-image-process.json"
    );
    assert_eq!(r.inject_entrypoint, "/etc/adx/custom-image-process.json");
}

#[test]
fn default_rootfs_uses_the_configured_oci_runtime_image() {
    let config = Config {
        runtime_profile: Some(image_environment()),
        ..Default::default()
    };
    let mut wanted = spec("");
    wanted.runtime_profile = Some(image_environment());
    let r = start_request(&wanted, "i-1", 1, &[], &config).unwrap();
    let root = r.rootfs.unwrap();
    assert_eq!(root.r#type, proto::RootfsSrcType::Image as i32);
    assert_eq!(
        root.source,
        Some(proto::rootfs_config::Source::ImageUrl(
            image_environment().rootfs.image
        ))
    );
    assert!(r.mounts.is_empty());
    assert_eq!(r.command, image_environment().bootstrap.entrypoint);
}

#[test]
fn custom_rootfs_mounts_the_oci_runtime_image_read_only() {
    let config = Config {
        runtime_profile: Some(image_environment()),
        ..Default::default()
    };
    let mut wanted = spec("ubuntu:24.04");
    wanted.runtime_profile = Some(image_environment());
    let r = start_request(&wanted, "i-1", 1, &[], &config).unwrap();
    assert_eq!(r.mounts.len(), 1);
    let mount = &r.mounts[0];
    assert_eq!(mount.r#type, "bind");
    assert_eq!(mount.target, "/__adx");
    assert_eq!(mount.options, ["ro", "rbind"]);
    assert_eq!(
        mount.source,
        Some(proto::mount::Source::ImageUrl(
            image_environment().bootstrap.image
        ))
    );
}

#[test]
fn s3_rootfs_mounts_the_configured_environment() {
    use adx_core::sandbox::{Rootfs, S3Source, StorageSource};

    let config = Config {
        runtime_profile: Some(runtime_profile()),
        ..Default::default()
    };
    let mut wanted = spec("");
    wanted.sandbox.rootfs = Some(Rootfs {
        readonly: false,
        source: StorageSource::S3(S3Source {
            endpoint: "https://s3.example".into(),
            bucket: "rootfs".into(),
            object: "application.erofs".into(),
            access_key_id: "key".into(),
            access_key_secret: "secret".into(),
        }),
    });
    let request = start_request(&wanted, "i-1", 1, &[], &config).unwrap();
    assert!(matches!(
        request.rootfs.unwrap().source,
        Some(proto::rootfs_config::Source::S3Config(_))
    ));
    assert_eq!(request.mounts.len(), 1);
    assert_eq!(request.mounts[0].target, "/__adx");
    assert_eq!(request.command, runtime_profile().bootstrap.entrypoint);
}

#[test]
fn old_or_unconfigured_environment_cannot_mount_arbitrary_host_files() {
    let mut wanted = spec("custom");
    let config = Config {
        runtime_profile: Some(runtime_profile()),
        ..Default::default()
    };
    wanted.runtime_profile.as_mut().unwrap().bootstrap.root = "/etc/other.img".into();
    assert!(start_request(&wanted, "i-1", 1, &[], &config).is_err());
    let mut source_drift = spec("");
    source_drift.runtime_profile.as_mut().unwrap().rootfs.path = "/etc/other.img".into();
    assert!(start_request(&source_drift, "i-1", 1, &[], &config).is_err());
    assert!(start_request(&spec("custom"), "i-1", 1, &[], &Config::default()).is_err());
}
