use adx_core::runtime_profile::{Bootstrap, Rootfs, RuntimeProfile};

fn runtime_profile(
    rootfs_type: &str,
    path: &str,
    rootfs_image: &str,
    bootstrap_type: &str,
    root: &str,
    bootstrap_image: &str,
) -> RuntimeProfile {
    RuntimeProfile {
        rootfs: Rootfs {
            runtime_class: "runc".into(),
            r#type: rootfs_type.into(),
            path: path.into(),
            image: rootfs_image.into(),
            readonly: false,
        },
        bootstrap: Bootstrap {
            r#type: bootstrap_type.into(),
            root: root.into(),
            image: bootstrap_image.into(),
            target: "/__adx".into(),
            entrypoint: vec!["/__adx/usr/local/bin/adx-execd".into()],
            image_process_config: "/etc/adx-image-process.json".into(),
        },
        env: Default::default(),
    }
}

#[test]
fn accepts_matching_erofs_and_oci_sources() {
    runtime_profile(
        "local",
        "/opt/adx/runtime.img",
        "",
        "erofs",
        "/opt/adx/runtime.img",
        "",
    )
    .validate()
    .unwrap();
    runtime_profile(
        "image",
        "",
        "registry/runtime@sha256:1111111111111111111111111111111111111111111111111111111111111111",
        "image",
        "",
        "registry/runtime@sha256:1111111111111111111111111111111111111111111111111111111111111111",
    )
    .validate()
    .unwrap();
}

#[test]
fn rejects_mixed_or_different_runtime_sources() {
    for value in [
        runtime_profile(
            "image",
            "",
            "registry/runtime@sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "erofs",
            "/opt/adx/runtime.img",
            "",
        ),
        runtime_profile(
            "image",
            "",
            "registry/runtime@sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "image",
            "",
            "registry/runtime@sha256:2222222222222222222222222222222222222222222222222222222222222222",
        ),
    ] {
        assert!(value.validate().is_err());
    }
}

#[test]
fn rejects_relative_image_process_config_path() {
    let mut value = runtime_profile(
        "local",
        "/opt/adx/runtime.img",
        "",
        "erofs",
        "/opt/adx/runtime.img",
        "",
    );
    value.bootstrap.image_process_config = "run/image-process.json".into();
    assert!(value.validate().is_err());
}
