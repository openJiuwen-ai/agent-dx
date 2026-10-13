use afs::config::{Cli, Config, MetaStoreBackend, Role};
use clap::Parser;

#[test]
fn shipped_examples_resolve_with_mutual_tls_and_node_identity_binding() {
    let examples = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples");
    let meta_path = examples.join("meta.toml");
    let node_path = examples.join("node.toml");
    let meta = Config::resolve(
        Role::Meta,
        Cli::parse_from(["afs-meta", "--config", meta_path.to_str().unwrap()]),
    )
    .unwrap();
    let node = Config::resolve(
        Role::Node,
        Cli::parse_from(["afs-node", "--config", node_path.to_str().unwrap()]),
    )
    .unwrap();

    assert!(meta.ownerfs && meta.dfs);
    assert!(node.ownerfs && node.dfs);
    assert_eq!(meta.trusted_node_certs.len(), 1);
    assert_eq!(node.trusted_node_certs.len(), 1);
    assert!(
        node.meta_endpoint
            .as_deref()
            .unwrap()
            .starts_with("https://")
    );
    assert!(
        node.peer_endpoint
            .as_deref()
            .unwrap()
            .starts_with("https://")
    );
    for tls in [meta.tls_config(), node.tls_config()] {
        assert!(matches!(tls, afs_transport::TlsConfig::MutualTls { .. }));
    }
}

#[test]
fn file_values_are_overridden_only_by_explicit_cli() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.toml");
    std::fs::write(&path, "id='file-node'\ngrpc_listen='127.0.0.1:9901'\n").unwrap();
    let cli = Cli::parse_from([
        "afs-node",
        "--config",
        path.to_str().unwrap(),
        "--id",
        "cli-node",
    ]);
    let cfg = Config::resolve(Role::Node, cli).unwrap();
    assert_eq!(cfg.id, "cli-node");
    assert_eq!(cfg.grpc_listen.to_string(), "127.0.0.1:9901");
}
#[test]
fn unknown_field_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.toml");
    std::fs::write(&path, "fs_mdoe='all'").unwrap();
    let cli = Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()]);
    assert!(
        Config::resolve(Role::Node, cli)
            .unwrap_err()
            .to_string()
            .contains("unknown field")
    );
}
#[test]
fn invalid_limits_and_uncompiled_mode_fail() {
    let cli = Cli::parse_from(["afs-node", "--timeout-ms", "0"]);
    assert!(Config::resolve(Role::Node, cli).is_err());
    #[cfg(not(feature = "ownerfs"))]
    {
        let cli = Cli::parse_from(["afs-node", "--fs", "ownerfs"]);
        assert!(Config::resolve(Role::Node, cli).is_err());
    }
    #[cfg(not(feature = "dfs"))]
    {
        let cli = Cli::parse_from(["afs-node", "--fs", "dfs"]);
        assert!(Config::resolve(Role::Node, cli).is_err());
    }
}

#[test]
fn meta_store_defaults_to_etcd_and_cli_overrides_toml() {
    let default = Config::resolve(Role::Meta, Cli::parse_from(["afs-meta"])).unwrap();
    assert_eq!(default.meta_store, MetaStoreBackend::Etcd);

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("meta.toml");
    std::fs::write(&path, "meta_store = 'memory'\n").unwrap();
    let file = Config::resolve(
        Role::Meta,
        Cli::parse_from(["afs-meta", "--config", path.to_str().unwrap()]),
    )
    .unwrap();
    assert_eq!(file.meta_store, MetaStoreBackend::InMemory);

    let cli = Config::resolve(
        Role::Meta,
        Cli::parse_from([
            "afs-meta",
            "--config",
            path.to_str().unwrap(),
            "--meta-store",
            "etcd",
        ]),
    )
    .unwrap();
    assert_eq!(cli.meta_store, MetaStoreBackend::Etcd);

    let local = Config::resolve(
        Role::Meta,
        Cli::parse_from(["afs-meta", "--meta-store", "local-file"]),
    )
    .unwrap();
    assert_eq!(local.meta_store, MetaStoreBackend::LocalFile);

    let redis = Config::resolve(
        Role::Meta,
        Cli::parse_from([
            "afs-meta",
            "--meta-store",
            "redis",
            "--redis-endpoint",
            "redis://127.0.0.1:6379/0",
        ]),
    )
    .unwrap();
    assert_eq!(redis.meta_store, MetaStoreBackend::Redis);
    assert_eq!(
        redis.redis_endpoint.as_deref(),
        Some("redis://127.0.0.1:6379/0")
    );
}

#[test]
fn volatile_meta_policy_defaults_to_false_and_cli_overrides_toml() {
    let default = Config::resolve(Role::Node, Cli::parse_from(["afs-node"])).unwrap();
    assert!(!default.allow_volatile_meta);

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.toml");
    std::fs::write(&path, "allow_volatile_meta = true\n").unwrap();
    let file = Config::resolve(
        Role::Node,
        Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()]),
    )
    .unwrap();
    assert!(file.allow_volatile_meta);

    let cli = Config::resolve(
        Role::Node,
        Cli::parse_from([
            "afs-node",
            "--config",
            path.to_str().unwrap(),
            "--allow-volatile-meta",
            "false",
        ]),
    )
    .unwrap();
    assert!(!cli.allow_volatile_meta);
}

#[test]
fn native_workspace_is_default_off_and_refuses_incomplete_or_wrong_backend() {
    let cfg = Config::resolve(Role::Node, Cli::parse_from(["afs-node"])).unwrap();
    assert!(!cfg.experimental_native_workspace);
    assert!(cfg.native_workspace.is_none());
    for (role, argv) in [
        (
            Role::Node,
            vec!["afs-node", "--experimental-native-workspace", "true"],
        ),
        (
            Role::Meta,
            vec!["afs-meta", "--experimental-native-workspace", "true"],
        ),
    ] {
        assert!(Config::resolve(role, Cli::parse_from(argv)).is_err());
    }
}

#[cfg(feature = "ownerfs")]
#[test]
fn native_workspace_validates_admin_paths_and_explicit_off_override() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.toml");
    let base = "fs='ownerfs'\nownerfs_mount='/tmp/owner'\nexperimental_native_workspace=true\n[native_workspace]\ncontrol_dir='/tmp/native-control'\nruntime='/usr/bin/runc'\nrootfs='/tmp/native-rootfs'\nidle_command=['/bin/view-observer','idle']\nidentity_command=['/bin/view-observer','identity']\nworkload_uid=501\nworkload_gid=501\n";
    std::fs::write(&path, base).unwrap();
    let cfg = Config::resolve(
        Role::Node,
        Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()]),
    )
    .unwrap();
    assert!(cfg.experimental_native_workspace);
    for invalid in [
        base.replace("'/usr/bin/runc'", "'relative/runc'"),
        base.replace("'/tmp/native-rootfs'", "'/tmp/native-control/rootfs'"),
        base.replace("'/tmp/native-rootfs'", "'/tmp/../rootfs'"),
        base.replace("workload_uid=501", "workload_uid=0"),
        base.replace("'/tmp/native-control'", "'/tmp/owner/control'"),
        base.replace("'/tmp/native-rootfs'", "'/tmp/owner/rootfs'"),
        base.replace("workload_gid=501", "workload_gid=0"),
        base.replace("fs='ownerfs'", "fs='dfs'"),
        base.replace("idle_command=['/bin/view-observer','idle']\n", ""),
        base.replace("identity_command=['/bin/view-observer','identity']\n", ""),
        base.replace(
            "idle_command=['/bin/view-observer','idle']",
            "idle_command=[]",
        ),
        base.replace(
            "idle_command=['/bin/view-observer','idle']",
            "idle_command=['relative','idle']",
        ),
        base.replace(
            "idle_command=['/bin/view-observer','idle']",
            "idle_command=['/bin/../observer','idle']",
        ),
        base.replace(
            "idle_command=['/bin/view-observer','idle']",
            "idle_command=['/bin/view\u{0}observer','idle']",
        ),
        base.replace(
            "idle_command=['/bin/view-observer','idle']",
            &format!("idle_command=['/bin/view-observer','{}']", "x".repeat(4097)),
        ),
    ] {
        std::fs::write(&path, invalid).unwrap();
        assert!(
            Config::resolve(
                Role::Node,
                Cli::parse_from(["afs-node", "--config", path.to_str().unwrap(),])
            )
            .is_err()
        );
    }
    std::fs::write(&path, base).unwrap();
    let cfg = Config::resolve(
        Role::Node,
        Cli::parse_from([
            "afs-node",
            "--config",
            path.to_str().unwrap(),
            "--experimental-native-workspace",
            "false",
        ]),
    )
    .unwrap();
    assert!(!cfg.experimental_native_workspace);
    assert!(cfg.native_workspace.is_some());
}

#[test]
fn ownerfs_workspace_bind_is_default_off_and_refuses_incomplete_or_wrong_backend() {
    let cfg = Config::resolve(Role::Node, Cli::parse_from(["afs-node"])).unwrap();
    assert!(!cfg.experimental_ownerfs_workspace_bind);
    assert!(cfg.ownerfs_workspace_bind.is_none());

    for (role, argv) in [
        (
            Role::Node,
            vec![
                "afs-node",
                "--fs",
                "ownerfs",
                "--ownerfs-mount",
                "/tmp/owner",
                "--experimental-ownerfs-workspace-bind",
                "true",
            ],
        ),
        (
            Role::Meta,
            vec![
                "afs-meta",
                "--fs",
                "ownerfs",
                "--ownerfs-mount",
                "/tmp/owner",
                "--experimental-ownerfs-workspace-bind",
                "true",
            ],
        ),
    ] {
        assert!(Config::resolve(role, Cli::parse_from(argv)).is_err());
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.toml");
    std::fs::write(
        &path,
        "fs='dfs'\nownerfs_mount='/tmp/owner'\nexperimental_ownerfs_workspace_bind=true\n[ownerfs_workspace_bind]\nworkspace='ws'\n",
    )
    .unwrap();
    assert!(
        Config::resolve(
            Role::Node,
            Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()])
        )
        .is_err()
    );
}

#[cfg(feature = "ownerfs")]
#[test]
fn ownerfs_workspace_bind_validates_workspace_name_and_explicit_off_override() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.toml");
    let base = "fs='ownerfs'\nownerfs_mount='/tmp/owner'\nexperimental_ownerfs_workspace_bind=true\n[ownerfs_workspace_bind]\nworkspace='ws'\n";
    std::fs::write(&path, base).unwrap();
    let cfg = Config::resolve(
        Role::Node,
        Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()]),
    )
    .unwrap();
    assert!(cfg.experimental_ownerfs_workspace_bind);
    assert_eq!(
        cfg.ownerfs_workspace_bind
            .as_ref()
            .map(|bind| bind.workspace.as_str()),
        Some("ws")
    );

    for invalid in ["", ".", "..", "nested/ws", "ws/leaf", "ws\u{0}leaf"] {
        std::fs::write(
            &path,
            format!(
                "fs='ownerfs'\nownerfs_mount='/tmp/owner'\nexperimental_ownerfs_workspace_bind=true\n[ownerfs_workspace_bind]\nworkspace={invalid:?}\n"
            ),
        )
        .unwrap();
        assert!(
            Config::resolve(
                Role::Node,
                Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()])
            )
            .is_err(),
            "workspace {invalid:?} should be rejected"
        );
    }

    std::fs::write(&path, base).unwrap();
    let cfg = Config::resolve(
        Role::Node,
        Cli::parse_from([
            "afs-node",
            "--config",
            path.to_str().unwrap(),
            "--experimental-ownerfs-workspace-bind",
            "false",
        ]),
    )
    .unwrap();
    assert!(!cfg.experimental_ownerfs_workspace_bind);
    assert!(cfg.ownerfs_workspace_bind.is_some());
}

#[cfg(feature = "ownerfs")]
#[test]
fn ownerfs_workspace_bind_rejects_simultaneous_native_workspace_mode() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.toml");
    std::fs::write(
        &path,
        "fs='ownerfs'\nownerfs_mount='/tmp/owner'\nexperimental_native_workspace=true\nexperimental_ownerfs_workspace_bind=true\n[native_workspace]\ncontrol_dir='/tmp/native-control'\nruntime='/usr/bin/runc'\nrootfs='/tmp/native-rootfs'\nidle_command=['/bin/view-observer','idle']\nidentity_command=['/bin/view-observer','identity']\nworkload_uid=501\nworkload_gid=501\n[ownerfs_workspace_bind]\nworkspace='ws'\n",
    )
    .unwrap();
    assert!(
        Config::resolve(
            Role::Node,
            Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()])
        )
        .is_err()
    );
}

#[cfg(feature = "dfs")]
fn external_config(extra: &str) -> afs_error::Result<Config> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s3.toml");
    std::fs::write(&path, format!("fs='dfs'\n{extra}\n[dfs_external_backend]\nstore_id='test-fs'\ngeneration='generation-1'\nendpoint='http://127.0.0.1:8333'\nbucket='afs-test'\nregion='us-east-1'\nallow_http=true\n")).unwrap();
    Config::resolve(
        Role::Node,
        Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()]),
    )
}

#[test]
#[cfg(feature = "dfs")]
fn external_backend_is_explicit_and_fixed_single_copy() {
    let config = external_config("").unwrap();
    let external = config.dfs_external_backend.as_ref().unwrap();
    assert_eq!(external.store_id, "test-fs");
    assert_eq!(config.dfs_desired_copies, 1);
    for counts in [
        "dfs_desired_copies=3",
        "dfs_local_copy='none'",
        "dfs_desired_copies=2\ndfs_sync_required_copies=2",
    ] {
        assert!(external_config(counts).is_err(), "must reject {counts}");
    }
    let ordinary =
        Config::resolve(Role::Node, Cli::parse_from(["afs-node", "--fs", "dfs"])).unwrap();
    assert!(ordinary.dfs_external_backend.is_none());
}

#[test]
#[cfg(feature = "dfs")]
fn external_backend_validates_identity_transport_and_credentials_without_exposing_secrets() {
    let mut config = external_config("").unwrap();
    let external = config.dfs_external_backend.as_mut().unwrap();
    external.generation = "../escape".into();
    assert!(external.validate().is_err());
    external.generation = "generation-1".into();
    external.allow_http = false;
    assert!(external.validate().is_err());
    external.allow_http = true;
    external.credentials_file = Some("relative.json".into());
    assert!(external.validate().is_err());
    assert!(
        !serde_json::to_string(&config)
            .unwrap()
            .contains("secret_access_key")
    );
}

#[test]
#[cfg(feature = "dfs")]
fn external_backend_cache_is_optional_and_rejects_unbounded_or_relative_settings() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache.toml");
    let base = "fs='dfs'\n[dfs_external_backend]\nstore_id='test-fs'\ngeneration='1'\nendpoint='http://127.0.0.1:8333'\nbucket='afs-test'\nregion='us-east-1'\nallow_http=true\n";
    let cache = "[dfs_external_backend.cache]\ndirectory='/var/tmp/afs-content-cache'\ncapacity_bytes=67108864\npending_bytes=8388608\n";
    std::fs::write(&path, format!("{base}{cache}")).unwrap();
    let resolve = || {
        Config::resolve(
            Role::Node,
            Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()]),
        )
    };
    assert!(
        resolve().is_ok(),
        "explicit bounded cache ON must be accepted"
    );
    for bad in [
        cache.replace("/var/tmp/afs-content-cache", "relative/cache"),
        cache.replace("capacity_bytes=67108864", "capacity_bytes=0"),
        cache.replace("pending_bytes=8388608", "pending_bytes=0"),
        cache.replace("pending_bytes=8388608", "pending_bytes=134217728"),
    ] {
        std::fs::write(&path, format!("{base}{bad}")).unwrap();
        assert!(
            resolve().is_err(),
            "invalid cache budget/path must be rejected"
        );
    }
    std::fs::write(&path, base).unwrap();
    assert!(
        resolve().is_ok(),
        "omitting cache preserves OFF configuration"
    );
}
