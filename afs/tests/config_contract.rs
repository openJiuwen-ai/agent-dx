use afs::config::{Cli, Config, MetaStoreBackend, Role};
use clap::Parser;
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
