use afs::config::{Cli, Config, MetaStoreBackend, Role};
use clap::Parser;

#[test]
fn p2_w2_native_write_limits_use_cli_file_defaults_and_bounded_resources() {
    fn limits(config: &Config) -> (u64, usize, usize, u64) {
        (
            config.dfs_dirty_budget_bytes,
            config.dfs_unpublished_descriptors,
            config.dfs_upload_inflight_chunks,
            config.dfs_background_commit_ms,
        )
    }
    let defaults = (64 * 1024 * 1024, 1024, 8, 5000);
    assert_eq!(
        limits(&Config::resolve(Role::Node, Cli::parse_from(["afs-node"])).unwrap()),
        defaults
    );
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("native-write.toml");
    for values in [
        (4 * 1024 * 1024, 1, 1, 1),
        defaults,
        (16 * 1024 * 1024, 32, 4, 60000),
    ] {
        std::fs::write(&path, format!(
            "dfs_dirty_budget_bytes={}\ndfs_unpublished_descriptors={}\ndfs_upload_inflight_chunks={}\ndfs_background_commit_ms={}\n",
            values.0, values.1, values.2, values.3,
        )).unwrap();
        let file = Config::resolve(
            Role::Node,
            Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()]),
        )
        .unwrap();
        assert_eq!(limits(&file), values);
        let cli = Config::resolve(
            Role::Node,
            Cli::parse_from([
                "afs-node",
                "--config",
                path.to_str().unwrap(),
                "--dfs-dirty-budget-bytes",
                "67108864",
                "--dfs-unpublished-descriptors",
                "1024",
                "--dfs-upload-inflight-chunks",
                "8",
                "--dfs-background-commit-ms",
                "5000",
            ]),
        )
        .unwrap();
        assert_eq!(limits(&cli), defaults);
    }
}

#[test]
fn p2_w2_complete_node_examples_resolve_top_level_write_limits() {
    for (name, contents) in [
        ("component", include_str!("../examples/node.toml")),
        (
            "deployment",
            include_str!("../../build/config/examples/afs/node.toml"),
        ),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("node.toml");
        std::fs::write(&path, contents).unwrap();
        let config = Config::resolve(
            Role::Node,
            Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()]),
        )
        .unwrap_or_else(|error| panic!("{name} complete example does not resolve: {error}"));
        assert_eq!(config.dfs_dirty_budget_bytes, 64 * 1024 * 1024, "{name}");
        assert_eq!(config.dfs_unpublished_descriptors, 1024, "{name}");
        assert_eq!(config.dfs_upload_inflight_chunks, 8, "{name}");
        assert_eq!(config.dfs_background_commit_ms, 5000, "{name}");
        assert_eq!(config.dfs_catalog_commit_queue, 256, "{name}");
        assert_eq!(config.trusted_node_certs.len(), 1, "{name}");
        assert!(config.trusted_node_certs.contains_key("node-a"), "{name}");
    }
}

#[test]
fn p2_w2_invalid_native_limits_are_not_silently_replaced() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("native-write.toml");
    for (key, flag, value) in [
        ("dfs_dirty_budget_bytes", "--dfs-dirty-budget-bytes", "0"),
        (
            "dfs_dirty_budget_bytes",
            "--dfs-dirty-budget-bytes",
            "4194303",
        ),
        (
            "dfs_dirty_budget_bytes",
            "--dfs-dirty-budget-bytes",
            "67108865",
        ),
        (
            "dfs_unpublished_descriptors",
            "--dfs-unpublished-descriptors",
            "0",
        ),
        (
            "dfs_unpublished_descriptors",
            "--dfs-unpublished-descriptors",
            "1025",
        ),
        (
            "dfs_upload_inflight_chunks",
            "--dfs-upload-inflight-chunks",
            "0",
        ),
        (
            "dfs_upload_inflight_chunks",
            "--dfs-upload-inflight-chunks",
            "9",
        ),
        (
            "dfs_background_commit_ms",
            "--dfs-background-commit-ms",
            "0",
        ),
        (
            "dfs_background_commit_ms",
            "--dfs-background-commit-ms",
            "60001",
        ),
    ] {
        std::fs::write(&path, format!("{key}={value}\n")).unwrap();
        let error = Config::resolve(
            Role::Node,
            Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()]),
        )
        .unwrap_err();
        assert_eq!(error.code(), afs_error::CONFIG_INVALID, "{key}={value}");
        let error =
            Config::resolve(Role::Node, Cli::parse_from(["afs-node", flag, value])).unwrap_err();
        assert_eq!(error.code(), afs_error::CONFIG_INVALID, "{flag} {value}");
        assert!(
            Config::resolve(
                Role::Node,
                Cli::parse_from([
                    "afs-node",
                    "--config",
                    path.to_str().unwrap(),
                    "--dfs-dirty-budget-bytes",
                    "67108864",
                    "--dfs-unpublished-descriptors",
                    "1024",
                    "--dfs-upload-inflight-chunks",
                    "8",
                    "--dfs-background-commit-ms",
                    "5000",
                ])
            )
            .is_ok(),
            "CLI must override the invalid file field: {key}"
        );
    }
}

#[test]
fn p2_r7_readahead_is_opt_in_and_only_a_bounded_distance() {
    assert_eq!(
        Config::resolve(Role::Node, Cli::parse_from(["afs-node"]))
            .unwrap()
            .dfs_readahead_max_bytes,
        0
    );
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("node.toml");
    for distance in [0, 8 * 1024 * 1024, 32 * 1024 * 1024] {
        std::fs::write(&path, format!("dfs_readahead_max_bytes={distance}\n")).unwrap();
        let file = Config::resolve(
            Role::Node,
            Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()]),
        )
        .unwrap();
        assert_eq!(file.dfs_readahead_max_bytes, distance);
        let cli = Config::resolve(
            Role::Node,
            Cli::parse_from([
                "afs-node",
                "--config",
                path.to_str().unwrap(),
                "--dfs-readahead-max-bytes",
                "0",
            ]),
        )
        .unwrap();
        assert_eq!(cli.dfs_readahead_max_bytes, 0);
    }
    assert!(
        Config::resolve(
            Role::Node,
            Cli::parse_from(["afs-node", "--dfs-readahead-max-bytes", "33554433"])
        )
        .is_err()
    );
}

#[test]
fn p2_r5_flight_limit_cli_is_exposed() {
    assert!(
        Cli::try_parse_from(["afs-node", "--dfs-fetch-max-inflight-objects", "8"]).is_ok(),
        "the bounded native fill-flight limit must be accepted by the public CLI"
    );
}

#[test]
fn p2_r5_flight_limit_is_bounded_and_file_cli_defaults_agree() {
    assert_eq!(
        Config::resolve(Role::Node, Cli::parse_from(["afs-node"]))
            .unwrap()
            .dfs_fetch_max_inflight_objects,
        8
    );
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("node.toml");
    for limit in [1, 4, 8] {
        std::fs::write(&path, format!("dfs_fetch_max_inflight_objects={limit}\n")).unwrap();
        let file = Config::resolve(
            Role::Node,
            Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()]),
        )
        .unwrap();
        assert_eq!(file.dfs_fetch_max_inflight_objects, limit);
        let cli = Config::resolve(
            Role::Node,
            Cli::parse_from([
                "afs-node",
                "--config",
                path.to_str().unwrap(),
                "--dfs-fetch-max-inflight-objects",
                "2",
            ]),
        )
        .unwrap();
        assert_eq!(cli.dfs_fetch_max_inflight_objects, 2);
    }
    for limit in [0, 9] {
        std::fs::write(&path, format!("dfs_fetch_max_inflight_objects={limit}\n")).unwrap();
        let error = Config::resolve(
            Role::Node,
            Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()]),
        )
        .unwrap_err();
        assert!(error.to_string().contains("1..=8"));
        assert!(
            Config::resolve(
                Role::Node,
                Cli::parse_from([
                    "afs-node",
                    "--dfs-fetch-max-inflight-objects",
                    &limit.to_string(),
                ]),
            )
            .is_err()
        );
        let overridden = Config::resolve(
            Role::Node,
            Cli::parse_from([
                "afs-node",
                "--config",
                path.to_str().unwrap(),
                "--dfs-fetch-max-inflight-objects",
                "2",
            ]),
        )
        .unwrap();
        assert_eq!(overridden.dfs_fetch_max_inflight_objects, 2);
    }
}

#[test]
fn p2_w3_catalog_queue_bounds_and_cli_precedence() {
    assert_eq!(
        Config::resolve(Role::Node, Cli::parse_from(["afs-node"]))
            .unwrap()
            .dfs_catalog_commit_queue,
        256
    );
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("node.toml");
    for limit in [1, 16, 256] {
        std::fs::write(&path, format!("dfs_catalog_commit_queue={limit}\n")).unwrap();
        let config = Config::resolve(
            Role::Node,
            Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()]),
        )
        .unwrap();
        assert_eq!(config.dfs_catalog_commit_queue, limit);
        let config = Config::resolve(
            Role::Node,
            Cli::parse_from([
                "afs-node",
                "--config",
                path.to_str().unwrap(),
                "--dfs-catalog-commit-queue",
                "2",
            ]),
        )
        .unwrap();
        assert_eq!(config.dfs_catalog_commit_queue, 2);
    }
    for limit in [0, 257] {
        std::fs::write(&path, format!("dfs_catalog_commit_queue={limit}\n")).unwrap();
        assert!(
            Config::resolve(
                Role::Node,
                Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()])
            )
            .is_err()
        );
        assert!(
            Config::resolve(
                Role::Node,
                Cli::parse_from(["afs-node", "--dfs-catalog-commit-queue", &limit.to_string()])
            )
            .is_err()
        );
    }
}

#[test]
fn p2_w0_sidecar_queue_is_bounded_and_file_cli_defaults_agree() {
    assert_eq!(
        Config::resolve(Role::Node, Cli::parse_from(["afs-node"]))
            .unwrap()
            .dfs_sidecar_install_queue,
        64
    );
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("node.toml");
    for limit in [1, 32, 64] {
        std::fs::write(&path, format!("dfs_sidecar_install_queue={limit}\n")).unwrap();
        let file = Config::resolve(
            Role::Node,
            Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()]),
        )
        .unwrap();
        assert_eq!(file.dfs_sidecar_install_queue, limit);
        let cli = Config::resolve(
            Role::Node,
            Cli::parse_from([
                "afs-node",
                "--config",
                path.to_str().unwrap(),
                "--dfs-sidecar-install-queue",
                "2",
            ]),
        )
        .unwrap();
        assert_eq!(cli.dfs_sidecar_install_queue, 2);
    }
    for limit in [0, 65] {
        std::fs::write(&path, format!("dfs_sidecar_install_queue={limit}\n")).unwrap();
        let error = Config::resolve(
            Role::Node,
            Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()]),
        )
        .unwrap_err();
        assert!(error.to_string().contains("1..=64"));
        assert!(
            Config::resolve(
                Role::Node,
                Cli::parse_from([
                    "afs-node",
                    "--dfs-sidecar-install-queue",
                    &limit.to_string()
                ])
            )
            .is_err()
        );
    }
}

#[test]
fn p2_c1a_outboard_wait_defaults_and_file_cli_boundaries() {
    let defaults = Config::resolve(Role::Node, Cli::parse_from(["afs-node"])).unwrap();
    assert_eq!(defaults.dfs_outboard_work_wait_ms, 2);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.toml");
    for wait in [0, 2, 50] {
        std::fs::write(&path, format!("dfs_outboard_work_wait_ms={wait}\n")).unwrap();
        let file = Config::resolve(
            Role::Node,
            Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()]),
        )
        .unwrap();
        assert_eq!(file.dfs_outboard_work_wait_ms, wait);
        let cli = Config::resolve(
            Role::Node,
            Cli::parse_from([
                "afs-node",
                "--config",
                path.to_str().unwrap(),
                "--dfs-outboard-work-wait-ms",
                "1",
            ]),
        )
        .unwrap();
        assert_eq!(cli.dfs_outboard_work_wait_ms, 1);
    }
    std::fs::write(&path, "dfs_outboard_work_wait_ms=51\n").unwrap();
    let error = Config::resolve(
        Role::Node,
        Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()]),
    )
    .unwrap_err();
    assert!(error.to_string().contains("0..=50"));
    assert!(
        Config::resolve(
            Role::Node,
            Cli::parse_from(["afs-node", "--dfs-outboard-work-wait-ms", "51",])
        )
        .is_err()
    );
}

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
fn p3_cli_retired_backend_errors_explain_safe_migration() {
    for args in [
        vec!["--meta-store", "etcd"],
        vec!["--meta-store=redis"],
        vec!["--etcd-endpoint", "http://127.0.0.1:2379"],
        vec!["--redis-endpoint=redis://127.0.0.1:6379/0"],
    ] {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_afs-meta"))
            .args(args)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        let text = String::from_utf8(output.stderr).unwrap();
        assert!(
            text.contains("retired")
                && text.contains("empty directory")
                && text.contains("original binary"),
            "missing safe migration guidance: {text}"
        );
    }
    for flag in ["--help", "--version"] {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_afs-meta"))
            .arg(flag)
            .output()
            .unwrap();
        assert!(output.status.success());
        assert!(
            !String::from_utf8(output.stderr)
                .unwrap()
                .contains("retired")
        );
    }
}

#[test]
fn p3_settings_default_is_local_file_without_retired_endpoints() {
    let cfg = Config::resolve(Role::Meta, Cli::parse_from(["afs-meta"])).unwrap();
    assert_eq!(cfg.meta_store, MetaStoreBackend::LocalFile);
    let output = serde_json::to_value(&cfg).unwrap();
    assert!(output.get("etcd_endpoint").is_none());
    assert!(output.get("redis_endpoint").is_none());
}
#[test]
fn p3_settings_retired_backend_selection_and_endpoints_are_rejected() {
    for (flag, value) in [
        ("--meta-store", "etcd"),
        ("--meta-store", "redis"),
        ("--etcd-endpoint", "http://127.0.0.1:2379"),
        ("--redis-endpoint", "redis://127.0.0.1:6379/0"),
    ] {
        if let Ok(cli) = Cli::try_parse_from(["afs-meta", flag, value]) {
            assert!(
                Config::resolve(Role::Meta, cli).is_err(),
                "retired CLI option accepted: {flag}={value}"
            );
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("retired.toml");
    for text in [
        "meta_store='etcd'",
        "meta_store='redis'",
        "etcd_endpoint='http://127.0.0.1:2379'",
        "redis_endpoint='redis://127.0.0.1:6379/0'",
    ] {
        std::fs::write(&path, text).unwrap();
        let result = Config::resolve(
            Role::Meta,
            Cli::parse_from(["afs-meta", "--config", path.to_str().unwrap()]),
        );
        assert!(result.is_err(), "retired TOML accepted: {text}");
    }
}
#[test]
fn p3_settings_finite_policy_defaults_match_the_approved_plan() {
    let cfg = Config::resolve(Role::Meta, Cli::parse_from(["afs-meta"])).unwrap();
    let output = serde_json::to_value(cfg).unwrap();
    for (key, value) in [
        ("meta_checkpoint_wal_bytes", 128 * 1024 * 1024_u64),
        ("meta_checkpoint_wal_records", 1_000_000),
        ("meta_checkpoint_interval_ms", 60_000),
        ("meta_wal_segment_bytes", 64 * 1024 * 1024),
        ("meta_request_retention_ms", 86_400_000),
        ("meta_request_outcomes_max", 1_000_000),
        ("meta_request_bytes_max", 256 * 1024 * 1024),
        ("meta_watch_events_max", 1_000_000),
        ("meta_watch_bytes_max", 64 * 1024 * 1024),
        ("meta_watch_retention_ms", 3_600_000),
    ] {
        assert_eq!(
            output[key],
            serde_json::json!(value),
            "missing or wrong default: {key}"
        );
    }
}
#[test]
fn p3_settings_finite_policy_toml_cli_precedence_and_zero_rejection() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("policy.toml");
    let fields = [
        ("meta_checkpoint_wal_bytes", 1_048_576_u64),
        ("meta_checkpoint_wal_records", 128),
        ("meta_checkpoint_interval_ms", 1000),
        ("meta_wal_segment_bytes", 1_048_576),
        ("meta_request_retention_ms", 5000),
        ("meta_request_outcomes_max", 128),
        ("meta_request_bytes_max", 1_048_576),
        ("meta_watch_events_max", 128),
        ("meta_watch_bytes_max", 1_048_576),
        ("meta_watch_retention_ms", 5000),
    ];
    for (key, value) in fields {
        std::fs::write(&path, format!("{key}={value}\n")).unwrap();
        let cfg = Config::resolve(
            Role::Meta,
            Cli::parse_from(["afs-meta", "--config", path.to_str().unwrap()]),
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(cfg).unwrap()[key],
            serde_json::json!(value)
        );
        let flag = format!("--{}", key.replace('_', "-"));
        let alternate = (value * 2).to_string();
        let cfg = Config::resolve(
            Role::Meta,
            Cli::try_parse_from([
                "afs-meta",
                "--config",
                path.to_str().unwrap(),
                &flag,
                &alternate,
            ])
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(cfg).unwrap()[key],
            serde_json::json!(value * 2)
        );
        std::fs::write(&path, format!("{key}=0\n")).unwrap();
        assert!(
            Config::resolve(
                Role::Meta,
                Cli::parse_from(["afs-meta", "--config", path.to_str().unwrap()])
            )
            .is_err(),
            "zero policy accepted: {key}"
        );
    }
}
#[test]
fn meta_store_defaults_to_local_file_and_cli_overrides_toml() {
    let default = Config::resolve(Role::Meta, Cli::parse_from(["afs-meta"])).unwrap();
    assert_eq!(default.meta_store, MetaStoreBackend::LocalFile);
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
            "local-file",
        ]),
    )
    .unwrap();
    assert_eq!(cli.meta_store, MetaStoreBackend::LocalFile);
}

#[test]
fn p3_log_limits_defaults_cli_override_and_invalid_bounds() {
    fn limits(cfg: &Config) -> (usize, usize, u64) {
        (
            cfg.meta_record_max_bytes,
            cfg.meta_batch_max_bytes,
            cfg.meta_wal_max_bytes,
        )
    }
    let defaults = afs::meta::store::journal::LogLimits::default();
    assert_eq!(
        limits(&Config::resolve(Role::Meta, Cli::parse_from(["afs-meta"])).unwrap()),
        (
            defaults.record_max_bytes,
            defaults.batch_max_bytes,
            defaults.wal_max_bytes
        )
    );
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("meta.toml");
    std::fs::write(&path, "meta_store='local-file'\nmeta_record_max_bytes=1024\nmeta_batch_max_bytes=2048\nmeta_wal_max_bytes=16384\nmeta_wal_segment_bytes=8192\n").unwrap();
    let file = Config::resolve(
        Role::Meta,
        Cli::parse_from(["afs-meta", "--config", path.to_str().unwrap()]),
    )
    .unwrap();
    assert_eq!(limits(&file), (1024, 2048, 16384));
    let cli = Config::resolve(
        Role::Meta,
        Cli::parse_from([
            "afs-meta",
            "--config",
            path.to_str().unwrap(),
            "--meta-record-max-bytes",
            "4096",
            "--meta-batch-max-bytes",
            "8192",
            "--meta-wal-max-bytes",
            "32768",
        ]),
    )
    .unwrap();
    assert_eq!(limits(&cli), (4096, 8192, 32768));
    for (record, batch, wal) in [
        (0u64, 2048u64, 16384u64),
        (255, 2048, 16384),
        (2048, 1024, 16384),
        (1024, 2048, 4096),
        (1024, 2048, 6143),
        (4294967296, 4294967296, 8589934592),
    ] {
        std::fs::write(&path, format!("meta_record_max_bytes={record}\nmeta_batch_max_bytes={batch}\nmeta_wal_max_bytes={wal}\n")).unwrap();
        let error = Config::resolve(
            Role::Meta,
            Cli::parse_from(["afs-meta", "--config", path.to_str().unwrap()]),
        )
        .unwrap_err();
        assert_eq!(error.code(), afs_error::CONFIG_INVALID);
    }
}

#[test]
fn p3_settings_reject_segment_above_wal_and_excessive_timer() {
    for args in [
        vec!["afs-meta", "--meta-wal-segment-bytes", "1073741825"],
        vec![
            "afs-meta",
            "--meta-checkpoint-interval-ms",
            "18446744073709551615",
        ],
    ] {
        assert_eq!(
            Config::resolve(Role::Meta, Cli::parse_from(args))
                .unwrap_err()
                .code(),
            afs_error::CONFIG_INVALID
        );
    }
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

    // Non-Node role should still be rejected.
    assert!(
        Config::resolve(
            Role::Meta,
            Cli::parse_from([
                "afs-meta",
                "--fs",
                "ownerfs",
                "--ownerfs-mount",
                "/tmp/owner",
                "--experimental-ownerfs-workspace-bind",
                "true",
                "--ownerfs-workspace-egress-root",
                "/tmp/egress",
            ])
        )
        .is_err()
    );

    // Node role without [ownerfs_workspace_bind] should now succeed — the section
    // is optional; bindmount is triggered dynamically by mkdir. The egress root
    // is required.
    assert!(
        Config::resolve(
            Role::Node,
            Cli::parse_from([
                "afs-node",
                "--fs",
                "ownerfs",
                "--ownerfs-mount",
                "/tmp/owner",
                "--experimental-ownerfs-workspace-bind",
                "true",
                "--ownerfs-workspace-egress-root",
                "/tmp/egress",
            ])
        )
        .is_ok()
    );

    // Node role with bind enabled but no egress root must fail.
    assert!(
        Config::resolve(
            Role::Node,
            Cli::parse_from([
                "afs-node",
                "--fs",
                "ownerfs",
                "--ownerfs-mount",
                "/tmp/owner",
                "--experimental-ownerfs-workspace-bind",
                "true",
            ])
        )
        .is_err()
    );

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
    let base = "fs='ownerfs'\nownerfs_mount='/tmp/owner'\nownerfs_workspace_egress_root='/tmp/egress'\nexperimental_ownerfs_workspace_bind=true\n[ownerfs_workspace_bind]\nworkspace='ws'\n";
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
    assert_eq!(
        cfg.ownerfs_workspace_egress_root.as_deref(),
        Some(std::path::Path::new("/tmp/egress"))
    );

    for invalid in ["", ".", "..", "nested/ws", "ws/leaf", "ws\u{0}leaf"] {
        std::fs::write(
            &path,
            format!(
                "fs='ownerfs'\nownerfs_mount='/tmp/owner'\nownerfs_workspace_egress_root='/tmp/egress'\nexperimental_ownerfs_workspace_bind=true\n[ownerfs_workspace_bind]\nworkspace={invalid:?}\n"
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
        "fs='ownerfs'\nownerfs_mount='/tmp/owner'\nownerfs_workspace_egress_root='/tmp/egress'\nexperimental_native_workspace=true\nexperimental_ownerfs_workspace_bind=true\n[native_workspace]\ncontrol_dir='/tmp/native-control'\nruntime='/usr/bin/runc'\nrootfs='/tmp/native-rootfs'\nidle_command=['/bin/view-observer','idle']\nidentity_command=['/bin/view-observer','identity']\nworkload_uid=501\nworkload_gid=501\n[ownerfs_workspace_bind]\nworkspace='ws'\n",
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

#[cfg(feature = "dfs")]
#[test]
fn peer_disk_cache_is_opt_in_and_requires_an_isolated_bounded_directory() {
    let default =
        Config::resolve(Role::Node, Cli::parse_from(["afs-node", "--fs", "dfs"])).unwrap();
    assert!(default.dfs_peer_cache.is_none());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.toml");
    let base = "fs='dfs'\ndata_dir='/srv/afs/data'\ndfs_mount='/mnt/afs'\n[dfs_peer_cache]\ndirectory='/srv/afs/peer-cache'\ncapacity_bytes=268435456\n";
    std::fs::write(&path, base).unwrap();
    let resolve = |role| {
        Config::resolve(
            role,
            Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()]),
        )
    };
    let cfg = resolve(Role::Node).unwrap();
    let cache = cfg.dfs_peer_cache.unwrap();
    assert_eq!(cache.capacity_bytes, 256 * 1024 * 1024);
    assert_eq!(cache.directory, std::path::Path::new("/srv/afs/peer-cache"));
    assert!(resolve(Role::Meta).is_err());
    for bad in [
        base.replace("'/srv/afs/peer-cache'", "'relative'"),
        base.replace("'/srv/afs/peer-cache'", "'/'"),
        base.replace("'/srv/afs/peer-cache'", "'/srv/../cache'"),
        base.replace("'/srv/afs/peer-cache'", "'/srv/afs/data/cache'"),
        base.replace("'/srv/afs/peer-cache'", "'/srv/afs'"),
        base.replace("'/srv/afs/peer-cache'", "'/mnt/afs/cache'"),
        base.replace("capacity_bytes=268435456", "capacity_bytes=0"),
        base.replace("capacity_bytes=268435456", "capacity_bytes=1099511627777"),
        base.replace(
            "capacity_bytes=268435456",
            "capacity_bytes=268435456\nresident_bytes=4194304",
        ),
    ] {
        std::fs::write(&path, bad).unwrap();
        assert!(resolve(Role::Node).is_err());
    }
}

#[test]
fn native_write_buffer_idle_baseline_is_not_a_live_quota() {
    let default = Config::resolve(Role::Node, Cli::parse_from(["afs-node"])).unwrap();
    assert_eq!(
        serde_json::to_value(default).unwrap()["dfs_write_buffer_idle_bytes"],
        134_217_728_u64
    );
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("node.toml");
    for bytes in [0_u64, 4096, 1_u64 << 40] {
        std::fs::write(&path, format!("dfs_write_buffer_idle_bytes = {bytes}\n")).unwrap();
        let config = Config::resolve(
            Role::Node,
            Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()]),
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(config).unwrap()["dfs_write_buffer_idle_bytes"],
            bytes
        );
    }
    std::fs::write(&path, "dfs_write_buffer_idle_bytes = 1099511627777\n").unwrap();
    assert!(
        Config::resolve(
            Role::Node,
            Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()])
        )
        .is_err()
    );
}

#[test]
fn read_diagnostics_default_off_and_explicit_cli_overrides_file() {
    let default = Config::resolve(Role::Node, Cli::parse_from(["afs-node"])).unwrap();
    assert!(!default.dfs_read_diagnostics_enabled);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("diagnostic.toml");
    std::fs::write(&path, "dfs_read_diagnostics_enabled=true\n").unwrap();
    let from_file = Config::resolve(
        Role::Node,
        Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()]),
    )
    .unwrap();
    assert!(from_file.dfs_read_diagnostics_enabled);
    let from_cli = Config::resolve(
        Role::Node,
        Cli::parse_from([
            "afs-node",
            "--config",
            path.to_str().unwrap(),
            "--dfs-read-diagnostics-enabled",
            "false",
        ]),
    )
    .unwrap();
    assert!(!from_cli.dfs_read_diagnostics_enabled);
}

#[cfg(feature = "ownerfs")]
#[test]
fn ownerfs_workspace_bind_requires_isolated_egress_root() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.toml");

    // Missing egress root already covered above; now exercise path shape.
    for (name, egress) in [
        ("relative", "relative/egress"),
        ("parent traversal", "/tmp/../owner-egress"),
        ("under mount", "/tmp/owner/egress"),
        ("under data dir", "/tmp/afs-x/data/egress"),
    ] {
        std::fs::write(
            &path,
            format!(
                "fs='ownerfs'\ndata_dir='/tmp/afs-x/data'\nownerfs_mount='/tmp/owner'\nownerfs_workspace_egress_root={egress:?}\nexperimental_ownerfs_workspace_bind=true\n[ownerfs_workspace_bind]\nworkspace='ws'\n"
            ),
        )
        .unwrap();
        let resolved = Config::resolve(
            Role::Node,
            Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()]),
        );
        assert!(
            resolved.is_err(),
            "egress root {name:?} ({egress:?}) should be rejected"
        );
    }

    // An isolated absolute egress root is accepted.
    std::fs::write(
        &path,
        "fs='ownerfs'\ndata_dir='/tmp/afs-x/data'\nownerfs_mount='/tmp/owner'\nownerfs_workspace_egress_root='/srv/egress'\nexperimental_ownerfs_workspace_bind=true\n[ownerfs_workspace_bind]\nworkspace='ws'\n",
    )
    .unwrap();
    let cfg = Config::resolve(
        Role::Node,
        Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()]),
    )
    .unwrap();
    assert_eq!(
        cfg.ownerfs_workspace_egress_root.as_deref(),
        Some(std::path::Path::new("/srv/egress"))
    );
}

#[test]
fn fuse_execution_and_dfs_cache_defaults_are_explicit() {
    let cfg = Config::resolve(Role::Node, Cli::default()).unwrap();
    let value = serde_json::to_value(cfg).unwrap();
    for (key, expected) in [
        ("fuse_threads", serde_json::json!(4)),
        ("fuse_clone_fd", serde_json::json!(true)),
        ("fuse_workers", serde_json::json!(32)),
        ("dfs_fuse_keep_cache", serde_json::json!(true)),
        ("dfs_fuse_entry_ttl_ms", serde_json::json!(0)),
        ("dfs_fuse_attr_ttl_ms", serde_json::json!(0)),
        ("fuse_max_readahead_bytes", serde_json::json!(1048576)),
    ] {
        assert_eq!(value.get(key), Some(&expected), "missing/wrong {key}");
    }
}

#[test]
fn fuse_execution_settings_merge_file_cli_and_validate_boundaries() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("node.toml");
    std::fs::write(
        &path,
        "fuse_threads=1\nfuse_clone_fd=false\nfuse_workers=1\ndfs_fuse_keep_cache=false\ndfs_fuse_entry_ttl_ms=60000\ndfs_fuse_attr_ttl_ms=60000\nfuse_max_readahead_bytes=16777216\n",
    ).unwrap();
    let cfg = Config::resolve(
        Role::Node,
        Cli::try_parse_from([
            "afs-node",
            "--config",
            path.to_str().unwrap(),
            "--fuse-threads",
            "64",
            "--fuse-workers",
            "1024",
            "--dfs-fuse-entry-ttl-ms",
            "0",
        ])
        .expect("public FUSE options must be recognized"),
    )
    .unwrap();
    let value = serde_json::to_value(cfg).unwrap();
    assert_eq!(value["fuse_threads"], 64);
    assert_eq!(value["fuse_workers"], 1024);
    assert_eq!(value["fuse_clone_fd"], false);
    assert_eq!(value["dfs_fuse_keep_cache"], false);
    assert_eq!(value["dfs_fuse_entry_ttl_ms"], 0);
    assert_eq!(value["dfs_fuse_attr_ttl_ms"], 60000);
    assert_eq!(value["fuse_max_readahead_bytes"], 16777216);
}

#[test]
fn fuse_execution_settings_reject_out_of_range_values_before_startup() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("node.toml");
    for (key, number) in [
        ("fuse_threads", 0),
        ("fuse_threads", 65),
        ("fuse_workers", 0),
        ("fuse_workers", 1025),
        ("dfs_fuse_entry_ttl_ms", 60001),
        ("dfs_fuse_attr_ttl_ms", 60001),
        ("fuse_max_readahead_bytes", 16777217),
    ] {
        std::fs::write(&path, format!("{key}={number}\n")).unwrap();
        let error = Config::resolve(
            Role::Node,
            Cli {
                config: Some(path.clone()),
                ..Cli::default()
            },
        )
        .unwrap_err();
        assert_eq!(error.code(), afs_error::CONFIG_INVALID);
        assert!(
            error.to_string().contains("FUSE"),
            "wrong range failure: {error}"
        );
    }
}

#[test]
fn p2_dfs_version_cache_bytes_default_and_file_configuration() {
    let defaults =
        serde_json::to_value(Config::resolve(Role::Node, Cli::default()).unwrap()).unwrap();
    assert_eq!(defaults["dfs_version_cache_bytes"], 32 * 1024 * 1024);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("node.toml");
    std::fs::write(&path, "dfs_version_cache_bytes=4194304\n").unwrap();
    let config = Config::resolve(
        Role::Node,
        Cli {
            config: Some(path),
            ..Cli::default()
        },
    )
    .unwrap();
    assert_eq!(
        serde_json::to_value(config).unwrap()["dfs_version_cache_bytes"],
        4 * 1024 * 1024
    );
}

#[test]
fn p2_dfs_version_cache_bytes_cli_overrides_file() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("node.toml");
    std::fs::write(&path, "dfs_version_cache_bytes=4194304\n").unwrap();
    let cli = Cli::try_parse_from([
        "afs-node",
        "--config",
        path.to_str().unwrap(),
        "--dfs-version-cache-bytes",
        "8388608",
    ])
    .expect("version cache byte retention has a public Node option");
    let config = Config::resolve(Role::Node, cli).unwrap();
    assert_eq!(
        serde_json::to_value(config).unwrap()["dfs_version_cache_bytes"],
        8 * 1024 * 1024
    );
}

#[test]
fn p2_dfs_version_cache_bytes_configuration_bounds() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("node.toml");
    for bytes in [32_u64 * 1024 * 1024, 1024 * 1024 * 1024] {
        std::fs::write(&path, format!("dfs_version_cache_bytes={bytes}\n")).unwrap();
        let config = Config::resolve(
            Role::Node,
            Cli {
                config: Some(path.clone()),
                ..Cli::default()
            },
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(config).unwrap()["dfs_version_cache_bytes"],
            bytes
        );
    }
    for bytes in [0_u64, 1024 * 1024 * 1024 + 1, 4 * 1024 * 1024 * 1024] {
        std::fs::write(&path, format!("dfs_version_cache_bytes={bytes}\n")).unwrap();
        let error = Config::resolve(
            Role::Node,
            Cli {
                config: Some(path.clone()),
                ..Cli::default()
            },
        )
        .unwrap_err();
        assert_eq!(error.code(), afs_error::CONFIG_INVALID);
        assert!(error.to_string().contains("version cache"), "{error}");
    }
    for bytes in ["0", "1073741825"] {
        let cli = Cli::try_parse_from(["afs-node", "--dfs-version-cache-bytes", bytes])
            .expect("byte option accepts an integer before configuration validation");
        assert_eq!(
            Config::resolve(Role::Node, cli).unwrap_err().code(),
            afs_error::CONFIG_INVALID
        );
    }
}

#[cfg(feature = "dfs")]
#[test]
fn c1b_version_cache_rejects_one_byte_before_node_start() {
    let error = Config::resolve(
        Role::Node,
        Cli::try_parse_from(["afs-node", "--dfs-version-cache-bytes", "1"]).unwrap(),
    )
    .unwrap_err();
    assert_eq!(error.code(), afs_error::CONFIG_INVALID);
    assert!(error.to_string().contains("version cache"), "{error}");
    assert!(error.to_string().contains("minimum"), "{error}");
}

#[cfg(feature = "dfs")]
#[test]
fn c1b_version_cache_rejects_table_larger_than_byte_limit() {
    let error = Config::resolve(
        Role::Node,
        Cli::try_parse_from([
            "afs-node",
            "--dfs-version-cache-entries",
            "65536",
            "--dfs-version-cache-bytes",
            "4194304",
        ])
        .unwrap(),
    )
    .unwrap_err();
    assert_eq!(error.code(), afs_error::CONFIG_INVALID);
    assert!(error.to_string().contains("minimum"), "{error}");
}

#[cfg(feature = "dfs")]
#[test]
fn c1b_version_cache_checks_file_bytes_with_cli_entry_override() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("node.toml");
    std::fs::write(
        &path,
        "dfs_version_cache_entries=1\ndfs_version_cache_bytes=4194304\n",
    )
    .unwrap();
    let error = Config::resolve(
        Role::Node,
        Cli::try_parse_from([
            "afs-node",
            "--config",
            path.to_str().unwrap(),
            "--dfs-version-cache-entries",
            "65536",
        ])
        .unwrap(),
    )
    .unwrap_err();
    assert_eq!(error.code(), afs_error::CONFIG_INVALID);
}

#[cfg(feature = "dfs")]
#[test]
fn c1b_version_cache_checks_file_entries_with_cli_byte_override() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("node.toml");
    std::fs::write(
        &path,
        "dfs_version_cache_entries=65536\ndfs_version_cache_bytes=33554432\n",
    )
    .unwrap();
    let error = Config::resolve(
        Role::Node,
        Cli::try_parse_from([
            "afs-node",
            "--config",
            path.to_str().unwrap(),
            "--dfs-version-cache-bytes",
            "4194304",
        ])
        .unwrap(),
    )
    .unwrap_err();
    assert_eq!(error.code(), afs_error::CONFIG_INVALID);
}

#[cfg(feature = "dfs")]
#[test]
fn c1b_version_cache_cli_can_repair_invalid_file_combination() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("node.toml");
    std::fs::write(
        &path,
        "dfs_version_cache_entries=65536\ndfs_version_cache_bytes=4194304\n",
    )
    .unwrap();
    for override_args in [
        ["--dfs-version-cache-entries", "1"],
        ["--dfs-version-cache-bytes", "33554432"],
    ] {
        let cli = Cli::try_parse_from([
            "afs-node",
            "--config",
            path.to_str().unwrap(),
            override_args[0],
            override_args[1],
        ])
        .unwrap();
        Config::resolve(Role::Node, cli).unwrap();
    }
}

#[cfg(feature = "dfs")]
#[test]
fn c1b_version_cache_accepts_exact_reported_floor_and_rejects_one_less() {
    let mut previous_minimum = 0;
    for entries in [1_usize, 4096, 65536] {
        let entries = entries.to_string();
        let error = Config::resolve(
            Role::Node,
            Cli::try_parse_from([
                "afs-node",
                "--dfs-version-cache-entries",
                &entries,
                "--dfs-version-cache-bytes",
                "1",
            ])
            .unwrap(),
        )
        .unwrap_err();
        let message = error.to_string();
        let minimum: u64 = message
            .split("minimum ")
            .nth(1)
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse()
            .unwrap();
        assert!(minimum > previous_minimum);
        assert!(minimum <= 32 * 1024 * 1024);
        previous_minimum = minimum;
        eprintln!("version cache entries={entries} minimum_bytes={minimum}");
        for bytes in [minimum - 1, minimum, minimum + 1] {
            let bytes_arg = bytes.to_string();
            let result = Config::resolve(
                Role::Node,
                Cli::try_parse_from([
                    "afs-node",
                    "--dfs-version-cache-entries",
                    &entries,
                    "--dfs-version-cache-bytes",
                    &bytes_arg,
                ])
                .unwrap(),
            );
            if bytes < minimum {
                assert_eq!(result.unwrap_err().code(), afs_error::CONFIG_INVALID);
            } else {
                assert_eq!(result.unwrap().dfs_version_cache_bytes, bytes);
            }
        }
    }
}

#[cfg(all(not(feature = "dfs"), feature = "ownerfs"))]
#[test]
fn c1b_owner_only_build_does_not_require_dfs_version_cache_storage() {
    let config = Config::resolve(
        Role::Node,
        Cli::try_parse_from(["afs-node", "--dfs-version-cache-bytes", "1"]).unwrap(),
    )
    .unwrap();
    assert_eq!(config.dfs_version_cache_bytes, 1);
}

#[cfg(feature = "dfs")]
#[test]
fn c1b_meta_role_does_not_allocate_the_node_version_cache() {
    let config = Config::resolve(
        Role::Meta,
        Cli::try_parse_from(["afs-meta", "--dfs-version-cache-bytes", "1"]).unwrap(),
    )
    .unwrap();
    assert_eq!(config.dfs_version_cache_bytes, 1);
}

#[test]
fn dfs_version_cache_capacity_has_bounded_file_cli_configuration() {
    let defaults =
        serde_json::to_value(Config::resolve(Role::Node, Cli::default()).unwrap()).unwrap();
    assert_eq!(defaults["dfs_version_cache_entries"], 4096);

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("node.toml");
    std::fs::write(&path, "dfs_version_cache_entries=7\n").unwrap();
    let file_config = Config::resolve(
        Role::Node,
        Cli {
            config: Some(path.clone()),
            ..Cli::default()
        },
    )
    .unwrap();
    assert_eq!(
        serde_json::to_value(file_config).unwrap()["dfs_version_cache_entries"],
        7
    );
    let cli_config = Config::resolve(
        Role::Node,
        Cli::try_parse_from([
            "afs-node",
            "--config",
            path.to_str().unwrap(),
            "--dfs-version-cache-entries",
            "1",
        ])
        .expect("the version cache capacity is a public Node option"),
    )
    .unwrap();
    assert_eq!(
        serde_json::to_value(cli_config).unwrap()["dfs_version_cache_entries"],
        1
    );

    for capacity in [0, 65537] {
        std::fs::write(&path, format!("dfs_version_cache_entries={capacity}\n")).unwrap();
        let error = Config::resolve(
            Role::Node,
            Cli {
                config: Some(path.clone()),
                ..Cli::default()
            },
        )
        .unwrap_err();
        assert_eq!(error.code(), afs_error::CONFIG_INVALID);
        assert!(error.to_string().contains("version cache"), "{error}");
    }
}

#[test]
fn p2_r6_serve_capacity_options_are_exposed() {
    let parsed = Cli::try_parse_from([
        "afs-node",
        "--dfs-peer-serve-resident-objects",
        "2",
        "--dfs-peer-serve-payload-bytes",
        "16777216",
    ]);
    assert!(
        parsed.is_ok(),
        "public serve configuration: {:?}",
        parsed.err()
    );
}

#[test]
fn p2_r6_serve_defaults_match_approved_capacity() {
    let config =
        serde_json::to_value(Config::resolve(Role::Node, Cli::default()).unwrap()).unwrap();
    assert_eq!(config["dfs_peer_serve_resident_objects"], 2);
    assert_eq!(config["dfs_peer_serve_payload_bytes"], 16 * 1024 * 1024);
}

#[test]
fn p2_t1_serve_wait_default_and_file_cli_share_one_bound() {
    let parsed = Cli::try_parse_from(["afs-node", "--dfs-peer-serve-wait-ms", "0"]).unwrap();
    assert_eq!(
        Config::resolve(Role::Node, parsed)
            .unwrap()
            .dfs_peer_serve_wait_ms,
        0
    );
    let defaults = Config::resolve(Role::Node, Cli::default()).unwrap();
    assert_eq!(defaults.dfs_peer_serve_wait_ms, 20);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("node.toml");
    for wait in [0, 1, 20, 50] {
        std::fs::write(&path, format!("dfs_peer_serve_wait_ms={wait}\n")).unwrap();
        let file = Config::resolve(
            Role::Node,
            Cli {
                config: Some(path.clone()),
                ..Cli::default()
            },
        )
        .unwrap();
        assert_eq!(file.dfs_peer_serve_wait_ms, wait);
        let cli = Config::resolve(
            Role::Node,
            Cli {
                config: Some(path.clone()),
                dfs_peer_serve_wait_ms: Some(0),
                ..Cli::default()
            },
        )
        .unwrap();
        assert_eq!(cli.dfs_peer_serve_wait_ms, 0);
    }
    std::fs::write(&path, "dfs_peer_serve_wait_ms=51\n").unwrap();
    assert!(
        Config::resolve(
            Role::Node,
            Cli {
                config: Some(path),
                ..Cli::default()
            }
        )
        .is_err()
    );
    assert!(
        Config::resolve(
            Role::Node,
            Cli {
                dfs_peer_serve_wait_ms: Some(51),
                ..Cli::default()
            }
        )
        .is_err()
    );
}

#[test]
fn p2_r6_serve_file_cli_capacity_is_bounded() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("node.toml");
    let resolve_file = || {
        Config::resolve(
            Role::Node,
            Cli {
                config: Some(path.clone()),
                ..Cli::default()
            },
        )
    };
    for (objects, bytes) in [
        (1, 4 * 1024 * 1024),
        (2, 16 * 1024 * 1024),
        (16, 64 * 1024 * 1024),
    ] {
        std::fs::write(
            &path,
            format!(
                "dfs_peer_serve_resident_objects={objects}\ndfs_peer_serve_payload_bytes={bytes}\n"
            ),
        )
        .unwrap();
        let config = resolve_file().unwrap();
        assert_eq!(config.dfs_peer_serve_resident_objects, objects);
        assert_eq!(config.dfs_peer_serve_payload_bytes, bytes);
    }
    for (objects, bytes) in [
        (0, 16777216_u64),
        (17, 16777216),
        (2, 0),
        (2, 4194303),
        (2, 67108865),
    ] {
        std::fs::write(
            &path,
            format!(
                "dfs_peer_serve_resident_objects={objects}\ndfs_peer_serve_payload_bytes={bytes}\n"
            ),
        )
        .unwrap();
        assert_eq!(
            resolve_file().unwrap_err().code(),
            afs_error::CONFIG_INVALID
        );
        let error = Config::resolve(
            Role::Node,
            Cli::try_parse_from([
                "afs-node",
                "--dfs-peer-serve-resident-objects",
                &objects.to_string(),
                "--dfs-peer-serve-payload-bytes",
                &bytes.to_string(),
            ])
            .unwrap(),
        )
        .unwrap_err();
        assert_eq!(error.code(), afs_error::CONFIG_INVALID);
        let overridden = Config::resolve(
            Role::Node,
            Cli::try_parse_from([
                "afs-node",
                "--config",
                path.to_str().unwrap(),
                "--dfs-peer-serve-resident-objects",
                "2",
                "--dfs-peer-serve-payload-bytes",
                "8388608",
            ])
            .unwrap(),
        )
        .unwrap();
        assert_eq!(overridden.dfs_peer_serve_resident_objects, 2);
        assert_eq!(overridden.dfs_peer_serve_payload_bytes, 8 * 1024 * 1024);
    }
}

#[test]
fn p2_r1_source_cache_options_are_exposed() {
    let parsed = Cli::try_parse_from([
        "afs-node",
        "--dfs-source-cache-max-ms",
        "5000",
        "--dfs-source-lookahead-chunks",
        "64",
        "--dfs-source-cache-bytes",
        "4194304",
    ]);
    assert!(
        parsed.is_ok(),
        "public source cache configuration: {:?}",
        parsed.err()
    );
}

#[test]
fn p2_r1_source_cache_defaults_match_bounded_reuse() {
    let config =
        serde_json::to_value(Config::resolve(Role::Node, Cli::default()).unwrap()).unwrap();
    assert_eq!(config["dfs_source_cache_max_ms"], 5000);
    assert_eq!(config["dfs_source_lookahead_chunks"], 64);
    assert_eq!(config["dfs_source_cache_bytes"], 4 * 1024 * 1024);
    assert!(config.get("dfs_read_source_cache_ttl_ms").is_none());
}

#[test]
fn p2_r1_source_cache_limits_resolve_after_cli_overrides() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("source-cache.toml");
    for (max_ms, chunks, bytes) in [(0, 0, 0), (5000, 64, 4194304), (60000, 128, 4194304)] {
        std::fs::write(&path, format!(
            "dfs_source_cache_max_ms={max_ms}\ndfs_source_lookahead_chunks={chunks}\ndfs_source_cache_bytes={bytes}\n"
        )).unwrap();
        let config = Config::resolve(
            Role::Node,
            Cli {
                config: Some(path.clone()),
                ..Cli::default()
            },
        )
        .unwrap();
        assert_eq!(
            (
                config.dfs_source_cache_max_ms,
                config.dfs_source_lookahead_chunks,
                config.dfs_source_cache_bytes
            ),
            (max_ms, chunks, bytes)
        );
    }
    for (max_ms, chunks, bytes) in [
        (60001, 64, 4194304),
        (5000, 129, 4194304),
        (5000, 64, 4194305),
    ] {
        std::fs::write(&path, format!(
            "dfs_source_cache_max_ms={max_ms}\ndfs_source_lookahead_chunks={chunks}\ndfs_source_cache_bytes={bytes}\n"
        )).unwrap();
        let error = Config::resolve(
            Role::Node,
            Cli {
                config: Some(path.clone()),
                ..Cli::default()
            },
        )
        .unwrap_err();
        assert_eq!(error.code(), afs_error::CONFIG_INVALID);
        let overridden = Config::resolve(
            Role::Node,
            Cli::try_parse_from([
                "afs-node",
                "--config",
                path.to_str().unwrap(),
                "--dfs-source-cache-max-ms",
                "5000",
                "--dfs-source-lookahead-chunks",
                "64",
                "--dfs-source-cache-bytes",
                "4194304",
            ])
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            (
                overridden.dfs_source_cache_max_ms,
                overridden.dfs_source_lookahead_chunks,
                overridden.dfs_source_cache_bytes
            ),
            (5000, 64, 4194304)
        );
        let error = Config::resolve(
            Role::Node,
            Cli::try_parse_from([
                "afs-node",
                "--dfs-source-cache-max-ms",
                &max_ms.to_string(),
                "--dfs-source-lookahead-chunks",
                &chunks.to_string(),
                "--dfs-source-cache-bytes",
                &bytes.to_string(),
            ])
            .unwrap(),
        )
        .unwrap_err();
        assert_eq!(error.code(), afs_error::CONFIG_INVALID);
    }
}

#[test]
fn p2_r1_replaced_source_cache_name_is_rejected_explicitly() {
    assert!(Cli::try_parse_from(["afs-node", "--dfs-read-source-cache-ttl-ms", "500"]).is_err());
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("old-source-cache.toml");
    std::fs::write(&path, "dfs_read_source_cache_ttl_ms=500\n").unwrap();
    let error = Config::resolve(
        Role::Node,
        Cli {
            config: Some(path),
            ..Cli::default()
        },
    )
    .unwrap_err();
    assert_eq!(error.code(), afs_error::CONFIG_INVALID);
    assert!(error.to_string().contains("dfs_read_source_cache_ttl_ms"));
}
