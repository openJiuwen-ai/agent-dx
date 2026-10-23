use adxlet::checkpoint::StorageConfig;

#[test]
fn s3_checkpoint_storage_initializes_without_prior_tls_calls() {
    let root = tempfile::tempdir().unwrap();
    let storage = StorageConfig::S3 {
        alias: "shared".into(),
        bucket: "checkpoints".into(),
        region: "us-east-1".into(),
        endpoint: Some("http://127.0.0.1:9000".into()),
        allow_http: true,
        prefix: "test".into(),
        root: root.path().into(),
        cache_budget_bytes: 1024,
        cache_max_entries: 500,
    };

    assert!(storage.build().is_ok());
}

#[test]
fn shared_scope_requires_an_explicit_endpoint_and_binds_the_alias() {
    let root = tempfile::tempdir().unwrap();
    let storage =
        |alias: &str, bucket: &str, region: &str, endpoint: Option<&str>, prefix: &str| {
            StorageConfig::S3 {
                alias: alias.into(),
                bucket: bucket.into(),
                region: region.into(),
                endpoint: endpoint.map(Into::into),
                allow_http: false,
                prefix: prefix.into(),
                root: root.path().into(),
                cache_budget_bytes: 1024,
                cache_max_entries: 500,
            }
        };
    let configured = storage(
        "shared-a",
        "checkpoints",
        "us-east-1",
        Some("https://objects.example.internal"),
        "checkpoints",
    );
    let differently_named = storage(
        "shared-b",
        "checkpoints",
        "us-east-1",
        Some("https://objects.example.internal"),
        "checkpoints",
    );

    assert_ne!(configured.shared_scope(), differently_named.shared_scope());
    for changed in [
        storage(
            "shared-a",
            "checkpoints",
            "us-east-1",
            Some("https://other.example.internal"),
            "checkpoints",
        ),
        storage(
            "shared-a",
            "other-checkpoints",
            "us-east-1",
            Some("https://objects.example.internal"),
            "checkpoints",
        ),
        storage(
            "shared-a",
            "checkpoints",
            "us-west-2",
            Some("https://objects.example.internal"),
            "checkpoints",
        ),
        storage(
            "shared-a",
            "checkpoints",
            "us-east-1",
            Some("https://objects.example.internal"),
            "other-checkpoints",
        ),
    ] {
        assert_ne!(configured.shared_scope(), changed.shared_scope());
    }
    assert!(
        storage("shared-a", "checkpoints", "us-east-1", None, "checkpoints")
            .shared_scope()
            .is_none()
    );
}
