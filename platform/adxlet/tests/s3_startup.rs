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
    };

    assert!(storage.build().is_ok());
}
