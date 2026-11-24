use super::*;

#[tokio::test]
async fn staging_probe_uses_local_storage_even_when_remote_requests_cannot_run() {
    let root = tempfile::tempdir().unwrap();
    let remote = object_store::limit::LimitStore::new(object_store::memory::InMemory::new(), 0);
    let store = ObjectCheckpointStore::new(
        "shared".into(),
        Arc::new(remote),
        "checkpoints".into(),
        root.path().into(),
        1024,
    )
    .unwrap();
    // Artifact cache bookkeeping is independent of capture staging.
    let index = root.path().join("cache-index");
    std::fs::remove_dir(&index).unwrap();
    std::fs::write(&index, b"unavailable").unwrap();

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        store.probe_capture_staging(),
    )
    .await
    .expect("staging qualification must not contact the object store")
    .unwrap();

    let staging = root.path().join("staging");
    std::fs::remove_dir(&staging).unwrap();
    assert!(store.probe_capture_staging().await.is_err());
}

#[tokio::test(start_paused = true)]
async fn idle_verification_is_paced_and_yields_to_execution() {
    let root = tempfile::tempdir().unwrap();
    let data = vec![42; 512 * 1024];
    std::fs::write(root.path().join("memory"), &data).unwrap();
    let manifest = Manifest {
        version: 1,
        size: data.len() as u64,
        directories: vec![],
        files: vec![FileEntry {
            path: "memory".into(),
            size: data.len() as u64,
            sha256: format!("{:x}", Sha256::digest(&data)),
            mode: 0o600,
        }],
    };
    let pin = Arc::new(());
    let idle = Arc::downgrade(&pin);
    let started = tokio::time::Instant::now();
    assert_eq!(
        ObjectCheckpointStore::valid_cached(root.path(), &manifest, CacheValidation::Idle(&idle))
            .await
            .unwrap(),
        Some(true)
    );
    assert!(started.elapsed() >= std::time::Duration::from_micros(data.len() as u64 / 16));
    let _execution = pin.clone();
    assert_eq!(
        ObjectCheckpointStore::valid_cached(root.path(), &manifest, CacheValidation::Idle(&idle))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        ObjectCheckpointStore::valid_cached(root.path(), &manifest, CacheValidation::Metadata)
            .await
            .unwrap(),
        Some(true)
    );
}
