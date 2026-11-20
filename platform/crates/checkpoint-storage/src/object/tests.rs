use super::*;

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
        ObjectCheckpointStore::valid_cached(root.path(), &manifest, Some(&idle))
            .await
            .unwrap(),
        Some(true)
    );
    assert!(started.elapsed() >= std::time::Duration::from_micros(data.len() as u64 / 16));
    let _execution = pin.clone();
    assert_eq!(
        ObjectCheckpointStore::valid_cached(root.path(), &manifest, Some(&idle))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        ObjectCheckpointStore::valid_cached(root.path(), &manifest, None)
            .await
            .unwrap(),
        Some(true)
    );
}
