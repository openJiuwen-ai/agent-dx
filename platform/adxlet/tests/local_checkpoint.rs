use adxlet::checkpoint::{CheckpointStore, LocalCheckpointStore};

#[tokio::test]
async fn local_only_publication_is_readable_without_forced_writeback() {
    let root = tempfile::Builder::new()
        .prefix("adx-local-checkpoint-io-")
        .tempdir()
        .unwrap();
    let store = LocalCheckpointStore::new(root.path().into()).unwrap();
    let staged = store.allocate().await.unwrap();
    std::fs::create_dir(staged.join("disk")).unwrap();
    std::fs::write(staged.join("memory"), b"saved memory").unwrap();
    std::fs::write(staged.join("disk/overlay"), b"saved disk").unwrap();
    let artifact = store.publish(&staged).await.unwrap();
    assert_eq!(artifact.storage, "local");
    assert_eq!(artifact.size_bytes, 22);
    // A reconstructed store can use the published files immediately; publication
    // does not rely on process-local state or an explicit disk sync.
    let reopened = LocalCheckpointStore::new(root.path().into()).unwrap();
    let restored = reopened.materialize(&artifact).await.unwrap();
    assert_eq!(
        std::fs::read(restored.join("memory")).unwrap(),
        b"saved memory"
    );
    assert_eq!(
        std::fs::read(restored.join("disk/overlay")).unwrap(),
        b"saved disk"
    );
    assert_eq!(store.retain_staged(&staged).await.unwrap(), artifact);
    let duplicate = store.duplicate(&artifact).await.unwrap();
    assert_ne!(duplicate.location, artifact.location);
    store.remove(&artifact).await.unwrap();
    assert!(reopened.materialize(&artifact).await.is_err());
    let copied = reopened.materialize(&duplicate).await.unwrap();
    assert_eq!(
        std::fs::read(copied.join("memory")).unwrap(),
        b"saved memory"
    );
}

#[tokio::test]
async fn local_only_rejects_empty_unsafe_and_truncated_artifacts() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalCheckpointStore::new(root.path().into()).unwrap();
    let staged = store.allocate().await.unwrap();
    assert!(store.publish(&staged).await.is_err());
    std::fs::write(staged.join("memory"), b"saved memory").unwrap();
    std::os::unix::fs::symlink(staged.join("memory"), staged.join("alias")).unwrap();
    assert!(store.publish(&staged).await.is_err());
    std::fs::remove_file(staged.join("alias")).unwrap();
    let socket = std::os::unix::net::UnixListener::bind(staged.join("socket")).unwrap();
    assert!(store.publish(&staged).await.is_err());
    drop(socket);
    std::fs::remove_file(staged.join("socket")).unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("memory"), b"saved memory").unwrap();
    assert!(store.publish(outside.path()).await.is_err());
    let artifact = store.publish(&staged).await.unwrap();
    std::fs::write(staged.join("memory"), b"short").unwrap();
    assert!(store.validate_artifact(&artifact).await.is_err());
    store.remove(&artifact).await.unwrap();
}
