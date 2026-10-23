use adxlet::checkpoint::{CheckpointAccess, LocalCheckpointStore};

#[tokio::test]
async fn local_only_publication_is_readable_without_forced_writeback() {
    let root = tempfile::Builder::new()
        .prefix("adx-local-checkpoint-io-")
        .tempdir()
        .unwrap();
    let store = LocalCheckpointStore::new(root.path().into()).unwrap();
    let staged = store.begin_capture().await.unwrap();
    std::fs::create_dir(staged.join("disk")).unwrap();
    std::fs::write(staged.join("memory"), b"saved memory").unwrap();
    std::fs::write(staged.join("disk/overlay"), b"saved disk").unwrap();
    let artifact = store.seal(&staged).await.unwrap();
    assert_eq!(artifact.storage, "local");
    assert_eq!(artifact.size_bytes, 22);
    // Reconstruction needs no process-local state.
    let reopened = LocalCheckpointStore::new(root.path().into()).unwrap();
    let restored = reopened.prepare_restore(&artifact).await.unwrap();
    assert_eq!(
        std::fs::read(restored.join("memory")).unwrap(),
        b"saved memory"
    );
    assert_eq!(
        std::fs::read(restored.join("disk/overlay")).unwrap(),
        b"saved disk"
    );
    assert_eq!(store.retain_staged(&staged).await.unwrap(), artifact);
    store.release(&artifact).await.unwrap();
    assert!(reopened.prepare_restore(&artifact).await.is_err());
}

#[tokio::test]
async fn local_only_rejects_empty_unsafe_and_truncated_artifacts() {
    let root = tempfile::Builder::new().tempdir_in("/tmp").unwrap();
    let store = LocalCheckpointStore::new(root.path().into()).unwrap();
    let staged = store.begin_capture().await.unwrap();
    assert!(store.seal(&staged).await.is_err());
    std::fs::write(staged.join("memory"), b"saved memory").unwrap();
    std::os::unix::fs::symlink(staged.join("memory"), staged.join("alias")).unwrap();
    assert!(store.seal(&staged).await.is_err());
    std::fs::remove_file(staged.join("alias")).unwrap();
    let socket = std::os::unix::net::UnixListener::bind(staged.join("socket")).unwrap();
    assert!(store.seal(&staged).await.is_err());
    drop(socket);
    std::fs::remove_file(staged.join("socket")).unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("memory"), b"saved memory").unwrap();
    assert!(store.seal(outside.path()).await.is_err());
    let artifact = store.seal(&staged).await.unwrap();
    std::fs::write(staged.join("memory"), b"short").unwrap();
    assert!(store.validate_artifact(&artifact).await.is_err());
    store.release(&artifact).await.unwrap();
}
