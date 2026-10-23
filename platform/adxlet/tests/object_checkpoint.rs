use adxlet::checkpoint::{CheckpointAccess, ObjectCheckpointStore};
use object_store::{memory::InMemory, ObjectStore};
use std::sync::Arc;

#[tokio::test]
async fn remote_checkpoint_survives_source_disk_loss() {
    let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let source_dir = tempfile::tempdir().unwrap();
    let source = ObjectCheckpointStore::new(
        "shared".into(),
        remote.clone(),
        "test".into(),
        source_dir.path().into(),
        1024,
    )
    .unwrap();
    let staged = source.begin_capture().await.unwrap();
    std::fs::write(staged.join("memory"), b"memory-state").unwrap();
    let artifact = source.seal(&staged).await.unwrap();
    drop(source);
    drop(source_dir);

    let destination_dir = tempfile::tempdir().unwrap();
    let destination = ObjectCheckpointStore::new(
        "shared".into(),
        remote,
        "test".into(),
        destination_dir.path().into(),
        1024,
    )
    .unwrap();
    let access = destination.prepare_restore(&artifact).await.unwrap();
    assert_eq!(
        std::fs::read(access.join("memory")).unwrap(),
        b"memory-state"
    );
}

#[tokio::test]
async fn node_release_keeps_shared_object_until_coordinator_reclaims_it() {
    use adx_checkpoint_storage::reclaim::{ArtifactReclaimer, ObjectReclaimer};
    let root = tempfile::tempdir().unwrap();
    let remote = Arc::new(InMemory::new());
    let store = ObjectCheckpointStore::new(
        "shared".into(),
        remote.clone(),
        "test".into(),
        root.path().into(),
        1024,
    )
    .unwrap();
    let staged = store.begin_capture().await.unwrap();
    std::fs::write(staged.join("memory"), b"snapshot").unwrap();
    let artifact = store.seal(&staged).await.unwrap();
    store.release(&artifact).await.unwrap();
    assert!(store.prepare_restore(&artifact).await.is_ok());
    ObjectReclaimer::new("shared".into(), remote, "test".into())
        .unwrap()
        .remove(&artifact)
        .await
        .unwrap();
    store.release(&artifact).await.unwrap();
    assert!(store.prepare_restore(&artifact).await.is_err());
}

#[tokio::test]
async fn cache_budget_evicts_only_unpinned_downloads() {
    let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let root = tempfile::tempdir().unwrap();
    let store = ObjectCheckpointStore::new(
        "shared".into(),
        remote,
        "test".into(),
        root.path().into(),
        12,
    )
    .unwrap();
    let mut artifacts = Vec::new();
    for value in [b'1', b'2', b'3'] {
        let staged = store.begin_capture().await.unwrap();
        std::fs::write(staged.join("memory"), vec![value; 6]).unwrap();
        let artifact = store.seal(&staged).await.unwrap();
        store.committed(&artifact).await.unwrap();
        artifacts.push(artifact);
    }
    let first = store.prepare_restore(&artifacts[0]).await.unwrap();
    let second = store.prepare_restore(&artifacts[1]).await.unwrap();
    let second_path = second.to_path_buf();
    drop(second);
    let third = store.prepare_restore(&artifacts[2]).await.unwrap();
    store.evict_unreferenced(0).await.unwrap();
    assert!(!second_path.exists());
    assert!(first.join("memory").exists());
    assert!(third.join("memory").exists());
}

#[tokio::test]
async fn committed_capture_restores_from_local_cache_after_remote_manifest_loss() {
    use object_store::{path::Path as ObjectPath, ObjectStoreExt};
    let remote = Arc::new(InMemory::new());
    let root = tempfile::tempdir().unwrap();
    let store = ObjectCheckpointStore::new(
        "shared".into(),
        remote.clone(),
        "test".into(),
        root.path().into(),
        1024 * 1024,
    )
    .unwrap();
    let staged = store.begin_capture().await.unwrap();
    std::fs::write(staged.join("memory"), b"local-pause").unwrap();
    let artifact = store.seal(&staged).await.unwrap();
    store.committed(&artifact).await.unwrap();
    store.committed(&artifact).await.unwrap();
    remote
        .delete(&ObjectPath::from(format!(
            "test/{}/manifest.json",
            artifact.location
        )))
        .await
        .unwrap();
    let restored = store.prepare_restore(&artifact).await.unwrap();
    assert_eq!(
        std::fs::read(restored.join("memory")).unwrap(),
        b"local-pause"
    );
}

#[tokio::test]
async fn upload_rejects_symlinks_and_keeps_staging_for_rollback() {
    let root = tempfile::tempdir().unwrap();
    let store = ObjectCheckpointStore::new(
        "shared".into(),
        Arc::new(InMemory::new()),
        "test".into(),
        root.path().into(),
        1024,
    )
    .unwrap();
    let staged = store.begin_capture().await.unwrap();
    std::fs::write(staged.join("memory"), b"recover-me").unwrap();
    std::os::unix::fs::symlink("/etc/passwd", staged.join("escape")).unwrap();
    assert!(store.seal(&staged).await.is_err());
    assert_eq!(std::fs::read(staged.join("memory")).unwrap(), b"recover-me");
}

#[tokio::test]
async fn scoped_capture_references_and_reclaimer_refuse_wrong_backend() {
    use adx_checkpoint_storage::reclaim::{ArtifactReclaimer, ObjectReclaimer};
    let remote = Arc::new(InMemory::new());
    let source_root = tempfile::tempdir().unwrap();
    let source = ObjectCheckpointStore::new_scoped(
        "shared".into(),
        "scope-a".into(),
        remote.clone(),
        "test".into(),
        source_root.path().into(),
        1024,
    )
    .unwrap();
    let staged = source.begin_capture().await.unwrap();
    let references = source.capture_references(&staged).unwrap();
    let reserved = references.iter().find(|a| a.storage == "shared").unwrap();
    assert_eq!(reserved.scope, "scope-a");
    assert_eq!(reserved.size_bytes, 0);
    std::fs::write(staged.join("memory"), b"saved").unwrap();
    let artifact = source.seal(&staged).await.unwrap();
    assert_eq!(artifact.scope, "scope-a");

    let other_root = tempfile::tempdir().unwrap();
    let other = ObjectCheckpointStore::new_scoped(
        "shared".into(),
        "scope-b".into(),
        remote.clone(),
        "test".into(),
        other_root.path().into(),
        1024,
    )
    .unwrap();
    assert!(other.prepare_restore(&artifact).await.is_err());
    assert!(
        ObjectReclaimer::new_scoped("shared".into(), "scope-b".into(), remote, "test".into(),)
            .unwrap()
            .remove(&artifact)
            .await
            .is_err()
    );
}

#[test]
fn explicit_s3_endpoint_is_required_for_a_delegable_scope() {
    use adxlet::checkpoint::StorageConfig;
    let with_endpoint = StorageConfig::S3 {
        alias: "shared".into(),
        bucket: "checkpoints".into(),
        region: "us-east-1".into(),
        endpoint: Some("https://objects.example.invalid".into()),
        allow_http: false,
        prefix: "test".into(),
        root: tempfile::tempdir().unwrap().path().into(),
        cache_budget_bytes: 1024,
        cache_max_entries: 500,
    };
    assert!(with_endpoint.shared_scope().is_some());
    let without_endpoint = StorageConfig::S3 {
        alias: "shared".into(),
        bucket: "checkpoints".into(),
        region: "us-east-1".into(),
        endpoint: None,
        allow_http: false,
        prefix: "test".into(),
        root: tempfile::tempdir().unwrap().path().into(),
        cache_budget_bytes: 1024,
        cache_max_entries: 500,
    };
    assert!(without_endpoint.shared_scope().is_none());
}
