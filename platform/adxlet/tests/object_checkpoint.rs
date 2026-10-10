use adxlet::checkpoint::{CheckpointStore, ObjectCheckpointStore};
use object_store::{memory::InMemory, path::Path as ObjectPath, ObjectStore, ObjectStoreExt};
use std::sync::Arc;

#[tokio::test]
async fn remote_checkpoint_survives_source_disk_loss_and_verifies_content() {
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
    let staged = source.allocate().await.unwrap();
    std::fs::create_dir(staged.join("nested")).unwrap();
    std::fs::write(staged.join("nested/memory"), b"memory-state").unwrap();
    let artifact = source.publish(&staged).await.unwrap();
    assert_eq!(artifact.storage, "shared");
    assert!(!artifact
        .location
        .contains(source_dir.path().to_str().unwrap()));
    drop(source);
    drop(source_dir);
    let destination_dir = tempfile::tempdir().unwrap();
    let destination = ObjectCheckpointStore::new(
        "shared".into(),
        remote.clone(),
        "test".into(),
        destination_dir.path().into(),
        1024,
    )
    .unwrap();
    let lease = destination.materialize(&artifact).await.unwrap();
    assert_eq!(
        std::fs::read(lease.join("nested/memory")).unwrap(),
        b"memory-state"
    );
    assert!(
        destination.remove(&artifact).await.is_err(),
        "active restore pins artifact"
    );
    drop(lease);
    destination.evict_unreferenced(0).await.unwrap();
    remote
        .put(
            &ObjectPath::from(format!("test/{}/files/nested/memory", artifact.location)),
            bytes::Bytes::from_static(b"corrupt-data").into(),
        )
        .await
        .unwrap();
    assert!(destination.materialize(&artifact).await.is_err());
    destination.remove(&artifact).await.unwrap();
    assert!(destination.materialize(&artifact).await.is_err());
}

#[tokio::test]
async fn cache_budget_evicts_only_unpinned_downloads() {
    let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let store = ObjectCheckpointStore::new(
        "shared".into(),
        remote,
        "test".into(),
        dir.path().into(),
        12,
    )
    .unwrap();
    let mut artifacts = vec![];
    for value in [b'1', b'2', b'3'] {
        let staged = store.allocate().await.unwrap();
        std::fs::write(staged.join("memory"), vec![value; 6]).unwrap();
        artifacts.push(store.publish(&staged).await.unwrap());
    }
    let first = store.materialize(&artifacts[0]).await.unwrap();
    let second = store.materialize(&artifacts[1]).await.unwrap();
    assert!(
        store.materialize(&artifacts[2]).await.is_err(),
        "pinned content consumes cache budget"
    );
    let second_path = second.to_path_buf();
    drop(second);
    let third = store.materialize(&artifacts[2]).await.unwrap();
    assert!(!second_path.exists());
    assert!(first.join("memory").exists());
    assert!(third.join("memory").exists());
}

#[tokio::test]
async fn upload_rejects_symlinks_and_leaves_staging_for_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let store = ObjectCheckpointStore::new(
        "shared".into(),
        Arc::new(InMemory::new()),
        "test".into(),
        dir.path().into(),
        1024,
    )
    .unwrap();
    let staged = store.allocate().await.unwrap();
    std::fs::write(staged.join("memory"), b"recover-me").unwrap();
    std::os::unix::fs::symlink("/etc/passwd", staged.join("escape")).unwrap();
    assert!(store.publish(&staged).await.is_err());
    assert_eq!(std::fs::read(staged.join("memory")).unwrap(), b"recover-me");
}

#[tokio::test]
async fn reusable_copy_has_independent_artifact_lifetime() {
    let root = tempfile::tempdir().unwrap();
    let store = ObjectCheckpointStore::new(
        "shared".into(),
        Arc::new(InMemory::new()),
        "test".into(),
        root.path().into(),
        1024,
    )
    .unwrap();
    let staging = store.allocate().await.unwrap();
    std::fs::write(staging.join("memory"), b"snapshot").unwrap();
    let source = store.publish(&staging).await.unwrap();
    let reusable = store.duplicate(&source).await.unwrap();
    assert_ne!(source.location, reusable.location);
    store.remove(&source).await.unwrap();
    let restored = store.materialize(&reusable).await.unwrap();
    assert_eq!(std::fs::read(restored.join("memory")).unwrap(), b"snapshot");
    drop(restored);
    store.remove(&reusable).await.unwrap();
}

#[test]
fn s3_client_and_control_tls_build_in_the_same_process() {
    let root = tempfile::tempdir().unwrap();
    let store = adxlet::checkpoint::StorageConfig::S3 {
        alias: "shared".into(),
        bucket: "checkpoints".into(),
        region: "us-east-1".into(),
        endpoint: Some("https://objects.example.invalid".into()),
        allow_http: false,
        prefix: "test".into(),
        root: root.path().into(),
        cache_budget_bytes: 1024,
    };
    let endpoint = tonic::transport::Endpoint::from_static("https://control.example.invalid")
        .tls_config(tonic::transport::ClientTlsConfig::new().domain_name("adx.internal"));
    assert!(endpoint.is_ok());
    assert!(store.build().is_ok());
}

#[tokio::test]
async fn orphan_gc_requires_reconciliation_and_preserves_live_and_foreign_uploads() {
    use adxlet::checkpoint::RemoteGcConfig;
    let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let old_root = tempfile::tempdir().unwrap();
    let old = ObjectCheckpointStore::new(
        "shared".into(),
        remote.clone(),
        "test".into(),
        old_root.path().into(),
        1024,
    )
    .unwrap()
    .with_owner(
        "node-a".into(),
        "boot-old".into(),
        RemoteGcConfig {
            min_age_seconds: 0,
            ..Default::default()
        },
    )
    .unwrap();
    let staged = old.allocate().await.unwrap();
    std::fs::write(staged.join("memory"), b"saved").unwrap();
    let retained = old.publish(&staged).await.unwrap();
    let orphan = old.duplicate(&retained).await.unwrap();
    // This write completed, but the process died before metadata registration.
    drop(old);
    drop(old_root);
    let new_root = tempfile::tempdir().unwrap();
    let current = ObjectCheckpointStore::new(
        "shared".into(),
        remote.clone(),
        "test".into(),
        new_root.path().into(),
        1024,
    )
    .unwrap()
    .with_owner(
        "node-a".into(),
        "boot-new".into(),
        RemoteGcConfig {
            min_age_seconds: 0,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(current.collect_remote_orphans().await.is_err());
    let active = current.duplicate(&retained).await.unwrap();
    let foreign_root = tempfile::tempdir().unwrap();
    let foreign = ObjectCheckpointStore::new(
        "shared".into(),
        remote.clone(),
        "test".into(),
        foreign_root.path().into(),
        1024,
    )
    .unwrap()
    .with_owner(
        "node-b".into(),
        "another-boot".into(),
        RemoteGcConfig::default(),
    )
    .unwrap();
    let foreign = foreign.duplicate(&retained).await.unwrap();
    current
        .authorize_remote_gc(std::slice::from_ref(&retained))
        .await
        .unwrap();
    assert_eq!(current.collect_remote_orphans().await.unwrap(), 1);
    assert!(current.materialize(&orphan).await.is_err());
    for artifact in [&retained, &active, &foreign] {
        assert!(current.materialize(artifact).await.is_ok());
    }
    assert_eq!(current.collect_remote_orphans().await.unwrap(), 0);
}

#[tokio::test]
async fn orphan_gc_keeps_unknown_objects_and_retries_pinned_artifacts() {
    use adxlet::checkpoint::RemoteGcConfig;
    let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let root = tempfile::tempdir().unwrap();
    let source = ObjectCheckpointStore::new(
        "shared".into(),
        remote.clone(),
        "test".into(),
        root.path().join("old"),
        1024,
    )
    .unwrap()
    .with_owner("node".into(), "old".into(), RemoteGcConfig::default())
    .unwrap();
    let staged = source.allocate().await.unwrap();
    std::fs::write(staged.join("memory"), b"state").unwrap();
    let artifact = source.publish(&staged).await.unwrap();
    let store = ObjectCheckpointStore::new(
        "shared".into(),
        remote.clone(),
        "test".into(),
        root.path().join("new"),
        1024,
    )
    .unwrap()
    .with_owner(
        "node".into(),
        "new".into(),
        RemoteGcConfig {
            min_age_seconds: 0,
            max_artifacts: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let mut invalid = artifact.clone();
    invalid.location = "../outside".into();
    assert!(store
        .authorize_remote_gc(&[artifact.clone(), invalid])
        .await
        .is_err());
    assert!(
        store.collect_remote_orphans().await.is_err(),
        "invalid catalog cannot open GC"
    );
    store.authorize_remote_gc(&[]).await.unwrap();
    let lease = store.materialize(&artifact).await.unwrap();
    assert_eq!(store.collect_remote_orphans().await.unwrap(), 0);
    drop(lease);
    assert_eq!(store.collect_remote_orphans().await.unwrap(), 1);
    let orphan = uuid::Uuid::new_v4();
    let owner = ObjectPath::from(format!("test/{orphan}/owner.json"));
    remote
        .put(
            &owner,
            br#"{"version":1,"node_id":"node","session_id":"old"}"#
                .to_vec()
                .into(),
        )
        .await
        .unwrap();
    let part = ObjectPath::from(format!("test/{orphan}/partial"));
    remote
        .put(&part, b"incomplete".to_vec().into())
        .await
        .unwrap();
    let unknown = ObjectPath::from(format!("test/{}/memory", uuid::Uuid::new_v4()));
    remote
        .put(&unknown, b"legacy".to_vec().into())
        .await
        .unwrap();
    let malformed = ObjectPath::from(format!("test/{}/owner.json", uuid::Uuid::new_v4()));
    remote
        .put(&malformed, b"invalid".to_vec().into())
        .await
        .unwrap();
    assert_eq!(store.collect_remote_orphans().await.unwrap(), 1);
    assert!(remote.head(&part).await.is_err());
    assert!(remote.head(&owner).await.is_err());
    assert!(remote.head(&unknown).await.is_ok());
    assert!(remote.head(&malformed).await.is_ok());
}

#[tokio::test]
async fn orphan_gc_grace_period_protects_recent_uploads() {
    use adxlet::checkpoint::RemoteGcConfig;
    let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let root = tempfile::tempdir().unwrap();
    let key = ObjectPath::from(format!("test/{}/owner.json", uuid::Uuid::new_v4()));
    remote
        .put(
            &key,
            br#"{"version":1,"node_id":"node","session_id":"old"}"#
                .to_vec()
                .into(),
        )
        .await
        .unwrap();
    let store = ObjectCheckpointStore::new(
        "shared".into(),
        remote.clone(),
        "test".into(),
        root.path().into(),
        1024,
    )
    .unwrap()
    .with_owner("node".into(), "new".into(), RemoteGcConfig::default())
    .unwrap();
    store.authorize_remote_gc(&[]).await.unwrap();
    assert_eq!(store.collect_remote_orphans().await.unwrap(), 0);
    assert!(remote.head(&key).await.is_ok());
}
