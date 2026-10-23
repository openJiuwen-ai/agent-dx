use adx_core::CheckpointArtifact;
use adxlet::checkpoint::{
    CheckpointAccess, ObjectCheckpointStore, StorageConfig, DEFAULT_CACHE_BUDGET_BYTES,
    DEFAULT_CACHE_MAX_ENTRIES,
};
use object_store::{memory::InMemory, path::Path as Key, ObjectStore, ObjectStoreExt};
use std::{os::unix::fs::MetadataExt, path::Path, sync::Arc, time::Duration};

fn store(
    root: &Path,
    remote: Arc<dyn ObjectStore>,
    bytes: u64,
    entries: usize,
) -> ObjectCheckpointStore {
    ObjectCheckpointStore::new_scoped(
        "shared".into(),
        "scope".into(),
        remote,
        "test".into(),
        root.into(),
        bytes,
    )
    .unwrap()
    .with_max_entries(entries)
    .unwrap()
}
async fn publish(
    store: &ObjectCheckpointStore,
    value: &[u8],
) -> (CheckpointArtifact, std::path::PathBuf) {
    let path = store.begin_capture().await.unwrap();
    std::fs::write(path.join("memory"), value).unwrap();
    let artifact = store.seal(&path).await.unwrap();
    (artifact, path)
}
async fn pause(
    store: &ObjectCheckpointStore,
    value: &[u8],
) -> (CheckpointArtifact, std::path::PathBuf) {
    let (artifact, path) = publish(store, value).await;
    store.committed(&artifact).await.unwrap();
    (artifact, path)
}
fn allocated(path: &Path) -> u64 {
    let metadata = std::fs::symlink_metadata(path).unwrap();
    let mut bytes = metadata.blocks() * 512;
    if metadata.is_dir() {
        bytes += std::fs::read_dir(path)
            .unwrap()
            .map(|e| allocated(&e.unwrap().path()))
            .sum::<u64>();
    }
    bytes
}
async fn removed(path: &Path) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("released overflow file must be reclaimed");
}
#[test]
fn s3_cache_defaults_are_500_entries_and_50_gib() {
    let config: StorageConfig = serde_json::from_value(serde_json::json!({
        "kind":"s3", "alias":"shared", "bucket":"test", "region":"us-east-1",
        "prefix":"test", "root":"/tmp/not-created"
    }))
    .unwrap();
    match config {
        StorageConfig::S3 {
            cache_budget_bytes,
            cache_max_entries,
            ..
        } => {
            assert_eq!(cache_max_entries, 500);
            assert_eq!(cache_budget_bytes, 53_687_091_200);
            assert_eq!(cache_max_entries, DEFAULT_CACHE_MAX_ENTRIES);
            assert_eq!(cache_budget_bytes, DEFAULT_CACHE_BUDGET_BYTES);
        }
        _ => panic!("expected S3 configuration"),
    }
}
#[tokio::test]
async fn entry_limit_evicts_lru_and_keeps_obs_recoverable() {
    let root = tempfile::tempdir().unwrap();
    let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = store(root.path(), remote.clone(), DEFAULT_CACHE_BUDGET_BYTES, 2);
    let (a, ap) = pause(&store, b"first").await;
    let (b, bp) = pause(&store, b"second").await;
    drop(store.prepare_restore(&a).await.unwrap());
    store
        .evict_unreferenced(DEFAULT_CACHE_BUDGET_BYTES)
        .await
        .unwrap();
    let (_, cp) = pause(&store, b"third").await;
    assert!(ap.exists());
    assert!(!bp.exists(), "least recently used checkpoint is evicted");
    assert!(cp.exists());
    assert!(remote
        .head(&Key::from(format!("test/{}/manifest.json", b.location)))
        .await
        .is_ok());
    let restored = store.prepare_restore(&b).await.unwrap();
    assert_eq!(std::fs::read(restored.join("memory")).unwrap(), b"second");
}
#[tokio::test]
async fn byte_limit_alone_evicts_while_entry_count_is_below_limit() {
    let root = tempfile::tempdir().unwrap();
    let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let initial = store(root.path(), remote.clone(), DEFAULT_CACHE_BUDGET_BYTES, 500);
    let (a, ap) = pause(&initial, b"first").await;
    let index = root
        .path()
        .join("cache-index")
        .join(format!("{}.json", a.location));
    let budget = allocated(&ap) + allocated(&index);
    drop(initial);
    let store = store(root.path(), remote, budget, 500);
    store
        .reconcile_local(std::slice::from_ref(&a))
        .await
        .unwrap();
    let (_, bp) = pause(&store, b"second").await;
    assert!(
        !ap.exists(),
        "bytes trigger eviction independently of entry count"
    );
    assert!(bp.exists());
}
#[tokio::test]
async fn all_pinned_entries_do_not_block_another_restore() {
    let root = tempfile::tempdir().unwrap();
    let store = store(root.path(), Arc::new(InMemory::new()), 1, 1);
    let mut leases = vec![];
    let mut artifacts = vec![];
    for value in [b"first".as_slice(), b"second", b"third"] {
        let (artifact, _) = publish(&store, value).await;
        let lease = store.prepare_restore(&artifact).await.unwrap();
        store.committed(&artifact).await.unwrap();
        assert_eq!(std::fs::read(lease.join("memory")).unwrap(), value);
        leases.push(lease);
        artifacts.push(artifact);
    }
    store.evict_unreferenced(0).await.unwrap();
    for lease in &leases {
        assert!(lease.join("memory").exists());
    }
    let path = leases.pop().unwrap().to_path_buf();
    removed(&path).await;
    for lease in &leases {
        assert!(lease.join("memory").exists());
    }
    for artifact in artifacts {
        assert!(store.validate_artifact(&artifact).await.is_ok());
    }
}
#[tokio::test]
async fn oversized_cold_download_is_reclaimed_after_last_lease() {
    let root = tempfile::tempdir().unwrap();
    let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let source = store(&root.path().join("source"), remote.clone(), 1, 1);
    let (artifact, staged) = pause(&source, b"larger-than-the-cache").await;
    assert!(
        !staged.exists(),
        "oversized Pause does not retain a reusable copy"
    );
    let target = store(&root.path().join("target"), remote, 1, 1);
    let first = target.prepare_restore(&artifact).await.unwrap();
    let second = target.prepare_restore(&artifact).await.unwrap();
    let path = first.to_path_buf();
    assert_eq!(
        path,
        second.to_path_buf(),
        "concurrent users share one local artifact"
    );
    drop(first);
    target.evict_unreferenced(0).await.unwrap();
    assert!(path.exists(), "another execution still owns this file");
    drop(second);
    removed(&path).await;
}
#[tokio::test]
async fn restart_rebuilds_recency_before_enforcing_smaller_entry_limit() {
    let root = tempfile::tempdir().unwrap();
    let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let old = store(root.path(), remote.clone(), DEFAULT_CACHE_BUDGET_BYTES, 10);
    let (a, ap) = pause(&old, b"first").await;
    let (b, bp) = pause(&old, b"second").await;
    let (c, cp) = pause(&old, b"third").await;
    drop(old.prepare_restore(&a).await.unwrap());
    old.evict_unreferenced(DEFAULT_CACHE_BUDGET_BYTES)
        .await
        .unwrap();
    drop(old);
    let current = store(root.path(), remote, DEFAULT_CACHE_BUDGET_BYTES, 2);
    current.reconcile_local(&[a, b, c]).await.unwrap();
    assert!(ap.exists(), "persisted latest access survives restart");
    assert!(!bp.exists());
    assert!(cp.exists());
}
#[tokio::test]
async fn restart_protects_live_files_before_trimming_and_cleans_partial_downloads() {
    let root = tempfile::tempdir().unwrap();
    let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let old = store(root.path(), remote.clone(), DEFAULT_CACHE_BUDGET_BYTES, 10);
    let (a, ap) = pause(&old, b"live-base").await;
    let (b, bp) = pause(&old, b"paused").await;
    drop(old);
    let partial = root.path().join("cache/.download-interrupted");
    std::fs::create_dir(&partial).unwrap();
    std::fs::write(partial.join("memory"), b"partial").unwrap();
    let current = store(root.path(), remote, 1, 1);
    let lease = current.prepare_restore(&a).await.unwrap();
    current.reconcile_local(&[a, b]).await.unwrap();
    assert!(ap.exists());
    assert!(!bp.exists());
    assert!(!partial.exists());
    drop(lease);
    removed(&ap).await;
}
#[tokio::test]
async fn reconciliation_drops_unretained_and_corrupt_copies_then_obs_restores() {
    let root = tempfile::tempdir().unwrap();
    let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let old = store(root.path(), remote.clone(), DEFAULT_CACHE_BUDGET_BYTES, 10);
    let (a, ap) = pause(&old, b"correct").await;
    let (_, orphan) = pause(&old, b"unretained").await;
    drop(old);
    std::fs::write(ap.join("memory"), b"corrupt").unwrap();
    let current = store(root.path(), remote, DEFAULT_CACHE_BUDGET_BYTES, 10);
    current
        .reconcile_local(std::slice::from_ref(&a))
        .await
        .unwrap();
    assert!(!ap.exists());
    assert!(!orphan.exists());
    let restored = current.prepare_restore(&a).await.unwrap();
    assert_eq!(std::fs::read(restored.join("memory")).unwrap(), b"correct");
}
#[tokio::test]
async fn same_artifact_concurrent_materialize_shares_one_download() {
    let root = tempfile::tempdir().unwrap();
    let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let source = store(
        &root.path().join("source"),
        remote.clone(),
        DEFAULT_CACHE_BUDGET_BYTES,
        500,
    );
    let (artifact, _) = pause(&source, b"shared").await;
    let target = Arc::new(store(&root.path().join("target"), remote, 1, 1));
    let mut tasks = vec![];
    for _ in 0..8 {
        let target = target.clone();
        let artifact = artifact.clone();
        tasks.push(tokio::spawn(async move {
            target.prepare_restore(&artifact).await.unwrap()
        }));
    }
    let mut leases = vec![];
    for task in tasks {
        leases.push(task.await.unwrap());
    }
    let path = leases[0].to_path_buf();
    for lease in &leases {
        assert_eq!(path, lease.to_path_buf());
    }
    drop(leases);
    removed(&path).await;
}

#[tokio::test]
async fn restart_without_index_preserves_the_live_original_staging_path() {
    let root = tempfile::tempdir().unwrap();
    let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let old = store(root.path(), remote.clone(), DEFAULT_CACHE_BUDGET_BYTES, 500);
    let (artifact, staged) = pause(&old, b"live-incremental-base").await;
    drop(old);
    std::fs::remove_file(
        root.path()
            .join("cache-index")
            .join(format!("{}.json", artifact.location)),
    )
    .unwrap();
    let current = store(root.path(), remote, 1, 1);
    let lease = current.prepare_restore(&artifact).await.unwrap();
    assert_eq!(
        lease.to_path_buf(),
        staged,
        "recovery must pin the path the live backend owns"
    );
    current.committed(&artifact).await.unwrap();
    current
        .reconcile_local(std::slice::from_ref(&artifact))
        .await
        .unwrap();
    assert!(staged.join("memory").exists());
    drop(lease);
    removed(&staged).await;
}

#[tokio::test]
async fn uncommitted_upload_is_not_a_reusable_eviction_candidate() {
    let root = tempfile::tempdir().unwrap();
    let store = store(root.path(), Arc::new(InMemory::new()), 1, 1);
    let (artifact, staged) = publish(&store, b"awaiting-authoritative-commit").await;
    drop(store.prepare_restore(&artifact).await.unwrap());
    store.evict_unreferenced(0).await.unwrap();
    assert!(
        staged.exists(),
        "LRU cannot discard an upload before metadata commit"
    );
    store.committed(&artifact).await.unwrap();
    assert!(
        !staged.exists(),
        "after commit the oversized reusable copy can be evicted"
    );
}
