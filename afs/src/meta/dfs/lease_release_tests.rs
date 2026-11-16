//! `ReleaseWriteLeases` (Meta side; Nodes do not call it yet, R106).
use super::*;
use crate::dfs::{CommitMetadataDelta, CommitMetadataMode, InodeAttributes, SyncInodeMetadata};
use crate::meta::store::{Store, journal::MemoryLog};

/// A DFS service and a file node-a created, with the write lease it holds.
async fn fixture(lease_seconds: u64) -> (DfsService, InodeRecord, WriteLease) {
    let store = Arc::new(
        Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
            .await
            .unwrap(),
    );
    let dfs = DfsService::with_replication_config(store, ReplicationConfig::local_single_copy());
    dfs.initialize_replication_config().await.unwrap();
    let (inode, lease) = dfs
        .create(CreateFileRequest {
            caller_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            operation_id: OperationId::new("create-f"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: InodeId::new("1"),
            name: b"f".to_vec(),
            attributes: InodeAttributes {
                mode: 0o100644,
                uid: 0,
                gid: 0,
                nlink: 1,
                atime_unix_ms: 1,
                mtime_unix_ms: 1,
                ctime_unix_ms: 1,
            },
            lease_seconds,
            first_generation: None,
            caller: crate::dfs::CallerContext {
                uid: 0,
                gid: 0,
                supplementary_gids: Vec::new(),
            },
        })
        .await
        .unwrap();
    (dfs, inode, lease)
}

async fn release(dfs: &DfsService, operation: &str, lease: &WriteLease) -> Result<usize> {
    dfs.release_write_leases(
        "node-a".into(),
        "session-a".into(),
        OperationId::new(operation),
        vec![lease.clone()],
    )
    .await
}

async fn stored(dfs: &DfsService, inode: &InodeRecord) -> WriteLease {
    match dfs
        .store
        .read(MetaRead::DfsWriteLease(inode.inode_id.clone()))
        .await
        .unwrap()
        .entity
    {
        Some(MetaEntity::DfsWriteLease(lease)) => lease,
        other => panic!("lease entity expected, got {other:?}"),
    }
}

async fn acquire(dfs: &DfsService, inode: &InodeRecord, operation: &str) -> WriteLease {
    dfs.open_write(
        "node-a".into(),
        "session-a".into(),
        OperationId::new(operation),
        inode.inode_id.clone(),
        30,
    )
    .await
    .unwrap()
    .1
}

fn sync(inode: &InodeRecord, lease: &WriteLease, operation: &str) -> SyncInodeMetadata {
    SyncInodeMetadata {
        operation_id: OperationId::new(operation),
        inode_id: inode.inode_id.clone(),
        write_lease: lease.clone(),
        expected_inode_revision: inode.revision,
        expected_head_version: inode.head_version.clone(),
        metadata_delta: CommitMetadataDelta {
            mode: CommitMetadataMode::Full,
            mtime_unix_ms: Some(7),
            ctime_unix_ms: Some(7),
            kill_suidgid: false,
        },
    }
}

/// A lease that no longer equals the presented copy is skipped, and that is
/// a success: there is nothing of the caller's left to release.
#[tokio::test]
async fn a_lease_that_changed_is_skipped_as_success() {
    let (dfs, inode, lease) = fixture(30).await;
    let mut older = lease.clone();
    older.expires_at_unix_ms -= 1;
    assert_eq!(release(&dfs, "release-stale", &older).await.unwrap(), 0);
    assert_eq!(
        stored(&dfs, &inode).await,
        lease,
        "the live lease is untouched"
    );
    let error = dfs
        .release_write_leases(
            "node-b".into(),
            "session-b".into(),
            OperationId::new("release-foreign"),
            vec![lease.clone()],
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        afs_error::META_DFS_CONFLICT,
        "only the owner releases"
    );
}

/// A release ends the lease now; the same operation id replays without
/// touching a lease taken since.
#[tokio::test]
async fn a_replayed_release_never_ends_a_lease_taken_since() {
    let (dfs, inode, lease) = fixture(30).await;
    assert_eq!(release(&dfs, "release-1", &lease).await.unwrap(), 1);
    let ended = stored(&dfs, &inode).await;
    assert!(ended.expires_at_unix_ms <= now_unix_ms());
    assert_eq!(
        ended.lease_epoch, lease.lease_epoch,
        "the epoch moves on the next acquire"
    );
    let again = acquire(&dfs, &inode, "acquire-2").await;
    assert_eq!(
        release(&dfs, "release-1", &lease).await.unwrap(),
        0,
        "replay"
    );
    assert_eq!(
        stored(&dfs, &inode).await,
        again,
        "the new lease survives the replay"
    );
    // The id stays bound to its release.
    let error = dfs
        .renew_write_lease(
            "node-a".into(),
            "session-a".into(),
            OperationId::new("release-1"),
            again.clone(),
            30,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), afs_error::META_CATALOG_INVALID_REQUEST);
}

/// The same owner renewing (or re-acquiring) while the lease is live keeps
/// the epoch and moves the expiry; a release of the earlier copy arriving
/// after that is skipped and the renewed lease stays valid.
#[tokio::test]
async fn a_release_arriving_after_a_renewal_is_skipped() {
    let (dfs, inode, lease) = fixture(5).await;
    let renewed = dfs
        .renew_write_lease(
            "node-a".into(),
            "session-a".into(),
            OperationId::new("renew-1"),
            lease.clone(),
            30,
        )
        .await
        .unwrap();
    assert_eq!(renewed.lease_epoch, lease.lease_epoch);
    assert_ne!(renewed.expires_at_unix_ms, lease.expires_at_unix_ms);
    assert_eq!(release(&dfs, "release-late", &lease).await.unwrap(), 0);
    assert_eq!(stored(&dfs, &inode).await, renewed);
    dfs.validate_write_lease("node-a", &inode.inode_id, &renewed)
        .await
        .expect("the renewed lease is still the current one");
}

/// After a release the next acquire takes the next epoch, so anything still
/// holding the released lease (a proxy session) is fenced.
#[tokio::test]
async fn an_acquire_after_a_release_takes_the_next_epoch() {
    let (dfs, inode, lease) = fixture(30).await;
    release(&dfs, "release", &lease).await.unwrap();
    let next = acquire(&dfs, &inode, "acquire").await;
    assert_eq!(next.lease_epoch, lease.lease_epoch + 1);
    let error = dfs
        .validate_write_lease("node-a", &inode.inode_id, &lease)
        .await
        .unwrap_err();
    assert_eq!(error.code(), afs_error::META_DFS_CONFLICT);
}

/// A metadata sync (and a commit: both go through `validate_write_lease`)
/// with a released lease is refused with the code an expired lease gets.
#[tokio::test]
async fn a_released_lease_is_refused_like_an_expired_one() {
    let (dfs, inode, lease) = fixture(30).await;
    release(&dfs, "release", &lease).await.unwrap();
    let released = dfs
        .sync_inode_metadata("node-a".into(), sync(&inode, &lease, "sync-released"))
        .await
        .unwrap_err();
    let (dfs, inode, lease) = fixture(1).await;
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    let expired = dfs
        .sync_inode_metadata("node-a".into(), sync(&inode, &lease, "sync-expired"))
        .await
        .unwrap_err();
    assert_eq!(released.code(), expired.code());
    assert_eq!(released.code(), afs_error::META_DFS_CONFLICT);
}
