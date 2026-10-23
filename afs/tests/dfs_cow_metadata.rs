use afs::{
    dfs::{
        ChunkId, CloneFileRange, CommitFileVersion, CommitMetadataDelta, CommitMetadataMode,
        Extent, FileVersion, FileVersionId, InodeAttributes, InodeId, InodeRecord, LayoutRoot,
        LayoutRootId, NamespaceId, OperationId, ReplicationConfig, WriteLease,
    },
    meta::{
        dfs::{CreateFileRequest, DfsService},
        store::{
            MetaEntity, MetaRead, MetaStore, MetaTxn, OperationResult, RequestKey, RequestOutcome,
            Store, StoreOperation, TxnCondition, TxnMutation, journal::MemoryLog,
        },
    },
};
use std::sync::Arc;
fn attrs() -> InodeAttributes {
    InodeAttributes {
        mode: 0o644,
        uid: 1000,
        gid: 1000,
        nlink: 1,
        atime_unix_ms: 1,
        mtime_unix_ms: 1,
        ctime_unix_ms: 1,
    }
}

fn create_request(operation: impl Into<String>, name: impl Into<Vec<u8>>) -> CreateFileRequest {
    CreateFileRequest {
        caller_id: "node-a".into(),
        owner_session_id: "session-a".into(),
        operation_id: OperationId::new(operation),
        namespace_id: NamespaceId::new("default"),
        parent_inode_id: InodeId::new("1"),
        name: name.into(),
        attributes: attrs(),
        lease_seconds: 30,
    }
}

fn empty_commit(
    inode: &InodeRecord,
    lease: WriteLease,
    operation: &str,
    length: u64,
) -> CommitFileVersion {
    let layout = LayoutRoot {
        id: LayoutRootId::new(format!("{operation}-layout")),
        file_length: length,
        inline_extents: vec![],
    };
    CommitFileVersion {
        gc_fence: None,
        operation_id: OperationId::new(operation),
        inode_id: inode.inode_id.clone(),
        write_lease: lease,
        expected_inode_revision: inode.revision,
        expected_head_version: inode.head_version.clone(),
        file_version: FileVersion {
            id: FileVersionId::new(format!("{operation}-version")),
            inode_id: inode.inode_id.clone(),
            parent_version: inode.head_version.clone(),
            length,
            layout_root: layout.id.clone(),
            created_at_unix_ms: 1,
        },
        layout_root: layout,
        chunk_receipts: vec![],
        clone_source: None,
        metadata_delta: CommitMetadataDelta {
            mode: CommitMetadataMode::DataOnly,
            mtime_unix_ms: None,
            ctime_unix_ms: None,
            kill_suidgid: false,
        },
    }
}

async fn fixture() -> (DfsService, Arc<Store>, CommitFileVersion) {
    let store = Arc::new(
        Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
            .await
            .unwrap(),
    );
    let service =
        DfsService::with_replication_config(store.clone(), ReplicationConfig::local_single_copy());
    service.initialize_replication_config().await.unwrap();
    let (source, lease) = service
        .create(create_request("create-source", b"source".to_vec()))
        .await
        .unwrap();
    service
        .commit_file_version(
            "node-a".into(),
            empty_commit(&source, lease, "source", 4 * 1024 * 1024 * 1024),
        )
        .await
        .unwrap();
    let (target, lease) = service
        .create(create_request("create-target", b"target".to_vec()))
        .await
        .unwrap();
    let mut commit = empty_commit(&target, lease, "clone", 1024 * 1024 * 1024);
    commit.clone_source = Some(CloneFileRange {
        version: FileVersionId::new("source-version"),
        source_offset: 1024 * 1024 * 1024,
        destination_offset: 0,
        length: 1024 * 1024 * 1024,
    });
    (service, store, commit)
}

#[tokio::test]
async fn cow_creates_independent_version_and_replays_the_exact_operation() {
    let (service, _, commit) = fixture().await;
    let result = service
        .commit_file_version("node-a".into(), commit.clone())
        .await
        .unwrap();
    assert_eq!(
        result.head_version,
        Some(FileVersionId::new("clone-version"))
    );
    assert_eq!(
        service
            .commit_file_version("node-a".into(), commit)
            .await
            .unwrap(),
        result
    );
    let (source, _) = service
        .get_file_version(FileVersionId::new("source-version"))
        .await
        .unwrap();
    assert_eq!(source.length, 4 * 1024 * 1024 * 1024);
    assert_eq!(
        service
            .get_inode(source.inode_id)
            .await
            .unwrap()
            .head_version,
        Some(source.id)
    );
}

#[tokio::test]
async fn cow_rejects_forged_layout_and_changed_retry() {
    let (service, _, mut commit) = fixture().await;
    commit.layout_root.inline_extents.push(Extent {
        file_offset: 0,
        length: 1,
        chunk_id: ChunkId::new("not-in-source"),
        chunk_offset: 0,
    });
    assert_eq!(
        service
            .commit_file_version("node-a".into(), commit.clone())
            .await
            .unwrap_err()
            .code(),
        afs_error::META_CATALOG_INVALID_REQUEST
    );
    commit.layout_root.inline_extents.clear();
    service
        .commit_file_version("node-a".into(), commit.clone())
        .await
        .unwrap();
    commit.clone_source.as_mut().unwrap().source_offset = 0;
    assert_eq!(
        service
            .commit_file_version("node-a".into(), commit)
            .await
            .unwrap_err()
            .code(),
        afs_error::META_CATALOG_INVALID_REQUEST
    );
}

#[tokio::test]
async fn cow_rejects_cross_namespace_source() {
    let (service, store, commit) = fixture().await;
    let (version, _) = service
        .get_file_version(FileVersionId::new("source-version"))
        .await
        .unwrap();
    let Some(MetaEntity::DfsInode(mut inode)) = store
        .read(MetaRead::DfsInode(version.inode_id))
        .await
        .unwrap()
        .entity
    else {
        panic!("missing source inode");
    };
    let request = RequestKey::new("fixture", "different-namespace");
    let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsSetInodeAttributes);
    txn.conditions.extend([
        TxnCondition::RequestAbsent(request.clone()),
        TxnCondition::EntityEquals(MetaEntity::DfsInode(inode.clone())),
    ]);
    inode.namespace_id = NamespaceId::new("other");
    txn.mutations.extend([
        TxnMutation::Put(MetaEntity::DfsInode(inode)),
        TxnMutation::RecordRequestOutcome(RequestOutcome {
            request,
            operation: StoreOperation::DfsSetInodeAttributes,
            result: OperationResult::Empty,
        }),
    ]);
    store.compare_and_commit(txn).await.unwrap();
    assert_eq!(
        service
            .commit_file_version("node-a".into(), commit)
            .await
            .unwrap_err()
            .code(),
        afs_error::META_CATALOG_INVALID_REQUEST
    );
}
