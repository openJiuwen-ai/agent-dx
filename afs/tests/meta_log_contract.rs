//! Local operation-log integration: domain operations, stable replay and watch.
//! Component boundary only; does not claim FUSE/RPC or physical power-loss coverage.
use afs::{
    dfs::{
        CallerContext, InodeAttributes, InodeId, NamespaceId, OperationId, RenameMode,
        RenameRequest, UnlinkRequest,
    },
    meta::{
        dfs::{CreateFileRequest, DfsService},
        store::{
            MetaRead, MetaStore, NodeSessionLease, RequestKey, Store, StoreRevision,
            journal::LogLimits, local_log::LocalLog,
        },
    },
};
use std::{path::Path, sync::Arc, time::Duration};

async fn open(path: &Path) -> Arc<dyn MetaStore> {
    // Only await the known channel-close/file-lock release after dropping Store.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match LocalLog::open(path, LogLimits::default()) {
                Ok(log) => {
                    break Arc::new(Store::open_log(Arc::new(log)).await.unwrap())
                        as Arc<dyn MetaStore>;
                }
                Err(error) if error.to_string().contains("Meta log already locked") => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => panic!("unexpected open error: {error}"),
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn p3_local_log_namespace_retry_watch_and_restart_are_equivalent() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path()).await;
    store
        .register_node_session(
            RequestKey::new("writer", "register"),
            NodeSessionLease {
                node_id: "writer".into(),
                session_id: "writer-session".into(),
                grpc_addr: "http://writer:7400".into(),
                data_addr: "http://writer:7500".into(),
                rest_addr: "http://writer:7600".into(),
                storage_devices: Vec::new(),
                lease_ttl: Duration::from_secs(60),
            },
        )
        .await
        .unwrap();
    let dfs = DfsService::new(store.clone());
    dfs.initialize_replication_config().await.unwrap();
    let namespace = NamespaceId::new("default");
    let root = InodeId::new("1");
    let request = CreateFileRequest {
        caller_id: "writer".into(),
        owner_session_id: "writer-session".into(),
        operation_id: OperationId::new("create"),
        namespace_id: namespace.clone(),
        parent_inode_id: root.clone(),
        name: b"first".to_vec(),
        attributes: InodeAttributes {
            mode: 0o644,
            uid: 0,
            gid: 0,
            nlink: 1,
            atime_unix_ms: 1,
            mtime_unix_ms: 1,
            ctime_unix_ms: 1,
        },
        lease_seconds: 60,
    };
    let created = dfs.create(request.clone()).await.unwrap();
    let old_view = store.read_view().await.unwrap();
    let caller = CallerContext {
        uid: 0,
        gid: 0,
        supplementary_gids: Vec::new(),
    };
    let rename = RenameRequest {
        caller_id: "writer".into(),
        operation_id: OperationId::new("rename"),
        namespace_id: namespace.clone(),
        old_parent_inode_id: root.clone(),
        old_name: b"first".to_vec(),
        new_parent_inode_id: root.clone(),
        new_name: b"second".to_vec(),
        mode: RenameMode::NoReplace,
        caller: caller.clone(),
    };
    let renamed = dfs.rename(rename.clone()).await.unwrap();
    assert!(
        dfs.lookup(namespace.clone(), root.clone(), b"first".to_vec())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        dfs.lookup(namespace.clone(), root.clone(), b"second".to_vec())
            .await
            .unwrap()
            .unwrap()
            .inode_id,
        created.0.inode_id
    );
    assert!(
        old_view
            .read(MetaRead::DfsDentry(afs::dfs::DentryKey {
                namespace_id: namespace.clone(),
                parent_inode_id: root.clone(),
                name: b"first".to_vec(),
            }))
            .await
            .unwrap()
            .entity
            .is_some(),
        "published changes must not mutate pinned old view"
    );
    drop(old_view);
    let watch = store.watch(StoreRevision::ZERO, usize::MAX).await.unwrap();
    watch.validate_order().unwrap();
    let id = std::fs::read(dir.path().join("IDENTITY")).unwrap();
    let wal = dir.path().join("wal-00000000000000000001.log");
    let before = std::fs::read(&wal).unwrap();
    drop(dfs);
    drop(store);

    let store = open(dir.path()).await;
    let dfs = DfsService::new(store.clone());
    dfs.initialize_replication_config().await.unwrap();
    assert_eq!(
        store.watch(StoreRevision::ZERO, usize::MAX).await.unwrap(),
        watch
    );
    assert_eq!(
        dfs.create(request).await.unwrap(),
        created,
        "retry returns the original inode/lease, not current renamed state"
    );
    assert_eq!(dfs.rename(rename).await.unwrap(), renamed);
    let entries = dfs.read_dir(namespace.clone(), root.clone()).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, b"second");
    assert_eq!(
        std::fs::read(&wal).unwrap(),
        before,
        "replay and retry must not append"
    );
    let unlink = UnlinkRequest {
        caller_id: "writer".into(),
        operation_id: OperationId::new("unlink"),
        namespace_id: namespace.clone(),
        parent_inode_id: root.clone(),
        name: b"second".to_vec(),
        caller,
    };
    let unlinked = dfs.unlink(unlink.clone()).await.unwrap();
    let after_unlink = std::fs::read(&wal).unwrap();
    let final_watch = store.watch(StoreRevision::ZERO, usize::MAX).await.unwrap();
    drop(dfs);
    drop(store);

    let store = open(dir.path()).await;
    let dfs = DfsService::new(store.clone());
    assert!(
        dfs.lookup(namespace, root, b"second".to_vec())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(dfs.unlink(unlink).await.unwrap(), unlinked);
    assert_eq!(
        store.watch(StoreRevision::ZERO, usize::MAX).await.unwrap(),
        final_watch
    );
    assert_eq!(std::fs::read(&wal).unwrap(), after_unlink);
    assert_eq!(std::fs::read(dir.path().join("IDENTITY")).unwrap(), id);
    assert!(!dir.path().join("snapshot").exists());
    assert!(!dir.path().join("wal").exists());
}
