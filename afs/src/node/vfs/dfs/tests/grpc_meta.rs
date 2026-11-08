use super::*;

#[cfg(feature = "dfs")]
struct CountingMetaLog {
    inner: Arc<dyn crate::meta::store::MetaLog>,
    commits: AtomicU64,
}

#[cfg(feature = "dfs")]
impl CountingMetaLog {
    fn new(inner: Arc<dyn crate::meta::store::MetaLog>) -> Self {
        Self {
            inner,
            commits: AtomicU64::new(0),
        }
    }

    fn commit_count(&self) -> u64 {
        self.commits.load(Ordering::SeqCst)
    }
}

#[cfg(feature = "dfs")]
impl crate::meta::store::MetaLog for CountingMetaLog {
    fn health(&self) -> crate::meta::store::MetaFuture<'_, ()> {
        self.inner.health()
    }
    fn persistence(&self) -> crate::meta::store::BackendPersistence {
        self.inner.persistence()
    }
    fn limits(&self) -> crate::meta::store::journal::AppendLimits {
        self.inner.limits()
    }
    fn checkpoint_limits(&self) -> crate::meta::store::journal::CheckpointLimits {
        self.inner.checkpoint_limits()
    }
    /// This wrapper does not forward checkpoints, so Store schedules none.
    fn supports_checkpoint(&self) -> bool {
        false
    }
    fn status(&self) -> crate::meta::store::MetaFuture<'_, crate::meta::store::journal::LogStatus> {
        self.inner.status()
    }
    fn can_append(
        &self,
        expected: crate::meta::store::journal::LogCursor,
        bytes: usize,
    ) -> crate::meta::store::MetaFuture<'_, bool> {
        self.inner.can_append(expected, bytes)
    }
    fn install_checkpoint(
        &self,
        _: crate::meta::store::journal::LogTip,
        _: Vec<u8>,
    ) -> crate::meta::store::MetaFuture<'_, ()> {
        Box::pin(async {
            Err(afs_error::Error::coded(
                afs_error::IO_UNAVAILABLE,
                "test wrapper does not install checkpoints",
            ))
        })
    }
    fn recover(
        &self,
    ) -> crate::meta::store::MetaFuture<'_, crate::meta::store::journal::RecoveredLog> {
        self.inner.recover()
    }
    fn append_and_sync(
        &self,
        expected: crate::meta::store::journal::LogCursor,
        records: Vec<crate::meta::store::log::Record>,
    ) -> crate::meta::store::MetaFuture<'_, crate::meta::store::journal::LogTip> {
        Box::pin(async move {
            let tip = self.inner.append_and_sync(expected, records).await?;
            self.commits.fetch_add(1, Ordering::SeqCst);
            Ok(tip)
        })
    }
}

#[cfg(feature = "dfs")]
#[derive(Clone)]
struct HoldCommitRepliesLayer {
    gate: Arc<HoldCommitRepliesGate>,
}

#[cfg(feature = "dfs")]
struct HoldCommitRepliesGate {
    first_delay: Duration,
    hits: AtomicUsize,
    first_done: AtomicBool,
    first_done_notify: tokio::sync::Notify,
    second_reached: AtomicBool,
    second_reached_notify: tokio::sync::Notify,
    release_second: AtomicBool,
    release_second_notify: tokio::sync::Notify,
}

#[cfg(feature = "dfs")]
impl HoldCommitRepliesLayer {
    fn new(first_delay: Duration) -> Self {
        Self {
            gate: Arc::new(HoldCommitRepliesGate {
                first_delay,
                hits: AtomicUsize::new(0),
                first_done: AtomicBool::new(false),
                first_done_notify: tokio::sync::Notify::new(),
                second_reached: AtomicBool::new(false),
                second_reached_notify: tokio::sync::Notify::new(),
                release_second: AtomicBool::new(false),
                release_second_notify: tokio::sync::Notify::new(),
            }),
        }
    }

    async fn wait_first_committed(&self, timeout: Duration) {
        let notified = self.gate.first_done_notify.notified();
        if self.gate.first_done.load(Ordering::SeqCst) {
            return;
        }
        tokio::time::timeout(timeout, notified)
            .await
            .expect("first CommitFileVersion did not reach post-handler delay");
    }

    async fn wait_second_reached(&self, timeout: Duration) {
        let notified = self.gate.second_reached_notify.notified();
        if self.gate.second_reached.load(Ordering::SeqCst) {
            return;
        }
        tokio::time::timeout(timeout, notified)
            .await
            .expect("second CommitFileVersion replay did not reach explicit response gate");
    }

    fn release_second(&self) {
        self.gate.release_second.store(true, Ordering::SeqCst);
        self.gate.release_second_notify.notify_waiters();
    }
}

#[cfg(feature = "dfs")]
impl<S> tower::Layer<S> for HoldCommitRepliesLayer {
    type Service = HoldCommitRepliesService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        HoldCommitRepliesService {
            inner,
            gate: self.gate.clone(),
        }
    }
}

#[cfg(feature = "dfs")]
#[derive(Clone)]
struct HoldCommitRepliesService<S> {
    inner: S,
    gate: Arc<HoldCommitRepliesGate>,
}

#[cfg(feature = "dfs")]
impl<S, B> tower::Service<tonic::codegen::http::Request<B>> for HoldCommitRepliesService<S>
where
    S: tower::Service<tonic::codegen::http::Request<B>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    S::Error: Send + 'static,
    B: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<
            dyn std::future::Future<Output = std::result::Result<Self::Response, Self::Error>>
                + Send,
        >,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: tonic::codegen::http::Request<B>) -> Self::Future {
        let path = request.uri().path().to_owned();
        let future = self.inner.call(request);
        let gate = self.gate.clone();
        Box::pin(async move {
            let response = future.await?;
            if path == "/afs.meta.v1.DfsMeta/CommitFileVersion" {
                match gate.hits.fetch_add(1, Ordering::SeqCst) {
                    0 => {
                        gate.first_done.store(true, Ordering::SeqCst);
                        gate.first_done_notify.notify_waiters();
                        tokio::time::sleep(gate.first_delay).await;
                    }
                    1 => {
                        gate.second_reached.store(true, Ordering::SeqCst);
                        gate.second_reached_notify.notify_waiters();
                        while !gate.release_second.load(Ordering::SeqCst) {
                            let notified = gate.release_second_notify.notified();
                            if gate.release_second.load(Ordering::SeqCst) {
                                break;
                            }
                            notified.await;
                        }
                    }
                    _ => {}
                }
            }
            Ok(response)
        })
    }
}

#[cfg(feature = "dfs")]
struct CapturingDfsMeta {
    inner: Arc<crate::node::rpc::meta::GrpcDfsMeta>,
    commits: Mutex<Vec<CommitFileVersion>>,
}

#[cfg(feature = "dfs")]
impl CapturingDfsMeta {
    fn new(inner: crate::node::rpc::meta::GrpcDfsMeta) -> Self {
        Self {
            inner: Arc::new(inner),
            commits: Mutex::new(Vec::new()),
        }
    }

    fn commits(&self) -> Vec<CommitFileVersion> {
        self.commits.lock().unwrap().clone()
    }
}

#[cfg(feature = "dfs")]
impl DfsMeta for CapturingDfsMeta {
    fn lookup(&self, parent: &InodeId, name: &[u8]) -> Result<Option<InodeRecord>> {
        DfsMeta::lookup(self.inner.as_ref(), parent, name)
    }

    fn create(
        &self,
        operation_id: &OperationId,
        parent: &InodeId,
        name: &[u8],
        attributes: InodeAttributes,
    ) -> Result<(InodeRecord, WriteLease)> {
        DfsMeta::create(self.inner.as_ref(), operation_id, parent, name, attributes)
    }

    fn get_inode(&self, inode_id: &InodeId) -> Result<InodeRecord> {
        DfsMeta::get_inode(self.inner.as_ref(), inode_id)
    }

    fn get_file_version(&self, version_id: &FileVersionId) -> Result<(FileVersion, LayoutRoot)> {
        DfsMeta::get_file_version(self.inner.as_ref(), version_id)
    }

    fn open_write(&self, inode_id: &InodeId) -> Result<(InodeRecord, WriteLease)> {
        DfsMeta::open_write(self.inner.as_ref(), inode_id)
    }

    fn resolve_lock_authority(&self, inode_id: &InodeId) -> Result<(InodeRecord, WriteLease)> {
        DfsMeta::resolve_lock_authority(self.inner.as_ref(), inode_id)
    }

    fn resolve_write_authority(&self, inode_id: &InodeId) -> Result<(InodeRecord, WriteLease)> {
        DfsMeta::resolve_write_authority(self.inner.as_ref(), inode_id)
    }

    fn renew_write_lease(&self, lease: WriteLease) -> Result<WriteLease> {
        DfsMeta::renew_write_lease(self.inner.as_ref(), lease)
    }

    fn sync_inode_metadata(&self, sync: SyncInodeMetadata) -> Result<InodeRecord> {
        DfsMeta::sync_inode_metadata(self.inner.as_ref(), sync)
    }

    fn commit_file_version(&self, commit: CommitFileVersion) -> Result<InodeRecord> {
        self.commits.lock().unwrap().push(commit.clone());
        DfsMeta::commit_file_version(self.inner.as_ref(), commit)
    }

    fn lookup_node_location(&self, node_id: &str) -> Result<Option<DfsNodeLocation>> {
        DfsMeta::lookup_node_location(self.inner.as_ref(), node_id)
    }

    fn current_node_session(&self, node_id: &str) -> Result<Option<String>> {
        DfsMeta::current_node_session(self.inner.as_ref(), node_id)
    }

    fn mkdir(
        &self,
        operation_id: &OperationId,
        parent: &InodeId,
        name: &[u8],
        attributes: InodeAttributes,
        caller: CallerContext,
    ) -> Result<InodeRecord> {
        DfsMeta::mkdir(
            self.inner.as_ref(),
            operation_id,
            parent,
            name,
            attributes,
            caller,
        )
    }

    fn read_dir_page(&self, parent: &InodeId, after: &[u8], limit: u32) -> Result<DirPage> {
        DfsMeta::read_dir_page(self.inner.as_ref(), parent, after, limit)
    }

    fn link(
        &self,
        operation_id: &OperationId,
        inode_id: &InodeId,
        expected_inode_revision: u64,
        parent: &InodeId,
        name: &[u8],
        caller: CallerContext,
    ) -> Result<InodeRecord> {
        DfsMeta::link(
            self.inner.as_ref(),
            operation_id,
            inode_id,
            expected_inode_revision,
            parent,
            name,
            caller,
        )
    }

    fn symlink(&self, request: SymlinkRequest) -> Result<InodeRecord> {
        DfsMeta::symlink(self.inner.as_ref(), request)
    }

    fn mknod(&self, request: MknodRequest) -> Result<InodeRecord> {
        DfsMeta::mknod(self.inner.as_ref(), request)
    }

    fn read_link(&self, request: ReadLinkRequest) -> Result<Vec<u8>> {
        DfsMeta::read_link(self.inner.as_ref(), request)
    }

    fn set_inode_attributes(&self, request: SetInodeAttrRequest) -> Result<InodeRecord> {
        DfsMeta::set_inode_attributes(self.inner.as_ref(), request)
    }

    fn get_xattr(&self, request: GetXattrRequest) -> Result<Vec<u8>> {
        DfsMeta::get_xattr(self.inner.as_ref(), request)
    }

    fn list_xattr(&self, request: ListXattrRequest) -> Result<Vec<Vec<u8>>> {
        DfsMeta::list_xattr(self.inner.as_ref(), request)
    }

    fn set_xattr(&self, request: SetXattrRequest) -> Result<InodeRecord> {
        DfsMeta::set_xattr(self.inner.as_ref(), request)
    }

    fn remove_xattr(&self, request: RemoveXattrRequest) -> Result<InodeRecord> {
        DfsMeta::remove_xattr(self.inner.as_ref(), request)
    }

    fn unlink(
        &self,
        operation_id: &OperationId,
        parent: &InodeId,
        name: &[u8],
        caller: CallerContext,
    ) -> Result<InodeRecord> {
        DfsMeta::unlink(self.inner.as_ref(), operation_id, parent, name, caller)
    }

    fn rmdir(
        &self,
        operation_id: &OperationId,
        parent: &InodeId,
        name: &[u8],
        caller: CallerContext,
    ) -> Result<InodeRecord> {
        DfsMeta::rmdir(self.inner.as_ref(), operation_id, parent, name, caller)
    }

    fn rename(
        &self,
        operation_id: &OperationId,
        old_parent: &InodeId,
        old_name: &[u8],
        new_parent: &InodeId,
        new_name: &[u8],
        mode: RenameMode,
        caller: CallerContext,
    ) -> Result<RenameOutcome> {
        DfsMeta::rename(
            self.inner.as_ref(),
            operation_id,
            old_parent,
            old_name,
            new_parent,
            new_name,
            mode,
            caller,
        )
    }
}

#[cfg(feature = "dfs")]
async fn grpc_dfs_test_fs(
    node_id: &str,
    session_id: &str,
    meta_endpoint: &str,
    local: Arc<LocalChunkStore>,
    timeout: Duration,
) -> (
    Arc<CapturingDfsMeta>,
    Arc<CountingChunkStore>,
    DistributedFs,
) {
    let meta = Arc::new(CapturingDfsMeta::new(
        crate::node::rpc::meta::GrpcDfsMeta::new(
            meta_endpoint,
            node_id.to_owned(),
            session_id.to_owned(),
            NamespaceId::new("default"),
            timeout,
            afs_transport::TlsConfig::Disabled,
        )
        .unwrap(),
    ));
    let peers = Arc::new(
        crate::node::rpc::peer::PeerConnectionPool::new(
            afs_transport::GrpcConfig::default(),
            afs_transport::TlsConfig::Disabled,
            4,
        )
        .unwrap(),
    );
    let data_plane =
        Arc::new(crate::node::rpc::peer::GrpcReplicaDataPlane::new(peers, timeout).unwrap());
    // Use the product R1 path and Meta placement authority. The test-only
    // LocalChunkStore trait adapter fabricates a group for mock Meta tests.
    let chunks = Arc::new(CountingChunkStore::new(Arc::new(
        crate::node::replication::DfsChunkSink::new_with_epoch(
            node_id.to_owned(),
            1,
            local.clone(),
            meta.inner.clone(),
            data_plane,
        ),
    )));
    let read_engine = Arc::new(crate::node::read::engine::DfsReadEngine::new(
        NamespaceId::new("default"),
        node_id.to_owned(),
        local,
        Arc::new(crate::node::read::engine::UnimplementedReadSourceProvider),
        Arc::new(crate::node::read::engine::UnimplementedChunkTransfer),
        crate::node::read::engine::DfsReadConfig::default(),
    ));
    (
        meta.clone(),
        chunks.clone(),
        test_dfs_new(
            NamespaceId::new("default"),
            node_id,
            session_id,
            meta,
            chunks,
            read_engine,
        ),
    )
}

#[cfg(feature = "dfs")]
async fn real_grpc_meta_fixture() -> (
    tempfile::TempDir,
    String,
    HoldCommitRepliesLayer,
    tokio::task::JoinHandle<()>,
    Arc<dyn crate::meta::store::MetaStore>,
    Arc<LocalChunkStore>,
    Arc<CountingMetaLog>,
) {
    use crate::meta::{
        Meta,
        store::{MetaRead, NodeSessionLease, RequestKey, Store, journal::MemoryLog},
    };
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    let temp = tempfile::tempdir().unwrap();
    let local_a = Arc::new(LocalChunkStore::open(temp.path().join("node-a"), "node-a").unwrap());
    let backend = Arc::new(CountingMetaLog::new(Arc::new(
        MemoryLog::new(Default::default()).unwrap(),
    )));
    let store: Arc<dyn crate::meta::store::MetaStore> =
        Arc::new(Store::open_log(backend.clone()).await.unwrap());
    store
        .register_node_session(
            RequestKey::new("node-a", "register-node-a"),
            NodeSessionLease {
                node_id: "node-a".into(),
                session_id: "session-a".into(),
                grpc_addr: "http://127.0.0.1:1".into(),
                data_addr: "http://127.0.0.1:1".into(),
                rest_addr: "http://127.0.0.1:1".into(),
                ownerfs_enabled: false,
                storage_devices: vec![local_a.device_descriptor().unwrap()],
                lease_ttl: Duration::from_secs(30),
            },
        )
        .await
        .unwrap();
    let meta = Meta::with_store(
        "meta-unknown-file-ack".into(),
        crate::runtime::Observability::new().unwrap(),
        store.clone(),
    );
    meta.dfs
        .as_ref()
        .unwrap()
        .initialize_replication_config()
        .await
        .unwrap();
    assert!(
        store
            .read(MetaRead::DfsReplicationConfig)
            .await
            .unwrap()
            .entity
            .is_some()
    );
    let meta = Arc::new(meta);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let meta_endpoint = format!("http://{}", listener.local_addr().unwrap());
    let gate = HoldCommitRepliesLayer::new(Duration::from_secs(3));
    let server_gate = gate.clone();
    let server = tokio::spawn(async move {
        Server::builder()
            .layer(server_gate)
            .add_service(afs_protocol::meta::dfs_meta_server::DfsMetaServer::new(
                crate::meta::rpc::DfsMetaRpc(meta),
            ))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (temp, meta_endpoint, gate, server, store, local_a, backend)
}

#[cfg(feature = "dfs")]
async fn pr93_resize_prepare_retry(new_length: usize) {
    use crate::meta::store::{MetaEntity, MetaRead};
    let (_temp, endpoint, gate, server, store, local, _backend) = real_grpc_meta_fixture().await;
    // This case injects a preparation failure, not a lost commit reply.
    gate.gate.hits.store(2, Ordering::SeqCst);
    let (meta, chunks, raw) = grpc_dfs_test_fs(
        "node-a",
        "session-a",
        &endpoint,
        local,
        Duration::from_secs(2),
    )
    .await;
    let references = crate::node::gc::GcReferences::new(meta.inner.clone());
    let fs = Arc::new(raw.with_gc_references(Some(references.clone())));
    let file = {
        let fs = fs.clone();
        run_blocking(move || {
            let file = fs
                .create(
                    &context(),
                    fs.root_inode(),
                    OsStr::new("pr93-resize"),
                    0o640,
                    libc::O_RDWR,
                )
                .unwrap();
            fs.write(&context(), file.handle, 0, &vec![11; 65549])
                .unwrap();
            fs.fsync(&context(), file.handle, SyncMode::Full).unwrap();
            file
        })
        .await
    };
    let handle = file.handle;
    let inode = file.entry.inode;
    let inode_id = fs.inode_id(inode).unwrap();
    let before = meta.commits();
    assert_eq!(before.len(), 1);
    assert!(
        before[0]
            .chunk_receipts
            .iter()
            .all(|receipt| receipt.chunk.is_segment_crc32c())
    );
    {
        let fs = fs.clone();
        let chunks = chunks.clone();
        run_blocking(move || {
            fs.setattr(
                &context(),
                inode,
                Some(handle),
                &AttributeChange {
                    size: Some(new_length as u64),
                    ..AttributeChange::default()
                },
            )
            .unwrap();
            chunks.fail_empty_prepare.store(true, Ordering::SeqCst);
            let error = fs.fsync(&context(), handle, SyncMode::Full).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("injected PR93 preparation failure")
            );
            assert!(!chunks.fail_empty_prepare.load(Ordering::SeqCst));
        })
        .await;
    }
    assert_eq!(
        meta.commits().len(),
        1,
        "failed preparation must not publish"
    );
    let (_, entities) = store.read_view().await.unwrap().snapshot().unwrap();
    let active: Vec<_> = entities
        .into_iter()
        .filter_map(|entity| match entity {
            MetaEntity::DfsReference(reference)
                if reference.inode_id.as_ref() == Some(&inode_id)
                    && reference.publication
                    && reference.retired_at_unix_ms.is_none()
                    && reference.reference_id.starts_with("write:") =>
            {
                Some(reference)
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        active.len(),
        1,
        "real Meta must have admitted one pending generation"
    );
    let admitted = active[0].clone();
    assert!(
        admitted.chunks.is_empty(),
        "per-chunk registration stays absent"
    );
    assert_eq!(
        store
            .read(MetaRead::DfsInode(inode_id.clone()))
            .await
            .unwrap()
            .entity
            .and_then(|entity| match entity {
                MetaEntity::DfsInode(inode) => inode.head_version,
                _ => None,
            }),
        Some(before[0].file_version.id.clone())
    );
    {
        let fs = fs.clone();
        run_blocking(move || fs.fsync(&context(), handle, SyncMode::Full))
            .await
            .unwrap();
    }
    let commits = meta.commits();
    assert_eq!(commits.len(), 2);
    let committed = &commits[1];
    assert_eq!(
        admitted.reference_id,
        format!(
            "write:{}",
            ChunkObject::generation_id(&committed.operation_id)
        ),
        "retry must use the identity admitted before the preparation failure"
    );
    assert_eq!(
        committed.gc_fence,
        Some(crate::dfs::GcFence::WriteGeneration)
    );
    let (_, entities) = store.read_view().await.unwrap().snapshot().unwrap();
    let references_after: Vec<_> = entities
        .into_iter()
        .filter_map(|entity| match entity {
            MetaEntity::DfsReference(reference)
                if reference.inode_id.as_ref() == Some(&inode_id)
                    && reference.reference_id.starts_with("write:") =>
            {
                Some(reference)
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        references_after.len(),
        2,
        "retry must not obtain a replacement generation"
    );
    assert!(
        references_after
            .iter()
            .any(|reference| reference.reference_id == admitted.reference_id
                && reference.retired_at_unix_ms.is_some())
    );
    let version = store
        .read(MetaRead::DfsFileVersion(committed.file_version.id.clone()))
        .await
        .unwrap();
    assert!(
        matches!(version.entity, Some(MetaEntity::DfsFileVersion(version)) if version.length == new_length as u64)
    );
    {
        let fs = fs.clone();
        run_blocking(move || {
            let mut got = vec![0; 80000];
            assert_eq!(
                fs.read(&context(), handle, 0, &mut got).unwrap(),
                new_length
            );
            let mut expected = vec![11; 65549];
            expected.resize(new_length, 0);
            assert_eq!(&got[..new_length], expected.as_slice());
            fs.release(&context(), handle).unwrap();
            references.flush().unwrap();
        })
        .await;
    }
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}

#[cfg(feature = "dfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pr93_fix_truncate_prepare_retry_uses_real_meta_generation() {
    pr93_resize_prepare_retry(32775).await;
}

#[cfg(feature = "dfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pr93_fix_extend_prepare_retry_uses_real_meta_generation() {
    pr93_resize_prepare_retry(70001).await;
}

#[cfg(feature = "dfs")]
struct ReleaseCommitGateOnDrop(HoldCommitRepliesLayer);

#[cfg(feature = "dfs")]
impl Drop for ReleaseCommitGateOnDrop {
    fn drop(&mut self) {
        self.0.release_second();
    }
}

#[cfg(feature = "dfs")]
#[derive(Clone, Debug, Eq, PartialEq)]
struct InodeMutationSnapshot {
    logical_length: u64,
    dirty: bool,
    dirty_extent_count: usize,
    visible_write_seq: u64,
    durable_write_seq: u64,
    committed_write_seq: u64,
    pending_operation_id: OperationId,
    pending_version_id: FileVersionId,
    pending_layout_id: LayoutRootId,
}

#[cfg(feature = "dfs")]
fn pending_file_commit_snapshot(
    fs: &DistributedFs,
    inode_id: &InodeId,
) -> (PendingFileCommit, InodeMutationSnapshot) {
    let cell = fs.write_state(inode_id).unwrap().unwrap();
    let state = cell.lock().unwrap();
    let pending = match state.in_flight.as_ref().unwrap() {
        InFlightCommit::File(pending) => pending.as_ref().clone(),
        _ => panic!("expected pending file commit"),
    };
    let snapshot = InodeMutationSnapshot {
        logical_length: state.logical_length,
        dirty: state.dirty,
        dirty_extent_count: state.dirty_extents.iter_extents().count(),
        visible_write_seq: state.visible_write_seq,
        durable_write_seq: state.durable_write_seq,
        committed_write_seq: state.committed_write_seq,
        pending_operation_id: pending.batch.commit.operation_id.clone(),
        pending_version_id: pending.batch.commit.file_version.id.clone(),
        pending_layout_id: pending.batch.commit.layout_root.id.clone(),
    };
    (pending, snapshot)
}

#[cfg(feature = "dfs")]
fn read_exact_from(fs: &DistributedFs, handle: FileHandle, offset: u64, len: usize) -> Vec<u8> {
    let mut bytes = vec![0; len];
    let read = fs.read(&context(), handle, offset, &mut bytes).unwrap();
    bytes.truncate(read);
    bytes
}

#[cfg(feature = "dfs")]
async fn run_blocking<T: Send + 'static>(action: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::time::timeout(Duration::from_secs(5), tokio::task::spawn_blocking(action))
        .await
        .expect("blocking DFS test operation timed out")
        .unwrap()
}

#[cfg(feature = "dfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_grpc_lock_resolver_preserves_locks_without_meta_commits() {
    use crate::meta::store::{MetaEntity, MetaRead};
    let (_temp, endpoint, _gate, server, store, local, _backend) = real_grpc_meta_fixture().await;
    let (_meta, _chunks, fs) = grpc_dfs_test_fs(
        "node-a",
        "session-a",
        &endpoint,
        local,
        Duration::from_secs(2),
    )
    .await;
    let fs = Arc::new(fs);
    let creating = fs.clone();
    let created = run_blocking(move || {
        creating.create(
            &context(),
            creating.root_inode(),
            OsStr::new("grpc-lock-resolver.bin"),
            0o640,
            libc::O_RDWR,
        )
    })
    .await
    .unwrap();
    let inode_id = fs.inode_id(created.entry.inode).unwrap();
    let before = store
        .read(MetaRead::DfsWriteLease(inode_id.clone()))
        .await
        .unwrap();
    let locking = fs.clone();
    run_blocking(move || {
        locking.setlk(
            &context(),
            created.entry.inode,
            created.handle,
            LockRequest::write(
                FileLockKind::Posix,
                lock_owner("mount-a", 71),
                71,
                lock_range(0, 99),
            ),
            None,
        )?;
        for _ in 0..32 {
            let conflict = locking.getlk(
                &context(),
                created.entry.inode,
                created.handle,
                LockRequest::write(
                    FileLockKind::Posix,
                    lock_owner("mount-b", 72),
                    72,
                    lock_range(0, 99),
                ),
            )?;
            assert!(conflict.is_some());
        }
        locking.release_locks(
            &context(),
            created.entry.inode,
            created.handle,
            lock_owner("mount-a", 71),
            ReleaseKind::PosixOwner,
        )?;
        assert!(
            locking
                .getlk(
                    &context(),
                    created.entry.inode,
                    created.handle,
                    LockRequest::write(
                        FileLockKind::Posix,
                        lock_owner("mount-b", 72),
                        72,
                        lock_range(0, 99)
                    )
                )?
                .is_none()
        );
        Ok::<(), Error>(())
    })
    .await
    .unwrap();
    let after = store.read(MetaRead::DfsWriteLease(inode_id)).await.unwrap();
    assert_eq!(
        after.revision, before.revision,
        "actual Node/Meta RPC lock path must not commit"
    );
    assert_eq!(after.entity, before.entity);
    assert!(matches!(after.entity, Some(MetaEntity::DfsWriteLease(_))));
    server.abort();
}

#[cfg(feature = "dfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_grpc_writable_reopen_resolves_write_authority_without_meta_mutation() {
    use crate::meta::store::{MetaEntity, MetaRead, RequestKey};
    let (_temp, endpoint, _gate, server, store, local, backend) = real_grpc_meta_fixture().await;
    let (_meta, _chunks, fs_raw) = grpc_dfs_test_fs(
        "node-a",
        "session-a",
        &endpoint,
        local,
        Duration::from_secs(2),
    )
    .await;
    let fs = Arc::new(fs_raw);
    let created = {
        let fs = fs.clone();
        run_blocking(move || {
            fs.create(
                &context(),
                fs.root_inode(),
                OsStr::new("grpc-write-authority-reopen.bin"),
                0o640,
                libc::O_RDWR,
            )
        })
        .await
        .unwrap()
    };
    let inode = created.entry.inode;
    let inode_id = fs.inode_id(inode).unwrap();
    {
        let fs = fs.clone();
        run_blocking(move || fs.release(&context(), created.handle))
            .await
            .unwrap();
    }
    let before = store
        .read(MetaRead::DfsWriteLease(inode_id.clone()))
        .await
        .unwrap();
    let before_commits = backend.commit_count();
    for _ in 0..4 {
        let opening = fs.clone();
        let handle = run_blocking(move || opening.open(&context(), inode, libc::O_RDWR))
            .await
            .unwrap();
        let releasing = fs.clone();
        run_blocking(move || releasing.release(&context(), handle))
            .await
            .unwrap();
    }
    let after = store.read(MetaRead::DfsWriteLease(inode_id)).await.unwrap();
    assert_eq!(after.revision, before.revision);
    assert_eq!(after.entity, before.entity);
    assert!(matches!(after.entity, Some(MetaEntity::DfsWriteLease(_))));
    assert_eq!(
        backend.commit_count(),
        before_commits,
        "production GrpcDfsMeta writable reopen must be a fresh read, not CAS/outcome mutation"
    );
    for request_id in [
        "session-a-dfs-2",
        "session-a-dfs-3",
        "session-a-dfs-4",
        "session-a-dfs-5",
    ] {
        assert!(
            store
                .read(MetaRead::RequestOutcome(RequestKey::new(
                    "node-a", request_id
                )))
                .await
                .unwrap()
                .request_outcome
                .is_none(),
            "live write authority request {request_id} must not persist an outcome"
        );
    }
    server.abort();
}

#[cfg(feature = "dfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_grpc_write_authority_foreign_owner_does_not_take_over_or_mutate() {
    use crate::meta::store::{MetaEntity, MetaRead, RequestKey};
    let (_temp, endpoint, _gate, server, store, local, backend) = real_grpc_meta_fixture().await;
    let (_meta_a, _chunks_a, fs_a_raw) = grpc_dfs_test_fs(
        "node-a",
        "session-a",
        &endpoint,
        local,
        Duration::from_secs(2),
    )
    .await;
    let fs_a = Arc::new(fs_a_raw);
    let created = {
        let fs = fs_a.clone();
        run_blocking(move || {
            fs.create(
                &context(),
                fs.root_inode(),
                OsStr::new("grpc-write-authority-foreign.bin"),
                0o640,
                libc::O_RDWR,
            )
        })
        .await
        .unwrap()
    };
    let inode_id = fs_a.inode_id(created.entry.inode).unwrap();
    let before = store
        .read(MetaRead::DfsWriteLease(inode_id.clone()))
        .await
        .unwrap();
    let before_commits = backend.commit_count();
    let (inode, lease) = {
        let endpoint = endpoint.clone();
        let inode_id = inode_id.clone();
        run_blocking(move || {
            let foreign = crate::node::rpc::meta::GrpcDfsMeta::new(
                &endpoint,
                "node-b".into(),
                "session-b".into(),
                NamespaceId::new("default"),
                Duration::from_secs(2),
                afs_transport::TlsConfig::Disabled,
            )
            .unwrap();
            DfsMeta::resolve_write_authority(&foreign, &inode_id)
        })
        .await
        .unwrap()
    };

    assert_eq!(inode.inode_id, inode_id);
    assert_eq!(lease.owner_node_id, "node-a");
    assert_eq!(lease.owner_session_id, "session-a");
    assert_eq!(
        backend.commit_count(),
        before_commits,
        "foreign live write authority resolution must not acquire or mutate Meta"
    );
    let after = store.read(MetaRead::DfsWriteLease(inode_id)).await.unwrap();
    assert_eq!(after.revision, before.revision);
    assert_eq!(after.entity, before.entity);
    assert!(matches!(after.entity, Some(MetaEntity::DfsWriteLease(_))));
    assert!(
        store
            .read(MetaRead::RequestOutcome(RequestKey::new(
                "node-b",
                "session-b-dfs-1",
            )))
            .await
            .unwrap()
            .request_outcome
            .is_none(),
        "foreign live write authority read must not persist an outcome"
    );
    server.abort();
}

#[cfg(feature = "dfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_grpc_unknown_file_commit_ack_blocks_inode_and_replays_exact_request() {
    use crate::meta::store::{MetaEntity, MetaRead, OperationResult, RequestKey, StoreOperation};

    let (_temp, meta_endpoint, gate, server, store, local_a, _backend) =
        real_grpc_meta_fixture().await;
    let (meta_a, chunks_a, fs_a_raw) = grpc_dfs_test_fs(
        "node-a",
        "session-a",
        &meta_endpoint,
        local_a,
        Duration::from_secs(2),
    )
    .await;
    let fs_a = Arc::new(fs_a_raw);

    let created_a = {
        let fs = fs_a.clone();
        run_blocking(move || {
            fs.create(
                &context(),
                fs.root_inode(),
                OsStr::new("unknown-file-ack-a.bin"),
                0o640,
                libc::O_RDWR,
            )
        })
        .await
        .unwrap()
    };
    let inode_backend_a = created_a.entry.inode;
    let handle_a = created_a.handle;
    let inode_a = fs_a.inode_id(inode_backend_a).unwrap();
    {
        let fs = fs_a.clone();
        run_blocking(move || fs.write(&context(), handle_a, 0, b"alpha"))
            .await
            .unwrap();
    }

    let created_b = {
        let fs = fs_a.clone();
        run_blocking(move || {
            fs.create(
                &context(),
                fs.root_inode(),
                OsStr::new("unknown-file-ack-b.bin"),
                0o640,
                libc::O_RDWR,
            )
        })
        .await
        .unwrap()
    };
    let handle_b = created_b.handle;

    let first_error = {
        let fs = fs_a.clone();
        run_blocking(move || fs.fsync(&context(), handle_a, SyncMode::Full))
            .await
            .unwrap_err()
    };
    assert_eq!(
        first_error.code(),
        afs_error::CLIENT_DEADLINE_EXCEEDED,
        "unexpected first commit error: {first_error:?}"
    );
    gate.wait_first_committed(Duration::from_secs(1)).await;

    let (pending, blocked_before_replay) = pending_file_commit_snapshot(&fs_a, &inode_a);
    assert_eq!(chunks_a.put_batch_count(), 1);
    assert_eq!(pending.frozen.through_seq, 1);
    assert_eq!(pending.frozen.logical_length, 5);
    assert_eq!(pending.batch.through_seq, 1);
    assert_eq!(pending.batch.commit.layout_root.file_length, 5);
    assert_eq!(pending.batch.commit.file_version.length, 5);
    assert_eq!(pending.batch.commit.chunk_receipts.len(), 1);

    let stored_inode = store
        .read(MetaRead::DfsInode(inode_a.clone()))
        .await
        .unwrap();
    let stored_inode = match stored_inode.entity.unwrap() {
        MetaEntity::DfsInode(inode) => inode,
        _ => panic!("expected stored inode"),
    };
    assert_eq!(
        stored_inode.head_version,
        Some(pending.batch.commit.file_version.id.clone())
    );
    let stored_version = store
        .read(MetaRead::DfsFileVersion(
            pending.batch.commit.file_version.id.clone(),
        ))
        .await
        .unwrap();
    let stored_version = match stored_version.entity.unwrap() {
        MetaEntity::DfsFileVersion(version) => version,
        _ => panic!("expected stored file version"),
    };
    assert_eq!(stored_version, pending.batch.commit.file_version);
    let stored_layout = store
        .read(MetaRead::DfsLayoutRoot(
            pending.batch.commit.layout_root.id.clone(),
        ))
        .await
        .unwrap();
    let stored_layout = match stored_layout.entity.unwrap() {
        MetaEntity::DfsLayoutRoot(layout) => layout,
        _ => panic!("expected stored layout root"),
    };
    assert_eq!(stored_layout, pending.batch.commit.layout_root);
    let outcome = store
        .read(MetaRead::RequestOutcome(RequestKey::new(
            "node-a",
            pending.batch.commit.operation_id.0.clone(),
        )))
        .await
        .unwrap()
        .request_outcome
        .expect("commit outcome must be durable before reply release");
    assert_eq!(outcome.operation, StoreOperation::DfsCommitFileVersion);
    match outcome.result.clone() {
        OperationResult::DfsNamespace { result, .. }
            if matches!(
                result.as_ref(),
                OperationResult::DfsInode(inode)
                    if inode.head_version == Some(pending.batch.commit.file_version.id.clone())
            ) => {}
        other => panic!("unexpected commit outcome: {other:?}"),
    }
    let replay = {
        let fs = fs_a.clone();
        tokio::task::spawn_blocking(move || fs.fsync(&context(), handle_a, SyncMode::Full))
    };
    gate.wait_second_reached(Duration::from_secs(1)).await;
    let release_gate_on_drop = ReleaseCommitGateOnDrop(gate.clone());

    let replayed_inode = store
        .read(MetaRead::DfsInode(inode_a.clone()))
        .await
        .unwrap();
    assert_eq!(
        replayed_inode.entity,
        Some(MetaEntity::DfsInode(stored_inode))
    );
    let replayed_version = store
        .read(MetaRead::DfsFileVersion(
            pending.batch.commit.file_version.id.clone(),
        ))
        .await
        .unwrap();
    assert_eq!(
        replayed_version.entity,
        Some(MetaEntity::DfsFileVersion(stored_version))
    );
    let replayed_layout = store
        .read(MetaRead::DfsLayoutRoot(
            pending.batch.commit.layout_root.id.clone(),
        ))
        .await
        .unwrap();
    assert_eq!(
        replayed_layout.entity,
        Some(MetaEntity::DfsLayoutRoot(stored_layout))
    );
    let replayed_outcome = store
        .read(MetaRead::RequestOutcome(RequestKey::new(
            "node-a",
            pending.batch.commit.operation_id.0.clone(),
        )))
        .await
        .unwrap();
    assert_eq!(replayed_outcome.request_outcome, Some(outcome));

    let mut blocked_write = {
        let fs = fs_a.clone();
        tokio::task::spawn_blocking(move || fs.write(&context(), handle_a, 5, b"!"))
    };
    let mut blocked_resize = {
        let fs = fs_a.clone();
        tokio::task::spawn_blocking(move || {
            fs.setattr(
                &context(),
                inode_backend_a,
                Some(handle_a),
                &AttributeChange {
                    size: Some(7),
                    ..AttributeChange::default()
                },
            )
        })
    };
    let mut blocked_sync = {
        let fs = fs_a.clone();
        tokio::task::spawn_blocking(move || fs.fsync(&context(), handle_a, SyncMode::Full))
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut blocked_write)
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut blocked_resize)
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut blocked_sync)
            .await
            .is_err()
    );
    let (still_pending, blocked_after_wait) = pending_file_commit_snapshot(&fs_a, &inode_a);
    assert_eq!(still_pending.batch.commit, pending.batch.commit);
    assert_eq!(blocked_after_wait, blocked_before_replay);
    assert_eq!(chunks_a.put_batch_count(), 1);
    let captured_commits = meta_a.commits();
    assert_eq!(captured_commits.len(), 2);
    assert_eq!(&captured_commits[0], pending.batch.commit.as_ref());
    assert_eq!(&captured_commits[1], pending.batch.commit.as_ref());

    {
        let fs = fs_a.clone();
        run_blocking(move || fs.write(&context(), handle_b, 0, b"bravo"))
            .await
            .unwrap();
    }
    {
        let fs = fs_a.clone();
        run_blocking(move || fs.fsync(&context(), handle_b, SyncMode::Full))
            .await
            .unwrap();
    }
    assert_eq!(chunks_a.put_batch_count(), 2);
    let b_bytes = {
        let fs = fs_a.clone();
        run_blocking(move || read_exact_from(&fs, handle_b, 0, 8)).await
    };
    assert_eq!(b_bytes, b"bravo".to_vec());
    let a_bytes = {
        let fs = fs_a.clone();
        run_blocking(move || read_exact_from(&fs, handle_a, 0, 8)).await
    };
    assert_eq!(a_bytes, b"alpha".to_vec());

    gate.release_second();
    drop(release_gate_on_drop);
    tokio::time::timeout(Duration::from_secs(2), replay)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), blocked_write)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        1
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), blocked_resize)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .size,
        7
    );
    tokio::time::timeout(Duration::from_secs(2), blocked_sync)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    {
        let fs = fs_a.clone();
        run_blocking(move || fs.fsync(&context(), handle_a, SyncMode::Full))
            .await
            .unwrap();
    }
    {
        let fs = fs_a.clone();
        run_blocking(move || fs.flush(&context(), handle_a))
            .await
            .unwrap();
    }
    {
        let fs = fs_a.clone();
        run_blocking(move || fs.release(&context(), handle_a))
            .await
            .unwrap();
    }

    let reopened = {
        let fs = fs_a.clone();
        run_blocking(move || fs.open(&context(), inode_backend_a, libc::O_RDONLY))
            .await
            .unwrap()
    };
    let final_read = {
        let fs = fs_a.clone();
        run_blocking(move || {
            let mut final_bytes = vec![0; 8];
            let read = fs.read(&context(), reopened, 0, &mut final_bytes).unwrap();
            final_bytes.truncate(read);
            let eof = fs
                .read(&context(), reopened, read as u64, &mut [0; 8])
                .unwrap();
            fs.release(&context(), reopened).unwrap();
            (final_bytes, eof)
        })
        .await
    };
    assert_eq!(final_read.0, b"alpha!\0".to_vec());
    assert_eq!(final_read.1, 0);
    {
        let fs = fs_a.clone();
        run_blocking(move || fs.release(&context(), handle_b))
            .await
            .unwrap();
    }
    server.abort();
}
