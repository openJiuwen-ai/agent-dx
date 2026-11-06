//! R05 end to end, in process: real Meta and gRPC, real `DistributedFs` and
//! `S3ChunkStore`, an in-memory object store and one real `Collector`. These
//! prove the object lifecycle ("nothing left behind, nothing live deleted");
//! MinIO behavior is the VM e2e scenario's job.

use std::{ffi::OsStr, sync::Arc, time::Duration};

use afs_protocol::meta::{dfs_meta_server::DfsMetaServer, meta_server::MetaServer};
use afs_transport::grpc::TlsConfig;
use object_store::ObjectStore;

use super::*;
use crate::{
    dfs::{DfsDurabilityPolicy, NamespaceId, ReplicationConfig},
    meta::{
        Meta,
        rpc::{DfsMetaRpc, MetaRpc},
        store::{MetaEntity, Store, local_log::LocalLog},
    },
    node::{
        chunk::LocalChunkStore,
        gc::{Collector, GcReferences},
        read::engine::{DfsReadConfig, DfsReadEngine, UnimplementedChunkTransfer},
        rpc::meta::{GrpcDfsMeta, register_node_with_readiness},
        vfs::{Backend, dfs::DistributedFs, types},
    },
    runtime::Observability,
};

const CHUNK: usize = 4 * 1024 * 1024;

fn context() -> types::RequestContext {
    types::RequestContext {
        uid: 0,
        gid: 0,
        pid: std::process::id(),
        umask: 0o022,
        supplementary_gids: Vec::new(),
    }
}

async fn on_plain_thread<T, F>(work: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let (reply, answer) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let _ = reply.send(work());
    });
    answer.await.expect("plain thread reply")
}

struct Cluster {
    _temp: tempfile::TempDir,
    store: Arc<Store>,
    backend: Arc<object_store::memory::InMemory>,
    s3: Arc<S3ChunkStore>,
    fs: Arc<DistributedFs>,
    collector: Arc<Collector>,
    _server: tokio::task::JoinHandle<()>,
}

impl Cluster {
    async fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let session_id = format!("s3-gc-{}", uuid::Uuid::new_v4().simple());
        let store = Arc::new(
            Store::open_log(Arc::new(
                LocalLog::open(temp.path().join("meta"), Default::default()).unwrap(),
            ))
            .await
            .unwrap(),
        );
        let mut meta = Meta::with_store(
            "s3-gc".to_owned(),
            Observability::new().unwrap(),
            store.clone(),
        );
        let service = crate::meta::dfs::DfsService::with_durability_policy(
            store.clone(),
            ReplicationConfig::local_single_copy(),
            DfsDurabilityPolicy::ExternalBackend {
                store_id: "filesystem-a".into(),
                generation: "generation-1".into(),
            },
        )
        .with_gc_grace_ms(0);
        service.initialize_replication_config().await.unwrap();
        meta.dfs = Some(service);
        let meta = Arc::new(meta);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(MetaServer::new(MetaRpc(meta.clone())))
                .add_service(DfsMetaServer::new(DfsMetaRpc(meta)))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let namespace = NamespaceId::new("default");
        let endpoint = format!("http://{address}");
        let meta_client = Arc::new(
            GrpcDfsMeta::new(
                &endpoint,
                "node-a".to_owned(),
                session_id.clone(),
                namespace.clone(),
                Duration::from_secs(10),
                TlsConfig::Disabled,
            )
            .unwrap(),
        );
        let chunks = Arc::new(LocalChunkStore::open(temp.path().join("chunks"), "node-a").unwrap());
        let device = chunks.device_descriptor().unwrap();
        register_node_with_readiness(
            &endpoint,
            afs_protocol::meta::NodeDescriptor {
                node_id: "node-a".to_owned(),
                endpoint: Some(afs_protocol::meta::NodeEndpoint {
                    grpc_addr: "http://127.0.0.1:1".to_owned(),
                    data_addr: "http://127.0.0.1:1".to_owned(),
                    rest_addr: "http://127.0.0.1:1".to_owned(),
                }),
                labels: std::collections::HashMap::new(),
                capabilities: vec!["dfs".to_owned()],
                session_id: session_id.clone(),
                storage_devices: vec![afs_protocol::meta::DfsStorageDevice {
                    device_id: device.device_id,
                    device_epoch: device.device_epoch,
                    catalog_revision: device.catalog_revision,
                    failure_domain: device.failure_domain,
                }],
            },
            Duration::from_secs(10),
            TlsConfig::Disabled,
        )
        .await
        .unwrap();

        let backend = Arc::new(object_store::memory::InMemory::new());
        let s3 = Arc::new(
            S3ChunkStore::with_backend(
                S3ChunkStoreOptions {
                    endpoint: "http://127.0.0.1:1".into(),
                    bucket: "chunks".into(),
                    region: "us-east-1".into(),
                    store_id: "filesystem-a".into(),
                    generation: "generation-1".into(),
                    allow_http: true,
                    unsigned_payload: false,
                    credentials: S3CredentialsSource::Environment,
                    request_timeout: Duration::from_secs(10),
                    max_retries: 0,
                    cache: None,
                    metrics: None,
                },
                backend.clone(),
            )
            .unwrap(),
        );
        let read_engine = Arc::new(
            DfsReadEngine::new(
                namespace.clone(),
                "node-a".to_owned(),
                chunks,
                meta_client.clone(),
                Arc::new(UnimplementedChunkTransfer),
                DfsReadConfig::default(),
            )
            .with_external_backend(s3.clone(), meta_client.clone()),
        );
        let references = GcReferences::new(meta_client.clone());
        let collector = Arc::new(Collector::new_external(
            references.clone(),
            s3.clone(),
            read_engine.clone(),
            "node-a".to_owned(),
            1,
        ));
        let fs = Arc::new(
            DistributedFs::new(
                namespace,
                "node-a",
                session_id,
                meta_client,
                s3.clone(),
                read_engine,
            )
            .with_write_pipeline(
                crate::node::chunk::DirtyBudget::new(64 * 1024 * 1024),
                crate::node::vfs::dfs::WritePipelineConfig {
                    unpublished_descriptors: 1024,
                    upload_inflight_chunks: 8,
                },
                tokio::runtime::Handle::current(),
                None,
            )
            .unwrap()
            .with_gc_references(Some(references)),
        );
        Self {
            _temp: temp,
            store,
            backend,
            s3,
            fs,
            collector,
            _server: server,
        }
    }

    async fn objects(&self) -> Vec<String> {
        let mut listed = self.backend.list(None);
        let mut keys = Vec::new();
        while let Some(meta) = tokio_stream::StreamExt::next(&mut listed).await {
            keys.push(meta.unwrap().location.to_string());
        }
        keys
    }

    async fn meta_chunks(&self) -> usize {
        let view = crate::meta::store::MetaStore::read_view(self.store.as_ref())
            .await
            .unwrap();
        let (_, entities) = view.snapshot().unwrap();
        entities
            .iter()
            .filter(|entity| matches!(entity, MetaEntity::DfsChunk(_)))
            .count()
    }

    async fn with_fs<T: Send + 'static>(
        &self,
        work: impl FnOnce(&DistributedFs) -> T + Send + 'static,
    ) -> T {
        let fs = self.fs.clone();
        on_plain_thread(move || work(&fs)).await
    }

    /// Closed files' write leases run out (30 s in production). An unexpired
    /// lease keeps its inode and all its versions, so tests end it here.
    async fn expire_write_leases(&self) {
        use crate::meta::store::{
            MetaStore, MetaTxn, OperationResult, RequestKey, RequestOutcome, StoreOperation,
            TxnCondition, TxnMutation,
        };
        let (_, entities) = self.store.read_view().await.unwrap().snapshot().unwrap();
        let key = RequestKey::new("test", uuid::Uuid::new_v4().simple().to_string());
        let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsCreate);
        txn.conditions
            .push(TxnCondition::RequestAbsent(key.clone()));
        for entity in entities {
            if let MetaEntity::DfsWriteLease(mut lease) = entity {
                lease.expires_at_unix_ms = 1;
                txn.mutations
                    .push(TxnMutation::Put(MetaEntity::DfsWriteLease(lease)));
            }
        }
        txn.mutations
            .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: key,
                operation: StoreOperation::DfsCreate,
                result: OperationResult::Empty,
            }));
        self.store.compare_and_commit(txn).await.unwrap();
    }

    /// Collection passes until the object store and Meta have stopped changing:
    /// long enough for a listing to pass over everything and a generation to end.
    async fn settle(&self) {
        let mut previous = None;
        let mut stable = 0;
        for _ in 0..60 {
            let collector = self.collector.clone();
            let fs = self.fs.clone();
            on_plain_thread(move || {
                fs.writeback_pending().unwrap();
                collector.collect_once().unwrap();
            })
            .await;
            tokio::time::sleep(Duration::from_millis(5)).await;
            let current = (self.objects().await.len(), self.meta_chunks().await);
            stable = if previous == Some(current) {
                stable + 1
            } else {
                0
            };
            previous = Some(current);
            if stable >= 8 {
                return;
            }
        }
    }
}

fn create(fs: &DistributedFs, name: &str) -> types::CreatedFile {
    fs.create(
        &context(),
        fs.root_inode(),
        OsStr::new(name),
        0o640,
        libc::O_RDWR,
    )
    .unwrap()
}

fn read_chunk(fs: &DistributedFs, handle: types::FileHandle) -> Vec<u8> {
    let mut body = vec![0; CHUNK];
    let read = fs.read(&context(), handle, 0, &mut body).unwrap();
    body.truncate(read);
    body
}

/// 删文件：对象与 Meta 记录都回收，残留 0。
#[tokio::test]
async fn deleting_a_file_leaves_no_object_and_no_meta_record() {
    let cluster = Cluster::new().await;
    cluster
        .with_fs(|fs| {
            let file = create(fs, "gone.bin");
            let body = (0..2 * CHUNK + 7)
                .map(|i| (i / CHUNK) as u8)
                .collect::<Vec<_>>();
            fs.write(&context(), file.handle, 0, &body).unwrap();
            fs.fsync(&context(), file.handle, types::SyncMode::Full)
                .unwrap();
            fs.release(&context(), file.handle).unwrap();
        })
        .await;
    assert_eq!(cluster.objects().await.len(), 3);
    cluster.settle().await;
    assert_eq!(
        cluster.objects().await.len(),
        3,
        "a linked file keeps its data"
    );
    cluster
        .with_fs(|fs| {
            fs.unlink(&context(), fs.root_inode(), OsStr::new("gone.bin"))
                .unwrap();
        })
        .await;
    cluster.expire_write_leases().await;
    cluster.settle().await;
    assert!(cluster.objects().await.is_empty());
    assert_eq!(cluster.meta_chunks().await, 0);
}

/// 覆盖写：已上传又被覆盖的满块（从未登记）和被替换的旧版本都回收，只留新版本。
#[tokio::test]
async fn overwriting_reclaims_the_uploaded_block_and_the_old_version() {
    let cluster = Cluster::new().await;
    let (inode, handle) = cluster
        .with_fs(|fs| {
            let file = create(fs, "over.bin");
            fs.write(&context(), file.handle, 0, &vec![1; CHUNK])
                .unwrap();
            fs.fsync(&context(), file.handle, types::SyncMode::Full)
                .unwrap();
            // 第二代：满块先上传，再被覆盖。
            fs.write(&context(), file.handle, 0, &vec![2; CHUNK])
                .unwrap();
            fs.writeback_pending().unwrap();
            (file.entry.inode, file.handle)
        })
        .await;
    for _ in 0..200 {
        if cluster.objects().await.len() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    cluster
        .with_fs(move |fs| {
            fs.write(&context(), handle, 0, &vec![3; CHUNK]).unwrap();
            fs.fsync(&context(), handle, types::SyncMode::Full).unwrap();
            fs.release(&context(), handle).unwrap();
        })
        .await;
    cluster.expire_write_leases().await;
    cluster.settle().await;
    assert_eq!(cluster.objects().await.len(), 1, "only the head's object");
    assert_eq!(cluster.meta_chunks().await, 1);
    let body = cluster
        .with_fs(move |fs| {
            let handle = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
            let body = read_chunk(fs, handle);
            fs.release(&context(), handle).unwrap();
            body
        })
        .await;
    assert_eq!(body, vec![3; CHUNK]);
}

/// 批量上传中途失败、崩溃留下的对象：Meta 从未登记，靠对账发现并回收。
#[tokio::test]
async fn an_object_no_write_owns_is_found_by_listing_and_reclaimed() {
    let cluster = Cluster::new().await;
    let s3 = cluster.s3.clone();
    on_plain_thread(move || {
        s3.put(StagedChunk::new_generational(
            OperationId::new("crashed-write"),
            vec![9; 1024],
        ))
        .unwrap();
    })
    .await;
    assert_eq!(cluster.objects().await.len(), 1);
    cluster.settle().await;
    assert!(cluster.objects().await.is_empty());
    assert_eq!(cluster.meta_chunks().await, 0);
}

/// 进行中的写：对象已上传、未提交，写代引用保护它，对账与回收都不能删；提交后可读。
#[tokio::test]
async fn an_uploaded_uncommitted_write_survives_collection() {
    let cluster = Cluster::new().await;
    let (inode, handle) = cluster
        .with_fs(|fs| {
            let file = create(fs, "inflight.bin");
            fs.write(&context(), file.handle, 0, &vec![4; CHUNK])
                .unwrap();
            fs.writeback_pending().unwrap();
            (file.entry.inode, file.handle)
        })
        .await;
    for _ in 0..200 {
        if !cluster.objects().await.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // Only the write generation's reference protects it: no lease, no inode root.
    cluster.expire_write_leases().await;
    let collector = cluster.collector.clone();
    on_plain_thread(move || {
        for _ in 0..12 {
            collector.collect_once().unwrap();
            std::thread::sleep(Duration::from_millis(2));
        }
    })
    .await;
    assert_eq!(cluster.objects().await.len(), 1);
    let body = cluster
        .with_fs(move |fs| {
            fs.fsync(&context(), handle, types::SyncMode::Full).unwrap();
            fs.release(&context(), handle).unwrap();
            let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
            let body = read_chunk(fs, reader);
            fs.release(&context(), reader).unwrap();
            body
        })
        .await;
    assert_eq!(body, vec![4; CHUNK]);
}

/// 删除旧对象 / 重写相同内容并发：两次写的对象不同，删一个不影响另一个（D-S2-1 的核心）。
#[tokio::test]
async fn deleting_old_content_cannot_take_out_a_rewrite_of_the_same_content() {
    let cluster = Cluster::new().await;
    let body = vec![6; CHUNK];
    let first = body.clone();
    cluster
        .with_fs(move |fs| {
            let old = create(fs, "old.bin");
            fs.write(&context(), old.handle, 0, &first).unwrap();
            fs.fsync(&context(), old.handle, types::SyncMode::Full)
                .unwrap();
            fs.release(&context(), old.handle).unwrap();
        })
        .await;
    assert_eq!(cluster.objects().await.len(), 1);
    // 新写者已经上传了相同内容，尚未提交；此时删除旧文件并回收。
    let second = body.clone();
    let (inode, handle) = cluster
        .with_fs(move |fs| {
            let new = create(fs, "new.bin");
            fs.write(&context(), new.handle, 0, &second).unwrap();
            fs.writeback_pending().unwrap();
            fs.unlink(&context(), fs.root_inode(), OsStr::new("old.bin"))
                .unwrap();
            (new.entry.inode, new.handle)
        })
        .await;
    for _ in 0..200 {
        if cluster.objects().await.len() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        cluster.objects().await.len(),
        2,
        "same content, two objects"
    );
    cluster.expire_write_leases().await;
    let collector = cluster.collector.clone();
    on_plain_thread(move || {
        for _ in 0..12 {
            collector.collect_once().unwrap();
            std::thread::sleep(Duration::from_millis(2));
        }
    })
    .await;
    assert_eq!(cluster.objects().await.len(), 1, "the old object is gone");
    let read = cluster
        .with_fs(move |fs| {
            fs.fsync(&context(), handle, types::SyncMode::Full).unwrap();
            fs.release(&context(), handle).unwrap();
            let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
            let read = read_chunk(fs, reader);
            fs.release(&context(), reader).unwrap();
            read
        })
        .await;
    assert_eq!(read, body);
}

/// unlink 后仍打开的文件：`open:` 引用保护它的对象；关闭后才回收。
#[tokio::test]
async fn an_unlinked_open_file_stays_readable_until_it_is_closed() {
    let cluster = Cluster::new().await;
    let handle = cluster
        .with_fs(|fs| {
            let file = create(fs, "held.bin");
            fs.write(&context(), file.handle, 0, &vec![7; CHUNK])
                .unwrap();
            fs.fsync(&context(), file.handle, types::SyncMode::Full)
                .unwrap();
            fs.release(&context(), file.handle).unwrap();
            let inode = file.entry.inode;
            let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
            fs.unlink(&context(), fs.root_inode(), OsStr::new("held.bin"))
                .unwrap();
            reader
        })
        .await;
    cluster.expire_write_leases().await;
    cluster.settle().await;
    assert_eq!(
        cluster.objects().await.len(),
        1,
        "the open reference protects it"
    );
    let read = cluster.with_fs(move |fs| read_chunk(fs, handle)).await;
    assert_eq!(read, vec![7; CHUNK]);
    cluster
        .with_fs(move |fs| fs.release(&context(), handle).unwrap())
        .await;
    cluster.expire_write_leases().await;
    cluster.settle().await;
    assert!(cluster.objects().await.is_empty());
    assert_eq!(cluster.meta_chunks().await, 0);
}
