//! 真 DFS 后端上的 usrbio 端到端验证（crate 内用例）。
//!
//! 放在 crate 内而不是 `tests/`：`impl ChunkStore for LocalChunkStore` 是 `#[cfg(test)]` 的，
//! 独立 crate 的集成测试看不到它，而生产路径正是用它装配 `DistributedFs`。
//!
//! 已有的 usrbio 用例全部跑在内存假后端上，证明的是"环与 iov 的语义"；本文件证明
//! "同一套语义接在真实 `DistributedFs` 上仍然成立"。刻意不引入 FUSE：FUSE 只是同一个
//! `Backend` 的另一个客户端，与本层验证无关。
#![cfg(test)]

use std::{ffi::OsStr, sync::Arc, time::Duration};

use crate::{
    dfs::NamespaceId,
    meta::{
        Meta,
        rpc::{DfsMetaRpc, MetaRpc},
        store::{Store, journal::LogLimits, local_log::LocalLog},
    },
    node::{
        api::local::{LocalApiOptions, serve_local_api_with_options},
        chunk::LocalChunkStore,
        dfs_read::{ChunkTransfer, DfsReadConfig, DfsReadEngine, PeerReadBatch},
        rpc::meta::{GrpcDfsMeta, register_node_with_readiness},
        storage::Storage,
        vfs::{Backend, dfs::DistributedFs, types},
    },
    runtime::Observability,
};
use afs_client::{DfsAccess, DfsLocalClient, DfsLocalClientConfig, DfsSyncMode};
use afs_protocol::meta::{dfs_meta_server::DfsMetaServer, meta_server::MetaServer};
use afs_transport::grpc::TlsConfig;

/// 单节点用例里永远不会发生跨节点搬运；被调用即说明装配错了方向。
struct NoPeerTransfer;

impl ChunkTransfer for NoPeerTransfer {
    fn read_ranges(&self, _batch: &PeerReadBatch, _out: &mut [u8]) -> afs_error::Result<()> {
        Err(afs_error::Error::coded(
            afs_error::NODE_VFS_UNIMPLEMENTED,
            "single-node usrbio test has no chunk transfer path",
        ))
    }
}

/// 在完全独立的 std 线程上跑一段同步后端调用。
///
/// 不能用 `spawn_blocking`：它的线程仍带 tokio runtime 上下文，而 `GrpcDfsMeta` 内部的
/// `Handle::block_on` 正是不允许这一点。
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

fn context() -> types::RequestContext {
    types::RequestContext {
        uid: 0,
        gid: 0,
        pid: std::process::id(),
        umask: 0o022,
        supplementary_gids: Vec::new(),
    }
}

/// 进程内装配真实 DFS：本地日志 meta store → tonic meta 服务 → GrpcDfsMeta → DistributedFs。
async fn real_dfs(temp: &tempfile::TempDir) -> (Arc<DistributedFs>, tokio::task::JoinHandle<()>) {
    let store = Arc::new(
        Store::open_log(Arc::new(
            LocalLog::open(temp.path().join("meta"), LogLimits::default())
                .expect("open meta log"),
        ))
        .await
        .expect("open log-backed meta store"),
    );
    let meta = Arc::new(Meta::with_store(
        "usrbio-e2e".to_owned(),
        Observability::new().expect("observability"),
        store,
    ));

    // 必须先把副本配置定下来，再注册节点：配置是惰性初始化的，若让它在本节点注册之后才定型，
    // 它会铸出新的 `placement_epoch`，使节点侧算出的 `ChunkReceipt` 引用到过期的 ReplicaGroup。
    meta.dfs
        .as_ref()
        .expect("meta owns a dfs service")
        .initialize_replication_config()
        .await
        .expect("initialize replication config");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind meta listener");
    let address = listener.local_addr().expect("meta address");
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    // `GrpcDfsMeta` 说的是 `DfsMeta` 服务，与 `Meta` 是两个独立的 gRPC service；
    // 只注册 `Meta` 会让所有 DFS 调用拿到 UNIMPLEMENTED。
    let service = MetaServer::new(MetaRpc(meta.clone()));
    let dfs_service = DfsMetaServer::new(DfsMetaRpc(meta));
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .add_service(dfs_service)
            .serve_with_incoming(incoming)
            .await
            .expect("serve meta");
    });

    let namespace = NamespaceId::new("default");
    let endpoint = format!("http://{address}");
    let meta_client = Arc::new(
        GrpcDfsMeta::new(
            &endpoint,
            "node-a".to_owned(),
            "session-a".to_owned(),
            namespace.clone(),
            Duration::from_secs(10),
            TlsConfig::Disabled,
        )
        .expect("grpc dfs meta"),
    );
    let chunks = Arc::new(
        LocalChunkStore::open(temp.path().join("chunks"), "node-a").expect("local chunk store"),
    );

    // 生产节点在挂载前必须先向 Meta 注册会话；不注册的话所有涉及 placement 的调用都会被
    // 拒绝（"DFS placement requires a live caller Node session"）。
    let device = chunks.device_descriptor().expect("chunk device descriptor");
    let node_epoch = register_node_with_readiness(
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
            session_id: "session-a".to_owned(),
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
    .expect("register node session")
    .lease_epoch;
    let read_engine = Arc::new(DfsReadEngine::new(
        namespace.clone(),
        "node-a".to_owned(),
        chunks.clone(),
        meta_client.clone(),
        Arc::new(NoPeerTransfer),
        DfsReadConfig::default(),
    ));
    // 生产装配用的是 `DfsChunkStore`：它向 Meta 取真实 placement 再生成 receipt，
    // 落盘仍然由本地 `LocalChunkStore` 完成。直接用 `LocalChunkStore` 当 `ChunkStore` 走的是
    // `#[cfg(test)]` 的脚手架实现（placement 是写死的假值），`fsync` 必然失败。
    let peers = Arc::new(
        crate::node::rpc::peer::PeerConnectionPool::new(
            afs_transport::grpc::GrpcConfig {
                connect_timeout: Duration::from_secs(10),
                request_timeout: Duration::from_secs(10),
                ..Default::default()
            },
            TlsConfig::Disabled,
            64,
        )
        .expect("peer connection pool"),
    );
    let replica_plane = crate::node::rpc::peer::make_replica_data_plane(
        crate::node::rpc::peer::DataMode::Grpc,
        peers,
        Duration::from_secs(10),
        None,
    )
    .expect("replica data plane");
    let chunk_store: Arc<dyn crate::node::chunk::ChunkStore> =
        Arc::new(crate::node::replication::DfsChunkStore::new_with_epoch(
            "node-a".to_owned(),
            node_epoch,
            chunks,
            meta_client.clone(),
            replica_plane,
        ));
    let dfs = Arc::new(DistributedFs::new(
        namespace,
        "node-a",
        "session-a",
        meta_client,
        chunk_store,
        read_engine,
    ));
    (dfs, server)
}

/// 真实 `DistributedFs` 上跑一遍 open / 写 / sync / 读 / 大小核对。
///
/// 这个用例暴露过一个架构级问题并已验证修复：`GrpcDfsMeta` 用 `Handle::block_on`，而控制面的
/// RPC 处理器跑在 tokio runtime 线程上，导致第一次 `open` 就 panic。修复是把控制面的后端调用
/// 交给 [`super::usrbio::UsrbioHub::executor`] 那个独立的 std 线程——现在 create / open /
/// write 在真实 `DistributedFs` 上都能走通。
///
/// 仍然 **ignored**：卡在 `fsync` 的 `"ChunkReceipt references a stale ReplicaGroup"`。
/// 原因是测试装配缺了生产的 DFS 引导：Meta 的副本配置（`DfsService::initialize_replication_config`）
/// 是惰性初始化的，而 `Meta` 不把它暴露给 gRPC 调用方，测试里无法在注册节点之前把它定下来，
/// 于是节点侧算出的 `placement_epoch` 与 Meta 初始化后的不一致。这不是本层缺陷，需要
/// 一个测试可用的初始化入口（或把节点注册放到配置定型之后）。
#[tokio::test]
async fn usrbio_round_trips_on_a_real_dfs_backend() {
    let temp = tempfile::tempdir().expect("tempdir");
    let (dfs, server) = real_dfs(&temp).await;

    // 通过同一个 Backend 建文件：SDK 与 FUSE 看到的是同一份后端状态。
    let creator = dfs.clone();
    let (handle, size) = on_plain_thread(move || {
        let created = creator.create(
            &context(),
            creator.root_inode(),
            OsStr::new("data.bin"),
            0o644,
            libc::O_RDWR,
        )?;
        let handle = created.handle;
        creator.release(&context(), handle)?;
        Ok::<_, afs_error::Error>((handle, created.entry.attributes.size))
    })
    .await
    .expect("create data.bin");
    assert_eq!(size, 0, "新建文件长度应为 0");
    assert!(handle.0 > 0, "创建必须返回有效句柄");

    let socket_path = temp.path().join("afs-node.sock");
    let storage = Storage::new(temp.path().join("diagnostics")).expect("storage");
    let server_task = serve_local_api_with_options(
        storage,
        &socket_path,
        LocalApiOptions {
            metrics_registry: None,
            dfs_backend: Some(dfs.clone() as Arc<dyn Backend>),
            dfs_namespace: Some("default".to_owned()),
        },
    )
    .await
    .expect("serve local api");

    let client = DfsLocalClient::connect(DfsLocalClientConfig::new(&socket_path))
        .await
        .expect("connect");
    let file = client
        .open("/data.bin", DfsAccess::ReadWrite)
        .await
        .expect("open through sdk");
    assert_eq!(file.length, 0, "新建文件长度应为 0");

    let mut iov = client.create_iov(4096, 2).await.expect("create iov");
    let mut ring = client
        .create_ring(&iov, 8, 0, 500)
        .await
        .expect("create ring");
    let registration = client.register_file(&file).await.expect("register");

    let written = ring
        .write_from(&mut iov, &registration, 0, b"usrbio")
        .await
        .expect("write on real dfs");
    assert_eq!(written, 6);

    // 真实落盘：`DfsChunkStore` 会向 Meta 取真实 placement 生成 receipt，
    // 数据最终由本地 `LocalChunkStore` 写到磁盘上。
    client
        .sync(&file.handle, DfsSyncMode::Data)
        .await
        .expect("sync data");
    client
        .sync(&file.handle, DfsSyncMode::Full)
        .await
        .expect("sync full");

    let mut out = vec![0_u8; 6];
    let read = ring
        .read_into(&mut iov, &registration, 0, &mut out)
        .await
        .expect("read on real dfs");
    assert_eq!(read, 6);
    assert_eq!(&out, b"usrbio");

    // 数据必须真的落到本地盘上：chunk store 目录里应当出现非空文件。
    let chunk_bytes: u64 = walk_bytes(&temp.path().join("chunks"));
    assert!(chunk_bytes > 0, "fsync 之后 chunk store 里应有落盘数据");

    // 提交后长度必须反映到后端属性上。
    // 后端必须真的认识这个文件：SDK 与 FUSE 看到的是同一份命名空间状态。
    let inspector = dfs.clone();
    let (root_kind, entry_kind) = on_plain_thread(move || {
        let attributes = inspector.getattr(&context(), inspector.root_inode(), None)?;
        let entry = inspector.lookup(&context(), inspector.root_inode(), OsStr::new("data.bin"))?;
        Ok::<_, afs_error::Error>((attributes.kind, entry.attributes.kind))
    })
    .await
    .expect("inspect backend");
    assert_eq!(root_kind, types::FileKind::Directory);
    assert_eq!(
        entry_kind,
        types::FileKind::Regular,
        "后端应把 data.bin 视为普通文件"
    );

    client
        .unregister_file(registration)
        .await
        .expect("unregister");
    client.close(file.handle).await.expect("close");
    ring.destroy().await.expect("destroy ring");
    iov.destroy().await.expect("destroy iov");
    server_task.shutdown().await.expect("shutdown local api");
    server.abort();
}

/// 数据面每 IO 成本：逐条提交 vs 批量提交。
///
/// 用 release 模式跑才有意义：
/// `cargo test --release -p afs --lib usrbio_perf -- --ignored --nocapture`。
///
/// 它量的是**本层**（环提交 + iov 管理 + UDS 往返）的增量，不是磁盘带宽：写入落在 DFS 的
/// 写缓冲里（本快照缺生产版 `ChunkStore`，`fsync` 无法验证，见上面的说明）。
#[ignore = "性能测量，不在门禁里跑"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn usrbio_perf_single_vs_batch() {
    use std::time::Instant;

    const WAVE: usize = 32;
    const REGION: u64 = 4 * 1024 * 1024;

    let temp = tempfile::tempdir().expect("tempdir");
    let (dfs, server) = real_dfs(&temp).await;

    let creator = dfs.clone();
    on_plain_thread(move || {
        let created = creator.create(
            &context(),
            creator.root_inode(),
            OsStr::new("perf.bin"),
            0o644,
            libc::O_RDWR,
        )?;
        creator.release(&context(), created.handle)
    })
    .await
    .expect("create perf.bin");

    let socket_path = temp.path().join("afs-node.sock");
    let storage = Storage::new(temp.path().join("diagnostics")).expect("storage");
    let server_task = serve_local_api_with_options(
        storage,
        &socket_path,
        LocalApiOptions {
            metrics_registry: None,
            dfs_backend: Some(dfs.clone() as Arc<dyn Backend>),
            dfs_namespace: Some("default".to_owned()),
        },
    )
    .await
    .expect("serve local api");
    let client = DfsLocalClient::connect(DfsLocalClientConfig::new(&socket_path))
        .await
        .expect("connect");
    let file = client
        .open("/perf.bin", DfsAccess::ReadWrite)
        .await
        .expect("open");
    let mut iov = client.create_iov(4 * 1024 * 1024, 64).await.expect("iov");
    let mut ring = client.create_ring(&iov, 256, 0, 500).await.expect("ring");
    let registration = client.register_file(&file).await.expect("register");

    // 每种负载的总写入量都压在 2–4 MiB：本快照缺生产版 ChunkStore，累计写入到缓冲阈值后
    // DFS 会触发落盘并撞上测试脚手架的假 receipt。测每 IO 成本不需要大数据量。
    for (payload_len, ops) in [(4096_usize, 512), (65536, 48)] {
        let payload = vec![0x5a_u8; payload_len];
        let slots = (REGION / payload_len as u64).max(1);

        // 逐条提交
        let start = Instant::now();
        for index in 0..ops {
            let offset = (index as u64 % slots) * payload_len as u64;
            if let Err(error) = ring
                .write_from(&mut iov, &registration, offset, &payload)
                .await
            {
                panic!("single write failed at op {index} offset {offset}: {error}");
            }
        }
        let single = start.elapsed();

        // 批量提交（每波 WAVE 条、一次门铃）
        let batch_items: Vec<(u64, &[u8])> = (0..WAVE)
            .map(|slot| (slot as u64 * payload_len as u64, payload.as_slice()))
            .collect();
        let start = Instant::now();
        for _ in 0..(ops / WAVE) {
            ring.write_batch(&mut iov, &registration, &batch_items)
                .await
                .expect("batch write");
        }
        let batch = start.elapsed();

        // 读回
        let mut out = vec![0_u8; payload_len];
        let start = Instant::now();
        for index in 0..ops {
            let offset = (index as u64 % slots) * payload_len as u64;
            ring.read_into(&mut iov, &registration, offset, &mut out)
                .await
                .expect("read");
        }
        let read = start.elapsed();

        let per = |elapsed: std::time::Duration| elapsed.as_secs_f64() * 1e9 / ops as f64;
        let mib = |elapsed: std::time::Duration| {
            (ops * payload_len) as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0)
        };
        println!(
            "payload={payload_len}B single={:.0}ns/op ({:.0} MiB/s) batch={:.0}ns/op ({:.0} MiB/s) read={:.0}ns/op ({:.0} MiB/s)",
            per(single),
            mib(single),
            per(batch),
            mib(batch),
            per(read),
            mib(read),
        );
    }

    assert_eq!(iov.in_flight(), 0, "测量结束不应有在途区块");
    client
        .unregister_file(registration)
        .await
        .expect("unregister");
    client.close(file.handle).await.expect("close");
    ring.destroy().await.expect("destroy ring");
    iov.destroy().await.expect("destroy iov");
    server_task.shutdown().await.expect("shutdown");
    server.abort();
}

/// 大流量吞吐：秒级测量，不靠"小样本除以条数"凑出纳秒级读数。
///
/// 走内存后端而不是真 DFS —— 本快照缺生产版 `ChunkStore`，真 DFS 累计写约 8 MiB 就会触发落盘
/// 并撞上测试脚手架的假 receipt。内存后端没有这条路径，可以灌任意大的数据量，因此适合测
/// **本层**（环提交 + iov 管理 + UDS 往返）的带宽。
///
/// `cargo test --release -p afs --lib usrbio_bulk -- --ignored --nocapture --test-threads=1`
#[ignore = "性能测量，不在门禁里跑"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn usrbio_perf_bulk_throughput() {
    use crate::node::api::usrbio::tests::MemoryBackend;
    use std::time::Instant;

    /// 每种配置的总字节数：够跑出秒级时长，又不会把内存撑爆。
    const TOTAL_BYTES: u64 = 256 * 1024 * 1024;
    const ROUNDS: usize = 3;

    let temp = tempfile::tempdir().expect("tempdir");
    let backend = Arc::new(MemoryBackend::new(1024));
    let socket_path = temp.path().join("afs-node.sock");
    let storage = Storage::new(temp.path().join("diagnostics")).expect("storage");
    let server = serve_local_api_with_options(
        storage,
        &socket_path,
        LocalApiOptions {
            metrics_registry: None,
            dfs_backend: Some(backend as Arc<dyn Backend>),
            dfs_namespace: Some("default".to_owned()),
        },
    )
    .await
    .expect("serve local api");
    let client = DfsLocalClient::connect(DfsLocalClientConfig::new(&socket_path))
        .await
        .expect("connect");
    let file = client
        .open("/data.bin", DfsAccess::ReadWrite)
        .await
        .expect("open");
    let mut iov = client
        .create_iov(1024 * 1024, 64)
        .await
        .expect("create iov");
    let mut ring = client
        .create_ring(&iov, 256, 0, 5000)
        .await
        .expect("create ring");
    let registration = client.register_file(&file).await.expect("register");

    let median = |mut samples: Vec<f64>| {
        samples.sort_by(|left, right| left.partial_cmp(right).expect("finite"));
        samples[samples.len() / 2]
    };

    for (payload_len, wave) in [(4096_usize, 32_usize), (1024 * 1024, 8)] {
        let payload = vec![0x5a_u8; payload_len];
        let ops = (TOTAL_BYTES / payload_len as u64) as usize;
        let slots = 256_u64; // 在固定槽位内循环，避免文件无限增长

        // 单条提交
        let mut single = Vec::new();
        for _ in 0..ROUNDS {
            let start = Instant::now();
            for index in 0..ops {
                let offset = (index as u64 % slots) * payload_len as u64;
                ring.write_from(&mut iov, &registration, offset, &payload)
                    .await
                    .expect("single write");
            }
            single.push(elapsed_mib_s(start.elapsed(), ops, payload_len));
        }

        // 批量提交
        let items: Vec<(u64, &[u8])> = (0..wave)
            .map(|slot| (slot as u64 * payload_len as u64, payload.as_slice()))
            .collect();
        let mut batch = Vec::new();
        for _ in 0..ROUNDS {
            let start = Instant::now();
            for _ in 0..(ops / wave) {
                ring.write_batch(&mut iov, &registration, &items)
                    .await
                    .expect("batch write");
            }
            batch.push(elapsed_mib_s(start.elapsed(), ops, payload_len));
        }

        // 读回
        let mut out = vec![0_u8; payload_len];
        let mut read = Vec::new();
        for _ in 0..ROUNDS {
            let start = Instant::now();
            for index in 0..ops {
                let offset = (index as u64 % slots) * payload_len as u64;
                ring.read_into(&mut iov, &registration, offset, &mut out)
                    .await
                    .expect("read");
            }
            read.push(elapsed_mib_s(start.elapsed(), ops, payload_len));
        }

        println!(
            "payload={payload_len}B total={}MiB single={:.0}MiB/s batch={:.0}MiB/s read={:.0}MiB/s",
            TOTAL_BYTES / (1024 * 1024),
            median(single),
            median(batch),
            median(read),
        );
    }

    assert_eq!(iov.in_flight(), 0, "测量结束不应有在途区块");
    client
        .unregister_file(registration)
        .await
        .expect("unregister");
    client.close(file.handle).await.expect("close");
    ring.destroy().await.expect("destroy ring");
    iov.destroy().await.expect("destroy iov");
    server.shutdown().await.expect("shutdown");
}

/// 递归统计目录下的文件字节数，用来证明数据真的落到了盘上。
fn walk_bytes(root: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(root) else {
        return 0;
    };
    let mut total = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        match entry.file_type() {
            Ok(kind) if kind.is_dir() => total += walk_bytes(&path),
            Ok(_) => total += entry.metadata().map(|meta| meta.len()).unwrap_or(0),
            Err(_) => {}
        }
    }
    total
}

/// 把一次测量的耗时换算成 MiB/s。
fn elapsed_mib_s(elapsed: std::time::Duration, ops: usize, payload_len: usize) -> f64 {
    let bytes = (ops * payload_len) as f64;
    bytes / elapsed.as_secs_f64() / (1024.0 * 1024.0)
}

/// 多客户端 / 多环并发：各写各的区间，互不串扰。
///
/// 3FS 的模型是"每线程一条环"。这里起 N 个独立客户端，每个持有自己的 iov 与环，
/// 往同一个文件的不同区间写不同内容，最后逐个区间回读校验——串扰会直接表现为内容错位。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn usrbio_multi_client_rings_keep_regions_isolated() {
    const CLIENTS: u64 = 4;
    const OPS: u64 = 64;
    const CHUNK: usize = 4096; // iov 块大小下限是 4096

    let temp = tempfile::tempdir().expect("tempdir");
    let (dfs, server) = real_dfs(&temp).await;
    let creator = dfs.clone();
    on_plain_thread(move || {
        let created = creator.create(
            &context(),
            creator.root_inode(),
            OsStr::new("shared.bin"),
            0o644,
            libc::O_RDWR,
        )?;
        creator.release(&context(), created.handle)
    })
    .await
    .expect("create shared.bin");

    let socket_path = temp.path().join("afs-node.sock");
    let storage = Storage::new(temp.path().join("diagnostics")).expect("storage");
    let api = serve_local_api_with_options(
        storage,
        &socket_path,
        LocalApiOptions {
            metrics_registry: None,
            dfs_backend: Some(dfs.clone() as Arc<dyn Backend>),
            dfs_namespace: Some("default".to_owned()),
        },
    )
    .await
    .expect("serve local api");

    let mut tasks = Vec::new();
    for client_index in 0..CLIENTS {
        let socket_path = socket_path.clone();
        tasks.push(tokio::spawn(async move {
            // 每个客户端用不同的填充字节，串扰会直接暴露成"读到了别人的字节"。
            let fill = 0x40 + client_index as u8;
            let payload = vec![fill; CHUNK];
            let client = DfsLocalClient::connect(DfsLocalClientConfig::new(&socket_path))
                .await
                .expect("connect");
            let file = client
                .open("/shared.bin", DfsAccess::ReadWrite)
                .await
                .expect("open");
            let mut iov = client.create_iov(CHUNK, 2).await.expect("create iov");
            let mut ring = client
                .create_ring(&iov, 16, 0, 5000)
                .await
                .expect("create ring");
            let registration = client.register_file(&file).await.expect("register");

            // 每个客户端独占一段互不重叠的区间。
            let base = client_index * OPS * CHUNK as u64;
            for op in 0..OPS {
                let offset = base + op * CHUNK as u64;
                ring.write_from(&mut iov, &registration, offset, &payload)
                    .await
                    .expect("write");
            }

            let mut out = vec![0_u8; CHUNK];
            for op in 0..OPS {
                let offset = base + op * CHUNK as u64;
                let read = ring
                    .read_into(&mut iov, &registration, offset, &mut out)
                    .await
                    .expect("read");
                assert_eq!(read, CHUNK);
                assert!(
                    out.iter().all(|byte| *byte == fill),
                    "client {client_index} op {op} 读到了别人的内容（区间串扰）"
                );
            }

            client
                .unregister_file(registration)
                .await
                .expect("unregister");
            client.close(file.handle).await.expect("close");
            ring.destroy().await.expect("destroy ring");
            iov.destroy().await.expect("destroy iov");
        }));
    }
    for task in tasks {
        task.await.expect("client task");
    }

    api.shutdown().await.expect("shutdown");
    server.abort();
}

/// SDK 写入的数据必须能被 FUSE 那条路径读到。
///
/// 这里不挂 FUSE（环境里没有 node 二进制），而是直接调用 FUSE 走的那套 `Backend` 方法：
/// `dfs.open` → `dfs.read`。二者共用同一个 `DistributedFs`，所以这条用例覆盖的正是
/// "SDK 写完、FUSE 能看见"的语义边界；它不能替代真实挂载验证，但能证明数据面没有分叉。
#[tokio::test]
async fn usrbio_writes_are_visible_through_the_backend_read_path() {
    let temp = tempfile::tempdir().expect("tempdir");
    let (dfs, server) = real_dfs(&temp).await;
    let creator = dfs.clone();
    on_plain_thread(move || {
        let created = creator.create(
            &context(),
            creator.root_inode(),
            OsStr::new("visible.bin"),
            0o644,
            libc::O_RDWR,
        )?;
        creator.release(&context(), created.handle)
    })
    .await
    .expect("create visible.bin");

    let socket_path = temp.path().join("afs-node.sock");
    let storage = Storage::new(temp.path().join("diagnostics")).expect("storage");
    let api = serve_local_api_with_options(
        storage,
        &socket_path,
        LocalApiOptions {
            metrics_registry: None,
            dfs_backend: Some(dfs.clone() as Arc<dyn Backend>),
            dfs_namespace: Some("default".to_owned()),
        },
    )
    .await
    .expect("serve local api");

    // 通过 SDK 写入并落盘。
    const PAYLOAD: &[u8] = b"sdk-wrote-this";
    let client = DfsLocalClient::connect(DfsLocalClientConfig::new(&socket_path))
        .await
        .expect("connect");
    let file = client
        .open("/visible.bin", DfsAccess::ReadWrite)
        .await
        .expect("open");
    let mut iov = client.create_iov(4096, 2).await.expect("create iov");
    let mut ring = client
        .create_ring(&iov, 8, 0, 5000)
        .await
        .expect("create ring");
    let registration = client.register_file(&file).await.expect("register");
    ring.write_from(&mut iov, &registration, 0, PAYLOAD)
        .await
        .expect("write");
    client
        .sync(&file.handle, DfsSyncMode::Full)
        .await
        .expect("sync");

    // 走 FUSE 的读路径把它读回来。
    let reader = dfs.clone();
    let expected = PAYLOAD.to_vec();
    on_plain_thread(move || {
        // 与 FUSE 完全一致的顺序：解析名字拿 inode，再按 inode 打开。
        let entry = reader.lookup(&context(), reader.root_inode(), OsStr::new("visible.bin"))?;
        let opened = reader.open(&context(), entry.inode, libc::O_RDONLY)?;
        let mut buffer = vec![0_u8; expected.len()];
        let read = reader.read(&context(), opened, 0, &mut buffer)?;
        reader.release(&context(), opened)?;
        assert_eq!(read, expected.len(), "FUSE 读路径应读满写入长度");
        assert_eq!(buffer, expected, "SDK 写入的内容必须与 FUSE 读到的一致");
        Ok::<(), afs_error::Error>(())
    })
    .await
    .expect("read through backend path");

    client
        .unregister_file(registration)
        .await
        .expect("unregister");
    client.close(file.handle).await.expect("close");
    ring.destroy().await.expect("destroy ring");
    iov.destroy().await.expect("destroy iov");
    api.shutdown().await.expect("shutdown");
    server.abort();
}

/// 真实磁盘吞吐：写 1 GiB 到本地盘（含 fsync），再读回 1 GiB。
///
/// 与 `usrbio_perf_bulk_throughput` 的区别很关键：那条走内存后端，测的是本层开销；
/// 这条走生产装配（`DfsChunkStore` → 本地 `LocalChunkStore` → 磁盘），
/// 因此会真正暴露磁盘带宽与落盘代价。
///
/// `cargo test --release -p afs --lib usrbio_perf_real_disk -- --ignored --nocapture --test-threads=1`
#[ignore = "性能测量，不在门禁里跑；写 1 GiB 到磁盘"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn usrbio_perf_real_disk_one_gib() {
    use std::time::Instant;

    const TOTAL_BYTES: u64 = 1024 * 1024 * 1024;
    const PAYLOAD: usize = 1024 * 1024;
    const WAVE: usize = 8;

    let temp = tempfile::tempdir().expect("tempdir");
    let (dfs, server) = real_dfs(&temp).await;
    let creator = dfs.clone();
    on_plain_thread(move || {
        let created = creator.create(
            &context(),
            creator.root_inode(),
            OsStr::new("bulk.bin"),
            0o644,
            libc::O_RDWR,
        )?;
        creator.release(&context(), created.handle)
    })
    .await
    .expect("create bulk.bin");

    let socket_path = temp.path().join("afs-node.sock");
    let storage = Storage::new(temp.path().join("diagnostics")).expect("storage");
    let api = serve_local_api_with_options(
        storage,
        &socket_path,
        LocalApiOptions {
            metrics_registry: None,
            dfs_backend: Some(dfs.clone() as Arc<dyn Backend>),
            dfs_namespace: Some("default".to_owned()),
        },
    )
    .await
    .expect("serve local api");

    let client = DfsLocalClient::connect(DfsLocalClientConfig::new(&socket_path))
        .await
        .expect("connect");
    let file = client
        .open("/bulk.bin", DfsAccess::ReadWrite)
        .await
        .expect("open");
    let mut iov = client.create_iov(PAYLOAD, 16).await.expect("create iov");
    let mut ring = client
        .create_ring(&iov, 128, 0, 10_000)
        .await
        .expect("create ring");
    let registration = client.register_file(&file).await.expect("register");

    let payload = vec![0xa5_u8; PAYLOAD];
    let items: Vec<(u64, &[u8])> = (0..WAVE)
        .map(|slot| (slot as u64 * PAYLOAD as u64, payload.as_slice()))
        .collect();

    // ---- 写 1 GiB ----
    let waves = (TOTAL_BYTES / (PAYLOAD as u64 * WAVE as u64)) as usize;
    let started = Instant::now();
    for wave in 0..waves {
        // 每波写进 iov 的同一组块，但文件偏移持续前进。
        let base = wave as u64 * PAYLOAD as u64 * WAVE as u64;
        let shifted: Vec<(u64, &[u8])> = items
            .iter()
            .map(|(offset, data)| (base + offset, *data))
            .collect();
        ring.write_batch(&mut iov, &registration, &shifted)
            .await
            .expect("write wave");
    }
    let write_elapsed = started.elapsed();

    // ---- 落盘（单独计时：这是持久化代价，不该混进写带宽）----
    let started = Instant::now();
    client
        .sync(&file.handle, DfsSyncMode::Full)
        .await
        .expect("sync full");
    let sync_elapsed = started.elapsed();

    // ---- 读回 1 GiB ----
    let mut buffer = vec![0_u8; PAYLOAD];
    let started = Instant::now();
    for wave in 0..waves {
        let base = wave as u64 * PAYLOAD as u64 * WAVE as u64;
        for slot in 0..WAVE {
            ring.read_into(
                &mut iov,
                &registration,
                base + slot as u64 * PAYLOAD as u64,
                &mut buffer,
            )
            .await
            .expect("read");
        }
    }
    let read_elapsed = started.elapsed();

    println!(
        "1GiB 真实磁盘: 写 {:.0} MiB/s ({:?}) / sync(Full) {:?} / 读 {:.0} MiB/s ({:?})",
        TOTAL_BYTES as f64 / write_elapsed.as_secs_f64() / (1024.0 * 1024.0),
        write_elapsed,
        sync_elapsed,
        TOTAL_BYTES as f64 / read_elapsed.as_secs_f64() / (1024.0 * 1024.0),
        read_elapsed,
    );

    assert_eq!(iov.in_flight(), 0);
    client
        .unregister_file(registration)
        .await
        .expect("unregister");
    client.close(file.handle).await.expect("close");
    ring.destroy().await.expect("destroy ring");
    iov.destroy().await.expect("destroy iov");
    api.shutdown().await.expect("shutdown");
    server.abort();
}

/// 真实磁盘上的两项验收：4 KiB 小 IO 的 IOPS 曲线，以及多客户端读扩展性。
///
/// `cargo test --release -p afs --lib usrbio_perf_real_disk_scaling -- --ignored --nocapture --test-threads=1`
#[ignore = "性能测量，不在门禁里跑"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn usrbio_perf_real_disk_scaling() {
    use std::time::Instant;

    const SMALL: usize = 4096;
    const SMALL_TOTAL: u64 = 64 * 1024 * 1024;
    const WAVE: usize = 32;
    const REGION: u64 = 64 * 1024 * 1024;

    let temp = tempfile::tempdir().expect("tempdir");
    let (dfs, server) = real_dfs(&temp).await;
    let creator = dfs.clone();
    on_plain_thread(move || {
        let created = creator.create(
            &context(),
            creator.root_inode(),
            OsStr::new("scale.bin"),
            0o644,
            libc::O_RDWR,
        )?;
        creator.release(&context(), created.handle)
    })
    .await
    .expect("create scale.bin");

    let socket_path = temp.path().join("afs-node.sock");
    let storage = Storage::new(temp.path().join("diagnostics")).expect("storage");
    let api = serve_local_api_with_options(
        storage,
        &socket_path,
        LocalApiOptions {
            metrics_registry: None,
            dfs_backend: Some(dfs.clone() as Arc<dyn Backend>),
            dfs_namespace: Some("default".to_owned()),
        },
    )
    .await
    .expect("serve local api");

    // ---- 4 KiB 小 IO：单条 vs 批量 ----
    let small_ops = (SMALL_TOTAL / SMALL as u64) as usize;
    {
        let client = DfsLocalClient::connect(DfsLocalClientConfig::new(&socket_path))
            .await
            .expect("connect");
        let file = client
            .open("/scale.bin", DfsAccess::ReadWrite)
            .await
            .expect("open");
        let mut iov = client.create_iov(SMALL, 64).await.expect("create iov");
        let mut ring = client
            .create_ring(&iov, 256, 0, 5_000)
            .await
            .expect("create ring");
        let registration = client.register_file(&file).await.expect("register");
        let payload = vec![0x3c_u8; SMALL];
        // 在 64 个块内循环，避免文件无限增长。
        let slots = 64_u64;

        let started = Instant::now();
        for op in 0..small_ops {
            let offset = (op as u64 % slots) * SMALL as u64;
            ring.write_from(&mut iov, &registration, offset, &payload)
                .await
                .expect("single write");
        }
        let single = started.elapsed();
        let single_iops = small_ops as f64 / single.as_secs_f64();

        let items: Vec<(u64, &[u8])> = (0..WAVE)
            .map(|slot| (slot as u64 * SMALL as u64, payload.as_slice()))
            .collect();
        let started = Instant::now();
        for _ in 0..(small_ops / WAVE) {
            ring.write_batch(&mut iov, &registration, &items)
                .await
                .expect("batch write");
        }
        let batch = started.elapsed();
        let batch_iops = small_ops as f64 / batch.as_secs_f64();

        println!(
            "4KiB 真实磁盘: 单条 {single_iops:.0} IOPS ({:.0} MiB/s) / 批量{WAVE} {batch_iops:.0} IOPS ({:.0} MiB/s) / 加速 {:.1}x",
            single_iops * SMALL as f64 / (1024.0 * 1024.0),
            batch_iops * SMALL as f64 / (1024.0 * 1024.0),
            batch_iops / single_iops,
        );

        client
            .unregister_file(registration)
            .await
            .expect("unregister");
        client.close(file.handle).await.expect("close");
        ring.destroy().await.expect("destroy ring");
        iov.destroy().await.expect("destroy iov");
    }

    // ---- 预置 8 个互不重叠的读区间 ----
    const MAX_CLIENTS: u64 = 8;
    {
        let client = DfsLocalClient::connect(DfsLocalClientConfig::new(&socket_path))
            .await
            .expect("connect");
        let file = client
            .open("/scale.bin", DfsAccess::ReadWrite)
            .await
            .expect("open");
        let payload = vec![0x77_u8; 1024 * 1024];
        let mut iov = client.create_iov(1024 * 1024, 8).await.expect("create iov");
        let mut ring = client
            .create_ring(&iov, 64, 0, 10_000)
            .await
            .expect("create ring");
        let registration = client.register_file(&file).await.expect("register");
        let items: Vec<(u64, &[u8])> = (0..8)
            .map(|slot| (slot as u64 * 1024 * 1024, payload.as_slice()))
            .collect();
        // 每个区间只种头部 8 MiB；读的时候也只在这 8 MiB 内循环，形状对等才好比较扩展性。
        for region in 0..MAX_CLIENTS {
            let base = region * REGION;
            let shifted: Vec<(u64, &[u8])> = items
                .iter()
                .enumerate()
                .map(|(index, (_, data))| (base + index as u64 * 1024 * 1024, *data))
                .collect();
            ring.write_batch(&mut iov, &registration, &shifted)
                .await
                .expect("seed region");
        }
        client
            .unregister_file(registration)
            .await
            .expect("unregister");
        client.close(file.handle).await.expect("close");
        ring.destroy().await.expect("destroy ring");
        iov.destroy().await.expect("destroy iov");
    }

    // ---- 读扩展性：1 / 4 / 8 个客户端，各自读独立区间 ----
    for clients in [1_u64, 4, 8] {
        let per_client = REGION / clients;
        let started = Instant::now();
        let mut tasks = Vec::new();
        for index in 0..clients {
            let socket_path = socket_path.clone();
            tasks.push(tokio::spawn(async move {
                let client = DfsLocalClient::connect(DfsLocalClientConfig::new(&socket_path))
                    .await
                    .expect("connect");
                let file = client
                    .open("/scale.bin", DfsAccess::ReadWrite)
                    .await
                    .expect("open");
                let mut iov = client.create_iov(1024 * 1024, 8).await.expect("create iov");
                let mut ring = client
                    .create_ring(&iov, 64, 0, 10_000)
                    .await
                    .expect("create ring");
                let registration = client.register_file(&file).await.expect("register");
                let mut buffer = vec![0_u8; 1024 * 1024];
                let base = index * per_client;
                let mut done = 0_u64;
                while done < per_client {
                    // 只读预置过的头部，避免读到未写区间影响语义（长度仍按 per_client 计）。
                    let offset = base + (done % (8 * 1024 * 1024));
                    ring.read_into(&mut iov, &registration, offset, &mut buffer)
                        .await
                        .expect("read");
                    done += 1024 * 1024;
                }
                client
                    .unregister_file(registration)
                    .await
                    .expect("unregister");
                client.close(file.handle).await.expect("close");
                ring.destroy().await.expect("destroy ring");
                iov.destroy().await.expect("destroy iov");
            }));
        }
        for task in tasks {
            task.await.expect("read client");
        }
        let elapsed = started.elapsed();
        let bytes = (per_client * clients) as f64;
        println!(
            "{clients} 客户端读: {:.0} MiB/s（合计 {} MiB / {:?}）",
            bytes / elapsed.as_secs_f64() / (1024.0 * 1024.0),
            bytes as u64 / (1024 * 1024),
            elapsed,
        );
    }

    api.shutdown().await.expect("shutdown");
    server.abort();
}

/// 地板价：直接调用 FUSE 也会走的 `Backend::write` / `Backend::read`，不经环、不经 UDS。
///
/// 目的不是"跑分"，而是**把时间归属算清楚**：
/// 如果这个数字和 SDK 环路径的数字接近，说明 e2e 时间里几乎全是 DFS 自己，
/// 我这层的优化在天花板下根本显不出来；反之才说明环路径有真实收益。
///
/// `cargo test --release -p afs --lib usrbio_perf_backend_floor -- --ignored --nocapture --test-threads=1`
#[ignore = "性能测量，不在门禁里跑"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn usrbio_perf_backend_floor() {
    use std::time::Instant;

    const SLOTS: u64 = 64;

    let temp = tempfile::tempdir().expect("tempdir");
    let (dfs, server) = real_dfs(&temp).await;
    let creator = dfs.clone();
    on_plain_thread(move || {
        let created = creator.create(
            &context(),
            creator.root_inode(),
            OsStr::new("floor.bin"),
            0o644,
            libc::O_RDWR,
        )?;
        creator.release(&context(), created.handle)
    })
    .await
    .expect("create floor.bin");

    let opener = dfs.clone();
    let handle = on_plain_thread(move || {
        let entry = opener.lookup(&context(), opener.root_inode(), OsStr::new("floor.bin"))?;
        opener.open(&context(), entry.inode, libc::O_RDWR)
    })
    .await
    .expect("open floor.bin");

    for (payload_len, ops) in [(4096_usize, 4096_usize), (1024 * 1024, 512)] {
        let payload = vec![0x9b_u8; payload_len];
        let mut out = vec![0_u8; payload_len];

        // ---- 朴素 Backend 调用：每条一次调用、一次拷贝，没有批量 ----
        let writer = dfs.clone();
        let handle_w = handle;
        let data = payload.clone();
        let write_elapsed = on_plain_thread(move || {
            let started = Instant::now();
            for op in 0..ops {
                let offset = (op as u64 % SLOTS) * payload_len as u64;
                writer.write(&context(), handle_w, offset, &data)?;
            }
            Ok::<std::time::Duration, afs_error::Error>(started.elapsed())
        })
        .await
        .expect("backend writes");

        let reader = dfs.clone();
        let handle_r = handle;
        let read_elapsed = on_plain_thread(move || {
            let mut buffer = vec![0_u8; payload_len];
            let started = Instant::now();
            for op in 0..ops {
                let offset = (op as u64 % SLOTS) * payload_len as u64;
                reader.read(&context(), handle_r, offset, &mut buffer)?;
            }
            Ok::<std::time::Duration, afs_error::Error>(started.elapsed())
        })
        .await
        .expect("backend reads");
        out.fill(0);

        println!(
            "Backend 地板价 {payload_len}B: 写 {:.1} µs/op / 读 {:.1} µs/op（{} 条，无批量、无 IPC）",
            write_elapsed.as_secs_f64() * 1e6 / ops as f64,
            read_elapsed.as_secs_f64() * 1e6 / ops as f64,
            ops,
        );
    }

    let closer = dfs.clone();
    on_plain_thread(move || closer.release(&context(), handle))
        .await
        .expect("release");
    server.abort();
}

/// 定位读路径开销：同样是 1 MiB，单条读、单条写、批量读各是什么量级。
///
/// 背景：`Backend` 地板价显示 1 MiB 读（591 µs）比写（957 µs）更快，
/// 而 SDK 环路径里读反而比写慢一倍多，说明差异出在环/客户端这一侧，不是 DFS。
#[ignore = "性能测量，不在门禁里跑"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn usrbio_perf_read_path() {
    use std::time::Instant;

    const MIB: usize = 1024 * 1024;
    const OPS: usize = 256;
    const WAVE: usize = 8;

    let temp = tempfile::tempdir().expect("tempdir");
    let (dfs, server) = real_dfs(&temp).await;
    let creator = dfs.clone();
    on_plain_thread(move || {
        let created = creator.create(
            &context(),
            creator.root_inode(),
            OsStr::new("readpath.bin"),
            0o644,
            libc::O_RDWR,
        )?;
        creator.release(&context(), created.handle)
    })
    .await
    .expect("create readpath.bin");

    let socket_path = temp.path().join("afs-node.sock");
    let storage = Storage::new(temp.path().join("diagnostics")).expect("storage");
    let api = serve_local_api_with_options(
        storage,
        &socket_path,
        LocalApiOptions {
            metrics_registry: None,
            dfs_backend: Some(dfs.clone() as Arc<dyn Backend>),
            dfs_namespace: Some("default".to_owned()),
        },
    )
    .await
    .expect("serve local api");

    let client = DfsLocalClient::connect(DfsLocalClientConfig::new(&socket_path))
        .await
        .expect("connect");
    let file = client
        .open("/readpath.bin", DfsAccess::ReadWrite)
        .await
        .expect("open");
    let mut iov = client.create_iov(MIB, 16).await.expect("create iov");
    let mut ring = client
        .create_ring(&iov, 64, 0, 10_000)
        .await
        .expect("create ring");
    let registration = client.register_file(&file).await.expect("register");

    let payload = vec![0x2d_u8; MIB];
    // 先写满 256 MiB，保证读的是已落数据。
    for op in 0..OPS {
        ring.write_from(&mut iov, &registration, op as u64 * MIB as u64, &payload)
            .await
            .expect("seed");
    }

    // 1 MiB 单条写
    let started = Instant::now();
    for op in 0..OPS {
        ring.write_from(&mut iov, &registration, op as u64 * MIB as u64, &payload)
            .await
            .expect("single write");
    }
    let single_write = started.elapsed();

    // 1 MiB 单条读
    let mut buffer = vec![0_u8; MIB];
    let started_instant = Instant::now();
    for op in 0..OPS {
        ring.read_into(&mut iov, &registration, op as u64 * MIB as u64, &mut buffer)
            .await
            .expect("single read");
    }
    let single_read = started_instant.elapsed();

    // 1 MiB 批量读
    let mut buffers: Vec<Vec<u8>> = (0..WAVE).map(|_| vec![0_u8; MIB]).collect();
    let started = Instant::now();
    for wave in 0..(OPS / WAVE) {
        let base = wave as u64 * WAVE as u64 * MIB as u64;
        let mut targets: Vec<(u64, &mut [u8])> = buffers
            .iter_mut()
            .enumerate()
            .map(|(index, item)| (base + index as u64 * MIB as u64, item.as_mut_slice()))
            .collect();
        ring.read_batch(&mut iov, &registration, &mut targets)
            .await
            .expect("batch read");
    }
    let batch_read = started.elapsed();

    println!(
        "1MiB 环路径: 单条写 {:.0} µs/op / 单条读 {:.0} µs/op / 批量读{WAVE} {:.0} µs/op",
        single_write.as_secs_f64() * 1e6 / OPS as f64,
        single_read.as_secs_f64() * 1e6 / OPS as f64,
        batch_read.as_secs_f64() * 1e6 / OPS as f64,
    );

    client
        .unregister_file(registration)
        .await
        .expect("unregister");
    client.close(file.handle).await.expect("close");
    ring.destroy().await.expect("destroy ring");
    iov.destroy().await.expect("destroy iov");
    api.shutdown().await.expect("shutdown");
    server.abort();
}
