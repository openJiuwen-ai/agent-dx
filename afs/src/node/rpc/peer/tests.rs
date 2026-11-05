#[cfg(feature = "dfs")]
#[test]
fn replica_data_frames_share_the_staged_allocation_and_legacy_wire() {
    use afs_protocol::node_data::{
        DfsPutReplicaFrame, DfsPutReplicaHeader, dfs_put_replica_frame::Body,
    };
    use prost::Message;

    #[derive(Clone, PartialEq, prost::Message)]
    struct LegacyFrame {
        #[prost(oneof = "LegacyBody", tags = "1, 2")]
        body: Option<LegacyBody>,
    }

    #[derive(Clone, PartialEq, prost::Oneof)]
    enum LegacyBody {
        #[prost(message, tag = "1")]
        Header(DfsPutReplicaHeader),
        #[prost(bytes = "vec", tag = "2")]
        Data(Vec<u8>),
    }

    let payload: Vec<u8> = (0..150 * 1024).map(|index| (index % 251) as u8).collect();
    let staged = crate::node::chunk::StagedChunk::new(
        crate::dfs::OperationId::new("replica-shared-wire"),
        payload.clone(),
    );
    let start = staged.bytes().as_ptr() as usize;
    let mut offset = 0;
    let mut count = 0;
    for frame in replica_data_frames(staged.clone()) {
        let Some(Body::Data(data)) = &frame.body else {
            panic!("replica data iterator returned a control frame");
        };
        assert_eq!(data.as_ptr() as usize, start + offset);
        assert_eq!(
            data.len(),
            (payload.len() - offset).min(super::super::data::DFS_REPLICA_FRAME_BYTES)
        );
        assert_eq!(&data[..], &payload[offset..offset + data.len()]);
        let legacy = LegacyFrame {
            body: Some(LegacyBody::Data(data.to_vec())),
        };
        let old_wire = legacy.encode_to_vec();
        let new_wire = frame.encode_to_vec();
        assert_eq!(new_wire, old_wire);
        assert_eq!(
            DfsPutReplicaFrame::decode(old_wire.as_slice()).unwrap(),
            frame
        );
        assert_eq!(LegacyFrame::decode(new_wire.as_slice()).unwrap(), legacy);
        offset += data.len();
        count += 1;
    }
    assert_eq!(count, 3);
    assert_eq!(offset, payload.len());
}

#[cfg(feature = "dfs")]
#[test]
fn cancelled_replica_frames_retain_allocation_until_last_owner() {
    use crate::node::chunk::{ChunkBuilder, StagedChunk, WriteBufferPool};
    use afs_protocol::node_data::dfs_put_replica_frame::Body;

    let capacity = 3 * super::super::data::DFS_REPLICA_FRAME_BYTES;
    for stop_pool in [false, true] {
        let pool = WriteBufferPool::new(capacity);
        let mut builder = ChunkBuilder::with_pool(pool.clone(), capacity).unwrap();
        let payload = vec![37; capacity - 17];
        builder.write_at(0, &payload).unwrap();
        let frozen = builder.freeze();
        let generation = frozen.allocation_id();
        let pointer = frozen.as_ptr();
        let staged =
            StagedChunk::from_shared(crate::dfs::OperationId::new("replica-frame-lease"), frozen);
        let mut frames = replica_data_frames(staged.clone());
        let first = frames.next().unwrap();
        let second = frames.next().unwrap();
        // Cancel the producer with an unsent tail and release the RPC's
        // original/ack owners. Queued or slow frame holders remain alive.
        drop(frames);
        drop(staged);
        let last_holder = second.clone();
        drop(second);
        assert_eq!(pool.snapshot().live_bytes, capacity);
        assert_eq!(pool.snapshot().idle_bytes, 0);
        let Some(Body::Data(data)) = &first.body else {
            panic!("missing data frame")
        };
        assert_eq!(data.as_ptr(), pointer);
        assert_eq!(&data[..], &payload[..data.len()]);
        drop(first);
        assert_eq!(pool.snapshot().live_bytes, capacity);
        if stop_pool {
            pool.stop();
        }
        drop(last_holder);
        assert_eq!(pool.snapshot().live_bytes, 0);
        if stop_pool {
            assert_eq!(pool.snapshot().idle_bytes, 0);
            assert_eq!(pool.snapshot().released_bytes, capacity as u64);
        } else {
            assert_eq!(pool.snapshot().idle_bytes, capacity);
            let reused = ChunkBuilder::with_pool(pool.clone(), capacity).unwrap();
            assert_eq!(reused.data_ptr(), pointer);
            assert_ne!(reused.allocation_id(), generation);
            assert_eq!(pool.snapshot().reuses, 1);
        }
    }
}

#[test]
#[cfg(feature = "rdma")]
fn poisoning_preserves_failure_category() {
    for code in [
        afs_error::CLIENT_PROTOCOL_VIOLATION,
        afs_error::NODE_TRANSFER_UNAVAILABLE,
    ] {
        let flag = std::sync::atomic::AtomicBool::new(false);
        let error = super::poison(&flag, code, "diagnostic");
        assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(error.code(), code);
    }
}
use super::*;
#[cfg(feature = "ownerfs")]
use crate::node::rpc::data::{
    OwnerFilesHandler, PeerAuthenticator, make_owner_files_server_with_handler,
    make_owner_files_server_with_handler_and_metrics,
};
#[cfg(feature = "ownerfs")]
use crate::node::vfs::ownerfs::{
    files::{FileIdentity, RemoteFile},
    remote::RemoteFiles,
    root::{RootGrant, RootId, RootRight},
};
use afs_protocol::node_data::{
    DataReadReply, DataWriteReply,
    node_data_server::{NodeData, NodeDataServer},
};
#[cfg(feature = "ownerfs")]
use afs_protocol::node_data::{
    OwnerReadReply, OwnerReadRequest, OwnerReleaseReply, OwnerWriteReply, OwnerWriteRequest,
};
#[cfg(feature = "ownerfs")]
use std::sync::atomic::{AtomicUsize as TestAtomicUsize, Ordering as TestOrdering};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, transport::Server};

#[cfg(feature = "dfs")]
pub(super) type FetchFixtureObjects = std::collections::BTreeMap<String, (ChunkObject, Vec<u8>)>;

#[cfg(feature = "dfs")]
#[derive(Clone)]
pub(super) struct DiskStreamFixture {
    pub(super) chunk: ChunkObject,
    pub(super) body: Arc<Vec<u8>>,
    pub(super) mode: u8,
    pub(super) calls: Arc<std::sync::atomic::AtomicUsize>,
    pub(super) fetch_objects: Option<Arc<FetchFixtureObjects>>,
    pub(super) fetch_gate: Option<(String, Arc<FetchRpcGate>)>,
}

#[cfg(feature = "dfs")]
#[derive(Default)]
pub(super) struct FetchRpcGate {
    entered: std::sync::atomic::AtomicUsize,
    release: std::sync::atomic::AtomicBool,
    changed: tokio::sync::Notify,
}

#[cfg(feature = "dfs")]
fn fetch_fixture_frames(
    request: &DfsReadRangesRequest,
    objects: &FetchFixtureObjects,
) -> Result<Vec<afs_protocol::node_data::DfsReadRangesFrame>, Status> {
    use afs_protocol::node_data::{
        DfsReadRangesCompletion, DfsReadRangesFrame, DfsReadRangesHeader,
    };
    let mut frames = Vec::new();
    for operation in &request.operations {
        let (chunk, body) = objects
            .get(&operation.chunk_id)
            .ok_or_else(|| Status::not_found("fixture object"))?;
        let start = operation.chunk_offset as usize;
        let end = start + operation.length as usize;
        let range = body
            .get(start..end)
            .ok_or_else(|| Status::invalid_argument("fixture range"))?;
        let full = start == 0 && operation.length == chunk.length;
        frames.push(DfsReadRangesFrame {
            body: Some(dfs_read_ranges_frame::Body::Header(DfsReadRangesHeader {
                crc32c_table: Default::default(),
                read_id: request.read_id.clone(),
                attempt_id: request.attempt_id.clone(),
                operation_index: operation.operation_index,
                chunk_id: operation.chunk_id.clone(),
                chunk_offset: operation.chunk_offset,
                length: operation.length,
                source_copy_id: operation.source_copy_id.clone(),
                blake3_outboard: if full && request.include_blake3_outboard {
                    wi6_encoded_outboard(body).into()
                } else {
                    Default::default()
                },
            })),
        });
        for data in range.chunks(STREAM_DATA_BYTES) {
            frames.push(DfsReadRangesFrame {
                body: Some(dfs_read_ranges_frame::Body::Data(data.to_vec().into())),
            });
        }
        frames.push(DfsReadRangesFrame {
            body: Some(dfs_read_ranges_frame::Body::Completion(
                DfsReadRangesCompletion {
                    crc32c_table: Default::default(),
                    read_id: request.read_id.clone(),
                    attempt_id: request.attempt_id.clone(),
                    operation_index: operation.operation_index,
                    source_copy_id: operation.source_copy_id.clone(),
                    transferred_bytes: operation.length,
                    range_checksum: blake3::hash(range).as_bytes().to_vec(),
                    range_checksum_algorithm: afs_protocol::node_data::DfsDigestAlgorithm::Blake3
                        as i32,
                },
            )),
        });
    }
    Ok(frames)
}

#[cfg(feature = "dfs")]
#[tonic::async_trait]
impl afs_protocol::node_data::dfs_chunks_server::DfsChunks for DiskStreamFixture {
    type ReadRangesStream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<
                    Item = Result<afs_protocol::node_data::DfsReadRangesFrame, Status>,
                > + Send,
        >,
    >;
    async fn put_replica_stream(
        &self,
        _request: Request<tonic::Streaming<afs_protocol::node_data::DfsPutReplicaFrame>>,
    ) -> Result<Response<afs_protocol::node_data::DfsPutReplicaReply>, Status> {
        Err(Status::unimplemented("unused fixture RPC"))
    }
    async fn put_replica_rdma(
        &self,
        _request: Request<afs_protocol::node_data::DfsPutReplicaRdmaRequest>,
    ) -> Result<Response<afs_protocol::node_data::DfsPutReplicaReply>, Status> {
        Err(Status::unimplemented("unused fixture RPC"))
    }
    async fn confirm_replica(
        &self,
        _request: Request<afs_protocol::node_data::DfsConfirmReplicaRequest>,
    ) -> Result<Response<afs_protocol::node_data::DfsReplicaAck>, Status> {
        Err(Status::unimplemented("unused fixture RPC"))
    }
    async fn negotiate_rdma(
        &self,
        _request: Request<afs_protocol::node_data::DfsNegotiateRdmaRequest>,
    ) -> Result<Response<afs_protocol::node_data::DfsNegotiateRdmaReply>, Status> {
        Err(Status::unimplemented("unused fixture RPC"))
    }
    async fn close_rdma(
        &self,
        _request: Request<afs_protocol::node_data::DfsCloseRdmaRequest>,
    ) -> Result<Response<afs_protocol::node_data::DfsCloseRdmaReply>, Status> {
        Err(Status::unimplemented("unused fixture RPC"))
    }
    async fn read_ranges_rdma(
        &self,
        _request: Request<afs_protocol::node_data::DfsReadRangesRdmaRequest>,
    ) -> Result<Response<afs_protocol::node_data::DfsReadRangesRdmaReply>, Status> {
        Err(Status::unimplemented("unused fixture RPC"))
    }
    async fn read_ranges(
        &self,
        request: Request<DfsReadRangesRequest>,
    ) -> Result<Response<Self::ReadRangesStream>, Status> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let request = request.into_inner();
        if let Some(objects) = &self.fetch_objects {
            if let Some((target, gate)) = &self.fetch_gate
                && request.operations.iter().any(|operation| {
                    (target.is_empty() || &operation.chunk_id == target)
                        && objects.get(&operation.chunk_id).is_some_and(|(chunk, _)| {
                            operation.chunk_offset == 0 && operation.length == chunk.length
                        })
                })
            {
                gate.entered
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                gate.changed.notify_waiters();
                loop {
                    let changed = gate.changed.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    if gate.release.load(std::sync::atomic::Ordering::Acquire) {
                        break;
                    }
                    changed.await;
                }
            }
            let frames = fetch_fixture_frames(&request, objects)?;
            let frames: Vec<_> = frames.into_iter().map(Ok).collect();
            let response =
                Response::new(Box::pin(tokio_stream::iter(frames)) as Self::ReadRangesStream);
            return Ok(response);
        }

        let mut body = self.body.as_ref().clone();
        if matches!(self.mode, 3 | 11) {
            body[0] ^= 1;
        }
        let (_, mut frames) = peer_disk_test_frames(&self.chunk, &body);
        for frame in &mut frames {
            match &mut frame.body {
                Some(dfs_read_ranges_frame::Body::Header(header)) => {
                    if self.mode >= 9 {
                        header.blake3_outboard = wi6_encoded_outboard(&self.body).into();
                    }
                    header.read_id.clone_from(&request.read_id);
                    header.attempt_id.clone_from(&request.attempt_id);
                    header
                        .source_copy_id
                        .clone_from(&request.operations[0].source_copy_id);
                }
                Some(dfs_read_ranges_frame::Body::Completion(completion)) => {
                    if self.mode == 14 {
                        let mut checksum = completion.range_checksum.to_vec();
                        checksum[0] ^= 1;
                        completion.range_checksum = checksum;
                    }
                    completion.read_id.clone_from(&request.read_id);
                    completion.attempt_id.clone_from(&request.attempt_id);
                    completion
                        .source_copy_id
                        .clone_from(&request.operations[0].source_copy_id);
                }
                _ => {}
            }
        }
        if self.mode == 6 {
            frames.push(afs_protocol::node_data::DfsReadRangesFrame {
                body: Some(dfs_read_ranges_frame::Body::Data(prost::bytes::Bytes::new())),
            });
        } else if self.mode == 7 {
            frames.insert(1, frames[0].clone());
        } else if self.mode == 8 {
            frames[1].body = Some(dfs_read_ranges_frame::Body::Data(
                vec![37; STREAM_DATA_BYTES + 1].into(),
            ));
        }
        if self.mode == 13 {
            frames.retain(|frame| {
                !matches!(
                    &frame.body,
                    Some(dfs_read_ranges_frame::Body::Completion(_))
                )
            });
        }
        let mut frames: Vec<_> = frames.into_iter().map(Ok).collect();
        if matches!(self.mode, 2 | 12 | 16) {
            frames.push(Err(Status::unavailable("late status after completion")));
        }
        Ok(Response::new(Box::pin(tokio_stream::iter(frames))))
    }
}

#[cfg(feature = "dfs")]
pub(super) fn disk_stream_batch(
    chunk: &ChunkObject,
    endpoint: String,
) -> crate::node::read::engine::PeerReadBatch {
    use crate::dfs::*;
    crate::node::read::engine::PeerReadBatch {
        file_version_id: FileVersionId::new("version"),
        layout_root_id: LayoutRootId::new("layout"),
        operations: vec![crate::node::read::engine::ResolvedReadOp {
            op: crate::node::read::engine::ChunkReadOp {
                chunk_id: chunk.id.clone(),
                chunk_offset: 0,
                length: chunk.length,
                output_offset: 0,
            },
            source: SourceCandidate {
                chunk_id: chunk.id.clone(),
                copy_id: CopyId::new("copy"),
                role: CopyRole::DurableReplica,
                state: CopyState::Ready,
                location: CopyLocation::Node {
                    node_id: "server".into(),
                    node_epoch: 1,
                    device_id: "local-0".into(),
                    device_epoch: 1,
                    catalog_revision: 1,
                },
                data_endpoint: Some(endpoint),
                load_hint: 0,
                read_grant: DfsReadGrant {
                    namespace_id: NamespaceId::new("test"),
                    file_version_id: FileVersionId::new("version"),
                    layout_root_id: LayoutRootId::new("layout"),
                    caller_node_id: "reader".into(),
                    caller_node_epoch: 1,
                    expires_at_unix_ms: u64::MAX,
                    fence: 1,
                    token: "test-token".into(),
                },
            },
        }],
    }
}

#[cfg(feature = "dfs")]
struct FetchRpcCase {
    _temp: tempfile::TempDir,
    engine: Arc<crate::node::read::engine::DfsReadEngine>,
    cache: Arc<crate::node::read::peer_cache::DiskPeerCache>,
    chunks: Vec<ChunkObject>,
    gate: Arc<FetchRpcGate>,
    calls: Arc<std::sync::atomic::AtomicUsize>,
    source_calls: Arc<std::sync::atomic::AtomicUsize>,
    registry: afs_metrics::Registry,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

#[cfg(feature = "dfs")]
async fn fetch_rpc_case(readahead: u64, gate_all: bool) -> FetchRpcCase {
    use crate::node::read::engine::{DfsReadConfig, DfsReadEngine, ReadSourceProvider};
    struct Sources {
        reply: crate::dfs::DfsChunkSourcesReply,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl ReadSourceProvider for Sources {
        fn sources_for(
            &self,
            request: crate::dfs::DfsChunkSourcesRequest,
        ) -> afs_error::Result<crate::dfs::DfsChunkSourcesReply> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(crate::dfs::DfsChunkSourcesReply {
                revision: 1,
                chunks: self
                    .reply
                    .chunks
                    .iter()
                    .filter(|chunk| request.chunk_ids.contains(&chunk.chunk_id))
                    .cloned()
                    .collect(),
            })
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let objects: std::collections::BTreeMap<_, _> = [17, 29]
        .into_iter()
        .map(|byte| {
            let staged = crate::node::chunk::StagedChunk::new(
                crate::dfs::OperationId::new(format!("r7-rpc-{byte}")),
                vec![byte; 64 * 1024],
            );
            (
                staged.chunk.id.0.clone(),
                (staged.chunk.clone(), staged.bytes().to_vec()),
            )
        })
        .collect();
    let chunks: Vec<_> = [17, 29]
        .into_iter()
        .map(|byte| {
            objects
                .values()
                .find(|(_, body)| body[0] == byte)
                .unwrap()
                .0
                .clone()
        })
        .collect();
    let objects = Arc::new(objects);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let gate = Arc::new(FetchRpcGate::default());
    let service = DiskStreamFixture {
        chunk: chunks[0].clone(),
        body: Arc::new(objects[&chunks[0].id.0].1.clone()),
        mode: 0,
        calls: calls.clone(),
        fetch_objects: Some(objects),
        fetch_gate: Some((
            if gate_all {
                String::new()
            } else {
                chunks[1].id.0.clone()
            },
            gate.clone(),
        )),
    };
    let server = tokio::spawn(
        Server::builder()
            .add_service(afs_protocol::node_data::dfs_chunks_server::DfsChunksServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let registry = afs_metrics::Registry::new();
    let metrics = ReadWorkMetrics::register(&registry).unwrap();
    let pool = Arc::new(
        PeerConnectionPool::new(
            afs_transport::GrpcConfig::default(),
            afs_transport::TlsConfig::Disabled,
            1,
        )
        .unwrap()
        .with_read_work_metrics(metrics),
    );
    let transfer = Arc::new(GrpcChunkTransfer::new(pool, Duration::from_secs(5)).unwrap());
    let source_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let sources = Sources {
        calls: source_calls.clone(),
        reply: crate::dfs::DfsChunkSourcesReply {
            revision: 1,
            chunks: chunks
                .iter()
                .map(|chunk| crate::dfs::ChunkSources {
                    chunk_id: chunk.id.clone(),
                    chunk: Some(chunk.clone()),
                    allowed_ranges: vec![(0, chunk.length)],
                    sources: vec![
                        disk_stream_batch(chunk, endpoint.clone()).operations[0]
                            .source
                            .clone(),
                    ],
                })
                .collect(),
        },
    };
    let cache = Arc::new(
        crate::node::read::peer_cache::DiskPeerCache::open(
            crate::node::read::peer_cache::PeerCacheOptions {
                directory: temp.path().join("cache"),
                capacity_bytes: 1024 * 1024,
                namespace_id: crate::dfs::NamespaceId::new("test"),
            },
        )
        .unwrap(),
    );
    let engine = Arc::new(
        DfsReadEngine::new(
            crate::dfs::NamespaceId::new("test"),
            "reader".into(),
            Arc::new(
                crate::node::chunk::LocalChunkStore::open(temp.path().join("local"), "reader")
                    .unwrap(),
            ),
            Arc::new(sources),
            transfer,
            DfsReadConfig {
                readahead_max_bytes: readahead,
                ..DfsReadConfig::default()
            },
        )
        .with_read_authority(Arc::new(
            crate::node::read::engine::LocalReadAuthority::new(
                1,
                std::time::Instant::now() + Duration::from_secs(30),
            ),
        ))
        .with_peer_cache(cache.clone()),
    );
    FetchRpcCase {
        _temp: temp,
        engine,
        cache,
        chunks,
        gate,
        calls,
        source_calls,
        registry,
        server,
    }
}

#[cfg(feature = "dfs")]
fn fetch_case_batch(case: &FetchRpcCase, cross: bool) -> crate::node::read::engine::ReadBatch {
    use crate::node::read::engine::{ChunkReadOp, ReadBatch};
    ReadBatch {
        file_version_id: Some(crate::dfs::FileVersionId::new("version")),
        layout_root_id: crate::dfs::LayoutRootId::new("layout"),
        lookahead: if cross {
            Vec::new()
        } else {
            vec![case.chunks[1].id.clone()]
        },
        ops: if cross {
            case.chunks
                .iter()
                .enumerate()
                .map(|(index, chunk)| ChunkReadOp {
                    chunk_id: chunk.id.clone(),
                    chunk_offset: if index == 0 { chunk.length - 8 } else { 0 },
                    length: 8,
                    output_offset: index * 8,
                })
                .collect()
        } else {
            vec![ChunkReadOp {
                chunk_id: case.chunks[0].id.clone(),
                chunk_offset: 0,
                length: 8,
                output_offset: 0,
            }]
        },
    }
}

#[cfg(feature = "dfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unified_actual_rpc_cross_object_cache_fill_hit_has_no_sync_fallback() {
    let mut case = fetch_rpc_case(0, true).await;
    case.engine = Arc::new(
        Arc::try_unwrap(case.engine)
            .ok()
            .unwrap()
            .with_read_pipeline(crate::node::read::admission::ReadPipelineLimits::default())
            .unwrap(),
    );
    let batch = fetch_case_batch(&case, true);
    let engine = case.engine.clone();
    let first = batch.clone();
    let reading = tokio::spawn(async move {
        engine
            .read_native_unified_async(
                first,
                16,
                std::time::Instant::now() + Duration::from_secs(3),
                Arc::new(()),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while case.gate.entered.load(std::sync::atomic::Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    case.gate
        .release
        .store(true, std::sync::atomic::Ordering::Release);
    case.gate.changed.notify_waiters();
    let output = reading.await.unwrap().unwrap();
    assert_eq!(&output[..8], &[17; 8]);
    assert_eq!(&output[8..], &[29; 8]);
    drop(output);
    let output = case
        .engine
        .read_native_unified_async(
            batch,
            16,
            std::time::Instant::now() + Duration::from_secs(3),
            Arc::new(()),
        )
        .await
        .unwrap();
    assert_eq!(&output[..8], &[17; 8]);
    assert_eq!(&output[8..], &[29; 8]);
    drop(output);
    assert_eq!(case.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    case.engine
        .object_fetcher()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    case.server.abort();
    let _ = case.server.await;
}

#[cfg(feature = "dfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unified_actual_rpc_cancelled_parent_keeps_descendant_lifetime_until_retirement() {
    struct Owner(Arc<std::sync::atomic::AtomicUsize>);
    impl Drop for Owner {
        fn drop(&mut self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let mut case = fetch_rpc_case(0, true).await;
    case.engine = Arc::new(
        Arc::try_unwrap(case.engine)
            .ok()
            .unwrap()
            .with_read_pipeline(crate::node::read::admission::ReadPipelineLimits::default())
            .unwrap(),
    );
    let batch = fetch_case_batch(&case, true);
    let engine = case.engine.clone();
    let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let lifetime = Arc::new(Owner(dropped.clone()));
    let reading = tokio::spawn(async move {
        engine
            .read_native_unified_async(
                batch,
                16,
                std::time::Instant::now() + Duration::from_secs(3),
                lifetime,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while case.gate.entered.load(std::sync::atomic::Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    reading.abort();
    assert!(reading.await.unwrap_err().is_cancelled());
    assert_eq!(
        dropped.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "actual children still own the original dispatch lifetime"
    );
    case.gate
        .release
        .store(true, std::sync::atomic::Ordering::Release);
    case.gate.changed.notify_waiters();
    tokio::time::timeout(Duration::from_secs(3), async {
        while dropped.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 1);
    case.engine
        .object_fetcher()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    case.server.abort();
    let _ = case.server.await;
}

#[cfg(feature = "dfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r7_actual_rpc_cross_object_read_overlaps_and_keeps_output_atomic() {
    let mut case = fetch_rpc_case(0, true).await;
    let batch = fetch_case_batch(&case, true);
    let engine = case.engine.clone();
    let reading = tokio::task::spawn_blocking(move || {
        let mut out = [0; 16];
        engine
            .read_batch_native_into_private(&batch, &mut out)
            .map(|()| out)
    });
    let overlapped = tokio::time::timeout(Duration::from_secs(3), async {
        while case.gate.entered.load(std::sync::atomic::Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok();
    let while_active = afs_metrics::encode_text(&case.registry).unwrap();
    case.gate
        .release
        .store(true, std::sync::atomic::Ordering::Release);
    case.gate.changed.notify_waiters();
    let output = reading.await.unwrap().unwrap();
    assert!(
        overlapped,
        "both full-object RPCs must enter before either is released"
    );
    assert!(
        while_active.contains("afs_dfs_peer_disk_cache_stream_reserved_bytes 4194304\n"),
        "{while_active}"
    );
    assert!(
        while_active.contains("afs_dfs_peer_disk_cache_stream_frame_bytes 524288\n"),
        "{while_active}"
    );
    assert_eq!(&output[..8], &[17; 8]);
    assert_eq!(&output[8..], &[29; 8]);
    assert_eq!(case.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(case.cache.stats().unwrap().entries, 2);
    case.engine
        .object_fetcher()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    case.server.abort();
    let _ = (&mut case.server).await;
}

#[cfg(feature = "dfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r7_actual_rpc_prefetch_is_sink_only_and_cancel_really_joins() {
    for mode in ["success", "global", "scope", "expiry"] {
        let mut case = fetch_rpc_case(8 * 1024 * 1024, false).await;
        let batch = fetch_case_batch(&case, false);
        let engine = case.engine.clone();
        let read_batch = batch.clone();
        assert_eq!(
            tokio::task::spawn_blocking(move || engine.read_batch_native_owned(&read_batch, 8))
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            [17; 8]
        );
        let engine = case.engine.clone();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let scope_valid = valid.clone();
        let original_deadline = std::time::Instant::now() + Duration::from_millis(500);
        tokio::task::spawn_blocking(move || {
            engine.prefetch_cached(
                &batch,
                1,
                Arc::new(move || {
                    if !scope_valid.load(std::sync::atomic::Ordering::Acquire)
                        || (mode == "expiry" && std::time::Instant::now() >= original_deadline)
                    {
                        return Err(afs_error::Error::coded(
                            afs_error::IO_UNAVAILABLE,
                            "test handle scope or original deadline retired",
                        ));
                    }
                    Ok(())
                }),
            )
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while case.gate.entered.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        if mode == "global" {
            case.engine.object_fetcher().unwrap().close();
        } else if mode == "success" {
            case.gate
                .release
                .store(true, std::sync::atomic::Ordering::Release);
            case.gate.changed.notify_waiters();
            tokio::time::timeout(Duration::from_secs(3), async {
                while case.cache.stats().unwrap().entries < 2 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
        }
        if mode == "scope" {
            valid.store(false, std::sync::atomic::Ordering::Release);
        }
        // Do not call global shutdown to make scope/expiry cancellation
        // pass. The quiet server remains gated until this job really ends.
        let retired_without_shutdown = if matches!(mode, "scope" | "expiry") {
            tokio::time::timeout(Duration::from_secs(2), async {
                while case.cache.has_fill_flight_for_test() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .is_ok()
        } else {
            true
        };
        if !retired_without_shutdown {
            case.gate
                .release
                .store(true, std::sync::atomic::Ordering::Release);
            case.gate.changed.notify_waiters();
        }
        tokio::time::timeout(
            Duration::from_secs(2),
            case.engine.object_fetcher().unwrap().shutdown(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            case.cache.stats().unwrap().entries,
            if mode == "success" { 2 } else { 1 }
        );
        assert_eq!(case.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(
            case.source_calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "prefetch must use the first R-1 lookup only"
        );
        let text = afs_metrics::encode_text(&case.registry).unwrap();
        assert!(
            text.contains("afs_dfs_peer_disk_cache_stream_reserved_bytes 0\n"),
            "{text}"
        );
        assert!(
            text.contains("afs_dfs_peer_disk_cache_stream_frame_bytes 0\n"),
            "{text}"
        );
        assert!(
            text.contains("afs_dfs_fetch_resources{resource=\"jobs\"} 0\n"),
            "{text}"
        );
        case.gate
            .release
            .store(true, std::sync::atomic::Ordering::Release);
        case.gate.changed.notify_waiters();
        case.server.abort();
        let _ = (&mut case.server).await;
        assert!(
            retired_without_shutdown,
            "{mode}: quiet RPC must join before global shutdown"
        );
    }
}

#[cfg(feature = "dfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_stream_actual_rpc_seals_only_after_eof_and_drains_on_failure() {
    use crate::node::{
        read::engine::ChunkTransfer,
        read::peer_cache::{DiskPeerCache, PeerCacheOptions},
    };
    // 9/10: peer proof, including disabled disk cache; 11: wrong body;
    // 12: late transport error; 13: missing Completion; 14: false wire root;
    // 15/16: exhausted proof budget, with success/late transport failure.
    for mode in 0..17 {
        let success = matches!(mode, 0 | 1 | 5 | 9 | 10 | 15);
        let consumer_root = mode < 15;
        let staged = crate::node::chunk::StagedChunk::new(
            crate::dfs::OperationId::new("actual-stream"),
            vec![
                37;
                if mode == 5 {
                    4 * 1024 * 1024 - 17
                } else if mode == 4 {
                    1024 * 1024
                } else {
                    512 * 1024
                }
            ],
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let service = DiskStreamFixture {
            chunk: staged.chunk.clone(),
            body: Arc::new(staged.bytes().to_vec()),
            mode,
            calls: calls.clone(),
            fetch_objects: None,
            fetch_gate: None,
        };
        let server = tokio::spawn(
            Server::builder()
                .add_service(
                    afs_protocol::node_data::dfs_chunks_server::DfsChunksServer::new(service),
                )
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry)
            .unwrap()
            .with_read_diagnostics(true);
        let pool = PeerConnectionPool::new(
            afs_transport::GrpcConfig::default(),
            afs_transport::TlsConfig::Disabled,
            1,
        )
        .unwrap()
        .with_read_work_metrics(metrics.clone());
        let transfer = GrpcChunkTransfer::new(
            Arc::new(pool),
            if mode == 4 {
                Duration::from_millis(500)
            } else {
                Duration::from_secs(5)
            },
        )
        .unwrap();
        let batch = disk_stream_batch(&staged.chunk, endpoint);
        tokio::task::spawn_blocking(move || {
            assert!(transfer.supports_verified_object_sink());
            let directory = tempfile::tempdir().unwrap();
            let cache = DiskPeerCache::open_with_outboard_budget(
                PeerCacheOptions {
                    directory: directory.path().join("cache"),
                    capacity_bytes: if matches!(mode, 1 | 10) {
                        4096
                    } else {
                        8 * 1024 * 1024
                    },
                    namespace_id: crate::dfs::NamespaceId::new("test"),
                },
                OutboardBudget::new(if consumer_root { 1024 * 1024 } else { 0 }),
            )
            .unwrap();
            let mut out = vec![0; 32 * 1024];
            {
                let mut operation = cache.operation().unwrap();
                let mut sink = operation.begin_stream(&staged.chunk).unwrap();
                if mode == 4 {
                    sink.delay_writes_for_test(Duration::from_millis(700));
                }
                let result = transfer.read_verified_object_to_sink(
                    &batch,
                    &staged.chunk,
                    13,
                    &mut out,
                    &mut sink,
                );
                if success {
                    let proof = result.unwrap().unwrap();
                    assert!(proof.private_output().iter().all(|byte| *byte == 37));
                    // A zero metadata ledger cannot retain even the cache
                    // index/entry controls. The same RPC still completes
                    // full-root verification and private range delivery.
                    assert_eq!(
                        sink.commit(proof).unwrap(),
                        !matches!(mode, 1 | 10 | 15),
                        "optional cache installation, mode {mode}"
                    );
                } else {
                    assert!(
                        result.is_err(),
                        "invalid/truncated/deadline stream must not seal"
                    );
                }
            }
            assert_eq!(
                cache.stats().unwrap().entries,
                usize::from(matches!(mode, 0 | 5 | 9)),
                "only budget-admitted healthy entries are retained, mode {mode}"
            );
            if mode >= 15 {
                assert_eq!(cache.proof_budget().used(), 0);
                assert_eq!(cache.proof_budget().resident_used(), 0);
            }
            assert_eq!(
                calls.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "cache rejection must not repeat RPC"
            );
            let diagnostics = metrics.diagnostics_snapshot().unwrap();
            assert_eq!(diagnostics.records.len(), 1);
            assert_eq!(diagnostics.overflow, 0);
            assert_eq!(diagnostics.clock_failures, 0);
            let record = &diagnostics.records[0];
            assert_eq!(record.stage, ReadDiagnosticStage::PeerObject);
            assert!(record.completed);
            assert_eq!(record.success, success, "mode {mode}");
            assert!(record.end_ns >= record.start_ns);
            assert!(record.consumer_before_body_ns <= record.consumer_ns);
            if record.success {
                assert!(record.first_data_ns >= record.start_ns);
                assert!(record.body_end_ns >= record.first_data_ns);
                assert!(record.body_end_ns >= record.start_ns);
                assert!(record.eof_ns >= record.body_end_ns);
                assert!(record.end_ns >= record.eof_ns);
                assert!(record.consumer_calls >= 3);
                let work = record.consumer_work;
                let blocks = staged.bytes().len().div_ceil(STREAM_FRAME_BACKING_BYTES) as u64;
                assert_eq!(work.sink_write.calls, blocks);
                assert_eq!(work.sink_write.bytes, staged.bytes().len() as u64);
                assert_eq!(work.range_extract.bytes, out.len() as u64);
                assert_eq!(
                    work.root_blake3.calls, 0,
                    "plain consumer hash is absent; the selected root owner authenticates the body"
                );
                assert_eq!(
                    work.root_blake3.bytes, 0,
                    "outboard work owns the consumer root without another plain hasher"
                );
                assert_eq!(
                    record.producer_root_work.calls,
                    if consumer_root { 0 } else { blocks + 1 }
                );
                assert_eq!(
                    record.producer_root_work.bytes,
                    if consumer_root {
                        0
                    } else {
                        staged.bytes().len() as u64
                    }
                );
                assert_eq!(record.interval_overflow, 0);
                assert!(
                    record.root_segment_overlap_ns.is_none(),
                    "one root owner leaves no duplicate producer/consumer root overlap"
                );
                assert_eq!(
                    work.segment_blake3.bytes,
                    if consumer_root {
                        staged.bytes().len() as u64
                    } else {
                        0
                    }
                );
                assert_eq!(
                    work.segment_blake3.calls,
                    if consumer_root { blocks + 1 } else { 0 }
                );
                let measured = work.sink_write.wall_ns
                    + work.range_extract.wall_ns
                    + work.root_blake3.wall_ns
                    + work.segment_blake3.wall_ns;
                assert!(
                    measured <= record.consumer_ns,
                    "exclusive children stay inside consumer parent"
                );
            }
            let families = registry.gather();
            if success {
                let events = families
                    .iter()
                    .find(|family| family.name() == "afs_dfs_peer_read_events_total")
                    .unwrap();
                let event_count = |event: &str| {
                    events
                        .get_metric()
                        .iter()
                        .filter(|metric| {
                            metric
                                .get_label()
                                .iter()
                                .any(|label| label.name() == "event" && label.value() == event)
                        })
                        .map(|metric| metric.get_counter().get_value())
                        .sum::<f64>()
                };
                assert_eq!(
                    event_count("root_fallback"),
                    if consumer_root { 0.0 } else { 1.0 }
                );
                assert_eq!(
                    event_count("stream_frame"),
                    (staged.bytes().len().div_ceil(STREAM_DATA_BYTES) + 2) as f64,
                    "wire frames stay unchanged"
                );
                let blocks = staged.bytes().len().div_ceil(STREAM_FRAME_BACKING_BYTES) as f64;
                assert_eq!(
                    event_count("stream_block_handoff"),
                    blocks,
                    "wire data frames must use fixed-block handoffs"
                );
                assert_eq!(
                    event_count("stream_block_write"),
                    blocks,
                    "one sink entry per handed-off block, even when cache is disabled"
                );
                assert_eq!(
                    event_count("stream_block_alloc"),
                    2.0,
                    "only two reusable payload blocks per operation"
                );
                let copies = families
                    .iter()
                    .find(|family| family.name() == "afs_dfs_read_copy_bytes_total")
                    .unwrap();
                let copied = copies
                    .get_metric()
                    .iter()
                    .filter(|metric| {
                        metric.get_label().iter().any(|label| {
                            label.name() == "stage" && label.value() == "peer_frame_to_block"
                        })
                    })
                    .map(|metric| metric.get_counter().get_value())
                    .sum::<f64>();
                assert_eq!(
                    copied,
                    staged.bytes().len() as f64,
                    "exactly one wire-to-block payload copy"
                );
            }
            let peak = families
                .iter()
                .find(|family| family.name() == "afs_dfs_peer_disk_cache_stream_frame_peak_bytes")
                .unwrap();
            assert_eq!(
                peak.get_metric()[0].get_gauge().get_value(),
                (2 * STREAM_FRAME_BACKING_BYTES) as f64,
                "both actual fixed allocations must be charged"
            );
            let current = families
                .iter()
                .find(|family| family.name() == "afs_dfs_peer_disk_cache_stream_frame_bytes")
                .unwrap();
            assert_eq!(
                current.get_metric()[0].get_gauge().get_value(),
                0.0,
                "producer and queued frame leases must drain before return"
            );
        })
        .await
        .unwrap();
        server.abort();
        let _ = server.await;
    }
}

#[cfg(feature = "dfs")]
#[test]
fn peer_stream_joined_root_receipt_requires_completion_meta_and_sink_identity() {
    use crate::node::read::peer_cache::{DiskPeerCache, PeerCacheOptions};
    for consumer_tree in [false, true] {
        for mode in 0..6 {
            let staged = crate::node::chunk::StagedChunk::new(
                crate::dfs::OperationId::new("joined-root"),
                vec![37; 512 * 1024],
            );
            let directory = tempfile::tempdir().unwrap();
            let open_cache = |name| {
                DiskPeerCache::open(PeerCacheOptions {
                    directory: directory.path().join(name),
                    capacity_bytes: 8 * 1024 * 1024,
                    namespace_id: crate::dfs::NamespaceId::new("test"),
                })
                .unwrap()
            };
            let cache = open_cache("cache");
            let other_cache = open_cache("other");
            let mut operation = cache.operation().unwrap();
            let mut other_operation = other_cache.operation().unwrap();
            let mut sink = operation.begin_stream(&staged.chunk).unwrap();
            let proof_budget = sink.proof_budget();
            let proof_baseline = proof_budget.used();
            let other_sink = other_operation.begin_stream(&staged.chunk).unwrap();
            let mut out = vec![0; 32 * 1024];
            let (request, mut frames) = peer_disk_test_frames(&staged.chunk, staged.bytes());
            if consumer_tree {
                let Some(dfs_read_ranges_frame::Body::Header(header)) = &mut frames[0].body else {
                    panic!("header");
                };
                header.blake3_outboard = wi6_encoded_outboard(staged.bytes()).into();
            }
            if mode == 5 {
                frames.pop();
            }
            let binding = sink.bind_output(13, &out).unwrap();
            let mut producer = PeerRootProducer::new(
                staged.chunk.length,
                binding.clone(),
                None,
                std::time::Instant::now() + Duration::from_secs(5),
            )
            .with_outboard_context(staged.chunk.clone(), sink.proof_budget());
            let mut decoder =
                ReadFrameDecoder::for_producer_object(&request, &staged.chunk).unwrap();
            decoder.stream_binding = Some(binding);
            let mut offset = 0;
            for mut frame in frames {
                if let Some(dfs_read_ranges_frame::Body::Header(header)) = &mut frame.body {
                    producer.accept_outboard_header(header, None).unwrap();
                    decoder
                        .install_producer_outboard(producer.outboard_builder.take())
                        .unwrap();
                }
                if let Some(dfs_read_ranges_frame::Body::Data(data)) = &frame.body {
                    producer.update(offset, data, None).unwrap();
                    offset += data.len();
                }
                consume_peer_frame_into_sink(
                    &mut decoder,
                    frame,
                    13,
                    &mut out,
                    &mut sink,
                    std::time::Instant::now() + Duration::from_secs(5),
                    PeerConsumerObservation::default(),
                )
                .unwrap();
                if let Some(active) = &decoder.active {
                    assert!(
                        active.checksum.is_none(),
                        "consumer has no duplicate plain range hasher; its outboard tree is independently verified"
                    );
                }
            }
            if mode == 1 {
                assert!(
                    decoder.finish_stream(&staged.chunk).is_err(),
                    "completion alone cannot substitute for joined root/EOF"
                );
                assert_eq!(
                    proof_budget.used(),
                    proof_baseline,
                    "unjoined metadata must retire without a proof"
                );
                continue;
            }
            let mut receipt = producer.finish_after_eof().unwrap();
            if mode == 2 {
                receipt.length -= 1;
            }
            if mode == 3 {
                match &mut receipt.digest {
                    PeerRootDigest::Producer(digest) => digest[0] ^= 1,
                    PeerRootDigest::ConsumerTree => {
                        receipt.digest = PeerRootDigest::Producer([0; 32])
                    }
                }
            }
            if mode == 4 {
                receipt.binding = other_sink.bind_output(13, &out).unwrap();
            }
            let proof = decoder.finish_stream_from_producer(
                &staged.chunk,
                receipt,
                &sink,
                13,
                &out,
                PeerConsumerObservation::default(),
            );
            if mode == 0 {
                let completed = sink.seal_completed(proof.unwrap(), 13, &out).unwrap();
                assert!(completed.private_output().iter().all(|byte| *byte == 37));
                assert!(sink.commit(completed).unwrap());
            } else {
                assert!(
                    proof.is_err(),
                    "wrong/truncated/missing receipt evidence cannot mint output"
                );
                assert_eq!(
                    proof_budget.used(),
                    proof_baseline,
                    "failed join must retire consumer metadata"
                );
            }
        }
    }
}

#[cfg(feature = "dfs")]
#[test]
fn peer_root_producer_rejects_reordered_and_truncated_blocks() {
    use crate::node::read::peer_cache::{DiskPeerCache, PeerCacheOptions};
    let staged = crate::node::chunk::StagedChunk::new(
        crate::dfs::OperationId::new("producer-order"),
        vec![37; 512],
    );
    let directory = tempfile::tempdir().unwrap();
    let cache = DiskPeerCache::open(PeerCacheOptions {
        directory: directory.path().join("cache"),
        capacity_bytes: 8192,
        namespace_id: crate::dfs::NamespaceId::new("test"),
    })
    .unwrap();
    let mut operation = cache.operation().unwrap();
    let sink = operation.begin_stream(&staged.chunk).unwrap();
    let out = [0; 8];
    let mut producer = PeerRootProducer::new(
        staged.chunk.length,
        sink.bind_output(0, &out).unwrap(),
        None,
        std::time::Instant::now() + Duration::from_secs(5),
    );
    assert!(producer.update(1, &[37; 8], None).is_err());
    producer.update(0, &[37; 8], None).unwrap();
    assert!(producer.update(0, &[37; 8], None).is_err());
    assert!(
        producer.finish_after_eof().is_err(),
        "partial root cannot create a receipt"
    );
    let mut expired = PeerRootProducer::new(
        staged.chunk.length,
        sink.bind_output(0, &out).unwrap(),
        None,
        std::time::Instant::now(),
    );
    assert!(expired.update(0, staged.bytes(), None).is_err());
    assert_eq!(
        expired.length, 0,
        "expired deadline rejects before scanning"
    );
    let mut complete = PeerRootProducer::new(
        staged.chunk.length,
        sink.bind_output(0, &out).unwrap(),
        None,
        std::time::Instant::now() + Duration::from_secs(5),
    );
    complete.update(0, staged.bytes(), None).unwrap();
    complete.deadline = std::time::Instant::now();
    assert!(
        complete.finish_after_eof().is_err(),
        "expired deadline cannot mint a complete receipt"
    );
}

#[cfg(feature = "dfs")]
#[tokio::test]
async fn r7_background_admission_first_poll_never_waits_for_metadata_locks() {
    struct Wake;
    impl std::task::Wake for Wake {
        fn wake(self: Arc<Self>) {}
    }
    let waker = std::task::Waker::from(Arc::new(Wake));
    let mut context = std::task::Context::from_waker(&waker);
    let pool = PeerConnectionPool::new(
        afs_transport::GrpcConfig::default(),
        afs_transport::TlsConfig::Disabled,
        1,
    )
    .unwrap();
    let channels = pool.state.lock().await;
    let mut background =
        Box::pin(pool.acquire_stream_work_mode("peer", 1, "http://127.0.0.1:9", true));
    assert!(matches!(
        std::future::Future::poll(background.as_mut(), &mut context),
        std::task::Poll::Ready(Ok(None))
    ));
    drop(background);
    let mut foreground =
        Box::pin(pool.acquire_stream_work_mode("peer", 1, "http://127.0.0.1:9", false));
    assert!(std::future::Future::poll(foreground.as_mut(), &mut context).is_pending());
    drop(channels);
    let active = foreground.await.unwrap().unwrap();
    drop(active);
    let admission = pool.stream_work.state.lock().unwrap();
    let mut background =
        Box::pin(pool.acquire_stream_work_mode("peer", 1, "http://127.0.0.1:9", true));
    assert!(matches!(
        std::future::Future::poll(background.as_mut(), &mut context),
        std::task::Poll::Ready(Ok(None))
    ));
    drop(background);
    drop(admission);
}

#[cfg(feature = "dfs")]
#[tokio::test]
async fn peer_stream_current_thread_is_unsupported_before_rpc() {
    use crate::node::{
        read::engine::ChunkTransfer,
        read::peer_cache::{DiskPeerCache, PeerCacheOptions},
    };
    let pool = PeerConnectionPool::new(
        afs_transport::GrpcConfig::default(),
        afs_transport::TlsConfig::Disabled,
        1,
    )
    .unwrap();
    let transfer = GrpcChunkTransfer::new(Arc::new(pool), Duration::from_millis(10)).unwrap();
    assert!(!transfer.supports_verified_object_sink());
    let staged = crate::node::chunk::StagedChunk::new(
        crate::dfs::OperationId::new("unsupported-stream"),
        vec![37; 64 * 1024],
    );
    let batch = disk_stream_batch(&staged.chunk, "http://127.0.0.1:1".into());
    let directory = tempfile::tempdir().unwrap();
    let cache = DiskPeerCache::open(PeerCacheOptions {
        directory: directory.path().join("cache"),
        capacity_bytes: 1024 * 1024,
        namespace_id: crate::dfs::NamespaceId::new("test"),
    })
    .unwrap();
    let mut operation = cache.operation().unwrap();
    let mut sink = operation.begin_stream(&staged.chunk).unwrap();
    let mut out = [99; 8];
    assert!(
        transfer
            .read_verified_object_to_sink(&batch, &staged.chunk, 0, &mut out, &mut sink)
            .unwrap()
            .is_none()
    );
    assert_eq!(out, [99; 8]);
}

#[cfg(feature = "dfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_stream_large_windows_are_unsupported_before_rpc() {
    use crate::node::read::engine::ChunkTransfer;
    for connection in [false, true] {
        let mut config = afs_transport::GrpcConfig::default();
        if connection {
            config.initial_connection_window_size += 1;
        } else {
            config.initial_stream_window_size += 1;
        }
        let pool = PeerConnectionPool::new(config, afs_transport::TlsConfig::Disabled, 1).unwrap();
        let transfer = GrpcChunkTransfer::new(Arc::new(pool), Duration::from_secs(1)).unwrap();
        assert!(!transfer.supports_verified_object_sink());
    }
}

#[cfg(feature = "dfs")]
#[test]
fn peer_stream_blocks_retain_capacity_and_controls_reject_payload() {
    use afs_protocol::node_data::DfsReadRangesFrame;
    let mut block = PeerStreamBlock::new(None).unwrap();
    let address = block.bytes.as_ptr();
    for length in [STREAM_FRAME_BACKING_BYTES, 7, STREAM_DATA_BYTES] {
        block.bytes.clear();
        block.bytes.resize(length, 37);
        assert_eq!(block.bytes.as_ptr(), address);
        assert_eq!(block.bytes.capacity(), STREAM_FRAME_BACKING_BYTES);
    }
    let mut data = DfsReadRangesFrame {
        body: Some(dfs_read_ranges_frame::Body::Data(
            vec![0; STREAM_DATA_BYTES].into(),
        )),
    };
    assert!(peer_stream_control_capacity(&mut data).is_err());
}

#[cfg(feature = "dfs")]
#[test]
fn peer_stream_complete_proof_cannot_seal_another_same_object_sink() {
    use crate::node::read::peer_cache::{DiskPeerCache, PeerCacheOptions};
    let directory = tempfile::tempdir().unwrap();
    let make_cache = |name| {
        DiskPeerCache::open(PeerCacheOptions {
            directory: directory.path().join(name),
            capacity_bytes: 1024 * 1024,
            namespace_id: crate::dfs::NamespaceId::new("test"),
        })
        .unwrap()
    };
    let cache_a = make_cache("a");
    let cache_b = make_cache("b");
    let staged = crate::node::chunk::StagedChunk::new(
        crate::dfs::OperationId::new("different-sink"),
        vec![37; 64 * 1024],
    );
    let mut operation_a = cache_a.operation().unwrap();
    let mut operation_b = cache_b.operation().unwrap();
    let mut sink_a = operation_a.begin_stream(&staged.chunk).unwrap();
    let mut sink_b = operation_b.begin_stream(&staged.chunk).unwrap();
    let mut out = [0; 8];
    let (request, frames) = peer_disk_test_frames(&staged.chunk, staged.bytes());
    let mut decoder = ReadFrameDecoder::for_object(&request, &staged.chunk).unwrap();
    decoder.stream_binding = Some(sink_a.bind_output(0, &out).unwrap());
    for frame in frames {
        consume_peer_frame_into_sink(
            &mut decoder,
            frame,
            0,
            &mut out,
            &mut sink_a,
            std::time::Instant::now() + Duration::from_secs(5),
            PeerConsumerObservation::default(),
        )
        .unwrap();
    }
    let proof = decoder.finish_stream(&staged.chunk).unwrap();
    sink_b.write(0, staged.bytes()).unwrap();
    assert!(sink_b.seal_completed(proof, 0, &out).is_err());
}

#[tokio::test]
async fn endpoint_client_applies_tls_policy_before_connecting() {
    let result = connect_data_client(DataClientOptions {
        endpoint: "https://127.0.0.1:1".into(),
        mode: DataMode::Grpc,
        rdma_device: None,
        timeout: Duration::from_millis(10),
        tls: afs_transport::TlsConfig::MutualTls {
            encryption_enabled: true,
            ca_certificate: "missing-ca.pem".into(),
            identity_certificate: "missing-client.pem".into(),
            identity_private_key: "missing-client-key.pem".into(),
            server_name: String::new(),
        },
    })
    .await;

    let error = match result {
        Ok(_) => panic!("invalid diagnostics TLS policy must fail before connecting"),
        Err(error) => error,
    };
    assert_eq!(error.code(), afs_error::CLIENT_ARGUMENT_INVALID);
    assert!(error.to_string().contains("server_name"));
}

#[derive(Clone, Copy)]
enum BadReplyKind {
    ReadShape,
    WriteCount,
}

#[derive(Clone, Copy)]
struct BadDataService {
    kind: BadReplyKind,
}

#[tonic::async_trait]
impl NodeData for BadDataService {
    async fn read(
        &self,
        _request: Request<DataReadRequest>,
    ) -> Result<Response<DataReadReply>, Status> {
        match self.kind {
            BadReplyKind::ReadShape => Ok(Response::new(DataReadReply {
                length: 8,
                data: b"short".to_vec(),
            })),
            BadReplyKind::WriteCount => Err(Status::unimplemented("read not used")),
        }
    }

    async fn write(
        &self,
        request: Request<DataWriteRequest>,
    ) -> Result<Response<DataWriteReply>, Status> {
        match self.kind {
            BadReplyKind::ReadShape => Err(Status::unimplemented("write not used")),
            BadReplyKind::WriteCount => Ok(Response::new(DataWriteReply {
                written: request.into_inner().data.len() as u32 + 1,
            })),
        }
    }
}

#[tokio::test]
async fn grpc_client_rejects_read_reply_shape_mismatch() {
    let (endpoint, server) = spawn_bad_data_server(BadReplyKind::ReadShape).await;
    let mut client = connect_data_client(DataClientOptions {
        endpoint,
        mode: DataMode::Grpc,
        rdma_device: None,
        timeout: Duration::from_secs(5),
        tls: Default::default(),
    })
    .await
    .unwrap();

    let error = client.read("bad.bin", 0, 8).await.unwrap_err();

    assert!(error.to_string().contains("read reply shape mismatch"));
    server.abort();
}

#[tokio::test]
async fn grpc_client_rejects_write_count_mismatch() {
    let (endpoint, server) = spawn_bad_data_server(BadReplyKind::WriteCount).await;
    let mut client = connect_data_client(DataClientOptions {
        endpoint,
        mode: DataMode::Grpc,
        rdma_device: None,
        timeout: Duration::from_secs(5),
        tls: Default::default(),
    })
    .await
    .unwrap();

    let error = client
        .write("bad.bin", 0, b"abcdefgh".to_vec())
        .await
        .unwrap_err();

    assert!(error.to_string().contains("write reply count mismatch"));
    server.abort();
}

#[tokio::test]
#[cfg(feature = "ownerfs")]
async fn owner_release_retries_transient_failures_without_losing_handle() {
    let handler = std::sync::Arc::new(ReleaseTestHandler::transient_failures(2));
    let (channel, server) = spawn_owner_release_server(handler.clone()).await;
    let client =
        owner_files_client_from_channel_with_runtime(channel, tokio::runtime::Handle::current());
    let grant = test_grant();
    let file = test_remote_file(b"handle-retry".to_vec());

    client.release(&grant, file).unwrap();

    wait_until(Duration::from_secs(2), || handler.attempts() >= 3).await;
    assert_eq!(handler.attempts(), 3);
    assert_eq!(handler.seen_handles(), vec![b"handle-retry".to_vec(); 3]);
    server.abort();
}

#[tokio::test]
#[cfg(feature = "ownerfs")]
async fn owner_release_treats_stale_handle_as_idempotent_success() {
    let handler = std::sync::Arc::new(ReleaseTestHandler::stale_handle());
    let (channel, server) = spawn_owner_release_server(handler.clone()).await;
    let client =
        owner_files_client_from_channel_with_runtime(channel, tokio::runtime::Handle::current());

    client
        .release(&test_grant(), test_remote_file(b"handle-stale".to_vec()))
        .unwrap();

    wait_until(Duration::from_secs(2), || handler.attempts() >= 1).await;
    tokio::time::sleep(RELEASE_INITIAL_BACKOFF * 3).await;
    assert_eq!(handler.attempts(), 1);
    assert_eq!(handler.seen_handles(), vec![b"handle-stale".to_vec()]);
    server.abort();
}

#[cfg(all(feature = "ownerfs", feature = "rdma"))]
#[tokio::test]
async fn owner_rdma_client_admission_rejects_before_endpoint_open() {
    let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
    let client = owner_files_client_from_channel(channel)
        .with_data_transport(
            DataMode::Rdma,
            Some("__afs_rdma_must_not_open__".to_owned()),
            Duration::from_millis(1),
        )
        .with_rdma_admission(Arc::new(tokio::sync::Semaphore::new(0)));
    let grant = RootGrant {
        id: RootId("job-admission".to_owned()),
        epoch: 1,
        home_node_id: "node-a".to_owned(),
        home_session_id: "home-session".to_owned(),
        holder_node_id: "node-b".to_owned(),
        session_id: "grant-session".to_owned(),
        access_generation: 1,
        rights: vec![RootRight::Read, RootRight::Write],
        fencing_token: "fence".to_owned(),
    };

    let error = match client.negotiate_owner_rdma(&grant).await {
        Ok(_) => panic!("zero client admission must fail before opening an endpoint"),
        Err(error) => error,
    };
    assert_eq!(error.code(), afs_error::NODE_RDMA_CAPACITY);
}

#[cfg(all(feature = "ownerfs", feature = "rdma"))]
#[tokio::test]
async fn owner_rdma_missing_device_errors_are_counted_by_client_timer() {
    let registry = afs_metrics::Registry::new();
    let metrics = crate::node::rpc::OwnerRpcMetrics::register(&registry).unwrap();
    let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
    let client = owner_files_client_from_channel_with_runtime_and_metrics(
        channel,
        tokio::runtime::Handle::current(),
        Some(metrics),
    )
    .with_data_transport(DataMode::Rdma, None, Duration::from_millis(1));
    let grant = test_grant();
    let file = test_remote_file(b"handle-rdma-missing".to_vec());

    tokio::task::spawn_blocking(move || {
        let mut one = [0_u8; 1];
        let read = client.read(&grant, &file, 0, &mut one).unwrap_err();
        assert_eq!(read.code(), afs_error::NODE_TRANSFER_UNSUPPORTED);
        let write = client.write(&grant, &file, 0, b"x").unwrap_err();
        assert_eq!(write.code(), afs_error::NODE_TRANSFER_UNSUPPORTED);
    })
    .await
    .expect("missing-device regression worker");

    assert_eq!(owner_rpc_histogram_count(&registry, "client", "read"), 1);
    assert_eq!(owner_rpc_histogram_count(&registry, "client", "write"), 1);
}

#[cfg(feature = "ownerfs")]
#[tokio::test]
async fn owner_grpc_payload_bytes_count_successful_logical_bytes() {
    let registry = afs_metrics::Registry::new();
    let metrics = crate::node::rpc::OwnerRpcMetrics::register(&registry).unwrap();
    let handler = std::sync::Arc::new(PayloadMetricsHandler::success());
    let (channel, server) = spawn_owner_payload_server(handler, metrics.clone()).await;
    let client = owner_files_client_from_channel_with_runtime_and_metrics(
        channel,
        tokio::runtime::Handle::current(),
        Some(metrics),
    );
    let grant = test_grant();
    let file = test_remote_file(b"handle-payload".to_vec());

    tokio::task::spawn_blocking(move || {
        let mut out = [0_u8; 8];
        assert_eq!(client.read(&grant, &file, 0, &mut out).unwrap(), 3);
        assert_eq!(&out[..3], b"abc");
        assert_eq!(client.write(&grant, &file, 0, b"hello").unwrap(), 2);
    })
    .await
    .expect("payload metrics worker");

    assert_eq!(owner_payload_bytes(&registry, "client", "read", "grpc"), 3);
    assert_eq!(owner_payload_bytes(&registry, "server", "read", "grpc"), 3);
    assert_eq!(owner_payload_bytes(&registry, "client", "write", "grpc"), 2);
    assert_eq!(owner_payload_bytes(&registry, "server", "write", "grpc"), 2);
    server.abort();
}

#[cfg(feature = "ownerfs")]
#[tokio::test]
async fn owner_grpc_write_preserves_request_contract() {
    let registry = afs_metrics::Registry::new();
    let metrics = crate::node::rpc::OwnerRpcMetrics::register(&registry).unwrap();
    let handler = std::sync::Arc::new(PayloadMetricsHandler::success());
    let (channel, server) = spawn_owner_payload_server(handler.clone(), metrics.clone()).await;
    let client = owner_files_client_from_channel_with_runtime_and_metrics(
        channel,
        tokio::runtime::Handle::current(),
        Some(metrics),
    );
    let grant = test_grant();
    let file = test_remote_file(b"handle-payload-contract".to_vec());
    let payload = b"write-contract-payload";
    let checksum = blake3::hash(payload).as_bytes().to_vec();

    tokio::task::spawn_blocking(move || {
        assert_eq!(
            client
                .write_with_options(
                    &grant,
                    &file,
                    4096,
                    payload,
                    WriteOptions { kill_suidgid: true },
                )
                .unwrap(),
            2
        );
    })
    .await
    .expect("write contract worker");

    assert_eq!(
        handler.writes(),
        vec![PayloadWriteRecord {
            offset: 4096,
            data: payload.to_vec(),
            length: payload.len() as u32,
            kill_suidgid: true,
            data_checksum: checksum,
        }]
    );
    assert_eq!(owner_payload_bytes(&registry, "client", "write", "grpc"), 2);
    assert_eq!(owner_payload_bytes(&registry, "server", "write", "grpc"), 2);
    server.abort();
}

#[cfg(feature = "ownerfs")]
#[tokio::test]
async fn owner_grpc_write_large_payload_preserves_request_contract() {
    let registry = afs_metrics::Registry::new();
    let metrics = crate::node::rpc::OwnerRpcMetrics::register(&registry).unwrap();
    let handler = std::sync::Arc::new(PayloadMetricsHandler::success());
    let (channel, server) = spawn_owner_payload_server(handler.clone(), metrics.clone()).await;
    let client = owner_files_client_from_channel_with_runtime_and_metrics(
        channel,
        tokio::runtime::Handle::current(),
        Some(metrics),
    );
    let grant = test_grant();
    let file = test_remote_file(b"handle-large-payload-contract".to_vec());
    let expected_payload = deterministic_bytes(1024 * 1024 - 1);
    let payload = expected_payload.clone();
    let checksum = blake3::hash(&expected_payload).as_bytes().to_vec();

    tokio::task::spawn_blocking(move || {
        assert_eq!(
            client
                .write_with_options(
                    &grant,
                    &file,
                    8192,
                    &payload,
                    WriteOptions { kill_suidgid: true },
                )
                .unwrap(),
            2
        );
    })
    .await
    .expect("large write contract worker");

    assert_eq!(
        handler.writes(),
        vec![PayloadWriteRecord {
            offset: 8192,
            data: expected_payload.clone(),
            length: expected_payload.len() as u32,
            kill_suidgid: true,
            data_checksum: checksum,
        }]
    );
    assert_eq!(owner_payload_bytes(&registry, "client", "write", "grpc"), 2);
    assert_eq!(owner_payload_bytes(&registry, "server", "write", "grpc"), 2);
    server.abort();
}

#[cfg(feature = "ownerfs")]
fn deterministic_bytes(len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| {
            let mixed = index
                .wrapping_mul(1_103_515_245)
                .wrapping_add(12_345)
                .rotate_left((index % 8) as u32);
            mixed as u8
        })
        .collect()
}

#[cfg(feature = "ownerfs")]
#[tokio::test]
async fn owner_grpc_write_rejects_bad_inline_checksum_before_handler() {
    let registry = afs_metrics::Registry::new();
    let metrics = crate::node::rpc::OwnerRpcMetrics::register(&registry).unwrap();
    let handler = std::sync::Arc::new(PayloadMetricsHandler::success());
    let (channel, server) = spawn_owner_payload_server(handler.clone(), metrics.clone()).await;
    let mut client = OwnerFilesClient::new(channel);
    let grant = test_grant();
    let file = test_remote_file(b"handle-inline-checksum".to_vec());
    let base = b"checksum-boundary".to_vec();
    let mut wrong_checksum = blake3::hash(&base).as_bytes().to_vec();
    wrong_checksum[0] ^= 0xff;

    async fn write_raw(
        client: &mut OwnerFilesClient<Channel>,
        grant: &RootGrant,
        file: &RemoteFile,
        data: Vec<u8>,
        length: u32,
        data_checksum: Vec<u8>,
    ) -> Result<OwnerWriteReply, tonic::Status> {
        client
            .write(request_with_current_context(OwnerWriteRequest {
                access: Some(root_access(grant)),
                handle: Some(file_handle(file)),
                offset: 0,
                data,
                length,
                plane: Some(grpc_plane()),
                kill_suidgid: false,
                data_checksum,
            }))
            .await
            .map(|reply| reply.into_inner())
    }

    let error = write_raw(
        &mut client,
        &grant,
        &file,
        base.clone(),
        base.len() as u32,
        wrong_checksum,
    )
    .await
    .expect_err("wrong 32-byte checksum must fail");
    assert_eq!(
        afs_transport::grpc::error_status::status_to_error(error).code(),
        afs_error::NODE_TRANSFER_CORRUPT_DATA
    );

    let error = write_raw(
        &mut client,
        &grant,
        &file,
        base.clone(),
        base.len() as u32,
        b"short-checksum".to_vec(),
    )
    .await
    .expect_err("malformed checksum length must fail");
    assert_eq!(
        afs_transport::grpc::error_status::status_to_error(error).code(),
        afs_error::NODE_TRANSFER_INVALID
    );

    let error = write_raw(
        &mut client,
        &grant,
        &file,
        base.clone(),
        base.len() as u32 + 1,
        blake3::hash(&base).as_bytes().to_vec(),
    )
    .await
    .expect_err("length/data mismatch must fail");
    assert_eq!(
        afs_transport::grpc::error_status::status_to_error(error).code(),
        afs_error::NODE_TRANSFER_INVALID
    );

    assert!(handler.writes().is_empty());
    assert_eq!(owner_payload_bytes(&registry, "server", "write", "grpc"), 0);

    assert_eq!(
        write_raw(
            &mut client,
            &grant,
            &file,
            base.clone(),
            base.len() as u32,
            blake3::hash(&base).as_bytes().to_vec(),
        )
        .await
        .expect("valid checksum must succeed")
        .written,
        2
    );
    assert_eq!(
        write_raw(
            &mut client,
            &grant,
            &file,
            base.clone(),
            base.len() as u32,
            Vec::new(),
        )
        .await
        .expect("empty legacy checksum must succeed")
        .written,
        2
    );

    let writes = handler.writes();
    assert_eq!(writes.len(), 2);
    assert_eq!(
        writes[0].data_checksum,
        blake3::hash(&base).as_bytes().to_vec()
    );
    assert!(writes[1].data_checksum.is_empty());
    assert_eq!(owner_payload_bytes(&registry, "server", "write", "grpc"), 4);
    server.abort();
}

#[cfg(feature = "ownerfs")]
#[tokio::test]
async fn owner_grpc_malformed_replies_do_not_count_successful_payload_bytes() {
    let registry = afs_metrics::Registry::new();
    let metrics = crate::node::rpc::OwnerRpcMetrics::register(&registry).unwrap();
    let handler = std::sync::Arc::new(PayloadMetricsHandler::malformed());
    let (channel, server) = spawn_owner_payload_server(handler, metrics.clone()).await;
    let client = owner_files_client_from_channel_with_runtime_and_metrics(
        channel,
        tokio::runtime::Handle::current(),
        Some(metrics),
    );
    let grant = test_grant();
    let file = test_remote_file(b"handle-payload-bad".to_vec());

    tokio::task::spawn_blocking(move || {
        let mut out = [0_u8; 8];
        let read = client.read(&grant, &file, 0, &mut out).unwrap_err();
        assert_eq!(read.code(), afs_error::NODE_TRANSFER_INVALID);
        let write = client.write(&grant, &file, 0, b"abc").unwrap_err();
        assert_eq!(write.code(), afs_error::NODE_TRANSFER_INVALID);
    })
    .await
    .expect("malformed payload metrics worker");

    for side in ["client", "server"] {
        assert_eq!(owner_payload_bytes(&registry, side, "read", "grpc"), 0);
        assert_eq!(owner_payload_bytes(&registry, side, "write", "grpc"), 0);
    }
    server.abort();
}

#[cfg(feature = "ownerfs")]
#[tokio::test]
async fn owner_prefetch_cache_hits_do_not_count_as_client_rpc_or_payload() {
    let registry = afs_metrics::Registry::new();
    let metrics = crate::node::rpc::OwnerRpcMetrics::register(&registry).unwrap();
    let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
    let client = owner_files_client_from_channel_with_runtime_and_metrics(
        channel,
        tokio::runtime::Handle::current(),
        Some(metrics),
    );
    let grant = test_grant();
    let file = test_remote_file(b"prefetched-handle".to_vec());
    client
        .prefetched_reads
        .lock()
        .unwrap()
        .insert(file.handle.clone(), b"cached".to_vec());

    let mut out = [0_u8; 6];
    assert_eq!(client.read(&grant, &file, 0, &mut out).unwrap(), 6);
    assert_eq!(&out, b"cached");
    assert_eq!(owner_rpc_histogram_count(&registry, "client", "read"), 0);
    assert_eq!(owner_payload_bytes(&registry, "client", "read", "grpc"), 0);
}

#[cfg(feature = "rdma")]
#[test]
fn cancellation_guard_poisons_unfinished_session_and_disarm_preserves_it() {
    let poisoned = Arc::new(AtomicBool::new(false));
    {
        let _guard = CancelPoisonGuard::new(poisoned.clone());
    }
    assert!(poisoned.load(Ordering::SeqCst));

    let poisoned = Arc::new(AtomicBool::new(false));
    CancelPoisonGuard::new(poisoned.clone()).disarm();
    assert!(!poisoned.load(Ordering::SeqCst));
}

async fn spawn_bad_data_server(kind: BadReplyKind) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(NodeDataServer::new(BadDataService { kind }))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (endpoint, server)
}

#[cfg(feature = "ownerfs")]
struct AllowPeer;

#[cfg(feature = "ownerfs")]
impl PeerAuthenticator for AllowPeer {
    fn authenticate(
        &self,
        _metadata: &tonic::metadata::MetadataMap,
        _remote_addr: Option<std::net::SocketAddr>,
        _peer_cert_der: Option<&[u8]>,
    ) -> afs_error::Result<String> {
        Ok("node-b".to_owned())
    }
}

#[cfg(feature = "ownerfs")]
enum ReleaseFailureMode {
    Transient { remaining: TestAtomicUsize },
    Stale,
}

#[cfg(feature = "ownerfs")]
enum PayloadMetricsMode {
    Success,
    Malformed,
}

#[cfg(feature = "ownerfs")]
struct PayloadMetricsHandler {
    mode: PayloadMetricsMode,
    writes: StdMutex<Vec<PayloadWriteRecord>>,
}

#[cfg(feature = "ownerfs")]
#[derive(Clone, Debug, Eq, PartialEq)]
struct PayloadWriteRecord {
    offset: u64,
    data: Vec<u8>,
    length: u32,
    kill_suidgid: bool,
    data_checksum: Vec<u8>,
}

#[cfg(feature = "ownerfs")]
impl PayloadMetricsHandler {
    fn success() -> Self {
        Self {
            mode: PayloadMetricsMode::Success,
            writes: StdMutex::new(Vec::new()),
        }
    }

    fn malformed() -> Self {
        Self {
            mode: PayloadMetricsMode::Malformed,
            writes: StdMutex::new(Vec::new()),
        }
    }

    fn writes(&self) -> Vec<PayloadWriteRecord> {
        self.writes.lock().unwrap().clone()
    }
}

#[cfg(feature = "ownerfs")]
impl OwnerFilesHandler for PayloadMetricsHandler {
    fn read(&self, peer: &str, _request: OwnerReadRequest) -> afs_error::Result<OwnerReadReply> {
        assert_eq!(peer, "node-b");
        match self.mode {
            PayloadMetricsMode::Success => Ok(OwnerReadReply {
                data: b"abc".to_vec().into(),
                read: 3,
                eof: true,
                data_checksum: Vec::new(),
            }),
            PayloadMetricsMode::Malformed => Ok(OwnerReadReply {
                data: b"abc".to_vec().into(),
                read: 4,
                eof: false,
                data_checksum: Vec::new(),
            }),
        }
    }

    fn write(&self, peer: &str, request: OwnerWriteRequest) -> afs_error::Result<OwnerWriteReply> {
        assert_eq!(peer, "node-b");
        self.writes.lock().unwrap().push(PayloadWriteRecord {
            offset: request.offset,
            data: request.data.clone(),
            length: request.length,
            kill_suidgid: request.kill_suidgid,
            data_checksum: request.data_checksum.clone(),
        });
        match self.mode {
            PayloadMetricsMode::Success => Ok(OwnerWriteReply { written: 2 }),
            PayloadMetricsMode::Malformed => Ok(OwnerWriteReply {
                written: request.length + 1,
            }),
        }
    }
}

#[cfg(feature = "ownerfs")]
struct ReleaseTestHandler {
    mode: ReleaseFailureMode,
    attempts: TestAtomicUsize,
    seen_handles: StdMutex<Vec<Vec<u8>>>,
}

#[cfg(feature = "ownerfs")]
impl ReleaseTestHandler {
    fn transient_failures(failures: usize) -> Self {
        Self {
            mode: ReleaseFailureMode::Transient {
                remaining: TestAtomicUsize::new(failures),
            },
            attempts: TestAtomicUsize::new(0),
            seen_handles: StdMutex::new(Vec::new()),
        }
    }

    fn stale_handle() -> Self {
        Self {
            mode: ReleaseFailureMode::Stale,
            attempts: TestAtomicUsize::new(0),
            seen_handles: StdMutex::new(Vec::new()),
        }
    }

    fn attempts(&self) -> usize {
        self.attempts.load(TestOrdering::SeqCst)
    }

    fn seen_handles(&self) -> Vec<Vec<u8>> {
        self.seen_handles.lock().unwrap().clone()
    }
}

#[cfg(feature = "ownerfs")]
impl OwnerFilesHandler for ReleaseTestHandler {
    fn release(
        &self,
        _authenticated_peer_node_id: &str,
        request: OwnerReleaseRequest,
    ) -> afs_error::Result<OwnerReleaseReply> {
        self.attempts.fetch_add(1, TestOrdering::SeqCst);
        let handle = request
            .handle
            .ok_or_else(|| {
                afs_error::Error::coded(afs_error::CLIENT_ARGUMENT_INVALID, "missing test handle")
            })?
            .opaque;
        self.seen_handles.lock().unwrap().push(handle);
        match &self.mode {
            ReleaseFailureMode::Transient { remaining } => {
                if remaining
                    .fetch_update(TestOrdering::SeqCst, TestOrdering::SeqCst, |current| {
                        (current > 0).then(|| current - 1)
                    })
                    .is_ok()
                {
                    return Err(afs_error::Error::coded(
                        afs_error::CLIENT_CONNECTION_UNAVAILABLE,
                        "transient release failure",
                    ));
                }
                Ok(OwnerReleaseReply {})
            }
            ReleaseFailureMode::Stale => Err(afs_error::Error::coded(
                afs_error::NODE_OWNER_STALE_HANDLE,
                "stale release handle",
            )),
        }
    }
}

#[cfg(feature = "ownerfs")]
async fn spawn_owner_release_server(
    handler: std::sync::Arc<ReleaseTestHandler>,
) -> (Channel, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(make_owner_files_server_with_handler(
                handler,
                std::sync::Arc::new(AllowPeer),
            ))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let channel = Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    (channel, server)
}

#[cfg(feature = "ownerfs")]
async fn spawn_owner_payload_server(
    handler: std::sync::Arc<PayloadMetricsHandler>,
    metrics: crate::node::rpc::OwnerRpcMetrics,
) -> (Channel, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(make_owner_files_server_with_handler_and_metrics(
                handler,
                std::sync::Arc::new(AllowPeer),
                metrics,
            ))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let channel = Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    (channel, server)
}

#[cfg(feature = "ownerfs")]
fn owner_rpc_histogram_count(registry: &afs_metrics::Registry, side: &str, method: &str) -> u64 {
    afs_metrics::encode_text(registry)
        .unwrap()
        .lines()
        .find_map(|line| {
            if line.starts_with("afs_ownerfiles_rpc_duration_seconds_count{")
                && line.contains(&format!("side=\"{side}\""))
                && line.contains(&format!("method=\"{method}\""))
            {
                line.rsplit_once(' ')
                    .and_then(|(_, value)| value.parse::<u64>().ok())
            } else {
                None
            }
        })
        .unwrap_or(0)
}

#[cfg(feature = "ownerfs")]
fn owner_payload_bytes(
    registry: &afs_metrics::Registry,
    side: &str,
    direction: &str,
    plane: &str,
) -> u64 {
    afs_metrics::encode_text(registry)
        .unwrap()
        .lines()
        .find_map(|line| {
            if line.starts_with("afs_ownerfiles_payload_bytes_total{")
                && line.contains(&format!("side=\"{side}\""))
                && line.contains(&format!("direction=\"{direction}\""))
                && line.contains(&format!("plane=\"{plane}\""))
            {
                line.rsplit_once(' ')
                    .and_then(|(_, value)| value.parse::<u64>().ok())
            } else {
                None
            }
        })
        .unwrap_or(0)
}

#[cfg(feature = "ownerfs")]
fn test_grant() -> RootGrant {
    RootGrant {
        id: RootId("root-a".to_owned()),
        epoch: 1,
        home_node_id: "node-a".to_owned(),
        home_session_id: "home-session-a".to_owned(),
        holder_node_id: "node-b".to_owned(),
        session_id: "grant-session-b".to_owned(),
        access_generation: 1,
        rights: vec![RootRight::Write],
        fencing_token: "fence-a".to_owned(),
    }
}

#[cfg(feature = "ownerfs")]
fn test_remote_file(handle: Vec<u8>) -> RemoteFile {
    RemoteFile {
        root_id: RootId("root-a".to_owned()),
        owner_node_id: "node-a".to_owned(),
        owner_session_id: "home-session-a".to_owned(),
        identity: FileIdentity(b"identity-a".to_vec()),
        handle,
    }
}

#[cfg(feature = "ownerfs")]
async fn wait_until(deadline: Duration, condition: impl Fn() -> bool) {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("condition did not become true before timeout");
}
#[cfg(feature = "dfs")]
fn range_request() -> DfsReadRangesRequest {
    DfsReadRangesRequest {
        include_blake3_outboard: false,
        read_id: "read".into(),
        attempt_id: "attempt".into(),
        file_version_id: "version".into(),
        layout_root_id: "layout".into(),
        operations: (0..2)
            .map(|index| DfsChunkReadOp {
                operation_index: index,
                chunk_id: format!("chunk-{index}"),
                chunk_offset: 0,
                length: 1,
                destination_offset: u64::from(index),
                source_copy_id: format!("copy-{index}"),
                grant: None,
            })
            .collect(),
    }
}

#[cfg(feature = "dfs")]
fn range_frames(
    request: &DfsReadRangesRequest,
    index: usize,
) -> Vec<afs_protocol::node_data::DfsReadRangesFrame> {
    use afs_protocol::node_data::{
        DfsReadRangesCompletion, DfsReadRangesFrame, DfsReadRangesHeader,
    };
    let op = &request.operations[index];
    vec![
        DfsReadRangesFrame {
            body: Some(dfs_read_ranges_frame::Body::Header(DfsReadRangesHeader {
                crc32c_table: Default::default(),
                read_id: request.read_id.clone(),
                attempt_id: request.attempt_id.clone(),
                operation_index: op.operation_index,
                chunk_id: op.chunk_id.clone(),
                chunk_offset: op.chunk_offset,
                length: op.length,
                source_copy_id: op.source_copy_id.clone(),
                blake3_outboard: Default::default(),
            })),
        },
        DfsReadRangesFrame {
            body: Some(dfs_read_ranges_frame::Body::Data(
                vec![b'a' + index as u8].into(),
            )),
        },
        DfsReadRangesFrame {
            body: Some(dfs_read_ranges_frame::Body::Completion(
                DfsReadRangesCompletion {
                    crc32c_table: Default::default(),
                    read_id: request.read_id.clone(),
                    attempt_id: request.attempt_id.clone(),
                    operation_index: op.operation_index,
                    source_copy_id: op.source_copy_id.clone(),
                    transferred_bytes: op.length,
                    range_checksum: blake3::hash(&[b'a' + index as u8]).as_bytes().to_vec(),
                    range_checksum_algorithm: afs_protocol::node_data::DfsDigestAlgorithm::Blake3
                        as i32,
                },
            )),
        },
    ]
}

#[cfg(feature = "dfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dfs_peer_receive_cap_reduces_actual_http2_data_frames() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut observations = Vec::new();
    for production in [false, true] {
        let payload = vec![37; 128 * 1024];
        let chunk = crate::node::chunk::StagedChunk::new(
            crate::dfs::OperationId::new("dfs-peer-receive-frames"),
            payload.clone(),
        )
        .chunk;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_address = listener.local_addr().unwrap();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fixture = DiskStreamFixture {
            chunk: chunk.clone(),
            body: Arc::new(payload.clone()),
            mode: 0,
            calls: calls.clone(),
            fetch_objects: None,
            fetch_gate: None,
        };
        let (stop_server, server_done) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(
            Server::builder()
                .add_service(
                    afs_protocol::node_data::dfs_chunks_server::DfsChunksServer::new(fixture),
                )
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = server_done.await;
                }),
        );

        let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", proxy_listener.local_addr().unwrap());
        let data_frames = Arc::new(std::sync::Mutex::new(Vec::new()));
        let frames = data_frames.clone();
        let (stop_proxy, proxy_done) = tokio::sync::oneshot::channel();
        let proxy = tokio::spawn(async move {
            let (client, _) = proxy_listener.accept().await.unwrap();
            let upstream = tokio::net::TcpStream::connect(server_address)
                .await
                .unwrap();
            let (mut client_read, mut client_write) = client.into_split();
            let (mut upstream_read, mut upstream_write) = upstream.into_split();
            let responses = async {
                loop {
                    let mut header = [0; 9];
                    if let Err(error) = upstream_read.read_exact(&mut header).await {
                        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
                        break;
                    }
                    let length = (usize::from(header[0]) << 16)
                        | (usize::from(header[1]) << 8)
                        | usize::from(header[2]);
                    assert!(length <= 2 * 1024 * 1024);
                    let mut body = vec![0; length];
                    upstream_read.read_exact(&mut body).await.unwrap();
                    let stream_id =
                        u32::from_be_bytes([header[5] & 0x7f, header[6], header[7], header[8]]);
                    if header[3] == 0 && stream_id != 0 && length != 0 {
                        frames.lock().unwrap().push(length);
                    }
                    client_write.write_all(&header).await.unwrap();
                    client_write.write_all(&body).await.unwrap();
                }
            };
            tokio::select! {
                result = tokio::io::copy(&mut client_read, &mut upstream_write) => { result.unwrap(); }
                () = responses => {}
                _ = proxy_done => {}
            }
        });

        let config = afs_transport::GrpcConfig::default();
        assert_eq!(config.initial_connection_window_size, 2 * 1024 * 1024);
        assert_eq!(config.initial_stream_window_size, 2 * 1024 * 1024);
        let pool =
            PeerConnectionPool::new(config.clone(), afs_transport::TlsConfig::Disabled, 1).unwrap();
        let channel = if production {
            pool.channel("node-a", 1, &endpoint).await.unwrap()
        } else {
            config
                .configure_client(Endpoint::from_shared(endpoint).unwrap())
                .max_frame_size(16 * 1024)
                .connect_lazy()
        };
        let mut client = afs_protocol::node_data::dfs_chunks_client::DfsChunksClient::new(channel);
        let mut request = range_request();
        request.operations.truncate(1);
        request.operations[0].chunk_id = chunk.id.0;
        request.operations[0].length = payload.len() as u64;
        let stream = client
            .read_ranges(request.clone())
            .await
            .unwrap()
            .into_inner();
        let mut output = vec![0; payload.len()];
        consume_peer_read_stream(&request, None, &mut output, stream, None, None)
            .await
            .unwrap();
        assert_eq!(output, payload);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        observations.push(data_frames.lock().unwrap().clone());

        drop(client);
        drop(pool);
        let _ = stop_proxy.send(());
        tokio::time::timeout(Duration::from_secs(3), proxy)
            .await
            .unwrap()
            .unwrap();
        let _ = stop_server.send(());
        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    eprintln!(
        "actual HTTP2 DATA payload lengths: control={:?}, production={:?}",
        observations[0], observations[1]
    );
    assert!(observations[0].iter().all(|length| *length <= 16 * 1024));
    assert!(observations[1].len() < observations[0].len());
    assert!(observations[1].iter().any(|length| *length > 16 * 1024));
    assert_eq!(
        observations[0].iter().sum::<usize>(),
        observations[1].iter().sum::<usize>()
    );
}

#[cfg(feature = "dfs")]
fn verified_object_frames(
    body: &[u8],
) -> (
    ChunkObject,
    DfsReadRangesRequest,
    Vec<afs_protocol::node_data::DfsReadRangesFrame>,
) {
    use afs_protocol::node_data::{DfsReadRangesCompletion, DfsReadRangesFrame};

    let chunk = crate::node::chunk::StagedChunk::new(
        crate::dfs::OperationId::new("verified-stream"),
        body.to_vec(),
    )
    .chunk;
    let mut request = range_request();
    request.include_blake3_outboard = true;
    request.operations.truncate(1);
    request.operations[0].chunk_id = chunk.id.0.clone();
    request.operations[0].length = chunk.length;
    let mut frames = vec![range_frames(&request, 0).remove(0)];
    for bytes in body.chunks(64 * 1024 - 1) {
        frames.push(DfsReadRangesFrame {
            body: Some(dfs_read_ranges_frame::Body::Data(bytes.to_vec().into())),
        });
    }
    frames.push(DfsReadRangesFrame {
        body: Some(dfs_read_ranges_frame::Body::Completion(
            DfsReadRangesCompletion {
                crc32c_table: Default::default(),
                read_id: request.read_id.clone(),
                attempt_id: request.attempt_id.clone(),
                operation_index: 0,
                source_copy_id: request.operations[0].source_copy_id.clone(),
                transferred_bytes: chunk.length,
                range_checksum: chunk.content_digest.bytes.to_vec(),
                range_checksum_algorithm: afs_protocol::node_data::DfsDigestAlgorithm::Blake3
                    as i32,
            },
        )),
    });
    (chunk, request, frames)
}

#[cfg(feature = "dfs")]
fn wi6_encoded_outboard(body: &[u8]) -> Vec<u8> {
    crate::node::chunk::outboard::build_borrowed(body, &OutboardBudget::new(1024 * 1024))
        .outboard
        .unwrap()
        .encoded()
        .to_vec()
}

#[cfg(feature = "dfs")]
fn wi6_frames_with_outboard(
    body: &[u8],
    encoded: &[u8],
) -> (
    ChunkObject,
    DfsReadRangesRequest,
    Vec<afs_protocol::node_data::DfsReadRangesFrame>,
) {
    let (chunk, request, mut frames) = verified_object_frames(body);
    let Some(dfs_read_ranges_frame::Body::Header(header)) = &mut frames[0].body else {
        panic!("header");
    };
    header.blake3_outboard = encoded.to_vec().into();
    (chunk, request, frames)
}

#[cfg(feature = "dfs")]
#[test]
fn wi6_peer_outboard_binds_actual_body_and_moves_shared_budget_until_last_owner() {
    let body = vec![71; 96 * 1024 + 17];
    let encoded = wi6_encoded_outboard(&body);
    let (chunk, request, frames) = wi6_frames_with_outboard(&body, &encoded);
    let budget = OutboardBudget::new(1024 * 1024);
    let registry = afs_metrics::Registry::new();
    let metrics = ReadWorkMetrics::register(&registry).unwrap();
    let mut decoder = ReadFrameDecoder::for_object(&request, &chunk)
        .unwrap()
        .with_outboard_budget(Some(budget.clone()));
    decoder.work_metrics = Some(metrics);
    let mut output = vec![0; body.len()];
    for frame in frames {
        decoder.accept(frame, &mut output).unwrap();
    }
    let proof = decoder.finish_object(&output).unwrap();
    assert_eq!(proof.bytes(), body);
    assert_eq!(proof.root_digest(), &chunk.content_digest.bytes);
    let outboard = proof.into_outboard().unwrap();
    assert_eq!(outboard.encoded(), encoded);
    assert!(budget.used() >= ChunkOutboard::resident_charge(encoded.len()).unwrap());
    let retained = outboard.clone();
    drop(outboard);
    assert!(
        budget.used() > 0,
        "cache/slow holders keep the same metadata lease"
    );
    drop(retained);
    assert_eq!(budget.used(), 0);
    let text = afs_metrics::encode_text(&registry).unwrap();
    assert!(
        text.contains(&format!(
            "afs_dfs_read_hash_bytes_total{{stage=\"outboard_body_hash\"}} {}\n",
            body.len()
        )),
        "{text}"
    );
    assert!(
        text.contains(&format!(
            "afs_dfs_read_copy_bytes_total{{stage=\"peer_outboard_metadata\"}} {}\n",
            encoded.len()
        )),
        "{text}"
    );
}

#[cfg(feature = "dfs")]
#[test]
fn wi6_peer_outboard_rejects_shape_cv_replay_and_partial_capabilities() {
    let body = vec![71; 96 * 1024 + 17];
    let encoded = wi6_encoded_outboard(&body);
    for mode in 0..6 {
        let mut supplied = encoded.clone();
        match mode {
            0 => supplied[0] ^= 1, // unknown format
            1 => {
                supplied.pop();
            } // truncated parent CVs
            2 => supplied.resize(8193, 0),
            3 => supplied[64] ^= 1, // valid shape, wrong body-bound parent
            _ => {}
        }
        let (mut chunk, request, frames) = wi6_frames_with_outboard(&body, &supplied);
        if mode == 4 {
            chunk.content_digest.bytes[0] ^= 1;
        }
        let budget = OutboardBudget::new(if mode == 5 { 0 } else { 1024 * 1024 });
        if mode == 5 {
            // Even a full pool cannot suppress validation of the fixed header.
            let mut malformed = frames;
            let Some(dfs_read_ranges_frame::Body::Header(header)) = &mut malformed[0].body else {
                panic!("header");
            };
            header.blake3_outboard = vec![0; 64].into();
            let mut decoder = ReadFrameDecoder::for_object(&request, &chunk)
                .unwrap()
                .with_outboard_budget(Some(budget.clone()));
            assert!(
                decoder
                    .accept(malformed.remove(0), &mut vec![0; body.len()])
                    .is_err()
            );
            assert_eq!(budget.used(), 0);
            continue;
        }
        let mut output = vec![0; body.len()];
        let mut decoder = ReadFrameDecoder::for_object(&request, &chunk)
            .unwrap()
            .with_outboard_budget(Some(budget.clone()));
        let rejected = match frames
            .into_iter()
            .try_for_each(|frame| decoder.accept(frame, &mut output))
        {
            Err(_) => {
                drop(decoder);
                true
            }
            Ok(()) => decoder.finish_object(&output).is_err(),
        };
        assert!(rejected, "mode {mode}");
        assert_eq!(budget.used(), 0, "rejected proof lease must drain");
    }
    let mut request = range_request();
    request.operations.truncate(1);
    let mut frame = range_frames(&request, 0).remove(0);
    let Some(dfs_read_ranges_frame::Body::Header(header)) = &mut frame.body else {
        panic!("header");
    };
    header.blake3_outboard = encoded.into();
    let mut decoder = ReadFrameDecoder::new(&request);
    let mut output = [99];
    assert!(decoder.accept(frame, &mut output).is_err());
    assert_eq!(
        output,
        [99],
        "partial proof rejection must precede body writes"
    );
}

#[cfg(feature = "dfs")]
#[test]
fn wi6_peer_producer_retires_wire_backing_and_keeps_standard_root() {
    use crate::node::read::peer_cache::{DiskPeerCache, PeerCacheOptions};
    let body = vec![47; 2 * STREAM_FRAME_BACKING_BYTES + 31];
    let encoded = wi6_encoded_outboard(&body);
    let (chunk, request, mut frames) = wi6_frames_with_outboard(&body, &encoded);
    let directory = tempfile::tempdir().unwrap();
    let cache = DiskPeerCache::open(PeerCacheOptions {
        directory: directory.path().join("cache"),
        capacity_bytes: 8 * 1024 * 1024,
        namespace_id: crate::dfs::NamespaceId::new("test"),
    })
    .unwrap();
    let mut operation = cache.operation().unwrap();
    let mut sink = operation.begin_stream(&chunk).unwrap();
    let mut output = [0; 31];
    let binding = sink.bind_output(0, &output).unwrap();
    let budget = sink.proof_budget();
    let baseline = budget.used();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let registry = afs_metrics::Registry::new();
    let metrics = ReadWorkMetrics::register(&registry).unwrap();
    let observation = PeerConsumerObservation {
        metrics: Some(&metrics),
        diagnostic: None,
    };
    let mut producer = PeerRootProducer::new(chunk.length, binding.clone(), None, deadline)
        .with_outboard_context(chunk.clone(), budget.clone());
    let Some(dfs_read_ranges_frame::Body::Header(header)) = &mut frames[0].body else {
        panic!("header");
    };
    producer
        .accept_outboard_header(header, Some(&metrics))
        .unwrap();
    assert!(
        header.blake3_outboard.is_empty(),
        "opaque decoder owner must not reach control queue"
    );
    peer_stream_control_capacity(&mut frames[0]).unwrap();
    let mut decoder = ReadFrameDecoder::for_producer_object(&request, &chunk).unwrap();
    decoder.stream_binding = Some(binding);
    decoder
        .install_producer_outboard(producer.outboard_builder.take())
        .unwrap();
    assert!(
        producer.outboard_builder.is_none(),
        "producer hands metadata work to the consumer"
    );
    let mut offset = 0;
    for frame in frames {
        if let Some(dfs_read_ranges_frame::Body::Data(data)) = &frame.body {
            producer.update(offset, data, Some(&metrics)).unwrap();
            if offset == 0 {
                let text = afs_metrics::encode_text(&registry).unwrap();
                assert!(
                    text.contains(
                        "afs_dfs_read_hash_bytes_total{stage=\"outboard_body_hash\"} 0\n"
                    ),
                    "producer root must not execute consumer outboard work: {text}"
                );
            }
            offset += data.len();
        }
        consume_peer_frame_into_sink(
            &mut decoder,
            frame,
            0,
            &mut output,
            &mut sink,
            deadline,
            observation,
        )
        .unwrap();
    }
    assert!(
        decoder.verified_object.as_ref().unwrap().outboard.is_none(),
        "Completion cannot mint a proof before real EOF/join"
    );
    let receipt = producer.finish_after_eof().unwrap();
    assert!(matches!(receipt.digest, PeerRootDigest::ConsumerTree));
    let proof = decoder
        .finish_stream_from_producer(&chunk, receipt, &sink, 0, &output, observation)
        .unwrap();
    let outboard = proof.into_outboard().unwrap();
    assert_eq!(outboard.encoded(), encoded);
    let text = afs_metrics::encode_text(&registry).unwrap();
    for (stage, expected) in [("root_in_receive", 0), ("outboard_body_hash", body.len())] {
        assert!(
            text.contains(&format!(
                "afs_dfs_read_hash_bytes_total{{stage=\"{stage}\"}} {expected}\n"
            )),
            "{text}"
        );
    }
    let retained = outboard.clone();
    drop(outboard);
    assert!(
        budget.used() > baseline,
        "last shared proof holder must retain its lease"
    );
    drop(retained);
    assert_eq!(
        budget.used(),
        baseline,
        "proof retirement preserves the live cache baseline"
    );
    assert_eq!(output.as_slice(), &body[..output.len()]);
    drop(sink);
    drop(operation);
    cache.shutdown().unwrap();
    assert_eq!(
        budget.used(),
        0,
        "shutdown retires the cache's fixed index charge"
    );
}

#[cfg(feature = "dfs")]
#[test]
fn wi6_r_producer_hash_is_reserved_for_header_fallbacks() {
    use crate::node::read::peer_cache::{DiskPeerCache, PeerCacheOptions};
    let body = vec![83; 96 * 1024 + 17];
    let encoded = wi6_encoded_outboard(&body);
    for mode in 0..4 {
        let budget = OutboardBudget::new(if mode == 2 { 0 } else { 1024 * 1024 });
        let directory = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open_with_outboard_budget(
            PeerCacheOptions {
                directory: directory.path().join("cache"),
                capacity_bytes: 8 * 1024 * 1024,
                namespace_id: crate::dfs::NamespaceId::new("test"),
            },
            budget.clone(),
        )
        .unwrap();
        let mut operation = cache.operation().unwrap();
        let (chunk, _, mut frames) = wi6_frames_with_outboard(&body, &encoded);
        let sink = operation.begin_stream(&chunk).unwrap();
        let output = [0; 31];
        let remaining = budget.limit() - budget.used();
        let holder = (mode == 3).then(|| budget.try_reserve(remaining).unwrap());
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        let mut producer = PeerRootProducer::new(
            chunk.length,
            sink.bind_output(0, &output).unwrap(),
            None,
            std::time::Instant::now() + Duration::from_secs(5),
        )
        .with_outboard_context(chunk.clone(), budget);
        let Some(dfs_read_ranges_frame::Body::Header(header)) = &mut frames[0].body else {
            panic!("header");
        };
        if mode == 1 {
            header.blake3_outboard = Default::default();
        }
        producer
            .accept_outboard_header(header, Some(&metrics))
            .unwrap();
        producer.update(0, &body, Some(&metrics)).unwrap();
        let expected = if mode <= 1 { 0 } else { body.len() };
        let text = afs_metrics::encode_text(&registry).unwrap();
        assert!(
            text.contains(&format!(
                "afs_dfs_read_hash_bytes_total{{stage=\"root_in_receive\"}} {expected}\n"
            )),
            "mode {mode}: consumer tree must replace only eligible duplicate producer work: {text}"
        );
        drop(holder);
    }
}

#[cfg(feature = "dfs")]
#[test]
fn wi6_peer_outboard_budget_exhaustion_keeps_full_root_and_eof_contract() {
    let body = vec![71; 96 * 1024 + 17];
    let encoded = wi6_encoded_outboard(&body);
    for occupied in [false, true] {
        let budget = OutboardBudget::new(if occupied { 8192 } else { 0 });
        let holder = occupied.then(|| budget.try_reserve(8192).unwrap());
        let (chunk, request, frames) = wi6_frames_with_outboard(&body, &encoded);
        let mut decoder = ReadFrameDecoder::for_object(&request, &chunk)
            .unwrap()
            .with_outboard_budget(Some(budget.clone()));
        let mut output = vec![0; body.len()];
        for frame in frames {
            decoder.accept(frame, &mut output).unwrap();
        }
        let proof = decoder.finish_object(&output).unwrap();
        assert_eq!(proof.bytes(), body);
        assert!(
            proof.into_outboard().is_none(),
            "unused CVs cannot mint a proof"
        );
        drop(holder);
        assert_eq!(budget.used(), 0);

        let (mut wrong, request, mut frames) = wi6_frames_with_outboard(&body, &encoded);
        wrong.content_digest.bytes[0] ^= 1;
        // A legacy peer omits optional proof; its self-consistent checksum
        // still cannot replace the trusted Meta root, even with budget zero.
        let Some(dfs_read_ranges_frame::Body::Header(header)) = &mut frames[0].body else {
            panic!("header");
        };
        header.blake3_outboard = Default::default();
        let mut decoder = ReadFrameDecoder::for_object(&request, &wrong)
            .unwrap()
            .with_outboard_budget(Some(OutboardBudget::new(0)));
        let mut output = vec![0; body.len()];
        assert!(
            frames
                .into_iter()
                .try_for_each(|frame| decoder.accept(frame, &mut output))
                .is_err()
        );
    }
}

#[cfg(feature = "dfs")]
async fn r3_assert_legacy_complete_stream_hashes_body_once(budget_limit: usize) {
    let body: Vec<u8> = (0..(2 * STREAM_FRAME_BACKING_BYTES + 17))
        .map(|index| (index % 251) as u8)
        .collect();
    let (chunk, request, frames) = verified_object_frames(&body);
    assert!(matches!(
        &frames[0].body,
        Some(dfs_read_ranges_frame::Body::Header(header))
            if header.blake3_outboard.is_empty()
    ));
    assert!(matches!(
        &frames.last().unwrap().body,
        Some(dfs_read_ranges_frame::Body::Completion(_))
    ));
    let registry = afs_metrics::Registry::new();
    let metrics = ReadWorkMetrics::register(&registry).unwrap();
    let budget = OutboardBudget::new(budget_limit);
    let mut out = vec![0xa5; body.len()];
    // Reuse the complete stream decoder, including Header/body/Completion
    // and real iterator EOF. This is not producer-only prefix accounting.
    let verified = consume_peer_read_stream(
        &request,
        Some(&chunk),
        &mut out,
        tokio_stream::iter(frames.into_iter().map(Ok)),
        Some(&metrics),
        Some(budget.clone()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(verified.bytes(), body);
    assert_eq!(verified.chunk(), &chunk);
    if budget_limit == 0 {
        assert_eq!(budget.used(), 0, "root-only fallback needs no proof lease");
    } else {
        assert!(budget.used() > 0, "legacy body built a charged cache tree");
    }
    let text = afs_metrics::encode_text(&registry).unwrap();
    let hash_bytes = |stage: &str| {
        let prefix = format!("afs_dfs_read_hash_bytes_total{{stage=\"{stage}\"}} ");
        text.lines()
            .find_map(|line| line.strip_prefix(&prefix))
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    let plain = hash_bytes("root_in_receive");
    let builder = hash_bytes("outboard_body_hash");
    drop(verified);
    assert_eq!(budget.used(), 0, "last proof holder releases its charge");
    assert_eq!(
        plain + builder,
        body.len() as u64,
        "legacy complete stream scanned body twice: plain={plain}, builder={builder}, budget={budget_limit}"
    );
    assert_eq!(
        plain,
        if budget_limit == 0 {
            body.len() as u64
        } else {
            0
        }
    );
    assert_eq!(
        builder,
        if budget_limit == 0 {
            0
        } else {
            body.len() as u64
        }
    );
    let fallback_prefix = "afs_dfs_peer_read_events_total{event=\"root_fallback\"} ";
    let fallbacks: u64 = text
        .lines()
        .find_map(|line| line.strip_prefix(fallback_prefix))
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(fallbacks, u64::from(budget_limit == 0));
}

#[cfg(feature = "dfs")]
#[tokio::test]
async fn r3_legacy_complete_stream_with_charged_tree_hashes_body_once() {
    r3_assert_legacy_complete_stream_hashes_body_once(1024 * 1024).await;
}

#[cfg(feature = "dfs")]
#[tokio::test]
async fn r3_legacy_complete_stream_with_root_only_budget_hashes_body_once() {
    r3_assert_legacy_complete_stream_hashes_body_once(0).await;
}

#[cfg(feature = "dfs")]
#[tokio::test]
async fn r3_legacy_tree_and_plain_roots_cannot_publish_before_real_eof() {
    let body = vec![29; 2 * STREAM_FRAME_BACKING_BYTES + 17];
    for budget_limit in [0, 1024 * 1024] {
        let (chunk, request, frames) = verified_object_frames(&body);
        let mut stream: Vec<_> = frames.into_iter().map(Ok).collect();
        stream.push(Err(Status::unavailable(
            "late status after complete legacy body",
        )));
        let budget = OutboardBudget::new(budget_limit);
        let mut private = vec![0xa5; body.len()];
        assert!(
            consume_peer_read_stream(
                &request,
                Some(&chunk),
                &mut private,
                tokio_stream::iter(stream),
                None,
                Some(budget.clone()),
            )
            .await
            .is_err(),
            "Completion alone cannot return a verified object"
        );
        assert_eq!(
            budget.used(),
            0,
            "late failure retires all optional proof owners"
        );
    }
}

#[cfg(feature = "dfs")]
#[tokio::test]
async fn verified_peer_stream_hashes_once_and_binds_trusted_root() {
    let body: Vec<u8> = (0..(1024 * 1024) + 17)
        .map(|index| (index % 251) as u8)
        .collect();
    let (chunk, request, frames) = verified_object_frames(&body);
    let registry = afs_metrics::Registry::new();
    let metrics = ReadWorkMetrics::register(&registry).unwrap();
    let mut out = vec![0; body.len()];
    let verified = consume_peer_read_stream(
        &request,
        Some(&chunk),
        &mut out,
        tokio_stream::iter(frames.into_iter().map(Ok)),
        Some(&metrics),
        None,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(verified.bytes(), body);
    assert_eq!(verified.chunk(), &chunk);
    let text = afs_metrics::encode_text(&registry).unwrap();
    assert!(
        text.contains(&format!(
            "afs_dfs_read_hash_bytes_total{{stage=\"root_in_receive\"}} {}\n",
            body.len()
        )),
        "{text}"
    );
    assert!(
        text.contains("afs_dfs_read_hash_bytes_total{stage=\"root_fallback\"} 0\n"),
        "{text}"
    );
}

#[cfg(feature = "dfs")]
#[tokio::test]
async fn verified_peer_stream_rejects_server_consistent_wrong_meta_root() {
    let (mut chunk, request, frames) = verified_object_frames(b"peer-consistent body");
    chunk.content_digest.bytes[0] ^= 1;
    let mut out = vec![0; chunk.length as usize];
    let error = consume_peer_read_stream(
        &request,
        Some(&chunk),
        &mut out,
        tokio_stream::iter(frames.into_iter().map(Ok)),
        None,
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
}

#[cfg(feature = "dfs")]
#[tokio::test]
async fn verified_peer_stream_requires_eof_after_complete_identity_checked_frames() {
    let (chunk, request, frames) = verified_object_frames(b"complete body");
    let mut missing_completion: Vec<_> = frames.iter().cloned().map(Ok).collect();
    missing_completion.pop();
    let mut late_status: Vec<_> = frames.iter().cloned().map(Ok).collect();
    late_status.push(Err(tonic::Status::unavailable("late stream failure")));
    let mut extra_data: Vec<_> = frames.into_iter().map(Ok).collect();
    extra_data.push(Ok(afs_protocol::node_data::DfsReadRangesFrame {
        body: Some(dfs_read_ranges_frame::Body::Data(vec![9].into())),
    }));
    for stream in [missing_completion, late_status, extra_data] {
        let mut out = vec![0; chunk.length as usize];
        assert!(
            consume_peer_read_stream(
                &request,
                Some(&chunk),
                &mut out,
                tokio_stream::iter(stream),
                None,
                None,
            )
            .await
            .is_err()
        );
    }
    let mut partial = request;
    partial.operations[0].length -= 1;
    assert!(ReadFrameDecoder::for_object(&partial, &chunk).is_err());
}

#[cfg(feature = "dfs")]
#[test]
fn verified_peer_proof_binds_private_buffer_and_cross_frame_outboard() {
    let body = vec![7; (1024 * 1024) + 17];
    let (chunk, request, frames) = verified_object_frames(&body);
    let mut out = vec![0; body.len()];
    let budget = OutboardBudget::new(1024 * 1024);
    let mut decoder = ReadFrameDecoder::for_object(&request, &chunk)
        .unwrap()
        .with_outboard_budget(Some(budget.clone()));
    for frame in frames.clone() {
        decoder.accept(frame, &mut out).unwrap();
    }
    let proof = decoder.finish_object(&out).unwrap();
    assert_eq!(proof.root_digest(), &chunk.content_digest.bytes);
    assert_eq!(proof.bytes().as_ptr(), out.as_ptr());
    let outboard = proof.into_outboard().unwrap();
    assert_eq!(outboard.digest(), &chunk.content_digest);
    let official = crate::node::chunk::outboard::build_borrowed(&body, &budget);
    assert_eq!(outboard.encoded(), official.outboard.unwrap().encoded());

    let mut decoder = ReadFrameDecoder::for_object(&request, &chunk).unwrap();
    for frame in frames {
        decoder.accept(frame, &mut out).unwrap();
    }
    let different = out.clone();
    assert!(decoder.finish_object(&different).is_err());
}

#[cfg(feature = "dfs")]
#[test]
fn range_frame_bytes_preserve_vec_wire_compatibility() {
    use afs_protocol::node_data::{
        DfsReadRangesCompletion, DfsReadRangesFrame, DfsReadRangesHeader,
    };
    use prost::Message;

    // Retain the former generated representation, including all oneof
    // tags, so both directions exercise the actual protobuf boundary.
    #[derive(Clone, PartialEq, prost::Message)]
    struct LegacyFrame {
        #[prost(oneof = "LegacyBody", tags = "1, 2, 3")]
        body: Option<LegacyBody>,
    }

    #[derive(Clone, PartialEq, prost::Oneof)]
    enum LegacyBody {
        #[prost(message, tag = "1")]
        Header(DfsReadRangesHeader),
        #[prost(bytes = "vec", tag = "2")]
        Data(Vec<u8>),
        #[prost(message, tag = "3")]
        Completion(DfsReadRangesCompletion),
    }

    let request = range_request();
    let mut frames = range_frames(&request, 0);
    frames.extend(range_frames(&request, 1));
    frames.push(DfsReadRangesFrame { body: None });
    for payload in [
        Vec::new(),
        vec![0xff],
        (0..64 * 1024).map(|index| (index % 251) as u8).collect(),
    ] {
        frames.push(DfsReadRangesFrame {
            body: Some(dfs_read_ranges_frame::Body::Data(payload.into())),
        });
    }
    for frame in frames {
        let legacy = LegacyFrame {
            body: frame.body.as_ref().map(|body| match body {
                dfs_read_ranges_frame::Body::Header(header) => LegacyBody::Header(header.clone()),
                dfs_read_ranges_frame::Body::Data(data) => {
                    let data: &prost::bytes::Bytes = data;
                    LegacyBody::Data(data.to_vec())
                }
                dfs_read_ranges_frame::Body::Completion(completion) => {
                    LegacyBody::Completion(completion.clone())
                }
            }),
        };
        let old_wire = legacy.encode_to_vec();
        let new_wire = frame.encode_to_vec();
        assert_eq!(old_wire, new_wire);
        assert_eq!(
            DfsReadRangesFrame::decode(old_wire.as_slice()).unwrap(),
            frame
        );
        assert_eq!(LegacyFrame::decode(new_wire.as_slice()).unwrap(), legacy);
    }
}

#[cfg(feature = "dfs")]
#[test]
fn range_decoder_requires_every_operation_and_completion() {
    let request = range_request();
    assert!(ReadFrameDecoder::new(&request).finish().is_err());
    let mut partial = ReadFrameDecoder::new(&request);
    let mut out = [0; 2];
    for frame in range_frames(&request, 0) {
        partial.accept(frame, &mut out).unwrap();
    }
    assert!(partial.finish().is_err());
    let mut complete = ReadFrameDecoder::new(&request);
    for index in 0..2 {
        for frame in range_frames(&request, index) {
            complete.accept(frame, &mut out).unwrap();
        }
    }
    complete.finish().unwrap();
    assert_eq!(&out, b"ab");
}

#[cfg(feature = "dfs")]
#[test]
fn range_decoder_rejects_duplicate_stale_and_corrupt_frames() {
    let request = range_request();
    let mut out = [0; 2];
    let mut decoder = ReadFrameDecoder::new(&request);
    for frame in range_frames(&request, 0) {
        decoder.accept(frame, &mut out).unwrap();
    }
    assert!(
        decoder
            .accept(range_frames(&request, 0).remove(0), &mut out)
            .is_err()
    );
    for malformed in 0..4 {
        let mut frames = range_frames(&request, 0);
        if let Some(dfs_read_ranges_frame::Body::Completion(completion)) = &mut frames[2].body {
            match malformed {
                0 => completion.attempt_id = "old-attempt".into(),
                1 => completion.operation_index = 1,
                2 => completion.range_checksum[0] ^= 1,
                _ => completion.source_copy_id = "other-copy".into(),
            }
        }
        let mut decoder = ReadFrameDecoder::new(&request);
        decoder.accept(frames.remove(0), &mut out).unwrap();
        decoder.accept(frames.remove(0), &mut out).unwrap();
        let error = decoder.accept(frames.remove(0), &mut out).unwrap_err();
        if malformed == 2 {
            assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
        } else {
            assert_eq!(error.code(), afs_error::CLIENT_PROTOCOL_VIOLATION);
        }
    }
}

#[cfg(feature = "dfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dfs_owner_client_call_deadline_bounds_but_never_extends_its_timeout() {
    use crate::node::call_deadline::within_timeout;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let hanging = tokio::spawn(async move {
        let mut sockets = Vec::new();
        while let Ok((socket, _)) = listener.accept().await {
            sockets.push(socket);
        }
    });
    let client = |endpoint: String, timeout| {
        let pool = PeerConnectionPool::new(
            afs_transport::GrpcConfig::default(),
            afs_transport::TlsConfig::Disabled,
            1,
        )
        .unwrap();
        GrpcDfsOwnerClient::new(Arc::new(pool), "owner".into(), 1, endpoint, timeout).unwrap()
    };
    let release_request = || afs_protocol::node_control::DfsOwnerReleaseRequest {
        handle: Some(afs_protocol::node_control::DfsOwnerHandle {
            owner_node_id: "owner".into(),
            ..Default::default()
        }),
    };
    let long = Duration::from_secs(30);
    let short = Duration::from_millis(40);
    for (configured, scope) in [(long, short), (short, long)] {
        let owner = client(endpoint.clone(), configured);
        let started = std::time::Instant::now();
        let result = tokio::task::spawn_blocking(move || {
            within_timeout(scope, || owner.release(release_request()))
        })
        .await
        .unwrap();
        assert!(result.is_err());
        assert!(started.elapsed() < Duration::from_secs(5));
    }
    hanging.abort();
}

#[cfg(feature = "dfs")]
#[tokio::test]
async fn peer_stream_shared_work_pins_one_channel_and_drains_after_last_block() {
    let registry = afs_metrics::Registry::new();
    let metrics = ReadWorkMetrics::register(&registry).unwrap();
    let pool = PeerConnectionPool::new(
        afs_transport::GrpcConfig::default(),
        afs_transport::TlsConfig::Disabled,
        1,
    )
    .unwrap()
    .with_read_work_metrics(metrics.clone());
    let endpoint = "http://127.0.0.1:9";
    let first = pool.acquire_stream_work("a", 1, endpoint).await.unwrap();
    let second = pool.acquire_stream_work("a", 1, endpoint).await.unwrap();
    assert!(Arc::ptr_eq(&first.session, &second.session));
    assert_eq!(first.session.blocks.lock().unwrap().len(), 2);
    let gauge = |name: &str| {
        let families = registry.gather();
        families
            .iter()
            .find(|family| family.name() == name)
            .unwrap()
            .get_metric()[0]
            .get_gauge()
            .get_value()
    };
    assert_eq!(
        gauge("afs_dfs_peer_disk_cache_stream_reserved_bytes"),
        (4 * 1024 * 1024) as f64
    );
    assert_eq!(
        gauge("afs_dfs_peer_disk_cache_stream_frame_bytes"),
        (2 * STREAM_FRAME_BACKING_BYTES) as f64
    );
    assert_eq!(
        first.session.controls.available_permits(),
        STREAM_FRAME_BACKING_BYTES
    );
    metrics.peer_stream_private_output(32 * 1024);
    assert_eq!(
        gauge("afs_dfs_peer_disk_cache_stream_reserved_bytes"),
        (4 * 1024 * 1024) as f64
    );
    assert!(
        pool.acquire_stream_work(&"a".repeat(STREAM_KEY_BYTES + 1), 1, endpoint)
            .await
            .is_err()
    );
    let controls = first
        .session
        .controls
        .clone()
        .acquire_many_owned(u32::try_from(STREAM_FRAME_BACKING_BYTES).unwrap())
        .await
        .unwrap();
    assert!(second.session.controls.clone().try_acquire_owned().is_err());
    drop(controls);
    let wait = Duration::from_millis(30);
    assert!(
        tokio::time::timeout(wait, pool.acquire_stream_work("a", 1, endpoint))
            .await
            .is_err()
    );
    // Even LRU eviction must not let a same-key replacement Channel enter
    // while the old connection still has active streams.
    pool.channel("b", 1, endpoint).await.unwrap();
    assert!(
        tokio::time::timeout(wait, pool.acquire_stream_work("b", 1, endpoint))
            .await
            .is_err()
    );
    let first_block = first.block().await.unwrap();
    let second_block = second.block().await.unwrap();
    assert_eq!(first.session.blocks.lock().unwrap().len(), 0);
    let waiting = tokio::spawn({
        let first = first.clone();
        async move { first.block().await }
    });
    tokio::task::yield_now().await;
    waiting.abort();
    assert!(waiting.await.is_err());
    let retired_session = first.session.clone();
    drop(first);
    drop(second);
    assert_eq!(
        pool.stream_work.state.lock().unwrap().active,
        2,
        "consumer-held blocks retain both operation tickets"
    );
    drop(first_block);
    assert_eq!(pool.stream_work.state.lock().unwrap().active, 1);
    drop(second_block);
    {
        let state = pool.stream_work.state.lock().unwrap();
        assert_eq!(state.active, 0);
        assert!(
            state.session.is_none(),
            "idle body allocations must be destroyed"
        );
    }
    assert!(
        retired_session.blocks.lock().unwrap().is_empty(),
        "actual buffers retire before zero admission, even while the session Arc is retained"
    );
    assert!(retired_session._work.lock().unwrap().is_none());
    for name in [
        "afs_dfs_peer_disk_cache_stream_frame_bytes",
        "afs_dfs_peer_disk_cache_stream_reserved_bytes",
    ] {
        let families = registry.gather();
        let family = families
            .iter()
            .find(|family| family.name() == name)
            .unwrap();
        assert_eq!(
            family.get_metric()[0].get_gauge().get_value(),
            0.0,
            "{name}"
        );
    }
    let next = pool.acquire_stream_work("b", 1, endpoint).await.unwrap();
    assert_eq!(next.session.blocks.lock().unwrap().len(), 2);
    drop(next);
    assert!(pool.stream_work.state.lock().unwrap().session.is_none());
}

#[cfg(feature = "ownerfs")]
#[tokio::test]
async fn peer_pool_owner_profile_is_separate_but_shares_epoch_fencing() {
    let pool = PeerConnectionPool::new(
        afs_transport::GrpcConfig::default(),
        afs_transport::TlsConfig::Disabled,
        4,
    )
    .unwrap();
    let endpoint = "http://127.0.0.1:9";
    pool.channel("peer", 1, endpoint).await.unwrap();
    pool.owner_files_channel("peer", 1, endpoint).await.unwrap();
    pool.owner_files_channel("peer", 1, endpoint).await.unwrap();
    {
        let state = pool.state.lock().await;
        assert_eq!(state.channels.len(), 2);
        assert!(
            state
                .channels
                .contains_key(&("peer".into(), 1, endpoint.into(), false))
        );
        assert!(
            state
                .channels
                .contains_key(&("peer".into(), 1, endpoint.into(), true))
        );
    }
    pool.owner_files_channel("peer", 2, endpoint).await.unwrap();
    assert_eq!(pool.state.lock().await.channels.len(), 1);
    assert!(pool.channel("peer", 1, endpoint).await.is_err());
    assert!(pool.long_wait_channel("peer", 1, endpoint).await.is_err());
    pool.channel("peer", 3, endpoint).await.unwrap();
    assert_eq!(pool.state.lock().await.channels.len(), 1);
    assert!(pool.owner_files_channel("peer", 2, endpoint).await.is_err());
}

#[tokio::test]
async fn peer_pool_never_regresses_epoch_even_after_channel_eviction() {
    let pool = Arc::new(
        PeerConnectionPool::new(
            afs_transport::GrpcConfig::default(),
            afs_transport::TlsConfig::Disabled,
            1,
        )
        .unwrap(),
    );
    let endpoint = "http://127.0.0.1:9";
    pool.channel("peer", 1, endpoint).await.unwrap();
    let release_old = Arc::new(tokio::sync::Notify::new());
    let delayed_pool = pool.clone();
    let delayed_gate = release_old.clone();
    let old_lookup = tokio::spawn(async move {
        delayed_gate.notified().await;
        delayed_pool.channel("peer", 1, endpoint).await
    });
    pool.channel("peer", 2, endpoint).await.unwrap();
    release_old.notify_one();
    assert!(old_lookup.await.unwrap().is_err());
    {
        let state = pool.state.lock().await;
        assert_eq!(state.high_water_epochs.get("peer"), Some(&2));
        assert!(
            state
                .channels
                .keys()
                .all(|(node, epoch, _, _)| node != "peer" || *epoch == 2)
        );
    }
    pool.channel("other", 1, endpoint).await.unwrap(); // evicts peer's channel, not its epoch
    assert!(pool.channel("peer", 1, endpoint).await.is_err());
    pool.channel("peer", 3, endpoint).await.unwrap();
    assert!(pool.channel("peer", 2, endpoint).await.is_err());
    assert_eq!(
        pool.state.lock().await.high_water_epochs.get("peer"),
        Some(&3)
    );
}

#[cfg(feature = "dfs")]
#[test]
fn dfs_auto_factories_without_rdma_pool_choose_grpc_and_required_rdma_fails() {
    let peers = Arc::new(
        PeerConnectionPool::new(
            afs_transport::GrpcConfig::default(),
            afs_transport::TlsConfig::Disabled,
            4,
        )
        .unwrap(),
    );

    let replica = make_replica_data_plane(
        DataMode::Auto,
        peers.clone(),
        Duration::from_millis(10),
        None,
    )
    .unwrap();
    assert_eq!(
        replica.mode(),
        crate::node::replication::ReplicaTransferMode::GrpcStream
    );
    assert!(
        make_replica_data_plane(
            DataMode::Rdma,
            peers.clone(),
            Duration::from_millis(10),
            None
        )
        .is_err()
    );

    let _transfer =
        make_chunk_transfer(DataMode::Auto, peers, Duration::from_millis(10), None).unwrap();
    assert!(
        make_chunk_transfer(
            DataMode::Rdma,
            Arc::new(
                PeerConnectionPool::new(
                    afs_transport::GrpcConfig::default(),
                    afs_transport::TlsConfig::Disabled,
                    4,
                )
                .unwrap()
            ),
            Duration::from_millis(10),
            None
        )
        .is_err()
    );
}
#[cfg(all(feature = "dfs", feature = "rdma"))]
mod dfs_rdma_tests {
    use super::*;

    #[test]
    fn dfs_rdma_unsupported_reply_contract_accepts_only_canonical_false() {
        let canonical = DfsNegotiateRdmaReply {
            session_id: 0,
            server_info: Vec::new(),
            capacity: 0,
            rdma_supported: false,
            handshake_version: RDMA_HANDSHAKE_VERSION,
        };
        assert!(dfs_rdma_canonical_unsupported(&canonical));

        let malformed_session = DfsNegotiateRdmaReply {
            session_id: 9,
            ..canonical.clone()
        };
        assert!(!dfs_rdma_canonical_unsupported(&malformed_session));

        let malformed_info = DfsNegotiateRdmaReply {
            server_info: vec![1],
            ..canonical.clone()
        };
        assert!(!dfs_rdma_canonical_unsupported(&malformed_info));

        let malformed_capacity = DfsNegotiateRdmaReply {
            capacity: 1,
            ..canonical.clone()
        };
        assert!(!dfs_rdma_canonical_unsupported(&malformed_capacity));

        let malformed_version = DfsNegotiateRdmaReply {
            handshake_version: RDMA_HANDSHAKE_VERSION + 1,
            ..canonical.clone()
        };
        assert!(!dfs_rdma_canonical_unsupported(&malformed_version));

        let supported = DfsNegotiateRdmaReply {
            rdma_supported: true,
            ..canonical
        };
        assert!(!dfs_rdma_canonical_unsupported(&supported));
    }

    #[test]
    fn read_completion_binds_attempt_copy_range_and_payload_checksum() {
        let request = DfsReadRangesRequest {
            include_blake3_outboard: false,
            read_id: "read".into(),
            attempt_id: "attempt".into(),
            file_version_id: "version".into(),
            layout_root_id: "layout".into(),
            operations: vec![DfsChunkReadOp {
                operation_index: 0,
                chunk_id: "chunk".into(),
                chunk_offset: 7,
                length: 3,
                destination_offset: 0,
                source_copy_id: "copy".into(),
                grant: None,
            }],
        };
        let reply = afs_protocol::node_data::DfsReadRangesRdmaReply {
            read_id: request.read_id.clone(),
            attempt_id: request.attempt_id.clone(),
            transferred_bytes: 3,
            completions: vec![afs_protocol::node_data::DfsReadRangesCompletion {
                crc32c_table: Default::default(),
                read_id: request.read_id.clone(),
                attempt_id: request.attempt_id.clone(),
                operation_index: 0,
                source_copy_id: "copy".into(),
                transferred_bytes: 3,
                range_checksum: blake3::hash(b"abc").as_bytes().to_vec(),
                range_checksum_algorithm: afs_protocol::node_data::DfsDigestAlgorithm::Blake3
                    as i32,
            }],
        };
        validate_dfs_rdma_read_reply(&request, &reply, b"abc").unwrap();
        assert_eq!(
            validate_dfs_rdma_read_reply(&request, &reply, b"abd")
                .unwrap_err()
                .code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
        let mut wrong = reply.clone();
        wrong.attempt_id = "another".into();
        assert_eq!(
            validate_dfs_rdma_read_reply(&request, &wrong, b"abc")
                .unwrap_err()
                .code(),
            afs_error::CLIENT_PROTOCOL_VIOLATION
        );
        let mut wrong = reply.clone();
        wrong.completions[0].source_copy_id = "another".into();
        assert_eq!(
            validate_dfs_rdma_read_reply(&request, &wrong, b"abc")
                .unwrap_err()
                .code(),
            afs_error::CLIENT_PROTOCOL_VIOLATION
        );
        let mut wrong = reply;
        wrong.completions[0].transferred_bytes = 2;
        assert_eq!(
            validate_dfs_rdma_read_reply(&request, &wrong, b"abc")
                .unwrap_err()
                .code(),
            afs_error::CLIENT_PROTOCOL_VIOLATION
        );
    }

    #[test]
    fn read_windows_split_at_registered_capacity_and_preserve_destinations() {
        use crate::dfs::*;
        use crate::node::read::engine::{ChunkReadOp, PeerReadBatch, ResolvedReadOp};
        let size = 3 * 1024 * 1024;
        let batch = PeerReadBatch {
            file_version_id: FileVersionId::new("version"),
            layout_root_id: LayoutRootId::new("layout"),
            operations: (0..2)
                .map(|i| ResolvedReadOp {
                    op: ChunkReadOp {
                        chunk_id: ChunkId::new(format!("chunk-{i}")),
                        chunk_offset: 0,
                        length: size as u64,
                        output_offset: i * size,
                    },
                    source: SourceCandidate {
                        copy_id: CopyId::new(format!("copy-{i}")),
                        chunk_id: ChunkId::new(format!("chunk-{i}")),
                        role: CopyRole::DurableReplica,
                        state: CopyState::Ready,
                        location: CopyLocation::Node {
                            node_id: "source".into(),
                            node_epoch: 1,
                            device_id: "disk".into(),
                            device_epoch: 1,
                            catalog_revision: 1,
                        },
                        data_endpoint: Some("http://127.0.0.1:1".into()),
                        load_hint: 0,
                        read_grant: DfsReadGrant {
                            namespace_id: NamespaceId::new("ns"),
                            file_version_id: FileVersionId::new("version"),
                            layout_root_id: LayoutRootId::new("layout"),
                            caller_node_id: "reader".into(),
                            caller_node_epoch: 1,
                            expires_at_unix_ms: u64::MAX,
                            fence: 1,
                            token: "fixture".into(),
                        },
                    },
                })
                .collect(),
        };
        let windows = pack_dfs_rdma_read_windows(&batch, 42).unwrap();
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].2, size);
        assert_eq!(windows[1].2, size);
        assert_eq!(windows[0].1, vec![(0, 0..size)]);
        assert_eq!(windows[1].1, vec![(size, 0..size)]);
        assert_eq!(windows[1].0.operations[0].operation_index, 0);
        assert_eq!(windows[1].0.operations[0].destination_offset, 0);
        assert_ne!(windows[0].0.attempt_id, windows[1].0.attempt_id);
    }
}
