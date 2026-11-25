//! H2 step 2b: the holder form of `ReadRanges` (plan/15 v3 §1.3, §2.2.2–§2.2.4).
use super::*;
use crate::dfs::{
    DfsAuthorizedHolderRead, DfsHolderGrant, Extent, FileVersionId, LayoutRoot, LayoutRootId,
    NamespaceId, ValidateDfsHolderGrant,
};
use crate::node::chunk::StagedChunk;
use crate::node::read::holders::{HolderInFlight, HolderRegistry};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_stream::StreamExt;

const TOKEN: &str =
    "dfs-read-holder-v1:0000000000000000000000000000000000000000000000000000000000000000";

/// Meta for the holder: answers for the one grant it was told is genuine.
struct FakeMeta {
    genuine: Mutex<Option<DfsHolderGrant>>,
    calls: AtomicUsize,
    /// Answer with this long a validity (ms).
    keep_ms: u64,
}

impl DfsHolderGrantValidator for FakeMeta {
    fn validate(
        &self,
        request: ValidateDfsHolderGrant,
    ) -> afs_error::Result<DfsAuthorizedHolderRead> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let genuine = self.genuine.lock().unwrap().clone();
        if genuine.as_ref() == Some(&request.grant) && request.peer_node_id == "reader" {
            Ok(DfsAuthorizedHolderRead {
                grant: request.grant,
                expires_at_unix_ms: crate::clock::now_unix_ms() + self.keep_ms,
            })
        } else {
            Err(afs_error::Error::new(
                afs_error::CLIENT_REMOTE_STATUS,
                afs_error::ErrorKind::PermissionDenied,
                "holder grant is not valid",
            ))
        }
    }
}

struct Holder {
    _temp: tempfile::TempDir,
    metrics: afs_metrics::Registry,
    local: Arc<LocalChunkStore>,
    meta: Arc<FakeMeta>,
    registry: Arc<HolderRegistry>,
    in_flight: Arc<HolderInFlight>,
    service: DfsChunksService,
    present: StagedChunk,
    absent: StagedChunk,
    version: FileVersionId,
    layout: LayoutRoot,
}

fn grant(version: &str, layout: &str) -> DfsHolderGrant {
    DfsHolderGrant {
        namespace_id: NamespaceId::new("namespace"),
        file_version_id: FileVersionId::new(version),
        layout_root_id: LayoutRootId::new(layout),
        caller_node_id: "reader".into(),
        caller_node_epoch: 1,
        holder_node_id: "holder".into(),
        holder_node_epoch: 9,
        // Fixed, so every call builds the same grant (2096-10-02).
        expires_at_unix_ms: 4_000_000_000_000,
        token: TOKEN.into(),
    }
}

/// A holder sharing version "v" (layout "l") of two 64 KiB Chunks, one of
/// them installed here; the file ends 1 KiB into the second Chunk... no: the
/// file is 128 KiB, the second Chunk's extent is cut at 96 KiB.
fn holder(keep_ms: u64) -> Holder {
    let temp = tempfile::tempdir().unwrap();
    // The longest product admission wait, so T19 can tell a fresh deadline
    // (taken after the wait) from one taken when the request arrived.
    let local = Arc::new(
        LocalChunkStore::open(temp.path(), "holder")
            .unwrap()
            .with_peer_serve_wait(std::time::Duration::from_millis(50))
            .unwrap(),
    );
    let registry_metrics = afs_metrics::Registry::new();
    let metrics = crate::node::metrics::work::ReadWorkMetrics::register(&registry_metrics);
    let local = match metrics {
        Ok(metrics) => Arc::new(
            Arc::try_unwrap(local)
                .ok()
                .unwrap()
                .with_read_work_metrics(metrics),
        ),
        Err(_) => local,
    };
    let present = StagedChunk::new(crate::dfs::OperationId::new("present"), vec![5; 65536]);
    let absent = StagedChunk::new(crate::dfs::OperationId::new("absent"), vec![6; 65536]);
    local.put(present.clone()).unwrap();
    let layout = LayoutRoot {
        id: LayoutRootId::new("l"),
        file_length: 65536 + 32768,
        inline_extents: vec![
            Extent {
                file_offset: 0,
                length: 65536,
                chunk_id: present.chunk.id.clone(),
                chunk_offset: 0,
            },
            Extent {
                file_offset: 65536,
                length: 65536,
                chunk_id: absent.chunk.id.clone(),
                chunk_offset: 0,
            },
        ],
    };
    let version = FileVersionId::new("v");
    let registry = HolderRegistry::new();
    registry.retain(&version, 1);
    registry.attach(&version, &layout);
    let in_flight = HolderInFlight::new();
    let meta = Arc::new(FakeMeta {
        genuine: Mutex::new(Some(grant("v", "l"))),
        calls: AtomicUsize::new(0),
        keep_ms,
    });
    let mut service = dfs_reuse_test_service(local.clone(), Arc::new(DenyDfsReadAuthorizer));
    service.holders = Some(crate::node::rpc::data::HolderServe::new(
        meta.clone(),
        registry.clone(),
        in_flight.clone(),
        "holder".into(),
        9,
    ));
    Holder {
        _temp: temp,
        metrics: registry_metrics,
        local,
        meta,
        registry,
        in_flight,
        service,
        present,
        absent,
        version,
        layout,
    }
}

fn holder_op(
    index: u32,
    chunk: &str,
    offset: u64,
    length: u64,
    grant: &DfsHolderGrant,
) -> afs_protocol::node_data::DfsChunkReadOp {
    afs_protocol::node_data::DfsChunkReadOp {
        operation_index: index,
        chunk_id: chunk.into(),
        chunk_offset: offset,
        length,
        destination_offset: 0,
        source_copy_id: "holder:holder:9".into(),
        grant: None,
        holder_grant: Some(grant.clone().into()),
        wait_ms: 0,
        fetch_if_absent: false,
    }
}

fn holder_request(ops: Vec<afs_protocol::node_data::DfsChunkReadOp>) -> DfsReadRangesRequest {
    DfsReadRangesRequest {
        include_blake3_outboard: false,
        read_id: "read".into(),
        attempt_id: "attempt".into(),
        file_version_id: "v".into(),
        layout_root_id: "l".into(),
        operations: ops,
    }
}

async fn read(
    service: &DfsChunksService,
    request: DfsReadRangesRequest,
) -> Result<Vec<u8>, Status> {
    let mut stream = service
        .read_ranges(Request::new(request))
        .await?
        .into_inner();
    let mut received = Vec::new();
    while let Some(frame) = stream.next().await {
        if let dfs_read_ranges_frame::Body::Data(data) = frame?.body.unwrap() {
            received.extend(data);
        }
    }
    Ok(received)
}

fn denied(result: Result<Vec<u8>, Status>) -> String {
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::PermissionDenied, "{status:?}");
    status.message().to_owned()
}

fn not_ready(result: Result<Vec<u8>, Status>) {
    let error = afs_transport::grpc::error_status::status_to_error(result.unwrap_err());
    assert_eq!(
        error.code(),
        afs_error::NODE_TRANSFER_NOT_READY,
        "{error:?}"
    );
}

#[tokio::test]
async fn h2_holder_serves_an_authorized_range_of_a_shared_version() {
    let h = holder(30_000);
    let g = grant("v", "l");
    let bytes = read(
        &h.service,
        holder_request(vec![holder_op(0, &h.present.chunk.id.0, 0, 65536, &g)]),
    )
    .await
    .unwrap();
    assert_eq!(bytes, h.present.bytes());
    // A second read within the authorization asks Meta no more.
    read(
        &h.service,
        holder_request(vec![holder_op(0, &h.present.chunk.id.0, 0, 4096, &g)]),
    )
    .await
    .unwrap();
    assert_eq!(h.meta.calls.load(Ordering::SeqCst), 1);
}

/// T1, T2: an invalid grant is refused before anything about this Node is
/// looked at: no wait, no in-flight registration, the same error for a
/// present, an in-flight and an absent Chunk, and `fetch_if_absent` changes
/// nothing.
#[tokio::test]
async fn h2_t1_t2_invalid_grant_reveals_nothing_and_starts_nothing() {
    let h = holder(30_000);
    let mut forged = grant("v", "l");
    forged.token = TOKEN.replace('0', "1");
    let _fetching = h.in_flight.begin(&h.absent.chunk.id);
    let unknown = StagedChunk::new(crate::dfs::OperationId::new("unknown"), vec![1; 4096]);
    let mut messages = Vec::new();
    for chunk in [
        &h.present.chunk.id.0,
        &h.absent.chunk.id.0,
        &unknown.chunk.id.0,
    ] {
        let mut op = holder_op(0, chunk, 0, 4096, &forged);
        op.wait_ms = 400;
        op.fetch_if_absent = true;
        let started = std::time::Instant::now();
        messages.push(denied(read(&h.service, holder_request(vec![op])).await));
        assert!(
            started.elapsed() < std::time::Duration::from_millis(300),
            "it waited"
        );
    }
    assert!(
        messages.windows(2).all(|pair| pair[0] == pair[1]),
        "{messages:?}"
    );
    // Only the one in-flight mark this test made.
    drop(_fetching);
    assert!(!h.in_flight.contains(&h.absent.chunk.id));
}

/// T3: waiters beyond the per-peer limit are answered not ready at once and
/// never hold a read slot.
#[tokio::test]
async fn h2_t3_waiters_are_bounded_and_hold_no_read_slot() {
    use crate::node::rpc::data::dfs_holder_serve::MAX_WAITERS_PER_PEER;
    let h = holder(30_000);
    let g = grant("v", "l");
    let _fetching = h.in_flight.begin(&h.absent.chunk.id);
    let service = Arc::new(h.service.clone());
    let mut waiting = Vec::new();
    for _ in 0..MAX_WAITERS_PER_PEER {
        let service = service.clone();
        let mut op = holder_op(0, &h.absent.chunk.id.0, 0, 4096, &g);
        op.wait_ms = 500;
        waiting.push(tokio::spawn(async move {
            read(&service, holder_request(vec![op])).await
        }));
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        service.read_permits.available_permits(),
        8,
        "a waiter holds a slot"
    );
    let mut op = holder_op(0, &h.absent.chunk.id.0, 0, 4096, &g);
    op.wait_ms = 500;
    let started = std::time::Instant::now();
    not_ready(read(&service, holder_request(vec![op])).await);
    assert!(started.elapsed() < std::time::Duration::from_millis(200));
    for task in waiting {
        not_ready(task.await.unwrap());
    }
}

/// T4: a wait that outlasts the authorization re-validates before a slot;
/// a caller Meta no longer accepts is then refused.
#[tokio::test]
async fn h2_t4_authorization_expired_during_the_wait_is_validated_again() {
    let h = holder(150);
    let g = grant("v", "l");
    let fetching = h.in_flight.begin(&h.absent.chunk.id);
    let mut op = holder_op(0, &h.absent.chunk.id.0, 0, 4096, &g);
    op.wait_ms = 500;
    let service = Arc::new(h.service.clone());
    let task = {
        let service = service.clone();
        tokio::spawn(async move { read(&service, holder_request(vec![op])).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    // The caller is fenced, then the Chunk arrives.
    *h.meta.genuine.lock().unwrap() = None;
    h.local.put(h.absent.clone()).unwrap();
    drop(fetching);
    denied(task.await.unwrap());
    assert_eq!(h.meta.calls.load(Ordering::SeqCst), 2);
}

/// T5, T20: request ids that differ from the grant's, and a request that
/// mixes a holder grant with a per-copy grant, are refused.
#[tokio::test]
async fn h2_t5_t20_mismatched_ids_and_mixed_grants_are_refused() {
    let h = holder(30_000);
    let g = grant("v", "l");
    let mut other_layout = holder_request(vec![holder_op(0, &h.present.chunk.id.0, 0, 4096, &g)]);
    other_layout.layout_root_id = "other".into();
    denied(read(&h.service, other_layout).await);
    let mut other_version = holder_request(vec![holder_op(0, &h.present.chunk.id.0, 0, 4096, &g)]);
    other_version.file_version_id = "other".into();
    denied(read(&h.service, other_version).await);
    let per_copy = dfs_read_request(h.present.chunk.id.0.clone(), 4096)
        .operations
        .remove(0);
    let mut mixed = holder_request(vec![holder_op(0, &h.present.chunk.id.0, 0, 4096, &g)]);
    let mut second = per_copy.clone();
    second.operation_index = 1;
    mixed.operations.push(second);
    denied(read(&h.service, mixed).await);
    // Both grants on one operation.
    let mut both = holder_op(0, &h.present.chunk.id.0, 0, 4096, &g);
    both.grant = per_copy.grant.clone();
    denied(read(&h.service, holder_request(vec![both])).await);
}

/// T8, T10, T10b: not shared here (or the empty sentinel) is not ready; a
/// range past the version's intervals or a Chunk outside its layout is
/// refused even with a cached authorization.
#[tokio::test]
async fn h2_t8_t10_ranges_are_checked_on_every_request() {
    let h = holder(30_000);
    let g = grant("v", "l");
    read(
        &h.service,
        holder_request(vec![holder_op(0, &h.present.chunk.id.0, 0, 4096, &g)]),
    )
    .await
    .unwrap();
    // Cached now. The second Chunk ends at 32 KiB in this version (T7/T10).
    h.local.put(h.absent.clone()).unwrap();
    read(
        &h.service,
        holder_request(vec![holder_op(0, &h.absent.chunk.id.0, 0, 32768, &g)]),
    )
    .await
    .unwrap();
    denied(
        read(
            &h.service,
            holder_request(vec![holder_op(0, &h.absent.chunk.id.0, 0, 32769, &g)]),
        )
        .await,
    );
    let other = StagedChunk::new(crate::dfs::OperationId::new("other"), vec![2; 4096]);
    h.local.put(other.clone()).unwrap();
    denied(
        read(
            &h.service,
            holder_request(vec![holder_op(0, &other.chunk.id.0, 0, 4096, &g)]),
        )
        .await,
    );
    assert_eq!(h.meta.calls.load(Ordering::SeqCst), 1);
    // Not shared any more: not ready (the caller is authorized).
    h.registry.release(&h.version, 1);
    not_ready(
        read(
            &h.service,
            holder_request(vec![holder_op(0, &h.present.chunk.id.0, 0, 4096, &g)]),
        )
        .await,
    );
    // The empty sentinel is never shared.
    *h.meta.genuine.lock().unwrap() = Some(grant("empty", "empty"));
    let sentinel = grant("empty", "empty");
    let mut request = holder_request(vec![holder_op(
        0,
        &h.present.chunk.id.0,
        0,
        4096,
        &sentinel,
    )]);
    request.file_version_id = "empty".into();
    request.layout_root_id = "empty".into();
    not_ready(read(&h.service, request).await);
    let _ = &h.layout;
}

/// T11, T12: per-copy entries refuse a holder token, and a holder token
/// never reaches the per-copy authorizer.
#[tokio::test]
async fn h2_t11_t12_grant_kinds_do_not_cross_entries() {
    let h = holder(30_000);
    let g = grant("v", "l");
    let holder_form = holder_request(vec![holder_op(0, &h.present.chunk.id.0, 0, 4096, &g)]);
    // RDMA-style validation of the per-copy path refuses the holder form.
    assert_eq!(
        validate_dfs_read_request(&holder_form).unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    assert!(dfs_read_caller_epoch("reader", &holder_form).is_err());
    // An operation with a valid per-copy grant that also carries a holder
    // grant is refused by the per-copy entries too (gRPC and RDMA).
    let mut both = dfs_read_request(h.present.chunk.id.0.clone(), 4096);
    both.operations[0].holder_grant = Some(g.clone().into());
    assert_eq!(
        validate_dfs_read_request(&both).unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    assert!(dfs_read_caller_epoch("reader", &both).is_err());
    // A per-copy token sent as a holder grant is refused by the holder path.
    let mut per_copy_token = g.clone();
    per_copy_token.token = format!("dfs-read-v1:{}", "0".repeat(64));
    denied(
        read(
            &h.service,
            holder_request(vec![holder_op(
                0,
                &h.present.chunk.id.0,
                0,
                4096,
                &per_copy_token,
            )]),
        )
        .await,
    );
    // A holder-looking copy id with a per-copy grant takes the per-copy path.
    let mut request = dfs_read_request(h.present.chunk.id.0.clone(), 4096);
    request.operations[0].source_copy_id = "holder:holder:9".into();
    assert!(read(&h.service, request).await.is_err());
    assert_eq!(h.meta.calls.load(Ordering::SeqCst), 0);
}

/// T14: the holder keeps an authorization at most 2 s; after that a fenced
/// caller is refused.
#[tokio::test]
async fn h2_t14_holder_authorization_is_kept_at_most_two_seconds() {
    let h = holder(30_000);
    let g = grant("v", "l");
    read(
        &h.service,
        holder_request(vec![holder_op(0, &h.present.chunk.id.0, 0, 4096, &g)]),
    )
    .await
    .unwrap();
    *h.meta.genuine.lock().unwrap() = None;
    read(
        &h.service,
        holder_request(vec![holder_op(0, &h.present.chunk.id.0, 0, 4096, &g)]),
    )
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2_050)).await;
    denied(
        read(
            &h.service,
            holder_request(vec![holder_op(0, &h.present.chunk.id.0, 0, 4096, &g)]),
        )
        .await,
    );
}

/// T19 (R2): a 300 ms wait for an in-flight Chunk ends in a served read.
/// Every read slot is busy when the Chunk arrives and frees 10 ms later: the
/// admission deadline must start after the wait, or the read is refused as
/// busy at once.
#[tokio::test]
async fn h2_t19_served_after_waiting_for_an_in_flight_chunk() {
    let h = holder(30_000);
    let g = grant("v", "l");
    let slots = h
        .service
        .read_permits
        .clone()
        .acquire_many_owned(8)
        .await
        .unwrap();
    let fetching = h.in_flight.begin(&h.absent.chunk.id);
    let mut op = holder_op(0, &h.absent.chunk.id.0, 0, 32768, &g);
    op.wait_ms = 500;
    let service = Arc::new(h.service.clone());
    let task = {
        let service = service.clone();
        tokio::spawn(async move { read(&service, holder_request(vec![op])).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    h.local.put(h.absent.clone()).unwrap();
    drop(fetching);
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    drop(slots);
    let bytes = task.await.unwrap().unwrap();
    assert_eq!(bytes, h.absent.bytes()[..32768]);
    // review/18 §8.3: the wait, the waiters, the stages are measured.
    let count = |name: &str, label: &str| histogram_count(&h.metrics, name, label);
    assert_eq!(
        count("afs_dfs_read_holder_wait_seconds", "outcome=\"ready\""),
        1
    );
    assert_eq!(count("afs_dfs_read_holder_waiters", ""), 1);
    for stage in ["authorize", "slot", "serve"] {
        assert_eq!(
            count(
                "afs_dfs_read_holder_request_seconds",
                &format!("stage=\"{stage}\"")
            ),
            1,
            "{stage}"
        );
    }
    // The slot wait covers the 8 slots held until the Chunk arrived.
    let slot = histogram_sum(
        &h.metrics,
        "afs_dfs_read_holder_request_seconds",
        "stage=\"slot\"",
    );
    assert!(slot >= 0.005, "{slot}");
}

/// `_count` of a histogram series whose labels contain `label`.
fn histogram_count(registry: &afs_metrics::Registry, name: &str, label: &str) -> u64 {
    histogram_value(registry, &format!("{name}_count"), label) as u64
}

fn histogram_sum(registry: &afs_metrics::Registry, name: &str, label: &str) -> f64 {
    histogram_value(registry, &format!("{name}_sum"), label)
}

fn histogram_value(registry: &afs_metrics::Registry, series: &str, label: &str) -> f64 {
    afs_metrics::encode_text(registry)
        .unwrap()
        .lines()
        .filter(|line| line.starts_with(series) && line.contains(label))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

/// Absent and not in flight, or `wait_ms` 0: not ready at once.
#[tokio::test]
async fn h2_absent_chunk_is_not_ready_without_waiting() {
    let h = holder(30_000);
    let g = grant("v", "l");
    let mut op = holder_op(0, &h.absent.chunk.id.0, 0, 4096, &g);
    op.wait_ms = 500;
    op.fetch_if_absent = true;
    let started = std::time::Instant::now();
    not_ready(read(&h.service, holder_request(vec![op])).await);
    assert!(started.elapsed() < std::time::Duration::from_millis(100));
    let _fetching = h.in_flight.begin(&h.absent.chunk.id);
    not_ready(
        read(
            &h.service,
            holder_request(vec![holder_op(0, &h.absent.chunk.id.0, 0, 4096, &g)]),
        )
        .await,
    );
}

/// Reader-side Meta for the end-to-end test: one Chunk, a writer that cannot
/// be reached, and one holder.
struct E2eMeta {
    reply: crate::dfs::DfsChunkSourcesReply,
    holder: crate::dfs::VersionHolder,
    holder_lookups: AtomicUsize,
}

impl crate::node::read::engine::ReadSourceProvider for E2eMeta {
    fn sources_for(
        &self,
        _: crate::dfs::DfsChunkSourcesRequest,
    ) -> afs_error::Result<crate::dfs::DfsChunkSourcesReply> {
        Ok(self.reply.clone())
    }

    fn version_holders_async(
        &self,
        _: crate::dfs::DfsVersionHoldersRequest,
    ) -> crate::node::read::admission::ReadFuture<'_, Vec<crate::dfs::VersionHolder>> {
        Box::pin(async move {
            self.holder_lookups.fetch_add(1, Ordering::SeqCst);
            Ok(vec![self.holder.clone()])
        })
    }
}

/// P1 (code review): a CRC-segment Chunk — what native DFS writes — read by a
/// real reader engine over the real gRPC transfer from a real holder service.
/// The writer is unreachable, so the bytes can only come from the holder.
#[tokio::test(flavor = "multi_thread")]
async fn h2_p1_crc_chunk_is_read_from_a_holder_end_to_end() {
    use crate::node::read::engine::{ChunkReadOp, DfsReadConfig, DfsReadEngine, ReadBatch};
    let temp = tempfile::tempdir().unwrap();
    // Holder: the CRC Chunk installed, the version shared with its layout.
    let holder_local =
        Arc::new(LocalChunkStore::open(temp.path().join("holder"), "holder").unwrap());
    let bytes: Vec<u8> = (0..(3 * 65536 + 17)).map(|i| (i % 251) as u8).collect();
    let staged =
        StagedChunk::new_crc32c(crate::dfs::OperationId::new("crc"), bytes.clone()).unwrap();
    assert!(staged.chunk.is_segment_crc32c());
    holder_local.put(staged.clone()).unwrap();
    let layout = LayoutRoot {
        id: LayoutRootId::new("l"),
        file_length: staged.chunk.length,
        inline_extents: vec![Extent {
            file_offset: 0,
            length: staged.chunk.length,
            chunk_id: staged.chunk.id.clone(),
            chunk_offset: 0,
        }],
    };
    let registry = HolderRegistry::new();
    let version = FileVersionId::new("v");
    registry.retain(&version, 1);
    registry.attach(&version, &layout);
    let meta = Arc::new(FakeMeta {
        genuine: Mutex::new(Some(grant("v", "l"))),
        calls: AtomicUsize::new(0),
        keep_ms: 30_000,
    });
    let mut service = dfs_reuse_test_service(holder_local.clone(), Arc::new(DenyDfsReadAuthorizer));
    service.holders = Some(crate::node::rpc::data::HolderServe::new(
        meta.clone(),
        registry,
        HolderInFlight::new(),
        "holder".into(),
        9,
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(
        Server::builder()
            .add_service(afs_protocol::node_data::dfs_chunks_server::DfsChunksServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    // Reader: Meta offers the unreachable writer and the holder.
    let writer = crate::dfs::SourceCandidate {
        copy_id: crate::dfs::CopyId::new("writer-copy"),
        chunk_id: staged.chunk.id.clone(),
        role: crate::dfs::CopyRole::DurableReplica,
        state: crate::dfs::CopyState::Ready,
        location: crate::dfs::CopyLocation::Node {
            node_id: "writer".into(),
            node_epoch: 1,
            device_id: "d".into(),
            device_epoch: 1,
            catalog_revision: 1,
        },
        data_endpoint: Some("http://127.0.0.1:9".into()),
        load_hint: 0,
        read_grant: crate::dfs::DfsReadGrant {
            namespace_id: NamespaceId::new("namespace"),
            file_version_id: FileVersionId::new("v"),
            layout_root_id: LayoutRootId::new("l"),
            caller_node_id: "reader".into(),
            caller_node_epoch: 1,
            expires_at_unix_ms: 4_000_000_000_000,
            fence: 1,
            token: "per-copy".into(),
        },
        holder_grant: None,
    };
    let reader_meta = Arc::new(E2eMeta {
        reply: crate::dfs::DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![crate::dfs::ChunkSources {
                chunk_id: staged.chunk.id.clone(),
                chunk: Some(staged.chunk.clone()),
                allowed_ranges: vec![(0, staged.chunk.length)],
                sources: vec![writer],
            }],
        },
        holder: crate::dfs::VersionHolder {
            node_id: "holder".into(),
            node_epoch: 9,
            data_endpoint: endpoint,
            grant: grant("v", "l"),
        },
        holder_lookups: AtomicUsize::new(0),
    });
    let registry_metrics = afs_metrics::Registry::new();
    let metrics = crate::node::metrics::work::ReadWorkMetrics::register(&registry_metrics).unwrap();
    let pool = Arc::new(
        crate::node::rpc::peer::PeerConnectionPool::new(
            afs_transport::GrpcConfig::default(),
            afs_transport::TlsConfig::Disabled,
            1,
        )
        .unwrap(),
    );
    let transfer = Arc::new(
        crate::node::rpc::peer::GrpcChunkTransfer::new(pool, std::time::Duration::from_secs(5))
            .unwrap(),
    );
    let reader_local = Arc::new(
        LocalChunkStore::open(temp.path().join("reader"), "reader")
            .unwrap()
            .with_read_work_metrics(metrics),
    );
    let engine = Arc::new(
        DfsReadEngine::new(
            NamespaceId::new("namespace"),
            "reader".into(),
            reader_local,
            reader_meta.clone(),
            transfer,
            DfsReadConfig::default(),
        )
        .with_read_authority(Arc::new(
            crate::node::read::engine::LocalReadAuthority::new(
                1,
                std::time::Instant::now() + std::time::Duration::from_secs(30),
            ),
        ))
        .with_holder_lists(std::time::Duration::from_secs(60)),
    );
    let batch = ReadBatch {
        file_version_id: Some(FileVersionId::new("v")),
        layout_root_id: LayoutRootId::new("l"),
        ops: vec![ChunkReadOp {
            chunk_id: staged.chunk.id.clone(),
            chunk_offset: 0,
            length: staged.chunk.length,
            output_offset: 0,
        }],
        lookahead: Vec::new(),
    };
    for _ in 0..2 {
        let engine = engine.clone();
        let batch = batch.clone();
        let read = tokio::task::spawn_blocking(move || {
            let mut out = vec![0; batch.ops[0].length as usize];
            engine.read_batch(&batch, &mut out).map(|_| out)
        })
        .await
        .unwrap();
        assert_eq!(read.expect("the holder serves the CRC Chunk"), bytes);
    }
    assert_eq!(reader_meta.holder_lookups.load(Ordering::SeqCst), 1);
    assert_eq!(
        meta.calls.load(Ordering::SeqCst),
        1,
        "one validation, then the cache"
    );
    let text = afs_metrics::encode_text(&registry_metrics).unwrap();
    let served = text
        .lines()
        .find_map(|line| {
            line.strip_prefix("afs_dfs_read_source_bytes_total{role=\"peer_holder\"} ")
        })
        .unwrap();
    assert_eq!(served.parse::<u64>().unwrap(), 2 * staged.chunk.length);
    // review/18 §8.3: each attempt by source role and outcome; route stages.
    let count = |name: &str, label: &str| histogram_count(&registry_metrics, name, label);
    let attempts = "afs_dfs_read_peer_attempt_seconds";
    let holder_ok = afs_metrics::encode_text(&registry_metrics)
        .unwrap()
        .lines()
        .filter(|line| {
            line.starts_with(&format!("{attempts}_count"))
                && line.contains("role=\"peer_holder\"")
                && line.contains("outcome=\"ok\"")
        })
        .filter_map(|line| line.rsplit(' ').next()?.parse::<u64>().ok())
        .sum::<u64>();
    assert_eq!(holder_ok, 2);
    assert_eq!(
        count(attempts, "role=\"durable\""),
        0,
        "the writer is never tried"
    );
    for stage in ["sources", "holder_list", "remote", "staging"] {
        assert_eq!(
            count("afs_dfs_read_route_seconds", &format!("stage=\"{stage}\"")),
            2,
            "{stage}"
        );
    }
    server.abort();
}

/// T1 (local branches): a grant that is expired, names another holder or
/// another holder epoch, or another caller is refused before Meta is asked,
/// with the same denial, and no read slot is taken: every slot is held while
/// the requests run, so one that needed a slot would end busy instead.
#[tokio::test]
async fn h2_t1_local_grant_checks_refuse_without_meta_or_a_slot() {
    let h = holder(30_000);
    let slots = h
        .service
        .read_permits
        .clone()
        .acquire_many_owned(8)
        .await
        .unwrap();
    let mut expired = grant("v", "l");
    expired.expires_at_unix_ms = 1;
    let mut other_holder = grant("v", "l");
    other_holder.holder_node_id = "someone-else".into();
    let mut other_epoch = grant("v", "l");
    other_epoch.holder_node_epoch = 10;
    let mut other_caller = grant("v", "l");
    other_caller.caller_node_id = "intruder".into();
    let mut messages = Vec::new();
    for bad in [expired, other_holder, other_epoch, other_caller] {
        messages.push(denied(
            read(
                &h.service,
                holder_request(vec![holder_op(0, &h.present.chunk.id.0, 0, 4096, &bad)]),
            )
            .await,
        ));
    }
    assert!(
        messages.windows(2).all(|pair| pair[0] == pair[1]),
        "{messages:?}"
    );
    assert_eq!(h.meta.calls.load(Ordering::SeqCst), 0);
    drop(slots);
}

/// Polls `ready` until it holds, failing after 5 s.
async fn until(what: &str, ready: impl Fn() -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !ready() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
}

fn holder_serve(h: &Holder) -> Arc<crate::node::rpc::data::HolderServe> {
    h.service.holders.clone().unwrap()
}

/// S3 (code review): the version stops being shared while a request waits:
/// the request ends not ready, even though the Chunk then arrives. Every
/// read slot stays taken, so only the check before the slot can answer
/// `NOT_READY` (a request reaching the slot queue would end busy instead).
#[tokio::test]
async fn h2_s3_version_unshared_during_the_wait_is_not_served() {
    let h = holder(30_000);
    let g = grant("v", "l");
    let _slots = h
        .service
        .read_permits
        .clone()
        .acquire_many_owned(8)
        .await
        .unwrap();
    let fetching = h.in_flight.begin(&h.absent.chunk.id);
    let mut op = holder_op(0, &h.absent.chunk.id.0, 0, 4096, &g);
    op.wait_ms = 500;
    let service = Arc::new(h.service.clone());
    let task = {
        let service = service.clone();
        tokio::spawn(async move { read(&service, holder_request(vec![op])).await })
    };
    let serve = holder_serve(&h);
    until("the request to wait", || serve.waiting() == 1).await;
    h.registry.release(&h.version, 1);
    h.local.put(h.absent.clone()).unwrap();
    drop(fetching);
    not_ready(task.await.unwrap());
}

/// S3 (code review), !318 P3-3: the version's last shared reader leaves
/// while a request waits for a Chunk still in flight: the release itself
/// wakes the wait, which ends at once (nothing else changes meanwhile).
#[tokio::test]
async fn h2_s3_wakeup_after_unsharing_ends_the_wait() {
    let h = holder(30_000);
    let g = grant("v", "l");
    let _fetching = h.in_flight.begin(&h.absent.chunk.id);
    let mut op = holder_op(0, &h.absent.chunk.id.0, 0, 4096, &g);
    op.wait_ms = 500;
    let service = Arc::new(h.service.clone());
    let task = {
        let service = service.clone();
        tokio::spawn(async move { read(&service, holder_request(vec![op])).await })
    };
    let serve = holder_serve(&h);
    until("the request to wait", || serve.waiting() == 1).await;
    let released = std::time::Instant::now();
    h.registry.release(&h.version, 1);
    not_ready(
        tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .expect("the wait ends")
            .unwrap(),
    );
    assert!(
        released.elapsed() < std::time::Duration::from_millis(300),
        "the release did not wake the wait: {:?}",
        released.elapsed()
    );
}

/// S3 (code review): the version stops being shared while the request
/// waits for a read slot: nothing is served from inside the slot.
#[tokio::test]
async fn h2_s3_version_unshared_before_the_slot_is_not_served() {
    let h = holder(30_000);
    let g = grant("v", "l");
    let slots = h
        .service
        .read_permits
        .clone()
        .acquire_many_owned(8)
        .await
        .unwrap();
    let (queued, at_slot) = tokio::sync::oneshot::channel();
    holder_serve(&h).on(
        crate::node::rpc::data::HolderTestPoint::SlotQueue,
        move || {
            let _ = queued.send(());
        },
    );
    let op = holder_op(0, &h.present.chunk.id.0, 0, 4096, &g);
    let service = Arc::new(h.service.clone());
    let task = {
        let service = service.clone();
        tokio::spawn(async move { read(&service, holder_request(vec![op])).await })
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), at_slot)
        .await
        .expect("the request reaches the slot queue")
        .unwrap();
    h.registry.release(&h.version, 1);
    drop(slots);
    not_ready(task.await.unwrap());
}

/// N2 (code review): one Chunk in flight, another missing and not expected:
/// not ready at once instead of waiting for the first.
#[tokio::test]
async fn h2_n2_waits_only_when_every_missing_chunk_is_in_flight() {
    let h = holder(30_000);
    let g = grant("v", "l");
    let other = StagedChunk::new(crate::dfs::OperationId::new("never"), vec![3; 4096]);
    let layout = LayoutRoot {
        id: LayoutRootId::new("l2"),
        file_length: 65536 + 4096,
        inline_extents: vec![
            Extent {
                file_offset: 0,
                length: 65536,
                chunk_id: h.absent.chunk.id.clone(),
                chunk_offset: 0,
            },
            Extent {
                file_offset: 65536,
                length: 4096,
                chunk_id: other.chunk.id.clone(),
                chunk_offset: 0,
            },
        ],
    };
    let version = FileVersionId::new("v2");
    h.registry.retain(&version, 2);
    h.registry.attach(&version, &layout);
    let g2 = grant("v2", "l2");
    *h.meta.genuine.lock().unwrap() = Some(g2.clone());
    let _fetching = h.in_flight.begin(&h.absent.chunk.id);
    let mut first = holder_op(0, &h.absent.chunk.id.0, 0, 4096, &g2);
    first.wait_ms = 500;
    let mut second = holder_op(1, &other.chunk.id.0, 0, 4096, &g2);
    second.wait_ms = 500;
    let mut request = holder_request(vec![first, second]);
    request.file_version_id = "v2".into();
    request.layout_root_id = "l2".into();
    let started = std::time::Instant::now();
    not_ready(read(&h.service, request).await);
    assert!(started.elapsed() < std::time::Duration::from_millis(200));
    let _ = g;
}

/// S1 (code review): presence before a slot comes from the catalog only. A
/// cataloged Chunk whose file is damaged is not "absent" (NOT_READY): the
/// holder goes on to serve it and the damage surfaces only when serving.
#[tokio::test]
async fn h2_s1_presence_is_decided_by_the_catalog_not_by_opening_the_file() {
    let h = holder(30_000);
    let g = grant("v", "l");
    let hash = &h.present.chunk.id.0;
    let mut damaged = 0;
    for entry in walk(h._temp.path()) {
        if entry
            .file_name()
            .is_some_and(|name| name.to_string_lossy() == hash.as_str())
        {
            std::fs::write(&entry, b"x").unwrap();
            damaged += 1;
        }
    }
    assert!(damaged > 0, "the Chunk file was not found");
    let result = read(
        &h.service,
        holder_request(vec![holder_op(0, &h.present.chunk.id.0, 0, 4096, &g)]),
    )
    .await;
    // Serving fails on the damaged file: neither served nor NOT_READY.
    let error = afs_transport::grpc::error_status::status_to_error(
        result.expect_err("a damaged copy is not served"),
    );
    assert_ne!(
        error.code(),
        afs_error::NODE_TRANSFER_NOT_READY,
        "{error:?}"
    );
}

fn walk(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                files.push(path);
            }
        }
    }
    files
}

/// The authenticated peer of a test server is always this Node.
struct FixedPeer(&'static str);
impl crate::node::rpc::data::PeerAuthenticator for FixedPeer {
    fn authenticate(
        &self,
        _: &tonic::metadata::MetadataMap,
        _: Option<std::net::SocketAddr>,
        _: Option<&[u8]>,
    ) -> afs_error::Result<String> {
        Ok(self.0.into())
    }
}

/// The writer of the P2 test: accepts its own per-copy grants and counts
/// reads; with a gate, each read waits (at most 10 s) until it opens.
#[derive(Default)]
struct CountingWriter {
    reads: AtomicUsize,
    gate: Option<(Mutex<bool>, std::sync::Condvar)>,
}
impl CountingWriter {
    fn gated() -> Self {
        Self {
            gate: Some((Mutex::new(false), std::sync::Condvar::new())),
            ..Default::default()
        }
    }
    fn open(&self) {
        if let Some((open, opened)) = &self.gate {
            *open.lock().unwrap() = true;
            opened.notify_all();
        }
    }
    fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
}
impl DfsReadAuthorizer for CountingWriter {
    fn authorize(&self, _: &str, request: &DfsReadRangesRequest) -> afs_error::Result<()> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if let Some((open, opened)) = &self.gate {
            let open = open.lock().unwrap();
            let _open = opened
                .wait_timeout_while(open, std::time::Duration::from_secs(10), |open| !*open)
                .unwrap();
        }
        if request.operations.iter().all(|op| {
            op.grant
                .as_ref()
                .is_some_and(|grant| grant.token == "per-copy")
        }) {
            Ok(())
        } else {
            Err(afs_error::Error::coded(
                afs_error::IO_PERMISSION_DENIED,
                "not a writer grant",
            ))
        }
    }
}
impl std::fmt::Debug for CountingWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CountingWriter")
    }
}

/// Meta for one holder of the P2 test: any well-formed holder grant is good.
struct AnyHolderGrant;
impl DfsHolderGrantValidator for AnyHolderGrant {
    fn validate(
        &self,
        request: ValidateDfsHolderGrant,
    ) -> afs_error::Result<DfsAuthorizedHolderRead> {
        Ok(DfsAuthorizedHolderRead {
            grant: request.grant,
            expires_at_unix_ms: crate::clock::now_unix_ms() + 30_000,
        })
    }
}

/// Reader-side Meta of the P2 test: the writer, and the other reader as the
/// one holder.
struct PairMeta {
    reply: crate::dfs::DfsChunkSourcesReply,
    holder: crate::dfs::VersionHolder,
}
impl crate::node::read::engine::ReadSourceProvider for PairMeta {
    fn sources_for(
        &self,
        _: crate::dfs::DfsChunkSourcesRequest,
    ) -> afs_error::Result<crate::dfs::DfsChunkSourcesReply> {
        Ok(self.reply.clone())
    }
    fn sources_for_async(
        &self,
        request: crate::dfs::DfsChunkSourcesRequest,
    ) -> crate::node::read::admission::ReadFuture<'_, crate::dfs::DfsChunkSourcesReply> {
        Box::pin(async move {
            let mut reply = self.sources_for(request.clone())?;
            reply
                .chunks
                .retain(|set| request.chunk_ids.contains(&set.chunk_id));
            Ok(reply)
        })
    }
    fn version_holders_async(
        &self,
        _: crate::dfs::DfsVersionHoldersRequest,
    ) -> crate::node::read::admission::ReadFuture<'_, Vec<crate::dfs::VersionHolder>> {
        Box::pin(async move { Ok(vec![self.holder.clone()]) })
    }
}

async fn serve(service: DfsChunksService) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(afs_protocol::node_data::dfs_chunks_server::DfsChunksServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    (endpoint, server)
}

/// P2 with the holder wait restored (as 2c will use it, review/18 a5): a
/// reader waits only for a holder ranked above it for the Chunk. The
/// higher-ranked reader starts first and is held fetching at the writer:
/// the other one waits for it and reads its copy, the writer serves once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h2_p2_a_reader_waits_for_a_higher_ranked_holder() {
    let pair = pair_read(|pool| pool.with_holder_wait_ms(200), Start::HigherFirst).await;
    assert_eq!(pair.timeouts, 0, "{:?}", pair.elapsed);
    assert_eq!(pair.waited_at_lower, 0);
    assert!(pair.waited_at_higher >= 1, "{:?}", pair.elapsed);
    assert_eq!(pair.writer_reads, 1, "{:?}", pair.elapsed);
}

/// P2: the lower-ranked reader starts first; the higher-ranked one never
/// waits for it (no cycle can form) and reads the writer itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h2_p2_a_reader_never_waits_for_a_lower_ranked_holder() {
    let pair = pair_read(|pool| pool.with_holder_wait_ms(200), Start::LowerFirst).await;
    assert_eq!(pair.timeouts, 0, "{:?}", pair.elapsed);
    assert_eq!(pair.waited_at_lower, 0, "{:?}", pair.elapsed);
    assert_eq!(pair.writer_reads, 2);
}

/// review/18 a5: with no holder wait (`dfs_holder_wait_ms = 0`, also a bare
/// pool) a reader asks a holder for nothing it does not have yet: no holder
/// ever waits, whoever starts first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h2_a5_readers_without_a_holder_wait_ask_none() {
    for start in [Start::Together, Start::HigherFirst] {
        let pair = pair_read(|pool| pool, start).await;
        assert_eq!(
            pair.waited_at_higher + pair.waited_at_lower,
            0,
            "{start:?} {:?}",
            pair.elapsed
        );
        assert_eq!(pair.timeouts, 0);
    }
}

/// !318 P2-4: two readers, two Chunks ranked oppositely (A above B for X, B
/// above A for Y). Each reads both in one batch, A as [X, Y] and B as
/// [Y, X]; each sees a published cache copy of its own top Chunk, so it
/// fetches that one first (both held at the gate until both are there) and
/// keeps it in flight until the batch ends. Then each asks the other, as
/// the higher-ranked holder, for its second Chunk. A reader whose batch
/// covers more than one Chunk asks no holder to wait, so neither waits for
/// a Chunk the other can only install after its own wait: no wait, no
/// timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h2_pr318_readers_of_crossed_chunks_never_wait_for_each_other() {
    let pair = pair_read(|pool| pool.with_holder_wait_ms(400), Start::Crossed).await;
    assert_eq!(pair.timeouts, 0, "{:?}", pair.elapsed);
    assert_eq!(
        pair.waited_at_higher + pair.waited_at_lower,
        0,
        "{:?}",
        pair.elapsed
    );
    assert_eq!(pair.writer_reads, 4);
}

#[derive(Clone, Copy, Debug)]
enum Start {
    Together,
    /// The reader ranked higher for the Chunk starts and is held fetching
    /// it from the writer when the other one starts.
    HigherFirst,
    LowerFirst,
    /// Two Chunks ranked oppositely; each reader reads both, its own top
    /// Chunk first (!318 P2-4).
    Crossed,
}

struct PairOutcome {
    elapsed: std::time::Duration,
    /// Waits each reader's holder service let the other one do.
    waited_at_higher: u64,
    waited_at_lower: u64,
    timeouts: u64,
    writer_reads: usize,
}

fn cache_event_count(registry: &afs_metrics::Registry, event: &str) -> u64 {
    let sample = format!("afs_dfs_read_cache_events_total{{event=\"{event}\"}} ");
    afs_metrics::encode_text(registry)
        .unwrap()
        .lines()
        .find_map(|line| {
            line.strip_prefix(sample.as_str())?
                .trim()
                .parse::<u64>()
                .ok()
        })
        .unwrap_or(0)
}

/// CRC Chunks for the pair test: one, or two ranked oppositely for r1 and
/// r2 (r1 above r2 for the first).
fn pair_chunks(two: bool) -> Vec<(StagedChunk, Vec<u8>)> {
    let make = |seed: u32| {
        let bytes: Vec<u8> = (0..(3 * 65536 + 17))
            .map(|i: u32| (i.wrapping_add(seed) % 249) as u8)
            .collect();
        let staged =
            StagedChunk::new_crc32c(crate::dfs::OperationId::new("pair"), bytes.clone()).unwrap();
        (staged, bytes)
    };
    let version = FileVersionId::new("v");
    let r1_above = |staged: &StagedChunk| {
        crate::node::read::engine::holder_ranks_above(&version, &staged.chunk.id, "r1", "r2")
    };
    if !two {
        return vec![make(0)];
    }
    let mut first = None;
    let mut second = None;
    for seed in 0.. {
        let chunk = make(seed);
        if r1_above(&chunk.0) {
            first.get_or_insert(chunk);
        } else {
            second.get_or_insert(chunk);
        }
        if first.is_some() && second.is_some() {
            break;
        }
    }
    vec![first.unwrap(), second.unwrap()]
}

/// Two readers, each the other's holder, over real transfers and holder
/// services; `configure` sets up both readers' peer connection pools. The
/// writer's gate orders the readers (no sleeps); every wait is bounded.
async fn pair_read(
    configure: fn(
        crate::node::rpc::peer::PeerConnectionPool,
    ) -> crate::node::rpc::peer::PeerConnectionPool,
    start: Start,
) -> PairOutcome {
    use crate::node::read::engine::{ChunkReadOp, DfsReadConfig, DfsReadEngine, ReadBatch};
    let temp = tempfile::tempdir().unwrap();
    let chunks = pair_chunks(matches!(start, Start::Crossed));
    let mut layout = LayoutRoot {
        id: LayoutRootId::new("l"),
        file_length: 0,
        inline_extents: Vec::new(),
    };
    for (staged, _) in &chunks {
        layout.inline_extents.push(Extent {
            file_offset: layout.file_length,
            length: staged.chunk.length,
            chunk_id: staged.chunk.id.clone(),
            chunk_offset: 0,
        });
        layout.file_length += staged.chunk.length;
    }
    // The writer.
    let writer_local =
        Arc::new(LocalChunkStore::open(temp.path().join("writer"), "writer").unwrap());
    for (staged, _) in &chunks {
        writer_local.put(staged.clone()).unwrap();
    }
    let writer_reads = Arc::new(if matches!(start, Start::Together) {
        CountingWriter::default()
    } else {
        CountingWriter::gated()
    });
    // One writer endpoint per reader, so the per-copy grants' caller matches
    // the peer each server authenticates; one store, one counter.
    let mut writer_endpoints = Vec::new();
    let mut writer_servers = Vec::new();
    for name in ["r1", "r2"] {
        let mut writer = dfs_reuse_test_service(writer_local.clone(), writer_reads.clone());
        writer.authenticator = Arc::new(FixedPeer(name));
        let (endpoint, server) = serve(writer).await;
        writer_endpoints.push(endpoint);
        writer_servers.push(server);
    }
    // Two reader Nodes, each a read engine and a holder service on one store.
    struct Node {
        engine: Arc<DfsReadEngine>,
        registry: afs_metrics::Registry,
        server: tokio::task::JoinHandle<()>,
    }
    let names = ["r1", "r2"];
    let locals: Vec<_> = names
        .iter()
        .map(|name| {
            let registry = afs_metrics::Registry::new();
            let metrics = crate::node::metrics::work::ReadWorkMetrics::register(&registry).unwrap();
            (
                Arc::new(
                    LocalChunkStore::open(temp.path().join(name), *name)
                        .unwrap()
                        .with_read_work_metrics(metrics),
                ),
                registry,
            )
        })
        .collect();
    // Bind both listeners first: each reader needs the other's endpoint.
    let mut listeners = Vec::new();
    let mut endpoints = Vec::new();
    for _ in names {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        endpoints.push(format!("http://{}", listener.local_addr().unwrap()));
        listeners.push(Some(listener));
    }
    let mut nodes = Vec::new();
    // The read-cache install loops, as the Node assembly starts them.
    let mut services = crate::runtime::Services::new();
    for (index, name) in names.iter().enumerate() {
        let other = names[1 - index];
        let (local, registry) = locals[index].clone();
        let per_copy = crate::dfs::DfsReadGrant {
            namespace_id: NamespaceId::new("namespace"),
            file_version_id: FileVersionId::new("v"),
            layout_root_id: LayoutRootId::new("l"),
            caller_node_id: (*name).into(),
            caller_node_epoch: 1,
            expires_at_unix_ms: 4_000_000_000_000,
            fence: 1,
            token: "per-copy".into(),
        };
        // Crossed: this reader's own top Chunk has a published cache copy
        // (served by the writer here), read before any holder; the other
        // Chunk only the durable copy, read after the holder.
        let own_top = |staged: &StagedChunk| {
            matches!(start, Start::Crossed) && chunks[index].0.chunk.id == staged.chunk.id
        };
        let writer_copy = |staged: &StagedChunk| crate::dfs::SourceCandidate {
            copy_id: crate::dfs::CopyId::new(if own_top(staged) {
                "cache-copy"
            } else {
                "writer-copy"
            }),
            chunk_id: staged.chunk.id.clone(),
            role: if own_top(staged) {
                crate::dfs::CopyRole::VerifiedCache
            } else {
                crate::dfs::CopyRole::DurableReplica
            },
            state: crate::dfs::CopyState::Ready,
            location: crate::dfs::CopyLocation::Node {
                node_id: "writer".into(),
                node_epoch: 1,
                device_id: "d".into(),
                device_epoch: 1,
                catalog_revision: 1,
            },
            data_endpoint: Some(writer_endpoints[index].clone()),
            load_hint: 0,
            read_grant: per_copy.clone(),
            holder_grant: None,
        };
        let mut holder_grant = grant("v", "l");
        holder_grant.caller_node_id = (*name).into();
        holder_grant.holder_node_id = other.into();
        let meta = Arc::new(PairMeta {
            reply: crate::dfs::DfsChunkSourcesReply {
                revision: 1,
                chunks: chunks
                    .iter()
                    .map(|(staged, _)| crate::dfs::ChunkSources {
                        chunk_id: staged.chunk.id.clone(),
                        chunk: Some(staged.chunk.clone()),
                        allowed_ranges: vec![(0, staged.chunk.length)],
                        sources: vec![writer_copy(staged)],
                    })
                    .collect(),
            },
            holder: crate::dfs::VersionHolder {
                node_id: other.into(),
                node_epoch: 9,
                data_endpoint: endpoints[1 - index].clone(),
                grant: holder_grant,
            },
        });
        let pool = Arc::new(configure(
            crate::node::rpc::peer::PeerConnectionPool::new(
                afs_transport::GrpcConfig::default(),
                afs_transport::TlsConfig::Disabled,
                4,
            )
            .unwrap(),
        ));
        let transfer = Arc::new(
            crate::node::rpc::peer::GrpcChunkTransfer::new(pool, std::time::Duration::from_secs(5))
                .unwrap(),
        );
        let engine = Arc::new(
            DfsReadEngine::new(
                NamespaceId::new("namespace"),
                (*name).into(),
                local.clone(),
                meta,
                transfer,
                DfsReadConfig::default(),
            )
            .with_read_authority(Arc::new(
                crate::node::read::engine::LocalReadAuthority::new(
                    1,
                    std::time::Instant::now() + std::time::Duration::from_secs(30),
                ),
            ))
            .with_distributed_cache("session".into(), 1 << 30)
            .with_holder_lists(std::time::Duration::from_secs(60)),
        );
        engine.start_distributed_cache(&mut services);
        let shared = HolderRegistry::new();
        shared.retain(&FileVersionId::new("v"), 1);
        shared.attach(&FileVersionId::new("v"), &layout);
        let mut service = dfs_reuse_test_service(local, Arc::new(DenyDfsReadAuthorizer));
        service.authenticator = Arc::new(FixedPeer(other));
        service.holders = Some(crate::node::rpc::data::HolderServe::new(
            Arc::new(AnyHolderGrant),
            shared,
            engine.holder_in_flight().unwrap(),
            (*name).into(),
            9,
        ));
        let listener = listeners[index].take().unwrap();
        let server = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(
                    afs_protocol::node_data::dfs_chunks_server::DfsChunksServer::new(service),
                )
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await;
        });
        nodes.push(Node {
            engine,
            registry,
            server,
        });
    }
    // Each reader's batch (its own top Chunk first) and expected bytes.
    let batch_of = |order: &[usize]| {
        let mut ops = Vec::new();
        let mut expected = Vec::new();
        for &chunk in order {
            let (staged, bytes) = &chunks[chunk];
            ops.push(ChunkReadOp {
                chunk_id: staged.chunk.id.clone(),
                chunk_offset: 0,
                length: staged.chunk.length,
                output_offset: expected.len(),
            });
            expected.extend_from_slice(bytes);
        }
        (
            ReadBatch {
                file_version_id: Some(FileVersionId::new("v")),
                layout_root_id: LayoutRootId::new("l"),
                ops,
                lookahead: Vec::new(),
            },
            expected,
        )
    };
    // r1 ranks above r2 for chunks[0] (`pair_chunks`); for one Chunk, find out.
    let higher = usize::from(!crate::node::read::engine::holder_ranks_above(
        &FileVersionId::new("v"),
        &chunks[0].0.chunk.id,
        names[0],
        names[1],
    ));
    let order = match start {
        Start::Together | Start::HigherFirst | Start::Crossed => [higher, 1 - higher],
        Start::LowerFirst => [1 - higher, higher],
    };
    let read_of = |index: usize| {
        let engine = nodes[index].engine.clone();
        let own_first: Vec<usize> = if chunks.len() == 1 {
            vec![0]
        } else if index == 0 {
            vec![0, 1]
        } else {
            vec![1, 0]
        };
        let (batch, expected) = batch_of(&own_first);
        tokio::task::spawn_blocking(move || {
            let mut out = vec![0; expected.len()];
            engine
                .read_batch(&batch, &mut out)
                .map(|()| out == expected)
        })
    };
    let started = std::time::Instant::now();
    let deadline = started + std::time::Duration::from_secs(10);
    // Bounded until-loops that stop early when a read already ended (its
    // result then decides the test instead of a hang).
    async fn until_or_ended(
        what: &str,
        deadline: std::time::Instant,
        reads: &[tokio::task::JoinHandle<afs_error::Result<bool>>],
        ready: impl Fn() -> bool,
    ) {
        while !ready() {
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for {what}"
            );
            if reads.iter().any(|read| read.is_finished()) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }
    let mut reads = Vec::new();
    match start {
        Start::Together => {
            reads.push(read_of(order[0]));
            reads.push(read_of(order[1]));
        }
        Start::HigherFirst | Start::LowerFirst => {
            reads.push(read_of(order[0]));
            // The first reader is at the writer: its Chunk is in flight.
            until_or_ended("the first fetch", deadline, &reads, || {
                writer_reads.reads() >= 1
            })
            .await;
            reads.push(read_of(order[1]));
            // The second one waits at the first, or reads the writer too.
            let first = &nodes[order[0]].registry;
            until_or_ended("the second reader", deadline, &reads, || {
                writer_reads.reads() >= 2 || cache_event_count(first, "holder_waited") >= 1
            })
            .await;
        }
        Start::Crossed => {
            reads.push(read_of(order[0]));
            reads.push(read_of(order[1]));
            until_or_ended("both first fetches", deadline, &reads, || {
                writer_reads.reads() >= 2
            })
            .await;
        }
    }
    writer_reads.open();
    for read in reads {
        let result = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), read)
            .await
            .expect("a read hangs")
            .unwrap();
        assert!(result.expect("both readers read"), "wrong bytes");
    }
    let elapsed = started.elapsed();
    let timeouts: u64 = nodes
        .iter()
        .map(|node| cache_event_count(&node.registry, "holder_wait_timeout"))
        .sum();
    let waited_at_higher = cache_event_count(&nodes[higher].registry, "holder_waited");
    let waited_at_lower = cache_event_count(&nodes[1 - higher].registry, "holder_waited");
    for node in nodes {
        node.server.abort();
    }
    for server in writer_servers {
        server.abort();
    }
    PairOutcome {
        elapsed,
        waited_at_higher,
        waited_at_lower,
        timeouts,
        writer_reads: writer_reads.reads(),
    }
}
