use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::node::chunk::{ChunkStore, StagedChunk};
use afs_protocol::node_data::{DfsChunkReadOp, DfsHixlGroupWindow, DfsHixlTransport, DfsReadGrant};
use afs_transport::hixl::ReadTransport;

struct FakeExportProvider {
    prepares: AtomicUsize,
    releases: AtomicUsize,
    quarantines: AtomicUsize,
    info: EngineInfo,
    additional_infos: Vec<EngineInfo>,
    prepared_lanes: Mutex<Vec<usize>>,
    released_windows: Mutex<Vec<(usize, u64, u64)>>,
    slot_capacity: usize,
    fail_prepare_at: AtomicUsize,
    invalid_export_at: AtomicUsize,
    fail_release_at: AtomicUsize,
}

impl FakeExportProvider {
    fn new() -> Self {
        Self::with_info(EngineInfo {
            engine_id: "fake-engine".into(),
            generation: 7,
            window_bytes: 4 * 1024 * 1024,
            transport: ReadTransport::Roce,
            buffer_pool_count: 0,
            buffer_pool_size_mib: 0,
            hccs_domain_id: None,
        })
    }

    fn with_info(info: EngineInfo) -> Self {
        Self {
            prepares: AtomicUsize::new(0),
            releases: AtomicUsize::new(0),
            quarantines: AtomicUsize::new(0),
            info,
            additional_infos: Vec::new(),
            prepared_lanes: Mutex::new(Vec::new()),
            released_windows: Mutex::new(Vec::new()),
            slot_capacity: MAX_HIXL_WINDOWS,
            fail_prepare_at: AtomicUsize::new(usize::MAX),
            invalid_export_at: AtomicUsize::new(usize::MAX),
            fail_release_at: AtomicUsize::new(usize::MAX),
        }
    }
}

impl ExportProvider for FakeExportProvider {
    fn info(&self) -> EngineInfo {
        self.info.clone()
    }

    fn lane_infos(&self) -> Vec<EngineInfo> {
        std::iter::once(self.info.clone())
            .chain(self.additional_infos.clone())
            .collect()
    }

    fn export_slots(&self, _lane: usize) -> usize {
        self.slot_capacity
    }

    fn prepare_export(
        &self,
        lane: usize,
        bytes: Vec<u8>,
        _deadline: Instant,
    ) -> Result<ExportWindow, HixlError> {
        let ordinal = self.prepares.fetch_add(1, Ordering::SeqCst) + 1;
        if ordinal == self.fail_prepare_at.load(Ordering::SeqCst) {
            return Err(HixlError {
                phase: afs_transport::hixl::HixlPhase::PrepareExport,
                native_status: None,
                kind: afs_transport::hixl::HixlErrorKind::ResourceExhausted,
                message: "injected export failure".into(),
                read_may_be_in_flight: false,
            });
        }
        let slot_id = {
            let mut prepared = self.prepared_lanes.lock().unwrap();
            let slot_id = u64::try_from(
                prepared
                    .iter()
                    .filter(|&&previous| previous == lane)
                    .count(),
            )
            .unwrap();
            prepared.push(lane);
            slot_id
        };
        Ok(ExportWindow {
            slot_id,
            generation: 9,
            address: if ordinal == self.invalid_export_at.load(Ordering::SeqCst) {
                0
            } else {
                0x1000
            },
            length: bytes.len(),
        })
    }

    fn release_export(&self, lane: usize, slot_id: u64, generation: u64) -> Result<(), HixlError> {
        let ordinal = self.releases.fetch_add(1, Ordering::SeqCst) + 1;
        self.released_windows
            .lock()
            .unwrap()
            .push((lane, slot_id, generation));
        if ordinal == self.fail_release_at.load(Ordering::SeqCst) {
            return Err(HixlError {
                phase: afs_transport::hixl::HixlPhase::ReleaseExport,
                native_status: None,
                kind: afs_transport::hixl::HixlErrorKind::NativeFailure,
                message: "injected release failure".into(),
                read_may_be_in_flight: false,
            });
        }
        Ok(())
    }

    fn quarantine(&self) -> Result<(), HixlError> {
        self.quarantines.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct CountingAuthorizer {
    calls: AtomicUsize,
}

impl DfsReadAuthorizer for CountingAuthorizer {
    fn authorize(&self, peer: &str, request: &DfsReadRangesRequest) -> afs_error::Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if peer == "reader"
            && request.operations.iter().all(|operation| {
                operation
                    .grant
                    .as_ref()
                    .is_some_and(|grant| grant.token == "fixture-grant")
            })
        {
            Ok(())
        } else {
            Err(afs_error::Error::coded(
                afs_error::NODE_TRANSFER_INVALID,
                "fixture HIXL grant rejected",
            ))
        }
    }
}

fn read_request(read_id: &str, token: &str) -> DfsReadRangesRequest {
    DfsReadRangesRequest {
        read_id: read_id.into(),
        attempt_id: "attempt".into(),
        file_version_id: "version".into(),
        layout_root_id: "layout".into(),
        include_blake3_outboard: false,
        operations: vec![DfsChunkReadOp {
            operation_index: 0,
            chunk_id: format!("blake3-{}-4", blake3::hash(b"data")),
            chunk_offset: 0,
            length: 4,
            destination_offset: 0,
            source_copy_id: "copy".into(),
            grant: Some(DfsReadGrant {
                namespace_id: "namespace".into(),
                file_version_id: "version".into(),
                layout_root_id: "layout".into(),
                caller_node_id: "reader".into(),
                caller_node_epoch: 1,
                expires_at_unix_ms: u64::MAX,
                fence: 1,
                token: token.into(),
            }),
        }],
    }
}

fn negotiate_request(read: DfsReadRangesRequest) -> DfsNegotiateHixlRequest {
    DfsNegotiateHixlRequest {
        read_request: Some(read),
        client_engine_id: "client-engine".into(),
        client_engine_generation: 1,
        protocol_version: HIXL_PROTOCOL_VERSION,
        caller_node_epoch: 1,
        source_node_epoch: 9,
        transport: DfsHixlTransport::Roce as i32,
        buffer_pool_count: 0,
        buffer_pool_size_mib: 0,
        hccs_domain_id: None,
        lane_count: 1,
    }
}

fn prepare_request(session_id: u64, read: DfsReadRangesRequest) -> DfsPrepareReadRangesHixlRequest {
    DfsPrepareReadRangesHixlRequest {
        session_id,
        batch_read_request: Some(read.clone()),
        batch_offset: 0,
        lane_id: 0,
        read_request: Some(read),
        caller_node_epoch: 1,
        source_node_epoch: 9,
    }
}

fn close_request(session_id: u64) -> DfsCloseHixlRequest {
    DfsCloseHixlRequest {
        session_id,
        caller_node_epoch: 1,
        source_node_epoch: 9,
        engine_id: "fake-engine".into(),
        engine_generation: 7,
    }
}

fn release_request(reply: &DfsPrepareReadRangesHixlReply) -> DfsReleaseHixlReadRequest {
    DfsReleaseHixlReadRequest {
        session_id: reply.session_id,
        read_id: reply.read_id.clone(),
        source_node_id: reply.source_node_id.clone(),
        source_node_epoch: reply.source_node_epoch,
        engine_id: reply.engine_id.clone(),
        engine_generation: reply.engine_generation,
        pool_generation: reply.pool_generation,
        caller_node_epoch: 1,
        windows: vec![DfsHixlReleaseWindow {
            attempt_id: reply.attempt_id.clone(),
            slot_id: reply.slot_id,
            slot_generation: reply.slot_generation,
            outcome: DfsHixlReleaseOutcome::Completed as i32,
            content_validated: true,
        }],
    }
}

fn local_store() -> (tempfile::TempDir, Arc<LocalChunkStore>) {
    let temp = tempfile::tempdir().expect("test tempdir");
    let local = Arc::new(LocalChunkStore::open(temp.path(), "source").expect("test store"));
    let staged = StagedChunk::new(crate::dfs::OperationId::new("fixture"), b"data".to_vec());
    local.put(staged).expect("test chunk");
    (temp, local)
}

fn service() -> (
    HixlReadService,
    Arc<FakeExportProvider>,
    Arc<CountingAuthorizer>,
) {
    let provider = Arc::new(FakeExportProvider::new());
    let service = HixlReadService::for_test(provider.clone(), "source".into(), 9);
    let authorizer = Arc::new(CountingAuthorizer {
        calls: AtomicUsize::new(0),
    });
    (service, provider, authorizer)
}

fn service_with_info(
    info: EngineInfo,
) -> (
    HixlReadService,
    Arc<FakeExportProvider>,
    Arc<CountingAuthorizer>,
) {
    let provider = Arc::new(FakeExportProvider::with_info(info));
    let service = HixlReadService::for_test(provider.clone(), "source".into(), 9);
    let authorizer = Arc::new(CountingAuthorizer {
        calls: AtomicUsize::new(0),
    });
    (service, provider, authorizer)
}

fn hccs_info() -> EngineInfo {
    EngineInfo {
        engine_id: "fake-engine".into(),
        generation: 7,
        window_bytes: 4 * 1024 * 1024,
        transport: ReadTransport::HccsBufferPool,
        buffer_pool_count: 4,
        buffer_pool_size_mib: 64,
        hccs_domain_id: Some(0),
    }
}

fn set_hccs_pool(request: &mut DfsNegotiateHixlRequest, count: u32, size_mib: u32) {
    request.transport = DfsHixlTransport::HccsBufferPool as i32;
    request.buffer_pool_count = count;
    request.buffer_pool_size_mib = size_mib;
    request.hccs_domain_id = Some(0);
}

#[tokio::test]
async fn negotiate_echoes_roce_transport_with_zero_pool_parameters() {
    let (service, _provider, authorizer) = service();
    let reply = service
        .negotiate(
            "reader".into(),
            negotiate_request(read_request("roce", "fixture-grant")),
            authorizer,
        )
        .await
        .expect("RoCE negotiation")
        .into_inner();

    assert_eq!(reply.protocol_version, HIXL_PROTOCOL_VERSION);
    assert_eq!(reply.transport, DfsHixlTransport::Roce as i32);
    assert_eq!(reply.buffer_pool_count, 0);
    assert_eq!(reply.buffer_pool_size_mib, 0);
    assert_eq!(reply.hccs_domain_id, None);
}

fn prewarm_request() -> DfsNegotiateHixlRequest {
    DfsNegotiateHixlRequest {
        read_request: None,
        ..negotiate_request(read_request("roce", "unused"))
    }
}

#[tokio::test]
async fn negotiate_prewarms_without_read_authority() {
    let (service, _provider, authorizer) = service();
    let reply = service
        .negotiate("reader".into(), prewarm_request(), authorizer)
        .await
        .expect("pre-warm negotiation")
        .into_inner();

    assert_eq!(reply.protocol_version, HIXL_PROTOCOL_VERSION);
    assert_eq!(reply.transport, DfsHixlTransport::Roce as i32);
    assert!(!reply.lanes.is_empty(), "pre-warm still binds lanes");
}

#[tokio::test]
async fn prewarm_rejects_stale_source_epoch_and_zero_caller_epoch() {
    let (service, _provider, authorizer) = service();
    let mut stale_source = prewarm_request();
    stale_source.source_node_epoch = 8;
    let status = service
        .negotiate("reader".into(), stale_source, authorizer.clone())
        .await
        .expect_err("stale source epoch must be denied");
    assert_eq!(status.code(), tonic::Code::PermissionDenied);

    let mut zero_caller = prewarm_request();
    zero_caller.caller_node_epoch = 0;
    let status = service
        .negotiate("reader".into(), zero_caller, authorizer)
        .await
        .expect_err("zero caller epoch must be denied");
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn negotiate_echoes_matching_hccs_buffer_pool_parameters() {
    let (service, _provider, authorizer) = service_with_info(hccs_info());
    let mut request = negotiate_request(read_request("hccs", "fixture-grant"));
    set_hccs_pool(&mut request, 4, 64);

    let reply = service
        .negotiate("reader".into(), request, authorizer)
        .await
        .expect("HCCS negotiation")
        .into_inner();

    assert_eq!(reply.protocol_version, HIXL_PROTOCOL_VERSION);
    assert_eq!(reply.transport, DfsHixlTransport::HccsBufferPool as i32);
    assert_eq!(reply.buffer_pool_count, 4);
    assert_eq!(reply.buffer_pool_size_mib, 64);
    assert_eq!(reply.hccs_domain_id, Some(0));
}

#[tokio::test]
async fn negotiate_checks_transport_after_authorization_without_allocating_session() {
    let (service, _provider, authorizer) = service();
    let mut request = negotiate_request(read_request("mismatch", "fixture-grant"));
    set_hccs_pool(&mut request, 4, 64);

    let error = service
        .negotiate("reader".into(), request, authorizer.clone())
        .await
        .expect_err("HCCS request must not match the RoCE source");

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert_eq!(authorizer.calls.load(Ordering::SeqCst), 1);
    assert!(service.registry.lock().unwrap().sessions.is_empty());
}

#[tokio::test]
async fn negotiate_rejects_unknown_or_invalid_transport_parameters() {
    let (service, _provider, authorizer) = service();
    let cases = [
        (99, 0, 0),
        (DfsHixlTransport::Roce as i32, 1, 1),
        (DfsHixlTransport::Roce as i32, 0, 1),
    ];

    for (index, (transport, count, size_mib)) in cases.iter().copied().enumerate() {
        let mut request =
            negotiate_request(read_request(&format!("invalid-{index}"), "fixture-grant"));
        request.transport = transport;
        request.buffer_pool_count = count;
        request.buffer_pool_size_mib = size_mib;
        let error = service
            .negotiate("reader".into(), request, authorizer.clone())
            .await
            .expect_err("invalid transport parameters must be rejected");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    assert_eq!(authorizer.calls.load(Ordering::SeqCst), cases.len());
    assert!(service.registry.lock().unwrap().sessions.is_empty());

    let (hccs_service, _provider, hccs_authorizer) = service_with_info(hccs_info());
    let mut invalid_hccs = negotiate_request(read_request("invalid-hccs", "fixture-grant"));
    set_hccs_pool(&mut invalid_hccs, 0, 64);
    let error = hccs_service
        .negotiate("reader".into(), invalid_hccs, hccs_authorizer.clone())
        .await
        .expect_err("invalid HCCS pool parameters must be rejected");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert_eq!(hccs_authorizer.calls.load(Ordering::SeqCst), 1);
    assert!(hccs_service.registry.lock().unwrap().sessions.is_empty());

    for (index, domain) in [None, Some(7)].into_iter().enumerate() {
        let (hccs_service, _provider, hccs_authorizer) = service_with_info(hccs_info());
        let mut request = negotiate_request(read_request(
            &format!("invalid-hccs-domain-{index}"),
            "fixture-grant",
        ));
        set_hccs_pool(&mut request, 4, 64);
        request.hccs_domain_id = domain;
        let error = hccs_service
            .negotiate("reader".into(), request, hccs_authorizer.clone())
            .await
            .expect_err("HCCS domain must match the source");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(hccs_authorizer.calls.load(Ordering::SeqCst), 1);
        assert!(hccs_service.registry.lock().unwrap().sessions.is_empty());
    }
}

#[tokio::test]
async fn negotiate_rejects_mismatched_protocol_before_authorization() {
    let (service, _provider, authorizer) = service();
    let mut request = negotiate_request(read_request("mismatched-version", "fixture-grant"));
    request.protocol_version = HIXL_PROTOCOL_VERSION + 1;

    let error = service
        .negotiate("reader".into(), request, authorizer.clone())
        .await
        .expect_err("a mismatched HIXL protocol version must be rejected");

    assert_eq!(error.code(), tonic::Code::Unimplemented);
    assert_eq!(authorizer.calls.load(Ordering::SeqCst), 0);
    assert!(service.registry.lock().unwrap().sessions.is_empty());
}

async fn admitted_fixture(
    service: &HixlReadService,
    authorizer: Arc<CountingAuthorizer>,
    local: Arc<LocalChunkStore>,
) -> DfsPrepareReadRangesHixlReply {
    let read = read_request("read", "fixture-grant");
    let session = service
        .negotiate(
            "reader".into(),
            negotiate_request(read.clone()),
            authorizer.clone(),
        )
        .await
        .unwrap()
        .into_inner();
    service
        .prepare(
            "reader".into(),
            prepare_request(session.session_id, read),
            authorizer,
            Some(local),
        )
        .await
        .unwrap()
        .into_inner()
}

#[tokio::test]
async fn never_submitted_window_is_released_without_quarantining_source() {
    let (_temp, local) = local_store();
    let (service, provider, authorizer) = service();
    let prepared = admitted_fixture(&service, authorizer.clone(), local.clone()).await;
    let mut request = release_request(&prepared);
    request.windows[0].outcome = DfsHixlReleaseOutcome::NotSubmitted as i32;
    assert!(
        service
            .release("reader".into(), request.clone())
            .await
            .is_err(),
        "never-submitted cannot claim validated content"
    );
    request.windows[0].content_validated = false;
    assert!(
        service
            .release("reader".into(), request.clone())
            .await
            .unwrap()
            .into_inner()
            .windows[0]
            .consumed
    );
    assert!(
        service
            .release("reader".into(), request)
            .await
            .unwrap()
            .into_inner()
            .windows[0]
            .already_released
    );
    assert_eq!(provider.quarantines.load(Ordering::SeqCst), 0);
    assert_eq!(provider.releases.load(Ordering::SeqCst), 1);
    service
        .prepare(
            "reader".into(),
            prepare_request(prepared.session_id, read_request("next", "fixture-grant")),
            authorizer,
            Some(local),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn shutdown_drain_stops_admission_but_accepts_release() {
    let (_temp, local) = local_store();
    let (service, provider, authorizer) = service();
    let prepared = admitted_fixture(&service, authorizer.clone(), local.clone()).await;
    let draining = service.clone();
    let task = tokio::spawn(async move { draining.drain(Duration::from_secs(1)).await });
    tokio::time::timeout(Duration::from_millis(200), async {
        while !service.registry.lock().unwrap().draining {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        !task.is_finished(),
        "pending source window must keep drain waiting"
    );
    assert!(
        service
            .negotiate(
                "reader".into(),
                negotiate_request(read_request("next", "fixture-grant")),
                authorizer.clone()
            )
            .await
            .is_err()
    );
    assert!(
        service
            .prepare(
                "reader".into(),
                prepare_request(prepared.session_id, read_request("next", "fixture-grant")),
                authorizer,
                Some(local)
            )
            .await
            .is_err()
    );
    assert_eq!(provider.releases.load(Ordering::SeqCst), 0);
    assert_eq!(provider.quarantines.load(Ordering::SeqCst), 0);
    service
        .release("reader".into(), release_request(&prepared))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    assert_eq!(provider.releases.load(Ordering::SeqCst), 1);
    assert_eq!(provider.quarantines.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn shutdown_drain_timeout_quarantines_and_keeps_pending_window() {
    let (_temp, local) = local_store();
    let (service, provider, authorizer) = service();
    admitted_fixture(&service, authorizer, local).await;
    let error = service.drain(Duration::from_millis(10)).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(provider.quarantines.load(Ordering::SeqCst), 1);
    assert_eq!(provider.releases.load(Ordering::SeqCst), 0);
    assert_eq!(service.registry.lock().unwrap().active_window_count(), 1);
}

fn batch_window_request(
    session_id: u64,
    batch: &DfsReadRangesRequest,
    offset: u64,
    length: u64,
    attempt: &str,
) -> DfsPrepareReadRangesHixlRequest {
    DfsPrepareReadRangesHixlRequest {
        session_id,
        read_request: Some(
            crate::node::rpc::hixl::batch::slice_read_batch(batch, offset, length, attempt)
                .expect("fixture window"),
        ),
        caller_node_epoch: 1,
        source_node_epoch: 9,
        batch_read_request: Some(batch.clone()),
        batch_offset: offset,
        lane_id: 0,
    }
}

fn group_prepare_request(
    session_id: u64,
    batch: &DfsReadRangesRequest,
    windows: &[(u64, u64, &str)],
) -> DfsPrepareReadRangesHixlGroupRequest {
    DfsPrepareReadRangesHixlGroupRequest {
        session_id,
        caller_node_epoch: 1,
        source_node_epoch: 9,
        batch_read_request: Some(batch.clone()),
        windows: windows
            .iter()
            .map(|(offset, length, attempt)| DfsHixlGroupWindow {
                batch_offset: *offset,
                lane_id: 0,
                read_request: Some(
                    crate::node::rpc::hixl::batch::slice_read_batch(
                        batch, *offset, *length, attempt,
                    )
                    .expect("fixture group window"),
                ),
            })
            .collect(),
    }
}

#[tokio::test]
async fn prepare_releases_invalid_export_and_quarantines_if_cleanup_fails() {
    for cleanup_fails in [false, true] {
        let (service, provider, authorizer) = service();
        let (_temp, local) = local_store();
        let batch = read_request("invalid-export", "fixture-grant");
        let session = service
            .negotiate(
                "reader".into(),
                negotiate_request(batch.clone()),
                authorizer.clone(),
            )
            .await
            .unwrap()
            .into_inner();
        provider.invalid_export_at.store(1, Ordering::SeqCst);
        if cleanup_fails {
            provider.fail_release_at.store(1, Ordering::SeqCst);
        }
        let error = service
            .prepare(
                "reader".into(),
                prepare_request(session.session_id, batch),
                authorizer,
                Some(local),
            )
            .await
            .expect_err("an invalid export descriptor must fail prepare");
        assert_eq!(error.code(), tonic::Code::Internal);
        assert_eq!(provider.releases.load(Ordering::SeqCst), 1);
        assert_eq!(test_support::sessions(&service)[0].active_windows, 0);
        assert_eq!(
            provider.quarantines.load(Ordering::SeqCst),
            if cleanup_fails { 1 } else { 0 }
        );
        assert_eq!(
            service.registry.lock().unwrap().admission_frozen,
            cleanup_fails
        );
    }
}

#[tokio::test]
async fn group_prepare_pins_every_window_from_one_verified_snapshot() {
    let (service, provider, authorizer) = service();
    let (_temp, local) = local_store();
    let batch = read_request("group", "fixture-grant");
    let session = service
        .negotiate(
            "reader".into(),
            negotiate_request(batch.clone()),
            authorizer.clone(),
        )
        .await
        .unwrap()
        .into_inner();
    let reply = service
        .prepare_group(
            "reader".into(),
            group_prepare_request(
                session.session_id,
                &batch,
                &[(0, 2, "window-0"), (2, 2, "window-1")],
            ),
            authorizer,
            Some(local),
        )
        .await
        .expect("group preparation")
        .into_inner();
    assert_eq!(reply.session_id, session.session_id);
    assert_eq!(reply.windows.len(), 2);
    assert_eq!(reply.windows[0].batch_offset, 0);
    assert_eq!(reply.windows[1].batch_offset, 2);
    let first = reply.windows[0].reply.as_ref().unwrap();
    let second = reply.windows[1].reply.as_ref().unwrap();
    assert_eq!(
        first.ranges[0].range_checksum,
        blake3::hash(b"da").as_bytes()
    );
    assert_eq!(
        second.ranges[0].range_checksum,
        blake3::hash(b"ta").as_bytes()
    );
    assert_ne!(first.slot_id, second.slot_id);
    assert_eq!(provider.prepares.load(Ordering::SeqCst), 2);
    let states = test_support::sessions(&service);
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].prepared_bytes, 4);
    assert_eq!(states[0].active_windows, 2);
    for window in &reply.windows {
        service
            .release(
                "reader".into(),
                release_request(window.reply.as_ref().unwrap()),
            )
            .await
            .unwrap();
    }
    let states = test_support::sessions(&service);
    assert_eq!(states[0].prepared_bytes, 0);
    assert_eq!(states[0].active_windows, 0);
}

#[tokio::test]
async fn group_prepare_reuses_a_snapshot_created_by_an_earlier_window() {
    let (service, provider, authorizer) = service();
    let (temp, local) = local_store();
    let batch = read_request("group", "fixture-grant");
    let session = service
        .negotiate(
            "reader".into(),
            negotiate_request(batch.clone()),
            authorizer.clone(),
        )
        .await
        .unwrap()
        .into_inner();
    let first = service
        .prepare(
            "reader".into(),
            batch_window_request(session.session_id, &batch, 0, 2, "window-0"),
            authorizer.clone(),
            Some(local.clone()),
        )
        .await
        .unwrap()
        .into_inner();
    let chunk = temp
        .path()
        .join("chunks")
        .join(&batch.operations[0].chunk_id);
    std::fs::write(chunk, b"evil").unwrap();
    let reply = service
        .prepare_group(
            "reader".into(),
            group_prepare_request(session.session_id, &batch, &[(2, 2, "window-1")]),
            authorizer,
            Some(local),
        )
        .await
        .expect("the verified snapshot serves the group without re-reading")
        .into_inner();
    let second = reply.windows[0].reply.as_ref().unwrap();
    assert_eq!(
        second.ranges[0].range_checksum,
        blake3::hash(b"ta").as_bytes()
    );
    assert_eq!(provider.prepares.load(Ordering::SeqCst), 2);
    for reply in [&first, second] {
        service
            .release("reader".into(), release_request(reply))
            .await
            .unwrap();
    }
    assert_eq!(service.registry.lock().unwrap().prepared_batch_bytes(), 0);
}

#[tokio::test]
async fn group_prepare_rolls_back_pinned_exports_when_a_later_export_fails() {
    let (service, provider, authorizer) = service();
    let (_temp, local) = local_store();
    let batch = read_request("group", "fixture-grant");
    let session = service
        .negotiate(
            "reader".into(),
            negotiate_request(batch.clone()),
            authorizer.clone(),
        )
        .await
        .unwrap()
        .into_inner();
    provider.fail_prepare_at.store(2, Ordering::SeqCst);
    let failed = service
        .prepare_group(
            "reader".into(),
            group_prepare_request(
                session.session_id,
                &batch,
                &[(0, 2, "window-0"), (2, 2, "window-1")],
            ),
            authorizer.clone(),
            Some(local.clone()),
        )
        .await;
    assert!(failed.is_err(), "a failed export refuses the whole group");
    assert_eq!(provider.releases.load(Ordering::SeqCst), 1);
    let states = test_support::sessions(&service);
    assert_eq!(states[0].prepared_bytes, 0);
    assert_eq!(states[0].active_windows, 0);
    assert_eq!(provider.quarantines.load(Ordering::SeqCst), 0);
    provider.fail_prepare_at.store(usize::MAX, Ordering::SeqCst);
    let retry = service
        .prepare_group(
            "reader".into(),
            group_prepare_request(
                session.session_id,
                &batch,
                &[(0, 2, "window-0"), (2, 2, "window-1")],
            ),
            authorizer,
            Some(local),
        )
        .await
        .expect("a rolled-back group can be prepared again")
        .into_inner();
    assert_eq!(retry.windows.len(), 2);
}

#[tokio::test]
async fn group_prepare_releases_invalid_export_and_quarantines_if_cleanup_fails() {
    for cleanup_fails in [false, true] {
        let (service, provider, authorizer) = service();
        let (_temp, local) = local_store();
        let batch = read_request("invalid-group-export", "fixture-grant");
        let session = service
            .negotiate(
                "reader".into(),
                negotiate_request(batch.clone()),
                authorizer.clone(),
            )
            .await
            .unwrap()
            .into_inner();
        provider.invalid_export_at.store(2, Ordering::SeqCst);
        if cleanup_fails {
            provider.fail_release_at.store(2, Ordering::SeqCst);
        }
        let error = service
            .prepare_group(
                "reader".into(),
                group_prepare_request(
                    session.session_id,
                    &batch,
                    &[(0, 2, "window-0"), (2, 2, "window-1")],
                ),
                authorizer,
                Some(local),
            )
            .await
            .expect_err("an invalid current export must fail the whole group");
        assert_eq!(error.code(), tonic::Code::Internal);
        assert_eq!(provider.releases.load(Ordering::SeqCst), 2);
        let states = test_support::sessions(&service);
        assert_eq!(states[0].active_windows, 0);
        assert_eq!(states[0].prepared_bytes, 0);
        assert_eq!(
            provider.quarantines.load(Ordering::SeqCst),
            if cleanup_fails { 1 } else { 0 }
        );
        assert_eq!(
            service.registry.lock().unwrap().admission_frozen,
            cleanup_fails
        );
    }
}

#[tokio::test]
async fn group_prepare_rejects_duplicate_window_keys_before_export() {
    let (service, provider, authorizer) = service();
    let (_temp, local) = local_store();
    let batch = read_request("group-duplicate", "fixture-grant");
    let session = service
        .negotiate(
            "reader".into(),
            negotiate_request(batch.clone()),
            authorizer.clone(),
        )
        .await
        .unwrap()
        .into_inner();
    let error = service
        .prepare_group(
            "reader".into(),
            group_prepare_request(
                session.session_id,
                &batch,
                &[(0, 2, "same-window"), (2, 2, "same-window")],
            ),
            authorizer,
            Some(local),
        )
        .await
        .expect_err("duplicate window keys must reject the whole group");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert_eq!(provider.prepares.load(Ordering::SeqCst), 0);
    assert_eq!(provider.releases.load(Ordering::SeqCst), 0);
    assert_eq!(test_support::sessions(&service)[0].active_windows, 0);
}

#[tokio::test]
async fn group_prepare_rejects_gaps_wrong_lanes_and_out_of_bounds_windows() {
    let (service, provider, authorizer) = service();
    let (_temp, local) = local_store();
    let batch = read_request("group", "fixture-grant");
    let session = service
        .negotiate(
            "reader".into(),
            negotiate_request(batch.clone()),
            authorizer.clone(),
        )
        .await
        .unwrap()
        .into_inner();
    let mut gap = group_prepare_request(
        session.session_id,
        &batch,
        &[(0, 2, "window-0"), (3, 1, "window-1")],
    );
    assert!(
        service
            .prepare_group(
                "reader".into(),
                gap.clone(),
                authorizer.clone(),
                Some(local.clone())
            )
            .await
            .is_err()
    );
    gap.windows[1].lane_id = 1;
    assert!(
        service
            .prepare_group(
                "reader".into(),
                gap,
                authorizer.clone(),
                Some(local.clone())
            )
            .await
            .is_err()
    );
    let mut mismatched = group_prepare_request(session.session_id, &batch, &[(0, 2, "window-0")]);
    mismatched.windows[0].batch_offset = 2;
    assert!(
        service
            .prepare_group("reader".into(), mismatched, authorizer, Some(local))
            .await
            .is_err()
    );
    assert_eq!(provider.prepares.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn group_prepare_reuses_the_snapshot_across_waves() {
    let (service, provider, authorizer) = service();
    let (temp, local) = local_store();
    let batch = read_request("group", "fixture-grant");
    let session = service
        .negotiate(
            "reader".into(),
            negotiate_request(batch.clone()),
            authorizer.clone(),
        )
        .await
        .unwrap()
        .into_inner();
    let first = service
        .prepare_group(
            "reader".into(),
            group_prepare_request(session.session_id, &batch, &[(0, 2, "window-0")]),
            authorizer.clone(),
            Some(local.clone()),
        )
        .await
        .expect("first group wave")
        .into_inner();
    let chunk = temp
        .path()
        .join("chunks")
        .join(&batch.operations[0].chunk_id);
    std::fs::write(chunk, b"evil").unwrap();
    let second = service
        .prepare_group(
            "reader".into(),
            group_prepare_request(session.session_id, &batch, &[(2, 2, "window-1")]),
            authorizer,
            Some(local),
        )
        .await
        .expect("the next wave reuses the verified snapshot")
        .into_inner();
    assert_eq!(
        second.windows[0].reply.as_ref().unwrap().ranges[0].range_checksum,
        blake3::hash(b"ta").as_bytes()
    );
    assert_eq!(provider.prepares.load(Ordering::SeqCst), 2);
    for reply in [
        first.windows[0].reply.as_ref().unwrap(),
        second.windows[0].reply.as_ref().unwrap(),
    ] {
        service
            .release("reader".into(), release_request(reply))
            .await
            .unwrap();
    }
    assert_eq!(service.registry.lock().unwrap().prepared_batch_bytes(), 0);
}

#[tokio::test]
async fn aligned_group_windows_reuse_batch_completions() {
    let provider = Arc::new(FakeExportProvider::new());
    let registry = afs_metrics::Registry::new();
    let metrics = HixlMetrics::register(&registry).expect("test metrics");
    let service = HixlReadService::for_test(provider, "source".into(), 9).with_metrics(metrics);
    let authorizer = Arc::new(CountingAuthorizer {
        calls: AtomicUsize::new(0),
    });
    let (_temp, local) = local_store();
    let mut batch = read_request("group", "fixture-grant");
    batch.operations[0].length = 2;
    let mut second = batch.operations[0].clone();
    second.operation_index = 1;
    second.chunk_offset = 2;
    second.length = 2;
    second.destination_offset = 2;
    batch.operations.push(second);
    let session = service
        .negotiate(
            "reader".into(),
            negotiate_request(batch.clone()),
            authorizer.clone(),
        )
        .await
        .unwrap()
        .into_inner();
    let reply = service
        .prepare_group(
            "reader".into(),
            group_prepare_request(
                session.session_id,
                &batch,
                &[(0, 2, "window-0"), (2, 2, "window-1")],
            ),
            authorizer,
            Some(local),
        )
        .await
        .expect("aligned group preparation")
        .into_inner();
    assert_eq!(
        reply.windows[0].reply.as_ref().unwrap().ranges[0].range_checksum,
        blake3::hash(b"da").as_bytes()
    );
    assert_eq!(
        reply.windows[1].reply.as_ref().unwrap().ranges[0].range_checksum,
        blake3::hash(b"ta").as_bytes()
    );
    let families: HashMap<_, _> = registry
        .gather()
        .into_iter()
        .map(|family| (family.name().to_owned(), family))
        .collect();
    assert_eq!(
        families["afs_hixl_source_range_checksums_reused_total"].get_metric()[0]
            .get_counter()
            .get_value(),
        2.0
    );
    assert_eq!(
        families["afs_hixl_source_range_checksums_computed_total"].get_metric()[0]
            .get_counter()
            .get_value(),
        0.0
    );
}

#[tokio::test]
async fn split_group_windows_recompute_split_range_checksums() {
    let provider = Arc::new(FakeExportProvider::new());
    let registry = afs_metrics::Registry::new();
    let metrics = HixlMetrics::register(&registry).expect("test metrics");
    let service = HixlReadService::for_test(provider, "source".into(), 9).with_metrics(metrics);
    let authorizer = Arc::new(CountingAuthorizer {
        calls: AtomicUsize::new(0),
    });
    let (_temp, local) = local_store();
    let batch = read_request("group", "fixture-grant");
    let session = service
        .negotiate(
            "reader".into(),
            negotiate_request(batch.clone()),
            authorizer.clone(),
        )
        .await
        .unwrap()
        .into_inner();
    let reply = service
        .prepare_group(
            "reader".into(),
            group_prepare_request(
                session.session_id,
                &batch,
                &[(0, 2, "window-0"), (2, 2, "window-1")],
            ),
            authorizer,
            Some(local),
        )
        .await
        .expect("split group preparation")
        .into_inner();
    assert_eq!(
        reply.windows[0].reply.as_ref().unwrap().ranges[0].range_checksum,
        blake3::hash(b"da").as_bytes()
    );
    assert_eq!(
        reply.windows[1].reply.as_ref().unwrap().ranges[0].range_checksum,
        blake3::hash(b"ta").as_bytes()
    );
    let families: HashMap<_, _> = registry
        .gather()
        .into_iter()
        .map(|family| (family.name().to_owned(), family))
        .collect();
    assert_eq!(
        families["afs_hixl_source_range_checksums_reused_total"].get_metric()[0]
            .get_counter()
            .get_value(),
        0.0
    );
    assert_eq!(
        families["afs_hixl_source_range_checksums_computed_total"].get_metric()[0]
            .get_counter()
            .get_value(),
        2.0
    );
}

#[tokio::test]
async fn logical_batch_snapshot_survives_backing_corruption_only_until_terminal_release() {
    let (service, _provider, authorizer) = service();
    let (temp, local) = local_store();
    let batch = read_request("snapshot", "fixture-grant");
    let session = service
        .negotiate(
            "reader".into(),
            negotiate_request(batch.clone()),
            authorizer.clone(),
        )
        .await
        .unwrap()
        .into_inner();
    let first = service
        .prepare(
            "reader".into(),
            batch_window_request(session.session_id, &batch, 0, 2, "window-0"),
            authorizer.clone(),
            Some(local.clone()),
        )
        .await
        .unwrap()
        .into_inner();
    service
        .release("reader".into(), release_request(&first))
        .await
        .unwrap();
    assert_eq!(service.registry.lock().unwrap().prepared_batch_bytes(), 4);
    let chunk = temp
        .path()
        .join("chunks")
        .join(&batch.operations[0].chunk_id);
    std::fs::write(chunk, b"evil").unwrap();
    let second = service
        .prepare(
            "reader".into(),
            batch_window_request(session.session_id, &batch, 2, 2, "window-1"),
            authorizer.clone(),
            Some(local.clone()),
        )
        .await
        .expect("the already verified immutable snapshot serves the next window")
        .into_inner();
    assert_eq!(
        second.ranges[0].range_checksum,
        blake3::hash(b"ta").as_bytes()
    );
    service
        .release("reader".into(), release_request(&second))
        .await
        .unwrap();
    assert_eq!(service.registry.lock().unwrap().prepared_batch_bytes(), 0);
    let next = read_request("new-request", "fixture-grant");
    assert!(
        service
            .prepare(
                "reader".into(),
                prepare_request(session.session_id, next),
                authorizer,
                Some(local),
            )
            .await
            .is_err(),
        "a separate request must verify the backing chunk again"
    );
}

#[tokio::test]
async fn one_release_request_covers_every_prepared_window() {
    let (service, provider, authorizer) = service();
    let (_temp, local) = local_store();
    let batch = read_request("batch-release", "fixture-grant");
    let session = service
        .negotiate(
            "reader".into(),
            negotiate_request(batch.clone()),
            authorizer.clone(),
        )
        .await
        .unwrap()
        .into_inner();
    let first = service
        .prepare(
            "reader".into(),
            batch_window_request(session.session_id, &batch, 0, 2, "window-0"),
            authorizer.clone(),
            Some(local.clone()),
        )
        .await
        .unwrap()
        .into_inner();
    let second = service
        .prepare(
            "reader".into(),
            batch_window_request(session.session_id, &batch, 2, 2, "window-1"),
            authorizer,
            Some(local),
        )
        .await
        .unwrap()
        .into_inner();
    let mut request = release_request(&first);
    request.windows.push(DfsHixlReleaseWindow {
        attempt_id: second.attempt_id.clone(),
        slot_id: second.slot_id,
        slot_generation: second.slot_generation,
        outcome: DfsHixlReleaseOutcome::Completed as i32,
        content_validated: true,
    });
    let reply = service
        .release("reader".into(), request)
        .await
        .expect("batched release")
        .into_inner();
    assert_eq!(reply.windows.len(), 2);
    assert!(reply.windows.iter().all(|window| window.consumed));
    assert_eq!(provider.releases.load(Ordering::SeqCst), 2);
    let registry = service.registry.lock().unwrap();
    assert_eq!(registry.prepared_batch_bytes(), 0);
    assert!(registry.sessions[&session.session_id].windows.is_empty());
}

#[tokio::test]
async fn release_failure_quarantines_and_preserves_the_unreleased_window() {
    let (service, provider, authorizer) = service();
    let (_temp, local) = local_store();
    let batch = read_request("release-failure", "fixture-grant");
    let session = service
        .negotiate(
            "reader".into(),
            negotiate_request(batch.clone()),
            authorizer.clone(),
        )
        .await
        .unwrap()
        .into_inner();
    let first = service
        .prepare(
            "reader".into(),
            batch_window_request(session.session_id, &batch, 0, 2, "window-0"),
            authorizer.clone(),
            Some(local.clone()),
        )
        .await
        .unwrap()
        .into_inner();
    let second = service
        .prepare(
            "reader".into(),
            batch_window_request(session.session_id, &batch, 2, 2, "window-1"),
            authorizer,
            Some(local),
        )
        .await
        .unwrap()
        .into_inner();
    let mut request = release_request(&first);
    request.windows.push(DfsHixlReleaseWindow {
        attempt_id: second.attempt_id.clone(),
        slot_id: second.slot_id,
        slot_generation: second.slot_generation,
        outcome: DfsHixlReleaseOutcome::Completed as i32,
        content_validated: true,
    });
    provider.fail_release_at.store(2, Ordering::SeqCst);
    service
        .release("reader".into(), request)
        .await
        .expect_err("the second native release fails");
    assert_eq!(provider.releases.load(Ordering::SeqCst), 2);
    assert_eq!(provider.quarantines.load(Ordering::SeqCst), 1);
    let registry = service.registry.lock().unwrap();
    assert!(registry.admission_frozen);
    assert!(registry.runtime_quarantined);
    let session = &registry.sessions[&session.session_id];
    assert_eq!(session.windows.len(), 1);
    assert_eq!(session.tombstones.by_key.len(), 1);
    assert!(session.windows.contains_key(&WindowKey {
        read_id: second.read_id.clone(),
        attempt_id: second.attempt_id.clone(),
    }));
}

#[tokio::test]
async fn logical_batch_rejects_changed_parent_and_out_of_order_window() {
    let (service, provider, authorizer) = service();
    let (_temp, local) = local_store();
    let batch = read_request("snapshot", "fixture-grant");
    let session = service
        .negotiate(
            "reader".into(),
            negotiate_request(batch.clone()),
            authorizer.clone(),
        )
        .await
        .unwrap()
        .into_inner();
    let first = service
        .prepare(
            "reader".into(),
            batch_window_request(session.session_id, &batch, 0, 1, "window-0"),
            authorizer.clone(),
            Some(local.clone()),
        )
        .await
        .unwrap()
        .into_inner();
    let mut changed = batch.clone();
    changed.operations[0].length = 3;
    for request in [
        batch_window_request(session.session_id, &changed, 1, 1, "window-1"),
        batch_window_request(session.session_id, &batch, 2, 1, "window-2"),
    ] {
        assert!(
            service
                .prepare(
                    "reader".into(),
                    request,
                    authorizer.clone(),
                    Some(local.clone())
                )
                .await
                .is_err()
        );
    }
    assert_eq!(provider.prepares.load(Ordering::SeqCst), 1);
    service
        .release("reader".into(), release_request(&first))
        .await
        .unwrap();
    service
        .close("reader".into(), close_request(session.session_id))
        .await
        .unwrap();
    assert_eq!(service.registry.lock().unwrap().prepared_batch_bytes(), 0);
}

#[tokio::test]
async fn snapshot_reuse_still_reauthorizes_the_complete_parent_request() {
    let (service, provider, authorizer) = service();
    let (_temp, local) = local_store();
    let batch = read_request("snapshot", "fixture-grant");
    let session = service
        .negotiate(
            "reader".into(),
            negotiate_request(batch.clone()),
            authorizer.clone(),
        )
        .await
        .unwrap()
        .into_inner();
    service
        .prepare(
            "reader".into(),
            batch_window_request(session.session_id, &batch, 0, 2, "window-0"),
            authorizer.clone(),
            Some(local.clone()),
        )
        .await
        .unwrap();
    let mut forged = batch;
    forged.operations[0].grant.as_mut().unwrap().token = "revoked".into();
    assert!(
        service
            .prepare(
                "reader".into(),
                batch_window_request(session.session_id, &forged, 2, 2, "window-1"),
                authorizer,
                Some(local)
            )
            .await
            .is_err()
    );
    assert_eq!(provider.prepares.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn expiry_and_drain_reclaim_a_snapshot_after_its_only_export_is_released() {
    for expire in [true, false] {
        let (service, provider, authorizer) = service();
        let (_temp, local) = local_store();
        let batch = read_request("partial-batch", "fixture-grant");
        let session = service
            .negotiate(
                "reader".into(),
                negotiate_request(batch.clone()),
                authorizer.clone(),
            )
            .await
            .unwrap()
            .into_inner();
        let window = service
            .prepare(
                "reader".into(),
                batch_window_request(session.session_id, &batch, 0, 2, "window-0"),
                authorizer,
                Some(local),
            )
            .await
            .unwrap()
            .into_inner();
        service
            .release("reader".into(), release_request(&window))
            .await
            .unwrap();
        {
            let mut registry = service.registry.lock().unwrap();
            assert_eq!(registry.prepared_batch_bytes(), 4);
            assert_eq!(registry.active_window_count(), 0);
            if expire {
                registry
                    .sessions
                    .get_mut(&session.session_id)
                    .unwrap()
                    .last_used = Instant::now() - HIXL_SESSION_TTL;
            }
        }
        if expire {
            service.expire().await.unwrap();
        } else {
            service.drain(Duration::ZERO).await.unwrap();
        }
        assert_eq!(service.registry.lock().unwrap().prepared_batch_bytes(), 0);
        assert_eq!(provider.releases.load(Ordering::SeqCst), 1);
        assert_eq!(provider.quarantines.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn multiple_lanes_share_one_snapshot_and_bind_replay_to_its_lane() {
    let (_temp, local) = local_store();
    let mut fake = FakeExportProvider::new();
    let mut second = fake.info.clone();
    second.engine_id = "fake-second-engine".into();
    fake.additional_infos.push(second);
    fake.slot_capacity = 1;
    let provider = Arc::new(fake);
    let service = HixlReadService::for_test(provider.clone(), "source".into(), 9);
    let authorizer = Arc::new(CountingAuthorizer {
        calls: AtomicUsize::new(0),
    });
    let batch = read_request("two-lanes", "fixture-grant");
    let mut negotiation = negotiate_request(batch.clone());
    negotiation.lane_count = 2;
    let session = service
        .negotiate("reader".into(), negotiation, authorizer.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(session.lanes.len(), 2);
    assert_eq!(session.lanes[1].engine_id, "fake-second-engine");
    let first_request = batch_window_request(session.session_id, &batch, 0, 2, "window-0");
    let first = service
        .prepare(
            "reader".into(),
            first_request.clone(),
            authorizer.clone(),
            Some(local.clone()),
        )
        .await
        .unwrap()
        .into_inner();
    let mut replay_on_another_lane = first_request;
    replay_on_another_lane.lane_id = 1;
    assert!(
        service
            .prepare(
                "reader".into(),
                replay_on_another_lane,
                authorizer.clone(),
                Some(local.clone())
            )
            .await
            .is_err()
    );
    let mut next = batch_window_request(session.session_id, &batch, 2, 2, "window-1");
    assert_eq!(
        service
            .prepare(
                "reader".into(),
                next.clone(),
                authorizer.clone(),
                Some(local.clone())
            )
            .await
            .unwrap_err()
            .code(),
        tonic::Code::ResourceExhausted
    );
    next.lane_id = 1;
    let second = service
        .prepare("reader".into(), next, authorizer, Some(local))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.engine_id, "fake-engine");
    assert_eq!(second.engine_id, "fake-second-engine");
    assert_eq!(first.slot_id, 0);
    assert_eq!(second.slot_id, 0);
    assert_eq!(*provider.prepared_lanes.lock().unwrap(), vec![0, 1]);
    assert_eq!(service.registry.lock().unwrap().prepared_batch_bytes(), 4);
    service
        .release("reader".into(), release_request(&first))
        .await
        .unwrap();
    assert_eq!(service.registry.lock().unwrap().prepared_batch_bytes(), 4);
    service
        .release("reader".into(), release_request(&second))
        .await
        .unwrap();
    assert_eq!(service.registry.lock().unwrap().prepared_batch_bytes(), 0);
    assert_eq!(
        *provider.released_windows.lock().unwrap(),
        vec![(0, 0, 9), (1, 0, 9)]
    );
}

#[tokio::test]
async fn mismatched_lane_count_and_out_of_range_lane_fail_before_export() {
    let (_temp, local) = local_store();
    let (service, provider, authorizer) = service();
    let batch = read_request("lanes", "fixture-grant");
    let mut wrong = negotiate_request(batch.clone());
    wrong.lane_count = 2;
    assert!(
        service
            .negotiate("reader".into(), wrong, authorizer.clone())
            .await
            .is_err()
    );
    assert!(service.registry.lock().unwrap().sessions.is_empty());
    let session = service
        .negotiate(
            "reader".into(),
            negotiate_request(batch.clone()),
            authorizer.clone(),
        )
        .await
        .unwrap()
        .into_inner();
    let mut request = prepare_request(session.session_id, batch);
    request.lane_id = 1;
    assert!(
        service
            .prepare("reader".into(), request, authorizer, Some(local))
            .await
            .is_err()
    );
    assert_eq!(provider.prepares.load(Ordering::SeqCst), 0);
    assert_eq!(service.registry.lock().unwrap().prepared_batch_bytes(), 0);
}

#[tokio::test]
async fn prepared_batch_budget_is_bounded_and_expiry_keeps_native_exports_pinned() {
    let (_temp, local) = local_store();
    let staged = StagedChunk::new(
        crate::dfs::OperationId::new("large-fixture"),
        vec![7; 4 * 1024 * 1024],
    );
    let chunk_id = staged.chunk.id.0.clone();
    local.put(staged).unwrap();
    let (service, provider, authorizer) = service();
    for index in 0..=16 {
        let mut batch = read_request(&format!("budget-{index}"), "fixture-grant");
        batch.operations[0].chunk_id = chunk_id.clone();
        batch.operations[0].length = 4 * 1024 * 1024;
        let session = service
            .negotiate(
                "reader".into(),
                negotiate_request(batch.clone()),
                authorizer.clone(),
            )
            .await
            .unwrap()
            .into_inner();
        let result = service
            .prepare(
                "reader".into(),
                batch_window_request(session.session_id, &batch, 0, 1, "window-0"),
                authorizer.clone(),
                Some(local.clone()),
            )
            .await;
        if index == 16 {
            assert_eq!(result.unwrap_err().code(), tonic::Code::ResourceExhausted);
        } else {
            result.unwrap();
        }
    }
    assert_eq!(provider.prepares.load(Ordering::SeqCst), 16);
    assert_eq!(
        service.registry.lock().unwrap().prepared_batch_bytes(),
        MAX_HIXL_PREPARED_BATCH_BYTES
    );
    service.expire_for_test();
    service.expire().await.unwrap();
    let registry = service.registry.lock().unwrap();
    assert_eq!(registry.prepared_batch_bytes(), 0);
    assert_eq!(registry.active_window_count(), 16);
    assert_eq!(provider.releases.load(Ordering::SeqCst), 0);
    assert_eq!(provider.quarantines.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn group_replay_precedes_closed_frozen_draining_and_capacity_checks() {
    #[derive(Clone, Copy)]
    enum AdmissionState {
        Closed,
        Frozen,
        Draining,
        Capacity,
    }

    for state in [
        AdmissionState::Closed,
        AdmissionState::Frozen,
        AdmissionState::Draining,
        AdmissionState::Capacity,
    ] {
        let (_temp, local) = local_store();
        let mut fake = FakeExportProvider::new();
        if matches!(state, AdmissionState::Capacity) {
            fake.slot_capacity = 2;
        }
        let provider = Arc::new(fake);
        let service = HixlReadService::for_test(provider.clone(), "source".into(), 9);
        let authorizer = Arc::new(CountingAuthorizer {
            calls: AtomicUsize::new(0),
        });
        let batch = read_request("group-replay", "fixture-grant");
        let session = service
            .negotiate(
                "reader".into(),
                negotiate_request(batch.clone()),
                authorizer.clone(),
            )
            .await
            .unwrap()
            .into_inner();
        let request = group_prepare_request(
            session.session_id,
            &batch,
            &[(0, 2, "window-0"), (2, 2, "window-1")],
        );
        let first = service
            .prepare_group(
                "reader".into(),
                request.clone(),
                authorizer.clone(),
                Some(local.clone()),
            )
            .await
            .expect("initial group prepare")
            .into_inner();
        match state {
            AdmissionState::Closed => {
                service
                    .close("reader".into(), close_request(session.session_id))
                    .await
                    .unwrap();
            }
            AdmissionState::Frozen => {
                service.registry.lock().unwrap().admission_frozen = true;
            }
            AdmissionState::Draining => {
                service.registry.lock().unwrap().draining = true;
            }
            AdmissionState::Capacity => {}
        }
        let replay = service
            .prepare_group(
                "reader".into(),
                request,
                authorizer.clone(),
                Some(local.clone()),
            )
            .await
            .expect("a complete replay bypasses new-window admission checks")
            .into_inner();
        for (original, replayed) in first.windows.iter().zip(&replay.windows) {
            assert_eq!(
                original.reply.as_ref().unwrap().slot_id,
                replayed.reply.as_ref().unwrap().slot_id
            );
        }
        assert_eq!(provider.prepares.load(Ordering::SeqCst), 2);

        let partial = service
            .prepare_group(
                "reader".into(),
                group_prepare_request(
                    session.session_id,
                    &batch,
                    &[(0, 2, "window-0"), (2, 2, "new-window")],
                ),
                authorizer.clone(),
                Some(local.clone()),
            )
            .await
            .expect_err("partial replay is invalid before admission checks");
        assert_eq!(partial.code(), tonic::Code::InvalidArgument);

        let expected = match state {
            AdmissionState::Capacity => tonic::Code::ResourceExhausted,
            _ => tonic::Code::Unavailable,
        };
        let new = service
            .prepare_group(
                "reader".into(),
                group_prepare_request(
                    session.session_id,
                    &batch,
                    &[(0, 2, "new-0"), (2, 2, "new-1")],
                ),
                authorizer,
                Some(local),
            )
            .await
            .expect_err("new windows remain subject to admission checks");
        assert_eq!(new.code(), expected);
        assert_eq!(provider.prepares.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn prepare_replay_reauthorizes_without_allocating_a_second_window() {
    let (_temp, local) = local_store();
    let (service, provider, authorizer) = service();
    let read = read_request("read", "fixture-grant");
    let session = service
        .negotiate(
            "reader".into(),
            negotiate_request(read.clone()),
            authorizer.clone(),
        )
        .await
        .expect("negotiate")
        .into_inner();
    let first = service
        .prepare(
            "reader".into(),
            prepare_request(session.session_id, read.clone()),
            authorizer.clone(),
            Some(local.clone()),
        )
        .await
        .expect("first prepare")
        .into_inner();
    let replay = service
        .prepare(
            "reader".into(),
            prepare_request(session.session_id, read),
            authorizer.clone(),
            Some(local),
        )
        .await
        .expect("replayed prepare")
        .into_inner();
    assert_eq!(first.slot_id, replay.slot_id);
    assert_eq!(first.slot_generation, replay.slot_generation);
    assert_eq!(provider.prepares.load(Ordering::SeqCst), 1);
    assert_eq!(authorizer.calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn wrong_peer_epoch_and_grant_are_rejected_before_export() {
    let (_temp, local) = local_store();
    let (service, provider, authorizer) = service();
    let read = read_request("read", "fixture-grant");
    let session = service
        .negotiate(
            "reader".into(),
            negotiate_request(read.clone()),
            authorizer.clone(),
        )
        .await
        .expect("negotiate")
        .into_inner();
    assert!(
        service
            .prepare(
                "intruder".into(),
                prepare_request(session.session_id, read.clone()),
                authorizer.clone(),
                Some(local.clone()),
            )
            .await
            .is_err()
    );
    let mut stale = prepare_request(session.session_id, read.clone());
    stale.caller_node_epoch = 2;
    assert!(
        service
            .prepare(
                "reader".into(),
                stale,
                authorizer.clone(),
                Some(local.clone()),
            )
            .await
            .is_err()
    );
    let mut forged = read;
    forged.operations[0].grant.as_mut().expect("grant").token = "forged".into();
    assert!(
        service
            .prepare(
                "reader".into(),
                prepare_request(session.session_id, forged),
                authorizer,
                Some(local),
            )
            .await
            .is_err()
    );
    assert_eq!(provider.prepares.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn malformed_prepare_is_rejected_before_authorization_or_export() {
    let (service, provider, authorizer) = service();
    let error = service
        .prepare(
            "reader".into(),
            DfsPrepareReadRangesHixlRequest {
                session_id: 1,
                read_request: None,
                caller_node_epoch: 1,
                source_node_epoch: 9,
                batch_read_request: None,
                batch_offset: 0,
                lane_id: 0,
            },
            authorizer.clone(),
            None,
        )
        .await
        .expect_err("missing read request");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert_eq!(provider.prepares.load(Ordering::SeqCst), 0);
    assert_eq!(authorizer.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn capacity_close_ttl_and_release_idempotence_keep_unknown_exports_pinned() {
    let (_temp, local) = local_store();
    let (service, provider, authorizer) = service();
    let first_read = read_request("read-0", "fixture-grant");
    let mut batch = first_read.clone();
    batch.operations = (0..MAX_HIXL_WINDOWS)
        .map(|index| {
            let mut operation = first_read.operations[0].clone();
            operation.operation_index = u32::try_from(index).unwrap();
            operation.destination_offset = u64::try_from(index).unwrap() * 4;
            operation
        })
        .collect();
    let session = service
        .negotiate(
            "reader".into(),
            negotiate_request(first_read.clone()),
            authorizer.clone(),
        )
        .await
        .expect("negotiate")
        .into_inner();
    let first_request = batch_window_request(session.session_id, &batch, 0, 4, "window-0");
    let first = service
        .prepare(
            "reader".into(),
            first_request.clone(),
            authorizer.clone(),
            Some(local.clone()),
        )
        .await
        .expect("first prepare")
        .into_inner();
    for index in 1..MAX_HIXL_WINDOWS {
        service
            .prepare(
                "reader".into(),
                batch_window_request(
                    session.session_id,
                    &batch,
                    u64::try_from(index).unwrap() * 4,
                    4,
                    &format!("window-{index}"),
                ),
                authorizer.clone(),
                Some(local.clone()),
            )
            .await
            .expect("window within capacity");
    }
    let empty = service
        .negotiate(
            "reader".into(),
            negotiate_request(read_request("empty", "fixture-grant")),
            authorizer.clone(),
        )
        .await
        .unwrap()
        .into_inner();
    for index in 0..3 {
        service
            .registry
            .lock()
            .unwrap()
            .sessions
            .get_mut(&empty.session_id)
            .unwrap()
            .last_used = Instant::now() - (HIXL_SESSION_TTL - Duration::from_secs(1));
        let active_since = Instant::now();
        let refused = service
            .prepare(
                "reader".into(),
                prepare_request(
                    empty.session_id,
                    read_request(&format!("refused-{index}"), "fixture-grant"),
                ),
                authorizer.clone(),
                Some(local.clone()),
            )
            .await
            .unwrap_err();
        assert_eq!(refused.code(), tonic::Code::ResourceExhausted);
        let registry = service.registry.lock().unwrap();
        let active = &registry.sessions[&empty.session_id];
        assert!(
            active.last_used >= active_since,
            "authenticated capacity refusal must refresh an active empty control session"
        );
        assert!(active.windows.is_empty());
    }
    let capacity = service
        .prepare(
            "reader".into(),
            prepare_request(
                session.session_id,
                read_request("read-capacity", "fixture-grant"),
            ),
            authorizer.clone(),
            Some(local.clone()),
        )
        .await
        .expect_err("capacity must be bounded");
    assert_eq!(capacity.code(), tonic::Code::ResourceExhausted);
    let mut unknown = release_request(&first);
    unknown.windows[0].outcome = DfsHixlReleaseOutcome::Unknown as i32;
    let unknown_reply = service
        .release("reader".into(), unknown)
        .await
        .expect("unknown release")
        .into_inner();
    assert!(!unknown_reply.windows[0].consumed);
    assert_eq!(provider.releases.load(Ordering::SeqCst), 0);
    let frozen = service
        .negotiate(
            "reader".into(),
            negotiate_request(read_request("after-unknown", "fixture-grant")),
            authorizer.clone(),
        )
        .await
        .expect_err("unknown native outcome must freeze new HIXL admission");
    assert_eq!(frozen.code(), tonic::Code::Unavailable);

    service.expire_for_test();
    let closed = service
        .close("reader".into(), close_request(session.session_id))
        .await
        .expect("close")
        .into_inner();
    assert!(closed.admitted);
    assert!(!closed.drained);
    assert_eq!(provider.quarantines.load(Ordering::SeqCst), 1);
    let replay_after_close = service
        .prepare(
            "reader".into(),
            first_request,
            authorizer.clone(),
            Some(local.clone()),
        )
        .await
        .expect("closed session must still replay an admitted window")
        .into_inner();
    assert_eq!(replay_after_close.slot_id, first.slot_id);
    let rejected = service
        .prepare(
            "reader".into(),
            prepare_request(
                session.session_id,
                read_request("after-close", "fixture-grant"),
            ),
            authorizer.clone(),
            Some(local.clone()),
        )
        .await
        .expect_err("closed session must reject new windows");
    assert_eq!(rejected.code(), tonic::Code::Unavailable);

    let released = service
        .release("reader".into(), release_request(&first))
        .await
        .expect("release")
        .into_inner();
    assert!(released.windows[0].consumed);
    let replay = service
        .release("reader".into(), release_request(&first))
        .await
        .expect("idempotent release")
        .into_inner();
    assert!(replay.windows[0].already_released);
    let mut mismatched = release_request(&first);
    mismatched.windows[0].content_validated = false;
    let mismatch = service
        .release("reader".into(), mismatched)
        .await
        .expect_err("release replay identity must include validation result");
    assert_eq!(mismatch.code(), tonic::Code::InvalidArgument);
    assert_eq!(provider.releases.load(Ordering::SeqCst), 1);

    for index in 1..=MAX_HIXL_WINDOWS {
        let read = read_request(&format!("after-close-{index}"), "fixture-grant");
        let result = service
            .prepare(
                "reader".into(),
                prepare_request(session.session_id, read),
                authorizer.clone(),
                Some(local.clone()),
            )
            .await;
        assert!(result.is_err(), "closed session must stay closed");
    }
}

#[tokio::test]
async fn failed_native_outcome_does_not_release_a_source_window() {
    let (_temp, local) = local_store();
    let (service, provider, authorizer) = service();
    let read = read_request("failed-read", "fixture-grant");
    let session = service
        .negotiate(
            "reader".into(),
            negotiate_request(read.clone()),
            authorizer.clone(),
        )
        .await
        .unwrap()
        .into_inner();
    let window = service
        .prepare(
            "reader".into(),
            prepare_request(session.session_id, read),
            authorizer,
            Some(local),
        )
        .await
        .unwrap()
        .into_inner();
    let mut release = release_request(&window);
    release.windows[0].outcome = DfsHixlReleaseOutcome::Failed as i32;
    release.windows[0].content_validated = false;
    let reply = service
        .release("reader".into(), release)
        .await
        .unwrap()
        .into_inner();
    assert!(!reply.windows[0].consumed);
    assert_eq!(provider.releases.load(Ordering::SeqCst), 0);
    assert_eq!(provider.quarantines.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn empty_close_remains_idempotent_across_periodic_expiration() {
    let (service, _provider, authorizer) = service();
    let session = service
        .negotiate(
            "reader".into(),
            negotiate_request(read_request("close-read", "fixture-grant")),
            authorizer,
        )
        .await
        .unwrap()
        .into_inner();
    let request = close_request(session.session_id);
    assert!(
        service
            .close("reader".into(), request.clone())
            .await
            .unwrap()
            .into_inner()
            .drained
    );
    service.expire().await.unwrap();
    assert!(
        service
            .close("reader".into(), request)
            .await
            .unwrap()
            .into_inner()
            .drained
    );
}

#[test]
fn tombstones_bound_release_deduplication() {
    let mut tombstones = Tombstones::default();
    for index in 0..(MAX_HIXL_RELEASE_TOMBSTONES + 8) {
        let key = WindowKey {
            read_id: format!("read-{index}"),
            attempt_id: "attempt".into(),
        };
        tombstones.insert(ReleaseTombstone {
            key,
            engine_id: "engine".into(),
            engine_generation: 1,
            pool_generation: 1,
            slot_id: index as u64 + 1,
            slot_generation: 1,
            caller_epoch: 1,
            source_epoch: 1,
            outcome: DfsHixlReleaseOutcome::Completed as i32,
            content_validated: true,
            inserted_at: Instant::now(),
        });
    }
    assert_eq!(tombstones.order.len(), MAX_HIXL_RELEASE_TOMBSTONES);
    assert_eq!(tombstones.by_key.len(), MAX_HIXL_RELEASE_TOMBSTONES);
}

#[test]
fn expired_empty_sessions_are_evicted_but_pending_windows_are_retained() {
    let expired_at = Instant::now() - HIXL_SESSION_TTL;
    let mut registry = Registry::default();
    registry.sessions.insert(
        1,
        Session {
            peer: "reader".into(),
            caller_epoch: 1,
            source_epoch: 9,
            engine_id: "engine".into(),
            engine_generation: 1,
            lanes: Vec::new(),
            closed: true,
            last_used: expired_at,
            windows: HashMap::new(),
            tombstones: Tombstones::default(),
            prepared_batch: None,
        },
    );
    let pending_key = WindowKey {
        read_id: "pending".into(),
        attempt_id: "attempt".into(),
    };
    let mut pending_windows = HashMap::new();
    pending_windows.insert(
        pending_key,
        WindowRecord {
            request: DfsReadRangesRequest::default(),
            reply: DfsPrepareReadRangesHixlReply::default(),
            batch_request: Arc::new(DfsReadRangesRequest::default()),
            batch_offset: 0,
            lane_id: 0,
        },
    );
    registry.sessions.insert(
        2,
        Session {
            peer: "reader".into(),
            caller_epoch: 1,
            source_epoch: 9,
            engine_id: "engine".into(),
            engine_generation: 1,
            lanes: Vec::new(),
            closed: true,
            last_used: expired_at,
            windows: pending_windows,
            tombstones: Tombstones::default(),
            prepared_batch: None,
        },
    );

    registry.expire_sessions();

    assert!(!registry.sessions.contains_key(&1));
    assert!(registry.sessions.contains_key(&2));
}

#[test]
fn release_identity_rejects_generation_replay() {
    let window = WindowRecord {
        request: DfsReadRangesRequest {
            read_id: "read".into(),
            attempt_id: "attempt".into(),
            file_version_id: "version".into(),
            layout_root_id: "layout".into(),
            include_blake3_outboard: false,
            operations: vec![DfsChunkReadOp {
                operation_index: 0,
                chunk_id: "chunk".into(),
                chunk_offset: 0,
                length: 1,
                destination_offset: 0,
                source_copy_id: "copy".into(),
                grant: Some(DfsReadGrant {
                    caller_node_epoch: 3,
                    ..DfsReadGrant::default()
                }),
            }],
        },
        reply: DfsPrepareReadRangesHixlReply {
            session_id: 5,
            source_node_epoch: 9,
            engine_id: "engine".into(),
            engine_generation: 11,
            pool_generation: 11,
            slot_id: 13,
            slot_generation: 17,
            ..DfsPrepareReadRangesHixlReply::default()
        },
        batch_request: Arc::new(DfsReadRangesRequest::default()),
        batch_offset: 0,
        lane_id: 0,
    };
    let request = DfsReleaseHixlReadRequest {
        session_id: 5,
        read_id: "read".into(),
        source_node_id: "source".into(),
        source_node_epoch: 9,
        caller_node_epoch: 3,
        engine_id: "engine".into(),
        engine_generation: 11,
        pool_generation: 11,
        windows: vec![DfsHixlReleaseWindow {
            attempt_id: "attempt".into(),
            slot_id: 13,
            slot_generation: 18,
            ..DfsHixlReleaseWindow::default()
        }],
    };
    assert!(validate_release_identity(&window, &request, &request.windows[0]).is_err());
}
