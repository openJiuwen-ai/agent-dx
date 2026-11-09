use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::node::chunk::{ChunkStore, StagedChunk};
use crate::node::rpc::hixl::test_support::{engine, native_error};
use afs_protocol::node_data::{DfsChunkReadOp, DfsHixlGroupWindow, DfsHixlTransport, DfsReadGrant};
use afs_transport::hixl::{HixlErrorKind, HixlPhase, ReadTransport};

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
    /// Quarantine calls that fail before one is accepted.
    fail_quarantines: AtomicUsize,
}

impl FakeExportProvider {
    fn new() -> Self {
        Self::with_info(engine_info(ReadTransport::Roce))
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
            fail_quarantines: AtomicUsize::new(0),
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

    fn prepare_export_shared(
        &self,
        lane: usize,
        data: Arc<Vec<u8>>,
        offset: usize,
        length: usize,
        _deadline: Instant,
    ) -> Result<ExportWindow, HixlError> {
        let bytes = data[offset..offset + length].to_vec();
        let ordinal = self.prepares.fetch_add(1, Ordering::SeqCst) + 1;
        if ordinal == self.fail_prepare_at.load(Ordering::SeqCst) {
            return Err(native_error(
                HixlPhase::PrepareExport,
                HixlErrorKind::ResourceExhausted,
                "injected export failure",
            ));
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
            return Err(native_error(
                HixlPhase::ReleaseExport,
                HixlErrorKind::NativeFailure,
                "injected release failure",
            ));
        }
        Ok(())
    }

    fn quarantine(&self) -> Result<(), HixlError> {
        self.quarantines.fetch_add(1, Ordering::SeqCst);
        if self
            .fail_quarantines
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok()
        {
            return Err(native_error(
                HixlPhase::Quarantine,
                HixlErrorKind::ResourceExhausted,
                "injected quarantine failure",
            ));
        }
        Ok(())
    }
}

fn engine_info(transport: ReadTransport) -> EngineInfo {
    let hccs = transport == ReadTransport::HccsBufferPool;
    EngineInfo {
        transport,
        buffer_pool_count: if hccs { 4 } else { 0 },
        buffer_pool_size_mib: if hccs { 64 } else { 0 },
        hccs_domain_id: hccs.then_some(0),
        ..engine("fake-engine", 7, 4 * 1024 * 1024)
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

fn prewarm_request() -> DfsNegotiateHixlRequest {
    DfsNegotiateHixlRequest {
        read_request: None,
        ..negotiate_request(read_request("roce", "unused"))
    }
}

fn set_hccs_pool(request: &mut DfsNegotiateHixlRequest, count: u32, size_mib: u32) {
    request.transport = DfsHixlTransport::HccsBufferPool as i32;
    request.buffer_pool_count = count;
    request.buffer_pool_size_mib = size_mib;
    request.hccs_domain_id = Some(0);
}

/// One window of a logical batch, prepared as a group of one.
#[derive(Clone, Default)]
struct OneWindow {
    session_id: u64,
    read_request: Option<DfsReadRangesRequest>,
    caller_node_epoch: u64,
    source_node_epoch: u64,
    batch_read_request: Option<DfsReadRangesRequest>,
    batch_offset: u64,
    lane_id: u32,
}

impl From<OneWindow> for DfsPrepareReadRangesHixlGroupRequest {
    fn from(window: OneWindow) -> Self {
        Self {
            session_id: window.session_id,
            caller_node_epoch: window.caller_node_epoch,
            source_node_epoch: window.source_node_epoch,
            batch_read_request: window.batch_read_request,
            windows: vec![DfsHixlGroupWindow {
                batch_offset: window.batch_offset,
                lane_id: window.lane_id,
                read_request: window.read_request,
            }],
        }
    }
}

/// The whole read as the single window of its own logical batch.
fn prepare_request(session_id: u64, read: DfsReadRangesRequest) -> OneWindow {
    OneWindow {
        session_id,
        batch_read_request: Some(read.clone()),
        read_request: Some(read),
        caller_node_epoch: 1,
        source_node_epoch: 9,
        ..OneWindow::default()
    }
}

fn window_request(
    session_id: u64,
    batch: &DfsReadRangesRequest,
    offset: u64,
    length: u64,
    attempt: &str,
) -> OneWindow {
    let group = group_request(session_id, batch, &[(offset, length, attempt)]);
    let window = group.windows[0].clone();
    OneWindow {
        session_id,
        read_request: window.read_request,
        caller_node_epoch: 1,
        source_node_epoch: 9,
        batch_read_request: Some(batch.clone()),
        batch_offset: offset,
        lane_id: 0,
    }
}

fn group_request(
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

fn window_replies(
    reply: &DfsPrepareReadRangesHixlGroupReply,
) -> Vec<&DfsPrepareReadRangesHixlReply> {
    reply
        .windows
        .iter()
        .map(|window| window.reply.as_ref().expect("prepared window reply"))
        .collect()
}

fn assert_checksum(reply: &DfsPrepareReadRangesHixlReply, bytes: &[u8]) {
    assert_eq!(
        reply.ranges[0].range_checksum,
        blake3::hash(bytes).as_bytes()
    );
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

/// One release request for windows of the same logical read.
fn release_all(replies: &[&DfsPrepareReadRangesHixlReply]) -> DfsReleaseHixlReadRequest {
    let mut request = release_request(replies[0]);
    request.windows = replies
        .iter()
        .flat_map(|reply| release_request(reply).windows)
        .collect();
    request
}

/// A source service over a fake export provider and a local store that
/// holds the 4-byte `data` chunk.
struct Fixture {
    service: HixlReadService,
    provider: Arc<FakeExportProvider>,
    authorizer: Arc<CountingAuthorizer>,
    temp: tempfile::TempDir,
    local: Arc<LocalChunkStore>,
}

impl Fixture {
    fn new() -> Self {
        Self::with_provider(FakeExportProvider::new())
    }

    fn with_transport(transport: ReadTransport) -> Self {
        Self::with_provider(FakeExportProvider::with_info(engine_info(transport)))
    }

    fn with_provider(provider: FakeExportProvider) -> Self {
        let provider = Arc::new(provider);
        let temp = tempfile::tempdir().expect("test tempdir");
        let local = Arc::new(LocalChunkStore::open(temp.path(), "source").expect("test store"));
        let staged = StagedChunk::new(crate::dfs::OperationId::new("fixture"), b"data".to_vec());
        local.put(staged).expect("test chunk");
        Self {
            service: HixlReadService::from_provider(provider.clone(), "source".into(), 9),
            provider,
            authorizer: Arc::new(CountingAuthorizer {
                calls: AtomicUsize::new(0),
            }),
            temp,
            local,
        }
    }

    fn with_metrics() -> (Self, afs_metrics::Registry) {
        let mut fixture = Self::new();
        let registry = afs_metrics::Registry::new();
        let metrics = HixlMetrics::register(&registry).expect("test metrics");
        fixture.service = fixture.service.clone().with_metrics(metrics);
        (fixture, registry)
    }

    fn prepares(&self) -> usize {
        self.provider.prepares.load(Ordering::SeqCst)
    }

    fn releases(&self) -> usize {
        self.provider.releases.load(Ordering::SeqCst)
    }

    fn quarantines(&self) -> usize {
        self.provider.quarantines.load(Ordering::SeqCst)
    }

    fn authorizations(&self) -> usize {
        self.authorizer.calls.load(Ordering::SeqCst)
    }

    fn prepared_bytes(&self) -> usize {
        self.service.registry.lock().unwrap().prepared_batch_bytes()
    }

    fn sessions(&self) -> Vec<test_support::SessionState> {
        test_support::sessions(&self.service)
    }

    fn corrupt_chunk(&self, batch: &DfsReadRangesRequest) {
        let chunk = self
            .temp
            .path()
            .join("chunks")
            .join(&batch.operations[0].chunk_id);
        std::fs::write(chunk, b"evil").unwrap();
    }

    async fn negotiate(
        &self,
        request: DfsNegotiateHixlRequest,
    ) -> Result<DfsNegotiateHixlReply, Status> {
        self.service
            .negotiate("reader".into(), request, self.authorizer.clone())
            .await
            .map(Response::into_inner)
    }

    /// Negotiates a session for `batch` and returns its id.
    async fn session(&self, batch: &DfsReadRangesRequest) -> u64 {
        self.negotiate(negotiate_request(batch.clone()))
            .await
            .expect("negotiate")
            .session_id
    }

    async fn prepare_as(
        &self,
        peer: &str,
        window: OneWindow,
        local: Option<Arc<LocalChunkStore>>,
    ) -> Result<DfsPrepareReadRangesHixlReply, Status> {
        let reply = self
            .service
            .prepare_group(peer.into(), window.into(), self.authorizer.clone(), local)
            .await?
            .into_inner();
        Ok(reply
            .windows
            .into_iter()
            .next()
            .and_then(|window| window.reply)
            .expect("a group of one returns its window"))
    }

    async fn prepare(&self, window: OneWindow) -> Result<DfsPrepareReadRangesHixlReply, Status> {
        self.prepare_as("reader", window, Some(self.local.clone()))
            .await
    }

    async fn group(
        &self,
        request: DfsPrepareReadRangesHixlGroupRequest,
    ) -> Result<DfsPrepareReadRangesHixlGroupReply, Status> {
        self.service
            .prepare_group(
                "reader".into(),
                request,
                self.authorizer.clone(),
                Some(self.local.clone()),
            )
            .await
            .map(Response::into_inner)
    }

    async fn release(
        &self,
        request: DfsReleaseHixlReadRequest,
    ) -> Result<DfsReleaseHixlReadReply, Status> {
        self.service
            .release("reader".into(), request)
            .await
            .map(Response::into_inner)
    }

    async fn close(&self, session_id: u64) -> Result<DfsCloseHixlReply, Status> {
        self.service
            .close("reader".into(), close_request(session_id))
            .await
            .map(Response::into_inner)
    }

    /// Negotiates and admits the whole fixture read as one window.
    async fn admitted(&self) -> DfsPrepareReadRangesHixlReply {
        let read = read_request("read", "fixture-grant");
        let session = self.session(&read).await;
        self.prepare(prepare_request(session, read)).await.unwrap()
    }

    /// Negotiates `batch` and prepares its two halves as separate waves.
    async fn two_waves(
        &self,
        batch: &DfsReadRangesRequest,
    ) -> (
        u64,
        DfsPrepareReadRangesHixlReply,
        DfsPrepareReadRangesHixlReply,
    ) {
        let session = self.session(batch).await;
        let first = self
            .prepare(window_request(session, batch, 0, 2, "window-0"))
            .await
            .unwrap();
        let second = self
            .prepare(window_request(session, batch, 2, 2, "window-1"))
            .await
            .unwrap();
        (session, first, second)
    }
}

#[tokio::test]
async fn negotiate_echoes_the_source_transport_and_pool_parameters() {
    for (source, transport, count, size_mib, domain) in [
        (ReadTransport::Roce, DfsHixlTransport::Roce, 0, 0, None),
        (
            ReadTransport::HccsBufferPool,
            DfsHixlTransport::HccsBufferPool,
            4,
            64,
            Some(0),
        ),
    ] {
        let fixture = Fixture::with_transport(source);
        let mut request = negotiate_request(read_request("echo", "fixture-grant"));
        if source == ReadTransport::HccsBufferPool {
            set_hccs_pool(&mut request, 4, 64);
        }
        let reply = fixture.negotiate(request).await.expect("negotiation");
        assert_eq!(reply.protocol_version, HIXL_PROTOCOL_VERSION);
        assert_eq!(
            (
                reply.transport,
                reply.buffer_pool_count,
                reply.buffer_pool_size_mib,
                reply.hccs_domain_id
            ),
            (transport as i32, count, size_mib, domain)
        );
    }
}

#[tokio::test]
async fn negotiate_prewarms_without_read_authority() {
    let fixture = Fixture::new();
    let reply = fixture
        .negotiate(prewarm_request())
        .await
        .expect("pre-warm negotiation");

    assert_eq!(reply.protocol_version, HIXL_PROTOCOL_VERSION);
    assert_eq!(reply.transport, DfsHixlTransport::Roce as i32);
    assert!(!reply.lanes.is_empty(), "pre-warm still binds lanes");
    assert_eq!(fixture.authorizations(), 0);
}

#[tokio::test]
async fn prewarm_rejects_stale_source_epoch_and_zero_caller_epoch() {
    let fixture = Fixture::new();
    let mut stale_source = prewarm_request();
    stale_source.source_node_epoch = 8;
    let mut zero_caller = prewarm_request();
    zero_caller.caller_node_epoch = 0;
    for request in [stale_source, zero_caller] {
        let status = fixture
            .negotiate(request)
            .await
            .expect_err("stale pre-warm epochs must be denied");
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }
}

#[tokio::test]
async fn negotiate_rejects_mismatches_without_allocating_a_session() {
    type Mutation = fn(&mut DfsNegotiateHixlRequest);
    use ReadTransport::{HccsBufferPool, Roce};
    use tonic::Code::{InvalidArgument, Unimplemented};
    // The protocol version is checked before authorization; transport, pool,
    // domain and lane parameters only after it.
    let cases: [(ReadTransport, Mutation, tonic::Code, usize); 9] = [
        (
            Roce,
            |request| request.protocol_version = HIXL_PROTOCOL_VERSION + 1,
            Unimplemented,
            0,
        ),
        (
            Roce,
            |request| set_hccs_pool(request, 4, 64),
            InvalidArgument,
            1,
        ),
        (Roce, |request| request.transport = 99, InvalidArgument, 1),
        (
            Roce,
            |request| {
                request.buffer_pool_count = 1;
                request.buffer_pool_size_mib = 1;
            },
            InvalidArgument,
            1,
        ),
        (
            Roce,
            |request| request.buffer_pool_size_mib = 1,
            InvalidArgument,
            1,
        ),
        (Roce, |request| request.lane_count = 2, InvalidArgument, 1),
        (
            HccsBufferPool,
            |request| set_hccs_pool(request, 0, 64),
            InvalidArgument,
            1,
        ),
        (
            HccsBufferPool,
            |request| {
                set_hccs_pool(request, 4, 64);
                request.hccs_domain_id = None;
            },
            InvalidArgument,
            1,
        ),
        (
            HccsBufferPool,
            |request| {
                set_hccs_pool(request, 4, 64);
                request.hccs_domain_id = Some(7);
            },
            InvalidArgument,
            1,
        ),
    ];

    for (index, (source, mutate, code, authorizations)) in cases.into_iter().enumerate() {
        let fixture = Fixture::with_transport(source);
        let mut request = negotiate_request(read_request("rejected", "fixture-grant"));
        mutate(&mut request);
        let error = fixture
            .negotiate(request)
            .await
            .expect_err("a mismatched negotiation must be rejected");
        assert_eq!(error.code(), code, "case {index}");
        assert_eq!(fixture.authorizations(), authorizations, "case {index}");
        assert!(fixture.sessions().is_empty(), "case {index}");
    }
}

#[tokio::test]
async fn never_submitted_window_is_released_without_quarantining_source() {
    let fixture = Fixture::new();
    let prepared = fixture.admitted().await;
    let mut request = release_request(&prepared);
    request.windows[0].outcome = DfsHixlReleaseOutcome::NotSubmitted as i32;
    assert!(
        fixture.release(request.clone()).await.is_err(),
        "never-submitted cannot claim validated content"
    );
    request.windows[0].content_validated = false;
    assert!(fixture.release(request.clone()).await.unwrap().windows[0].consumed);
    assert!(fixture.release(request).await.unwrap().windows[0].already_released);
    assert_eq!((fixture.quarantines(), fixture.releases()), (0, 1));
    fixture
        .prepare(prepare_request(
            prepared.session_id,
            read_request("next", "fixture-grant"),
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn shutdown_drain_stops_admission_but_accepts_release() {
    let fixture = Fixture::new();
    let prepared = fixture.admitted().await;
    let draining = fixture.service.clone();
    let task = tokio::spawn(async move { draining.drain(Duration::from_secs(1)).await });
    tokio::time::timeout(Duration::from_millis(200), async {
        while !fixture.service.registry.lock().unwrap().draining {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        !task.is_finished(),
        "pending source window must keep drain waiting"
    );
    let next = read_request("next", "fixture-grant");
    assert!(
        fixture
            .negotiate(negotiate_request(next.clone()))
            .await
            .is_err()
    );
    assert!(
        fixture
            .prepare(prepare_request(prepared.session_id, next))
            .await
            .is_err()
    );
    assert_eq!((fixture.releases(), fixture.quarantines()), (0, 0));
    fixture.release(release_request(&prepared)).await.unwrap();
    task.await.unwrap().unwrap();
    assert_eq!((fixture.releases(), fixture.quarantines()), (1, 0));
}

#[tokio::test]
async fn shutdown_drain_timeout_quarantines_and_keeps_pending_window() {
    let fixture = Fixture::new();
    fixture.admitted().await;
    let error = fixture
        .service
        .drain(Duration::from_millis(10))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!((fixture.quarantines(), fixture.releases()), (1, 0));
    assert_eq!(
        fixture
            .service
            .registry
            .lock()
            .unwrap()
            .active_window_count(),
        1
    );
}

#[tokio::test]
async fn prepare_releases_invalid_export_and_quarantines_if_cleanup_fails() {
    // A group of one releases only its invalid export; an invalid later
    // window also rolls back the export pinned before it.
    let one: &[(u64, u64, &str)] = &[(0, 4, "window-0")];
    let two: &[(u64, u64, &str)] = &[(0, 2, "window-0"), (2, 2, "window-1")];
    for (windows, invalid_at) in [(one, 1), (two, 2)] {
        for cleanup_fails in [false, true] {
            let fixture = Fixture::new();
            let batch = read_request("invalid-export", "fixture-grant");
            let session = fixture.session(&batch).await;
            fixture
                .provider
                .invalid_export_at
                .store(invalid_at, Ordering::SeqCst);
            if cleanup_fails {
                fixture
                    .provider
                    .fail_release_at
                    .store(invalid_at, Ordering::SeqCst);
            }
            let error = fixture
                .group(group_request(session, &batch, windows))
                .await
                .expect_err("an invalid export descriptor must fail the whole group");
            assert_eq!(error.code(), tonic::Code::Internal);
            assert_eq!(fixture.releases(), invalid_at);
            let state = &fixture.sessions()[0];
            assert_eq!((state.active_windows, state.prepared_bytes), (0, 0));
            assert_eq!(fixture.quarantines(), usize::from(cleanup_fails));
            assert_eq!(
                fixture.service.registry.lock().unwrap().admission_frozen,
                cleanup_fails
            );
        }
    }
}

#[tokio::test]
async fn group_prepare_pins_every_window_from_one_verified_snapshot() {
    let fixture = Fixture::new();
    let batch = read_request("group", "fixture-grant");
    let session = fixture.session(&batch).await;
    let reply = fixture
        .group(group_request(
            session,
            &batch,
            &[(0, 2, "window-0"), (2, 2, "window-1")],
        ))
        .await
        .expect("group preparation");
    assert_eq!(reply.session_id, session);
    assert_eq!(
        reply
            .windows
            .iter()
            .map(|window| window.batch_offset)
            .collect::<Vec<_>>(),
        [0, 2]
    );
    let windows = window_replies(&reply);
    assert_checksum(windows[0], b"da");
    assert_checksum(windows[1], b"ta");
    assert_ne!(windows[0].slot_id, windows[1].slot_id);
    assert_eq!(fixture.prepares(), 2);
    let states = fixture.sessions();
    assert_eq!(states.len(), 1);
    assert_eq!((states[0].prepared_bytes, states[0].active_windows), (4, 2));
    for window in windows {
        fixture.release(release_request(window)).await.unwrap();
    }
    let state = &fixture.sessions()[0];
    assert_eq!((state.prepared_bytes, state.active_windows), (0, 0));
}

#[tokio::test]
async fn group_prepare_rolls_back_pinned_exports_when_a_later_export_fails() {
    let fixture = Fixture::new();
    let batch = read_request("group", "fixture-grant");
    let session = fixture.session(&batch).await;
    let request = group_request(session, &batch, &[(0, 2, "window-0"), (2, 2, "window-1")]);
    fixture.provider.fail_prepare_at.store(2, Ordering::SeqCst);
    assert!(
        fixture.group(request.clone()).await.is_err(),
        "a failed export refuses the whole group"
    );
    assert_eq!((fixture.releases(), fixture.quarantines()), (1, 0));
    let state = &fixture.sessions()[0];
    assert_eq!((state.prepared_bytes, state.active_windows), (0, 0));
    fixture
        .provider
        .fail_prepare_at
        .store(usize::MAX, Ordering::SeqCst);
    let retry = fixture
        .group(request)
        .await
        .expect("a rolled-back group can be prepared again");
    assert_eq!(retry.windows.len(), 2);
}

#[tokio::test]
async fn group_prepare_rejects_duplicate_window_keys_before_export() {
    let fixture = Fixture::new();
    let batch = read_request("group-duplicate", "fixture-grant");
    let session = fixture.session(&batch).await;
    let error = fixture
        .group(group_request(
            session,
            &batch,
            &[(0, 2, "same-window"), (2, 2, "same-window")],
        ))
        .await
        .expect_err("duplicate window keys must reject the whole group");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert_eq!((fixture.prepares(), fixture.releases()), (0, 0));
    assert_eq!(fixture.sessions()[0].active_windows, 0);
}

#[tokio::test]
async fn group_prepare_rejects_gaps_lanes_changed_parents_and_out_of_order_windows() {
    let fixture = Fixture::new();
    let batch = read_request("group", "fixture-grant");
    let session = fixture.session(&batch).await;
    let gap = group_request(session, &batch, &[(0, 2, "window-0"), (3, 1, "window-1")]);
    let mut mixed_lanes = group_request(session, &batch, &[(0, 2, "window-0"), (2, 2, "window-1")]);
    mixed_lanes.windows[1].lane_id = 1;
    let mut unconfigured_lane = group_request(session, &batch, &[(0, 2, "window-0")]);
    unconfigured_lane.windows[0].lane_id = 1;
    let mut mismatched = group_request(session, &batch, &[(0, 2, "window-0")]);
    mismatched.windows[0].batch_offset = 2;
    for request in [gap, mixed_lanes, unconfigured_lane, mismatched] {
        assert!(fixture.group(request).await.is_err());
    }
    assert_eq!((fixture.prepares(), fixture.prepared_bytes()), (0, 0));

    // A later wave must continue the admitted parent batch in order.
    let first = fixture
        .prepare(window_request(session, &batch, 0, 1, "window-0"))
        .await
        .unwrap();
    let mut changed = batch.clone();
    changed.operations[0].length = 3;
    for request in [
        window_request(session, &changed, 1, 1, "window-1"),
        window_request(session, &batch, 2, 1, "window-2"),
    ] {
        assert!(fixture.prepare(request).await.is_err());
    }
    assert_eq!(fixture.prepares(), 1);
    fixture.release(release_request(&first)).await.unwrap();
    fixture.close(session).await.unwrap();
    assert_eq!(fixture.prepared_bytes(), 0);
}

#[tokio::test]
async fn group_windows_reuse_aligned_completions_and_recompute_split_ranges() {
    for (aligned, reused, computed) in [(true, 2.0, 0.0), (false, 0.0, 2.0)] {
        let (fixture, registry) = Fixture::with_metrics();
        let mut batch = read_request("group", "fixture-grant");
        if aligned {
            batch.operations[0].length = 2;
            let mut second = batch.operations[0].clone();
            second.operation_index = 1;
            second.chunk_offset = 2;
            second.destination_offset = 2;
            batch.operations.push(second);
        }
        let session = fixture.session(&batch).await;
        let reply = fixture
            .group(group_request(
                session,
                &batch,
                &[(0, 2, "window-0"), (2, 2, "window-1")],
            ))
            .await
            .expect("group preparation");
        let windows = window_replies(&reply);
        assert_checksum(windows[0], b"da");
        assert_checksum(windows[1], b"ta");
        let families: HashMap<_, _> = registry
            .gather()
            .into_iter()
            .map(|family| (family.name().to_owned(), family))
            .collect();
        let counter = |name: &str| families[name].get_metric()[0].get_counter().get_value();
        assert_eq!(
            counter("afs_hixl_source_range_checksums_reused_total"),
            reused
        );
        assert_eq!(
            counter("afs_hixl_source_range_checksums_computed_total"),
            computed
        );
    }
}

#[tokio::test]
async fn logical_batch_snapshot_survives_backing_corruption_only_until_terminal_release() {
    let fixture = Fixture::new();
    let batch = read_request("snapshot", "fixture-grant");
    let session = fixture.session(&batch).await;
    let first = fixture
        .prepare(window_request(session, &batch, 0, 1, "window-0"))
        .await
        .unwrap();
    fixture.release(release_request(&first)).await.unwrap();
    assert_eq!(fixture.prepared_bytes(), 4);
    fixture.corrupt_chunk(&batch);
    // Later waves, including one prepared while an earlier window is still
    // pinned, are served from the already verified immutable snapshot.
    let second = fixture
        .prepare(window_request(session, &batch, 1, 1, "window-1"))
        .await
        .expect("the verified snapshot serves the next wave");
    let third = fixture
        .prepare(window_request(session, &batch, 2, 2, "window-2"))
        .await
        .expect("the verified snapshot serves a wave beside a pinned window");
    assert_checksum(&second, b"a");
    assert_checksum(&third, b"ta");
    assert_eq!(fixture.prepares(), 3);
    for reply in [&second, &third] {
        fixture.release(release_request(reply)).await.unwrap();
    }
    assert_eq!(fixture.prepared_bytes(), 0);
    let next = read_request("new-request", "fixture-grant");
    assert!(
        fixture
            .prepare(prepare_request(session, next))
            .await
            .is_err(),
        "a separate request must verify the backing chunk again"
    );
}

#[tokio::test]
async fn one_release_request_covers_every_prepared_window() {
    let fixture = Fixture::new();
    let (session, first, second) = fixture
        .two_waves(&read_request("batch-release", "fixture-grant"))
        .await;
    let reply = fixture
        .release(release_all(&[&first, &second]))
        .await
        .expect("batched release");
    assert_eq!(reply.windows.len(), 2);
    assert!(reply.windows.iter().all(|window| window.consumed));
    assert_eq!(fixture.releases(), 2);
    let registry = fixture.service.registry.lock().unwrap();
    assert_eq!(registry.prepared_batch_bytes(), 0);
    assert!(registry.sessions[&session].windows.is_empty());
}

#[tokio::test]
async fn release_failure_quarantines_and_preserves_the_unreleased_window() {
    let fixture = Fixture::new();
    let (session, first, second) = fixture
        .two_waves(&read_request("release-failure", "fixture-grant"))
        .await;
    fixture.provider.fail_release_at.store(2, Ordering::SeqCst);
    fixture
        .release(release_all(&[&first, &second]))
        .await
        .expect_err("the second native release fails");
    assert_eq!((fixture.releases(), fixture.quarantines()), (2, 1));
    let registry = fixture.service.registry.lock().unwrap();
    assert!(registry.admission_frozen);
    assert!(registry.runtime_quarantined);
    let session = &registry.sessions[&session];
    assert_eq!(session.windows.len(), 1);
    assert_eq!(session.tombstones.by_key.len(), 1);
    assert!(session.windows.contains_key(&WindowKey {
        read_id: second.read_id.clone(),
        attempt_id: second.attempt_id.clone(),
    }));
}

#[tokio::test]
async fn snapshot_reuse_still_reauthorizes_the_complete_parent_request() {
    let fixture = Fixture::new();
    let batch = read_request("snapshot", "fixture-grant");
    let session = fixture.session(&batch).await;
    fixture
        .prepare(window_request(session, &batch, 0, 2, "window-0"))
        .await
        .unwrap();
    let mut forged = batch;
    forged.operations[0].grant.as_mut().unwrap().token = "revoked".into();
    assert!(
        fixture
            .prepare(window_request(session, &forged, 2, 2, "window-1"))
            .await
            .is_err()
    );
    assert_eq!(fixture.prepares(), 1);
}

#[tokio::test]
async fn expiry_and_drain_reclaim_a_snapshot_after_its_only_export_is_released() {
    for expire in [true, false] {
        let fixture = Fixture::new();
        let batch = read_request("partial-batch", "fixture-grant");
        let session = fixture.session(&batch).await;
        let window = fixture
            .prepare(window_request(session, &batch, 0, 2, "window-0"))
            .await
            .unwrap();
        fixture.release(release_request(&window)).await.unwrap();
        {
            let mut registry = fixture.service.registry.lock().unwrap();
            assert_eq!(registry.prepared_batch_bytes(), 4);
            assert_eq!(registry.active_window_count(), 0);
            if expire {
                registry.sessions.get_mut(&session).unwrap().last_used =
                    Instant::now() - HIXL_SESSION_TTL;
            }
        }
        if expire {
            fixture.service.expire().await.unwrap();
        } else {
            fixture.service.drain(Duration::ZERO).await.unwrap();
        }
        assert_eq!(fixture.prepared_bytes(), 0);
        assert_eq!((fixture.releases(), fixture.quarantines()), (1, 0));
    }
}

#[tokio::test]
async fn multiple_lanes_share_one_snapshot_and_bind_replay_to_its_lane() {
    let mut fake = FakeExportProvider::new();
    let mut second = fake.info.clone();
    second.engine_id = "fake-second-engine".into();
    fake.additional_infos.push(second);
    fake.slot_capacity = 1;
    let fixture = Fixture::with_provider(fake);
    let batch = read_request("two-lanes", "fixture-grant");
    let mut negotiation = negotiate_request(batch.clone());
    negotiation.lane_count = 2;
    let session = fixture.negotiate(negotiation).await.unwrap();
    assert_eq!(session.lanes.len(), 2);
    assert_eq!(session.lanes[1].engine_id, "fake-second-engine");
    let session = session.session_id;
    let first_request = window_request(session, &batch, 0, 2, "window-0");
    let first = fixture.prepare(first_request.clone()).await.unwrap();
    let mut replay_on_another_lane = first_request;
    replay_on_another_lane.lane_id = 1;
    assert!(fixture.prepare(replay_on_another_lane).await.is_err());
    let mut next = window_request(session, &batch, 2, 2, "window-1");
    assert_eq!(
        fixture.prepare(next.clone()).await.unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
    next.lane_id = 1;
    let second = fixture.prepare(next).await.unwrap();
    assert_eq!(first.engine_id, "fake-engine");
    assert_eq!(second.engine_id, "fake-second-engine");
    assert_eq!((first.slot_id, second.slot_id), (0, 0));
    assert_eq!(*fixture.provider.prepared_lanes.lock().unwrap(), vec![0, 1]);
    assert_eq!(fixture.prepared_bytes(), 4);
    fixture.release(release_request(&first)).await.unwrap();
    assert_eq!(fixture.prepared_bytes(), 4);
    fixture.release(release_request(&second)).await.unwrap();
    assert_eq!(fixture.prepared_bytes(), 0);
    assert_eq!(
        *fixture.provider.released_windows.lock().unwrap(),
        vec![(0, 0, 9), (1, 0, 9)]
    );
}

#[tokio::test]
async fn prepared_batch_budget_is_bounded_and_expiry_keeps_native_exports_pinned() {
    let fixture = Fixture::new();
    let staged = StagedChunk::new(
        crate::dfs::OperationId::new("large-fixture"),
        vec![7; 4 * 1024 * 1024],
    );
    let chunk_id = staged.chunk.id.0.clone();
    fixture.local.put(staged).unwrap();
    for index in 0..=16 {
        let mut batch = read_request(&format!("budget-{index}"), "fixture-grant");
        batch.operations[0].chunk_id = chunk_id.clone();
        batch.operations[0].length = 4 * 1024 * 1024;
        let session = fixture.session(&batch).await;
        let result = fixture
            .prepare(window_request(session, &batch, 0, 1, "window-0"))
            .await;
        if index == 16 {
            assert_eq!(result.unwrap_err().code(), tonic::Code::ResourceExhausted);
        } else {
            result.unwrap();
        }
    }
    assert_eq!(fixture.prepares(), 16);
    assert_eq!(fixture.prepared_bytes(), MAX_HIXL_PREPARED_BATCH_BYTES);
    fixture.service.expire_for_test();
    fixture.service.expire().await.unwrap();
    let registry = fixture.service.registry.lock().unwrap();
    assert_eq!(registry.prepared_batch_bytes(), 0);
    assert_eq!(registry.active_window_count(), 16);
    assert_eq!((fixture.releases(), fixture.quarantines()), (0, 1));
}

#[tokio::test]
async fn group_replay_reauthorizes_and_precedes_closed_frozen_draining_and_capacity_checks() {
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
        let mut fake = FakeExportProvider::new();
        if matches!(state, AdmissionState::Capacity) {
            fake.slot_capacity = 2;
        }
        let fixture = Fixture::with_provider(fake);
        let batch = read_request("group-replay", "fixture-grant");
        let session = fixture.session(&batch).await;
        let request = group_request(session, &batch, &[(0, 2, "window-0"), (2, 2, "window-1")]);
        let first = fixture
            .group(request.clone())
            .await
            .expect("initial group prepare");
        match state {
            AdmissionState::Closed => {
                fixture.close(session).await.unwrap();
            }
            AdmissionState::Frozen => {
                fixture.service.registry.lock().unwrap().admission_frozen = true;
            }
            AdmissionState::Draining => {
                fixture.service.registry.lock().unwrap().draining = true;
            }
            AdmissionState::Capacity => {}
        }
        let replay = fixture
            .group(request)
            .await
            .expect("a complete replay bypasses new-window admission checks");
        for (original, replayed) in window_replies(&first)
            .into_iter()
            .zip(window_replies(&replay))
        {
            assert_eq!(
                (original.slot_id, original.slot_generation),
                (replayed.slot_id, replayed.slot_generation)
            );
        }
        assert_eq!(fixture.prepares(), 2);
        // Negotiation, the first prepare and the replay each authorize.
        assert_eq!(fixture.authorizations(), 3);

        let partial = fixture
            .group(group_request(
                session,
                &batch,
                &[(0, 2, "window-0"), (2, 2, "new-window")],
            ))
            .await
            .expect_err("partial replay is invalid before admission checks");
        assert_eq!(partial.code(), tonic::Code::InvalidArgument);

        let expected = match state {
            AdmissionState::Capacity => tonic::Code::ResourceExhausted,
            _ => tonic::Code::Unavailable,
        };
        let new = fixture
            .group(group_request(
                session,
                &batch,
                &[(0, 2, "new-0"), (2, 2, "new-1")],
            ))
            .await
            .expect_err("new windows remain subject to admission checks");
        assert_eq!(new.code(), expected);
        assert_eq!(fixture.prepares(), 2);
    }
}

#[tokio::test]
async fn wrong_peer_epoch_and_grant_are_rejected_before_export() {
    let fixture = Fixture::new();
    let read = read_request("read", "fixture-grant");
    let session = fixture.session(&read).await;
    let local = Some(fixture.local.clone());
    assert!(
        fixture
            .prepare_as("intruder", prepare_request(session, read.clone()), local)
            .await
            .is_err()
    );
    let mut stale = prepare_request(session, read.clone());
    stale.caller_node_epoch = 2;
    assert!(fixture.prepare(stale).await.is_err());
    let mut forged = read;
    forged.operations[0].grant.as_mut().expect("grant").token = "forged".into();
    assert!(
        fixture
            .prepare(prepare_request(session, forged))
            .await
            .is_err()
    );
    assert_eq!(fixture.prepares(), 0);
}

#[tokio::test]
async fn malformed_prepare_is_rejected_before_authorization_or_export() {
    let fixture = Fixture::new();
    let malformed = OneWindow {
        session_id: 1,
        caller_node_epoch: 1,
        source_node_epoch: 9,
        ..OneWindow::default()
    };
    let error = fixture
        .prepare_as("reader", malformed, None)
        .await
        .expect_err("missing read request");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert_eq!((fixture.prepares(), fixture.authorizations()), (0, 0));
}

#[tokio::test]
async fn capacity_close_ttl_and_release_idempotence_keep_unknown_exports_pinned() {
    let fixture = Fixture::new();
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
    let session = fixture.session(&first_read).await;
    let first_request = window_request(session, &batch, 0, 4, "window-0");
    let first = fixture
        .prepare(first_request.clone())
        .await
        .expect("first prepare");
    for index in 1..MAX_HIXL_WINDOWS {
        let offset = u64::try_from(index).unwrap() * 4;
        fixture
            .prepare(window_request(
                session,
                &batch,
                offset,
                4,
                &format!("window-{index}"),
            ))
            .await
            .expect("window within capacity");
    }
    let empty = fixture
        .session(&read_request("empty", "fixture-grant"))
        .await;
    for index in 0..3 {
        fixture
            .service
            .registry
            .lock()
            .unwrap()
            .sessions
            .get_mut(&empty)
            .unwrap()
            .last_used = Instant::now() - (HIXL_SESSION_TTL - Duration::from_secs(1));
        let active_since = Instant::now();
        let refused = fixture
            .prepare(prepare_request(
                empty,
                read_request(&format!("refused-{index}"), "fixture-grant"),
            ))
            .await
            .unwrap_err();
        assert_eq!(refused.code(), tonic::Code::ResourceExhausted);
        let registry = fixture.service.registry.lock().unwrap();
        let active = &registry.sessions[&empty];
        assert!(
            active.last_used >= active_since,
            "authenticated capacity refusal must refresh an active empty control session"
        );
        assert!(active.windows.is_empty());
    }
    let capacity = fixture
        .prepare(prepare_request(
            session,
            read_request("read-capacity", "fixture-grant"),
        ))
        .await
        .expect_err("capacity must be bounded");
    assert_eq!(capacity.code(), tonic::Code::ResourceExhausted);
    let mut unknown = release_request(&first);
    unknown.windows[0].outcome = DfsHixlReleaseOutcome::Unknown as i32;
    let unknown_reply = fixture.release(unknown).await.expect("unknown release");
    assert!(!unknown_reply.windows[0].consumed);
    assert_eq!(fixture.releases(), 0);
    let frozen = fixture
        .negotiate(negotiate_request(read_request(
            "after-unknown",
            "fixture-grant",
        )))
        .await
        .expect_err("unknown native outcome must freeze new HIXL admission");
    assert_eq!(frozen.code(), tonic::Code::Unavailable);

    fixture.service.expire_for_test();
    let closed = fixture.close(session).await.expect("close");
    assert!(!closed.drained);
    assert_eq!(fixture.quarantines(), 1);
    let replay_after_close = fixture
        .prepare(first_request)
        .await
        .expect("closed session must still replay an admitted window");
    assert_eq!(replay_after_close.slot_id, first.slot_id);
    let rejected = fixture
        .prepare(prepare_request(
            session,
            read_request("after-close", "fixture-grant"),
        ))
        .await
        .expect_err("closed session must reject new windows");
    assert_eq!(rejected.code(), tonic::Code::Unavailable);

    let released = fixture
        .release(release_request(&first))
        .await
        .expect("release");
    assert!(released.windows[0].consumed);
    let replay = fixture
        .release(release_request(&first))
        .await
        .expect("idempotent release");
    assert!(replay.windows[0].already_released);
    let mut mismatched = release_request(&first);
    mismatched.windows[0].content_validated = false;
    let mismatch = fixture
        .release(mismatched)
        .await
        .expect_err("release replay identity must include validation result");
    assert_eq!(mismatch.code(), tonic::Code::InvalidArgument);
    assert_eq!(fixture.releases(), 1);

    for index in 1..=MAX_HIXL_WINDOWS {
        let read = read_request(&format!("after-close-{index}"), "fixture-grant");
        let result = fixture.prepare(prepare_request(session, read)).await;
        assert!(result.is_err(), "closed session must stay closed");
    }
}

#[tokio::test]
async fn failed_expiry_quarantine_keeps_admission_frozen_until_a_retry_succeeds() {
    let (fixture, metrics) = Fixture::with_metrics();
    fixture.admitted().await;
    fixture.service.expire_for_test();
    fixture.provider.fail_quarantines.store(1, Ordering::SeqCst);
    assert!(fixture.service.expire().await.is_err());
    {
        let registry = fixture.service.registry.lock().unwrap();
        assert!(registry.admission_frozen);
        assert!(!registry.runtime_quarantined);
    }
    // A failed quarantine must not reopen admission for any session.
    let refused = fixture
        .negotiate(negotiate_request(read_request("next", "fixture-grant")))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::Unavailable);
    fixture.service.expire().await.unwrap();
    let registry = fixture.service.registry.lock().unwrap();
    assert!(registry.admission_frozen && registry.runtime_quarantined);
    assert_eq!((fixture.releases(), fixture.quarantines()), (0, 2));
    // Each periodic pass is observable even when the runtime never reached
    // the quarantined state that `/health` reports.
    let expiries = |result: &str| {
        metrics
            .gather()
            .into_iter()
            .filter(|family| family.name() == "afs_hixl_operation_duration_seconds")
            .flat_map(|family| family.get_metric().to_vec())
            .filter(|metric| {
                let labels = metric
                    .get_label()
                    .iter()
                    .map(|label| (label.name(), label.value()))
                    .collect::<Vec<_>>();
                labels.contains(&("phase", "expire")) && labels.contains(&("result", result))
            })
            .map(|metric| metric.get_histogram().get_sample_count())
            .sum::<u64>()
    };
    assert_eq!((expiries("error"), expiries("ok")), (1, 1));
}

#[tokio::test]
async fn failed_forced_quarantine_is_retried_by_the_next_expiry() {
    let fixture = Fixture::new();
    let window = fixture.admitted().await;
    fixture.provider.fail_quarantines.store(1, Ordering::SeqCst);
    let mut release = release_request(&window);
    release.windows[0].outcome = DfsHixlReleaseOutcome::Unknown as i32;
    release.windows[0].content_validated = false;
    assert!(fixture.release(release).await.is_err());
    // The session stays open, so only the failed quarantine itself can
    // bring the next pass back to the runtime.
    fixture.service.expire().await.unwrap();
    let registry = fixture.service.registry.lock().unwrap();
    assert!(registry.admission_frozen && registry.runtime_quarantined);
    assert_eq!((fixture.releases(), fixture.quarantines()), (0, 2));
}

#[tokio::test]
async fn empty_close_remains_idempotent_across_periodic_expiration() {
    let fixture = Fixture::new();
    let session = fixture
        .session(&read_request("close-read", "fixture-grant"))
        .await;
    assert!(fixture.close(session).await.unwrap().drained);
    fixture.service.expire().await.unwrap();
    assert!(fixture.close(session).await.unwrap().drained);
}

#[tokio::test]
async fn a_small_group_may_carry_more_operations_than_a_legacy_batch() {
    let fixture = Fixture::new();
    let mut batch = read_request("many-ops", "fixture-grant");
    let template = batch.operations[0].clone();
    // 129 one-byte operations: well under 8 MiB, over the legacy op limit.
    batch.operations = (0..=crate::node::dfs_read::MAX_DFS_READ_OPS)
        .map(|index| DfsChunkReadOp {
            operation_index: index as u32,
            chunk_offset: (index % 4) as u64,
            length: 1,
            destination_offset: index as u64,
            ..template.clone()
        })
        .collect();
    let session = fixture.session(&batch).await;
    let length = batch.operations.len() as u64;
    let mut request = group_request(session, &batch, &[]);
    request.windows = vec![DfsHixlGroupWindow {
        batch_offset: 0,
        lane_id: 0,
        read_request: Some(
            crate::node::rpc::hixl::batch::slice_read_group(&batch, 0, length, "window-0").unwrap(),
        ),
    }];
    let reply = fixture
        .group(request)
        .await
        .expect("a small group with many operations is admitted");
    assert_eq!(window_replies(&reply)[0].packed_length, length);
}

#[tokio::test]
async fn source_reads_run_outside_the_registry_lock_and_hold_a_reservation() {
    let fixture = Fixture::new();
    let batch = read_request("unlocked", "fixture-grant");
    let session = fixture.session(&batch).await;
    let registry = fixture.service.registry.clone();
    let (entered, entering) = std::sync::mpsc::channel();
    let (resume, resuming) = std::sync::mpsc::channel::<()>();
    *fixture.service.before_source_read.lock().unwrap() = Some(Box::new(move || {
        entered.send(()).unwrap();
        resuming.recv().unwrap();
    }));
    let service = fixture.service.clone();
    let authorizer = fixture.authorizer.clone();
    let local = fixture.local.clone();
    let request = group_request(session, &batch, &[(0, 4, "window-0")]);
    let prepare = tokio::spawn(async move {
        service
            .prepare_group("reader".into(), request, authorizer, Some(local))
            .await
    });
    tokio::task::spawn_blocking(move || entering.recv().unwrap())
        .await
        .unwrap();
    {
        let registry = registry
            .try_lock()
            .expect("the source read must not hold the registry lock");
        assert_eq!(
            registry.active_window_count(),
            1,
            "the reserved export counts against capacity while unlocked"
        );
        assert_eq!(registry.reserved_snapshot_bytes, 4);
    }
    resume.send(()).unwrap();
    prepare.await.unwrap().unwrap();
    let registry = registry.lock().unwrap();
    assert_eq!(registry.reserved_snapshot_bytes, 0);
    assert!(registry.reserved_windows.is_empty());
    assert_eq!(registry.active_window_count(), 1);
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
    let closed_session = |windows| Session {
        peer: "reader".into(),
        caller_epoch: 1,
        source_epoch: 9,
        engine_id: "engine".into(),
        engine_generation: 1,
        lanes: Vec::new(),
        closed: true,
        last_used: expired_at,
        windows,
        tombstones: Tombstones::default(),
        prepared_batch: None,
    };
    let pending = WindowRecord {
        request: DfsReadRangesRequest::default(),
        reply: DfsPrepareReadRangesHixlReply::default(),
        batch_request: Arc::new(DfsReadRangesRequest::default()),
        batch_offset: 0,
        lane_id: 0,
    };
    let pending_key = WindowKey {
        read_id: "pending".into(),
        attempt_id: "attempt".into(),
    };
    let mut registry = Registry::default();
    registry.sessions.insert(1, closed_session(HashMap::new()));
    registry
        .sessions
        .insert(2, closed_session(HashMap::from([(pending_key, pending)])));

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
