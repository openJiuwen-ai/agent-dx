//! Loopback tests: a real OwnerFiles tonic server with the Home window service
//! over in-memory export slots, and an `OwnerPeerClient` whose native reader
//! pulls those slots from the same memory.

use super::super::batch;
use super::super::*;
use super::*;
use crate::node::data_plane::hixl::ReadLane;
use crate::node::rpc::data::owner_hixl::OwnerHixlService;
use crate::node::rpc::data::owner_hixl::test_support::{
    HOME_ENGINE, MemoryExports, MemorySource, OPEN_HANDLE, tree_path,
};
use crate::node::rpc::data::{OwnerFilesHandler, OwnerFilesService, PeerAuthenticator};
use crate::node::rpc::hixl::test_support::{engine, native_error, negotiations, operations};
use crate::node::rpc::peer::{
    OwnerPeerClient, owner_files_client_from_channel_with_runtime_and_metrics,
};
use crate::node::vfs::ownerfs::files::FileIdentity;
use crate::node::vfs::ownerfs::remote::RemoteFiles;
use crate::node::vfs::ownerfs::root::{RootId, RootRight};
use afs_protocol::node_data::{
    OwnerFileAttr, OwnerFileKind, OwnerOpenReply, OwnerOpenRequest, OwnerReadReply,
    OwnerReadRequest, owner_files_server::OwnerFilesServer,
};
use afs_transport::hixl::{
    EngineInfo, HixlError, HixlErrorKind, HixlPhase, HixlReadOutcome, HixlReadTiming,
};
use std::ffi::OsStr;
use std::sync::atomic::AtomicUsize;
use tokio_stream::wrappers::TcpListenerStream;

const SLOT: usize = 4096;
const FILE_BYTES: usize = 50_000;

struct MemoryReader {
    exports: Arc<MemoryExports>,
    info: EngineInfo,
    peer_engine: &'static str,
    batches: AtomicUsize,
    corrupt: AtomicBool,
    /// While set, `connect` waits, like a native connect still in progress.
    hold_connect: AtomicBool,
    connects: AtomicUsize,
    hold_read: AtomicBool,
    hold_read_at: AtomicUsize,
    reads_started: AtomicUsize,
    unknown_read: AtomicBool,
    quarantines: AtomicUsize,
}

impl ReadLane for MemoryReader {
    fn connect(&self, peer: &str, _deadline: Instant) -> Result<(), HixlError> {
        assert_eq!(peer, self.peer_engine);
        while self.hold_connect.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(1));
        }
        self.connects.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn prepare_read_batch_buffer(&self, length: usize) -> Result<Vec<u8>, HixlError> {
        Ok(Vec::with_capacity(length))
    }

    fn read_batch_timed_into(
        &self,
        peer: &str,
        windows: &[(u64, usize)],
        mut output: Vec<u8>,
        _deadline: Instant,
    ) -> Result<HixlReadOutcome, HixlError> {
        assert_eq!(peer, self.peer_engine);
        let ordinal = self.reads_started.fetch_add(1, Ordering::SeqCst) + 1;
        while self.hold_read.load(Ordering::SeqCst)
            || self.hold_read_at.load(Ordering::SeqCst) == ordinal
        {
            std::thread::sleep(Duration::from_millis(1));
        }
        if self.unknown_read.load(Ordering::SeqCst) {
            return Err(HixlError {
                read_may_be_in_flight: true,
                ..native_error(
                    HixlPhase::PollRead,
                    HixlErrorKind::DeadlineExceeded,
                    "DMA outcome unknown",
                )
            });
        }
        assert!(output.is_empty());
        for (address, length) in windows {
            if *length > self.info.window_bytes {
                return Err(native_error(
                    HixlPhase::SubmitRead,
                    HixlErrorKind::InvalidArgument,
                    "window exceeds the reader slot",
                ));
            }
            let bytes = self.exports.read(*address, *length).ok_or_else(|| {
                native_error(
                    HixlPhase::SubmitRead,
                    HixlErrorKind::InvalidArgument,
                    "window is not exported",
                )
            })?;
            output.extend_from_slice(&bytes);
        }
        if self.corrupt.load(Ordering::SeqCst) {
            output[0] ^= 0xff;
        }
        self.batches.fetch_add(1, Ordering::SeqCst);
        Ok(HixlReadOutcome {
            bytes: output,
            timing: HixlReadTiming::default(),
        })
    }
}

struct MemoryRuntime(Arc<MemoryReader>);

impl ReadRuntime for MemoryRuntime {
    fn info(&self) -> EngineInfo {
        self.0.info.clone()
    }

    fn infos(&self) -> Vec<EngineInfo> {
        vec![self.0.info.clone()]
    }

    fn lane_count(&self) -> usize {
        1
    }

    fn lane(&self, lane: usize) -> Option<Arc<dyn ReadLane>> {
        (lane == 0).then(|| self.0.clone() as Arc<dyn ReadLane>)
    }

    fn quarantine(&self) -> Result<(), HixlError> {
        self.0.quarantines.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct AllowPeer;

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

/// gRPC OPEN and the original single-range READ over the same file.
struct FileHandler {
    source: Arc<MemorySource>,
    grpc_reads: AtomicUsize,
    opens: AtomicUsize,
    disable_prefetch: AtomicBool,
}

impl OwnerFilesHandler for FileHandler {
    fn open(&self, peer: &str, request: OwnerOpenRequest) -> afs_error::Result<OwnerOpenReply> {
        assert_eq!(peer, "node-b");
        // A path of the tree opens that file; any other path the single file.
        let tree = self.source.tree.get(&request.path);
        let (size, identity, mtime_ns) = match &tree {
            Some(entry) => (
                entry.data.len() as u64,
                entry.identity.clone(),
                entry.mtime_ns,
            ),
            None => (
                self.source.data.lock().unwrap().len() as u64,
                b"identity-a".to_vec(),
                0,
            ),
        };
        let identity = afs_protocol::node_data::FileIdentity { opaque: identity };
        // Every OPEN is a distinct Home handle, like the real executor.
        self.disable_prefetch
            .store(request.disable_prefetch, Ordering::SeqCst);
        let open = self.opens.fetch_add(1, Ordering::SeqCst);
        let mut handle = OPEN_HANDLE.to_vec();
        handle.extend_from_slice(format!("-{open}").as_bytes());
        if tree.is_some() {
            handle.push(b'#');
            handle.extend_from_slice(&request.path);
        }
        Ok(OwnerOpenReply {
            handle: Some(OwnerHandle { opaque: handle }),
            file_identity: Some(identity.clone()),
            owner_session_id: "home-session-a".to_owned(),
            attr: Some(OwnerFileAttr {
                identity: Some(identity),
                kind: OwnerFileKind::Regular as i32,
                mode: 0o100_644,
                size,
                nlink: 1,
                mtime_ns,
                ctime_ns: mtime_ns,
                ..Default::default()
            }),
            prefetched_data: None,
        })
    }

    fn read(&self, peer: &str, request: OwnerReadRequest) -> afs_error::Result<OwnerReadReply> {
        assert_eq!(peer, "node-b");
        self.grpc_reads.fetch_add(1, Ordering::SeqCst);
        let handle = request
            .handle
            .as_ref()
            .map_or(&[][..], |handle| &handle.opaque);
        let data = match tree_path(handle) {
            Some(path) => self.source.tree.get(path).unwrap().data,
            None => self.source.data.lock().unwrap().clone(),
        };
        let start = (request.offset as usize).min(data.len());
        let end = (start + request.length as usize).min(data.len());
        Ok(OwnerReadReply {
            data: data[start..end].to_vec().into(),
            read: (end - start) as u32,
            eof: end - start < request.length as usize,
            data_checksum: Vec::new(),
        })
    }
}

struct Fixture {
    client: Arc<OwnerPeerClient>,
    exports: Arc<MemoryExports>,
    source: Arc<MemorySource>,
    native: Arc<MemoryReader>,
    reader: Arc<OwnerHixlReader>,
    handler: Arc<FileHandler>,
    /// The Home window service; attached to the server only with `home_hixl`.
    home: Arc<OwnerHixlService>,
    registry: afs_metrics::Registry,
    _server: tokio::task::JoinHandle<()>,
}

impl Fixture {
    fn reads(&self, outcome: &str) -> u64 {
        self.registry
            .gather()
            .into_iter()
            .find(|family| family.name() == "afs_ownerfs_hixl_reads_total")
            .and_then(|family| {
                family
                    .get_metric()
                    .iter()
                    .find(|metric| {
                        metric
                            .get_label()
                            .iter()
                            .any(|label| label.name() == "outcome" && label.value() == outcome)
                    })
                    .map(|metric| metric.get_counter().get_value() as u64)
            })
            .unwrap_or(0)
    }

    /// Negotiations this reader sent, successful or not.
    fn negotiations(&self) -> u64 {
        negotiations(&self.registry)
    }

    fn connects(&self) -> usize {
        self.native.connects.load(Ordering::SeqCst)
    }

    async fn wait_until(&self, what: &str, done: impl Fn(&Self) -> bool) {
        for _ in 0..500 {
            if done(self) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting until {what}");
    }

    async fn wait_released(&self) {
        for _ in 0..200 {
            if self.exports.active() == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("HIXL slots were not released");
    }
}

fn policy(mode: DataMode) -> OwnerHixlPolicy {
    OwnerHixlPolicy {
        mode,
        max_group_bytes: 4 * SLOT,
        max_inflight: 1,
        host_budget_bytes: 1024 * 1024,
        min_file_bytes: 1,
        receive_slots: 16,
        sessions_per_home: 1,
        session_idle: Duration::from_secs(20),
        batch_max_file_bytes: 0,
    }
}

fn grant() -> RootGrant {
    RootGrant {
        id: RootId("root-a".to_owned()),
        epoch: 1,
        home_node_id: "node-a".to_owned(),
        home_session_id: "home-session-a".to_owned(),
        holder_node_id: "node-b".to_owned(),
        session_id: "grant-session-b".to_owned(),
        access_generation: 1,
        rights: vec![RootRight::Read],
        fencing_token: "fence-a".to_owned(),
    }
}

fn content() -> Vec<u8> {
    (0..FILE_BYTES).map(|index| (index % 251) as u8).collect()
}

async fn fixture(policy: OwnerHixlPolicy, home_hixl: bool) -> Fixture {
    fixture_with(policy, home_hixl, MemorySource::new(content(), 3000)).await
}

async fn fixture_with(policy: OwnerHixlPolicy, home_hixl: bool, source: MemorySource) -> Fixture {
    fixture_windows(policy, home_hixl, source, SLOT, SLOT).await
}

/// A reader whose native window is `reader_slot` bytes against a Home
/// window of `home_slot` bytes.
async fn fixture_windows(
    policy: OwnerHixlPolicy,
    home_hixl: bool,
    source: MemorySource,
    home_slot: usize,
    reader_slot: usize,
) -> Fixture {
    fixture_parking(
        policy,
        home_hixl,
        source,
        home_slot,
        reader_slot,
        RELEASE_PARK,
    )
    .await
}

fn memory_reader(
    exports: Arc<MemoryExports>,
    info: EngineInfo,
    peer_engine: &'static str,
) -> Arc<MemoryReader> {
    Arc::new(MemoryReader {
        exports,
        info,
        peer_engine,
        batches: AtomicUsize::new(0),
        corrupt: AtomicBool::new(false),
        hold_connect: AtomicBool::new(false),
        connects: AtomicUsize::new(0),
        hold_read: AtomicBool::new(false),
        hold_read_at: AtomicUsize::new(0),
        reads_started: AtomicUsize::new(0),
        unknown_read: AtomicBool::new(false),
        quarantines: AtomicUsize::new(0),
    })
}

/// A reader that flushes a parked release after `park` without a prepare.
async fn fixture_parking(
    policy: OwnerHixlPolicy,
    home_hixl: bool,
    source: MemorySource,
    home_slot: usize,
    reader_slot: usize,
    park: Duration,
) -> Fixture {
    let exports = Arc::new(MemoryExports::new(home_slot, 8));
    fixture_with_exports(policy, home_hixl, source, exports, reader_slot, park).await
}

async fn fixture_with_exports(
    policy: OwnerHixlPolicy,
    home_hixl: bool,
    source: MemorySource,
    exports: Arc<MemoryExports>,
    reader_slot: usize,
    park: Duration,
) -> Fixture {
    let native = memory_reader(
        exports.clone(),
        engine("127.0.0.1:7940", 3, reader_slot),
        HOME_ENGINE,
    );
    let source = Arc::new(source);
    let handler = Arc::new(FileHandler {
        source: source.clone(),
        grpc_reads: AtomicUsize::new(0),
        opens: AtomicUsize::new(0),
        disable_prefetch: AtomicBool::new(false),
    });
    let mut service = OwnerFilesService::new(handler.clone(), Arc::new(AllowPeer));
    let home = Arc::new(OwnerHixlService::from_parts(
        exports.clone(),
        source.clone(),
    ));
    if home_hixl {
        service = service.with_hixl(home.clone());
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(OwnerFilesServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let registry = afs_metrics::Registry::new();
    let reader = Arc::new(
        OwnerHixlReader::from_runtime(
            Arc::new(MemoryRuntime(native.clone())),
            "node-b".to_owned(),
            policy,
            Duration::from_secs(5),
        )
        .unwrap()
        .with_release_park(park)
        .with_metrics(HixlMetrics::register(&registry).unwrap()),
    );
    let client = owner_files_client_from_channel_with_runtime_and_metrics(
        channel,
        tokio::runtime::Handle::current(),
        None,
    )
    .with_data_transport(policy.mode, None, Duration::from_secs(5))
    .with_hixl(reader.clone(), "node-a".to_owned());
    Fixture {
        client: Arc::new(client),
        exports,
        source,
        native,
        reader,
        handler,
        home,
        registry,
        _server: server,
    }
}

/// Runs synchronous OwnerFiles calls off the async runtime, like FUSE workers.
async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(work).await.unwrap()
}

fn open(client: &OwnerPeerClient, flags: i32) -> RemoteFile {
    client
        .open(&grant(), OsStr::new("file"), flags, None)
        .unwrap()
        .0
}

fn read(
    client: &OwnerPeerClient,
    file: &RemoteFile,
    offset: u64,
    length: usize,
) -> afs_error::Result<Vec<u8>> {
    let mut out = vec![0; length];
    let read = client.read(&grant(), file, offset, &mut out)?;
    out.truncate(read);
    Ok(out)
}

/// Reads the whole file in 1000-byte READs, which straddle every window edge.
fn scan(client: &OwnerPeerClient, file: &RemoteFile) -> Vec<u8> {
    let mut all = Vec::new();
    loop {
        let chunk = read(client, file, all.len() as u64, 1000).unwrap();
        if chunk.is_empty() {
            return all;
        }
        all.extend(chunk);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequential_reads_fill_growing_windows_and_stop_at_eof() {
    let fixture = fixture(policy(DataMode::Hixl), true).await;
    let client = fixture.client.clone();
    let (all, tail) = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        let all = scan(&client, &file);
        // Inside the final EOF window: a short count, no new prepare.
        let tail = read(&client, &file, 49_000, 2000).unwrap();
        (all, tail)
    })
    .await;
    assert_eq!(all, content());
    assert_eq!(tail, content()[49_000..]);
    // Windows of 4, 8, 16, 16 and a final 16 KiB request ending at EOF. That
    // request pins four slots before the read and publishes two of them.
    assert_eq!(fixture.native.batches.load(Ordering::SeqCst), 5);
    assert_eq!(fixture.exports.exports.load(Ordering::SeqCst), 15);
    assert_eq!(fixture.handler.grpc_reads.load(Ordering::SeqCst), 0);
    assert!(fixture.reads("window_hit") > 40);
    fixture.wait_released().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_uses_hixl_for_an_eligible_open_from_the_first_read() {
    let fixture = fixture(policy(DataMode::Auto), true).await;
    let client = fixture.client.clone();
    let reads = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        // Random reads use HIXL; the streak still controls read-ahead.
        [20_000, 21_000, 22_000].map(|offset| read(&client, &file, offset, 1000).unwrap())
    })
    .await;
    for (index, data) in reads.iter().enumerate() {
        let offset = 20_000 + index * 1000;
        assert_eq!(*data, content()[offset..offset + 1000]);
    }
    assert_eq!(fixture.handler.grpc_reads.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.native.batches.load(Ordering::SeqCst), 3);
    assert_eq!(fixture.negotiations(), 1);
    assert_eq!(fixture.reads("fallback"), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn required_offset_zero_and_window_continuations_need_no_streak() {
    let fixture = fixture(policy(DataMode::Hixl), true).await;
    let client = fixture.client.clone();
    blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        // Offset 0 opens a window at once; the jump fetches only its length.
        assert_eq!(read(&client, &file, 0, 1000).unwrap(), content()[..1000]);
        assert_eq!(
            read(&client, &file, 30_000, 1000).unwrap(),
            content()[30_000..31_000]
        );
    })
    .await;
    assert_eq!(fixture.native.batches.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.handler.grpc_reads.load(Ordering::SeqCst), 0);
}

/// Two opens of one Home prepare concurrently only with a session pool.
async fn peak_home_prepares(sessions_per_home: usize) -> usize {
    let mut pooled = policy(DataMode::Hixl);
    pooled.sessions_per_home = sessions_per_home;
    let source = MemorySource::new(content(), 3000).with_overlap_probe(2);
    let fixture = fixture_with(pooled, true, source).await;
    let readers = (0..2).map(|_| {
        let client = fixture.client.clone();
        tokio::task::spawn_blocking(move || {
            let file = open(&client, libc::O_RDONLY);
            read(&client, &file, 0, 1000).unwrap()
        })
    });
    for reader in readers.collect::<Vec<_>>() {
        assert_eq!(reader.await.unwrap(), content()[..1000]);
    }
    fixture.wait_released().await;
    fixture.source.peak_overlap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_session_pool_overlaps_window_prepares_on_one_home() {
    assert_eq!(peak_home_prepares(1).await, 1);
    assert_eq!(peak_home_prepares(2).await, 2);
}

#[test]
fn pooled_sessions_split_the_remote_export_slots() {
    assert_eq!(window_slots(16, 16, 1), 16);
    assert_eq!(window_slots(16, 16, 4), 4);
    assert_eq!(window_slots(8, 16, 2), 4);
    assert_eq!(window_slots(16, 4, 2), 4);
    assert_eq!(window_slots(2, 16, 4), 0);
    assert_eq!(window_slots(128, 128, 4), 32);
    assert_eq!(window_slots(128, 128, 1), 64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_sessions_can_prepare_a_128_mib_group_from_a_128_slot_pool() {
    const WINDOW: usize = 4 * 1024 * 1024;
    const GROUP: usize = 32 * WINDOW;
    let mut wide = policy(DataMode::Hixl);
    wide.max_group_bytes = GROUP;
    wide.receive_slots = 128;
    wide.sessions_per_home = 4;
    let fixture = fixture_with_exports(
        wide,
        true,
        MemorySource::new(content(), 3000),
        Arc::new(MemoryExports::new(WINDOW, 128)),
        WINDOW,
        RELEASE_PARK,
    )
    .await;
    let client = fixture.client.clone();
    let file = blocking(move || open(&client, libc::O_RDONLY)).await;
    let request = FetchRequest {
        home: "node-a".to_owned(),
        client: fixture.client.cloned_client().unwrap(),
        access: root_access(&grant()),
        handle: file.handle,
        offset: 0,
        size: GROUP,
        deadline: Instant::now() + Duration::from_secs(5),
    };
    let result = fixture.reader.clone().fetch(request, None).await.unwrap();
    let Fetched::Window(window) = result else {
        panic!("the negotiated HIXL fetch should return a window");
    };
    assert_eq!(window.bytes.as_ref(), content());
    // Home reserves the complete Prepare range before it discovers EOF.
    assert_eq!(fixture.exports.exports.load(Ordering::SeqCst), 32);
    fixture.wait_released().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn small_and_writable_opens_keep_the_grpc_path() {
    let mut small = policy(DataMode::Auto);
    small.min_file_bytes = FILE_BYTES as u64 + 1;
    let fixture = fixture(small, true).await;
    let client = fixture.client.clone();
    blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        assert_eq!(read(&client, &file, 0, 1000).unwrap(), content()[..1000]);
        let file = open(&client, libc::O_RDWR);
        assert_eq!(read(&client, &file, 0, 1000).unwrap(), content()[..1000]);
    })
    .await;
    assert_eq!(fixture.handler.grpc_reads.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.native.batches.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn home_without_capability_falls_back_only_in_auto_mode() {
    let fixture = fixture(policy(DataMode::Auto), false).await;
    // A warm first asks the Home once; it must not make the reads retry or
    // open a native connection that the Home cannot serve.
    let client = fixture.client.clone();
    blocking(move || client.warm(&grant())).await;
    fixture
        .wait_until("the warm negotiation finished", |fixture| {
            fixture.negotiations() == 1
        })
        .await;
    let client = fixture.client.clone();
    let data = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        let first = read(&client, &file, 0, 1000).unwrap();
        let second = read(&client, &file, 1000, 1000).unwrap();
        [first, second].concat()
    })
    .await;
    assert_eq!(data, content()[..2000]);
    assert_eq!(fixture.handler.grpc_reads.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.negotiations(), 1, "reads do not ask the Home again");
    assert_eq!(fixture.connects(), 0);

    let fixture = self::fixture(policy(DataMode::Hixl), false).await;
    let client = fixture.client.clone();
    let error = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        read(&client, &file, 0, 1000).unwrap_err()
    })
    .await;
    assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNSUPPORTED);
    assert_eq!(fixture.handler.grpc_reads.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn draining_home_returns_an_error_in_both_modes() {
    for mode in [DataMode::Auto, DataMode::Hixl] {
        let fixture = fixture(policy(mode), true).await;
        fixture.home.drain(Duration::ZERO).await.unwrap();
        let client = fixture.client.clone();
        let error = blocking(move || {
            let file = open(&client, libc::O_RDONLY);
            read(&client, &file, 0, 1000).unwrap_err()
        })
        .await;
        assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNAVAILABLE);
        assert_eq!(fixture.handler.grpc_reads.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.native.batches.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refused_prepare_propagates_even_after_a_partial_window_hit() {
    let fixture = fixture(policy(DataMode::Auto), true).await;
    let client = fixture.client.clone();
    let file = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        assert_eq!(read(&client, &file, 0, 1000).unwrap(), content()[..1000]);
        file
    })
    .await;
    fixture.wait_released().await;
    fixture.home.drain(Duration::ZERO).await.unwrap();
    let client = fixture.client.clone();
    let error = blocking(move || read(&client, &file, 4000, 1000).unwrap_err()).await;
    assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNAVAILABLE);
    assert_eq!(fixture.native.batches.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.handler.grpc_reads.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.exports.active(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_fills_only_the_tail_after_an_unsupported_prepare() {
    for reply in [false, true] {
        let fixture = fixture(policy(DataMode::Auto), true).await;
        fixture
            .exports
            .fail_export_unsupported_at
            .store(2, Ordering::SeqCst);
        let client = fixture.client.clone();
        let data = blocking(move || {
            let file = open(&client, libc::O_RDONLY);
            assert_eq!(read(&client, &file, 0, 1000).unwrap(), content()[..1000]);
            if reply {
                client
                    .read_reply(&grant(), &file, 4000, 1000)
                    .unwrap()
                    .to_vec()
            } else {
                read(&client, &file, 4000, 1000).unwrap()
            }
        })
        .await;
        assert_eq!(data, content()[4000..5000]);
        assert_eq!(fixture.native.batches.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.handler.grpc_reads.load(Ordering::SeqCst), 1);
        fixture.wait_released().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_quarantines_unknown_completion_without_retrying_over_grpc() {
    let fixture = fixture(policy(DataMode::Auto), true).await;
    fixture.native.unknown_read.store(true, Ordering::SeqCst);
    let client = fixture.client.clone();
    let error = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        read(&client, &file, 0, 1000).unwrap_err()
    })
    .await;
    assert_eq!(error.code(), afs_error::IO_TIMEOUT);
    fixture
        .wait_until("Home quarantine", |fixture| {
            fixture.exports.quarantines.load(Ordering::SeqCst) > 0
        })
        .await;
    assert!(fixture.exports.active() > 0);
    assert_eq!(fixture.native.quarantines.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.handler.grpc_reads.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_hixl_disables_open_payload_and_rejects_writable_and_direct_opens() {
    let fixture = fixture(policy(DataMode::Hixl), true).await;
    let client = fixture.client.clone();
    blocking(move || {
        for flags in [
            libc::O_RDWR,
            libc::O_WRONLY,
            libc::O_RDONLY | libc::O_TRUNC,
            libc::O_RDONLY | libc::O_DIRECT,
        ] {
            let error = client
                .open(&grant(), OsStr::new("file"), flags, None)
                .expect_err("explicit HIXL must reject this open before Home RPC");
            assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNSUPPORTED);
        }
        let file = open(&client, libc::O_RDONLY);
        assert_eq!(read(&client, &file, 0, 1000).unwrap(), content()[..1000]);
    })
    .await;
    assert_eq!(fixture.handler.opens.load(Ordering::SeqCst), 1);
    assert!(fixture.handler.disable_prefetch.load(Ordering::SeqCst));
    assert_eq!(fixture.handler.grpc_reads.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_empty_hixl_read_does_not_prepare_a_directory_batch() {
    let fixture = batch_fixture(batch_policy(DataMode::Hixl), true).await;
    let client = fixture.client.clone();
    blocking(move || {
        let file = open_path(&client, "d/f00");
        let bytes = client.read_reply(&grant(), &file, 0, 0).unwrap();
        assert!(bytes.is_empty());
    })
    .await;
    assert_eq!(fixture.native.reads_started.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.handler.grpc_reads.load(Ordering::SeqCst), 1);
}

async fn parking_fixture(park: Duration) -> Fixture {
    let source = MemorySource::new(content(), 3000);
    fixture_parking(policy(DataMode::Hixl), true, source, SLOT, SLOT, park).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_window_release_rides_on_the_next_prepare_of_its_session() {
    let fixture = parking_fixture(Duration::from_secs(30)).await;
    let client = fixture.client.clone();
    let all = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        scan(&client, &file)
    })
    .await;
    assert_eq!(all, content());
    assert!(operations(&fixture.registry, "prepare") >= 3);
    assert_eq!(operations(&fixture.registry, "release"), 0);
    assert_eq!(fixture.exports.quarantines.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_parked_release_is_flushed_on_its_own() {
    let fixture = parking_fixture(Duration::from_millis(300)).await;
    let client = fixture.client.clone();
    let first = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        read(&client, &file, 0, 1000).unwrap()
    })
    .await;
    assert_eq!(first, content()[..1000]);
    assert!(fixture.exports.active() > 0);
    assert_eq!(operations(&fixture.registry, "release"), 0);
    fixture.wait_released().await;
    fixture
        .wait_until("the flush is observed", |fixture| {
            operations(&fixture.registry, "release") == 1
        })
        .await;
    assert_eq!(operations(&fixture.registry, "prepare"), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_session_sends_its_parked_release_before_closing() {
    let fixture = parking_fixture(Duration::from_secs(30)).await;
    let client = fixture.client.clone();
    let file = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        assert_eq!(read(&client, &file, 0, 1000).unwrap(), content()[..1000]);
        file
    })
    .await;
    assert!(fixture.exports.active() > 0);
    let pool = fixture.reader.home_pool("node-a").unwrap();
    pool.slots[0]
        .try_lock()
        .unwrap()
        .session
        .as_ref()
        .unwrap()
        .poisoned
        .store(true, Ordering::Release);
    let client = fixture.client.clone();
    let next = blocking(move || read(&client, &file, SLOT as u64, 1000).unwrap()).await;
    assert_eq!(next, content()[SLOT..SLOT + 1000]);
    // Closing with the window still pinned would have quarantined Home.
    assert_eq!(fixture.exports.quarantines.load(Ordering::SeqCst), 0);
    assert_eq!(operations(&fixture.registry, "release"), 1);
    assert_eq!(fixture.negotiations(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_session_idle_limit_replaces_an_idle_session() {
    let mut short = policy(DataMode::Hixl);
    short.session_idle = Duration::from_secs(1);
    let fixture = fixture_parking(
        short,
        true,
        MemorySource::new(content(), 3000),
        SLOT,
        SLOT,
        Duration::from_secs(30),
    )
    .await;
    let client = fixture.client.clone();
    let file = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        assert_eq!(read(&client, &file, 0, 1000).unwrap(), content()[..1000]);
        file
    })
    .await;
    assert_eq!(fixture.negotiations(), 1);
    let pool = fixture.reader.home_pool("node-a").unwrap();
    pool.slots[0].try_lock().unwrap().touched = Some(Instant::now() - Duration::from_secs(2));
    let client = fixture.client.clone();
    let next = blocking(move || read(&client, &file, SLOT as u64, 1000).unwrap()).await;
    assert_eq!(next, content()[SLOT..SLOT + 1000]);
    assert_eq!(fixture.negotiations(), 2);
    assert_eq!(fixture.exports.quarantines.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_slot_is_never_delivered() {
    let fixture = fixture(policy(DataMode::Hixl), true).await;
    fixture.native.corrupt.store(true, Ordering::SeqCst);
    let client = fixture.client.clone();
    let error = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        read(&client, &file, 0, 1000).unwrap_err()
    })
    .await;
    assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
    fixture.wait_released().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_the_fetch_waiter_keeps_exports_until_the_native_read_finishes() {
    let fixture = parking_fixture(RELEASE_PARK).await;
    let file = {
        let client = fixture.client.clone();
        blocking(move || open(&client, libc::O_RDONLY)).await
    };
    let request = FetchRequest {
        home: "node-a".to_owned(),
        client: fixture.client.cloned_client().unwrap(),
        access: root_access(&grant()),
        handle: file.handle,
        offset: 0,
        size: 1000,
        deadline: Instant::now() + Duration::from_secs(5),
    };
    fixture.native.hold_read.store(true, Ordering::SeqCst);
    let waiter = tokio::spawn(fixture.reader.clone().fetch(request, None));
    let started = tokio::time::timeout(Duration::from_secs(3), async {
        while fixture.native.reads_started.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    drop(waiter);
    let active = fixture.exports.active();
    let releases = operations(&fixture.registry, "release");
    fixture.native.hold_read.store(false, Ordering::SeqCst);
    started.expect("native read entered");
    assert_eq!(active, 1);
    assert_eq!(releases, 0);
    fixture.wait_released().await;
    assert_eq!(fixture.native.batches.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_window_is_an_open_snapshot_until_this_node_changes_the_file() {
    let fixture = fixture(policy(DataMode::Hixl), true).await;
    let client = fixture.client.clone();
    let source = fixture.source.clone();
    let (cached, refreshed) = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        read(&client, &file, 0, 1000).unwrap();
        source.data.lock().unwrap()[..SLOT].fill(0xee);
        let cached = read(&client, &file, 1000, 1000).unwrap();
        client.invalidate_hixl(Some(&FileIdentity(b"identity-a".to_vec())));
        let refreshed = read(&client, &file, 2000, 1000).unwrap();
        (cached, refreshed)
    })
    .await;
    assert_eq!(cached, content()[1000..2000]);
    assert_eq!(refreshed, vec![0xee; 1000]);
    assert_eq!(fixture.native.batches.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prefetch_serves_the_next_window_without_a_foreground_fetch() {
    let mut prefetch = policy(DataMode::Hixl);
    prefetch.max_inflight = 2;
    let fixture = fixture(prefetch, true).await;
    let client = fixture.client.clone();
    let all = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        scan(&client, &file)
    })
    .await;
    assert_eq!(all, content());
    assert!(fixture.reads("prefetch_hit") >= 2);
    fixture.wait_released().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prefetch_starts_as_soon_as_a_window_is_installed() {
    // With depth 2 the next window is requested when the current one lands,
    // not after the reader has consumed half of it, so its Home prepare
    // overlaps the whole consumption of the current window.
    let mut prefetch = policy(DataMode::Hixl);
    prefetch.max_inflight = 2;
    let fixture = fixture(prefetch, true).await;
    let client = fixture.client.clone();
    let first = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        read(&client, &file, 0, 100).unwrap()
    })
    .await;
    assert_eq!(first, content()[..100]);
    let mut started = false;
    for _ in 0..200 {
        if fixture.native.batches.load(Ordering::SeqCst) >= 2 {
            started = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        started,
        "the next window must be fetched before the reader reaches its middle"
    );
    fixture.wait_released().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_reply_serves_a_window_hit_without_copying() {
    // A reply that lies in the cached window shares the window's bytes, so
    // a hit costs no allocation or copy before it reaches the kernel.
    let fixture = fixture(policy(DataMode::Hixl), true).await;
    let client = fixture.client.clone();
    // The first READ misses and installs the window; the next two hit it.
    let (first, second, third) = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        let first = client.read_reply(&grant(), &file, 0, 100).unwrap();
        let second = client.read_reply(&grant(), &file, 100, 100).unwrap();
        let third = client.read_reply(&grant(), &file, 200, 100).unwrap();
        (first, second, third)
    })
    .await;
    assert_eq!(&first[..], &content()[..100]);
    assert_eq!(&second[..], &content()[100..200]);
    assert_eq!(&third[..], &content()[200..300]);
    assert_eq!(
        third.as_ptr() as usize,
        second.as_ptr() as usize + 100,
        "window hits are slices of the same cached window"
    );
    assert_eq!(fixture.native.batches.load(Ordering::SeqCst), 1);
    fixture.wait_released().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_window_reply_keeps_the_budget_after_its_open_is_released() {
    let fixture = fixture(policy(DataMode::Hixl), true).await;
    let client = fixture.client.clone();
    let reply = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        read(&client, &file, 0, 100).unwrap();
        let reply = client.read_reply(&grant(), &file, 100, 100).unwrap();
        client.hixl_read_session().unwrap().released(&file.handle);
        reply
    })
    .await;
    assert_eq!(&reply[..], &content()[100..200]);
    assert_eq!(
        fixture.reader.budget.used.load(Ordering::SeqCst),
        SLOT as u64
    );
    drop(reply);
    assert_eq!(fixture.reader.budget.used.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_a_prefetch_waiter_keeps_the_budget_until_the_fetch_finishes() {
    let mut prefetched = policy(DataMode::Hixl);
    prefetched.max_inflight = 2;
    let fixture = fixture(prefetched, true).await;
    fixture.native.hold_read_at.store(2, Ordering::SeqCst);
    let client = fixture.client.clone();
    let file = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        read(&client, &file, 0, 100).unwrap();
        file
    })
    .await;
    let started = tokio::time::timeout(Duration::from_secs(3), async {
        while fixture.native.reads_started.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    fixture
        .client
        .hixl_read_session()
        .unwrap()
        .released(&file.handle);
    let held = fixture.reader.budget.used.load(Ordering::SeqCst);
    fixture.native.hold_read_at.store(0, Ordering::SeqCst);
    started.expect("the native prefetch entered");
    assert_eq!(held, (2 * SLOT) as u64);
    fixture
        .wait_until("the detached prefetch returned its budget", |fixture| {
            fixture.reader.budget.used.load(Ordering::SeqCst) == 0
        })
        .await;
    fixture.wait_released().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_reply_enforces_the_payload_limit_even_on_a_cached_eof_tail() {
    let fixture = fixture(policy(DataMode::Hixl), true).await;
    let client = fixture.client.clone();
    blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        let offset = (FILE_BYTES - 10) as u64;
        assert_eq!(
            read(&client, &file, offset, 100).unwrap(),
            &content()[FILE_BYTES - 10..]
        );
        let error = client
            .read_reply(
                &grant(),
                &file,
                offset,
                crate::node::storage::MAX_TRANSFER_BYTES + 1,
            )
            .unwrap_err();
        assert_eq!(error.code(), afs_error::CLIENT_ARGUMENT_INVALID);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reader_negotiates_crc32c_slot_checksums_with_a_capable_home() {
    let fixture = fixture(policy(DataMode::Hixl), true).await;
    let client = fixture.client.clone();
    let all = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        scan(&client, &file)
    })
    .await;
    assert_eq!(all, content());
    assert_eq!(
        fixture.home.negotiated_slot_checksums(),
        vec![
            afs_protocol::node_data::DfsDigestAlgorithm::Crc32c as i32,
            afs_protocol::node_data::DfsDigestAlgorithm::Blake3 as i32,
        ],
        "a reader offers CRC32C first and keeps BLAKE3 for older Homes"
    );
    fixture.wait_released().await;
}

#[test]
fn window_ramp_doubles_from_one_slot_to_the_maximum() {
    assert_eq!(next_ramp(0, SLOT, 4 * SLOT), SLOT);
    assert_eq!(next_ramp(SLOT, SLOT, 4 * SLOT), 2 * SLOT);
    assert_eq!(next_ramp(4 * SLOT, SLOT, 4 * SLOT), 4 * SLOT);
    assert_eq!(next_ramp(0, SLOT, SLOT / 2), SLOT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reader_with_smaller_slots_than_home_reads_through_hixl() {
    let fixture = fixture_windows(
        policy(DataMode::Hixl),
        true,
        MemorySource::new(content(), 3000),
        SLOT,
        SLOT / 2,
    )
    .await;
    let client = fixture.client.clone();
    let all = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        scan(&client, &file)
    })
    .await;
    assert_eq!(all, content());
    assert_eq!(fixture.handler.grpc_reads.load(Ordering::SeqCst), 0);
    fixture.wait_released().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_home_with_smaller_windows_than_the_reader_still_serves_full_content() {
    // The configured window is an upper bound: the negotiated capacity is
    // min(local slot, Home window) times the shared slot count, so a smaller
    // Home window shortens every prepare instead of failing it.
    let mut sized = policy(DataMode::Hixl);
    sized.max_group_bytes = 4 * SLOT;
    let fixture = fixture_windows(
        sized,
        true,
        MemorySource::new(content(), 3000),
        SLOT / 2,
        SLOT,
    )
    .await;
    let client = fixture.client.clone();
    let all = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        scan(&client, &file)
    })
    .await;
    assert_eq!(all, content());
    assert_eq!(fixture.handler.grpc_reads.load(Ordering::SeqCst), 0);
    fixture.wait_released().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_truncating_open_drops_windows_of_earlier_opens() {
    let fixture = fixture(policy(DataMode::Auto), true).await;
    let client = fixture.client.clone();
    let source = fixture.source.clone();
    let (cached, refreshed) = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        read(&client, &file, 0, 1000).unwrap();
        source.data.lock().unwrap()[..SLOT].fill(0xee);
        let cached = read(&client, &file, 1000, 1000).unwrap();
        let _truncating = open(&client, libc::O_WRONLY | libc::O_TRUNC);
        let refreshed = read(&client, &file, 2000, 1000).unwrap();
        (cached, refreshed)
    })
    .await;
    assert_eq!(cached, content()[1000..2000]);
    assert_eq!(refreshed, vec![0xee; 1000]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_cached_window_is_charged_to_the_host_budget() {
    let mut tight = policy(DataMode::Auto);
    tight.host_budget_bytes = SLOT as u64;
    let fixture = fixture(tight, true).await;
    let client = fixture.client.clone();
    let reader = fixture.client.hixl_read_session().unwrap().reader.clone();
    let data = blocking(move || {
        let first = open(&client, libc::O_RDONLY);
        let second = open(&client, libc::O_RDONLY);
        let a = read(&client, &first, 0, 1000).unwrap();
        // The first open's window holds the whole budget.
        assert_eq!(reader.budget.used.load(Ordering::SeqCst), SLOT as u64);
        let b = read(&client, &second, 0, 1000).unwrap();
        [a, b].concat()
    })
    .await;
    assert_eq!(data, [&content()[..1000], &content()[..1000]].concat());
    assert_eq!(
        fixture.handler.grpc_reads.load(Ordering::SeqCst),
        0,
        "the second open reads through HIXL without caching past the budget"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_home_table_evicts_idle_pools_and_otherwise_reports_backpressure() {
    let fixture = fixture(policy(DataMode::Auto), true).await;
    let reader = fixture.client.hixl_read_session().unwrap().reader.clone();
    for index in 0..MAX_OWNER_HIXL_HOMES {
        let pool = reader.home_pool(&format!("idle-home-{index}")).unwrap();
        pool.set_unsupported(
            Instant::now()
                .checked_add(UNSUPPORTED_RETRY)
                .map(|until| ("old".to_owned(), until)),
        );
        assert!(pool.unsupported("old"));
        assert!(!pool.unsupported("new"));
    }
    assert!(
        reader.home_pool("new-home").is_some(),
        "idle unsupported pools must make room"
    );
    let busy = (0..MAX_OWNER_HIXL_HOMES)
        .map(|index| reader.home_pool(&format!("busy-home-{index}")))
        .collect::<Vec<_>>();
    assert!(busy.iter().all(Option::is_some));
    assert!(reader.home_pool("one-too-many").is_none());
    let client = fixture.client.clone();
    let error = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        read(&client, &file, 0, 1000).unwrap_err()
    })
    .await;
    assert_eq!(error.code(), afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED);
    assert_eq!(fixture.handler.grpc_reads.load(Ordering::SeqCst), 0);
    drop(busy);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn warming_a_grant_returns_before_the_home_connect_finishes() {
    let fixture = fixture(policy(DataMode::Hixl), true).await;
    fixture.native.hold_connect.store(true, Ordering::SeqCst);
    let client = fixture.client.clone();
    blocking(move || client.warm(&grant())).await;
    assert_eq!(
        fixture.connects(),
        0,
        "warm returned while the connect still runs"
    );

    fixture.native.hold_connect.store(false, Ordering::SeqCst);
    fixture
        .wait_until("the warmed session is connected", |fixture| {
            fixture.connects() == 1
        })
        .await;
    assert_eq!(fixture.negotiations(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_first_read_after_warming_reuses_the_warmed_session() {
    let fixture = fixture(policy(DataMode::Hixl), true).await;
    let client = fixture.client.clone();
    blocking(move || client.warm(&grant())).await;
    fixture
        .wait_until("the warmed session is connected", |fixture| {
            fixture.connects() == 1
        })
        .await;

    let client = fixture.client.clone();
    let all = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        scan(&client, &file)
    })
    .await;
    assert_eq!(all, content());
    assert_eq!(fixture.negotiations(), 1, "no foreground negotiation");
    assert_eq!(fixture.connects(), 1, "no foreground connect");
    fixture.wait_released().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_warms_of_one_home_negotiate_once() {
    let mut policy = policy(DataMode::Hixl);
    policy.sessions_per_home = 2;
    let fixture = fixture(policy, true).await;
    fixture.native.hold_connect.store(true, Ordering::SeqCst);
    let client = fixture.client.clone();
    blocking(move || {
        client.warm(&grant());
        client.warm(&grant());
    })
    .await;
    fixture
        .wait_until("the first warm negotiated", |fixture| {
            fixture.negotiations() == 1
        })
        .await;

    fixture.native.hold_connect.store(false, Ordering::SeqCst);
    fixture
        .wait_until("the warmed session is connected", |fixture| {
            fixture.connects() == 1
        })
        .await;
    // A late second negotiation would show up within this window.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(fixture.negotiations(), 1);
    assert_eq!(fixture.connects(), 1);
}

/// Slot size of the batch tests: one slot holds two 6000-byte files.
const BATCH_SLOT: usize = 16 * 1024;
const BATCH_FILE: usize = 6000;

fn batch_policy(mode: DataMode) -> OwnerHixlPolicy {
    OwnerHixlPolicy {
        max_group_bytes: 4 * BATCH_SLOT,
        // Below this, auto opens are served by batches or gRPC only.
        min_file_bytes: 1024 * 1024,
        batch_max_file_bytes: BATCH_SLOT as u64,
        ..policy(mode)
    }
}

fn batch_content(index: usize) -> Vec<u8> {
    (0..BATCH_FILE)
        .map(|offset| ((offset + index * 7) % 251) as u8)
        .collect()
}

/// `d/f00` .. `d/f15`, 6000 bytes each: a batch of four slots packs eight.
async fn batch_fixture(policy: OwnerHixlPolicy, home_hixl: bool) -> Fixture {
    let source = MemorySource::new(content(), 3000);
    source.tree.insert_directory("d");
    for index in 0..16 {
        source
            .tree
            .insert(&format!("d/f{index:02}"), batch_content(index));
    }
    fixture_windows(policy, home_hixl, source, BATCH_SLOT, BATCH_SLOT).await
}

fn open_path(client: &OwnerPeerClient, path: &str) -> RemoteFile {
    client
        .open(&grant(), OsStr::new(path), libc::O_RDONLY, None)
        .unwrap()
        .0
}

/// Opens a file and reads it whole: a copying READ, then zero-copy READs.
fn read_file(client: &OwnerPeerClient, path: &str) -> Vec<u8> {
    let file = open_path(client, path);
    let mut all = read(client, &file, 0, 2500).unwrap();
    loop {
        let chunk = client
            .read_reply(&grant(), &file, all.len() as u64, 2500)
            .unwrap();
        if chunk.is_empty() {
            return all;
        }
        all.extend_from_slice(&chunk);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequential_small_files_are_served_from_directory_batches() {
    let fixture = batch_fixture(batch_policy(DataMode::Auto), true).await;
    let client = fixture.client.clone();
    let files = blocking(move || {
        (0..16)
            .map(|index| read_file(&client, &format!("d/f{index:02}")))
            .collect::<Vec<_>>()
    })
    .await;
    for (index, data) in files.iter().enumerate() {
        assert_eq!(*data, batch_content(index), "f{index:02}");
    }
    // The first file starts a batch and waits for it; a quarter into it the
    // lookahead fetches the second half of the directory.
    assert_eq!(fixture.native.batches.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.reads("batch_fetch"), 1);
    assert_eq!(fixture.reads("fetch"), 0, "no single-file window");
    assert_eq!(fixture.handler.grpc_reads.load(Ordering::SeqCst), 0);
    assert!(fixture.reads("batch_hit") >= 16 * 3);
    fixture.wait_released().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_hit_is_a_slice_of_the_batch_buffer() {
    let fixture = batch_fixture(batch_policy(DataMode::Hixl), true).await;
    let client = fixture.client.clone();
    blocking(move || {
        let first = open_path(&client, "d/f00");
        assert_eq!(
            read(&client, &first, 0, 100).unwrap(),
            batch_content(0)[..100]
        );
        let second = open_path(&client, "d/f01");
        let head = client.read_reply(&grant(), &second, 0, 1000).unwrap();
        let tail = client.read_reply(&grant(), &second, 1000, 1000).unwrap();
        assert_eq!(head, batch_content(1)[..1000]);
        // Both READs point into the one buffer the native READ filled.
        assert_eq!(head.as_ptr().wrapping_add(1000), tail.as_ptr());
        // Past EOF the claimed slice still answers, without a fetch.
        assert!(
            client
                .read_reply(&grant(), &second, BATCH_FILE as u64, 1000)
                .unwrap()
                .is_empty()
        );
    })
    .await;
    assert_eq!(fixture.native.batches.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.reads("fetch"), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_that_sees_a_newer_version_does_not_use_the_batch() {
    let fixture = batch_fixture(batch_policy(DataMode::Auto), true).await;
    let client = fixture.client.clone();
    let source = fixture.source.clone();
    let rewritten = vec![9_u8; BATCH_FILE];
    let expected = rewritten.clone();
    let data = blocking(move || {
        assert_eq!(read_file(&client, "d/f00"), batch_content(0));
        // Home rewrites f01 after the batch packed it.
        source.tree.write("d/f01", rewritten);
        read_file(&client, "d/f01")
    })
    .await;
    assert_eq!(data, expected);
    assert!(fixture.handler.grpc_reads.load(Ordering::SeqCst) > 0);
    assert!(fixture.reads("batch_fallback") > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_local_write_drops_the_files_batch_slice() {
    let fixture = batch_fixture(batch_policy(DataMode::Auto), true).await;
    let client = fixture.client.clone();
    blocking(move || {
        assert_eq!(read_file(&client, "d/f00"), batch_content(0));
        client
            .hixl_read_session()
            .unwrap()
            .invalidate(b"identity:d/f01");
        assert_eq!(read_file(&client, "d/f01"), batch_content(1));
    })
    .await;
    assert!(fixture.handler.grpc_reads.load(Ordering::SeqCst) > 0);
    assert_eq!(fixture.native.batches.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backward_misses_do_not_start_batches() {
    let fixture = batch_fixture(batch_policy(DataMode::Auto), true).await;
    let client = fixture.client.clone();
    blocking(move || {
        assert_eq!(read_file(&client, "d/f05"), batch_content(5));
        assert_eq!(read_file(&client, "d/f02"), batch_content(2));
    })
    .await;
    assert_eq!(fixture.native.batches.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.reads("batch_fetch"), 1);
    assert!(fixture.handler.grpc_reads.load(Ordering::SeqCst) > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_files_and_disabled_batches_keep_their_paths() {
    let fixture = batch_fixture(batch_policy(DataMode::Hixl), true).await;
    fixture
        .source
        .tree
        .insert("d/large", vec![3; BATCH_SLOT + 1]);
    let client = fixture.client.clone();
    let data = blocking(move || read_file(&client, "d/large")).await;
    assert_eq!(data, vec![3; BATCH_SLOT + 1]);
    assert_eq!(fixture.reads("batch_fetch"), 0);
    assert!(fixture.reads("fetch") > 0);

    let mut off = batch_policy(DataMode::Auto);
    off.batch_max_file_bytes = 0;
    let fixture = batch_fixture(off, true).await;
    let client = fixture.client.clone();
    let data = blocking(move || read_file(&client, "d/f00")).await;
    assert_eq!(data, batch_content(0));
    assert_eq!(fixture.native.batches.load(Ordering::SeqCst), 0);
    assert!(fixture.handler.grpc_reads.load(Ordering::SeqCst) > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_files_fall_back_per_mode_when_home_lacks_hixl() {
    let fixture = batch_fixture(batch_policy(DataMode::Auto), false).await;
    let client = fixture.client.clone();
    let data = blocking(move || read_file(&client, "d/f00")).await;
    assert_eq!(data, batch_content(0));
    assert!(fixture.handler.grpc_reads.load(Ordering::SeqCst) > 0);

    let fixture = batch_fixture(batch_policy(DataMode::Hixl), false).await;
    let client = fixture.client.clone();
    let error = blocking(move || {
        let file = open_path(&client, "d/f00");
        read(&client, &file, 0, 1000).unwrap_err()
    })
    .await;
    assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNSUPPORTED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_propagates_a_directory_batch_checksum_error_without_grpc() {
    let fixture = batch_fixture(batch_policy(DataMode::Auto), true).await;
    fixture.native.corrupt.store(true, Ordering::SeqCst);
    let client = fixture.client.clone();
    let error = blocking(move || {
        let file = open_path(&client, "d/f00");
        read(&client, &file, 0, 1000).unwrap_err()
    })
    .await;
    assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
    assert_eq!(fixture.handler.grpc_reads.load(Ordering::SeqCst), 0);
    fixture.wait_released().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_returns_backpressure_when_the_directory_batch_budget_is_full() {
    let mut policy = batch_policy(DataMode::Auto);
    policy.host_budget_bytes = 1;
    let fixture = batch_fixture(policy, true).await;
    let client = fixture.client.clone();
    let error = blocking(move || {
        let file = open_path(&client, "d/f00");
        read(&client, &file, 0, 1000).unwrap_err()
    })
    .await;
    assert_eq!(error.code(), afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED);
    assert_eq!(fixture.handler.grpc_reads.load(Ordering::SeqCst), 0);
}

fn store_route(name: &str, version: FileVersion) -> BatchRoute {
    BatchRoute {
        parent: b"root-a\0d".to_vec(),
        parent_path: b"d".to_vec(),
        name: name.as_bytes().to_vec(),
        version,
        batch_only: true,
    }
}

fn landed(files: &[(&str, usize)]) -> batch::FetchedBatch {
    let total = files.iter().map(|(_, length)| *length).sum::<usize>();
    let mut start = 0;
    batch::FetchedBatch {
        bytes: vec![7; total],
        files: files
            .iter()
            .map(|(name, length)| {
                let range = start..start + length;
                start += length;
                (
                    name.as_bytes().to_vec(),
                    FileVersion {
                        size: *length as u64,
                        mtime_ns: 1,
                        ctime_ns: 1,
                    },
                    range,
                )
            })
            .collect(),
        next: b"z".to_vec(),
    }
}

fn version(size: u64) -> FileVersion {
    FileVersion {
        size,
        mtime_ns: 1,
        ctime_ns: 1,
    }
}

#[test]
fn a_file_written_while_its_batch_is_in_flight_is_not_published() {
    let budget = Arc::new(WindowBudget::new(1024 * 1024));
    let mut store = batch::BatchStore::default();
    let start = store
        .begin(b"root-a\0d", b"a", 4096, 64 * 1024, &budget)
        .unwrap();
    store.invalidate(b"a");
    store.finish(
        b"root-a\0d",
        start,
        Ok(Some(landed(&[("a", 100), ("b", 100)]))),
    );
    assert!(matches!(
        store.claim(b"a", &store_route("a", version(100)), false),
        Claim::Miss
    ));
    assert!(matches!(
        store.claim(b"b", &store_route("b", version(100)), false),
        Claim::Hit(..)
    ));
}

#[test]
fn claimed_batch_slices_keep_the_budget_and_a_stale_version_misses() {
    let budget = Arc::new(WindowBudget::new(1024 * 1024));
    let mut store = batch::BatchStore::default();
    let start = store
        .begin(b"root-a\0d", b"a", 4096, 64 * 1024, &budget)
        .unwrap();
    assert_eq!(budget.used.load(Ordering::SeqCst), 4096);
    store.finish(
        b"root-a\0d",
        start,
        Ok(Some(landed(&[("a", 100), ("b", 100), ("c", 100)]))),
    );
    assert_eq!(store.held(), 300);
    assert_eq!(budget.used.load(Ordering::SeqCst), 300);
    // The open saw another size: the slice is dropped, not served.
    assert!(matches!(
        store.claim(b"a", &store_route("a", version(101)), false),
        Claim::Miss
    ));
    let Claim::Hit(bytes, lookahead) = store.claim(b"b", &store_route("b", version(100)), false)
    else {
        panic!("b is resident");
    };
    assert_eq!(bytes.len(), 100);
    assert_eq!(lookahead, Some(b"z".to_vec()), "a quarter is claimed");
    // No lookahead started, so the next hit asks again.
    let Claim::Hit(last, lookahead) = store.claim(b"c", &store_route("c", version(100)), false)
    else {
        panic!("c is resident");
    };
    assert_eq!(lookahead, Some(b"z".to_vec()));
    assert_eq!(store.held(), 0);
    let last_slice = last.slice(10..20);
    drop(last);
    drop(bytes);
    assert_eq!(budget.used.load(Ordering::SeqCst), 300);
    drop(last_slice);
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
}

#[test]
fn evicting_a_batch_does_not_refund_a_claimed_slice() {
    let budget = Arc::new(WindowBudget::new(4096));
    let mut store = batch::BatchStore::default();
    let start = store
        .begin(b"root-a\0d", b"a", 4096, 4096, &budget)
        .unwrap();
    store.finish(
        b"root-a\0d",
        start,
        Ok(Some(landed(&[("a", 1024), ("b", 1024)]))),
    );
    let Claim::Hit(bytes, _) = store.claim(b"a", &store_route("a", version(1024)), false) else {
        panic!("a is resident");
    };
    let later = Instant::now() + Duration::from_secs(2);
    assert!(
        store
            .begin_at(b"root-a\0e", b"c", 4096, 4096, &budget, false, later)
            .is_none()
    );
    assert_eq!(store.held(), 0);
    assert_eq!(budget.used.load(Ordering::SeqCst), 2048);
    assert_eq!(bytes.len(), 1024);
    drop(bytes);
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    assert!(
        store
            .begin(b"root-a\0e", b"c", 4096, 4096, &budget)
            .is_some()
    );
}

#[test]
fn only_a_first_touch_or_the_next_anchor_starts_a_batch() {
    let budget = Arc::new(WindowBudget::new(1024 * 1024));
    let mut store = batch::BatchStore::default();
    assert!(matches!(
        store.claim(b"x", &store_route("m", version(100)), true),
        Claim::Start
    ));
    let start = store
        .begin(b"root-a\0d", b"m", 4096, 64 * 1024, &budget)
        .unwrap();
    // In flight: a later name waits, an earlier one does not.
    assert!(matches!(
        store.claim(b"x", &store_route("n", version(100)), true),
        Claim::Wait(_)
    ));
    assert!(matches!(
        store.claim(b"x", &store_route("a", version(100)), true),
        Claim::Miss
    ));
    store.finish(b"root-a\0d", start, Ok(Some(landed(&[("m", 100)]))));
    assert!(matches!(
        store.claim(b"x", &store_route("q", version(100)), true),
        Claim::Miss
    ));
    assert!(matches!(
        store.claim(b"x", &store_route("z", version(100)), true),
        Claim::Start
    ));
}

fn dir_route(parent: &str, name: &str, size: u64) -> BatchRoute {
    BatchRoute {
        parent: format!("root-a\0{parent}").into_bytes(),
        parent_path: parent.as_bytes().to_vec(),
        name: name.as_bytes().to_vec(),
        version: version(size),
        batch_only: true,
    }
}

fn dir_batch(names: &[&str], length: usize, next: &str) -> batch::FetchedBatch {
    let mut batch = landed(&names.iter().map(|name| (*name, length)).collect::<Vec<_>>());
    batch.next = next.as_bytes().to_vec();
    batch
}

#[test]
fn a_new_directory_does_not_evict_batches_other_directories_are_reading() {
    let budget = Arc::new(WindowBudget::new(1024 * 1024));
    let mut store = batch::BatchStore::default();
    // Two directories each read the first file of a two-file batch; the
    // store is then full.
    for dir in ["a", "b"] {
        let start = store
            .begin(
                format!("root-a\0{dir}").as_bytes(),
                b"1",
                4096,
                8192,
                &budget,
            )
            .unwrap();
        let files = [format!("{dir}1"), format!("{dir}2")];
        let names = files.iter().map(String::as_str).collect::<Vec<_>>();
        store.finish(
            format!("root-a\0{dir}").as_bytes(),
            start,
            Ok(Some(dir_batch(&names, 2048, "3"))),
        );
        let first = format!("{dir}1");
        assert!(matches!(
            store.claim(first.as_bytes(), &dir_route(dir, "1", 2048), false),
            Claim::Hit(..)
        ));
    }
    // A third directory finds no room: it is refused instead of taking the
    // place of a batch that still holds unread files.
    assert!(
        store
            .begin(b"root-a\0c", b"1", 4096, 8192, &budget)
            .is_none()
    );
    for dir in ["a", "b"] {
        let second = format!("{dir}2");
        assert!(
            matches!(
                store.claim(second.as_bytes(), &dir_route(dir, "2", 2048), false),
                Claim::Hit(..)
            ),
            "{dir}2 is still resident"
        );
    }
    // Both batches are read: the third directory starts now.
    assert!(matches!(
        store.claim(b"c1", &dir_route("c", "1", 2048), true),
        Claim::Start
    ));
    assert!(
        store
            .begin(b"root-a\0c", b"1", 4096, 8192, &budget)
            .is_some()
    );
}

/// Lands a two-file batch of `dir` at `anchor` and claims its first file.
fn read_first(
    store: &mut batch::BatchStore,
    budget: &Arc<WindowBudget>,
    dir: &str,
    anchor: &str,
    next: &str,
) -> Option<Vec<u8>> {
    let parent = format!("root-a\0{dir}");
    let start = store
        .begin(parent.as_bytes(), anchor.as_bytes(), 4096, 8192, budget)
        .unwrap();
    let files = [format!("{dir}{anchor}1"), format!("{dir}{anchor}2")];
    let names = files.iter().map(String::as_str).collect::<Vec<_>>();
    store.finish(
        parent.as_bytes(),
        start,
        Ok(Some(dir_batch(&names, 2048, next))),
    );
    let Claim::Hit(_, lookahead) = store.claim(
        files[0].as_bytes(),
        &dir_route(dir, &format!("{anchor}1"), 2048),
        false,
    ) else {
        panic!("{} is resident", files[0]);
    };
    lookahead
}

#[test]
fn an_idle_batch_makes_room_and_a_lookahead_never_evicts_a_read_batch() {
    let budget = Arc::new(WindowBudget::new(1024 * 1024));
    let mut store = batch::BatchStore::default();
    let next = read_first(&mut store, &budget, "a", "1", "3").unwrap();
    assert_eq!(next, b"3");
    read_first(&mut store, &budget, "b", "1", "3");
    // Full: a's lookahead does not take b's place and is offered again.
    assert!(
        store
            .begin_lookahead(b"root-a\0a", &next, 4096, 8192, &budget)
            .is_none()
    );
    assert!(matches!(
        store.claim(b"a12", &dir_route("a", "12", 2048), false),
        Claim::Hit(_, Some(_))
    ));
    // a is read through; c takes its room and the store is full again.
    assert!(
        store
            .begin(b"root-a\0c", b"1", 4096, 8192, &budget)
            .is_some()
    );
    // b stops reading: after the idle time its batch makes room.
    let later = Instant::now() + Duration::from_secs(2);
    assert!(
        store
            .begin_at(b"root-a\0d", b"1", 4096, 8192, &budget, false, later)
            .is_some()
    );
    assert!(matches!(
        store.claim(b"b12", &dir_route("b", "12", 2048), false),
        Claim::Miss
    ));
}

#[test]
fn a_needed_batch_drops_an_unread_lookahead_and_its_directory_resumes_there() {
    let budget = Arc::new(WindowBudget::new(1024 * 1024));
    let mut store = batch::BatchStore::default();
    let cap = 3 * 4096;
    let parent = b"root-a\0a";
    let start = store.begin(parent, b"1", 4096, cap, &budget).unwrap();
    store.finish(
        parent,
        start,
        Ok(Some(dir_batch(&["a11", "a12"], 2048, "3"))),
    );
    assert!(matches!(
        store.claim(b"a11", &dir_route("a", "11", 2048), false),
        Claim::Hit(_, Some(_))
    ));
    let start = store
        .begin_lookahead(parent, b"3", 4096, cap, &budget)
        .unwrap();
    // The lookahead started, so the current batch stops asking for it.
    assert!(matches!(
        store.claim(b"a12", &dir_route("a", "12", 2048), false),
        Claim::Hit(_, None)
    ));
    store.finish(parent, start, Ok(Some(dir_batch(&["a3", "a4"], 2048, "5"))));
    let start = store.begin(b"root-a\0b", b"1", 4096, cap, &budget).unwrap();
    store.finish(
        b"root-a\0b",
        start,
        Ok(Some(dir_batch(&["b11", "b12"], 2048, "3"))),
    );
    // c needs a batch: a's unread lookahead gives way, b's batch stays.
    assert!(
        store
            .begin(b"root-a\0c", b"1", 4096, cap - 4096, &budget)
            .is_some()
    );
    assert!(matches!(
        store.claim(b"b11", &dir_route("b", "11", 2048), false),
        Claim::Hit(..)
    ));
    // a starts again where the dropped lookahead began.
    assert!(matches!(
        store.claim(b"a3", &dir_route("a", "3", 2048), true),
        Claim::Start
    ));
}

#[test]
fn a_refused_start_lets_the_next_miss_of_its_directory_try_again() {
    let budget = Arc::new(WindowBudget::new(1024 * 1024));
    let mut store = batch::BatchStore::default();
    read_first(&mut store, &budget, "a", "1", "3");
    read_first(&mut store, &budget, "b", "1", "3");
    // a reached its next anchor but has no room.
    assert!(matches!(
        store.claim(b"a3", &dir_route("a", "3", 2048), true),
        Claim::Start
    ));
    assert!(
        store
            .begin(b"root-a\0a", b"3", 4096, 8192, &budget)
            .is_none()
    );
    // The following file is past the anchor, yet it may start.
    assert!(matches!(
        store.claim(b"a4", &dir_route("a", "4", 2048), true),
        Claim::Start
    ));
}
