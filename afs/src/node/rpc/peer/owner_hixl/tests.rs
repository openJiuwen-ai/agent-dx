//! Loopback tests: a real OwnerFiles tonic server with the Home window service
//! over in-memory export slots, and an `OwnerPeerClient` whose native reader
//! pulls those slots from the same memory.

use super::super::hixl::ReadLane;
use super::*;
use crate::node::rpc::data::owner_hixl::OwnerHixlService;
use crate::node::rpc::data::owner_hixl::test_support::{
    HOME_ENGINE, MemoryExports, MemorySource, OPEN_HANDLE,
};
use crate::node::rpc::data::{
    OwnerFilesHandler, OwnerFilesService, PeerAuthenticator, make_owner_files_server_from,
};
use crate::node::rpc::hixl::test_support::{engine, native_error, negotiations};
use crate::node::rpc::peer::{
    OwnerPeerClient, owner_files_client_from_channel_with_runtime_and_metrics,
};
use crate::node::vfs::ownerfs::files::FileIdentity;
use crate::node::vfs::ownerfs::remote::RemoteFiles;
use crate::node::vfs::ownerfs::root::{RootId, RootRight};
use afs_protocol::node_data::{
    OwnerFileAttr, OwnerFileKind, OwnerOpenReply, OwnerOpenRequest, OwnerReadReply,
    OwnerReadRequest,
};
use afs_transport::hixl::{EngineInfo, HixlError, HixlPhase, HixlReadOutcome, HixlReadTiming};
use std::ffi::OsStr;
use std::sync::atomic::AtomicUsize;
use tokio_stream::wrappers::TcpListenerStream;

const SLOT: usize = 4096;
const FILE_BYTES: usize = 50_000;

struct MemoryReader {
    exports: Arc<MemoryExports>,
    info: EngineInfo,
    batches: AtomicUsize,
    corrupt: AtomicBool,
}

impl ReadLane for MemoryReader {
    fn connect(&self, peer: &str, _deadline: Instant) -> Result<(), HixlError> {
        assert_eq!(peer, HOME_ENGINE);
        Ok(())
    }

    fn prepare_read_batch_buffer(&self, length: usize) -> Result<Vec<u8>, HixlError> {
        Ok(vec![0; length])
    }

    fn read_batch_timed_into(
        &self,
        peer: &str,
        windows: &[(u64, usize)],
        mut output: Vec<u8>,
        _deadline: Instant,
    ) -> Result<HixlReadOutcome, HixlError> {
        assert_eq!(peer, HOME_ENGINE);
        let mut offset = 0;
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
            output[offset..offset + length].copy_from_slice(&bytes);
            offset += length;
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
}

impl OwnerFilesHandler for FileHandler {
    fn open(&self, peer: &str, _request: OwnerOpenRequest) -> afs_error::Result<OwnerOpenReply> {
        assert_eq!(peer, "node-b");
        let size = self.source.data.lock().unwrap().len() as u64;
        let identity = afs_protocol::node_data::FileIdentity {
            opaque: b"identity-a".to_vec(),
        };
        // Every OPEN is a distinct Home handle, like the real executor.
        let open = self.opens.fetch_add(1, Ordering::SeqCst);
        let mut handle = OPEN_HANDLE.to_vec();
        handle.extend_from_slice(format!("-{open}").as_bytes());
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
                ..Default::default()
            }),
            prefetched_data: None,
        })
    }

    fn read(&self, peer: &str, request: OwnerReadRequest) -> afs_error::Result<OwnerReadReply> {
        assert_eq!(peer, "node-b");
        self.grpc_reads.fetch_add(1, Ordering::SeqCst);
        let data = self.source.data.lock().unwrap();
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

fn policy(mode: HixlOwnerFsReadMode) -> OwnerHixlPolicy {
    OwnerHixlPolicy {
        mode,
        max_window_bytes: 4 * SLOT,
        max_inflight: 1,
        host_budget_bytes: 1024 * 1024,
        min_file_bytes: 1,
        receive_slots: 16,
        sessions_per_home: 1,
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
    fixture_windows(policy, home_hixl, source, SLOT).await
}

/// A reader whose native window is `reader_slot` bytes against a Home
/// window of `SLOT` bytes.
async fn fixture_windows(
    policy: OwnerHixlPolicy,
    home_hixl: bool,
    source: MemorySource,
    reader_slot: usize,
) -> Fixture {
    let exports = Arc::new(MemoryExports::new(SLOT, 8));
    let source = Arc::new(source);
    let handler = Arc::new(FileHandler {
        source: source.clone(),
        grpc_reads: AtomicUsize::new(0),
        opens: AtomicUsize::new(0),
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
            .add_service(make_owner_files_server_from(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let native = Arc::new(MemoryReader {
        exports: exports.clone(),
        info: engine("127.0.0.1:7940", 3, reader_slot),
        batches: AtomicUsize::new(0),
        corrupt: AtomicBool::new(false),
    });
    let registry = afs_metrics::Registry::new();
    let reader = OwnerHixlReader::from_runtime(
        Arc::new(MemoryRuntime(native.clone())),
        "node-b".to_owned(),
        policy,
        Duration::from_secs(5),
    )
    .unwrap()
    .with_metrics(HixlMetrics::register(&registry).unwrap());
    let client = owner_files_client_from_channel_with_runtime_and_metrics(
        channel,
        tokio::runtime::Handle::current(),
        None,
    )
    .with_hixl(Arc::new(reader), "node-a".to_owned());
    Fixture {
        client: Arc::new(client),
        exports,
        source,
        native,
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
    let fixture = fixture(policy(HixlOwnerFsReadMode::Required), true).await;
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
    // Windows of 4, 8, 16, 16 and a final 16 KiB request ending at EOF.
    assert_eq!(fixture.native.batches.load(Ordering::SeqCst), 5);
    assert_eq!(fixture.exports.exports.load(Ordering::SeqCst), 13);
    assert_eq!(fixture.handler.grpc_reads.load(Ordering::SeqCst), 0);
    assert!(fixture.reads("window_hit") > 40);
    fixture.wait_released().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preferred_opens_a_window_only_after_two_sequential_reads() {
    let fixture = fixture(policy(HixlOwnerFsReadMode::Preferred), true).await;
    let client = fixture.client.clone();
    let reads = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        // A random read, then the kernel readahead that follows it, then the
        // read that confirms the stream is sequential.
        [20_000, 21_000, 22_000].map(|offset| read(&client, &file, offset, 1000).unwrap())
    })
    .await;
    for (index, data) in reads.iter().enumerate() {
        let offset = 20_000 + index * 1000;
        assert_eq!(*data, content()[offset..offset + 1000]);
    }
    assert_eq!(fixture.handler.grpc_reads.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.native.batches.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.reads("fallback"), 2);
    assert_eq!(fixture.reads("fetch"), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn required_offset_zero_and_window_continuations_need_no_streak() {
    let fixture = fixture(policy(HixlOwnerFsReadMode::Required), true).await;
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
    let mut pooled = policy(HixlOwnerFsReadMode::Required);
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn small_and_writable_opens_keep_the_grpc_path() {
    let mut small = policy(HixlOwnerFsReadMode::Preferred);
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
async fn home_without_capability_falls_back_only_in_preferred_mode() {
    let fixture = fixture(policy(HixlOwnerFsReadMode::Preferred), false).await;
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

    let fixture = self::fixture(policy(HixlOwnerFsReadMode::Required), false).await;
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
async fn refused_negotiation_falls_back_only_in_preferred_mode() {
    let fixture = fixture(policy(HixlOwnerFsReadMode::Preferred), true).await;
    // An idle drain stops Home admission without pinning anything.
    fixture.home.drain(Duration::ZERO).await.unwrap();
    let client = fixture.client.clone();
    let data = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        let first = scan(&client, &file);
        let file = open(&client, libc::O_RDONLY);
        [first, read(&client, &file, 0, 1000).unwrap()].concat()
    })
    .await;
    assert_eq!(data, [content(), content()[..1000].to_vec()].concat());
    assert_eq!(fixture.native.batches.load(Ordering::SeqCst), 0);
    // The refusal is remembered per Home instead of renegotiated per read.
    assert_eq!(fixture.negotiations(), 1);

    let fixture = self::fixture(policy(HixlOwnerFsReadMode::Required), true).await;
    fixture.home.drain(Duration::ZERO).await.unwrap();
    let client = fixture.client.clone();
    let error = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        read(&client, &file, 0, 1000).unwrap_err()
    })
    .await;
    assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNAVAILABLE);
    assert_eq!(fixture.handler.grpc_reads.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refused_prepare_falls_back_without_leaving_slots_pinned() {
    let fixture = fixture(policy(HixlOwnerFsReadMode::Preferred), true).await;
    let client = fixture.client.clone();
    let file = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        assert_eq!(read(&client, &file, 0, 1000).unwrap(), content()[..1000]);
        file
    })
    .await;
    fixture.wait_released().await;
    // The pooled session survives; only its next prepare is refused.
    fixture.home.drain(Duration::ZERO).await.unwrap();
    let client = fixture.client.clone();
    let rest = blocking(move || {
        (1..20)
            .flat_map(|index| read(&client, &file, index * 1000, 1000).unwrap())
            .collect::<Vec<_>>()
    })
    .await;
    assert_eq!(rest, content()[1000..20_000]);
    assert_eq!(fixture.native.batches.load(Ordering::SeqCst), 1);
    assert!(fixture.handler.grpc_reads.load(Ordering::SeqCst) > 0);
    assert_eq!(fixture.exports.active(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_slot_is_never_delivered() {
    let fixture = fixture(policy(HixlOwnerFsReadMode::Required), true).await;
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
async fn a_window_is_an_open_snapshot_until_this_node_changes_the_file() {
    let fixture = fixture(policy(HixlOwnerFsReadMode::Required), true).await;
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
    let mut prefetch = policy(HixlOwnerFsReadMode::Required);
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
        policy(HixlOwnerFsReadMode::Required),
        true,
        MemorySource::new(content(), 3000),
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
async fn a_truncating_open_drops_windows_of_earlier_opens() {
    let fixture = fixture(policy(HixlOwnerFsReadMode::Required), true).await;
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
    let mut tight = policy(HixlOwnerFsReadMode::Preferred);
    tight.host_budget_bytes = SLOT as u64;
    let fixture = fixture(tight, true).await;
    let client = fixture.client.clone();
    let reader = fixture.client.hixl_files().unwrap().reader.clone();
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
        1,
        "the second open falls back instead of caching past the budget"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_home_table_evicts_idle_pools_and_preferred_reads_fall_back() {
    let fixture = fixture(policy(HixlOwnerFsReadMode::Preferred), true).await;
    let reader = fixture.client.hixl_files().unwrap().reader.clone();
    for index in 0..MAX_OWNER_HIXL_HOMES {
        let pool = reader.home_pool(&format!("idle-home-{index}")).unwrap();
        pool.set_unsupported(Instant::now().checked_add(UNSUPPORTED_RETRY));
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
    let data = blocking(move || {
        let file = open(&client, libc::O_RDONLY);
        read(&client, &file, 0, 1000).unwrap()
    })
    .await;
    assert_eq!(data, content()[..1000]);
    assert_eq!(fixture.handler.grpc_reads.load(Ordering::SeqCst), 1);
    drop(busy);
}
