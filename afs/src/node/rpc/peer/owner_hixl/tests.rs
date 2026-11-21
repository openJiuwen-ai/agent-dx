//! Loopback tests: a real OwnerFiles tonic server with the Home window service
//! over in-memory export slots, and an `OwnerPeerClient` whose native reader
//! pulls those slots from the same memory.

use super::super::batch;
use super::super::*;
use super::*;
use crate::node::data_plane::hixl::ReadLane;
use crate::node::rpc::data::owner_hixl::test_support::{
    HOME_ENGINE, MemoryExports, MemorySource, OPEN_HANDLE, tree_path,
};
use crate::node::rpc::data::owner_hixl::{OwnerHixlService, OwnerWindowSource};
use crate::node::rpc::data::{
    OwnerFilesHandler, OwnerFilesService, PeerAuthenticator, presented_access,
    remote_file_for_handle,
};
use crate::node::rpc::hixl::test_support::{engine, native_error, negotiations, operations};
use crate::node::rpc::peer::{
    OwnerPeerClient, owner_files_client_from_channel_with_runtime_and_metrics,
};
use crate::node::vfs::ownerfs::files::FileIdentity;
use crate::node::vfs::ownerfs::remote::RemoteFiles;
use crate::node::vfs::ownerfs::root::{RootId, RootRight};
use afs_protocol::node_data::{
    OwnerFileAttr, OwnerFileKind, OwnerOpenReply, OwnerOpenRequest, OwnerReadReply,
    OwnerReadRequest, OwnerWriteReply, OwnerWriteRequest, owner_files_server::OwnerFilesServer,
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
    unknown_write: AtomicBool,
    corrupt_next_write: AtomicBool,
    writes: AtomicUsize,
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
                    HixlPhase::CompleteRead,
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

    fn write_batch(
        &self,
        peer: &str,
        windows: &[(u64, usize)],
        mut bytes: Vec<u8>,
        _deadline: Instant,
    ) -> Result<(), HixlError> {
        assert_eq!(peer, self.peer_engine);
        self.writes.fetch_add(1, Ordering::SeqCst);
        if self.unknown_write.load(Ordering::SeqCst) {
            return Err(HixlError {
                read_may_be_in_flight: true,
                ..native_error(
                    HixlPhase::WriteBatch,
                    HixlErrorKind::DeadlineExceeded,
                    "DMA outcome unknown",
                )
            });
        }
        if self.corrupt_next_write.swap(false, Ordering::SeqCst) {
            bytes[0] ^= 0xff;
        }
        let mut body = bytes.as_slice();
        for &(address, length) in windows {
            let (part, rest) = body.split_at(length);
            self.exports.write(address, part);
            body = rest;
        }
        assert!(body.is_empty());
        Ok(())
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

/// Ordinary OwnerFiles RPCs over the same file as the HIXL service.
struct FileHandler {
    source: Arc<MemorySource>,
    grpc_reads: AtomicUsize,
    grpc_writes: AtomicUsize,
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
            None => {
                let mut data = self.source.data.lock().unwrap();
                if request.flags & (libc::O_TRUNC as u32) != 0 {
                    data.clear();
                }
                (data.len() as u64, b"identity-a".to_vec(), 0)
            }
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

    fn write(&self, peer: &str, request: OwnerWriteRequest) -> afs_error::Result<OwnerWriteReply> {
        assert_eq!(peer, "node-b");
        self.grpc_writes.fetch_add(1, Ordering::SeqCst);
        let access = presented_access(request.access)?;
        let file = remote_file_for_handle(&access, request.handle.unwrap().opaque);
        let written = self.source.write(
            peer,
            &access,
            &file,
            request.offset,
            &request.data,
            crate::node::vfs::types::WriteOptions {
                kill_suidgid: request.kill_suidgid,
            },
        )?;
        Ok(OwnerWriteReply {
            written: u32::try_from(written).unwrap(),
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
    write_prepares: Arc<AtomicUsize>,
    lost_read_prepare_replies: Arc<AtomicUsize>,
    /// The Home window service; attached to the server only with `home_hixl`.
    home: Arc<OwnerHixlService>,
    registry: afs_metrics::Registry,
    _server: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn with_client<T: Send + 'static>(
        &self,
        work: impl FnOnce(Arc<OwnerPeerClient>) -> T + Send + 'static,
    ) -> T {
        let client = self.client.clone();
        tokio::task::spawn_blocking(move || work(client))
            .await
            .unwrap()
    }

    async fn with_open<T: Send + 'static>(
        &self,
        flags: i32,
        work: impl FnOnce(Arc<OwnerPeerClient>, RemoteFile) -> T + Send + 'static,
    ) -> T {
        self.with_client(move |client| {
            let file = open(&client, flags);
            work(client, file)
        })
        .await
    }

    async fn read_error(&self, path: &'static str, offset: u64, size: usize) -> afs_error::Error {
        self.with_client(move |client| {
            let file = open_path(&client, path);
            read(&client, &file, offset, size).unwrap_err()
        })
        .await
    }

    fn grpc_reads(&self) -> usize {
        self.handler.grpc_reads.load(Ordering::SeqCst)
    }

    fn native_read_batches(&self) -> usize {
        self.native.batches.load(Ordering::SeqCst)
    }

    fn native_writes(&self) -> usize {
        self.native.writes.load(Ordering::SeqCst)
    }

    fn home_quarantines(&self) -> usize {
        self.exports.quarantines.load(Ordering::SeqCst)
    }

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
        min_write_bytes: 1,
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
        rights: vec![RootRight::Lookup, RootRight::Read, RootRight::Write],
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
        unknown_write: AtomicBool::new(false),
        corrupt_next_write: AtomicBool::new(false),
        writes: AtomicUsize::new(0),
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
    fixture_with_commit_loss(policy, home_hixl, source, exports, reader_slot, park, None).await
}

struct CommitLoss {
    requests: Arc<StdMutex<Vec<prost::bytes::Bytes>>>,
    drop_request: bool,
    drop_recovery: bool,
    late_commit: Option<LateCommit>,
}

struct LateCommit {
    entered: Arc<tokio::sync::Notify>,
    finished: StdMutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

async fn fixture_with_commit_loss(
    policy: OwnerHixlPolicy,
    home_hixl: bool,
    source: MemorySource,
    exports: Arc<MemoryExports>,
    reader_slot: usize,
    park: Duration,
    lost_commits: Option<CommitLoss>,
) -> Fixture {
    let lost_commits = lost_commits.map(Arc::new);
    let native = memory_reader(
        exports.clone(),
        engine("127.0.0.1:7940", 3, reader_slot),
        HOME_ENGINE,
    );
    let source = Arc::new(source);
    let handler = Arc::new(FileHandler {
        source: source.clone(),
        grpc_reads: AtomicUsize::new(0),
        grpc_writes: AtomicUsize::new(0),
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
    let write_prepares = Arc::new(AtomicUsize::new(0));
    let prepare_count = write_prepares.clone();
    let lost_read_prepare_replies = Arc::new(AtomicUsize::new(0));
    let lose_prepare_replies = lost_read_prepare_replies.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(tower::layer::layer_fn(
                move |inner: tonic::service::Routes| {
                    let lost_commits = lost_commits.clone();
                    let prepare_count = prepare_count.clone();
                    let lose_prepare_replies = lose_prepare_replies.clone();
                    tower::service_fn(move |request: axum::http::Request<tonic::body::Body>| {
                        use tower::ServiceExt;
                        let lose_prepare_reply =
                            (request.uri().path().ends_with("/PrepareReadHixl")
                                || request.uri().path().ends_with("/PrepareBatchHixl"))
                                && lose_prepare_replies
                                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                                        left.checked_sub(1)
                                    })
                                    .is_ok();
                        if request.uri().path().ends_with("/PrepareWriteHixl") {
                            prepare_count.fetch_add(1, Ordering::SeqCst);
                        }
                        let inner = inner.clone();
                        let lost_commits = lost_commits.clone();
                        async move {
                            if let Some(loss) = lost_commits {
                                if loss.drop_recovery
                                    && request.uri().path().ends_with("/AbortWriteHixl")
                                {
                                    return Ok(tonic::Status::unavailable("injected lost Abort")
                                        .into_http());
                                }
                                if !request.uri().path().ends_with("/CommitWriteHixl") {
                                    return inner.oneshot(request).await;
                                }
                                let (parts, body) = request.into_parts();
                                let wire = axum::body::to_bytes(axum::body::Body::new(body), 4096)
                                    .await
                                    .unwrap();
                                loss.requests.lock().unwrap().push(wire.clone());
                                let request = axum::http::Request::from_parts(
                                    parts,
                                    tonic::body::Body::new(axum::body::Body::from(wire)),
                                );
                                if let Some(late) = &loss.late_commit {
                                    let finished = late.finished.lock().unwrap().take();
                                    if let Some(finished) = finished {
                                        tokio::spawn(async move {
                                            inner.oneshot(request).await.unwrap();
                                            let _ = finished.send(());
                                        });
                                        // Return the first lost reply only after Home is in file I/O.
                                        late.entered.notified().await;
                                    }
                                } else if !loss.drop_request {
                                    let _ = inner.oneshot(request).await?;
                                }
                                return Ok(tonic::Status::deadline_exceeded(
                                    "injected lost commit reply",
                                )
                                .into_http());
                            }
                            let reply = inner.oneshot(request).await?;
                            if lose_prepare_reply {
                                return Ok(tonic::Status::unavailable(
                                    "injected lost read prepare reply",
                                )
                                .into_http());
                            }
                            Ok(reply)
                        }
                    })
                },
            ))
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
        write_prepares,
        lost_read_prepare_replies,
        home,
        registry,
        _server: server,
    }
}

/// Runs synchronous OwnerFiles calls off the async runtime, like FUSE workers.
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
    let (all, tail) = fixture
        .with_open(libc::O_RDONLY, move |client, file| {
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
    assert_eq!(fixture.native_read_batches(), 5);
    assert_eq!(fixture.exports.exports.load(Ordering::SeqCst), 15);
    assert_eq!(fixture.grpc_reads(), 0);
    assert!(fixture.reads("window_hit") > 40);
    fixture.wait_released().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_uses_hixl_for_an_eligible_open_from_the_first_read() {
    let fixture = fixture(policy(DataMode::Auto), true).await;
    let reads = fixture
        .with_open(libc::O_RDONLY, move |client, file| {
            // Random reads use HIXL; the streak still controls read-ahead.
            [20_000, 21_000, 22_000].map(|offset| read(&client, &file, offset, 1000).unwrap())
        })
        .await;
    for (index, data) in reads.iter().enumerate() {
        let offset = 20_000 + index * 1000;
        assert_eq!(*data, content()[offset..offset + 1000]);
    }
    assert_eq!(fixture.grpc_reads(), 0);
    assert_eq!(fixture.native_read_batches(), 3);
    assert_eq!(fixture.negotiations(), 1);
    assert_eq!(fixture.reads("fallback"), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn required_offset_zero_and_window_continuations_need_no_streak() {
    let fixture = fixture(policy(DataMode::Hixl), true).await;
    fixture
        .with_open(libc::O_RDONLY, move |client, file| {
            // Offset 0 opens a window at once; the jump fetches only its length.
            assert_eq!(read(&client, &file, 0, 1000).unwrap(), content()[..1000]);
            assert_eq!(
                read(&client, &file, 30_000, 1000).unwrap(),
                content()[30_000..31_000]
            );
        })
        .await;
    assert_eq!(fixture.native_read_batches(), 2);
    assert_eq!(fixture.grpc_reads(), 0);
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
    let file = fixture
        .with_client(move |client| open(&client, libc::O_RDONLY))
        .await;
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
    fixture
        .with_open(libc::O_RDONLY, move |client, file| {
            assert_eq!(read(&client, &file, 0, 1000).unwrap(), content()[..1000]);
            let file = open(&client, libc::O_RDWR);
            assert_eq!(read(&client, &file, 0, 1000).unwrap(), content()[..1000]);
        })
        .await;
    assert_eq!(fixture.grpc_reads(), 2);
    assert_eq!(fixture.native_read_batches(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn home_without_capability_falls_back_only_in_auto_mode() {
    let fixture = fixture(policy(DataMode::Auto), false).await;
    // A warm first asks the Home once; it must not make the reads retry or
    // open a native connection that the Home cannot serve.
    fixture
        .with_client(move |client| client.warm(&grant()))
        .await;
    fixture
        .wait_until("the warm negotiation finished", |fixture| {
            fixture.negotiations() == 1
        })
        .await;
    let data = fixture
        .with_open(libc::O_RDONLY, move |client, file| {
            let first = read(&client, &file, 0, 1000).unwrap();
            let second = read(&client, &file, 1000, 1000).unwrap();
            [first, second].concat()
        })
        .await;
    assert_eq!(data, content()[..2000]);
    assert_eq!(fixture.grpc_reads(), 2);
    assert_eq!(fixture.negotiations(), 1, "reads do not ask the Home again");
    assert_eq!(fixture.connects(), 0);

    let fixture = self::fixture(policy(DataMode::Hixl), false).await;
    let error = fixture.read_error("file", 0, 1000).await;
    assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNSUPPORTED);
    assert_eq!(fixture.grpc_reads(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn draining_home_returns_an_error_in_both_modes() {
    for mode in [DataMode::Auto, DataMode::Hixl] {
        let fixture = fixture(policy(mode), true).await;
        fixture.home.drain(Duration::ZERO).await.unwrap();
        let error = fixture.read_error("file", 0, 1000).await;
        assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNAVAILABLE);
        assert_eq!(fixture.grpc_reads(), 0);
        assert_eq!(fixture.native_read_batches(), 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refused_prepare_propagates_even_after_a_partial_window_hit() {
    let fixture = fixture(policy(DataMode::Auto), true).await;
    let file = fixture
        .with_open(libc::O_RDONLY, move |client, file| {
            assert_eq!(read(&client, &file, 0, 1000).unwrap(), content()[..1000]);
            file
        })
        .await;
    fixture.wait_released().await;
    fixture.home.drain(Duration::ZERO).await.unwrap();
    let error = fixture
        .with_client(move |client| read(&client, &file, 4000, 1000).unwrap_err())
        .await;
    assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNAVAILABLE);
    assert_eq!(fixture.native_read_batches(), 1);
    assert_eq!(fixture.grpc_reads(), 0);
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
        let data = fixture
            .with_open(libc::O_RDONLY, move |client, file| {
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
        assert_eq!(fixture.native_read_batches(), 1);
        assert_eq!(fixture.grpc_reads(), 1);
        let pool = fixture.reader.home_pool("node-a").unwrap();
        assert!(
            pool.slots[0]
                .lock()
                .await
                .session
                .as_ref()
                .unwrap()
                .poisoned
                .load(Ordering::Acquire)
        );
        fixture.wait_released().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_quarantines_unknown_completion_without_retrying_over_grpc() {
    let fixture = fixture(policy(DataMode::Auto), true).await;
    fixture.native.unknown_read.store(true, Ordering::SeqCst);
    let error = fixture.read_error("file", 0, 1000).await;
    assert_eq!(error.code(), afs_error::IO_TIMEOUT);
    fixture
        .wait_until("Home quarantine", |fixture| fixture.home_quarantines() > 0)
        .await;
    assert!(fixture.exports.active() > 0);
    assert_eq!(fixture.native.quarantines.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.grpc_reads(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_hixl_disables_open_payload_and_opens_buffered_writers() {
    let fixture = fixture(policy(DataMode::Hixl), true).await;
    fixture
        .with_client(move |client| {
            for flags in [libc::O_RDWR, libc::O_WRONLY, libc::O_RDONLY | libc::O_TRUNC] {
                let file = open(&client, flags);
                if flags == libc::O_RDWR {
                    assert_eq!(read(&client, &file, 0, 1000).unwrap(), content()[..1000]);
                }
            }
            let error = client
                .open(
                    &grant(),
                    OsStr::new("file"),
                    libc::O_RDONLY | libc::O_DIRECT,
                    None,
                )
                .unwrap_err();
            assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNSUPPORTED);
        })
        .await;
    assert_eq!(fixture.handler.opens.load(Ordering::SeqCst), 3);
    assert!(fixture.handler.disable_prefetch.load(Ordering::SeqCst));
    assert_eq!(fixture.grpc_reads(), 0);
    assert!(fixture.source.data.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_explicit_hixl_negotiation_prevents_truncating_home_open() {
    for flags in [libc::O_RDWR | libc::O_TRUNC, libc::O_RDONLY | libc::O_TRUNC] {
        let fixture = fixture(policy(DataMode::Hixl), false).await;
        let error = fixture
            .with_client(move |client| {
                client
                    .open(&grant(), OsStr::new("file"), flags, None)
                    .unwrap_err()
            })
            .await;
        assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNSUPPORTED);
        assert_eq!(fixture.handler.opens.load(Ordering::SeqCst), 0);
        assert_eq!(*fixture.source.data.lock().unwrap(), content());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rdwr_hixl_demand_reads_do_not_retain_stale_bytes() {
    let fixture = fixture(policy(DataMode::Hixl), true).await;
    let source = fixture.source.clone();
    fixture
        .with_open(libc::O_RDWR, move |client, file| {
            assert_eq!(read(&client, &file, 0, 16).unwrap(), content()[..16]);
            source.data.lock().unwrap()[..16].fill(42);
            assert_eq!(read(&client, &file, 0, 16).unwrap(), [42; 16]);
        })
        .await;
    assert_eq!(fixture.native_read_batches(), 2);
    assert_eq!(fixture.grpc_reads(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_empty_hixl_read_does_not_prepare_a_directory_batch() {
    let fixture = batch_fixture(batch_policy(DataMode::Hixl), true).await;
    fixture
        .with_client(move |client| {
            let file = open_path(&client, "d/f00");
            let bytes = client.read_reply(&grant(), &file, 0, 0).unwrap();
            assert!(bytes.is_empty());
        })
        .await;
    assert_eq!(fixture.native.reads_started.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.grpc_reads(), 1);
}

async fn parking_fixture(park: Duration) -> Fixture {
    let source = MemorySource::new(content(), 3000);
    fixture_parking(policy(DataMode::Hixl), true, source, SLOT, SLOT, park).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_window_release_rides_on_the_next_prepare_of_its_session() {
    let fixture = parking_fixture(Duration::from_secs(30)).await;
    let all = fixture
        .with_open(libc::O_RDONLY, move |client, file| scan(&client, &file))
        .await;
    assert_eq!(all, content());
    assert!(operations(&fixture.registry, "prepare") >= 3);
    assert_eq!(operations(&fixture.registry, "release"), 0);
    assert_eq!(fixture.home_quarantines(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_parked_release_is_flushed_on_its_own() {
    let fixture = parking_fixture(Duration::from_millis(300)).await;
    let first = fixture
        .with_open(libc::O_RDONLY, move |client, file| {
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
    let file = fixture
        .with_open(libc::O_RDONLY, move |client, file| {
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
    let next = fixture
        .with_client(move |client| read(&client, &file, SLOT as u64, 1000).unwrap())
        .await;
    assert_eq!(next, content()[SLOT..SLOT + 1000]);
    // Closing with the window still pinned would have quarantined Home.
    assert_eq!(fixture.home_quarantines(), 0);
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
    let file = fixture
        .with_open(libc::O_RDONLY, move |client, file| {
            assert_eq!(read(&client, &file, 0, 1000).unwrap(), content()[..1000]);
            file
        })
        .await;
    assert_eq!(fixture.negotiations(), 1);
    let pool = fixture.reader.home_pool("node-a").unwrap();
    pool.slots[0].try_lock().unwrap().touched = Some(Instant::now() - Duration::from_secs(2));
    let next = fixture
        .with_client(move |client| read(&client, &file, SLOT as u64, 1000).unwrap())
        .await;
    assert_eq!(next, content()[SLOT..SLOT + 1000]);
    assert_eq!(fixture.negotiations(), 2);
    assert_eq!(fixture.home_quarantines(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_slot_is_never_delivered() {
    let fixture = fixture(policy(DataMode::Hixl), true).await;
    fixture.native.corrupt.store(true, Ordering::SeqCst);
    let error = fixture.read_error("file", 0, 1000).await;
    assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
    fixture.wait_released().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_the_fetch_waiter_keeps_exports_until_the_native_read_finishes() {
    let fixture = parking_fixture(RELEASE_PARK).await;
    let file = {
        fixture
            .with_client(move |client| open(&client, libc::O_RDONLY))
            .await
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
    assert_eq!(fixture.native_read_batches(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_window_is_an_open_snapshot_until_this_node_changes_the_file() {
    let fixture = fixture(policy(DataMode::Hixl), true).await;
    let source = fixture.source.clone();
    let (cached, refreshed) = fixture
        .with_open(libc::O_RDONLY, move |client, file| {
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
    assert_eq!(fixture.native_read_batches(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prefetch_serves_the_next_window_without_a_foreground_fetch() {
    let mut prefetch = policy(DataMode::Hixl);
    prefetch.max_inflight = 2;
    let fixture = fixture(prefetch, true).await;
    let all = fixture
        .with_open(libc::O_RDONLY, move |client, file| scan(&client, &file))
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
    let first = fixture
        .with_open(libc::O_RDONLY, move |client, file| {
            read(&client, &file, 0, 100).unwrap()
        })
        .await;
    assert_eq!(first, content()[..100]);
    let mut started = false;
    for _ in 0..200 {
        if fixture.native_read_batches() >= 2 {
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
    // The first READ misses and installs the window; the next two hit it.
    let (first, second, third) = fixture
        .with_open(libc::O_RDONLY, |client, file| {
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
    assert_eq!(fixture.native_read_batches(), 1);
    fixture.wait_released().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_window_reply_keeps_the_budget_after_its_open_is_released() {
    let fixture = fixture(policy(DataMode::Hixl), true).await;
    let reply = fixture
        .with_open(libc::O_RDONLY, move |client, file| {
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
    let file = fixture
        .with_open(libc::O_RDONLY, move |client, file| {
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
    fixture
        .with_open(libc::O_RDONLY, move |client, file| {
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
    let all = fixture
        .with_open(libc::O_RDONLY, move |client, file| scan(&client, &file))
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
    let all = fixture
        .with_open(libc::O_RDONLY, move |client, file| scan(&client, &file))
        .await;
    assert_eq!(all, content());
    assert_eq!(fixture.grpc_reads(), 0);
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
    let all = fixture
        .with_open(libc::O_RDONLY, move |client, file| scan(&client, &file))
        .await;
    assert_eq!(all, content());
    assert_eq!(fixture.grpc_reads(), 0);
    fixture.wait_released().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_truncating_open_drops_windows_of_earlier_opens() {
    let fixture = fixture(policy(DataMode::Auto), true).await;
    let source = fixture.source.clone();
    let (cached, refreshed) = fixture
        .with_open(libc::O_RDONLY, move |client, file| {
            read(&client, &file, 0, 1000).unwrap();
            source.data.lock().unwrap()[..SLOT].fill(0xee);
            let cached = read(&client, &file, 1000, 1000).unwrap();
            let _truncating = open(&client, libc::O_WRONLY | libc::O_TRUNC);
            // Refill the file so a fresh read sees bytes distinct from the cached window.
            source.data.lock().unwrap().resize(FILE_BYTES, 0xee);
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
    let reader = fixture.client.hixl_read_session().unwrap().reader.clone();
    let data = fixture
        .with_client(move |client| {
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
        fixture.grpc_reads(),
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
    let error = fixture.read_error("file", 0, 1000).await;
    assert_eq!(error.code(), afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED);
    assert_eq!(fixture.grpc_reads(), 0);
    drop(busy);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn warming_a_grant_returns_before_the_home_connect_finishes() {
    let fixture = fixture(policy(DataMode::Hixl), true).await;
    fixture.native.hold_connect.store(true, Ordering::SeqCst);
    fixture
        .with_client(move |client| client.warm(&grant()))
        .await;
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
    fixture
        .with_client(move |client| client.warm(&grant()))
        .await;
    fixture
        .wait_until("the warmed session is connected", |fixture| {
            fixture.connects() == 1
        })
        .await;

    let all = fixture
        .with_open(libc::O_RDONLY, move |client, file| scan(&client, &file))
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
    fixture
        .with_client(move |client| {
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

#[tokio::test]
async fn lost_read_prepare_replies_release_window_and_batch_without_native_reads() {
    let fixture = batch_fixture(batch_policy(DataMode::Hixl), true).await;
    let file = fixture
        .with_client(|client| open(&client, libc::O_RDONLY))
        .await;
    let rpc = fixture.client.cloned_client().unwrap();
    let window_request = || FetchRequest {
        home: "node-a".to_owned(),
        client: rpc.clone(),
        access: root_access(&grant()),
        handle: file.handle.clone(),
        offset: 0,
        size: SLOT,
        deadline: Instant::now() + Duration::from_secs(5),
    };
    let batch_request = || batch::BatchRequest {
        home: "node-a".to_owned(),
        client: rpc.clone(),
        access: root_access(&grant()),
        parent_path: b"d".to_vec(),
        anchor: b"f00".to_vec(),
        max_file_bytes: BATCH_SLOT as u64,
        deadline: Instant::now() + Duration::from_secs(5),
    };
    fixture
        .lost_read_prepare_replies
        .store(PREPARE_ATTEMPTS, Ordering::SeqCst);
    let window_error = fixture
        .reader
        .clone()
        .fetch(window_request(), None)
        .await
        .err()
        .unwrap();
    assert_eq!(window_error.kind(), afs_error::ErrorKind::Unavailable);
    assert_eq!(fixture.lost_read_prepare_replies.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.exports.active(), 0);

    fixture
        .lost_read_prepare_replies
        .store(PREPARE_ATTEMPTS, Ordering::SeqCst);
    let batch_error = fixture
        .reader
        .clone()
        .fetch_batch(batch_request())
        .await
        .err()
        .unwrap();
    assert_eq!(batch_error.kind(), afs_error::ErrorKind::Unavailable);
    assert_eq!(fixture.lost_read_prepare_replies.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.exports.active(), 0);
    assert_eq!(fixture.native_read_batches(), 0);
    assert_eq!(fixture.home_quarantines(), 0);

    let fetched = fixture
        .reader
        .clone()
        .fetch(window_request(), None)
        .await
        .unwrap();
    let Fetched::Window(window) = fetched else {
        panic!("recovered session should serve a window");
    };
    assert_eq!(window.bytes.as_ref(), &content()[..SLOT]);
    assert_eq!(fixture.negotiations(), 1);
}

#[tokio::test]
async fn unsupported_batch_leaves_the_session_available_for_a_window() {
    let fixture = batch_fixture(batch_policy(DataMode::Auto), true).await;
    fixture
        .exports
        .fail_export_unsupported_at
        .store(1, Ordering::SeqCst);
    let rpc = fixture.client.cloned_client().unwrap();
    let result = fixture
        .reader
        .clone()
        .fetch_batch(batch::BatchRequest {
            home: "node-a".to_owned(),
            client: rpc.clone(),
            access: root_access(&grant()),
            parent_path: b"d".to_vec(),
            anchor: b"f00".to_vec(),
            max_file_bytes: BATCH_SLOT as u64,
            deadline: Instant::now() + Duration::from_secs(5),
        })
        .await
        .unwrap();
    assert!(matches!(result, batch::BatchFetched::Unavailable));

    let file = fixture
        .with_client(move |client| open(&client, libc::O_RDONLY))
        .await;
    let result = fixture
        .reader
        .clone()
        .fetch(
            FetchRequest {
                home: "node-a".to_owned(),
                client: rpc,
                access: root_access(&grant()),
                handle: file.handle,
                offset: 0,
                size: SLOT,
                deadline: Instant::now() + Duration::from_secs(5),
            },
            None,
        )
        .await
        .unwrap();
    let Fetched::Window(window) = result else {
        panic!("the existing session should still serve a window");
    };
    assert_eq!(window.bytes.as_ref(), &content()[..SLOT]);
    assert_eq!(fixture.negotiations(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequential_small_files_are_served_from_directory_batches() {
    let fixture = batch_fixture(batch_policy(DataMode::Auto), true).await;
    let files = fixture
        .with_client(move |client| {
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
    assert_eq!(fixture.native_read_batches(), 2);
    assert_eq!(fixture.reads("batch_fetch"), 1);
    assert_eq!(fixture.reads("fetch"), 0, "no single-file window");
    assert_eq!(fixture.grpc_reads(), 0);
    assert!(fixture.reads("batch_hit") >= 16 * 3);
    fixture.wait_released().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_hit_is_a_slice_of_the_batch_buffer() {
    let fixture = batch_fixture(batch_policy(DataMode::Hixl), true).await;
    fixture
        .with_client(move |client| {
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
    assert_eq!(fixture.native_read_batches(), 1);
    assert_eq!(fixture.reads("fetch"), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_that_sees_a_newer_version_does_not_use_the_batch() {
    let fixture = batch_fixture(batch_policy(DataMode::Auto), true).await;
    let source = fixture.source.clone();
    let rewritten = vec![9_u8; BATCH_FILE];
    let expected = rewritten.clone();
    let data = fixture
        .with_client(move |client| {
            assert_eq!(read_file(&client, "d/f00"), batch_content(0));
            // Home rewrites f01 after the batch packed it.
            source.tree.write("d/f01", rewritten);
            read_file(&client, "d/f01")
        })
        .await;
    assert_eq!(data, expected);
    assert!(fixture.grpc_reads() > 0);
    assert!(fixture.reads("batch_fallback") > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_local_write_drops_the_files_batch_slice() {
    let fixture = batch_fixture(batch_policy(DataMode::Auto), true).await;
    fixture
        .with_client(move |client| {
            assert_eq!(read_file(&client, "d/f00"), batch_content(0));
            client
                .hixl_read_session()
                .unwrap()
                .invalidate(b"identity:d/f01");
            assert_eq!(read_file(&client, "d/f01"), batch_content(1));
        })
        .await;
    assert!(fixture.grpc_reads() > 0);
    assert_eq!(fixture.native_read_batches(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backward_misses_do_not_start_batches() {
    let fixture = batch_fixture(batch_policy(DataMode::Auto), true).await;
    fixture
        .with_client(move |client| {
            assert_eq!(read_file(&client, "d/f05"), batch_content(5));
            assert_eq!(read_file(&client, "d/f02"), batch_content(2));
        })
        .await;
    assert_eq!(fixture.native_read_batches(), 1);
    assert_eq!(fixture.reads("batch_fetch"), 1);
    assert!(fixture.grpc_reads() > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_files_and_disabled_batches_keep_their_paths() {
    let fixture = batch_fixture(batch_policy(DataMode::Hixl), true).await;
    fixture
        .source
        .tree
        .insert("d/large", vec![3; BATCH_SLOT + 1]);
    let data = fixture
        .with_client(move |client| read_file(&client, "d/large"))
        .await;
    assert_eq!(data, vec![3; BATCH_SLOT + 1]);
    assert_eq!(fixture.reads("batch_fetch"), 0);
    assert!(fixture.reads("fetch") > 0);

    let mut off = batch_policy(DataMode::Auto);
    off.batch_max_file_bytes = 0;
    let fixture = batch_fixture(off, true).await;
    let data = fixture
        .with_client(move |client| read_file(&client, "d/f00"))
        .await;
    assert_eq!(data, batch_content(0));
    assert_eq!(fixture.native_read_batches(), 0);
    assert!(fixture.grpc_reads() > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_files_fall_back_per_mode_when_home_lacks_hixl() {
    let fixture = batch_fixture(batch_policy(DataMode::Auto), false).await;
    let data = fixture
        .with_client(move |client| read_file(&client, "d/f00"))
        .await;
    assert_eq!(data, batch_content(0));
    assert!(fixture.grpc_reads() > 0);

    let fixture = batch_fixture(batch_policy(DataMode::Hixl), false).await;
    let error = fixture.read_error("d/f00", 0, 1000).await;
    assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNSUPPORTED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_propagates_a_directory_batch_checksum_error_without_grpc() {
    let fixture = batch_fixture(batch_policy(DataMode::Auto), true).await;
    fixture.native.corrupt.store(true, Ordering::SeqCst);
    let error = fixture.read_error("d/f00", 0, 1000).await;
    assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
    assert_eq!(fixture.grpc_reads(), 0);
    fixture.wait_released().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_returns_backpressure_when_the_directory_batch_budget_is_full() {
    let mut policy = batch_policy(DataMode::Auto);
    policy.host_budget_bytes = 1;
    let fixture = batch_fixture(policy, true).await;
    let error = fixture.read_error("d/f00", 0, 1000).await;
    assert_eq!(error.code(), afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED);
    assert_eq!(fixture.grpc_reads(), 0);
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
                (name.as_bytes().to_vec(), version(*length as u64), range)
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

fn begin_test_batch(
    store: &mut batch::BatchStore,
    budget: &Arc<WindowBudget>,
    anchor: &str,
    capacity: usize,
) -> batch::BatchStart {
    store
        .begin(b"root-a\0d", anchor.as_bytes(), 4096, capacity, budget)
        .unwrap()
}

#[test]
fn a_file_written_while_its_batch_is_in_flight_is_not_published() {
    let budget = Arc::new(WindowBudget::new(1024 * 1024));
    let mut store = batch::BatchStore::default();
    let start = begin_test_batch(&mut store, &budget, "a", 64 * 1024);
    store.invalidate(b"a");
    store.finish(
        b"root-a\0d",
        start,
        Ok(Some(landed(&[("a", 100), ("b", 100)]))),
    );
    assert!(matches!(
        store.claim(b"a", &dir_route("d", "a", 100), false),
        Claim::Miss
    ));
    assert!(matches!(
        store.claim(b"b", &dir_route("d", "b", 100), false),
        Claim::Hit(..)
    ));
}

#[test]
fn claimed_batch_slices_keep_the_budget_and_a_stale_version_misses() {
    let budget = Arc::new(WindowBudget::new(1024 * 1024));
    let mut store = batch::BatchStore::default();
    let start = begin_test_batch(&mut store, &budget, "a", 64 * 1024);
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
        store.claim(b"a", &dir_route("d", "a", 101), false),
        Claim::Miss
    ));
    let Claim::Hit(bytes, lookahead) = store.claim(b"b", &dir_route("d", "b", 100), false) else {
        panic!("b is resident");
    };
    assert_eq!(bytes.len(), 100);
    assert_eq!(lookahead, Some(b"z".to_vec()), "a quarter is claimed");
    // No lookahead started, so the next hit asks again.
    let Claim::Hit(last, lookahead) = store.claim(b"c", &dir_route("d", "c", 100), false) else {
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
    let start = begin_test_batch(&mut store, &budget, "a", 4096);
    store.finish(
        b"root-a\0d",
        start,
        Ok(Some(landed(&[("a", 1024), ("b", 1024)]))),
    );
    let Claim::Hit(bytes, _) = store.claim(b"a", &dir_route("d", "a", 1024), false) else {
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
        store.claim(b"x", &dir_route("d", "m", 100), true),
        Claim::Start
    ));
    let start = begin_test_batch(&mut store, &budget, "m", 64 * 1024);
    // In flight: a later name waits, an earlier one does not.
    assert!(matches!(
        store.claim(b"x", &dir_route("d", "n", 100), true),
        Claim::Wait(_)
    ));
    assert!(matches!(
        store.claim(b"x", &dir_route("d", "a", 100), true),
        Claim::Miss
    ));
    store.finish(b"root-a\0d", start, Ok(Some(landed(&[("m", 100)]))));
    assert!(matches!(
        store.claim(b"x", &dir_route("d", "q", 100), true),
        Claim::Miss
    ));
    assert!(matches!(
        store.claim(b"x", &dir_route("d", "z", 100), true),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_writer_uses_write_size_before_prepare_or_reusing_a_window() {
    let threshold = crate::config::HixlConfig::default().ownerfs_min_write_bytes;
    assert_eq!(threshold, 512 * 1024);
    let mut auto = policy(DataMode::Auto);
    auto.min_write_bytes = threshold;
    let fixture = fixture_windows(
        auto,
        true,
        MemorySource::new(content(), threshold),
        threshold,
        threshold,
    )
    .await;

    let start = FILE_BYTES as u64;
    let below = vec![0x39; threshold - 1];
    let sent = below.clone();
    let file = fixture
        .with_open(libc::O_WRONLY, move |peer, file| {
            assert_eq!(
                peer.write(&grant(), &file, start, &sent).unwrap(),
                sent.len(),
            );
            file
        })
        .await;
    assert_eq!(fixture.handler.grpc_writes.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.write_prepares.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.native_writes(), 0);

    let equal_offset = start + below.len() as u64;
    let equal = vec![0x55; threshold];
    let sent = equal.clone();
    let file = fixture
        .with_client(move |peer| {
            assert_eq!(
                peer.write(&grant(), &file, equal_offset, &sent).unwrap(),
                sent.len(),
            );
            file
        })
        .await;
    assert_eq!(fixture.handler.grpc_writes.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.write_prepares.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.native_writes(), 1);

    let slot = fixture.reader.home_pool("node-a").unwrap().slots[0].clone();
    let token = slot
        .lock()
        .await
        .write_window
        .as_ref()
        .unwrap()
        .token()
        .to_owned();
    let tail = b"inline tail";
    fixture
        .with_client(move |peer| {
            let tail_offset = equal_offset + threshold as u64;
            assert_eq!(
                peer.write(&grant(), &file, tail_offset, tail).unwrap(),
                tail.len(),
            );
        })
        .await;
    assert_eq!(fixture.handler.grpc_writes.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.write_prepares.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.native_writes(), 1);
    assert_eq!(
        slot.lock().await.write_window.as_ref().unwrap().token(),
        token,
    );
    assert_eq!(
        *fixture.source.data.lock().unwrap(),
        [content(), below, equal, tail.to_vec()].concat(),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_hixl_writer_uses_hixl_below_the_auto_threshold() {
    let mut explicit = policy(DataMode::Hixl);
    explicit.min_write_bytes = SLOT;
    let fixture = fixture(explicit, true).await;
    let body = b"small explicit write";
    fixture
        .with_open(libc::O_WRONLY, move |peer, file| {
            assert_eq!(
                peer.write(&grant(), &file, FILE_BYTES as u64, body)
                    .unwrap(),
                body.len(),
            );
        })
        .await;
    assert_eq!(fixture.handler.grpc_writes.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.write_prepares.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.native_writes(), 1);
    assert_eq!(
        *fixture.source.data.lock().unwrap(),
        [content(), body.to_vec()].concat(),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_without_home_hixl_declines_before_prepare() {
    let fixture = fixture(policy(DataMode::Auto), false).await;
    fixture
        .with_open(libc::O_WRONLY, move |peer, file| {
            let hixl = peer.hixl_read_session().unwrap();
            let client = peer.cloned_client().unwrap();
            assert_eq!(
                hixl.write(
                    &peer.runtime,
                    &client,
                    &grant(),
                    &file,
                    0,
                    b"abc",
                    crate::node::vfs::types::WriteOptions::default()
                )
                .unwrap(),
                None,
            );
        })
        .await;
    assert_eq!(fixture.exports.active(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_flushes_parked_read_and_returns_home_short_write() {
    let source = MemorySource::new(content(), 3000);
    let fixture = fixture_with_exports(
        policy(DataMode::Hixl),
        true,
        source,
        Arc::new(MemoryExports::new(SLOT, 2)),
        SLOT,
        Duration::from_secs(30),
    )
    .await;
    let exports = fixture.exports.clone();
    let input = vec![0x37; SLOT + 17];
    let file_end = content().len() as u64;
    let body = input.clone();
    fixture
        .with_open(libc::O_RDWR, move |peer, file| {
            assert_eq!(read(&peer, &file, 0, 1000).unwrap(), content()[..1000]);
            assert!(
                exports.active() > 0,
                "the read release must still be parked"
            );
            assert_eq!(
                peer.write_with_options(
                    &grant(),
                    &file,
                    file_end,
                    &body,
                    crate::node::vfs::types::WriteOptions { kill_suidgid: true }
                )
                .unwrap(),
                3000,
            );
            assert_eq!(
                read(&peer, &file, file_end, body.len()).unwrap(),
                body[..3000]
            );
        })
        .await;
    assert_eq!(fixture.native_writes(), 1);
    let data = fixture.source.data.lock().unwrap();
    assert_eq!(&data[..file_end as usize], &content());
    assert_eq!(&data[file_end as usize..], &input[..3000]);
    assert_eq!(fixture.grpc_reads(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_unknown_completion_quarantines_without_grpc_fallback() {
    let fixture = fixture(policy(DataMode::Auto), true).await;
    fixture.native.unknown_write.store(true, Ordering::SeqCst);
    let error = fixture
        .with_open(libc::O_WRONLY, move |peer, file| {
            peer.write(&grant(), &file, 0, b"uncommitted").unwrap_err()
        })
        .await;
    assert_eq!(
        error.code(),
        afs_error::NODE_OWNER_WRITE_NOT_EXECUTED_TIMEOUT
    );
    assert_eq!(crate::error::errno(&error), libc::ETIMEDOUT);
    assert_eq!(fixture.native_writes(), 1);
    assert_eq!(fixture.native.quarantines.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.home_quarantines(), 1);
    assert_eq!(fixture.exports.active(), 1);
    assert_eq!(*fixture.source.data.lock().unwrap(), content());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_recovers_append_receipt_after_lost_commit_replies() {
    let lost = Arc::new(StdMutex::new(Vec::new()));
    let source = MemorySource::new(content(), 2);
    let fixture = fixture_with_commit_loss(
        policy(DataMode::Auto),
        true,
        source,
        Arc::new(MemoryExports::new(SLOT, 2)),
        SLOT,
        RELEASE_PARK,
        Some(CommitLoss {
            requests: lost.clone(),
            drop_request: false,
            drop_recovery: false,
            late_commit: None,
        }),
    )
    .await;
    let written = fixture
        .with_open(libc::O_WRONLY | libc::O_APPEND, |peer, file| {
            peer.write(&grant(), &file, content().len() as u64, b"once")
                .unwrap()
        })
        .await;
    assert_eq!(written, 2);
    let requests = lost.lock().unwrap();
    assert_eq!(requests.len(), PREPARE_ATTEMPTS);
    assert!(
        requests
            .iter()
            .all(|request| !request.is_empty() && request == &requests[0])
    );
    assert_eq!(fixture.native_writes(), 1);
    assert_eq!(fixture.exports.active(), 0);
    assert_eq!(fixture.home_quarantines(), 0);
    assert_eq!(
        *fixture.source.data.lock().unwrap(),
        [content(), b"on".to_vec()].concat()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_distinguishes_fenced_lost_requests_from_unreachable_recovery() {
    for drop_recovery in [false, true] {
        let fixture = fixture_with_commit_loss(
            policy(DataMode::Auto),
            true,
            MemorySource::new(content(), SLOT),
            Arc::new(MemoryExports::new(SLOT, 2)),
            SLOT,
            RELEASE_PARK,
            Some(CommitLoss {
                requests: Arc::default(),
                drop_request: true,
                drop_recovery,
                late_commit: None,
            }),
        )
        .await;
        let error = fixture
            .with_open(libc::O_WRONLY, |peer, file| {
                peer.write(&grant(), &file, 0, b"uncommitted").unwrap_err()
            })
            .await;
        assert_eq!(
            error.code(),
            if drop_recovery {
                afs_error::NODE_OWNER_WRITE_UNKNOWN
            } else {
                afs_error::NODE_OWNER_WRITE_NOT_EXECUTED
            }
        );
        assert_eq!(fixture.native_writes(), 1);
        assert_eq!(fixture.handler.grpc_writes.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.exports.active(), usize::from(drop_recovery));
        assert_eq!(*fixture.source.data.lock().unwrap(), content());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_after_unknown_commit_observes_late_home_write() {
    assert_late_commit_is_visible("file", false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_read_after_unknown_commit_observes_late_home_write() {
    assert_late_commit_is_visible("d/late", true).await;
}

async fn assert_late_commit_is_visible(path: &'static str, batched: bool) {
    let (policy, slot) = if batched {
        (batch_policy(DataMode::Auto), BATCH_SLOT)
    } else {
        (policy(DataMode::Auto), SLOT)
    };
    let original = if batched { batch_content(0) } else { content() };
    let source = MemorySource::new(original.clone(), slot);
    if batched {
        source.tree.insert_directory("d");
        source.tree.insert(path, original.clone());
    }
    let entered = Arc::new(tokio::sync::Notify::new());
    let resume = source.pause_next_write(entered.clone());
    let (finished, home_committed) = tokio::sync::oneshot::channel();
    let fixture = fixture_with_commit_loss(
        policy,
        true,
        source,
        Arc::new(MemoryExports::new(slot, 8)),
        slot,
        RELEASE_PARK,
        Some(CommitLoss {
            requests: Arc::default(),
            drop_request: false,
            drop_recovery: true,
            late_commit: Some(LateCommit {
                entered,
                finished: StdMutex::new(Some(finished)),
            }),
        }),
    )
    .await;
    const REPLACEMENT: &[u8] = b"late commit";
    let (reader, writer) = fixture
        .with_client(move |client| {
            let reader = open_path(&client, path);
            let writer = client
                .open(&grant(), OsStr::new(path), libc::O_WRONLY, None)
                .unwrap()
                .0;
            assert_ne!(reader.handle, writer.handle);
            assert_eq!(reader.identity, writer.identity);
            (reader, writer)
        })
        .await;
    let error = fixture
        .with_client(move |client| client.write(&grant(), &writer, 0, REPLACEMENT).unwrap_err())
        .await;
    assert_eq!(error.code(), afs_error::NODE_OWNER_WRITE_UNKNOWN);
    assert_eq!(fixture.native_writes(), 1);
    assert_eq!(fixture.handler.grpc_writes.load(Ordering::SeqCst), 0);

    // Exercise both a previously opened FD and one opened while Home's write is pending.
    let (reader, reopened) = fixture
        .with_client(move |client| {
            let reopened = open_path(&client, path);
            for fd in [&reader, &reopened] {
                assert_eq!(
                    read(&client, fd, 0, REPLACEMENT.len()).unwrap(),
                    original[..REPLACEMENT.len()],
                );
            }
            (reader, reopened)
        })
        .await;
    resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), home_committed)
        .await
        .expect("Home Commit must finish after releasing the write gate")
        .unwrap();

    let home_data = if batched {
        fixture.source.tree.get(path.as_bytes()).unwrap().data
    } else {
        fixture.source.data.lock().unwrap().clone()
    };
    assert_eq!(&home_data[..REPLACEMENT.len()], REPLACEMENT);

    fixture
        .with_client(move |client| {
            assert_eq!(
                read(&client, &reader, 0, REPLACEMENT.len()).unwrap(),
                REPLACEMENT
            );
            assert_eq!(
                client
                    .read_reply(&grant(), &reopened, 0, REPLACEMENT.len())
                    .unwrap()
                    .as_ref(),
                REPLACEMENT,
            );
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_recovers_same_handle_after_home_rejects_checksum() {
    let fixture = fixture(policy(DataMode::Hixl), true).await;
    fixture
        .native
        .corrupt_next_write
        .store(true, Ordering::SeqCst);
    let file_end = content().len() as u64;
    fixture
        .with_open(libc::O_RDWR, move |peer, file| {
            let error = peer.write(&grant(), &file, file_end, b"bad").unwrap_err();
            assert_eq!(
                error.code(),
                afs_error::NODE_OWNER_WRITE_NOT_EXECUTED_CHECKSUM
            );
            assert!(crate::node::vfs::ownerfs::remote::write_was_not_executed(
                &error
            ));
            assert_eq!(peer.write(&grant(), &file, file_end, b"ok").unwrap(), 2);
            assert_eq!(read(&peer, &file, file_end, 2).unwrap(), b"ok");
        })
        .await;
    assert_eq!(fixture.native_writes(), 2);
    assert_eq!(
        *fixture.source.data.lock().unwrap(),
        [content(), b"ok".to_vec()].concat()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_rolls_session_after_the_last_allowed_ticket() {
    let fixture = fixture(policy(DataMode::Hixl), true).await;
    let slot = fixture.reader.home_pool("node-a").unwrap().slots[0].clone();
    let file = fixture
        .with_client(move |peer| open(&peer, libc::O_WRONLY))
        .await;
    let session = {
        let mut guard = slot.lock().await;
        guard.write_tickets = MAX_SESSION_WRITE_TICKETS - 1;
        guard.session.as_ref().unwrap().reply.session_id
    };
    let (file, written) = fixture
        .with_client(move |peer| {
            let written = peer.write(&grant(), &file, 0, b"first").unwrap();
            (file, written)
        })
        .await;
    assert_eq!(written, 5);
    {
        let guard = slot.lock().await;
        assert_eq!(guard.write_tickets, MAX_SESSION_WRITE_TICKETS);
        assert_eq!(guard.session.as_ref().unwrap().reply.session_id, session);
    }
    assert_eq!(
        fixture
            .with_client(move |peer| peer.write(&grant(), &file, 5, b"second").unwrap())
            .await,
        6
    );
    let guard = slot.lock().await;
    assert_eq!(guard.write_tickets, 1);
    assert_ne!(guard.session.as_ref().unwrap().reply.session_id, session);
    assert_eq!(fixture.native_writes(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_reuses_one_session_window_for_alternating_sizes() {
    let fixture = fixture_with_exports(
        policy(DataMode::Hixl),
        true,
        MemorySource::new(content(), 2 * SLOT),
        Arc::new(MemoryExports::new(SLOT, 2)),
        SLOT,
        Duration::from_secs(1),
    )
    .await;
    let slot = fixture.reader.home_pool("node-a").unwrap().slots[0].clone();
    let (file, next_offset) = fixture
        .with_open(libc::O_WRONLY, move |peer, file| {
            let start = FILE_BYTES as u64;
            assert_eq!(peer.write(&grant(), &file, start, b"small").unwrap(), 5);
            (file, start + 5)
        })
        .await;
    let first = slot
        .lock()
        .await
        .write_window
        .as_ref()
        .unwrap()
        .token()
        .to_owned();
    let payload = vec![0x59; SLOT + 1];
    let sent = payload.clone();
    let file = fixture
        .with_client(move |peer| {
            assert_eq!(
                peer.write(&grant(), &file, next_offset, &sent).unwrap(),
                sent.len()
            );
            file
        })
        .await;
    let reserved = fixture.exports.exports.load(Ordering::SeqCst);
    assert_eq!(
        reserved, 3,
        "one-slot grant is returned before the two-slot Prepare"
    );
    assert_eq!(fixture.exports.releases.load(Ordering::SeqCst), 1);
    let second = slot
        .lock()
        .await
        .write_window
        .as_ref()
        .unwrap()
        .token()
        .to_owned();
    assert_ne!(first, second);
    let sent = payload.clone();
    fixture
        .with_client(move |peer| {
            let offset = next_offset + sent.len() as u64;
            assert_eq!(peer.write(&grant(), &file, offset, b"end").unwrap(), 3);
            assert_eq!(
                peer.write(&grant(), &file, offset + 3, &sent).unwrap(),
                sent.len()
            );
            peer.release(&grant(), file).unwrap();
        })
        .await;
    assert_eq!(fixture.native_writes(), 4);
    assert_eq!(
        fixture.exports.exports.load(Ordering::SeqCst),
        reserved,
        "smaller writes preserve the capacity used by the next large write"
    );
    assert_eq!(fixture.exports.active(), 0);
    assert_eq!(
        *fixture.source.data.lock().unwrap(),
        [
            content(),
            b"small".to_vec(),
            payload.clone(),
            b"end".to_vec(),
            payload
        ]
        .concat()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_returns_unconsumed_windows_on_read_close_and_idle() {
    for cleanup in ["read", "close", "idle"] {
        let park = if cleanup == "idle" {
            Duration::from_millis(150)
        } else {
            Duration::from_secs(30)
        };
        let fixture = fixture_with_exports(
            policy(DataMode::Hixl),
            true,
            MemorySource::new(content(), SLOT),
            Arc::new(MemoryExports::new(SLOT, 2)),
            SLOT,
            park,
        )
        .await;
        let slot = fixture.reader.home_pool("node-a").unwrap().slots[0].clone();
        let file = fixture
            .with_open(libc::O_RDWR, move |peer, file| {
                assert_eq!(peer.write(&grant(), &file, 0, b"body").unwrap(), 4);
                file
            })
            .await;
        assert!(slot.lock().await.write_window.is_some());
        match cleanup {
            "read" => {
                fixture
                    .with_client(move |peer| {
                        assert_eq!(read(&peer, &file, 0, 4).unwrap(), b"body");
                    })
                    .await
            }
            "close" => {
                fixture
                    .with_client(move |peer| peer.release(&grant(), file).unwrap())
                    .await
            }
            _ => {}
        }
        if cleanup != "read" {
            fixture.wait_released().await;
        }
        assert!(slot.lock().await.write_window.is_none());
        assert_eq!(fixture.home_quarantines(), 0);
    }
}
