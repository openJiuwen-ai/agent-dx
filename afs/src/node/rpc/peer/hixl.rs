//! Reader-initiated immutable DFS Host-to-Host transfers.

use super::{
    GrpcChunkTransfer, PeerConnectionPool, PeerRuntime, dfs_corrupt_error, dfs_protocol_error,
    wire_dfs_read_grant,
};
use crate::node::rpc::hixl::batch::{pack_read_batch, pack_read_group};
use crate::{
    config::HixlDfsReadMode,
    node::{
        dfs_read::{ChunkTransfer, DfsReadPhaseMetrics, PeerReadBatch, ReadGroupPolicy},
        rpc::hixl::{
            HIXL_PROTOCOL_VERSION, HixlMetrics, Operation, failed_read_outcome,
            is_refused as is_refused_error, is_unsupported, observe_optional, runtime_error,
            uncertain_read,
        },
    },
};
use afs_protocol::node_data::{
    DfsCloseHixlRequest, DfsDigestAlgorithm, DfsHixlGroupWindow, DfsHixlLane,
    DfsHixlReleaseOutcome, DfsHixlReleaseWindow, DfsHixlTransport, DfsNegotiateHixlReply,
    DfsNegotiateHixlRequest, DfsPrepareReadRangesHixlGroupRequest, DfsPrepareReadRangesHixlReply,
    DfsReadRangesRequest, DfsReleaseHixlReadRequest, dfs_chunks_client::DfsChunksClient,
};
use afs_transport::{
    grpc::error_status::status_to_error,
    hixl::{EngineInfo, HixlError, HixlReadOutcome, HixlRuntime, HixlRuntimeGroup, ReadTransport},
};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, OwnedMutexGuard, watch};
use tonic::transport::Channel;

#[derive(Clone)]
struct ReadWindow {
    request: DfsReadRangesRequest,
    mappings: Vec<(usize, std::ops::Range<usize>)>,
    length: usize,
}

type PlannedWindow = (ReadWindow, u64, usize);
type PreparedWindow = (ReadWindow, DfsPrepareReadRangesHixlReply, usize);

/// Native calls are the only replaceable boundary in the reader control path.
pub(super) trait ReadLane: Send + Sync {
    fn connect(&self, peer: &str, deadline: Instant) -> Result<(), HixlError>;
    /// Allocates the private output for one native multi-window batch.
    fn prepare_read_batch_buffer(&self, length: usize) -> Result<Vec<u8>, HixlError>;
    /// Submits every window through one native SDK batch call.
    fn read_batch_timed_into(
        &self,
        peer: &str,
        windows: &[(u64, usize)],
        output: Vec<u8>,
        deadline: Instant,
    ) -> Result<HixlReadOutcome, HixlError>;
}

impl ReadLane for HixlRuntime {
    fn connect(&self, peer: &str, deadline: Instant) -> Result<(), HixlError> {
        self.connect(peer, deadline)
    }

    fn prepare_read_batch_buffer(&self, length: usize) -> Result<Vec<u8>, HixlError> {
        HixlRuntime::prepare_read_batch_buffer(self, length)
    }

    fn read_batch_timed_into(
        &self,
        peer: &str,
        windows: &[(u64, usize)],
        output: Vec<u8>,
        deadline: Instant,
    ) -> Result<HixlReadOutcome, HixlError> {
        self.read_batch_timed_into(peer, windows, output, deadline)
    }
}

pub(super) trait ReadRuntime: Send + Sync {
    fn info(&self) -> EngineInfo;
    fn infos(&self) -> Vec<EngineInfo>;
    fn lane_count(&self) -> usize;
    fn lane(&self, lane: usize) -> Option<Arc<dyn ReadLane>>;
    fn quarantine(&self) -> Result<(), HixlError>;
}

impl ReadRuntime for HixlRuntimeGroup {
    fn info(&self) -> EngineInfo {
        self.info()
    }

    fn infos(&self) -> Vec<EngineInfo> {
        self.infos()
    }

    fn lane_count(&self) -> usize {
        self.len()
    }

    fn lane(&self, lane: usize) -> Option<Arc<dyn ReadLane>> {
        self.lane(lane).map(|runtime| runtime as Arc<dyn ReadLane>)
    }

    fn quarantine(&self) -> Result<(), HixlError> {
        self.quarantine()
    }
}

fn pack_windows_with_ops(
    request: DfsReadRangesRequest,
    capacity: usize,
    max_ops: usize,
) -> afs_error::Result<Vec<ReadWindow>> {
    if capacity == 0 || capacity > 4 * 1024 * 1024 || request.operations.is_empty() {
        return Err(dfs_protocol_error(
            "HIXL packed-window capacity or operations are invalid",
        ));
    }
    let mut windows = Vec::new();
    let base = DfsReadRangesRequest {
        operations: Vec::new(),
        ..request.clone()
    };
    let mut current = ReadWindow {
        request: base.clone(),
        mappings: Vec::new(),
        length: 0,
    };
    for operation in request.operations {
        let total = usize::try_from(operation.length)
            .map_err(|_| dfs_protocol_error("HIXL range length overflow"))?;
        let destination = usize::try_from(operation.destination_offset)
            .map_err(|_| dfs_protocol_error("HIXL scatter offset overflow"))?;
        if total == 0 {
            return Err(dfs_protocol_error("HIXL range is empty"));
        }
        let mut offset = 0;
        while offset < total {
            if current.length == capacity || current.request.operations.len() == max_ops {
                current.request.attempt_id = format!("{}-hixl-{}", base.attempt_id, windows.len());
                windows.push(current);
                current = ReadWindow {
                    request: base.clone(),
                    mappings: Vec::new(),
                    length: 0,
                };
            }
            let count = (total - offset).min(capacity - current.length);
            let mut part = operation.clone();
            part.operation_index = u32::try_from(current.request.operations.len())
                .map_err(|_| dfs_protocol_error("HIXL operation index overflow"))?;
            part.chunk_offset = operation
                .chunk_offset
                .checked_add(
                    u64::try_from(offset)
                        .map_err(|_| dfs_protocol_error("HIXL chunk offset overflow"))?,
                )
                .ok_or_else(|| dfs_protocol_error("HIXL chunk range overflow"))?;
            part.length =
                u64::try_from(count).map_err(|_| dfs_protocol_error("HIXL length overflow"))?;
            part.destination_offset = u64::try_from(current.length)
                .map_err(|_| dfs_protocol_error("HIXL packed offset overflow"))?;
            let scatter = destination
                .checked_add(offset)
                .ok_or_else(|| dfs_protocol_error("HIXL scatter range overflow"))?;
            current.request.operations.push(part);
            current
                .mappings
                .push((scatter, current.length..current.length + count));
            current.length += count;
            offset += count;
        }
    }
    current.request.attempt_id = format!("{}-hixl-{}", base.attempt_id, windows.len());
    windows.push(current);
    Ok(windows)
}

fn scatter_prepared(chunks: Vec<PreparedChunk>, out: &mut [u8]) -> afs_error::Result<()> {
    // Every window was validated before this point; validate the destinations
    // first so a malformed mapping cannot partially publish the batch.
    for chunk in &chunks {
        let mut offset = 0_usize;
        for window in &chunk.windows {
            offset = offset
                .checked_add(window.length)
                .ok_or_else(|| dfs_protocol_error("HIXL prepared length overflow"))?;
            if offset > chunk.bytes.len() {
                return Err(dfs_protocol_error("HIXL prepared chunk exceeds READ"));
            }
            validate_output_scatter(window, out.len())?;
        }
    }
    for chunk in chunks {
        let mut offset = 0_usize;
        for window in chunk.windows {
            let end = offset + window.length;
            scatter_prepared_window(&window, &chunk.bytes[offset..end], out)?;
            offset = end;
        }
    }
    Ok(())
}

fn validate_output_scatter(window: &ReadWindow, output_len: usize) -> afs_error::Result<()> {
    for (destination, range) in &window.mappings {
        if range.start > range.end
            || range.end > window.length
            || destination
                .checked_add(range.len())
                .is_none_or(|end| end > output_len)
        {
            return Err(dfs_protocol_error("HIXL prepared scatter exceeds output"));
        }
    }
    Ok(())
}

fn scatter_prepared_window(
    window: &ReadWindow,
    bytes: &[u8],
    output: &mut [u8],
) -> afs_error::Result<()> {
    for (destination, range) in &window.mappings {
        let end = destination
            .checked_add(range.len())
            .ok_or_else(|| dfs_protocol_error("HIXL prepared scatter overflow"))?;
        let target = output
            .get_mut(*destination..end)
            .ok_or_else(|| dfs_protocol_error("HIXL prepared scatter exceeds output"))?;
        let source = bytes
            .get(range.clone())
            .ok_or_else(|| dfs_protocol_error("HIXL prepared scatter exceeds READ"))?;
        target.copy_from_slice(source);
    }
    Ok(())
}

fn validate_prepare(
    window: &ReadWindow,
    session: &DfsNegotiateHixlReply,
    reply: &DfsPrepareReadRangesHixlReply,
    lane_id: usize,
) -> afs_error::Result<()> {
    let lane = session
        .lanes
        .get(lane_id)
        .ok_or_else(|| dfs_protocol_error("HIXL descriptor lane is not negotiated"))?;
    let length = u64::try_from(window.length)
        .map_err(|_| dfs_protocol_error("HIXL window length overflow"))?;
    let address_end = reply
        .address
        .checked_add(reply.capacity)
        .and_then(|end| usize::try_from(end).ok());
    if reply.session_id != session.session_id
        || reply.read_id != window.request.read_id
        || reply.attempt_id != window.request.attempt_id
        || reply.source_node_id != session.source_node_id
        || reply.source_node_epoch != session.source_node_epoch
        || reply.engine_id != lane.engine_id
        || reply.engine_generation != lane.engine_generation
        || reply.pool_generation != lane.engine_generation
        || reply.slot_generation == 0
        || reply.slot_id >= u64::from(lane.export_slots)
        || reply.address == 0
        || address_end.is_none()
        || reply.capacity == 0
        || reply.capacity > lane.window_bytes
        || reply.packed_length != length
        || length > reply.capacity
        || reply.ranges.len() != window.request.operations.len()
        || reply.completions.len() != window.request.operations.len()
    {
        return Err(dfs_protocol_error(
            "HIXL prepared window identity, capacity or address is invalid",
        ));
    }
    for ((op, range), completion) in window
        .request
        .operations
        .iter()
        .zip(&reply.ranges)
        .zip(&reply.completions)
    {
        let blake3_shape = range.range_checksum_algorithm == DfsDigestAlgorithm::Blake3 as i32
            && range.range_checksum.len() == 32;
        let crc32c_shape = range.range_checksum_algorithm == DfsDigestAlgorithm::Crc32c as i32
            && range.range_checksum.len() == 4;
        if range.operation_index != op.operation_index
            || range.chunk_id != op.chunk_id
            || range.chunk_offset != op.chunk_offset
            || range.length != op.length
            || range.packed_offset != op.destination_offset
            || range.source_copy_id != op.source_copy_id
            || !(blake3_shape || crc32c_shape)
            || completion.read_id != window.request.read_id
            || completion.attempt_id != window.request.attempt_id
            || completion.operation_index != op.operation_index
            || completion.source_copy_id != op.source_copy_id
            || completion.transferred_bytes != op.length
            || completion.range_checksum_algorithm != range.range_checksum_algorithm
            || completion.range_checksum != range.range_checksum
        {
            return Err(dfs_protocol_error(
                "HIXL range descriptor or completion differs from the request",
            ));
        }
        if op
            .destination_offset
            .checked_add(op.length)
            .is_none_or(|end| end > length)
        {
            return Err(dfs_protocol_error("HIXL packed range exceeds the window"));
        }
    }
    Ok(())
}

/// Meta-bound identity a CRC32C range must still match when it is promoted.
#[derive(Clone)]
struct CrcExpectation {
    content_digest: crate::dfs::ContentDigest,
    length: u64,
}

impl CrcExpectation {
    fn of(chunk: &crate::dfs::ChunkObject) -> Self {
        Self {
            content_digest: chunk.content_digest.clone(),
            length: chunk.length,
        }
    }
}

/// Descriptors of every CRC32C object carried by one batch.
type CrcLookup = std::collections::HashMap<String, CrcExpectation>;

fn crc_lookup(batch: &PeerReadBatch, extra: Option<&crate::dfs::ChunkObject>) -> Option<CrcLookup> {
    let mut lookup = CrcLookup::new();
    for operation in &batch.operations {
        if let Some(chunk) = &operation.chunk
            && chunk.is_segment_crc32c()
        {
            lookup.insert(chunk.id.0.clone(), CrcExpectation::of(chunk));
        }
    }
    if let Some(chunk) = extra.filter(|chunk| chunk.is_segment_crc32c()) {
        lookup.insert(chunk.id.0.clone(), CrcExpectation::of(chunk));
    }
    (!lookup.is_empty()).then_some(lookup)
}

fn validate_bytes(
    reply: &DfsPrepareReadRangesHixlReply,
    bytes: &[u8],
    crc: Option<&CrcLookup>,
) -> afs_error::Result<()> {
    if usize::try_from(reply.packed_length).ok() != Some(bytes.len()) {
        return Err(dfs_protocol_error("HIXL native READ length differs"));
    }
    for range in &reply.ranges {
        let start = usize::try_from(range.packed_offset)
            .map_err(|_| dfs_protocol_error("HIXL checksum offset overflow"))?;
        let length = usize::try_from(range.length)
            .map_err(|_| dfs_protocol_error("HIXL checksum length overflow"))?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| dfs_protocol_error("HIXL checksum range overflow"))?;
        let data = bytes
            .get(start..end)
            .ok_or_else(|| dfs_protocol_error("HIXL checksum range exceeds READ"))?;
        match range.range_checksum_algorithm {
            algorithm if algorithm == DfsDigestAlgorithm::Blake3 as i32 => {
                if crc.is_some_and(|crc| crc.contains_key(&range.chunk_id)) {
                    return Err(dfs_protocol_error("HIXL CRC range is labelled as BLAKE3"));
                }
                if range.range_checksum.as_slice() != blake3::hash(data).as_bytes() {
                    return Err(dfs_corrupt_error("HIXL range checksum differs"));
                }
            }
            algorithm if algorithm == DfsDigestAlgorithm::Crc32c as i32 => {
                let Some(expected) = crc.and_then(|crc| crc.get(&range.chunk_id)) else {
                    return Err(dfs_protocol_error(
                        "HIXL CRC32C range has no Meta-bound descriptor",
                    ));
                };
                let checksum = crate::node::chunk_crc32c::checksum(data).to_le_bytes();
                if range.range_checksum.as_slice() != checksum {
                    return Err(dfs_corrupt_error("HIXL CRC range checksum differs"));
                }
                validate_crc_descriptor(range, data, expected)?;
            }
            _ => {
                return Err(dfs_protocol_error(
                    "HIXL range checksum algorithm is unsupported",
                ));
            }
        }
    }
    Ok(())
}

/// Complete aligned segments bind to the Meta descriptor exactly like the gRPC
/// lane; a window slice that cuts a segment relies on the range checksum above
/// plus the authenticated source's storage verification.
fn validate_crc_descriptor(
    range: &afs_protocol::node_data::DfsHixlReadRange,
    data: &[u8],
    expected: &CrcExpectation,
) -> afs_error::Result<()> {
    let segment = crate::node::chunk_crc32c::SEGMENT as u64;
    let complete = range.chunk_offset.is_multiple_of(segment)
        && (range.length.is_multiple_of(segment)
            || range.chunk_offset.checked_add(range.length) == Some(expected.length));
    if !complete {
        if !range.crc32c_table.is_empty() {
            return Err(dfs_protocol_error("HIXL CRC table on a partial range"));
        }
        return Ok(());
    }
    if range.crc32c_table.is_empty() || range.crc32c_table.len() > 4096 {
        return Err(dfs_protocol_error("HIXL CRC descriptor missing"));
    }
    let table: crate::node::chunk_crc32c::CrcTable = serde_json::from_slice(&range.crc32c_table)
        .map_err(|_| dfs_corrupt_error("HIXL CRC descriptor is invalid"))?;
    if !table.valid_shape(expected.length) || table.descriptor_digest() != expected.content_digest {
        return Err(dfs_corrupt_error("HIXL CRC descriptor differs from Meta"));
    }
    let base = usize::try_from(range.chunk_offset / segment)
        .map_err(|_| dfs_protocol_error("HIXL CRC segment index overflow"))?;
    for (relative, bytes) in data.chunks(crate::node::chunk_crc32c::SEGMENT).enumerate() {
        if table.checksums.get(base + relative) != Some(&crate::node::chunk_crc32c::checksum(bytes))
        {
            return Err(dfs_corrupt_error("HIXL CRC segment differs from Meta"));
        }
    }
    Ok(())
}

/// Bounded hash pool for native window verification. Every window is hashed
/// independently, so a group completes in roughly one window's hash time
/// instead of the sum of its windows.
pub(crate) fn validation_pool() -> Option<&'static rayon_core::ThreadPool> {
    static POOL: std::sync::OnceLock<Option<rayon_core::ThreadPool>> = std::sync::OnceLock::new();
    POOL.get_or_init(|| {
        let workers = std::thread::available_parallelism()
            .map_or(4, |parallel| parallel.get())
            .clamp(1, 8);
        rayon_core::ThreadPoolBuilder::new()
            .num_threads(workers)
            .thread_name(|index| format!("afs-hixl-validate-{index}"))
            .build()
            .ok()
    })
    .as_ref()
}

/// Validates each window against its prepared checksum. Results keep request
/// order; the caller publishes only after every digest matches.
fn validate_windows(
    windows: &[(&DfsPrepareReadRangesHixlReply, &[u8])],
    crc: Option<&CrcLookup>,
) -> Vec<(Duration, afs_error::Result<()>)> {
    let slots = StdMutex::new(
        windows
            .iter()
            .map(|_| None)
            .collect::<Vec<Option<(Duration, afs_error::Result<()>)>>>(),
    );
    let validate = |index: usize| {
        let (reply, bytes) = windows[index];
        let started = Instant::now();
        let result = validate_bytes(reply, bytes, crc);
        let elapsed = started.elapsed();
        match slots.lock() {
            Ok(mut slots) => slots[index] = Some((elapsed, result)),
            Err(poisoned) => poisoned.into_inner()[index] = Some((elapsed, result)),
        }
    };
    match validation_pool() {
        Some(pool) => pool.install(|| {
            rayon_core::scope(|scope| {
                for index in 0..windows.len() {
                    scope.spawn(move |_| validate(index));
                }
            });
        }),
        None => (0..windows.len()).for_each(validate),
    }
    let slots = slots
        .into_inner()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    slots
        .into_iter()
        .map(|slot| {
            slot.unwrap_or_else(|| {
                (
                    Duration::ZERO,
                    Err(dfs_protocol_error("HIXL window validation did not run")),
                )
            })
        })
        .collect()
}

fn may_fallback(mode: HixlDfsReadMode, error: &afs_error::Error) -> bool {
    mode == HixlDfsReadMode::Preferred && is_unsupported(error)
}

const MAX_HIXL_PEERS: usize = 16;
const MAX_HIXL_ENGINES: usize = MAX_HIXL_PEERS * 16;
/// A source that refused HIXL admission in preferred mode (its generation is
/// quarantined or draining) is not negotiated again for this long; reads from
/// it stay on gRPC meanwhile instead of each starting a warm negotiation.
const REFUSED_RETRY: Duration = Duration::from_secs(60);

/// Sources close a control session after 30 s without activity, so a reader
/// renegotiates a session idle this long instead of finding it closed.
const SESSION_REUSE_LIMIT: Duration = Duration::from_secs(20);

fn session_idle_limit(idle_ms: u64) -> Duration {
    Duration::from_millis(idle_ms).min(SESSION_REUSE_LIMIT)
}

type PeerKey = (String, u64, String);
type PeerSlots = Arc<Mutex<HashMap<PeerKey, Arc<Mutex<PeerSlot>>>>>;
type EngineIdentity = (String, u64, u64);
type EngineBindings = Arc<Mutex<HashMap<SocketAddr, EngineIdentity>>>;

struct PeerSlot {
    session: Option<Arc<PeerSession>>,
    touched: Instant,
    /// Present while a detached warm task negotiates and connects this peer.
    /// Waiters subscribe instead of starting a second native connection.
    warming: Option<watch::Sender<bool>>,
}
struct PeerSession {
    reply: DfsNegotiateHixlReply,
    caller_epoch: u64,
    client: DfsChunksClient<Channel>,
    poisoned: Arc<AtomicBool>,
}
struct PeerLease {
    session: Arc<PeerSession>,
    guard: OwnedMutexGuard<PeerSlot>,
    /// Negotiated by this acquire rather than reused from an earlier read.
    fresh: bool,
}

enum WarmStart {
    Started(Box<WarmTask>),
    InProgress,
    Skip,
}

#[derive(Clone)]
struct WarmTask {
    context: ReadContext,
    slot: Arc<Mutex<PeerSlot>>,
    node_id: String,
    node_epoch: u64,
    address: String,
    /// `None` pre-warms the transport without a read authority.
    authority: Option<DfsReadRangesRequest>,
    caller_epoch: u64,
}

impl WarmTask {
    async fn run(self) {
        let key = (self.node_id.clone(), self.node_epoch, self.address.clone());
        let deadline = Instant::now().checked_add(self.context.timeout);
        let outcome = match deadline {
            Some(deadline) => {
                self.context
                    .establish(
                        &self.node_id,
                        self.node_epoch,
                        &self.address,
                        self.authority.as_ref(),
                        self.caller_epoch,
                        deadline,
                    )
                    .await
            }
            None => Err(dfs_protocol_error("HIXL warm deadline overflow")),
        };
        let mut duplicate = None;
        let mut guard = self.slot.lock().await;
        match outcome {
            Ok(Some(session)) if guard.session.is_none() => {
                guard.session = Some(session);
                guard.touched = Instant::now();
            }
            Ok(Some(session)) => duplicate = Some(session),
            _ => {}
        }
        guard.warming = None;
        // A failed or unsupported warm leaves an empty slot that must not
        // count against the peer budget.
        if guard.session.is_none() {
            retire_unbound_peer(&self.context.slots, &key, &self.slot).await;
        }
        drop(guard);
        if let Some(session) = duplicate {
            let _ = close_session(&session, self.context.timeout).await;
        }
    }

    async fn cancel(self) {
        self.slot.lock().await.warming = None;
    }
}

#[derive(Clone)]
struct ReadContext {
    peers: Arc<PeerConnectionPool>,
    native: Arc<dyn ReadRuntime>,
    mode: HixlDfsReadMode,
    timeout: Duration,
    slots: PeerSlots,
    engines: EngineBindings,
    metrics: Option<HixlMetrics>,
    phase_metrics: Option<DfsReadPhaseMetrics>,
    group: Option<ReadGroupPolicy>,
    receive_slots: usize,
    session_idle: Duration,
    /// Pins each peer to one device lane so concurrent peers transfer on
    /// different devices. Peers share a lane once every lane is taken.
    lane_bindings: Arc<StdMutex<HashMap<PeerKey, usize>>>,
    /// Sources that refused admission, with the time they may be negotiated
    /// again. Bounded by `MAX_HIXL_PEERS`; expired entries are dropped.
    refused: Arc<StdMutex<HashMap<PeerKey, Instant>>>,
}

/// Optional reader-pull transport for committed DFS chunks.
/// Construction fails for off mode, zero/excessive deadlines or an invalid runtime.
pub struct HixlChunkTransfer {
    context: ReadContext,
    runtime: PeerRuntime,
    crc: GrpcChunkTransfer,
    next_read: AtomicU64,
}

impl HixlChunkTransfer {
    pub fn new(
        peers: Arc<PeerConnectionPool>,
        native: Arc<HixlRuntimeGroup>,
        mode: HixlDfsReadMode,
        timeout: Duration,
    ) -> afs_error::Result<Self> {
        Self::from_runtime(peers, native, mode, timeout)
    }

    fn from_runtime(
        peers: Arc<PeerConnectionPool>,
        native: Arc<dyn ReadRuntime>,
        mode: HixlDfsReadMode,
        timeout: Duration,
    ) -> afs_error::Result<Self> {
        if mode == HixlDfsReadMode::Off || timeout.is_zero() || timeout > Duration::from_secs(300) {
            return Err(afs_error::Error::coded(
                afs_error::NODE_TRANSFER_INVALID,
                "HIXL mode and deadline are invalid",
            ));
        }
        let crc = GrpcChunkTransfer::new(peers.clone(), timeout)?;
        Ok(Self {
            context: ReadContext {
                peers,
                native,
                mode,
                timeout,
                slots: Arc::new(Mutex::new(HashMap::new())),
                engines: Arc::new(Mutex::new(HashMap::new())),
                metrics: None,
                phase_metrics: None,
                group: None,
                receive_slots: 16,
                session_idle: SESSION_REUSE_LIMIT,
                lane_bindings: Arc::new(StdMutex::new(HashMap::new())),
                refused: Arc::new(StdMutex::new(HashMap::new())),
            },
            runtime: PeerRuntime::current_or_new().map_err(|error| error.0)?,
            crc,
            next_read: AtomicU64::new(1),
        })
    }

    /// Enables a bounded ReadGroup: one native SDK batch carrying every
    /// prepared window of a single lane.
    pub fn with_read_group(mut self, group: ReadGroupPolicy) -> Self {
        self.context.group = Some(group);
        self
    }

    fn grpc_fallback(&self, timeout: Duration) -> afs_error::Result<GrpcChunkTransfer> {
        let mut fallback = GrpcChunkTransfer::new(self.context.peers.clone(), timeout)?;
        if let Some(group) = self.context.group {
            fallback = fallback.with_read_group(Some(group.bytes as u64));
        }
        Ok(fallback)
    }

    /// Retires a peer session idle for `idle_ms`, at most `SESSION_REUSE_LIMIT`.
    pub fn with_session_idle(mut self, idle_ms: u64) -> Self {
        self.context.session_idle = session_idle_limit(idle_ms);
        self
    }

    /// Bounds a prepare wave by the local receive slots of one lane.
    pub fn with_receive_slots(mut self, receive_slots: usize) -> Self {
        self.context.receive_slots = receive_slots.max(1);
        self
    }

    pub fn with_metrics(mut self, metrics: HixlMetrics) -> Self {
        self.context.metrics = Some(metrics);
        self
    }

    /// Attaches observation-only phase sampling for native windows and the
    /// control steps around them.
    pub fn with_phase_metrics(mut self, metrics: DfsReadPhaseMetrics) -> Self {
        self.context.phase_metrics = Some(metrics);
        self
    }

    /// Starts a detached warm task for a preferred-mode peer. Returns true when
    /// this batch should stay on gRPC: the task started here or another task is
    /// already connecting the same peer. Required mode never uses this path.
    fn start_warm(
        &self,
        peer_identity: (String, u64),
        address: String,
        authority: DfsReadRangesRequest,
    ) -> bool {
        let (node_id, node_epoch) = peer_identity;
        let task = match self.runtime.block_on(
            self.context
                .begin_warm(node_id, node_epoch, address, authority),
        ) {
            WarmStart::Started(task) => task,
            WarmStart::InProgress => return true,
            WarmStart::Skip => return false,
        };
        if self.runtime.spawn_background(task.clone().run()) {
            return true;
        }
        self.runtime.block_on(task.cancel());
        false
    }

    /// Pre-establishes the HIXL control session for one peer without a read.
    /// Startup wiring calls this from a dedicated thread so the first read
    /// skips negotiation. Best effort: a failed or unsupported pre-warm
    /// leaves the lazy first-read connect path unchanged. Returns true once
    /// the peer holds a session. Must not run inside the async runtime.
    pub fn prewarm(
        &self,
        node_id: &str,
        node_epoch: u64,
        address: &str,
        caller_epoch: u64,
    ) -> bool {
        let Some(deadline) = Instant::now().checked_add(self.context.timeout) else {
            return false;
        };
        match self.runtime.block_on(self.context.begin_prewarm(
            node_id.to_owned(),
            node_epoch,
            address.to_owned(),
            caller_epoch,
        )) {
            WarmStart::Started(task) => {
                if !self.runtime.spawn_background(task.clone().run()) {
                    // The task was never polled; clearing the flag keeps the
                    // slot reusable for the lazy path.
                    self.runtime.block_on(task.cancel());
                    return false;
                }
            }
            // InProgress: another task is connecting. Skip covers a connected
            // peer and a busy slot alike. Both settle below, and the session
            // check tells the caller which one happened.
            WarmStart::InProgress | WarmStart::Skip => {}
        }
        let _ = self.runtime.block_on(
            self.context
                .settle_warm(node_id, node_epoch, address, deadline),
        );
        self.runtime
            .block_on(self.context.has_session(node_id, node_epoch, address))
    }

    /// Shared native window path. `crc_chunk` marks a CRC32C object: every
    /// promoted window must then carry that object's CRC32C digest.
    fn read_native_batch(
        &self,
        batch: &PeerReadBatch,
        out: &mut [u8],
        crc_chunk: Option<&crate::dfs::ChunkObject>,
    ) -> afs_error::Result<()> {
        match self.context.group {
            Some(group) => batch.validate_group(out.len(), group.bytes as u64)?,
            None => batch.validate(out.len())?,
        }
        let source = &batch.operations[0].source;
        let crate::dfs::CopyLocation::Node {
            node_id,
            node_epoch,
            ..
        } = &source.location
        else {
            return Err(dfs_protocol_error("HIXL source is not a Node copy"));
        };
        let address = source
            .node_data_endpoint()
            .ok_or_else(|| dfs_protocol_error("HIXL source has no gRPC endpoint"))?
            .to_owned();
        let sequence = self
            .next_read
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| {
                afs_error::Error::coded(
                    afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
                    "HIXL read identity space exhausted",
                )
            })?;
        let request = DfsReadRangesRequest {
            read_id: format!(
                "{}-{}-{}-{sequence}",
                source.read_grant.caller_node_id,
                source.read_grant.caller_node_epoch,
                self.context.native.info().generation
            ),
            attempt_id: format!("{node_id}-{node_epoch}-{sequence}"),
            file_version_id: batch.file_version_id.0.clone(),
            layout_root_id: batch.layout_root_id.0.clone(),
            operations: batch
                .operations
                .iter()
                .enumerate()
                .map(|(index, item)| {
                    Ok(afs_protocol::node_data::DfsChunkReadOp {
                        operation_index: u32::try_from(index)
                            .map_err(|_| dfs_protocol_error("HIXL operation index overflow"))?,
                        chunk_id: item.op.chunk_id.0.clone(),
                        chunk_offset: item.op.chunk_offset,
                        length: item.op.length,
                        destination_offset: u64::try_from(item.op.output_offset)
                            .map_err(|_| dfs_protocol_error("HIXL output offset overflow"))?,
                        source_copy_id: item.source.copy_id.0.clone(),
                        grant: Some(wire_dfs_read_grant(&item.source.read_grant)),
                    })
                })
                .collect::<afs_error::Result<_>>()?,
            include_blake3_outboard: false,
        };
        let started = Instant::now();
        let deadline = started
            .checked_add(self.context.timeout)
            .ok_or_else(|| dfs_protocol_error("HIXL deadline overflow"))?;
        let preferred = self.context.mode == HixlDfsReadMode::Preferred;
        let refused = preferred
            && self
                .context
                .is_refused(&(node_id.clone(), *node_epoch, address.clone()));
        let warming = preferred
            && !refused
            && self.start_warm(
                (node_id.clone(), *node_epoch),
                address.clone(),
                request.clone(),
            );
        if refused || warming {
            let result = self.read_over_grpc(batch, out, crc_chunk, deadline);
            if warming {
                if crc_chunk.is_none()
                    && !carries_crc32c(batch)
                    && result.is_ok()
                    && let Some(metrics) = &self.context.metrics
                {
                    metrics.observe_cold_fallback();
                }
            } else {
                self.context
                    .observe(Operation::Fallback, started.elapsed(), result.is_ok());
            }
            return result;
        }
        let context = self.context.clone();
        let peer_identity = (node_id.clone(), *node_epoch);
        let cancel = Arc::new(Cancellation::default());
        let output_len = out.len();
        let crc = crc_lookup(batch, crc_chunk);
        let result = self.runtime.block_on(async move {
            let command_cancel = cancel.clone();
            let task = tokio::spawn(async move {
                context
                    .read_batch(
                        peer_identity,
                        address,
                        request,
                        output_len,
                        deadline,
                        crc,
                        command_cancel,
                    )
                    .await
            });
            wait_owned_task(task, deadline, cancel).await
        })?;
        match result {
            ReadResult::Complete(chunks) => {
                // Native IO and the control lifecycle own their buffers. Only a
                // complete validated batch reaches this caller-owned slice.
                let scatter_started = Instant::now();
                scatter_prepared(chunks, out)?;
                self.context
                    .observe_phase("scatter", scatter_started.elapsed());
                Ok(())
            }
            ReadResult::Unsupported => {
                self.context
                    .observe(Operation::Fallback, started.elapsed(), true);
                self.read_over_grpc(batch, out, crc_chunk, deadline)
            }
        }
    }

    /// Serves a batch HIXL cannot take on gRPC: CRC32C objects through the CRC
    /// adapter, everything else through the plain peer stream.
    ///
    /// `supports_batched_crc` lets the engine hand CRC32C ranges over through
    /// `read_ranges`, each object on its operation. The CRC adapter takes one
    /// range per request and the plain stream only BLAKE3 completions, so a
    /// batch carrying CRC32C objects is served one operation at a time.
    fn read_over_grpc(
        &self,
        batch: &PeerReadBatch,
        out: &mut [u8],
        crc_chunk: Option<&crate::dfs::ChunkObject>,
        deadline: Instant,
    ) -> afs_error::Result<()> {
        if let Some(chunk) = crc_chunk {
            remaining(deadline)?;
            return self.read_crc32c_over_grpc(batch, chunk, out);
        }
        if !carries_crc32c(batch) {
            return self
                .grpc_fallback(remaining(deadline)?)?
                .read_ranges(batch, out);
        }
        for item in &batch.operations {
            let start = item.op.output_offset;
            let target = usize::try_from(item.op.length)
                .ok()
                .and_then(|length| start.checked_add(length))
                .and_then(|end| out.get_mut(start..end))
                .ok_or_else(|| dfs_protocol_error("HIXL fallback range exceeds output"))?;
            let mut single = item.clone();
            single.op.output_offset = 0;
            let single = PeerReadBatch {
                file_version_id: batch.file_version_id.clone(),
                layout_root_id: batch.layout_root_id.clone(),
                operations: vec![single],
            };
            let budget = remaining(deadline)?;
            match &item.chunk {
                Some(chunk) if chunk.is_segment_crc32c() => {
                    self.read_crc32c_over_grpc(&single, chunk, target)?
                }
                _ => self.grpc_fallback(budget)?.read_ranges(&single, target)?,
            }
        }
        Ok(())
    }

    fn read_crc32c_over_grpc(
        &self,
        batch: &PeerReadBatch,
        chunk: &crate::dfs::ChunkObject,
        out: &mut [u8],
    ) -> afs_error::Result<()> {
        let result = self.crc.read_crc32c_range(batch, chunk, out);
        if result.is_ok()
            && let Some(metrics) = &self.context.metrics
        {
            metrics.observe_crc_fallback();
        }
        result
    }
}

/// Validated native output that has not yet reached the caller buffer.
struct PreparedChunk {
    bytes: Vec<u8>,
    windows: Vec<ReadWindow>,
}

enum ReadResult {
    Complete(Vec<PreparedChunk>),
    Unsupported,
}

enum Acquired {
    Lease(PeerLease),
    Unsupported,
}

impl ChunkTransfer for HixlChunkTransfer {
    /// Windows carry one CRC32C range descriptor per operation, so CRC32C
    /// chunks batch with their neighbours instead of going one chunk at a time.
    fn supports_batched_crc(&self) -> bool {
        true
    }

    /// CRC32C objects travel through the same native windows: the source
    /// serves a CRC32C range checksum plus its Meta-bound segment table, and
    /// the window is validated against both. The gRPC adapter remains the
    /// fallback for a preferred-mode cold peer and for an unsupported source.
    fn read_crc32c_range(
        &self,
        batch: &PeerReadBatch,
        chunk: &crate::dfs::ChunkObject,
        out: &mut [u8],
    ) -> afs_error::Result<()> {
        self.read_native_batch(batch, out, Some(chunk))
            .inspect_err(|error| report_batch_failure(batch, out.len(), error))
    }

    fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> afs_error::Result<()> {
        self.read_native_batch(batch, out, None)
            .inspect_err(|error| report_batch_failure(batch, out.len(), error))
    }
}

fn is_crc32c_operation(operation: &crate::node::dfs_read::ResolvedReadOp) -> bool {
    operation
        .chunk
        .as_ref()
        .is_some_and(crate::dfs::ChunkObject::is_segment_crc32c)
}

fn carries_crc32c(batch: &PeerReadBatch) -> bool {
    batch.operations.iter().any(is_crc32c_operation)
}

/// Reports why one HIXL read batch failed.
///
/// The read engine answers a failed batch by retrying every operation on its
/// own, so a batch failure costs as many single-range reads as the batch had
/// operations. The reason therefore has to stay visible at the default log
/// level; otherwise a transport-level retry looks like ordinary overhead.
fn report_batch_failure(batch: &PeerReadBatch, output_len: usize, error: &afs_error::Error) {
    let crc_operations = batch
        .operations
        .iter()
        .filter(|operation| is_crc32c_operation(operation))
        .count();
    let bytes = batch.operations.iter().fold(0_u64, |sum, operation| {
        sum.saturating_add(operation.op.length)
    });
    afs_logging::error!(
        "HIXL read batch failed";
        "code" => format!("{:?}", error.code()),
        "operations" => batch.operations.len(),
        "crc_operations" => crc_operations,
        "bytes" => bytes,
        "output_len" => output_len,
        "error" => error.to_string()
    );
}

#[derive(Default)]
struct Cancellation {
    cancelled: AtomicBool,
    peer: StdMutex<Option<Arc<AtomicBool>>>,
}

impl Cancellation {
    fn publish(&self, peer: Arc<AtomicBool>) -> afs_error::Result<()> {
        let mut target = self
            .peer
            .lock()
            .map_err(|_| dfs_protocol_error("HIXL cancellation lock is poisoned"))?;
        if self.cancelled.load(Ordering::Acquire) {
            peer.store(true, Ordering::Release);
        }
        *target = Some(peer);
        Ok(())
    }
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        if let Ok(peer) = self.peer.lock()
            && let Some(peer) = peer.as_ref()
        {
            peer.store(true, Ordering::Release);
        }
    }
}

struct CancelGuard {
    cancel: Arc<Cancellation>,
    armed: bool,
}
impl Drop for CancelGuard {
    fn drop(&mut self) {
        if self.armed {
            self.cancel.cancel();
        }
    }
}

async fn wait_owned_task<T>(
    task: tokio::task::JoinHandle<afs_error::Result<T>>,
    deadline: Instant,
    cancel: Arc<Cancellation>,
) -> afs_error::Result<T> {
    let mut guard = CancelGuard {
        cancel,
        armed: true,
    };
    let result = tokio::time::timeout(remaining(deadline)?, task)
        .await
        .map_err(|_| {
            afs_error::Error::coded(
                afs_error::CLIENT_DEADLINE_EXCEEDED,
                "HIXL read total deadline exceeded",
            )
        })?
        .map_err(|error| {
            afs_error::Error::coded(afs_error::CLIENT_WORKER_FAILED, error.to_string())
        })?;
    guard.armed = false;
    result
}

pub(super) fn remaining(deadline: Instant) -> afs_error::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|value| !value.is_zero())
        .ok_or_else(|| {
            afs_error::Error::coded(
                afs_error::CLIENT_DEADLINE_EXCEEDED,
                "HIXL read deadline expired",
            )
        })
}

fn validate_negotiation(
    reply: &DfsNegotiateHixlReply,
    node: &str,
    epoch: u64,
    local_info: &EngineInfo,
) -> afs_error::Result<()> {
    let endpoint = reply
        .engine_id
        .parse::<SocketAddr>()
        .map_err(|_| dfs_protocol_error("HIXL source engine address is invalid"))?;
    let (
        expected_transport,
        expected_buffer_pool_count,
        expected_buffer_pool_size_mib,
        expected_hccs_domain_id,
    ) = engine_transport_parameters(local_info)?;
    let actual_transport = DfsHixlTransport::try_from(reply.transport)
        .map_err(|_| dfs_protocol_error("HIXL source transport is unknown"))?;
    if reply.protocol_version != HIXL_PROTOCOL_VERSION
        || reply.session_id == 0
        || reply.source_node_id != node
        || reply.source_node_epoch != epoch
        || reply.engine_generation == 0
        || reply.window_bytes == 0
        || reply.window_bytes > 4 * 1024 * 1024
        || reply.export_slots == 0
        || reply.export_slots > 64
        || actual_transport != expected_transport
        || reply.buffer_pool_count != expected_buffer_pool_count
        || reply.buffer_pool_size_mib != expected_buffer_pool_size_mib
        || reply.hccs_domain_id != expected_hccs_domain_id
        || endpoint.port() == 0
        || endpoint.ip().is_unspecified()
        || endpoint.ip().is_multicast()
    {
        return Err(dfs_protocol_error(
            "HIXL negotiation identity or capability is invalid",
        ));
    }
    Ok(())
}

/// Binds a peer to the least-used lane. The bounded table is best effort: peer
/// identities churn on restart, so it starts over once `max_keys` is reached.
pub(super) fn least_used_lane<K: std::hash::Hash + Eq + Clone>(
    bindings: &StdMutex<HashMap<K, usize>>,
    key: &K,
    lanes: usize,
    max_keys: usize,
) -> usize {
    let lanes = lanes.max(1);
    if lanes == 1 {
        return 0;
    }
    let mut bindings = bindings
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(lane) = bindings.get(key) {
        return *lane % lanes;
    }
    if bindings.len() >= max_keys {
        bindings.clear();
    }
    let mut counts = vec![0_usize; lanes];
    for lane in bindings.values() {
        counts[*lane % lanes] += 1;
    }
    let lane = counts
        .iter()
        .enumerate()
        .min_by_key(|(_, count)| **count)
        .map_or(0, |(index, _)| index);
    bindings.insert(key.clone(), lane);
    lane
}

/// One negotiated lane's identity, capacity and endpoint, unique in its reply.
pub(super) fn lane_is_valid(
    lane: &DfsHixlLane,
    index: usize,
    endpoints: &mut std::collections::HashSet<SocketAddr>,
) -> bool {
    let Ok(endpoint) = lane.engine_id.parse::<SocketAddr>() else {
        return false;
    };
    usize::try_from(lane.lane_id).ok() == Some(index)
        && lane.engine_generation != 0
        && lane.window_bytes != 0
        && lane.window_bytes <= 4 * 1024 * 1024
        && lane.export_slots != 0
        && lane.export_slots <= 64
        && endpoint.port() != 0
        && !endpoint.ip().is_unspecified()
        && !endpoint.ip().is_multicast()
        && !matches!(endpoint.ip(), std::net::IpAddr::V4(address) if address.is_broadcast())
        && endpoints.insert(endpoint)
}

/// Connects every local lane to its negotiated remote lane.
pub(super) async fn connect_lanes(
    native: Arc<dyn ReadRuntime>,
    lanes: Vec<DfsHixlLane>,
    deadline: Instant,
) -> afs_error::Result<()> {
    tokio::task::spawn_blocking(move || {
        for (index, remote) in lanes.iter().enumerate() {
            native
                .lane(index)
                .ok_or_else(|| dfs_protocol_error("HIXL local lane disappeared"))?
                .connect(&remote.engine_id, deadline)
                .map_err(runtime_error)?;
        }
        Ok(())
    })
    .await
    .map_err(|error| afs_error::Error::coded(afs_error::CLIENT_WORKER_FAILED, error.to_string()))?
}

fn validate_lanes(reply: &DfsNegotiateHixlReply, local: &[EngineInfo]) -> afs_error::Result<()> {
    if local.is_empty() || local.len() > 16 || reply.lanes.len() != local.len() {
        return Err(dfs_protocol_error(
            "HIXL negotiated lane count differs from local configuration",
        ));
    }
    let mut engines = std::collections::HashSet::new();
    for (index, lane) in reply.lanes.iter().enumerate() {
        if !lane_is_valid(lane, index, &mut engines) {
            return Err(dfs_protocol_error(
                "HIXL lane identity or capacity is invalid",
            ));
        }
        if index == 0
            && (lane.engine_id != reply.engine_id
                || lane.engine_generation != reply.engine_generation
                || lane.window_bytes != reply.window_bytes
                || lane.export_slots != reply.export_slots)
        {
            return Err(dfs_protocol_error(
                "HIXL primary lane differs from control identity",
            ));
        }
        let (transport, count, size, domain) = engine_transport_parameters(&local[index])?;
        if transport as i32 != reply.transport
            || count != reply.buffer_pool_count
            || size != reply.buffer_pool_size_mib
            || domain != reply.hccs_domain_id
        {
            return Err(dfs_protocol_error(
                "HIXL local lanes differ from negotiated transport",
            ));
        }
    }
    Ok(())
}

pub(super) fn engine_transport_parameters(
    info: &EngineInfo,
) -> afs_error::Result<(DfsHixlTransport, u32, u32, Option<u64>)> {
    match info.transport {
        ReadTransport::Roce => {
            if info.buffer_pool_count != 0
                || info.buffer_pool_size_mib != 0
                || info.hccs_domain_id.is_some()
            {
                return Err(dfs_protocol_error(
                    "HIXL local RoCE relay parameters must be empty",
                ));
            }
            Ok((DfsHixlTransport::Roce, 0, 0, None))
        }
        ReadTransport::HccsBufferPool => {
            if info.buffer_pool_count == 0 || info.buffer_pool_size_mib == 0 {
                return Err(dfs_protocol_error(
                    "HIXL local HCCS buffer-pool parameters are invalid",
                ));
            }
            let count = u32::try_from(info.buffer_pool_count)
                .map_err(|_| dfs_protocol_error("HIXL local HCCS buffer-pool count overflows"))?;
            let size_mib = u32::try_from(info.buffer_pool_size_mib)
                .map_err(|_| dfs_protocol_error("HIXL local HCCS buffer-pool size overflows"))?;
            let domain_id = info
                .hccs_domain_id
                .ok_or_else(|| dfs_protocol_error("HIXL local HCCS super-pod domain is absent"))?;
            Ok((
                DfsHixlTransport::HccsBufferPool,
                count,
                size_mib,
                Some(domain_id),
            ))
        }
    }
}

impl ReadContext {
    fn observe(&self, phase: Operation, elapsed: Duration, success: bool) {
        observe_optional(self.metrics.as_ref(), phase, elapsed, success);
    }

    /// Whether `key` refused admission and its retry time has not come yet.
    fn is_refused(&self, key: &PeerKey) -> bool {
        let mut refused = self
            .refused
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        refused.retain(|_, until| *until > now);
        refused.contains_key(key)
    }

    /// Records a refusal until `until`, or clears it with `None`. A full
    /// table keeps its entries; that source is then renegotiated as before.
    fn set_refused(&self, key: PeerKey, until: Option<Instant>) {
        let mut refused = self
            .refused
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        refused.retain(|_, until| *until > now);
        match until {
            Some(until) if refused.contains_key(&key) || refused.len() < MAX_HIXL_PEERS => {
                refused.insert(key, until);
            }
            Some(_) => {}
            None => {
                refused.remove(&key);
            }
        }
    }

    fn observe_phase(&self, phase: &'static str, elapsed: Duration) {
        if let Some(metrics) = &self.phase_metrics {
            metrics.observe_phase(phase, elapsed);
        }
    }

    /// Binds a peer to the least-used device lane. Concurrent peers therefore
    /// transfer on separate devices instead of serializing on one lane; a peer
    /// without a free lane shares the least-loaded one.
    fn bind_lane(&self, key: &PeerKey) -> usize {
        least_used_lane(
            &self.lane_bindings,
            key,
            self.native.lane_count(),
            MAX_HIXL_PEERS * 4,
        )
    }

    /// Waits until a detached warm task for this peer settles. A failed warm
    /// clears the flag, so the caller falls back to the inline connect.
    async fn settle_warm(
        &self,
        node_id: &str,
        node_epoch: u64,
        address: &str,
        deadline: Instant,
    ) -> afs_error::Result<()> {
        loop {
            let key = (node_id.to_owned(), node_epoch, address.to_owned());
            let slot = {
                let slots = self.slots.lock().await;
                slots.get(&key).cloned()
            };
            let Some(slot) = slot else {
                return Ok(());
            };
            let warming = match slot.try_lock() {
                Ok(guard) => {
                    if guard.session.is_some() {
                        return Ok(());
                    }
                    guard.warming.clone()
                }
                Err(_) => return Ok(()),
            };
            let Some(warming) = warming else {
                return Ok(());
            };
            let mut changed = warming.subscribe();
            // Holding a sender here would keep this channel open after the warm
            // task clears the flag, so the wait would only end at the deadline.
            drop(warming);
            let wait = remaining(deadline)?;
            // The warm task drops the sender when it clears the flag, so a
            // closed channel also ends the wait and the loop rechecks the slot.
            let _ = tokio::time::timeout(wait, changed.changed()).await;
        }
    }

    /// Starts a detached warm task for a peer without a session. `InProgress`
    /// lets the caller keep the current batch on gRPC while the existing task
    /// connects; `Skip` means the normal blocking HIXL path must be used.
    async fn begin_warm(
        &self,
        node_id: String,
        node_epoch: u64,
        address: String,
        authority: DfsReadRangesRequest,
    ) -> WarmStart {
        let Some(caller_epoch) = authority
            .operations
            .first()
            .and_then(|op| op.grant.as_ref())
            .map(|grant| grant.caller_node_epoch)
        else {
            return WarmStart::Skip;
        };
        self.start_warm_task(node_id, node_epoch, address, Some(authority), caller_epoch)
            .await
    }

    /// Starts a detached pre-warm task for a peer without a session and
    /// without a read authority. The transport session is established so the
    /// first read skips negotiation; every window still authorizes its own
    /// grant afterwards. The caller epoch must be this Node's registered
    /// incarnation so later read grants accept the pre-warmed session.
    async fn begin_prewarm(
        &self,
        node_id: String,
        node_epoch: u64,
        address: String,
        caller_epoch: u64,
    ) -> WarmStart {
        if caller_epoch == 0 {
            return WarmStart::Skip;
        }
        self.start_warm_task(node_id, node_epoch, address, None, caller_epoch)
            .await
    }

    /// Whether a peer slot already holds a control session.
    async fn has_session(&self, node_id: &str, node_epoch: u64, address: &str) -> bool {
        let key = (node_id.to_owned(), node_epoch, address.to_owned());
        let slot = {
            let slots = self.slots.lock().await;
            slots.get(&key).cloned()
        };
        let Some(slot) = slot else {
            return false;
        };
        slot.lock().await.session.is_some()
    }

    async fn start_warm_task(
        &self,
        node_id: String,
        node_epoch: u64,
        address: String,
        authority: Option<DfsReadRangesRequest>,
        caller_epoch: u64,
    ) -> WarmStart {
        let key = (node_id.clone(), node_epoch, address.clone());
        let slot = {
            let mut slots = self.slots.lock().await;
            if !slots.contains_key(&key) && slots.len() >= MAX_HIXL_PEERS {
                return WarmStart::Skip;
            }
            slots
                .entry(key.clone())
                .or_insert_with(|| {
                    Arc::new(Mutex::new(PeerSlot {
                        session: None,
                        touched: Instant::now(),
                        warming: None,
                    }))
                })
                .clone()
        };
        let Ok(mut guard) = slot.try_lock() else {
            return WarmStart::Skip;
        };
        if guard.session.is_some() {
            return WarmStart::Skip;
        }
        if guard.warming.is_some() {
            return WarmStart::InProgress;
        }
        let (sender, _) = watch::channel(false);
        guard.warming = Some(sender);
        drop(guard);
        WarmStart::Started(Box::new(WarmTask {
            context: self.clone(),
            slot,
            node_id,
            node_epoch,
            address,
            authority,
            caller_epoch,
        }))
    }

    /// Negotiates and connects a peer control session. The caller must have
    /// verified that the slot has no session; a concurrent warm task is
    /// serialized by the slot's `warming` flag. A read authority negotiates a
    /// serving session; `None` pre-warms the transport without one.
    async fn establish(
        &self,
        node_id: &str,
        node_epoch: u64,
        address: &str,
        authority: Option<&DfsReadRangesRequest>,
        caller_epoch: u64,
        deadline: Instant,
    ) -> afs_error::Result<Option<Arc<PeerSession>>> {
        let channel = self.peers.channel(node_id, node_epoch, address).await?;
        let mut client = DfsChunksClient::new(channel)
            .max_encoding_message_size(self.peers.config.max_encoding_message_bytes)
            .max_decoding_message_size(self.peers.config.max_decoding_message_bytes);
        let info = self.native.info();
        let (transport, buffer_pool_count, buffer_pool_size_mib, hccs_domain_id) =
            engine_transport_parameters(&info)?;
        let mut request = afs_tracing::request_with_current_context(DfsNegotiateHixlRequest {
            read_request: authority.cloned(),
            client_engine_id: info.engine_id.clone(),
            client_engine_generation: info.generation,
            protocol_version: HIXL_PROTOCOL_VERSION,
            caller_node_epoch: caller_epoch,
            source_node_epoch: node_epoch,
            transport: transport as i32,
            buffer_pool_count,
            buffer_pool_size_mib,
            hccs_domain_id,
            lane_count: u32::try_from(self.native.lane_count())
                .map_err(|_| dfs_protocol_error("HIXL lane count overflow"))?,
        });
        request.set_timeout(remaining(deadline)?);
        let start = Instant::now();
        let response = client
            .negotiate_hixl(request)
            .await
            .map_err(status_to_error);
        self.observe(Operation::Negotiate, start.elapsed(), response.is_ok());
        let reply = match response {
            Ok(response) => response.into_inner(),
            Err(error) if may_fallback(self.mode, &error) => return Ok(None),
            // Only the source's own coded status carries this code; a lost
            // transport reply decodes as `CLIENT_REMOTE_STATUS`.
            Err(error) if self.mode == HixlDfsReadMode::Preferred && is_refused_error(&error) => {
                self.set_refused(
                    (node_id.to_owned(), node_epoch, address.to_owned()),
                    Instant::now().checked_add(REFUSED_RETRY),
                );
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        self.set_refused((node_id.to_owned(), node_epoch, address.to_owned()), None);
        // The source registered this session when it answered; every failure
        // from here on must close it instead of leaving it to the source TTL.
        let session = PeerSession {
            reply,
            caller_epoch,
            client,
            poisoned: Arc::new(AtomicBool::new(false)),
        };
        if let Err(error) = self
            .admit_negotiated(&session.reply, node_id, node_epoch, &info, deadline)
            .await
        {
            session.poisoned.store(true, Ordering::Release);
            let _ = close_session(&session, self.timeout).await;
            return Err(error);
        }
        Ok(Some(Arc::new(session)))
    }

    /// Validates a negotiated reply, binds its engines and connects its lanes.
    async fn admit_negotiated(
        &self,
        reply: &DfsNegotiateHixlReply,
        node_id: &str,
        node_epoch: u64,
        info: &EngineInfo,
        deadline: Instant,
    ) -> afs_error::Result<()> {
        validate_negotiation(reply, node_id, node_epoch, info)?;
        validate_lanes(reply, &self.native.infos())?;
        bind_engines(
            &mut *self.engines.lock().await,
            reply.lanes.iter().map(|lane| {
                (
                    lane.engine_id.as_str(),
                    (
                        reply.source_node_id.clone(),
                        reply.source_node_epoch,
                        lane.engine_generation,
                    ),
                )
            }),
            MAX_HIXL_ENGINES,
        )?;
        let start = Instant::now();
        let connected = connect_lanes(self.native.clone(), reply.lanes.clone(), deadline).await;
        self.observe(Operation::Connect, start.elapsed(), connected.is_ok());
        connected
    }

    async fn acquire(
        &self,
        node_id: &str,
        node_epoch: u64,
        address: &str,
        authority: &DfsReadRangesRequest,
        deadline: Instant,
    ) -> afs_error::Result<Acquired> {
        self.settle_warm(node_id, node_epoch, address, deadline)
            .await?;
        let key = (node_id.to_owned(), node_epoch, address.to_owned());
        let (slot, mut guard) = loop {
            let slot = {
                let mut slots = self.slots.lock().await;
                if !slots.contains_key(&key) && slots.len() >= MAX_HIXL_PEERS {
                    return Err(afs_error::Error::coded(
                        afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
                        "HIXL peer budget exhausted",
                    ));
                }
                slots
                    .entry(key.clone())
                    .or_insert_with(|| {
                        Arc::new(Mutex::new(PeerSlot {
                            session: None,
                            touched: Instant::now(),
                            warming: None,
                        }))
                    })
                    .clone()
            };
            // One peer admits a single logical read at a time. Concurrent FUSE
            // reads must queue for that admission instead of failing: a cold
            // readahead fan-out would otherwise turn the peer limit into errors.
            let wait = remaining(deadline)?;
            let guard = tokio::time::timeout(wait, slot.clone().lock_owned())
                .await
                .map_err(|_| {
                    afs_error::Error::coded(
                        afs_error::CLIENT_DEADLINE_EXCEEDED,
                        "HIXL peer read admission exceeded the batch deadline",
                    )
                })?;
            // The slot may have been reclaimed while this read queued.
            if !self
                .slots
                .lock()
                .await
                .get(&key)
                .is_some_and(|current| Arc::ptr_eq(current, &slot))
            {
                drop(guard);
                continue;
            }
            if guard.session.is_none() && guard.warming.is_some() {
                drop(guard);
                self.settle_warm(node_id, node_epoch, address, deadline)
                    .await?;
                continue;
            }
            break (slot, guard);
        };
        let caller_epoch = authority
            .operations
            .first()
            .and_then(|op| op.grant.as_ref())
            .map(|grant| grant.caller_node_epoch)
            .ok_or_else(|| dfs_protocol_error("HIXL authority has no caller epoch"))?;
        if guard.session.as_ref().is_some_and(|session| {
            session.poisoned.load(Ordering::Acquire)
                || caller_epoch != session.caller_epoch
                || guard.touched.elapsed() >= self.session_idle
        }) {
            let start = Instant::now();
            let closed = retire_idle_session(&mut guard, self.timeout).await;
            self.observe(Operation::Close, start.elapsed(), closed);
        }
        let fresh = guard.session.is_none();
        if fresh {
            let established = self
                .establish(
                    node_id,
                    node_epoch,
                    address,
                    Some(authority),
                    caller_epoch,
                    deadline,
                )
                .await;
            match established {
                Ok(Some(session)) => guard.session = Some(session),
                Ok(None) => {
                    retire_unbound_peer(&self.slots, &key, &slot).await;
                    return Ok(Acquired::Unsupported);
                }
                Err(error) => {
                    retire_unbound_peer(&self.slots, &key, &slot).await;
                    return Err(error);
                }
            }
        }
        let session = guard
            .session
            .as_ref()
            .ok_or_else(|| dfs_protocol_error("HIXL peer session was not published"))?
            .clone();
        guard.touched = Instant::now();
        Ok(Acquired::Lease(PeerLease {
            session,
            guard,
            fresh,
        }))
    }

    /// Reads one batch. A reused session whose first prepare finds it closed
    /// on the source (expired, restarted admission) is renegotiated once;
    /// nothing was admitted on it, so the retry repeats no transfer.
    #[allow(clippy::too_many_arguments)]
    async fn read_batch(
        &self,
        peer_identity: (String, u64),
        address: String,
        authority: DfsReadRangesRequest,
        output_len: usize,
        deadline: Instant,
        crc: Option<CrcLookup>,
        cancel: Arc<Cancellation>,
    ) -> afs_error::Result<ReadResult> {
        let mut renegotiated = false;
        loop {
            let mut stale = false;
            let result = self
                .read_batch_attempt(
                    &peer_identity,
                    &address,
                    authority.clone(),
                    output_len,
                    deadline,
                    crc.as_ref(),
                    &cancel,
                    &mut stale,
                )
                .await;
            if stale && !renegotiated && !cancel.cancelled.load(Ordering::Acquire) {
                renegotiated = true;
                continue;
            }
            return result;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn read_batch_attempt(
        &self,
        peer_identity: &(String, u64),
        address: &str,
        authority: DfsReadRangesRequest,
        output_len: usize,
        deadline: Instant,
        crc: Option<&CrcLookup>,
        cancel: &Cancellation,
        stale: &mut bool,
    ) -> afs_error::Result<ReadResult> {
        let mut lease = match self
            .acquire(
                &peer_identity.0,
                peer_identity.1,
                address,
                &authority,
                deadline,
            )
            .await?
        {
            Acquired::Lease(lease) => lease,
            Acquired::Unsupported => return Ok(ReadResult::Unsupported),
        };
        cancel.publish(lease.session.poisoned.clone())?;
        let capacity = lease.session.reply.lanes.iter().try_fold(
            self.native.info().window_bytes,
            |capacity, lane| {
                usize::try_from(lane.window_bytes)
                    .map(|remote| capacity.min(remote))
                    .map_err(|_| dfs_protocol_error("HIXL remote lane capacity overflow"))
            },
        )?;
        let group = self.group;
        let (batch, max_window_ops) = match group {
            Some(_) => (
                pack_read_group(authority.clone())?,
                crate::node::dfs_read::MAX_DFS_GROUP_OPS,
            ),
            None => (
                pack_read_batch(authority.clone())?,
                crate::node::dfs_read::MAX_DFS_READ_OPS,
            ),
        };
        let lane = self.bind_lane(&(peer_identity.0.clone(), peer_identity.1, address.to_owned()));
        let mut batch_offset = 0_u64;
        let planned = pack_windows_with_ops(authority, capacity, max_window_ops)?
            .into_iter()
            .map(|window| {
                let offset = batch_offset;
                batch_offset = batch_offset
                    .checked_add(
                        u64::try_from(window.length)
                            .map_err(|_| dfs_protocol_error("HIXL batch length overflow"))?,
                    )
                    .ok_or_else(|| dfs_protocol_error("HIXL batch offset overflow"))?;
                Ok((window, offset, lane))
            })
            .collect::<afs_error::Result<Vec<_>>>()?;
        // A wave is one Prepare RPC and one native SDK batch over prepared
        // windows of the peer's bound lane. It never asks for more simultaneous
        // exports than the source lane can hold or the local lane can receive.
        let export_slots = usize::try_from(
            lease
                .session
                .reply
                .lanes
                .get(lane)
                .map_or(0, |lane| lane.export_slots),
        )
        .unwrap_or(0);
        // Two waves are exported at once: the next wave is prepared while the
        // previous one reads, so each wave takes at most half of the source
        // lane's export slots. Native READs still run one wave at a time.
        let wave_width = group
            .map_or(usize::MAX, |group| group.max_descriptors)
            .min(self.receive_slots)
            .min((export_slots / 2).max(1))
            .clamp(1, crate::node::dfs_read::MAX_DFS_GROUP_OPS);
        let waves = planned.chunks(wave_width).collect::<Vec<_>>();
        let cancelled = || {
            cancel.cancelled.load(Ordering::Acquire).then(|| {
                WindowFailure::from(afs_error::Error::coded(
                    afs_error::CLIENT_DEADLINE_EXCEEDED,
                    "HIXL read cancelled before next wave",
                ))
            })
        };
        let Some(first) = waves.first() else {
            lease.guard.touched = Instant::now();
            remaining(deadline)?;
            return Ok(ReadResult::Complete(Vec::new()));
        };
        if let Some(failure) = cancelled() {
            return Err(self.abort_batch(&mut lease, failure).await);
        }
        // Admission remains ordered: wave k+1 is prepared only after wave k's
        // prepare returned, and its export overlaps wave k's native READ.
        let mut prepared = match self
            .prepare_group_windows_within_budget(&lease.session, first, &batch, deadline)
            .await
        {
            Ok(prepared) => prepared,
            Err(failure) => {
                report_wave_failure("prepare", planned.len(), first.len(), &failure);
                // A reused session the source no longer serves refuses the
                // first prepare before admitting any window.
                *stale =
                    !lease.fresh && failure.error.code() == afs_error::NODE_TRANSFER_UNAVAILABLE;
                return Err(self.abort_batch(&mut lease, failure).await);
            }
        };
        let mut chunks = Vec::new();
        for (index, wave) in waves.iter().enumerate() {
            let read =
                self.read_prepared_group(&lease.session, prepared, output_len, deadline, crc);
            let (read, next) = match waves.get(index + 1) {
                Some(next) if cancelled().is_none() => {
                    let prepare = self.prepare_group_windows_within_budget(
                        &lease.session,
                        next,
                        &batch,
                        deadline,
                    );
                    let (read, next_prepared) = tokio::join!(read, prepare);
                    (read, Some((next.len(), next_prepared)))
                }
                _ => (read.await, None),
            };
            match (read, next) {
                (Ok(mut ready), next) => {
                    chunks.append(&mut ready);
                    prepared = match next {
                        Some((_, Ok(next_prepared))) => next_prepared,
                        Some((windows, Err(failure))) => {
                            report_wave_failure("prepare", planned.len(), windows, &failure);
                            return Err(self.abort_batch(&mut lease, failure).await);
                        }
                        None if index + 1 < waves.len() => {
                            let failure = cancelled().unwrap_or_else(|| {
                                WindowFailure::from(dfs_protocol_error(
                                    "HIXL wave was neither prepared nor cancelled",
                                ))
                            });
                            return Err(self.abort_batch(&mut lease, failure).await);
                        }
                        None => Vec::new(),
                    };
                }
                (Err(failure), next) => {
                    report_wave_failure("read", planned.len(), wave.len(), &failure);
                    let failure = match next {
                        // The next wave never reached a native READ.
                        Some((_, Ok(unread))) => {
                            self.released_failure(
                                &lease.session,
                                &unread,
                                DfsHixlReleaseOutcome::NotSubmitted,
                                failure,
                            )
                            .await
                        }
                        Some((_, Err(other))) => failure.merged(other),
                        None => failure,
                    };
                    return Err(self.abort_batch(&mut lease, failure).await);
                }
            }
        }
        lease.guard.touched = Instant::now();
        remaining(deadline)?;
        Ok(ReadResult::Complete(chunks))
    }

    async fn abort_batch(&self, lease: &mut PeerLease, failure: WindowFailure) -> afs_error::Error {
        let started = Instant::now();
        let closed = close_session(&lease.session, self.timeout).await;
        self.observe(Operation::Close, started.elapsed(), closed.is_ok());
        if failure.fence_peer || closed.is_err() {
            lease.session.poisoned.store(true, Ordering::Release);
        } else {
            lease.guard.session = None;
        }
        if failure.quarantine_local {
            let native = self.native.clone();
            // Only uncertain local native lifetimes fence unrelated local lanes.
            let _ = tokio::task::spawn_blocking(move || native.quarantine()).await;
        }
        failure.error
    }

    /// Releases what a stopped group admitted and merges the release result
    /// into the failure; a failed release fences the peer.
    async fn released_failure(
        &self,
        session: &PeerSession,
        prepared: &[PreparedWindow],
        outcome: DfsHixlReleaseOutcome,
        failure: WindowFailure,
    ) -> WindowFailure {
        let mut failure = failure;
        failure.fence_peer |= self.release_group(session, prepared, outcome).await;
        failure
    }

    /// Prepares every ordered window of one logical group in a single control
    /// round trip.
    /// The source returns one export descriptor per window from the same
    /// verified parent snapshot. A malformed reply releases every admitted
    /// window before the batch is aborted.
    async fn prepare_group_windows(
        &self,
        session: &PeerSession,
        wave: &[PlannedWindow],
        batch: &DfsReadRangesRequest,
        deadline: Instant,
    ) -> Result<Vec<PreparedWindow>, WindowFailure> {
        let windows = wave
            .iter()
            .map(|(window, offset, lane)| {
                Ok(DfsHixlGroupWindow {
                    batch_offset: *offset,
                    lane_id: u32::try_from(*lane)
                        .map_err(|_| dfs_protocol_error("HIXL lane index overflow"))?,
                    read_request: Some(window.request.clone()),
                })
            })
            .collect::<afs_error::Result<Vec<_>>>()?;
        let mut request =
            afs_tracing::request_with_current_context(DfsPrepareReadRangesHixlGroupRequest {
                session_id: session.reply.session_id,
                caller_node_epoch: session.caller_epoch,
                source_node_epoch: session.reply.source_node_epoch,
                batch_read_request: Some(batch.clone()),
                windows,
            });
        request.set_timeout(remaining(deadline)?);
        let started = Instant::now();
        let response = session
            .client
            .clone()
            .prepare_read_ranges_hixl_group(request)
            .await
            .map_err(status_to_error);
        let prepare_elapsed = started.elapsed();
        self.observe(Operation::Prepare, prepare_elapsed, response.is_ok());
        self.observe_phase("prepare", prepare_elapsed);
        let reply = response.map_err(WindowFailure::from_prepare)?.into_inner();
        let complete =
            reply.session_id == session.reply.session_id && reply.windows.len() == wave.len();
        if complete {
            let mut prepared = Vec::with_capacity(wave.len());
            let mut invalid = None;
            for ((window, offset, lane), admitted) in wave.iter().zip(&reply.windows) {
                let lane_wire = u32::try_from(*lane)
                    .map_err(|_| dfs_protocol_error("HIXL lane index overflow"))?;
                let valid = admitted.batch_offset == *offset && admitted.lane_id == lane_wire;
                let Some(window_reply) = admitted.reply.as_ref() else {
                    invalid = Some("HIXL group prepare reply is missing a window");
                    break;
                };
                if !valid || validate_prepare(window, &session.reply, window_reply, *lane).is_err()
                {
                    invalid = Some("HIXL group prepare reply window is invalid");
                    break;
                }
                prepared.push((window.clone(), window_reply.clone(), *lane));
            }
            if invalid.is_none() {
                return Ok(prepared);
            }
        }
        let mut failure = WindowFailure::from(dfs_protocol_error(
            "HIXL group prepare reply does not match its windows",
        ));
        failure.fence_peer |= self
            .release_replies(
                session,
                reply
                    .windows
                    .iter()
                    .filter_map(|window| window.reply.as_ref()),
                DfsHixlReleaseOutcome::NotSubmitted,
            )
            .await;
        Err(failure)
    }

    /// Prepares one wave, halving it while the source reports a capacity
    /// shortfall.
    ///
    /// The source charges every served range against one shared payload
    /// budget, which concurrent readers can consume at any moment. A wave the
    /// budget cannot admit is a sizing signal, not a transport fault: retrying
    /// half of it keeps the session and the group, while propagating the error
    /// would end the session and leave the engine to retry every range on its
    /// own. The narrowed width applies to the rest of this wave only, so one
    /// contended moment does not narrow every later wave.
    async fn prepare_group_windows_within_budget(
        &self,
        session: &PeerSession,
        wave: &[PlannedWindow],
        batch: &DfsReadRangesRequest,
        deadline: Instant,
    ) -> Result<Vec<PreparedWindow>, WindowFailure> {
        let mut prepared = Vec::with_capacity(wave.len());
        let mut start = 0;
        let mut width = wave.len();
        while start < wave.len() {
            let end = (start + width).min(wave.len());
            match self
                .prepare_group_windows(session, &wave[start..end], batch, deadline)
                .await
            {
                Ok(mut ready) => {
                    prepared.append(&mut ready);
                    start = end;
                }
                Err(failure)
                    if failure.error.code() == afs_error::IO_CAPACITY && end - start > 1 =>
                {
                    width = (end - start) / 2;
                }
                Err(failure) => return Err(failure),
            }
        }
        Ok(prepared)
    }

    async fn release_replies<'a>(
        &self,
        session: &PeerSession,
        replies: impl Iterator<Item = &'a DfsPrepareReadRangesHixlReply>,
        outcome: DfsHixlReleaseOutcome,
    ) -> bool {
        let mut failed = false;
        for reply in replies {
            let mut client = session.client.clone();
            let started = Instant::now();
            let result = release_windows(
                &mut client,
                release_request(session, reply, outcome, false),
                self.timeout,
            )
            .await;
            self.observe(Operation::Release, started.elapsed(), result.is_ok());
            failed |= result.is_err();
        }
        failed
    }

    async fn read_prepared_group(
        &self,
        session: &PeerSession,
        prepared: Vec<PreparedWindow>,
        output_len: usize,
        deadline: Instant,
        crc: Option<&CrcLookup>,
    ) -> Result<Vec<PreparedChunk>, WindowFailure> {
        let Some((_, first_reply, lane_id)) = prepared.first() else {
            return Ok(Vec::new());
        };
        let lane_id = *lane_id;
        let peer = first_reply.engine_id.clone();
        let mut total = 0_usize;
        for (window, _, lane) in &prepared {
            if *lane != lane_id {
                let failure =
                    WindowFailure::from(dfs_protocol_error("HIXL group spans more than one lane"));
                return Err(self
                    .released_failure(
                        session,
                        &prepared,
                        DfsHixlReleaseOutcome::NotSubmitted,
                        failure,
                    )
                    .await);
            }
            if let Err(error) = validate_output_scatter(window, output_len) {
                let failure = WindowFailure::from(error);
                return Err(self
                    .released_failure(
                        session,
                        &prepared,
                        DfsHixlReleaseOutcome::NotSubmitted,
                        failure,
                    )
                    .await);
            }
            total = total.checked_add(window.length).ok_or_else(|| {
                WindowFailure::from(dfs_protocol_error("HIXL group length overflow"))
            })?;
        }
        let Some(native) = self.native.lane(lane_id) else {
            let failure = WindowFailure::from(dfs_protocol_error("HIXL prepared lane disappeared"));
            return Err(self
                .released_failure(
                    session,
                    &prepared,
                    DfsHixlReleaseOutcome::NotSubmitted,
                    failure,
                )
                .await);
        };
        let buffers_started = Instant::now();
        let native_buffer = native.prepare_read_batch_buffer(total);
        self.observe_phase("buffers", buffers_started.elapsed());
        let native_buffer = match native_buffer {
            Ok(native_buffer) => native_buffer,
            Err(error) => {
                let failure = WindowFailure::from_native(error, false);
                return Err(self
                    .released_failure(
                        session,
                        &prepared,
                        DfsHixlReleaseOutcome::NotSubmitted,
                        failure,
                    )
                    .await);
            }
        };
        let windows = prepared
            .iter()
            .map(|(window, reply, _)| (reply.address, window.length))
            .collect::<Vec<_>>();
        let started = Instant::now();
        let transfer = tokio::task::spawn_blocking(move || {
            native.read_batch_timed_into(&peer, &windows, native_buffer, deadline)
        })
        .await;
        let elapsed = started.elapsed();
        self.observe_phase("transfer", elapsed);

        let outcome = match transfer {
            Ok(result) => result,
            Err(error) => {
                self.observe(Operation::Read, elapsed, false);
                let failure = WindowFailure::worker_failed(error.to_string());
                return Err(self
                    .released_failure(session, &prepared, DfsHixlReleaseOutcome::Unknown, failure)
                    .await);
            }
        };
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                self.observe(Operation::Read, elapsed, false);
                if let Some(metrics) = &self.metrics {
                    metrics.observe_lane_read(lane_id, elapsed, false);
                }
                let outcome = failed_read_outcome(&error);
                let failure = WindowFailure::from_native(error, false);
                return Err(self
                    .released_failure(session, &prepared, outcome, failure)
                    .await);
            }
        };
        self.observe(Operation::Read, elapsed, true);
        if let Some(metrics) = &self.metrics {
            metrics.observe_lane_read(lane_id, elapsed, true);
            metrics.observe_read_timing(&outcome.timing);
            metrics.observe_native_batch(prepared.len(), total);
        }
        if outcome.bytes.len() != total {
            let failure = WindowFailure::from(dfs_protocol_error(
                "HIXL group READ returned a short buffer",
            ));
            return Err(self
                .released_failure(
                    session,
                    &prepared,
                    DfsHixlReleaseOutcome::Completed,
                    failure,
                )
                .await);
        }

        let mut failure = None;
        let lengths = prepared
            .iter()
            .map(|(window, _, _)| window.length)
            .collect::<Vec<_>>();
        let replies = prepared
            .iter()
            .map(|(_, reply, _)| reply.clone())
            .collect::<Vec<_>>();
        let crc = crc.cloned();
        let bytes = outcome.bytes;
        let validate_started = Instant::now();
        // Hashing a group waits on the validation pool; keep that wait off the
        // async worker that also serves releases and closes.
        let validated = tokio::task::spawn_blocking(move || {
            let mut ranges = Vec::with_capacity(replies.len());
            let mut offset = 0_usize;
            for (reply, length) in replies.iter().zip(&lengths) {
                let end = offset + length;
                ranges.push((reply, &bytes[offset..end]));
                offset = end;
            }
            let validations = validate_windows(&ranges, crc.as_ref());
            drop(ranges);
            (bytes, validations)
        })
        .await;
        self.observe_phase("validate", validate_started.elapsed());
        let (bytes, window_validations) = match validated {
            Ok(validated) => validated,
            Err(error) => {
                let failure = WindowFailure::from(afs_error::Error::coded(
                    afs_error::CLIENT_WORKER_FAILED,
                    error.to_string(),
                ));
                return Err(self
                    .released_failure(
                        session,
                        &prepared,
                        DfsHixlReleaseOutcome::Completed,
                        failure,
                    )
                    .await);
            }
        };
        let mut validations = Vec::with_capacity(window_validations.len());
        for (elapsed, validation) in window_validations {
            self.observe(Operation::Validate, elapsed, validation.is_ok());
            validations.push(validation);
        }

        let operations = prepared
            .iter()
            .zip(&validations)
            .map(|((_, reply, _), validation)| ReleaseOperation {
                reply,
                outcome: DfsHixlReleaseOutcome::Completed,
                content_validated: validation.is_ok(),
            })
            .collect::<Vec<_>>();
        let release_started = Instant::now();
        let released = match release_batch_request(session, &operations) {
            Ok(request) => {
                let mut client = session.client.clone();
                release_windows(&mut client, request, self.timeout).await
            }
            Err(error) => Err(error),
        };
        let release_elapsed = release_started.elapsed();
        self.observe(Operation::Release, release_elapsed, released.is_ok());
        self.observe_phase("release", release_elapsed);
        let released_ok = released.is_ok();
        if let Err(error) = released {
            merge_window_failure(&mut failure, WindowFailure::from(error));
        }
        for ((window, _, _), validation) in prepared.iter().zip(validations) {
            let content_validated = validation.is_ok();
            if let Err(error) = validation {
                merge_window_failure(&mut failure, WindowFailure::from(error));
            }
            if content_validated
                && released_ok
                && let Some(metrics) = &self.metrics
            {
                match u64::try_from(window.length) {
                    Ok(byte_count) => {
                        metrics.record_validated_bytes(byte_count);
                        metrics.record_lane_validated_bytes(lane_id, byte_count);
                    }
                    Err(_) => merge_window_failure(
                        &mut failure,
                        WindowFailure::from(dfs_protocol_error("HIXL metric byte count overflow")),
                    ),
                }
            }
        }
        match failure {
            Some(failure) => Err(failure),
            None => Ok(vec![PreparedChunk {
                bytes,
                windows: prepared
                    .iter()
                    .map(|(window, _, _)| window.clone())
                    .collect(),
            }]),
        }
    }

    async fn release_group(
        &self,
        session: &PeerSession,
        prepared: &[PreparedWindow],
        outcome: DfsHixlReleaseOutcome,
    ) -> bool {
        let operations = prepared
            .iter()
            .map(|(_, reply, _)| ReleaseOperation {
                reply,
                outcome,
                content_validated: false,
            })
            .collect::<Vec<_>>();
        self.release_batch(session, &operations).await
    }

    async fn release_batch(
        &self,
        session: &PeerSession,
        operations: &[ReleaseOperation<'_>],
    ) -> bool {
        let started = Instant::now();
        let result = match release_batch_request(session, operations) {
            Ok(request) => {
                let mut client = session.client.clone();
                release_windows(&mut client, request, self.timeout).await
            }
            Err(error) => Err(error),
        };
        self.observe(Operation::Release, started.elapsed(), result.is_ok());
        result.is_err()
    }
}

fn merge_window_failure(target: &mut Option<WindowFailure>, failure: WindowFailure) {
    if let Some(first) = target {
        first.fence_peer |= failure.fence_peer;
        first.quarantine_local |= failure.quarantine_local;
    } else {
        *target = Some(failure);
    }
}

/// Remembers which peer generation owns each engine endpoint. Reusing an
/// endpoint across generations needs a restart, because the native connection
/// stays. Endpoints are keyed by their parsed address, so another textual
/// form of the same IP and port is the same engine. Either every engine is
/// bound or none is.
pub(super) fn bind_engines<'a, I: PartialEq>(
    engines: &mut HashMap<SocketAddr, I>,
    lanes: impl IntoIterator<Item = (&'a str, I)>,
    max_engines: usize,
) -> afs_error::Result<()> {
    let mut fresh = Vec::new();
    for (engine_id, identity) in lanes {
        let endpoint = engine_id.parse::<SocketAddr>().map_err(|_| {
            afs_error::Error::coded(
                afs_error::CLIENT_PROTOCOL_VIOLATION,
                "HIXL engine address is invalid",
            )
        })?;
        match engines.get(&endpoint) {
            Some(previous) if previous != &identity => {
                return Err(afs_error::Error::coded(
                    afs_error::NODE_TRANSFER_DRAIN_INCOMPLETE,
                    "HIXL engine address was reused by a different peer generation",
                ));
            }
            Some(_) => {}
            None => fresh.push((endpoint, identity)),
        }
    }
    if engines.len().saturating_add(fresh.len()) > max_engines {
        return Err(afs_error::Error::coded(
            afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
            "HIXL native connection budget exhausted",
        ));
    }
    engines.extend(fresh);
    Ok(())
}

struct ReleaseOperation<'a> {
    reply: &'a DfsPrepareReadRangesHixlReply,
    outcome: DfsHixlReleaseOutcome,
    content_validated: bool,
}

fn release_request(
    session: &PeerSession,
    reply: &DfsPrepareReadRangesHixlReply,
    outcome: DfsHixlReleaseOutcome,
    content_validated: bool,
) -> DfsReleaseHixlReadRequest {
    DfsReleaseHixlReadRequest {
        session_id: reply.session_id,
        read_id: reply.read_id.clone(),
        source_node_id: reply.source_node_id.clone(),
        source_node_epoch: reply.source_node_epoch,
        engine_id: reply.engine_id.clone(),
        engine_generation: reply.engine_generation,
        pool_generation: reply.pool_generation,
        caller_node_epoch: session.caller_epoch,
        windows: vec![DfsHixlReleaseWindow {
            attempt_id: reply.attempt_id.clone(),
            slot_id: reply.slot_id,
            slot_generation: reply.slot_generation,
            outcome: outcome as i32,
            content_validated,
        }],
    }
}

fn release_batch_request(
    session: &PeerSession,
    operations: &[ReleaseOperation<'_>],
) -> afs_error::Result<DfsReleaseHixlReadRequest> {
    let Some(first) = operations.first() else {
        return Err(dfs_protocol_error("HIXL release batch is empty"));
    };
    let reply = first.reply;
    if operations
        .iter()
        .any(|operation| !same_release_identity(reply, operation.reply))
    {
        return Err(dfs_protocol_error(
            "HIXL release batch spans multiple source windows",
        ));
    }
    Ok(DfsReleaseHixlReadRequest {
        session_id: reply.session_id,
        read_id: reply.read_id.clone(),
        source_node_id: reply.source_node_id.clone(),
        source_node_epoch: reply.source_node_epoch,
        engine_id: reply.engine_id.clone(),
        engine_generation: reply.engine_generation,
        pool_generation: reply.pool_generation,
        caller_node_epoch: session.caller_epoch,
        windows: operations
            .iter()
            .map(|operation| DfsHixlReleaseWindow {
                attempt_id: operation.reply.attempt_id.clone(),
                slot_id: operation.reply.slot_id,
                slot_generation: operation.reply.slot_generation,
                outcome: operation.outcome as i32,
                content_validated: operation.content_validated,
            })
            .collect(),
    })
}

fn same_release_identity(
    left: &DfsPrepareReadRangesHixlReply,
    right: &DfsPrepareReadRangesHixlReply,
) -> bool {
    left.session_id == right.session_id
        && left.read_id == right.read_id
        && left.source_node_id == right.source_node_id
        && left.source_node_epoch == right.source_node_epoch
        && left.engine_id == right.engine_id
        && left.engine_generation == right.engine_generation
        && left.pool_generation == right.pool_generation
}

async fn release_windows(
    client: &mut DfsChunksClient<Channel>,
    request: DfsReleaseHixlReadRequest,
    timeout: Duration,
) -> afs_error::Result<()> {
    let mut last_error = None;
    for _ in 0..3 {
        let mut command = afs_tracing::request_with_current_context(request.clone());
        command.set_timeout(timeout);
        match client.release_hixl_read(command).await {
            Ok(reply) => {
                let reply = reply.into_inner();
                if reply.windows.len() != request.windows.len() {
                    return Err(dfs_protocol_error(
                        "HIXL release reply does not cover every prepared window",
                    ));
                }
                for (window, result) in request.windows.iter().zip(&reply.windows) {
                    let outcome = DfsHixlReleaseOutcome::try_from(window.outcome)
                        .map_err(|_| dfs_protocol_error("HIXL release outcome is unknown"))?;
                    if outcome == DfsHixlReleaseOutcome::Unknown {
                        if result.consumed {
                            return Err(dfs_protocol_error(
                                "HIXL source released an unknown native outcome",
                            ));
                        }
                    } else if !result.consumed {
                        return Err(dfs_protocol_error(
                            "HIXL completed source window was not released",
                        ));
                    }
                }
                return Ok(());
            }
            Err(status) => {
                let error = status_to_error(status);
                if !matches!(
                    error.kind(),
                    afs_error::ErrorKind::Unavailable | afs_error::ErrorKind::DeadlineExceeded
                ) {
                    return Err(error);
                }
                last_error = Some(error);
            }
        }
    }
    Err(last_error.unwrap_or_else(|| dfs_protocol_error("HIXL release retry produced no result")))
}

async fn close_session(session: &PeerSession, timeout: Duration) -> afs_error::Result<()> {
    let mut client = session.client.clone();
    let mut request = afs_tracing::request_with_current_context(DfsCloseHixlRequest {
        session_id: session.reply.session_id,
        caller_node_epoch: session.caller_epoch,
        source_node_epoch: session.reply.source_node_epoch,
        engine_id: session.reply.engine_id.clone(),
        engine_generation: session.reply.engine_generation,
    });
    request.set_timeout(timeout);
    let reply = client
        .close_hixl(request)
        .await
        .map_err(status_to_error)?
        .into_inner();
    if !reply.drained {
        return Err(afs_error::Error::coded(
            afs_error::NODE_TRANSFER_DRAIN_INCOMPLETE,
            "HIXL source session remains unresolved",
        ));
    }
    Ok(())
}

async fn retire_idle_session(slot: &mut PeerSlot, timeout: Duration) -> bool {
    // The per-peer guard and non-poisoned state prove every previous window
    // was released. Retiring this empty control session does not reset the
    // native connection or free any memory if the close response is lost.
    if let Some(session) = slot.session.take() {
        return close_session(&session, timeout).await.is_ok();
    }
    true
}

/// Drops a slot without a session from the peer table. The caller holds the
/// slot guard, so no queued read can publish a session into the removed slot;
/// queued reads see the slot replaced and start over.
async fn retire_unbound_peer(slots: &PeerSlots, key: &PeerKey, expected: &Arc<Mutex<PeerSlot>>) {
    let mut slots = slots.lock().await;
    if slots
        .get(key)
        .is_some_and(|current| Arc::ptr_eq(current, expected))
    {
        slots.remove(key);
    }
}

struct WindowFailure {
    error: afs_error::Error,
    fence_peer: bool,
    quarantine_local: bool,
}

impl WindowFailure {
    /// Keeps this failure as the reported error while taking the stronger
    /// fencing and quarantine demands of a concurrent one.
    fn merged(mut self, other: Self) -> Self {
        self.fence_peer |= other.fence_peer;
        self.quarantine_local |= other.quarantine_local;
        self
    }

    /// A prepare that ran out of capacity did not corrupt the session: the
    /// source still serves narrower waves, so the caller retries with fewer
    /// windows instead of fencing the peer and losing the group.
    fn from_prepare(error: afs_error::Error) -> Self {
        let fence_peer = error.code() != afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED
            && error.code() != afs_error::IO_CAPACITY;
        Self {
            error,
            fence_peer,
            quarantine_local: false,
        }
    }

    fn from_native(error: afs_transport::hixl::HixlError, release_failed: bool) -> Self {
        use afs_transport::hixl::HixlErrorKind;
        let quarantine_local = uncertain_read(&error);
        let fence_peer =
            quarantine_local || error.kind != HixlErrorKind::ResourceExhausted || release_failed;
        Self {
            error: runtime_error(error),
            fence_peer,
            quarantine_local,
        }
    }

    fn worker_failed(message: String) -> Self {
        Self {
            error: afs_error::Error::coded(afs_error::CLIENT_WORKER_FAILED, message),
            fence_peer: true,
            quarantine_local: true,
        }
    }
}

impl From<afs_error::Error> for WindowFailure {
    fn from(error: afs_error::Error) -> Self {
        Self {
            error,
            fence_peer: true,
            quarantine_local: false,
        }
    }
}

/// Reports why a prepared HIXL wave failed.
///
/// A failed wave tears its session down and leaves the read engine to retry
/// every window of the wave on its own, so the reason must stay visible at the
/// default log level instead of disappearing into an ordinary retry.
fn report_wave_failure(
    phase: &'static str,
    planned: usize,
    windows: usize,
    failure: &WindowFailure,
) {
    // Capacity shortfalls narrow the wave and retry inside the transport, so
    // they are expected under contention and not worth an error record.
    if failure.error.code() == afs_error::IO_CAPACITY {
        return;
    }
    afs_logging::error!(
        "HIXL prepared wave failed";
        "phase" => phase,
        "planned" => planned,
        "windows" => windows,
        "code" => format!("{:?}", failure.error.code()),
        "fence_peer" => failure.fence_peer,
        "error" => failure.error.to_string()
    );
}

#[cfg(test)]
mod tests;
