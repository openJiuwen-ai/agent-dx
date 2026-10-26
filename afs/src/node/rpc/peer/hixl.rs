//! Reader-initiated immutable DFS Host-to-Host transfers.

use super::{
    GrpcChunkTransfer, PeerConnectionPool, PeerRuntime, dfs_corrupt_error, dfs_protocol_error,
    wire_dfs_read_grant,
};
use crate::node::rpc::hixl::batch::{pack_read_batch, pack_read_group};
use crate::{
    config::HixlDfsReadMode,
    node::{
        dfs_read::{ChunkTransfer, DfsReadPayloadMetrics, PeerReadBatch, ReadGroupPolicy},
        rpc::hixl::{HIXL_PROTOCOL_VERSION, HixlMetrics, Operation, runtime_error},
    },
};
use afs_protocol::node_data::{
    DfsCloseHixlRequest, DfsDigestAlgorithm, DfsHixlGroupWindow, DfsHixlReleaseOutcome,
    DfsHixlReleaseWindow, DfsHixlReleaseWindowResult, DfsHixlTransport, DfsNegotiateHixlReply,
    DfsNegotiateHixlRequest, DfsPrepareReadRangesHixlGroupRequest, DfsPrepareReadRangesHixlReply,
    DfsPrepareReadRangesHixlRequest, DfsReadRangesRequest, DfsReleaseHixlReadRequest,
    dfs_chunks_client::DfsChunksClient,
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

type PlannedWindow = (usize, ReadWindow, u64, usize);
type PreparedWindow = (usize, ReadWindow, DfsPrepareReadRangesHixlReply, usize);
type CompletedWindow = (usize, ReadWindow, Vec<u8>);
type LaneResult = (usize, ReadWindow, Result<Vec<u8>, WindowFailure>);

struct TimedReadResult {
    ordinal: usize,
    window: ReadWindow,
    reply: DfsPrepareReadRangesHixlReply,
    lane_id: usize,
    transfer: Result<HixlReadOutcome, HixlError>,
    elapsed: Duration,
}

struct TimedReadSubmission {
    ordinal: usize,
    window: ReadWindow,
    reply: DfsPrepareReadRangesHixlReply,
    lane_id: usize,
    native: Arc<dyn ReadLane>,
    output: Vec<u8>,
}

/// Native calls are the only replaceable boundary in the reader control path.
trait ReadLane: Send + Sync {
    fn connect(&self, peer: &str, deadline: Instant) -> Result<(), HixlError>;
    fn prepare_read_buffer(&self, length: usize) -> Result<Vec<u8>, HixlError>;
    /// Allocates the private output for one native multi-window batch. The
    /// default keeps single-window backends working.
    fn prepare_read_batch_buffer(&self, length: usize) -> Result<Vec<u8>, HixlError> {
        self.prepare_read_buffer(length)
    }
    fn read(
        &self,
        peer: &str,
        address: u64,
        length: usize,
        deadline: Instant,
    ) -> Result<Vec<u8>, HixlError>;
    fn read_timed_into(
        &self,
        peer: &str,
        address: u64,
        output: Vec<u8>,
        deadline: Instant,
    ) -> Result<HixlReadOutcome, HixlError>;
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

    fn read(
        &self,
        peer: &str,
        address: u64,
        length: usize,
        deadline: Instant,
    ) -> Result<Vec<u8>, HixlError> {
        self.read(peer, address, length, deadline)
    }

    fn prepare_read_buffer(&self, length: usize) -> Result<Vec<u8>, HixlError> {
        self.prepare_read_buffer(length)
    }

    fn prepare_read_batch_buffer(&self, length: usize) -> Result<Vec<u8>, HixlError> {
        HixlRuntime::prepare_read_batch_buffer(self, length)
    }

    fn read_timed_into(
        &self,
        peer: &str,
        address: u64,
        output: Vec<u8>,
        deadline: Instant,
    ) -> Result<HixlReadOutcome, HixlError> {
        self.read_timed_into(peer, address, output, deadline)
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

trait ReadRuntime: Send + Sync {
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

async fn prepare_wave<P, PF, R, RF>(
    wave: &[PlannedWindow],
    mut prepare: P,
    mut release: R,
) -> Result<Vec<PreparedWindow>, WindowFailure>
where
    P: FnMut(ReadWindow, u64, usize) -> PF,
    PF: std::future::Future<Output = Result<DfsPrepareReadRangesHixlReply, WindowFailure>>,
    R: FnMut(DfsPrepareReadRangesHixlReply, DfsHixlReleaseOutcome) -> RF,
    RF: std::future::Future<Output = afs_error::Result<()>>,
{
    let mut prepared = Vec::with_capacity(wave.len());
    for (ordinal, window, offset, lane) in wave {
        match prepare(window.clone(), *offset, *lane).await {
            Ok(reply) => prepared.push((*ordinal, window.clone(), reply, *lane)),
            Err(mut failure) => {
                for (_, _, reply, _) in prepared {
                    failure.fence_peer |= release(reply, DfsHixlReleaseOutcome::NotSubmitted)
                        .await
                        .is_err();
                }
                return Err(failure);
            }
        }
    }
    Ok(prepared)
}

struct WaveCompletion {
    completed: Vec<CompletedWindow>,
    failure: Option<WindowFailure>,
}

async fn collect_wave(mut tasks: tokio::task::JoinSet<LaneResult>) -> WaveCompletion {
    let mut completed = Vec::new();
    let mut failure: Option<WindowFailure> = None;
    // Keep sibling tasks alive even after a failed lane or the caller's timeout.
    while let Some(result) = tasks.join_next().await {
        let error = match result {
            Ok((ordinal, window, Ok(bytes))) => {
                completed.push((ordinal, window, bytes));
                continue;
            }
            Ok((_, _, Err(error))) => error,
            Err(error) => WindowFailure::worker_failed(error.to_string()),
        };
        if let Some(first) = &mut failure {
            first.fence_peer |= error.fence_peer;
            first.quarantine_local |= error.quarantine_local;
        } else {
            failure = Some(error);
        }
    }
    WaveCompletion { completed, failure }
}

#[cfg(test)]
fn pack_windows(
    request: DfsReadRangesRequest,
    capacity: usize,
) -> afs_error::Result<Vec<ReadWindow>> {
    pack_windows_with_ops(request, capacity, crate::node::dfs_read::MAX_DFS_READ_OPS)
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

fn parallel_window_capacity(
    batch: &DfsReadRangesRequest,
    capacity: usize,
    lanes: usize,
) -> afs_error::Result<usize> {
    if lanes == 0 || lanes > 16 || capacity == 0 {
        return Err(dfs_protocol_error(
            "HIXL parallel lane count or capacity is invalid",
        ));
    }
    let bytes = batch
        .operations
        .iter()
        .try_fold(0_usize, |sum, operation| {
            usize::try_from(operation.length)
                .ok()
                .and_then(|length| sum.checked_add(length))
                .ok_or_else(|| dfs_protocol_error("HIXL parallel batch length overflow"))
        })?;
    if bytes == 0 {
        return Err(dfs_protocol_error("HIXL parallel batch is empty"));
    }
    // A lane is an execution resource, not a reason to shrink an IO. Fill the
    // negotiated window before adding another one, even when lanes stay idle.
    Ok(capacity)
}

fn scatter_complete(mut completed: Vec<CompletedWindow>, out: &mut [u8]) -> afs_error::Result<()> {
    // Validate every destination before publishing any bytes, then restore the
    // original request order (including its last-operation-wins overlap rule).
    for (_, window, bytes) in &completed {
        for (destination, range) in &window.mappings {
            if destination
                .checked_add(range.len())
                .is_none_or(|end| end > out.len())
                || bytes.get(range.clone()).is_none()
            {
                return Err(dfs_protocol_error(
                    "HIXL completed scatter exceeds its buffer",
                ));
            }
        }
    }
    completed.sort_by_key(|(ordinal, _, _)| *ordinal);
    for (_, window, bytes) in completed {
        for (destination, range) in window.mappings {
            let end = destination
                .checked_add(range.len())
                .ok_or_else(|| dfs_protocol_error("HIXL scatter overflow"))?;
            let target = out
                .get_mut(destination..end)
                .ok_or_else(|| dfs_protocol_error("HIXL scatter exceeds output"))?;
            let source = bytes
                .get(range)
                .ok_or_else(|| dfs_protocol_error("HIXL scatter exceeds READ"))?;
            target.copy_from_slice(source);
        }
    }
    Ok(())
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
        if range.operation_index != op.operation_index
            || range.chunk_id != op.chunk_id
            || range.chunk_offset != op.chunk_offset
            || range.length != op.length
            || range.packed_offset != op.destination_offset
            || range.source_copy_id != op.source_copy_id
            || range.range_checksum_algorithm != DfsDigestAlgorithm::Blake3 as i32
            || range.range_checksum.len() != 32
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

fn validate_bytes(reply: &DfsPrepareReadRangesHixlReply, bytes: &[u8]) -> afs_error::Result<()> {
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
        if range.range_checksum.as_slice() != blake3::hash(data).as_bytes() {
            return Err(dfs_corrupt_error("HIXL range checksum differs"));
        }
    }
    Ok(())
}

/// Bounded hash pool for native window verification. Every window is hashed
/// independently, so a group completes in roughly one window's hash time
/// instead of the sum of its windows.
fn validation_pool() -> Option<&'static rayon_core::ThreadPool> {
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
        let result = validate_bytes(reply, bytes);
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

fn may_fallback(mode: HixlDfsReadMode, dispatched: bool, error: &afs_error::Error) -> bool {
    mode == HixlDfsReadMode::Preferred
        && !dispatched
        && error.code() == afs_error::NODE_TRANSFER_UNSUPPORTED
        && error.kind() == afs_error::ErrorKind::Unimplemented
}

const MAX_HIXL_PEERS: usize = 16;
const MAX_HIXL_ENGINES: usize = MAX_HIXL_PEERS * 16;

/// A zero idle limit disables retirement for the process lifetime.
fn session_idle_limit(idle_ms: u64) -> Option<Duration> {
    (idle_ms > 0).then(|| Duration::from_millis(idle_ms))
}

type PeerKey = (String, u64, String);
type PeerSlots = Arc<Mutex<HashMap<PeerKey, Arc<Mutex<PeerSlot>>>>>;
type EngineIdentity = (String, u64, u64);
type EngineBindings = Arc<Mutex<HashMap<String, EngineIdentity>>>;

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
}

enum WarmStart {
    Started(Box<WarmTask>),
    InProgress,
    Skip,
}

#[derive(Clone)]
struct WarmTask {
    context: ReadContext,
    key: PeerKey,
    slot: Arc<Mutex<PeerSlot>>,
    node_id: String,
    node_epoch: u64,
    address: String,
    authority: DfsReadRangesRequest,
    caller_epoch: u64,
}

impl WarmTask {
    async fn run(self) {
        let deadline = Instant::now().checked_add(self.context.timeout);
        let outcome = match deadline {
            Some(deadline) => {
                self.context
                    .establish(
                        &self.node_id,
                        self.node_epoch,
                        &self.address,
                        &self.authority,
                        self.caller_epoch,
                        deadline,
                    )
                    .await
            }
            None => Err(dfs_protocol_error("HIXL warm deadline overflow")),
        };
        let unsupported = matches!(&outcome, Ok(None));
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
        drop(guard);
        if let Some(session) = duplicate {
            let _ = close_session(&session, self.context.timeout).await;
        }
        if unsupported {
            retire_unbound_peer(&self.context.slots, &self.key, &self.slot).await;
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
    move_metrics: Option<crate::node::dfs_read::DfsReadMoveMetrics>,
    payload_metrics: Option<DfsReadPayloadMetrics>,
    group: Option<ReadGroupPolicy>,
    receive_slots: usize,
    session_idle: Option<Duration>,
    /// Pins each peer to one device lane so concurrent peers transfer on
    /// different devices. Peers share a lane once every lane is taken.
    lane_bindings: Arc<StdMutex<HashMap<PeerKey, usize>>>,
}

/// Optional reader-pull transport for committed DFS chunks.
/// Construction fails for off mode, zero/excessive deadlines or an invalid runtime.
pub struct HixlChunkTransfer {
    context: ReadContext,
    runtime: PeerRuntime,
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
        Ok(Self {
            context: ReadContext {
                peers,
                native,
                mode,
                timeout,
                slots: Arc::new(Mutex::new(HashMap::new())),
                engines: Arc::new(Mutex::new(HashMap::new())),
                metrics: None,
                move_metrics: None,
                payload_metrics: None,
                group: None,
                receive_slots: 16,
                session_idle: None,
                lane_bindings: Arc::new(StdMutex::new(HashMap::new())),
            },
            runtime: PeerRuntime::current_or_new().map_err(|error| error.0)?,
            next_read: AtomicU64::new(1),
        })
    }

    /// Enables a bounded ReadGroup: one native SDK batch carrying every
    /// prepared window of a single lane.
    pub fn with_read_group(mut self, group: ReadGroupPolicy, receive_slots: usize) -> Self {
        self.context.group = Some(group);
        self.context.receive_slots = receive_slots.max(1);
        self
    }

    fn grpc_fallback(&self, timeout: Duration) -> afs_error::Result<GrpcChunkTransfer> {
        let mut fallback = GrpcChunkTransfer::new(self.context.peers.clone(), timeout)?;
        if let Some(group) = self.context.group {
            fallback = fallback.with_read_group(Some(group.bytes as u64));
        }
        Ok(fallback)
    }

    /// Retires an idle peer session after `idle_ms`; zero keeps it for the process.
    pub fn with_session_idle(mut self, idle_ms: u64) -> Self {
        self.context.session_idle = session_idle_limit(idle_ms);
        self
    }

    pub fn with_metrics(mut self, metrics: HixlMetrics) -> Self {
        self.context.metrics = Some(metrics);
        self
    }

    /// Attaches the shared payload-movement measurement handle. HIXL records its
    /// native window read into it so it compares with the grpc receive phase.
    pub fn with_move_metrics(mut self, metrics: crate::node::dfs_read::DfsReadMoveMetrics) -> Self {
        self.context.move_metrics = Some(metrics);
        self
    }

    /// Enables prepared HIXL payload measurement. Buffers are reserved before
    /// each prepared wave's payload interval; the baseline path stays unchanged.
    pub fn with_payload_metrics(mut self, metrics: DfsReadPayloadMetrics) -> Self {
        self.context.payload_metrics = Some(metrics);
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
}

/// Validated native output that has not yet reached the caller buffer.
struct PreparedChunk {
    bytes: Vec<u8>,
    windows: Vec<ReadWindow>,
}

enum ReadResult {
    Complete(Vec<CompletedWindow>),
    PreparedComplete(Vec<PreparedChunk>),
    Unsupported,
}

enum Acquired {
    Lease(PeerLease),
    Unsupported,
}

impl ChunkTransfer for HixlChunkTransfer {
    fn transport(&self) -> &'static str {
        "hixl"
    }

    /// CRC32C objects are single-range by construction and the native window
    /// protocol carries BLAKE3 digests only, so keep them on the gRPC adapter.
    fn read_crc32c_range(
        &self,
        batch: &PeerReadBatch,
        chunk: &crate::dfs::ChunkObject,
        out: &mut [u8],
    ) -> afs_error::Result<()> {
        GrpcChunkTransfer::new(self.context.peers.clone(), self.context.timeout)?
            .read_crc32c_range(batch, chunk, out)
    }

    fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> afs_error::Result<()> {
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
        if self.context.mode == HixlDfsReadMode::Preferred
            && self.start_warm(
                (node_id.clone(), *node_epoch),
                address.clone(),
                request.clone(),
            )
        {
            let remaining = remaining(deadline)?;
            let result = self.grpc_fallback(remaining)?.read_ranges(batch, out);
            if result.is_ok()
                && let Some(metrics) = &self.context.metrics
            {
                metrics.observe_cold_fallback();
            }
            return result;
        }
        let context = self.context.clone();
        let peer_identity = (node_id.clone(), *node_epoch);
        let cancel = Arc::new(Cancellation::default());
        let prepared_output_len = self.context.payload_metrics.as_ref().map(|_| out.len());
        let result = self.runtime.block_on(async move {
            let command_cancel = cancel.clone();
            let task = tokio::spawn(async move {
                context
                    .read_batch(
                        peer_identity,
                        address,
                        request,
                        prepared_output_len,
                        deadline,
                        command_cancel,
                    )
                    .await
            });
            wait_owned_task(task, deadline, cancel).await
        })?;
        match result {
            ReadResult::Complete(windows) => {
                // Native IO and the control lifecycle own their buffers. Only a
                // complete validated batch reaches this caller-owned slice.
                scatter_complete(windows, out)
            }
            ReadResult::PreparedComplete(chunks) => {
                let scatter_started = Instant::now();
                scatter_prepared(chunks, out)?;
                self.context
                    .observe_phase("scatter", scatter_started.elapsed());
                Ok(())
            }
            ReadResult::Unsupported => {
                let remaining = remaining(deadline)?;
                self.context
                    .observe(Operation::Fallback, started.elapsed(), true);
                self.grpc_fallback(remaining)?.read_ranges(batch, out)
            }
        }
    }
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

fn remaining(deadline: Instant) -> afs_error::Result<Duration> {
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
    if !reply.hixl_supported
        || reply.protocol_version != HIXL_PROTOCOL_VERSION
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

fn validate_lanes(reply: &DfsNegotiateHixlReply, local: &[EngineInfo]) -> afs_error::Result<()> {
    if local.is_empty() || local.len() > 16 || reply.lanes.len() != local.len() {
        return Err(dfs_protocol_error(
            "HIXL negotiated lane count differs from local configuration",
        ));
    }
    let mut engines = std::collections::HashSet::new();
    for (index, lane) in reply.lanes.iter().enumerate() {
        let endpoint = lane
            .engine_id
            .parse::<SocketAddr>()
            .map_err(|_| dfs_protocol_error("HIXL lane endpoint is invalid"))?;
        if usize::try_from(lane.lane_id).ok() != Some(index)
            || lane.engine_generation == 0
            || lane.window_bytes == 0
            || lane.window_bytes > 4 * 1024 * 1024
            || lane.export_slots == 0
            || lane.export_slots > 64
            || endpoint.port() == 0
            || endpoint.ip().is_unspecified()
            || endpoint.ip().is_multicast()
            || matches!(endpoint.ip(), std::net::IpAddr::V4(address) if address.is_broadcast())
            || !engines.insert(endpoint)
        {
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

fn engine_transport_parameters(
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
        if let Some(metrics) = &self.metrics {
            metrics.observe(phase, elapsed, success);
        }
        // The native window READ is the pure payload movement of the HIXL pull;
        // every other phase is protocol and must not enter the movement sample.
        if phase == Operation::Read
            && let Some(metrics) = &self.move_metrics
        {
            let result = if success { "ok" } else { "error" };
            metrics.observe("hixl", result, elapsed);
        }
    }

    fn observe_phase(&self, phase: &'static str, elapsed: Duration) {
        if let Some(metrics) = &self.payload_metrics {
            metrics.observe_phase("hixl", phase, elapsed);
        }
    }

    /// Binds a peer to the least-used device lane. Concurrent peers therefore
    /// transfer on separate devices instead of serializing on one lane; a peer
    /// without a free lane shares the least-loaded one.
    fn bind_lane(&self, key: &PeerKey) -> usize {
        let lanes = self.native.lane_count().max(1);
        if lanes == 1 {
            return 0;
        }
        let mut bindings = match self.lane_bindings.lock() {
            Ok(bindings) => bindings,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(lane) = bindings.get(key) {
            return *lane % lanes;
        }
        // Peer epochs churn on restart; keep the best-effort binding table bounded.
        if bindings.len() >= MAX_HIXL_PEERS * 4 {
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
            key,
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
    /// serialized by the slot's `warming` flag.
    async fn establish(
        &self,
        node_id: &str,
        node_epoch: u64,
        address: &str,
        authority: &DfsReadRangesRequest,
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
            read_request: Some(authority.clone()),
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
            Err(error) if may_fallback(self.mode, false, &error) => return Ok(None),
            Err(error) => return Err(error),
        };
        validate_negotiation(&reply, node_id, node_epoch, &info)?;
        validate_lanes(&reply, &self.native.infos())?;
        {
            let mut engines = self.engines.lock().await;
            for lane in &reply.lanes {
                bind_engine(
                    &mut engines,
                    &DfsNegotiateHixlReply {
                        engine_id: lane.engine_id.clone(),
                        engine_generation: lane.engine_generation,
                        ..reply.clone()
                    },
                )?;
            }
        }
        let start = Instant::now();
        let native = self.native.clone();
        let remote_lanes = reply.lanes.clone();
        let connected = tokio::task::spawn_blocking(move || {
            for (index, remote) in remote_lanes.iter().enumerate() {
                let lane = native
                    .lane(index)
                    .ok_or_else(|| dfs_protocol_error("HIXL local lane disappeared"))?;
                lane.connect(&remote.engine_id, deadline)
                    .map_err(runtime_error)?;
            }
            Ok::<_, afs_error::Error>(())
        })
        .await
        .map_err(|error| {
            afs_error::Error::coded(afs_error::CLIENT_WORKER_FAILED, error.to_string())
        })?;
        self.observe(Operation::Connect, start.elapsed(), connected.is_ok());
        if let Err(error) = connected {
            let session = PeerSession {
                reply,
                caller_epoch,
                client,
                poisoned: Arc::new(AtomicBool::new(true)),
            };
            let _ = close_session(&session, self.timeout).await;
            return Err(error);
        }
        Ok(Some(Arc::new(PeerSession {
            reply,
            caller_epoch,
            client,
            poisoned: Arc::new(AtomicBool::new(false)),
        })))
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
            let guard = slot.clone().try_lock_owned().map_err(|_| {
                afs_error::Error::coded(
                    afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
                    "HIXL peer already has an admitted read",
                )
            })?;
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
        if let Some(session) = &guard.session {
            if session.poisoned.load(Ordering::Acquire) || caller_epoch != session.caller_epoch {
                let start = Instant::now();
                let closed = retire_idle_session(&mut guard, self.timeout).await;
                self.observe(Operation::Close, start.elapsed(), closed);
            } else if let Some(idle) = self.session_idle
                && guard.touched.elapsed() >= idle
            {
                let start = Instant::now();
                let closed = retire_idle_session(&mut guard, self.timeout).await;
                self.observe(Operation::Close, start.elapsed(), closed);
            }
        }
        if guard.session.is_none() {
            match self
                .establish(
                    node_id,
                    node_epoch,
                    address,
                    authority,
                    caller_epoch,
                    deadline,
                )
                .await?
            {
                Some(session) => guard.session = Some(session),
                None => {
                    drop(guard);
                    retire_unbound_peer(&self.slots, &key, &slot).await;
                    return Ok(Acquired::Unsupported);
                }
            }
        }
        let session = guard
            .session
            .as_ref()
            .ok_or_else(|| dfs_protocol_error("HIXL peer session was not published"))?
            .clone();
        guard.touched = Instant::now();
        Ok(Acquired::Lease(PeerLease { session, guard }))
    }

    async fn read_batch(
        &self,
        peer_identity: (String, u64),
        address: String,
        authority: DfsReadRangesRequest,
        prepared_output_len: Option<usize>,
        deadline: Instant,
        cancel: Arc<Cancellation>,
    ) -> afs_error::Result<ReadResult> {
        let prepared_measurement = prepared_output_len.is_some();
        let mut payload_elapsed = Duration::ZERO;
        let mut lease = match self
            .acquire(
                &peer_identity.0,
                peer_identity.1,
                &address,
                &authority,
                deadline,
            )
            .await?
        {
            Acquired::Lease(lease) => lease,
            Acquired::Unsupported => return Ok(ReadResult::Unsupported),
        };
        cancel.publish(lease.session.poisoned.clone())?;
        let lane_count = self.native.lane_count();
        let capacity = lease.session.reply.lanes.iter().try_fold(
            self.native.info().window_bytes,
            |capacity, lane| {
                usize::try_from(lane.window_bytes)
                    .map(|remote| capacity.min(remote))
                    .map_err(|_| dfs_protocol_error("HIXL remote lane capacity overflow"))
            },
        )?;
        let group = self.group;
        let batch = if group.is_some() {
            pack_read_group(authority.clone())?
        } else {
            pack_read_batch(authority.clone())?
        };
        let payload_bytes = if prepared_measurement {
            Some(
                batch
                    .operations
                    .iter()
                    .try_fold(0_usize, |sum, operation| {
                        usize::try_from(operation.length)
                            .ok()
                            .and_then(|length| sum.checked_add(length))
                            .ok_or_else(|| dfs_protocol_error("HIXL payload byte count overflow"))
                    })?,
            )
        } else {
            None
        };
        let capacity = parallel_window_capacity(&batch, capacity, lane_count)?;
        let lane = self.bind_lane(&(peer_identity.0.clone(), peer_identity.1, address.clone()));
        let mut batch_offset = 0_u64;
        let max_window_ops = if group.is_some() {
            crate::node::dfs_read::MAX_DFS_GROUP_OPS
        } else {
            crate::node::dfs_read::MAX_DFS_READ_OPS
        };
        let planned = pack_windows_with_ops(authority, capacity, max_window_ops)?
            .into_iter()
            .enumerate()
            .map(|(index, window)| {
                let offset = batch_offset;
                batch_offset = batch_offset
                    .checked_add(
                        u64::try_from(window.length)
                            .map_err(|_| dfs_protocol_error("HIXL batch length overflow"))?,
                    )
                    .ok_or_else(|| dfs_protocol_error("HIXL batch offset overflow"))?;
                Ok((index, window, offset, lane))
            })
            .collect::<afs_error::Result<Vec<_>>>()?;
        // A group is one native SDK call over every prepared window of the
        // peer's bound lane, so the group path also applies on multi-device
        // readers. The wave never asks for more simultaneous exports than the
        // source lane can hold.
        let export_slots = usize::try_from(
            lease
                .session
                .reply
                .lanes
                .get(lane)
                .map_or(0, |lane| lane.export_slots),
        )
        .unwrap_or(0);
        let group_windows = group
            .filter(|_| prepared_output_len.is_some() && planned.len() > 1)
            .map(|group| {
                group
                    .max_descriptors
                    .min(self.receive_slots)
                    .min(export_slots)
                    .clamp(1, crate::node::dfs_read::MAX_DFS_GROUP_OPS)
            });
        let mut completed = Vec::with_capacity(planned.len());
        let mut prepared_chunks = Vec::new();
        // Every planned window is on the peer's bound lane, so an ungrouped
        // wave is bounded by that lane's export capacity as well.
        let wave_width = group_windows.unwrap_or_else(|| lane_count.min(export_slots).max(1));
        for wave in planned.chunks(wave_width) {
            // Admission remains ordered while native transfers run independently.
            let prepared = if group_windows.is_some() {
                self.prepare_group_windows(&lease.session, wave, &batch, deadline)
                    .await
            } else {
                prepare_wave(
                    wave,
                    |window, offset, lane| {
                        let context = self.clone();
                        let session = lease.session.clone();
                        let batch = batch.clone();
                        let cancel = cancel.clone();
                        async move {
                            if cancel.cancelled.load(Ordering::Acquire) {
                                return Err(WindowFailure::from(afs_error::Error::coded(
                                    afs_error::CLIENT_DEADLINE_EXCEEDED,
                                    "HIXL read cancelled before next window",
                                )));
                            }
                            context
                                .prepare_window(&session, &window, &batch, offset, lane, deadline)
                                .await
                        }
                    },
                    |reply, outcome| {
                        let context = self.clone();
                        let session = lease.session.clone();
                        async move {
                            let mut client = session.client.clone();
                            let started = Instant::now();
                            let result = release_window(
                                &mut client,
                                release_request(&session, &reply, outcome, false),
                                context.timeout,
                            )
                            .await;
                            context.observe(Operation::Release, started.elapsed(), result.is_ok());
                            result
                        }
                    },
                )
                .await
            };
            let prepared = match prepared {
                Ok(prepared) => prepared,
                Err(failure) => {
                    self.observe_prepared_payload_error(payload_elapsed);
                    return Err(self.abort_batch(&mut lease, failure).await);
                }
            };
            if let Some(output_len) = prepared_output_len {
                let read = if group_windows.is_some() {
                    self.read_prepared_group(
                        &lease.session,
                        prepared,
                        output_len,
                        deadline,
                        &mut payload_elapsed,
                    )
                    .await
                } else {
                    self.read_prepared_wave(
                        &lease.session,
                        prepared,
                        output_len,
                        deadline,
                        &mut payload_elapsed,
                    )
                    .await
                };
                match read {
                    Ok(mut chunks) => prepared_chunks.append(&mut chunks),
                    Err(failure) => {
                        self.observe_prepared_payload_error(payload_elapsed);
                        return Err(self.abort_batch(&mut lease, failure).await);
                    }
                }
            } else {
                let mut tasks = tokio::task::JoinSet::new();
                for (ordinal, window, reply, lane) in prepared {
                    let context = self.clone();
                    let session = lease.session.clone();
                    tasks.spawn(async move {
                        let result = context
                            .read_prepared(&session, &window, reply, lane, deadline)
                            .await;
                        (ordinal, window, result)
                    });
                }
                let wave = collect_wave(tasks).await;
                if let Some(failure) = wave.failure {
                    return Err(self.abort_batch(&mut lease, failure).await);
                }
                completed.extend(wave.completed);
            }
        }
        lease.guard.touched = Instant::now();
        if let Err(error) = remaining(deadline) {
            self.observe_prepared_payload_error(payload_elapsed);
            return Err(error);
        }
        if let (Some(metrics), Some(bytes)) = (&self.payload_metrics, payload_bytes) {
            metrics.observe("hixl", "ok", bytes, payload_elapsed);
            Ok(ReadResult::PreparedComplete(prepared_chunks))
        } else {
            Ok(ReadResult::Complete(completed))
        }
    }

    fn observe_prepared_payload_error(&self, elapsed: Duration) {
        if !elapsed.is_zero()
            && let Some(metrics) = &self.payload_metrics
        {
            metrics.observe("hixl", "error", 0, elapsed);
        }
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

    async fn prepare_window(
        &self,
        session: &PeerSession,
        window: &ReadWindow,
        batch: &DfsReadRangesRequest,
        batch_offset: u64,
        lane_id: usize,
        deadline: Instant,
    ) -> Result<DfsPrepareReadRangesHixlReply, WindowFailure> {
        let mut client = session.client.clone();
        let mut request =
            afs_tracing::request_with_current_context(DfsPrepareReadRangesHixlRequest {
                session_id: session.reply.session_id,
                read_request: Some(window.request.clone()),
                caller_node_epoch: session.caller_epoch,
                source_node_epoch: session.reply.source_node_epoch,
                batch_read_request: Some(batch.clone()),
                batch_offset,
                lane_id: u32::try_from(lane_id)
                    .map_err(|_| dfs_protocol_error("HIXL lane index overflow"))?,
            });
        request.set_timeout(remaining(deadline)?);
        let started = Instant::now();
        let response = client
            .prepare_read_ranges_hixl(request)
            .await
            .map_err(status_to_error);
        self.observe(Operation::Prepare, started.elapsed(), response.is_ok());
        let reply = response.map_err(WindowFailure::from_prepare)?.into_inner();
        validate_prepare(window, &session.reply, &reply, lane_id)?;
        Ok(reply)
    }

    /// Prepares every window of one group wave in a single control round trip.
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
            .map(|(_, window, offset, lane)| {
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
            for ((ordinal, window, offset, lane), admitted) in wave.iter().zip(&reply.windows) {
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
                prepared.push((*ordinal, window.clone(), window_reply.clone(), *lane));
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
            let result = release_window(
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

    async fn read_prepared(
        &self,
        session: &PeerSession,
        window: &ReadWindow,
        reply: DfsPrepareReadRangesHixlReply,
        lane_id: usize,
        deadline: Instant,
    ) -> Result<Vec<u8>, WindowFailure> {
        let mut client = session.client.clone();
        let native = self
            .native
            .lane(lane_id)
            .ok_or_else(|| dfs_protocol_error("HIXL prepared lane disappeared"))?;
        let engine = reply.engine_id.clone();
        let address = reply.address;
        let length = window.length;
        let started = Instant::now();
        let transfer =
            tokio::task::spawn_blocking(move || native.read(&engine, address, length, deadline))
                .await
                .map_err(|error| WindowFailure::worker_failed(error.to_string()))?;
        let elapsed = started.elapsed();
        self.observe(Operation::Read, elapsed, transfer.is_ok());
        if let Some(metrics) = &self.metrics {
            metrics.observe_lane_read(lane_id, elapsed, transfer.is_ok());
        }
        let (outcome, validation) = match &transfer {
            Ok(bytes) => {
                let start = Instant::now();
                let result = validate_bytes(&reply, bytes);
                self.observe(Operation::Validate, start.elapsed(), result.is_ok());
                (native_release_outcome(&transfer), result)
            }
            Err(_) => (native_release_outcome(&transfer), Ok(())),
        };
        let release = release_request(
            session,
            &reply,
            outcome,
            transfer.is_ok() && validation.is_ok(),
        );
        let started = Instant::now();
        let released = release_window(&mut client, release, self.timeout).await;
        self.observe(Operation::Release, started.elapsed(), released.is_ok());
        let bytes = match transfer {
            Ok(bytes) => bytes,
            Err(error) => return Err(WindowFailure::from_native(error, released.is_err())),
        };
        validation?;
        released?;
        if let Some(metrics) = &self.metrics {
            let bytes = u64::try_from(bytes.len())
                .map_err(|_| dfs_protocol_error("HIXL metric byte count overflow"))?;
            metrics.record_validated_bytes(bytes);
            metrics.record_lane_validated_bytes(lane_id, bytes);
        }
        Ok(bytes)
    }

    async fn read_prepared_wave(
        &self,
        session: &PeerSession,
        prepared: Vec<PreparedWindow>,
        output_len: usize,
        deadline: Instant,
        payload_elapsed: &mut Duration,
    ) -> Result<Vec<PreparedChunk>, WindowFailure> {
        let mut submissions = Vec::with_capacity(prepared.len());
        let mut preparation_failure = None;
        let buffers_started = Instant::now();
        for (ordinal, window, reply, lane_id) in &prepared {
            if let Err(error) = validate_output_scatter(window, output_len) {
                preparation_failure = Some(WindowFailure::from(error));
                break;
            }
            let Some(native) = self.native.lane(*lane_id) else {
                preparation_failure = Some(WindowFailure::from(dfs_protocol_error(
                    "HIXL prepared lane disappeared",
                )));
                break;
            };
            match native.prepare_read_buffer(window.length) {
                Ok(output) => submissions.push(TimedReadSubmission {
                    ordinal: *ordinal,
                    window: window.clone(),
                    reply: reply.clone(),
                    lane_id: *lane_id,
                    native,
                    output,
                }),
                Err(error) => {
                    preparation_failure = Some(WindowFailure::from_native(error, false));
                    break;
                }
            }
        }
        self.observe_phase("buffers", buffers_started.elapsed());
        if let Some(mut failure) = preparation_failure {
            failure.fence_peer |= self.release_not_submitted_wave(session, &prepared).await;
            return Err(failure);
        }

        let started = Instant::now();
        let mut tasks = tokio::task::JoinSet::new();
        for submission in submissions {
            let peer = submission.reply.engine_id.clone();
            tasks.spawn(async move {
                let TimedReadSubmission {
                    ordinal,
                    window,
                    reply,
                    lane_id,
                    native,
                    output,
                } = submission;
                let read_started = Instant::now();
                let transfer = tokio::task::spawn_blocking(move || {
                    native.read_timed_into(&peer, reply.address, output, deadline)
                })
                .await;
                let elapsed = read_started.elapsed();
                match transfer {
                    Ok(transfer) => Ok(TimedReadResult {
                        ordinal,
                        window,
                        reply,
                        lane_id,
                        transfer,
                        elapsed,
                    }),
                    Err(error) => Err(WindowFailure::worker_failed(error.to_string())),
                }
            });
        }

        let mut results = Vec::with_capacity(prepared.len());
        let mut failure = None;
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(Ok(result)) => results.push(result),
                Ok(Err(error)) => merge_window_failure(&mut failure, error),
                Err(error) => merge_window_failure(
                    &mut failure,
                    WindowFailure::worker_failed(error.to_string()),
                ),
            }
        }

        // Publish only after every window has been validated, so a failed
        // window leaves the caller buffer untouched.
        results.sort_by_key(|result| result.ordinal);
        let all_transfers_succeeded = failure.is_none()
            && results.len() == prepared.len()
            && results.iter().all(|result| result.transfer.is_ok());
        let lengths_match = all_transfers_succeeded
            && results.iter().all(|result| {
                result
                    .transfer
                    .as_ref()
                    .is_ok_and(|outcome| outcome.bytes.len() == result.window.length)
            });
        *payload_elapsed = payload_elapsed.saturating_add(started.elapsed());

        for result in &results {
            self.observe(Operation::Read, result.elapsed, result.transfer.is_ok());
            self.observe_phase("transfer", result.elapsed);
            if let Some(metrics) = &self.metrics {
                metrics.observe_lane_read(result.lane_id, result.elapsed, result.transfer.is_ok());
            }
            if let (Some(metrics), Ok(outcome)) = (&self.metrics, &result.transfer) {
                metrics.observe_read_timing(&outcome.timing);
            }
        }

        static EMPTY_BYTES: [u8; 0] = [];
        let ranges = results
            .iter()
            .map(|result| match &result.transfer {
                Ok(outcome) => (&result.reply, outcome.bytes.as_slice()),
                Err(_) => (&result.reply, EMPTY_BYTES.as_slice()),
            })
            .collect::<Vec<_>>();
        let mut validations = Vec::with_capacity(results.len());
        let validate_started = Instant::now();
        let window_validations = validate_windows(&ranges);
        self.observe_phase("validate", validate_started.elapsed());
        for (result, (elapsed, validation)) in results.iter().zip(window_validations) {
            if result.transfer.is_err() {
                validations.push(Ok(()));
                continue;
            }
            self.observe(Operation::Validate, elapsed, validation.is_ok());
            validations.push(validation);
        }
        if all_transfers_succeeded && !lengths_match {
            merge_window_failure(
                &mut failure,
                WindowFailure::from(dfs_protocol_error(
                    "HIXL native READ returned an unexpected buffer length",
                )),
            );
        }
        let can_publish = lengths_match && validations.iter().all(|validation| validation.is_ok());
        let mut chunks = Vec::with_capacity(results.len());

        for (result, validation) in results.into_iter().zip(validations) {
            let window_length = result.window.length;
            let (release_outcome, transfer_failure) = match &result.transfer {
                Ok(_) => (DfsHixlReleaseOutcome::Completed, None),
                Err(error) => (
                    if error.read_may_be_in_flight {
                        DfsHixlReleaseOutcome::Unknown
                    } else {
                        DfsHixlReleaseOutcome::NotSubmitted
                    },
                    Some(error.clone()),
                ),
            };
            let content_validated = result.transfer.is_ok() && validation.is_ok();
            let mut client = session.client.clone();
            let release_started = Instant::now();
            let released = release_window(
                &mut client,
                release_request(session, &result.reply, release_outcome, content_validated),
                self.timeout,
            )
            .await;
            let release_elapsed = release_started.elapsed();
            self.observe(Operation::Release, release_elapsed, released.is_ok());
            self.observe_phase("release", release_elapsed);
            let release_succeeded = released.is_ok();

            if let Some(error) = transfer_failure {
                merge_window_failure(
                    &mut failure,
                    WindowFailure::from_native(error, released.is_err()),
                );
            }
            if let Err(error) = validation {
                merge_window_failure(&mut failure, WindowFailure::from(error));
            }
            if let Err(error) = released {
                merge_window_failure(&mut failure, WindowFailure::from(error));
            }

            if content_validated
                && release_succeeded
                && let Some(metrics) = &self.metrics
            {
                match u64::try_from(window_length) {
                    Ok(byte_count) => {
                        metrics.record_validated_bytes(byte_count);
                        metrics.record_lane_validated_bytes(result.lane_id, byte_count);
                    }
                    Err(_) => merge_window_failure(
                        &mut failure,
                        WindowFailure::from(dfs_protocol_error("HIXL metric byte count overflow")),
                    ),
                }
            }
            if can_publish
                && content_validated
                && let Ok(outcome) = result.transfer
            {
                chunks.push(PreparedChunk {
                    bytes: outcome.bytes,
                    windows: vec![result.window],
                });
            }
        }

        match failure {
            Some(failure) => Err(failure),
            None => Ok(chunks),
        }
    }

    /// One native SDK call over every prepared window of a single lane. Each
    /// window keeps its own checksum and release record; the validated native
    /// buffer is returned for one scatter into the caller output.
    async fn read_prepared_group(
        &self,
        session: &PeerSession,
        prepared: Vec<PreparedWindow>,
        output_len: usize,
        deadline: Instant,
        payload_elapsed: &mut Duration,
    ) -> Result<Vec<PreparedChunk>, WindowFailure> {
        let Some((_, _, first_reply, lane_id)) = prepared.first() else {
            return Ok(Vec::new());
        };
        let lane_id = *lane_id;
        let peer = first_reply.engine_id.clone();
        let mut total = 0_usize;
        for (_, window, _, lane) in &prepared {
            if *lane != lane_id {
                let mut failure =
                    WindowFailure::from(dfs_protocol_error("HIXL group spans more than one lane"));
                failure.fence_peer |= self.release_not_submitted_wave(session, &prepared).await;
                return Err(failure);
            }
            if let Err(error) = validate_output_scatter(window, output_len) {
                let mut failure = WindowFailure::from(error);
                failure.fence_peer |= self.release_not_submitted_wave(session, &prepared).await;
                return Err(failure);
            }
            total = total.checked_add(window.length).ok_or_else(|| {
                WindowFailure::from(dfs_protocol_error("HIXL group length overflow"))
            })?;
        }
        let Some(native) = self.native.lane(lane_id) else {
            let mut failure =
                WindowFailure::from(dfs_protocol_error("HIXL prepared lane disappeared"));
            failure.fence_peer |= self.release_not_submitted_wave(session, &prepared).await;
            return Err(failure);
        };
        let buffers_started = Instant::now();
        let native_buffer = native.prepare_read_batch_buffer(total);
        self.observe_phase("buffers", buffers_started.elapsed());
        let native_buffer = match native_buffer {
            Ok(native_buffer) => native_buffer,
            Err(error) => {
                let mut failure = WindowFailure::from_native(error, false);
                failure.fence_peer |= self.release_not_submitted_wave(session, &prepared).await;
                return Err(failure);
            }
        };
        let windows = prepared
            .iter()
            .map(|(_, window, reply, _)| (reply.address, window.length))
            .collect::<Vec<_>>();
        let started = Instant::now();
        let transfer = tokio::task::spawn_blocking(move || {
            native.read_batch_timed_into(&peer, &windows, native_buffer, deadline)
        })
        .await;
        let elapsed = started.elapsed();
        *payload_elapsed = payload_elapsed.saturating_add(elapsed);
        self.observe_phase("transfer", elapsed);

        let outcome = match transfer {
            Ok(result) => result,
            Err(error) => {
                self.observe(Operation::Read, elapsed, false);
                let release_failed = self
                    .release_group(session, &prepared, DfsHixlReleaseOutcome::Unknown)
                    .await;
                let mut failure = WindowFailure::worker_failed(error.to_string());
                failure.fence_peer |= release_failed;
                return Err(failure);
            }
        };
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                self.observe(Operation::Read, elapsed, false);
                if let Some(metrics) = &self.metrics {
                    metrics.observe_lane_read(lane_id, elapsed, false);
                }
                let release_outcome = if error.read_may_be_in_flight {
                    DfsHixlReleaseOutcome::Unknown
                } else {
                    DfsHixlReleaseOutcome::NotSubmitted
                };
                let release_failed = self
                    .release_group(session, &prepared, release_outcome)
                    .await;
                return Err(WindowFailure::from_native(error, release_failed));
            }
        };
        self.observe(Operation::Read, elapsed, true);
        if let Some(metrics) = &self.metrics {
            metrics.observe_lane_read(lane_id, elapsed, true);
            metrics.observe_read_timing(&outcome.timing);
            metrics.observe_native_batch(prepared.len(), total);
        }
        if outcome.bytes.len() != total {
            let release_failed = self
                .release_group(session, &prepared, DfsHixlReleaseOutcome::Completed)
                .await;
            let mut failure = WindowFailure::from(dfs_protocol_error(
                "HIXL group READ returned a short buffer",
            ));
            failure.fence_peer |= release_failed;
            return Err(failure);
        }

        let mut failure = None;
        let mut ranges = Vec::with_capacity(prepared.len());
        let mut offset = 0_usize;
        for (_, window, reply, _) in &prepared {
            let end = offset + window.length;
            ranges.push((reply, &outcome.bytes[offset..end]));
            offset = end;
        }
        let mut validations = Vec::with_capacity(ranges.len());
        let validate_started = Instant::now();
        let window_validations = validate_windows(&ranges);
        self.observe_phase("validate", validate_started.elapsed());
        for (elapsed, validation) in window_validations {
            self.observe(Operation::Validate, elapsed, validation.is_ok());
            validations.push(validation);
        }

        let operations = prepared
            .iter()
            .zip(&validations)
            .map(|((_, _, reply, _), validation)| ReleaseOperation {
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
        for ((_, window, _, _), validation) in prepared.iter().zip(validations) {
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
                bytes: outcome.bytes,
                windows: prepared
                    .iter()
                    .map(|(_, window, _, _)| window.clone())
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
            .map(|(_, _, reply, _)| ReleaseOperation {
                reply,
                outcome,
                content_validated: false,
            })
            .collect::<Vec<_>>();
        self.release_batch(session, &operations).await
    }

    async fn release_not_submitted_wave(
        &self,
        session: &PeerSession,
        prepared: &[PreparedWindow],
    ) -> bool {
        let operations = prepared
            .iter()
            .map(|(_, _, reply, _)| ReleaseOperation {
                reply,
                outcome: DfsHixlReleaseOutcome::NotSubmitted,
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
                release_windows(&mut client, request, self.timeout)
                    .await
                    .map(|_| ())
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

fn bind_engine(
    engines: &mut HashMap<String, EngineIdentity>,
    reply: &DfsNegotiateHixlReply,
) -> afs_error::Result<()> {
    let identity = (
        reply.source_node_id.clone(),
        reply.source_node_epoch,
        reply.engine_generation,
    );
    if let Some(previous) = engines.get(&reply.engine_id) {
        if previous != &identity {
            return Err(afs_error::Error::coded(
                afs_error::NODE_TRANSFER_DRAIN_INCOMPLETE,
                "HIXL engine address was reused by a different peer generation",
            ));
        }
        return Ok(());
    }
    if engines.len() >= MAX_HIXL_ENGINES {
        return Err(afs_error::Error::coded(
            afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
            "HIXL native connection budget exhausted",
        ));
    }
    engines.insert(reply.engine_id.clone(), identity);
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

async fn release_window(
    client: &mut DfsChunksClient<Channel>,
    request: DfsReleaseHixlReadRequest,
    timeout: Duration,
) -> afs_error::Result<()> {
    release_windows(client, request, timeout).await.map(|_| ())
}

async fn release_windows(
    client: &mut DfsChunksClient<Channel>,
    request: DfsReleaseHixlReadRequest,
    timeout: Duration,
) -> afs_error::Result<Vec<DfsHixlReleaseWindowResult>> {
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
                return Ok(reply.windows);
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
    if !reply.admitted || !reply.drained {
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

async fn retire_unbound_peer(slots: &PeerSlots, key: &PeerKey, expected: &Arc<Mutex<PeerSlot>>) {
    // The caller still owns the per-peer guard and has no native session.
    let mut slots = slots.lock().await;
    if slots
        .get(key)
        .is_some_and(|current| Arc::ptr_eq(current, expected))
    {
        slots.remove(key);
    }
}

fn native_release_outcome(
    result: &Result<Vec<u8>, afs_transport::hixl::HixlError>,
) -> DfsHixlReleaseOutcome {
    match result {
        Ok(_) => DfsHixlReleaseOutcome::Completed,
        Err(error) if !error.read_may_be_in_flight => DfsHixlReleaseOutcome::NotSubmitted,
        Err(_) => DfsHixlReleaseOutcome::Unknown,
    }
}

struct WindowFailure {
    error: afs_error::Error,
    fence_peer: bool,
    quarantine_local: bool,
}

impl WindowFailure {
    fn from_prepare(error: afs_error::Error) -> Self {
        let fence_peer = error.code() != afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED;
        Self {
            error,
            fence_peer,
            quarantine_local: false,
        }
    }

    fn from_native(error: afs_transport::hixl::HixlError, release_failed: bool) -> Self {
        use afs_transport::hixl::HixlErrorKind;
        let quarantine_local = error.read_may_be_in_flight
            || matches!(
                error.kind,
                HixlErrorKind::EnginePoisoned | HixlErrorKind::ShutdownIncomplete
            );
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HixlDfsReadMode;
    use afs_protocol::node_data::{
        DfsChunkReadOp, DfsDigestAlgorithm, DfsHixlLane, DfsHixlReadRange, DfsHixlTransport,
        DfsReadGrant, DfsReadRangesCompletion,
    };
    use afs_transport::hixl::{EngineInfo, ReadTransport};

    fn control_session() -> Arc<PeerSession> {
        Arc::new(PeerSession {
            reply: descriptor().1,
            caller_epoch: 1,
            client: DfsChunksClient::new(
                tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
            ),
            poisoned: Arc::new(AtomicBool::new(false)),
        })
    }

    #[tokio::test]
    async fn released_idle_session_is_retired_even_when_control_close_fails() {
        let mut slot = PeerSlot {
            session: Some(control_session()),
            touched: Instant::now() - Duration::from_secs(20),
            warming: None,
        };
        assert!(!retire_idle_session(&mut slot, Duration::from_millis(20)).await);
        assert!(
            slot.session.is_none(),
            "a fully released control session must not stick on CloseHixl failure"
        );
    }

    #[test]
    fn zero_session_idle_limit_disables_retirement() {
        assert_eq!(session_idle_limit(0), None);
        assert_eq!(session_idle_limit(1), Some(Duration::from_millis(1)));
        assert_eq!(session_idle_limit(300_000), Some(Duration::from_secs(300)));
    }

    #[tokio::test]
    async fn unsupported_peers_do_not_consume_native_peer_budget() {
        let slots: PeerSlots = Arc::new(Mutex::new(HashMap::new()));
        for index in 0..MAX_HIXL_PEERS + 1 {
            let key = (format!("unsupported-{index}"), 1, "grpc".into());
            let slot = Arc::new(Mutex::new(PeerSlot {
                session: None,
                touched: Instant::now(),
                warming: None,
            }));
            {
                let mut map = slots.lock().await;
                assert!(
                    map.len() < MAX_HIXL_PEERS,
                    "unsupported peers exhausted native admission"
                );
                map.insert(key.clone(), slot.clone());
            }
            retire_unbound_peer(&slots, &key, &slot).await;
        }
        assert!(slots.lock().await.is_empty());
    }

    #[test]
    fn definite_admission_refusal_does_not_become_unknown_dma() {
        use afs_transport::hixl::{HixlError, HixlErrorKind, HixlPhase};
        let mut error = HixlError {
            phase: HixlPhase::SubmitRead,
            native_status: None,
            kind: HixlErrorKind::ResourceExhausted,
            message: "owner queue full".into(),
            read_may_be_in_flight: false,
        };
        assert_eq!(
            native_release_outcome(&Err(error.clone())),
            DfsHixlReleaseOutcome::NotSubmitted
        );
        error.read_may_be_in_flight = true;
        assert_eq!(
            native_release_outcome(&Err(error)),
            DfsHixlReleaseOutcome::Unknown
        );
    }

    #[test]
    fn prepare_capacity_refusal_is_retryable_without_session_fencing() {
        let failure = WindowFailure::from_prepare(afs_error::Error::coded(
            afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
            "export pool full",
        ));
        assert!(!failure.fence_peer);
        let failure = WindowFailure::from_prepare(afs_error::Error::coded(
            afs_error::NODE_TRANSFER_UNAVAILABLE,
            "prepare reply lost",
        ));
        assert!(failure.fence_peer);
    }

    fn request() -> DfsReadRangesRequest {
        DfsReadRangesRequest {
            read_id: "r".into(),
            attempt_id: "a".into(),
            file_version_id: "v".into(),
            layout_root_id: "l".into(),
            include_blake3_outboard: false,
            operations: vec![DfsChunkReadOp {
                operation_index: 0,
                chunk_id: "chunk".into(),
                chunk_offset: 2,
                length: 3,
                destination_offset: 5,
                source_copy_id: "copy".into(),
                grant: Some(DfsReadGrant {
                    token: "grant".into(),
                    ..Default::default()
                }),
            }],
        }
    }

    fn descriptor() -> (
        ReadWindow,
        DfsNegotiateHixlReply,
        DfsPrepareReadRangesHixlReply,
    ) {
        let mut request = request();
        request.operations[0].destination_offset = 0;
        let window = ReadWindow {
            request: request.clone(),
            mappings: vec![(5, 0..3)],
            length: 3,
        };
        let session = DfsNegotiateHixlReply {
            session_id: 1,
            hixl_supported: true,
            source_node_id: "source".into(),
            source_node_epoch: 7,
            engine_id: "127.0.0.1:7920".into(),
            engine_generation: 9,
            window_bytes: 4,
            export_slots: 2,
            protocol_version: HIXL_PROTOCOL_VERSION,
            transport: DfsHixlTransport::Roce as i32,
            buffer_pool_count: 0,
            buffer_pool_size_mib: 0,
            hccs_domain_id: None,
            lanes: vec![DfsHixlLane {
                lane_id: 0,
                engine_id: "127.0.0.1:7920".into(),
                engine_generation: 9,
                window_bytes: 4,
                export_slots: 2,
            }],
        };
        let checksum = blake3::hash(b"abc").as_bytes().to_vec();
        let reply = DfsPrepareReadRangesHixlReply {
            session_id: 1,
            read_id: request.read_id,
            attempt_id: request.attempt_id,
            source_node_id: session.source_node_id.clone(),
            source_node_epoch: 7,
            engine_id: session.engine_id.clone(),
            engine_generation: 9,
            pool_generation: 9,
            slot_id: 0,
            slot_generation: 1,
            address: 4096,
            capacity: 4,
            packed_length: 3,
            ranges: vec![DfsHixlReadRange {
                operation_index: 0,
                chunk_id: "chunk".into(),
                chunk_offset: 2,
                length: 3,
                packed_offset: 0,
                source_copy_id: "copy".into(),
                range_checksum: checksum.clone(),
                range_checksum_algorithm: DfsDigestAlgorithm::Blake3 as i32,
            }],
            completions: vec![DfsReadRangesCompletion {
                crc32c_table: Default::default(),
                read_id: "r".into(),
                attempt_id: "a".into(),
                operation_index: 0,
                source_copy_id: "copy".into(),
                transferred_bytes: 3,
                range_checksum: checksum,
                range_checksum_algorithm: DfsDigestAlgorithm::Blake3 as i32,
            }],
        };
        (window, session, reply)
    }

    fn engine_info(
        transport: ReadTransport,
        count: usize,
        size_mib: usize,
        hccs_domain_id: Option<u64>,
    ) -> EngineInfo {
        EngineInfo {
            engine_id: "client-engine".into(),
            generation: 1,
            window_bytes: 4,
            transport,
            buffer_pool_count: count,
            buffer_pool_size_mib: size_mib,
            hccs_domain_id,
        }
    }

    #[test]
    fn negotiation_accepts_matching_roce_and_hccs_parameters() {
        let (_, mut reply, _) = descriptor();
        validate_negotiation(
            &reply,
            "source",
            7,
            &engine_info(ReadTransport::Roce, 0, 0, None),
        )
        .unwrap();

        reply.transport = DfsHixlTransport::HccsBufferPool as i32;
        reply.buffer_pool_count = 4;
        reply.buffer_pool_size_mib = 64;
        reply.hccs_domain_id = Some(0);
        validate_negotiation(
            &reply,
            "source",
            7,
            &engine_info(ReadTransport::HccsBufferPool, 4, 64, Some(0)),
        )
        .unwrap();
    }

    #[test]
    fn negotiation_rejects_unknown_or_mismatched_transport_parameters() {
        let (_, reply, _) = descriptor();
        let local_roce = engine_info(ReadTransport::Roce, 0, 0, None);
        let mut cases = Vec::new();

        let mut unknown = reply.clone();
        unknown.transport = 99;
        cases.push((unknown, local_roce.clone()));

        let mut roce_pool = reply.clone();
        roce_pool.buffer_pool_count = 1;
        cases.push((roce_pool, local_roce.clone()));

        let mut roce_domain = reply.clone();
        roce_domain.hccs_domain_id = Some(0);
        cases.push((roce_domain, local_roce.clone()));

        let mut hccs_reply = reply.clone();
        hccs_reply.transport = DfsHixlTransport::HccsBufferPool as i32;
        hccs_reply.buffer_pool_count = 4;
        hccs_reply.buffer_pool_size_mib = 64;
        cases.push((
            hccs_reply.clone(),
            engine_info(ReadTransport::HccsBufferPool, 8, 64, Some(0)),
        ));
        hccs_reply.buffer_pool_size_mib = 32;
        cases.push((
            hccs_reply,
            engine_info(ReadTransport::HccsBufferPool, 4, 64, Some(0)),
        ));

        let mut empty_hccs = reply;
        empty_hccs.transport = DfsHixlTransport::HccsBufferPool as i32;
        cases.push((
            empty_hccs,
            engine_info(ReadTransport::HccsBufferPool, 4, 64, Some(0)),
        ));

        let mut missing_domain = descriptor().1;
        missing_domain.transport = DfsHixlTransport::HccsBufferPool as i32;
        missing_domain.buffer_pool_count = 4;
        missing_domain.buffer_pool_size_mib = 64;
        cases.push((
            missing_domain,
            engine_info(ReadTransport::HccsBufferPool, 4, 64, Some(0)),
        ));

        let mut wrong_domain = descriptor().1;
        wrong_domain.transport = DfsHixlTransport::HccsBufferPool as i32;
        wrong_domain.buffer_pool_count = 4;
        wrong_domain.buffer_pool_size_mib = 64;
        wrong_domain.hccs_domain_id = Some(7);
        cases.push((
            wrong_domain,
            engine_info(ReadTransport::HccsBufferPool, 4, 64, Some(0)),
        ));

        for (bad_reply, local) in cases {
            let error = validate_negotiation(&bad_reply, "source", 7, &local)
                .expect_err("invalid HIXL transport negotiation");
            assert_eq!(error.code(), afs_error::CLIENT_PROTOCOL_VIOLATION);
            assert!(!may_fallback(HixlDfsReadMode::Preferred, false, &error));
        }
    }

    #[test]
    fn negotiation_rejects_older_protocol_versions() {
        for version in [1, 2, 3] {
            let (_, mut reply, _) = descriptor();
            reply.protocol_version = version;
            let error = validate_negotiation(
                &reply,
                "source",
                7,
                &engine_info(ReadTransport::Roce, 0, 0, None),
            )
            .expect_err("older HIXL replies must be rejected");
            assert_eq!(error.code(), afs_error::CLIENT_PROTOCOL_VIOLATION);
        }
    }

    #[tokio::test]
    async fn prepare_wave_refusal_releases_all_admitted_windows_as_not_submitted() {
        let (window, _, _) = descriptor();
        let planned = (0..3)
            .map(|index| (index, window.clone(), index as u64 * 3, index))
            .collect::<Vec<_>>();
        let attempts = Arc::new(AtomicU64::new(0));
        let releases = Arc::new(StdMutex::new(Vec::new()));
        let result = prepare_wave(
            &planned,
            |_, _, lane| {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, Ordering::Relaxed);
                    if lane == 2 {
                        return Err(WindowFailure::from_prepare(afs_error::Error::coded(
                            afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
                            "third lane refused",
                        )));
                    }
                    Ok(DfsPrepareReadRangesHixlReply {
                        slot_id: lane as u64,
                        ..Default::default()
                    })
                }
            },
            |reply, outcome| {
                let releases = releases.clone();
                async move {
                    releases.lock().unwrap().push((reply.slot_id, outcome));
                    Ok(())
                }
            },
        )
        .await;
        let failure = result
            .err()
            .expect("prepare must refuse the wave before any READ");
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
        assert_eq!(
            *releases.lock().unwrap(),
            vec![
                (0, DfsHixlReleaseOutcome::NotSubmitted),
                (1, DfsHixlReleaseOutcome::NotSubmitted)
            ]
        );
        assert!(!failure.fence_peer);
        assert!(!failure.quarantine_local);
    }

    #[tokio::test]
    async fn collect_wave_waits_for_sibling_cleanup_after_unknown_lane_failure() {
        use afs_transport::hixl::{HixlError, HixlErrorKind, HixlPhase};

        let (window, _, _) = descriptor();
        let other = window.clone();
        let releases = Arc::new(AtomicU64::new(0));
        let (failed, failure_seen) = tokio::sync::oneshot::channel();
        let (finish, finishing) = tokio::sync::oneshot::channel();
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move {
            failed.send(()).unwrap();
            (
                0,
                window,
                Err(WindowFailure::from_native(
                    HixlError {
                        phase: HixlPhase::SubmitRead,
                        native_status: None,
                        kind: HixlErrorKind::EnginePoisoned,
                        message: "unknown DMA".into(),
                        read_may_be_in_flight: true,
                    },
                    false,
                )),
            )
        });
        let sibling_releases = releases.clone();
        tasks.spawn(async move {
            finishing.await.unwrap();
            sibling_releases.fetch_add(1, Ordering::Release);
            let bytes = vec![7; other.length];
            (1, other, Ok(bytes))
        });
        let collection = tokio::spawn(collect_wave(tasks));
        failure_seen.await.unwrap();
        tokio::task::yield_now().await;
        assert!(!collection.is_finished());
        assert_eq!(releases.load(Ordering::Acquire), 0);
        finish.send(()).unwrap();
        let wave = collection.await.unwrap();
        assert!(wave.failure.unwrap().quarantine_local);
        assert_eq!(releases.load(Ordering::Acquire), 1);
        assert_eq!(wave.completed.len(), 1);
    }

    #[tokio::test]
    async fn outer_timeout_keeps_all_lane_cleanup_alive() {
        let (window, _, _) = descriptor();
        let releases = Arc::new(AtomicU64::new(0));
        let mut finishers = Vec::new();
        let mut tasks = tokio::task::JoinSet::new();
        for lane in 0..2 {
            let (finish, finishing) = tokio::sync::oneshot::channel();
            finishers.push(finish);
            let window = window.clone();
            let releases = releases.clone();
            tasks.spawn(async move {
                finishing.await.unwrap();
                releases.fetch_add(1, Ordering::Release);
                let bytes = vec![7; window.length];
                (lane, window, Ok(bytes))
            });
        }
        let cancel = Arc::new(Cancellation::default());
        let peer = Arc::new(AtomicBool::new(false));
        cancel.publish(peer.clone()).unwrap();
        let (returned, returning) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let wave = collect_wave(tasks).await;
            assert!(wave.failure.is_none());
            returned.send(wave.completed.len()).unwrap();
            Ok(())
        });
        let error = wait_owned_task(
            task,
            Instant::now() + Duration::from_millis(10),
            cancel.clone(),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), afs_error::CLIENT_DEADLINE_EXCEEDED);
        assert!(cancel.cancelled.load(Ordering::Acquire));
        assert!(peer.load(Ordering::Acquire));
        assert_eq!(releases.load(Ordering::Acquire), 0);
        for finish in finishers {
            finish.send(()).unwrap();
        }
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), returning)
                .await
                .unwrap()
                .unwrap(),
            2
        );
        assert_eq!(releases.load(Ordering::Acquire), 2);
    }

    #[test]
    fn out_of_order_completion_preserves_overlap_order_and_atomic_scatter_validation() {
        let mut authority = request();
        authority.operations[0].length = 4;
        authority.operations[0].destination_offset = 0;
        authority.operations[0].chunk_id = "first".into();
        let mut second = authority.operations[0].clone();
        second.operation_index = 1;
        second.chunk_id = "last".into();
        authority.operations.push(second);
        let mut completed = pack_windows(authority, 2)
            .unwrap()
            .into_iter()
            .enumerate()
            .map(|(ordinal, window)| {
                let byte = if window.request.operations[0].chunk_id == "first" {
                    b'a'
                } else {
                    b'b'
                };
                let bytes = vec![byte; window.length];
                (ordinal, window, bytes)
            })
            .collect::<Vec<_>>();
        completed.reverse();
        let mut output = [0; 4];
        scatter_complete(completed.clone(), &mut output).unwrap();
        assert_eq!(&output, b"bbbb");
        completed[0].1.mappings[0].0 = usize::MAX;
        let mut unchanged = [9; 4];
        assert!(scatter_complete(completed, &mut unchanged).is_err());
        assert_eq!(unchanged, [9; 4]);
    }

    #[test]
    fn only_uncertain_local_native_lifetimes_quarantine_the_local_group() {
        use afs_transport::hixl::{HixlError, HixlErrorKind, HixlPhase};
        for code in [
            afs_error::NODE_TRANSFER_INVALID,
            afs_error::NODE_TRANSFER_CORRUPT_DATA,
            afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
        ] {
            assert!(
                !WindowFailure::from_prepare(afs_error::Error::coded(code, "source refused"))
                    .quarantine_local
            );
        }
        let mut native = HixlError {
            phase: HixlPhase::SubmitRead,
            native_status: None,
            kind: HixlErrorKind::ResourceExhausted,
            message: "queue full".into(),
            read_may_be_in_flight: false,
        };
        assert!(!WindowFailure::from_native(native.clone(), true).quarantine_local);
        native.read_may_be_in_flight = true;
        assert!(WindowFailure::from_native(native, false).quarantine_local);
        assert!(WindowFailure::worker_failed("unmanaged lane".into()).quarantine_local);
    }

    #[test]
    fn parallel_windows_fill_capacity_and_preserve_packed_identity() {
        let mut authority = request();
        authority.operations[0].length = 17;
        let batch = pack_read_batch(authority.clone()).unwrap();
        for lane_count in [1, 2, 4, 8, 16] {
            let capacity = parallel_window_capacity(&batch, 4, lane_count).unwrap();
            let windows = pack_windows(authority.clone(), capacity).unwrap();
            assert_eq!(
                windows
                    .iter()
                    .map(|window| window.length)
                    .collect::<Vec<_>>(),
                vec![4, 4, 4, 4, 1],
                "extra lanes must not multiply windows for the same batch"
            );
            let mut offset = 0_u64;
            for window in windows {
                assert_eq!(
                    crate::node::rpc::hixl::batch::slice_read_batch(
                        &batch,
                        offset,
                        u64::try_from(window.length).unwrap(),
                        &window.request.attempt_id
                    )
                    .unwrap(),
                    window.request
                );
                offset += u64::try_from(window.length).unwrap();
            }
            assert_eq!(offset, 17);
        }
        assert!(parallel_window_capacity(&batch, 4, 0).is_err());
        assert!(parallel_window_capacity(&batch, 4, 17).is_err());
    }

    #[test]
    fn lane_negotiation_rejects_count_order_duplicate_engine_and_primary_mismatch() {
        let (_, reply, _) = descriptor();
        let local = engine_info(ReadTransport::Roce, 0, 0, None);
        validate_lanes(&reply, std::slice::from_ref(&local)).unwrap();
        let mut two = reply.clone();
        let mut second = two.lanes[0].clone();
        second.lane_id = 1;
        second.engine_id = "127.0.0.1:7921".into();
        two.lanes.push(second);
        validate_lanes(&two, &[local.clone(), local.clone()]).unwrap();
        assert!(validate_lanes(&two, std::slice::from_ref(&local)).is_err());
        let mut wrong_order = two.clone();
        wrong_order.lanes.swap(0, 1);
        assert!(validate_lanes(&wrong_order, &[local.clone(), local.clone()]).is_err());
        let mut duplicate = two;
        duplicate.lanes[1].engine_id = duplicate.lanes[0].engine_id.clone();
        assert!(validate_lanes(&duplicate, &[local.clone(), local.clone()]).is_err());
        duplicate.engine_id = "[::1]:7920".into();
        duplicate.lanes[0].engine_id = duplicate.engine_id.clone();
        duplicate.lanes[1].engine_id = "[0:0:0:0:0:0:0:1]:7920".into();
        assert!(validate_lanes(&duplicate, &[local.clone(), local.clone()]).is_err());
        let mut wrong_primary = reply;
        wrong_primary.lanes[0].engine_generation += 1;
        assert!(validate_lanes(&wrong_primary, &[local]).is_err());
    }

    #[test]
    fn packing_splits_large_operation_and_preserves_grant_and_scatter() {
        let mut request = request();
        request.operations[0].length = 7;
        let windows = pack_windows(request, 4).unwrap();
        assert_eq!(windows.len(), 2);
        assert_eq!(
            windows.iter().map(|w| w.length).collect::<Vec<_>>(),
            vec![4, 3]
        );
        assert_eq!(windows[0].mappings, vec![(5, 0..4)]);
        assert_eq!(windows[1].mappings, vec![(9, 0..3)]);
        assert_eq!(windows[1].request.operations[0].chunk_offset, 6);
        assert_ne!(windows[0].request.attempt_id, windows[1].request.attempt_id);
        for window in windows {
            assert_eq!(
                window.request.operations[0].grant.as_ref().unwrap().token,
                "grant"
            );
        }
    }

    #[test]
    fn packed_parent_slices_match_windows_and_scatter_to_original_destinations() {
        use crate::node::rpc::hixl::batch::slice_read_batch;

        let mut authority = request();
        authority.attempt_id = "parent-attempt".into();
        let mut first = authority.operations.remove(0);
        first.chunk_id = "chunk-a".into();
        first.chunk_offset = 10;
        first.length = 5;
        first.destination_offset = 3;
        first.source_copy_id = "copy-a".into();
        first.grant.as_mut().unwrap().token = "grant-a".into();

        let mut second = first.clone();
        second.operation_index = 1;
        second.chunk_id = "chunk-b".into();
        second.chunk_offset = 40;
        second.length = 4;
        second.destination_offset = 12;
        second.source_copy_id = "copy-b".into();
        second.grant.as_mut().unwrap().token = "grant-b".into();

        let mut third = first.clone();
        third.operation_index = 2;
        third.chunk_offset = 90;
        third.length = 3;
        third.destination_offset = 22;
        third.source_copy_id = "copy-c".into();
        authority.operations = vec![first, second, third];

        let packed = pack_read_batch(authority.clone()).unwrap();
        assert_eq!(
            packed
                .operations
                .iter()
                .map(|operation| operation.destination_offset)
                .collect::<Vec<_>>(),
            vec![0, 5, 9]
        );

        let sources = std::collections::HashMap::from([
            (
                "chunk-a".to_owned(),
                (0..128)
                    .map(|byte| u8::try_from(byte).unwrap())
                    .collect::<Vec<_>>(),
            ),
            (
                "chunk-b".to_owned(),
                (128..256)
                    .map(|byte| u8::try_from(byte).unwrap())
                    .collect::<Vec<_>>(),
            ),
        ]);
        let mut expected = vec![0_u8; 25];
        for operation in &authority.operations {
            let source = sources
                .get(&operation.chunk_id)
                .expect("fixture source chunk exists");
            let destination = usize::try_from(operation.destination_offset).unwrap();
            let source_start = usize::try_from(operation.chunk_offset).unwrap();
            let length = usize::try_from(operation.length).unwrap();
            expected[destination..destination + length]
                .copy_from_slice(&source[source_start..source_start + length]);
        }

        for capacity in [1, 3, 4] {
            let windows = pack_windows(authority.clone(), capacity).unwrap();
            assert!(
                windows.len() > 1,
                "capacity {capacity} must split the batch"
            );
            let mut output = vec![0_u8; expected.len()];
            let mut batch_offset = 0_u64;

            for window in &windows {
                let window_length = u64::try_from(window.length).unwrap();
                let sliced = slice_read_batch(
                    &packed,
                    batch_offset,
                    window_length,
                    &window.request.attempt_id,
                )
                .unwrap();
                assert_eq!(
                    sliced,
                    window.request.clone(),
                    "capacity {capacity}, batch offset {batch_offset}"
                );

                let mut window_bytes = vec![0_u8; window.length];
                for operation in &window.request.operations {
                    let source = sources
                        .get(&operation.chunk_id)
                        .expect("fixture source chunk exists");
                    let source_start = usize::try_from(operation.chunk_offset).unwrap();
                    let packed_start = usize::try_from(operation.destination_offset).unwrap();
                    let length = usize::try_from(operation.length).unwrap();
                    window_bytes[packed_start..packed_start + length]
                        .copy_from_slice(&source[source_start..source_start + length]);
                }
                for (destination, packed_range) in &window.mappings {
                    let end = destination.checked_add(packed_range.len()).unwrap();
                    output[*destination..end].copy_from_slice(&window_bytes[packed_range.clone()]);
                }
                batch_offset = batch_offset.checked_add(window_length).unwrap();
            }

            assert_eq!(batch_offset, 12);
            assert_eq!(output, expected, "capacity {capacity}");
        }
    }

    #[test]
    fn descriptor_rejects_stale_identity_overflow_or_bad_ranges_before_read() {
        let (window, session, reply) = descriptor();
        validate_prepare(&window, &session, &reply, 0).unwrap();
        let mut populated_range = reply.clone();
        populated_range.capacity = populated_range.packed_length;
        validate_prepare(&window, &session, &populated_range, 0).unwrap();
        for change in 0..9 {
            let mut bad = reply.clone();
            match change {
                0 => bad.engine_generation += 1,
                1 => bad.source_node_epoch += 1,
                2 => bad.slot_id = 2,
                3 => bad.address = u64::MAX,
                4 => bad.packed_length = 5,
                5 => bad.ranges[0].chunk_offset += 1,
                6 => bad.completions[0].source_copy_id = "wrong".into(),
                7 => bad.ranges[0].range_checksum.pop().map(|_| ()).unwrap(),
                _ => bad.slot_generation = 0,
            }
            assert!(
                validate_prepare(&window, &session, &bad, 0).is_err(),
                "mutation {change}"
            );
        }
    }

    #[test]
    fn completed_native_read_still_requires_exact_bytes_and_checksum() {
        let (_, _, reply) = descriptor();
        validate_bytes(&reply, b"abc").unwrap();
        assert_eq!(
            validate_bytes(&reply, b"abd").unwrap_err().code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
        assert!(validate_bytes(&reply, b"ab").is_err());
    }

    #[test]
    fn parallel_window_validation_preserves_order_and_reports_each_digest() {
        let (_, _, good) = descriptor();
        let mut bad = good.clone();
        bad.ranges[0].range_checksum = blake3::hash(b"other").as_bytes().to_vec();
        let windows = vec![
            (&good, b"abc".as_slice()),
            (&bad, b"abc".as_slice()),
            (&good, b"abc".as_slice()),
        ];
        let observed = validate_windows(&windows);
        assert_eq!(observed.len(), 3);
        assert!(observed[0].1.is_ok());
        assert_eq!(
            observed[1]
                .1
                .as_ref()
                .expect_err("mismatched digest must fail")
                .code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
        assert!(observed[2].1.is_ok());
    }

    #[test]
    fn only_typed_pre_dispatch_capability_refusal_can_fallback() {
        let unsupported =
            afs_error::Error::coded(afs_error::NODE_TRANSFER_UNSUPPORTED, "unsupported");
        assert!(may_fallback(
            HixlDfsReadMode::Preferred,
            false,
            &unsupported
        ));
        assert!(!may_fallback(
            HixlDfsReadMode::Preferred,
            true,
            &unsupported
        ));
        assert!(!may_fallback(
            HixlDfsReadMode::Required,
            false,
            &unsupported
        ));
        let bare = afs_transport::grpc::error_status::status_to_error(
            tonic::Status::unimplemented("arbitrary"),
        );
        assert!(!may_fallback(HixlDfsReadMode::Preferred, false, &bare));
        assert!(!may_fallback(
            HixlDfsReadMode::Preferred,
            false,
            &afs_error::Error::coded(afs_error::IO_TIMEOUT, "timeout")
        ));
    }

    #[test]
    fn engine_address_cannot_be_reconnected_across_source_generations() {
        let (_, mut reply, _) = descriptor();
        let mut engines = HashMap::new();
        bind_engine(&mut engines, &reply).unwrap();
        bind_engine(&mut engines, &reply).unwrap();
        reply.engine_generation += 1;
        assert_eq!(
            bind_engine(&mut engines, &reply).unwrap_err().code(),
            afs_error::NODE_TRANSFER_DRAIN_INCOMPLETE
        );
        assert_eq!(engines[&reply.engine_id].2, 9);
    }

    #[tokio::test]
    async fn outer_deadline_detaches_owned_cleanup_and_fences_the_peer() {
        let cancel = Arc::new(Cancellation::default());
        let peer = Arc::new(AtomicBool::new(false));
        cancel.publish(peer.clone()).unwrap();
        let (finish, finishing) = tokio::sync::oneshot::channel();
        let (returned, returning) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let owned_buffer = vec![42_u8; 4];
            finishing.await.unwrap();
            returned.send(owned_buffer).unwrap();
            Ok(())
        });
        let error = wait_owned_task(
            task,
            Instant::now() + Duration::from_millis(10),
            cancel.clone(),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), afs_error::CLIENT_DEADLINE_EXCEEDED);
        assert!(peer.load(Ordering::Acquire));
        assert!(cancel.cancelled.load(Ordering::Acquire));
        finish.send(()).unwrap();
        assert_eq!(returning.await.unwrap(), vec![42; 4]);
    }

    mod loopback_integration {
        use super::*;
        use crate::node::chunk::{ChunkStore, LocalChunkStore, StagedChunk};
        use crate::node::dfs_read::{ChunkReadOp, ResolvedReadOp};
        use crate::node::rpc::data::{
            DenyDfsReplicaAuthorizer, DfsChunkTransportResources, DfsReadAuthorizer,
            PeerAuthenticator, hixl as source_hixl, make_dfs_chunks_server_with_transport,
        };
        use afs_transport::hixl::{ExportWindow, HixlError, HixlErrorKind, HixlPhase};
        use tokio_stream::wrappers::TcpListenerStream;

        struct MemoryWindow {
            lane: usize,
            generation: u64,
            bytes: Vec<u8>,
        }

        #[derive(Default)]
        struct MemoryExports {
            next_address: u64,
            generations: [u64; 2],
            active: HashMap<u64, MemoryWindow>,
            reads: Vec<(usize, u64, usize)>,
        }

        #[derive(Default)]
        struct ConnectGate {
            closed: bool,
            entered: usize,
            completed: usize,
        }

        #[derive(Default)]
        struct NativeBoundary {
            memory: StdMutex<MemoryExports>,
            prepare_calls: AtomicU64,
            batch_reads: AtomicU64,
            reader_quarantines: AtomicU64,
            source_quarantines: AtomicU64,
            connects: StdMutex<ConnectGate>,
            connects_changed: std::sync::Condvar,
        }

        impl NativeBoundary {
            fn close_connect_gate(&self) {
                self.connects.lock().unwrap().closed = true;
            }
            fn open_connect_gate(&self) {
                self.connects.lock().unwrap().closed = false;
                self.connects_changed.notify_all();
            }
            async fn wait_connect_entered(&self, count: usize) {
                for _ in 0..300 {
                    if self.connects.lock().unwrap().entered >= count {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                panic!("warm task did not enter connect");
            }
        }

        fn boundary_error(phase: HixlPhase, kind: HixlErrorKind, message: &str) -> HixlError {
            HixlError {
                phase,
                kind,
                native_status: None,
                message: message.into(),
                read_may_be_in_flight: false,
            }
        }

        fn boundary_infos(
            first_port: u16,
            generation: u64,
            lanes: u16,
            window_bytes: usize,
        ) -> Vec<EngineInfo> {
            (0..lanes)
                .map(|offset| EngineInfo {
                    engine_id: format!("127.0.0.1:{}", first_port + offset),
                    generation,
                    window_bytes,
                    transport: ReadTransport::Roce,
                    buffer_pool_count: 0,
                    buffer_pool_size_mib: 0,
                    hccs_domain_id: None,
                })
                .collect()
        }

        struct SourceNative {
            boundary: Arc<NativeBoundary>,
            lanes: u16,
            window_bytes: usize,
            export_slots: usize,
        }

        impl source_hixl::ExportProvider for SourceNative {
            fn info(&self) -> EngineInfo {
                self.lane_infos().remove(0)
            }

            fn lane_infos(&self) -> Vec<EngineInfo> {
                boundary_infos(7920, 7, self.lanes, self.window_bytes)
            }

            fn export_slots(&self, _lane: usize) -> usize {
                self.export_slots
            }

            fn prepare_export(
                &self,
                lane: usize,
                bytes: Vec<u8>,
                _deadline: Instant,
            ) -> Result<ExportWindow, HixlError> {
                if self.boundary.prepare_calls.fetch_add(1, Ordering::SeqCst) == 3 {
                    return Err(boundary_error(
                        HixlPhase::PrepareExport,
                        HixlErrorKind::ResourceExhausted,
                        "one definite refusal in the second wave",
                    ));
                }
                let mut memory = self.boundary.memory.lock().unwrap();
                let active = memory
                    .active
                    .values()
                    .filter(|window| window.lane == lane)
                    .count();
                if active >= self.export_slots {
                    return Err(boundary_error(
                        HixlPhase::PrepareExport,
                        HixlErrorKind::ResourceExhausted,
                        "the lane export slots are still active",
                    ));
                }
                memory.generations[lane] += 1;
                let generation = memory.generations[lane];
                memory.next_address += 0x100;
                let address = 0x1000 + memory.next_address;
                let length = bytes.len();
                memory.active.insert(
                    address,
                    MemoryWindow {
                        lane,
                        generation,
                        bytes,
                    },
                );
                Ok(ExportWindow {
                    slot_id: 0,
                    generation,
                    address,
                    length,
                })
            }

            fn release_export(
                &self,
                lane: usize,
                slot_id: u64,
                generation: u64,
            ) -> Result<(), HixlError> {
                let mut memory = self.boundary.memory.lock().unwrap();
                let address = memory
                    .active
                    .iter()
                    .find_map(|(address, window)| {
                        (slot_id == 0 && window.lane == lane && window.generation == generation)
                            .then_some(*address)
                    })
                    .ok_or_else(|| {
                        boundary_error(
                            HixlPhase::ReleaseExport,
                            HixlErrorKind::InvalidState,
                            "release did not identify the active lane generation",
                        )
                    })?;
                memory.active.remove(&address);
                Ok(())
            }

            fn quarantine(&self) -> Result<(), HixlError> {
                self.boundary
                    .source_quarantines
                    .fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        struct ReaderNative {
            boundary: Arc<NativeBoundary>,
            lanes: u16,
            window_bytes: usize,
        }

        impl ReadRuntime for ReaderNative {
            fn info(&self) -> EngineInfo {
                self.infos().remove(0)
            }

            fn infos(&self) -> Vec<EngineInfo> {
                boundary_infos(7930, 11, self.lanes, self.window_bytes)
            }

            fn lane_count(&self) -> usize {
                usize::from(self.lanes)
            }

            fn lane(&self, lane: usize) -> Option<Arc<dyn ReadLane>> {
                let infos = self.infos();
                infos.get(lane).map(|_| {
                    Arc::new(ReaderLane {
                        boundary: self.boundary.clone(),
                        source_engine: format!("127.0.0.1:{}", 7920 + lane as u16),
                        lane,
                    }) as Arc<dyn ReadLane>
                })
            }

            fn quarantine(&self) -> Result<(), HixlError> {
                self.boundary
                    .reader_quarantines
                    .fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        struct ReaderLane {
            boundary: Arc<NativeBoundary>,
            source_engine: String,
            lane: usize,
        }

        impl ReadLane for ReaderLane {
            fn connect(&self, peer: &str, _deadline: Instant) -> Result<(), HixlError> {
                if peer != self.source_engine {
                    return Err(boundary_error(
                        HixlPhase::Connect,
                        HixlErrorKind::InvalidArgument,
                        "wrong lane engine",
                    ));
                }
                let mut gate = self.boundary.connects.lock().unwrap();
                gate.entered += 1;
                while gate.closed {
                    gate = self.boundary.connects_changed.wait(gate).unwrap();
                }
                gate.completed += 1;
                Ok(())
            }

            fn read(
                &self,
                peer: &str,
                address: u64,
                length: usize,
                deadline: Instant,
            ) -> Result<Vec<u8>, HixlError> {
                self.connect(peer, deadline)?;
                let mut memory = self.boundary.memory.lock().unwrap();
                let window = memory.active.get(&address).ok_or_else(|| {
                    boundary_error(
                        HixlPhase::SubmitRead,
                        HixlErrorKind::InvalidState,
                        "READ has no live source export",
                    )
                })?;
                if window.lane != self.lane || window.bytes.len() != length {
                    return Err(boundary_error(
                        HixlPhase::SubmitRead,
                        HixlErrorKind::InvalidArgument,
                        "READ differs from source lane or length",
                    ));
                }
                let bytes = window.bytes.clone();
                memory.reads.push((self.lane, address, length));
                Ok(bytes)
            }

            fn prepare_read_buffer(&self, length: usize) -> Result<Vec<u8>, HixlError> {
                if length == 0 {
                    return Err(boundary_error(
                        HixlPhase::SubmitRead,
                        HixlErrorKind::InvalidArgument,
                        "empty read buffer",
                    ));
                }
                let mut output = Vec::new();
                output
                    .try_reserve_exact(length)
                    .map_err(|error| HixlError {
                        phase: HixlPhase::SubmitRead,
                        native_status: None,
                        kind: HixlErrorKind::ResourceExhausted,
                        message: format!("test read buffer allocation failed: {error}"),
                        read_may_be_in_flight: false,
                    })?;
                output.resize(length, 0);
                Ok(output)
            }

            fn read_timed_into(
                &self,
                peer: &str,
                address: u64,
                mut output: Vec<u8>,
                deadline: Instant,
            ) -> Result<HixlReadOutcome, HixlError> {
                let bytes = self.read(peer, address, output.len(), deadline)?;
                output.copy_from_slice(&bytes);
                Ok(HixlReadOutcome {
                    bytes: output,
                    timing: afs_transport::hixl::HixlReadTiming {
                        owner_queue: Duration::from_micros(1),
                        sdk_transfer: Some(Duration::from_micros(2)),
                        output_copy: Some(Duration::from_micros(1)),
                    },
                })
            }

            fn read_batch_timed_into(
                &self,
                peer: &str,
                windows: &[(u64, usize)],
                mut output: Vec<u8>,
                deadline: Instant,
            ) -> Result<HixlReadOutcome, HixlError> {
                self.boundary.batch_reads.fetch_add(1, Ordering::SeqCst);
                let mut offset = 0;
                for (address, length) in windows {
                    let bytes = self.read(peer, *address, *length, deadline)?;
                    let end = offset + length;
                    output[offset..end].copy_from_slice(&bytes);
                    offset = end;
                }
                if offset != output.len() {
                    return Err(boundary_error(
                        HixlPhase::SubmitRead,
                        HixlErrorKind::InvalidArgument,
                        "batch READ output differs from its windows",
                    ));
                }
                Ok(HixlReadOutcome {
                    bytes: output,
                    timing: afs_transport::hixl::HixlReadTiming {
                        owner_queue: Duration::from_micros(1),
                        sdk_transfer: Some(Duration::from_micros(2)),
                        output_copy: Some(Duration::from_micros(1)),
                    },
                })
            }
        }

        struct TestAuthority;

        impl PeerAuthenticator for TestAuthority {
            fn authenticate(
                &self,
                _: &tonic::metadata::MetadataMap,
                _: Option<SocketAddr>,
                _: Option<&[u8]>,
            ) -> afs_error::Result<String> {
                Ok("reader".into())
            }
        }

        impl DfsReadAuthorizer for TestAuthority {
            fn authorize(
                &self,
                peer: &str,
                request: &DfsReadRangesRequest,
            ) -> afs_error::Result<()> {
                if peer != "reader"
                    || request.operations.iter().any(|operation| {
                        operation
                            .grant
                            .as_ref()
                            .is_none_or(|grant| grant.token != "offline-grant")
                    })
                {
                    return Err(afs_error::Error::coded(
                        afs_error::NODE_TRANSFER_INVALID,
                        "offline grant rejected",
                    ));
                }
                Ok(())
            }
        }

        fn read_batch(chunk_id: crate::dfs::ChunkId, endpoint: &str) -> PeerReadBatch {
            let version = crate::dfs::FileVersionId::new("version");
            let layout = crate::dfs::LayoutRootId::new("layout");
            PeerReadBatch {
                file_version_id: version.clone(),
                layout_root_id: layout.clone(),
                operations: vec![ResolvedReadOp {
                    op: ChunkReadOp {
                        chunk_id: chunk_id.clone(),
                        chunk_offset: 0,
                        length: 8,
                        output_offset: 1,
                    },
                    source: crate::dfs::SourceCandidate {
                        copy_id: crate::dfs::CopyId::new("copy"),
                        chunk_id,
                        role: crate::dfs::CopyRole::DurableReplica,
                        state: crate::dfs::CopyState::Ready,
                        location: crate::dfs::CopyLocation::Node {
                            node_id: "source".into(),
                            node_epoch: 9,
                            device_id: "disk".into(),
                            device_epoch: 1,
                            catalog_revision: 1,
                        },
                        data_endpoint: Some(endpoint.into()),
                        load_hint: 0,
                        read_grant: crate::dfs::DfsReadGrant {
                            namespace_id: crate::dfs::NamespaceId::new("namespace"),
                            file_version_id: version,
                            layout_root_id: layout,
                            caller_node_id: "reader".into(),
                            caller_node_epoch: 1,
                            expires_at_unix_ms: u64::MAX,
                            fence: 1,
                            token: "offline-grant".into(),
                        },
                    },
                }],
            }
        }

        struct Harness {
            _temp: tempfile::TempDir,
            native: Arc<NativeBoundary>,
            source: Arc<source_hixl::HixlReadService>,
            reader: Arc<HixlChunkTransfer>,
            batch: PeerReadBatch,
            endpoint: String,
            stop: tokio::sync::oneshot::Sender<()>,
            server: tokio::task::JoinHandle<()>,
        }

        async fn harness() -> Harness {
            harness_with(2, 2, 1).await
        }

        async fn preferred_harness() -> Harness {
            harness_with_mode(2, 2, 1, HixlDfsReadMode::Preferred).await
        }

        async fn group_harness() -> Harness {
            harness_with(1, 4, 8).await
        }

        async fn harness_with(lanes: u16, window_bytes: usize, export_slots: usize) -> Harness {
            harness_with_mode(lanes, window_bytes, export_slots, HixlDfsReadMode::Required).await
        }

        async fn harness_with_mode(
            lanes: u16,
            window_bytes: usize,
            export_slots: usize,
            mode: HixlDfsReadMode,
        ) -> Harness {
            harness_with_capabilities(lanes, window_bytes, export_slots, mode, true, None).await
        }

        async fn harness_with_capabilities(
            lanes: u16,
            window_bytes: usize,
            export_slots: usize,
            mode: HixlDfsReadMode,
            source_supported: bool,
            group: Option<ReadGroupPolicy>,
        ) -> Harness {
            let temp = tempfile::tempdir().unwrap();
            let local = Arc::new(LocalChunkStore::open(temp.path(), "source").unwrap());
            let staged = StagedChunk::new(
                crate::dfs::OperationId::new("offline-read"),
                b"abcdefgh".to_vec(),
            );
            let chunk_id = staged.chunk.id.clone();
            local.put(staged).unwrap();
            let native = Arc::new(NativeBoundary::default());
            let source = Arc::new(source_hixl::HixlReadService::for_test(
                Arc::new(SourceNative {
                    boundary: native.clone(),
                    lanes,
                    window_bytes,
                    export_slots,
                }),
                "source".into(),
                9,
            ));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let service = make_dfs_chunks_server_with_transport(
                Some(local),
                Arc::new(TestAuthority),
                Arc::new(TestAuthority),
                Arc::new(DenyDfsReplicaAuthorizer),
                None,
                Duration::from_secs(5),
                DfsChunkTransportResources {
                    rdma_sessions: crate::node::rpc::control::RdmaSessionRegistry::new(None),
                    payload_metrics: None,
                    hixl: source_supported.then(|| source.clone()),
                },
            );
            let (stop, stopping) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(service)
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                        let _ = stopping.await;
                    })
                    .await
                    .unwrap();
            });
            let peers = Arc::new(
                PeerConnectionPool::new(
                    afs_transport::GrpcConfig::default(),
                    afs_transport::TlsConfig::Disabled,
                    16,
                )
                .unwrap(),
            );
            let mut reader = HixlChunkTransfer::from_runtime(
                peers,
                Arc::new(ReaderNative {
                    boundary: native.clone(),
                    lanes,
                    window_bytes,
                }),
                mode,
                Duration::from_secs(5),
            )
            .unwrap();
            if let Some(group) = group {
                reader = reader.with_read_group(group, export_slots);
            }
            let reader = Arc::new(reader);
            let batch = read_batch(chunk_id, &endpoint);
            Harness {
                _temp: temp,
                native,
                source,
                reader,
                batch,
                endpoint,
                stop,
                server,
            }
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn small_batches_keep_one_window_on_the_bound_lane() {
            let Harness {
                _temp,
                native,
                source,
                reader,
                mut batch,
                stop,
                server,
                ..
            } = harness().await;
            batch.operations[0].op.length = 2;
            for expected_lanes in [vec![0], vec![0, 0], vec![0, 0, 0]] {
                let reader = reader.clone();
                let batch = batch.clone();
                let output = tokio::task::spawn_blocking(move || {
                    let mut output = vec![b'!'; 4];
                    reader.read_ranges(&batch, &mut output).unwrap();
                    output
                })
                .await
                .unwrap();
                assert_eq!(output, b"!ab!");
                let memory = native.memory.lock().unwrap();
                assert_eq!(
                    memory.reads.iter().map(|read| read.0).collect::<Vec<_>>(),
                    expected_lanes,
                    "one small batch uses one window on the peer's bound lane"
                );
                assert!(memory.reads.iter().all(|read| read.2 == 2));
                assert!(memory.active.is_empty());
                drop(memory);
                let sessions = source_hixl::test_support::sessions(&source);
                assert_eq!(sessions.len(), 1, "one peer keeps one control session");
                assert_eq!(sessions[0].prepared_bytes, 0);
                assert_eq!(sessions[0].active_windows, 0);
            }
            stop.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(1), server)
                .await
                .unwrap()
                .unwrap();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn peers_spread_over_lanes_before_sharing_one() {
            let Harness {
                _temp,
                reader,
                stop,
                server,
                ..
            } = harness().await;
            let first = ("peer-a".to_owned(), 9, "grpc-a".to_owned());
            let second = ("peer-b".to_owned(), 9, "grpc-b".to_owned());
            let third = ("peer-c".to_owned(), 9, "grpc-c".to_owned());
            assert_eq!(reader.context.bind_lane(&first), 0);
            assert_eq!(reader.context.bind_lane(&second), 1);
            assert_eq!(
                reader.context.bind_lane(&first),
                0,
                "a bound peer keeps its lane"
            );
            assert_eq!(
                reader.context.bind_lane(&third),
                0,
                "a peer without a free lane shares the least-used one"
            );
            stop.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(1), server)
                .await
                .unwrap()
                .unwrap();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn unsupported_fallback_preserves_the_group_budget() {
            let Harness {
                _temp,
                native,
                reader,
                mut batch,
                stop,
                server,
                ..
            } = harness_with_capabilities(
                2,
                2,
                1,
                HixlDfsReadMode::Preferred,
                false,
                Some(ReadGroupPolicy {
                    bytes: 1024,
                    max_inflight: 1,
                    max_descriptors: crate::node::dfs_read::MAX_DFS_GROUP_OPS,
                    host_budget_bytes: 2048,
                }),
            )
            .await;
            let template = batch.operations[0].clone();
            batch.operations = (0..=crate::node::dfs_read::MAX_DFS_READ_OPS)
                .map(|index| {
                    let mut operation = template.clone();
                    operation.op.length = 1;
                    operation.op.output_offset = index;
                    operation
                })
                .collect();
            let expected = vec![b'a'; batch.operations.len()];

            let output = tokio::task::spawn_blocking(move || {
                let mut output = vec![b'!'; batch.operations.len()];
                reader
                    .grpc_fallback(reader.context.timeout)?
                    .read_ranges(&batch, &mut output)?;
                Ok::<_, afs_error::Error>(output)
            })
            .await
            .unwrap()
            .unwrap();
            assert_eq!(output, expected);
            assert!(native.memory.lock().unwrap().reads.is_empty());
            stop.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(1), server)
                .await
                .unwrap()
                .unwrap();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn preferred_cold_batch_uses_grpc_while_the_session_warms() {
            let Harness {
                _temp,
                native,
                source,
                reader,
                mut batch,
                stop,
                server,
                ..
            } = preferred_harness().await;
            batch.operations[0].op.length = 2;
            native.close_connect_gate();
            let first = {
                let reader = reader.clone();
                let batch = batch.clone();
                tokio::time::timeout(
                    Duration::from_secs(2),
                    tokio::task::spawn_blocking(move || {
                        let mut output = vec![b'!'; 4];
                        reader.read_ranges(&batch, &mut output)?;
                        Ok::<_, afs_error::Error>(output)
                    }),
                )
                .await
                .expect("cold gRPC fallback must not wait for the HIXL connect")
                .unwrap()
                .unwrap()
            };
            assert_eq!(first, b"!ab!");
            native.wait_connect_entered(1).await;
            assert_eq!(
                native.connects.lock().unwrap().completed,
                0,
                "the first batch must finish while the native connect is still blocked"
            );
            assert!(
                native.memory.lock().unwrap().reads.is_empty(),
                "the cold batch must not use a native HIXL read"
            );

            native.open_connect_gate();
            wait_warm_session(&reader, &batch).await;
            let second = {
                let reader = reader.clone();
                let batch = batch.clone();
                tokio::task::spawn_blocking(move || {
                    let mut output = vec![b'!'; 4];
                    reader.read_ranges(&batch, &mut output)?;
                    Ok::<_, afs_error::Error>(output)
                })
                .await
                .unwrap()
                .unwrap()
            };
            assert_eq!(second, b"!ab!");
            assert_eq!(
                native.memory.lock().unwrap().reads.len(),
                1,
                "the warmed session must serve the next batch natively"
            );
            assert_eq!(
                source_hixl::test_support::sessions(&source).len(),
                1,
                "warming must negotiate exactly one control session"
            );
            stop.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(1), server)
                .await
                .unwrap()
                .unwrap();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn settling_a_warm_slot_returns_when_the_task_clears_the_flag() {
            let Harness {
                reader,
                batch,
                stop,
                server,
                ..
            } = harness().await;
            let context = reader.context.clone();
            let crate::dfs::CopyLocation::Node {
                node_id,
                node_epoch,
                ..
            } = &batch.operations[0].source.location
            else {
                panic!("loopback source must be a node copy");
            };
            let address = batch.operations[0]
                .source
                .node_data_endpoint()
                .unwrap()
                .to_owned();
            let key = (node_id.clone(), *node_epoch, address.clone());
            let slot = Arc::new(Mutex::new(PeerSlot {
                session: None,
                touched: Instant::now(),
                warming: None,
            }));
            context.slots.lock().await.insert(key.clone(), slot.clone());
            let (sender, _) = watch::channel(false);
            slot.lock().await.warming = Some(sender);
            let release = tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                slot.lock().await.warming = None;
            });
            let deadline = Instant::now() + Duration::from_secs(2);
            tokio::time::timeout(
                Duration::from_secs(1),
                context.settle_warm(node_id, *node_epoch, &address, deadline),
            )
            .await
            .expect("settle must observe the cleared warm flag")
            .unwrap();
            release.await.unwrap();
            stop.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(1), server)
                .await
                .unwrap()
                .unwrap();
        }

        async fn wait_warm_session(reader: &HixlChunkTransfer, batch: &PeerReadBatch) {
            let crate::dfs::CopyLocation::Node {
                node_id,
                node_epoch,
                ..
            } = &batch.operations[0].source.location
            else {
                panic!("loopback source must be a node copy");
            };
            let address = batch.operations[0]
                .source
                .node_data_endpoint()
                .unwrap()
                .to_owned();
            let key = (node_id.clone(), *node_epoch, address);
            for _ in 0..300 {
                let slot = {
                    let slots = reader.context.slots.lock().await;
                    slots.get(&key).cloned()
                };
                if let Some(slot) = slot {
                    let guard = slot.lock().await;
                    if guard.session.is_some() && guard.warming.is_none() {
                        return;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("warm session was not published");
        }

        fn expect_window<T>(result: Result<T, WindowFailure>, context: &str) -> T {
            match result {
                Ok(value) => value,
                Err(failure) => panic!("{context}: {}", failure.error),
            }
        }

        fn read_authority(batch: &PeerReadBatch) -> DfsReadRangesRequest {
            DfsReadRangesRequest {
                read_id: "group-read".into(),
                attempt_id: "group-attempt".into(),
                file_version_id: batch.file_version_id.0.clone(),
                layout_root_id: batch.layout_root_id.0.clone(),
                include_blake3_outboard: false,
                operations: batch
                    .operations
                    .iter()
                    .enumerate()
                    .map(|(index, item)| DfsChunkReadOp {
                        operation_index: index as u32,
                        chunk_id: item.op.chunk_id.0.clone(),
                        chunk_offset: item.op.chunk_offset,
                        length: item.op.length,
                        destination_offset: item.op.output_offset as u64,
                        source_copy_id: item.source.copy_id.0.clone(),
                        grant: Some(wire_dfs_read_grant(&item.source.read_grant)),
                    })
                    .collect(),
            }
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn prepared_group_submits_one_native_batch_and_releases_every_window() {
            let Harness {
                _temp,
                native,
                source,
                reader,
                batch,
                stop,
                server,
                ..
            } = group_harness().await;
            let authority = read_authority(&batch);
            let context = reader.context.clone();
            let deadline = Instant::now() + Duration::from_secs(5);
            let crate::dfs::CopyLocation::Node {
                node_id,
                node_epoch,
                ..
            } = &batch.operations[0].source.location
            else {
                panic!("loopback source must be a node copy");
            };
            let address = batch.operations[0]
                .source
                .node_data_endpoint()
                .unwrap()
                .to_owned();
            let lease = match context
                .acquire(node_id, *node_epoch, &address, &authority, deadline)
                .await
                .unwrap()
            {
                Acquired::Lease(lease) => lease,
                Acquired::Unsupported => panic!("loopback HIXL must be supported"),
            };
            let session = lease.session.clone();
            let packed = pack_read_batch(authority).unwrap();
            let windows = pack_windows(packed.clone(), 4).unwrap();
            assert_eq!(windows.len(), 2);
            let mut prepared = Vec::new();
            let mut offset = 0_u64;
            for (ordinal, window) in windows.into_iter().enumerate() {
                let reply = expect_window(
                    context
                        .prepare_window(&session, &window, &packed, offset, 0, deadline)
                        .await,
                    "group window preparation",
                );
                offset += u64::try_from(window.length).unwrap();
                prepared.push((ordinal, window, reply, 0_usize));
            }
            let mut output = vec![0_u8; 8];
            let mut payload = Duration::ZERO;
            let chunks = expect_window(
                context
                    .read_prepared_group(&session, prepared, output.len(), deadline, &mut payload)
                    .await,
                "group native read",
            );
            scatter_prepared(chunks, &mut output).unwrap();

            assert_eq!(output, b"abcdefgh");
            assert_eq!(native.batch_reads.load(Ordering::SeqCst), 1);
            assert!(payload > Duration::ZERO);
            let sessions = source_hixl::test_support::sessions(&source);
            assert_eq!(sessions.len(), 1);
            assert_eq!(sessions[0].prepared_bytes, 0);
            assert_eq!(sessions[0].active_windows, 0);
            assert!(native.memory.lock().unwrap().active.is_empty());
            drop(lease);
            stop.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(1), server)
                .await
                .unwrap()
                .unwrap();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn group_prepare_admits_all_windows_from_one_snapshot() {
            let Harness {
                _temp,
                native,
                source,
                reader,
                batch,
                stop,
                server,
                ..
            } = group_harness().await;
            let authority = read_authority(&batch);
            let context = reader.context.clone();
            let deadline = Instant::now() + Duration::from_secs(5);
            let crate::dfs::CopyLocation::Node {
                node_id,
                node_epoch,
                ..
            } = &batch.operations[0].source.location
            else {
                panic!("loopback source must be a node copy");
            };
            let address = batch.operations[0]
                .source
                .node_data_endpoint()
                .unwrap()
                .to_owned();
            let lease = match context
                .acquire(node_id, *node_epoch, &address, &authority, deadline)
                .await
                .unwrap()
            {
                Acquired::Lease(lease) => lease,
                Acquired::Unsupported => panic!("loopback HIXL must be supported"),
            };
            let session = lease.session.clone();
            let packed = pack_read_batch(authority).unwrap();
            let mut offset = 0_u64;
            let planned = pack_windows(packed.clone(), 4)
                .unwrap()
                .into_iter()
                .enumerate()
                .map(|(ordinal, window)| {
                    let start = offset;
                    offset += u64::try_from(window.length).unwrap();
                    (ordinal, window, start, 0_usize)
                })
                .collect::<Vec<_>>();
            let prepared = expect_window(
                context
                    .prepare_group_windows(&session, &planned, &packed, deadline)
                    .await,
                "one group prepare admits every window",
            );
            assert_eq!(prepared.len(), 2);
            let sessions = source_hixl::test_support::sessions(&source);
            assert_eq!(sessions.len(), 1);
            assert_eq!(sessions[0].active_windows, 2);
            assert_eq!(
                sessions[0].prepared_bytes, 8,
                "both windows share one verified parent snapshot"
            );
            let mut output = vec![0_u8; 8];
            let mut payload = Duration::ZERO;
            let chunks = expect_window(
                context
                    .read_prepared_group(&session, prepared, output.len(), deadline, &mut payload)
                    .await,
                "group native read",
            );
            scatter_prepared(chunks, &mut output).unwrap();
            assert_eq!(output, b"abcdefgh");
            assert_eq!(native.batch_reads.load(Ordering::SeqCst), 1);
            let sessions = source_hixl::test_support::sessions(&source);
            assert_eq!(sessions[0].active_windows, 0);
            assert_eq!(sessions[0].prepared_bytes, 0);
            assert!(native.memory.lock().unwrap().active.is_empty());
            drop(lease);
            stop.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(1), server)
                .await
                .unwrap()
                .unwrap();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn prepared_payload_metrics_cover_native_wave_and_caller_scatter() {
            let Harness {
                _temp,
                native,
                source,
                reader,
                mut batch,
                stop,
                server,
                ..
            } = harness().await;
            batch.operations[0].op.length = 4;
            let registry = afs_metrics::Registry::new();
            let payload_metrics = DfsReadPayloadMetrics::register(&registry).unwrap();
            let reader = Arc::new(
                HixlChunkTransfer::from_runtime(
                    reader.context.peers.clone(),
                    Arc::new(ReaderNative {
                        boundary: native.clone(),
                        lanes: 2,
                        window_bytes: 2,
                    }),
                    HixlDfsReadMode::Required,
                    Duration::from_secs(5),
                )
                .unwrap()
                .with_payload_metrics(payload_metrics),
            );
            let output = tokio::task::spawn_blocking(move || {
                let mut output = vec![b'!'; 10];
                reader.read_ranges(&batch, &mut output).unwrap();
                output
            })
            .await
            .unwrap();

            assert_eq!(output, b"!abcd!!!!!");
            let families: HashMap<_, _> = registry
                .gather()
                .into_iter()
                .map(|family| (family.name().to_owned(), family))
                .collect();
            let payload = &families["afs_dfs_read_payload_seconds"];
            let successful_payload = payload
                .get_metric()
                .iter()
                .find(|metric| {
                    metric
                        .get_label()
                        .iter()
                        .any(|label| label.name() == "transport" && label.value() == "hixl")
                        && metric
                            .get_label()
                            .iter()
                            .any(|label| label.name() == "result" && label.value() == "ok")
                })
                .unwrap();
            assert_eq!(successful_payload.get_histogram().get_sample_count(), 1);
            assert_eq!(
                families["afs_dfs_read_payload_bytes_total"]
                    .get_metric()
                    .iter()
                    .find(|metric| {
                        metric
                            .get_label()
                            .iter()
                            .any(|label| label.name() == "transport" && label.value() == "hixl")
                    })
                    .unwrap()
                    .get_counter()
                    .get_value(),
                4.0
            );
            let sessions = source_hixl::test_support::sessions(&source);
            assert_eq!(sessions.len(), 1);
            assert_eq!(sessions[0].prepared_bytes, 0);
            assert_eq!(sessions[0].active_windows, 0);
            assert!(native.memory.lock().unwrap().active.is_empty());
            stop.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(1), server)
                .await
                .unwrap()
                .unwrap();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn prepared_payload_failure_has_no_success_bytes_or_publication() {
            let Harness {
                _temp,
                native,
                source,
                reader,
                batch,
                stop,
                server,
                ..
            } = harness().await;
            let registry = afs_metrics::Registry::new();
            let payload_metrics = DfsReadPayloadMetrics::register(&registry).unwrap();
            let reader = Arc::new(
                HixlChunkTransfer::from_runtime(
                    reader.context.peers.clone(),
                    Arc::new(ReaderNative {
                        boundary: native.clone(),
                        lanes: 2,
                        window_bytes: 2,
                    }),
                    HixlDfsReadMode::Required,
                    Duration::from_secs(5),
                )
                .unwrap()
                .with_payload_metrics(payload_metrics),
            );
            let (result, output) = tokio::task::spawn_blocking(move || {
                let mut output = vec![b'!'; 10];
                let result = reader.read_ranges(&batch, &mut output);
                (result, output)
            })
            .await
            .unwrap();

            assert_eq!(
                result.unwrap_err().code(),
                afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED
            );
            assert_eq!(output, b"!!!!!!!!!!");
            let families: HashMap<_, _> = registry
                .gather()
                .into_iter()
                .map(|family| (family.name().to_owned(), family))
                .collect();
            let samples = &families["afs_dfs_read_payload_seconds"];
            let count_for =
                |transport: &str, result: &str| {
                    samples
                        .get_metric()
                        .iter()
                        .find(|metric| {
                            metric.get_label().iter().any(|label| {
                                label.name() == "transport" && label.value() == transport
                            }) && metric
                                .get_label()
                                .iter()
                                .any(|label| label.name() == "result" && label.value() == result)
                        })
                        .unwrap()
                        .get_histogram()
                        .get_sample_count()
                };
            assert_eq!(count_for("hixl", "ok"), 0);
            assert_eq!(count_for("hixl", "error"), 1);
            let hixl_bytes = families["afs_dfs_read_payload_bytes_total"]
                .get_metric()
                .iter()
                .find(|metric| {
                    metric
                        .get_label()
                        .iter()
                        .any(|label| label.name() == "transport" && label.value() == "hixl")
                })
                .unwrap()
                .get_counter()
                .get_value();
            assert_eq!(hixl_bytes, 0.0);
            let sessions = source_hixl::test_support::sessions(&source);
            assert_eq!(sessions.len(), 1);
            assert!(sessions[0].closed);
            assert_eq!(sessions[0].prepared_bytes, 0);
            assert_eq!(sessions[0].active_windows, 0);
            assert!(native.memory.lock().unwrap().active.is_empty());
            stop.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(1), server)
                .await
                .unwrap()
                .unwrap();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn later_wave_prepare_refusal_closes_snapshot_and_allows_fresh_session() {
            let Harness {
                _temp,
                native,
                source,
                reader,
                batch,
                endpoint,
                stop,
                server,
            } = harness_with(2, 2, 2).await;
            let failed_reader = reader.clone();
            let failed_batch = batch.clone();
            let (failed, output) = tokio::task::spawn_blocking(move || {
                let mut output = vec![b'!'; 10];
                let result = failed_reader.read_ranges(&failed_batch, &mut output);
                (result, output)
            })
            .await
            .unwrap();
            assert_eq!(
                failed.unwrap_err().code(),
                afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED
            );
            assert_eq!(output, b"!!!!!!!!!!");
            let sessions = source_hixl::test_support::sessions(&source);
            assert_eq!(sessions.len(), 1);
            let retired = &sessions[0];
            assert!(
                retired.closed,
                "the partial batch control session must close"
            );
            assert_eq!(
                retired.prepared_bytes, 0,
                "close must discard the old snapshot"
            );
            assert_eq!(retired.active_windows, 0);
            assert_eq!(
                retired
                    .outcomes
                    .iter()
                    .filter(|&&outcome| outcome == DfsHixlReleaseOutcome::Completed as i32)
                    .count(),
                2
            );
            assert_eq!(
                retired
                    .outcomes
                    .iter()
                    .filter(|&&outcome| outcome == DfsHixlReleaseOutcome::NotSubmitted as i32)
                    .count(),
                1
            );
            let key = ("source".into(), 9, endpoint);
            let slot = reader.context.slots.lock().await.get(&key).unwrap().clone();
            assert!(slot.lock().await.session.is_none());
            {
                let memory = native.memory.lock().unwrap();
                assert!(memory.active.is_empty());
                assert_eq!(
                    memory.reads.len(),
                    2,
                    "the refused wave must never enter READ"
                );
            }
            assert_eq!(native.reader_quarantines.load(Ordering::SeqCst), 0);
            assert_eq!(native.source_quarantines.load(Ordering::SeqCst), 0);

            let recovered_reader = reader.clone();
            let (recovered, output) = tokio::task::spawn_blocking(move || {
                let mut output = vec![b'!'; 10];
                let result = recovered_reader.read_ranges(&batch, &mut output);
                (result, output)
            })
            .await
            .unwrap();
            recovered.unwrap();
            assert_eq!(output, b"!abcdefgh!");
            let sessions = source_hixl::test_support::sessions(&source);
            assert_eq!(sessions.len(), 2);
            let fresh = sessions
                .iter()
                .find(|session| session.session_id != retired.session_id)
                .unwrap();
            assert!(!fresh.closed);
            assert_eq!(fresh.prepared_bytes, 0);
            assert_eq!(fresh.active_windows, 0);
            assert_eq!(
                fresh.outcomes,
                vec![DfsHixlReleaseOutcome::Completed as i32; 4]
            );
            assert_eq!(
                slot.lock().await.session.as_ref().unwrap().reply.session_id,
                fresh.session_id
            );
            assert!(native.memory.lock().unwrap().active.is_empty());
            assert_eq!(native.memory.lock().unwrap().reads.len(), 6);
            assert_eq!(native.reader_quarantines.load(Ordering::SeqCst), 0);
            assert_eq!(native.source_quarantines.load(Ordering::SeqCst), 0);
            stop.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(1), server)
                .await
                .unwrap()
                .unwrap();
        }
    }
}
