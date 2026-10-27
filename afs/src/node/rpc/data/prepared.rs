//! Bounded authenticated snapshots for aligned TCP payload measurements.

use super::{DfsChunksService, DfsReadRangesCompletion, authenticate_peer};
use afs_protocol::node_data::{
    DfsFetchPreparedReadRequest, DfsPreparedReadReply, DfsReadRangesFrame, DfsReadRangesHeader,
    DfsReadRangesRequest, DfsReleasePreparedReadReply, DfsReleasePreparedReadRequest,
    dfs_read_ranges_frame,
};
use std::{
    collections::HashMap,
    io::Read,
    sync::{
        Arc, Mutex, MutexGuard, Weak,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tonic::{Request, Response, Status};

/// The TCP snapshot budget follows the sequential ReadGroup budget so the TCP
/// baseline accepts the same aggregate batch sizes as HIXL. Each batch is at
/// most 128 MiB, with at most eight active items and 256 MiB reserved across
/// preparing, ready, and in-flight payloads.
pub(super) const MAX_PREPARED_READ_BYTES: usize =
    crate::node::dfs_read::MAX_DFS_GROUP_BYTES as usize;
const MAX_PREPARED_READ_ITEMS: usize = 8;
const MAX_PREPARED_READ_AGGREGATE_BYTES: usize = 256 * 1024 * 1024;
const MAX_PREPARED_READ_TTL: Duration = Duration::from_secs(5);
const PREPARED_READ_FRAME_BYTES: usize = 64 * 1024;

static NEXT_PREPARED_READ_ID: AtomicU64 = AtomicU64::new(1);

pub(super) struct PreparedReadRegistry {
    state: Mutex<PreparedReadState>,
}

#[derive(Default)]
struct PreparedReadState {
    entries: HashMap<String, PreparedReadEntry>,
    active_items: usize,
    reserved_bytes: usize,
}

struct PreparedReadEntry {
    peer: String,
    read_id: String,
    attempt_id: String,
    request: DfsReadRangesRequest,
    bytes: Vec<u8>,
    completions: Vec<DfsReadRangesCompletion>,
    grant_expires_at_unix_ms: u64,
    expires_at: Instant,
    _reservation: Arc<PreparedReadReservation>,
}

struct PreparedReadPayload {
    request: DfsReadRangesRequest,
    bytes: Vec<u8>,
    completions: Vec<DfsReadRangesCompletion>,
    expires_at: Instant,
}

struct PreparedReadTake {
    payload: PreparedReadPayload,
    reservation: Arc<PreparedReadReservation>,
}

struct PreparedReadReservation {
    registry: Weak<PreparedReadRegistry>,
    bytes: usize,
}

impl PreparedReadRegistry {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(PreparedReadState::default()),
        }
    }

    fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Arc<PreparedReadReservation>, Status> {
        if bytes == 0 || bytes > MAX_PREPARED_READ_BYTES {
            return Err(Status::invalid_argument(
                "prepared DFS read exceeds its per-batch byte budget",
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| Status::internal("prepared DFS registry is poisoned"))?;
        if state.active_items >= MAX_PREPARED_READ_ITEMS
            || bytes > MAX_PREPARED_READ_AGGREGATE_BYTES.saturating_sub(state.reserved_bytes)
        {
            return Err(Status::resource_exhausted(
                "prepared DFS snapshot budget is full",
            ));
        }
        let active_items = state
            .active_items
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("prepared DFS item count overflowed"))?;
        let reserved_bytes = state
            .reserved_bytes
            .checked_add(bytes)
            .ok_or_else(|| Status::resource_exhausted("prepared DFS byte budget overflowed"))?;
        state.active_items = active_items;
        state.reserved_bytes = reserved_bytes;
        Ok(Arc::new(PreparedReadReservation {
            registry: Arc::downgrade(self),
            bytes,
        }))
    }

    fn insert(&self, id: String, entry: PreparedReadEntry) -> Result<(), Status> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Status::internal("prepared DFS registry is poisoned"))?;
        if state.entries.contains_key(&id) {
            return Err(Status::internal("prepared DFS ID collision"));
        }
        state.entries.insert(id, entry);
        Ok(())
    }

    fn take(
        &self,
        peer: &str,
        request: &DfsFetchPreparedReadRequest,
    ) -> Result<PreparedReadTake, Status> {
        let now_unix_ms = unix_time_ms()?;
        let entry = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| Status::internal("prepared DFS registry is poisoned"))?;
            let entry = state
                .entries
                .get(&request.prepared_read_id)
                .ok_or_else(|| Status::not_found("prepared DFS snapshot is absent"))?;
            if entry.peer != peer
                || entry.read_id != request.read_id
                || entry.attempt_id != request.attempt_id
            {
                return Err(Status::permission_denied(
                    "prepared DFS snapshot belongs to another peer or attempt",
                ));
            }
            if entry.expires_at <= Instant::now() || entry.grant_expires_at_unix_ms <= now_unix_ms {
                let expired = state.entries.remove(&request.prepared_read_id);
                drop(state);
                drop(expired);
                return Err(Status::not_found("prepared DFS snapshot has expired"));
            }
            state
                .entries
                .remove(&request.prepared_read_id)
                .ok_or_else(|| Status::not_found("prepared DFS snapshot is absent"))?
        };
        Ok(PreparedReadTake {
            payload: PreparedReadPayload {
                request: entry.request,
                bytes: entry.bytes,
                completions: entry.completions,
                expires_at: entry.expires_at,
            },
            reservation: entry._reservation,
        })
    }

    fn release(&self, peer: &str, request: &DfsReleasePreparedReadRequest) -> Result<bool, Status> {
        let removed = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| Status::internal("prepared DFS registry is poisoned"))?;
            let Some(entry) = state.entries.get(&request.prepared_read_id) else {
                return Ok(false);
            };
            if entry.peer != peer
                || entry.read_id != request.read_id
                || entry.attempt_id != request.attempt_id
            {
                return Err(Status::permission_denied(
                    "prepared DFS snapshot belongs to another peer or attempt",
                ));
            }
            state.entries.remove(&request.prepared_read_id)
        };
        let released = removed.is_some();
        drop(removed);
        Ok(released)
    }

    fn expire(&self, id: &str, expires_at: Instant) {
        let expired = {
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(error) => {
                    self.state.clear_poison();
                    error.into_inner()
                }
            };
            if state
                .entries
                .get(id)
                .is_some_and(|entry| entry.expires_at == expires_at)
            {
                state.entries.remove(id)
            } else {
                None
            }
        };
        drop(expired);
    }

    #[cfg(test)]
    pub(super) fn retained_usage(&self) -> (usize, usize) {
        self.state
            .lock()
            .map(|state| (state.active_items, state.reserved_bytes))
            .unwrap_or_default()
    }
}

impl Drop for PreparedReadReservation {
    fn drop(&mut self) {
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        let mut state = match registry.state.lock() {
            Ok(state) => state,
            Err(error) => {
                registry.state.clear_poison();
                error.into_inner()
            }
        };
        state.active_items = state.active_items.saturating_sub(1);
        state.reserved_bytes = state.reserved_bytes.saturating_sub(self.bytes);
    }
}

impl DfsChunksService {
    pub(super) async fn prepare_prepared_read(
        &self,
        request: Request<DfsReadRangesRequest>,
    ) -> Result<Response<DfsPreparedReadReply>, Status> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| Status::internal("prepared DFS reads require a Tokio runtime"))?;
        let peer = authenticate_peer(self.authenticator.as_ref(), &request)?;
        let request = request.into_inner();
        super::validate_dfs_read_request(&request)?;
        for operation in &request.operations {
            if operation
                .grant
                .as_ref()
                .is_none_or(|grant| grant.caller_node_id != peer)
            {
                return Err(Status::permission_denied(
                    "DFS read grant caller does not match the authenticated peer",
                ));
            }
        }
        let length = packed_dfs_read_length(&request, MAX_PREPARED_READ_BYTES)?;
        let authorizer = self.authorizer.clone();
        let authorized_peer = peer.clone();
        let (request, peer) = tokio::task::spawn_blocking(move || {
            authorizer
                .authorize(&authorized_peer, &request)
                .map(|()| (request, authorized_peer))
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?
        .map_err(super::error_to_status)?;
        let local = self.local_chunks.clone().ok_or_else(|| {
            super::coded_status(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "DFS local ChunkStore is not available",
            )
        })?;
        let reservation = self.prepared_reads.reserve(length)?;
        let read_request = request.clone();
        let (bytes, completions, reservation) = tokio::task::spawn_blocking(move || {
            let (bytes, completions) = read_packed_dfs_ranges_with_budget(
                &local,
                &read_request,
                length,
                MAX_PREPARED_READ_BYTES,
            )?;
            Ok::<_, Status>((bytes, completions, reservation))
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))??;

        let (grant_expires_at_unix_ms, expires_at) = prepared_read_expiry(&request)?;
        if expires_at <= Instant::now() {
            return Err(Status::permission_denied(
                "DFS read grant expired during snapshot preparation",
            ));
        }
        let packed_length = u64::try_from(length)
            .map_err(|_| Status::internal("prepared DFS length exceeds protocol range"))?;
        let prepared_read_id = new_prepared_read_id()?;
        let read_id = request.read_id.clone();
        let attempt_id = request.attempt_id.clone();
        let completions_for_reply = completions.clone();
        self.prepared_reads.insert(
            prepared_read_id.clone(),
            PreparedReadEntry {
                peer,
                read_id: read_id.clone(),
                attempt_id: attempt_id.clone(),
                request,
                bytes,
                completions,
                grant_expires_at_unix_ms,
                expires_at,
                _reservation: reservation,
            },
        )?;
        let registry = Arc::downgrade(&self.prepared_reads);
        let expiry_id = prepared_read_id.clone();
        runtime.spawn(async move {
            tokio::time::sleep_until(tokio::time::Instant::from_std(expires_at)).await;
            if let Some(registry) = registry.upgrade() {
                registry.expire(&expiry_id, expires_at);
            }
        });

        Ok(Response::new(DfsPreparedReadReply {
            prepared_read_id,
            read_id,
            attempt_id,
            packed_length,
            completions: completions_for_reply,
        }))
    }

    pub(super) async fn fetch_prepared_read_snapshot(
        &self,
        request: Request<DfsFetchPreparedReadRequest>,
    ) -> Result<Response<PinPreparedReadStream>, Status> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| Status::internal("prepared DFS reads require a Tokio runtime"))?;
        let peer = authenticate_peer(self.authenticator.as_ref(), &request)?;
        let request = request.into_inner();
        validate_prepared_read_identity(
            &request.prepared_read_id,
            &request.read_id,
            &request.attempt_id,
        )?;
        let PreparedReadTake {
            payload,
            reservation,
        } = self.prepared_reads.take(&peer, &request)?;
        let lease = Arc::new(Mutex::new(Some(reservation.clone())));
        let expiry_lease = lease.clone();
        let expires_at = payload.expires_at;
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        let receiver = Arc::new(Mutex::new(receiver));
        let expiry_receiver = receiver.clone();
        runtime.spawn(async move {
            tokio::time::sleep_until(tokio::time::Instant::from_std(expires_at)).await;
            stop_prepared_read_receiver(&expiry_receiver);
            release_prepared_read_reservation(&expiry_lease);
        });
        let metrics = self.payload_metrics.clone();
        runtime.spawn(async move {
            let result = stream_prepared_read(&payload, &sender, metrics.as_ref()).await;
            drop(payload);
            if let Err(error) = result
                && error.code() != tonic::Code::Cancelled
            {
                let _ = sender.try_send(Err(error));
            }
            drop(reservation);
        });
        Ok(Response::new(Box::pin(PreparedReadStream {
            receiver,
            reservation: lease,
        })))
    }

    pub(super) async fn release_prepared_read_snapshot(
        &self,
        request: Request<DfsReleasePreparedReadRequest>,
    ) -> Result<Response<DfsReleasePreparedReadReply>, Status> {
        let peer = authenticate_peer(self.authenticator.as_ref(), &request)?;
        let request = request.into_inner();
        validate_prepared_read_identity(
            &request.prepared_read_id,
            &request.read_id,
            &request.attempt_id,
        )?;
        let released = self.prepared_reads.release(&peer, &request)?;
        Ok(Response::new(DfsReleasePreparedReadReply { released }))
    }
}

pub(super) type PinPreparedReadStream =
    std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<DfsReadRangesFrame, Status>> + Send>>;

struct PreparedReadStream {
    receiver: Arc<Mutex<tokio::sync::mpsc::Receiver<Result<DfsReadRangesFrame, Status>>>>,
    reservation: Arc<Mutex<Option<Arc<PreparedReadReservation>>>>,
}

impl tokio_stream::Stream for PreparedReadStream {
    type Item = Result<DfsReadRangesFrame, Status>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let item = lock_prepared_read_receiver(&this.receiver).poll_recv(context);
        if matches!(&item, Poll::Ready(None)) {
            release_prepared_read_reservation(&this.reservation);
        }
        item
    }
}

impl Drop for PreparedReadStream {
    fn drop(&mut self) {
        stop_prepared_read_receiver(&self.receiver);
        release_prepared_read_reservation(&self.reservation);
    }
}

fn release_prepared_read_reservation(
    reservation: &Arc<Mutex<Option<Arc<PreparedReadReservation>>>>,
) {
    let mut reservation = match reservation.lock() {
        Ok(reservation) => reservation,
        Err(error) => {
            reservation.clear_poison();
            error.into_inner()
        }
    };
    drop(reservation.take());
}

fn lock_prepared_read_receiver(
    receiver: &Mutex<tokio::sync::mpsc::Receiver<Result<DfsReadRangesFrame, Status>>>,
) -> MutexGuard<'_, tokio::sync::mpsc::Receiver<Result<DfsReadRangesFrame, Status>>> {
    match receiver.lock() {
        Ok(receiver) => receiver,
        Err(error) => {
            receiver.clear_poison();
            error.into_inner()
        }
    }
}

fn stop_prepared_read_receiver(
    receiver: &Arc<Mutex<tokio::sync::mpsc::Receiver<Result<DfsReadRangesFrame, Status>>>>,
) {
    let mut receiver = lock_prepared_read_receiver(receiver);
    receiver.close();
    while let Ok(frame) = receiver.try_recv() {
        drop(frame);
    }
}

fn validate_prepared_read_identity(
    id: &str,
    read_id: &str,
    attempt_id: &str,
) -> Result<(), Status> {
    if id.is_empty()
        || id.len() > 128
        || read_id.is_empty()
        || read_id.len() > 512
        || attempt_id.is_empty()
        || attempt_id.len() > 512
    {
        return Err(Status::invalid_argument(
            "prepared DFS read identity is empty or exceeds its bound",
        ));
    }
    Ok(())
}

fn prepared_read_expiry(request: &DfsReadRangesRequest) -> Result<(u64, Instant), Status> {
    let grant_expiry = request
        .operations
        .iter()
        .filter_map(|operation| operation.grant.as_ref())
        .map(|grant| grant.expires_at_unix_ms)
        .min()
        .ok_or_else(|| Status::permission_denied("prepared DFS read has no grant"))?;
    // Capture monotonic time before wall time so the lease deadline is never
    // later than the grant deadline due to sampling the clocks separately.
    let monotonic_started_at = Instant::now();
    let now = SystemTime::now();
    let grant_deadline = UNIX_EPOCH
        .checked_add(Duration::from_millis(grant_expiry))
        .ok_or_else(|| Status::invalid_argument("DFS grant expiry exceeds system time range"))?;
    let remaining = grant_deadline
        .duration_since(now)
        .map_err(|_| Status::permission_denied("DFS read grant expired during preparation"))?;
    let ttl = remaining.min(MAX_PREPARED_READ_TTL);
    let deadline = monotonic_started_at
        .checked_add(ttl)
        .ok_or_else(|| Status::internal("prepared DFS lease deadline overflowed"))?;
    Ok((grant_expiry, deadline))
}

fn unix_time_ms() -> Result<u64, Status> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Status::internal("system clock is before the Unix epoch"))?
        .as_millis();
    u64::try_from(millis).map_err(|_| Status::internal("system time exceeds protocol range"))
}

fn new_prepared_read_id() -> Result<String, Status> {
    let sequence = NEXT_PREPARED_READ_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .map_err(|_| Status::resource_exhausted("prepared DFS ID sequence exhausted"))?;
    let mut entropy = [0_u8; 16];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut random| random.read_exact(&mut entropy))
        .map_err(|error| {
            Status::unavailable(format!("prepared DFS ID entropy unavailable: {error}"))
        })?;
    let mut id = String::with_capacity(49);
    for byte in entropy {
        use std::fmt::Write;
        write!(&mut id, "{byte:02x}")
            .map_err(|_| Status::internal("prepared DFS ID formatting failed"))?;
    }
    use std::fmt::Write;
    write!(&mut id, "-{sequence:016x}")
        .map_err(|_| Status::internal("prepared DFS ID formatting failed"))?;
    Ok(id)
}

async fn stream_prepared_read(
    payload: &PreparedReadPayload,
    sender: &tokio::sync::mpsc::Sender<Result<DfsReadRangesFrame, Status>>,
    metrics: Option<&super::DfsPayloadMetrics>,
) -> Result<(), Status> {
    let send = |body| async move {
        tokio::select! {
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(payload.expires_at)) => {
                Err(Status::deadline_exceeded("prepared DFS read lease expired during fetch"))
            }
            result = sender.send(Ok(DfsReadRangesFrame { body: Some(body) })) => {
                result.map_err(|_| Status::cancelled("prepared DFS read consumer disconnected"))
            }
        }
    };
    let mut packed_cursor = 0_usize;
    for (operation, completion) in payload.request.operations.iter().zip(&payload.completions) {
        if sender.is_closed() {
            return Ok(());
        }
        send(dfs_read_ranges_frame::Body::Header(DfsReadRangesHeader {
            crc32c_table: Default::default(),
            read_id: payload.request.read_id.clone(),
            attempt_id: payload.request.attempt_id.clone(),
            operation_index: operation.operation_index,
            chunk_id: operation.chunk_id.clone(),
            chunk_offset: operation.chunk_offset,
            length: operation.length,
            source_copy_id: operation.source_copy_id.clone(),
            blake3_outboard: prost::bytes::Bytes::new(),
        }))
        .await?;
        let start = packed_cursor;
        let length = usize::try_from(operation.length)
            .map_err(|_| Status::internal("prepared DFS range length exceeds address space"))?;
        let end = start
            .checked_add(length)
            .filter(|end| *end <= payload.bytes.len())
            .ok_or_else(|| Status::internal("prepared DFS range exceeds its snapshot"))?;
        let range = payload
            .bytes
            .get(start..end)
            .ok_or_else(|| Status::internal("prepared DFS range is absent from its snapshot"))?;
        let mut offset = 0;
        while offset < range.len() {
            if sender.is_closed() {
                return Ok(());
            }
            let end = offset
                .saturating_add(PREPARED_READ_FRAME_BYTES)
                .min(range.len());
            let data = prost::bytes::Bytes::copy_from_slice(&range[offset..end]);
            let sent_bytes = data.len();
            send(dfs_read_ranges_frame::Body::Data(data)).await?;
            if let Some(metrics) = metrics {
                metrics.record("grpc", "send", "read", sent_bytes as u64);
            }
            offset = end;
        }
        send(dfs_read_ranges_frame::Body::Completion(completion.clone())).await?;
        packed_cursor = end;
    }
    Ok(())
}

pub(super) fn validate_packed_dfs_read(request: &DfsReadRangesRequest) -> Result<usize, Status> {
    let length = packed_dfs_read_length(request, crate::node::chunk::MAX_STAGED_CHUNK_BYTES)?;
    let mut destination_offset = 0_u64;
    for operation in &request.operations {
        if operation.destination_offset != destination_offset {
            return Err(Status::invalid_argument(
                "DFS read windows must be tightly packed",
            ));
        }
        destination_offset = destination_offset
            .checked_add(operation.length)
            .ok_or_else(|| Status::invalid_argument("packed DFS destination overflowed"))?;
    }
    Ok(length)
}

pub(super) fn packed_dfs_read_length(
    request: &DfsReadRangesRequest,
    byte_budget: usize,
) -> Result<usize, Status> {
    // The snapshot is always packed in operation order. The request's
    // destination offsets describe the caller's output layout and may contain
    // gaps; streaming headers retain those original operations unchanged.
    let byte_budget = u64::try_from(byte_budget)
        .map_err(|_| Status::invalid_argument("packed DFS byte budget exceeds protocol range"))?;
    let mut length = 0_u64;
    for op in &request.operations {
        length = length
            .checked_add(op.length)
            .filter(|length| *length <= byte_budget)
            .ok_or_else(|| Status::invalid_argument("packed DFS read exceeds its byte budget"))?;
    }
    usize::try_from(length)
        .map_err(|_| Status::invalid_argument("packed DFS read length exceeds address space"))
}

pub(super) fn read_packed_dfs_ranges(
    local: &crate::node::chunk::LocalChunkStore,
    request: &DfsReadRangesRequest,
    length: usize,
) -> Result<(Vec<u8>, Vec<DfsReadRangesCompletion>), Status> {
    let budget = usize::try_from(crate::node::dfs_read::MAX_DFS_READ_BYTES)
        .map_err(|_| Status::invalid_argument("DFS read byte budget exceeds platform capacity"))?;
    read_packed_dfs_ranges_with_budget(local, request, length, budget)
}

/// Chunk scans are independent reads plus BLAKE3, so a batch completes in
/// roughly one Chunk's scan time instead of the sum of its Chunks.
fn source_scan_pool() -> Option<&'static rayon_core::ThreadPool> {
    static POOL: std::sync::OnceLock<Option<rayon_core::ThreadPool>> = std::sync::OnceLock::new();
    POOL.get_or_init(|| {
        let workers = std::thread::available_parallelism()
            .map_or(4, |parallel| parallel.get())
            .clamp(1, MAX_SOURCE_SCAN_THREADS);
        rayon_core::ThreadPoolBuilder::new()
            .num_threads(workers)
            .thread_name(|index| format!("afs-source-scan-{index}"))
            .build()
            .ok()
    })
    .as_ref()
}

/// A verified group batch holds at most 128 MiB of 4 MiB Chunks, so this also
/// bounds the transient per-Chunk copy buffers to 64 MiB.
const MAX_SOURCE_SCAN_THREADS: usize = 16;

/// One range's transport digest and its bounded descriptor.
#[derive(Clone)]
struct PackedRangeDigest {
    checksum: Vec<u8>,
    algorithm: i32,
    table: Vec<u8>,
}

/// One Chunk's requested ranges in packed-output order.
struct PackedChunkGroup {
    chunk_id: String,
    /// Operation index, relative output range, and absolute packed offset.
    ranges: Vec<(usize, crate::node::chunk::VerifiedRangeCopy, usize)>,
    relative_length: usize,
}

/// Runs one independent Chunk scan per slot on the bounded scan pool.
fn run_chunk_scans<F>(count: usize, scan: F)
where
    F: Fn(usize) + Send + Sync,
{
    match source_scan_pool() {
        Some(pool) => {
            let scan = &scan;
            pool.install(|| {
                rayon_core::scope(|scope| {
                    for index in 0..count {
                        scope.spawn(move |_| scan(index));
                    }
                });
            });
        }
        None => (0..count).for_each(scan),
    }
}

/// Verifies one immutable Chunk once, copies every requested range into `out`
/// and returns the per-range digests from that same scan.
fn scan_packed_chunk(
    local: &crate::node::chunk::LocalChunkStore,
    group: &PackedChunkGroup,
    out: &mut [u8],
    deadline: Instant,
) -> Result<Vec<PackedRangeDigest>, Status> {
    let id = crate::dfs::ChunkId::new(group.chunk_id.clone());
    // CRC32C objects are bound to a Meta descriptor, so each range digest is
    // the CRC of the copied bytes plus that bounded descriptor.
    if let Some(table) = local
        .peer_crc32c_table(&id)
        .map_err(super::error_to_status)?
    {
        let descriptor =
            serde_json::to_vec(&table).map_err(|error| Status::internal(error.to_string()))?;
        let mut digests = Vec::with_capacity(group.ranges.len());
        for (_, range, _) in &group.ranges {
            let end = range
                .output_offset
                .checked_add(range.length)
                .ok_or_else(|| Status::internal("packed DFS range overflow"))?;
            let payload = local
                .read_peer_range_until(&id, range.chunk_offset, range.length, deadline)
                .map_err(super::error_to_status)?;
            let data = payload.as_slice();
            if data.len() != range.length {
                return Err(super::coded_status(
                    afs_error::NODE_TRANSFER_CORRUPT_DATA,
                    "immutable Chunk ended before packed DFS range",
                ));
            }
            out.get_mut(range.output_offset..end)
                .ok_or_else(|| Status::internal("packed DFS range exceeds its buffer"))?
                .copy_from_slice(data);
            digests.push(PackedRangeDigest {
                checksum: crate::node::chunk_crc32c::checksum(data)
                    .to_le_bytes()
                    .to_vec(),
                algorithm: afs_protocol::node_data::DfsDigestAlgorithm::Crc32c as i32,
                table: descriptor.clone(),
            });
        }
        return Ok(digests);
    }
    let copies = group
        .ranges
        .iter()
        .map(|(_, range, _)| *range)
        .collect::<Vec<_>>();
    match local.read_ranges_at_uncommitted_with_checksums(&id, &copies, out) {
        Ok((copied, checksums))
            if copied.len() == copies.len()
                && copied
                    .iter()
                    .zip(&copies)
                    .all(|(count, range)| *count == range.length)
                && checksums.len() == copies.len() =>
        {
            Ok(checksums
                .into_iter()
                .map(|checksum| PackedRangeDigest {
                    checksum: checksum.to_vec(),
                    algorithm: afs_protocol::node_data::DfsDigestAlgorithm::Blake3 as i32,
                    table: Vec::new(),
                })
                .collect())
        }
        Ok(_) => Err(super::coded_status(
            afs_error::NODE_TRANSFER_CORRUPT_DATA,
            "immutable Chunk ended before packed DFS range",
        )),
        Err(error) => Err(super::error_to_status(error)),
    }
}

/// Returns each Chunk group's packed-output span when every group's ranges are
/// contiguous and the spans tile the whole output. The scan can then write the
/// final snapshot directly instead of copying the payload a second time.
fn contiguous_group_spans(
    groups: &[PackedChunkGroup],
    length: usize,
) -> Option<Vec<(usize, usize)>> {
    let mut spans = Vec::with_capacity(groups.len());
    for group in groups {
        let (_, _, first_output) = group.ranges.first()?;
        let mut cursor = *first_output;
        for (_, range, output_offset) in &group.ranges {
            if *output_offset != cursor {
                return None;
            }
            cursor = cursor.checked_add(range.length)?;
        }
        if cursor != first_output.checked_add(group.relative_length)? {
            return None;
        }
        spans.push((*first_output, cursor));
    }
    let mut ordered = (0..spans.len()).collect::<Vec<_>>();
    ordered.sort_unstable_by_key(|&index| spans[index].0);
    let mut cursor = 0_usize;
    for index in ordered {
        if spans[index].0 != cursor {
            return None;
        }
        cursor = spans[index].1;
    }
    (cursor == length).then_some(spans)
}

/// Group snapshots use the explicit group budget. Each Chunk is verified and
/// hashed once; the per-range BLAKE3 digests come from that same scan.
pub(super) fn read_packed_dfs_ranges_with_budget(
    local: &crate::node::chunk::LocalChunkStore,
    request: &DfsReadRangesRequest,
    length: usize,
    budget: usize,
) -> Result<(Vec<u8>, Vec<DfsReadRangesCompletion>), Status> {
    if packed_dfs_read_length(request, budget)? != length {
        return Err(Status::invalid_argument(
            "packed DFS read length does not match its operations",
        ));
    }
    let mut bytes = vec![0; length];
    // Chunk order must stay deterministic for error selection; each Chunk is
    // verified and hashed independently, so the scans run concurrently.
    let mut groups = Vec::<PackedChunkGroup>::new();
    let mut group_index = HashMap::<String, usize>::new();
    let mut packed_cursor = 0_usize;
    for (index, op) in request.operations.iter().enumerate() {
        if op.length == 0 || op.chunk_offset.checked_add(op.length).is_none() {
            return Err(Status::invalid_argument(
                "packed DFS read contains an invalid source range",
            ));
        }
        let start = packed_cursor;
        let range_length = usize::try_from(op.length)
            .map_err(|_| Status::invalid_argument("DFS packed output length is too large"))?;
        let end = start
            .checked_add(range_length)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| Status::invalid_argument("DFS packed output range exceeds buffer"))?;
        packed_cursor = end;
        let slot = match group_index.get(&op.chunk_id) {
            Some(slot) => *slot,
            None => {
                let slot = groups.len();
                group_index.insert(op.chunk_id.clone(), slot);
                groups.push(PackedChunkGroup {
                    chunk_id: op.chunk_id.clone(),
                    ranges: Vec::new(),
                    relative_length: 0,
                });
                slot
            }
        };
        let relative_offset = groups[slot].relative_length;
        groups[slot].relative_length = groups[slot]
            .relative_length
            .checked_add(range_length)
            .ok_or_else(|| Status::invalid_argument("DFS packed chunk length overflow"))?;
        groups[slot].ranges.push((
            index,
            crate::node::chunk::VerifiedRangeCopy {
                chunk_offset: op.chunk_offset,
                length: range_length,
                output_offset: relative_offset,
            },
            start,
        ));
    }

    let mut range_digests = vec![None; request.operations.len()];
    let deadline = local.peer_serve_deadline();
    if let Some(spans) = contiguous_group_spans(&groups, length) {
        let mut ordered = (0..groups.len()).collect::<Vec<_>>();
        ordered.sort_unstable_by_key(|&index| spans[index].0);
        let mut slices = (0..groups.len())
            .map(|_| None)
            .collect::<Vec<Option<&mut [u8]>>>();
        let mut remainder: &mut [u8] = &mut bytes;
        for index in ordered {
            let (start, end) = spans[index];
            let (head, tail) = remainder.split_at_mut(end - start);
            slices[index] = Some(head);
            remainder = tail;
        }
        let outputs = Mutex::new(slices);
        let slots = Mutex::new(
            (0..groups.len())
                .map(|_| None)
                .collect::<Vec<Option<Result<Vec<PackedRangeDigest>, Status>>>>(),
        );
        let scan = |index: usize| {
            let output = match outputs.lock() {
                Ok(mut outputs) => outputs[index].take(),
                Err(poisoned) => poisoned.into_inner()[index].take(),
            };
            let result = output
                .ok_or_else(|| Status::internal("verified DFS range output slice disappeared"))
                .and_then(|output| scan_packed_chunk(local, &groups[index], output, deadline));
            match slots.lock() {
                Ok(mut slots) => slots[index] = Some(result),
                Err(poisoned) => poisoned.into_inner()[index] = Some(result),
            }
        };
        run_chunk_scans(groups.len(), scan);
        drop(outputs);
        let mut slots = slots
            .into_inner()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for (index, group) in groups.iter().enumerate() {
            let scan = slots[index]
                .take()
                .ok_or_else(|| Status::internal("verified DFS range checksum scan did not run"))?;
            let digests = scan?;
            #[cfg(test)]
            crate::node::chunk::note_verified_scan_for_test();
            if digests.len() != group.ranges.len() {
                return Err(Status::internal(
                    "verified DFS range checksum count changed",
                ));
            }
            for ((operation_index, _, _), digest) in group.ranges.iter().zip(digests) {
                range_digests[*operation_index] = Some(digest);
            }
        }
    } else {
        // Interleaved Chunk operations cannot share one final slice, so each
        // scan keeps a private buffer and the caller copies it afterwards.
        type ChunkScan = Result<(Vec<u8>, Vec<PackedRangeDigest>), Status>;
        let slots = Mutex::new(
            (0..groups.len())
                .map(|_| None)
                .collect::<Vec<Option<ChunkScan>>>(),
        );
        let scan = |index: usize| {
            let group = &groups[index];
            let mut buffer = vec![0; group.relative_length];
            let result = scan_packed_chunk(local, group, &mut buffer, deadline)
                .map(|checksums| (buffer, checksums));
            match slots.lock() {
                Ok(mut slots) => slots[index] = Some(result),
                Err(poisoned) => poisoned.into_inner()[index] = Some(result),
            }
        };
        run_chunk_scans(groups.len(), scan);
        let mut slots = slots
            .into_inner()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for (index, group) in groups.iter().enumerate() {
            let scan = slots[index]
                .take()
                .ok_or_else(|| Status::internal("verified DFS range checksum scan did not run"))?;
            let (buffer, digests) = scan?;
            #[cfg(test)]
            crate::node::chunk::note_verified_scan_for_test();
            let mut offset = 0_usize;
            for ((operation_index, range, output_offset), digest) in
                group.ranges.iter().zip(digests)
            {
                let end = offset
                    .checked_add(range.length)
                    .filter(|end| *end <= buffer.len())
                    .ok_or_else(|| Status::internal("verified DFS range output overflow"))?;
                let output_end = output_offset
                    .checked_add(range.length)
                    .filter(|end| *end <= bytes.len())
                    .ok_or_else(|| Status::internal("verified DFS output range exceeds buffer"))?;
                bytes[*output_offset..output_end].copy_from_slice(&buffer[offset..end]);
                offset = end;
                range_digests[*operation_index] = Some(digest);
            }
        }
    }

    let mut completions = Vec::with_capacity(request.operations.len());
    for (index, op) in request.operations.iter().enumerate() {
        let digest = range_digests[index]
            .take()
            .ok_or_else(|| Status::internal("verified DFS range checksum disappeared"))?;
        completions.push(DfsReadRangesCompletion {
            crc32c_table: digest.table,
            read_id: request.read_id.clone(),
            attempt_id: request.attempt_id.clone(),
            operation_index: op.operation_index,
            source_copy_id: op.source_copy_id.clone(),
            transferred_bytes: op.length,
            range_checksum: digest.checksum,
            range_checksum_algorithm: digest.algorithm,
        });
    }
    Ok((bytes, completions))
}

#[cfg(test)]
mod tests {
    use super::super::{
        DenyDfsReplicaAuthorizer, DfsChunksService, DfsReadAuthorizer, PeerAuthenticator,
    };
    use crate::node::chunk::ChunkStore;
    use afs_protocol::node_data::{
        DfsChunkReadOp, DfsReadRangesRequest, dfs_chunks_server::DfsChunks, dfs_read_ranges_frame,
    };
    use std::{
        net::SocketAddr,
        sync::{Arc, Condvar, Mutex},
    };
    use tokio_stream::StreamExt;
    use tonic::{Request, metadata::MetadataMap};

    struct FixturePeer(&'static str);

    impl PeerAuthenticator for FixturePeer {
        fn authenticate(
            &self,
            _: &MetadataMap,
            _: Option<SocketAddr>,
            _: Option<&[u8]>,
        ) -> afs_error::Result<String> {
            Ok(self.0.into())
        }
    }

    struct FixtureReadAuthority;

    impl DfsReadAuthorizer for FixtureReadAuthority {
        fn authorize(&self, peer: &str, request: &DfsReadRangesRequest) -> afs_error::Result<()> {
            if peer == "reader"
                && request.operations.iter().all(|op| {
                    op.grant
                        .as_ref()
                        .is_some_and(|grant| grant.token == "fixture-grant")
                })
            {
                Ok(())
            } else {
                Err(afs_error::Error::coded(
                    afs_error::NODE_TRANSFER_INVALID,
                    "fixture read grant rejected",
                ))
            }
        }
    }

    fn service(local_chunks: Arc<crate::node::chunk::LocalChunkStore>) -> DfsChunksService {
        DfsChunksService {
            local_chunks: Some(local_chunks),
            authenticator: Arc::new(FixturePeer("reader")),
            authorizer: Arc::new(FixtureReadAuthority),
            replica_authorizer: Arc::new(DenyDfsReplicaAuthorizer),
            replica_data_plane: None,
            replica_permits: Arc::new(tokio::sync::Semaphore::new(8)),
            read_permits: Arc::new(tokio::sync::Semaphore::new(8)),
            read_limits: Default::default(),
            disk_slots: Arc::new(tokio::sync::Semaphore::new(1)),
            open_setup_gates: Arc::new(Default::default()),
            replica_timeout: std::time::Duration::from_secs(30),
            rdma_sessions: crate::node::rpc::control::RdmaSessionRegistry::new(None),
            payload_metrics: None,
            hixl: None,
            prepared_reads: Arc::new(super::PreparedReadRegistry::new()),
        }
    }

    fn read_request(chunk_id: String, length: u64) -> DfsReadRangesRequest {
        DfsReadRangesRequest {
            read_id: "prepared-read".into(),
            attempt_id: "prepared-attempt".into(),
            file_version_id: "fixture-version".into(),
            layout_root_id: "fixture-layout".into(),
            include_blake3_outboard: false,
            operations: vec![DfsChunkReadOp {
                operation_index: 0,
                chunk_id,
                chunk_offset: 0,
                length,
                destination_offset: 0,
                source_copy_id: "fixture-copy".into(),
                grant: Some(afs_protocol::node_data::DfsReadGrant {
                    namespace_id: "fixture-namespace".into(),
                    file_version_id: "fixture-version".into(),
                    layout_root_id: "fixture-layout".into(),
                    caller_node_id: "reader".into(),
                    caller_node_epoch: 1,
                    expires_at_unix_ms: u64::MAX,
                    fence: 1,
                    token: "fixture-grant".into(),
                }),
            }],
        }
    }

    fn error_code<T>(result: Result<T, tonic::Status>) -> tonic::Code {
        match result {
            Ok(_) => panic!("expected prepared-read request to fail"),
            Err(status) => status.code(),
        }
    }

    async fn wait_for_no_retained_payload(service: &DfsChunksService) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if service.prepared_reads.retained_usage() == (0, 0) {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("prepared payload reservation should be reclaimed");
    }

    #[test]
    fn packed_read_hashes_each_range_during_the_single_chunk_scan() {
        use crate::node::chunk::{
            StagedChunk, reset_verified_scan_count_for_test, verified_scan_count_for_test,
        };

        let temp = tempfile::tempdir().unwrap();
        let local = crate::node::chunk::LocalChunkStore::open(temp.path(), "node").unwrap();
        let bytes: Vec<_> = (0..70_000).map(|index| (index % 251) as u8).collect();
        let staged = StagedChunk::new(
            crate::dfs::OperationId::new("packed-single-scan"),
            bytes.clone(),
        );
        local.put(staged.clone()).unwrap();
        let mut request = read_request(staged.chunk.id.0.clone(), 70_000);
        request.operations = vec![
            DfsChunkReadOp {
                operation_index: 0,
                chunk_id: staged.chunk.id.0.clone(),
                chunk_offset: 0,
                length: 70_000,
                destination_offset: 0,
                source_copy_id: "copy-a".into(),
                grant: None,
            },
            DfsChunkReadOp {
                operation_index: 1,
                chunk_id: staged.chunk.id.0.clone(),
                chunk_offset: 40_000,
                length: 30_000,
                destination_offset: 70_000,
                source_copy_id: "copy-b".into(),
                grant: None,
            },
        ];
        reset_verified_scan_count_for_test();
        let (packed, completions) =
            super::read_packed_dfs_ranges_with_budget(&local, &request, 100_000, 1024 * 1024)
                .unwrap();
        assert_eq!(verified_scan_count_for_test(), 1);
        assert_eq!(&packed[..70_000], &bytes[..]);
        assert_eq!(&packed[70_000..], &bytes[40_000..70_000]);
        assert_eq!(
            completions[0].range_checksum,
            staged.chunk.content_digest.bytes.to_vec()
        );
        assert_eq!(
            completions[1].range_checksum,
            blake3::hash(&bytes[40_000..70_000]).as_bytes().to_vec()
        );
    }

    #[tokio::test]
    async fn prepared_read_returns_checksums_before_fetch_and_fetch_is_single_use() {
        use crate::node::chunk::StagedChunk;

        let temp = tempfile::tempdir().unwrap();
        let local =
            Arc::new(crate::node::chunk::LocalChunkStore::open(temp.path(), "node").unwrap());
        let expected = vec![11, 22, 33, 44];
        let chunk = StagedChunk::new(
            crate::dfs::OperationId::new("prepared-fixture"),
            expected.clone(),
        );
        local.put(chunk.clone()).unwrap();
        let service = service(local);
        let request = read_request(chunk.chunk.id.0, expected.len() as u64);

        let reply = service
            .prepare_read_ranges(Request::new(request))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.read_id, "prepared-read");
        assert_eq!(reply.attempt_id, "prepared-attempt");
        assert_eq!(reply.packed_length, 4);
        assert_eq!(reply.completions.len(), 1);
        assert_eq!(reply.completions[0].transferred_bytes, 4);
        assert_eq!(
            reply.completions[0].range_checksum,
            blake3::hash(&expected).as_bytes().to_vec()
        );

        let fetch = afs_protocol::node_data::DfsFetchPreparedReadRequest {
            prepared_read_id: reply.prepared_read_id.clone(),
            read_id: reply.read_id,
            attempt_id: reply.attempt_id,
        };
        let response = service
            .fetch_prepared_read(Request::new(fetch.clone()))
            .await
            .unwrap();
        let frames = response.into_inner().collect::<Vec<_>>().await;
        let mut payload = Vec::new();
        let mut completion_count = 0;
        for frame in frames {
            let frame = frame.unwrap();
            match frame.body {
                Some(dfs_read_ranges_frame::Body::Data(data)) => payload.extend(data),
                Some(dfs_read_ranges_frame::Body::Completion(_)) => completion_count += 1,
                _ => {}
            }
        }
        assert_eq!(payload, expected);
        assert_eq!(completion_count, 1);
        assert_eq!(service.prepared_reads.retained_usage(), (0, 0));

        let replay = service.fetch_prepared_read(Request::new(fetch)).await;
        assert_eq!(
            error_code(replay),
            tonic::Code::NotFound,
            "a prepared snapshot must be consumed only once"
        );
    }

    #[tokio::test]
    async fn prepared_read_packs_ranges_in_request_order_and_preserves_headers() {
        use crate::node::chunk::{ChunkStore, StagedChunk};

        let temp = tempfile::tempdir().unwrap();
        let local =
            Arc::new(crate::node::chunk::LocalChunkStore::open(temp.path(), "node").unwrap());
        let expected_chunk = (0_u8..64).collect::<Vec<_>>();
        let chunk = StagedChunk::new(
            crate::dfs::OperationId::new("prepared-gapped-destinations"),
            expected_chunk.clone(),
        );
        local.put(chunk.clone()).unwrap();
        let service = service(local);

        let mut request = read_request(chunk.chunk.id.0, 3);
        request.operations[0].chunk_offset = 4;
        request.operations[0].destination_offset = 12;
        request.operations[0].operation_index = 0;
        request.operations[0].source_copy_id = "first-copy".into();
        let mut second = request.operations[0].clone();
        second.operation_index = 1;
        second.chunk_offset = 20;
        second.length = 2;
        second.destination_offset = 99;
        second.source_copy_id = "second-copy".into();
        request.operations.push(second);

        let reply = service
            .prepare_read_ranges(Request::new(request))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.packed_length, 5);
        assert_eq!(reply.completions.len(), 2);

        let response = service
            .fetch_prepared_read(Request::new(
                afs_protocol::node_data::DfsFetchPreparedReadRequest {
                    prepared_read_id: reply.prepared_read_id,
                    read_id: reply.read_id,
                    attempt_id: reply.attempt_id,
                },
            ))
            .await
            .unwrap();
        let frames = response.into_inner().collect::<Vec<_>>().await;
        let mut headers = Vec::new();
        let mut payload = Vec::new();
        for frame in frames {
            match frame.unwrap().body {
                Some(dfs_read_ranges_frame::Body::Header(header)) => headers.push(header),
                Some(dfs_read_ranges_frame::Body::Data(data)) => payload.extend(data),
                _ => {}
            }
        }
        assert_eq!(payload, [4, 5, 6, 20, 21]);
        assert_eq!(headers.len(), 2);
        assert_eq!(headers[0].operation_index, 0);
        assert_eq!(headers[0].chunk_offset, 4);
        assert_eq!(headers[0].source_copy_id, "first-copy");
        assert_eq!(headers[1].operation_index, 1);
        assert_eq!(headers[1].chunk_offset, 20);
        assert_eq!(headers[1].source_copy_id, "second-copy");
    }

    #[tokio::test]
    async fn prepared_read_is_bound_to_peer_read_and_attempt() {
        use crate::node::chunk::{ChunkStore, StagedChunk};

        let temp = tempfile::tempdir().unwrap();
        let local =
            Arc::new(crate::node::chunk::LocalChunkStore::open(temp.path(), "node").unwrap());
        let chunk = StagedChunk::new(crate::dfs::OperationId::new("prepared-binding"), vec![8, 9]);
        local.put(chunk.clone()).unwrap();
        let service = service(local);
        let prepared = service
            .prepare_read_ranges(Request::new(read_request(chunk.chunk.id.0, 2)))
            .await
            .unwrap()
            .into_inner();
        let fetch = afs_protocol::node_data::DfsFetchPreparedReadRequest {
            prepared_read_id: prepared.prepared_read_id,
            read_id: "prepared-read".into(),
            attempt_id: "prepared-attempt".into(),
        };

        for (read_id, attempt_id) in [
            ("another-read", "prepared-attempt"),
            ("prepared-read", "another-attempt"),
        ] {
            let mut wrong_identity = fetch.clone();
            wrong_identity.read_id = read_id.into();
            wrong_identity.attempt_id = attempt_id.into();
            assert_eq!(
                error_code(
                    service
                        .fetch_prepared_read(Request::new(wrong_identity))
                        .await
                ),
                tonic::Code::PermissionDenied
            );
        }

        let mut wrong_peer = service.clone();
        wrong_peer.authenticator = Arc::new(FixturePeer("other-reader"));
        assert_eq!(
            error_code(
                wrong_peer
                    .fetch_prepared_read(Request::new(fetch.clone()))
                    .await
            ),
            tonic::Code::PermissionDenied
        );

        assert!(
            service
                .fetch_prepared_read(Request::new(fetch))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn prepared_read_expiry_reclaims_payload_without_another_request() {
        use crate::node::chunk::{ChunkStore, StagedChunk};

        let temp = tempfile::tempdir().unwrap();
        let local =
            Arc::new(crate::node::chunk::LocalChunkStore::open(temp.path(), "node").unwrap());
        let chunk = StagedChunk::new(crate::dfs::OperationId::new("prepared-expiry"), vec![3; 32]);
        local.put(chunk.clone()).unwrap();
        let service = service(local);
        let mut request = read_request(chunk.chunk.id.0, 32);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        request.operations[0]
            .grant
            .as_mut()
            .unwrap()
            .expires_at_unix_ms = now + 500;

        service
            .prepare_read_ranges(Request::new(request))
            .await
            .unwrap();
        assert_eq!(service.prepared_reads.retained_usage(), (1, 32));
        tokio::time::sleep(std::time::Duration::from_millis(650)).await;
        assert_eq!(service.prepared_reads.retained_usage(), (0, 0));
    }

    struct PrepareGate {
        entered: tokio::sync::Notify,
        open: Mutex<bool>,
        changed: Condvar,
    }

    impl PrepareGate {
        fn wait(&self) {
            let mut open = self.open.lock().unwrap();
            while !*open {
                open = self.changed.wait(open).unwrap();
            }
        }

        fn release(&self) {
            *self.open.lock().unwrap() = true;
            self.changed.notify_all();
        }
    }

    struct BlockingReadAuthority(Arc<PrepareGate>);

    impl DfsReadAuthorizer for BlockingReadAuthority {
        fn authorize(&self, peer: &str, request: &DfsReadRangesRequest) -> afs_error::Result<()> {
            self.0.entered.notify_one();
            self.0.wait();
            FixtureReadAuthority.authorize(peer, request)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_prepare_does_not_publish_or_leak_its_reservation() {
        use crate::node::chunk::{ChunkStore, StagedChunk};

        let temp = tempfile::tempdir().unwrap();
        let local =
            Arc::new(crate::node::chunk::LocalChunkStore::open(temp.path(), "node").unwrap());
        let chunk = StagedChunk::new(crate::dfs::OperationId::new("prepared-cancel"), vec![7; 64]);
        local.put(chunk.clone()).unwrap();
        let mut service = service(local);
        let gate = Arc::new(PrepareGate {
            entered: tokio::sync::Notify::new(),
            open: Mutex::new(false),
            changed: Condvar::new(),
        });
        service.authorizer = Arc::new(BlockingReadAuthority(gate.clone()));
        let request = read_request(chunk.chunk.id.0, 64);
        let observer = service.clone();
        let pending =
            tokio::spawn(async move { service.prepare_read_ranges(Request::new(request)).await });

        tokio::time::timeout(std::time::Duration::from_secs(2), gate.entered.notified())
            .await
            .expect("prepare should reach the blocking authorizer");
        pending.abort();
        gate.release();
        let _ = pending.await;
        wait_for_no_retained_payload(&observer).await;
    }

    #[tokio::test]
    async fn dropping_fetch_stream_releases_in_flight_payload() {
        use crate::node::chunk::{ChunkStore, StagedChunk};

        let temp = tempfile::tempdir().unwrap();
        let local =
            Arc::new(crate::node::chunk::LocalChunkStore::open(temp.path(), "node").unwrap());
        let chunk = StagedChunk::new(
            crate::dfs::OperationId::new("prepared-stream-drop"),
            vec![5; 1024 * 1024],
        );
        local.put(chunk.clone()).unwrap();
        let service = service(local);
        let reply = service
            .prepare_read_ranges(Request::new(read_request(chunk.chunk.id.0, 1024 * 1024)))
            .await
            .unwrap()
            .into_inner();
        let response = service
            .fetch_prepared_read(Request::new(
                afs_protocol::node_data::DfsFetchPreparedReadRequest {
                    prepared_read_id: reply.prepared_read_id,
                    read_id: reply.read_id,
                    attempt_id: reply.attempt_id,
                },
            ))
            .await
            .unwrap();
        drop(response);
        wait_for_no_retained_payload(&service).await;
    }

    #[tokio::test]
    async fn in_flight_stream_expiry_releases_payload_without_receiver_progress() {
        use crate::node::chunk::{ChunkStore, StagedChunk};

        let temp = tempfile::tempdir().unwrap();
        let local =
            Arc::new(crate::node::chunk::LocalChunkStore::open(temp.path(), "node").unwrap());
        let chunk = StagedChunk::new(
            crate::dfs::OperationId::new("prepared-stream-expiry"),
            vec![6; 1024 * 1024],
        );
        local.put(chunk.clone()).unwrap();
        let service = service(local);
        let mut request = read_request(chunk.chunk.id.0, 1024 * 1024);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        request.operations[0]
            .grant
            .as_mut()
            .unwrap()
            .expires_at_unix_ms = now + 500;
        let reply = service
            .prepare_read_ranges(Request::new(request))
            .await
            .unwrap()
            .into_inner();
        let response = service
            .fetch_prepared_read(Request::new(
                afs_protocol::node_data::DfsFetchPreparedReadRequest {
                    prepared_read_id: reply.prepared_read_id,
                    read_id: reply.read_id,
                    attempt_id: reply.attempt_id,
                },
            ))
            .await
            .unwrap();
        let stream = response.into_inner();

        tokio::time::sleep(std::time::Duration::from_millis(650)).await;
        assert_eq!(service.prepared_reads.retained_usage(), (0, 0));
        drop(stream);
    }

    #[tokio::test]
    async fn release_is_peer_bound_and_idempotent() {
        use crate::node::chunk::{ChunkStore, StagedChunk};

        let temp = tempfile::tempdir().unwrap();
        let local =
            Arc::new(crate::node::chunk::LocalChunkStore::open(temp.path(), "node").unwrap());
        let chunk = StagedChunk::new(crate::dfs::OperationId::new("prepared-release"), vec![1, 2]);
        local.put(chunk.clone()).unwrap();
        let service = service(local);
        let reply = service
            .prepare_read_ranges(Request::new(read_request(chunk.chunk.id.0, 2)))
            .await
            .unwrap()
            .into_inner();
        let release = afs_protocol::node_data::DfsReleasePreparedReadRequest {
            prepared_read_id: reply.prepared_read_id,
            read_id: reply.read_id,
            attempt_id: reply.attempt_id,
        };

        let mut wrong_peer = service.clone();
        wrong_peer.authenticator = Arc::new(FixturePeer("other-reader"));
        assert_eq!(
            error_code(
                wrong_peer
                    .release_prepared_read(Request::new(release.clone()))
                    .await
            ),
            tonic::Code::PermissionDenied
        );

        let released = service
            .release_prepared_read(Request::new(release.clone()))
            .await
            .unwrap()
            .into_inner();
        assert!(released.released);
        let repeated = service
            .release_prepared_read(Request::new(release))
            .await
            .unwrap()
            .into_inner();
        assert!(!repeated.released);
        assert_eq!(service.prepared_reads.retained_usage(), (0, 0));
    }

    #[tokio::test]
    async fn prepared_snapshots_enforce_the_aggregate_host_budget() {
        use crate::node::chunk::{ChunkStore, StagedChunk};

        let temp = tempfile::tempdir().unwrap();
        let local =
            Arc::new(crate::node::chunk::LocalChunkStore::open(temp.path(), "node").unwrap());
        let chunk = StagedChunk::new(
            crate::dfs::OperationId::new("prepared-budget"),
            vec![1; 8 * 1024 * 1024],
        );
        local.put(chunk.clone()).unwrap();
        let service = service(local);
        let mut prepared = Vec::new();
        for index in 0..8 {
            let mut request = read_request(chunk.chunk.id.0.clone(), 8 * 1024 * 1024);
            request.read_id = format!("prepared-read-{index}");
            request.attempt_id = format!("prepared-attempt-{index}");
            prepared.push(
                service
                    .prepare_read_ranges(Request::new(request))
                    .await
                    .unwrap()
                    .into_inner(),
            );
        }
        assert_eq!(
            service.prepared_reads.retained_usage(),
            (8, 64 * 1024 * 1024)
        );

        let mut over_budget = read_request(chunk.chunk.id.0, 1);
        over_budget.read_id = "over-budget-read".into();
        over_budget.attempt_id = "over-budget-attempt".into();
        assert_eq!(
            error_code(service.prepare_read_ranges(Request::new(over_budget)).await),
            tonic::Code::ResourceExhausted
        );

        for reply in prepared {
            service
                .release_prepared_read(Request::new(
                    afs_protocol::node_data::DfsReleasePreparedReadRequest {
                        prepared_read_id: reply.prepared_read_id,
                        read_id: reply.read_id,
                        attempt_id: reply.attempt_id,
                    },
                ))
                .await
                .unwrap();
        }
        assert_eq!(service.prepared_reads.retained_usage(), (0, 0));
    }
}
