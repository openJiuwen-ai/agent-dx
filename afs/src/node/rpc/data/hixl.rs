//! Authenticated DFS source-window control for the optional HIXL rH2H path.
//!
//! This module owns only the control-plane lease. HixlRuntime owns the
//! fixed ACL Host memory and the native request state; the registry below
//! keeps the AFS grant/session/window identity that makes a runtime export
//! eligible for a remote READ. In particular, closing a session only stops
//! admission. It never claims that an accepted native request has drained.

use std::cell::Cell;
use std::collections::{HashMap, HashSet, VecDeque, hash_map::Entry};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use afs_protocol::node_data::{
    DfsCloseHixlReply, DfsCloseHixlRequest, DfsDigestAlgorithm, DfsHixlLane,
    DfsHixlPreparedGroupWindow, DfsHixlReadRange, DfsHixlReleaseOutcome, DfsHixlReleaseWindow,
    DfsHixlReleaseWindowResult, DfsHixlTransport, DfsNegotiateHixlReply, DfsNegotiateHixlRequest,
    DfsPrepareReadRangesHixlGroupReply, DfsPrepareReadRangesHixlGroupRequest,
    DfsPrepareReadRangesHixlReply, DfsPrepareReadRangesHixlRequest, DfsReadRangesCompletion,
    DfsReadRangesRequest, DfsReleaseHixlReadReply, DfsReleaseHixlReadRequest,
};
use afs_transport::grpc::error_status::{coded_status, error_to_status};
use afs_transport::hixl::{EngineInfo, ExportWindow, HixlError, HixlRuntimeGroup, ReadTransport};
use tonic::{Response, Status};

use super::snapshot::{PackedRangeDigest, read_hixl_snapshot_ranges, validate_packed_dfs_read};
use super::{DfsReadAuthorizer, dfs_read_caller_epoch, validate_dfs_read_request};
use crate::node::chunk::LocalChunkStore;
use crate::node::dfs_read::{MAX_DFS_GROUP_BYTES, MAX_DFS_READ_BYTES};
use crate::node::rpc::hixl::batch::{
    pack_read_batch, pack_read_group, slice_read_batch, slice_read_group,
};
use crate::node::rpc::hixl::{HIXL_PROTOCOL_VERSION, HixlMetrics, Operation};

const MAX_HIXL_SESSIONS: usize = 64;
const MAX_HIXL_WINDOWS: usize = 64;
const MAX_HIXL_RELEASE_TOMBSTONES: usize = 256;
const HIXL_SESSION_TTL: Duration = Duration::from_secs(30);
// Private verified snapshots are separate from native Host exports. Limit their
// aggregate size before reading any source data; each legacy DFS batch is at
// most 8 MiB, while one explicitly grouped batch may reach the group budget.
const MAX_HIXL_PREPARED_BATCH_BYTES: usize = 64 * 1024 * 1024;
const MAX_HIXL_GROUP_BATCH_BYTES: usize = MAX_DFS_GROUP_BYTES as usize;
const MAX_HIXL_LEGACY_BATCH_BYTES: usize = MAX_DFS_READ_BYTES as usize;

fn snapshot_budget(group_batch: bool) -> usize {
    if group_batch {
        MAX_HIXL_GROUP_BATCH_BYTES
    } else {
        MAX_HIXL_PREPARED_BATCH_BYTES
    }
}

/// A bounded source-window registry and its native export provider.
#[derive(Clone)]
pub struct HixlReadService {
    provider: Arc<dyn ExportProvider>,
    node_id: String,
    node_epoch: u64,
    registry: Arc<Mutex<Registry>>,
    metrics: Option<HixlMetrics>,
}

impl HixlReadService {
    /// Creates the Node-side source-window service for one native HIXL group.
    pub fn new(runtime: Arc<HixlRuntimeGroup>, node_id: String, node_epoch: u64) -> Self {
        Self::from_provider(
            Arc::new(RuntimeExportProvider { runtime }),
            node_id,
            node_epoch,
        )
    }

    fn from_provider(provider: Arc<dyn ExportProvider>, node_id: String, node_epoch: u64) -> Self {
        Self {
            provider,
            node_id,
            node_epoch,
            registry: Arc::new(Mutex::new(Registry::default())),
            metrics: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        provider: Arc<dyn ExportProvider>,
        node_id: String,
        node_epoch: u64,
    ) -> Self {
        Self::from_provider(provider, node_id, node_epoch)
    }

    pub fn with_metrics(mut self, metrics: HixlMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    fn observe(&self, phase: Operation, started: Instant, success: bool) {
        if let Some(metrics) = &self.metrics {
            metrics.observe(phase, started.elapsed(), success);
        }
    }

    /// Stops admission for expired sessions and quarantines their pending windows.
    /// It never recycles an export on the strength of elapsed time.
    pub async fn expire(&self) -> Result<(), Status> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut registry = service
                .registry
                .lock()
                .map_err(|_| Status::internal("HIXL registry lock is poisoned"))?;
            registry.expire_sessions();
            service.quarantine_pending_windows(&mut registry, false)
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?
    }

    /// Stops new window admission while allowing authenticated releases to
    /// drain within a bounded grace period. Timeout retains pending resources.
    pub async fn drain(&self, timeout: Duration) -> Result<(), Status> {
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "HIXL drain deadline overflow",
            )
        })?;
        loop {
            let service = self.clone();
            let pending = tokio::task::spawn_blocking(move || {
                let mut registry = service
                    .registry
                    .lock()
                    .map_err(|_| Status::internal("HIXL registry lock is poisoned"))?;
                registry.draining = true;
                for session in registry.sessions.values_mut() {
                    session.prepared_batch = None;
                }
                let pending = registry.active_window_count();
                if pending != 0 && Instant::now() >= deadline {
                    service.quarantine_pending_windows(&mut registry, true)?;
                    return Err(coded_status(
                        afs_error::NODE_TRANSFER_DRAIN_INCOMPLETE,
                        "HIXL source drain deadline exceeded; pending windows retained",
                    ));
                }
                Ok(pending)
            })
            .await
            .map_err(|error| Status::internal(error.to_string()))??;
            if pending == 0 {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Performs authenticated HIXL negotiation and allocates only a control
    /// session. No source bytes or raw address are exposed by this method.
    pub async fn negotiate(
        &self,
        peer: String,
        request: DfsNegotiateHixlRequest,
        authorizer: Arc<dyn DfsReadAuthorizer>,
    ) -> Result<Response<DfsNegotiateHixlReply>, Status> {
        let service = self.clone();
        let started = Instant::now();
        let result = tokio::task::spawn_blocking(move || {
            service.negotiate_blocking(&peer, request, authorizer)
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?;
        self.observe(Operation::Negotiate, started, result.is_ok());
        result
    }

    /// Revalidates every grant, reads verified ranges, and pins one export
    /// window. Replay of the same read/attempt returns the existing window.
    pub async fn prepare(
        &self,
        peer: String,
        request: DfsPrepareReadRangesHixlRequest,
        authorizer: Arc<dyn DfsReadAuthorizer>,
        local: Option<Arc<LocalChunkStore>>,
    ) -> Result<Response<DfsPrepareReadRangesHixlReply>, Status> {
        let service = self.clone();
        let started = Instant::now();
        let result = tokio::task::spawn_blocking(move || {
            service.prepare_blocking(&peer, request, authorizer, local)
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?;
        self.observe(Operation::Prepare, started, result.is_ok());
        result
    }

    /// Prepares every ordered window of one logical group in a single control
    /// round trip. The source verifies the parent batch once, pins one private
    /// snapshot, and exports each window from that snapshot. Either all windows
    /// are admitted or previously pinned exports are rolled back.
    pub async fn prepare_group(
        &self,
        peer: String,
        request: DfsPrepareReadRangesHixlGroupRequest,
        authorizer: Arc<dyn DfsReadAuthorizer>,
        local: Option<Arc<LocalChunkStore>>,
    ) -> Result<Response<DfsPrepareReadRangesHixlGroupReply>, Status> {
        let service = self.clone();
        let started = Instant::now();
        let result = tokio::task::spawn_blocking(move || {
            service.prepare_group_blocking(&peer, request, authorizer, local)
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?;
        self.observe(Operation::Prepare, started, result.is_ok());
        result
    }

    /// Releases one known terminal window. A duplicate release is answered by
    /// the bounded tombstone table. Unknown native outcomes remain pinned.
    pub async fn release(
        &self,
        peer: String,
        request: DfsReleaseHixlReadRequest,
    ) -> Result<Response<DfsReleaseHixlReadReply>, Status> {
        let service = self.clone();
        let started = Instant::now();
        let result = tokio::task::spawn_blocking(move || service.release_blocking(&peer, request))
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        self.observe(Operation::Release, started, result.is_ok());
        result
    }

    /// Closes admission for a session. It deliberately does not drain or
    /// release still-live export windows.
    pub async fn close(
        &self,
        peer: String,
        request: DfsCloseHixlRequest,
    ) -> Result<Response<DfsCloseHixlReply>, Status> {
        let service = self.clone();
        let started = Instant::now();
        let result = tokio::task::spawn_blocking(move || service.close_blocking(&peer, request))
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        self.observe(Operation::Close, started, result.is_ok());
        result
    }

    fn negotiate_blocking(
        &self,
        peer: &str,
        request: DfsNegotiateHixlRequest,
        authorizer: Arc<dyn DfsReadAuthorizer>,
    ) -> Result<Response<DfsNegotiateHixlReply>, Status> {
        // A pre-warm negotiation carries no read authority: it only builds
        // the transport session, and every window prepare still authorizes
        // its own grant. Node identity stays bound to the epochs either way.
        let read = request.read_request.as_ref();
        if let Some(read) = read {
            validate_hixl_identity(
                peer,
                self.node_epoch,
                request.caller_node_epoch,
                request.source_node_epoch,
                read,
            )?;
        } else if request.caller_node_epoch == 0 || request.source_node_epoch != self.node_epoch {
            return Err(Status::permission_denied(
                "HIXL pre-warm caller or source epoch is stale",
            ));
        }
        if request.protocol_version != HIXL_PROTOCOL_VERSION {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_UNSUPPORTED,
                "HIXL protocol version is unsupported",
            ));
        }
        if request.client_engine_id.is_empty()
            || request.client_engine_id.len() > 512
            || request.client_engine_generation == 0
        {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "HIXL client engine identity is invalid",
            ));
        }
        if let Some(read) = read {
            self.authorize(authorizer.as_ref(), peer, read)?;
        }

        let info = self.provider.info();
        let lane_infos = self.provider.lane_infos();
        if lane_infos.is_empty()
            || lane_infos.len() > 16
            || usize::try_from(request.lane_count).ok() != Some(lane_infos.len())
        {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "HIXL source and reader lane counts must match",
            ));
        }
        let (transport, buffer_pool_count, buffer_pool_size_mib, hccs_domain_id) =
            negotiated_transport(&info)?;
        let mut lanes = Vec::with_capacity(lane_infos.len());
        for (index, lane) in lane_infos.iter().enumerate() {
            if negotiated_transport(lane)?
                != (
                    transport,
                    buffer_pool_count,
                    buffer_pool_size_mib,
                    hccs_domain_id,
                )
                || lane.window_bytes != info.window_bytes
                || lane.generation == 0
                || lane.engine_id.is_empty()
            {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "HIXL lanes have inconsistent transport or identity",
                ));
            }
            lanes.push(DfsHixlLane {
                lane_id: u32::try_from(index)
                    .map_err(|_| Status::internal("HIXL lane index overflow"))?,
                engine_id: lane.engine_id.clone(),
                engine_generation: lane.generation,
                window_bytes: u64::try_from(lane.window_bytes)
                    .map_err(|_| Status::internal("HIXL lane window overflow"))?,
                export_slots: effective_export_slots(self.provider.export_slots(index))?,
            });
        }
        validate_transport_request(
            &request,
            transport,
            buffer_pool_count,
            buffer_pool_size_mib,
            hccs_domain_id,
        )?;
        let export_slots = effective_export_slots(self.provider.export_slots(0))?;
        let window_bytes = u64::try_from(info.window_bytes).map_err(|_| {
            coded_status(
                afs_error::NODE_TRANSFER_INTERNAL,
                "HIXL window size exceeds protocol range",
            )
        })?;
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Status::internal("HIXL registry lock is poisoned"))?;
        registry.expire_sessions();
        self.quarantine_pending_windows(&mut registry, false)?;
        if registry.admission_frozen || registry.draining {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "HIXL engine generation is quarantined",
            ));
        }
        if registry.sessions.len() >= MAX_HIXL_SESSIONS {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
                "HIXL session capacity is exhausted",
            ));
        }
        let session_id = registry.next_session_id()?;
        registry.sessions.insert(
            session_id,
            Session {
                peer: peer.to_owned(),
                caller_epoch: request.caller_node_epoch,
                source_epoch: request.source_node_epoch,
                engine_id: info.engine_id.clone(),
                engine_generation: info.generation,
                lanes: lane_infos,
                closed: false,
                last_used: Instant::now(),
                windows: HashMap::new(),
                tombstones: Tombstones::default(),
                prepared_batch: None,
            },
        );
        Ok(Response::new(DfsNegotiateHixlReply {
            session_id,
            hixl_supported: true,
            source_node_id: self.node_id.clone(),
            source_node_epoch: self.node_epoch,
            engine_id: info.engine_id,
            engine_generation: info.generation,
            window_bytes,
            export_slots,
            protocol_version: HIXL_PROTOCOL_VERSION,
            transport: transport as i32,
            buffer_pool_count,
            buffer_pool_size_mib,
            hccs_domain_id,
            lanes,
        }))
    }

    fn prepare_blocking(
        &self,
        peer: &str,
        request: DfsPrepareReadRangesHixlRequest,
        authorizer: Arc<dyn DfsReadAuthorizer>,
        local: Option<Arc<LocalChunkStore>>,
    ) -> Result<Response<DfsPrepareReadRangesHixlReply>, Status> {
        let read = request.read_request.clone().ok_or_else(|| {
            coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "HIXL read request is absent",
            )
        })?;
        let batch = request.batch_read_request.clone().ok_or_else(|| {
            coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "HIXL logical read batch is absent",
            )
        })?;
        let caller_epoch = dfs_read_caller_epoch(peer, &batch)?;
        if request.caller_node_epoch != caller_epoch {
            return Err(Status::permission_denied(
                "HIXL caller epoch does not match the read grant",
            ));
        }
        if request.source_node_epoch != self.node_epoch {
            return Err(Status::permission_denied(
                "HIXL source epoch does not match this node",
            ));
        }
        self.authorize(authorizer.as_ref(), peer, &batch)?;
        let length = validate_packed_dfs_read(&read)?;
        // A batch larger than the legacy DFS budget is an explicit ReadGroup
        // aggregate; it keeps a separate snapshot budget and operation limit.
        let group_batch = batch
            .operations
            .iter()
            .try_fold(0_u64, |sum, operation| sum.checked_add(operation.length))
            .is_some_and(|total| total > MAX_DFS_READ_BYTES);
        let packed_batch = if group_batch {
            pack_read_group(batch.clone())
        } else {
            pack_read_batch(batch.clone())
        }
        .map_err(error_to_status)?;
        if packed_batch != batch {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "HIXL batch destinations must be tightly packed",
            ));
        }
        let expected = if group_batch {
            slice_read_group(
                &batch,
                request.batch_offset,
                u64::try_from(length)
                    .map_err(|_| Status::invalid_argument("HIXL window length overflow"))?,
                &read.attempt_id,
            )
        } else {
            slice_read_batch(
                &batch,
                request.batch_offset,
                u64::try_from(length)
                    .map_err(|_| Status::invalid_argument("HIXL window length overflow"))?,
                &read.attempt_id,
            )
        }
        .map_err(error_to_status)?;
        let lane_id = usize::try_from(request.lane_id)
            .map_err(|_| Status::invalid_argument("HIXL lane index overflow"))?;
        let lane_info = self
            .provider
            .lane_infos()
            .get(lane_id)
            .cloned()
            .ok_or_else(|| {
                coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "HIXL lane is not configured",
                )
            })?;
        if expected != read || length > lane_info.window_bytes {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "HIXL window differs from its logical batch or native capacity",
            ));
        }
        let batch_offset = usize::try_from(request.batch_offset)
            .map_err(|_| Status::invalid_argument("HIXL batch offset overflow"))?;
        let batch_length = batch
            .operations
            .iter()
            .try_fold(0_usize, |sum, operation| {
                usize::try_from(operation.length)
                    .ok()
                    .and_then(|length| sum.checked_add(length))
                    .ok_or_else(|| Status::invalid_argument("HIXL logical batch length overflow"))
            })?;
        let local = local.ok_or_else(|| {
            coded_status(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "DFS local ChunkStore is not available for HIXL",
            )
        })?;
        let export_slots =
            usize::try_from(effective_export_slots(self.provider.export_slots(lane_id))?)
                .map_err(|_| Status::internal("HIXL export slot count overflow"))?;

        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Status::internal("HIXL registry lock is poisoned"))?;
        registry.expire_sessions();
        self.quarantine_pending_windows(&mut registry, false)?;
        let at_capacity = registry.lane_window_count(lane_id) >= export_slots;
        let prepared_bytes = registry.prepared_batch_bytes();
        let admission_frozen = registry.admission_frozen;
        let draining = registry.draining;
        let session = registry.session_mut(
            request.session_id,
            peer,
            caller_epoch,
            request.source_node_epoch,
        )?;
        let key = WindowKey::from_read(&read);
        if let Some(window) = session.windows.get(&key) {
            if window.request != read
                || window.batch_request.as_ref() != &batch
                || window.batch_offset != request.batch_offset
                || window.lane_id != lane_id
            {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "HIXL prepare replay identity does not match the original request",
                ));
            }
            session.last_used = Instant::now();
            return Ok(Response::new(window.reply.clone()));
        }
        if session.tombstones.contains(&key) {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "HIXL read window was already released",
            ));
        }
        if session.closed {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "HIXL session is closed for new windows",
            ));
        }
        if admission_frozen || draining {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "HIXL engine generation is quarantined",
            ));
        }
        // Authority and session state were checked above. A definite capacity
        // refusal is still authenticated control-session activity.
        session.last_used = Instant::now();
        if at_capacity {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
                "HIXL export-window capacity is exhausted",
            ));
        }

        if let Some(prepared) = &session.prepared_batch {
            if prepared.request.as_ref() != &batch || prepared.next_offset != batch_offset {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "HIXL batch identity changed or window is out of order",
                ));
            }
        } else {
            if batch_offset != 0 {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "HIXL logical batch must start at offset zero",
                ));
            }
            if batch_length > snapshot_budget(group_batch).saturating_sub(prepared_bytes) {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
                    "HIXL verified batch snapshot budget is exhausted",
                ));
            }
            let started = Instant::now();
            // Both budgets bound the snapshot before any source byte is read,
            // so CRC32C ranges fill it directly like BLAKE3 ranges do.
            let packed = read_hixl_snapshot_ranges(
                &local,
                &batch,
                batch_length,
                if group_batch {
                    MAX_HIXL_GROUP_BATCH_BYTES
                } else {
                    MAX_HIXL_LEGACY_BATCH_BYTES
                },
            );
            self.observe(Operation::SourceRead, started, packed.is_ok());
            let (bytes, completions) = packed?;
            let range_digests = batch_range_checksums(&batch, &completions);
            session.prepared_batch = Some(PreparedBatch {
                request: Arc::new(batch),
                bytes: Arc::new(bytes),
                next_offset: 0,
                range_digests,
            });
        }
        let prepared = session
            .prepared_batch
            .as_ref()
            .ok_or_else(|| Status::internal("HIXL verified batch snapshot was lost"))?;
        let end = batch_offset
            .checked_add(length)
            .ok_or_else(|| Status::invalid_argument("HIXL batch window end overflow"))?;
        let bytes = prepared
            .bytes
            .get(batch_offset..end)
            .ok_or_else(|| Status::invalid_argument("HIXL window exceeds the verified batch"))?
            .to_vec();
        let batch_request = prepared.request.clone();
        let completions = window_completions(
            &read,
            &bytes,
            &prepared.range_digests,
            &local,
            self.metrics.as_ref(),
        )?;
        let export = self
            .provider
            .prepare_export(lane_id, bytes, Instant::now() + HIXL_SESSION_TTL)
            .map_err(runtime_status)?;
        let rollback_failed = Cell::new(false);
        let outcome = (|| -> Result<Response<DfsPrepareReadRangesHixlReply>, Status> {
            let export =
                PendingExport::new(self.provider.as_ref(), lane_id, export, &rollback_failed);
            let info = self
                .provider
                .lane_infos()
                .get(lane_id)
                .cloned()
                .ok_or_else(|| Status::internal("HIXL lane disappeared during prepare"))?;
            let capacity = u64::try_from(export.window.length).map_err(|_| {
                coded_status(
                    afs_error::NODE_TRANSFER_INTERNAL,
                    "HIXL export capacity exceeds protocol range",
                )
            })?;
            let packed_length = u64::try_from(length).map_err(|_| {
                coded_status(
                    afs_error::NODE_TRANSFER_INTERNAL,
                    "HIXL packed length exceeds protocol range",
                )
            })?;
            if export.window.address == 0
                || export.window.length == 0
                || export.window.slot_id >= export_slots as u64
                || export.window.generation == 0
                || packed_length > capacity
                || export.window.address.checked_add(packed_length).is_none()
            {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_INTERNAL,
                    "HIXL runtime returned an invalid export descriptor",
                ));
            }
            if session.lanes.get(lane_id) != Some(&info) {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_UNAVAILABLE,
                    "HIXL engine generation changed during session",
                ));
            }
            let ranges = read
                .operations
                .iter()
                .zip(&completions)
                .map(|(operation, completion)| DfsHixlReadRange {
                    operation_index: operation.operation_index,
                    chunk_id: operation.chunk_id.clone(),
                    chunk_offset: operation.chunk_offset,
                    length: operation.length,
                    packed_offset: operation.destination_offset,
                    source_copy_id: operation.source_copy_id.clone(),
                    range_checksum: completion.range_checksum.clone(),
                    range_checksum_algorithm: completion.range_checksum_algorithm,
                    crc32c_table: completion.crc32c_table.clone(),
                })
                .collect::<Vec<_>>();
            let reply = DfsPrepareReadRangesHixlReply {
                session_id: request.session_id,
                read_id: read.read_id.clone(),
                attempt_id: read.attempt_id.clone(),
                source_node_id: self.node_id.clone(),
                source_node_epoch: self.node_epoch,
                engine_id: info.engine_id,
                engine_generation: info.generation,
                pool_generation: info.generation,
                slot_id: export.window.slot_id,
                slot_generation: export.window.generation,
                address: export.window.address,
                capacity,
                packed_length,
                ranges,
                completions,
            };
            session.last_used = Instant::now();
            session
                .prepared_batch
                .as_mut()
                .ok_or_else(|| Status::internal("HIXL verified batch snapshot was lost"))?
                .next_offset = end;
            session.windows.insert(
                key,
                WindowRecord {
                    request: read,
                    reply: reply.clone(),
                    batch_request,
                    batch_offset: request.batch_offset,
                    lane_id,
                },
            );
            export.disarm();
            Ok(Response::new(reply))
        })();
        match outcome {
            Ok(response) => Ok(response),
            Err(status) => {
                if rollback_failed.get() {
                    self.quarantine_pending_windows(&mut registry, true)?;
                }
                Err(status)
            }
        }
    }

    fn prepare_group_blocking(
        &self,
        peer: &str,
        request: DfsPrepareReadRangesHixlGroupRequest,
        authorizer: Arc<dyn DfsReadAuthorizer>,
        local: Option<Arc<LocalChunkStore>>,
    ) -> Result<Response<DfsPrepareReadRangesHixlGroupReply>, Status> {
        let batch = request.batch_read_request.clone().ok_or_else(|| {
            coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "HIXL logical read batch is absent",
            )
        })?;
        if request.windows.is_empty() || request.windows.len() > MAX_HIXL_WINDOWS {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "HIXL group window count is invalid",
            ));
        }
        let caller_epoch = dfs_read_caller_epoch(peer, &batch)?;
        if request.caller_node_epoch != caller_epoch {
            return Err(Status::permission_denied(
                "HIXL caller epoch does not match the read grant",
            ));
        }
        if request.source_node_epoch != self.node_epoch {
            return Err(Status::permission_denied(
                "HIXL source epoch does not match this node",
            ));
        }
        self.authorize(authorizer.as_ref(), peer, &batch)?;
        let group_batch = batch
            .operations
            .iter()
            .try_fold(0_u64, |sum, operation| sum.checked_add(operation.length))
            .is_some_and(|total| total > MAX_DFS_READ_BYTES);
        let packed_batch = if group_batch {
            pack_read_group(batch.clone())
        } else {
            pack_read_batch(batch.clone())
        }
        .map_err(error_to_status)?;
        if packed_batch != batch {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "HIXL batch destinations must be tightly packed",
            ));
        }
        let batch_length = batch
            .operations
            .iter()
            .try_fold(0_usize, |sum, operation| {
                usize::try_from(operation.length)
                    .ok()
                    .and_then(|length| sum.checked_add(length))
                    .ok_or_else(|| Status::invalid_argument("HIXL logical batch length overflow"))
            })?;
        let first_lane = request.windows[0].lane_id;
        let lane_id = usize::try_from(first_lane)
            .map_err(|_| Status::invalid_argument("HIXL lane index overflow"))?;
        let lane_info = self
            .provider
            .lane_infos()
            .get(lane_id)
            .cloned()
            .ok_or_else(|| {
                coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "HIXL lane is not configured",
                )
            })?;
        let export_slots =
            usize::try_from(effective_export_slots(self.provider.export_slots(lane_id))?)
                .map_err(|_| Status::internal("HIXL export slot count overflow"))?;
        if request.windows.len() > export_slots {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
                "HIXL group exceeds export-window capacity",
            ));
        }
        let mut expected = request.windows[0].batch_offset;
        let mut plans = Vec::with_capacity(request.windows.len());
        let mut window_keys = HashSet::with_capacity(request.windows.len());
        for window in &request.windows {
            if window.lane_id != first_lane {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "HIXL group spans more than one lane",
                ));
            }
            if window.batch_offset != expected {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "HIXL group windows must be contiguous and ordered",
                ));
            }
            let read = window.read_request.clone().ok_or_else(|| {
                coded_status(afs_error::NODE_TRANSFER_INVALID, "HIXL window is absent")
            })?;
            if !window_keys.insert(WindowKey::from_read(&read)) {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "HIXL group contains duplicate window keys",
                ));
            }
            let length = validate_packed_dfs_read(&read)?;
            let length_u64 = u64::try_from(length)
                .map_err(|_| Status::invalid_argument("HIXL window length overflow"))?;
            let sliced = if group_batch {
                slice_read_group(&batch, expected, length_u64, &read.attempt_id)
            } else {
                slice_read_batch(&batch, expected, length_u64, &read.attempt_id)
            }
            .map_err(error_to_status)?;
            if sliced != read || length > lane_info.window_bytes {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "HIXL window differs from its logical batch or native capacity",
                ));
            }
            let end = expected
                .checked_add(length_u64)
                .ok_or_else(|| Status::invalid_argument("HIXL batch window end overflow"))?;
            plans.push(GroupWindowPlan {
                read,
                length,
                offset: expected,
                end,
            });
            expected = end;
        }
        if usize::try_from(expected)
            .ok()
            .is_none_or(|end| end > batch_length)
        {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "HIXL group windows must stay within the logical batch",
            ));
        }
        let local = local.ok_or_else(|| {
            coded_status(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "DFS local ChunkStore is not available for HIXL",
            )
        })?;
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Status::internal("HIXL registry lock is poisoned"))?;
        registry.expire_sessions();
        self.quarantine_pending_windows(&mut registry, false)?;
        let prepared_bytes = registry.prepared_batch_bytes();
        let admission_frozen = registry.admission_frozen || registry.draining;
        let at_capacity = registry.lane_window_count(lane_id) + plans.len() > export_slots;
        let outcome = self.prepare_group_session(
            &mut registry,
            request.session_id,
            peer,
            caller_epoch,
            request.source_node_epoch,
            &batch,
            batch_length,
            group_batch,
            &local,
            lane_id,
            first_lane,
            export_slots,
            &plans,
            prepared_bytes,
            admission_frozen,
            at_capacity,
        );
        match outcome {
            Ok(windows) => Ok(Response::new(DfsPrepareReadRangesHixlGroupReply {
                session_id: request.session_id,
                windows,
            })),
            Err((status, rollback_failed)) => {
                if rollback_failed {
                    self.quarantine_pending_windows(&mut registry, true)?;
                }
                Err(status)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_group_session(
        &self,
        registry: &mut Registry,
        session_id: u64,
        peer: &str,
        caller_epoch: u64,
        source_epoch: u64,
        batch: &DfsReadRangesRequest,
        batch_length: usize,
        group_batch: bool,
        local: &LocalChunkStore,
        lane_id: usize,
        lane_wire_id: u32,
        export_slots: usize,
        windows: &[GroupWindowPlan],
        prepared_bytes: usize,
        admission_frozen: bool,
        at_capacity: bool,
    ) -> Result<Vec<DfsHixlPreparedGroupWindow>, (Status, bool)> {
        let session = registry
            .session_mut(session_id, peer, caller_epoch, source_epoch)
            .map_err(|status| (status, false))?;
        session.last_used = Instant::now();
        let mut replay = Vec::with_capacity(windows.len());
        let mut replayed = 0_usize;
        for plan in windows {
            let key = WindowKey::from_read(&plan.read);
            if let Some(existing) = session.windows.get(&key) {
                if existing.request != plan.read
                    || existing.batch_request.as_ref() != batch
                    || existing.batch_offset != plan.offset
                    || existing.lane_id != lane_id
                {
                    return Err((
                        coded_status(
                            afs_error::NODE_TRANSFER_INVALID,
                            "HIXL prepare replay identity does not match the original request",
                        ),
                        false,
                    ));
                }
                replay.push(Some(existing.reply.clone()));
                replayed += 1;
            } else {
                if session.tombstones.contains(&key) {
                    return Err((
                        coded_status(
                            afs_error::NODE_TRANSFER_UNAVAILABLE,
                            "HIXL read window was already released",
                        ),
                        false,
                    ));
                }
                replay.push(None);
            }
        }
        if replayed == windows.len() {
            return replay
                .into_iter()
                .flatten()
                .zip(windows)
                .map(|(reply, plan)| {
                    Ok(DfsHixlPreparedGroupWindow {
                        batch_offset: plan.offset,
                        lane_id: lane_wire_id,
                        reply: Some(reply),
                    })
                })
                .collect::<Result<Vec<_>, (Status, bool)>>();
        }
        if replayed != 0 {
            return Err((
                coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "HIXL group replay is partial",
                ),
                false,
            ));
        }
        if session.closed {
            return Err((
                coded_status(
                    afs_error::NODE_TRANSFER_UNAVAILABLE,
                    "HIXL session is closed for new windows",
                ),
                false,
            ));
        }
        if admission_frozen {
            return Err((
                coded_status(
                    afs_error::NODE_TRANSFER_UNAVAILABLE,
                    "HIXL engine generation is quarantined",
                ),
                false,
            ));
        }
        if at_capacity {
            return Err((
                coded_status(
                    afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
                    "HIXL export-window capacity is exhausted",
                ),
                false,
            ));
        }
        let first_offset = windows[0].offset;
        let mut created_snapshot = false;
        if let Some(prepared) = &session.prepared_batch {
            let next = usize::try_from(first_offset).map_err(|_| {
                (
                    Status::internal("HIXL group offset exceeds the source address space"),
                    false,
                )
            })?;
            if prepared.request.as_ref() != batch || prepared.next_offset != next {
                return Err((
                    coded_status(
                        afs_error::NODE_TRANSFER_INVALID,
                        "HIXL batch identity changed or group is out of order",
                    ),
                    false,
                ));
            }
        } else {
            if first_offset != 0 {
                return Err((
                    coded_status(
                        afs_error::NODE_TRANSFER_INVALID,
                        "HIXL group must start at offset zero for a new snapshot",
                    ),
                    false,
                ));
            }
            if batch_length > snapshot_budget(group_batch).saturating_sub(prepared_bytes) {
                return Err((
                    coded_status(
                        afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
                        "HIXL verified batch snapshot budget is exhausted",
                    ),
                    false,
                ));
            }
            let started = Instant::now();
            // Both budgets bound the snapshot before any source byte is read,
            // so CRC32C ranges fill it directly like BLAKE3 ranges do.
            let packed = read_hixl_snapshot_ranges(
                local,
                batch,
                batch_length,
                if group_batch {
                    MAX_HIXL_GROUP_BATCH_BYTES
                } else {
                    MAX_HIXL_LEGACY_BATCH_BYTES
                },
            );
            self.observe(Operation::SourceRead, started, packed.is_ok());
            let (bytes, completions) = packed.map_err(|status| (status, false))?;
            let range_digests = batch_range_checksums(batch, &completions);
            session.prepared_batch = Some(PreparedBatch {
                request: Arc::new(batch.clone()),
                bytes: Arc::new(bytes),
                next_offset: 0,
                range_digests,
            });
            created_snapshot = true;
        }
        let mut replies = Vec::with_capacity(windows.len());
        let mut pinned = Vec::with_capacity(windows.len());
        let mut inserted = Vec::with_capacity(windows.len());
        let mut failure = None;
        let rollback_failed = Cell::new(false);
        for plan in windows {
            let step = (|| {
                let (snapshot, start, batch_request, completions) = {
                    let prepared = session
                        .prepared_batch
                        .as_ref()
                        .ok_or_else(|| Status::internal("HIXL verified batch snapshot was lost"))?;
                    let start = usize::try_from(plan.offset)
                        .map_err(|_| Status::internal("HIXL group offset overflow"))?;
                    let end = usize::try_from(plan.end)
                        .map_err(|_| Status::internal("HIXL group offset overflow"))?;
                    let window = prepared.bytes.get(start..end).ok_or_else(|| {
                        coded_status(
                            afs_error::NODE_TRANSFER_INVALID,
                            "HIXL group window exceeds the verified batch",
                        )
                    })?;
                    let completions = window_completions(
                        &plan.read,
                        window,
                        &prepared.range_digests,
                        local,
                        self.metrics.as_ref(),
                    )?;
                    (
                        Arc::clone(&prepared.bytes),
                        start,
                        prepared.request.clone(),
                        completions,
                    )
                };
                let length = plan
                    .end
                    .checked_sub(plan.offset)
                    .and_then(|length| usize::try_from(length).ok())
                    .ok_or_else(|| Status::internal("HIXL window length overflow"))?;
                let export = self
                    .provider
                    .prepare_export_shared(
                        lane_id,
                        snapshot,
                        start,
                        length,
                        Instant::now() + HIXL_SESSION_TTL,
                    )
                    .map_err(runtime_status)?;
                let export =
                    PendingExport::new(self.provider.as_ref(), lane_id, export, &rollback_failed);
                let info = self
                    .provider
                    .lane_infos()
                    .get(lane_id)
                    .cloned()
                    .ok_or_else(|| Status::internal("HIXL lane disappeared during prepare"))?;
                let capacity = u64::try_from(export.window.length).map_err(|_| {
                    coded_status(
                        afs_error::NODE_TRANSFER_INTERNAL,
                        "HIXL export capacity exceeds protocol range",
                    )
                })?;
                let packed_length = u64::try_from(plan.length).map_err(|_| {
                    coded_status(
                        afs_error::NODE_TRANSFER_INTERNAL,
                        "HIXL packed length exceeds protocol range",
                    )
                })?;
                if export.window.address == 0
                    || export.window.length == 0
                    || export.window.slot_id >= export_slots as u64
                    || export.window.generation == 0
                    || packed_length > capacity
                    || export.window.address.checked_add(packed_length).is_none()
                {
                    return Err(coded_status(
                        afs_error::NODE_TRANSFER_INTERNAL,
                        "HIXL runtime returned an invalid export descriptor",
                    ));
                }
                if session.lanes.get(lane_id) != Some(&info) {
                    return Err(coded_status(
                        afs_error::NODE_TRANSFER_UNAVAILABLE,
                        "HIXL engine generation changed during session",
                    ));
                }
                let ranges = plan
                    .read
                    .operations
                    .iter()
                    .zip(&completions)
                    .map(|(operation, completion)| DfsHixlReadRange {
                        operation_index: operation.operation_index,
                        chunk_id: operation.chunk_id.clone(),
                        chunk_offset: operation.chunk_offset,
                        length: operation.length,
                        packed_offset: operation.destination_offset,
                        source_copy_id: operation.source_copy_id.clone(),
                        range_checksum: completion.range_checksum.clone(),
                        range_checksum_algorithm: completion.range_checksum_algorithm,
                        crc32c_table: completion.crc32c_table.clone(),
                    })
                    .collect::<Vec<_>>();
                Ok((
                    DfsPrepareReadRangesHixlReply {
                        session_id,
                        read_id: plan.read.read_id.clone(),
                        attempt_id: plan.read.attempt_id.clone(),
                        source_node_id: self.node_id.clone(),
                        source_node_epoch: self.node_epoch,
                        engine_id: info.engine_id,
                        engine_generation: info.generation,
                        pool_generation: info.generation,
                        slot_id: export.window.slot_id,
                        slot_generation: export.window.generation,
                        address: export.window.address,
                        capacity,
                        packed_length,
                        ranges,
                        completions,
                    },
                    batch_request,
                    export,
                ))
            })();
            match step {
                Ok((reply, batch_request, export)) => {
                    let key = WindowKey::from_read(&plan.read);
                    match session.windows.entry(key.clone()) {
                        Entry::Vacant(entry) => {
                            entry.insert(WindowRecord {
                                request: plan.read.clone(),
                                reply: reply.clone(),
                                batch_request,
                                batch_offset: plan.offset,
                                lane_id,
                            });
                            pinned.push((reply.slot_id, reply.slot_generation));
                            export.disarm();
                        }
                        Entry::Occupied(_) => {
                            failure = Some(coded_status(
                                afs_error::NODE_TRANSFER_INVALID,
                                "HIXL group window key is already admitted",
                            ));
                            break;
                        }
                    }
                    inserted.push(key);
                    replies.push(DfsHixlPreparedGroupWindow {
                        batch_offset: plan.offset,
                        lane_id: lane_wire_id,
                        reply: Some(reply),
                    });
                }
                Err(status) => {
                    failure = Some(status);
                    break;
                }
            }
        }
        if let Some(status) = failure {
            for key in inserted {
                session.windows.remove(&key);
            }
            if created_snapshot {
                session.prepared_batch = None;
            }
            session.last_used = Instant::now();
            let mut rollback_failed = rollback_failed.get();
            for (slot_id, generation) in pinned {
                rollback_failed |= self
                    .provider
                    .release_export(lane_id, slot_id, generation)
                    .is_err();
            }
            return Err((status, rollback_failed));
        }
        let end = windows.last().map_or(0, |plan| plan.end);
        let end = usize::try_from(end)
            .map_err(|_| (Status::internal("HIXL group end overflow"), false))?;
        session
            .prepared_batch
            .as_mut()
            .ok_or_else(|| {
                (
                    Status::internal("HIXL verified batch snapshot was lost"),
                    false,
                )
            })?
            .next_offset = end;
        session.last_used = Instant::now();
        Ok(replies)
    }

    fn release_blocking(
        &self,
        peer: &str,
        request: DfsReleaseHixlReadRequest,
    ) -> Result<Response<DfsReleaseHixlReadReply>, Status> {
        if request.session_id == 0
            || request.read_id.is_empty()
            || request.source_node_id != self.node_id
            || request.source_node_epoch != self.node_epoch
            || request.caller_node_epoch == 0
            || request.engine_id.is_empty()
            || request.windows.is_empty()
            || request.windows.len() > MAX_HIXL_WINDOWS
        {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "HIXL release identity is incomplete",
            ));
        }
        let mut outcomes = Vec::with_capacity(request.windows.len());
        for window in &request.windows {
            if window.attempt_id.is_empty() || window.slot_generation == 0 {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "HIXL release window identity is incomplete",
                ));
            }
            let outcome = DfsHixlReleaseOutcome::try_from(window.outcome).map_err(|_| {
                coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "HIXL release outcome is unknown",
                )
            })?;
            if outcome == DfsHixlReleaseOutcome::Unspecified {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "HIXL release outcome is required",
                ));
            }
            if outcome == DfsHixlReleaseOutcome::NotSubmitted && window.content_validated {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "a never-submitted HIXL window cannot claim validated content",
                ));
            }
            outcomes.push(outcome);
        }
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Status::internal("HIXL registry lock is poisoned"))?;
        registry.expire_sessions();
        self.quarantine_pending_windows(&mut registry, false)?;
        let mut results = vec![DfsHixlReleaseWindowResult::default(); request.windows.len()];
        let mut pending = Vec::new();
        let mut pinned = false;
        let mut release_error = None;
        {
            let session = registry.session_mut(
                request.session_id,
                peer,
                request.caller_node_epoch,
                request.source_node_epoch,
            )?;
            for ((index, window), outcome) in request
                .windows
                .iter()
                .enumerate()
                .zip(outcomes.iter().copied())
            {
                let key = WindowKey {
                    read_id: request.read_id.clone(),
                    attempt_id: window.attempt_id.clone(),
                };
                if session.tombstones.matches(&key, &request, window) {
                    results[index] = DfsHixlReleaseWindowResult {
                        consumed: true,
                        already_released: true,
                    };
                    continue;
                }
                let record = session.windows.get(&key).ok_or_else(|| {
                    coded_status(
                        afs_error::NODE_TRANSFER_INVALID,
                        "HIXL release does not identify an admitted window",
                    )
                })?;
                validate_release_identity(record, &request, window)?;
                if matches!(
                    outcome,
                    DfsHixlReleaseOutcome::Unknown | DfsHixlReleaseOutcome::Failed
                ) {
                    pinned = true;
                    continue;
                }
                pending.push((
                    index,
                    key,
                    record.lane_id,
                    record.reply.slot_id,
                    record.reply.slot_generation,
                    ReleaseTombstone::from_window(record, window.outcome, window.content_validated),
                ));
            }
            for (index, key, lane_id, slot_id, slot_generation, identity) in pending {
                if let Err(error) = self
                    .provider
                    .release_export(lane_id, slot_id, slot_generation)
                {
                    release_error = Some(runtime_status(error));
                    pinned = true;
                    break;
                }
                session.windows.remove(&key);
                session.tombstones.insert(identity);
                session.last_used = Instant::now();
                results[index].consumed = true;
            }
            if session.windows.is_empty()
                && session
                    .prepared_batch
                    .as_ref()
                    .is_some_and(|batch| batch.next_offset == batch.bytes.len())
            {
                session.prepared_batch = None;
            }
            session.last_used = Instant::now();
        }
        if pinned {
            self.quarantine_pending_windows(&mut registry, true)?;
        }
        if let Some(status) = release_error {
            return Err(status);
        }
        Ok(Response::new(DfsReleaseHixlReadReply { windows: results }))
    }

    fn close_blocking(
        &self,
        peer: &str,
        request: DfsCloseHixlRequest,
    ) -> Result<Response<DfsCloseHixlReply>, Status> {
        if request.session_id == 0
            || request.source_node_epoch != self.node_epoch
            || request.caller_node_epoch == 0
            || request.engine_id.is_empty()
        {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "HIXL close identity is incomplete",
            ));
        }
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Status::internal("HIXL registry lock is poisoned"))?;
        registry.expire_sessions();
        self.quarantine_pending_windows(&mut registry, false)?;
        let drained = {
            let session = registry.session_mut(
                request.session_id,
                peer,
                request.caller_node_epoch,
                request.source_node_epoch,
            )?;
            if request.engine_id != session.engine_id
                || request.engine_generation != session.engine_generation
            {
                return Err(Status::permission_denied(
                    "HIXL close engine identity does not match the session",
                ));
            }
            session.closed = true;
            session.prepared_batch = None;
            session.last_used = Instant::now();
            session.windows.is_empty()
        };
        self.quarantine_pending_windows(&mut registry, false)?;
        Ok(Response::new(DfsCloseHixlReply {
            admitted: true,
            drained,
        }))
    }

    #[cfg(test)]
    fn expire_for_test(&self) {
        let mut registry = self.registry.lock().expect("test registry lock");
        for session in registry.sessions.values_mut() {
            session.closed = true;
        }
    }

    fn quarantine_pending_windows(
        &self,
        registry: &mut Registry,
        force: bool,
    ) -> Result<(), Status> {
        if !force && !registry.has_closed_pending_windows() {
            return Ok(());
        }
        registry.admission_frozen = true;
        for session in registry.sessions.values_mut() {
            session.prepared_batch = None;
        }
        if registry.runtime_quarantined {
            return Ok(());
        }
        self.provider.quarantine().map_err(runtime_status)?;
        registry.runtime_quarantined = true;
        Ok(())
    }

    fn authorize(
        &self,
        authorizer: &dyn DfsReadAuthorizer,
        peer: &str,
        read: &DfsReadRangesRequest,
    ) -> Result<(), Status> {
        let started = Instant::now();
        let result = authorizer.authorize(peer, read);
        self.observe(Operation::Authorize, started, result.is_ok());
        result.map_err(error_to_status)
    }
}

pub(crate) trait ExportProvider: Send + Sync {
    fn info(&self) -> EngineInfo;
    fn lane_infos(&self) -> Vec<EngineInfo> {
        vec![self.info()]
    }
    fn export_slots(&self, lane: usize) -> usize;
    fn prepare_export(
        &self,
        lane: usize,
        bytes: Vec<u8>,
        deadline: Instant,
    ) -> Result<ExportWindow, HixlError>;
    fn prepare_export_shared(
        &self,
        lane: usize,
        data: Arc<Vec<u8>>,
        offset: usize,
        length: usize,
        deadline: Instant,
    ) -> Result<ExportWindow, HixlError> {
        let window = data
            .get(offset..offset.saturating_add(length))
            .ok_or_else(invalid_export_range)?
            .to_vec();
        self.prepare_export(lane, window, deadline)
    }
    fn release_export(&self, lane: usize, slot_id: u64, generation: u64) -> Result<(), HixlError>;
    fn quarantine(&self) -> Result<(), HixlError>;
}

struct PendingExport<'a> {
    provider: &'a dyn ExportProvider,
    window: ExportWindow,
    lane_id: usize,
    rollback_failed: &'a Cell<bool>,
    armed: bool,
}

impl<'a> PendingExport<'a> {
    fn new(
        provider: &'a dyn ExportProvider,
        lane_id: usize,
        window: ExportWindow,
        rollback_failed: &'a Cell<bool>,
    ) -> Self {
        Self {
            provider,
            window,
            lane_id,
            rollback_failed,
            armed: true,
        }
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for PendingExport<'_> {
    fn drop(&mut self) {
        if self.armed
            && self
                .provider
                .release_export(self.lane_id, self.window.slot_id, self.window.generation)
                .is_err()
        {
            self.rollback_failed.set(true);
        }
    }
}

struct RuntimeExportProvider {
    runtime: Arc<HixlRuntimeGroup>,
}

impl ExportProvider for RuntimeExportProvider {
    fn info(&self) -> EngineInfo {
        self.runtime.info()
    }

    fn lane_infos(&self) -> Vec<EngineInfo> {
        self.runtime.infos()
    }

    fn export_slots(&self, lane: usize) -> usize {
        self.runtime
            .lane(lane)
            .map_or(0, |runtime| runtime.status().export_slots)
    }

    fn prepare_export(
        &self,
        lane: usize,
        bytes: Vec<u8>,
        deadline: Instant,
    ) -> Result<ExportWindow, HixlError> {
        self.runtime
            .lane(lane)
            .ok_or_else(invalid_runtime_lane)?
            .prepare_export(bytes, deadline)
    }

    fn prepare_export_shared(
        &self,
        lane: usize,
        data: Arc<Vec<u8>>,
        offset: usize,
        length: usize,
        deadline: Instant,
    ) -> Result<ExportWindow, HixlError> {
        self.runtime
            .lane(lane)
            .ok_or_else(invalid_runtime_lane)?
            .prepare_export_shared(data, offset, length, deadline)
    }

    fn release_export(&self, lane: usize, slot_id: u64, generation: u64) -> Result<(), HixlError> {
        self.runtime
            .lane(lane)
            .ok_or_else(invalid_runtime_lane)?
            .release_export(slot_id, generation)
    }

    fn quarantine(&self) -> Result<(), HixlError> {
        self.runtime.quarantine()
    }
}

fn invalid_runtime_lane() -> HixlError {
    HixlError {
        phase: afs_transport::hixl::HixlPhase::PrepareExport,
        kind: afs_transport::hixl::HixlErrorKind::InvalidArgument,
        native_status: None,
        message: "HIXL lane is not configured".into(),
        read_may_be_in_flight: false,
    }
}

fn invalid_export_range() -> HixlError {
    HixlError {
        phase: afs_transport::hixl::HixlPhase::PrepareExport,
        kind: afs_transport::hixl::HixlErrorKind::InvalidArgument,
        native_status: None,
        message: "HIXL export range exceeds the shared snapshot".into(),
        read_may_be_in_flight: false,
    }
}

fn batch_range_checksums(
    batch: &DfsReadRangesRequest,
    completions: &[DfsReadRangesCompletion],
) -> HashMap<(String, u64, u64), PackedRangeDigest> {
    batch
        .operations
        .iter()
        .zip(completions)
        .map(|(operation, completion)| {
            (
                (
                    operation.chunk_id.clone(),
                    operation.chunk_offset,
                    operation.length,
                ),
                PackedRangeDigest {
                    checksum: completion.range_checksum.clone(),
                    algorithm: completion.range_checksum_algorithm,
                    table: completion.crc32c_table.clone(),
                },
            )
        })
        .collect()
}

fn window_completions(
    read: &DfsReadRangesRequest,
    bytes: &[u8],
    reusable: &HashMap<(String, u64, u64), PackedRangeDigest>,
    local: &LocalChunkStore,
    metrics: Option<&HixlMetrics>,
) -> Result<Vec<DfsReadRangesCompletion>, Status> {
    let mut reused = 0_usize;
    let mut computed = 0_usize;
    let completions = read
        .operations
        .iter()
        .map(|operation| {
            let start = usize::try_from(operation.destination_offset)
                .map_err(|_| Status::invalid_argument("HIXL range offset overflow"))?;
            let length = usize::try_from(operation.length)
                .map_err(|_| Status::invalid_argument("HIXL range length overflow"))?;
            let end = start
                .checked_add(length)
                .ok_or_else(|| Status::invalid_argument("HIXL range end overflow"))?;
            let range = bytes.get(start..end).ok_or_else(|| {
                Status::invalid_argument("HIXL range exceeds the verified window")
            })?;
            let digest = match reusable.get(&(
                operation.chunk_id.clone(),
                operation.chunk_offset,
                operation.length,
            )) {
                Some(digest) => {
                    reused += 1;
                    digest.clone()
                }
                None => {
                    computed += 1;
                    // A window slice can cut a stored range, so recompute the
                    // same transport digest the snapshot used for this Chunk:
                    // CRC32C objects stay bound to their Meta descriptor.
                    match local
                        .peer_crc32c_table(&crate::dfs::ChunkId::new(operation.chunk_id.clone()))
                        .map_err(error_to_status)?
                    {
                        // A slice that cuts a segment carries no descriptor;
                        // the reader binds it by its range checksum.
                        Some(table) => PackedRangeDigest {
                            checksum: crate::node::chunk_crc32c::checksum(range)
                                .to_le_bytes()
                                .to_vec(),
                            algorithm: DfsDigestAlgorithm::Crc32c as i32,
                            table: if table
                                .covers_whole_segments(operation.chunk_offset, operation.length)
                            {
                                serde_json::to_vec(&table)
                                    .map_err(|error| Status::internal(error.to_string()))?
                            } else {
                                Vec::new()
                            },
                        },
                        None => PackedRangeDigest {
                            checksum: blake3::hash(range).as_bytes().to_vec(),
                            algorithm: DfsDigestAlgorithm::Blake3 as i32,
                            table: Vec::new(),
                        },
                    }
                }
            };
            Ok(DfsReadRangesCompletion {
                crc32c_table: digest.table,
                read_id: read.read_id.clone(),
                attempt_id: read.attempt_id.clone(),
                operation_index: operation.operation_index,
                source_copy_id: operation.source_copy_id.clone(),
                transferred_bytes: operation.length,
                range_checksum: digest.checksum,
                range_checksum_algorithm: digest.algorithm,
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    if let Some(metrics) = metrics {
        metrics.observe_source_checksums(reused, computed);
    }
    Ok(completions)
}

#[derive(Default)]
struct Registry {
    next_session: u64,
    sessions: HashMap<u64, Session>,
    admission_frozen: bool,
    runtime_quarantined: bool,
    draining: bool,
}

impl Registry {
    fn expire_sessions(&mut self) {
        let now = Instant::now();
        for session in self.sessions.values_mut() {
            if now.duration_since(session.last_used) >= HIXL_SESSION_TTL {
                session.closed = true;
            }
            if session.closed {
                session.prepared_batch = None;
            }
            session.tombstones.expire(now);
        }
        self.sessions.retain(|_, session| {
            !(session.closed
                && session.windows.is_empty()
                && session.tombstones.is_empty()
                && now.duration_since(session.last_used) >= HIXL_SESSION_TTL)
        });
    }

    fn has_closed_pending_windows(&self) -> bool {
        self.sessions
            .values()
            .any(|session| session.closed && !session.windows.is_empty())
    }

    fn next_session_id(&mut self) -> Result<u64, Status> {
        let id = if self.next_session == 0 {
            1
        } else {
            self.next_session
        };
        self.next_session = id.checked_add(1).ok_or_else(|| {
            coded_status(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "HIXL session id space exhausted",
            )
        })?;
        Ok(id)
    }

    fn active_window_count(&self) -> usize {
        self.sessions
            .values()
            .map(|session| session.windows.len())
            .sum()
    }

    fn lane_window_count(&self, lane: usize) -> usize {
        self.sessions
            .values()
            .flat_map(|session| session.windows.values())
            .filter(|window| window.lane_id == lane)
            .count()
    }

    fn prepared_batch_bytes(&self) -> usize {
        self.sessions
            .values()
            .filter_map(|session| session.prepared_batch.as_ref())
            .map(|batch| batch.bytes.len())
            .sum()
    }

    fn session_mut(
        &mut self,
        session_id: u64,
        peer: &str,
        caller_epoch: u64,
        source_epoch: u64,
    ) -> Result<&mut Session, Status> {
        let session = self.sessions.get_mut(&session_id).ok_or_else(|| {
            coded_status(afs_error::NODE_TRANSFER_UNAVAILABLE, "unknown HIXL session")
        })?;
        if session.peer != peer
            || session.caller_epoch != caller_epoch
            || session.source_epoch != source_epoch
        {
            return Err(Status::permission_denied(
                "HIXL session identity does not match the authenticated peer",
            ));
        }
        Ok(session)
    }
}

struct Session {
    peer: String,
    caller_epoch: u64,
    source_epoch: u64,
    engine_id: String,
    engine_generation: u64,
    lanes: Vec<EngineInfo>,
    closed: bool,
    last_used: Instant,
    windows: HashMap<WindowKey, WindowRecord>,
    tombstones: Tombstones,
    prepared_batch: Option<PreparedBatch>,
}

struct PreparedBatch {
    request: Arc<DfsReadRangesRequest>,
    bytes: Arc<Vec<u8>>,
    next_offset: usize,
    range_digests: HashMap<(String, u64, u64), PackedRangeDigest>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct WindowKey {
    read_id: String,
    attempt_id: String,
}

impl WindowKey {
    fn from_read(read: &DfsReadRangesRequest) -> Self {
        Self {
            read_id: read.read_id.clone(),
            attempt_id: read.attempt_id.clone(),
        }
    }
}

struct WindowRecord {
    request: DfsReadRangesRequest,
    reply: DfsPrepareReadRangesHixlReply,
    batch_request: Arc<DfsReadRangesRequest>,
    batch_offset: u64,
    lane_id: usize,
}

/// One validated native window of a group request. All offsets are absolute
/// in the verified parent batch.
struct GroupWindowPlan {
    read: DfsReadRangesRequest,
    length: usize,
    offset: u64,
    end: u64,
}

#[derive(Clone)]
struct ReleaseTombstone {
    key: WindowKey,
    engine_id: String,
    engine_generation: u64,
    pool_generation: u64,
    slot_id: u64,
    slot_generation: u64,
    caller_epoch: u64,
    source_epoch: u64,
    outcome: i32,
    content_validated: bool,
    inserted_at: Instant,
}

impl ReleaseTombstone {
    fn from_window(window: &WindowRecord, outcome: i32, content_validated: bool) -> Self {
        Self {
            key: WindowKey::from_read(&window.request),
            engine_id: window.reply.engine_id.clone(),
            engine_generation: window.reply.engine_generation,
            pool_generation: window.reply.pool_generation,
            slot_id: window.reply.slot_id,
            slot_generation: window.reply.slot_generation,
            caller_epoch: window.request.operations[0]
                .grant
                .as_ref()
                .map_or(0, |grant| grant.caller_node_epoch),
            source_epoch: window.reply.source_node_epoch,
            outcome,
            content_validated,
            inserted_at: Instant::now(),
        }
    }
}

#[derive(Default)]
struct Tombstones {
    by_key: HashMap<WindowKey, ReleaseTombstone>,
    order: VecDeque<WindowKey>,
}

impl Tombstones {
    fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    fn expire(&mut self, now: Instant) {
        while let Some(key) = self.order.front().cloned() {
            let expired = self
                .by_key
                .get(&key)
                .is_none_or(|entry| now.duration_since(entry.inserted_at) >= HIXL_SESSION_TTL);
            if !expired {
                break;
            }
            self.order.pop_front();
            self.by_key.remove(&key);
        }
    }

    fn contains(&self, key: &WindowKey) -> bool {
        self.by_key.contains_key(key)
    }

    fn matches(
        &self,
        key: &WindowKey,
        request: &DfsReleaseHixlReadRequest,
        release: &DfsHixlReleaseWindow,
    ) -> bool {
        self.by_key.get(key).is_some_and(|entry| {
            entry.engine_id == request.engine_id
                && entry.engine_generation == request.engine_generation
                && entry.pool_generation == request.pool_generation
                && entry.slot_id == release.slot_id
                && entry.slot_generation == release.slot_generation
                && entry.caller_epoch == request.caller_node_epoch
                && entry.source_epoch == request.source_node_epoch
                && entry.outcome == release.outcome
                && entry.content_validated == release.content_validated
        })
    }

    fn insert(&mut self, tombstone: ReleaseTombstone) {
        if self.by_key.contains_key(&tombstone.key) {
            return;
        }
        let key = tombstone.key.clone();
        self.by_key.insert(key.clone(), tombstone);
        self.order.push_back(key);
        while self.order.len() > MAX_HIXL_RELEASE_TOMBSTONES {
            if let Some(oldest) = self.order.pop_front() {
                self.by_key.remove(&oldest);
            }
        }
    }
}

fn validate_hixl_identity(
    peer: &str,
    source_epoch: u64,
    caller_epoch: u64,
    requested_source_epoch: u64,
    read: &DfsReadRangesRequest,
) -> Result<(), Status> {
    validate_dfs_read_request(read)?;
    if caller_epoch == 0 || requested_source_epoch != source_epoch {
        return Err(Status::permission_denied(
            "HIXL caller or source epoch is stale",
        ));
    }
    if dfs_read_caller_epoch(peer, read)? != caller_epoch {
        return Err(Status::permission_denied(
            "HIXL caller epoch does not match the read grant",
        ));
    }
    Ok(())
}

fn negotiated_transport(
    info: &EngineInfo,
) -> Result<(DfsHixlTransport, u32, u32, Option<u64>), Status> {
    match info.transport {
        ReadTransport::Roce => {
            if info.buffer_pool_count != 0
                || info.buffer_pool_size_mib != 0
                || info.hccs_domain_id.is_some()
            {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "HIXL RoCE relay parameters must be empty",
                ));
            }
            Ok((DfsHixlTransport::Roce, 0, 0, None))
        }
        ReadTransport::HccsBufferPool => {
            if info.buffer_pool_count == 0 || info.buffer_pool_size_mib == 0 {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "HIXL HCCS buffer-pool parameters are invalid",
                ));
            }
            let count = u32::try_from(info.buffer_pool_count).map_err(|_| {
                coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "HIXL HCCS buffer-pool count exceeds protocol range",
                )
            })?;
            let size_mib = u32::try_from(info.buffer_pool_size_mib).map_err(|_| {
                coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "HIXL HCCS buffer-pool size exceeds protocol range",
                )
            })?;
            let domain_id = info.hccs_domain_id.ok_or_else(|| {
                coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "HIXL HCCS super-pod domain is absent",
                )
            })?;
            Ok((
                DfsHixlTransport::HccsBufferPool,
                count,
                size_mib,
                Some(domain_id),
            ))
        }
    }
}

fn validate_transport_request(
    request: &DfsNegotiateHixlRequest,
    expected_transport: DfsHixlTransport,
    expected_buffer_pool_count: u32,
    expected_buffer_pool_size_mib: u32,
    expected_hccs_domain_id: Option<u64>,
) -> Result<(), Status> {
    let requested_transport = DfsHixlTransport::try_from(request.transport).map_err(|_| {
        coded_status(
            afs_error::NODE_TRANSFER_INVALID,
            "HIXL transport is unknown",
        )
    })?;
    if requested_transport != expected_transport
        || request.buffer_pool_count != expected_buffer_pool_count
        || request.buffer_pool_size_mib != expected_buffer_pool_size_mib
        || request.hccs_domain_id != expected_hccs_domain_id
    {
        return Err(coded_status(
            afs_error::NODE_TRANSFER_INVALID,
            "HIXL transport or buffer-pool parameters do not match",
        ));
    }
    Ok(())
}

fn effective_export_slots(slots: usize) -> Result<u32, Status> {
    let slots = slots.min(MAX_HIXL_WINDOWS);
    if slots == 0 {
        return Err(coded_status(
            afs_error::NODE_TRANSFER_UNAVAILABLE,
            "HIXL export-slot pool is unavailable",
        ));
    }
    u32::try_from(slots).map_err(|_| {
        coded_status(
            afs_error::NODE_TRANSFER_INTERNAL,
            "HIXL export-slot capacity exceeds protocol range",
        )
    })
}

fn validate_release_identity(
    window: &WindowRecord,
    request: &DfsReleaseHixlReadRequest,
    release: &DfsHixlReleaseWindow,
) -> Result<(), Status> {
    if window.reply.session_id != request.session_id
        || window.reply.source_node_epoch != request.source_node_epoch
        || window.reply.engine_id != request.engine_id
        || window.reply.engine_generation != request.engine_generation
        || window.reply.pool_generation != request.pool_generation
        || window.reply.slot_id != release.slot_id
        || window.reply.slot_generation != release.slot_generation
        || window
            .request
            .operations
            .first()
            .and_then(|operation| operation.grant.as_ref())
            .is_none_or(|grant| grant.caller_node_epoch != request.caller_node_epoch)
    {
        return Err(Status::permission_denied(
            "HIXL release identity does not match the prepared window",
        ));
    }
    Ok(())
}

fn runtime_status(error: HixlError) -> Status {
    error_to_status(crate::node::rpc::hixl::runtime_error(error))
}

pub(crate) fn unsupported_status() -> Status {
    coded_status(
        afs_error::NODE_TRANSFER_UNSUPPORTED,
        "HIXL DFS read source-window capability is unavailable",
    )
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::HixlReadService;

    pub(crate) struct SessionState {
        pub session_id: u64,
        pub closed: bool,
        pub prepared_bytes: usize,
        pub active_windows: usize,
        pub outcomes: Vec<i32>,
    }

    pub(crate) fn sessions(service: &HixlReadService) -> Vec<SessionState> {
        let registry = service.registry.lock().expect("test source registry lock");
        let mut states = registry
            .sessions
            .iter()
            .map(|(session_id, session)| SessionState {
                session_id: *session_id,
                closed: session.closed,
                prepared_bytes: session
                    .prepared_batch
                    .as_ref()
                    .map_or(0, |batch| batch.bytes.len()),
                active_windows: session.windows.len(),
                outcomes: session
                    .tombstones
                    .by_key
                    .values()
                    .map(|entry| entry.outcome)
                    .collect(),
            })
            .collect::<Vec<_>>();
        states.sort_by_key(|state| state.session_id);
        states
    }
}

#[cfg(test)]
mod tests;
