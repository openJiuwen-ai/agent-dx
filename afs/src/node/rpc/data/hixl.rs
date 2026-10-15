//! Authenticated DFS source-window control for the optional HIXL rH2H path.
//!
//! This module owns only the control-plane lease. HixlRuntime owns the
//! fixed ACL Host memory and the native request state; the registry below
//! keeps the AFS grant/session/window identity that makes a runtime export
//! eligible for a remote READ. In particular, closing a session only stops
//! admission. It never claims that an accepted native request has drained.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use afs_protocol::node_data::{
    DfsCloseHixlReply, DfsCloseHixlRequest, DfsDigestAlgorithm, DfsHixlLane,
    DfsHixlPreparedGroupWindow, DfsHixlReadRange, DfsHixlReleaseOutcome, DfsHixlTransport,
    DfsNegotiateHixlReply, DfsNegotiateHixlRequest, DfsPrepareReadRangesHixlGroupReply,
    DfsPrepareReadRangesHixlGroupRequest, DfsPrepareReadRangesHixlReply,
    DfsPrepareReadRangesHixlRequest, DfsReadRangesCompletion, DfsReadRangesRequest,
    DfsReleaseHixlReadReply, DfsReleaseHixlReadRequest,
};
use afs_transport::grpc::error_status::{coded_status, error_to_status};
use afs_transport::hixl::{EngineInfo, ExportWindow, HixlError, HixlRuntimeGroup, ReadTransport};
use tonic::{Response, Status};

use super::prepared::{
    read_packed_dfs_ranges, read_packed_dfs_ranges_with_budget, validate_packed_dfs_read,
};
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
        let read = request.read_request.as_ref().ok_or_else(|| {
            coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "HIXL read authority is absent",
            )
        })?;
        validate_hixl_identity(
            peer,
            self.node_epoch,
            request.caller_node_epoch,
            request.source_node_epoch,
            read,
        )?;
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
        self.authorize(authorizer.as_ref(), peer, read)?;

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
            let packed = if group_batch {
                read_packed_dfs_ranges_with_budget(
                    &local,
                    &batch,
                    batch_length,
                    MAX_HIXL_GROUP_BATCH_BYTES,
                )
            } else {
                read_packed_dfs_ranges(&local, &batch, batch_length)
            };
            self.observe(Operation::SourceRead, started, packed.is_ok());
            let (bytes, completions) = packed?;
            let range_checksums = batch_range_checksums(&batch, &completions);
            session.prepared_batch = Some(PreparedBatch {
                request: Arc::new(batch),
                bytes,
                next_offset: 0,
                range_checksums,
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
            &prepared.range_checksums,
            self.metrics.as_ref(),
        )?;
        let export = self
            .provider
            .prepare_export(lane_id, bytes, Instant::now() + HIXL_SESSION_TTL)
            .map_err(runtime_status)?;
        let info = self
            .provider
            .lane_infos()
            .get(lane_id)
            .cloned()
            .ok_or_else(|| Status::internal("HIXL lane disappeared during prepare"))?;
        let capacity = u64::try_from(export.length).map_err(|_| {
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
        if export.address == 0
            || export.length == 0
            || export.slot_id >= export_slots as u64
            || export.generation == 0
            || packed_length > capacity
            || export.address.checked_add(packed_length).is_none()
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
            slot_id: export.slot_id,
            slot_generation: export.generation,
            address: export.address,
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
        Ok(Response::new(reply))
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
        if registry.admission_frozen || registry.draining {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "HIXL engine generation is quarantined",
            ));
        }
        if registry.lane_window_count(lane_id) + plans.len() > export_slots {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
                "HIXL export-window capacity is exhausted",
            ));
        }
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
    ) -> Result<Vec<DfsHixlPreparedGroupWindow>, (Status, bool)> {
        let session = registry
            .session_mut(session_id, peer, caller_epoch, source_epoch)
            .map_err(|status| (status, false))?;
        session.last_used = Instant::now();
        if session.closed {
            return Err((
                coded_status(
                    afs_error::NODE_TRANSFER_UNAVAILABLE,
                    "HIXL session is closed for new windows",
                ),
                false,
            ));
        }
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
            let packed = if group_batch {
                read_packed_dfs_ranges_with_budget(
                    local,
                    batch,
                    batch_length,
                    MAX_HIXL_GROUP_BATCH_BYTES,
                )
            } else {
                read_packed_dfs_ranges(local, batch, batch_length)
            };
            self.observe(Operation::SourceRead, started, packed.is_ok());
            let (bytes, completions) = packed.map_err(|status| (status, false))?;
            let range_checksums = batch_range_checksums(batch, &completions);
            session.prepared_batch = Some(PreparedBatch {
                request: Arc::new(batch.clone()),
                bytes,
                next_offset: 0,
                range_checksums,
            });
            created_snapshot = true;
        }
        let mut replies = Vec::with_capacity(windows.len());
        let mut pinned = Vec::with_capacity(windows.len());
        let mut inserted = Vec::with_capacity(windows.len());
        let mut failure = None;
        for plan in windows {
            let step = (|| -> Result<(DfsPrepareReadRangesHixlReply, Arc<DfsReadRangesRequest>), Status> {
                let (bytes, batch_request, completions) = {
                    let prepared = session.prepared_batch.as_ref().ok_or_else(|| {
                        Status::internal("HIXL verified batch snapshot was lost")
                    })?;
                    let start = usize::try_from(plan.offset)
                        .map_err(|_| Status::internal("HIXL group offset overflow"))?;
                    let end = usize::try_from(plan.end)
                        .map_err(|_| Status::internal("HIXL group offset overflow"))?;
                    let bytes = prepared
                        .bytes
                        .get(start..end)
                        .ok_or_else(|| {
                            coded_status(
                                afs_error::NODE_TRANSFER_INVALID,
                                "HIXL group window exceeds the verified batch",
                            )
                        })?
                        .to_vec();
                    let completions = window_completions(
                        &plan.read,
                        &bytes,
                        &prepared.range_checksums,
                        self.metrics.as_ref(),
                    )?;
                    (bytes, prepared.request.clone(), completions)
                };
                let export = self
                    .provider
                    .prepare_export(lane_id, bytes, Instant::now() + HIXL_SESSION_TTL)
                    .map_err(runtime_status)?;
                let info = self
                    .provider
                    .lane_infos()
                    .get(lane_id)
                    .cloned()
                    .ok_or_else(|| {
                        Status::internal("HIXL lane disappeared during prepare")
                    })?;
                let capacity = u64::try_from(export.length).map_err(|_| {
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
                if export.address == 0
                    || export.length == 0
                    || export.slot_id >= export_slots as u64
                    || export.generation == 0
                    || packed_length > capacity
                    || export.address.checked_add(packed_length).is_none()
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
                        slot_id: export.slot_id,
                        slot_generation: export.generation,
                        address: export.address,
                        capacity,
                        packed_length,
                        ranges,
                        completions,
                    },
                    batch_request,
                ))
            })();
            match step {
                Ok((reply, batch_request)) => {
                    pinned.push((reply.slot_id, reply.slot_generation));
                    let key = WindowKey::from_read(&plan.read);
                    session.windows.insert(
                        key.clone(),
                        WindowRecord {
                            request: plan.read.clone(),
                            reply: reply.clone(),
                            batch_request,
                            batch_offset: plan.offset,
                            lane_id,
                        },
                    );
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
            let mut rollback_failed = false;
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
            || request.attempt_id.is_empty()
            || request.source_node_id != self.node_id
            || request.source_node_epoch != self.node_epoch
            || request.caller_node_epoch == 0
            || request.engine_id.is_empty()
            || request.slot_generation == 0
        {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "HIXL release identity is incomplete",
            ));
        }
        let outcome = DfsHixlReleaseOutcome::try_from(request.outcome).map_err(|_| {
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
        if outcome == DfsHixlReleaseOutcome::NotSubmitted && request.content_validated {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "a never-submitted HIXL window cannot claim validated content",
            ));
        }
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Status::internal("HIXL registry lock is poisoned"))?;
        registry.expire_sessions();
        self.quarantine_pending_windows(&mut registry, false)?;
        let key = WindowKey {
            read_id: request.read_id.clone(),
            attempt_id: request.attempt_id.clone(),
        };
        let prepared = {
            let session = registry.session_mut(
                request.session_id,
                peer,
                request.caller_node_epoch,
                request.source_node_epoch,
            )?;
            if session.tombstones.matches(&key, &request) {
                session.last_used = Instant::now();
                return Ok(Response::new(DfsReleaseHixlReadReply {
                    consumed: true,
                    already_released: true,
                }));
            }
            let window = session.windows.get(&key).ok_or_else(|| {
                coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "HIXL release does not identify an admitted window",
                )
            })?;
            validate_release_identity(window, &request)?;
            if matches!(
                outcome,
                DfsHixlReleaseOutcome::Unknown | DfsHixlReleaseOutcome::Failed
            ) {
                session.last_used = Instant::now();
                None
            } else {
                Some((
                    window.lane_id,
                    window.reply.slot_id,
                    window.reply.slot_generation,
                    ReleaseTombstone::from_window(
                        window,
                        request.outcome,
                        request.content_validated,
                    ),
                ))
            }
        };
        if prepared.is_none() {
            self.quarantine_pending_windows(&mut registry, true)?;
            return Ok(Response::new(DfsReleaseHixlReadReply {
                consumed: false,
                already_released: false,
            }));
        }
        let Some((lane_id, slot_id, slot_generation, identity)) = prepared else {
            return Err(Status::internal("HIXL release state was lost"));
        };
        self.provider
            .release_export(lane_id, slot_id, slot_generation)
            .map_err(runtime_status)?;
        let session = registry.session_mut(
            request.session_id,
            peer,
            request.caller_node_epoch,
            request.source_node_epoch,
        )?;
        session.windows.remove(&key);
        session.tombstones.insert(identity);
        if session.windows.is_empty()
            && session
                .prepared_batch
                .as_ref()
                .is_some_and(|batch| batch.next_offset == batch.bytes.len())
        {
            session.prepared_batch = None;
        }
        session.last_used = Instant::now();
        Ok(Response::new(DfsReleaseHixlReadReply {
            consumed: true,
            already_released: false,
        }))
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
    fn release_export(&self, lane: usize, slot_id: u64, generation: u64) -> Result<(), HixlError>;
    fn quarantine(&self) -> Result<(), HixlError>;
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

fn batch_range_checksums(
    batch: &DfsReadRangesRequest,
    completions: &[DfsReadRangesCompletion],
) -> HashMap<(String, u64, u64), Vec<u8>> {
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
                completion.range_checksum.clone(),
            )
        })
        .collect()
}

fn window_completions(
    read: &DfsReadRangesRequest,
    bytes: &[u8],
    reusable: &HashMap<(String, u64, u64), Vec<u8>>,
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
            let range_checksum = match reusable.get(&(
                operation.chunk_id.clone(),
                operation.chunk_offset,
                operation.length,
            )) {
                Some(checksum) => {
                    reused += 1;
                    checksum.clone()
                }
                None => {
                    computed += 1;
                    blake3::hash(range).as_bytes().to_vec()
                }
            };
            Ok(DfsReadRangesCompletion {
                crc32c_table: Default::default(),
                read_id: read.read_id.clone(),
                attempt_id: read.attempt_id.clone(),
                operation_index: operation.operation_index,
                source_copy_id: operation.source_copy_id.clone(),
                transferred_bytes: operation.length,
                range_checksum,
                range_checksum_algorithm: DfsDigestAlgorithm::Blake3 as i32,
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
    bytes: Vec<u8>,
    next_offset: usize,
    range_checksums: HashMap<(String, u64, u64), Vec<u8>>,
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

    fn matches(&self, key: &WindowKey, request: &DfsReleaseHixlReadRequest) -> bool {
        self.by_key.get(key).is_some_and(|entry| {
            entry.engine_id == request.engine_id
                && entry.engine_generation == request.engine_generation
                && entry.pool_generation == request.pool_generation
                && entry.slot_id == request.slot_id
                && entry.slot_generation == request.slot_generation
                && entry.caller_epoch == request.caller_node_epoch
                && entry.source_epoch == request.source_node_epoch
                && entry.outcome == request.outcome
                && entry.content_validated == request.content_validated
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
) -> Result<(), Status> {
    if window.reply.session_id != request.session_id
        || window.reply.source_node_epoch != request.source_node_epoch
        || window.reply.engine_id != request.engine_id
        || window.reply.engine_generation != request.engine_generation
        || window.reply.pool_generation != request.pool_generation
        || window.reply.slot_id != request.slot_id
        || window.reply.slot_generation != request.slot_generation
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
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::node::chunk::{ChunkStore, StagedChunk};
    use afs_protocol::node_data::{
        DfsChunkReadOp, DfsHixlGroupWindow, DfsHixlTransport, DfsReadGrant,
    };
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
                address: 0x1000,
                length: bytes.len(),
            })
        }

        fn release_export(
            &self,
            lane: usize,
            slot_id: u64,
            generation: u64,
        ) -> Result<(), HixlError> {
            self.releases.fetch_add(1, Ordering::SeqCst);
            self.released_windows
                .lock()
                .unwrap()
                .push((lane, slot_id, generation));
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

    fn prepare_request(
        session_id: u64,
        read: DfsReadRangesRequest,
    ) -> DfsPrepareReadRangesHixlRequest {
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
            attempt_id: reply.attempt_id.clone(),
            source_node_id: reply.source_node_id.clone(),
            source_node_epoch: reply.source_node_epoch,
            engine_id: reply.engine_id.clone(),
            engine_generation: reply.engine_generation,
            pool_generation: reply.pool_generation,
            slot_id: reply.slot_id,
            slot_generation: reply.slot_generation,
            caller_node_epoch: 1,
            outcome: DfsHixlReleaseOutcome::Completed as i32,
            content_validated: true,
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
    async fn negotiate_rejects_old_protocol_before_authorization() {
        let (service, _provider, authorizer) = service();
        let mut request = negotiate_request(read_request("old-version", "fixture-grant"));
        request.protocol_version = 1;

        let error = service
            .negotiate("reader".into(), request, authorizer.clone())
            .await
            .expect_err("HIXL v1 must be rejected");

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
        request.outcome = DfsHixlReleaseOutcome::NotSubmitted as i32;
        assert!(
            service
                .release("reader".into(), request.clone())
                .await
                .is_err(),
            "never-submitted cannot claim validated content"
        );
        request.content_validated = false;
        assert!(
            service
                .release("reader".into(), request.clone())
                .await
                .unwrap()
                .into_inner()
                .consumed
        );
        assert!(
            service
                .release("reader".into(), request)
                .await
                .unwrap()
                .into_inner()
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
        let mut mismatched =
            group_prepare_request(session.session_id, &batch, &[(0, 2, "window-0")]);
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
        unknown.outcome = DfsHixlReleaseOutcome::Unknown as i32;
        let unknown_reply = service
            .release("reader".into(), unknown)
            .await
            .expect("unknown release")
            .into_inner();
        assert!(!unknown_reply.consumed);
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
        assert!(released.consumed);
        let replay = service
            .release("reader".into(), release_request(&first))
            .await
            .expect("idempotent release")
            .into_inner();
        assert!(replay.already_released);
        let mut mismatched = release_request(&first);
        mismatched.content_validated = false;
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
        release.outcome = DfsHixlReleaseOutcome::Failed as i32;
        release.content_validated = false;
        let reply = service
            .release("reader".into(), release)
            .await
            .unwrap()
            .into_inner();
        assert!(!reply.consumed);
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
            attempt_id: "attempt".into(),
            source_node_epoch: 9,
            caller_node_epoch: 3,
            engine_id: "engine".into(),
            engine_generation: 11,
            pool_generation: 11,
            slot_id: 13,
            slot_generation: 18,
            ..DfsReleaseHixlReadRequest::default()
        };
        assert!(validate_release_identity(&window, &request).is_err());
    }
}
