//! Authenticated DFS source-window control for the optional HIXL rH2H path.
//!
//! This module owns only the control-plane lease. HixlRuntime owns the
//! fixed ACL Host memory and the native request state; the registry below
//! keeps the AFS grant/session/window identity that makes a runtime export
//! eligible for a remote READ. In particular, closing a session only stops
//! admission. It never claims that an accepted native request has drained.

use std::cell::Cell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use afs_protocol::node_data::{
    DfsCloseHixlReply, DfsCloseHixlRequest, DfsDigestAlgorithm, DfsHixlLane,
    DfsHixlPreparedGroupWindow, DfsHixlReadRange, DfsHixlReleaseOutcome, DfsHixlReleaseWindow,
    DfsHixlReleaseWindowResult, DfsHixlTransport, DfsNegotiateHixlReply, DfsNegotiateHixlRequest,
    DfsPrepareReadRangesHixlGroupReply, DfsPrepareReadRangesHixlGroupRequest,
    DfsPrepareReadRangesHixlReply, DfsReadRangesCompletion, DfsReadRangesRequest,
    DfsReleaseHixlReadReply, DfsReleaseHixlReadRequest,
};
use afs_transport::grpc::error_status::{coded_status, error_to_status};
use afs_transport::hixl::{EngineInfo, ExportWindow, HixlError, HixlRuntimeGroup, ReadTransport};
use tonic::{Response, Status};

use super::snapshot::{PackedRangeDigest, read_hixl_snapshot_ranges, validate_packed_dfs_read};
use super::{DfsReadAuthorizer, dfs_read_caller_epoch, validate_dfs_read_request};
use crate::node::chunk::LocalChunkStore;
use crate::node::dfs_read::{MAX_DFS_GROUP_BYTES, MAX_DFS_READ_BYTES, MAX_DFS_READ_OPS};
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
    #[cfg(test)]
    before_source_read: Arc<Mutex<Option<SourceReadHook>>>,
}

#[cfg(test)]
type SourceReadHook = Box<dyn FnOnce() + Send>;

impl HixlReadService {
    /// Creates the Node-side source-window service for one native HIXL group.
    pub fn new(runtime: Arc<HixlRuntimeGroup>, node_id: String, node_epoch: u64) -> Self {
        Self::from_provider(runtime_export_provider(runtime), node_id, node_epoch)
    }

    pub(crate) fn from_provider(
        provider: Arc<dyn ExportProvider>,
        node_id: String,
        node_epoch: u64,
    ) -> Self {
        Self {
            provider,
            node_id,
            node_epoch,
            registry: Arc::new(Mutex::new(Registry::default())),
            metrics: None,
            #[cfg(test)]
            before_source_read: Arc::new(Mutex::new(None)),
        }
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

    /// Runs one blocking control step off the async runtime and reports it
    /// into the bounded metrics.
    async fn spawn_phase<T, F>(&self, phase: Operation, work: F) -> Result<T, Status>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, Status> + Send + 'static,
    {
        let started = Instant::now();
        let result = tokio::task::spawn_blocking(work)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        self.observe(phase, started, result.is_ok());
        result
    }

    /// Stops admission for expired sessions and quarantines their pending windows.
    /// It never recycles an export on the strength of elapsed time.
    pub async fn expire(&self) -> Result<(), Status> {
        let service = self.clone();
        self.spawn_phase(Operation::Expire, move || {
            let mut registry = service.lock_registry()?;
            registry.expire_sessions();
            service.quarantine_pending_windows(&mut registry, false)
        })
        .await
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
                let mut registry = service.lock_registry()?;
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
        self.spawn_phase(Operation::Negotiate, move || {
            service.negotiate_blocking(&peer, request, authorizer)
        })
        .await
    }

    /// Prepares every ordered window of one logical group in a single control
    /// round trip. The source verifies the parent batch once, pins one private
    /// snapshot, and exports each window from that snapshot. Either all windows
    /// are admitted or previously pinned exports are rolled back. Replay of the
    /// same read/attempt returns the existing windows.
    pub async fn prepare_group(
        &self,
        peer: String,
        request: DfsPrepareReadRangesHixlGroupRequest,
        authorizer: Arc<dyn DfsReadAuthorizer>,
        local: Option<Arc<LocalChunkStore>>,
    ) -> Result<Response<DfsPrepareReadRangesHixlGroupReply>, Status> {
        let service = self.clone();
        self.spawn_phase(Operation::Prepare, move || {
            service.prepare_group_blocking(&peer, request, authorizer, local)
        })
        .await
    }

    /// Releases one known terminal window. A duplicate release is answered by
    /// the bounded tombstone table. Unknown native outcomes remain pinned.
    pub async fn release(
        &self,
        peer: String,
        request: DfsReleaseHixlReadRequest,
    ) -> Result<Response<DfsReleaseHixlReadReply>, Status> {
        let service = self.clone();
        self.spawn_phase(Operation::Release, move || {
            service.release_blocking(&peer, request)
        })
        .await
    }

    /// Closes admission for a session. It deliberately does not drain or
    /// release still-live export windows.
    pub async fn close(
        &self,
        peer: String,
        request: DfsCloseHixlRequest,
    ) -> Result<Response<DfsCloseHixlReply>, Status> {
        let service = self.clone();
        self.spawn_phase(Operation::Close, move || {
            service.close_blocking(&peer, request)
        })
        .await
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
        validate_client_engine(
            request.protocol_version,
            &request.client_engine_id,
            request.client_engine_generation,
        )?;
        if let Some(read) = read {
            self.authorize(authorizer.as_ref(), peer, read)?;
        }

        let info = self.provider.info();
        let (parameters, lanes) = negotiated_lanes(self.provider.as_ref(), request.lane_count)?;
        let (transport, buffer_pool_count, buffer_pool_size_mib, hccs_domain_id) = parameters;
        validate_transport_request(
            request.transport,
            request.buffer_pool_count,
            request.buffer_pool_size_mib,
            request.hccs_domain_id,
            parameters,
        )?;
        let export_slots = effective_export_slots(self.provider.export_slots(0))?;
        let window_bytes = u64::try_from(info.window_bytes).map_err(|_| {
            coded_status(
                afs_error::NODE_TRANSFER_INTERNAL,
                "HIXL window size exceeds protocol range",
            )
        })?;
        let mut registry = self.lock_registry()?;
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
                lanes: self.provider.lane_infos(),
                closed: false,
                last_used: Instant::now(),
                windows: HashMap::new(),
                tombstones: Tombstones::default(),
                prepared_batch: None,
            },
        );
        Ok(Response::new(DfsNegotiateHixlReply {
            session_id,
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
        // A batch beyond either legacy limit is a group: a small group may
        // still carry more operations than a legacy batch admits.
        let total_bytes = batch
            .operations
            .iter()
            .try_fold(0_u64, |sum, operation| sum.checked_add(operation.length));
        let group_batch = batch.operations.len() > MAX_DFS_READ_OPS
            || total_bytes.is_some_and(|total| total > MAX_DFS_READ_BYTES);
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
        let batch_length = usize::try_from(
            total_bytes
                .ok_or_else(|| Status::invalid_argument("HIXL logical batch length overflow"))?,
        )
        .map_err(|_| Status::invalid_argument("HIXL logical batch length overflow"))?;
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
        let group = PreparedGroup {
            session_id: request.session_id,
            peer,
            caller_epoch,
            source_epoch: request.source_node_epoch,
            batch: &batch,
            batch_length,
            group_batch,
            lane_id,
            lane_wire_id: first_lane,
            export_slots,
            windows: &plans,
        };
        let rollback_failed = Cell::new(false);
        let outcome = self.prepare_group_windows(&group, &local, &rollback_failed);
        if rollback_failed.get() {
            let mut registry = self.lock_registry()?;
            self.quarantine_pending_windows(&mut registry, true)?;
        }
        Ok(Response::new(DfsPrepareReadRangesHixlGroupReply {
            session_id: request.session_id,
            windows: outcome?,
        }))
    }

    /// Admits one group in three steps so the registry lock never covers
    /// source I/O: reserve the snapshot budget and export slots under the
    /// lock; read, hash and export without it; then commit under the lock
    /// after checking that admission, the session and the snapshot are still
    /// the ones the reservation saw. A refused commit rolls back its exports.
    fn prepare_group_windows(
        &self,
        group: &PreparedGroup<'_>,
        local: &LocalChunkStore,
        rollback_failed: &Cell<bool>,
    ) -> Result<Vec<DfsHixlPreparedGroupWindow>, Status> {
        let admission = {
            let mut registry = self.lock_registry()?;
            registry.expire_sessions();
            self.quarantine_pending_windows(&mut registry, false)?;
            match self.admit_group(&mut registry, group)? {
                Admitted::Replay(replies) => return Ok(replies),
                Admitted::New(admission) => admission,
            }
        };
        let created_snapshot = admission.snapshot.is_none();
        let snapshot = match &admission.snapshot {
            Some(snapshot) => snapshot.clone(),
            None => {
                #[cfg(test)]
                if let Some(hook) = self
                    .before_source_read
                    .lock()
                    .ok()
                    .and_then(|mut hook| hook.take())
                {
                    hook();
                }
                let started = Instant::now();
                // Both budgets bound the snapshot before any source byte is
                // read, so CRC32C ranges fill it directly like BLAKE3 ranges.
                let packed = read_hixl_snapshot_ranges(
                    local,
                    group.batch,
                    if group.group_batch {
                        MAX_HIXL_GROUP_BATCH_BYTES
                    } else {
                        MAX_HIXL_LEGACY_BATCH_BYTES
                    },
                );
                self.observe(Operation::SourceRead, started, packed.is_ok());
                let (bytes, completions) = packed?;
                Snapshot {
                    request: Arc::new(group.batch.clone()),
                    range_digests: Arc::new(batch_range_checksums(group.batch, &completions)),
                    bytes: Arc::new(bytes),
                }
            }
        };
        // Pending exports roll back on drop until the commit admits them.
        let mut pending = Vec::with_capacity(group.windows.len());
        let mut replies = Vec::with_capacity(group.windows.len());
        for plan in group.windows {
            let (reply, export) = self.export_window(
                group,
                plan,
                &snapshot,
                admission.lane_info.as_ref(),
                local,
                rollback_failed,
            )?;
            pending.push(export);
            replies.push(reply);
        }
        let mut registry = self.lock_registry()?;
        admission.reservation.settle(&mut registry);
        let committed =
            self.commit_group(&mut registry, group, created_snapshot, snapshot, &replies)?;
        if let Some(replay) = committed {
            return Ok(replay);
        }
        pending.into_iter().for_each(PendingExport::disarm);
        Ok(replies
            .into_iter()
            .zip(group.windows)
            .map(|(reply, plan)| DfsHixlPreparedGroupWindow {
                batch_offset: plan.offset,
                lane_id: group.lane_wire_id,
                reply: Some(reply),
            })
            .collect())
    }

    /// The locked admission step. A complete replay still precedes closed,
    /// frozen, draining and capacity refusals.
    fn admit_group(
        &self,
        registry: &mut Registry,
        group: &PreparedGroup<'_>,
    ) -> Result<Admitted, Status> {
        let admission_frozen = registry.admission_frozen || registry.draining;
        let lane_windows = registry.lane_window_count(group.lane_id)
            + registry
                .reserved_windows
                .get(&group.lane_id)
                .copied()
                .unwrap_or(0);
        let prepared_bytes = registry.prepared_batch_bytes() + registry.reserved_snapshot_bytes;
        let session = registry.session_mut(
            group.session_id,
            group.peer,
            group.caller_epoch,
            group.source_epoch,
        )?;
        session.last_used = Instant::now();
        if let Some(replay) = replay_group(session, group)? {
            return Ok(Admitted::Replay(replay));
        }
        if session.closed {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "HIXL session is closed for new windows",
            ));
        }
        if admission_frozen {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "HIXL engine generation is quarantined",
            ));
        }
        if lane_windows + group.windows.len() > group.export_slots {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
                "HIXL export-window capacity is exhausted",
            ));
        }
        let first_offset = group.windows[0].offset;
        let snapshot = match &session.prepared_batch {
            Some(prepared) => {
                let next = usize::try_from(first_offset).map_err(|_| {
                    Status::internal("HIXL group offset exceeds the source address space")
                })?;
                if prepared.request.as_ref() != group.batch || prepared.next_offset != next {
                    return Err(coded_status(
                        afs_error::NODE_TRANSFER_INVALID,
                        "HIXL batch identity changed or group is out of order",
                    ));
                }
                Some(Snapshot {
                    request: prepared.request.clone(),
                    bytes: prepared.bytes.clone(),
                    range_digests: prepared.range_digests.clone(),
                })
            }
            None => {
                if first_offset != 0 {
                    return Err(coded_status(
                        afs_error::NODE_TRANSFER_INVALID,
                        "HIXL group must start at offset zero for a new snapshot",
                    ));
                }
                if group.batch_length
                    > snapshot_budget(group.group_batch).saturating_sub(prepared_bytes)
                {
                    return Err(coded_status(
                        afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
                        "HIXL verified batch snapshot budget is exhausted",
                    ));
                }
                None
            }
        };
        let lane_info = session.lanes.get(group.lane_id).cloned();
        let snapshot_bytes = if snapshot.is_none() {
            group.batch_length
        } else {
            0
        };
        Ok(Admitted::New(Admission {
            snapshot,
            lane_info,
            reservation: Reservation::hold(
                &self.registry,
                registry,
                group.lane_id,
                group.windows.len(),
                snapshot_bytes,
            ),
        }))
    }

    /// Exports one window of the snapshot outside the registry lock.
    fn export_window<'a>(
        &'a self,
        group: &PreparedGroup<'_>,
        plan: &GroupWindowPlan,
        snapshot: &Snapshot,
        lane_info: Option<&EngineInfo>,
        local: &LocalChunkStore,
        rollback_failed: &'a Cell<bool>,
    ) -> Result<(DfsPrepareReadRangesHixlReply, PendingExport<'a>), Status> {
        let start = usize::try_from(plan.offset)
            .map_err(|_| Status::internal("HIXL group offset overflow"))?;
        let end = usize::try_from(plan.end)
            .map_err(|_| Status::internal("HIXL group offset overflow"))?;
        let window = snapshot.bytes.get(start..end).ok_or_else(|| {
            coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "HIXL group window exceeds the verified batch",
            )
        })?;
        let completions = window_completions(
            &plan.read,
            window,
            &snapshot.range_digests,
            local,
            self.metrics.as_ref(),
        )?;
        let length = end - start;
        let export = self
            .provider
            .prepare_export_shared(
                group.lane_id,
                Arc::clone(&snapshot.bytes),
                start,
                length,
                Instant::now() + HIXL_SESSION_TTL,
            )
            .map_err(runtime_status)?;
        let export = PendingExport::new(
            self.provider.as_ref(),
            group.lane_id,
            export,
            rollback_failed,
        );
        let info = self
            .provider
            .lane_infos()
            .get(group.lane_id)
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
            || export.window.slot_id >= group.export_slots as u64
            || export.window.generation == 0
            || packed_length > capacity
            || export.window.address.checked_add(packed_length).is_none()
        {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INTERNAL,
                "HIXL runtime returned an invalid export descriptor",
            ));
        }
        if lane_info != Some(&info) {
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
        let reply = DfsPrepareReadRangesHixlReply {
            session_id: group.session_id,
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
        };
        Ok((reply, export))
    }

    /// The locked commit step. `Ok(Some)` is an identical prepare that won
    /// while this one worked unlocked; the caller rolls back its exports.
    fn commit_group(
        &self,
        registry: &mut Registry,
        group: &PreparedGroup<'_>,
        created_snapshot: bool,
        snapshot: Snapshot,
        replies: &[DfsPrepareReadRangesHixlReply],
    ) -> Result<Option<Vec<DfsHixlPreparedGroupWindow>>, Status> {
        let admission_frozen = registry.admission_frozen || registry.draining;
        let session = registry.session_mut(
            group.session_id,
            group.peer,
            group.caller_epoch,
            group.source_epoch,
        )?;
        if let Some(replay) = replay_group(session, group)? {
            return Ok(Some(replay));
        }
        if session.closed {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "HIXL session is closed for new windows",
            ));
        }
        if admission_frozen {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "HIXL engine generation is quarantined",
            ));
        }
        let first_offset = usize::try_from(group.windows[0].offset)
            .map_err(|_| Status::internal("HIXL group offset overflow"))?;
        let unchanged = match (created_snapshot, &session.prepared_batch) {
            (true, None) => true,
            (false, Some(prepared)) => {
                Arc::ptr_eq(&prepared.bytes, &snapshot.bytes)
                    && prepared.next_offset == first_offset
            }
            _ => false,
        };
        if !unchanged {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "HIXL batch identity changed or group is out of order",
            ));
        }
        for (plan, reply) in group.windows.iter().zip(replies) {
            session.windows.insert(
                WindowKey::from_read(&plan.read),
                WindowRecord {
                    request: plan.read.clone(),
                    reply: reply.clone(),
                    batch_request: snapshot.request.clone(),
                    batch_offset: plan.offset,
                    lane_id: group.lane_id,
                },
            );
        }
        let end = group.windows.last().map_or(0, |plan| plan.end);
        let end = usize::try_from(end).map_err(|_| Status::internal("HIXL group end overflow"))?;
        match &mut session.prepared_batch {
            Some(prepared) => prepared.next_offset = end,
            None => {
                session.prepared_batch = Some(PreparedBatch {
                    request: snapshot.request,
                    bytes: snapshot.bytes,
                    next_offset: end,
                    range_digests: snapshot.range_digests,
                });
            }
        }
        session.last_used = Instant::now();
        Ok(None)
    }

    fn lock_registry(&self) -> Result<std::sync::MutexGuard<'_, Registry>, Status> {
        self.registry
            .lock()
            .map_err(|_| Status::internal("HIXL registry lock is poisoned"))
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
        let mut registry = self.lock_registry()?;
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
                if outcome == DfsHixlReleaseOutcome::Unknown {
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
        let mut registry = self.lock_registry()?;
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
        Ok(Response::new(DfsCloseHixlReply { drained }))
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
        // A frozen registry whose runtime quarantine failed keeps retrying it,
        // even when the trigger was a forced one with no closed session.
        let retry = registry.admission_frozen && !registry.runtime_quarantined;
        if !force && !retry && !registry.has_closed_pending_windows() {
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
    fn prepare_export_shared(
        &self,
        lane: usize,
        data: Arc<Vec<u8>>,
        offset: usize,
        length: usize,
        deadline: Instant,
    ) -> Result<ExportWindow, HixlError>;
    fn release_export(&self, lane: usize, slot_id: u64, generation: u64) -> Result<(), HixlError>;
    fn quarantine(&self) -> Result<(), HixlError>;
}

pub(super) struct PendingExport<'a> {
    provider: &'a dyn ExportProvider,
    window: ExportWindow,
    lane_id: usize,
    rollback_failed: &'a Cell<bool>,
    armed: bool,
}

impl<'a> PendingExport<'a> {
    pub(super) fn new(
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

    pub(super) fn disarm(mut self) {
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

/// The production export boundary shared by the DFS and OwnerFs sources.
pub(super) fn runtime_export_provider(runtime: Arc<HixlRuntimeGroup>) -> Arc<dyn ExportProvider> {
    Arc::new(RuntimeExportProvider { runtime })
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
    /// Export slots per lane held by groups that are reading or exporting
    /// outside the lock.
    reserved_windows: HashMap<usize, usize>,
    /// New snapshot bytes held by those groups.
    reserved_snapshot_bytes: usize,
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
        next_session_id(&mut self.next_session, "HIXL session id space exhausted")
    }

    /// Admitted windows plus the exports of groups still being prepared.
    fn active_window_count(&self) -> usize {
        self.sessions
            .values()
            .map(|session| session.windows.len())
            .sum::<usize>()
            + self.reserved_windows.values().sum::<usize>()
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
    range_digests: Arc<HashMap<(String, u64, u64), PackedRangeDigest>>,
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

/// One validated group prepare request.
struct PreparedGroup<'a> {
    session_id: u64,
    peer: &'a str,
    caller_epoch: u64,
    source_epoch: u64,
    batch: &'a DfsReadRangesRequest,
    batch_length: usize,
    group_batch: bool,
    lane_id: usize,
    lane_wire_id: u32,
    export_slots: usize,
    windows: &'a [GroupWindowPlan],
}

/// The verified parent batch bytes that a group's windows are exported from.
#[derive(Clone)]
struct Snapshot {
    request: Arc<DfsReadRangesRequest>,
    bytes: Arc<Vec<u8>>,
    range_digests: Arc<HashMap<(String, u64, u64), PackedRangeDigest>>,
}

enum Admitted {
    Replay(Vec<DfsHixlPreparedGroupWindow>),
    New(Admission),
}

/// What the locked admission step granted to the unlocked step.
struct Admission {
    /// `None` when this group must read a new snapshot.
    snapshot: Option<Snapshot>,
    lane_info: Option<EngineInfo>,
    reservation: Reservation,
}

/// Export slots and snapshot bytes held for a group between its admission and
/// its commit, so concurrent prepares cannot overcommit while it works
/// unlocked. Dropping it takes the registry lock; under the lock, `settle`.
struct Reservation {
    registry: Option<Arc<Mutex<Registry>>>,
    lane_id: usize,
    windows: usize,
    snapshot_bytes: usize,
}

impl Reservation {
    fn hold(
        owner: &Arc<Mutex<Registry>>,
        registry: &mut Registry,
        lane_id: usize,
        windows: usize,
        snapshot_bytes: usize,
    ) -> Self {
        *registry.reserved_windows.entry(lane_id).or_default() += windows;
        registry.reserved_snapshot_bytes += snapshot_bytes;
        Self {
            registry: Some(owner.clone()),
            lane_id,
            windows,
            snapshot_bytes,
        }
    }

    fn settle(mut self, registry: &mut Registry) {
        self.registry = None;
        self.give_back(registry);
    }

    fn give_back(&self, registry: &mut Registry) {
        if let Some(reserved) = registry.reserved_windows.get_mut(&self.lane_id) {
            *reserved = reserved.saturating_sub(self.windows);
            if *reserved == 0 {
                registry.reserved_windows.remove(&self.lane_id);
            }
        }
        registry.reserved_snapshot_bytes = registry
            .reserved_snapshot_bytes
            .saturating_sub(self.snapshot_bytes);
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if let Some(owner) = self.registry.take() {
            match owner.lock() {
                Ok(mut registry) => self.give_back(&mut registry),
                Err(poisoned) => self.give_back(&mut poisoned.into_inner()),
            }
        }
    }
}

/// Answers a complete replay of admitted windows. A partial replay, a replay
/// whose identity differs, or a released window is refused.
fn replay_group(
    session: &Session,
    group: &PreparedGroup<'_>,
) -> Result<Option<Vec<DfsHixlPreparedGroupWindow>>, Status> {
    let mut replay = Vec::with_capacity(group.windows.len());
    for plan in group.windows {
        let key = WindowKey::from_read(&plan.read);
        if let Some(existing) = session.windows.get(&key) {
            if existing.request != plan.read
                || existing.batch_request.as_ref() != group.batch
                || existing.batch_offset != plan.offset
                || existing.lane_id != group.lane_id
            {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "HIXL prepare replay identity does not match the original request",
                ));
            }
            replay.push(DfsHixlPreparedGroupWindow {
                batch_offset: plan.offset,
                lane_id: group.lane_wire_id,
                reply: Some(existing.reply.clone()),
            });
        } else if session.tombstones.contains(&key) {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "HIXL read window was already released",
            ));
        }
    }
    match replay.len() {
        0 => Ok(None),
        replayed if replayed == group.windows.len() => Ok(Some(replay)),
        _ => Err(coded_status(
            afs_error::NODE_TRANSFER_INVALID,
            "HIXL group replay is partial",
        )),
    }
}

/// One validated native window of a group request. All offsets are absolute
/// in the verified parent batch.
struct GroupWindowPlan {
    read: DfsReadRangesRequest,
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

pub(super) type TransportParameters = (DfsHixlTransport, u32, u32, Option<u64>);

pub(super) fn negotiated_transport(info: &EngineInfo) -> Result<TransportParameters, Status> {
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

pub(super) fn validate_client_engine(
    protocol_version: u32,
    engine_id: &str,
    generation: u64,
) -> Result<(), Status> {
    if protocol_version != HIXL_PROTOCOL_VERSION {
        return Err(coded_status(
            afs_error::NODE_TRANSFER_UNSUPPORTED,
            "HIXL protocol version is unsupported",
        ));
    }
    if engine_id.is_empty() || engine_id.len() > 512 || generation == 0 {
        return Err(coded_status(
            afs_error::NODE_TRANSFER_INVALID,
            "HIXL client engine identity is invalid",
        ));
    }
    Ok(())
}

/// The transport parameters and lane list this source offers a reader with
/// `lane_count` lanes. Every local lane must agree on them.
pub(super) fn negotiated_lanes(
    provider: &dyn ExportProvider,
    lane_count: u32,
) -> Result<(TransportParameters, Vec<DfsHixlLane>), Status> {
    let info = provider.info();
    let lane_infos = provider.lane_infos();
    if lane_infos.is_empty()
        || lane_infos.len() > 16
        || usize::try_from(lane_count).ok() != Some(lane_infos.len())
    {
        return Err(coded_status(
            afs_error::NODE_TRANSFER_INVALID,
            "HIXL source and reader lane counts must match",
        ));
    }
    let parameters = negotiated_transport(&info)?;
    let mut lanes = Vec::with_capacity(lane_infos.len());
    for (index, lane) in lane_infos.iter().enumerate() {
        if negotiated_transport(lane)? != parameters
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
            export_slots: effective_export_slots(provider.export_slots(index))?,
        });
    }
    Ok((parameters, lanes))
}

pub(super) fn validate_transport_request(
    transport: i32,
    buffer_pool_count: u32,
    buffer_pool_size_mib: u32,
    hccs_domain_id: Option<u64>,
    expected: TransportParameters,
) -> Result<(), Status> {
    let requested = DfsHixlTransport::try_from(transport).map_err(|_| {
        coded_status(
            afs_error::NODE_TRANSFER_INVALID,
            "HIXL transport is unknown",
        )
    })?;
    if (
        requested,
        buffer_pool_count,
        buffer_pool_size_mib,
        hccs_domain_id,
    ) != expected
    {
        return Err(coded_status(
            afs_error::NODE_TRANSFER_INVALID,
            "HIXL transport or buffer-pool parameters do not match",
        ));
    }
    Ok(())
}

pub(super) fn effective_export_slots(slots: usize) -> Result<u32, Status> {
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

pub(super) fn runtime_status(error: HixlError) -> Status {
    error_to_status(crate::node::rpc::hixl::runtime_error(error))
}

/// Allocates the next control-session id, refusing to wrap.
pub(super) fn next_session_id(counter: &mut u64, message: &'static str) -> Result<u64, Status> {
    let id = (*counter).max(1);
    *counter = id
        .checked_add(1)
        .ok_or_else(|| coded_status(afs_error::NODE_TRANSFER_UNAVAILABLE, message))?;
    Ok(id)
}

pub(crate) fn unsupported_status(message: &'static str) -> Status {
    coded_status(afs_error::NODE_TRANSFER_UNSUPPORTED, message)
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

    /// Forgets every session as if the source had expired and evicted it.
    pub(crate) fn forget_sessions(service: &HixlReadService) {
        service
            .registry
            .lock()
            .expect("test source registry lock")
            .sessions
            .clear();
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
