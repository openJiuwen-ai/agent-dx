//! Home-side OwnerFs HIXL read-ahead windows.
//!
//! A session is only a transport lease opened by an authenticated peer that
//! presented one valid RootAccess. Every prepare carries its own RootAccess
//! and open handle and runs the same Home validation as `OwnerFiles.Read`
//! before any byte is read. The prepared bytes are copied into fixed HIXL
//! export slots that stay immutable until the reader releases them. Unknown
//! native outcomes, unreleased slots of a closed session and drain timeouts
//! freeze admission and keep the shared runtime quarantined, exactly like the
//! DFS source registry.

use std::cell::Cell;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use afs_protocol::node_data::{
    DfsHixlLane, DfsHixlReleaseOutcome, DfsHixlTransport, OwnerCloseHixlReply,
    OwnerCloseHixlRequest, OwnerHixlSlot, OwnerNegotiateHixlReply, OwnerNegotiateHixlRequest,
    OwnerPrepareReadHixlReply, OwnerPrepareReadHixlRequest, OwnerReleaseHixlReadReply,
    OwnerReleaseHixlReadRequest,
};
use afs_transport::grpc::error_status::{coded_status, error_to_status};
use afs_transport::hixl::{EngineInfo, HixlRuntimeGroup};
use tonic::{Response, Status};

use super::hixl::{
    ExportProvider, PendingExport, effective_export_slots, negotiated_transport,
    runtime_export_provider, runtime_status,
};
use super::{presented_access, remote_file_for_handle};
use crate::node::rpc::OwnerRpcMetrics;
use crate::node::rpc::hixl::{HIXL_PROTOCOL_VERSION, HixlMetrics, Operation};
use crate::node::vfs::ownerfs::OwnerFsPeerExecutor;
use crate::node::vfs::ownerfs::files::RemoteFile;
use crate::node::vfs::ownerfs::root::PresentedRootAccess;

const MAX_OWNER_HIXL_SESSIONS: usize = 64;
const MAX_OWNER_HIXL_RELEASE_TOMBSTONES: usize = 256;
const MAX_OWNER_HIXL_READ_ID_BYTES: usize = 256;
const OWNER_HIXL_SESSION_TTL: Duration = Duration::from_secs(30);
/// One prepare exports at most this many bytes; it matches the reader's
/// `ownerfs_window_bytes` upper bound.
pub(crate) const MAX_OWNER_HIXL_READ_BYTES: u64 = 64 * 1024 * 1024;
/// Private staging copies that exist between the source read and the export.
const MAX_OWNER_HIXL_STAGING_BYTES: usize = 256 * 1024 * 1024;
const OWNER_HIXL_EXPORT_DEADLINE: Duration = Duration::from_secs(5);

/// The Home file boundary used by prepare. Production delegates to the
/// OwnerFs peer executor so authorization stays identical to `Read`.
pub(crate) trait OwnerWindowSource: Send + Sync {
    fn authorize_transport(
        &self,
        peer: &str,
        access: &PresentedRootAccess,
    ) -> afs_error::Result<()>;
    fn read(
        &self,
        peer: &str,
        access: &PresentedRootAccess,
        file: &RemoteFile,
        offset: u64,
        out: &mut [u8],
    ) -> afs_error::Result<usize>;
}

impl OwnerWindowSource for OwnerFsPeerExecutor {
    fn authorize_transport(
        &self,
        peer: &str,
        access: &PresentedRootAccess,
    ) -> afs_error::Result<()> {
        OwnerFsPeerExecutor::authorize_transport(self, peer, access)
    }

    fn read(
        &self,
        peer: &str,
        access: &PresentedRootAccess,
        file: &RemoteFile,
        offset: u64,
        out: &mut [u8],
    ) -> afs_error::Result<usize> {
        OwnerFsPeerExecutor::read(self, peer, access, file, offset, out)
    }
}

/// Bounded OwnerFs window registry over the shared HIXL export provider.
#[derive(Clone)]
pub struct OwnerHixlService {
    provider: Arc<dyn ExportProvider>,
    source: Arc<dyn OwnerWindowSource>,
    registry: Arc<Mutex<Registry>>,
    staging: Arc<AtomicUsize>,
    metrics: Option<HixlMetrics>,
    owner_metrics: Option<OwnerRpcMetrics>,
}

impl OwnerHixlService {
    /// Creates the Home window service over the process HIXL runtime group.
    pub fn new(runtime: Arc<HixlRuntimeGroup>, executor: OwnerFsPeerExecutor) -> Self {
        Self::from_parts(runtime_export_provider(runtime), Arc::new(executor))
    }

    pub(crate) fn from_parts(
        provider: Arc<dyn ExportProvider>,
        source: Arc<dyn OwnerWindowSource>,
    ) -> Self {
        Self {
            provider,
            source,
            registry: Arc::new(Mutex::new(Registry::default())),
            staging: Arc::new(AtomicUsize::new(0)),
            metrics: None,
            owner_metrics: None,
        }
    }

    pub fn with_metrics(mut self, metrics: HixlMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn with_owner_metrics(mut self, metrics: OwnerRpcMetrics) -> Self {
        self.owner_metrics = Some(metrics);
        self
    }

    fn observe(&self, phase: Operation, started: Instant, success: bool) {
        if let Some(metrics) = &self.metrics {
            metrics.observe(phase, started.elapsed(), success);
        }
    }

    /// Closes idle sessions and quarantines slots that a closed session left
    /// unreleased. Elapsed time never recycles an export.
    pub async fn expire(&self) -> Result<(), Status> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut registry = service.lock_registry()?;
            registry.expire_sessions();
            service.quarantine_pending(&mut registry, false)
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?
    }

    /// Stops admission and waits for authenticated releases until `timeout`.
    /// A timeout keeps every pending slot and quarantines the runtime.
    pub async fn drain(&self, timeout: Duration) -> Result<(), Status> {
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "OwnerFs HIXL drain deadline overflow",
            )
        })?;
        loop {
            let service = self.clone();
            let pending = tokio::task::spawn_blocking(move || {
                let mut registry = service.lock_registry()?;
                registry.draining = true;
                let pending = registry.active_slot_count();
                if pending != 0 && Instant::now() >= deadline {
                    service.quarantine_pending(&mut registry, true)?;
                    return Err(coded_status(
                        afs_error::NODE_TRANSFER_DRAIN_INCOMPLETE,
                        "OwnerFs HIXL drain deadline exceeded; pending slots retained",
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

    pub async fn negotiate(
        &self,
        peer: String,
        request: OwnerNegotiateHixlRequest,
    ) -> Result<Response<OwnerNegotiateHixlReply>, Status> {
        let service = self.clone();
        let started = Instant::now();
        let result =
            tokio::task::spawn_blocking(move || service.negotiate_blocking(&peer, request))
                .await
                .map_err(|error| Status::internal(error.to_string()))?;
        self.observe(Operation::Negotiate, started, result.is_ok());
        result.map(Response::new)
    }

    pub async fn prepare(
        &self,
        peer: String,
        request: OwnerPrepareReadHixlRequest,
    ) -> Result<Response<OwnerPrepareReadHixlReply>, Status> {
        let service = self.clone();
        let started = Instant::now();
        let result = tokio::task::spawn_blocking(move || service.prepare_blocking(&peer, request))
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        self.observe(Operation::Prepare, started, result.is_ok());
        if let Some(metrics) = &self.owner_metrics {
            metrics.observe("server", "OwnerFiles.PrepareReadHixl", started.elapsed());
            if let Ok(reply) = &result {
                metrics.record_payload("server", "read", "hixl", reply.read);
            }
        }
        result.map(Response::new)
    }

    pub async fn release(
        &self,
        peer: String,
        request: OwnerReleaseHixlReadRequest,
    ) -> Result<Response<OwnerReleaseHixlReadReply>, Status> {
        let service = self.clone();
        let started = Instant::now();
        let result = tokio::task::spawn_blocking(move || service.release_blocking(&peer, request))
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        self.observe(Operation::Release, started, result.is_ok());
        result.map(Response::new)
    }

    pub async fn close(
        &self,
        peer: String,
        request: OwnerCloseHixlRequest,
    ) -> Result<Response<OwnerCloseHixlReply>, Status> {
        let service = self.clone();
        let started = Instant::now();
        let result = tokio::task::spawn_blocking(move || service.close_blocking(&peer, request))
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        self.observe(Operation::Close, started, result.is_ok());
        result.map(Response::new)
    }

    fn lock_registry(&self) -> Result<std::sync::MutexGuard<'_, Registry>, Status> {
        self.registry
            .lock()
            .map_err(|_| Status::internal("OwnerFs HIXL registry lock is poisoned"))
    }

    fn negotiate_blocking(
        &self,
        peer: &str,
        request: OwnerNegotiateHixlRequest,
    ) -> Result<OwnerNegotiateHixlReply, Status> {
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
        let access = presented_access(request.access.clone()).map_err(error_to_status)?;
        let started = Instant::now();
        let authorized = self.source.authorize_transport(peer, &access);
        self.observe(Operation::Authorize, started, authorized.is_ok());
        authorized.map_err(error_to_status)?;

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
                export_slots: effective_export_slots(self.provider.export_slots(index))?,
            });
        }
        let (transport, buffer_pool_count, buffer_pool_size_mib, hccs_domain_id) = parameters;
        let requested = DfsHixlTransport::try_from(request.transport).map_err(|_| {
            coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "HIXL transport is unknown",
            )
        })?;
        if requested != transport
            || request.buffer_pool_count != buffer_pool_count
            || request.buffer_pool_size_mib != buffer_pool_size_mib
            || request.hccs_domain_id != hccs_domain_id
        {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "HIXL transport or buffer-pool parameters do not match",
            ));
        }

        let mut registry = self.lock_registry()?;
        registry.expire_sessions();
        self.quarantine_pending(&mut registry, false)?;
        if registry.admission_frozen || registry.draining {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "HIXL engine generation is quarantined or draining",
            ));
        }
        if registry.sessions.len() >= MAX_OWNER_HIXL_SESSIONS {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
                "OwnerFs HIXL session capacity is exhausted",
            ));
        }
        let session_id = registry.next_session_id()?;
        registry.sessions.insert(
            session_id,
            Session {
                peer: peer.to_owned(),
                home_session_id: access.home_session_id.clone(),
                lanes: lane_infos,
                closed: false,
                last_used: Instant::now(),
                reads: HashMap::new(),
                tombstones: Tombstones::default(),
            },
        );
        Ok(OwnerNegotiateHixlReply {
            session_id,
            home_session_id: access.home_session_id,
            protocol_version: HIXL_PROTOCOL_VERSION,
            transport: transport as i32,
            buffer_pool_count,
            buffer_pool_size_mib,
            hccs_domain_id,
            lanes,
        })
    }

    fn prepare_blocking(
        &self,
        peer: &str,
        request: OwnerPrepareReadHixlRequest,
    ) -> Result<OwnerPrepareReadHixlReply, Status> {
        if request.session_id == 0
            || request.read_id.is_empty()
            || request.read_id.len() > MAX_OWNER_HIXL_READ_ID_BYTES
            || request.length == 0
            || request.length > MAX_OWNER_HIXL_READ_BYTES
            || request.offset.checked_add(request.length).is_none()
        {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "OwnerFs HIXL prepare identity or range is invalid",
            ));
        }
        let access = presented_access(request.access.clone()).map_err(error_to_status)?;
        let handle = request.handle.clone().ok_or_else(|| {
            coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "OwnerFs HIXL prepare is missing its open handle",
            )
        })?;
        let file = remote_file_for_handle(&access, handle.opaque);
        let length = usize::try_from(request.length).map_err(|_| {
            coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "OwnerFs HIXL prepare length overflows",
            )
        })?;
        let lane_id = usize::try_from(request.lane_id).map_err(|_| {
            coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "OwnerFs HIXL lane overflows",
            )
        })?;

        let window_bytes = {
            let mut registry = self.lock_registry()?;
            registry.expire_sessions();
            self.quarantine_pending(&mut registry, false)?;
            let (replay, window_bytes) = {
                let session = registry.admitting_session(request.session_id, peer)?;
                if session.home_session_id != access.home_session_id {
                    return Err(Status::permission_denied(
                        "OwnerFs HIXL prepare belongs to another Home session",
                    ));
                }
                let window_bytes = session
                    .lanes
                    .get(lane_id)
                    .map(|lane| lane.window_bytes)
                    .filter(|bytes| *bytes != 0)
                    .ok_or_else(|| {
                        coded_status(
                            afs_error::NODE_TRANSFER_INVALID,
                            "OwnerFs HIXL lane is not negotiated",
                        )
                    })?;
                if session.tombstones.contains(&request.read_id) {
                    return Err(coded_status(
                        afs_error::NODE_TRANSFER_INVALID,
                        "OwnerFs HIXL read was already released",
                    ));
                }
                let replay = match session.reads.get(&request.read_id) {
                    Some(record) if record.request == request => Some(record.reply.clone()),
                    Some(_) => {
                        return Err(coded_status(
                            afs_error::NODE_TRANSFER_INVALID,
                            "OwnerFs HIXL replay differs from the admitted prepare",
                        ));
                    }
                    None => {
                        session.last_used = Instant::now();
                        let needed = length.div_ceil(window_bytes);
                        if needed > self.provider.export_slots(lane_id) {
                            return Err(coded_status(
                                afs_error::NODE_TRANSFER_INVALID,
                                "OwnerFs HIXL prepare exceeds the lane export pool",
                            ));
                        }
                        None
                    }
                };
                (replay, window_bytes)
            };
            if let Some(reply) = replay {
                drop(registry);
                // Replay still proves the grant and handle are current.
                self.authorize_read(peer, &access, &file)?;
                return Ok(reply);
            }
            let needed = length.div_ceil(window_bytes);
            if registry.lane_slot_count(lane_id).saturating_add(needed)
                > self.provider.export_slots(lane_id)
            {
                return Err(exhausted("OwnerFs HIXL export slots are in use"));
            }
            window_bytes
        };

        let _staging = StagingLease::reserve(&self.staging, length)?;
        let started = Instant::now();
        let staged = self.read_source(peer, &access, &file, request.offset, length);
        self.observe(Operation::SourceRead, started, staged.is_ok());
        let staged = staged?;
        let read = staged.len();
        let eof = read < length;
        if read == 0 {
            return Ok(OwnerPrepareReadHixlReply {
                session_id: request.session_id,
                read_id: request.read_id,
                lane_id: request.lane_id,
                engine_id: String::new(),
                engine_generation: 0,
                read: 0,
                eof,
                slots: Vec::new(),
            });
        }
        let checksums = slot_checksums(&staged, window_bytes);
        let staged = Arc::new(staged);

        let mut registry = self.lock_registry()?;
        registry.expire_sessions();
        self.quarantine_pending(&mut registry, false)?;
        if registry.admission_frozen || registry.draining {
            return Err(unavailable());
        }
        let lane = registry
            .admitting_session(request.session_id, peer)?
            .lanes
            .get(lane_id)
            .cloned()
            .ok_or_else(|| Status::internal("OwnerFs HIXL lane disappeared"))?;
        // A transport retry may have raced this attempt; the identical
        // request that won keeps its slots and this copy is never exported.
        match registry
            .admitting_session(request.session_id, peer)?
            .reads
            .get(&request.read_id)
        {
            Some(record) if record.request == request => return Ok(record.reply.clone()),
            Some(_) => {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "OwnerFs HIXL replay differs from the admitted prepare",
                ));
            }
            None => {}
        }
        let deadline = Instant::now() + OWNER_HIXL_EXPORT_DEADLINE;
        let rollback_failed = Cell::new(false);
        let mut pending = Vec::with_capacity(checksums.len());
        let mut slots = Vec::with_capacity(checksums.len());
        for (index, checksum) in checksums.into_iter().enumerate() {
            let offset = index * window_bytes;
            let slot_length = (read - offset).min(window_bytes);
            let exported = self.provider.prepare_export_shared(
                lane_id,
                staged.clone(),
                offset,
                slot_length,
                deadline,
            );
            let window = match exported {
                Ok(window) => window,
                Err(error) => {
                    drop(pending);
                    if rollback_failed.get() {
                        self.quarantine_pending(&mut registry, true)?;
                    }
                    return Err(runtime_status(error));
                }
            };
            if window.length != slot_length || window.address == 0 || window.generation == 0 {
                pending.push(PendingExport::new(
                    self.provider.as_ref(),
                    lane_id,
                    window,
                    &rollback_failed,
                ));
                drop(pending);
                if rollback_failed.get() {
                    self.quarantine_pending(&mut registry, true)?;
                }
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_INTERNAL,
                    "HIXL export returned an inconsistent window",
                ));
            }
            slots.push(OwnerHixlSlot {
                slot_id: window.slot_id,
                slot_generation: window.generation,
                address: window.address,
                length: slot_length as u64,
                checksum,
            });
            pending.push(PendingExport::new(
                self.provider.as_ref(),
                lane_id,
                window,
                &rollback_failed,
            ));
        }
        let reply = OwnerPrepareReadHixlReply {
            session_id: request.session_id,
            read_id: request.read_id.clone(),
            lane_id: request.lane_id,
            engine_id: lane.engine_id.clone(),
            engine_generation: lane.generation,
            read: read as u64,
            eof,
            slots,
        };
        for export in pending {
            export.disarm();
        }
        let session = registry.admitting_session(request.session_id, peer)?;
        session.last_used = Instant::now();
        session.reads.insert(
            request.read_id.clone(),
            ReadRecord {
                request,
                reply: reply.clone(),
                lane_id,
            },
        );
        Ok(reply)
    }

    fn authorize_read(
        &self,
        peer: &str,
        access: &PresentedRootAccess,
        file: &RemoteFile,
    ) -> Result<(), Status> {
        let started = Instant::now();
        let result = self.source.read(peer, access, file, 0, &mut []);
        self.observe(Operation::Authorize, started, result.is_ok());
        result.map(|_| ()).map_err(error_to_status)
    }

    /// Reads until `length` bytes or EOF. Each call revalidates the grant and
    /// open handle, exactly like a sequence of `OwnerFiles.Read` calls.
    fn read_source(
        &self,
        peer: &str,
        access: &PresentedRootAccess,
        file: &RemoteFile,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, Status> {
        let mut staged = Vec::new();
        staged
            .try_reserve_exact(length)
            .map_err(|_| exhausted("OwnerFs HIXL staging allocation failed"))?;
        staged.resize(length, 0);
        let mut filled = 0;
        while filled < length {
            let position = offset
                .checked_add(filled as u64)
                .ok_or_else(|| Status::internal("OwnerFs HIXL source offset overflow"))?;
            let read = self
                .source
                .read(peer, access, file, position, &mut staged[filled..])
                .map_err(error_to_status)?;
            if read == 0 {
                break;
            }
            if read > length - filled {
                return Err(Status::internal("OwnerFs source read exceeded its buffer"));
            }
            filled += read;
        }
        staged.truncate(filled);
        Ok(staged)
    }

    fn release_blocking(
        &self,
        peer: &str,
        request: OwnerReleaseHixlReadRequest,
    ) -> Result<OwnerReleaseHixlReadReply, Status> {
        let outcome = DfsHixlReleaseOutcome::try_from(request.outcome).map_err(|_| {
            coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "HIXL release outcome is unknown",
            )
        })?;
        if request.session_id == 0
            || request.read_id.is_empty()
            || request.engine_generation == 0
            || outcome == DfsHixlReleaseOutcome::Unspecified
            || (outcome == DfsHixlReleaseOutcome::NotSubmitted && request.content_validated)
        {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "OwnerFs HIXL release identity is incomplete",
            ));
        }
        let mut registry = self.lock_registry()?;
        registry.expire_sessions();
        self.quarantine_pending(&mut registry, false)?;
        let mut release_error = None;
        let mut pinned = false;
        let reply = {
            let session = registry.session_mut(request.session_id, peer)?;
            session.last_used = Instant::now();
            if session.tombstones.matches(&request) {
                OwnerReleaseHixlReadReply {
                    consumed: true,
                    already_released: true,
                }
            } else {
                let record = session.reads.get(&request.read_id).ok_or_else(|| {
                    coded_status(
                        afs_error::NODE_TRANSFER_INVALID,
                        "OwnerFs HIXL release does not identify an admitted read",
                    )
                })?;
                if record.reply.engine_generation != request.engine_generation {
                    return Err(Status::permission_denied(
                        "OwnerFs HIXL release engine identity does not match",
                    ));
                }
                if matches!(
                    outcome,
                    DfsHixlReleaseOutcome::Unknown | DfsHixlReleaseOutcome::Failed
                ) {
                    pinned = true;
                    OwnerReleaseHixlReadReply::default()
                } else {
                    let lane_id = record.lane_id;
                    let slots = record.reply.slots.clone();
                    for slot in &slots {
                        if let Err(error) = self.provider.release_export(
                            lane_id,
                            slot.slot_id,
                            slot.slot_generation,
                        ) {
                            release_error = Some(runtime_status(error));
                            pinned = true;
                            break;
                        }
                    }
                    if release_error.is_none() {
                        session.reads.remove(&request.read_id);
                        session.tombstones.insert(&request);
                        OwnerReleaseHixlReadReply {
                            consumed: true,
                            already_released: false,
                        }
                    } else {
                        OwnerReleaseHixlReadReply::default()
                    }
                }
            }
        };
        if pinned {
            self.quarantine_pending(&mut registry, true)?;
        }
        if let Some(status) = release_error {
            return Err(status);
        }
        Ok(reply)
    }

    fn close_blocking(
        &self,
        peer: &str,
        request: OwnerCloseHixlRequest,
    ) -> Result<OwnerCloseHixlReply, Status> {
        if request.session_id == 0 {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "OwnerFs HIXL close identity is incomplete",
            ));
        }
        let mut registry = self.lock_registry()?;
        registry.expire_sessions();
        self.quarantine_pending(&mut registry, false)?;
        let drained = {
            let session = registry.session_mut(request.session_id, peer)?;
            session.closed = true;
            session.last_used = Instant::now();
            session.reads.is_empty()
        };
        self.quarantine_pending(&mut registry, false)?;
        Ok(OwnerCloseHixlReply {
            admitted: true,
            drained,
        })
    }

    fn quarantine_pending(&self, registry: &mut Registry, force: bool) -> Result<(), Status> {
        if !force && !registry.has_closed_pending_slots() {
            return Ok(());
        }
        registry.admission_frozen = true;
        if registry.runtime_quarantined {
            return Ok(());
        }
        self.provider.quarantine().map_err(runtime_status)?;
        registry.runtime_quarantined = true;
        Ok(())
    }
}

/// Hashes each export slot independently on the shared HIXL hash pool.
fn slot_checksums(bytes: &[u8], window_bytes: usize) -> Vec<Vec<u8>> {
    let chunks = bytes.chunks(window_bytes).collect::<Vec<_>>();
    let digests = Mutex::new(vec![Vec::new(); chunks.len()]);
    let hash = |index: usize| {
        let digest = blake3::hash(chunks[index]).as_bytes().to_vec();
        match digests.lock() {
            Ok(mut digests) => digests[index] = digest,
            Err(poisoned) => poisoned.into_inner()[index] = digest,
        }
    };
    match crate::node::rpc::peer::hixl::validation_pool() {
        Some(pool) if chunks.len() > 1 => pool.install(|| {
            rayon_core::scope(|scope| {
                for index in 0..chunks.len() {
                    scope.spawn(move |_| hash(index));
                }
            });
        }),
        _ => (0..chunks.len()).for_each(hash),
    }
    digests
        .into_inner()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn exhausted(message: &'static str) -> Status {
    coded_status(afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED, message)
}

fn unavailable() -> Status {
    coded_status(
        afs_error::NODE_TRANSFER_UNAVAILABLE,
        "OwnerFs HIXL admission is quarantined or draining",
    )
}

pub(crate) fn unsupported_status() -> Status {
    coded_status(
        afs_error::NODE_TRANSFER_UNSUPPORTED,
        "OwnerFs HIXL read-ahead capability is unavailable",
    )
}

/// Reserves private staging bytes before any source read.
struct StagingLease<'a> {
    counter: &'a AtomicUsize,
    bytes: usize,
}

impl<'a> StagingLease<'a> {
    fn reserve(counter: &'a AtomicUsize, bytes: usize) -> Result<Self, Status> {
        counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|total| *total <= MAX_OWNER_HIXL_STAGING_BYTES)
            })
            .map_err(|_| exhausted("OwnerFs HIXL staging budget is exhausted"))?;
        Ok(Self { counter, bytes })
    }
}

impl Drop for StagingLease<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.bytes, Ordering::AcqRel);
    }
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
            if now.duration_since(session.last_used) >= OWNER_HIXL_SESSION_TTL {
                session.closed = true;
            }
            session.tombstones.expire(now);
        }
        self.sessions.retain(|_, session| {
            !(session.closed
                && session.reads.is_empty()
                && session.tombstones.is_empty()
                && now.duration_since(session.last_used) >= OWNER_HIXL_SESSION_TTL)
        });
    }

    fn has_closed_pending_slots(&self) -> bool {
        self.sessions
            .values()
            .any(|session| session.closed && !session.reads.is_empty())
    }

    fn next_session_id(&mut self) -> Result<u64, Status> {
        let id = self.next_session.max(1);
        self.next_session = id.checked_add(1).ok_or_else(|| {
            coded_status(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "OwnerFs HIXL session id space exhausted",
            )
        })?;
        Ok(id)
    }

    fn active_slot_count(&self) -> usize {
        self.sessions
            .values()
            .flat_map(|session| session.reads.values())
            .map(|read| read.reply.slots.len())
            .sum()
    }

    fn lane_slot_count(&self, lane: usize) -> usize {
        self.sessions
            .values()
            .flat_map(|session| session.reads.values())
            .filter(|read| read.lane_id == lane)
            .map(|read| read.reply.slots.len())
            .sum()
    }

    fn session_mut(&mut self, session_id: u64, peer: &str) -> Result<&mut Session, Status> {
        let session = self.sessions.get_mut(&session_id).ok_or_else(|| {
            coded_status(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "unknown OwnerFs HIXL session",
            )
        })?;
        if session.peer != peer {
            return Err(Status::permission_denied(
                "OwnerFs HIXL session belongs to another peer",
            ));
        }
        Ok(session)
    }

    /// A session that may still admit a new prepare.
    fn admitting_session(&mut self, session_id: u64, peer: &str) -> Result<&mut Session, Status> {
        if self.admission_frozen || self.draining {
            return Err(unavailable());
        }
        let session = self.session_mut(session_id, peer)?;
        if session.closed {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "OwnerFs HIXL session is closed",
            ));
        }
        Ok(session)
    }
}

struct Session {
    peer: String,
    home_session_id: String,
    lanes: Vec<EngineInfo>,
    closed: bool,
    last_used: Instant,
    reads: HashMap<String, ReadRecord>,
    tombstones: Tombstones,
}

struct ReadRecord {
    request: OwnerPrepareReadHixlRequest,
    reply: OwnerPrepareReadHixlReply,
    lane_id: usize,
}

struct ReleaseTombstone {
    engine_generation: u64,
    outcome: i32,
    content_validated: bool,
    inserted_at: Instant,
}

#[derive(Default)]
struct Tombstones {
    by_read: HashMap<String, ReleaseTombstone>,
    order: VecDeque<String>,
}

impl Tombstones {
    fn is_empty(&self) -> bool {
        self.by_read.is_empty()
    }

    fn contains(&self, read_id: &str) -> bool {
        self.by_read.contains_key(read_id)
    }

    fn expire(&mut self, now: Instant) {
        while let Some(read_id) = self.order.front() {
            let expired = self.by_read.get(read_id).is_none_or(|entry| {
                now.duration_since(entry.inserted_at) >= OWNER_HIXL_SESSION_TTL
            });
            if !expired {
                break;
            }
            if let Some(read_id) = self.order.pop_front() {
                self.by_read.remove(&read_id);
            }
        }
    }

    fn matches(&self, request: &OwnerReleaseHixlReadRequest) -> bool {
        self.by_read.get(&request.read_id).is_some_and(|entry| {
            entry.engine_generation == request.engine_generation
                && entry.outcome == request.outcome
                && entry.content_validated == request.content_validated
        })
    }

    fn insert(&mut self, request: &OwnerReleaseHixlReadRequest) {
        if self.by_read.contains_key(&request.read_id) {
            return;
        }
        self.by_read.insert(
            request.read_id.clone(),
            ReleaseTombstone {
                engine_generation: request.engine_generation,
                outcome: request.outcome,
                content_validated: request.content_validated,
                inserted_at: Instant::now(),
            },
        );
        self.order.push_back(request.read_id.clone());
        while self.order.len() > MAX_OWNER_HIXL_RELEASE_TOMBSTONES {
            if let Some(oldest) = self.order.pop_front() {
                self.by_read.remove(&oldest);
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;
