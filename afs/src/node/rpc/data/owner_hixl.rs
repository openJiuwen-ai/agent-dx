//! Home-side OwnerFs HIXL read-ahead windows and write tickets.
//!
//! A session is only a transport lease opened by an authenticated peer that
//! presented one valid RootAccess. Every prepare carries its own RootAccess
//! and open handle and runs the same Home validation as `OwnerFiles.Read`
//! before any byte is read. The prepared bytes are copied into fixed HIXL
//! export slots that stay immutable until the reader releases them. Unknown
//! native outcomes, unreleased slots of a closed session and drain timeouts
//! freeze admission and keep the shared runtime quarantined.
//! Write tickets reuse those sessions and the same export pool.

use std::cell::Cell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use afs_protocol::node_data::{
    FileIdentity, HixlReleaseOutcome, OwnerBatchFile as WireBatchFile, OwnerCloseHixlReply,
    OwnerCloseHixlRequest, OwnerHixlSlot, OwnerNegotiateHixlReply, OwnerNegotiateHixlRequest,
    OwnerPrepareBatchHixlReply, OwnerPrepareBatchHixlRequest, OwnerPrepareReadHixlReply,
    OwnerPrepareReadHixlRequest, OwnerReleaseHixlReadReply, OwnerReleaseHixlReadRequest,
};
use afs_transport::grpc::error_status::{coded_status, error_to_status};
use afs_transport::hixl::{EngineInfo, HixlRuntimeGroup, MAX_BATCH_WINDOWS};
use tonic::{Response, Status};

use super::hixl::{
    negotiated_lanes, next_session_id, runtime_status, validate_client_engine,
    validate_transport_request,
};
use super::{presented_access, remote_file_for_handle, time_ns};
use crate::config::MAX_OWNERFS_HIXL_READ_GROUP_BYTES;
use crate::node::data_plane::hixl::{
    ExportProvider, HixlMetrics, Operation, PendingExport, SlotChecksum, runtime_export_provider,
};
use crate::node::rpc::OwnerRpcMetrics;
use crate::node::rpc::hixl::HIXL_PROTOCOL_VERSION;
use crate::node::vfs::ownerfs::files::RemoteFile;
use crate::node::vfs::ownerfs::root::{PresentedRootAccess, RootId};
use crate::node::vfs::ownerfs::{
    OwnerFsPeerExecutor, PeerBatchDirectory, PeerBatchDirectoryVersion, PeerBatchFile,
};

mod write;

const MAX_OWNER_HIXL_SESSIONS: usize = 64;
const MAX_OWNER_HIXL_RELEASE_TOMBSTONES: usize = 256;
const MAX_OWNER_HIXL_READ_ID_BYTES: usize = 256;
const OWNER_HIXL_SESSION_TTL: Duration = Duration::from_secs(30);
const OWNER_HIXL_EXPORT_DEADLINE: Duration = Duration::from_secs(5);
/// Bounds the Home source pool that reads the slots of a window concurrently.
const MAX_OWNER_SOURCE_THREADS: usize = 8;
/// Files one batch prepare packs at most; bounds the reply size.
const MAX_OWNER_HIXL_BATCH_FILES: usize = 1024;
/// Maximum entries in a complete HIXL directory snapshot.
pub(crate) const MAX_OWNER_HIXL_BATCH_SCAN: usize = 4096;
/// Longest parent path or anchor name a batch prepare accepts (Linux PATH_MAX).
const MAX_OWNER_HIXL_BATCH_PATH_BYTES: usize = 4096;
/// Snapshots retained by one service (shared by its clones).
const MAX_OWNER_HIXL_BATCH_SNAPSHOTS: usize = 8;
/// Sum of name vector and per-name allocations allowed during collection.
const MAX_OWNER_HIXL_BATCH_SNAPSHOT_BYTES: usize = 1024 * 1024;

/// The Home file boundary used by prepare and commit. Production delegates to the
/// OwnerFs peer executor so each transfer uses the regular file-operation authority.
pub(crate) trait OwnerWindowSource: Send + Sync {
    fn authorize_transport(
        &self,
        peer: &str,
        access: &PresentedRootAccess,
    ) -> afs_error::Result<()>;
    fn authorize_write(
        &self,
        peer: &str,
        access: &PresentedRootAccess,
        file: &RemoteFile,
    ) -> afs_error::Result<()>;
    fn write(
        &self,
        peer: &str,
        access: &PresentedRootAccess,
        file: &RemoteFile,
        offset: u64,
        data: &[u8],
        options: crate::node::vfs::types::WriteOptions,
    ) -> afs_error::Result<usize>;

    fn read(
        &self,
        peer: &str,
        access: &PresentedRootAccess,
        file: &RemoteFile,
        offset: u64,
        out: &mut [u8],
    ) -> afs_error::Result<usize>;
    /// The same read for a caller that copies concurrently: implementations
    /// should not serialize it behind the open handle.
    fn read_positional(
        &self,
        peer: &str,
        access: &PresentedRootAccess,
        file: &RemoteFile,
        offset: u64,
        out: &mut [u8],
    ) -> afs_error::Result<usize> {
        self.read(peer, access, file, offset, out)
    }

    /// Opens a root-relative directory for a batch prepare after validating
    /// the grant for reading. The directory keeps the root admitted.
    fn batch_directory(
        &self,
        peer: &str,
        access: &PresentedRootAccess,
        parent: &OsStr,
    ) -> afs_error::Result<Box<dyn OwnerBatchDirectory>>;
}

/// Identity and version of one regular file a batch packs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BatchEntry {
    pub(crate) identity: Vec<u8>,
    pub(crate) size: u64,
    pub(crate) mtime_ns: u64,
    pub(crate) ctime_ns: u64,
}

/// One Home directory of a batch prepare.
pub(crate) trait OwnerBatchDirectory: Send + Sync {
    /// Identity and change timestamps of the open directory.
    fn version(&self) -> afs_error::Result<PeerBatchDirectoryVersion>;
    /// Complete entry names in byte order, or `None` on budget exhaustion.
    fn names_bounded(
        &self,
        max_entries: usize,
        max_bytes: usize,
    ) -> afs_error::Result<Option<Vec<OsString>>>;
    /// The entry without following a final symlink; `None` unless it is a
    /// regular file.
    fn stat(&self, name: &OsStr) -> afs_error::Result<Option<BatchEntry>>;
    /// Opens a regular file read-only without following a final symlink.
    fn open(&self, name: &OsStr) -> afs_error::Result<Box<dyn OwnerBatchSource>>;
}

/// One file opened by a batch prepare.
pub(crate) trait OwnerBatchSource: Send {
    /// The version of this open, from its `fstat`.
    fn entry(&self) -> &BatchEntry;
    fn read_at(&self, offset: u64, out: &mut [u8]) -> afs_error::Result<usize>;
}

impl OwnerWindowSource for OwnerFsPeerExecutor {
    fn authorize_transport(
        &self,
        peer: &str,
        access: &PresentedRootAccess,
    ) -> afs_error::Result<()> {
        OwnerFsPeerExecutor::authorize_transport(self, peer, access)
    }

    fn authorize_write(
        &self,
        peer: &str,
        access: &PresentedRootAccess,
        file: &RemoteFile,
    ) -> afs_error::Result<()> {
        self.authorize_data_write(peer, access, file)
    }

    fn write(
        &self,
        peer: &str,
        access: &PresentedRootAccess,
        file: &RemoteFile,
        offset: u64,
        data: &[u8],
        options: crate::node::vfs::types::WriteOptions,
    ) -> afs_error::Result<usize> {
        self.write_with_options(peer, access, file, offset, data, options)
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

    fn read_positional(
        &self,
        peer: &str,
        access: &PresentedRootAccess,
        file: &RemoteFile,
        offset: u64,
        out: &mut [u8],
    ) -> afs_error::Result<usize> {
        // The duplicated descriptor keeps the copy off the open-handle lock.
        OwnerFsPeerExecutor::peer_read_positional(self, peer, access, file, offset, out)
    }

    fn batch_directory(
        &self,
        peer: &str,
        access: &PresentedRootAccess,
        parent: &OsStr,
    ) -> afs_error::Result<Box<dyn OwnerBatchDirectory>> {
        Ok(Box::new(self.peer_batch_directory(peer, access, parent)?))
    }
}

fn wire_entry(entry: &crate::node::vfs::ownerfs::PeerBatchEntry) -> BatchEntry {
    BatchEntry {
        identity: entry.identity.0.clone(),
        size: entry.size,
        mtime_ns: time_ns(entry.mtime),
        ctime_ns: time_ns(entry.ctime),
    }
}

impl OwnerBatchDirectory for PeerBatchDirectory {
    fn version(&self) -> afs_error::Result<PeerBatchDirectoryVersion> {
        PeerBatchDirectory::version(self)
    }

    fn names_bounded(
        &self,
        max_entries: usize,
        max_bytes: usize,
    ) -> afs_error::Result<Option<Vec<OsString>>> {
        PeerBatchDirectory::names_bounded(self, max_entries, max_bytes)
    }

    fn stat(&self, name: &OsStr) -> afs_error::Result<Option<BatchEntry>> {
        Ok(PeerBatchDirectory::stat(self, name)?
            .as_ref()
            .map(wire_entry))
    }

    fn open(&self, name: &OsStr) -> afs_error::Result<Box<dyn OwnerBatchSource>> {
        let file = PeerBatchDirectory::open(self, name)?;
        let entry = wire_entry(file.entry());
        Ok(Box::new(ExecutorBatchSource { file, entry }))
    }
}

struct ExecutorBatchSource {
    file: PeerBatchFile,
    entry: BatchEntry,
}

impl OwnerBatchSource for ExecutorBatchSource {
    fn entry(&self) -> &BatchEntry {
        &self.entry
    }

    fn read_at(&self, offset: u64, out: &mut [u8]) -> afs_error::Result<usize> {
        self.file.read_at(offset, out)
    }
}

/// Bounded OwnerFs window registry over the shared HIXL export provider.
#[derive(Clone)]
pub struct OwnerHixlService {
    provider: Arc<dyn ExportProvider>,
    source: Arc<dyn OwnerWindowSource>,
    transfers: Arc<HomeTransfers>,
    batch_snapshots: Arc<Mutex<VecDeque<BatchDirectorySnapshot>>>,
    metrics: Option<HixlMetrics>,
    owner_metrics: Option<OwnerRpcMetrics>,
    trusted_peers: Option<Arc<HashSet<String>>>,
    buffered_writes: bool,
    /// Slot algorithms from the most recent negotiation offer.
    #[cfg(test)]
    offered_checksums: Arc<Mutex<Vec<i32>>>,
}

#[derive(Default)]
struct HomeTransfers {
    registry: Mutex<Registry>,
    preparing: AtomicUsize,
    writes_changed: Condvar,
}

struct BatchDirectorySnapshot {
    root: RootId,
    parent: OsString,
    epoch: u64,
    generation: u64,
    home_session: String,
    version: PeerBatchDirectoryVersion,
    // None records an over-budget directory at this version.
    names: Option<Arc<Vec<OsString>>>,
}

impl BatchDirectorySnapshot {
    fn belongs_to(&self, access: &PresentedRootAccess, parent: &OsStr) -> bool {
        self.root == access.id && self.parent.as_os_str() == parent
    }

    fn matches(&self, access: &PresentedRootAccess, version: PeerBatchDirectoryVersion) -> bool {
        self.epoch == access.epoch
            && self.generation == access.access_generation
            && self.home_session == access.home_session_id
            && self.version == version
    }
}

struct PreparingExport<'a>(&'a AtomicUsize);

impl Drop for PreparingExport<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
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
            transfers: Arc::new(HomeTransfers::default()),
            batch_snapshots: Arc::new(Mutex::new(VecDeque::new())),
            metrics: None,
            owner_metrics: None,
            trusted_peers: None,
            buffered_writes: cfg!(test),
            #[cfg(test)]
            offered_checksums: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Slot algorithms the most recent negotiation offered.
    #[cfg(test)]
    pub(crate) fn negotiated_slot_checksums(&self) -> Vec<i32> {
        self.offered_checksums
            .lock()
            .ok()
            .map(|offer| offer.clone())
            .unwrap_or_default()
    }

    pub(crate) fn with_policy(mut self, trusted_peers: &[String], buffered_writes: bool) -> Self {
        self.trusted_peers = Some(Arc::new(trusted_peers.iter().cloned().collect()));
        self.buffered_writes = buffered_writes;
        self
    }

    pub(super) fn check_peer(&self, peer: &str) -> Result<(), Status> {
        if self
            .trusted_peers
            .as_ref()
            .is_some_and(|peers| !peers.contains(peer))
        {
            return Err(unsupported_status(
                "peer is not enabled for HIXL registered slots",
            ));
        }
        Ok(())
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

    /// Closes idle sessions and quarantines slots that a closed session left
    /// unreleased. Elapsed time never recycles an export.
    pub async fn expire(&self) -> Result<(), Status> {
        let service = self.clone();
        self.spawn_phase(Operation::Expire, move || {
            let mut registry = service.lock_registry()?;
            registry.expire_sessions();
            service.quarantine_pending(&mut registry, false)
        })
        .await
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
        self.drain_until(deadline).await
    }

    /// Drains Home exports and admitted Prepare workers within the Node's budget.
    pub(crate) async fn drain_until(&self, deadline: Instant) -> Result<(), Status> {
        loop {
            let service = self.clone();
            let pending = tokio::task::spawn_blocking(move || {
                let mut registry = service.lock_registry()?;
                registry.draining = true;
                let pending = registry.active_slot_count() != 0
                    || service.transfers.preparing.load(Ordering::Acquire) != 0;
                if pending && Instant::now() >= deadline {
                    let message = match service.quarantine_pending(&mut registry, true) {
                        Ok(()) => "OwnerFs HIXL drain deadline exceeded; pending slots retained".into(),
                        Err(error) => format!("OwnerFs HIXL drain deadline exceeded; runtime quarantine failed: {error}"),
                    };
                    return Err(coded_status(
                        afs_error::NODE_TRANSFER_DRAIN_INCOMPLETE,
                        message,
                    ));
                }
                Ok(pending)
            })
            .await
            .map_err(|error| Status::internal(error.to_string()))??;
            if !pending {
                return Ok(());
            }
            tokio::time::sleep(
                Duration::from_millis(10).min(deadline.saturating_duration_since(Instant::now())),
            )
            .await;
        }
    }

    pub async fn negotiate(
        &self,
        peer: String,
        request: OwnerNegotiateHixlRequest,
    ) -> Result<Response<OwnerNegotiateHixlReply>, Status> {
        let service = self.clone();
        self.spawn_phase(Operation::Negotiate, move || {
            service.negotiate_blocking(&peer, request)
        })
        .await
        .map(Response::new)
    }

    pub async fn prepare(
        &self,
        peer: String,
        request: OwnerPrepareReadHixlRequest,
    ) -> Result<Response<OwnerPrepareReadHixlReply>, Status> {
        let service = self.clone();
        let started = Instant::now();
        let result = self
            .spawn_phase(Operation::Prepare, move || {
                service.prepare_blocking(&peer, request)
            })
            .await;
        if let Some(metrics) = &self.owner_metrics {
            metrics.observe("server", "OwnerFiles.PrepareReadHixl", started.elapsed());
            if let Ok(reply) = &result {
                metrics.record_payload("server", "read", "hixl", reply.read);
            }
        }
        result.map(Response::new)
    }

    pub async fn prepare_batch(
        &self,
        peer: String,
        request: OwnerPrepareBatchHixlRequest,
    ) -> Result<Response<OwnerPrepareBatchHixlReply>, Status> {
        let service = self.clone();
        let started = Instant::now();
        let result = self
            .spawn_phase(Operation::Prepare, move || {
                service.prepare_batch_blocking(&peer, request)
            })
            .await;
        if let Some(metrics) = &self.owner_metrics {
            metrics.observe("server", "OwnerFiles.PrepareBatchHixl", started.elapsed());
            if let Ok(reply) = &result {
                metrics.record_payload("server", "read", "hixl", reply.bytes);
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
        self.spawn_phase(Operation::Release, move || {
            service.release_blocking(&peer, request)
        })
        .await
        .map(Response::new)
    }

    pub async fn close(
        &self,
        peer: String,
        request: OwnerCloseHixlRequest,
    ) -> Result<Response<OwnerCloseHixlReply>, Status> {
        let service = self.clone();
        self.spawn_phase(Operation::Close, move || {
            service.close_blocking(&peer, request)
        })
        .await
        .map(Response::new)
    }

    fn lock_registry(&self) -> Result<std::sync::MutexGuard<'_, Registry>, Status> {
        self.transfers
            .registry
            .lock()
            .map_err(|_| Status::internal("OwnerFs HIXL registry lock is poisoned"))
    }

    /// Register a new Prepare while holding the same lock that freezes admission.
    fn track_prepare(
        &self,
        _admission: &std::sync::MutexGuard<'_, Registry>,
    ) -> PreparingExport<'_> {
        self.transfers.preparing.fetch_add(1, Ordering::Relaxed);
        PreparingExport(&self.transfers.preparing)
    }

    fn negotiate_blocking(
        &self,
        peer: &str,
        request: OwnerNegotiateHixlRequest,
    ) -> Result<OwnerNegotiateHixlReply, Status> {
        validate_client_engine(
            request.protocol_version,
            &request.client_engine_id,
            request.client_engine_generation,
        )?;
        let access = presented_access(request.access.clone()).map_err(error_to_status)?;
        let started = Instant::now();
        let authorized = self.source.authorize_transport(peer, &access);
        self.observe(Operation::Authorize, started, authorized.is_ok());
        authorized.map_err(error_to_status)?;

        let (parameters, lanes) = negotiated_lanes(self.provider.as_ref(), request.lane_count)?;
        let (transport, buffer_pool_count, buffer_pool_size_mib, hccs_domain_id) = parameters;
        validate_transport_request(
            request.transport,
            request.buffer_pool_count,
            request.buffer_pool_size_mib,
            request.hccs_domain_id,
            parameters,
        )?;
        let checksum = SlotChecksum::negotiate(&request.slot_checksum_algorithms);
        #[cfg(test)]
        if let Ok(mut offer) = self.offered_checksums.lock() {
            offer.clone_from(&request.slot_checksum_algorithms);
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
        if registry.sessions.values().filter(|s| !s.closed).count() >= MAX_OWNER_HIXL_SESSIONS {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
                "OwnerFs HIXL session capacity is exhausted",
            ));
        }
        // Keep recent Close/Release replies replayable without charging drained
        // sessions to the live quota. IDs never repeat, so an evicted request
        // fails closed; never evict a session that still owns native slots.
        if registry.sessions.len() >= 2 * MAX_OWNER_HIXL_SESSIONS {
            let oldest = registry
                .sessions
                .iter()
                .filter(|(_, s)| s.closed && !s.has_pending_slots())
                .min_by_key(|(_, s)| s.last_used)
                .map(|(&id, _)| id)
                .ok_or_else(|| {
                    coded_status(
                        afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
                        "OwnerFs HIXL sessions are still draining",
                    )
                })?;
            registry.sessions.remove(&oldest);
        }
        let session_id = registry.next_session_id()?;
        registry.sessions.insert(
            session_id,
            Session {
                peer: peer.to_owned(),
                home_session_id: access.home_session_id.clone(),
                lanes: self.provider.lane_infos(),
                closed: false,
                last_used: Instant::now(),
                reads: HashMap::new(),
                writes: HashMap::new(),
                cancelled_successors: HashMap::new(),
                tombstones: Tombstones::default(),
                checksum,
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
            slot_checksum_algorithm: checksum.wire() as i32,
            buffered_write_supported: self.buffered_writes,
        })
    }

    fn prepare_registry(
        &self,
        peer: &str,
        session_id: u64,
        read_context: Option<(&str, Option<&OwnerReleaseHixlReadRequest>)>,
    ) -> Result<std::sync::MutexGuard<'_, Registry>, Status> {
        let mut registry = self.lock_registry()?;
        registry.expire_sessions();
        self.quarantine_pending(&mut registry, false)?;
        if let Some((home_session_id, prior_release)) = read_context {
            registry.bound_session(session_id, peer, home_session_id)?;
            self.apply_prior_release(&mut registry, peer, session_id, prior_release)?;
        }
        Ok(registry)
    }

    fn check_prepare_capacity(
        &self,
        registry: &Registry,
        lane_id: usize,
        slots: usize,
        invalid: &'static str,
    ) -> Result<(), Status> {
        let export_slots = self.provider.export_slots(lane_id);
        if slots == 0 || slots > export_slots.min(MAX_BATCH_WINDOWS) {
            return Err(coded_status(afs_error::NODE_TRANSFER_INVALID, invalid));
        }
        if registry.lane_slot_count(lane_id).saturating_add(slots) > export_slots {
            return Err(exhausted("OwnerFs HIXL export slots are in use"));
        }
        Ok(())
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
            || request.length > MAX_OWNERFS_HIXL_READ_GROUP_BYTES as u64
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

        let (window_bytes, checksum, admitted_at, _preparing) = {
            let mut registry = self.prepare_registry(
                peer,
                request.session_id,
                Some((&access.home_session_id, request.prior_release.as_ref())),
            )?;
            // Admitted reads remain replayable even when new admission stops.
            let session = registry.session_mut(request.session_id, peer)?;
            let checksum = session.checksum;
            let replay = session.replay_window(&request)?;
            if let Some(reply) = replay {
                drop(registry);
                self.authorize_read(peer, &access, &file)?;
                return Ok(reply);
            }
            let session = registry.admitting_session(request.session_id, peer)?;
            let window_bytes = session.prepare_slot_bytes(lane_id, request.slot_bytes)?;
            let admitted_at = session.admit_read_id(&request.read_id)?;
            self.check_prepare_capacity(
                &registry,
                lane_id,
                length.div_ceil(window_bytes),
                "OwnerFs HIXL prepare exceeds the lane export pool",
            )?;
            (
                window_bytes,
                checksum,
                admitted_at,
                self.track_prepare(&registry),
            )
        };

        let rollback_failed = Cell::new(false);
        let reply = self.fill_and_export(
            peer,
            request,
            &access,
            &file,
            lane_id,
            length,
            window_bytes,
            checksum,
            admitted_at,
            &rollback_failed,
        );
        if rollback_failed.get() {
            let mut registry = self.lock_registry()?;
            self.quarantine_pending(&mut registry, true)?;
        }
        reply
    }

    /// Pins the requested slots against one deadline and rolls them back on failure.
    fn reserve_exports<'a>(
        &'a self,
        lane_id: usize,
        capacities: impl ExactSizeIterator<Item = usize>,
        rollback_failed: &'a Cell<bool>,
    ) -> Result<Vec<PendingExport<'a>>, Status> {
        let deadline = Instant::now() + OWNER_HIXL_EXPORT_DEADLINE;
        let capacities: Vec<_> = capacities.collect();
        let reservations = self
            .provider
            .reserve_exports(lane_id, &capacities, deadline)
            .map_err(|error| {
                if error.kind == afs_transport::hixl::HixlErrorKind::EnginePoisoned {
                    rollback_failed.set(true);
                }
                runtime_status(error)
            })?;
        // Arm every guard before validating: even an inconsistent result must roll back the full group.
        let pending: Vec<_> = reservations
            .into_iter()
            .map(|slot| PendingExport::new(self.provider.as_ref(), lane_id, slot, rollback_failed))
            .collect();
        if pending.len() != capacities.len()
            || pending.iter().zip(capacities).any(|(slot, capacity)| {
                let window = slot.window();
                window.length != capacity || window.address == 0 || window.generation == 0
            })
        {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INTERNAL,
                "HIXL export returned an inconsistent window",
            ));
        }
        Ok(pending)
    }

    /// Pins the export slots of a window, reads the file straight into them
    /// and publishes the filled prefix. Every slot that is not published is
    /// released when `pending` drops; a failed release sets `rollback_failed`.
    #[allow(clippy::too_many_arguments)]
    fn fill_and_export(
        &self,
        peer: &str,
        request: OwnerPrepareReadHixlRequest,
        access: &PresentedRootAccess,
        file: &RemoteFile,
        lane_id: usize,
        length: usize,
        window_bytes: usize,
        checksum: SlotChecksum,
        admitted_at: Instant,
        rollback_failed: &Cell<bool>,
    ) -> Result<OwnerPrepareReadHixlReply, Status> {
        // A rejected caller never pins slots another read could use.
        self.authorize_read(peer, access, file)?;
        let capacities = (0..length.div_ceil(window_bytes))
            .map(|index| (length - index * window_bytes).min(window_bytes));
        let mut pending = self.reserve_exports(lane_id, capacities, rollback_failed)?;

        let started = Instant::now();
        let filled = self.read_source(
            peer,
            access,
            file,
            request.offset,
            pending
                .iter_mut()
                .map(PendingExport::as_mut_slice)
                .collect(),
            window_bytes,
            checksum,
        );
        self.observe(Operation::SourceRead, started, filled.is_ok());
        let (read, checksums) = filled?;
        let eof = read < length;
        if read == 0 {
            self.prepare_registry(peer, request.session_id, None)?
                .session_mut(request.session_id, peer)?
                .check_read_id(&request.read_id, Some(admitted_at))?;
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
        // Slots past the end of the file are never published.
        pending.truncate(checksums.len());

        // An identical retry may have published its slots while this attempt filled its own.
        let mut registry = self.prepare_registry(peer, request.session_id, None)?;
        let session = registry.session_mut(request.session_id, peer)?;
        session.check_read_id(&request.read_id, Some(admitted_at))?;
        if let Some(reply) = session.replay_window(&request)? {
            return Ok(reply);
        }
        let lane = registry
            .admitting_session(request.session_id, peer)?
            .lanes
            .get(lane_id)
            .cloned()
            .ok_or_else(|| Status::internal("OwnerFs HIXL lane disappeared"))?;
        let slots = pending
            .iter()
            .zip(checksums)
            .enumerate()
            .map(|(index, (reservation, checksum))| {
                export_descriptor(
                    reservation,
                    (read - index * window_bytes).min(window_bytes),
                    checksum,
                )
            })
            .collect();
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
                admitted: Admitted::Window {
                    request,
                    reply: reply.clone(),
                },
                lane_id,
                native_outcome_unknown: false,
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

    /// Packs whole small files of one directory into export slots. The grant
    /// is validated once for reading the root, the same right a peer OPEN for
    /// reading needs; every packed file is opened, checked against the version
    /// it was planned with and read whole. A file that fails is zeroed and
    /// left out of the reply instead of failing the batch.
    fn prepare_batch_blocking(
        &self,
        peer: &str,
        request: OwnerPrepareBatchHixlRequest,
    ) -> Result<OwnerPrepareBatchHixlReply, Status> {
        let invalid =
            |message: &'static str| coded_status(afs_error::NODE_TRANSFER_INVALID, message);
        if request.session_id == 0
            || request.read_id.is_empty()
            || request.read_id.len() > MAX_OWNER_HIXL_READ_ID_BYTES
            || request.max_bytes == 0
            || request.max_bytes > MAX_OWNERFS_HIXL_READ_GROUP_BYTES as u64
            || request.min_file_bytes >= request.max_file_bytes
            || request.parent_path.len() > MAX_OWNER_HIXL_BATCH_PATH_BYTES
            || request.anchor_name.len() > MAX_OWNER_HIXL_BATCH_PATH_BYTES
        {
            return Err(invalid(
                "OwnerFs HIXL batch prepare identity or bounds are invalid",
            ));
        }
        let access = presented_access(request.access.clone()).map_err(error_to_status)?;
        let lane_id =
            usize::try_from(request.lane_id).map_err(|_| invalid("OwnerFs HIXL lane overflows"))?;
        let max_bytes = usize::try_from(request.max_bytes)
            .map_err(|_| invalid("OwnerFs HIXL batch length overflows"))?;
        let parent = OsStr::from_bytes(&request.parent_path).to_owned();

        let (slot_bytes, max_slots, checksum, admitted_at, _preparing) = {
            let mut registry = self.prepare_registry(
                peer,
                request.session_id,
                Some((&access.home_session_id, request.prior_release.as_ref())),
            )?;
            let session = registry.session_mut(request.session_id, peer)?;
            let checksum = session.checksum;
            let replay = session.replay_batch(&request)?;
            if let Some(reply) = replay {
                drop(registry);
                // Replay still proves the grant is current.
                self.batch_directory(peer, &access, &parent)?;
                return Ok(reply);
            }
            let session = registry.admitting_session(request.session_id, peer)?;
            let slot_bytes = session.prepare_slot_bytes(lane_id, request.slot_bytes)?;
            if usize::try_from(request.max_file_bytes).map_or(true, |bytes| bytes > slot_bytes) {
                return Err(invalid("OwnerFs HIXL batch file limit exceeds one slot"));
            }
            let admitted_at = session.admit_read_id(&request.read_id)?;
            let max_slots = max_bytes / slot_bytes;
            self.check_prepare_capacity(
                &registry,
                lane_id,
                max_slots,
                "OwnerFs HIXL batch prepare exceeds the lane export pool",
            )?;
            (
                slot_bytes,
                max_slots,
                checksum,
                admitted_at,
                self.track_prepare(&registry),
            )
        };

        let directory = self.batch_directory(peer, &access, &parent)?;
        let names = self
            .batch_directory_names(directory.as_ref(), &access, &parent)
            .map_err(error_to_status)?;
        // Without a complete sorted listing there is no safe continuation.
        // An empty batch lets the reader use its ordinary file-read path.
        let plan = names.map_or_else(BatchPlan::default, |names| {
            plan_batch(
                directory.as_ref(),
                &names,
                &request.anchor_name,
                request.min_file_bytes,
                request.max_file_bytes,
                slot_bytes,
                max_slots,
            )
        });
        if plan.slots.is_empty() {
            self.prepare_registry(peer, request.session_id, None)?
                .session_mut(request.session_id, peer)?
                .check_read_id(&request.read_id, Some(admitted_at))?;
            return Ok(OwnerPrepareBatchHixlReply {
                session_id: request.session_id,
                read_id: request.read_id,
                lane_id: request.lane_id,
                next_name: plan.next,
                ..Default::default()
            });
        }
        let rollback_failed = Cell::new(false);
        let reply = self.fill_batch_and_export(
            peer,
            request,
            directory.as_ref(),
            plan,
            lane_id,
            checksum,
            admitted_at,
            &rollback_failed,
        );
        if rollback_failed.get() {
            let mut registry = self.lock_registry()?;
            self.quarantine_pending(&mut registry, true)?;
        }
        reply
    }

    fn batch_directory_names(
        &self,
        directory: &dyn OwnerBatchDirectory,
        access: &PresentedRootAccess,
        parent: &OsStr,
    ) -> afs_error::Result<Option<Arc<Vec<OsString>>>> {
        let collect = || {
            directory
                .names_bounded(
                    MAX_OWNER_HIXL_BATCH_SCAN,
                    MAX_OWNER_HIXL_BATCH_SNAPSHOT_BYTES,
                )
                .map(|names| names.map(Arc::new))
        };
        // Bound key allocations too. The caller already bounds the parent path.
        if access.id.0.len() > MAX_OWNER_HIXL_BATCH_PATH_BYTES
            || access.home_session_id.len() > MAX_OWNER_HIXL_BATCH_PATH_BYTES
        {
            return collect();
        }
        // The caller has reopened the directory under a current read grant.
        // All filesystem work stays outside the snapshot lock.
        let version = directory.version().ok();
        if let Ok(mut snapshots) = self.batch_snapshots.lock()
            && let Some(index) = snapshots
                .iter()
                .position(|item| item.belongs_to(access, parent))
            && let Some(snapshot) = snapshots.remove(index)
            && version.is_some_and(|version| snapshot.matches(access, version))
        {
            let names = snapshot.names.clone();
            snapshots.push_back(snapshot);
            return Ok(names);
        }

        let names = collect()?;
        // Never publish a snapshot from a failed stamp check, a concurrent
        // mutation during enumeration. Cache a stable over-budget result too.
        if let Some(version) = version
            && directory.version().ok() == Some(version)
            && let Ok(mut snapshots) = self.batch_snapshots.lock()
        {
            snapshots.retain(|item| !item.belongs_to(access, parent));
            if snapshots.len() == MAX_OWNER_HIXL_BATCH_SNAPSHOTS {
                snapshots.pop_front();
            }
            snapshots.push_back(BatchDirectorySnapshot {
                root: access.id.clone(),
                parent: parent.to_owned(),
                epoch: access.epoch,
                generation: access.access_generation,
                home_session: access.home_session_id.clone(),
                version,
                names: names.clone(),
            });
        }
        Ok(names)
    }

    fn batch_directory(
        &self,
        peer: &str,
        access: &PresentedRootAccess,
        parent: &OsStr,
    ) -> Result<Box<dyn OwnerBatchDirectory>, Status> {
        let started = Instant::now();
        let directory = self.source.batch_directory(peer, access, parent);
        self.observe(Operation::Authorize, started, directory.is_ok());
        directory.map_err(error_to_status)
    }

    /// Pins one export slot per planned slot, fills them concurrently on the
    /// Home source pool and publishes the files that were read whole. Every
    /// slot that is not published is released when `pending` drops.
    #[allow(clippy::too_many_arguments)]
    fn fill_batch_and_export(
        &self,
        peer: &str,
        request: OwnerPrepareBatchHixlRequest,
        directory: &dyn OwnerBatchDirectory,
        plan: BatchPlan,
        lane_id: usize,
        checksum: SlotChecksum,
        admitted_at: Instant,
        rollback_failed: &Cell<bool>,
    ) -> Result<OwnerPrepareBatchHixlReply, Status> {
        let mut pending = self.reserve_exports(
            lane_id,
            plan.slots.iter().map(|slot| slot.used),
            rollback_failed,
        )?;

        let started = Instant::now();
        let filled = fill_batch_slots(
            directory,
            &plan,
            pending
                .iter_mut()
                .map(PendingExport::as_mut_slice)
                .collect(),
            checksum,
        );
        self.observe(Operation::SourceRead, started, true);

        // An identical retry may have published its slots while this attempt filled its own.
        let mut registry = self.prepare_registry(peer, request.session_id, None)?;
        let session = registry.session_mut(request.session_id, peer)?;
        session.check_read_id(&request.read_id, Some(admitted_at))?;
        if let Some(reply) = session.replay_batch(&request)? {
            return Ok(reply);
        }
        let lane = registry
            .admitting_session(request.session_id, peer)?
            .lanes
            .get(lane_id)
            .cloned()
            .ok_or_else(|| Status::internal("OwnerFs HIXL lane disappeared"))?;
        let mut slots = Vec::with_capacity(plan.slots.len());
        let mut files = Vec::new();
        let mut bytes = 0_u64;
        for (index, ((reservation, planned), (digest, whole))) in
            pending.iter().zip(&plan.slots).zip(filled).enumerate()
        {
            let slot_index = u32::try_from(index)
                .map_err(|_| Status::internal("OwnerFs HIXL batch slot index overflows"))?;
            slots.push(export_descriptor(reservation, planned.used, digest));
            bytes = bytes.saturating_add(planned.used as u64);
            for (file, whole) in planned.files.iter().zip(whole) {
                if whole {
                    files.push(WireBatchFile {
                        name: file.name.as_bytes().to_vec(),
                        identity: Some(FileIdentity {
                            opaque: file.entry.identity.clone(),
                        }),
                        size: file.entry.size,
                        mtime_ns: file.entry.mtime_ns,
                        ctime_ns: file.entry.ctime_ns,
                        slot_index,
                        offset_in_slot: file.offset as u64,
                    });
                }
            }
        }
        let reply = OwnerPrepareBatchHixlReply {
            session_id: request.session_id,
            read_id: request.read_id.clone(),
            lane_id: request.lane_id,
            engine_id: lane.engine_id.clone(),
            engine_generation: lane.generation,
            bytes,
            slots,
            files,
            next_name: plan.next,
        };
        for export in pending {
            export.disarm();
        }
        let session = registry.admitting_session(request.session_id, peer)?;
        session.last_used = Instant::now();
        session.reads.insert(
            request.read_id.clone(),
            ReadRecord {
                admitted: Admitted::Batch {
                    request,
                    reply: reply.clone(),
                },
                lane_id,
                native_outcome_unknown: false,
            },
        );
        Ok(reply)
    }

    /// Reads the file from `offset` straight into the pinned export slots, one
    /// task per slot on the Home source pool, and digests each slot once it is
    /// filled. A single thread copying from the page cache slows to about half
    /// speed when the file's pages sit on another socket, so the slots of a
    /// window are read concurrently. Each source call revalidates the grant and
    /// open handle, exactly like a sequence of `OwnerFiles.Read` calls. The
    /// window ends at the first short slot (EOF) and later slots are never
    /// exported; the first failing slot in file order decides the error.
    #[allow(clippy::too_many_arguments)]
    fn read_source(
        &self,
        peer: &str,
        access: &PresentedRootAccess,
        file: &RemoteFile,
        offset: u64,
        slots: Vec<&mut [u8]>,
        slot_bytes: usize,
        checksum: SlotChecksum,
    ) -> Result<(usize, Vec<Vec<u8>>), Status> {
        let fill = |index: usize, out: &mut [u8]| -> Result<(usize, Vec<u8>), Status> {
            let start = index
                .checked_mul(slot_bytes)
                .and_then(|relative| u64::try_from(relative).ok())
                .and_then(|relative| offset.checked_add(relative))
                .ok_or_else(|| Status::internal("OwnerFs HIXL source offset overflow"))?;
            let mut filled = 0;
            while filled < out.len() {
                let position = start
                    .checked_add(filled as u64)
                    .ok_or_else(|| Status::internal("OwnerFs HIXL source offset overflow"))?;
                let read = self
                    .source
                    .read_positional(peer, access, file, position, &mut out[filled..])
                    .map_err(error_to_status)?;
                if read == 0 {
                    break;
                }
                if read > out.len() - filled {
                    return Err(Status::internal("OwnerFs source read exceeded its buffer"));
                }
                filled += read;
            }
            Ok((filled, checksum.digest(&out[..filled])))
        };
        let capacities = slots.iter().map(|slot| slot.len()).collect::<Vec<_>>();
        let results = map_source_slots(slots, fill);
        let mut filled = 0;
        let mut checksums = Vec::with_capacity(results.len());
        for (index, result) in results.into_iter().enumerate() {
            let (read, digest) =
                result.ok_or_else(|| Status::internal("OwnerFs HIXL slot read did not run"))??;
            if read > 0 {
                checksums.push(digest);
            }
            filled += read;
            if read < capacities[index] {
                break;
            }
        }
        Ok((filled, checksums))
    }

    fn release_blocking(
        &self,
        peer: &str,
        request: OwnerReleaseHixlReadRequest,
    ) -> Result<OwnerReleaseHixlReadReply, Status> {
        let outcome = release_outcome(&request)?;
        let mut registry = self.lock_registry()?;
        registry.expire_sessions();
        self.quarantine_pending(&mut registry, false)?;
        self.release_locked(&mut registry, peer, &request, outcome)
    }

    /// Applies the release a prepare carries for the previous read of its
    /// session. It runs before admission, so a refused prepare still frees
    /// those slots; a replayed prepare finds the release tombstone.
    fn apply_prior_release(
        &self,
        registry: &mut Registry,
        peer: &str,
        session_id: u64,
        release: Option<&OwnerReleaseHixlReadRequest>,
    ) -> Result<(), Status> {
        let Some(release) = release else {
            return Ok(());
        };
        if release.session_id != session_id {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "OwnerFs HIXL prior release names another session",
            ));
        }
        let started = Instant::now();
        let released = release_outcome(release)
            .and_then(|outcome| self.release_locked(registry, peer, release, outcome));
        self.observe(Operation::Release, started, released.is_ok());
        released.map(|_| ())
    }

    fn release_locked(
        &self,
        registry: &mut Registry,
        peer: &str,
        request: &OwnerReleaseHixlReadRequest,
        outcome: HixlReleaseOutcome,
    ) -> Result<OwnerReleaseHixlReadReply, Status> {
        let mut release_error = None;
        let mut pinned = false;
        let reply = {
            let session = registry.session_mut(request.session_id, peer)?;
            if session.tombstones.matches(request) {
                session.last_used = Instant::now();
                OwnerReleaseHixlReadReply {
                    consumed: true,
                    already_released: true,
                }
            } else {
                let Some(record) = session.reads.get_mut(&request.read_id) else {
                    return session.retire_unpublished_read(request, outcome);
                };
                if record.admitted.engine_generation() != request.engine_generation {
                    return Err(Status::permission_denied(
                        "OwnerFs HIXL release engine identity does not match",
                    ));
                }
                if outcome == HixlReleaseOutcome::NotSubmitted && record.native_outcome_unknown {
                    return Err(coded_status(
                        afs_error::NODE_TRANSFER_INVALID,
                        "OwnerFs HIXL read has an unknown native transfer outcome",
                    ));
                }
                session.last_used = Instant::now();
                if outcome == HixlReleaseOutcome::Unknown {
                    record.native_outcome_unknown = true;
                    pinned = true;
                    OwnerReleaseHixlReadReply::default()
                } else {
                    let lane_id = record.lane_id;
                    let slots = record
                        .admitted
                        .slots()
                        .iter()
                        .map(|slot| (slot.slot_id, slot.slot_generation))
                        .collect();
                    // SAFETY: publication consumed all writable guards; the authenticated reader
                    // identified this read generation and confirmed a terminal outcome for the group.
                    if let Err(error) = unsafe { self.provider.release_exports(lane_id, slots) } {
                        release_error = Some(runtime_status(error));
                        pinned = true;
                    }
                    if release_error.is_none() {
                        session.reads.remove(&request.read_id);
                        session.tombstones.insert(request);
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
            self.quarantine_pending(registry, true)?;
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
        if request.session_id == 0 || request.home_session_id.is_empty() {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "OwnerFs HIXL close identity is incomplete",
            ));
        }
        let mut registry = self.lock_registry()?;
        let drained = {
            let session =
                registry.bound_session(request.session_id, peer, &request.home_session_id)?;
            session.closed = true;
            session.last_used = Instant::now();
            !session.has_pending_slots()
        };
        self.quarantine_pending(&mut registry, false)?;
        Ok(OwnerCloseHixlReply { drained })
    }

    fn quarantine_pending(&self, registry: &mut Registry, force: bool) -> Result<(), Status> {
        // A frozen registry whose runtime quarantine failed keeps retrying it,
        // even when the trigger was a forced one with no closed session.
        let retry = registry.admission_frozen && !registry.runtime_quarantined;
        if !force && !retry && !registry.has_closed_pending_slots() {
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

#[derive(Default)]
struct BatchPlan {
    slots: Vec<PlannedSlot>,
    /// First entry after the packed range; empty at the end of the directory.
    next: Vec<u8>,
}

#[derive(Default)]
struct PlannedSlot {
    files: Vec<PlannedFile>,
    used: usize,
}

struct PlannedFile {
    name: OsString,
    entry: BatchEntry,
    offset: usize,
    length: usize,
}

/// Packs eligible regular files in name order from the anchor, whole files
/// only and never across a slot. Stops when no slot is left for the next
/// file or at the file limit; that entry becomes `next`.
fn plan_batch(
    directory: &dyn OwnerBatchDirectory,
    names: &[OsString],
    anchor: &[u8],
    min_file_bytes: u64,
    max_file_bytes: u64,
    slot_bytes: usize,
    max_slots: usize,
) -> BatchPlan {
    let start = names.partition_point(|name| name.as_bytes() < anchor);
    let mut plan = BatchPlan::default();
    let mut files = 0_usize;
    for name in &names[start..] {
        if files == MAX_OWNER_HIXL_BATCH_FILES {
            plan.next = name.as_bytes().to_vec();
            break;
        }
        // An entry that vanished or cannot be examined is skipped like any
        // other ineligible entry; the reader reads it on its own path.
        let Ok(Some(entry)) = directory.stat(name) else {
            continue;
        };
        if entry.size <= min_file_bytes || entry.size > max_file_bytes {
            continue;
        }
        let Ok(length) = usize::try_from(entry.size) else {
            continue;
        };
        let fits = plan.slots.last().is_some_and(|slot| {
            slot.used
                .checked_add(length)
                .is_some_and(|end| end <= slot_bytes)
        });
        if !fits {
            if plan.slots.len() == max_slots {
                plan.next = name.as_bytes().to_vec();
                break;
            }
            plan.slots.push(PlannedSlot::default());
        }
        let Some(slot) = plan.slots.last_mut() else {
            break;
        };
        slot.files.push(PlannedFile {
            name: name.clone(),
            entry,
            offset: slot.used,
            length,
        });
        slot.used += length;
        files += 1;
    }
    plan
}

/// Fills every planned slot on the Home source pool and digests its used
/// prefix. Returns, per slot, the digest and which files were read whole.
fn fill_batch_slots(
    directory: &dyn OwnerBatchDirectory,
    plan: &BatchPlan,
    buffers: Vec<&mut [u8]>,
    checksum: SlotChecksum,
) -> Vec<(Vec<u8>, Vec<bool>)> {
    let fill = |index: usize, out: &mut [u8]| {
        let slot = &plan.slots[index];
        let whole = slot
            .files
            .iter()
            .map(|file| {
                let Some(region) = file
                    .offset
                    .checked_add(file.length)
                    .and_then(|end| out.get_mut(file.offset..end))
                else {
                    return false;
                };
                let read_whole = read_batch_file(directory, file, region).is_some();
                if !read_whole {
                    region.fill(0);
                }
                read_whole
            })
            .collect::<Vec<_>>();
        let used = slot.used.min(out.len());
        (checksum.digest(&out[..used]), whole)
    };
    map_source_slots(buffers, fill)
        .into_iter()
        .zip(&plan.slots)
        .map(|(result, slot)| result.unwrap_or_else(|| (Vec::new(), vec![false; slot.files.len()])))
        .collect()
}

/// Run each fill on the Home source pool and collect results in slot order.
fn map_source_slots<T: Send>(
    buffers: Vec<&mut [u8]>,
    fill: impl Fn(usize, &mut [u8]) -> T + Sync,
) -> Vec<Option<T>> {
    let results = Mutex::new((0..buffers.len()).map(|_| None).collect::<Vec<_>>());
    let run = |index: usize, out: &mut [u8]| {
        let result = fill(index, out);
        match results.lock() {
            Ok(mut results) => results[index] = Some(result),
            Err(poisoned) => poisoned.into_inner()[index] = Some(result),
        }
    };
    match owner_source_pool() {
        Some(pool) if buffers.len() > 1 => pool.install(|| {
            rayon_core::scope(|scope| {
                for (index, out) in buffers.into_iter().enumerate() {
                    let run = &run;
                    scope.spawn(move |_| run(index, out));
                }
            });
        }),
        _ => {
            for (index, out) in buffers.into_iter().enumerate() {
                run(index, out);
            }
        }
    }
    results
        .into_inner()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Opens one planned file and reads exactly its planned length. The open must
/// still be the planned version, so the published identity, size and
/// timestamps describe the bytes in the slot. `None` when the file was not
/// read whole.
fn read_batch_file(
    directory: &dyn OwnerBatchDirectory,
    file: &PlannedFile,
    out: &mut [u8],
) -> Option<()> {
    let source = directory.open(&file.name).ok()?;
    if *source.entry() != file.entry {
        return None;
    }
    let mut filled = 0;
    while filled < out.len() {
        let position = u64::try_from(filled).ok()?;
        let read = source.read_at(position, &mut out[filled..]).ok()?;
        if read == 0 || read > out.len() - filled {
            return None;
        }
        filled += read;
    }
    Some(())
}

/// Reads the slots of a window concurrently, separate from the shared reader
/// validation pool.
fn owner_source_pool() -> Option<&'static rayon_core::ThreadPool> {
    static POOL: std::sync::OnceLock<Option<rayon_core::ThreadPool>> = std::sync::OnceLock::new();
    POOL.get_or_init(|| {
        let workers = std::thread::available_parallelism()
            .map_or(4, |parallel| parallel.get())
            .clamp(1, MAX_OWNER_SOURCE_THREADS);
        rayon_core::ThreadPoolBuilder::new()
            .num_threads(workers)
            .thread_name(|index| format!("afs-owner-source-{index}"))
            .build()
            .ok()
    })
    .as_ref()
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

/// The outcome of a release, rejected when its identity is incomplete.
fn release_outcome(request: &OwnerReleaseHixlReadRequest) -> Result<HixlReleaseOutcome, Status> {
    let outcome = HixlReleaseOutcome::try_from(request.outcome).map_err(|_| {
        coded_status(
            afs_error::NODE_TRANSFER_INVALID,
            "HIXL release outcome is unknown",
        )
    })?;
    if request.session_id == 0
        || request.read_id.is_empty()
        || request.read_id.len() > MAX_OWNER_HIXL_READ_ID_BYTES
        || request.engine_generation == 0
        || outcome == HixlReleaseOutcome::Unspecified
        || (outcome == HixlReleaseOutcome::NotSubmitted && request.content_validated)
    {
        return Err(coded_status(
            afs_error::NODE_TRANSFER_INVALID,
            "OwnerFs HIXL release identity is incomplete",
        ));
    }
    Ok(outcome)
}

pub(crate) use super::hixl::unsupported_status;

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
                && !session.has_pending_slots()
                && session.tombstones.is_empty()
                && now.duration_since(session.last_used) >= OWNER_HIXL_SESSION_TTL)
        });
    }

    fn has_closed_pending_slots(&self) -> bool {
        self.sessions
            .values()
            .any(|session| session.closed && session.has_uncertain_slots())
    }

    fn next_session_id(&mut self) -> Result<u64, Status> {
        next_session_id(
            &mut self.next_session,
            "OwnerFs HIXL session id space exhausted",
        )
    }

    fn active_slot_count(&self) -> usize {
        self.sessions
            .values()
            .map(|session| session.slot_count(None))
            .sum()
    }

    fn lane_slot_count(&self, lane: usize) -> usize {
        self.sessions
            .values()
            .map(|session| session.slot_count(Some(lane)))
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

    fn bound_session(
        &mut self,
        session_id: u64,
        peer: &str,
        home_session_id: &str,
    ) -> Result<&mut Session, Status> {
        let session = self.session_mut(session_id, peer)?;
        if session.home_session_id != home_session_id {
            return Err(Status::permission_denied(
                "OwnerFs HIXL request belongs to another Home session",
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
    writes: HashMap<String, write::WriteRecord>,
    cancelled_successors: HashMap<String, u64>,
    tombstones: Tombstones,
    checksum: SlotChecksum,
}

impl Session {
    fn has_pending_slots(&self) -> bool {
        !self.reads.is_empty() || self.writes.values().any(|write| write.slot_count() != 0)
    }

    fn has_uncertain_slots(&self) -> bool {
        !self.reads.is_empty()
            || self
                .writes
                .values()
                .any(write::WriteRecord::needs_quarantine)
    }

    fn slot_count(&self, lane: Option<usize>) -> usize {
        let reads: usize = self
            .reads
            .values()
            .filter(|read| lane.is_none_or(|lane| read.lane_id == lane))
            .map(|read| read.admitted.slots().len())
            .sum();
        let writes: usize = self
            .writes
            .values()
            .filter(|write| lane.is_none_or(|lane| write.lane_id() == Some(lane)))
            .map(write::WriteRecord::slot_count)
            .sum();
        reads + writes
    }

    fn prepare_slot_bytes(&self, lane_id: usize, slot_bytes: u64) -> Result<usize, Status> {
        let lane_bytes = self
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
        usize::try_from(slot_bytes)
            .ok()
            .filter(|bytes| (1..=lane_bytes).contains(bytes))
            .ok_or_else(|| {
                coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "OwnerFs HIXL slot size is outside the lane window",
                )
            })
    }

    fn replay_window(
        &self,
        request: &OwnerPrepareReadHixlRequest,
    ) -> Result<Option<OwnerPrepareReadHixlReply>, Status> {
        match self
            .reads
            .get(&request.read_id)
            .map(|record| &record.admitted)
        {
            Some(Admitted::Window {
                request: admitted,
                reply,
            }) if admitted == request => Ok(Some(reply.clone())),
            Some(_) => Err(prepare_replay_mismatch()),
            None => Ok(None),
        }
    }

    fn replay_batch(
        &self,
        request: &OwnerPrepareBatchHixlRequest,
    ) -> Result<Option<OwnerPrepareBatchHixlReply>, Status> {
        match self
            .reads
            .get(&request.read_id)
            .map(|record| &record.admitted)
        {
            Some(Admitted::Batch {
                request: admitted,
                reply,
            }) if admitted == request => Ok(Some(reply.clone())),
            Some(_) => Err(prepare_replay_mismatch()),
            None => Ok(None),
        }
    }

    /// NotSubmitted can arrive before its Prepare or while it is filling slots.
    /// The authenticated session and negotiated Home generation fence that ID
    /// under the same registry lock that publishes prepared slots.
    fn retire_unpublished_read(
        &mut self,
        request: &OwnerReleaseHixlReadRequest,
        outcome: HixlReleaseOutcome,
    ) -> Result<OwnerReleaseHixlReadReply, Status> {
        if outcome != HixlReleaseOutcome::NotSubmitted || self.tombstones.contains(&request.read_id)
        {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "OwnerFs HIXL release does not identify an admitted read or matching release",
            ));
        }
        if !self
            .lanes
            .iter()
            .any(|lane| lane.generation == request.engine_generation)
        {
            return Err(Status::permission_denied(
                "OwnerFs HIXL release engine identity does not match",
            ));
        }
        self.last_used = Instant::now();
        self.tombstones.insert(request);
        Ok(OwnerReleaseHixlReadReply {
            consumed: true,
            already_released: false,
        })
    }

    fn admit_read_id(&mut self, read_id: &str) -> Result<Instant, Status> {
        self.check_read_id(read_id, None)?;
        self.last_used = Instant::now();
        Ok(self.last_used)
    }

    fn check_read_id(&self, read_id: &str, admitted_at: Option<Instant>) -> Result<(), Status> {
        if self.tombstones.contains(read_id) {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "OwnerFs HIXL read was already released",
            ));
        }
        // Legacy IDs are opaque. Once an abandoned ID leaves bounded history,
        // the session can drain existing replies but cannot admit another read.
        if self.tombstones.evicted_not_submitted {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "OwnerFs HIXL session no longer covers abandoned read IDs",
            ));
        }
        // A release could have retired this attempt while source I/O ran.
        // Fail closed if its tombstone could already have left bounded history.
        if admitted_at
            .zip(self.tombstones.last_evicted_release)
            .is_some_and(|(admitted_at, released_at)| released_at >= admitted_at)
        {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "OwnerFs HIXL release history no longer covers the preparing read",
            ));
        }
        Ok(())
    }
}

fn prepare_replay_mismatch() -> Status {
    coded_status(
        afs_error::NODE_TRANSFER_INVALID,
        "OwnerFs HIXL replay differs from the admitted prepare",
    )
}

/// Describes the initialized prefix of a reserved slot for the peer.
fn export_descriptor(
    reservation: &PendingExport<'_>,
    length: usize,
    checksum: Vec<u8>,
) -> OwnerHixlSlot {
    let window = reservation.window();
    OwnerHixlSlot {
        slot_id: window.slot_id,
        slot_generation: window.generation,
        address: window.address,
        length: length as u64,
        checksum,
    }
}

struct ReadRecord {
    admitted: Admitted,
    lane_id: usize,
    native_outcome_unknown: bool,
}

/// The admitted prepare a read id answers on replay.
enum Admitted {
    Window {
        request: OwnerPrepareReadHixlRequest,
        reply: OwnerPrepareReadHixlReply,
    },
    Batch {
        request: OwnerPrepareBatchHixlRequest,
        reply: OwnerPrepareBatchHixlReply,
    },
}

impl Admitted {
    fn slots(&self) -> &[OwnerHixlSlot] {
        match self {
            Self::Window { reply, .. } => &reply.slots,
            Self::Batch { reply, .. } => &reply.slots,
        }
    }

    fn engine_generation(&self) -> u64 {
        match self {
            Self::Window { reply, .. } => reply.engine_generation,
            Self::Batch { reply, .. } => reply.engine_generation,
        }
    }
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
    last_evicted_release: Option<Instant>,
    evicted_not_submitted: bool,
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
            self.evict_oldest();
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
            self.evict_oldest();
        }
    }

    fn evict_oldest(&mut self) {
        if let Some(read_id) = self.order.pop_front()
            && let Some(entry) = self.by_read.remove(&read_id)
        {
            self.last_evicted_release = Some(entry.inserted_at);
            self.evicted_not_submitted |= entry.outcome == HixlReleaseOutcome::NotSubmitted as i32;
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;
