//! Reader-side OwnerFs HIXL read-ahead windows.
//!
//! Only read-only remote opens are eligible. A READ that misses the open's
//! window asks Home to prepare `[offset, offset + size)` of that open handle,
//! pulls every exported slot through one native batch READ, verifies each slot
//! digest and releases the slots. The verified bytes become the open's window:
//! later READs inside it are local copies. Like the OPEN prefetch, a window is
//! the Home content at prepare time, reused only by the same open and dropped
//! at release or after this node writes the same file.

use super::OwnerRuntime;
use super::hixl::{
    ReadRuntime, bind_engines, connect_lanes, engine_transport_parameters, lane_is_valid,
    least_used_lane, remaining, validation_pool,
};
use crate::config::HixlOwnerFsReadMode;
use crate::node::rpc::OwnerRpcMetrics;
use crate::node::rpc::hixl::{
    HIXL_PROTOCOL_VERSION, HixlMetrics, Operation, failed_read_outcome, is_refused, is_unsupported,
    observe_optional, runtime_error, uncertain_read,
};
use crate::node::vfs::ownerfs::files::RemoteFile;
use crate::node::vfs::ownerfs::root::RootGrant;
use afs_protocol::node_data::{
    DfsHixlReleaseOutcome, OwnerCloseHixlRequest, OwnerHandle, OwnerNegotiateHixlReply,
    OwnerNegotiateHixlRequest, OwnerPrepareReadHixlReply, OwnerPrepareReadHixlRequest,
    OwnerReleaseHixlReadRequest, RootAccess, owner_files_client::OwnerFilesClient,
};
use afs_transport::grpc::error_status::status_to_error;
use afs_transport::hixl::{HixlErrorKind, HixlRuntimeGroup};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, OwnedMutexGuard, oneshot};
use tonic::transport::Channel;

const MAX_OWNER_HIXL_HOMES: usize = 64;
const MAX_OWNER_HIXL_ENGINES: usize = MAX_OWNER_HIXL_HOMES * 16;
const MAX_OWNER_HIXL_SLOTS: usize = 64;
/// Home closes a session after 30 s without traffic; reuse stays below that.
const SESSION_REUSE_LIMIT: Duration = Duration::from_secs(20);
/// A Home that reported the capability as unsupported, or refused admission
/// in preferred mode, is not asked again for this long, so fallbacks do not
/// pay a negotiation each.
const UNSUPPORTED_RETRY: Duration = Duration::from_secs(60);
/// Home closes a session after 30 s without activity; a pool unused this long
/// keeps nothing Home still serves and may be evicted.
const HOME_POOL_IDLE: Duration = Duration::from_secs(60);
const PREPARE_ATTEMPTS: usize = 3;
/// Mirrors the `[hixl].ownerfs_sessions_per_home` configuration bound.
pub const MAX_OWNER_HIXL_SESSIONS_PER_HOME: usize = 4;
const RELEASE_ATTEMPTS: usize = 3;

/// Window sizing shared by every open of this process.
#[derive(Clone, Copy, Debug)]
pub struct OwnerHixlPolicy {
    pub mode: HixlOwnerFsReadMode,
    pub max_window_bytes: usize,
    pub max_inflight: usize,
    pub host_budget_bytes: u64,
    pub min_file_bytes: u64,
    /// Local receive slots of each lane: one native batch READ needs one per slot.
    pub receive_slots: usize,
    /// Control sessions per Home. Each session admits one window fetch, so
    /// N sessions let N opens of one Home prepare and transfer concurrently.
    pub sessions_per_home: usize,
}

/// Process-wide reader state: one control session per Home, engine bindings,
/// lane bindings and the shared window budget.
pub struct OwnerHixlReader {
    native: Arc<dyn ReadRuntime>,
    node_id: String,
    policy: OwnerHixlPolicy,
    timeout: Duration,
    budget: Arc<WindowBudget>,
    homes: StdMutex<HashMap<String, Arc<HomePool>>>,
    engines: StdMutex<HashMap<SocketAddr, (String, u64)>>,
    lane_bindings: StdMutex<HashMap<String, usize>>,
    next_read: AtomicU64,
    metrics: Option<HixlMetrics>,
    owner_metrics: Option<OwnerRpcMetrics>,
}

impl OwnerHixlReader {
    /// Construction fails for off mode, an invalid deadline or an empty runtime.
    pub fn new(
        native: Arc<HixlRuntimeGroup>,
        node_id: String,
        policy: OwnerHixlPolicy,
        timeout: Duration,
    ) -> afs_error::Result<Self> {
        Self::from_runtime(native, node_id, policy, timeout)
    }

    pub(super) fn from_runtime(
        native: Arc<dyn ReadRuntime>,
        node_id: String,
        policy: OwnerHixlPolicy,
        timeout: Duration,
    ) -> afs_error::Result<Self> {
        if policy.mode == HixlOwnerFsReadMode::Off
            || timeout.is_zero()
            || timeout > Duration::from_secs(300)
            || native.lane_count() == 0
            || policy.max_window_bytes == 0
            || policy.receive_slots == 0
            || !(1..=2).contains(&policy.max_inflight)
            || !(1..=MAX_OWNER_HIXL_SESSIONS_PER_HOME).contains(&policy.sessions_per_home)
        {
            return Err(afs_error::Error::coded(
                afs_error::NODE_TRANSFER_INVALID,
                "OwnerFs HIXL mode, deadline, policy or runtime is invalid",
            ));
        }
        Ok(Self {
            native,
            node_id,
            budget: Arc::new(WindowBudget::new(policy.host_budget_bytes)),
            policy,
            timeout,
            homes: StdMutex::new(HashMap::new()),
            engines: StdMutex::new(HashMap::new()),
            lane_bindings: StdMutex::new(HashMap::new()),
            next_read: AtomicU64::new(1),
            metrics: None,
            owner_metrics: None,
        })
    }

    pub fn with_metrics(mut self, metrics: HixlMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn with_owner_metrics(mut self, metrics: OwnerRpcMetrics) -> Self {
        self.owner_metrics = Some(metrics);
        self
    }

    fn observe(&self, phase: Operation, elapsed: Duration, success: bool) {
        observe_optional(self.metrics.as_ref(), phase, elapsed, success);
    }

    fn count(&self, outcome: ReadOutcome) {
        if let Some(metrics) = &self.metrics {
            metrics.observe_ownerfs_read(outcome.label());
        }
    }

    fn slot_bytes(&self) -> usize {
        self.native.info().window_bytes
    }

    /// Binds a Home to the least-used lane, mirroring the DFS reader.
    fn bind_lane(&self, home: &str) -> usize {
        least_used_lane(
            &self.lane_bindings,
            &home.to_owned(),
            self.native.lane_count(),
            MAX_OWNER_HIXL_HOMES * 4,
        )
    }

    fn next_read_id(&self) -> afs_error::Result<String> {
        let sequence = self
            .next_read
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| {
                afs_error::Error::coded(
                    afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
                    "OwnerFs HIXL read identity space exhausted",
                )
            })?;
        Ok(format!(
            "{}-{}-{sequence}",
            self.node_id,
            self.native.info().generation
        ))
    }

    /// The Home's session pool. A full table first evicts idle pools:
    /// unsupported Homes and Homes not read from for `HOME_POOL_IDLE`.
    /// `None` means every pool is in use; the caller then treats this Home
    /// like one without the capability.
    fn home_pool(&self, home: &str) -> Option<Arc<HomePool>> {
        let mut homes = self
            .homes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(pool) = homes.get(home) {
            return Some(pool.clone());
        }
        if homes.len() >= MAX_OWNER_HIXL_HOMES {
            homes.retain(|_, pool| Arc::strong_count(pool) > 1 || !pool.is_idle());
        }
        if homes.len() >= MAX_OWNER_HIXL_HOMES {
            return None;
        }
        let pool = Arc::new(HomePool::new(self.policy.sessions_per_home));
        homes.insert(home.to_owned(), pool.clone());
        Some(pool)
    }

    /// Leases one of the Home's control sessions, negotiating it when absent,
    /// stale or about to expire on Home. `None` means Home lacks the
    /// capability or, in preferred mode, refused admission. The pool permit
    /// bounds concurrent fetches per Home; the
    /// slot guard proves every earlier window of that session was released.
    async fn acquire(
        &self,
        home: &str,
        client: &OwnerFilesClient<Channel>,
        access: &RootAccess,
        deadline: Instant,
    ) -> afs_error::Result<Option<HomeLease>> {
        let Some(pool) = self.home_pool(home) else {
            return Ok(None);
        };
        if pool.unsupported() {
            return Ok(None);
        }
        let admission = || {
            afs_error::Error::coded(
                afs_error::CLIENT_DEADLINE_EXCEEDED,
                "OwnerFs HIXL Home admission exceeded the read deadline",
            )
        };
        let permit =
            tokio::time::timeout(remaining(deadline)?, pool.permits.clone().acquire_owned())
                .await
                .map_err(|_| admission())?
                .map_err(|_| protocol_error("OwnerFs HIXL Home pool is closed"))?;
        // One permit per slot, and a lease drops its slot guard before its
        // permit, so a permit holder always finds an unlocked slot.
        let guard = pool
            .slots
            .iter()
            .find_map(|slot| slot.clone().try_lock_owned().ok());
        let mut guard = match guard {
            Some(guard) => guard,
            None => tokio::time::timeout(remaining(deadline)?, pool.slots[0].clone().lock_owned())
                .await
                .map_err(|_| admission())?,
        };
        if pool.unsupported() {
            return Ok(None);
        }
        let idle = guard
            .touched
            .is_some_and(|touched| touched.elapsed() >= SESSION_REUSE_LIMIT);
        let stale = guard.session.as_ref().is_some_and(|session| {
            session.poisoned.load(Ordering::Acquire)
                || session.reply.home_session_id != access.home_session_id
                || idle
        });
        if stale && let Some(session) = guard.session.take() {
            let started = Instant::now();
            let closed = close_session(&session, self.timeout).await;
            self.observe(Operation::Close, started.elapsed(), closed.is_ok());
        }
        if guard.session.is_none() {
            match self.establish(home, client, access, deadline).await? {
                Some(session) => {
                    guard.session = Some(Arc::new(session));
                    pool.set_unsupported(None);
                }
                None => {
                    pool.set_unsupported(Instant::now().checked_add(UNSUPPORTED_RETRY));
                    return Ok(None);
                }
            }
        }
        let session = guard
            .session
            .clone()
            .ok_or_else(|| protocol_error("OwnerFs HIXL session was not published"))?;
        guard.touched = Some(Instant::now());
        Ok(Some(HomeLease {
            session,
            _guard: guard,
            _permit: permit,
        }))
    }

    async fn establish(
        &self,
        home: &str,
        client: &OwnerFilesClient<Channel>,
        access: &RootAccess,
        deadline: Instant,
    ) -> afs_error::Result<Option<HomeSession>> {
        let info = self.native.info();
        let (transport, buffer_pool_count, buffer_pool_size_mib, hccs_domain_id) =
            engine_transport_parameters(&info)?;
        let mut request = afs_tracing::request_with_current_context(OwnerNegotiateHixlRequest {
            access: Some(access.clone()),
            client_engine_id: info.engine_id.clone(),
            client_engine_generation: info.generation,
            protocol_version: HIXL_PROTOCOL_VERSION,
            transport: transport as i32,
            buffer_pool_count,
            buffer_pool_size_mib,
            hccs_domain_id,
            lane_count: u32::try_from(self.native.lane_count())
                .map_err(|_| protocol_error("OwnerFs HIXL lane count overflow"))?,
        });
        request.set_timeout(remaining(deadline)?);
        let mut client = client.clone();
        let started = Instant::now();
        let response = client
            .negotiate_hixl(request)
            .await
            .map_err(status_to_error);
        self.observe(Operation::Negotiate, started.elapsed(), response.is_ok());
        let reply = match response {
            Ok(reply) => reply.into_inner(),
            Err(error) if is_unsupported(&error) => return Ok(None),
            Err(error)
                if self.policy.mode == HixlOwnerFsReadMode::Preferred && is_refused(&error) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        // Home registered this session when it answered; every failure from
        // here on must close it instead of leaving it to the Home TTL.
        let session = HomeSession {
            reply,
            client,
            poisoned: Arc::new(AtomicBool::new(false)),
        };
        if let Err(error) = self
            .admit_negotiated(home, &session.reply, access, deadline)
            .await
        {
            session.poisoned.store(true, Ordering::Release);
            let _ = close_session(&session, self.timeout).await;
            return Err(error);
        }
        Ok(Some(session))
    }

    /// Validates a negotiated reply, binds its engines and connects its lanes.
    async fn admit_negotiated(
        &self,
        home: &str,
        reply: &OwnerNegotiateHixlReply,
        access: &RootAccess,
        deadline: Instant,
    ) -> afs_error::Result<()> {
        self.validate_negotiation(reply, access)?;
        bind_engines(
            &mut self
                .engines
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            reply.lanes.iter().map(|lane| {
                (
                    lane.engine_id.as_str(),
                    (home.to_owned(), lane.engine_generation),
                )
            }),
            MAX_OWNER_HIXL_ENGINES,
        )?;
        let started = Instant::now();
        let connected = connect_lanes(self.native.clone(), reply.lanes.clone(), deadline).await;
        self.observe(Operation::Connect, started.elapsed(), connected.is_ok());
        connected
    }

    fn validate_negotiation(
        &self,
        reply: &OwnerNegotiateHixlReply,
        access: &RootAccess,
    ) -> afs_error::Result<()> {
        let local = self.native.infos();
        let (transport, count, size, domain) = engine_transport_parameters(&self.native.info())?;
        if reply.session_id == 0
            || reply.protocol_version != HIXL_PROTOCOL_VERSION
            || reply.home_session_id != access.home_session_id
            || reply.transport != transport as i32
            || reply.buffer_pool_count != count
            || reply.buffer_pool_size_mib != size
            || reply.hccs_domain_id != domain
            || local.is_empty()
            || reply.lanes.len() != local.len()
        {
            return Err(protocol_error(
                "OwnerFs HIXL negotiation identity or capability is invalid",
            ));
        }
        let mut endpoints = HashSet::new();
        for (index, lane) in reply.lanes.iter().enumerate() {
            if !lane_is_valid(lane, index, &mut endpoints) {
                return Err(protocol_error(
                    "OwnerFs HIXL lane identity or capacity is invalid",
                ));
            }
        }
        Ok(())
    }

    /// Fetches one verified window. Runs as an owned task: the caller may stop
    /// waiting, but the task still completes Release or fencing itself.
    async fn fetch(self: Arc<Self>, request: FetchRequest) -> afs_error::Result<Fetched> {
        let started = Instant::now();
        let result = self.fetch_inner(&request).await;
        if let Some(metrics) = &self.owner_metrics {
            metrics.observe("client", "read_hixl_window", started.elapsed());
        }
        result
    }

    async fn fetch_inner(&self, request: &FetchRequest) -> afs_error::Result<Fetched> {
        let deadline = request.deadline;
        let Some(lease) = self
            .acquire(&request.home, &request.client, &request.access, deadline)
            .await?
        else {
            return Ok(Fetched::Fallback);
        };
        let lane_id = self.bind_lane(&request.home);
        let remote = lease
            .session
            .reply
            .lanes
            .get(lane_id)
            .cloned()
            .ok_or_else(|| protocol_error("OwnerFs HIXL lane is not negotiated"))?;
        let slot_bytes = usize::try_from(remote.window_bytes)
            .map_err(|_| protocol_error("OwnerFs HIXL remote window overflow"))?
            .min(self.slot_bytes());
        let slots = window_slots(
            usize::try_from(remote.export_slots).unwrap_or(0),
            self.policy.receive_slots,
            self.policy.sessions_per_home,
        );
        let capacity = slot_bytes.saturating_mul(slots);
        let length = request.size.min(capacity);
        if length == 0 {
            return Err(protocol_error("OwnerFs HIXL window capacity is empty"));
        }
        let prepare = OwnerPrepareReadHixlRequest {
            session_id: lease.session.reply.session_id,
            read_id: self.next_read_id()?,
            access: Some(request.access.clone()),
            handle: Some(OwnerHandle {
                opaque: request.handle.clone(),
            }),
            offset: request.offset,
            length: length as u64,
            lane_id: u32::try_from(lane_id)
                .map_err(|_| protocol_error("OwnerFs HIXL lane overflow"))?,
            slot_bytes: slot_bytes as u64,
        };
        let started = Instant::now();
        let prepared = prepare_with_retry(&lease.session.client, &prepare, deadline).await;
        self.observe(Operation::Prepare, started.elapsed(), prepared.is_ok());
        let reply = match prepared {
            Ok(reply) => reply,
            Err(error) if is_capacity(&error) => return Ok(Fetched::Exhausted),
            Err(error) if is_unsupported(&error) => {
                lease.session.poisoned.store(true, Ordering::Release);
                return Ok(Fetched::Fallback);
            }
            Err(error)
                if self.policy.mode == HixlOwnerFsReadMode::Preferred && is_refused(&error) =>
            {
                lease.session.poisoned.store(true, Ordering::Release);
                if let Some(pool) = self.home_pool(&request.home) {
                    pool.set_unsupported(Instant::now().checked_add(UNSUPPORTED_RETRY));
                }
                return Ok(Fetched::Fallback);
            }
            Err(error) => {
                if fences_session(&error) {
                    lease.session.poisoned.store(true, Ordering::Release);
                }
                return Err(error);
            }
        };
        if let Err(error) = validate_prepare(&prepare, &reply, &remote, slot_bytes, slots) {
            lease.session.poisoned.store(true, Ordering::Release);
            if !reply.slots.is_empty() {
                let _ = self
                    .release(
                        &lease.session,
                        &reply,
                        DfsHixlReleaseOutcome::NotSubmitted,
                        false,
                    )
                    .await;
            }
            return Err(error);
        }
        if reply.read == 0 {
            return Ok(Fetched::Window(FetchedWindow {
                offset: request.offset,
                bytes: Vec::new(),
                eof: true,
            }));
        }
        let read = usize::try_from(reply.read)
            .map_err(|_| protocol_error("OwnerFs HIXL read length overflow"))?;
        let native = self
            .native
            .lane(lane_id)
            .ok_or_else(|| protocol_error("OwnerFs HIXL local lane disappeared"))?;
        let buffer = match native.prepare_read_batch_buffer(read) {
            Ok(buffer) => buffer,
            Err(error) => {
                self.release_detached(lease, reply, DfsHixlReleaseOutcome::NotSubmitted, false);
                return Err(runtime_error(error));
            }
        };
        let windows = reply
            .slots
            .iter()
            .map(|slot| Ok((slot.address, usize::try_from(slot.length)?)))
            .collect::<Result<Vec<_>, std::num::TryFromIntError>>()
            .map_err(|_| protocol_error("OwnerFs HIXL slot length overflow"))?;
        let engine = reply.engine_id.clone();
        let started = Instant::now();
        let transfer = tokio::task::spawn_blocking(move || {
            native.read_batch_timed_into(&engine, &windows, buffer, deadline)
        })
        .await;
        let elapsed = started.elapsed();
        let outcome = match transfer {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(error)) => {
                self.observe(Operation::Read, elapsed, false);
                let uncertain = uncertain_read(&error);
                let release_outcome = failed_read_outcome(&error);
                if uncertain {
                    self.quarantine_local().await;
                }
                let exhausted = !uncertain && error.kind == HixlErrorKind::ResourceExhausted;
                if !exhausted {
                    lease.session.poisoned.store(true, Ordering::Release);
                }
                self.release_detached(lease, reply, release_outcome, false);
                if exhausted {
                    return Ok(Fetched::Exhausted);
                }
                return Err(runtime_error(error));
            }
            Err(error) => {
                self.observe(Operation::Read, elapsed, false);
                lease.session.poisoned.store(true, Ordering::Release);
                self.quarantine_local().await;
                self.release_detached(lease, reply, DfsHixlReleaseOutcome::Unknown, false);
                return Err(afs_error::Error::coded(
                    afs_error::CLIENT_WORKER_FAILED,
                    error.to_string(),
                ));
            }
        };
        self.observe(Operation::Read, elapsed, true);
        if let Some(metrics) = &self.metrics {
            metrics.observe_lane_read(lane_id, elapsed, true);
            metrics.observe_read_timing(&outcome.timing);
            metrics.observe_native_batch(reply.slots.len(), read);
        }
        let started = Instant::now();
        // Slot hashing waits on the validation pool; keep it off async workers.
        let checked = reply.clone();
        let validation = tokio::task::spawn_blocking(move || {
            let bytes = outcome.bytes;
            let validated = bytes.len() == read && validate_slots(&checked, &bytes);
            (bytes, validated)
        })
        .await;
        let (bytes, validated) = match validation {
            Ok(validation) => validation,
            Err(error) => {
                lease.session.poisoned.store(true, Ordering::Release);
                self.release_detached(lease, reply, DfsHixlReleaseOutcome::Completed, false);
                return Err(afs_error::Error::coded(
                    afs_error::CLIENT_WORKER_FAILED,
                    error.to_string(),
                ));
            }
        };
        self.observe(Operation::Validate, started.elapsed(), validated);
        if !validated {
            lease.session.poisoned.store(true, Ordering::Release);
            self.release_detached(lease, reply, DfsHixlReleaseOutcome::Completed, false);
            return Err(afs_error::Error::coded(
                afs_error::NODE_TRANSFER_CORRUPT_DATA,
                "OwnerFs HIXL slot checksum differs",
            ));
        }
        if let Some(metrics) = &self.metrics {
            metrics.record_validated_bytes(reply.read);
            metrics.record_lane_validated_bytes(lane_id, reply.read);
        }
        let eof = reply.eof;
        self.release_detached(lease, reply, DfsHixlReleaseOutcome::Completed, true);
        Ok(Fetched::Window(FetchedWindow {
            offset: request.offset,
            bytes,
            eof,
        }))
    }

    /// Releases in the background while still holding the Home lease, so the
    /// verified window reaches the caller one control round trip earlier and
    /// the next fetch on this Home still observes every slot released.
    fn release_detached(
        &self,
        lease: HomeLease,
        reply: OwnerPrepareReadHixlReply,
        outcome: DfsHixlReleaseOutcome,
        content_validated: bool,
    ) {
        let timeout = self.timeout;
        let metrics = self.metrics.clone();
        let task = async move {
            let started = Instant::now();
            let result =
                release_slots(&lease.session, &reply, outcome, content_validated, timeout).await;
            observe_optional(
                metrics.as_ref(),
                Operation::Release,
                started.elapsed(),
                result.is_ok(),
            );
            if result.is_err() || outcome == DfsHixlReleaseOutcome::Unknown {
                lease.session.poisoned.store(true, Ordering::Release);
            }
            drop(lease);
        };
        tokio::spawn(task);
    }

    async fn release(
        &self,
        session: &HomeSession,
        reply: &OwnerPrepareReadHixlReply,
        outcome: DfsHixlReleaseOutcome,
        content_validated: bool,
    ) -> afs_error::Result<()> {
        let started = Instant::now();
        let result = release_slots(session, reply, outcome, content_validated, self.timeout).await;
        self.observe(Operation::Release, started.elapsed(), result.is_ok());
        result
    }

    async fn quarantine_local(&self) {
        let native = self.native.clone();
        // Only an uncertain local native lifetime fences the local group.
        let _ = tokio::task::spawn_blocking(move || native.quarantine()).await;
    }
}

#[derive(Clone, Copy)]
enum ReadOutcome {
    WindowHit,
    Fetch,
    PrefetchHit,
    Fallback,
}

impl ReadOutcome {
    fn label(self) -> &'static str {
        match self {
            Self::WindowHit => "window_hit",
            Self::Fetch => "fetch",
            Self::PrefetchHit => "prefetch_hit",
            Self::Fallback => "fallback",
        }
    }
}

#[derive(Default)]
struct HomeSlot {
    session: Option<Arc<HomeSession>>,
    touched: Option<Instant>,
}

/// Up to `sessions_per_home` control sessions of one Home.
struct HomePool {
    permits: Arc<tokio::sync::Semaphore>,
    slots: Vec<Arc<Mutex<HomeSlot>>>,
    unsupported_until: StdMutex<Option<Instant>>,
}

impl HomePool {
    fn new(sessions: usize) -> Self {
        let sessions = sessions.max(1);
        Self {
            permits: Arc::new(tokio::sync::Semaphore::new(sessions)),
            slots: (0..sessions)
                .map(|_| Arc::new(Mutex::new(HomeSlot::default())))
                .collect(),
            unsupported_until: StdMutex::new(None),
        }
    }

    fn unsupported(&self) -> bool {
        self.unsupported_until
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some_and(|until| Instant::now() < until)
    }

    fn set_unsupported(&self, until: Option<Instant>) {
        *self
            .unsupported_until
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = until;
    }

    /// No lease holds a slot, and every session left in a slot is one Home
    /// has already closed. An unlocked slot has released all its windows.
    fn is_idle(&self) -> bool {
        self.slots.iter().all(|slot| {
            slot.try_lock().is_ok_and(|slot| {
                slot.session.is_none()
                    || slot
                        .touched
                        .is_none_or(|touched| touched.elapsed() >= HOME_POOL_IDLE)
            })
        })
    }
}

/// Slots one window may use. Every pooled session can hold a window on the
/// Home lane at once, so the sessions split that lane's export pool; one
/// native batch READ needs one local receive slot per window slot.
fn window_slots(remote_export_slots: usize, receive_slots: usize, sessions: usize) -> usize {
    (remote_export_slots / sessions.max(1))
        .min(receive_slots)
        .min(MAX_OWNER_HIXL_SLOTS)
}

struct HomeSession {
    reply: OwnerNegotiateHixlReply,
    client: OwnerFilesClient<Channel>,
    poisoned: Arc<AtomicBool>,
}

/// Holding the guard proves every earlier window of this session was
/// released. Fields drop in order: the slot unlocks before its permit returns.
struct HomeLease {
    session: Arc<HomeSession>,
    _guard: OwnedMutexGuard<HomeSlot>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

struct FetchRequest {
    home: String,
    client: OwnerFilesClient<Channel>,
    access: RootAccess,
    handle: Vec<u8>,
    offset: u64,
    size: usize,
    deadline: Instant,
}

struct FetchedWindow {
    offset: u64,
    bytes: Vec<u8>,
    eof: bool,
}

enum Fetched {
    Window(FetchedWindow),
    /// Home lacks the capability; preferred mode may serve the READ by gRPC.
    Fallback,
    /// A definite slot or budget shortfall before any native submission.
    Exhausted,
}

fn protocol_error(message: &'static str) -> afs_error::Error {
    afs_error::Error::coded(afs_error::CLIENT_PROTOCOL_VIOLATION, message)
}

fn is_capacity(error: &afs_error::Error) -> bool {
    error.code() == afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED
        || error.code() == afs_error::IO_CAPACITY
}

/// Transport, protocol and session failures retire the Home session.
/// Authorization, handle and file I/O failures belong to one open.
fn fences_session(error: &afs_error::Error) -> bool {
    matches!(
        error.code(),
        afs_error::NODE_TRANSFER_UNAVAILABLE
            | afs_error::NODE_TRANSFER_INVALID
            | afs_error::NODE_TRANSFER_INTERNAL
            | afs_error::NODE_TRANSFER_DRAIN_INCOMPLETE
            | afs_error::NODE_TRANSFER_CORRUPT_DATA
            | afs_error::CLIENT_REMOTE_STATUS
            | afs_error::CLIENT_PROTOCOL_VIOLATION
            | afs_error::CLIENT_PROTOCOL_INVALID_ERROR_DETAIL
            | afs_error::CLIENT_CONNECTION_UNAVAILABLE
            | afs_error::CLIENT_DEADLINE_EXCEEDED
            | afs_error::CLIENT_WORKER_FAILED
    )
}

/// Only a lost transport outcome replays the identical attempt; Home answers
/// it from the admitted record.
fn retries_prepare(error: &afs_error::Error) -> bool {
    matches!(
        error.kind(),
        afs_error::ErrorKind::Unavailable | afs_error::ErrorKind::DeadlineExceeded
    ) && error.code() != afs_error::NODE_TRANSFER_UNAVAILABLE
        && error.code() != afs_error::NODE_OWNER_GRANT_UNAVAILABLE
}

async fn prepare_with_retry(
    client: &OwnerFilesClient<Channel>,
    prepare: &OwnerPrepareReadHixlRequest,
    deadline: Instant,
) -> afs_error::Result<OwnerPrepareReadHixlReply> {
    let mut last = None;
    for _ in 0..PREPARE_ATTEMPTS {
        let mut command = afs_tracing::request_with_current_context(prepare.clone());
        command.set_timeout(remaining(deadline)?);
        let mut client = client.clone();
        match client.prepare_read_hixl(command).await {
            Ok(reply) => return Ok(reply.into_inner()),
            Err(status) => {
                let error = status_to_error(status);
                if !retries_prepare(&error) {
                    return Err(error);
                }
                last = Some(error);
            }
        }
    }
    Err(last.unwrap_or_else(|| protocol_error("OwnerFs HIXL prepare produced no result")))
}

fn validate_prepare(
    request: &OwnerPrepareReadHixlRequest,
    reply: &OwnerPrepareReadHixlReply,
    lane: &afs_protocol::node_data::DfsHixlLane,
    slot_bytes: usize,
    max_slots: usize,
) -> afs_error::Result<()> {
    if reply.session_id != request.session_id
        || reply.read_id != request.read_id
        || reply.lane_id != request.lane_id
        || reply.read > request.length
        || reply.eof != (reply.read < request.length)
    {
        return Err(protocol_error(
            "OwnerFs HIXL prepare reply identity differs",
        ));
    }
    if reply.read == 0 {
        return if reply.slots.is_empty() {
            Ok(())
        } else {
            Err(protocol_error("OwnerFs HIXL empty read carries slots"))
        };
    }
    if reply.engine_id != lane.engine_id
        || reply.engine_generation != lane.engine_generation
        || reply.slots.is_empty()
        || reply.slots.len() > max_slots
    {
        return Err(protocol_error("OwnerFs HIXL prepare reply lane differs"));
    }
    let mut total = 0_u64;
    for slot in &reply.slots {
        if slot.address == 0
            || slot.slot_generation == 0
            || slot.length == 0
            || usize::try_from(slot.length).map_or(true, |length| length > slot_bytes)
            || slot.checksum.len() != blake3::OUT_LEN
        {
            return Err(protocol_error("OwnerFs HIXL slot descriptor is invalid"));
        }
        total = total
            .checked_add(slot.length)
            .ok_or_else(|| protocol_error("OwnerFs HIXL slot length overflow"))?;
    }
    if total != reply.read {
        return Err(protocol_error("OwnerFs HIXL slots do not cover the read"));
    }
    Ok(())
}

/// Verifies every slot digest on the shared bounded hash pool.
fn validate_slots(reply: &OwnerPrepareReadHixlReply, bytes: &[u8]) -> bool {
    let mut ranges = Vec::with_capacity(reply.slots.len());
    let mut offset = 0_usize;
    for slot in &reply.slots {
        let Ok(length) = usize::try_from(slot.length) else {
            return false;
        };
        let Some(end) = offset.checked_add(length) else {
            return false;
        };
        let Some(data) = bytes.get(offset..end) else {
            return false;
        };
        ranges.push((data, slot.checksum.as_slice()));
        offset = end;
    }
    if offset != bytes.len() {
        return false;
    }
    let valid = AtomicBool::new(true);
    let check = |index: usize| {
        let (data, expected) = ranges[index];
        if blake3::hash(data).as_bytes().as_slice() != expected {
            valid.store(false, Ordering::Release);
        }
    };
    match validation_pool() {
        Some(pool) if ranges.len() > 1 => pool.install(|| {
            rayon_core::scope(|scope| {
                for index in 0..ranges.len() {
                    scope.spawn(move |_| check(index));
                }
            });
        }),
        _ => (0..ranges.len()).for_each(check),
    }
    valid.load(Ordering::Acquire)
}

async fn release_slots(
    session: &HomeSession,
    reply: &OwnerPrepareReadHixlReply,
    outcome: DfsHixlReleaseOutcome,
    content_validated: bool,
    timeout: Duration,
) -> afs_error::Result<()> {
    let request = OwnerReleaseHixlReadRequest {
        session_id: reply.session_id,
        read_id: reply.read_id.clone(),
        engine_generation: reply.engine_generation,
        outcome: outcome as i32,
        content_validated,
    };
    let mut last = None;
    for _ in 0..RELEASE_ATTEMPTS {
        let mut command = afs_tracing::request_with_current_context(request.clone());
        command.set_timeout(timeout);
        let mut client = session.client.clone();
        match client.release_hixl_read(command).await {
            Ok(response) => {
                let response = response.into_inner();
                let pinned = outcome == DfsHixlReleaseOutcome::Unknown;
                if response.consumed == pinned {
                    return Err(protocol_error(
                        "OwnerFs HIXL release result differs from the reported outcome",
                    ));
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
                last = Some(error);
            }
        }
    }
    Err(last.unwrap_or_else(|| protocol_error("OwnerFs HIXL release produced no result")))
}

async fn close_session(session: &HomeSession, timeout: Duration) -> afs_error::Result<()> {
    let mut request = afs_tracing::request_with_current_context(OwnerCloseHixlRequest {
        session_id: session.reply.session_id,
    });
    request.set_timeout(timeout);
    let mut client = session.client.clone();
    let reply = client
        .close_hixl(request)
        .await
        .map_err(status_to_error)?
        .into_inner();
    if !reply.drained {
        return Err(afs_error::Error::coded(
            afs_error::NODE_TRANSFER_DRAIN_INCOMPLETE,
            "OwnerFs HIXL Home session remains unresolved",
        ));
    }
    Ok(())
}

/// Shared byte budget for cached and prefetching windows.
struct WindowBudget {
    limit: u64,
    used: AtomicU64,
}

impl WindowBudget {
    fn new(limit: u64) -> Self {
        Self {
            limit,
            used: AtomicU64::new(0),
        }
    }

    fn reserve(self: &Arc<Self>, bytes: usize) -> Option<BudgetLease> {
        let bytes = u64::try_from(bytes).ok()?;
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|total| *total <= self.limit)
            })
            .ok()?;
        Some(BudgetLease {
            budget: self.clone(),
            bytes,
        })
    }
}

struct BudgetLease {
    budget: Arc<WindowBudget>,
    bytes: u64,
}

impl Drop for BudgetLease {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

/// A verified window owned by one open.
struct CachedWindow {
    offset: u64,
    bytes: Vec<u8>,
    eof: bool,
    epoch: u64,
    _budget: Option<BudgetLease>,
}

impl CachedWindow {
    fn end(&self) -> u64 {
        self.offset.saturating_add(self.bytes.len() as u64)
    }

    /// Copies from `cursor` when it falls inside the window.
    fn copy_at(&self, cursor: u64, out: &mut [u8]) -> usize {
        if cursor < self.offset || cursor >= self.end() {
            return 0;
        }
        let Ok(start) = usize::try_from(cursor - self.offset) else {
            return 0;
        };
        let count = (self.bytes.len() - start).min(out.len());
        out[..count].copy_from_slice(&self.bytes[start..start + count]);
        count
    }
}

struct PendingWindow {
    offset: u64,
    epoch: u64,
    receiver: oneshot::Receiver<afs_error::Result<Fetched>>,
    budget: Option<BudgetLease>,
}

#[derive(Default)]
struct WindowState {
    window: Option<CachedWindow>,
    pending: Option<PendingWindow>,
    next_offset: u64,
    ramp: usize,
    /// READs in a row that started where the previous one ended.
    streak: u32,
}

/// Consecutive sequential READs that confirm a stream after a jump. The kernel
/// readahead that follows one random read is a single sequential READ, so it
/// alone does not open a window.
const SEQUENTIAL_STREAK: u32 = 2;

struct OpenEntry {
    identity: Vec<u8>,
    epoch: AtomicU64,
    state: StdMutex<WindowState>,
}

/// Result of a window read for one caller buffer.
pub(super) enum WindowRead {
    /// Every requested byte, or a short count that stops at a recorded EOF.
    Done(usize),
    /// The first `served` bytes came from windows; the caller reads the rest
    /// through the original gRPC path.
    Fallback { served: usize },
}

/// Per-`OwnerPeerClient` open registry. Absent for off mode, so every hook
/// below leaves the original read path untouched.
pub(super) struct OwnerHixlFiles {
    reader: Arc<OwnerHixlReader>,
    home: String,
    opens: StdMutex<HashMap<Vec<u8>, Arc<OpenEntry>>>,
}

impl OwnerHixlFiles {
    pub(super) fn new(reader: Arc<OwnerHixlReader>, home: String) -> Self {
        Self {
            reader,
            home,
            opens: StdMutex::new(HashMap::new()),
        }
    }

    fn opens(&self) -> std::sync::MutexGuard<'_, HashMap<Vec<u8>, Arc<OpenEntry>>> {
        self.opens
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Records a remote open as eligible: read-only, not `O_PATH`/`O_DIRECT`,
    /// and in preferred mode at least `min_file_bytes` long at open.
    pub(super) fn opened(&self, handle: &[u8], identity: &[u8], flags: i32, size: u64) {
        let policy = self.reader.policy;
        let eligible = flags & libc::O_ACCMODE == libc::O_RDONLY
            && flags & (libc::O_PATH | libc::O_DIRECT) == 0
            && (policy.mode == HixlOwnerFsReadMode::Required || size >= policy.min_file_bytes);
        if eligible {
            self.opens().insert(
                handle.to_vec(),
                Arc::new(OpenEntry {
                    identity: identity.to_vec(),
                    epoch: AtomicU64::new(0),
                    state: StdMutex::new(WindowState::default()),
                }),
            );
        }
    }

    pub(super) fn released(&self, handle: &[u8]) {
        self.opens().remove(handle);
    }

    /// Drops every window and prefetch of a file after this node changed it.
    pub(super) fn invalidate(&self, identity: &[u8]) {
        if identity.is_empty() {
            return;
        }
        for entry in self.opens().values() {
            if entry.identity == identity {
                entry.epoch.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    /// Serves `out` from this open's windows, fetching missing windows through
    /// HIXL. Must run outside the async runtime, like every OwnerFiles call.
    pub(super) fn read(
        &self,
        runtime: &OwnerRuntime,
        client: &OwnerFilesClient<Channel>,
        grant: &RootGrant,
        file: &RemoteFile,
        offset: u64,
        out: &mut [u8],
    ) -> afs_error::Result<WindowRead> {
        let Some(entry) = self.opens().get(&file.handle).cloned() else {
            return Ok(WindowRead::Fallback { served: 0 });
        };
        let Some(handle) = runtime.handle() else {
            return Ok(WindowRead::Fallback { served: 0 });
        };
        let mut state = entry
            .state
            .lock()
            .map_err(|_| protocol_error("OwnerFs HIXL window lock poisoned"))?;
        let epoch = entry.epoch.load(Ordering::Acquire);
        if state
            .window
            .as_ref()
            .is_some_and(|window| window.epoch != epoch)
        {
            state.window = None;
        }
        if state
            .pending
            .as_ref()
            .is_some_and(|pending| pending.epoch != epoch)
        {
            state.pending = None;
        }
        state.streak = if offset == state.next_offset {
            state.streak.saturating_add(1)
        } else {
            0
        };
        // A stream starts at offset 0, or continues after a confirmed streak.
        let stream =
            offset == state.next_offset && (offset == 0 || state.streak >= SEQUENTIAL_STREAK);
        let reader = &self.reader;
        let policy = reader.policy;
        let mut served = 0_usize;
        let mut cursor = offset;
        let mut fetched = false;
        while served < out.len() {
            if let Some(window) = &state.window {
                let copied = window.copy_at(cursor, &mut out[served..]);
                if copied > 0 {
                    served += copied;
                    cursor += copied as u64;
                    continue;
                }
                if window.eof && cursor >= window.end() {
                    break;
                }
            }
            let wanted = out.len() - served;
            let sequential = served > 0 || stream;
            if let Some(pending) = state.pending.take()
                && pending.offset == cursor
            {
                let waited = handle.block_on(async {
                    match remaining(Instant::now() + reader.timeout) {
                        Ok(wait) => tokio::time::timeout(wait, pending.receiver).await.ok(),
                        Err(_) => None,
                    }
                });
                if let Some(Ok(Ok(Fetched::Window(window)))) = waited
                    && entry.epoch.load(Ordering::Acquire) == epoch
                {
                    state.window = Some(CachedWindow {
                        offset: window.offset,
                        bytes: window.bytes,
                        eof: window.eof,
                        epoch,
                        _budget: pending.budget,
                    });
                    reader.count(ReadOutcome::PrefetchHit);
                    continue;
                }
                // A failed or stale prefetch is discarded; fetch normally below.
            }
            let size = if sequential {
                state.ramp = next_ramp(state.ramp, reader.slot_bytes(), policy.max_window_bytes);
                state.ramp.max(wanted)
            } else if policy.mode == HixlOwnerFsReadMode::Required {
                state.ramp = 0;
                wanted
            } else {
                state.ramp = 0;
                return self.fallback(&mut state, offset, served, out.len());
            };
            // The cached window is replaced; free its budget first. Every
            // window outlives this READ, so each one holds budget for its
            // bytes; a READ the budget cannot cover is served without one.
            state.window = None;
            let (size, budget) = match reader.budget.reserve(size) {
                Some(budget) => (size, Some(budget)),
                None if size > wanted => match reader.budget.reserve(wanted) {
                    Some(budget) => (wanted, Some(budget)),
                    None => (wanted, None),
                },
                None => (size, None),
            };
            if budget.is_none() && policy.mode != HixlOwnerFsReadMode::Required {
                return self.fallback(&mut state, offset, served, out.len());
            }
            let request = FetchRequest {
                home: self.home.clone(),
                client: client.clone(),
                access: super::root_access(grant),
                handle: file.handle.clone(),
                offset: cursor,
                size,
                deadline: Instant::now()
                    .checked_add(reader.timeout)
                    .ok_or_else(|| protocol_error("OwnerFs HIXL deadline overflow"))?,
            };
            let deadline = request.deadline;
            let task = handle.spawn(reader.clone().fetch(request));
            let result = handle.block_on(async move {
                tokio::time::timeout(remaining(deadline)?, task)
                    .await
                    .map_err(|_| {
                        afs_error::Error::coded(
                            afs_error::CLIENT_DEADLINE_EXCEEDED,
                            "OwnerFs HIXL window fetch exceeded its deadline",
                        )
                    })?
                    .map_err(|error| {
                        afs_error::Error::coded(afs_error::CLIENT_WORKER_FAILED, error.to_string())
                    })?
            })?;
            match result {
                // Without budget the window serves only this READ.
                Fetched::Window(window) if budget.is_none() => {
                    fetched = true;
                    let transient = CachedWindow {
                        offset: window.offset,
                        bytes: window.bytes,
                        eof: window.eof,
                        epoch,
                        _budget: None,
                    };
                    let copied = transient.copy_at(cursor, &mut out[served..]);
                    served += copied;
                    cursor += copied as u64;
                    if copied == 0 {
                        break;
                    }
                }
                Fetched::Window(window) => {
                    fetched = true;
                    state.window = Some(CachedWindow {
                        offset: window.offset,
                        bytes: window.bytes,
                        eof: window.eof,
                        epoch,
                        _budget: budget,
                    });
                }
                Fetched::Fallback | Fetched::Exhausted
                    if policy.mode == HixlOwnerFsReadMode::Preferred =>
                {
                    return self.fallback(&mut state, offset, served, out.len());
                }
                Fetched::Fallback => {
                    return Err(afs_error::Error::coded(
                        afs_error::NODE_TRANSFER_UNSUPPORTED,
                        "OwnerFs HIXL is required but Home does not support it",
                    ));
                }
                Fetched::Exhausted => {
                    return Err(afs_error::Error::coded(
                        afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
                        "OwnerFs HIXL slots are exhausted and fallback is disabled",
                    ));
                }
            }
        }
        state.next_offset = offset.saturating_add(served as u64);
        reader.count(if fetched {
            ReadOutcome::Fetch
        } else {
            ReadOutcome::WindowHit
        });
        if let Some(metrics) = &reader.owner_metrics {
            metrics.record_payload("client", "read", "hixl", served as u64);
        }
        if policy.max_inflight > 1 {
            self.start_prefetch(&handle, &mut state, grant, file, client, epoch);
        }
        Ok(WindowRead::Done(served))
    }

    fn fallback(
        &self,
        state: &mut WindowState,
        offset: u64,
        served: usize,
        requested: usize,
    ) -> afs_error::Result<WindowRead> {
        state.next_offset = offset.saturating_add(requested as u64);
        self.reader.count(ReadOutcome::Fallback);
        if served > 0
            && let Some(metrics) = &self.reader.owner_metrics
        {
            metrics.record_payload("client", "read", "hixl", served as u64);
        }
        Ok(WindowRead::Fallback { served })
    }

    /// Prefetches the next window once the consumer passes the middle of the
    /// current one. The task owns its lease and releases its slots itself.
    fn start_prefetch(
        &self,
        handle: &tokio::runtime::Handle,
        state: &mut WindowState,
        grant: &RootGrant,
        file: &RemoteFile,
        client: &OwnerFilesClient<Channel>,
        epoch: u64,
    ) {
        let Some(window) = &state.window else {
            return;
        };
        let middle = window
            .offset
            .saturating_add((window.bytes.len() / 2) as u64);
        if state.pending.is_some()
            || window.eof
            || state.next_offset < middle
            || state.next_offset >= window.end()
        {
            return;
        }
        let offset = window.end();
        let reader = &self.reader;
        let size = next_ramp(
            state.ramp,
            reader.slot_bytes(),
            reader.policy.max_window_bytes,
        );
        let Some(budget) = reader.budget.reserve(size) else {
            return;
        };
        let Some(deadline) = Instant::now().checked_add(reader.timeout) else {
            return;
        };
        state.ramp = size;
        let request = FetchRequest {
            home: self.home.clone(),
            client: client.clone(),
            access: super::root_access(grant),
            handle: file.handle.clone(),
            offset,
            size,
            deadline,
        };
        let (sender, receiver) = oneshot::channel();
        let fetch = reader.clone().fetch(request);
        handle.spawn(async move {
            let _ = sender.send(fetch.await);
        });
        state.pending = Some(PendingWindow {
            offset,
            epoch,
            receiver,
            budget: Some(budget),
        });
    }
}

/// Doubles a sequential window from one slot up to the configured maximum.
fn next_ramp(previous: usize, slot_bytes: usize, max_window: usize) -> usize {
    let slot = slot_bytes.max(1);
    if previous == 0 {
        slot.min(max_window.max(slot))
    } else {
        previous.saturating_mul(2).min(max_window.max(slot))
    }
}

#[cfg(test)]
mod tests;
