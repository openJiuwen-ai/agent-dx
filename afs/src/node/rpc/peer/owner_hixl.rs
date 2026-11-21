//! Reader-side OwnerFs HIXL read-ahead windows.
//!
//! Only read-only remote opens are eligible. A READ that misses the open's
//! window asks Home to prepare `[offset, offset + size)` of that open handle,
//! pulls every exported slot through one native batch READ, verifies each slot
//! digest and parks the release on the session: the next prepare of that
//! session carries it, or an idle flush sends it. The verified bytes become
//! the open's window: later READs inside it are local copies. Like the OPEN
//! prefetch, a window is the Home content at prepare time, reused only by the
//! same open and dropped at release or after this node writes the same file.

mod batch;

mod read_session;
pub(super) use read_session::{OwnerReadSession, WindowRead};

use super::hixl::{
    bind_engines, connect_lanes, engine_transport_parameters, lane_is_valid, least_used_lane,
    remaining,
};
use crate::config::DataMode;
use crate::node::data_plane::hixl::{
    Completion, ReadRuntime, ReadWindow, SlotChecksum, checked_slot_length, read_batch,
};
use crate::node::rpc::OwnerRpcMetrics;
use crate::node::rpc::hixl::{
    HIXL_PROTOCOL_VERSION, HixlMetrics, Operation, is_unsupported, observe_optional,
};
use afs_protocol::node_data::{
    HixlLane, HixlReleaseOutcome, OwnerCloseHixlRequest, OwnerHandle, OwnerHixlSlot,
    OwnerNegotiateHixlReply, OwnerNegotiateHixlRequest, OwnerPrepareBatchHixlReply,
    OwnerPrepareReadHixlReply, OwnerPrepareReadHixlRequest, OwnerReleaseHixlReadRequest,
    RootAccess, owner_files_client::OwnerFilesClient,
};
use afs_transport::grpc::error_status::status_to_error;
use afs_transport::hixl::{HixlRuntimeGroup, MAX_BATCH_WINDOWS};
use prost::bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, OwnedMutexGuard};
use tonic::transport::Channel;

const MAX_OWNER_HIXL_HOMES: usize = 64;
const MAX_OWNER_HIXL_ENGINES: usize = MAX_OWNER_HIXL_HOMES * 16;
/// Home closes a session after 30 s without traffic; reuse stays below that.
const MAX_SESSION_IDLE: Duration = Duration::from_secs(20);
/// Cache an explicit lack of support for this Home generation.
const UNSUPPORTED_RETRY: Duration = Duration::from_secs(60);
/// Home closes a session after 30 s without activity; a pool unused this long
/// keeps nothing Home still serves and may be evicted.
const HOME_POOL_IDLE: Duration = Duration::from_secs(60);
const PREPARE_ATTEMPTS: usize = 3;
/// Mirrors the `[hixl].ownerfs_sessions_per_home` configuration bound.
const MAX_OWNER_HIXL_SESSIONS_PER_HOME: usize = 4;
const RELEASE_ATTEMPTS: usize = 3;
/// A parked release that no prepare of its session carries within this long
/// is sent on its own, so idle slots return to Home promptly.
const RELEASE_PARK: Duration = Duration::from_millis(10);

/// Window sizing shared by every open of this process.
#[derive(Clone, Copy, Debug)]
pub struct OwnerHixlPolicy {
    pub mode: DataMode,
    pub max_group_bytes: usize,
    pub max_inflight: usize,
    pub host_budget_bytes: u64,
    pub min_file_bytes: u64,
    /// Local receive slots of each lane: one native batch READ needs one per slot.
    pub receive_slots: usize,
    /// Control sessions per Home. Each session admits one window fetch, so
    /// N sessions let N opens of one Home prepare and transfer concurrently.
    pub sessions_per_home: usize,
    /// Reader reuse limit. Home closes its own inactive sessions separately.
    pub session_idle: Duration,
    /// Read-only opens above the OPEN prefetch limit and at most this long are
    /// served from directory batches; 0 disables batches.
    pub batch_max_file_bytes: u64,
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
    /// Homes with a background warm in flight; a second warm of one is skipped.
    warming: StdMutex<HashSet<String>>,
    engines: StdMutex<HashMap<SocketAddr, (String, u64)>>,
    lane_bindings: StdMutex<HashMap<String, usize>>,
    next_read: AtomicU64,
    release_park: Duration,
    metrics: Option<HixlMetrics>,
    owner_metrics: Option<OwnerRpcMetrics>,
}

impl OwnerHixlReader {
    /// Construction fails for a non-HIXL mode, an invalid deadline or an empty runtime.
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
        if !matches!(policy.mode, DataMode::Auto | DataMode::Hixl)
            || timeout.is_zero()
            || timeout > Duration::from_secs(300)
            || native.lane_count() == 0
            || policy.max_group_bytes == 0
            || policy.receive_slots == 0
            || !(1..=2).contains(&policy.max_inflight)
            || !(1..=MAX_OWNER_HIXL_SESSIONS_PER_HOME).contains(&policy.sessions_per_home)
            || !(Duration::from_millis(1)..=MAX_SESSION_IDLE).contains(&policy.session_idle)
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
            warming: StdMutex::new(HashSet::new()),
            engines: StdMutex::new(HashMap::new()),
            lane_bindings: StdMutex::new(HashMap::new()),
            next_read: AtomicU64::new(1),
            release_park: RELEASE_PARK,
            metrics: None,
            owner_metrics: None,
        })
    }

    #[cfg(test)]
    fn with_release_park(mut self, park: Duration) -> Self {
        self.release_park = park;
        self
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

    /// Binds a Home to the least-used lane.
    fn bind_lane(&self, home: &str) -> usize {
        least_used_lane(
            &self.lane_bindings,
            home,
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
    /// `None` means every pool is in use.
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

    /// Negotiates and connects one session of `home` in the background, so
    /// the first window fetch does not pay the native connect. The connection
    /// outlives the session: a later renegotiation only exchanges identities.
    /// Failures are logged and left to the read path, which retries on demand.
    fn warm(
        self: &Arc<Self>,
        handle: &tokio::runtime::Handle,
        home: String,
        client: OwnerFilesClient<Channel>,
        access: RootAccess,
    ) {
        if !self
            .warming
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(home.clone())
        {
            return;
        }
        let reader = self.clone();
        handle.spawn(async move {
            let warmed = match Instant::now().checked_add(reader.timeout) {
                Some(deadline) => reader
                    .acquire(&home, &client, &access, deadline)
                    .await
                    .map(|lease| lease.is_some()),
                None => Err(protocol_error("OwnerFs HIXL deadline overflow")),
            };
            reader
                .warming
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&home);
            match warmed {
                Ok(true) => afs_logging::info!("ownerfs.hixl_prewarm_connected";
                    "home" => home.as_str(),
                ),
                Ok(false) => afs_logging::info!("ownerfs.hixl_prewarm_unavailable";
                    "home" => home.as_str(),
                ),
                Err(error) => afs_logging::warn!("ownerfs.hixl_prewarm_failed";
                    "home" => home.as_str(),
                    "code" => error.code().to_string(),
                    "error" => error.to_string(),
                ),
            }
        });
    }

    /// Leases one of the Home's control sessions, negotiating it when absent,
    /// stale or about to expire on Home. `None` means Home lacks the
    /// capability. The pool permit bounds concurrent fetches per Home; the
    /// slot guard proves every earlier window of that session was released.
    async fn acquire(
        &self,
        home: &str,
        client: &OwnerFilesClient<Channel>,
        access: &RootAccess,
        deadline: Instant,
    ) -> afs_error::Result<Option<HomeLease>> {
        let pool = self.home_pool(home).ok_or_else(|| {
            afs_error::Error::coded(
                afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
                "OwnerFs HIXL Home session table is full",
            )
        })?;
        if pool.unsupported(&access.home_session_id) {
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
        if pool.unsupported(&access.home_session_id) {
            return Ok(None);
        }
        let idle = guard
            .touched
            .is_some_and(|touched| touched.elapsed() >= self.policy.session_idle);
        let stale = guard.session.as_ref().is_some_and(|session| {
            session.poisoned.load(Ordering::Acquire)
                || session.reply.home_session_id != access.home_session_id
                || idle
        });
        if stale && let Some(session) = guard.session.take() {
            // Home quarantines a session closed while a read holds slots.
            if let Some(ticket) = guard.parked.take() {
                let _ = self
                    .release(&session, &ticket, HixlReleaseOutcome::Completed, true)
                    .await;
            }
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
                    pool.set_unsupported(
                        Instant::now()
                            .checked_add(UNSUPPORTED_RETRY)
                            .map(|until| (access.home_session_id.clone(), until)),
                    );
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
            guard,
            flush: ParkedFlush {
                pool,
                park: self.release_park,
                timeout: self.timeout,
                metrics: self.metrics.clone(),
            },
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
        let lane_count = u32::try_from(self.native.lane_count())
            .map_err(|_| protocol_error("OwnerFs HIXL lane count overflow"))?;
        let mut request = afs_tracing::request_with_current_context(OwnerNegotiateHixlRequest {
            access: Some(access.clone()),
            client_engine_id: info.engine_id.clone(),
            client_engine_generation: info.generation,
            protocol_version: HIXL_PROTOCOL_VERSION,
            transport: transport as i32,
            buffer_pool_count,
            buffer_pool_size_mib,
            hccs_domain_id,
            lane_count,
            slot_checksum_algorithms: SlotChecksum::OFFER
                .iter()
                .map(|algorithm| algorithm.wire() as i32)
                .collect(),
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
            || SlotChecksum::from_reply(reply.slot_checksum_algorithm).is_none()
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

    fn read_lane(
        &self,
        home: &str,
        session: &HomeSession,
    ) -> afs_error::Result<(usize, HixlLane, usize, usize)> {
        let lane_id = self.bind_lane(home);
        let remote = session
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
        Ok((lane_id, remote, slot_bytes, slots))
    }

    /// Fetches one verified window. Runs as an owned task: the caller may stop
    /// waiting, but the task keeps the budget and completes Release or fencing.
    async fn fetch(
        self: Arc<Self>,
        request: FetchRequest,
        budget: Option<BudgetLease>,
    ) -> afs_error::Result<Fetched> {
        let started = Instant::now();
        let result = self.fetch_inner(&request, budget).await;
        if let Some(metrics) = &self.owner_metrics {
            metrics.observe("client", "read_hixl_window", started.elapsed());
        }
        result
    }

    async fn fetch_inner(
        &self,
        request: &FetchRequest,
        budget: Option<BudgetLease>,
    ) -> afs_error::Result<Fetched> {
        let deadline = request.deadline;
        let Some(mut lease) = self
            .acquire(&request.home, &request.client, &request.access, deadline)
            .await?
        else {
            return Ok(Fetched::Fallback);
        };
        let (lane_id, remote, slot_bytes, slots) = self.read_lane(&request.home, &lease.session)?;
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
            prior_release: lease.prior_release(),
        };
        let reply = match self
            .prepare(
                &mut lease,
                &prepare,
                prepare.prior_release.as_ref(),
                deadline,
                |mut client, command| async move { client.prepare_read_hixl(command).await },
            )
            .await
        {
            Ok(reply) => reply,
            Err(error) if is_unsupported(&error) => {
                lease.session.poisoned.store(true, Ordering::Release);
                return Ok(Fetched::Fallback);
            }
            Err(error) => return Err(error),
        };
        let checksum = SlotChecksum::from_reply(lease.session.reply.slot_checksum_algorithm)
            .ok_or_else(|| protocol_error("OwnerFs HIXL slot checksum is not negotiated"))?;
        if let Err(error) = validate_prepare(&prepare, &reply, &remote, slot_bytes, slots, checksum)
        {
            self.reject_prepare(
                &lease.session,
                (!reply.slots.is_empty()).then(|| (&reply).into()),
            )
            .await;
            return Err(error);
        }
        if reply.read == 0 {
            return Ok(Fetched::Window(FetchedWindow {
                offset: request.offset,
                bytes: Bytes::new(),
                eof: true,
            }));
        }
        let eof = reply.eof;
        let ticket = (&reply).into();
        let bytes = self
            .pull_slots(
                lease,
                lane_id,
                reply.engine_id,
                reply.slots,
                ticket,
                checksum,
                deadline,
            )
            .await?;
        Ok(Fetched::Window(FetchedWindow {
            offset: request.offset,
            bytes: match budget {
                Some(lease) => lease.into_bytes(bytes),
                None => bytes.into(),
            },
            eof,
        }))
    }

    /// Releases or parks the Owner ticket after the shared batch pull.
    #[allow(clippy::too_many_arguments)]
    async fn pull_slots(
        &self,
        mut lease: HomeLease,
        lane_id: usize,
        engine: String,
        slots: Vec<OwnerHixlSlot>,
        ticket: ReadTicket,
        checksum: SlotChecksum,
        deadline: Instant,
    ) -> afs_error::Result<Vec<u8>> {
        let windows = slots
            .into_iter()
            .map(|slot| ReadWindow {
                address: slot.address,
                length: slot.length,
                checksum: slot.checksum,
            })
            .collect();
        match read_batch(
            &self.native,
            self.metrics.as_ref(),
            lane_id,
            engine,
            windows,
            checksum,
            deadline,
        )
        .await
        {
            Ok(bytes) => {
                lease.park(ticket);
                Ok(bytes)
            }
            Err(failure) => {
                if !failure.reuse_session {
                    lease.session.poisoned.store(true, Ordering::Release);
                }
                let outcome = match failure.completion {
                    Completion::NotSubmitted => HixlReleaseOutcome::NotSubmitted,
                    Completion::Completed => HixlReleaseOutcome::Completed,
                    Completion::Unknown => HixlReleaseOutcome::Unknown,
                };
                self.release_detached(lease, ticket, outcome, false);
                Err(failure.error)
            }
        }
    }

    async fn prepare<Req: Clone, Reply, SendReply>(
        &self,
        lease: &mut HomeLease,
        request: &Req,
        prior_release: Option<&OwnerReleaseHixlReadRequest>,
        deadline: Instant,
        send: impl Fn(OwnerFilesClient<Channel>, tonic::Request<Req>) -> SendReply,
    ) -> afs_error::Result<Reply>
    where
        SendReply: std::future::Future<Output = Result<tonic::Response<Reply>, tonic::Status>>,
    {
        let started = Instant::now();
        let result = prepare_with_retry(&lease.session.client, request, deadline, send).await;
        self.observe(Operation::Prepare, started.elapsed(), result.is_ok());
        if let Err(error) = &result {
            lease.restore_prior_release(prior_release);
            if fences_session(error) {
                lease.session.poisoned.store(true, Ordering::Release);
            }
        }
        result
    }

    /// Fences a session that returned an invalid reply and releases any
    /// advertised slots without submitting a native read.
    async fn reject_prepare(&self, session: &HomeSession, ticket: Option<ReadTicket>) {
        session.poisoned.store(true, Ordering::Release);
        if let Some(ticket) = ticket {
            let _ = self
                .release(session, &ticket, HixlReleaseOutcome::NotSubmitted, false)
                .await;
        }
    }

    /// Releases in the background while still holding the Home lease, so the
    /// verified window reaches the caller one control round trip earlier and
    /// the next fetch on this Home still observes every slot released.
    fn release_detached(
        &self,
        lease: HomeLease,
        reply: ReadTicket,
        outcome: HixlReleaseOutcome,
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
            if result.is_err() || outcome == HixlReleaseOutcome::Unknown {
                lease.session.poisoned.store(true, Ordering::Release);
            }
            drop(lease);
        };
        tokio::spawn(task);
    }

    async fn release(
        &self,
        session: &HomeSession,
        reply: &ReadTicket,
        outcome: HixlReleaseOutcome,
        content_validated: bool,
    ) -> afs_error::Result<()> {
        let started = Instant::now();
        let result = release_slots(session, reply, outcome, content_validated, self.timeout).await;
        self.observe(Operation::Release, started.elapsed(), result.is_ok());
        result
    }
}

#[derive(Clone, Copy)]
enum ReadOutcome {
    WindowHit,
    Fetch,
    PrefetchHit,
    Fallback,
    /// A READ served from the open's directory-batch slice.
    BatchHit,
    /// A READ waited for a directory batch in flight.
    BatchWait,
    /// A READ started a directory batch.
    BatchFetch,
    /// A batch-eligible READ took its single-file path.
    BatchFallback,
}

impl ReadOutcome {
    fn label(self) -> &'static str {
        match self {
            Self::WindowHit => "window_hit",
            Self::Fetch => "fetch",
            Self::PrefetchHit => "prefetch_hit",
            Self::Fallback => "fallback",
            Self::BatchHit => "batch_hit",
            Self::BatchWait => "batch_wait",
            Self::BatchFetch => "batch_fetch",
            Self::BatchFallback => "batch_fallback",
        }
    }
}

#[derive(Default)]
struct HomeSlot {
    session: Option<Arc<HomeSession>>,
    touched: Option<Instant>,
    /// Completed release of the session's last read, not yet sent to Home.
    parked: Option<ReadTicket>,
}

/// Up to `sessions_per_home` control sessions of one Home.
struct HomePool {
    permits: Arc<tokio::sync::Semaphore>,
    slots: Vec<Arc<Mutex<HomeSlot>>>,
    unsupported_until: StdMutex<Option<(String, Instant)>>,
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

    fn unsupported(&self, generation: &str) -> bool {
        self.unsupported_until
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|(session, until)| session == generation && Instant::now() < *until)
    }

    fn set_unsupported(&self, until: Option<(String, Instant)>) {
        *self
            .unsupported_until
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = until;
    }

    /// No lease holds a slot, no release is parked, and every session left
    /// in a slot is one Home has already closed.
    fn is_idle(&self) -> bool {
        self.slots.iter().all(|slot| {
            slot.try_lock().is_ok_and(|slot| {
                slot.parked.is_none()
                    && (slot.session.is_none()
                        || slot
                            .touched
                            .is_none_or(|touched| touched.elapsed() >= HOME_POOL_IDLE))
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
        .min(MAX_BATCH_WINDOWS)
}

struct HomeSession {
    reply: OwnerNegotiateHixlReply,
    client: OwnerFilesClient<Channel>,
    poisoned: Arc<AtomicBool>,
}

/// Holding the guard proves every earlier window of this session was
/// released, except the one release parked in the slot. Fields drop in
/// order: the slot unlocks before its permit returns.
struct HomeLease {
    session: Arc<HomeSession>,
    guard: OwnedMutexGuard<HomeSlot>,
    flush: ParkedFlush,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl HomeLease {
    /// The parked release, carried by this lease's next prepare.
    fn prior_release(&mut self) -> Option<OwnerReleaseHixlReadRequest> {
        self.guard
            .parked
            .take()
            .map(|ticket| ticket.release(HixlReleaseOutcome::Completed, true))
    }

    /// Parks a carried release again when its prepare got no reply: Home may
    /// or may not have applied it, and a repeat hits the release tombstone.
    fn restore_prior_release(&mut self, release: Option<&OwnerReleaseHixlReadRequest>) {
        if let Some(release) = release {
            self.guard.parked = Some(ReadTicket {
                session_id: release.session_id,
                read_id: release.read_id.clone(),
                engine_generation: release.engine_generation,
            });
        }
    }

    /// Parks a validated read's release for the session's next prepare.
    fn park(&mut self, ticket: ReadTicket) {
        self.guard.parked = Some(ticket);
    }
}

impl Drop for HomeLease {
    /// A release still parked when the lease ends is flushed unless a later
    /// lease of the slot carries it first.
    fn drop(&mut self) {
        let Some(read_id) = self
            .guard
            .parked
            .as_ref()
            .map(|ticket| ticket.read_id.clone())
        else {
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let slot = OwnedMutexGuard::mutex(&self.guard).clone();
        runtime.spawn(self.flush.clone().run(slot, read_id));
    }
}

/// Sends a parked release that no prepare carried within the park interval.
#[derive(Clone)]
struct ParkedFlush {
    pool: Arc<HomePool>,
    park: Duration,
    timeout: Duration,
    metrics: Option<HixlMetrics>,
}

impl ParkedFlush {
    async fn run(self, slot: Arc<Mutex<HomeSlot>>, read_id: String) {
        tokio::time::sleep(self.park).await;
        let Ok(_permit) = self.pool.permits.clone().acquire_owned().await else {
            return;
        };
        // A held slot needs no flush: its holder carries or re-parks the
        // release. The guard drops before the permit.
        let Ok(mut guard) = slot.try_lock_owned() else {
            return;
        };
        if guard
            .parked
            .as_ref()
            .is_none_or(|ticket| ticket.read_id != read_id)
        {
            return;
        }
        let (Some(ticket), Some(session)) = (guard.parked.take(), guard.session.clone()) else {
            return;
        };
        let started = Instant::now();
        let result = release_slots(
            &session,
            &ticket,
            HixlReleaseOutcome::Completed,
            true,
            self.timeout,
        )
        .await;
        observe_optional(
            self.metrics.as_ref(),
            Operation::Release,
            started.elapsed(),
            result.is_ok(),
        );
        if result.is_err() {
            session.poisoned.store(true, Ordering::Release);
        }
    }
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
    bytes: Bytes,
    eof: bool,
}

enum Fetched {
    Window(FetchedWindow),
    /// Home lacks the capability; Auto may try the next backend.
    Fallback,
}

fn unsupported_read() -> afs_error::Error {
    afs_error::Error::coded(
        afs_error::NODE_TRANSFER_UNSUPPORTED,
        "OwnerFs HIXL read is unsupported by Home",
    )
}

fn protocol_error(message: &'static str) -> afs_error::Error {
    afs_error::Error::coded(afs_error::CLIENT_PROTOCOL_VIOLATION, message)
}

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

async fn prepare_with_retry<Req: Clone, Reply, SendReply>(
    client: &OwnerFilesClient<Channel>,
    prepare: &Req,
    deadline: Instant,
    send: impl Fn(OwnerFilesClient<Channel>, tonic::Request<Req>) -> SendReply,
) -> afs_error::Result<Reply>
where
    SendReply: std::future::Future<Output = Result<tonic::Response<Reply>, tonic::Status>>,
{
    let mut last = None;
    for _ in 0..PREPARE_ATTEMPTS {
        let mut command = afs_tracing::request_with_current_context(prepare.clone());
        command.set_timeout(remaining(deadline)?);
        match send(client.clone(), command).await {
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
    lane: &afs_protocol::node_data::HixlLane,
    slot_bytes: usize,
    max_slots: usize,
    checksum: SlotChecksum,
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
        checked_slot_length(
            slot.address,
            slot.slot_generation,
            slot.length,
            slot.checksum.len(),
            slot_bytes,
            checksum,
        )?;
        total = total
            .checked_add(slot.length)
            .ok_or_else(|| protocol_error("OwnerFs HIXL slot length overflow"))?;
    }
    if total != reply.read {
        return Err(protocol_error("OwnerFs HIXL slots do not cover the read"));
    }
    Ok(())
}

/// Identifies one admitted prepare for its release.
struct ReadTicket {
    session_id: u64,
    read_id: String,
    engine_generation: u64,
}

impl ReadTicket {
    fn release(
        &self,
        outcome: HixlReleaseOutcome,
        content_validated: bool,
    ) -> OwnerReleaseHixlReadRequest {
        OwnerReleaseHixlReadRequest {
            session_id: self.session_id,
            read_id: self.read_id.clone(),
            engine_generation: self.engine_generation,
            outcome: outcome as i32,
            content_validated,
        }
    }
}

impl From<&OwnerPrepareReadHixlReply> for ReadTicket {
    fn from(reply: &OwnerPrepareReadHixlReply) -> Self {
        Self {
            session_id: reply.session_id,
            read_id: reply.read_id.clone(),
            engine_generation: reply.engine_generation,
        }
    }
}

impl From<&OwnerPrepareBatchHixlReply> for ReadTicket {
    fn from(reply: &OwnerPrepareBatchHixlReply) -> Self {
        Self {
            session_id: reply.session_id,
            read_id: reply.read_id.clone(),
            engine_generation: reply.engine_generation,
        }
    }
}

async fn release_slots(
    session: &HomeSession,
    reply: &ReadTicket,
    outcome: HixlReleaseOutcome,
    content_validated: bool,
    timeout: Duration,
) -> afs_error::Result<()> {
    let request = reply.release(outcome, content_validated);
    let mut last = None;
    for _ in 0..RELEASE_ATTEMPTS {
        let mut command = afs_tracing::request_with_current_context(request.clone());
        command.set_timeout(timeout);
        let mut client = session.client.clone();
        match client.release_hixl_read(command).await {
            Ok(response) => {
                let response = response.into_inner();
                let pinned = outcome == HixlReleaseOutcome::Unknown;
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

impl BudgetLease {
    /// Charges the backing allocation until its final shared slice drops.
    fn into_bytes(mut self, bytes: Vec<u8>) -> Bytes {
        let charge = u64::try_from(bytes.capacity())
            .unwrap_or(u64::MAX)
            .min(self.bytes);
        self.budget
            .used
            .fetch_sub(self.bytes - charge, Ordering::AcqRel);
        self.bytes = charge;
        Bytes::from_owner(BudgetedBytes {
            bytes,
            _lease: self,
        })
    }
}

impl Drop for BudgetLease {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

struct BudgetedBytes {
    bytes: Vec<u8>,
    _lease: BudgetLease,
}

impl AsRef<[u8]> for BudgetedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
