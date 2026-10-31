//! Async RPC coordination around the synchronous scheduler and Redis repository.
use adx_transport::rpc::RpcClient;
mod artifacts;
mod autoscaling;
mod checkpoint_cleanup;
mod claims;
mod cloning;
mod collection;
mod environment_control;
mod metrics;
mod node_retirement;
mod observation;
mod recovery;
mod resume;
mod snapshot_preparation;
mod snapshots;
mod volumes;
use crate::{
    storage::{ClaimOutcome, LocalClaim, NodeSession, Session, StoredEnvironment, StoredNode},
    Coordinator, Node, Placement,
};
use adx_core::{EnvironmentRecord, EnvironmentSpec, EnvironmentState, Error, Result};
use adx_protocol::{
    auth::{tenant, Peers, Principal},
    control as pb, status,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, watch, Mutex, Notify};
use tonic::{transport::Endpoint, Request, Response, Status};

struct LiveNode {
    expired: bool,
    session: String,
    sequence: u64,
    last_seen: tokio::time::Instant,
    inspected: bool,
    report: pb::RegisterNodeRequest,
}
struct State {
    session: Session,
    scheduler: Coordinator,
    specs: BTreeMap<String, EnvironmentSpec>,
    environments: BTreeMap<String, StoredEnvironment>,
    failed_environments: BTreeSet<String>,
    nodes: BTreeMap<String, StoredNode>,
    needs_recovery: bool,
    placement: Placement,
    recovery_cursor: Option<String>,
    resume_cursor: Option<String>,
    collection_cursor: Option<String>,
    checkpoint_cleanup_cursor: Option<String>,
    live: BTreeMap<String, LiveNode>,
    recovering: BTreeMap<String, tokio::time::Instant>,
    retired_sessions: BTreeSet<(String, String)>,
    scheduling_deadlines: BTreeMap<String, tokio::time::Instant>,
    // Local ownership CAS batches run outside the state lock. Cancellation must
    // wait until their result has been synchronized or authoritatively fenced.
    claims_in_flight: BTreeMap<String, usize>,
    create_requests: BTreeMap<String, BTreeSet<String>>,
    local_reservation_deadlines: BTreeMap<String, (adx_core::Assignment, tokio::time::Instant)>,
    volume_waiters: BTreeMap<String, BTreeSet<String>>,
}

fn requires_node_reconciliation(environment: &StoredEnvironment) -> bool {
    environment.result.as_ref().is_none_or(|record| {
        // Failed records need a local controller for cleanup and explicit deletion,
        // including expired ownership. Storage fences invalidated results so a
        // returning node cannot republish Running or reserve their resources.
        record.state != EnvironmentState::Deleted || record.resources_held || record.restart_pending
    })
}

impl State {
    fn remember_environment(&mut self, stored: StoredEnvironment) {
        let id = stored.spec.id.clone();
        if stored
            .result
            .as_ref()
            .is_some_and(|r| r.state == EnvironmentState::Failed)
        {
            self.failed_environments.insert(id.clone());
        } else {
            self.failed_environments.remove(&id);
        }
        self.environments.insert(id, stored);
    }
    fn overdue(&self, id: &str, timeout: Duration) -> bool {
        self.live
            .get(id)
            .is_some_and(|live| !live.expired && live.last_seen.elapsed() >= timeout)
            || self
                .recovering
                .get(id)
                .is_some_and(|since| since.elapsed() >= timeout)
    }
    fn healthy(&self) -> Result<()> {
        if self.needs_recovery {
            Err(Error::Unavailable(
                "scheduler requires authoritative recovery".into(),
            ))
        } else {
            Ok(())
        }
    }
    async fn invalidate_node(&mut self, id: &str) -> Result<()> {
        self.invalidate_node_with_departure(id, false).await
    }
    async fn invalidate_node_with_departure(&mut self, id: &str, departed: bool) -> Result<()> {
        let mut node = self.nodes.get(id).ok_or(Error::Conflict)?.clone();
        let session_id = node.session.as_ref().ok_or(Error::Conflict)?.id.clone();
        node.node.available = false;
        node.session.as_mut().ok_or(Error::Conflict)?.routable = false;
        self.scheduler.register(node.node.clone())?;
        self.nodes.insert(id.to_owned(), node);
        let saved = match self
            .session
            .invalidate_node_with_departure(id, &session_id, departed)
            .await
        {
            Ok(saved) => saved,
            Err(error) => {
                self.needs_recovery = true;
                return Err(error);
            }
        };
        self.nodes.insert(id.to_owned(), saved.nodes[id].clone());
        self.scheduler.generation = self.scheduler.generation.max(saved.generation);
        for (environment_id, stored) in saved.environments {
            if stored.assignment.node_id == id && stored.invalidated {
                if self
                    .scheduler
                    .snapshot()
                    .environments()
                    .contains_key(&environment_id)
                {
                    if let Err(error) = self.scheduler.release(&stored.assignment) {
                        self.needs_recovery = true;
                        return Err(error);
                    }
                }
                self.specs
                    .insert(environment_id.clone(), stored.spec.clone());
                self.remember_environment(stored);
            }
        }
        self.recovering.remove(id);
        if let Some(live) = self.live.get_mut(id) {
            live.inspected = false;
            live.expired = true;
        }
        Ok(())
    }
    async fn drive(&mut self) -> Result<bool> {
        self.healthy()?;
        let Some(shard) = self.scheduler.take_ready_shard() else {
            return Ok(false);
        };
        // A round may serve multiple requests; it has no single request parent.
        let round = self.scheduler.schedule_round(shard)?;
        // A Snapshot may have moved to another shard without an assignment.
        // Keep driving ready work instead of waiting for an unrelated event.
        let progress =
            round.yielded || !round.assignments.is_empty() || !self.scheduler.ready.is_empty();
        for assignment in round.assignments {
            let spec = self
                .specs
                .get(&assignment.environment_id)
                .ok_or(Error::Conflict)?
                .clone();
            let recovery_now = recovery::now()?;
            let recovery = self
                .environments
                .get(&spec.id)
                .filter(|i| i.invalidated || i.resume_point(recovery_now).is_some())
                .cloned();
            let saved = if let Some(old) = recovery {
                self.session
                    .reserve_recovery(&old.assignment, assignment.clone(), recovery::now()?)
                    .await
            } else if crate::storage::has_volumes(&spec) {
                self.session
                    .reserve_with_request(
                        spec.clone(),
                        assignment.clone(),
                        self.create_requests
                            .get(&spec.id)
                            .and_then(|ids| ids.first())
                            .map_or("", String::as_str),
                    )
                    .await
            } else {
                Ok(StoredEnvironment {
                    creation_generation: assignment.generation,
                    creation: Some(crate::storage::CreationSummary::new(
                        self.create_requests
                            .get(&spec.id)
                            .and_then(|ids| ids.first())
                            .map_or("", String::as_str),
                    )?),
                    last_interruption: None,
                    volume_execution: None,
                    resume: None,
                    failed_since_unix_seconds: None,
                    recovery: None,
                    invalidated: false,
                    spec: spec.clone(),
                    assignment: assignment.clone(),
                    result: None,
                })
            };
            match saved {
                Ok(record) => {
                    if let Some(source) = record
                        .recovery
                        .as_ref()
                        .filter(|recovery| recovery.source.node_id != record.assignment.node_id)
                        .and_then(|recovery| self.live.get_mut(&recovery.source.node_id))
                    {
                        // A live former owner still has a paused controller.
                        // Require the existing closed-admission handshake to
                        // retire it against the complete global reference catalog.
                        source.inspected = false;
                    }
                    self.remember_environment(record);
                }
                Err(Error::VolumeBusy(uid)) => {
                    let queued = self
                        .scheduler
                        .requests
                        .get(&spec.id)
                        .ok_or(Error::Conflict)?
                        .1
                        .clone();
                    let mut placement = self
                        .scheduler
                        .volume_placement
                        .get(&spec.id)
                        .ok_or(Error::Conflict)?
                        .clone();
                    self.scheduler.release(&assignment)?;
                    self.scheduler.submit(queued)?;
                    placement.waiting_for = Some(uid.clone());
                    self.scheduler.set_volume_placement(&spec.id, placement);
                    self.volume_waiters
                        .entry(uid)
                        .or_default()
                        .insert(spec.id.clone());
                }
                Err(Error::Conflict)
                    if self
                        .environments
                        .get(&assignment.environment_id)
                        .is_some_and(|i| i.invalidated) =>
                {
                    self.scheduler.release(&assignment)?;
                    let fresh = self.session.get(&assignment.environment_id).await?;
                    self.environments
                        .insert(assignment.environment_id.clone(), fresh);
                }
                Err(Error::Conflict) => {
                    // Rebuild the whole precomputed round after a competing owner
                    // or generation wins. Only uncommitted work is requeued.
                    self.needs_recovery = true;
                    self.recover_authoritative_state().await?;
                    return Ok(true);
                }
                Err(error) => {
                    self.needs_recovery = true;
                    return Err(error);
                }
            }
        }
        if let Some(error) = round.error {
            return Err(error);
        }
        Ok(progress)
    }
}
struct Inner {
    business: Arc<std::sync::RwLock<Option<observation::BusinessSnapshot>>>,
    business_fenced: Arc<std::sync::atomic::AtomicBool>,
    volume_domains: std::sync::RwLock<BTreeMap<String, adx_core::volumes::StorageDomain>>,
    created: std::sync::atomic::AtomicU64,
    reclaimers: std::sync::RwLock<adx_checkpoint_storage::reclaim::Reclaimers>,
    resume_admission: Arc<tokio::sync::Semaphore>,
    resume_locks: std::sync::Mutex<BTreeMap<String, std::sync::Weak<Mutex<()>>>>,
    state: Mutex<State>,
    create_locks: std::sync::Mutex<BTreeMap<String, std::sync::Weak<Mutex<()>>>>,
    claim_sender: mpsc::Sender<PendingClaim>,
    changed: Notify,
    closing: watch::Sender<bool>,
    peers: Peers,
    node_tls: RpcClient,
    timeout: Duration,
    heartbeat_timeout: Duration,
}

struct PendingClaim {
    spec: EnvironmentSpec,
    candidate: LocalClaim,
    response: oneshot::Sender<Result<ClaimOutcome>>,
}

const CLAIM_BATCH_CAPACITY: usize = 1_024;
const CLAIM_BATCH_MAX: usize = 256;
const CLAIM_BATCH_WINDOW: Duration = Duration::from_millis(1);

async fn run_claim_batches(session: Session, mut requests: mpsc::Receiver<PendingClaim>) {
    while let Some(first) = requests.recv().await {
        let mut batch = Vec::with_capacity(CLAIM_BATCH_MAX);
        batch.push(first);
        let deadline = tokio::time::Instant::now() + CLAIM_BATCH_WINDOW;
        while batch.len() < CLAIM_BATCH_MAX {
            match requests.try_recv() {
                Ok(request) => batch.push(request),
                Err(mpsc::error::TryRecvError::Disconnected) => break,
                Err(mpsc::error::TryRecvError::Empty) => {
                    if tokio::time::Instant::now() >= deadline {
                        break;
                    }
                    tokio::time::sleep_until(deadline).await;
                }
            }
        }
        let claims = batch
            .iter()
            .map(|request| (request.spec.clone(), request.candidate.clone()))
            .collect();
        match session.claim_batch(claims).await {
            Ok(outcomes) => {
                for (request, outcome) in batch.into_iter().zip(outcomes) {
                    let _ = request.response.send(outcome);
                }
            }
            Err(error) => {
                for request in batch {
                    let _ = request.response.send(Err(error.clone()));
                }
            }
        }
    }
}

impl Inner {
    async fn claim(&self, spec: EnvironmentSpec, candidate: LocalClaim) -> Result<ClaimOutcome> {
        let (response, receiver) = oneshot::channel();
        self.claim_sender
            .send(PendingClaim {
                spec,
                candidate,
                response,
            })
            .await
            .map_err(|_| Error::Unavailable("ownership claim queue closed".into()))?;
        receiver
            .await
            .map_err(|_| Error::Unavailable("ownership claim worker stopped".into()))?
    }
}

fn scheduling_timeout(seconds: u64) -> Option<Duration> {
    let seconds = if seconds == 0 { 30 } else { seconds };
    if seconds > 86_400 {
        return None;
    }
    Some(Duration::from_secs(seconds))
}

fn create_deadline(
    request: &pb::CreateEnvironmentRequest,
) -> std::result::Result<adx_transport::deadline::Deadline, adx_transport::deadline::DeadlineError>
{
    if request.create_deadline_unix_millis != 0 {
        adx_transport::deadline::Deadline::from_unix_millis(request.create_deadline_unix_millis)
    } else {
        adx_transport::deadline::Deadline::after(Duration::from_secs(
            if request.create_timeout_seconds == 0 {
                90
            } else {
                request.create_timeout_seconds
            },
        ))
    }
}

#[derive(Clone)]
pub struct CoordinatorRpc(Arc<Inner>);
impl CoordinatorRpc {
    /// Session must be acquired once by process startup. Restored nodes start
    /// unavailable and must register again before receiving new assignments.
    pub async fn new(
        session: Session,
        placement: Placement,
        peers: Peers,
        node_tls: impl Into<RpcClient>,
        timeout: Duration,
    ) -> Result<Self> {
        Self::with_heartbeat_timeout(
            session,
            placement,
            peers,
            node_tls,
            timeout,
            Duration::from_secs(30),
        )
        .await
    }
    pub async fn with_heartbeat_timeout(
        session: Session,
        placement: Placement,
        peers: Peers,
        node_tls: impl Into<RpcClient>,
        timeout: Duration,
        heartbeat_timeout: Duration,
    ) -> Result<Self> {
        Box::pin(Self::with_heartbeat_timeout_inner(
            session,
            placement,
            peers,
            node_tls.into(),
            timeout,
            heartbeat_timeout,
        ))
        .await
    }

    async fn with_heartbeat_timeout_inner(
        session: Session,
        placement: Placement,
        peers: Peers,
        node_tls: RpcClient,
        timeout: Duration,
        heartbeat_timeout: Duration,
    ) -> Result<Self> {
        if timeout.is_zero() || heartbeat_timeout.is_zero() {
            return Err(Error::Invalid("RPC timeout must be positive".into()));
        }
        let mut saved = session.snapshot().await?;
        session.refresh_inspection_inventory().await?;
        // The pending queues are intentionally memory-only. Release their old
        // source pins after a new Coordinator epoch has fenced all previous writers.
        for snapshot in session.retained_snapshots().await? {
            for reference in snapshot.references {
                if let adx_core::snapshots::Reference::Restore { environment_id } = &reference {
                    if !saved.environments.contains_key(environment_id) {
                        session.release_snapshot(&snapshot.id, reference).await?;
                    }
                }
            }
        }
        // Persist unreachability before exposing a recovered routing snapshot.
        for node in saved.nodes.values_mut() {
            node.node.available = false;
            node.session
                .get_or_insert(NodeSession {
                    id: String::new(),
                    sequence: 0,
                    routable: false,
                })
                .routable = false;
            if node.retirement.as_ref().is_some_and(|r| r.unregistered) {
                continue;
            }
            *node = session
                .register_session_with_scopes(
                    node.node.clone(),
                    node.address.clone(),
                    node.proxy_address.clone(),
                    node.session.clone(),
                    node.checkpoint_scopes.clone(),
                )
                .await?;
        }
        let mut scheduler = Coordinator::restore(&saved, placement)?;
        scheduler.use_generation_source(session.clone());
        let specs = saved
            .environments
            .iter()
            .map(|(id, i)| (id.clone(), i.spec.clone()))
            .collect();
        // Monotonic heartbeat timestamps cannot survive a process restart. Give
        // recovered nodes one bounded registration grace, with routes kept closed.
        let now = tokio::time::Instant::now();
        let recovering = saved.nodes.keys().map(|id| (id.clone(), now)).collect();
        let (claim_sender, claim_requests) = mpsc::channel(CLAIM_BATCH_CAPACITY);
        tokio::spawn(run_claim_batches(session.clone(), claim_requests));
        let volume_changes = session.committed_changes();
        let monitoring = session.clone();
        let business = match monitoring.business_counters().await {
            Ok(counters) => Some(observation::BusinessSnapshot {
                counters,
                refreshed: std::time::Instant::now(),
            }),
            Err(Error::Conflict) => return Err(Error::Conflict),
            Err(error) => {
                adx_observability::warn!(%error, "initial business metrics unavailable; refresh will retry");
                None
            }
        };
        let departed_sessions = session.departed_node_sessions().await?;
        let service = Self(Arc::new(Inner {
            business: Arc::new(std::sync::RwLock::new(business)),
            business_fenced: Default::default(),
            created: Default::default(),
            create_locks: Default::default(),
            reclaimers: Default::default(),
            resume_admission: Arc::new(tokio::sync::Semaphore::new(1024)),
            resume_locks: std::sync::Mutex::new(BTreeMap::new()),
            state: Mutex::new(State {
                session,
                scheduler,
                specs,
                failed_environments: saved
                    .environments
                    .iter()
                    .filter(|(_, i)| {
                        i.result
                            .as_ref()
                            .is_some_and(|r| r.state == EnvironmentState::Failed)
                    })
                    .map(|(id, _)| id.clone())
                    .collect(),
                environments: saved.environments,
                nodes: saved.nodes,
                needs_recovery: false,
                placement,
                recovery_cursor: None,
                resume_cursor: None,
                collection_cursor: None,
                checkpoint_cleanup_cursor: None,
                live: BTreeMap::new(),
                recovering,
                retired_sessions: departed_sessions,
                scheduling_deadlines: BTreeMap::new(),
                claims_in_flight: BTreeMap::new(),
                create_requests: BTreeMap::new(),
                local_reservation_deadlines: BTreeMap::new(),
                volume_waiters: BTreeMap::new(),
            }),
            volume_domains: Default::default(),
            claim_sender,
            changed: Notify::new(),
            closing: watch::channel(false).0,
            peers,
            node_tls,
            timeout,
            heartbeat_timeout,
        }));
        let weak = Arc::downgrade(&service.0.business);
        let fenced = service.0.business_fenced.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let Some(cache) = weak.upgrade() else {
                    break;
                };
                match monitoring.business_counters().await {
                    Ok(counters) => {
                        if let Ok(mut cache) = cache.write() {
                            *cache = Some(observation::BusinessSnapshot {
                                counters,
                                refreshed: std::time::Instant::now(),
                            });
                        }
                    }
                    Err(Error::Conflict) => {
                        fenced.store(true, std::sync::atomic::Ordering::Release);
                    }
                    Err(_) => (),
                }
            }
        });
        service.watch_volume_changes(volume_changes);
        Ok(service)
    }
    /// End node directory streams before the RPC server drains, including
    /// subscribers blocked on a full response channel.
    pub fn close_subscriptions(&self) {
        self.0.closing.send_replace(true);
    }

    pub fn with_reclaimers(self, reclaimers: adx_checkpoint_storage::reclaim::Reclaimers) -> Self {
        *self
            .0
            .reclaimers
            .write()
            .expect("reclaimer registry lock poisoned") = reclaimers;
        self
    }
    /// Invalidate expired executions and their routes before admitting a returning node.
    pub async fn expire_nodes(&self) -> Result<usize> {
        let mut state = self.0.state.lock().await;
        state.recover_authoritative_state().await?;
        state.healthy()?;
        let ids: Vec<_> = state
            .nodes
            .keys()
            .filter(|id| state.overdue(id, self.0.heartbeat_timeout))
            .cloned()
            .collect();
        for id in &ids {
            let result = state.invalidate_node(id).await;
            self.0.changed.notify_waiters();
            result?;
        }
        drop(state);
        self.expire_local_reservations().await?;
        Ok(ids.len())
    }

    async fn expire_local_reservations(&self) -> Result<()> {
        let expired: Vec<_> = self
            .0
            .state
            .lock()
            .await
            .local_reservation_deadlines
            .iter()
            .filter(|(_, (_, deadline))| *deadline <= tokio::time::Instant::now())
            .map(|(id, (assignment, _))| (id.clone(), assignment.clone()))
            .collect();
        for (id, assignment) in expired {
            let _publication = self.create_guard(format!("publication:{id}")).await;
            let mut state = self.0.state.lock().await;
            if state
                .local_reservation_deadlines
                .get(&id)
                .is_none_or(|(current, deadline)| {
                    current != &assignment || *deadline > tokio::time::Instant::now()
                })
            {
                continue;
            }
            let spec = match state.environments.get(&id) {
                Some(record) if record.assignment == assignment && record.result.is_none() => {
                    Some(record.spec.clone())
                }
                None => state.specs.get(&id).cloned(),
                _ => None,
            };
            if spec.is_none() {
                state.local_reservation_deadlines.remove(&id);
                continue;
            }
            if spec.as_ref().is_some_and(crate::storage::has_volumes) {
                state.local_reservation_deadlines.remove(&id);
                continue;
            }
            // A persistent opt-in may still have an unfinished Claim. This CAS
            // cannot erase a result or a replacement assignment.
            if let Err(error) = state.session.abandon_initial(&assignment).await {
                state.needs_recovery = true;
                return Err(error);
            }
            drop(state);
            if let Some(spec) = spec {
                self.discard_unconfirmed_locked(&spec).await;
            }
        }
        Ok(())
    }

    /// A restarted process can retain unexpired assignments only if it is the
    /// authenticated service at the existing address. Probe without holding the
    /// scheduler lock; the caller rechecks the old session and deadline on commit.
    async fn replacement_predecessor(&self, r: &pb::RegisterNodeRequest) -> Option<String> {
        let predecessor = {
            let state = self.0.state.lock().await;
            let live = state.live.get(&r.node_id)?;
            if !r.reconciling
                || live.session == r.session_id
                || live.last_seen.elapsed() >= self.0.heartbeat_timeout
                || live.report.node_address != r.node_address
                || live.report.proxy_address != r.proxy_address
                || state
                    .retired_sessions
                    .contains(&(r.node_id.clone(), r.session_id.clone()))
            {
                return None;
            }
            live.session.clone()
        };
        let endpoint = self
            .0
            .node_tls
            .endpoint(&r.node_address)
            .ok()?
            .connect_timeout(self.0.timeout)
            .timeout(self.0.timeout);
        let channel = endpoint.connect().await.ok()?;
        let actual = pb::node_service_client::NodeServiceClient::new(self.0.node_tls.wrap(channel))
            .get_session(pb::GetNodeSessionRequest {})
            .await
            .ok()?
            .into_inner();
        (actual.node_id == r.node_id && actual.session_id == r.session_id).then_some(predecessor)
    }

    fn create_lock(&self, key: String) -> Arc<Mutex<()>> {
        let mut locks = self
            .0
            .create_locks
            .lock()
            .expect("create lock registry poisoned");
        locks.retain(|_, lock| lock.strong_count() > 0);
        match locks.get(&key).and_then(std::sync::Weak::upgrade) {
            Some(lock) => lock,
            None => {
                let lock = Arc::new(Mutex::new(()));
                locks.insert(key, Arc::downgrade(&lock));
                lock
            }
        }
    }
    async fn create_guard(&self, key: String) -> tokio::sync::OwnedMutexGuard<()> {
        self.create_lock(key).lock_owned().await
    }
    async fn create_from_request(
        &self,
        r: pb::CreateEnvironmentRequest,
    ) -> std::result::Result<Response<pb::EnvironmentResult>, Status> {
        let schedule_timeout = scheduling_timeout(r.schedule_timeout_seconds)
            .ok_or_else(|| Status::invalid_argument("schedule timeout must not exceed 24 hours"))?;
        let deadline = create_deadline(&r)?;
        if r.request_id.len() > 256 {
            return Err(Status::invalid_argument("request ID too long"));
        }
        let request_id = r.request_id.clone();
        let expected_assignment = r
            .expected_assignment
            .clone()
            .map(TryInto::try_into)
            .transpose()
            .map_err(status)?;
        let raw = r
            .spec
            .ok_or_else(|| Status::invalid_argument("spec required"))?;
        tenant(r.caller.as_ref(), &raw.tenant_id)?;
        let session = {
            let mut state = self.0.state.lock().await;
            state.recover_authoritative_state().await.map_err(status)?;
            state.healthy().map_err(status)?;
            state.session.clone()
        };
        if let Some(record) = session
            .published_request(&raw.tenant_id, &request_id)
            .await
            .map_err(status)?
        {
            return Ok(Response::new(pb::EnvironmentResult {
                newly_created: false,
                record: Some(record.try_into().map_err(status)?),
                durability: pb::Durability::Published as i32,
            }));
        }
        let spec = match session.get(&raw.id).await {
            Ok(stored) if stored.spec.tenant_id != raw.tenant_id => {
                return Err(Status::not_found("environment not found"));
            }
            Ok(stored) if stored.result.is_some() => {
                let record = stored
                    .result
                    .ok_or_else(|| Status::internal("confirmed result missing"))?;
                let record = if record.state == EnvironmentState::Running {
                    // Keep the replay transaction off the RPC future's stack.
                    Box::pin(session.publish_existing(record, &request_id))
                        .await
                        .map_err(status)?
                } else {
                    record
                };
                return Ok(Response::new(pb::EnvironmentResult {
                    newly_created: false,
                    record: Some(record.try_into().map_err(status)?),
                    durability: pb::Durability::Published as i32,
                }));
            }
            Ok(_) | Err(Error::NotFound) => {
                cloning::normalize(&session, raw).await.map_err(status)?
            }
            Err(error) => return Err(status(error)),
        };
        let service = self.clone();
        // Keep the large creation future behind a pointer before adding trace wrappers.
        // The task remains detached from cancellation of the RPC response wait.
        let creation = Box::pin(async move {
            service
                .create(
                    spec,
                    schedule_timeout,
                    deadline,
                    expected_assignment,
                    request_id,
                )
                .await
                .map(Response::new)
        });
        tokio::spawn(adx_observability::trace::Trace::child("coordinator.create").run(creation))
            .await
            .map_err(|_| Status::internal("creation task failed"))?
    }

    async fn create(
        &self,
        spec: EnvironmentSpec,
        schedule_timeout: Duration,
        create_deadline: adx_transport::deadline::Deadline,
        _expected_assignment: Option<adx_core::Assignment>,
        request_id: String,
    ) -> std::result::Result<pb::EnvironmentResult, Status> {
        // Lock identities before selecting an execution. No scheduler lock is
        // held while another operation, storage, or a remote node is awaited.
        let request_key = serde_json::to_string(&("request", &spec.tenant_id, &request_id))
            .map_err(|_| Status::invalid_argument("invalid request identity"))?;
        let _request = if request_id.is_empty() {
            None
        } else {
            Some(
                tokio::time::timeout_at(create_deadline.instant(), self.create_guard(request_key))
                    .await
                    .map_err(|_| {
                        Status::deadline_exceeded("create deadline waiting for request")
                    })?,
            )
        };
        let _environment = tokio::time::timeout_at(
            create_deadline.instant(),
            self.create_guard(format!("environment:{}", spec.id)),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("create deadline waiting for environment"))?;
        let outcome = match tokio::time::timeout_at(
            create_deadline.instant() + adx_transport::deadline::CREATE_RESPONSE_GRACE,
            Box::pin(self.create_attempt(
                spec.clone(),
                schedule_timeout,
                create_deadline,
                request_id,
            )),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(_) => Err(Status::deadline_exceeded(
                "create operation deadline exceeded",
            )),
        };
        if outcome.is_err() {
            self.discard_unconfirmed(&spec).await;
        }
        outcome
    }

    async fn discard_unconfirmed(&self, spec: &EnvironmentSpec) {
        let _publication = self.create_guard(format!("publication:{}", spec.id)).await;
        self.discard_unconfirmed_locked(spec).await;
    }

    async fn discard_unconfirmed_locked(&self, spec: &EnvironmentSpec) {
        let mut state = self.0.state.lock().await;
        if state.recover_authoritative_state().await.is_err() {
            return;
        }
        let session = state.session.clone();
        // Read and install under the state lock so node invalidation cannot
        // advance the catalog between the durable read and memory update.
        match session.get(&spec.id).await {
            Ok(stored) => {
                let _ = state.accept_stored_claim(stored);
                return;
            }
            Err(Error::NotFound) => (),
            Err(_) => {
                state.needs_recovery = true;
                return;
            }
        }
        if state.environments.get(&spec.id).is_some_and(|e| {
            e.result
                .as_ref()
                .is_some_and(|r| r.state == EnvironmentState::Running)
        }) {
            return;
        }
        if state.scheduler.retire_claim(&spec.id).is_err() {
            state.needs_recovery = true;
            return;
        }
        if state.scheduler.forget_deleted(&spec.id).is_err() {
            state.needs_recovery = true;
            return;
        }
        state.environments.remove(&spec.id);
        state.specs.remove(&spec.id);
        state.scheduling_deadlines.remove(&spec.id);
        state.create_requests.remove(&spec.id);
        state.local_reservation_deadlines.remove(&spec.id);
        drop(state);
        if let Some(snapshot_id) = &spec.snapshot_id {
            let _ = session
                .release_snapshot(
                    snapshot_id,
                    adx_core::snapshots::Reference::Restore {
                        environment_id: spec.id.clone(),
                    },
                )
                .await;
        }
        self.0.changed.notify_waiters();
    }

    async fn create_attempt(
        &self,
        spec: EnvironmentSpec,
        schedule_timeout: Duration,
        create_deadline: adx_transport::deadline::Deadline,
        request_id: String,
    ) -> std::result::Result<pb::EnvironmentResult, Status> {
        let inspection = self.create_guard(format!("publication:{}", spec.id)).await;
        let session = { self.0.state.lock().await.session.clone() };
        if let Some(record) = session
            .published_request(&spec.tenant_id, &request_id)
            .await
            .map_err(status)?
        {
            return Ok(pb::EnvironmentResult {
                newly_created: false,
                record: Some(record.try_into().map_err(status)?),
                durability: pb::Durability::Published as i32,
            });
        }
        match session.get(&spec.id).await {
            Ok(stored) if stored.spec.tenant_id != spec.tenant_id => {
                return Err(Status::not_found("environment not found"))
            }
            Ok(stored) if stored.result.is_some() => {
                let record = stored
                    .result
                    .ok_or_else(|| Status::internal("confirmed environment result missing"))?;
                let record = if record.state == EnvironmentState::Running {
                    Box::pin(session.publish_existing(record, &request_id))
                        .await
                        .map_err(status)?
                } else {
                    record
                };
                return Ok(pb::EnvironmentResult {
                    newly_created: false,
                    record: Some(record.try_into().map_err(status)?),
                    durability: pb::Durability::Published as i32,
                });
            }
            Err(Error::NotFound) => {
                let mut state = self.0.state.lock().await;
                if state
                    .environments
                    .get(&spec.id)
                    .is_some_and(|e| e.result.is_some())
                {
                    state.scheduler.retire_claim(&spec.id).map_err(status)?;
                    state.scheduler.forget_deleted(&spec.id).map_err(status)?;
                    state.environments.remove(&spec.id);
                    state.specs.remove(&spec.id);
                    state.create_requests.remove(&spec.id);
                }
            }
            Ok(_) => (),
            Err(error) => return Err(status(error)),
        }
        {
            let state = self.0.state.lock().await;
            if state.specs.get(&spec.id).is_some_and(|old| old != &spec) {
                return Err(Status::already_exists(
                    "environment ID has a different pending specification",
                ));
            }
        }
        // Snapshot reference IO is serialized by this Environment's lock rather
        // than the scheduler mutex. The reference keeps the prepared descriptor
        // stable until the create operation completes or is cancelled.
        let prepared_snapshot = if let Some(id) = &spec.snapshot_id {
            session
                .acquire_snapshot(
                    id,
                    &spec.tenant_id,
                    adx_core::snapshots::Reference::Restore {
                        environment_id: spec.id.clone(),
                    },
                )
                .await
                .map_err(status)?;
            Some(
                session
                    .get_snapshot(id)
                    .await
                    .map_err(status)?
                    .try_into()
                    .map_err(status)?,
            )
        } else {
            None
        };
        self.0
            .state
            .lock()
            .await
            .create_requests
            .entry(spec.id.clone())
            .or_default()
            .insert(request_id);
        drop(inspection);
        let mut schedule_deadline = None;
        loop {
            create_deadline.remaining().map_err(|_| {
                Status::deadline_exceeded("create deadline exceeded during central scheduling")
            })?;
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let mut state = self.0.state.lock().await;
            state.recover_authoritative_state().await.map_err(status)?;
            state.healthy().map_err(status)?;
            if state.claims_in_flight.contains_key(&spec.id) {
                drop(state);
                tokio::time::timeout_at(create_deadline.instant(), changed)
                    .await
                    .map_err(|_| {
                        Status::deadline_exceeded("create deadline waiting for optional claim")
                    })?;
                continue;
            }
            if schedule_deadline.is_some_and(|deadline| {
                tokio::time::Instant::now() >= deadline && !state.specs.contains_key(&spec.id)
            }) {
                return Err(Status::deadline_exceeded(
                    "central scheduling queue deadline exceeded; retry the same Environment ID",
                ));
            }
            if let Some(existing) = state.specs.get(&spec.id) {
                if existing != &spec {
                    return Err(Status::already_exists(
                        "environment ID has a different specification",
                    ));
                }
                schedule_deadline.get_or_insert_with(|| {
                    *state
                        .scheduling_deadlines
                        .entry(spec.id.clone())
                        .or_insert_with(|| {
                            (tokio::time::Instant::now() + schedule_timeout)
                                .min(create_deadline.instant())
                        })
                });
            } else {
                let domains = self.volume_domains(&state).map_err(status)?;
                state
                    .prepare_volumes(&spec, &domains)
                    .await
                    .map_err(status)?;
                state.scheduler.submit(spec.clone()).map_err(status)?;
                state.specs.insert(spec.id.clone(), spec.clone());
                let deadline =
                    (tokio::time::Instant::now() + schedule_timeout).min(create_deadline.instant());
                state.scheduling_deadlines.insert(spec.id.clone(), deadline);
                schedule_deadline.get_or_insert(deadline);
            }
            let drive = state.drive().await;
            if drive.as_ref().is_err() || drive.as_ref().is_ok_and(|progress| *progress) {
                self.0.changed.notify_waiters();
            }
            let progress = drive.map_err(status)?;
            if state.environments.contains_key(&spec.id) {
                state.scheduling_deadlines.remove(&spec.id);
                // Only successful publication is durable; this assignment is held in memory.
                let stored = state
                    .environments
                    .get(&spec.id)
                    .ok_or_else(|| Status::not_found("environment reservation disappeared"))?
                    .clone();
                if stored.invalidated || stored.recovery.as_ref().is_some_and(|r| r.pending) {
                    return Err(Status::failed_precondition(
                        "existing environment requires recovery/query",
                    ));
                }
                if let Some(result) = stored.result {
                    if result.state == EnvironmentState::Failed {
                        if let Some(failure) = &result.failure {
                            if failure.reason == adx_core::RuntimeFailureReason::DeadlineExceeded {
                                return Err(status(Error::DeadlineExceeded(
                                    failure.message.clone(),
                                )));
                            }
                        }
                    }
                    if result.state != EnvironmentState::Running {
                        return Err(Status::failed_precondition(
                            "environment already completed; query its result",
                        ));
                    }
                    return Ok(pb::EnvironmentResult {
                        newly_created: false,
                        record: Some(result.try_into().map_err(status)?),
                        durability: pb::Durability::Published as i32,
                    });
                }
                let node = state
                    .nodes
                    .get(&stored.assignment.node_id)
                    .ok_or_else(|| Status::unavailable("owner node unavailable"))?
                    .clone();
                if !node.node.available
                    || state.live.get(&node.node.id).is_none_or(|n| {
                        n.last_seen.elapsed() >= self.0.heartbeat_timeout || !n.inspected
                    })
                {
                    drop(state);
                    self.retry_initial(&spec.id, &stored.assignment).await?;
                    tokio::time::timeout_at(create_deadline.instant(), changed)
                        .await
                        .map_err(|_| {
                            Status::deadline_exceeded(
                                "create deadline exceeded waiting for a ready node",
                            )
                        })?;
                    continue;
                }
                let snapshot = prepared_snapshot.clone();
                drop(state);
                let endpoint = self
                    .0
                    .node_tls
                    .endpoint(&node.address)
                    .map_err(|_| Status::invalid_argument("invalid node address"))?
                    .connect_timeout(self.0.timeout.min(create_deadline.remaining()?))
                    .timeout(adx_transport::deadline::create_response_timeout(
                        create_deadline.remaining()?,
                    ));
                let channel = match endpoint.connect().await {
                    Ok(channel) => channel,
                    Err(_) => {
                        self.retry_initial(&spec.id, &stored.assignment).await?;
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                let request = pb::StartAssignedEnvironmentRequest {
                    create_deadline_unix_millis: create_deadline.unix_millis(),
                    snapshot,
                    node_session_id: node
                        .session
                        .as_ref()
                        .ok_or_else(|| Status::unavailable("node session missing"))?
                        .id
                        .clone(),
                    spec: Some(spec.clone().into()),
                    assignment: Some(stored.assignment.clone().try_into().map_err(status)?),
                };
                let mut request = adx_observability::trace::inject(request);
                request.set_timeout(adx_transport::deadline::create_response_timeout(
                    create_deadline.remaining()?,
                ));
                let result = match pb::node_service_client::NodeServiceClient::new(
                    self.0.node_tls.wrap(channel),
                )
                .create_environment(request)
                .await
                {
                    Ok(result) => result.into_inner(),
                    Err(error)
                        if error.code() == tonic::Code::ResourceExhausted
                            && error
                                .metadata()
                                .get("adx-backend-failure")
                                .is_none_or(|value| value != "true") =>
                    {
                        // A node can hold an as-yet unconfirmed local attempt.
                        // Keep this confirmed owner; retry admission after that
                        // attempt resolves, never allocate another generation.
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                    Err(error)
                        if adx_protocol::deadline_failure(&error).is_none()
                            && error
                                .metadata()
                                .get("adx-backend-failure")
                                .is_none_or(|value| value != "true")
                            && matches!(
                                error.code(),
                                tonic::Code::Unavailable
                                    | tonic::Code::FailedPrecondition
                                    | tonic::Code::Cancelled
                                    | tonic::Code::Unknown
                                    | tonic::Code::DeadlineExceeded
                            ) =>
                    {
                        self.retry_initial(&spec.id, &stored.assignment).await?;
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let record: EnvironmentRecord = result
                    .record
                    .clone()
                    .ok_or_else(|| Status::unavailable("node result missing"))?
                    .try_into()
                    .map_err(status)?;
                if record.spec != spec
                    || record.assignment != stored.assignment
                    || record.state != EnvironmentState::Running
                {
                    return Err(Status::failed_precondition(
                        "node returned inconsistent result",
                    ));
                }
                if result.durability == pb::Durability::Published as i32 {
                    let session = self.0.state.lock().await.session.clone();
                    if session.get(&spec.id).await.map_err(status)?.result.as_ref() != Some(&record)
                    {
                        return Err(Status::unavailable("node result has not been committed"));
                    }
                } else if result.durability != pb::Durability::Journaled as i32 {
                    return Err(Status::unavailable("unknown result durability"));
                }
                return Ok(result);
            }
            drop(state);
            let deadline = schedule_deadline.expect("submitted request has a queue deadline");
            if progress {
                tokio::task::yield_now().await;
            } else {
                if tokio::time::timeout_at(deadline.min(create_deadline.instant()), changed)
                    .await
                    .is_ok()
                {
                    continue;
                }
                if let Some(failure) = self.expire_pending(&spec, deadline).await? {
                    return Err(adx_protocol::scheduling_deadline(failure));
                }
            }
        }
    }

    async fn retry_initial(
        &self,
        id: &str,
        assignment: &adx_core::Assignment,
    ) -> std::result::Result<(), Status> {
        let _publication = self.create_guard(format!("publication:{id}")).await;
        let mut state = self.0.state.lock().await;
        state.recover_authoritative_state().await.map_err(status)?;
        state.healthy().map_err(status)?;
        // A timed-out Redis write may have succeeded before its reply was lost.
        match state.session.get(id).await {
            Ok(stored) if stored.result.is_some() => {
                state.accept_stored_claim(stored).map_err(status)?;
                self.0.changed.notify_waiters();
                return Ok(());
            }
            Ok(stored) => {
                if stored.assignment != *assignment
                    || stored.invalidated
                    || stored.recovery.is_some()
                {
                    state.accept_stored_claim(stored).map_err(status)?;
                    self.0.changed.notify_waiters();
                    return Ok(());
                }
                // Only the optional path has a durable pending owner. Retire its
                // exact opaque record before selecting a replacement execution.
                if let Err(error) = state.session.abandon_initial(assignment).await {
                    state.needs_recovery = true;
                    return Err(status(error));
                }
                match state.session.get(id).await {
                    Ok(current) => {
                        state.accept_stored_claim(current).map_err(status)?;
                        self.0.changed.notify_waiters();
                        return Ok(());
                    }
                    Err(Error::NotFound) => (),
                    Err(error) => {
                        state.needs_recovery = true;
                        return Err(status(error));
                    }
                }
            }
            Err(Error::NotFound) => (),
            Err(error) => return Err(status(error)),
        }
        if state
            .environments
            .get(id)
            .is_some_and(|e| e.assignment == *assignment && e.result.is_none())
        {
            state.scheduler.retry(assignment).map_err(status)?;
            state.environments.remove(id);
            state.local_reservation_deadlines.remove(id);
            self.0.changed.notify_waiters();
        }
        Ok(())
    }
    async fn expire_pending(
        &self,
        spec: &EnvironmentSpec,
        deadline: tokio::time::Instant,
    ) -> std::result::Result<Option<adx_core::scheduling::SchedulingFailure>, Status> {
        let mut state = self.0.state.lock().await;
        state.recover_authoritative_state().await.map_err(status)?;
        state.healthy().map_err(status)?;
        if state.claims_in_flight.contains_key(&spec.id) {
            return Err(Status::deadline_exceeded(
                "central scheduling queue deadline exceeded with ownership claim in flight; query the same Environment ID",
            ));
        }
        if state.environments.contains_key(&spec.id)
            || state.scheduling_deadlines.get(&spec.id) != Some(&deadline)
        {
            return Ok(None);
        }
        // Diagnose and cancel under the same lock as assignment/reservation.
        let failure = state.scheduler.scheduling_failure(spec).map_err(status)?;
        if !state.scheduler.cancel_pending(&spec.id) {
            return Ok(None);
        }
        state.specs.remove(&spec.id);
        state.scheduling_deadlines.remove(&spec.id);
        state.forget_volume_waiter(&spec.id);
        state.session.unpin_volumes(spec).await.map_err(status)?;
        if let Some(snapshot_id) = &spec.snapshot_id {
            state
                .session
                .release_snapshot(
                    snapshot_id,
                    adx_core::snapshots::Reference::Restore {
                        environment_id: spec.id.clone(),
                    },
                )
                .await
                .map_err(status)?;
        }
        self.0.changed.notify_waiters();
        Ok(Some(failure))
    }
    async fn commit(
        &self,
        record: EnvironmentRecord,
        session_id: String,
    ) -> Result<EnvironmentRecord> {
        Box::pin(self.commit_releasing(record, session_id, vec![], &[])).await
    }

    async fn commit_releasing(
        &self,
        record: EnvironmentRecord,
        session_id: String,
        stopped_runtime_ids: Vec<String>,
        cleanup_artifacts: &[adx_core::CheckpointArtifact],
    ) -> Result<EnvironmentRecord> {
        let _publication = self
            .create_guard(format!("publication:{}", record.spec.id))
            .await;
        let mut state = self.0.state.lock().await;
        state.recover_authoritative_state().await?;
        state.healthy()?;
        let id = &record.assignment.node_id;
        if state.overdue(id, self.0.heartbeat_timeout) {
            let result = state.invalidate_node(id).await;
            self.0.changed.notify_waiters();
            result?;
        }
        if state.live.get(id).is_none_or(|live| live.expired) {
            return Err(Error::Conflict);
        }
        if state
            .nodes
            .get(&record.assignment.node_id)
            .and_then(|n| n.session.as_ref())
            .is_none_or(|s| s.id != session_id)
        {
            return Err(Error::Conflict);
        }
        let first_create = record.state == EnvironmentState::Running
            && record.restart_attempts == 0
            && state
                .environments
                .get(&record.spec.id)
                .is_some_and(|stored| {
                    stored.result.is_none() && stored.recovery.is_none() && !stored.invalidated
                });
        let initial = state
            .environments
            .get(&record.spec.id)
            .is_some_and(|e| e.result.is_none() && e.recovery.is_none());
        if initial
            && state
                .environments
                .get(&record.spec.id)
                .is_none_or(|e| e.assignment != record.assignment)
        {
            return Err(Error::Conflict);
        }
        let session = state.session.clone();
        let creation = state
            .environments
            .get(&record.spec.id)
            .and_then(|stored| stored.creation.clone());
        let request_ids = state
            .create_requests
            .get(&record.spec.id)
            .cloned()
            .unwrap_or_default();
        drop(state);
        let mut authoritative = None;
        let mut accepted = if initial && record.state == EnvironmentState::Running {
            let winner = session
                .publish_initial_observed(
                    record.clone(),
                    &request_ids.into_iter().collect::<Vec<_>>(),
                    &session_id,
                    creation,
                )
                .await?;
            if winner.assignment != record.assignment || winner.runtime.id != record.runtime.id {
                session.abandon_initial(&record.assignment).await?;
                self.discard_unconfirmed_locked(&record.spec).await;
                return Err(Error::Conflict);
            }
            winner
        } else if initial && !crate::storage::has_volumes(&record.spec) {
            // Retire an optional pending Claim as well as the memory reservation;
            // unsuccessful initial attempts have no durable creation result.
            session.abandon_initial(&record.assignment).await?;
            let mut state = self.0.state.lock().await;
            if state
                .environments
                .get(&record.spec.id)
                .is_some_and(|e| e.assignment == record.assignment)
            {
                state.scheduler.retire_claim(&record.spec.id)?;
                state.scheduler.forget_deleted(&record.spec.id)?;
                state.environments.remove(&record.spec.id);
                state.specs.remove(&record.spec.id);
                state.create_requests.remove(&record.spec.id);
                state.local_reservation_deadlines.remove(&record.spec.id);
                state.scheduling_deadlines.remove(&record.spec.id);
            }
            self.0.changed.notify_waiters();
            return Ok(record);
        } else {
            let committed = session
                .commit_releasing_with_cleanup_state(
                    record,
                    &session_id,
                    &stopped_runtime_ids,
                    cleanup_artifacts,
                )
                .await?;
            authoritative = committed.stored;
            committed.result
        };
        if initial && accepted.state != EnvironmentState::Deleted {
            authoritative = Some(session.get(&accepted.spec.id).await?);
        }
        let mut state = self.0.state.lock().await;
        if state.needs_recovery
            || state
                .environments
                .get(&accepted.spec.id)
                .is_some_and(|stored| {
                    stored.invalidated
                        || stored
                            .result
                            .as_ref()
                            .is_some_and(|result| result.revision > accepted.revision)
                })
            || (initial
                && state
                    .environments
                    .get(&accepted.spec.id)
                    .is_none_or(|stored| {
                        stored.assignment != accepted.assignment
                            || stored.invalidated
                            || stored.result.is_some()
                    }))
        {
            // Recovery may have rebuilt the catalog while Redis was publishing.
            // Reinstall the authoritative result before acknowledging success.
            state.recover_authoritative_state().await?;
            // A successful deletion has already retired the durable record. Its
            // receipt was validated by commit_with_state; finish cache cleanup.
            if accepted.state != EnvironmentState::Deleted {
                let durable = state.session.get(&accepted.spec.id).await?;
                accepted = durable.result.clone().ok_or(Error::Conflict)?;
                authoritative = Some(durable.clone());
                state.accept_stored_claim(durable)?;
            }
        }
        if state
            .environments
            .get(&accepted.spec.id)
            .is_some_and(|e| e.assignment != accepted.assignment)
        {
            return Err(Error::Conflict);
        }
        if first_create {
            self.0
                .created
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        if let Some(stored) =
            authoritative.filter(|stored| stored.assignment == accepted.assignment)
        {
            state.remember_environment(stored);
        }
        if let Some(stored) = state.environments.get_mut(&accepted.spec.id) {
            stored.spec = accepted.spec.clone();
            stored.result = Some(accepted.clone());
            if accepted.state != EnvironmentState::Failed {
                stored.failed_since_unix_seconds = None;
            }
            if accepted.state != EnvironmentState::Paused {
                if let Some(recovery) = &mut stored.recovery {
                    recovery.pending = false;
                }
            }
        }
        let held = state
            .environments
            .get(&accepted.spec.id)
            .is_some_and(|s| s.resources_held());
        if !state.needs_recovery
            && !held
            && state
                .scheduler
                .snapshot()
                .environments()
                .contains_key(&accepted.spec.id)
        {
            if let Err(error) = state.scheduler.release(&accepted.assignment) {
                state.needs_recovery = true;
                return Err(error);
            }
        }
        if !state.needs_recovery
            && held
            && !state
                .scheduler
                .snapshot()
                .environments()
                .contains_key(&accepted.spec.id)
        {
            if let Err(error) = state
                .scheduler
                .restore_assignment(&accepted.spec, &accepted.assignment)
            {
                state.needs_recovery = true;
                return Err(error);
            }
        }
        if let Some(stored) = state.environments.get_mut(&accepted.spec.id) {
            stored.result = Some(accepted.clone());
        }
        if accepted.state == EnvironmentState::Deleted {
            if let Some(snapshot_id) = &accepted.spec.snapshot_id {
                state
                    .session
                    .release_snapshot(
                        snapshot_id,
                        adx_core::snapshots::Reference::Restore {
                            environment_id: accepted.spec.id.clone(),
                        },
                    )
                    .await?;
            }
            if !state.needs_recovery {
                if let Err(error) = state.scheduler.forget_deleted(&accepted.spec.id) {
                    state.needs_recovery = true;
                    return Err(error);
                }
            }
            state.environments.remove(&accepted.spec.id);
            state.failed_environments.remove(&accepted.spec.id);
            state.specs.remove(&accepted.spec.id);
            state.scheduling_deadlines.remove(&accepted.spec.id);
        }
        if initial || accepted.state == EnvironmentState::Deleted {
            state.local_reservation_deadlines.remove(&accepted.spec.id);
            state.create_requests.remove(&accepted.spec.id);
        }
        self.0.changed.notify_waiters();
        Ok(accepted)
    }
}
#[tonic::async_trait]
impl pb::coordinator_service_server::CoordinatorService for CoordinatorRpc {
    async fn retire_dormant(
        &self,
        request: Request<pb::GetEnvironmentRequest>,
    ) -> std::result::Result<Response<pb::EnvironmentResult>, Status> {
        if self.0.peers.authenticate(&request)? != Principal::ApiServer {
            return Err(Status::permission_denied("API Server required"));
        }
        self.retire_dormant(request.into_inner())
            .await
            .map(Response::new)
    }
    async fn ensure_running(
        &self,
        request: Request<pb::EnsureRunningRequest>,
    ) -> std::result::Result<Response<pb::GetEnvironmentResponse>, Status> {
        if !matches!(
            self.0.peers.authenticate(&request)?,
            Principal::ApiServer | Principal::Ingress
        ) {
            return Err(Status::permission_denied("API Server or Ingress required"));
        }
        let service = self.clone();
        let request = request.into_inner();
        let permit = self
            .0
            .resume_admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("resume wait queue is full"))?;
        tokio::spawn(async move {
            let _permit = permit;
            service.ensure_running(request).await.map(Response::new)
        })
        .await
        .map_err(|_| Status::internal("resume task failed"))?
    }
    async fn get_environment_control(
        &self,
        request: Request<pb::GetEnvironmentControlRequest>,
    ) -> std::result::Result<Response<pb::EnvironmentControlRecord>, Status> {
        self.get_environment_control_rpc(request).await
    }
    async fn batch_get_environment_control(
        &self,
        request: Request<pb::BatchGetEnvironmentControlRequest>,
    ) -> std::result::Result<Response<pb::BatchGetEnvironmentControlResponse>, Status> {
        self.batch_get_environment_control_rpc(request).await
    }
    async fn update_environment_control(
        &self,
        request: Request<pb::UpdateEnvironmentControlRequest>,
    ) -> std::result::Result<Response<pb::EnvironmentControlRecord>, Status> {
        self.update_environment_control_rpc(request).await
    }
    type WatchNodesStream =
        tokio_stream::wrappers::ReceiverStream<std::result::Result<pb::NodeDirectory, Status>>;
    async fn watch_nodes(
        &self,
        request: Request<pb::WatchNodesRequest>,
    ) -> std::result::Result<Response<Self::WatchNodesStream>, Status> {
        if self.0.peers.authenticate(&request)? != Principal::ApiServer {
            return Err(Status::permission_denied("API Server required"));
        }
        let mut closing = self.0.closing.subscribe();
        if *closing.borrow() {
            return Err(Status::unavailable("Coordinator shutting down"));
        }
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let service = self.clone();
        tokio::spawn(async move {
            let forward = async {
                loop {
                    let frame =
                        {
                            let state = service.0.state.lock().await;
                            state.healthy().map_err(status).map(|()| pb::NodeDirectory {
                                epoch: state.session.epoch(),
                                valid_for_millis: 2500,
                                nodes: state
                                    .nodes
                                    .values()
                                    .filter(|n| {
                                        n.session.as_ref().is_some_and(|session| {
                                            state
                                                .live_claimant(
                                                    &n.node.id,
                                                    &session.id,
                                                    service.0.heartbeat_timeout,
                                                )
                                                .is_ok()
                                        })
                                    })
                                    .filter_map(|n| {
                                        let (capacity, allocatable) =
                                            state.scheduler.node_resources(&n.node.id)?;
                                        let (capacity_devices, allocatable_devices) =
                                            state.scheduler.node_devices(&n.node.id)?;
                                        Some(pb::NodeEndpoint {
                                    node_id: n.node.id.clone(),
                                    address: n.address.clone(),
                                    relay_address: n.proxy_address.clone(),
                                    session_id: n
                                        .session
                                        .as_ref()
                                        .expect("node directory filters entries without a session")
                                        .id
                                        .clone(),
                                    capacity: Some(capacity.into()),
                                    allocatable: Some(allocatable.into()),
                                    labels: n.node.labels.clone().into_iter().collect(),
                                    accepting_allocations: n.node.available,
                                    capacity_devices: capacity_devices
                                        .into_iter()
                                        .map(Into::into)
                                        .collect(),
                                    allocatable_devices: allocatable_devices
                                        .into_iter()
                                        .map(Into::into)
                                        .collect(),
                                })
                                    })
                                    .collect(),
                            })
                        };
                    let failed = frame.is_err();
                    if tx.send(frame).await.is_err() || failed {
                        break;
                    }
                    tokio::select! { _ = tx.closed() => break, _ = tokio::time::sleep(Duration::from_secs(1)) => (), _ = service.0.changed.notified() => () }
                }
            };
            tokio::select! {
                _ = closing.wait_for(|value| *value) => {},
                _ = forward => {},
            }
        });
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }
    async fn get_scheduling_queue(
        &self,
        _request: Request<pb::GetSchedulingQueueRequest>,
    ) -> std::result::Result<Response<pb::GetSchedulingQueueResponse>, Status> {
        let state = self.0.state.lock().await;
        state.healthy().map_err(status)?;
        let environments = state
            .scheduler
            .pending_requests()
            .into_iter()
            .take(10_000)
            .map(|request| pb::PendingEnvironment {
                spec: Some(request.spec.into()),
                enqueue_time_millis: request.enqueue_time_millis,
            })
            .collect();
        Ok(Response::new(pb::GetSchedulingQueueResponse {
            environments,
        }))
    }
    async fn set_node_scheduling(
        &self,
        request: Request<pb::SetNodeSchedulingRequest>,
    ) -> std::result::Result<Response<pb::NodeSchedulingState>, Status> {
        let r = request.into_inner();
        if r.node_id.trim().is_empty() {
            return Err(Status::invalid_argument("node_id required"));
        }
        let mut state = self.0.state.lock().await;
        state.recover_authoritative_state().await.map_err(status)?;
        state.healthy().map_err(status)?;
        let locally_accepting = state
            .live
            .get(&r.node_id)
            .filter(|live| !live.expired && live.last_seen.elapsed() < self.0.heartbeat_timeout)
            .is_some_and(|live| live.report.accepting_allocations && !live.report.reconciling);
        let saved = state
            .session
            .set_node_scheduling(
                &r.node_id,
                !r.accepting_allocations,
                r.accepting_allocations && locally_accepting,
            )
            .await
            .map_err(status)?;
        let shard = match state
            .scheduler
            .register_at(saved.node.clone(), saved.shard_id)
        {
            Ok(shard) => shard,
            Err(error) => {
                state.needs_recovery = true;
                self.0.changed.notify_waiters();
                return Err(status(error));
            }
        };
        if shard != saved.shard_id {
            state.needs_recovery = true;
            self.0.changed.notify_waiters();
            return Err(Status::internal("node shard mismatch; recovery required"));
        }
        state.nodes.insert(r.node_id.clone(), saved);
        self.0.changed.notify_waiters();
        Ok(Response::new(pb::NodeSchedulingState {
            node_id: r.node_id,
            accepting_allocations: r.accepting_allocations,
        }))
    }
    async fn prepare_create(
        &self,
        request: Request<pb::LocalEnvironmentCreateRequest>,
    ) -> std::result::Result<Response<pb::PreparedEnvironment>, Status> {
        let Principal::Node(node) = self.0.peers.authenticate(&request)? else {
            return Err(Status::permission_denied("Adxlet required"));
        };
        self.prepare_local(&node, request.into_inner())
            .await
            .map(Response::new)
    }
    async fn reserve_local_environment(
        &self,
        request: Request<pb::ClaimEnvironmentRequest>,
    ) -> std::result::Result<Response<pb::ClaimEnvironmentResponse>, Status> {
        let trace =
            adx_observability::trace::Trace::rpc("coordinator.reserve_local_environment", &request);
        let Principal::Node(node) = self.0.peers.authenticate(&request)? else {
            return Err(Status::permission_denied("Adxlet required"));
        };
        let service = self.clone();
        let request = request.into_inner();
        tokio::spawn(trace.run(Box::pin(async move {
            service
                .claim_local(node, request, false)
                .await
                .map(Response::new)
        })))
        .await
        .map_err(|_| Status::internal("local reservation task failed"))?
    }
    async fn claim_environment(
        &self,
        request: Request<pb::ClaimEnvironmentRequest>,
    ) -> std::result::Result<Response<pb::ClaimEnvironmentResponse>, Status> {
        let trace = adx_observability::trace::Trace::rpc("coordinator.claim_environment", &request);
        let Principal::Node(node) = self.0.peers.authenticate(&request)? else {
            return Err(Status::permission_denied("Adxlet required"));
        };
        let service = self.clone();
        let request = request.into_inner();
        tokio::spawn(trace.run(Box::pin(async move {
            service
                .claim_local(node, request, true)
                .await
                .map(Response::new)
        })))
        .await
        .map_err(|_| Status::internal("claim task failed"))?
    }
    async fn forward_create(
        &self,
        request: Request<pb::LocalEnvironmentCreateRequest>,
    ) -> std::result::Result<Response<pb::EnvironmentResult>, Status> {
        let trace = adx_observability::trace::Trace::rpc("coordinator.forward_create", &request);
        let Principal::Node(node) = self.0.peers.authenticate(&request)? else {
            return Err(Status::permission_denied("Adxlet required"));
        };
        let request = request.into_inner();
        {
            let mut state = self.0.state.lock().await;
            state.recover_authoritative_state().await.map_err(status)?;
            state
                .live_claimant(&node, &request.node_session_id, self.0.heartbeat_timeout)
                .map_err(status)?;
        }
        let create = request
            .create
            .ok_or_else(|| Status::invalid_argument("create required"))?;
        let service = self.clone();
        tokio::spawn(trace.run(Box::pin(async move {
            service.create_from_request(create).await
        })))
        .await
        .map_err(|_| Status::internal("forwarded creation task failed"))?
    }

    async fn inspect_node(
        &self,
        request: Request<pb::InspectNodeRequest>,
    ) -> std::result::Result<Response<pb::InspectNodeResponse>, Status> {
        let trace = request
            .metadata()
            .contains_key("traceparent")
            .then(|| adx_observability::trace::Trace::rpc("coordinator.inspect_node", &request));
        let observation = async {
            let principal = self.0.peers.authenticate(&request)?;
            let r = request.into_inner();
            if principal != Principal::Node(r.node_id.clone()) {
                return Err(Status::permission_denied("node identity mismatch"));
            }
            let mut state = self.0.state.lock().await;
            state.recover_authoritative_state().await.map_err(status)?;
            state.healthy().map_err(status)?;
            let live = state.live.get(&r.node_id).ok_or_else(|| {
                Status::failed_precondition("register node before reconciliation")
            })?;
            if live.session != r.session_id
                || live.expired
                || live.last_seen.elapsed() >= self.0.heartbeat_timeout
            {
                return Err(Status::failed_precondition(
                    "live node session required for inspection",
                ));
            }
            let (cached_snapshots, holds) =
                state.session.inspection_inventory().await.map_err(status)?;
            let mut retained = BTreeMap::new();
            let mut retain = |artifact: &adx_core::CheckpointArtifact| {
                if artifact.storage == "local" {
                    return;
                }
                let key = (artifact.storage.clone(), artifact.location.clone());
                let entry = retained.entry(key).or_insert_with(|| artifact.clone());
                if artifact.size_bytes > entry.size_bytes {
                    *entry = artifact.clone();
                }
            };
            for environment in state.environments.values() {
                if let Some(record) = &environment.result {
                    for point in record
                        .checkpoint
                        .iter()
                        .chain(record.snapshot_source.iter())
                    {
                        retain(&point.artifact);
                    }
                    for artifact in &record.execution_artifacts {
                        retain(artifact);
                    }
                }
            }
            for hold in holds {
                for artifact in hold.artifacts {
                    retain(&artifact);
                }
            }
            let retained_artifacts = retained.into_values().map(Into::into).collect();
            let records = state
                .environments
                .values()
                .filter(|i| i.assignment.node_id == r.node_id && requires_node_reconciliation(i))
                .map(|stored| stored.effective_record().try_into())
                .collect::<Result<Vec<_>>>()
                .map_err(status)?;
            let snapshots = cached_snapshots
                .into_iter()
                .filter(|snapshot| {
                    snapshot.source_node_id == r.node_id
                        && snapshot.state != adx_core::snapshots::SnapshotState::Deleted
                })
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>>>()
                .map_err(status)?;
            state
                .live
                .get_mut(&r.node_id)
                .ok_or_else(|| Status::unavailable("node heartbeat state disappeared"))?
                .inspected = true;
            Ok(Response::new(pb::InspectNodeResponse {
                records,
                snapshots,
                retained_artifacts,
                coordinator_epoch: state.session.epoch(),
            }))
        };
        if let Some(trace) = trace {
            trace.run_result(observation).await
        } else {
            let result = observation.await;
            if result.is_err() {
                let trace = adx_observability::trace::Trace::child("coordinator.inspect_node");
                trace.scope(adx_observability::trace::error);
            }
            result
        }
    }

    async fn unregister_node(
        &self,
        request: Request<pb::UnregisterNodeRequest>,
    ) -> std::result::Result<Response<pb::UnregisterNodeResponse>, Status> {
        // Once accepted, complete the ownership transition even if the
        // departing process disconnects or its bounded wait expires.
        let service = self.clone();
        tokio::spawn(async move { service.unregister_node_request(request).await })
            .await
            .map_err(|_| Status::internal("node departure task failed"))?
    }

    async fn register_node(
        &self,
        request: Request<pb::RegisterNodeRequest>,
    ) -> std::result::Result<Response<pb::RegisterNodeResponse>, Status> {
        // Periodic registration/heartbeats do not create request traces.
        let principal = self.0.peers.authenticate(&request)?;
        let r = request.into_inner();
        if principal != Principal::Node(r.node_id.clone()) {
            return Err(Status::permission_denied(
                "node identity does not match certificate",
            ));
        }
        if r.session_id.is_empty()
            || r.session_id.len() > 128
            || r.heartbeat_sequence == 0
            || (r.reconciling && r.accepting_allocations)
        {
            return Err(Status::invalid_argument(
                "node session, monotonic heartbeat and closed reconciliation admission required",
            ));
        }
        for mount in r.storage_mounts.values() {
            adx_core::volumes::StorageMount::try_from(mount.clone()).map_err(status)?;
        }
        let node = Node::try_from(r.clone()).map_err(status)?;
        for address in [&r.node_address, &r.proxy_address] {
            if address.trim().is_empty() || address.contains(['/', '@', '?', '#']) {
                return Err(Status::invalid_argument(
                    "advertised host:port address required",
                ));
            }
            Endpoint::from_shared(format!("https://{address}"))
                .map_err(|_| Status::invalid_argument("invalid advertised address"))?;
        }
        let service = self.clone();
        tokio::spawn(async move {
            let predecessor = service.replacement_predecessor(&r).await;
            let mut state = service.0.state.lock().await;
            state.recover_authoritative_state().await.map_err(status)?;
            state.healthy().map_err(status)?;
            if state.nodes.get(&r.node_id).is_some_and(|n| {
                n.retirement.as_ref().is_some_and(|r| r.unregistered)
                    && n.session.as_ref().is_some_and(|s| s.id == r.session_id)
            }) {
                return Err(Status::failed_precondition("unregistered node session"));
            }
            if state
                .retired_sessions
                .contains(&(r.node_id.clone(), r.session_id.clone()))
            {
                return Err(Status::failed_precondition("retired node session"));
            }
            if state.overdue(&r.node_id, service.0.heartbeat_timeout) {
                let result = state.invalidate_node(&r.node_id).await;
                service.0.changed.notify_waiters();
                result.map_err(status)?;
            }
            let mut inspected = false;
            if let Some(live) = state.live.get(&r.node_id) {
                if live.session == r.session_id {
                    if r.heartbeat_sequence < live.sequence
                        || (r.heartbeat_sequence == live.sequence && live.report != r)
                    {
                        return Err(Status::failed_precondition("stale heartbeat"));
                    }
                    if r.heartbeat_sequence == live.sequence && live.expired {
                        return Err(Status::failed_precondition(
                            "expired heartbeat; reconciliation required",
                        ));
                    }
                    if r.heartbeat_sequence == live.sequence {
                        return Ok(Response::new(pb::RegisterNodeResponse {
                            shard_id: state.nodes[&r.node_id].shard_id as u32,
                            coordinator_epoch: state.session.epoch(),
                        }));
                    }
                    inspected =
                        live.inspected && live.last_seen.elapsed() < service.0.heartbeat_timeout;
                } else if live.last_seen.elapsed() < service.0.heartbeat_timeout
                    && (predecessor.as_ref() != Some(&live.session)
                        || live.report.node_address != r.node_address
                        || live.report.proxy_address != r.proxy_address)
                {
                    return Err(Status::failed_precondition(
                        "another node process is still registered",
                    ));
                }
            }
            if !r.reconciling && !inspected {
                return Err(Status::failed_precondition("node requires reconciliation"));
            }
            let saved = match state
                .session
                .register_session_with_scopes(
                    node.clone(),
                    r.node_address.clone(),
                    r.proxy_address.clone(),
                    Some(NodeSession {
                        id: r.session_id.clone(),
                        sequence: r.heartbeat_sequence,
                        routable: !r.reconciling,
                    }),
                    r.checkpoint_scopes.iter().cloned().collect(),
                )
                .await
            {
                Ok(saved) => saved,
                Err(error) => {
                    if matches!(error, Error::Unavailable(_) | Error::Conflict) {
                        state.needs_recovery = true;
                        service.0.changed.notify_waiters();
                    }
                    return Err(status(error));
                }
            };
            let shard = state
                .scheduler
                .register_at(saved.node.clone(), saved.shard_id)
                .map_err(status)?;
            if shard != saved.shard_id {
                state.needs_recovery = true;
                return Err(Status::internal("node shard mismatch; recovery required"));
            }
            if !r.reconciling && inspected && saved.node.available {
                state.scheduler.reconsider_node(&r.node_id);
            }
            state.nodes.insert(saved.node.id.clone(), saved);
            state.recovering.remove(&r.node_id);
            if !r.reconciling
                && inspected
                && state
                    .live
                    .get(&r.node_id)
                    .is_some_and(|live| live.report.reconciling)
            {
                state
                    .session
                    .release_execution_holds(&r.node_id, &r.session_id)
                    .await
                    .map_err(status)?;
            }
            let topology_changed = state.live.get(&r.node_id).is_none_or(|old| {
                old.expired
                    || old.session != r.session_id
                    || old.inspected != inspected
                    || old.report.reconciling != r.reconciling
                    || old.report.storage_mounts != r.storage_mounts
            });
            if let Some(old) = state.live.remove(&r.node_id) {
                if old.session != r.session_id {
                    state
                        .retired_sessions
                        .insert((r.node_id.clone(), old.session));
                }
            }
            state.live.insert(
                r.node_id.clone(),
                LiveNode {
                    expired: false,
                    session: r.session_id.clone(),
                    sequence: r.heartbeat_sequence,
                    last_seen: tokio::time::Instant::now(),
                    inspected,
                    report: r,
                },
            );
            if topology_changed {
                let domains = service.volume_domains(&state).map_err(status)?;
                state.refresh_volume_topology(&domains).map_err(status)?;
            }
            service.0.changed.notify_waiters();
            Ok(Response::new(pb::RegisterNodeResponse {
                shard_id: u32::try_from(shard).map_err(|_| Status::internal("shard overflow"))?,
                coordinator_epoch: state.session.epoch(),
            }))
        })
        .await
        .map_err(|_| Status::internal("registration task failed"))?
    }
    async fn create_environment(
        &self,
        request: Request<pb::CreateEnvironmentRequest>,
    ) -> std::result::Result<Response<pb::EnvironmentResult>, Status> {
        let trace =
            adx_observability::trace::Trace::rpc("coordinator.create_environment", &request);
        trace
            .run_result(async {
                if self.0.peers.authenticate(&request)? != Principal::ApiServer {
                    return Err(Status::permission_denied("Frontend caller required"));
                }
                Box::pin(self.create_from_request(request.into_inner())).await
            })
            .await
    }
    async fn get_environment(
        &self,
        request: Request<pb::GetEnvironmentRequest>,
    ) -> std::result::Result<Response<pb::GetEnvironmentResponse>, Status> {
        let trace = adx_observability::trace::Trace::rpc("coordinator.get_environment", &request);
        trace
            .run_result(async {
                let principal = self.0.peers.authenticate(&request)?;
                let r = request.into_inner();
                let (session, cached) = {
                    let state = self.0.state.lock().await;
                    (
                        state.session.clone(),
                        state.environments.get(&r.environment_id).cloned(),
                    )
                };
                let stored = match cached {
                    Some(stored) if stored.result.is_none() => stored,
                    _ => session.get(&r.environment_id).await.map_err(status)?,
                };
                let state = self.0.state.lock().await;
                match principal {
                    Principal::ApiServer => tenant(r.caller.as_ref(), &stored.spec.tenant_id)?,
                    // Trusted Ingress reads policy metadata before applying the
                    // public endpoint's configured authentication mode.
                    Principal::Ingress => (),
                    Principal::Node(ref id) if id == &stored.assignment.node_id => (),
                    _ => {
                        return Err(Status::permission_denied(
                            "caller may not read this environment",
                        ))
                    }
                }
                let node = state
                    .nodes
                    .get(&stored.assignment.node_id)
                    .ok_or_else(|| Status::unavailable("owner node missing"))?;
                let record = stored.effective_record();
                Ok(Response::new(pb::GetEnvironmentResponse {
                    record: Some(record.try_into().map_err(status)?),
                    node_address: node.address.clone(),
                    relay_address: node.proxy_address.clone(),
                }))
            })
            .await
    }
    async fn commit_environment(
        &self,
        request: Request<pb::CommitEnvironmentRequest>,
    ) -> std::result::Result<Response<pb::CommitEnvironmentResponse>, Status> {
        let trace =
            adx_observability::trace::Trace::rpc("coordinator.commit_environment", &request);
        trace
            .run_result(async {
                let principal = self.0.peers.authenticate(&request)?;
                let r = request.into_inner();
                let record: EnvironmentRecord = r
                    .record
                    .ok_or_else(|| Status::invalid_argument("record required"))?
                    .try_into()
                    .map_err(status)?;
                if principal != Principal::Node(record.assignment.node_id.clone()) {
                    return Err(Status::permission_denied("only owning node may commit"));
                }
                let cleanup_artifacts = r
                    .cleanup_artifacts
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<adx_core::CheckpointArtifact>>>()
                    .map_err(status)?;
                let service = self.clone();
                tokio::spawn(
                    adx_observability::trace::Trace::child("coordinator.commit").run(Box::pin(
                        async move {
                            let record = service
                                .commit_releasing(
                                    record,
                                    r.node_session_id,
                                    r.stopped_runtime_ids,
                                    &cleanup_artifacts,
                                )
                                .await
                                .map_err(status)?;
                            Ok(Response::new(pb::CommitEnvironmentResponse {
                                record: Some(record.try_into().map_err(status)?),
                            }))
                        },
                    )),
                )
                .await
                .map_err(|_| Status::internal("commit task failed"))?
            })
            .await
    }
}

#[cfg(test)]
mod timeout_tests {
    use super::*;

    #[test]
    fn deleting_a_queued_recovery_retires_it_before_clearing_the_fence() {
        let spec: EnvironmentSpec = serde_json::from_value(serde_json::json!({
            "id": "queued-recovery", "tenant_id": "tenant", "image": "image",
            "runtime_class": "runsc", "priority": 0,
            "resources": {"cpu_millis": 100, "memory_bytes": 128, "disk_bytes": 0}
        }))
        .unwrap();
        let mut scheduler = Coordinator::new(1, Placement::Pack).unwrap();
        scheduler.submit_recovery(spec.clone()).unwrap();
        scheduler.forget_deleted(&spec.id).unwrap();
        assert_eq!(scheduler.pending(0).unwrap(), 0);
        assert!(scheduler.submit(spec).is_ok());
    }

    #[test]
    fn central_queue_timeout_has_a_bounded_default() {
        assert_eq!(scheduling_timeout(0), Some(Duration::from_secs(30)));
        assert_eq!(scheduling_timeout(7), Some(Duration::from_secs(7)));
        assert_eq!(scheduling_timeout(86_401), None);
    }
}

#[cfg(test)]
mod reconciliation_tests {
    use super::*;

    #[test]
    fn deleted_identity_can_enter_the_center_queue_after_failed_claim_retirement() {
        let spec: EnvironmentSpec = serde_json::from_value(serde_json::json!({
            "id": "named", "tenant_id": "tenant", "image": "image",
            "runtime_class": "runsc", "priority": 0,
            "resources": {"cpu_millis": 100, "memory_bytes": 128, "disk_bytes": 0}
        }))
        .unwrap();
        let mut scheduler = Coordinator::new(1, Placement::Pack).unwrap();
        scheduler.retire_claim(&spec.id).unwrap();
        assert!(matches!(
            scheduler.submit(spec.clone()),
            Err(Error::Conflict)
        ));
        scheduler.forget_deleted(&spec.id).unwrap();
        assert!(scheduler.submit(spec.clone()).is_ok());
        assert!(
            matches!(scheduler.forget_deleted(&spec.id), Err(Error::Conflict)),
            "only an identity without active scheduling work may be forgotten"
        );
    }

    #[test]
    fn failed_without_resources_remains_deletable_after_node_restart() {
        let spec: EnvironmentSpec = serde_json::from_value(serde_json::json!({
            "id": "failed", "tenant_id": "tenant", "image": "image",
            "runtime_class": "runsc", "priority": 0,
            "resources": {"cpu_millis": 100, "memory_bytes": 128, "disk_bytes": 0}
        }))
        .unwrap();
        let assignment = adx_core::Assignment {
            environment_id: spec.id.clone(),
            node_id: "node".into(),
            shard_id: 0,
            generation: 1,
            devices: vec![],
        };
        let mut environment = StoredEnvironment {
            creation_generation: assignment.generation,
            creation: None,
            last_interruption: None,
            volume_execution: None,
            resume: None,
            failed_since_unix_seconds: None,
            recovery: None,
            invalidated: false,
            spec: spec.clone(),
            assignment: assignment.clone(),
            result: Some(EnvironmentRecord {
                observation: None,
                execution_artifacts: vec![],
                snapshot_source: None,
                failure: None,
                spec,
                assignment,
                state: EnvironmentState::Failed,
                revision: 2,
                runtime: adx_core::Runtime {
                    id: "failed-1".into(),
                    ip: None,
                },
                resources_held: false,
                checkpoint: None,
                last_operation: None,
                restart_attempts: 0,
                restart_pending: false,
            }),
        };
        assert!(
            requires_node_reconciliation(&environment),
            "the node needs the Failed record to handle explicit deletion after restart"
        );
        environment.invalidated = true;
        assert!(
            requires_node_reconciliation(&environment),
            "an invalidated Failed record must remain manageable for cleanup and deletion"
        );
    }
}

#[cfg(test)]
#[path = "../tests/common/mod.rs"]
pub(crate) mod cancellation_test_redis;

#[cfg(test)]
mod cancellation_tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires isolated Redis; set ADX_TEST_REDIS_SERVER"]
    async fn expiry_waits_for_local_claim_then_preserves_its_assignment() {
        let mut redis = cancellation_test_redis::Redis::new().await;
        let session = redis.store().await.begin(1).await.unwrap();
        let mut rpc = CoordinatorRpc::new(
            session.clone(),
            Placement::Pack,
            Peers::network(),
            RpcClient::network(Principal::Coordinator),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        // Replace the batch consumer so persistence is released at an exact
        // boundary, without sleeps or a race against the real batch window.
        let (sender, mut claims) = mpsc::channel(1);
        // Volume notifications retain a Weak reference. Rebuild the uniquely
        // owned fixture so replacing the consumer does not require zero Weak refs.
        let mut inner = Arc::try_unwrap(rpc.0).ok().expect("fixture has one owner");
        inner.claim_sender = sender;
        rpc = CoordinatorRpc(Arc::new(inner));
        rpc.watch_volume_changes(session.committed_changes());
        let spec: EnvironmentSpec = serde_json::from_value(serde_json::json!({
            "id":"claim-race", "tenant_id":"tenant", "image":"image",
            "runtime_class":"runsc", "priority":0,
            "resources":{"cpu_millis":100,"memory_bytes":128,"disk_bytes":0}
        }))
        .unwrap();
        let node = Node {
            id: "node".into(),
            capacity: spec.resources,
            available: true,
            labels: Default::default(),
            devices: vec![],
            runtime_classes: vec!["runsc".into()],
        };
        let stored = session
            .register_session(
                node.clone(),
                "127.0.0.1:1".into(),
                "127.0.0.1:2".into(),
                Some(NodeSession {
                    id: "boot".into(),
                    sequence: 1,
                    routable: true,
                }),
            )
            .await
            .unwrap();
        {
            let mut state = rpc.0.state.lock().await;
            state.scheduler.register(node).unwrap();
            state.nodes.insert("node".into(), stored);
            state.live.insert(
                "node".into(),
                LiveNode {
                    expired: false,
                    session: "boot".into(),
                    sequence: 1,
                    last_seen: tokio::time::Instant::now(),
                    inspected: true,
                    report: pb::RegisterNodeRequest::default(),
                },
            );
        }
        let claiming = tokio::spawn({
            let service = rpc.clone();
            let spec = spec.clone();
            async move {
                service
                    .claim_local(
                        "node".into(),
                        pb::ClaimEnvironmentRequest {
                            create_deadline_unix_millis: 0,
                            request_id: String::new(),
                            spec: Some(spec.into()),
                            node_session_id: "boot".into(),
                            devices: vec![],
                            caller: Some(pb::CallerContext {
                                tenant_id: "tenant".into(),
                                administrator: false,
                            }),
                        },
                        true,
                    )
                    .await
            }
        });
        let pending = claims.recv().await.unwrap();
        let deadline = tokio::time::Instant::now();
        {
            let mut state = rpc.0.state.lock().await;
            assert_eq!(state.claims_in_flight[&spec.id], 1);
            state.scheduler.submit(spec.clone()).unwrap();
            state.specs.insert(spec.id.clone(), spec.clone());
            state.scheduling_deadlines.insert(spec.id.clone(), deadline);
        }
        let uncertain = rpc.expire_pending(&spec, deadline).await.unwrap_err();
        assert_eq!(uncertain.code(), tonic::Code::DeadlineExceeded);
        assert!(adx_protocol::scheduling_failure(&uncertain).is_none());
        assert_eq!(rpc.0.state.lock().await.scheduler.pending(0).unwrap(), 1);
        let outcome = session
            .claim(pending.spec, &pending.candidate)
            .await
            .unwrap();
        pending.response.send(Ok(outcome)).unwrap();
        claiming.await.unwrap().unwrap();
        assert!(rpc.0.state.lock().await.claims_in_flight.is_empty());
        assert!(rpc.expire_pending(&spec, deadline).await.unwrap().is_none());
        assert_eq!(
            session.get(&spec.id).await.unwrap().assignment.node_id,
            "node"
        );

        let mut cancelled = spec.clone();
        cancelled.id = "cancelled".into();
        {
            let mut state = rpc.0.state.lock().await;
            state.scheduler.submit(cancelled.clone()).unwrap();
            state.specs.insert(cancelled.id.clone(), cancelled.clone());
            state
                .scheduling_deadlines
                .insert(cancelled.id.clone(), deadline);
        }
        let failure = rpc
            .expire_pending(&cancelled, deadline)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failure.rejections["insufficient_cpu"], 1);
        assert_eq!(
            session.get(&cancelled.id).await.unwrap_err(),
            Error::NotFound
        );
        assert_eq!(rpc.0.state.lock().await.scheduler.pending(0).unwrap(), 0);
        redis.crash();
    }
}

#[cfg(test)]
mod publication_tests;
