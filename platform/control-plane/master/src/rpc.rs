//! Async RPC coordination around the synchronous scheduler and Redis repository.
mod cloning;
mod metrics;
mod recovery;
mod snapshots;
use crate::{
    storage::{NodeSession, Session, StoredInstance, StoredNode},
    Master, Node, Placement,
};
use adx_core::{Error, InstanceRecord, InstanceSpec, InstanceState, Result};
use adx_protocol::{
    auth::{tenant, Peers, Principal},
    control as pb, status,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{Mutex, Notify};
use tonic::{
    transport::{ClientTlsConfig, Endpoint},
    Request, Response, Status,
};

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
    scheduler: Master,
    specs: BTreeMap<String, InstanceSpec>,
    instances: BTreeMap<String, StoredInstance>,
    nodes: BTreeMap<String, StoredNode>,
    needs_recovery: bool,
    recovery_cursor: Option<String>,
    live: BTreeMap<String, LiveNode>,
    recovering: BTreeMap<String, tokio::time::Instant>,
    retired_sessions: BTreeSet<(String, String)>,
}
impl State {
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
        let mut node = self.nodes.get(id).ok_or(Error::Conflict)?.clone();
        let session_id = node.session.as_ref().ok_or(Error::Conflict)?.id.clone();
        node.node.available = false;
        node.session.as_mut().unwrap().routable = false;
        self.scheduler.register(node.node.clone())?;
        self.nodes.insert(id.to_owned(), node);
        let saved = match self.session.invalidate_node(id, &session_id).await {
            Ok(saved) => saved,
            Err(error) => {
                self.needs_recovery = true;
                return Err(error);
            }
        };
        self.nodes.insert(id.to_owned(), saved.nodes[id].clone());
        self.scheduler.generation = self.scheduler.generation.max(saved.generation);
        for (instance_id, stored) in saved.instances {
            if stored.assignment.node_id == id && stored.invalidated {
                if self
                    .scheduler
                    .snapshot()
                    .instances()
                    .contains_key(&instance_id)
                {
                    if let Err(error) = self.scheduler.release(&stored.assignment) {
                        self.needs_recovery = true;
                        return Err(error);
                    }
                }
                self.specs.insert(instance_id.clone(), stored.spec.clone());
                self.instances.insert(instance_id, stored);
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
        let round = adx_observability::trace::Trace::child("shard.schedule_round")
            .scope(|| self.scheduler.schedule_round(shard))?;
        let progress = round.yielded || !round.assignments.is_empty();
        for assignment in round.assignments {
            let spec = self
                .specs
                .get(&assignment.instance_id)
                .ok_or(Error::Conflict)?
                .clone();
            let recovery = self
                .instances
                .get(&spec.id)
                .filter(|i| i.invalidated)
                .cloned();
            let saved = if let Some(old) = recovery {
                self.session
                    .reserve_recovery(&old.assignment, assignment.clone(), recovery::now()?)
                    .await
            } else {
                self.session.reserve(spec, assignment.clone()).await
            };
            match saved {
                Ok(record) => {
                    self.instances.insert(record.spec.id.clone(), record);
                }
                Err(Error::Conflict)
                    if self
                        .instances
                        .get(&assignment.instance_id)
                        .is_some_and(|i| i.invalidated) =>
                {
                    self.scheduler.release(&assignment)?;
                    let fresh = self.session.get(&assignment.instance_id).await?;
                    self.instances.insert(assignment.instance_id.clone(), fresh);
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
    state: Mutex<State>,
    changed: Notify,
    peers: Peers,
    node_tls: ClientTlsConfig,
    timeout: Duration,
    heartbeat_timeout: Duration,
}
#[derive(Clone)]
pub struct MasterRpc(Arc<Inner>);
impl MasterRpc {
    /// Session must be acquired once by process startup. Restored nodes start
    /// unavailable and must register again before receiving new assignments.
    pub async fn new(
        session: Session,
        placement: Placement,
        peers: Peers,
        node_tls: ClientTlsConfig,
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
        node_tls: ClientTlsConfig,
        timeout: Duration,
        heartbeat_timeout: Duration,
    ) -> Result<Self> {
        if timeout.is_zero() || heartbeat_timeout.is_zero() {
            return Err(Error::Invalid("RPC timeout must be positive".into()));
        }
        let mut saved = session.snapshot().await?;
        // The pending queues are intentionally memory-only. Release their old
        // source pins after a new Master epoch has fenced all previous writers.
        for snapshot in session.retained_snapshots().await? {
            for reference in snapshot.references {
                if let adx_core::snapshots::Reference::Restore { instance_id } = &reference {
                    if !saved.instances.contains_key(instance_id) {
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
            *node = session
                .register_session(
                    node.node.clone(),
                    node.address.clone(),
                    node.proxy_address.clone(),
                    node.session.clone(),
                )
                .await?;
        }
        let scheduler = Master::restore(&saved, placement)?;
        let specs = saved
            .instances
            .iter()
            .map(|(id, i)| (id.clone(), i.spec.clone()))
            .collect();
        // Monotonic heartbeat timestamps cannot survive a process restart. Give
        // recovered nodes one bounded registration grace, with routes kept closed.
        let now = tokio::time::Instant::now();
        let recovering = saved.nodes.keys().map(|id| (id.clone(), now)).collect();
        Ok(Self(Arc::new(Inner {
            state: Mutex::new(State {
                session,
                scheduler,
                specs,
                instances: saved.instances,
                nodes: saved.nodes,
                needs_recovery: false,
                recovery_cursor: None,
                live: BTreeMap::new(),
                recovering,
                retired_sessions: BTreeSet::new(),
            }),
            changed: Notify::new(),
            peers,
            node_tls,
            timeout,
            heartbeat_timeout,
        })))
    }
    /// Invalidate expired executions and their routes before admitting a returning node.
    pub async fn expire_nodes(&self) -> Result<usize> {
        let mut state = self.0.state.lock().await;
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
        Ok(ids.len())
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
        let endpoint = Endpoint::from_shared(format!("https://{}", r.node_address))
            .ok()?
            .tls_config(self.0.node_tls.clone())
            .ok()?
            .connect_timeout(self.0.timeout)
            .timeout(self.0.timeout);
        let channel = endpoint.connect().await.ok()?;
        let actual = pb::node_service_client::NodeServiceClient::new(channel)
            .get_session(pb::GetNodeSessionRequest {})
            .await
            .ok()?
            .into_inner();
        (actual.node_id == r.node_id && actual.session_id == r.session_id).then_some(predecessor)
    }

    async fn create(&self, spec: InstanceSpec) -> std::result::Result<pb::InstanceResult, Status> {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let mut state = self.0.state.lock().await;
            state.healthy().map_err(status)?;
            if let Some(existing) = state.specs.get(&spec.id) {
                if existing != &spec {
                    return Err(Status::already_exists(
                        "instance ID has a different specification",
                    ));
                }
            } else {
                if let Some(id) = &spec.snapshot_id {
                    state
                        .session
                        .acquire_snapshot(
                            id,
                            &spec.tenant_id,
                            adx_core::snapshots::Reference::Restore {
                                instance_id: spec.id.clone(),
                            },
                        )
                        .await
                        .map_err(status)?;
                }
                if let Err(error) = state.scheduler.submit(spec.clone()) {
                    if let Some(id) = &spec.snapshot_id {
                        state
                            .session
                            .release_snapshot(
                                id,
                                adx_core::snapshots::Reference::Restore {
                                    instance_id: spec.id.clone(),
                                },
                            )
                            .await
                            .map_err(status)?;
                    }
                    return Err(status(error));
                }
                state.specs.insert(spec.id.clone(), spec.clone());
            }
            let drive = state.drive().await;
            if drive.as_ref().is_err() || drive.as_ref().is_ok_and(|progress| *progress) {
                self.0.changed.notify_waiters();
            }
            let progress = drive.map_err(status)?;
            if state.instances.contains_key(&spec.id) {
                // Recheck storage session before using a cached assignment.
                let stored = state.session.get(&spec.id).await.map_err(status)?;
                if let Some(result) = stored.result {
                    if result.state != InstanceState::Running {
                        return Err(Status::failed_precondition(
                            "instance already completed; query its result",
                        ));
                    }
                    return Ok(pb::InstanceResult {
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
                    return Err(Status::unavailable("owner node requires reconciliation"));
                }
                let snapshot = match &spec.snapshot_id {
                    Some(id) => {
                        let snapshot = state.session.get_snapshot(id).await.map_err(status)?;
                        if !snapshot
                            .references
                            .contains(&adx_core::snapshots::Reference::Restore {
                                instance_id: spec.id.clone(),
                            })
                        {
                            return Err(Status::failed_precondition("snapshot reference missing"));
                        }
                        Some(snapshot.try_into().map_err(status)?)
                    }
                    None => None,
                };
                drop(state);
                let endpoint = Endpoint::from_shared(format!("https://{}", node.address))
                    .map_err(|_| Status::invalid_argument("invalid node address"))?
                    .tls_config(self.0.node_tls.clone())
                    .map_err(|_| Status::internal("invalid node TLS configuration"))?
                    .connect_timeout(self.0.timeout)
                    .timeout(self.0.timeout);
                let channel = endpoint
                    .connect()
                    .await
                    .map_err(|_| Status::unavailable("node connection unavailable"))?;
                let request = pb::StartAssignedInstanceRequest {
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
                let result = pb::node_service_client::NodeServiceClient::new(channel)
                    .create_instance(adx_observability::trace::inject(request))
                    .await?
                    .into_inner();
                let record: InstanceRecord = result
                    .record
                    .clone()
                    .ok_or_else(|| Status::unavailable("node result missing"))?
                    .try_into()
                    .map_err(status)?;
                if record.spec != spec
                    || record.assignment != stored.assignment
                    || record.state != InstanceState::Running
                {
                    return Err(Status::failed_precondition(
                        "node returned inconsistent result",
                    ));
                }
                if result.durability == pb::Durability::Published as i32 {
                    let state = self.0.state.lock().await;
                    if state
                        .session
                        .get(&spec.id)
                        .await
                        .map_err(status)?
                        .result
                        .as_ref()
                        != Some(&record)
                    {
                        return Err(Status::unavailable("node result has not been committed"));
                    }
                } else if result.durability != pb::Durability::Journaled as i32 {
                    return Err(Status::unavailable("unknown result durability"));
                }
                return Ok(result);
            }
            drop(state);
            if progress {
                tokio::task::yield_now().await;
            } else {
                changed.await;
            }
        }
    }
    async fn commit(&self, record: InstanceRecord, session_id: String) -> Result<InstanceRecord> {
        let mut state = self.0.state.lock().await;
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
        let accepted = state.session.commit(record).await?;
        if let Some(stored) = state.instances.get_mut(&accepted.spec.id) {
            stored.result = Some(accepted.clone());
            if accepted.state != InstanceState::Paused {
                if let Some(recovery) = &mut stored.recovery {
                    recovery.pending = false;
                }
            }
        }
        let held = state
            .instances
            .get(&accepted.spec.id)
            .is_some_and(|s| s.resources_held());
        if !state.needs_recovery
            && !held
            && state
                .scheduler
                .snapshot()
                .instances()
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
                .instances()
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
        if let Some(stored) = state.instances.get_mut(&accepted.spec.id) {
            stored.result = Some(accepted.clone());
        }
        self.0.changed.notify_waiters();
        Ok(accepted)
    }
}
#[tonic::async_trait]
impl pb::master_service_server::MasterService for MasterRpc {
    async fn inspect_node(
        &self,
        request: Request<pb::InspectNodeRequest>,
    ) -> std::result::Result<Response<pb::InspectNodeResponse>, Status> {
        let trace = adx_observability::trace::Trace::rpc("master.inspect_node", &request);
        trace
            .run_result(async {
                let principal = self.0.peers.authenticate(&request)?;
                let r = request.into_inner();
                if principal != Principal::Node(r.node_id.clone()) {
                    return Err(Status::permission_denied("node identity mismatch"));
                }
                let mut state = self.0.state.lock().await;
                state.healthy().map_err(status)?;
                let live = state.live.get(&r.node_id).ok_or_else(|| {
                    Status::failed_precondition("register node before reconciliation")
                })?;
                if live.session != r.session_id || !live.report.reconciling {
                    return Err(Status::failed_precondition(
                        "node must enter reconciliation first",
                    ));
                }
                let snapshot = state.session.snapshot().await.map_err(status)?;
                let retained_checkpoints = snapshot
                    .instances
                    .values()
                    .filter_map(|i| i.result.as_ref().and_then(|r| r.checkpoint.as_ref()))
                    .filter(|cp| cp.artifact.storage != "local")
                    .cloned()
                    .map(Into::into)
                    .collect();
                let records = snapshot
                    .instances
                    .into_values()
                    .filter(|i| i.assignment.node_id == r.node_id)
                    .map(|i| {
                        i.result
                            .unwrap_or_else(|| InstanceRecord {
                                restart_attempts: 0,
                                restart_pending: false,
                                runtime_id: format!("{}-{}", i.spec.id, i.assignment.generation),
                                spec: i.spec,
                                assignment: i.assignment,
                                state: InstanceState::Pending,
                                revision: 0,
                                resources_held: true,
                                runtime_ip: None,
                                checkpoint: None,
                                last_operation: None,
                            })
                            .try_into()
                    })
                    .collect::<Result<Vec<_>>>()
                    .map_err(status)?;
                let snapshots = state
                    .session
                    .node_snapshots(&r.node_id)
                    .await
                    .map_err(status)?
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>>>()
                    .map_err(status)?;
                state.live.get_mut(&r.node_id).unwrap().inspected = true;
                Ok(Response::new(pb::InspectNodeResponse {
                    records,
                    snapshots,
                    retained_checkpoints,
                    master_epoch: state.session.epoch(),
                }))
            })
            .await
    }

    async fn register_node(
        &self,
        request: Request<pb::RegisterNodeRequest>,
    ) -> std::result::Result<Response<pb::RegisterNodeResponse>, Status> {
        let trace = adx_observability::trace::Trace::rpc("master.register_node", &request);
        trace
            .run_result(async {
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
                    state.healthy().map_err(status)?;
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
                                    master_epoch: state.session.epoch(),
                                }));
                            }
                            inspected = live.inspected
                                && live.last_seen.elapsed() < service.0.heartbeat_timeout;
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
                        .register_session(
                            node.clone(),
                            r.node_address.clone(),
                            r.proxy_address.clone(),
                            Some(NodeSession {
                                id: r.session_id.clone(),
                                sequence: r.heartbeat_sequence,
                                routable: !r.reconciling,
                            }),
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
                    let shard = state.scheduler.register(node).map_err(status)?;
                    if shard != saved.shard_id {
                        state.needs_recovery = true;
                        return Err(Status::internal("node shard mismatch; recovery required"));
                    }
                    state.nodes.insert(saved.node.id.clone(), saved);
                    state.recovering.remove(&r.node_id);
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
                    service.0.changed.notify_waiters();
                    Ok(Response::new(pb::RegisterNodeResponse {
                        shard_id: u32::try_from(shard)
                            .map_err(|_| Status::internal("shard overflow"))?,
                        master_epoch: state.session.epoch(),
                    }))
                })
                .await
                .map_err(|_| Status::internal("registration task failed"))?
            })
            .await
    }
    async fn create_instance(
        &self,
        request: Request<pb::CreateInstanceRequest>,
    ) -> std::result::Result<Response<pb::InstanceResult>, Status> {
        let trace = adx_observability::trace::Trace::rpc("master.create_instance", &request);
        trace
            .run_result(async {
                if self.0.peers.authenticate(&request)? != Principal::ApiServer {
                    return Err(Status::permission_denied("Frontend caller required"));
                }
                let r = request.into_inner();
                let raw = r
                    .spec
                    .ok_or_else(|| Status::invalid_argument("spec required"))?;
                tenant(r.caller.as_ref(), &raw.tenant_id)?;
                let spec = {
                    let state = self.0.state.lock().await;
                    state.healthy().map_err(status)?;
                    cloning::normalize(&state.session, raw)
                        .await
                        .map_err(status)?
                };
                let service = self.clone();
                tokio::spawn(
                    adx_observability::trace::Trace::child("master.create")
                        .run(async move { service.create(spec).await.map(Response::new) }),
                )
                .await
                .map_err(|_| Status::internal("creation task failed"))?
            })
            .await
    }
    async fn get_instance(
        &self,
        request: Request<pb::GetInstanceRequest>,
    ) -> std::result::Result<Response<pb::GetInstanceResponse>, Status> {
        let trace = adx_observability::trace::Trace::rpc("master.get_instance", &request);
        trace
            .run_result(async {
                let principal = self.0.peers.authenticate(&request)?;
                let r = request.into_inner();
                let state = self.0.state.lock().await;
                let stored = state.session.get(&r.instance_id).await.map_err(status)?;
                match principal {
                    Principal::ApiServer => tenant(r.caller.as_ref(), &stored.spec.tenant_id)?,
                    Principal::Node(ref id) if id == &stored.assignment.node_id => (),
                    _ => {
                        return Err(Status::permission_denied(
                            "caller may not read this instance",
                        ))
                    }
                }
                let node = state
                    .nodes
                    .get(&stored.assignment.node_id)
                    .ok_or_else(|| Status::unavailable("owner node missing"))?;
                let record = stored.result.unwrap_or_else(|| InstanceRecord {
                    restart_attempts: 0,
                    restart_pending: false,
                    runtime_id: format!("{}-{}", stored.spec.id, stored.assignment.generation),
                    spec: stored.spec,
                    assignment: stored.assignment,
                    state: InstanceState::Pending,
                    revision: 0,
                    resources_held: true,
                    runtime_ip: None,
                    checkpoint: None,
                    last_operation: None,
                });
                Ok(Response::new(pb::GetInstanceResponse {
                    record: Some(record.try_into().map_err(status)?),
                    node_address: node.address.clone(),
                    node_proxy_address: node.proxy_address.clone(),
                }))
            })
            .await
    }
    async fn commit_instance(
        &self,
        request: Request<pb::CommitInstanceRequest>,
    ) -> std::result::Result<Response<pb::CommitInstanceResponse>, Status> {
        let trace = adx_observability::trace::Trace::rpc("master.commit_instance", &request);
        trace
            .run_result(async {
                let principal = self.0.peers.authenticate(&request)?;
                let r = request.into_inner();
                let record: InstanceRecord = r
                    .record
                    .ok_or_else(|| Status::invalid_argument("record required"))?
                    .try_into()
                    .map_err(status)?;
                if principal != Principal::Node(record.assignment.node_id.clone()) {
                    return Err(Status::permission_denied("only owning node may commit"));
                }
                let service = self.clone();
                tokio::spawn(adx_observability::trace::Trace::child("master.commit").run(
                    async move {
                        let record = service
                            .commit(record, r.node_session_id)
                            .await
                            .map_err(status)?;
                        Ok(Response::new(pb::CommitInstanceResponse {
                            record: Some(record.try_into().map_err(status)?),
                        }))
                    },
                ))
                .await
                .map_err(|_| Status::internal("commit task failed"))?
            })
            .await
    }
}
