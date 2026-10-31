//! One shared publication view, refreshed from committed storage only.
use crate::storage::{
    ControlChange, PublicationReset, PublicationValue, Session, StoredEnvironment, StoredNode,
};
use adx_core::{Error, Result};
use adx_protocol::{
    auth::{Peers, Principal},
    control as pb,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{broadcast, mpsc, watch, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
fn security(value: adx_core::execution::DataPlaneSecurityMode) -> i32 {
    match value.effective() {
        // The current deployment default is tls-token. Resolve inheritance at
        // publication so every Ingress sees one authoritative per-environment mode.
        adx_core::execution::DataPlaneSecurityMode::Inherit => {
            pb::DataPlaneSecurityMode::DataPlaneSecurityTlsToken as i32
        }
        adx_core::execution::DataPlaneSecurityMode::Tls => {
            pb::DataPlaneSecurityMode::DataPlaneSecurityTls as i32
        }
        adx_core::execution::DataPlaneSecurityMode::TlsToken => {
            pb::DataPlaneSecurityMode::DataPlaneSecurityTlsToken as i32
        }
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum PublicationState {
    CatchingUp,
    Ready,
}
struct View {
    revision: Option<u64>,
    state: PublicationState,
    nodes: BTreeMap<String, Arc<StoredNode>>,
    sources: BTreeMap<String, Arc<StoredEnvironment>>,
    routes: BTreeMap<String, pb::PublishedRoute>,
    environments: BTreeMap<String, pb::PublishedEnvironment>,
}
#[derive(Clone)]
pub struct RoutePublisher {
    session: Session,
    peers: Peers,
    view: Arc<Mutex<View>>,
    refresh_lock: Arc<Mutex<()>>,
    committed: Arc<Mutex<Option<broadcast::Receiver<ControlChange>>>>,
    changes: broadcast::Sender<pb::RouteFrame>,
    environment_changes: broadcast::Sender<pb::EnvironmentDirectoryFrame>,
    closing: watch::Sender<bool>,
}
impl RoutePublisher {
    pub fn new(session: Session, peers: Peers) -> Self {
        let (changes, _) = broadcast::channel(64);
        let (environment_changes, _) = broadcast::channel(64);
        let committed = session.committed_changes();
        Self {
            session,
            peers,
            view: Arc::new(Mutex::new(View {
                revision: None,
                state: PublicationState::CatchingUp,
                nodes: BTreeMap::new(),
                sources: BTreeMap::new(),
                routes: BTreeMap::new(),
                environments: BTreeMap::new(),
            })),
            committed: Arc::new(Mutex::new(Some(committed))),
            refresh_lock: Arc::new(Mutex::new(())),
            changes,
            environment_changes,
            closing: watch::channel(false).0,
        }
    }
    /// End long-lived directory streams before the RPC server drains. Otherwise
    /// idle subscriptions can keep an old Coordinator alive until forced kill.
    pub fn close_subscriptions(&self) {
        self.closing.send_replace(true);
    }
    pub async fn refresh(&self) -> Result<()> {
        let _refresh = self.refresh_lock.lock().await;
        if self.view.lock().await.revision.is_none() {
            self.session.publication_reset(PublicationReset::Startup, 0);
        }
        let result = self.refresh_inner().await;
        if result.is_err() {
            self.view.lock().await.state = PublicationState::CatchingUp;
        }
        result
    }
    /// Rebuild an unavailable publication view from committed storage.
    ///
    /// A healthy view advances through `ControlChange` events. Polling the
    /// Redis revision while healthy can race with event delivery and turn an
    /// ordinary commit into a full control-hash scan.
    pub async fn recover(&self) -> Result<()> {
        if self.view.lock().await.state == PublicationState::Ready
            && self.session.publication_ready()
        {
            return Ok(());
        }
        self.refresh().await
    }
    async fn refresh_inner(&self) -> Result<()> {
        self.session.repair_publication().await?;
        let (snapshot, receiver) = self.session.publication_snapshot()?;
        let mut next = BTreeMap::new();
        for (id, environment) in &snapshot.environments {
            let node = snapshot
                .nodes
                .get(&environment.assignment.node_id)
                .ok_or(Error::Conflict)?;
            if let Some(route) = published_route(environment, node)? {
                next.insert(id.clone(), route);
            }
        }
        let mut next_environments = BTreeMap::new();
        for (id, environment) in &snapshot.environments {
            if environment.effective_record().state == adx_core::EnvironmentState::Deleted {
                continue;
            }
            let record = environment.effective_record();
            let node = snapshot
                .nodes
                .get(&environment.assignment.node_id)
                .ok_or(Error::Conflict)?;
            next_environments.insert(
                id.clone(),
                pb::PublishedEnvironment {
                    compatibility: route_compatibility(environment),
                    record: Some(record.try_into()?),
                    node_address: node.address.clone(),
                    relay_address: node.proxy_address.clone(),
                },
            );
        }
        let mut view = self.view.lock().await;
        let frame = pb::RouteFrame {
            epoch: self.session.epoch(),
            revision: snapshot.revision,
            base_revision: view.revision.unwrap_or(0),
            reset: true,
            upserts: next.values().cloned().collect(),
            deleted: vec![],
        };
        let environment_frame = pb::EnvironmentDirectoryFrame {
            epoch: self.session.epoch(),
            revision: snapshot.revision,
            base_revision: view.revision.unwrap_or(0),
            reset: true,
            upserts: next_environments.values().cloned().collect(),
            deleted: vec![],
        };
        view.routes = next;
        view.environments = next_environments;
        view.nodes = snapshot.nodes;
        view.sources = snapshot.environments;
        view.revision = Some(snapshot.revision);
        view.state = PublicationState::Ready;
        self.session.publication_applied(snapshot.revision);
        *self.committed.lock().await = Some(receiver);
        let _ = self.changes.send(frame);
        let _ = self.environment_changes.send(environment_frame);
        Ok(())
    }

    async fn refresh_incremental(&self, changes: Vec<ControlChange>) -> Result<()> {
        let _refresh = self.refresh_lock.lock().await;
        let base_revision = self.view.lock().await.revision.ok_or(Error::Conflict)?;
        let changes: Vec<_> = changes
            .into_iter()
            .filter(|change| change.revision > base_revision)
            .collect();
        if changes.is_empty() {
            return Ok(());
        }
        if changes
            .iter()
            .scan(base_revision, |previous, change| {
                let contiguous = change.revision == previous.saturating_add(1);
                *previous = change.revision;
                Some(contiguous)
            })
            .any(|contiguous| !contiguous)
            || changes
                .iter()
                .any(|change| change.dirty || change.epoch != self.session.epoch())
        {
            return Err(Error::Conflict);
        }
        let revision = changes.last().expect("nonempty changes").revision;
        let mut nodes = BTreeMap::new();
        let mut environments = BTreeMap::new();
        for value in changes.into_iter().flat_map(|change| change.values) {
            match value {
                PublicationValue::Node(id, node) => {
                    nodes.insert(id, node);
                }
                PublicationValue::Environment(id, environment) => {
                    environments.insert(id, environment);
                }
            }
        }
        let mut view = self.view.lock().await;
        if view.state != PublicationState::Ready || view.revision != Some(base_revision) {
            return Err(Error::Conflict);
        }
        let mut route_upserts = BTreeMap::new();
        let mut route_deleted = BTreeSet::new();
        let mut environment_upserts = BTreeMap::new();
        let mut environment_deleted = BTreeSet::new();

        for (id, node) in nodes {
            let Some(node) = node else {
                view.nodes.remove(&id);
                let affected: Vec<_> = view
                    .environments
                    .iter()
                    .filter(|(_, e)| {
                        e.record
                            .as_ref()
                            .and_then(|r| r.assignment.as_ref())
                            .is_some_and(|a| a.node_id == id)
                    })
                    .map(|(id, _)| id.clone())
                    .collect();
                for id in affected {
                    view.environments.remove(&id);
                    view.sources.remove(&id);
                    environment_deleted.insert(id.clone());
                    apply_route(&id, None, &mut view, &mut route_upserts, &mut route_deleted);
                }
                continue;
            };
            let changed = view.nodes.get(&id).is_none_or(|previous| {
                previous.address != node.address
                    || previous.proxy_address != node.proxy_address
                    || previous.session.as_ref().map(|session| session.routable)
                        != node.session.as_ref().map(|session| session.routable)
            });
            view.nodes.insert(id.clone(), node.clone());
            if !changed {
                continue;
            }
            let affected: Vec<_> = view
                .sources
                .iter()
                .filter(|(_, environment)| environment.assignment.node_id == id)
                .map(|(id, environment)| (id.clone(), environment.clone()))
                .collect();
            for (environment_id, environment) in affected {
                if environment.effective_record().state != adx_core::EnvironmentState::Deleted {
                    let published = published_environment(&environment, &node)?;
                    if view.environments.get(&environment_id) != Some(&published) {
                        view.environments
                            .insert(environment_id.clone(), published.clone());
                        environment_upserts.insert(environment_id.clone(), published);
                    }
                }
                apply_route(
                    &environment_id,
                    published_route(&environment, &node)?,
                    &mut view,
                    &mut route_upserts,
                    &mut route_deleted,
                );
            }
        }

        for (id, environment) in environments {
            let Some(environment) = environment else {
                view.sources.remove(&id);
                environment_upserts.remove(&id);
                if view.environments.remove(&id).is_some() {
                    environment_deleted.insert(id.clone());
                }
                apply_route(&id, None, &mut view, &mut route_upserts, &mut route_deleted);
                continue;
            };
            view.sources.insert(id.clone(), environment.clone());
            let node = view
                .nodes
                .get(&environment.assignment.node_id)
                .cloned()
                .ok_or(Error::NotFound)?;
            if environment.effective_record().state == adx_core::EnvironmentState::Deleted {
                environment_upserts.remove(&id);
                if view.environments.remove(&id).is_some() {
                    environment_deleted.insert(id.clone());
                }
            } else {
                let published = published_environment(&environment, &node)?;
                if view.environments.get(&id) != Some(&published) {
                    view.environments.insert(id.clone(), published.clone());
                    environment_upserts.insert(id.clone(), published);
                }
            }
            apply_route(
                &id,
                published_route(&environment, &node)?,
                &mut view,
                &mut route_upserts,
                &mut route_deleted,
            );
        }

        let frame = pb::RouteFrame {
            epoch: self.session.epoch(),
            revision,
            base_revision,
            reset: false,
            upserts: route_upserts.into_values().collect(),
            deleted: route_deleted.into_iter().collect(),
        };
        let environment_frame = pb::EnvironmentDirectoryFrame {
            epoch: self.session.epoch(),
            revision,
            base_revision,
            reset: false,
            upserts: environment_upserts.into_values().collect(),
            deleted: environment_deleted.into_iter().collect(),
        };
        view.revision = Some(revision);
        view.state = PublicationState::Ready;
        self.session.publication_applied(revision);
        let _ = self.changes.send(frame);
        let _ = self.environment_changes.send(environment_frame);
        Ok(())
    }

    pub async fn run(self, interval: Duration) {
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut committed = self
            .committed
            .lock()
            .await
            .take()
            .expect("route publisher may only run once");
        let mut warned = false;
        loop {
            // A failed reset stays in CatchingUp. Never let an ordinary batch
            // turn the old view Ready while the missing history is unresolved.
            if self.view.lock().await.state == PublicationState::CatchingUp {
                match self.refresh().await {
                    Ok(()) => {
                        committed = self
                            .committed
                            .lock()
                            .await
                            .take()
                            .expect("reset installs cursor");
                        warned = false;
                    }
                    Err(_) => {
                        if !warned {
                            tracing_unavailable();
                            warned = true;
                        }
                        tokio::time::sleep(interval).await;
                    }
                }
                continue;
            }
            let mut reset = false;
            let mut reason = PublicationReset::Discontinuity;
            let mut skipped = 0;
            let mut batch = Vec::new();
            tokio::select! {
                changed = committed.recv() => match changed {
                    Ok(change) => batch.push(change),
                    Err(broadcast::error::RecvError::Lagged(count)) => { reset = true; reason = PublicationReset::ReceiveLag; skipped = count; },
                    Err(broadcast::error::RecvError::Closed) => return,
                },
                _ = tick.tick() => {
                    reset = !self.session.publication_ready();
                    reason = PublicationReset::AmbiguousWrite;
                },
            }
            // Drain promptly and yield after a bounded amount of work. Sleeping
            // at the oldest retained cursor can overflow the queue again before
            // the first recovered batch is consumed.
            if !reset {
                if let Err((lost_reason, count)) = drain_changes(&mut committed, &mut batch) {
                    reset = true;
                    reason = lost_reason;
                    skipped = count;
                }
            }
            if !reset && !batch.is_empty() {
                reset = self.refresh_incremental(batch).await.is_err();
            }
            if reset {
                self.session.publication_reset(reason, skipped);
                // Both recv and drain loss explicitly enter CatchingUp. Readiness
                // of the previous view cannot suppress a known missing history.
                self.view.lock().await.state = PublicationState::CatchingUp;
                match self.refresh().await {
                    Ok(()) => {
                        committed = self
                            .committed
                            .lock()
                            .await
                            .take()
                            .expect("reset installs cursor");
                    }
                    Err(_) => {
                        if !warned {
                            tracing_unavailable();
                            warned = true;
                        }
                        tokio::time::sleep(interval).await;
                    }
                }
            }
            tokio::task::yield_now().await;
        }
    }
}

fn drain_changes(
    receiver: &mut broadcast::Receiver<ControlChange>,
    batch: &mut Vec<ControlChange>,
) -> std::result::Result<(), (PublicationReset, u64)> {
    let started = std::time::Instant::now();
    while batch.len() < 256 && started.elapsed() < Duration::from_millis(2) {
        match receiver.try_recv() {
            Ok(change) => batch.push(change),
            Err(broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed) => {
                break
            }
            Err(broadcast::error::TryRecvError::Lagged(count)) => {
                return Err((PublicationReset::BatchLag, count))
            }
        }
    }
    Ok(())
}

fn published_environment(
    environment: &StoredEnvironment,
    node: &StoredNode,
) -> Result<pb::PublishedEnvironment> {
    Ok(pb::PublishedEnvironment {
        compatibility: route_compatibility(environment),
        record: Some(environment.effective_record().try_into()?),
        node_address: node.address.clone(),
        relay_address: node.proxy_address.clone(),
    })
}

fn route_compatibility(environment: &StoredEnvironment) -> Option<pb::SandboxRouteCompatibility> {
    use sha2::{Digest, Sha256};
    let envd_gateway_auth = environment
        .spec
        .envd
        .as_ref()
        .is_some_and(|service| service.gateway_auth);
    environment
        .spec
        .compatibility
        .as_ref()
        .map(|compatibility| pb::SandboxRouteCompatibility {
            envd_token_sha256: format!(
                "{:x}",
                Sha256::digest(compatibility.envd_access_token.as_bytes())
            ),
            traffic_token_sha256: format!(
                "{:x}",
                Sha256::digest(compatibility.traffic_access_token.as_bytes())
            ),
            allow_public_traffic: compatibility.allow_public_traffic,
            envd_gateway_auth,
            envd_signing_key: if envd_gateway_auth {
                compatibility.envd_access_token.clone()
            } else {
                String::new()
            },
            envd_ready: environment.result.as_ref().is_some_and(|record| {
                record.state == adx_core::EnvironmentState::Running
                    && record.spec.required_services().any(|name| name == "envd")
            }),
        })
}

fn published_route(
    environment: &StoredEnvironment,
    node: &StoredNode,
) -> Result<Option<pb::PublishedRoute>> {
    let Some(record) = environment.result.as_ref().filter(|record| {
        record.state == adx_core::EnvironmentState::Running
            && environment
                .volume_execution
                .as_ref()
                .is_none_or(|execution| {
                    !execution.released && execution.runtime_id == record.runtime.id
                })
            && node.session.as_ref().is_none_or(|session| session.routable)
    }) else {
        return Ok(None);
    };
    Ok(Some(pb::PublishedRoute {
        compatibility: route_compatibility(environment),
        environment_id: record.spec.id.clone(),
        tenant_id: record.spec.tenant_id.clone(),
        runtime_id: record.runtime.id.clone(),
        runtime_ip: record.runtime.ip.ok_or(Error::Conflict)?.to_string(),
        relay_address: node.proxy_address.clone(),
        generation: record.assignment.generation,
        environment_revision: record.revision,
        tunnel_security_mode: security(record.spec.execution.data_plane.tunnel),
        port_forward_security_mode: security(record.spec.execution.data_plane.port_forward),
        private_service_ports: record
            .spec
            .execution
            .private_service_ports
            .iter()
            .copied()
            .map(u32::from)
            .collect(),
        forwarded_ports: record
            .spec
            .execution
            .ports
            .iter()
            .map(|port| u32::from(*port))
            .collect(),
    }))
}

fn apply_route(
    id: &str,
    route: Option<pb::PublishedRoute>,
    view: &mut View,
    upserts: &mut BTreeMap<String, pb::PublishedRoute>,
    deleted: &mut BTreeSet<String>,
) {
    match route {
        Some(route) if view.routes.get(id) != Some(&route) => {
            view.routes.insert(id.to_owned(), route.clone());
            upserts.insert(id.to_owned(), route);
            deleted.remove(id);
        }
        Some(_) => {}
        None if view.routes.remove(id).is_some() => {
            upserts.remove(id);
            deleted.insert(id.to_owned());
        }
        None => {}
    }
}

#[tonic::async_trait]
impl pb::environment_directory_service_server::EnvironmentDirectoryService for RoutePublisher {
    type WatchEnvironmentsStream =
        ReceiverStream<std::result::Result<pb::EnvironmentDirectoryFrame, Status>>;

    async fn watch_environments(
        &self,
        request: Request<pb::WatchEnvironmentsRequest>,
    ) -> std::result::Result<Response<Self::WatchEnvironmentsStream>, Status> {
        if self.peers.authenticate(&request)? != Principal::ApiServer {
            return Err(Status::permission_denied("API Server identity required"));
        }
        let mut closing = self.closing.subscribe();
        if *closing.borrow() {
            return Err(Status::unavailable("Coordinator shutting down"));
        }
        let view = self.view.lock().await;
        if view.state != PublicationState::Ready {
            return Err(Status::unavailable("environment publication not ready"));
        }
        let mut updates = self.environment_changes.subscribe();
        let revision = view
            .revision
            .ok_or_else(|| Status::unavailable("environment publication has no revision"))?;
        let full = pb::EnvironmentDirectoryFrame {
            epoch: self.session.epoch(),
            revision,
            base_revision: 0,
            reset: true,
            upserts: view.environments.values().cloned().collect(),
            deleted: Vec::new(),
        };
        drop(view);
        let (tx, rx) = mpsc::channel(8);
        tokio::spawn(async move {
            let forward = async {
                if tx.send(Ok(full)).await.is_err() {
                    return;
                }
                loop {
                    let next = tokio::select! {_=tx.closed()=>return,next=updates.recv()=>next};
                    match next {
                        Ok(frame) => {
                            if tx.send(Ok(frame)).await.is_err() {
                                return;
                            }
                        }
                        Err(_) => {
                            let _ = tx
                                .send(Err(Status::out_of_range(
                                    "environment history lost; resubscribe for full snapshot",
                                )))
                                .await;
                            return;
                        }
                    }
                }
            };
            tokio::select! {
                _ = closing.wait_for(|value| *value) => {},
                _ = forward => {},
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
fn tracing_unavailable() {
    eprintln!("Coordinator route publication awaiting authoritative storage");
}

#[tonic::async_trait]
impl pb::route_service_server::RouteService for RoutePublisher {
    type WatchRoutesStream = ReceiverStream<std::result::Result<pb::RouteFrame, Status>>;
    async fn watch_routes(
        &self,
        request: Request<pb::WatchRoutesRequest>,
    ) -> std::result::Result<Response<Self::WatchRoutesStream>, Status> {
        if self.peers.authenticate(&request)? != Principal::Ingress {
            return Err(Status::permission_denied("Ingress identity required"));
        }
        let mut closing = self.closing.subscribe();
        if *closing.borrow() {
            return Err(Status::unavailable("Coordinator shutting down"));
        }
        let view = self.view.lock().await;
        if view.state != PublicationState::Ready {
            return Err(Status::unavailable("route publication not ready"));
        }
        // Subscribe under the same lock as the full snapshot so there is no list/watch gap.
        let mut updates = self.changes.subscribe();
        let revision = view
            .revision
            .ok_or_else(|| Status::unavailable("route publication has no revision"))?;
        let full = pb::RouteFrame {
            epoch: self.session.epoch(),
            revision,
            reset: true,
            upserts: view.routes.values().cloned().collect(),
            ..Default::default()
        };
        drop(view);
        let (tx, rx) = mpsc::channel(8);
        tokio::spawn(async move {
            let forward = async {
                if tx.send(Ok(full)).await.is_err() {
                    return;
                }
                loop {
                    let next = tokio::select! {_=tx.closed()=>return,next=updates.recv()=>next};
                    match next {
                        Ok(frame) => {
                            if tx.send(Ok(frame)).await.is_err() {
                                return;
                            }
                        }
                        Err(_) => {
                            let _ = tx
                                .send(Err(Status::out_of_range(
                                    "route history lost; resubscribe for full snapshot",
                                )))
                                .await;
                            return;
                        }
                    }
                }
            };
            tokio::select! {
                _ = closing.wait_for(|value| *value) => {},
                _ = forward => {},
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn overflow_between_first_receive_and_batch_drain_requires_reset() {
        let (sender, mut receiver) = broadcast::channel(1024);
        let change = |revision| ControlChange {
            revision,
            values: vec![],
            dirty: false,
            fields: vec![],
            epoch: 1,
        };
        sender.send(change(1)).unwrap();
        let mut batch = vec![receiver.recv().await.unwrap()];
        for revision in 2..=1030 {
            sender.send(change(revision)).unwrap();
        }
        assert!(matches!(
            drain_changes(&mut receiver, &mut batch),
            Err((PublicationReset::BatchLag, 5))
        ));
    }
    #[test]
    fn batch_budget_does_not_drain_a_continuously_busy_receiver() {
        let (sender, mut receiver) = broadcast::channel(1024);
        for revision in 1..=1024 {
            sender
                .send(ControlChange {
                    revision,
                    values: vec![],
                    dirty: false,
                    fields: vec![],
                    epoch: 1,
                })
                .unwrap();
        }
        let mut batch = vec![];
        assert!(drain_changes(&mut receiver, &mut batch).is_ok());
        assert!(!batch.is_empty());
        assert!(batch.len() <= 256);
        assert!(!receiver.is_empty());
    }

    #[test]
    fn route_uses_authoritative_envd_auth_and_preserves_sdk_signing_key() {
        let mut environment: StoredEnvironment = serde_json::from_value(serde_json::json!({
            "creation_generation": 1,
            "spec": {
                "id": "e2b-test", "tenant_id": "tenant", "image": "image",
                "runtime_class": "runtime", "priority": 0,
                "resources": {"cpu_millis": 1000, "memory_bytes": 1024, "disk_bytes": 0},
                "envd": {"version": "0.9.0", "gateway_auth": true,
                    "ingress_sources": ["10.0.0.2"]},
                "compatibility": {"template_id": "base",
                    "envd_access_token": "sdk-signing-key", "traffic_access_token": "traffic-key",
                    "allow_public_traffic": false}
            },
            "assignment": {"environment_id": "e2b-test", "node_id": "node",
                "shard_id": 0, "generation": 1, "devices": []},
            "result": null
        }))
        .unwrap();
        let compatibility = route_compatibility(&environment).unwrap();
        assert!(compatibility.envd_gateway_auth);
        assert_eq!(compatibility.envd_signing_key, "sdk-signing-key");
        assert!(!compatibility.envd_ready);

        environment.spec.envd.as_mut().unwrap().gateway_auth = false;
        let compatibility = route_compatibility(&environment).unwrap();
        assert!(!compatibility.envd_gateway_auth);
        assert!(compatibility.envd_signing_key.is_empty());
        environment.spec.envd = None;
        assert!(!route_compatibility(&environment).unwrap().envd_gateway_auth);
    }
}
