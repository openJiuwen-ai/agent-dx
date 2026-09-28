//! One shared publication view, refreshed from committed storage only.
use crate::storage::{ControlChange, Session, StoredEnvironment, StoredNode};
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
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
fn security(value: adx_core::sandbox::DataPlaneSecurityMode) -> i32 {
    match value {
        // The current deployment default is tls-token. Resolve inheritance at
        // publication so every Ingress sees one authoritative per-environment mode.
        adx_core::sandbox::DataPlaneSecurityMode::Inherit => {
            pb::DataPlaneSecurityMode::DataPlaneSecurityTlsToken as i32
        }
        adx_core::sandbox::DataPlaneSecurityMode::Tls => {
            pb::DataPlaneSecurityMode::DataPlaneSecurityTls as i32
        }
        adx_core::sandbox::DataPlaneSecurityMode::TlsToken => {
            pb::DataPlaneSecurityMode::DataPlaneSecurityTlsToken as i32
        }
    }
}
struct View {
    revision: Option<u64>,
    available: bool,
    nodes: BTreeMap<String, StoredNode>,
    routes: BTreeMap<String, pb::PublishedRoute>,
    environments: BTreeMap<String, pb::PublishedEnvironment>,
}
#[derive(Clone)]
pub struct RoutePublisher {
    session: Session,
    peers: Peers,
    view: Arc<Mutex<View>>,
    committed: Arc<Mutex<Option<broadcast::Receiver<ControlChange>>>>,
    changes: broadcast::Sender<pb::RouteFrame>,
    environment_changes: broadcast::Sender<pb::EnvironmentDirectoryFrame>,
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
                available: false,
                nodes: BTreeMap::new(),
                routes: BTreeMap::new(),
                environments: BTreeMap::new(),
            })),
            committed: Arc::new(Mutex::new(Some(committed))),
            changes,
            environment_changes,
        }
    }
    pub async fn refresh(&self) -> Result<()> {
        let result = self.refresh_inner().await;
        if result.is_err() {
            self.view.lock().await.available = false;
        }
        result
    }
    /// Rebuild an unavailable publication view from committed storage.
    ///
    /// A healthy view advances through `ControlChange` events. Polling the
    /// Redis revision while healthy can race with event delivery and turn an
    /// ordinary commit into a full control-hash scan.
    pub async fn recover(&self) -> Result<()> {
        if self.view.lock().await.available {
            return Ok(());
        }
        self.refresh().await
    }
    async fn refresh_inner(&self) -> Result<()> {
        let revision = self.session.revision().await?;
        {
            let mut view = self.view.lock().await;
            if view.revision == Some(revision) {
                view.available = true;
                return Ok(());
            }
        }
        let snapshot = self.session.snapshot().await?;
        let mut next = BTreeMap::new();
        for r in snapshot.routes()? {
            let i = &snapshot.environments[&r.environment_id];
            let record = i.result.as_ref().ok_or(Error::Conflict)?;
            next.insert(
                r.environment_id.clone(),
                pb::PublishedRoute {
                    environment_id: r.environment_id,
                    tenant_id: i.spec.tenant_id.clone(),
                    runtime_id: record.runtime.id.clone(),
                    runtime_ip: record.runtime.ip.ok_or(Error::Conflict)?.to_string(),
                    relay_address: r.proxy_address,
                    generation: r.generation,
                    environment_revision: r.environment_revision,
                    tunnel_security_mode: security(i.spec.sandbox.data_plane.tunnel),
                    port_forward_security_mode: security(i.spec.sandbox.data_plane.port_forward),
                    forwarded_ports: i
                        .spec
                        .sandbox
                        .ports
                        .iter()
                        .map(|port| u32::from(*port))
                        .collect(),
                },
            );
        }
        let mut next_environments = BTreeMap::new();
        for (id, environment) in &snapshot.environments {
            if environment.effective_record().state == adx_core::EnvironmentState::Deleted {
                continue;
            }
            let record = environment.effective_record();
            let node = &snapshot.nodes[&environment.assignment.node_id];
            next_environments.insert(
                id.clone(),
                pb::PublishedEnvironment {
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
            reset: false,
            upserts: next
                .iter()
                .filter(|(id, r)| view.routes.get(*id) != Some(r))
                .map(|(_, r)| r.clone())
                .collect(),
            deleted: view
                .routes
                .keys()
                .filter(|id| !next.contains_key(*id))
                .cloned()
                .collect(),
        };
        let environment_frame = pb::EnvironmentDirectoryFrame {
            epoch: self.session.epoch(),
            revision: snapshot.revision,
            base_revision: view.revision.unwrap_or(0),
            reset: false,
            upserts: next_environments
                .iter()
                .filter(|(id, environment)| view.environments.get(*id) != Some(environment))
                .map(|(_, environment)| environment.clone())
                .collect(),
            deleted: view
                .environments
                .keys()
                .filter(|id| !next_environments.contains_key(*id))
                .cloned()
                .collect(),
        };
        view.routes = next;
        view.environments = next_environments;
        view.nodes = snapshot.nodes;
        view.revision = Some(snapshot.revision);
        view.available = true;
        let _ = self.changes.send(frame);
        let _ = self.environment_changes.send(environment_frame);
        Ok(())
    }

    async fn refresh_incremental(&self, changes: Vec<ControlChange>) -> Result<()> {
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
            || changes.iter().any(|change| change.fields.is_empty())
        {
            return self.refresh_inner().await;
        }
        let revision = changes.last().expect("nonempty changes").revision;
        let mut environment_ids = BTreeSet::new();
        let mut node_ids = BTreeSet::new();
        for field in changes.into_iter().flat_map(|change| change.fields) {
            if let Some(id) = field.strip_prefix("environment:") {
                environment_ids.insert(id.to_owned());
            } else if let Some(id) = field.strip_prefix("node:") {
                node_ids.insert(id.to_owned());
            } else {
                return self.refresh_inner().await;
            }
        }

        let mut nodes = BTreeMap::new();
        for id in &node_ids {
            nodes.insert(id.clone(), self.session.get_node(id).await?);
        }
        let mut environments = BTreeMap::new();
        for id in &environment_ids {
            let environment = match self.session.get(id).await {
                Ok(environment) => Some(environment),
                Err(Error::NotFound) => None,
                Err(error) => return Err(error),
            };
            if let Some(environment) = &environment {
                let node_id = environment.assignment.node_id.clone();
                if !nodes.contains_key(&node_id) {
                    nodes.insert(node_id.clone(), self.session.get_node(&node_id).await?);
                }
            }
            environments.insert(id.clone(), environment);
        }

        let mut view = self.view.lock().await;
        if view.revision != Some(base_revision) {
            return Err(Error::Conflict);
        }
        let mut route_upserts = BTreeMap::new();
        let mut route_deleted = BTreeSet::new();
        let mut environment_upserts = BTreeMap::new();
        let mut environment_deleted = BTreeSet::new();

        for id in node_ids {
            let node = nodes.get(&id).ok_or(Error::NotFound)?;
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
                .environments
                .iter()
                .filter(|(_, environment)| {
                    environment
                        .record
                        .as_ref()
                        .and_then(|record| record.assignment.as_ref())
                        .is_some_and(|assignment| assignment.node_id == id)
                })
                .map(|(environment_id, environment)| (environment_id.clone(), environment.clone()))
                .collect();
            for (environment_id, mut published) in affected {
                published.node_address = node.address.clone();
                published.relay_address = node.proxy_address.clone();
                if view.environments.get(&environment_id) != Some(&published) {
                    view.environments
                        .insert(environment_id.clone(), published.clone());
                    environment_upserts.insert(environment_id.clone(), published.clone());
                }
                apply_route(
                    &environment_id,
                    route_from_published(&published, node)?,
                    &mut view,
                    &mut route_upserts,
                    &mut route_deleted,
                );
            }
        }

        for (id, environment) in environments {
            let Some(environment) = environment else {
                environment_upserts.remove(&id);
                if view.environments.remove(&id).is_some() {
                    environment_deleted.insert(id.clone());
                }
                apply_route(&id, None, &mut view, &mut route_upserts, &mut route_deleted);
                continue;
            };
            let node = nodes
                .get(&environment.assignment.node_id)
                .ok_or(Error::NotFound)?;
            view.nodes
                .insert(environment.assignment.node_id.clone(), node.clone());
            if environment.effective_record().state == adx_core::EnvironmentState::Deleted {
                environment_upserts.remove(&id);
                if view.environments.remove(&id).is_some() {
                    environment_deleted.insert(id.clone());
                }
            } else {
                let published = published_environment(&environment, node)?;
                if view.environments.get(&id) != Some(&published) {
                    view.environments.insert(id.clone(), published.clone());
                    environment_upserts.insert(id.clone(), published);
                }
            }
            apply_route(
                &id,
                published_route(&environment, node)?,
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
        view.available = true;
        drop(view);
        let _ = self.changes.send(frame);
        let _ = self.environment_changes.send(environment_frame);
        Ok(())
    }

    pub async fn run(self, interval: Duration) {
        let mut tick = tokio::time::interval(interval);
        let mut committed = self
            .committed
            .lock()
            .await
            .take()
            .expect("route publisher may only run once");
        loop {
            let changes = tokio::select! {
                biased;
                changed = committed.recv() => {
                    let first = match changed {
                        Ok(change) => change,
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            if self.refresh().await.is_err() { tracing_unavailable(); }
                            continue;
                        }
                        Err(broadcast::error::RecvError::Closed) => return,
                    };
                    // Coalesce nearby commits without postponing publication
                    // indefinitely when writes remain continuous.
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    let mut changes = vec![first];
                    loop {
                        match committed.try_recv() {
                            Ok(change) => changes.push(change),
                            Err(broadcast::error::TryRecvError::Empty) => break,
                            Err(broadcast::error::TryRecvError::Lagged(_)) => {
                                changes.clear();
                                break;
                            }
                            Err(broadcast::error::TryRecvError::Closed) => return,
                        }
                    }
                    Some(changes)
                }
                _ = tick.tick() => None,
            };
            let result = match changes {
                Some(changes) if !changes.is_empty() => self.refresh_incremental(changes).await,
                _ => self.recover().await,
            };
            if result.is_err() {
                self.view.lock().await.available = false;
                tracing_unavailable();
            }
        }
    }
}

fn published_environment(
    environment: &StoredEnvironment,
    node: &StoredNode,
) -> Result<pb::PublishedEnvironment> {
    Ok(pb::PublishedEnvironment {
        record: Some(environment.effective_record().try_into()?),
        node_address: node.address.clone(),
        relay_address: node.proxy_address.clone(),
    })
}

fn published_route(
    environment: &StoredEnvironment,
    node: &StoredNode,
) -> Result<Option<pb::PublishedRoute>> {
    let Some(record) = environment.result.as_ref().filter(|record| {
        record.state == adx_core::EnvironmentState::Running
            && node.session.as_ref().is_none_or(|session| session.routable)
    }) else {
        return Ok(None);
    };
    Ok(Some(pb::PublishedRoute {
        environment_id: record.spec.id.clone(),
        tenant_id: record.spec.tenant_id.clone(),
        runtime_id: record.runtime.id.clone(),
        runtime_ip: record.runtime.ip.ok_or(Error::Conflict)?.to_string(),
        relay_address: node.proxy_address.clone(),
        generation: record.assignment.generation,
        environment_revision: record.revision,
        tunnel_security_mode: security(record.spec.sandbox.data_plane.tunnel),
        port_forward_security_mode: security(record.spec.sandbox.data_plane.port_forward),
        forwarded_ports: record
            .spec
            .sandbox
            .ports
            .iter()
            .map(|port| u32::from(*port))
            .collect(),
    }))
}

fn route_from_published(
    environment: &pb::PublishedEnvironment,
    node: &StoredNode,
) -> Result<Option<pb::PublishedRoute>> {
    let Some(record) = environment.record.as_ref().filter(|record| {
        record.state == pb::EnvironmentState::Running as i32
            && node.session.as_ref().is_none_or(|session| session.routable)
    }) else {
        return Ok(None);
    };
    let spec = record.spec.as_ref().ok_or(Error::Conflict)?;
    let assignment = record.assignment.as_ref().ok_or(Error::Conflict)?;
    let runtime = record.runtime.as_ref().ok_or(Error::Conflict)?;
    let sandbox = spec.sandbox.as_ref().ok_or(Error::Conflict)?;
    let data_plane = sandbox.data_plane.as_ref().ok_or(Error::Conflict)?;
    let inherited = pb::DataPlaneSecurityMode::DataPlaneSecurityTlsToken as i32;
    Ok(Some(pb::PublishedRoute {
        environment_id: spec.id.clone(),
        tenant_id: spec.tenant_id.clone(),
        runtime_id: runtime.id.clone(),
        runtime_ip: runtime.ip.clone(),
        relay_address: node.proxy_address.clone(),
        generation: assignment.generation,
        environment_revision: record.revision,
        tunnel_security_mode: if data_plane.tunnel == 0 {
            inherited
        } else {
            data_plane.tunnel
        },
        port_forward_security_mode: if data_plane.port_forward == 0 {
            inherited
        } else {
            data_plane.port_forward
        },
        forwarded_ports: sandbox.ports.clone(),
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
        let view = self.view.lock().await;
        if !view.available {
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
        let view = self.view.lock().await;
        if !view.available {
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
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
