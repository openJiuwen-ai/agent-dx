//! One shared publication view, refreshed from committed storage only.
use crate::storage::Session;
use adx_core::{Error, Result};
use adx_protocol::{
    auth::{Peers, Principal},
    control as pb,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
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
    routes: BTreeMap<String, pb::PublishedRoute>,
    environments: BTreeMap<String, pb::PublishedEnvironment>,
}
#[derive(Clone)]
pub struct RoutePublisher {
    session: Session,
    peers: Peers,
    view: Arc<Mutex<View>>,
    changes: broadcast::Sender<pb::RouteFrame>,
    environment_changes: broadcast::Sender<pb::EnvironmentDirectoryFrame>,
}
impl RoutePublisher {
    pub fn new(session: Session, peers: Peers) -> Self {
        let (changes, _) = broadcast::channel(64);
        let (environment_changes, _) = broadcast::channel(64);
        Self {
            session,
            peers,
            view: Arc::new(Mutex::new(View {
                revision: None,
                available: false,
                routes: BTreeMap::new(),
                environments: BTreeMap::new(),
            })),
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
    async fn refresh_inner(&self) -> Result<()> {
        let mut view = self.view.lock().await;
        let revision = self.session.revision().await?;
        if view.revision == Some(revision) {
            view.available = true;
            return Ok(());
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
        view.revision = Some(snapshot.revision);
        view.available = true;
        let _ = self.changes.send(frame);
        let _ = self.environment_changes.send(environment_frame);
        Ok(())
    }
    pub async fn run(self, interval: Duration) {
        let mut tick = tokio::time::interval(interval);
        let mut committed = self.session.committed_revisions();
        loop {
            tokio::select! {
                _ = tick.tick() => {},
                changed = committed.changed() => {
                    if changed.is_err() {
                        continue;
                    }
                    // Coalesce nearby commits without postponing publication
                    // indefinitely when writes remain continuous.
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    committed.borrow_and_update();
                }
            }
            if self.refresh().await.is_err() {
                tracing_unavailable();
            }
        }
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
