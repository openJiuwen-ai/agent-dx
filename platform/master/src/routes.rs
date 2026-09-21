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
        // publication so every Edge sees one authoritative per-capsule mode.
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
    capsules: BTreeMap<String, pb::PublishedCapsule>,
}
#[derive(Clone)]
pub struct RoutePublisher {
    session: Session,
    peers: Peers,
    view: Arc<Mutex<View>>,
    changes: broadcast::Sender<pb::RouteFrame>,
    capsule_changes: broadcast::Sender<pb::CapsuleDirectoryFrame>,
}
impl RoutePublisher {
    pub fn new(session: Session, peers: Peers) -> Self {
        let (changes, _) = broadcast::channel(64);
        let (capsule_changes, _) = broadcast::channel(64);
        Self {
            session,
            peers,
            view: Arc::new(Mutex::new(View {
                revision: None,
                available: false,
                routes: BTreeMap::new(),
                capsules: BTreeMap::new(),
            })),
            changes,
            capsule_changes,
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
            let i = &snapshot.capsules[&r.capsule_id];
            let record = i.result.as_ref().ok_or(Error::Conflict)?;
            next.insert(
                r.capsule_id.clone(),
                pb::PublishedRoute {
                    capsule_id: r.capsule_id,
                    tenant_id: i.spec.tenant_id.clone(),
                    runtime_id: record.runtime.id.clone(),
                    runtime_ip: record.runtime.ip.ok_or(Error::Conflict)?.to_string(),
                    node_proxy_address: r.proxy_address,
                    generation: r.generation,
                    capsule_revision: r.capsule_revision,
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
        let mut next_capsules = BTreeMap::new();
        for (id, capsule) in &snapshot.capsules {
            let record = capsule.effective_record();
            let node = &snapshot.nodes[&capsule.assignment.node_id];
            next_capsules.insert(
                id.clone(),
                pb::PublishedCapsule {
                    record: Some(record.try_into()?),
                    node_address: node.address.clone(),
                    node_proxy_address: node.proxy_address.clone(),
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
        let capsule_frame = pb::CapsuleDirectoryFrame {
            epoch: self.session.epoch(),
            revision: snapshot.revision,
            base_revision: view.revision.unwrap_or(0),
            reset: false,
            upserts: next_capsules
                .iter()
                .filter(|(id, capsule)| view.capsules.get(*id) != Some(capsule))
                .map(|(_, capsule)| capsule.clone())
                .collect(),
            deleted: view
                .capsules
                .keys()
                .filter(|id| !next_capsules.contains_key(*id))
                .cloned()
                .collect(),
        };
        view.routes = next;
        view.capsules = next_capsules;
        view.revision = Some(snapshot.revision);
        view.available = true;
        let _ = self.changes.send(frame);
        let _ = self.capsule_changes.send(capsule_frame);
        Ok(())
    }
    pub async fn run(self, interval: Duration) {
        let mut tick = tokio::time::interval(interval);
        loop {
            tick.tick().await;
            if self.refresh().await.is_err() {
                tracing_unavailable();
            }
        }
    }
}

#[tonic::async_trait]
impl pb::capsule_directory_service_server::CapsuleDirectoryService for RoutePublisher {
    type WatchCapsulesStream =
        ReceiverStream<std::result::Result<pb::CapsuleDirectoryFrame, Status>>;

    async fn watch_capsules(
        &self,
        request: Request<pb::WatchCapsulesRequest>,
    ) -> std::result::Result<Response<Self::WatchCapsulesStream>, Status> {
        if self.peers.authenticate(&request)? != Principal::ApiServer {
            return Err(Status::permission_denied("API Server identity required"));
        }
        let view = self.view.lock().await;
        if !view.available {
            return Err(Status::unavailable("capsule publication not ready"));
        }
        let mut updates = self.capsule_changes.subscribe();
        let revision = view
            .revision
            .ok_or_else(|| Status::unavailable("capsule publication has no revision"))?;
        let full = pb::CapsuleDirectoryFrame {
            epoch: self.session.epoch(),
            revision,
            base_revision: 0,
            reset: true,
            upserts: view.capsules.values().cloned().collect(),
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
                                "capsule history lost; resubscribe for full snapshot",
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
    eprintln!("Master route publication awaiting authoritative storage");
}
#[tonic::async_trait]
impl pb::route_service_server::RouteService for RoutePublisher {
    type WatchRoutesStream = ReceiverStream<std::result::Result<pb::RouteFrame, Status>>;
    async fn watch_routes(
        &self,
        request: Request<pb::WatchRoutesRequest>,
    ) -> std::result::Result<Response<Self::WatchRoutesStream>, Status> {
        if self.peers.authenticate(&request)? != Principal::Edge {
            return Err(Status::permission_denied("Edge identity required"));
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
