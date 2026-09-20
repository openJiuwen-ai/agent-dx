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
struct View {
    revision: Option<u64>,
    available: bool,
    routes: BTreeMap<String, pb::PublishedRoute>,
    instances: BTreeMap<String, pb::PublishedInstance>,
}
#[derive(Clone)]
pub struct RoutePublisher {
    session: Session,
    peers: Peers,
    view: Arc<Mutex<View>>,
    changes: broadcast::Sender<pb::RouteFrame>,
    instance_changes: broadcast::Sender<pb::InstanceDirectoryFrame>,
}
impl RoutePublisher {
    pub fn new(session: Session, peers: Peers) -> Self {
        let (changes, _) = broadcast::channel(64);
        let (instance_changes, _) = broadcast::channel(64);
        Self {
            session,
            peers,
            view: Arc::new(Mutex::new(View {
                revision: None,
                available: false,
                routes: BTreeMap::new(),
                instances: BTreeMap::new(),
            })),
            changes,
            instance_changes,
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
            let i = &snapshot.instances[&r.instance_id];
            let record = i.result.as_ref().ok_or(Error::Conflict)?;
            next.insert(
                r.instance_id.clone(),
                pb::PublishedRoute {
                    instance_id: r.instance_id,
                    tenant_id: i.spec.tenant_id.clone(),
                    runtime_id: record.runtime_id.clone(),
                    runtime_ip: record.runtime_ip.ok_or(Error::Conflict)?.to_string(),
                    node_proxy_address: r.proxy_address,
                    generation: r.generation,
                    instance_revision: r.instance_revision,
                },
            );
        }
        let mut next_instances = BTreeMap::new();
        for (id, instance) in &snapshot.instances {
            let record = instance.effective_record();
            let node = &snapshot.nodes[&instance.assignment.node_id];
            next_instances.insert(
                id.clone(),
                pb::PublishedInstance {
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
        let instance_frame = pb::InstanceDirectoryFrame {
            epoch: self.session.epoch(),
            revision: snapshot.revision,
            base_revision: view.revision.unwrap_or(0),
            reset: false,
            upserts: next_instances
                .iter()
                .filter(|(id, instance)| view.instances.get(*id) != Some(instance))
                .map(|(_, instance)| instance.clone())
                .collect(),
            deleted: view
                .instances
                .keys()
                .filter(|id| !next_instances.contains_key(*id))
                .cloned()
                .collect(),
        };
        view.routes = next;
        view.instances = next_instances;
        view.revision = Some(snapshot.revision);
        view.available = true;
        let _ = self.changes.send(frame);
        let _ = self.instance_changes.send(instance_frame);
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
impl pb::instance_directory_service_server::InstanceDirectoryService for RoutePublisher {
    type WatchInstancesStream =
        ReceiverStream<std::result::Result<pb::InstanceDirectoryFrame, Status>>;

    async fn watch_instances(
        &self,
        request: Request<pb::WatchInstancesRequest>,
    ) -> std::result::Result<Response<Self::WatchInstancesStream>, Status> {
        if self.peers.authenticate(&request)? != Principal::ApiServer {
            return Err(Status::permission_denied("API Server identity required"));
        }
        let view = self.view.lock().await;
        if !view.available {
            return Err(Status::unavailable("instance publication not ready"));
        }
        let mut updates = self.instance_changes.subscribe();
        let revision = view
            .revision
            .ok_or_else(|| Status::unavailable("instance publication has no revision"))?;
        let full = pb::InstanceDirectoryFrame {
            epoch: self.session.epoch(),
            revision,
            base_revision: 0,
            reset: true,
            upserts: view.instances.values().cloned().collect(),
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
                                "instance history lost; resubscribe for full snapshot",
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
