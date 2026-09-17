//! Real Node Manager client -> real Node Proxy gRPC handler over local UDS.
use adx_core::{Assignment, Error, InstanceRecord, InstanceSpec, InstanceState, Resources};
use adx_node_manager::{routes::UdsRoutes, Routes};
use data_plane_gateway::node::route_control::proto as pb;
use data_plane_gateway::{
    common::protocol::GatewayPolicy,
    node::{
        route_control::{
            proto::{
                node_proxy_service_server::{NodeProxyService, NodeProxyServiceServer},
                update_binding_request::Binding,
                RuntimeTarget, UpdateBindingRequest, UpdateBindingResponse,
            },
            BindingService,
        },
        NodeProxy,
    },
};
use std::{sync::Arc, time::Duration};
use tokio::{net::UnixListener, sync::Semaphore};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status};

fn record() -> InstanceRecord {
    InstanceRecord {
        restart_attempts: 0,
        restart_pending: false,
        spec: InstanceSpec {
            snapshot_id: None,
            lifecycle: Default::default(),
            env: Default::default(),
            scheduling: Default::default(),
            id: "i".into(),
            tenant_id: "t".into(),
            image: "rrt".into(),
            runtime: "runsc".into(),
            resources: Resources {
                cpu_millis: 1000,
                memory_bytes: 1 << 30,
                disk_bytes: 1 << 30,
            },
            priority: 0,
        },
        assignment: Assignment {
            devices: vec![],
            instance_id: "i".into(),
            node_id: "n".into(),
            shard_id: 0,
            generation: 42,
        },
        runtime_id: "i-42".into(),
        runtime_ip: Some("10.0.0.2".parse().unwrap()),
        checkpoint: None,
        last_operation: None,
        state: InstanceState::Starting,
        revision: 1,
        resources_held: true,
    }
}
fn active(generation: u64, revision: u64) -> UpdateBindingRequest {
    UpdateBindingRequest {
        proxy_session_id: String::new(),
        sync_epoch: 0,
        instance_id: "i".into(),
        ownership_generation: generation,
        binding_revision: revision,
        binding: Some(Binding::Active(RuntimeTarget {
            runtime_id: format!("i-{generation}"),
            ip: "10.0.0.2".into(),
        })),
    }
}
fn service() -> Arc<BindingService> {
    Arc::new(BindingService::new(Arc::new(
        NodeProxy::new(GatewayPolicy::new(vec![])).with_route_enforcement(),
    )))
}
struct Server {
    directory: tempfile::TempDir,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn start<T: NodeProxyService>(service: T) -> (Server, UdsRoutes) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("route.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(NodeProxyServiceServer::new(service))
            .serve_with_incoming(UnixListenerStream::new(listener))
            .await
            .unwrap();
    });
    let adapter = UdsRoutes::new(path, Duration::from_secs(2)).unwrap();
    adapter.begin_reconcile().await.unwrap();
    adapter.finish_reconcile().await.unwrap();
    (Server { directory, task }, adapter)
}
struct Handler(Arc<BindingService>);
#[tonic::async_trait]
impl NodeProxyService for Handler {
    async fn get_instance_activity(
        &self,
        _: Request<pb::GetInstanceActivityRequest>,
    ) -> Result<Response<pb::InstanceActivityState>, Status> {
        Err(Status::unimplemented("fixture has no activity"))
    }

    async fn get_binding_state(
        &self,
        r: Request<data_plane_gateway::node::route_control::proto::GetBindingStateRequest>,
    ) -> Result<Response<data_plane_gateway::node::route_control::proto::BindingState>, Status>
    {
        self.0.get_binding_state(r).await
    }
    async fn begin_bindings(
        &self,
        r: Request<data_plane_gateway::node::route_control::proto::BindingState>,
    ) -> Result<Response<data_plane_gateway::node::route_control::proto::BindingState>, Status>
    {
        self.0.begin_bindings(r).await
    }
    async fn replace_bindings(
        &self,
        r: Request<data_plane_gateway::node::route_control::proto::BindingSnapshot>,
    ) -> Result<Response<data_plane_gateway::node::route_control::proto::BindingState>, Status>
    {
        self.0.replace_bindings(r).await
    }

    async fn update_binding(
        &self,
        request: Request<UpdateBindingRequest>,
    ) -> Result<Response<UpdateBindingResponse>, Status> {
        self.0.update_binding(request).await
    }
}
#[tokio::test]
async fn grpc_applies_retries_and_retirement_fences_old_activation() {
    let handler = service();
    let (_server, adapter) = start(Handler(handler.clone())).await;
    adapter.activate(&record()).await.unwrap();
    adapter.activate(&record()).await.unwrap();
    adapter.retire(&record()).await.unwrap();
    adapter.retire(&record()).await.unwrap();
    assert_eq!(adapter.activate(&record()).await, Err(Error::Conflict));
    assert_eq!(
        handler.apply(active(42, 2)).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
}
#[tokio::test]
async fn ownership_generation_and_same_version_conflicts_are_checked() {
    let handler = service();
    let state = handler.begin(handler.inspect().await).await.unwrap();
    handler
        .replace(
            data_plane_gateway::node::route_control::proto::BindingSnapshot {
                proxy_session_id: state.proxy_session_id.clone(),
                sync_epoch: state.sync_epoch,
                bindings: vec![],
            },
        )
        .await
        .unwrap();
    let active = |g, r| {
        let mut v = active(g, r);
        v.proxy_session_id = state.proxy_session_id.clone();
        v.sync_epoch = state.sync_epoch;
        v
    };
    handler.apply(active(43, 2)).await.unwrap();
    assert_eq!(
        handler.apply(active(42, 999)).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    let mut changed = active(43, 2);
    if let Some(Binding::Active(target)) = &mut changed.binding {
        target.ip = "10.0.0.3".into();
    }
    assert_eq!(
        handler.apply(changed).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    handler.apply(active(43, 4)).await.unwrap();
}
struct Delayed {
    service: Arc<BindingService>,
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    done: Arc<Semaphore>,
}
#[tonic::async_trait]
impl NodeProxyService for Delayed {
    async fn get_instance_activity(
        &self,
        _: Request<pb::GetInstanceActivityRequest>,
    ) -> Result<Response<pb::InstanceActivityState>, Status> {
        Err(Status::unimplemented("fixture has no activity"))
    }

    async fn get_binding_state(
        &self,
        r: Request<data_plane_gateway::node::route_control::proto::GetBindingStateRequest>,
    ) -> Result<Response<data_plane_gateway::node::route_control::proto::BindingState>, Status>
    {
        self.service.get_binding_state(r).await
    }
    async fn begin_bindings(
        &self,
        r: Request<data_plane_gateway::node::route_control::proto::BindingState>,
    ) -> Result<Response<data_plane_gateway::node::route_control::proto::BindingState>, Status>
    {
        self.service.begin_bindings(r).await
    }
    async fn replace_bindings(
        &self,
        r: Request<data_plane_gateway::node::route_control::proto::BindingSnapshot>,
    ) -> Result<Response<data_plane_gateway::node::route_control::proto::BindingState>, Status>
    {
        self.service.replace_bindings(r).await
    }

    async fn update_binding(
        &self,
        request: Request<UpdateBindingRequest>,
    ) -> Result<Response<UpdateBindingResponse>, Status> {
        let message = request.into_inner();
        if matches!(message.binding, Some(Binding::Active(_))) {
            self.entered.add_permits(1);
            // Emulate a delayed transport delivery independent of the canceled client.
            let service = self.service.clone();
            let release = self.release.clone();
            let done = self.done.clone();
            let result = tokio::spawn(async move {
                release.acquire().await.unwrap().forget();
                let result = service.apply(message).await;
                assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);
                done.add_permits(1);
            });
            result.await.unwrap();
            Err(Status::failed_precondition("activation superseded"))
        } else {
            Ok(Response::new(self.service.apply(message).await?))
        }
    }
}
#[tokio::test]
async fn delayed_activation_cannot_revive_a_retired_binding() {
    let handler = service();
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let done = Arc::new(Semaphore::new(0));
    let (_server, adapter) = start(Delayed {
        service: handler,
        entered: entered.clone(),
        release: release.clone(),
        done: done.clone(),
    })
    .await;
    let request = {
        let adapter = adapter.clone();
        tokio::spawn(async move { adapter.activate(&record()).await })
    };
    entered.acquire().await.unwrap().forget();
    request.abort();
    adapter.retire(&record()).await.unwrap();
    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), done.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}
#[tokio::test]
async fn binding_socket_cannot_replace_an_active_listener() {
    let (server, _adapter) = start(Handler(service())).await;
    let path = server.directory.path().join("route.sock");
    assert_eq!(
        data_plane_gateway::node::bind_route_control(path.to_str().unwrap())
            .await
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::AddrInUse
    );
}

#[tokio::test]
async fn startup_snapshot_gates_traffic_and_proxy_restart_replays_local_catalog() {
    let proxy = Arc::new(NodeProxy::new(GatewayPolicy::new(vec![])).with_route_enforcement());
    let handler = Arc::new(BindingService::new(proxy.clone()));
    assert!(!proxy.ready());
    let (mut server, adapter) = start(Handler(handler.clone())).await;
    assert!(proxy.ready());
    adapter.begin_reconcile().await.unwrap();
    assert!(!proxy.ready());
    adapter.activate(&record()).await.unwrap();
    assert!(!proxy.ready());
    adapter.finish_reconcile().await.unwrap();
    assert!(proxy.ready());
    let old = handler.inspect().await;
    let mut delayed = active(42, 100);
    delayed.proxy_session_id = old.proxy_session_id;
    delayed.sync_epoch = old.sync_epoch;
    server.task.abort();
    let _ = (&mut server.task).await;
    let path = server.directory.path().join("route.sock");
    std::fs::remove_file(&path).unwrap();
    let replacement = Arc::new(NodeProxy::new(GatewayPolicy::new(vec![])).with_route_enforcement());
    let new = Arc::new(BindingService::new(replacement.clone()));
    let new_handler = new.clone();
    let listener = UnixListener::bind(path).unwrap();
    server.task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(NodeProxyServiceServer::new(Handler(new_handler)))
            .serve_with_incoming(UnixListenerStream::new(listener))
            .await
            .unwrap();
    });
    assert!(!replacement.ready());
    adapter.ensure_synced().await.unwrap();
    assert!(replacement.ready());
    assert_eq!(
        new.apply(delayed).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    adapter.retire(&record()).await.unwrap();
}
#[tokio::test]
async fn full_snapshot_retries_and_invalid_snapshot_never_open_admission() {
    use data_plane_gateway::node::route_control::proto::BindingSnapshot;
    let proxy = Arc::new(NodeProxy::new(GatewayPolicy::new(vec![])).with_route_enforcement());
    let service = BindingService::new(proxy.clone());
    let state = service.begin(service.inspect().await).await.unwrap();
    let mut binding = active(1, 2);
    binding.proxy_session_id = state.proxy_session_id.clone();
    binding.sync_epoch = state.sync_epoch;
    let snapshot = BindingSnapshot {
        proxy_session_id: state.proxy_session_id.clone(),
        sync_epoch: state.sync_epoch,
        bindings: vec![binding.clone()],
    };
    let mut invalid = snapshot.clone();
    invalid.bindings.push(binding);
    assert!(service.replace(invalid).await.is_err());
    assert!(!proxy.ready());
    let accepted = service.replace(snapshot.clone()).await.unwrap();
    assert!(proxy.ready());
    assert_eq!(service.replace(snapshot).await.unwrap(), accepted);
    let next = service.begin(service.inspect().await).await.unwrap();
    assert!(!proxy.ready());
    service
        .replace(BindingSnapshot {
            proxy_session_id: next.proxy_session_id,
            sync_epoch: next.sync_epoch,
            bindings: vec![],
        })
        .await
        .unwrap();
    assert!(proxy.ready());
}
