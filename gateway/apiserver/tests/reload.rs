//! Real RPC transport with a deliberately stale API Server ownership cache.
use adx_apiserver::{
    clients::Clients,
    config::Config,
    operations::{Kind, Operations},
};
use adx_protocol::control as pb;
use serde_json::json;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};

#[derive(Clone)]
struct Service(Arc<Mutex<State>>);

struct State {
    owner: pb::GetEnvironmentResponse,
    lookups: usize,
    reloads: usize,
    executions: usize,
    lookup_unavailable: bool,
}

// Expand before async_trait so every generated method receives its RPC lifetime.
macro_rules! service_impl {
    ($trait:path, { $($implemented:item)* }, [$($name:ident($request:ident) -> $response:ident),* $(,)?]) => {
        #[tonic::async_trait]
        impl $trait for Service {
            $($implemented)*
            $(async fn $name(&self, _: Request<pb::$request>) -> Result<Response<pb::$response>, Status> {
                Err(Status::unimplemented("unused test RPC"))
            })*
        }
    };
}

service_impl!(pb::coordinator_service_server::CoordinatorService, {
    type WatchNodesStream = futures_util::stream::Empty<Result<pb::NodeDirectory, Status>>;
    async fn watch_nodes(&self, _: Request<pb::WatchNodesRequest>) -> Result<Response<Self::WatchNodesStream>, Status> {
        Err(Status::unimplemented("directory updates intentionally withheld"))
    }
    async fn get_environment(&self, request: Request<pb::GetEnvironmentRequest>) -> Result<Response<pb::GetEnvironmentResponse>, Status> {
        let mut state = self.0.lock().unwrap();
        state.lookups += 1;
        if state.lookup_unavailable {
            return Err(Status::unavailable("coordinator unavailable"));
        }
        assert_eq!(request.get_ref().environment_id, "environment");
        assert_eq!(request.get_ref().caller.as_ref().unwrap().tenant_id, "tenant");
        Ok(Response::new(state.owner.clone()))
    }
}, [
    get_scheduling_queue(GetSchedulingQueueRequest) -> GetSchedulingQueueResponse,
    set_node_scheduling(SetNodeSchedulingRequest) -> NodeSchedulingState,
    prepare_create(LocalEnvironmentCreateRequest) -> PreparedEnvironment,
    claim_environment(ClaimEnvironmentRequest) -> ClaimEnvironmentResponse,
    forward_create(LocalEnvironmentCreateRequest) -> EnvironmentResult,
    register_node(RegisterNodeRequest) -> RegisterNodeResponse,
    inspect_node(InspectNodeRequest) -> InspectNodeResponse,
    create_environment(CreateEnvironmentRequest) -> EnvironmentResult,
    commit_environment(CommitEnvironmentRequest) -> CommitEnvironmentResponse,
]);

service_impl!(pb::node_service_server::NodeService, {
    async fn reload_environment(&self, request: Request<pb::ReloadEnvironmentRequest>) -> Result<Response<pb::EnvironmentResult>, Status> {
        let request = request.into_inner();
        let mut state = self.0.lock().unwrap();
        state.reloads += 1;
        let record = state.owner.record.as_mut().unwrap();
        assert_eq!(request.assignment, record.assignment);
        assert_eq!(request.caller.as_ref().unwrap().tenant_id, "tenant");
        let replay = record.last_operation.as_ref().is_some_and(|op| {
            op.id == request.operation_id
                && op.kind == pb::LifecycleKind::Reload as i32
                && op.expected_revision == request.expected_revision
        });
        if !replay {
            if request.expected_revision != record.revision {
                return Err(Status::failed_precondition("identity or version conflict"));
            }
            record.revision += 1;
            record.last_operation = Some(pb::CompletedOperation {
                id: request.operation_id,
                kind: pb::LifecycleKind::Reload as i32,
                expected_revision: request.expected_revision,
            });
        }
        let result = pb::EnvironmentResult {
            record: Some(record.clone()),
            durability: pb::Durability::Published as i32,
        };
        if !replay {
            state.executions += 1;
        }
        Ok(Response::new(result))
    }
    async fn delete_environment(&self, _: Request<pb::DeleteEnvironmentRequest>) -> Result<Response<pb::EnvironmentResult>, Status> {
        let mut record = self.0.lock().unwrap().owner.record.clone().unwrap();
        record.state = pb::EnvironmentState::Deleted as i32;
        record.resources_held = false;
        Ok(Response::new(pb::EnvironmentResult {
            record: Some(record),
            durability: pb::Durability::Published as i32,
        }))
    }
}, [
    create_local_environment(LocalEnvironmentCreateRequest) -> EnvironmentResult,
    recover_environment(RecoverEnvironmentRequest) -> EnvironmentResult,
    get_session(GetNodeSessionRequest) -> GetNodeSessionResponse,
    create_environment(StartAssignedEnvironmentRequest) -> EnvironmentResult,
    pause_environment(PauseEnvironmentRequest) -> EnvironmentResult,
    resume_environment(ResumeEnvironmentRequest) -> EnvironmentResult,
    update_network_policy(UpdateNetworkPolicyRequest) -> EnvironmentResult,
    create_snapshot(CreateSnapshotRequest) -> CreateSnapshotResponse,
    collect_snapshot(CollectSnapshotRequest) -> CollectSnapshotResponse,
]);

struct Fixture {
    clients: Arc<Clients>,
    operations: Operations,
    state: Arc<Mutex<State>>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Fixture {
    async fn new() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let owner = pb::GetEnvironmentResponse {
            record: Some(pb::EnvironmentRecord {
                spec: Some(pb::EnvironmentSpec {
                    id: "environment".into(),
                    tenant_id: "tenant".into(),
                    ..Default::default()
                }),
                assignment: Some(pb::Assignment {
                    environment_id: "environment".into(),
                    node_id: "node".into(),
                    generation: 1,
                    ..Default::default()
                }),
                state: pb::EnvironmentState::Running as i32,
                revision: 3,
                resources_held: true,
                ..Default::default()
            }),
            node_address: address.clone(),
            relay_address: "127.0.0.1:9999".into(),
        };
        let state = Arc::new(Mutex::new(State {
            owner: owner.clone(),
            lookups: 0,
            reloads: 0,
            executions: 0,
            lookup_unavailable: false,
        }));
        let service = Service(state.clone());
        let server = tokio::spawn(async move {
            let incoming = futures_util::stream::unfold(listener, |listener| async {
                let socket = listener.accept().await.map(|(socket, _)| socket);
                Some((socket, listener))
            });
            tonic::transport::Server::builder()
                .add_service(
                    pb::coordinator_service_server::CoordinatorServiceServer::new(service.clone()),
                )
                .add_service(pb::node_service_server::NodeServiceServer::new(service))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let config: Config = serde_json::from_value(json!({
            "listen": "127.0.0.1:0", "coordinator_address": address,
            "internal_security": "network", "ingress_mode": "standalone",
            "rpc_timeout_seconds": 2, "cache_entries": 16, "auth_cache_ttl_seconds": 1
        }))
        .unwrap();
        let clients = Clients::new(config).unwrap();
        let mut stale = owner;
        // Checkpoint has been published at revision 3, but its watch delta has not arrived.
        stale.record.as_mut().unwrap().revision = 2;
        clients.put_owner(stale).await.unwrap();
        Self {
            operations: Operations::new(clients.clone()),
            clients,
            state,
            server,
        }
    }

    async fn execute(&self, kind: Kind) -> Result<serde_json::Value, Status> {
        self.operations
            .execute(
                kind,
                "environment",
                "reload-test",
                json!({}),
                &pb::CallerContext {
                    tenant_id: "tenant".into(),
                    administrator: false,
                },
            )
            .await
    }
}

#[tokio::test]
async fn reload_refreshes_checkpoint_revision_and_replays_without_new_execution() {
    let fixture = Fixture::new().await;
    assert_eq!(
        fixture.execute(Kind::Reload).await.unwrap(),
        json!({"success": true})
    );
    assert_eq!(
        fixture.execute(Kind::Reload).await.unwrap(),
        json!({"success": true})
    );
    let state = fixture.state.lock().unwrap();
    assert_eq!(state.lookups, 2);
    assert_eq!(state.reloads, 2);
    assert_eq!(state.executions, 1);
    assert_eq!(state.owner.record.as_ref().unwrap().revision, 4);
}

#[tokio::test]
async fn delete_still_uses_cached_owner_when_coordinator_is_unavailable() {
    let fixture = Fixture::new().await;
    fixture.state.lock().unwrap().lookup_unavailable = true;
    assert_eq!(
        fixture.execute(Kind::Delete).await.unwrap(),
        serde_json::Value::Null
    );
    assert_eq!(fixture.state.lock().unwrap().lookups, 0);
    // The confirmed result also updates the local cache.
    let caller = pb::CallerContext {
        tenant_id: "tenant".into(),
        administrator: false,
    };
    let owner = fixture
        .clients
        .owner("environment", &caller, false)
        .await
        .unwrap();
    assert_eq!(
        owner.record.unwrap().state,
        pb::EnvironmentState::Deleted as i32
    );
}

#[tokio::test]
async fn reload_does_not_execute_from_stale_cache_when_refresh_fails() {
    let fixture = Fixture::new().await;
    fixture.state.lock().unwrap().lookup_unavailable = true;
    assert_eq!(
        fixture.execute(Kind::Reload).await.unwrap_err().code(),
        tonic::Code::Unavailable
    );
    let state = fixture.state.lock().unwrap();
    assert_eq!(state.lookups, 1);
    assert_eq!(state.reloads, 0);
}
