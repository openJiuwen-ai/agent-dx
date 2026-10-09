//! Replay capacity is a cache budget, never a create admission limit.
use adx_apiserver::{clients::Clients, config::Config, sandbox_service::SandboxService};
use adx_protocol::control as pb;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::Notify;
use tonic::{Request, Response, Status};

#[derive(Default)]
struct State {
    owners: Mutex<HashMap<String, pb::GetEnvironmentResponse>>,
    starts: AtomicUsize,
    create_calls: AtomicUsize,
    lookups: AtomicUsize,
    response_error: AtomicUsize,
    block_response: AtomicBool,
    release: Notify,
}
#[derive(Clone)]
struct Service(Arc<State>);
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
        Err(Status::unimplemented("central create fixture"))
    }
    async fn get_environment(&self, request: Request<pb::GetEnvironmentRequest>) -> Result<Response<pb::GetEnvironmentResponse>, Status> {
        self.0.lookups.fetch_add(1, Ordering::SeqCst);
        let owner = self.0.owners.lock().unwrap().get(&request.get_ref().environment_id).cloned()
            .ok_or_else(|| Status::not_found("environment not found"))?;
        Ok(Response::new(owner))
    }
    async fn create_environment(&self, request: Request<pb::CreateEnvironmentRequest>) -> Result<Response<pb::EnvironmentResult>, Status> {
        self.0.create_calls.fetch_add(1, Ordering::SeqCst);
        let spec = request.into_inner().spec.unwrap();
        let record = {
            let mut owners = self.0.owners.lock().unwrap();
            if let Some(owner) = owners.get(&spec.id) {
                assert_eq!(owner.record.as_ref().unwrap().spec.as_ref(), Some(&spec));
                owner.record.clone().unwrap()
            } else {
                self.0.starts.fetch_add(1, Ordering::SeqCst);
                let record = pb::EnvironmentRecord {
                    assignment: Some(pb::Assignment { environment_id: spec.id.clone(), node_id: "node".into(), generation: 1, ..Default::default() }),
                    spec: Some(spec.clone()), state: pb::EnvironmentState::Running as i32, revision: 1,
                    resources_held: true, ..Default::default()
                };
                owners.insert(spec.id, pb::GetEnvironmentResponse { record: Some(record.clone()), node_address: "127.0.0.1:9000".into(), relay_address: "127.0.0.1:9443".into() });
                record
            }
        };
        if self.0.block_response.load(Ordering::SeqCst) {
            self.0.release.notified().await;
        }
        let error = tonic::Code::from_i32(self.0.response_error.load(Ordering::SeqCst) as i32);
        if error != tonic::Code::Ok {
            return Err(Status::new(error, "injected missing create response"));
        }
        Ok(Response::new(pb::EnvironmentResult { record: Some(record), durability: pb::Durability::Published as i32 }))
    }
}, [
    get_scheduling_queue(GetSchedulingQueueRequest) -> GetSchedulingQueueResponse,
    set_node_scheduling(SetNodeSchedulingRequest) -> NodeSchedulingState,
    prepare_create(LocalEnvironmentCreateRequest) -> PreparedEnvironment,
    claim_environment(ClaimEnvironmentRequest) -> ClaimEnvironmentResponse,
    forward_create(LocalEnvironmentCreateRequest) -> EnvironmentResult,
    register_node(RegisterNodeRequest) -> RegisterNodeResponse,
    inspect_node(InspectNodeRequest) -> InspectNodeResponse,
    commit_environment(CommitEnvironmentRequest) -> CommitEnvironmentResponse,
]);
#[tonic::async_trait]
impl pb::environment_directory_service_server::EnvironmentDirectoryService for Service {
    type WatchEnvironmentsStream = std::pin::Pin<
        Box<dyn futures_util::Stream<Item = Result<pb::EnvironmentDirectoryFrame, Status>> + Send>,
    >;
    async fn watch_environments(
        &self,
        _: Request<pb::WatchEnvironmentsRequest>,
    ) -> Result<Response<Self::WatchEnvironmentsStream>, Status> {
        // Withhold all deltas so retries must use the owning RPC result/read,
        // while ordinary new names can start after the initial empty reset.
        let full = pb::EnvironmentDirectoryFrame {
            epoch: 1,
            revision: 1,
            reset: true,
            ..Default::default()
        };
        Ok(Response::new(Box::pin(
            futures_util::stream::once(async { Ok(full) }).chain(futures_util::stream::pending()),
        )))
    }
}
use futures_util::StreamExt;
struct Fixture {
    service: Arc<SandboxService>,
    state: Arc<State>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Fixture {
    async fn new() -> Self {
        Self::with_retention(600).await
    }
    async fn with_retention(retention: u64) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let state = Arc::new(State::default());
        let endpoint = Service(state.clone());
        let server = tokio::spawn(async move {
            let incoming = futures_util::stream::unfold(listener, |listener| async {
                let socket = listener.accept().await.map(|(socket, _)| socket);
                Some((socket, listener))
            });
            tonic::transport::Server::builder()
                .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(endpoint.clone()))
                .add_service(pb::environment_directory_service_server::EnvironmentDirectoryServiceServer::new(endpoint))
                .serve_with_incoming(incoming).await.unwrap();
        });
        let config: Config = serde_json::from_value(json!({
            "listen":"127.0.0.1:0", "coordinator_address":address, "internal_security":"network", "ingress_mode":"standalone",
            "rpc_timeout_seconds":2, "cache_entries":1, "auth_cache_ttl_seconds":1,
            "create_unknown_retention_seconds":retention
        })).unwrap();
        let service = SandboxService::new(Clients::new(config).unwrap());
        tokio::time::timeout(Duration::from_secs(3), async {
            while service.inspect("tenant", "not-created").await.is_err() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        Self {
            service,
            state,
            server,
        }
    }
    async fn create(&self, id: &str, request_id: &str) -> Result<Value, Status> {
        Box::pin(
            self.service
                .create(spec(id), json!({"name":id}), request_id, &caller()),
        )
        .await
    }
    async fn wait_starts(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.state.starts.load(Ordering::SeqCst) < count {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
}
fn spec(id: &str) -> pb::EnvironmentSpec {
    pb::EnvironmentSpec {
        id: id.into(),
        tenant_id: "tenant".into(),
        runtime_class: "runsc".into(),
        ..Default::default()
    }
}
fn caller() -> pb::CallerContext {
    pb::CallerContext {
        tenant_id: "tenant".into(),
        administrator: false,
    }
}
#[tokio::test]
async fn completed_cache_capacity_never_rejects_new_creates() {
    let fixture = Fixture::new().await;
    for index in 0..12 {
        let id = format!("environment-{index}");
        let result = fixture
            .create(&id, &format!("request-{index}"))
            .await
            .unwrap();
        assert_eq!(result["instanceId"], id);
    }
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 12);
    // The first response was evicted: current ownership still converges to it.
    assert_eq!(
        fixture.create("environment-0", "request-0").await.unwrap()["instanceId"],
        "environment-0"
    );
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 12);
}
#[tokio::test]
async fn unknown_results_do_not_block_new_names_and_retry_original_identity() {
    let fixture = Fixture::new().await;
    let unknown_codes = [
        tonic::Code::Unavailable,
        tonic::Code::DeadlineExceeded,
        tonic::Code::Cancelled,
        tonic::Code::Unknown,
        tonic::Code::Internal,
    ];
    for (index, code) in unknown_codes.into_iter().enumerate() {
        fixture
            .state
            .response_error
            .store(code as usize, Ordering::SeqCst);
        let error = fixture
            .create(&format!("unknown-{index}"), &format!("request-{index}"))
            .await
            .unwrap_err();
        assert_eq!(error.code(), code);
    }
    fixture.state.response_error.store(0, Ordering::SeqCst);
    fixture.create("new", "new-request").await.unwrap();
    // All unknown outcomes retain their arguments while unrelated completed
    // responses are evicted from the capacity-one cache.
    for index in 0..unknown_codes.len() {
        assert_eq!(
            fixture
                .create("other-name", &format!("request-{index}"))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::AlreadyExists
        );
    }
    let starts = fixture.state.starts.load(Ordering::SeqCst);
    let calls = fixture.state.create_calls.load(Ordering::SeqCst);
    fixture.create("unknown-0", "request-0").await.unwrap();
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), starts);
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), calls);
}
#[tokio::test]
async fn active_creates_are_not_evicted_and_same_request_is_serialized() {
    let fixture = Fixture::with_retention(1).await;
    fixture.state.block_response.store(true, Ordering::SeqCst);
    let spawn = |id: &'static str, request_id: &'static str| {
        let service = fixture.service.clone();
        tokio::spawn(async move {
            service
                .create(spec(id), json!({"name":id}), request_id, &caller())
                .await
        })
    };
    let first = spawn("one", "request-one");
    fixture.wait_starts(1).await;
    // GC runs during this blocked RPC, but cannot remove its active identity.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let duplicate = spawn("one", "request-one");
    let second = spawn("two", "request-two");
    fixture.wait_starts(2).await;
    fixture.state.block_response.store(false, Ordering::SeqCst);
    fixture.state.release.notify_waiters();
    assert_eq!(
        first.await.unwrap().unwrap(),
        duplicate.await.unwrap().unwrap()
    );
    assert_eq!(second.await.unwrap().unwrap()["instanceId"], "two");
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn expired_unknown_request_is_collected_without_querying_or_changing_environment() {
    let fixture = Fixture::with_retention(1).await;
    fixture
        .state
        .response_error
        .store(tonic::Code::Unavailable as usize, Ordering::SeqCst);
    assert_eq!(
        fixture.create("old", "request").await.unwrap_err().code(),
        tonic::Code::Unavailable
    );
    assert_eq!(
        fixture.create("new", "request").await.unwrap_err().code(),
        tonic::Code::AlreadyExists
    );
    let lookups = fixture.state.lookups.load(Ordering::SeqCst);
    let calls = fixture.state.create_calls.load(Ordering::SeqCst);
    // Allow the first unknown TTL and the following periodic sweep to pass.
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert_eq!(fixture.state.lookups.load(Ordering::SeqCst), lookups);
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), calls);
    assert_eq!(
        fixture.state.owners.lock().unwrap()["old"]
            .record
            .as_ref()
            .unwrap()
            .state,
        pb::EnvironmentState::Running as i32
    );
    fixture.state.response_error.store(0, Ordering::SeqCst);
    // Beyond the retry window, the old Request ID no longer pins its arguments.
    assert_eq!(
        fixture.create("new", "request").await.unwrap()["instanceId"],
        "new"
    );
    let starts = fixture.state.starts.load(Ordering::SeqCst);
    assert_eq!(
        fixture.create("old", "retry-old").await.unwrap()["instanceId"],
        "old"
    );
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), starts);
}
