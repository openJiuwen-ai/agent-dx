//! Replay capacity is a cache budget, never a create admission limit.
use adx_apiserver::{
    clients::Clients, config::Config, environment_service::EnvironmentService, http::Api,
    operations::Kind,
};
use adx_protocol::control as pb;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Notify,
};
use tonic::{Request, Response, Status};

#[derive(Default)]
struct State {
    owners: Mutex<HashMap<String, pb::GetEnvironmentResponse>>,
    starts: AtomicUsize,
    create_calls: AtomicUsize,
    lookups: AtomicUsize,
    response_error: AtomicUsize,
    block_response: AtomicBool,
    pending_response: AtomicBool,
    deadlines: Mutex<Vec<u64>>,
    timeout_phase: Mutex<Option<String>>,
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
        let deadline = request.get_ref().create_deadline_unix_millis;
        self.0.deadlines.lock().unwrap().push(request.get_ref().create_deadline_unix_millis);
        self.0.create_calls.fetch_add(1, Ordering::SeqCst);
        let spec = request.into_inner().spec.unwrap();
        // The coordinator validates and canonicalizes E2B Environment specs
        // through the core model before publishing them.
        let spec = if spec.compatibility.is_some() {
            adx_core::EnvironmentSpec::try_from(spec).unwrap().into()
        } else {
            spec
        };
        let record = {
            let mut owners = self.0.owners.lock().unwrap();
            if let Some(owner) = owners.get_mut(&spec.id) {
                assert_eq!(owner.record.as_ref().unwrap().spec.as_ref(), Some(&spec));
                if !self.0.pending_response.load(Ordering::SeqCst) {
                    let record = owner.record.as_mut().unwrap();
                    if record.state == pb::EnvironmentState::Pending as i32 {
                        record.state = pb::EnvironmentState::Running as i32;
                        record.revision = 1;
                    }
                }
                owner.record.clone().unwrap()
            } else {
                self.0.starts.fetch_add(1, Ordering::SeqCst);
                let record = pb::EnvironmentRecord {
                    failure: None,
assignment: Some(pb::Assignment { environment_id: spec.id.clone(), node_id: "node".into(), generation: 1, ..Default::default() }),
                    spec: Some(spec.clone()), state: if self.0.pending_response.load(Ordering::SeqCst) { pb::EnvironmentState::Pending as i32 } else { pb::EnvironmentState::Running as i32 }, revision: 1,
                    resources_held: true, ..Default::default()
                };
                owners.insert(spec.id, pb::GetEnvironmentResponse { record: Some(record.clone()), node_address: "127.0.0.1:9000".into(), relay_address: "127.0.0.1:9443".into() });
                record
            }
        };
        if self.0.block_response.load(Ordering::SeqCst) {
            self.0.release.notified().await;
        }
        let phase = self.0.timeout_phase.lock().unwrap().clone();
        if let Some(phase) = phase {
            let deadline = adx_transport::deadline::Deadline::from_unix_millis(deadline).unwrap();
            tokio::time::sleep_until(deadline.instant()).await;
            // The actual failed result reaches the caller after the business
            // deadline, rather than being masked by a transport timer.
            tokio::time::sleep(Duration::from_millis(20)).await;
            let message = format!("create deadline exceeded during {phase}");
            let mut failed = record.clone();
            failed.state = pb::EnvironmentState::Failed as i32;
            failed.resources_held = false;
            failed.failure = Some(Box::new(adx_core::RuntimeFailure {
                reason: adx_core::RuntimeFailureReason::DeadlineExceeded,
                message: message.clone(), exit_code: None, probe_failure: None,
            }.into()));
            let id = failed.spec.as_ref().unwrap().id.clone();
            self.0.owners.lock().unwrap().get_mut(&id).unwrap().record = Some(failed);
            return Err(adx_protocol::status(adx_core::Error::DeadlineExceeded(message)));
        }
        let error = tonic::Code::from_i32(self.0.response_error.load(Ordering::SeqCst) as i32);
        if error != tonic::Code::Ok {
            return Err(Status::new(error, "injected missing create response"));
        }
        Ok(Response::new(pb::EnvironmentResult { record: Some(record), durability: pb::Durability::Published as i32 }))
    }
    async fn get_environment_control(&self, request: Request<pb::GetEnvironmentControlRequest>) -> Result<Response<pb::EnvironmentControlRecord>, Status> {
        let id = request.into_inner().id;
        let owner = self.0.owners.lock().unwrap().get(&id).cloned()
            .ok_or_else(|| Status::not_found("environment not found"))?;
        let compatibility = owner.record
            .and_then(|record| record.spec)
            .and_then(|spec| spec.compatibility)
            .ok_or_else(|| Status::not_found("sandbox control not found"))?;
        Ok(Response::new(pb::EnvironmentControlRecord {
            environment_id: id,
            compatibility: Some(compatibility),
            expires_at_unix_ms: 1,
            started_at_unix_ms: 1,
            revision: 1,
            generation: 1,
            expiry_claimed: false,
        }))
    }
}, [
    update_environment_control(UpdateEnvironmentControlRequest) -> EnvironmentControlRecord,
    list_environment_controls(ListEnvironmentControlsRequest) -> ListEnvironmentControlsResponse,
    get_scheduling_queue(GetSchedulingQueueRequest) -> GetSchedulingQueueResponse,
    set_node_scheduling(SetNodeSchedulingRequest) -> NodeSchedulingState,
    prepare_create(LocalEnvironmentCreateRequest) -> PreparedEnvironment,
    claim_environment(ClaimEnvironmentRequest) -> ClaimEnvironmentResponse,
    forward_create(LocalEnvironmentCreateRequest) -> EnvironmentResult,
    register_node(RegisterNodeRequest) -> RegisterNodeResponse,
    inspect_node(InspectNodeRequest) -> InspectNodeResponse,
    commit_environment(CommitEnvironmentRequest) -> CommitEnvironmentResponse,
    ensure_running(EnsureRunningRequest) -> GetEnvironmentResponse,
    retire_dormant(GetEnvironmentRequest) -> EnvironmentResult,
]);

#[tonic::async_trait]
impl pb::auth_service_server::AuthService for Service {
    async fn verify_api_key(
        &self,
        request: Request<pb::VerifyApiKeyRequest>,
    ) -> Result<Response<pb::VerifyApiKeyResponse>, Status> {
        if request.into_inner().api_key.len() < 32 {
            return Err(Status::unauthenticated("invalid API key"));
        }
        Ok(Response::new(pb::VerifyApiKeyResponse {
            caller: Some(caller()),
            expires_at_unix_seconds: 0,
        }))
    }
}
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
    service: Arc<EnvironmentService>,
    clients: Arc<Clients>,
    api: Arc<Api>,
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
        adx_transport::install_crypto_provider();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let state = Arc::new(State::default());
        let endpoint = Service(state.clone());
        let auth = endpoint.clone();
        let server = tokio::spawn(async move {
            let incoming = futures_util::stream::unfold(listener, |listener| async {
                let socket = listener.accept().await.map(|(socket, _)| socket);
                Some((socket, listener))
            });
            tonic::transport::Server::builder()
                .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(endpoint.clone()))
                .add_service(pb::environment_directory_service_server::EnvironmentDirectoryServiceServer::new(endpoint))
                .add_service(pb::auth_service_server::AuthServiceServer::new(auth))
                .serve_with_incoming(incoming).await.unwrap();
        });
        let config: Config = serde_json::from_value(json!({
            "listen":"127.0.0.1:0", "coordinator_address":address, "internal_security":"network", "ingress_mode":"standalone",
            "rpc_timeout_seconds":2, "cache_entries":1, "auth_cache_ttl_seconds":1,
            "create_unknown_retention_seconds":retention,
            "e2b": {
                "domain":"sandbox.test",
                "envdVersion":"0.9.0",
                "runtimeProfiles": {
                    "e2b": {
                        "rootfs":{"runtime_class":"runsc","type":"local","path":"/rootfs","readonly":true},
                        "bootstrap":{"type":"erofs","root":"/rootfs","target":"/rootfs","entrypoint":["/rootfs/init"]},
                        "env":{"ADX_ENVD_PATH":"/usr/local/bin/envd","ADX_ENVD_SHA256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
                    }
                },
                "templates":{"templates":{"base":{
                    "runtimeProfileRef":"e2b","defaultUser":"user","defaultWorkdir":"/workspace",
                    "cpuMillicores":1000,"memoryMiB":512,"capabilities":["envd"]
                }}}
            }
        })).unwrap();
        let clients = Clients::new(config).unwrap();
        let service = EnvironmentService::new(clients.clone());
        let api = Api::new(clients.clone()).unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while service.inspect("tenant", "not-created").await.is_err() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        Self {
            service,
            clients,
            api,
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
        compatibility: None,
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
async fn confirmed_timeout_survives_rpc_timers_and_replay() {
    let phase = "backend preparation/runtime start";
    let fixture = Fixture::new().await;
    *fixture.state.timeout_phase.lock().unwrap() = Some(phase.into());
    let input = json!({"name":"phase-timeout", "createTimeoutSeconds":35, "scheduleTimeoutSeconds":1, "initCallTimeoutSeconds":1});
    let start = tokio::time::Instant::now();
    let error = Box::pin(fixture.service.create(
        spec("phase-timeout"),
        input.clone(),
        "create-phase",
        &caller(),
    ))
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::DeadlineExceeded, "{error:?}");
    assert!(error.message().contains(phase), "{error:?}");
    assert!(adx_protocol::deadline_failure(&error).is_some());
    assert!(start.elapsed() >= Duration::from_secs(35));
    assert!(start.elapsed() < Duration::from_secs(38));
    let replay = Box::pin(fixture.service.create(
        spec("phase-timeout"),
        input.clone(),
        "create-phase",
        &caller(),
    ))
    .await
    .unwrap_err();
    assert_eq!(replay.message(), error.message());
    // A different request ID must recover the same persisted failure too.
    let published = fixture.state.owners.lock().unwrap()["phase-timeout"].clone();
    fixture.clients.put_owner(published).await.unwrap();
    let replay = Box::pin(fixture.service.create(
        spec("phase-timeout"),
        input,
        "create-other",
        &caller(),
    ))
    .await
    .unwrap_err();
    assert_eq!(replay.message(), error.message());
    assert!(adx_protocol::deadline_failure(&replay).is_some());
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), 1);
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
        assert_eq!(result["environmentId"], id);
    }
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 12);
    // The first response was evicted: current ownership still converges to it.
    assert_eq!(
        fixture.create("environment-0", "request-0").await.unwrap()["environmentId"],
        "environment-0"
    );
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 12);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn e2b_create_through_http_listener_completes_on_default_worker_stack() {
    let fixture = Fixture::new().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let api = fixture.api.clone();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        hyper::server::conn::http1::Builder::new()
            .serve_connection(
                TokioIo::new(socket),
                service_fn(move |request| api.clone().serve(request)),
            )
            .await
            .unwrap();
    });
    let request = br#"{"templateID":"base"}"#;
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let request = format!(
        "POST /v2/sandboxes HTTP/1.1\r\nHost: localhost\r\nX-API-Key: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        "k".repeat(32),
        request.len(),
        std::str::from_utf8(request).unwrap(),
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    server.await.unwrap();
    assert!(
        response.starts_with(b"HTTP/1.1 201"),
        "{}",
        String::from_utf8_lossy(&response)
    );
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
            Box::pin(service.create(spec(id), json!({"name":id}), request_id, &caller())).await
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
    assert_eq!(second.await.unwrap().unwrap()["environmentId"], "two");
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
        fixture.create("new", "request").await.unwrap()["environmentId"],
        "new"
    );
    let starts = fixture.state.starts.load(Ordering::SeqCst);
    assert_eq!(
        fixture.create("old", "retry-old").await.unwrap()["environmentId"],
        "old"
    );
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), starts);
}

#[tokio::test]
async fn pending_owner_retry_drives_the_original_assignment() {
    let fixture = Fixture::new().await;
    fixture.state.pending_response.store(true, Ordering::SeqCst);
    fixture
        .state
        .response_error
        .store(tonic::Code::Unavailable as usize, Ordering::SeqCst);
    assert_eq!(
        fixture
            .create("pending", "request")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    let assignment = fixture.state.owners.lock().unwrap()["pending"]
        .record
        .as_ref()
        .unwrap()
        .assignment
        .clone();
    fixture
        .state
        .pending_response
        .store(false, Ordering::SeqCst);
    fixture.state.response_error.store(0, Ordering::SeqCst);
    assert_eq!(
        fixture.create("pending", "request").await.unwrap()["environmentId"],
        "pending"
    );
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), 2);
    let deadlines = fixture.state.deadlines.lock().unwrap();
    assert!(deadlines[0] > 0);
    assert_eq!(deadlines[0], deadlines[1]);
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.state.owners.lock().unwrap()["pending"]
            .record
            .as_ref()
            .unwrap()
            .assignment,
        assignment
    );
}

#[tokio::test]
async fn failed_owner_retry_is_terminal_and_does_not_repeat_start() {
    let fixture = Fixture::new().await;
    fixture
        .state
        .response_error
        .store(tonic::Code::Unavailable as usize, Ordering::SeqCst);
    assert!(fixture.create("failed", "request").await.is_err());
    {
        let mut owners = fixture.state.owners.lock().unwrap();
        let record = owners.get_mut("failed").unwrap().record.as_mut().unwrap();
        record.state = pb::EnvironmentState::Failed as i32;
        record.resources_held = false;
    }
    fixture.state.response_error.store(0, Ordering::SeqCst);
    assert_eq!(
        fixture
            .create("failed", "request")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn pending_retry_rejects_a_changed_generation() {
    let fixture = Fixture::new().await;
    fixture.state.pending_response.store(true, Ordering::SeqCst);
    fixture
        .state
        .response_error
        .store(tonic::Code::Unavailable as usize, Ordering::SeqCst);
    assert!(fixture.create("pending-fenced", "request").await.is_err());
    // First retry observes and binds the Pending assignment, then loses its response.
    assert!(fixture.create("pending-fenced", "request").await.is_err());
    fixture
        .state
        .owners
        .lock()
        .unwrap()
        .get_mut("pending-fenced")
        .unwrap()
        .record
        .as_mut()
        .unwrap()
        .assignment
        .as_mut()
        .unwrap()
        .generation = 2;
    assert_eq!(
        fixture
            .create("pending-fenced", "request")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn revision_fenced_operation_refreshes_and_rejects_a_stale_merge_before_node_rpc() {
    let fixture = Fixture::new().await;
    fixture.create("revision", "create-revision").await.unwrap();
    {
        let mut owners = fixture.state.owners.lock().unwrap();
        owners
            .get_mut("revision")
            .unwrap()
            .record
            .as_mut()
            .unwrap()
            .revision = 2;
    }
    let lookups = fixture.state.lookups.load(Ordering::SeqCst);
    let stale = fixture
        .service
        .execute_at_revision(
            Kind::Network,
            "revision",
            "stale-network",
            network_payload(),
            &caller(),
            1,
        )
        .await
        .unwrap_err();
    assert_eq!(stale.code(), tonic::Code::Aborted);
    assert_eq!(fixture.state.lookups.load(Ordering::SeqCst), lookups + 1);

    let current = fixture
        .service
        .execute_at_revision(
            Kind::Network,
            "revision",
            "current-network",
            network_payload(),
            &caller(),
            2,
        )
        .await
        .unwrap_err();
    assert_ne!(current.code(), tonic::Code::Aborted);
}

fn network_payload() -> Value {
    json!({
        "schemaVersion": 2,
        "traffic": {
            "ingressDefaultAction": "allow",
            "egressDefaultAction": "allow",
            "rules": [],
            "mode": "stateful"
        }
    })
}
