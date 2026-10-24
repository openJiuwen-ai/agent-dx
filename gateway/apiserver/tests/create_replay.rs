//! Replay capacity is a cache budget, never a create admission limit.
use adx_apiserver::{clients::Clients, config::Config, environment_service::EnvironmentService};
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
    entry_node: Mutex<Option<pb::NodeEndpoint>>,
    owners: Mutex<HashMap<String, pb::GetEnvironmentResponse>>,
    starts: AtomicUsize,
    create_calls: AtomicUsize,
    lookups: AtomicUsize,
    response_error: AtomicUsize,
    scheduling_expired: AtomicBool,
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
    type WatchNodesStream = std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<pb::NodeDirectory, Status>> + Send>>;
    async fn watch_nodes(&self, _: Request<pb::WatchNodesRequest>) -> Result<Response<Self::WatchNodesStream>, Status> {
        let node = self.0.entry_node.lock().unwrap().clone().ok_or_else(|| Status::unimplemented("central create fixture"))?;
        Ok(Response::new(Box::pin(futures_util::stream::once(async move {
            Ok(pb::NodeDirectory { epoch: 1, valid_for_millis: 5000, nodes: vec![node] })
        }).chain(futures_util::stream::pending()))))
    }
    async fn get_environment(&self, request: Request<pb::GetEnvironmentRequest>) -> Result<Response<pb::GetEnvironmentResponse>, Status> {
        self.0.lookups.fetch_add(1, Ordering::SeqCst);
        let owner = self.0.owners.lock().unwrap().get(&request.get_ref().environment_id).cloned()
            .ok_or_else(|| Status::not_found("environment not found"))?;
        Ok(Response::new(owner))
    }
    async fn create_environment(&self, request: Request<pb::CreateEnvironmentRequest>) -> Result<Response<pb::EnvironmentResult>, Status> {
        let _trace = adx_observability::trace::Trace::rpc("test.coordinator.create_environment", &request);
        let deadline = request.get_ref().create_deadline_unix_millis;
        self.0.deadlines.lock().unwrap().push(request.get_ref().create_deadline_unix_millis);
        self.0.create_calls.fetch_add(1, Ordering::SeqCst);
        let spec = request.into_inner().spec.unwrap();
        if self.0.scheduling_expired.load(Ordering::SeqCst) {
            return Err(adx_protocol::scheduling_deadline(adx_core::scheduling::SchedulingFailure {
                rejections: [("insufficient_cpu".into(), 1)].into(),
                ..Default::default()
            }));
        }
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
                    execution_artifacts: vec![],
            snapshot_source: None,
                    failure: None,
assignment: Some(pb::Assignment { environment_id: spec.id.clone(), node_id: self.0.entry_node.lock().unwrap().as_ref().map_or_else(|| "node".into(), |node| node.node_id.clone()), generation: 1, ..Default::default() }),
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
}, [
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
        Self::with_mode(retention, false).await
    }
    async fn with_mode(retention: u64, local_first: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let state = Arc::new(State::default());
        if local_first {
            *state.entry_node.lock().unwrap() = Some(pb::NodeEndpoint {
                node_id: "arbitrary-entry".into(),
                address: address.clone(),
                relay_address: address.clone(),
                session_id: "boot".into(),
                accepting_allocations: true,
                ..Default::default()
            });
        }
        let endpoint = Service(state.clone());
        let server = tokio::spawn(async move {
            let incoming = futures_util::stream::unfold(listener, |listener| async {
                let socket = listener.accept().await.map(|(socket, _)| socket);
                Some((socket, listener))
            });
            tonic::transport::Server::builder()
                .add_service(pb::auth_service_server::AuthServiceServer::new(endpoint.clone()))
                .add_service(pb::node_service_server::NodeServiceServer::new(endpoint.clone()))
                .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(endpoint.clone()))
                .add_service(pb::environment_directory_service_server::EnvironmentDirectoryServiceServer::new(endpoint))
                .serve_with_incoming(incoming).await.unwrap();
        });
        let config: Config = serde_json::from_value(json!({
            "listen":"127.0.0.1:0", "coordinator_address":address, "internal_security":"network", "ingress_mode":"standalone",
            "rpc_timeout_seconds":2, "cache_entries":1, "auth_cache_ttl_seconds":1,
            "create_mode":if local_first { "local_first" } else { "central" },
            "create_unknown_retention_seconds":retention
        })).unwrap();
        let clients = Clients::new(config).unwrap();
        let service = EnvironmentService::new(clients.clone());
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

#[tokio::test]
async fn snapshot_create_uses_coordinator_even_with_a_live_local_first_entry() {
    let fixture = Fixture::with_mode(600, true).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while fixture.clients.nodes().await.is_err() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        fixture.clients.nodes().await.unwrap()[0].node_id,
        "arbitrary-entry"
    );
    let mut spec = spec("snapshot-clone");
    spec.snapshot_id = Some("snapshot".into());
    let request = pb::CreateEnvironmentRequest {
        spec: Some(spec),
        caller: Some(caller()),
        ..Default::default()
    };
    let (first, replay) = tokio::join!(
        fixture
            .clients
            .create_environment(request.clone(), Duration::from_secs(2)),
        fixture
            .clients
            .create_environment(request, Duration::from_secs(2)),
    );
    assert_eq!(first.unwrap(), replay.unwrap());
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 1);
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
async fn cancelled_schedule_is_cached_without_resubmission_or_runtime_start() {
    let fixture = Fixture::new().await;
    fixture
        .state
        .scheduling_expired
        .store(true, Ordering::SeqCst);
    let error = fixture
        .create("too-large", "create-too-large")
        .await
        .unwrap_err();
    assert!(adx_protocol::scheduling_failure(&error).is_some());
    let replay = fixture
        .create("too-large", "create-too-large")
        .await
        .unwrap_err();
    assert_eq!(replay.details(), error.details());
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 0);
    assert!(fixture.state.owners.lock().unwrap().is_empty());
    // A deliberate new logical request may wait for placement again.
    let error = fixture
        .create("too-large", "create-new-budget")
        .await
        .unwrap_err();
    assert!(adx_protocol::scheduling_failure(&error).is_some());
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn creation_metrics_wait_for_result_and_count_replay_as_a_response() {
    let fixture = Fixture::new().await;
    fixture.state.block_response.store(true, Ordering::SeqCst);
    let service = fixture.service.clone();
    let pending = tokio::spawn(async move {
        service
            .create(
                spec("observed"),
                json!({"name":"observed"}),
                "observed",
                &caller(),
            )
            .await
    });
    fixture.wait_starts(1).await;
    let metrics = fixture.service.metrics();
    assert!(metrics.contains("adx_environment_create_in_flight 1\n"));
    assert!(metrics.contains("adx_environment_create_responses_total{outcome=\"success\"} 0\n"));
    fixture.state.release.notify_one();
    pending.await.unwrap().unwrap();
    fixture.create("observed", "observed").await.unwrap();
    let metrics = fixture.service.metrics();
    assert!(metrics.contains("adx_environment_create_in_flight 0\n"));
    assert!(metrics.contains("adx_environment_create_responses_total{outcome=\"success\"} 2\n"));
    assert!(!metrics.contains("environment_id="));
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn private_metrics_include_rejected_http_arrivals_without_reporting_success() {
    adx_transport::install_crypto_provider();
    let fixture = Fixture::new().await;
    let api = adx_apiserver::http::Api::new(fixture.clients.clone()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let api_service = api.clone();
    let http = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let api = api_service.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |request| api.clone().serve(request));
                hyper::server::conn::http1::Builder::new()
                    .serve_connection(hyper_util::rt::TokioIo::new(socket), service)
                    .await
                    .unwrap();
            });
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let monitoring = listener.local_addr().unwrap();
    let metrics = tokio::spawn(adx_apiserver::metrics::serve(
        listener,
        api.environment_service.clone(),
    ));
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    for path in [
        "/api/environment/v1/environments",
        "/api/environment/create/",
        "/api/%65nvironment/v1/environments",
    ] {
        let response = client
            .post(format!("http://{address}{path}"))
            .body("{}")
            .send()
            .await
            .unwrap();
        assert!(!response.status().is_success());
    }
    let text = client
        .get(format!("http://{monitoring}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(text.contains("adx_environment_create_requests_total{source=\"http\"} 3\n"));
    assert!(text.contains("adx_environment_create_responses_total{outcome=\"success\"} 0\n"));
    assert!(text.contains("adx_environment_create_in_flight 0\n"));
    assert_eq!(
        client
            .post(format!("http://{monitoring}/metrics"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 0);
    http.abort();
    metrics.abort();
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

#[tonic::async_trait]
impl pb::auth_service_server::AuthService for Service {
    async fn verify_api_key(
        &self,
        _: Request<pb::VerifyApiKeyRequest>,
    ) -> Result<Response<pb::VerifyApiKeyResponse>, Status> {
        Ok(Response::new(pb::VerifyApiKeyResponse {
            caller: Some(caller()),
            expires_at_unix_seconds: 0,
        }))
    }
}
service_impl!(pb::node_service_server::NodeService, {
    async fn create_local_environment(&self, request: Request<pb::LocalEnvironmentCreateRequest>) -> Result<Response<pb::EnvironmentResult>, Status> {
        let trace = adx_observability::trace::Trace::rpc("test.node.create_local_environment", &request);
        trace.run_result(async {
            let create = request.into_inner().create.unwrap();
            pb::coordinator_service_server::CoordinatorService::create_environment(self, adx_observability::trace::inject(create)).await
        }).await
    }
}, [
    recover_environment(RecoverEnvironmentRequest) -> EnvironmentResult,
    get_session(GetNodeSessionRequest) -> GetNodeSessionResponse,
    create_environment(StartAssignedEnvironmentRequest) -> EnvironmentResult,
    delete_environment(DeleteEnvironmentRequest) -> EnvironmentResult,
    pause_environment(PauseEnvironmentRequest) -> EnvironmentResult,
    resume_environment(ResumeEnvironmentRequest) -> EnvironmentResult,
    ensure_environment_running(EnsureEnvironmentRunningRequest) -> EnvironmentResult,
    update_network_policy(UpdateNetworkPolicyRequest) -> EnvironmentResult,
    reload_environment(ReloadEnvironmentRequest) -> EnvironmentResult,
    create_snapshot(CreateSnapshotRequest) -> CreateSnapshotResponse,
    collect_snapshot(CollectSnapshotRequest) -> CollectSnapshotResponse,
]);

static TRACE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn traced_create(
    exporter: &opentelemetry_sdk::trace::InMemorySpanExporter,
    mode: &str,
    failure: bool,
    local: bool,
) -> (Vec<opentelemetry_sdk::trace::SpanData>, String) {
    adx_transport::install_crypto_provider();
    let fixture = Fixture::with_mode(600, local).await;
    if local {
        tokio::time::timeout(Duration::from_secs(2), async {
            while fixture.clients.nodes().await.is_err() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    fixture.state.block_response.store(true, Ordering::SeqCst);
    if failure {
        fixture
            .state
            .response_error
            .store(tonic::Code::InvalidArgument as usize, Ordering::SeqCst);
    }
    let api = adx_apiserver::http::Api::new(fixture.clients.clone()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let serving = api.clone();
    let http = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let api = serving.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |request| api.clone().serve(request));
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(hyper_util::rt::TokioIo::new(socket), service)
                    .await;
            });
        }
    });
    let before = exporter.get_finished_spans().unwrap().len();
    let input = json!({"name":"trace-create", "image":"image", "runtimeClass":"runsc"});
    let embedded = mode == "embedded";
    let service = api.environment_service.clone();
    let mode = mode.to_owned();
    let request = tokio::spawn(async move {
        if embedded {
            let result = adx_observability::trace::Trace::child("test.embedded_create")
                .run(service.create_request(input, "trace-create", &caller()))
                .await;
            assert_eq!(result.is_err(), failure);
        } else {
            let client = reqwest::Client::builder().no_proxy().build().unwrap();
            let response = client
                .post(format!("http://{address}/api/environment/v1/environments"))
                .header(
                    "authorization",
                    format!("Bearer {}", "trace-test".repeat(4)),
                )
                .header(
                    "accept",
                    if mode == "sse" {
                        "text/event-stream"
                    } else {
                        "application/json"
                    },
                )
                .body(input.to_string())
                .send()
                .await
                .unwrap();
            let text = response.text().await.unwrap();
            if mode == "sse" {
                assert!(text.contains("event: final"), "{text}");
            }
            assert_eq!(text.contains("error"), failure, "{text}");
        }
    });
    fixture.wait_starts(1).await;
    let in_progress = exporter.get_finished_spans().unwrap();
    assert!(
        !in_progress[before..]
            .iter()
            .any(|span| span.name == "environment.create"
                && span
                    .attributes
                    .iter()
                    .any(|a| a.key.as_str() == "environment.id"
                        && a.value.to_string() == "default-trace-create")),
        "create must not finish before confirmation"
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
    fixture.state.release.notify_one();
    request.await.unwrap();
    tokio::task::yield_now().await;
    let completed = exporter.get_finished_spans().unwrap();
    let total = completed[before..]
        .iter()
        .find(|span| {
            span.name == "environment.create"
                && span.attributes.iter().any(|a| {
                    a.key.as_str() == "environment.id"
                        && a.value.to_string() == "default-trace-create"
                })
        })
        .expect("total create span missing");
    let result = completed[before..]
        .iter()
        .filter(|span| span.span_context.trace_id() == total.span_context.trace_id())
        .cloned()
        .collect();
    let metrics = api.environment_service.metrics();
    http.abort();
    (result, metrics)
}

#[tokio::test]
async fn create_total_span_matches_metric_boundary_for_json_sse_and_embedded() {
    let _serial = TRACE_TEST_LOCK.lock().await;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_tracer_provider(provider.clone());
    for mode in ["json", "sse", "embedded"] {
        let (spans, metrics) = traced_create(&exporter, mode, false, false).await;
        let creates: Vec<_> = spans
            .iter()
            .filter(|span| span.name == "environment.create")
            .collect();
        assert_eq!(
            creates.len(),
            1,
            "{mode}: exactly one total create span required"
        );
        let create = creates[0];
        assert!(create
            .attributes
            .iter()
            .any(|a| a.key.as_str() == "environment.id"
                && a.value.to_string() == "default-trace-create"));
        let elapsed = create
            .end_time
            .duration_since(create.start_time)
            .unwrap()
            .as_secs_f64();
        assert!(elapsed >= 0.03, "{mode}: confirmation wait missing");
        let measured: f64 = metrics
            .lines()
            .find_map(|line| line.strip_prefix("adx_environment_create_duration_seconds_sum "))
            .unwrap()
            .parse()
            .unwrap();
        assert!(
            (elapsed - measured).abs() < 0.02,
            "{mode}: span={elapsed}, metric={measured}"
        );
        let parent = spans
            .iter()
            .find(|s| s.span_context.span_id() == create.parent_span_id)
            .unwrap();
        assert_eq!(
            parent.span_context.trace_id(),
            create.span_context.trace_id()
        );
        assert_eq!(
            parent.name,
            if mode == "embedded" {
                "test.embedded_create"
            } else {
                "POST /api/environment/v1/environments"
            }
        );
        if mode == "sse" {
            assert!(parent.end_time < create.end_time);
        }
    }
}

#[tokio::test]
async fn create_rpc_server_spans_are_children_of_the_matching_client_span() {
    let _serial = TRACE_TEST_LOCK.lock().await;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_tracer_provider(provider.clone());
    for local in [false, true] {
        let (spans, _) = traced_create(&exporter, "sse", false, local).await;
        let (client, server) = if local {
            (
                "apiserver.create_local",
                "test.node.create_local_environment",
            )
        } else {
            ("apiserver.create", "test.coordinator.create_environment")
        };
        let client = spans.iter().find(|s| s.name == client).unwrap();
        let server = spans.iter().find(|s| s.name == server).unwrap();
        assert_eq!(server.parent_span_id, client.span_context.span_id());
        assert_eq!(
            server.span_context.trace_id(),
            client.span_context.trace_id()
        );
        assert!(client.start_time <= server.start_time && client.end_time >= server.end_time);
    }
}

#[tokio::test]
async fn create_total_span_records_errors_for_json_sse_and_embedded() {
    let _serial = TRACE_TEST_LOCK.lock().await;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_tracer_provider(provider.clone());
    for mode in ["json", "sse", "embedded"] {
        let (spans, _) = traced_create(&exporter, mode, true, false).await;
        let create = spans
            .iter()
            .find(|span| span.name == "environment.create")
            .expect("total create span missing");
        assert!(
            matches!(create.status, opentelemetry::trace::Status::Error { .. }),
            "{mode}: {:?}",
            create.status
        );
    }
}

#[tokio::test]
async fn cancelled_create_ends_total_span_without_losing_its_parent() {
    let _serial = TRACE_TEST_LOCK.lock().await;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_tracer_provider(provider.clone());
    let fixture = Fixture::new().await;
    fixture.state.block_response.store(true, Ordering::SeqCst);
    let service = fixture.service.clone();
    let request = tokio::spawn(
        adx_observability::trace::Trace::child("test.cancelled_create").run(async move {
            service
                .create_request(
                    json!({"name":"cancelled-create","image":"image"}),
                    "cancelled-create",
                    &caller(),
                )
                .await
        }),
    );
    fixture.wait_starts(1).await;
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    let spans = exporter.get_finished_spans().unwrap();
    let parent = spans
        .iter()
        .find(|s| s.name == "test.cancelled_create")
        .unwrap();
    let create = spans
        .iter()
        .find(|s| {
            s.name == "environment.create"
                && s.parent_span_id == parent.span_context.span_id()
                && s.span_context.trace_id() == parent.span_context.trace_id()
        })
        .unwrap();
    assert_eq!(create.parent_span_id, parent.span_context.span_id());
    assert!(create
        .attributes
        .iter()
        .any(|a| a.key.as_str() == "environment.create.outcome"
            && a.value.to_string() == "cancelled"));
    let metrics = fixture.service.metrics();
    assert!(metrics.contains("adx_environment_create_in_flight 0\n"));
    assert!(metrics.contains("adx_environment_create_responses_total{outcome=\"cancelled\"} 1\n"));
    fixture.state.release.notify_one();
}
