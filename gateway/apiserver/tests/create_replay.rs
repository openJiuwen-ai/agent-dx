//! Replay capacity is a cache budget, never a create admission limit.
#[path = "support/e2b_create_errors.rs"]
mod e2b_create_errors;
#[path = "support/http.rs"]
mod http;
#[path = "support/templates.rs"]
mod templates;

use adx_activator::{local_environment::LocalEnvironment, Activator};
use adx_agent_api::{
    activator::Control, local::LocalControl, managed::ManagedService, request::RequestContext,
};
use adx_agent_core::network::SandboxNetworkPolicy;
use adx_agent_store::{AgentState, MemoryRepository};
use adx_apiserver::{
    clients::Clients, config::Config, environment_service::EnvironmentService, http::Api,
    operations::Kind,
};
use adx_protocol::control as pb;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap},
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
    entry_node: Mutex<Option<pb::NodeEndpoint>>,
    owners: Mutex<HashMap<String, pb::GetEnvironmentResponse>>,
    metadata: Mutex<HashMap<String, HashMap<String, String>>>,
    starts: AtomicUsize,
    create_calls: AtomicUsize,
    lookups: AtomicUsize,
    response_error: AtomicUsize,
    auth_error: AtomicUsize,
    control_error: AtomicUsize,
    scheduling_expired: AtomicBool,
    block_response: AtomicBool,
    pending_response: AtomicBool,
    deadlines: Mutex<Vec<u64>>,
    timeout_phase: Mutex<Option<String>>,
    snapshot: Mutex<Option<pb::ReusableSnapshot>>,
    snapshot_calls: AtomicUsize,
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
        let deadline = request.get_ref().create_deadline_unix_millis;
        self.0.deadlines.lock().unwrap().push(request.get_ref().create_deadline_unix_millis);
        self.0.create_calls.fetch_add(1, Ordering::SeqCst);
        let request = request.into_inner();
        let user_metadata = request.user_metadata;
        let spec = request.spec.unwrap();
        // The coordinator validates and canonicalizes E2B Environment specs
        // through the core model before publishing them.
        let spec = if spec.compatibility.is_some() {
            adx_core::EnvironmentSpec::try_from(spec).unwrap().into()
        } else {
            spec
        };
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
assignment: Some(pb::Assignment { environment_id: spec.id.clone(), node_id: "node".into(), generation: 1, ..Default::default() }),
                    spec: Some(spec.clone()), state: if self.0.pending_response.load(Ordering::SeqCst) { pb::EnvironmentState::Pending as i32 } else { pb::EnvironmentState::Running as i32 }, revision: 1,
                    resources_held: true, ..Default::default()
                };
                owners.insert(spec.id, pb::GetEnvironmentResponse { record: Some(record.clone()), node_address: "127.0.0.1:9000".into(), relay_address: "127.0.0.1:9443".into() });
                self.0.metadata.lock().unwrap().insert(record.spec.as_ref().unwrap().id.clone(), user_metadata);
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
    async fn get_environment_metadata(&self, request: Request<pb::GetEnvironmentMetadataRequest>) -> Result<Response<pb::EnvironmentMetadataRecord>, Status> {
        let request = request.into_inner();
        let owner = self.0.owners.lock().unwrap().get(&request.id).cloned()
            .ok_or_else(|| Status::not_found("environment not found"))?;
        let record = owner.record.ok_or_else(|| Status::not_found("environment not found"))?;
        let spec = record.spec.ok_or_else(|| Status::not_found("environment not found"))?;
        if request.caller.as_ref().is_none_or(|caller| caller.tenant_id != spec.tenant_id) {
            return Err(Status::not_found("environment not found"));
        }
        Ok(Response::new(pb::EnvironmentMetadataRecord {
            environment_id: request.id.clone(),
            user_metadata: self.0.metadata.lock().unwrap().get(&request.id).cloned().unwrap_or_default(),
            state: record.state,
        }))
    }
    async fn list_e2b_environment_metadata(&self, request: Request<pb::ListEnvironmentMetadataRequest>) -> Result<Response<pb::ListEnvironmentMetadataResponse>, Status> {
        let request = request.into_inner();
        let caller = request.caller.ok_or_else(|| Status::unauthenticated("caller required"))?;
        let owners = self.0.owners.lock().unwrap().clone();
        let metadata = self.0.metadata.lock().unwrap().clone();
        let mut records = owners.into_iter().filter_map(|(id, owner)| {
            let record = owner.record?;
            let spec = record.spec?;
            let values = metadata.get(&id).cloned().unwrap_or_default();
            (caller.tenant_id == spec.tenant_id
                && spec.compatibility.is_some()
                && (request.states.is_empty() || request.states.contains(&record.state))
                && request.metadata_filter.iter().all(|(key, value)| values.get(key) == Some(value)))
                .then_some(pb::EnvironmentMetadataRecord { environment_id: id, user_metadata: values, state: record.state })
        }).collect::<Vec<_>>();
        records.sort_by(|left, right| left.environment_id.cmp(&right.environment_id));
        let start = if request.cursor.is_empty() {
            0
        } else {
            request.cursor.strip_prefix("fixture:")
                .and_then(|value| value.parse::<usize>().ok())
                .ok_or_else(|| Status::invalid_argument("invalid fixture cursor"))?
        }.min(records.len());
        let limit = usize::try_from(request.limit).unwrap_or(0);
        let end = start.saturating_add(limit).min(records.len());
        let next_cursor = if end < records.len() {
            format!("fixture:{end}")
        } else {
            String::new()
        };
        Ok(Response::new(pb::ListEnvironmentMetadataResponse { records: records[start..end].to_vec(), next_cursor }))
    }
    async fn get_environment_control(&self, request: Request<pb::GetEnvironmentControlRequest>) -> Result<Response<pb::EnvironmentControlRecord>, Status> {
        let error = tonic::Code::from_i32(self.0.control_error.load(Ordering::SeqCst) as i32);
        if error != tonic::Code::Ok {
            return Err(Status::new(error, "injected control read failure"));
        }
        let id = request.into_inner().id;
        let owner = self.0.owners.lock().unwrap().get(&id).cloned()
            .ok_or_else(|| Status::not_found("environment not found"))?;
        let policy = owner.record
            .and_then(|record| record.spec)
            .and_then(|spec| spec.run_policy)
            .ok_or_else(|| Status::not_found("run policy disabled"))?;
        Ok(Response::new(pb::EnvironmentControlRecord {
            environment_id: id,
            timeout_override_seconds: None,
            expires_at_unix_ms: policy.timeout_seconds.saturating_mul(1000).saturating_add(1),
            started_at_unix_ms: 1,
            revision: 1,
            generation: 1,
            expiry_claimed: false,
        }))
    }
}, [
    update_environment_control(UpdateEnvironmentControlRequest) -> EnvironmentControlRecord,
    list_environment_metadata(ListEnvironmentMetadataRequest) -> ListEnvironmentMetadataResponse,
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
        let error = tonic::Code::from_i32(self.0.auth_error.load(Ordering::SeqCst) as i32);
        if error != tonic::Code::Ok {
            return Err(Status::new(error, "injected authentication failure"));
        }
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
impl pb::snapshot_service_server::SnapshotService for Service {
    async fn publish_snapshot(
        &self,
        _: Request<pb::PublishSnapshotRequest>,
    ) -> Result<Response<pb::ReusableSnapshot>, Status> {
        Err(Status::unimplemented("fixture only resolves snapshots"))
    }

    async fn get_snapshot(
        &self,
        request: Request<pb::GetSnapshotRequest>,
    ) -> Result<Response<pb::ReusableSnapshot>, Status> {
        self.0.snapshot_calls.fetch_add(1, Ordering::SeqCst);
        let snapshot = self
            .0
            .snapshot
            .lock()
            .unwrap()
            .clone()
            .filter(|snapshot| snapshot.id == request.get_ref().id)
            .ok_or_else(|| Status::not_found("snapshot not found"))?;
        Ok(Response::new(snapshot))
    }

    async fn list_snapshots(
        &self,
        _: Request<pb::ListSnapshotsRequest>,
    ) -> Result<Response<pb::ListSnapshotsResponse>, Status> {
        Err(Status::unimplemented("fixture only resolves snapshots"))
    }

    async fn delete_snapshot(
        &self,
        _: Request<pb::DeleteSnapshotRequest>,
    ) -> Result<Response<pb::ReusableSnapshot>, Status> {
        Err(Status::unimplemented("fixture only resolves snapshots"))
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
    templates: Arc<templates::TemplateSource>,
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
        adx_transport::install_crypto_provider();
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
        let auth = endpoint.clone();
        let snapshots = endpoint.clone();
        let server = tokio::spawn(async move {
            let incoming = futures_util::stream::unfold(listener, |listener| async {
                let socket = listener.accept().await.map(|(socket, _)| socket);
                Some((socket, listener))
            });
            tonic::transport::Server::builder()
                .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(endpoint.clone()))
                .add_service(pb::environment_directory_service_server::EnvironmentDirectoryServiceServer::new(endpoint))
                .add_service(pb::auth_service_server::AuthServiceServer::new(auth))
                .add_service(pb::snapshot_service_server::SnapshotServiceServer::new(snapshots))
                .serve_with_incoming(incoming).await.unwrap();
        });
        let config: Config = serde_json::from_value(json!({
            "listen":"127.0.0.1:0", "coordinator_address":address, "internal_security":"network", "ingress_mode":"standalone",
            "rpc_timeout_seconds":2, "cache_entries":1, "auth_cache_ttl_seconds":1,
            "create_unknown_retention_seconds":retention,
            "create_mode":if local_first { "local_first" } else { "central" },
            "e2b": {
                "domain":"sandbox.test",
                "envdVersion":"0.9.0",
                "templates":{"base":{"name":"base","version":"1"}},
                "requiredNodeLabels":{"adx.runtime.profile":"e2b-v1"}
            },
            "runtime_profile": {
                "rootfs":{"runtime_class":"runsc","type":"local","path":"/rootfs","readonly":true},
                "bootstrap":{"type":"erofs","root":"/rootfs","target":"/rootfs","entrypoint":["/rootfs/init"]},
                "env":{"ADX_ENVD_PATH":"/usr/local/bin/envd","ADX_ENVD_SHA256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","ADX_ENVD_DEFAULT_USER":"user","ADX_ENVD_DEFAULT_WORKDIR":"/workspace"}
            }
        })).unwrap();
        let clients = Clients::new(config).unwrap();
        let service = EnvironmentService::new(clients.clone());
        let (managed, templates) =
            templates::TemplateSource::managed(vec![("tenant".into(), native_template())]);
        let api =
            Api::with_templates_and_environment(clients.clone(), service.clone(), managed).unwrap();
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
            templates,
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

fn native_template() -> adx_agent_core::TemplateVersion {
    serde_json::from_value(json!({
        "name":"base","version":"1",
        "image":format!("registry.example/base@sha256:{}", "a".repeat(64)),
        "isolation_runtime":"runsc",
        "env":{"NATIVE_TEMPLATE":"true"},
        "resources":{"cpu_millis":1250,"memory_mib":768,"cpu_limit_millis":1500,"memory_limit_mib":1024},
        "service":[{"protocol":"http","port":8080}],
        "lifecycle":{"idle_timeout_seconds":600,"idle_action":"pause","pause":{"retention":{"kind":"forever"},"timeout_seconds":30,"storage":"shared"}},
        "init_timeout_seconds":30,
        "readiness_probe":{"tcp_socket":{"port":8080},"period_seconds":3,"timeout_seconds":2}
    })).unwrap()
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
    let request = br#"{"templateID":"base","envVars":{"REQUEST_VALUE":"ok"}}"#;
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
    assert_eq!(
        fixture.templates.calls(),
        vec![("tenant".into(), "base".into(), "1".into())]
    );
    let owner = fixture
        .state
        .owners
        .lock()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .clone();
    let spec = owner.record.unwrap().spec.unwrap();
    assert_eq!(spec.image, native_template().image);
    assert_eq!(spec.env["NATIVE_TEMPLATE"], "true");
    assert_eq!(spec.env["REQUEST_VALUE"], "ok");
    assert_eq!(spec.resources.as_ref().unwrap().cpu_millis, 1250);
    let execution = spec.execution.as_ref().unwrap();
    assert!(execution.inherit_entrypoint);
    assert_eq!(execution.ports, vec![8080]);
    assert_eq!(execution.limits.as_ref().unwrap().cpu_millis, 1500);
    assert_eq!(
        execution.limits.as_ref().unwrap().memory_bytes,
        1024 * 1_048_576
    );
    assert_eq!(execution.init_timeout_seconds, 30);
    assert!(execution.readiness_probe.is_some());
    assert_eq!(spec.lifecycle.as_ref().unwrap().idle_timeout_seconds, 0);
    let policy = spec.run_policy.as_ref().unwrap();
    assert_eq!(policy.timeout_seconds, 300);
    assert_eq!(policy.action, pb::RunExpiryAction::Delete as i32);
    assert_eq!(
        spec.scheduling.unwrap().required_node[0].match_labels["adx.runtime.profile"],
        "e2b-v1"
    );
    let identity = spec.template_identity.unwrap();
    assert_eq!(identity.tenant_id, "tenant");
    assert_eq!(identity.name, "base");
    assert_eq!(identity.version, "1");
}

#[tokio::test]
async fn e2b_create_rejects_control_environment_overrides_before_template_lookup() {
    let fixture = Fixture::new().await;
    let headers = [
        ("X-API-Key", "k".repeat(32)),
        ("Content-Type", "application/json".into()),
    ];
    let response = crate::http::request(
        fixture.api.clone(),
        "POST",
        "/v2/sandboxes",
        &headers,
        r#"{"templateID":"base","envVars":{"ADX_ENVD_PATH":"injected"}}"#,
    )
    .await;
    assert_eq!(response.status().as_u16(), 400);
    assert!(fixture.templates.calls().is_empty());
}

#[tokio::test]
async fn configured_native_alias_not_found_does_not_fallback_to_same_named_snapshot() {
    let fixture = Fixture::new().await;
    *fixture.state.snapshot.lock().unwrap() = Some(pb::ReusableSnapshot {
        id: "base".into(),
        template: Some(spec("snapshot-base")),
        state: pb::SnapshotState::Ready as i32,
        ..Default::default()
    });
    let (managed, source) =
        templates::TemplateSource::managed(vec![("other-tenant".into(), native_template())]);
    let api = Api::with_templates_and_environment(
        fixture.clients.clone(),
        fixture.service.clone(),
        managed,
    )
    .unwrap();
    let headers = [
        ("X-API-Key", "k".repeat(32)),
        ("Content-Type", "application/json".into()),
    ];
    let response = http::request(
        api,
        "POST",
        "/v2/sandboxes",
        &headers,
        r#"{"templateID":"base"}"#,
    )
    .await;
    assert_eq!(response.status().as_u16(), 404);
    assert_eq!(fixture.state.snapshot_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        source.calls(),
        vec![("tenant".into(), "base".into(), "1".into())]
    );
}

#[tokio::test]
async fn create_rejects_oversized_platform_metadata_before_any_create_rpc() {
    let fixture = Fixture::new().await;
    let metadata = (0..17)
        .map(|index| (format!("key-{index}"), "x".repeat(4096)))
        .collect::<BTreeMap<_, _>>();
    let error = fixture
        .service
        .create_with_metadata(
            spec("metadata-too-large"),
            json!({}),
            "metadata-too-large",
            &caller(),
            metadata,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn native_template_query_uses_real_local_control_without_binding_or_environment_create() {
    let fixture = Fixture::new().await;
    let environment = Arc::new(
        LocalEnvironment::new(
            fixture.service.clone(),
            Duration::from_secs(60),
            SandboxNetworkPolicy {
                sandbox_cidrs: vec!["10.0.0.0/24".into()],
                internal_cidrs: vec![],
                public_egress: false,
                egress_allow: vec![],
                runtime_policies: Default::default(),
            },
        )
        .unwrap(),
    );
    let activator = Arc::new(Activator::new(
        AgentState::new(Arc::new(MemoryRepository::default())),
        environment,
    ));
    let control = Arc::new(LocalControl::new(activator));
    let managed = Arc::new(ManagedService::new(control.clone()));
    let context = RequestContext::new(Duration::from_secs(3));
    control
        .publish(&context, "tenant", &native_template())
        .await
        .unwrap();
    let api = Api::with_templates_and_environment(
        fixture.clients.clone(),
        fixture.service.clone(),
        managed,
    )
    .unwrap();
    let template = api.native_template("tenant", "base", "1").await.unwrap();
    assert_eq!(template, native_template());
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 0);
    assert!(control
        .binding(
            &context,
            &adx_agent_core::Scope {
                tenant: "tenant".into(),
                template: "base".into(),
                version: "1".into(),
                binding_id: "unused".into(),
            }
        )
        .await
        .is_err());
}

#[tokio::test]
async fn e2b_list_delegates_metadata_filters_and_opaque_cursors_to_platform() {
    let fixture = Fixture::new().await;
    let headers = [
        ("X-API-Key", "k".repeat(32)),
        ("Content-Type", "application/json".into()),
    ];
    for metadata in [
        r#"{"team":"core","purpose":"one"}"#,
        r#"{"team":"core","purpose":"two"}"#,
    ] {
        let response = http::request(
            fixture.api.clone(),
            "POST",
            "/v2/sandboxes",
            &headers,
            &format!(r#"{{"templateID":"base","metadata":{metadata}}}"#),
        )
        .await;
        assert_eq!(response.status().as_u16(), 201);
    }
    let page = http::request(
        fixture.api.clone(),
        "GET",
        "/v2/sandboxes?metadata=team%3Dcore&limit=1",
        &headers,
        "",
    )
    .await;
    assert_eq!(page.status().as_u16(), 200);
    assert_eq!(page.headers().get("x-next-token").unwrap(), "fixture:1");
    let page: Value = serde_json::from_slice(page.body()).unwrap();
    assert_eq!(page.as_array().unwrap().len(), 1);
    assert_eq!(page[0]["metadata"]["team"], "core");
    assert_eq!(page[0].get("envdAccessToken"), None);
    let next = http::request(
        fixture.api.clone(),
        "GET",
        "/v2/sandboxes?metadata=team%3Dcore&limit=1&nextToken=fixture%3A1",
        &headers,
        "",
    )
    .await;
    assert_eq!(next.status().as_u16(), 200);
    let next: Value = serde_json::from_slice(next.body()).unwrap();
    assert_eq!(next.as_array().unwrap().len(), 1);
    assert_ne!(page[0]["sandboxID"], next[0]["sandboxID"]);
}

#[tokio::test]
async fn generic_run_policy_does_not_make_a_native_environment_an_e2b_sandbox() {
    let fixture = Fixture::new().await;
    let mut native = spec("native-with-runtime-policy");
    native.run_policy = Some(pb::RunPolicy {
        timeout_seconds: 90_000,
        action: pb::RunExpiryAction::Delete as i32,
    });
    fixture
        .service
        .create(
            native,
            json!({"name":"native-with-runtime-policy"}),
            "native-runtime-policy",
            &caller(),
        )
        .await
        .unwrap();
    // The generic control exists independently of E2B.
    assert!(fixture
        .clients
        .environment_control("native-with-runtime-policy", &caller())
        .await
        .is_ok());
    let headers = [
        ("X-API-Key", "k".repeat(32)),
        ("Content-Type", "application/json".into()),
    ];
    for (method, suffix, body) in [
        ("GET", "", ""),
        ("DELETE", "", ""),
        ("POST", "/timeout", r#"{"timeout":60}"#),
        ("POST", "/pause", "{}"),
        ("POST", "/snapshots", "{}"),
        ("PUT", "/network", "{}"),
    ] {
        let response = http::request(
            fixture.api.clone(),
            method,
            &format!("/sandboxes/native-with-runtime-policy{suffix}"),
            &headers,
            body,
        )
        .await;
        assert_eq!(response.status().as_u16(), 404, "{method} {suffix}");
    }
    let response = http::request(
        fixture.api.clone(),
        "POST",
        "/v2/sandboxes/native-with-runtime-policy/connect",
        &headers,
        "{}",
    )
    .await;
    assert_eq!(response.status().as_u16(), 404);
    let list = http::request(fixture.api.clone(), "GET", "/v2/sandboxes", &headers, "").await;
    assert_eq!(list.status().as_u16(), 200);
    assert!(serde_json::from_slice::<Value>(list.body())
        .unwrap()
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 1);
    assert!(fixture
        .state
        .owners
        .lock()
        .unwrap()
        .contains_key("native-with-runtime-policy"));
}

#[tokio::test]
async fn e2b_snapshot_clone_uses_the_persisted_spec_after_alias_resolution() {
    let fixture = Fixture::new().await;
    let headers = [
        ("X-API-Key", "k".repeat(32)),
        ("Content-Type", "application/json".into()),
    ];
    let first = http::request(
        fixture.api.clone(),
        "POST",
        "/v2/sandboxes",
        &headers,
        r#"{"templateID":"base","metadata":{"source":"one"}}"#,
    )
    .await;
    assert_eq!(first.status().as_u16(), 201);
    let mut original = fixture
        .state
        .owners
        .lock()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .record
        .as_ref()
        .unwrap()
        .spec
        .clone()
        .unwrap();
    // The published alias may be removed or repointed after the snapshot. Restore must consume
    // the persisted specification and never attempt to resolve this historical identity.
    original.compatibility.as_mut().unwrap().template_id = "deleted-alias".into();
    *fixture.state.snapshot.lock().unwrap() = Some(pb::ReusableSnapshot {
        id: "snapshot-1".into(),
        template: Some(original.clone()),
        state: pb::SnapshotState::Ready as i32,
        ..Default::default()
    });

    let cloned = http::request(
        fixture.api.clone(),
        "POST",
        "/v2/sandboxes",
        &headers,
        r#"{"templateID":"snapshot-1","timeout":60,"autoPause":true,"metadata":{"clone":"two"}}"#,
    )
    .await;
    assert_eq!(cloned.status().as_u16(), 201);
    let cloned = fixture
        .state
        .owners
        .lock()
        .unwrap()
        .values()
        .map(|owner| {
            owner
                .record
                .as_ref()
                .unwrap()
                .spec
                .as_ref()
                .unwrap()
                .clone()
        })
        .find(|spec| spec.id != original.id)
        .unwrap();
    assert_eq!(fixture.state.snapshot_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.templates.calls(),
        vec![("tenant".into(), "base".into(), "1".into())]
    );
    assert_eq!(cloned.image, original.image);
    assert_eq!(cloned.resources, original.resources);
    assert_eq!(cloned.env, original.env);
    assert_eq!(cloned.runtime_profile, original.runtime_profile);
    assert_eq!(original.run_policy.as_ref().unwrap().timeout_seconds, 300);
    assert_eq!(cloned.run_policy.as_ref().unwrap().timeout_seconds, 60);
    assert_eq!(
        cloned.run_policy.as_ref().unwrap().action,
        pb::RunExpiryAction::Pause as i32
    );
    let original_compatibility = original.compatibility.unwrap();
    let cloned_compatibility = cloned.compatibility.unwrap();
    assert_eq!(cloned.template_identity, original.template_identity);
    assert_ne!(
        cloned_compatibility.envd_access_token,
        original_compatibility.envd_access_token
    );
    assert_ne!(
        cloned_compatibility.traffic_access_token,
        original_compatibility.traffic_access_token
    );
    let metadata = fixture.state.metadata.lock().unwrap();
    assert_eq!(metadata[&original.id]["source"], "one");
    assert_eq!(metadata[&cloned.id]["clone"], "two");
    assert!(!metadata[&cloned.id].contains_key("source"));
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
