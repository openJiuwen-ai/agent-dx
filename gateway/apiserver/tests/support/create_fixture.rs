const FAMILY_ID: &str = "550e8400-e29b-41d4-a716-446655440010";
const BUILD_ID: &str = "550e8400-e29b-41d4-a716-446655440011";

#[derive(Default)]
struct State {
    entry_node: Mutex<Option<pb::NodeEndpoint>>,
    local_persistence: Mutex<Vec<bool>>,
    owners: Mutex<HashMap<String, pb::GetEnvironmentResponse>>,
    receipts: Mutex<HashMap<(String, String), String>>,
    starts: AtomicUsize,
    create_calls: AtomicUsize,
    lookups: AtomicUsize,
    response_error: AtomicUsize,
    auth_error: AtomicUsize,
    control_error: AtomicUsize,
    control_read_calls: AtomicUsize,
    batch_control_calls: AtomicUsize,
    control_owner_unavailable: AtomicBool,
    next_control_read_error: AtomicUsize,
    missing_resume_resources: AtomicBool,
    next_control_update_error: AtomicUsize,
    lose_zero_update_reply: AtomicBool,
    staged_run_timeouts: Mutex<HashMap<String, u64>>,
    control_updates: Mutex<Vec<pb::UpdateEnvironmentControlRequest>>,
    control_order: Mutex<Vec<&'static str>>,
    resume_requests: Mutex<Vec<pb::EnsureRunningRequest>>,
    scheduling_expired: AtomicBool,
    block_response: AtomicBool,
    pending_response: AtomicBool,
    deadlines: Mutex<Vec<u64>>,
    timeout_phase: Mutex<Option<String>>,
    snapshot: Mutex<Option<pb::ReusableSnapshot>>,
    snapshot_calls: AtomicUsize,
    snapshot_error: AtomicUsize,
    snapshot_requests: Mutex<Vec<(String, String)>>,
    preparation_calls: AtomicUsize,
    preparation_error: AtomicUsize,
    registered_tenant: Mutex<String>,
    resolved_snapshot_id: Mutex<String>,
    preparation_requests: Mutex<Vec<(String, String, bool)>>,
    current_preparation: Mutex<Option<pb::SnapshotPreparationView>>,
    release: Notify,
}
fn fixture_control(
    state: &State,
    id: &str,
    caller: &pb::CallerContext,
) -> Result<pb::EnvironmentControlRecord, Box<Status>> {
    let error = tonic::Code::from_i32(state.control_error.load(Ordering::SeqCst) as i32);
    if error != tonic::Code::Ok {
        return Err(Box::new(Status::new(
            error,
            "injected control read failure",
        )));
    }
    let owners = state.owners.lock().unwrap();
    let record = owners
        .get(id)
        .and_then(|owner| owner.record.as_ref())
        .ok_or_else(|| Status::not_found("environment not found"))?;
    let spec = record
        .spec
        .as_ref()
        .filter(|spec| spec.tenant_id == caller.tenant_id)
        .ok_or_else(|| Status::not_found("environment not found"))?;
    let policy = spec
        .run_policy
        .as_ref()
        .ok_or_else(|| Status::not_found("run policy disabled"))?;
    let initial_timeout = state
        .resume_requests
        .lock()
        .unwrap()
        .iter()
        .rev()
        .find(|request| request.environment_id == id)
        .and_then(|request| request.run_timeout_seconds)
        .unwrap_or(policy.timeout_seconds);
    let timeout = state
        .control_updates
        .lock()
        .unwrap()
        .iter()
        .filter(|request| request.id == id)
        .fold(initial_timeout, |current, request| {
            if request.extend_only {
                current.max(request.timeout_seconds)
            } else {
                request.timeout_seconds
            }
        });
    Ok(pb::EnvironmentControlRecord {
        environment_id: id.into(),
        expires_at_unix_ms: timeout.saturating_mul(1000).saturating_add(1),
        started_at_unix_ms: 1,
        revision: record.revision,
        generation: record
            .assignment
            .as_ref()
            .map_or(1, |assignment| assignment.generation),
    })
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
        let request = request.into_inner();
        let spec = request.spec.unwrap();
        // The coordinator validates and canonicalizes E2B Environment specs
        // through the core model before publishing them.
        let mut spec = if spec.compatibility.is_some() {
            adx_core::EnvironmentSpec::try_from(spec).unwrap().into()
        } else {
            spec
        };
        let identity = (spec.tenant_id.clone(), request.request_id);
        if !identity.1.is_empty() {
            if let Some(id) = self.0.receipts.lock().unwrap().get(&identity) {
                spec = self.0.owners.lock().unwrap()[id].record.as_ref().unwrap().spec.clone().unwrap();
            }
        }
        if self.0.scheduling_expired.load(Ordering::SeqCst) {
            return Err(adx_protocol::scheduling_deadline(adx_core::scheduling::SchedulingFailure {
                rejections: [("insufficient_cpu".into(), 1)].into(),
                ..Default::default()
            }));
        }
        let (record, newly_created) = {
            let mut owners = self.0.owners.lock().unwrap();
            if let Some(owner) = owners.get_mut(&spec.id) {
                if !self.0.pending_response.load(Ordering::SeqCst) {
                    let record = owner.record.as_mut().unwrap();
                    if record.state == pb::EnvironmentState::Pending as i32 {
                        record.state = pb::EnvironmentState::Running as i32;
                        record.revision = 1;
                    }
                }
                (owner.record.clone().unwrap(), false)
            } else {
                self.0.starts.fetch_add(1, Ordering::SeqCst);
                let record = pb::EnvironmentRecord {
                    creation_request_id: identity.1.clone(),
                    observation: None,
                    execution_artifacts: vec![],
            snapshot_source: None,
                    failure: None,
assignment: Some(pb::Assignment { environment_id: spec.id.clone(), node_id: self.0.entry_node.lock().unwrap().as_ref().map_or_else(|| "node".into(), |node| node.node_id.clone()), generation: 1, ..Default::default() }),
                    spec: Some(spec.clone()), state: if self.0.pending_response.load(Ordering::SeqCst) { pb::EnvironmentState::Pending as i32 } else { pb::EnvironmentState::Running as i32 }, revision: 1,
                    resources_held: true, ..Default::default()
                };
                owners.insert(spec.id, pb::GetEnvironmentResponse { record: Some(record.clone()), node_address: "127.0.0.1:9000".into(), relay_address: "127.0.0.1:9443".into() });
                (record, true)
            }
        };
        if record.state == pb::EnvironmentState::Running as i32 && !identity.1.is_empty() {
            self.0.receipts.lock().unwrap().entry(identity).or_insert_with(|| record.spec.as_ref().unwrap().id.clone());
        }
        if self.0.block_response.load(Ordering::SeqCst) {
            self.0.release.notified().await;
        }
        let phase = self.0.timeout_phase.lock().unwrap().clone()
            .filter(|_| record.state != pb::EnvironmentState::Failed as i32);
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
        Ok(Response::new(pb::EnvironmentResult { newly_created, record: Some(record), durability: pb::Durability::Published as i32 , ..Default::default()}))
    }
    async fn ensure_running(&self, request: Request<pb::EnsureRunningRequest>) -> Result<Response<pb::GetEnvironmentResponse>, Status> {
        let request = request.into_inner();
        let caller = request.caller.clone().ok_or_else(|| Status::unauthenticated("caller required"))?;
        self.0.control_order.lock().unwrap().push("ensure_running");
        let mut owners = self.0.owners.lock().unwrap();
        let owner = owners.get_mut(&request.environment_id).ok_or_else(|| Status::not_found("environment not found"))?;
        let record = owner.record.as_mut().unwrap();
        if record.spec.as_ref().unwrap().tenant_id != caller.tenant_id {
            return Err(Status::permission_denied("wrong tenant"));
        }
        if record.state == pb::EnvironmentState::Paused as i32 {
            self.0.resume_requests.lock().unwrap().push(request.clone());
        }
        record.state = pb::EnvironmentState::Running as i32;
        record.revision += 1;
        if self.0.missing_resume_resources.load(Ordering::SeqCst) {
            record.spec.as_mut().unwrap().resources = None;
        }
        self.0.control_owner_unavailable.store(false, Ordering::SeqCst);
        Ok(Response::new(owner.clone()))
    }
    async fn update_environment_control(&self, request: Request<pb::UpdateEnvironmentControlRequest>) -> Result<Response<pb::EnvironmentControlRecord>, Status> {
        let request = request.into_inner();
        self.0.control_order.lock().unwrap().push("update");
        let current_state = self.0.owners.lock().unwrap().get(&request.id)
            .and_then(|owner| owner.record.as_ref())
            .map(|record| record.state);
        // A live Paused owner stages its next duration; an unavailable owner
        // still requires EnsureRunning before it can process updates.
        let live = current_state.is_some_and(|state| state == pb::EnvironmentState::Running as i32 || state == pb::EnvironmentState::Paused as i32);
        if !live || self.0.control_owner_unavailable.load(Ordering::SeqCst) {
            return Err(Status::unavailable("timer owner unavailable before resume"));
        }
        let error = tonic::Code::from_i32(self.0.next_control_update_error.swap(0, Ordering::SeqCst) as i32);
        if error != tonic::Code::Ok {
            return Err(Status::new(error, "injected timer update failure"));
        }
        let caller = request.caller.clone().ok_or_else(|| Status::unauthenticated("caller required"))?;
        let _ = fixture_control(&self.0, &request.id, &caller).map_err(|error| *error)?;
        if current_state == Some(pb::EnvironmentState::Paused as i32) {
            self.0.staged_run_timeouts.lock().unwrap().insert(request.id.clone(), request.timeout_seconds);
        }
        self.0.control_updates.lock().unwrap().push(request.clone());
        let control = fixture_control(&self.0, &request.id, &caller).map_err(|error| *error)?;
        if request.timeout_seconds == 0 && self.0.lose_zero_update_reply.swap(false, Ordering::SeqCst) {
            // Model the expiry completing before its RPC reply is lost.
            let mut owners = self.0.owners.lock().unwrap();
            let record = owners.get_mut(&request.id).unwrap().record.as_mut().unwrap();
            record.state = if record.spec.as_ref().unwrap().run_policy.as_ref().unwrap().action == pb::RunExpiryAction::Pause as i32 {
                pb::EnvironmentState::Paused
            } else {
                pb::EnvironmentState::Deleted
            } as i32;
            return Err(Status::deadline_exceeded("injected lost expiry reply"));
        }
        Ok(Response::new(control))
    }
    async fn get_environment_control(&self, request: Request<pb::GetEnvironmentControlRequest>) -> Result<Response<pb::EnvironmentControlRecord>, Status> {
        self.0.control_read_calls.fetch_add(1, Ordering::SeqCst);
        let error = tonic::Code::from_i32(self.0.next_control_read_error.swap(0, Ordering::SeqCst) as i32);
        if error != tonic::Code::Ok {
            return Err(Status::new(error, "injected response control read failure"));
        }
        let request = request.into_inner();
        let caller = request.caller.ok_or_else(|| Status::unauthenticated("caller required"))?;
        Ok(Response::new(fixture_control(&self.0, &request.id, &caller).map_err(|error| *error)?))
    }
    async fn batch_get_environment_control(&self, request: Request<pb::BatchGetEnvironmentControlRequest>) -> Result<Response<pb::BatchGetEnvironmentControlResponse>, Status> {
        self.0.batch_control_calls.fetch_add(1, Ordering::SeqCst);
        let request = request.into_inner();
        let caller = request.caller.ok_or_else(|| Status::unauthenticated("caller required"))?;
        let mut records = Vec::new();
        for id in request.ids {
            match fixture_control(&self.0, &id, &caller) {
                Ok(record) => records.push(record),
                Err(error) if error.code() == tonic::Code::NotFound => (),
                Err(error) => return Err(*error),
            }
        }
        Ok(Response::new(pb::BatchGetEnvironmentControlResponse { records }))
    }
}, [
    get_scheduling_queue(GetSchedulingQueueRequest) -> GetSchedulingQueueResponse,
    set_node_scheduling(SetNodeSchedulingRequest) -> NodeSchedulingState,
    prepare_create(LocalEnvironmentCreateRequest) -> PreparedEnvironment,
    reserve_local_environment(ClaimEnvironmentRequest) -> ClaimEnvironmentResponse,
    claim_environment(ClaimEnvironmentRequest) -> ClaimEnvironmentResponse,
    forward_create(LocalEnvironmentCreateRequest) -> EnvironmentResult,
    unregister_node(UnregisterNodeRequest) -> UnregisterNodeResponse,
    register_node(RegisterNodeRequest) -> RegisterNodeResponse,
    inspect_node(InspectNodeRequest) -> InspectNodeResponse,
    commit_environment(CommitEnvironmentRequest) -> CommitEnvironmentResponse,
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
        let api_key = request.into_inner().api_key;
        if api_key.len() < 32 {
            return Err(Status::unauthenticated("invalid API key"));
        }
        let mut identity = caller();
        identity.administrator = api_key == "admin-detail-key-00000000000000000000";
        if api_key.starts_with("other-") {
            identity.tenant_id = "other-tenant".into();
        } else if api_key == "foreign-key-00000000000000000000" {
            identity.tenant_id = "foreign".into();
        }
        Ok(Response::new(pb::VerifyApiKeyResponse {
            caller: Some(identity),
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
        let request = request.into_inner();
        let tenant = request
            .caller
            .as_ref()
            .map(|caller| caller.tenant_id.as_str())
            .unwrap_or("");
        self.0
            .snapshot_requests
            .lock()
            .unwrap()
            .push((tenant.into(), request.id.clone()));
        let error = tonic::Code::from_i32(self.0.snapshot_error.load(Ordering::SeqCst) as i32);
        if error != tonic::Code::Ok {
            return Err(Status::new(error, "injected snapshot source failure"));
        }
        let snapshot = self
            .0
            .snapshot
            .lock()
            .unwrap()
            .clone()
            .filter(|snapshot| {
                snapshot.id == request.id
                    && snapshot
                        .template
                        .as_ref()
                        .is_some_and(|spec| spec.tenant_id == tenant)
            })
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
service_impl!(pb::snapshot_preparation_service_server::SnapshotPreparationService, {
    async fn resolve_snapshot_template(
        &self,
        request: Request<pb::ResolveSnapshotTemplateRequest>,
    ) -> Result<Response<pb::SnapshotPreparationView>, Status> {
        self.0.preparation_calls.fetch_add(1, Ordering::SeqCst);
        let error = tonic::Code::from_i32(self.0.preparation_error.load(Ordering::SeqCst) as i32);
        if error != tonic::Code::Ok {
            return Err(Status::new(error, "injected preparation resolution failure"));
        }
        let request = request.into_inner();
        let tenant = request.caller.as_ref().map(|caller| caller.tenant_id.as_str()).unwrap_or("");
        self.0.preparation_requests.lock().unwrap().push((tenant.into(), request.reference.clone(), request.registration_only));
        if request.reference.contains(':') {
            return Err(Status::invalid_argument("fixture requires a family reference"));
        }
        let name = request.reference.as_str();
        if (name != "base" && name != FAMILY_ID) || tenant != *self.0.registered_tenant.lock().unwrap() {
            return Err(Status::not_found("no registered preparation"));
        }
        if request.registration_only {
            if let Some(current) = self.0.current_preparation.lock().unwrap().clone() {
                return Ok(Response::new(current));
            }
        }
        let snapshot_id = self.0.resolved_snapshot_id.lock().unwrap().clone();
        let snapshot = self.0.snapshot.lock().unwrap().clone().filter(|snapshot| {
            snapshot.id == snapshot_id && snapshot.state == pb::SnapshotState::Ready as i32
                && snapshot.template.as_ref().is_some_and(|spec| spec.tenant_id == tenant)
        });
        if !request.registration_only && snapshot.is_none() {
            return Err(Status::failed_precondition("registered family has no available ready snapshot"));
        }
        Ok(Response::new(pb::SnapshotPreparationView {
            template_id: FAMILY_ID.into(), name: "base".into(), preparation_id: BUILD_ID.into(),
            phase: if snapshot.is_some() { pb::SnapshotPreparationPhase::Ready } else { pb::SnapshotPreparationPhase::Registered } as i32,
            resources: snapshot.as_ref().and_then(|snapshot| snapshot.template.as_ref()).and_then(|spec| spec.resources),
            created_at_unix_ms: 1,
            template_updated_at_unix_ms: 5,
            preparation_created_at_unix_ms: 2,
            preparation_updated_at_unix_ms: 3,
            last_spawned_at_unix_ms: Some(5),
            spawn_count: Some(2),
            envd_version: "0.9.0".into(),
            snapshot_available: snapshot.is_some(), snapshot_id, ..Default::default()
        }))
    }
    async fn delete_snapshot_template(
        &self,
        request: Request<pb::DeleteSnapshotTemplateRequest>,
    ) -> Result<Response<pb::DeleteSnapshotTemplateResponse>, Status> {
        if !request.into_inner().require_unused {
            return Err(Status::invalid_argument("E2B deletion must protect live references"));
        }
        let error = tonic::Code::from_i32(self.0.preparation_error.load(Ordering::SeqCst) as i32);
        Err(Status::new(if error == tonic::Code::Ok { tonic::Code::NotFound } else { error }, "template has live references"))
    }
}, [
    register_snapshot_preparation(RegisterSnapshotPreparationRequest) -> SnapshotPreparationView,
    start_snapshot_preparation(StartSnapshotPreparationRequest) -> SnapshotPreparationView,
    get_snapshot_preparation(GetSnapshotPreparationRequest) -> SnapshotPreparationView,
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
        Self::with_mode(retention, false).await
    }
    async fn with_mode(retention: u64, local_first: bool) -> Self {
        Self::with_claim_mode(retention, local_first, None).await
    }
    async fn with_claim_mode(retention: u64, local_first: bool, persist: Option<bool>) -> Self {
        adx_transport::install_crypto_provider();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let state = Arc::new(State::default());
        *state.registered_tenant.lock().unwrap() = "tenant".into();
        *state.resolved_snapshot_id.lock().unwrap() = "base-snapshot".into();
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
        let snapshots = endpoint.clone();
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
                .add_service(pb::snapshot_service_server::SnapshotServiceServer::new(snapshots.clone()))
                .add_service(pb::snapshot_preparation_service_server::SnapshotPreparationServiceServer::new(snapshots))
                .serve_with_incoming(incoming).await.unwrap();
        });
        let mut config = json!({
            "listen":"127.0.0.1:0", "coordinator_address":address, "internal_security":"network", "ingress_mode":"standalone",
            "rpc_timeout_seconds":2, "cache_entries":1, "auth_cache_ttl_seconds":1,
            "create_unknown_retention_seconds":retention,
            "create_mode":if local_first { "local_first" } else { "central" },
            "e2b": {
                "domain":"sandbox.test",
                "envdVersion":"0.9.0",
                "envdIngressSources":["10.0.0.2"],
                "requiredNodeLabels":{"adx.runtime.profile":"e2b-v1"}
            },
            "runtime_profile": {
                "rootfs":{"runtime_class":"runsc","type":"local","path":"/rootfs","readonly":true},
                "bootstrap":{"type":"erofs","root":"/rootfs","target":"/rootfs","entrypoint":["/rootfs/init"]},
                "env":{"ADX_ENVD_PATH":"/usr/local/bin/envd","ADX_ENVD_SHA256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","ADX_ENVD_DEFAULT_USER":"user","ADX_ENVD_DEFAULT_WORKDIR":"/workspace"}
            }
        });
        if let Some(persist) = persist {
            config["persist_local_claim"] = json!(persist);
        }
        let config: Config = serde_json::from_value(config).unwrap();
        *state.snapshot.lock().unwrap() = Some(snapshot_source(&config));
        let clients = Clients::new(config).unwrap();
        let api = Api::new(clients.clone()).unwrap();
        let service = api.environment_service.clone();
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

fn snapshot_source(config: &Config) -> pb::ReusableSnapshot {
    use adx_core::execution::{
        NetworkAction, NetworkDirection, NetworkPeer, NetworkPolicy, NetworkProtocol, NetworkRule,
        PortRange, TrafficMode, TrafficPolicy,
    };
    let mut source = contract::create_spec_with_environment(
        json!({
            "namespace":"e2b", "name":"base-source",
            "image":format!("registry.example/base@sha256:{}", "a".repeat(64)),
            "runtime":"runsc", "cpu":1250, "memory":768, "cpu_limit":1500, "mem_limit":1024,
            "inheritEntrypoint":true, "env":{"SNAPSHOT_SOURCE":"true"}, "ports":["8080"],
            "idleTimeoutSeconds":0, "initCallTimeoutSeconds":30,
            "readinessProbe":{"tcpSocket":{"port":8080},"periodSeconds":3,"timeoutSeconds":2}
        }),
        &caller(),
        config.runtime_profile.as_ref(),
    )
    .unwrap();
    let rules = vec![
        NetworkRule {
            action: NetworkAction::Deny,
            direction: NetworkDirection::Ingress,
            protocol: NetworkProtocol::Tcp,
            peer: NetworkPeer::default(),
            environment_port: 0,
            environment_port_range: Some(PortRange {
                first: 49983,
                last: 49983,
            }),
            priority: u32::MAX - 2,
        },
        NetworkRule {
            action: NetworkAction::Allow,
            direction: NetworkDirection::Ingress,
            protocol: NetworkProtocol::Tcp,
            peer: NetworkPeer {
                cidr: "10.0.0.2/32".into(),
                ..Default::default()
            },
            environment_port: 0,
            environment_port_range: Some(PortRange {
                first: 49983,
                last: 49983,
            }),
            priority: u32::MAX - 1,
        },
    ];
    let execution = source.execution.as_mut().unwrap();
    execution.private_service_ports = vec![49983];
    execution.protected_ingress = rules.iter().cloned().map(Into::into).collect();
    execution.network = Some(
        NetworkPolicy {
            traffic: Some(TrafficPolicy {
                ingress_default_action: NetworkAction::Allow,
                egress_default_action: NetworkAction::Allow,
                rules,
                mode: TrafficMode::Stateful,
            }),
            dns: None,
        }
        .into(),
    );
    source.envd = Some(pb::EnvdServiceConfig {
        version: "0.9.0".into(),
        gateway_auth: true,
        access_token: None,
        ingress_sources: vec!["10.0.0.2".into()],
    });
    source.scheduling.as_mut().unwrap().labels = [
        ("native-source".into(), "inherited".into()),
        ("source".into(), "from-native".into()),
    ]
    .into();
    source.scheduling.as_mut().unwrap().required_node = vec![pb::LabelSelector {
        match_labels: [("adx.runtime.profile".into(), "e2b-v1".into())].into(),
        ..Default::default()
    }];
    let source: pb::EnvironmentSpec = adx_core::EnvironmentSpec::try_from(source).unwrap().into();
    pb::ReusableSnapshot {
        id: "base-snapshot".into(),
        source_node_id: "node".into(),
        source_runtime_id: format!("{}-1", source.id),
        template: Some(source),
        artifact: Some(pb::CheckpointArtifact {
            storage: "shared".into(),
            scope: "fixture".into(),
            location: "fixture/base".into(),
            size_bytes: 1,
        }),
        kind: pb::SnapshotKind::Golden as i32,
        created_at_unix_ms: 1,
        revision: 1,
        state: pb::SnapshotState::Ready as i32,
        ..Default::default()
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

service_impl!(pb::node_service_server::NodeService, {
    async fn create_local_environment(&self, request: Request<pb::LocalEnvironmentCreateRequest>) -> Result<Response<pb::EnvironmentResult>, Status> {
        let trace = adx_observability::trace::Trace::rpc("test.node.create_local_environment", &request);
        trace.run_result(async {
            let request = request.into_inner();
            self.0.local_persistence.lock().unwrap().push(request.persist_claim);
            let create = request.create.unwrap();
            pb::coordinator_service_server::CoordinatorService::create_environment(self, adx_observability::trace::inject(create)).await
        }).await
    }
}, [
    get_environment_control(NodeGetEnvironmentControlRequest) -> EnvironmentControlRecord,
    batch_get_environment_control(NodeBatchGetEnvironmentControlRequest) -> BatchGetEnvironmentControlResponse,
    update_environment_control(NodeUpdateEnvironmentControlRequest) -> EnvironmentControlRecord,
    apply_node_upgrade_control(NodeUpgradeControl) -> NodeUpgradeInspection,
    inspect_node_upgrade(NodeUpgradeInspectRequest) -> NodeUpgradeInspection,
    wake_node_reconciliation(WakeNodeReconciliationRequest) -> WakeNodeReconciliationResponse,
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
    collect_checkpoint_cleanup(CollectCheckpointCleanupRequest) -> CollectCheckpointCleanupResponse,
]);
