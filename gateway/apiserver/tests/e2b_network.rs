//! E2B network updates exercise the HTTP adapter, revision merge, and node RPC.
#[path = "support/http.rs"]
mod http;

use adx_apiserver::{clients::Clients, config::Config, http::Api};
use adx_protocol::control as pb;
use http::request as send_http_request;
use serde_json::json;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};

#[derive(Clone)]
struct Service(Arc<Mutex<State>>);

struct State {
    owner: pb::GetEnvironmentResponse,
    lookups: usize,
    node_calls: usize,
    fail_node: bool,
    unsupported_pause: bool,
    unsupported_snapshot: bool,
    bump_on_lookup: Option<usize>,
    last_request: Option<pb::UpdateNetworkPolicyRequest>,
}

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
        Err(Status::unimplemented("network fixture has no node watch"))
    }
    async fn get_environment(&self, request: Request<pb::GetEnvironmentRequest>) -> Result<Response<pb::GetEnvironmentResponse>, Status> {
        let mut state = self.0.lock().unwrap();
        state.lookups += 1;
        assert_eq!(request.get_ref().environment_id, "sandbox");
        if state.bump_on_lookup == Some(state.lookups) {
            state.owner.record.as_mut().unwrap().revision += 1;
        }
        Ok(Response::new(state.owner.clone()))
    }
}, [
    reserve_local_environment(ClaimEnvironmentRequest) -> ClaimEnvironmentResponse,
    ensure_running(EnsureRunningRequest) -> GetEnvironmentResponse,
    retire_dormant(GetEnvironmentRequest) -> EnvironmentResult,
    get_environment_control(GetEnvironmentControlRequest) -> EnvironmentControlRecord,
    batch_get_environment_control(BatchGetEnvironmentControlRequest) -> BatchGetEnvironmentControlResponse,
    update_environment_control(UpdateEnvironmentControlRequest) -> EnvironmentControlRecord,
    get_scheduling_queue(GetSchedulingQueueRequest) -> GetSchedulingQueueResponse,
    set_node_scheduling(SetNodeSchedulingRequest) -> NodeSchedulingState,
    prepare_create(LocalEnvironmentCreateRequest) -> PreparedEnvironment,
    claim_environment(ClaimEnvironmentRequest) -> ClaimEnvironmentResponse,
    forward_create(LocalEnvironmentCreateRequest) -> EnvironmentResult,
    unregister_node(UnregisterNodeRequest) -> UnregisterNodeResponse,
    register_node(RegisterNodeRequest) -> RegisterNodeResponse,
    inspect_node(InspectNodeRequest) -> InspectNodeResponse,
    create_environment(CreateEnvironmentRequest) -> EnvironmentResult,
    commit_environment(CommitEnvironmentRequest) -> CommitEnvironmentResponse,
]);

#[tonic::async_trait]
impl pb::auth_service_server::AuthService for Service {
    async fn verify_api_key(
        &self,
        request: Request<pb::VerifyApiKeyRequest>,
    ) -> Result<Response<pb::VerifyApiKeyResponse>, Status> {
        if request.into_inner().api_key.len() != 32 {
            return Err(Status::unauthenticated("invalid key"));
        }
        Ok(Response::new(pb::VerifyApiKeyResponse {
            caller: Some(pb::CallerContext {
                tenant_id: "tenant".into(),
                administrator: false,
            }),
            expires_at_unix_seconds: 0,
        }))
    }
}

service_impl!(
    pb::environment_directory_service_server::EnvironmentDirectoryService,
    {
        type WatchEnvironmentsStream = std::pin::Pin<
            Box<
                dyn futures_util::Stream<Item = Result<pb::EnvironmentDirectoryFrame, Status>>
                    + Send,
            >,
        >;
        async fn watch_environments(
            &self,
            _: Request<pb::WatchEnvironmentsRequest>,
        ) -> Result<Response<Self::WatchEnvironmentsStream>, Status> {
            let owner = self.0.lock().unwrap().owner.clone();
            Ok(Response::new(Box::pin(
                futures_util::stream::once(async move {
                    Ok(pb::EnvironmentDirectoryFrame {
                        epoch: 1,
                        revision: 1,
                        reset: true,
                        upserts: vec![pb::PublishedEnvironment {
                            record: owner.record,
                            node_address: owner.node_address,
                            relay_address: owner.relay_address,
                            compatibility: None,
                        }],
                        ..Default::default()
                    })
                })
                .chain(futures_util::stream::pending()),
            )))
        }
    },
    []
);
use futures_util::StreamExt;

service_impl!(pb::node_service_server::NodeService, {
    async fn pause_environment(
        &self,
        _: Request<pb::PauseEnvironmentRequest>,
    ) -> Result<Response<pb::EnvironmentResult>, Status> {
        let mut state = self.0.lock().unwrap();
        state.node_calls += 1;
        if state.unsupported_pause {
            return Err(adx_protocol::status(
                adx_core::Error::UnsupportedCapability(
                    adx_core::UnsupportedCapability::CheckpointRestore,
                ),
            ));
        }
        Err(Status::unimplemented("unused test RPC"))
    }

    async fn create_snapshot(
        &self,
        _: Request<pb::CreateSnapshotRequest>,
    ) -> Result<Response<pb::CreateSnapshotResponse>, Status> {
        let mut state = self.0.lock().unwrap();
        state.node_calls += 1;
        if state.unsupported_snapshot {
            return Err(adx_protocol::status(
                adx_core::Error::UnsupportedCapability(
                    adx_core::UnsupportedCapability::CheckpointRestore,
                ),
            ));
        }
        Err(Status::unimplemented("unused test RPC"))
    }

    async fn update_network_policy(
        &self,
        request: Request<pb::UpdateNetworkPolicyRequest>,
    ) -> Result<Response<pb::EnvironmentResult>, Status> {
        let request = request.into_inner();
        let mut state = self.0.lock().unwrap();
        state.node_calls += 1;
        if state.fail_node {
            return Err(Status::unavailable("injected node failure"));
        }
        let record = {
            let record = state.owner.record.as_mut().unwrap();
            assert_eq!(request.assignment, record.assignment);
            assert_eq!(request.expected_revision, record.revision);
            record.revision += 1;
            record.spec.as_mut().unwrap().execution.as_mut().unwrap().network = request.network.clone();
            record.last_operation = Some(pb::CompletedOperation {
                id: request.operation_id.clone(),
                kind: pb::LifecycleKind::Network as i32,
                expected_revision: request.expected_revision,
            });
            record.clone()
        };
        state.last_request = Some(request);
        Ok(Response::new(pb::EnvironmentResult {
            newly_created: false,
            record: Some(record),
            durability: pb::Durability::Published as i32,
         ..Default::default()}))
    }
}, [
    get_environment_control(NodeGetEnvironmentControlRequest) -> EnvironmentControlRecord,
    batch_get_environment_control(NodeBatchGetEnvironmentControlRequest) -> BatchGetEnvironmentControlResponse,
    update_environment_control(NodeUpdateEnvironmentControlRequest) -> EnvironmentControlRecord,
    create_local_environment(LocalEnvironmentCreateRequest) -> EnvironmentResult,
    apply_node_upgrade_control(NodeUpgradeControl) -> NodeUpgradeInspection,
    inspect_node_upgrade(NodeUpgradeInspectRequest) -> NodeUpgradeInspection,
    wake_node_reconciliation(WakeNodeReconciliationRequest) -> WakeNodeReconciliationResponse,
    recover_environment(RecoverEnvironmentRequest) -> EnvironmentResult,
    get_session(GetNodeSessionRequest) -> GetNodeSessionResponse,
    create_environment(StartAssignedEnvironmentRequest) -> EnvironmentResult,
    delete_environment(DeleteEnvironmentRequest) -> EnvironmentResult,
    resume_environment(ResumeEnvironmentRequest) -> EnvironmentResult,
    ensure_environment_running(EnsureEnvironmentRunningRequest) -> EnvironmentResult,
    reload_environment(ReloadEnvironmentRequest) -> EnvironmentResult,
    collect_snapshot(CollectSnapshotRequest) -> CollectSnapshotResponse,
    collect_checkpoint_cleanup(CollectCheckpointCleanupRequest) -> CollectCheckpointCleanupResponse,
]);

struct Fixture {
    api: Arc<Api>,
    state: Arc<Mutex<State>>,
    grpc: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.grpc.abort();
    }
}

impl Fixture {
    async fn new() -> Self {
        adx_transport::install_crypto_provider();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let state = Arc::new(Mutex::new(State {
            owner: owner(&address),
            lookups: 0,
            node_calls: 0,
            fail_node: false,
            unsupported_pause: false,
            unsupported_snapshot: false,
            bump_on_lookup: None,
            last_request: None,
        }));
        let service = Service(state.clone());
        let grpc = tokio::spawn(async move {
            let incoming = futures_util::stream::unfold(listener, |listener| async {
                Some((listener.accept().await.map(|(socket, _)| socket), listener))
            });
            tonic::transport::Server::builder()
                .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(service.clone()))
                .add_service(pb::auth_service_server::AuthServiceServer::new(service.clone()))
                .add_service(pb::environment_directory_service_server::EnvironmentDirectoryServiceServer::new(service.clone()))
                .add_service(pb::node_service_server::NodeServiceServer::new(service))
                .serve_with_incoming(incoming).await.unwrap();
        });
        let config: Config = serde_json::from_value(json!({
            "listen":"127.0.0.1:0", "coordinator_address": address,
            "internal_security":"network", "ingress_mode":"standalone",
            "rpc_timeout_seconds":2, "cache_entries":8, "auth_cache_ttl_seconds":1,
            "e2b":{"domain":"sandbox.test", "envdVersion":"0.9.0",  "envdIngressSources":["10.0.0.2"]},
            "runtime_profile":{
                "rootfs":{"runtime_class":"runsc","type":"local","path":"/rootfs","readonly":true},
                "bootstrap":{"type":"erofs","root":"/rootfs","target":"/rootfs","entrypoint":["/rootfs/init"]},
                "env":{"ADX_ENVD_PATH":"/usr/local/bin/envd","ADX_ENVD_SHA256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","ADX_ENVD_DEFAULT_USER":"user","ADX_ENVD_DEFAULT_WORKDIR":"/workspace"}
            }
        })).unwrap();
        let clients = Clients::new(config).unwrap();
        let api = Api::new(clients).unwrap();
        // Native operations read the watched directory. Observe its initial
        // publication before submitting any mutation; E2B uses a fresh lookup.
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                match api.environment_service.inspect("tenant", "sandbox").await {
                    Ok(Some(_)) => break,
                    Err(error) if error.code() == tonic::Code::Unavailable => {
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    }
                    result => panic!("unexpected fixture directory state: {result:?}"),
                }
            }
        })
        .await
        .expect("fixture environment publication was not observed");
        Self { api, state, grpc }
    }

    async fn request(&self, method: &str, path: &str, body: &str) -> (u16, String) {
        let headers = [
            ("X-API-Key", "k".repeat(32)),
            ("Content-Type", "application/json".into()),
        ];
        let response = send_http_request(self.api.clone(), method, path, &headers, body).await;
        let status = response.status().as_u16();
        (status, String::from_utf8(response.into_body()).unwrap())
    }

    async fn put(&self, body: &str) -> (u16, String) {
        self.request("PUT", "/sandboxes/sandbox/network", body)
            .await
    }

    async fn checkpoint(&self, path: &str) -> (u16, String) {
        self.request("POST", path, r#"{"memory":true}"#).await
    }
}

fn owner(address: &str) -> pb::GetEnvironmentResponse {
    let network = pb::NetworkPolicy {
        traffic: Some(pb::TrafficPolicy {
            ingress_default_action: pb::NetworkAction::Allow as i32,
            egress_default_action: pb::NetworkAction::Deny as i32,
            mode: pb::TrafficMode::Stateless as i32,
            rules: vec![pb::NetworkRule {
                action: pb::NetworkAction::Allow as i32,
                direction: pb::NetworkDirection::Both as i32,
                protocol: pb::NetworkProtocol::Tcp as i32,
                peer: Some(pb::NetworkPeer {
                    cidr: "10.0.0.0/8".into(),
                    ..Default::default()
                }),
                priority: 7,
                ..Default::default()
            }],
        }),
        dns: Some(pb::DnsPolicy {
            default_action: pb::NetworkAction::Allow as i32,
            rules: vec![pb::DnsRule {
                action: pb::NetworkAction::Deny as i32,
                pattern: "blocked.test".into(),
            }],
        }),
    };
    pb::GetEnvironmentResponse {
        record: Some(pb::EnvironmentRecord {
            spec: Some(pb::EnvironmentSpec {
                id: "sandbox".into(),
                tenant_id: "tenant".into(),
                execution: Some(pb::ExecutionOptions {
                    network: Some(network),
                    ..Default::default()
                }),
                compatibility: Some(pb::EnvironmentCompatibility {
                    template_id: "base".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            assignment: Some(pb::Assignment {
                environment_id: "sandbox".into(),
                node_id: "node".into(),
                generation: 1,
                ..Default::default()
            }),
            state: pb::EnvironmentState::Running as i32,
            revision: 9,
            resources_held: true,
            ..Default::default()
        }),
        node_address: address.into(),
        relay_address: "127.0.0.1:9".into(),
    }
}

#[tokio::test]
async fn native_network_update_keeps_typed_policy_and_operation_fence() {
    let fixture = Fixture::new().await;
    fixture
        .api
        .environment_service
        .execute(
            adx_apiserver::operations::Kind::Network,
            "sandbox",
            "native-network-operation",
            json!({"schemaVersion":2,"traffic":{
                "ingressDefaultAction":"deny","egressDefaultAction":"allow",
                "rules":[],"mode":"stateful"
            }}),
            &pb::CallerContext {
                tenant_id: "tenant".into(),
                administrator: false,
            },
        )
        .await
        .unwrap();
    let state = fixture.state.lock().unwrap();
    let request = state.last_request.as_ref().unwrap();
    assert_eq!(request.operation_id, "native-network-operation");
    assert_eq!(request.expected_revision, 9);
    let traffic = request.network.as_ref().unwrap().traffic.as_ref().unwrap();
    assert_eq!(
        traffic.ingress_default_action,
        pb::NetworkAction::Deny as i32
    );
    assert_eq!(
        traffic.egress_default_action,
        pb::NetworkAction::Allow as i32
    );
}

#[tokio::test]
async fn typed_network_retry_rejects_changed_policy_with_the_same_operation_id() {
    let fixture = Fixture::new().await;
    let mut policy: adx_core::execution::NetworkPolicy = {
        let state = fixture.state.lock().unwrap();
        state
            .owner
            .record
            .as_ref()
            .unwrap()
            .spec
            .as_ref()
            .unwrap()
            .execution
            .as_ref()
            .unwrap()
            .network
            .clone()
            .unwrap()
            .try_into()
            .unwrap()
    };
    fixture.state.lock().unwrap().fail_node = true;
    let caller = pb::CallerContext {
        tenant_id: "tenant".into(),
        administrator: false,
    };
    let service = &fixture.api.environment_service;
    let first = service
        .update_network_at_revision(
            "sandbox",
            "same-network-operation",
            Some(policy.clone()),
            &caller,
            9,
        )
        .await
        .unwrap_err();
    assert_eq!(first.code(), tonic::Code::Unavailable);
    policy.traffic.as_mut().unwrap().egress_default_action =
        adx_core::execution::NetworkAction::Allow;
    let changed = service
        .update_network_at_revision(
            "sandbox",
            "same-network-operation",
            Some(policy),
            &caller,
            9,
        )
        .await
        .unwrap_err();
    assert_eq!(changed.code(), tonic::Code::FailedPrecondition);
    assert_eq!(fixture.state.lock().unwrap().node_calls, 2);
}

#[tokio::test]
async fn put_network_projects_egress_and_preserves_native_policy() {
    let fixture = Fixture::new().await;
    assert_eq!(
        fixture
            .put(r#"{"allowOut":["1.2.3.4"],"denyOut":["10.0.0.0/8"]}"#)
            .await
            .0,
        204
    );
    let state = fixture.state.lock().unwrap();
    let request = state.last_request.as_ref().unwrap();
    assert_eq!(request.expected_revision, 9);
    let policy = request.network.as_ref().unwrap();
    let traffic = policy.traffic.as_ref().unwrap();
    assert_eq!(
        traffic.ingress_default_action,
        pb::NetworkAction::Allow as i32
    );
    assert_eq!(
        traffic.egress_default_action,
        pb::NetworkAction::Allow as i32
    );
    assert_eq!(traffic.mode, pb::TrafficMode::Stateless as i32);
    assert_eq!(
        traffic.rules[0].direction,
        pb::NetworkDirection::Ingress as i32
    );
    assert_eq!(traffic.rules[1].priority, 200);
    assert_eq!(traffic.rules[2].priority, 100);
    assert_eq!(
        policy.dns.as_ref().unwrap().rules[0].pattern,
        "blocked.test"
    );
}

#[tokio::test]
async fn put_network_maps_domains_to_native_egress_peers() {
    let fixture = Fixture::new().await;
    let (status, response) = fixture
        .put(r#"{"allowOut":["API.Example.test.","*.BÜCHER.example.","1.2.3.4"],"denyOut":["0.0.0.0/0"],"allow_internet_access":false}"#)
        .await;
    assert_eq!(status, 204, "{response}");
    let state = fixture.state.lock().unwrap();
    assert_eq!(state.node_calls, 1);
    let request = state.last_request.as_ref().unwrap();
    assert_eq!(request.expected_revision, 9);
    let policy = request.network.as_ref().unwrap();
    let traffic = policy.traffic.as_ref().unwrap();
    assert_eq!(traffic.mode, pb::TrafficMode::Stateless as i32);
    assert_eq!(
        traffic.ingress_default_action,
        pb::NetworkAction::Allow as i32
    );
    assert_eq!(
        traffic.egress_default_action,
        pb::NetworkAction::Deny as i32
    );
    assert_eq!(traffic.rules.len(), 5);
    assert_eq!(
        traffic.rules[0].direction,
        pb::NetworkDirection::Ingress as i32
    );
    for (rule, domain) in traffic.rules[1..3]
        .iter()
        .zip(["api.example.test", "*.xn--bcher-kva.example"])
    {
        assert_eq!(rule.direction, pb::NetworkDirection::Egress as i32);
        assert_eq!(rule.action, pb::NetworkAction::Allow as i32);
        assert_eq!(rule.priority, 200);
        let peer = rule.peer.as_ref().unwrap();
        assert_eq!(peer.domain, domain);
        assert!(peer.cidr.is_empty());
    }
    assert_eq!(traffic.rules[3].peer.as_ref().unwrap().cidr, "1.2.3.4/32");
    assert_eq!(traffic.rules[4].action, pb::NetworkAction::Deny as i32);
    assert_eq!(traffic.rules[4].priority, 100);
    assert_eq!(
        policy.dns.as_ref().unwrap().rules[0].pattern,
        "blocked.test"
    );
}

#[tokio::test]
async fn malformed_network_destinations_do_not_reach_node() {
    for value in [
        "*",
        "api.*.example.test",
        "https://example.test",
        "example.test/path",
        "example.test:443",
        "example..test",
        "1.2.3.4/33",
    ] {
        let fixture = Fixture::new().await;
        let (status, response) = fixture.put(&json!({"allowOut": [value]}).to_string()).await;
        assert_eq!(status, 400, "{value}: {response}");
        let body: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(body["error_code"], "invalid_request", "{value}: {response}");
        assert_eq!(fixture.state.lock().unwrap().node_calls, 0);
    }
}

#[tokio::test]
async fn empty_network_clears_egress_and_false_defaults_deny() {
    let fixture = Fixture::new().await;
    assert_eq!(fixture.put("{}").await.0, 204);
    {
        let state = fixture.state.lock().unwrap();
        let traffic = state
            .last_request
            .as_ref()
            .unwrap()
            .network
            .as_ref()
            .unwrap()
            .traffic
            .as_ref()
            .unwrap();
        assert_eq!(traffic.rules.len(), 1);
        assert_eq!(
            traffic.egress_default_action,
            pb::NetworkAction::Allow as i32
        );
    }
    let fixture = Fixture::new().await;
    assert_eq!(
        fixture.put(r#"{"allow_internet_access":false}"#).await.0,
        204
    );
    assert_eq!(
        fixture
            .state
            .lock()
            .unwrap()
            .last_request
            .as_ref()
            .unwrap()
            .network
            .as_ref()
            .unwrap()
            .traffic
            .as_ref()
            .unwrap()
            .egress_default_action,
        pb::NetworkAction::Deny as i32
    );
}

#[tokio::test]
async fn unsupported_network_forms_do_not_reach_node() {
    for body in [
        r#"{"egressProxy":"x"}"#,
        r#"{"rules":{}}"#,
        r#"{"denyOut":["example.test"]}"#,
        r#"{"denyOut":["*.example.test"]}"#,
        r#"{"denyOut":["2001:db8::1"]}"#,
    ] {
        let fixture = Fixture::new().await;
        let (status, response) = fixture.put(body).await;
        assert_eq!(status, 400, "{response}");
        let body: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(body["error_code"], "capability_not_supported", "{response}");
        assert_eq!(fixture.state.lock().unwrap().node_calls, 0);
    }
}

#[tokio::test]
async fn node_failure_is_not_reported_as_no_content() {
    let fixture = Fixture::new().await;
    fixture.state.lock().unwrap().fail_node = true;
    assert_ne!(fixture.put("{}").await.0, 204);
    // Native operation replay retries one unavailable response with the same
    // fenced operation; neither failure is converted to an HTTP success.
    assert_eq!(fixture.state.lock().unwrap().node_calls, 2);
}

#[tokio::test]
async fn checkpoint_capability_rejection_preserves_the_e2b_error_contract() {
    for (path, pause) in [
        ("/sandboxes/sandbox/pause", true),
        ("/sandboxes/sandbox/snapshots", false),
    ] {
        let fixture = Fixture::new().await;
        if pause {
            fixture.state.lock().unwrap().unsupported_pause = true;
        } else {
            fixture.state.lock().unwrap().unsupported_snapshot = true;
        }
        let (status, body) = fixture.checkpoint(path).await;
        assert_eq!(status, 400, "{path}: {body}");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&body).unwrap(),
            json!({
                "code": 400,
                "message": "capability not supported: checkpoint_restore",
                "error_code": "capability_not_supported",
            })
        );
        assert_eq!(fixture.state.lock().unwrap().node_calls, 1, "{path}");
    }
}

#[tokio::test]
async fn revision_race_between_merge_and_execute_returns_conflict_without_node_call() {
    let fixture = Fixture::new().await;
    fixture.state.lock().unwrap().bump_on_lookup = Some(2);
    assert_eq!(fixture.put("{}").await.0, 409);
    let state = fixture.state.lock().unwrap();
    assert_eq!(state.lookups, 2);
    assert_eq!(state.node_calls, 0);
}

#[tokio::test]
async fn paused_sandbox_is_rejected_before_node_mutation() {
    let fixture = Fixture::new().await;
    fixture
        .state
        .lock()
        .unwrap()
        .owner
        .record
        .as_mut()
        .unwrap()
        .state = pb::EnvironmentState::Paused as i32;
    assert_eq!(fixture.put("{}").await.0, 409);
    assert_eq!(fixture.state.lock().unwrap().node_calls, 0);
}
