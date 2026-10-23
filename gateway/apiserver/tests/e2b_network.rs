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
    get_environment_metadata(GetEnvironmentMetadataRequest) -> EnvironmentMetadataRecord,
    list_environment_metadata(ListEnvironmentMetadataRequest) -> ListEnvironmentMetadataResponse,
    list_e2b_environment_metadata(ListEnvironmentMetadataRequest) -> ListEnvironmentMetadataResponse,
    ensure_running(EnsureRunningRequest) -> GetEnvironmentResponse,
    retire_dormant(GetEnvironmentRequest) -> EnvironmentResult,
    get_environment_control(GetEnvironmentControlRequest) -> EnvironmentControlRecord,
    update_environment_control(UpdateEnvironmentControlRequest) -> EnvironmentControlRecord,
    list_environment_controls(ListEnvironmentControlsRequest) -> ListEnvironmentControlsResponse,
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
            Ok(Response::new(Box::pin(
                futures_util::stream::once(async {
                    Ok(pb::EnvironmentDirectoryFrame {
                        epoch: 1,
                        revision: 1,
                        reset: true,
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
    async fn update_network_policy(&self, request: Request<pb::UpdateNetworkPolicyRequest>) -> Result<Response<pb::EnvironmentResult>, Status> {
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
            record: Some(record),
            durability: pb::Durability::Published as i32,
        }))
    }
}, [
    create_local_environment(LocalEnvironmentCreateRequest) -> EnvironmentResult,
    recover_environment(RecoverEnvironmentRequest) -> EnvironmentResult,
    get_session(GetNodeSessionRequest) -> GetNodeSessionResponse,
    create_environment(StartAssignedEnvironmentRequest) -> EnvironmentResult,
    delete_environment(DeleteEnvironmentRequest) -> EnvironmentResult,
    pause_environment(PauseEnvironmentRequest) -> EnvironmentResult,
    resume_environment(ResumeEnvironmentRequest) -> EnvironmentResult,
    ensure_environment_running(EnsureEnvironmentRunningRequest) -> EnvironmentResult,
    reload_environment(ReloadEnvironmentRequest) -> EnvironmentResult,
    create_snapshot(CreateSnapshotRequest) -> CreateSnapshotResponse,
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
            "e2b":{"domain":"sandbox.test", "envdVersion":"0.9.0",
            "templates":{"base":{"name":"base","version":"1"}}},
            "runtime_profile":{
                "rootfs":{"runtime_class":"runsc","type":"local","path":"/rootfs","readonly":true},
                "bootstrap":{"type":"erofs","root":"/rootfs","target":"/rootfs","entrypoint":["/rootfs/init"]},
                "env":{"ADX_ENVD_PATH":"/usr/local/bin/envd","ADX_ENVD_SHA256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","ADX_ENVD_DEFAULT_USER":"user","ADX_ENVD_DEFAULT_WORKDIR":"/workspace"}
            }
        })).unwrap();
        let clients = Clients::new(config).unwrap();
        Self {
            api: Api::new(clients).unwrap(),
            state,
            grpc,
        }
    }

    async fn put(&self, body: &str) -> (u16, String) {
        let headers = [
            ("X-API-Key", "k".repeat(32)),
            ("Content-Type", "application/json".into()),
        ];
        let response = send_http_request(
            self.api.clone(),
            "PUT",
            "/sandboxes/sandbox/network",
            &headers,
            body,
        )
        .await;
        let status = response.status().as_u16();
        (status, String::from_utf8(response.into_body()).unwrap())
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
        r#"{"allowOut":["example.test"]}"#,
        r#"{"denyOut":["2001:db8::1"]}"#,
    ] {
        let fixture = Fixture::new().await;
        let (status, response) = fixture.put(body).await;
        assert_eq!(status, 400, "{response}");
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
