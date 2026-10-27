//! Real Redis and in-process control/RPC integration. Runtime execution and
//! business readiness are fakes; these tests do not certify a real sandbox.
mod common;
use adx_coordinator::{
    auth::{AuthRpc, Credential},
    rpc::CoordinatorRpc,
    storage::{prewarm::template_pool_id, Session},
    Placement,
};
use adx_core::{
    prewarm::{management::*, *},
    EnvironmentRecord, EnvironmentSpec, Resources, Result,
};
use adx_protocol::{
    auth::{Peers, Principal},
    control as pb,
};
use adx_transport::rpc::RpcClient;
use adxlet::{
    prewarm::*, rpc::NodeRpc, Adxlet, Durability, Readiness, Routes, RuntimeDriver, StateSink,
};
use pb::{coordinator_service_server::CoordinatorService, prewarm_service_server::PrewarmService};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tonic::{Request, Response, Status};

fn node_request<T>(value: T) -> Request<T> {
    let mut request = Request::new(value);
    request
        .metadata_mut()
        .insert("adx-component", "node".parse().unwrap());
    request.metadata_mut().insert_bin(
        "adx-node-id-bin",
        tonic::metadata::MetadataValue::from_bytes(b"node"),
    );
    request
}
fn api_request<T>(value: T) -> Request<T> {
    let mut request = Request::new(value);
    request
        .metadata_mut()
        .insert("adx-component", "apiserver".parse().unwrap());
    request
}
struct Backend {
    cache: Arc<ArtifactCache>,
    running: Mutex<BTreeSet<String>>,
    starts: AtomicUsize,
    fail_next: AtomicBool,
}
#[async_trait::async_trait]
impl RuntimeDriver for Backend {
    async fn start(
        &self,
        _: &EnvironmentSpec,
        id: &str,
        _: u64,
        _: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<std::net::IpAddr> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(adx_core::Error::Invalid("fixture start rejected".into()));
        }
        self.running.lock().unwrap().insert(id.into());
        Ok("10.0.0.2".parse().unwrap())
    }
    async fn is_running(&self, id: &str) -> Result<bool> {
        Ok(self.running.lock().unwrap().contains(id))
    }
    async fn remove(&self, id: &str) -> Result<()> {
        self.running.lock().unwrap().remove(id);
        self.cache.release(id).await
    }
}
struct Checks;
#[async_trait::async_trait]
impl Readiness for Checks {
    async fn wait_ready(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
}
#[async_trait::async_trait]
impl Routes for Checks {
    async fn activate(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
    async fn retire(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
}
struct Sink(CoordinatorRpc);
#[async_trait::async_trait]
impl StateSink for Sink {
    async fn commit(&self, record: &EnvironmentRecord) -> Result<Durability> {
        self.0
            .commit_environment(node_request(pb::CommitEnvironmentRequest {
                node_session_id: "boot".into(),
                record: Some(record.clone().try_into()?),
                stopped_runtime_ids: vec![],
            }))
            .await
            .map_err(|e| adx_core::Error::Unavailable(e.to_string()))?;
        Ok(Durability::Published)
    }
}
#[derive(Clone)]
struct ArtifactService {
    node: NodeRpc,
    lose_prepare_ack: Arc<AtomicBool>,
}
#[tonic::async_trait]
impl pb::preparation_node_service_server::PreparationNodeService for ArtifactService {
    async fn artifact(
        &self,
        r: Request<pb::ArtifactRequest>,
    ) -> std::result::Result<Response<pb::ArtifactResponse>, Status> {
        let prepare = matches!(
            serde_json::from_slice::<ArtifactCommand>(&r.get_ref().command_json).unwrap(),
            ArtifactCommand::Prepare { .. }
        );
        let answer =
            pb::preparation_node_service_server::PreparationNodeService::artifact(&self.node, r)
                .await?;
        if prepare && self.lose_prepare_ack.swap(false, Ordering::SeqCst) {
            Err(Status::unavailable("prepare acknowledgement lost"))
        } else {
            Ok(answer)
        }
    }
}
struct Rig {
    _redis: common::Redis,
    _files: tempfile::TempDir,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    session: Session,
    rpc: CoordinatorRpc,
    node: NodeRpc,
    cache: Arc<ArtifactCache>,
    backend: Arc<Backend>,
    mutation: Mutation,
    profile: PreparationProfile,
    lose: Arc<AtomicBool>,
    http: String,
}
impl Drop for Rig {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}
impl Rig {
    async fn new() -> Self {
        adx_transport::install_crypto_provider();
        let redis = common::Redis::new().await;
        let session = redis.store().await.begin(1).await.unwrap();
        let rpc = CoordinatorRpc::new(
            session.clone(),
            Placement::Pack,
            Peers::network(),
            RpcClient::network(Principal::Coordinator),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        let files = tempfile::tempdir().unwrap();
        let mut bytes = vec![0; 4096];
        bytes[1024..1028].copy_from_slice(&[0xe2, 0xe1, 0xf5, 0xe0]);
        std::fs::write(files.path().join("source.img"), &bytes).unwrap();
        let image = format!("app@sha256:{}", "a".repeat(64));
        let profile = PreparationProfile {
            source_ref: SourceRef {
                tenant: "tenant".into(),
                template: "app".into(),
                version: "1".into(),
                template_digest: format!("sha256:{}", "b".repeat(64)),
            },
            image: image.clone(),
            runtime_class: "runc".into(),
            resources: Resources {
                cpu_millis: 100,
                memory_bytes: 1048576,
                disk_bytes: 0,
            },
            architecture: if std::env::consts::ARCH == "aarch64" {
                "arm64"
            } else {
                "amd64"
            }
            .into(),
            runtime_recipe_digest: fingerprint(
                &Option::<adx_core::runtime_profile::RuntimeProfile>::None,
            )
            .unwrap(),
            security_digest: format!("sha256:{}", "c".repeat(64)),
            late_binding_contract_digest: format!("sha256:{}", "d".repeat(64)),
            contract_version: 1,
        };
        let cache = Arc::new(
            ArtifactCache::new(
                ArtifactCacheConfig {
                    root: files.path().join("cache"),
                    max_disk_bytes: 32768,
                    sources: vec![ArtifactSource {
                        image,
                        path: files.path().join("source.img"),
                        sha256: format!("sha256:{:x}", Sha256::digest(bytes)),
                        architecture: if std::env::consts::ARCH == "aarch64" {
                            "arm64"
                        } else {
                            "amd64"
                        }
                        .into(),
                        runtime_classes: vec!["runc".into()],
                        process: ImageProcess {
                            version: 1,
                            args: vec!["/app".into()],
                            cwd: "/".into(),
                            user: "0:0".into(),
                        },
                        env: BTreeMap::new(),
                    }],
                },
                None,
            )
            .unwrap(),
        );
        let backend = Arc::new(Backend {
            cache: cache.clone(),
            running: Mutex::default(),
            starts: AtomicUsize::new(0),
            fail_next: AtomicBool::new(false),
        });
        let manager = Arc::new(Adxlet::new(
            "node".into(),
            backend.clone(),
            Arc::new(Checks),
            Arc::new(Checks),
            Arc::new(Sink(rpc.clone())),
        ));
        let capacity = Resources {
            cpu_millis: 10000,
            memory_bytes: 104857600,
            disk_bytes: 104857600,
        };
        manager
            .update_capacity(capacity, Duration::from_secs(60))
            .unwrap();
        let node = NodeRpc::new(manager, Peers::network(), "boot".into())
            .with_artifact_cache(cache.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let loss = Arc::new(AtomicBool::new(false));
        let service = ArtifactService {
            node: node.clone(),
            lose_prepare_ack: loss.clone(),
        };
        let n = node.clone();
        let node_server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(pb::node_service_server::NodeServiceServer::new(n))
                .add_service(
                    pb::preparation_node_service_server::PreparationNodeServiceServer::new(service),
                )
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let mut report = pb::RegisterNodeRequest {
            node_id: "node".into(),
            node_address: address.clone(),
            proxy_address: address,
            runtime_classes: vec!["runc".into()],
            capacity: Some(capacity.into()),
            session_id: "boot".into(),
            heartbeat_sequence: 1,
            reconciling: true,
            ..Default::default()
        };
        rpc.register_node(node_request(report.clone()))
            .await
            .unwrap();
        rpc.inspect_node(node_request(pb::InspectNodeRequest {
            node_id: "node".into(),
            session_id: "boot".into(),
        }))
        .await
        .unwrap();
        report.heartbeat_sequence = 2;
        report.reconciling = false;
        report.accepting_allocations = true;
        rpc.register_node(node_request(report)).await.unwrap();
        let mutation = Mutation {
            pool_id: template_pool_id(&profile.source_ref).unwrap(),
            request_id: "configure".into(),
            request_path: "/prewarm-test".into(),
            condition: Condition::Absent,
            command: Command::Put {
                profile: Box::new(profile.clone()),
                policy: PoolPolicy {
                    mode: PoolMode::ImageRootfs,
                    enabled: true,
                    capacity: PoolCapacity {
                        unit: CapacityUnit::ArtifactNodeReplicas,
                        min_ready: 1,
                        target_ready: 1,
                        max_live: 1,
                    },
                    refill: RefillPolicy {
                        prepare_concurrency: 1,
                        initial_backoff_seconds: 1,
                        max_backoff_seconds: 1,
                    },
                    health: HealthPolicy {
                        interval_seconds: 1,
                        max_staleness_seconds: 30,
                    },
                    ready_ttl_seconds: 300,
                    acquire_wait_ms: 0,
                    idle_budget: IdleBudget {
                        cpu_millis: None,
                        memory_mib: None,
                        disk_bytes: 32768,
                    },
                },
            },
        };
        let control_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let control_address = control_listener.local_addr().unwrap();
        session
            .bootstrap_credential(
                &"a".repeat(32),
                &Credential {
                    tenant_id: "tenant".into(),
                    administrator: true,
                    expires_at_unix_seconds: 0,
                },
            )
            .await
            .unwrap();
        session
            .bootstrap_credential(
                &"b".repeat(32),
                &Credential {
                    tenant_id: "tenant".into(),
                    administrator: false,
                    expires_at_unix_seconds: 0,
                },
            )
            .await
            .unwrap();
        let auth = AuthRpc::new(session.clone(), Peers::network());
        let c = rpc.clone();
        let control_server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    pb::coordinator_service_server::CoordinatorServiceServer::new(c.clone()),
                )
                .add_service(pb::prewarm_service_server::PrewarmServiceServer::new(c))
                .add_service(pb::auth_service_server::AuthServiceServer::new(auth))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                    control_listener,
                ))
                .await
                .unwrap();
        });
        let config=serde_json::from_value(json!({"listen":"127.0.0.1:0","coordinator_address":control_address.to_string(),"internal_security":"network","ingress_mode":"standalone","loopback_http":true,"rpc_timeout_seconds":3,"cache_entries":16,"auth_cache_ttl_seconds":1})).unwrap();
        let api =
            adx_apiserver::http::Api::new(adx_apiserver::clients::Clients::new(config).unwrap())
                .unwrap();
        let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http = format!("http://{}", http_listener.local_addr().unwrap());
        let http_server = tokio::spawn(async move {
            loop {
                let (stream, _) = http_listener.accept().await.unwrap();
                let api = api.clone();
                tokio::spawn(async move {
                    hyper::server::conn::http1::Builder::new()
                        .serve_connection(
                            hyper_util::rt::TokioIo::new(stream),
                            hyper::service::service_fn(move |r| api.clone().serve(r)),
                        )
                        .await
                        .unwrap();
                });
            }
        });
        Self {
            _redis: redis,
            _files: files,
            tasks: vec![node_server, control_server, http_server],
            session,
            rpc,
            node,
            cache,
            backend,
            mutation,
            profile,
            lose: loss,
            http,
        }
    }
    async fn mutate(&self, mutation: &Mutation) -> Accepted {
        let response = self
            .rpc
            .mutate_pool(api_request(pb::PrewarmMutationRequest {
                caller: Some(pb::CallerContext {
                    tenant_id: "tenant".into(),
                    administrator: true,
                }),
                mutation_json: serde_json::to_vec(mutation).unwrap(),
                contract_version: 1,
            }))
            .await
            .unwrap()
            .into_inner();
        serde_json::from_slice(&response.json).unwrap()
    }
    async fn pool(&self) -> PoolRecord {
        self.session
            .get_pool("tenant", &self.mutation.pool_id)
            .await
            .unwrap()
    }
    fn create_request(&self, id: &str) -> pb::CreateEnvironmentRequest {
        let spec = EnvironmentSpec {
            id: id.into(),
            tenant_id: "tenant".into(),
            image: self.profile.image.clone(),
            runtime_class: "runc".into(),
            resources: self.profile.resources,
            priority: 0,
            env: BTreeMap::from([("USER_KEY".into(), format!("private-{id}"))]),
            runtime_profile: None,
            snapshot_id: None,
            lifecycle: Default::default(),
            execution: adx_core::execution::ExecutionOptions {
                inherit_entrypoint: true,
                rootfs: Some(adx_core::execution::Rootfs {
                    readonly: false,
                    source: adx_core::execution::StorageSource::Image(self.profile.image.clone()),
                }),
                ..Default::default()
            },
            scheduling: Default::default(),
        };
        pb::CreateEnvironmentRequest {
            spec: Some(spec.into()),
            caller: Some(pb::CallerContext {
                tenant_id: "tenant".into(),
                administrator: false,
            }),
            preparation_context: Some(
                PreparationContext {
                    source_ref: self.profile.source_ref.clone(),
                    preference: PreparationPreference::Prefer,
                    contract_version: 1,
                }
                .into(),
            ),
            create_timeout_seconds: 10,
            ..Default::default()
        }
    }
    async fn create(&self, id: &str) -> pb::EnvironmentRecord {
        self.rpc
            .create_environment(api_request(self.create_request(id)))
            .await
            .unwrap()
            .into_inner()
            .record
            .unwrap()
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn refill_shared_artifact_bind_unique_environments_and_drain_only_after_confirmed_stop() {
    use pb::node_service_server::NodeService;
    let rig = Box::pin(Rig::new()).await;
    rig.mutate(&rig.mutation).await;
    rig.rpc.reconcile_prewarm().await.unwrap();
    assert_eq!(rig.pool().await.artifacts[0].state, SlotState::Preparing);
    rig.rpc.reconcile_prewarm().await.unwrap();
    let pool = rig.pool().await;
    let id = pool.artifacts[0].id.clone();
    assert_eq!(pool.artifacts[0].state, SlotState::Ready);
    assert_eq!(rig.backend.starts.load(Ordering::SeqCst), 0);
    let (a, replay) = tokio::join!(rig.create("adx-a"), rig.create("adx-a"));
    assert_eq!(a, replay);
    let b = rig.create("adx-b").await;
    assert_ne!(
        a.runtime.as_ref().unwrap().id,
        b.runtime.as_ref().unwrap().id
    );
    assert_eq!(rig.backend.starts.load(Ordering::SeqCst), 2);
    assert_eq!(
        rig.session
            .preparation_attempt("tenant", "adx-a")
            .await
            .unwrap()
            .artifact_id,
        Some(id.clone())
    );
    assert!(!serde_json::to_string(&rig.pool().await)
        .unwrap()
        .contains("private-adx"));
    assert!(rig.cache.observe(&id).await.unwrap().in_use);
    let file = rig.cache.config().root.join(&id).join("rootfs.img");
    let bytes = std::fs::read(&file).unwrap();
    std::fs::remove_file(&file).unwrap();
    let mut retry = Request::new(pb::StartAssignedEnvironmentRequest {
        spec: a.spec.clone(),
        assignment: a.assignment.clone(),
        node_session_id: "boot".into(),
        prepared_artifact_id: id.clone(),
        ..Default::default()
    });
    retry
        .metadata_mut()
        .insert("adx-component", "coordinator".parse().unwrap());
    assert_eq!(
        rig.node.create_environment(retry).await.unwrap_err().code(),
        tonic::Code::Unavailable,
        "missing bytes with a live execution lease are not proof of Environment absence"
    );
    std::fs::write(file, bytes).unwrap();
    let deletion = Mutation {
        request_id: "delete".into(),
        condition: Condition::Revision(1),
        command: Command::Delete,
        ..rig.mutation.clone()
    };
    let accepted = rig.mutate(&deletion).await;
    assert_eq!(accepted.pool.phase, PoolPhase::Deleting);
    let cancel = Mutation {
        request_id: "cannot-cancel-delete".into(),
        condition: Condition::Revision(accepted.pool.revision),
        command: Command::Action {
            action: Action::Suspend,
        },
        ..rig.mutation.clone()
    };
    assert!(rig
        .rpc
        .mutate_pool(api_request(pb::PrewarmMutationRequest {
            caller: Some(pb::CallerContext {
                tenant_id: "tenant".into(),
                administrator: true
            }),
            mutation_json: serde_json::to_vec(&cancel).unwrap(),
            contract_version: 1
        }))
        .await
        .is_err());
    rig.rpc.reconcile_prewarm().await.unwrap();
    assert!(rig.pool().await.artifacts[0].size_bytes > 0);
    let operation = rig
        .session
        .get_prewarm_operation("tenant", &accepted.operation.operation_id)
        .await
        .unwrap();
    assert_eq!(operation.state, "reconciling");
    for record in [a, b] {
        let old_spec = record.spec.clone();
        let old_assignment = record.assignment.clone();
        rig.node
            .delete_environment(api_request(pb::DeleteEnvironmentRequest {
                assignment: record.assignment,
                caller: Some(pb::CallerContext {
                    tenant_id: "tenant".into(),
                    administrator: false,
                }),
                node_session_id: "boot".into(),
                failed_revision: None,
            }))
            .await
            .unwrap();
        let mut late = Request::new(pb::StartAssignedEnvironmentRequest {
            spec: old_spec.clone(),
            assignment: old_assignment.clone(),
            node_session_id: "boot".into(),
            prepared_artifact_id: id.clone(),
            ..Default::default()
        });
        late.metadata_mut()
            .insert("adx-component", "coordinator".parse().unwrap());
        assert!(
            rig.node.create_environment(late).await.is_err(),
            "deleted execution cannot pin/start again"
        );
        let candidate = adx_coordinator::storage::LocalClaim {
            request_id: String::new(),
            node_id: "node".into(),
            node_session_id: "boot".into(),
            devices: vec![],
        };
        let spec: EnvironmentSpec = old_spec.unwrap().try_into().unwrap();
        assert!(rig.session.claim(spec.clone(), &candidate).await.is_err());
        assert!(rig
            .session
            .claim_batch(vec![(spec, candidate)])
            .await
            .unwrap()[0]
            .is_err());
    }
    tokio::time::sleep(Duration::from_millis(1100)).await;
    rig.rpc.reconcile_prewarm().await.unwrap();
    assert!(rig
        .session
        .get_pool("tenant", &rig.mutation.pool_id)
        .await
        .is_err());
    assert_eq!(
        rig.session
            .get_prewarm_operation("tenant", &accepted.operation.operation_id)
            .await
            .unwrap()
            .state,
        "succeeded"
    );
    assert!(rig.cache.prepare(&id, &rig.profile, 32768).await.is_err());
    let metrics = rig.rpc.metrics().await.unwrap();
    assert!(metrics.contains("adx_prewarm_artifact_hit_requests_total"));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn lost_prepare_ack_quarantines_bytes_and_serves_cold_without_reassigning_the_artifact() {
    let mut rig = Box::pin(Rig::new()).await;
    rig.mutate(&rig.mutation).await;
    rig.rpc.reconcile_prewarm().await.unwrap();
    let id = rig.pool().await.artifacts[0].id.clone();
    rig.lose.store(true, Ordering::SeqCst);
    rig.rpc.reconcile_prewarm().await.unwrap();
    let pool = rig.pool().await;
    assert_eq!(pool.artifacts[0].state, SlotState::Quarantined);
    assert!(pool.artifacts[0].size_bytes > 0);
    rig.create("adx-cold").await;
    assert_eq!(
        rig.session
            .preparation_attempt("tenant", "adx-cold")
            .await
            .unwrap()
            .selection,
        Selection::Cold
    );
    assert!(!rig.cache.observe(&id).await.unwrap().in_use);
    assert_eq!(rig.pool().await.artifacts.len(), 1);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    rig.rpc.reconcile_prewarm().await.unwrap();
    let refill_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        rig.rpc.reconcile_prewarm().await.unwrap();
        let current = rig.pool().await;
        assert!(!current
            .artifacts
            .iter()
            .any(|a| a.id == id && a.state == SlotState::Ready));
        if current.artifacts.first().is_some_and(|a| a.id != id) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < refill_deadline,
            "refill did not converge after quarantine cleanup"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(rig.cache.prepare(&id, &rig.profile, 32768).await.is_err());
    let before = rig.pool().await;
    rig._redis.crash();
    rig._redis.start().await;
    let recovered = rig._redis.store().await.begin(1).await.unwrap();
    assert_eq!(
        recovered
            .get_pool("tenant", &rig.mutation.pool_id)
            .await
            .unwrap(),
        before
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn public_management_enforces_auth_preconditions_replay_and_unsupported_backend_mode() {
    let rig = Box::pin(Rig::new()).await;
    let client = reqwest::Client::new();
    let url = format!(
        "{}/api/environment/v1/prewarm-pools/{}",
        rig.http, rig.mutation.pool_id
    );
    assert_eq!(client.get(&url).send().await.unwrap().status(), 401);
    assert_eq!(
        client
            .get(&url)
            .bearer_auth("b".repeat(32))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let body = match &rig.mutation.command {
        Command::Put { profile, policy } => json!({"profile":profile,"policy":policy}),
        _ => unreachable!(),
    };
    let put = || {
        client
            .put(&url)
            .bearer_auth("a".repeat(32))
            .header("x-adx-request-id", "http-configure")
            .json(&body)
    };
    assert_eq!(put().send().await.unwrap().status(), 428);
    let first = put().header("if-none-match", "*").send().await.unwrap();
    assert_eq!(first.status(), 202);
    assert_eq!(first.headers()["etag"], "\"revision-1\"");
    let data: serde_json::Value = first.json().await.unwrap();
    let replay = put().header("if-none-match", "*").send().await.unwrap();
    assert_eq!(replay.status(), 202);
    assert_eq!(replay.json::<serde_json::Value>().await.unwrap(), data);
    let stale = client
        .post(format!("{url}/actions"))
        .bearer_auth("a".repeat(32))
        .header("x-adx-request-id", "suspend")
        .header("if-match", "\"revision-99\"")
        .json(&json!({"action":"suspend"}))
        .send()
        .await
        .unwrap();
    assert_eq!(stale.status(), 412);
    assert_eq!(
        stale.json::<serde_json::Value>().await.unwrap()["code"],
        412
    );
    let mut invalid = body;
    invalid["policy"]["mode"] = "runtime_slot".into();
    invalid["policy"]["capacity"]["unit"] = "slots".into();
    invalid["policy"]["idle_budget"]["cpu_millis"] = 100.into();
    invalid["policy"]["idle_budget"]["memory_mib"] = 1.into();
    let rejected = client
        .put(format!(
            "{}/api/environment/v1/prewarm-pools/unsupported",
            rig.http
        ))
        .bearer_auth("a".repeat(32))
        .header("x-adx-request-id", "unsupported")
        .header("if-none-match", "*")
        .json(&invalid)
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), 501);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn rejected_prepared_execution_keeps_its_terminal_identity_across_request_aliases() {
    let rig = Box::pin(Rig::new()).await;
    rig.mutate(&rig.mutation).await;
    rig.rpc.reconcile_prewarm().await.unwrap();
    rig.rpc.reconcile_prewarm().await.unwrap();
    rig.backend.fail_next.store(true, Ordering::SeqCst);
    let mut request = rig.create_request("adx-failed");
    request.request_id = "first-failed-request".into();
    assert!(rig
        .rpc
        .create_environment(api_request(request.clone()))
        .await
        .is_err());
    let stored = rig.session.get("adx-failed").await.unwrap();
    assert_eq!(
        stored.result.as_ref().unwrap().state,
        adx_core::EnvironmentState::Failed
    );
    let attempt = rig
        .session
        .preparation_attempt("tenant", "adx-failed")
        .await
        .unwrap();
    assert_eq!(attempt.assignment.as_ref(), Some(&stored.assignment));
    request.request_id = "other-request-alias".into();
    let replay = rig
        .rpc
        .create_environment(api_request(request))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert_eq!(replay.state, pb::EnvironmentState::Failed as i32);
    assert_eq!(rig.backend.starts.load(Ordering::SeqCst), 1);
    assert_eq!(
        rig.session.get("adx-failed").await.unwrap().assignment,
        stored.assignment
    );
}
