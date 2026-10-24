//! Real Redis and gRPC; runtime memory and object storage are controlled fixtures.
mod common;
use adx_coordinator::{rpc::CoordinatorRpc, storage::Session, Placement};
use adx_core::{EnvironmentRecord, EnvironmentSpec, Resources, Result};
use adx_protocol::{
    auth::{Peers, Principal},
    control as pb,
};
use adx_transport::rpc::{RpcChannel, RpcClient};
use adxlet::{
    checkpoint::{CheckpointCooperation, ObjectCheckpointStore},
    rpc::{CoordinatorStateSink, NodeRpc},
    Adxlet, Readiness, Routes, RuntimeDriver,
};
use async_trait::async_trait;
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};

fn spec() -> EnvironmentSpec {
    EnvironmentSpec {
        id: "sleep".into(),
        tenant_id: "tenant".into(),
        image: "image".into(),
        runtime_class: "test-runtime".into(),
        resources: Resources {
            cpu_millis: 100,
            memory_bytes: 128,
            disk_bytes: 0,
        },
        lifecycle: adx_core::lifecycle::LifecyclePolicy {
            pause: Some(adx_core::lifecycle::PausePolicy {
                storage: adx_core::lifecycle::CheckpointStorage::Shared,
                ..Default::default()
            }),
            ..Default::default()
        },
        snapshot_id: None,
        runtime_profile: None,
        env: Default::default(),
        priority: 0,
        scheduling: Default::default(),
        execution: Default::default(),
    }
}
fn caller() -> Option<pb::CallerContext> {
    Some(pb::CallerContext {
        tenant_id: "tenant".into(),
        administrator: false,
    })
}

#[derive(Default)]
struct Backend {
    relay: Mutex<Option<Arc<data_plane_gateway::node::Relay>>>,
    versioned_routes: Mutex<Option<Arc<adxlet::routes::UdsRoutes>>>,
    running: Mutex<BTreeMap<String, adxlet::RuntimeObservation>>,
    restored: AtomicUsize,
    restore_gate: Mutex<Option<Arc<tokio::sync::Notify>>>,
    restore_entered: tokio::sync::Notify,
    fail_after_gate: AtomicBool,
}
#[async_trait]
impl RuntimeDriver for Backend {
    async fn inventory(&self) -> Result<Vec<adxlet::RuntimeObservation>> {
        Ok(self.running.lock().unwrap().values().cloned().collect())
    }
    async fn start(
        &self,
        spec: &EnvironmentSpec,
        runtime: &str,
        generation: u64,
        _: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<std::net::IpAddr> {
        self.running.lock().unwrap().insert(
            runtime.into(),
            adxlet::RuntimeObservation {
                environment_id: spec.id.clone(),
                tenant_id: spec.tenant_id.clone(),
                runtime_id: runtime.into(),
                generation,
                running: true,
            },
        );
        Ok("127.0.0.1".parse().unwrap())
    }
    async fn is_running(&self, runtime: &str) -> Result<bool> {
        Ok(self.running.lock().unwrap().contains_key(runtime))
    }
    async fn remove(&self, runtime: &str) -> Result<()> {
        self.running.lock().unwrap().remove(runtime);
        Ok(())
    }
    async fn checkpoint_supported(&self, _: &str) -> Result<()> {
        Ok(())
    }
    async fn checkpoint(&self, _: &str, path: &Path, _: Duration) -> Result<()> {
        std::fs::write(path.join("memory"), b"continued-state:42").unwrap();
        Ok(())
    }
    async fn restore_from(
        &self,
        spec: &EnvironmentSpec,
        runtime: &str,
        generation: u64,
        devices: &[adx_core::scheduling::DeviceAllocation],
        path: &Path,
        _: Option<&adx_core::runtime::RuntimeIdentity>,
    ) -> Result<std::net::IpAddr> {
        assert_eq!(
            std::fs::read(path.join("memory")).unwrap(),
            b"continued-state:42"
        );
        self.restored.fetch_add(1, Ordering::SeqCst);
        let ip = self.start(spec, runtime, generation, devices).await?;
        self.restore_entered.notify_one();
        let gate = self.restore_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.notified().await;
        }
        if self.fail_after_gate.load(Ordering::SeqCst) {
            return Err(adx_core::Error::Unavailable(
                "node process terminated during restore".into(),
            ));
        }
        Ok(ip)
    }
}
#[async_trait]
impl Readiness for Backend {
    async fn wait_ready(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
}
#[async_trait]
impl Routes for Backend {
    async fn begin_reconcile(&self) -> Result<()> {
        let routes = self.versioned_routes.lock().unwrap().clone();
        if let Some(routes) = routes {
            routes.begin_reconcile().await?;
        }
        Ok(())
    }
    async fn finish_reconcile(&self) -> Result<()> {
        let routes = self.versioned_routes.lock().unwrap().clone();
        if let Some(routes) = routes {
            routes.finish_reconcile().await?;
        }
        Ok(())
    }
    async fn activate(&self, record: &EnvironmentRecord) -> Result<()> {
        let routes = self.versioned_routes.lock().unwrap().clone();
        if let Some(routes) = routes {
            return routes.activate(record).await;
        }
        let relay = self.relay.lock().unwrap().clone();
        if let Some(relay) = relay {
            relay
                .activate_route(
                    record.spec.id.clone(),
                    record.runtime.id.clone(),
                    record.runtime.ip.unwrap(),
                )
                .await;
        }
        Ok(())
    }
    async fn retire(&self, record: &EnvironmentRecord) -> Result<()> {
        let routes = self.versioned_routes.lock().unwrap().clone();
        if let Some(routes) = routes {
            return routes.retire(record).await;
        }
        let relay = self.relay.lock().unwrap().clone();
        if let Some(relay) = relay {
            relay
                .retire_route(record.spec.id.clone(), record.runtime.id.clone())
                .await;
        }
        Ok(())
    }
    async fn retire_orphan(&self, _: &adxlet::RuntimeObservation) -> Result<()> {
        Ok(())
    }
}
#[async_trait]
impl CheckpointCooperation for Backend {
    async fn prepare(&self, _: &EnvironmentRecord, _: &str) -> Result<()> {
        Ok(())
    }
    async fn abort_unstarted(&self, _: &EnvironmentRecord, _: &str) -> Result<()> {
        Ok(())
    }
}

struct Worker {
    store: Arc<ObjectCheckpointStore>,
    sink: CoordinatorStateSink,
    manager: Arc<Adxlet>,
    backend: Arc<Backend>,
    report: pb::RegisterNodeRequest,
    task: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn channel(address: &str, principal: Principal) -> RpcChannel {
    RpcClient::network(principal).wrap(
        Channel::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap(),
    )
}
async fn worker(
    id: &str,
    coordinator: &str,
    remote: Arc<object_store::memory::InMemory>,
    address: Option<&str>,
) -> Worker {
    let dir = tempfile::tempdir().unwrap();
    let backend = Arc::new(Backend::default());
    let session = format!("{id}-{}", uuid::Uuid::new_v4());
    let sink = CoordinatorStateSink::with_rpc_channel(
        channel(coordinator, Principal::Node(id.into())).await,
        Duration::from_secs(2),
    )
    .unwrap()
    .with_session(session.clone());
    let store = Arc::new(
        ObjectCheckpointStore::new(
            "obs".into(),
            remote,
            "checkpoints".into(),
            dir.path().into(),
            1024,
        )
        .unwrap(),
    );
    let manager = Arc::new(
        Adxlet::new(
            id.into(),
            backend.clone(),
            backend.clone(),
            backend.clone(),
            Arc::new(sink.clone()),
        )
        .with_checkpointing(store.clone(), backend.clone())
        .unwrap(),
    );
    manager
        .update_capacity(spec().resources, Duration::from_secs(60))
        .unwrap();
    let listener = TcpListener::bind(address.unwrap_or("127.0.0.1:0"))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let node = NodeRpc::new(manager.clone(), Peers::network(), session.clone());
    let task = tokio::spawn(async move {
        Server::builder()
            .add_service(pb::node_service_server::NodeServiceServer::new(node))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    Worker {
        store,
        sink,
        manager,
        backend,
        task,
        _dir: dir,
        report: pb::RegisterNodeRequest {
            node_id: id.into(),
            node_address: address.to_string(),
            proxy_address: format!("{id}:9002"),
            capacity: Some(spec().resources.into()),
            runtime_classes: vec!["test-runtime".into()],
            labels: [("ADX_CHECKPOINT_STORAGE".into(), "obs".into())].into(),
            session_id: session,
            heartbeat_sequence: 1,
            reconciling: true,
            ..Default::default()
        },
    }
}
async fn register(address: &str, worker: &mut Worker) {
    worker.manager.pause_lifecycle().await;
    worker
        .sink
        .reconnect(channel(address, Principal::Node(worker.report.node_id.clone())).await);
    worker.report.reconciling = true;
    worker.report.accepting_allocations = false;
    let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(address, Principal::Node(worker.report.node_id.clone())).await,
    );
    client.register_node(worker.report.clone()).await.unwrap();
    let catalog = client
        .inspect_node(pb::InspectNodeRequest {
            node_id: worker.report.node_id.clone(),
            session_id: worker.report.session_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    worker
        .manager
        .reconcile(
            catalog
                .records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>>>()
                .unwrap(),
        )
        .await
        .unwrap();
    worker.report.heartbeat_sequence += 1;
    worker.report.reconciling = false;
    worker.report.accepting_allocations = true;
    client.register_node(worker.report.clone()).await.unwrap();
}
struct Rig {
    _redis: common::Redis,
    session: Session,
    rpc: CoordinatorRpc,
    routes: adx_coordinator::routes::RoutePublisher,
    address: String,
    server: tokio::task::JoinHandle<()>,
    remote: Arc<object_store::memory::InMemory>,
    source: Worker,
    target: Worker,
}
impl Drop for Rig {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Rig {
    async fn new(timeout: Duration) -> Self {
        Self::with_ttl(timeout, 3600).await
    }
    async fn with_ttl(timeout: Duration, ttl_seconds: u64) -> Self {
        let redis = common::Redis::new().await;
        let session = redis.store().await.begin(1).await.unwrap();
        let rpc = CoordinatorRpc::new(
            session.clone(),
            Placement::Pack,
            Peers::network(),
            RpcClient::network(Principal::Coordinator),
            timeout,
        )
        .await
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let service = rpc.clone();
        let routes =
            adx_coordinator::routes::RoutePublisher::new(session.clone(), Peers::network());
        routes.refresh().await.unwrap();
        let route_service = routes.clone();
        let auth_service = adx_coordinator::auth::AuthRpc::new(session.clone(), Peers::network());
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(pb::auth_service_server::AuthServiceServer::new(
                    auth_service,
                ))
                .add_service(pb::route_service_server::RouteServiceServer::new(
                    route_service,
                ))
                .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let remote = Arc::new(object_store::memory::InMemory::new());
        let mut source = worker("a", &address, remote.clone(), None).await;
        let mut target = worker("b", &address, remote.clone(), None).await;
        register(&address, &mut source).await;
        let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
            channel(&address, Principal::ApiServer).await,
        );
        let created = client
            .create_environment(pb::CreateEnvironmentRequest {
                spec: Some(spec().into()),
                caller: caller(),
                schedule_timeout_seconds: 3,
                create_timeout_seconds: 10,
                create_deadline_unix_millis: 0,
                expected_assignment: None,
            })
            .await
            .unwrap()
            .into_inner();
        let record = created.record.unwrap();
        let mut node = pb::node_service_client::NodeServiceClient::new(
            channel(&source.report.node_address, Principal::ApiServer).await,
        );
        node.pause_environment(pb::PauseEnvironmentRequest {
            assignment: record.assignment,
            caller: caller(),
            operation_id: "pause-original".into(),
            expected_revision: record.revision,
            retention: Some(adx_core::lifecycle::RetentionPolicy::Ttl { ttl_seconds }.into()),
            timeout_seconds: 10,
        })
        .await
        .unwrap();
        register(&address, &mut target).await;
        Self {
            _redis: redis,
            session,
            rpc,
            routes,
            address,
            server,
            remote,
            source,
            target,
        }
    }
    async fn restart_coordinator(&mut self, register_nodes: bool) {
        self.server.abort();
        let _ = (&mut self.server).await;
        self.session = self._redis.store().await.begin(1).await.unwrap();
        self.rpc = CoordinatorRpc::new(
            self.session.clone(),
            Placement::Pack,
            Peers::network(),
            RpcClient::network(Principal::Coordinator),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        let listener = TcpListener::bind(&self.address).await.unwrap();
        let service = self.rpc.clone();
        self.server = tokio::spawn(async move {
            Server::builder()
                .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        if register_nodes {
            for worker in [&mut self.source, &mut self.target] {
                worker.report.heartbeat_sequence += 1;
                worker.report.reconciling = true;
                register(&self.address, worker).await;
            }
        }
    }
    async fn resume(
        &self,
        traffic: bool,
    ) -> std::result::Result<pb::GetEnvironmentResponse, tonic::Status> {
        let role = if traffic {
            Principal::Ingress
        } else {
            Principal::ApiServer
        };
        pb::coordinator_service_client::CoordinatorServiceClient::new(
            channel(&self.address, role).await,
        )
        .ensure_running(pb::EnsureRunningRequest {
            expected_runtime_id: "sleep-1".into(),
            environment_id: "sleep".into(),
            caller: caller(),

            timeout_seconds: 3,
        })
        .await
        .map(tonic::Response::into_inner)
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn coordinator_autoscaling_http_queries_authoritative_node_ownership_without_credentials() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut rig = Rig::new(Duration::from_secs(2)).await;
    let pending = spec();
    let mut pending = EnvironmentSpec {
        id: "a-pending".into(),
        ..pending
    };
    let assignment = adx_core::Assignment {
        environment_id: pending.id.clone(),
        node_id: "a".into(),
        shard_id: 0,
        generation: 100,
        devices: vec![],
    };
    rig.session
        .reserve(pending.clone(), assignment)
        .await
        .unwrap();
    pending.id = "b-pending".into();
    pending.tenant_id = "other".into();
    rig.session
        .reserve(
            pending.clone(),
            adx_core::Assignment {
                environment_id: pending.id.clone(),
                node_id: "b".into(),
                shard_id: 0,
                generation: 101,
                devices: vec![],
            },
        )
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let rpc = rig.rpc.clone();
    let task = tokio::spawn(adx_coordinator::autoscaling::serve(listener, rpc));
    async fn call(
        address: std::net::SocketAddr,
        method: &str,
        path: &str,
    ) -> (u16, serde_json::Value) {
        let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
        socket.write_all(format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").as_bytes()).await.unwrap();
        let mut bytes = Vec::new();
        tokio::time::timeout(Duration::from_secs(3), socket.read_to_end(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        let response = String::from_utf8(bytes).unwrap();
        let (headers, body) = response.split_once("\r\n\r\n").unwrap();
        (
            headers.split_whitespace().nth(1).unwrap().parse().unwrap(),
            serde_json::from_str(body).unwrap(),
        )
    }
    let base = "/instance-manager/query-tenant-instances";
    let (code, page) = call(address, "GET", &format!("{base}?node_id=a&page_size=1")).await;
    assert_eq!(code, 200);
    assert_eq!(page["count"], 2);
    assert_eq!(page["instanceInfos"][0]["instanceID"], "a-pending");
    assert_eq!(page["instanceInfos"][0]["status"], "pending");
    assert_eq!(page["instanceInfos"][0]["resources_held"], true);
    let (_, page) = call(
        address,
        "GET",
        &format!("{base}?node_id=a&page_size=1&page=2"),
    )
    .await;
    assert_eq!(page["count"], 2);
    assert_eq!(page["instanceInfos"][0]["instanceID"], "sleep");
    assert_eq!(page["instanceInfos"][0]["resources_held"], false);
    assert_eq!(page["instanceInfos"][0]["node_id"], "a");
    for (filter, count) in [
        ("tenant_id=other", 1),
        ("instance_id=sleep&node_id=b", 0),
        ("node_id=a&page=9", 2),
    ] {
        let (code, page) = call(address, "GET", &format!("{base}?{filter}")).await;
        assert_eq!(code, 200);
        assert_eq!(page["count"], count);
    }
    for invalid in [
        "page=0",
        "page_size=1001",
        "page=18446744073709551615&page_size=1000",
        "node_id=",
        "node_id=a&node_id=b",
        "nodeId=a",
    ] {
        assert_eq!(
            call(address, "GET", &format!("{base}?{invalid}")).await.0,
            400
        );
    }
    assert_eq!(
        call(address, "GET", &format!("{base}?node_id=missing"))
            .await
            .0,
        404
    );
    let (code, queue) = call(address, "GET", "/global-scheduler/scheduling_queue").await;
    assert_eq!(code, 200);
    assert_eq!(queue, serde_json::json!({"count": 0, "instanceInfos": []}));
    for (method, status, paused) in [("POST", "evicting", true), ("DELETE", "normal", false)] {
        let (code, body) = call(
            address,
            method,
            "/global-scheduler/node/localschedulingstatus?node_id=a",
        )
        .await;
        assert_eq!(code, 200);
        assert_eq!(body["status"], status);
        assert_eq!(
            rig.session.snapshot().await.unwrap().nodes["a"].scheduling_paused,
            paused
        );
        let (_, page) = call(address, "GET", &format!("{base}?node_id=a")).await;
        assert_eq!(page["node"]["scheduling_paused"], paused);
        if paused {
            assert_eq!(page["node"]["accepting_allocations"], false);
            let rpc_address = rig.address.clone();
            let create = tokio::spawn(async move {
                let mut queued = spec();
                queued.id = "http-queued".into();
                queued
                    .scheduling
                    .required_node
                    .push(adx_core::scheduling::LabelSelector {
                        expressions: vec![adx_core::scheduling::LabelRequirement {
                            key: "NODE_ID".into(),
                            op: adx_core::scheduling::SelectorOp::In,
                            values: vec!["a".into()],
                        }],
                        ..Default::default()
                    });
                pb::coordinator_service_client::CoordinatorServiceClient::new(
                    channel(&rpc_address, Principal::ApiServer).await,
                )
                .create_environment(pb::CreateEnvironmentRequest {
                    spec: Some(queued.into()),
                    caller: caller(),
                    schedule_timeout_seconds: 1,
                    create_timeout_seconds: 10,
                    create_deadline_unix_millis: 0,
                    expected_assignment: None,
                })
                .await
            });
            tokio::time::timeout(Duration::from_millis(900), async {
                loop {
                    let (code, queue) =
                        call(address, "GET", "/global-scheduler/scheduling_queue").await;
                    assert_eq!(code, 200);
                    if queue["count"] == 1 {
                        assert_eq!(queue["instanceInfos"][0]["instanceID"], "http-queued");
                        assert_eq!(queue["instanceInfos"][0]["resources"]["cpu"], "100m");
                        assert_eq!(
                            queue["instanceInfos"][0]["schedulingRequirements"],
                            serde_json::json!({"runtimeClass": "test-runtime"})
                        );
                        assert!(queue["instanceInfos"][0]["waitDurationMs"].is_string());
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            assert!(create.await.unwrap().is_err());
        }
    }
    assert_eq!(
        call(
            address,
            "POST",
            "/global-scheduler/node/localschedulingstatus"
        )
        .await
        .0,
        400
    );
    assert_eq!(
        call(
            address,
            "POST",
            "/global-scheduler/node/localschedulingstatus?node_id=missing"
        )
        .await
        .0,
        404
    );
    rig._redis.crash();
    let (code, error) = call(address, "GET", &format!("{base}?node_id=a")).await;
    assert_eq!(code, 503);
    assert_eq!(error["error"]["code"], "Unavailable");
    assert!(error.get("count").is_none());
    task.abort();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn operator_pause_blocks_manual_and_traffic_restore_until_unpaused() {
    let rig = Rig::new(Duration::from_secs(2)).await;
    let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(&rig.address, Principal::ApiServer).await,
    );
    client
        .set_node_scheduling(pb::SetNodeSchedulingRequest {
            node_id: "a".into(),
            accepting_allocations: false,
        })
        .await
        .unwrap();
    for traffic in [false, true] {
        let response = tokio::time::timeout(Duration::from_millis(150), rig.resume(traffic)).await;
        assert!(
            response.is_err(),
            "paused node accepted restore: {response:?}"
        );
        assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 0);
        assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);
    }
    client
        .set_node_scheduling(pb::SetNodeSchedulingRequest {
            node_id: "a".into(),
            accepting_allocations: true,
        })
        .await
        .unwrap();
    let restored = rig.resume(false).await.unwrap().record.unwrap();
    assert_eq!(restored.assignment.unwrap().node_id, "a");
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 1);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn multiple_ingress_and_manual_resume_restore_once_on_original_node() {
    let rig = Rig::new(Duration::from_secs(2)).await;
    let mut requests = Vec::new();
    for i in 0..12 {
        let address = rig.address.clone();
        requests.push(tokio::spawn(async move {
            pb::coordinator_service_client::CoordinatorServiceClient::new(
                channel(
                    &address,
                    if i % 2 == 0 {
                        Principal::Ingress
                    } else {
                        Principal::ApiServer
                    },
                )
                .await,
            )
            .ensure_running(pb::EnsureRunningRequest {
                expected_runtime_id: "sleep-1".into(),
                environment_id: "sleep".into(),
                caller: caller(),

                timeout_seconds: 3,
            })
            .await
            .unwrap()
            .into_inner()
        }));
    }
    for request in requests {
        assert_eq!(
            request
                .await
                .unwrap()
                .record
                .unwrap()
                .assignment
                .unwrap()
                .node_id,
            "a"
        );
    }
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 1);
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);
    assert_eq!(rig.source.backend.running.lock().unwrap().len(), 1);
    assert!(rig.session.get("sleep").await.unwrap().resume.is_none());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn definite_local_capacity_rejection_restores_same_state_on_another_node() {
    use adxlet::checkpoint::CheckpointAccess;
    let mut rig = Rig::new(Duration::from_secs(2)).await;
    rig.source
        .manager
        .update_capacity(
            Resources {
                cpu_millis: 50,
                memory_bytes: 64,
                disk_bytes: 0,
            },
            Duration::from_secs(60),
        )
        .unwrap();
    let restored = rig.resume(false).await.unwrap().record.unwrap();
    let assignment = restored.assignment.unwrap();
    assert_eq!(assignment.node_id, "b");
    assert!(assignment.generation > 1);
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 0);
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 1);
    assert!(rig.source.backend.running.lock().unwrap().is_empty());
    assert_eq!(rig.target.backend.running.lock().unwrap().len(), 1);
    let checkpoint = rig
        .session
        .get("sleep")
        .await
        .unwrap()
        .result
        .unwrap()
        .checkpoint
        .unwrap()
        .artifact;
    let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(&rig.address, Principal::Node("a".into())).await,
    );
    rig.source.report.heartbeat_sequence += 1;
    assert_eq!(
        client
            .register_node(rig.source.report.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    rig.source.manager.pause_lifecycle().await;
    rig.source.report.heartbeat_sequence += 1;
    rig.source.report.reconciling = true;
    rig.source.report.accepting_allocations = false;
    client
        .register_node(rig.source.report.clone())
        .await
        .unwrap();
    let catalog = client
        .inspect_node(pb::InspectNodeRequest {
            node_id: "a".into(),
            session_id: rig.source.report.session_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    rig.source
        .manager
        .reconcile_retained(
            catalog
                .records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>>>()
                .unwrap(),
            catalog
                .snapshots
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>>>()
                .unwrap(),
            catalog
                .retained_artifacts
                .into_iter()
                .map(adx_core::CheckpointArtifact::try_from)
                .collect::<Result<Vec<_>>>()
                .unwrap(),
        )
        .await
        .unwrap();
    rig.source.report.heartbeat_sequence += 1;
    rig.source.report.reconciling = false;
    rig.source.report.accepting_allocations = true;
    client
        .register_node(rig.source.report.clone())
        .await
        .unwrap();
    assert_eq!(rig.source.manager.drain().await.unwrap(), 0);
    rig.target
        .store
        .validate_artifact(&checkpoint)
        .await
        .unwrap();
    assert_eq!(rig.target.backend.running.lock().unwrap().len(), 1);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn all_nodes_reject_capacity_without_repeated_assignment_churn() {
    let rig = Rig::new(Duration::from_secs(2)).await;
    for worker in [&rig.source, &rig.target] {
        worker
            .manager
            .update_capacity(
                Resources {
                    cpu_millis: 50,
                    memory_bytes: 64,
                    disk_bytes: 0,
                },
                Duration::from_secs(60),
            )
            .unwrap();
    }
    assert_eq!(
        rig.resume(true).await.unwrap_err().code(),
        tonic::Code::DeadlineExceeded
    );
    let stalled = rig.session.get("sleep").await.unwrap();
    assert_eq!(stalled.assignment.node_id, "b");
    assert_eq!(
        stalled.result.unwrap().state,
        adx_core::EnvironmentState::Paused
    );
    assert_eq!(stalled.resume.as_ref().unwrap().rejected_nodes.len(), 2);
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 0);
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);
    rig.rpc.resume_pending().await.unwrap();
    assert_eq!(
        rig.session.get("sleep").await.unwrap().assignment,
        stalled.assignment
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn transient_owner_rpc_deadline_waits_within_the_resume_budget() {
    let rig = Arc::new(Rig::new(Duration::from_millis(150)).await);
    let gate = Arc::new(tokio::sync::Notify::new());
    *rig.source.backend.restore_gate.lock().unwrap() = Some(gate.clone());
    let wake = {
        let rig = rig.clone();
        tokio::spawn(async move { rig.resume(true).await })
    };
    rig.source.backend.restore_entered.notified().await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        !wake.is_finished(),
        "a node RPC deadline must not end the longer caller resume budget"
    );
    let pending = rig.session.get("sleep").await.unwrap();
    assert_eq!(pending.assignment.node_id, "a");
    assert!(pending.resume.is_some());
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);
    gate.notify_one();
    let restored = wake.await.unwrap().unwrap().record.unwrap();
    assert_eq!(
        restored.state,
        adx_protocol::control::EnvironmentState::Running as i32
    );
    assert_eq!(restored.assignment.unwrap().node_id, "a");
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 1);
    assert_eq!(rig.source.backend.running.lock().unwrap().len(), 1);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn ambiguous_resume_deadline_retains_owner_and_never_starts_another_node() {
    let rig = Rig::new(Duration::from_millis(150)).await;
    let gate = Arc::new(tokio::sync::Notify::new());
    *rig.source.backend.restore_gate.lock().unwrap() = Some(gate.clone());
    assert!(rig.resume(true).await.is_err());
    assert_eq!(
        rig.session.get("sleep").await.unwrap().assignment.node_id,
        "a"
    );
    assert!(
        !rig.session
            .get("sleep")
            .await
            .unwrap()
            .resume
            .unwrap()
            .local_rejected
    );
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);
    gate.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if rig
                .session
                .get("sleep")
                .await
                .unwrap()
                .result
                .unwrap()
                .state
                == adx_core::EnvironmentState::Running
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        rig.resume(false)
            .await
            .unwrap()
            .record
            .unwrap()
            .assignment
            .unwrap()
            .node_id,
        "a"
    );
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 1);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn sleeping_node_loss_only_restores_after_demand() {
    let rig = Rig::new(Duration::from_secs(2)).await;
    rig.session
        .invalidate_node("a", &rig.source.report.session_id)
        .await
        .unwrap();
    assert_eq!(
        rig.session
            .get("sleep")
            .await
            .unwrap()
            .result
            .unwrap()
            .state,
        adx_core::EnvironmentState::Paused
    );
    assert_eq!(rig.rpc.recover_environments().await.unwrap(), 0);
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);
    assert_eq!(
        rig.resume(true)
            .await
            .unwrap()
            .record
            .unwrap()
            .assignment
            .unwrap()
            .node_id,
        "b"
    );
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 1);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn cached_background_sweep_retries_a_live_committed_wake_after_timeout() {
    let rig = Rig::new(Duration::from_secs(2)).await;
    let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(&rig.address, Principal::ApiServer).await,
    );
    client
        .set_node_scheduling(pb::SetNodeSchedulingRequest {
            node_id: "a".into(),
            accepting_allocations: false,
        })
        .await
        .unwrap();
    let error = client
        .ensure_running(pb::EnsureRunningRequest {
            expected_runtime_id: "sleep-1".into(),
            environment_id: "sleep".into(),
            caller: caller(),
            timeout_seconds: 1,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
    assert!(rig.session.get("sleep").await.unwrap().resume.is_some());
    client
        .set_node_scheduling(pb::SetNodeSchedulingRequest {
            node_id: "a".into(),
            accepting_allocations: true,
        })
        .await
        .unwrap();
    assert_eq!(rig.rpc.resume_pending().await.unwrap(), 1);
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 1);
    assert!(rig.session.get("sleep").await.unwrap().resume.is_none());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn redis_restart_preserves_pending_wake_and_restore_point() {
    let mut rig = Rig::new(Duration::from_secs(2)).await;
    let pending = rig.session.request_resume("sleep", 1).await.unwrap();
    rig.restart_coordinator(true).await;
    rig._redis.crash();
    rig._redis.start().await;
    let after = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match rig.session.get("sleep").await {
                Ok(record) => break record,
                Err(adx_core::Error::Unavailable(_)) => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                Err(error) => panic!("{error}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(after, pending);
    assert_eq!(rig.rpc.resume_pending().await.unwrap(), 1);
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 1);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn coordinator_restart_reconciles_the_durable_wake_without_new_identity() {
    let mut rig = Rig::new(Duration::from_secs(2)).await;
    let pending = rig.session.request_resume("sleep", 1).await.unwrap();
    rig.restart_coordinator(true).await;
    assert_eq!(
        rig.session.get("sleep").await.unwrap().resume,
        pending.resume
    );
    assert_eq!(rig.rpc.resume_pending().await.unwrap(), 1);
    assert_eq!(
        rig.session.get("sleep").await.unwrap().assignment,
        pending.assignment
    );
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 1);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn coordinator_restart_during_uncommitted_restore_cleans_then_retries() {
    coordinator_restart_uncommitted_restore(false).await;
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn coordinator_restart_reconciles_uncommitted_restore_with_real_versioned_relay() {
    coordinator_restart_uncommitted_restore(true).await;
}

struct VersionedRelay {
    routes: Arc<adxlet::routes::UdsRoutes>,
    task: tokio::task::JoinHandle<()>,
    _directory: tempfile::TempDir,
}
impl Drop for VersionedRelay {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl VersionedRelay {
    async fn start() -> Self {
        use data_plane_gateway::node::route_control::proto::relay_service_server::RelayServiceServer;
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("relay.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let relay = Arc::new(
            data_plane_gateway::node::Relay::new(
                data_plane_gateway::common::protocol::GatewayPolicy::for_local_mock(vec![
                    "127.0.0.0/8".parse().unwrap(),
                ]),
            )
            .with_route_enforcement(),
        );
        let service = data_plane_gateway::node::route_control::BindingService::new(relay);
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(RelayServiceServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(listener))
                .await
                .unwrap();
        });
        let routes =
            Arc::new(adxlet::routes::UdsRoutes::new(socket, Duration::from_secs(2)).unwrap());
        routes.begin_reconcile().await.unwrap();
        routes.finish_reconcile().await.unwrap();
        Self {
            routes,
            task,
            _directory: directory,
        }
    }
}

async fn coordinator_restart_uncommitted_restore(versioned_routes: bool) {
    let mut rig = Rig::new(Duration::from_millis(150)).await;
    let relay = if versioned_routes {
        Some(VersionedRelay::start().await)
    } else {
        None
    };
    if let Some(relay) = &relay {
        *rig.source.backend.versioned_routes.lock().unwrap() = Some(relay.routes.clone());
    }
    let gate = Arc::new(tokio::sync::Notify::new());
    *rig.source.backend.restore_gate.lock().unwrap() = Some(gate.clone());
    assert!(rig.resume(true).await.is_err());
    let pending = rig.session.get("sleep").await.unwrap();
    let orphan = rig
        .source
        .backend
        .running
        .lock()
        .unwrap()
        .keys()
        .next()
        .unwrap()
        .clone();
    assert_eq!(
        pending.result.as_ref().unwrap().state,
        adx_core::EnvironmentState::Paused
    );
    rig.server.abort();
    let _ = (&mut rig.server).await;
    rig.session = rig._redis.store().await.begin(1).await.unwrap();
    rig.rpc = CoordinatorRpc::new(
        rig.session.clone(),
        Placement::Pack,
        Peers::network(),
        RpcClient::network(Principal::Coordinator),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    let listener = TcpListener::bind(&rig.address).await.unwrap();
    let service = rig.rpc.clone();
    rig.server = tokio::spawn(async move {
        Server::builder()
            .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    rig.source
        .sink
        // Keep publication rejected while every accepted retry drains. Otherwise
        // a queued sync can legitimately publish Running during re-registration,
        // and this fixture no longer tests an uncommitted execution.
        .reconnect(channel(&rig.address, Principal::Node("b".into())).await);
    gate.notify_one();
    rig.source.manager.pause_lifecycle().await;
    let handle = rig
        .source
        .manager
        .environment(pending.spec.clone(), pending.assignment.clone())
        .unwrap();
    assert!(handle.sync().await.is_err());
    assert_eq!(
        rig.session
            .get("sleep")
            .await
            .unwrap()
            .result
            .unwrap()
            .state,
        adx_core::EnvironmentState::Paused
    );
    rig.source
        .sink
        .reconnect(channel(&rig.address, Principal::Node("a".into())).await);
    *rig.source.backend.restore_gate.lock().unwrap() = None;
    for worker in [&mut rig.source, &mut rig.target] {
        worker.report.heartbeat_sequence += 1;
        register(&rig.address, worker).await;
    }
    assert!(!rig
        .source
        .backend
        .running
        .lock()
        .unwrap()
        .contains_key(&orphan));
    assert_eq!(
        rig.session.get("sleep").await.unwrap().resume,
        pending.resume
    );
    let restored = rig.resume(false).await.unwrap().record.unwrap();
    assert_eq!(restored.assignment.unwrap().node_id, "a");
    assert_eq!(rig.source.backend.running.lock().unwrap().len(), 1);
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 2);
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn adxlet_restart_cleans_an_uncommitted_restore_before_retrying() {
    let mut rig = Rig::new(Duration::from_millis(150)).await;
    let gate = Arc::new(tokio::sync::Notify::new());
    *rig.source.backend.restore_gate.lock().unwrap() = Some(gate.clone());
    assert!(rig.resume(true).await.is_err());
    let old = rig.source.backend.clone();
    old.fail_after_gate.store(true, Ordering::SeqCst);
    let old_runtime = old.running.lock().unwrap().keys().next().unwrap().clone();
    rig.source.task.abort();
    let _ = (&mut rig.source.task).await;
    // A new node process uses the same execution backend and shared object store.
    let mut replacement = worker(
        "a",
        &rig.address,
        rig.remote.clone(),
        Some(&rig.source.report.node_address),
    )
    .await;
    // The old backend remains observable after process death. Copy its inventory
    // into the replacement fixture before authoritative reconciliation.
    *replacement.backend.running.lock().unwrap() = old.running.lock().unwrap().clone();
    gate.notify_one();
    tokio::task::yield_now().await;
    register(&rig.address, &mut replacement).await;
    assert!(!replacement
        .backend
        .running
        .lock()
        .unwrap()
        .contains_key(&old_runtime));
    rig.source = replacement;
    let restored = rig.resume(false).await.unwrap().record.unwrap();
    assert_eq!(restored.assignment.unwrap().node_id, "a");
    assert_eq!(rig.source.backend.running.lock().unwrap().len(), 1);
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 1);
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn first_traffic_through_multiple_real_ingress_instances_wakes_and_forwards() {
    multiple_ingress_traffic(false).await;
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn timed_out_http_requests_retry_through_other_ingress_replicas_and_restore_once() {
    multiple_ingress_traffic(true).await;
}

async fn multiple_ingress_traffic(timeout_retry: bool) {
    use data_plane_gateway::{
        common::protocol::GatewayPolicy,
        ingress::{
            coordinator_routes::CoordinatorConnection, AccessKind, DataPlaneL4Connector,
            H2PoolConfig, Ingress, IngressAuthenticator, IngressRouteResolver, RouteStore,
        },
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut rig = Rig::new(Duration::from_secs(2)).await;
    const KEY: &str = "valid-ingress-wake-test-key-at-least-32-bytes";
    rig.session
        .bootstrap_credential(
            KEY,
            &adx_coordinator::auth::Credential {
                tenant_id: "tenant".into(),
                administrator: false,
                expires_at_unix_seconds: 0,
            },
        )
        .await
        .unwrap();
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = backend.local_addr().unwrap().port();
    let executions = Arc::new(Mutex::new(BTreeMap::<String, usize>::new()));
    let counted = executions.clone();
    let echo = tokio::spawn(async move {
        loop {
            let (mut socket, _) = backend.accept().await.unwrap();
            let counted = counted.clone();
            tokio::spawn(async move {
                if timeout_retry {
                    let mut request = Vec::new();
                    let mut byte = [0; 1];
                    while !request.ends_with(b"\r\n\r\n") {
                        if socket.read_exact(&mut byte).await.is_err() {
                            return;
                        }
                        request.extend_from_slice(&byte);
                    }
                    let request = String::from_utf8(request).unwrap();
                    assert!(!request
                        .to_ascii_lowercase()
                        .contains("x-adx-processing-timeout-ms"));
                    let id = request
                        .lines()
                        .find_map(|line| {
                            line.split_once(':')
                                .filter(|(key, _)| key.eq_ignore_ascii_case("x-adx-request-id"))
                                .map(|(_, value)| value.trim())
                        })
                        .unwrap()
                        .to_owned();
                    *counted.lock().unwrap().entry(id.clone()).or_default() += 1;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{id}",
                        id.len()
                    );
                    socket.write_all(response.as_bytes()).await.unwrap();
                    return;
                }
                let (mut read, mut write) = socket.split();
                let _ = tokio::io::copy(&mut read, &mut write).await;
            });
        }
    });
    let relay = Arc::new(
        data_plane_gateway::node::Relay::new(GatewayPolicy::for_local_mock(vec!["127.0.0.0/8"
            .parse()
            .unwrap()]))
        .with_route_enforcement(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    rig.source.report.proxy_address = listener.local_addr().unwrap().to_string();
    rig.source.report.heartbeat_sequence += 1;
    *rig.source.backend.relay.lock().unwrap() = Some(relay.clone());
    register(&rig.address, &mut rig.source).await;
    let relay_task = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let relay = relay.clone();
            tokio::spawn(async move {
                let _ = relay.serve_h2(socket).await;
            });
        }
    });
    rig.session
        .advertise(
            "test",
            &format!("http://{}", rig.address),
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    let publisher = {
        let routes = rig.routes.clone();
        tokio::spawn(async move {
            routes.run(Duration::from_millis(10)).await;
        })
    };
    let mut watchers = Vec::new();
    let mut ingresses = Vec::new();
    for _ in 0..3 {
        let store = Arc::new(RouteStore::new());
        let connection = Arc::new(CoordinatorConnection::new(serde_json::from_value(serde_json::json!({
            "redis_url":rig._redis.url, "namespace":"test", "tls":{"mode":"network"}, "rpc_timeout_seconds":2,
            "refresh_seconds":1, "auth_cache_seconds":1,"auth_cache_entries":10
        })).unwrap()).unwrap());
        watchers.push({
            let connection = connection.clone();
            let store = store.clone();
            tokio::spawn(async move {
                connection.run(store).await;
            })
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !store.ready() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let resolver = Arc::new(
            IngressRouteResolver::new(store)
                .stream_only()
                .with_demand_resolver(connection.clone()),
        );
        ingresses.push(Arc::new(
            Ingress::new(
                resolver,
                DataPlaneL4Connector::new(H2PoolConfig {
                    tls_config: None,
                    connections_per_node: 1,
                    max_connections_per_node: 1,
                    ..Default::default()
                }),
                IngressAuthenticator::with_verifier(connection),
                port,
                8765,
                "127.0.0.1:1",
                vec![],
            )
            .with_client_acl(vec!["127.0.0.0/8".parse().unwrap()], false),
        ));
    }
    let mut requests = Vec::new();
    assert!(ingresses[0]
        .open_l4_stream(
            "sleep",
            port,
            AccessKind::Direct,
            "invalid-key",
            "unauthorized-probe".into()
        )
        .await
        .is_err());
    assert!(rig.source.backend.restored.load(Ordering::SeqCst) <= 1);
    if timeout_retry {
        let gate = Arc::new(tokio::sync::Notify::new());
        *rig.source.backend.restore_gate.lock().unwrap() = Some(gate.clone());
        for i in 0..12 {
            let ingress = ingresses[i % 3].clone();
            requests.push(tokio::spawn(async move {
                let response =
                    ingress_http_attempt(ingress, port, KEY, &format!("logical-{i}"), 1000).await;
                assert!(response.starts_with("HTTP/1.1 503"), "{response}");
                assert!(response.contains("ENV_RESUMING"), "{response}");
                assert!(
                    response.contains("\"requestForwarded\":false"),
                    "{response}"
                );
            }));
        }
        for request in requests.drain(..) {
            request.await.unwrap();
        }
        assert!(
            executions.lock().unwrap().is_empty(),
            "timed-out requests must not reach the backend"
        );
        let intent = rig.session.get("sleep").await.unwrap().resume.unwrap();
        assert!(!intent.local_rejected);
        assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 1);
        for i in 0..12 {
            // Every retry uses a different replica and the same timeout configured
            // for both HTTP attempts; each gateway has its own route store and pool.
            let ingress = ingresses[(i + 1) % 3].clone();
            requests.push(tokio::spawn(async move {
                let response =
                    ingress_http_attempt(ingress, port, KEY, &format!("logical-{i}"), 1000).await;
                assert!(response.starts_with("HTTP/1.1 200"), "{response}");
                assert!(response.ends_with(&format!("logical-{i}")), "{response}");
            }));
        }
        // Retries are already waiting on other replicas while the original
        // physical restore is still gated. They must join its durable intent.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(executions.lock().unwrap().is_empty());
        assert_eq!(
            rig.session
                .get("sleep")
                .await
                .unwrap()
                .resume
                .unwrap()
                .operation_id,
            intent.operation_id
        );
        assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 1);
        gate.notify_one();
        for request in requests.drain(..) {
            request.await.unwrap();
        }
        assert!(rig.session.get("sleep").await.unwrap().resume.is_none());
        let counts = executions.lock().unwrap();
        assert_eq!(counts.len(), 12);
        assert!(counts.values().all(|count| *count == 1));
    } else {
        for ingress in ingresses {
            requests.push(tokio::spawn(async move {
                let mut stream = ingress
                    .open_l4_stream(
                        "sleep",
                        port,
                        AccessKind::Direct,
                        KEY,
                        "first-business-request".into(),
                    )
                    .await
                    .unwrap();
                stream.write_all(b"resume-then-forward").await.unwrap();
                let mut response = [0u8; 19];
                stream.read_exact(&mut response).await.unwrap();
                assert_eq!(&response, b"resume-then-forward");
            }));
        }
        for request in requests {
            tokio::time::timeout(Duration::from_secs(5), request)
                .await
                .unwrap()
                .unwrap();
        }
    }
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 1);
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);
    for task in watchers {
        task.abort();
    }
    publisher.abort();
    relay_task.abort();
    echo.abort();
}

async fn ingress_http_attempt(
    ingress: Arc<data_plane_gateway::ingress::Ingress>,
    port: u16,
    key: &str,
    request_id: &str,
    timeout_ms: u64,
) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let cert = include_bytes!("../../../gateway/tests/fixtures/ingress-cert.pem");
    let key_pem = include_bytes!("../../../gateway/tests/fixtures/ingress-key.pem");
    let certificates = rustls_pemfile::certs(&mut &cert[..])
        .collect::<std::io::Result<Vec<_>>>()
        .unwrap();
    let private_key = rustls_pemfile::private_key(&mut &key_pem[..])
        .unwrap()
        .unwrap();
    let server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates.clone(), private_key)
        .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    for cert in certificates {
        roots.add(cert).unwrap();
    }
    let client = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown, stopped) = tokio::sync::watch::channel(false);
    let service = tokio::spawn(ingress.serve_http_tls(
        listener,
        tokio_rustls::TlsAcceptor::from(Arc::new(server)),
        stopped,
    ));
    let socket = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut socket = tokio_rustls::TlsConnector::from(Arc::new(client))
        .connect(
            rustls::pki_types::ServerName::try_from("localhost").unwrap(),
            socket,
        )
        .await
        .unwrap();
    socket.write_all(format!("POST /sleep/{port}/invoke HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {key}\r\nX-ADX-Request-ID: {request_id}\r\nX-ADX-Processing-Timeout-Ms: {timeout_ms}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), socket.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    shutdown.send(true).unwrap();
    service.await.unwrap().unwrap();
    String::from_utf8(response).unwrap()
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn delete_serializes_with_restore_and_fences_its_late_result() {
    let rig = Rig::new(Duration::from_secs(2)).await;
    let gate = Arc::new(tokio::sync::Notify::new());
    *rig.source.backend.restore_gate.lock().unwrap() = Some(gate.clone());
    let wake = {
        let address = rig.address.clone();
        tokio::spawn(async move {
            pb::coordinator_service_client::CoordinatorServiceClient::new(
                channel(&address, Principal::Ingress).await,
            )
            .ensure_running(pb::EnsureRunningRequest {
                environment_id: "sleep".into(),
                caller: caller(),

                timeout_seconds: 3,
                expected_runtime_id: "sleep-1".into(),
            })
            .await
        })
    };
    rig.source.backend.restore_entered.notified().await;
    let paused = rig.session.get("sleep").await.unwrap().result.unwrap();
    let mut node = pb::node_service_client::NodeServiceClient::new(
        channel(&rig.source.report.node_address, Principal::ApiServer).await,
    );
    let mut deleting = Box::pin(node.delete_environment(pb::DeleteEnvironmentRequest {
        assignment: Some(paused.assignment.clone().try_into().unwrap()),
        caller: caller(),
        failed_revision: None,
        node_session_id: String::new(),
    }));
    assert!(futures_util::poll!(deleting.as_mut()).is_pending());
    gate.notify_one();
    let deleted = deleting.await.unwrap().into_inner();
    assert_eq!(
        deleted.record.unwrap().state,
        pb::EnvironmentState::Deleted as i32
    );
    let _ = wake.await.unwrap();
    assert!(rig.source.backend.running.lock().unwrap().is_empty());
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);
    assert!(rig.resume(true).await.is_err());
    let mut late = paused;
    late.state = adx_core::EnvironmentState::Running;
    late.revision += 10;
    late.resources_held = true;
    late.runtime.id = "sleep-1-r100".into();
    late.runtime.ip = Some("127.0.0.1".parse().unwrap());
    late.last_operation = Some(adx_core::CompletedOperation {
        id: "late-resume".into(),
        kind: adx_core::LifecycleKind::Resume,
        expected_revision: 4,
    });
    assert_eq!(
        rig.session.commit(late).await,
        Err(adx_core::Error::NotFound)
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn expired_dormant_restore_point_cannot_be_awakened() {
    let rig = Rig::with_ttl(Duration::from_secs(2), 1).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        rig.resume(true).await.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 0);
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);
    rig.source.manager.expire_checkpoints().await.unwrap();
    assert_eq!(
        rig.session.get("sleep").await,
        Err(adx_core::Error::NotFound)
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn deletion_and_expiry_retire_dormancy_when_the_source_is_lost() {
    for expired in [false, true] {
        let mut rig = Rig::with_ttl(Duration::from_secs(2), if expired { 1 } else { 3600 }).await;
        rig.session
            .invalidate_node("a", &rig.source.report.session_id)
            .await
            .unwrap();
        if expired {
            // Load the staged lost-node catalog through startup, as production
            // invalidation normally updates the live Coordinator cache itself.
            rig.restart_coordinator(false).await;
            tokio::time::sleep(Duration::from_millis(1100)).await;
            assert_eq!(rig.rpc.expire_dormant().await.unwrap(), 1);
        } else {
            let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
                channel(&rig.address, Principal::ApiServer).await,
            );
            let result = client
                .retire_dormant(pb::GetEnvironmentRequest {
                    environment_id: "sleep".into(),
                    caller: caller(),
                })
                .await
                .unwrap()
                .into_inner();
            assert_eq!(
                result.record.unwrap().state,
                pb::EnvironmentState::Deleted as i32
            );
        }
        assert_eq!(
            rig.session.get("sleep").await,
            Err(adx_core::Error::NotFound)
        );
        assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);
        assert!(rig.resume(true).await.is_err());
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn background_resume_round_does_not_starve_later_intents() {
    let mut rig = Rig::new(Duration::from_secs(2)).await;
    let template = rig.session.get("sleep").await.unwrap().result.unwrap();
    for index in 0..16 {
        let id = format!("a-blocked-{index:02}");
        let mut record = template.clone();
        record.spec.id = id.clone();
        record.assignment.environment_id = id.clone();
        record.assignment.node_id = "b".into();
        record.assignment.generation = index + 2;
        record.runtime.id = format!("{id}-{}", index + 2);
        record.checkpoint.as_mut().unwrap().source_runtime_id = record.runtime.id.clone();
        rig.session
            .reserve(record.spec.clone(), record.assignment.clone())
            .await
            .unwrap();
        rig.session.commit(record).await.unwrap();
        rig.session.request_resume(&id, 1).await.unwrap();
    }
    rig.session.request_resume("sleep", 1).await.unwrap();
    // Only the source rejoins. The staged intents on b remain blocked and
    // must not prevent the next bounded round from reaching the live source.
    rig.restart_coordinator(false).await;
    rig.source.report.heartbeat_sequence += 1;
    rig.source.report.reconciling = true;
    register(&rig.address, &mut rig.source).await;
    rig.rpc.resume_pending().await.unwrap();
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 0);
    rig.rpc.resume_pending().await.unwrap();
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 1);
    assert!(rig.session.get("sleep").await.unwrap().resume.is_none());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn original_checkpoint_cache_pressure_does_not_reject_resume() {
    use adxlet::checkpoint::CheckpointAccess;
    let rig = Rig::new(Duration::from_secs(2)).await;
    let stage = rig.source.store.begin_capture().await.unwrap();
    std::fs::write(stage.join("occupied"), vec![42; 1024]).unwrap();
    let artifact = rig.source.store.seal(&stage).await.unwrap();
    let active = rig.source.store.prepare_restore(&artifact).await.unwrap();
    let restored = rig.resume(false).await.unwrap().record.unwrap();
    assert_eq!(restored.assignment.unwrap().node_id, "a");
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 1);
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);
    assert_eq!(
        std::fs::read(active.join("occupied")).unwrap(),
        vec![42; 1024]
    );
}
