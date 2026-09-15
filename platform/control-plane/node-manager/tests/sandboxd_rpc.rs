//! Local gRPC/UDS contract tests against the pinned protocol, not sandboxd E2E.
use adx_core::{InstanceSpec, Resources};
use adx_node_manager::{
    sandboxd::{proto::*, Config, Sandboxd},
    RuntimeBackend,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::UnixListener, sync::Semaphore};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status};

#[derive(Clone)]
struct Server {
    requests: Arc<Mutex<Vec<String>>>,
    running: Arc<Mutex<bool>>,
    xpu: Arc<Mutex<Vec<XpuAllocation>>>,
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    ip: String,
    labels: Arc<Mutex<std::collections::HashMap<String, String>>>,
    transport_error: bool,
    invalid_argument: bool,
    retain_after_delete: bool,
}
impl Default for Server {
    fn default() -> Self {
        Self {
            requests: Arc::default(),
            running: Arc::default(),
            xpu: Arc::default(),
            entered: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(1)),
            ip: "10.0.0.2".into(),
            labels: Arc::default(),
            transport_error: false,
            invalid_argument: false,
            retain_after_delete: false,
        }
    }
}
#[tonic::async_trait]
impl sandbox_service_server::SandboxService for Server {
    async fn start(
        &self,
        request: Request<StartRequest>,
    ) -> Result<Response<StartResponse>, Status> {
        let request = request.into_inner();
        assert!(request.sandbox_id.is_empty());
        *self.labels.lock().unwrap() = request.labels.clone();
        *self.xpu.lock().unwrap() = request.xpu_allocations.clone();
        self.requests
            .lock()
            .unwrap()
            .push(format!("start:{}", request.sandbox_id));
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        if self.invalid_argument {
            return Err(Status::invalid_argument("invalid start request"));
        }
        if self.transport_error {
            return Err(Status::unavailable("connection interrupted"));
        }
        *self.running.lock().unwrap() = true;
        Ok(Response::new(StartResponse {
            id: "generated-backend-id".into(),
            sandbox_ip: self.ip.clone(),
            ..Default::default()
        }))
    }
    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        self.requests
            .lock()
            .unwrap()
            .push(format!("delete:{}", request.into_inner().id));
        if !self.retain_after_delete {
            *self.running.lock().unwrap() = false;
        }
        Ok(Response::new(DeleteResponse::default()))
    }
    async fn list(
        &self,
        request: Request<ListSandboxesRequest>,
    ) -> Result<Response<ListSandboxesResponse>, Status> {
        let request = request.into_inner();
        let id = request.id;
        self.requests.lock().unwrap().push(format!("list:{id}"));
        if !*self.running.lock().unwrap() || (!id.is_empty() && id != "generated-backend-id") {
            if !id.is_empty() {
                return Err(Status::not_found("sandbox not found"));
            }
            return Ok(Response::new(ListSandboxesResponse::default()));
        }
        let labels = self.labels.lock().unwrap().clone();
        if request
            .selector
            .iter()
            .any(|(k, v)| labels.get(k) != Some(v))
        {
            return Ok(Response::new(ListSandboxesResponse::default()));
        }
        Ok(Response::new(ListSandboxesResponse {
            sandboxes: vec![SandboxStatus {
                id: "generated-backend-id".into(),
                labels,
                state: SandboxState::Running as i32,
                ..Default::default()
            }],
        }))
    }
    async fn stats(
        &self,
        request: Request<StatsRequest>,
    ) -> Result<Response<StatsResponse>, Status> {
        assert_eq!(request.into_inner().id, "generated-backend-id");
        Ok(Response::new(StatsResponse {
            memory_usage_bytes: 123,
            cpu_usage_ns: 456,
            ..Default::default()
        }))
    }
    async fn checkpoint(
        &self,
        _: Request<CheckpointRequest>,
    ) -> Result<Response<CheckpointResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn wait(&self, _: Request<WaitRequest>) -> Result<Response<WaitResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn list_available_runtimes(
        &self,
        _: Request<ListAvailableRuntimesRequest>,
    ) -> Result<Response<ListAvailableRuntimesResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn set_network_policy(
        &self,
        _: Request<SetNetworkPolicyRequest>,
    ) -> Result<Response<SetNetworkPolicyResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
}
struct Harness {
    _directory: tempfile::TempDir,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Harness {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn connect(server: Server) -> (Sandboxd, Harness) {
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("rpc.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(sandbox_service_server::SandboxServiceServer::new(server))
            .serve_with_incoming(UnixListenerStream::new(listener))
            .await
            .unwrap();
    });
    let adapter = Sandboxd::connect(
        socket,
        Config {
            rpc_timeout: Duration::from_secs(2),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    (
        adapter,
        Harness {
            _directory: directory,
            task,
        },
    )
}
fn spec() -> InstanceSpec {
    InstanceSpec {
        env: Default::default(),
        scheduling: Default::default(),
        id: "i".into(),
        tenant_id: "tenant".into(),
        image: "rrt:test".into(),
        runtime: "runsc".into(),
        resources: Resources {
            cpu_millis: 1000,
            memory_bytes: 1 << 30,
            disk_bytes: 1 << 30,
        },
        priority: 0,
    }
}

#[tokio::test]
async fn deleted_id_not_found_confirms_absence() {
    let (adapter, _server) = connect(Server::default()).await;
    assert_eq!(
        adapter
            .start(&spec(), "i-1", 1, &[])
            .await
            .unwrap()
            .to_string(),
        "10.0.0.2"
    );
    assert!(adapter.is_running("i-1").await.unwrap());
    assert_eq!(adapter.stats("i-1").await.unwrap().memory_usage_bytes, 123);
    adapter.remove("i-1").await.unwrap();
    assert!(!adapter.is_running("i-1").await.unwrap());
    adapter.remove("i-1").await.unwrap();
}

#[tokio::test]
async fn cleanup_waits_for_start_after_caller_cancellation() {
    let server = Server {
        release: Arc::new(Semaphore::new(0)),
        ..Default::default()
    };
    let (adapter, _server) = connect(server.clone()).await;
    let start = {
        let adapter = adapter.clone();
        tokio::spawn(async move { adapter.start(&spec(), "i-1", 1, &[]).await })
    };
    server.entered.acquire().await.unwrap().forget();
    start.abort();
    let remove = {
        let adapter = adapter.clone();
        tokio::spawn(async move { adapter.remove("i-1").await })
    };
    // Start has not settled; no Delete RPC is allowed to overtake it.
    tokio::task::yield_now().await;
    assert_eq!(*server.requests.lock().unwrap(), vec!["start:"]);
    server.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(3), remove)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        *server.requests.lock().unwrap(),
        vec![
            "start:",
            "delete:generated-backend-id",
            "list:generated-backend-id"
        ]
    );
}

#[tokio::test]
async fn transport_failure_does_not_claim_cleanup_or_repeat_start() {
    let server = Server {
        transport_error: true,
        ..Default::default()
    };
    let (adapter, _server) = connect(server.clone()).await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert!(adapter.remove("i-1").await.is_err());
    assert_eq!(*server.requests.lock().unwrap(), vec!["start:"]);
}

#[tokio::test]
async fn malformed_ip_can_be_cleaned_after_completed_start() {
    let (adapter, _server) = connect(Server {
        ip: String::new(),
        ..Default::default()
    })
    .await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    adapter.remove("i-1").await.unwrap();
}

#[tokio::test]
async fn delete_ack_without_absence_is_not_success() {
    let (adapter, _server) = connect(Server {
        retain_after_delete: true,
        ..Default::default()
    })
    .await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    assert!(adapter.remove("i-1").await.is_err());
}

#[tokio::test]
async fn concrete_gpu_and_npu_cards_reach_the_pinned_backend_protocol() {
    use adx_core::scheduling::*;
    let server = Server::default();
    let (adapter, _server) = connect(server.clone()).await;
    let mut spec = spec();
    spec.scheduling.devices = vec![
        DeviceRequest {
            kind: DeviceKind::Gpu,
            model: Some("a".into()),
            count: 1,
        },
        DeviceRequest {
            kind: DeviceKind::Npu,
            model: None,
            count: 1,
        },
    ];
    let cards = vec![
        DeviceAllocation {
            id: 3,
            kind: DeviceKind::Gpu,
            model: "a".into(),
        },
        DeviceAllocation {
            id: 7,
            kind: DeviceKind::Npu,
            model: "x".into(),
        },
    ];
    adapter.start(&spec, "i-1", 1, &cards).await.unwrap();
    assert_eq!(
        *server.xpu.lock().unwrap(),
        vec![
            XpuAllocation {
                r#type: "gpu".into(),
                device_ids: vec![3]
            },
            XpuAllocation {
                r#type: "npu".into(),
                device_ids: vec![7]
            }
        ]
    );
}

#[tokio::test]
async fn rejected_start_can_be_cleaned_without_uncertain_outcome() {
    let (adapter, _server) = connect(Server {
        invalid_argument: true,
        ..Default::default()
    })
    .await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    adapter.remove("i-1").await.unwrap();
}

#[tokio::test]
async fn restart_recovers_generated_backend_id_from_labels() {
    let server = Server::default();
    let (adapter, harness) = connect(server.clone()).await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    drop(adapter);
    let restored = Sandboxd::connect(
        harness._directory.path().join("rpc.sock"),
        Config::default(),
    )
    .await
    .unwrap();
    let observed = restored.inventory().await.unwrap();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].runtime_id, "i-1");
    assert_eq!(observed[0].generation, 1);
    assert!(restored.is_running("i-1").await.unwrap());
    restored.stats("i-1").await.unwrap();
    restored.remove("i-1").await.unwrap();
    assert!(!*server.running.lock().unwrap());
    assert_eq!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|v| v.starts_with("start:"))
            .count(),
        1
    );
}

#[tokio::test]
async fn cleanup_after_restart_finds_uncommitted_runtime_by_labels() {
    let server = Server::default();
    let (adapter, harness) = connect(server.clone()).await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    let restored = Sandboxd::connect(
        harness._directory.path().join("rpc.sock"),
        Config::default(),
    )
    .await
    .unwrap();
    restored.remove("i-1").await.unwrap();
    assert!(!*server.running.lock().unwrap());
}
