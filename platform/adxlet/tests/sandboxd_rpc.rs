//! Local gRPC/UDS contract tests against the pinned protocol, not sandboxd E2E.
#[path = "common/authority.rs"]
mod authority;
use adx_core::{Assignment, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Resources};
use adxlet::{
    sandboxd::{connect_when_ready, proto::*, start_request, Config, Sandboxd},
    Adxlet, Durability, Readiness, Routes, RuntimeDriver, StateSink,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::UnixListener, sync::Semaphore};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status};

#[derive(Clone)]
struct Server {
    requests: Arc<Mutex<Vec<String>>>,
    starts: Arc<Mutex<Vec<StartRequest>>>,
    network_updates: Arc<Mutex<Vec<SetNetworkPolicyRequest>>>,
    unready: Arc<std::sync::atomic::AtomicUsize>,
    running: Arc<Mutex<bool>>,
    xpu: Arc<Mutex<Vec<XpuAllocation>>>,
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    ip: String,
    labels: Arc<Mutex<std::collections::HashMap<String, String>>>,
    transport_error: bool,
    transport_error_after_start: bool,
    invalid_argument: bool,
    unsettled_error: Option<tonic::Code>,
    settled_error: Option<tonic::Code>,
    settled_error_after_start: bool,
    retain_after_delete: bool,
    exit_message: Option<String>,
    start_timeout: Option<String>,
}
impl Default for Server {
    fn default() -> Self {
        Self {
            requests: Arc::default(),
            starts: Arc::default(),
            network_updates: Arc::default(),
            unready: Arc::default(),
            running: Arc::default(),
            xpu: Arc::default(),
            entered: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(1)),
            ip: "10.0.0.2".into(),
            labels: Arc::default(),
            transport_error: false,
            transport_error_after_start: false,
            invalid_argument: false,
            unsettled_error: None,
            settled_error: None,
            settled_error_after_start: false,
            retain_after_delete: false,
            exit_message: None,
            start_timeout: None,
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
        self.starts.lock().unwrap().push(request.clone());
        *self.labels.lock().unwrap() = request.labels.clone();
        *self.xpu.lock().unwrap() = request.xpu_allocations.clone();
        self.requests
            .lock()
            .unwrap()
            .push(format!("start:{}", request.sandbox_id));
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        if let Some(message) = &self.start_timeout {
            let mut status = Status::deadline_exceeded(message);
            status
                .metadata_mut()
                .insert("sandboxd-start-failure", "true".parse().unwrap());
            return Err(status);
        }
        if self.invalid_argument {
            return Err(Status::invalid_argument("invalid start request"));
        }
        if let Some(code) = self.unsettled_error {
            return Err(Status::new(
                code,
                "runtime start failed without settlement proof",
            ));
        }
        if self.transport_error {
            return Err(Status::unavailable("connection interrupted"));
        }
        if let Some(code) = self.settled_error {
            *self.running.lock().unwrap() = self.settled_error_after_start;
            let mut status = Status::new(code, "start failed and rollback completed");
            status
                .metadata_mut()
                .insert("sandboxd-start-settled", "true".parse().unwrap());
            return Err(status);
        }
        *self.running.lock().unwrap() = true;
        if self.transport_error_after_start {
            return Err(Status::unavailable("response interrupted after start"));
        }
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
        if (!*self.running.lock().unwrap() && self.exit_message.is_none())
            || (!id.is_empty() && id != "generated-backend-id")
        {
            if !id.is_empty() {
                return Err(Status::not_found("environment not found"));
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
                state: if *self.running.lock().unwrap() {
                    SandboxState::Running as i32
                } else {
                    SandboxState::Exited as i32
                },
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
        request: Request<CheckpointRequest>,
    ) -> Result<Response<CheckpointResponse>, Status> {
        let r = request.into_inner();
        assert_eq!(r.id, "generated-backend-id");
        self.requests
            .lock()
            .unwrap()
            .push(format!("checkpoint:leave_running={}", r.leave_running));
        assert_eq!(r.timeout_seconds, 60);
        assert_eq!(r.snapshot_type, "Full");
        std::fs::write(
            std::path::Path::new(&r.checkpoint_dir).join("memory"),
            b"state",
        )
        .unwrap();
        *self.running.lock().unwrap() = r.leave_running;
        Ok(Response::new(CheckpointResponse {}))
    }
    async fn wait(&self, request: Request<WaitRequest>) -> Result<Response<WaitResponse>, Status> {
        self.requests
            .lock()
            .unwrap()
            .push(format!("wait:{}", request.into_inner().id));
        Ok(Response::new(WaitResponse {
            status: 0,
            exit_code: 137,
            message: self.exit_message.clone().unwrap_or_default(),
        }))
    }
    async fn list_available_runtimes(
        &self,
        _: Request<ListAvailableRuntimesRequest>,
    ) -> Result<Response<ListAvailableRuntimesResponse>, Status> {
        use std::sync::atomic::Ordering;
        if self
            .unready
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(Status::unavailable("environment service not ready"));
        }
        Ok(Response::new(ListAvailableRuntimesResponse {
            runtime_classes: vec!["runsc".into()],
            runtimes: vec![RuntimeInfo {
                runtime_class: "runsc".into(),
                supports_checkpoint_restore: true,
                checkpoint_handoff_path: "/run/backend/checkpoint".into(),
                restore_env_path: "/run/backend/environment".into(),
            }],
        }))
    }
    async fn set_network_policy(
        &self,
        request: Request<SetNetworkPolicyRequest>,
    ) -> Result<Response<SetNetworkPolicyResponse>, Status> {
        self.network_updates
            .lock()
            .unwrap()
            .push(request.into_inner());
        Ok(Response::new(SetNetworkPolicyResponse {}))
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
    connect_with_env(server, HashMap::new()).await
}
async fn connect_with_env(server: Server, env: HashMap<String, String>) -> (Sandboxd, Harness) {
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
            env,
            rpc_timeout: Duration::from_secs(2),
            runtime_logs: Some(adxlet::runtime_logs::RuntimeLogPolicy {
                directory: directory.path().join("runtime-logs"),
                ..Default::default()
            }),
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
#[tokio::test]
async fn discovers_runtime_classes_from_sandboxd_uds() {
    let (adapter, _server) = connect(Server::default()).await;
    assert_eq!(
        adapter.available_runtime_classes().await.unwrap(),
        ["runsc"]
    );
}

#[tokio::test]
async fn running_runsc_retains_input_after_restore_acknowledgement() {
    let (adapter, _server) = connect(Server::default()).await;
    let mut source = spec();
    source.runtime_class = "runsc".into();
    let checkpoint = tempfile::tempdir().unwrap();
    adapter
        .restore(&source, "background-restore", 1, &[], checkpoint.path())
        .await
        .unwrap();
    // Start may acknowledge before background page loading has finished.
    // The controller must retain its file access and durable input reference.
    assert!(adapter.retains_checkpoint_files(&source.runtime_class));
    assert!(!adapter.retains_capture_files(&source.runtime_class));
    adapter.remove("background-restore").await.unwrap();
}
fn spec() -> EnvironmentSpec {
    EnvironmentSpec {
        runtime_profile: None,
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        scheduling: Default::default(),
        id: "i".into(),
        tenant_id: "tenant".into(),
        image: "execd:test".into(),
        runtime_class: "runsc".into(),
        resources: Resources {
            cpu_millis: 1000,
            memory_bytes: 1 << 30,
            disk_bytes: 1 << 30,
        },
        priority: 0,
        execution: Default::default(),
    }
}

#[test]
fn kata_start_prepares_guest_pty_without_privileged_guest_capabilities() {
    let mut spec = spec();
    spec.runtime_class = "kata".into();
    let request = start_request(&spec, "i-1", 1, &[], &Config::default()).unwrap();
    let pts = request
        .mounts
        .iter()
        .find(|mount| mount.target == "/dev/pts")
        .expect("guest-native PTY requires an OCI devpts mount");
    assert_eq!(pts.r#type, "devpts");
    assert_eq!(pts.source, Some(mount::Source::HostPath("devpts".into())));
    for option in [
        "newinstance",
        "ptmxmode=0666",
        "mode=0620",
        "gid=5",
        "nosuid",
        "noexec",
    ] {
        assert!(pts.options.iter().any(|value| value == option));
    }
}

#[test]
fn local_bind_mount_includes_the_oci_bind_flag() {
    let mut spec = spec();
    spec.execution.mounts.push(adx_core::execution::Mount {
        create_if_missing: false,
        create_options: None,
        kind: "bind".into(),
        target: "/workspace".into(),
        options: vec!["rw".into(), "nosuid".into()],
        source: adx_core::execution::StorageSource::Local("/shared/workspace".into()).into(),
    });
    let request = start_request(&spec, "i-1", 1, &[], &Config::default()).unwrap();
    let mount = request
        .mounts
        .iter()
        .find(|mount| mount.target == "/workspace")
        .unwrap();
    assert!(mount
        .options
        .iter()
        .any(|option| option == "bind" || option == "rbind"));
    assert!(mount.options.iter().any(|option| option == "rw"));
    assert!(mount.options.iter().any(|option| option == "nosuid"));
}

#[test]
fn kata_start_preserves_explicit_terminal_mount() {
    let mut spec = spec();
    spec.runtime_class = "kata".into();
    spec.execution.mounts.push(adx_core::execution::Mount {
        create_if_missing: false,
        create_options: None,
        kind: "bind".into(),
        target: "/dev/pts".into(),
        options: vec!["rbind".into()],
        source: adx_core::execution::StorageSource::Local("/runtime/pts".into()).into(),
    });
    let request = start_request(&spec, "i-1", 1, &[], &Config::default()).unwrap();
    let mounts: Vec<_> = request
        .mounts
        .iter()
        .filter(|mount| mount.target == "/dev/pts")
        .collect();
    assert_eq!(mounts.len(), 1);
    assert_eq!(mounts[0].r#type, "bind");
    assert_eq!(
        mounts[0].source,
        Some(mount::Source::HostPath("/runtime/pts".into()))
    );
}

#[test]
fn other_runtimes_keep_their_terminal_preparation() {
    for runtime in ["runsc", "runc", "firecracker"] {
        let mut spec = spec();
        spec.runtime_class = runtime.into();
        let request = start_request(&spec, "i-1", 1, &[], &Config::default()).unwrap();
        assert!(!request
            .mounts
            .iter()
            .any(|mount| mount.target == "/dev/pts"));
    }
}

#[test]
fn start_maps_public_environment_contract_to_backend_protocol() {
    use adx_core::execution as model;
    let mut spec = spec();
    spec.execution = model::ExecutionOptions {
        rootfs: Some(model::Rootfs {
            readonly: true,
            source: model::StorageSource::Image("registry.example/app:v1".into()),
        }),
        mounts: vec![model::Mount {
            create_if_missing: false,
            create_options: None,
            kind: "bind".into(),
            target: "/workspace".into(),
            options: vec!["ro".into()],
            source: model::StorageSource::S3(model::S3Source {
                endpoint: "https://s3.example".into(),
                bucket: "artifacts".into(),
                object: "workspace.erofs".into(),
                access_key_id: "key".into(),
                access_key_secret: "secret".into(),
            })
            .into(),
        }],
        network: Some(model::NetworkPolicy {
            traffic: Some(model::TrafficPolicy {
                ingress_default_action: model::NetworkAction::Deny,
                egress_default_action: model::NetworkAction::Allow,
                rules: vec![],
                mode: model::TrafficMode::Stateful,
            }),
            dns: None,
        }),
        ports: vec![8080],
        inherit_entrypoint: true,
        limits: model::ResourceLimits {
            cpu_millis: 1500,
            memory_bytes: 2 << 30,
            disk_bytes: 4 << 30,
        },
        extra_config: "runtime-option=true".into(),
        ..Default::default()
    };
    let request = start_request(&spec, "i-1", 1, &[], &Config::default()).unwrap();
    let rootfs = request.rootfs.unwrap();
    assert_eq!(rootfs.r#type, RootfsSrcType::Image as i32);
    assert_eq!(
        rootfs.source,
        Some(rootfs_config::Source::ImageUrl(
            "registry.example/app:v1".into()
        ))
    );
    assert_eq!(request.mounts.len(), 1);
    assert!(matches!(
        request.mounts[0].source,
        Some(mount::Source::S3Config(_))
    ));
    assert_eq!(request.resources["CPU"], 1500.0);
    assert_eq!(request.resources["Memory"], 2048.0);
    assert_eq!(request.writable_layer_limit_bytes, 4 << 30);
    assert_eq!(request.extra_config, "runtime-option=true");
    assert_eq!(request.inject_entrypoint, "/etc/adx-image-process.json");
    assert_eq!(
        request.envs["ADX_IMAGE_PROCESS_CONFIG"],
        "/etc/adx-image-process.json"
    );
    let rules = &request.network_policy.unwrap().traffic.unwrap().rules;
    for port in [50090, 8080] {
        let rule = rules
            .iter()
            .find(|rule| {
                rule.sandbox_port_range
                    .as_ref()
                    .is_some_and(|range| range.first == port && range.last == port)
            })
            .expect("platform port must use a schema-v2 exact range");
        assert_eq!(rule.sandbox_port, 0);
        assert_eq!(rule.priority, u32::MAX);
    }
}

#[test]
fn start_maps_s3_rootfs_credentials() {
    use adx_core::execution as model;
    let mut spec = spec();
    spec.image.clear();
    spec.execution.rootfs = Some(model::Rootfs {
        readonly: false,
        source: model::StorageSource::S3(model::S3Source {
            endpoint: "https://s3.example".into(),
            bucket: "rootfs".into(),
            object: "app.erofs".into(),
            access_key_id: "key".into(),
            access_key_secret: "secret".into(),
        }),
    });
    let rootfs = start_request(&spec, "i-1", 1, &[], &Config::default())
        .unwrap()
        .rootfs
        .unwrap();
    let Some(rootfs_config::Source::S3Config(s3)) = rootfs.source else {
        panic!("S3 rootfs must reach sandboxd");
    };
    assert_eq!(s3.bucket, "rootfs");
    assert_eq!(s3.access_key_id, "key");
    assert_eq!(s3.access_key_secret, "secret");
}

#[tokio::test]
async fn dynamic_network_policy_uses_generated_backend_identity_and_can_clear() {
    use adx_core::execution as model;
    let server = Server::default();
    let (adapter, _harness) = connect(server.clone()).await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    let policy = model::NetworkPolicy {
        traffic: Some(model::TrafficPolicy {
            ingress_default_action: model::NetworkAction::Deny,
            egress_default_action: model::NetworkAction::Allow,
            rules: vec![],
            mode: model::TrafficMode::Stateful,
        }),
        dns: None,
    };
    adapter
        .set_network_policy("i-1", Some(&policy), &[8080], None)
        .await
        .unwrap();
    adapter
        .set_network_policy("i-1", None, &[8080], None)
        .await
        .unwrap();
    let updates = server.network_updates.lock().unwrap();
    assert_eq!(updates.len(), 2);
    assert_eq!(updates[0].sandbox_id, "generated-backend-id");
    let rules = &updates[0]
        .network_policy
        .as_ref()
        .unwrap()
        .traffic
        .as_ref()
        .unwrap()
        .rules;
    for port in [50090, 8080] {
        let rule = rules
            .iter()
            .find(|rule| {
                rule.sandbox_port_range
                    .as_ref()
                    .is_some_and(|range| range.first == port && range.last == port)
            })
            .expect("platform port must use a schema-v2 exact range");
        assert_eq!(rule.sandbox_port, 0);
        assert_eq!(rule.priority, u32::MAX);
    }
    assert!(updates[1].network_policy.is_none());
}

#[tokio::test]
async fn deleted_id_not_found_confirms_absence() {
    let server = Server::default();
    let (adapter, _server) = connect(server.clone()).await;
    assert_eq!(
        adapter
            .start(&spec(), "i-1", 1, &[])
            .await
            .unwrap()
            .to_string(),
        "10.0.0.2"
    );
    assert!(adapter.is_running("i-1").await.unwrap());
    {
        let starts = server.starts.lock().unwrap();
        assert!(starts[0].stdout.ends_with("/runtime-logs/i-1.out"));
        assert!(starts[0].stderr.ends_with("/runtime-logs/i-1.err"));
    }
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
async fn settled_start_errors_release_resources_after_confirmed_cleanup() {
    for code in [tonic::Code::Unknown, tonic::Code::Internal] {
        for remains in [false, true] {
            let server = Server {
                settled_error: Some(code),
                settled_error_after_start: remains,
                ..Default::default()
            };
            let (adapter, _server) = connect(server.clone()).await;
            assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
            adapter.remove("i-1").await.unwrap();
            assert!(!*server.running.lock().unwrap());
            assert_eq!(server.starts.lock().unwrap().len(), 1);
            if remains {
                assert!(server
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|x| x == "delete:generated-backend-id"));
            }
        }
    }
}

#[tokio::test]
async fn ambiguous_start_keeps_cleanup_pending_without_repeating_start() {
    let server = Server {
        transport_error: true,
        ..Default::default()
    };
    let (adapter, _server) = connect(server.clone()).await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert!(adapter.remove("i-1").await.is_err());
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert_eq!(*server.requests.lock().unwrap(), vec!["start:", "list:"]);
}

#[tokio::test]
async fn late_backend_of_failed_start_is_deleted_not_adopted() {
    let server = Server {
        transport_error: true,
        ..Default::default()
    };
    let (adapter, _server) = connect(server.clone()).await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert!(adapter.remove("i-1").await.is_err());
    *server.running.lock().unwrap() = true;
    // Targeted cleanup retries discover and remove the late execution.
    adapter.remove("i-1").await.unwrap();
    assert!(!*server.running.lock().unwrap());
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    assert_eq!(server.starts.lock().unwrap().len(), 1);
    assert!(server
        .requests
        .lock()
        .unwrap()
        .iter()
        .any(|s| s == "delete:generated-backend-id"));
}

#[derive(Default)]
struct LifecycleHooks {
    records: Mutex<Vec<EnvironmentRecord>>,
    activations: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl Readiness for LifecycleHooks {
    async fn wait_ready(&self, _: &EnvironmentRecord) -> adx_core::Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl Routes for LifecycleHooks {
    async fn activate(&self, _: &EnvironmentRecord) -> adx_core::Result<()> {
        self.activations
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    async fn retire(&self, _: &EnvironmentRecord) -> adx_core::Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl StateSink for LifecycleHooks {
    async fn commit(&self, record: &EnvironmentRecord) -> adx_core::Result<Durability> {
        self.records.lock().unwrap().push(record.clone());
        Ok(Durability::Published)
    }
}

fn lifecycle_node(adapter: Sandboxd, hooks: &Arc<LifecycleHooks>) -> Adxlet {
    let node = Adxlet::new(
        "n".into(),
        Arc::new(adapter),
        hooks.clone(),
        hooks.clone(),
        hooks.clone(),
    );
    node.update_capacity(spec().resources, Duration::from_secs(30))
        .unwrap();
    node
}

fn lifecycle_assignment() -> Assignment {
    Assignment {
        environment_id: "i".into(),
        node_id: "n".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    }
}

#[tokio::test]
async fn failed_create_retries_late_backend_cleanup_without_polling() {
    let server = Server {
        transport_error: true,
        ..Default::default()
    };
    let (adapter, _server) = connect(server.clone()).await;
    let hooks = Arc::new(LifecycleHooks::default());
    let hub = adxlet::control_stream::RuntimeControlHub::new(vec![7; 32], Duration::from_secs(1))
        .unwrap();
    let control = adxlet::runtime_control::RuntimeControlClient::new(50090, Duration::from_secs(1))
        .unwrap()
        .with_stream(hub);
    let node = lifecycle_node(adapter, &hooks)
        .with_runtime_control(control)
        .unwrap();
    let environment = node.environment(spec(), lifecycle_assignment()).unwrap();
    assert!(environment.create().await.is_err());
    assert_eq!(node.used(), Resources::default());
    assert_eq!(
        environment.sync().await.unwrap().record.state,
        EnvironmentState::Failed
    );
    *server.running.lock().unwrap() = true;
    tokio::time::timeout(Duration::from_secs(5), async {
        while *server.running.lock().unwrap() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(!*server.running.lock().unwrap());
    assert_eq!(node.used(), Resources::default());
    assert!(environment.create().await.is_err());
    assert_eq!(server.starts.lock().unwrap().len(), 1);
    assert_eq!(
        hooks.activations.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert!(hooks
        .records
        .lock()
        .unwrap()
        .iter()
        .all(|r| r.state != EnvironmentState::Running));
}

#[tokio::test]
async fn start_timeout_keeps_targeted_cleanup_for_late_backend() {
    let server = Server {
        release: Arc::new(Semaphore::new(0)),
        ..Default::default()
    };
    let (adapter, _server) = connect(server.clone()).await;
    let hooks = Arc::new(LifecycleHooks::default());
    let hub = adxlet::control_stream::RuntimeControlHub::new(vec![7; 32], Duration::from_secs(1))
        .unwrap();
    let control = adxlet::runtime_control::RuntimeControlClient::new(50090, Duration::from_secs(1))
        .unwrap()
        .with_stream(hub);
    let node = lifecycle_node(adapter, &hooks)
        .with_runtime_control(control)
        .unwrap()
        .with_operation_timeout(Duration::from_millis(100))
        .unwrap();
    let environment = node.environment(spec(), lifecycle_assignment()).unwrap();
    let create = {
        let environment = environment.clone();
        tokio::spawn(async move { environment.create().await })
    };
    server.entered.acquire().await.unwrap().forget();
    assert!(tokio::time::timeout(Duration::from_secs(1), create)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    let failed = environment.sync().await.unwrap().record;
    assert_eq!(failed.state, EnvironmentState::Failed);
    assert!(!failed.resources_held);
    assert_eq!(node.used(), Resources::default());
    server.release.add_permits(1);
    // The RPC has expired; model a backend completing work after cancellation.
    tokio::time::sleep(Duration::from_millis(20)).await;
    *server.running.lock().unwrap() = true;
    tokio::time::timeout(Duration::from_secs(5), async {
        // Wait for the server's delayed side effect, then automatic cleanup.
        loop {
            if server
                .requests
                .lock()
                .unwrap()
                .iter()
                .any(|request| request == "delete:generated-backend-id")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(!*server.running.lock().unwrap());
    let failed = environment.sync().await.unwrap().record;
    assert_eq!(failed.state, EnvironmentState::Failed);
    assert!(!failed.resources_held);
    assert_eq!(node.used(), Resources::default());
    assert!(environment.create().await.is_err());
    assert_eq!(server.starts.lock().unwrap().len(), 1);
    assert_eq!(
        hooks.activations.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert!(hooks
        .records
        .lock()
        .unwrap()
        .iter()
        .all(|r| r.state != EnvironmentState::Running));
}

#[tokio::test]
async fn transport_failure_can_clean_an_execution_discovered_by_labels() {
    let server = Server {
        transport_error_after_start: true,
        ..Default::default()
    };
    let (adapter, _server) = connect(server.clone()).await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());

    adapter.remove("i-1").await.unwrap();

    assert_eq!(
        *server.requests.lock().unwrap(),
        vec![
            "start:",
            "list:",
            "delete:generated-backend-id",
            "list:generated-backend-id",
        ]
    );
    assert!(!*server.running.lock().unwrap());
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
        settled_error: Some(tonic::Code::InvalidArgument),
        ..Default::default()
    })
    .await;
    assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
    adapter.remove("i-1").await.unwrap();
}

#[tokio::test]
async fn every_start_error_is_failed_and_absence_allows_cleanup() {
    for code in [
        tonic::Code::InvalidArgument,
        tonic::Code::Unimplemented,
        tonic::Code::PermissionDenied,
        tonic::Code::Unauthenticated,
    ] {
        // Every failed create is terminal for this execution identity.
        // A definite rejection needs no late-result cleanup retry.
        let (adapter, _server) = connect(Server {
            unsettled_error: Some(code),
            ..Default::default()
        })
        .await;
        assert!(adapter.start(&spec(), "i-1", 1, &[]).await.is_err());
        adapter.remove("i-1").await.unwrap();
        assert!(
            adapter.start(&spec(), "i-1", 1, &[]).await.is_err(),
            "status {code:?}"
        );
    }
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
async fn daemon_reconnect_adopts_preserved_runtime_without_second_start() {
    let server = Server::default();
    let (adapter, mut harness) = connect(server.clone()).await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    assert_eq!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.starts_with("start:"))
            .count(),
        1
    );

    let socket = harness._directory.path().join("rpc.sock");
    harness.task.abort();
    tokio::task::yield_now().await;
    std::fs::remove_file(&socket).unwrap();
    let listener = UnixListener::bind(&socket).unwrap();
    let service = server.clone();
    harness.task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(sandbox_service_server::SandboxServiceServer::new(service))
            .serve_with_incoming(UnixListenerStream::new(listener))
            .await
            .unwrap();
    });

    let reconnected = connect_when_ready(
        socket,
        Config {
            rpc_timeout: Duration::from_millis(100),
            ..Default::default()
        },
        Duration::from_secs(2),
        Duration::from_millis(10),
    )
    .await
    .unwrap();
    let observed = reconnected.inventory().await.unwrap();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].environment_id, "i");
    assert_eq!(observed[0].runtime_id, "i-1");
    assert_eq!(observed[0].generation, 1);
    assert!(reconnected.is_running("i-1").await.unwrap());
    assert_eq!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.starts_with("start:"))
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

#[tokio::test]
async fn checkpoint_uses_physical_id_and_restore_generates_new_backend_id_with_fresh_environment() {
    let server = Server::default();
    let starts = server.starts.clone();
    server.release.add_permits(1);
    let (adapter, _harness) = connect(server.clone()).await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    let checkpoint = tempfile::tempdir().unwrap();
    adapter
        .checkpoint("i-1", checkpoint.path(), Duration::from_secs(60))
        .await
        .unwrap();
    assert!(server
        .requests
        .lock()
        .unwrap()
        .iter()
        .any(|r| r == "checkpoint:leave_running=false"));
    adapter.remove("i-1").await.unwrap();
    adapter
        .restore(&spec(), "i-1-r5", 1, &[], checkpoint.path())
        .await
        .unwrap();
    assert!(adapter.is_running("i-1-r5").await.unwrap());
    let requests = starts.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|r| r.sandbox_id.is_empty()));
    assert_eq!(
        requests[0].envs["ADX_CHECKPOINT_HANDOFF_FILE"],
        "/run/backend/checkpoint"
    );
    assert_eq!(requests[1].envs["ADX_RUNTIME_ID"], "i-1-r5");
    assert_eq!(requests[1].envs["ADX_ENV_FILE"], "/run/backend/environment");
    assert_eq!(
        requests[1].checkpoint_info.as_ref().unwrap().checkpoint_dir,
        checkpoint.path().to_str().unwrap()
    );
}

#[tokio::test]
async fn node_startup_waits_for_backend_capabilities_readiness() {
    let server = Server::default();
    server.unready.store(2, std::sync::atomic::Ordering::SeqCst);
    let (adapter, _harness) = connect(server).await;
    adapter.wait_ready().await.unwrap();
}

#[tokio::test]
async fn node_startup_waits_when_sandboxd_socket_appears_late() {
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("late.sock");
    let waiting = tokio::spawn(connect_when_ready(
        socket.clone(),
        Config {
            rpc_timeout: Duration::from_millis(50),
            ..Default::default()
        },
        Duration::from_millis(50),
        Duration::from_millis(10),
    ));
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(!waiting.is_finished());

    let listener = UnixListener::bind(&socket).unwrap();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(sandbox_service_server::SandboxServiceServer::new(
                Server::default(),
            ))
            .serve_with_incoming(UnixListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::timeout(Duration::from_secs(2), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    server.abort();
}

#[tokio::test]
async fn workload_checkpoint_keeps_backend_running() {
    let server = Server::default();
    let (adapter, _harness) = connect(server.clone()).await;
    adapter.start(&spec(), "i-1", 1, &[]).await.unwrap();
    let checkpoint = tempfile::tempdir().unwrap();
    adapter
        .checkpoint_running("i-1", checkpoint.path(), Duration::from_secs(60))
        .await
        .unwrap();
    assert!(adapter.is_running("i-1").await.unwrap());
    assert!(server
        .requests
        .lock()
        .unwrap()
        .iter()
        .any(|r| r == "checkpoint:leave_running=true"));
}

#[tokio::test]
async fn stateless_policy_allows_execd_and_published_port_replies() {
    use adx_core::execution as model;
    let policy = model::NetworkPolicy {
        traffic: Some(model::TrafficPolicy {
            ingress_default_action: model::NetworkAction::Deny,
            egress_default_action: model::NetworkAction::Deny,
            rules: vec![],
            mode: model::TrafficMode::Stateless,
        }),
        dns: None,
    };
    let mut spec = spec();
    spec.execution.network = Some(policy.clone());
    spec.execution.ports = vec![8080];
    let server = Server::default();
    let (adapter, _harness) = connect(server.clone()).await;
    adapter.start(&spec, "i-1", 1, &[]).await.unwrap();
    adapter
        .set_network_policy("i-1", Some(&policy), &[8080], None)
        .await
        .unwrap();
    let starts = server.starts.lock().unwrap();
    let updates = server.network_updates.lock().unwrap();
    for policy in [
        starts[0].network_policy.as_ref().unwrap(),
        updates[0].network_policy.as_ref().unwrap(),
    ] {
        let traffic = policy.traffic.as_ref().unwrap();
        for port in [50090, 8080] {
            assert!(
                traffic.rules.iter().any(|rule| {
                    rule.action == NetworkPolicyAction::Allow as i32
                        && rule.direction == NetworkDirection::Both as i32
                        && rule.protocol == NetworkProtocol::Tcp as i32
                        && rule.priority == u32::MAX
                        && rule
                            .sandbox_port_range
                            .as_ref()
                            .is_some_and(|range| range.first == port && range.last == port)
                }),
                "stateless TCP replies from platform service port {port} must be allowed"
            );
        }
    }
}

#[tokio::test]
async fn blocked_policy_preserves_only_node_control_endpoint_at_start_and_update() {
    assert_blocked_control_endpoint(adx_core::execution::TrafficMode::Stateless).await;
}

#[tokio::test]
async fn stateful_block_preserves_only_node_control_endpoint_at_start_and_update() {
    // NetworkPolicy.block() normalizes to stateful deny in both directions.
    assert_blocked_control_endpoint(adx_core::execution::TrafficMode::Stateful).await;
}

async fn assert_blocked_control_endpoint(mode: adx_core::execution::TrafficMode) {
    use adx_core::execution as model;
    let policy = model::NetworkPolicy {
        traffic: Some(model::TrafficPolicy {
            ingress_default_action: model::NetworkAction::Deny,
            egress_default_action: model::NetworkAction::Deny,
            rules: vec![],
            mode,
        }),
        dns: None,
    };
    let env = HashMap::from([
        (
            "ADX_RUNTIME_CONTROL_ADDRESS".into(),
            "http://10.231.16.1:19003".into(),
        ),
        (
            "ADX_RUNTIME_CONTROL_SECRET".into(),
            "0123456789abcdef0123456789abcdef".into(),
        ),
    ]);
    let mut spec = spec();
    spec.execution.network = Some(policy.clone());
    // A user-provided callback must never become a platform-owned ACL exception.
    spec.env.insert(
        "ADX_RUNTIME_CONTROL_ADDRESS".into(),
        "http://10.99.1.1:5555".into(),
    );
    let request = start_request(
        &spec,
        "i-1",
        1,
        &[],
        &Config {
            env: env.clone(),
            ..Config::default()
        },
    )
    .unwrap();
    let server = Server::default();
    let (adapter, _harness) = connect_with_env(server.clone(), env).await;
    adapter.start(&spec, "i-1", 1, &[]).await.unwrap();
    adapter
        .set_network_policy("i-1", Some(&policy), &[], None)
        .await
        .unwrap();
    let starts = server.starts.lock().unwrap();
    let updates = server.network_updates.lock().unwrap();
    for policy in [
        request.network_policy.as_ref().unwrap(),
        starts[0].network_policy.as_ref().unwrap(),
        updates[0].network_policy.as_ref().unwrap(),
    ] {
        let traffic = policy.traffic.as_ref().unwrap();
        assert_eq!(
            traffic.mode,
            match mode {
                model::TrafficMode::Stateful => TrafficPolicyMode::Stateful as i32,
                model::TrafficMode::Stateless => TrafficPolicyMode::Stateless as i32,
            }
        );
        assert_eq!(
            traffic.ingress_default_action,
            NetworkPolicyAction::Deny as i32
        );
        assert_eq!(
            traffic.egress_default_action,
            NetworkPolicyAction::Deny as i32
        );
        let control: Vec<_> = traffic
            .rules
            .iter()
            .filter(|rule| {
                rule.direction == NetworkDirection::Both as i32 && rule.sandbox_port_range.is_none()
            })
            .collect();
        assert_eq!(
            control.len(),
            1,
            "only the node control TCP peer endpoint bypasses user egress policy"
        );
        let rule = control[0];
        assert_eq!(rule.action, NetworkPolicyAction::Allow as i32);
        assert_eq!(rule.protocol, NetworkProtocol::Tcp as i32);
        assert_eq!(rule.priority, u32::MAX);
        let peer = rule.peer.as_ref().unwrap();
        assert_eq!(peer.cidr, "10.231.16.1/32");
        assert!(peer.address.is_empty() && peer.domain.is_empty());
        assert_eq!(peer.port, 0);
        let port = peer.port_range.as_ref().unwrap();
        assert_eq!((port.first, port.last), (19003, 19003));
        assert!(rule.sandbox_port_range.is_none());
        assert_eq!(
            traffic.rules.len(),
            2,
            "only Execd and the control endpoint are allowed"
        );
    }
    let request = start_request(&spec, "i-1", 1, &[], &Config::default()).unwrap();
    assert!(!request
        .network_policy
        .unwrap()
        .traffic
        .unwrap()
        .rules
        .iter()
        .any(|r| r.direction == NetworkDirection::Both as i32 && r.sandbox_port_range.is_none()));
}

#[tokio::test]
async fn blocked_restore_uses_target_node_control_endpoint_and_credential() {
    use adx_core::{execution as model, runtime::RuntimeIdentity};
    use adx_protocol::runtime_stream::execution_token;

    let mut spec = spec();
    spec.execution.network = Some(model::NetworkPolicy {
        traffic: Some(model::TrafficPolicy {
            ingress_default_action: model::NetworkAction::Deny,
            egress_default_action: model::NetworkAction::Deny,
            rules: vec![],
            mode: model::TrafficMode::Stateful,
        }),
        dns: None,
    });
    let source = RuntimeIdentity {
        environment_id: spec.id.clone(),
        runtime_id: "i-1".into(),
        ownership_generation: 1,
    };
    // A checkpoint can contain the old node's callback environment. None of
    // these values may control the target's network exception or credential.
    spec.env.insert(
        "ADX_RUNTIME_CONTROL_ADDRESS".into(),
        "http://10.231.16.1:19003".into(),
    );
    spec.env
        .insert("ADX_RUNTIME_CONTROL_TOKEN".into(), "old-token".into());
    spec.env
        .insert("ADX_RUNTIME_CONTROL_SECRET".into(), "old-secret".into());
    let target_address = "http://10.232.16.1:19004";
    let target_secret = "target-node-secret-0123456789abcdef";
    let target = RuntimeIdentity {
        environment_id: spec.id.clone(),
        runtime_id: "i-2".into(),
        ownership_generation: 2,
    };
    let server = Server::default();
    let (adapter, _harness) = connect_with_env(
        server.clone(),
        HashMap::from([
            ("ADX_RUNTIME_CONTROL_ADDRESS".into(), target_address.into()),
            ("ADX_RUNTIME_CONTROL_SECRET".into(), target_secret.into()),
        ]),
    )
    .await;
    let checkpoint = tempfile::tempdir().unwrap();
    adapter
        .restore_from(
            &spec,
            &target.runtime_id,
            target.ownership_generation,
            &[],
            checkpoint.path(),
            Some(&source),
        )
        .await
        .unwrap();
    let starts = server.starts.lock().unwrap();
    let request = &starts[0];
    assert_eq!(request.envs["ADX_RUNTIME_CONTROL_ADDRESS"], target_address);
    assert_eq!(
        request.envs["ADX_RUNTIME_CONTROL_TOKEN"],
        execution_token(target_secret.as_bytes(), &target).unwrap()
    );
    assert!(!request.envs.contains_key("ADX_RUNTIME_CONTROL_SECRET"));
    assert_eq!(
        request.checkpoint_info.as_ref().unwrap().checkpoint_dir,
        checkpoint.path().to_str().unwrap()
    );
    assert_eq!(
        serde_json::from_str::<RuntimeIdentity>(&request.envs["ADX_RESTORE_ORIGIN"]).unwrap(),
        source
    );
    let traffic = request
        .network_policy
        .as_ref()
        .unwrap()
        .traffic
        .as_ref()
        .unwrap();
    assert_eq!(traffic.mode, TrafficPolicyMode::Stateful as i32);
    assert_eq!(
        traffic.ingress_default_action,
        NetworkPolicyAction::Deny as i32
    );
    assert_eq!(
        traffic.egress_default_action,
        NetworkPolicyAction::Deny as i32
    );
    let peer_rules: Vec<_> = traffic
        .rules
        .iter()
        .filter(|rule| rule.sandbox_port_range.is_none())
        .collect();
    assert_eq!(peer_rules.len(), 1);
    let rule = peer_rules[0];
    assert_eq!(rule.action, NetworkPolicyAction::Allow as i32);
    assert_eq!(rule.direction, NetworkDirection::Both as i32);
    assert_eq!(rule.protocol, NetworkProtocol::Tcp as i32);
    assert_eq!(rule.priority, u32::MAX);
    let peer = rule.peer.as_ref().unwrap();
    assert_eq!(peer.cidr, "10.232.16.1/32");
    assert_eq!(peer.port_range.as_ref().unwrap().first, 19004);
    assert_eq!(peer.port_range.as_ref().unwrap().last, 19004);
    assert_eq!(traffic.rules.len(), 2);
}

#[tokio::test]
async fn stateful_policy_keeps_execd_replies_independent_of_conntrack_generation() {
    use adx_core::execution as model;
    let policy = model::NetworkPolicy {
        traffic: Some(model::TrafficPolicy {
            ingress_default_action: model::NetworkAction::Deny,
            egress_default_action: model::NetworkAction::Deny,
            rules: vec![],
            mode: model::TrafficMode::Stateful,
        }),
        dns: None,
    };
    let mut spec = spec();
    spec.execution.network = Some(policy.clone());
    let server = Server::default();
    let (adapter, _harness) = connect(server.clone()).await;
    adapter.start(&spec, "i-1", 1, &[]).await.unwrap();
    adapter
        .set_network_policy("i-1", Some(&policy), &[], None)
        .await
        .unwrap();
    let starts = server.starts.lock().unwrap();
    let updates = server.network_updates.lock().unwrap();
    for policy in [
        starts[0].network_policy.as_ref().unwrap(),
        updates[0].network_policy.as_ref().unwrap(),
    ] {
        let traffic = policy.traffic.as_ref().unwrap();
        assert_eq!(
            traffic.ingress_default_action,
            NetworkPolicyAction::Deny as i32
        );
        assert_eq!(
            traffic.egress_default_action,
            NetworkPolicyAction::Deny as i32
        );
        assert!(
            traffic.rules.iter().any(|rule| {
                rule.action == NetworkPolicyAction::Allow as i32
                    && rule.direction == NetworkDirection::Both as i32
                    && rule.protocol == NetworkProtocol::Tcp as i32
                    && rule.priority == u32::MAX
                    && rule
                        .sandbox_port_range
                        .as_ref()
                        .is_some_and(|range| range.first == 50090 && range.last == 50090)
            }),
            "Execd replies must survive updates to the stateful policy generation"
        );
    }
}

#[tokio::test]
async fn exited_backend_uses_wait_reason_before_deletion_and_not_exit_code_alone() {
    for (message, reason) in [
        (
            "sandbox was oom-killed by the kernel (memory cgroup limit exceeded)",
            adx_core::RuntimeFailureReason::OomKilled,
        ),
        ("", adx_core::RuntimeFailureReason::Exited),
    ] {
        let server = Server {
            exit_message: Some(message.into()),
            ..Default::default()
        };
        let (adapter, _guard) = connect(server.clone()).await;
        RuntimeDriver::start(&adapter, &spec(), "i-1", 1, &[])
            .await
            .unwrap();
        assert!(adapter.observe("i-1").await.unwrap().running);
        assert!(!server
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|v| v.starts_with("wait:")));
        *server.running.lock().unwrap() = false;
        let status = adapter.observe("i-1").await.unwrap();
        assert!(!status.running);
        let failure = status.failure.unwrap();
        assert_eq!(failure.reason, reason);
        assert_eq!(failure.exit_code, Some(137));
        assert!(server
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|v| v == "wait:generated-backend-id"));
    }
}

#[test]
fn constructor_cwd_is_owned_by_spec_and_does_not_change_execd_process_cwd() {
    let mut spec = spec();
    spec.env
        .insert("ADX_ENVIRONMENT_CWD".into(), "/spoofed".into());
    let mut config = Config::default();
    config
        .env
        .insert("ADX_ENVIRONMENT_CWD".into(), "/deployment".into());
    let omitted = start_request(&spec, "i-1", 1, &[], &config).unwrap();
    assert!(!omitted.envs.contains_key("ADX_ENVIRONMENT_CWD"));
    spec.execution.cwd = Some("/workspace/nested".into());
    let request = start_request(&spec, "i-1", 1, &[], &config).unwrap();
    assert_eq!(request.envs["ADX_ENVIRONMENT_CWD"], "/workspace/nested");
    assert_eq!(request.cwd, "/");
}

#[tokio::test]
async fn cold_start_uses_create_deadline_beyond_the_node_rpc_cap() {
    let server = Server {
        release: Arc::new(Semaphore::new(0)),
        ..Default::default()
    };
    let (adapter, _server) = connect(server.clone()).await;
    let task = {
        let adapter = adapter.clone();
        tokio::spawn(async move {
            adapter
                .start_with_deadline(
                    &spec(),
                    "i-1",
                    1,
                    &[],
                    tokio::time::Instant::now() + Duration::from_secs(5),
                )
                .await
        })
    };
    server.entered.acquire().await.unwrap().forget();
    // The fixture's node RPC cap is two seconds; cold import takes longer.
    tokio::time::sleep(Duration::from_millis(2200)).await;
    server.release.add_permits(1);
    assert_eq!(task.await.unwrap().unwrap().to_string(), "10.0.0.2");
    assert_eq!(server.starts.lock().unwrap().len(), 1);
    adapter.remove("i-1").await.unwrap();
}

#[tokio::test]
async fn expired_create_deadline_never_issues_start() {
    let server = Server::default();
    let (adapter, _server) = connect(server.clone()).await;
    assert!(adapter
        .start_with_deadline(
            &spec(),
            "i-1",
            1,
            &[],
            tokio::time::Instant::now() - Duration::from_millis(1)
        )
        .await
        .is_err());
    assert!(server.starts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn controller_cold_preparation_uses_total_create_budget_then_readiness_budget() {
    let server = Server {
        release: Arc::new(Semaphore::new(0)),
        ..Default::default()
    };
    let (adapter, _server) = connect(server.clone()).await;
    let hooks = Arc::new(LifecycleHooks::default());
    let node = lifecycle_node(adapter, &hooks)
        .with_operation_timeout(Duration::from_millis(100))
        .unwrap();
    let environment = node.environment(spec(), lifecycle_assignment()).unwrap();
    let task = {
        let environment = environment.clone();
        tokio::spawn(async move {
            environment
                .create_with_deadline(tokio::time::Instant::now() + Duration::from_secs(3))
                .await
        })
    };
    server.entered.acquire().await.unwrap().forget();
    tokio::time::sleep(Duration::from_millis(200)).await;
    server.release.add_permits(1);
    assert_eq!(
        task.await.unwrap().unwrap().record.state,
        EnvironmentState::Running
    );
    assert_eq!(
        environment
            .create_with_deadline(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .record
            .state,
        EnvironmentState::Running
    );
    assert_eq!(server.starts.lock().unwrap().len(), 1);
    environment.delete().await.unwrap();
}

#[tokio::test]
async fn internal_backend_timeouts_keep_stage_before_create_deadline() {
    for message in [
        "image pull (rootfs): net/http: TLS handshake timeout",
        "runtime start (runc): waiting for runc container start: context deadline exceeded",
    ] {
        let server = Server {
            start_timeout: Some(message.into()),
            ..Default::default()
        };
        let (adapter, _server) = connect(server.clone()).await;
        let hooks = Arc::new(LifecycleHooks::default());
        let node = lifecycle_node(adapter, &hooks);
        let environment = node.environment(spec(), lifecycle_assignment()).unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let error = environment
            .create_with_deadline(deadline)
            .await
            .unwrap_err();
        assert!(tokio::time::Instant::now() < deadline);
        let status = adx_protocol::status(error);
        assert_eq!(status.code(), tonic::Code::DeadlineExceeded, "{status:?}");
        assert!(status.message().contains(message), "{status:?}");
        assert!(adx_protocol::deadline_failure(&status).is_some());
        let record = environment.sync().await.unwrap().record;
        assert_eq!(record.state, EnvironmentState::Failed);
        assert_eq!(record.failure.unwrap().message, status.message());
        assert!(!record.resources_held);
        assert_eq!(server.starts.lock().unwrap().len(), 1);
        assert_eq!(
            environment
                .create_with_deadline(deadline)
                .await
                .unwrap_err(),
            adx_core::Error::Conflict
        );
        assert_eq!(server.starts.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn backend_start_errors_preserve_non_timeout_status_and_message() {
    for code in [
        tonic::Code::InvalidArgument,
        tonic::Code::NotFound,
        tonic::Code::AlreadyExists,
        tonic::Code::PermissionDenied,
        tonic::Code::Unauthenticated,
        tonic::Code::ResourceExhausted,
        tonic::Code::FailedPrecondition,
        tonic::Code::Aborted,
        tonic::Code::OutOfRange,
        tonic::Code::Unimplemented,
        tonic::Code::Internal,
        tonic::Code::Unknown,
        tonic::Code::Cancelled,
        tonic::Code::Unavailable,
        tonic::Code::DataLoss,
        tonic::Code::DeadlineExceeded,
    ] {
        let (adapter, _server) = connect(Server {
            unsettled_error: Some(code),
            ..Default::default()
        })
        .await;
        let error = adapter
            .start_with_deadline(
                &spec(),
                "i-1",
                1,
                &[],
                tokio::time::Instant::now() + Duration::from_secs(30),
            )
            .await
            .unwrap_err();
        let status = adx_protocol::status(error);
        assert_eq!(status.code(), code, "{status:?}");
        assert_eq!(
            status.metadata().get("adx-backend-failure").unwrap(),
            "true"
        );
        assert!(status
            .message()
            .contains("runtime start failed without settlement proof"));
        assert!(adx_protocol::deadline_failure(&status).is_none());
    }
}

#[tokio::test]
async fn backend_transport_failure_remains_unavailable() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let transport = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap_err();
    let status = adx_protocol::status(adx_protocol::backend_error(Status::from_error(Box::new(
        transport,
    ))));
    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert!(status.message().contains("sandboxd transport"));
    assert!(adx_protocol::deadline_failure(&status).is_none());
}

#[tokio::test]
async fn mixed_router_delete_before_start_fences_jiuwenbox_too() {
    let (sandboxd, _harness) = connect(Server::default()).await;
    let directory = tempfile::tempdir().unwrap();
    let config = serde_json::from_value(serde_json::json!({
        "socket": directory.path().join("unused.sock"),
        "state_dir": directory.path().join("journal"),
        "bootstrap": {"image":directory.path().join("unused.erofs"),"sha256":"a".repeat(64),"target":"/__adx","command":["/__adx/usr/local/bin/adx-execd"]},
        "classes": {"supervisor":{"images":{"execd:test":{}}}}
    })).unwrap();
    let jiuwenbox = adxlet::jiuwenbox::Jiuwenbox::with_host(
        config,
        Config::default(),
        Arc::new(adxlet::jiuwenbox::host::LinuxHost),
    )
    .await
    .unwrap();
    let router =
        adxlet::runtime_router::RuntimeRouter::new(Some(sandboxd), Some(jiuwenbox.clone()))
            .unwrap();
    router.remove("i-1").await.unwrap();
    assert!(jiuwenbox.owns("i-1").await.unwrap());
    let mut wanted = spec();
    wanted.runtime_class = "supervisor".into();
    wanted.resources.disk_bytes = 0;
    assert!(matches!(
        router.start(&wanted, "i-1", 1, &[]).await,
        Err(adx_core::Error::Conflict)
    ));
}

#[tokio::test]
async fn public_authority_cycle_collects_sandboxd_orphan_without_local_controller() {
    let server = Server::default();
    let (creator, harness) = connect(server.clone()).await;
    let adapter = Sandboxd::connect(
        harness._directory.path().join("rpc.sock"),
        Config::default(),
    )
    .await
    .unwrap();
    let router = adxlet::runtime_router::RuntimeRouter::new(Some(adapter), None).unwrap();
    let node = authority::manager(Arc::new(router));
    let mut authority = authority::Fixture::new().await;
    authority.cycle(&node).await.unwrap();
    // An external successful create has no corresponding node controller/catalog entry.
    creator.start(&spec(), "i-1", 1, &[]).await.unwrap();
    assert!(*server.running.lock().unwrap());
    authority.cycle(&node).await.unwrap();
    assert!(*server.running.lock().unwrap());
    authority.registration.reconciling = true;
    authority.cycle(&node).await.unwrap();
    assert!(!*server.running.lock().unwrap());
    assert_eq!(server.starts.lock().unwrap().len(), 1);
    assert_eq!(node.used(), Resources::default());
    assert!(node.accepting_allocations());
    assert!(
        authority
            .authority
            .reports
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .accepting_allocations
    );
}

#[tokio::test]
async fn public_authority_cycle_reaps_transport_lost_create_after_empty_scan() {
    let server = Server {
        transport_error: true,
        ..Default::default()
    };
    let (creator, harness) = connect(server.clone()).await;
    let adapter = Sandboxd::connect(
        harness._directory.path().join("rpc.sock"),
        Config::default(),
    )
    .await
    .unwrap();
    let node = authority::manager(Arc::new(adapter));
    assert!(creator.start(&spec(), "i-1", 1, &[]).await.is_err());
    let mut authority = authority::Fixture::new().await;
    authority.cycle(&node).await.unwrap();
    // The first complete inventory was empty; the failed create materializes later.
    *server.running.lock().unwrap() = true;
    authority.cycle(&node).await.unwrap();
    assert!(*server.running.lock().unwrap());
    authority.registration.reconciling = true;
    authority.cycle(&node).await.unwrap();
    assert!(!*server.running.lock().unwrap());
    assert!(server
        .requests
        .lock()
        .unwrap()
        .iter()
        .any(|r| r == "delete:generated-backend-id"));
    assert_eq!(server.starts.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn blocked_tunnel_listeners_are_trusted_and_never_add_an_egress_exception() {
    use adx_core::execution as model;
    let policy = model::NetworkPolicy {
        traffic: Some(model::TrafficPolicy {
            ingress_default_action: model::NetworkAction::Deny,
            egress_default_action: model::NetworkAction::Deny,
            rules: vec![],
            mode: model::TrafficMode::Stateful,
        }),
        dns: None,
    };
    for http_port in [8766, 18766] {
        let mut spec = spec();
        spec.execution.network = Some(policy.clone());
        spec.execution.tunnel_http_port = Some(http_port);
        spec.env
            .insert("EXECD_TUNNEL_WS_PORT".into(), "12345".into());
        let server = Server::default();
        let (adapter, _harness) = connect(server.clone()).await;
        adapter.start(&spec, "i-1", 1, &[]).await.unwrap();
        let router = adxlet::runtime_router::RuntimeRouter::new(Some(adapter), None).unwrap();
        router
            .set_network_policy("i-1", Some(&policy), &[], Some(http_port))
            .await
            .unwrap();
        let starts = server.starts.lock().unwrap();
        let updates = server.network_updates.lock().unwrap();
        assert_eq!(
            starts[0].envs["EXECD_TUNNEL_WS_PORT"],
            (http_port - 1).to_string()
        );
        for policy in [
            starts[0].network_policy.as_ref().unwrap(),
            updates[0].network_policy.as_ref().unwrap(),
        ] {
            let traffic = policy.traffic.as_ref().unwrap();
            let rules: Vec<_> = traffic
                .rules
                .iter()
                .filter(|rule| {
                    rule.sandbox_port_range
                        .as_ref()
                        .is_some_and(|range| range.first == u32::from(http_port - 1))
                })
                .collect();
            assert_eq!(rules.len(), 1);
            assert_eq!(rules[0].direction, NetworkDirection::Ingress as i32);
            assert_eq!(rules[0].protocol, NetworkProtocol::Tcp as i32);
            assert_eq!(rules[0].priority, u32::MAX);
            assert!(!traffic.rules.iter().any(|rule| rule
                .sandbox_port_range
                .as_ref()
                .is_some_and(|range| range.first == u32::from(http_port) || range.first == 12345)));
        }
    }
    let mut spec = spec();
    spec.env
        .insert("EXECD_TUNNEL_WS_PORT".into(), "12345".into());
    let request = start_request(&spec, "i-1", 1, &[], &Config::default()).unwrap();
    assert!(!request.envs.contains_key("EXECD_TUNNEL_WS_PORT"));
}
