//! Local readiness HTTP contract checks, independent of a full platform deployment.
use adx_core::{Assignment, InstanceRecord, InstanceSpec, InstanceState, Resources, Result};
use adx_node_manager::{readiness::RrtReadiness, Readiness, RuntimeBackend};
use async_trait::async_trait;
use std::{
    net::IpAddr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

struct Runtime {
    running: AtomicBool,
    checks: AtomicUsize,
}
#[async_trait]
impl RuntimeBackend for Runtime {
    async fn start(
        &self,
        _: &InstanceSpec,
        _: &str,
        _: u64,
        _: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<IpAddr> {
        unreachable!()
    }
    async fn remove(&self, _: &str) -> Result<()> {
        unreachable!()
    }
    async fn is_running(&self, _: &str) -> Result<bool> {
        self.checks.fetch_add(1, Ordering::SeqCst);
        Ok(self.running.load(Ordering::SeqCst))
    }
}
fn record() -> InstanceRecord {
    InstanceRecord {
        restart_attempts: 0,
        restart_pending: false,
        spec: InstanceSpec {
            snapshot_id: None,
            lifecycle: Default::default(),
            env: Default::default(),
            scheduling: Default::default(),
            id: "i".into(),
            tenant_id: "t".into(),
            runtime: "runsc".into(),
            image: "rrt".into(),
            resources: Resources {
                cpu_millis: 1000,
                memory_bytes: 1 << 30,
                disk_bytes: 1 << 30,
            },
            priority: 0,
        },
        assignment: Assignment {
            devices: vec![],
            instance_id: "i".into(),
            node_id: "n".into(),
            shard_id: 0,
            generation: 1,
        },
        state: InstanceState::Starting,
        revision: 1,
        runtime_id: "i-1".into(),
        resources_held: true,
        runtime_ip: Some("127.0.0.1".parse().unwrap()),
        checkpoint: None,
        last_operation: None,
    }
}
struct Server(tokio::task::JoinHandle<()>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}
async fn serve(
    body: &'static str,
    stopped: Option<Arc<Runtime>>,
) -> (u16, Server, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let requests = Arc::new(AtomicUsize::new(0));
    let count = requests.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut data = vec![0; 4096];
            let length = socket.read(&mut data).await.unwrap();
            assert!(String::from_utf8_lossy(&data[..length])
                .starts_with("GET /control/v1/status HTTP/1.1"));
            count.fetch_add(1, Ordering::SeqCst);
            if let Some(runtime) = &stopped {
                runtime.running.store(false, Ordering::SeqCst);
            }
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
        }
    });
    (port, Server(task), requests)
}
fn runtime(running: bool) -> Arc<Runtime> {
    Arc::new(Runtime {
        running: AtomicBool::new(running),
        checks: AtomicUsize::new(0),
    })
}
fn checker(runtime: Arc<Runtime>, port: u16) -> RrtReadiness {
    RrtReadiness::new(
        runtime,
        port,
        Duration::from_millis(10),
        Duration::from_millis(100),
        Duration::from_millis(500),
    )
    .unwrap()
}
#[tokio::test]
async fn requires_runtime_and_valid_rrt_response() {
    let runtime = runtime(true);
    let (port, _server, requests) = serve(r#"{"identity":{"instance_id":"i","runtime_id":"i-1","ownership_generation":1},"revision":1,"phase":"running","checkpoint":null,"active_requests":0,"active_commands":0}"#, None).await;
    checker(runtime.clone(), port)
        .wait_ready(&record())
        .await
        .unwrap();
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.checks.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn an_http_200_without_rrt_status_is_not_ready() {
    let (port, _server, requests) = serve(r#"{"status":"starting"}"#, None).await;
    assert!(checker(runtime(true), port)
        .wait_ready(&record())
        .await
        .is_err());
    assert!(requests.load(Ordering::SeqCst) > 1);
}
#[tokio::test]
async fn exited_runtime_does_not_probe_or_publish_ready() {
    let (port, _server, requests) = serve(r#"{"identity":{"instance_id":"i","runtime_id":"i-1","ownership_generation":1},"revision":1,"phase":"running","checkpoint":null,"active_requests":0,"active_commands":0}"#, None).await;
    assert!(checker(runtime(false), port)
        .wait_ready(&record())
        .await
        .is_err());
    assert_eq!(requests.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn runtime_exit_during_probe_is_not_ready() {
    let runtime = runtime(true);
    let (port, _server, _) = serve(r#"{"identity":{"instance_id":"i","runtime_id":"i-1","ownership_generation":1},"revision":1,"phase":"running","checkpoint":null,"active_requests":0,"active_commands":0}"#, Some(runtime.clone())).await;
    assert!(checker(runtime, port).wait_ready(&record()).await.is_err());
}
