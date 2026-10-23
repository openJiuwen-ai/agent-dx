//! Local readiness HTTP contract checks, independent of a full platform deployment.
use adx_core::{
    Assignment, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Resources, Result,
};
use adxlet::{readiness::ExecdReadiness, Readiness, RuntimeDriver};
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
impl RuntimeDriver for Runtime {
    async fn start(
        &self,
        _: &EnvironmentSpec,
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
fn record() -> EnvironmentRecord {
    EnvironmentRecord {
        failure: None,
        restart_attempts: 0,
        restart_pending: false,
        spec: EnvironmentSpec {
            template_identity: None,
            compatibility: None,
            runtime_profile: None,
            snapshot_id: None,
            lifecycle: Default::default(),
            env: Default::default(),
            scheduling: Default::default(),
            id: "i".into(),
            tenant_id: "t".into(),
            runtime_class: "runsc".into(),
            image: "execd".into(),
            resources: Resources {
                cpu_millis: 1000,
                memory_bytes: 1 << 30,
                disk_bytes: 1 << 30,
            },
            priority: 0,
            execution: Default::default(),
        },
        assignment: Assignment {
            devices: vec![],
            environment_id: "i".into(),
            node_id: "n".into(),
            shard_id: 0,
            generation: 1,
        },
        state: EnvironmentState::Starting,
        revision: 1,
        runtime: adx_core::Runtime {
            id: "i-1".into(),
            ip: Some("127.0.0.1".parse().unwrap()),
        },
        resources_held: true,
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
    body: impl Into<String>,
    stopped: Option<Arc<Runtime>>,
) -> (u16, Server, Arc<AtomicUsize>) {
    let body = body.into();
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
fn checker(runtime: Arc<Runtime>, port: u16) -> ExecdReadiness {
    ExecdReadiness::new(
        runtime,
        port,
        Duration::from_millis(10),
        Duration::from_millis(100),
        Duration::from_millis(500),
    )
    .unwrap()
}
#[tokio::test]
async fn requires_runtime_and_valid_execd_response() {
    let runtime = runtime(true);
    let (port, _server, requests) = serve(r#"{"identity":{"environment_id":"i","runtime_id":"i-1","ownership_generation":1},"revision":1,"phase":"running","checkpoint":null,"active_requests":0,"active_commands":0}"#, None).await;
    checker(runtime.clone(), port)
        .wait_ready(&record())
        .await
        .unwrap();
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.checks.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn an_http_200_without_execd_status_is_not_ready() {
    let (port, _server, requests) = serve(r#"{"status":"starting"}"#, None).await;
    assert!(checker(runtime(true), port)
        .wait_ready(&record())
        .await
        .is_err());
    assert!(requests.load(Ordering::SeqCst) > 1);
}
#[tokio::test]
async fn exited_runtime_does_not_publish_ready() {
    let (port, _server, requests) = serve(r#"{"identity":{"environment_id":"i","runtime_id":"i-1","ownership_generation":1},"revision":1,"phase":"running","checkpoint":null,"active_requests":0,"active_commands":0}"#, None).await;
    assert!(checker(runtime(false), port)
        .wait_ready(&record())
        .await
        .is_err());
    assert!(requests.load(Ordering::SeqCst) > 1);
}

#[tokio::test]
async fn failed_execd_probes_do_not_poll_the_backend() {
    let runtime = runtime(true);
    let (port, _server, requests) = serve(r#"{"status":"starting"}"#, None).await;
    assert!(checker(runtime.clone(), port)
        .wait_ready(&record())
        .await
        .is_err());
    assert!(requests.load(Ordering::SeqCst) > 1);
    assert_eq!(runtime.checks.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn runtime_exit_during_probe_is_not_ready() {
    let runtime = runtime(true);
    let (port, _server, _) = serve(r#"{"identity":{"environment_id":"i","runtime_id":"i-1","ownership_generation":1},"revision":1,"phase":"running","checkpoint":null,"active_requests":0,"active_commands":0}"#, Some(runtime.clone())).await;
    assert!(checker(runtime, port).wait_ready(&record()).await.is_err());
}

#[tokio::test]
async fn detached_background_command_does_not_keep_idle_activity_open() {
    let (port, _server, _) = serve(
        r#"{"identity":{"environment_id":"i","runtime_id":"i-1","ownership_generation":1},"revision":1,"phase":"running","checkpoint":null,"active_requests":0,"active_commands":1,"activity_revision":7}"#,
        None,
    )
    .await;
    assert_eq!(
        checker(runtime(true), port)
            .activity(&record())
            .await
            .unwrap(),
        (7, 0)
    );
}

#[tokio::test]
async fn active_client_request_still_blocks_idle_activity() {
    let (port, _server, _) = serve(
        r#"{"identity":{"environment_id":"i","runtime_id":"i-1","ownership_generation":1},"revision":1,"phase":"running","checkpoint":null,"active_requests":1,"active_commands":1,"activity_revision":8}"#,
        None,
    )
    .await;
    assert_eq!(
        checker(runtime(true), port)
            .activity(&record())
            .await
            .unwrap(),
        (8, 1)
    );
}

#[tokio::test]
async fn e2b_requires_ready_envd_with_the_requested_protocol_version() {
    for (service, ready) in [
        (serde_json::json!({}), false),
        (
            serde_json::json!({"envd":{"phase":"starting","version":"0.9.0"}}),
            false,
        ),
        (serde_json::json!({"envd":{"phase":"ready"}}), false),
        (
            serde_json::json!({"envd":{"phase":"ready","version":"0.8.0"}}),
            false,
        ),
        (
            serde_json::json!({"envd":{"phase":"failed","version":"0.9.0"}}),
            false,
        ),
        (
            serde_json::json!({"envd":{"phase":"ready","version":"0.9.0"}}),
            true,
        ),
    ] {
        let mut record = record();
        record.spec.compatibility = Some(adx_core::environment_control::EnvironmentCompatibility {
            template_id: "base".into(),
            envd_version: "0.9.0".into(),
            envd_access_token: "a".repeat(32),
            traffic_access_token: "b".repeat(32),
            timeout_seconds: 60,
            timeout_action: "delete".into(),
            allow_public_traffic: true,
        });
        let body = serde_json::json!({
            "identity":{"environment_id":"i","runtime_id":"i-1","ownership_generation":1},
            "revision":1,"phase":"running","checkpoint":null,"active_requests":0,"active_commands":0,
            "required_services":service,
        }).to_string();
        let (port, _server, _) = serve(body, None).await;
        assert_eq!(
            checker(runtime(true), port)
                .wait_ready(&record)
                .await
                .is_ok(),
            ready
        );
    }
}

#[tokio::test]
async fn e2b_activity_requires_ready_envd_with_matching_version() {
    for (service, healthy) in [
        (serde_json::json!({}), false),
        (
            serde_json::json!({"envd":{"phase":"failed","version":"0.9.0"}}),
            false,
        ),
        (
            serde_json::json!({"envd":{"phase":"ready","version":"0.8.0"}}),
            false,
        ),
        (
            serde_json::json!({"envd":{"phase":"ready","version":"0.9.0"}}),
            true,
        ),
    ] {
        let mut record = record();
        record.spec.compatibility = Some(adx_core::environment_control::EnvironmentCompatibility {
            template_id: "base".into(),
            envd_version: "0.9.0".into(),
            envd_access_token: "a".repeat(32),
            traffic_access_token: "b".repeat(32),
            allow_public_traffic: true,
            timeout_seconds: 60,
            timeout_action: "delete".into(),
        });
        let body = serde_json::json!({
            "identity":{"environment_id":"i","runtime_id":"i-1","ownership_generation":1},
            "revision":1,"phase":"running","checkpoint":null,
            "active_requests":0,"active_commands":0,"activity_revision":1,
            "required_services":service,
        })
        .to_string();
        let (port, _server, _) = serve(body, None).await;
        assert_eq!(
            checker(runtime(true), port).activity(&record).await.is_ok(),
            healthy
        );
    }
}

#[tokio::test]
async fn terminal_readiness_failure_is_not_retried_as_transport_error() {
    let (port, _server, requests) = serve(r#"{"identity":{"environment_id":"i","runtime_id":"i-1","ownership_generation":1},"revision":2,"phase":"failed","failure":{"kind":"readiness_timeout","message":"HTTP 503","consecutive_failures":3},"checkpoint":null,"active_requests":0,"active_commands":0}"#, None).await;
    let result = checker(runtime(true), port)
        .wait_ready(&record())
        .await
        .unwrap_err();
    assert!(matches!(result, adx_core::Error::ProbeFailed(_)));
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn probe_observation_uses_http_when_control_stream_is_disconnected() {
    let (port, _server, requests) = serve(r#"{"identity":{"environment_id":"i","runtime_id":"i-1","ownership_generation":1},"revision":2,"phase":"failed","failure":{"kind":"healthcheck_failed","message":"HTTP 503","consecutive_failures":3,"http_status":503},"checkpoint":null,"active_requests":0,"active_commands":0}"#, None).await;
    let hub =
        adxlet::control_stream::RuntimeControlHub::new(vec![7; 32], Duration::from_millis(100))
            .unwrap();
    let record = record();
    let client =
        adxlet::runtime_control::RuntimeControlClient::new(port, Duration::from_millis(100))
            .unwrap()
            .with_stream(hub);
    let checker = ExecdReadiness::with_client(
        runtime(true),
        client,
        Duration::from_millis(10),
        Duration::from_millis(100),
        Duration::from_millis(500),
    )
    .unwrap();
    checker.expect_runtime(&record).unwrap();
    let failure = checker.probe_failure(&record).await.unwrap().unwrap();
    assert_eq!(failure.http_status, Some(503));
    assert_eq!(failure.consecutive_failures, 3);
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    assert!(matches!(
        checker.wait_ready(&record).await,
        Err(adx_core::Error::ProbeFailed(_))
    ));
}
