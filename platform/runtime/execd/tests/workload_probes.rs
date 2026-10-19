//! Real Execd process + Adxlet HTTP readiness, without sandboxd or a kernel checkpoint.
use adx_core::{
    probes::*, runtime::*, Assignment, EnvironmentRecord, EnvironmentSpec, EnvironmentState,
    Resources, Result,
};
use adxlet::{
    readiness::ExecdReadiness, runtime_control::RuntimeControlClient, Readiness, RuntimeDriver,
};
use async_trait::async_trait;
use std::{
    net::IpAddr,
    os::unix::process::CommandExt,
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

struct Backend;
#[async_trait]
impl RuntimeDriver for Backend {
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
        Ok(true)
    }
}
struct Process {
    child: Child,
    _temp: tempfile::TempDir,
    record: EnvironmentRecord,
    client: RuntimeControlClient,
}
impl Drop for Process {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!(
                "Execd fixture log: {}",
                std::fs::read_to_string(self._temp.path().join("execd.log")).unwrap_or_default()
            );
        }
        let id = i32::try_from(self.child.id()).unwrap();
        // SAFETY: the child was launched in a private process group; kill its managed
        // workload descendants as well so process fixtures cannot outlive the test.
        unsafe {
            libc::kill(-id, libc::SIGKILL);
        }
        let _ = self.child.wait();
    }
}
fn start(config: ProbeConfig) -> Process {
    let temp = tempfile::tempdir().unwrap();
    let workload = temp.path().join("process.json");
    std::fs::write(
        &workload,
        r#"{"version":1,"args":["/bin/sh","-c","sleep 60"],"cwd":"/","user":""}"#,
    )
    .unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let log = std::fs::File::create(temp.path().join("execd.log")).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_adx-execd"));
    command
        .env_remove("ADX_SEED_FILE")
        .env_remove("EXECD_TUNNEL_WS_PORT")
        .env_remove("EXECD_TUNNEL_ONLY")
        .env_remove("ADX_RUNTIME_CONTROL_ADDRESS")
        .env_remove("ADX_EXECD_CONTROL_SOCKET_PATH")
        .env("EXECD_HTTP_ONLY", "1")
        .env("EXECD_HTTP_PORT", port.to_string())
        .env("EXECD_HTTP_TOKEN", "probe-test")
        .env("ADX_ENVIRONMENT_ID", "probe")
        .env("ADX_RUNTIME_ID", "probe-1")
        .env("ADX_OWNERSHIP_GENERATION", "1")
        .env("ADX_IMAGE_PROCESS_CONFIG", workload)
        .env(CONFIG_ENV, serde_json::to_string(&config).unwrap())
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log))
        .process_group(0);
    let child = command.spawn().unwrap();
    let record = EnvironmentRecord {
        failure: None,
        spec: EnvironmentSpec {
            compatibility: None,
            runtime_profile: None,
            snapshot_id: None,
            lifecycle: Default::default(),
            env: Default::default(),
            scheduling: Default::default(),
            id: "probe".into(),
            tenant_id: "t".into(),
            image: "image".into(),
            runtime_class: "runsc".into(),
            resources: Resources::default(),
            priority: 0,
            execution: Default::default(),
        },
        assignment: Assignment {
            environment_id: "probe".into(),
            node_id: "n".into(),
            shard_id: 0,
            generation: 1,
            devices: vec![],
        },
        state: EnvironmentState::Starting,
        revision: 1,
        runtime: adx_core::Runtime {
            id: "probe-1".into(),
            ip: Some("127.0.0.1".parse().unwrap()),
        },
        resources_held: true,
        checkpoint: None,
        last_operation: None,
        restart_attempts: 0,
        restart_pending: false,
    };
    let client = RuntimeControlClient::new(port, Duration::from_secs(1))
        .unwrap()
        .with_token("probe-test")
        .unwrap();
    Process {
        child,
        _temp: temp,
        record,
        client,
    }
}
fn probe(action: ProbeAction) -> ProbeSpec {
    ProbeSpec {
        action,
        initial_delay_seconds: 0,
        period_seconds: 1,
        timeout_seconds: 1,
        failure_threshold: 1,
    }
}
async fn status(process: &Process) -> RuntimeStatus {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(status) = process.client.status(&process.record).await {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}
async fn wait_failed(process: &Process) -> RuntimeStatus {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let status = status(process).await;
            if status.failure.is_some() {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn readiness_failure_reaches_real_adxlet_http_waiter() {
    let process = start(ProbeConfig {
        readiness: Some(probe(ProbeAction::Exec {
            command: vec![
                "/bin/sh".into(),
                "-c".into(),
                "echo not-ready >&2; exit 7".into(),
            ],
        })),
        healthcheck: None,
        init_timeout_seconds: 1,
    });
    let checker = ExecdReadiness::with_client(
        Arc::new(Backend),
        process.client.clone(),
        Duration::from_millis(10),
        Duration::from_secs(1),
        Duration::from_secs(4),
    )
    .unwrap();
    let error = checker.wait_ready(&process.record).await.unwrap_err();
    assert!(
        matches!(error, adx_core::Error::ProbeFailed(_)),
        "unexpected readiness error: {error:?}"
    );
    let failed = status(&process).await;
    assert_eq!(
        failed.failure.as_ref().unwrap().kind,
        ProbeFailureKind::ReadinessTimeout
    );
    assert_eq!(failed.failure.as_ref().unwrap().exit_code, Some(7));
    assert!(failed.failure.unwrap().message.contains("not-ready"));
}
#[tokio::test]
async fn healthcheck_is_armed_after_readiness_resets_failures_and_preserves_idle_activity() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let requests = Arc::new(AtomicUsize::new(0));
    let count = requests.clone();
    let server = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 1024];
            assert!(stream.read(&mut request).await.unwrap() > 0);
            let index = count.fetch_add(1, Ordering::SeqCst);
            let status = if index == 0 || index == 2 { 200 } else { 503 };
            stream
                .write_all(
                    format!("HTTP/1.1 {status} Test\r\nContent-Length: 0\r\n\r\n").as_bytes(),
                )
                .await
                .unwrap();
        }
    });
    let readiness = probe(ProbeAction::HttpGet {
        port,
        path: "/ready".into(),
    });
    let mut healthcheck = readiness.clone();
    healthcheck.failure_threshold = 2;
    let process = start(ProbeConfig {
        readiness: Some(readiness),
        healthcheck: Some(healthcheck),
        init_timeout_seconds: 4,
    });
    let failed = wait_failed(&process).await;
    assert_eq!(requests.load(Ordering::SeqCst), 5);
    assert_eq!(failed.failure.unwrap().consecutive_failures, 2);
    assert_eq!(failed.activity_revision, 1);
    assert_eq!(failed.active_commands, 0);
    server.abort();
}
