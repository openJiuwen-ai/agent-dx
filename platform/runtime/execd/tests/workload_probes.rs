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
    port: u16,
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
    start_with_local_workload(config, false)
}

fn start_with_local_workload(config: ProbeConfig, local: bool) -> Process {
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
        .env(CONFIG_ENV, serde_json::to_string(&config).unwrap())
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log))
        .process_group(0);
    if local {
        command.env_remove("ADX_IMAGE_PROCESS_CONFIG").env(
            "ADX_WORKLOAD_PROCESS",
            serde_json::json!({"commands":[["/bin/sh","-c","sleep 60"]]}).to_string(),
        );
    } else {
        command
            .env_remove("ADX_WORKLOAD_PROCESS")
            .env("ADX_IMAGE_PROCESS_CONFIG", workload);
    }
    let child = command.spawn().unwrap();
    let record = EnvironmentRecord {
        observation: None,
        execution_artifacts: vec![],
        snapshot_source: None,
        failure: None,
        spec: EnvironmentSpec {
            envd: None,
            run_policy: None,
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
        port,
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
async fn local_workload_readiness_failure_reaches_real_adxlet_http_waiter() {
    let process = start_with_local_workload(
        ProbeConfig {
            readiness: Some(probe(ProbeAction::Exec {
                command: vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    "echo local-not-ready >&2; exit 7".into(),
                ],
            })),
            healthcheck: None,
            init_timeout_seconds: 1,
        },
        true,
    );
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
        "{error:?}"
    );
    let failure = status(&process).await.failure.unwrap();
    assert_eq!(failure.kind, ProbeFailureKind::ReadinessTimeout);
    assert_eq!(failure.exit_code, Some(7));
    assert!(failure.message.contains("local-not-ready"));
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

async fn probe_progress(process: &Process) -> serde_json::Value {
    let body = r#"{"action":"runtime.probes","args":{}}"#;
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", process.port))
        .await
        .unwrap();
    stream.write_all(format!("POST /invoke HTTP/1.1\r\nHost: localhost\r\nX-Auth: probe-test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
    let mut response = Vec::new();
    loop {
        let mut chunk = [0; 4096];
        let count = stream.read(&mut chunk).await.unwrap();
        assert!(count > 0, "incomplete probe response");
        response.extend_from_slice(&chunk[..count]);
        assert!(response.len() < 65536);
        if let Some(end) = response.windows(4).position(|part| part == b"\r\n\r\n") {
            let headers = std::str::from_utf8(&response[..end]).unwrap();
            let length: usize = headers
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .unwrap()
                .1
                .trim()
                .parse()
                .unwrap();
            if response.len() >= end + 4 + length {
                break;
            }
        }
    }
    let response = String::from_utf8(response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    serde_json::from_str(response.split_once("\r\n\r\n").unwrap().1).unwrap()
}

#[tokio::test]
async fn native_probe_progress_exposes_failure_then_recovery_without_runtime_replacement() {
    use std::sync::atomic::AtomicBool;
    let healthy = Arc::new(AtomicBool::new(false));
    let flag = healthy.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 1024];
            assert!(stream.read(&mut request).await.unwrap() > 0);
            let code = if flag.load(Ordering::SeqCst) {
                200
            } else {
                503
            };
            stream
                .write_all(format!("HTTP/1.1 {code} Test\r\nContent-Length: 0\r\n\r\n").as_bytes())
                .await
                .unwrap();
        }
    });
    let mut healthcheck = probe(ProbeAction::HttpGet {
        port,
        path: "/".into(),
    });
    healthcheck.failure_threshold = 10;
    let process = start(ProbeConfig {
        readiness: None,
        healthcheck: Some(healthcheck),
        init_timeout_seconds: 4,
    });
    status(&process).await;
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let progress = probe_progress(&process).await;
            assert_eq!(progress["runtime_id"], "probe-1");
            assert_eq!(progress["ownership_generation"], 1);
            if progress["healthcheck_consecutive_failures"]
                .as_u64()
                .is_some_and(|n| n > 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(status(&process).await.phase, RuntimePhase::Running);
    healthy.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            if probe_progress(&process).await["healthcheck_consecutive_failures"] == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let recovered = status(&process).await;
    assert_eq!(recovered.phase, RuntimePhase::Running);
    assert!(recovered.failure.is_none());
    assert_eq!(recovered.identity.runtime_id, "probe-1");
    server.abort();
}
