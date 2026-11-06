//! Exercise the real supervisor's signal and CLI exit paths with an observable Drain RPC.
use adx_deployment::supervisor::{self, Request};
use adx_protocol::relay as pb;
use serde_json::{json, Value};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct NodeAdmin(Arc<AtomicUsize>, Arc<AtomicBool>);

#[tonic::async_trait]
impl pb::node_admin_service_server::NodeAdminService for NodeAdmin {
    async fn drain(
        &self,
        _: tonic::Request<pb::DrainRequest>,
    ) -> Result<tonic::Response<pb::DrainResponse>, tonic::Status> {
        self.0.fetch_add(1, Ordering::SeqCst);
        if self.1.load(Ordering::SeqCst) {
            return Ok(tonic::Response::new(pb::DrainResponse {
                deleted_environments: 0,
            }));
        }
        Err(tonic::Status::unavailable(
            "deletion publication unavailable",
        ))
    }
}

struct Fixture {
    root: tempfile::TempDir,
    config: PathBuf,
    state: PathBuf,
    child: Child,
    admin: Option<tokio::task::JoinHandle<()>>,
    drains: Arc<AtomicUsize>,
    allow_drain: Arc<AtomicBool>,
}

impl Fixture {
    async fn start(mode: &str, admin_available: bool) -> Self {
        let root = tempfile::Builder::new()
            .prefix("adx-exit-")
            .tempdir_in("/tmp")
            .unwrap();
        let package = root.path().join("package");
        fs::create_dir_all(package.join("bin")).unwrap();
        for name in ["adx-coordinator", "adxlet"] {
            let binary = package.join("bin").join(name);
            fs::write(
                &binary,
                "#!/bin/sh\ntrap 'kill \"$sleeper\" 2>/dev/null; wait \"$sleeper\" 2>/dev/null; printf drain > \"$0.stopped\"; exit 0' TERM\ntrap 'kill \"$sleeper\" 2>/dev/null; wait \"$sleeper\" 2>/dev/null; printf preserve > \"$0.stopped\"; exit 0' USR1\nsleep 100 &\nsleeper=$!\nprintf ready > \"$0.ready\"\nwait\n",
            )
            .unwrap();
            fs::set_permissions(binary, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let state = root.path().join("state");
        fs::create_dir(&state).unwrap();
        let drains = Arc::new(AtomicUsize::new(0));
        let allow_drain = Arc::new(AtomicBool::new(false));
        let admin = if admin_available {
            let listener = tokio::net::UnixListener::bind(state.join("node-admin.sock")).unwrap();
            let node = NodeAdmin(drains.clone(), allow_drain.clone());
            Some(tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(pb::node_admin_service_server::NodeAdminServiceServer::new(
                        node,
                    ))
                    .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(listener))
                    .await
                    .unwrap();
            }))
        } else {
            None
        };
        let config = root.path().join("deployment.yaml");
        fs::write(
            &config,
            serde_json::to_vec(&json!({
                "schema_version": 1, "package_dir": package, "state_dir": state,
                "redis_url": "redis://127.0.0.1:6379/", "namespace": "test",
                "restart_delay_ms": 1000, "stop_timeout_seconds": 1,
                "shutdown_mode": mode,
                "services": [
                    {"id": "coordinator", "role": "coordinator"},
                    {"id": "node", "role": "adxlet", "config": {"proxy_mode": "standalone"}}
                ]
            }))
            .unwrap(),
        )
        .unwrap();
        let log = fs::File::create(root.path().join("supervisor.log")).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_adxctl"))
            .args(["run", "--config"])
            .arg(&config)
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap();
        let mut fixture = Self {
            root,
            config,
            state,
            child,
            admin,
            drains,
            allow_drain,
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                assert!(
                    fixture.child.try_wait().unwrap().is_none(),
                    "supervisor failed: {}",
                    fixture.logs()
                );
                if fixture.status().await.is_some()
                    && ["adx-coordinator", "adxlet"]
                        .iter()
                        .all(|name| package.join("bin").join(format!("{name}.ready")).exists())
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("supervisor and fixture services must become ready");
        fixture
    }

    async fn status(&self) -> Option<Value> {
        supervisor::request(&self.state, Request::Status, Duration::from_secs(1))
            .await
            .ok()
    }

    fn logs(&self) -> String {
        fs::read_to_string(self.root.path().join("supervisor.log")).unwrap()
    }

    fn assert_exit_signals(&self, node_mode: &str) {
        let bin = self.root.path().join("package/bin");
        assert_eq!(
            fs::read_to_string(bin.join("adxlet.stopped")).unwrap(),
            node_mode
        );
        assert_eq!(
            fs::read_to_string(bin.join("adx-coordinator.stopped")).unwrap(),
            "drain"
        );
    }

    async fn cli(&self, arguments: &[&str]) -> std::process::Output {
        let config = self.config.clone();
        let arguments = arguments
            .iter()
            .map(|arg| (*arg).to_owned())
            .collect::<Vec<_>>();
        tokio::task::spawn_blocking(move || {
            Command::new(env!("CARGO_BIN_EXE_adxctl"))
                .arg("--config")
                .arg(config)
                .args(arguments)
                .output()
                .unwrap()
        })
        .await
        .unwrap()
    }

    async fn exited(&mut self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    assert!(status.success(), "{}", self.logs());
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("supervisor must stop within its process deadlines");
        assert!(!self.state.join("supervisor.sock").exists());
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = Command::new(env!("CARGO_BIN_EXE_adxctl"))
                .arg("--config")
                .arg(&self.config)
                .args(["stop", "--mode", "preserve"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
        if let Some(admin) = &self.admin {
            admin.abort();
        }
    }
}

#[tokio::test]
async fn configured_preserve_skips_drain_even_when_deletion_cannot_be_published() {
    let mut fixture = Fixture::start("preserve", true).await;
    assert_eq!(fixture.status().await.unwrap()["shutdown_mode"], "preserve");
    let output = fixture.cli(&["stop"]).await;
    assert!(output.status.success(), "{:?}", output);
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap()["shutdown_mode"],
        "preserve"
    );
    fixture.exited().await;
    fixture.assert_exit_signals("preserve");
    assert_eq!(fixture.drains.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn preserve_override_does_not_require_an_admin_socket() {
    let mut fixture = Fixture::start("drain", false).await;
    let output = fixture.cli(&["stop", "--mode", "preserve"]).await;
    assert!(output.status.success(), "{:?}", output);
    fixture.exited().await;
    fixture.assert_exit_signals("preserve");
    assert_eq!(fixture.drains.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn preserve_stops_remaining_services_after_adxlet_has_exited() {
    let mut fixture = Fixture::start("preserve", true).await;
    let status = fixture.status().await.unwrap();
    let node_pid = status["services"]
        .as_array()
        .unwrap()
        .iter()
        .find(|service| service["id"] == "node")
        .unwrap()["pid"]
        .as_u64()
        .unwrap();
    assert!(Command::new("kill")
        .arg("-TERM")
        .arg(node_pid.to_string())
        .status()
        .unwrap()
        .success());
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let status = fixture.status().await.unwrap();
            if status["services"]
                .as_array()
                .unwrap()
                .iter()
                .find(|service| service["id"] == "node")
                .unwrap()["pid"]
                .is_null()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(fixture.cli(&["stop"]).await.status.success());
    fixture.exited().await;
    assert_eq!(fixture.drains.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn explicit_drain_overrides_preserve_and_retains_dependencies_on_failure() {
    let mut fixture = Fixture::start("preserve", true).await;
    let before = fixture.status().await.unwrap();
    let output = fixture.cli(&["stop", "--mode", "drain"]).await;
    assert!(!output.status.success());
    let error = String::from_utf8(output.stderr).unwrap();
    assert!(error.contains("drain") && error.contains("node"), "{error}");
    assert_eq!(fixture.drains.load(Ordering::SeqCst), 1);
    let after = fixture.status().await.unwrap();
    assert_eq!(before["services"], after["services"]);
    assert_eq!(after["shutdown_mode"], "preserve");
    assert!(fixture.cli(&["stop"]).await.status.success());
    fixture.exited().await;
    fixture.assert_exit_signals("preserve");
    assert_eq!(fixture.drains.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn explicit_drain_uses_departure_signal_after_cleanup_succeeds() {
    let mut fixture = Fixture::start("preserve", true).await;
    fixture.allow_drain.store(true, Ordering::SeqCst);
    let output = fixture.cli(&["stop", "--mode", "drain"]).await;
    assert!(output.status.success(), "{output:?}");
    fixture.exited().await;
    fixture.assert_exit_signals("drain");
    assert_eq!(fixture.drains.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn plain_stop_uses_the_running_configuration_after_yaml_changes() {
    let mut fixture = Fixture::start("preserve", true).await;
    let mut config: Value = serde_json::from_slice(&fs::read(&fixture.config).unwrap()).unwrap();
    config["shutdown_mode"] = json!("drain");
    fs::write(&fixture.config, serde_json::to_vec(&config).unwrap()).unwrap();
    assert!(fixture.cli(&["stop"]).await.status.success());
    fixture.exited().await;
    assert_eq!(fixture.drains.load(Ordering::SeqCst), 0);
}

async fn signal_preserve(signal: &str) {
    let mut fixture = Fixture::start("preserve", true).await;
    assert!(Command::new("kill")
        .arg(signal)
        .arg(fixture.child.id().to_string())
        .status()
        .unwrap()
        .success());
    fixture.exited().await;
    fixture.assert_exit_signals("preserve");
    assert_eq!(fixture.drains.load(Ordering::SeqCst), 0);
    assert!(fixture.logs().contains("mode=preserve"));
}

#[tokio::test]
async fn sigterm_uses_configured_preserve() {
    signal_preserve("-TERM").await;
}

#[tokio::test]
async fn sigint_uses_configured_preserve() {
    signal_preserve("-INT").await;
}

#[tokio::test]
async fn signals_use_configured_drain_and_keep_dependencies_on_failure() {
    for signal in ["-TERM", "-INT"] {
        let mut fixture = Fixture::start("drain", true).await;
        assert!(Command::new("kill")
            .arg(signal)
            .arg(fixture.child.id().to_string())
            .status()
            .unwrap()
            .success());
        tokio::time::timeout(Duration::from_secs(3), async {
            while fixture.drains.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(fixture.status().await.unwrap()["services"]
            .as_array()
            .unwrap()
            .iter()
            .all(|service| service["pid"].is_number()));
        assert!(fixture
            .cli(&["stop", "--mode", "preserve"])
            .await
            .status
            .success());
        fixture.exited().await;
        assert_eq!(fixture.drains.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn unknown_wire_mode_is_rejected_without_stopping_or_draining() {
    let mut fixture = Fixture::start("drain", true).await;
    let mut stream = tokio::net::UnixStream::connect(fixture.state.join("supervisor.sock"))
        .await
        .unwrap();
    stream.write_all(b"stop presreve\n").await.unwrap();
    let mut data = Vec::new();
    stream.read_to_end(&mut data).await.unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&data).unwrap()["ok"], false);
    assert!(fixture.status().await.is_some());
    assert_eq!(fixture.drains.load(Ordering::SeqCst), 0);
    assert!(fixture
        .cli(&["stop", "--mode", "preserve"])
        .await
        .status
        .success());
    fixture.exited().await;
}
