//! Real Adxlet HTTP client + EXECD process + backend handoff fixture.
//! This does not invoke sandboxd or create a real kernel checkpoint.
use adx_core::{
    runtime::*, Assignment, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Resources,
};
use adxlet::runtime_control::RuntimeControlClient;
use futures_util::StreamExt;
use std::{
    ffi::CString,
    fs::OpenOptions,
    io::Write,
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Runtime {
    child: Child,
    temp: tempfile::TempDir,
    port: u16,
    ws_port: u16,
    writer: Option<std::fs::File>,
}
impl Drop for Runtime {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if std::thread::panicking() {
            eprintln!(
                "{}",
                std::fs::read_to_string(self.temp.path().join("execd.log")).unwrap_or_default()
            );
        }
    }
}
fn record(generation: u64) -> EnvironmentRecord {
    EnvironmentRecord {
        restart_attempts: 0,
        restart_pending: false,
        spec: EnvironmentSpec {
            runtime_profile: None,
            snapshot_id: None,
            lifecycle: Default::default(),
            env: Default::default(),
            scheduling: Default::default(),
            id: "i".into(),
            tenant_id: "t".into(),
            image: "execd".into(),
            runtime_class: "runsc".into(),
            priority: 0,
            resources: Resources::default(),
            sandbox: Default::default(),
        },
        assignment: Assignment {
            devices: vec![],
            environment_id: "i".into(),
            node_id: "n".into(),
            shard_id: 0,
            generation,
        },
        state: EnvironmentState::Running,
        revision: 1,
        runtime: adx_core::Runtime {
            id: format!("i-{generation}"),
            ip: Some("127.0.0.1".parse().unwrap()),
        },
        resources_held: true,
        checkpoint: None,
        last_operation: None,
    }
}
impl Runtime {
    async fn start() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let ws_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let ws_port = ws_listener.local_addr().unwrap().port();
        let tunnel_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let tunnel_port = tunnel_listener.local_addr().unwrap().port();
        let fifo = temp.path().join("handoff");
        let name = CString::new(fifo.to_str().unwrap()).unwrap();
        // SAFETY: name is NUL terminated and remains valid for the call; mkfifo does not retain
        // the pointer.
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        let writer = Some(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&fifo)
                .unwrap(),
        );
        let log = std::fs::File::create(temp.path().join("execd.log")).unwrap();
        std::fs::write(
            temp.path().join("env"),
            "ADX_ENVIRONMENT_ID=i\nADX_RUNTIME_ID=i-1\nADX_OWNERSHIP_GENERATION=1\n",
        )
        .unwrap();
        drop((listener, ws_listener, tunnel_listener));
        let child = Command::new(env!("CARGO_BIN_EXE_adx-execd"))
            .env_remove("ADX_SEED_FILE")
            .env_remove("EXECD_TUNNEL_ONLY")
            .env_remove("EXECD_TUNNEL_WS_PORT")
            .env_remove("ADX_IMAGE_PROCESS_CONFIG")
            .env_remove("EXECD_HTTP_ONLY")
            .env("EXECD_TUNNEL_WS_PORT", ws_port.to_string())
            .env("EXECD_TUNNEL_HTTP_PORT", tunnel_port.to_string())
            .env("EXECD_HTTP_PORT", port.to_string())
            .env("EXECD_HTTP_TOKEN", "before")
            .env("ADX_EXECD_CONTROL_SOCKET_PATH", temp.path())
            .env("ADX_ENV_FILE", temp.path().join("env"))
            .env("ADX_CHECKPOINT_HANDOFF_FILE", &fifo)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        let runtime = Self {
            child,
            temp,
            port,
            ws_port,
            writer,
        };
        let client = runtime.client("before");
        tokio::time::timeout(Duration::from_secs(10), async {
            while client.status(&record(1)).await.is_err() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        runtime
    }
    fn client(&self, token: &str) -> RuntimeControlClient {
        RuntimeControlClient::new(self.port, Duration::from_secs(2))
            .unwrap()
            .with_token(token)
            .unwrap()
    }
    fn handoff(&mut self, environment: &str, outcome: &str) {
        std::fs::write(self.temp.path().join("env"), environment).unwrap();
        let mut writer = self.writer.take().unwrap();
        writer.write_all(outcome.as_bytes()).unwrap();
        drop(writer);
    }
    async fn raw(&self, path: &str, token: &str, body: &str) -> String {
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", self.port))
            .await
            .unwrap();
        let request = format!("POST {path} HTTP/1.1\r\nHost: localhost\r\nX-Auth: {token}\r\nContent-Length: {}\r\n\r\n{body}", body.len());
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = vec![];
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let mut buf = [0; 4096];
                let count = stream.read(&mut buf).await.unwrap();
                if count == 0 {
                    break;
                }
                response.extend_from_slice(&buf[..count]);
                if let Some(end) = response.windows(4).position(|v| v == b"\r\n\r\n") {
                    let header = String::from_utf8_lossy(&response[..end]);
                    let length = header
                        .lines()
                        .find_map(|l| {
                            l.to_lowercase()
                                .strip_prefix("content-length:")
                                .and_then(|s| s.trim().parse::<usize>().ok())
                        })
                        .unwrap();
                    if response.len() >= end + 4 + length {
                        break;
                    }
                }
            }
        })
        .await
        .unwrap();
        String::from_utf8(response).unwrap()
    }
}

#[tokio::test]
async fn real_http_client_prepares_retries_aborts_and_refreshes_restored_identity() {
    let mut runtime = Runtime::start().await;
    let client = runtime.client("before");
    assert!(runtime.client("wrong").status(&record(1)).await.is_err());
    assert!(client.status(&record(2)).await.is_err());
    let initial = client.status(&record(1)).await.unwrap();
    assert_eq!((initial.active_requests, initial.active_commands), (0, 0));
    let (mut old_tunnel, _) =
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{}", runtime.ws_port))
            .await
            .unwrap();
    let prepared = client
        .prepare(&record(1), "a", initial.revision)
        .await
        .unwrap();
    assert_eq!(
        client
            .prepare(&record(1), "a", initial.revision)
            .await
            .unwrap(),
        prepared
    );
    assert!(runtime
        .raw("/invoke", "before", r#"{"action":"ping"}"#)
        .await
        .starts_with("HTTP/1.1 503"));
    let aborted = client
        .abort_unstarted(&record(1), "a", prepared.revision)
        .await
        .unwrap();
    client
        .prepare(&record(1), "b", aborted.revision)
        .await
        .unwrap();
    assert!(client
        .prepare(&record(1), "a", initial.revision)
        .await
        .is_err());
    runtime.handoff(
        "ADX_ENVIRONMENT_ID=i\nADX_RUNTIME_ID=i-2\nADX_OWNERSHIP_GENERATION=2\nEXECD_HTTP_TOKEN=after\n",
        "restore",
    );
    let restored_client = runtime.client("after");
    let restored = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(status) = restored_client.status(&record(2)).await {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(restored.phase, RuntimePhase::Running);
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(Ok(message)) = old_tunnel.next().await {
            if message.is_close() {
                break;
            }
        }
    })
    .await
    .expect("inherited tunnel connection must retire");
    let (_new_tunnel, _) =
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{}", runtime.ws_port))
            .await
            .unwrap();
    assert_eq!(
        restored.checkpoint.unwrap().phase,
        CheckpointPhase::Restored
    );
    assert!(client.status(&record(1)).await.is_err());
    assert!(client.status(&record(2)).await.is_err());
    assert!(runtime
        .raw("/invoke", "after", r#"{"action":"ping"}"#)
        .await
        .starts_with("HTTP/1.1 200"));
}

#[tokio::test]
async fn invalid_restore_identity_fails_closed_and_control_rejects_bad_json() {
    let mut runtime = Runtime::start().await;
    assert!(runtime
        .raw("/control/v1/checkpoint/prepare", "before", "{}")
        .await
        .starts_with("HTTP/1.1 400"));
    let client = runtime.client("before");
    client.prepare(&record(1), "a", 1).await.unwrap();
    runtime.handoff(
        "ADX_ENVIRONMENT_ID=i\nADX_RUNTIME_ID=i-stale\nADX_OWNERSHIP_GENERATION=1\n",
        "restore",
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while client.status(&record(1)).await.unwrap().phase != RuntimePhase::Failed {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(runtime
        .raw("/invoke", "before", r#"{"action":"ping"}"#)
        .await
        .starts_with("HTTP/1.1 503"));
}

#[tokio::test]
async fn same_owner_restore_accepts_new_execution_and_rejects_source_control_identity() {
    let mut runtime = Runtime::start().await;
    let client = runtime.client("before");
    client.prepare(&record(1), "same-node", 1).await.unwrap();
    runtime.handoff("ADX_ENVIRONMENT_ID=i\nADX_RUNTIME_ID=i-1-r5\nADX_OWNERSHIP_GENERATION=1\nEXECD_HTTP_TOKEN=after\n", "restore");
    let mut target = record(1);
    target.runtime.id = "i-1-r5".into();
    let restored = runtime.client("after");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(status) = restored.status(&target).await {
                assert_eq!(status.phase, RuntimePhase::Running);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(restored.status(&record(1)).await.is_err());
}

async fn local_checkpoint(path: std::path::PathBuf) -> String {
    let mut stream = tokio::net::UnixStream::connect(path).await.unwrap();
    stream
        .write_all(b"POST /checkpoint HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n")
        .await
        .unwrap();
    let mut response = String::new();
    tokio::time::timeout(
        Duration::from_secs(10),
        stream.read_to_string(&mut response),
    )
    .await
    .unwrap()
    .unwrap();
    response
}
#[tokio::test]
async fn unix_checkpoint_requires_handoff_and_node_ack_and_rejects_concurrency() {
    let mut runtime = Runtime::start().await;
    let client = runtime.client("before");
    let request = tokio::spawn(local_checkpoint(runtime.temp.path().join("execd.sock")));
    let id = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(id) = client
                .status(&record(1))
                .await
                .unwrap()
                .requested_checkpoint
            {
                break id;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(local_checkpoint(runtime.temp.path().join("execd.sock"))
        .await
        .starts_with("HTTP/1.1 409"));
    let status = client.status(&record(1)).await.unwrap();
    client
        .prepare(&record(1), &id, status.revision)
        .await
        .unwrap();
    runtime.handoff(
        "ADX_ENVIRONMENT_ID=i\nADX_RUNTIME_ID=i-1\nADX_OWNERSHIP_GENERATION=1\n",
        "resume",
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while client.status(&record(1)).await.unwrap().phase != RuntimePhase::Running {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(!request.is_finished());
    client
        .finish_checkpoint(&record(1), &id, None)
        .await
        .unwrap();
    let response = request.await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains(r#"{"status":"completed"}"#));
    // ACK replay is harmless; a later request must not be completed by this ACK.
    client
        .finish_checkpoint(&record(1), &id, None)
        .await
        .unwrap();
    let next = tokio::spawn(local_checkpoint(runtime.temp.path().join("execd.sock")));
    let next_id = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(id) = client
                .status(&record(1))
                .await
                .unwrap()
                .requested_checkpoint
            {
                break id;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(client
        .finish_checkpoint(&record(1), &id, None)
        .await
        .is_err());
    client
        .finish_checkpoint(&record(1), &next_id, Some("backend unavailable".into()))
        .await
        .unwrap();
    assert!(next.await.unwrap().starts_with("HTTP/1.1 503"));
}

#[tokio::test]
async fn unix_checkpoint_listener_rearms_after_restore() {
    let mut runtime = Runtime::start().await;
    runtime
        .client("before")
        .prepare(&record(1), "external", 1)
        .await
        .unwrap();
    runtime.handoff(
        "ADX_ENVIRONMENT_ID=i\nADX_RUNTIME_ID=i-2\nADX_OWNERSHIP_GENERATION=2\nEXECD_HTTP_TOKEN=after\n",
        "restore",
    );
    let client = runtime.client("after");
    tokio::time::timeout(Duration::from_secs(2), async {
        while client.status(&record(2)).await.is_err() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let request = tokio::spawn(local_checkpoint(runtime.temp.path().join("execd.sock")));
    let id = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(id) = client
                .status(&record(2))
                .await
                .unwrap()
                .requested_checkpoint
            {
                break id;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    client
        .finish_checkpoint(&record(2), &id, Some("test rejection".into()))
        .await
        .unwrap();
    assert!(request.await.unwrap().starts_with("HTTP/1.1 503"));
}
