//! Real Node Manager HTTP client + RRT process + backend handoff fixture.
//! This does not invoke sandboxd or create a real kernel checkpoint.
use adx_core::{runtime::*, Assignment, InstanceRecord, InstanceSpec, InstanceState, Resources};
use adx_node_manager::runtime_control::RuntimeControlClient;
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
                std::fs::read_to_string(self.temp.path().join("rrt.log")).unwrap_or_default()
            );
        }
    }
}
fn record(generation: u64) -> InstanceRecord {
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
            image: "rrt".into(),
            runtime: "runsc".into(),
            priority: 0,
            resources: Resources::default(),
        },
        assignment: Assignment {
            devices: vec![],
            instance_id: "i".into(),
            node_id: "n".into(),
            shard_id: 0,
            generation,
        },
        state: InstanceState::Running,
        revision: 1,
        runtime_id: format!("i-{generation}"),
        resources_held: true,
        runtime_ip: Some("127.0.0.1".parse().unwrap()),
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
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        let writer = Some(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&fifo)
                .unwrap(),
        );
        let log = std::fs::File::create(temp.path().join("rrt.log")).unwrap();
        std::fs::write(
            temp.path().join("env"),
            "ADX_INSTANCE_ID=i\nADX_RUNTIME_ID=i-1\nADX_OWNERSHIP_GENERATION=1\n",
        )
        .unwrap();
        drop((listener, ws_listener, tunnel_listener));
        let child = Command::new(env!("CARGO_BIN_EXE_rrt-runtime"))
            .env_remove("ADX_SEED_FILE")
            .env_remove("RRT_TUNNEL_ONLY")
            .env_remove("RRT_TUNNEL_WS_PORT")
            .env_remove("ADX_IMAGE_PROCESS_CONFIG")
            .env_remove("RRT_HTTP_ONLY")
            .env("RRT_TUNNEL_WS_PORT", ws_port.to_string())
            .env("RRT_TUNNEL_HTTP_PORT", tunnel_port.to_string())
            .env("RRT_HTTP_PORT", port.to_string())
            .env("RRT_HTTP_TOKEN", "before")
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
        "ADX_INSTANCE_ID=i\nADX_RUNTIME_ID=i-2\nADX_OWNERSHIP_GENERATION=2\nRRT_HTTP_TOKEN=after\n",
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
        "ADX_INSTANCE_ID=i\nADX_RUNTIME_ID=i-stale\nADX_OWNERSHIP_GENERATION=1\n",
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
    runtime.handoff("ADX_INSTANCE_ID=i\nADX_RUNTIME_ID=i-1-r5\nADX_OWNERSHIP_GENERATION=1\nRRT_HTTP_TOKEN=after\n", "restore");
    let mut target = record(1);
    target.runtime_id = "i-1-r5".into();
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
