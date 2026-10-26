//! A long-lived inherited process must not stall Execd's control event loop.
use std::{
    os::unix::process::CommandExt,
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct ProcessGroup(Child);
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        // SAFETY: this child was spawned as the leader of a dedicated process
        // group. Kill only that test-owned group, including its inherited child.
        unsafe {
            libc::kill(-(self.0.id() as i32), libc::SIGKILL);
        }
        let _ = self.0.wait();
    }
}

async fn entrypoint_poll(port: u16, token: &str) -> Option<serde_json::Value> {
    let body = r#"{"action":"entrypoint.poll","args":{"wait_timeout":0}}"#;
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .ok()?;
    stream
        .write_all(
            format!(
                "POST /invoke HTTP/1.1\r\nHost: localhost\r\nX-Auth: {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        )
        .await
        .ok()?;
    let mut response = Vec::new();
    loop {
        let mut buffer = [0; 4096];
        let count = stream.read(&mut buffer).await.ok()?;
        if count == 0 || response.len() + count > 64 * 1024 {
            return None;
        }
        response.extend_from_slice(&buffer[..count]);
        let Some(header_end) = response.windows(4).position(|value| value == b"\r\n\r\n") else {
            continue;
        };
        let header = std::str::from_utf8(&response[..header_end]).ok()?;
        let length = header.lines().find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .and_then(|value| value.trim().parse::<usize>().ok())
        })?;
        let body_start = header_end + 4;
        if response.len() >= body_start + length {
            return serde_json::from_slice(&response[body_start..body_start + length]).ok();
        }
    }
}

fn process_is_running(pid: i32) -> bool {
    let output = Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "stat="])
        .output()
        .expect("inspect descendant process");
    let state = String::from_utf8_lossy(&output.stdout);
    output.status.success() && !state.trim().is_empty() && !state.trim_start().starts_with('Z')
}

#[tokio::test]
async fn inherited_long_running_process_keeps_control_responsive() {
    for attempt in 0..4 {
        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("started");
        let config = directory.path().join("process.json");
        std::fs::write(&config, serde_json::json!({
            "version":1,"args":["/bin/sh","-c","printf started > \"$1\"; exec sleep 60","entrypoint",marker],
            "cwd":"/","user":""
        }).to_string()).unwrap();
        let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = reserved.local_addr().unwrap().port();
        drop(reserved);
        let log = std::fs::File::create(directory.path().join("execd.log")).unwrap();
        let mut child = ProcessGroup(
            Command::new(env!("CARGO_BIN_EXE_adx-execd"))
                .env_clear()
                .env("PATH", "/usr/bin:/bin")
                .env("ADX_IMAGE_PROCESS_CONFIG", config)
                .env("ADX_ENVIRONMENT_ID", "entrypoint-test")
                .env("ADX_RUNTIME_ID", "entrypoint-test-1")
                .env("ADX_OWNERSHIP_GENERATION", "1")
                .env("EXECD_HTTP_PORT", port.to_string())
                .env("EXECD_HTTP_TOKEN", "entrypoint-regression-token")
                .process_group(0)
                .stdin(Stdio::null())
                .stdout(Stdio::from(log.try_clone().unwrap()))
                .stderr(Stdio::from(log))
                .spawn()
                .unwrap(),
        );
        let ready = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                assert!(child.0.try_wait().unwrap().is_none(), "Execd exited during startup");
                let probe = async {
                    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1",port)).await.ok()?;
                    stream.write_all(b"GET /control/v1/status HTTP/1.1\r\nHost: localhost\r\nX-Auth: entrypoint-regression-token\r\nConnection: close\r\n\r\n").await.ok()?;
                    let mut response = Vec::new();
                    loop {
                        let mut buffer = [0; 4096];
                        let count = stream.read(&mut buffer).await.ok()?;
                        if count == 0 || response.len() + count > 64 * 1024 { return None; }
                        response.extend_from_slice(&buffer[..count]);
                        if let Some(end) = response.windows(4).position(|v| v == b"\r\n\r\n") {
                            let header = std::str::from_utf8(&response[..end]).ok()?;
                            let length = header.lines().find_map(|line| {
                                line.to_ascii_lowercase().strip_prefix("content-length:")
                                    .and_then(|length| length.trim().parse::<usize>().ok())
                            })?;
                            if response.len() >= end + 4 + length {
                                return response.starts_with(b"HTTP/1.1 200 ").then_some(());
                            }
                        }
                    }
                };
                if matches!(tokio::time::timeout(Duration::from_millis(200),probe).await, Ok(Some(())))
                    && marker.exists()
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await;
        assert!(
            ready.is_ok(),
            "attempt {attempt}: inherited process started={}, Execd control blocked; log: {}",
            marker.exists(),
            std::fs::read_to_string(directory.path().join("execd.log")).unwrap()
        );
    }
}

#[tokio::test]
async fn explicit_workload_commands_override_the_image_defaults() {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o777)).unwrap();
    let marker = directory.path().join("first");
    let second = directory.path().join("second");
    let config = directory.path().join("process.json");
    std::fs::write(
        &config,
        serde_json::json!({
            "version":1,"args":["/bin/false"],"cwd":"/","user":""
        })
        .to_string(),
    )
    .unwrap();
    let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reserved.local_addr().unwrap().port();
    drop(reserved);
    let override_spec = serde_json::json!({
        "commands":[
            ["/bin/sh","-c","printf first > \"$1\"; exec sleep 60","first",marker],
            ["/bin/sh","-c","printf second > \"$1\"; exec sleep 60","second",second]
        ]
    });
    let log_path = directory.path().join("execd.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let mut child = ProcessGroup(
        Command::new(env!("CARGO_BIN_EXE_adx-execd"))
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("ADX_IMAGE_PROCESS_CONFIG", config)
            .env("ADX_WORKLOAD_PROCESS", override_spec.to_string())
            .env("ADX_ENVIRONMENT_ID", "override-test")
            .env("ADX_RUNTIME_ID", "override-test-1")
            .env("ADX_OWNERSHIP_GENERATION", "1")
            .env("EXECD_HTTP_PORT", port.to_string())
            .env("EXECD_HTTP_TOKEN", "override-regression-token")
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap(),
    );
    let ready = tokio::time::timeout(Duration::from_secs(5), async {
        while std::fs::read_to_string(&marker).unwrap_or_default() != "first"
            || std::fs::read_to_string(&second).unwrap_or_default() != "second"
        {
            if child.0.try_wait().unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        ready.is_ok() && marker.exists() && second.exists(),
        "{}",
        std::fs::read_to_string(log_path).unwrap()
    );
    assert_eq!(std::fs::read_to_string(marker).unwrap(), "first");
    assert_eq!(std::fs::read_to_string(second).unwrap(), "second");
}

#[tokio::test]
async fn failed_workload_command_kills_sibling_process_tree() {
    let directory = tempfile::tempdir().unwrap();
    let descendant_pid = directory.path().join("descendant.pid");
    let config = directory.path().join("process.json");
    std::fs::write(
        &config,
        serde_json::json!({
            "version":1,"args":["/bin/false"],"cwd":"/","user":""
        })
        .to_string(),
    )
    .unwrap();
    let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reserved.local_addr().unwrap().port();
    drop(reserved);
    let override_spec = serde_json::json!({
        "commands":[
            ["/bin/sh","-c","sleep 60 & echo $! > \"$1\"; wait","sibling",descendant_pid],
            ["/bin/sh","-c","sleep 0.3; exit 42"]
        ]
    });
    let log_path = directory.path().join("execd.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let mut child = ProcessGroup(
        Command::new(env!("CARGO_BIN_EXE_adx-execd"))
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("ADX_IMAGE_PROCESS_CONFIG", config)
            .env("ADX_WORKLOAD_PROCESS", override_spec.to_string())
            .env("ADX_ENVIRONMENT_ID", "process-tree-test")
            .env("ADX_RUNTIME_ID", "process-tree-test-1")
            .env("ADX_OWNERSHIP_GENERATION", "1")
            .env("EXECD_HTTP_PORT", port.to_string())
            .env("EXECD_HTTP_TOKEN", "process-tree-regression-token")
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap(),
    );

    let exited = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "Execd exited during process-tree cleanup"
            );
            if let Some(result) = entrypoint_poll(port, "process-tree-regression-token").await {
                if result.get("status").and_then(serde_json::Value::as_str) == Some("exited") {
                    return result;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "workload did not fail; log: {}",
            std::fs::read_to_string(&log_path).unwrap()
        )
    });
    assert_eq!(
        exited.get("exit_code").and_then(serde_json::Value::as_i64),
        Some(42)
    );
    let pid = std::fs::read_to_string(&descendant_pid)
        .unwrap()
        .trim()
        .parse::<i32>()
        .unwrap();
    let cleaned = tokio::time::timeout(Duration::from_secs(3), async {
        while process_is_running(pid) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        cleaned.is_ok(),
        "descendant {pid} survived failed workload cleanup; log: {}",
        std::fs::read_to_string(log_path).unwrap()
    );
}
