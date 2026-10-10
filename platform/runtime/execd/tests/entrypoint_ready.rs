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
