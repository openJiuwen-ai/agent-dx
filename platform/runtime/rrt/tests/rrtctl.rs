use std::process::Command;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rrtctl_exec_outputs_json() {
    // Start an ephemeral rrt server.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        rrt_daemon::serve_with_listener(listener).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Run the built rrtctl binary.
    let out = Command::new(env!("CARGO_BIN_EXE_rrtctl"))
        .args(["exec", &format!("http://{addr}"), "echo", "hi-bridge"])
        .output()
        .unwrap();

    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["returncode"], 0);
    assert!(v["stdout"].as_str().unwrap().contains("hi-bridge"));
}
