use rrt_daemon::pb::process_client::ProcessClient;
use rrt_daemon::pb::ExecRequest;
use std::time::Duration;

#[tokio::test]
async fn exec_echo_returns_stdout() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        rrt_daemon::serve_with_listener(listener).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut client = ProcessClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let resp = client
        .exec(ExecRequest {
            cmd: "echo".into(),
            args: vec!["hello-rrt".into()],
            cwd: String::new(),
            env: Default::default(),
            timeout_ms: 0,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.exit_code, 0);
    assert_eq!(String::from_utf8_lossy(&resp.stdout).trim(), "hello-rrt");
}

#[tokio::test]
async fn health_check_reports_healthy() {
    use rrt_daemon::pb::health_client::HealthClient;
    use rrt_daemon::pb::HealthCheckRequest;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        rrt_daemon::serve_with_listener(listener).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let mut c = HealthClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let r = c.check(HealthCheckRequest {}).await.unwrap().into_inner();
    assert!(r.healthy);
    assert!(!r.version.is_empty());
}
