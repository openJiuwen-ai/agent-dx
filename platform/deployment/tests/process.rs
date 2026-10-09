use adx_deployment::{
    config::Deployment,
    supervisor::{self, Request},
};
use adx_protocol::relay as pb;
use serde_json::json;
use std::{
    os::unix::fs::PermissionsExt,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
fn test_deployment(root: &Path, services: serde_json::Value) -> Deployment {
    serde_json::from_value(json!({"schema_version":1,"with_afs":true,"package_dir":root,"state_dir":root.join("state"),"redis_url":"redis://localhost:6379/","namespace":"test","restart_delay_ms":20,"stop_timeout_seconds":1,"services":services})).unwrap()
}
fn install_test_binary(root: &Path, name: &str, script: &str) {
    std::fs::create_dir_all(root.join("bin")).unwrap();
    let binary_path = root.join("bin").join(name);
    std::fs::write(&binary_path, script).unwrap();
    std::fs::set_permissions(binary_path, std::fs::Permissions::from_mode(0o700)).unwrap();
}
async fn wait_until_ready(root: &Path) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(status) =
                supervisor::request(root, Request::Status, Duration::from_secs(1)).await
            {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap()
}

fn graceful_service_script() -> &'static str {
    "#!/bin/sh\ntrap 'exit 0' TERM\nwhile :; do sleep 1; done\n"
}

fn service_status<'a>(status: &'a serde_json::Value, service_id: &str) -> &'a serde_json::Value {
    status
        .get("services")
        .and_then(serde_json::Value::as_array)
        .and_then(|services| {
            services.iter().find(|service| {
                service.get("id").and_then(serde_json::Value::as_str) == Some(service_id)
            })
        })
        .expect("service must be present in supervisor status")
}

#[tokio::test]
async fn repeated_exits_keep_restarting_until_recovery_and_scoped_stop() {
    let temp_directory = tempfile::Builder::new()
        .prefix("adx-p-")
        .tempdir_in("/tmp")
        .unwrap();
    let root = temp_directory.path();
    install_test_binary(
        root,
        "adx-coordinator",
        "#!/bin/sh\ncounter=\"$0.attempts\"\nn=0\n[ ! -f \"$counter\" ] || n=$(cat \"$counter\")\nn=$((n+1))\necho \"$n\" > \"$counter\"\n[ \"$n\" -gt 6 ] || exit 1\nexec sleep 100\n",
    );
    install_test_binary(root, "adx-apiserver", "#!/bin/sh\nexec sleep 100\n");
    let services =
        json!([{"id":"coordinator","role":"coordinator"},{"id":"api","role":"apiserver"}]);
    let deployment = test_deployment(root, services.clone());
    let state_directory = deployment.state_dir.clone();
    let supervisor_task = tokio::spawn(supervisor::run(deployment));
    wait_until_ready(&state_directory).await;
    assert!(supervisor::run(test_deployment(root, services))
        .await
        .is_err());
    let recovery = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status = wait_until_ready(&state_directory).await;
            let coordinator = service_status(&status, "coordinator");
            if coordinator
                .get("restarts")
                .and_then(serde_json::Value::as_u64)
                .is_some_and(|count| count >= 6)
                && coordinator
                    .get("pid")
                    .is_some_and(serde_json::Value::is_number)
            {
                assert_eq!(
                    coordinator.get("failed"),
                    Some(&serde_json::Value::Bool(false))
                );
                assert!(service_status(&status, "api")
                    .get("pid")
                    .is_some_and(serde_json::Value::is_number));
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    supervisor::request(&state_directory, Request::Stop, Duration::from_secs(5))
        .await
        .unwrap();
    supervisor_task.await.unwrap().unwrap();
    assert!(!state_directory.join("supervisor.sock").exists());
    recovery.expect("service must recover after more than six consecutive exits");
}
#[tokio::test]
async fn spawn_failures_keep_retrying_and_clear_failure_after_recovery() {
    let temp_directory = tempfile::Builder::new()
        .prefix("adx-spawn-")
        .tempdir_in("/tmp")
        .unwrap();
    let root = temp_directory.path();
    // Executable validation succeeds; the missing interpreter makes spawn fail.
    install_test_binary(root, "adx-coordinator", "#!/adx-missing-interpreter\n");
    let deployment = test_deployment(root, json!([{"id":"coordinator","role":"coordinator"}]));
    let state_directory = deployment.state_dir.clone();
    let supervisor_task = tokio::spawn(supervisor::run(deployment));
    let recovery = tokio::time::timeout(Duration::from_secs(5), async {
        let mut repaired = false;
        loop {
            let status = wait_until_ready(&state_directory).await;
            let service = service_status(&status, "coordinator");
            if !repaired
                && service
                    .get("restarts")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|count| count >= 6)
            {
                assert_eq!(service.get("failed"), Some(&serde_json::Value::Bool(true)));
                assert_eq!(service.get("pid"), Some(&serde_json::Value::Null));
                install_test_binary(root, "adx-coordinator", "#!/bin/sh\nexec sleep 100\n");
                repaired = true;
            } else if repaired && service.get("pid").is_some_and(serde_json::Value::is_number) {
                assert_eq!(service.get("failed"), Some(&serde_json::Value::Bool(false)));
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    supervisor::request(&state_directory, Request::Stop, Duration::from_secs(5))
        .await
        .unwrap();
    supervisor_task.await.unwrap().unwrap();
    recovery.expect("spawn retries must continue until the executable becomes runnable");
}

struct NodeAdmin(Arc<AtomicBool>);
#[tonic::async_trait]
impl pb::node_admin_service_server::NodeAdminService for NodeAdmin {
    async fn drain(
        &self,
        _: tonic::Request<pb::DrainRequest>,
    ) -> Result<tonic::Response<pb::DrainResponse>, tonic::Status> {
        if !self.0.load(Ordering::Acquire) {
            return Err(tonic::Status::unavailable("commit failed"));
        }
        Ok(tonic::Response::new(pb::DrainResponse {
            deleted_environments: 1,
        }))
    }
}
#[tokio::test]
async fn failed_environment_cleanup_keeps_dependencies_running_then_retries() {
    let temp_directory = tempfile::Builder::new()
        .prefix("adx-p-")
        .tempdir_in("/tmp")
        .unwrap();
    let root = temp_directory.path();
    for binary_name in ["adx-coordinator", "adxlet"] {
        install_test_binary(root, binary_name, graceful_service_script());
    }
    let deployment = test_deployment(
        root,
        json!([{"id":"coordinator","role":"coordinator"},{"id":"node","role":"adxlet","config":{"proxy_mode":"standalone"}}]),
    );
    let state_directory = deployment.state_dir.clone();
    std::fs::create_dir_all(&state_directory).unwrap();
    let listener = tokio::net::UnixListener::bind(state_directory.join("node-admin.sock")).unwrap();
    let allow_drain = Arc::new(AtomicBool::new(false));
    let service = NodeAdmin(allow_drain.clone());
    let rpc_task = tokio::spawn(async {
        tonic::transport::Server::builder()
            .add_service(pb::node_admin_service_server::NodeAdminServiceServer::new(
                service,
            ))
            .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(listener))
            .await
            .unwrap()
    });
    let supervisor_task = tokio::spawn(supervisor::run(deployment));
    let before = wait_until_ready(&state_directory).await;
    assert!(
        supervisor::request(&state_directory, Request::Stop, Duration::from_secs(5))
            .await
            .is_err()
    );
    let after = wait_until_ready(&state_directory).await;
    assert_eq!(before.get("services"), after.get("services"));
    allow_drain.store(true, Ordering::Release);
    supervisor::request(&state_directory, Request::Stop, Duration::from_secs(5))
        .await
        .unwrap();
    supervisor_task.await.unwrap().unwrap();
    rpc_task.abort();
}

#[tokio::test]
async fn supervisor_drains_rotated_logs_on_stop() {
    let temp_directory = tempfile::Builder::new()
        .prefix("adx-logs-")
        .tempdir_in("/tmp")
        .unwrap();
    let root = temp_directory.path();
    install_test_binary(root,"adx-coordinator","#!/bin/sh\ni=1; while [ $i -le 120 ]; do printf 'entry-%s\\n' \"$i\"; i=$((i+1)); done\nexec sleep 100\n");
    let mut deployment = test_deployment(
        root,
        json!([{"id":"coordinator","role":"coordinator","config":{}}]),
    );
    deployment.logging.enabled = true;
    deployment.logging.max_file_bytes = 64;
    deployment.logging.max_files = 100;
    let state_directory = deployment.state_dir.clone();
    let supervisor_task = tokio::spawn(supervisor::run(deployment));
    wait_until_ready(&state_directory).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if std::fs::read_dir(state_directory.join("logs"))
                .unwrap()
                .count()
                > 2
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    supervisor::request(&state_directory, Request::Stop, Duration::from_secs(5))
        .await
        .unwrap();
    supervisor_task.await.unwrap().unwrap();
    let mut files = std::fs::read_dir(state_directory.join("logs"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "gz"))
        .collect::<Vec<_>>();
    files.sort();
    let mut content = Vec::new();
    use std::io::Read;
    for file in files {
        flate2::read::GzDecoder::new(std::fs::File::open(file).unwrap())
            .read_to_end(&mut content)
            .unwrap();
    }
    content.extend(std::fs::read(state_directory.join("logs/coordinator.log")).unwrap());
    let expected = (1..=120)
        .map(|sequence| format!("entry-{sequence}\n"))
        .collect::<String>();
    assert_eq!(content, expected.as_bytes());
}

#[tokio::test]
async fn afs_services_start_and_stop_under_supervisor() {
    let temp_directory = tempfile::Builder::new()
        .prefix("adx-afs-stop-")
        .tempdir_in("/tmp")
        .unwrap();
    let root = temp_directory.path();
    for binary_name in ["adx-coordinator", "afs-meta", "afs-node"] {
        install_test_binary(root, binary_name, graceful_service_script());
    }
    let meta_config = root.join("meta.toml");
    let node_config = root.join("node.toml");
    std::fs::write(&meta_config, "id='meta'\n").unwrap();
    std::fs::write(&node_config, "id='node-a'\n").unwrap();
    let deployment = test_deployment(
        root,
        json!([
            {"id":"coordinator","role":"coordinator"},
            {"id":"meta","role":"afs-meta","config":{"config_file":meta_config}},
            {"id":"node","role":"afs-node","config":{"config_file":node_config}}
        ]),
    );
    let state_directory = deployment.state_dir.clone();
    let supervisor_task = tokio::spawn(supervisor::run(deployment));
    wait_until_ready(&state_directory).await;

    supervisor::request(&state_directory, Request::Stop, Duration::from_secs(5))
        .await
        .unwrap();
    supervisor_task.await.unwrap().unwrap();
    assert!(!state_directory.join("supervisor.sock").exists());
}

#[tokio::test]
async fn afs_stop_timeout_fails_without_stopping_meta_or_restarting_node() {
    let temp_directory = tempfile::Builder::new()
        .prefix("adx-afs-stop-timeout-")
        .tempdir_in("/tmp")
        .unwrap();
    let root = temp_directory.path();
    install_test_binary(root, "afs-meta", "#!/bin/sh\nexec sleep 100\n");
    install_test_binary(
        root,
        "afs-node",
        "#!/bin/sh\ntrap '' TERM\nwhile :; do sleep 1; done\n",
    );
    let meta_config = root.join("meta.toml");
    let node_config = root.join("node.toml");
    std::fs::write(&meta_config, "id='meta'\n").unwrap();
    std::fs::write(&node_config, "id='node-a'\n").unwrap();
    let deployment = test_deployment(
        root,
        json!([
            {"id":"meta","role":"afs-meta","config":{"config_file":meta_config}},
            {"id":"node","role":"afs-node","config":{"config_file":node_config}}
        ]),
    );
    let state_directory = deployment.state_dir.clone();
    let supervisor_task = tokio::spawn(supervisor::run(deployment));
    wait_until_ready(&state_directory).await;

    let stop_error = supervisor::request(&state_directory, Request::Stop, Duration::from_secs(3))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        stop_error.contains("supervisor operation failed"),
        "unexpected stop error: {stop_error}"
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    let status = supervisor::request(&state_directory, Request::Status, Duration::from_secs(3))
        .await
        .unwrap();
    assert!(service_status(&status, "meta")["pid"].is_number());
    assert_eq!(
        service_status(&status, "node")["failed"],
        serde_json::Value::Bool(true)
    );
    assert!(service_status(&status, "node")["pid"].is_null());

    supervisor_task.abort();
    let _ = supervisor_task.await;
}

#[tokio::test]
async fn afs_node_exit_124_on_sigterm_fails_stop_and_keeps_meta_running() {
    let temp_directory = tempfile::Builder::new()
        .prefix("adx-afs-stop-exit-124-")
        .tempdir_in("/tmp")
        .unwrap();
    let root = temp_directory.path();
    install_test_binary(root, "afs-meta", "#!/bin/sh\nexec sleep 100\n");
    install_test_binary(
        root,
        "afs-node",
        "#!/bin/sh\ntrap 'exit 124' TERM\nwhile :; do sleep 1; done\n",
    );
    let meta_config = root.join("meta.toml");
    let node_config = root.join("node.toml");
    std::fs::write(&meta_config, "id='meta'\n").unwrap();
    std::fs::write(&node_config, "id='node-a'\n").unwrap();
    let deployment = test_deployment(
        root,
        json!([
            {"id":"meta","role":"afs-meta","config":{"config_file":meta_config}},
            {"id":"node","role":"afs-node","config":{"config_file":node_config}}
        ]),
    );
    let state_directory = deployment.state_dir.clone();
    let supervisor_task = tokio::spawn(supervisor::run(deployment));
    wait_until_ready(&state_directory).await;

    let stop_error = supervisor::request(&state_directory, Request::Stop, Duration::from_secs(3))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        stop_error.contains("supervisor operation failed"),
        "unexpected stop error: {stop_error}"
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    let status = supervisor::request(&state_directory, Request::Status, Duration::from_secs(3))
        .await
        .unwrap();
    assert!(service_status(&status, "meta")["pid"].is_number());
    assert_eq!(
        service_status(&status, "node")["failed"],
        serde_json::Value::Bool(true)
    );
    assert!(service_status(&status, "node")["pid"].is_null());

    supervisor_task.abort();
    let _ = supervisor_task.await;
}

#[tokio::test]
async fn afs_node_reaped_before_stop_fails_stop_and_keeps_meta_running() {
    let temp_directory = tempfile::Builder::new()
        .prefix("adx-afs-prestop-exit-")
        .tempdir_in("/tmp")
        .unwrap();
    let root = temp_directory.path();
    install_test_binary(root, "afs-meta", "#!/bin/sh\nexec sleep 100\n");
    install_test_binary(root, "afs-node", "#!/bin/sh\nexit 124\n");
    let meta_config = root.join("meta.toml");
    let node_config = root.join("node.toml");
    std::fs::write(&meta_config, "id='meta'\n").unwrap();
    std::fs::write(&node_config, "id='node-a'\n").unwrap();
    let deployment = test_deployment(
        root,
        json!([
            {"id":"meta","role":"afs-meta","config":{"config_file":meta_config}},
            {"id":"node","role":"afs-node","config":{"config_file":node_config}}
        ]),
    );
    let state_directory = deployment.state_dir.clone();
    let supervisor_task = tokio::spawn(supervisor::run(deployment));
    wait_until_ready(&state_directory).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status =
                supervisor::request(&state_directory, Request::Status, Duration::from_secs(3))
                    .await
                    .unwrap();
            if service_status(&status, "node")["failed"] == serde_json::Value::Bool(true) {
                assert!(service_status(&status, "node")["pid"].is_null());
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();

    let stop_error = supervisor::request(&state_directory, Request::Stop, Duration::from_secs(3))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        stop_error.contains("supervisor operation failed"),
        "unexpected stop error: {stop_error}"
    );
    let status = supervisor::request(&state_directory, Request::Status, Duration::from_secs(3))
        .await
        .unwrap();
    assert!(service_status(&status, "meta")["pid"].is_number());
    assert!(service_status(&status, "node")["pid"].is_null());

    supervisor_task.abort();
    let _ = supervisor_task.await;
}

#[tokio::test]
async fn afs_status_reports_actual_http_health() {
    let temp_directory = tempfile::Builder::new()
        .prefix("adx-afs-health-")
        .tempdir_in("/tmp")
        .unwrap();
    let root = temp_directory.path();
    install_test_binary(root, "afs-node", graceful_service_script());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let health_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buffer = [0; 512];
        let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buffer)
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::write_all(
            &mut stream,
            b"HTTP/1.1 200 OK\r\nContent-Length: 18\r\n\r\n{\"status\":\"ready\"}",
        )
        .await
        .unwrap();
    });
    let node_config = root.join("node.toml");
    std::fs::write(&node_config, "id='node-a'\n").unwrap();
    let deployment = test_deployment(
        root,
        json!([{
            "id":"node",
            "role":"afs-node",
            "config":{"config_file":node_config,"health_url":format!("http://{address}/health")}
        }]),
    );
    let state_directory = deployment.state_dir.clone();
    let supervisor_task = tokio::spawn(supervisor::run(deployment));

    let status = wait_until_ready(&state_directory).await;

    assert_eq!(
        service_status(&status, "node")["health"]["ready"],
        serde_json::Value::Bool(true)
    );
    supervisor::request(&state_directory, Request::Stop, Duration::from_secs(5))
        .await
        .unwrap();
    supervisor_task.await.unwrap().unwrap();
    health_task.await.unwrap();
}

#[tokio::test]
async fn afs_status_does_not_treat_http_200_degraded_as_ready() {
    let temp_directory = tempfile::Builder::new()
        .prefix("adx-afs-health-")
        .tempdir_in("/tmp")
        .unwrap();
    let root = temp_directory.path();
    install_test_binary(root, "afs-node", graceful_service_script());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let health_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buffer = [0; 512];
        let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buffer)
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::write_all(
            &mut stream,
            b"HTTP/1.1 200 OK\r\nContent-Length: 21\r\n\r\n{\"status\":\"degraded\"}",
        )
        .await
        .unwrap();
    });
    let node_config = root.join("node.toml");
    std::fs::write(&node_config, "id='node-a'\n").unwrap();
    let deployment = test_deployment(
        root,
        json!([{
            "id":"node",
            "role":"afs-node",
            "config":{"config_file":node_config,"health_url":format!("http://{address}/health")}
        }]),
    );
    let state_directory = deployment.state_dir.clone();
    let supervisor_task = tokio::spawn(supervisor::run(deployment));

    let status = wait_until_ready(&state_directory).await;

    assert_eq!(
        service_status(&status, "node")["health"]["ready"],
        serde_json::Value::Bool(false)
    );
    supervisor::request(&state_directory, Request::Stop, Duration::from_secs(5))
        .await
        .unwrap();
    supervisor_task.await.unwrap().unwrap();
    health_task.await.unwrap();
}
