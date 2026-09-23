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
    serde_json::from_value(json!({"schema_version":1,"package_dir":root,"state_dir":root.join("state"),"redis_url":"redis://localhost:6379/","namespace":"test","restart_limit":2,"restart_delay_ms":20,"stop_timeout_seconds":1,"services":services})).unwrap()
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
async fn restart_budget_lock_and_scoped_stop() {
    let temp_directory = tempfile::Builder::new()
        .prefix("adx-p-")
        .tempdir_in("/tmp")
        .unwrap();
    let root = temp_directory.path();
    install_test_binary(root, "adx-coordinator", "#!/bin/sh\nexit 1\n");
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
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status = wait_until_ready(&state_directory).await;
            let coordinator = service_status(&status, "coordinator");
            if coordinator.get("failed") == Some(&serde_json::Value::Bool(true)) {
                assert_eq!(
                    coordinator.get("restarts"),
                    Some(&serde_json::Value::from(2))
                );
                assert!(service_status(&status, "api")
                    .get("pid")
                    .is_some_and(serde_json::Value::is_number));
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
    assert!(!state_directory.join("supervisor.sock").exists());
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
        install_test_binary(root, binary_name, "#!/bin/sh\nexec sleep 100\n");
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
