use adx_control_cli::{config::Deployment, supervisor};
use adx_protocol::node_proxy as pb;
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
fn config(root: &Path, services: serde_json::Value) -> Deployment {
    serde_json::from_value(json!({"schema_version":1,"package_dir":root,"state_dir":root.join("state"),"redis_url":"redis://localhost:6379/","namespace":"test","restart_limit":2,"restart_delay_ms":20,"stop_timeout_seconds":1,"services":services})).unwrap()
}
fn bin(root: &Path, name: &str, script: &str) {
    std::fs::create_dir_all(root.join("bin")).unwrap();
    let p = root.join("bin").join(name);
    std::fs::write(&p, script).unwrap();
    std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o700)).unwrap();
}
async fn ready(root: &Path) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(s) = supervisor::request(root, "status", Duration::from_secs(1)).await {
                return s;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn restart_budget_lock_and_scoped_stop() {
    let tmp = tempfile::Builder::new()
        .prefix("adx-p-")
        .tempdir_in("/tmp")
        .unwrap();
    let root = tmp.path();
    bin(root, "adx-master", "#!/bin/sh\nexit 1\n");
    bin(root, "adx-sandbox-api", "#!/bin/sh\nexec sleep 100\n");
    let services = json!([{"id":"master","role":"master"},{"id":"api","role":"sandbox-api"}]);
    let d = config(root, services.clone());
    let state = d.state_dir.clone();
    let task = tokio::spawn(supervisor::run(d));
    ready(&state).await;
    assert!(supervisor::run(config(root, services)).await.is_err());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let s = ready(&state).await;
            if s["services"][0]["failed"] == true {
                assert_eq!(s["services"][0]["restarts"], 2);
                assert!(s["services"][1]["pid"].is_number());
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    supervisor::request(&state, "stop", Duration::from_secs(5))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    assert!(!state.join("supervisor.sock").exists());
}
struct Admin(Arc<AtomicBool>);
#[tonic::async_trait]
impl pb::node_admin_service_server::NodeAdminService for Admin {
    async fn drain(
        &self,
        _: tonic::Request<pb::DrainRequest>,
    ) -> Result<tonic::Response<pb::DrainResponse>, tonic::Status> {
        if !self.0.load(Ordering::Acquire) {
            return Err(tonic::Status::unavailable("commit failed"));
        }
        Ok(tonic::Response::new(pb::DrainResponse {
            deleted_instances: 1,
        }))
    }
}
#[tokio::test]
async fn failed_instance_cleanup_keeps_dependencies_running_then_retries() {
    let tmp = tempfile::Builder::new()
        .prefix("adx-p-")
        .tempdir_in("/tmp")
        .unwrap();
    let root = tmp.path();
    for n in ["adx-master", "adx-node-manager"] {
        bin(root, n, "#!/bin/sh\nexec sleep 100\n");
    }
    let d = config(
        root,
        json!([{"id":"master","role":"master"},{"id":"node","role":"node-manager"}]),
    );
    let state = d.state_dir.clone();
    std::fs::create_dir_all(&state).unwrap();
    let listener = tokio::net::UnixListener::bind(state.join("node-admin.sock")).unwrap();
    let allow = Arc::new(AtomicBool::new(false));
    let service = Admin(allow.clone());
    let rpc = tokio::spawn(async {
        tonic::transport::Server::builder()
            .add_service(pb::node_admin_service_server::NodeAdminServiceServer::new(
                service,
            ))
            .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(listener))
            .await
            .unwrap()
    });
    let task = tokio::spawn(supervisor::run(d));
    let before = ready(&state).await;
    assert!(supervisor::request(&state, "stop", Duration::from_secs(5))
        .await
        .is_err());
    let after = ready(&state).await;
    assert_eq!(before["services"], after["services"]);
    allow.store(true, Ordering::Release);
    supervisor::request(&state, "stop", Duration::from_secs(5))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    rpc.abort();
}
