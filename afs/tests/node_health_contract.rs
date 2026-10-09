use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use afs::{
    config::{Cli, Config, Role},
    node::{
        Node, NodeReadiness,
        api::rest,
        rpc::meta::{MetaPersistenceCapability, NodeRegistrationReadiness},
    },
    runtime::Observability,
};
use axum::{
    body::to_bytes,
    http::{Request, StatusCode},
};
use serde_json::Value;
use tower::ServiceExt;

fn test_config(data_dir: PathBuf) -> Config {
    Config::resolve(
        Role::Node,
        Cli {
            data_dir: Some(data_dir),
            uds_path: Some(PathBuf::from("/tmp/afs-node-health-test.sock")),
            data_mode: Some("grpc".into()),
            ..Cli::default()
        },
    )
    .unwrap()
}

fn node_with_readiness(config: Config, readiness: Arc<NodeReadiness>) -> Arc<Node> {
    Arc::new(Node {
        config,
        observability: Observability::new().unwrap(),
        session_id: "session-health-test".into(),
        readiness,
        #[cfg(feature = "ownerfs")]
        ownerfs: None,
        #[cfg(feature = "dfs")]
        dfs: None,
    })
}

fn mark_resources_ready(readiness: &NodeReadiness) {
    readiness.note_resource_observation(true, None, false, None);
}

fn registration(
    persistence: MetaPersistenceCapability,
    healthy: bool,
    persistent_ready: bool,
    detail: &str,
) -> NodeRegistrationReadiness {
    NodeRegistrationReadiness {
        lease_epoch: 7,
        expires_at_unix_ms: 123,
        meta_backend_persistence: persistence,
        meta_backend_healthy: healthy,
        meta_persistence_ready: persistent_ready,
        meta_persistence_detail: detail.into(),
    }
}

async fn get_health(node: Arc<Node>) -> (StatusCode, Value) {
    let response = rest::router(node)
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
async fn node_health_can_report_foundation_ready_without_configured_mounts() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let config = test_config(data_dir);
    let readiness = Arc::new(NodeReadiness::new(
        false, None, None, None, None, "grpc", None,
    ));
    mark_resources_ready(&readiness);
    let node = node_with_readiness(config, readiness);

    let (status, body) = get_health(node).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ready");
    assert_eq!(body["scope"], "foundation");
    assert_eq!(body["configured_filesystems_ready"], false);
    assert_eq!(body["checks"]["mounts"]["configured"], false);
    assert_eq!(body["checks"]["data_device"]["ready"], true);
}

#[tokio::test]
async fn node_health_returns_unavailable_when_configured_mount_is_absent() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let missing_mount = temp.path().join("missing-ownerfs-mount");
    let mut config = test_config(data_dir);
    config.ownerfs_mount = Some(missing_mount.clone());
    let readiness = Arc::new(NodeReadiness::new(
        false,
        None,
        None,
        Some(missing_mount),
        None,
        "grpc",
        None,
    ));
    mark_resources_ready(&readiness);
    let node = node_with_readiness(config, readiness);

    let (status, body) = get_health(node).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["status"], "degraded");
    assert_eq!(body["checks"]["mounts"]["configured"], true);
    assert_eq!(body["checks"]["mounts"]["ready"], false);
    assert_eq!(body["checks"]["mounts"]["ownerfs"]["ready"], false);
}

#[tokio::test]
async fn node_health_returns_unavailable_after_known_meta_registration_failure() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let config = test_config(data_dir);
    let readiness = Arc::new(NodeReadiness::new(
        true,
        Some(7),
        Some(Instant::now()),
        None,
        None,
        "grpc",
        None,
    ));
    readiness.note_registration_failure("injected heartbeat failure");
    mark_resources_ready(&readiness);
    let node = node_with_readiness(config, readiness);

    let (status, body) = get_health(node).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["status"], "degraded");
    assert_eq!(body["checks"]["meta_persistence"]["ready"], false);
    assert_eq!(body["checks"]["node_registration"]["ready"], false);
    assert_eq!(
        body["checks"]["node_registration"]["last_error"],
        "injected heartbeat failure"
    );
}

#[tokio::test]
async fn node_health_recovers_after_retryable_meta_registration_failure_clears() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let config = test_config(data_dir);
    let readiness = Arc::new(NodeReadiness::new(
        true,
        Some(7),
        Some(Instant::now()),
        None,
        None,
        "grpc",
        None,
    ));
    readiness.note_registration_failure("retryable heartbeat failure");
    mark_resources_ready(&readiness);
    let node = node_with_readiness(config, readiness.clone());

    let (failed_status, failed_body) = get_health(node.clone()).await;

    assert_eq!(failed_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(failed_body["checks"]["node_registration"]["ready"], false);

    readiness.note_registration_success(true);
    let (recovered_status, recovered_body) = get_health(node).await;

    assert_eq!(recovered_status, StatusCode::OK);
    assert_eq!(recovered_body["checks"]["node_registration"]["ready"], true);
    assert_eq!(
        recovered_body["checks"]["node_registration"]["last_error"],
        Value::Null
    );
}

#[tokio::test]
async fn node_health_uses_latest_registration_persistence_readiness() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let config = test_config(data_dir);
    let readiness = Arc::new(NodeReadiness::new(
        true,
        Some(7),
        Some(Instant::now()),
        None,
        None,
        "grpc",
        None,
    ));
    mark_resources_ready(&readiness);
    let node = node_with_readiness(config, readiness.clone());

    readiness.note_registration_success_with_persistence(
        false,
        Some("volatile backend health probe passed".into()),
    );
    let (volatile_status, volatile_body) = get_health(node.clone()).await;
    assert_eq!(volatile_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(volatile_body["checks"]["meta_persistence"]["ready"], false);
    assert_eq!(
        volatile_body["checks"]["meta_persistence"]["backend_persistence"],
        "unknown"
    );
    assert_eq!(
        volatile_body["checks"]["meta_persistence"]["last_error"],
        "volatile backend health probe passed"
    );

    readiness.note_registration_success_with_persistence(
        true,
        Some("persistent backend health probe passed".into()),
    );
    let (persistent_status, persistent_body) = get_health(node).await;
    assert_eq!(persistent_status, StatusCode::OK);
    assert_eq!(persistent_body["checks"]["meta_persistence"]["ready"], true);
    assert_eq!(
        persistent_body["checks"]["meta_persistence"]["persistent_ready"],
        true
    );
    assert_eq!(
        persistent_body["checks"]["meta_persistence"]["last_error"],
        Value::Null
    );
}

#[tokio::test]
async fn node_health_rejects_volatile_meta_without_explicit_policy() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let config = test_config(data_dir);
    let readiness = Arc::new(NodeReadiness::new(
        true,
        Some(7),
        Some(Instant::now()),
        None,
        None,
        "grpc",
        None,
    ));
    readiness.note_registration_capability(&registration(
        MetaPersistenceCapability::Volatile,
        true,
        false,
        "volatile backend health probe passed",
    ));
    mark_resources_ready(&readiness);
    let node = node_with_readiness(config, readiness);

    let (status, body) = get_health(node).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["checks"]["meta_persistence"]["ready"], false);
    assert_eq!(
        body["checks"]["meta_persistence"]["functional_ready"],
        false
    );
    assert_eq!(
        body["checks"]["meta_persistence"]["persistent_ready"],
        false
    );
    assert_eq!(
        body["checks"]["meta_persistence"]["allow_volatile_meta"],
        false
    );
    assert_eq!(
        body["checks"]["meta_persistence"]["backend_persistence"],
        "volatile"
    );
    assert_eq!(body["checks"]["meta_persistence"]["backend_healthy"], true);
}

#[tokio::test]
async fn node_health_allows_healthy_volatile_meta_only_when_policy_opts_in() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let mut config = test_config(data_dir);
    config.allow_volatile_meta = true;
    let readiness = Arc::new(NodeReadiness::new(
        true,
        Some(7),
        Some(Instant::now()),
        None,
        None,
        "grpc",
        None,
    ));
    readiness.set_allow_volatile_meta(true);
    mark_resources_ready(&readiness);
    let node = node_with_readiness(config, readiness.clone());

    readiness.note_registration_capability(&registration(
        MetaPersistenceCapability::Volatile,
        true,
        false,
        "volatile backend health probe passed",
    ));
    let (ready_status, ready_body) = get_health(node.clone()).await;
    assert_eq!(ready_status, StatusCode::OK);
    assert_eq!(ready_body["checks"]["meta_persistence"]["ready"], false);
    assert_eq!(
        ready_body["checks"]["meta_persistence"]["functional_ready"],
        true
    );
    assert_eq!(
        ready_body["checks"]["meta_persistence"]["persistent_ready"],
        false
    );
    assert_eq!(
        ready_body["checks"]["meta_persistence"]["allow_volatile_meta"],
        true
    );
    assert_eq!(
        ready_body["checks"]["meta_persistence"]["backend_persistence"],
        "volatile"
    );

    readiness.note_registration_capability(&registration(
        MetaPersistenceCapability::Volatile,
        false,
        false,
        "volatile backend health probe failed",
    ));
    let (unhealthy_status, unhealthy_body) = get_health(node.clone()).await;
    assert_eq!(unhealthy_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(unhealthy_body["checks"]["meta_persistence"]["ready"], false);
    assert_eq!(
        unhealthy_body["checks"]["meta_persistence"]["backend_healthy"],
        false
    );

    readiness.note_registration_capability(&registration(
        MetaPersistenceCapability::Unknown,
        true,
        false,
        "Meta persistence capability was not reported by heartbeat",
    ));
    let (unknown_status, unknown_body) = get_health(node).await;
    assert_eq!(unknown_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(unknown_body["checks"]["meta_persistence"]["ready"], false);
    assert_eq!(
        unknown_body["checks"]["meta_persistence"]["backend_persistence"],
        "unknown"
    );
}

#[tokio::test]
async fn initial_meta_persistence_update_preserves_registration_age() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let config = test_config(data_dir);
    let readiness = Arc::new(NodeReadiness::new(
        true,
        Some(7),
        Some(Instant::now() - Duration::from_secs(31)),
        None,
        None,
        "grpc",
        None,
    ));
    readiness.note_meta_persistence(true, None);
    mark_resources_ready(&readiness);
    let node = node_with_readiness(config, readiness);

    let (status, body) = get_health(node).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["checks"]["meta_persistence"]["functional_ready"], true);
    assert_eq!(body["checks"]["node_registration"]["ready"], false);
    assert_eq!(
        body["checks"]["node_registration"]["last_error"],
        "node registration heartbeat is stale"
    );
}

#[tokio::test]
async fn initial_registration_capability_preserves_registration_age() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let mut config = test_config(data_dir);
    config.allow_volatile_meta = true;
    let readiness = Arc::new(NodeReadiness::new(
        true,
        Some(7),
        Some(Instant::now() - Duration::from_secs(31)),
        None,
        None,
        "grpc",
        None,
    ));
    readiness.set_allow_volatile_meta(true);
    readiness.note_initial_registration_capability(&registration(
        MetaPersistenceCapability::Volatile,
        true,
        false,
        "volatile backend health probe passed",
    ));
    mark_resources_ready(&readiness);
    let node = node_with_readiness(config, readiness);

    let (status, body) = get_health(node).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["checks"]["meta_persistence"]["functional_ready"], true);
    assert_eq!(body["checks"]["node_registration"]["ready"], false);
    assert_eq!(
        body["checks"]["node_registration"]["last_error"],
        "node registration heartbeat is stale"
    );
}

#[tokio::test]
async fn node_health_fails_closed_on_stale_meta_heartbeat() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let config = test_config(data_dir);
    let readiness = Arc::new(NodeReadiness::new(
        true,
        Some(7),
        Some(Instant::now()),
        None,
        None,
        "grpc",
        None,
    ));
    readiness.note_meta_persistence(true, None);
    mark_resources_ready(&readiness);
    readiness.force_stale_registration_for_tests();
    let node = node_with_readiness(config, readiness);

    let (status, body) = get_health(node).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["checks"]["node_registration"]["ready"], false);
    assert_eq!(
        body["checks"]["node_registration"]["last_error"],
        "node registration heartbeat is stale"
    );
}

#[tokio::test]
async fn node_health_rejects_substituted_non_afs_mount() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let substituted_mount = PathBuf::from("/proc");
    let mut config = test_config(data_dir);
    config.ownerfs_mount = Some(substituted_mount.clone());
    let readiness = Arc::new(NodeReadiness::new(
        false,
        None,
        None,
        Some(substituted_mount),
        None,
        "grpc",
        None,
    ));
    mark_resources_ready(&readiness);
    let node = node_with_readiness(config, readiness);

    let (status, body) = get_health(node).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["checks"]["mounts"]["ownerfs"]["ready"], false);
    assert_eq!(
        body["checks"]["mounts"]["ownerfs"]["expected_source"],
        "afs-ownerfs"
    );
}

#[tokio::test]
async fn repeated_node_health_gets_do_not_touch_data_dir() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let before = std::fs::metadata(&data_dir).unwrap().modified().unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let config = test_config(data_dir.clone());
    let readiness = Arc::new(NodeReadiness::new(
        false, None, None, None, None, "grpc", None,
    ));
    mark_resources_ready(&readiness);
    let node = node_with_readiness(config, readiness);

    for _ in 0..3 {
        let (status, body) = get_health(node.clone()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["checks"]["data_device"]["ready"], true);
    }

    let after = std::fs::metadata(&data_dir).unwrap().modified().unwrap();
    assert_eq!(
        before, after,
        "health GET must not create, remove, or sync probe files"
    );
}

#[tokio::test]
async fn node_health_fails_closed_before_resource_sampler_observes_device() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let config = test_config(data_dir);
    let readiness = Arc::new(NodeReadiness::new(
        false, None, None, None, None, "grpc", None,
    ));
    let node = node_with_readiness(config, readiness);

    let (status, body) = get_health(node).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["checks"]["data_device"]["ready"], false);
    assert_eq!(body["checks"]["data_device"]["observed"], false);
    assert_eq!(
        body["checks"]["data_device"]["error"],
        "no readiness observation has completed"
    );
}

#[tokio::test]
async fn node_health_reports_data_device_failure_from_sampler_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let config = test_config(data_dir);
    let readiness = Arc::new(NodeReadiness::new(
        false, None, None, None, None, "grpc", None,
    ));
    readiness.note_resource_observation(false, Some("read-only filesystem".into()), false, None);
    let node = node_with_readiness(config, readiness);

    let (status, body) = get_health(node).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["checks"]["data_device"]["ready"], false);
    assert_eq!(body["checks"]["data_device"]["observed"], true);
    assert_eq!(
        body["checks"]["data_device"]["error"],
        "read-only filesystem"
    );
}

#[tokio::test]
async fn node_health_fails_closed_on_stale_resource_observation() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let config = test_config(data_dir);
    let readiness = Arc::new(NodeReadiness::new(
        false, None, None, None, None, "grpc", None,
    ));
    mark_resources_ready(&readiness);
    readiness.force_stale_resource_observation_for_tests();
    let node = node_with_readiness(config, readiness);

    let (status, body) = get_health(node).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["checks"]["data_device"]["stale"], true);
    assert_eq!(
        body["checks"]["data_device"]["error"],
        "readiness observation is stale"
    );
}

#[tokio::test]
async fn node_health_requires_observed_rdma_availability_when_rdma_required() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let mut config = test_config(data_dir);
    config.data_mode = "rdma".into();
    config.rdma_device = Some("missing-rdma-test-device".into());
    let readiness = Arc::new(NodeReadiness::new(
        false,
        None,
        None,
        None,
        None,
        "rdma",
        Some("missing-rdma-test-device"),
    ));
    readiness.note_resource_observation(
        true,
        None,
        false,
        Some("RDMA device missing-rdma-test-device has no ACTIVE port".into()),
    );
    let node = node_with_readiness(config, readiness);

    let (status, body) = get_health(node).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["checks"]["rdma"]["required"], true);
    assert_eq!(body["checks"]["rdma"]["ready"], false);
    assert_eq!(body["checks"]["rdma"]["available"], false);
}

#[test]
fn rdma_sampler_requires_accepted_port_one_not_any_active_port() {
    let temp = tempfile::tempdir().unwrap();
    let device = temp.path().join("rxe-test");
    std::fs::create_dir_all(device.join("ports/1")).unwrap();
    std::fs::create_dir_all(device.join("ports/2")).unwrap();
    std::fs::write(device.join("ports/1/state"), "DOWN\n").unwrap();
    std::fs::write(device.join("ports/2/state"), "ACTIVE\n").unwrap();

    let (available, error) =
        afs::node::sample_rdma_sysfs_at(temp.path(), true, true, Some("rxe-test"));

    assert!(!available);
    assert!(error.unwrap().contains("accepted port 1"));
}

#[test]
fn rdma_sampler_rejects_active_defer_and_accepts_exact_active() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("rxe-test/ports/1/state");
    std::fs::create_dir_all(state.parent().unwrap()).unwrap();
    for rejected in ["5: ACTIVE_DEFER\n", "INACTIVE\n", "4: ACTIVE_EXTRA\n"] {
        std::fs::write(&state, rejected).unwrap();
        let (available, error) =
            afs::node::sample_rdma_sysfs_at(temp.path(), true, true, Some("rxe-test"));
        assert!(!available, "accepted non-ACTIVE state {rejected:?}");
        assert!(error.unwrap().contains("not ACTIVE"));
    }
    for accepted in ["4: ACTIVE\n", "ACTIVE\n"] {
        std::fs::write(&state, accepted).unwrap();
        let (available, error) =
            afs::node::sample_rdma_sysfs_at(temp.path(), true, true, Some("rxe-test"));
        assert!(available);
        assert!(error.is_none());
    }
}

#[tokio::test]
async fn blocked_sampler_shutdown_returns_error_from_real_pending_worker() {
    let readiness = Arc::new(NodeReadiness::new(
        false, None, None, None, None, "grpc", None,
    ));
    let release = readiness.start_blocked_resource_sample_for_tests();

    let result = readiness
        .shutdown_resource_sampler_with_timeout(Duration::from_millis(20))
        .await;

    assert!(result.is_err());
    let _ = release.send(());
}

#[tokio::test]
async fn resource_sampler_is_singleflight_and_reports_blocked_worker() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let config = test_config(data_dir.clone());
    let readiness = Arc::new(NodeReadiness::new(
        false, None, None, None, None, "grpc", None,
    ));
    let release = readiness.start_blocked_resource_sample_for_tests();

    assert!(!readiness.start_resource_sample(data_dir));
    let node = node_with_readiness(config, readiness);
    let (status, body) = get_health(node).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        body["checks"]["data_device"]["error"],
        "readiness sampler is still running"
    );
    let _ = release.send(());
}
