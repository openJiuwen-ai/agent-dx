//! Runtime-facing REST. Explicit diagnostics exercise real APIs, never run on POSIX hot paths.
//!
//! `/v1/diagnostics` 是显式演示入口：Node B→Meta Ping→A 控制 Ping→A 数据写/读。
//! 它调用正式 caller/adapter，不是另写一套模拟传输。正常 FUSE 回调不会自动触发这些 RPC。
//! REST 面向 runtime 的 snapshot/publish API 尚未实现；不要用本诊断接口推断镜像发布成功。
use crate::error::RestError;
use crate::node::{
    AfsMountIdentity, Node, current_visible_afs_mount_id_in_mountinfo,
    rpc::{
        meta::{self, MetaPersistenceCapability},
        peer::{DataClientOptions, DataMode, connect_data_client},
    },
};
use afs_error::{
    CLIENT_ARGUMENT_INVALID, CLIENT_CONNECTION_UNAVAILABLE, DIAGNOSTICS_NOT_CONFIGURED, Error,
    METRICS_FAILED, NODE_TRANSFER_CORRUPT_DATA,
};
use afs_protocol::node_control::{PingRequest, node_control_client::NodeControlClient};
use afs_tracing::{Instrument, tracing};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

pub fn router(node: Arc<Node>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/ping", get(ping))
        .route("/v1/diagnostics", post(diagnostics))
        .route("/metrics", get(metrics))
        .with_state(node)
}
async fn health(State(node): State<Arc<Node>>) -> (StatusCode, Json<Value>) {
    let report = node_health_report(&node);
    let status = if report.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(report.body))
}
async fn ping(State(node): State<Arc<Node>>) -> Json<Value> {
    node.observability.record("node", "rest_ping", true);
    Json(json!({"message":format!("pong from {}",node.config.id)}))
}
async fn metrics(State(node): State<Arc<Node>>) -> Result<String, RestError> {
    afs_metrics::encode_text(&node.observability.registry)
        .map_err(|e| RestError(Error::coded(METRICS_FAILED, e.to_string())))
}
async fn diagnostics(State(node): State<Arc<Node>>) -> Result<Json<Value>, RestError> {
    let result = diagnose(node.clone())
        .instrument(tracing::info_span!("node.diagnostics"))
        .await;
    node.observability
        .record("node", "diagnostics", result.is_ok());
    result.map(Json).map_err(|e| {
        afs_logging::error!("node.diagnostics failed";"error"=>e.to_string());
        RestError(e)
    })
}
async fn diagnose(node: Arc<Node>) -> afs_error::Result<Value> {
    let config = &node.config;
    let meta_endpoint = config.meta_endpoint.as_ref().ok_or_else(|| {
        Error::coded(
            DIAGNOSTICS_NOT_CONFIGURED,
            "diagnostics requires meta_endpoint",
        )
    })?;
    let peer_endpoint = config.peer_endpoint.as_ref().ok_or_else(|| {
        Error::coded(
            DIAGNOSTICS_NOT_CONFIGURED,
            "diagnostics requires peer_endpoint",
        )
    })?;
    let timeout = Duration::from_millis(config.timeout_ms);
    let meta_pong = meta::ping(meta_endpoint, &config.id, timeout).await?;
    let grpc_config = afs_transport::grpc::GrpcConfig {
        connect_timeout: timeout,
        request_timeout: timeout,
        ..Default::default()
    };
    let channel = grpc_config
        .configure_client(
            tonic::transport::Endpoint::from_shared(peer_endpoint.clone())
                .map_err(|e| Error::coded(CLIENT_ARGUMENT_INVALID, e.to_string()))?,
        )
        .connect()
        .await
        .map_err(|e| Error::coded(CLIENT_CONNECTION_UNAVAILABLE, e.to_string()))?;
    let mut control = NodeControlClient::new(afs_tracing::traced_channel(channel));
    let pong = control
        .ping(afs_tracing::request_with_current_context(PingRequest {
            payload: "ping".into(),
        }))
        .await
        .map_err(afs_transport::grpc::error_status::status_to_error)?
        .into_inner();
    let mode = match config.data_mode.as_str() {
        "grpc" => DataMode::Grpc,
        "rdma" => DataMode::Rdma,
        _ => DataMode::Auto,
    };
    let mut data = connect_data_client(DataClientOptions {
        endpoint: peer_endpoint.clone(),
        mode,
        rdma_device: config.rdma_device.clone(),
        timeout,
    })
    .await?;
    let chosen = data.mode().to_owned();
    // Dedicated foundation diagnostic file, not an OwnerFs/DFS file or publication.
    let name = format!("probe-{}", config.id);
    let transfer = async {
        let written = data.write(&name, 0, b"AFShello".to_vec()).await?;
        let read = data.read(&name, 0, 8).await?;
        if written != 8 || read != b"AFShello" {
            return Err::<(), Error>(Error::coded(
                NODE_TRANSFER_CORRUPT_DATA,
                "data roundtrip mismatch",
            ));
        }
        Ok(())
    }
    .await;
    let closed = data.close().await;
    transfer?;
    closed?;
    afs_logging::info!("node.diagnostics completed";"mode"=>&chosen,"bytes"=>8);
    Ok(json!({"ok":true,"mode":chosen,"bytes":8,"meta":meta_pong,"peer_control":pong.payload}))
}

struct HealthReport {
    ready: bool,
    body: Value,
}

fn node_health_report(node: &Node) -> HealthReport {
    let meta_functional_ready = node.readiness.meta_persistence_usable();
    let meta_persistent_ready = node.readiness.meta_persistent_ready();
    let registration_ready = node.readiness.node_registration_ready();
    let data_device = data_device_check(node);
    let ownerfs_mount = mount_check(node.readiness.ownerfs_mount(), "afs-ownerfs");
    let dfs_mount = mount_check(node.readiness.dfs_mount(), "afs-dfs");
    let mounts_ready = ownerfs_mount.ready && dfs_mount.ready;
    let mounts_configured = ownerfs_mount.configured || dfs_mount.configured;
    let rdma = rdma_check(node);
    let ready = meta_functional_ready
        && registration_ready
        && data_device.ready
        && mounts_ready
        && rdma.ready;
    let scope = if mounts_configured && mounts_ready {
        "configured_fs"
    } else {
        "foundation"
    };

    HealthReport {
        ready,
        body: json!({
            "status": if ready { "ready" } else { "degraded" },
            "role": "node",
            "id": node.config.id,
            "scope": scope,
            "ownerfs": node.config.ownerfs,
            "dfs": node.config.dfs,
            "configured_filesystems_ready": mounts_configured && mounts_ready,
            "checks": {
                "meta_persistence": {
                    "required": node.readiness.meta_required(),
                    "ready": meta_persistent_ready,
                    "functional_ready": meta_functional_ready,
                    "persistent_ready": meta_persistent_ready,
                    "allow_volatile_meta": node.readiness.allow_volatile_meta(),
                    "backend_persistence": meta_persistence_label(node.readiness.meta_backend_persistence()),
                    "backend_healthy": node.readiness.meta_backend_healthy(),
                    "source": "Meta store durability capability reported by registration heartbeat",
                    "last_error": node.readiness.meta_persistence_error()
                },
                "node_registration": {
                    "required": node.readiness.meta_required(),
                    "ready": registration_ready,
                    "session_id": node.session_id,
                    "lease_epoch": node.readiness.registered_epoch(),
                    "last_success_age_ms": node.readiness.node_registration_age_ms(),
                    "fresh_for_ms": 30_000,
                    "last_error": node.readiness.node_registration_error()
                },
                "data_device": data_device.body,
                "mounts": {
                    "configured": mounts_configured,
                    "ready": mounts_ready,
                    "ownerfs": ownerfs_mount.body,
                    "dfs": dfs_mount.body
                },
                "rdma": rdma.body
            }
        }),
    }
}

fn meta_persistence_label(persistence: MetaPersistenceCapability) -> &'static str {
    match persistence {
        MetaPersistenceCapability::Persistent => "persistent",
        MetaPersistenceCapability::Volatile => "volatile",
        MetaPersistenceCapability::Unknown => "unknown",
    }
}

struct CheckResult {
    configured: bool,
    ready: bool,
    body: Value,
}

fn data_device_check(node: &Node) -> CheckResult {
    let observation = node.readiness.data_device_snapshot();
    let ready = observation.ready();
    CheckResult {
        configured: true,
        ready,
        body: json!({
            "configured": true,
            "ready": ready,
            "path": node.config.data_dir,
            "source": observation.source(),
            "observed": observation.observed(),
            "observed_age_ms": observation.age_ms(),
            "fresh_for_ms": 30_000,
            "stale": observation.stale(),
            "error": observation.error()
        }),
    }
}

fn mount_check(identity: Option<&AfsMountIdentity>, expected_source: &str) -> CheckResult {
    let Some(identity) = identity else {
        return CheckResult {
            configured: false,
            ready: true,
            body: json!({"configured":false,"ready":true}),
        };
    };
    let path = identity.path();
    let ready = proc_mountinfo_has_afs_mount(identity);
    CheckResult {
        configured: true,
        ready,
        body: json!({
            "configured": true,
            "ready": ready,
            "path": path,
            "startup_mount_id": identity.mount_id(),
            "startup_capture_error": identity.capture_error(),
            "expected_source": expected_source,
            "expected_fstype": "fuse",
            "source": "/proc/self/mountinfo",
            "error": if ready { Value::Null } else { json!("path is not an active AFS FUSE mount with the expected source") }
        }),
    }
}

fn proc_mountinfo_has_afs_mount(identity: &AfsMountIdentity) -> bool {
    let Some(startup_mount_id) = identity.mount_id() else {
        return false;
    };
    let Ok(mountinfo) = std::fs::read("/proc/self/mountinfo") else {
        return false;
    };
    current_visible_afs_mount_id_in_mountinfo(
        &mountinfo,
        identity.path(),
        identity.expected_source(),
    )
    .is_some_and(|mount_id| mount_id == startup_mount_id)
}

fn rdma_check(node: &Node) -> CheckResult {
    let required = node.readiness.rdma_required();
    let configured = node.readiness.rdma_configured();
    let observation = node.readiness.rdma_available_snapshot();
    let available = observation.ready();
    let ready = !required || available;
    CheckResult {
        configured,
        ready,
        body: json!({
            "required": required,
            "configured": configured,
            "ready": ready,
            "available": available,
            "device": node.readiness.rdma_device(),
            "source": observation.source(),
            "observed": observation.observed(),
            "observed_age_ms": observation.age_ms(),
            "fresh_for_ms": 30_000,
            "stale": observation.stale(),
            "error": observation.error()
        }),
    }
}
