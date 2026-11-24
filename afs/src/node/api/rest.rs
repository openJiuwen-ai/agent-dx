//! Runtime-facing REST. Explicit diagnostics exercise real APIs, never run on POSIX hot paths.
//!
//! `/v1/diagnostics` 是显式演示入口：Node B→Meta Ping→A 控制 Ping→A 数据写/读
//! （探测流程在 `node::diagnostics::probe`）。它调用正式 caller/adapter，不是另写一套
//! 模拟传输。正常 FUSE 回调不会自动触发这些 RPC。
//! REST 面向 runtime 的 snapshot/publish API 尚未实现；不要用本诊断接口推断镜像发布成功。
mod hixl;

use crate::error::RestError;
use crate::node::{
    AfsMountIdentity, Node, current_visible_afs_mount_id_in_mountinfo,
    rpc::meta::MetaPersistenceCapability,
};
use afs_error::{Error, METRICS_FAILED};
use afs_tracing::{Instrument, tracing};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use serde_json::{Value, json};
use std::sync::Arc;

pub fn router(node: Arc<Node>) -> Router {
    let router = Router::new()
        .route("/health", get(health))
        .route("/v1/ping", get(ping))
        .route("/v1/diagnostics", post(diagnostics))
        .route("/metrics", get(metrics));
    #[cfg(feature = "dfs")]
    let router = if node.config.dfs_read_diagnostics_enabled {
        router.route("/v1/diagnostics/dfs-read-timing", get(read_timing))
    } else {
        router
    };
    router.with_state(node)
}
/// Read-only fixed-capacity journal. Serialization runs outside measured reads.
#[cfg(feature = "dfs")]
async fn read_timing(State(node): State<Arc<Node>>) -> Json<Value> {
    Json(json!({
        "clock": "CLOCK_MONOTONIC",
        "scope": "coarse client intervals; overlap is not additive",
        "snapshot": node.read_work_metrics.diagnostics_snapshot(),
    }))
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
async fn metrics(
    State(node): State<Arc<Node>>,
    #[cfg(all(feature = "dfs", feature = "rdma"))] sessions: Option<
        axum::Extension<crate::node::rpc::control::RdmaSessionRegistry>,
    >,
) -> Result<String, RestError> {
    #[cfg(feature = "ownerfs")]
    hixl::snapshot_metrics(&node);
    let encoded = afs_metrics::encode_text(&node.observability.registry)
        .map_err(|e| RestError(Error::coded(METRICS_FAILED, e.to_string())))?;
    #[cfg(feature = "rdma")]
    {
        let stats = afs_transport::rdma::resource_stats();
        #[cfg(feature = "dfs")]
        let dfs = Some((
            match sessions {
                Some(axum::Extension(sessions)) => sessions.dfs_unbound_sessions().await,
                None => 0,
            },
            crate::node::rpc::peer::DfsRdmaPool::unbound_server_replies(),
        ));
        #[cfg(not(feature = "dfs"))]
        let dfs = None;
        Ok(append_rdma_metrics(encoded, stats, dfs))
    }
    #[cfg(not(feature = "rdma"))]
    Ok(encoded)
}

#[cfg(feature = "rdma")]
fn append_rdma_metrics(
    encoded: String,
    stats: afs_transport::rdma::RdmaResourceStats,
    dfs: Option<(usize, u64)>,
) -> String {
    let Some(body) = encoded.strip_suffix("# EOF\n") else {
        afs_logging::warn!(
            "RDMA metrics omitted because the registry snapshot lacks its EOF marker"
        );
        return encoded;
    };
    let dfs = dfs.map_or_else(String::new, |(sessions, replies)| {
        format!(
            "# TYPE afs_dfs_rdma_unbound_sessions gauge\nafs_dfs_rdma_unbound_sessions {sessions}\n\
             # TYPE afs_dfs_rdma_unbound_server_replies counter\n\
             afs_dfs_rdma_unbound_server_replies_total {replies}\n"
        )
    });
    format!(
        "{body}# TYPE afs_rdma_endpoint_opens counter\nafs_rdma_endpoint_opens_total {}\n\
         # TYPE afs_rdma_registered_bytes gauge\nafs_rdma_registered_bytes {}\n\
         # TYPE afs_rdma_failed_retirements counter\nafs_rdma_failed_retirements_total {}\n\
         # TYPE afs_rdma_retained_endpoints gauge\nafs_rdma_retained_endpoints {}\n\
         # TYPE afs_rdma_retained_registered_bytes gauge\nafs_rdma_retained_registered_bytes {}\n\
         {dfs}# EOF\n",
        stats.endpoint_opens,
        stats.registered_bytes,
        stats.failed_retirements,
        stats.retained_endpoints,
        stats.retained_registered_bytes
    )
}

#[cfg(all(test, feature = "rdma"))]
#[test]
fn rdma_openmetrics_retention_and_failure_samples_precede_one_eof() {
    let encoded = afs_metrics::encode_text(&afs_metrics::registry()).unwrap();
    let snapshot = append_rdma_metrics(
        encoded,
        afs_transport::rdma::RdmaResourceStats {
            failed_retirements: 3,
            retained_endpoints: 2,
            retained_registered_bytes: 4096,
            ..Default::default()
        },
        Some((1, 5)),
    );
    assert_eq!(snapshot.matches("# EOF").count(), 1);
    assert_eq!(snapshot.lines().last(), Some("# EOF"));
    assert!(snapshot.ends_with("# EOF\n"));
    for sample in [
        "# TYPE afs_rdma_failed_retirements counter\nafs_rdma_failed_retirements_total 3\n",
        "# TYPE afs_rdma_retained_endpoints gauge\nafs_rdma_retained_endpoints 2\n",
        "# TYPE afs_rdma_retained_registered_bytes gauge\nafs_rdma_retained_registered_bytes 4096\n",
        "# TYPE afs_dfs_rdma_unbound_sessions gauge\nafs_dfs_rdma_unbound_sessions 1\n",
        "# TYPE afs_dfs_rdma_unbound_server_replies counter\nafs_dfs_rdma_unbound_server_replies_total 5\n",
    ] {
        assert!(snapshot.contains(sample));
    }
}
async fn diagnostics(State(node): State<Arc<Node>>) -> Result<Json<Value>, RestError> {
    let result = crate::node::diagnostics::probe::diagnose(node.clone())
        .instrument(tracing::info_span!("node.diagnostics"))
        .await;
    node.observability
        .record("node", "diagnostics", result.is_ok());
    result.map(Json).map_err(|e| {
        afs_logging::error!("node.diagnostics failed";"error"=>e.to_string());
        RestError(e)
    })
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
    let hixl = hixl::health(node);
    let ready = meta_functional_ready
        && registration_ready
        && data_device.ready
        && mounts_ready
        && rdma.ready
        && hixl.ready;
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
                "rdma": rdma.body,
                "hixl": hixl.body
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
