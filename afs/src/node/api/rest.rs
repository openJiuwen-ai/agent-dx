//! Runtime-facing REST. Explicit diagnostics exercise real APIs, never run on POSIX hot paths.
//!
//! `/v1/diagnostics` 是显式演示入口：Node B→Meta Ping→A 控制 Ping→A 数据写/读
//! （探测流程在 `node::diagnostics::probe`）。它调用正式 caller/adapter，不是另写一套
//! 模拟传输。正常 FUSE 回调不会自动触发这些 RPC。
//! REST 面向 runtime 的 snapshot/publish API 尚未实现；不要用本诊断接口推断镜像发布成功。
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
    #[cfg(all(feature = "ownerfs", feature = "dfs"))]
    if let (Some(metrics), Some(runtime)) = (&node.hixl_metrics, &node.hixl_runtime) {
        metrics.snapshot(&runtime.status());
    }
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
    #[cfg(all(feature = "ownerfs", feature = "dfs"))]
    let hixl = {
        let runtime = node.hixl_runtime.as_ref();
        let status = runtime.map(|runtime| runtime.status());
        let infos = runtime.map(|runtime| runtime.infos()).unwrap_or_default();
        let lane_statuses = runtime
            .map(|runtime| runtime.lane_statuses())
            .unwrap_or_default();
        let lane_count = runtime.map_or(0, |runtime| runtime.len());
        hixl_check(
            &node.config.hixl,
            node.config.data_mode,
            node.config.ownerfs,
            status.as_ref(),
            lane_count,
            &infos,
            &lane_statuses,
        )
    };
    #[cfg(not(all(feature = "ownerfs", feature = "dfs")))]
    let hixl = hixl_check(
        &node.config.hixl,
        node.config.data_mode,
        node.config.ownerfs,
        None,
        0,
        &[],
        &[],
    );
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

fn hixl_check(
    config: &crate::config::HixlConfig,
    data_mode: crate::config::DataMode,
    ownerfs: bool,
    status: Option<&afs_transport::hixl::RuntimeStatus>,
    lane_count: usize,
    infos: &[afs_transport::hixl::EngineInfo],
    lane_statuses: &[afs_transport::hixl::RuntimeStatus],
) -> CheckResult {
    use crate::config::DataMode;
    use afs_transport::hixl::RuntimeState;
    let configured = config.enabled(data_mode, ownerfs);
    let running = status.is_some_and(|status| status.state == RuntimeState::Running);
    let startup_fallback = configured && data_mode == DataMode::Auto && status.is_none();
    let ready = !configured || running || startup_fallback;
    let devices = config.device_configs();
    #[cfg(feature = "dfs")]
    let protocol_version = status.map(|_| crate::node::rpc::hixl::HIXL_PROTOCOL_VERSION);
    #[cfg(not(feature = "dfs"))]
    let protocol_version = Option::<u32>::None;
    let lanes = (0..lane_count)
        .map(|index| {
            let device = devices.get(index);
            let info = infos.get(index);
            let lane_status = lane_statuses.get(index);
            let state = lane_status
                .map(|status| status.state.to_string())
                .unwrap_or_else(|| "unknown".into());
            let engine_id = info
                .map(|info| info.engine_id.as_str())
                .or_else(|| device.map(|device| device.engine_id.as_str()));
            let hccs_domain_id = lane_status
                .and_then(|status| status.hccs_domain_id)
                .or_else(|| info.and_then(|info| info.hccs_domain_id));
            json!({
                "lane_id": index,
                "device_id": device.map(|device| device.device_id),
                "device_listen_port": device.and_then(|device| device.device_listen_port),
                "engine_id": engine_id,
                "engine_generation": info.map(|info| info.generation),
                "state": state,
                "allocated_bytes": lane_status.map_or(0, |status| status.allocated_bytes),
                "relay_hbm_budget_bytes": lane_status
                    .map_or(0, |status| status.relay_hbm_budget_bytes),
                "quarantined_bytes": lane_status.map_or(0, |status| status.quarantined_bytes),
                "receive_slots_in_use": lane_status.map_or(0, |status| status.receive_slots_in_use),
                "export_slots_in_use": lane_status.map_or(0, |status| status.export_slots_in_use),
                "hccs_domain_id": hccs_domain_id
            })
        })
        .collect::<Vec<_>>();
    CheckResult {
        configured,
        ready,
        body: json!({
            "configured": configured, "ready": ready, "data_mode": data_mode,
            "transport": config.transport,
            "relay_hbm_budget_bytes": status.map_or(0, |status| status.relay_hbm_budget_bytes),
            "hccs_domain_id": status.and_then(|status| status.hccs_domain_id),
            "lane_count": lane_count,
            "internal_protocol_version": protocol_version,
            "lanes": lanes,
            "state": status
                .map(|status| status.state.to_string())
                .unwrap_or_else(|| if configured {"unsupported"} else {"off"}.into()),
            "startup_fallback": startup_fallback,
            "engine_generation": status.map(|status| status.generation),
            "allocated_bytes": status.map_or(0, |status| status.allocated_bytes),
            "quarantined_bytes": status.map_or(0, |status| status.quarantined_bytes),
            "receive_slots_in_use": status.map_or(0, |status| status.receive_slots_in_use),
            "export_slots_in_use": status.map_or(0, |status| status.export_slots_in_use)
        }),
    }
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

#[cfg(test)]
mod hixl_health_tests {
    use super::hixl_check;
    use crate::config::{DataMode, HixlConfig, HixlDeviceConfig, HixlTransport};
    use afs_transport::hixl::{EngineInfo, ReadTransport, RuntimeState, RuntimeStatus};

    const MIB: usize = 1024 * 1024;

    /// An idle running HCCS lane with a 64 MiB relay budget in domain 7.
    fn hccs_status() -> RuntimeStatus {
        RuntimeStatus {
            state: RuntimeState::Running,
            generation: 0,
            allocated_bytes: 0,
            quarantined_bytes: 0,
            export_slots: 16,
            receive_slots_in_use: 0,
            export_slots_in_use: 0,
            transport: ReadTransport::HccsBufferPool,
            relay_hbm_budget_bytes: 64 * MIB,
            hccs_domain_id: Some(7),
        }
    }

    #[test]
    fn quarantined_generation_is_degraded_in_auto_mode() {
        let status = RuntimeStatus {
            state: RuntimeState::Quarantined,
            generation: 7,
            allocated_bytes: 128,
            quarantined_bytes: 4,
            receive_slots_in_use: 1,
            transport: ReadTransport::Roce,
            relay_hbm_budget_bytes: 0,
            hccs_domain_id: None,
            ..hccs_status()
        };
        let config = HixlConfig {
            engine_id: "127.0.0.1:7920".into(),
            ..Default::default()
        };
        let check = hixl_check(
            &config,
            DataMode::Auto,
            true,
            Some(&status),
            1,
            &[],
            std::slice::from_ref(&status),
        );
        assert!(!check.ready);
        assert_eq!(check.body["quarantined_bytes"], 4);
        assert_eq!(check.body["startup_fallback"], false);
        assert!(!check.body.to_string().contains("address"));
    }

    #[test]
    fn hccs_health_reports_transport_and_separate_hbm_budget() {
        let status = RuntimeStatus {
            generation: 11,
            allocated_bytes: 32 * MIB,
            export_slots: 4,
            ..hccs_status()
        };
        let config = HixlConfig {
            transport: HixlTransport::HccsBufferPool,
            ..Default::default()
        };
        let check = hixl_check(
            &config,
            DataMode::Hixl,
            true,
            Some(&status),
            1,
            &[],
            std::slice::from_ref(&status),
        );
        assert!(check.ready);
        assert_eq!(check.body["transport"], "hccs-buffer-pool");
        assert_eq!(check.body["relay_hbm_budget_bytes"], 64 * MIB);
        assert_eq!(check.body["allocated_bytes"], 32 * MIB);
        assert_eq!(check.body["hccs_domain_id"], 7);
    }

    #[test]
    fn health_reports_aggregate_runtime_status_and_each_configured_lane() {
        let config = HixlConfig {
            transport: HixlTransport::HccsBufferPool,
            device_id: 2,
            engine_id: "127.0.0.1:7920".into(),
            device_listen_port: Some(17920),
            additional_devices: vec![HixlDeviceConfig {
                device_id: 5,
                engine_id: "127.0.0.1:7921".into(),
                device_listen_port: Some(17921),
            }],
            ..Default::default()
        };
        let lane_statuses = vec![
            RuntimeStatus {
                generation: 10,
                allocated_bytes: 24 * MIB,
                receive_slots_in_use: 1,
                export_slots_in_use: 1,
                ..hccs_status()
            },
            RuntimeStatus {
                generation: 11,
                allocated_bytes: 32 * MIB,
                quarantined_bytes: 4,
                receive_slots_in_use: 2,
                export_slots_in_use: 1,
                ..hccs_status()
            },
        ];
        let aggregate = RuntimeStatus {
            generation: 10,
            allocated_bytes: 56 * MIB,
            quarantined_bytes: 4,
            export_slots: 32,
            receive_slots_in_use: 3,
            export_slots_in_use: 2,
            relay_hbm_budget_bytes: 128 * MIB,
            ..hccs_status()
        };
        let infos =
            [("127.0.0.1:7920", 10), ("127.0.0.1:7921", 11)].map(|(engine_id, generation)| {
                EngineInfo {
                    engine_id: engine_id.into(),
                    generation,
                    window_bytes: 4 * MIB,
                    transport: ReadTransport::HccsBufferPool,
                    buffer_pool_count: 4,
                    buffer_pool_size_mib: 8,
                    hccs_domain_id: Some(7),
                }
            });

        let check = hixl_check(
            &config,
            DataMode::Hixl,
            true,
            Some(&aggregate),
            2,
            &infos,
            &lane_statuses,
        );

        assert!(check.ready);
        assert_eq!(check.body["lane_count"], 2);
        #[cfg(feature = "dfs")]
        assert_eq!(check.body["internal_protocol_version"], 1);
        assert_eq!(check.body["relay_hbm_budget_bytes"], 128 * MIB);
        assert_eq!(check.body["receive_slots_in_use"], 3);
        assert_eq!(check.body["export_slots_in_use"], 2);
        assert_eq!(check.body["allocated_bytes"], 56 * MIB);
        assert_eq!(check.body["quarantined_bytes"], 4);
        assert_eq!(check.body["lanes"].as_array().unwrap().len(), 2);
        assert_eq!(check.body["lanes"][0]["lane_id"], 0);
        assert_eq!(check.body["lanes"][0]["device_id"], 2);
        assert_eq!(check.body["lanes"][0]["device_listen_port"], 17920);
        assert_eq!(check.body["lanes"][0]["engine_id"], "127.0.0.1:7920");
        assert_eq!(check.body["lanes"][1]["device_id"], 5);
        assert_eq!(check.body["lanes"][1]["device_listen_port"], 17921);
        assert_eq!(check.body["lanes"][1]["engine_id"], "127.0.0.1:7921");
        assert_eq!(check.body["lanes"][1]["engine_generation"], 11);
        assert_eq!(check.body["lanes"][1]["allocated_bytes"], 32 * MIB);
        assert_eq!(check.body["lanes"][1]["hccs_domain_id"], 7);
        assert!(!check.body.to_string().contains("\"address\""));
        assert!(!check.body.to_string().contains("virtual_address"));
        assert!(!check.body.to_string().contains("token"));
    }

    #[test]
    fn off_health_has_zero_runtime_lanes_even_with_default_device_config() {
        let check = hixl_check(
            &HixlConfig::default(),
            DataMode::Grpc,
            true,
            None,
            0,
            &[],
            &[],
        );

        assert!(check.ready);
        assert_eq!(check.body["lane_count"], 0);
        assert!(check.body["internal_protocol_version"].is_null());
        assert_eq!(check.body["lanes"].as_array().unwrap().len(), 0);
        assert_eq!(check.body["data_mode"], "grpc");
    }
}
