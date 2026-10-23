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
async fn metrics(State(node): State<Arc<Node>>) -> Result<String, RestError> {
    #[cfg(feature = "dfs")]
    if let (Some(metrics), Some(runtime)) = (&node.hixl_metrics, &node.hixl_runtime) {
        metrics.snapshot(&runtime.status());
    }
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
    let tls = config.tls_config();
    let meta_pong = meta::ping(meta_endpoint, &config.id, timeout, tls.clone()).await?;
    let grpc_config = afs_transport::grpc::GrpcConfig {
        connect_timeout: timeout,
        request_timeout: timeout,
        ..Default::default()
    };
    let security = afs_transport::grpc::SecurityManager::new(tls.clone())
        .map_err(|e| Error::coded(CLIENT_ARGUMENT_INVALID, e.to_string()))?;
    let endpoint = grpc_config.configure_client(
        tonic::transport::Endpoint::from_shared(security.endpoint_uri(peer_endpoint))
            .map_err(|e| Error::coded(CLIENT_ARGUMENT_INVALID, e.to_string()))?,
    );
    let channel = security
        .connect(endpoint)
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
        tls,
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
    #[cfg(feature = "dfs")]
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
            status.as_ref(),
            lane_count,
            &infos,
            &lane_statuses,
        )
    };
    #[cfg(not(feature = "dfs"))]
    let hixl = hixl_check(&node.config.hixl, None, 0, &[], &[]);
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
    status: Option<&afs_transport::hixl::RuntimeStatus>,
    lane_count: usize,
    infos: &[afs_transport::hixl::EngineInfo],
    lane_statuses: &[afs_transport::hixl::RuntimeStatus],
) -> CheckResult {
    use crate::config::HixlDfsReadMode;
    use afs_transport::hixl::RuntimeState;
    let mode = config.dfs_read_mode;
    let configured = mode != HixlDfsReadMode::Off;
    let ready = match mode {
        HixlDfsReadMode::Off => true,
        HixlDfsReadMode::Preferred if status.is_none() => true,
        _ => status.is_some_and(|status| status.state == RuntimeState::Running),
    };
    let mode_label = match mode {
        HixlDfsReadMode::Off => "off",
        HixlDfsReadMode::Preferred => "preferred",
        HixlDfsReadMode::Required => "required",
    };
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
                "index": index,
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
            "configured": configured, "ready": ready, "mode": mode_label,
            "transport": config.transport,
            "relay_hbm_budget_bytes": status.map_or(0, |status| status.relay_hbm_budget_bytes),
            "hccs_domain_id": status.and_then(|status| status.hccs_domain_id),
            "lane_count": lane_count,
            "internal_protocol_version": protocol_version,
            "lanes": lanes,
            "state": status
                .map(|status| status.state.to_string())
                .unwrap_or_else(|| if configured {"unsupported"} else {"off"}.into()),
            "fallback": if mode == HixlDfsReadMode::Preferred && status.is_none() {
                Some("grpc")
            } else {
                None
            },
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
    use crate::config::{HixlConfig, HixlDeviceConfig, HixlDfsReadMode, HixlTransport};
    use afs_transport::hixl::{EngineInfo, RuntimeState, RuntimeStatus};

    #[test]
    fn quarantined_generation_is_degraded_even_in_preferred_mode() {
        let status = RuntimeStatus {
            transport: afs_transport::hixl::ReadTransport::Roce,
            relay_hbm_budget_bytes: 0,
            hccs_domain_id: None,
            state: RuntimeState::Quarantined,
            generation: 7,
            allocated_bytes: 128,
            quarantined_bytes: 4,
            receive_slots_in_use: 1,
            export_slots_in_use: 0,
            export_slots: 16,
        };
        let config = HixlConfig {
            dfs_read_mode: HixlDfsReadMode::Preferred,
            ..Default::default()
        };
        let check = hixl_check(
            &config,
            Some(&status),
            1,
            &[],
            std::slice::from_ref(&status),
        );
        assert!(!check.ready);
        assert_eq!(check.body["quarantined_bytes"], 4);
        assert!(check.body["fallback"].is_null());
        assert!(!check.body.to_string().contains("address"));
    }

    #[test]
    fn hccs_health_reports_transport_and_separate_hbm_budget() {
        let status = RuntimeStatus {
            state: RuntimeState::Running,
            generation: 11,
            allocated_bytes: 32 * 1024 * 1024,
            quarantined_bytes: 0,
            export_slots: 4,
            receive_slots_in_use: 0,
            export_slots_in_use: 0,
            transport: afs_transport::hixl::ReadTransport::HccsBufferPool,
            relay_hbm_budget_bytes: 64 * 1024 * 1024,
            hccs_domain_id: Some(7),
        };
        let config = HixlConfig {
            dfs_read_mode: HixlDfsReadMode::Required,
            transport: HixlTransport::HccsBufferPool,
            ..Default::default()
        };
        let check = hixl_check(
            &config,
            Some(&status),
            1,
            &[],
            std::slice::from_ref(&status),
        );
        assert!(check.ready);
        assert_eq!(check.body["transport"], "hccs-buffer-pool");
        assert_eq!(check.body["relay_hbm_budget_bytes"], 64 * 1024 * 1024);
        assert_eq!(check.body["allocated_bytes"], 32 * 1024 * 1024);
        assert_eq!(check.body["hccs_domain_id"], 7);
    }

    #[test]
    fn health_reports_aggregate_runtime_status_and_each_configured_lane() {
        let config = HixlConfig {
            dfs_read_mode: HixlDfsReadMode::Required,
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
                state: RuntimeState::Running,
                generation: 10,
                allocated_bytes: 24 * 1024 * 1024,
                quarantined_bytes: 0,
                export_slots: 16,
                receive_slots_in_use: 1,
                export_slots_in_use: 1,
                transport: afs_transport::hixl::ReadTransport::HccsBufferPool,
                relay_hbm_budget_bytes: 64 * 1024 * 1024,
                hccs_domain_id: Some(7),
            },
            RuntimeStatus {
                state: RuntimeState::Running,
                generation: 11,
                allocated_bytes: 32 * 1024 * 1024,
                quarantined_bytes: 4,
                export_slots: 16,
                receive_slots_in_use: 2,
                export_slots_in_use: 1,
                transport: afs_transport::hixl::ReadTransport::HccsBufferPool,
                relay_hbm_budget_bytes: 64 * 1024 * 1024,
                hccs_domain_id: Some(7),
            },
        ];
        let aggregate = RuntimeStatus {
            state: RuntimeState::Running,
            generation: 10,
            allocated_bytes: 56 * 1024 * 1024,
            quarantined_bytes: 4,
            export_slots: 32,
            receive_slots_in_use: 3,
            export_slots_in_use: 2,
            transport: afs_transport::hixl::ReadTransport::HccsBufferPool,
            relay_hbm_budget_bytes: 128 * 1024 * 1024,
            hccs_domain_id: Some(7),
        };
        let infos = vec![
            EngineInfo {
                engine_id: "127.0.0.1:7920".into(),
                generation: 10,
                window_bytes: 4 * 1024 * 1024,
                transport: afs_transport::hixl::ReadTransport::HccsBufferPool,
                buffer_pool_count: 4,
                buffer_pool_size_mib: 8,
                hccs_domain_id: Some(7),
            },
            EngineInfo {
                engine_id: "127.0.0.1:7921".into(),
                generation: 11,
                window_bytes: 4 * 1024 * 1024,
                transport: afs_transport::hixl::ReadTransport::HccsBufferPool,
                buffer_pool_count: 4,
                buffer_pool_size_mib: 8,
                hccs_domain_id: Some(7),
            },
        ];

        let check = hixl_check(&config, Some(&aggregate), 2, &infos, &lane_statuses);

        assert!(check.ready);
        assert_eq!(check.body["lane_count"], 2);
        #[cfg(feature = "dfs")]
        assert_eq!(check.body["internal_protocol_version"], 5);
        assert_eq!(check.body["relay_hbm_budget_bytes"], 128 * 1024 * 1024);
        assert_eq!(check.body["receive_slots_in_use"], 3);
        assert_eq!(check.body["export_slots_in_use"], 2);
        assert_eq!(check.body["allocated_bytes"], 56 * 1024 * 1024);
        assert_eq!(check.body["quarantined_bytes"], 4);
        assert_eq!(check.body["lanes"].as_array().unwrap().len(), 2);
        assert_eq!(check.body["lanes"][0]["index"], 0);
        assert_eq!(check.body["lanes"][0]["lane_id"], 0);
        assert_eq!(check.body["lanes"][0]["device_id"], 2);
        assert_eq!(check.body["lanes"][0]["device_listen_port"], 17920);
        assert_eq!(check.body["lanes"][0]["engine_id"], "127.0.0.1:7920");
        assert_eq!(check.body["lanes"][1]["device_id"], 5);
        assert_eq!(check.body["lanes"][1]["device_listen_port"], 17921);
        assert_eq!(check.body["lanes"][1]["engine_id"], "127.0.0.1:7921");
        assert_eq!(check.body["lanes"][1]["engine_generation"], 11);
        assert_eq!(check.body["lanes"][1]["allocated_bytes"], 32 * 1024 * 1024);
        assert_eq!(check.body["lanes"][1]["hccs_domain_id"], 7);
        assert!(!check.body.to_string().contains("\"address\""));
        assert!(!check.body.to_string().contains("virtual_address"));
        assert!(!check.body.to_string().contains("token"));
    }

    #[test]
    fn off_health_has_zero_runtime_lanes_even_with_default_device_config() {
        let check = hixl_check(&HixlConfig::default(), None, 0, &[], &[]);

        assert!(check.ready);
        assert_eq!(check.body["lane_count"], 0);
        assert!(check.body["internal_protocol_version"].is_null());
        assert_eq!(check.body["lanes"].as_array().unwrap().len(), 0);
    }
}
