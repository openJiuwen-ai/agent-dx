//! HIXL backend status rendered for the Node REST endpoints.

use super::{CheckResult, Node};
use afs_transport::hixl::{ReadTransport, RuntimeStatus};
use serde_json::json;

#[cfg(feature = "ownerfs")]
pub(super) fn snapshot_metrics(node: &Node) {
    if let (Some(metrics), Some(runtime)) = (&node.hixl_metrics, &node.hixl_runtime) {
        metrics.snapshot(&runtime.status());
    }
}

pub(super) fn health(node: &Node) -> CheckResult {
    #[cfg(feature = "ownerfs")]
    {
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
    }
    #[cfg(not(feature = "ownerfs"))]
    hixl_check(
        &node.config.hixl,
        node.config.data_mode,
        node.config.ownerfs,
        None,
        0,
        &[],
        &[],
    )
}

fn actual_mode(status: &RuntimeStatus) -> &'static str {
    match status.transport {
        ReadTransport::Roce if status.relay_hbm_budget_bytes != 0 => "roce_relay",
        ReadTransport::Roce => "roce_direct",
        ReadTransport::Hccs => "hccs_relay",
        ReadTransport::HccsFabric => "hccs_fabric",
    }
}

#[cfg(feature = "ownerfs")]
fn protocol_version(status: Option<&RuntimeStatus>) -> Option<u32> {
    status.map(|_| crate::node::rpc::hixl::HIXL_PROTOCOL_VERSION)
}

#[cfg(not(feature = "ownerfs"))]
fn protocol_version(_: Option<&RuntimeStatus>) -> Option<u32> {
    None
}

fn hixl_check(
    config: &crate::config::HixlConfig,
    data_mode: crate::config::DataMode,
    ownerfs: bool,
    status: Option<&RuntimeStatus>,
    lane_count: usize,
    infos: &[afs_transport::hixl::EngineInfo],
    lane_statuses: &[RuntimeStatus],
) -> CheckResult {
    use crate::config::DataMode;
    use afs_transport::hixl::RuntimeState;
    let configured = config.enabled(data_mode, ownerfs);
    let running = status.is_some_and(|status| status.state == RuntimeState::Running);
    let startup_fallback = configured && data_mode == DataMode::Auto && status.is_none();
    let ready = !configured || running || startup_fallback;
    let devices = config.device_configs();

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
                "actual_mode": lane_status.map(actual_mode),
                "fabric_mem": lane_status.is_some_and(|status| status.transport.is_fabric_mem()),
                "internal_protocol_version": protocol_version(lane_status),
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
            "transport": config.transport, "buffer_pool": config.buffer_pool,
            "relay_hbm_budget_bytes": status.map_or(0, |status| status.relay_hbm_budget_bytes),
            "hccs_domain_id": status.and_then(|status| status.hccs_domain_id),
            "lane_count": lane_count,
            "actual_mode": status.map(actual_mode),
            "fabric_mem": status.is_some_and(|status| status.transport.is_fabric_mem()),
            "internal_protocol_version": protocol_version(status),
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
            transport: ReadTransport::Hccs,
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
            transport: HixlTransport::Hccs,
            buffer_pool: true,
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
        assert_eq!(check.body["transport"], "hccs");
        assert_eq!(check.body["buffer_pool"], true);
        assert_eq!(check.body["relay_hbm_budget_bytes"], 64 * MIB);
        assert_eq!(check.body["allocated_bytes"], 32 * MIB);
        assert_eq!(check.body["hccs_domain_id"], 7);
    }

    #[test]
    fn health_reports_aggregate_runtime_status_and_each_configured_lane() {
        let config = HixlConfig {
            transport: HixlTransport::Hccs,
            buffer_pool: true,
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
                    transport: ReadTransport::Hccs,
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
        #[cfg(feature = "ownerfs")]
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
        assert_eq!(check.body["lanes"][1]["actual_mode"], "hccs_relay");
        assert_eq!(check.body["lanes"][1]["fabric_mem"], false);
        assert!(!check.body.to_string().contains("\"address\""));
        assert!(!check.body.to_string().contains("virtual_address"));
        assert!(!check.body.to_string().contains("token"));
    }

    #[cfg(feature = "ownerfs")]
    #[test]
    fn health_reports_fabric_from_the_runtime_even_when_requested_mode_differs() {
        let status = RuntimeStatus {
            transport: ReadTransport::HccsFabric,
            relay_hbm_budget_bytes: 0,
            ..hccs_status()
        };
        let config = HixlConfig {
            engine_id: "127.0.0.1:7920".into(),
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
        assert_eq!(check.body["transport"], "roce");
        assert_eq!(check.body["actual_mode"], "hccs_fabric");
        assert_eq!(check.body["internal_protocol_version"], 1);
        assert_eq!(check.body["lanes"][0]["fabric_mem"], true);
        assert_eq!(check.body["lanes"][0]["actual_mode"], "hccs_fabric");
        assert_eq!(check.body["lanes"][0]["internal_protocol_version"], 1);
        assert_eq!(check.body["lanes"][0]["relay_hbm_budget_bytes"], 0);
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
        assert!(check.body["actual_mode"].is_null());
        assert_eq!(check.body["fabric_mem"], false);
        assert_eq!(check.body["data_mode"], "grpc");
    }
}
