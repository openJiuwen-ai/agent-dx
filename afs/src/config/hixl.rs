//! `[hixl]`: the HIXL transport for buffered OwnerFs remote file operations.
//!
//! The section is parsed with the rest of the file and validated once every
//! other setting is merged, because its checks need the role, the enabled
//! filesystems, the data mode and the TLS material.

use super::{DataMode, MAX_OWNERFS_HIXL_READ_GROUP_BYTES, Role, invalid};
use crate::node::storage::MAX_TRANSFER_BYTES;
use afs_error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::net::SocketAddr;

/// How the peer-side HIXL runtime moves bytes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HixlTransport {
    #[default]
    Roce,
    Hccs,
}

/// One HIXL lane's device identity and administrator-managed endpoints.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HixlDeviceConfig {
    pub device_id: i32,
    pub engine_id: String,
    pub device_listen_port: Option<u16>,
}

/// Fixed Host-memory pools and the administrator-managed HIXL device set.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct HixlConfig {
    pub transport: HixlTransport,
    pub device_id: i32,
    pub engine_id: String,
    pub device_listen_port: Option<u16>,
    pub additional_devices: Vec<HixlDeviceConfig>,
    pub window_bytes: usize,
    pub receive_slots: usize,
    pub export_slots: usize,
    /// Idle peer control-session reuse limit, 1..=20000 ms. Sources close a
    /// session after 30 s without activity, so a reader never reuses one past
    /// 20 s.
    pub session_idle_ms: u64,
    /// Relay through device memory instead of registering Host slots directly.
    pub buffer_pool: bool,
    pub buffer_pool_count: usize,
    pub buffer_pool_size_mib: usize,
    /// Target upper bound for one OwnerFs read-ahead window. The negotiated
    /// capacity is `min(local window_bytes, Home window_bytes) × shared slots`,
    /// so a smaller Home window shortens prepares below this value. Defaults to
    /// 32 MiB, eight 4 MiB windows, so the next prepare overlaps the current
    /// one; the comparison is recorded in the deployment guide.
    pub ownerfs_read_group_bytes: usize,
    /// 1 fetches windows on demand; 2 also prefetches the next window. Defaults
    /// to 2 so the next prepare overlaps the consumption of the current window;
    /// the comparison is recorded in the deployment guide.
    pub ownerfs_read_group_max_inflight: usize,
    /// Process budget for cached and prefetching OwnerFs windows and resident
    /// directory batches. Defaults to 1 GiB, which holds sixteen 32 MiB
    /// batches, so several directories can be read concurrently.
    pub ownerfs_read_group_host_budget_bytes: u64,
    /// Auto only enables windows for files at least this large at open.
    pub ownerfs_min_file_bytes: u64,
    /// Minimum size of one buffered write for Auto to try HIXL (inclusive).
    pub ownerfs_min_write_bytes: usize,
    /// Allow buffered HIXL writes on both ingress and Home.
    pub ownerfs_buffered_writes: bool,
    /// Enable cross-request caching only for immutable, long-lived read grants.
    pub ownerfs_snapshot_reads: bool,
    /// Nodes explicitly trusted to share this runtime's registered HIXL slots.
    pub trusted_peers: Vec<String>,
    /// Reader control sessions per Home; each admits one window fetch at a
    /// time. Defaults to 2 so two opens of one Home prepare and transfer at
    /// once; the measurements are in the deployment guide.
    pub ownerfs_sessions_per_home: usize,
    /// Serves read-only opens of small files from directory batches: one Home
    /// prepare packs a run of files of one directory into the export slots.
    pub ownerfs_batch_read: bool,
    /// Largest file a directory batch packs, 4097..=4 MiB; above
    /// `window_bytes` the slot size applies, since files never span slots.
    pub ownerfs_batch_max_file_bytes: u64,
}

/// One read-ahead group: 32 slots of the largest window, enough to cover the
/// bandwidth-delay product of a faster future link at the default prefetch
/// depth. The negotiated capacity and the Host slot budget still bound every
/// deployment.
const MAX_OWNERFS_HIXL_READ_GROUP_HOST_BUDGET_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MAX_OWNERFS_HIXL_MIN_FILE_BYTES: u64 = 1024 * 1024 * 1024 * 1024;
const MAX_OWNERFS_HIXL_SESSIONS_PER_HOME: usize = 4;
/// One Host slot pool bound; both pools of every device lane together may
/// reserve 1 GiB, so a node that serves and reads sizes each pool from the
/// same budget.
pub(crate) const MAX_OWNERFS_HIXL_SLOTS_PER_POOL: usize = 128;
const MAX_OWNERFS_HIXL_HOST_SLOT_BYTES: usize = 1024 * 1024 * 1024;
/// Files up to the OwnerFs OPEN prefetch limit arrive inside the OPEN reply.
const MIN_OWNERFS_HIXL_BATCH_FILE_BYTES: u64 = 4096 + 1;
const MAX_OWNERFS_HIXL_BATCH_FILE_BYTES: u64 = 4 * 1024 * 1024;

impl Default for HixlConfig {
    fn default() -> Self {
        Self {
            transport: HixlTransport::Roce,
            device_id: 0,
            engine_id: String::new(),
            device_listen_port: None,
            additional_devices: Vec::new(),
            window_bytes: 4 * 1024 * 1024,
            receive_slots: 16,
            export_slots: 16,
            session_idle_ms: 20_000,
            buffer_pool: false,
            buffer_pool_count: 4,
            buffer_pool_size_mib: 8,
            ownerfs_read_group_bytes: 32 * 1024 * 1024,
            ownerfs_read_group_max_inflight: 2,
            ownerfs_read_group_host_budget_bytes: 1024 * 1024 * 1024,
            ownerfs_min_file_bytes: 1024 * 1024,
            ownerfs_min_write_bytes: 512 * 1024,
            ownerfs_buffered_writes: false,
            ownerfs_snapshot_reads: false,
            trusted_peers: Vec::new(),
            ownerfs_sessions_per_home: 2,
            ownerfs_batch_read: true,
            ownerfs_batch_max_file_bytes: MAX_OWNERFS_HIXL_BATCH_FILE_BYTES,
        }
    }
}

/// Deployment facts that decide whether a Node may enable HIXL.
#[derive(Clone, Copy)]
struct HixlDeployment {
    ownerfs: bool,
    dfs: bool,
    data_mode: DataMode,
    mutual_tls: bool,
    bound_peers: bool,
}

impl HixlConfig {
    pub(crate) fn enabled(&self, mode: DataMode, ownerfs: bool) -> bool {
        mode == DataMode::Hixl || (mode == DataMode::Auto && ownerfs && !self.engine_id.is_empty())
    }

    fn validate(&self, role: Role, deployment: HixlDeployment) -> Result<()> {
        let HixlDeployment {
            ownerfs,
            dfs,
            data_mode,
            mutual_tls,
            bound_peers,
        } = deployment;
        if self.transport == HixlTransport::Hccs && !self.buffer_pool {
            return Err(invalid("HIXL transport hccs requires buffer_pool = true"));
        }
        if self.ownerfs_min_write_bytes > MAX_TRANSFER_BYTES {
            return Err(invalid(format!(
                "hixl.ownerfs_min_write_bytes must be in 0..={MAX_TRANSFER_BYTES}"
            )));
        }
        let lanes = self.additional_devices.len() + 1;
        let pool_bytes = self
            .receive_slots
            .checked_add(self.export_slots)
            .and_then(|slots| slots.checked_mul(self.window_bytes))
            .and_then(|bytes| bytes.checked_mul(lanes));
        let buffer_pool_bytes = self
            .buffer_pool_count
            .checked_mul(self.buffer_pool_size_mib)
            .and_then(|mib| mib.checked_mul(2))
            .and_then(|pools| pools.checked_mul(1024 * 1024));
        if self.window_bytes == 0
            || self.window_bytes > 4 * 1024 * 1024
            || self.receive_slots == 0
            || self.export_slots == 0
            || self.receive_slots > MAX_OWNERFS_HIXL_SLOTS_PER_POOL
            || self.export_slots > MAX_OWNERFS_HIXL_SLOTS_PER_POOL
            || !(1..=20_000).contains(&self.session_idle_ms)
            || pool_bytes.is_none_or(|bytes| bytes > MAX_OWNERFS_HIXL_HOST_SLOT_BYTES)
            || self.buffer_pool_count == 0
            || self.buffer_pool_count > 64
            || self.buffer_pool_size_mib == 0
            || self.buffer_pool_size_mib > 64
            || buffer_pool_bytes.is_none_or(|bytes| bytes > 512 * 1024 * 1024)
        {
            return Err(invalid(
                "HIXL requires 1..4 MiB windows, 1..128 Host slots per pool, <=1 GiB Host slot memory across all device lanes, 1..64 HBM buffer-pool count and size, <=512 MiB HBM budget, and session_idle_ms in 1..=20000",
            ));
        }
        let enabled = self.enabled(data_mode, ownerfs);
        self.validate_device_set(enabled)?;
        if !enabled {
            return Ok(());
        }
        if role != Role::Node || !ownerfs {
            return Err(invalid("data_mode = hixl requires Node OwnerFs"));
        }
        if data_mode == DataMode::Hixl && dfs {
            return Err(invalid("data_mode = hixl is unsupported for DFS"));
        }
        if data_mode == DataMode::Hixl && !cfg!(feature = "ownerfs") {
            return Err(invalid(
                "HIXL requires an OwnerFs build",
            ));
        }
        self.validate_ownerfs()?;
        if !mutual_tls || !bound_peers {
            return Err(invalid(
                "HIXL requires mTLS and administrator-managed trusted_node_certs",
            ));
        }
        if data_mode == DataMode::Hixl && !cfg!(feature = "hixl") {
            return Err(invalid("required HIXL feature is not compiled in"));
        }
        Ok(())
    }

    /// OwnerFs windows are bounded by both local slot pools: one prepare
    /// exports at most one window per remote export slot, and one native batch
    /// READ occupies one receive slot per window. Pooled sessions of one Home
    /// can each hold a window at once, so they split the export pool.
    fn validate_ownerfs(&self) -> Result<()> {
        let sessions = self.ownerfs_sessions_per_home;
        let slot_capacity = self
            .receive_slots
            .min(self.export_slots / sessions.max(1))
            .checked_mul(self.window_bytes);
        let inflight_bytes = u64::try_from(self.ownerfs_read_group_bytes)
            .ok()
            .and_then(|bytes| {
                u64::try_from(self.ownerfs_read_group_max_inflight)
                    .ok()
                    .and_then(|inflight| bytes.checked_mul(inflight))
            });
        if self.ownerfs_read_group_bytes < self.window_bytes
            || self.ownerfs_read_group_bytes > MAX_OWNERFS_HIXL_READ_GROUP_BYTES
            || !self
                .ownerfs_read_group_bytes
                .is_multiple_of(self.window_bytes)
            || slot_capacity.is_none_or(|capacity| self.ownerfs_read_group_bytes > capacity)
            || !(1..=2).contains(&self.ownerfs_read_group_max_inflight)
            || !(1..=MAX_OWNERFS_HIXL_SESSIONS_PER_HOME).contains(&sessions)
            || inflight_bytes.is_none_or(|bytes| self.ownerfs_read_group_host_budget_bytes < bytes)
            || self.ownerfs_read_group_host_budget_bytes
                > MAX_OWNERFS_HIXL_READ_GROUP_HOST_BUDGET_BYTES
            || self.ownerfs_min_file_bytes > MAX_OWNERFS_HIXL_MIN_FILE_BYTES
        {
            return Err(invalid(
                "hixl.ownerfs_read_group_bytes must be a multiple of window_bytes up to 128 MiB and min(receive_slots, export_slots / ownerfs_sessions_per_home) windows, ownerfs_sessions_per_home must be 1..=4, ownerfs_read_group_max_inflight must be 1 or 2, ownerfs_read_group_host_budget_bytes must cover every in-flight window up to 4 GiB, and ownerfs_min_file_bytes must be <= 1 TiB",
            ));
        }
        if !(MIN_OWNERFS_HIXL_BATCH_FILE_BYTES..=MAX_OWNERFS_HIXL_BATCH_FILE_BYTES)
            .contains(&self.ownerfs_batch_max_file_bytes)
        {
            return Err(invalid(
                "hixl.ownerfs_batch_max_file_bytes must be 4097..=4194304",
            ));
        }
        Ok(())
    }

    fn validate_device_set(&self, enabled: bool) -> Result<()> {
        const MAX_HIXL_DEVICES: usize = 16;

        let device_count = self.additional_devices.len() + 1;
        if device_count > MAX_HIXL_DEVICES {
            return Err(invalid(format!(
                "hixl supports at most {MAX_HIXL_DEVICES} devices including the primary device"
            )));
        }
        let multiple_devices = device_count > 1;
        let primary = HixlDeviceConfig {
            device_id: self.device_id,
            engine_id: self.engine_id.clone(),
            device_listen_port: self.device_listen_port,
        };
        let mut device_ids = HashSet::with_capacity(device_count);
        let mut engine_ids = HashSet::with_capacity(device_count);
        let mut listener_ports = HashSet::with_capacity(device_count);
        for (index, device) in std::iter::once(&primary)
            .chain(self.additional_devices.iter())
            .enumerate()
        {
            if device.device_id < 0 {
                return Err(invalid(format!(
                    "hixl device at index {index} must have device_id >= 0"
                )));
            }
            if !device_ids.insert(device.device_id) {
                return Err(invalid(format!(
                    "hixl device_id {} is configured more than once",
                    device.device_id
                )));
            }

            if device.device_listen_port == Some(0) {
                return Err(invalid(format!(
                    "hixl device at index {index} listener port must be in 1..=65535"
                )));
            }
            if multiple_devices && device.device_listen_port.is_none() {
                return Err(invalid(
                    "hixl.device_listen_port must be set for every device in a multi-device config",
                ));
            }
            if let Some(port) = device.device_listen_port
                && !listener_ports.insert(port)
            {
                return Err(invalid(format!(
                    "hixl device_listen_port {port} is configured more than once"
                )));
            }

            if device.engine_id.is_empty() {
                if index == 0 && !enabled {
                    continue;
                }
                return Err(invalid(format!(
                    "hixl device at index {index} engine_id must be an explicit IP:port"
                )));
            }
            let endpoint: SocketAddr = device.engine_id.parse().map_err(|_| {
                invalid(format!(
                    "hixl device at index {index} engine_id must be an explicit IP:port"
                ))
            })?;
            if endpoint.port() == 0
                || endpoint.ip().is_unspecified()
                || endpoint.ip().is_multicast()
                || matches!(endpoint.ip(), std::net::IpAddr::V4(address) if address.is_broadcast())
            {
                return Err(invalid(format!(
                    "hixl device at index {index} engine_id must be a non-zero unicast IP:port"
                )));
            }
            if !engine_ids.insert(endpoint) {
                return Err(invalid(format!(
                    "hixl engine_id {} is configured more than once",
                    device.engine_id
                )));
            }
        }
        Ok(())
    }

    fn runtime_options_for_device(
        &self,
        device: &HixlDeviceConfig,
    ) -> afs_transport::hixl::RuntimeOptions {
        afs_transport::hixl::RuntimeOptions {
            device_id: device.device_id,
            engine_id: device.engine_id.clone(),
            device_listen_port: device.device_listen_port,
            window_bytes: self.window_bytes,
            receive_slots: self.receive_slots,
            export_slots: self.export_slots,
            transport: match self.transport {
                HixlTransport::Roce => afs_transport::hixl::ReadTransport::Roce,
                HixlTransport::Hccs => afs_transport::hixl::ReadTransport::Hccs,
            },
            buffer_pool: self.buffer_pool,
            buffer_pool_count: self.buffer_pool_count,
            buffer_pool_size_mib: self.buffer_pool_size_mib,
        }
    }

    pub fn runtime_options_per_device(&self) -> Vec<afs_transport::hixl::RuntimeOptions> {
        self.device_configs()
            .iter()
            .map(|device| self.runtime_options_for_device(device))
            .collect()
    }

    pub fn device_configs(&self) -> Vec<HixlDeviceConfig> {
        let mut devices = Vec::new();
        devices.push(HixlDeviceConfig {
            device_id: self.device_id,
            engine_id: self.engine_id.clone(),
            device_listen_port: self.device_listen_port,
        });
        devices.extend(self.additional_devices.iter().cloned());
        devices
    }
}

/// Checks the `[hixl]` table against the merged configuration.
pub(super) fn validate(role: Role, cfg: &super::Config) -> Result<()> {
    cfg.hixl.validate(
        role,
        HixlDeployment {
            ownerfs: cfg.ownerfs,
            dfs: cfg.dfs,
            data_mode: cfg.data_mode,
            // mTLS needs the CA, this node's identity and key, and a server
            // name; the encryption flag alone proves nothing about them.
            mutual_tls: cfg.tls_ca_certificate.is_some()
                && cfg.tls_identity_certificate.is_some()
                && cfg.tls_identity_private_key.is_some()
                && cfg.tls_server_name.is_some(),
            bound_peers: !cfg.trusted_node_certs.is_empty(),
        },
    )?;
    if cfg.hixl.enabled(cfg.data_mode, cfg.ownerfs)
        && (cfg.hixl.trusted_peers.is_empty()
            || cfg.hixl.trusted_peers.iter().any(|peer| {
                peer.is_empty() || !cfg.trusted_node_certs.contains_key(peer)
            }))
    {
        return Err(invalid(
            "hixl.trusted_peers must name at least one node from trusted_node_certs",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod hixl_config_tests {
    use super::super::FileConfig;
    use super::{DataMode, HixlConfig, HixlDeployment, HixlDeviceConfig, Role};

    fn ownerfs_deployment() -> HixlDeployment {
        HixlDeployment {
            ownerfs: true,
            dfs: false,
            data_mode: DataMode::Auto,
            mutual_tls: true,
            bound_peers: true,
        }
    }

    fn validate_hixl_disabled(config: &HixlConfig) -> afs_error::Result<()> {
        config.validate(
            Role::Node,
            HixlDeployment {
                ownerfs: false,
                dfs: false,
                data_mode: DataMode::Grpc,
                mutual_tls: false,
                bound_peers: false,
            },
        )
    }

    fn ownerfs_config() -> HixlConfig {
        HixlConfig {
            engine_id: "127.0.0.1:7920".into(),
            ..Default::default()
        }
    }

    #[test]
    fn auto_write_threshold_defaults_to_512_kib_and_accepts_the_full_request_range() {
        assert_eq!(HixlConfig::default().ownerfs_min_write_bytes, 512 * 1024);
        for size in [0, 512 * 1024, 1024 * 1024, 1024 * 1024 + 1] {
            let config = HixlConfig {
                ownerfs_min_write_bytes: size,
                ..ownerfs_config()
            };
            assert_eq!(
                config.validate(Role::Node, ownerfs_deployment()).is_ok(),
                size <= 1024 * 1024
            );
        }
    }

    #[test]
    fn hixl_requires_node_ownerfs_and_mutual_tls() {
        let config = ownerfs_config();
        let ownerfs = ownerfs_deployment();
        assert!(
            config
                .validate(
                    Role::Node,
                    HixlDeployment {
                        ownerfs: false,
                        data_mode: DataMode::Hixl,
                        ..ownerfs
                    }
                )
                .is_err()
        );
        assert!(
            config
                .validate(
                    Role::Node,
                    HixlDeployment {
                        data_mode: DataMode::Hixl,
                        dfs: true,
                        ..ownerfs
                    }
                )
                .is_err()
        );
        assert!(
            config
                .validate(
                    Role::Node,
                    HixlDeployment {
                        bound_peers: false,
                        ..ownerfs
                    }
                )
                .is_err()
        );
        assert!(config.validate(Role::Meta, ownerfs).is_err());
        let unaddressed = HixlConfig {
            engine_id: String::new(),
            ..config
        };
        assert!(
            unaddressed
                .validate(
                    Role::Node,
                    HixlDeployment {
                        data_mode: DataMode::Hixl,
                        ..ownerfs
                    }
                )
                .is_err()
        );
    }

    #[test]
    fn ownerfs_window_must_fit_both_slot_pools_and_the_budget() {
        let ownerfs = ownerfs_deployment();
        let valid = ownerfs_config();
        let explicit = HixlDeployment {
            data_mode: DataMode::Hixl,
            ..ownerfs
        };
        if cfg!(feature = "hixl") {
            assert!(valid.validate(Role::Node, explicit).is_ok());
        } else {
            assert!(valid.validate(Role::Node, explicit).is_err());
        }
        let auto = ownerfs_config();
        let cases = [
            HixlConfig {
                ownerfs_read_group_bytes: 6 * 1024 * 1024,
                ..auto.clone()
            },
            HixlConfig {
                ownerfs_read_group_bytes: 128 * 1024 * 1024,
                ..auto.clone()
            },
            HixlConfig {
                receive_slots: 2,
                ..auto.clone()
            },
            HixlConfig {
                ownerfs_read_group_max_inflight: 3,
                ..auto.clone()
            },
            HixlConfig {
                ownerfs_read_group_max_inflight: 2,
                ownerfs_read_group_host_budget_bytes: 16 * 1024 * 1024,
                ..auto.clone()
            },
            HixlConfig {
                ownerfs_min_file_bytes: 2 * 1024 * 1024 * 1024 * 1024,
                ..auto.clone()
            },
            HixlConfig {
                ownerfs_batch_max_file_bytes: 4096,
                ..auto.clone()
            },
            HixlConfig {
                ownerfs_batch_max_file_bytes: 4 * 1024 * 1024 + 1,
                ..auto.clone()
            },
        ];
        for config in cases {
            assert!(config.validate(Role::Node, ownerfs).is_err());
        }
        let prefetch = HixlConfig {
            ownerfs_read_group_max_inflight: 2,
            ..auto
        };
        assert!(prefetch.validate(Role::Node, ownerfs).is_ok());
    }

    #[test]
    fn pooled_sessions_split_the_export_slots_of_one_window() {
        let ownerfs = ownerfs_deployment();
        // 32 MiB windows need eight 4 MiB slots per session.
        for (sessions, export_slots, valid) in [
            (4, 32, true),
            (4, 24, false),
            (2, 16, true),
            (2, 8, false),
            (0, 16, false),
            (5, 64, false),
        ] {
            let config = HixlConfig {
                ownerfs_sessions_per_home: sessions,
                export_slots,
                ..ownerfs_config()
            };
            assert_eq!(
                config.validate(Role::Node, ownerfs).is_ok(),
                valid,
                "sessions={sessions}, export_slots={export_slots}"
            );
        }
        assert_eq!(HixlConfig::default().ownerfs_sessions_per_home, 2);
        assert_eq!(
            HixlConfig::default().ownerfs_read_group_bytes,
            32 * 1024 * 1024
        );
    }

    #[test]
    fn host_slot_pools_reserve_one_gib_and_cap_the_group() {
        let ownerfs = ownerfs_deployment();
        let reserved = HixlConfig {
            receive_slots: 128,
            export_slots: 128,
            ownerfs_read_group_bytes: 128 * 1024 * 1024,
            ..ownerfs_config()
        };
        // 128 + 128 slots of 4 MiB is exactly the 1 GiB Host slot budget.
        assert!(reserved.validate(Role::Node, ownerfs).is_ok());
        assert!(
            HixlConfig {
                receive_slots: 129,
                ..reserved.clone()
            }
            .validate(Role::Node, ownerfs)
            .is_err()
        );
        assert!(
            HixlConfig {
                export_slots: 129,
                ..reserved.clone()
            }
            .validate(Role::Node, ownerfs)
            .is_err()
        );
        // The group cap stays at 128 MiB even though the pools could hold more.
        assert!(
            HixlConfig {
                ownerfs_read_group_bytes: 132 * 1024 * 1024,
                ..reserved.clone()
            }
            .validate(Role::Node, ownerfs)
            .is_err()
        );
        // Every device lane allocates its own pool, so the 1 GiB budget is the
        // sum over lanes: two lanes take 64/64 each, four lanes 32/32.
        let extra = |id: i32| HixlDeviceConfig {
            device_id: id,
            engine_id: format!("127.0.0.1:{}", 7920 + id as u16),
            device_listen_port: Some(30650 + id as u16),
        };
        let two_lanes = HixlConfig {
            device_listen_port: Some(30650),
            additional_devices: vec![extra(1)],
            ..reserved.clone()
        };
        assert!(two_lanes.validate(Role::Node, ownerfs).is_err());
        assert!(
            HixlConfig {
                receive_slots: 64,
                export_slots: 64,
                ..two_lanes
            }
            .validate(Role::Node, ownerfs)
            .is_ok()
        );
        assert!(
            HixlConfig {
                device_listen_port: Some(30650),
                additional_devices: vec![extra(1), extra(2), extra(3)],
                receive_slots: 32,
                export_slots: 32,
                ownerfs_read_group_bytes: 32 * 1024 * 1024,
                ..ownerfs_config()
            }
            .validate(Role::Node, ownerfs)
            .is_ok()
        );
    }

    #[test]
    fn ownerfs_bounds_do_not_apply_while_ownerfs_mode_is_off() {
        let config = HixlConfig {
            receive_slots: 2,
            export_slots: 2,
            ..Default::default()
        };
        assert!(validate_hixl_disabled(&config).is_ok());
    }

    #[test]
    fn ownerfs_defaults_are_off_and_parse_from_toml() {
        let config = HixlConfig::default();
        assert!(!config.enabled(DataMode::Auto, true));
        assert_eq!(config.ownerfs_read_group_bytes, 32 * 1024 * 1024);
        assert_eq!(config.ownerfs_read_group_max_inflight, 2);
        assert_eq!(
            config.ownerfs_read_group_host_budget_bytes,
            1024 * 1024 * 1024
        );
        assert!(config.ownerfs_batch_read);
        assert_eq!(config.ownerfs_batch_max_file_bytes, 4 * 1024 * 1024);
        let parsed: HixlConfig = toml::from_str(
            r#"
                engine_id = "127.0.0.1:7920"
                ownerfs_read_group_bytes = 33554432
                ownerfs_read_group_max_inflight = 2
                ownerfs_batch_read = false
                ownerfs_batch_max_file_bytes = 1048576
            "#,
        )
        .unwrap();
        assert!(parsed.enabled(DataMode::Auto, true));
        assert_eq!(parsed.ownerfs_read_group_bytes, 32 * 1024 * 1024);
        assert!(parsed.validate(Role::Node, ownerfs_deployment()).is_ok());
    }

    fn valid_multi_device_config() -> HixlConfig {
        HixlConfig {
            device_id: 3,
            engine_id: "127.0.0.1:7920".into(),
            device_listen_port: Some(17920),
            additional_devices: vec![HixlDeviceConfig {
                device_id: 4,
                engine_id: "127.0.0.1:7921".into(),
                device_listen_port: Some(17921),
            }],
            ..Default::default()
        }
    }

    #[test]
    fn default_off_configuration_keeps_one_device_without_an_engine_address() {
        let config = HixlConfig::default();

        assert!(validate_hixl_disabled(&config).is_ok());
        assert_eq!(config.device_configs().len(), 1);
        assert_eq!(config.device_configs()[0].engine_id, "");
        assert_eq!(config.runtime_options_per_device().len(), 1);
    }

    #[test]
    fn session_idle_stays_below_the_source_session_ttl() {
        assert_eq!(HixlConfig::default().session_idle_ms, 20_000);
        for (idle_ms, valid) in [(0, false), (1, true), (20_000, true), (20_001, false)] {
            let mut config = valid_multi_device_config();
            config.session_idle_ms = idle_ms;
            assert_eq!(
                validate_hixl_disabled(&config).is_ok(),
                valid,
                "session_idle_ms = {idle_ms}"
            );
        }
    }

    #[test]
    fn toml_array_tables_deserialize_as_additional_device_lanes() {
        let file: FileConfig = toml::from_str(
            r#"
                [hixl]
                device_id = 3
                engine_id = "127.0.0.1:7920"
                device_listen_port = 17920

                [[hixl.additional_devices]]
                device_id = 4
                engine_id = "127.0.0.1:7921"
                device_listen_port = 17921
            "#,
        )
        .unwrap();
        let config = file.hixl.unwrap();

        assert!(validate_hixl_disabled(&config).is_ok());
        assert_eq!(config.device_configs().len(), 2);
        assert_eq!(config.device_configs()[1].device_id, 4);
    }

    #[test]
    fn device_options_keep_primary_first_and_copy_shared_runtime_budgets() {
        let config = valid_multi_device_config();
        let options = config.runtime_options_per_device();

        assert_eq!(options.len(), 2);
        assert_eq!(options[0].device_id, 3);
        assert_eq!(options[0].engine_id, "127.0.0.1:7920");
        assert_eq!(options[1].device_id, 4);
        assert_eq!(options[1].engine_id, "127.0.0.1:7921");
        assert_eq!(options[0].window_bytes, options[1].window_bytes);
        assert_eq!(options[0].receive_slots, options[1].receive_slots);
        assert_eq!(options[0].export_slots, options[1].export_slots);
        assert_eq!(options[0].buffer_pool_count, options[1].buffer_pool_count);
        assert_eq!(
            options[0].buffer_pool_size_mib,
            options[1].buffer_pool_size_mib
        );
    }

    #[test]
    fn device_set_rejects_duplicate_ids_engines_and_listener_ports() {
        type DeviceChange = fn(&mut HixlConfig);
        let cases: [(&str, DeviceChange); 3] = [
            ("device_id", |config| {
                config.additional_devices[0].device_id = config.device_id;
            }),
            ("engine_id", |config| {
                config.additional_devices[0].engine_id = config.engine_id.clone();
            }),
            ("device_listen_port", |config| {
                config.additional_devices[0].device_listen_port = config.device_listen_port;
            }),
        ];
        for (field, change) in cases {
            let mut config = valid_multi_device_config();
            change(&mut config);
            assert!(
                validate_hixl_disabled(&config).is_err(),
                "duplicate {field}"
            );
        }
    }

    #[test]
    fn multi_device_set_requires_explicit_nonzero_listener_ports() {
        for (primary, extra) in [
            (None, Some(17921)),
            (Some(17920), None),
            (Some(17920), Some(0)),
        ] {
            let mut config = valid_multi_device_config();
            config.device_listen_port = primary;
            config.additional_devices[0].device_listen_port = extra;
            assert!(
                validate_hixl_disabled(&config).is_err(),
                "listener ports: primary={primary:?}, extra={extra:?}"
            );
        }
    }

    #[test]
    fn device_set_is_limited_to_sixteen_devices_including_primary() {
        let mut config = valid_multi_device_config();
        // Sixteen lanes of two 8-slot pools fit the 1 GiB Host slot budget.
        config.receive_slots = 8;
        config.export_slots = 8;
        config.window_bytes = 4 * 1024 * 1024;
        let primary_device_id = config.device_id;
        config.additional_devices = (0..16)
            .filter(|device_id| *device_id != primary_device_id)
            .map(|device_id| HixlDeviceConfig {
                device_id,
                engine_id: format!("127.0.0.1:{}", 8000 + device_id),
                device_listen_port: Some((18_000 + device_id) as u16),
            })
            .collect();
        assert!(validate_hixl_disabled(&config).is_ok());

        config.additional_devices.push(HixlDeviceConfig {
            device_id: 16,
            engine_id: "127.0.0.1:8016".into(),
            device_listen_port: Some(18016),
        });
        assert!(validate_hixl_disabled(&config).is_err());
    }

    #[test]
    fn off_configuration_still_rejects_invalid_nonempty_engine_addresses() {
        for address in ["not-an-ip-port", "224.0.0.1:7920", "127.0.0.1:0"] {
            let config = HixlConfig {
                engine_id: address.into(),
                ..Default::default()
            };
            assert!(
                validate_hixl_disabled(&config).is_err(),
                "off mode must still reject {address}"
            );
        }
    }
}
