//! Defaults < TOML < explicit CLI, following main's typed configuration mechanism.
//! Configuration belongs to processes; libraries receive resolved value objects.
//!
//! 例：TOML 写 fs="ownerfs"，启动时传 --fs dfs，则运行 DistributedFs。
//! Cli 字段采用 Option，避免 clap 默认值把 TOML 的显式设置覆盖掉。
//! 编译 feature 决定代码是否存在，运行配置决定现有代码是否实例化；两者不能混用。
//! 这里只选后端和通道，不定义文件授权、存储位置或镜像发布策略。

use afs_error::{Error, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::{Component, Path, PathBuf},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Meta,
    Node,
}

/// Kernel I/O policy for read-only DFS FUSE opens. Writable opens remain cached.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum DfsFuseReadMode {
    #[default]
    Cached,
    Direct,
}
/// Optional immutable DFS read transport. Replication and OwnerFs retain data_mode.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HixlDfsReadMode {
    #[default]
    Off,
    Preferred,
    Required,
}

/// Optional OwnerFs HIXL read-ahead windows for read-only remote opens.
/// Writable opens and the gRPC single-range read keep their existing path.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HixlOwnerFsReadMode {
    #[default]
    Off,
    Preferred,
    Required,
}

/// Sequential read aggregation. `off` keeps the legacy per-request path.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum DfsReadGroupMode {
    #[default]
    Off,
    Sequential,
}

/// Immutable DFS read transport. HCCS uses two HBM buffer pools
/// as the A3 relay path; RoCE remains the default direct transport.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HixlTransport {
    #[default]
    Roce,
    HccsBufferPool,
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
    pub dfs_read_mode: HixlDfsReadMode,
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
    /// Peer Node IDs whose HIXL control sessions are pre-established at
    /// startup so the first read skips negotiation.
    pub prewarm_nodes: Vec<String>,
    pub buffer_pool_count: usize,
    pub buffer_pool_size_mib: usize,
    pub ownerfs_read_mode: HixlOwnerFsReadMode,
    /// Target upper bound for one OwnerFs read-ahead window. The negotiated
    /// capacity is `min(local window_bytes, Home window_bytes) × shared slots`,
    /// so a smaller Home window shortens prepares below this value.
    pub ownerfs_window_bytes: usize,
    /// 1 fetches windows on demand; 2 also prefetches the next window.
    pub ownerfs_max_inflight: usize,
    /// Process budget for cached and prefetching OwnerFs windows.
    pub ownerfs_host_budget_bytes: u64,
    /// Preferred mode only enables windows for files at least this large at open.
    pub ownerfs_min_file_bytes: u64,
    /// Reader control sessions per Home; each admits one window fetch at a time.
    pub ownerfs_sessions_per_home: usize,
}

const MAX_OWNERFS_HIXL_WINDOW_BYTES: usize = 64 * 1024 * 1024;
const MAX_OWNERFS_HIXL_HOST_BUDGET_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MAX_OWNERFS_HIXL_MIN_FILE_BYTES: u64 = 1024 * 1024 * 1024 * 1024;
const MAX_OWNERFS_HIXL_SESSIONS_PER_HOME: usize = 4;

impl Default for HixlConfig {
    fn default() -> Self {
        Self {
            dfs_read_mode: HixlDfsReadMode::Off,
            transport: HixlTransport::Roce,
            device_id: 0,
            engine_id: String::new(),
            device_listen_port: None,
            additional_devices: Vec::new(),
            window_bytes: 4 * 1024 * 1024,
            receive_slots: 16,
            export_slots: 16,
            session_idle_ms: 20_000,
            prewarm_nodes: Vec::new(),
            buffer_pool_count: 4,
            buffer_pool_size_mib: 8,
            ownerfs_read_mode: HixlOwnerFsReadMode::Off,
            ownerfs_window_bytes: 16 * 1024 * 1024,
            ownerfs_max_inflight: 1,
            ownerfs_host_budget_bytes: 256 * 1024 * 1024,
            ownerfs_min_file_bytes: 1024 * 1024,
            ownerfs_sessions_per_home: 1,
        }
    }
}

impl HixlConfig {
    fn validate(&self, role: Role, deployment: HixlDeployment) -> Result<()> {
        let HixlDeployment {
            dfs,
            ownerfs,
            grpc_data,
            mutual_tls,
            bound_peers,
        } = deployment;
        let pool_bytes = self
            .receive_slots
            .checked_add(self.export_slots)
            .and_then(|slots| slots.checked_mul(self.window_bytes));
        let buffer_pool_bytes = self
            .buffer_pool_count
            .checked_mul(self.buffer_pool_size_mib)
            .and_then(|mib| mib.checked_mul(2))
            .and_then(|pools| pools.checked_mul(1024 * 1024));
        if self.window_bytes == 0
            || self.window_bytes > 4 * 1024 * 1024
            || self.receive_slots == 0
            || self.export_slots == 0
            || self.receive_slots > 64
            || self.export_slots > 64
            || !(1..=20_000).contains(&self.session_idle_ms)
            || pool_bytes.is_none_or(|bytes| bytes > 512 * 1024 * 1024)
            || self.buffer_pool_count == 0
            || self.buffer_pool_count > 64
            || self.buffer_pool_size_mib == 0
            || self.buffer_pool_size_mib > 64
            || buffer_pool_bytes.is_none_or(|bytes| bytes > 512 * 1024 * 1024)
        {
            return Err(invalid(
                "HIXL requires device_id >= 0, 1..4 MiB windows, 1..64 Host slots per pool, 1..64 HBM buffer-pool count and size, <=512 MiB Host and HBM budgets, and session_idle_ms in 1..=20000",
            ));
        }
        self.validate_device_set()?;
        if !self.prewarm_nodes.is_empty() && self.dfs_read_mode == HixlDfsReadMode::Off {
            return Err(invalid(
                "hixl.prewarm_nodes requires dfs_read_mode preferred or required",
            ));
        }
        self.validate_ownerfs(role, ownerfs, grpc_data)?;
        if self.dfs_read_mode == HixlDfsReadMode::Off
            && self.ownerfs_read_mode == HixlOwnerFsReadMode::Off
        {
            return Ok(());
        }
        if self.dfs_read_mode != HixlDfsReadMode::Off && (role != Role::Node || !dfs) {
            return Err(invalid("hixl.dfs_read_mode requires Node DFS"));
        }
        if !mutual_tls || !bound_peers {
            return Err(invalid(
                "HIXL requires mTLS and administrator-managed trusted_node_certs",
            ));
        }
        if (self.dfs_read_mode == HixlDfsReadMode::Required
            || self.ownerfs_read_mode == HixlOwnerFsReadMode::Required)
            && !cfg!(feature = "hixl")
        {
            return Err(invalid("required HIXL feature is not compiled in"));
        }
        Ok(())
    }

    /// OwnerFs windows are bounded by both local slot pools: one prepare
    /// exports at most one window per remote export slot, and one native batch
    /// READ occupies one receive slot per window. Pooled sessions of one Home
    /// can each hold a window at once, so they split the export pool.
    fn validate_ownerfs(&self, role: Role, ownerfs: bool, grpc_data: bool) -> Result<()> {
        // Off keeps every existing [hixl] table valid, including small slot pools.
        if self.ownerfs_read_mode == HixlOwnerFsReadMode::Off {
            return Ok(());
        }
        let sessions = self.ownerfs_sessions_per_home;
        let slot_capacity = self
            .receive_slots
            .min(self.export_slots / sessions.max(1))
            .checked_mul(self.window_bytes);
        let inflight_bytes = u64::try_from(self.ownerfs_window_bytes)
            .ok()
            .and_then(|bytes| {
                u64::try_from(self.ownerfs_max_inflight)
                    .ok()
                    .and_then(|inflight| bytes.checked_mul(inflight))
            });
        if self.ownerfs_window_bytes < self.window_bytes
            || self.ownerfs_window_bytes > MAX_OWNERFS_HIXL_WINDOW_BYTES
            || !self.ownerfs_window_bytes.is_multiple_of(self.window_bytes)
            || slot_capacity.is_none_or(|capacity| self.ownerfs_window_bytes > capacity)
            || !(1..=2).contains(&self.ownerfs_max_inflight)
            || !(1..=MAX_OWNERFS_HIXL_SESSIONS_PER_HOME).contains(&sessions)
            || inflight_bytes.is_none_or(|bytes| self.ownerfs_host_budget_bytes < bytes)
            || self.ownerfs_host_budget_bytes > MAX_OWNERFS_HIXL_HOST_BUDGET_BYTES
            || self.ownerfs_min_file_bytes > MAX_OWNERFS_HIXL_MIN_FILE_BYTES
        {
            return Err(invalid(
                "hixl.ownerfs_window_bytes must be a multiple of window_bytes up to 64 MiB and min(receive_slots, export_slots / ownerfs_sessions_per_home) windows, ownerfs_sessions_per_home must be 1..=4, ownerfs_max_inflight must be 1 or 2, ownerfs_host_budget_bytes must cover every in-flight window up to 4 GiB, and ownerfs_min_file_bytes must be <= 1 TiB",
            ));
        }
        if role != Role::Node || !ownerfs {
            return Err(invalid("hixl.ownerfs_read_mode requires Node OwnerFs"));
        }
        if !grpc_data {
            return Err(invalid(
                "hixl.ownerfs_read_mode requires data_mode = \"grpc\"; OwnerFs RDMA and HIXL are exclusive",
            ));
        }
        if !cfg!(feature = "dfs") {
            return Err(invalid(
                "hixl.ownerfs_read_mode requires a build with the dfs feature",
            ));
        }
        Ok(())
    }

    fn validate_device_set(&self) -> Result<()> {
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
                if index == 0
                    && self.dfs_read_mode == HixlDfsReadMode::Off
                    && self.ownerfs_read_mode == HixlOwnerFsReadMode::Off
                {
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
                HixlTransport::HccsBufferPool => afs_transport::hixl::ReadTransport::HccsBufferPool,
            },
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
/// Deployment facts that decide which HIXL modes a Node may enable.
#[derive(Clone, Copy)]
struct HixlDeployment {
    dfs: bool,
    ownerfs: bool,
    grpc_data: bool,
    mutual_tls: bool,
    bound_peers: bool,
}

/// Meta authority backend. `memory` is useful for disposable local runs but
/// loses root/session authority whenever afs-meta exits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
pub enum MetaStoreBackend {
    #[serde(rename = "local-file")]
    #[value(name = "local-file")]
    LocalFile,
    #[serde(rename = "memory")]
    #[value(name = "memory")]
    InMemory,
}
/// Administrator-only experimental single-container workspace configuration.
/// Eligibility is fixed when OwnerFs is constructed, never switched online.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NativeWorkspaceConfig {
    pub control_dir: PathBuf,
    pub runtime: PathBuf,
    pub rootfs: PathBuf,
    pub idle_command: Vec<String>,
    pub identity_command: Vec<String>,
    pub workload_uid: u32,
    pub workload_gid: u32,
}

/// Administrator-enabled OwnerFs workspace bind mount entry.
/// The name is a single first-level OwnerFs workspace under the mount root.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceBindConfig {
    pub workspace: String,
}

/// External object storage durability. Deployment of the S3 service is external
/// to AFS. Credentials remain in the process environment or an administrator file.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DfsExternalBackendConfig {
    pub store_id: String,
    pub generation: String,
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    #[serde(default)]
    pub allow_http: bool,
    #[serde(default)]
    pub unsigned_payload: bool,
    pub credentials_file: Option<PathBuf>,
    /// Optional disposable content cache; omission preserves backend-only reads.
    pub cache: Option<DfsExternalCacheConfig>,
}

pub(crate) const MAX_DFS_EXTERNAL_CACHE_CAPACITY_BYTES: u64 = 1 << 40;

/// Bounded asynchronous cache. Neither pending nor disk data is a durable replica.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DfsExternalCacheConfig {
    pub directory: PathBuf,
    pub capacity_bytes: u64,
    pub pending_bytes: u64,
    /// Like JuiceFS cache-large-write: full-sized write chunks are opt-in.
    #[serde(default)]
    pub cache_large_write: bool,
}

/// Optional native Peer read disk cache. It carries no durability or authority.
/// Payload memory is temporary; there is no resident RAM cache configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DfsPeerCacheConfig {
    pub directory: PathBuf,
    pub capacity_bytes: u64,
}

impl DfsExternalBackendConfig {
    /// Reject ambiguous namespaces, implicit plaintext and relative secret paths.
    pub fn validate(&self) -> Result<()> {
        for value in [&self.store_id, &self.generation, &self.bucket, &self.region] {
            if value.is_empty()
                || value.len() > 128
                || !value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"_-.".contains(&byte))
                || value == "."
                || value == ".."
            {
                return Err(invalid(
                    "external backend identity fields require safe non-empty path segments",
                ));
            }
        }
        let endpoint = tonic::transport::Endpoint::from_shared(self.endpoint.clone())
            .map_err(|_| invalid("external backend endpoint requires an http(s) URI"))?;
        let uri = endpoint.uri();
        if !matches!(uri.scheme_str(), Some("http" | "https"))
            || uri.host().is_none()
            || uri
                .authority()
                .is_some_and(|authority| authority.as_str().contains('@'))
            || uri.query().is_some()
            || !matches!(uri.path(), "" | "/")
            || (uri.scheme_str() == Some("http") && !self.allow_http)
        {
            return Err(invalid(
                "external backend requires an explicit HTTPS origin, or allow_http=true for an isolated test endpoint",
            ));
        }
        if self
            .credentials_file
            .as_ref()
            .is_some_and(|path| !path.is_absolute())
        {
            return Err(invalid(
                "external backend credentials_file must be absolute",
            ));
        }
        if let Some(cache) = &self.cache
            && (!cache.directory.is_absolute()
                || cache.capacity_bytes == 0
                || cache.pending_bytes == 0
                || cache.pending_bytes > cache.capacity_bytes
                || cache.capacity_bytes > MAX_DFS_EXTERNAL_CACHE_CAPACITY_BYTES
                || usize::try_from(cache.pending_bytes).is_err())
        {
            return Err(invalid(
                "external cache requires an absolute directory, capacity up to 1 TiB and non-zero pending bytes no greater than capacity",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Parser, Default)]
#[command(version, about = "AFS process foundation")]
/// 命令行输入层。None 表示用户未传入，因此应继续考虑 TOML 与最终默认值。
pub struct Cli {
    #[arg(long)]
    pub config: Option<PathBuf>,
    #[arg(long)]
    pub id: Option<String>,
    #[arg(long)]
    pub grpc_listen: Option<SocketAddr>,
    #[arg(long)]
    pub rest_listen: Option<SocketAddr>,
    #[arg(long)]
    pub meta_endpoint: Option<String>,
    /// Meta authority: local-file (default, new-format directory) or volatile memory.
    /// Retired etcd/Redis data requires its original binary; it is not imported.
    #[arg(long, value_enum)]
    pub meta_store: Option<MetaStoreBackend>,
    /// Maximum bytes of one framed Meta operation-log record.
    #[arg(long)]
    pub meta_record_max_bytes: Option<usize>,
    /// Maximum bytes of a durable Meta operation-log batch.
    #[arg(long)]
    pub meta_batch_max_bytes: Option<usize>,
    /// Total WAL byte limit; writes fail closed when no space remains.
    #[arg(long)]
    pub meta_wal_max_bytes: Option<u64>,
    /// Background checkpoint WAL byte threshold.
    #[arg(long)]
    pub meta_checkpoint_wal_bytes: Option<u64>,
    /// Background checkpoint record threshold.
    #[arg(long)]
    pub meta_checkpoint_wal_records: Option<u64>,
    /// Background checkpoint interval in milliseconds.
    #[arg(long)]
    pub meta_checkpoint_interval_ms: Option<u64>,
    /// Soft segment rotation size, no greater than the WAL limit.
    #[arg(long)]
    pub meta_wal_segment_bytes: Option<u64>,
    /// Idempotent outcome retention in milliseconds.
    #[arg(long)]
    pub meta_request_retention_ms: Option<u64>,
    /// Maximum retained idempotent outcomes.
    #[arg(long)]
    pub meta_request_outcomes_max: Option<usize>,
    /// Maximum retained outcome bytes (owned capacities).
    #[arg(long)]
    pub meta_request_bytes_max: Option<u64>,
    /// Maximum retained watch events.
    #[arg(long)]
    pub meta_watch_events_max: Option<usize>,
    /// Maximum retained watch bytes (owned capacities).
    #[arg(long)]
    pub meta_watch_bytes_max: Option<u64>,
    /// Watch event retention in milliseconds.
    #[arg(long)]
    pub meta_watch_retention_ms: Option<u64>,
    /// Allow a node to become functionally ready with a healthy volatile Meta.
    #[arg(long)]
    pub allow_volatile_meta: Option<bool>,
    /// Enable centralized HRW placement for new OwnerFs workspaces on Meta.
    #[arg(long)]
    pub ownerfs_mkdir_load_balance: Option<bool>,
    #[arg(long)]
    pub peer_endpoint: Option<String>,
    /// 对外发布的本 Node gRPC URI；监听 0.0.0.0 时必须显式指定。
    #[arg(long)]
    pub advertise_endpoint: Option<String>,
    /// Encrypt AFS internal traffic after mutual certificate authentication (default false).
    #[arg(long)]
    pub tls_encryption_enabled: Option<bool>,
    #[arg(long)]
    pub tls_ca_certificate: Option<PathBuf>,
    #[arg(long)]
    pub tls_identity_certificate: Option<PathBuf>,
    #[arg(long)]
    pub tls_identity_private_key: Option<PathBuf>,
    #[arg(long)]
    pub tls_server_name: Option<String>,
    #[arg(long)]
    pub fs: Option<String>,
    #[arg(long)]
    pub data_mode: Option<String>,
    #[arg(long)]
    pub rdma_device: Option<String>,
    #[arg(long)]
    pub data_dir: Option<PathBuf>,
    #[arg(long)]
    pub uds_path: Option<PathBuf>,
    #[arg(long)]
    pub ownerfs_mount: Option<PathBuf>,
    /// Experimental managed-container native path; default false, not production READY.
    #[arg(long)]
    pub experimental_native_workspace: Option<bool>,
    /// Experimental OwnerFs workspace bind mount path; default false, not production READY.
    #[arg(long)]
    pub experimental_ownerfs_workspace_bind: Option<bool>,
    /// Native egress root for OwnerFs workspace bind targets; absolute, outside
    /// ownerfs_mount and data_dir, required when experimental_ownerfs_workspace_bind.
    #[arg(long)]
    pub ownerfs_workspace_egress_root: Option<PathBuf>,
    #[arg(long)]
    pub dfs_mount: Option<PathBuf>,
    /// FUSE receive threads shared by OwnerFs and DFS (1..=64).
    #[arg(long)]
    pub fuse_threads: Option<u16>,
    /// Clone the FUSE descriptor only when multiple receive threads are used.
    #[arg(long)]
    pub fuse_clone_fd: Option<bool>,
    /// Bounded callback executor worker count (1..=1024).
    #[arg(long)]
    pub fuse_workers: Option<u16>,
    /// Maximum accepted FUSE tasks, including queued and asynchronous work.
    #[arg(long)]
    pub fuse_max_tasks: Option<usize>,
    /// Keep DFS kernel pages only across opens of the exact committed version.
    #[arg(long)]
    pub dfs_fuse_keep_cache: Option<bool>,
    /// Zero preserves strict path freshness; positive values opt into relaxed caching.
    #[arg(long)]
    pub dfs_fuse_entry_ttl_ms: Option<u32>,
    #[arg(long)]
    pub dfs_fuse_attr_ttl_ms: Option<u32>,
    /// Kernel read-ahead limit, independent of the DFS object prefetch budget.
    #[arg(long)]
    pub fuse_max_readahead_bytes: Option<u32>,
    /// Maximum cached immutable DFS version/layout entries (1..=65536).
    #[arg(long)]
    pub dfs_version_cache_entries: Option<usize>,
    /// Retained immutable metadata estimate (1..=1GiB), separate from active readers and RSS.
    #[arg(long)]
    pub dfs_version_cache_bytes: Option<u64>,
    /// Maximum shared outboard range-work admission wait (0..=50 ms).
    #[arg(long)]
    pub dfs_outboard_work_wait_ms: Option<u64>,
    /// Queued plus active catalog commit requests (1..=256), with a separate 1MiB control bound.
    #[arg(long)]
    pub dfs_catalog_commit_queue: Option<usize>,
    /// Ordinary DFS FUSE front-owned payload across files (8MiB..=1TiB), not total memory.
    #[arg(long)]
    pub dfs_write_queue_bytes: Option<u64>,
    /// Native Node live write-body capacity (4..=64MiB); S3 budgets are separate.
    #[arg(long)]
    pub dfs_dirty_budget_bytes: Option<u64>,
    /// GC data-protection grace after a reference retires, in milliseconds
    /// (0..=600000). Governs when retired references stop protecting inodes
    /// and chunks. Reference *records* additionally linger as replay
    /// tombstones for a derived admission window beyond this value.
    #[arg(long)]
    pub dfs_gc_grace_ms: Option<u64>,
    /// Native uploaded but unpublished objects (1..=1024), within 4MiB metadata.
    #[arg(long)]
    pub dfs_unpublished_descriptors: Option<usize>,
    /// Native Node upload tasks (1..=8), sharing the live-body budget.
    #[arg(long)]
    pub dfs_upload_inflight_chunks: Option<usize>,
    /// Native background commit scheduling period (1..=60000ms), not a deadline.
    #[arg(long)]
    pub dfs_background_commit_ms: Option<u64>,
    /// Queued plus active optional sidecar installs (1..=64).
    #[arg(long)]
    pub dfs_sidecar_install_queue: Option<usize>,
    /// Concurrent native cache fill-flight records (1..=8), not active body transfers.
    #[arg(long)]
    pub dfs_fetch_max_inflight_objects: Option<usize>,
    /// Native object lookahead distance (0..=32MiB); 0 disables background fetch.
    #[arg(long)]
    pub dfs_readahead_max_bytes: Option<u64>,
    /// Verified Peer resident objects (1..=16), sharing the payload byte limit.
    #[arg(long)]
    pub dfs_peer_serve_resident_objects: Option<usize>,
    /// Shared Peer resident, in-flight and retained frame bytes (4..=64 MiB).
    #[arg(long)]
    pub dfs_peer_serve_payload_bytes: Option<u64>,
    /// One native Peer admission wait deadline (0..=50 ms); zero never waits.
    #[arg(long)]
    pub dfs_peer_serve_wait_ms: Option<u64>,
    /// Read-only DFS FUSE opens: cached (default) or direct. Direct mmap needs kernel support.
    #[arg(long, value_enum)]
    pub dfs_fuse_read_mode: Option<DfsFuseReadMode>,
    /// Filesystem-wide immutable DFS replica target count. Applied by Meta at initialization.
    #[arg(long)]
    pub dfs_desired_copies: Option<u16>,
    /// Replica acknowledgements required before a DFS FileVersion can commit.
    #[arg(long)]
    pub dfs_sync_required_copies: Option<u16>,
    #[arg(long)]
    pub dfs_min_distinct_nodes: Option<u16>,
    #[arg(long)]
    pub dfs_min_distinct_failure_domains: Option<u16>,
    /// required, preferred, or none.
    #[arg(long)]
    pub dfs_local_copy: Option<String>,
    #[arg(long)]
    pub dfs_read_max_ops_per_batch: Option<usize>,
    #[arg(long)]
    pub dfs_read_max_inflight_bytes: Option<u64>,
    #[arg(long)]
    pub dfs_source_cache_max_ms: Option<u64>,
    #[arg(long)]
    pub dfs_source_lookahead_chunks: Option<usize>,
    #[arg(long)]
    pub dfs_source_cache_bytes: Option<usize>,
    /// Bounded coarse read timing for explicit diagnosis only; disabled by default.
    #[arg(long)]
    pub dfs_read_diagnostics_enabled: Option<bool>,
    #[arg(long)]
    pub dfs_native_open_read_authority: Option<bool>,
    /// Native gRPC read CRC policy (default off); receiver opts in to B checks.
    /// All serving peers must use the same mode.
    #[arg(long)]
    pub dfs_read_crc_mode: Option<DfsReadCrcMode>,
    /// Write native objects with segmented CRC32C; native DFS Nodes default to true.
    #[arg(long)]
    pub dfs_crc32c_writes: Option<bool>,
    /// Omit per-chunk GC registration; CRC commits fence their own write generations.
    #[arg(long)]
    pub dfs_optimistic_gc_commit: Option<bool>,
    /// Sequential read aggregation: off or sequential.
    #[arg(long, value_enum)]
    pub dfs_read_group_mode: Option<DfsReadGroupMode>,
    /// ReadGroup payload budget in bytes; 1 MiB..=128 MiB, 4 KiB aligned.
    #[arg(long)]
    pub dfs_read_group_bytes: Option<u64>,
    #[arg(long)]
    pub dfs_read_group_max_inflight: Option<usize>,
    #[arg(long)]
    pub dfs_read_group_max_descriptors: Option<usize>,
    #[arg(long)]
    pub dfs_read_group_host_budget_bytes: Option<u64>,
    #[arg(long)]
    pub timeout_ms: Option<u64>,
    #[arg(long)]
    pub log_level: Option<String>,
    #[arg(long)]
    pub trace_enabled: Option<bool>,
    #[arg(long)]
    pub trace_endpoint: Option<String>,
    #[arg(long)]
    pub trace_sample_ratio: Option<f64>,
    #[arg(long)]
    pub print_config: bool,
}
impl Cli {
    /// Parse the process arguments with standard Clap exit codes and formatting.
    /// Retired backend options get a migration hint before the original error;
    /// they are never accepted or mapped to a different authority implicitly.
    pub fn parse_process() -> Self {
        let arguments: Vec<_> = std::env::args_os().collect();
        match Self::try_parse_from(&arguments) {
            Ok(cli) => cli,
            Err(error) => {
                let retired = arguments.windows(2).any(|pair| {
                    pair[0] == "--meta-store" && (pair[1] == "etcd" || pair[1] == "redis")
                }) || arguments.iter().any(|argument| {
                    let argument = argument.to_string_lossy();
                    matches!(
                        argument.as_ref(),
                        "--meta-store=etcd"
                            | "--meta-store=redis"
                            | "--etcd-endpoint"
                            | "--redis-endpoint"
                    ) || argument.starts_with("--etcd-endpoint=")
                        || argument.starts_with("--redis-endpoint=")
                });
                if retired
                    && !matches!(
                        error.kind(),
                        clap::error::ErrorKind::DisplayHelp
                            | clap::error::ErrorKind::DisplayVersion
                    )
                {
                    eprintln!(
                        "Meta etcd/Redis options are retired. Use local-file with an empty directory; retain the original binary and directory to recover old data. No implicit authority migration is performed."
                    );
                }
                error.exit()
            }
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
/// TOML 输入层。拒绝未知字段，让拼错开关在启动时失败，而不是静默使用默认行为。
struct FileConfig {
    id: Option<String>,
    grpc_listen: Option<SocketAddr>,
    rest_listen: Option<SocketAddr>,
    meta_endpoint: Option<String>,
    meta_store: Option<MetaStoreBackend>,
    meta_record_max_bytes: Option<usize>,
    meta_batch_max_bytes: Option<usize>,
    meta_wal_max_bytes: Option<u64>,
    meta_checkpoint_wal_bytes: Option<u64>,
    meta_checkpoint_wal_records: Option<u64>,
    meta_checkpoint_interval_ms: Option<u64>,
    meta_wal_segment_bytes: Option<u64>,
    meta_request_retention_ms: Option<u64>,
    meta_request_outcomes_max: Option<usize>,
    meta_request_bytes_max: Option<u64>,
    meta_watch_events_max: Option<usize>,
    meta_watch_bytes_max: Option<u64>,
    meta_watch_retention_ms: Option<u64>,
    allow_volatile_meta: Option<bool>,
    ownerfs_mkdir_load_balance: Option<bool>,
    peer_endpoint: Option<String>,
    advertise_endpoint: Option<String>,
    tls_encryption_enabled: Option<bool>,
    tls_ca_certificate: Option<PathBuf>,
    tls_identity_certificate: Option<PathBuf>,
    tls_identity_private_key: Option<PathBuf>,
    tls_server_name: Option<String>,
    /// TLS 验证后再用证书 DER 的完整字节绑定 Node ID；仅由受信管理配置设置。
    trusted_node_certs: Option<HashMap<String, PathBuf>>,
    fs: Option<String>,
    data_mode: Option<String>,
    rdma_device: Option<String>,
    hixl: Option<HixlConfig>,
    data_dir: Option<PathBuf>,
    uds_path: Option<PathBuf>,
    ownerfs_mount: Option<PathBuf>,
    experimental_native_workspace: Option<bool>,
    native_workspace: Option<NativeWorkspaceConfig>,
    experimental_ownerfs_workspace_bind: Option<bool>,
    ownerfs_workspace_bind: Option<WorkspaceBindConfig>,
    ownerfs_workspace_egress_root: Option<PathBuf>,
    dfs_mount: Option<PathBuf>,
    fuse_threads: Option<u16>,
    fuse_clone_fd: Option<bool>,
    fuse_workers: Option<u16>,
    fuse_max_tasks: Option<usize>,
    dfs_read_pipeline: Option<crate::node::read_pipeline::ReadPipelineLimits>,
    dfs_fuse_keep_cache: Option<bool>,
    dfs_fuse_entry_ttl_ms: Option<u32>,
    dfs_fuse_attr_ttl_ms: Option<u32>,
    fuse_max_readahead_bytes: Option<u32>,
    dfs_version_cache_entries: Option<usize>,
    dfs_version_cache_bytes: Option<u64>,
    dfs_outboard_work_wait_ms: Option<u64>,
    dfs_catalog_commit_queue: Option<usize>,
    dfs_write_queue_bytes: Option<u64>,
    dfs_dirty_budget_bytes: Option<u64>,
    dfs_gc_grace_ms: Option<u64>,
    dfs_unpublished_descriptors: Option<usize>,
    dfs_upload_inflight_chunks: Option<usize>,
    dfs_background_commit_ms: Option<u64>,
    dfs_sidecar_install_queue: Option<usize>,
    dfs_fetch_max_inflight_objects: Option<usize>,
    dfs_readahead_max_bytes: Option<u64>,
    dfs_peer_serve_resident_objects: Option<usize>,
    dfs_peer_serve_payload_bytes: Option<u64>,
    dfs_peer_serve_wait_ms: Option<u64>,
    dfs_external_backend: Option<DfsExternalBackendConfig>,
    dfs_peer_cache: Option<DfsPeerCacheConfig>,
    dfs_write_buffer_idle_bytes: Option<u64>,
    dfs_fuse_read_mode: Option<DfsFuseReadMode>,
    dfs_desired_copies: Option<u16>,
    dfs_sync_required_copies: Option<u16>,
    dfs_min_distinct_nodes: Option<u16>,
    dfs_min_distinct_failure_domains: Option<u16>,
    dfs_local_copy: Option<String>,
    dfs_read_max_ops_per_batch: Option<usize>,
    dfs_read_max_inflight_bytes: Option<u64>,
    dfs_source_cache_max_ms: Option<u64>,
    dfs_source_lookahead_chunks: Option<usize>,
    dfs_source_cache_bytes: Option<usize>,
    dfs_read_diagnostics_enabled: Option<bool>,
    dfs_native_open_read_authority: Option<bool>,
    dfs_read_crc_mode: Option<DfsReadCrcMode>,
    dfs_crc32c_writes: Option<bool>,
    dfs_optimistic_gc_commit: Option<bool>,
    dfs_read_group_mode: Option<DfsReadGroupMode>,
    dfs_read_group_bytes: Option<u64>,
    dfs_read_group_max_inflight: Option<usize>,
    dfs_read_group_max_descriptors: Option<usize>,
    dfs_read_group_host_budget_bytes: Option<u64>,
    timeout_ms: Option<u64>,
    log_level: Option<String>,
    trace_enabled: Option<bool>,
    trace_endpoint: Option<String>,
    trace_sample_ratio: Option<f64>,
}
/// Remote native CRC-format reads only; write integrity is independent.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum DfsReadCrcMode {
    Both,
    Receiver,
    #[default]
    Off,
}
impl DfsReadCrcMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Both => "both",
            Self::Receiver => "receiver",
            Self::Off => "off",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
/// 解析和校验后的进程配置。模块接收这个值，不各自重复读取配置文件或环境变量。
pub struct Config {
    pub id: String,
    pub grpc_listen: SocketAddr,
    pub rest_listen: SocketAddr,
    pub meta_endpoint: Option<String>,
    pub meta_store: MetaStoreBackend,
    pub meta_record_max_bytes: usize,
    pub meta_batch_max_bytes: usize,
    pub meta_wal_max_bytes: u64,
    pub meta_checkpoint_wal_bytes: u64,
    pub meta_checkpoint_wal_records: u64,
    pub meta_checkpoint_interval_ms: u64,
    pub meta_wal_segment_bytes: u64,
    pub meta_request_retention_ms: u64,
    pub meta_request_outcomes_max: usize,
    pub meta_request_bytes_max: u64,
    pub meta_watch_events_max: usize,
    pub meta_watch_bytes_max: u64,
    pub meta_watch_retention_ms: u64,
    pub allow_volatile_meta: bool,
    pub ownerfs_mkdir_load_balance: bool,
    pub peer_endpoint: Option<String>,
    pub advertise_endpoint: Option<String>,
    pub tls_encryption_enabled: bool,
    pub tls_ca_certificate: Option<PathBuf>,
    pub tls_identity_certificate: Option<PathBuf>,
    pub tls_identity_private_key: Option<PathBuf>,
    pub tls_server_name: Option<String>,
    pub trusted_node_certs: HashMap<String, PathBuf>,
    pub ownerfs: bool,
    pub dfs: bool,
    pub data_mode: String,
    pub rdma_device: Option<String>,
    pub hixl: HixlConfig,
    pub data_dir: PathBuf,
    pub uds_path: PathBuf,
    pub ownerfs_mount: Option<PathBuf>,
    pub experimental_native_workspace: bool,
    pub native_workspace: Option<NativeWorkspaceConfig>,
    pub experimental_ownerfs_workspace_bind: bool,
    pub ownerfs_workspace_bind: Option<WorkspaceBindConfig>,
    pub ownerfs_workspace_egress_root: Option<PathBuf>,
    pub dfs_mount: Option<PathBuf>,
    pub fuse_threads: u16,
    pub fuse_clone_fd: bool,
    pub fuse_workers: u16,
    pub fuse_max_tasks: usize,
    pub dfs_read_pipeline: crate::node::read_pipeline::ReadPipelineLimits,
    pub dfs_fuse_keep_cache: bool,
    pub dfs_fuse_entry_ttl_ms: u32,
    pub dfs_fuse_attr_ttl_ms: u32,
    pub fuse_max_readahead_bytes: u32,
    pub dfs_version_cache_entries: usize,
    pub dfs_version_cache_bytes: u64,
    pub dfs_outboard_work_wait_ms: u64,
    pub dfs_catalog_commit_queue: usize,
    pub dfs_write_queue_bytes: u64,
    pub dfs_dirty_budget_bytes: u64,
    pub dfs_gc_grace_ms: u64,
    pub dfs_unpublished_descriptors: usize,
    pub dfs_upload_inflight_chunks: usize,
    pub dfs_background_commit_ms: u64,
    pub dfs_sidecar_install_queue: usize,
    pub dfs_fetch_max_inflight_objects: usize,
    pub dfs_readahead_max_bytes: u64,
    pub dfs_peer_serve_resident_objects: usize,
    pub dfs_peer_serve_payload_bytes: u64,
    pub dfs_peer_serve_wait_ms: u64,
    pub dfs_external_backend: Option<DfsExternalBackendConfig>,
    pub dfs_peer_cache: Option<DfsPeerCacheConfig>,
    /// Reusable idle write-body capacity only, not a live-body quota.
    pub dfs_write_buffer_idle_bytes: u64,
    pub dfs_fuse_read_mode: DfsFuseReadMode,
    pub dfs_desired_copies: u16,
    pub dfs_sync_required_copies: u16,
    pub dfs_min_distinct_nodes: u16,
    pub dfs_min_distinct_failure_domains: u16,
    pub dfs_local_copy: String,
    pub dfs_read_max_ops_per_batch: usize,
    pub dfs_read_max_inflight_bytes: u64,
    pub dfs_source_cache_max_ms: u64,
    pub dfs_source_lookahead_chunks: usize,
    pub dfs_source_cache_bytes: usize,
    pub dfs_read_diagnostics_enabled: bool,
    pub dfs_native_open_read_authority: bool,
    pub dfs_read_crc_mode: DfsReadCrcMode,
    pub dfs_crc32c_writes: bool,
    pub dfs_optimistic_gc_commit: bool,
    pub dfs_read_group_mode: DfsReadGroupMode,
    pub dfs_read_group_bytes: u64,
    pub dfs_read_group_max_inflight: usize,
    pub dfs_read_group_max_descriptors: usize,
    pub dfs_read_group_host_budget_bytes: u64,
    pub timeout_ms: u64,
    pub log_level: String,
    pub trace_enabled: bool,
    pub trace_endpoint: String,
    pub trace_sample_ratio: f64,
    #[serde(skip)]
    pub print_config: bool,
}
impl Config {
    /// 按默认值 < TOML < 显式 CLI 合并；所有可发现的配置错误在监听服务之前返回。
    /// Meta 可以没有文件后端；Node 至少启用一个。未指定 fs 时使用当前编译进去的集合。
    pub fn resolve(role: Role, cli: Cli) -> Result<Self> {
        let file: FileConfig = match cli.config {
            Some(path) => toml::from_str(&std::fs::read_to_string(path)?).map_err(|error| {
                invalid(format!("{error}; Meta supports local-file (new-format/empty directory) or volatile memory; retired backend data must use its original binary and directory"))
            })?,
            None => FileConfig::default(),
        };
        if role == Role::Node
            && (cli.ownerfs_mkdir_load_balance.is_some()
                || file.ownerfs_mkdir_load_balance.is_some())
        {
            return Err(invalid(
                "ownerfs_mkdir_load_balance is only valid for afs-meta",
            ));
        }
        let id = cli.id.or(file.id).unwrap_or_else(|| {
            match role {
                Role::Node => "node",
                Role::Meta => "meta",
            }
            .into()
        });
        if id.is_empty()
            || id.len() > 128
            || !id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
        {
            return Err(invalid("id must contain 1..128 letters, digits, _ or -"));
        }
        // cfg! 查询本次二进制的编译能力；Backend 内的 #[cfg] 才实际裁掉后端模块。
        let requested = cli.fs.or(file.fs);
        let (ownerfs, dfs) = match requested.as_deref() {
            None => (cfg!(feature = "ownerfs"), cfg!(feature = "dfs")),
            Some("all") => (true, true),
            Some("ownerfs") => (true, false),
            Some("dfs") => (false, true),
            _ => return Err(invalid("fs must be ownerfs, dfs or all")),
        };
        if role == Role::Node
            && ((ownerfs && !cfg!(feature = "ownerfs")) || (dfs && !cfg!(feature = "dfs")))
        {
            return Err(invalid("requested filesystem backend is not compiled in"));
        }
        if role == Role::Node && !ownerfs && !dfs {
            return Err(invalid(
                "node requires at least one compiled filesystem backend",
            ));
        }
        let data_mode = cli
            .data_mode
            .or(file.data_mode)
            .unwrap_or_else(|| "grpc".into());
        if !["grpc", "rdma", "auto"].contains(&data_mode.as_str()) {
            return Err(invalid("data_mode must be grpc, rdma or auto"));
        }
        if data_mode == "rdma" && !cfg!(feature = "rdma") {
            return Err(invalid("rdma feature is not compiled in"));
        }
        // This policy is implemented only for native CRC-format gRPC reads.
        // Keep unrelated backends and transports on their existing path.
        let default_read_crc_mode =
            if dfs && file.dfs_external_backend.is_none() && data_mode == "grpc" {
                DfsReadCrcMode::default()
            } else {
                DfsReadCrcMode::Both
            };
        let dfs_read_crc_mode = cli
            .dfs_read_crc_mode
            .or(file.dfs_read_crc_mode)
            .unwrap_or(default_read_crc_mode);
        let dfs_desired_copies = cli
            .dfs_desired_copies
            .or(file.dfs_desired_copies)
            .unwrap_or(1);
        let dfs_sync_required_copies = cli
            .dfs_sync_required_copies
            .or(file.dfs_sync_required_copies)
            .unwrap_or(1);
        let dfs_min_distinct_nodes = cli
            .dfs_min_distinct_nodes
            .or(file.dfs_min_distinct_nodes)
            .unwrap_or(1);
        let dfs_min_distinct_failure_domains = cli
            .dfs_min_distinct_failure_domains
            .or(file.dfs_min_distinct_failure_domains)
            .unwrap_or(1);
        let dfs_local_copy = cli
            .dfs_local_copy
            .or(file.dfs_local_copy)
            .unwrap_or_else(|| "required".into());
        if dfs_sync_required_copies == 0
            || dfs_sync_required_copies > dfs_desired_copies
            || dfs_min_distinct_nodes == 0
            || dfs_min_distinct_nodes > dfs_desired_copies
            || dfs_min_distinct_failure_domains == 0
            || dfs_min_distinct_failure_domains > dfs_desired_copies
        {
            return Err(invalid(
                "DFS replication counts must be non-zero and no greater than dfs_desired_copies",
            ));
        }
        if !["required", "preferred", "none"].contains(&dfs_local_copy.as_str()) {
            return Err(invalid(
                "dfs_local_copy must be required, preferred or none",
            ));
        }
        let dfs_read_max_ops_per_batch = cli
            .dfs_read_max_ops_per_batch
            .or(file.dfs_read_max_ops_per_batch)
            .unwrap_or(128);
        let dfs_read_max_inflight_bytes = cli
            .dfs_read_max_inflight_bytes
            .or(file.dfs_read_max_inflight_bytes)
            .unwrap_or(8 * 1024 * 1024);
        let dfs_source_cache_max_ms = cli
            .dfs_source_cache_max_ms
            .or(file.dfs_source_cache_max_ms)
            .unwrap_or(5000);
        let dfs_source_lookahead_chunks = cli
            .dfs_source_lookahead_chunks
            .or(file.dfs_source_lookahead_chunks)
            .unwrap_or(64);
        let dfs_source_cache_bytes = cli
            .dfs_source_cache_bytes
            .or(file.dfs_source_cache_bytes)
            .unwrap_or(4 * 1024 * 1024);
        if dfs_source_lookahead_chunks > 128 || dfs_source_cache_bytes > 4 * 1024 * 1024 {
            return Err(invalid(
                "DFS source metadata requires lookahead <= 128 chunks and cache <= 4194304 bytes",
            ));
        }
        let dfs_read_group_mode = cli
            .dfs_read_group_mode
            .or(file.dfs_read_group_mode)
            .unwrap_or_default();
        let dfs_read_group_bytes = cli
            .dfs_read_group_bytes
            .or(file.dfs_read_group_bytes)
            .unwrap_or(8 * 1024 * 1024);
        let dfs_read_group_max_inflight = cli
            .dfs_read_group_max_inflight
            .or(file.dfs_read_group_max_inflight)
            .unwrap_or(1);
        let dfs_read_group_max_descriptors = cli
            .dfs_read_group_max_descriptors
            .or(file.dfs_read_group_max_descriptors)
            .unwrap_or(128);
        let dfs_read_group_host_budget_bytes = cli
            .dfs_read_group_host_budget_bytes
            .or(file.dfs_read_group_host_budget_bytes)
            .unwrap_or(1024 * 1024 * 1024);
        if dfs_read_max_ops_per_batch == 0
            || dfs_read_max_inflight_bytes == 0
            || dfs_source_cache_max_ms > 60_000
        {
            return Err(invalid(
                "DFS read budgets must be non-zero and source cache TTL must be <= 60000 ms",
            ));
        }
        let fuse_threads = cli.fuse_threads.or(file.fuse_threads).unwrap_or(4);
        let fuse_workers = cli.fuse_workers.or(file.fuse_workers).unwrap_or(32);
        let fuse_max_tasks = cli
            .fuse_max_tasks
            .or(file.fuse_max_tasks)
            .unwrap_or(usize::from(fuse_workers) * 8);
        let dfs_read_pipeline = file.dfs_read_pipeline.unwrap_or_default();
        let dfs_read_pipeline = dfs_read_pipeline.validate()?;
        let dfs_fuse_entry_ttl_ms = cli
            .dfs_fuse_entry_ttl_ms
            .or(file.dfs_fuse_entry_ttl_ms)
            .unwrap_or(0);
        let dfs_fuse_attr_ttl_ms = cli
            .dfs_fuse_attr_ttl_ms
            .or(file.dfs_fuse_attr_ttl_ms)
            .unwrap_or(0);
        let fuse_max_readahead_bytes = cli
            .fuse_max_readahead_bytes
            .or(file.fuse_max_readahead_bytes)
            .unwrap_or(16 * 1024 * 1024);
        if !(1..=64).contains(&fuse_threads)
            || !(1..=1024).contains(&fuse_workers)
            || !(1..=65_536).contains(&fuse_max_tasks)
            || dfs_fuse_entry_ttl_ms > 60_000
            || dfs_fuse_attr_ttl_ms > 60_000
            || fuse_max_readahead_bytes > 16 * 1024 * 1024
        {
            return Err(invalid(
                "FUSE threads must be 1..=64, workers 1..=1024, accepted tasks 1..=65536, DFS TTLs <= 60000 ms and read-ahead <= 16 MiB",
            ));
        }
        let dfs_version_cache_entries = cli
            .dfs_version_cache_entries
            .or(file.dfs_version_cache_entries)
            .unwrap_or(4096);
        if !(1..=65536).contains(&dfs_version_cache_entries) {
            return Err(invalid("DFS version cache entries must be 1..=65536"));
        }
        let dfs_version_cache_bytes = cli
            .dfs_version_cache_bytes
            .or(file.dfs_version_cache_bytes)
            .unwrap_or(32 * 1024 * 1024);
        if !(1..=1024 * 1024 * 1024).contains(&dfs_version_cache_bytes) {
            return Err(invalid("DFS version cache bytes must be 1..=1073741824"));
        }
        #[cfg(feature = "dfs")]
        if role == Role::Node {
            let minimum =
                crate::node::vfs::dfs::version_cache_minimum_bytes(dfs_version_cache_entries)
                    .ok_or_else(|| invalid("DFS version cache minimum byte estimate overflows"))?;
            if usize::try_from(dfs_version_cache_bytes)
                .ok()
                .is_none_or(|bytes| bytes < minimum)
            {
                return Err(invalid(format!(
                    "DFS version cache bytes {dfs_version_cache_bytes} is below minimum {minimum} for {dfs_version_cache_entries} entries",
                )));
            }
        }
        let dfs_outboard_work_wait_ms = cli
            .dfs_outboard_work_wait_ms
            .or(file.dfs_outboard_work_wait_ms)
            .unwrap_or(2);
        if dfs_outboard_work_wait_ms > 50 {
            return Err(invalid("DFS outboard work wait must be 0..=50 ms"));
        }
        let dfs_catalog_commit_queue = cli
            .dfs_catalog_commit_queue
            .or(file.dfs_catalog_commit_queue)
            .unwrap_or(256);
        if !(1..=256).contains(&dfs_catalog_commit_queue) {
            return Err(invalid("DFS catalog commit queue must be 1..=256"));
        }
        let dfs_write_queue_bytes = cli
            .dfs_write_queue_bytes
            .or(file.dfs_write_queue_bytes)
            .unwrap_or(2 * 1024 * 1024 * 1024);
        if !(8 * 1024 * 1024..=1024 * 1024 * 1024 * 1024).contains(&dfs_write_queue_bytes)
            || usize::try_from(dfs_write_queue_bytes).is_err()
        {
            return Err(invalid(
                "DFS write queue bytes must be 8MiB..=1TiB and fit usize",
            ));
        }
        let dfs_dirty_budget_bytes = cli
            .dfs_dirty_budget_bytes
            .or(file.dfs_dirty_budget_bytes)
            .unwrap_or(64 * 1024 * 1024);
        if !(4 * 1024 * 1024..=64 * 1024 * 1024).contains(&dfs_dirty_budget_bytes) {
            return Err(invalid("DFS native dirty budget bytes must be 4..=64MiB"));
        }
        let dfs_gc_grace_ms = cli
            .dfs_gc_grace_ms
            .or(file.dfs_gc_grace_ms)
            .unwrap_or(1_000);
        if dfs_gc_grace_ms > 600_000 {
            return Err(invalid("DFS GC grace must be 0..=600000ms"));
        }
        let dfs_unpublished_descriptors = cli
            .dfs_unpublished_descriptors
            .or(file.dfs_unpublished_descriptors)
            .unwrap_or(1024);
        if !(1..=1024).contains(&dfs_unpublished_descriptors) {
            return Err(invalid("DFS unpublished descriptors must be 1..=1024"));
        }
        let dfs_upload_inflight_chunks = cli
            .dfs_upload_inflight_chunks
            .or(file.dfs_upload_inflight_chunks)
            .unwrap_or(8);
        if !(1..=8).contains(&dfs_upload_inflight_chunks) {
            return Err(invalid("DFS native upload inflight chunks must be 1..=8"));
        }
        let dfs_background_commit_ms = cli
            .dfs_background_commit_ms
            .or(file.dfs_background_commit_ms)
            .unwrap_or(5000);
        if !(1..=60000).contains(&dfs_background_commit_ms) {
            return Err(invalid(
                "DFS native background commit period must be 1..=60000ms",
            ));
        }
        let dfs_sidecar_install_queue = cli
            .dfs_sidecar_install_queue
            .or(file.dfs_sidecar_install_queue)
            .unwrap_or(64);
        if !(1..=64).contains(&dfs_sidecar_install_queue) {
            return Err(invalid("DFS sidecar install queue must be 1..=64"));
        }
        let dfs_fetch_max_inflight_objects = cli
            .dfs_fetch_max_inflight_objects
            .or(file.dfs_fetch_max_inflight_objects)
            .unwrap_or(8);
        if !(1..=8).contains(&dfs_fetch_max_inflight_objects) {
            return Err(invalid("DFS fetch inflight objects must be 1..=8"));
        }
        let dfs_readahead_max_bytes = cli
            .dfs_readahead_max_bytes
            .or(file.dfs_readahead_max_bytes)
            .unwrap_or(0);
        if dfs_readahead_max_bytes > 32 * 1024 * 1024 {
            return Err(invalid("DFS readahead distance must be 0..=32MiB"));
        }
        let dfs_peer_serve_resident_objects = cli
            .dfs_peer_serve_resident_objects
            .or(file.dfs_peer_serve_resident_objects)
            .unwrap_or(2);
        if !(1..=16).contains(&dfs_peer_serve_resident_objects) {
            return Err(invalid("DFS Peer serve resident objects must be 1..=16"));
        }
        let dfs_peer_serve_payload_bytes = cli
            .dfs_peer_serve_payload_bytes
            .or(file.dfs_peer_serve_payload_bytes)
            .unwrap_or(16 * 1024 * 1024);
        if !(4 * 1024 * 1024..=64 * 1024 * 1024).contains(&dfs_peer_serve_payload_bytes) {
            return Err(invalid("DFS Peer serve payload bytes must be 4..=64 MiB"));
        }
        let dfs_peer_serve_wait_ms = cli
            .dfs_peer_serve_wait_ms
            .or(file.dfs_peer_serve_wait_ms)
            .unwrap_or(20);
        if dfs_peer_serve_wait_ms > 50 {
            return Err(invalid("DFS Peer serve wait must be 0..=50 ms"));
        }
        let log_defaults = crate::meta::store::journal::LogLimits::default();
        let maintenance = crate::meta::store::journal::MaintenancePolicy::default();
        let history = crate::meta::store::HistoryLimits::default();
        let dfs_crc32c_writes = cli
            .dfs_crc32c_writes
            .or(file.dfs_crc32c_writes)
            .unwrap_or(role == Role::Node && dfs && file.dfs_external_backend.is_none());
        let dfs_optimistic_gc_commit = cli
            .dfs_optimistic_gc_commit
            .or(file.dfs_optimistic_gc_commit)
            .unwrap_or(dfs_crc32c_writes);
        if !(1024 * 1024..=128 * 1024 * 1024).contains(&dfs_read_group_bytes)
            || !dfs_read_group_bytes.is_multiple_of(4096)
            || !(1..=2).contains(&dfs_read_group_max_inflight)
            || !(1..=128).contains(&dfs_read_group_max_descriptors)
            || dfs_read_group_host_budget_bytes < dfs_read_group_bytes
        {
            return Err(invalid(
                "DFS read group requires 1..=128 MiB 4 KiB-aligned bytes, 1..=2 inflight, 1..=128 descriptors and a sufficient Host budget",
            ));
        }
        let cfg = Self {
            grpc_listen: cli.grpc_listen.or(file.grpc_listen).unwrap_or_else(|| {
                ([127, 0, 0, 1], if role == Role::Meta { 7400 } else { 7500 }).into()
            }),
            rest_listen: cli.rest_listen.or(file.rest_listen).unwrap_or_else(|| {
                ([127, 0, 0, 1], if role == Role::Meta { 7401 } else { 7501 }).into()
            }),
            meta_endpoint: cli.meta_endpoint.or(file.meta_endpoint),
            meta_store: cli
                .meta_store
                .or(file.meta_store)
                .unwrap_or(MetaStoreBackend::LocalFile),
            meta_record_max_bytes: cli
                .meta_record_max_bytes
                .or(file.meta_record_max_bytes)
                .unwrap_or(log_defaults.record_max_bytes),
            meta_batch_max_bytes: cli
                .meta_batch_max_bytes
                .or(file.meta_batch_max_bytes)
                .unwrap_or(log_defaults.batch_max_bytes),
            meta_wal_max_bytes: cli
                .meta_wal_max_bytes
                .or(file.meta_wal_max_bytes)
                .unwrap_or(log_defaults.wal_max_bytes),
            meta_checkpoint_wal_bytes: cli
                .meta_checkpoint_wal_bytes
                .or(file.meta_checkpoint_wal_bytes)
                .unwrap_or(maintenance.wal_bytes),
            meta_checkpoint_wal_records: cli
                .meta_checkpoint_wal_records
                .or(file.meta_checkpoint_wal_records)
                .unwrap_or(maintenance.wal_records),
            meta_checkpoint_interval_ms: cli
                .meta_checkpoint_interval_ms
                .or(file.meta_checkpoint_interval_ms)
                .unwrap_or(60_000),
            meta_wal_segment_bytes: cli
                .meta_wal_segment_bytes
                .or(file.meta_wal_segment_bytes)
                .unwrap_or(log_defaults.segment_bytes),
            meta_request_retention_ms: cli
                .meta_request_retention_ms
                .or(file.meta_request_retention_ms)
                .unwrap_or(86_400_000),
            meta_request_outcomes_max: cli
                .meta_request_outcomes_max
                .or(file.meta_request_outcomes_max)
                .unwrap_or(history.request_count),
            meta_request_bytes_max: cli
                .meta_request_bytes_max
                .or(file.meta_request_bytes_max)
                .unwrap_or(history.request_bytes),
            meta_watch_events_max: cli
                .meta_watch_events_max
                .or(file.meta_watch_events_max)
                .unwrap_or(history.event_count),
            meta_watch_bytes_max: cli
                .meta_watch_bytes_max
                .or(file.meta_watch_bytes_max)
                .unwrap_or(history.event_bytes),
            meta_watch_retention_ms: cli
                .meta_watch_retention_ms
                .or(file.meta_watch_retention_ms)
                .unwrap_or(3_600_000),
            allow_volatile_meta: cli
                .allow_volatile_meta
                .or(file.allow_volatile_meta)
                .unwrap_or(false),
            ownerfs_mkdir_load_balance: cli
                .ownerfs_mkdir_load_balance
                .or(file.ownerfs_mkdir_load_balance)
                .unwrap_or(false),
            peer_endpoint: cli.peer_endpoint.or(file.peer_endpoint),
            advertise_endpoint: cli.advertise_endpoint.or(file.advertise_endpoint),
            tls_encryption_enabled: cli
                .tls_encryption_enabled
                .or(file.tls_encryption_enabled)
                .unwrap_or(false),
            tls_ca_certificate: cli.tls_ca_certificate.or(file.tls_ca_certificate),
            tls_identity_certificate: cli
                .tls_identity_certificate
                .or(file.tls_identity_certificate),
            tls_identity_private_key: cli
                .tls_identity_private_key
                .or(file.tls_identity_private_key),
            tls_server_name: cli.tls_server_name.or(file.tls_server_name),
            trusted_node_certs: file.trusted_node_certs.unwrap_or_default(),
            data_dir: cli
                .data_dir
                .or(file.data_dir)
                .unwrap_or_else(|| PathBuf::from(format!("/tmp/afs-{id}/data"))),
            uds_path: cli
                .uds_path
                .or(file.uds_path)
                .unwrap_or_else(|| PathBuf::from(format!("/tmp/afs-{id}/local.sock"))),
            id,
            ownerfs,
            dfs,
            data_mode,
            rdma_device: cli.rdma_device.or(file.rdma_device),
            hixl: file.hixl.unwrap_or_default(),
            ownerfs_mount: cli.ownerfs_mount.or(file.ownerfs_mount),
            experimental_native_workspace: cli
                .experimental_native_workspace
                .or(file.experimental_native_workspace)
                .unwrap_or(false),
            native_workspace: file.native_workspace,
            experimental_ownerfs_workspace_bind: cli
                .experimental_ownerfs_workspace_bind
                .or(file.experimental_ownerfs_workspace_bind)
                .unwrap_or(false),
            ownerfs_workspace_bind: file.ownerfs_workspace_bind,
            ownerfs_workspace_egress_root: cli
                .ownerfs_workspace_egress_root
                .or(file.ownerfs_workspace_egress_root),
            dfs_mount: cli.dfs_mount.or(file.dfs_mount),
            fuse_threads,
            fuse_clone_fd: cli.fuse_clone_fd.or(file.fuse_clone_fd).unwrap_or(true),
            fuse_workers,
            fuse_max_tasks,
            dfs_read_pipeline,
            dfs_fuse_keep_cache: cli
                .dfs_fuse_keep_cache
                .or(file.dfs_fuse_keep_cache)
                .unwrap_or(true),
            dfs_fuse_entry_ttl_ms,
            dfs_fuse_attr_ttl_ms,
            fuse_max_readahead_bytes,
            dfs_version_cache_entries,
            dfs_version_cache_bytes,
            dfs_outboard_work_wait_ms,
            dfs_catalog_commit_queue,
            dfs_write_queue_bytes,
            dfs_dirty_budget_bytes,
            dfs_gc_grace_ms,
            dfs_unpublished_descriptors,
            dfs_upload_inflight_chunks,
            dfs_background_commit_ms,
            dfs_sidecar_install_queue,
            dfs_fetch_max_inflight_objects,
            dfs_readahead_max_bytes,
            dfs_peer_serve_resident_objects,
            dfs_peer_serve_payload_bytes,
            dfs_peer_serve_wait_ms,
            dfs_external_backend: file.dfs_external_backend,
            dfs_peer_cache: file.dfs_peer_cache,
            dfs_write_buffer_idle_bytes: file
                .dfs_write_buffer_idle_bytes
                .unwrap_or(128 * 1024 * 1024),
            dfs_fuse_read_mode: cli
                .dfs_fuse_read_mode
                .or(file.dfs_fuse_read_mode)
                .unwrap_or_default(),
            dfs_desired_copies,
            dfs_sync_required_copies,
            dfs_min_distinct_nodes,
            dfs_min_distinct_failure_domains,
            dfs_local_copy,
            dfs_read_max_ops_per_batch,
            dfs_read_max_inflight_bytes,
            dfs_source_cache_max_ms,
            dfs_source_lookahead_chunks,
            dfs_source_cache_bytes,
            dfs_read_crc_mode,
            dfs_native_open_read_authority: cli
                .dfs_native_open_read_authority
                .or(file.dfs_native_open_read_authority)
                .unwrap_or(false),
            dfs_read_diagnostics_enabled: cli
                .dfs_read_diagnostics_enabled
                .or(file.dfs_read_diagnostics_enabled)
                .unwrap_or(false),
            dfs_crc32c_writes,
            dfs_optimistic_gc_commit,
            dfs_read_group_mode,
            dfs_read_group_bytes,
            dfs_read_group_max_inflight,
            dfs_read_group_max_descriptors,
            dfs_read_group_host_budget_bytes,
            timeout_ms: cli.timeout_ms.or(file.timeout_ms).unwrap_or(5000),
            log_level: cli
                .log_level
                .or(file.log_level)
                .unwrap_or_else(|| "info".into()),
            trace_enabled: cli.trace_enabled.or(file.trace_enabled).unwrap_or(false),
            trace_endpoint: cli
                .trace_endpoint
                .or(file.trace_endpoint)
                .unwrap_or_else(|| "http://127.0.0.1:4317".into()),
            trace_sample_ratio: cli
                .trace_sample_ratio
                .or(file.trace_sample_ratio)
                .unwrap_or(0.01),
            print_config: cli.print_config,
        };
        if let Some(external) = &cfg.dfs_external_backend {
            external.validate()?;
            if !cfg.dfs
                || cfg.dfs_desired_copies != 1
                || cfg.dfs_sync_required_copies != 1
                || cfg.dfs_min_distinct_nodes != 1
                || cfg.dfs_min_distinct_failure_domains != 1
                || cfg.dfs_local_copy != "required"
            {
                return Err(invalid(
                    "external backend requires DFS with fixed single-copy defaults; Node replication combinations are unsupported",
                ));
            }
        }
        if cfg.dfs_write_buffer_idle_bytes > (1_u64 << 40)
            || usize::try_from(cfg.dfs_write_buffer_idle_bytes).is_err()
        {
            return Err(invalid(
                "dfs_write_buffer_idle_bytes must be in 0..=1 TiB and fit this platform; it limits idle capacity only",
            ));
        }
        if let Some(cache) = &cfg.dfs_peer_cache {
            let overlaps = |other: &std::path::Path| {
                cache.directory.starts_with(other) || other.starts_with(&cache.directory)
            };
            if role != Role::Node
                || !cfg.dfs
                || cfg.dfs_external_backend.is_some()
                || !cache.directory.is_absolute()
                || cache.directory.parent().is_none()
                || cache.directory.components().any(|part| {
                    matches!(
                        part,
                        std::path::Component::ParentDir | std::path::Component::CurDir
                    )
                })
                || cache.capacity_bytes == 0
                || cache.capacity_bytes > MAX_DFS_EXTERNAL_CACHE_CAPACITY_BYTES
                || overlaps(&cfg.data_dir)
                || [&cfg.dfs_mount, &cfg.ownerfs_mount]
                    .into_iter()
                    .flatten()
                    .any(|mount| overlaps(mount))
            {
                return Err(invalid(
                    "dfs_peer_cache requires a native DFS Node, a separate absolute directory without traversal, and capacity in 1..=1 TiB; external-backend Peer caching is not enabled",
                ));
            }
        }
        if cfg.timeout_ms == 0 || cfg.timeout_ms > 300_000 {
            return Err(invalid("timeout_ms must be in 1..300000"));
        }
        if !(0.0..=1.0).contains(&cfg.trace_sample_ratio) {
            return Err(invalid("trace_sample_ratio must be 0..1"));
        }
        afs_logging::parse_level(&cfg.log_level).map_err(|e| invalid(e.to_string()))?;
        for endpoint in [
            &cfg.meta_endpoint,
            &cfg.peer_endpoint,
            &cfg.advertise_endpoint,
        ]
        .into_iter()
        .flatten()
        {
            if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
                return Err(invalid("gRPC endpoint must be an http(s) URI"));
            }
            tonic::transport::Endpoint::from_shared(endpoint.clone())
                .map_err(|e| invalid(e.to_string()))?;
        }
        if !cfg.uds_path.is_absolute() || !cfg.data_dir.is_absolute() {
            return Err(invalid("uds_path and data_dir must be absolute"));
        }
        for mount in [&cfg.ownerfs_mount, &cfg.dfs_mount].into_iter().flatten() {
            if !mount.is_absolute() {
                return Err(invalid("filesystem mount paths must be absolute"));
            }
        }
        if cfg.ownerfs_mount.is_some() && !cfg.ownerfs {
            return Err(invalid("ownerfs_mount requires fs=ownerfs or fs=all"));
        }
        if cfg.experimental_ownerfs_workspace_bind {
            if role != Role::Node || !cfg.ownerfs || cfg.ownerfs_mount.is_none() {
                return Err(invalid(
                    "experimental_ownerfs_workspace_bind requires Node OwnerFs with a mount",
                ));
            }
            if cfg.experimental_native_workspace {
                return Err(invalid(
                    "experimental_ownerfs_workspace_bind cannot be enabled with experimental_native_workspace",
                ));
            }
            // The egress root must exist as a stable, admin-configured native
            // path: absolute, no parent traversal, and outside both the FUSE
            // mount and the private data directory so bind targets never cover
            // the authoritative FUSE tree (D1/D6).
            let egress_root = cfg.ownerfs_workspace_egress_root.as_ref().ok_or_else(|| {
                invalid(
                    "experimental_ownerfs_workspace_bind requires ownerfs_workspace_egress_root",
                )
            })?;
            if !egress_root.is_absolute()
                || egress_root
                    .components()
                    .any(|component| matches!(component, Component::ParentDir))
            {
                return Err(invalid(
                    "ownerfs_workspace_egress_root must be absolute and contain no parent traversal",
                ));
            }
            if egress_root.starts_with(cfg.ownerfs_mount.as_ref().expect("mount checked"))
                || egress_root.starts_with(&cfg.data_dir)
            {
                return Err(invalid(
                    "ownerfs_workspace_egress_root must be outside OwnerFs mount and data directories",
                ));
            }
            // Multi-workspace bindmount is triggered dynamically by mkdir.
            // The legacy [ownerfs_workspace_bind] section is optional and only
            // used for pre-declared workspace names if present.
            if let Some(bind) = cfg.ownerfs_workspace_bind.as_ref() {
                validate_ownerfs_workspace_name(&bind.workspace)?;
            }
        }
        if cfg.experimental_native_workspace {
            if role != Role::Node || !cfg.ownerfs || cfg.ownerfs_mount.is_none() {
                return Err(invalid(
                    "experimental_native_workspace requires Node OwnerFs with a mount",
                ));
            }
            let native = cfg.native_workspace.as_ref().ok_or_else(|| {
                invalid(
                "experimental_native_workspace requires administrator native_workspace settings"
            )
            })?;
            for path in [&native.control_dir, &native.runtime, &native.rootfs] {
                if !path.is_absolute()
                    || path
                        .components()
                        .any(|c| matches!(c, std::path::Component::ParentDir))
                {
                    return Err(invalid(
                        "native workspace paths must be absolute and contain no parent traversal",
                    ));
                }
            }
            for path in [&native.control_dir, &native.rootfs, &native.runtime] {
                if path.starts_with(cfg.ownerfs_mount.as_ref().expect("native mount checked"))
                    || path.starts_with(&cfg.data_dir)
                {
                    return Err(invalid(
                        "native controller/runtime/rootfs must be outside OwnerFs mount and data directories",
                    ));
                }
            }
            if native.workload_uid == 0 || native.workload_gid == 0 {
                return Err(invalid(
                    "native workspace workload uid/gid must be non-root",
                ));
            }
            if native.control_dir == native.rootfs
                || native.control_dir.starts_with(&native.rootfs)
                || native.rootfs.starts_with(&native.control_dir)
            {
                return Err(invalid(
                    "native workspace control directory and rootfs must be disjoint",
                ));
            }
            validate_native_workspace_command(&native.idle_command, "idle_command")?;
            validate_native_workspace_command(&native.identity_command, "identity_command")?;
        }
        if cfg.dfs_mount.is_some() && !cfg.dfs {
            return Err(invalid("dfs_mount requires fs=dfs or fs=all"));
        }
        if cfg.ownerfs_mount.is_some() && cfg.ownerfs_mount == cfg.dfs_mount {
            return Err(invalid(
                "ownerfs_mount and dfs_mount must be different paths",
            ));
        }
        let tls_count = [
            cfg.tls_ca_certificate.is_some(),
            cfg.tls_identity_certificate.is_some(),
            cfg.tls_identity_private_key.is_some(),
            cfg.tls_server_name.is_some(),
        ]
        .into_iter()
        .filter(|value| *value)
        .count();
        if cfg.tls_encryption_enabled && tls_count == 0 {
            return Err(invalid(
                "tls_encryption_enabled requires all four tls_* identity fields",
            ));
        }
        if tls_count != 0 && tls_count != 4 {
            return Err(invalid("all four tls_* fields are required together"));
        }
        for path in cfg.trusted_node_certs.values() {
            if !path.is_absolute() {
                return Err(invalid("trusted_node_certs paths must be absolute"));
            }
        }
        cfg.validate_crc32c_writes()?;
        if cfg.dfs_read_crc_mode != DfsReadCrcMode::Both
            && (!cfg.dfs || cfg.dfs_external_backend.is_some() || cfg.data_mode != "grpc")
        {
            return Err(invalid(
                "dfs_read_crc_mode overrides require native DFS grpc",
            ));
        }
        cfg.meta_log_limits()?;
        cfg.meta_maintenance_policy()?;
        cfg.meta_history_limits()?;
        cfg.hixl.validate(
            role,
            HixlDeployment {
                dfs: cfg.dfs,
                ownerfs: cfg.ownerfs,
                grpc_data: cfg.data_mode == "grpc",
                mutual_tls: tls_count == 4,
                bound_peers: !cfg.trusted_node_certs.is_empty(),
            },
        )?;
        Ok(cfg)
    }

    pub(crate) fn validate_crc32c_writes(&self) -> Result<()> {
        if self.dfs_crc32c_writes
            && (!self.dfs || self.dfs_external_backend.is_some() || !self.dfs_optimistic_gc_commit)
        {
            return Err(invalid(
                "dfs_crc32c_writes requires native storage and dfs_optimistic_gc_commit",
            ));
        }
        Ok(())
    }

    pub(crate) fn meta_log_limits(&self) -> Result<crate::meta::store::journal::LogLimits> {
        if self.meta_wal_segment_bytes > self.meta_wal_max_bytes {
            return Err(invalid(
                "meta_wal_segment_bytes must not exceed meta_wal_max_bytes",
            ));
        }
        crate::meta::store::journal::LogLimits {
            record_max_bytes: self.meta_record_max_bytes,
            batch_max_bytes: self.meta_batch_max_bytes,
            wal_max_bytes: self.meta_wal_max_bytes,
            segment_bytes: self.meta_wal_segment_bytes,
            ..Default::default()
        }
        .validate()
        .map_err(|error| invalid(error.to_string()))
    }

    pub(crate) fn meta_maintenance_policy(
        &self,
    ) -> Result<crate::meta::store::journal::MaintenancePolicy> {
        use std::time::Duration;
        let defaults = crate::meta::store::journal::MaintenancePolicy::default();
        crate::meta::store::journal::MaintenancePolicy {
            wal_bytes: self.meta_checkpoint_wal_bytes,
            wal_records: self.meta_checkpoint_wal_records,
            interval: Duration::from_millis(self.meta_checkpoint_interval_ms),
            // Short test/operator intervals must also shorten the polling period.
            poll: defaults
                .poll
                .min(Duration::from_millis(self.meta_checkpoint_interval_ms)),
            ..defaults
        }
        .validate()
        .map_err(|error| invalid(error.to_string()))
    }

    pub(crate) fn meta_history_limits(&self) -> Result<crate::meta::store::HistoryLimits> {
        use std::time::Duration;
        crate::meta::store::HistoryLimits {
            request_count: self.meta_request_outcomes_max,
            request_bytes: self.meta_request_bytes_max,
            request_retention: Duration::from_millis(self.meta_request_retention_ms),
            event_count: self.meta_watch_events_max,
            event_bytes: self.meta_watch_bytes_max,
            event_retention: Duration::from_millis(self.meta_watch_retention_ms),
            ..Default::default()
        }
        .validate()
        .map_err(|error| invalid(error.to_string()))
    }

    /// gRPC builders共用安全配置；只有配置了完整证书集合才启用 mTLS。
    /// Meta authority and node-to-node filesystem RPCs also require
    /// `trusted_node_certs` to bind a verified certificate to one Node ID.
    pub fn tls_config(&self) -> afs_transport::grpc::TlsConfig {
        match (
            &self.tls_ca_certificate,
            &self.tls_identity_certificate,
            &self.tls_identity_private_key,
            &self.tls_server_name,
        ) {
            (Some(ca), Some(cert), Some(key), Some(server_name)) => {
                afs_transport::grpc::TlsConfig::MutualTls {
                    encryption_enabled: self.tls_encryption_enabled,
                    ca_certificate: ca.clone(),
                    identity_certificate: cert.clone(),
                    identity_private_key: key.clone(),
                    server_name: server_name.clone(),
                }
            }
            _ => afs_transport::grpc::TlsConfig::Disabled,
        }
    }
}
fn invalid(message: impl Into<String>) -> Error {
    afs_error::Error::coded(afs_error::CONFIG_INVALID, message)
}

fn validate_ownerfs_workspace_name(workspace: &str) -> Result<()> {
    if workspace.is_empty()
        || workspace == "."
        || workspace == ".."
        || workspace.contains('/')
        || workspace.contains('\0')
    {
        return Err(invalid(
            "ownerfs_workspace_bind.workspace must be one first-level workspace name",
        ));
    }
    Ok(())
}

fn validate_native_workspace_command(argv: &[String], field: &str) -> Result<()> {
    const MAX_ARGS: usize = 32;
    const MAX_ARG_BYTES: usize = 4096;
    if argv.is_empty() || argv.len() > MAX_ARGS {
        return Err(invalid(format!(
            "native_workspace.{field} must contain 1..{MAX_ARGS} arguments"
        )));
    }
    for arg in argv {
        if arg.is_empty() || arg.len() > MAX_ARG_BYTES || arg.contains('\0') {
            return Err(invalid(format!(
                "native_workspace.{field} arguments must be nonempty, <= {MAX_ARG_BYTES} bytes, and contain no NUL"
            )));
        }
    }
    let program = Path::new(&argv[0]);
    if !program.is_absolute()
        || program.components().any(|component| {
            matches!(
                component,
                Component::CurDir | Component::ParentDir | Component::Prefix(_)
            )
        })
    {
        return Err(invalid(format!(
            "native_workspace.{field} executable must be absolute and contain no traversal"
        )));
    }
    if program.file_name().is_none() {
        return Err(invalid(format!(
            "native_workspace.{field} executable must name a file"
        )));
    }
    Ok(())
}

/// Normalize a configured certificate file to the leaf DER identity exposed by tonic.
/// Explicit service launchers reuse this parser instead of maintaining a second decoder.
pub fn read_certificate_der(path: &std::path::Path) -> Result<Vec<u8>> {
    let bytes = std::fs::read(path).map_err(|source| {
        invalid(format!(
            "failed to read trusted node cert {}: {source}",
            path.display()
        ))
    })?;
    if let Some(der) = first_pem_certificate_der(&bytes)? {
        Ok(der)
    } else if bytes.is_empty() {
        Err(invalid(format!(
            "trusted node cert {} is empty",
            path.display()
        )))
    } else {
        Ok(bytes)
    }
}

fn first_pem_certificate_der(bytes: &[u8]) -> Result<Option<Vec<u8>>> {
    let text = match std::str::from_utf8(bytes) {
        Ok(text) => text,
        Err(_) => return Ok(None),
    };
    const BEGIN: &str = "-----BEGIN CERTIFICATE-----";
    const END: &str = "-----END CERTIFICATE-----";
    let Some(begin) = text.find(BEGIN) else {
        return Ok(None);
    };
    let body_start = begin + BEGIN.len();
    let Some(relative_end) = text[body_start..].find(END) else {
        return Err(invalid("PEM certificate is missing END CERTIFICATE"));
    };
    let body = text[body_start..body_start + relative_end]
        .chars()
        .filter(|c| !c.is_ascii_whitespace())
        .collect::<String>();
    Ok(Some(decode_base64(&body)?))
}

fn decode_base64(input: &str) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(input.len() * 3 / 4);
    let mut buffer = 0u32;
    let mut bits = 0u8;
    let mut padding = false;
    for byte in input.bytes() {
        let value = match byte {
            b'A'..=b'Z' => u32::from(byte - b'A'),
            b'a'..=b'z' => u32::from(byte - b'a' + 26),
            b'0'..=b'9' => u32::from(byte - b'0' + 52),
            b'+' => 62,
            b'/' => 63,
            b'=' => {
                padding = true;
                continue;
            }
            _ => return Err(invalid("PEM certificate contains invalid base64")),
        };
        if padding {
            return Err(invalid("PEM certificate has data after base64 padding"));
        }
        buffer = (buffer << 6) | value;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(((buffer >> bits) & 0xff) as u8);
        }
    }
    if out.is_empty() {
        return Err(invalid("PEM certificate has empty base64 body"));
    }
    Ok(out)
}

#[cfg(test)]
mod hixl_config_tests {
    use super::{
        HixlConfig, HixlDeployment, HixlDeviceConfig, HixlDfsReadMode, HixlOwnerFsReadMode, Role,
    };

    fn dfs_deployment(dfs: bool, mutual_tls: bool, bound_peers: bool) -> HixlDeployment {
        HixlDeployment {
            dfs,
            ownerfs: false,
            grpc_data: true,
            mutual_tls,
            bound_peers,
        }
    }

    fn ownerfs_deployment() -> HixlDeployment {
        HixlDeployment {
            dfs: false,
            ownerfs: true,
            grpc_data: true,
            mutual_tls: true,
            bound_peers: true,
        }
    }

    fn ownerfs_config(mode: HixlOwnerFsReadMode) -> HixlConfig {
        HixlConfig {
            ownerfs_read_mode: mode,
            engine_id: "127.0.0.1:7920".into(),
            ..Default::default()
        }
    }

    #[test]
    fn ownerfs_mode_is_independent_of_dfs_mode() {
        let config = ownerfs_config(HixlOwnerFsReadMode::Preferred);
        assert!(config.validate(Role::Node, ownerfs_deployment()).is_ok());
        assert_eq!(config.dfs_read_mode, HixlDfsReadMode::Off);
    }

    #[test]
    fn ownerfs_mode_requires_node_ownerfs_grpc_and_mutual_tls() {
        let config = ownerfs_config(HixlOwnerFsReadMode::Preferred);
        let deployment = ownerfs_deployment();
        assert!(
            config
                .validate(
                    Role::Node,
                    HixlDeployment {
                        ownerfs: false,
                        ..deployment
                    }
                )
                .is_err()
        );
        assert!(
            config
                .validate(
                    Role::Node,
                    HixlDeployment {
                        grpc_data: false,
                        ..deployment
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
                        ..deployment
                    }
                )
                .is_err()
        );
        assert!(config.validate(Role::Meta, deployment).is_err());
        let unaddressed = HixlConfig {
            engine_id: String::new(),
            ..config
        };
        assert!(unaddressed.validate(Role::Node, deployment).is_err());
    }

    #[test]
    fn ownerfs_window_must_fit_both_slot_pools_and_the_budget() {
        let deployment = ownerfs_deployment();
        let valid = ownerfs_config(HixlOwnerFsReadMode::Required);
        if cfg!(feature = "hixl") {
            assert!(valid.validate(Role::Node, deployment).is_ok());
        } else {
            assert!(valid.validate(Role::Node, deployment).is_err());
        }
        let preferred = ownerfs_config(HixlOwnerFsReadMode::Preferred);
        let cases = [
            HixlConfig {
                ownerfs_window_bytes: 6 * 1024 * 1024,
                ..preferred.clone()
            },
            HixlConfig {
                ownerfs_window_bytes: 128 * 1024 * 1024,
                ..preferred.clone()
            },
            HixlConfig {
                receive_slots: 2,
                ..preferred.clone()
            },
            HixlConfig {
                ownerfs_max_inflight: 3,
                ..preferred.clone()
            },
            HixlConfig {
                ownerfs_max_inflight: 2,
                ownerfs_host_budget_bytes: 16 * 1024 * 1024,
                ..preferred.clone()
            },
            HixlConfig {
                ownerfs_min_file_bytes: 2 * 1024 * 1024 * 1024 * 1024,
                ..preferred.clone()
            },
        ];
        for config in cases {
            assert!(config.validate(Role::Node, deployment).is_err());
        }
        let prefetch = HixlConfig {
            ownerfs_max_inflight: 2,
            ..preferred
        };
        assert!(prefetch.validate(Role::Node, deployment).is_ok());
    }

    #[test]
    fn pooled_sessions_split_the_export_slots_of_one_window() {
        let deployment = ownerfs_deployment();
        let pooled = |sessions, export_slots| HixlConfig {
            ownerfs_sessions_per_home: sessions,
            export_slots,
            ..ownerfs_config(HixlOwnerFsReadMode::Preferred)
        };
        // 16 MiB windows need four 4 MiB slots per session.
        assert!(pooled(4, 16).validate(Role::Node, deployment).is_ok());
        assert!(pooled(4, 12).validate(Role::Node, deployment).is_err());
        assert!(pooled(2, 8).validate(Role::Node, deployment).is_ok());
        assert!(pooled(0, 16).validate(Role::Node, deployment).is_err());
        assert!(pooled(5, 64).validate(Role::Node, deployment).is_err());
        assert_eq!(HixlConfig::default().ownerfs_sessions_per_home, 1);
    }

    #[test]
    fn ownerfs_bounds_do_not_apply_while_ownerfs_mode_is_off() {
        let config = HixlConfig {
            receive_slots: 2,
            export_slots: 2,
            ..Default::default()
        };
        assert!(
            config
                .validate(Role::Node, dfs_deployment(false, false, false))
                .is_ok()
        );
    }

    #[test]
    fn ownerfs_defaults_are_off_and_parse_from_toml() {
        let config = HixlConfig::default();
        assert_eq!(config.ownerfs_read_mode, HixlOwnerFsReadMode::Off);
        assert_eq!(config.ownerfs_window_bytes, 16 * 1024 * 1024);
        assert_eq!(config.ownerfs_max_inflight, 1);
        let parsed: HixlConfig = toml::from_str(
            r#"
                engine_id = "127.0.0.1:7920"
                ownerfs_read_mode = "preferred"
                ownerfs_window_bytes = 33554432
                ownerfs_max_inflight = 2
            "#,
        )
        .unwrap();
        assert_eq!(parsed.ownerfs_read_mode, HixlOwnerFsReadMode::Preferred);
        assert_eq!(parsed.ownerfs_window_bytes, 32 * 1024 * 1024);
        assert!(parsed.validate(Role::Node, ownerfs_deployment()).is_ok());
    }

    fn valid_multi_device_config() -> HixlConfig {
        HixlConfig {
            dfs_read_mode: HixlDfsReadMode::Off,
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

        assert!(
            config
                .validate(Role::Node, dfs_deployment(false, false, false))
                .is_ok()
        );
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
                config
                    .validate(Role::Node, dfs_deployment(false, false, false))
                    .is_ok(),
                valid,
                "session_idle_ms = {idle_ms}"
            );
        }
    }

    #[test]
    fn prewarm_nodes_requires_an_active_read_mode() {
        assert!(HixlConfig::default().prewarm_nodes.is_empty());

        let mut inactive = valid_multi_device_config();
        inactive.prewarm_nodes = vec!["peer-a".into()];
        assert!(
            inactive
                .validate(Role::Node, dfs_deployment(true, true, true))
                .is_err(),
            "pre-warm peers with dfs_read_mode off must be rejected"
        );

        let mut active = valid_multi_device_config();
        active.dfs_read_mode = HixlDfsReadMode::Preferred;
        active.prewarm_nodes = vec!["peer-a".into(), "peer-b".into()];
        assert!(
            active
                .validate(Role::Node, dfs_deployment(true, true, true))
                .is_ok()
        );
    }

    #[test]
    fn toml_array_tables_deserialize_as_additional_device_lanes() {
        let file: super::FileConfig = toml::from_str(
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

        assert!(
            config
                .validate(Role::Node, dfs_deployment(false, false, false))
                .is_ok()
        );
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
        let mut duplicate_id = valid_multi_device_config();
        duplicate_id.additional_devices[0].device_id = duplicate_id.device_id;
        assert!(
            duplicate_id
                .validate(Role::Node, dfs_deployment(false, false, false))
                .is_err()
        );

        let mut duplicate_engine = valid_multi_device_config();
        duplicate_engine.additional_devices[0].engine_id = duplicate_engine.engine_id.clone();
        assert!(
            duplicate_engine
                .validate(Role::Node, dfs_deployment(false, false, false))
                .is_err()
        );

        let mut duplicate_port = valid_multi_device_config();
        duplicate_port.additional_devices[0].device_listen_port = duplicate_port.device_listen_port;
        assert!(
            duplicate_port
                .validate(Role::Node, dfs_deployment(false, false, false))
                .is_err()
        );
    }

    #[test]
    fn multi_device_set_requires_explicit_nonzero_listener_ports() {
        let mut config = valid_multi_device_config();
        config.device_listen_port = None;
        assert!(
            config
                .validate(Role::Node, dfs_deployment(false, false, false))
                .is_err()
        );

        config.device_listen_port = Some(17920);
        config.additional_devices[0].device_listen_port = None;
        assert!(
            config
                .validate(Role::Node, dfs_deployment(false, false, false))
                .is_err()
        );

        config.additional_devices[0].device_listen_port = Some(0);
        assert!(
            config
                .validate(Role::Node, dfs_deployment(false, false, false))
                .is_err()
        );
    }

    #[test]
    fn device_set_is_limited_to_sixteen_devices_including_primary() {
        let mut config = valid_multi_device_config();
        let primary_device_id = config.device_id;
        config.additional_devices = (0..16)
            .filter(|device_id| *device_id != primary_device_id)
            .map(|device_id| HixlDeviceConfig {
                device_id,
                engine_id: format!("127.0.0.1:{}", 8000 + device_id),
                device_listen_port: Some((18_000 + device_id) as u16),
            })
            .collect();
        assert!(
            config
                .validate(Role::Node, dfs_deployment(false, false, false))
                .is_ok()
        );

        config.additional_devices.push(HixlDeviceConfig {
            device_id: 16,
            engine_id: "127.0.0.1:8016".into(),
            device_listen_port: Some(18016),
        });
        assert!(
            config
                .validate(Role::Node, dfs_deployment(false, false, false))
                .is_err()
        );
    }

    #[test]
    fn off_configuration_still_rejects_invalid_nonempty_engine_addresses() {
        let config = HixlConfig {
            engine_id: "not-an-ip-port".into(),
            ..Default::default()
        };

        assert!(
            config
                .validate(Role::Node, dfs_deployment(false, false, false))
                .is_err()
        );
    }

    #[test]
    fn engine_endpoints_must_be_unicast_with_nonzero_ports() {
        let mut config = HixlConfig {
            device_id: 1,
            engine_id: "224.0.0.1:7920".into(),
            ..Default::default()
        };
        assert!(
            config
                .validate(Role::Node, dfs_deployment(false, false, false))
                .is_err()
        );

        config.engine_id = "127.0.0.1:0".into();
        assert!(
            config
                .validate(Role::Node, dfs_deployment(false, false, false))
                .is_err()
        );
    }
}

#[cfg(test)]
mod certificate_tests {
    use super::*;

    #[test]
    fn read_config_delivery_defaults_and_overrides() {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("node.toml");
        std::fs::write(&config, "id = 'b'\nfs = 'dfs'\n").unwrap();
        let cfg = Cli::try_parse_from(["afs-node", "--config", config.to_str().unwrap()]).unwrap();
        let cfg = Config::resolve(Role::Node, cfg).unwrap();
        assert_eq!(cfg.dfs_read_pipeline.max_reply_bytes, 256 * 1024 * 1024);
        assert_eq!(cfg.fuse_max_readahead_bytes, 16 * 1024 * 1024);
        assert_eq!(cfg.dfs_readahead_max_bytes, 0);
        assert!(!cfg.dfs_native_open_read_authority);
        assert!(cfg.dfs_crc32c_writes);
        assert_eq!(cfg.dfs_read_crc_mode, DfsReadCrcMode::Off);
        let cli = Cli::try_parse_from([
            "afs-node",
            "--config",
            config.to_str().unwrap(),
            "--fuse-max-readahead-bytes",
            "0",
        ])
        .unwrap();
        assert_eq!(
            Config::resolve(Role::Node, cli)
                .unwrap()
                .fuse_max_readahead_bytes,
            0
        );
        for bytes in [8, 256, 512] {
            std::fs::write(&config, format!("id = 'b'\nfs = 'dfs'\nfuse_max_readahead_bytes = 0\n[dfs_read_pipeline]\nmax_reply_bytes = {}\n", bytes * 1024 * 1024)).unwrap();
            let cfg = Config::resolve(
                Role::Node,
                Cli::try_parse_from(["afs-node", "--config", config.to_str().unwrap()]).unwrap(),
            )
            .unwrap();
            assert_eq!(cfg.dfs_read_pipeline.max_reply_bytes, bytes * 1024 * 1024);
            assert_eq!(cfg.fuse_max_readahead_bytes, 0);
        }
    }

    #[test]
    fn trusted_certificates_normalize_pem_and_preserve_der() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("peer.crt");
        std::fs::write(
            &path,
            b"-----BEGIN CERTIFICATE-----\nAQIDBA==\n-----END CERTIFICATE-----",
        )
        .unwrap();
        assert_eq!(read_certificate_der(&path).unwrap(), vec![1, 2, 3, 4]);
        std::fs::write(&path, [1, 2, 3, 4]).unwrap();
        assert_eq!(read_certificate_der(&path).unwrap(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn malformed_or_empty_trusted_certificate_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("peer.crt");
        for bytes in [b"".as_slice(), b"-----BEGIN CERTIFICATE-----\nAQID"] {
            std::fs::write(&path, bytes).unwrap();
            assert_eq!(
                read_certificate_der(&path).unwrap_err().code(),
                afs_error::CONFIG_INVALID
            );
        }
    }

    #[test]
    fn dfs_external_backend_unsigned_payload_defaults_false_and_accepts_true() {
        let default: DfsExternalBackendConfig = toml::from_str(
            r#"
store_id = "filesystem-a"
generation = "generation-1"
endpoint = "https://s3.example.invalid"
bucket = "chunks"
region = "us-east-1"
"#,
        )
        .unwrap();
        assert!(!default.unsigned_payload);

        let explicit: DfsExternalBackendConfig = toml::from_str(
            r#"
store_id = "filesystem-a"
generation = "generation-1"
endpoint = "https://s3.example.invalid"
bucket = "chunks"
region = "us-east-1"
unsigned_payload = true
"#,
        )
        .unwrap();
        assert!(explicit.unsigned_payload);
    }
}

#[cfg(test)]
mod crc32c_config_tests {
    use super::*;
    #[test]
    fn crc32c_native_defaults_cover_replica_and_local_copy_policies() {
        for policy in ["required", "preferred", "none"] {
            let cfg = Config::resolve(
                Role::Node,
                Cli::parse_from([
                    "afs-node",
                    "--fs",
                    "dfs",
                    "--dfs-desired-copies",
                    "3",
                    "--dfs-sync-required-copies",
                    "2",
                    "--dfs-min-distinct-nodes",
                    "2",
                    "--dfs-local-copy",
                    policy,
                ]),
            )
            .unwrap();
            assert!(cfg.dfs_crc32c_writes && cfg.dfs_optimistic_gc_commit);
        }
        let cfg =
            Config::resolve(Role::Node, Cli::parse_from(["afs-node", "--fs", "dfs"])).unwrap();
        assert!(cfg.dfs_crc32c_writes && cfg.dfs_optimistic_gc_commit);
    }

    #[test]
    fn crc32c_defaults_exclude_other_backends_and_explicit_disable_is_coherent() {
        for role in [Role::Node, Role::Meta] {
            let cfg = Config::resolve(role, Cli::parse_from(["afs", "--fs", "ownerfs"])).unwrap();
            assert!(!cfg.dfs_crc32c_writes && !cfg.dfs_optimistic_gc_commit);
        }
        let cfg = Config::resolve(
            Role::Node,
            Cli::parse_from(["afs-node", "--fs", "dfs", "--dfs-crc32c-writes", "false"]),
        )
        .unwrap();
        assert!(!cfg.dfs_crc32c_writes && !cfg.dfs_optimistic_gc_commit);
        assert!(
            Config::resolve(
                Role::Node,
                Cli::parse_from([
                    "afs-node",
                    "--fs",
                    "dfs",
                    "--dfs-optimistic-gc-commit",
                    "false",
                ])
            )
            .is_err()
        );
        let mut cfg =
            Config::resolve(Role::Node, Cli::parse_from(["afs-node", "--fs", "dfs"])).unwrap();
        cfg.dfs_optimistic_gc_commit = false;
        assert!(cfg.validate_crc32c_writes().is_err());
        cfg.dfs_optimistic_gc_commit = true;
        cfg.dfs = false;
        assert!(cfg.validate_crc32c_writes().is_err());
    }
}

#[cfg(test)]
mod crc_read_modes_contract_tests {
    use super::*;
    #[test]
    fn crc_read_modes_explicit_config_survives_resolution() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            r#"fs = "dfs"
data_mode = "grpc"
dfs_read_crc_mode = "receiver"
"#,
        )
        .unwrap();
        let cli = Cli::parse_from(["afs-node", "--config", file.path().to_str().unwrap()]);
        let cfg = Config::resolve(Role::Node, cli).unwrap();
        assert_eq!(
            serde_json::to_value(cfg).unwrap()["dfs_read_crc_mode"],
            serde_json::json!("receiver"),
            "configured CRC read mode must survive resolution"
        );
    }
}

#[cfg(test)]
mod gc_grace_config_tests {
    use super::*;
    #[test]
    fn gc_grace_defaults_to_one_second() {
        let cfg = Config::resolve(
            Role::Meta,
            Cli::parse_from(["afs-meta", "--meta-store", "memory"]),
        )
        .unwrap();
        assert_eq!(cfg.dfs_gc_grace_ms, 1_000);
    }

    #[test]
    fn gc_grace_accepts_file_and_cli_overrides() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), "dfs_gc_grace_ms = 5000\n").unwrap();
        let cli = Cli::parse_from([
            "afs-meta",
            "--meta-store",
            "memory",
            "--config",
            file.path().to_str().unwrap(),
        ]);
        let cfg = Config::resolve(Role::Meta, cli).unwrap();
        assert_eq!(cfg.dfs_gc_grace_ms, 5_000);
    }

    #[test]
    fn gc_grace_rejects_values_beyond_ten_minutes() {
        let result = Config::resolve(
            Role::Meta,
            Cli::parse_from([
                "afs-meta",
                "--meta-store",
                "memory",
                "--dfs-gc-grace-ms",
                "600001",
            ]),
        );
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod crc_read_modes_validation_tests {
    use super::*;
    #[test]
    fn crc_read_modes_cli_defaults_and_scope_preserve_write_crc() {
        for mode in ["both", "receiver", "off"] {
            let cfg = Config::resolve(
                Role::Node,
                Cli::parse_from([
                    "afs-node",
                    "--fs",
                    "dfs",
                    "--data-mode",
                    "grpc",
                    "--dfs-read-crc-mode",
                    mode,
                ]),
            )
            .unwrap();
            assert_eq!(cfg.dfs_read_crc_mode.as_str(), mode);
            assert!(cfg.dfs_crc32c_writes && cfg.dfs_optimistic_gc_commit);
        }
        let cfg =
            Config::resolve(Role::Node, Cli::parse_from(["afs-node", "--fs", "dfs"])).unwrap();
        assert_eq!(cfg.dfs_read_crc_mode, DfsReadCrcMode::Off);
        assert!(Cli::try_parse_from(["afs-node", "--dfs-read-crc-mode", "maybe"]).is_err());
        assert!(
            Config::resolve(
                Role::Node,
                Cli::parse_from([
                    "afs-node",
                    "--fs",
                    "dfs",
                    "--data-mode",
                    "rdma",
                    "--dfs-read-crc-mode",
                    "off"
                ])
            )
            .is_err()
        );
    }
}

#[cfg(test)]
mod crc_read_default_off_tests {
    use super::*;

    #[test]
    fn crc_read_default_off_keeps_native_write_checks_and_optional_receiver() {
        let cfg =
            Config::resolve(Role::Node, Cli::parse_from(["afs-node", "--fs", "dfs"])).unwrap();
        assert_eq!(cfg.dfs_read_crc_mode, DfsReadCrcMode::Off);
        assert_eq!(DfsReadCrcMode::default(), DfsReadCrcMode::Off);
        assert!(cfg.dfs_crc32c_writes && cfg.dfs_optimistic_gc_commit);
        assert_eq!(
            serde_json::to_value(&cfg).unwrap()["dfs_read_crc_mode"],
            "off"
        );
        for mode in ["receiver", "both", "off"] {
            let cfg = Config::resolve(
                Role::Node,
                Cli::parse_from(["afs-node", "--fs", "dfs", "--dfs-read-crc-mode", mode]),
            )
            .unwrap();
            assert_eq!(cfg.dfs_read_crc_mode.as_str(), mode);
            assert!(cfg.dfs_crc32c_writes);
        }
    }

    #[test]
    fn crc_read_default_off_does_not_activate_native_policy_on_other_backends() {
        for role in [Role::Node, Role::Meta] {
            let cfg = Config::resolve(role, Cli::parse_from(["afs", "--fs", "ownerfs"])).unwrap();
            assert_eq!(cfg.dfs_read_crc_mode, DfsReadCrcMode::Both);
            assert!(!cfg.dfs_crc32c_writes);
        }
        let cfg = Config::resolve(
            Role::Node,
            Cli::parse_from(["afs-node", "--fs", "dfs", "--data-mode", "auto"]),
        )
        .unwrap();
        assert_eq!(cfg.dfs_read_crc_mode, DfsReadCrcMode::Both);
        assert!(cfg.dfs_crc32c_writes);
    }

    #[test]
    fn crc_read_default_off_respects_toml_and_cli_precedence() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            "fs = \"dfs\"\ndata_mode = \"grpc\"\ndfs_read_crc_mode = \"receiver\"\n",
        )
        .unwrap();
        let path = file.path().to_str().unwrap();
        let from_file =
            Config::resolve(Role::Node, Cli::parse_from(["afs-node", "--config", path])).unwrap();
        assert_eq!(from_file.dfs_read_crc_mode, DfsReadCrcMode::Receiver);
        assert!(from_file.dfs_crc32c_writes);
        let cli = Config::resolve(
            Role::Node,
            Cli::parse_from(["afs-node", "--config", path, "--dfs-read-crc-mode", "off"]),
        )
        .unwrap();
        assert_eq!(cli.dfs_read_crc_mode, DfsReadCrcMode::Off);
        assert!(cli.dfs_crc32c_writes);
    }
}

#[cfg(test)]
mod read_concurrency_config_tests {
    use super::*;

    #[test]
    fn read_concurrency_admission_is_independent_of_workers() {
        let cli = Cli::try_parse_from([
            "afs-node",
            "--fs",
            "dfs",
            "--fuse-workers",
            "32",
            "--fuse-max-tasks",
            "2",
        ]);
        assert!(
            cli.is_ok(),
            "independent task capacity must be configurable"
        );
        let cfg = Config::resolve(Role::Node, cli.unwrap()).unwrap();
        let value = serde_json::to_value(cfg).unwrap();
        assert_eq!(value["fuse_workers"], 32);
        assert_eq!(value["fuse_max_tasks"], 2);
    }

    #[test]
    fn read_concurrency_default_capacity_preserves_worker_multiplier() {
        let cfg = Config::resolve(
            Role::Node,
            Cli::parse_from(["afs-node", "--fs", "dfs", "--fuse-workers", "3"]),
        )
        .unwrap();
        assert_eq!(serde_json::to_value(cfg).unwrap()["fuse_max_tasks"], 24);
    }

    #[test]
    fn read_concurrency_admission_rejects_zero() {
        let cli =
            Cli::try_parse_from(["afs-node", "--fs", "dfs", "--fuse-max-tasks", "0"]).unwrap();
        assert!(Config::resolve(Role::Node, cli).is_err());
    }
}
