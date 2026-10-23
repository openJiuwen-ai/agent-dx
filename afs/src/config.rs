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
    collections::HashMap,
    net::SocketAddr,
    path::{Component, Path, PathBuf},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Meta,
    Node,
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
    #[arg(long)]
    pub peer_endpoint: Option<String>,
    /// 对外发布的本 Node gRPC URI；监听 0.0.0.0 时必须显式指定。
    #[arg(long)]
    pub advertise_endpoint: Option<String>,
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
    /// Native Node live write-body capacity (4..=64MiB); S3 budgets are separate.
    #[arg(long)]
    pub dfs_dirty_budget_bytes: Option<u64>,
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
    /// Write native objects with segmented CRC32C; native DFS Nodes default to true.
    #[arg(long)]
    pub dfs_crc32c_writes: Option<bool>,
    /// Omit per-chunk GC registration; CRC commits fence their own write generations.
    #[arg(long)]
    pub dfs_optimistic_gc_commit: Option<bool>,
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
    peer_endpoint: Option<String>,
    advertise_endpoint: Option<String>,
    tls_ca_certificate: Option<PathBuf>,
    tls_identity_certificate: Option<PathBuf>,
    tls_identity_private_key: Option<PathBuf>,
    tls_server_name: Option<String>,
    /// TLS 验证后再用证书 DER 的完整字节绑定 Node ID；仅由受信管理配置设置。
    trusted_node_certs: Option<HashMap<String, PathBuf>>,
    fs: Option<String>,
    data_mode: Option<String>,
    rdma_device: Option<String>,
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
    dfs_fuse_keep_cache: Option<bool>,
    dfs_fuse_entry_ttl_ms: Option<u32>,
    dfs_fuse_attr_ttl_ms: Option<u32>,
    fuse_max_readahead_bytes: Option<u32>,
    dfs_version_cache_entries: Option<usize>,
    dfs_version_cache_bytes: Option<u64>,
    dfs_outboard_work_wait_ms: Option<u64>,
    dfs_catalog_commit_queue: Option<usize>,
    dfs_dirty_budget_bytes: Option<u64>,
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
    dfs_crc32c_writes: Option<bool>,
    dfs_optimistic_gc_commit: Option<bool>,
    timeout_ms: Option<u64>,
    log_level: Option<String>,
    trace_enabled: Option<bool>,
    trace_endpoint: Option<String>,
    trace_sample_ratio: Option<f64>,
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
    pub peer_endpoint: Option<String>,
    pub advertise_endpoint: Option<String>,
    pub tls_ca_certificate: Option<PathBuf>,
    pub tls_identity_certificate: Option<PathBuf>,
    pub tls_identity_private_key: Option<PathBuf>,
    pub tls_server_name: Option<String>,
    pub trusted_node_certs: HashMap<String, PathBuf>,
    pub ownerfs: bool,
    pub dfs: bool,
    pub data_mode: String,
    pub rdma_device: Option<String>,
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
    pub dfs_fuse_keep_cache: bool,
    pub dfs_fuse_entry_ttl_ms: u32,
    pub dfs_fuse_attr_ttl_ms: u32,
    pub fuse_max_readahead_bytes: u32,
    pub dfs_version_cache_entries: usize,
    pub dfs_version_cache_bytes: u64,
    pub dfs_outboard_work_wait_ms: u64,
    pub dfs_catalog_commit_queue: usize,
    pub dfs_dirty_budget_bytes: u64,
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
    pub dfs_crc32c_writes: bool,
    pub dfs_optimistic_gc_commit: bool,
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
            .unwrap_or(1024 * 1024);
        if !(1..=64).contains(&fuse_threads)
            || !(1..=1024).contains(&fuse_workers)
            || dfs_fuse_entry_ttl_ms > 60_000
            || dfs_fuse_attr_ttl_ms > 60_000
            || fuse_max_readahead_bytes > 16 * 1024 * 1024
        {
            return Err(invalid(
                "FUSE threads must be 1..=64, workers 1..=1024, DFS TTLs <= 60000 ms and read-ahead <= 16 MiB",
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
        let dfs_dirty_budget_bytes = cli
            .dfs_dirty_budget_bytes
            .or(file.dfs_dirty_budget_bytes)
            .unwrap_or(64 * 1024 * 1024);
        if !(4 * 1024 * 1024..=64 * 1024 * 1024).contains(&dfs_dirty_budget_bytes) {
            return Err(invalid("DFS native dirty budget bytes must be 4..=64MiB"));
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
            peer_endpoint: cli.peer_endpoint.or(file.peer_endpoint),
            advertise_endpoint: cli.advertise_endpoint.or(file.advertise_endpoint),
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
            dfs_dirty_budget_bytes,
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
            dfs_read_diagnostics_enabled: cli
                .dfs_read_diagnostics_enabled
                .or(file.dfs_read_diagnostics_enabled)
                .unwrap_or(false),
            dfs_crc32c_writes,
            dfs_optimistic_gc_commit,
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
        if tls_count != 0 && tls_count != 4 {
            return Err(invalid("all four tls_* fields are required together"));
        }
        for path in cfg.trusted_node_certs.values() {
            if !path.is_absolute() {
                return Err(invalid("trusted_node_certs paths must be absolute"));
            }
        }
        cfg.validate_crc32c_writes()?;
        cfg.meta_log_limits()?;
        cfg.meta_maintenance_policy()?;
        cfg.meta_history_limits()?;
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
mod certificate_tests {
    use super::*;

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
