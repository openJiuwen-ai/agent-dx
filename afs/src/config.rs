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
use std::{collections::HashMap, net::SocketAddr, path::PathBuf};

/// Default kernel entry/attribute TTL of DFS mounts, in milliseconds.
pub const DEFAULT_DFS_KERNEL_CACHE_TTL_MS: u64 = 1_000;
/// Largest accepted `dfs_kernel_cache_ttl_ms`.
pub const MAX_DFS_KERNEL_CACHE_TTL_MS: u64 = 60_000;

mod common;
mod cross;
mod dfs;
mod fuse;
mod meta;
#[cfg(any(feature = "meta-redis", feature = "meta-etcd"))]
pub(crate) use meta::MetaStoreTarget;
mod outlet;
mod ownerfs;
mod tls;

pub use outlet::{DEFAULT_MOUNT_OPTIONS, OutletConfig};

pub use tls::read_certificate_der;

/// Whether `id` is safe to embed in filesystem paths and request keys.
///
/// The rule (`1..=128` bytes of ASCII letters, digits, `_`, `-`) is the one the
/// local node id has always been held to at config resolution. It is also
/// applied to node ids that arrive from the cluster (Meta registration and the
/// outlet snapshot), because consumers build mount points and symlink targets
/// by joining the id under a configured root: an id containing `/`, `..` or an
/// absolute path would escape that root.
#[must_use]
pub fn is_safe_node_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Meta,
    Node,
}
/// Meta authority backend. `memory` is useful for disposable local runs but
/// loses root/session authority whenever afs-meta exits. `redis` and `etcd`
/// keep only the operation log and checkpoints in that service; Meta stays a
/// single process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
pub enum MetaStoreBackend {
    #[serde(rename = "local-file")]
    #[value(name = "local-file")]
    LocalFile,
    #[serde(rename = "memory")]
    #[value(name = "memory")]
    InMemory,
    #[serde(rename = "redis")]
    #[value(name = "redis")]
    Redis,
    #[serde(rename = "etcd")]
    #[value(name = "etcd")]
    Etcd,
}
/// Node-to-Node data transport. `rdma` requires the `rdma` feature; `auto`
/// negotiates per peer and falls back to gRPC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum DataMode {
    Grpc,
    Rdma,
    Auto,
}
/// `dfs_local_copy`: whether the writing Node must hold a replica. Meta persists
/// the choice as [`crate::dfs::LocalCopyPolicy`] when it initializes the filesystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum DfsLocalCopy {
    Required,
    Preferred,
    None,
}
impl From<DfsLocalCopy> for crate::dfs::LocalCopyPolicy {
    fn from(value: DfsLocalCopy) -> Self {
        match value {
            DfsLocalCopy::Required => Self::Required,
            DfsLocalCopy::Preferred => Self::Preferred,
            DfsLocalCopy::None => Self::NotRequired,
        }
    }
}
/// `dfs_replica_failure_policy`: what a write does when a synchronous replica
/// cannot be written (R12, decision 3). Meta persists it once, beside the
/// replication config, as [`crate::dfs::ReplicaFailurePolicy`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum DfsReplicaFailurePolicy {
    /// Commit with at least `dfs_min_degraded_copies` copies and repair the rest.
    #[default]
    Degrade,
    /// Fail the write unless every synchronous copy persists.
    Strict,
}
impl From<DfsReplicaFailurePolicy> for crate::dfs::ReplicaFailurePolicy {
    fn from(value: DfsReplicaFailurePolicy) -> Self {
        match value {
            DfsReplicaFailurePolicy::Degrade => Self::Degrade,
            DfsReplicaFailurePolicy::Strict => Self::Strict,
        }
    }
}
/// Administrator-enabled OwnerFs workspace bind mount entry.
/// The name is a single first-level OwnerFs workspace under the mount root.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceBindConfig {
    pub workspace: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceApiConfig {
    pub listen: SocketAddr,
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
    /// `false` signs every PUT body with SHA-256 (SigV4 signed payload);
    /// `true` sends `UNSIGNED-PAYLOAD`. Request authentication is SigV4 either
    /// way. Default: unsigned for an `https` endpoint, signed for `http`.
    pub unsigned_payload: Option<bool>,
    /// Whole-body checksum sent as a signed request header on every PUT. The
    /// store rejects a body that does not match it. Default: `crc64nvme` when
    /// the payload is unsigned, otherwise `none`.
    pub payload_checksum: Option<DfsExternalPayloadChecksum>,
    /// Concurrent object PUTs, 1..=8 (default 8).
    pub upload_concurrency: Option<usize>,
    /// Request worker threads, 1..=8 (default: `upload_concurrency`, at most 4).
    pub worker_threads: Option<usize>,
    pub credentials_file: Option<PathBuf>,
    /// Optional disposable content cache; omission preserves backend-only reads.
    pub cache: Option<DfsExternalCacheConfig>,
}

/// `dfs_external_backend.payload_checksum`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DfsExternalPayloadChecksum {
    /// No checksum header; only a signed payload lets the store detect a
    /// body damaged in transit.
    None,
    /// `x-amz-checksum-crc64nvme`.
    Crc64nvme,
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
    /// Whether PUT bodies go out unsigned, after defaults. Over TLS the
    /// transport already authenticates the body, and the checksum header
    /// lets the store reject a damaged one; a plaintext endpoint keeps the
    /// signed payload.
    pub fn effective_unsigned_payload(&self) -> bool {
        self.unsigned_payload
            .unwrap_or_else(|| self.endpoint.starts_with("https://"))
    }

    /// The PUT checksum header after defaults.
    pub fn effective_payload_checksum(&self) -> DfsExternalPayloadChecksum {
        self.payload_checksum
            .unwrap_or(if self.effective_unsigned_payload() {
                DfsExternalPayloadChecksum::Crc64nvme
            } else {
                DfsExternalPayloadChecksum::None
            })
    }

    /// This configuration with every defaulted upload setting made explicit,
    /// as `--print-config` shows it.
    #[must_use]
    pub fn with_defaults(mut self) -> Self {
        self.unsigned_payload = Some(self.effective_unsigned_payload());
        self.payload_checksum = Some(self.effective_payload_checksum());
        self.upload_concurrency = Some(self.effective_upload_concurrency());
        self.worker_threads = Some(self.effective_worker_threads());
        self
    }

    /// Concurrent object PUTs after defaults.
    pub fn effective_upload_concurrency(&self) -> usize {
        self.upload_concurrency.unwrap_or(8)
    }

    /// Request worker threads after defaults.
    pub fn effective_worker_threads(&self) -> usize {
        self.worker_threads
            .unwrap_or_else(|| self.effective_upload_concurrency().min(4))
    }

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
            .upload_concurrency
            .is_some_and(|value| !(1..=8).contains(&value))
            || self
                .worker_threads
                .is_some_and(|value| !(1..=8).contains(&value))
        {
            return Err(invalid(
                "external backend upload_concurrency and worker_threads must be 1..=8",
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
    /// Meta authority: local-file (default, new-format directory), redis, etcd
    /// or volatile memory.
    #[arg(long, value_enum)]
    pub meta_store: Option<MetaStoreBackend>,
    /// Redis URL of the `redis` Meta log (required with it, rejected otherwise).
    #[arg(long)]
    pub meta_redis_url: Option<String>,
    /// etcd endpoints of the `etcd` Meta log, comma separated (required with
    /// it, rejected otherwise).
    #[arg(long, value_delimiter = ',')]
    pub meta_etcd_endpoints: Option<Vec<String>>,
    /// The etcd server's `--max-request-bytes` (default 1572864, etcd's own
    /// default); bounds one append and one checkpoint chunk.
    #[arg(long)]
    pub meta_etcd_max_request_bytes: Option<usize>,
    /// Key prefix of the `redis`/`etcd` Meta log (default `afs/meta`).
    #[arg(long)]
    pub meta_store_prefix: Option<String>,
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
    /// Test only: allow a node to become functionally ready with a healthy volatile Meta.
    #[arg(long, hide = true)]
    pub allow_volatile_meta: Option<bool>,
    /// Enable centralized HRW placement for new OwnerFs workspaces on Meta.
    #[arg(long)]
    pub ownerfs_mkdir_load_balance: Option<bool>,
    /// Diagnostics only: the peer that the Node REST `diagnose` route probes.
    #[arg(long, hide = true)]
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
    #[arg(long, value_enum)]
    pub data_mode: Option<DataMode>,
    #[arg(long)]
    pub rdma_device: Option<String>,
    #[arg(long)]
    pub data_dir: Option<PathBuf>,
    #[arg(long)]
    pub uds_path: Option<PathBuf>,
    #[arg(long)]
    pub ownerfs_mount: Option<PathBuf>,
    /// Experimental OwnerFs workspace bind mount path; default false, not production READY.
    #[arg(long)]
    pub experimental_ownerfs_workspace_bind: Option<bool>,
    /// Native egress root for OwnerFs workspace bind targets; absolute, outside
    /// ownerfs_mount and data_dir, required when experimental_ownerfs_workspace_bind.
    #[arg(long)]
    pub ownerfs_workspace_egress_root: Option<PathBuf>,
    /// tmpfs directory that holds the outlet public names. All outlet paths must
    /// be set together; leaving them unset disables the feature.
    #[arg(long)]
    pub outlet_public_root: Option<PathBuf>,
    /// Root under which per-home data trees are mounted.
    #[arg(long)]
    pub outlet_import_root: Option<PathBuf>,
    /// Convergence period of the outlet mirror (10..=60000ms), default 250.
    #[arg(long)]
    pub outlet_mirror_interval_ms: Option<u64>,
    /// Mount options for the import mounts `afs-node` performs itself. Matches
    /// the kernel defaults autofs uses today, so the data path keeps the same
    /// semantics it has under a static mount.
    #[arg(long)]
    pub outlet_import_mount_options: Option<String>,
    /// Path of the users tree on *every* node, used to build the mount source
    /// `<home>:<outlet_users_root>`. A cluster-wide convention: a peer's path
    /// cannot be discovered without a node manifest, which this design
    /// deliberately does not have.
    #[arg(long)]
    pub outlet_users_root: Option<PathBuf>,
    /// Address peers should use to reach this node's NFS export, e.g.
    /// `192.168.2.101` or `data-node-a.cluster`. Host only: the port is NFS's.
    ///
    /// This node declares only its own address; peers learn it from the Meta
    /// session snapshot, so adding a node never requires editing any other
    /// node's configuration. Defaults to the host part of `grpc_listen`.
    #[arg(long)]
    pub outlet_nfs_addr: Option<String>,
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
    /// DFS write admission and fsync deadline, in milliseconds.
    #[arg(long)]
    pub dfs_write_timeout_ms: Option<u64>,
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
    /// S3 bucket pages listed per GC pass to find unregistered objects (1..=100).
    #[arg(long)]
    pub dfs_external_gc_list_pages_per_pass: Option<usize>,
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
    /// Background scrub rate of local DFS copies in bytes per second
    /// (0 disables, otherwise 1MiB..=1GiB; default 32MiB).
    #[arg(long)]
    pub dfs_scrub_bytes_per_second: Option<u64>,
    /// Delay before the first scrub round after Node start (0..=86400 s).
    #[arg(long)]
    pub dfs_scrub_start_delay_seconds: Option<u64>,
    /// Minimum time between the starts of two scrub rounds (1..=2592000 s).
    #[arg(long)]
    pub dfs_scrub_round_interval_seconds: Option<u64>,
    /// Kernel entry/attribute TTL of the DFS mount (0..=60000 ms; default
    /// 1000; 0 disables kernel caching of names and attributes).
    #[arg(long)]
    pub dfs_kernel_cache_ttl_ms: Option<u64>,
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
    #[arg(long, value_enum)]
    pub dfs_local_copy: Option<DfsLocalCopy>,
    /// Filesystem-wide immutable: `degrade` (default) or `strict`. Applied by Meta at initialization.
    #[arg(long, value_enum)]
    pub dfs_replica_failure_policy: Option<DfsReplicaFailurePolicy>,
    /// Fewest copies a degraded write keeps, in 1..=dfs_sync_required_copies (default 1).
    #[arg(long)]
    pub dfs_min_degraded_copies: Option<u16>,
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
    #[arg(long, hide = true)]
    pub dfs_read_diagnostics_enabled: Option<bool>,
    /// Remote-read body verification for native DFS gRPC: `receiver`
    /// (default) verifies peer bytes against the Meta-trusted CRC table;
    /// `off` delivers them unchecked. Only the reading Node's setting matters.
    #[arg(long)]
    pub dfs_read_crc_mode: Option<DfsReadCrcMode>,
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
    /// Print the compiled-in build identity and exit.
    ///
    /// The crate version alone cannot distinguish two builds of the same source
    /// tree, which matters when several users share a host and need to prove
    /// which binary is running.
    #[arg(long)]
    pub build_info: bool,
}

impl Cli {
    /// Provenance of the running binary, for `--build-info`. `build.rs` reports
    /// the commit and whether the tree was dirty, or `unknown` when the build
    /// had no git metadata and `AFS_BUILD_ID` was unset.
    #[must_use]
    pub fn build_info() -> String {
        let id = option_env!("AFS_BUILD_ID").unwrap_or("unknown");
        format!(
            "afs {} ({id}, {}/{})",
            env!("CARGO_PKG_VERSION"),
            std::env::consts::OS,
            std::env::consts::ARCH,
        )
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
/// TOML 输入层。拒绝未知字段，让拼错开关在启动时失败，而不是静默使用默认行为。
struct FileConfig {
    connectors: Option<crate::node::connectors::ConnectorsConfig>,
    dfs_read_parallelism: Option<usize>,
    dfs_read_stripe_bytes: Option<usize>,
    dfs_read_cache_bytes: Option<u64>,
    id: Option<String>,
    grpc_listen: Option<SocketAddr>,
    rest_listen: Option<SocketAddr>,
    meta_endpoint: Option<String>,
    meta_store: Option<MetaStoreBackend>,
    meta_redis_url: Option<String>,
    meta_etcd_endpoints: Option<Vec<String>>,
    meta_etcd_max_request_bytes: Option<usize>,
    meta_store_prefix: Option<String>,
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
    data_mode: Option<DataMode>,
    rdma_device: Option<String>,
    data_dir: Option<PathBuf>,
    uds_path: Option<PathBuf>,
    ownerfs_mount: Option<PathBuf>,
    experimental_ownerfs_workspace_bind: Option<bool>,
    ownerfs_workspace_bind: Option<WorkspaceBindConfig>,
    ownerfs_workspace_api: Option<WorkspaceApiConfig>,
    ownerfs_workspace_default_user: Option<String>,
    ownerfs_workspace_default_uid: Option<u32>,
    ownerfs_workspace_default_gid: Option<u32>,
    ownerfs_workspace_egress_root: Option<PathBuf>,
    outlet_public_root: Option<PathBuf>,
    outlet_import_root: Option<PathBuf>,
    outlet_mirror_interval_ms: Option<u64>,
    outlet_import_mount_options: Option<String>,
    outlet_users_root: Option<PathBuf>,
    outlet_nfs_addr: Option<String>,
    dfs_mount: Option<PathBuf>,
    fuse_threads: Option<u16>,
    fuse_clone_fd: Option<bool>,
    fuse_workers: Option<u16>,
    fuse_max_tasks: Option<usize>,
    dfs_read_pipeline: Option<crate::node::read::admission::ReadPipelineLimits>,
    fuse_max_readahead_bytes: Option<u32>,
    dfs_version_cache_entries: Option<usize>,
    dfs_version_cache_bytes: Option<u64>,
    dfs_outboard_work_wait_ms: Option<u64>,
    dfs_catalog_commit_queue: Option<usize>,
    dfs_write_queue_bytes: Option<u64>,
    dfs_dirty_budget_bytes: Option<u64>,
    dfs_write_timeout_ms: Option<u64>,
    dfs_gc_grace_ms: Option<u64>,
    dfs_unpublished_descriptors: Option<usize>,
    dfs_upload_inflight_chunks: Option<usize>,
    dfs_background_commit_ms: Option<u64>,
    dfs_sidecar_install_queue: Option<usize>,
    dfs_external_gc_list_pages_per_pass: Option<usize>,
    dfs_fetch_max_inflight_objects: Option<usize>,
    dfs_readahead_max_bytes: Option<u64>,
    dfs_peer_serve_resident_objects: Option<usize>,
    dfs_peer_serve_payload_bytes: Option<u64>,
    dfs_peer_serve_wait_ms: Option<u64>,
    dfs_scrub_bytes_per_second: Option<u64>,
    dfs_scrub_start_delay_seconds: Option<u64>,
    dfs_scrub_round_interval_seconds: Option<u64>,
    dfs_kernel_cache_ttl_ms: Option<u64>,
    dfs_external_backend: Option<DfsExternalBackendConfig>,
    dfs_peer_cache: Option<DfsPeerCacheConfig>,
    dfs_write_buffer_idle_bytes: Option<u64>,
    dfs_desired_copies: Option<u16>,
    dfs_sync_required_copies: Option<u16>,
    dfs_min_distinct_nodes: Option<u16>,
    dfs_min_distinct_failure_domains: Option<u16>,
    dfs_local_copy: Option<DfsLocalCopy>,
    dfs_replica_failure_policy: Option<DfsReplicaFailurePolicy>,
    dfs_min_degraded_copies: Option<u16>,
    dfs_read_max_ops_per_batch: Option<usize>,
    dfs_read_max_inflight_bytes: Option<u64>,
    dfs_source_cache_max_ms: Option<u64>,
    dfs_source_lookahead_chunks: Option<usize>,
    dfs_source_cache_bytes: Option<usize>,
    dfs_read_diagnostics_enabled: Option<bool>,
    dfs_read_crc_mode: Option<DfsReadCrcMode>,
    timeout_ms: Option<u64>,
    log_level: Option<String>,
    trace_enabled: Option<bool>,
    trace_endpoint: Option<String>,
    trace_sample_ratio: Option<f64>,
}
/// Remote native CRC-format reads only; write integrity is independent.
/// Users choose `off` or `receiver`, and only for native DFS gRPC. The
/// default is `receiver`: a corrupt remote copy is rejected and the read moves
/// to another copy instead of returning the bad bytes (D-S4-1). `off` is an
/// explicit opt-out that may deliver silent corruption. `Both` (sender and
/// receiver verify) is the fixed internal policy elsewhere and the meaning of
/// a peer request without a mode header; it is not configurable.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum DfsReadCrcMode {
    #[value(skip)]
    #[serde(skip_deserializing)]
    Both,
    #[default]
    Receiver,
    Off,
}
impl DfsReadCrcMode {
    #[cfg(feature = "dfs")]
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
    pub connectors: crate::node::connectors::ConnectorsConfig,
    pub dfs_read_parallelism: usize,
    pub dfs_read_stripe_bytes: usize,
    pub dfs_read_cache_bytes: u64,
    pub id: String,
    pub grpc_listen: SocketAddr,
    pub rest_listen: SocketAddr,
    pub meta_endpoint: Option<String>,
    pub meta_store: MetaStoreBackend,
    /// Credentials in the URL are not printed.
    #[serde(serialize_with = "meta::serialize_redacted_url")]
    pub meta_redis_url: Option<String>,
    pub meta_etcd_endpoints: Vec<String>,
    pub meta_etcd_max_request_bytes: usize,
    pub meta_store_prefix: String,
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
    pub data_mode: DataMode,
    pub rdma_device: Option<String>,
    pub data_dir: PathBuf,
    pub uds_path: PathBuf,
    pub ownerfs_mount: Option<PathBuf>,
    pub experimental_ownerfs_workspace_bind: bool,
    pub ownerfs_workspace_bind: Option<WorkspaceBindConfig>,
    pub ownerfs_workspace_api: Option<WorkspaceApiConfig>,
    pub ownerfs_workspace_default_user: String,
    pub ownerfs_workspace_default_uid: Option<u32>,
    pub ownerfs_workspace_default_gid: Option<u32>,
    pub ownerfs_workspace_egress_root: Option<PathBuf>,
    /// Outlet public (name) layer. `Some` only when all outlet paths are
    /// configured; the feature is off otherwise.
    pub outlet: Option<OutletConfig>,
    pub dfs_mount: Option<PathBuf>,
    pub fuse_threads: u16,
    pub fuse_clone_fd: bool,
    pub fuse_workers: u16,
    pub fuse_max_tasks: usize,
    pub dfs_read_pipeline: crate::node::read::admission::ReadPipelineLimits,
    pub fuse_max_readahead_bytes: u32,
    pub dfs_version_cache_entries: usize,
    pub dfs_version_cache_bytes: u64,
    pub dfs_outboard_work_wait_ms: u64,
    pub dfs_catalog_commit_queue: usize,
    pub dfs_write_queue_bytes: u64,
    pub dfs_dirty_budget_bytes: u64,
    pub dfs_write_timeout_ms: u64,
    pub dfs_gc_grace_ms: u64,
    pub dfs_unpublished_descriptors: usize,
    pub dfs_upload_inflight_chunks: usize,
    pub dfs_background_commit_ms: u64,
    pub dfs_sidecar_install_queue: usize,
    pub dfs_external_gc_list_pages_per_pass: usize,
    pub dfs_fetch_max_inflight_objects: usize,
    pub dfs_readahead_max_bytes: u64,
    pub dfs_peer_serve_resident_objects: usize,
    pub dfs_peer_serve_payload_bytes: u64,
    pub dfs_peer_serve_wait_ms: u64,
    pub dfs_scrub_bytes_per_second: u64,
    pub dfs_scrub_start_delay_seconds: u64,
    pub dfs_scrub_round_interval_seconds: u64,
    pub dfs_kernel_cache_ttl_ms: u64,
    pub dfs_external_backend: Option<DfsExternalBackendConfig>,
    pub dfs_peer_cache: Option<DfsPeerCacheConfig>,
    /// Reusable idle write-body capacity only, not a live-body quota.
    pub dfs_write_buffer_idle_bytes: u64,
    pub dfs_desired_copies: u16,
    pub dfs_sync_required_copies: u16,
    pub dfs_min_distinct_nodes: u16,
    pub dfs_min_distinct_failure_domains: u16,
    pub dfs_local_copy: DfsLocalCopy,
    pub dfs_replica_failure_policy: DfsReplicaFailurePolicy,
    pub dfs_min_degraded_copies: u16,
    pub dfs_read_max_ops_per_batch: usize,
    pub dfs_read_max_inflight_bytes: u64,
    pub dfs_source_cache_max_ms: u64,
    pub dfs_source_lookahead_chunks: usize,
    pub dfs_source_cache_bytes: usize,
    pub dfs_read_diagnostics_enabled: bool,
    pub dfs_read_crc_mode: DfsReadCrcMode,
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
    ///
    /// Each domain resolves its values in the original order, then checks that
    /// need the merged configuration run in the original order too; error
    /// messages and defaults are those of the single function this replaced.
    pub fn resolve(role: Role, mut cli: Cli) -> Result<Self> {
        let mut file = common::read_file(cli.config.as_deref())?;
        common::reject_node_meta_options(role, &cli, &file)?;
        let id = common::resolve_id(role, &mut cli, &mut file)?;
        let (ownerfs, dfs) = common::resolve_filesystems(role, &mut cli, &mut file)?;
        let data_mode = common::resolve_data_mode(&mut cli, &mut file)?;
        let dfs_read_crc_mode = dfs::resolve_read_crc_mode(&cli, &file, dfs, data_mode);
        let replication = dfs::resolve_replication(&mut cli, &mut file)?;
        let read = dfs::resolve_read_budget(&cli, &file)?;
        let fuse = fuse::resolve(&cli, &mut file)?;
        let version_cache = dfs::resolve_version_cache(role, &cli, &file)?;
        let tuning = dfs::resolve_tuning(&cli, &file)?;
        let meta = meta::resolve(&cli, &file);
        // Hoisted out of `merge` because the outlet layer advertises an NFS
        // address and derives its default from this node's own listen host.
        let grpc_listen: SocketAddr = cli.grpc_listen.or(file.grpc_listen).unwrap_or_else(|| {
            ([127, 0, 0, 1], if role == Role::Meta { 7400 } else { 7500 }).into()
        });
        let outlet = outlet::resolve(&cli, &file, grpc_listen)?;
        let resolved = Resolved {
            id,
            ownerfs,
            dfs,
            data_mode,
            dfs_read_crc_mode,
            replication,
            read,
            fuse,
            version_cache,
            tuning,
            meta,
            grpc_listen,
            outlet,
        };
        let cfg = Self::merge(role, cli, file, resolved);
        dfs::validate_storage(role, &cfg)?;
        common::validate_process(&cfg)?;
        ownerfs::validate(role, &cfg)?;
        cross::validate_mounts(&cfg)?;
        tls::validate(&cfg)?;
        cross::validate_read_crc(&cfg)?;
        cfg.meta_log_limits()?;
        cfg.meta_store_target()?;
        if cfg.connectors.nydus.enabled
            && (role != Role::Node || !cfg.dfs || cfg.dfs_external_backend.is_some())
        {
            return Err(invalid("Nydus connector requires a native DFS Node"));
        }
        cfg.meta_maintenance_policy()?;
        cfg.meta_history_limits()?;
        Ok(cfg)
    }

    /// The configuration literal: CLI over TOML over defaults for the plain
    /// values, the domain results for the rest. Pure; no check runs here.
    fn merge(role: Role, cli: Cli, file: FileConfig, resolved: Resolved) -> Self {
        let Resolved {
            id,
            ownerfs,
            dfs,
            data_mode,
            dfs_read_crc_mode,
            replication,
            read,
            fuse,
            version_cache: (dfs_version_cache_entries, dfs_version_cache_bytes),
            tuning,
            meta,
            grpc_listen,
            outlet,
        } = resolved;
        Self {
            grpc_listen,
            rest_listen: cli.rest_listen.or(file.rest_listen).unwrap_or_else(|| {
                ([127, 0, 0, 1], if role == Role::Meta { 7401 } else { 7501 }).into()
            }),
            meta_endpoint: cli.meta_endpoint.or(file.meta_endpoint),
            connectors: file.connectors.unwrap_or_default(),
            dfs_read_parallelism: file.dfs_read_parallelism.unwrap_or(1),
            dfs_read_stripe_bytes: file.dfs_read_stripe_bytes.unwrap_or(256 * 1024),
            dfs_read_cache_bytes: file.dfs_read_cache_bytes.unwrap_or(0),
            meta_store: meta.meta_store,
            meta_redis_url: meta.meta_redis_url,
            meta_etcd_endpoints: meta.meta_etcd_endpoints,
            meta_etcd_max_request_bytes: meta.meta_etcd_max_request_bytes,
            meta_store_prefix: meta.meta_store_prefix,
            meta_record_max_bytes: meta.meta_record_max_bytes,
            meta_batch_max_bytes: meta.meta_batch_max_bytes,
            meta_wal_max_bytes: meta.meta_wal_max_bytes,
            meta_checkpoint_wal_bytes: meta.meta_checkpoint_wal_bytes,
            meta_checkpoint_wal_records: meta.meta_checkpoint_wal_records,
            meta_checkpoint_interval_ms: meta.meta_checkpoint_interval_ms,
            meta_wal_segment_bytes: meta.meta_wal_segment_bytes,
            meta_request_retention_ms: meta.meta_request_retention_ms,
            meta_request_outcomes_max: meta.meta_request_outcomes_max,
            meta_request_bytes_max: meta.meta_request_bytes_max,
            meta_watch_events_max: meta.meta_watch_events_max,
            meta_watch_bytes_max: meta.meta_watch_bytes_max,
            meta_watch_retention_ms: meta.meta_watch_retention_ms,
            allow_volatile_meta: meta.allow_volatile_meta,
            ownerfs_mkdir_load_balance: meta.ownerfs_mkdir_load_balance,
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
            ownerfs_mount: cli.ownerfs_mount.or(file.ownerfs_mount),
            experimental_ownerfs_workspace_bind: cli
                .experimental_ownerfs_workspace_bind
                .or(file.experimental_ownerfs_workspace_bind)
                .unwrap_or(false),
            ownerfs_workspace_bind: file.ownerfs_workspace_bind,
            ownerfs_workspace_api: file.ownerfs_workspace_api,
            ownerfs_workspace_default_user: file
                .ownerfs_workspace_default_user
                .unwrap_or_else(|| "agentos".into()),
            ownerfs_workspace_default_uid: file.ownerfs_workspace_default_uid,
            ownerfs_workspace_default_gid: file.ownerfs_workspace_default_gid,
            ownerfs_workspace_egress_root: cli
                .ownerfs_workspace_egress_root
                .or(file.ownerfs_workspace_egress_root),
            outlet,
            dfs_mount: cli.dfs_mount.or(file.dfs_mount),
            fuse_threads: fuse.fuse_threads,
            fuse_clone_fd: cli.fuse_clone_fd.or(file.fuse_clone_fd).unwrap_or(true),
            fuse_workers: fuse.fuse_workers,
            fuse_max_tasks: fuse.fuse_max_tasks,
            dfs_read_pipeline: fuse.dfs_read_pipeline,
            fuse_max_readahead_bytes: fuse.fuse_max_readahead_bytes,
            dfs_version_cache_entries,
            dfs_version_cache_bytes,
            dfs_outboard_work_wait_ms: tuning.dfs_outboard_work_wait_ms,
            dfs_catalog_commit_queue: tuning.dfs_catalog_commit_queue,
            dfs_write_queue_bytes: tuning.dfs_write_queue_bytes,
            dfs_dirty_budget_bytes: tuning.dfs_dirty_budget_bytes,
            dfs_write_timeout_ms: tuning.dfs_write_timeout_ms,
            dfs_gc_grace_ms: tuning.dfs_gc_grace_ms,
            dfs_unpublished_descriptors: tuning.dfs_unpublished_descriptors,
            dfs_upload_inflight_chunks: tuning.dfs_upload_inflight_chunks,
            dfs_background_commit_ms: tuning.dfs_background_commit_ms,
            dfs_sidecar_install_queue: tuning.dfs_sidecar_install_queue,
            dfs_external_gc_list_pages_per_pass: tuning.dfs_external_gc_list_pages_per_pass,
            dfs_fetch_max_inflight_objects: tuning.dfs_fetch_max_inflight_objects,
            dfs_readahead_max_bytes: tuning.dfs_readahead_max_bytes,
            dfs_peer_serve_resident_objects: tuning.dfs_peer_serve_resident_objects,
            dfs_peer_serve_payload_bytes: tuning.dfs_peer_serve_payload_bytes,
            dfs_peer_serve_wait_ms: tuning.dfs_peer_serve_wait_ms,
            dfs_scrub_bytes_per_second: tuning.dfs_scrub_bytes_per_second,
            dfs_scrub_start_delay_seconds: tuning.dfs_scrub_start_delay_seconds,
            dfs_scrub_round_interval_seconds: tuning.dfs_scrub_round_interval_seconds,
            dfs_kernel_cache_ttl_ms: tuning.dfs_kernel_cache_ttl_ms,
            dfs_external_backend: file
                .dfs_external_backend
                .map(DfsExternalBackendConfig::with_defaults),
            dfs_peer_cache: file.dfs_peer_cache,
            dfs_write_buffer_idle_bytes: file
                .dfs_write_buffer_idle_bytes
                .unwrap_or(128 * 1024 * 1024),
            dfs_desired_copies: replication.dfs_desired_copies,
            dfs_sync_required_copies: replication.dfs_sync_required_copies,
            dfs_min_distinct_nodes: replication.dfs_min_distinct_nodes,
            dfs_min_distinct_failure_domains: replication.dfs_min_distinct_failure_domains,
            dfs_local_copy: replication.dfs_local_copy,
            dfs_replica_failure_policy: replication.dfs_replica_failure_policy,
            dfs_min_degraded_copies: replication.dfs_min_degraded_copies,
            dfs_read_max_ops_per_batch: read.dfs_read_max_ops_per_batch,
            dfs_read_max_inflight_bytes: read.dfs_read_max_inflight_bytes,
            dfs_source_cache_max_ms: read.dfs_source_cache_max_ms,
            dfs_source_lookahead_chunks: read.dfs_source_lookahead_chunks,
            dfs_source_cache_bytes: read.dfs_source_cache_bytes,
            dfs_read_crc_mode,
            dfs_read_diagnostics_enabled: cli
                .dfs_read_diagnostics_enabled
                .or(file.dfs_read_diagnostics_enabled)
                .unwrap_or(false),
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
        }
    }
}

/// Values resolved and range-checked before the configuration literal.
struct Resolved {
    id: String,
    ownerfs: bool,
    dfs: bool,
    data_mode: DataMode,
    dfs_read_crc_mode: DfsReadCrcMode,
    replication: dfs::DfsReplication,
    read: dfs::DfsReadBudget,
    fuse: fuse::FuseSettings,
    version_cache: (usize, u64),
    tuning: dfs::DfsTuning,
    meta: meta::MetaSettings,
    grpc_listen: SocketAddr,
    outlet: Option<OutletConfig>,
}

fn invalid(message: impl Into<String>) -> Error {
    afs_error::Error::coded(afs_error::CONFIG_INVALID, message)
}

#[cfg(test)]
mod certificate_tests {
    use super::*;

    #[cfg(feature = "dfs")]
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
        assert_eq!(cfg.dfs_read_crc_mode, DfsReadCrcMode::Receiver);
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
    fn dfs_external_backend_payload_defaults_follow_the_endpoint_scheme() {
        use DfsExternalPayloadChecksum::{Crc64nvme, None as NoChecksum};
        let parse = |endpoint: &str, extra: &str| -> DfsExternalBackendConfig {
            toml::from_str(&format!(
                "store_id = \"filesystem-a\"\ngeneration = \"generation-1\"\nendpoint = \"{endpoint}\"\nbucket = \"chunks\"\nregion = \"us-east-1\"\nallow_http = true\n{extra}"
            ))
            .unwrap()
        };
        // (endpoint, explicit settings) -> (unsigned payload, checksum header)
        for (endpoint, extra, unsigned, checksum) in [
            // TLS: unsigned body, CRC64NVME header.
            ("https://s3.example.invalid", "", true, Crc64nvme),
            // Plaintext: the signed payload stays.
            ("http://127.0.0.1:9000", "", false, NoChecksum),
            // Explicit settings win in both directions.
            (
                "https://s3.example.invalid",
                "unsigned_payload = false\n",
                false,
                NoChecksum,
            ),
            (
                "http://127.0.0.1:9000",
                "unsigned_payload = true\n",
                true,
                Crc64nvme,
            ),
            (
                "https://s3.example.invalid",
                "payload_checksum = \"none\"\n",
                true,
                NoChecksum,
            ),
            (
                "https://s3.example.invalid",
                "unsigned_payload = false\npayload_checksum = \"crc64nvme\"\n",
                false,
                Crc64nvme,
            ),
        ] {
            let config = parse(endpoint, extra);
            config.validate().unwrap();
            assert_eq!(
                config.effective_unsigned_payload(),
                unsigned,
                "{endpoint} {extra}"
            );
            assert_eq!(
                config.effective_payload_checksum(),
                checksum,
                "{endpoint} {extra}"
            );
            let explicit = config.with_defaults();
            assert_eq!(explicit.unsigned_payload, Some(unsigned));
            assert_eq!(explicit.payload_checksum, Some(checksum));
            assert_eq!(explicit.upload_concurrency, Some(8));
            assert_eq!(explicit.worker_threads, Some(4));
        }
    }

    #[test]
    fn dfs_external_backend_upload_settings_default_and_validate() {
        let base = r#"
store_id = "filesystem-a"
generation = "generation-1"
endpoint = "https://s3.example.invalid"
bucket = "chunks"
region = "us-east-1"
"#;
        let default: DfsExternalBackendConfig = toml::from_str(base).unwrap();
        default.validate().unwrap();
        assert_eq!(default.effective_upload_concurrency(), 8);
        assert_eq!(default.effective_worker_threads(), 4);

        let explicit: DfsExternalBackendConfig = toml::from_str(&format!(
            "{base}payload_checksum = \"crc64nvme\"\nupload_concurrency = 2\n"
        ))
        .unwrap();
        explicit.validate().unwrap();
        assert_eq!(
            explicit.payload_checksum,
            Some(DfsExternalPayloadChecksum::Crc64nvme)
        );
        assert_eq!(explicit.effective_upload_concurrency(), 2);
        assert_eq!(explicit.effective_worker_threads(), 2);

        for line in [
            "upload_concurrency = 0",
            "upload_concurrency = 9",
            "worker_threads = 0",
            "worker_threads = 9",
        ] {
            let invalid: DfsExternalBackendConfig =
                toml::from_str(&format!("{base}{line}\n")).unwrap();
            assert!(invalid.validate().is_err(), "{line} must be rejected");
        }
        assert!(
            toml::from_str::<DfsExternalBackendConfig>(&format!(
                "{base}payload_checksum = \"md5\"\n"
            ))
            .is_err()
        );
    }
}

#[cfg(all(test, feature = "dfs"))]
mod crc32c_config_tests {
    use super::*;
    #[test]
    fn crc32c_write_mode_follows_storage_and_has_no_switch() {
        // S1a-7 (R21, F6): native storage always writes CRC32C objects with
        // write-generation GC fences; the two switches no longer exist.
        for flag in ["--dfs-crc32c-writes", "--dfs-optimistic-gc-commit"] {
            assert!(Cli::try_parse_from(["afs-node", "--fs", "dfs", flag, "true"]).is_err());
        }
        for key in ["dfs_crc32c_writes", "dfs_optimistic_gc_commit"] {
            let file = tempfile::NamedTempFile::new().unwrap();
            std::fs::write(file.path(), format!("fs = \"dfs\"\n{key} = true\n")).unwrap();
            let path = file.path().to_str().unwrap();
            assert!(
                Cli::try_parse_from(["afs-node", "--config", path])
                    .map_err(|error| error.to_string())
                    .and_then(|cli| Config::resolve(Role::Node, cli).map_err(|e| e.to_string()))
                    .is_err(),
                "{key} must be rejected"
            );
        }
    }
}

#[cfg(all(test, feature = "dfs"))]
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

#[cfg(all(test, feature = "dfs"))]
mod crc_read_modes_validation_tests {
    use super::*;
    #[test]
    fn crc_read_modes_cli_defaults_and_scope_preserve_write_crc() {
        for mode in ["receiver", "off"] {
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
        }
        let cfg =
            Config::resolve(Role::Node, Cli::parse_from(["afs-node", "--fs", "dfs"])).unwrap();
        assert_eq!(cfg.dfs_read_crc_mode, DfsReadCrcMode::Receiver);
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
mod crc_read_default_receiver_tests {
    use super::*;

    /// Remote reads verify bodies by default so a bad replica is never handed
    /// to the application (D-S4-1); `off` stays an explicit opt-out.
    #[cfg(feature = "dfs")]
    #[test]
    fn crc_read_default_receiver_keeps_native_write_checks_and_optional_off() {
        let cfg =
            Config::resolve(Role::Node, Cli::parse_from(["afs-node", "--fs", "dfs"])).unwrap();
        assert_eq!(cfg.dfs_read_crc_mode, DfsReadCrcMode::Receiver);
        assert_eq!(DfsReadCrcMode::default(), DfsReadCrcMode::Receiver);
        assert_eq!(
            serde_json::to_value(&cfg).unwrap()["dfs_read_crc_mode"],
            "receiver"
        );
        for mode in ["receiver", "off"] {
            let cfg = Config::resolve(
                Role::Node,
                Cli::parse_from(["afs-node", "--fs", "dfs", "--dfs-read-crc-mode", mode]),
            )
            .unwrap();
            assert_eq!(cfg.dfs_read_crc_mode.as_str(), mode);
        }
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn crc_read_mode_both_is_not_configurable() {
        assert!(
            Cli::try_parse_from(["afs-node", "--fs", "dfs", "--dfs-read-crc-mode", "both"])
                .is_err()
        );
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            "fs = \"dfs\"\ndata_mode = \"grpc\"\ndfs_read_crc_mode = \"both\"\n",
        )
        .unwrap();
        let cli = Cli::parse_from(["afs-node", "--config", file.path().to_str().unwrap()]);
        assert!(Config::resolve(Role::Node, cli).is_err());
    }

    #[test]
    fn crc_read_default_off_does_not_activate_native_policy_on_other_backends() {
        // Node rejects a backend this build did not compile; Meta resolves any `fs`.
        #[cfg(feature = "ownerfs")]
        let roles = [Role::Node, Role::Meta];
        #[cfg(not(feature = "ownerfs"))]
        let roles = [Role::Meta];
        for role in roles {
            let cfg = Config::resolve(role, Cli::parse_from(["afs", "--fs", "ownerfs"])).unwrap();
            assert_eq!(cfg.dfs_read_crc_mode, DfsReadCrcMode::Both);
        }
        #[cfg(feature = "dfs")]
        {
            let cfg = Config::resolve(
                Role::Node,
                Cli::parse_from(["afs-node", "--fs", "dfs", "--data-mode", "auto"]),
            )
            .unwrap();
            assert_eq!(cfg.dfs_read_crc_mode, DfsReadCrcMode::Both);
        }
    }

    #[cfg(feature = "dfs")]
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
        let cli = Config::resolve(
            Role::Node,
            Cli::parse_from(["afs-node", "--config", path, "--dfs-read-crc-mode", "off"]),
        )
        .unwrap();
        assert_eq!(cli.dfs_read_crc_mode, DfsReadCrcMode::Off);
    }
}

#[cfg(test)]
mod read_concurrency_config_tests {
    use super::*;

    // FUSE task capacity is shared by both backends; use one this build compiled.
    const BACKEND: &str = if cfg!(feature = "dfs") {
        "dfs"
    } else {
        "ownerfs"
    };

    #[test]
    fn read_concurrency_admission_is_independent_of_workers() {
        let cli = Cli::try_parse_from([
            "afs-node",
            "--fs",
            BACKEND,
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
            Cli::parse_from(["afs-node", "--fs", BACKEND, "--fuse-workers", "3"]),
        )
        .unwrap();
        assert_eq!(serde_json::to_value(cfg).unwrap()["fuse_max_tasks"], 24);
    }

    #[test]
    fn read_concurrency_admission_rejects_zero() {
        let cli =
            Cli::try_parse_from(["afs-node", "--fs", BACKEND, "--fuse-max-tasks", "0"]).unwrap();
        assert!(Config::resolve(Role::Node, cli).is_err());
    }
}
