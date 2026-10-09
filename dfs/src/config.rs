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
    #[serde(rename = "etcd")]
    #[value(name = "etcd")]
    Etcd,
    #[serde(rename = "redis")]
    #[value(name = "redis")]
    Redis,
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
    /// etcd 后端的连接端点；memory 后端不使用。
    #[arg(long)]
    pub etcd_endpoint: Option<String>,
    /// Redis 持久 Meta 后端的连接 URI；例如 redis://127.0.0.1:6379/0。
    #[arg(long)]
    pub redis_endpoint: Option<String>,
    /// Meta 后端：etcd（默认）、local-file 或 memory。
    #[arg(long, value_enum)]
    pub meta_store: Option<MetaStoreBackend>,
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
    #[arg(long)]
    pub dfs_mount: Option<PathBuf>,
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
    pub dfs_read_source_cache_ttl_ms: Option<u64>,
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
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
/// TOML 输入层。拒绝未知字段，让拼错开关在启动时失败，而不是静默使用默认行为。
struct FileConfig {
    id: Option<String>,
    grpc_listen: Option<SocketAddr>,
    rest_listen: Option<SocketAddr>,
    meta_endpoint: Option<String>,
    etcd_endpoint: Option<String>,
    redis_endpoint: Option<String>,
    meta_store: Option<MetaStoreBackend>,
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
    dfs_mount: Option<PathBuf>,
    dfs_desired_copies: Option<u16>,
    dfs_sync_required_copies: Option<u16>,
    dfs_min_distinct_nodes: Option<u16>,
    dfs_min_distinct_failure_domains: Option<u16>,
    dfs_local_copy: Option<String>,
    dfs_read_max_ops_per_batch: Option<usize>,
    dfs_read_max_inflight_bytes: Option<u64>,
    dfs_read_source_cache_ttl_ms: Option<u64>,
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
    pub etcd_endpoint: Option<String>,
    pub redis_endpoint: Option<String>,
    pub meta_store: MetaStoreBackend,
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
    pub dfs_mount: Option<PathBuf>,
    pub dfs_desired_copies: u16,
    pub dfs_sync_required_copies: u16,
    pub dfs_min_distinct_nodes: u16,
    pub dfs_min_distinct_failure_domains: u16,
    pub dfs_local_copy: String,
    pub dfs_read_max_ops_per_batch: usize,
    pub dfs_read_max_inflight_bytes: u64,
    pub dfs_read_source_cache_ttl_ms: u64,
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
            Some(path) => toml::from_str(&std::fs::read_to_string(path)?)
                .map_err(|e| invalid(e.to_string()))?,
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
        let dfs_read_source_cache_ttl_ms = cli
            .dfs_read_source_cache_ttl_ms
            .or(file.dfs_read_source_cache_ttl_ms)
            .unwrap_or(500);
        if dfs_read_max_ops_per_batch == 0
            || dfs_read_max_inflight_bytes == 0
            || dfs_read_source_cache_ttl_ms > 60_000
        {
            return Err(invalid(
                "DFS read budgets must be non-zero and source cache TTL must be <= 60000 ms",
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
            etcd_endpoint: cli.etcd_endpoint.or(file.etcd_endpoint),
            redis_endpoint: cli.redis_endpoint.or(file.redis_endpoint),
            meta_store: cli
                .meta_store
                .or(file.meta_store)
                .unwrap_or(MetaStoreBackend::Etcd),
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
            dfs_mount: cli.dfs_mount.or(file.dfs_mount),
            dfs_desired_copies,
            dfs_sync_required_copies,
            dfs_min_distinct_nodes,
            dfs_min_distinct_failure_domains,
            dfs_local_copy,
            dfs_read_max_ops_per_batch,
            dfs_read_max_inflight_bytes,
            dfs_read_source_cache_ttl_ms,
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
            &cfg.etcd_endpoint,
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
        if let Some(endpoint) = &cfg.redis_endpoint {
            redis::Client::open(endpoint.as_str()).map_err(|e| invalid(e.to_string()))?;
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
            let bind = cfg.ownerfs_workspace_bind.as_ref().ok_or_else(|| {
                invalid(
                    "experimental_ownerfs_workspace_bind requires administrator ownerfs_workspace_bind settings",
                )
            })?;
            validate_ownerfs_workspace_name(&bind.workspace)?;
        }
        if cfg.experimental_native_workspace {
            if role != Role::Node || !cfg.ownerfs || cfg.ownerfs_mount.is_none() {
                return Err(invalid(
                    "experimental_native_workspace requires Node OwnerFs with a mount",
                ));
            }
            let native = cfg.native_workspace.as_ref().ok_or_else(|| invalid(
                "experimental_native_workspace requires administrator native_workspace settings"
            ))?;
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
        Ok(cfg)
    }

    /// gRPC builders共用安全配置；只有配置了完整证书集合才启用 mTLS。
    /// OwnerFiles 远端业务还要求 `trusted_node_certs` 与证书身份匹配。
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
}
