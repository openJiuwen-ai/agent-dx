//! Process identity, filesystem selection, data mode and process-level checks.

use super::{Cli, Config, FileConfig, Role, invalid};
use afs_error::Result;
use std::path::Path;

/// The TOML layer (`--config`), or defaults when no file is given.
pub(super) fn read_file(path: Option<&Path>) -> Result<FileConfig> {
    let file: FileConfig = match path {
        Some(path) => toml::from_str(&std::fs::read_to_string(path)?)
            .map_err(|error| invalid(error.to_string()))?,
        None => FileConfig::default(),
    };
    Ok(file)
}

/// Options that only afs-meta accepts.
pub(super) fn reject_node_meta_options(role: Role, cli: &Cli, file: &FileConfig) -> Result<()> {
    if role == Role::Node
        && (cli.ownerfs_mkdir_load_balance.is_some() || file.ownerfs_mkdir_load_balance.is_some())
    {
        return Err(invalid(
            "ownerfs_mkdir_load_balance is only valid for afs-meta",
        ));
    }
    Ok(())
}

pub(super) fn resolve_id(role: Role, cli: &mut Cli, file: &mut FileConfig) -> Result<String> {
    let id = cli.id.take().or(file.id.take()).unwrap_or_else(|| {
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
    Ok(id)
}

/// `(ownerfs, dfs)`: the requested filesystems, checked against this build.
pub(super) fn resolve_filesystems(
    role: Role,
    cli: &mut Cli,
    file: &mut FileConfig,
) -> Result<(bool, bool)> {
    // cfg! 查询本次二进制的编译能力；Backend 内的 #[cfg] 才实际裁掉后端模块。
    let requested = cli.fs.take().or(file.fs.take());
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
    Ok((ownerfs, dfs))
}

pub(super) fn resolve_data_mode(cli: &mut Cli, file: &mut FileConfig) -> Result<String> {
    let data_mode = cli
        .data_mode
        .take()
        .or(file.data_mode.take())
        .unwrap_or_else(|| "grpc".into());
    if !["grpc", "rdma", "auto"].contains(&data_mode.as_str()) {
        return Err(invalid("data_mode must be grpc, rdma or auto"));
    }
    if data_mode == "rdma" && !cfg!(feature = "rdma") {
        return Err(invalid("rdma feature is not compiled in"));
    }
    Ok(data_mode)
}

/// Timeout, tracing, log level, endpoint URIs and absolute paths.
pub(super) fn validate_process(cfg: &Config) -> Result<()> {
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
    Ok(())
}
