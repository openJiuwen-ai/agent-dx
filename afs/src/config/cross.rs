//! Checks across domains: mount ownership and the read CRC mode's backend.

use super::{Config, DfsReadCrcMode, invalid};
use afs_error::Result;

/// `dfs_mount` needs DFS, and the two mounts must differ.
pub(super) fn validate_mounts(cfg: &Config) -> Result<()> {
    if cfg.dfs_mount.is_some() && !cfg.dfs {
        return Err(invalid("dfs_mount requires fs=dfs or fs=all"));
    }
    if cfg.ownerfs_mount.is_some() && cfg.ownerfs_mount == cfg.dfs_mount {
        return Err(invalid(
            "ownerfs_mount and dfs_mount must be different paths",
        ));
    }
    Ok(())
}

pub(super) fn validate_read_crc(cfg: &Config) -> Result<()> {
    if cfg.dfs_read_crc_mode != DfsReadCrcMode::Both
        && (!cfg.dfs || cfg.dfs_external_backend.is_some() || cfg.data_mode != "grpc")
    {
        return Err(invalid("dfs_read_crc_mode requires native DFS grpc"));
    }
    Ok(())
}
