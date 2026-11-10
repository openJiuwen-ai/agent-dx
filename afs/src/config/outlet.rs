//! Cross-node outlet name layer configuration.
//!
//! The layer is configured as a unit: a partially configured layer would
//! publish names pointing at an import layout the deployer never described, so
//! resolution rejects it rather than guessing defaults for the missing half.

use super::{Cli, FileConfig, invalid};
use afs_error::Result;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf};

/// Cross-node outlet name layer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OutletConfig {
    /// tmpfs mount holding the published `name -> target` symlinks.
    pub public_root: PathBuf,
    /// Root under which `<home>/users/<data_dir>` trees are mounted.
    pub import_root: PathBuf,
    /// Convergence period. Affects publish latency only, never the data path.
    pub mirror_interval_ms: u64,
    /// H11: minimum lifetime of a superseded generation directory.
    pub generation_grace_ms: u64,
    /// Largest actimeo on any import mount; informational, backs the grace
    /// floor check.
    pub max_import_actimeo_ms: u64,
    /// Path of the users tree on every node, for the source `<home>:<path>`.
    pub users_root: PathBuf,
    /// Options handed to `mount(2)` for every import mount.
    pub mount_options: String,
    /// Address peers use to reach this node's export. Advertised through the
    /// Meta session, so no peer needs to resolve this node's id.
    pub nfs_addr: String,
}

impl OutletConfig {
    #[must_use]
    pub fn mirror_interval(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.mirror_interval_ms)
    }
}

/// Default mount options for the import mounts: kernel NFSv4.1 with `hard`,
/// matching the autofs setup in service today. `nosuid,nodev` are a security
/// requirement, and `hard` keeps a peer outage from turning into a silent
/// short read.
pub const DEFAULT_MOUNT_OPTIONS: &str = "vers=4.1,hard,timeo=600,retrans=2,noresvport,nosuid,nodev";

/// Resolves the layer from CLI over TOML. `None` disables the feature.
///
/// `grpc_listen` is the merged value: it is the default source for the address
/// this node advertises, so it must already be resolved here.
pub(super) fn resolve(
    cli: &Cli,
    file: &FileConfig,
    grpc_listen: SocketAddr,
) -> Result<Option<OutletConfig>> {
    let (Some(public), Some(import_root)) = (
        cli.outlet_public_root
            .clone()
            .or(file.outlet_public_root.clone()),
        cli.outlet_import_root
            .clone()
            .or(file.outlet_import_root.clone()),
    ) else {
        // all-or-none: a half-configured name layer would publish names
        // pointing at an unknown import layout.
        if cli.outlet_public_root.is_some()
            || file.outlet_public_root.is_some()
            || cli.outlet_import_root.is_some()
            || file.outlet_import_root.is_some()
        {
            return Err(invalid(
                "outlet_public_root and outlet_import_root must be set together",
            ));
        }
        return Ok(None);
    };
    if !public.is_absolute() || !import_root.is_absolute() {
        return Err(invalid("outlet paths must be absolute"));
    }
    if public == import_root {
        return Err(invalid("outlet public root and import root must differ"));
    }
    let interval_ms = cli
        .outlet_mirror_interval_ms
        .or(file.outlet_mirror_interval_ms)
        .unwrap_or(250);
    if !(10..=60_000).contains(&interval_ms) {
        return Err(invalid("outlet mirror interval must be 10..=60000ms"));
    }
    let grace_ms = cli
        .outlet_generation_grace_ms
        .or(file.outlet_generation_grace_ms)
        .unwrap_or(120_000);
    if !(1_000..=604_800_000).contains(&grace_ms) {
        return Err(invalid(
            "outlet generation grace must be 1000..=604800000ms",
        ));
    }
    // H11 (spec TC18): the window must cover mirror fan-out (interval x 10) and
    // NFS directory-cache lag (actimeo x 2). `actimeo` is not configured here —
    // it belongs to each import mount — so the deployer supplies the largest
    // one in use; default 60s matches the deployment examples.
    let actimeo_ms = cli
        .outlet_max_import_actimeo_ms
        .or(file.outlet_max_import_actimeo_ms)
        .unwrap_or(60_000);
    let floor = interval_ms
        .saturating_mul(10)
        .max(actimeo_ms.saturating_mul(2));
    if grace_ms < floor {
        return Err(invalid(format!(
            "outlet generation grace ({grace_ms}ms) must be at least \
             max(mirror interval x 10, import actimeo x 2) = {floor}ms"
        )));
    }
    // The mount source is `<home>:<users_root>`. Without it there is nothing
    // to mount, so this is a config error rather than a runtime warning.
    let users_root = cli
        .outlet_users_root
        .clone()
        .or(file.outlet_users_root.clone())
        .ok_or_else(|| invalid("outlet_users_root is required"))?;
    if !users_root.is_absolute() {
        return Err(invalid("outlet_users_root must be absolute"));
    }
    let options = cli
        .outlet_import_mount_options
        .clone()
        .or(file.outlet_import_mount_options.clone())
        .unwrap_or_else(|| DEFAULT_MOUNT_OPTIONS.to_owned());
    if options.trim().is_empty() {
        return Err(invalid("outlet_import_mount_options must not be empty"));
    }
    // This node's own NFS address: what peers will dial. Declared once, here,
    // and carried to every peer through the Meta session, so adding a node
    // never edits another node's config.
    let nfs_addr = match cli.outlet_nfs_addr.clone().or(file.outlet_nfs_addr.clone()) {
        Some(value) => {
            let value = value.trim().to_owned();
            if value.is_empty() || value.contains('/') {
                return Err(invalid("outlet_nfs_addr must be a host, not a URL or path"));
            }
            value
        }
        None => {
            // Same node, so the gRPC listen host is the right default; an
            // unspecified bind gives no address to advertise.
            let ip = grpc_listen.ip();
            if ip.is_unspecified() {
                return Err(invalid(
                    "outlet_nfs_addr is required when grpc_listen \
                     binds an unspecified address",
                ));
            }
            ip.to_string()
        }
    };
    Ok(Some(OutletConfig {
        public_root: public,
        import_root,
        mirror_interval_ms: interval_ms,
        generation_grace_ms: grace_ms,
        max_import_actimeo_ms: actimeo_ms,
        users_root,
        mount_options: options,
        nfs_addr,
    }))
}
