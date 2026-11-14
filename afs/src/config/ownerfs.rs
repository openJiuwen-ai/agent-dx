//! OwnerFs mount and workspace bind checks (OwnerFs 同事代码, moved verbatim).

use super::{Config, Role, invalid};
use afs_error::Result;
use std::path::Component;

pub(super) fn validate(role: Role, cfg: &Config) -> Result<()> {
    if let Some(api) = &cfg.ownerfs_workspace_api {
        if role != Role::Node || !cfg.ownerfs || cfg.ownerfs_mount.is_none() || cfg.outlet.is_none()
        {
            return Err(invalid(
                "ownerfs_workspace_api requires Node OwnerFs with a mount and outlet",
            ));
        }
        if [cfg.grpc_listen, cfg.rest_listen].iter().any(|other| {
            api.listen.port() == other.port()
                && (api.listen.ip() == other.ip()
                    || api.listen.ip().is_unspecified()
                    || other.ip().is_unspecified())
        }) {
            return Err(invalid(
                "ownerfs_workspace_api.listen conflicts with another listener",
            ));
        }
    }
    if cfg.ownerfs_workspace_default_uid == Some(u32::MAX)
        || cfg.ownerfs_workspace_default_gid == Some(u32::MAX)
        || cfg.ownerfs_workspace_default_user.is_empty()
        || cfg.ownerfs_workspace_default_user.as_bytes().contains(&0)
    {
        return Err(invalid("invalid OwnerFs workspace default identity"));
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
        // The egress root must exist as a stable, admin-configured native
        // path: absolute, no parent traversal, and outside both the FUSE
        // mount and the private data directory so bind targets never cover
        // the authoritative FUSE tree (D1/D6).
        let egress_root = cfg.ownerfs_workspace_egress_root.as_ref().ok_or_else(|| {
            invalid("experimental_ownerfs_workspace_bind requires ownerfs_workspace_egress_root")
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
    Ok(())
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

#[cfg(feature = "ownerfs")]
impl Config {
    pub(crate) fn workspace_default_identity(&self) -> Result<(u32, u32)> {
        let lookup = |flag, configured| -> Result<u32> {
            if let Some(value) = configured {
                return Ok(value);
            }
            let output = std::process::Command::new("id")
                .args([flag, "--", &self.ownerfs_workspace_default_user])
                .output()
                .map_err(|error| invalid(format!("workspace account lookup failed: {error}")))?;
            if !output.status.success() {
                return Ok(1000);
            }
            std::str::from_utf8(&output.stdout)
                .ok()
                .and_then(|text| text.trim().parse::<u32>().ok())
                .filter(|value| *value != u32::MAX)
                .ok_or_else(|| invalid("workspace account lookup returned an invalid identity"))
        };
        Ok((
            lookup("-u", self.ownerfs_workspace_default_uid)?,
            lookup("-g", self.ownerfs_workspace_default_gid)?,
        ))
    }
}

#[cfg(all(test, feature = "ownerfs"))]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn workspace_defaults_resolve_home_account_and_explicit_overrides() {
        let mut cfg =
            Config::resolve(Role::Node, super::super::Cli::parse_from(["afs-node"])).unwrap();
        cfg.ownerfs_workspace_default_user = "root".into();
        assert_eq!(cfg.workspace_default_identity().unwrap(), (0, 0));
        cfg.ownerfs_workspace_default_user = format!("afs-missing-{}", uuid::Uuid::new_v4());
        assert_eq!(cfg.workspace_default_identity().unwrap(), (1000, 1000));
        cfg.ownerfs_workspace_default_uid = Some(0);
        cfg.ownerfs_workspace_default_gid = Some(12345);
        assert_eq!(cfg.workspace_default_identity().unwrap(), (0, 12345));
    }
}
