//! OwnerFs mount and workspace bind checks (OwnerFs 同事代码, moved verbatim).

use super::{Config, Role, invalid};
use afs_error::Result;
use std::path::Component;

pub(super) fn validate(role: Role, cfg: &Config) -> Result<()> {
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
