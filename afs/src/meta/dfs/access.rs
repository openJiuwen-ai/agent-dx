//! Namespace view helpers, names and POSIX permission checks.

use super::*;

pub(super) async fn inode_from_view(
    view: &MetaReadView,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<InodeRecord> {
    let root = inode_id.0 == "1";
    match view.read(MetaRead::DfsInode(inode_id)).await?.entity {
        Some(MetaEntity::DfsInode(inode)) if inode.namespace_id == *namespace_id => Ok(inode),
        None if root => Ok(root_inode(namespace_id.clone())),
        _ => Err(Error::coded(
            afs_error::NODE_VFS_NOT_FOUND,
            "DFS inode was not found",
        )),
    }
}

pub(super) async fn dentry_from_view(
    view: &MetaReadView,
    key: DentryKey,
) -> Result<Option<Dentry>> {
    match view.read(MetaRead::DfsDentry(key)).await?.entity {
        Some(MetaEntity::DfsDentry(dentry)) => Ok(Some(dentry)),
        _ => Ok(None),
    }
}

pub(super) async fn directory_entries_from_view(
    view: &MetaReadView,
    namespace_id: &NamespaceId,
    parent_inode_id: &InodeId,
) -> Result<Vec<Dentry>> {
    let snapshot = view
        .read(MetaRead::DfsDirectory {
            namespace_id: namespace_id.clone(),
            parent_inode_id: parent_inode_id.clone(),
        })
        .await?;
    Ok(snapshot
        .entities
        .into_iter()
        .filter_map(|entity| match entity {
            MetaEntity::DfsDentry(dentry) => Some(dentry),
            _ => None,
        })
        .collect())
}

pub(super) async fn directory_is_descendant_or_same(
    view: &MetaReadView,
    namespace_id: &NamespaceId,
    ancestor_inode_id: &InodeId,
    child_inode_id: &InodeId,
) -> Result<bool> {
    if ancestor_inode_id == child_inode_id {
        return Ok(true);
    }
    let mut current = child_inode_id.clone();
    let mut seen = HashSet::new();
    while seen.insert(current.clone()) {
        let Some(parent) = parent_inode_from_view(view, namespace_id, &current).await? else {
            return Ok(false);
        };
        if &parent == ancestor_inode_id {
            return Ok(true);
        }
        if parent.0 == "1" {
            return Ok(false);
        }
        current = parent;
    }
    Ok(true)
}

async fn parent_inode_from_view(
    view: &MetaReadView,
    namespace_id: &NamespaceId,
    inode_id: &InodeId,
) -> Result<Option<InodeId>> {
    if inode_id.0 == "1" {
        return Ok(None);
    }
    let snapshot = view
        .read(MetaRead::DfsDirectoryParent {
            namespace_id: namespace_id.clone(),
            child_inode_id: inode_id.clone(),
        })
        .await?;
    Ok(match snapshot.entity {
        Some(MetaEntity::DfsDentry(dentry)) => Some(dentry.key.parent_inode_id),
        _ => None,
    })
}

pub(super) fn push_parent_namespace_change(
    txn: &mut MetaTxn,
    parent: Option<InodeRecord>,
    delta: i32,
) {
    let Some(mut parent) = parent else {
        return;
    };
    txn.conditions
        .push(TxnCondition::EntityEquals(MetaEntity::DfsInode(
            parent.clone(),
        )));
    let now = now_unix_ms();
    parent.attributes.mtime_unix_ms = now;
    parent.attributes.ctime_unix_ms = now;
    if delta >= 0 {
        parent.attributes.nlink = parent.attributes.nlink.saturating_add(delta as u32);
    } else {
        parent.attributes.nlink = parent.attributes.nlink.saturating_sub(delta.unsigned_abs());
    }
    parent.revision = parent.revision.saturating_add(1);
    txn.mutations
        .push(TxnMutation::Put(MetaEntity::DfsInode(parent)));
}

pub(super) fn create_parent_retry_drift_only(
    previous: &InodeRecord,
    current: &InodeRecord,
) -> bool {
    if previous == current
        || previous.namespace_id != current.namespace_id
        || previous.inode_id != current.inode_id
        || previous.kind != InodeKind::Directory
        || current.kind != InodeKind::Directory
        || previous.head_version != current.head_version
        || previous.symlink_target != current.symlink_target
        || previous.xattrs != current.xattrs
        || previous.attributes.mode != current.attributes.mode
        || previous.attributes.uid != current.attributes.uid
        || previous.attributes.gid != current.attributes.gid
        || previous.attributes.nlink != current.attributes.nlink
        || previous.attributes.atime_unix_ms != current.attributes.atime_unix_ms
    {
        return false;
    }
    current.revision > previous.revision
        && current.attributes.mtime_unix_ms >= previous.attributes.mtime_unix_ms
        && current.attributes.ctime_unix_ms >= previous.attributes.ctime_unix_ms
}

pub(super) fn validate_name(name: &[u8]) -> Result<()> {
    if name.len() > 255 {
        return Err(name_too_long(
            "DFS name exceeds the supported component length",
        ));
    }
    if name.is_empty() || name == b"." || name == b".." || name.contains(&b'/') {
        return Err(invalid_name_error());
    }
    Ok(())
}

pub(super) fn validate_symlink_target(target: &[u8]) -> Result<()> {
    if target.is_empty() {
        return Err(invalid("DFS symlink target must not be empty"));
    }
    if target.len() > 4096 {
        return Err(name_too_long("DFS symlink target exceeds supported length"));
    }
    Ok(())
}

pub(super) fn xattr_key(name: &[u8]) -> Result<Vec<u8>> {
    if name.is_empty() || name.len() > 255 {
        return Err(name_too_long("DFS xattr name length is unsupported"));
    }
    if !name.starts_with(b"user.") {
        return Err(not_supported("DFS currently supports only user.* xattrs"));
    }
    Ok(name.to_vec())
}

#[derive(Clone, Copy)]
pub(super) enum AccessMode {
    Read,
    Write,
}

#[derive(Clone, Copy)]
pub(super) enum XattrAccess {
    Read,
    Write,
}

pub(super) fn validate_attr_update_permission(
    caller: &CallerContext,
    inode: &InodeRecord,
    update: &crate::dfs::InodeAttrUpdate,
) -> Result<()> {
    if let Some(uid) = update.uid
        && caller.uid != 0
        && !(caller.uid == inode.attributes.uid && uid == inode.attributes.uid)
    {
        return Err(operation_not_permitted(
            "DFS chown requires privileged caller context",
        ));
    }
    if let Some(gid) = update.gid
        && caller.uid != 0
        && !(caller.uid == inode.attributes.uid
            && (gid == inode.attributes.gid || caller_in_group(caller, gid)))
    {
        return Err(operation_not_permitted(
            "DFS chgrp requires owner with target group membership or privileged caller context",
        ));
    }
    if update.mode.is_some() && caller.uid != 0 && caller.uid != inode.attributes.uid {
        return Err(operation_not_permitted(
            "DFS chmod requires owner or privileged caller context",
        ));
    }
    if (update.atime_unix_ms.is_some() || update.mtime_unix_ms.is_some())
        && caller.uid != 0
        && caller.uid != inode.attributes.uid
    {
        if !update.timestamps_now {
            return Err(operation_not_permitted(
                "DFS explicit timestamps require owner or privileged caller context",
            ));
        }
        ensure_basic_access(caller, inode, AccessMode::Write)?;
    }
    Ok(())
}

pub(super) fn cleared_write_privileges(mode: u32) -> u32 {
    let clear = 0o4000 | if mode & 0o0010 != 0 { 0o2000 } else { 0 };
    mode & !clear
}

pub(super) fn ensure_xattr_permission(
    caller: &CallerContext,
    inode: &InodeRecord,
    name: &[u8],
    access: XattrAccess,
) -> Result<()> {
    if !matches!(inode.kind, InodeKind::Regular | InodeKind::Directory) {
        return match access {
            XattrAccess::Read => Err(no_data(
                "DFS user xattrs are not available on this inode kind",
            )),
            XattrAccess::Write => Err(operation_not_permitted(
                "DFS user xattrs can be written only on regular files and directories",
            )),
        };
    }
    if matches!(inode.kind, InodeKind::Directory)
        && inode.attributes.mode & 0o1000 != 0
        && matches!(access, XattrAccess::Write)
        && caller.uid != 0
        && caller.uid != inode.attributes.uid
    {
        return Err(operation_not_permitted(
            "DFS sticky directory xattr writes require owner or privileged caller context",
        ));
    }
    let mode = match access {
        XattrAccess::Read => AccessMode::Read,
        XattrAccess::Write => AccessMode::Write,
    };
    let _ = name;
    ensure_basic_access(caller, inode, mode)
}

pub(super) fn inherited_child_gid(caller: &CallerContext, parent: Option<&InodeRecord>) -> u32 {
    if let Some(parent) = parent
        && parent.attributes.mode & 0o2000 != 0
    {
        parent.attributes.gid
    } else {
        caller.gid
    }
}

pub(super) fn ensure_sticky_parent_allows(
    caller: &CallerContext,
    parent: Option<&InodeRecord>,
    victim: &InodeRecord,
) -> Result<()> {
    let Some(parent) = parent else {
        return Ok(());
    };
    if parent.attributes.mode & 0o1000 == 0
        || caller.uid == 0
        || caller.uid == parent.attributes.uid
        || caller.uid == victim.attributes.uid
    {
        Ok(())
    } else {
        Err(permission_denied(
            "DFS sticky directory mutation requires directory owner, victim owner, or privileged caller context",
        ))
    }
}

pub(super) fn ensure_directory_create_access(
    caller: &CallerContext,
    inode: &InodeRecord,
) -> Result<()> {
    if caller.uid == 0 {
        return Ok(());
    }
    let shift = if caller.uid == inode.attributes.uid {
        6
    } else if caller_in_group(caller, inode.attributes.gid) {
        3
    } else {
        0
    };
    if ((inode.attributes.mode >> shift) & 0o3) == 0o3 {
        Ok(())
    } else {
        Err(permission_denied(
            "DFS namespace mutation requires parent write and search permission",
        ))
    }
}

pub(super) fn ensure_basic_access(
    caller: &CallerContext,
    inode: &InodeRecord,
    access: AccessMode,
) -> Result<()> {
    if caller.uid == 0 {
        return Ok(());
    }
    let mask = match access {
        AccessMode::Read => 0o4,
        AccessMode::Write => 0o2,
    };
    let shift = if caller.uid == inode.attributes.uid {
        6
    } else if caller_in_group(caller, inode.attributes.gid) {
        3
    } else {
        0
    };
    if ((inode.attributes.mode >> shift) & mask) != 0 {
        Ok(())
    } else {
        Err(permission_denied(
            "DFS caller lacks inode permission bits for metadata operation",
        ))
    }
}

fn caller_in_group(caller: &CallerContext, gid: u32) -> bool {
    caller.gid == gid || caller.supplementary_gids.contains(&gid)
}
