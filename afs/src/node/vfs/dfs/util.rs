use super::*;

pub(super) fn owner_request_context() -> RequestContext {
    RequestContext {
        uid: 0,
        gid: 0,
        pid: 0,
        umask: 0,
        supplementary_gids: Vec::new(),
    }
}

// Renewal thresholds belong to the fault layer's lease keeper.
pub(super) use crate::node::fault::lease_keeper::{
    ensure_fresh, should_background_renew, should_renew,
};

pub(super) fn has_o_dsync(flags: i32) -> bool {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        flags & libc::O_DSYNC != 0
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        let _ = flags;
        false
    }
}

pub(super) fn file_kind(kind: InodeKind) -> FileKind {
    match kind {
        InodeKind::Regular => FileKind::Regular,
        InodeKind::Directory => FileKind::Directory,
        InodeKind::Symlink => FileKind::Symlink,
        InodeKind::Special(kind) => FileKind::Special(vfs_special_kind(kind)),
    }
}

pub(super) fn dfs_special_kind(kind: SpecialFileKind) -> SpecialNodeKind {
    match kind {
        SpecialFileKind::Fifo => SpecialNodeKind::Fifo,
        SpecialFileKind::Socket => SpecialNodeKind::Socket,
        SpecialFileKind::BlockDevice { rdev } => SpecialNodeKind::BlockDevice { rdev },
        SpecialFileKind::CharDevice { rdev } => SpecialNodeKind::CharDevice { rdev },
    }
}

fn vfs_special_kind(kind: SpecialNodeKind) -> SpecialFileKind {
    match kind {
        SpecialNodeKind::Fifo => SpecialFileKind::Fifo,
        SpecialNodeKind::Socket => SpecialFileKind::Socket,
        SpecialNodeKind::BlockDevice { rdev } => SpecialFileKind::BlockDevice { rdev },
        SpecialNodeKind::CharDevice { rdev } => SpecialFileKind::CharDevice { rdev },
    }
}

pub(super) fn dfs_owner_special_node(
    kind: SpecialFileKind,
) -> afs_protocol::node_control::DfsOwnerSpecialNode {
    let (kind, rdev) = match kind {
        SpecialFileKind::Fifo => (afs_protocol::node_control::DfsOwnerSpecialNodeKind::Fifo, 0),
        SpecialFileKind::Socket => (
            afs_protocol::node_control::DfsOwnerSpecialNodeKind::Socket,
            0,
        ),
        SpecialFileKind::BlockDevice { rdev } => (
            afs_protocol::node_control::DfsOwnerSpecialNodeKind::BlockDevice,
            rdev,
        ),
        SpecialFileKind::CharDevice { rdev } => (
            afs_protocol::node_control::DfsOwnerSpecialNodeKind::CharDevice,
            rdev,
        ),
    };
    afs_protocol::node_control::DfsOwnerSpecialNode {
        kind: kind.into(),
        rdev,
    }
}

pub(super) fn attributes(inode: &InodeRecord, size: u64, blocks: u64) -> FileAttributes {
    let attrs = &inode.attributes;
    FileAttributes {
        kind: file_kind(inode.kind),
        size,
        blocks,
        mode: attrs.mode,
        uid: attrs.uid,
        gid: attrs.gid,
        nlink: attrs.nlink,
        atime: unix_ms(attrs.atime_unix_ms),
        mtime: unix_ms(attrs.mtime_unix_ms),
        ctime: unix_ms(attrs.ctime_unix_ms),
    }
}

pub(super) fn file_attributes_from_owner_attr(
    attr: afs_protocol::node_control::DfsOwnerFileAttr,
) -> Result<FileAttributes> {
    let kind = match attr.kind {
        1 => FileKind::Regular,
        2 => FileKind::Directory,
        3 => FileKind::Symlink,
        _ => {
            return Err(invalid("DFS owner getattr returned unsupported file kind"));
        }
    };
    Ok(FileAttributes {
        kind,
        size: attr.size,
        blocks: attr.size.div_ceil(512),
        mode: attr.mode,
        uid: attr.uid,
        gid: attr.gid,
        nlink: attr.nlink,
        atime: unix_ms(attr.atime_unix_ms),
        mtime: unix_ms(attr.mtime_unix_ms),
        ctime: unix_ms(attr.ctime_unix_ms),
    })
}

pub(super) use crate::clock::now_unix_ms;

fn unix_ms(value: u64) -> SystemTime {
    UNIX_EPOCH + std::time::Duration::from_millis(value)
}

pub(super) fn system_time_ms(value: SystemTime) -> u64 {
    value
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

pub(super) fn allocated_blocks(
    layout: &LayoutRoot,
    logical_length: u64,
    frozen: Option<&DirtyExtentMap>,
    active: &DirtyExtentMap,
) -> Result<u64> {
    let mut ranges = Vec::new();
    for extent in &layout.inline_extents {
        push_allocated_range(
            &mut ranges,
            extent.file_offset,
            extent.length,
            logical_length,
        )?;
    }
    if let Some(frozen) = frozen {
        push_dirty_allocated_ranges(&mut ranges, frozen, logical_length)?;
    }
    push_dirty_allocated_ranges(&mut ranges, active, logical_length)?;
    ranges_to_blocks(ranges)
}

fn push_dirty_allocated_ranges(
    ranges: &mut Vec<(u64, u64)>,
    map: &DirtyExtentMap,
    logical_length: u64,
) -> Result<()> {
    for extent in map.iter_extents() {
        if extent.data.is_some() || extent.builder.is_some() || extent.native_upload.is_some() {
            push_allocated_range(ranges, extent.file_offset, extent.length, logical_length)?;
        }
    }
    Ok(())
}

fn push_allocated_range(
    ranges: &mut Vec<(u64, u64)>,
    offset: u64,
    length: u64,
    logical_length: u64,
) -> Result<()> {
    if offset >= logical_length || length == 0 {
        return Ok(());
    }
    let end = offset
        .checked_add(length)
        .ok_or_else(|| invalid("allocated range overflow"))?
        .min(logical_length);
    if end > offset {
        ranges.push((offset, end));
    }
    Ok(())
}

fn ranges_to_blocks(mut ranges: Vec<(u64, u64)>) -> Result<u64> {
    if ranges.is_empty() {
        return Ok(0);
    }
    ranges.sort_by_key(|range| range.0);
    let mut total = 0u64;
    let (mut start, mut end) = ranges[0];
    for (next_start, next_end) in ranges.into_iter().skip(1) {
        if next_start <= end {
            end = end.max(next_end);
        } else {
            total = total
                .checked_add(end - start)
                .ok_or_else(|| invalid("allocated byte count overflow"))?;
            start = next_start;
            end = next_end;
        }
    }
    total = total
        .checked_add(end - start)
        .ok_or_else(|| invalid("allocated byte count overflow"))?;
    Ok(total.div_ceil(512))
}

pub(super) fn caller_context(ctx: &RequestContext) -> CallerContext {
    CallerContext {
        uid: ctx.uid,
        gid: ctx.gid,
        supplementary_gids: ctx.supplementary_gids.clone(),
    }
}

pub(super) fn slice_directory_entries(
    mut rows: Vec<DirectoryEntry>,
    cookie: u64,
    max_entries: usize,
) -> Result<Vec<DirectoryEntry>> {
    let start = usize::try_from(cookie).map_err(|_| invalid("directory cookie is too large"))?;
    rows.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(rows
        .into_iter()
        .enumerate()
        .skip(start)
        .take(max_entries)
        .map(|(index, mut entry)| {
            entry.next_cookie = u64::try_from(index + 1).unwrap_or(u64::MAX);
            entry
        })
        .collect())
}

pub(super) fn rename_mode(flags: RenameFlags) -> Result<RenameMode> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        if flags.0 == 0 {
            return Ok(RenameMode::Replace);
        }
        if flags.0 == libc::RENAME_NOREPLACE {
            return Ok(RenameMode::NoReplace);
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        if flags.0 == 0 {
            return Ok(RenameMode::Replace);
        }
    }
    Err(Error::coded(
        afs_error::NODE_VFS_UNIMPLEMENTED,
        "DFS rename supports only default replace and RENAME_NOREPLACE",
    ))
}

pub(super) fn xattr_set_mode(flags: i32) -> Result<XattrSetMode> {
    if flags == 0 {
        return Ok(XattrSetMode::Upsert);
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        if flags == libc::XATTR_CREATE {
            return Ok(XattrSetMode::Create);
        }
        if flags == libc::XATTR_REPLACE {
            return Ok(XattrSetMode::Replace);
        }
    }
    Err(invalid("invalid DFS xattr set flags"))
}

pub(super) fn invalid(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_VFS_INVALID, message)
}

pub(super) fn lock_error(error: LockError) -> Error {
    Error::from(std::io::Error::from_raw_os_error(error.errno()))
}

pub(super) fn file_too_large() -> Error {
    Error::from(std::io::Error::from_raw_os_error(libc::EFBIG))
}

pub(super) fn unavailable(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_VFS_UNAVAILABLE, message)
}

pub(super) fn is_definite_commit_rejection(error: &Error) -> bool {
    matches!(
        error.code(),
        afs_error::META_CATALOG_INVALID_REQUEST
            | afs_error::META_DFS_CONFLICT
            | afs_error::META_DFS_GC_CHANGED
    )
}

pub(super) fn stale(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_DFS_STALE_HANDLE, message)
}

pub(super) fn bad_file_descriptor(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_BAD_FILE_DESCRIPTOR, message)
}
