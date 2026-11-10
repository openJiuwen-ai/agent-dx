//! Request-side validation and credential helpers.

use crate::node::vfs::types::{RequestContext, SyncMode};
use fuser::TimeOrNow;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

static NEXT_INGRESS_SESSION: AtomicU64 = AtomicU64::new(1);

pub(super) fn unsupported_lock_errno(start: u64, end: u64, typ: i32) -> i32 {
    if start > end || !matches!(typ, libc::F_RDLCK | libc::F_WRLCK | libc::F_UNLCK) {
        libc::EINVAL
    } else {
        libc::EOPNOTSUPP
    }
}

pub(super) fn validate_xattr_set(flags: i32, position: u32) -> std::result::Result<(), i32> {
    // Linux has no positional xattrs. Never forward a platform extension or an
    // invalid CREATE+REPLACE combination as a successful upsert.
    if position != 0 || !matches!(flags, 0 | libc::XATTR_CREATE | libc::XATTR_REPLACE) {
        return Err(libc::EINVAL);
    }
    Ok(())
}

pub(super) fn sync_mode(datasync: bool) -> SyncMode {
    if datasync {
        SyncMode::DataOnly
    } else {
        SyncMode::Full
    }
}

pub(super) fn next_ingress_session_id() -> String {
    let sequence = NEXT_INGRESS_SESSION.fetch_add(1, Ordering::Relaxed);
    format!("fuse-{}-{sequence}", std::process::id())
}

pub(super) fn combine_empty_results(
    first: std::result::Result<(), i32>,
    second: std::result::Result<(), i32>,
) -> std::result::Result<(), i32> {
    match (first, second) {
        (Err(error), _) | (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

pub(super) fn time_or_now(value: TimeOrNow) -> SystemTime {
    match value {
        TimeOrNow::SpecificTime(time) => time,
        TimeOrNow::Now => SystemTime::now(),
    }
}

pub(super) fn timestamps_now_only(atime: Option<&TimeOrNow>, mtime: Option<&TimeOrNow>) -> bool {
    let mut saw_timestamp = false;
    for timestamp in [atime, mtime].into_iter().flatten() {
        saw_timestamp = true;
        if !matches!(timestamp, TimeOrNow::Now) {
            return false;
        }
    }
    saw_timestamp
}

pub(super) fn os_str_bytes(value: &OsStr) -> &[u8] {
    use std::os::unix::ffi::OsStrExt;
    value.as_bytes()
}

// The kernel still enforces DefaultPermissions. FUSE headers supply uid/gid,
// but not supplementary groups. Resolve groups only for metadata operations,
// and fail closed if the caller disappeared or changed credentials. Data IO
// does not pay for /proc reads. No PID/group cache survives setgroups().
pub(super) fn request_groups(context: &RequestContext) -> Vec<u32> {
    let path = PathBuf::from(format!("/proc/{}", context.pid));
    let observed = (|| -> Option<Vec<u32>> {
        let before = std::fs::read_to_string(path.join("stat")).ok()?;
        let status = std::fs::read_to_string(path.join("status")).ok()?;
        let second = std::fs::read_to_string(path.join("status")).ok()?;
        let after = std::fs::read_to_string(path.join("stat")).ok()?;
        if process_start(&before)? != process_start(&after)? {
            return None;
        }
        let groups = status_groups(&status, context.uid, context.gid)?;
        if groups != status_groups(&second, context.uid, context.gid)? {
            return None;
        }
        Some(groups)
    })();
    observed.unwrap_or_default()
}

pub(super) fn process_start(stat: &str) -> Option<&str> {
    // comm may contain spaces and ')'; fields after the last ')' start at 3.
    stat.rsplit_once(')')?.1.split_whitespace().nth(19)
}

pub(super) fn status_groups(status: &str, uid: u32, gid: u32) -> Option<Vec<u32>> {
    let fields = |name: &str| status.lines().find_map(|line| line.strip_prefix(name));
    let fs_id =
        |name: &str| -> Option<u32> { fields(name)?.split_whitespace().nth(3)?.parse().ok() };
    if fs_id("Uid:")? != uid || fs_id("Gid:")? != gid {
        return None;
    }
    let mut groups = fields("Groups:")?
        .split_whitespace()
        .map(str::parse::<u32>)
        .collect::<std::result::Result<Vec<_>, _>>()
        .ok()?;
    groups.sort_unstable();
    groups.dedup();
    Some(groups)
}
