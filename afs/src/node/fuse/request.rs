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

/// One namespace operation on behalf of the kernel caller. The backend checks
/// the caller against the directories involved, and a FUSE header carries
/// only uid and primary gid. Resolving supplementary groups reads /proc, so it
/// is paid only when the backend refuses that identity: the operation is then
/// sent once more with the groups. A permission refusal changes nothing, so
/// the second attempt is the only one that can take effect.
pub(super) fn with_groups_after_denial<T>(
    mut context: RequestContext,
    groups: impl FnOnce(&RequestContext) -> Vec<u32>,
    operation: impl Fn(&RequestContext) -> afs_error::Result<T>,
) -> afs_error::Result<T> {
    match operation(&context) {
        Err(error) if error.code() == afs_error::IO_PERMISSION_DENIED => {
            let groups = groups(&context);
            if groups.is_empty() {
                return Err(error);
            }
            context.supplementary_gids = groups;
            operation(&context)
        }
        result => result,
    }
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

#[cfg(test)]
mod group_retry_tests {
    use super::*;
    use afs_error::Error;
    use std::sync::Mutex;

    fn group_gated(seen: &Mutex<Vec<Vec<u32>>>, ctx: &RequestContext) -> afs_error::Result<u32> {
        seen.lock().unwrap().push(ctx.supplementary_gids.clone());
        if ctx.gid == 2000 || ctx.supplementary_gids.contains(&2000) {
            Ok(7)
        } else {
            Err(Error::coded(afs_error::IO_PERMISSION_DENIED, "denied"))
        }
    }

    fn namespace_context(gid: u32) -> RequestContext {
        RequestContext {
            uid: 1000,
            gid,
            pid: 1,
            umask: 0,
            supplementary_gids: Vec::new(),
        }
    }

    /// FUSE headers carry no supplementary groups and resolving them reads
    /// /proc. A namespace operation pays for that only when the backend refuses
    /// the caller's uid and primary gid: it is then sent once more with the
    /// groups, so a directory writable through a supplementary group works.
    #[test]
    fn namespace_operations_resolve_supplementary_groups_only_after_a_denial() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let lookups = AtomicUsize::new(0);
        let run = |gid: u32, groups: &[u32]| {
            let seen = Mutex::new(Vec::new());
            let result = with_groups_after_denial(
                namespace_context(gid),
                |_| {
                    lookups.fetch_add(1, Ordering::SeqCst);
                    groups.to_vec()
                },
                |ctx| group_gated(&seen, ctx),
            );
            (result, seen.into_inner().unwrap())
        };

        let (result, seen) = run(2000, &[3000]);
        assert_eq!(result.unwrap(), 7);
        assert_eq!(
            lookups.load(Ordering::SeqCst),
            0,
            "no /proc read on success"
        );
        assert_eq!(seen, vec![Vec::<u32>::new()]);

        let (result, seen) = run(1000, &[2000, 3000]);
        assert_eq!(result.unwrap(), 7);
        assert_eq!(lookups.load(Ordering::SeqCst), 1);
        assert_eq!(seen, vec![Vec::new(), vec![2000, 3000]]);

        let (result, seen) = run(1000, &[3000]);
        assert_eq!(result.unwrap_err().code(), afs_error::IO_PERMISSION_DENIED);
        assert_eq!(seen.len(), 2, "exactly one retry");

        let (result, seen) = run(1000, &[]);
        assert_eq!(result.unwrap_err().code(), afs_error::IO_PERMISSION_DENIED);
        assert_eq!(seen.len(), 1, "nothing to retry with");
    }

    /// Only a permission refusal is retried: any other failure may have taken
    /// effect and is reported as it is.
    #[test]
    fn other_failures_are_not_retried_with_groups() {
        let attempts = Mutex::new(0);
        let result: afs_error::Result<()> = with_groups_after_denial(
            namespace_context(1000),
            |_| vec![2000],
            |_| {
                *attempts.lock().unwrap() += 1;
                Err(Error::coded(afs_error::IO_ALREADY_EXISTS, "exists"))
            },
        );
        assert_eq!(result.unwrap_err().code(), afs_error::IO_ALREADY_EXISTS);
        assert_eq!(*attempts.lock().unwrap(), 1);
    }

    /// Resending is the DFS mount's rule alone: OwnerFs and the test mount
    /// keep the answer their backend gave to the header identity.
    #[test]
    fn only_the_dfs_mount_resends_denied_operations_with_groups() {
        use crate::node::fuse::policy::{DefaultFusePolicy, DfsFusePolicy, FuseMountPolicy};
        struct KeepsDefaults;
        impl FuseMountPolicy for KeepsDefaults {
            fn label(&self) -> &'static str {
                "ownerfs"
            }
        }
        assert!(DfsFusePolicy::new(std::time::Duration::ZERO).resends_denied_with_groups());
        assert!(!DefaultFusePolicy.resends_denied_with_groups());
        assert!(!KeepsDefaults.resends_denied_with_groups());
    }
}
