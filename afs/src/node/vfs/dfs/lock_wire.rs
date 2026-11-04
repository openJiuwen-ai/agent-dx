use super::*;

impl DistributedFs {
    pub(super) fn wire_lock_owner(
        owner: &FileLockOwner,
    ) -> afs_protocol::node_control::DfsOwnerLockOwner {
        afs_protocol::node_control::DfsOwnerLockOwner {
            ingress_session_id: Self::raw_lock_session(&owner.ingress_session_id).to_owned(),
            kernel_owner: owner.kernel_owner,
        }
    }

    fn file_lock_owner_from_wire(
        owner: afs_protocol::node_control::DfsOwnerLockOwner,
    ) -> Result<FileLockOwner> {
        if owner.ingress_session_id.is_empty() {
            return Err(invalid("DFS lock owner session is empty"));
        }
        Ok(FileLockOwner {
            ingress_session_id: owner.ingress_session_id,
            kernel_owner: owner.kernel_owner,
        })
    }

    fn wire_lock_range(range: FileLockRange) -> afs_protocol::node_control::DfsOwnerLockRange {
        afs_protocol::node_control::DfsOwnerLockRange {
            start: range.start,
            end: range.end,
        }
    }

    pub(super) fn file_lock_range_from_wire(
        range: afs_protocol::node_control::DfsOwnerLockRange,
    ) -> Result<FileLockRange> {
        let range = FileLockRange {
            start: range.start,
            end: range.end,
        };
        if !range.is_valid() {
            return Err(invalid("DFS lock range is invalid"));
        }
        Ok(range)
    }

    fn wire_lock_kind(kind: FileLockKind) -> afs_protocol::node_control::DfsOwnerLockKind {
        match kind {
            FileLockKind::Posix => afs_protocol::node_control::DfsOwnerLockKind::Posix,
            FileLockKind::Flock => afs_protocol::node_control::DfsOwnerLockKind::Flock,
        }
    }

    pub(super) fn file_lock_kind_from_wire(kind: i32) -> Result<FileLockKind> {
        match afs_protocol::node_control::DfsOwnerLockKind::try_from(kind) {
            Ok(afs_protocol::node_control::DfsOwnerLockKind::Posix) => Ok(FileLockKind::Posix),
            Ok(afs_protocol::node_control::DfsOwnerLockKind::Flock) => Ok(FileLockKind::Flock),
            _ => Err(invalid("DFS lock kind is invalid")),
        }
    }

    fn wire_lock_type(lock_type: FileLockType) -> afs_protocol::node_control::DfsOwnerLockType {
        match lock_type {
            FileLockType::Read => afs_protocol::node_control::DfsOwnerLockType::Read,
            FileLockType::Write => afs_protocol::node_control::DfsOwnerLockType::Write,
            FileLockType::Unlock => afs_protocol::node_control::DfsOwnerLockType::Unlock,
        }
    }

    pub(super) fn file_lock_type_from_wire(lock_type: i32) -> Result<FileLockType> {
        match afs_protocol::node_control::DfsOwnerLockType::try_from(lock_type) {
            Ok(afs_protocol::node_control::DfsOwnerLockType::Read) => Ok(FileLockType::Read),
            Ok(afs_protocol::node_control::DfsOwnerLockType::Write) => Ok(FileLockType::Write),
            Ok(afs_protocol::node_control::DfsOwnerLockType::Unlock) => Ok(FileLockType::Unlock),
            _ => Err(invalid("DFS lock type is invalid")),
        }
    }

    pub(super) fn wire_lock_request(
        request: &LockRequest,
    ) -> afs_protocol::node_control::DfsOwnerLockRequest {
        afs_protocol::node_control::DfsOwnerLockRequest {
            kind: Self::wire_lock_kind(request.kind) as i32,
            owner: Some(Self::wire_lock_owner(&request.owner)),
            pid: request.pid,
            range: Some(Self::wire_lock_range(request.range)),
            lock_type: Self::wire_lock_type(request.lock_type) as i32,
        }
    }

    pub(super) fn wire_lock_conflict(
        conflict: FileLockConflict,
    ) -> afs_protocol::node_control::DfsOwnerLockConflict {
        afs_protocol::node_control::DfsOwnerLockConflict {
            kind: Self::wire_lock_kind(conflict.kind) as i32,
            owner: Some(Self::wire_lock_owner(&conflict.owner)),
            pid: conflict.pid,
            range: Some(Self::wire_lock_range(conflict.range)),
            lock_type: Self::wire_lock_type(conflict.lock_type) as i32,
        }
    }

    pub(super) fn file_lock_conflict_from_wire(
        conflict: afs_protocol::node_control::DfsOwnerLockConflict,
    ) -> Result<FileLockConflict> {
        Ok(FileLockConflict {
            kind: Self::file_lock_kind_from_wire(conflict.kind)?,
            owner: Self::file_lock_owner_from_wire(
                conflict
                    .owner
                    .ok_or_else(|| invalid("DFS lock conflict owner is missing"))?,
            )?,
            pid: conflict.pid,
            range: Self::file_lock_range_from_wire(
                conflict
                    .range
                    .ok_or_else(|| invalid("DFS lock conflict range is missing"))?,
            )?,
            lock_type: Self::file_lock_type_from_wire(conflict.lock_type)?,
        })
    }

    pub(super) fn raw_lock_conflict(mut conflict: FileLockConflict) -> FileLockConflict {
        conflict.owner.ingress_session_id =
            Self::raw_lock_session(&conflict.owner.ingress_session_id).to_owned();
        conflict
    }

    pub(super) fn wire_lock_waiter(
        waiter: &LockWaiterId,
    ) -> afs_protocol::node_control::DfsOwnerLockWaiter {
        afs_protocol::node_control::DfsOwnerLockWaiter {
            ingress_session_id: Self::raw_lock_session(&waiter.ingress_session_id).to_owned(),
            request_id: waiter.request_id,
        }
    }

    pub(super) fn lock_release_kind_from_wire(release_kind: i32) -> Result<ReleaseKind> {
        match afs_protocol::node_control::DfsOwnerLockReleaseKind::try_from(release_kind) {
            Ok(afs_protocol::node_control::DfsOwnerLockReleaseKind::PosixOwner) => {
                Ok(ReleaseKind::PosixOwner)
            }
            Ok(afs_protocol::node_control::DfsOwnerLockReleaseKind::FlockOwner) => {
                Ok(ReleaseKind::FlockOwner)
            }
            _ => Err(invalid("DFS lock release kind is invalid")),
        }
    }

    pub(super) fn wire_lock_release_kind(
        release_kind: ReleaseKind,
    ) -> afs_protocol::node_control::DfsOwnerLockReleaseKind {
        match release_kind {
            ReleaseKind::PosixOwner => {
                afs_protocol::node_control::DfsOwnerLockReleaseKind::PosixOwner
            }
            ReleaseKind::FlockOwner => {
                afs_protocol::node_control::DfsOwnerLockReleaseKind::FlockOwner
            }
        }
    }
}
