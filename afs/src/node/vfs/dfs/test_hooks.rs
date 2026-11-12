//! Test-only pause/inject points, grouped so the product struct carries one
//! `#[cfg(test)]` field instead of one per hook.
use super::*;

#[derive(Default)]
pub(super) struct TestHooks {
    pub(super) native_read_before_delivery: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    pub(super) native_read_after_committed: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    pub(super) backend_inode_after_missing: Mutex<Option<Arc<std::sync::Barrier>>>,
    pub(super) handle_registration_after_route_check: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    pub(super) readonly_version_before_install: Mutex<Option<Arc<std::sync::Barrier>>>,
    pub(super) writeback_after_prepare_pause: Mutex<Option<Duration>>,
    /// Shorter read-view reuse period, so a test read can cross it.
    pub(super) native_read_view_ttl: Mutex<Option<Duration>>,
    /// Runs after a read through the write state resolved and protected its view.
    pub(super) write_state_read_after_view: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl DistributedFs {
    pub(crate) fn test_inode_id(&self, inode: BackendInode) -> Result<InodeId> {
        self.inode_id(inode)
    }
}
