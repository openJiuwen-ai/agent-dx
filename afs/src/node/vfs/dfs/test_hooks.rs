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
}
