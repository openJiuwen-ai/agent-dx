//! Runtime-local counters observed through the HTTP control status endpoint.
//! Adxlet decides idle policy; counters have no transport or lifecycle side effects.
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
static CHANGED: tokio::sync::Notify = tokio::sync::Notify::const_new();
pub(super) async fn changed() {
    CHANGED.notified().await;
}
static REVISION: AtomicU64 = AtomicU64::new(1);
pub fn revision() -> u64 {
    REVISION.load(Ordering::SeqCst)
}
static ACTIVE: AtomicI64 = AtomicI64::new(0);
static COMMANDS: AtomicI64 = AtomicI64::new(0);
static ADMISSION: std::sync::Mutex<bool> = std::sync::Mutex::new(true);
#[derive(Clone, Copy)]
pub(crate) enum ActivitySource {
    DirectHttp,
    Checkpoint,
    Tunnel,
}
#[must_use]
pub(crate) struct ActiveGuard;
#[must_use]
pub struct CommandActivityGuard;
pub(crate) fn enter(_: ActivitySource) -> ActiveGuard {
    ACTIVE.fetch_add(1, Ordering::SeqCst);
    REVISION.fetch_add(1, Ordering::SeqCst);
    CHANGED.notify_one();
    ActiveGuard
}
pub(crate) fn try_enter(source: ActivitySource) -> adx_core::Result<ActiveGuard> {
    let admission = ADMISSION.lock().unwrap_or_else(|p| p.into_inner());
    if !*admission {
        return Err(adx_core::Error::Conflict);
    }
    Ok(enter(source))
}
pub(crate) fn quiesce(expected_revision: u64) -> adx_core::Result<()> {
    let mut admission = ADMISSION.lock().unwrap_or_else(|p| p.into_inner());
    if active_count() != 0 || revision() != expected_revision {
        return Err(adx_core::Error::Conflict);
    }
    *admission = false;
    Ok(())
}
pub(crate) fn reopen() {
    *ADMISSION.lock().unwrap_or_else(|p| p.into_inner()) = true;
}
impl Drop for ActiveGuard {
    fn drop(&mut self) {
        ACTIVE.fetch_sub(1, Ordering::SeqCst);
        REVISION.fetch_add(1, Ordering::SeqCst);
        CHANGED.notify_one();
    }
}
pub fn enter_command() -> CommandActivityGuard {
    COMMANDS.fetch_add(1, Ordering::SeqCst);
    REVISION.fetch_add(1, Ordering::SeqCst);
    CHANGED.notify_one();
    CommandActivityGuard
}
impl Drop for CommandActivityGuard {
    fn drop(&mut self) {
        COMMANDS.fetch_sub(1, Ordering::SeqCst);
        REVISION.fetch_add(1, Ordering::SeqCst);
        CHANGED.notify_one();
    }
}
pub fn active_count() -> i64 {
    ACTIVE.load(Ordering::SeqCst)
}
pub fn active_command_count() -> i64 {
    COMMANDS.load(Ordering::SeqCst)
}
