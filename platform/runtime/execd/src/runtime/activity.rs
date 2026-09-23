//! Runtime-local counters observed through the HTTP control status endpoint.
//! Adxlet decides idle policy; counters have no transport or lifecycle side effects.
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
static REVISION: AtomicU64 = AtomicU64::new(1);
pub fn revision() -> u64 {
    REVISION.load(Ordering::SeqCst)
}
static ACTIVE: AtomicI64 = AtomicI64::new(0);
static COMMANDS: AtomicI64 = AtomicI64::new(0);
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
    ActiveGuard
}
impl Drop for ActiveGuard {
    fn drop(&mut self) {
        ACTIVE.fetch_sub(1, Ordering::SeqCst);
        REVISION.fetch_add(1, Ordering::SeqCst);
    }
}
pub fn enter_command() -> CommandActivityGuard {
    COMMANDS.fetch_add(1, Ordering::SeqCst);
    REVISION.fetch_add(1, Ordering::SeqCst);
    CommandActivityGuard
}
impl Drop for CommandActivityGuard {
    fn drop(&mut self) {
        COMMANDS.fetch_sub(1, Ordering::SeqCst);
        REVISION.fetch_add(1, Ordering::SeqCst);
    }
}
pub fn active_count() -> i64 {
    ACTIVE.load(Ordering::SeqCst)
}
pub fn active_command_count() -> i64 {
    COMMANDS.load(Ordering::SeqCst)
}
