//! Opt-in single-file HIXL diagnostics. Compare timestamps only within one process.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static CLOCK: OnceLock<Option<Instant>> = OnceLock::new();
static NEXT_GROUP: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy)]
pub(crate) struct GroupTrace {
    clock: Instant,
    group_id: u64,
    offset: u64,
    requested_len: u64,
}

impl GroupTrace {
    pub(crate) fn reader(offset: u64, requested_len: usize) -> Option<Self> {
        Self::new(offset, requested_len as u64, || {
            NEXT_GROUP.fetch_add(1, Ordering::Relaxed)
        })
    }

    pub(crate) fn home(offset: u64, requested_len: u64) -> Option<Self> {
        Self::new(offset, requested_len, || 0)
    }

    fn new(offset: u64, requested_len: u64, group_id: impl FnOnce() -> u64) -> Option<Self> {
        let clock = CLOCK.get_or_init(|| {
            (std::env::var_os("AFS_HIXL_TRACE_GROUP").as_deref() == Some(std::ffi::OsStr::new("1")))
                .then(Instant::now)
        });
        Some(Self {
            clock: (*clock)?,
            group_id: group_id(),
            offset,
            requested_len,
        })
    }

    /// Use `group_id` for reader-local correlation and `read_id` for the RPC boundary.
    pub(crate) fn emit(
        self,
        event: &'static str,
        read_id: &str,
        prepare_len: u64,
        bytes: u64,
        outcome: &'static str,
    ) {
        let mono_ns = u64::try_from(self.clock.elapsed().as_nanos()).unwrap_or(u64::MAX);
        afs_logging::info!("ownerfs.hixl_group";
            "event" => event,
            "mono_ns" => mono_ns,
            "group_id" => self.group_id,
            "read_id" => read_id,
            "offset" => self.offset,
            "requested_len" => self.requested_len,
            "prepare_len" => prepare_len,
            "bytes" => bytes,
            "outcome" => outcome);
    }
}
