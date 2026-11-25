//! Backend-owned export reservations and rollback.

use afs_transport::hixl::{EngineInfo, ExportWindow, HixlError, HixlRuntimeGroup, ReservedExport};
use std::{cell::Cell, sync::Arc, time::Instant};

/// The export boundary a source fills in place. Production uses the
/// shared native runtime; tests use in-memory slots.
pub(crate) trait ExportProvider: Send + Sync {
    fn info(&self) -> EngineInfo;
    fn lane_infos(&self) -> Vec<EngineInfo> {
        vec![self.info()]
    }
    fn export_slots(&self, lane: usize) -> usize;
    /// Reserves one group in descriptor order. On failure no slot is published;
    /// failure to roll back a partial reservation fences the engine.
    fn reserve_exports(
        &self,
        lane: usize,
        lengths: &[usize],
        deadline: Instant,
    ) -> Result<Vec<ReservedExport>, HixlError>;
    /// # Safety
    /// Follow [`afs_transport::hixl::HixlRuntime::release_exports`]: no Rust
    /// borrow or native read may use these slots after release.
    unsafe fn release_exports(&self, lane: usize, slots: Vec<(u64, u64)>) -> Result<(), HixlError>;
    fn quarantine(&self) -> Result<(), HixlError>;
}

pub(crate) struct PendingExport<'a> {
    provider: &'a dyn ExportProvider,
    reservation: ReservedExport,
    lane_id: usize,
    rollback_failed: &'a Cell<bool>,
    armed: bool,
}

impl<'a> PendingExport<'a> {
    pub(crate) fn new(
        provider: &'a dyn ExportProvider,
        lane_id: usize,
        reservation: ReservedExport,
        rollback_failed: &'a Cell<bool>,
    ) -> Self {
        Self {
            provider,
            reservation,
            lane_id,
            rollback_failed,
            armed: true,
        }
    }

    pub(crate) fn window(&self) -> &ExportWindow {
        self.reservation.window()
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        self.reservation.as_mut_slice()
    }

    pub(crate) fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for PendingExport<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let window = self.reservation.window();
        // SAFETY: this guard owns the only writable reservation and is
        // dropping, so its borrows are over. An armed guard's descriptor
        // never reached the reader; publication consumes the guard first.
        let result = unsafe {
            self.provider
                .release_exports(self.lane_id, vec![(window.slot_id, window.generation)])
        };
        if result.is_err() {
            self.rollback_failed.set(true);
        }
    }
}

struct RuntimeExportProvider {
    runtime: Arc<HixlRuntimeGroup>,
}

/// The production export boundary.
pub(crate) fn runtime_export_provider(runtime: Arc<HixlRuntimeGroup>) -> Arc<dyn ExportProvider> {
    Arc::new(RuntimeExportProvider { runtime })
}

impl ExportProvider for RuntimeExportProvider {
    fn info(&self) -> EngineInfo {
        self.runtime.info()
    }

    fn lane_infos(&self) -> Vec<EngineInfo> {
        self.runtime.infos()
    }

    fn export_slots(&self, lane: usize) -> usize {
        self.runtime
            .lane(lane)
            .map_or(0, |runtime| runtime.status().export_slots)
    }

    fn reserve_exports(
        &self,
        lane: usize,
        lengths: &[usize],
        deadline: Instant,
    ) -> Result<Vec<ReservedExport>, HixlError> {
        self.runtime
            .lane(lane)
            .ok_or_else(invalid_runtime_lane)?
            .reserve_exports(lengths.to_vec(), deadline)
    }

    unsafe fn release_exports(&self, lane: usize, slots: Vec<(u64, u64)>) -> Result<(), HixlError> {
        let runtime = self.runtime.lane(lane).ok_or_else(invalid_runtime_lane)?;
        // SAFETY: the caller established that every local and native user of these generations has finished.
        unsafe { runtime.release_exports(slots) }
    }

    fn quarantine(&self) -> Result<(), HixlError> {
        self.runtime.quarantine()
    }
}

fn invalid_runtime_lane() -> HixlError {
    HixlError {
        phase: afs_transport::hixl::HixlPhase::PrepareExport,
        kind: afs_transport::hixl::HixlErrorKind::InvalidArgument,
        native_status: None,
        message: "HIXL lane is not configured".into(),
        read_may_be_in_flight: false,
    }
}
