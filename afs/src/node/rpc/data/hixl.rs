//! HIXL source-side helpers shared by the OwnerFs directory-batch Home path.
//!
//! This module keeps the export-slot provider and negotiation helpers the
//! OwnerFs `PrepareReadHixl` / `PrepareBatchHixl` service needs.

use crate::config::MAX_OWNERFS_HIXL_SLOTS_PER_POOL;
use crate::node::rpc::hixl::{TransportParameters, transport_parameters};
use afs_protocol::node_data::{HixlLane, HixlTransport};
use afs_transport::grpc::error_status::{coded_status, error_to_status};
use afs_transport::hixl::{EngineInfo, ExportWindow, HixlError, HixlRuntimeGroup, ReservedExport};
use std::cell::Cell;
use std::sync::Arc;
use std::time::Instant;
use tonic::Status;

/// The Export boundary the OwnerFs Home fills in place. Production uses the
/// shared native runtime; tests use in-memory slots.
pub(crate) trait ExportProvider: Send + Sync {
    fn info(&self) -> EngineInfo;
    fn lane_infos(&self) -> Vec<EngineInfo> {
        vec![self.info()]
    }
    fn export_slots(&self, lane: usize) -> usize;
    /// Pins one export slot for the caller to fill in place. Roll it back with
    /// `release_export` like any prepared export.
    fn reserve_export(
        &self,
        lane: usize,
        length: usize,
        deadline: Instant,
    ) -> Result<ReservedExport, HixlError>;
    /// # Safety
    ///
    /// Follow [`afs_transport::hixl::HixlRuntime::release_export`]: no Rust
    /// borrow or native read may use this slot after release.
    unsafe fn release_export(
        &self,
        lane: usize,
        slot_id: u64,
        generation: u64,
    ) -> Result<(), HixlError>;
    fn quarantine(&self) -> Result<(), HixlError>;
}

pub(super) struct PendingExport<'a> {
    provider: &'a dyn ExportProvider,
    reservation: ReservedExport,
    lane_id: usize,
    rollback_failed: &'a Cell<bool>,
    armed: bool,
}

impl<'a> PendingExport<'a> {
    pub(super) fn new(
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

    pub(super) fn window(&self) -> &ExportWindow {
        self.reservation.window()
    }

    pub(super) fn as_mut_slice(&mut self) -> &mut [u8] {
        self.reservation.as_mut_slice()
    }

    pub(super) fn disarm(mut self) {
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
                .release_export(self.lane_id, window.slot_id, window.generation)
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
pub(super) fn runtime_export_provider(runtime: Arc<HixlRuntimeGroup>) -> Arc<dyn ExportProvider> {
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

    fn reserve_export(
        &self,
        lane: usize,
        length: usize,
        deadline: Instant,
    ) -> Result<ReservedExport, HixlError> {
        self.runtime
            .lane(lane)
            .ok_or_else(invalid_runtime_lane)?
            .reserve_export(length, deadline)
    }

    unsafe fn release_export(
        &self,
        lane: usize,
        slot_id: u64,
        generation: u64,
    ) -> Result<(), HixlError> {
        let runtime = self.runtime.lane(lane).ok_or_else(invalid_runtime_lane)?;
        // SAFETY: the caller of this adapter guarantees local and native
        // users of this generation have finished.
        unsafe { runtime.release_export(slot_id, generation) }
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

fn negotiated_transport(info: &EngineInfo) -> Result<TransportParameters, Status> {
    transport_parameters(info)
        .map_err(|message| coded_status(afs_error::NODE_TRANSFER_INVALID, message))
}

/// The transport parameters and lane list this source offers a reader with
/// `lane_count` lanes. Every local lane must agree on them.
pub(super) fn negotiated_lanes(
    provider: &dyn ExportProvider,
    lane_count: u32,
) -> Result<(TransportParameters, Vec<HixlLane>), Status> {
    let info = provider.info();
    let lane_infos = provider.lane_infos();
    if lane_infos.is_empty()
        || lane_infos.len() > 16
        || usize::try_from(lane_count).ok() != Some(lane_infos.len())
    {
        return Err(coded_status(
            afs_error::NODE_TRANSFER_INVALID,
            "HIXL source and reader lane counts must match",
        ));
    }
    let parameters = negotiated_transport(&info)?;
    let mut lanes = Vec::with_capacity(lane_infos.len());
    for (index, lane) in lane_infos.iter().enumerate() {
        if negotiated_transport(lane)? != parameters
            || lane.window_bytes != info.window_bytes
            || lane.generation == 0
            || lane.engine_id.is_empty()
        {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "HIXL lanes have inconsistent transport or identity",
            ));
        }
        lanes.push(HixlLane {
            lane_id: u32::try_from(index)
                .map_err(|_| Status::internal("HIXL lane index overflow"))?,
            engine_id: lane.engine_id.clone(),
            engine_generation: lane.generation,
            window_bytes: u64::try_from(lane.window_bytes)
                .map_err(|_| Status::internal("HIXL lane window overflow"))?,
            export_slots: export_slot_count(provider.export_slots(index))?,
        });
    }
    Ok((parameters, lanes))
}

pub(super) fn validate_transport_request(
    transport: i32,
    buffer_pool_count: u32,
    buffer_pool_size_mib: u32,
    hccs_domain_id: Option<u64>,
    expected: TransportParameters,
) -> Result<(), Status> {
    let requested = HixlTransport::try_from(transport).map_err(|_| {
        coded_status(
            afs_error::NODE_TRANSFER_INVALID,
            "HIXL transport is unknown",
        )
    })?;
    if (
        requested,
        buffer_pool_count,
        buffer_pool_size_mib,
        hccs_domain_id,
    ) != expected
    {
        return Err(coded_status(
            afs_error::NODE_TRANSFER_INVALID,
            "HIXL transport or buffer-pool parameters do not match",
        ));
    }
    Ok(())
}

fn export_slot_count(slots: usize) -> Result<u32, Status> {
    if !(1..=MAX_OWNERFS_HIXL_SLOTS_PER_POOL).contains(&slots) {
        return Err(coded_status(
            afs_error::NODE_TRANSFER_UNAVAILABLE,
            "HIXL export-slot pool is unavailable",
        ));
    }
    u32::try_from(slots).map_err(|_| {
        coded_status(
            afs_error::NODE_TRANSFER_INTERNAL,
            "HIXL export-slot capacity exceeds protocol range",
        )
    })
}

pub(super) fn validate_client_engine(
    protocol_version: u32,
    engine_id: &str,
    generation: u64,
) -> Result<(), Status> {
    if protocol_version != crate::node::rpc::hixl::HIXL_PROTOCOL_VERSION {
        return Err(coded_status(
            afs_error::NODE_TRANSFER_UNSUPPORTED,
            "HIXL protocol version is unsupported",
        ));
    }
    if engine_id.is_empty() || engine_id.len() > 512 || generation == 0 {
        return Err(coded_status(
            afs_error::NODE_TRANSFER_INVALID,
            "HIXL client engine identity is invalid",
        ));
    }
    Ok(())
}

pub(super) fn runtime_status(error: HixlError) -> Status {
    error_to_status(crate::node::rpc::hixl::runtime_error(error))
}

/// Allocates the next control-session id, refusing to wrap.
pub(super) fn next_session_id(counter: &mut u64, message: &'static str) -> Result<u64, Status> {
    let id = (*counter).max(1);
    *counter = id
        .checked_add(1)
        .ok_or_else(|| coded_status(afs_error::NODE_TRANSFER_UNAVAILABLE, message))?;
    Ok(id)
}

pub(crate) fn unsupported_status(message: &'static str) -> Status {
    coded_status(afs_error::NODE_TRANSFER_UNSUPPORTED, message)
}
