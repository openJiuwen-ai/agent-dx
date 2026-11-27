//! HIXL source-side helpers shared by the OwnerFs directory-batch Home path.
//!
//! This module keeps the wire negotiation helpers the
//! OwnerFs `PrepareReadHixl` / `PrepareBatchHixl` service needs.

use crate::config::MAX_OWNERFS_HIXL_SLOTS_PER_POOL;
use crate::node::data_plane::hixl::{ExportProvider, runtime_error};
use crate::node::rpc::hixl::{HIXL_PROTOCOL_VERSION, TransportParameters, transport_parameters};
use afs_protocol::node_data::HixlLane;
use afs_transport::grpc::error_status::{coded_status, error_to_status};
use afs_transport::hixl::{EngineInfo, HixlError};
use tonic::Status;

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
    if protocol_version != HIXL_PROTOCOL_VERSION {
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
    error_to_status(runtime_error(error))
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
