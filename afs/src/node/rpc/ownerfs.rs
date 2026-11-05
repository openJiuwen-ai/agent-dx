//! OwnerFs reservation conversion and node-to-node workspace preparation.

use afs_error::{Error, Result};
use afs_protocol::{
    meta::RootReservation as WireReservation,
    node_control::{PrepareOwnerRootRequest, owner_control_client::OwnerControlClient},
};
use tonic::transport::Channel;

use crate::node::vfs::ownerfs::root::{RootCreateParams, RootId, RootReservation};

pub(super) fn reservation_wire(value: &RootReservation) -> WireReservation {
    WireReservation {
        root_id: value.id.0.clone(),
        root_epoch: value.epoch,
        home_node_id: value.home_node_id.clone(),
        session_id: value.session_id.clone(),
        create_intent_id: value.create_intent_id.clone(),
        prepare_token: value.prepare_token.clone(),
        requester_node_id: value.requester_node_id.clone(),
        requester_session_id: value.requester_session_id.clone(),
    }
}

pub(super) fn reservation_from_wire(value: WireReservation) -> Result<RootReservation> {
    if value.root_id.is_empty()
        || value.root_epoch == 0
        || value.home_node_id.is_empty()
        || value.session_id.is_empty()
        || value.create_intent_id.is_empty()
        || value.prepare_token.is_empty()
        || value.requester_node_id.is_empty()
        || value.requester_session_id.is_empty()
    {
        return Err(Error::coded(
            afs_error::CLIENT_PROTOCOL_VIOLATION,
            "workspace reservation identity is incomplete",
        ));
    }
    Ok(RootReservation {
        id: RootId(value.root_id),
        epoch: value.root_epoch,
        home_node_id: value.home_node_id,
        session_id: value.session_id,
        create_intent_id: value.create_intent_id,
        prepare_token: value.prepare_token,
        requester_node_id: value.requester_node_id,
        requester_session_id: value.requester_session_id,
    })
}

/// Execute one preparation call. Transport errors leave the outcome uncertain.
pub async fn prepare_root(
    channel: Channel,
    reservation: &RootReservation,
    params: &RootCreateParams,
) -> Result<()> {
    OwnerControlClient::new(channel)
        .prepare_owner_root(PrepareOwnerRootRequest {
            reservation: Some(reservation_wire(reservation)),
            name: params.name.clone(),
            uid: params.uid,
            gid: params.gid,
            mode: params.mode,
        })
        .await
        .map_err(afs_transport::grpc::error_status::status_to_error)?;
    Ok(())
}
