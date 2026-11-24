//! HIXL reader plumbing shared by the OwnerFs read-ahead windows.
//!
//! This module keeps the lane, deadline and engine-binding helpers the OwnerFs
//! reader needs, without any of its window state.

use super::protocol_error;
use crate::config::MAX_OWNERFS_HIXL_SLOTS_PER_POOL;
use crate::node::data_plane::hixl::ReadRuntime;
use crate::node::rpc::hixl::{TransportParameters, runtime_error, transport_parameters};
use afs_protocol::node_data::HixlLane;
use afs_transport::hixl::EngineInfo;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

pub(super) fn remaining(deadline: Instant) -> afs_error::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|value| !value.is_zero())
        .ok_or_else(|| {
            afs_error::Error::coded(
                afs_error::CLIENT_DEADLINE_EXCEEDED,
                "HIXL read deadline expired",
            )
        })
}

/// Binds a peer to the least-used lane. The bounded table is best effort: peer
/// identities churn on restart, so it starts over once `max_keys` is reached.
pub(super) fn least_used_lane(
    bindings: &StdMutex<HashMap<String, usize>>,
    key: &str,
    lanes: usize,
    max_keys: usize,
) -> usize {
    let lanes = lanes.max(1);
    if lanes == 1 {
        return 0;
    }
    let mut bindings = bindings
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(lane) = bindings.get(key) {
        return *lane % lanes;
    }
    if bindings.len() >= max_keys {
        bindings.clear();
    }
    let mut counts = vec![0_usize; lanes];
    for lane in bindings.values() {
        counts[*lane % lanes] += 1;
    }
    let lane = counts
        .iter()
        .enumerate()
        .min_by_key(|(_, count)| **count)
        .map_or(0, |(index, _)| index);
    bindings.insert(key.to_owned(), lane);
    lane
}

/// One negotiated lane's identity, capacity and endpoint, unique in its reply.
pub(super) fn lane_is_valid(
    lane: &HixlLane,
    index: usize,
    endpoints: &mut HashSet<SocketAddr>,
) -> bool {
    let Ok(endpoint) = lane.engine_id.parse::<SocketAddr>() else {
        return false;
    };
    usize::try_from(lane.lane_id).ok() == Some(index)
        && lane.engine_generation != 0
        && lane.window_bytes != 0
        && lane.window_bytes <= 4 * 1024 * 1024
        && usize::try_from(lane.export_slots)
            .is_ok_and(|slots| (1..=MAX_OWNERFS_HIXL_SLOTS_PER_POOL).contains(&slots))
        && endpoint.port() != 0
        && !endpoint.ip().is_unspecified()
        && !endpoint.ip().is_multicast()
        && !matches!(endpoint.ip(), std::net::IpAddr::V4(address) if address.is_broadcast())
        && endpoints.insert(endpoint)
}

/// Connects every local lane to its negotiated remote lane.
pub(super) async fn connect_lanes(
    native: Arc<dyn ReadRuntime>,
    lanes: Vec<HixlLane>,
    deadline: Instant,
) -> afs_error::Result<()> {
    tokio::task::spawn_blocking(move || {
        for (index, remote) in lanes.iter().enumerate() {
            native
                .lane(index)
                .ok_or_else(|| protocol_error("HIXL local lane disappeared"))?
                .connect(&remote.engine_id, deadline)
                .map_err(runtime_error)?;
        }
        Ok(())
    })
    .await
    .map_err(|error| afs_error::Error::coded(afs_error::CLIENT_WORKER_FAILED, error.to_string()))?
}

pub(super) fn engine_transport_parameters(
    info: &EngineInfo,
) -> afs_error::Result<TransportParameters> {
    transport_parameters(info).map_err(protocol_error)
}

/// Remembers which peer generation owns each engine endpoint. Reusing an
/// endpoint across generations needs a restart, because the native connection
/// stays. Either every engine is bound or none is.
pub(super) fn bind_engines<'a>(
    engines: &mut HashMap<SocketAddr, (String, u64)>,
    lanes: impl IntoIterator<Item = (&'a str, (String, u64))>,
    max_engines: usize,
) -> afs_error::Result<()> {
    let mut fresh = Vec::new();
    for (engine_id, identity) in lanes {
        let endpoint = engine_id.parse::<SocketAddr>().map_err(|_| {
            afs_error::Error::coded(
                afs_error::CLIENT_PROTOCOL_VIOLATION,
                "HIXL engine address is invalid",
            )
        })?;
        match engines.get(&endpoint) {
            Some(previous) if previous != &identity => {
                return Err(afs_error::Error::coded(
                    afs_error::NODE_TRANSFER_DRAIN_INCOMPLETE,
                    "HIXL engine address was reused by a different peer generation",
                ));
            }
            Some(_) => {}
            None => fresh.push((endpoint, identity)),
        }
    }
    if engines.len().saturating_add(fresh.len()) > max_engines {
        return Err(afs_error::Error::coded(
            afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
            "HIXL native connection budget exhausted",
        ));
    }
    engines.extend(fresh);
    Ok(())
}
