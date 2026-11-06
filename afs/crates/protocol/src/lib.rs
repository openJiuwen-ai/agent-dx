//! Wire contracts only; service implementations belong to Meta and Node.
#![forbid(unsafe_code)]

/// Cluster wire protocol generation: Node↔Meta registration and the Node↔Node
/// service layout. Carried by `RegisterNodeRequest.protocol_version` and checked
/// only by Meta's `RegisterNode`; Node↔Node channels do not negotiate it, so a
/// layout change bumps it and old Nodes are refused at registration.
pub const CLUSTER_PROTOCOL_VERSION: u32 = 4;

pub mod meta {
    tonic::include_proto!("afs.meta.v1");
}
pub mod node_control {
    tonic::include_proto!("afs.node.control.v1");
}
pub mod node_data {
    tonic::include_proto!("afs.node.data.v1");
}
pub mod local_api {
    tonic::include_proto!("afs.local.v1");
}

/// Structured failure detail carried inside google.rpc.Status.
pub mod error {
    tonic::include_proto!("afs.error.v1");
}
