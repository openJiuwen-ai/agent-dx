//! Wire contracts only; service implementations belong to Meta and Node.
#![forbid(unsafe_code)]
pub mod meta {
    tonic::include_proto!("afs.meta.v1");
}
pub mod node_control {
    tonic::include_proto!("afs.node.control.v1");
}
pub mod node_data {
    /// Maximum body of a complete ordinary range reply, excluding protobuf
    /// overhead. Outboard/object streaming has separate, smaller wire bounds.
    pub const DFS_COMPACT_RANGE_MAX_BYTES: usize = 128 * 1024;

    tonic::include_proto!("afs.node.data.v1");
}
pub mod local_api {
    tonic::include_proto!("afs.local.v1");
}

/// Structured failure detail carried inside google.rpc.Status.
pub mod error {
    tonic::include_proto!("afs.error.v1");
}
