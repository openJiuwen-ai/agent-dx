//! The Node↔Node control services are split by backend, and that layout is part
//! of the cluster protocol generation.

use afs_protocol::{
    CLUSTER_PROTOCOL_VERSION,
    node_control::{
        dfs_owner_control_server::SERVICE_NAME as DFS_OWNER_CONTROL,
        node_control_server::SERVICE_NAME as NODE_CONTROL,
        owner_control_server::SERVICE_NAME as OWNER_CONTROL,
    },
};

/// Every `rpc` of one service, in declaration order.
fn rpcs_of(proto: &str, service: &str) -> Vec<String> {
    let start = format!("service {service} {{");
    let body = proto
        .split_once(&start)
        .unwrap_or_else(|| panic!("service {service} is declared"))
        .1
        .split_once("\n}")
        .expect("service body ends")
        .0;
    body.lines()
        .filter_map(|line| line.trim().strip_prefix("rpc "))
        .map(|rpc| rpc.split('(').next().expect("rpc name").to_owned())
        .collect()
}

#[test]
fn node_control_rpcs_are_split_by_backend() {
    let proto = include_str!("../proto/node_control.proto");
    assert_eq!(proto.matches("\nservice ").count(), 3);
    assert_eq!(
        rpcs_of(proto, "NodeControl"),
        ["NegotiateData", "CloseData", "Ping"]
    );
    assert_eq!(
        rpcs_of(proto, "OwnerControl"),
        [
            "PrepareOwnerRoot",
            "OwnerGetLock",
            "OwnerSetLock",
            "OwnerAckLockWait",
            "OwnerCancelLockWait",
            "OwnerReleaseLocks",
            "OwnerReleaseLockSession",
            "OwnerNegotiateData",
            "OwnerCloseData",
        ]
    );
    assert_eq!(
        rpcs_of(proto, "DfsOwnerControl"),
        [
            "DfsOwnerOpen",
            "DfsOwnerGetAttr",
            "DfsOwnerMknod",
            "DfsOwnerGetLock",
            "DfsOwnerSetLock",
            "DfsOwnerCancelLockWait",
            "DfsOwnerAcknowledgeLockWait",
            "DfsOwnerReleaseLocks",
            "DfsOwnerReleaseLockSession",
            "DfsOwnerRelease",
        ]
    );
}

#[test]
fn service_paths_keep_the_package() {
    assert_eq!(NODE_CONTROL, "afs.node.control.v1.NodeControl");
    assert_eq!(OWNER_CONTROL, "afs.node.control.v1.OwnerControl");
    assert_eq!(DFS_OWNER_CONTROL, "afs.node.control.v1.DfsOwnerControl");
}

/// Node↔Node channels do not negotiate: a layout change must bump this so old
/// Nodes are refused at registration instead of failing per call at runtime.
/// Generation 5 pages `DfsMeta.ReadDir` (S4a R72); generation 6 carries the
/// replica failure contract in placement snapshots and write grants (S4b R12).
#[test]
fn cluster_protocol_generation_includes_the_split_layout() {
    assert_eq!(CLUSTER_PROTOCOL_VERSION, 7);
}
