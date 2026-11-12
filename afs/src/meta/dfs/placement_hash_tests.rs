//! Pins `placement_hash` output: a change here means every replica group gets a
//! new placement epoch and group id, so S1c may not move these values.

use super::placement_hash;
use crate::dfs::{LocalCopyPolicy, ReplicaTarget, ReplicationConfig, StorageDeviceDescriptor};

fn config(
    desired: u16,
    sync: u16,
    nodes: u16,
    domains: u16,
    local_copy: LocalCopyPolicy,
) -> ReplicationConfig {
    ReplicationConfig {
        desired_copies: desired,
        sync_required_copies: sync,
        min_distinct_nodes: nodes,
        min_distinct_failure_domains: domains,
        local_copy,
    }
}

fn target(
    node: &str,
    node_epoch: u64,
    device_epoch: u64,
    catalog_revision: u64,
    rack: &str,
) -> ReplicaTarget {
    ReplicaTarget {
        node_id: format!("node-{node}"),
        node_epoch,
        data_endpoint: format!("http://node-{node}:7001"),
        device: StorageDeviceDescriptor {
            device_id: format!("dev-{node}"),
            device_epoch,
            catalog_revision,
            failure_domain: rack.into(),
        },
    }
}

fn a() -> ReplicaTarget {
    target("a", 1, 1, 0, "rack-1")
}

fn b() -> ReplicaTarget {
    target("b", 2, 3, 0, "rack-2")
}

fn c() -> ReplicaTarget {
    target("c", 5, 8, 0, "rack-3")
}

#[test]
fn characterize_placement_hash_output_is_pinned_for_fixed_inputs() {
    let required = LocalCopyPolicy::Required;
    assert_eq!(
        placement_hash(&config(1, 1, 1, 1, required), &[a()]),
        0x394e_b187_af95_b942
    );
    assert_eq!(
        placement_hash(&config(2, 2, 2, 1, required), &[a(), b()]),
        0x946d_0d4b_3a3d_75e4
    );
    assert_eq!(
        placement_hash(
            &config(3, 2, 2, 2, LocalCopyPolicy::Preferred),
            &[a(), b(), c()]
        ),
        0xefad_1519_d6f1_c42a
    );
    assert_eq!(
        placement_hash(
            &config(2, 2, 2, 1, LocalCopyPolicy::NotRequired),
            &[a(), b()]
        ),
        0x5800_b79a_1e78_e4d2
    );
    // Chain order is part of the placement identity.
    assert_eq!(
        placement_hash(&config(2, 2, 2, 1, required), &[b(), a()]),
        0xccb0_6985_d933_dac2
    );
}

#[test]
fn characterize_placement_hash_ignores_catalog_revision() {
    let config = config(2, 2, 2, 1, LocalCopyPolicy::Required);
    let advanced = target("a", 1, 1, 42, "rack-1");
    assert_eq!(
        placement_hash(&config, &[advanced, b()]),
        placement_hash(&config, &[a(), b()])
    );
}
