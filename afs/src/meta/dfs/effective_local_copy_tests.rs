//! M1-M3: placement follows the effective local-copy rule. No production path
//! lowers it yet; these tests show `build_replica_groups` reads `effective`, and
//! that effective = declared leaves placement epochs at their pinned values.

use super::{build_replica_groups, effective_placement_hash, placement_hash};
use crate::{
    dfs::{
        EffectiveLocalCopy, LocalCopyPolicy, ReplicaGroup, ReplicationConfig,
        StorageDeviceDescriptor,
    },
    meta::store::NodeSession,
};

use LocalCopyPolicy::{NotRequired, Preferred, Required};

fn config(local_copy: LocalCopyPolicy) -> ReplicationConfig {
    ReplicationConfig {
        desired_copies: 2,
        sync_required_copies: 2,
        min_distinct_nodes: 2,
        min_distinct_failure_domains: 1,
        local_copy,
    }
}

fn session(node: &str) -> NodeSession {
    NodeSession {
        node_id: node.into(),
        session_id: format!("{node}-session"),
        grpc_addr: format!("http://{node}:7000"),
        data_addr: format!("http://{node}:7001"),
        rest_addr: format!("http://{node}:7002"),
        nfs_addr: String::new(),
        ownerfs_enabled: false,
        storage_devices: vec![StorageDeviceDescriptor {
            device_id: format!("dev-{node}"),
            device_epoch: 1,
            catalog_revision: 0,
            failure_domain: format!("rack-{node}"),
        }],
        lease_epoch: 1,
        expires_at_unix_ms: u64::MAX,
    }
}

fn sessions(nodes: &[&str]) -> Vec<NodeSession> {
    nodes.iter().map(|node| session(node)).collect()
}

fn groups(
    declared: LocalCopyPolicy,
    effective: LocalCopyPolicy,
    nodes: &[&str],
) -> afs_error::Result<Vec<ReplicaGroup>> {
    build_replica_groups(
        "caller",
        &config(declared),
        crate::dfs::ReplicaFailureConfig::strict(),
        EffectiveLocalCopy::lowered_for_test(declared, effective),
        &sessions(nodes),
    )
}

fn all_headed_by_caller(groups: &[ReplicaGroup]) -> bool {
    groups
        .iter()
        .all(|group| group.targets[0].node_id == "caller")
}

#[test]
fn caller_without_storage_is_refused_only_when_effective_is_required() {
    let storage = ["b", "c"];
    for (declared, effective, refused) in [
        (Required, Required, true),
        (Preferred, Required, true),
        (NotRequired, Required, true),
        (Required, Preferred, false),
        (Required, NotRequired, false),
    ] {
        let result = groups(declared, effective, &storage);
        assert_eq!(result.is_err(), refused, "{declared:?}/{effective:?}");
        if let Ok(groups) = result {
            assert!(groups.iter().all(|group| {
                group
                    .targets
                    .iter()
                    .all(|target| target.node_id != "caller")
            }));
        }
    }
}

#[test]
fn caller_with_storage_heads_the_chain_only_when_effective_prefers_it() {
    let storage = ["b", "c", "caller"];
    for (declared, effective, headed) in [
        (Required, Required, true),
        (NotRequired, Required, true),
        (NotRequired, Preferred, true),
        (Required, Preferred, true),
        (Required, NotRequired, false),
        (Preferred, NotRequired, false),
    ] {
        let groups = groups(declared, effective, &storage).unwrap();
        assert_eq!(
            all_headed_by_caller(&groups),
            headed,
            "{declared:?}/{effective:?}"
        );
    }
}

/// Effective = declared is the only production value: every epoch `placement_hash`
/// pins stays what it was, so S1 does not re-generate any replica group.
#[test]
fn declared_effective_keeps_group_epochs_at_the_pinned_hash() {
    for policy in [Required, Preferred, NotRequired] {
        let config = config(policy);
        let groups = build_replica_groups(
            "caller",
            &config,
            crate::dfs::ReplicaFailureConfig::strict(),
            EffectiveLocalCopy::declared(&config),
            &sessions(&["b", "c", "caller"]),
        )
        .unwrap();
        for group in groups {
            assert_eq!(
                group.placement_epoch,
                placement_hash(&config, &group.targets)
            );
        }
    }
}

/// Lowering the rule changes the epoch, so groups built under the old rule stop
/// matching; and only the effective value is mixed in, not the declared one.
#[test]
fn placement_epoch_mixes_the_effective_value() {
    let targets = groups(Required, Required, &["b", "c", "caller"]).unwrap()[0]
        .targets
        .clone();
    let lowered = effective_placement_hash(
        &config(Required),
        EffectiveLocalCopy::lowered_for_test(Required, Preferred),
        &targets,
    );
    assert_ne!(lowered, placement_hash(&config(Required), &targets));
    assert_eq!(lowered, placement_hash(&config(Preferred), &targets));
}
