//! Golden namespace request digests.
//!
//! `namespace_request_digest` is `blake3(serde_json(request))`; the digest is
//! persisted in WAL records and checkpoints to bind idempotent retries. Field
//! names, order and serde attributes of every digested request are therefore
//! a storage format. Changing any of them requires a format version bump and
//! regenerated goldens:
//! `cargo test -p afs --lib request_digest_golden_tests::print_request_digest_goldens -- --ignored --nocapture`.

use super::*;
use crate::dfs::{
    ChunkReceipt, CloneFileRange, CommitMetadataDelta, ContentDigest, DigestAlgorithm, Extent,
    GcFence, ReplicaTarget, StorageDeviceDescriptor,
};

fn caller() -> CallerContext {
    CallerContext {
        uid: 1000,
        gid: 1001,
        supplementary_gids: vec![10, 20],
    }
}

fn attributes() -> InodeAttributes {
    InodeAttributes {
        mode: 0o100644,
        uid: 1000,
        gid: 1001,
        nlink: 1,
        atime_unix_ms: 11,
        mtime_unix_ms: 12,
        ctime_unix_ms: 13,
    }
}

fn digest() -> ContentDigest {
    ContentDigest {
        algorithm: DigestAlgorithm::Blake3,
        bytes: [0xab; 32],
    }
}

fn chunk() -> ChunkObject {
    ChunkObject {
        id: crate::dfs::ChunkId::new(format!("blake3-{}-42", "ab".repeat(32))),
        length: 42,
        content_digest: digest(),
        encoding: crate::dfs::ChunkEncoding::Raw,
    }
}

fn ack() -> ReplicaAck {
    ReplicaAck {
        operation_id: OperationId::new("op"),
        chunk_id: chunk().id,
        placement_revision: 7,
        placement_epoch: 3,
        node_id: "node-a".into(),
        node_epoch: 2,
        device_id: "dev-a".into(),
        device_epoch: 4,
        catalog_revision: 5,
        persisted_bytes: 42,
        verified_digest: digest(),
    }
}

fn replication_claim() -> ReplicationClaim {
    ReplicationClaim {
        task_id: ReplicationTaskId::new("repair:c"),
        operation_id: OperationId::new("claim"),
        worker_node_id: "node-a".into(),
        worker_node_epoch: 2,
        worker_session_id: "session-a".into(),
        expires_at_unix_ms: 99,
        fence: 1,
        chunk: chunk(),
        source_copy_id: CopyId::new("copy"),
        placement_revision: 7,
        replica_group: ReplicaGroup {
            id: ReplicaGroupId::new("group"),
            placement_epoch: 3,
            targets: vec![ReplicaTarget {
                node_id: "node-a".into(),
                node_epoch: 2,
                data_endpoint: "http://node-a:2".into(),
                device: StorageDeviceDescriptor {
                    device_id: "dev-a".into(),
                    device_epoch: 4,
                    catalog_revision: 5,
                    failure_domain: "fd-a".into(),
                },
            }],
        },
        replication: ReplicationConfig::local_single_copy(),
    }
}

fn commit(gc_fence: Option<GcFence>, clone_source: Option<CloneFileRange>) -> CommitFileVersion {
    CommitFileVersion {
        gc_fence,
        operation_id: OperationId::new("op"),
        inode_id: InodeId::new("inode"),
        write_lease: WriteLease {
            inode_id: InodeId::new("inode"),
            owner_node_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            lease_epoch: 2,
            expires_at_unix_ms: 99,
        },
        expected_inode_revision: 6,
        expected_head_version: Some(FileVersionId::new("v0")),
        file_version: FileVersion {
            id: FileVersionId::new("v1"),
            inode_id: InodeId::new("inode"),
            parent_version: Some(FileVersionId::new("v0")),
            length: 42,
            layout_root: LayoutRootId::new("layout"),
            created_at_unix_ms: 8,
        },
        layout_root: LayoutRoot {
            id: LayoutRootId::new("layout"),
            file_length: 42,
            inline_extents: vec![Extent {
                file_offset: 0,
                length: 42,
                chunk_id: chunk().id,
                chunk_offset: 0,
            }],
        },
        chunk_receipts: vec![ChunkReceipt {
            operation_id: OperationId::new("op"),
            chunk: chunk(),
            placement_revision: 7,
            placement_epoch: 3,
            replica_group_id: ReplicaGroupId::new("group"),
            durable_acks: vec![ack()],
            external_commit: None,
        }],
        clone_source,
        metadata_delta: CommitMetadataDelta {
            mode: CommitMetadataMode::Full,
            mtime_unix_ms: Some(12),
            ctime_unix_ms: Some(13),
            kill_suidgid: true,
        },
    }
}

fn case<T: serde::Serialize>(name: &'static str, request: &T) -> (&'static str, String, String) {
    let json = String::from_utf8(serde_json::to_vec(request).unwrap()).unwrap();
    let digest = blake3::Hash::from(namespace_request_digest(request).unwrap())
        .to_hex()
        .to_string();
    (name, json, digest)
}

fn cases() -> Vec<(&'static str, String, String)> {
    vec![
        case(
            "create",
            &CreateFileRequest {
                caller_id: "node-a".into(),
                owner_session_id: "session-a".into(),
                operation_id: OperationId::new("op"),
                namespace_id: NamespaceId::new("ns"),
                parent_inode_id: InodeId::new("1"),
                name: b"file".to_vec(),
                attributes: attributes(),
                lease_seconds: 30,
            },
        ),
        case(
            "mkdir",
            &MkdirRequest {
                caller_id: "node-a".into(),
                operation_id: OperationId::new("op"),
                namespace_id: NamespaceId::new("ns"),
                parent_inode_id: InodeId::new("1"),
                name: b"dir".to_vec(),
                attributes: attributes(),
                caller: caller(),
            },
        ),
        case(
            "mknod",
            &MknodRequest {
                caller_id: "node-a".into(),
                operation_id: OperationId::new("op"),
                namespace_id: NamespaceId::new("ns"),
                parent_inode_id: InodeId::new("1"),
                name: b"dev".to_vec(),
                kind: SpecialNodeKind::CharDevice { rdev: 259 },
                attributes: attributes(),
                caller: caller(),
            },
        ),
        case(
            "unlink",
            &UnlinkRequest {
                caller_id: "node-a".into(),
                operation_id: OperationId::new("op"),
                namespace_id: NamespaceId::new("ns"),
                parent_inode_id: InodeId::new("1"),
                name: b"file".to_vec(),
                caller: caller(),
            },
        ),
        case(
            "rename",
            &RenameRequest {
                caller_id: "node-a".into(),
                operation_id: OperationId::new("op"),
                namespace_id: NamespaceId::new("ns"),
                old_parent_inode_id: InodeId::new("1"),
                old_name: b"old".to_vec(),
                new_parent_inode_id: InodeId::new("2"),
                new_name: b"new".to_vec(),
                mode: RenameMode::NoReplace,
                caller: caller(),
            },
        ),
        case(
            "link",
            &LinkRequest {
                caller_id: "node-a".into(),
                operation_id: OperationId::new("op"),
                namespace_id: NamespaceId::new("ns"),
                existing_inode_id: InodeId::new("inode"),
                expected_inode_revision: 6,
                parent_inode_id: InodeId::new("1"),
                name: b"alias".to_vec(),
                caller: caller(),
            },
        ),
        case(
            "symlink",
            &SymlinkRequest {
                caller_id: "node-a".into(),
                operation_id: OperationId::new("op"),
                namespace_id: NamespaceId::new("ns"),
                parent_inode_id: InodeId::new("1"),
                name: b"link".to_vec(),
                target: b"../target".to_vec(),
                attributes: attributes(),
                caller: caller(),
            },
        ),
        case(
            "claim_replication_task",
            &ClaimReplicationTask {
                caller_id: "node-a".into(),
                caller_session_id: "session-a".into(),
                caller_node_epoch: 2,
                operation_id: OperationId::new("op"),
                lease_seconds: 30,
            },
        ),
        case(
            "report_replication_task",
            &ReportReplicationTask {
                caller_id: "node-a".into(),
                caller_session_id: "session-a".into(),
                caller_node_epoch: 2,
                operation_id: OperationId::new("op"),
                claim: replication_claim(),
                durable_acks: vec![ack()],
                error: Some("failed".into()),
                source_invalid: true,
            },
        ),
        case(
            "report_chunk_corruption",
            &ReportChunkCorruption {
                caller_id: "node-a".into(),
                caller_session_id: "session-a".into(),
                caller_node_epoch: 2,
                operation_id: OperationId::new("op"),
                chunk_id: chunk().id,
                device_id: "dev-a".into(),
                device_epoch: 4,
                catalog_revision: 5,
            },
        ),
        case("commit_without_fence", &commit(None, None)),
        case(
            "commit_with_generation_fence_and_clone",
            &commit(
                Some(GcFence::WriteGeneration),
                Some(CloneFileRange {
                    version: FileVersionId::new("source"),
                    source_offset: 1,
                    destination_offset: 2,
                    length: 3,
                }),
            ),
        ),
    ]
}

const GOLDEN: &[(&str, &str, &str)] = &[
    (
        "create",
        r#"{"caller_id":"node-a","owner_session_id":"session-a","operation_id":"op","namespace_id":"ns","parent_inode_id":"1","name":[102,105,108,101],"attributes":{"mode":33188,"uid":1000,"gid":1001,"nlink":1,"atime_unix_ms":11,"mtime_unix_ms":12,"ctime_unix_ms":13},"lease_seconds":30}"#,
        "da0f3d62ec19e3adbea045e87d1ecc510f3e40b6337601c68ff62908774b9978",
    ),
    (
        "mkdir",
        r#"{"caller_id":"node-a","operation_id":"op","namespace_id":"ns","parent_inode_id":"1","name":[100,105,114],"attributes":{"mode":33188,"uid":1000,"gid":1001,"nlink":1,"atime_unix_ms":11,"mtime_unix_ms":12,"ctime_unix_ms":13},"caller":{"uid":1000,"gid":1001,"supplementary_gids":[10,20]}}"#,
        "57283d44c77659939a662c3539e6ba12d4bd2549b139028f82577ef37e526a18",
    ),
    (
        "mknod",
        r#"{"caller_id":"node-a","operation_id":"op","namespace_id":"ns","parent_inode_id":"1","name":[100,101,118],"kind":{"CharDevice":{"rdev":259}},"attributes":{"mode":33188,"uid":1000,"gid":1001,"nlink":1,"atime_unix_ms":11,"mtime_unix_ms":12,"ctime_unix_ms":13},"caller":{"uid":1000,"gid":1001,"supplementary_gids":[10,20]}}"#,
        "a1c6200b4ca460720d2a8bef08c3549a7aab6105e1e1816c7349c98f8f924718",
    ),
    (
        "unlink",
        r#"{"caller_id":"node-a","operation_id":"op","namespace_id":"ns","parent_inode_id":"1","name":[102,105,108,101],"caller":{"uid":1000,"gid":1001,"supplementary_gids":[10,20]}}"#,
        "19017fe8a2ab1385fb421552969230973a3e9d9a7c42cba89e2fd610364b129e",
    ),
    (
        "rename",
        r#"{"caller_id":"node-a","operation_id":"op","namespace_id":"ns","old_parent_inode_id":"1","old_name":[111,108,100],"new_parent_inode_id":"2","new_name":[110,101,119],"mode":"NoReplace","caller":{"uid":1000,"gid":1001,"supplementary_gids":[10,20]}}"#,
        "ff4431b48df5b68b1a31032d6c6e2e8fbf65c451d84f93fa9fe93e839e59d492",
    ),
    (
        "link",
        r#"{"caller_id":"node-a","operation_id":"op","namespace_id":"ns","existing_inode_id":"inode","expected_inode_revision":6,"parent_inode_id":"1","name":[97,108,105,97,115],"caller":{"uid":1000,"gid":1001,"supplementary_gids":[10,20]}}"#,
        "f4e5cf2ed4bbdf7b93c709f3ce4404174ddb468948cc28c46181b1f122dd4c73",
    ),
    (
        "symlink",
        r#"{"caller_id":"node-a","operation_id":"op","namespace_id":"ns","parent_inode_id":"1","name":[108,105,110,107],"target":[46,46,47,116,97,114,103,101,116],"attributes":{"mode":33188,"uid":1000,"gid":1001,"nlink":1,"atime_unix_ms":11,"mtime_unix_ms":12,"ctime_unix_ms":13},"caller":{"uid":1000,"gid":1001,"supplementary_gids":[10,20]}}"#,
        "b85de63d8cac698aa4ff26859a2cea7a19bd5bfe0d454f874969d4b9701a7879",
    ),
    (
        "claim_replication_task",
        r#"{"caller_id":"node-a","caller_session_id":"session-a","caller_node_epoch":2,"operation_id":"op","lease_seconds":30}"#,
        "f4305ac8609f698a7a9a4732105313e67b704b45638169a3901388c9d8dcd925",
    ),
    (
        "report_replication_task",
        r#"{"caller_id":"node-a","caller_session_id":"session-a","caller_node_epoch":2,"operation_id":"op","claim":{"task_id":"repair:c","operation_id":"claim","worker_node_id":"node-a","worker_node_epoch":2,"worker_session_id":"session-a","expires_at_unix_ms":99,"fence":1,"chunk":{"id":"blake3-abababababababababababababababababababababababababababababababab-42","length":42,"content_digest":{"algorithm":"Blake3","bytes":[171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171]},"encoding":"Raw"},"source_copy_id":"copy","placement_revision":7,"replica_group":{"id":"group","placement_epoch":3,"targets":[{"node_id":"node-a","node_epoch":2,"data_endpoint":"http://node-a:2","device":{"device_id":"dev-a","device_epoch":4,"catalog_revision":5,"failure_domain":"fd-a"}}]},"replication":{"desired_copies":1,"sync_required_copies":1,"min_distinct_nodes":1,"min_distinct_failure_domains":1,"local_copy":"Required"}},"durable_acks":[{"operation_id":"op","chunk_id":"blake3-abababababababababababababababababababababababababababababababab-42","placement_revision":7,"placement_epoch":3,"node_id":"node-a","node_epoch":2,"device_id":"dev-a","device_epoch":4,"catalog_revision":5,"persisted_bytes":42,"verified_digest":{"algorithm":"Blake3","bytes":[171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171]}}],"error":"failed","source_invalid":true}"#,
        "337eadca687cc28dc7b0e34734ace97f7009ca4bfd973524027b6cfe8db97789",
    ),
    (
        "report_chunk_corruption",
        r#"{"caller_id":"node-a","caller_session_id":"session-a","caller_node_epoch":2,"operation_id":"op","chunk_id":"blake3-abababababababababababababababababababababababababababababababab-42","device_id":"dev-a","device_epoch":4,"catalog_revision":5}"#,
        "a76ff0a9d421e483f338f8900fb7eb488a6e9b12e42e0aad7956a347a0be470e",
    ),
    (
        "commit_without_fence",
        r#"{"operation_id":"op","inode_id":"inode","write_lease":{"inode_id":"inode","owner_node_id":"node-a","owner_session_id":"session-a","lease_epoch":2,"expires_at_unix_ms":99},"expected_inode_revision":6,"expected_head_version":"v0","file_version":{"id":"v1","inode_id":"inode","parent_version":"v0","length":42,"layout_root":"layout","created_at_unix_ms":8},"layout_root":{"id":"layout","file_length":42,"inline_extents":[{"file_offset":0,"length":42,"chunk_id":"blake3-abababababababababababababababababababababababababababababababab-42","chunk_offset":0}]},"chunk_receipts":[{"operation_id":"op","chunk":{"id":"blake3-abababababababababababababababababababababababababababababababab-42","length":42,"content_digest":{"algorithm":"Blake3","bytes":[171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171]},"encoding":"Raw"},"placement_revision":7,"placement_epoch":3,"replica_group_id":"group","durable_acks":[{"operation_id":"op","chunk_id":"blake3-abababababababababababababababababababababababababababababababab-42","placement_revision":7,"placement_epoch":3,"node_id":"node-a","node_epoch":2,"device_id":"dev-a","device_epoch":4,"catalog_revision":5,"persisted_bytes":42,"verified_digest":{"algorithm":"Blake3","bytes":[171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171]}}]}],"clone_source":null,"metadata_delta":{"mode":"Full","mtime_unix_ms":12,"ctime_unix_ms":13,"kill_suidgid":true}}"#,
        "174e85ec80b8076245be61581089a2273053bd6091e697f9bcef3062eb1cbfb8",
    ),
    (
        "commit_with_generation_fence_and_clone",
        r#"{"gc_fence":"WriteGeneration","operation_id":"op","inode_id":"inode","write_lease":{"inode_id":"inode","owner_node_id":"node-a","owner_session_id":"session-a","lease_epoch":2,"expires_at_unix_ms":99},"expected_inode_revision":6,"expected_head_version":"v0","file_version":{"id":"v1","inode_id":"inode","parent_version":"v0","length":42,"layout_root":"layout","created_at_unix_ms":8},"layout_root":{"id":"layout","file_length":42,"inline_extents":[{"file_offset":0,"length":42,"chunk_id":"blake3-abababababababababababababababababababababababababababababababab-42","chunk_offset":0}]},"chunk_receipts":[{"operation_id":"op","chunk":{"id":"blake3-abababababababababababababababababababababababababababababababab-42","length":42,"content_digest":{"algorithm":"Blake3","bytes":[171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171]},"encoding":"Raw"},"placement_revision":7,"placement_epoch":3,"replica_group_id":"group","durable_acks":[{"operation_id":"op","chunk_id":"blake3-abababababababababababababababababababababababababababababababab-42","placement_revision":7,"placement_epoch":3,"node_id":"node-a","node_epoch":2,"device_id":"dev-a","device_epoch":4,"catalog_revision":5,"persisted_bytes":42,"verified_digest":{"algorithm":"Blake3","bytes":[171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171,171]}}]}],"clone_source":{"version":"source","source_offset":1,"destination_offset":2,"length":3},"metadata_delta":{"mode":"Full","mtime_unix_ms":12,"ctime_unix_ms":13,"kill_suidgid":true}}"#,
        "739958aee8058c6c338e96a3914fc7ff32db90e4a3db6b184fd097f8a7739e13",
    ),
];

#[test]
fn namespace_request_digests_match_golden() {
    let actual = cases();
    let names = actual.iter().map(|(name, ..)| *name).collect::<Vec<_>>();
    let golden_names = GOLDEN.iter().map(|(name, ..)| *name).collect::<Vec<_>>();
    assert_eq!(
        names, golden_names,
        "golden table must list every request case"
    );
    for ((name, json, digest), (_, golden_json, golden_digest)) in actual.iter().zip(GOLDEN) {
        assert_eq!(
            (json.as_str(), digest.as_str()),
            (*golden_json, *golden_digest),
            "namespace request digest for {name} changed: the digest is persisted, \
             so this is a storage format change; bump the format version and \
             regenerate with print_request_digest_goldens"
        );
    }
}

#[test]
#[ignore = "prints regenerated goldens"]
fn print_request_digest_goldens() {
    for (name, json, digest) in cases() {
        println!("    (\n        {name:?},\n        r#\"{json}\"#,\n        {digest:?},\n    ),");
    }
}
