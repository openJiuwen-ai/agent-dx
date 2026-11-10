//! Pins the postcard encoding of Meta WAL/checkpoint payload types.
//!
//! Postcard encodes an enum variant as its declaration index (a varint; every
//! index here is below 128, so it is the first byte). Reordering, inserting or
//! removing a variant therefore changes the durable format. Each enum is
//! listed with an exhaustive `match` without a wildcard, so a new variant also
//! fails to compile here until it is pinned.
//!
//! Changing any pinned index or golden byte string is a format change: bump
//! the owning format version (Envelope `VERSION`, `SCHEMA`, `IMAGE_SCHEMA`)
//! and regenerate with
//! `cargo test -p afs --lib log::format_golden_tests::print_log_goldens -- --ignored --nocapture`.

use serde::Serialize;

use super::*;
use crate::dfs::{
    ChunkEncoding, ChunkId, ContentDigest, CopyLocation, CopyRole, CopyState, DfsDurabilityPolicy,
    DigestAlgorithm, GcReference, GcState, InodeId, InodeKind, LocalCopyPolicy, PlacementHealth,
    ReplicaGroupId, ReplicationTaskState, SpecialNodeKind,
};
use crate::meta::store::{RootCommandType, RootRight, codec::tests::entity_fixtures, state::State};

fn check_variants<T: Serialize>(name: &str, samples: &[(u8, T)], pinned: fn(&T) -> u8) {
    for (position, (index, sample)) in samples.iter().enumerate() {
        assert_eq!(
            usize::from(*index),
            position,
            "{name}: list every variant once, in declaration order"
        );
        assert_eq!(
            pinned(sample),
            *index,
            "{name}: sample {position} has another variant"
        );
        let bytes = postcard::to_allocvec(sample).unwrap();
        assert_eq!(
            bytes[0], *index,
            "{name} variant {position} changed its postcard index: this is a durable \
             format change; bump the format version and update the pinned table"
        );
    }
}

macro_rules! pin_variants {
    ($ty:ty { $($index:literal: $pattern:pat => $sample:expr),+ $(,)? }) => {{
        // No wildcard arm: an unpinned new variant fails to compile.
        fn pinned(value: &$ty) -> u8 {
            match value {
                $($pattern => $index,)+
            }
        }
        let samples: Vec<(u8, $ty)> = vec![$(($index, $sample)),+];
        check_variants(stringify!($ty), &samples, pinned);
    }};
}

fn digest() -> ContentDigest {
    ContentDigest {
        algorithm: DigestAlgorithm::Blake3,
        bytes: [7; 32],
    }
}

fn chunk() -> crate::dfs::ChunkObject {
    crate::dfs::ChunkObject {
        id: ChunkId::new("c"),
        length: 1,
        content_digest: digest(),
        encoding: ChunkEncoding::Raw,
    }
}

fn entity(index: usize) -> MetaEntity {
    entity_fixtures().swap_remove(index)
}

fn replication_task() -> ReplicationTask {
    match entity(15) {
        MetaEntity::DfsReplicationTask(task) => task,
        other => panic!("fixture 15 is {}", other.name()),
    }
}

fn saved_claim() -> SavedClaim {
    SavedClaim {
        task_id: ReplicationTaskId::new("task"),
        operation_id: OperationId::new("op"),
        worker_node_id: "a".into(),
        worker_node_epoch: 1,
        worker_session_id: "s".into(),
        expires_at_unix_ms: 42,
        fence: 1,
        chunk: ValueId(0),
        source_copy_id: CopyId::new("copy"),
        placement_revision: 2,
        replica_group: ReplicaGroup {
            id: ReplicaGroupId::new("g"),
            placement_epoch: 1,
            targets: Vec::new(),
        },
        replication: ValueId(1),
    }
}

fn request() -> RequestKey {
    RequestKey::new("a", "r")
}

#[test]
fn meta_store_enum_variant_indices_are_pinned() {
    pin_variants!(RootRight {
        0: RootRight::Lookup => RootRight::Lookup,
        1: RootRight::Read => RootRight::Read,
        2: RootRight::Write => RootRight::Write,
        3: RootRight::Admin => RootRight::Admin,
    });
    pin_variants!(RootCommandType {
        0: RootCommandType::RevokeAccess => RootCommandType::RevokeAccess,
        1: RootCommandType::InvalidateCache => RootCommandType::InvalidateCache,
    });
    pin_variants!(MetaEntity {
        0: MetaEntity::NodeSession(_) => entity(0),
        1: MetaEntity::RootReservation(_) => entity(1),
        2: MetaEntity::Root(_) => entity(2),
        3: MetaEntity::RootGrant(_) => entity(3),
        4: MetaEntity::RootCommand(_) => entity(4),
        5: MetaEntity::RootCommandAck(_) => entity(5),
        6: MetaEntity::DfsDentry(_) => entity(6),
        7: MetaEntity::DfsInode(_) => entity(7),
        8: MetaEntity::DfsFileVersion(_) => entity(8),
        9: MetaEntity::DfsLayoutRoot(_) => entity(9),
        10: MetaEntity::DfsChunk(_) => entity(10),
        11: MetaEntity::DfsReplicationConfig(_) => entity(11),
        12: MetaEntity::DfsDurabilityPolicy(_) => entity(12),
        13: MetaEntity::DfsPlacement(_) => entity(13),
        14: MetaEntity::DfsCopy(_) => entity(14),
        15: MetaEntity::DfsReplicationTask(_) => entity(15),
        16: MetaEntity::DfsWriteLease(_) => entity(16),
        17: MetaEntity::RootEpochWatermark(_) => MetaEntity::RootEpochWatermark(3),
        18: MetaEntity::DfsReference(_) => MetaEntity::DfsReference(reference()),
        19: MetaEntity::DfsGc(_) => MetaEntity::DfsGc(gc_state()),
        20: MetaEntity::DfsReplicaFailureConfig(_) => MetaEntity::DfsReplicaFailureConfig(
            crate::dfs::ReplicaFailureConfig::default(),
        ),
    });
    pin_variants!(MetaKey {
        0: MetaKey::NodeSession { .. } => MetaKey::NodeSession {
            node_id: "a".into(),
            session_id: "s".into(),
        },
        1: MetaKey::CurrentNodeSession { .. } => MetaKey::CurrentNodeSession {
            node_id: "a".into(),
        },
        2: MetaKey::CurrentNodeSessions => MetaKey::CurrentNodeSessions,
        3: MetaKey::Root { .. } => MetaKey::Root { root_id: "r".into() },
        4: MetaKey::RootReservation { .. } => MetaKey::RootReservation { root_id: "r".into() },
        5: MetaKey::RootGrant { .. } => MetaKey::RootGrant {
            root_id: "r".into(),
            holder_node_id: "b".into(),
            holder_session_id: "t".into(),
        },
        6: MetaKey::RootCommand { .. } => MetaKey::RootCommand {
            command_id: "c".into(),
        },
        7: MetaKey::RootCommandAck { .. } => MetaKey::RootCommandAck {
            command_id: "c".into(),
            home_node_id: "a".into(),
            home_session_id: "s".into(),
        },
        8: MetaKey::RequestOutcome(_) => MetaKey::RequestOutcome(request()),
        9: MetaKey::DfsDentry(_) => MetaKey::DfsDentry(crate::dfs::DentryKey {
            namespace_id: crate::dfs::NamespaceId::new("n"),
            parent_inode_id: InodeId::new("p"),
            name: b"x".to_vec(),
        }),
        10: MetaKey::DfsInode(_) => MetaKey::DfsInode(InodeId::new("i")),
        11: MetaKey::DfsFileVersion(_) => MetaKey::DfsFileVersion(crate::dfs::FileVersionId::new("v")),
        12: MetaKey::DfsLayoutRoot(_) => MetaKey::DfsLayoutRoot(crate::dfs::LayoutRootId::new("l")),
        13: MetaKey::DfsChunk(_) => MetaKey::DfsChunk(ChunkId::new("c")),
        14: MetaKey::DfsReplicationConfig => MetaKey::DfsReplicationConfig,
        15: MetaKey::DfsDurabilityPolicy => MetaKey::DfsDurabilityPolicy,
        16: MetaKey::DfsPlacement(_) => MetaKey::DfsPlacement(ChunkId::new("c")),
        17: MetaKey::DfsCopy(_) => MetaKey::DfsCopy(CopyId::new("copy")),
        18: MetaKey::DfsReplicationTask(_) => MetaKey::DfsReplicationTask(ReplicationTaskId::new("t")),
        19: MetaKey::DfsWriteLease(_) => MetaKey::DfsWriteLease(InodeId::new("i")),
        20: MetaKey::RootEpochWatermark => MetaKey::RootEpochWatermark,
        21: MetaKey::DfsReference { .. } => MetaKey::DfsReference {
            node_id: "a".into(),
            session_id: "s".into(),
            reference_id: "write:g".into(),
        },
        22: MetaKey::DfsGc => MetaKey::DfsGc,
        23: MetaKey::DfsReplicaFailureConfig => MetaKey::DfsReplicaFailureConfig,
    });
    pin_variants!(StoreOperation {
        0: StoreOperation::RegisterNode => StoreOperation::RegisterNode,
        1: StoreOperation::ReserveRoot => StoreOperation::ReserveRoot,
        2: StoreOperation::AbortRoot => StoreOperation::AbortRoot,
        3: StoreOperation::DeactivateRoot => StoreOperation::DeactivateRoot,
        4: StoreOperation::ActivateRoot => StoreOperation::ActivateRoot,
        5: StoreOperation::AcquireRoot => StoreOperation::AcquireRoot,
        6: StoreOperation::RecoverRoot => StoreOperation::RecoverRoot,
        7: StoreOperation::BeginRootRevocation => StoreOperation::BeginRootRevocation,
        8: StoreOperation::CommitRootRevocation => StoreOperation::CommitRootRevocation,
        9: StoreOperation::AckRootCommand => StoreOperation::AckRootCommand,
        10: StoreOperation::DfsCreate => StoreOperation::DfsCreate,
        11: StoreOperation::DfsMkdir => StoreOperation::DfsMkdir,
        12: StoreOperation::DfsMknod => StoreOperation::DfsMknod,
        13: StoreOperation::DfsInitializeNamespace => StoreOperation::DfsInitializeNamespace,
        14: StoreOperation::DfsUnlink => StoreOperation::DfsUnlink,
        15: StoreOperation::DfsRmdir => StoreOperation::DfsRmdir,
        16: StoreOperation::DfsRename => StoreOperation::DfsRename,
        17: StoreOperation::DfsLink => StoreOperation::DfsLink,
        18: StoreOperation::DfsSymlink => StoreOperation::DfsSymlink,
        19: StoreOperation::DfsSetInodeAttributes => StoreOperation::DfsSetInodeAttributes,
        20: StoreOperation::DfsSetXattr => StoreOperation::DfsSetXattr,
        21: StoreOperation::DfsRemoveXattr => StoreOperation::DfsRemoveXattr,
        22: StoreOperation::DfsAcquireWriteLease => StoreOperation::DfsAcquireWriteLease,
        23: StoreOperation::DfsRenewWriteLease => StoreOperation::DfsRenewWriteLease,
        24: StoreOperation::DfsSyncInodeMetadata => StoreOperation::DfsSyncInodeMetadata,
        25: StoreOperation::DfsCommitFileVersion => StoreOperation::DfsCommitFileVersion,
        26: StoreOperation::DfsInitializeReplicationConfig => StoreOperation::DfsInitializeReplicationConfig,
        27: StoreOperation::DfsInitializeDurabilityPolicy => StoreOperation::DfsInitializeDurabilityPolicy,
        28: StoreOperation::DfsClaimReplicationTask => StoreOperation::DfsClaimReplicationTask,
        29: StoreOperation::DfsReportReplicationTask => StoreOperation::DfsReportReplicationTask,
        30: StoreOperation::DfsReportChunkCorruption => StoreOperation::DfsReportChunkCorruption,
        31: StoreOperation::CleanupRootGrants => StoreOperation::CleanupRootGrants,
        32: StoreOperation::CleanupRootCommands => StoreOperation::CleanupRootCommands,
        33: StoreOperation::DfsUpdateReference => StoreOperation::DfsUpdateReference,
        34: StoreOperation::DfsCollectGarbage => StoreOperation::DfsCollectGarbage,
        35: StoreOperation::DfsInitializeReplicaFailureConfig => StoreOperation::DfsInitializeReplicaFailureConfig,
    });
}

#[test]
fn meta_log_enum_variant_indices_are_pinned() {
    let decision = match golden_record() {
        Record::Decision { decision, .. } => decision,
        _ => panic!("test record is not a decision"),
    };
    pin_variants!(Record {
        0: Record::Decision { .. } => Record::Decision {
            decision: decision.clone(),
            history: None,
        },
        1: Record::History(_) => Record::History(history_delta()),
        2: Record::AppendLayout { .. } => Record::AppendLayout {
            decision,
            history: None,
            base: LayoutAppendBase { layout: crate::dfs::LayoutRootId::new("base"), length: 1 },
        },
    });
    pin_variants!(Change {
        0: Change::Put { .. } => Change::Put {
            key: MetaKey::DfsGc,
            value: ValueId(0),
        },
        1: Change::Delete { .. } => Change::Delete { key: MetaKey::DfsGc },
    });
    pin_variants!(SavedResult {
        0: SavedResult::NodeSession(_) => SavedResult::NodeSession(ValueId(0)),
        1: SavedResult::RootReservation(_) => SavedResult::RootReservation(ValueId(0)),
        2: SavedResult::RootRecord(_) => SavedResult::RootRecord(ValueId(0)),
        3: SavedResult::RootGrant(_) => SavedResult::RootGrant(ValueId(0)),
        4: SavedResult::RootCommand(_) => SavedResult::RootCommand(ValueId(0)),
        5: SavedResult::RootCommandAck(_) => SavedResult::RootCommandAck(ValueId(0)),
        6: SavedResult::DfsInode(_) => SavedResult::DfsInode(ValueId(0)),
        7: SavedResult::DfsInodeWithLease { .. } => SavedResult::DfsInodeWithLease {
            inode: ValueId(0),
            lease: ValueId(1),
        },
        8: SavedResult::DfsWriteLease(_) => SavedResult::DfsWriteLease(ValueId(0)),
        9: SavedResult::DfsReplicationClaim { .. } => SavedResult::DfsReplicationClaim {
            request_digest: [1; 32],
            claim: Some(ValueId(0)),
        },
        10: SavedResult::DfsReplicationTask(_) => SavedResult::DfsReplicationTask(ValueId(0)),
        11: SavedResult::DfsRename { .. } => SavedResult::DfsRename {
            inode: ValueId(0),
            replaced_inode: None,
        },
        12: SavedResult::Empty => SavedResult::Empty,
    });
    pin_variants!(PooledValue {
        0: PooledValue::Entity(_) => PooledValue::Entity(entity(0)),
        1: PooledValue::ClaimedTask { .. } => PooledValue::ClaimedTask {
            task: replication_task(),
            claim: ValueId(0),
        },
        2: PooledValue::Claim(_) => PooledValue::Claim(saved_claim()),
    });
}

#[test]
fn dfs_persisted_enum_variant_indices_are_pinned() {
    pin_variants!(SpecialNodeKind {
        0: SpecialNodeKind::Fifo => SpecialNodeKind::Fifo,
        1: SpecialNodeKind::Socket => SpecialNodeKind::Socket,
        2: SpecialNodeKind::BlockDevice { .. } => SpecialNodeKind::BlockDevice { rdev: 1 },
        3: SpecialNodeKind::CharDevice { .. } => SpecialNodeKind::CharDevice { rdev: 1 },
    });
    pin_variants!(InodeKind {
        0: InodeKind::Regular => InodeKind::Regular,
        1: InodeKind::Directory => InodeKind::Directory,
        2: InodeKind::Symlink => InodeKind::Symlink,
        3: InodeKind::Special(_) => InodeKind::Special(SpecialNodeKind::Fifo),
    });
    pin_variants!(DigestAlgorithm {
        0: DigestAlgorithm::Blake3 => DigestAlgorithm::Blake3,
        1: DigestAlgorithm::Crc32cSegmentsV1 => DigestAlgorithm::Crc32cSegmentsV1,
    });
    pin_variants!(ChunkEncoding {
        0: ChunkEncoding::Raw => ChunkEncoding::Raw,
    });
    pin_variants!(DfsDurabilityPolicy {
        0: DfsDurabilityPolicy::NodeReplicated => DfsDurabilityPolicy::NodeReplicated,
        1: DfsDurabilityPolicy::ExternalBackend { .. } => DfsDurabilityPolicy::ExternalBackend {
            store_id: "store".into(),
            generation: "g".into(),
        },
    });
    pin_variants!(crate::dfs::ReplicaFailurePolicy {
        0: crate::dfs::ReplicaFailurePolicy::Degrade => crate::dfs::ReplicaFailurePolicy::Degrade,
        1: crate::dfs::ReplicaFailurePolicy::Strict => crate::dfs::ReplicaFailurePolicy::Strict,
    });
    pin_variants!(LocalCopyPolicy {
        0: LocalCopyPolicy::Required => LocalCopyPolicy::Required,
        1: LocalCopyPolicy::Preferred => LocalCopyPolicy::Preferred,
        2: LocalCopyPolicy::NotRequired => LocalCopyPolicy::NotRequired,
    });
    pin_variants!(CopyRole {
        0: CopyRole::DurableReplica => CopyRole::DurableReplica,
        1: CopyRole::VerifiedCache => CopyRole::VerifiedCache,
        2: CopyRole::ExternalCommitted => CopyRole::ExternalCommitted,
    });
    pin_variants!(CopyState {
        0: CopyState::Reserved0 => CopyState::Reserved0,
        1: CopyState::Ready => CopyState::Ready,
        2: CopyState::Corrupt => CopyState::Corrupt,
        3: CopyState::Deleting => CopyState::Deleting,
    });
    pin_variants!(CopyLocation {
        0: CopyLocation::Node { .. } => CopyLocation::Node {
            node_id: "a".into(),
            node_epoch: 1,
            device_id: "d".into(),
            device_epoch: 1,
            catalog_revision: 1,
        },
        1: CopyLocation::External { .. } => CopyLocation::External {
            store_id: "store".into(),
            object_key: "object".into(),
            object_revision: "g".into(),
        },
    });
    pin_variants!(PlacementHealth {
        0: PlacementHealth::Satisfied => PlacementHealth::Satisfied,
        1: PlacementHealth::UnderReplicated => PlacementHealth::UnderReplicated,
        2: PlacementHealth::BlockedNoSource => PlacementHealth::BlockedNoSource,
    });
    pin_variants!(ReplicationTaskState {
        0: ReplicationTaskState::Pending => ReplicationTaskState::Pending,
        1: ReplicationTaskState::Running => ReplicationTaskState::Running,
        2: ReplicationTaskState::RetryWaiting => ReplicationTaskState::RetryWaiting,
        3: ReplicationTaskState::Completed => ReplicationTaskState::Completed,
        4: ReplicationTaskState::BlockedNoSource => ReplicationTaskState::BlockedNoSource,
    });
}

fn reference() -> GcReference {
    GcReference {
        node_id: "a".into(),
        session_id: "s".into(),
        reference_id: "write:g".into(),
        inode_id: Some(InodeId::new("i")),
        publication: true,
        chunks: vec![chunk()],
        retired_at_unix_ms: Some(42),
        open_read: Some(crate::dfs::OpenReadIdentity {
            version_id: crate::dfs::FileVersionId::new("v"),
            layout_root_id: crate::dfs::LayoutRootId::new("l"),
            closed: false,
        }),
        pinned_version: Some(crate::dfs::FileVersionId::new("v")),
    }
}

fn gc_state() -> GcState {
    GcState {
        generation: 2,
        chunks: vec![ChunkId::new("c")],
        waiting_nodes: vec!["a".into()],
    }
}

fn history_delta() -> super::super::history::Delta {
    super::super::history::Delta {
        revision: StoreRevision(3),
        selected_at_unix_ms: 42,
        compacted_from: StoreRevision(1),
        compacted_to: StoreRevision(2),
        requests: Vec::new(),
    }
}

/// A deterministic create decision: one dentry and one inode at revision 1.
/// Real registrations read the wall clock for lease expiry, so they are not
/// usable as goldens.
pub(in crate::meta::store) fn golden_record() -> Record {
    let dentry = entity(6);
    let inode = entity(7);
    let MetaEntity::DfsInode(inode_record) = inode.clone() else {
        panic!("fixture 7 is not an inode");
    };
    let events = [dentry, inode]
        .into_iter()
        .enumerate()
        .map(|(index, entity)| WatchEvent {
            revision: StoreRevision(1),
            event_index: index as u32,
            key: entity_key(&entity),
            change: WatchChange::Put(entity),
        })
        .collect::<Vec<_>>();
    let reply = RequestOutcome {
        request: request(),
        operation: StoreOperation::DfsCreate,
        result: OperationResult::DfsNamespace {
            request_digest: [5; 32],
            result: Box::new(OperationResult::DfsInode(inode_record)),
        },
    };
    RecordDecision::new(
        DecisionInput {
            request: request(),
            operation: StoreOperation::DfsCreate,
            revision_before: StoreRevision(0),
            revision_after: StoreRevision(1),
            completed_at_unix_ms: 12_345,
            events: &events,
            outcomes: std::slice::from_ref(&reply),
            reply: &reply,
            node_epochs: vec![("a".into(), 1)],
        },
        1024 * 1024,
    )
    .unwrap()
    .into()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn cases() -> Vec<(String, Vec<u8>)> {
    let mut cases = entity_fixtures()
        .into_iter()
        .map(|entity| {
            (
                format!("entity.{}", entity.name()),
                postcard::to_allocvec(&entity).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    for entity in [
        MetaEntity::RootEpochWatermark(3),
        MetaEntity::DfsReference(reference()),
        MetaEntity::DfsGc(gc_state()),
    ] {
        cases.push((
            format!("entity.{}", entity.name()),
            postcard::to_allocvec(&entity).unwrap(),
        ));
    }
    let record = golden_record();
    cases.push((
        "record.create".into(),
        postcard::to_allocvec(&record).unwrap(),
    ));
    cases.push((
        "record.history".into(),
        postcard::to_allocvec(&Record::History(history_delta())).unwrap(),
    ));
    let state = State::default().replay_record(&record).unwrap();
    cases.push((
        "state_image.create".into(),
        StateImage::encode(&state, 1024 * 1024).unwrap(),
    ));
    cases
}

#[test]
fn meta_log_payload_bytes_match_golden() {
    let actual = cases();
    let names = actual
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>();
    let golden = GOLDEN.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    assert_eq!(names, golden, "golden table must list every case");
    for ((name, bytes), (_, expected)) in actual.iter().zip(GOLDEN) {
        assert_eq!(
            hex(bytes),
            *expected,
            "{name} encoding changed: this is a durable format change; bump the \
             owning format version and regenerate with print_log_goldens"
        );
    }
}

#[test]
#[ignore = "prints regenerated goldens"]
fn print_log_goldens() {
    for (name, bytes) in cases() {
        println!("    ({name:?}, {:?}),", hex(&bytes));
    }
}

const GOLDEN: &[(&str, &str)] = &[
    ("entity.node_session", "00016101730167016401720100012a"),
    (
        "entity.root_reservation",
        "0101720101610173016301700161017302",
    ),
    ("entity.root", "020172010161017301700203"),
    (
        "entity.root_grant",
        "03017201016101730162017402020102016603",
    ),
    ("entity.root_command", "040163016101730172010201"),
    (
        "entity.root_command_ack",
        "0501630161017301720102000872656a6563746564",
    ),
    ("entity.dfs_dentry", "06016e01700200ff0169"),
    (
        "entity.dfs_inode",
        "07016e016900a403010201030405010176000104303066660200ff06",
    ),
    ("entity.dfs_file_version", "08017601690001016c2a"),
    ("entity.dfs_layout_root", "09016c01010001016300"),
    (
        "entity.dfs_chunk",
        "0a01630100000000000000000000000000000000000000000000000000000000000000000000",
    ),
    ("entity.dfs_replication_config", "0b0101010100"),
    ("entity.dfs_durability_policy", "0c010573746f72650167"),
    ("entity.dfs_placement", "0d0163016701010104636f707900"),
    (
        "entity.dfs_copy",
        "0e04636f7079016300010573746f7265066f626a65637401670101000000000000000000000000000000000000000000000000000000000000000000",
    ),
    (
        "entity.dfs_replication_task",
        "0f047461736b016301010002012a0105726574727900",
    ),
    ("entity.dfs_write_lease", "10016901610173012a"),
    ("entity.root_epoch_watermark", "1103"),
    (
        "entity.dfs_reference",
        "12016101730777726974653a67010169010101630100070707070707070707070707070707070707070707070707070707070707070700012a010176016c00010176",
    ),
    ("entity.dfs_gc", "1302010163010161"),
    (
        "record.create",
        "0002016101720a0001b96000020006016e01700200ff01690007016e016900a403010201030405010176000104303066660200ff06020009016e01700200ff00000a01690101016101720a0105050505050505050505050505050505050505050505050505050505050505050601000101610100",
    ),
    ("record.history", "01032a010200"),
    (
        "state_image.create",
        "41465343030100000101b960020006016e01700200ff01690007016e016900a403010201030405010176000104303066660200ff060209016e01700200ff01000a0169010101016101720a0105050505050505050505050505050505050505050505050505050505050505050601b960010201000009016e01700200ff000101000a01690101016101",
    ),
];
