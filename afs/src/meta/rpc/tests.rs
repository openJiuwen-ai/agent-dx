use super::*;
use tonic::Code;

#[tokio::test]
async fn p3_watch_rpc_idle_response_expires_without_delivering_old_commands() {
    use super::super::store::{
        MetaEntity, MetaStore, MetaTxn, OperationResult, RequestOutcome, Store, StoreOperation,
        TxnCondition, TxnMutation, journal::MemoryLog,
    };
    use tokio_stream::StreamExt;
    let store = Arc::new(
        Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
            .await
            .unwrap(),
    );
    let key = RequestKey::new("watch-test", "install");
    let command = RootCommandRecord {
        command_id: "command".into(),
        home_node_id: "node".into(),
        home_session_id: "session".into(),
        root_id: "root".into(),
        root_epoch: 1,
        old_access_generation: 1,
        command_type: StoreRootCommandType::InvalidateCache,
    };
    let mut txn = MetaTxn::new(key.clone(), StoreOperation::BeginRootRevocation);
    txn.conditions
        .push(TxnCondition::RequestAbsent(key.clone()));
    txn.mutations.extend([
        TxnMutation::Put(MetaEntity::RootCommand(command.clone())),
        TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: key,
            operation: StoreOperation::BeginRootRevocation,
            result: OperationResult::RootCommand(command),
        }),
    ]);
    store.compare_and_commit(txn).await.unwrap();
    let meta = Arc::new(super::super::Meta::with_store(
        "watch-test".into(),
        crate::runtime::Observability::new().unwrap(),
        store.clone(),
    ));
    let mut stream = OwnerRootsRpc(meta)
        .watch_root_commands(Request::new(WatchRootCommandsRequest {
            node_id: "node".into(),
            session_id: "session".into(),
            after_revision: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    // Keep the response entirely unpolled: deadline must also detach idle
    // application queues, not only be checked after a consumer wakes it.
    tokio::time::sleep(Duration::from_secs(31)).await;
    let result = stream.next().await;
    assert!(
        matches!(result, Some(Err(_))),
        "an expired response must not serve old commands"
    );
    assert!(stream.next().await.is_none());
    store.health().await.unwrap();
}
#[test]
fn registration_reply_uses_remaining_actual_lease_after_readiness_probe() {
    let session = super::super::store::NodeSession {
        node_id: "node-a".into(),
        session_id: "session-a".into(),
        grpc_addr: String::new(),
        data_addr: String::new(),
        rest_addr: String::new(),
        nfs_addr: String::new(),
        ownerfs_enabled: true,
        storage_devices: Vec::new(),
        lease_epoch: 7,
        expires_at_unix_ms: 60_000,
    };
    let readiness = BackendReadiness::observed(BackendPersistence::Persistent, true, "ready");
    let remaining = register_node_reply(session.clone(), readiness.clone(), 56_000);
    assert_eq!(remaining.lease_remaining_ms, 4_000);
    assert_eq!(remaining.expires_at_unix_ms, session.expires_at_unix_ms);
    assert_eq!(remaining.lease_epoch, session.lease_epoch);
    // A replay or a slow readiness probe must not fabricate a fresh lease.
    for now in [60_000, 90_000] {
        assert_eq!(
            register_node_reply(session.clone(), readiness.clone(), now).lease_remaining_ms,
            0
        );
    }
}

#[test]
fn chunk_sources_wire_preserves_bounded_grant_ranges() {
    use prost::Message;

    let chunk = crate::node::chunk::StagedChunk::new(
        crate::dfs::OperationId::new("source-range-codec"),
        b"data".to_vec(),
    )
    .chunk;
    let encoded = wire_chunk_sources_reply(crate::dfs::DfsChunkSourcesReply {
        revision: 9,
        chunks: vec![crate::dfs::ChunkSources {
            chunk_id: chunk.id.clone(),
            chunk: Some(chunk),
            sources: Vec::new(),
            allowed_ranges: vec![(1, 2)],
        }],
    });
    let decoded =
        afs_protocol::meta::GetDfsChunkSourcesReply::decode(encoded.encode_to_vec().as_slice())
            .unwrap();
    assert_eq!(decoded, encoded);
    assert_eq!(decoded.revision, 9);
    assert_eq!(decoded.chunks[0].allowed_ranges.len(), 1);
    assert_eq!(decoded.chunks[0].allowed_ranges[0].offset, 1);
    assert_eq!(decoded.chunks[0].allowed_ranges[0].length, 2);
}

#[test]
fn external_commit_boundary_requires_explicit_digest_and_safe_identity() {
    let valid = afs_protocol::meta::DfsExternalCommitReceipt {
        store_id: "filesystem".into(),
        generation: "g1".into(),
        object_key: "immutable-key".into(),
        persisted_bytes: 4,
        verified_digest: vec![7; 32],
        verified_digest_algorithm: afs_protocol::meta::DfsDigestAlgorithm::Blake3 as i32,
    };
    let decoded = domain_external_commit(valid.clone()).unwrap();
    assert_eq!(decoded.generation, "g1");
    let mut invalid_digest = valid.clone();
    invalid_digest.verified_digest.pop();
    assert!(domain_external_commit(invalid_digest).is_err());
    let mut invalid_algorithm = valid.clone();
    invalid_algorithm.verified_digest_algorithm = 0;
    assert!(domain_external_commit(invalid_algorithm).is_err());
    let mut invalid_identity = valid;
    invalid_identity.generation = "../other".into();
    assert!(domain_external_commit(invalid_identity).is_err());
}

fn presented_access() -> PresentedRootAccess {
    PresentedRootAccess {
        root_id: "workspace-a".into(),
        root_epoch: 1,
        home_node_id: "node-a".into(),
        holder_node_id: "node-b".into(),
        session_id: "session-b".into(),
        access_generation: 1,
        fencing_token: "fence:workspace-a:session-b:1".into(),
        home_session_id: "session-a".into(),
    }
}

#[test]
fn validate_root_access_binds_meta_tls_to_validator_not_presented_holder() {
    let presented = presented_access();
    validate_root_access_identities(Some("node-a"), "node-b", "node-a", "session-a", &presented)
        .expect("Home A should validate B's grant when P2P observed peer is B");
}

#[test]
fn authority_caller_must_match_authenticated_node() {
    validate_caller(Some("node-a"), "node-a").unwrap();
    assert_eq!(
        validate_caller(Some("node-a"), "node-b")
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
}

#[test]
fn n2b1_root_command_batch_compaction_maps_typed_recovery_cursor() {
    let reply = root_command_batch_reply_from_domain(
        super::super::owner_roots::RootCommandBatch::Compacted {
            requested_after: StoreRevision(10),
            compacted_to: StoreRevision(20),
            recovery: crate::meta::store::RecoveryCursor {
                resume_after: StoreRevision(19),
                reason: RecoveryReason::WatchCompacted,
            },
        },
    );
    let Some(afs_protocol::meta::root_command_batch_reply::Result::Compacted(compacted)) =
        reply.result
    else {
        panic!("expected compacted batch reply");
    };
    assert_eq!(compacted.requested_after, 10);
    assert_eq!(compacted.compacted_to, 20);
    let cursor = compacted.recovery_cursor.expect("recovery cursor");
    assert_eq!(cursor.resume_after, 19);
    assert_eq!(
        cursor.reason,
        RootCommandRecoveryReason::WatchCompacted as i32
    );
}

#[test]
fn n2b1_root_command_batch_unsupported_maps_typed_reply() {
    let reply = root_command_batch_reply_from_domain(
        super::super::owner_roots::RootCommandBatch::Unsupported {
            message: "batch polling unavailable".into(),
        },
    );
    let Some(afs_protocol::meta::root_command_batch_reply::Result::Unsupported(unsupported)) =
        reply.result
    else {
        panic!("expected unsupported batch reply");
    };
    assert_eq!(unsupported.message, "batch polling unavailable");
}

#[test]
fn validate_root_access_rejects_wrong_meta_tls_validator() {
    let presented = presented_access();
    let error = validate_root_access_identities(
        Some("node-b"),
        "node-b",
        "node-a",
        "session-a",
        &presented,
    )
    .unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied);
}

#[test]
fn validate_root_access_rejects_wrong_observed_peer() {
    let presented = presented_access();
    let error = validate_root_access_identities(
        Some("node-a"),
        "node-x",
        "node-a",
        "session-a",
        &presented,
    )
    .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
}
