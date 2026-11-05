use super::*;
use crate::node::vfs::dfs::DfsMeta;

/// The effective rule is not on the wire: a Node derives it while decoding.
#[test]
fn decoded_placement_snapshot_derives_effective_local_copy_from_declared() {
    for policy in [
        crate::dfs::LocalCopyPolicy::Required,
        crate::dfs::LocalCopyPolicy::Preferred,
        crate::dfs::LocalCopyPolicy::NotRequired,
    ] {
        let replication = crate::dfs::ReplicationConfig {
            local_copy: policy,
            ..crate::dfs::ReplicationConfig::local_single_copy()
        };
        let snapshot = domain_placement_snapshot(afs_protocol::meta::DfsPlacementSnapshot {
            revision: 7,
            replication: Some(wire_replication_config(replication.clone())),
            replica_groups: Vec::new(),
        })
        .unwrap();
        assert_eq!(snapshot.replication, replication);
        assert_eq!(
            snapshot.local_copy,
            crate::dfs::EffectiveLocalCopy::declared(&replication)
        );
        assert_eq!(snapshot.local_copy.effective_policy(), policy);
    }
}

#[test]
fn chunk_source_ranges_wire_roundtrip_and_legacy_fallback() {
    use prost::Message;

    let chunk = current_read_authorization().chunk.unwrap();
    let wire = afs_protocol::meta::DfsChunkSources {
        chunk_id: chunk.chunk_id.clone(),
        chunk: Some(chunk),
        sources: Vec::new(),
        allowed_ranges: vec![afs_protocol::meta::dfs_authorized_read::Range {
            offset: 1,
            length: 2,
        }],
    };
    let decoded =
        afs_protocol::meta::DfsChunkSources::decode(wire.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decoded, wire);
    assert_eq!(
        domain_dfs_chunk_sources(decoded).unwrap().allowed_ranges,
        vec![(1, 2)]
    );

    let mut legacy_wire = wire;
    legacy_wire.allowed_ranges.clear();
    let legacy =
        afs_protocol::meta::DfsChunkSources::decode(legacy_wire.encode_to_vec().as_slice())
            .unwrap();
    assert!(
        domain_dfs_chunk_sources(legacy)
            .unwrap()
            .allowed_ranges
            .is_empty()
    );
    let old_json = serde_json::json!({ "chunk_id": "legacy", "chunk": null, "sources": [] });
    let legacy: crate::dfs::ChunkSources = serde_json::from_value(old_json).unwrap();
    assert!(legacy.allowed_ranges.is_empty());
}

#[test]
fn chunk_source_ranges_wire_rejects_unbounded_or_invalid_hints() {
    let chunk = current_read_authorization().chunk.unwrap();
    let range = |offset, length| afs_protocol::meta::dfs_authorized_read::Range { offset, length };
    for allowed_ranges in [
        vec![range(0, 1); 129],
        vec![range(0, 0)],
        vec![range(u64::MAX, 1)],
        vec![range(3, 2)],
    ] {
        let error = domain_dfs_chunk_sources(afs_protocol::meta::DfsChunkSources {
            chunk_id: chunk.chunk_id.clone(),
            chunk: Some(chunk.clone()),
            sources: Vec::new(),
            allowed_ranges,
        })
        .unwrap_err();
        assert_eq!(error.code(), CLIENT_PROTOCOL_VIOLATION);
    }
    assert!(
        domain_dfs_chunk_sources(afs_protocol::meta::DfsChunkSources {
            chunk_id: chunk.chunk_id,
            chunk: None,
            sources: Vec::new(),
            allowed_ranges: vec![range(0, 1)],
        })
        .is_err()
    );
}

#[test]
fn external_authorization_wire_rejects_node_receiver_and_changed_object_key() {
    let chunk_id = crate::dfs::ChunkId::new(format!("blake3-{}-4", "a".repeat(64)));
    let key = crate::dfs::external_object_key("filesystem", "g1", &chunk_id).unwrap();
    let valid = afs_protocol::meta::DfsExternalAuthorizedRead {
        authorized: Some(afs_protocol::meta::DfsAuthorizedRead {
            open_sources: Vec::new(),

            validation: Some(afs_protocol::meta::DfsReadValidation {
                grant: Some(afs_protocol::meta::DfsReadGrant::default()),
                chunk_id: chunk_id.0.clone(),
                copy_id: "external-copy".into(),
                chunk_offset: 0,
                length: 4,
            }),
            allowed_ranges: vec![afs_protocol::meta::dfs_authorized_read::Range {
                offset: 0,
                length: 4,
            }],
            expires_at_unix_ms: 10,
        }),
        chunk: Some(afs_protocol::meta::DfsChunkObject {
            chunk_id: chunk_id.0,
            length: 4,
            content_digest: vec![0xaa; 32],
            content_digest_algorithm: afs_protocol::meta::DfsDigestAlgorithm::Blake3 as i32,
        }),
        location: Some(afs_protocol::meta::DfsCopyLocation {
            location: Some(afs_protocol::meta::dfs_copy_location::Location::External(
                afs_protocol::meta::dfs_copy_location::External {
                    store_id: "filesystem".into(),
                    object_key: key,
                    object_revision: "g1".into(),
                },
            )),
        }),
    };
    assert!(domain_external_authorized_read(valid.clone()).is_ok());
    let mut missing_identity = valid.clone();
    missing_identity.chunk = None;
    assert!(domain_external_authorized_read(missing_identity).is_err());
    let mut wrong_key = valid.clone();
    if let Some(afs_protocol::meta::dfs_copy_location::Location::External(external)) =
        wrong_key.location.as_mut().unwrap().location.as_mut()
    {
        external.object_key.push_str("-alias");
    }
    assert!(domain_external_authorized_read(wrong_key).is_err());
    let mut node_receiver = valid;
    node_receiver.location = Some(afs_protocol::meta::DfsCopyLocation {
        location: Some(afs_protocol::meta::dfs_copy_location::Location::Node(
            afs_protocol::meta::dfs_copy_location::Node::default(),
        )),
    });
    assert!(domain_external_authorized_read(node_receiver).is_err());
}

fn current_read_inode(head: &str) -> afs_protocol::meta::DfsInodeRecord {
    afs_protocol::meta::DfsInodeRecord {
        namespace_id: "default".into(),
        inode_id: "inode:test".into(),
        kind: afs_protocol::meta::DfsInodeKind::Regular as i32,
        attributes: Some(afs_protocol::meta::DfsInodeAttributes {
            mode: 0o640,
            uid: 1000,
            gid: 1000,
            nlink: 1,
            atime_unix_ms: 1,
            mtime_unix_ms: 1,
            ctime_unix_ms: 1,
        }),
        head_version_id: head.into(),
        revision: 1,
        symlink_target: Vec::new(),
        xattrs: Vec::new(),
        special_node: None,
    }
}

fn current_read_authorization() -> afs_protocol::meta::DfsExternalAuthorizedRead {
    let chunk_id = crate::dfs::ChunkId::new(format!("blake3-{}-4", "b".repeat(64)));
    let key = crate::dfs::external_object_key("filesystem", "g1", &chunk_id).unwrap();
    afs_protocol::meta::DfsExternalAuthorizedRead {
        authorized: Some(afs_protocol::meta::DfsAuthorizedRead {
            open_sources: Vec::new(),

            validation: Some(afs_protocol::meta::DfsReadValidation {
                grant: Some(afs_protocol::meta::DfsReadGrant::default()),
                chunk_id: chunk_id.0.clone(),
                copy_id: "external-copy".into(),
                chunk_offset: 0,
                length: 4,
            }),
            allowed_ranges: vec![afs_protocol::meta::dfs_authorized_read::Range {
                offset: 0,
                length: 4,
            }],
            expires_at_unix_ms: 10,
        }),
        chunk: Some(afs_protocol::meta::DfsChunkObject {
            chunk_id: chunk_id.0,
            length: 4,
            content_digest: vec![0xbb; 32],
            content_digest_algorithm: afs_protocol::meta::DfsDigestAlgorithm::Blake3 as i32,
        }),
        location: Some(afs_protocol::meta::DfsCopyLocation {
            location: Some(afs_protocol::meta::dfs_copy_location::Location::External(
                afs_protocol::meta::dfs_copy_location::External {
                    store_id: "filesystem".into(),
                    object_key: key,
                    object_revision: "g1".into(),
                },
            )),
        }),
    }
}

#[test]
fn current_external_reply_rejects_head_changed_authorizations() {
    let reply = afs_protocol::meta::AuthorizeCurrentExternalReadReply {
        inode: Some(current_read_inode("next")),
        head_changed: true,
        authorizations: vec![current_read_authorization()],
    };
    assert_eq!(
        domain_authorize_current_external_read_reply(reply)
            .unwrap_err()
            .code(),
        CLIENT_PROTOCOL_VIOLATION
    );
}

#[test]
fn current_external_reply_requires_inode() {
    let reply = afs_protocol::meta::AuthorizeCurrentExternalReadReply {
        inode: None,
        head_changed: false,
        authorizations: Vec::new(),
    };
    assert_eq!(
        domain_authorize_current_external_read_reply(reply)
            .unwrap_err()
            .code(),
        CLIENT_PROTOCOL_VIOLATION
    );
}

#[test]
fn current_external_reply_decodes_authorized_and_head_changed_shapes() {
    let authorized = afs_protocol::meta::AuthorizeCurrentExternalReadReply {
        inode: Some(current_read_inode("version")),
        head_changed: false,
        authorizations: vec![current_read_authorization()],
    };
    assert!(matches!(
        domain_authorize_current_external_read_reply(authorized).unwrap(),
        crate::dfs::AuthorizeCurrentExternalReadReply::Authorized { authorizations, .. }
            if authorizations.len() == 1
    ));

    let head_changed = afs_protocol::meta::AuthorizeCurrentExternalReadReply {
        inode: Some(current_read_inode("next")),
        head_changed: true,
        authorizations: Vec::new(),
    };
    assert!(matches!(
        domain_authorize_current_external_read_reply(head_changed).unwrap(),
        crate::dfs::AuthorizeCurrentExternalReadReply::HeadChanged(inode)
            if inode.head_version == Some(crate::dfs::FileVersionId::new("next"))
    ));
}

#[test]
fn external_version_reply_requires_bounded_positive_validity() {
    let valid = afs_protocol::meta::AuthorizeExternalVersionReadReply {
        authorizations: vec![current_read_authorization()],
        remaining_validity_ms: 5_000,
    };
    assert!(matches!(
        domain_authorize_external_version_read_reply(valid).unwrap(),
        crate::dfs::AuthorizeExternalVersionReadReply {
            authorizations,
            remaining_validity_ms: 5_000
        } if authorizations.len() == 1
    ));
    for remaining_validity_ms in [0, 5_001] {
        let reply = afs_protocol::meta::AuthorizeExternalVersionReadReply {
            authorizations: vec![current_read_authorization()],
            remaining_validity_ms,
        };
        assert_eq!(
            domain_authorize_external_version_read_reply(reply)
                .unwrap_err()
                .code(),
            CLIENT_PROTOCOL_VIOLATION
        );
    }
}

#[test]
fn current_external_unsupported_filter_is_exact_tonic_unimplemented() {
    assert!(current_external_read_is_unimplemented(
        &tonic::Status::unimplemented("old meta")
    ));
    assert!(!current_external_read_is_unimplemented(
        &tonic::Status::permission_denied("no")
    ));
    assert!(!current_external_read_is_unimplemented(
        &tonic::Status::deadline_exceeded("slow")
    ));
}

use std::time::Instant;
use tokio::net::TcpListener;

async fn hanging_endpoint() -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut sockets = Vec::new();
        while let Ok((socket, _peer)) = listener.accept().await {
            sockets.push(socket);
        }
    });
    (format!("http://{addr}"), task)
}

fn test_meta(endpoint: &str, timeout: Duration) -> GrpcDfsMeta {
    GrpcDfsMeta::new(
        endpoint,
        "node-a".into(),
        "session-a".into(),
        crate::dfs::NamespaceId::new("default"),
        timeout,
        TlsConfig::Disabled,
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_meta_call_on_an_async_worker_is_refused_and_the_async_method_runs() {
    let (endpoint, server) = hanging_endpoint().await;
    let meta = test_meta(&endpoint, Duration::from_millis(30));
    let refused = DfsMeta::current_node_session(&meta, "node-b").unwrap_err();
    assert_eq!(refused.code(), afs_error::CLIENT_WORKER_FAILED);
    assert!(refused.to_string().contains("Tokio runtime"), "{refused}");
    let refused =
        DfsMeta::resolve_write_authority(&meta, &crate::dfs::InodeId::new("i")).unwrap_err();
    assert_eq!(refused.code(), afs_error::CLIENT_WORKER_FAILED);

    let session = meta.current_node_session_async("node-b".into()).await;
    assert_eq!(
        session.unwrap_err().code(),
        afs_error::CLIENT_DEADLINE_EXCEEDED
    );
    let authority = meta
        .resolve_write_authority_async(crate::dfs::InodeId::new("i"))
        .await;
    assert_eq!(
        authority.unwrap_err().code(),
        afs_error::CLIENT_DEADLINE_EXCEEDED
    );
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_rpc_metrics_count_real_adapter_invocations_even_on_failure() {
    let (endpoint, server) = hanging_endpoint().await;
    let registry = afs_metrics::Registry::new();
    let metrics = crate::node::read_work::ReadWorkMetrics::register(&registry).unwrap();
    let meta = test_meta(&endpoint, Duration::from_millis(30)).with_read_work_metrics(metrics);
    tokio::task::spawn_blocking(move || {
        use crate::node::{read::engine::ReadSourceProvider, vfs::dfs::DfsMeta};
        assert!(
            meta.get_inode(&crate::dfs::InodeId::new("missing"))
                .is_err()
        );
        assert!(
            meta.get_file_version(&crate::dfs::FileVersionId::new("missing"))
                .is_err()
        );
        assert!(
            meta.sources_for(crate::dfs::DfsChunkSourcesRequest {
                caller_id: "node-a".into(),
                namespace_id: crate::dfs::NamespaceId::new("default"),
                file_version_id: crate::dfs::FileVersionId::new("missing"),
                layout_root_id: crate::dfs::LayoutRootId::new("missing"),
                chunk_ids: Vec::new(),
            })
            .is_err()
        );
        assert!(
            meta.validate_read_grants(crate::dfs::ValidateDfsReadGrants {
                receiver_node_id: "node-a".into(),
                receiver_node_epoch: 1,
                peer_node_id: "node-b".into(),
                validations: Vec::new(),
            })
            .is_err()
        );
    })
    .await
    .unwrap();
    server.abort();
    let families = registry.gather();
    let calls = families
        .iter()
        .find(|family| family.name() == "afs_dfs_read_rpc_calls_total")
        .unwrap();
    assert_eq!(calls.get_metric().len(), 5);
    for metric in calls.get_metric() {
        let method = metric
            .get_label()
            .iter()
            .find(|label| label.name() == "method")
            .unwrap()
            .value();
        let expected = if method == "peer_read_ranges" {
            0.0
        } else {
            1.0
        };
        assert_eq!(metric.get_counter().get_value(), expected, "{method}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_read_diagnostics_cover_full_failed_meta_calls() {
    use crate::node::read_work::{ReadDiagnosticStage, ReadWorkMetrics};
    let (endpoint, server) = hanging_endpoint().await;
    let registry = afs_metrics::Registry::new();
    let metrics = ReadWorkMetrics::register(&registry)
        .unwrap()
        .with_read_diagnostics(true);
    let meta =
        test_meta(&endpoint, Duration::from_millis(30)).with_read_work_metrics(metrics.clone());
    tokio::task::spawn_blocking(move || {
        use crate::node::{read::engine::ReadSourceProvider, vfs::dfs::DfsMeta};
        assert_eq!(
            meta.get_inode(&crate::dfs::InodeId::new("missing"))
                .unwrap_err()
                .code(),
            afs_error::CLIENT_DEADLINE_EXCEEDED
        );
        assert_eq!(
            meta.get_file_version(&crate::dfs::FileVersionId::new("missing"))
                .unwrap_err()
                .code(),
            afs_error::CLIENT_DEADLINE_EXCEEDED
        );
        assert!(
            meta.sources_for(crate::dfs::DfsChunkSourcesRequest {
                caller_id: "node-a".into(),
                namespace_id: crate::dfs::NamespaceId::new("default"),
                file_version_id: crate::dfs::FileVersionId::new("missing"),
                layout_root_id: crate::dfs::LayoutRootId::new("missing"),
                chunk_ids: Vec::new(),
            })
            .is_err()
        );
        assert!(
            meta.validate_read_grants(crate::dfs::ValidateDfsReadGrants {
                receiver_node_id: "node-a".into(),
                receiver_node_epoch: 1,
                peer_node_id: "node-b".into(),
                validations: Vec::new(),
            })
            .is_err()
        );
        assert!(
            meta.lookup(&crate::dfs::InodeId::new("root"), b"missing")
                .is_err()
        );
        assert!(meta.lookup_node_location("missing").is_err());
    })
    .await
    .unwrap();
    server.abort();
    let snapshot = metrics.diagnostics_snapshot().unwrap();
    let expected = [
        ReadDiagnosticStage::MetaGetInode,
        ReadDiagnosticStage::MetaGetFileVersion,
        ReadDiagnosticStage::MetaGetChunkSources,
        ReadDiagnosticStage::MetaValidateReadGrants,
        ReadDiagnosticStage::MetaLookup,
        ReadDiagnosticStage::MetaLookupNodeLocation,
    ];
    assert_eq!(snapshot.records.len(), expected.len());
    for (record, stage) in snapshot.records.iter().zip(expected) {
        assert_eq!(record.stage, stage);
        assert!(record.completed && !record.success);
        assert!(record.end_ns > record.start_ns);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_write_diagnostics_cover_controls_and_keep_cached_placement_local() {
    use crate::node::read_work::{ReadDiagnosticStage, ReadWorkMetrics};
    let (endpoint, server) = hanging_endpoint().await;
    for enabled in [false, true] {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry)
            .unwrap()
            .with_read_diagnostics(enabled);
        let meta =
            test_meta(&endpoint, Duration::from_millis(30)).with_read_work_metrics(metrics.clone());
        tokio::task::spawn_blocking(move || {
            use crate::node::{replication::PlacementProvider, vfs::dfs::DfsMeta};
            let cached = Arc::new(crate::dfs::PlacementSnapshot {
                revision: 9,
                local_copy: crate::dfs::EffectiveLocalCopy::declared(
                    &crate::dfs::ReplicationConfig::local_single_copy(),
                ),
                replication: crate::dfs::ReplicationConfig::local_single_copy(),
                replica_groups: Vec::new(),
            });
            *meta.placement.lock().unwrap() = Some(cached.clone());
            assert!(Arc::ptr_eq(&meta.snapshot().unwrap(), &cached));
            if let Some(snapshot) = meta.read_work.as_ref().unwrap().diagnostics_snapshot() {
                assert!(
                    snapshot.records.is_empty(),
                    "cached placement is local work"
                );
            }
            // Refresh really invokes the transport; its failure leaves the
            // previously cached placement usable without another RPC.
            assert!(meta.refresh(10).is_err());
            assert!(Arc::ptr_eq(&meta.snapshot().unwrap(), &cached));
            assert!(
                meta.create(
                    &crate::dfs::OperationId::new("diagnostic-create"),
                    &crate::dfs::InodeId::new("root"),
                    b"missing",
                    crate::dfs::InodeAttributes {
                        mode: 0o644,
                        uid: 1,
                        gid: 1,
                        nlink: 1,
                        atime_unix_ms: 0,
                        mtime_unix_ms: 0,
                        ctime_unix_ms: 0,
                    },
                )
                .is_err()
            );
            let inode = crate::dfs::InodeId::new("missing");
            assert!(meta.open_write(&inode).is_err());
            assert!(meta.resolve_write_authority(&inode).is_err());
            assert!(meta.renew_write_lease(test_lease()).is_err());
            assert!(meta.sync_inode_metadata(test_metadata_sync()).is_err());
        })
        .await
        .unwrap();
        match metrics.diagnostics_snapshot() {
            None => assert!(!enabled),
            Some(snapshot) => {
                let expected = [
                    (
                        ReadDiagnosticStage::MetaPlacementSnapshot,
                        "meta_placement_rpc",
                    ),
                    (ReadDiagnosticStage::MetaCreate, "meta_create_rpc"),
                    (ReadDiagnosticStage::MetaOpenWrite, "meta_open_write_rpc"),
                    (
                        ReadDiagnosticStage::MetaResolveWriteAuthority,
                        "meta_resolve_write_rpc",
                    ),
                    (
                        ReadDiagnosticStage::MetaRenewWriteLease,
                        "meta_renew_write_lease_rpc",
                    ),
                    (
                        ReadDiagnosticStage::MetaSyncInodeMetadata,
                        "meta_sync_inode_metadata_rpc",
                    ),
                ];
                assert_eq!(snapshot.records.len(), expected.len());
                assert_eq!(snapshot.overflow, 0);
                for (record, (stage, phase)) in snapshot.records.iter().zip(expected) {
                    assert_eq!(record.stage, stage);
                    assert!(record.completed && !record.success);
                    assert!(record.end_ns >= record.start_ns);
                    let aggregate = snapshot
                        .coarse_phases
                        .iter()
                        .find(|entry| entry.phase == phase)
                        .unwrap();
                    assert_eq!(aggregate.calls, 1, "{phase}");
                    assert!(aggregate.last_end_ns >= aggregate.first_start_ns);
                }
            }
        }
    }
    server.abort();
}

fn test_lease() -> crate::dfs::WriteLease {
    crate::dfs::WriteLease {
        inode_id: crate::dfs::InodeId::new("inode:test"),
        owner_node_id: "node-a".into(),
        owner_session_id: "session-a".into(),
        lease_epoch: 1,
        expires_at_unix_ms: u64::MAX,
    }
}

fn test_metadata_sync() -> crate::dfs::SyncInodeMetadata {
    crate::dfs::SyncInodeMetadata {
        operation_id: crate::dfs::OperationId::new("op:sync"),
        inode_id: crate::dfs::InodeId::new("inode:test"),
        write_lease: test_lease(),
        expected_inode_revision: 1,
        expected_head_version: None,
        metadata_delta: crate::dfs::CommitMetadataDelta {
            mode: crate::dfs::CommitMetadataMode::Full,
            mtime_unix_ms: Some(1),
            ctime_unix_ms: Some(1),
            kill_suidgid: false,
        },
    }
}

fn test_commit() -> crate::dfs::CommitFileVersion {
    let layout_id = crate::dfs::LayoutRootId::new("layout:test");
    crate::dfs::CommitFileVersion {
        gc_fence: None,
        operation_id: crate::dfs::OperationId::new("op:commit"),
        inode_id: crate::dfs::InodeId::new("inode:test"),
        write_lease: test_lease(),
        expected_inode_revision: 1,
        expected_head_version: None,
        file_version: crate::dfs::FileVersion {
            id: crate::dfs::FileVersionId::new("version:test"),
            inode_id: crate::dfs::InodeId::new("inode:test"),
            parent_version: None,
            length: 0,
            layout_root: layout_id.clone(),
            created_at_unix_ms: 1,
        },
        layout_root: crate::dfs::LayoutRoot {
            id: layout_id,
            file_length: 0,
            inline_extents: Vec::new(),
        },
        chunk_receipts: Vec::new(),
        clone_source: None,
        metadata_delta: crate::dfs::CommitMetadataDelta {
            mode: crate::dfs::CommitMetadataMode::Full,
            mtime_unix_ms: Some(1),
            ctime_unix_ms: Some(1),
            kill_suidgid: false,
        },
    }
}

fn test_digest(byte: u8) -> crate::dfs::ContentDigest {
    crate::dfs::ContentDigest {
        algorithm: crate::dfs::DigestAlgorithm::Blake3,
        bytes: [byte; 32],
    }
}

fn test_replica_target(node: &str, index: u64) -> crate::dfs::ReplicaTarget {
    crate::dfs::ReplicaTarget {
        node_id: node.into(),
        node_epoch: index + 10,
        data_endpoint: format!("https://{node}:19000"),
        device: crate::dfs::StorageDeviceDescriptor {
            device_id: format!("device-{node}"),
            device_epoch: index + 20,
            catalog_revision: index + 30,
            failure_domain: format!("rack-{index}"),
        },
    }
}

fn test_replication_claim() -> crate::dfs::ReplicationClaim {
    crate::dfs::ReplicationClaim {
        task_id: crate::dfs::ReplicationTaskId::new("task:claim"),
        operation_id: crate::dfs::OperationId::new("op:claim"),
        worker_node_id: "node-a".into(),
        worker_node_epoch: 11,
        worker_session_id: "session-a".into(),
        expires_at_unix_ms: 123_456,
        fence: 9,
        chunk: crate::dfs::ChunkObject {
            id: crate::dfs::ChunkId::new("chunk:claim"),
            length: 4096,
            content_digest: test_digest(7),
            encoding: crate::dfs::ChunkEncoding::Raw,
        },
        source_copy_id: crate::dfs::CopyId::new("copy:source"),
        placement_revision: 77,
        replica_group: crate::dfs::ReplicaGroup {
            id: crate::dfs::ReplicaGroupId::new("group:claim"),
            placement_epoch: 88,
            targets: vec![
                test_replica_target("node-a", 1),
                test_replica_target("node-b", 2),
            ],
        },
        replication: crate::dfs::ReplicationConfig {
            desired_copies: 2,
            sync_required_copies: 2,
            min_distinct_nodes: 2,
            min_distinct_failure_domains: 1,
            local_copy: crate::dfs::LocalCopyPolicy::Preferred,
        },
    }
}

#[test]
fn replication_claim_wire_round_trip_preserves_authority_identity() {
    let claim = test_replication_claim();
    let wire = wire_replication_claim(&claim);

    assert_eq!(wire.task_id, "task:claim");
    assert_eq!(wire.operation_id, "op:claim");
    assert_eq!(wire.worker_node_id, "node-a");
    assert_eq!(wire.worker_session_id, "session-a");
    assert_eq!(wire.source_copy_id, "copy:source");

    let decoded = domain_replication_claim(wire).unwrap();
    assert_eq!(decoded, claim);
}

#[test]
fn replication_claim_rejects_invalid_chunk_digest_length() {
    let claim = test_replication_claim();
    let mut wire = wire_replication_claim(&claim);
    wire.chunk.as_mut().unwrap().content_digest = vec![1, 2, 3];

    let error = domain_replication_claim(wire).expect_err("invalid digest should fail");
    assert_eq!(error.code(), afs_error::CLIENT_PROTOCOL_VIOLATION);
}

#[test]
fn replication_task_rejects_unspecified_state() {
    let task = afs_protocol::meta::DfsReplicationTask {
        task_id: "task:bad".into(),
        chunk_id: "chunk:bad".into(),
        placement_epoch: 1,
        desired_copies: 1,
        existing_copy_ids: Vec::new(),
        state: afs_protocol::meta::DfsReplicationTaskState::Unspecified as i32,
        attempt: 0,
        next_retry_unix_ms: 0,
        last_error: None,
        claim: None,
    };

    let error = domain_replication_task(task).expect_err("unspecified state should fail");
    assert_eq!(error.code(), afs_error::CLIENT_PROTOCOL_VIOLATION);
}

#[test]
fn dfs_meta_typed_tonic_timeout_is_client_deadline() {
    let mut status = tonic::Status::cancelled("Timeout expired");
    status.set_source(std::sync::Arc::new(tonic::TimeoutExpired(())));

    let error = GrpcDfsMeta::dfs_meta_status_to_error(status);

    assert_eq!(error.code(), afs_error::CLIENT_DEADLINE_EXCEEDED);
    assert_eq!(error.kind(), afs_error::ErrorKind::DeadlineExceeded);
}

#[test]
fn dfs_meta_plain_cancelled_remains_remote_cancelled() {
    let error = GrpcDfsMeta::dfs_meta_status_to_error(tonic::Status::cancelled("remote cancelled"));

    assert_eq!(error.code(), afs_error::CLIENT_REMOTE_STATUS);
    assert_eq!(error.kind(), afs_error::ErrorKind::Cancelled);
    assert_eq!(error.message(), "remote cancelled");
}

#[test]
fn dfs_meta_rich_business_error_identity_is_unchanged() {
    let original = Error::coded(afs_error::CLIENT_ARGUMENT_INVALID, "invalid argument");
    let status = afs_transport::grpc::error_status::error_to_status(original.clone());

    let error = GrpcDfsMeta::dfs_meta_status_to_error(status);

    assert_eq!(error, original);
}

async fn assert_deadline_elapsed(
    label: &'static str,
    operation: impl FnOnce() -> afs_error::Result<()> + Send + 'static,
) {
    let started = Instant::now();
    let result = tokio::task::spawn_blocking(operation).await.unwrap();
    let elapsed = started.elapsed();
    let error = result.expect_err(label);
    assert_eq!(
        error.code(),
        afs_error::CLIENT_DEADLINE_EXCEEDED,
        "{label}: unexpected error {error:?}"
    );
    assert!(
        elapsed < Duration::from_millis(300),
        "{label} should honor the focused timeout, elapsed={elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_lock_resolver_honors_configured_deadline() {
    let (endpoint, listener) = hanging_endpoint().await;
    let meta = test_meta(&endpoint, Duration::from_millis(40));
    assert_deadline_elapsed("lock resolver configured deadline", move || {
        DfsMeta::resolve_lock_authority(&meta, &crate::dfs::InodeId::new("inode:test")).map(|_| ())
    })
    .await;
    listener.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_dfs_meta_focused_timeouts_bound_hanging_rpc() {
    let (endpoint, listener) = hanging_endpoint().await;
    let configured_long = Duration::from_secs(1);
    let focused = Duration::from_millis(40);

    let meta = std::sync::Arc::new(test_meta(&endpoint, configured_long));
    use crate::node::call_deadline::within_timeout as within;
    let lease_meta = meta.clone();
    assert_deadline_elapsed("renew lease timeout", move || {
        within(focused, || {
            DfsMeta::renew_write_lease(lease_meta.as_ref(), test_lease()).map(|_| ())
        })
    })
    .await;

    let sync_meta = meta.clone();
    assert_deadline_elapsed("metadata sync timeout", move || {
        within(focused, || {
            DfsMeta::sync_inode_metadata(sync_meta.as_ref(), test_metadata_sync()).map(|_| ())
        })
    })
    .await;

    let commit_meta = meta.clone();
    assert_deadline_elapsed("file commit timeout", move || {
        within(focused, || {
            DfsMeta::commit_file_version(commit_meta.as_ref(), test_commit()).map(|_| ())
        })
    })
    .await;

    let session_meta = meta.clone();
    assert_deadline_elapsed("node session timeout", move || {
        within(focused, || {
            DfsMeta::current_node_session(session_meta.as_ref(), "node-a").map(|_| ())
        })
    })
    .await;

    let configured_short = std::sync::Arc::new(test_meta(&endpoint, focused));
    assert_deadline_elapsed("configured timeout remains a cap", move || {
        within(configured_long, || {
            DfsMeta::renew_write_lease(configured_short.as_ref(), test_lease()).map(|_| ())
        })
    })
    .await;

    listener.abort();
}
