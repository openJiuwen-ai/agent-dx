use afs::meta::store::{
    journal::{LogLimits, LogTip, RecoveredLog},
    log::Record,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use afs::{
    meta::{
        Meta,
        owner_roots::{ActivateRootInput, ReserveRootInput, WireRootReservation},
        rest,
        store::{
            MetaFuture, MetaLog, NodeSessionLease, RequestKey, RootRight as StoreRootRight, Store,
            journal::MemoryLog,
        },
    },
    runtime::Observability,
};
use axum::{
    body::to_bytes,
    http::{Request, StatusCode},
};
use serde_json::Value;
use tower::ServiceExt;

struct SwitchableBackend {
    inner: MemoryLog,
    reject_load: AtomicBool,
}

impl SwitchableBackend {
    fn reject_load(&self) {
        self.reject_load.store(true, Ordering::Release);
    }
}

impl MetaLog for SwitchableBackend {
    fn persistence(&self) -> afs::meta::store::BackendPersistence {
        self.inner.persistence()
    }
    fn limits(&self) -> LogLimits {
        self.inner.limits()
    }
    fn recover(&self) -> MetaFuture<'_, RecoveredLog> {
        self.inner.recover()
    }
    fn append_and_sync(&self, expected: LogTip, records: Vec<Record>) -> MetaFuture<'_, LogTip> {
        self.inner.append_and_sync(expected, records)
    }
    fn health(&self) -> MetaFuture<'_, ()> {
        Box::pin(async move {
            if self.reject_load.load(Ordering::Acquire) {
                Err(afs_error::Error::coded(
                    afs_error::IO_UNAVAILABLE,
                    "injected backend read failure",
                ))
            } else {
                self.inner.health().await
            }
        })
    }
}

struct HangingBackend {
    inner: MemoryLog,
    loads: AtomicUsize,
    notified: tokio::sync::Notify,
}

impl MetaLog for HangingBackend {
    fn limits(&self) -> LogLimits {
        self.inner.limits()
    }
    fn recover(&self) -> MetaFuture<'_, RecoveredLog> {
        self.inner.recover()
    }
    fn append_and_sync(&self, expected: LogTip, records: Vec<Record>) -> MetaFuture<'_, LogTip> {
        self.inner.append_and_sync(expected, records)
    }
    fn persistence(&self) -> afs::meta::store::BackendPersistence {
        self.inner.persistence()
    }
    fn health(&self) -> MetaFuture<'_, ()> {
        Box::pin(async move {
            self.loads.fetch_add(1, Ordering::AcqRel);
            self.notified.notified().await;
            self.inner.health().await
        })
    }
}

async fn meta_with_memory_store() -> (Arc<Meta>, Arc<Store>) {
    let store = Arc::new(
        Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
            .await
            .unwrap(),
    );
    let meta = Arc::new(Meta::with_store(
        "meta-rest-test".into(),
        Observability::new().unwrap(),
        store.clone(),
    ));
    (meta, store)
}

async fn register_node_with_ttl(meta: &Meta, session_id: &str, ttl: Duration) {
    meta.register_node(
        RequestKey::new(
            format!("node-a/{session_id}"),
            format!("register-{session_id}"),
        ),
        NodeSessionLease {
            node_id: "node-a".into(),
            session_id: session_id.into(),
            grpc_addr: "http://node-a:7400".into(),
            data_addr: "http://node-a:7500".into(),
            rest_addr: "http://node-a:7600".into(),
            storage_devices: Vec::new(),
            lease_ttl: ttl,
        },
    )
    .await
    .unwrap();
}

async fn activate_root(meta: &Meta, root_id: &str, session_id: &str) {
    let reserved = meta
        .owner_roots
        .reserve_root(ReserveRootInput {
            request_id: format!("reserve-{root_id}"),
            root_id: root_id.into(),
            preferred_home_node_id: "node-a".into(),
            session_id: session_id.into(),
            rights: vec![StoreRootRight::Lookup, StoreRootRight::Write],
            expected_root_epoch: 1,
            create_intent_id: format!("mkdir-{root_id}"),
        })
        .await
        .unwrap()
        .reservation;
    meta.owner_roots
        .activate_root(ActivateRootInput {
            request_id: format!("activate-{root_id}"),
            reservation: WireRootReservation {
                root_id: reserved.root_id,
                root_epoch: reserved.root_epoch,
                home_node_id: reserved.home_node_id,
                session_id: reserved.home_session_id,
                create_intent_id: reserved.create_intent_id,
                prepare_token: reserved.prepare_token,
            },
            local_prepare_id: format!("local-{root_id}"),
        })
        .await
        .unwrap();
}

async fn get_json(meta: Arc<Meta>, path: &str) -> (StatusCode, Value) {
    let response = rest::router(meta)
        .oneshot(
            Request::builder()
                .uri(path)
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
    let json = serde_json::from_slice(&body).unwrap();
    (status, json)
}

#[tokio::test]
async fn health_requires_a_meta_store() {
    let meta = Arc::new(Meta::new(
        "meta-rest-test".into(),
        Observability::new().unwrap(),
    ));

    let (status, body) = get_json(meta, "/health").await;

    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(body["error"]["kind"], "Unimplemented");
}

#[tokio::test]
async fn root_location_reports_serving_for_live_exact_home_session() {
    let (meta, _store) = meta_with_memory_store().await;
    register_node_with_ttl(&meta, "session-live", Duration::from_secs(30)).await;
    activate_root(&meta, "workspace-live", "session-live").await;

    let (status, body) = get_json(meta, "/v1/roots/workspace-live").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["root_id"], "workspace-live");
    assert_eq!(body["status"], "serving");
    assert_eq!(body["home_serving"], true);
    assert_eq!(body["home_node_id"], "node-a");
    assert_eq!(body["home_session_id"], "session-live");
    assert_eq!(body["home_grpc_addr"], "http://node-a:7400");
}

#[tokio::test]
async fn root_location_keeps_owner_but_reports_unavailable_for_expired_home_session() {
    let (meta, _store) = meta_with_memory_store().await;
    register_node_with_ttl(&meta, "session-expired", Duration::from_millis(50)).await;
    activate_root(&meta, "workspace-expired", "session-expired").await;
    tokio::time::sleep(Duration::from_millis(80)).await;

    let (status, body) = get_json(meta, "/v1/roots/workspace-expired").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["root_id"], "workspace-expired");
    assert_eq!(body["status"], "unavailable");
    assert_eq!(body["home_serving"], false);
    assert_eq!(body["owner"], "node-a");
    assert_eq!(body["home_node_id"], "node-a");
    assert_eq!(body["home_session_id"], "session-expired");
    assert_eq!(body["home_grpc_addr"], "http://node-a:7400");
}

#[tokio::test]
async fn health_uses_backend_probe_not_cached_read_view() {
    let backend = Arc::new(SwitchableBackend::default());
    let store = Arc::new(Store::open_log(backend.clone()).await.unwrap());
    let meta = Arc::new(Meta::with_store(
        "meta-rest-test".into(),
        Observability::new().unwrap(),
        store,
    ));
    register_node_with_ttl(&meta, "session-live", Duration::from_secs(30)).await;
    activate_root(&meta, "workspace-live", "session-live").await;

    backend.reject_load();

    let (health_status, health_body) = get_json(meta.clone(), "/health").await;
    assert_eq!(health_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(health_body["error"]["kind"], "Unavailable");

    let (root_status, root_body) = get_json(meta.clone(), "/v1/roots/workspace-live").await;
    assert_eq!(root_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(root_body["error"]["kind"], "Unavailable");

    let (replication_status, replication_body) =
        get_json(meta, "/v1/dfs/chunks/unknown/replication").await;
    assert_eq!(replication_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(replication_body["error"]["kind"], "Unavailable");
}

#[tokio::test]
async fn health_backend_probe_is_bounded_when_backend_stalls() {
    let store = Arc::new(
        Store::open_log(Arc::new(HangingBackend::default()))
            .await
            .unwrap(),
    );
    let meta = Arc::new(Meta::with_store(
        "meta-rest-test".into(),
        Observability::new().unwrap(),
        store,
    ));

    let (health_status, health_body) =
        tokio::time::timeout(Duration::from_secs(3), get_json(meta, "/health"))
            .await
            .expect("health probe must complete within the REST readiness deadline");

    assert_eq!(health_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(health_body["error"]["kind"], "Unavailable");
    assert!(
        health_body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("backend health probe timed out")
    );
}

#[tokio::test]
async fn chunk_replication_reports_observed_availability_without_claiming_data_loss() {
    use afs::dfs::*;
    use afs::meta::store::{
        MetaEntity, MetaStore, MetaTxn, OperationResult, RequestOutcome, StoreOperation,
        TxnCondition, TxnMutation,
    };
    let (meta, store) = meta_with_memory_store().await;
    let device = StorageDeviceDescriptor {
        device_id: "disk-a".into(),
        device_epoch: 1,
        catalog_revision: 1,
        failure_domain: "node-a".into(),
    };
    let register = |session: &str, descriptor: StorageDeviceDescriptor| NodeSessionLease {
        node_id: "node-a".into(),
        session_id: session.into(),
        grpc_addr: "http://node-a:7400".into(),
        data_addr: "http://node-a:7500".into(),
        rest_addr: "http://node-a:7600".into(),
        storage_devices: vec![descriptor],
        lease_ttl: Duration::from_secs(30),
    };
    store
        .register_node_session(
            RequestKey::new("node-a", "rest-register-1"),
            register("source-1", device.clone()),
        )
        .await
        .unwrap();
    let id = ChunkId::new("rest-chunk");
    let copy_id = CopyId::new("rest-copy");
    let copy = CopyRecord {
        id: copy_id.clone(),
        chunk_id: id.clone(),
        role: CopyRole::DurableReplica,
        state: CopyState::Ready,
        location: CopyLocation::Node {
            node_id: "node-a".into(),
            node_epoch: 1,
            device_id: "disk-a".into(),
            device_epoch: 1,
            catalog_revision: 1,
        },
        persisted_bytes: 4,
        verified_digest: ContentDigest {
            algorithm: DigestAlgorithm::Blake3,
            bytes: [7; 32],
        },
    };
    let placement = PlacementRecord {
        chunk_id: id.clone(),
        replica_group_id: ReplicaGroupId::new("rest-group"),
        placement_epoch: 1,
        desired_copies: 2,
        copies: vec![copy_id.clone()],
        health: PlacementHealth::UnderReplicated,
    };
    let task = ReplicationTask {
        id: ReplicationTaskId::new("rest-task"),
        chunk_id: id.clone(),
        placement_epoch: 1,
        desired_copies: 2,
        existing_copies: vec![copy_id],
        state: ReplicationTaskState::Pending,
        attempt: 0,
        next_retry_unix_ms: 0,
        last_error: None,
        claim: None,
    };
    let request = RequestKey::new("fixture", "rest-replication");
    let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsCommitFileVersion);
    txn.conditions
        .push(TxnCondition::RequestAbsent(request.clone()));
    txn.mutations.extend([
        TxnMutation::Put(MetaEntity::DfsChunk(ChunkObject {
            id: id.clone(),
            length: 4,
            content_digest: copy.verified_digest.clone(),
            encoding: ChunkEncoding::Raw,
        })),
        TxnMutation::Put(MetaEntity::DfsCopy(copy)),
        TxnMutation::Put(MetaEntity::DfsPlacement(placement)),
        TxnMutation::Put(MetaEntity::DfsReplicationTask(task)),
        TxnMutation::RecordRequestOutcome(RequestOutcome {
            request,
            operation: StoreOperation::DfsCommitFileVersion,
            result: OperationResult::Empty,
        }),
    ]);
    store.compare_and_commit(txn).await.unwrap();
    let path = "/v1/dfs/chunks/rest-chunk/replication";
    let (status, body) = get_json(meta.clone(), path).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["health"], "UnderReplicated");
    assert_eq!(body["available_copies"], 1);
    assert_eq!(body["tasks"][0]["state"], "Pending");
    assert_eq!(body["loss_confirmed"], false);

    let mut replaced = device.clone();
    replaced.device_epoch = 2;
    store
        .register_node_session(
            RequestKey::new("node-a", "rest-register-2"),
            register("source-2", replaced),
        )
        .await
        .unwrap();
    let (status, body) = get_json(meta.clone(), path).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["health"], "BlockedNoSource");
    assert_eq!(body["available_copies"], 0);
    assert_eq!(body["copies"][0]["record"]["state"], "Ready");
    assert_eq!(body["loss_confirmed"], false);

    store
        .register_node_session(
            RequestKey::new("node-a", "rest-register-3"),
            register("source-3", device),
        )
        .await
        .unwrap();
    let (_, body) = get_json(meta.clone(), path).await;
    assert_eq!(body["available_copies"], 1);
    let (status, _) = get_json(meta, "/v1/dfs/chunks/missing/replication").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn external_replication_summary_reports_catalog_eligibility_without_backend_probe() {
    use afs::dfs::*;
    use afs::meta::store::{
        MetaEntity, MetaKey, MetaStore, MetaTxn, OperationResult, RequestOutcome, StoreOperation,
        TxnCondition, TxnMutation, TxnOutcome,
    };

    let (meta, store) = meta_with_memory_store().await;
    let digest = ContentDigest {
        algorithm: DigestAlgorithm::Blake3,
        bytes: [7; 32],
    };
    let id = ChunkId::new(format!(
        "blake3-{}-4",
        blake3::Hash::from(digest.bytes).to_hex()
    ));
    let copy_id = CopyId::new("external-rest-copy");
    let mut copy = CopyRecord {
        id: copy_id.clone(),
        chunk_id: id.clone(),
        role: CopyRole::ExternalCommitted,
        state: CopyState::Ready,
        location: CopyLocation::External {
            store_id: "rest-store".into(),
            object_key: external_object_key("rest-store", "generation-1", &id).unwrap(),
            object_revision: "generation-1".into(),
        },
        persisted_bytes: 4,
        verified_digest: digest.clone(),
    };
    let request = RequestKey::new("fixture", "external-rest");
    let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsCommitFileVersion);
    txn.conditions.extend([
        TxnCondition::RequestAbsent(request.clone()),
        TxnCondition::Missing(MetaKey::DfsDurabilityPolicy),
    ]);
    txn.mutations.extend([
        TxnMutation::Put(MetaEntity::DfsDurabilityPolicy(
            DfsDurabilityPolicy::ExternalBackend {
                store_id: "rest-store".into(),
                generation: "generation-1".into(),
            },
        )),
        TxnMutation::Put(MetaEntity::DfsChunk(ChunkObject {
            id: id.clone(),
            length: 4,
            content_digest: digest,
            encoding: ChunkEncoding::Raw,
        })),
        TxnMutation::Put(MetaEntity::DfsCopy(copy.clone())),
        TxnMutation::Put(MetaEntity::DfsPlacement(PlacementRecord {
            chunk_id: id.clone(),
            replica_group_id: ReplicaGroupId::new("external"),
            placement_epoch: 1,
            desired_copies: 1,
            copies: vec![copy_id],
            health: PlacementHealth::Satisfied,
        })),
        TxnMutation::RecordRequestOutcome(RequestOutcome {
            request,
            operation: StoreOperation::DfsCommitFileVersion,
            result: OperationResult::Empty,
        }),
    ]);
    assert!(matches!(
        store.compare_and_commit(txn).await.unwrap(),
        TxnOutcome::Committed { .. }
    ));
    // This fixture has no Node sessions or S3 client; the response describes
    // committed catalog eligibility and must not claim backend reachability.
    let path = format!("/v1/dfs/chunks/{}/replication", id.0);
    let (status, body) = get_json(meta.clone(), &path).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["health"], "Satisfied");
    assert_eq!(body["available_copies"], 1);
    assert_eq!(body["copies"][0]["available"], true);
    assert_eq!(
        body["availability_scope"],
        "external_committed_catalog_eligibility"
    );
    assert_eq!(body["backend_reachability"], "not_probed");
    assert_eq!(body["loss_confirmed"], false);

    let original_copy = copy.clone();
    if let CopyLocation::External {
        object_revision, ..
    } = &mut copy.location
    {
        *object_revision = "generation-2".into();
    }
    let request = RequestKey::new("fixture", "external-rest-wrong-generation");
    let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsCommitFileVersion);
    txn.conditions.extend([
        TxnCondition::RequestAbsent(request.clone()),
        TxnCondition::EntityEquals(MetaEntity::DfsCopy(original_copy)),
    ]);
    txn.mutations.extend([
        TxnMutation::Put(MetaEntity::DfsCopy(copy)),
        TxnMutation::RecordRequestOutcome(RequestOutcome {
            request,
            operation: StoreOperation::DfsCommitFileVersion,
            result: OperationResult::Empty,
        }),
    ]);
    assert!(matches!(
        store.compare_and_commit(txn).await.unwrap(),
        TxnOutcome::Committed { .. }
    ));
    let (status, body) = get_json(meta, &path).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["health"], "BlockedNoSource");
    assert_eq!(body["available_copies"], 0);
    assert_eq!(body["copies"][0]["available"], false);
    assert_eq!(body["backend_reachability"], "not_probed");
    assert_eq!(body["loss_confirmed"], false);
}

impl Default for SwitchableBackend {
    fn default() -> Self {
        Self {
            inner: MemoryLog::new(Default::default()).unwrap(),
            reject_load: AtomicBool::new(false),
        }
    }
}

impl Default for HangingBackend {
    fn default() -> Self {
        Self {
            inner: MemoryLog::new(Default::default()).unwrap(),
            loads: AtomicUsize::new(0),
            notified: tokio::sync::Notify::new(),
        }
    }
}
