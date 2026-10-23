use super::*;
use adx_core::{CheckpointArtifact, RestorePoint};

fn artifact() -> CheckpointArtifact {
    CheckpointArtifact {
        storage: "shared".into(),
        scope: String::new(),
        location: uuid::Uuid::new_v4().to_string(),
        size_bytes: 8,
    }
}

async fn assert_no_deletion_markers(rig: &common::Redis) {
    let mut connection = redis::Client::open(rig.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let count: usize = redis::cmd("HLEN")
        .arg("adx:{test}:control:v1:artifact-gc")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(count, 0, "deletion must not retain durable markers");
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn anonymous_artifact_follows_environment_but_live_inputs_delay_reclamation() {
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    register(&session, "node").await;
    let assignment = Assignment {
        environment_id: "owner".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    let specification = spec("owner");
    session
        .reserve(specification.clone(), assignment.clone())
        .await
        .unwrap();
    let a = artifact();
    let mut r = running(specification, assignment);
    r.checkpoint = Some(RestorePoint {
        origin: None,
        id: "pause".into(),
        artifact: a.clone(),
        expires_at_unix_seconds: None,
        source_runtime_id: r.runtime.id.clone(),
    });
    r.execution_artifacts.push(a.clone());
    session.commit(r.clone()).await.unwrap();
    r.checkpoint = None;
    r.revision += 1;
    session.commit(r.clone()).await.unwrap();
    assert!(
        session.claim_artifact_deletion(&a, None).await.is_err(),
        "old input is still mapped"
    );
    let session = store.begin(1).await.unwrap();
    assert_eq!(session.shared_artifacts().await.unwrap(), vec![a.clone()]);
    assert!(
        session.claim_artifact_deletion(&a, None).await.is_err(),
        "restart must preserve input protection"
    );
    r.state = EnvironmentState::Deleted;
    r.resources_held = false;
    r.runtime.ip = None;
    r.execution_artifacts.clear();
    r.revision += 1;
    session.commit(r).await.unwrap();
    let deletion = session.claim_artifact_deletion(&a, None).await.unwrap();
    assert!(session.claim_artifact_deletion(&a, None).await.is_err());
    // Publication and deletion of the same identity are serialized in this Coordinator.
    let assignment = Assignment {
        environment_id: "late".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 2,
        devices: vec![],
    };
    session
        .reserve(spec("late"), assignment.clone())
        .await
        .unwrap();
    let mut late = running(spec("late"), assignment);
    late.execution_artifacts.push(a.clone());
    assert_eq!(session.commit(late).await.unwrap_err(), Error::Conflict);
    assert_no_deletion_markers(&rig).await;
    drop(deletion);
    let _retry = session.claim_artifact_deletion(&a, None).await.unwrap();
    session.forget_artifact(&a).await.unwrap();
    session.forget_artifact(&a).await.unwrap();
    assert!(session.shared_artifacts().await.unwrap().is_empty());
    assert_no_deletion_markers(&rig).await;
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn node_loss_keeps_execution_inputs_until_reconciled_cleanup_acknowledgement() {
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let s = store.begin(1).await.unwrap();
    s.register_session(
        node("node"),
        "node:1".into(),
        "node:2".into(),
        Some(adx_coordinator::storage::NodeSession {
            id: "old".into(),
            sequence: 1,
            routable: true,
        }),
    )
    .await
    .unwrap();
    let assignment = Assignment {
        environment_id: "owner".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    s.reserve(spec("owner"), assignment.clone()).await.unwrap();
    let a = artifact();
    let mut r = running(spec("owner"), assignment);
    r.execution_artifacts.push(a.clone());
    s.commit(r).await.unwrap();
    s.invalidate_node("node", "old").await.unwrap();
    let mut failed = s.get("owner").await.unwrap().result.unwrap();
    failed.state = EnvironmentState::Deleted;
    failed.execution_artifacts.clear();
    failed.revision += 1;
    s.commit(failed).await.unwrap();
    let s = store.begin(1).await.unwrap();
    assert_eq!(s.execution_holds().await.unwrap().len(), 1);
    assert!(s.claim_artifact_deletion(&a, None).await.is_err());
    assert!(
        s.release_execution_holds("node", "old").await.is_err(),
        "lost heartbeat is not a cleanup acknowledgement"
    );
    s.register_session(
        node("node"),
        "node:1".into(),
        "node:2".into(),
        Some(adx_coordinator::storage::NodeSession {
            id: "new".into(),
            sequence: 1,
            routable: true,
        }),
    )
    .await
    .unwrap();
    s.release_execution_holds("node", "new").await.unwrap();
    let _deletion = s.claim_artifact_deletion(&a, None).await.unwrap();
}

struct LostDeletionAck {
    inner: adx_checkpoint_storage::reclaim::ObjectReclaimer,
    lose_once: std::sync::atomic::AtomicBool,
    fail_before_remove: bool,
}
#[async_trait::async_trait]
impl adx_checkpoint_storage::reclaim::ArtifactReclaimer for LostDeletionAck {
    fn storage_alias(&self) -> &str {
        "shared"
    }
    async fn remove(&self, a: &CheckpointArtifact) -> adx_core::Result<()> {
        let fail = self
            .lose_once
            .swap(false, std::sync::atomic::Ordering::SeqCst);
        if fail && self.fail_before_remove {
            return Err(Error::Unavailable(
                "storage unavailable before deletion".into(),
            ));
        }
        self.inner.remove(a).await?;
        if fail {
            return Err(Error::Unavailable(
                "deletion committed; acknowledgement lost".into(),
            ));
        }
        Ok(())
    }
}
fn reclaimers(
    store: std::sync::Arc<dyn adx_checkpoint_storage::reclaim::ArtifactReclaimer>,
) -> adx_checkpoint_storage::reclaim::Reclaimers {
    let mut stores = adx_checkpoint_storage::reclaim::Reclaimers::default();
    stores.insert(store).unwrap();
    stores
}
async fn rpc(
    session: adx_coordinator::storage::Session,
    stores: adx_checkpoint_storage::reclaim::Reclaimers,
) -> adx_coordinator::rpc::CoordinatorRpc {
    adx_coordinator::rpc::CoordinatorRpc::new(
        session,
        Placement::Pack,
        adx_protocol::auth::Peers::network(),
        adx_transport::rpc::RpcClient::network(adx_protocol::auth::Principal::Coordinator),
        std::time::Duration::from_secs(1),
    )
    .await
    .unwrap()
    .with_reclaimers(stores)
}
async fn deletion_candidate(s: &adx_coordinator::storage::Session) -> CheckpointArtifact {
    register(s, "source").await;
    let assignment = Assignment {
        environment_id: "owner".into(),
        node_id: "source".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    s.reserve(spec("owner"), assignment.clone()).await.unwrap();
    let a = artifact();
    let mut r = running(spec("owner"), assignment);
    r.execution_artifacts.push(a.clone());
    s.commit(r.clone()).await.unwrap();
    r.state = EnvironmentState::Deleted;
    r.resources_held = false;
    r.runtime.ip = None;
    r.execution_artifacts.clear();
    r.revision += 1;
    s.commit(r).await.unwrap();
    a
}

async fn cleanup_candidate(s: &adx_coordinator::storage::Session) -> CheckpointArtifact {
    register(s, "source").await;
    let assignment = Assignment {
        environment_id: "cleanup-owner".into(),
        node_id: "source".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    s.reserve(spec("cleanup-owner"), assignment.clone())
        .await
        .unwrap();
    let a = artifact();
    let mut record = running(spec("cleanup-owner"), assignment);
    record.checkpoint = Some(RestorePoint {
        origin: None,
        id: "checkpoint".into(),
        artifact: a.clone(),
        expires_at_unix_seconds: Some(1),
        source_runtime_id: record.runtime.id.clone(),
    });
    s.commit(record.clone()).await.unwrap();
    record.state = EnvironmentState::Deleted;
    record.resources_held = false;
    record.runtime.ip = None;
    record.checkpoint = None;
    record.revision += 1;
    s.commit_with_cleanup(record, &[]).await.unwrap();
    a
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn checkpoint_outbox_reclaims_shared_artifacts_without_a_source_node() {
    use object_store::{path::Path as Key, ObjectStoreExt};
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    let a = cleanup_candidate(&s).await;
    let remote = std::sync::Arc::new(object_store::memory::InMemory::new());
    let key = Key::from(format!("test/{}/files/memory", a.location));
    remote.put(&key, b"memory".to_vec().into()).await.unwrap();

    let unconfigured = rpc(
        s.clone(),
        adx_checkpoint_storage::reclaim::Reclaimers::default(),
    )
    .await;
    assert!(unconfigured.collect_checkpoint_cleanups().await.is_err());
    assert!(remote.get(&key).await.is_ok());
    assert_eq!(s.checkpoint_cleanups().await.unwrap().len(), 1);

    let reclaimer = std::sync::Arc::new(
        adx_checkpoint_storage::reclaim::ObjectReclaimer::new(
            "shared".into(),
            remote.clone(),
            "test".into(),
        )
        .unwrap(),
    );
    let coordinator = rpc(s.clone(), reclaimers(reclaimer)).await;
    assert_eq!(coordinator.collect_checkpoint_cleanups().await.unwrap(), 1);
    assert!(remote.get(&key).await.is_err());
    assert!(s.checkpoint_cleanups().await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn coordinator_retries_committed_deletion_after_lost_ack_and_restart_without_source_node() {
    use object_store::{path::Path as Key, ObjectStoreExt};
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let s = store.begin(1).await.unwrap();
    let a = deletion_candidate(&s).await;
    let remote = std::sync::Arc::new(object_store::memory::InMemory::new());
    let key = Key::from(format!("test/{}/files/memory", a.location));
    remote.put(&key, b"memory".to_vec().into()).await.unwrap();
    let reclaimer = std::sync::Arc::new(LostDeletionAck {
        inner: adx_checkpoint_storage::reclaim::ObjectReclaimer::new(
            "shared".into(),
            remote.clone(),
            "test".into(),
        )
        .unwrap(),
        lose_once: std::sync::atomic::AtomicBool::new(true),
        fail_before_remove: false,
    });
    let first = rpc(s.clone(), reclaimers(reclaimer.clone())).await;
    assert!(first.collect_shared_artifacts().await.is_err());
    assert!(
        remote.get(&key).await.is_err(),
        "physical deletion already committed"
    );
    assert_eq!(s.shared_artifacts().await.unwrap(), vec![a]);
    assert_no_deletion_markers(&rig).await;
    drop(first);
    drop(s);
    drop(store);
    // Reconnect rather than clone: no in-memory deletion state survives a restart.
    let s = rig.store().await.begin(1).await.unwrap();
    let second = rpc(s.clone(), reclaimers(reclaimer)).await;
    assert_eq!(second.collect_shared_artifacts().await.unwrap(), 1);
    assert!(s.shared_artifacts().await.unwrap().is_empty());
    assert_eq!(second.collect_shared_artifacts().await.unwrap(), 0);
    assert_no_deletion_markers(&rig).await;
}

struct GatedDeletion {
    inner: adx_checkpoint_storage::reclaim::ObjectReclaimer,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    calls: std::sync::atomic::AtomicUsize,
}
#[async_trait::async_trait]
impl adx_checkpoint_storage::reclaim::ArtifactReclaimer for GatedDeletion {
    fn storage_alias(&self) -> &str {
        "shared"
    }
    async fn remove(&self, a: &CheckpointArtifact) -> adx_core::Result<()> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.entered.notify_one();
        self.release.notified().await;
        self.inner.remove(a).await
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn concurrent_collectors_delete_once_and_exclude_only_the_reclaiming_artifact() {
    use object_store::{path::Path as Key, ObjectStoreExt};
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    let a = deletion_candidate(&s).await;
    let remote = std::sync::Arc::new(object_store::memory::InMemory::new());
    let key = Key::from(format!("test/{}/files/memory", a.location));
    remote.put(&key, b"memory".to_vec().into()).await.unwrap();
    let probe = std::sync::Arc::new(GatedDeletion {
        inner: adx_checkpoint_storage::reclaim::ObjectReclaimer::new(
            "shared".into(),
            remote.clone(),
            "test".into(),
        )
        .unwrap(),
        entered: Default::default(),
        release: Default::default(),
        calls: Default::default(),
    });
    let coordinator = rpc(s.clone(), reclaimers(probe.clone())).await;
    let first = tokio::spawn({
        let coordinator = coordinator.clone();
        async move { coordinator.collect_shared_artifacts().await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), probe.entered.notified())
        .await
        .unwrap();
    assert_eq!(
        coordinator.collect_shared_artifacts().await.unwrap_err(),
        Error::Conflict
    );
    let assignment = Assignment {
        environment_id: "late".into(),
        node_id: "source".into(),
        shard_id: 0,
        generation: 2,
        devices: vec![],
    };
    s.reserve(spec("late"), assignment.clone()).await.unwrap();
    let mut late = running(spec("late"), assignment);
    late.execution_artifacts.push(a);
    assert_eq!(s.commit(late.clone()).await.unwrap_err(), Error::Conflict);
    late.execution_artifacts.clear();
    s.commit(late).await.unwrap();
    assert_eq!(probe.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    probe.release.notify_one();
    assert_eq!(first.await.unwrap().unwrap(), 1);
    assert!(remote.get(&key).await.is_err());
    assert!(s.shared_artifacts().await.unwrap().is_empty());
    // Treat the first successful response as lost; retry observes completed deletion.
    assert_eq!(coordinator.collect_shared_artifacts().await.unwrap(), 0);
    assert_eq!(probe.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_no_deletion_markers(&rig).await;
}

struct PartialDeletion {
    inner: adx_checkpoint_storage::reclaim::ObjectReclaimer,
    remote: std::sync::Arc<object_store::memory::InMemory>,
    first: object_store::path::Path,
    fail_once: std::sync::atomic::AtomicBool,
}
#[async_trait::async_trait]
impl adx_checkpoint_storage::reclaim::ArtifactReclaimer for PartialDeletion {
    fn storage_alias(&self) -> &str {
        "shared"
    }
    async fn remove(&self, a: &CheckpointArtifact) -> adx_core::Result<()> {
        if self
            .fail_once
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            use object_store::ObjectStoreExt;
            self.remote.delete(&self.first).await.unwrap();
            return Err(Error::Unavailable(
                "interrupted after deleting one object".into(),
            ));
        }
        self.inner.remove(a).await
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn partial_storage_deletion_converges_after_redis_crash_and_coordinator_restart() {
    use object_store::{path::Path as Key, ObjectStoreExt};
    let mut rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    let a = deletion_candidate(&s).await;
    let remote = std::sync::Arc::new(object_store::memory::InMemory::new());
    let memory = Key::from(format!("test/{}/files/memory", a.location));
    let manifest = Key::from(format!("test/{}/manifest.json", a.location));
    let sibling = Key::from(format!("test/{}-other/files/memory", a.location));
    for key in [&memory, &manifest, &sibling] {
        remote.put(key, b"data".to_vec().into()).await.unwrap();
    }
    let probe = std::sync::Arc::new(PartialDeletion {
        inner: adx_checkpoint_storage::reclaim::ObjectReclaimer::new(
            "shared".into(),
            remote.clone(),
            "test".into(),
        )
        .unwrap(),
        remote: remote.clone(),
        first: memory.clone(),
        fail_once: std::sync::atomic::AtomicBool::new(true),
    });
    let coordinator = rpc(s.clone(), reclaimers(probe.clone())).await;
    assert!(coordinator.collect_shared_artifacts().await.is_err());
    assert!(remote.get(&memory).await.is_err());
    assert!(remote.get(&manifest).await.is_ok());
    assert_eq!(s.shared_artifacts().await.unwrap(), vec![a.clone()]);
    drop(coordinator);
    drop(s);
    rig.crash();
    rig.start().await;
    let s = rig.store().await.begin(1).await.unwrap();
    assert_eq!(s.shared_artifacts().await.unwrap(), vec![a]);
    assert_no_deletion_markers(&rig).await;
    let coordinator = rpc(s.clone(), reclaimers(probe)).await;
    assert_eq!(coordinator.collect_shared_artifacts().await.unwrap(), 1);
    assert!(remote.get(&manifest).await.is_err());
    assert!(remote.get(&sibling).await.is_ok());
    assert!(s.shared_artifacts().await.unwrap().is_empty());
    assert_eq!(coordinator.collect_shared_artifacts().await.unwrap(), 0);
    assert_no_deletion_markers(&rig).await;
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn deletion_exclusion_is_per_artifact_and_cancellation_releases_it() {
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    let a = artifact();
    let ready = std::sync::Arc::new(tokio::sync::Notify::new());
    let task = tokio::spawn({
        let s = s.clone();
        let a = a.clone();
        let ready = ready.clone();
        async move {
            let _deletion = s.claim_artifact_deletion(&a, None).await.unwrap();
            ready.notify_one();
            std::future::pending::<()>().await;
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), ready.notified())
        .await
        .unwrap();
    assert!(s.claim_artifact_deletion(&a, None).await.is_err());
    let other = artifact();
    let _unrelated = s.claim_artifact_deletion(&other, None).await.unwrap();
    assert_no_deletion_markers(&rig).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let _retry = s.claim_artifact_deletion(&a, None).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn failed_deletion_keeps_inventory_and_repeated_retries_leave_no_markers() {
    use object_store::{path::Path as Key, ObjectStoreExt};
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    register(&s, "node").await;
    let remote = std::sync::Arc::new(object_store::memory::InMemory::new());
    let reclaimer = std::sync::Arc::new(LostDeletionAck {
        inner: adx_checkpoint_storage::reclaim::ObjectReclaimer::new(
            "shared".into(),
            remote.clone(),
            "test".into(),
        )
        .unwrap(),
        lose_once: std::sync::atomic::AtomicBool::new(true),
        fail_before_remove: true,
    });
    for round in 0..12 {
        let id = format!("owner-{round}");
        let assignment = Assignment {
            environment_id: id.clone(),
            node_id: "node".into(),
            shard_id: 0,
            generation: round + 1,
            devices: vec![],
        };
        s.reserve(spec(&id), assignment.clone()).await.unwrap();
        let a = artifact();
        let mut record = running(spec(&id), assignment);
        record.execution_artifacts.push(a.clone());
        s.commit(record.clone()).await.unwrap();
        let coordinator = rpc(s.clone(), reclaimers(reclaimer.clone())).await;
        assert_eq!(coordinator.collect_shared_artifacts().await.unwrap(), 0);
        drop(coordinator);
        record.state = EnvironmentState::Deleted;
        record.resources_held = false;
        record.runtime.ip = None;
        record.execution_artifacts.clear();
        record.revision += 1;
        s.commit(record).await.unwrap();
        let coordinator = rpc(s.clone(), reclaimers(reclaimer.clone())).await;
        let key = Key::from(format!("test/{}/files/memory", a.location));
        remote.put(&key, b"memory".to_vec().into()).await.unwrap();
        reclaimer
            .lose_once
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(coordinator.collect_shared_artifacts().await.is_err());
        assert!(remote.get(&key).await.is_ok());
        assert_eq!(s.shared_artifacts().await.unwrap(), vec![a]);
        assert_eq!(coordinator.collect_shared_artifacts().await.unwrap(), 1);
        assert!(remote.get(&key).await.is_err());
        assert!(s.shared_artifacts().await.unwrap().is_empty());
        assert_eq!(coordinator.collect_shared_artifacts().await.unwrap(), 0);
        assert_no_deletion_markers(&rig).await;
    }
}
#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn coordinator_collects_shared_snapshot_with_source_node_offline() {
    use object_store::{path::Path as Key, ObjectStoreExt};
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    let a = artifact();
    let snapshot = adx_core::snapshots::Snapshot::new(
        "saved".into(),
        vec![],
        spec("owner"),
        "offline-source".into(),
        "owner-1".into(),
        a.clone(),
    )
    .unwrap();
    s.publish_snapshot(snapshot).await.unwrap();
    s.delete_snapshot("saved", "tenant").await.unwrap();
    let remote = std::sync::Arc::new(object_store::memory::InMemory::new());
    let key = Key::from(format!("test/{}/files/memory", a.location));
    remote.put(&key, b"memory".to_vec().into()).await.unwrap();
    let reclaimer = std::sync::Arc::new(
        adx_checkpoint_storage::reclaim::ObjectReclaimer::new(
            "shared".into(),
            remote.clone(),
            "test".into(),
        )
        .unwrap(),
    );
    let coordinator = rpc(s.clone(), reclaimers(reclaimer)).await;
    assert_eq!(coordinator.collect_snapshots().await.unwrap(), 1);
    assert!(remote.get(&key).await.is_err());
    assert_eq!(
        s.get_snapshot("saved").await.unwrap().state,
        adx_core::snapshots::SnapshotState::Deleted
    );
}
