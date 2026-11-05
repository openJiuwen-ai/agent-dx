//! Real-Redis regressions for independent checkpoint metadata transactions.
use super::*;
use adx_core::{snapshots::Snapshot, CheckpointArtifact};

fn spec(id: &str) -> EnvironmentSpec {
    serde_json::from_value(serde_json::json!({
        "id": id, "tenant_id": "tenant", "image": "image",
        "runtime_class": "runsc", "priority": 0,
        "resources": {"cpu_millis": 100, "memory_bytes": 128, "disk_bytes": 0}
    }))
    .unwrap()
}

fn artifact(id: &str) -> CheckpointArtifact {
    CheckpointArtifact {
        storage: "shared".into(),
        scope: String::new(),
        location: id.into(),
        size_bytes: 1,
    }
}

async fn running(session: &Session, id: &str) -> EnvironmentRecord {
    let spec = spec(id);
    let assignment = Assignment {
        environment_id: id.into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(spec.clone(), assignment.clone())
        .await
        .unwrap();
    let record = EnvironmentRecord {
        spec,
        assignment,
        runtime: adx_core::Runtime {
            id: format!("{id}-1"),
            ip: Some("10.0.0.2".parse().unwrap()),
        },
        state: EnvironmentState::Running,
        revision: 2,
        resources_held: true,
        observation: None,
        execution_artifacts: vec![],
        snapshot_source: None,
        failure: None,
        restart_attempts: 0,
        restart_pending: false,
        checkpoint: None,
        last_operation: None,
    };
    session.commit(record.clone()).await.unwrap();
    record
}

async fn register(session: &Session) {
    session
        .register(
            Node {
                id: "node".into(),
                runtime_classes: vec!["runsc".into()],
                capacity: spec("x").resources,
                available: true,
                labels: Default::default(),
                devices: vec![],
            },
            "127.0.0.1:9000".into(),
            "127.0.0.1:9001".into(),
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated real Redis"]
async fn independent_artifact_attachments_accept_the_same_header() {
    const COUNT: usize = 128;
    let rig = transport_test_redis::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    register(&session).await;
    for index in 0..COUNT {
        running(&session, &format!("source-{index}")).await;
    }
    // Freeze the read point, not the scheduling: unrelated commits must succeed
    // even when all callers read the header before any of them writes.
    let [raw_header] = store.fields([HEADER.into()]).await.unwrap();
    let header = session.header(&raw_header).unwrap();
    let mut pending = Vec::new();
    for index in 0..COUNT {
        let field = format!("environment:source-{index}");
        let [old] = store.fields([field.clone()]).await.unwrap();
        let mut next: StoredEnvironment = decode(old.as_deref().unwrap()).unwrap();
        let result = next.result.as_mut().unwrap();
        result
            .execution_artifacts
            .push(artifact(&format!("capture-{index}")));
        result.revision += 1;
        next.validate().unwrap();
        let counters = CounterUpdate::load(&store, field.clone()).await.unwrap();
        pending.push((field, old, encode(&next).unwrap(), counters));
    }
    let mut tasks = tokio::task::JoinSet::new();
    for (field, old, next, counters) in pending {
        let store = store.clone();
        let header = header.clone();
        tasks.spawn(async move {
            store
                .object_cas_with_counters(
                    &header,
                    Some((&field, old.as_deref(), next)),
                    Some(&counters),
                )
                .await
                .unwrap()
        });
    }
    let mut applied = 0;
    while let Some(result) = tasks.join_next().await {
        applied += usize::from(result.unwrap());
    }
    assert_eq!(
        applied, COUNT,
        "unrelated artifact attachments invalidated each other's header"
    );
    assert_eq!(store.fields([HEADER.into()]).await.unwrap(), [raw_header]);
    assert_eq!(session.shared_artifacts().await.unwrap().len(), COUNT);
}

#[tokio::test]
#[ignore = "requires isolated real Redis"]
async fn snapshot_publication_does_not_advance_unrelated_control_header() {
    let rig = transport_test_redis::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    session.refresh_inspection_inventory().await.unwrap();
    let before = store.fields([HEADER.into()]).await.unwrap();
    let snapshot = Snapshot::new(
        "saved".into(),
        vec![],
        spec("source"),
        "node".into(),
        "source-1".into(),
        artifact("saved"),
    )
    .unwrap();
    session.publish_snapshot(snapshot.clone()).await.unwrap();
    assert_eq!(
        store.fields([HEADER.into()]).await.unwrap(),
        before,
        "a no-volume snapshot must not invalidate independent environment commits"
    );
    assert_eq!(
        session.inspection_inventory().await.unwrap().0,
        vec![snapshot.clone()]
    );
    assert!(matches!(
        session
            .claim_artifact_deletion(&snapshot.artifact, None)
            .await,
        Err(Error::Conflict)
    ));
}

#[tokio::test]
#[ignore = "requires isolated real Redis"]
async fn local_artifact_registration_obeys_deletion_guard() {
    let rig = transport_test_redis::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    register(&session).await;
    let mut record = running(&session, "source").await;
    let mut local = artifact("/checkpoint/local");
    local.storage = "local".into();
    let deletion = session.claim_artifact_deletion(&local, None).await.unwrap();
    record.execution_artifacts.push(local);
    record.revision += 1;
    assert!(
        matches!(session.commit(record.clone()).await, Err(Error::Conflict)),
        "local references must also exclude in-flight cleanup"
    );
    drop(deletion);
    session.commit(record).await.unwrap();
}

#[tokio::test]
#[ignore = "requires isolated real Redis"]
async fn artifact_attachment_retains_object_counter_and_epoch_fences() {
    let rig = transport_test_redis::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    register(&session).await;
    let mut record = running(&session, "source").await;
    let field = "environment:source";
    let [raw_header, old] = store.fields([HEADER.into(), field.into()]).await.unwrap();
    let header = session.header(&raw_header).unwrap();
    let counters = CounterUpdate::load(&store, field.into()).await.unwrap();
    let mut next: StoredEnvironment = decode(old.as_deref().unwrap()).unwrap();
    next.result
        .as_mut()
        .unwrap()
        .execution_artifacts
        .push(artifact("candidate"));
    next.result.as_mut().unwrap().revision += 1;
    let bytes = encode(&next).unwrap();
    // A changed fact rejects the write even when the environment itself is unchanged.
    let mut command = redis::cmd("HSET");
    command
        .arg(format!("{}:business", store.key))
        .arg(field)
        .arg("changed");
    store.query::<u64>(command).await.unwrap();
    assert!(!store
        .object_cas_with_counters(
            &header,
            Some((field, old.as_deref(), bytes.clone())),
            Some(&counters)
        )
        .await
        .unwrap());
    let mut command = redis::cmd("HSET");
    command
        .arg(format!("{}:business", store.key))
        .arg(field)
        .arg(encode(&counters.value).unwrap());
    store.query::<u64>(command).await.unwrap();
    // A new environment revision must not be overwritten by a stale caller.
    record.revision += 1;
    record.execution_artifacts.push(artifact("winner"));
    session.commit(record).await.unwrap();
    assert!(!store
        .object_cas_with_counters(&header, Some((field, old.as_deref(), bytes.clone())), None)
        .await
        .unwrap());
    let _new_coordinator = store.begin(1).await.unwrap();
    let [current] = store.fields([field.into()]).await.unwrap();
    assert!(!store
        .object_cas_with_counters(&header, Some((field, current.as_deref(), bytes)), None)
        .await
        .unwrap());
    assert!(
        session.shared_artifacts().await.is_err(),
        "old epoch remains fenced"
    );
}

#[tokio::test]
#[ignore = "requires isolated real Redis"]
async fn pending_cleanup_excludes_aliases_until_retirement_or_cancellation() {
    let rig = transport_test_redis::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    register(&session).await;
    let source = running(&session, "source").await;
    let mut other = running(&session, "other").await;
    let old = session.get("source").await.unwrap();
    let mut deleted = source.clone();
    deleted.state = EnvironmentState::Deleted;
    deleted.resources_held = false;
    deleted.runtime.ip = None;
    deleted.revision += 1;
    for storage in ["local", "shared"] {
        let mut candidate = artifact(&format!("unregistered-{storage}"));
        candidate.storage = storage.into();
        let prepared = session
            .prepare_checkpoint_cleanup(&old, &deleted, std::slice::from_ref(&candidate))
            .await
            .unwrap()
            .unwrap();
        other.execution_artifacts = vec![candidate.clone()];
        other.revision += 1;
        assert!(matches!(
            session.commit(other.clone()).await,
            Err(Error::Conflict)
        ));
        let snapshot = Snapshot::new(
            format!("saved-{storage}"),
            vec![],
            source.spec.clone(),
            "node".into(),
            source.runtime.id.clone(),
            candidate.clone(),
        )
        .unwrap();
        assert!(matches!(
            session.publish_snapshot(snapshot).await,
            Err(Error::Conflict)
        ));
        let mut unrelated = other.clone();
        unrelated.execution_artifacts = vec![artifact(&format!("unrelated-{storage}"))];
        session.commit(unrelated).await.unwrap();
        // Dropping an aborted preparation releases only its scoped exclusion.
        drop(prepared);
        other.revision += 1;
        session.commit(other.clone()).await.unwrap();
        assert!(matches!(
            session
                .prepare_checkpoint_cleanup(&old, &deleted, std::slice::from_ref(&candidate))
                .await,
            Err(Error::Conflict) | Ok(None)
        ));
    }
}
