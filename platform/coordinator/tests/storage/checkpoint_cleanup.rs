use super::*;
use adx_core::{CheckpointArtifact, CheckpointCleanup, RestorePoint};

fn artifact(location: &str) -> CheckpointArtifact {
    CheckpointArtifact {
        storage: "shared".into(),
        scope: "scope".into(),
        location: location.into(),
        size_bytes: 1,
    }
}

fn local_artifact(location: &str, size_bytes: u64) -> CheckpointArtifact {
    CheckpointArtifact {
        storage: "local".into(),
        scope: String::new(),
        location: location.into(),
        size_bytes,
    }
}

async fn create(session: &adx_coordinator::storage::Session, generation: u64) -> EnvironmentRecord {
    let assignment = Assignment {
        environment_id: "cleanup".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation,
        devices: vec![],
    };
    session
        .reserve(spec("cleanup"), assignment.clone())
        .await
        .unwrap();
    let mut record = running(spec("cleanup"), assignment);
    record.checkpoint = Some(RestorePoint {
        origin: None,
        id: "cp".into(),
        artifact: artifact(&format!("artifact-{generation}")),
        expires_at_unix_seconds: Some(1),
        source_runtime_id: record.runtime.id.clone(),
    });
    session.commit(record.clone()).await.unwrap();
    record
}

fn deleted(mut record: EnvironmentRecord) -> EnvironmentRecord {
    record.state = EnvironmentState::Deleted;
    record.revision += 1;
    record.resources_held = false;
    record.checkpoint = None;
    record.runtime.ip = None;
    record
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn cleanup_outbox_ack_is_generation_payload_and_epoch_fenced() {
    let rig = common::Redis::new().await;
    let db = rig.store().await;
    let first = db.begin(1).await.unwrap();
    register(&first, "node").await;
    let record = deleted(create(&first, 1).await);
    first
        .commit_with_cleanup(record.clone(), &[artifact("backlog"), artifact("backlog")])
        .await
        .unwrap();
    let tasks = first.checkpoint_cleanups().await.unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].artifacts.len(), 2);
    first
        .commit_with_cleanup(record.clone(), &[artifact("backlog")])
        .await
        .unwrap();
    assert_eq!(
        first
            .commit_with_cleanup(record.clone(), &[artifact("injected")])
            .await,
        Err(Error::Conflict)
    );
    // Reused public name has a different immutable generation and object identity.
    let replacement = deleted(create(&first, 2).await);
    first.commit(replacement).await.unwrap();
    assert_eq!(first.checkpoint_cleanups().await.unwrap().len(), 2);
    // The short name-keyed receipt now belongs to generation two; generation
    // one's immutable cleanup receipt still confirms its exact lost ACK.
    first
        .commit_with_cleanup(record, &[artifact("backlog")])
        .await
        .unwrap();
    let second = db.begin(1).await.unwrap();
    assert_eq!(
        first.finish_checkpoint_cleanup(&tasks[0]).await,
        Err(Error::Conflict)
    );
    let mut tampered = tasks[0].clone();
    tampered.artifacts.push(artifact("injected"));
    assert_eq!(
        second.finish_checkpoint_cleanup(&tampered).await,
        Err(Error::Conflict)
    );
    second.finish_checkpoint_cleanup(&tasks[0]).await.unwrap();
    second.finish_checkpoint_cleanup(&tasks[0]).await.unwrap();
    let remaining = second.checkpoint_cleanups().await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].assignment.generation, 2);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn cleanup_receipt_and_object_fence_survive_physical_ack_and_short_receipt_expiry() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let record = deleted(create(&session, 1).await);
    session.commit(record.clone()).await.unwrap();
    let task = session.checkpoint_cleanups().await.unwrap().pop().unwrap();
    session.finish_checkpoint_cleanup(&task).await.unwrap();
    let client = redis::Client::open(rig.url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let _: u64 = redis::cmd("DEL")
        .arg("adx:{test}:control:v1:deleted:cleanup")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        session
            .commit_with_cleanup(record.clone(), &task.artifacts)
            .await
            .unwrap(),
        record
    );
    assert!(session.checkpoint_cleanups().await.unwrap().is_empty());
    let mut forged = record.clone();
    forged.spec.tenant_id = "other".into();
    assert_eq!(
        session.commit_with_cleanup(forged, &task.artifacts).await,
        Err(Error::NotFound)
    );
    let a = Assignment {
        environment_id: "replacement".into(),
        generation: 2,
        ..record.assignment.clone()
    };
    session
        .reserve(spec("replacement"), a.clone())
        .await
        .unwrap();
    let mut replacement = running(spec("replacement"), a);
    replacement.checkpoint = Some(RestorePoint {
        origin: None,
        id: "stale".into(),
        artifact: task.artifacts[0].clone(),
        expires_at_unix_seconds: Some(1),
        source_runtime_id: replacement.runtime.id.clone(),
    });
    assert_eq!(
        session.commit(replacement.clone()).await,
        Err(Error::Conflict)
    );
    replacement
        .checkpoint
        .as_mut()
        .unwrap()
        .artifact
        .scope
        .clear();
    assert_eq!(
        session.commit(replacement.clone()).await,
        Err(Error::Conflict)
    );
    replacement.checkpoint.as_mut().unwrap().artifact.scope = "different-backend".into();
    session.commit(replacement).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn cleanup_rejects_aliases_of_retained_environment_and_snapshot() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let record = create(&session, 1).await;
    let alias = record.checkpoint.as_ref().unwrap().artifact.clone();
    let other = Assignment {
        environment_id: "other".into(),
        generation: 2,
        ..record.assignment.clone()
    };
    session.reserve(spec("other"), other.clone()).await.unwrap();
    let mut retained = running(spec("other"), other);
    retained.checkpoint = Some(RestorePoint {
        origin: None,
        id: "alias".into(),
        artifact: alias.clone(),
        expires_at_unix_seconds: Some(1),
        source_runtime_id: retained.runtime.id.clone(),
    });
    session.commit(retained.clone()).await.unwrap();
    assert_eq!(
        session.commit(deleted(record.clone())).await,
        Err(Error::Conflict)
    );
    assert!(session.checkpoint_cleanups().await.unwrap().is_empty());
    // Preserve the reference in a reusable snapshot while retiring the other owner.
    retained.checkpoint = None;
    retained.revision += 1;
    session.commit(retained).await.unwrap();
    let snapshot = adx_core::snapshots::Snapshot {
        id: "snapshot".into(),
        names: vec![],
        template: record.spec.clone(),
        source_node_id: "node".into(),
        source_runtime_id: record.runtime.id.clone(),
        artifact: alias.clone(),
        revision: 1,
        state: adx_core::snapshots::SnapshotState::Ready,
        references: Default::default(),
    };
    session.publish_snapshot(snapshot).await.unwrap();
    assert_eq!(session.commit(deleted(record)).await, Err(Error::Conflict));
    assert!(session.checkpoint_cleanups().await.unwrap().is_empty());
    let task = CheckpointCleanup {
        assignment: retained_assignment(),
        revision: 3,
        artifacts: vec![alias],
    };
    assert!(!session
        .checkpoint_cleanup_unreferenced(&task, None)
        .await
        .unwrap());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn cleanup_rejects_local_snapshot_source_and_execution_artifact_aliases() {
    use adx_coordinator::snapshots::{Reference, Snapshot};

    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let artifact = local_artifact("/checkpoints/local-alias", 1);
    let snapshot_task = CheckpointCleanup {
        assignment: retained_assignment(),
        revision: 3,
        artifacts: vec![artifact.clone()],
    };

    let snapshot = Snapshot::new(
        "local-source".into(),
        vec![],
        spec("source"),
        "node".into(),
        "source-1".into(),
        artifact.clone(),
    )
    .unwrap();
    session.publish_snapshot(snapshot.clone()).await.unwrap();
    session
        .acquire_snapshot(
            &snapshot.id,
            "tenant",
            Reference::Restore {
                environment_id: "clone".into(),
            },
        )
        .await
        .unwrap();
    let mut clone_spec = spec("clone");
    clone_spec.snapshot_id = Some(snapshot.id.clone());
    let clone_assignment = Assignment {
        environment_id: "clone".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 2,
        devices: vec![],
    };
    session
        .reserve(clone_spec.clone(), clone_assignment.clone())
        .await
        .unwrap();
    let mut clone = running(clone_spec, clone_assignment);
    clone.snapshot_source = Some(RestorePoint {
        origin: Some(snapshot.origin().unwrap()),
        id: snapshot.id,
        artifact: artifact.clone(),
        expires_at_unix_seconds: None,
        source_runtime_id: snapshot.source_runtime_id,
    });
    session.commit(clone).await.unwrap();
    let client = redis::Client::open(rig.url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let _: u64 = redis::cmd("HDEL")
        .arg("adx:{test}:control:v1:snapshots")
        .arg("local-source")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(
        !session
            .checkpoint_cleanup_unreferenced(&snapshot_task, None)
            .await
            .unwrap(),
        "a local snapshot source remains a live borrow when its catalog entry is missing"
    );

    let execution_artifact = local_artifact("/checkpoints/local-input", 1);
    let execution_task = CheckpointCleanup {
        assignment: retained_assignment(),
        revision: 3,
        artifacts: vec![execution_artifact.clone()],
    };
    let execution_assignment = Assignment {
        environment_id: "execution-user".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 3,
        devices: vec![],
    };
    session
        .reserve(spec("execution-user"), execution_assignment.clone())
        .await
        .unwrap();
    let mut execution = running(spec("execution-user"), execution_assignment);
    execution.execution_artifacts.push(execution_artifact);
    session.commit(execution).await.unwrap();
    assert!(
        !session
            .checkpoint_cleanup_unreferenced(&execution_task, Some("clone"))
            .await
            .unwrap(),
        "a local execution artifact remains a live input"
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn cleanup_rejects_execution_hold_alias() {
    use adx_coordinator::storage::NodeSession;

    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    session
        .register_session(
            node("node"),
            "127.0.0.1:9000".into(),
            "127.0.0.1:9001".into(),
            Some(NodeSession {
                id: "old".into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    let assignment = Assignment {
        environment_id: "held".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(spec("held"), assignment.clone())
        .await
        .unwrap();
    let artifact = artifact("held");
    let mut record = running(spec("held"), assignment);
    record.execution_artifacts.push(artifact.clone());
    session.commit(record).await.unwrap();
    session.invalidate_node("node", "old").await.unwrap();
    let mut retired = session.get("held").await.unwrap().result.unwrap();
    retired.state = EnvironmentState::Deleted;
    retired.resources_held = false;
    retired.runtime.ip = None;
    retired.execution_artifacts.clear();
    retired.revision += 1;
    session.commit(retired).await.unwrap();

    let task = CheckpointCleanup {
        assignment: retained_assignment(),
        revision: 3,
        artifacts: vec![artifact],
    };
    assert!(
        !session
            .checkpoint_cleanup_unreferenced(&task, None)
            .await
            .unwrap(),
        "an execution input hold survives the retired record"
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn deleting_zero_sized_capture_reservation_creates_durable_cleanup_fence() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let assignment = Assignment {
        environment_id: "capture".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(spec("capture"), assignment.clone())
        .await
        .unwrap();
    let reservation = local_artifact("/checkpoints/capture", 0);
    let mut record = running(spec("capture"), assignment);
    record.execution_artifacts.push(reservation.clone());
    session.commit(record.clone()).await.unwrap();
    let mut deleted = deleted(record);
    deleted.execution_artifacts.clear();
    session.commit(deleted.clone()).await.unwrap();

    let recovered = rig.store().await.begin(1).await.unwrap();
    let task = recovered
        .checkpoint_cleanups()
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(task.artifacts, vec![reservation]);
    recovered.commit_with_cleanup(deleted, &[]).await.unwrap();
    assert_eq!(
        recovered.checkpoint_cleanups().await.unwrap(),
        vec![task.clone()]
    );

    let reuse_assignment = Assignment {
        environment_id: "capture-reuse".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 2,
        devices: vec![],
    };
    recovered
        .reserve(spec("capture-reuse"), reuse_assignment.clone())
        .await
        .unwrap();
    let mut reuse = running(spec("capture-reuse"), reuse_assignment);
    reuse.execution_artifacts.push(task.artifacts[0].clone());
    assert_eq!(recovered.commit(reuse).await, Err(Error::Conflict));
}

fn retained_assignment() -> Assignment {
    Assignment {
        environment_id: "cleanup".into(),
        node_id: "node".into(),
        generation: 1,
        shard_id: 0,
        devices: vec![],
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn snapshot_publication_and_cleanup_retirement_share_the_atomic_fence() {
    let rig = common::Redis::new().await;
    for attempt in 0..8 {
        let store = adx_coordinator::storage::RedisStore::connect(
            &rig.url,
            &format!("cleanup-race-{attempt}"),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        let session = store.begin(1).await.unwrap();
        register(&session, "node").await;
        let record = create(&session, 1).await;
        let before = session.revision().await.unwrap();
        let snapshot = adx_core::snapshots::Snapshot::new(
            "alias".into(),
            vec![],
            record.spec.clone(),
            "node".into(),
            record.runtime.id.clone(),
            record.checkpoint.as_ref().unwrap().artifact.clone(),
        )
        .unwrap();
        let (publication, retirement) = tokio::join!(
            session.publish_snapshot(snapshot),
            session.commit(deleted(record))
        );
        assert_ne!(
            publication.is_ok(),
            retirement.is_ok(),
            "an artifact must have exactly one live or retired owner"
        );
        assert_eq!(session.revision().await.unwrap(), before + 1);
        if publication.is_ok() {
            assert!(session.checkpoint_cleanups().await.unwrap().is_empty());
        }
    }
}
