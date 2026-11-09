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

fn test_assignment(environment_id: &str, generation: u64) -> Assignment {
    Assignment {
        environment_id: environment_id.into(),
        node_id: "node".into(),
        shard_id: 0,
        generation,
        devices: vec![],
    }
}

async fn create(session: &adx_coordinator::storage::Session, generation: u64) -> EnvironmentRecord {
    let assignment = test_assignment("cleanup", generation);
    session
        .reserve(spec("cleanup"), assignment.clone())
        .await
        .unwrap();
    let mut record = running(spec("cleanup"), assignment);
    record.checkpoint = Some(RestorePoint {
        recovery_profile: Default::default(),
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
async fn local_cleanup_outbox_survives_restart_and_ack_is_payload_fenced() {
    let rig = common::Redis::new().await;
    let db = rig.store().await;
    let first = db.begin(1).await.unwrap();
    register(&first, "node").await;
    let record = deleted(create(&first, 1).await);
    let local = local_artifact("/checkpoints/obsolete", 1);
    first
        .commit_with_cleanup(record.clone(), &[local.clone(), local.clone()])
        .await
        .unwrap();
    let tasks = first.checkpoint_cleanups().await.unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].artifacts, vec![local.clone()]);
    assert_eq!(first.shared_artifacts().await.unwrap().len(), 1);
    // Normalization makes identical request retries independent of duplicate order.
    first
        .commit_with_cleanup(record.clone(), std::slice::from_ref(&local))
        .await
        .unwrap();
    assert_eq!(
        first
            .commit_with_cleanup(record.clone(), &[local_artifact("/injected", 1)])
            .await,
        Err(Error::Conflict)
    );
    let second = db.begin(1).await.unwrap();
    assert_eq!(second.checkpoint_cleanups().await.unwrap(), tasks);
    assert_eq!(
        first.finish_checkpoint_cleanup(&tasks[0]).await,
        Err(Error::Conflict)
    );
    let mut tampered = tasks[0].clone();
    tampered.artifacts.push(local_artifact("/injected", 1));
    assert_eq!(
        second.finish_checkpoint_cleanup(&tampered).await,
        Err(Error::Conflict)
    );
    // A lost physical ACK leaves the full task; an exact retry removes only that task.
    assert_eq!(second.checkpoint_cleanups().await.unwrap(), tasks);
    second.finish_checkpoint_cleanup(&tasks[0]).await.unwrap();
    second.finish_checkpoint_cleanup(&tasks[0]).await.unwrap();
    assert!(second.checkpoint_cleanups().await.unwrap().is_empty());
    second
        .commit_with_cleanup(record.clone(), &[local])
        .await
        .unwrap();
    assert!(second.checkpoint_cleanups().await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn cleanup_receipt_expires_and_same_name_recreation_cannot_replay_old_delete() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let record = deleted(create(&session, 1).await);
    let local = local_artifact("/checkpoints/old", 1);
    session
        .commit_with_cleanup(record.clone(), std::slice::from_ref(&local))
        .await
        .unwrap();
    let task = session.checkpoint_cleanups().await.unwrap().pop().unwrap();
    session.finish_checkpoint_cleanup(&task).await.unwrap();
    let client = redis::Client::open(rig.url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let ttl: i64 = redis::cmd("TTL")
        .arg("adx:{test}:control:v1:deleted:cleanup")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!((1..=600).contains(&ttl));
    let _: u64 = redis::cmd("DEL")
        .arg("adx:{test}:control:v1:deleted:cleanup")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        session
            .commit_with_cleanup(record.clone(), std::slice::from_ref(&local))
            .await,
        Err(Error::NotFound)
    );
    let replacement = create(&session, 2).await;
    assert_eq!(
        session.commit_with_cleanup(record, &[local]).await,
        Err(Error::Conflict)
    );
    assert_eq!(
        session.get("cleanup").await.unwrap().result,
        Some(replacement)
    );
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg("adx:{test}:control:v1:checkpoint-*-fences")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(keys.is_empty());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn shared_checkpoint_uses_registry_and_retained_references_without_cleanup_outbox() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let record = create(&session, 1).await;
    let shared = record.checkpoint.as_ref().unwrap().artifact.clone();
    let snapshot = adx_core::snapshots::Snapshot::new(
        "retained".into(),
        vec![],
        record.spec.clone(),
        "node".into(),
        record.runtime.id.clone(),
        shared.clone(),
    )
    .unwrap();
    session.publish_snapshot(snapshot).await.unwrap();
    session.commit(deleted(record)).await.unwrap();
    assert!(session.checkpoint_cleanups().await.unwrap().is_empty());
    assert_eq!(
        session.shared_artifacts().await.unwrap(),
        vec![shared.clone()]
    );
    assert!(session
        .claim_artifact_deletion(&shared, None)
        .await
        .is_err());
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
        assignment: test_assignment("cleanup", 1),
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
    let clone_assignment = test_assignment("clone", 2);
    session
        .reserve(clone_spec.clone(), clone_assignment.clone())
        .await
        .unwrap();
    let mut clone = running(clone_spec, clone_assignment);
    clone.snapshot_source = Some(RestorePoint {
        recovery_profile: Default::default(),
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
        assignment: test_assignment("cleanup", 1),
        revision: 3,
        artifacts: vec![execution_artifact.clone()],
    };
    let execution_assignment = test_assignment("execution-user", 3);
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
    let assignment = test_assignment("held", 1);
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
        assignment: test_assignment("cleanup", 1),
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
async fn deleting_zero_sized_local_capture_reservation_retains_only_pending_work() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let assignment = test_assignment("capture", 1);
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

    recovered.finish_checkpoint_cleanup(&task).await.unwrap();
    assert!(recovered.checkpoint_cleanups().await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn lost_local_node_does_not_expire_or_acknowledge_its_cleanup_responsibility() {
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    register(&session, "node").await;
    let retired = deleted(create(&session, 1).await);
    session
        .commit_with_cleanup(retired, &[local_artifact("/checkpoints/lost-node", 1)])
        .await
        .unwrap();
    let pending = session.checkpoint_cleanups().await.unwrap();
    let session = store.begin(1).await.unwrap();
    let coordinator = adx_coordinator::rpc::CoordinatorRpc::new(
        session.clone(),
        Placement::Pack,
        adx_protocol::auth::Peers::network(),
        adx_transport::rpc::RpcClient::network(adx_protocol::auth::Principal::Coordinator),
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    // No live authenticated source node is available to remove local bytes.
    assert_eq!(coordinator.collect_checkpoint_cleanups().await.unwrap(), 0);
    assert_eq!(session.checkpoint_cleanups().await.unwrap(), pending);
    let mut connection = redis::Client::open(rig.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let ttl: i64 = redis::cmd("TTL")
        .arg("adx:{test}:control:v1:checkpoint-cleanups")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(ttl, -1, "lost nodes cannot expire accepted cleanup work");
}
