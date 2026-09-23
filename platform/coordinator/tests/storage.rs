#[path = "storage/claims.rs"]
mod claims;
mod common;
use adx_coordinator::{storage::StoredNode, Coordinator, Node, Placement};
use adx_core::{
    Assignment, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Error, Resources,
};

fn spec(id: &str) -> EnvironmentSpec {
    EnvironmentSpec {
        runtime_profile: None,
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        id: id.into(),
        tenant_id: "tenant".into(),
        image: "image".into(),
        runtime_class: "runc".into(),
        resources: Resources {
            cpu_millis: 100,
            memory_bytes: 128,
            disk_bytes: 0,
        },
        priority: 0,
        scheduling: Default::default(),
        sandbox: Default::default(),
    }
}
fn node(id: &str) -> Node {
    Node {
        id: id.into(),
        capacity: spec("x").resources,
        available: true,
        labels: Default::default(),
        devices: vec![],
    }
}
fn running(spec: EnvironmentSpec, assignment: Assignment) -> EnvironmentRecord {
    EnvironmentRecord {
        restart_attempts: 0,
        restart_pending: false,
        runtime: adx_core::Runtime {
            id: format!("{}-{}", spec.id, assignment.generation),
            ip: Some("10.0.0.2".parse().unwrap()),
        },
        spec,
        assignment,
        state: EnvironmentState::Running,
        revision: 2,
        resources_held: true,
        checkpoint: None,
        last_operation: None,
    }
}
async fn register(s: &adx_coordinator::storage::Session, id: &str) -> StoredNode {
    s.register(node(id), "127.0.0.1:9000".into(), "127.0.0.1:9001".into())
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn administrative_scheduling_pause_survives_node_heartbeats() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let registered = register(&session, "node").await;
    assert!(registered.node.available);
    assert!(!registered.scheduling_paused);

    let paused = session
        .set_node_scheduling("node", true, false)
        .await
        .unwrap();
    assert!(paused.scheduling_paused);
    assert!(!paused.node.available);

    let heartbeat = register(&session, "node").await;
    assert!(heartbeat.scheduling_paused);
    assert!(!heartbeat.node.available);

    let resumed = session
        .set_node_scheduling("node", false, true)
        .await
        .unwrap();
    assert!(!resumed.scheduling_paused);
    assert!(resumed.node.available);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn runtime_network_policy_is_the_only_mutable_spec_field() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let assignment = Assignment {
        environment_id: "networked".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(spec("networked"), assignment.clone())
        .await
        .unwrap();
    let initial = running(spec("networked"), assignment);
    session.commit(initial.clone()).await.unwrap();

    let mut updated = initial;
    updated.spec.sandbox.network = Some(adx_core::sandbox::NetworkPolicy::default());
    updated.revision = 3;
    updated.last_operation = Some(adx_core::CompletedOperation {
        id: "network-1".into(),
        kind: adx_core::LifecycleKind::Network,
        expected_revision: 2,
    });
    assert_eq!(session.commit(updated.clone()).await.unwrap(), updated);
    let stored = session.get("networked").await.unwrap();
    assert_eq!(stored.spec, updated.spec);
    assert_eq!(stored.result.as_ref().unwrap().spec, updated.spec);

    let mut changed_resources = updated;
    changed_resources.spec.resources.cpu_millis += 1;
    changed_resources.revision = 4;
    assert_eq!(
        session.commit(changed_resources).await,
        Err(Error::Conflict)
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn restart_recovers_assignment_and_fences_old_writer() {
    let rig = common::Redis::new().await;
    let db = rig.store().await;
    let first = db.begin(2).await.unwrap();
    assert_eq!(register(&first, "z").await.shard_id, 0);
    assert_eq!(register(&first, "a").await.shard_id, 1);
    let mut scheduler =
        Coordinator::restore(&first.snapshot().await.unwrap(), Placement::Pack).unwrap();
    // Restored nodes must re-register before they can receive new work.
    scheduler.submit(spec("environment")).unwrap();
    assert!(scheduler.schedule(0).unwrap().is_none());
    scheduler.register(node("z")).unwrap();
    let a = scheduler.schedule(0).unwrap().unwrap();
    first.reserve(spec("environment"), a.clone()).await.unwrap();
    let r = running(spec("environment"), a.clone());
    first.commit(r.clone()).await.unwrap();
    let version = first.snapshot().await.unwrap().revision;
    first.commit(r.clone()).await.unwrap();
    assert_eq!(first.snapshot().await.unwrap().revision, version);
    let second = db.begin(2).await.unwrap();
    assert_eq!(first.commit(r.clone()).await, Err(Error::Conflict));
    let snapshot = second.snapshot().await.unwrap();
    assert_eq!(snapshot.routes().unwrap().len(), 1);
    let mut restored = Coordinator::restore(&snapshot, Placement::Pack).unwrap();
    assert_eq!(restored.register(node("z")).unwrap(), 0);
    assert_eq!(restored.register(node("a")).unwrap(), 1);
    restored.submit(spec("waiting")).unwrap();
    assert!(restored.schedule(0).unwrap().is_none());
    let mut deleted = r.clone();
    deleted.state = EnvironmentState::Deleted;
    deleted.revision = 4;
    deleted.resources_held = false;
    second.commit(deleted.clone()).await.unwrap();
    assert_eq!(second.commit(r).await, Err(Error::Conflict));
    assert!(second
        .snapshot()
        .await
        .unwrap()
        .routes()
        .unwrap()
        .is_empty());
    let snap = second.snapshot().await.unwrap();
    let mut restored = Coordinator::restore(&snap, Placement::Pack).unwrap();
    assert_eq!(restored.pending(0).unwrap(), 0);
    restored.register(node("z")).unwrap();
    assert_eq!(restored.submit(spec("environment")), Err(Error::Conflict));
    restored.submit(spec("new")).unwrap();
    let newer = restored.schedule(0).unwrap().unwrap();
    assert!(newer.generation > a.generation);
    second.reserve(spec("new"), newer).await.unwrap();
    assert!(db.begin(3).await.is_err());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn conflicting_commits_never_publish_a_partial_route() {
    let rig = common::Redis::new().await;
    let db = rig.store().await;
    let s = db.begin(1).await.unwrap();
    register(&s, "n").await;
    let a = Assignment {
        environment_id: "i".into(),
        node_id: "n".into(),
        shard_id: 0,
        generation: (1_u64 << 53) + 7,
        devices: vec![],
    };
    s.reserve(spec("i"), a.clone()).await.unwrap();
    let r = running(spec("i"), a);
    let (x, y) = tokio::join!(s.commit(r.clone()), s.commit(r.clone()));
    x.unwrap();
    y.unwrap();
    let before = s.snapshot().await.unwrap();
    let mut forged = r.clone();
    forged.spec.tenant_id = "another".into();
    assert_eq!(s.commit(forged).await, Err(Error::Conflict));
    let mut changed = r.clone();
    changed.runtime.ip = Some("10.0.0.3".parse().unwrap());
    assert_eq!(s.commit(changed).await, Err(Error::Conflict));
    let mut stale = r.clone();
    stale.assignment.generation -= 1;
    assert_eq!(s.commit(stale).await, Err(Error::Conflict));
    assert_eq!(s.snapshot().await.unwrap(), before);
    let mut invalid = r.clone();
    invalid.revision += 1;
    invalid.resources_held = false;
    assert!(s.commit(invalid).await.is_err());
    assert_eq!(s.snapshot().await.unwrap(), before);
    // A cleanup failure removes routing but continues to occupy the node.
    let mut failed = r;
    failed.state = EnvironmentState::Failed;
    failed.revision += 1;
    s.commit(failed).await.unwrap();
    let snap = s.snapshot().await.unwrap();
    assert!(snap.routes().unwrap().is_empty());
    let mut coordinator = Coordinator::restore(&snap, Placement::Pack).unwrap();
    coordinator.register(node("n")).unwrap();
    coordinator.submit(spec("next")).unwrap();
    assert!(coordinator.schedule(0).unwrap().is_none());
}

#[tokio::test]
#[ignore = "requires real Redis; build/ci/run.py storage"]
async fn aof_crash_restart_preserves_state_and_reconnect_does_not_change_epoch() {
    let mut rig = common::Redis::new().await;
    let db = rig.store().await;
    let s = db.begin(1).await.unwrap();
    register(&s, "node").await;
    let assignment = Assignment {
        environment_id: "i".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    s.reserve(spec("i"), assignment.clone()).await.unwrap();
    let r = running(spec("i"), assignment);
    s.commit(r.clone()).await.unwrap();
    let before = s.snapshot().await.unwrap();
    rig.crash();
    assert!(matches!(
        s.commit(r.clone()).await,
        Err(Error::Unavailable(_))
    ));
    rig.start().await;
    // Use the existing Coordinator session: a Redis reconnect does not acquire a new epoch.
    assert_eq!(s.snapshot().await.unwrap(), before);
    s.commit(r).await.unwrap();
    let restarted = rig.store().await.begin(1).await.unwrap();
    let after = restarted.snapshot().await.unwrap();
    assert_eq!(after.environments, before.environments);
    assert_eq!(after.generation, before.generation);
    assert!(after.revision > before.revision);
    assert_eq!(s.snapshot().await, Err(Error::Conflict));
}

#[tokio::test]
#[ignore = "requires real Redis; build/ci/run.py storage"]
async fn missing_or_future_schema_is_not_silently_reinitialized() {
    let rig = common::Redis::new().await;
    let db = rig.store().await;
    let s = db.begin(1).await.unwrap();
    register(&s, "n").await;
    let client = redis::Client::open(rig.url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let key = "adx:{test}:control:v1";
    let original: String = redis::cmd("HGET")
        .arg(key)
        .arg("header")
        .query_async(&mut conn)
        .await
        .unwrap();
    let _: usize = redis::cmd("HDEL")
        .arg(key)
        .arg("header")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(matches!(db.begin(1).await, Err(Error::Unavailable(_))));
    let mut header: serde_json::Value = serde_json::from_str(&original).unwrap();
    header["schema"] = serde_json::json!(99);
    let _: usize = redis::cmd("HSET")
        .arg(key)
        .arg("header")
        .arg(header.to_string())
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(matches!(db.begin(1).await, Err(Error::Unavailable(_))));
    let still_exists: bool = redis::cmd("HEXISTS")
        .arg(key)
        .arg("node:n")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(still_exists);
}

#[tokio::test]
#[ignore = "requires real Redis; build/ci/run.py storage"]
async fn rejected_assignment_can_be_replaced_without_accepting_its_late_result() {
    let rig = common::Redis::new().await;
    let db = rig.store().await;
    let s = db.begin(1).await.unwrap();
    register(&s, "a").await;
    register(&s, "b").await;
    let old = Assignment {
        environment_id: "i".into(),
        node_id: "a".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    s.reserve(spec("i"), old.clone()).await.unwrap();
    let mut next = old.clone();
    next.node_id = "b".into();
    next.generation = 2;
    s.replace_rejected(&old, next.clone()).await.unwrap();
    let before = s.snapshot().await.unwrap();
    s.replace_rejected(&old, next.clone()).await.unwrap();
    assert_eq!(s.snapshot().await.unwrap(), before);
    assert_eq!(
        s.commit(running(spec("i"), old.clone())).await,
        Err(Error::Conflict)
    );
    s.commit(running(spec("i"), next.clone())).await.unwrap();
    let mut third = next.clone();
    third.node_id = "a".into();
    third.generation = 3;
    assert_eq!(s.replace_rejected(&next, third).await, Err(Error::Conflict));
    let routes = s.snapshot().await.unwrap().routes().unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].node_id, "b");
    assert_eq!(routes[0].generation, 2);
}

#[tokio::test]
#[ignore = "requires real Redis; build/ci/run.py storage"]
async fn credential_bootstrap_is_idempotent_and_fenced() {
    let redis = common::Redis::new().await;
    let store = redis.store().await;
    let session = store.begin(1).await.unwrap();
    let key = "test-key-".repeat(5);
    let credential = adx_coordinator::auth::Credential {
        tenant_id: "t".into(),
        administrator: false,
        expires_at_unix_seconds: 0,
    };
    session
        .bootstrap_credential(&key, &credential)
        .await
        .unwrap();
    session
        .bootstrap_credential(&key, &credential)
        .await
        .unwrap();
    let digest = adx_coordinator::auth::digest(&key).unwrap();
    let stored = session.credential(&digest).await.unwrap();
    assert_eq!(stored.tenant_id, "t");
    let mut other = credential.clone();
    other.administrator = true;
    assert_eq!(
        session
            .bootstrap_credential(&key, &other)
            .await
            .unwrap_err(),
        adx_core::Error::Conflict
    );
    let _next = store.begin(1).await.unwrap();
    assert_eq!(
        session
            .bootstrap_credential(&key, &credential)
            .await
            .unwrap_err(),
        adx_core::Error::Conflict
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn discovery_expires_and_rejects_superseded_coordinator() {
    use std::time::Duration;
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let first = store.begin(1).await.unwrap();
    let discovery =
        adx_discovery::RedisDiscovery::new(&rig.url, "test", Duration::from_secs(1)).unwrap();
    assert_eq!(discovery.lookup().await, Err(Error::NotFound));
    first
        .advertise("test", "https://127.0.0.1:9100", Duration::from_millis(150))
        .await
        .unwrap();
    let endpoint = discovery.lookup().await.unwrap();
    assert_eq!(endpoint.epoch, first.epoch());
    assert_eq!(endpoint.address, "https://127.0.0.1:9100");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(discovery.lookup().await, Err(Error::NotFound));
    first
        .advertise("test", "https://127.0.0.1:9100", Duration::from_secs(5))
        .await
        .unwrap();
    let second = store.begin(1).await.unwrap();
    // The old advertisement still exists, but its epoch no longer authorizes it.
    assert_eq!(discovery.lookup().await, Err(Error::NotFound));
    assert_eq!(
        first
            .advertise("test", "https://127.0.0.1:9100", Duration::from_secs(5))
            .await,
        Err(Error::Conflict)
    );
    second
        .advertise("test", "https://127.0.0.1:9200", Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        discovery.lookup().await.unwrap().address,
        "https://127.0.0.1:9200"
    );
    assert!(second
        .advertise("wrong", "https://127.0.0.1:9200", Duration::from_secs(5))
        .await
        .is_err());
    assert!(second
        .advertise("test", "http://127.0.0.1:9200/path", Duration::from_secs(5))
        .await
        .is_err());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn pause_resume_persists_recovery_point_fences_old_results_and_restores_capacity() {
    use adx_core::{CheckpointArtifact, CompletedOperation, LifecycleKind, RestorePoint};
    let rig = common::Redis::new().await;
    let db = rig.store().await;
    let session = db.begin(1).await.unwrap();
    register(&session, "node").await;
    let mut scheduler =
        Coordinator::restore(&session.snapshot().await.unwrap(), Placement::Pack).unwrap();
    scheduler.register(node("node")).unwrap();
    scheduler.submit(spec("environment")).unwrap();
    let assignment = scheduler.schedule(0).unwrap().unwrap();
    session
        .reserve(spec("environment"), assignment.clone())
        .await
        .unwrap();
    let initial = running(spec("environment"), assignment.clone());
    session.commit(initial.clone()).await.unwrap();
    let mut paused = initial.clone();
    paused.state = EnvironmentState::Paused;
    paused.revision = 4;
    paused.resources_held = false;
    paused.runtime.ip = None;
    paused.checkpoint = Some(RestorePoint {
        origin: None,
        id: "pause".into(),
        source_runtime_id: initial.runtime.id.clone(),
        expires_at_unix_seconds: u64::MAX,
        artifact: CheckpointArtifact {
            storage: "local".into(),
            location: "/checkpoints/cp".into(),
            size_bytes: 123,
        },
    });
    paused.last_operation = Some(CompletedOperation {
        id: "pause".into(),
        kind: LifecycleKind::Pause,
        expected_revision: 2,
    });
    session.commit(paused.clone()).await.unwrap();
    scheduler.release(&assignment).unwrap();
    assert!(session
        .snapshot()
        .await
        .unwrap()
        .routes()
        .unwrap()
        .is_empty());
    let mut resumed = paused.clone();
    resumed.state = EnvironmentState::Running;
    resumed.revision = 6;
    resumed.resources_held = true;
    resumed.runtime.id = format!("{}-r5", initial.runtime.id);
    resumed.runtime.ip = Some("10.0.0.3".parse().unwrap());
    resumed.last_operation = Some(CompletedOperation {
        id: "resume".into(),
        kind: LifecycleKind::Resume,
        expected_revision: 4,
    });
    session.commit(resumed.clone()).await.unwrap();
    scheduler
        .restore_assignment(&resumed.spec, &assignment)
        .unwrap();
    scheduler.submit(spec("waiting")).unwrap();
    assert!(scheduler.schedule(0).unwrap().is_none());
    assert_eq!(session.commit(paused).await, Err(Error::Conflict));
    let restarted = db.begin(1).await.unwrap();
    assert_eq!(
        restarted.snapshot().await.unwrap().environments["environment"]
            .result
            .as_ref(),
        Some(&resumed)
    );
    assert_eq!(
        restarted.snapshot().await.unwrap().routes().unwrap().len(),
        1
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn sqlite_replays_pause_and_resume_after_redis_and_node_sink_restart() {
    use adx_core::{CheckpointArtifact, CompletedOperation, LifecycleKind, RestorePoint};
    use adxlet::{journal::JournalSink, Durability, StateSink};
    use std::sync::Arc;
    struct Sink(Arc<adx_coordinator::storage::Session>);
    #[async_trait::async_trait]
    impl StateSink for Sink {
        async fn commit(&self, r: &EnvironmentRecord) -> adx_core::Result<Durability> {
            self.0.commit(r.clone()).await?;
            Ok(Durability::Published)
        }
    }
    let mut rig = common::Redis::new().await;
    let db = rig.store().await;
    let session = Arc::new(db.begin(1).await.unwrap());
    register(&session, "n").await;
    let assignment = Assignment {
        environment_id: "journal".into(),
        node_id: "n".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(spec("journal"), assignment.clone())
        .await
        .unwrap();
    let initial = running(spec("journal"), assignment);
    session.commit(initial.clone()).await.unwrap();
    let mut paused = initial.clone();
    paused.state = EnvironmentState::Paused;
    paused.revision = 4;
    paused.resources_held = false;
    paused.runtime.ip = None;
    paused.checkpoint = Some(RestorePoint {
        origin: None,
        id: "cp".into(),
        source_runtime_id: initial.runtime.id.clone(),
        expires_at_unix_seconds: u64::MAX,
        artifact: CheckpointArtifact {
            storage: "local".into(),
            location: "/tmp/checkpoint".into(),
            size_bytes: 1,
        },
    });
    paused.last_operation = Some(CompletedOperation {
        id: "pause".into(),
        kind: LifecycleKind::Pause,
        expected_revision: 2,
    });
    let mut resumed = paused.clone();
    resumed.state = EnvironmentState::Running;
    resumed.revision = 6;
    resumed.runtime.id = "journal-1-r5".into();
    resumed.resources_held = true;
    resumed.runtime.ip = initial.runtime.ip;
    resumed.last_operation = Some(CompletedOperation {
        id: "resume".into(),
        kind: LifecycleKind::Resume,
        expected_revision: 4,
    });
    let tmp = tempfile::tempdir().unwrap();
    let upstream = Arc::new(Sink(session.clone()));
    let sink = JournalSink::new(tmp.path().join("degraded.sqlite"), upstream.clone());
    rig.crash();
    assert_eq!(sink.commit(&paused).await.unwrap(), Durability::Journaled);
    assert_eq!(sink.commit(&resumed).await.unwrap(), Durability::Journaled);
    drop(sink);
    rig.start().await;
    let sink = JournalSink::new(tmp.path().join("degraded.sqlite"), upstream);
    sink.recover(&[initial]).await.unwrap();
    let snapshot = session.snapshot().await.unwrap();
    assert_eq!(snapshot.environments["journal"].result, Some(resumed));
    assert_eq!(snapshot.routes().unwrap().len(), 1);
    assert_eq!(sink.pending().await.unwrap(), 0);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn restart_attempts_survive_coordinator_restart_and_cannot_be_reset() {
    let rig = common::Redis::new().await;
    let db = rig.store().await;
    let session = db.begin(1).await.unwrap();
    register(&session, "n").await;
    let mut request = spec("restart");
    request.lifecycle.restart = Some(adx_core::lifecycle::RestartPolicy {
        max_attempts: 2,
        initial_backoff_seconds: 1,
        max_backoff_seconds: 10,
    });
    let assignment = Assignment {
        environment_id: request.id.clone(),
        node_id: "n".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(request.clone(), assignment.clone())
        .await
        .unwrap();
    let mut result = running(request, assignment);
    session.commit(result.clone()).await.unwrap();
    result.state = EnvironmentState::Failed;
    result.revision = 3;
    result.resources_held = false;
    result.restart_pending = true;
    session.commit(result.clone()).await.unwrap();
    result.state = EnvironmentState::Running;
    result.revision = 5;
    result.runtime.id = "restart-1-r4".into();
    result.resources_held = true;
    result.restart_pending = false;
    result.restart_attempts = 1;
    session.commit(result.clone()).await.unwrap();
    let restored = db.begin(1).await.unwrap();
    assert_eq!(
        restored.snapshot().await.unwrap().environments["restart"].result,
        Some(result.clone())
    );
    result.revision += 1;
    result.restart_attempts = 0;
    assert_eq!(restored.commit(result).await, Err(Error::Conflict));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn credential_revocation_survives_restart_and_bootstrap_replay() {
    use adx_coordinator::auth::{digest, Credential};
    let mut redis = common::Redis::new().await;
    let store = redis.store().await;
    let first = store.begin(1).await.unwrap();
    let key = "test-only-tenant-api-key-01234567890123456789";
    let credential = Credential {
        tenant_id: "tenant".into(),
        administrator: false,
        expires_at_unix_seconds: 0,
    };
    first.bootstrap_credential(key, &credential).await.unwrap();
    let id = digest(key).unwrap();
    assert_eq!(first.list_credentials().await.unwrap().len(), 1);
    first.revoke_credential(&id).await.unwrap();
    first.revoke_credential(&id).await.unwrap();
    assert!(matches!(first.credential(&id).await, Err(Error::NotFound)));
    let _next = store.begin(1).await.unwrap();
    assert!(matches!(
        first.revoke_credential(&id).await,
        Err(Error::Conflict)
    ));
    redis.crash();
    redis.start().await;
    let second = redis.store().await.begin(1).await.unwrap();
    second.bootstrap_credential(key, &credential).await.unwrap();
    assert!(matches!(second.credential(&id).await, Err(Error::NotFound)));
    assert!(second.list_credentials().await.unwrap().is_empty());
    second
        .bootstrap_credential(
            &"admin".repeat(10),
            &Credential {
                administrator: true,
                ..credential
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        second
            .revoke_credential(&digest(&"admin".repeat(10)).unwrap())
            .await,
        Err(Error::Invalid(_))
    ));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn expired_executions_remain_invalid_after_redis_and_coordinator_restart() {
    let mut rig = common::Redis::new().await;
    let db = rig.store().await;
    let session = db.begin(1).await.unwrap();
    session
        .register_session(
            node("lost"),
            "127.0.0.1:9000".into(),
            "127.0.0.1:9001".into(),
            Some(adx_coordinator::storage::NodeSession {
                id: "boot".into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    register(&session, "healthy").await;
    let mut records = vec![];
    for (index, (id, owner)) in [
        ("running", "lost"),
        ("pending", "lost"),
        ("kept", "healthy"),
    ]
    .into_iter()
    .enumerate()
    {
        let assignment = Assignment {
            environment_id: id.into(),
            node_id: owner.into(),
            shard_id: 0,
            generation: index as u64 + 1,
            devices: vec![],
        };
        session.reserve(spec(id), assignment.clone()).await.unwrap();
        if id != "pending" {
            let result = running(spec(id), assignment);
            session.commit(result.clone()).await.unwrap();
            records.push(result);
        }
    }
    assert_eq!(
        session.invalidate_node("lost", "wrong").await,
        Err(Error::Conflict)
    );
    let failed = session.invalidate_node("lost", "boot").await.unwrap();
    assert_eq!(failed.routes().unwrap().len(), 1);
    for id in ["running", "pending"] {
        let environment = &failed.environments[id];
        assert!(environment.invalidated);
        let result = environment.result.as_ref().unwrap();
        assert_eq!(result.state, EnvironmentState::Failed);
        assert!(!result.resources_held && !result.restart_pending);
        assert!(result.runtime.ip.is_none());
    }
    assert!(!failed.environments["kept"].invalidated);
    assert_eq!(
        session
            .invalidate_node("lost", "boot")
            .await
            .unwrap()
            .revision,
        failed.revision
    );
    rig.crash();
    rig.start().await;
    let recovered = rig.store().await.begin(1).await.unwrap();
    assert!(recovered.get("running").await.unwrap().invalidated);
    let mut stale = records[0].clone();
    stale.revision = 1000;
    assert_eq!(recovered.commit(stale.clone()).await, Err(Error::Conflict));
    stale.state = EnvironmentState::Failed;
    stale.resources_held = false;
    stale.runtime.ip = None;
    recovered.commit(stale).await.unwrap();
    assert_eq!(
        recovered.get("kept").await.unwrap().result,
        Some(records[1].clone())
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn recovery_assignment_is_durable_and_old_generation_cannot_publish() {
    let rig = common::Redis::new().await;
    let db = rig.store().await;
    let session = db.begin(1).await.unwrap();
    session
        .register_session(
            node("source"),
            "source:1".into(),
            "source:2".into(),
            Some(adx_coordinator::storage::NodeSession {
                id: "boot".into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    register(&session, "target").await;
    let previous = Assignment {
        environment_id: "held".into(),
        node_id: "source".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(spec("held"), previous.clone())
        .await
        .unwrap();
    let mut r = running(spec("held"), previous.clone());
    r.checkpoint = Some(adx_core::RestorePoint {
        id: "pause".into(),
        origin: None,
        expires_at_unix_seconds: u64::MAX,
        source_runtime_id: "held-1".into(),
        artifact: adx_core::CheckpointArtifact {
            storage: "shared".into(),
            location: "artifact".into(),
            size_bytes: 8,
        },
    });
    session.commit(r.clone()).await.unwrap();
    let replacement = Assignment {
        node_id: "target".into(),
        generation: 2,
        ..previous.clone()
    };
    assert!(session
        .reserve_recovery(&previous, replacement.clone(), 1)
        .await
        .is_err());
    session.invalidate_node("source", "boot").await.unwrap();
    let saved = session
        .reserve_recovery(&previous, replacement.clone(), 1)
        .await
        .expect("shared checkpoint transfers under a new generation");
    assert_eq!(saved.assignment, replacement);
    assert!(saved.resources_held());
    assert_eq!(
        saved.result.as_ref().unwrap().state,
        EnvironmentState::Paused
    );
    assert_eq!(
        saved,
        session
            .reserve_recovery(&previous, replacement.clone(), 1)
            .await
            .unwrap()
    );
    let restarted = db.begin(1).await.unwrap();
    assert_eq!(restarted.get("held").await.unwrap(), saved);
    let scheduler =
        Coordinator::restore(&restarted.snapshot().await.unwrap(), Placement::Pack).unwrap();
    assert!(
        scheduler.snapshot().environments().contains_key("held"),
        "pending recovery reservation survives Coordinator restart"
    );
    r.revision = 1000;
    assert_eq!(restarted.commit(r).await, Err(Error::Conflict));
    let mut restored = saved.result.unwrap();
    restored.state = EnvironmentState::Running;
    restored.resources_held = true;
    restored.runtime.ip = Some("10.0.0.3".parse().unwrap());
    restored.runtime.id = "held-2-r2".into();
    restored.revision += 2;
    restored.last_operation = Some(adx_core::CompletedOperation {
        id: "recover-2".into(),
        kind: adx_core::LifecycleKind::Resume,
        expected_revision: 1,
    });
    restarted.commit(restored.clone()).await.unwrap();
    assert_eq!(
        restarted.snapshot().await.unwrap().routes().unwrap()[0].node_id,
        "target"
    );
    assert_eq!(restarted.commit(restored.clone()).await.unwrap(), restored);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn recovery_never_uses_missing_local_or_expired_checkpoint() {
    for kind in ["missing", "local", "expired"] {
        let rig = common::Redis::new().await;
        let session = rig.store().await.begin(1).await.unwrap();
        session
            .register_session(
                node("source"),
                "source:1".into(),
                "source:2".into(),
                Some(adx_coordinator::storage::NodeSession {
                    id: "boot".into(),
                    sequence: 1,
                    routable: true,
                }),
            )
            .await
            .unwrap();
        register(&session, "target").await;
        let previous = Assignment {
            environment_id: "held".into(),
            node_id: "source".into(),
            shard_id: 0,
            generation: 1,
            devices: vec![],
        };
        session
            .reserve(spec("held"), previous.clone())
            .await
            .unwrap();
        let mut r = running(spec("held"), previous.clone());
        if kind != "missing" {
            r.checkpoint = Some(adx_core::RestorePoint {
                id: "pause".into(),
                origin: None,
                expires_at_unix_seconds: if kind == "expired" { 1 } else { u64::MAX },
                source_runtime_id: "held-1".into(),
                artifact: adx_core::CheckpointArtifact {
                    storage: if kind == "local" { "local" } else { "shared" }.into(),
                    location: "artifact".into(),
                    size_bytes: 8,
                },
            });
        }
        session.commit(r).await.unwrap();
        session.invalidate_node("source", "boot").await.unwrap();
        let before = session.get("held").await.unwrap();
        assert!(before.recovery_point(2).is_none(), "{kind}");
        assert!(
            session
                .reserve_recovery(
                    &previous,
                    Assignment {
                        node_id: "target".into(),
                        generation: 2,
                        ..previous.clone()
                    },
                    2
                )
                .await
                .is_err(),
            "{kind}"
        );
        assert_eq!(session.get("held").await.unwrap(), before);
        assert_eq!(before.result.unwrap().state, EnvironmentState::Failed);
    }
}

#[path = "storage/admin_keys.rs"]
mod admin_keys;
