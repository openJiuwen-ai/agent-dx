mod common;
use adx_core::{Assignment, Error, InstanceRecord, InstanceSpec, InstanceState, Resources};
use adx_master::{storage::StoredNode, Master, Node, Placement};

fn spec(id: &str) -> InstanceSpec {
    InstanceSpec {
        env: Default::default(),
        id: id.into(),
        tenant_id: "tenant".into(),
        image: "image".into(),
        runtime: "runc".into(),
        resources: Resources {
            cpu_millis: 100,
            memory_bytes: 128,
            disk_bytes: 0,
        },
        priority: 0,
        scheduling: Default::default(),
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
fn running(spec: InstanceSpec, assignment: Assignment) -> InstanceRecord {
    InstanceRecord {
        runtime_id: format!("{}-{}", spec.id, assignment.generation),
        spec,
        assignment,
        state: InstanceState::Running,
        revision: 2,
        resources_held: true,
        runtime_ip: Some("10.0.0.2".parse().unwrap()),
    }
}
async fn register(s: &adx_master::storage::Session, id: &str) -> StoredNode {
    s.register(node(id), "127.0.0.1:9000".into(), "127.0.0.1:9001".into())
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn restart_recovers_assignment_and_fences_old_writer() {
    let rig = common::Redis::new().await;
    let db = rig.store().await;
    let first = db.begin(2).await.unwrap();
    assert_eq!(register(&first, "z").await.domain_id, 0);
    assert_eq!(register(&first, "a").await.domain_id, 1);
    let mut scheduler = Master::restore(&first.snapshot().await.unwrap(), Placement::Pack).unwrap();
    // Restored nodes must re-register before they can receive new work.
    scheduler.submit(spec("instance")).unwrap();
    assert!(scheduler.schedule(0).unwrap().is_none());
    scheduler.register(node("z")).unwrap();
    let a = scheduler.schedule(0).unwrap().unwrap();
    first.reserve(spec("instance"), a.clone()).await.unwrap();
    let r = running(spec("instance"), a.clone());
    first.commit(r.clone()).await.unwrap();
    let version = first.snapshot().await.unwrap().revision;
    first.commit(r.clone()).await.unwrap();
    assert_eq!(first.snapshot().await.unwrap().revision, version);
    let second = db.begin(2).await.unwrap();
    assert_eq!(first.commit(r.clone()).await, Err(Error::Conflict));
    let snapshot = second.snapshot().await.unwrap();
    assert_eq!(snapshot.routes().unwrap().len(), 1);
    let mut restored = Master::restore(&snapshot, Placement::Pack).unwrap();
    assert_eq!(restored.register(node("z")).unwrap(), 0);
    assert_eq!(restored.register(node("a")).unwrap(), 1);
    restored.submit(spec("waiting")).unwrap();
    assert!(restored.schedule(0).unwrap().is_none());
    let mut deleted = r.clone();
    deleted.state = InstanceState::Deleted;
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
    let mut restored = Master::restore(&snap, Placement::Pack).unwrap();
    assert_eq!(restored.pending(0).unwrap(), 0);
    restored.register(node("z")).unwrap();
    assert_eq!(restored.submit(spec("instance")), Err(Error::Conflict));
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
        instance_id: "i".into(),
        node_id: "n".into(),
        domain_id: 0,
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
    changed.runtime_ip = Some("10.0.0.3".parse().unwrap());
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
    failed.state = InstanceState::Failed;
    failed.revision += 1;
    s.commit(failed).await.unwrap();
    let snap = s.snapshot().await.unwrap();
    assert!(snap.routes().unwrap().is_empty());
    let mut master = Master::restore(&snap, Placement::Pack).unwrap();
    master.register(node("n")).unwrap();
    master.submit(spec("next")).unwrap();
    assert!(master.schedule(0).unwrap().is_none());
}

#[tokio::test]
#[ignore = "requires real Redis; build/ci/run.py storage"]
async fn aof_crash_restart_preserves_state_and_reconnect_does_not_change_epoch() {
    let mut rig = common::Redis::new().await;
    let db = rig.store().await;
    let s = db.begin(1).await.unwrap();
    register(&s, "node").await;
    let assignment = Assignment {
        instance_id: "i".into(),
        node_id: "node".into(),
        domain_id: 0,
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
    // Use the existing Master session: a Redis reconnect does not acquire a new epoch.
    assert_eq!(s.snapshot().await.unwrap(), before);
    s.commit(r).await.unwrap();
    let restarted = rig.store().await.begin(1).await.unwrap();
    let after = restarted.snapshot().await.unwrap();
    assert_eq!(after.instances, before.instances);
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
        instance_id: "i".into(),
        node_id: "a".into(),
        domain_id: 0,
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
    let credential = adx_master::auth::Credential {
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
    let digest = adx_master::auth::digest(&key).unwrap();
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
async fn discovery_expires_and_rejects_superseded_master() {
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
        .advertise("test", "http://127.0.0.1:9200", Duration::from_secs(5))
        .await
        .is_err());
}
